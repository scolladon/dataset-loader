import { PassThrough, Readable, Writable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { buildAugmentSuffix } from '../adapters/pipeline/augment-transform.js'
import { FanInStream } from '../adapters/pipeline/fan-in-stream.js'
import { createFanOutTransform } from '../adapters/pipeline/fan-out-transform.js'
import {
  type BatchMiddleware,
  type CreateWriterPort,
  type FetchResult,
  formatErrorMessage,
  type GroupTracker,
  type HeaderProvider,
  type LoggerPort,
  type Operation,
  type PhaseProgress,
  type ProgressListener,
  type ProgressPort,
  type ReaderPort,
  SkipDatasetError,
  type StatePort,
  type WatermarkEntry,
  type Writer,
} from '../ports/types.js'
import { type DatasetKey } from './dataset-key.js'
import { type ReaderKey } from './reader-key.js'
import { type Watermark } from './watermark.js'
import { type WatermarkKey } from './watermark-key.js'
import { type WatermarkStore } from './watermark-store.js'

export interface PipelineEntry {
  readonly index: number
  readonly label: string
  readonly readerKey: ReaderKey
  readonly watermarkKey: WatermarkKey
  readonly datasetKey: DatasetKey
  readonly operation: Operation
  readonly augmentColumns: Record<string, string>
  readonly fetcher: ReaderPort
  header(): Promise<string>
}

export interface ReaderBundle {
  readonly readerKey: ReaderKey
  readonly watermark: Watermark | undefined
  readonly entries: readonly PipelineEntry[]
}

export function groupByReader(
  entries: readonly PipelineEntry[],
  watermarks: WatermarkStore
): ReaderBundle[] {
  const map = new Map<string, ReaderBundle>()
  for (const entry of entries) {
    const watermark = watermarks.get(entry.watermarkKey)
    const key = `${entry.readerKey.toString()}\u0000${watermark?.toString() ?? ''}`
    const existing = map.get(key)
    if (existing) {
      map.set(key, { ...existing, entries: [...existing.entries, entry] })
    } else {
      map.set(key, { readerKey: entry.readerKey, watermark, entries: [entry] })
    }
  }
  return [...map.values()]
}

interface PipelineInput {
  readonly entries: readonly PipelineEntry[]
  readonly watermarks: WatermarkStore
  readonly createWriter: CreateWriterPort
  readonly state: StatePort
  readonly progress: ProgressPort
  readonly logger: LoggerPort
}

interface PipelineResult {
  readonly entriesProcessed: number
  readonly entriesSkipped: number
  readonly entriesFailed: number
  readonly groupsUploaded: number
  readonly exitCode: number
}

export class DatasetGroup {
  private constructor(
    readonly key: string,
    readonly datasetKey: DatasetKey,
    readonly operation: Operation,
    readonly entries: readonly PipelineEntry[]
  ) {}

  static from(
    key: string,
    datasetKey: DatasetKey,
    operation: Operation,
    entries: readonly PipelineEntry[]
  ): DatasetGroup {
    return new DatasetGroup(key, datasetKey, operation, entries)
  }
}

export function createHeaderProvider(
  entries: readonly PipelineEntry[]
): HeaderProvider {
  return {
    resolveHeader: async () => {
      for (const entry of entries) {
        const h = await entry.header()
        if (h) return h
      }
      return ''
    },
  }
}

interface EntryResult {
  readonly status: 'processed' | 'skipped' | 'failed'
  readonly watermark?: WatermarkEntry
}

interface GroupResult {
  readonly processed: number
  readonly skipped: number
  readonly failed: number
  readonly watermarks: readonly WatermarkEntry[]
}

interface WriterSlot {
  readonly writer: Writer
  readonly chunker: Writable
  readonly fanIn: FanInStream
  readonly group: DatasetGroup
  readonly tracker: GroupTracker
  readonly entryResults: EntryResult[]
}

type Sink = { entry: PipelineEntry; slot: WriterSlot; sink: Writable }

export async function executePipeline(
  input: PipelineInput
): Promise<PipelineResult> {
  const groups = groupByDataset(input.entries)
  const bundles = groupByReader(input.entries, input.watermarks)
  const phase = input.progress.create('Processing', input.entries.length)

  // Phase 1: initialise one writer slot per DatasetGroup
  const slots = new Map<string, WriterSlot>()
  const skipResults: GroupResult[] = []

  for (const group of groups) {
    const tracker = phase.trackGroup(
      group.datasetKey.name,
      group.datasetKey.org !== undefined
    )
    const listener: ProgressListener = {
      onSinkReady: (id: string) => tracker.updateParentId(id),
      onChunkWritten: () => tracker.incrementParts(),
      onRowsWritten: (count: number) => tracker.addRows(count),
    }
    const writer = input.createWriter.create(
      group.datasetKey,
      group.operation,
      listener,
      createHeaderProvider(group.entries)
    )
    try {
      const chunker = await writer.init()
      slots.set(group.key, {
        writer,
        chunker,
        fanIn: new FanInStream(chunker, group.entries.length),
        group,
        tracker,
        entryResults: [],
      })
    } catch (error) {
      tracker.stop()
      if (error instanceof SkipDatasetError) {
        input.logger.warn((error as SkipDatasetError).message)
        skipResults.push({
          processed: 0,
          skipped: group.entries.length,
          failed: 0,
          watermarks: [],
        })
      } else {
        await writer
          .abort()
          .catch((e: unknown) =>
            input.logger.debug(`abort failed: ${formatErrorMessage(e)}`)
          )
        skipResults.push({
          processed: 0,
          skipped: 0,
          failed: group.entries.length,
          watermarks: [],
        })
      }
    }
  }

  // Phase 2: process reader bundles (solo or fan-out) concurrently
  await Promise.all(
    bundles
      .filter(bundle =>
        bundle.entries.some(e => slots.has(e.datasetKey.toString()))
      )
      .map(bundle =>
        processBundle(bundle, slots, input, phase).catch(
          /* v8 ignore next */
          (error: unknown) =>
            input.logger.warn(`Bundle failed: ${formatErrorMessage(error)}`)
        )
      )
  )

  // Phase 3: finalize all writer slots
  const groupResults = await Promise.all(
    [...slots.values()].map(slot => finalizeSlot(slot, input))
  )

  const { store, pipelineResult } = aggregateResults(
    [...skipResults, ...groupResults],
    input.watermarks
  )
  phase.stop()
  await input.state.write(store)

  return pipelineResult
}

async function processBundle(
  bundle: ReaderBundle,
  slots: Map<string, WriterSlot>,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const active = bundle.entries.filter(e => slots.has(e.datasetKey.toString()))
  /* v8 ignore next -- outer filter ensures at least one entry has a slot; defensive guard */
  if (active.length === 0) return

  // Create fanIn slots upfront — FanInStream counter must decrement even if fetch fails
  const sinks = active.map(entry => {
    const slot = slots.get(entry.datasetKey.toString())!
    const suffix = buildAugmentSuffix(entry.augmentColumns)
    const transforms: BatchMiddleware[] = []
    if (suffix) {
      transforms.push(batch => batch.map(line => line + suffix))
    }
    return { entry, slot, sink: slot.fanIn.createSlot(transforms) }
  })

  let result: FetchResult
  try {
    result = await active[0].fetcher.fetch(bundle.watermark)
  } catch (err) {
    for (const { entry, slot, sink } of sinks) {
      sink.destroy()
      recordEntryFailure(entry, slot, err, input.logger, phase)
    }
    return
  }

  if (sinks.length === 1) {
    await pipeSingleEntry(sinks[0], result, input, phase)
  } else {
    await pipeFanOutEntries(sinks, result, input, phase)
  }
}

function pipelineWithEntryTracking(
  source: Readable,
  { entry, slot, sink }: Sink,
  result: FetchResult,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  return pipeline(source, sink)
    .then((): void => {
      slot.entryResults.push(
        resolveEntryResult(
          entry,
          result.fileCount(),
          result.watermark(),
          slot.tracker,
          phase
        )
      )
    })
    .catch((err: Error): void => {
      recordEntryFailure(entry, slot, err, input.logger, phase)
    })
}

async function pipeSingleEntry(
  sink: Sink,
  result: FetchResult,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  await pipelineWithEntryTracking(
    Readable.from(result.lines),
    sink,
    result,
    input,
    phase
  )
}

async function pipeFanOutEntries(
  sinks: Sink[],
  result: FetchResult,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const channels = sinks.map(() => new PassThrough({ objectMode: true }))
  const fanOut = createFanOutTransform(channels, (err, ch) => {
    const idx = channels.indexOf(ch as PassThrough)
    input.logger.warn(
      `Entry '${sinks[idx].entry.label}' fan-out write failed: ${formatErrorMessage(err)}`
    )
  })
  await Promise.all([
    pipeline(Readable.from(result.lines), fanOut).catch((err: Error) => {
      input.logger.warn(`Fan-out source failed: ${formatErrorMessage(err)}`)
      channels.forEach(ch => ch.destroy(err))
    }),
    ...sinks.map((sink, i) =>
      pipelineWithEntryTracking(channels[i], sink, result, input, phase)
    ),
  ])
}

async function finalizeSlot(
  slot: WriterSlot,
  input: PipelineInput
): Promise<GroupResult> {
  // Wait for chunker to finish — FanInStream owns end() and calls it when all producer slots close
  await finished(slot.chunker).catch(() => {
    // chunker errors are already reflected in slot.entryResults
  })
  try {
    return await finalizeGroup(slot.group, slot.entryResults, slot.writer)
  } catch (error) {
    input.logger.warn(
      `Finalize failed for '${slot.group.datasetKey.name}': ${formatErrorMessage(error)}`
    )
    await slot.writer.abort().catch(
      /* v8 ignore next */
      (abortError: unknown) =>
        input.logger.debug(`abort failed: ${formatErrorMessage(abortError)}`)
    )
    return {
      processed: 0,
      skipped: 0,
      failed: slot.group.entries.length,
      watermarks: [],
    }
    /* v8 ignore next 3 -- finally runs in all paths; exception-propagation branch not triggered */
  } finally {
    slot.tracker.stop()
  }
}

async function finalizeGroup(
  group: DatasetGroup,
  entryResults: readonly EntryResult[],
  writer: Writer
): Promise<GroupResult> {
  const skipped = entryResults.filter(r => r.status === 'skipped').length
  const failed = entryResults.filter(r => r.status === 'failed').length
  const hasData = entryResults.some(r => r.status === 'processed')

  if (failed > 0) {
    await writer.abort()
    return {
      processed: 0,
      skipped,
      failed: group.entries.length - skipped,
      watermarks: [],
    }
  }

  if (hasData) {
    await writer.finalize()
    return {
      processed: group.entries.length - skipped - failed,
      skipped,
      failed: 0,
      watermarks: entryResults.flatMap(r => (r.watermark ? [r.watermark] : [])),
    }
  }

  await writer.skip()
  return { processed: 0, skipped, failed: 0, watermarks: [] }
}

function aggregateResults(
  results: readonly GroupResult[],
  initialStore: WatermarkStore
): {
  readonly store: WatermarkStore
  readonly pipelineResult: PipelineResult
} {
  const {
    entriesProcessed,
    entriesSkipped,
    entriesFailed,
    groupsUploaded,
    store,
  } = results.reduce(
    (acc, { processed, skipped, failed, watermarks }) => ({
      entriesProcessed: acc.entriesProcessed + processed,
      entriesSkipped: acc.entriesSkipped + skipped,
      entriesFailed: acc.entriesFailed + failed,
      groupsUploaded: acc.groupsUploaded + (processed > 0 ? 1 : 0),
      store: watermarks.reduce(
        (s, { key, watermark }) => s.set(key, watermark),
        acc.store
      ),
    }),
    {
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: 0,
      groupsUploaded: 0,
      store: initialStore,
    }
  )

  return {
    store,
    pipelineResult: {
      entriesProcessed,
      entriesSkipped,
      entriesFailed,
      groupsUploaded,
      exitCode: computeExitCode(entriesProcessed, entriesFailed),
    },
  }
}

function groupByDataset(entries: readonly PipelineEntry[]): DatasetGroup[] {
  const map = new Map<string, DatasetGroup>()
  for (const entry of entries) {
    const key = entry.datasetKey.toString()
    const existing = map.get(key)
    if (existing) {
      map.set(
        key,
        DatasetGroup.from(key, existing.datasetKey, existing.operation, [
          ...existing.entries,
          entry,
        ])
      )
    } else {
      map.set(
        key,
        DatasetGroup.from(key, entry.datasetKey, entry.operation, [entry])
      )
    }
  }
  return [...map.values()]
}

function recordEntryFailure(
  entry: PipelineEntry,
  slot: WriterSlot,
  err: unknown,
  logger: LoggerPort,
  phase: Pick<PhaseProgress, 'tick'>
): void {
  logger.warn(`Entry '${entry.label}' failed: ${formatErrorMessage(err)}`)
  phase.tick(`  [${entry.index}] ${entry.label} — failed`)
  slot.entryResults.push({ status: 'failed' })
}

function resolveEntryResult(
  entry: PipelineEntry,
  fileCount: number,
  wm: Watermark | undefined,
  tracker: Pick<GroupTracker, 'addFiles'>,
  phase: { tick: (detail: string) => void }
): EntryResult {
  tracker.addFiles(fileCount)
  if (fileCount === 0) {
    phase.tick(`  [${entry.index}] ${entry.label} — skipped (no new records)`)
    return { status: 'skipped' }
  }
  phase.tick(`  [${entry.index}] ${entry.label} — done`)
  return {
    status: 'processed',
    watermark: wm ? { key: entry.watermarkKey, watermark: wm } : undefined,
  }
}

function computeExitCode(processed: number, failed: number): number {
  if (failed === 0) return 0
  if (processed > 0) return 1
  return 2
}
