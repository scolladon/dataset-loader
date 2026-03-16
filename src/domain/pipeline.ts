import { PassThrough, Readable, type Writable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import {
  buildAugmentSuffix,
  createAugmentTransform,
} from '../adapters/augment-transform.js'
import { createFanOutTransform } from '../adapters/fan-out-transform.js'
import { createRowCounter } from '../adapters/row-counter.js'
import {
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

export interface PipelineInput {
  readonly entries: readonly PipelineEntry[]
  readonly watermarks: WatermarkStore
  readonly createWriter: CreateWriterPort
  readonly state: StatePort
  readonly progress: ProgressPort
  readonly logger: LoggerPort
}

export interface PipelineResult {
  readonly entriesProcessed: number
  readonly entriesSkipped: number
  readonly entriesFailed: number
  readonly groupsUploaded: number
  readonly exitCode: number
}

export class DatasetGroup implements HeaderProvider {
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

  async resolveHeader(): Promise<string> {
    if (this.entries.length === 0)
      throw new Error('DatasetGroup has no entries')
    for (const entry of this.entries) {
      const h = await entry.header()
      if (h) return h
    }
    return ''
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
  readonly group: DatasetGroup
  readonly tracker: GroupTracker
  pendingEntries: number
  entryResults: EntryResult[]
}

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
    }
    const writer = input.createWriter.create(
      group.datasetKey,
      group.operation,
      listener,
      group
    )
    try {
      const chunker = await writer.init()
      slots.set(group.key, {
        writer,
        chunker,
        group,
        tracker,
        pendingEntries: group.entries.length,
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
        processBundleEntries(bundle, slots, input, phase).catch(
          (error: unknown) => {
            input.logger.warn(`Bundle failed: ${formatErrorMessage(error)}`)
          }
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

async function processBundleEntries(
  bundle: ReaderBundle,
  slots: Map<string, WriterSlot>,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick' | 'trackGroup'>
): Promise<void> {
  const active = bundle.entries.filter(e => slots.has(e.datasetKey.toString()))
  if (active.length === 0) return

  if (active.length === 1) {
    const entry = active[0]
    const slot = slots.get(entry.datasetKey.toString())!
    const result = await pipeEntry(
      entry,
      input,
      slot.chunker,
      phase,
      slot.tracker
    )
    slot.entryResults.push(result)
  } else {
    await fanOutEntries(active, bundle.watermark, slots, input, phase)
  }

  for (const entry of active) {
    const slot = slots.get(entry.datasetKey.toString())!
    slot.pendingEntries--
    if (slot.pendingEntries === 0) {
      slot.chunker.end()
      await finished(slot.chunker)
    }
  }
}

interface ChannelOutcome {
  readonly entry: PipelineEntry
  readonly slot: WriterSlot
  readonly ok: boolean
}

async function fanOutEntries(
  entries: readonly PipelineEntry[],
  watermark: Watermark | undefined,
  slots: Map<string, WriterSlot>,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  // all entries in a fan-out bundle share the same reader key and therefore the same fetcher
  let result: FetchResult
  try {
    result = await entries[0].fetcher.fetch(watermark)
  } catch (err) {
    for (const entry of entries) {
      const slot = slots.get(entry.datasetKey.toString())!
      input.logger.warn(
        `Entry '${entry.label}' failed: ${formatErrorMessage(err)}`
      )
      phase.tick(`  [${entry.index}] ${entry.label} — failed`)
      slot.entryResults.push({ status: 'failed' })
    }
    return
  }

  const channels = entries.map(() => new PassThrough({ objectMode: true }))

  const fanOut = createFanOutTransform(channels, (err, ch) => {
    const idx = channels.indexOf(ch as PassThrough)
    if (idx >= 0) {
      input.logger.warn(
        `Entry '${entries[idx].label}' fan-out write failed: ${formatErrorMessage(err)}`
      )
    }
  })

  const sourcePipeline = pipeline(Readable.from(result.lines), fanOut).catch(
    (err: Error) => {
      input.logger.warn(`Fan-out source failed: ${formatErrorMessage(err)}`)
      channels.forEach(ch => ch.destroy(err))
    }
  )

  const outcomes = await Promise.all([
    sourcePipeline,
    ...entries.map((entry, i): Promise<ChannelOutcome> => {
      const slot = slots.get(entry.datasetKey.toString())!
      const augmented = createAugmentTransform(
        buildAugmentSuffix(entry.augmentColumns)
      )
      const counted = createRowCounter(slot.tracker)
      counted.pipe(slot.chunker, { end: false })

      return pipeline(channels[i], augmented, counted)
        .then((): ChannelOutcome => {
          counted.unpipe(slot.chunker)
          return { entry, slot, ok: true }
        })
        .catch((err: Error): ChannelOutcome => {
          counted.unpipe(slot.chunker)
          input.logger.warn(
            `Entry '${entry.label}' failed: ${formatErrorMessage(err)}`
          )
          return { entry, slot, ok: false }
        })
    }),
  ])

  // Read shared values once — all pipelines (source + channels) are now complete
  const fileCount = result.fileCount()
  const wm = result.watermark()

  for (const outcome of outcomes.slice(1) as ChannelOutcome[]) {
    const { entry, slot, ok } = outcome
    if (!ok) {
      slot.entryResults.push({ status: 'failed' })
      continue
    }
    slot.entryResults.push(
      resolveEntryResult(entry, fileCount, wm, slot.tracker, phase)
    )
  }
}

async function finalizeSlot(
  slot: WriterSlot,
  input: PipelineInput
): Promise<GroupResult> {
  try {
    return await finalizeGroup(slot.group, slot.entryResults, slot.writer)
  } catch (error) {
    input.logger.warn(
      `Finalize failed for '${slot.group.datasetKey.name}': ${formatErrorMessage(error)}`
    )
    await slot.writer.abort().catch((abortError: unknown) => {
      input.logger.debug(`abort failed: ${formatErrorMessage(abortError)}`)
    })
    return {
      processed: 0,
      skipped: 0,
      failed: slot.group.entries.length,
      watermarks: [],
    }
  } finally {
    slot.tracker.stop()
  }
}

async function pipeEntry(
  entry: PipelineEntry,
  input: PipelineInput,
  chunker: Writable,
  phase: { tick: (detail: string) => void },
  tracker: Pick<GroupTracker, 'addFiles' | 'addRows'>
): Promise<EntryResult> {
  try {
    const watermark = input.watermarks.get(entry.watermarkKey)
    const result = await entry.fetcher.fetch(watermark)

    const suffix = buildAugmentSuffix(entry.augmentColumns)
    const source = Readable.from(result.lines)
    const augmented = createAugmentTransform(suffix)
    const counted = createRowCounter(tracker)
    counted.pipe(chunker, { end: false })

    try {
      await pipeline(source, augmented, counted)
    } finally {
      counted.unpipe(chunker)
    }

    return resolveEntryResult(
      entry,
      result.fileCount(),
      result.watermark(),
      tracker,
      phase
    )
  } catch (error) {
    input.logger.warn(
      `Entry '${entry.label}' failed: ${formatErrorMessage(error)}`
    )
    return { status: 'failed' }
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
