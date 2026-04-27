import {
  PassThrough,
  Readable,
  type Transform,
  type Writable,
} from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { buildAugmentSuffix } from '../adapters/pipeline/augment-transform.js'
import { createByteProgressTransform } from '../adapters/pipeline/byte-progress-transform.js'
import { type FanInStream } from '../adapters/pipeline/fan-in-stream.js'
import { createFanOutTransform } from '../adapters/pipeline/fan-out-transform.js'
import {
  type BatchMiddleware,
  type FetchResult,
  formatErrorMessage,
  type GroupTracker,
  type LoggerPort,
  type PhaseProgress,
  type ProjectionLayout,
  SkipDatasetError,
  type WatermarkEntry,
  type Writer,
} from '../ports/types.js'
import {
  type DatasetGroup,
  type PipelineEntry,
  type ReaderBundle,
} from './pipeline-groups.js'
import { layoutsEqual } from './pipeline-layout.js'
import { buildSObjectRowProjection } from './sobject-row-projection.js'
import { type Watermark } from './watermark.js'

export interface EntryResult {
  readonly status: 'processed' | 'skipped' | 'failed'
  readonly watermark?: WatermarkEntry
}

export interface GroupResult {
  readonly processed: number
  readonly skipped: number
  readonly failed: number
  readonly watermarks: readonly WatermarkEntry[]
}

export interface WriterSlot {
  readonly writer: Writer
  readonly chunker: Writable
  // Dataset metadata column order (undefined for FileWriter and for
  // DatasetWriter without alignment). Pipeline builds per-entry layouts
  // from this at project() time.
  readonly datasetFields?: readonly string[]
  readonly fanIn: FanInStream
  readonly group: DatasetGroup
  readonly tracker: GroupTracker
  readonly entryResults: EntryResult[]
}

type Sink = { entry: PipelineEntry; slot: WriterSlot; sink: Writable }
type SinkWithLayout = Sink & { layout: ProjectionLayout | undefined }

interface BundleRunInput {
  readonly logger: LoggerPort
}

// Defensive guard: writers only emit datasetFields when their reader is
// expected to project. Mis-wiring this would mis-emit rows instead of
// failing loudly. Always-throws when triggered; never reached in practice.
/* v8 ignore next 6 */
function assertNoLayoutWithoutProject(sinks: readonly SinkWithLayout[]): void {
  if (sinks.some(s => s.layout !== undefined)) {
    throw new Error(
      'internal: layout supplied for non-projecting reader (plumbing bug)'
    )
  }
}

// Build the per-entry ProjectionLayout for an SObject sink, or undefined
// for ELF/CSV or file targets. Throws SkipDatasetError on set/overlap
// mismatch — the caller records a per-entry failure.
function buildLayoutFor(
  entry: PipelineEntry,
  slot: WriterSlot
): ProjectionLayout | undefined {
  if (!slot.datasetFields) return undefined
  if (entry.alignment.readerKind !== 'sobject') return undefined
  return buildSObjectRowProjection({
    datasetName: slot.group.datasetKey.name,
    entryLabel: entry.alignment.entryLabel,
    readerFields: entry.alignment.providedFields,
    augmentColumns: entry.alignment.augmentColumns,
    datasetFields: slot.datasetFields,
  })
}

function dedupTrackers(sinks: readonly Sink[]): GroupTracker[] {
  const seen = new Set<GroupTracker>()
  const trackers: GroupTracker[] = []
  for (const { slot } of sinks) {
    if (seen.has(slot.tracker)) continue
    seen.add(slot.tracker)
    trackers.push(slot.tracker)
  }
  return trackers
}

export async function processBundle(
  bundle: ReaderBundle,
  slots: Map<string, WriterSlot>,
  input: BundleRunInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const active = bundle.entries.filter(e => slots.has(e.datasetKey.toString()))
  /* v8 ignore next -- outer filter ensures at least one entry has a slot; defensive guard */
  if (active.length === 0) return

  // Build per-entry projection layouts from each sink's datasetFields +
  // that entry's alignment (so augment *values* can differ per entry while
  // the dataset's column order stays shared). SkipDatasetError from the
  // projector build destroys the sink and records a per-entry failure.
  // The FanInStream counter is always released — either via sink.destroy
  // on failure, or via the normal pipeline close path on success.
  const viableSinks: SinkWithLayout[] = []
  for (const entry of active) {
    const slot = slots.get(entry.datasetKey.toString())!
    let layout: ProjectionLayout | undefined
    try {
      layout = buildLayoutFor(entry, slot)
    } catch (err) {
      const sink = slot.fanIn.createSlot([])
      sink.destroy()
      recordEntryFailure(entry, slot, err, input.logger, phase)
      continue
    }
    const transforms: BatchMiddleware[] = []
    if (layout === undefined) {
      const suffix = buildAugmentSuffix(entry.augmentColumns)
      if (suffix) transforms.push(batch => batch.map(line => line + suffix))
    }
    const sink = slot.fanIn.createSlot(transforms)
    viableSinks.push({ entry, slot, sink, layout })
  }
  if (viableSinks.length === 0) return

  // Fan-out constraint: all sinks sharing this reader must produce
  // identical projection layouts.
  const reader = active[0].fetcher
  if (reader.project) {
    const ref = viableSinks[0].layout
    const allEqual = viableSinks.every(s => layoutsEqual(ref, s.layout))
    if (!allEqual) {
      const err = new SkipDatasetError(
        'Cannot share SObject reader across sinks with divergent projections; split the config entries so their readerKeys differ'
      )
      for (const s of viableSinks) {
        s.sink.destroy()
        recordEntryFailure(s.entry, s.slot, err, input.logger, phase)
      }
      return
    }
    if (ref !== undefined) reader.project(ref)
  } else {
    assertNoLayoutWithoutProject(viableSinks)
  }

  const sinks = viableSinks.map(s => ({
    entry: s.entry,
    slot: s.slot,
    sink: s.sink,
  }))

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

  // Surface fetch-time totals onto each sink's per-reader bar. Trackers are
  // shared across entries within a single dataset slot, so dedupe before
  // calling setTotal — otherwise the same total is announced N times.
  if (result.total) {
    const total = result.total
    for (const tracker of dedupTrackers(sinks)) {
      tracker.setTotal(total.count, total.unit)
    }
  }

  if (sinks.length === 1) {
    await pipeSingleEntry(sinks[0], result, input, phase)
  } else {
    await pipeFanOutEntries(sinks, result, input, phase)
  }
}

function pipelineWithEntryTracking(
  source: Readable,
  middleware: Transform | undefined,
  { entry, slot, sink }: Sink,
  result: FetchResult,
  input: BundleRunInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const completed = middleware
    ? pipeline(source, middleware, sink)
    : pipeline(source, sink)
  return completed
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
  input: BundleRunInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const source = Readable.from(result.lines)
  const byteProgress =
    result.total?.unit === 'bytes'
      ? createByteProgressTransform(
          result.total.bytesRead,
          dedupTrackers([sink])
        )
      : undefined
  await pipelineWithEntryTracking(
    source,
    byteProgress,
    sink,
    result,
    input,
    phase
  )
}

async function pipeFanOutEntries(
  sinks: Sink[],
  result: FetchResult,
  input: BundleRunInput,
  phase: Pick<PhaseProgress, 'tick'>
): Promise<void> {
  const channels = sinks.map(() => new PassThrough({ objectMode: true }))
  const fanOut = createFanOutTransform(channels, (err, ch) => {
    const idx = channels.indexOf(ch as PassThrough)
    input.logger.warn(
      `Entry '${sinks[idx].entry.label}' fan-out write failed: ${formatErrorMessage(err)}`
    )
  })
  const source = Readable.from(result.lines)
  const byteProgress =
    result.total?.unit === 'bytes'
      ? createByteProgressTransform(
          result.total.bytesRead,
          dedupTrackers(sinks)
        )
      : undefined
  const sourcePipeline = byteProgress
    ? pipeline(source, byteProgress, fanOut)
    : pipeline(source, fanOut)
  await Promise.all([
    sourcePipeline.catch((err: Error) => {
      input.logger.warn(`Fan-out source failed: ${formatErrorMessage(err)}`)
      channels.forEach(ch => ch.destroy(err))
    }),
    ...sinks.map((sink, i) =>
      pipelineWithEntryTracking(
        channels[i],
        undefined,
        sink,
        result,
        input,
        phase
      )
    ),
  ])
}

export async function finalizeSlot(
  slot: WriterSlot,
  input: BundleRunInput
): Promise<GroupResult> {
  // Wait for chunker to finish — FanInStream owns end() and calls it when
  // all producer slots close.
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
