import { FanInStream } from '../adapters/pipeline/fan-in-stream.js'
import {
  type CreateWriterPort,
  formatErrorMessage,
  type GroupTracker,
  type LoggerPort,
  type PhaseProgress,
  type ProgressListener,
  type ProgressPort,
  SkipDatasetError,
  type StatePort,
  type Writer,
} from '../ports/types.js'
import {
  finalizeSlot,
  type GroupResult,
  processBundle,
  type WriterSlot,
} from './bundle-runner.js'
import {
  createHeaderProvider,
  type DatasetGroup,
  groupByDataset,
  groupByReader,
  type PipelineEntry,
  type ReaderBundle,
} from './pipeline-groups.js'
import { type WatermarkStore } from './watermark-store.js'

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

interface PreparedGroup {
  readonly group: DatasetGroup
  readonly tracker: GroupTracker
  readonly writer: Writer
}

type InitOutcome =
  | { readonly kind: 'slot'; readonly slot: WriterSlot }
  | { readonly kind: 'skip'; readonly result: GroupResult }

type PendingInits = ReadonlyMap<string, Promise<InitOutcome>>

export async function executePipeline(
  input: PipelineInput
): Promise<PipelineResult> {
  const groups = groupByDataset(input.entries)
  const bundles = groupByReader(input.entries, input.watermarks)
  const phase = input.progress.create('Processing', input.entries.length)

  const inits = startInits(groups, input, phase)
  await processBundles(bundles, inits, input, phase)
  const outcomes = await Promise.all(inits.values())
  const groupResults = await Promise.all(
    [...slotsOf(outcomes).values()].map(slot => finalizeSlot(slot, input))
  )

  const { store, pipelineResult } = aggregateResults(
    [...skipResultsOf(outcomes), ...groupResults],
    input.watermarks
  )
  phase.stop()
  await input.state.write(store)

  return pipelineResult
}

// Writers are created in group order and their inits run concurrently. The map
// keeps group order, so results stay in that order whatever order the inits
// settle in.
function startInits(
  groups: readonly DatasetGroup[],
  input: PipelineInput,
  phase: PhaseProgress
): PendingInits {
  const prepared = groups.map(group => prepareGroup(group, input, phase))
  return new Map(prepared.map(p => [p.group.key, initGroup(p, input)] as const))
}

// Reader bundles (solo or fan-out) run concurrently, each as soon as the
// writers it feeds are ready, so a slow init elsewhere does not hold it back.
async function processBundles(
  bundles: readonly ReaderBundle[],
  inits: PendingInits,
  input: PipelineInput,
  phase: PhaseProgress
): Promise<void> {
  await Promise.all(
    bundles.map(bundle => runWhenReady(bundle, inits, input, phase))
  )
}

// A reader shared by several datasets reads its source once and therefore
// waits for all of them.
async function runWhenReady(
  bundle: ReaderBundle,
  inits: PendingInits,
  input: PipelineInput,
  phase: PhaseProgress
): Promise<void> {
  const slots = slotsOf(await Promise.all(initsFedBy(bundle, inits)))
  // Stryker disable next-line ConditionalExpression: equivalent mutant — processBundle returns on its own when no entry has a slot
  if (slots.size === 0) return
  await processBundle(bundle, slots, input, phase).catch(
    /* v8 ignore next */
    (error: unknown) =>
      input.logger.warn(`Bundle failed: ${formatErrorMessage(error)}`)
  )
}

function initsFedBy(
  bundle: ReaderBundle,
  inits: PendingInits
): Promise<InitOutcome>[] {
  const keys = new Set(bundle.entries.map(e => e.datasetKey.toString()))
  return [...inits].filter(([key]) => keys.has(key)).map(([, init]) => init)
}

function slotsOf(outcomes: readonly InitOutcome[]): Map<string, WriterSlot> {
  return new Map(
    outcomes.flatMap(o =>
      o.kind === 'slot' ? [[o.slot.group.key, o.slot] as const] : []
    )
  )
}

function skipResultsOf(outcomes: readonly InitOutcome[]): GroupResult[] {
  return outcomes.flatMap(o => (o.kind === 'skip' ? [o.result] : []))
}

function prepareGroup(
  group: DatasetGroup,
  input: PipelineInput,
  phase: PhaseProgress
): PreparedGroup {
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
    createHeaderProvider(group.entries),
    group.entries[0].alignment,
    group.entries[0].bootstrap
  )
  return { group, tracker, writer }
}

async function initGroup(
  prepared: PreparedGroup,
  input: PipelineInput
): Promise<InitOutcome> {
  const { group, tracker, writer } = prepared
  try {
    const { chunker, datasetFields } = await writer.init()
    const slot: WriterSlot = {
      writer,
      chunker,
      datasetFields,
      fanIn: new FanInStream(chunker, group.entries.length),
      group,
      tracker,
      // Stryker disable next-line ArrayDeclaration: equivalent mutant — entryResults is only read through status/watermark filters, which ignore a non-result element
      entryResults: [],
    }
    return { kind: 'slot', slot }
  } catch (error) {
    return initFailure(error, prepared, input)
  }
}

async function initFailure(
  error: unknown,
  { group, tracker, writer }: PreparedGroup,
  input: PipelineInput
): Promise<InitOutcome> {
  tracker.stop()
  const entryCount = group.entries.length
  if (error instanceof SkipDatasetError) {
    input.logger.warn(error.message)
    return skipOutcome({ skipped: entryCount, failed: 0 })
  }
  input.logger.warn(
    `Writer init failed for '${group.datasetKey.name}': ${formatErrorMessage(error)}`
  )
  await writer
    .abort()
    .catch((e: unknown) =>
      input.logger.debug(`abort failed: ${formatErrorMessage(e)}`)
    )
  return skipOutcome({ skipped: 0, failed: entryCount })
}

function skipOutcome(
  counts: Pick<GroupResult, 'skipped' | 'failed'>
): InitOutcome {
  return { kind: 'skip', result: { processed: 0, ...counts, watermarks: [] } }
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

function computeExitCode(processed: number, failed: number): number {
  if (failed === 0) return 0
  if (processed > 0) return 1
  return 2
}
