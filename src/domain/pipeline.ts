import { FanInStream } from '../adapters/pipeline/fan-in-stream.js'
import {
  type CreateWriterPort,
  formatErrorMessage,
  type LoggerPort,
  type ProgressListener,
  type ProgressPort,
  SkipDatasetError,
  type StatePort,
} from '../ports/types.js'
import {
  finalizeSlot,
  type GroupResult,
  processBundle,
  type WriterSlot,
} from './bundle-runner.js'
import {
  createHeaderProvider,
  groupByDataset,
  groupByReader,
  type PipelineEntry,
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
      createHeaderProvider(group.entries),
      group.entries[0].alignment
    )
    try {
      const { chunker, datasetFields } = await writer.init()
      slots.set(group.key, {
        writer,
        chunker,
        datasetFields,
        fanIn: new FanInStream(chunker, group.entries.length),
        group,
        tracker,
        entryResults: [],
      })
    } catch (error) {
      tracker.stop()
      if (error instanceof SkipDatasetError) {
        input.logger.warn(error.message)
        skipResults.push({
          processed: 0,
          skipped: group.entries.length,
          failed: 0,
          watermarks: [],
        })
      } else {
        input.logger.warn(
          `Writer init failed for '${group.datasetKey.name}': ${formatErrorMessage(error)}`
        )
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
