import { CsvStream } from '../adapters/csv-stream.js'
import {
  type CreateUploaderPort,
  type FetchPort,
  formatErrorMessage,
  type GroupTracker,
  type LoggerPort,
  type Operation,
  type PhaseProgress,
  type ProgressPort,
  type StatePort,
  type Uploader,
  type UploadListener,
  type WatermarkEntry,
} from '../ports/types.js'
import { type DatasetKey } from './dataset-key.js'
import { type WatermarkKey } from './watermark-key.js'
import { type WatermarkStore } from './watermark-store.js'

export interface PipelineEntry {
  readonly index: number
  readonly label: string
  readonly watermarkKey: WatermarkKey
  readonly datasetKey: DatasetKey
  readonly operation: Operation
  readonly augmentColumns: Record<string, string>
  readonly fetcher: FetchPort
}

export interface PipelineInput {
  readonly entries: readonly PipelineEntry[]
  readonly watermarks: WatermarkStore
  readonly createUploader: CreateUploaderPort
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

interface DatasetGroup {
  readonly key: string
  readonly datasetKey: DatasetKey
  readonly operation: Operation
  readonly entries: readonly PipelineEntry[]
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

export async function executePipeline(
  input: PipelineInput
): Promise<PipelineResult> {
  const groups = groupByDataset(input.entries)
  const phase = input.progress.create('Processing', input.entries.length)

  const tasks: Promise<GroupResult>[] = []
  for (const group of groups) {
    tasks.push(
      processDatasetGroup(group, input, phase).catch(
        (error: unknown): GroupResult => {
          input.logger.warn(
            `Dataset group failed: ${formatErrorMessage(error)}`
          )
          return {
            processed: 0,
            skipped: 0,
            failed: group.entries.length,
            watermarks: [],
          }
        }
      )
    )
  }

  const results = await Promise.all(tasks)
  const { store, pipelineResult } = aggregateResults(results, input.watermarks)

  phase.stop()
  await input.state.write(store)

  return pipelineResult
}

async function processDatasetGroup(
  group: DatasetGroup,
  input: PipelineInput,
  phase: Pick<PhaseProgress, 'tick' | 'trackGroup'>
): Promise<GroupResult> {
  const tracker = phase.trackGroup(group.datasetKey.name)
  const listener: UploadListener = {
    onParentCreated: (parentId: string) => tracker.updateParentId(parentId),
    onPartUploaded: () => tracker.incrementParts(),
  }
  const uploader = input.createUploader.create(
    group.datasetKey,
    group.operation,
    listener
  )
  const csvStream = new CsvStream()

  try {
    const entryResults = await processEntries(
      group.entries,
      input,
      phase,
      csvStream,
      uploader,
      tracker
    )
    return await finalizeGroup(group, entryResults, uploader, input.logger)
  } catch (error) {
    await uploader.abort().catch((abortError: unknown) => {
      input.logger.debug(
        `sink.abort() failed: ${formatErrorMessage(abortError)}`
      )
    })
    throw error
  } finally {
    tracker.stop()
  }
}

async function processEntries(
  entries: readonly PipelineEntry[],
  input: PipelineInput,
  phase: { tick: (detail: string) => void },
  csvStream: CsvStream,
  uploader: Uploader,
  tracker: Pick<GroupTracker, 'addFiles' | 'addRows'>
): Promise<readonly EntryResult[]> {
  const results: EntryResult[] = []
  for (const entry of entries) {
    results.push(
      await processEntry(entry, input, phase, csvStream, uploader, tracker)
    )
  }
  return results
}

async function processEntry(
  entry: PipelineEntry,
  input: PipelineInput,
  phase: { tick: (detail: string) => void },
  csvStream: CsvStream,
  uploader: Uploader,
  tracker: Pick<GroupTracker, 'addFiles' | 'addRows'>
): Promise<EntryResult> {
  try {
    const watermark = input.watermarks.get(entry.watermarkKey)
    const fetchResult = await entry.fetcher.fetch(watermark)

    const hadHeaders = csvStream.headersEmitted
    let totalLines = 0
    const countedStreams = countStreams(fetchResult.streams, () => {
      tracker.addFiles(1)
    })
    let isFirstLine = !hadHeaders
    for await (const csvLine of csvStream.transform(
      countedStreams,
      entry.augmentColumns
    )) {
      await uploader.write(csvLine)
      totalLines++
      if (isFirstLine) {
        isFirstLine = false
      } else {
        tracker.addRows(1)
      }
    }

    const rowCount = computeDataRowCount(hadHeaders, totalLines)

    if (rowCount === 0) {
      phase.tick(`  [${entry.index}] ${entry.label} — skipped (no new records)`)
      return { status: 'skipped' }
    }

    phase.tick(`  [${entry.index}] ${entry.label} — ${rowCount} rows`)
    const wm = fetchResult.watermark()
    return {
      status: 'processed',
      watermark: wm ? { key: entry.watermarkKey, watermark: wm } : undefined,
    }
  } catch (error) {
    input.logger.warn(`Fetch failed: ${formatErrorMessage(error)}`)
    return { status: 'failed' }
  }
}

async function finalizeGroup(
  group: DatasetGroup,
  entryResults: readonly EntryResult[],
  uploader: Uploader,
  logger: LoggerPort
): Promise<GroupResult> {
  const skipped = entryResults.filter(r => r.status === 'skipped').length
  const failed = entryResults.filter(r => r.status === 'failed').length
  const hasData = entryResults.some(r => r.status === 'processed')

  if (failed > 0) {
    await uploader.abort()
    return {
      processed: 0,
      skipped,
      failed: group.entries.length - skipped,
      watermarks: [],
    }
  }

  if (hasData) {
    const uploadResult = await uploader.process()
    logger.debug(`InsightsExternalData created: ${uploadResult.parentId}`)
  } else {
    await uploader.abort()
  }

  const watermarks = hasData
    ? entryResults.flatMap(r => (r.watermark ? [r.watermark] : []))
    : []

  return {
    processed: hasData ? group.entries.length - skipped - failed : 0,
    skipped,
    failed: 0,
    watermarks,
  }
}

function computeDataRowCount(hadHeaders: boolean, totalLines: number): number {
  return !hadHeaders && totalLines > 0 ? totalLines - 1 : totalLines
}

function aggregateResults(
  results: readonly GroupResult[],
  initialStore: WatermarkStore
): { readonly store: WatermarkStore; readonly pipelineResult: PipelineResult } {
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
      map.set(key, { ...existing, entries: [...existing.entries, entry] })
    } else {
      map.set(key, {
        key,
        datasetKey: entry.datasetKey,
        operation: entry.operation,
        entries: [entry],
      })
    }
  }
  return [...map.values()]
}

async function* countStreams<T>(
  source: AsyncIterable<T>,
  onItem: () => void
): AsyncGenerator<T> {
  for await (const item of source) {
    onItem()
    yield item
  }
}

function computeExitCode(processed: number, failed: number): number {
  if (failed === 0) return 0
  if (processed > 0) return 1
  return 2
}
