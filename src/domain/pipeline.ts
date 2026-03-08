import { CsvStream } from '../adapters/csv-stream.js'
import {
  type CreateUploaderPort,
  type FetchPort,
  type LoggerPort,
  type Operation,
  type ProgressPort,
  type StatePort,
} from '../ports/types.js'
import { type DatasetKey } from './dataset-key.js'
import { type Watermark } from './watermark.js'
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
  readonly entries: PipelineEntry[]
}

interface GroupResult {
  processed: number
  skipped: number
  failed: number
  watermarks: { key: WatermarkKey; watermark: Watermark }[]
}

export async function executePipeline(
  input: PipelineInput
): Promise<PipelineResult> {
  const groups = groupByDataset(input.entries)
  const phase = input.progress.create('Processing', input.entries.length)

  let entriesProcessed = 0
  let entriesSkipped = 0
  let entriesFailed = 0
  let groupsUploaded = 0
  let store = input.watermarks

  const tasks = groups.map(group =>
    processDatasetGroup(group, input, phase).catch(
      (error: unknown): GroupResult => {
        input.logger.warn(
          `Dataset group failed: ${error instanceof Error ? error.message : 'unknown error'}`
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

  const results = await Promise.all(tasks)
  for (const { processed, skipped, failed, watermarks: wms } of results) {
    entriesProcessed += processed
    entriesSkipped += skipped
    entriesFailed += failed
    if (processed > 0) groupsUploaded++
    for (const { key, watermark } of wms) {
      store = store.set(key, watermark)
    }
  }

  phase.stop()
  await input.state.write(store)

  return {
    entriesProcessed,
    entriesSkipped,
    entriesFailed,
    groupsUploaded,
    exitCode: computeExitCode(entriesProcessed, entriesFailed),
  }
}

async function processDatasetGroup(
  group: DatasetGroup,
  input: PipelineInput,
  phase: { tick: (detail: string) => void }
): Promise<GroupResult> {
  const uploader = input.createUploader.create(
    group.datasetKey,
    group.operation
  )
  const csvStream = new CsvStream()
  let hasData = false
  let failed = 0
  let skipped = 0
  const watermarks: { key: WatermarkKey; watermark: Watermark }[] = []

  try {
    for (const entry of group.entries) {
      try {
        const watermark = input.watermarks.get(entry.watermarkKey)
        const fetchResult = await entry.fetcher.fetch(watermark)

        const hadHeaders = csvStream.headersEmitted
        let totalLines = 0
        for await (const csvLine of csvStream.transform(
          fetchResult.streams,
          entry.augmentColumns
        )) {
          await uploader.write(csvLine)
          totalLines++
        }

        const rowCount =
          !hadHeaders && totalLines > 0 ? totalLines - 1 : totalLines

        if (rowCount === 0) {
          phase.tick(
            `  [${entry.index}] ${entry.label} — skipped (no new records)`
          )
          skipped++
        } else {
          phase.tick(`  [${entry.index}] ${entry.label} — ${rowCount} rows`)
          hasData = true
          const wm = fetchResult.watermark()
          if (wm) {
            watermarks.push({ key: entry.watermarkKey, watermark: wm })
          }
        }
      } catch (error) {
        failed++
        input.logger.warn(
          `Fetch failed: ${error instanceof Error ? error.message : 'unknown error'}`
        )
      }
    }

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
      input.logger.debug(
        `InsightsExternalData created: ${uploadResult.parentId}`
      )
    } else {
      await uploader.abort()
    }
  } catch (error) {
    await uploader.abort().catch((abortError: unknown) => {
      input.logger.debug(
        `sink.abort() failed: ${abortError instanceof Error ? abortError.message : abortError}`
      )
    })
    throw error
  }

  return {
    processed: hasData ? group.entries.length - skipped - failed : 0,
    skipped,
    failed: 0,
    watermarks: hasData ? watermarks : [],
  }
}

function groupByDataset(entries: readonly PipelineEntry[]): DatasetGroup[] {
  const map = new Map<string, DatasetGroup>()
  for (const entry of entries) {
    const key = entry.datasetKey.toString()
    const existing = map.get(key)
    if (existing) {
      existing.entries.push(entry)
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

function computeExitCode(processed: number, failed: number): number {
  if (failed === 0) return 0
  if (processed > 0) return 1
  return 2
}
