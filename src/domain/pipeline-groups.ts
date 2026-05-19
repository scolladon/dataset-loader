import {
  type AlignmentSpec,
  type BootstrapSpec,
  type HeaderProvider,
  type Operation,
  type ReaderPort,
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
  readonly alignment: AlignmentSpec
  // Per-entry bootstrap wiring. Optional only because legacy tests construct
  // PipelineEntry literals without it; runtime always sets it for dataset
  // targets (file targets are unaffected — FileWriter ignores the spec).
  readonly bootstrap?: BootstrapSpec
  header(): Promise<string>
}

export interface ReaderBundle {
  readonly readerKey: ReaderKey
  readonly watermark: Watermark | undefined
  readonly entries: readonly PipelineEntry[]
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

export function groupByDataset(
  entries: readonly PipelineEntry[]
): DatasetGroup[] {
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
