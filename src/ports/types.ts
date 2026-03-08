import { type Readable } from 'node:stream'
import { type DatasetKey } from '../domain/dataset-key.js'
import { type Watermark } from '../domain/watermark.js'
import { type WatermarkStore } from '../domain/watermark-store.js'

export interface QueryResult<T> {
  totalSize: number
  done: boolean
  nextRecordsUrl?: string
  records: T[]
}

export type EntryType = 'elf' | 'sobject'
export type Operation = 'Append' | 'Overwrite'

export interface FetchResult {
  readonly streams: AsyncIterable<Readable>
  readonly totalHint: number
  readonly watermark: () => Watermark | undefined
}

export interface FetchPort {
  fetch(watermark?: Watermark): Promise<FetchResult>
}

export interface UploadResult {
  readonly parentId: string
  readonly partIds: readonly string[]
}

export interface Uploader {
  write(csvLine: string): Promise<void>
  process(): Promise<UploadResult>
  abort(): Promise<void>
}

export interface CreateUploaderPort {
  create(dataset: DatasetKey, operation: Operation): Uploader
}

export interface SalesforcePort {
  readonly apiVersion: string
  query<T>(soql: string): Promise<QueryResult<T>>
  queryMore<T>(nextRecordsUrl: string): Promise<QueryResult<T>>
  getBlob(path: string): Promise<unknown>
  getBlobStream(path: string): Promise<Readable>
  post<T>(path: string, body: Record<string, unknown>): Promise<T>
  patch<T>(path: string, body: Record<string, unknown>): Promise<T>
  del(path: string): Promise<void>
}

export interface StatePort {
  read(): Promise<WatermarkStore>
  write(store: WatermarkStore): Promise<void>
}

export interface PhaseProgress {
  tick(detail: string): void
  stop(): void
}

export interface ProgressPort {
  create(label: string, total: number): PhaseProgress
}

export interface LoggerPort {
  info(message: string): void
  warn(message: string): void
  debug(message: string): void
}
