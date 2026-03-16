import { type Readable, type Writable } from 'node:stream'
import { type DatasetKey } from '../domain/dataset-key.js'
import { type Watermark } from '../domain/watermark.js'
import { type WatermarkKey } from '../domain/watermark-key.js'
import { type WatermarkStore } from '../domain/watermark-store.js'

export const SF_IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export function formatErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'unknown error'
}

export interface WatermarkEntry {
  readonly key: WatermarkKey
  readonly watermark: Watermark
}

export interface ElfShape {
  readonly name?: string
  readonly type: 'elf'
  readonly sourceOrg: string
  readonly eventType: string
  readonly interval: string
}

export interface SObjectShape {
  readonly name?: string
  readonly type: 'sobject'
  readonly sourceOrg: string
  readonly sobject: string
}

export type EntryShape = ElfShape | SObjectShape

export interface QueryResult<T> {
  totalSize: number
  done: boolean
  nextRecordsUrl?: string
  records: T[]
}

export type EntryType = 'elf' | 'sobject'
export type Operation = 'Append' | 'Overwrite'

export interface FetchResult {
  readonly lines: AsyncIterable<string>
  readonly watermark: () => Watermark | undefined
  readonly fileCount: () => number
}

export interface ReaderPort {
  fetch(watermark?: Watermark): Promise<FetchResult>
  header(): Promise<string>
}

export interface WriterResult {
  readonly parentId: string
  readonly partCount: number
}

export interface Writer {
  init(): Promise<Writable>
  finalize(): Promise<WriterResult>
  abort(): Promise<void>
  skip(): Promise<void>
}

export class SkipDatasetError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SkipDatasetError'
  }
}

export interface ProgressListener {
  onSinkReady(parentId: string): void
  onChunkWritten(): void
}

export interface HeaderProvider {
  resolveHeader(): Promise<string>
}

export interface CreateWriterPort {
  create(
    dataset: DatasetKey,
    operation: Operation,
    listener: ProgressListener,
    headerProvider: HeaderProvider
  ): Writer
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

export interface GroupTracker {
  updateParentId(parentId: string): void
  incrementParts(): void
  addFiles(count: number): void
  addRows(count: number): void
  stop(): void
}

export interface PhaseProgress {
  tick(detail: string): void
  trackGroup(label: string, withParts?: boolean): GroupTracker
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
