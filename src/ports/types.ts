import { type Readable, type Writable } from 'node:stream'
import { type DatasetKey } from '../domain/dataset-key.js'
import { type Watermark } from '../domain/watermark.js'
import { type WatermarkKey } from '../domain/watermark-key.js'
import { type WatermarkStore } from '../domain/watermark-store.js'

export const SF_IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/
export const SOQL_RELATIONSHIP_PATH_PATTERN =
  /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/

export function formatErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'unknown error'
}

export interface WatermarkEntry {
  readonly key: WatermarkKey
  readonly watermark: Watermark
}

export interface ElfShape {
  readonly name?: string
  readonly sourceOrg: string
  readonly eventLog: string
  readonly interval: string
}

export interface SObjectShape {
  readonly name?: string
  readonly sourceOrg: string
  readonly sObject: string
}

export interface CsvShape {
  readonly name?: string
  readonly csvFile: string
}

export type EntryShape = ElfShape | SObjectShape | CsvShape

export function isElf<T extends EntryShape>(entry: T): entry is T & ElfShape {
  return 'eventLog' in entry
}

export function isSObject<T extends EntryShape>(
  entry: T
): entry is T & SObjectShape {
  return 'sObject' in entry
}

export function isCsv<T extends EntryShape>(entry: T): entry is T & CsvShape {
  return 'csvFile' in entry
}

export interface QueryResult<T> {
  totalSize: number
  done: boolean
  nextRecordsUrl?: string
  records: T[]
}

export type Operation = 'Append' | 'Overwrite'
export type BatchMiddleware = (batch: string[]) => string[]
export type ReaderKind = 'sobject' | 'elf' | 'csv'

export interface FetchResult {
  readonly lines: AsyncIterable<string[]>
  readonly watermark: () => Watermark | undefined
  readonly fileCount: () => number
}

export interface ReaderPort {
  fetch(watermark?: Watermark): Promise<FetchResult>
  header(): Promise<string>
  // Optional: SObject readers project rows into the dataset's column order
  // when supplied a layout. ELF / CSV readers do not implement this — their
  // column order is the source-of-truth (file/blob), checked at audit time.
  project?(layout: ProjectionLayout): void
}

export interface WriterResult {
  readonly parentId: string
  readonly partCount: number
}

export interface WriterInitResult {
  readonly chunker: Writable
  // Dataset metadata's canonical column order (CRMA target). Present only
  // for DatasetWriter; undefined for FileWriter. The pipeline uses this to
  // build a per-entry ProjectionLayout (each entry can have different
  // augment values — e.g. different sourceOrg augmentColumns).
  readonly datasetFields?: readonly string[]
}

export interface Writer {
  init(): Promise<WriterInitResult>
  finalize(): Promise<WriterResult>
  abort(): Promise<void>
  skip(): Promise<void>
}

export interface AlignmentSpec {
  readonly readerKind: ReaderKind
  readonly entryLabel: string
  // Source column names in source order. For SObject: dotted config fields
  // (translated to underscores by the projection builder). For ELF:
  // LogFileFieldNames split. For CSV: file header parsed via parseCsvHeader.
  readonly providedFields: readonly string[]
  readonly augmentColumns: Readonly<Record<string, string>>
}

export type AuditOutcome =
  | { readonly kind: 'pass' }
  | { readonly kind: 'warn'; readonly message: string }
  | { readonly kind: 'fail'; readonly message: string }

export class SkipDatasetError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SkipDatasetError'
  }
}

export interface ProjectionLayout {
  readonly targetSize: number
  readonly augmentSlots: ReadonlyArray<{ pos: number; quoted: string }>
  readonly outputIndex: Int32Array
}

export interface ProgressListener {
  onSinkReady(parentId: string): void
  onChunkWritten(): void
  onRowsWritten(count: number): void
}

export interface HeaderProvider {
  resolveHeader(): Promise<string>
}

export interface CreateWriterPort {
  create(
    dataset: DatasetKey,
    operation: Operation,
    listener: ProgressListener,
    headerProvider: HeaderProvider,
    alignment?: AlignmentSpec
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
