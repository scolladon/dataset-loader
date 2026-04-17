import { Writable } from 'node:stream'
import { constants, createGzip, type Gzip } from 'node:zlib'
import { type DatasetKey } from '../../domain/dataset-key.js'
import {
  type CreateWriterPort,
  type HeaderProvider,
  type Operation,
  type ProgressListener,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  SkipDatasetError,
  type Writer,
  type WriterResult,
} from '../../ports/types.js'
import { DEFAULT_CONCURRENCY } from '../sf-client.js'

interface CreateResponse {
  id: string
}

interface GzipChunkState {
  readonly gz: Gzip
  readonly chunks: Buffer[]
  compressedSize: number
  pendingBytes: number
}

interface DatasetMetadata {
  objects?: { numberOfLinesToIgnore?: number }[]
}

const PART_MAX_BYTES = 10 * 1024 * 1024
export const UPLOAD_HIGH_WATER = DEFAULT_CONCURRENCY

function base64Length(byteCount: number): number {
  return Math.ceil(byteCount / 3) * 4
}

// Wait for gzip readable side to finish emitting all data.
// gz.end(callback) fires on 'finish' (writable done) which can precede
// the final 'data' events from the readable side, producing truncated output.
function endGzip(state: GzipChunkState): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    state.gz.on('end', resolve)
    state.gz.on('error', reject)
    state.gz.end()
  })
}

export class GzipChunkingWritable extends Writable {
  private chunk: GzipChunkState
  private gzError: Error | undefined
  private partNumber = 0
  private readonly uploadPromises: Promise<void>[] = []
  private readonly pendingUploads = new Set<Promise<void>>()

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly parentId: string,
    private readonly listener?: ProgressListener,
    private readonly uploadHighWater = UPLOAD_HIGH_WATER,
    private readonly partMaxBytes = PART_MAX_BYTES
  ) {
    super({ objectMode: true })
    this.chunk = this.createChunkState()
  }

  get partCount(): number {
    return this.partNumber
  }

  override _write(
    batch: string[],
    _enc: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    /* v8 ignore next 4 -- gzip stream errors are async; this guards against a prior error */
    if (this.gzError) {
      callback(this.gzError)
      return
    }
    this.listener?.onRowsWritten(batch.length)
    // Fast path: if the whole batch fits in the current part, encode to a
    // Buffer once and hand that single chunk to zlib. Using Buffer (not a
    // string) avoids both (a) the extra Buffer.byteLength UTF-8 scan and
    // (b) zlib's internal string→Buffer conversion on the hot path. Falls
    // through to the per-line path when the batch would span a 10 MB part
    // boundary so rotation stays line-precise.
    if (batch.length > 0) {
      const whole = Buffer.from(batch.join('\n') + '\n', 'utf8')
      const wholeBytes = whole.length
      const projected =
        this.chunk.compressedSize + this.chunk.pendingBytes + wholeBytes
      if (base64Length(projected) < this.partMaxBytes) {
        this.chunk.gz.write(whole)
        this.chunk.pendingBytes += wholeBytes
        callback()
        return
      }
    }
    // Slow path: batch straddles a 10 MB part boundary — walk line-by-line.
    this.writeBatch(batch, 0, callback)
  }

  private writeBatch(
    batch: string[],
    i: number,
    callback: (error?: Error | null) => void
  ): void {
    while (i < batch.length) {
      /* v8 ignore next 4 -- gzip errors are asynchronous; checked before each line as a guard */
      if (this.gzError) {
        callback(this.gzError)
        return
      }
      const line = batch[i++]
      const data = line + '\n'
      const lineBytes = Buffer.byteLength(data)
      if (this.wouldExceed(lineBytes)) {
        this.rotateIfNeeded(line, lineBytes, batch, i, callback)
        return
      }
      this.chunk.gz.write(data)
      this.chunk.pendingBytes += lineBytes
    }
    callback()
  }

  private rotateIfNeeded(
    line: string,
    lineBytes: number,
    batch: string[],
    i: number,
    callback: (error?: Error | null) => void
  ): void {
    this.chunk.gz.flush(() => {
      /* v8 ignore next 4 -- gzip errors are asynchronous; checked post-flush as a guard */
      if (this.gzError) {
        callback(this.gzError)
        return
      }
      this.chunk.pendingBytes = 0
      if (!this.wouldExceed(lineBytes)) {
        this.writeLineToGz(line, lineBytes)
        this.writeBatch(batch, i, callback)
        return
      }
      this.finishAndRotate(line, lineBytes, batch, i, callback)
    })
  }

  private finishAndRotate(
    line: string,
    lineBytes: number,
    batch: string[],
    i: number,
    callback: (error?: Error | null) => void
  ): void {
    const finished = this.chunk
    endGzip(finished)
      .then(async () => {
        const compressed = Buffer.concat(finished.chunks)
        const upload = this.uploadPart(compressed)
        // Auto-remove from pending set on settle; suppress unhandled rejection
        // until _final's Promise.all catches it
        upload
          .finally(() => this.pendingUploads.delete(upload))
          .catch(() => {
            // Rejection handled by Promise.all in _final
          })
        this.pendingUploads.add(upload)
        this.uploadPromises.push(upload)
        if (this.pendingUploads.size >= this.uploadHighWater) {
          await Promise.race([...this.pendingUploads])
        }
        this.chunk = this.createChunkState()
        this.writeLineToGz(line, lineBytes)
        this.writeBatch(batch, i, callback)
      })
      .catch((err: Error) => callback(err))
  }

  private writeLineToGz(line: string, lineBytes: number): void {
    this.chunk.gz.write(line + '\n')
    this.chunk.pendingBytes += lineBytes
  }

  override _final(callback: (error?: Error | null) => void): void {
    const hadData = this.hasData
    endGzip(this.chunk)
      .then(() => {
        if (hadData) {
          const compressed = Buffer.concat(this.chunk.chunks)
          this.uploadPromises.push(this.uploadPart(compressed))
        }
        return Promise.all(this.uploadPromises)
      })
      .then(() => callback(), callback)
  }

  // Intentionally ignores failures — called during abort (parent will be deleted)
  // and during finalize (to ensure all parts are settled before Action:Process)
  async drainUploads(): Promise<void> {
    await Promise.allSettled(this.uploadPromises)
  }

  private get hasData(): boolean {
    return this.chunk.compressedSize > 0 || this.chunk.pendingBytes > 0
  }

  private createChunkState(): GzipChunkState {
    const state: GzipChunkState = {
      gz: createGzip({ level: 3 }),
      chunks: [],
      compressedSize: 0,
      pendingBytes: 0,
    }
    state.gz.on('data', (c: Buffer) => {
      state.chunks.push(c)
      state.compressedSize += c.length
    })
    state.gz.on('error', (err: Error) => {
      this.gzError = err
      this.destroy(err)
    })
    return state
  }

  private wouldExceed(additionalBytes: number): boolean {
    const estimatedSize =
      this.chunk.compressedSize + this.chunk.pendingBytes + additionalBytes
    return this.hasData && base64Length(estimatedSize) >= this.partMaxBytes
  }

  private uploadPart(compressed: Buffer): Promise<void> {
    const pn = ++this.partNumber
    return this.sfPort
      .post<CreateResponse>(`${this.basePath}/InsightsExternalDataPart`, {
        InsightsExternalDataId: this.parentId,
        PartNumber: pn,
        DataFile: compressed.toString('base64'),
      })
      .then(() => {
        this.listener?.onChunkWritten()
      })
  }
}

export class LazyGzipChunkingWritable extends Writable {
  private _chunker?: GzipChunkingWritable
  private _parentId?: string

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly datasetName: string,
    private readonly operation: Operation,
    private readonly metadataJson: string,
    private readonly listener?: ProgressListener
  ) {
    super({ objectMode: true })
  }

  get parentId(): string | undefined {
    return this._parentId
  }

  get partCount(): number {
    /* v8 ignore next -- _chunker is always set when parentId is set; defensive default */
    return this._chunker?.partCount ?? 0
  }

  async drainUploads(): Promise<void> {
    await this._chunker?.drainUploads()
  }

  override _write(
    batch: string[],
    _enc: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    if (this._chunker) {
      this._chunker.write(batch, callback)
      return
    }
    this.createChunker()
      .then(() => this._chunker!.write(batch, callback))
      .catch(callback)
  }

  override _final(callback: (error?: Error | null) => void): void {
    if (!this._chunker) {
      callback()
      return
    }
    this._chunker.once('finish', () => callback())
    this._chunker.once('error', callback)
    this._chunker.end()
  }

  private async createChunker(): Promise<void> {
    const result = await this.sfPort.post<CreateResponse>(
      `${this.basePath}/InsightsExternalData`,
      {
        EdgemartAlias: this.datasetName,
        Format: 'Csv',
        Operation: this.operation,
        Action: 'None',
        MetadataJson: Buffer.from(this.metadataJson, 'utf-8').toString(
          'base64'
        ),
      }
    )
    this._parentId = result.id
    this.listener?.onSinkReady(this._parentId)
    this._chunker = new GzipChunkingWritable(
      this.sfPort,
      this.basePath,
      this._parentId,
      this.listener
    )
  }
}

export class DatasetWriter implements Writer {
  private lazyWritable?: LazyGzipChunkingWritable
  private readonly basePath: string
  private readonly datasetName: string

  constructor(
    private readonly sfPort: SalesforcePort,
    dataset: DatasetKey,
    private readonly operation: Operation,
    private readonly listener?: ProgressListener
  ) {
    this.basePath = `/services/data/v${sfPort.apiVersion}/sobjects`
    this.datasetName = dataset.name
    if (!SF_IDENTIFIER_PATTERN.test(this.datasetName)) {
      throw new Error(`Invalid dataset name: '${this.datasetName}'`)
    }
  }

  async init(): Promise<Writable> {
    const metadata = await this.queryExistingMetadata()
    if (!metadata) {
      throw new SkipDatasetError(
        `No existing metadata for dataset '${this.datasetName}', skipping`
      )
    }
    const patched = this.normalizeMetadata(metadata)
    this.lazyWritable = new LazyGzipChunkingWritable(
      this.sfPort,
      this.basePath,
      this.datasetName,
      this.operation,
      patched,
      this.listener
    )
    return this.lazyWritable
  }

  async finalize(): Promise<WriterResult> {
    if (!this.lazyWritable) {
      throw new Error('Not initialized')
    }
    if (!this.lazyWritable.parentId) {
      return { parentId: '', partCount: 0 }
    }
    await this.lazyWritable.drainUploads()
    await this.sfPort.patch(
      `${this.basePath}/InsightsExternalData/${this.lazyWritable.parentId}`,
      {
        Action: 'Process',
        Mode: 'Incremental',
      }
    )
    return {
      parentId: this.lazyWritable.parentId,
      partCount: this.lazyWritable.partCount,
    }
  }

  async abort(): Promise<void> {
    if (!this.lazyWritable?.parentId) return
    await this.lazyWritable.drainUploads()
    await this.sfPort.del(
      `${this.basePath}/InsightsExternalData/${this.lazyWritable.parentId}`
    )
  }

  async skip(): Promise<void> {
    return this.abort()
  }

  private normalizeMetadata(metadataJson: string): string {
    try {
      const parsed: unknown = JSON.parse(metadataJson)
      if (
        parsed === null ||
        typeof parsed !== 'object' ||
        Array.isArray(parsed)
      ) {
        throw new Error('metadata root must be an object')
      }
      const meta = parsed as DatasetMetadata
      if (meta.objects !== undefined && !Array.isArray(meta.objects)) {
        throw new Error('metadata.objects must be an array')
      }
      const objects = meta.objects?.map(obj => ({
        ...obj,
        numberOfLinesToIgnore: 0,
      }))
      return JSON.stringify({ ...meta, objects })
    } catch (err: unknown) {
      /* v8 ignore next -- JSON.parse and our own guards always throw Error */
      const message = err instanceof Error ? err.message : String(err)
      throw new Error(
        `Failed to parse metadata for dataset '${this.datasetName}': ${message}`
      )
    }
  }

  private async queryExistingMetadata(): Promise<string | null> {
    const result = await this.sfPort.query<{
      MetadataJson: string | null
    }>(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${this.datasetName}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    if (result.records.length === 0 || !result.records[0].MetadataJson) {
      return null
    }
    const blob = await this.sfPort.getBlob(result.records[0].MetadataJson)
    return typeof blob === 'string' ? blob : JSON.stringify(blob)
  }
}

export class DatasetWriterFactory implements CreateWriterPort {
  constructor(private readonly sfPort: SalesforcePort) {}

  create(
    dataset: DatasetKey,
    operation: Operation,
    listener: ProgressListener,
    _headerProvider: HeaderProvider
  ): Writer {
    return new DatasetWriter(this.sfPort, dataset, operation, listener)
  }
}
