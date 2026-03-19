import { Writable } from 'node:stream'
import { createGzip, type Gzip } from 'node:zlib'
import { type DatasetKey } from '../domain/dataset-key.js'
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
} from '../ports/types.js'

interface CreateResponse {
  id: string
}

interface GzipChunkState {
  readonly gz: Gzip
  readonly chunks: Buffer[]
  compressedSize: number
  pendingBytes: number
}

interface CrmaMetadata {
  objects?: { numberOfLinesToIgnore?: number }[]
}

const PART_MAX_BYTES = 10 * 1024 * 1024
const FLUSH_THRESHOLD = 64 * 1024

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

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly parentId: string,
    private readonly listener?: ProgressListener
  ) {
    super({ objectMode: true })
    this.chunk = this.createChunkState()
  }

  get partCount(): number {
    return this.partNumber
  }

  override _write(
    line: string,
    _enc: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    if (this.gzError) {
      callback(this.gzError)
      return
    }

    const lineBytes = Buffer.byteLength(line + '\n')

    if (this.wouldExceed(lineBytes)) {
      this.chunk.gz.flush(() => {
        if (this.gzError) {
          callback(this.gzError)
          return
        }
        this.chunk.pendingBytes = 0
        if (this.wouldExceed(lineBytes)) {
          const finished = this.chunk
          endGzip(finished)
            .then(() => {
              const compressed = Buffer.concat(finished.chunks)
              this.uploadPromises.push(this.uploadPart(compressed))
              this.chunk = this.createChunkState()
              this.addLine(line, callback)
            })
            .catch((err: Error) => callback(err))
        } else {
          this.addLine(line, callback)
        }
      })
    } else {
      this.addLine(line, callback)
    }
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

  // Intentionally ignores failures — called during abort, parent will be deleted
  async drainUploads(): Promise<void> {
    await Promise.allSettled(this.uploadPromises)
  }

  private get hasData(): boolean {
    return this.chunk.compressedSize > 0 || this.chunk.pendingBytes > 0
  }

  private createChunkState(): GzipChunkState {
    const state: GzipChunkState = {
      gz: createGzip(),
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

  private addLine(
    line: string,
    callback: (error?: Error | null) => void
  ): void {
    const data = line + '\n'
    this.chunk.gz.write(data)
    this.chunk.pendingBytes += Buffer.byteLength(data)
    if (this.chunk.pendingBytes >= FLUSH_THRESHOLD) {
      this.chunk.gz.flush(() => {
        this.chunk.pendingBytes = 0
        callback()
      })
    } else {
      callback()
    }
  }

  private wouldExceed(additionalBytes: number): boolean {
    const estimatedSize =
      this.chunk.compressedSize + this.chunk.pendingBytes + additionalBytes
    return this.hasData && base64Length(estimatedSize) >= PART_MAX_BYTES
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

export class DatasetWriter implements Writer {
  private parentId: string | undefined
  private chunker: GzipChunkingWritable | undefined
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
    this.parentId = await this.createParent(patched)
    this.listener?.onSinkReady(this.parentId)
    this.chunker = new GzipChunkingWritable(
      this.sfPort,
      this.basePath,
      this.parentId,
      this.listener
    )
    return this.chunker
  }

  async finalize(): Promise<WriterResult> {
    if (!this.parentId || !this.chunker) {
      throw new Error('Not initialized')
    }
    await this.sfPort.patch(
      `${this.basePath}/InsightsExternalData/${this.parentId}`,
      {
        Action: 'Process',
        Mode: 'Incremental',
      }
    )
    return { parentId: this.parentId, partCount: this.chunker.partCount }
  }

  async abort(): Promise<void> {
    if (this.chunker) {
      await this.chunker.drainUploads()
    }
    if (this.parentId) {
      await this.sfPort.del(
        `${this.basePath}/InsightsExternalData/${this.parentId}`
      )
    }
  }

  async skip(): Promise<void> {
    return this.abort()
  }

  private normalizeMetadata(metadataJson: string): string {
    const meta: CrmaMetadata = JSON.parse(metadataJson)
    return JSON.stringify({
      ...meta,
      objects: meta.objects?.map(obj => ({
        ...obj,
        numberOfLinesToIgnore: 0,
      })),
    })
  }

  private async createParent(metadataJson: string): Promise<string> {
    const result = await this.sfPort.post<CreateResponse>(
      `${this.basePath}/InsightsExternalData`,
      {
        EdgemartAlias: this.datasetName,
        Format: 'Csv',
        Operation: this.operation,
        Action: 'None',
        MetadataJson: Buffer.from(metadataJson, 'utf-8').toString('base64'),
      }
    )
    return result.id
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
