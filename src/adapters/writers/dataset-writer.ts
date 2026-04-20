import { Writable } from 'node:stream'
import { createGzip, type Gzip } from 'node:zlib'
import { type DatasetKey } from '../../domain/dataset-key.js'
import { checkSchemaAlignment } from '../../domain/schema-check.js'
import { buildSObjectRowProjection } from '../../domain/sobject-row-projection.js'
import {
  type AlignmentSpec,
  type CreateWriterPort,
  type HeaderProvider,
  type Operation,
  type ProgressListener,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  SkipDatasetError,
  type Writer,
  type WriterInitResult,
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

interface DatasetMetadataField {
  fullyQualifiedName?: string
}

interface DatasetMetadataObject {
  numberOfLinesToIgnore?: number
  fields?: DatasetMetadataField[]
}

interface DatasetMetadata {
  objects?: DatasetMetadataObject[]
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
      if (!this.exceedsPartLimit(wholeBytes)) {
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

  // Pessimistic projection: `compressed + pending + additional` is the
  // worst-case size assuming zlib produced zero compression. Monotonic in the
  // input bytes, so if this doesn't exceed partMaxBytes (after base64) the
  // actual emitted part can't either.
  private exceedsPartLimit(additionalBytes: number): boolean {
    const estimatedSize =
      this.chunk.compressedSize + this.chunk.pendingBytes + additionalBytes
    return base64Length(estimatedSize) >= this.partMaxBytes
  }

  // Slow-path predicate: same projection, plus a guard that says "never rotate
  // an empty part" — otherwise an oversized first line would spin forever
  // rotating through empty parts.
  private wouldExceed(additionalBytes: number): boolean {
    return this.hasData && this.exceedsPartLimit(additionalBytes)
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
    private readonly listener?: ProgressListener,
    private readonly alignment?: AlignmentSpec
  ) {
    this.basePath = `/services/data/v${sfPort.apiVersion}/sobjects`
    this.datasetName = dataset.name
    if (!SF_IDENTIFIER_PATTERN.test(this.datasetName)) {
      throw new Error(`Invalid dataset name: '${this.datasetName}'`)
    }
  }

  async init(): Promise<WriterInitResult> {
    const metadata = await this.queryExistingMetadata()
    if (!metadata) {
      throw new SkipDatasetError(
        `No existing metadata for dataset '${this.datasetName}', skipping`
      )
    }
    const { patched, parsed } = this.parseMetadata(metadata)
    // Dataset fields are extracted lazily — only when alignment actually
    // requires them (SObject always; ELF/CSV only with non-empty
    // providedFields). Legacy metadata without a `fields` array is still
    // accepted for the no-alignment and empty-providedFields paths.
    const datasetFields = this.alignment
      ? this.validateAlignment(parsed, this.alignment)
      : undefined
    this.lazyWritable = new LazyGzipChunkingWritable(
      this.sfPort,
      this.basePath,
      this.datasetName,
      this.operation,
      patched,
      this.listener
    )
    return { chunker: this.lazyWritable, datasetFields }
  }

  private validateAlignment(
    parsed: DatasetMetadata,
    alignment: AlignmentSpec
  ): readonly string[] | undefined {
    if (alignment.readerKind === 'sobject') {
      // SObject always needs dataset fields — pipeline rebuilds the per-entry
      // layout via buildSObjectRowProjection.
      const datasetFields = this.extractDatasetFields(parsed)
      buildSObjectRowProjection({
        datasetName: this.datasetName,
        entryLabel: alignment.entryLabel,
        readerFields: alignment.providedFields,
        augmentColumns: alignment.augmentColumns,
        datasetFields,
      })
      return datasetFields
    }
    // ELF/CSV: reject augment-vs-reader overlap before the order check so the
    // user sees a precise diagnostic instead of a generic set mismatch.
    this.rejectAugmentOverlap(alignment)
    if (alignment.providedFields.length === 0) {
      // ELF with no prior log file, or empty CSV header — schema check is
      // not actionable here; the audit's WARN is the authoritative signal.
      // Legacy datasets without a `fields` metadata array are accepted on
      // this path.
      return undefined
    }
    const datasetFields = this.extractDatasetFields(parsed)
    const provided = [
      ...alignment.providedFields,
      ...Object.keys(alignment.augmentColumns),
    ]
    const result = checkSchemaAlignment({
      datasetName: this.datasetName,
      entryLabel: alignment.entryLabel,
      expected: datasetFields,
      provided,
      checkOrder: true,
    })
    if (!result.ok) throw new SkipDatasetError(result.reason)
    return datasetFields
  }

  private rejectAugmentOverlap(alignment: AlignmentSpec): void {
    const provided = new Set(
      alignment.providedFields.map(f => f.replace(/\./g, '_').toLowerCase())
    )
    const overlap = Object.keys(alignment.augmentColumns).filter(k =>
      provided.has(k.replace(/\./g, '_').toLowerCase())
    )
    if (overlap.length === 0) return
    throw new SkipDatasetError(
      `Schema overlap for dataset '${this.datasetName}' (entry '${alignment.entryLabel}'):\n` +
        `  augment columns also present as reader fields: [${overlap.join(', ')}]`
    )
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

  private parseMetadata(metadataJson: string): {
    patched: string
    parsed: DatasetMetadata
  } {
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
      return {
        patched: JSON.stringify({ ...meta, objects }),
        parsed: meta,
      }
    } catch (err: unknown) {
      /* v8 ignore next -- JSON.parse and our own guards always throw Error */
      const message = err instanceof Error ? err.message : String(err)
      throw new Error(
        `Failed to parse metadata for dataset '${this.datasetName}': ${message}`
      )
    }
  }

  private extractDatasetFields(meta: DatasetMetadata): readonly string[] {
    const obj0 = meta.objects?.[0]
    if (!obj0 || !Array.isArray(obj0.fields) || obj0.fields.length === 0) {
      throw new SkipDatasetError(
        `Dataset '${this.datasetName}' metadata has no objects[0].fields; cannot enforce column alignment`
      )
    }
    const names: string[] = []
    for (const field of obj0.fields) {
      const name = field.fullyQualifiedName
      if (typeof name !== 'string' || name.length === 0) {
        throw new SkipDatasetError(
          `Dataset '${this.datasetName}' metadata has fields without fullyQualifiedName`
        )
      }
      names.push(name)
    }
    return names
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
    _headerProvider: HeaderProvider,
    alignment?: AlignmentSpec
  ): Writer {
    return new DatasetWriter(
      this.sfPort,
      dataset,
      operation,
      listener,
      alignment
    )
  }
}
