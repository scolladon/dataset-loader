import { Writable } from 'node:stream'
import { constants, createGzip, type Gzip } from 'node:zlib'
import { type DatasetKey } from '../../domain/dataset-key.js'
import { synthesise } from '../../domain/metadata-synthesizer.js'
import { checkSchemaAlignment } from '../../domain/schema-check.js'
import { buildSObjectRowProjection } from '../../domain/sobject-row-projection.js'
import {
  type AlignmentSpec,
  type BootstrapMetadataProvider,
  type BootstrapSpec,
  type CreateWriterPort,
  type HeaderProvider,
  type LoggerPort,
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

const BASE64_GROUP_CHARS = 4
const BASE64_GROUP_BYTES = 3
const NEWLINE = 0x0a
const PART_ENTITY_FIELD = 'entity_part'
const PART_FILE_FIELD = 'DataFile'
const PART_FILE_NAME = 'part.gz'

// Capacity kept at the base64-era budget (7,864,317 raw bytes) although parts now travel as binary.
function partCapacity(partMaxBytes: number): number {
  return (
    Math.floor((partMaxBytes - 1) / BASE64_GROUP_CHARS) * BASE64_GROUP_BYTES
  )
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
  private readonly capacity: number

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly parentId: string,
    private readonly listener?: ProgressListener,
    private readonly uploadHighWater = UPLOAD_HIGH_WATER,
    partMaxBytes = PART_MAX_BYTES
  ) {
    super({ objectMode: true })
    this.capacity = partCapacity(partMaxBytes)
    this.chunk = this.createChunkState()
  }

  get partCount(): number {
    return this.partNumber
  }

  // Pessimistic: pending bytes are counted as if they did not compress at all.
  private get room(): number {
    return this.capacity - this.chunk.compressedSize - this.chunk.pendingBytes
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
    if (batch.length === 0) {
      callback()
      return
    }
    // One Buffer per batch avoids a UTF-8 length scan and zlib's own
    // string-to-Buffer conversion on the hot path.
    const encoded = Buffer.from(batch.join('\n') + '\n')
    if (encoded.length <= this.room) {
      this.append(encoded)
      callback()
      return
    }
    this.writeAcrossBoundary(encoded).then(() => callback(), callback)
  }

  // Cuts at the last newline byte that fits, so a quoted multi-line SObject
  // value may span two parts.
  private async writeAcrossBoundary(encoded: Buffer): Promise<void> {
    let rest = encoded
    while (rest.length > 0) {
      await this.syncFlush()
      const cut = this.fittingPrefix(rest)
      if (cut === 0) {
        await this.rotatePart()
        continue
      }
      this.append(rest.subarray(0, cut))
      rest = rest.subarray(cut)
    }
  }

  // Buffers always end with a newline, so one that fits whole is cut at its
  // own end. An empty part takes up to the first newline, so an oversized line
  // cannot rotate forever.
  private fittingPrefix(buffer: Buffer): number {
    const room = this.room
    const lastBreak = room > 0 ? buffer.lastIndexOf(NEWLINE, room - 1) : -1
    if (lastBreak >= 0) return lastBreak + 1
    return this.hasData ? 0 : buffer.indexOf(NEWLINE) + 1
  }

  private syncFlush(): Promise<void> {
    if (this.chunk.pendingBytes === 0) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      this.chunk.gz.flush(constants.Z_SYNC_FLUSH, (error?: Error | null) => {
        if (error) {
          reject(error)
          return
        }
        this.chunk.pendingBytes = 0
        resolve()
      })
    })
  }

  private async rotatePart(): Promise<void> {
    const finished = this.chunk
    await endGzip(finished)
    this.trackUpload(this.uploadPart(Buffer.concat(finished.chunks)))
    if (this.pendingUploads.size >= this.uploadHighWater) {
      await Promise.race([...this.pendingUploads])
    }
    this.chunk = this.createChunkState()
  }

  private append(buffer: Buffer): void {
    this.chunk.gz.write(buffer)
    this.chunk.pendingBytes += buffer.length
  }

  private trackUpload(upload: Promise<void>): void {
    // Auto-remove from pending set on settle; suppress unhandled rejection
    // until _final's Promise.all catches it
    upload
      .finally(() => this.pendingUploads.delete(upload))
      .catch(() => {
        // Rejection handled by Promise.all in _final
      })
    this.pendingUploads.add(upload)
    this.uploadPromises.push(upload)
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

  private uploadPart(compressed: Buffer): Promise<void> {
    const partNumber = ++this.partNumber
    return this.sfPort
      .postMultipart<CreateResponse>(
        `${this.basePath}/InsightsExternalDataPart`,
        {
          entityField: PART_ENTITY_FIELD,
          entity: {
            InsightsExternalDataId: this.parentId,
            PartNumber: partNumber,
          },
          fileField: PART_FILE_FIELD,
          fileName: PART_FILE_NAME,
          data: compressed,
        }
      )
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
    private readonly alignment?: AlignmentSpec,
    private readonly bootstrapProvider?: BootstrapMetadataProvider,
    private readonly overrideMetadata: boolean = false,
    private readonly logger?: LoggerPort
  ) {
    this.basePath = `/services/data/v${sfPort.apiVersion}/sobjects`
    this.datasetName = dataset.name
    if (!SF_IDENTIFIER_PATTERN.test(this.datasetName)) {
      throw new Error(`Invalid dataset name: '${this.datasetName}'`)
    }
  }

  async init(): Promise<WriterInitResult> {
    const resolved = await this.resolveMetadata()
    const { patched, parsed } = this.parseMetadata(resolved.json)
    if (resolved.source === 'synthesised') {
      this.logger?.info(
        `Bootstrapping new dataset '${this.datasetName}' (synthesised metadata)`
      )
      this.logger?.debug(
        `Synthesised MetadataJson for '${this.datasetName}': ${resolved.json}`
      )
    }
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

  // Strategy chain: prefer existing metadata when present and overrideMetadata
  // is not set, otherwise synthesise from the bootstrap provider. With
  // bootstrap always-on (per design §Writer integration), the only path that
  // throws SkipDatasetError is "no existing AND no provider" — which happens
  // when the caller (factory) declined to construct a provider (e.g. unknown
  // entry kind). That's a programming error, not a user error.
  private async resolveMetadata(): Promise<{
    json: string
    source: 'existing' | 'synthesised'
  }> {
    if (!this.overrideMetadata) {
      const existing = await this.queryExistingMetadata()
      if (existing) return { json: existing, source: 'existing' }
    }
    if (!this.bootstrapProvider) {
      throw new SkipDatasetError(
        `No existing metadata for dataset '${this.datasetName}' and no bootstrap provider configured`
      )
    }
    const schema = await this.bootstrapProvider.buildSourceSchema()
    return { json: synthesise(schema), source: 'synthesised' }
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
  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly logger?: LoggerPort
  ) {}

  create(
    dataset: DatasetKey,
    operation: Operation,
    listener: ProgressListener,
    _headerProvider: HeaderProvider,
    alignment?: AlignmentSpec,
    bootstrap?: BootstrapSpec
  ): Writer {
    return new DatasetWriter(
      this.sfPort,
      dataset,
      operation,
      listener,
      alignment,
      bootstrap?.provider,
      bootstrap?.overrideMetadata ?? false,
      this.logger
    )
  }
}
