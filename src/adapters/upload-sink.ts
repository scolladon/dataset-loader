import { createGzip, type Gzip } from 'node:zlib'
import { parse } from 'csv-parse/sync'
import { type DatasetKey } from '../domain/dataset-key.js'
import {
  type CreateUploaderPort,
  type Operation,
  type QueryResult,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  type Uploader,
  type UploadListener,
  type UploadResult,
} from '../ports/types.js'

interface InsightsExternalData {
  Id: string
  MetadataJson: string | null
}

interface CreateResponse {
  id: string
}

const PART_MAX_BYTES = 10 * 1024 * 1024

function base64Length(byteCount: number): number {
  return Math.ceil(byteCount / 3) * 4
}

function generateMetadataJson(headers: readonly string[]): string {
  const fields = headers.map(name => ({
    fullyQualifiedName: name,
    name,
    type: 'Text',
    label: name,
  }))
  return JSON.stringify({
    fileFormat: {
      charsetName: 'UTF-8',
      fieldsDelimitedBy: ',',
      fieldsEnclosedBy: '"',
      linesTerminatedBy: '\n',
    },
    objects: [
      { connector: 'CSV', fullyQualifiedName: 'data', label: 'data', fields },
    ],
  })
}

class GzipCompressor {
  private gz!: Gzip
  private chunks!: Buffer[]
  private compressedSize!: number

  constructor() {
    this.init()
  }

  get size(): number {
    return this.compressedSize
  }

  write(data: string): void {
    this.gz.write(data)
  }

  flush(): Promise<void> {
    return new Promise<void>(resolve => {
      this.gz.flush(resolve)
    })
  }

  finalize(): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      this.gz.on('error', reject)
      this.gz.end(() => resolve(Buffer.concat(this.chunks)))
    })
  }

  reset(): void {
    this.init()
  }

  private init(): void {
    this.gz = createGzip()
    this.chunks = []
    this.compressedSize = 0
    this.gz.on('data', (chunk: Buffer) => {
      this.chunks.push(chunk)
      this.compressedSize += chunk.length
    })
  }
}

class MetadataInitializer {
  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly datasetName: string,
    private readonly operation: Operation
  ) {}

  async createParent(headers: readonly string[]): Promise<string> {
    if (!SF_IDENTIFIER_PATTERN.test(this.datasetName)) {
      throw new Error(`Invalid dataset name: '${this.datasetName}'`)
    }

    const metadataJson = await this.resolveMetadata(headers)

    const headerRecord = await this.sfPort.post<CreateResponse>(
      `${this.basePath}/InsightsExternalData`,
      {
        EdgemartAlias: this.datasetName,
        Format: 'Csv',
        Operation: this.operation,
        Action: 'None',
        MetadataJson: Buffer.from(metadataJson, 'utf-8').toString('base64'),
      }
    )
    return headerRecord.id
  }

  private async resolveMetadata(headers: readonly string[]): Promise<string> {
    const existingResult: QueryResult<InsightsExternalData> =
      await this.sfPort.query(
        `SELECT Id, MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${this.datasetName}' AND Status = 'Completed' ORDER BY CreatedDate DESC LIMIT 1`
      )

    const metadataBlobUrl =
      existingResult.records.length > 0
        ? existingResult.records[0].MetadataJson
        : null

    if (metadataBlobUrl) {
      const blob = await this.sfPort.getBlob(metadataBlobUrl)
      return typeof blob === 'string' ? blob : JSON.stringify(blob)
    }
    return generateMetadataJson(headers)
  }
}

class PartUploader {
  private readonly compressor = new GzipCompressor()
  private partNumber = 0
  private readonly partIds: string[] = []

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly basePath: string,
    private readonly parentId: string,
    private readonly listener?: UploadListener
  ) {}

  async addLine(csvLine: string): Promise<void> {
    if (
      this.compressor.size > 0 &&
      base64Length(this.compressor.size + Buffer.byteLength(csvLine)) >=
        PART_MAX_BYTES
    ) {
      await this.uploadPart()
    }
    this.compressor.write(csvLine)
    await this.compressor.flush()
  }

  async flushRemaining(): Promise<readonly string[]> {
    if (this.compressor.size > 0) {
      await this.uploadPart()
    }
    return this.partIds
  }

  private async uploadPart(): Promise<void> {
    const compressed = await this.compressor.finalize()
    this.compressor.reset()
    const pn = ++this.partNumber
    const result = await this.sfPort.post<CreateResponse>(
      `${this.basePath}/InsightsExternalDataPart`,
      {
        InsightsExternalDataId: this.parentId,
        PartNumber: pn,
        DataFile: compressed.toString('base64'),
      }
    )
    this.partIds.push(result.id)
    this.listener?.onPartUploaded()
  }
}

class CrmaUploadSink implements Uploader {
  private readonly basePath: string
  private readonly datasetName: string
  private readonly initializer: MetadataInitializer
  private parentId: string | undefined
  private partUploader: PartUploader | undefined
  private aborted = false

  constructor(
    private readonly sfPort: SalesforcePort,
    dataset: DatasetKey,
    operation: Operation,
    private readonly listener?: UploadListener
  ) {
    this.basePath = `/services/data/v${sfPort.apiVersion}/sobjects`
    this.datasetName = dataset.name
    this.initializer = new MetadataInitializer(
      sfPort,
      this.basePath,
      this.datasetName,
      operation
    )
  }

  async write(csvLine: string): Promise<void> {
    if (this.aborted) throw new Error('Sink has been aborted')

    if (!this.partUploader) {
      const [parsed] = parse(csvLine) as string[][]
      this.parentId = await this.initializer.createParent(parsed)
      this.listener?.onParentCreated(this.parentId)
      this.partUploader = new PartUploader(
        this.sfPort,
        this.basePath,
        this.parentId,
        this.listener
      )
    }

    await this.partUploader.addLine(csvLine)
  }

  async process(): Promise<UploadResult> {
    if (!this.parentId || !this.partUploader) {
      throw new Error('No data was written to the sink')
    }
    const partIds = await this.partUploader.flushRemaining()
    await this.sfPort.patch(
      `${this.basePath}/InsightsExternalData/${this.parentId}`,
      {
        Action: 'Process',
        Mode: 'Incremental',
      }
    )
    return { parentId: this.parentId, partIds }
  }

  async abort(): Promise<void> {
    this.aborted = true
    if (this.parentId) {
      await this.sfPort.del(
        `${this.basePath}/InsightsExternalData/${this.parentId}`
      )
    }
  }
}

export class UploadSinkFactory implements CreateUploaderPort {
  constructor(private readonly sfPort: SalesforcePort) {}

  create(
    dataset: DatasetKey,
    operation: Operation,
    listener?: UploadListener
  ): Uploader {
    return new CrmaUploadSink(this.sfPort, dataset, operation, listener)
  }
}
