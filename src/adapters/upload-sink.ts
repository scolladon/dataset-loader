import { createGzip, type Gzip } from 'node:zlib'
import { parse } from 'csv-parse/sync'
import { type DatasetKey } from '../domain/dataset-key.js'
import {
  type CreateUploaderPort,
  type Operation,
  type QueryResult,
  type SalesforcePort,
  type Uploader,
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

class CrmaUploadSink implements Uploader {
  private readonly basePath: string
  private readonly datasetName: string
  private parentId: string | undefined
  private initPromise: Promise<void> | null = null
  private partNumber = 0
  private headers: string[] | undefined
  private readonly partIds: string[] = []
  private aborted = false
  private readonly compressor = new GzipCompressor()

  constructor(
    private readonly sfPort: SalesforcePort,
    dataset: DatasetKey,
    private readonly operation: Operation
  ) {
    this.basePath = `/services/data/v${sfPort.apiVersion}/sobjects`
    this.datasetName = dataset.name
  }

  async write(csvLine: string): Promise<void> {
    if (this.aborted) throw new Error('Sink has been aborted')

    if (!this.headers) {
      const [parsed] = parse(csvLine) as string[][]
      this.headers = parsed
      await this.ensureInitialized()
    }

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

  async process(): Promise<UploadResult> {
    if (!this.parentId) throw new Error('No data was written to the sink')
    if (this.compressor.size > 0) {
      await this.uploadPart()
    }
    await this.sfPort.patch(
      `${this.basePath}/InsightsExternalData/${this.parentId}`,
      {
        Action: 'Process',
        Mode: 'Incremental',
      }
    )
    return { parentId: this.parentId, partIds: this.partIds }
  }

  async abort(): Promise<void> {
    this.aborted = true
    if (this.parentId) {
      await this.sfPort.del(
        `${this.basePath}/InsightsExternalData/${this.parentId}`
      )
    }
  }

  private async initialize(): Promise<void> {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(this.datasetName)) {
      throw new Error(`Invalid dataset name: '${this.datasetName}'`)
    }

    const existingResult: QueryResult<InsightsExternalData> =
      await this.sfPort.query(
        `SELECT Id, MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${this.datasetName}' AND Status = 'Completed' ORDER BY CreatedDate DESC LIMIT 1`
      )

    let metadataJson: string
    const metadataBlobUrl =
      existingResult.records.length > 0
        ? existingResult.records[0].MetadataJson
        : null
    if (metadataBlobUrl) {
      const blob = await this.sfPort.getBlob(metadataBlobUrl)
      metadataJson = typeof blob === 'string' ? blob : JSON.stringify(blob)
    } else {
      metadataJson = generateMetadataJson(this.headers!)
    }

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
    this.parentId = headerRecord.id
  }

  private ensureInitialized(): Promise<void> {
    if (!this.initPromise) {
      this.initPromise = this.initialize()
    }
    return this.initPromise
  }

  private async uploadPart(): Promise<void> {
    const compressed = await this.compressor.finalize()
    this.compressor.reset()
    const pn = ++this.partNumber
    const result = await this.sfPort.post<CreateResponse>(
      `${this.basePath}/InsightsExternalDataPart`,
      {
        InsightsExternalDataId: this.parentId!,
        PartNumber: pn,
        DataFile: compressed.toString('base64'),
      }
    )
    this.partIds.push(result.id)
  }
}

export class UploadSinkFactory implements CreateUploaderPort {
  constructor(private readonly sfPort: SalesforcePort) {}

  create(dataset: DatasetKey, operation: Operation): Uploader {
    return new CrmaUploadSink(this.sfPort, dataset, operation)
  }
}
