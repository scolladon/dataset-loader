import { gzip } from 'node:zlib'
import { promisify } from 'node:util'
import { parse } from 'csv-parse/sync'
import { type SfClient, type QueryResult } from '../core/sf-client.js'

const gzipAsync = promisify(gzip)

interface InsightsExternalData {
  Id: string
  MetadataJson: string | null
}

interface CreateResponse {
  id: string
}

const PART_MAX_BYTES = 10 * 1024 * 1024

function generateMetadataJson(headers: string[]): string {
  const fields = headers.map((name) => ({
    fullyQualifiedName: name,
    name,
    type: 'Text',
    label: name,
  }))
  return JSON.stringify({
    fileFormat: { charsetName: 'UTF-8', fieldsDelimitedBy: ',', linesTerminatedBy: '\n' },
    objects: [{ connector: 'CSV', fullyQualifiedName: 'data', label: 'data', fields }],
  })
}

function splitIntoChunks(buffer: Buffer): Buffer[] {
  const chunks: Buffer[] = []
  for (let offset = 0; offset < buffer.length; offset += PART_MAX_BYTES) {
    chunks.push(buffer.subarray(offset, offset + PART_MAX_BYTES))
  }
  return chunks
}

export async function upload(
  client: SfClient,
  dataset: string,
  csv: string,
  operation: 'Append' | 'Overwrite'
): Promise<void> {
  const basePath = `/services/data/v${client.apiVersion}/sobjects`

  const existingResult: QueryResult<InsightsExternalData> = await client.query(
    `SELECT Id, MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${dataset}' AND Status = 'Completed' ORDER BY CreatedDate DESC LIMIT 1`
  )

  const parsed: string[][] = parse(csv, { relax_column_count: true, to: 1 })
  const headers = parsed[0] ?? []
  const metadataJson = existingResult.records.length > 0 && existingResult.records[0].MetadataJson
    ? existingResult.records[0].MetadataJson
    : generateMetadataJson(headers)

  const headerRecord = await client.post<CreateResponse>(`${basePath}/InsightsExternalData`, {
    EdgemartAlias: dataset,
    Format: 'Csv',
    Operation: operation,
    Action: 'None',
    MetadataJson: metadataJson,
  })

  const parentId = headerRecord.id
  const compressed = await gzipAsync(Buffer.from(csv, 'utf-8'))
  const chunks = splitIntoChunks(compressed)

  await Promise.all(
    chunks.map((chunk, index) =>
      client.post(`${basePath}/InsightsExternalDataPart`, {
        InsightsExternalDataId: parentId,
        PartNumber: index + 1,
        DataLength: chunk.length,
        CompressedDataFile: chunk.toString('base64'),
      })
    )
  )

  await client.patch(`${basePath}/InsightsExternalData/${parentId}`, {
    Action: 'Process',
  })
}
