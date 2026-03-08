import { stringify } from 'csv-stringify/sync'
import { type SfClient, type QueryResult } from '../core/sf-client.js'
import { type FetchResult } from '../types.js'

export async function fetchSObject(
  client: SfClient,
  sobject: string,
  fields: string[],
  dateField: string,
  watermark?: string,
  where?: string
): Promise<FetchResult | null> {
  const conditions: string[] = []
  if (watermark) conditions.push(`${dateField} > ${watermark}`)
  if (where) conditions.push(`(${where})`)

  const whereClause = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : ''
  const soql = `SELECT ${fields.join(', ')} FROM ${sobject}${whereClause} ORDER BY ${dateField} ASC`

  let result: QueryResult<Record<string, unknown>> = await client.query(soql)
  const records: Record<string, unknown>[] = [...result.records]

  while (!result.done && result.nextRecordsUrl) {
    result = await client.queryMore(result.nextRecordsUrl)
    records.push(...result.records)
  }

  if (records.length === 0) return null

  const rows = records.map((record) => fields.map((f) => record[f] ?? ''))
  const csv = stringify([fields, ...rows], { quoted: true, quoted_empty: true }).trimEnd()

  const lastRecord = records[records.length - 1]
  const newWatermark = String(lastRecord[dateField])

  return { csv, newWatermark }
}
