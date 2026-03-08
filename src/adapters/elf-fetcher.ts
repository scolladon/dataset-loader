import { parse } from 'csv-parse/sync'
import { stringify } from 'csv-stringify/sync'
import { type SfClient, type QueryResult } from '../core/sf-client.js'
import { type FetchResult } from '../types.js'

interface EventLogFileRecord {
  Id: string
  LogDate: string
  LogFile: string
}

export async function fetchElf(
  client: SfClient,
  eventType: string,
  interval: string,
  watermark?: string
): Promise<FetchResult | null> {
  const watermarkClause = watermark ? ` AND LogDate > ${watermark}` : ''
  const soql = `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE EventType = '${eventType}' AND Interval = '${interval}'${watermarkClause} ORDER BY LogDate ASC`

  let result: QueryResult<EventLogFileRecord> = await client.query(soql)
  const records: EventLogFileRecord[] = [...result.records]

  while (!result.done && result.nextRecordsUrl) {
    result = await client.queryMore(result.nextRecordsUrl)
    records.push(...result.records)
  }

  if (records.length === 0) return null

  const blobs = await Promise.all(
    records.map((record) =>
      client.getBlob(`/services/data/v${client.apiVersion}/sobjects/EventLogFile/${record.Id}/LogFile`)
    )
  )

  let header: string[] | undefined
  const allDataRows: string[][] = []

  for (const blob of blobs) {
    const parsed: string[][] = parse(blob, { relax_column_count: true })
    if (parsed.length === 0) continue

    if (!header) {
      header = parsed[0]
    }
    allDataRows.push(...parsed.slice(1))
  }

  if (!header) return null

  const csv = stringify([header, ...allDataRows], { quoted: true, quoted_empty: true }).trimEnd()
  const newWatermark = records[records.length - 1].LogDate

  return { csv, newWatermark }
}
