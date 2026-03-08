import { type Readable } from 'node:stream'
import { Watermark } from '../domain/watermark.js'
import {
  type FetchPort,
  type FetchResult,
  type QueryResult,
  type SalesforcePort,
} from '../ports/types.js'
import { queryPages } from './query-pages.js'

interface EventLogFileRecord {
  Id: string
  LogDate: string
  LogFile: string
}

export class ElfFetcher implements FetchPort {
  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly eventType: string,
    private readonly interval: string
  ) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(eventType)) {
      throw new Error(`Invalid eventType: '${eventType}'`)
    }
    if (!['Daily', 'Hourly'].includes(interval)) {
      throw new Error(`Invalid interval: '${interval}'`)
    }
  }

  async fetch(watermark?: Watermark): Promise<FetchResult> {
    const watermarkClause = watermark
      ? ` AND LogDate > ${watermark.toSoqlLiteral()}`
      : ''
    const soql = `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE EventType = '${this.eventType}' AND Interval = '${this.interval}'${watermarkClause} ORDER BY LogDate ASC`

    const firstPage: QueryResult<EventLogFileRecord> =
      await this.sfPort.query(soql)
    const records: EventLogFileRecord[] = []
    for await (const page of queryPages(firstPage, (url: string) =>
      this.sfPort.queryMore<EventLogFileRecord>(url)
    )) {
      for (const record of page.records) {
        records.push(record)
      }
    }

    if (records.length === 0) {
      return {
        streams: (async function* () {
          /* empty */
        })(),
        totalHint: 0,
        watermark: () => undefined,
      }
    }

    const sfPort = this.sfPort
    const blobUrl = (record: EventLogFileRecord): string =>
      `/services/data/v${sfPort.apiVersion}/sobjects/EventLogFile/${record.Id}/LogFile`

    return {
      streams: (async function* (): AsyncGenerator<Readable> {
        for (const record of records) {
          yield await sfPort.getBlobStream(blobUrl(record))
        }
      })(),
      totalHint: records.length,
      watermark: () =>
        Watermark.fromString(records[records.length - 1].LogDate),
    }
  }
}
