import { type Readable } from 'node:stream'
import { Watermark } from '../domain/watermark.js'
import {
  type FetchPort,
  type FetchResult,
  type QueryResult,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
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
    if (!SF_IDENTIFIER_PATTERN.test(eventType)) {
      throw new Error(`Invalid eventType: '${eventType}'`)
    }
    if (!['Daily', 'Hourly'].includes(interval)) {
      throw new Error(`Invalid interval: '${interval}'`)
    }
  }

  async fetch(watermark?: Watermark): Promise<FetchResult> {
    const baseWhere = `EventType = '${this.eventType}' AND Interval = '${this.interval}'`
    const soql = watermark
      ? `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE ${baseWhere} AND LogDate > ${watermark.toSoqlLiteral()} ORDER BY LogDate ASC`
      : `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE ${baseWhere} ORDER BY LogDate DESC LIMIT 1`

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
      watermark: () =>
        Watermark.fromString(records[records.length - 1].LogDate),
    }
  }
}
