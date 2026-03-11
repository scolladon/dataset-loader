import { createInterface } from 'node:readline'
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

    let filesProcessed = 0

    if (records.length === 0) {
      return {
        lines: (async function* () {
          /* empty */
        })(),
        watermark: () => undefined,
        fileCount: () => filesProcessed,
      }
    }

    // Alias for async generator which cannot reference `this`
    const sfPort = this.sfPort
    const blobUrl = (record: EventLogFileRecord): string =>
      `/services/data/v${sfPort.apiVersion}/sobjects/EventLogFile/${record.Id}/LogFile`

    return {
      lines: (async function* (): AsyncGenerator<string> {
        const streamPromises = records.map(record =>
          sfPort.getBlobStream(blobUrl(record))
        )
        streamPromises.forEach(p =>
          p.catch(() => {
            /* suppress unhandled rejection for pre-fetched streams */
          })
        )

        const pending = new Map(
          streamPromises.map((p, i) => [
            i,
            p.then(stream => ({ index: i, stream })),
          ])
        )

        while (pending.size > 0) {
          const { index, stream } = await Promise.race(pending.values())
          pending.delete(index)

          const rl = createInterface({ input: stream, crlfDelay: Infinity })
          let isFirstLine = true
          for await (const line of rl) {
            if (line.length === 0) continue
            if (isFirstLine) {
              isFirstLine = false
              continue
            }
            yield line
          }
          rl.close()
          stream.destroy()
          filesProcessed++
        }
      })(),
      watermark: () =>
        Watermark.fromString(records[records.length - 1].LogDate),
      fileCount: () => filesProcessed,
    }
  }
}
