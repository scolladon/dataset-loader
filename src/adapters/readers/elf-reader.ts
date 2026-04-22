import { StringDecoder } from 'node:string_decoder'
import { DateBounds } from '../../domain/date-bounds.js'
import { Watermark } from '../../domain/watermark.js'
import {
  type FetchResult,
  type QueryResult,
  type ReaderPort,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
} from '../../ports/types.js'
import { AsyncChannel } from '../pipeline/async-channel.js'

const BATCH_SIZE = 2000

interface EventLogFileRecord {
  Id: string
  LogDate: string
  LogFile: string
}

export class ElfReader implements ReaderPort {
  private _header?: string

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly eventType: string,
    private readonly interval: string,
    private readonly bounds: DateBounds = DateBounds.none()
  ) {
    if (!SF_IDENTIFIER_PATTERN.test(eventType)) {
      throw new Error(`Invalid eventType: '${eventType}'`)
    }
    if (!['Daily', 'Hourly'].includes(interval)) {
      throw new Error(`Invalid interval: '${interval}'`)
    }
  }

  async header(): Promise<string> {
    return this._header ?? ''
  }

  async fetch(watermark?: Watermark): Promise<FetchResult> {
    const conds: string[] = [
      `EventType = '${this.eventType}'`,
      `Interval = '${this.interval}'`,
    ]
    const lower = this.bounds.lowerConditionFor('LogDate', watermark)
    if (lower) conds.push(lower)
    const upper = this.bounds.upperConditionFor('LogDate')
    if (upper) conds.push(upper)
    const soql = `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE ${conds.join(' AND ')} ORDER BY LogDate ASC`

    const firstPage: QueryResult<EventLogFileRecord> =
      await this.sfPort.query(soql)

    if (firstPage.records.length === 0) {
      return {
        lines: (async function* () {
          /* empty */
        })(),
        watermark: () => undefined,
        fileCount: () => 0,
      }
    }

    let filesProcessed = 0
    let lastRecord: EventLogFileRecord = firstPage.records.at(-1)!

    const sfPort = this.sfPort
    const blobUrl = (record: EventLogFileRecord): string =>
      `/services/data/v${sfPort.apiVersion}/sobjects/EventLogFile/${record.Id}/LogFile`

    // Async channel: all blob processors push batches here concurrently.
    // Backpressure is applied per-push when the queue reaches highWater.
    const channel = new AsyncChannel<string[]>()

    // processPage fires queryMore for the next page AND all blob fetches for
    // this page concurrently, so blobs across all pages are fetched in parallel.
    const processPage = async (
      page: QueryResult<EventLogFileRecord>
    ): Promise<void> => {
      if (page.records.length > 0) {
        // Set synchronously — pages are fetched in ascending order so the last
        // page to start always has the highest LogDate.
        lastRecord = page.records.at(-1)!
      }

      const nextPagePromise =
        !page.done && page.nextRecordsUrl
          ? sfPort
              .queryMore<EventLogFileRecord>(page.nextRecordsUrl)
              .then(processPage)
          : Promise.resolve()

      const blobPromises = page.records.map(async record => {
        const stream = await sfPort.getBlobStream(blobUrl(record))
        try {
          const decoder = new StringDecoder('utf-8')
          let remainder = ''
          let isFirstLine = true
          let pending: string[] = []

          const flushPending = async () => {
            if (pending.length === 0) return
            await channel.push(pending)
            pending = []
          }

          for await (const rawChunk of stream) {
            const text =
              remainder +
              (typeof rawChunk === 'string'
                ? rawChunk
                : decoder.write(rawChunk as Buffer))
            const parts = text.split('\n')
            remainder = parts.pop()!
            for (const rawLine of parts) {
              const line = rawLine.endsWith('\r')
                ? rawLine.slice(0, -1)
                : rawLine
              if (line.length === 0) continue
              if (isFirstLine) {
                isFirstLine = false
                this._header ??= line
                continue
              }
              pending.push(line)
              if (pending.length >= BATCH_SIZE) await flushPending()
            }
          }
          const tail = remainder + decoder.end()
          if (tail.length > 0) {
            const line = tail.endsWith('\r') ? tail.slice(0, -1) : tail
            if (line.length > 0) {
              if (isFirstLine) {
                this._header ??= line
              } else {
                pending.push(line)
              }
            }
          }
          await flushPending()
          filesProcessed++
        } finally {
          stream.destroy()
        }
      })

      await Promise.all([nextPagePromise, ...blobPromises])
    }

    return {
      lines: (async function* (): AsyncGenerator<string[]> {
        const producer = processPage(firstPage)
          .then(() => channel.close())
          .catch((err: Error) => channel.fail(err))

        for await (const batch of channel) {
          yield batch
        }

        await producer
      })(),
      watermark: () => Watermark.fromString(lastRecord.LogDate),
      fileCount: () => filesProcessed,
    }
  }
}
