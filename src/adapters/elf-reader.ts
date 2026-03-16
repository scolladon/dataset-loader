import { createInterface } from 'node:readline'
import { PassThrough } from 'node:stream'
import { Watermark } from '../domain/watermark.js'
import {
  type FetchResult,
  type QueryResult,
  type ReaderPort,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
} from '../ports/types.js'

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
    private readonly interval: string
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
    const baseWhere = `EventType = '${this.eventType}' AND Interval = '${this.interval}'`
    const soql = watermark
      ? `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE ${baseWhere} AND LogDate > ${watermark.toSoqlLiteral()} ORDER BY LogDate ASC`
      : `SELECT Id, LogDate, LogFile FROM EventLogFile WHERE ${baseWhere} ORDER BY LogDate DESC LIMIT 1`

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
    let lastRecord: EventLogFileRecord | undefined

    const sfPort = this.sfPort
    const self = this
    const blobUrl = (record: EventLogFileRecord): string =>
      `/services/data/v${sfPort.apiVersion}/sobjects/EventLogFile/${record.Id}/LogFile`

    // Aggregation stream: all pages' blobs write here concurrently
    const aggStream = new PassThrough({ objectMode: true })

    // Serialize writes to aggStream so at most one drain/error listener pair is
    // active at a time, regardless of how many blobs are fetched concurrently.
    // Without this, N blobs waiting for drain simultaneously would add N
    // once('error') listeners, exceeding Node.js's default MaxListeners=10.
    let writeSeq: Promise<void> = Promise.resolve()
    const writeToAgg = (chunk: string): Promise<void> => {
      const next = writeSeq.then(async () => {
        if (aggStream.destroyed) return
        if (!aggStream.write(chunk)) {
          await new Promise<void>((resolve, reject) => {
            const onDrain = () => {
              aggStream.off('error', onError)
              resolve()
            }
            const onError = (err: Error) => {
              aggStream.off('drain', onDrain)
              reject(err)
            }
            aggStream.once('drain', onDrain)
            aggStream.once('error', onError)
          })
        }
      })
      writeSeq = next.catch(() => {
        // keep the chain alive after errors; aggStream.destroyed guard handles post-error writes
      })
      return next
    }

    // processPage fires queryMore for the next page AND all blob fetches for
    // this page concurrently, so blobs across all pages are fetched in parallel.
    async function processPage(
      page: QueryResult<EventLogFileRecord>
    ): Promise<void> {
      if (page.records.length > 0) {
        // Set synchronously — pages are fetched in ascending order so the last
        // page to start always has the highest LogDate.
        lastRecord = page.records.at(-1)
      }

      const nextPagePromise =
        !page.done && page.nextRecordsUrl
          ? sfPort
              .queryMore<EventLogFileRecord>(page.nextRecordsUrl)
              .then(processPage)
          : Promise.resolve()

      const blobPromises = page.records.map(async record => {
        const stream = await sfPort.getBlobStream(blobUrl(record))
        const rl = createInterface({ input: stream, crlfDelay: Infinity })
        let isFirstLine = true
        for await (const line of rl) {
          if (line.length === 0) continue
          if (isFirstLine) {
            isFirstLine = false
            self._header ??= line // schema is the same across all blobs
            continue
          }
          await writeToAgg(line)
        }
        rl.close()
        stream.destroy()
        filesProcessed++
      })

      await Promise.all([nextPagePromise, ...blobPromises])
    }

    return {
      lines: (async function* (): AsyncGenerator<string> {
        const producer = processPage(firstPage)
          .then(() => aggStream.end())
          .catch((err: Error) => aggStream.destroy(err))

        for await (const line of aggStream) {
          yield line as string
        }

        await producer
      })(),
      watermark: () =>
        lastRecord ? Watermark.fromString(lastRecord.LogDate) : undefined,
      fileCount: () => filesProcessed,
    }
  }
}
