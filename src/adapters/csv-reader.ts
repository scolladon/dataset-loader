import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import { Watermark } from '../domain/watermark.js'
import { type FetchResult, type ReaderPort } from '../ports/types.js'

export class CsvReader implements ReaderPort {
  private _headerPromise?: Promise<string>

  constructor(private readonly filePath: string) {}

  async header(): Promise<string> {
    this._headerPromise ??= this._readHeader()
    return this._headerPromise
  }

  private async _readHeader(): Promise<string> {
    const stream = createReadStream(this.filePath)
    const rl = createInterface({ input: stream, crlfDelay: Infinity })
    try {
      for await (const line of rl) {
        return line
      }
      return ''
    } finally {
      rl.close()
      stream.destroy()
    }
  }

  async fetch(_watermark?: Watermark): Promise<FetchResult> {
    const filePath = this.filePath
    const consumedAt = Watermark.fromString(new Date().toISOString())
    return {
      lines: (async function* (): AsyncGenerator<string> {
        const stream = createReadStream(filePath)
        const rl = createInterface({ input: stream, crlfDelay: Infinity })
        try {
          const iter = rl[Symbol.asyncIterator]()
          await iter.next() // skip header
          for await (const line of iter) {
            if (line.length === 0) continue
            yield line
          }
        } finally {
          rl.close()
          stream.destroy()
        }
      })(),
      watermark: () => consumedAt,
      fileCount: () => 1,
    }
  }
}
