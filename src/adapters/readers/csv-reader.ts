import { createReadStream, type ReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createInterface } from 'node:readline'
import { Watermark } from '../../domain/watermark.js'
import { type FetchResult, type ReaderPort } from '../../ports/types.js'

const BATCH_SIZE = 2000

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
    // Stat once at fetch start — gives the byte total without re-reading.
    const fileStat = await stat(filePath)
    // Hold the ReadStream reference in outer scope so the pipeline can read
    // its live `bytesRead` counter for progress without re-encoding lines.
    // Created lazily inside the generator on first iteration so an unconsumed
    // FetchResult does not leak an open file handle.
    let stream: ReadStream | undefined
    return {
      lines: (async function* (): AsyncGenerator<string[]> {
        stream = createReadStream(filePath)
        const rl = createInterface({ input: stream, crlfDelay: Infinity })
        try {
          const iter = rl[Symbol.asyncIterator]()
          await iter.next() // skip header
          let batch: string[] = []
          for await (const line of iter) {
            if (line.length === 0) continue
            batch.push(line)
            if (batch.length >= BATCH_SIZE) {
              yield batch
              batch = []
            }
          }
          if (batch.length > 0) yield batch
        } finally {
          rl.close()
          stream.destroy()
        }
      })(),
      watermark: () => consumedAt,
      fileCount: () => 1,
      total: { count: fileStat.size, unit: 'bytes' },
      bytesRead: () => stream?.bytesRead ?? 0,
    }
  }
}
