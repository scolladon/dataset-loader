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
    // stat is intentionally eager: the byte total is part of the FetchResult
    // contract regardless of whether `lines` is iterated, and `stat` is cheap
    // (a single fstat). The actual file handle (createReadStream) is opened
    // lazily inside the generator so an unconsumed FetchResult never leaks.
    const fileStat = await stat(filePath)
    // Hold the ReadStream reference in outer scope so byte progress can read
    // its live `bytesRead` counter without re-encoding lines.
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
      total: {
        unit: 'bytes',
        count: fileStat.size,
        bytesRead: () => stream?.bytesRead ?? 0,
      },
    }
  }
}
