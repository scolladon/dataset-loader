import { Writable } from 'node:stream'
import { type BatchMiddleware } from '../../ports/types.js'

export class FanInStream {
  private remaining: number

  constructor(
    private readonly downstream: Writable,
    count: number
  ) {
    this.remaining = count
    // biome-ignore lint/suspicious/noEmptyBlockStatements: absorb error events — errors propagate to producers via write callbacks
    downstream.on('error', () => {})
  }

  createSlot(transforms: readonly BatchMiddleware[] = []): Writable {
    const slot = new Writable({
      objectMode: true,
      write: (chunk: string[], _enc, cb) => {
        let data = chunk
        for (const transform of transforms) {
          data = transform(data)
        }
        this.downstream.write(data, cb)
      },
    })
    slot.once('close', () => {
      if (--this.remaining === 0) {
        this.downstream.end()
      }
    })
    return slot
  }
}
