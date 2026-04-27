import { Transform } from 'node:stream'
import { type GroupTracker } from '../../ports/types.js'

// objectMode Transform that, on each batch, reads the source's cumulative
// byte counter and reports the delta to all dedup'd trackers. Composes with
// `pipeline()` for native backpressure and error propagation, replacing the
// async-generator wrap that re-encoded lines just to count their bytes.
export function createByteProgressTransform(
  readBytes: () => number,
  trackers: readonly GroupTracker[]
): Transform {
  let last = 0
  return new Transform({
    objectMode: true,
    transform(batch: string[], _enc, cb) {
      const now = readBytes()
      const delta = now - last
      last = now
      if (Number.isFinite(delta) && delta > 0) {
        for (const t of trackers) t.addBytes(delta)
      }
      cb(null, batch)
    },
  })
}
