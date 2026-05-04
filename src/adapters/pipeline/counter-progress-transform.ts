import { Transform } from 'node:stream'
import { type GroupTracker } from '../../ports/types.js'

type CounterUnit = 'bytes' | 'files'

/* v8 ignore next 3 -- exhaustiveness guard; unreachable while CounterUnit
   is a closed union and the dispatch below covers every member. */
function unreachableCounterUnit(value: never): never {
  throw new Error(`unreachable: unexpected CounterUnit ${String(value)}`)
}

// Strict per-unit dispatch — using a switch with an exhaustiveness guard
// (rather than a binary `unit === 'bytes' ? ... : ...`) means a stray
// value (e.g. an empty string passed by a buggy caller) throws instead
// of silently falling through to `addFiles`. This also kills the equivalent
// mutant where `'files'` could be replaced with any non-`'bytes'` literal.
function reportDelta(
  tracker: GroupTracker,
  unit: CounterUnit,
  delta: number
): void {
  switch (unit) {
    case 'bytes':
      tracker.addBytes(delta)
      return
    case 'files':
      tracker.addFiles(delta)
      return
    /* v8 ignore next 2 -- exhaustiveness guard; unreachable as above. */
    default:
      unreachableCounterUnit(unit)
  }
}

// objectMode Transform that, on each batch and again at stream end, reads
// the source's cumulative counter and reports the delta to all dedup'd
// trackers. Composes with `pipeline()` for native backpressure / error
// propagation. The `flush(cb)` reconciliation captures any tail movement
// (e.g. an ELF blob whose `filesProcessed++` lands after the producer
// promise resolves but before the generator returns), so end-of-stream
// the bar reaches the source's true total.
export function createCounterProgressTransform(
  read: () => number,
  trackers: readonly GroupTracker[],
  unit: CounterUnit
): Transform {
  let last = 0
  const report = (delta: number): void => {
    if (Number.isFinite(delta) && delta > 0) {
      for (const t of trackers) reportDelta(t, unit, delta)
    }
  }
  const tick = (): void => {
    const now = read()
    const delta = now - last
    last = now
    report(delta)
  }
  return new Transform({
    objectMode: true,
    transform(batch: string[], _enc, cb) {
      tick()
      cb(null, batch)
    },
    flush(cb) {
      tick()
      cb()
    },
  })
}
