import { PassThrough, Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { createFanOutTransform } from '../../../src/adapters/pipeline/fan-out-transform.js'

async function collect(stream: PassThrough): Promise<string[]> {
  const batches: string[][] = []
  stream.on('data', (b: string[]) => batches.push(b))
  return new Promise(resolve => stream.on('end', () => resolve(batches.flat())))
}

describe('createFanOutTransform', () => {
  it('given two channels, when source emits three batches, then both channels receive all batches', async () => {
    // Arrange
    const ch1 = new PassThrough({ objectMode: true })
    const ch2 = new PassThrough({ objectMode: true })
    const errors: Error[] = []
    const sut = createFanOutTransform([ch1, ch2], err => errors.push(err))
    const p1 = collect(ch1)
    const p2 = collect(ch2)

    // Act
    await pipeline(Readable.from([['a', 'b'], ['c']]), sut)
    const [r1, r2] = await Promise.all([p1, p2])

    // Assert
    expect(r1).toEqual(['a', 'b', 'c'])
    expect(r2).toEqual(['a', 'b', 'c'])
    expect(errors).toHaveLength(0)
    expect(ch1.readableEnded).toBe(true)
    expect(ch2.readableEnded).toBe(true)
  })

  it('given channel with repeated backpressure, when pipeline completes, then no error listeners leaked', async () => {
    // Arrange: highWaterMark=1 forces backpressure on almost every write
    const ch = new PassThrough({ objectMode: true, highWaterMark: 1 })
    const sut = createFanOutTransform([ch], () => {
      // no-op: errors are not expected in this test
    })

    // Act: slow consumer (one item per event-loop tick) to induce repeated backpressure+drain cycles
    const results: string[] = []
    const consuming = (async () => {
      for await (const batch of ch) {
        results.push(...(batch as string[]))
        await new Promise<void>(resolve => setImmediate(resolve))
      }
    })()
    await pipeline(
      Readable.from(Array.from({ length: 12 }, (_, i) => [`c${i}`])),
      sut
    )
    await consuming

    // Assert: only the once('error') from createFanOutTransform setup remains — none leaked by promiseWrite
    // (≤2 allows for an async-iterator internal 'error' listener in some Node.js versions)
    expect(results).toHaveLength(12)
    expect(ch.listenerCount('error')).toBeLessThanOrEqual(2)
  })

  it('given channel emits error while blocked on backpressure, when writing, then onChannelError is called via .catch handler', async () => {
    // Arrange: highWaterMark=0 so ch1.write() immediately returns false (backpressure)
    const ch1 = new PassThrough({ objectMode: true, highWaterMark: 0 })
    ch1.on('error', () => {
      /* noop */
    }) // prevent unhandled error event

    const capturedErrors: Error[] = []
    const sut = createFanOutTransform([ch1], err => capturedErrors.push(err))

    // Act: start pipeline — write returns false → promiseWrite registers once('error', onError)
    const pipePromise = pipeline(Readable.from([['item1']]), sut).catch(() => {
      /* noop */
    })

    // Wait for promiseWrite to have registered its error listener
    await new Promise<void>(resolve => setImmediate(resolve))

    // Destroy ch1 BEFORE drain fires — triggers onError (lines 11-13) then .catch (lines 40-41)
    ch1.destroy(new Error('error during backpressure'))

    await pipePromise

    // Assert: onChannelError was called (covers lines 40-41 in the .catch handler)
    expect(capturedErrors.length).toBeGreaterThanOrEqual(1)
    expect(
      capturedErrors.some(e => e.message === 'error during backpressure')
    ).toBe(true)
    // Kills L11 StringLiteral: mutation uses stream.off("", onDrain) which leaves
    // the 'drain' listener registered; the original correctly removes it on error
    expect(ch1.listenerCount('drain')).toBe(0)
  })

  it('given channel closes before pipeline runs, when pipeline runs, then only active channel receives data', async () => {
    // Arrange — kills ch.once('close', () => active.delete(ch)) mutation:
    // without close handler, ch1 stays in active; writing to destroyed ch1 errors
    const ch1 = new PassThrough({ objectMode: true })
    const ch2 = new PassThrough({ objectMode: true })
    const errors: Error[] = []
    const sut = createFanOutTransform([ch1, ch2], err => errors.push(err))
    const p2 = collect(ch2)

    // Destroy ch1 and wait for 'close' to fire → active.delete(ch1) executes
    ch1.resume() // switch to flowing mode so readable side drains and 'close' can fire
    ch1.destroy()
    await new Promise<void>(resolve => ch1.once('close', resolve))

    // Act: pipeline only writes to ch2 (ch1 removed from active)
    await pipeline(Readable.from([['a', 'b'], ['c']]), sut)
    const r2 = await p2

    // Assert — no error thrown; ch2 received all batches
    expect(r2).toEqual(['a', 'b', 'c'])
    expect(errors).toHaveLength(0)
  })

  it('given one channel errors, when source emits batches, then onChannelError is called and other channel still receives all lines', async () => {
    // Arrange
    const ch1 = new PassThrough({ objectMode: true })
    const ch2 = new PassThrough({ objectMode: true })
    const errors: string[] = []
    const sut = createFanOutTransform([ch1, ch2], err =>
      errors.push(err.message)
    )
    const p1 = collect(ch1)

    // Destroy ch2 immediately to simulate a dead channel
    ch2.destroy(new Error('channel dead'))

    // Act
    await pipeline(Readable.from([['a', 'b'], ['c']]), sut)
    const r1 = await p1

    // Assert
    expect(r1).toEqual(['a', 'b', 'c'])
    expect(errors).toEqual(['channel dead'])
  })
})
