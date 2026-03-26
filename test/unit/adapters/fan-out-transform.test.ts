import { PassThrough, Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { createFanOutTransform } from '../../../src/adapters/fan-out-transform.js'

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
