import { PassThrough, Readable, Writable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { FanInStream } from '../../../src/adapters/pipeline/fan-in-stream.js'

function collectWritable(): { writable: Writable; lines: string[] } {
  const lines: string[] = []
  const writable = new PassThrough({ objectMode: true })
  writable.on('data', (batch: string[]) => lines.push(...batch))
  return { writable, lines }
}

describe('FanInStream', () => {
  it('given one slot, when slot writes and closes, then downstream receives all data and ends', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot()

    // Act
    await pipeline(Readable.from([['a', 'b'], ['c']]), slot)
    await finished(downstream)

    // Assert
    expect(lines).toEqual(['a', 'b', 'c'])
    expect(downstream.readableEnded).toBe(true)
  })

  it('given two slots, when both write concurrently and close, then downstream receives all data and ends once', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 2)
    const slot0 = sut.createSlot()
    const slot1 = sut.createSlot()

    // Act
    await Promise.all([
      pipeline(Readable.from([['a', 'b']]), slot0),
      pipeline(Readable.from([['c', 'd']]), slot1),
    ])
    await finished(downstream)

    // Assert
    expect(lines.sort()).toEqual(['a', 'b', 'c', 'd'])
    expect(downstream.readableEnded).toBe(true)
  })

  it('given two slots, when one slot is destroyed before closing, then downstream still ends after the other slot closes', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 2)
    const slot0 = sut.createSlot()
    const slot1 = sut.createSlot()

    // Act — destroy slot0 immediately (simulates a failed entry pipeline)
    slot0.destroy()
    await pipeline(Readable.from([['x']]), slot1)
    await finished(downstream)

    // Assert
    expect(lines).toEqual(['x'])
    expect(downstream.readableEnded).toBe(true)
  })

  it('given downstream that errors, when slot writes, then write callback receives the error', async () => {
    // Arrange
    const downstream = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        cb(new Error('downstream exploded'))
      },
    })
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot()

    // Act & Assert
    await expect(pipeline(Readable.from([['row']]), slot)).rejects.toThrow(
      'downstream exploded'
    )
  })

  it('given slot with augment transform, when data flows through, then lines have suffix appended', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot([batch => batch.map(l => l + ',"extra"')])

    // Act
    await pipeline(Readable.from([['"a"', '"b"']]), slot)
    await finished(downstream)

    // Assert
    expect(lines).toEqual(['"a","extra"', '"b","extra"'])
  })

  it('given slot with empty transforms array, when data flows through, then lines are unchanged', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot([])

    // Act
    await pipeline(Readable.from([['"a"']]), slot)
    await finished(downstream)

    // Assert
    expect(lines).toEqual(['"a"'])
  })

  it('given two slots with different augment transforms, when both write, then each applies its own transform', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 2)
    const slot0 = sut.createSlot([batch => batch.map(l => l + ',"org1"')])
    const slot1 = sut.createSlot([batch => batch.map(l => l + ',"org2"')])

    // Act
    await Promise.all([
      pipeline(Readable.from([['"a"']]), slot0),
      pipeline(Readable.from([['"b"']]), slot1),
    ])
    await finished(downstream)

    // Assert
    expect(lines.sort()).toEqual(['"a","org1"', '"b","org2"'])
  })

  it('given slow downstream, when producer writes many chunks, then backpressure holds in-flight count bounded', async () => {
    // Arrange — downstream that intentionally delays its write callback
    let inFlight = 0
    let maxInFlight = 0
    const downstream = new Writable({
      objectMode: true,
      highWaterMark: 1,
      write(_chunk, _enc, cb) {
        inFlight++
        if (inFlight > maxInFlight) maxInFlight = inFlight
        setTimeout(() => {
          inFlight--
          cb()
        }, 2)
      },
    })
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot()

    // Act — pump 30 batches through the slot
    const batches = Array.from({ length: 30 }, (_, i) => [`row-${i}`])
    await pipeline(Readable.from(batches), slot)
    await finished(downstream)

    // Assert — the delayed callback creates backpressure; we never have more
    // than a tiny number of concurrent in-flight writes (strict bound = 1 given
    // highWaterMark:1, allow slack of 2 for timer race).
    expect(maxInFlight).toBeLessThanOrEqual(2)
  })

  it('given N slots, when all close in sequence, then downstream ends exactly once', async () => {
    // Arrange
    let endCount = 0
    const downstream = new PassThrough({ objectMode: true })
    downstream.on('end', () => endCount++)
    const sut = new FanInStream(downstream, 4)
    downstream.resume() // keep flowing

    // Act
    await Promise.all(
      [0, 1, 2, 3].map(i =>
        pipeline(Readable.from([[`s${i}`]]), sut.createSlot())
      )
    )
    await finished(downstream)

    // Assert
    expect(endCount).toBe(1)
    expect(downstream.readableEnded).toBe(true)
  })

  it('given slot with chained transforms, when data flows through, then transforms applied in order', async () => {
    // Arrange
    const { writable: downstream, lines } = collectWritable()
    const sut = new FanInStream(downstream, 1)
    const slot = sut.createSlot([
      batch => batch.map(l => l + ',A'),
      batch => batch.map(l => l + ',B'),
    ])

    // Act
    await pipeline(Readable.from([['x']]), slot)
    await finished(downstream)

    // Assert
    expect(lines).toEqual(['x,A,B'])
  })
})
