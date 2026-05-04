import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { createCounterProgressTransform } from '../../../../src/adapters/pipeline/counter-progress-transform.js'
import { type GroupTracker } from '../../../../src/ports/types.js'

function mockTracker(): GroupTracker {
  return {
    updateParentId: vi.fn(),
    incrementParts: vi.fn(),
    addFiles: vi.fn(),
    addRows: vi.fn(),
    addBytes: vi.fn(),
    setTotal: vi.fn(),
    stop: vi.fn(),
  }
}

async function drain(stream: Readable): Promise<void> {
  for await (const _ of stream) {
    // discard
  }
}

describe('createCounterProgressTransform — bytes', () => {
  it('given monotonically increasing bytes, when batches flow, then deltas are reported per batch', async () => {
    // Arrange
    const tracker = mockTracker()
    const read = vi
      .fn()
      .mockReturnValueOnce(6)
      .mockReturnValueOnce(9)
      .mockReturnValueOnce(9)
    const sut = createCounterProgressTransform(read, [tracker], 'bytes')
    const source = Readable.from([['ab', 'cd'], ['ef']])

    // Act
    await drain(source.pipe(sut))

    // Assert — 2 batch ticks (6, 3); flush sees same value (delta 0, ignored)
    expect(tracker.addBytes).toHaveBeenCalledTimes(2)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(1, 6)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(2, 3)
    expect(tracker.addFiles).not.toHaveBeenCalled()
  })

  it('given multiple trackers, when a batch is processed, then every tracker receives the same delta', async () => {
    // Arrange
    const t1 = mockTracker()
    const t2 = mockTracker()
    const read = vi.fn().mockReturnValue(10)
    const sut = createCounterProgressTransform(read, [t1, t2], 'bytes')
    const source = Readable.from([['x']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(t1.addBytes).toHaveBeenCalledWith(10)
    expect(t2.addBytes).toHaveBeenCalledWith(10)
  })

  it('given non-monotonic counter, when delta is zero, then the tracker is not called', async () => {
    // Arrange — defensive against a buggy source rewinding the counter.
    const tracker = mockTracker()
    const read = vi.fn().mockReturnValue(5)
    const sut = createCounterProgressTransform(read, [tracker], 'bytes')
    const source = Readable.from([['x'], ['y']])

    // Act
    await drain(source.pipe(sut))

    // Assert — first batch reports delta 5, subsequent batches and flush
    // report delta 0 (silent).
    expect(tracker.addBytes).toHaveBeenCalledTimes(1)
    expect(tracker.addBytes).toHaveBeenCalledWith(5)
  })

  it('given non-finite counter, when delta is computed, then the tracker is not called', async () => {
    // Arrange — defensive against a source returning NaN/Infinity.
    const tracker = mockTracker()
    const read = vi.fn().mockReturnValue(Number.NaN)
    const sut = createCounterProgressTransform(read, [tracker], 'bytes')
    const source = Readable.from([['x']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addBytes).not.toHaveBeenCalled()
  })

  it('given the transform, when used, then it operates in objectMode and passes batches through unchanged', async () => {
    // Arrange
    const tracker = mockTracker()
    const read = vi.fn().mockReturnValue(1)
    const sut = createCounterProgressTransform(read, [tracker], 'bytes')
    const source = Readable.from([['ab', 'cd'], ['ef']])
    const collected: string[][] = []

    // Act
    for await (const batch of source.pipe(sut)) {
      collected.push(batch as string[])
    }

    // Assert
    expect(collected).toEqual([['ab', 'cd'], ['ef']])
  })

  it('given counter advances after the last batch, when the stream ends, then flush reports the tail delta', async () => {
    // Arrange — simulate a source whose cumulative counter keeps advancing
    // after the final batch yields (e.g. ELF blob completion lands after
    // the channel drains).
    const tracker = mockTracker()
    const read = vi
      .fn()
      .mockReturnValueOnce(4) // first batch
      .mockReturnValueOnce(7) // flush sees +3 tail
    const sut = createCounterProgressTransform(read, [tracker], 'bytes')
    const source = Readable.from([['x']])

    // Act
    await drain(source.pipe(sut))

    // Assert — batch tick reports 4, flush tick reports 3
    expect(tracker.addBytes).toHaveBeenCalledTimes(2)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(1, 4)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(2, 3)
  })
})

describe('createCounterProgressTransform — files', () => {
  it('given files unit, when batches flow, then deltas land on addFiles only', async () => {
    // Arrange
    const tracker = mockTracker()
    const read = vi.fn().mockReturnValueOnce(1).mockReturnValueOnce(3)
    const sut = createCounterProgressTransform(read, [tracker], 'files')
    const source = Readable.from([['a'], ['b']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addFiles).toHaveBeenCalledTimes(2)
    expect(tracker.addFiles).toHaveBeenNthCalledWith(1, 1)
    expect(tracker.addFiles).toHaveBeenNthCalledWith(2, 2)
    expect(tracker.addBytes).not.toHaveBeenCalled()
  })

  it('given files unit and counter advancing post-stream, when flush fires, then tail delta lands on addFiles', async () => {
    // Arrange
    const tracker = mockTracker()
    const read = vi
      .fn()
      .mockReturnValueOnce(2) // first batch
      .mockReturnValueOnce(5) // flush
    const sut = createCounterProgressTransform(read, [tracker], 'files')
    const source = Readable.from([['a']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addFiles).toHaveBeenCalledTimes(2)
    expect(tracker.addFiles).toHaveBeenNthCalledWith(1, 2)
    expect(tracker.addFiles).toHaveBeenNthCalledWith(2, 3)
  })

  it('given files unit and an empty stream, when flush fires alone, then no delta is reported', async () => {
    // Arrange — empty batches: only flush runs; counter stays at 0.
    const tracker = mockTracker()
    const read = vi.fn().mockReturnValue(0)
    const sut = createCounterProgressTransform(read, [tracker], 'files')
    const source = Readable.from([])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addFiles).not.toHaveBeenCalled()
  })
})
