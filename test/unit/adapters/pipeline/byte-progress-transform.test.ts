import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { createByteProgressTransform } from '../../../../src/adapters/pipeline/byte-progress-transform.js'
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

describe('createByteProgressTransform', () => {
  it('given monotonically increasing bytesRead, when batches flow, then deltas are reported per batch', async () => {
    // Arrange
    const tracker = mockTracker()
    const readBytes = vi.fn().mockReturnValueOnce(6).mockReturnValueOnce(9)
    const sut = createByteProgressTransform(readBytes, [tracker])
    const source = Readable.from([['ab', 'cd'], ['ef']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addBytes).toHaveBeenCalledTimes(2)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(1, 6)
    expect(tracker.addBytes).toHaveBeenNthCalledWith(2, 3)
  })

  it('given multiple trackers, when a batch is processed, then every tracker receives the same delta', async () => {
    // Arrange
    const t1 = mockTracker()
    const t2 = mockTracker()
    const readBytes = vi.fn().mockReturnValueOnce(10)
    const sut = createByteProgressTransform(readBytes, [t1, t2])
    const source = Readable.from([['x']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(t1.addBytes).toHaveBeenCalledWith(10)
    expect(t2.addBytes).toHaveBeenCalledWith(10)
  })

  it('given non-monotonic bytesRead, when delta is zero or negative, then addBytes is not called', async () => {
    // Arrange — defensive against a buggy source rewinding the counter.
    const tracker = mockTracker()
    const readBytes = vi.fn().mockReturnValueOnce(5).mockReturnValueOnce(5)
    const sut = createByteProgressTransform(readBytes, [tracker])
    const source = Readable.from([['x'], ['y']])

    // Act
    await drain(source.pipe(sut))

    // Assert — only the first batch (delta from 0) reports
    expect(tracker.addBytes).toHaveBeenCalledTimes(1)
    expect(tracker.addBytes).toHaveBeenCalledWith(5)
  })

  it('given non-finite bytesRead, when delta is computed, then addBytes is not called', async () => {
    // Arrange — defensive against a source returning NaN/Infinity.
    const tracker = mockTracker()
    const readBytes = vi.fn().mockReturnValue(Number.NaN)
    const sut = createByteProgressTransform(readBytes, [tracker])
    const source = Readable.from([['x']])

    // Act
    await drain(source.pipe(sut))

    // Assert
    expect(tracker.addBytes).not.toHaveBeenCalled()
  })

  it('given the transform, when used, then it operates in objectMode and passes batches through unchanged', async () => {
    // Arrange
    const tracker = mockTracker()
    const readBytes = vi.fn().mockReturnValue(1)
    const sut = createByteProgressTransform(readBytes, [tracker])
    const source = Readable.from([['ab', 'cd'], ['ef']])
    const collected: string[][] = []

    // Act
    for await (const batch of source.pipe(sut)) {
      collected.push(batch as string[])
    }

    // Assert
    expect(collected).toEqual([['ab', 'cd'], ['ef']])
  })
})
