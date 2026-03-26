import { describe, expect, it, vi } from 'vitest'
import { createRowCounter } from '../../../src/adapters/row-counter.js'

describe('createRowCounter', () => {
  it('given tracker, when batches pass through, then addRows called with batch size', async () => {
    const tracker = { addRows: vi.fn() }
    const sut = createRowCounter(tracker)
    const output: string[][] = []
    sut.on('data', (batch: string[]) => output.push(batch))

    sut.write(['line1', 'line2', 'line3'])
    sut.write(['line4'])
    sut.end()
    await new Promise(resolve => sut.on('end', resolve))

    expect(tracker.addRows).toHaveBeenCalledTimes(2)
    expect(tracker.addRows).toHaveBeenNthCalledWith(1, 3)
    expect(tracker.addRows).toHaveBeenNthCalledWith(2, 1)
    expect(output).toEqual([['line1', 'line2', 'line3'], ['line4']])
  })

  it('given tracker, when no batches pass through, then addRows is never called', async () => {
    const tracker = { addRows: vi.fn() }
    const sut = createRowCounter(tracker)
    sut.on('data', () => {
      // drain stream to allow 'end' event to fire
    })

    sut.end()
    await new Promise(resolve => sut.on('end', resolve))

    expect(tracker.addRows).not.toHaveBeenCalled()
  })
})
