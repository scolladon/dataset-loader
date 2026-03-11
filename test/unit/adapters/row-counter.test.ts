import { describe, expect, it, vi } from 'vitest'
import { createRowCounter } from '../../../src/adapters/row-counter.js'

describe('createRowCounter', () => {
  it('given tracker, when lines pass through, then addRows called per line', async () => {
    const tracker = { addRows: vi.fn() }
    const sut = createRowCounter(tracker)
    const output: string[] = []
    sut.on('data', (chunk: string) => output.push(chunk))

    sut.write('line1')
    sut.write('line2')
    sut.write('line3')
    sut.end()
    await new Promise(resolve => sut.on('end', resolve))

    expect(tracker.addRows).toHaveBeenCalledTimes(3)
    expect(tracker.addRows).toHaveBeenCalledWith(1)
    expect(output).toEqual(['line1', 'line2', 'line3'])
  })

  it('given tracker, when no lines pass through, then addRows is never called', async () => {
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
