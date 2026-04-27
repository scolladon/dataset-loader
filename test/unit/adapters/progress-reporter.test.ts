import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ProgressReporter } from '../../../src/adapters/progress-reporter.js'
import { type ProgressListener } from '../../../src/ports/types.js'

type MockBar = {
  increment: ReturnType<typeof vi.fn>
  update: ReturnType<typeof vi.fn>
  start: ReturnType<typeof vi.fn>
  setTotal: ReturnType<typeof vi.fn>
}

type MockMultiBar = {
  create: ReturnType<typeof vi.fn>
  stop: ReturnType<typeof vi.fn>
  remove: ReturnType<typeof vi.fn>
  bars: MockBar[]
}

// Capture the most recently created mock MultiBar, its bars, and constructor options
let lastMultiBar: MockMultiBar | undefined
let lastMultiBarOptions: Record<string, unknown> | undefined
let restoreIsTTY: (() => void) | undefined

function setStderrIsTTY(value: boolean): void {
  restoreIsTTY = () => {
    delete (process.stderr as NodeJS.WriteStream & { isTTY?: boolean }).isTTY
  }
  Object.defineProperty(process.stderr, 'isTTY', {
    value,
    configurable: true,
    writable: true,
  })
}

vi.mock('cli-progress', () => ({
  default: {
    MultiBar: vi.fn(function MockMultiBarCtor(
      this: MockMultiBar,
      options: Record<string, unknown>
    ) {
      lastMultiBarOptions = options
      const bars: MockBar[] = []
      lastMultiBar = {
        create: vi.fn(() => {
          const bar = {
            increment: vi.fn(),
            update: vi.fn(),
            start: vi.fn(),
            setTotal: vi.fn(),
          }
          bars.push(bar)
          return bar
        }),
        stop: vi.fn(),
        remove: vi.fn(),
        bars,
      }
      return lastMultiBar
    }),
  },
}))

beforeEach(() => {
  restoreIsTTY?.()
  restoreIsTTY = undefined
  lastMultiBar = undefined
  lastMultiBarOptions = undefined
  vi.clearAllMocks()
})

describe('ProgressReporter', () => {
  it('given zero total, when creating phase, then no MultiBar is constructed', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    sut.create('Fetching', 0)

    // Assert — lastMultiBar only gets set when MultiBar constructor runs
    expect(lastMultiBar).toBeUndefined()
  })

  it('given zero total, when tick and stop are called, then no bar is incremented or stopped', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 0)

    // Act
    phase.tick('item')
    phase.stop()

    // Assert — noops leave no observable side effect
    expect(lastMultiBar).toBeUndefined()
  })

  it('given zero total, when tracking group, then returns noop tracker that does not throw', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 0)

    // Act
    const tracker = phase.trackGroup('MyDataset')

    // Assert — noop methods exist and are callable without error
    expect(() => tracker.updateParentId('06Vxxx')).not.toThrow()
    expect(() => tracker.incrementParts()).not.toThrow()
    expect(() => tracker.addFiles(2)).not.toThrow()
    expect(() => tracker.addRows(100)).not.toThrow()
    expect(() => tracker.addBytes(2048)).not.toThrow()
    expect(() => tracker.setTotal(10, 'rows')).not.toThrow()
    expect(() => tracker.stop()).not.toThrow()
  })

  it('given positive total, when creating phase, then MultiBar constructed with expected format and options', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    sut.create('Fetching', 5)

    // Assert
    expect(lastMultiBarOptions).toEqual(
      expect.objectContaining({
        format: expect.stringContaining('Fetching'),
        clearOnComplete: false,
        hideCursor: true,
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
      })
    )
  })

  it('given total of 1, when creating phase, then main bar created with unit "item"', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    sut.create('Test', 1)

    // Assert
    expect(lastMultiBar!.create).toHaveBeenNthCalledWith(1, 1, 0, {
      unit: 'item',
    })
  })

  it('given total greater than 1, when creating phase, then main bar created with unit "items"', () => {
    // Arrange
    const sut = new ProgressReporter()

    // Act
    sut.create('Test', 2)

    // Assert
    expect(lastMultiBar!.create).toHaveBeenNthCalledWith(1, 2, 0, {
      unit: 'items',
    })
  })

  it('given non-TTY stderr, when creating phase, then main bar is started with total and unit', () => {
    // Arrange
    setStderrIsTTY(false)
    const sut = new ProgressReporter()

    // Act
    sut.create('Test', 1)

    // Assert
    const mainBar = lastMultiBar!.bars[0]
    expect(mainBar.start).toHaveBeenCalledWith(1, 0, { unit: 'item' })
  })

  it('given TTY stderr, when creating phase, then main bar start is not called', () => {
    // Arrange
    setStderrIsTTY(true)
    const sut = new ProgressReporter()

    // Act
    sut.create('Test', 1)

    // Assert
    const mainBar = lastMultiBar!.bars[0]
    expect(mainBar.start).not.toHaveBeenCalled()
  })

  it('given positive total, when tick is called, then main bar is incremented', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 2)
    const mainBar = lastMultiBar!.bars[0]

    // Act
    phase.tick('item 1')
    phase.tick('item 2')

    // Assert
    expect(mainBar.increment).toHaveBeenCalledTimes(2)
  })

  it('given positive total, when stop is called, then MultiBar is stopped', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Fetching', 1)

    // Act
    phase.stop()

    // Assert
    expect(lastMultiBar!.stop).toHaveBeenCalledTimes(1)
  })

  it('given tracker with parts, when tracking group, then group bar format includes value and unit placeholders', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('MyDataset', true)

    // Assert — 2nd create call is the group bar; 4th arg carries the format
    expect(lastMultiBar!.create).toHaveBeenNthCalledWith(
      2,
      0,
      0,
      {},
      {
        format: expect.stringContaining('→ {value} {unit}'),
      }
    )
  })

  it('given tracker without parts, when tracking group, then group bar format excludes value placeholder', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('MyDataset', false)

    // Assert — 2nd create call is the group bar; extract format to use .not.toContain
    const groupCreateCall = lastMultiBar!.create.mock.calls[1] as [
      number,
      number,
      object,
      { format: string },
    ]
    expect(groupCreateCall[3].format).not.toContain('{value}')
    // Kills L71 StringLiteral: empty template has none of these placeholders
    expect(groupCreateCall[3].format).toContain('{files}')
    expect(groupCreateCall[3].format).toContain('{rows}')
    expect(groupCreateCall[3].format).toContain('MyDataset')
  })

  it('given non-TTY stderr, when tracking group, then group bar started with zero-initialized payload', () => {
    // Arrange
    setStderrIsTTY(false)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', false)

    // Assert
    const groupBar = lastMultiBar!.bars[1]
    expect(groupBar.start).toHaveBeenCalledWith(
      0,
      0,
      expect.objectContaining({
        files: 0,
        filesUnit: 'files',
        rows: 0,
        rowsUnit: 'rows',
      })
    )
  })

  it('given TTY stderr, when tracking group, then group bar start is not called', () => {
    // Arrange
    setStderrIsTTY(true)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS')

    // Assert
    const groupBar = lastMultiBar!.bars[1]
    expect(groupBar.start).not.toHaveBeenCalled()
  })

  it('given positive total, when addFiles is called, then group bar updates with correct files count', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset')
    const groupBar = lastMultiBar!.bars[1] // bars[0]=main bar, bars[1]=group bar

    // Act
    tracker.addFiles(3)

    // Assert — first arg is 0 because withParts=false
    expect(groupBar.update).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ files: 3, filesUnit: 'files' })
    )
  })

  it('given positive total, when addFiles called with one file, then filesUnit is singular', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.addFiles(1)

    // Assert
    expect(groupBar.update).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ filesUnit: 'file' })
    )
  })

  it('given positive total, when addRows is called, then group bar updates with correct rows count', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.addRows(150)

    // Assert
    expect(groupBar.update).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ rows: 150, rowsUnit: 'rows' })
    )
  })

  it('given tracker, when addRows called with exactly 1, then rowsUnit is singular', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.addRows(1)

    // Assert
    expect(groupBar.update).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ rowsUnit: 'row' })
    )
  })

  it('given tracker with parts, when incrementParts called once, then group bar updated with unit "part"', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS', true)
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.incrementParts()

    // Assert
    expect(groupBar.update).toHaveBeenCalledWith(
      1,
      expect.objectContaining({ unit: 'part' })
    )
  })

  it('given tracker with parts, when incrementParts is called, then group bar updates with incremented part count', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset', true)
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.incrementParts()
    tracker.incrementParts()

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      2,
      expect.objectContaining({ unit: 'parts' })
    )
  })

  it('given tracker without parts, when incrementParts is called, then group bar is not updated', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset')
    const groupBar = lastMultiBar!.bars[1]
    groupBar.update.mockClear()

    // Act
    tracker.incrementParts()

    // Assert
    expect(groupBar.update).not.toHaveBeenCalled()
  })

  it('given tracker, when stop is called, then group bar is removed from MultiBar', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.stop()

    // Assert
    expect(lastMultiBar!.remove).toHaveBeenCalledWith(groupBar)
  })

  it('given two group trackers, when updating one, then they accumulate state independently', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 2)
    const tracker1 = phase.trackGroup('DS1')
    const tracker2 = phase.trackGroup('DS2')
    const groupBar2 = lastMultiBar!.bars[2] // bars[0]=main, bars[1]=DS1, bars[2]=DS2

    // Act
    tracker1.addFiles(3)
    tracker1.addRows(100)
    tracker2.addFiles(1)
    tracker2.addRows(50)

    // Assert — DS2 bar updated with its own counts, not DS1's; first arg is 0 (no parts)
    expect(groupBar2.update).toHaveBeenLastCalledWith(
      0,
      expect.objectContaining({ files: 1, rows: 50 })
    )

    tracker1.stop()
    tracker2.stop()
    phase.stop()
  })

  it('given active tracker, when onRowsWritten called via listener, then rows accumulate in group bar', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('ds', true)
    const groupBar = lastMultiBar!.bars[1]
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
      onRowsWritten: (count: number) => tracker.addRows(count),
    }

    // Act
    listener.onRowsWritten(100)

    // Assert — withParts=true but parts=0, so first arg is 0
    expect(groupBar.update).toHaveBeenCalledWith(
      0,
      expect.objectContaining({ rows: 100 })
    )
  })

  it('given tracker without total, when addBytes is called, then bar value stays 0 (default)', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.addBytes(2048)

    // Assert — without setTotal, bytes don't drive the bar value (still 0, no parts)
    expect(groupBar.update).toHaveBeenLastCalledWith(0, expect.any(Object))
  })

  it('given setTotal called with rows unit, when addRows is called, then bar value reflects rows count', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(1000, 'rows')
    tracker.addRows(250)

    // Assert
    expect(groupBar.setTotal).toHaveBeenCalledWith(1000)
    expect(groupBar.update).toHaveBeenLastCalledWith(
      250,
      expect.objectContaining({ rows: 250 })
    )
  })

  it('given setTotal called with files unit, when addFiles is called, then bar value reflects files count', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(5, 'files')
    tracker.addFiles(2)

    // Assert
    expect(groupBar.setTotal).toHaveBeenCalledWith(5)
    expect(groupBar.update).toHaveBeenLastCalledWith(
      2,
      expect.objectContaining({ files: 2 })
    )
  })

  it('given setTotal called with bytes unit, when addBytes is called, then bar value reflects accumulated bytes', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(10_000, 'bytes')
    tracker.addBytes(1500)
    tracker.addBytes(2500)

    // Assert
    expect(groupBar.setTotal).toHaveBeenCalledWith(10_000)
    expect(groupBar.update).toHaveBeenLastCalledWith(4000, expect.any(Object))
  })

  it('given setTotal with parts tracker, when incrementParts is called, then parts still wins via withParts default', () => {
    // Arrange — withParts=true keeps parts as the visual driver until setTotal
    // overrides; setTotal('rows') redirects the value to rows (kills mutation
    // where parts would always win).
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS', true)
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.incrementParts()
    tracker.setTotal(100, 'rows')
    tracker.addRows(40)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      40,
      expect.objectContaining({ rows: 40 })
    )
  })

  it('given setTotal called with count of zero, when invoked, then bar total is set to zero (the boundary is accepted)', () => {
    // Arrange — kills the `count < 0` → `count <= 0` mutation: zero must pass.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(0, 'rows')

    // Assert
    expect(groupBar.setTotal).toHaveBeenCalledWith(0)
  })

  it('given non-TTY stderr and withParts tracker, when starting the group bar, then payload.unit is initialized to "parts"', () => {
    // Arrange — kills the `{ unit: 'parts' }` → `{}` and `'parts'` → `''`
    // mutations on the initial payload.
    setStderrIsTTY(false)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', true)

    // Assert
    const groupBar = lastMultiBar!.bars[1]
    expect(groupBar.start).toHaveBeenCalledWith(
      0,
      0,
      expect.objectContaining({ unit: 'parts' })
    )
  })

  it('given non-parts tracker, when addRows is called, then bar.update payload omits the unit field', () => {
    // Arrange — kills the `if (withParts)` → `true` mutation in updateBar:
    // when withParts=false the `unit` field must not be added to the payload.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS', false)
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.addRows(10)

    // Assert
    const lastCall = groupBar.update.mock.calls.at(-1) as
      | [number, Record<string, unknown>]
      | undefined
    expect(lastCall?.[1]).not.toHaveProperty('unit')
  })

  it.each([
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['negative', -1],
    ['fractional', 1.5],
  ])('given setTotal called with %s, when invoked, then bar.setTotal is not called', (_label, badCount) => {
    // Arrange — defends against malformed Salesforce responses or test mocks.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(badCount, 'rows')

    // Assert
    expect(groupBar.setTotal).not.toHaveBeenCalled()
  })

  it('given two setTotal calls with same unit, when called, then totals are summed', () => {
    // Arrange — two readers fanning into the same dataset (e.g. two ELF event
    // types into one dataset) both contribute to the same shared tracker.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(1000, 'rows')
    tracker.setTotal(500, 'rows')
    tracker.addRows(750)

    // Assert — bar total is summed, value tracks rows counter as before
    expect(groupBar.setTotal).toHaveBeenNthCalledWith(1, 1000)
    expect(groupBar.setTotal).toHaveBeenNthCalledWith(2, 1500)
    expect(groupBar.update).toHaveBeenLastCalledWith(
      750,
      expect.objectContaining({ rows: 750 })
    )
  })

  it('given setTotal calls with mixed units, when called, then bar reverts to counter-only', () => {
    // Arrange — ELF reader (files unit) + SObject reader (rows unit) sharing
    // a dataset slot. Mixed units can't be sensibly merged on one bar.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(5, 'files')
    tracker.setTotal(1000, 'rows')
    tracker.addFiles(2)
    tracker.addRows(300)

    // Assert — bar total reset to 0, value drops back to non-unit behaviour (0)
    expect(groupBar.setTotal).toHaveBeenNthCalledWith(1, 5)
    expect(groupBar.setTotal).toHaveBeenLastCalledWith(0)
    expect(groupBar.update).toHaveBeenLastCalledWith(0, expect.any(Object))
  })

  it('given mixed-unit fallback then same-unit setTotal, when called, then bar stays in counter-only mode', () => {
    // Arrange — once mixed-unit fallback engages, subsequent same-unit calls
    // do not re-establish a total; the bar stays counter-only for the run.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(5, 'files')
    tracker.setTotal(1000, 'rows')
    tracker.setTotal(2000, 'rows')
    tracker.addRows(100)

    // Assert — fallback is sticky; subsequent setTotal calls are ignored,
    // the bar stays at 0 with counters still ticking in the payload.
    expect(groupBar.setTotal).toHaveBeenCalledTimes(2)
    expect(groupBar.update).toHaveBeenLastCalledWith(
      0,
      expect.objectContaining({ rows: 100 })
    )
  })

  it('given tracker, when updateParentId is called, then group bar is not updated', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Processing', 1)
    const tracker = phase.trackGroup('MyDataset')
    const groupBar = lastMultiBar!.bars[1]
    groupBar.update.mockClear()

    // Act
    tracker.updateParentId('06Vxxx')

    // Assert — parentId is not displayed in the progress bar
    expect(groupBar.update).not.toHaveBeenCalled()
  })
})
