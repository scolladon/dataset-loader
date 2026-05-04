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

  it('given a dataset label containing cli-progress tokens, when tracking group, then the label is sanitized in the format string', () => {
    // Arrange — a user-supplied dataset name like `{value}` would be re-
    // substituted by cli-progress and corrupt the display. Sanitization
    // strips `{` and `}` so the literal text stays in place.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('My{value}Set', false)

    // Assert
    const groupCreateCall = lastMultiBar!.create.mock.calls[1] as [
      number,
      number,
      object,
      { format: string },
    ]
    expect(groupCreateCall[3].format).not.toContain('{value}Set')
    expect(groupCreateCall[3].format).toContain('MyvalueSet')
  })

  it('given a phase label containing cli-progress tokens, when creating a phase, then the label is sanitized in the MultiBar format', () => {
    // Arrange — symmetric protection for the parent bar.
    const sut = new ProgressReporter()

    // Act
    sut.create('Pro{bar}cessing', 1)

    // Assert
    expect(lastMultiBarOptions).toEqual(
      expect.objectContaining({
        format: expect.stringContaining('Probarcessing'),
      })
    )
    expect(lastMultiBarOptions?.format).not.toContain('Pro{bar}cessing')
  })

  it('given tracker with parts, when tracking group, then group bar format includes bar, value/total, unit, and parts placeholders', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('MyDataset', true)

    // Assert — 2nd create call is the group bar; 4th arg carries the format
    const groupCreateCall = lastMultiBar!.create.mock.calls[1] as [
      number,
      number,
      object,
      { format: string },
    ]
    expect(groupCreateCall[3].format).toContain('{bar}')
    expect(groupCreateCall[3].format).toContain('{value}/{total}')
    expect(groupCreateCall[3].format).toContain('{unit}')
    expect(groupCreateCall[3].format).toContain('{files}')
    expect(groupCreateCall[3].format).toContain('{rows}')
    expect(groupCreateCall[3].format).toContain('MyDataset')
    // Kills the parts-suffix mutation: format must contain a parts arrow
    expect(groupCreateCall[3].format).toContain('→ {parts} {partUnit}')
  })

  it('given tracker without parts, when tracking group, then group bar format includes bar but omits parts placeholders', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('MyDataset', false)

    // Assert — 2nd create call is the group bar; 4th arg carries the format
    const groupCreateCall = lastMultiBar!.create.mock.calls[1] as [
      number,
      number,
      object,
      { format: string },
    ]
    expect(groupCreateCall[3].format).toContain('{bar}')
    expect(groupCreateCall[3].format).toContain('{value}/{total}')
    expect(groupCreateCall[3].format).toContain('{unit}')
    expect(groupCreateCall[3].format).toContain('{files}')
    expect(groupCreateCall[3].format).toContain('{rows}')
    expect(groupCreateCall[3].format).toContain('MyDataset')
    // No parts arrow when withParts=false. The terminating placeholder is
    // `{rowsUnit}` — anchoring on `$` kills mutations that append stray
    // text in the false branch of the parts ternary.
    expect(groupCreateCall[3].format).not.toContain('{parts}')
    expect(groupCreateCall[3].format).not.toContain('{partUnit}')
    expect(groupCreateCall[3].format).not.toContain('→')
    expect(groupCreateCall[3].format).toMatch(/\{rowsUnit\}$/)
  })

  it('given non-TTY stderr, when tracking group, then group bar started with zero-initialized payload', () => {
    // Arrange
    setStderrIsTTY(false)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', false)

    // Assert — placeholder total of 1 (not 0) so cli-progress doesn't
    // render `value === total` as "complete" before any setTotal lands.
    const groupBar = lastMultiBar!.bars[1]
    expect(groupBar.start).toHaveBeenCalledWith(
      1,
      0,
      expect.objectContaining({
        files: 0,
        filesUnit: 'files',
        rows: 0,
        rowsUnit: 'rows',
      })
    )
  })

  it('given a fresh tracker before any setTotal, when the group bar is created, then its initial total is the placeholder 1 (not 0)', () => {
    // Arrange — `cli-progress` renders `value === total` as a fully-filled
    // bar; an initial `(0, 0)` group bar therefore appears 100% complete
    // before any data flows. The placeholder of 1 forces `0/1` (empty).
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', false)

    // Assert — the 2nd `multibar.create` call (1st is the parent bar) is
    // the group bar; its total argument must be ≥ 1.
    const groupCreateCall = lastMultiBar!.create.mock.calls[1] as [
      number,
      number,
      object,
      { format: string },
    ]
    expect(groupCreateCall[0]).toBe(1)
    expect(groupCreateCall[1]).toBe(0)
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

  it('given tracker with parts, when incrementParts called once, then group bar payload carries parts=1 and singular partUnit', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS', true)
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.incrementParts()

    // Assert — bar value driven by parts (no setTotal yet; withParts uses parts)
    expect(groupBar.update).toHaveBeenCalledWith(
      1,
      expect.objectContaining({ parts: 1, partUnit: 'part' })
    )
  })

  it('given tracker with parts, when incrementParts is called twice, then group bar updates with incremented parts count and plural partUnit', () => {
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
      expect.objectContaining({ parts: 2, partUnit: 'parts' })
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

  it('given setTotal called with count of zero, when invoked, then the tracker stays in counter-only mode', () => {
    // Arrange — `cli-progress` renders `total=0` as a garbage/empty bar,
    // so an "empty fetch" reply (e.g. SOQL totalSize=0) must leave the bar
    // alone and let the existing zero-state render. Asserts the `count <= 0`
    // boundary in setTotal: zero must be rejected, AND a subsequent same-
    // unit call should still establish the total (i.e. zero must NOT have
    // latched the unit).
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(0, 'rows')
    tracker.setTotal(50, 'rows')

    // Assert — zero is silently dropped; subsequent positive call lands
    expect(groupBar.setTotal).toHaveBeenCalledTimes(1)
    expect(groupBar.setTotal).toHaveBeenCalledWith(50)
  })

  it('given non-TTY stderr and withParts tracker, when starting the group bar, then payload includes parts=0 and partUnit "parts"', () => {
    // Arrange — kills the `{ parts: 0, partUnit: 'parts' }` → `{}` mutation
    // on the initial payload, AND ensures the bar `unit` defaults to 'items'
    // before any setTotal has landed (kills `unitLabel` undefined-case
    // string-literal mutations).
    setStderrIsTTY(false)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', true)

    // Assert — see "zero-initialized payload" test for placeholder rationale.
    const groupBar = lastMultiBar!.bars[1]
    expect(groupBar.start).toHaveBeenCalledWith(
      1,
      0,
      expect.objectContaining({ parts: 0, partUnit: 'parts', unit: 'items' })
    )
  })

  it('given non-TTY stderr and tracker without parts, when starting the group bar, then payload omits parts and partUnit', () => {
    // Arrange — kills the `(withParts ? {parts,partUnit} : {})` → `{}`
    // inversion mutation, asserting the no-parts branch.
    setStderrIsTTY(false)
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)

    // Act
    phase.trackGroup('DS', false)

    // Assert
    const groupBar = lastMultiBar!.bars[1]
    const startCall = groupBar.start.mock.calls.at(-1) as
      | [number, number, Record<string, unknown>]
      | undefined
    expect(startCall?.[2]).not.toHaveProperty('parts')
    expect(startCall?.[2]).not.toHaveProperty('partUnit')
  })

  it('given non-parts tracker, when addRows is called, then bar.update payload omits parts and partUnit', () => {
    // Arrange — kills the `if (withParts) { payload.parts = ...; payload.partUnit = ... }`
    // → `true` mutation in updateBar: when withParts=false the parts payload
    // fields must not be set. The bar `unit` is always present (it labels
    // the bar's progress driver).
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
    expect(lastCall?.[1]).not.toHaveProperty('parts')
    expect(lastCall?.[1]).not.toHaveProperty('partUnit')
    // bar driver unit is always present and defaults to 'items' before setTotal
    expect(lastCall?.[1]).toHaveProperty('unit', 'items')
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

    // Assert — bar total reverts to the placeholder (1, not 0) so the
    // counter-only fallback renders empty, not "complete".
    expect(groupBar.setTotal).toHaveBeenNthCalledWith(1, 5)
    expect(groupBar.setTotal).toHaveBeenLastCalledWith(1)
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

  it('given setTotal with rows unit, when addRows lands a singular value, then payload.unit reads "row"', () => {
    // Arrange — kills the `value === 1` boundary mutation in unitLabel for the
    // rows case (would otherwise read 'rows' for a 1-row bar value).
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(10, 'rows')
    tracker.addRows(1)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      1,
      expect.objectContaining({ unit: 'row' })
    )
  })

  it('given setTotal with rows unit, when addRows lands a plural value, then payload.unit reads "rows"', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(100, 'rows')
    tracker.addRows(50)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      50,
      expect.objectContaining({ unit: 'rows' })
    )
  })

  it('given setTotal with files unit, when addFiles lands a singular value, then payload.unit reads "file"', () => {
    // Arrange — kills the `value === 1` boundary mutation in unitLabel for files.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(10, 'files')
    tracker.addFiles(1)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      1,
      expect.objectContaining({ unit: 'file' })
    )
  })

  it('given setTotal with files unit, when addFiles lands a plural value, then payload.unit reads "files"', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(10, 'files')
    tracker.addFiles(3)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      3,
      expect.objectContaining({ unit: 'files' })
    )
  })

  it('given setTotal with bytes unit, when addBytes lands, then payload.unit reads "bytes"', () => {
    // Arrange
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(2048, 'bytes')
    tracker.addBytes(1024)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      1024,
      expect.objectContaining({ unit: 'bytes' })
    )
  })

  it('given mixed-unit fallback, when addRows is called afterwards, then payload.unit reverts to "items"', () => {
    // Arrange — kills the unitLabel undefined-case mutation: fallback must
    // re-label the bar driver as the neutral 'items'.
    const sut = new ProgressReporter()
    const phase = sut.create('Test', 1)
    const tracker = phase.trackGroup('DS')
    const groupBar = lastMultiBar!.bars[1]

    // Act
    tracker.setTotal(5, 'files')
    tracker.setTotal(1000, 'rows')
    tracker.addRows(50)

    // Assert
    expect(groupBar.update).toHaveBeenLastCalledWith(
      0,
      expect.objectContaining({ unit: 'items', rows: 50 })
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
