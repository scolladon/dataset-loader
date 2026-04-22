import { describe, expect, it } from 'vitest'
import { DryRunRenderer } from '../../../src/application/dry-run-renderer.js'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import {
  csvEntry as csv,
  elfEntry as elf,
  makeCaptureLogger as makeLogger,
  resolvedOf as resolved,
  sobjectEntry as sobject,
} from '../../fixtures/application.js'

const ISO_JAN = '2026-01-01T00:00:00.000Z'
const ISO_FEB = '2026-02-01T00:00:00.000Z'
const ISO_MAR = '2026-03-01T00:00:00.000Z'

describe('DryRunRenderer', () => {
  it('given empty bounds, when rendering, then emits the legacy single-line format and no warnings', () => {
    // Arrange
    const { logger, logs, warns } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    const result = sut.render(
      [resolved(sobject)],
      WatermarkStore.empty(),
      DateBounds.none()
    )

    // Assert
    expect(logs[0]).toBe('Dry run — planned entries:')
    expect(logs.some(l => l.includes('(watermark: (none))'))).toBe(true)
    expect(logs.some(l => l.includes('Configured window'))).toBe(false)
    expect(warns).toEqual([])
    expect(result.entriesProcessed).toBe(0)
  })

  it('given non-empty bounds, when rendering, then emits header + configured window + blank separator + entry block', () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    sut.render(
      [resolved(sobject)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, ISO_MAR)
    )

    // Assert — exact-position asserts kill StringLiteral mutations on each log line
    expect(logs[0]).toBe('Dry run — planned entries:')
    expect(logs[1]).toBe(`Configured window: [${ISO_JAN}, ${ISO_MAR}]`)
    expect(logs[2]).toBe('')
    expect(logs[3]).toBe('  accounts → org:ana:DS')
    expect(logs[4]).toBe('    watermark: (none)')
    expect(logs[5]).toContain('    effective: LastModifiedDate >=')
    expect(logs[5]).toContain('AND LastModifiedDate <=')
  })

  it('given CSV entry with bounds, when rendering, then emits the n/a watermark line and no effective clause', () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    sut.render(
      [resolved(csv)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, undefined)
    )

    // Assert
    expect(
      logs.some(l =>
        l.includes('watermark: n/a (CSV entry — bounds do not apply)')
      )
    ).toBe(true)
    expect(logs.some(l => l.includes('effective:'))).toBe(false)
  })

  it('given seeded watermark and bounds, when rendering, then the watermark value shows and the annotation surfaces', () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)
    const store = WatermarkStore.empty().set(
      WatermarkKey.fromEntry(sobject),
      Watermark.fromString(ISO_FEB)
    )

    // Act — start < watermark → REWIND annotation on the effective line
    sut.render([resolved(sobject)], store, DateBounds.from(ISO_JAN, undefined))

    // Assert
    expect(logs.some(l => l === `    watermark: ${ISO_FEB}`)).toBe(true)
    expect(logs.some(l => l.includes('REWIND'))).toBe(true)
  })

  it('given bounds that trigger a warning, when rendering, then warning is emitted before the plan header', () => {
    // Arrange — fresh-state ELF + no --start-date fires FIRST_RUN_ELF
    const { logger, logs, warns } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    sut.render([resolved(elf)], WatermarkStore.empty(), DateBounds.none())

    // Assert
    expect(warns.some(w => w.includes('FIRST_RUN_ELF'))).toBe(true)
    expect(logs[0]).toBe('Dry run — planned entries:')
  })

  it('given ELF entry with bounds, when rendering, then effective line uses LogDate as the date field', () => {
    // Arrange — M3 gap: the `isElfEntry(entry) ? 'LogDate' : entry.dateField`
    // ternary had no ELF-branch coverage at this layer. With both bounds
    // set and no seeded watermark, FIRST_RUN_ELF is suppressed (hasStart),
    // FRESH_END_ONLY is suppressed (hasStart), and no bounds-vs-watermark
    // annotation fires (all four predicates take `undefined` watermark).
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    sut.render(
      [resolved(elf)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, ISO_MAR)
    )

    // Assert — exact effective line forces the ternary to pick 'LogDate'
    expect(logs).toContain(
      `    effective: LogDate >= ${ISO_JAN} AND LogDate <= ${ISO_MAR}`
    )
  })

  it('given multiple entries with empty bounds, when rendering legacy format, then each entry gets its own line', () => {
    // M3/M4 gap — the loop in the legacy path was only exercised with one entry.
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)
    const store = WatermarkStore.empty()
      .set(WatermarkKey.fromEntry(sobject), Watermark.fromString(ISO_FEB))
      .set(WatermarkKey.fromEntry(elf), Watermark.fromString(ISO_JAN))

    // Act
    sut.render([resolved(sobject), resolved(elf)], store, DateBounds.none())

    // Assert — one legacy line per non-CSV entry (in declaration order)
    expect(logs).toContain(`  accounts → org:ana:DS (watermark: ${ISO_FEB})`)
    expect(logs).toContain(`  logins → org:ana:DS (watermark: ${ISO_JAN})`)
  })
})
