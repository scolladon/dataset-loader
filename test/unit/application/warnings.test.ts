import { describe, expect, it } from 'vitest'
import { type ConfigEntry } from '../../../src/adapters/config-loader.js'
import {
  boundsMessages,
  computeWarnings,
  dryRunAnnotation,
  firstRunMessages,
} from '../../../src/application/warnings.js'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import {
  csvEntry as csv,
  elfEntry as elf,
  resolvedOf as resolved,
  sobjectEntry as sobject,
  sobjectEntryOverwrite as sobjectOverwrite,
} from '../../fixtures/application.js'

const ISO_JAN = '2026-01-01T00:00:00.000Z'
const ISO_FEB = '2026-02-01T00:00:00.000Z'
const ISO_MAR = '2026-03-01T00:00:00.000Z'

function storeWith(entry: ConfigEntry, watermarkIso: string): WatermarkStore {
  return WatermarkStore.empty().set(
    WatermarkKey.fromEntry(entry),
    Watermark.fromString(watermarkIso)
  )
}

describe('firstRunMessages', () => {
  it('given --start-date is set, when computing, then no first-run messages fire', () => {
    const bounds = DateBounds.from(ISO_JAN, undefined)
    expect(
      firstRunMessages([resolved(elf)], WatermarkStore.empty(), bounds)
    ).toEqual([])
  })

  it('given fresh state + ELF entry + no bounds, when computing, then FIRST_RUN_ELF fires', () => {
    const bounds = DateBounds.none()
    const out = firstRunMessages(
      [resolved(elf)],
      WatermarkStore.empty(),
      bounds
    )
    expect(out).toHaveLength(1)
    expect(out[0]).toContain('FIRST_RUN_ELF')
    expect(out[0]).toContain('--start-date')
  })

  it('given fresh state + SObject entry + no bounds, when computing, then no warning fires', () => {
    const bounds = DateBounds.none()
    expect(
      firstRunMessages([resolved(sobject)], WatermarkStore.empty(), bounds)
    ).toEqual([])
  })

  it('given fresh state + SObject entry + --end-date only, when computing, then FRESH_END_ONLY fires', () => {
    const bounds = DateBounds.from(undefined, ISO_MAR)
    const out = firstRunMessages(
      [resolved(sobject)],
      WatermarkStore.empty(),
      bounds
    )
    expect(out).toHaveLength(1)
    expect(out[0]).toContain('FRESH_END_ONLY')
  })

  it('given fresh state + ELF entry + --end-date only, when computing, then FRESH_END_ONLY fires (ELF branch is suppressed)', () => {
    const bounds = DateBounds.from(undefined, ISO_MAR)
    const out = firstRunMessages(
      [resolved(elf)],
      WatermarkStore.empty(),
      bounds
    )
    expect(out).toHaveLength(1)
    expect(out[0]).toContain('FRESH_END_ONLY')
  })

  it('given seeded watermark + ELF entry + no bounds, when computing, then nothing fires (not fresh)', () => {
    const bounds = DateBounds.none()
    expect(
      firstRunMessages([resolved(elf)], storeWith(elf, ISO_JAN), bounds)
    ).toEqual([])
  })

  it('given fresh state + CSV entry + --end-date only, when computing, then nothing fires (CSV excluded)', () => {
    const bounds = DateBounds.from(undefined, ISO_MAR)
    expect(
      firstRunMessages([resolved(csv)], WatermarkStore.empty(), bounds)
    ).toEqual([])
  })
})

describe('boundsMessages', () => {
  it('given empty bounds, when computing, then no messages', () => {
    expect(
      boundsMessages(
        [resolved(sobject)],
        WatermarkStore.empty(),
        DateBounds.none()
      )
    ).toEqual([])
  })

  it('given bounds + all-CSV entries, when computing, then the "all CSV, no effect" message fires', () => {
    const bounds = DateBounds.from(ISO_JAN, undefined)
    const out = boundsMessages([resolved(csv)], WatermarkStore.empty(), bounds)
    expect(out).toHaveLength(1)
    expect(out[0]).toContain('all selected entries are CSV')
  })

  it('given --start-date before watermark, when computing, then REWIND fires with exact template', () => {
    // M2 tightening — exact-string assertion kills StringLiteral mutations
    // on the full warning template (not just the `REWIND` keyword).
    const bounds = DateBounds.from(ISO_JAN, undefined)
    const out = boundsMessages(
      [resolved(sobject)],
      storeWith(sobject, ISO_FEB),
      bounds
    )
    expect(out).toEqual([
      `[accounts] REWIND: --start-date is before watermark ${ISO_FEB}; previously-loaded records will be re-loaded; watermark may regress.`,
    ])
  })

  it('given --start-date after watermark, when computing, then HOLE fires with exact template', () => {
    const bounds = DateBounds.from(ISO_MAR, undefined)
    const out = boundsMessages(
      [resolved(sobject)],
      storeWith(sobject, ISO_FEB),
      bounds
    )
    expect(out).toEqual([
      `[accounts] HOLE: --start-date is after watermark ${ISO_FEB}; records between the watermark and --start-date will be skipped this run AND by subsequent incremental runs (watermark will jump past the gap as soon as any in-window record loads).`,
    ])
  })

  it('given --start-date equals watermark under Append, when computing, then BOUNDARY fires with exact template', () => {
    const bounds = DateBounds.from(ISO_FEB, undefined)
    const out = boundsMessages(
      [resolved(sobject)],
      storeWith(sobject, ISO_FEB),
      bounds
    )
    expect(out).toEqual([
      `[accounts] BOUNDARY: --start-date equals watermark ${ISO_FEB}; under operation Append the boundary record will be appended again (duplicate row). Bump --start-date past the watermark, or use operation Overwrite.`,
    ])
  })

  it('given --start-date equals watermark under Overwrite, when computing, then no BOUNDARY fires', () => {
    const bounds = DateBounds.from(ISO_FEB, undefined)
    const out = boundsMessages(
      [resolved(sobjectOverwrite)],
      storeWith(sobjectOverwrite, ISO_FEB),
      bounds
    )
    expect(out).toEqual([])
  })

  it('given --end-date before watermark, when computing, then EMPTY fires with exact template', () => {
    const bounds = DateBounds.from(undefined, ISO_JAN)
    const out = boundsMessages(
      [resolved(sobject)],
      storeWith(sobject, ISO_FEB),
      bounds
    )
    expect(out).toEqual([
      `[accounts] EMPTY: --end-date is before watermark ${ISO_FEB}; query window is empty — no records will load. To replay this range, use a separate --state-file (see RUN_BOOK).`,
    ])
  })
})

describe('dryRunAnnotation', () => {
  it('given --start-date before watermark, when annotating, then returns exact REWIND suffix', () => {
    const bounds = DateBounds.from(ISO_JAN, undefined)
    const wm = Watermark.fromString(ISO_FEB)
    expect(dryRunAnnotation(sobject, wm, bounds)).toBe(
      '  (REWIND: --start-date before watermark — watermark may regress)'
    )
  })

  it('given --start-date after watermark, when annotating, then returns exact HOLE suffix', () => {
    // I4 gap — previously only REWIND and EMPTY were tested.
    const bounds = DateBounds.from(ISO_MAR, undefined)
    const wm = Watermark.fromString(ISO_FEB)
    expect(dryRunAnnotation(sobject, wm, bounds)).toBe(
      '  (HOLE: --start-date after watermark — records in the gap will never be back-filled)'
    )
  })

  it('given --start-date equals watermark under Append, when annotating, then returns exact BOUNDARY suffix', () => {
    // I4 gap — Append-only BOUNDARY annotation.
    const bounds = DateBounds.from(ISO_FEB, undefined)
    const wm = Watermark.fromString(ISO_FEB)
    expect(dryRunAnnotation(sobject, wm, bounds)).toBe(
      '  (BOUNDARY: --start-date equals watermark — boundary record will be re-appended (duplicate))'
    )
  })

  it('given --start-date equals watermark under Overwrite, when annotating, then returns empty string (no annotation)', () => {
    // Regression guard — Overwrite must not surface BOUNDARY.
    const bounds = DateBounds.from(ISO_FEB, undefined)
    const wm = Watermark.fromString(ISO_FEB)
    expect(dryRunAnnotation(sobjectOverwrite, wm, bounds)).toBe('')
  })

  it('given no bounds conflict, when annotating, then returns empty string', () => {
    expect(dryRunAnnotation(sobject, undefined, DateBounds.none())).toBe('')
  })

  it('given --end-date before watermark, when annotating, then returns EMPTY suffix', () => {
    const bounds = DateBounds.from(undefined, ISO_JAN)
    const wm = Watermark.fromString(ISO_FEB)
    expect(dryRunAnnotation(sobject, wm, bounds)).toBe(
      '  (EMPTY: end-date before watermark — no records will load)'
    )
  })
})

describe('computeWarnings', () => {
  it('given fresh-state ELF entry + --end-date only, when computing, then both FRESH_END_ONLY and EMPTY-family are collected in order', () => {
    // Fresh state → FRESH_END_ONLY fires; no watermark means EMPTY (which
    // requires a watermark) does NOT fire. This confirms the ordering:
    // firstRun messages precede bounds messages.
    const bounds = DateBounds.from(undefined, ISO_MAR)
    const out = computeWarnings([resolved(elf)], WatermarkStore.empty(), bounds)
    expect(out.length).toBeGreaterThanOrEqual(1)
    expect(out[0]).toContain('FRESH_END_ONLY')
  })

  it('given no bounds and no entries in fresh state, when computing, then no messages', () => {
    expect(
      computeWarnings([], WatermarkStore.empty(), DateBounds.none())
    ).toEqual([])
  })
})
