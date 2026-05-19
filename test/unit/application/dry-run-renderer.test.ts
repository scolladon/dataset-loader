import { describe, expect, it, vi } from 'vitest'
import { DryRunRenderer } from '../../../src/application/dry-run-renderer.js'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import type { SalesforcePort } from '../../../src/ports/types.js'
import {
  csvEntry as csv,
  elfEntry as elf,
  makeCaptureLogger as makeLogger,
  resolvedOf as resolved,
  sobjectEntry as sobject,
} from '../../fixtures/application.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

const ISO_JAN = '2026-01-01T00:00:00.000Z'
const ISO_FEB = '2026-02-01T00:00:00.000Z'
const ISO_MAR = '2026-03-01T00:00:00.000Z'

describe('DryRunRenderer', () => {
  it('given empty bounds, when rendering, then emits the multi-line plan with watermark on its own line and no Configured window / effective lines', async () => {
    // Arrange
    const { logger, logs, warns } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    const result = await sut.render(
      [resolved(sobject)],
      WatermarkStore.empty(),
      DateBounds.none()
    )

    // Assert — exact output, in order: header, label line, watermark line.
    expect(logs).toEqual([
      'Dry run — planned entries:',
      '  accounts → org:ana:DS',
      '    watermark: (none)',
    ])
    expect(warns).toEqual([])
    expect(result.entriesProcessed).toBe(0)
  })

  it('given non-empty bounds, when rendering, then emits header + configured window + blank separator + 3-line entry block', async () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render(
      [resolved(sobject)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, ISO_MAR)
    )

    // Assert — exact-position assertions kill StringLiteral mutations on each
    // log line (including the blank separator).
    expect(logs[0]).toBe('Dry run — planned entries:')
    expect(logs[1]).toBe(`Configured window: [${ISO_JAN}, ${ISO_MAR}]`)
    expect(logs[2]).toBe('')
    expect(logs[3]).toBe('  accounts → org:ana:DS')
    expect(logs[4]).toBe('    watermark: (none)')
    expect(logs[5]).toBe(
      `    effective: LastModifiedDate >= ${ISO_JAN} AND LastModifiedDate <= ${ISO_MAR}`
    )
  })

  it('given CSV entry with bounds, when rendering, then emits n/a watermark copy and no effective line', async () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render(
      [resolved(csv)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, undefined)
    )

    // Assert — the CSV phrasing targets the column semantics (watermarks do
    // not apply), independent of bounds.
    expect(logs).toContain(
      '    watermark: n/a (CSV entry — watermarks do not apply)'
    )
    expect(logs.some(l => l.includes('effective:'))).toBe(false)
  })

  it('given CSV entry with empty bounds, when rendering, then still emits the same n/a watermark copy (unified CSV handling)', async () => {
    // Arrange — regression guard: the unified renderer must use the CSV
    // copy regardless of whether bounds are set.
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render([resolved(csv)], WatermarkStore.empty(), DateBounds.none())

    // Assert
    expect(logs).toContain(
      '    watermark: n/a (CSV entry — watermarks do not apply)'
    )
    expect(logs.some(l => l.includes('effective:'))).toBe(false)
    expect(logs.some(l => l.startsWith('Configured window:'))).toBe(false)
  })

  it('given seeded watermark and bounds, when rendering, then the watermark value shows on its own line and the annotation surfaces', async () => {
    // Arrange
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)
    const store = WatermarkStore.empty().set(
      WatermarkKey.fromEntry(sobject),
      Watermark.fromString(ISO_FEB)
    )

    // Act — start < watermark → REWIND annotation on the effective line
    await sut.render(
      [resolved(sobject)],
      store,
      DateBounds.from(ISO_JAN, undefined)
    )

    // Assert
    expect(logs).toContain(`    watermark: ${ISO_FEB}`)
    expect(logs.some(l => l.includes('REWIND'))).toBe(true)
  })

  it('given bounds that trigger a warning, when rendering, then warning is emitted before the plan header', async () => {
    // Arrange — fresh-state ELF + no --start-date fires FIRST_RUN_ELF
    const { logger, logs, warns } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render([resolved(elf)], WatermarkStore.empty(), DateBounds.none())

    // Assert
    expect(warns.some(w => w.includes('FIRST_RUN_ELF'))).toBe(true)
    expect(logs[0]).toBe('Dry run — planned entries:')
  })

  it('given ELF entry with bounds, when rendering, then effective line uses LogDate as the date field', async () => {
    // Arrange — both bounds set and no watermark, so no warning annotation
    // overlays the effective line.
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render(
      [resolved(elf)],
      WatermarkStore.empty(),
      DateBounds.from(ISO_JAN, ISO_MAR)
    )

    // Assert
    expect(logs).toContain(
      `    effective: LogDate >= ${ISO_JAN} AND LogDate <= ${ISO_MAR}`
    )
  })

  it('given multiple entries with empty bounds, when rendering, then each gets its own label + watermark pair (no inline combined format)', async () => {
    // Arrange — multi-entry coverage of the unified format.
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)
    const store = WatermarkStore.empty()
      .set(WatermarkKey.fromEntry(sobject), Watermark.fromString(ISO_FEB))
      .set(WatermarkKey.fromEntry(elf), Watermark.fromString(ISO_JAN))

    // Act
    await sut.render(
      [resolved(sobject), resolved(elf)],
      store,
      DateBounds.none()
    )

    // Assert — no inline `(watermark: …)` fragments; each entry is two lines.
    expect(logs).toEqual([
      'Dry run — planned entries:',
      '  accounts → org:ana:DS',
      `    watermark: ${ISO_FEB}`,
      '  logins → org:ana:DS',
      `    watermark: ${ISO_JAN}`,
    ])
    expect(logs.some(l => l.includes('(watermark:'))).toBe(false)
  })

  it('given empty bounds, when rendering, then no Configured window line is emitted', async () => {
    // Arrange — regression guard for the top-level `if (!bounds.isEmpty())`.
    const { logger, logs } = makeLogger()
    const sut = new DryRunRenderer(logger)

    // Act
    await sut.render(
      [resolved(sobject)],
      WatermarkStore.empty(),
      DateBounds.none()
    )

    // Assert
    expect(logs.some(l => l.startsWith('Configured window:'))).toBe(false)
    expect(logs.some(l => l === '')).toBe(false)
  })

  describe('bootstrap and override annotations', () => {
    function portWithEmptyDatasetQuery(): SalesforcePort {
      return makeSfPort({
        query: vi
          .fn()
          .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
      })
    }
    function portWithExistingDataset(): SalesforcePort {
      return makeSfPort({
        query: vi.fn().mockResolvedValue({
          totalSize: 1,
          done: true,
          records: [{ MetadataJson: '/blob/url' }],
        }),
      })
    }

    it('given an entry whose target dataset does not exist, when rendering, then [BOOTSTRAP] precedes the entry label', async () => {
      // Arrange
      const { logger, logs } = makeLogger()
      const sfPorts = new Map<string, SalesforcePort>([
        ['ana', portWithEmptyDatasetQuery()],
      ])
      const sut = new DryRunRenderer(logger, sfPorts)

      // Act
      await sut.render(
        [resolved(sobject)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(logs.some(l => l.startsWith('  [BOOTSTRAP] accounts'))).toBe(true)
      expect(logs.some(l => l.startsWith('  [OVERRIDE]'))).toBe(false)
    })

    it('given an existing dataset target with overrideMetadata:true, when rendering, then [OVERRIDE] precedes the entry label', async () => {
      // Arrange
      const { logger, logs } = makeLogger()
      const sfPorts = new Map<string, SalesforcePort>([
        ['ana', portWithExistingDataset()],
      ])
      const sut = new DryRunRenderer(logger, sfPorts)
      const entry = {
        ...sobject,
        overrideMetadata: true,
        operation: 'Overwrite' as const,
      }

      // Act
      await sut.render(
        [resolved(entry)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(logs.some(l => l.startsWith('  [OVERRIDE] accounts'))).toBe(true)
      expect(logs.some(l => l.startsWith('  [BOOTSTRAP]'))).toBe(false)
    })

    it('given an existing dataset target without override, when rendering, then no annotation is emitted (existing-dataset normal path)', async () => {
      // Arrange
      const { logger, logs } = makeLogger()
      const sfPorts = new Map<string, SalesforcePort>([
        ['ana', portWithExistingDataset()],
      ])
      const sut = new DryRunRenderer(logger, sfPorts)

      // Act
      await sut.render(
        [resolved(sobject)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(logs.some(l => l.includes('[BOOTSTRAP]'))).toBe(false)
      expect(logs.some(l => l.includes('[OVERRIDE]'))).toBe(false)
    })

    it('given no sfPorts are wired into the renderer, when rendering, then no annotation is emitted (legacy/test paths)', async () => {
      // Arrange — original 2-arg constructor
      const { logger, logs } = makeLogger()
      const sut = new DryRunRenderer(logger)

      // Act
      await sut.render(
        [resolved(sobject)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(logs.some(l => l.includes('[BOOTSTRAP]'))).toBe(false)
      expect(logs.some(l => l.includes('[OVERRIDE]'))).toBe(false)
    })

    it('given two entries targeting the same dataset, when rendering, then exactly one SOQL is issued (per-target dedup)', async () => {
      // Arrange
      const { logger } = makeLogger()
      const querySpy = vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] })
      const port = makeSfPort({ query: querySpy })
      const sfPorts = new Map<string, SalesforcePort>([['ana', port]])
      const sut = new DryRunRenderer(logger, sfPorts)

      // Act
      await sut.render(
        [resolved(sobject), resolved(elf)], // both target ana:DS
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(querySpy).toHaveBeenCalledTimes(1)
    })

    it('given a file target (no targetOrg), when rendering, then no SOQL is issued and no annotation appears', async () => {
      // Arrange
      const { logger, logs } = makeLogger()
      const querySpy = vi.fn()
      const port = makeSfPort({ query: querySpy })
      const sfPorts = new Map<string, SalesforcePort>([['ana', port]])
      const sut = new DryRunRenderer(logger, sfPorts)
      const fileTargetCsv = {
        ...csv,
        targetOrg: undefined,
        targetDataset: undefined,
        targetFile: './out/data.csv',
      }

      // Act
      await sut.render(
        [resolved(fileTargetCsv)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(querySpy).not.toHaveBeenCalled()
      expect(logs.some(l => l.includes('[BOOTSTRAP]'))).toBe(false)
    })

    it('given a target org with no sfPort wired, when rendering, then no annotation is emitted (graceful)', async () => {
      // Arrange
      const { logger, logs } = makeLogger()
      const sfPorts = new Map<string, SalesforcePort>() // empty
      const sut = new DryRunRenderer(logger, sfPorts)

      // Act
      await sut.render(
        [resolved(sobject)],
        WatermarkStore.empty(),
        DateBounds.none()
      )

      // Assert
      expect(logs.some(l => l.includes('[BOOTSTRAP]'))).toBe(false)
    })
  })
})
