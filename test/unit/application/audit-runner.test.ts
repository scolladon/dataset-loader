import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { AuditRunner } from '../../../src/application/audit-runner.js'
import { type SalesforcePort } from '../../../src/ports/types.js'
import {
  csvEntry as csv,
  elfEntry as elf,
  makeCaptureLogger as makeLogger,
  resolvedOf as resolved,
  sobjectEntry as sobject,
} from '../../fixtures/application.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

vi.mock('../../../src/domain/auditor.js', () => ({
  buildAuditChecks: vi.fn(() => []),
  runAudit: vi.fn().mockResolvedValue({ passed: true }),
}))

import { buildAuditChecks, runAudit } from '../../../src/domain/auditor.js'

describe('AuditRunner', () => {
  // `process.exitCode` is module-global; saving/restoring around each test
  // prevents cross-file ordering leaks. `clearAllMocks` resets call history
  // so `buildAuditChecks.mock.calls[0]` in each test is the test's own call.
  let savedExitCode: typeof process.exitCode
  beforeEach(() => {
    savedExitCode = process.exitCode
    process.exitCode = undefined
    vi.clearAllMocks()
  })
  afterEach(() => {
    process.exitCode = savedExitCode
  })

  it('given passing audit, when running, then returns zero failures and does not set exit code', async () => {
    // Arrange
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: true })
    const { logger, logs } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    const result = await sut.run(
      [resolved(sobject), resolved(elf), resolved(csv)],
      new Map<string, SalesforcePort>([
        ['src', makeSfPort()],
        ['ana', makeSfPort()],
      ])
    )

    // Assert
    expect(logs[0]).toBe('Audit — pre-flight checks:')
    expect(result.entriesFailed).toBe(0)
    expect(process.exitCode).toBeUndefined()
  })

  it('given failing audit, when running, then returns one failure and sets exit code 2', async () => {
    // Arrange
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: false })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    const result = await sut.run([resolved(sobject)], new Map())

    // Assert
    expect(result.entriesFailed).toBe(1)
    expect(process.exitCode).toBe(2)
  })

  it('given mixed entries, when building audit entries, then each kind gets the correct readerKind', async () => {
    // Arrange — regression guard on the SObject / ELF / CSV discrimination
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: true })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    await sut.run([resolved(sobject), resolved(elf), resolved(csv)], new Map())

    // Assert
    const auditEntries = vi.mocked(buildAuditChecks).mock.calls[0][0]
    expect(auditEntries).toHaveLength(3)
    expect(auditEntries[0].readerKind).toBe('sobject')
    expect(auditEntries[1].readerKind).toBe('elf')
    expect(auditEntries[2].readerKind).toBe('csv')
  })

  it('given CSV-only entries, when running, then audit entries contain only csv kind with sourceOrg sentinel', async () => {
    // Arrange — M4 gap: the CSV branch of buildAuditEntry was exercised in
    // the mixed test above, but isolating it catches a mutation that swaps
    // the csv / sobject / elf dispatch order. Assert the `<csv>` sentinel
    // explicitly so a StringLiteral mutation on that value is caught.
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: true })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    await sut.run([resolved(csv)], new Map())

    // Assert
    const auditEntries = vi.mocked(buildAuditChecks).mock.calls[0][0]
    expect(auditEntries).toHaveLength(1)
    expect(auditEntries[0].readerKind).toBe('csv')
    expect(auditEntries[0].sourceOrg).toBe('<csv>')
  })

  it('given empty entries, when running, then runAudit is still invoked with an empty list and result reports no failures', async () => {
    // Arrange — Zod guarantees entries.length >= 1 at the command layer,
    // but AuditRunner itself must behave sanely if fed an empty slice
    // (e.g. a future dispatch that filters all entries out).
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: true })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    const result = await sut.run([], new Map())

    // Assert
    expect(vi.mocked(buildAuditChecks).mock.calls[0][0]).toEqual([])
    expect(result.entriesFailed).toBe(0)
    expect(process.exitCode).toBeUndefined()
  })
})
