import { describe, expect, it, vi } from 'vitest'
import {
  type CsvEntry,
  type ElfEntry,
  type ResolvedEntry,
  type SObjectEntry,
} from '../../../src/adapters/config-loader.js'
import { AuditRunner } from '../../../src/application/audit-runner.js'
import {
  type LoggerPort,
  type SalesforcePort,
} from '../../../src/ports/types.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

vi.mock('../../../src/domain/auditor.js', () => ({
  buildAuditChecks: vi.fn(() => []),
  runAudit: vi.fn().mockResolvedValue({ passed: true }),
}))

import { buildAuditChecks, runAudit } from '../../../src/domain/auditor.js'

const sobject: SObjectEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  sObject: 'Account',
  fields: ['Id', 'Name'],
  dateField: 'LastModifiedDate',
}

const elf: ElfEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  eventLog: 'Login',
  interval: 'Daily',
}

const csv: CsvEntry = {
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  csvFile: './fake.csv',
}

function resolved(
  entry: SObjectEntry | ElfEntry | CsvEntry,
  index = 0
): ResolvedEntry {
  return { entry, index, augmentColumns: {} }
}

function makeLogger() {
  const logs: string[] = []
  const warns: string[] = []
  const logger: LoggerPort = {
    info: (m: string) => logs.push(m),
    warn: (m: string) => warns.push(m),
    debug: (_m: string) => {
      /* no-op */
    },
  }
  return { logger, logs, warns }
}

describe('AuditRunner', () => {
  it('given passing audit, when running, then returns zero failures and does not set exit code', async () => {
    // Arrange
    process.exitCode = undefined
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
    process.exitCode = undefined
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: false })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    try {
      // Act
      const result = await sut.run([resolved(sobject)], new Map())

      // Assert
      expect(result.entriesFailed).toBe(1)
      expect(process.exitCode).toBe(2)
    } finally {
      process.exitCode = undefined
    }
  })

  it('given mixed entries, when building audit entries, then each kind gets the correct readerKind', async () => {
    // Arrange — regression guard on the SObject / ELF / CSV discrimination
    const buildSpy = vi.mocked(buildAuditChecks)
    buildSpy.mockClear()
    vi.mocked(runAudit).mockResolvedValueOnce({ passed: true })
    const { logger } = makeLogger()
    const sut = new AuditRunner(logger)

    // Act
    await sut.run([resolved(sobject), resolved(elf), resolved(csv)], new Map())

    // Assert
    const auditEntries = buildSpy.mock.calls[0][0]
    expect(auditEntries).toHaveLength(3)
    expect(auditEntries[0].readerKind).toBe('sobject')
    expect(auditEntries[1].readerKind).toBe('elf')
    expect(auditEntries[2].readerKind).toBe('csv')
  })
})
