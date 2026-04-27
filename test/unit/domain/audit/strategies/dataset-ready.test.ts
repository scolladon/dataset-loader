import { describe, expect, it, vi } from 'vitest'
import {
  buildAuditChecks,
  runAudit,
} from '../../../../../src/domain/audit/runner.js'
import {
  auditEntryOf,
  createMockLogger,
  createMockSfPort,
} from '../../../../fixtures/audit.js'

describe('Dataset ready check', () => {
  it('given entries with targetOrg and targetDataset, when building checks, then includes check per unique (org, dataset)', () => {
    // Arrange
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_One',
      }),
      auditEntryOf({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_Two',
      }),
      auditEntryOf({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_One',
      }),
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readyChecks = sut.filter(c => c.label.includes("' ready"))
    expect(readyChecks.length).toBe(2)
    expect(readyChecks.map(c => c.label)).toEqual(
      expect.arrayContaining([
        "anaA: dataset 'DS_One' ready",
        "anaA: dataset 'DS_Two' ready",
      ])
    )
  })

  it('given entries without targetDataset, when building checks, then skips dataset ready check', () => {
    // Arrange
    const entries = [
      auditEntryOf({ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const datasetChecks = sut.filter(c => c.label.includes('dataset'))
    expect(datasetChecks.length).toBe(0)
  })

  it('given dataset ready check, when executing, then queries with correct InsightsExternalData SOQL', async () => {
    // Arrange
    const anaMock = createMockSfPort()
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      }),
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', anaMock],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    await datasetCheck.execute()

    // Assert
    expect(anaMock.query).toHaveBeenCalledWith(
      "SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = 'MyDataset' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1"
    )
  })

  it('given dataset query returns records, when executing check, then returns true', async () => {
    // Arrange
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      }),
    ]
    const sfPort = createMockSfPort('ok')
    sfPort.query = vi.fn(async () => ({
      totalSize: 1,
      done: true,
      records: [{ MetadataJson: '/blob/url' }],
    })) as SalesforcePort['query']
    sfPort.getBlob = vi.fn(async () => '{}')
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', sfPort],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes("' ready"))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given dataset query returns 0 records, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      }),
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', createMockSfPort('empty')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given dataset query throws, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      }),
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', createMockSfPort('fail')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given dataset ready check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      auditEntryOf({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })
})
