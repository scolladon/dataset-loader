import { describe, expect, it, vi } from 'vitest'
import { buildAuditChecks, runAudit } from '../../../src/domain/auditor.js'
import {
  type LoggerPort,
  type SalesforcePort,
} from '../../../src/ports/types.js'

function createMockSfPort(
  queryResult: 'ok' | 'fail' | 'empty' = 'ok'
): SalesforcePort {
  const queryFns = {
    ok: vi.fn(async () => ({
      totalSize: 1,
      done: true,
      records: [{ Id: '001' }],
    })),
    fail: vi.fn(async () => {
      throw new Error('access denied')
    }),
    empty: vi.fn(async () => ({
      totalSize: 0,
      done: true,
      records: [],
    })),
  }
  return {
    query: queryFns[queryResult],
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    getBlobStream: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
    apiVersion: '62.0',
  }
}

function createMockLogger(): LoggerPort {
  return { info: vi.fn(), warn: vi.fn(), debug: vi.fn() }
}

describe('buildAuditChecks', () => {
  it('given entries, when building checks, then includes auth check for all unique orgs', () => {
    // Arrange
    const entries = [
      { isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' },
      {
        isElf: false,
        sourceOrg: 'srcB',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['srcB', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const authChecks = sut.filter(c => c.label.includes('auth'))
    expect(authChecks.length).toBe(3)
  })

  it('given ELF entries, when building checks, then includes EventLogFile check for ELF source orgs', () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const elfChecks = sut.filter(c => c.label.includes('EventLogFile'))
    expect(elfChecks.length).toBe(1)
    expect(elfChecks[0].org).toBe('srcA')
  })

  it('given no ELF entries, when building checks, then skips EventLogFile check', () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const elfChecks = sut.filter(c => c.label.includes('EventLogFile'))
    expect(elfChecks.length).toBe(0)
  })

  it('given ELF file-target entry without targetOrg, when building checks, then includes auth and EventLogFile checks for sourceOrg', () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'srcA' }]
    const sfPorts = new Map([['srcA', createMockSfPort()]])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const authChecks = sut.filter(c => c.label.includes('auth'))
    expect(authChecks.length).toBe(1)
    expect(authChecks[0].org).toBe('srcA')
    const elfChecks = sut.filter(c => c.label.includes('EventLogFile'))
    expect(elfChecks.length).toBe(1)
    expect(elfChecks[0].org).toBe('srcA')
    const insightsChecks = sut.filter(c =>
      c.label.includes('InsightsExternalData')
    )
    expect(insightsChecks.length).toBe(0)
  })

  it('given sfPorts map missing an org entry, when executing check, then check returns false', async () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const authCheck = checks.find(
      c => c.org === 'srcA' && c.label.includes('auth')
    )!

    // Act
    const sut = await authCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given InsightsExternalData query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map([
      ['srcA', createMockSfPort('ok')],
      ['anaA', createMockSfPort('fail')],
    ])

    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    const sut = await insightsCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given EventLogFile check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const elfCheck = checks.find(c => c.label.includes('EventLogFile'))!

    // Act
    const sut = await elfCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given InsightsExternalData check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    const sut = await insightsCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given auth check, when executing, then queries with Organization SOQL', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [{ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }]
    const sfPorts = new Map([
      ['src', sfMock],
      ['ana', createMockSfPort()],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const authCheck = checks.find(
      c => c.org === 'src' && c.label.includes('auth')
    )!

    // Act
    await authCheck.execute()

    // Assert — kills 'SELECT Id FROM Organization LIMIT 1' mutation
    expect(sfMock.query).toHaveBeenCalledWith(
      'SELECT Id FROM Organization LIMIT 1'
    )
  })

  it('given ELF check, when executing, then queries with EventLogFile SOQL', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [{ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }]
    const sfPorts = new Map([
      ['src', sfMock],
      ['ana', createMockSfPort()],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const elfCheck = checks.find(c => c.label.includes('EventLogFile'))!

    // Act
    await elfCheck.execute()

    // Assert — kills 'SELECT Id FROM EventLogFile LIMIT 1' mutation
    expect(sfMock.query).toHaveBeenCalledWith(
      'SELECT Id FROM EventLogFile LIMIT 1'
    )
  })

  it('given InsightsExternalData check, when executing, then queries with InsightsExternalData SOQL', async () => {
    // Arrange
    const anaMock = createMockSfPort()
    const entries = [{ isElf: false, sourceOrg: 'src', targetOrg: 'ana' }]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', anaMock],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    await insightsCheck.execute()

    // Assert — kills 'SELECT Id FROM InsightsExternalData LIMIT 1' mutation
    expect(anaMock.query).toHaveBeenCalledWith(
      'SELECT Id FROM InsightsExternalData LIMIT 1'
    )
  })

  it('given analytic orgs, when building checks, then includes InsightsExternalData check', () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const insightsChecks = sut.filter(c =>
      c.label.includes('InsightsExternalData')
    )
    expect(insightsChecks.length).toBe(1)
    expect(insightsChecks[0].org).toBe('anaA')
  })
})

describe('runAudit', () => {
  it('given passing check, when running audit, then logs [PASS] with check label', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: auth and connectivity',
        execute: async () => true,
      },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert — kills 'PASS' string literal mutation
    expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('[PASS]'))
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('src: auth and connectivity')
    )
  })

  it('given all checks pass, when running audit, then logs "All checks passed"', async () => {
    // Arrange
    const checks = [
      { org: 'src', label: 'src: auth', execute: async () => true },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert — kills 'All checks passed' string literal mutation
    expect(logger.info).toHaveBeenCalledWith('All checks passed')
  })

  it('given failing check, when running audit, then logs "Some checks failed"', async () => {
    // Arrange
    const checks = [
      { org: 'src', label: 'src: auth', execute: async () => false },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert — kills 'Some checks failed' string literal mutation
    expect(logger.info).toHaveBeenCalledWith('Some checks failed')
  })

  it('given all checks pass, when running audit, then returns passed true', async () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }]
    const sfPorts = new Map([
      ['src', createMockSfPort('ok')],
      ['ana', createMockSfPort('ok')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const logger = createMockLogger()

    // Act
    const sut = await runAudit(checks, logger)

    // Assert
    expect(sut.passed).toBe(true)
    expect(logger.info).toHaveBeenCalled()
  })

  it('given check that throws, when running audit, then logs FAIL with check label', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: exploding check',
        execute: async () => {
          throw new Error('unexpected failure')
        },
      },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert
    expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('[FAIL]'))
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('src: exploding check')
    )
  })

  it('given check that throws, when running audit, then audit marks it as failed', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: exploding check',
        execute: async () => {
          throw new Error('unexpected failure')
        },
      },
    ]
    const logger = createMockLogger()

    // Act
    const sut = await runAudit(checks, logger)

    // Assert
    expect(sut.passed).toBe(false)
  })

  it('given auth check fails, when running audit, then returns passed false', async () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }]
    const sfPorts = new Map([
      ['src', createMockSfPort('fail')],
      ['ana', createMockSfPort('ok')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const logger = createMockLogger()

    // Act
    const sut = await runAudit(checks, logger)

    // Assert
    expect(sut.passed).toBe(false)
  })
})

describe('SObject read access check', () => {
  it('given SObject entries, when building checks, then includes check per unique (org, sObject)', () => {
    // Arrange
    const entries = [
      { isElf: false, sourceOrg: 'srcA', sObject: 'Account' },
      { isElf: false, sourceOrg: 'srcA', sObject: 'Contact' },
      { isElf: false, sourceOrg: 'srcA', sObject: 'Account' },
    ]
    const sfPorts = new Map([['srcA', createMockSfPort()]])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    expect(readChecks.length).toBe(2)
    expect(readChecks.map(c => c.label)).toEqual(
      expect.arrayContaining([
        'srcA: Account read access',
        'srcA: Contact read access',
      ])
    )
  })

  it('given no SObject entries, when building checks, then skips sObject read access check', () => {
    // Arrange
    const entries = [{ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    expect(readChecks.length).toBe(0)
  })

  it('given sObject check, when executing, then queries with correct sObject SOQL', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [{ isElf: false, sourceOrg: 'src', sObject: 'Account' }]
    const sfPorts = new Map([['src', sfMock]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    await readCheck.execute()

    // Assert
    expect(sfMock.query).toHaveBeenCalledWith('SELECT Id FROM Account LIMIT 1')
  })

  it('given sObject query succeeds, when executing check, then returns true', async () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'src', sObject: 'Account' }]
    const sfPorts = new Map([['src', createMockSfPort('ok')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut).toBe(true)
  })

  it('given sObject query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'src', sObject: 'Account' }]
    const sfPorts = new Map([['src', createMockSfPort('fail')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })

  it('given sObject check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'src', sObject: 'Account' }]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })
})

describe('Dataset ready check', () => {
  it('given entries with targetOrg and targetDataset, when building checks, then includes check per unique (org, dataset)', () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_One',
      },
      {
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_Two',
      },
      {
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_One',
      },
    ]
    const sfPorts = new Map([
      ['srcA', createMockSfPort()],
      ['anaA', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const datasetChecks = sut.filter(c => c.label.includes('dataset'))
    expect(datasetChecks.length).toBe(2)
    expect(datasetChecks.map(c => c.label)).toEqual(
      expect.arrayContaining([
        "anaA: dataset 'DS_One' ready",
        "anaA: dataset 'DS_Two' ready",
      ])
    )
  })

  it('given entries without targetDataset, when building checks, then skips dataset ready check', () => {
    // Arrange
    const entries = [{ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }]
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
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      },
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
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      },
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', createMockSfPort('ok')],
    ])
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut).toBe(true)
  })

  it('given dataset query returns 0 records, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      },
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
    expect(sut).toBe(false)
  })

  it('given dataset query throws, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      },
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
    expect(sut).toBe(false)
  })

  it('given dataset ready check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'MyDataset',
      },
    ]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const datasetCheck = checks.find(c => c.label.includes('dataset'))!

    // Act
    const sut = await datasetCheck.execute()

    // Assert
    expect(sut).toBe(false)
  })
})

describe('Combined checks', () => {
  it('given SObject entry targeting a dataset, when building checks, then produces both read access and dataset ready checks', () => {
    // Arrange
    const entries = [
      {
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        sObject: 'Account',
        targetDataset: 'DS_Account',
      },
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    const datasetChecks = sut.filter(c => c.label.includes('dataset'))
    expect(readChecks.length).toBe(1)
    expect(readChecks[0].label).toBe('src: Account read access')
    expect(datasetChecks.length).toBe(1)
    expect(datasetChecks[0].label).toBe("ana: dataset 'DS_Account' ready")
  })
})
