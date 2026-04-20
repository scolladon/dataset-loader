import { describe, expect, it, vi } from 'vitest'
import {
  type AuditEntry,
  buildAuditChecks,
  runAudit,
} from '../../../src/domain/auditor.js'
import {
  type LoggerPort,
  type SalesforcePort,
} from '../../../src/ports/types.js'

// Adapter: pre-existing tests build entries with `{ isElf, sourceOrg, ... }`.
// The new `AuditEntry` shape (readerKind, augmentColumns, ...) is shipped
// for the schemaAlignment strategy. This helper preserves the legacy test
// inputs without rewriting every test body.
function legacyEntry(o: {
  isElf?: boolean
  sourceOrg: string
  targetOrg?: string
  sObject?: string
  targetDataset?: string
}): AuditEntry {
  return {
    readerKind: o.isElf ? 'elf' : o.sObject ? 'sobject' : 'csv',
    sourceOrg: o.sourceOrg,
    targetOrg: o.targetOrg,
    targetDataset: o.targetDataset,
    sObject: o.sObject,
    augmentColumns: {},
    eventType: o.isElf ? 'EventType' : undefined,
    interval: o.isElf ? 'Daily' : undefined,
  }
}

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
      legacyEntry({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
      legacyEntry({
        isElf: false,
        sourceOrg: 'srcB',
        targetOrg: 'anaA',
      }),
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
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
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
      legacyEntry({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      }),
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
    const entries = [legacyEntry({ isElf: true, sourceOrg: 'srcA' })]
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
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const authCheck = checks.find(
      c => c.org === 'srcA' && c.label.includes('auth')
    )!

    // Act
    const sut = await authCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given InsightsExternalData query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
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
    expect(sut.kind).toBe('fail')
  })

  it('given EventLogFile check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const elfCheck = checks.find(c => c.label.includes('EventLogFile'))!

    // Act
    const sut = await elfCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given InsightsExternalData check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()

    const checks = buildAuditChecks(entries, sfPorts)
    const insightsCheck = checks.find(c =>
      c.label.includes('InsightsExternalData')
    )!

    // Act
    const sut = await insightsCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given auth check, when executing, then queries with Organization SOQL', async () => {
    // Arrange
    const sfMock = createMockSfPort()
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }),
    ]
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
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }),
    ]
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
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'src', targetOrg: 'ana' }),
    ]
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
      legacyEntry({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
      }),
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
        execute: async () => ({ kind: 'pass' as const }),
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

  it('given warning check, when running audit, then logs [WARN] with the message and still reports passed', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: dataset schema alignment',
        execute: async () => ({
          kind: 'warn' as const,
          message: 'casing differs',
        }),
      },
    ]
    const logger = createMockLogger()

    // Act
    const result = await runAudit(checks, logger)

    // Assert — WARN label is emitted and overall result is still passed
    expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('[WARN]'))
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('casing differs')
    )
    expect(result.passed).toBe(true)
  })

  it('given all checks pass, when running audit, then logs "All checks passed"', async () => {
    // Arrange
    const checks = [
      {
        org: 'src',
        label: 'src: auth',
        execute: async () => ({ kind: 'pass' as const }),
      },
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
      {
        org: 'src',
        label: 'src: auth',
        execute: async () => ({ kind: 'fail' as const, message: 'mock fail' }),
      },
    ]
    const logger = createMockLogger()

    // Act
    await runAudit(checks, logger)

    // Assert — kills 'Some checks failed' string literal mutation
    expect(logger.info).toHaveBeenCalledWith('Some checks failed')
  })

  it('given all checks pass, when running audit, then returns passed true', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }),
    ]
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
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'src', targetOrg: 'ana' }),
    ]
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
      legacyEntry({ isElf: false, sourceOrg: 'srcA', sObject: 'Account' }),
      legacyEntry({ isElf: false, sourceOrg: 'srcA', sObject: 'Contact' }),
      legacyEntry({ isElf: false, sourceOrg: 'srcA', sObject: 'Account' }),
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
    const entries = [
      legacyEntry({ isElf: true, sourceOrg: 'srcA', targetOrg: 'anaA' }),
    ]
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
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
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
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['src', createMockSfPort('ok')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given sObject query fails, when executing check, then returns false', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map([['src', createMockSfPort('fail')]])
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given sObject check with missing sfPort, when executing, then returns false', async () => {
    // Arrange
    const entries = [
      legacyEntry({ isElf: false, sourceOrg: 'src', sObject: 'Account' }),
    ]
    const sfPorts = new Map<string, SalesforcePort>()
    const checks = buildAuditChecks(entries, sfPorts)
    const readCheck = checks.find(c => c.label.includes('read access'))!

    // Act
    const sut = await readCheck.execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })
})

describe('Dataset ready check', () => {
  it('given entries with targetOrg and targetDataset, when building checks, then includes check per unique (org, dataset)', () => {
    // Arrange
    const entries = [
      legacyEntry({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_One',
      }),
      legacyEntry({
        isElf: false,
        sourceOrg: 'srcA',
        targetOrg: 'anaA',
        targetDataset: 'DS_Two',
      }),
      legacyEntry({
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
      legacyEntry({ isElf: false, sourceOrg: 'srcA', targetOrg: 'anaA' }),
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
      legacyEntry({
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
      legacyEntry({
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
      legacyEntry({
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
      legacyEntry({
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
      legacyEntry({
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

describe('schemaAlignment strategy', () => {
  // Helper: build a fake SF port whose `query` routes by SOQL substring,
  // so a single port can serve both the InsightsExternalData metadata
  // query and (for ELF) the LogFileFieldNames query.
  function metadataPort(opts: {
    metadataJson?: string | null
    logFileFieldNames?: string | null
    spy?: { metadataCalls: number }
  }): SalesforcePort {
    return {
      apiVersion: '62.0',
      query: vi.fn(async (soql: string) => {
        if (soql.includes('InsightsExternalData')) {
          if (opts.spy) opts.spy.metadataCalls++
          return opts.metadataJson === null
            ? { totalSize: 0, done: true, records: [] }
            : {
                totalSize: 1,
                done: true,
                records: [{ MetadataJson: '/blob/url' }],
              }
        }
        if (soql.includes('EventLogFile')) {
          return opts.logFileFieldNames === null
            ? { totalSize: 0, done: true, records: [] }
            : {
                totalSize: 1,
                done: true,
                records: [{ LogFileFieldNames: opts.logFileFieldNames }],
              }
        }
        return { totalSize: 0, done: true, records: [] }
        // biome-ignore lint/suspicious/noExplicitAny: fake mock
      }) as any,
      queryMore: vi.fn(),
      getBlob: vi.fn(async () => opts.metadataJson ?? '{}'),
      getBlobStream: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      del: vi.fn(),
    }
  }

  function findSchemaCheck(
    checks: ReturnType<typeof buildAuditChecks>,
    datasetName: string
  ) {
    return checks.find(c =>
      c.label.includes(`'${datasetName}' schema alignment`)
    )!
  }

  const fields = (...names: string[]) =>
    JSON.stringify({
      objects: [{ fields: names.map(n => ({ fullyQualifiedName: n })) }],
    })

  it('given SObject entry with matching field set, when running schema check, then PASS', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId', 'Name'],
        augmentColumns: { OrgId: '00D' },
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      [
        'ana',
        metadataPort({ metadataJson: fields('UserId', 'Name', 'OrgId') }),
      ],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given SObject entry with a missing dataset field, when running schema check, then FAIL listing the missing field', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: fields('UserId', 'Missing') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') expect(sut.message).toMatch(/Missing/)
  })

  it('given SObject entry with augment-vs-reader overlap, when running schema check, then FAIL listing the overlap', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId', 'OrgId'],
        augmentColumns: { OrgId: '00D' },
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: fields('UserId', 'OrgId') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') expect(sut.message).toMatch(/overlap/i)
  })

  it('given SObject entry with case-only diff, when running schema check, then WARN', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['userid', 'name'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: fields('UserId', 'Name') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('warn')
    if (sut.kind === 'warn') expect(sut.message).toMatch(/casing/i)
  })

  it('given ELF entry with matching order, when running schema check, then PASS', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'elf',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        eventType: 'Login',
        interval: 'Daily',
        augmentColumns: { OrgId: '00D' },
      },
    ]
    const sfPorts = new Map([
      [
        'src',
        metadataPort({
          metadataJson: null,
          logFileFieldNames: 'EVENT_TYPE,USER_ID',
        }),
      ],
      [
        'ana',
        metadataPort({
          metadataJson: fields('EVENT_TYPE', 'USER_ID', 'OrgId'),
        }),
      ],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given ELF entry with order mismatch, when running schema check, then FAIL with positional diff', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'elf',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        eventType: 'Login',
        interval: 'Daily',
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      [
        'src',
        metadataPort({
          metadataJson: null,
          logFileFieldNames: 'B,A', // dataset expects A,B
        }),
      ],
      ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') expect(sut.message).toMatch(/Order mismatch/)
  })

  it('given ELF entry with no prior EventLogFile, when running schema check, then WARN', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'elf',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        eventType: 'Login',
        interval: 'Daily',
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({ logFileFieldNames: null })],
      ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('warn')
    if (sut.kind === 'warn')
      expect(sut.message).toMatch(/no prior EventLogFile/i)
  })

  it('given dataset with no prior metadata, when running schema check, then PASS (datasetReady is the authoritative failure)', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: null })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert: schemaAlignment yields to datasetReady which already FAILed
    expect(sut.kind).toBe('pass')
  })

  it('given dataset metadata without objects[0].fields, when running schema check, then FAIL', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const malformed = JSON.stringify({ objects: [] })
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: malformed })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') expect(sut.message).toMatch(/objects\[0\]\.fields/)
  })

  it('given datasetReady runs ahead of schemaAlignment, when both execute, then each issues its own MetadataJson SOQL (no shared cache)', async () => {
    // Arrange — datasetReady uses a fast count-only query; schemaAlignment
    // fetches the blob. They are independent; each check makes its own call.
    const spy = { metadataCalls: 0 }
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: fields('UserId'), spy })],
    ])

    // Act
    const checks = buildAuditChecks(entries, sfPorts)
    const readyCheck = checks.find(c => c.label.includes("' ready"))!
    const schemaCheck = checks.find(c => c.label.includes('schema alignment'))!
    const [r1, r2] = await Promise.all([
      readyCheck.execute(),
      schemaCheck.execute(),
    ])

    // Assert — both pass; two independent metadata queries happened
    expect(r1.kind).toBe('pass')
    expect(r2.kind).toBe('pass')
    expect(spy.metadataCalls).toBe(2)
  })

  it('given CSV entry with missing file path, when running schema check, then FAIL', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'csv',
        sourceOrg: '<csv>',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        augmentColumns: {},
        csvFile: undefined,
      },
    ]
    const sfPorts = new Map([
      ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
    if (sut.kind === 'fail') expect(sut.message).toMatch(/could not be read/)
  })

  it('given CSV entry with non-existent file, when running schema check, then FAIL', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'csv',
        sourceOrg: '<csv>',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        augmentColumns: {},
        csvFile: '/tmp/this-file-does-not-exist-xyz.csv',
      },
    ]
    const sfPorts = new Map([
      ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given CSV entry matching dataset schema, when running schema check, then PASS', async () => {
    // Arrange — write a real temp CSV so the fs path is covered
    const { writeFileSync, mkdtempSync, rmSync } = await import('node:fs')
    const { tmpdir } = await import('node:os')
    const { join } = await import('node:path')
    const dir = mkdtempSync(join(tmpdir(), 'auditor-csv-'))
    const file = join(dir, 'in.csv')
    writeFileSync(file, 'A,B\n1,2\n')
    try {
      const entries: AuditEntry[] = [
        {
          readerKind: 'csv',
          sourceOrg: '<csv>',
          targetOrg: 'ana',
          targetDataset: 'DS_X',
          augmentColumns: {},
          csvFile: file,
        },
      ]
      const sfPorts = new Map([
        ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
      ])

      // Act
      const sut = await findSchemaCheck(
        buildAuditChecks(entries, sfPorts),
        'DS_X'
      ).execute()

      // Assert
      expect(sut.kind).toBe('pass')
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  it('given ELF entry with casing-only diff vs dataset metadata, when running schema check, then WARN', async () => {
    // Arrange — provided and expected sets match case-insensitively but not case-sensitively
    const entries: AuditEntry[] = [
      {
        readerKind: 'elf',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        eventType: 'Login',
        interval: 'Daily',
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      [
        'src',
        metadataPort({
          metadataJson: null,
          logFileFieldNames: 'event_type,user_id', // lowercase
        }),
      ],
      [
        'ana',
        metadataPort({
          metadataJson: fields('EVENT_TYPE', 'USER_ID'), // uppercase
        }),
      ],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('warn')
    if (sut.kind === 'warn') expect(sut.message).toMatch(/casing/i)
  })

  it('given malformed metadata (not an object), when running schema check, then FAIL', async () => {
    // Arrange — metadata is a JSON array, not an object
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: '[]' })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given metadata with non-array fields, when running schema check, then FAIL', async () => {
    // Arrange — objects[0].fields is a string, not an array
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const malformed = JSON.stringify({ objects: [{ fields: 'oops' }] })
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: malformed })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given metadata field with empty-string fullyQualifiedName, when running schema check, then FAIL', async () => {
    // Arrange — fullyQualifiedName is '' — hits the `name.length === 0` branch
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const malformed = JSON.stringify({
      objects: [{ fields: [{ fullyQualifiedName: '' }] }],
    })
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: malformed })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given unparseable metadata JSON, when running schema check, then FAIL', async () => {
    // Arrange — invalid JSON in MetadataJson blob
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: 'not json at all' })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('fail')
  })

  it('given ELF entry with missing sourceOrg connection, when running schema check, then WARN (no prior blob)', async () => {
    // Arrange — sourceOrg not present in sfPorts map
    const entries: AuditEntry[] = [
      {
        readerKind: 'elf',
        sourceOrg: 'missing-src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        eventType: 'Login',
        interval: 'Daily',
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['ana', metadataPort({ metadataJson: fields('A', 'B') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('warn')
  })

  it('given getBlob returning a non-string object, when fetching metadata, then stringifies for downstream parse', async () => {
    // Arrange — getBlob returns an already-parsed object (Salesforce client
    // may deserialize blobs for the caller). fetchMetadata must stringify.
    const asObject = {
      objects: [{ fields: [{ fullyQualifiedName: 'UserId' }] }],
    }
    const port: SalesforcePort = {
      apiVersion: '62.0',
      query: vi.fn(async () => ({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
        // biome-ignore lint/suspicious/noExplicitAny: fake mock
      })) as any,
      queryMore: vi.fn(),
      getBlob: vi.fn(async () => asObject),
      getBlobStream: vi.fn(),
      post: vi.fn(),
      patch: vi.fn(),
      del: vi.fn(),
    }
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: {},
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', port],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut.kind).toBe('pass')
  })

  it('given config matching dataset case-sensitively, when running schema check, then PASS without casing diff warning', async () => {
    // Arrange
    const entries: AuditEntry[] = [
      {
        readerKind: 'sobject',
        sourceOrg: 'src',
        targetOrg: 'ana',
        targetDataset: 'DS_X',
        sObject: 'User',
        readerFields: ['UserId'],
        augmentColumns: { OrgId: '00D' },
      },
    ]
    const sfPorts = new Map([
      ['src', metadataPort({})],
      ['ana', metadataPort({ metadataJson: fields('UserId', 'OrgId') })],
    ])

    // Act
    const sut = await findSchemaCheck(
      buildAuditChecks(entries, sfPorts),
      'DS_X'
    ).execute()

    // Assert
    expect(sut).toEqual({ kind: 'pass' })
  })
})

describe('Combined checks', () => {
  it('given SObject entry targeting a dataset, when building checks, then produces both read access and dataset ready checks', () => {
    // Arrange
    const entries = [
      legacyEntry({
        isElf: false,
        sourceOrg: 'src',
        targetOrg: 'ana',
        sObject: 'Account',
        targetDataset: 'DS_Account',
      }),
    ]
    const sfPorts = new Map([
      ['src', createMockSfPort()],
      ['ana', createMockSfPort()],
    ])

    // Act
    const sut = buildAuditChecks(entries, sfPorts)

    // Assert
    const readChecks = sut.filter(c => c.label.includes('read access'))
    const readyChecks = sut.filter(c => c.label.includes("' ready"))
    expect(readChecks.length).toBe(1)
    expect(readChecks[0].label).toBe('src: Account read access')
    expect(readyChecks.length).toBe(1)
    expect(readyChecks[0].label).toBe("ana: dataset 'DS_Account' ready")
  })
})
