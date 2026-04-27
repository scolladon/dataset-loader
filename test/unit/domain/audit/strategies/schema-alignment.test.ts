import { describe, expect, it, vi } from 'vitest'
import {
  buildAuditChecks,
  runAudit,
} from '../../../../../src/domain/audit/runner.js'
import {
  createMockLogger,
  createMockSfPort,
  legacyEntry,
} from '../../../../fixtures/audit.js'

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
