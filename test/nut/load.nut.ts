import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import os from 'node:os'
import { join } from 'node:path'
import { MockTestOrgData, TestContext } from '@salesforce/core/testSetup'
import { ensureJsonMap, ensureString } from '@salesforce/ts-types'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import CrmaLoad from '../../src/commands/crma/load.js'
import {
  FakeConnectionBuilder,
  type RequestHandler,
} from '../fixtures/fake-connection-builder.js'

const OCLIF_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGBREAK', 'SIGHUP'] as const

// ── Test data constants ───────────────────────────────────────────────
const INSIGHTS_DATA_PREFIX = '0Ib'
const INSIGHTS_PART_PREFIX = '0Ic'
const LOG_DATE_MAR_01 = '2026-03-01T00:00:00.000+0000'
const LOG_DATE_MAR_02 = '2026-03-02T00:00:00.000+0000'
const LOG_DATE_MAR_03 = '2026-03-03T00:00:00.000+0000'
const WATERMARK_DATE = '2026-03-01T00:00:00.000Z'
const ELF_CSV_HEADERS = ['EVENT_TYPE', 'USER_ID']
const LARGE_ROW_COUNT = 5000
const SOBJECT_PAGE_SIZE = 2000
const SOBJECT_PAGE2_START = 2000
const SOBJECT_PAGE2_END = 3500
const LARGE_PART_ROW_COUNT = 4000
const LARGE_PART_RANDOM_BYTES = 4096

function captureSignalListeners(): Map<string, NodeJS.SignalsListener[]> {
  const snapshot = new Map<string, NodeJS.SignalsListener[]>()
  for (const signal of OCLIF_SIGNALS) {
    snapshot.set(signal, [
      ...(process.listeners(signal) as NodeJS.SignalsListener[]),
    ])
  }
  return snapshot
}

function removeLeakedSignalListeners(
  before: Map<string, NodeJS.SignalsListener[]>
): void {
  for (const signal of OCLIF_SIGNALS) {
    const previous = new Set(before.get(signal) ?? [])
    for (const listener of process.listeners(
      signal
    ) as NodeJS.SignalsListener[]) {
      if (!previous.has(listener)) {
        process.removeListener(signal, listener)
      }
    }
  }
}

async function runCommand(argv: string[]): Promise<unknown> {
  const before = captureSignalListeners()
  try {
    return await CrmaLoad.run(argv, process.cwd())
  } finally {
    removeLeakedSignalListeners(before)
  }
}

function elfEntry(overrides: Record<string, unknown> = {}) {
  return {
    type: 'elf' as const,
    sourceOrg: 'src-org',
    analyticOrg: 'ana-org',
    dataset: 'DS',
    eventType: 'Login',
    interval: 'Daily' as const,
    ...overrides,
  }
}

function sobjectEntry(overrides: Record<string, unknown> = {}) {
  return {
    type: 'sobject' as const,
    sourceOrg: 'src-org',
    analyticOrg: 'ana-org',
    dataset: 'DS2',
    sobject: 'Account',
    fields: ['Id', 'Name'],
    dateField: 'LastModifiedDate',
    ...overrides,
  }
}

function fileElfEntry(
  outputPath: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    type: 'elf' as const,
    sourceOrg: 'src-org',
    dataset: outputPath,
    eventType: 'Login',
    interval: 'Daily' as const,
    ...overrides,
  }
}

function fileSObjectEntry(
  outputPath: string,
  overrides: Record<string, unknown> = {}
) {
  return {
    type: 'sobject' as const,
    sourceOrg: 'src-org',
    dataset: outputPath,
    sobject: 'Account',
    fields: ['Id', 'Name'],
    dateField: 'LastModifiedDate',
    ...overrides,
  }
}

function csvContent(headers: string[], rows: string[][]): string {
  const headerLine = headers.map(h => `"${h}"`).join(',')
  const dataLines = rows.map(r => r.map(c => `"${c}"`).join(','))
  return [headerLine, ...dataLines].join('\n') + '\n'
}

function makeConfigJson(entries: Record<string, unknown>[]): string {
  return JSON.stringify({ entries })
}

interface TempFiles {
  dir: string
  configPath: string
  statePath: string
}

function createTempFiles(configContent: string): TempFiles {
  const dir = mkdtempSync(join(os.tmpdir(), 'nut-'))
  const configPath = join(dir, 'config.json')
  const statePath = join(dir, 'state.json')
  writeFileSync(configPath, configContent)
  return { dir, configPath, statePath }
}

function readState(statePath: string): Record<string, string> {
  try {
    const raw = JSON.parse(readFileSync(statePath, 'utf-8'))
    return raw.watermarks ?? {}
  } catch (error: unknown) {
    if (
      error instanceof Error &&
      'code' in error &&
      (error as NodeJS.ErrnoException).code === 'ENOENT'
    ) {
      return {}
    }
    throw error
  }
}

describe('CrmaLoad NUT', () => {
  const $$ = new TestContext({ setup: false })
  const sourceOrg = new MockTestOrgData('src-test-id', { username: 'src-org' })
  const analyticOrg = new MockTestOrgData('ana-test-id', {
    username: 'ana-org',
  })
  const secondSourceOrg = new MockTestOrgData('src2-test-id', {
    username: 'src2-org',
  })
  const secondAnalyticOrg = new MockTestOrgData('ana2-test-id', {
    username: 'ana2-org',
  })

  let savedExitCode: number | undefined
  let tmp: TempFiles | undefined

  beforeEach(async () => {
    $$.init()
    await $$.stubAuths(
      sourceOrg,
      analyticOrg,
      secondSourceOrg,
      secondAnalyticOrg
    )
    $$.stubAliases({
      'src-org': sourceOrg.username,
      'ana-org': analyticOrg.username,
      'src2-org': secondSourceOrg.username,
      'ana2-org': secondAnalyticOrg.username,
    })

    savedExitCode = process.exitCode
    process.exitCode = undefined
  })

  afterEach(() => {
    process.exitCode = savedExitCode
    globalThis.fetch = originalFetch
    $$.restore()
    if (tmp) {
      rmSync(tmp.dir, { recursive: true, force: true })
      tmp = undefined
    }
  })

  // ── Response factories ──────────────────────────────────────────────

  function defaultOrgResponse() {
    return {
      totalSize: 1,
      done: true,
      records: [{ Id: '00D000000000001', Name: 'TestOrg' }],
    }
  }

  function defaultElfQueryResponse(logDates: string[]) {
    return {
      totalSize: logDates.length,
      done: true,
      records: logDates.map((d, i) => ({
        Id: `07l000000000${String(i).padStart(3, '0')}`,
        LogDate: d,
        LogFile: `/logfile${i}`,
      })),
    }
  }

  function defaultSObjectQueryResponse(
    records: Record<string, unknown>[],
    done = true,
    nextRecordsUrl?: string
  ) {
    return { totalSize: records.length, done, nextRecordsUrl, records }
  }

  function defaultInsightsQueryResponse() {
    return { totalSize: 0, done: true, records: [] }
  }

  const METADATA_BLOB_URL =
    '/services/data/v65.0/sobjects/InsightsExternalData/06Vmeta/MetadataJson'
  const DEFAULT_METADATA_JSON = JSON.stringify({
    objects: [{ name: 'DS', numberOfLinesToIgnore: 1 }],
  })

  function defaultMetadataQueryResponse() {
    return {
      totalSize: 1,
      done: true,
      records: [{ MetadataJson: METADATA_BLOB_URL }],
    }
  }

  function defaultCreateResponse(prefix: string) {
    return { id: `${prefix}000000000001` }
  }

  // ── Connection bridge ───────────────────────────────────────────────

  const originalFetch = globalThis.fetch

  function applyConnection(handler: RequestHandler): void {
    $$.fakeConnectionRequest = request => {
      const reqMap = ensureJsonMap(request)
      const url = ensureString(reqMap.url)
      const method = ensureString(reqMap.method ?? 'GET')
      const body = reqMap.body as string | undefined
      const value = handler(url, method, body)
      return Promise.resolve(value) as Promise<
        ReturnType<typeof $$.fakeConnectionRequest>
      >
    }
    // Mock fetch for getBlobStream (true HTTP streaming bypasses jsforce)
    globalThis.fetch = async (input: string | URL | Request) => {
      const url =
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url
      const path = new URL(url).pathname
      const value = handler(path, 'GET', undefined)
      const text = typeof value === 'string' ? value : JSON.stringify(value)
      return new Response(text, { status: 200 })
    }
  }

  // ── Scenario helpers ────────────────────────────────────────────────

  function orgOnlyConnection(): void {
    applyConnection(
      new FakeConnectionBuilder().withFallback(defaultOrgResponse()).build()
    )
  }

  function elfPipeline(elfResponse: unknown, elfCsv: string): void {
    applyConnection(
      new FakeConnectionBuilder()
        .onQuery('Organization')
        .returns(defaultOrgResponse())
        .onQuery('EventLogFile')
        .excluding('InsightsExternalData')
        .returns(elfResponse)
        .onGet('/sobjects/EventLogFile/')
        .including('/LogFile')
        .returns(elfCsv)
        .onQuery('InsightsExternalData')
        .including('MetadataJson')
        .returns(defaultMetadataQueryResponse())
        .onGet(METADATA_BLOB_URL)
        .returns(DEFAULT_METADATA_JSON)
        .onQuery('InsightsExternalData')
        .including('EdgemartAlias')
        .returns(defaultInsightsQueryResponse())
        .onPost('InsightsExternalData')
        .excluding('Part')
        .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
        .onPost('InsightsExternalDataPart')
        .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
        .onPatch('InsightsExternalData')
        .returns({ success: true })
        .build()
    )
  }

  function sobjectPipeline(
    sobjectName: string,
    sobjectResponse: unknown
  ): void {
    applyConnection(
      new FakeConnectionBuilder()
        .onQuery('Organization')
        .returns(defaultOrgResponse())
        .onQuery(sobjectName)
        .returns(sobjectResponse)
        .onQuery('InsightsExternalData')
        .including('MetadataJson')
        .returns(defaultMetadataQueryResponse())
        .onGet(METADATA_BLOB_URL)
        .returns(DEFAULT_METADATA_JSON)
        .onQuery('InsightsExternalData')
        .including('EdgemartAlias')
        .returns(defaultInsightsQueryResponse())
        .onPost('InsightsExternalData')
        .excluding('Part')
        .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
        .onPost('InsightsExternalDataPart')
        .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
        .onPatch('InsightsExternalData')
        .returns({ success: true })
        .build()
    )
  }

  // ── Tests ───────────────────────────────────────────────────────────

  describe('help output', () => {
    it('given command class, when inspecting summary, then summary describes the command purpose', () => {
      // Act
      const sut = CrmaLoad.summary

      // Assert
      expect(sut).toBeDefined()
      expect(sut).toContain('Load')
      expect(sut).toContain('CRMA')
    })

    it('given command class, when inspecting flags, then all expected flags are defined', () => {
      // Act
      const sut = CrmaLoad.flags

      // Assert
      expect(sut).toHaveProperty('config-file')
      expect(sut).toHaveProperty('state-file')
      expect(sut).toHaveProperty('audit')
      expect(sut).toHaveProperty('dry-run')
      expect(sut).toHaveProperty('entry')
    })

    it('given command class, when inspecting examples, then examples are provided', () => {
      // Act
      const sut = CrmaLoad.examples

      // Assert
      expect(sut).toBeDefined()
      expect(sut.length).toBeGreaterThan(0)
    })
  })

  describe('command validation', () => {
    it('given invalid org alias in config, when running command, then produces config loading error', async () => {
      // Arrange
      tmp = createTempFiles(
        makeConfigJson([elfEntry({ sourceOrg: 'org:with:colons' })])
      )

      // Act
      const sut = runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      await expect(sut).rejects.toThrow('Config loading failed')
    })

    it('given invalid config JSON file, when running command, then produces config loading error', async () => {
      // Arrange
      tmp = createTempFiles('{ not valid json }}}')

      // Act
      const sut = runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      await expect(sut).rejects.toThrow('Config loading failed')
    })

    it('given nonexistent config file path, when running command, then produces config loading error', async () => {
      // Act
      const sut = runCommand(['--config-file', '/nonexistent/path/config.json'])

      // Assert
      await expect(sut).rejects.toThrow('Config loading failed')
    })

    it('given valid config with entries, when specifying nonexistent entry name, then produces entry not found error', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      orgOnlyConnection()

      // Act
      const sut = runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--entry',
        'nonexistent',
      ])

      // Assert
      await expect(sut).rejects.toThrow(
        'Entry \'nonexistent\' not found. Ensure your config entries have a "name" field.'
      )
    })
  })

  describe('dry-run', () => {
    it('given valid config with entries, when running with dry-run flag, then returns zero counts without executing', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      orgOnlyConnection()

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--dry-run',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })

    it('given existing watermark in state file, when running with dry-run, then returns zero counts preserving state', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      writeFileSync(
        tmp.statePath,
        JSON.stringify({
          watermarks: { 'src-org:elf:Login:Daily': WATERMARK_DATE },
        })
      )
      orgOnlyConnection()

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--dry-run',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })

    it('given valid config with multiple named entries, when running with dry-run and entry filter by name, then returns zero counts for filtered entry', async () => {
      // Arrange
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ name: 'login-events', dataset: 'DS1' }),
          sobjectEntry({ name: 'accounts', dataset: 'DS2' }),
        ])
      )
      orgOnlyConnection()

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--dry-run',
        '--entry',
        'accounts',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })
  })

  describe('audit', () => {
    it('given audit flag with all checks passing, when running command, then returns zero failures', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      orgOnlyConnection()

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--audit',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })

    it('given audit flag with Organization query failing, when running command, then reports one failure with exit code 2', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .throws('AUTH_FAILED')
          .withFallback(defaultOrgResponse())
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--audit',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })

    it('given audit flag with EventLogFile permission failing, when running command, then reports one failure with exit code 2', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('EventLogFile')
          .throws('INSUFFICIENT_ACCESS')
          .withFallback(defaultOrgResponse())
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--audit',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })

    it('given audit flag with InsightsExternalData permission failing, when running command, then reports one failure with exit code 2', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('InsightsExternalData')
          .throws('INSUFFICIENT_ACCESS')
          .withFallback(defaultOrgResponse())
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--audit',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })
  })

  describe('e2e pipeline - ELF', () => {
    it('given valid ELF config with one log record, when running full pipeline, then processes one entry and persists watermark', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [
        ['Login', '005xx0000001'],
        ['Login', '005xx0000002'],
      ])
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      elfPipeline(defaultElfQueryResponse([LOG_DATE_MAR_01]), elfCsv)

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(readState(tmp.statePath)['src-org:elf:Login:Daily']).toBe(
        LOG_DATE_MAR_01
      )
    })

    it('given ELF query returns no records, when running pipeline, then skips entry with zero groups uploaded', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      elfPipeline({ totalSize: 0, done: true, records: [] }, '')

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 1,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })

    it('given ELF query throws error, when running pipeline, then marks entry as failed with exit code 2', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .throws('ELF_QUERY_FAILED')
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })
  })

  describe('e2e pipeline - SObject', () => {
    it('given valid SObject config with two records, when running full pipeline, then processes one entry and persists latest watermark', async () => {
      // Arrange
      const records = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
        {
          Id: '001000000000002',
          Name: 'Beta',
          LastModifiedDate: LOG_DATE_MAR_02,
        },
      ]
      tmp = createTempFiles(makeConfigJson([sobjectEntry()]))
      sobjectPipeline('Account', defaultSObjectQueryResponse(records))

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(readState(tmp.statePath)['src-org:sobject:Account']).toBe(
        LOG_DATE_MAR_02
      )
    })

    it('given SObject query returns no records, when running pipeline, then skips entry with zero groups uploaded', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([sobjectEntry()]))
      sobjectPipeline('Account', defaultSObjectQueryResponse([]))

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 1,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })

    it('given paginated SObject results across two pages, when running pipeline, then processes all pages and persists latest watermark', async () => {
      // Arrange
      const page1Records = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
      ]
      const page2Records = [
        {
          Id: '001000000000002',
          Name: 'Beta',
          LastModifiedDate: LOG_DATE_MAR_02,
        },
      ]
      const nextUrl = '/services/data/v65.0/query/01g000000000001-2000'
      tmp = createTempFiles(makeConfigJson([sobjectEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('Account')
          .returns(defaultSObjectQueryResponse(page1Records, false, nextUrl))
          .onGet('query/01g000000000001-2000')
          .returns(defaultSObjectQueryResponse(page2Records, true))
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(readState(tmp.statePath)['src-org:sobject:Account']).toBe(
        LOG_DATE_MAR_02
      )
    })
  })

  describe('multi-entry scenarios', () => {
    it('given ELF and SObject entries both succeeding, when running pipeline, then processes both with no exit code', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [['Login', '005xx0000001']])
      const sobjectRecords = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
      ]
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ dataset: 'DS1' }),
          sobjectEntry({ dataset: 'DS2' }),
        ])
      )
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .onQuery('Account')
          .returns(defaultSObjectQueryResponse(sobjectRecords))
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 2,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 2,
      })
      expect(process.exitCode).toBeUndefined()
    })

    it('given ELF succeeds and SObject throws, when running pipeline, then reports one failure with exit code >= 1', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [['Login', '005xx0000001']])
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ dataset: 'DS1' }),
          sobjectEntry({ dataset: 'DS2' }),
        ])
      )
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .onQuery('Account')
          .throws('SOBJECT_QUERY_FAILED')
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 1,
      })
      expect(process.exitCode).toBe(1)
    })

    it('given both ELF and SObject throw errors, when running pipeline, then reports two failures with exit code 2', async () => {
      // Arrange
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ dataset: 'DS1' }),
          sobjectEntry({ dataset: 'DS2' }),
        ])
      )
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .throws('ELF_FAILED')
          .onQuery('Account')
          .throws('SOBJECT_FAILED')
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 2,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })

    it('given two entries with entry filter selecting second by name, when running pipeline, then only named entry runs', async () => {
      // Arrange
      const sobjectRecords = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
      ]
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ dataset: 'DS1' }),
          sobjectEntry({ name: 'my-sobject', dataset: 'DS2' }),
        ])
      )
      sobjectPipeline('Account', defaultSObjectQueryResponse(sobjectRecords))

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--entry',
        'my-sobject',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
    })

    it('given two named entries, when filtering by name, then only matching entry runs', async () => {
      // Arrange
      const sobjectRecords = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
      ]
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry({ name: 'login-events', dataset: 'DS1' }),
          sobjectEntry({ name: 'accounts', dataset: 'DS2' }),
        ])
      )
      sobjectPipeline('Account', defaultSObjectQueryResponse(sobjectRecords))

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--entry',
        'accounts',
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
    })

    it('given entries with names, when filtering by nonexistent name, then throws entry not found', async () => {
      // Arrange
      tmp = createTempFiles(
        makeConfigJson([elfEntry({ name: 'login-events' })])
      )
      orgOnlyConnection()

      // Act
      const sut = runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
        '--entry',
        'nonexistent',
      ])

      // Assert
      await expect(sut).rejects.toThrow("Entry 'nonexistent' not found.")
    })
  })

  describe('multi-config complex scenario', () => {
    it('given five entries across four orgs with mixed types, when running pipeline, then all five succeed with correct watermarks', async () => {
      // Arrange
      const largeRows: string[][] = []
      for (let i = 0; i < LARGE_ROW_COUNT; i++) {
        largeRows.push([`Login`, `005xx${String(i).padStart(7, '0')}`])
      }
      const largeCsv = csvContent(ELF_CSV_HEADERS, largeRows)
      const sobjectRecordsPage1: Record<string, unknown>[] = []
      for (let i = 0; i < SOBJECT_PAGE_SIZE; i++) {
        sobjectRecordsPage1.push({
          Id: `001${String(i).padStart(12, '0')}`,
          Name: `Account_${i}`,
          LastModifiedDate: LOG_DATE_MAR_01,
        })
      }
      const sobjectRecordsPage2: Record<string, unknown>[] = []
      for (let i = SOBJECT_PAGE2_START; i < SOBJECT_PAGE2_END; i++) {
        sobjectRecordsPage2.push({
          Id: `001${String(i).padStart(12, '0')}`,
          Name: `Account_${i}`,
          LastModifiedDate: LOG_DATE_MAR_02,
        })
      }
      const entries = [
        elfEntry({
          sourceOrg: 'src-org',
          analyticOrg: 'ana-org',
          dataset: 'ElfDS1',
          eventType: 'Login',
        }),
        elfEntry({
          sourceOrg: 'src-org',
          analyticOrg: 'ana-org',
          dataset: 'ElfDS2',
          eventType: 'API',
        }),
        sobjectEntry({
          sourceOrg: 'src-org',
          analyticOrg: 'ana2-org',
          dataset: 'AcctDS',
          sobject: 'Account',
          fields: ['Id', 'Name'],
        }),
        sobjectEntry({
          sourceOrg: 'src2-org',
          analyticOrg: 'ana-org',
          dataset: 'CaseDS',
          sobject: 'Case',
          fields: ['Id', 'Subject'],
          dateField: 'LastModifiedDate',
        }),
        elfEntry({
          sourceOrg: 'src2-org',
          analyticOrg: 'ana2-org',
          dataset: 'ElfDS3',
          eventType: 'Report',
        }),
      ]
      tmp = createTempFiles(makeConfigJson(entries))
      const nextUrl = '/services/data/v65.0/query/sobject-page2'
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .calls(url => {
            if (url.includes('Login'))
              return defaultElfQueryResponse([LOG_DATE_MAR_01])
            if (url.includes('API'))
              return defaultElfQueryResponse([LOG_DATE_MAR_02])
            if (url.includes('Report'))
              return defaultElfQueryResponse([LOG_DATE_MAR_03])
            return defaultElfQueryResponse([LOG_DATE_MAR_01])
          })
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(largeCsv)
          .onQuery('Account')
          .returns(
            defaultSObjectQueryResponse(sobjectRecordsPage1, false, nextUrl)
          )
          .onGet('query/sobject-page2')
          .returns(defaultSObjectQueryResponse(sobjectRecordsPage2, true))
          .onQuery('Case')
          .excluding('InsightsExternalData')
          .returns(
            defaultSObjectQueryResponse([
              {
                Id: '500000000000001',
                Subject: 'Test Case',
                LastModifiedDate: LOG_DATE_MAR_01,
              },
            ])
          )
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 5,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 5,
      })
      expect(process.exitCode).toBeUndefined()
      const watermarks = readState(tmp.statePath)
      expect(watermarks['src-org:elf:Login:Daily']).toBe(LOG_DATE_MAR_01)
      expect(watermarks['src-org:elf:API:Daily']).toBe(LOG_DATE_MAR_02)
      expect(watermarks['src-org:sobject:Account']).toBe(LOG_DATE_MAR_02)
      expect(watermarks['src2-org:sobject:Case']).toBe(LOG_DATE_MAR_01)
      expect(watermarks['src2-org:elf:Report:Daily']).toBe(LOG_DATE_MAR_03)
    })

    it('given large CSV exceeding part size threshold, when running pipeline, then uploads multiple parts successfully', async () => {
      // Arrange
      const { randomBytes } = await import('node:crypto')
      const largeRows: string[][] = []
      for (let i = 0; i < LARGE_PART_ROW_COUNT; i++) {
        largeRows.push([
          `Login`,
          randomBytes(LARGE_PART_RANDOM_BYTES).toString('base64'),
        ])
      }
      const largeCsv = csvContent(ELF_CSV_HEADERS, largeRows)
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      let partCount = 0
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(largeCsv)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .calls(() => {
            partCount++
            return defaultCreateResponse(INSIGHTS_PART_PREFIX)
          })
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(partCount).toBeGreaterThanOrEqual(2)
    })
  })

  describe('watermark persistence', () => {
    it('given successful first run writes watermark, when second run finds no new records, then entry is skipped', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [['Login', '005xx0000001']])
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      elfPipeline(defaultElfQueryResponse([LOG_DATE_MAR_01]), elfCsv)

      // Act (first run — writes watermark)
      await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert (watermark persisted)
      expect(readState(tmp.statePath)['src-org:elf:Login:Daily']).toBe(
        LOG_DATE_MAR_01
      )

      // Arrange (second run — no new records)
      elfPipeline({ totalSize: 0, done: true, records: [] }, '')

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 1,
        entriesFailed: 0,
        groupsUploaded: 0,
      })
    })
  })

  describe('error handling', () => {
    it('given SF API connection error during ELF fetch, when running pipeline, then marks entry as failed with exit code', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .throws('API_ERROR: Connection refused')
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })

    it('given InsightsExternalData POST fails during upload, when running pipeline, then marks entry as failed with exit code', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [['Login', '005xx0000001']])
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .throws('UPLOAD_FAILED: Insufficient storage')
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 1,
        groupsUploaded: 0,
      })
      expect(process.exitCode).toBe(2)
    })

    it('given corrupted state file with invalid JSON, when running pipeline, then throws error', async () => {
      // Arrange
      tmp = createTempFiles(makeConfigJson([elfEntry()]))
      writeFileSync(tmp.statePath, '{ corrupted json !!!')
      orgOnlyConnection()

      // Act
      const sut = runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      await expect(sut).rejects.toThrow()
    })
  })

  describe('file-target dry-run', () => {
    it('given file-target ELF entry, when running with dry-run flag, then shows file: prefixed dataset key', async () => {
      // Arrange
      const outputPath = join(os.tmpdir(), 'nut-file-target-dryrun.csv')
      tmp = createTempFiles(makeConfigJson([fileElfEntry(outputPath)]))
      orgOnlyConnection()
      const loggedLines: string[] = []
      const originalLog = CrmaLoad.prototype.log
      CrmaLoad.prototype.log = (msg: string) => {
        loggedLines.push(msg ?? '')
      }

      // Act
      try {
        const sut = await runCommand([
          '--config-file',
          tmp.configPath,
          '--state-file',
          tmp.statePath,
          '--dry-run',
        ])

        // Assert
        expect(sut).toEqual({
          entriesProcessed: 0,
          entriesSkipped: 0,
          entriesFailed: 0,
          groupsUploaded: 0,
        })
        const planLine = loggedLines.find(l => l.includes('file:'))
        expect(planLine).toBeDefined()
        expect(planLine).toContain(`file:${outputPath}`)
      } finally {
        CrmaLoad.prototype.log = originalLog
      }
    })
  })

  describe('e2e pipeline - file-target', () => {
    it('given file-target ELF entry with one log record, when running pipeline, then writes CSV with header and data rows', async () => {
      // Arrange
      const elfCsv = csvContent(ELF_CSV_HEADERS, [
        ['Login', '005xx0000001'],
        ['Login', '005xx0000002'],
      ])
      const outputPath = join(os.tmpdir(), `nut-file-out-${Date.now()}.csv`)
      tmp = createTempFiles(makeConfigJson([fileElfEntry(outputPath)]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(existsSync(outputPath)).toBe(true)
      const written = readFileSync(outputPath, 'utf-8')
      expect(written).toContain('EVENT_TYPE')
      expect(written).toContain('USER_ID')
      expect(written).toContain('Login')
    })

    it('given file-target SObject entry with records, when running pipeline, then writes CSV with header and data', async () => {
      // Arrange
      const records = [
        {
          Id: '001000000000001',
          Name: 'Acme',
          LastModifiedDate: LOG_DATE_MAR_01,
        },
        {
          Id: '001000000000002',
          Name: 'Beta',
          LastModifiedDate: LOG_DATE_MAR_02,
        },
      ]
      const outputPath = join(os.tmpdir(), `nut-file-sobject-${Date.now()}.csv`)
      tmp = createTempFiles(makeConfigJson([fileSObjectEntry(outputPath)]))
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('Account')
          .returns(defaultSObjectQueryResponse(records))
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toEqual({
        entriesProcessed: 1,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 1,
      })
      expect(existsSync(outputPath)).toBe(true)
      const written = readFileSync(outputPath, 'utf-8')
      expect(written).toContain('Id,Name')
      expect(written).toContain('Acme')
    })
  })

  describe('e2e pipeline - reader fan-out', () => {
    it('given two entries sharing same ELF reader, when pipeline runs, then both targets receive data and watermarks advance', async () => {
      // Arrange — one CRMA org target + one file target, same ELF source
      const elfCsv = csvContent(ELF_CSV_HEADERS, [
        ['Login', '005xx0000001'],
        ['Login', '005xx0000002'],
      ])
      const outputPath = join(os.tmpdir(), `nut-fanout-${Date.now()}.csv`)
      tmp = createTempFiles(
        makeConfigJson([
          elfEntry(), // org target
          fileElfEntry(outputPath), // file target — same sourceOrg+eventType+interval
        ])
      )
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .onQuery('InsightsExternalData')
          .including('MetadataJson')
          .returns(defaultMetadataQueryResponse())
          .onGet(METADATA_BLOB_URL)
          .returns(DEFAULT_METADATA_JSON)
          .onQuery('InsightsExternalData')
          .including('EdgemartAlias')
          .returns(defaultInsightsQueryResponse())
          .onPost('InsightsExternalData')
          .excluding('Part')
          .returns(defaultCreateResponse(INSIGHTS_DATA_PREFIX))
          .onPost('InsightsExternalDataPart')
          .returns(defaultCreateResponse(INSIGHTS_PART_PREFIX))
          .onPatch('InsightsExternalData')
          .returns({ success: true })
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert — both entries processed
      expect(sut).toMatchObject({
        entriesProcessed: 2,
        entriesSkipped: 0,
        entriesFailed: 0,
      })
      // File target received data
      expect(existsSync(outputPath)).toBe(true)
      const written = readFileSync(outputPath, 'utf-8')
      expect(written).toContain('EVENT_TYPE')
      expect(written).toContain('Login')
      // Watermark written to state file
      const state = readState(tmp.statePath)
      const watermarkValues = Object.values(state)
      expect(watermarkValues.length).toBe(1) // both entries share the same watermarkKey (same ELF source)
      expect(
        watermarkValues.every(v => new Date(v) >= new Date(LOG_DATE_MAR_01))
      ).toBe(true)
    })

    it('given two file-target entries sharing same ELF reader, when pipeline runs, then both output files receive data', async () => {
      // Arrange — two different file targets, same ELF source
      const elfCsv = csvContent(ELF_CSV_HEADERS, [['Login', '005xx0000001']])
      const out1 = join(os.tmpdir(), `nut-fanout-1-${Date.now()}.csv`)
      const out2 = join(os.tmpdir(), `nut-fanout-2-${Date.now()}.csv`)
      tmp = createTempFiles(
        makeConfigJson([
          fileElfEntry(out1),
          fileElfEntry(out2), // same config → same reader
        ])
      )
      applyConnection(
        new FakeConnectionBuilder()
          .onQuery('Organization')
          .returns(defaultOrgResponse())
          .onQuery('EventLogFile')
          .excluding('InsightsExternalData')
          .returns(defaultElfQueryResponse([LOG_DATE_MAR_01]))
          .onGet('/sobjects/EventLogFile/')
          .including('/LogFile')
          .returns(elfCsv)
          .build()
      )

      // Act
      const sut = await runCommand([
        '--config-file',
        tmp.configPath,
        '--state-file',
        tmp.statePath,
      ])

      // Assert
      expect(sut).toMatchObject({
        entriesProcessed: 2,
        entriesSkipped: 0,
        entriesFailed: 0,
      })
      expect(readFileSync(out1, 'utf-8')).toContain('Login')
      expect(readFileSync(out2, 'utf-8')).toContain('Login')
    })
  })
})
