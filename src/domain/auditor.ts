import {
  type AuditOutcome,
  formatErrorMessage,
  type LoggerPort,
  type QueryResult,
  type ReaderKind,
  type SalesforcePort,
  SkipDatasetError,
} from '../ports/types.js'
import { parseCsvHeader } from './column-name.js'
import { checkSchemaAlignment } from './schema-check.js'
import { buildSObjectRowProjection } from './sobject-row-projection.js'

interface AuditCheck {
  readonly org: string
  readonly label: string
  readonly execute: () => Promise<AuditOutcome>
}

export interface AuditEntry {
  readonly sourceOrg: string
  readonly targetOrg?: string
  readonly targetDataset?: string
  readonly readerKind: ReaderKind
  readonly sObject?: string
  readonly readerFields?: readonly string[]
  readonly augmentColumns: Readonly<Record<string, string>>
  readonly eventType?: string
  readonly interval?: string
  readonly csvFile?: string
}

interface AuditContext {
  // Needed by schemaAlignment for ELF — the strategy's bound `sfPort` is the
  // target org's connection, but `EventLogFile.LogFileFieldNames` lives in
  // the source org. We look up the source port from this map at evaluate time.
  readonly sfPorts: ReadonlyMap<string, SalesforcePort>
}

interface AuditCheckStrategy {
  readonly select: (entry: AuditEntry) => { org: string; key: string }[]
  readonly label: (org: string, key: string) => string
  readonly evaluate: (
    sfPort: SalesforcePort,
    key: string,
    entry: AuditEntry,
    ctx: AuditContext
  ) => Promise<AuditOutcome>
}

const pass = (): AuditOutcome => ({ kind: 'pass' })
const fail = (message: string): AuditOutcome => ({ kind: 'fail', message })
const warn = (message: string): AuditOutcome => ({ kind: 'warn', message })

const authConnectivity: AuditCheckStrategy = {
  select: e =>
    e.targetOrg
      ? [
          { org: e.sourceOrg, key: 'auth' },
          { org: e.targetOrg, key: 'auth' },
        ]
      : [{ org: e.sourceOrg, key: 'auth' }],
  label: org => `${org}: auth and connectivity`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM Organization LIMIT 1')
    return pass()
  },
}

const elfAccess: AuditCheckStrategy = {
  select: e =>
    e.readerKind === 'elf' ? [{ org: e.sourceOrg, key: 'elf' }] : [],
  label: org => `${org}: EventLogFile access (ViewEventLogFiles)`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM EventLogFile LIMIT 1')
    return pass()
  },
}

const insightsAccess: AuditCheckStrategy = {
  select: e => (e.targetOrg ? [{ org: e.targetOrg, key: 'insights' }] : []),
  label: org => `${org}: InsightsExternalData access`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM InsightsExternalData LIMIT 1')
    return pass()
  },
}

// sObject values are validated against SF_IDENTIFIER_PATTERN at config parse boundary
const sobjectReadAccess: AuditCheckStrategy = {
  select: e => (e.sObject ? [{ org: e.sourceOrg, key: e.sObject }] : []),
  label: (org, key) => `${org}: ${key} read access`,
  evaluate: async (sfPort, key) => {
    await sfPort.query(`SELECT Id FROM ${key} LIMIT 1`)
    return pass()
  },
}

// targetDataset values are validated against SF_IDENTIFIER_PATTERN at config parse boundary
const datasetReady: AuditCheckStrategy = {
  select: e =>
    e.targetOrg && e.targetDataset
      ? [{ org: e.targetOrg, key: e.targetDataset }]
      : [],
  label: (org, key) => `${org}: dataset '${key}' ready`,
  evaluate: async (sfPort, key) => {
    // Fast path: verify at least one completed-status record exists. The
    // actual metadata blob is fetched (and memoised) by schemaAlignment.
    const result: QueryResult<unknown> = await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${key}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    return result.records.length > 0
      ? pass()
      : fail(`Dataset '${key}' has no prior metadata`)
  },
}

const schemaAlignment: AuditCheckStrategy = {
  select: e =>
    e.targetOrg && e.targetDataset
      ? [{ org: e.targetOrg, key: e.targetDataset }]
      : [],
  label: (org, key) => `${org}: dataset '${key}' schema alignment`,
  evaluate: async (sfPort, key, entry, ctx) => {
    const metadata = await fetchMetadata(sfPort, key)
    if (!metadata) {
      // datasetReady already FAILed; nothing to compare against
      return pass()
    }
    const datasetFields = extractDatasetFields(metadata)
    if (!datasetFields) {
      return fail(
        `Dataset '${key}' metadata has no objects[0].fields; cannot enforce column alignment`
      )
    }

    const providedFields = await resolveProvidedFields(entry, ctx)
    if (providedFields === 'warn:no-prior-elf') {
      return warn(
        `No prior EventLogFile for ${entry.eventType}/${entry.interval}; schema check skipped`
      )
    }
    if (providedFields === 'fail:csv-missing') {
      return fail(
        `CSV file '${entry.csvFile}' could not be read for schema check`
      )
    }

    return runSchemaChecks(entry, key, datasetFields, providedFields)
  },
}

function runSchemaChecks(
  entry: AuditEntry,
  datasetName: string,
  datasetFields: readonly string[],
  providedFields: readonly string[]
): AuditOutcome {
  const augmentKeys = Object.keys(entry.augmentColumns)
  const overlap = detectOverlap(providedFields, augmentKeys)
  if (overlap.length > 0) {
    return fail(
      `Schema overlap for dataset '${datasetName}': augment columns also present as reader fields: [${overlap.join(', ')}]`
    )
  }

  if (entry.readerKind === 'sobject') {
    return runSObjectCheck(entry, datasetName, datasetFields, providedFields)
  }

  const provided = [...providedFields, ...augmentKeys]
  // `targetDataset` is guaranteed by the schemaAlignment selector.
  /* v8 ignore next */
  const entryLabel = entry.targetDataset ?? datasetName
  const result = checkSchemaAlignment({
    datasetName,
    entryLabel,
    expected: datasetFields,
    provided,
    checkOrder: true,
  })
  if (!result.ok) return fail(result.reason)
  if (result.casingDiff) {
    return warn(
      `Schema casing differs from dataset '${datasetName}' metadata; dataset will keep its canonical casing`
    )
  }
  return pass()
}

function runSObjectCheck(
  entry: AuditEntry,
  datasetName: string,
  datasetFields: readonly string[],
  providedFields: readonly string[]
): AuditOutcome {
  // `targetDataset` is guaranteed by the schemaAlignment selector.
  /* v8 ignore next */
  const entryLabel = entry.targetDataset ?? datasetName
  try {
    buildSObjectRowProjection({
      datasetName,
      entryLabel,
      readerFields: providedFields,
      augmentColumns: entry.augmentColumns,
      datasetFields,
    })
  } catch (err) {
    // buildSObjectRowProjection throws only SkipDatasetError. Any other
    // error is a plumbing bug — rethrow to let buildChecks surface it.
    /* v8 ignore next */
    if (!(err instanceof SkipDatasetError)) throw err
    return fail(err.message)
  }
  // Casing WARN for SObject: set-only match, but raw case-sensitive sets differ
  const exactProvided = new Set([
    ...providedFields.map(f => f.replace(/\./g, '_')),
    ...Object.keys(entry.augmentColumns),
  ])
  const missesCase = datasetFields.some(n => !exactProvided.has(n))
  if (missesCase) {
    return warn(
      `Schema casing differs from dataset '${datasetName}' metadata; dataset will keep its canonical casing`
    )
  }
  return pass()
}

function detectOverlap(
  readerFields: readonly string[],
  augmentKeys: readonly string[]
): string[] {
  const readerNormalized = new Set(
    readerFields.map(f => f.replace(/\./g, '_').toLowerCase())
  )
  return augmentKeys.filter(k =>
    readerNormalized.has(k.replace(/\./g, '_').toLowerCase())
  )
}

async function resolveProvidedFields(
  entry: AuditEntry,
  ctx: AuditContext
): Promise<readonly string[] | 'warn:no-prior-elf' | 'fail:csv-missing'> {
  if (entry.readerKind === 'sobject') {
    // readerFields is always populated for SObject entries (commands layer
    // sets it from config.fields). Fallback is defensive.
    /* v8 ignore next */
    return entry.readerFields ?? []
  }
  if (entry.readerKind === 'elf') {
    const sourcePort = ctx.sfPorts.get(entry.sourceOrg)
    if (!sourcePort) return 'warn:no-prior-elf'
    // Safe interpolation: eventType is SF_IDENTIFIER_PATTERN-constrained,
    // interval is z.enum(['Daily','Hourly']) — both validated at config
    // parse (config-loader.ts:175-180). Neither admits single quotes.
    const result = await sourcePort.query<{ LogFileFieldNames: string | null }>(
      `SELECT LogFileFieldNames FROM EventLogFile WHERE EventType = '${entry.eventType}' AND Interval = '${entry.interval}' ORDER BY LogDate DESC LIMIT 1`
    )
    const raw = result.records[0]?.LogFileFieldNames
    if (!raw) return 'warn:no-prior-elf'
    return parseCsvHeader(raw)
  }
  // CSV
  if (!entry.csvFile) return 'fail:csv-missing'
  try {
    const { readFile } = await import('node:fs/promises')
    const content = await readFile(entry.csvFile, 'utf-8')
    const firstLine = content.split('\n', 1)[0]
    return parseCsvHeader(firstLine)
  } catch {
    return 'fail:csv-missing'
  }
}

function extractDatasetFields(metadataJson: string): readonly string[] | null {
  try {
    const parsed: unknown = JSON.parse(metadataJson)
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))
      return null
    const objects = (parsed as { objects?: unknown }).objects
    if (!Array.isArray(objects) || objects.length === 0) return null
    const obj0 = objects[0] as { fields?: unknown }
    if (!Array.isArray(obj0.fields) || obj0.fields.length === 0) return null
    const names: string[] = []
    for (const f of obj0.fields) {
      const name = (f as { fullyQualifiedName?: unknown }).fullyQualifiedName
      if (typeof name !== 'string' || name.length === 0) return null
      names.push(name)
    }
    return names
    /* v8 ignore next 3 -- JSON parse failures surface as null; defensive */
  } catch {
    return null
  }
}

async function fetchMetadata(
  sfPort: SalesforcePort,
  datasetName: string
): Promise<string | null> {
  const result: QueryResult<{ MetadataJson: string | null }> =
    await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${datasetName}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
  if (result.records.length === 0 || !result.records[0].MetadataJson)
    return null
  const blob = await sfPort.getBlob(result.records[0].MetadataJson)
  return typeof blob === 'string' ? blob : JSON.stringify(blob)
}

const STRATEGIES: readonly AuditCheckStrategy[] = [
  authConnectivity,
  elfAccess,
  insightsAccess,
  sobjectReadAccess,
  datasetReady,
  schemaAlignment,
]

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const ctx: AuditContext = { sfPorts }
  return STRATEGIES.flatMap(s => buildChecks(entries, s, sfPorts, ctx))
}

function buildChecks(
  entries: readonly AuditEntry[],
  strategy: AuditCheckStrategy,
  sfPorts: ReadonlyMap<string, SalesforcePort>,
  ctx: AuditContext
): readonly AuditCheck[] {
  const seen = new Set<string>()
  const checks: AuditCheck[] = []

  for (const entry of entries) {
    for (const { org, key } of strategy.select(entry)) {
      const dedupKey = `${org}::${key}`
      if (seen.has(dedupKey)) continue
      seen.add(dedupKey)
      checks.push({
        org,
        label: strategy.label(org, key),
        execute: async () => {
          const sfPort = sfPorts.get(org)
          if (!sfPort) return fail(`No SF connection for org '${org}'`)
          try {
            return await strategy.evaluate(sfPort, key, entry, ctx)
          } catch (e) {
            return fail(formatErrorMessage(e))
          }
        },
      })
    }
  }
  return checks
}

export async function runAudit(
  checks: readonly AuditCheck[],
  logger: LoggerPort
): Promise<{ readonly passed: boolean }> {
  let allPassed = true
  const promises: Promise<{ check: AuditCheck; outcome: AuditOutcome }>[] = []
  for (const check of checks) {
    promises.push(
      check
        .execute()
        .then(outcome => ({ check, outcome }))
        .catch(e => ({
          check,
          outcome: fail(formatErrorMessage(e)) satisfies AuditOutcome,
        }))
    )
  }
  const results = await Promise.allSettled(promises)

  for (const result of results) {
    /* v8 ignore next -- allSettled always fulfills since each promise has .catch() */
    if (result.status !== 'fulfilled') continue
    const { check, outcome } = result.value
    const label = outcomeLabel(outcome)
    const detail = outcome.kind === 'pass' ? '' : `: ${outcomeMessage(outcome)}`
    logger.info(`  [${label}] ${check.label}${detail}`)
    if (outcome.kind === 'fail') allPassed = false
  }

  logger.info(allPassed ? 'All checks passed' : 'Some checks failed')
  return { passed: allPassed }
}

function outcomeLabel(outcome: AuditOutcome): 'PASS' | 'WARN' | 'FAIL' {
  if (outcome.kind === 'pass') return 'PASS'
  if (outcome.kind === 'warn') return 'WARN'
  return 'FAIL'
}

function outcomeMessage(outcome: AuditOutcome): string {
  /* v8 ignore next -- pass path is handled by the caller */
  if (outcome.kind === 'pass') return ''
  return outcome.message
}
