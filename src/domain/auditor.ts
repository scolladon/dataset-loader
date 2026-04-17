import {
  type LoggerPort,
  type QueryResult,
  type SalesforcePort,
} from '../ports/types.js'

interface AuditCheck {
  readonly org: string
  readonly label: string
  readonly execute: () => Promise<boolean>
}

interface AuditEntry {
  readonly isElf: boolean
  readonly sourceOrg: string
  readonly targetOrg?: string
  readonly sObject?: string
  readonly targetDataset?: string
}

interface AuditCheckStrategy {
  readonly select: (entry: AuditEntry) => { org: string; key: string }[]
  readonly label: (org: string, key: string) => string
  readonly evaluate: (sfPort: SalesforcePort, key: string) => Promise<boolean>
}

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
    return true
  },
}

const elfAccess: AuditCheckStrategy = {
  select: e => (e.isElf ? [{ org: e.sourceOrg, key: 'elf' }] : []),
  label: org => `${org}: EventLogFile access (ViewEventLogFiles)`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM EventLogFile LIMIT 1')
    return true
  },
}

const insightsAccess: AuditCheckStrategy = {
  select: e => (e.targetOrg ? [{ org: e.targetOrg, key: 'insights' }] : []),
  label: org => `${org}: InsightsExternalData access`,
  evaluate: async sfPort => {
    await sfPort.query('SELECT Id FROM InsightsExternalData LIMIT 1')
    return true
  },
}

// sObject values are validated against SF_IDENTIFIER_PATTERN at config parse boundary
const sobjectReadAccess: AuditCheckStrategy = {
  select: e => (e.sObject ? [{ org: e.sourceOrg, key: e.sObject }] : []),
  label: (org, key) => `${org}: ${key} read access`,
  evaluate: async (sfPort, key) => {
    await sfPort.query(`SELECT Id FROM ${key} LIMIT 1`)
    return true
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
    const result: QueryResult<unknown> = await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${key}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    return result.records.length > 0
  },
}

const STRATEGIES: readonly AuditCheckStrategy[] = [
  authConnectivity,
  elfAccess,
  insightsAccess,
  sobjectReadAccess,
  datasetReady,
]

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  return STRATEGIES.flatMap(s => buildChecks(entries, s, sfPorts))
}

function buildChecks(
  entries: readonly AuditEntry[],
  strategy: AuditCheckStrategy,
  sfPorts: ReadonlyMap<string, SalesforcePort>
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
          if (!sfPort) return false
          try {
            return await strategy.evaluate(sfPort, key)
          } catch {
            return false
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
  const promises: Promise<{ check: AuditCheck; passed: boolean }>[] = []
  for (const check of checks) {
    promises.push(
      check
        .execute()
        .then(passed => ({ check, passed }))
        .catch(() => ({ check, passed: false }))
    )
  }
  const results = await Promise.allSettled(promises)

  for (const result of results) {
    /* v8 ignore next -- allSettled always fulfills since each promise has .catch() */
    if (result.status !== 'fulfilled') continue
    const { check, passed } = result.value
    logger.info(`  [${passed ? 'PASS' : 'FAIL'}] ${check.label}`)
    if (!passed) allPassed = false
  }

  logger.info(allPassed ? 'All checks passed' : 'Some checks failed')
  return { passed: allPassed }
}
