import {
  type EntryType,
  type LoggerPort,
  type SalesforcePort,
} from '../ports/types.js'

export interface AuditCheck {
  readonly org: string
  readonly label: string
  readonly execute: () => Promise<boolean>
}

interface AuditEntry {
  readonly type: EntryType
  readonly sourceOrg: string
  readonly analyticOrg: string
}

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  return [
    ...buildAuthChecks(entries, sfPorts),
    ...buildElfChecks(entries, sfPorts),
    ...buildInsightsChecks(entries, sfPorts),
  ]
}

function buildAuthChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const uniqueOrgs = new Set<string>()
  for (const entry of entries) {
    uniqueOrgs.add(entry.sourceOrg)
    uniqueOrgs.add(entry.analyticOrg)
  }

  const checks: AuditCheck[] = []
  for (const org of uniqueOrgs) {
    const sfPort = sfPorts.get(org)
    checks.push({
      org,
      label: `${org}: auth and connectivity`,
      execute: async () => {
        if (!sfPort) return false
        try {
          await sfPort.query('SELECT Id FROM Organization LIMIT 1')
          return true
        } catch {
          return false
        }
      },
    })
  }
  return checks
}

function buildElfChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const elfSourceOrgs = new Set<string>()
  for (const entry of entries) {
    if (entry.type === 'elf') elfSourceOrgs.add(entry.sourceOrg)
  }

  const checks: AuditCheck[] = []
  for (const org of elfSourceOrgs) {
    const sfPort = sfPorts.get(org)
    checks.push({
      org,
      label: `${org}: EventLogFile access (ViewEventLogFiles)`,
      execute: async () => {
        if (!sfPort) return false
        try {
          await sfPort.query('SELECT Id FROM EventLogFile LIMIT 1')
          return true
        } catch {
          return false
        }
      },
    })
  }
  return checks
}

function buildInsightsChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const analyticOrgs = new Set<string>()
  for (const entry of entries) {
    analyticOrgs.add(entry.analyticOrg)
  }

  const checks: AuditCheck[] = []
  for (const org of analyticOrgs) {
    const sfPort = sfPorts.get(org)
    checks.push({
      org,
      label: `${org}: InsightsExternalData access`,
      execute: async () => {
        if (!sfPort) return false
        try {
          await sfPort.query('SELECT Id FROM InsightsExternalData LIMIT 1')
          return true
        } catch {
          return false
        }
      },
    })
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
    if (result.status !== 'fulfilled') continue
    const { check, passed } = result.value
    logger.info(`  [${passed ? 'PASS' : 'FAIL'}] ${check.label}`)
    if (!passed) allPassed = false
  }

  logger.info(allPassed ? 'All checks passed' : 'Some checks failed')
  return { passed: allPassed }
}
