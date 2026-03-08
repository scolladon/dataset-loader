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
  const checks: AuditCheck[] = []

  const uniqueOrgs = new Set<string>()
  for (const entry of entries) {
    uniqueOrgs.add(entry.sourceOrg)
    uniqueOrgs.add(entry.analyticOrg)
  }

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

  const elfSourceOrgs = new Set(
    entries.filter(e => e.type === 'elf').map(e => e.sourceOrg)
  )
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

  const analyticOrgs = new Set(entries.map(e => e.analyticOrg))
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
  const results = await Promise.allSettled(
    checks.map(async check => {
      const passed = await check.execute()
      return { check, passed }
    })
  )

  for (const result of results) {
    if (result.status === 'rejected') {
      allPassed = false
      continue
    }
    const { check, passed } = result.value
    logger.info(`  [${passed ? 'PASS' : 'FAIL'}] ${check.label}`)
    if (!passed) allPassed = false
  }

  logger.info(allPassed ? 'All checks passed' : 'Some checks failed')
  return { passed: allPassed }
}
