import { type LoggerPort, type SalesforcePort } from '../ports/types.js'

interface AuditCheck {
  readonly org: string
  readonly label: string
  readonly execute: () => Promise<boolean>
}

interface AuditEntry {
  readonly isElf: boolean
  readonly sourceOrg: string
  readonly targetOrg?: string
}

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  return [
    ...buildOrgChecks(
      collectOrgs(entries, e =>
        e.targetOrg ? [e.sourceOrg, e.targetOrg] : [e.sourceOrg]
      ),
      org => `${org}: auth and connectivity`,
      'SELECT Id FROM Organization LIMIT 1',
      sfPorts
    ),
    ...buildOrgChecks(
      collectOrgs(entries, e => (e.isElf ? [e.sourceOrg] : [])),
      org => `${org}: EventLogFile access (ViewEventLogFiles)`,
      'SELECT Id FROM EventLogFile LIMIT 1',
      sfPorts
    ),
    ...buildOrgChecks(
      collectOrgs(entries, e => (e.targetOrg ? [e.targetOrg] : [])),
      org => `${org}: InsightsExternalData access`,
      'SELECT Id FROM InsightsExternalData LIMIT 1',
      sfPorts
    ),
  ]
}

function collectOrgs(
  entries: readonly AuditEntry[],
  selector: (e: AuditEntry) => string[]
): Set<string> {
  const orgs = new Set<string>()
  for (const entry of entries) {
    for (const org of selector(entry)) {
      orgs.add(org)
    }
  }
  return orgs
}

function buildOrgChecks(
  orgs: Set<string>,
  labelFn: (org: string) => string,
  soql: string,
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  return [...orgs].map(org => {
    const sfPort = sfPorts.get(org)
    return {
      org,
      label: labelFn(org),
      execute: async () => {
        if (!sfPort) return false
        try {
          await sfPort.query(soql)
          return true
        } catch {
          return false
        }
      },
    }
  })
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
