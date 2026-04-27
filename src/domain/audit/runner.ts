import {
  type AuditOutcome,
  formatErrorMessage,
  type LoggerPort,
  type SalesforcePort,
} from '../../ports/types.js'
import {
  type AuditCheck,
  type AuditCheckStrategy,
  type AuditContext,
  type AuditEntry,
  fail,
} from './audit-strategy.js'
import { authConnectivity } from './strategies/auth-connectivity.js'
import { datasetReady } from './strategies/dataset-ready.js'
import { elfAccess } from './strategies/elf-access.js'
import { insightsAccess } from './strategies/insights-access.js'
import { schemaAlignment } from './strategies/schema-alignment.js'
import { sobjectFieldAccess } from './strategies/sobject-field-access.js'

// Strategy registry. The unified `buildChecks` runs each strategy through the
// same select/merge/evaluate pipeline; `merge` defaults to first-entry-wins
// when omitted, so simple permission checks stay one-liners while FLS gets
// per-key field union for free.
const STRATEGIES: readonly AuditCheckStrategy<unknown>[] = [
  authConnectivity,
  elfAccess,
  insightsAccess,
  sobjectFieldAccess,
  datasetReady,
  schemaAlignment,
] as readonly AuditCheckStrategy<unknown>[]

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const ctx: AuditContext = { sfPorts }
  return STRATEGIES.flatMap(s => buildChecks(entries, s, sfPorts, ctx))
}

function buildChecks<P>(
  entries: readonly AuditEntry[],
  strategy: AuditCheckStrategy<P>,
  sfPorts: ReadonlyMap<string, SalesforcePort>,
  ctx: AuditContext
): readonly AuditCheck[] {
  // Default merge: first contributing entry wins (matches the legacy
  // strategy semantics, where evaluate() used to receive the first AuditEntry
  // that hit a given dedup key).
  const merge =
    strategy.merge ??
    ((existing: P | undefined, entry: AuditEntry): P =>
      existing ?? (entry as unknown as P))

  // Two-pass: aggregate per-key payloads, then build one check per key.
  const slots = new Map<string, { org: string; key: string; payload: P }>()
  for (const entry of entries) {
    for (const { org, key } of strategy.select(entry)) {
      const dedupKey = `${org}::${key}`
      const existing = slots.get(dedupKey)?.payload
      slots.set(dedupKey, { org, key, payload: merge(existing, entry) })
    }
  }

  const checks: AuditCheck[] = []
  for (const { org, key, payload } of slots.values()) {
    checks.push({
      org,
      label: strategy.label(org, key),
      execute: async () => {
        const sfPort = sfPorts.get(org)
        if (!sfPort) return fail(`No SF connection for org '${org}'`)
        try {
          return await strategy.evaluate(sfPort, key, payload, ctx)
        } catch (e) {
          return fail(formatErrorMessage(e))
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
