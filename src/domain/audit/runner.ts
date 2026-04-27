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

// Each registered strategy is wrapped via `asBuilder` so its `Payload` type
// is captured at registration; the registry stores opaque builders that hide
// per-strategy generics from the runner.
type ChecksBuilder = (
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>,
  ctx: AuditContext
) => readonly AuditCheck[]

// `AuditCheckStrategy<P>` is a conditional type that makes `merge` required
// when P deviates from AuditEntry. Both branches share the same runtime shape
// (an `optional merge`); widening to this view at the strategy↔runner seam
// lets `buildChecks` consume a single non-conditional shape.
type StrategyView<P> = {
  readonly select: (
    entry: AuditEntry
  ) => readonly { org: string; key: string }[]
  readonly merge?: (existing: P | undefined, entry: AuditEntry) => P
  readonly label: (org: string, key: string) => string
  readonly evaluate: (
    sfPort: SalesforcePort,
    key: string,
    payload: P,
    ctx: AuditContext
  ) => Promise<import('../../ports/types.js').AuditOutcome>
}

function asBuilder<P>(strategy: AuditCheckStrategy<P>): ChecksBuilder {
  const view = strategy as StrategyView<P>
  return (entries, sfPorts, ctx) => buildChecks(entries, view, sfPorts, ctx)
}

const STRATEGIES: readonly ChecksBuilder[] = [
  asBuilder(authConnectivity),
  asBuilder(elfAccess),
  asBuilder(insightsAccess),
  asBuilder(sobjectFieldAccess),
  asBuilder(datasetReady),
  asBuilder(schemaAlignment),
]

export function buildAuditChecks(
  entries: readonly AuditEntry[],
  sfPorts: ReadonlyMap<string, SalesforcePort>
): readonly AuditCheck[] {
  const ctx: AuditContext = { sfPorts }
  return STRATEGIES.flatMap(build => build(entries, sfPorts, ctx))
}

function buildChecks<P>(
  entries: readonly AuditEntry[],
  strategy: StrategyView<P>,
  sfPorts: ReadonlyMap<string, SalesforcePort>,
  ctx: AuditContext
): readonly AuditCheck[] {
  // First-entry-wins default. The conditional type on AuditCheckStrategy
  // makes `merge` REQUIRED whenever Payload deviates from AuditEntry — so
  // when we land in this branch, P is provably AuditEntry and the cast is
  // identity at runtime. The cast is needed only because TS cannot project
  // the conditional-type guarantee into a generic `P` here.
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
    const detail = outcome.kind === 'pass' ? '' : `: ${outcome.message}`
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
