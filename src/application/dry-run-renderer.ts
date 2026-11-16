import {
  type ConfigEntry,
  entryLabel,
  isCsvEntry,
  isElfEntry,
  type ResolvedEntry,
} from '../adapters/config-loader.js'
import { DatasetKey } from '../domain/dataset-key.js'
import { type DateBounds } from '../domain/date-bounds.js'
import { WatermarkKey } from '../domain/watermark-key.js'
import { type WatermarkStore } from '../domain/watermark-store.js'
import {
  type LoggerPort,
  type QueryResult,
  type SalesforcePort,
} from '../ports/types.js'
import { type DatasetLoadResult, EMPTY_RESULT } from './load-inputs.js'
import { computeWarnings, dryRunAnnotation } from './warnings.js'

// Renders the `--dry-run` dispatch output. Single render shape for every
// entry — two indented lines (label/key + watermark), plus a third
// `effective:` line when bounds are non-empty. `Configured window:` +
// blank separator fire only when bounds are non-empty.
//
// CSV entries always show `watermark: n/a (CSV entry — watermarks do not
// apply)` regardless of bounds: CSVs have no watermark concept, so the
// phrasing is about why the column is n/a, not about bounds.
//
// Warnings are emitted through the injected LoggerPort before the plan
// header so operators see them in the same terminal flow. Returns a
// zeroed result — dry-run never processes data.
export class DryRunRenderer {
  constructor(
    private readonly logger: LoggerPort,
    // Optional existence check — when provided, the renderer emits a
    // `[BOOTSTRAP]` annotation for dataset targets without a prior completed
    // load and `[OVERRIDE]` for entries that would replace an existing
    // dataset via `overrideMetadata: true`. Left undefined in audit-only or
    // unit-test paths so the renderer stays usable without an SF port.
    private readonly sfPorts?: ReadonlyMap<string, SalesforcePort>
  ) {}

  async render(
    entries: readonly ResolvedEntry[],
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): Promise<DatasetLoadResult> {
    for (const msg of computeWarnings(entries, watermarks, bounds)) {
      this.logger.warn(msg)
    }
    this.logger.info('Dry run — planned entries:')
    if (!bounds.isEmpty()) {
      this.logger.info(`Configured window: ${bounds.toString()}`)
      this.logger.info('')
    }
    const existenceByDataset = await this.precomputeExistence(entries)
    for (const { entry } of entries) {
      await this.renderEntry(entry, watermarks, bounds, existenceByDataset)
    }
    return EMPTY_RESULT
  }

  // Per-dataset existence check, keyed by `org datasetName`. One SOQL
  // per unique target; entries fanning out to the same dataset share the
  // result.
  private async precomputeExistence(
    entries: readonly ResolvedEntry[]
  ): Promise<Map<string, boolean>> {
    if (!this.sfPorts) return new Map()
    const ports = this.sfPorts
    const checks = await Promise.all(
      [...uniqueTargets(entries)].map(([key, { org, name }]) =>
        existenceOf(ports.get(org), key, name)
      )
    )
    return new Map(checks.filter(check => check !== undefined))
  }

  private async renderEntry(
    entry: ConfigEntry,
    watermarks: WatermarkStore,
    bounds: DateBounds,
    existence: Map<string, boolean>
  ): Promise<void> {
    const dk = DatasetKey.fromEntry(entry)
    const annotation = this.bootstrapAnnotation(entry, existence)
    this.logger.info(`  ${annotation}${entryLabel(entry)} → ${dk.toString()}`)
    if (isCsvEntry(entry)) {
      this.logger.info(
        '    watermark: n/a (CSV entry — watermarks do not apply)'
      )
      return
    }
    const wm = watermarks.get(WatermarkKey.fromEntry(entry))
    this.logger.info(`    watermark: ${wm?.toString() ?? '(none)'}`)
    if (bounds.isEmpty()) return
    const dateField = isElfEntry(entry) ? 'LogDate' : entry.dateField
    const lower = bounds.lowerConditionFor(dateField, wm)
    const upper = bounds.upperConditionFor(dateField)
    const soql = [lower, upper].filter(Boolean).join(' AND ')
    this.logger.info(
      `    effective: ${soql}${dryRunAnnotation(entry, wm, bounds)}`
    )
  }

  // [BOOTSTRAP] = no prior load: dataset will be created. Worst-case typo cost: ghost dataset.
  // [OVERRIDE]  = existing dataset + overrideMetadata:true: schema AND data replaced. Typo cost: data loss.
  private bootstrapAnnotation(
    entry: ConfigEntry,
    existence: Map<string, boolean>
  ): string {
    if (!entry.targetOrg || !entry.targetDataset) return ''
    const key = `${entry.targetOrg} ${entry.targetDataset}`
    const exists = existence.get(key)
    if (exists === undefined) return ''
    if (!exists) return '[BOOTSTRAP] '
    if (entry.overrideMetadata) return '[OVERRIDE] '
    return ''
  }
}

function uniqueTargets(
  entries: readonly ResolvedEntry[]
): Map<string, { org: string; name: string }> {
  const targets = new Map<string, { org: string; name: string }>()
  for (const { entry } of entries) {
    if (!entry.targetOrg || !entry.targetDataset) continue
    const key = `${entry.targetOrg} ${entry.targetDataset}`
    targets.set(key, { org: entry.targetOrg, name: entry.targetDataset })
  }
  return targets
}

async function existenceOf(
  port: SalesforcePort | undefined,
  key: string,
  name: string
): Promise<readonly [string, boolean] | undefined> {
  if (!port) return undefined
  try {
    const result: QueryResult<unknown> = await port.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${name}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    return [key, result.records.length > 0]
  } catch {
    // A failed existence check must never crash a dry run; the audit reports
    // query failures, so the dataset is treated as absent here.
    return [key, false]
  }
}
