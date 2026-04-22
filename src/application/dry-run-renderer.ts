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
import { type LoggerPort } from '../ports/types.js'
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
  constructor(private readonly logger: LoggerPort) {}

  render(
    entries: readonly ResolvedEntry[],
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): DatasetLoadResult {
    for (const msg of computeWarnings(entries, watermarks, bounds)) {
      this.logger.warn(msg)
    }
    this.logger.info('Dry run — planned entries:')
    if (!bounds.isEmpty()) {
      this.logger.info(`Configured window: ${bounds.toString()}`)
      this.logger.info('')
    }
    for (const { entry } of entries) {
      this.renderEntry(entry, watermarks, bounds)
    }
    return EMPTY_RESULT
  }

  private renderEntry(
    entry: ConfigEntry,
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): void {
    const dk = DatasetKey.fromEntry(entry)
    this.logger.info(`  ${entryLabel(entry)} → ${dk.toString()}`)
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
}
