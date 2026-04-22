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

// Renders the `--dry-run` dispatch output. Two output shapes live here:
//   - Legacy (bounds empty): one line per entry with the stored watermark
//     inline.
//   - Bounded (non-empty bounds): a header, configured-window line, blank
//     separator, then one entry block of 3 lines each (label, watermark,
//     effective SOQL with warning annotation).
// Warnings are emitted via the injected LoggerPort before the entry plan is
// printed, so operators see them in the same terminal flow. Returns a zeroed
// result — dry-run never processes data.
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
    if (bounds.isEmpty()) {
      this.renderLegacy(entries, watermarks)
      return EMPTY_RESULT
    }
    this.logger.info(`Configured window: ${bounds.toString()}`)
    this.logger.info('')
    for (const { entry } of entries) {
      this.renderBoundedEntry(entry, watermarks, bounds)
    }
    return EMPTY_RESULT
  }

  private renderLegacy(
    entries: readonly ResolvedEntry[],
    watermarks: WatermarkStore
  ): void {
    for (const { entry } of entries) {
      const wk = WatermarkKey.fromEntry(entry)
      const wm = watermarks.get(wk)?.toString() ?? '(none)'
      const dk = DatasetKey.fromEntry(entry)
      this.logger.info(
        `  ${entryLabel(entry)} → ${dk.toString()} (watermark: ${wm})`
      )
    }
  }

  private renderBoundedEntry(
    entry: ConfigEntry,
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): void {
    const dk = DatasetKey.fromEntry(entry)
    this.logger.info(`  ${entryLabel(entry)} → ${dk.toString()}`)
    if (isCsvEntry(entry)) {
      this.logger.info('    watermark: n/a (CSV entry — bounds do not apply)')
      return
    }
    const wm = watermarks.get(WatermarkKey.fromEntry(entry))
    this.logger.info(`    watermark: ${wm?.toString() ?? '(none)'}`)
    const dateField = isElfEntry(entry) ? 'LogDate' : entry.dateField
    // Precondition: bounds are non-empty here (the caller guards on
    // `bounds.isEmpty()`), so at least one of lower/upper is defined.
    const lower = bounds.lowerConditionFor(dateField, wm)
    const upper = bounds.upperConditionFor(dateField)
    const soql = [lower, upper].filter(Boolean).join(' AND ')
    const annotation = dryRunAnnotation(entry, wm, bounds)
    this.logger.info(`    effective: ${soql}${annotation}`)
  }
}
