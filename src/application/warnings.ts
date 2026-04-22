import {
  type ConfigEntry,
  entryLabel,
  isCsvEntry,
  isElfEntry,
  type ResolvedEntry,
} from '../adapters/config-loader.js'
import { type DateBounds } from '../domain/date-bounds.js'
import { type Watermark } from '../domain/watermark.js'
import { WatermarkKey } from '../domain/watermark-key.js'
import { type WatermarkStore } from '../domain/watermark-store.js'

// Pure functions that compute the warning messages emitted before every
// non-audit run (dry-run or real pipeline). Kept free of I/O so the 6-way
// taxonomy can be unit-tested exhaustively without spinning the SfCommand
// machinery. `computeWarnings` is the single entry point; callers emit each
// string through their logger.

export function computeWarnings(
  entries: readonly ResolvedEntry[],
  watermarks: WatermarkStore,
  bounds: DateBounds
): string[] {
  const messages: string[] = []
  messages.push(...firstRunMessages(entries, watermarks, bounds))
  messages.push(...boundsMessages(entries, watermarks, bounds))
  return messages
}

// Exported for targeted unit-testing; prefer `computeWarnings` in production.
export function firstRunMessages(
  entries: readonly ResolvedEntry[],
  watermarks: WatermarkStore,
  bounds: DateBounds
): string[] {
  if (bounds.hasStart()) return []
  const out: string[] = []
  for (const { entry } of entries) {
    if (isCsvEntry(entry)) continue
    if (watermarks.get(WatermarkKey.fromEntry(entry))) continue
    const label = entryLabel(entry)
    if (bounds.hasEnd()) {
      out.push(freshEndOnlyMessage(label))
    } else if (isElfEntry(entry)) {
      out.push(firstRunElfMessage(label))
    }
  }
  return out
}

export function boundsMessages(
  entries: readonly ResolvedEntry[],
  watermarks: WatermarkStore,
  bounds: DateBounds
): string[] {
  // Stryker disable next-line ConditionalExpression: equivalent mutant.
  // With empty bounds all four inner predicates return false, and Zod
  // guarantees entries.length >= 1 — the early-return is a hot-path
  // short-circuit, not a correctness gate.
  if (bounds.isEmpty()) return []
  const nonCsv = entries.filter(({ entry }) => !isCsvEntry(entry))
  if (nonCsv.length === 0) return [allCsvNoEffectMessage()]
  const out: string[] = []
  for (const { entry } of nonCsv) {
    const wm = watermarks.get(WatermarkKey.fromEntry(entry))
    const msg = pickBoundsMessage(entry, wm, bounds)
    if (msg) out.push(msg)
  }
  return out
}

function pickBoundsMessage(
  entry: ConfigEntry,
  wm: Watermark | undefined,
  bounds: DateBounds
): string | undefined {
  const label = entryLabel(entry)
  if (bounds.rewindsBelow(wm)) return rewindMessage(label, wm)
  if (bounds.leavesHoleAbove(wm)) return holeMessage(label, wm)
  if (bounds.matchesWatermark(wm) && entry.operation === 'Append')
    return boundaryMessage(label, wm)
  if (bounds.endsBeforeWatermark(wm)) return emptyMessage(label, wm)
  return undefined
}

// Annotation suffix appended to the dry-run `effective:` line. Kept separate
// from the warning messages because the annotation phrasing is tighter (no
// per-entry label, no watermark value) than the full warning template.
export function dryRunAnnotation(
  entry: ConfigEntry,
  wm: Watermark | undefined,
  bounds: DateBounds
): string {
  if (bounds.rewindsBelow(wm))
    return '  (REWIND: --start-date before watermark — watermark may regress)'
  if (bounds.leavesHoleAbove(wm))
    return '  (HOLE: --start-date after watermark — records in the gap will never be back-filled)'
  if (bounds.matchesWatermark(wm) && entry.operation === 'Append')
    return '  (BOUNDARY: --start-date equals watermark — boundary record will be re-appended (duplicate))'
  if (bounds.endsBeforeWatermark(wm))
    return '  (EMPTY: end-date before watermark — no records will load)'
  return ''
}

// ── Message templates ───────────────────────────────────────────────────

function firstRunElfMessage(label: string): string {
  return `[${label}] FIRST_RUN_ELF: no watermark and no --start-date; every log file ever emitted for this event type will be downloaded. On busy orgs this is thousands of blobs. Pass --start-date (e.g. --start-date 2026-01-01T00:00:00.000Z) to cap the initial pull. See README Advanced Usage.`
}

function freshEndOnlyMessage(label: string): string {
  return `[${label}] FRESH_END_ONLY: no watermark yet and --end-date provided without --start-date; the watermark will advance to this run's max dateField (at or before --end-date), so records created after --end-date will be skipped until --end-date is dropped. Pass --start-date on the first run to make the window explicit.`
}

function allCsvNoEffectMessage(): string {
  return '--start-date / --end-date provided but all selected entries are CSV; bounds have no effect. CSV entries are streamed in full.'
}

function rewindMessage(label: string, wm: Watermark | undefined): string {
  return `[${label}] REWIND: --start-date is before watermark ${wm}; previously-loaded records will be re-loaded; watermark may regress.`
}

function holeMessage(label: string, wm: Watermark | undefined): string {
  return `[${label}] HOLE: --start-date is after watermark ${wm}; records between the watermark and --start-date will be skipped this run AND by subsequent incremental runs (watermark will jump past the gap as soon as any in-window record loads).`
}

function boundaryMessage(label: string, wm: Watermark | undefined): string {
  return `[${label}] BOUNDARY: --start-date equals watermark ${wm}; under operation Append the boundary record will be appended again (duplicate row). Bump --start-date past the watermark, or use operation Overwrite.`
}

function emptyMessage(label: string, wm: Watermark | undefined): string {
  return `[${label}] EMPTY: --end-date is before watermark ${wm}; query window is empty — no records will load. To replay this range, use a separate --state-file (see RUN_BOOK).`
}
