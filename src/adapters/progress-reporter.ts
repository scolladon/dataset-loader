import cliProgress from 'cli-progress'
import {
  type GroupTracker,
  type PhaseProgress,
  type ProgressPort,
  type ProgressUnit,
} from '../ports/types.js'

function partUnit(count: number): string {
  return count === 1 ? 'part' : 'parts'
}

// `cli-progress` performs `{token}` substitution against the bar payload.
// A user-controlled dataset name like `{value}` would be re-substituted at
// render time and corrupt the display. Stripping braces from any label
// embedded into a format string prevents that collision (display-only
// concern; no escalation surface).
function sanitizeLabel(value: string): string {
  return value.replace(/[{}]/g, '')
}

/* v8 ignore next 3 -- exhaustiveness guard; only fires if a new ProgressUnit
   is added without updating the dispatchers below. */
function assertNever(value: never): never {
  throw new Error(`unreachable: unexpected ProgressUnit ${String(value)}`)
}

// Label for the bar's progress driver — matches whichever unit `setTotal`
// declared. Until a total is declared (counter-only mode), the bar reads
// in neutral 'items' so the format renders without unit churn. The
// `default` branch enforces compile-time exhaustiveness: adding a new unit
// to `ProgressUnit` makes this fail to type-check.
function unitLabel(
  progressUnit: ProgressUnit | undefined,
  value: number
): string {
  switch (progressUnit) {
    case 'rows':
      return value === 1 ? 'row' : 'rows'
    case 'files':
      return value === 1 ? 'file' : 'files'
    case 'bytes':
      return 'bytes'
    case undefined:
      return 'items'
    /* v8 ignore next 2 -- exhaustiveness guard; statically unreachable
       because the switch already covers every member of `ProgressUnit | undefined`. */
    default:
      return assertNever(progressUnit)
  }
}

const NOOP_GROUP_TRACKER: GroupTracker = {
  updateParentId: () => {
    /* noop */
  },
  incrementParts: () => {
    /* noop */
  },
  addFiles: () => {
    /* noop */
  },
  addRows: () => {
    /* noop */
  },
  addBytes: () => {
    /* noop */
  },
  setTotal: () => {
    /* noop */
  },
  stop: () => {
    /* noop */
  },
}

export class ProgressReporter implements ProgressPort {
  create(label: string, total: number): PhaseProgress {
    if (total === 0) {
      return {
        tick: () => {
          /* noop */
        },
        trackGroup: () => NOOP_GROUP_TRACKER,
        stop: () => {
          /* noop */
        },
      }
    }

    const multibar = new cliProgress.MultiBar({
      format: `${sanitizeLabel(label).padEnd(10)} {bar} {value}/{total} {unit} | {duration_formatted} elapsed`,
      clearOnComplete: false,
      hideCursor: true,
      barCompleteChar: '█',
      barIncompleteChar: '░',
    })

    const unit = total === 1 ? 'item' : 'items'
    const bar = multibar.create(total, 0, { unit })
    // Workaround: cli-progress MultiBar.create() skips bar.start() in non-TTY mode,
    // leaving default total=100, empty payload, and startTime=0.
    // In TTY mode, create() already calls start() internally — guard to avoid double-start.
    if (!process.stderr.isTTY) {
      bar.start(total, 0, { unit })
    }

    return {
      tick: (): void => {
        bar.increment()
      },
      trackGroup: (groupLabel: string, withParts = false): GroupTracker => {
        let parts = 0
        let files = 0
        let rows = 0
        let bytes = 0
        let progressUnit: ProgressUnit | undefined
        let totalDeclared = 0
        // Once two readers contribute totals with different units (e.g. ELF
        // 'files' + SObject 'rows' fanning into the same dataset slot), no
        // single bar value can represent both. Latch the bar into counter-
        // only mode for the rest of the run — sticky to keep the display
        // stable in the face of non-deterministic Promise.all completion order.
        let mixedUnits = false
        // Format always carries `{bar} {value}/{total} {unit}` — the visual
        // bar fills as a real total + value land via setTotal/addX. Counters
        // (`{files}`, `{rows}`) are informational and tick continuously
        // regardless of which unit drives the bar. Parts are appended only
        // for dataset targets (CRMA writer emits parts; FileWriter doesn't).
        const format =
          `  ${sanitizeLabel(groupLabel)} {bar} {value}/{total} {unit} | ` +
          `{files} {filesUnit}, {rows} {rowsUnit}` +
          (withParts ? ` → {parts} {partUnit}` : ``)
        const payload: Record<string, string | number> = {
          files: 0,
          filesUnit: 'files',
          rows: 0,
          rowsUnit: 'rows',
          unit: unitLabel(undefined, 0),
          ...(withParts ? { parts: 0, partUnit: partUnit(0) } : {}),
        }

        const groupBar = multibar.create(0, 0, {}, { format })
        // Workaround: same non-TTY issue as the main bar (see above).
        if (!process.stderr.isTTY) {
          groupBar.start(0, 0, payload)
        }

        // Switch is exhaustive over `ProgressUnit | undefined` — the
        // `default` branch's `assertNever` triggers a compile error if a
        // new unit is added without updating this dispatcher, preventing
        // silent fall-through to the parts/0 fallback.
        const progressValue = (): number => {
          switch (progressUnit) {
            case 'rows':
              return rows
            case 'files':
              return files
            case 'bytes':
              return bytes
            case undefined:
              return withParts ? parts : 0
            /* v8 ignore next 2 -- exhaustiveness guard; unreachable because
               every `ProgressUnit | undefined` member is covered above. */
            default:
              return assertNever(progressUnit)
          }
        }

        const updateBar = (): void => {
          const value = progressValue()
          payload.files = files
          payload.filesUnit = files === 1 ? 'file' : 'files'
          payload.rows = rows
          payload.rowsUnit = rows === 1 ? 'row' : 'rows'
          payload.unit = unitLabel(progressUnit, value)
          if (withParts) {
            payload.parts = parts
            payload.partUnit = partUnit(parts)
          }
          groupBar.update(value, payload)
        }

        return {
          updateParentId: (_id: string): void => {
            /* noop — parentId not displayed */
          },
          incrementParts: (): void => {
            if (withParts) {
              parts++
              updateBar()
            }
          },
          addFiles: (count: number): void => {
            files += count
            updateBar()
          },
          addRows: (count: number): void => {
            rows += count
            updateBar()
          },
          addBytes: (count: number): void => {
            bytes += count
            updateBar()
          },
          setTotal: (count: number, unit: ProgressUnit): void => {
            // Defend against malformed external counts (e.g. a corrupted
            // Salesforce response): only accept finite, non-negative integers.
            // Reject zero too — `cli-progress` renders `total=0` as a
            // garbage/empty bar, so a "no rows to fetch" reply must leave
            // the tracker in counter-only mode rather than declare a unit.
            if (
              !Number.isFinite(count) ||
              !Number.isInteger(count) ||
              count <= 0
            ) {
              return
            }
            if (mixedUnits) return
            if (progressUnit === undefined) {
              progressUnit = unit
              totalDeclared = count
              groupBar.setTotal(count)
            } else if (progressUnit === unit) {
              totalDeclared += count
              groupBar.setTotal(totalDeclared)
            } else {
              mixedUnits = true
              progressUnit = undefined
              totalDeclared = 0
              groupBar.setTotal(0)
            }
            updateBar()
          },
          stop: (): void => {
            multibar.remove(groupBar)
          },
        }
      },
      stop: (): void => {
        multibar.stop()
      },
    }
  }
}
