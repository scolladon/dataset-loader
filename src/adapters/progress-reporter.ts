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
      format: `${label.padEnd(10)} {bar} {value}/{total} {unit} | {duration_formatted} elapsed`,
      clearOnComplete: false,
      hideCursor: true,
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
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
        const format = withParts
          ? `  ${groupLabel} — {files} {filesUnit}, {rows} {rowsUnit} → {value} {unit}`
          : `  ${groupLabel} — {files} {filesUnit}, {rows} {rowsUnit}`
        const payload: Record<string, string | number> = {
          files: 0,
          filesUnit: 'files',
          rows: 0,
          rowsUnit: 'rows',
          ...(withParts ? { unit: 'parts' } : {}),
        }

        const groupBar = multibar.create(0, 0, {}, { format })
        // Workaround: same non-TTY issue as the main bar (see above).
        if (!process.stderr.isTTY) {
          groupBar.start(0, 0, payload)
        }

        const progressValue = (): number => {
          if (progressUnit === 'rows') return rows
          if (progressUnit === 'files') return files
          if (progressUnit === 'bytes') return bytes
          return withParts ? parts : 0
        }

        const updateBar = (): void => {
          payload.files = files
          payload.filesUnit = files === 1 ? 'file' : 'files'
          payload.rows = rows
          payload.rowsUnit = rows === 1 ? 'row' : 'rows'
          if (withParts) payload.unit = partUnit(parts)
          groupBar.update(progressValue(), payload)
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
