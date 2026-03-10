import cliProgress from 'cli-progress'
import {
  type GroupTracker,
  type PhaseProgress,
  type ProgressPort,
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
      format: `${label.padEnd(10)} {bar} {value}/{total} {unit} | {duration}s elapsed`,
      clearOnComplete: true,
      hideCursor: true,
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
    })

    const unit = total === 1 ? 'item' : 'items'
    const bar = multibar.create(total, 0, { unit })
    // Workaround: cli-progress MultiBar.create() skips bar.start() in non-TTY mode,
    // leaving default total=100, empty payload, and startTime=0.
    bar.start(total, 0, { unit })

    return {
      tick: (): void => {
        bar.increment()
      },
      trackGroup: (groupLabel: string): GroupTracker => {
        let parts = 0
        let files = 0
        let rows = 0
        let parentId = '...'
        const groupBar = multibar.create(
          0,
          0,
          {},
          {
            format: `  ${groupLabel} ({parentId}) — {files} {filesUnit}, {rows} {rowsUnit} → {value} {unit}`,
          }
        )
        // Workaround: same non-TTY issue as the main bar (see above).
        groupBar.start(0, 0, {
          parentId,
          files: 0,
          filesUnit: 'files',
          rows: 0,
          rowsUnit: 'rows',
          unit: 'parts',
        })

        const updateBar = (): void => {
          groupBar.update(parts, {
            parentId,
            files,
            filesUnit: files === 1 ? 'file' : 'files',
            rows,
            rowsUnit: rows === 1 ? 'row' : 'rows',
            unit: partUnit(parts),
          })
        }

        return {
          updateParentId: (id: string): void => {
            parentId = id
            updateBar()
          },
          incrementParts: (): void => {
            parts++
            updateBar()
          },
          addFiles: (count: number): void => {
            files += count
            updateBar()
          },
          addRows: (count: number): void => {
            rows += count
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
