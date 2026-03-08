import cliProgress from 'cli-progress'
import { type PhaseProgress, type ProgressPort } from '../ports/types.js'

export class ProgressReporter implements ProgressPort {
  create(label: string, total: number): PhaseProgress {
    if (total === 0) {
      return {
        tick: () => {
          /* noop */
        },
        stop: () => {
          /* noop */
        },
      }
    }

    const multibar = new cliProgress.MultiBar({
      format: `${label.padEnd(10)} {bar} {value}/{total} {unit} | {duration}s elapsed | ~{eta}s remaining`,
      clearOnComplete: true,
      hideCursor: true,
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      etaBuffer: 3,
    })

    const unit = total === 1 ? 'item' : 'items'
    const bar = multibar.create(total, 0, { unit })

    return {
      tick: (detail: string): void => {
        multibar.log(`${detail}\n`)
        bar.increment()
      },
      stop: (): void => {
        multibar.stop()
      },
    }
  }
}
