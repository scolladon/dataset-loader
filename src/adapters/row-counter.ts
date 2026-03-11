import { PassThrough } from 'node:stream'
import { type GroupTracker } from '../ports/types.js'

export function createRowCounter(
  tracker: Pick<GroupTracker, 'addRows'>
): PassThrough {
  const counter = new PassThrough({ objectMode: true })
  counter.on('data', () => tracker.addRows(1))
  return counter
}
