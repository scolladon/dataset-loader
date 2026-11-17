// Prints the lines changed under src/ against the merge base with origin/main
// as a Stryker `--mutate` list (`file:start-end,…`). Prints nothing when no
// source line changed, so the caller can skip the run.
import { execFileSync } from 'node:child_process'
import { pathToFileURL } from 'node:url'

const NEW_FILE_PREFIX = '+++ b/'
const SOURCE_FILE = /^src\/.*\.ts$/
const HUNK_HEADER = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/
const SINGLE_LINE = 1
const MAX_DIFF_BYTES = 256 * 1024 * 1024

function sourceFileOf(headerLine) {
  if (!headerLine.startsWith(NEW_FILE_PREFIX)) return undefined
  const path = headerLine.slice(NEW_FILE_PREFIX.length)
  return SOURCE_FILE.test(path) ? path : undefined
}

function rangeOf(file, hunkLine) {
  const hunk = HUNK_HEADER.exec(hunkLine)
  if (!hunk) return undefined
  const start = Number(hunk[1])
  const count = hunk[2] === undefined ? SINGLE_LINE : Number(hunk[2])
  return count > 0 ? `${file}:${start}-${start + count - 1}` : undefined
}

export function mutateRangesFromDiff(diff) {
  const ranges = []
  let file
  for (const line of diff.split('\n')) {
    if (line.startsWith('+++ ')) {
      file = sourceFileOf(line)
      continue
    }
    const range = file === undefined ? undefined : rangeOf(file, line)
    if (range !== undefined) ranges.push(range)
  }
  return ranges
}

function changedSourceDiff() {
  return execFileSync(
    'git',
    [
      '--no-pager',
      'diff',
      '--no-ext-diff',
      '--unified=0',
      '--diff-filter=AM',
      '--merge-base',
      'origin/main',
      '--',
      'src',
    ],
    { encoding: 'utf8', maxBuffer: MAX_DIFF_BYTES }
  )
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.stdout.write(mutateRangesFromDiff(changedSourceDiff()).join(','))
}
