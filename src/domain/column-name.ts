// Parse a raw CSV header line into a normalised list of column names.
//
// Handles: leading BOM, trailing CR, surrounding whitespace, double-quoted
// cells (including commas inside quotes and `""` escape). Empty cells are
// filtered so that a trailing comma or `a,,b` does not introduce a phantom
// column.
export function parseCsvHeader(rawLine: string): readonly string[] {
  if (rawLine.length === 0) return []

  let line = rawLine
  if (line.charCodeAt(0) === 0xfeff) line = line.slice(1)
  if (line.endsWith('\r')) line = line.slice(0, -1)

  const cells: string[] = []
  const len = line.length
  let i = 0

  while (i < len) {
    while (i < len && isWhitespace(line[i])) i++

    let cell = ''
    if (i < len && line[i] === '"') {
      i++
      while (i < len) {
        if (line[i] === '"') {
          if (i + 1 < len && line[i + 1] === '"') {
            cell += '"'
            i += 2
            continue
          }
          i++
          break
        }
        cell += line[i]
        i++
      }
      while (i < len && isWhitespace(line[i])) i++
    } else {
      while (i < len && line[i] !== ',') {
        cell += line[i]
        i++
      }
      cell = cell.trim()
    }

    if (cell.length > 0) cells.push(cell)

    if (i < len && line[i] === ',') i++
  }

  return cells
}

function isWhitespace(ch: string): boolean {
  return ch === ' ' || ch === '\t'
}
