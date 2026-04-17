// Excel and Google Sheets evaluate leading = + - @ | as formulas (or DDE
// payloads) even inside double-quoted CSV cells. Prefix a TAB
// (OWASP-recommended) so the cell renders as text. `|` covers legacy DDE
// (e.g. `cmd|'/c calc'!A0`) that still fires on old Excel configurations.
const FORMULA_PREFIX = /^[=+\-@|\t\r]/
function csvQuote(value: string): string {
  const escaped = value.includes('"') ? value.replaceAll('"', '""') : value
  const guarded = FORMULA_PREFIX.test(escaped) ? `\t${escaped}` : escaped
  return `"${guarded}"`
}

export function buildAugmentSuffix(columns: Record<string, string>): string {
  const values = Object.values(columns)
  if (values.length === 0) return ''
  return ',' + values.map(csvQuote).join(',')
}

// Column names are emitted unquoted in the header row. Safe because Zod's
// `datasetColumnName` (config-loader.ts) restricts them to
// `[a-zA-Z_][a-zA-Z0-9_.]*`, which excludes every formula-initiating
// character. If that regex is relaxed, revisit this function.
export function buildAugmentHeaderSuffix(
  columns: Record<string, string>
): string {
  const keys = Object.keys(columns)
  if (keys.length === 0) return ''
  return ',' + keys.join(',')
}
