// Excel and Google Sheets evaluate leading = + - @ as formulas even inside
// double-quoted CSV cells. Prefix a TAB (OWASP-recommended) so the cell renders
// as text.
const FORMULA_PREFIX = /^[=+\-@\t\r]/
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

export function buildAugmentHeaderSuffix(
  columns: Record<string, string>
): string {
  const keys = Object.keys(columns)
  if (keys.length === 0) return ''
  return ',' + keys.join(',')
}
