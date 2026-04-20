// Excel and Google Sheets evaluate leading = + - @ | as formulas (or DDE
// payloads) even inside double-quoted CSV cells. Prefix a TAB
// (OWASP-recommended) so the cell renders as text. `|` covers legacy DDE
// (`cmd|'/c calc'!A0`); CR splits cells in some parsers.
const FORMULA_PREFIX = /^[=+\-@|\t\r]/

/**
 * Quote a single CSV cell value with csv-stringify parity
 * (`{ quoted: true, quoted_empty: true }`) and OWASP formula-injection guard.
 */
export function csvQuote(value: string): string {
  const escaped = value.includes('"') ? value.replaceAll('"', '""') : value
  const guarded = FORMULA_PREFIX.test(escaped) ? `\t${escaped}` : escaped
  return `"${guarded}"`
}
