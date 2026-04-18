import { csvQuote } from './csv-quote.js'

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
