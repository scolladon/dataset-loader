import { parse } from 'csv-parse/sync'
import { stringify } from 'csv-stringify/sync'

export function augment(csv: string, columns: Record<string, string>): string {
  const keys = Object.keys(columns)
  if (keys.length === 0) return csv

  const records: string[][] = parse(csv, { relax_column_count: true })
  if (records.length === 0) return csv

  const values = keys.map((k) => columns[k])
  const result = records.map((row, i) =>
    i === 0 ? [...row, ...keys] : [...row, ...values]
  )

  return stringify(result, { quoted: true, quoted_empty: true }).trimEnd()
}
