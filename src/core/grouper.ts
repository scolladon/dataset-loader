import { parse } from 'csv-parse/sync'
import { stringify } from 'csv-stringify/sync'
import { type GroupInput, type GroupResult } from '../types.js'

export function group(results: GroupInput[]): Map<string, GroupResult> {
  const groups = new Map<string, { header: string[]; rows: string[][]; operation: 'Append' | 'Overwrite' }>()

  for (const { key, csv, operation } of results) {
    const records: string[][] = parse(csv, { relax_column_count: true })
    if (records.length === 0) continue

    const [header, ...dataRows] = records
    const existing = groups.get(key)

    if (!existing) {
      groups.set(key, { header, rows: dataRows, operation })
    } else {
      existing.rows.push(...dataRows)
    }
  }

  const result = new Map<string, GroupResult>()
  for (const [key, { header, rows, operation }] of groups) {
    const csv = stringify([header, ...rows], { quoted: true, quoted_empty: true }).trimEnd()
    result.set(key, { csv, operation })
  }

  return result
}
