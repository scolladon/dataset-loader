import { Readable } from 'node:stream'
import { stringify } from 'csv-stringify/sync'
import { Watermark } from '../domain/watermark.js'
import {
  type FetchPort,
  type FetchResult,
  type QueryResult,
  type SalesforcePort,
} from '../ports/types.js'
import { queryPages } from './query-pages.js'

export class SObjectFetcher implements FetchPort {
  private readonly queryFields: string[]

  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly sobject: string,
    private readonly fields: string[],
    private readonly dateField: string,
    private readonly where?: string,
    private readonly queryLimit?: number
  ) {
    const apiNamePattern = /^[a-zA-Z_][a-zA-Z0-9_]*$/
    if (!apiNamePattern.test(sobject)) {
      throw new Error(`Invalid sobject: '${sobject}'`)
    }
    for (const field of fields) {
      if (!apiNamePattern.test(field)) {
        throw new Error(`Invalid field: '${field}'`)
      }
    }
    if (!apiNamePattern.test(dateField)) {
      throw new Error(`Invalid dateField: '${dateField}'`)
    }
    this.queryFields = fields.includes(dateField)
      ? fields
      : [...fields, dateField]
  }

  async fetch(watermark?: Watermark): Promise<FetchResult> {
    const conditions: string[] = []
    if (watermark)
      conditions.push(`${this.dateField} > ${watermark.toSoqlLiteral()}`)
    if (this.where) conditions.push(`(${this.where})`)

    const whereClause =
      conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : ''
    const limitClause = this.queryLimit ? ` LIMIT ${this.queryLimit}` : ''
    const soql = `SELECT ${this.queryFields.join(', ')} FROM ${this.sobject}${whereClause} ORDER BY ${this.dateField} ASC${limitClause}`

    const firstPage: QueryResult<Record<string, unknown>> =
      await this.sfPort.query(soql)
    let lastWatermark: Watermark | undefined

    const fields = this.fields
    const dateField = this.dateField
    const sfPort = this.sfPort

    return {
      streams: (async function* (): AsyncGenerator<Readable> {
        for await (const page of queryPages(firstPage, (url: string) =>
          sfPort.queryMore<Record<string, unknown>>(url)
        )) {
          if (page.records.length === 0) continue
          const rows: string[][] = []
          for (const record of page.records) {
            lastWatermark = Watermark.fromString(String(record[dateField]))
            rows.push(fields.map(f => String(record[f] ?? '')))
          }
          const csvText = stringify([fields, ...rows], {
            quoted: true,
            quoted_empty: true,
          })
          yield Readable.from(Buffer.from(csvText))
        }
      })(),
      totalHint: firstPage.totalSize,
      watermark: () => lastWatermark,
    }
  }
}
