import { stringify } from 'csv-stringify/sync'
import { Watermark } from '../domain/watermark.js'
import {
  type FetchResult,
  type QueryResult,
  type ReaderPort,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  SOQL_RELATIONSHIP_PATH_PATTERN,
} from '../ports/types.js'

export interface SObjectReaderConfig {
  readonly sobject: string
  readonly fields: string[]
  readonly dateField: string
  readonly where?: string
  readonly queryLimit?: number
}

function resolveField(record: Record<string, unknown>, field: string): unknown {
  let current: unknown = record
  for (const part of field.split('.')) {
    if (current === null || typeof current !== 'object') return null
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

class SObjectHeader {
  constructor(private readonly fields: readonly string[]) {}
  toString(): string {
    return this.fields.join(',')
  }
}

export class SObjectReader implements ReaderPort {
  private readonly queryFields: string[]
  private readonly sobject: string
  private readonly fields: string[]
  private readonly dateField: string
  private readonly where?: string
  private readonly queryLimit?: number

  constructor(
    private readonly sfPort: SalesforcePort,
    config: SObjectReaderConfig
  ) {
    if (!SF_IDENTIFIER_PATTERN.test(config.sobject)) {
      throw new Error(`Invalid sobject: '${config.sobject}'`)
    }
    for (const field of config.fields) {
      if (!SOQL_RELATIONSHIP_PATH_PATTERN.test(field)) {
        throw new Error(`Invalid field: '${field}'`)
      }
    }
    if (!SF_IDENTIFIER_PATTERN.test(config.dateField)) {
      throw new Error(`Invalid dateField: '${config.dateField}'`)
    }
    this.sobject = config.sobject
    this.fields = config.fields
    this.dateField = config.dateField
    this.where = config.where
    this.queryLimit = config.queryLimit
    this.queryFields = config.fields.includes(config.dateField)
      ? config.fields
      : [...config.fields, config.dateField]
  }

  async header(): Promise<string> {
    return new SObjectHeader(this.fields).toString()
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
    let pagesProcessed = 0
    let lastRecord: Record<string, unknown> | undefined

    const fields = this.fields
    const dateField = this.dateField
    const sfPort = this.sfPort

    async function* generateLines(): AsyncGenerator<string[]> {
      let currentPage = firstPage

      while (true) {
        const nextPromise =
          !currentPage.done && currentPage.nextRecordsUrl
            ? sfPort.queryMore<Record<string, unknown>>(
                currentPage.nextRecordsUrl
              )
            : null

        if (currentPage.records.length > 0) {
          const batch = currentPage.records.map(record =>
            stringify(
              [fields.map(f => String(resolveField(record, f) ?? ''))],
              { quoted: true, quoted_empty: true }
            ).trimEnd()
          )
          lastRecord = currentPage.records.at(-1)
          pagesProcessed++
          yield batch
        }

        if (!nextPromise) break
        currentPage = await nextPromise
      }
    }

    return {
      lines: generateLines(),
      watermark: () =>
        lastRecord
          ? Watermark.fromString(String(lastRecord[dateField]))
          : undefined,
      fileCount: () => pagesProcessed,
    }
  }
}
