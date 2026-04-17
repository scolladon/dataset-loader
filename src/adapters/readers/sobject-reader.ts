import { Watermark } from '../../domain/watermark.js'
import {
  type FetchResult,
  type QueryResult,
  type ReaderPort,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  SOQL_RELATIONSHIP_PATH_PATTERN,
} from '../../ports/types.js'

interface SObjectReaderConfig {
  readonly sobject: string
  readonly fields: string[]
  readonly dateField: string
  readonly where?: string
  readonly queryLimit?: number
}

type FieldAccessor = (record: Record<string, unknown>) => unknown

function buildFieldAccessor(field: string): FieldAccessor {
  if (!field.includes('.')) {
    return record => record[field]
  }
  const parts = field.split('.')
  return record => {
    let current: unknown = record
    for (const part of parts) {
      if (current === null || typeof current !== 'object') return null
      current = (current as Record<string, unknown>)[part]
    }
    return current
  }
}

// Excel and Google Sheets evaluate leading = + - @ | as formulas (or DDE
// payloads) even inside double-quoted CSV cells. Prefix a TAB
// (OWASP-recommended) so the cell renders as text. CR splits cells, and `|`
// covers legacy DDE (e.g. `cmd|'/c calc'!A0`) that still fires on old Excel
// configurations.
const FORMULA_PREFIX = /^[=+\-@|\t\r]/
function quoteCsvField(value: string): string {
  const escaped = value.includes('"') ? value.replaceAll('"', '""') : value
  const guarded = FORMULA_PREFIX.test(escaped) ? `\t${escaped}` : escaped
  return `"${guarded}"`
}

function formatFieldValue(value: unknown): string {
  if (value === null || value === undefined) return '""'
  return quoteCsvField(typeof value === 'string' ? value : String(value))
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
  private readonly fieldAccessors: readonly FieldAccessor[]
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
    this.fieldAccessors = config.fields.map(buildFieldAccessor)
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

    const accessors = this.fieldAccessors
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
          // Hand-roll CSV quoting per page: csv-stringify/sync was invoked once
          // per record which re-ran its option parser N times per page. This
          // loop keeps identical output (quoted: true, quoted_empty: true).
          const batch = new Array<string>(currentPage.records.length)
          for (let r = 0; r < currentPage.records.length; r++) {
            const record = currentPage.records[r]
            const cells = new Array<string>(accessors.length)
            for (let c = 0; c < accessors.length; c++) {
              cells[c] = formatFieldValue(accessors[c](record))
            }
            batch[r] = cells.join(',')
          }
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
