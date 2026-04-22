import { csvQuote } from '../../domain/csv-quote.js'
import { DateBounds } from '../../domain/date-bounds.js'
import { Watermark } from '../../domain/watermark.js'
import {
  type FetchResult,
  type ProjectionLayout,
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
  readonly bounds: DateBounds
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

function formatFieldValue(value: unknown): string {
  if (value === null || value === undefined) return '""'
  return csvQuote(typeof value === 'string' ? value : String(value))
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
  private readonly bounds: DateBounds
  private layout?: ProjectionLayout

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
    this.bounds = config.bounds
    this.queryFields = config.fields.includes(config.dateField)
      ? config.fields
      : [...config.fields, config.dateField]
  }

  async header(): Promise<string> {
    return new SObjectHeader(this.fields).toString()
  }

  project(layout: ProjectionLayout): void {
    if (this.layout !== undefined) {
      throw new Error('SObjectReader.project called twice')
    }
    if (layout.outputIndex.length !== this.fields.length) {
      throw new Error(
        `SObjectReader.project: outputIndex length ${layout.outputIndex.length} !== reader fields length ${this.fields.length}`
      )
    }
    this.layout = layout
  }

  async fetch(watermark?: Watermark): Promise<FetchResult> {
    const conditions: string[] = []
    const lower = this.bounds.lowerConditionFor(this.dateField, watermark)
    if (lower) conditions.push(lower)
    const upper = this.bounds.upperConditionFor(this.dateField)
    if (upper) conditions.push(upper)
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
    const layout = this.layout

    // Build the row encoder once per fetch. Layout-aware path projects each
    // record into the dataset's column order with augment values inlined at
    // their target positions; legacy path emits cells in reader order (used
    // by file targets and any reader without a configured layout).
    const buildRow: (record: Record<string, unknown>) => string = layout
      ? record => {
          const out = new Array<string>(layout.targetSize)
          const augmentSlots = layout.augmentSlots
          const outputIndex = layout.outputIndex
          for (let i = 0; i < augmentSlots.length; i++) {
            out[augmentSlots[i].pos] = augmentSlots[i].quoted
          }
          for (let c = 0; c < accessors.length; c++) {
            out[outputIndex[c]] = formatFieldValue(accessors[c](record))
          }
          return out.join(',')
        }
      : record => {
          const cells = new Array<string>(accessors.length)
          for (let c = 0; c < accessors.length; c++) {
            cells[c] = formatFieldValue(accessors[c](record))
          }
          return cells.join(',')
        }

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
          const batch = new Array<string>(currentPage.records.length)
          for (let r = 0; r < currentPage.records.length; r++) {
            batch[r] = buildRow(currentPage.records[r])
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
