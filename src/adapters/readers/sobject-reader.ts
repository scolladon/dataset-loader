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
  type SoqlResource,
} from '../../ports/types.js'

interface SObjectReaderConfig {
  readonly sobject: string
  readonly fields: string[]
  readonly dateField: string
  readonly where?: string
  readonly queryLimit?: number
  readonly bounds: DateBounds
  readonly resource?: SoqlResource
}

type FirstPageQuery = (
  soql: string
) => Promise<QueryResult<Record<string, unknown>>>

const DEFAULT_SOQL_RESOURCE: SoqlResource = 'query'

const FIRST_PAGE_QUERY: Readonly<
  Record<SoqlResource, (port: SalesforcePort) => FirstPageQuery>
> = {
  query: port => soql => port.query(soql),
  queryAll: port => soql => port.queryAll(soql),
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

type RowEncoder = (record: Record<string, unknown>) => string

interface PageStreamSource {
  readonly sfPort: SalesforcePort
  readonly firstPage: QueryResult<Record<string, unknown>>
  readonly encodeRow: RowEncoder
  readonly dateField: string
}

function assertValidConfig(config: SObjectReaderConfig): void {
  if (!SF_IDENTIFIER_PATTERN.test(config.sobject)) {
    throw new Error(`Invalid sobject: '${config.sobject}'`)
  }
  const invalidField = config.fields.find(
    field => !SOQL_RELATIONSHIP_PATH_PATTERN.test(field)
  )
  if (invalidField !== undefined) {
    throw new Error(`Invalid field: '${invalidField}'`)
  }
  if (!SF_IDENTIFIER_PATTERN.test(config.dateField)) {
    throw new Error(`Invalid dateField: '${config.dateField}'`)
  }
}

function queryFieldsOf(fields: string[], dateField: string): string[] {
  return fields.includes(dateField) ? fields : [...fields, dateField]
}

function layoutRowEncoder(
  layout: ProjectionLayout,
  accessors: readonly FieldAccessor[]
): RowEncoder {
  const { augmentSlots, outputIndex } = layout
  return record => {
    const out = new Array<string>(layout.targetSize)
    for (let i = 0; i < augmentSlots.length; i++) {
      out[augmentSlots[i].pos] = augmentSlots[i].quoted
    }
    for (let c = 0; c < accessors.length; c++) {
      out[outputIndex[c]] = formatFieldValue(accessors[c](record))
    }
    return out.join(',')
  }
}

function readerOrderRowEncoder(
  accessors: readonly FieldAccessor[]
): RowEncoder {
  return record => {
    const cells = new Array<string>(accessors.length)
    for (let c = 0; c < accessors.length; c++) {
      cells[c] = formatFieldValue(accessors[c](record))
    }
    return cells.join(',')
  }
}

class SObjectHeader {
  constructor(private readonly fields: readonly string[]) {}
  toString(): string {
    return this.fields.join(',')
  }
}

class SObjectPageStream {
  private pagesProcessed = 0
  private lastRecord?: Record<string, unknown>

  constructor(private readonly source: PageStreamSource) {}

  async *lines(): AsyncGenerator<string[]> {
    let currentPage = this.source.firstPage
    while (true) {
      const nextPromise = this.nextPageOf(currentPage)
      if (currentPage.records.length > 0) {
        this.track(currentPage.records)
        yield this.encodePage(currentPage.records)
      }
      if (!nextPromise) return
      currentPage = await nextPromise
    }
  }

  watermark(): Watermark | undefined {
    return this.lastRecord
      ? Watermark.fromString(String(this.lastRecord[this.source.dateField]))
      : undefined
  }

  pageCount(): number {
    return this.pagesProcessed
  }

  private nextPageOf(
    page: QueryResult<Record<string, unknown>>
  ): Promise<QueryResult<Record<string, unknown>>> | null {
    if (page.done || !page.nextRecordsUrl) return null
    return this.source.sfPort.queryMore<Record<string, unknown>>(
      page.nextRecordsUrl
    )
  }

  private track(records: Record<string, unknown>[]): void {
    this.lastRecord = records.at(-1)
    this.pagesProcessed++
  }

  private encodePage(records: Record<string, unknown>[]): string[] {
    const batch = new Array<string>(records.length)
    for (let r = 0; r < records.length; r++) {
      batch[r] = this.source.encodeRow(records[r])
    }
    return batch
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
  private readonly firstPageQuery: FirstPageQuery
  private layout?: ProjectionLayout

  constructor(
    private readonly sfPort: SalesforcePort,
    config: SObjectReaderConfig
  ) {
    assertValidConfig(config)
    this.sobject = config.sobject
    this.fields = config.fields
    this.fieldAccessors = config.fields.map(buildFieldAccessor)
    this.dateField = config.dateField
    this.where = config.where
    this.queryLimit = config.queryLimit
    this.bounds = config.bounds
    this.queryFields = queryFieldsOf(config.fields, config.dateField)
    this.firstPageQuery =
      FIRST_PAGE_QUERY[config.resource ?? DEFAULT_SOQL_RESOURCE](sfPort)
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
    const firstPage: QueryResult<Record<string, unknown>> =
      await this.firstPageQuery(this.buildSoql(watermark))
    const stream = new SObjectPageStream({
      sfPort: this.sfPort,
      firstPage,
      encodeRow: this.rowEncoder(),
      dateField: this.dateField,
    })
    return {
      lines: stream.lines(),
      watermark: () => stream.watermark(),
      fileCount: () => stream.pageCount(),
      // totalSize already reflects any LIMIT clause — Salesforce returns the
      // post-LIMIT match count, not the raw filter count.
      total: { count: firstPage.totalSize, unit: 'rows' },
    }
  }

  private buildSoql(watermark?: Watermark): string {
    const conditions = this.conditionsFor(watermark)
    const whereClause =
      conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : ''
    const limitClause = this.queryLimit ? ` LIMIT ${this.queryLimit}` : ''
    return `SELECT ${this.queryFields.join(', ')} FROM ${this.sobject}${whereClause} ORDER BY ${this.dateField} ASC${limitClause}`
  }

  private conditionsFor(watermark?: Watermark): string[] {
    const conditions: string[] = []
    const lower = this.bounds.lowerConditionFor(this.dateField, watermark)
    if (lower) conditions.push(lower)
    const upper = this.bounds.upperConditionFor(this.dateField)
    if (upper) conditions.push(upper)
    if (this.where) conditions.push(`(${this.where})`)
    return conditions
  }

  // Build the row encoder once per fetch. Layout-aware path projects each
  // record into the dataset's column order with augment values inlined at
  // their target positions; legacy path emits cells in reader order (used
  // by file targets and any reader without a configured layout).
  private rowEncoder(): RowEncoder {
    return this.layout
      ? layoutRowEncoder(this.layout, this.fieldAccessors)
      : readerOrderRowEncoder(this.fieldAccessors)
  }
}
