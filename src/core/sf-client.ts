import { Connection } from '@salesforce/core'
import pLimit, { type LimitFunction } from 'p-limit'

export interface QueryResult<T> {
  totalSize: number
  done: boolean
  nextRecordsUrl?: string
  records: T[]
}

export interface SfClientOptions {
  concurrency?: number
  retryBaseDelayMs?: number
}

interface HttpError {
  statusCode?: number
}

const DEFAULT_CONCURRENCY = 25
const MAX_RETRIES = 3
const DEFAULT_BASE_DELAY_MS = 1000

function isHttpError(err: unknown): err is HttpError {
  return typeof err === 'object' && err !== null && 'statusCode' in err
}

async function withRetry<T>(fn: () => Promise<T>, baseDelayMs: number): Promise<T> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (err: unknown) {
      if (isHttpError(err) && err.statusCode === 429 && attempt < MAX_RETRIES - 1) {
        const delay = baseDelayMs * Math.pow(2, attempt)
        await new Promise((resolve) => setTimeout(resolve, delay))
        continue
      }
      throw err
    }
  }
  throw new Error('Unreachable')
}

export class SfClient {
  private readonly connection: Connection
  private readonly limiter: LimitFunction
  private readonly retryBaseDelayMs: number

  constructor(connection: Connection, options: SfClientOptions = {}) {
    this.connection = connection
    this.limiter = pLimit(options.concurrency ?? DEFAULT_CONCURRENCY)
    this.retryBaseDelayMs = options.retryBaseDelayMs ?? DEFAULT_BASE_DELAY_MS
  }

  async query<T>(soql: string): Promise<QueryResult<T>> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<QueryResult<T>>({
            method: 'GET',
            url: `/services/data/v${this.connection.version}/query?q=${encodeURIComponent(soql)}`,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.retryBaseDelayMs
      )
    )
  }

  async queryMore<T>(nextRecordsUrl: string): Promise<QueryResult<T>> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<QueryResult<T>>({
            method: 'GET',
            url: nextRecordsUrl,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.retryBaseDelayMs
      )
    )
  }

  async getBlob(path: string): Promise<string> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<string>({
            method: 'GET',
            url: path,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.retryBaseDelayMs
      )
    )
  }

  async post<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<T>({
            method: 'POST',
            url: path,
            headers: {
              'Content-Type': 'application/json',
              'Accept-Encoding': 'gzip',
            },
            body: JSON.stringify(body),
          }),
        this.retryBaseDelayMs
      )
    )
  }

  async patch<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<T>({
            method: 'PATCH',
            url: path,
            headers: {
              'Content-Type': 'application/json',
              'Accept-Encoding': 'gzip',
            },
            body: JSON.stringify(body),
          }),
        this.retryBaseDelayMs
      )
    )
  }

  get apiVersion(): string {
    return this.connection.version
  }
}
