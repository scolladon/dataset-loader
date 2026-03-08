import { Readable } from 'node:stream'

import { type Connection } from '@salesforce/core'
import pLimit from 'p-limit'
import { type QueryResult, type SalesforcePort } from '../ports/types.js'

export interface SalesforceClientOptions {
  readonly concurrency?: number
  readonly retryBaseDelayMs?: number
}

const DEFAULT_CONCURRENCY = 25
const MAX_RETRIES = 3
const DEFAULT_BASE_DELAY_MS = 1000

function isHttpError(err: unknown): err is { statusCode?: number } {
  return typeof err === 'object' && err !== null && 'statusCode' in err
}

function formatError(err: unknown): Error {
  if (!(err instanceof Error)) return new Error(String(err))
  const data = (err as unknown as Record<string, unknown>).data
  if (data) {
    const details = Array.isArray(data)
      ? data
          .map(
            (e: { message?: string; errorCode?: string }) =>
              [e.errorCode, e.message].filter(Boolean).join(': ') ||
              JSON.stringify(e)
          )
          .join('; ')
      : JSON.stringify(data)
    return new Error(`${err.message}: ${details}`)
  }
  return err
}

async function withRetry<T>(
  fn: () => Promise<T>,
  baseDelayMs: number
): Promise<T> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (err: unknown) {
      if (
        isHttpError(err) &&
        err.statusCode === 429 &&
        attempt < MAX_RETRIES - 1
      ) {
        const halfDelay = (baseDelayMs * Math.pow(2, attempt)) / 2
        const delay = halfDelay + Math.random() * halfDelay
        await new Promise(resolve => setTimeout(resolve, delay))
        continue
      }
      throw formatError(err)
    }
  }
  throw new Error('Unreachable')
}

export class SalesforceClient implements SalesforcePort {
  readonly apiVersion: string
  private readonly limiter: ReturnType<typeof pLimit>
  private readonly baseDelay: number

  constructor(
    private readonly connection: Connection,
    options: SalesforceClientOptions = {}
  ) {
    this.apiVersion = connection.version
    this.limiter = pLimit(options.concurrency ?? DEFAULT_CONCURRENCY)
    this.baseDelay = options.retryBaseDelayMs ?? DEFAULT_BASE_DELAY_MS
  }

  query<T>(soql: string): Promise<QueryResult<T>> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<QueryResult<T>>({
            method: 'GET',
            url: `/services/data/v${this.connection.version}/query?q=${encodeURIComponent(soql)}`,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.baseDelay
      )
    )
  }

  queryMore<T>(nextRecordsUrl: string): Promise<QueryResult<T>> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<QueryResult<T>>({
            method: 'GET',
            url: nextRecordsUrl,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.baseDelay
      )
    )
  }

  getBlob(path: string): Promise<unknown> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request({
            method: 'GET',
            url: path,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.baseDelay
      )
    )
  }

  getBlobStream(path: string): Promise<Readable> {
    return this.limiter(() =>
      withRetry(async () => {
        let res = await this.fetchStream(path)
        if (res.status === 401) {
          await this.connection.refreshAuth()
          res = await this.fetchStream(path)
        }
        if (!res.ok) {
          throw Object.assign(new Error(`HTTP ${res.status}`), {
            statusCode: res.status,
          })
        }
        return Readable.fromWeb(
          res.body as Parameters<typeof Readable.fromWeb>[0]
        )
      }, this.baseDelay)
    )
  }

  private fetchStream(path: string): Promise<Response> {
    return fetch(`${this.connection.instanceUrl}${path}`, {
      headers: { Authorization: `Bearer ${this.connection.accessToken}` },
    })
  }

  post<T>(path: string, body: Record<string, unknown>): Promise<T> {
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
        this.baseDelay
      )
    )
  }

  patch<T>(path: string, body: Record<string, unknown>): Promise<T> {
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
        this.baseDelay
      )
    )
  }

  del(path: string): Promise<void> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<void>({
            method: 'DELETE',
            url: path,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.baseDelay
      )
    )
  }
}
