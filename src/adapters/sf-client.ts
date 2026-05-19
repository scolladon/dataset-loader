import { Readable } from 'node:stream'

import { type Connection } from '@salesforce/core'
import pLimit from 'p-limit'
import {
  type DescribeField,
  type QueryResult,
  type SalesforcePort,
  type SObjectDescribe,
} from '../ports/types.js'

interface SalesforceClientOptions {
  readonly concurrency?: number
  readonly retryBaseDelayMs?: number
}

export const DEFAULT_CONCURRENCY = 25
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
  let lastError: unknown
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await fn()
    } catch (err: unknown) {
      lastError = err
      const isRetryable =
        isHttpError(err) && err.statusCode === 429 && attempt < MAX_RETRIES - 1
      if (!isRetryable) {
        throw formatError(err)
      }
      const halfDelay = (baseDelayMs * Math.pow(2, attempt)) / 2
      const delay = halfDelay + Math.random() * halfDelay
      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }
  throw formatError(lastError)
}

// Raw SF Describe response shape — much wider than we consume. We project
// down to the narrow `DescribeField` at the adapter boundary so the
// synthesiser never sees attributes it doesn't care about.
interface RawDescribeField {
  readonly name: string
  readonly label: string
  readonly type: string
  readonly precision?: number
  readonly scale?: number
  readonly referenceTo?: readonly string[]
  readonly relationshipName?: string | null
}

interface RawSObjectDescribe {
  readonly name: string
  readonly fields: readonly RawDescribeField[]
}

export class SalesforceClient implements SalesforcePort {
  readonly apiVersion: string
  private readonly limiter: ReturnType<typeof pLimit>
  private readonly baseDelay: number
  // Promise-valued cache so concurrent callers share one in-flight request.
  // Failures evict their slot to avoid memoising transient errors.
  private readonly describeCache = new Map<string, Promise<SObjectDescribe>>()

  constructor(
    private readonly connection: Connection,
    options: SalesforceClientOptions = {}
  ) {
    this.apiVersion = connection.version
    this.limiter = pLimit(options.concurrency ?? DEFAULT_CONCURRENCY)
    this.baseDelay = options.retryBaseDelayMs ?? DEFAULT_BASE_DELAY_MS
  }

  describe(sobjectName: string): Promise<SObjectDescribe> {
    const cached = this.describeCache.get(sobjectName)
    if (cached) return cached
    const inflight = this.fetchDescribe(sobjectName).catch((err: unknown) => {
      this.describeCache.delete(sobjectName)
      throw err
    })
    this.describeCache.set(sobjectName, inflight)
    return inflight
  }

  private fetchDescribe(sobjectName: string): Promise<SObjectDescribe> {
    return this.limiter(() =>
      withRetry(
        () =>
          this.connection.request<RawSObjectDescribe>({
            method: 'GET',
            url: `/services/data/v${this.connection.version}/sobjects/${sobjectName}/describe`,
            headers: { 'Accept-Encoding': 'gzip' },
          }),
        this.baseDelay
      ).then(projectDescribe)
    )
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
    this.assertSameOrigin(nextRecordsUrl)
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

  private assertSameOrigin(url: string): void {
    if (!url) {
      throw new Error('Refusing to follow empty nextRecordsUrl')
    }
    // Protocol-relative URLs (//host/path) resolve to an off-origin host even
    // though they start with '/' — reject them explicitly.
    if (url.startsWith('//')) {
      throw new Error(
        `Refusing to follow protocol-relative nextRecordsUrl: ${url}`
      )
    }
    if (url.startsWith('/')) return
    const base = this.connection.instanceUrl
    if (!base) {
      throw new Error(
        `Refusing to follow absolute nextRecordsUrl without instanceUrl: ${url}`
      )
    }
    let targetOrigin: string
    let baseOrigin: string
    try {
      targetOrigin = new URL(url).origin
      baseOrigin = new URL(base).origin
    } catch {
      throw new Error(`Refusing to follow malformed nextRecordsUrl: ${url}`)
    }
    // URL.origin normalises host (lowercase, punycode, default ports) and
    // ignores userinfo, so this blocks subdomain suffix, userinfo, and
    // uppercase/IDN bypass attempts that a naive startsWith misses.
    if (targetOrigin === baseOrigin) return
    throw new Error(
      `Refusing to follow nextRecordsUrl outside of instanceUrl: ${url}`
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
    const token = this.connection.accessToken ?? ''
    // Accept-Encoding: gzip is explicit here only for parity with the
    // request() call sites. Node's undici-based fetch already adds it by
    // default and auto-decompresses the body before handing it to us, so
    // this is not a wire-level change and Readable.fromWeb(res.body) receives
    // plain bytes regardless.
    return fetch(`${this.connection.instanceUrl}${path}`, {
      headers: {
        Authorization: `Bearer ${token}`,
        'Accept-Encoding': 'gzip',
      },
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

function projectDescribe(raw: RawSObjectDescribe): SObjectDescribe {
  return {
    name: raw.name,
    fields: raw.fields.map(projectField),
  }
}

function projectField(raw: RawDescribeField): DescribeField {
  return {
    name: raw.name,
    label: raw.label,
    type: raw.type,
    precision: raw.precision,
    scale: raw.scale,
    referenceTo: raw.referenceTo,
    relationshipName: raw.relationshipName,
  }
}
