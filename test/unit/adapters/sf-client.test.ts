import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SalesforceClient } from '../../../src/adapters/sf-client.js'
import {
  type QueryResult,
  type SalesforcePort,
} from '../../../src/ports/types.js'

function mockConnection(requestFn: (...args: unknown[]) => unknown) {
  return { version: '62.0', request: requestFn } as never
}

describe('SalesforceClient', () => {
  let requestSpy: ReturnType<typeof vi.fn>
  let sut: SalesforcePort

  beforeEach(() => {
    requestSpy = vi.fn()
    sut = new SalesforceClient(mockConnection(requestSpy), {
      retryBaseDelayMs: 0,
    })
  })

  describe('query', () => {
    it('given a SOQL string, when querying, then sends GET with encoded SOQL and gzip header', async () => {
      // Arrange
      const expected: QueryResult<{ Id: string }> = {
        totalSize: 1,
        done: true,
        records: [{ Id: '001' }],
      }
      requestSpy.mockResolvedValue(expected)

      // Act
      const result = await sut.query('SELECT Id FROM Account')

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'GET',
        url: '/services/data/v62.0/query?q=SELECT%20Id%20FROM%20Account',
        headers: { 'Accept-Encoding': 'gzip' },
      })
      expect(result).toEqual(expected)
    })
  })

  describe('queryMore', () => {
    it('given a nextRecordsUrl, when querying more, then sends GET to that URL', async () => {
      // Arrange
      const expected: QueryResult<{ Id: string }> = {
        totalSize: 0,
        done: true,
        records: [],
      }
      requestSpy.mockResolvedValue(expected)
      const nextUrl = '/services/data/v62.0/query/01gxx-2000'

      // Act
      const result = await sut.queryMore(nextUrl)

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'GET',
        url: nextUrl,
        headers: { 'Accept-Encoding': 'gzip' },
      })
      expect(result).toEqual(expected)
    })
  })

  describe('getBlob', () => {
    it('given a blob path, when fetching, then sends GET with gzip header', async () => {
      // Arrange
      requestSpy.mockResolvedValue('csv,data\n1,2')

      // Act
      const result = await sut.getBlob(
        '/services/data/v62.0/sobjects/EventLogFile/0AT/LogFile'
      )

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'GET',
        url: '/services/data/v62.0/sobjects/EventLogFile/0AT/LogFile',
        headers: { 'Accept-Encoding': 'gzip' },
      })
      expect(result).toBe('csv,data\n1,2')
    })
  })

  describe('getBlobStream', () => {
    afterEach(() => {
      vi.unstubAllGlobals()
    })

    it('given blob path, when getBlobStream, then fetches with Bearer token and returns readable stream', async () => {
      // Arrange
      const csvContent = '"H1","H2"\n"a","b"\n"c","d"'
      const fetchSpy = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(csvContent))
            controller.close()
          },
        }),
      })
      vi.stubGlobal('fetch', fetchSpy)
      const connection = {
        version: '62.0',
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth: vi.fn(),
        request: vi.fn(),
      } as never
      const sut = new SalesforceClient(connection, { retryBaseDelayMs: 0 })

      // Act
      const stream = await sut.getBlobStream(
        '/services/data/v62.0/sobjects/EventLogFile/0AT1/LogFile'
      )

      // Assert
      const chunks: Buffer[] = []
      for await (const chunk of stream) {
        chunks.push(
          Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string)
        )
      }
      expect(Buffer.concat(chunks).toString()).toBe(csvContent)
      expect(fetchSpy).toHaveBeenCalledWith(
        'https://test.salesforce.com/services/data/v62.0/sobjects/EventLogFile/0AT1/LogFile',
        { headers: { Authorization: 'Bearer token123' } }
      )
    })

    it('given 401 response, when getBlobStream, then refreshes auth and retries', async () => {
      // Arrange
      const csvContent = 'data'
      const fetchSpy = vi
        .fn()
        .mockResolvedValueOnce({ ok: false, status: 401, body: null })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          body: new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(csvContent))
              controller.close()
            },
          }),
        })
      vi.stubGlobal('fetch', fetchSpy)
      const refreshAuth = vi.fn()
      const connection = {
        version: '62.0',
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth,
        request: vi.fn(),
      } as never
      const sut = new SalesforceClient(connection, { retryBaseDelayMs: 0 })

      // Act
      const stream = await sut.getBlobStream('/path')

      // Assert
      const chunks: Buffer[] = []
      for await (const chunk of stream) {
        chunks.push(
          Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string)
        )
      }
      expect(Buffer.concat(chunks).toString()).toBe(csvContent)
      expect(refreshAuth).toHaveBeenCalledOnce()
      expect(fetchSpy).toHaveBeenCalledTimes(2)
    })

    it('given non-401 error response, when getBlobStream, then throws with status code', async () => {
      // Arrange
      vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue({ ok: false, status: 500, body: null })
      )
      const connection = {
        version: '62.0',
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth: vi.fn(),
        request: vi.fn(),
      } as never
      const sut = new SalesforceClient(connection, { retryBaseDelayMs: 0 })

      // Act & Assert
      await expect(sut.getBlobStream('/path')).rejects.toThrow('HTTP 500')
    })
  })

  describe('post', () => {
    it('given a path and body, when posting, then sends POST with JSON body and gzip headers', async () => {
      // Arrange
      const body = { EdgemartAlias: 'MyDataset' }
      requestSpy.mockResolvedValue({ id: '06V' })

      // Act
      const result = await sut.post(
        '/services/data/v62.0/sobjects/InsightsExternalData',
        body
      )

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'POST',
        url: '/services/data/v62.0/sobjects/InsightsExternalData',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Encoding': 'gzip',
        },
        body: JSON.stringify(body),
      })
      expect(result).toEqual({ id: '06V' })
    })
  })

  describe('patch', () => {
    it('given a path and body, when patching, then sends PATCH with JSON body', async () => {
      // Arrange
      const body = { Action: 'Process' }
      requestSpy.mockResolvedValue(null)

      // Act
      await sut.patch(
        '/services/data/v62.0/sobjects/InsightsExternalData/06V',
        body
      )

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'PATCH',
        url: '/services/data/v62.0/sobjects/InsightsExternalData/06V',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Encoding': 'gzip',
        },
        body: JSON.stringify(body),
      })
    })
  })

  describe('concurrency', () => {
    it('given concurrency of 2, when making 4 parallel requests, then at most 2 run simultaneously', async () => {
      // Arrange
      let running = 0
      let maxRunning = 0
      const sut = new SalesforceClient(
        mockConnection(async () => {
          running++
          maxRunning = Math.max(maxRunning, running)
          await new Promise(r => setTimeout(r, 50))
          running--
          return { totalSize: 0, done: true, records: [] }
        }),
        { concurrency: 2, retryBaseDelayMs: 0 }
      )

      // Act
      await Promise.all([
        sut.query('SELECT Id FROM A'),
        sut.query('SELECT Id FROM B'),
        sut.query('SELECT Id FROM C'),
        sut.query('SELECT Id FROM D'),
      ])

      // Assert
      expect(maxRunning).toBe(2)
    })
  })

  describe('retry', () => {
    it('given HTTP 429, when querying, then retries and succeeds on subsequent attempt', async () => {
      // Arrange
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      const expected = { totalSize: 0, done: true, records: [] }
      requestSpy
        .mockRejectedValueOnce(error429)
        .mockRejectedValueOnce(error429)
        .mockResolvedValueOnce(expected)

      // Act
      const result = await sut.query('SELECT Id FROM Account')

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(3)
      expect(result).toEqual(expected)
    })

    it('given HTTP 429 on all 3 attempts, when querying, then throws after exhausting retries', async () => {
      // Arrange
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      requestSpy.mockRejectedValue(error429)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'Too Many Requests'
      )
      expect(requestSpy).toHaveBeenCalledTimes(3)
    })

    it('given non-429 error, when querying, then throws immediately without retry', async () => {
      // Arrange
      const error500 = Object.assign(new Error('Server Error'), {
        statusCode: 500,
      })
      requestSpy.mockRejectedValue(error500)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'Server Error'
      )
      expect(requestSpy).toHaveBeenCalledTimes(1)
    })
  })

  describe('formatError', () => {
    it('given non-Error thrown value, when request fails, then wraps string in Error', async () => {
      // Arrange
      requestSpy.mockRejectedValue('plain string error')

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'plain string error'
      )
    })

    it('given Error with data array, when request fails, then formats as errorCode: message', async () => {
      // Arrange
      const error = Object.assign(new Error('API failure'), {
        data: [{ errorCode: 'INVALID', message: 'bad' }],
      })
      requestSpy.mockRejectedValue(error)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'API failure: INVALID: bad'
      )
    })

    it('given Error with data array item missing errorCode and message, when request fails, then JSON-stringifies item', async () => {
      // Arrange
      const error = Object.assign(new Error('API failure'), {
        data: [{ something: 'else' }],
      })
      requestSpy.mockRejectedValue(error)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'API failure: {"something":"else"}'
      )
    })

    it('given Error with data as plain object, when request fails, then JSON.stringifies data', async () => {
      // Arrange
      const error = Object.assign(new Error('API failure'), {
        data: { detail: 'some info' },
      })
      requestSpy.mockRejectedValue(error)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'API failure: {"detail":"some info"}'
      )
    })
  })

  describe('default options', () => {
    it('given no options, when constructing, then exposes apiVersion from connection', () => {
      // Act
      const sut = new SalesforceClient(mockConnection(vi.fn()))

      // Assert
      expect(sut.apiVersion).toBe('62.0')
    })
  })
})
