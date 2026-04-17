import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SalesforceClient } from '../../../src/adapters/sf-client.js'
import {
  type QueryResult,
  type SalesforcePort,
} from '../../../src/ports/types.js'

interface MockConnection {
  version: string
  instanceUrl?: string
  accessToken?: string
  refreshAuth?: () => Promise<void>
  request: (...args: unknown[]) => unknown
}

function mockConnection(
  overrides: Partial<MockConnection> = {}
): MockConnection {
  return {
    version: '62.0',
    request: vi.fn(),
    ...overrides,
  }
}

describe('SalesforceClient', () => {
  let requestSpy: ReturnType<typeof vi.fn>
  let sut: SalesforcePort

  beforeEach(() => {
    requestSpy = vi.fn()
    sut = new SalesforceClient(
      mockConnection({ request: requestSpy }) as never,
      { retryBaseDelayMs: 0 }
    )
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

    it('given an absolute URL matching instanceUrl, when querying more, then allows the request', async () => {
      // Arrange
      const instanceUrl = 'https://my-org.my.salesforce.com'
      const absoluteUrl = `${instanceUrl}/services/data/v62.0/query/01gxx-2000`
      const client = new SalesforceClient(
        mockConnection({ request: requestSpy, instanceUrl }) as never,
        { retryBaseDelayMs: 0 }
      )
      requestSpy.mockResolvedValue({ totalSize: 0, done: true, records: [] })

      // Act
      await client.queryMore(absoluteUrl)

      // Assert
      expect(requestSpy).toHaveBeenCalled()
    })

    it.each([
      ['off-origin', 'https://attacker.example.com/steal-token'],
      [
        'suffix bypass (host as prefix)',
        'https://my-org.my.salesforce.com.attacker.com/x',
      ],
      ['userinfo bypass', 'https://my-org.my.salesforce.com@attacker.com/x'],
      ['protocol-relative', '//attacker.example.com/steal-token'],
      ['empty string', ''],
      ['malformed URL', 'https://['],
    ])('given %s nextRecordsUrl, when querying more, then throws without making a request', (_name, badUrl) => {
      // Arrange — defend against malicious server redirecting next page to an attacker host
      const client = new SalesforceClient(
        mockConnection({
          request: requestSpy,
          instanceUrl: 'https://my-org.my.salesforce.com',
        }) as never,
        { retryBaseDelayMs: 0 }
      )

      // Act & Assert — validation throws synchronously before the request is made
      expect(() => client.queryMore(badUrl)).toThrow(/Refusing|empty/)
      expect(requestSpy).not.toHaveBeenCalled()
    })

    it('given uppercased-host nextRecordsUrl matching instanceUrl origin, when querying more, then allows the request', async () => {
      // Arrange — URL.origin normalises the host to lowercase, so this is safe to follow
      const client = new SalesforceClient(
        mockConnection({
          request: requestSpy,
          instanceUrl: 'https://my-org.my.salesforce.com',
        }) as never,
        { retryBaseDelayMs: 0 }
      )
      requestSpy.mockResolvedValue({ totalSize: 0, done: true, records: [] })

      // Act
      await client.queryMore(
        'https://MY-ORG.MY.SALESFORCE.COM/services/data/v62.0/query/01gxx-2000'
      )

      // Assert
      expect(requestSpy).toHaveBeenCalled()
    })

    it('given absolute nextRecordsUrl with no instanceUrl configured, when querying more, then throws', () => {
      // Arrange — defense in depth: if the client is built without an instanceUrl we cannot verify origin
      const client = new SalesforceClient(
        mockConnection({
          request: requestSpy,
          instanceUrl: undefined,
        }) as never,
        { retryBaseDelayMs: 0 }
      )

      // Act & Assert
      expect(() =>
        client.queryMore('https://my-org.my.salesforce.com/services/...')
      ).toThrow(/without instanceUrl/)
      expect(requestSpy).not.toHaveBeenCalled()
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
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth: vi.fn(),
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act
      const stream = await sfClient.getBlobStream(
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
        {
          headers: {
            Authorization: 'Bearer token123',
            'Accept-Encoding': 'gzip',
          },
        }
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
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth,
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act
      const stream = await sfClient.getBlobStream('/path')

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
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth: vi.fn(),
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act & Assert
      await expect(sfClient.getBlobStream('/path')).rejects.toThrow('HTTP 500')
    })

    it('given 200 response, when getBlobStream, then does not call refreshAuth', async () => {
      // Arrange
      const fetchSpy = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: new ReadableStream({
          start(controller) {
            controller.close()
          },
        }),
      })
      vi.stubGlobal('fetch', fetchSpy)
      const refreshAuth = vi.fn()
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth,
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act
      await sfClient.getBlobStream('/path')

      // Assert
      expect(refreshAuth).not.toHaveBeenCalled()
    })

    it('given undefined accessToken, when getBlobStream, then sends empty Bearer header and relies on 401 retry path', async () => {
      // Arrange — kills the `accessToken ?? ''` fallback mutation
      const csvContent = 'body'
      const fetchSpy = vi.fn().mockResolvedValueOnce({
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
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: undefined,
        refreshAuth: vi.fn(),
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act
      await sfClient.getBlobStream('/path')

      // Assert — empty Bearer, not "Bearer undefined"
      expect(fetchSpy).toHaveBeenCalledWith(
        'https://test.salesforce.com/path',
        expect.objectContaining({
          headers: expect.objectContaining({ Authorization: 'Bearer ' }),
        })
      )
    })

    it('given non-ok response, when getBlobStream, then thrown error has statusCode', async () => {
      // Arrange
      vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue({ ok: false, status: 500, body: null })
      )
      const connection = mockConnection({
        instanceUrl: 'https://test.salesforce.com',
        accessToken: 'token123',
        refreshAuth: vi.fn(),
        request: vi.fn(),
      })
      const sfClient = new SalesforceClient(connection as never, {
        retryBaseDelayMs: 0,
      })

      // Act
      let thrownError: unknown
      try {
        await sfClient.getBlobStream('/path')
      } catch (err) {
        thrownError = err
      }

      // Assert
      expect((thrownError as { statusCode: number }).statusCode).toBe(500)
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

  describe('del', () => {
    it('given a path, when deleting, then sends DELETE with gzip header', async () => {
      // Arrange
      requestSpy.mockResolvedValue(undefined)

      // Act
      await sut.del(
        '/services/data/v62.0/sobjects/InsightsExternalData/06V000000000001'
      )

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'DELETE',
        url: '/services/data/v62.0/sobjects/InsightsExternalData/06V000000000001',
        headers: { 'Accept-Encoding': 'gzip' },
      })
    })

    it('given server error, when deleting, then propagates error', async () => {
      // Arrange
      requestSpy.mockRejectedValue(new Error('delete failed'))

      // Act & Assert
      await expect(
        sut.del('/services/data/v62.0/sobjects/InsightsExternalData/06V')
      ).rejects.toThrow('delete failed')
    })
  })

  describe('concurrency', () => {
    it('given concurrency of 2, when making 4 parallel requests, then at most 2 run simultaneously', async () => {
      // Arrange
      let running = 0
      let maxRunning = 0
      const sfClient = new SalesforceClient(
        mockConnection({
          request: async () => {
            running++
            maxRunning = Math.max(maxRunning, running)
            await new Promise(r => setTimeout(r, 50))
            running--
            return { totalSize: 0, done: true, records: [] }
          },
        }) as never,
        { concurrency: 2, retryBaseDelayMs: 0 }
      )

      // Act
      await Promise.all([
        sfClient.query('SELECT Id FROM A'),
        sfClient.query('SELECT Id FROM B'),
        sfClient.query('SELECT Id FROM C'),
        sfClient.query('SELECT Id FROM D'),
      ])

      // Assert
      expect(maxRunning).toBe(2)
    })
  })

  describe('isHttpError', () => {
    it('given null thrown, when querying, then wraps in Error without retry', async () => {
      // Arrange — kills `err !== null` check: without it, `'statusCode' in null` throws TypeError
      requestSpy.mockRejectedValue(null)

      // Act & Assert — formatError(null) = Error('null'); TypeError message would not contain 'null'
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow('null')
      expect(requestSpy).toHaveBeenCalledTimes(1)
    })

    it('given null thrown, when querying, then throws Error with exactly message null', async () => {
      // Arrange
      requestSpy.mockRejectedValue(null)

      // Act
      let thrownError: unknown
      try {
        await sut.query('SELECT Id FROM Account')
      } catch (err) {
        thrownError = err
      }

      // Assert
      expect(thrownError).toBeInstanceOf(Error)
      expect((thrownError as Error).message).toMatch(/^null$/)
      expect(requestSpy).toHaveBeenCalledTimes(1)
    })

    it('given plain string thrown, when querying, then wraps in Error without retry', async () => {
      // Arrange — kills `typeof err === 'object'` check: string fails typeof object
      requestSpy.mockRejectedValue('raw string error')

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'raw string error'
      )
      expect(requestSpy).toHaveBeenCalledTimes(1)
    })

    it('given plain string thrown, when querying, then wraps in Error (not TypeError)', async () => {
      // Arrange — kills L17 ConditionalExpression: typeof guard → true makes
      // 'statusCode' in 'string' throw TypeError whose message contains the string value,
      // so toThrow('raw string') passes but it is NOT a wrapped Error instance
      requestSpy.mockRejectedValue('raw string error')

      // Act
      let caught: unknown
      try {
        await sut.query('SELECT Id FROM Account')
      } catch (e) {
        caught = e
      }

      // Assert
      expect(caught).toBeInstanceOf(Error)
      expect(caught).not.toBeInstanceOf(TypeError)
    })

    it('given object without statusCode thrown, when querying, then does not retry', async () => {
      // Arrange — kills `statusCode in err` check: object without statusCode is not HTTP error
      requestSpy.mockRejectedValue({ message: 'no status code' })

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow()
      expect(requestSpy).toHaveBeenCalledTimes(1)
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

    it('given HTTP 429 on all attempts, when querying, then delays between retries but not after the last', async () => {
      // Arrange — kills `attempt < MAX_RETRIES - 1` → `attempt < MAX_RETRIES` mutation
      // Original: 2 delays (after attempts 0 and 1, not after final attempt 2)
      // Mutation: 3 delays (also waits after the last attempt before the loop exits)
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      requestSpy.mockRejectedValue(error429)
      const setTimeoutSpy = vi.spyOn(global, 'setTimeout')

      // Act
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'Too Many Requests'
      )

      // Assert
      expect(setTimeoutSpy).toHaveBeenCalledTimes(2)
      setTimeoutSpy.mockRestore()
    })

    it('given HTTP 429, when retrying, then delay uses exponential backoff with random=0', async () => {
      // Arrange — with random()=0: delay = halfDelay + 0 = halfDelay
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      const mathRandomSpy = vi.spyOn(Math, 'random').mockReturnValue(0)
      const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
      const sfClient = new SalesforceClient(
        mockConnection({ request: requestSpy }) as never,
        { retryBaseDelayMs: 100 }
      )
      requestSpy
        .mockRejectedValueOnce(error429)
        .mockRejectedValueOnce(error429)
        .mockResolvedValueOnce({ totalSize: 0, done: true, records: [] })

      // Act
      await sfClient.query('SELECT Id FROM Account')

      // Assert — attempt 0: (100*1)/2 = 50; attempt 1: (100*2)/2 = 100
      expect(setTimeoutSpy).toHaveBeenNthCalledWith(1, expect.any(Function), 50)
      expect(setTimeoutSpy).toHaveBeenNthCalledWith(
        2,
        expect.any(Function),
        100
      )
      mathRandomSpy.mockRestore()
      setTimeoutSpy.mockRestore()
    })

    it('given HTTP 429, when retrying, then delay uses exponential backoff with random=1', async () => {
      // Arrange — kills L54 arithmetic mutations:
      // original: delay = halfDelay + 1*halfDelay = 2*halfDelay
      // mutation "-": delay = halfDelay - 1*halfDelay = 0
      // mutation "/": delay = halfDelay + 1/halfDelay ≈ halfDelay
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      const mathRandomSpy = vi.spyOn(Math, 'random').mockReturnValue(1)
      const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
      const sfClient = new SalesforceClient(
        mockConnection({ request: requestSpy }) as never,
        { retryBaseDelayMs: 100 }
      )
      requestSpy
        .mockRejectedValueOnce(error429)
        .mockRejectedValueOnce(error429)
        .mockResolvedValueOnce({ totalSize: 0, done: true, records: [] })

      // Act
      await sfClient.query('SELECT Id FROM Account')

      // Assert — attempt 0: halfDelay=50, delay=50+50=100; attempt 1: halfDelay=100, delay=100+100=200
      expect(setTimeoutSpy).toHaveBeenNthCalledWith(
        1,
        expect.any(Function),
        100
      )
      expect(setTimeoutSpy).toHaveBeenNthCalledWith(
        2,
        expect.any(Function),
        200
      )
      mathRandomSpy.mockRestore()
      setTimeoutSpy.mockRestore()
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

    it('given Error with two data array items, when request fails, then joins items with "; "', async () => {
      // Arrange — kills L31 StringLiteral: join("") would produce "INVALID: badDUPLICATE: exists"
      const error = Object.assign(new Error('API failure'), {
        data: [
          { errorCode: 'INVALID', message: 'bad' },
          { errorCode: 'DUPLICATE', message: 'exists' },
        ],
      })
      requestSpy.mockRejectedValue(error)

      // Act & Assert
      await expect(sut.query('SELECT Id FROM Account')).rejects.toThrow(
        'API failure: INVALID: bad; DUPLICATE: exists'
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

    it('given non-Error object thrown, when request fails, then result is an Error instance', async () => {
      // Arrange
      requestSpy.mockRejectedValue({ code: 'CUSTOM' })

      // Act
      let thrownError: unknown
      try {
        await sut.query('SELECT Id FROM Account')
      } catch (err) {
        thrownError = err
      }

      // Assert
      expect(thrownError).toBeInstanceOf(Error)
    })
  })

  describe('default options', () => {
    it('given no options, when constructing, then exposes apiVersion from connection', () => {
      // Arrange
      const connection = mockConnection()

      // Act
      const sfClient = new SalesforceClient(connection as never)

      // Assert
      expect(sfClient.apiVersion).toBe('62.0')
    })
  })
})
