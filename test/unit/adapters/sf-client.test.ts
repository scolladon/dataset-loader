import { type Readable } from 'node:stream'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  DEFAULT_CONCURRENCY,
  SalesforceClient,
} from '../../../src/adapters/sf-client.js'
import {
  type MultipartBody,
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
    ])(
      'given %s nextRecordsUrl, when querying more, then throws without making a request',
      (_name, badUrl) => {
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
      }
    )

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

  describe('postMultipart', () => {
    const PATH = '/services/data/v62.0/sobjects/InsightsExternalDataPart'
    const BOUNDARY_HEADER = /^multipart\/form-data; boundary="(.+)"$/

    function multipartBody(data: Buffer): MultipartBody {
      return {
        entityField: 'entity_part',
        entity: { InsightsExternalDataId: '06V000000000001', PartNumber: 1 },
        fileField: 'DataFile',
        fileName: 'part.gz',
        data,
      }
    }

    function expectedHead(boundary: string): string {
      return [
        `--${boundary}`,
        'Content-Disposition: form-data; name="entity_part"',
        'Content-Type: application/json',
        '',
        '{"InsightsExternalDataId":"06V000000000001","PartNumber":1}',
        `--${boundary}`,
        'Content-Type: application/octet-stream',
        'Content-Disposition: form-data; name="DataFile"; filename="part.gz"',
        '',
        '',
      ].join('\r\n')
    }

    function boundaryOf(call: unknown[]): string {
      const request = call[0] as { headers: Record<string, string> }
      return BOUNDARY_HEADER.exec(request.headers['Content-Type'])?.[1] ?? ''
    }

    it('given a body, when posting multipart, then request gets POST, the path, a quoted boundary and the pinned payload bytes', async () => {
      // Arrange
      const data = Buffer.from([1, 2, 3])
      requestSpy.mockResolvedValue({ id: '06W' })

      // Act
      const result = await sut.postMultipart(PATH, multipartBody(data))

      // Assert
      const request = requestSpy.mock.calls[0][0]
      const boundary = boundaryOf(requestSpy.mock.calls[0])
      expect(request.method).toBe('POST')
      expect(request.url).toBe(PATH)
      expect(request.headers['Content-Type']).toMatch(BOUNDARY_HEADER)
      expect(request.headers['Accept-Encoding']).toBe('gzip')
      expect(request.body).toEqual(
        Buffer.concat([
          Buffer.from(expectedHead(boundary), 'utf8'),
          data,
          Buffer.from(`\r\n--${boundary}--\r\n`, 'utf8'),
        ])
      )
      expect(result).toEqual({ id: '06W' })
    })

    it('given a body, when posting multipart, then the boundary starts with the dataset-loader prefix', async () => {
      // Arrange
      requestSpy.mockResolvedValue({})

      // Act
      await sut.postMultipart(PATH, multipartBody(Buffer.from([1])))

      // Assert
      expect(boundaryOf(requestSpy.mock.calls[0])).toMatch(/^dataset-loader-/)
    })

    it('given two calls, when posting multipart, then each call uses a fresh boundary', async () => {
      // Arrange
      requestSpy.mockResolvedValue({})

      // Act
      await sut.postMultipart(PATH, multipartBody(Buffer.from([1])))
      await sut.postMultipart(PATH, multipartBody(Buffer.from([1])))

      // Assert
      expect(boundaryOf(requestSpy.mock.calls[0])).not.toBe(
        boundaryOf(requestSpy.mock.calls[1])
      )
    })

    it('given data with 0x00, 0xff and invalid UTF-8 sequences, when posting multipart, then the data bytes are copied verbatim', async () => {
      // Arrange
      const data = Buffer.from([0x00, 0xff, 0xc3, 0x28, 0xfe, 0x80, 0x00])
      requestSpy.mockResolvedValue({})

      // Act
      await sut.postMultipart(PATH, multipartBody(data))

      // Assert
      const payload = requestSpy.mock.calls[0][0].body as Buffer
      const headLength = Buffer.byteLength(
        expectedHead(boundaryOf(requestSpy.mock.calls[0])),
        'utf8'
      )
      expect(payload.subarray(headLength, headLength + data.length)).toEqual(
        data
      )
    })

    it('given 429 then success, when posting multipart, then both requests carry the same Buffer instance', async () => {
      // Arrange
      const error429 = Object.assign(new Error('Too Many Requests'), {
        statusCode: 429,
      })
      requestSpy
        .mockRejectedValueOnce(error429)
        .mockResolvedValueOnce({ id: '06W' })

      // Act
      await sut.postMultipart(PATH, multipartBody(Buffer.from([1, 2, 3])))

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(2)
      expect(requestSpy.mock.calls[1][0].body).toBe(
        requestSpy.mock.calls[0][0].body
      )
    })

    it('given a non-429 error with data, when posting multipart, then the error is formatted', async () => {
      // Arrange
      const error = Object.assign(new Error('API failure'), {
        data: [{ errorCode: 'INVALID', message: 'bad' }],
      })
      requestSpy.mockRejectedValue(error)

      // Act & Assert
      await expect(
        sut.postMultipart(PATH, multipartBody(Buffer.from([1])))
      ).rejects.toThrow('API failure: INVALID: bad')
    })

    it('given the main limiter is saturated, when posting multipart, then it waits for a free slot', async () => {
      // Arrange
      let release: (value: unknown) => void = () => undefined
      requestSpy
        .mockImplementationOnce(
          () => new Promise(resolve => (release = resolve))
        )
        .mockResolvedValue({})
      const sut = new SalesforceClient(
        mockConnection({ request: requestSpy }) as never,
        { concurrency: 1, retryBaseDelayMs: 0 }
      )
      const query = sut.query('SELECT Id FROM Account')

      // Act
      const posted = sut.postMultipart(PATH, multipartBody(Buffer.from([1])))
      await vi.waitFor(() => expect(requestSpy).toHaveBeenCalledTimes(1))
      await Promise.resolve()

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(1)
      release({ totalSize: 0, done: true, records: [] })
      await query
      await posted
      expect(requestSpy).toHaveBeenCalledTimes(2)
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

  describe('describe', () => {
    it('given a sObject name, when called, then sends GET to the sObject describe endpoint and projects to the narrow DescribeField shape', async () => {
      // Arrange — full SF describe payload has many extra attrs; the adapter
      // projects down to the narrow DescribeField shape the synthesiser consumes.
      requestSpy.mockResolvedValue({
        name: 'User',
        custom: false,
        keyPrefix: '005',
        fields: [
          {
            name: 'Id',
            label: 'User ID',
            type: 'id',
            precision: 0,
            scale: 0,
            length: 18,
            referenceTo: [],
            relationshipName: null,
            extraAttr: 'ignored',
          },
          {
            name: 'ProfileId',
            label: 'Profile ID',
            type: 'reference',
            referenceTo: ['Profile'],
            relationshipName: 'Profile',
          },
        ],
      })

      // Act
      const result = await sut.describe('User')

      // Assert
      expect(requestSpy).toHaveBeenCalledWith({
        method: 'GET',
        url: '/services/data/v62.0/sobjects/User/describe',
        headers: { 'Accept-Encoding': 'gzip' },
      })
      expect(result).toEqual({
        name: 'User',
        fields: [
          {
            name: 'Id',
            label: 'User ID',
            type: 'id',
            precision: 0,
            scale: 0,
            length: 18,
            referenceTo: [],
            relationshipName: null,
          },
          {
            name: 'ProfileId',
            label: 'Profile ID',
            type: 'reference',
            precision: undefined,
            scale: undefined,
            referenceTo: ['Profile'],
            relationshipName: 'Profile',
          },
        ],
      })
    })

    it('given two sequential calls for the same sObject, when called, then exactly one underlying request fires', async () => {
      // Arrange
      requestSpy.mockResolvedValue({ name: 'User', fields: [] })

      // Act
      await sut.describe('User')
      await sut.describe('User')

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(1)
    })

    it('given two concurrent calls for the same sObject, when awaited, then exactly one in-flight request fires and both resolve to the same payload', async () => {
      // Arrange — never resolve the request until both callers have arrived,
      // proving the cache shares the in-flight Promise, not the resolved value.
      let resolveRequest!: (value: unknown) => void
      const pending = new Promise(resolve => {
        resolveRequest = resolve
      })
      requestSpy.mockReturnValue(pending)

      // Act
      const a = sut.describe('User')
      const b = sut.describe('User')
      resolveRequest({ name: 'User', fields: [] })
      const [resA, resB] = await Promise.all([a, b])

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(1)
      expect(resA).toEqual(resB)
    })

    it('given different sObjects, when called, then each fires its own request', async () => {
      // Arrange
      requestSpy.mockImplementation(({ url }: { url: string }) =>
        Promise.resolve({
          name: url.split('/').slice(-2, -1)[0],
          fields: [],
        })
      )

      // Act
      await sut.describe('User')
      await sut.describe('Profile')

      // Assert
      expect(requestSpy).toHaveBeenCalledTimes(2)
    })

    it('given the underlying request fails, when called, then the formatted SF error propagates and the failure is NOT cached', async () => {
      // Arrange
      requestSpy
        .mockRejectedValueOnce(
          Object.assign(new Error('INVALID_TYPE'), {
            data: [
              { errorCode: 'INVALID_TYPE', message: 'sObject not supported' },
            ],
          })
        )
        .mockResolvedValueOnce({ name: 'User', fields: [] })

      // Act / Assert — first call surfaces a formatted error
      await expect(sut.describe('User')).rejects.toThrow(/INVALID_TYPE/)

      // Act / Assert — second call retries (cache must not memoise failures)
      const result = await sut.describe('User')
      expect(result).toEqual({ name: 'User', fields: [] })
      expect(requestSpy).toHaveBeenCalledTimes(2)
    })
  })

  describe('getBlobStream slot holding', () => {
    const instanceUrl = 'https://test.salesforce.com'
    // The rule itself, as a literal: 20 of the 25 slots go to downloads.
    const DOWNLOAD_SLOTS = 20

    afterEach(() => vi.unstubAllGlobals())

    function okResponse(text: string) {
      return {
        ok: true,
        status: 200,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(text))
            controller.close()
          },
        }),
      }
    }

    function openResponse() {
      return { ok: true, status: 200, body: new ReadableStream() }
    }

    function erroringResponse(error: Error) {
      return {
        ok: true,
        status: 200,
        body: new ReadableStream({
          start(controller) {
            controller.error(error)
          },
        }),
      }
    }

    async function drain(stream: Readable): Promise<string> {
      let text = ''
      for await (const chunk of stream) text += chunk.toString()
      return text
    }

    const tick = () => new Promise(resolve => setTimeout(resolve, 0))

    function track(promise: Promise<unknown>) {
      const state = { settled: false }
      promise.then(
        () => {
          state.settled = true
        },
        () => {
          state.settled = true
        }
      )
      return state
    }

    function buildClient(
      options: { concurrency?: number } = {},
      overrides: Partial<MockConnection> = {}
    ) {
      return new SalesforceClient(
        mockConnection({
          instanceUrl,
          accessToken: 't',
          refreshAuth: vi.fn(),
          ...overrides,
        }) as never,
        { retryBaseDelayMs: 0, ...options }
      )
    }

    async function expectNextDownloadProceeds(sut: SalesforceClient) {
      const next = await sut.getBlobStream('/blob/next')
      expect(await drain(next)).toBe('data')
    }

    it('given one download slot and an open first body, when a second getBlobStream is issued, then it resolves only after the first body is drained', async () => {
      // Arrange
      const fetchSpy = vi
        .fn()
        .mockImplementation(() => Promise.resolve(okResponse('data')))
      vi.stubGlobal('fetch', fetchSpy)
      const sut = buildClient({ concurrency: 1 })
      const first = await sut.getBlobStream('/blob/1')

      // Act
      const secondPromise = sut.getBlobStream('/blob/2')
      const second = track(secondPromise)
      await tick()

      // Assert
      expect(fetchSpy).toHaveBeenCalledTimes(1)
      expect(second.settled).toBe(false)
      await drain(first)
      const secondStream = await secondPromise
      expect(fetchSpy).toHaveBeenCalledTimes(2)
      await drain(secondStream)
    })

    it('given DEFAULT_CONCURRENCY plus extra downloads and queries, when all are issued, then in-flight calls never exceed DEFAULT_CONCURRENCY', async () => {
      // Arrange
      const downloadCount = 30
      const queryCount = 10
      let inFlight = 0
      let peak = 0
      let settled = 0
      const streams: Readable[] = []
      const releases: Array<() => void> = []
      const enter = () => {
        inFlight++
        peak = Math.max(peak, inFlight)
      }
      vi.stubGlobal(
        'fetch',
        vi.fn().mockImplementation(() => {
          enter()
          return Promise.resolve(openResponse())
        })
      )
      const request = vi.fn().mockImplementation(
        () =>
          new Promise(resolve => {
            enter()
            releases.push(() => {
              inFlight--
              resolve({ totalSize: 0, done: true, records: [] })
            })
          })
      )
      const sut = buildClient({}, { request })
      const calls = [
        ...Array.from({ length: downloadCount }, (_, i) =>
          sut.getBlobStream(`/blob/${i}`).then(stream => streams.push(stream))
        ),
        ...Array.from({ length: queryCount }, () => sut.query('SELECT Id')),
      ].map(call => call.then(() => settled++))

      // Act
      while (settled < downloadCount + queryCount) {
        await tick()
        const release = releases.shift()
        if (release) release()
        else if (streams.length > 0) {
          inFlight--
          streams.shift()?.destroy()
        }
      }
      await Promise.all(calls)

      // Assert
      expect(peak).toBe(DEFAULT_CONCURRENCY)
    })

    describe('slot release paths at one download slot', () => {
      it('given a body that errors, when it is consumed, then it rejects and the next download proceeds', async () => {
        // Arrange
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce(erroringResponse(new Error('reset')))
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 })
        const first = await sut.getBlobStream('/blob/1')

        // Act & Assert
        await expect(drain(first)).rejects.toThrow('reset')
        await expectNextDownloadProceeds(sut)
      })

      it('given an open body, when the consumer destroys it, then the next download proceeds', async () => {
        // Arrange
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce(openResponse())
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 })
        const first = await sut.getBlobStream('/blob/1')

        // Act
        first.destroy()

        // Assert
        await expectNextDownloadProceeds(sut)
      })

      it('given fetch rejects, when downloading, then it rejects and the next download proceeds', async () => {
        // Arrange
        const fetchSpy = vi
          .fn()
          .mockRejectedValueOnce(new Error('network down'))
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 })

        // Act & Assert
        await expect(sut.getBlobStream('/blob/1')).rejects.toThrow(
          'network down'
        )
        await expectNextDownloadProceeds(sut)
      })

      it('given a non-ok status, when downloading, then it rejects and the next download proceeds', async () => {
        // Arrange
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce({ ok: false, status: 500, body: null })
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 })

        // Act & Assert
        await expect(sut.getBlobStream('/blob/1')).rejects.toThrow('HTTP 500')
        await expectNextDownloadProceeds(sut)
      })

      it('given a 401 then a 200, when downloading, then both fetches share one slot released after the drain', async () => {
        // Arrange
        const refreshAuth = vi.fn().mockResolvedValue(undefined)
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce({ ok: false, status: 401, body: null })
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 }, { refreshAuth })
        const first = await sut.getBlobStream('/blob/1')
        const secondPromise = sut.getBlobStream('/blob/2')
        await tick()

        // Act
        const callsBeforeDrain = fetchSpy.mock.calls.length
        await drain(first)
        await drain(await secondPromise)

        // Assert
        expect(callsBeforeDrain).toBe(2)
        expect(fetchSpy).toHaveBeenCalledTimes(3)
        expect(refreshAuth).toHaveBeenCalledTimes(1)
      })

      it('given a 401 whose refresh rejects, when downloading, then it rejects and the next download proceeds', async () => {
        // Arrange
        const refreshAuth = vi
          .fn()
          .mockRejectedValueOnce(new Error('refresh failed'))
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce({ ok: false, status: 401, body: null })
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 }, { refreshAuth })

        // Act & Assert
        await expect(sut.getBlobStream('/blob/1')).rejects.toThrow(
          'refresh failed'
        )
        await expectNextDownloadProceeds(sut)
      })

      it('given a 429 then a 200, when downloading, then one stream is opened and the slot is released after the drain', async () => {
        // Arrange
        const fetchSpy = vi
          .fn()
          .mockResolvedValueOnce({ ok: false, status: 429, body: null })
          .mockImplementation(() => Promise.resolve(okResponse('data')))
        vi.stubGlobal('fetch', fetchSpy)
        const sut = buildClient({ concurrency: 1 })
        const first = await sut.getBlobStream('/blob/1')
        const secondPromise = sut.getBlobStream('/blob/2')
        await tick()

        // Act
        const callsBeforeDrain = fetchSpy.mock.calls.length
        await drain(first)
        await drain(await secondPromise)

        // Assert
        expect(callsBeforeDrain).toBe(2)
        expect(fetchSpy).toHaveBeenCalledTimes(3)
      })
    })

    it('given one client as source and target with DEFAULT_CONCURRENCY open blob bodies, when posting, then the post still gets a slot', async () => {
      // Arrange
      const fetchSpy = vi
        .fn()
        .mockImplementation(() => Promise.resolve(openResponse()))
      vi.stubGlobal('fetch', fetchSpy)
      const request = vi.fn().mockResolvedValue({ id: '0Pp000000000001' })
      const sut = buildClient({}, { request })
      const downloads = Array.from({ length: DEFAULT_CONCURRENCY }, (_, i) =>
        sut.getBlobStream(`/blob/${i}`)
      )
      const opened = await Promise.all(downloads.slice(0, DOWNLOAD_SLOTS))

      // Act
      const result = await sut.post(
        '/services/data/v62.0/sobjects/InsightsExternalData',
        {}
      )

      // Assert
      expect(result).toEqual({ id: '0Pp000000000001' })
      expect(fetchSpy).toHaveBeenCalledTimes(DOWNLOAD_SLOTS)
      opened.forEach(stream => stream.destroy())
      const rest = await Promise.all(downloads.slice(DOWNLOAD_SLOTS))
      rest.forEach(stream => stream.destroy())
    })

    it('given every download slot holds an open body, when one more getBlobStream is issued, then it waits until one body is destroyed', async () => {
      // Arrange
      vi.stubGlobal(
        'fetch',
        vi.fn().mockImplementation(() => Promise.resolve(openResponse()))
      )
      const sut = buildClient()
      const opened = await Promise.all(
        Array.from({ length: DOWNLOAD_SLOTS }, (_, i) =>
          sut.getBlobStream(`/blob/${i}`)
        )
      )
      const extraPromise = sut.getBlobStream('/blob/extra')
      const extra = track(extraPromise)
      await tick()

      // Act
      const pendingBefore = !extra.settled
      opened[0].destroy()
      const extraStream = await extraPromise

      // Assert
      expect(pendingBefore).toBe(true)
      extraStream.destroy()
      opened.slice(1).forEach(stream => stream.destroy())
    })

    it('given concurrency 2 and one open blob body, when a second getBlobStream and a query are issued, then the query resolves and the second download waits for the first to close', async () => {
      // Arrange
      const fetchSpy = vi
        .fn()
        .mockResolvedValueOnce(openResponse())
        .mockImplementation(() => Promise.resolve(openResponse()))
      vi.stubGlobal('fetch', fetchSpy)
      const result = { totalSize: 0, done: true, records: [] }
      const sut = buildClient(
        { concurrency: 2 },
        { request: vi.fn().mockResolvedValue(result) }
      )
      const first = await sut.getBlobStream('/blob/1')
      const secondPromise = sut.getBlobStream('/blob/2')
      const second = track(secondPromise)

      // Act
      const queried = await sut.query('SELECT Id')
      await tick()

      // Assert
      expect(queried).toEqual(result)
      expect(fetchSpy).toHaveBeenCalledTimes(1)
      expect(second.settled).toBe(false)
      first.destroy()
      const secondStream = await secondPromise
      secondStream.destroy()
    })
  })
})
