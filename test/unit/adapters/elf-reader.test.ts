import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { ElfReader } from '../../../src/adapters/elf-reader.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { type SalesforcePort } from '../../../src/ports/types.js'

function makeSfPort(overrides: Partial<SalesforcePort> = {}): SalesforcePort {
  return {
    apiVersion: '62.0',
    query: vi.fn(),
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    getBlobStream: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
    ...overrides,
  }
}

async function collectLines(
  iterable: AsyncIterable<string[]>
): Promise<string[]> {
  const lines: string[] = []
  for await (const batch of iterable) lines.push(...batch)
  return lines
}

describe('ElfReader', () => {
  it('given records exist with watermark, when fetching, then yields data lines skipping headers', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(
          Readable.from([Buffer.from('header1\ndata1a\ndata1b\n')])
        )
        .mockResolvedValueOnce(
          Readable.from([Buffer.from('header2\ndata2a\n')])
        ),
    })

    const sut = new ElfReader(sfPort, 'LightningPageView', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    // blobs are fetched concurrently — order reflects completion, not insertion order
    expect([...lines].sort()).toEqual(['data1a', 'data1b', 'data2a'])
    expect(result.fileCount()).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then yields zero lines and watermark is undefined', async () => {
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(0)
    expect(result.fileCount()).toBe(0)
    expect(result.watermark()).toBeUndefined()
  })

  it('given watermark, when fetching, then includes watermark in SOQL', async () => {
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    const sut = new ElfReader(sfPort, 'Login', 'Hourly')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate > 2026-01-01T00:00:00.000Z')
  })

  it('given no watermark, when fetching, then queries only the latest record', async () => {
    const querySpy = vi.fn().mockResolvedValue({
      totalSize: 1,
      done: true,
      records: [
        { Id: '0AT1', LogDate: '2026-03-05T00:00:00.000Z', LogFile: '' },
      ],
    })
    const sfPort = makeSfPort({
      query: querySpy,
      getBlobStream: vi
        .fn()
        .mockResolvedValue(Readable.from([Buffer.from('hdr\ndata\n')])),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch()

    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('ORDER BY LogDate DESC')
    expect(soql).toContain('LIMIT 1')
  })

  it('given watermark, when fetching, then queries all records ascending without limit', async () => {
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('LIMIT')
  })

  it('given blob content split across multiple chunks, when fetching, then reassembles lines correctly', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      // Blob split mid-line: 'header\nda' + 'ta1\ndata2\n'
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([
            Buffer.from('header\nda'),
            Buffer.from('ta1\ndata2\n'),
          ])
        ),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given invalid eventType, when creating reader, then throws', () => {
    const sfPort = makeSfPort()
    const act = () => new ElfReader(sfPort, 'bad type!', 'Daily')
    expect(act).toThrow('Invalid eventType')
  })

  it('given invalid interval, when creating reader, then throws', () => {
    const sfPort = makeSfPort()
    const act = () => new ElfReader(sfPort, 'Login', 'Weekly')
    expect(act).toThrow('Invalid interval')
  })

  it('given paginated ELF records, when fetching, then prefetches next page while streaming current page blobs', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(Readable.from([Buffer.from('h1\na\n')]))
        .mockResolvedValueOnce(Readable.from([Buffer.from('h2\nb\n')])),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(2)
    expect(result.fileCount()).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given blob with empty lines, when fetching, then skips empty lines', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValue(
          Readable.from([Buffer.from('header\n\ndata1\n\ndata2\n')])
        ),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toEqual(['data1', 'data2'])
  })

  it('given multiple blobs with different download times, when fetching, then processes blobs in completion order', async () => {
    const order: string[] = []
    let resolveBlob1!: (v: Readable) => void
    let resolveBlob2!: (v: Readable) => void
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              resolveBlob1 = (v: Readable) => {
                order.push('blob1-resolved')
                resolve(v)
              }
            })
        )
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              resolveBlob2 = (v: Readable) => {
                order.push('blob2-resolved')
                resolve(v)
              }
            })
        ),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const linesPromise = collectLines(result.lines)
    // Wait for async generator to start iterating and wire up getBlobStream mocks
    await vi.waitFor(() => expect(resolveBlob2).toBeDefined(), {
      timeout: 2000,
    })
    resolveBlob2(Readable.from([Buffer.from('h2\nfrom_blob2\n')]))
    resolveBlob1(Readable.from([Buffer.from('h1\nfrom_blob1\n')]))
    const lines = await linesPromise

    expect(lines).toEqual(['from_blob2', 'from_blob1'])
    expect(order).toEqual(['blob2-resolved', 'blob1-resolved'])
  })

  it('given two pages, when fetching, then blob fetching across pages runs concurrently', async () => {
    const callOrder: string[] = []
    let resolvePage1Blob!: (v: Readable) => void

    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockImplementationOnce(
          () =>
            new Promise<Readable>(resolve => {
              callOrder.push('page1-blob-requested')
              resolvePage1Blob = (v: Readable) => {
                callOrder.push('page1-blob-resolved')
                resolve(v)
              }
            })
        )
        .mockImplementationOnce(() => {
          callOrder.push('page2-blob-requested')
          return Promise.resolve(Readable.from([Buffer.from('h2\ndata2\n')]))
        }),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const linesPromise = collectLines(result.lines)

    // page2 blob is requested concurrently while page1 blob is still pending
    await vi.waitFor(
      () => expect(callOrder).toContain('page2-blob-requested'),
      { timeout: 2000 }
    )
    expect(callOrder).not.toContain('page1-blob-resolved')

    resolvePage1Blob(Readable.from([Buffer.from('h1\ndata1\n')]))
    const lines = await linesPromise

    expect(lines).toHaveLength(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given getBlobStream rejects on second blob, when fetching, then error propagates', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlobStream: vi
        .fn()
        .mockResolvedValueOnce(Readable.from([Buffer.from('h\nok\n')]))
        .mockRejectedValueOnce(new Error('stream failure')),
    })

    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()

    await expect(collectLines(result.lines)).rejects.toThrow('stream failure')
  })

  it('given records exist, when header() called after fetch streams data, then returns first CSV line', async () => {
    const record = {
      Id: '0AT1',
      LogDate: '2024-01-01T00:00:00.000Z',
      LogFile: '',
    }
    const blob = Readable.from([
      Buffer.from('TIMESTAMP_DERIVED,USER_ID\n2024-01-01,user1'),
    ])
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [record],
      }),
      getBlobStream: vi.fn().mockResolvedValue(blob),
    })
    const sut = new ElfReader(sfPort, 'Login', 'Daily')

    const result = await sut.fetch()
    for await (const _ of result.lines) {
      /* consume */
    }
    const header = await sut.header()

    expect(header).toBe('TIMESTAMP_DERIVED,USER_ID')
  })

  it('given no records, when header() called, then returns empty string', async () => {
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })
    const sut = new ElfReader(sfPort, 'Login', 'Daily')
    await sut.fetch()
    expect(await sut.header()).toBe('')
  })

  it('given 11 concurrent blobs with slow consumer, when fetching, then no MaxListenersExceeded warning is emitted', async () => {
    // Arrange
    const maxListenerWarnings: string[] = []
    const onWarning = (w: Error) => {
      if (w.name === 'MaxListenersExceededWarning')
        maxListenerWarnings.push(w.message)
    }
    process.on('warning', onWarning)

    try {
      const records = Array.from({ length: 11 }, (_, i) => ({
        Id: `0AT${i}`,
        LogDate: `2026-03-${String(i + 1).padStart(2, '0')}T00:00:00.000Z`,
        LogFile: '',
      }))
      const sfPort = makeSfPort({
        query: vi
          .fn()
          .mockResolvedValue({ totalSize: 11, done: true, records }),
        // 4 data lines per blob → 44 total, fills objectMode highWaterMark=16 to force backpressure
        getBlobStream: vi
          .fn()
          .mockImplementation(() =>
            Promise.resolve(
              Readable.from([
                Buffer.from('header\nline1\nline2\nline3\nline4\n'),
              ])
            )
          ),
      })
      const sut = new ElfReader(sfPort, 'LightningPageView', 'Daily')

      // Act
      const result = await sut.fetch(
        Watermark.fromString('2026-02-28T00:00:00.000Z')
      )
      const lines: string[] = []
      for await (const batch of result.lines) {
        lines.push(...batch)
        await new Promise<void>(resolve => setImmediate(resolve)) // slow consumer to induce backpressure
      }

      // Assert
      expect(lines).toHaveLength(44)
      await new Promise<void>(resolve => setImmediate(resolve))
      expect(maxListenerWarnings).toHaveLength(0)
    } finally {
      process.off('warning', onWarning)
    }
  })
})
