import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { ElfFetcher } from '../../../src/adapters/elf-fetcher.js'
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
  iterable: AsyncIterable<string>
): Promise<string[]> {
  const lines: string[] = []
  for await (const line of iterable) lines.push(line)
  return lines
}

describe('ElfFetcher', () => {
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
          Readable.from(Buffer.from('header1\ndata1a\ndata1b\n'))
        )
        .mockResolvedValueOnce(Readable.from(Buffer.from('header2\ndata2a\n'))),
    })

    const sut = new ElfFetcher(sfPort, 'LightningPageView', 'Daily')
    const result = await sut.fetch(
      Watermark.fromString('2026-02-28T00:00:00.000Z')
    )
    const lines = await collectLines(result.lines)

    expect(lines).toEqual(['data1a', 'data1b', 'data2a'])
    expect(result.fileCount()).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then yields zero lines and watermark is undefined', async () => {
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
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

    const sut = new ElfFetcher(sfPort, 'Login', 'Hourly')
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
        .mockResolvedValue(Readable.from(Buffer.from('hdr\ndata\n'))),
    })

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
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

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    const soql: string = querySpy.mock.calls[0][0]
    expect(soql).toContain('ORDER BY LogDate ASC')
    expect(soql).not.toContain('LIMIT')
  })

  it('given invalid eventType, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    const act = () => new ElfFetcher(sfPort, 'bad type!', 'Daily')
    expect(act).toThrow('Invalid eventType')
  })

  it('given invalid interval, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    const act = () => new ElfFetcher(sfPort, 'Login', 'Weekly')
    expect(act).toThrow('Invalid interval')
  })

  it('given paginated ELF records, when fetching, then collects all records and yields lines from all blobs', async () => {
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
        .mockResolvedValueOnce(Readable.from(Buffer.from('h1\na\n')))
        .mockResolvedValueOnce(Readable.from(Buffer.from('h2\nb\n'))),
    })

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
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
          Readable.from(Buffer.from('header\n\ndata1\n\ndata2\n'))
        ),
    })

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
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

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const linesPromise = collectLines(result.lines)
    // Wait for async generator to start iterating and wire up getBlobStream mocks
    await vi.waitFor(() => expect(resolveBlob2).toBeDefined(), {
      timeout: 2000,
    })
    resolveBlob2(Readable.from(Buffer.from('h2\nfrom_blob2\n')))
    resolveBlob1(Readable.from(Buffer.from('h1\nfrom_blob1\n')))
    const lines = await linesPromise

    expect(lines).toEqual(['from_blob2', 'from_blob1'])
    expect(order).toEqual(['blob2-resolved', 'blob1-resolved'])
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
        .mockResolvedValueOnce(Readable.from(Buffer.from('h\nok\n')))
        .mockRejectedValueOnce(new Error('stream failure')),
    })

    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()

    await expect(collectLines(result.lines)).rejects.toThrow('stream failure')
  })
})
