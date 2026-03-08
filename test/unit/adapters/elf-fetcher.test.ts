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

async function collectStreams(
  streams: AsyncIterable<Readable>
): Promise<Readable[]> {
  const result: Readable[] = []
  for await (const stream of streams) {
    result.push(stream)
  }
  return result
}

describe('ElfFetcher', () => {
  it('given records exist, when fetching, then yields one Readable per record', async () => {
    // Arrange
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
        .mockResolvedValueOnce(Readable.from(Buffer.from('blob1')))
        .mockResolvedValueOnce(Readable.from(Buffer.from('blob2'))),
    })

    // Act
    const sut = new ElfFetcher(sfPort, 'LightningPageView', 'Daily')
    const result = await sut.fetch()
    const streams = await collectStreams(result.streams)

    // Assert
    expect(streams).toHaveLength(2)
    expect(result.totalHint).toBe(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then yields zero streams and watermark is undefined', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    // Act
    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const streams = await collectStreams(result.streams)

    // Assert
    expect(streams).toHaveLength(0)
    expect(result.totalHint).toBe(0)
    expect(result.watermark()).toBeUndefined()
  })

  it('given watermark, when fetching, then includes watermark in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new ElfFetcher(sfPort, 'Login', 'Hourly')
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LogDate > 2026-01-01T00:00:00.000Z')
  })

  it('given invalid eventType, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    expect(() => new ElfFetcher(sfPort, 'bad type!', 'Daily')).toThrow(
      'Invalid eventType'
    )
  })

  it('given invalid interval, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    expect(() => new ElfFetcher(sfPort, 'Login', 'Weekly')).toThrow(
      'Invalid interval'
    )
  })

  it('given paginated ELF records, when fetching, then collects all records and yields one stream per record', async () => {
    // Arrange
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
        .mockResolvedValueOnce(Readable.from(Buffer.from('a')))
        .mockResolvedValueOnce(Readable.from(Buffer.from('b'))),
    })

    // Act
    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()
    const streams = await collectStreams(result.streams)

    // Assert
    expect(streams).toHaveLength(2)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given getBlobStream rejects on second blob, when fetching, then first stream is yielded before error', async () => {
    // Arrange
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
        .mockResolvedValueOnce(Readable.from(Buffer.from('ok')))
        .mockRejectedValueOnce(new Error('stream failure')),
    })

    // Act
    const sut = new ElfFetcher(sfPort, 'Login', 'Daily')
    const result = await sut.fetch()

    // Assert
    const streams: Readable[] = []
    await expect(async () => {
      for await (const stream of result.streams) {
        streams.push(stream)
      }
    }).rejects.toThrow('stream failure')
    expect(streams).toHaveLength(1)
  })
})
