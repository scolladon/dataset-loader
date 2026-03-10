import { type Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { SObjectFetcher } from '../../../src/adapters/sobject-fetcher.js'
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

async function readStream(stream: Readable): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string))
  }
  return Buffer.concat(chunks).toString()
}

async function collectStreamTexts(
  streams: AsyncIterable<Readable>
): Promise<string[]> {
  const result: string[] = []
  for await (const stream of streams) {
    result.push(await readStream(stream))
  }
  return result
}

describe('SObjectFetcher', () => {
  it('given records exist, when fetching, then yields one Readable with CSV per page', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          {
            Id: '001A',
            Name: 'Acme',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
          {
            Id: '001B',
            Name: 'Globex',
            LastModifiedDate: '2026-03-02T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const texts = await collectStreamTexts(result.streams)

    // Assert
    expect(texts).toHaveLength(1)
    expect(texts[0]).toContain('Id')
    expect(texts[0]).toContain('Acme')
    expect(texts[0]).toContain('Globex')
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
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const texts = await collectStreamTexts(result.streams)

    // Assert
    expect(texts).toHaveLength(0)
    expect(result.watermark()).toBeUndefined()
  })

  it('given paginated results, when fetching, then yields one Readable per page', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: false,
        nextRecordsUrl: '/next1',
        records: [
          { Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' },
          { Id: '002', LastModifiedDate: '2026-01-02T00:00:00.000Z' },
        ],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: true,
        records: [{ Id: '003', LastModifiedDate: '2026-01-03T00:00:00.000Z' }],
      }),
    })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const texts = await collectStreamTexts(result.streams)

    // Assert
    expect(texts).toHaveLength(2)
    expect(texts[0]).toContain('001')
    expect(texts[0]).toContain('002')
    expect(texts[1]).toContain('003')
    expect(result.watermark()?.toString()).toBe('2026-01-03T00:00:00.000Z')
  })

  it('given watermark and where clause, when fetching, then includes both in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      where: 'Industry != null',
    })
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LastModifiedDate > 2026-01-01T00:00:00.000Z')
    expect(soql).toContain('(Industry != null)')
  })

  it('given dateField not in fields, when fetching, then adds dateField to query but not to CSV headers', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001', LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
    })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const texts = await collectStreamTexts(result.streams)

    // Assert
    expect(texts).toHaveLength(1)
    expect(texts[0]).toContain('Id')
    expect(texts[0]).not.toContain('LastModifiedDate')
    expect(result.watermark()?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given limit, when fetching, then includes LIMIT clause in SOQL', async () => {
    // Arrange
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      queryLimit: 50,
    })
    await sut.fetch()

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LIMIT 50')
  })

  it('given null field value in record, when fetching, then converts to empty string in CSV', async () => {
    // Arrange
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: null,
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    // Act
    const sut = new SObjectFetcher(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const texts = await collectStreamTexts(result.streams)

    // Assert
    expect(texts).toHaveLength(1)
    expect(texts[0]).toContain('001')
    expect(texts[0]).toContain('""')
  })

  it('given invalid sobject name, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    expect(
      () =>
        new SObjectFetcher(sfPort, {
          sobject: 'bad name!',
          fields: ['Id'],
          dateField: 'LastModifiedDate',
        })
    ).toThrow('Invalid sobject')
  })

  it('given invalid field name, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    expect(
      () =>
        new SObjectFetcher(sfPort, {
          sobject: 'Account',
          fields: ['bad field!'],
          dateField: 'LastModifiedDate',
        })
    ).toThrow('Invalid field')
  })

  it('given invalid dateField name, when creating fetcher, then throws', () => {
    const sfPort = makeSfPort()
    expect(
      () =>
        new SObjectFetcher(sfPort, {
          sobject: 'Account',
          fields: ['Id'],
          dateField: 'bad date!',
        })
    ).toThrow('Invalid dateField')
  })
})
