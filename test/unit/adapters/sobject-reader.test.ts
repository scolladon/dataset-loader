import { describe, expect, it, vi } from 'vitest'
import { SObjectReader } from '../../../src/adapters/sobject-reader.js'
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

describe('SObjectReader', () => {
  it('given records exist, when fetching, then yields CSV lines without header', async () => {
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

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(2)
    expect(lines[0]).toContain('001A')
    expect(lines[0]).toContain('Acme')
    expect(lines[1]).toContain('001B')
    expect(lines[1]).toContain('Globex')
    expect(lines.join('\n')).not.toMatch(/^"Id"/)
    expect(result.watermark()?.toString()).toBe('2026-03-02T00:00:00.000Z')
    expect(result.fileCount()).toBe(1)
  })

  it('given no records, when fetching, then yields zero lines and watermark is undefined', async () => {
    const sfPort = makeSfPort({
      query: vi
        .fn()
        .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(0)
    expect(result.watermark()).toBeUndefined()
    expect(result.fileCount()).toBe(0)
  })

  it('given paginated results, when fetching, then yields lines from all pages', async () => {
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

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'LastModifiedDate'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(3)
    expect(lines[0]).toContain('001')
    expect(lines[2]).toContain('003')
    expect(result.watermark()?.toString()).toBe('2026-01-03T00:00:00.000Z')
    expect(result.fileCount()).toBe(2)
  })

  it('given watermark and where clause, when fetching, then includes both in SOQL', async () => {
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      where: 'Industry != null',
    })
    await sut.fetch(Watermark.fromString('2026-01-01T00:00:00.000Z'))

    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LastModifiedDate > 2026-01-01T00:00:00.000Z')
    expect(soql).toContain('(Industry != null)')
  })

  it('given dateField not in fields, when fetching, then adds dateField to query but not to CSV output', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001', LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
    })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('001')
    expect(lines[0]).not.toContain('LastModifiedDate')
    expect(result.watermark()?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given limit, when fetching, then includes LIMIT clause in SOQL', async () => {
    const querySpy = vi
      .fn()
      .mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sfPort = makeSfPort({ query: querySpy })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      queryLimit: 50,
    })
    await sut.fetch()

    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('LIMIT 50')
  })

  it('given null field value in record, when fetching, then converts to empty string in CSV', async () => {
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

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('001')
    expect(lines[0]).toContain('""')
  })

  it('given field value with embedded comma and quotes, when fetching, then properly CSV-quoted', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [
          {
            Id: '001',
            Name: 'O"Brien, Jr.',
            LastModifiedDate: '2026-03-01T00:00:00.000Z',
          },
        ],
      }),
    })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain('O""Brien')
  })

  it('given invalid sobject name, when creating reader, then throws', () => {
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'bad name!',
        fields: ['Id'],
        dateField: 'LastModifiedDate',
      })
    expect(act).toThrow('Invalid sobject')
  })

  it('given invalid field name, when creating reader, then throws', () => {
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'Account',
        fields: ['bad field!'],
        dateField: 'LastModifiedDate',
      })
    expect(act).toThrow('Invalid field')
  })

  it('given invalid dateField name, when creating reader, then throws', () => {
    const sfPort = makeSfPort()
    const act = () =>
      new SObjectReader(sfPort, {
        sobject: 'Account',
        fields: ['Id'],
        dateField: 'bad date!',
      })
    expect(act).toThrow('Invalid dateField')
  })

  it('given single page with records, when fetching, then watermark is last record dateField value', async () => {
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 3,
        done: true,
        records: [
          { Id: '001', LastModifiedDate: '2026-01-01T00:00:00.000Z' },
          { Id: '002', LastModifiedDate: '2026-01-02T00:00:00.000Z' },
          { Id: '003', LastModifiedDate: '2026-01-03T00:00:00.000Z' },
        ],
      }),
    })

    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    const result = await sut.fetch()
    await collectLines(result.lines)

    expect(result.watermark()?.toString()).toBe('2026-01-03T00:00:00.000Z')
  })

  it('given fields [Id, Name, CreatedDate], when header() called, then returns comma-separated field names', async () => {
    const sfPort = makeSfPort()
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name', 'CreatedDate'],
      dateField: 'CreatedDate',
    })
    expect(await sut.header()).toBe('Id,Name,CreatedDate')
  })

  it('given fields [Id, Name], when header() called before fetch(), then is available immediately', async () => {
    const sfPort = makeSfPort()
    const sut = new SObjectReader(sfPort, {
      sobject: 'Account',
      fields: ['Id', 'Name'],
      dateField: 'CreatedDate',
    })
    // No fetch() called — header is derived from config
    expect(await sut.header()).toBe('Id,Name')
  })
})
