import { describe, it, expect, vi } from 'vitest'
import { fetchSObject } from '../../src/adapters/sobject-fetcher.js'
import { type SfClient } from '../../src/core/sf-client.js'

function makeClient(overrides: Partial<SfClient> = {}): SfClient {
  return {
    apiVersion: '62.0',
    query: vi.fn(),
    queryMore: vi.fn(),
    ...overrides,
  } as unknown as SfClient
}

describe('SObjectFetcher', () => {
  it('given records exist, when fetching, then returns CSV with headers and watermark', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '001A', Name: 'Acme', LastModifiedDate: '2026-03-01T00:00:00.000Z' },
          { Id: '001B', Name: 'Globex', LastModifiedDate: '2026-03-02T00:00:00.000Z' },
        ],
      }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchSObject(sut, 'Account', ['Id', 'Name', 'LastModifiedDate'], 'LastModifiedDate')

    // Assert
    expect(result).not.toBeNull()
    const lines = result!.csv.split('\n')
    expect(lines[0]).toBe('"Id","Name","LastModifiedDate"')
    expect(lines[1]).toBe('"001A","Acme","2026-03-01T00:00:00.000Z"')
    expect(lines[2]).toBe('"001B","Globex","2026-03-02T00:00:00.000Z"')
    expect(result!.newWatermark).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then returns null', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchSObject(sut, 'Account', ['Id'], 'LastModifiedDate')

    // Assert
    expect(result).toBeNull()
  })

  it('given watermark and where clause, when fetching, then includes both in SOQL', async () => {
    // Arrange
    const querySpy = vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sut = makeClient({ query: querySpy } as unknown as Partial<SfClient>)

    // Act
    await fetchSObject(sut, 'Account', ['Id'], 'LastModifiedDate', '2026-01-01T00:00:00.000Z', 'Industry != null')

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toContain('WHERE LastModifiedDate > 2026-01-01T00:00:00.000Z AND (Industry != null)')
    expect(soql).toContain('ORDER BY LastModifiedDate ASC')
  })

  it('given no watermark or where, when fetching, then SOQL has no WHERE clause', async () => {
    // Arrange
    const querySpy = vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sut = makeClient({ query: querySpy } as unknown as Partial<SfClient>)

    // Act
    await fetchSObject(sut, 'Account', ['Id'], 'LastModifiedDate')

    // Assert
    const soql = querySpy.mock.calls[0][0]
    expect(soql).toBe('SELECT Id FROM Account ORDER BY LastModifiedDate ASC')
  })

  it('given paginated results, when fetching, then follows queryMore', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next',
        records: [{ Id: '001A', LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [{ Id: '001B', LastModifiedDate: '2026-03-02T00:00:00.000Z' }],
      }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchSObject(sut, 'Account', ['Id', 'LastModifiedDate'], 'LastModifiedDate')

    // Assert
    const lines = result!.csv.split('\n')
    expect(lines).toHaveLength(3)
    expect(vi.mocked(sut.queryMore)).toHaveBeenCalledWith('/next')
  })

  it('given null field values, when fetching, then outputs empty quoted strings', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001A', Name: null, LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchSObject(sut, 'Account', ['Id', 'Name', 'LastModifiedDate'], 'LastModifiedDate')

    // Assert
    expect(result!.csv.split('\n')[1]).toBe('"001A","","2026-03-01T00:00:00.000Z"')
  })

  it('given field values with quotes, when fetching, then escapes double quotes', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '001A', Name: 'Say "Hi"', LastModifiedDate: '2026-03-01T00:00:00.000Z' }],
      }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchSObject(sut, 'Account', ['Id', 'Name', 'LastModifiedDate'], 'LastModifiedDate')

    // Assert
    expect(result!.csv.split('\n')[1]).toContain('"Say ""Hi"""')
  })
})
