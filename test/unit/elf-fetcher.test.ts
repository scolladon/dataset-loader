import { describe, it, expect, vi } from 'vitest'
import { fetchElf } from '../../src/adapters/elf-fetcher.js'
import { type SfClient } from '../../src/core/sf-client.js'

function makeClient(overrides: Partial<SfClient> = {}): SfClient {
  return {
    apiVersion: '62.0',
    query: vi.fn(),
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    ...overrides,
  } as unknown as SfClient
}

describe('ElfFetcher', () => {
  it('given records exist, when fetching, then returns merged CSV and watermark', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [
          { Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' },
          { Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' },
        ],
      }),
      getBlob: vi
        .fn()
        .mockResolvedValueOnce('"H1","H2"\n"a","b"')
        .mockResolvedValueOnce('"H1","H2"\n"c","d"'),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchElf(sut, 'LightningPageView', 'Daily')

    // Assert
    expect(result).not.toBeNull()
    expect(result!.csv).toBe('"H1","H2"\n"a","b"\n"c","d"')
    expect(result!.newWatermark).toBe('2026-03-02T00:00:00.000Z')
  })

  it('given no records, when fetching, then returns null', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchElf(sut, 'Login', 'Hourly')

    // Assert
    expect(result).toBeNull()
  })

  it('given watermark provided, when fetching, then includes watermark in SOQL', async () => {
    // Arrange
    const querySpy = vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] })
    const sut = makeClient({ query: querySpy } as unknown as Partial<SfClient>)

    // Act
    await fetchElf(sut, 'Login', 'Daily', '2026-01-01T00:00:00.000Z')

    // Assert
    expect(querySpy).toHaveBeenCalledWith(
      expect.stringContaining('AND LogDate > 2026-01-01T00:00:00.000Z')
    )
  })

  it('given paginated results, when fetching, then follows queryMore', async () => {
    // Arrange
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: false,
        nextRecordsUrl: '/next',
        records: [{ Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' }],
      }),
      queryMore: vi.fn().mockResolvedValue({
        totalSize: 2,
        done: true,
        records: [{ Id: '0AT2', LogDate: '2026-03-02T00:00:00.000Z', LogFile: '' }],
      }),
      getBlob: vi
        .fn()
        .mockResolvedValueOnce('"H"\n"r1"')
        .mockResolvedValueOnce('"H"\n"r2"'),
    } as unknown as Partial<SfClient>)

    // Act
    const result = await fetchElf(sut, 'Login', 'Daily')

    // Assert
    expect(result!.csv).toBe('"H"\n"r1"\n"r2"')
    expect(vi.mocked(sut.queryMore)).toHaveBeenCalledWith('/next')
  })

  it('given single record, when fetching, then downloads blob via correct URL', async () => {
    // Arrange
    const getBlobSpy = vi.fn().mockResolvedValue('"H"\n"v"')
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '0AT1', LogDate: '2026-03-01T00:00:00.000Z', LogFile: '' }],
      }),
      getBlob: getBlobSpy,
    } as unknown as Partial<SfClient>)

    // Act
    await fetchElf(sut, 'Login', 'Daily')

    // Assert
    expect(getBlobSpy).toHaveBeenCalledWith('/services/data/v62.0/sobjects/EventLogFile/0AT1/LogFile')
  })
})
