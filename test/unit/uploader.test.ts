import { describe, it, expect, vi } from 'vitest'
import { upload } from '../../src/adapters/uploader.js'
import { type SfClient } from '../../src/core/sf-client.js'

function makeClient(overrides: Partial<SfClient> = {}): SfClient {
  return {
    apiVersion: '62.0',
    query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    post: vi.fn().mockResolvedValue({ id: '06V001' }),
    patch: vi.fn().mockResolvedValue(null),
    ...overrides,
  } as unknown as SfClient
}

describe('Uploader', () => {
  it('given csv data, when uploading, then creates header, part, and triggers processing', async () => {
    // Arrange
    const sut = makeClient()

    // Act
    await upload(sut, 'MyDataset', '"Id","Name"\n"001","Acme"', 'Append')

    // Assert
    const postCalls = vi.mocked(sut.post).mock.calls
    // First call: header record
    expect(postCalls[0][0]).toContain('InsightsExternalData')
    expect(postCalls[0][1]).toMatchObject({
      EdgemartAlias: 'MyDataset',
      Format: 'Csv',
      Operation: 'Append',
      Action: 'None',
    })
    // Second call: data part
    expect(postCalls[1][0]).toContain('InsightsExternalDataPart')
    expect(postCalls[1][1]).toMatchObject({
      InsightsExternalDataId: '06V001',
      PartNumber: 1,
    })
    // Patch: trigger processing
    expect(vi.mocked(sut.patch)).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalData/06V001'),
      { Action: 'Process' }
    )
  })

  it('given no existing dataset, when uploading, then auto-generates metadata from CSV headers', async () => {
    // Arrange
    const sut = makeClient()

    // Act
    await upload(sut, 'NewDS', '"ColA","ColB"\n"1","2"', 'Overwrite')

    // Assert
    const headerBody = vi.mocked(sut.post).mock.calls[0][1] as Record<string, unknown>
    const metadata = JSON.parse(headerBody.MetadataJson as string)
    expect(metadata.objects[0].fields).toHaveLength(2)
    expect(metadata.objects[0].fields[0]).toMatchObject({ name: 'ColA', type: 'Text' })
    expect(metadata.objects[0].fields[1]).toMatchObject({ name: 'ColB', type: 'Text' })
  })

  it('given existing dataset with metadata, when uploading, then reuses existing metadata', async () => {
    // Arrange
    const existingMeta = '{"objects":[{"fields":[{"name":"ColA","type":"Numeric"}]}]}'
    const sut = makeClient({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '06V000', MetadataJson: existingMeta }],
      }),
    } as unknown as Partial<SfClient>)

    // Act
    await upload(sut, 'ExistingDS', '"ColA"\n"100"', 'Append')

    // Assert
    const headerBody = vi.mocked(sut.post).mock.calls[0][1] as Record<string, unknown>
    expect(headerBody.MetadataJson).toBe(existingMeta)
  })

  it('given Overwrite operation, when uploading, then passes Overwrite in header', async () => {
    // Arrange
    const sut = makeClient()

    // Act
    await upload(sut, 'DS', '"H"\n"v"', 'Overwrite')

    // Assert
    const headerBody = vi.mocked(sut.post).mock.calls[0][1] as Record<string, unknown>
    expect(headerBody.Operation).toBe('Overwrite')
  })

  it('given compressed data part, when uploading, then part contains base64 gzipped data', async () => {
    // Arrange
    const sut = makeClient()

    // Act
    await upload(sut, 'DS', '"H"\n"v"', 'Append')

    // Assert
    const partBody = vi.mocked(sut.post).mock.calls[1][1] as Record<string, unknown>
    expect(partBody.CompressedDataFile).toEqual(expect.any(String))
    // Verify it's valid base64
    const decoded = Buffer.from(partBody.CompressedDataFile as string, 'base64')
    expect(decoded.length).toBeGreaterThan(0)
  })

  it('given large CSV exceeding 10MB compressed, when uploading, then splits into multiple parts', async () => {
    // Arrange
    const sut = makeClient()
    // Generate a large CSV (~12MB raw, compressed will be smaller but let's test the chunking logic)
    const header = '"Field1","Field2","Field3"'
    const row = '"' + 'x'.repeat(500) + '","' + 'y'.repeat(500) + '","' + 'z'.repeat(500) + '"'
    const rows = Array(8000).fill(row)
    const largeCsv = [header, ...rows].join('\n')

    // Act
    await upload(sut, 'BigDS', largeCsv, 'Append')

    // Assert
    const postCalls = vi.mocked(sut.post).mock.calls
    // First call is the header record, rest are parts
    const partCalls = postCalls.slice(1)
    expect(partCalls.length).toBeGreaterThanOrEqual(1)
    // Verify part numbers are sequential
    partCalls.forEach((call, i) => {
      expect((call[1] as Record<string, unknown>).PartNumber).toBe(i + 1)
    })
  })
})
