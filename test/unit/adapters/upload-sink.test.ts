import { describe, expect, it, vi } from 'vitest'
import { UploadSinkFactory } from '../../../src/adapters/upload-sink.js'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import {
  type SalesforcePort,
  type UploadListener,
} from '../../../src/ports/types.js'

function makeSfPort(overrides: Partial<SalesforcePort> = {}): SalesforcePort {
  return {
    apiVersion: '62.0',
    query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    queryMore: vi.fn(),
    getBlob: vi.fn().mockResolvedValue(''),
    getBlobStream: vi.fn(),
    post: vi.fn().mockResolvedValue({ id: '06V000000000001' }),
    patch: vi.fn().mockResolvedValue(null),
    del: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

const dsKey = DatasetKey.fromEntry({
  analyticOrg: 'ana',
  dataset: 'MyDataset',
})

describe('UploadSinkFactory', () => {
  it('given csv lines written, when process called, then creates parent, uploads parts, and triggers processing', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id","Name"\n')
    await sut.write('"001","Acme"\n')
    const result = await sut.process()

    // Assert
    expect(sfPort.post).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({
        EdgemartAlias: 'MyDataset',
        Format: 'Csv',
        Operation: 'Append',
        Action: 'None',
      })
    )
    expect(sfPort.post).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('InsightsExternalDataPart'),
      expect.objectContaining({
        InsightsExternalDataId: '06V000000000001',
        PartNumber: 1,
      })
    )
    expect(sfPort.patch).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalData/06V000000000001'),
      { Action: 'Process', Mode: 'Incremental' }
    )
    expect(result.parentId).toBe('06V000000000001')
    expect(result.partIds).toEqual(['06V000000000001'])
  })

  it('given no writes, when process called, then throws', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act & Assert
    await expect(sut.process()).rejects.toThrow()
  })

  it('given csv lines written, when abort called, then deletes InsightsExternalData record', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Overwrite')
    await sut.write('"Id"\n')
    await sut.write('"001"\n')

    // Act
    await sut.abort()

    // Assert
    expect(sfPort.del).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalData/06V')
    )
  })

  it('given no data written, when abort called, then does not call API', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Overwrite')

    // Act
    await sut.abort()

    // Assert
    expect(sfPort.del).not.toHaveBeenCalled()
  })

  it('given existing dataset metadata, when first write, then reuses existing metadata json', async () => {
    // Arrange
    const existingMeta = JSON.stringify({
      fileFormat: { charsetName: 'UTF-8' },
      objects: [{ fields: [{ name: 'Id', type: 'Text' }] }],
    })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '06V000000000002', MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
    })
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id"\n')
    await sut.write('"001"\n')
    await sut.process()

    // Assert
    expect(sfPort.post).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({
        MetadataJson: expect.any(String),
      })
    )
    const metadataB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock
      .calls[0][1].MetadataJson as string
    const decoded = Buffer.from(metadataB64, 'base64').toString('utf-8')
    expect(JSON.parse(decoded)).toMatchObject({
      fileFormat: { charsetName: 'UTF-8' },
    })
  })

  it('given multiple csv lines, when process called, then uploads at least one part', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id"\n')
    await sut.write('"001"\n')
    await sut.write('"002"\n')
    await sut.write('"003"\n')
    await sut.process()

    // Assert
    expect(sfPort.post).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalDataPart'),
      expect.objectContaining({ PartNumber: 1 })
    )
  })

  it('given invalid dataset name, when creating sink, then write throws', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const badKey = DatasetKey.fromEntry({
      analyticOrg: 'ana',
      dataset: 'bad name!',
    })
    const sut = factory.create(badKey, 'Append')

    // Act & Assert
    await expect(sut.write('"Id"\n')).rejects.toThrow('Invalid dataset name')
  })

  it('given write after abort, when writing, then throws', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')
    await sut.abort()

    // Act & Assert
    await expect(sut.write('"Id"\n')).rejects.toThrow('Sink has been aborted')
  })

  it('given no existing metadata, when first write, then generates metadata from parsed headers', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id","Name"\n')
    await sut.write('"001","Acme"\n')
    await sut.process()

    // Assert
    expect(sfPort.post).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({ MetadataJson: expect.any(String) })
    )
    const metadataB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock
      .calls[0][1].MetadataJson as string
    const decoded = JSON.parse(
      Buffer.from(metadataB64, 'base64').toString('utf-8')
    )
    expect(decoded.objects[0].fields).toHaveLength(2)
    expect(decoded.objects[0].fields[0].name).toBe('Id')
    expect(decoded.fileFormat.fieldsEnclosedBy).toBe('"')
  })

  it('given existing metadata as JSON object, when first write, then stringifies it', async () => {
    // Arrange
    const metaObj = { fileFormat: { charsetName: 'UTF-8' }, objects: [] }
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ Id: '06V000000000002', MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(metaObj),
    })
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id"\n')
    await sut.write('"001"\n')
    await sut.process()

    // Assert
    expect(sfPort.post).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({ MetadataJson: expect.any(String) })
    )
    const metadataB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock
      .calls[0][1].MetadataJson as string
    const decoded = JSON.parse(
      Buffer.from(metadataB64, 'base64').toString('utf-8')
    )
    expect(decoded).toMatchObject({ fileFormat: { charsetName: 'UTF-8' } })
  })

  it('given listener provided, when first write creates parent, then calls onParentCreated with parentId', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const listener: UploadListener = {
      onParentCreated: vi.fn(),
      onPartUploaded: vi.fn(),
    }
    const sut = factory.create(dsKey, 'Append', listener)

    // Act
    await sut.write('"Id"\n')

    // Assert
    expect(listener.onParentCreated).toHaveBeenCalledWith('06V000000000001')
  })

  it('given listener provided, when process flushes part, then calls onPartUploaded', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const listener: UploadListener = {
      onParentCreated: vi.fn(),
      onPartUploaded: vi.fn(),
    }
    const sut = factory.create(dsKey, 'Append', listener)

    // Act
    await sut.write('"Id"\n')
    await sut.write('"001"\n')
    await sut.process()

    // Assert
    expect(listener.onPartUploaded).toHaveBeenCalled()
  })

  it('given no listener provided, when writing and processing, then completes without error', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    // Act
    await sut.write('"Id"\n')
    await sut.write('"001"\n')
    const result = await sut.process()

    // Assert
    expect(result.parentId).toBe('06V000000000001')
  })

  it('given large data exceeding part max, when process called, then splits into multiple parts', async () => {
    // Arrange
    const { randomBytes } = await import('node:crypto')
    const sfPort = makeSfPort()
    const factory = new UploadSinkFactory(sfPort)
    const sut = factory.create(dsKey, 'Append')

    await sut.write('"Col"\n')
    for (let i = 0; i < 12000; i++) {
      await sut.write(`"${randomBytes(1024).toString('base64')}"\n`)
    }
    const result = await sut.process()

    // Assert
    expect(result.partIds.length).toBeGreaterThanOrEqual(2)
    const PART_MAX_BYTES = 10 * 1024 * 1024
    for (const call of (sfPort.post as ReturnType<typeof vi.fn>).mock.calls) {
      if ((call[0] as string).includes('InsightsExternalDataPart')) {
        expect((call[1].DataFile as string).length).toBeLessThanOrEqual(
          PART_MAX_BYTES
        )
      }
    }
  })
})
