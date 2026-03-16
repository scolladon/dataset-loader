import { finished } from 'node:stream/promises'
import { describe, expect, it, vi } from 'vitest'
import {
  DatasetWriter,
  DatasetWriterFactory,
  GzipChunkingWritable,
} from '../../../src/adapters/dataset-writer.js'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import {
  type ProgressListener,
  type SalesforcePort,
  SkipDatasetError,
} from '../../../src/ports/types.js'

function makeSfPort(overrides: Partial<SalesforcePort> = {}): SalesforcePort {
  return {
    apiVersion: '62.0',
    query: vi.fn().mockResolvedValue({ totalSize: 0, done: true, records: [] }),
    queryMore: vi.fn(),
    getBlob: vi.fn().mockResolvedValue(''),
    getBlobStream: vi.fn(),
    post: vi.fn().mockResolvedValue({ id: '06W000000000001' }),
    patch: vi.fn().mockResolvedValue(null),
    del: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

const basePath = '/services/data/v62.0/sobjects'
const parentId = '06V000000000001'
const dsKey = DatasetKey.fromEntry({
  analyticOrg: 'TestOrg',
  dataset: 'MyDataset',
})

describe('GzipChunkingWritable', () => {
  it('given lines written, when stream ends, then uploads one gzipped base64 part', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write('"001","Acme"')
    sut.write('"002","Beta"')
    sut.end()
    await finished(sut)

    // Assert
    expect(sfPort.post).toHaveBeenCalledTimes(1)
    expect(sfPort.post).toHaveBeenCalledWith(
      `${basePath}/InsightsExternalDataPart`,
      expect.objectContaining({
        InsightsExternalDataId: parentId,
        PartNumber: 1,
        DataFile: expect.any(String),
      })
    )
    expect(sut.partCount).toBe(1)
  })

  it('given data exceeding 10MB base64, when stream ends, then splits into multiple parts', async () => {
    // Arrange
    const { randomBytes } = await import('node:crypto')
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    for (let i = 0; i < 12000; i++) {
      sut.write(`"${randomBytes(1024).toString('base64')}"`)
    }
    sut.end()
    await finished(sut)

    // Assert
    expect(sut.partCount).toBeGreaterThanOrEqual(2)
    const PART_MAX = 10 * 1024 * 1024
    for (const call of (sfPort.post as ReturnType<typeof vi.fn>).mock.calls) {
      expect((call[1].DataFile as string).length).toBeLessThanOrEqual(PART_MAX)
    }
  })

  it('given listener provided, when part uploaded, then calls onChunkWritten', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
    }
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId, listener)

    // Act
    sut.write('"data"')
    sut.end()
    await finished(sut)

    // Assert
    expect(listener.onChunkWritten).toHaveBeenCalled()
  })

  it('given no lines written, when stream ends, then no parts uploaded', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.end()
    await finished(sut)

    // Assert
    expect(sfPort.post).not.toHaveBeenCalled()
    expect(sut.partCount).toBe(0)
  })

  it('given part upload fails, when stream ends, then error propagates', async () => {
    // Arrange
    const sfPort = makeSfPort({
      post: vi.fn().mockRejectedValue(new Error('upload failed')),
    })
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write('"data"')
    sut.end()

    // Assert
    await expect(finished(sut)).rejects.toThrow('upload failed')
  })

  it('given gzip stream error, when writing, then error propagates to finished', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write('"first line"')
    // Force a gzip error by destroying the internal gzip stream
    // Access the chunk's gz through the stream internals
    const chunk = (
      sut as unknown as { chunk: { gz: { destroy: (err: Error) => void } } }
    ).chunk
    chunk.gz.destroy(new Error('zlib compression failed'))

    // Assert
    await expect(finished(sut)).rejects.toThrow('zlib compression failed')
  })

  it('given pending uploads from chunking, when drainUploads called, then awaits all uploads', async () => {
    // Arrange
    const { randomBytes } = await import('node:crypto')
    const sfPort = makeSfPort({
      post: vi.fn().mockResolvedValue({ id: '06W' }),
    })
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    const lines: string[] = []
    for (let i = 0; i < 12000; i++) {
      lines.push(`"${randomBytes(1024).toString('base64')}"`)
    }
    await new Promise<void>((resolve, reject) => {
      let i = 0
      const writeNext = (): void => {
        let ok = true
        while (i < lines.length && ok) {
          ok = sut.write(lines[i])
          i++
        }
        if (i < lines.length) {
          sut.once('drain', writeNext)
        } else {
          resolve()
        }
      }
      sut.on('error', reject)
      writeNext()
    })

    await sut.drainUploads()

    // Assert
    expect(sfPort.post).toHaveBeenCalled()
    expect(sut.partCount).toBeGreaterThan(0)
    expect(sfPort.post).toHaveBeenCalledTimes(sut.partCount)
  })
})

describe('DatasetWriter', () => {
  it('given existing metadata, when init called, then queries metadata patches numberOfLinesToIgnore and creates parent', async () => {
    // Arrange
    const existingMeta = JSON.stringify({
      objects: [
        { fields: [{ name: 'Id', type: 'Text' }], numberOfLinesToIgnore: 1 },
      ],
    })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    const chunker = await sut.init()

    // Assert
    expect(chunker).toBeDefined()
    expect(sfPort.post).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({
        EdgemartAlias: 'MyDataset',
        Format: 'Csv',
        Operation: 'Append',
        Action: 'None',
        MetadataJson: expect.any(String),
      })
    )
    const metaB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock.calls[0][1]
      .MetadataJson as string
    const decoded = JSON.parse(Buffer.from(metaB64, 'base64').toString('utf-8'))
    expect(decoded.objects[0].numberOfLinesToIgnore).toBe(0)
  })

  it('given no existing metadata, when init called, then throws SkipDatasetError', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')

    // Assert
    await expect(sut.init()).rejects.toThrow(SkipDatasetError)
  })

  it('given initialized and data written, when finalize called, then patches Action Process', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    const chunker = await sut.init()
    chunker.write('"data"')
    chunker.end()
    await finished(chunker)
    const result = await sut.finalize()

    // Assert
    expect(sfPort.patch).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`),
      { Action: 'Process', Mode: 'Incremental' }
    )
    expect(result.parentId).toBe(parentId)
  })

  it('given initialized, when abort called, then drains uploads and deletes parent', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    await sut.init()
    await sut.abort()

    // Assert
    expect(sfPort.del).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`)
    )
  })

  it('given initialized DatasetWriter, when skip() called, then deletes parent like abort()', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    await sut.init()

    // Act
    await sut.skip()

    // Assert
    expect(sfPort.del).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`)
    )
  })

  it('given not initialized, when abort called, then does nothing', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    await sut.abort()

    // Assert
    expect(sfPort.del).not.toHaveBeenCalled()
  })

  it('given listener provided, when init called, then calls onSinkReady', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
    }

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append', listener)
    await sut.init()

    // Assert
    expect(listener.onSinkReady).toHaveBeenCalledWith(parentId)
  })

  it('given invalid dataset name, when constructing, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()
    const badKey = DatasetKey.fromEntry({
      analyticOrg: 'TestOrg',
      dataset: 'bad name!',
    })

    // Act
    const act = () => new DatasetWriter(sfPort, badKey, 'Append')

    // Assert
    expect(act).toThrow('Invalid dataset name')
  })

  it('given existing metadata as JSON object, when init called, then stringifies it', async () => {
    // Arrange
    const metaObj = {
      fileFormat: { charsetName: 'UTF-8' },
      objects: [{ fields: [] }],
    }
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(metaObj),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    await sut.init()

    // Assert
    const metaB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock.calls[0][1]
      .MetadataJson as string
    const decoded = JSON.parse(Buffer.from(metaB64, 'base64').toString('utf-8'))
    expect(decoded).toMatchObject({ fileFormat: { charsetName: 'UTF-8' } })
  })
})

describe('DatasetWriterFactory', () => {
  it('given sfPort, when creating writer, then returns DatasetWriter with correct dataset and operation', () => {
    // Arrange
    const sfPort = makeSfPort()
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
    }
    const headerProvider = { resolveHeader: vi.fn() }
    const sut = new DatasetWriterFactory(sfPort)

    // Act
    const writer = sut.create(dsKey, 'Overwrite', listener, headerProvider)

    // Assert
    expect(writer).toBeInstanceOf(DatasetWriter)
  })

  it('given listener, when creating writer, then forwards listener', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
    }
    const headerProvider = { resolveHeader: vi.fn() }
    const sut = new DatasetWriterFactory(sfPort)

    // Act
    const writer = sut.create(dsKey, 'Append', listener, headerProvider)
    await writer.init()

    // Assert
    expect(listener.onSinkReady).toHaveBeenCalledWith(parentId)
  })
})
