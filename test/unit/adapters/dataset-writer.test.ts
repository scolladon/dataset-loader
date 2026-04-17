import { finished } from 'node:stream/promises'
import { type Gzip, type ZlibOptions } from 'node:zlib'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  DatasetWriter,
  DatasetWriterFactory,
  GzipChunkingWritable,
  LazyGzipChunkingWritable,
  UPLOAD_HIGH_WATER,
} from '../../../src/adapters/writers/dataset-writer.js'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import {
  type ProgressListener,
  type SalesforcePort,
  SkipDatasetError,
} from '../../../src/ports/types.js'

let createGzipOptions: ZlibOptions[] = []
let capturedGzipInstances: Gzip[] = []

vi.mock('node:zlib', async importOriginal => {
  const real = await importOriginal<typeof import('node:zlib')>()
  return {
    ...real,
    createGzip: (opts?: ZlibOptions) => {
      createGzipOptions.push(opts ?? {})
      const gz = real.createGzip(opts)
      capturedGzipInstances.push(gz)
      return gz
    },
  }
})

beforeEach(() => {
  createGzipOptions = []
  capturedGzipInstances = []
})

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
  targetOrg: 'TestOrg',
  targetDataset: 'MyDataset',
})

describe('GzipChunkingWritable', () => {
  it('given new GzipChunkingWritable, when initialized, then uses level 3 compression', () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    new GzipChunkingWritable(sfPort, basePath, parentId)

    // Assert
    expect(createGzipOptions[0]).toEqual(expect.objectContaining({ level: 3 }))
  })

  it('given empty batch written, when stream ends, then does not upload any part', async () => {
    // Arrange — kills the `if (batch.length > 0)` fast-path guard
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act — push an empty batch (edge case for the fast path)
    sut.write([])
    sut.end()
    await finished(sut)

    // Assert — nothing to upload
    expect(sfPort.post).not.toHaveBeenCalled()
    expect(sut.partCount).toBe(0)
  })

  it('given lines written, when stream ends, then uploads one gzipped base64 part', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write(['"001","Acme"', '"002","Beta"'])
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
    const batch = Array.from(
      { length: 12000 },
      () => `"${randomBytes(1024).toString('base64')}"`
    )
    sut.write(batch)
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
      onRowsWritten: vi.fn(),
    }
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId, listener)

    // Act
    sut.write(['"data"'])
    sut.end()
    await finished(sut)

    // Assert
    expect(listener.onChunkWritten).toHaveBeenCalled()
  })

  it('given listener, when batch written, then onRowsWritten called with batch length', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
      onRowsWritten: vi.fn(),
    }
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId, listener)

    // Act
    sut.write(['"a"', '"b"', '"c"'])
    sut.end()
    await finished(sut)

    // Assert
    expect(listener.onRowsWritten).toHaveBeenCalledWith(3)
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
    sut.write(['"data"'])
    sut.end()

    // Assert
    await expect(finished(sut)).rejects.toThrow('upload failed')
  })

  it('given upload fails during part rotation in write, when stream ends, then error propagates', async () => {
    // Arrange
    const sfPort = makeSfPort({
      post: vi.fn().mockRejectedValue(new Error('upload failed during write')),
    })
    // Use a 1-byte threshold so the second item in a batch triggers rotation
    const sut = new GzipChunkingWritable(
      sfPort,
      basePath,
      parentId,
      undefined,
      undefined,
      1
    )

    // Act — two-item batch: first item populates the chunk, second triggers rotation
    // so the upload fires in _write() before _final()
    sut.write(['"data1"', '"data2"'])
    sut.end()

    // Assert
    await expect(finished(sut)).rejects.toThrow('upload failed during write')
  })

  it('given post throws synchronously during chunk rotation, when writing, then finishAndRotate catch propagates error', async () => {
    // Arrange
    const sfPort = makeSfPort({
      post: vi.fn().mockImplementation(() => {
        throw new Error('sync post failure')
      }),
    })
    // Small partMaxBytes forces rotation after minimal data
    const sut = new GzipChunkingWritable(
      sfPort,
      basePath,
      parentId,
      undefined,
      25,
      100
    )

    // Act — write enough data to trigger a chunk rotation
    const batch = Array.from(
      { length: 200 },
      (_, i) => `"line-${i}-${'x'.repeat(50)}"`
    )
    sut.write(batch)

    // Assert
    await expect(finished(sut)).rejects.toThrow('sync post failure')
  })

  it('given gzip stream error, when writing, then error propagates to finished', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write(['"first line"'])
    capturedGzipInstances[0].destroy(new Error('zlib compression failed'))

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
    const batch = Array.from(
      { length: 12000 },
      () => `"${randomBytes(1024).toString('base64')}"`
    )
    await new Promise<void>((resolve, reject) => {
      sut.write(batch, err => (err ? reject(err) : resolve()))
    })

    await sut.drainUploads()

    // Assert
    expect(sfPort.post).toHaveBeenCalled()
    expect(sut.partCount).toBeGreaterThan(0)
    expect(sfPort.post).toHaveBeenCalledTimes(sut.partCount)
  })

  it('given upload started during rotation, when drainUploads called, then waits for upload to settle', async () => {
    // Arrange — upload resolves only when manually triggered
    const uploadResolvers: Array<() => void> = []
    const sfPort = makeSfPort({
      post: vi.fn().mockImplementation(
        () =>
          new Promise<{ id: string }>(resolve => {
            uploadResolvers.push(() => resolve({ id: '06W' }))
          })
      ),
    })
    // Use 1-byte partMaxBytes to guarantee rotation on the second line
    const sut = new GzipChunkingWritable(
      sfPort,
      basePath,
      parentId,
      undefined,
      UPLOAD_HIGH_WATER,
      1
    )

    // Act — write two lines: first writes to gz, second triggers rotation and starts an upload
    await new Promise<void>((resolve, reject) => {
      sut.write(['"a"', '"b"'], err => (err ? reject(err) : resolve()))
    })
    expect(uploadResolvers.length).toBeGreaterThan(0)

    let drainResolved = false
    const drainPromise = sut.drainUploads().then(() => {
      drainResolved = true
    })

    // Upload is still pending — drain should be waiting, not resolved
    await Promise.resolve()
    // Kills L167 BlockStatement: empty drainUploads resolves immediately → drainResolved=true
    expect(drainResolved).toBe(false)

    // Resolve all uploads and verify drain completes
    for (const r of uploadResolvers) r()
    await drainPromise
    expect(drainResolved).toBe(true)
  })

  it('given upload concurrency cap is reached, when a new chunk rotation is triggered, then producer blocks until a slot is freed', {
    timeout: 30000,
  }, async () => {
    // Arrange: uploads complete only when explicitly resolved.
    // Use a small uploadHighWater (4) so the test only needs ~4 rotations instead
    // of the production UPLOAD_HIGH_WATER (DEFAULT_CONCURRENCY = 25).
    const TEST_HIGH_WATER = 4
    const { randomBytes } = await import('node:crypto')
    const uploadResolvers: Array<() => void> = []
    let concurrentUploads = 0
    let maxConcurrentUploads = 0
    const sfPort = makeSfPort({
      post: vi.fn().mockImplementation(
        () =>
          new Promise<{ id: string }>(resolve => {
            concurrentUploads++
            maxConcurrentUploads = Math.max(
              maxConcurrentUploads,
              concurrentUploads
            )
            uploadResolvers.push(() => {
              concurrentUploads--
              resolve({ id: '06W' })
            })
          })
      ),
    })
    const sut = new GzipChunkingWritable(
      sfPort,
      basePath,
      parentId,
      undefined,
      TEST_HIGH_WATER
    )

    // Pre-generate a pool then slice into lines. 20000 lines per batch ensures ≥ 4
    // chunk rotations across both batches regardless of gzip compression ratio.
    const pool = randomBytes(1024 * 20000)
    const batch = Array.from(
      { length: 20000 },
      (_, i) =>
        `"${pool.subarray(i * 1024, (i + 1) * 1024).toString('base64')}"`
    )

    // Act
    // Batch 1: completes without backpressure (uploads started, held in-flight)
    await new Promise<void>((resolve, reject) => {
      sut.write(batch, err => (err ? reject(err) : resolve()))
    })

    // Batch 2: pauses when TEST_HIGH_WATER rotations are in-flight
    let batch2Completed = false
    const batch2Promise = new Promise<void>((resolve, reject) => {
      sut.write(batch, err => {
        batch2Completed = true
        if (err) reject(err)
        else resolve()
      })
    })

    // Wait for all TEST_HIGH_WATER uploads to be in-flight
    await vi.waitFor(
      () =>
        expect(uploadResolvers.length).toBeGreaterThanOrEqual(TEST_HIGH_WATER),
      { timeout: 10000 }
    )

    // Assert: backpressure is holding the second write
    expect(batch2Completed).toBe(false)
    expect(maxConcurrentUploads).toBeLessThanOrEqual(TEST_HIGH_WATER)

    // Release slots; batch2 may stall again on subsequent rotations so drain continuously
    const drainInterval = setInterval(() => {
      while (uploadResolvers.length > 0) uploadResolvers.shift()!()
    }, 1)
    await batch2Promise
    clearInterval(drainInterval)

    while (uploadResolvers.length > 0) uploadResolvers.shift()!()
    await sut.drainUploads()
    expect(batch2Completed).toBe(true)
  })

  it('given pendingUploads reaches HIGH_WATER, when rotation triggers in _write, then write blocks until slot freed', async () => {
    // Arrange — uploads never auto-resolve
    const uploadResolvers: Array<() => void> = []
    const sfPort = makeSfPort({
      post: vi.fn().mockImplementation(
        () =>
          new Promise<{ id: string }>(resolve => {
            uploadResolvers.push(() => resolve({ id: '06W' }))
          })
      ),
    })
    // HIGH_WATER=2, 1-byte partMaxBytes: any non-empty chunk triggers a rotation
    const TEST_HIGH_WATER = 2
    const sut = new GzipChunkingWritable(
      sfPort,
      basePath,
      parentId,
      undefined,
      TEST_HIGH_WATER,
      1
    )

    // Batch1: fills HIGH_WATER-1=1 slot.
    // Lines '"a"' + '"b"': '"b"' triggers rotation 1 (upload1, size=1, no block).
    // After write callback: chunk still holds '"b"' with pendingBytes > 0.
    await new Promise<void>((resolve, reject) => {
      sut.write(['"a"', '"b"'], err => (err ? reject(err) : resolve()))
    })
    expect(uploadResolvers.length).toBe(1)

    // Batch2: single line. Chunk is non-empty ('"b"' pending), so '"c"' immediately
    // triggers rotation 2 (upload2, size=2=HIGH_WATER) → must block.
    // With mutations (>2 / false / {}): no block → write completes immediately.
    let secondWriteCompleted = false
    sut.write(['"c"'], err => {
      secondWriteCompleted = !err
    })

    // Allow async flush + endGzip + backpressure check to settle
    await new Promise(resolve => setTimeout(resolve, 100))

    // Kills L121:73 (BlockStatement={}), L121:23 (ConditionalExpression false), L121:23 (EqualityOperator >)
    expect(secondWriteCompleted).toBe(false)
    expect(uploadResolvers.length).toBe(TEST_HIGH_WATER)

    // Release one slot — write should unblock and complete
    uploadResolvers[0]()
    await vi.waitFor(() => expect(secondWriteCompleted).toBe(true), {
      timeout: 5000,
    })
  })

  it('given two lines written to GzipChunkingWritable, when stream ends, then decompressed content has each line followed by a newline', async () => {
    // Arrange
    const { gunzip } = await import('node:zlib')
    const { promisify } = await import('node:util')
    const gunzipAsync = promisify(gunzip)
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write(['"line1"', '"line2"'])
    sut.end()
    await finished(sut)

    // Assert
    const b64 = (sfPort.post as ReturnType<typeof vi.fn>).mock.calls[0][1]
      .DataFile as string
    const compressed = Buffer.from(b64, 'base64')
    const decompressed = await gunzipAsync(compressed)
    expect(decompressed.toString()).toBe('"line1"\n"line2"\n')
  })

  it('given a batch of three lines written, when stream ends, then decompressed content contains exactly three lines (not four)', async () => {
    // Arrange
    const { gunzip } = await import('node:zlib')
    const { promisify } = await import('node:util')
    const gunzipAsync = promisify(gunzip)
    const sfPort = makeSfPort()
    const sut = new GzipChunkingWritable(sfPort, basePath, parentId)

    // Act
    sut.write(['"a"', '"b"', '"c"'])
    sut.end()
    await finished(sut)

    // Assert
    const b64 = (sfPort.post as ReturnType<typeof vi.fn>).mock.calls[0][1]
      .DataFile as string
    const compressed = Buffer.from(b64, 'base64')
    const decompressed = await gunzipAsync(compressed)
    const lines = decompressed
      .toString()
      .split('\n')
      .filter(l => l.length > 0)
    expect(lines).toHaveLength(3)
  })
})

describe('DatasetWriter', () => {
  it('given existing metadata, when data written, then patches numberOfLinesToIgnore and creates parent with correct metadata', async () => {
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
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert — kills L300: basePath '' would produce '/InsightsExternalData' not '/services/data/v62.0/...'
    expect(sfPort.post).toHaveBeenCalledWith(
      expect.stringContaining(
        '/services/data/v62.0/sobjects/InsightsExternalData'
      ),
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

  it('given existing metadata, when init called, then queries both Completed and CompletedWithWarnings statuses', async () => {
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

    // Assert
    expect(sfPort.query).toHaveBeenCalledWith(
      expect.stringContaining(
        "Status IN ('Completed', 'CompletedWithWarnings')"
      )
    )
  })

  it('given dataset completed with warnings, when data written, then uses its metadata', async () => {
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
        records: [{ MetadataJson: '/blob/url/completed-with-warnings' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    const chunker = await sut.init()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert
    const metaB64 = (sfPort.post as ReturnType<typeof vi.fn>).mock.calls[0][1]
      .MetadataJson as string
    const decoded = JSON.parse(Buffer.from(metaB64, 'base64').toString('utf-8'))
    expect(decoded.objects[0].numberOfLinesToIgnore).toBe(0)
  })

  it.each([
    ['malformed JSON', 'not json'],
    ['non-object root', '"just a string"'],
    ['non-array objects field', '{"objects": "not-an-array"}'],
  ])('given %s in existing metadata, when init called, then rejects with dataset name in the error', async (_name, badMeta) => {
    // Arrange — kills partial try/catch mutations in normalizeMetadata
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(badMeta),
    })
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')

    // Act & Assert
    await expect(sut.init()).rejects.toThrow(
      /Failed to parse metadata for dataset/
    )
    await expect(sut.init()).rejects.toThrow(dsKey.name)
  })

  it('given no existing metadata, when init called, then throws SkipDatasetError', async () => {
    // Arrange
    const sfPort = makeSfPort()

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')

    // Assert — kills L311: empty message would not contain 'No existing metadata'
    await expect(sut.init()).rejects.toThrow('No existing metadata')
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
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)
    const result = await sut.finalize()

    // Assert
    expect(sfPort.patch).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`),
      { Action: 'Process', Mode: 'Incremental' }
    )
    expect(result.parentId).toBe(parentId)
    // Kills L232 (empty partCount getter body) and L234 (&&0 mutation):
    expect(result.partCount).toBe(1)
  })

  it('given initialized and data written, when finalize called, then drains uploads before patching Action Process', async () => {
    // Arrange
    const callOrder: string[] = []
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(existingMeta),
      post: vi.fn().mockResolvedValue({ id: parentId }),
      patch: vi.fn().mockImplementation(() => {
        callOrder.push('patch')
        return Promise.resolve(null)
      }),
    })
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    const chunker = await sut.init()
    vi.spyOn(
      chunker as LazyGzipChunkingWritable,
      'drainUploads'
    ).mockImplementation(async () => {
      callOrder.push('drain')
    })
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Act
    await sut.finalize()

    // Assert
    expect(callOrder).toEqual(['drain', 'patch'])
  })

  it('given initialized and data written, when abort called, then drains uploads and deletes parent', async () => {
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
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)
    await sut.abort()

    // Assert
    expect(sfPort.del).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`)
    )
  })

  it('given initialized DatasetWriter and data written, when skip() called, then deletes parent like abort()', async () => {
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
    const chunker = await sut.init()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Act
    await sut.skip()

    // Assert
    expect(sfPort.del).toHaveBeenCalledWith(
      expect.stringContaining(`InsightsExternalData/${parentId}`)
    )
  })

  it('given init called but no data written, when finalize called, then does NOT post parent and does NOT patch Action:Process', async () => {
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
    chunker.end()
    await finished(chunker)
    const result = await sut.finalize()

    // Assert
    expect(sfPort.post).not.toHaveBeenCalled()
    expect(sfPort.patch).not.toHaveBeenCalled()
    // Kills L331: {} vs { parentId: '', partCount: 0 }
    expect(result).toEqual({ parentId: '', partCount: 0 })
  })

  it('given init called but no data written, when abort called, then does NOT post parent and does NOT delete', async () => {
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
    expect(sfPort.post).not.toHaveBeenCalled()
    expect(sfPort.del).not.toHaveBeenCalled()
  })

  it('given init called but no data written, when skip called, then does NOT post parent and does NOT delete', async () => {
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
    await sut.skip()

    // Assert
    expect(sfPort.post).not.toHaveBeenCalled()
    expect(sfPort.del).not.toHaveBeenCalled()
  })

  it('given data written for first time, when writing, then creates parent before forwarding data', async () => {
    // Arrange
    const existingMeta = JSON.stringify({ objects: [{ fields: [] }] })
    const postCallsBeforeWrite: number[] = []
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
    postCallsBeforeWrite.push(
      (sfPort.post as ReturnType<typeof vi.fn>).mock.calls.length
    )
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert
    expect(postCallsBeforeWrite[0]).toBe(0)
    expect(sfPort.post).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalData'),
      expect.objectContaining({ EdgemartAlias: 'MyDataset' })
    )
  })

  it('given listener provided, when first data written, then calls onSinkReady', async () => {
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
      onRowsWritten: vi.fn(),
    }

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append', listener)
    const chunker = await sut.init()
    expect(listener.onSinkReady).not.toHaveBeenCalled()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert
    expect(listener.onSinkReady).toHaveBeenCalledWith(parentId)
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

  it('given listener provided, when init called, then onSinkReady is NOT yet called', async () => {
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
      onRowsWritten: vi.fn(),
    }

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append', listener)
    await sut.init()

    // Assert
    expect(listener.onSinkReady).not.toHaveBeenCalled()
  })

  it('given not initialized, when finalize called, then throws Not initialized', async () => {
    // Arrange
    const sfPort = makeSfPort()
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')

    // Act & Assert
    await expect(sut.finalize()).rejects.toThrow('Not initialized')
  })

  it('given two batches written, when finalizing, then second write uses existing chunker', async () => {
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
    const chunker = await sut.init()

    // Act — two separate writes trigger the _chunker early-return on the second call
    chunker.write(['"row1"'])
    chunker.write(['"row2"'])
    chunker.end()
    await finished(chunker)

    // Assert — both rows uploaded in one part
    expect(sfPort.post).toHaveBeenCalledWith(
      expect.stringContaining('InsightsExternalDataPart'),
      expect.objectContaining({
        InsightsExternalDataId: parentId,
        PartNumber: 1,
      })
    )
    // Kills L246:9/L246:24: second write reuses _chunker, so InsightsExternalData created only once
    expect(sfPort.post).toHaveBeenCalledTimes(2)
  })

  it('given invalid dataset name, when constructing, then throws', () => {
    // Arrange
    const sfPort = makeSfPort()
    const badKey = DatasetKey.fromEntry({
      targetOrg: 'TestOrg',
      targetDataset: 'bad name!',
    })

    // Act
    const act = () => new DatasetWriter(sfPort, badKey, 'Append')

    // Assert
    expect(act).toThrow('Invalid dataset name')
  })

  it('given existing metadata without objects field, when data written, then normalizes without crashing', async () => {
    // Arrange — kills L363: meta.objects.map() throws when objects is undefined
    const metaWithoutObjects = JSON.stringify({
      fileFormat: { charsetName: 'UTF-8' },
    })
    const sfPort = makeSfPort({
      query: vi.fn().mockResolvedValue({
        totalSize: 1,
        done: true,
        records: [{ MetadataJson: '/blob/url' }],
      }),
      getBlob: vi.fn().mockResolvedValue(metaWithoutObjects),
      post: vi.fn().mockResolvedValue({ id: parentId }),
    })

    // Act
    const sut = new DatasetWriter(sfPort, dsKey, 'Append')
    const chunker = await sut.init()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert — no crash; data was uploaded
    expect(sfPort.post).toHaveBeenCalled()
  })

  it('given existing metadata as JSON object, when data written, then stringifies it', async () => {
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
    const chunker = await sut.init()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

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
      onRowsWritten: vi.fn(),
    }
    const headerProvider = { resolveHeader: vi.fn() }
    const sut = new DatasetWriterFactory(sfPort)

    // Act
    const writer = sut.create(dsKey, 'Overwrite', listener, headerProvider)

    // Assert
    expect(writer).toBeInstanceOf(DatasetWriter)
  })

  it('given listener, when creating writer and data written, then forwards listener', async () => {
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
      onRowsWritten: vi.fn(),
    }
    const headerProvider = { resolveHeader: vi.fn() }
    const sut = new DatasetWriterFactory(sfPort)

    // Act
    const writer = sut.create(dsKey, 'Append', listener, headerProvider)
    const chunker = await writer.init()
    chunker.write(['"data"'])
    chunker.end()
    await finished(chunker)

    // Assert
    expect(listener.onSinkReady).toHaveBeenCalledWith(parentId)
  })
})
