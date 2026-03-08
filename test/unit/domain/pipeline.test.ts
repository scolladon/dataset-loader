import { Readable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import {
  executePipeline,
  type PipelineEntry,
} from '../../../src/domain/pipeline.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import {
  type CreateUploaderPort,
  type FetchPort,
  type FetchResult,
  type LoggerPort,
  type ProgressPort,
  type StatePort,
  type Uploader,
} from '../../../src/ports/types.js'

function csvReadable(content: string): Readable {
  return Readable.from(Buffer.from(content))
}

function createMockLogger(): LoggerPort {
  return { info: vi.fn(), warn: vi.fn(), debug: vi.fn() }
}

function createMockProgress(): ProgressPort {
  return { create: vi.fn(() => ({ tick: vi.fn(), stop: vi.fn() })) }
}

function createMockState(
  store: WatermarkStore = WatermarkStore.empty()
): StatePort {
  return {
    read: vi.fn(async () => store),
    write: vi.fn(async () => {
      /* noop */
    }),
  }
}

function createMockUploader(overrides: Partial<Uploader> = {}): Uploader {
  return {
    write: vi.fn(async () => {
      /* noop */
    }),
    process: vi.fn(async () => ({
      parentId: '06V000000000001',
      partIds: ['06W000000000001'],
    })),
    abort: vi.fn(async () => {
      /* noop */
    }),
    ...overrides,
  }
}

function createFetchResult(
  csvStreams: string[],
  watermark?: Watermark
): FetchResult {
  return {
    streams: (async function* () {
      for (const csv of csvStreams) {
        yield csvReadable(csv)
      }
    })(),
    totalHint: csvStreams.length,
    watermark: () => watermark,
  }
}

function mockFetcher(fetchFn: FetchPort['fetch']): FetchPort {
  return { fetch: vi.fn(fetchFn) }
}

function createEntry(
  overrides: Partial<PipelineEntry> & { fetcher: FetchPort }
): PipelineEntry {
  return {
    index: 0,
    label: 'elf:Login',
    watermarkKey: WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      eventType: 'Login',
      interval: 'Daily',
    }),
    datasetKey: DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS' }),
    operation: 'Append',
    augmentColumns: {},
    ...overrides,
  }
}

describe('executePipeline (streaming)', () => {
  it('given entries with data, when executing, then streams csv lines into uploader and updates watermarks', async () => {
    // Arrange
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"H"\n"v"\n'], watermark)
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesProcessed).toBe(1)
    expect(sut.groupsUploaded).toBe(1)
    expect(sut.exitCode).toBe(0)
    expect(uploader.write).toHaveBeenCalledTimes(2)
    expect(uploader.process).toHaveBeenCalledTimes(1)
    expect(state.write).toHaveBeenCalledTimes(1)
    const writtenStore: WatermarkStore = (
      state.write as ReturnType<typeof vi.fn>
    ).mock.calls[0][0]
    const wk = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      eventType: 'Login',
      interval: 'Daily',
    })
    expect(writtenStore.get(wk)?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given empty source (no new records), when executing, then skips entry', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(uploader.write).not.toHaveBeenCalled()
    expect(uploader.process).not.toHaveBeenCalled()
  })

  it('given fetch error, when executing, then counts as failed and aborts uploader', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => {
      throw new Error('network error')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(uploader.process).not.toHaveBeenCalled()
  })

  it('given two entries same dataset, when executing, then both stream into same uploader', async () => {
    // Arrange
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher1 = mockFetcher(async () =>
      createFetchResult(['"H"\n"a"\n'], wm)
    )
    const fetcher2 = mockFetcher(async () =>
      createFetchResult(['"H"\n"b"\n'], wm)
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const state = createMockState()

    const entry1 = createEntry({ fetcher: fetcher1, index: 0 })
    const entry2 = createEntry({
      fetcher: fetcher2,
      index: 1,
      label: 'elf:Logout',
      watermarkKey: WatermarkKey.fromEntry({
        type: 'elf',
        sourceOrg: 'src',
        eventType: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(createUploader.create).toHaveBeenCalledTimes(1)
    expect(uploader.write).toHaveBeenCalledTimes(3)
    expect(uploader.process).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(2)
    expect(sut.groupsUploaded).toBe(1)
  })

  it('given augmentColumns, when streaming, then csv lines contain augmented data', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"Id"\n"001"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher, augmentColumns: { Org: 'prod' } })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    const firstLine = (uploader.write as ReturnType<typeof vi.fn>).mock
      .calls[0][0] as string
    const secondLine = (uploader.write as ReturnType<typeof vi.fn>).mock
      .calls[1][0] as string
    expect(firstLine).toContain('Id')
    expect(firstLine).toContain('Org')
    expect(secondLine).toContain('001')
    expect(secondLine).toContain('prod')
  })

  it('given mixed success and failure across datasets, when executing, then partial success with correct exit code', async () => {
    // Arrange
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"H"\n"v"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })

    const goodUploader = createMockUploader()
    const badUploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi
        .fn()
        .mockReturnValueOnce(goodUploader)
        .mockReturnValueOnce(badUploader),
    }

    const goodEntry = createEntry({
      fetcher: goodFetcher,
      index: 0,
      label: 'good',
    })
    const badEntry = createEntry({
      fetcher: badFetcher,
      index: 1,
      label: 'bad',
      datasetKey: DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS2' }),
      watermarkKey: WatermarkKey.fromEntry({
        type: 'elf',
        sourceOrg: 'src',
        eventType: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesProcessed).toBe(1)
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(1)
  })

  it('given uploader.process throws, when executing, then catches group error and counts all entries as failed', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"H"\n"v"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader({
      process: vi.fn(async () => {
        throw new Error('upload crash')
      }),
    })
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(sut.exitCode).toBe(2)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('upload crash')
    )
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given all entries fail, when executing, then exit code is 2', async () => {
    // Arrange
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher: badFetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(sut.exitCode).toBe(2)
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given entry with data and no watermark returned, when executing, then processes without storing watermark', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult(['"H"\n"v"\n']))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesProcessed).toBe(1)
    expect(uploader.process).toHaveBeenCalled()
    const writtenStore: WatermarkStore = (
      state.write as ReturnType<typeof vi.fn>
    ).mock.calls[0][0]
    const wk = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      eventType: 'Login',
      interval: 'Daily',
    })
    expect(writtenStore.get(wk)).toBeUndefined()
  })

  it('given all entries empty in a group, when executing, then aborts uploader without processing', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(sut.groupsUploaded).toBe(0)
    expect(uploader.process).not.toHaveBeenCalled()
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given uploader.abort throws during error handling, when executing, then still counts entries as failed', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"H"\n"v"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader({
      process: vi.fn(async () => {
        throw new Error('process failed')
      }),
      abort: vi.fn(async () => {
        throw new Error('abort also failed')
      }),
    })
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('abort also failed')
    )
  })

  it('given non-Error rejection in group, when executing, then logs unknown error', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"H"\n"v"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader({
      process: vi.fn(async () => {
        throw 'string error'
      }),
    })
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('unknown error')
    )
  })

  it('given non-Error rejection in entry fetch, when executing, then logs unknown error', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => {
      throw 42
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('unknown error')
    )
  })

  it('given partial failure within group with two entries, when executing, then aborts entire group', async () => {
    // Arrange
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"H"\n"v"\n'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fetch error')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = { create: vi.fn(() => uploader) }

    const goodEntry = createEntry({ fetcher: goodFetcher, index: 0 })
    const badEntry = createEntry({
      fetcher: badFetcher,
      index: 1,
      label: 'elf:Logout',
      watermarkKey: WatermarkKey.fromEntry({
        type: 'elf',
        sourceOrg: 'src',
        eventType: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(2)
    expect(sut.entriesProcessed).toBe(0)
    expect(uploader.abort).toHaveBeenCalled()
    expect(uploader.process).not.toHaveBeenCalled()
  })
})
