import { PassThrough, type Writable } from 'node:stream'
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
  type GroupTracker,
  type LoggerPort,
  type ProgressPort,
  SkipDatasetError,
  type StatePort,
  type Uploader,
  type UploadResult,
} from '../../../src/ports/types.js'

function createMockLogger(): LoggerPort {
  return { info: vi.fn(), warn: vi.fn(), debug: vi.fn() }
}

function createMockGroupTracker(): GroupTracker {
  return {
    updateParentId: vi.fn(),
    incrementParts: vi.fn(),
    addFiles: vi.fn(),
    addRows: vi.fn(),
    stop: vi.fn(),
  }
}

function createMockProgress(
  tracker: GroupTracker = createMockGroupTracker()
): ProgressPort {
  return {
    create: vi.fn(() => ({
      tick: vi.fn(),
      trackGroup: vi.fn(() => tracker),
      stop: vi.fn(),
    })),
  }
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

function createFetchResult(
  lines: string[],
  watermark?: Watermark,
  fileCount?: number
): FetchResult {
  return {
    lines: (async function* () {
      for (const l of lines) yield l
    })(),
    watermark: () => watermark,
    fileCount: () => fileCount ?? (lines.length > 0 ? 1 : 0),
  }
}

interface MockUploader extends Uploader {
  readonly _writtenLines: string[]
  readonly _writable: PassThrough
}

function createMockUploader(overrides: Partial<Uploader> = {}): MockUploader {
  const writtenLines: string[] = []
  const mockWritable = new PassThrough({ objectMode: true })
  mockWritable.on('data', (chunk: string) => writtenLines.push(chunk))
  return {
    init: vi.fn(async () => mockWritable),
    finalize: vi.fn(
      async (): Promise<UploadResult> => ({
        parentId: '06V000000000001',
        partCount: 1,
      })
    ),
    abort: vi.fn(async () => {
      /* noop */
    }),
    _writtenLines: writtenLines,
    _writable: mockWritable,
    ...overrides,
  } as MockUploader
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
  it('given entries with data, when executing, then pipes lines into uploader and updates watermarks', async () => {
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"v1"', '"v2"'], watermark)
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const state = createMockState()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesProcessed).toBe(1)
    expect(sut.groupsUploaded).toBe(1)
    expect(sut.exitCode).toBe(0)
    expect(uploader._writtenLines).toHaveLength(2)
    expect(uploader.finalize).toHaveBeenCalledTimes(1)
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
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(uploader._writtenLines).toHaveLength(0)
    expect(uploader.finalize).not.toHaveBeenCalled()
  })

  it('given fetch error, when executing, then counts as failed and aborts uploader', async () => {
    const fetcher = mockFetcher(async () => {
      throw new Error('network error')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(uploader.finalize).not.toHaveBeenCalled()
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given source stream errors mid-iteration, when executing, then counts as failed and does not finalize', async () => {
    const fetcher = mockFetcher(async () => ({
      lines: (async function* () {
        yield '"v1"'
        throw new Error('stream error mid-way')
      })(),
      watermark: () => undefined,
      fileCount: () => 1,
    }))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const logger = createMockLogger()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(uploader.abort).toHaveBeenCalled()
    expect(uploader.finalize).not.toHaveBeenCalled()
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('stream error mid-way')
    )
  })

  it('given two entries same dataset, when executing, then both pipe into same uploader', async () => {
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher1 = mockFetcher(async () => createFetchResult(['"a"'], wm))
    const fetcher2 = mockFetcher(async () => createFetchResult(['"b"'], wm))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

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

    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(createUploader.create).toHaveBeenCalledTimes(1)
    expect(uploader._writtenLines).toHaveLength(2)
    expect(uploader.finalize).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(2)
    expect(sut.groupsUploaded).toBe(1)
  })

  it('given augmentColumns, when piping, then lines contain augmented data', async () => {
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"001"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

    await executePipeline({
      entries: [createEntry({ fetcher, augmentColumns: { Org: 'prod' } })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(uploader._writtenLines[0]).toContain('001')
    expect(uploader._writtenLines[0]).toContain('prod')
  })

  it('given mixed success and failure across datasets, when executing, then partial success with correct exit code', async () => {
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })

    const goodUploader = createMockUploader()
    const badUploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn((dataset: DatasetKey) =>
        dataset.name === 'DS' ? goodUploader : badUploader
      ),
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

    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesProcessed).toBe(1)
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(1)
  })

  it('given uploader.finalize throws, when executing, then catches group error and counts all entries as failed', async () => {
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader({
      finalize: vi.fn(async () => {
        throw new Error('upload crash')
      }),
    })
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const logger = createMockLogger()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(sut.exitCode).toBe(2)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('upload crash')
    )
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given all entries fail, when executing, then exit code is 2', async () => {
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

    const sut = await executePipeline({
      entries: [createEntry({ fetcher: badFetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(uploader.abort).toHaveBeenCalled()
  })

  it('given entry with data and no watermark returned, when executing, then processes without storing watermark', async () => {
    const fetcher = mockFetcher(async () => createFetchResult(['"v"']))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const state = createMockState()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesProcessed).toBe(1)
    expect(uploader.finalize).toHaveBeenCalled()
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

  it('given single empty entry, when executing, then aborts uploader and writes state', async () => {
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const state = createMockState()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesSkipped).toBe(1)
    expect(sut.groupsUploaded).toBe(0)
    expect(uploader.finalize).not.toHaveBeenCalled()
    expect(uploader.abort).toHaveBeenCalled()
    expect(state.write).toHaveBeenCalledTimes(1)
  })

  it('given uploader.abort throws during error handling, when executing, then still counts entries as failed', async () => {
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const uploader = createMockUploader({
      finalize: vi.fn(async () => {
        throw new Error('process failed')
      }),
      abort: vi.fn(async () => {
        throw new Error('abort also failed')
      }),
    })
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const logger = createMockLogger()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    expect(sut.entriesFailed).toBe(1)
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('abort also failed')
    )
  })

  it('given entries with data, when executing, then reports files and rows to tracker', async () => {
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"r1"', '"r2"'], watermark)
    )
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }
    const tracker = createMockGroupTracker()
    const progress = createMockProgress(tracker)

    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    expect(tracker.addFiles).toHaveBeenCalledWith(1)
    expect(tracker.addFiles).toHaveBeenCalledTimes(1)
    expect(tracker.addRows).toHaveBeenCalledWith(1)
    expect(tracker.addRows).toHaveBeenCalledTimes(2)
  })

  it('given upload listener wired, when uploader invokes onParentCreated during init, then updates tracker parentId', async () => {
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"v"'], watermark)
    )
    const tracker = createMockGroupTracker()
    const progress = createMockProgress(tracker)
    const createUploader: CreateUploaderPort = {
      create: vi.fn((_ds, _op, listener) => {
        const writable = new PassThrough({ objectMode: true })
        writable.resume()
        return createMockUploader({
          init: vi.fn(async () => {
            listener.onParentCreated('06Vxxx')
            return writable
          }),
        })
      }),
    }

    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    expect(tracker.updateParentId).toHaveBeenCalledWith('06Vxxx')
  })

  it('given uploader.init throws SkipDatasetError, when executing, then warns and skips all entries', async () => {
    const fetcher = mockFetcher(async () => createFetchResult(['"data"']))
    const skipUploader = createMockUploader({
      init: vi.fn(async () => {
        throw new SkipDatasetError('No metadata for ds')
      }),
    })
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => skipUploader),
    }
    const logger = createMockLogger()

    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesFailed).toBe(0)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('No metadata')
    )
    expect(skipUploader.abort).not.toHaveBeenCalled()
    expect(skipUploader.finalize).not.toHaveBeenCalled()
  })

  it('given multiple entries piping concurrently, when all finish, then all lines written and finalize called once', async () => {
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher1 = mockFetcher(async () =>
      createFetchResult(['row_a1', 'row_a2'], wm)
    )
    const fetcher2 = mockFetcher(async () => createFetchResult(['row_b1'], wm))
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

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

    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(uploader._writtenLines).toHaveLength(3)
    expect(uploader._writtenLines).toEqual(
      expect.arrayContaining(['row_a1', 'row_a2', 'row_b1'])
    )
    expect(createUploader.create).toHaveBeenCalledTimes(1)
    expect(uploader.finalize).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(2)
    expect(sut.groupsUploaded).toBe(1)
  })

  it('given partial failure within group, when executing, then aborts entire group', async () => {
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fetch error')
    })
    const uploader = createMockUploader()
    const createUploader: CreateUploaderPort = {
      create: vi.fn(() => uploader),
    }

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

    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createUploader,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesFailed).toBe(2)
    expect(sut.entriesProcessed).toBe(0)
    expect(uploader.abort).toHaveBeenCalled()
    expect(uploader.finalize).not.toHaveBeenCalled()
  })
})
