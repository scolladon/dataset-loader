import { PassThrough, type Writable } from 'node:stream'
import { describe, expect, it, vi } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import {
  createHeaderProvider,
  DatasetGroup,
  executePipeline,
  groupByReader,
  type PipelineEntry,
  type ReaderBundle,
} from '../../../src/domain/pipeline.js'
import { ReaderKey } from '../../../src/domain/reader-key.js'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import {
  type CreateWriterPort,
  type FetchResult,
  type GroupTracker,
  type LoggerPort,
  type ProgressListener,
  type ProgressPort,
  type ReaderPort,
  SkipDatasetError,
  type StatePort,
  type Writer,
  type WriterResult,
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
      if (lines.length > 0) yield lines
    })(),
    watermark: () => watermark,
    fileCount: () => fileCount ?? (lines.length > 0 ? 1 : 0),
  }
}

interface MockWriter extends Writer {
  readonly _writtenLines: string[]
  readonly _writable: PassThrough
}

function createMockWriter(overrides: Partial<Writer> = {}): MockWriter {
  const writtenLines: string[] = []
  const mockWritable = new PassThrough({ objectMode: true })
  mockWritable.on('data', (batch: string[]) => writtenLines.push(...batch))
  return {
    init: vi.fn(async () => mockWritable),
    finalize: vi.fn(
      async (): Promise<WriterResult> => ({
        parentId: '06V000000000001',
        partCount: 1,
      })
    ),
    abort: vi.fn(async () => {
      /* noop */
    }),
    skip: vi.fn(async () => {
      /* noop */
    }),
    _writtenLines: writtenLines,
    _writable: mockWritable,
    ...overrides,
  } as MockWriter
}

function mockFetcher(fetchFn: ReaderPort['fetch']): ReaderPort {
  return {
    fetch: vi.fn(fetchFn),
    header: vi.fn(async () => 'HEADER'),
  }
}

function createEntry(
  overrides: Partial<PipelineEntry> & { fetcher: ReaderPort }
): PipelineEntry {
  return {
    index: 0,
    label: 'elf:Login',
    readerKey: ReaderKey.forElf('src', 'Login', 'Daily'),
    watermarkKey: WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    }),
    datasetKey: DatasetKey.fromEntry({ targetOrg: 'ana', targetDataset: 'DS' }),
    operation: 'Append',
    augmentColumns: {},
    header: vi.fn(async () => 'HEADER'),
    ...overrides,
  }
}

function createEntryWithReader(
  readerKey: ReaderKey,
  overrides: Partial<PipelineEntry> & { fetcher: ReaderPort }
): PipelineEntry {
  return { ...createEntry(overrides), readerKey }
}

function buildPipelineInput(
  entries: PipelineEntry[],
  overrides: {
    createWriter?: CreateWriterPort
    watermarks?: WatermarkStore
    state?: StatePort
  } = {}
) {
  const defaultWriter = createMockWriter()
  return {
    entries,
    watermarks: overrides.watermarks ?? WatermarkStore.empty(),
    createWriter: overrides.createWriter ?? {
      create: vi.fn(() => defaultWriter),
    },
    state: overrides.state ?? createMockState(),
    progress: createMockProgress(),
    logger: createMockLogger(),
  }
}

describe('DatasetGroup', () => {
  it('given key, datasetKey, operation, and entries, when created via from(), then exposes all properties', () => {
    // Arrange
    const datasetKey = DatasetKey.fromEntry({
      targetOrg: 'ana',
      targetDataset: 'DS',
    })
    const entry = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
    })

    // Act
    const sut = DatasetGroup.from('my-key', datasetKey, 'Append', [entry])

    // Assert
    expect(sut.key).toBe('my-key')
    expect(sut.datasetKey).toBe(datasetKey)
    expect(sut.operation).toBe('Append')
    expect(sut.entries).toHaveLength(1)
  })
})

describe('createHeaderProvider', () => {
  it('given entries where first has header, when resolveHeader is called, then returns first non-empty header', async () => {
    // Arrange
    const first = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      header: vi.fn(async () => 'col1,col2'),
    })
    const second = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      header: vi.fn(async () => 'other'),
    })
    const sut = createHeaderProvider([first, second])

    // Act
    const result = await sut.resolveHeader()

    // Assert
    expect(result).toBe('col1,col2')
  })

  it('given entries where first has no header yet, when resolveHeader is called, then returns header from next entry', async () => {
    // Arrange
    const first = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      header: vi.fn(async () => ''),
    })
    const second = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      header: vi.fn(async () => 'col1,col2'),
    })
    const sut = createHeaderProvider([first, second])

    // Act
    const result = await sut.resolveHeader()

    // Assert
    expect(result).toBe('col1,col2')
  })

  it('given entries all with empty header, when resolveHeader is called, then returns empty string', async () => {
    // Arrange
    const entry = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      header: vi.fn(async () => ''),
    })
    const sut = createHeaderProvider([entry])

    // Act
    const result = await sut.resolveHeader()

    // Assert
    expect(result).toBe('')
  })

  it('given no entries, when resolveHeader is called, then returns empty string', async () => {
    // Arrange
    const sut = createHeaderProvider([])

    // Act
    const result = await sut.resolveHeader()

    // Assert
    expect(result).toBe('')
  })
})

describe('executePipeline (streaming)', () => {
  it('given entries with data, when executing, then pipes lines into writer and updates watermarks', async () => {
    // Arrange
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"v1"', '"v2"'], watermark)
    )
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    expect(sut.entriesProcessed).toBe(1)
    expect(sut.groupsUploaded).toBe(1)
    expect(sut.exitCode).toBe(0)
    expect(writer._writtenLines).toHaveLength(2)
    expect(writer.finalize).toHaveBeenCalledTimes(1)
    expect(state.write).toHaveBeenCalledTimes(1)
    const writtenStore: WatermarkStore = (
      state.write as ReturnType<typeof vi.fn>
    ).mock.calls[0][0]
    const wk = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    })
    expect(writtenStore.get(wk)?.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given empty source (no new records), when executing, then skips entry', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(writer._writtenLines).toHaveLength(0)
    expect(writer.finalize).not.toHaveBeenCalled()
  })

  it('given fetch error, when executing, then counts as failed and aborts writer', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => {
      throw new Error('network error')
    })
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(writer.finalize).not.toHaveBeenCalled()
    expect(writer.abort).toHaveBeenCalled()
  })

  it('given source stream errors mid-iteration, when executing, then counts as failed and does not finalize', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => ({
      lines: (async function* () {
        yield '"v1"'
        throw new Error('stream error mid-way')
      })(),
      watermark: () => undefined,
      fileCount: () => 1,
    }))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(writer.abort).toHaveBeenCalled()
    expect(writer.finalize).not.toHaveBeenCalled()
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('stream error mid-way')
    )
  })

  it('given writable emits error on first write, when executing, then counts as failed and aborts writer', async () => {
    // Arrange — simulate LazyGzipChunkingWritable failing to create parent on first write
    const { Writable } = await import('node:stream')
    const errorWritable = new Writable({
      objectMode: true,
      write(_chunk, _enc, callback) {
        callback(new Error('parent creation failed'))
      },
    })
    const fetcher = mockFetcher(async () => createFetchResult(['"v1"']))
    const writer = createMockWriter({ init: vi.fn(async () => errorWritable) })
    const createWriter: CreateWriterPort = { create: vi.fn(() => writer) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(0)
    expect(writer.abort).toHaveBeenCalled()
    expect(writer.finalize).not.toHaveBeenCalled()
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('parent creation failed')
    )
  })

  it('given two entries same dataset, when executing, then both pipe into same writer', async () => {
    // Arrange
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher1 = mockFetcher(async () => createFetchResult(['"a"'], wm))
    const fetcher2 = mockFetcher(async () => createFetchResult(['"b"'], wm))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const entry1 = createEntry({ fetcher: fetcher1, index: 0 })
    const entry2 = createEntry({
      fetcher: fetcher2,
      index: 1,
      label: 'elf:Logout',
      watermarkKey: WatermarkKey.fromEntry({
        sourceOrg: 'src',
        eventLog: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(createWriter.create).toHaveBeenCalledTimes(1)
    expect(writer._writtenLines).toHaveLength(2)
    expect(writer.finalize).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(2)
    expect(sut.groupsUploaded).toBe(1)
  })

  it('given augmentColumns, when piping, then lines contain augmented data', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"001"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher, augmentColumns: { Org: 'prod' } })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(writer._writtenLines[0]).toContain('001')
    expect(writer._writtenLines[0]).toContain('prod')
  })

  it('given mixed success and failure across datasets, when executing, then partial success with correct exit code', async () => {
    // Arrange
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })
    const goodWriter = createMockWriter()
    const badWriter = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn((dataset: DatasetKey) =>
        dataset.name === 'DS' ? goodWriter : badWriter
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
      readerKey: ReaderKey.forElf('src', 'Logout', 'Daily'),
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS2',
      }),
      watermarkKey: WatermarkKey.fromEntry({
        sourceOrg: 'src',
        eventLog: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesProcessed).toBe(1)
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(1)
  })

  it('given writer.finalize throws, when executing, then catches group error and counts all entries as failed', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const writer = createMockWriter({
      finalize: vi.fn(async () => {
        throw new Error('upload crash')
      }),
    })
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
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
    expect(writer.abort).toHaveBeenCalled()
  })

  it('given all entries fail, when executing, then exit code is 2', async () => {
    // Arrange
    const badFetcher = mockFetcher(async () => {
      throw new Error('fail')
    })
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher: badFetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(writer.abort).toHaveBeenCalled()
  })

  it('given entry with data and no watermark returned, when executing, then processes without storing watermark', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult(['"v"']))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesProcessed).toBe(1)
    expect(writer.finalize).toHaveBeenCalled()
    const writtenStore: WatermarkStore = (
      state.write as ReturnType<typeof vi.fn>
    ).mock.calls[0][0]
    const wk = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    })
    expect(writtenStore.get(wk)).toBeUndefined()
  })

  it('given single empty entry, when executing, then calls skip (not abort) and writes state', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const state = createMockState()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state,
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesSkipped).toBe(1)
    expect(sut.groupsUploaded).toBe(0)
    expect(writer.finalize).not.toHaveBeenCalled()
    expect(writer.abort).not.toHaveBeenCalled()
    expect(writer.skip).toHaveBeenCalledTimes(1)
    expect(state.write).toHaveBeenCalledTimes(1)
  })

  it('given writer.abort throws during error handling, when executing, then still counts entries as failed', async () => {
    // Arrange
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const writer = createMockWriter({
      finalize: vi.fn(async () => {
        throw new Error('process failed')
      }),
      abort: vi.fn(async () => {
        throw new Error('abort also failed')
      }),
    })
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesFailed).toBe(1)
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('abort also failed')
    )
  })

  it('given entries with data, when executing, then reports files and rows to tracker', async () => {
    // Arrange
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"r1"', '"r2"'], watermark)
    )
    const tracker = createMockGroupTracker()
    const progress = createMockProgress(tracker)
    const createWriter: CreateWriterPort = {
      create: vi.fn((_ds, _op, listener) => {
        const writable = new PassThrough({ objectMode: true })
        writable.on('data', (batch: string[]) =>
          listener.onRowsWritten(batch.length)
        )
        writable.resume()
        return createMockWriter({ init: vi.fn(async () => writable) })
      }),
    }

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert
    expect(tracker.addFiles).toHaveBeenCalledWith(1)
    expect(tracker.addFiles).toHaveBeenCalledTimes(1)
    expect(tracker.addRows).toHaveBeenCalledWith(2)
    expect(tracker.addRows).toHaveBeenCalledTimes(1)
  })

  it('given progress listener wired, when writer invokes onSinkReady during init, then updates tracker parentId', async () => {
    // Arrange
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"v"'], watermark)
    )
    const tracker = createMockGroupTracker()
    const progress = createMockProgress(tracker)
    const createWriter: CreateWriterPort = {
      create: vi.fn((_ds, _op, listener) => {
        const writable = new PassThrough({ objectMode: true })
        writable.resume()
        return createMockWriter({
          init: vi.fn(async () => {
            listener.onSinkReady('06Vxxx')
            return writable
          }),
        })
      }),
    }

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert
    expect(tracker.updateParentId).toHaveBeenCalledWith('06Vxxx')
  })

  it('given progress listener wired, when writer invokes onChunkWritten, then increments tracker parts', async () => {
    // Arrange
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher = mockFetcher(async () =>
      createFetchResult(['"v"'], watermark)
    )
    const tracker = createMockGroupTracker()
    const progress = createMockProgress(tracker)
    const createWriter: CreateWriterPort = {
      create: vi.fn((_ds, _op, listener) => {
        const writable = new PassThrough({ objectMode: true })
        writable.on('data', () => listener.onChunkWritten())
        writable.resume()
        return createMockWriter({ init: vi.fn(async () => writable) })
      }),
    }

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert
    expect(tracker.incrementParts).toHaveBeenCalled()
  })

  it('given writer.init throws SkipDatasetError, when executing, then warns and skips all entries', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult(['"data"']))
    const skipWriter = createMockWriter({
      init: vi.fn(async () => {
        throw new SkipDatasetError('No metadata for ds')
      }),
    })
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => skipWriter),
    }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert
    expect(sut.entriesSkipped).toBe(1)
    expect(sut.entriesFailed).toBe(0)
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('No metadata')
    )
    expect(skipWriter.abort).not.toHaveBeenCalled()
    expect(skipWriter.finalize).not.toHaveBeenCalled()
  })

  it('given multiple entries piping concurrently, when all finish, then all lines written and finalize called once', async () => {
    // Arrange
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const fetcher1 = mockFetcher(async () =>
      createFetchResult(['row_a1', 'row_a2'], wm)
    )
    const fetcher2 = mockFetcher(async () => createFetchResult(['row_b1'], wm))
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const entry1 = createEntry({ fetcher: fetcher1, index: 0 })
    const entry2 = createEntry({
      fetcher: fetcher2,
      index: 1,
      label: 'elf:Logout',
      readerKey: ReaderKey.forElf('src', 'Logout', 'Daily'),
      watermarkKey: WatermarkKey.fromEntry({
        sourceOrg: 'src',
        eventLog: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(writer._writtenLines).toHaveLength(3)
    expect(writer._writtenLines).toEqual(
      expect.arrayContaining(['row_a1', 'row_a2', 'row_b1'])
    )
    expect(createWriter.create).toHaveBeenCalledTimes(1)
    expect(writer.finalize).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(2)
    expect(sut.groupsUploaded).toBe(1)
  })

  it('given partial failure within group, when executing, then aborts entire group', async () => {
    // Arrange
    const goodFetcher = mockFetcher(async () =>
      createFetchResult(
        ['"v"'],
        Watermark.fromString('2026-03-01T00:00:00.000Z')
      )
    )
    const badFetcher = mockFetcher(async () => {
      throw new Error('fetch error')
    })
    const writer = createMockWriter()
    const createWriter: CreateWriterPort = {
      create: vi.fn(() => writer),
    }
    const goodEntry = createEntry({ fetcher: goodFetcher, index: 0 })
    const badEntry = createEntry({
      fetcher: badFetcher,
      index: 1,
      label: 'elf:Logout',
      readerKey: ReaderKey.forElf('src', 'Logout', 'Daily'),
      watermarkKey: WatermarkKey.fromEntry({
        sourceOrg: 'src',
        eventLog: 'Logout',
        interval: 'Daily',
      }),
    })

    // Act
    const sut = await executePipeline({
      entries: [goodEntry, badEntry],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(sut.entriesFailed).toBe(2)
    expect(sut.entriesProcessed).toBe(0)
    expect(writer.abort).toHaveBeenCalled()
    expect(writer.finalize).not.toHaveBeenCalled()
  })

  it('given two entries sharing readerKey and watermark, when pipeline executes, then fetch called once and both writers receive data', async () => {
    // Arrange
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"a","1"\n', '"b","2"\n'],
        Watermark.fromString('2024-01-02T00:00:00.000Z')
      )
    )
    const writer1 = createMockWriter()
    const writer2 = createMockWriter()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })
    const mockCreateWriter = vi
      .fn()
      .mockReturnValueOnce(writer1)
      .mockReturnValueOnce(writer2)

    // Act
    await executePipeline(
      buildPipelineInput([entry1, entry2], {
        createWriter: { create: mockCreateWriter },
      })
    )

    // Assert — fetch called only once despite two entries
    expect(fetcher.fetch).toHaveBeenCalledTimes(1)
    expect(writer1._writtenLines).toHaveLength(2)
    expect(writer2._writtenLines).toHaveLength(2)
    expect(writer1.finalize).toHaveBeenCalledTimes(1)
    expect(writer2.finalize).toHaveBeenCalledTimes(1)
  })

  it('given two entries sharing readerKey, when source stream errors, then both entries counted as failed', async () => {
    // Arrange
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const errorFetcher = mockFetcher(async () => ({
      lines: (async function* () {
        yield '"a","1"\n'
        throw new Error('mid-stream failure')
      })(),
      watermark: () => Watermark.fromString('2024-01-02T00:00:00.000Z'),
      fileCount: () => 1,
    }))
    const writer1 = createMockWriter()
    const writer2 = createMockWriter()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })
    const mockCreateWriter = vi
      .fn()
      .mockReturnValueOnce(writer1)
      .mockReturnValueOnce(writer2)

    // Act
    const result = await executePipeline(
      buildPipelineInput([entry1, entry2], {
        createWriter: { create: mockCreateWriter },
      })
    )

    // Assert — both entries failed (mid-stream source error)
    expect(result.entriesFailed).toBe(2)
    expect(result.entriesProcessed).toBe(0)
  })

  it('given two entries sharing readerKey, when fetcher.fetch() rejects, then both entries counted as failed and writers abort', async () => {
    // Arrange
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const errorFetcher = mockFetcher(async () => {
      throw new Error('fetch rejected')
    })
    const writer1 = createMockWriter()
    const writer2 = createMockWriter()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })
    const mockCreateWriter = vi
      .fn()
      .mockReturnValueOnce(writer1)
      .mockReturnValueOnce(writer2)

    // Act
    const result = await executePipeline(
      buildPipelineInput([entry1, entry2], {
        createWriter: { create: mockCreateWriter },
      })
    )

    // Assert — fetch rejection before any data: both entries failed, both writers aborted
    expect(result.entriesFailed).toBe(2)
    expect(result.entriesProcessed).toBe(0)
    expect(writer1.abort).toHaveBeenCalled()
    expect(writer2.abort).toHaveBeenCalled()
    expect(writer1.finalize).not.toHaveBeenCalled()
    expect(writer2.finalize).not.toHaveBeenCalled()
  })

  it('given two entries sharing readerKey where one sink write fails, when pipeline executes, then logs fan-out channel write error', async () => {
    // Arrange
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"a","1"\n'],
        Watermark.fromString('2024-01-02T00:00:00.000Z')
      )
    )
    const writer1 = createMockWriter()
    const { Writable } = await import('node:stream')
    const errorWritable = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        cb(new Error('sink write failure'))
      },
    })
    const writer2 = createMockWriter({ init: vi.fn(async () => errorWritable) })
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })
    const mockCreateWriter = vi
      .fn()
      .mockReturnValueOnce(writer1)
      .mockReturnValueOnce(writer2)
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: mockCreateWriter },
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — fan-out channel error logged, only entry2 failed
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('fan-out write failed')
    )
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesProcessed).toBe(1)
  })

  it('given two entries with same readerKey but different watermarks, when pipeline executes, then fetch called twice', async () => {
    // Arrange
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey1 = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const wmKey2 = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
      name: 'other',
    })
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"a"\n'],
        Watermark.fromString('2024-01-02T00:00:00.000Z')
      )
    )
    const store = WatermarkStore.empty()
      .set(wmKey1, Watermark.fromString('2024-01-01T00:00:00.000Z'))
      .set(wmKey2, Watermark.fromString('2024-02-01T00:00:00.000Z'))
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher,
      watermarkKey: wmKey1,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher,
      watermarkKey: wmKey2,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })

    // Act
    await executePipeline(
      buildPipelineInput([entry1, entry2], { watermarks: store })
    )

    // Assert — diverged watermarks → no dedup
    expect(fetcher.fetch).toHaveBeenCalledTimes(2)
  })

  it('given writer.init throws generic error, when executing, then counts entry as failed and calls abort', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult(['"data"']))
    const writer = createMockWriter({
      init: vi.fn(async () => {
        throw new Error('init crashed')
      }),
    })
    const createWriter: CreateWriterPort = { create: vi.fn(() => writer) }
    const logger = createMockLogger()

    // Act
    const sut = await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — covers the non-SkipDatasetError else-branch (pipeline.ts:186-197)
    expect(sut.entriesFailed).toBe(1)
    expect(sut.exitCode).toBe(2)
    expect(writer.abort).toHaveBeenCalled()
  })

  it('given writer.init throws generic error and abort also throws, when executing, then logs debug for abort failure', async () => {
    // Arrange
    const fetcher = mockFetcher(async () => createFetchResult(['"data"']))
    const writer = createMockWriter({
      init: vi.fn(async () => {
        throw new Error('init crashed')
      }),
      abort: vi.fn(async () => {
        throw new Error('abort also crashed')
      }),
    })
    const createWriter: CreateWriterPort = { create: vi.fn(() => writer) }
    const logger = createMockLogger()

    // Act
    await executePipeline({
      entries: [createEntry({ fetcher })],
      watermarks: WatermarkStore.empty(),
      createWriter,
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — covers the .catch() in abort() call (pipeline.ts:188-190)
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('abort also crashed')
    )
  })

  it('given entry with CRM Analytics target org, when executing, then trackGroup is called with org true', async () => {
    // Arrange — kills L152: org !== undefined → false
    const trackGroup = vi.fn(() => createMockGroupTracker())
    const progress: ProgressPort = {
      create: vi.fn(() => ({ tick: vi.fn(), trackGroup, stop: vi.fn() })),
    }
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const entry = createEntry({
      fetcher,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS',
      }),
    })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert
    expect(trackGroup).toHaveBeenCalledWith('DS', true)
  })

  it('given entry with file target (no targetOrg), when executing, then trackGroup is called with org false', async () => {
    // Arrange — kills L152: org !== undefined → true (inverted)
    const trackGroup = vi.fn(() => createMockGroupTracker())
    const progress: ProgressPort = {
      create: vi.fn(() => ({ tick: vi.fn(), trackGroup, stop: vi.fn() })),
    }
    const fetcher = mockFetcher(async () => createFetchResult([]))
    const entry = createEntry({
      fetcher,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
    })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert
    expect(trackGroup).toHaveBeenCalledWith('./out.csv', false)
  })

  it('given group with one processed and one skipped entry, when executing, then finalizes and counts correctly', async () => {
    // Arrange — kills L379 some→every and L394 arithmetic (- skipped → + skipped)
    const datasetKey = DatasetKey.fromEntry({
      targetOrg: 'ana',
      targetDataset: 'DS',
    })
    const wm = Watermark.fromString('2026-03-01T00:00:00.000Z')
    const entry1 = createEntryWithReader(
      ReaderKey.forElf('src', 'Login', 'Daily'),
      {
        index: 0,
        label: 'elf:Login',
        fetcher: mockFetcher(async () => createFetchResult(['"v"'], wm)),
        watermarkKey: WatermarkKey.fromEntry({
          sourceOrg: 'src',
          eventLog: 'Login',
          interval: 'Daily',
        }),
        datasetKey,
        augmentColumns: {},
      }
    )
    const entry2 = createEntryWithReader(
      ReaderKey.forElf('src', 'Logout', 'Daily'),
      {
        index: 1,
        label: 'elf:Logout',
        fetcher: mockFetcher(async () => createFetchResult([])),
        watermarkKey: WatermarkKey.fromEntry({
          sourceOrg: 'src',
          eventLog: 'Logout',
          interval: 'Daily',
        }),
        datasetKey,
        augmentColumns: {},
      }
    )
    const writer = createMockWriter()

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => writer) },
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(writer.finalize).toHaveBeenCalledTimes(1)
    expect(sut.entriesProcessed).toBe(1)
    expect(sut.entriesSkipped).toBe(1)
  })

  it('given group with one failed and one skipped entry, when executing, then aborts and counts correctly', async () => {
    // Arrange — kills L386: group.entries.length - skipped → + skipped (2+1=3 vs 2-1=1)
    const datasetKey = DatasetKey.fromEntry({
      targetOrg: 'ana',
      targetDataset: 'DS',
    })
    const entry1 = createEntryWithReader(
      ReaderKey.forElf('src', 'Login', 'Daily'),
      {
        index: 0,
        label: 'elf:Login',
        fetcher: mockFetcher(async () => {
          throw new Error('fetch failed')
        }),
        watermarkKey: WatermarkKey.fromEntry({
          sourceOrg: 'src',
          eventLog: 'Login',
          interval: 'Daily',
        }),
        datasetKey,
        augmentColumns: {},
      }
    )
    const entry2 = createEntryWithReader(
      ReaderKey.forElf('src', 'Logout', 'Daily'),
      {
        index: 1,
        label: 'elf:Logout',
        fetcher: mockFetcher(async () => createFetchResult([])),
        watermarkKey: WatermarkKey.fromEntry({
          sourceOrg: 'src',
          eventLog: 'Logout',
          interval: 'Daily',
        }),
        datasetKey,
        augmentColumns: {},
      }
    )
    const writer = createMockWriter()

    // Act
    const sut = await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => writer) },
      state: createMockState(),
      progress: createMockProgress(),
      logger: createMockLogger(),
    })

    // Assert
    expect(writer.abort).toHaveBeenCalledTimes(1)
    expect(sut.entriesFailed).toBe(1)
    expect(sut.entriesSkipped).toBe(1)
  })

  it('given two entries sharing readerKey where first sink write fails, when pipeline executes, then logs first entry label in fan-out error', async () => {
    // Arrange — kills L308: idx >= 0 → idx > 0 (index 0 never logged with mutation)
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"a"'],
        Watermark.fromString('2024-01-02T00:00:00.000Z')
      )
    )
    const { Writable } = await import('node:stream')
    const errorWritable = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        cb(new Error('sink write failure'))
      },
    })
    const writer1 = createMockWriter({ init: vi.fn(async () => errorWritable) })
    const writer2 = createMockWriter()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })
    const mockCreateWriter = vi
      .fn()
      .mockReturnValueOnce(writer1)
      .mockReturnValueOnce(writer2)
    const logger = createMockLogger()

    // Act
    await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: mockCreateWriter },
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — fan-out channel 0 error logged with entry1's label
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("Entry 'e1' fan-out write failed")
    )
  })

  it('given entries, when executing, then stops the group tracker after finalizing each slot', async () => {
    // Arrange — kills L367: finally { slot.tracker.stop() } → {}
    const tracker = createMockGroupTracker()
    const progress: ProgressPort = {
      create: vi.fn(() => ({
        tick: vi.fn(),
        trackGroup: vi.fn(() => tracker),
        stop: vi.fn(),
      })),
    }
    const entry = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
    })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert — tracker.stop() must be called once per group in the finally block
    expect(tracker.stop).toHaveBeenCalledTimes(1)
  })

  it('given entries, when executing, then creates progress phase with Processing label and entry count', async () => {
    // Arrange — kills L143 StringLiteral: 'Processing' → ''
    const progress: ProgressPort = {
      create: vi.fn(() => ({
        tick: vi.fn(),
        trackGroup: vi.fn(() => createMockGroupTracker()),
        stop: vi.fn(),
      })),
    }
    const entries = [
      createEntry({ fetcher: mockFetcher(async () => createFetchResult([])) }),
      createEntry({ fetcher: mockFetcher(async () => createFetchResult([])) }),
    ]

    // Act
    await executePipeline({
      entries,
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert — progress.create called with 'Processing' and total entry count
    expect(progress.create).toHaveBeenCalledWith('Processing', 2)
  })

  it('given fetch error, when executing, then warns with entry label and error message and ticks failed status', async () => {
    // Arrange — kills L258 (warn template) and L260 (tick template)
    const tick = vi.fn()
    const progress: ProgressPort = {
      create: vi.fn(() => ({
        tick,
        trackGroup: vi.fn(() => createMockGroupTracker()),
        stop: vi.fn(),
      })),
    }
    const logger = createMockLogger()
    const fetcher = mockFetcher(async () => {
      throw new Error('network error')
    })
    const entry = createEntry({ fetcher, label: 'elf:Login', index: 3 })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger,
    })

    // Assert — warn includes entry label and error; tick includes 'failed'
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("Entry 'elf:Login' failed:")
    )
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('network error')
    )
    expect(tick).toHaveBeenCalledWith(expect.stringContaining('failed'))
  })

  it('given empty source, when executing, then ticks with skipped message including entry label', async () => {
    // Arrange — kills L482 StringLiteral: tick template → ''
    const tick = vi.fn()
    const progress: ProgressPort = {
      create: vi.fn(() => ({
        tick,
        trackGroup: vi.fn(() => createMockGroupTracker()),
        stop: vi.fn(),
      })),
    }
    const entry = createEntry({
      fetcher: mockFetcher(async () => createFetchResult([])),
      label: 'elf:Login',
      index: 0,
    })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert — tick called with skipped message referencing entry
    expect(tick).toHaveBeenCalledWith(expect.stringContaining('skipped'))
    expect(tick).toHaveBeenCalledWith(expect.stringContaining('elf:Login'))
  })

  it('given entries with data, when executing, then ticks with done message including entry label', async () => {
    // Arrange — kills L485 StringLiteral: tick template → ''
    const tick = vi.fn()
    const progress: ProgressPort = {
      create: vi.fn(() => ({
        tick,
        trackGroup: vi.fn(() => createMockGroupTracker()),
        stop: vi.fn(),
      })),
    }
    const entry = createEntry({
      fetcher: mockFetcher(async () =>
        createFetchResult(
          ['"v1"'],
          Watermark.fromString('2026-03-01T00:00:00.000Z')
        )
      ),
      label: 'elf:Login',
      index: 0,
    })

    // Act
    await executePipeline({
      entries: [entry],
      watermarks: WatermarkStore.empty(),
      createWriter: { create: vi.fn(() => createMockWriter()) },
      state: createMockState(),
      progress,
      logger: createMockLogger(),
    })

    // Assert — tick called with done message referencing entry
    expect(tick).toHaveBeenCalledWith(expect.stringContaining('done'))
    expect(tick).toHaveBeenCalledWith(expect.stringContaining('elf:Login'))
  })

  it('given shared-reader entries with source stream error, when executing, then warns Fan-out source failed', async () => {
    // Arrange — kills L316 StringLiteral: 'Fan-out source failed: ...' → ''
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const errorFetcher = mockFetcher(async () => ({
      lines: (async function* () {
        yield '"a"\n'
        throw new Error('mid-stream failure')
      })(),
      watermark: () => undefined,
      fileCount: () => 1,
    }))
    const logger = createMockLogger()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher: errorFetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })

    // Act
    await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: {
        create: vi
          .fn()
          .mockReturnValueOnce(createMockWriter())
          .mockReturnValueOnce(createMockWriter()),
      },
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — fan-out source error logged
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Fan-out source failed')
    )
  })

  it('given shared-reader entries where one sink fails, when executing, then warns with entry-specific failure message', async () => {
    // Arrange — kills L334 StringLiteral: "Entry '...' failed: ..." → ''
    const readerKey = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const fetcher = mockFetcher(async () =>
      createFetchResult(
        ['"a"\n'],
        Watermark.fromString('2024-01-02T00:00:00.000Z')
      )
    )
    const { Writable } = await import('node:stream')
    const errorWritable = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        cb(new Error('sink write failure'))
      },
    })
    const writer2 = createMockWriter({ init: vi.fn(async () => errorWritable) })
    const logger = createMockLogger()
    const entry1 = createEntryWithReader(readerKey, {
      index: 0,
      label: 'e1',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({
        targetOrg: 'ana',
        targetDataset: 'DS1',
      }),
      augmentColumns: {},
    })
    const entry2 = createEntryWithReader(readerKey, {
      index: 1,
      label: 'e2',
      fetcher,
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      augmentColumns: {},
    })

    // Act
    await executePipeline({
      entries: [entry1, entry2],
      watermarks: WatermarkStore.empty(),
      createWriter: {
        create: vi
          .fn()
          .mockReturnValueOnce(createMockWriter())
          .mockReturnValueOnce(writer2),
      },
      state: createMockState(),
      progress: createMockProgress(),
      logger,
    })

    // Assert — warn includes entry label and "failed:" (not "fan-out write failed")
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("Entry 'e2' failed:")
    )
  })
})

describe('groupByReader', () => {
  it('given two entries with same readerKey and same watermark, when grouping, then one bundle', () => {
    // Arrange
    const key = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const wm = Watermark.fromString('2024-01-01T00:00:00.000Z')
    const store = WatermarkStore.empty().set(wmKey, wm)
    const e1 = createEntryWithReader(key, {
      fetcher: mockFetcher(async () => createFetchResult([])),
      watermarkKey: wmKey,
    })
    const e2 = createEntryWithReader(key, {
      fetcher: mockFetcher(async () => createFetchResult([])),
      watermarkKey: wmKey,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
    })

    // Act
    const sut: ReaderBundle[] = groupByReader([e1, e2], store)

    // Assert
    expect(sut).toHaveLength(1)
    expect(sut[0].entries).toHaveLength(2)
    expect(sut[0].watermark?.toString()).toBe(wm.toString())
  })

  it('given two entries with same readerKey but different watermarks, when grouping, then two bundles', () => {
    // Arrange
    const key = ReaderKey.forElf('prod', 'Login', 'Daily')
    const wmKey1 = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const wmKey2 = WatermarkKey.fromEntry({
      sourceOrg: 'prod',
      eventLog: 'Login',
      interval: 'Daily',
      name: 'other',
    })
    const store = WatermarkStore.empty()
      .set(wmKey1, Watermark.fromString('2024-01-01T00:00:00.000Z'))
      .set(wmKey2, Watermark.fromString('2024-02-01T00:00:00.000Z'))
    const e1 = createEntryWithReader(key, {
      fetcher: mockFetcher(async () => createFetchResult([])),
      watermarkKey: wmKey1,
    })
    const e2 = createEntryWithReader(key, {
      fetcher: mockFetcher(async () => createFetchResult([])),
      watermarkKey: wmKey2,
      datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
    })

    // Act
    const sut: ReaderBundle[] = groupByReader([e1, e2], store)

    // Assert
    expect(sut).toHaveLength(2)
  })

  it('given two entries with different readerKeys, when grouping, then two bundles', () => {
    // Arrange
    const e1 = createEntryWithReader(
      ReaderKey.forElf('prod', 'Login', 'Daily'),
      {
        fetcher: mockFetcher(async () => createFetchResult([])),
      }
    )
    const e2 = createEntryWithReader(
      ReaderKey.forElf('prod', 'Login', 'Hourly'),
      {
        fetcher: mockFetcher(async () => createFetchResult([])),
        datasetKey: DatasetKey.fromEntry({ targetFile: './out.csv' }),
      }
    )

    // Act
    const sut: ReaderBundle[] = groupByReader([e1, e2], WatermarkStore.empty())

    // Assert
    expect(sut).toHaveLength(2)
  })
})
