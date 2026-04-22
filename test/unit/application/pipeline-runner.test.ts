import { randomUUID } from 'node:crypto'
import { rmSync, writeFileSync } from 'node:fs'
import os from 'node:os'
import { join } from 'node:path'
import { Messages } from '@salesforce/core'
import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'
import {
  PipelineRunner,
  type PipelineRunnerDeps,
} from '../../../src/application/pipeline-runner.js'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import type { PipelineEntry } from '../../../src/domain/pipeline.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'
import { loadDatasetLoadMessages } from '../../../src/ports/messages.js'
import {
  type CreateWriterPort,
  type ProgressPort,
  type StatePort,
} from '../../../src/ports/types.js'
import {
  csvEntry as csv,
  elfEntry as elf,
  makeCaptureLogger as makeLogger,
  resolvedOf as resolved,
  sobjectEntry as sobject,
} from '../../fixtures/application.js'
import { makeSfPort } from '../../fixtures/sf-port.js'

// Shallow mock: executePipeline is the seam between PipelineRunner and the
// domain pipeline. Mocking it lets us verify the runner's responsibilities
// (warning emission, summary logging, reader dedup, exit-code propagation)
// without spinning up the full pipeline machinery. The real end-to-end path
// is covered by NUT tests.
vi.mock('../../../src/domain/pipeline.js', async orig => {
  const actual = await orig<typeof import('../../../src/domain/pipeline.js')>()
  return {
    ...actual,
    executePipeline: vi.fn().mockResolvedValue({
      entriesProcessed: 1,
      entriesSkipped: 0,
      entriesFailed: 0,
      groupsUploaded: 1,
      exitCode: 0,
    }),
  }
})

import { executePipeline } from '../../../src/domain/pipeline.js'

const createWriter: CreateWriterPort = {
  create: vi.fn(() => ({
    init: vi.fn(),
    finalize: vi.fn(),
    abort: vi.fn(),
    skip: vi.fn(),
  })),
}

const progress: ProgressPort = {
  create: vi.fn(() => ({
    tick: vi.fn(),
    trackGroup: vi.fn(() => ({
      updateParentId: vi.fn(),
      incrementParts: vi.fn(),
      addFiles: vi.fn(),
      addRows: vi.fn(),
      stop: vi.fn(),
    })),
    stop: vi.fn(),
  })),
}

const state: StatePort = {
  read: vi.fn().mockResolvedValue(WatermarkStore.empty()),
  write: vi.fn().mockResolvedValue(undefined),
}

function makeRunner() {
  const { logger, logs, warns } = makeLogger()
  const deps: PipelineRunnerDeps = {
    logger,
    messages: loadDatasetLoadMessages(),
    createWriter,
    progress,
  }
  return { runner: new PipelineRunner(deps), logs, warns }
}

describe('PipelineRunner', () => {
  let savedExitCode: typeof process.exitCode
  beforeAll(() => {
    // Required because `loadDatasetLoadMessages()` reads the bundle that
    // the command module normally imports at import time.
    Messages.importMessagesDirectoryFromMetaUrl(
      new URL('../../../package.json', import.meta.url).toString()
    )
  })
  beforeEach(() => {
    savedExitCode = process.exitCode
    process.exitCode = undefined
    vi.clearAllMocks()
    vi.mocked(executePipeline).mockResolvedValue({
      entriesProcessed: 2,
      entriesSkipped: 1,
      entriesFailed: 0,
      groupsUploaded: 1,
      exitCode: 0,
    })
  })
  afterEach(() => {
    process.exitCode = savedExitCode
  })

  it('given warnings-triggering inputs, when running, then every warning is emitted through logger.warn', async () => {
    // Arrange — fresh-state ELF + no --start-date → FIRST_RUN_ELF fires
    const { runner, warns } = makeRunner()

    // Act
    await runner.run(
      [resolved(elf)],
      new Map([['src', makeSfPort()]]),
      WatermarkStore.empty(),
      state,
      DateBounds.none()
    )

    // Assert — kills the BlockStatement mutant that removes the warn-loop
    expect(warns.some(w => w.includes('FIRST_RUN_ELF'))).toBe(true)
  })

  it('given executePipeline result, when running, then summary log uses the exact "Done:" template with every counter', async () => {
    // Arrange — kills the StringLiteral mutant on the summary template
    vi.mocked(executePipeline).mockResolvedValueOnce({
      entriesProcessed: 3,
      entriesSkipped: 2,
      entriesFailed: 1,
      groupsUploaded: 1,
      exitCode: 0,
    })
    const { runner, logs } = makeRunner()

    // Act
    await runner.run(
      [resolved(sobject)],
      new Map([['src', makeSfPort()]]),
      WatermarkStore.empty(),
      state,
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )

    // Assert
    expect(logs).toContain(
      'Done: 3 processed, 2 skipped, 1 failed, 1 groups uploaded'
    )
  })

  it('given executePipeline returns non-zero exit code, when running, then process.exitCode is propagated', async () => {
    // Arrange
    vi.mocked(executePipeline).mockResolvedValueOnce({
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: 2,
      groupsUploaded: 0,
      exitCode: 3,
    })
    const { runner } = makeRunner()

    // Act
    const result = await runner.run(
      [resolved(sobject)],
      new Map([['src', makeSfPort()]]),
      WatermarkStore.empty(),
      state,
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )

    // Assert
    expect(process.exitCode).toBe(3)
    expect(result.entriesFailed).toBe(2)
  })

  it('given two CSV entries sharing the same csvFile, when running, then only one CsvReader instance is built (reader-cache hits)', async () => {
    // Arrange — kills the `if (!existing)` block-statement + boolean
    // mutants at pipeline-runner.ts:122 by asserting that the fetcher
    // instance is shared across entries that resolve to the same readerKey.
    // Need a real file on disk because CsvReader.header() reads it during
    // pass 2 (resolveAlignment).
    const csvPath = join(
      os.tmpdir(),
      `pipeline-runner-dedup-${randomUUID()}.csv`
    )
    writeFileSync(csvPath, 'col\nrow\n')
    const csvClone = { ...csv, csvFile: csvPath }
    const { runner } = makeRunner()

    try {
      // Act — two CSV entries pointing at the same file
      await runner.run(
        [resolved(csvClone, 0), resolved(csvClone, 1)],
        new Map(),
        WatermarkStore.empty(),
        state,
        DateBounds.none()
      )

      // Assert — same fetcher instance shared across both slots
      const call = vi.mocked(executePipeline).mock.calls[0][0]
      const entries = call.entries as readonly PipelineEntry[]
      expect(entries).toHaveLength(2)
      expect(entries[0].fetcher).toBe(entries[1].fetcher)
    } finally {
      rmSync(csvPath, { force: true })
    }
  })

  it('given SObject entry, when running, then readerKey is an sobject-kind key (not elf-kind)', async () => {
    // Arrange — kills the ConditionalExpression mutant in createReaderKey
    // (`if (isElfEntry(entry))` → `if (true)`) by asserting that an SObject
    // entry produces a readerKey whose serialized form starts with
    // `sobject` and not `elf`.
    const { runner } = makeRunner()

    // Act
    await runner.run(
      [resolved(sobject)],
      new Map([['src', makeSfPort()]]),
      WatermarkStore.empty(),
      state,
      DateBounds.none()
    )

    // Assert
    const call = vi.mocked(executePipeline).mock.calls[0][0]
    const entries = call.entries as readonly PipelineEntry[]
    expect(entries[0].readerKey.toString().startsWith('sobject\x00')).toBe(true)
  })

  it('given ELF entry, when running, then readerKey is an elf-kind key', async () => {
    // Arrange — symmetric kill for the same ConditionalExpression mutant.
    const { runner } = makeRunner()

    // Act — seed a watermark so FIRST_RUN_ELF is suppressed
    await runner.run(
      [resolved(elf)],
      new Map([
        [
          'src',
          makeSfPort({
            query: vi
              .fn()
              .mockResolvedValue({ totalSize: 0, done: true, records: [] }),
          }),
        ],
      ]),
      WatermarkStore.empty(),
      state,
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )

    // Assert
    const call = vi.mocked(executePipeline).mock.calls[0][0]
    const entries = call.entries as readonly PipelineEntry[]
    expect(entries[0].readerKey.toString().startsWith('elf\x00')).toBe(true)
  })
})
