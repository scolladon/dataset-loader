import { randomUUID } from 'node:crypto'
import { createWriteStream, existsSync } from 'node:fs'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  FileWriter,
  FileWriterFactory,
} from '../../../src/adapters/writers/file-writer.js'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import { type ProgressListener } from '../../../src/ports/types.js'

vi.mock('node:fs', async importOriginal => {
  const real = await importOriginal<typeof import('node:fs')>()
  return { ...real, createWriteStream: vi.fn(real.createWriteStream) }
})

const mockHeaderProvider = {
  resolveHeader: vi.fn().mockResolvedValue('COL_A,COL_B'),
}

let testDir: string
let filePath: string

beforeEach(async () => {
  testDir = join(tmpdir(), `file-writer-${randomUUID()}`)
  await mkdir(testDir, { recursive: true })
  filePath = join(testDir, 'output.csv')
  // Reset any per-test mock overrides; the spy falls back to its initial real implementation
  vi.mocked(createWriteStream).mockReset()
})

afterEach(async () => {
  await rm(testDir, { recursive: true, force: true })
})

describe('FileWriter', () => {
  it('given FileWriter in Overwrite mode, when data is written, then creates file with header then data lines each terminated by newline', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['line1', 'line2']]), chunker)
    await sut.finalize()

    // Assert
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\nline1\nline2\n')
  })

  it('given FileWriter in Overwrite mode, when data is written, then finalize returns parentId and partCount zero', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['data']]), chunker)
    const result = await sut.finalize()

    // Assert
    expect(result).toEqual({ parentId: filePath, partCount: 0 })
  })

  it('given FileWriter in Overwrite mode, when abort is called after partial write, then deletes the file', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    await sut.init()
    // WriteStream opens the file asynchronously; wait for it to exist before asserting deletion
    // Kills L82 BlockStatement: empty abort() leaves file on disk
    await vi.waitFor(() => expect(existsSync(filePath)).toBe(true), {
      timeout: 1000,
    })

    // Act
    await sut.abort()

    // Assert
    expect(existsSync(filePath)).toBe(false)
  })

  it('given FileWriter in Overwrite mode, when skip is called, then deletes the empty file created during init', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    await sut.init()

    // Act
    await sut.skip()

    // Assert
    expect(existsSync(filePath)).toBe(false)
  })

  it('given FileWriter in Overwrite mode, when abort is called without init, then resolves without error', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)

    // Act & Assert
    await expect(sut.abort()).resolves.toBeUndefined()
  })

  it('given FileWriter in Overwrite mode, when an empty batch is written, then file content has no spurious newline', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const writable = await sut.init()

    // Act
    writable.write([])
    writable.write(['a'])
    writable.end()
    await finished(writable)

    // Assert
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\na\n')
  })

  it('given FileWriter in Overwrite mode on pre-existing file, when data is written, then overwrites existing content', async () => {
    // Arrange — kills `this.operation === 'Append'` → `true` mutation
    await writeFile(filePath, 'OLD_HEADER\nold-line\n')
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['new-line']]), chunker)
    await sut.finalize()

    // Assert — content must not contain old data
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\nnew-line\n')
  })

  it('given FileWriter in Append mode on empty file, when data is written, then writes header', async () => {
    // Arrange — kills `size > 0` → `>= 0` mutation (empty file should NOT be treated as non-empty)
    await writeFile(filePath, '')
    const sut = new FileWriter(filePath, 'Append', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['data']]), chunker)
    await sut.finalize()

    // Assert — header must be written even though file existed with 0 bytes
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\ndata\n')
  })

  it('given FileWriter in Overwrite mode, when two batches are written, then header appears exactly once', async () => {
    // Arrange — kills `this.headerWritten = true` → `false` mutation
    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const writable = await sut.init()

    // Act — two separate write calls = two batches
    writable.write(['row1'])
    writable.write(['row2'])
    writable.end()
    await finished(writable)

    // Assert
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\nrow1\nrow2\n')
  })

  it('given FileWriter in Append mode on non-empty file, when data is written, then appends data without re-writing header', async () => {
    // Arrange
    await writeFile(filePath, 'COL_A,COL_B\nexisting-line\n')
    const sut = new FileWriter(filePath, 'Append', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['new-line']]), chunker)
    await sut.finalize()

    // Assert
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\nexisting-line\nnew-line\n')
  })

  it('given FileWriter in Append mode on non-empty file, when skip is called, then preserves existing file content', async () => {
    // Arrange
    await writeFile(filePath, 'COL_A,COL_B\nexisting-line\n')
    const sut = new FileWriter(filePath, 'Append', mockHeaderProvider)
    await sut.init()

    // Act
    await sut.skip()

    // Assert
    expect(await readFile(filePath, 'utf-8')).toBe(
      'COL_A,COL_B\nexisting-line\n'
    )
  })

  it('given FileWriter in Append mode, when skip is called, then does not delete the file', async () => {
    // Arrange — kills `this.operation === 'Overwrite'` → `true` mutation in skip()
    const sut = new FileWriter(filePath, 'Append', mockHeaderProvider)
    await sut.init() // creates file (empty) via createWriteStream with flags='w'

    // Act
    await sut.skip()

    // Assert — Append mode skip must NOT delete; with mutation 'true', rmSync would delete
    expect(existsSync(filePath)).toBe(true)
  })

  it('given FileWriter in Append mode on missing file, when data is written, then creates file with header', async () => {
    // Arrange
    const sut = new FileWriter(filePath, 'Append', mockHeaderProvider)
    const chunker = await sut.init()

    // Act
    await pipeline(Readable.from([['data']]), chunker)
    await sut.finalize()

    // Assert
    const content = await readFile(filePath, 'utf-8')
    expect(content).toBe('COL_A,COL_B\ndata\n')
  })

  it('given FileWriter with a ProgressListener, when batches are written, then onRowsWritten is called with each batch size', async () => {
    // Arrange
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
      onRowsWritten: vi.fn(),
    }
    const sut = new FileWriter(
      filePath,
      'Overwrite',
      mockHeaderProvider,
      listener
    )
    const writable = await sut.init()

    // Act
    writable.write(['a', 'b'])
    writable.write(['c'])
    writable.end()
    await finished(writable)

    // Assert
    expect(listener.onRowsWritten).toHaveBeenCalledTimes(2)
    expect(listener.onRowsWritten).toHaveBeenNthCalledWith(1, 2)
    expect(listener.onRowsWritten).toHaveBeenNthCalledWith(2, 1)
  })

  it('given stream write callback fires with error, when writing data, then error propagates', async () => {
    // Arrange
    const mockStream = new Writable({
      write(_chunk, _enc, cb) {
        cb(new Error('ENOSPC: no space left on device'))
      },
    })
    mockStream.on('error', () => {
      /* noop */
    }) // suppress unhandled stream 'error' event
    vi.mocked(createWriteStream).mockReturnValueOnce(
      mockStream as ReturnType<typeof createWriteStream>
    )

    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    const chunker = await sut.init()
    chunker.write(['line1'])

    // Act & Assert
    await expect(finished(chunker)).rejects.toThrow(
      'ENOSPC: no space left on device'
    )
  })

  it('given stream end callback fires with error, when closing stream, then error propagates', async () => {
    // Arrange
    const mockStream = new Writable({
      write(_chunk, _enc, cb) {
        cb()
      },
      final(cb) {
        cb(new Error('close failed'))
      },
    })
    mockStream.on('error', () => {
      /* noop */
    }) // suppress unhandled stream 'error' event
    vi.mocked(createWriteStream).mockReturnValueOnce(
      mockStream as ReturnType<typeof createWriteStream>
    )

    const sut = new FileWriter(filePath, 'Overwrite', mockHeaderProvider)
    await sut.init()

    // Act & Assert
    await expect(sut.finalize()).rejects.toThrow('close failed')
  })
})

describe('FileWriterFactory', () => {
  it('given FileWriterFactory, when creating a writer and writing data, then forwards the listener to FileWriter', async () => {
    // Arrange
    const listener: ProgressListener = {
      onSinkReady: vi.fn(),
      onChunkWritten: vi.fn(),
      onRowsWritten: vi.fn(),
    }
    const headerProvider = { resolveHeader: vi.fn().mockResolvedValue('COL') }
    const dsKey = DatasetKey.fromEntry({ targetFile: filePath })
    const sut = new FileWriterFactory()

    // Act
    const writer = sut.create(dsKey, 'Overwrite', listener, headerProvider)
    const writable = await writer.init()
    writable.write(['row1'])
    writable.end()
    await finished(writable)

    // Assert
    expect(listener.onRowsWritten).toHaveBeenCalledWith(1)
  })
})
