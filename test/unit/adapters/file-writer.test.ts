import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { rm } from 'node:fs/promises'
import { join } from 'node:path'
import { Readable } from 'node:stream'
import { finished, pipeline } from 'node:stream/promises'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  FileWriter,
  FileWriterFactory,
} from '../../../src/adapters/writers/file-writer.js'
import { DatasetKey } from '../../../src/domain/dataset-key.js'
import { type ProgressListener } from '../../../src/ports/types.js'

const TMP_DIR = join(process.cwd(), 'test', 'tmp')
const FILE_PATH = join(TMP_DIR, 'output.csv')

const mockHeaderProvider = {
  resolveHeader: vi.fn().mockResolvedValue('COL_A,COL_B'),
}

beforeEach(() => {
  mkdirSync(TMP_DIR, { recursive: true })
  vi.clearAllMocks()
})

afterEach(async () => {
  await rm(TMP_DIR, { recursive: true, force: true })
})

describe('Given FileWriter in Overwrite mode', () => {
  describe('When data is written', () => {
    it('Then creates file with header then data lines each terminated by newline', async () => {
      // Arrange
      const sut = new FileWriter(FILE_PATH, 'Overwrite', mockHeaderProvider)
      const chunker = await sut.init()

      // Act
      await pipeline(Readable.from([['line1', 'line2']]), chunker)
      await sut.finalize()

      // Assert
      const content = readFileSync(FILE_PATH, 'utf-8')
      expect(content).toBe('COL_A,COL_B\nline1\nline2\n')
    })

    it('Then finalize returns { parentId: filePath, partCount: 0 }', async () => {
      const sut = new FileWriter(FILE_PATH, 'Overwrite', mockHeaderProvider)
      const chunker = await sut.init()
      await pipeline(Readable.from([['data']]), chunker)
      const result = await sut.finalize()
      expect(result).toEqual({ parentId: FILE_PATH, partCount: 0 })
    })
  })

  describe('When abort() is called after partial write', () => {
    it('Then deletes the file', async () => {
      const sut = new FileWriter(FILE_PATH, 'Overwrite', mockHeaderProvider)
      await sut.init()
      await sut.abort()
      expect(existsSync(FILE_PATH)).toBe(false)
    })
  })

  describe('When skip() is called', () => {
    it('Then deletes the empty file created during init()', async () => {
      const sut = new FileWriter(FILE_PATH, 'Overwrite', mockHeaderProvider)
      await sut.init()
      await sut.skip()
      expect(existsSync(FILE_PATH)).toBe(false)
    })
  })

  describe('When an empty batch is written', () => {
    it('Then file content is unchanged (no spurious newline)', async () => {
      // Arrange
      const sut = new FileWriter(FILE_PATH, 'Overwrite', mockHeaderProvider)
      const writable = await sut.init()

      // Act
      writable.write([])
      writable.write(['a'])
      writable.end()
      await finished(writable)

      // Assert
      const content = readFileSync(FILE_PATH, 'utf-8')
      expect(content).toBe('COL_A,COL_B\na\n')
    })
  })
})

describe('Given FileWriter in Append mode on non-empty file', () => {
  beforeEach(() => {
    writeFileSync(FILE_PATH, 'COL_A,COL_B\nexisting-line\n')
  })

  describe('When data is written', () => {
    it('Then appends data without re-writing header', async () => {
      const sut = new FileWriter(FILE_PATH, 'Append', mockHeaderProvider)
      const chunker = await sut.init()
      await pipeline(Readable.from([['new-line']]), chunker)
      await sut.finalize()
      const content = readFileSync(FILE_PATH, 'utf-8')
      expect(content).toBe('COL_A,COL_B\nexisting-line\nnew-line\n')
    })
  })

  describe('When skip() is called', () => {
    it('Then preserves existing file content', async () => {
      const sut = new FileWriter(FILE_PATH, 'Append', mockHeaderProvider)
      await sut.init()
      await sut.skip()
      expect(readFileSync(FILE_PATH, 'utf-8')).toBe(
        'COL_A,COL_B\nexisting-line\n'
      )
    })
  })
})

describe('Given FileWriter in Append mode on missing file', () => {
  describe('When data is written', () => {
    it('Then creates file with header', async () => {
      const sut = new FileWriter(FILE_PATH, 'Append', mockHeaderProvider)
      const chunker = await sut.init()
      await pipeline(Readable.from([['data']]), chunker)
      await sut.finalize()
      const content = readFileSync(FILE_PATH, 'utf-8')
      expect(content).toBe('COL_A,COL_B\ndata\n')
    })
  })
})

describe('Given FileWriter with a ProgressListener', () => {
  describe('When batches are written', () => {
    it('Then onRowsWritten is called with each batch size', async () => {
      // Arrange
      const listener: ProgressListener = {
        onSinkReady: vi.fn(),
        onChunkWritten: vi.fn(),
        onRowsWritten: vi.fn(),
      }
      const sut = new FileWriter(
        FILE_PATH,
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
  })
})

describe('Given FileWriterFactory', () => {
  describe('When creating a writer and writing data', () => {
    it('Then forwards the listener to FileWriter', async () => {
      // Arrange
      const listener: ProgressListener = {
        onSinkReady: vi.fn(),
        onChunkWritten: vi.fn(),
        onRowsWritten: vi.fn(),
      }
      const headerProvider = { resolveHeader: vi.fn().mockResolvedValue('COL') }
      const dsKey = DatasetKey.fromEntry({ targetFile: FILE_PATH })
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
})
