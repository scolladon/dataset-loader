import { type ReadStream } from 'node:fs'
import { Readable } from 'node:stream'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { CsvReader } from '../../../src/adapters/readers/csv-reader.js'
import { Watermark } from '../../../src/domain/watermark.js'

vi.mock('node:fs', () => ({
  createReadStream: vi.fn(),
}))

import { createReadStream } from 'node:fs'

function makeStream(content: string): ReadStream {
  return Readable.from(Buffer.from(content)) as unknown as ReadStream
}

async function collectLines(
  iterable: AsyncIterable<string[]>
): Promise<string[]> {
  const lines: string[] = []
  for await (const batch of iterable) lines.push(...batch)
  return lines
}

describe('CsvReader', () => {
  beforeEach(() => {
    vi.resetAllMocks()
  })

  describe('header()', () => {
    it('given CSV file with header and data rows, when calling header, then returns first line', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        makeStream('col1,col2,col3\nval1,val2,val3\n')
      )
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.header()

      // Assert
      expect(result).toBe('col1,col2,col3')
    })

    it('given header called twice, when calling header, then opens file only once', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        makeStream('col1,col2\nrow1\n')
      )
      const sut = new CsvReader('./data/test.csv')

      // Act
      await sut.header()
      await sut.header()

      // Assert
      expect(createReadStream).toHaveBeenCalledTimes(1)
    })

    it('given empty file, when calling header, then returns empty string', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(makeStream(''))
      const sut = new CsvReader('./data/empty.csv')

      // Act
      const result = await sut.header()

      // Assert
      expect(result).toBe('')
    })
  })

  describe('fetch()', () => {
    it('given CSV file with header and data rows, when fetching, then yields data lines without header', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        makeStream('col1,col2\n"val1","val2"\n"val3","val4"\n')
      )
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.fetch()
      const lines = await collectLines(result.lines)

      // Assert
      expect(lines).toEqual(['"val1","val2"', '"val3","val4"'])
    })

    it('given CSV file with empty lines, when fetching, then skips empty lines', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        makeStream('header\nrow1\n\nrow2\n')
      )
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.fetch()
      const lines = await collectLines(result.lines)

      // Assert
      expect(lines).toEqual(['row1', 'row2'])
    })

    it('given any file, when fetching, then fileCount is always 1', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(makeStream('header\nrow1\n'))
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.fetch()
      await collectLines(result.lines)

      // Assert
      expect(result.fileCount()).toBe(1)
    })

    it('given any file, when fetching, then watermark is a valid ISO 8601 timestamp', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(makeStream('header\nrow1\n'))
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.fetch()
      await collectLines(result.lines)

      // Assert — verify against the domain's own validator, not an inline regex
      const wm = result.watermark()
      expect(wm).toBeDefined()
      expect(() => Watermark.fromString(wm!.toString())).not.toThrow()
    })

    it('given a watermark from a previous run, when fetching, then ignores it and reads full file', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        makeStream('header\nrow1\nrow2\n')
      )
      const sut = new CsvReader('./data/test.csv')
      const previousWatermark = Watermark.fromString('2026-01-01T00:00:00.000Z')

      // Act — pass a real watermark; all rows must still appear (CSV is non-incremental)
      const result = await sut.fetch(previousWatermark)
      const lines = await collectLines(result.lines)

      // Assert
      expect(lines).toHaveLength(2)
    })

    it('given header-only file, when fetching, then yields no lines and fileCount is still 1', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(makeStream('header\n'))
      const sut = new CsvReader('./data/test.csv')

      // Act
      const result = await sut.fetch()
      const lines = await collectLines(result.lines)

      // Assert
      expect(lines).toHaveLength(0)
      expect(result.fileCount()).toBe(1)
    })

    it('given non-existent file, when fetching and iterating lines, then throws', async () => {
      // Arrange
      vi.mocked(createReadStream).mockReturnValue(
        (() => {
          const r = new Readable({
            read() {
              /* no-op: push is driven externally */
            },
          })
          process.nextTick(() => r.destroy(new Error('ENOENT: no such file')))
          return r as unknown as ReadStream
        })()
      )
      const sut = new CsvReader('./data/missing.csv')

      // Act & Assert
      const result = await sut.fetch()
      await expect(collectLines(result.lines)).rejects.toThrow('ENOENT')
    })
  })

  it('given header called before fetch, when fetching, then yields correct data lines', async () => {
    // Arrange
    vi.mocked(createReadStream)
      .mockReturnValueOnce(makeStream('col1,col2\nval1,val2\n'))
      .mockReturnValueOnce(makeStream('col1,col2\nval1,val2\n'))
    const sut = new CsvReader('./data/test.csv')

    // Act
    const header = await sut.header()
    const result = await sut.fetch()
    const lines = await collectLines(result.lines)

    // Assert
    expect(header).toBe('col1,col2')
    expect(lines).toEqual(['val1,val2'])
  })
})
