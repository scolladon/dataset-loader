import { Readable } from 'node:stream'
import { describe, expect, it } from 'vitest'
import { CsvStream } from '../../../src/adapters/csv-stream.js'

function csvStream(content: string): Readable {
  return Readable.from(Buffer.from(content))
}

async function* toAsyncIterable<T>(...items: T[]): AsyncGenerator<T> {
  for (const item of items) {
    yield item
  }
}

async function collectLines(gen: AsyncGenerator<string>): Promise<string[]> {
  const result: string[] = []
  for await (const line of gen) {
    result.push(line)
  }
  return result
}

describe('CsvStream', () => {
  it('given single stream with headers and rows, when transforming, then yields header line and row lines', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable(
      csvStream('"Id","Name"\n"001","Acme"\n"002","Foo"')
    )

    // Act
    const lines = await collectLines(sut.transform(streams, {}))

    // Assert
    expect(lines).toEqual(['"Id","Name"\n', '"001","Acme"\n', '"002","Foo"\n'])
  })

  it('given multiple streams, when transforming, then header emitted once and subsequent headers skipped', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable(
      csvStream('"Id","Name"\n"001","Acme"'),
      csvStream('"Id","Name"\n"002","Bar"')
    )

    // Act
    const lines = await collectLines(sut.transform(streams, {}))

    // Assert
    expect(lines).toEqual(['"Id","Name"\n', '"001","Acme"\n', '"002","Bar"\n'])
  })

  it('given two transform calls, when transforming, then headers emitted once across calls', async () => {
    // Arrange
    const sut = new CsvStream()
    const firstStreams = toAsyncIterable(csvStream('"Id"\n"001"'))
    const secondStreams = toAsyncIterable(csvStream('"Id"\n"002"'))

    // Act
    const firstLines = await collectLines(sut.transform(firstStreams, {}))
    const secondLines = await collectLines(sut.transform(secondStreams, {}))

    // Assert
    expect(firstLines).toEqual(['"Id"\n', '"001"\n'])
    expect(secondLines).toEqual(['"002"\n'])
  })

  it('given augment columns, when transforming, then appended to header and values to rows', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable(csvStream('"Id"\n"001"'))

    // Act
    const lines = await collectLines(
      sut.transform(streams, { Source: 'ELF', Type: 'Login' })
    )

    // Assert
    expect(lines).toEqual(['"Id","Source","Type"\n', '"001","ELF","Login"\n'])
  })

  it('given empty stream with only header, when transforming, then yields only header line', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable(csvStream('"Id","Name"\n'))

    // Act
    const lines = await collectLines(sut.transform(streams, {}))

    // Assert
    expect(lines).toEqual(['"Id","Name"\n'])
  })

  it('given empty augment columns, when transforming, then no extra columns added', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable(csvStream('"Id"\n"001"'))

    // Act
    const lines = await collectLines(sut.transform(streams, {}))

    // Assert
    expect(lines).toEqual(['"Id"\n', '"001"\n'])
  })

  it('given empty async iterable, when transforming, then yields nothing', async () => {
    // Arrange
    const sut = new CsvStream()
    const streams = toAsyncIterable<Readable>()

    // Act
    const lines = await collectLines(sut.transform(streams, {}))

    // Assert
    expect(lines).toEqual([])
    expect(sut.headersEmitted).toBe(false)
  })

  it('given augment columns on second transform call, when transforming, then augments rows only without re-emitting header', async () => {
    // Arrange
    const sut = new CsvStream()
    const firstStreams = toAsyncIterable(csvStream('"Id"\n"001"'))
    const secondStreams = toAsyncIterable(csvStream('"Id"\n"002"'))

    // Act
    const firstLines = await collectLines(
      sut.transform(firstStreams, { Source: 'ELF' })
    )
    const secondLines = await collectLines(
      sut.transform(secondStreams, { Source: 'ELF' })
    )

    // Assert
    expect(firstLines).toEqual(['"Id","Source"\n', '"001","ELF"\n'])
    expect(secondLines).toEqual(['"002","ELF"\n'])
  })
})
