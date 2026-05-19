import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { CsvBootstrapProvider } from '../../../../src/adapters/readers/csv-bootstrap-provider.js'

describe('CsvBootstrapProvider', () => {
  let dir: string

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'csv-bootstrap-'))
  })

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true })
  })

  async function tmpFile(content: string): Promise<string> {
    const p = join(dir, 'data.csv')
    await writeFile(p, content, 'utf-8')
    return p
  }

  it('given a CSV with header id,name,amount, when build, then emits three Text fields preserving source order', async () => {
    // Arrange
    const filePath = await tmpFile('id,name,amount\n1,Alpha,100\n')
    const sut = new CsvBootstrapProvider({
      filePath,
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields).toEqual([
      { name: 'id', label: 'id', type: 'Text', origin: 'reader' },
      { name: 'name', label: 'name', type: 'Text', origin: 'reader' },
      { name: 'amount', label: 'amount', type: 'Text', origin: 'reader' },
    ])
  })

  it('given an empty CSV file, when build, then throws with bootstrap-requires-header message', async () => {
    // Arrange
    const filePath = await tmpFile('')
    const sut = new CsvBootstrapProvider({
      filePath,
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act / Assert
    await expect(sut.buildSourceSchema()).rejects.toThrow(
      /CSV bootstrap requires at least one header line/
    )
  })

  it('given augment columns, when build, then appended after reader fields with origin: augment', async () => {
    // Arrange
    const filePath = await tmpFile('a,b\n')
    const sut = new CsvBootstrapProvider({
      filePath,
      datasetName: 'X',
      augmentColumns: { Tag: 'batch-1' },
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(
      result.fields.map(f => ({ name: f.name, origin: f.origin }))
    ).toEqual([
      { name: 'a', origin: 'reader' },
      { name: 'b', origin: 'reader' },
      { name: 'Tag', origin: 'augment' },
    ])
  })

  it('given a non-existent file path, when build, then throws an ENOENT-shaped error', async () => {
    // Arrange
    const sut = new CsvBootstrapProvider({
      filePath: join(dir, 'does-not-exist.csv'),
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act / Assert
    await expect(sut.buildSourceSchema()).rejects.toThrow(
      /ENOENT|no such file/i
    )
  })

  it('given a header with quoted columns and trailing commas, when build, then parseCsvHeader normalises', async () => {
    // Arrange
    const filePath = await tmpFile('"id","first name","amount",\n')
    const sut = new CsvBootstrapProvider({
      filePath,
      datasetName: 'X',
      augmentColumns: {},
    })

    // Act
    const result = await sut.buildSourceSchema()

    // Assert
    expect(result.fields.map(f => f.name)).toEqual([
      'id',
      'first name',
      'amount',
    ])
  })
})
