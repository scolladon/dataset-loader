import { describe, expect, it } from 'vitest'
import {
  SynthesizeError,
  synthesise,
} from '../../../src/domain/metadata-synthesizer.js'
import type {
  CrmaMetadataJson,
  SourceField,
  SourceSchema,
} from '../../../src/domain/metadata-types.js'

const baseSchema = (fields: SourceField[]): SourceSchema => ({
  datasetName: 'TestDataset',
  label: 'TestDataset',
  fields,
})

describe('synthesise', () => {
  it('given a mixed Text+Numeric+Date+augment schema, when synthesise, then JSON parses with the expected wire shape', () => {
    // Arrange
    const schema = baseSchema([
      { name: 'Id', label: 'Id', type: 'Text', origin: 'reader' },
      {
        name: 'Total',
        label: 'Total',
        type: 'Numeric',
        precision: 10,
        scale: 2,
        origin: 'reader',
      },
      {
        name: 'When',
        label: 'When',
        type: 'Date',
        format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        origin: 'reader',
      },
      {
        name: 'SourceOrg',
        label: 'SourceOrg',
        type: 'Text',
        origin: 'augment',
      },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.fileFormat.numberOfLinesToIgnore).toBe(0)
    expect(sut.fileFormat.charsetName).toBe('UTF-8')
    expect(sut.objects).toHaveLength(1)
    expect(sut.objects[0].name).toBe('TestDataset')
    expect(sut.objects[0].fields).toHaveLength(4)

    const [idField, totalField, whenField, augField] = sut.objects[0].fields
    expect(idField).toEqual({
      name: 'Id',
      fullyQualifiedName: 'Id',
      label: 'Id',
      type: 'Text',
    })
    expect(totalField).toEqual({
      name: 'Total',
      fullyQualifiedName: 'Total',
      label: 'Total',
      type: 'Numeric',
      precision: 10,
      scale: 2,
      defaultValue: '0',
    })
    expect(whenField).toEqual({
      name: 'When',
      fullyQualifiedName: 'When',
      label: 'When',
      type: 'Date',
      format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    })
    expect(whenField).not.toHaveProperty('defaultValue')
    expect(augField).toEqual({
      name: 'SourceOrg',
      fullyQualifiedName: 'SourceOrg',
      label: 'SourceOrg',
      type: 'Text',
    })
  })

  it('given Numeric with precision below CRMA minimum, when synthesise, then precision coerced to 18', () => {
    // Arrange
    const schema = baseSchema([
      {
        name: 'A',
        label: 'A',
        type: 'Numeric',
        precision: 0,
        scale: 0,
        origin: 'reader',
      },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.objects[0].fields[0].precision).toBe(18)
  })

  it('given Numeric with precision above CRMA maximum, when synthesise, then precision coerced to 18', () => {
    // Arrange
    const schema = baseSchema([
      {
        name: 'A',
        label: 'A',
        type: 'Numeric',
        precision: 20,
        scale: 0,
        origin: 'reader',
      },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.objects[0].fields[0].precision).toBe(18)
  })

  it('given Numeric with scale greater than precision, when synthesise, then scale clamped to precision', () => {
    // Arrange
    const schema = baseSchema([
      {
        name: 'A',
        label: 'A',
        type: 'Numeric',
        precision: 5,
        scale: 10,
        origin: 'reader',
      },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    const field = sut.objects[0].fields[0]
    expect(field.precision).toBe(5)
    expect(field.scale).toBe(5)
  })

  it('given Numeric without precision or scale, when synthesise, then defaults applied (precision 18 scale 0 defaultValue 0)', () => {
    // Arrange — provider can omit precision/scale; synthesiser must fill in
    const schema = baseSchema([
      { name: 'A', label: 'A', type: 'Numeric', origin: 'reader' },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.objects[0].fields[0]).toEqual({
      name: 'A',
      fullyQualifiedName: 'A',
      label: 'A',
      type: 'Numeric',
      precision: 18,
      scale: 0,
      defaultValue: '0',
    })
  })

  it('given Date without explicit format, when synthesise, then default datetime format applied', () => {
    // Arrange — provider can omit format; synthesiser must fill in
    const schema = baseSchema([
      { name: 'When', label: 'When', type: 'Date', origin: 'reader' },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.objects[0].fields[0]).toEqual({
      name: 'When',
      fullyQualifiedName: 'When',
      label: 'When',
      type: 'Date',
      format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    })
  })

  it('given duplicate field names with different casing, when synthesise, then throws SynthesizeError', () => {
    // Arrange
    const schema = baseSchema([
      { name: 'Amount', label: 'Amount', type: 'Text', origin: 'reader' },
      { name: 'amount', label: 'amount', type: 'Text', origin: 'reader' },
    ])

    // Act / Assert
    expect(() => synthesise(schema)).toThrow(SynthesizeError)
    expect(() => synthesise(schema)).toThrow(/Duplicate field/)
  })

  it('given an invalid dataset name, when synthesise, then throws SynthesizeError', () => {
    // Arrange
    const schema: SourceSchema = {
      datasetName: '123-bad-name',
      label: 'bad',
      fields: [{ name: 'A', label: 'A', type: 'Text', origin: 'reader' }],
    }

    // Act / Assert
    expect(() => synthesise(schema)).toThrow(SynthesizeError)
    expect(() => synthesise(schema)).toThrow(/Invalid dataset name/)
  })

  it('given an empty fields list, when synthesise, then throws SynthesizeError', () => {
    // Arrange
    const schema = baseSchema([])

    // Act / Assert
    expect(() => synthesise(schema)).toThrow(SynthesizeError)
    expect(() => synthesise(schema)).toThrow(/no fields/)
  })

  it('given an invalid field name (dotted path not translated), when synthesise, then throws SynthesizeError', () => {
    // Arrange
    const schema = baseSchema([
      { name: 'Bad.Name', label: 'Bad.Name', type: 'Text', origin: 'reader' },
    ])

    // Act / Assert
    expect(() => synthesise(schema)).toThrow(SynthesizeError)
    expect(() => synthesise(schema)).toThrow(/Invalid field name/)
  })

  it('given an augment column among reader columns, when synthesise, then ordering is preserved (synthesiser is order-preserving)', () => {
    // Arrange — augment in the middle, intentionally non-canonical
    const schema = baseSchema([
      { name: 'A', label: 'A', type: 'Text', origin: 'reader' },
      { name: 'Aug', label: 'Aug', type: 'Text', origin: 'augment' },
      { name: 'B', label: 'B', type: 'Text', origin: 'reader' },
    ])

    // Act
    const sut = JSON.parse(synthesise(schema)) as CrmaMetadataJson

    // Assert
    expect(sut.objects[0].fields.map(f => f.name)).toEqual(['A', 'Aug', 'B'])
  })
})
