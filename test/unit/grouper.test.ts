import { describe, it, expect } from 'vitest'
import { group } from '../../src/core/grouper.js'
import { type GroupInput } from '../../src/types.js'

describe('Grouper', () => {
  it('given single result, when grouping, then returns it as-is', () => {
    // Arrange
    const inputs: GroupInput[] = [{ key: 'org:ds', csv: '"H1"\n"r1"', operation: 'Append' }]

    // Act
    const sut = group(inputs)

    // Assert
    expect(sut.size).toBe(1)
    expect(sut.get('org:ds')!.csv).toBe('"H1"\n"r1"')
    expect(sut.get('org:ds')!.operation).toBe('Append')
  })

  it('given two results with same key, when grouping, then merges CSV data rows', () => {
    // Arrange
    const inputs: GroupInput[] = [
      { key: 'org:ds', csv: '"H1","H2"\n"a","b"', operation: 'Append' },
      { key: 'org:ds', csv: '"H1","H2"\n"c","d"\n"e","f"', operation: 'Append' },
    ]

    // Act
    const sut = group(inputs)

    // Assert
    expect(sut.size).toBe(1)
    const lines = sut.get('org:ds')!.csv.split('\n')
    expect(lines[0]).toBe('"H1","H2"')
    expect(lines[1]).toBe('"a","b"')
    expect(lines[2]).toBe('"c","d"')
    expect(lines[3]).toBe('"e","f"')
  })

  it('given results with different keys, when grouping, then keeps them separate', () => {
    // Arrange
    const inputs: GroupInput[] = [
      { key: 'org:ds1', csv: '"H"\n"a"', operation: 'Append' },
      { key: 'org:ds2', csv: '"H"\n"b"', operation: 'Overwrite' },
    ]

    // Act
    const sut = group(inputs)

    // Assert
    expect(sut.size).toBe(2)
    expect(sut.get('org:ds1')!.csv).toBe('"H"\n"a"')
    expect(sut.get('org:ds2')!.csv).toBe('"H"\n"b"')
  })

  it('given empty inputs, when grouping, then returns empty map', () => {
    // Act
    const sut = group([])

    // Assert
    expect(sut.size).toBe(0)
  })

  it('given result with only header (no data), when merging, then does not add empty lines', () => {
    // Arrange
    const inputs: GroupInput[] = [
      { key: 'k', csv: '"H"\n"a"', operation: 'Append' },
      { key: 'k', csv: '"H"', operation: 'Append' },
    ]

    // Act
    const sut = group(inputs)

    // Assert
    expect(sut.get('k')!.csv).toBe('"H"\n"a"')
  })
})
