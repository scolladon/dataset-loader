import { describe, it, expect } from 'vitest'
import { augment } from '../../src/core/augmenter.js'

describe('Augmenter', () => {
  it('given csv and columns, when augmenting, then appends headers and values', () => {
    // Arrange
    const csv = '"Name","Id"\n"Acme","001"'
    const columns = { OrgId: '00D001', Env: 'Prod' }

    // Act
    const sut = augment(csv, columns)

    // Assert
    expect(sut).toBe('"Name","Id","OrgId","Env"\n"Acme","001","00D001","Prod"')
  })

  it('given empty columns, when augmenting, then returns csv unchanged', () => {
    // Arrange
    const csv = '"Name"\n"Acme"'

    // Act
    const sut = augment(csv, {})

    // Assert
    expect(sut).toBe(csv)
  })

  it('given multiple data rows, when augmenting, then appends values to all rows', () => {
    // Arrange
    const csv = '"H1"\n"r1"\n"r2"\n"r3"'
    const columns = { X: 'val' }

    // Act
    const sut = augment(csv, columns)

    // Assert
    const lines = sut.split('\n')
    expect(lines[0]).toBe('"H1","X"')
    expect(lines[1]).toBe('"r1","val"')
    expect(lines[2]).toBe('"r2","val"')
    expect(lines[3]).toBe('"r3","val"')
  })

  it('given values with double quotes, when augmenting, then escapes them', () => {
    // Arrange
    const csv = '"H"\n"v"'
    const columns = { Col: 'say "hello"' }

    // Act
    const sut = augment(csv, columns)

    // Assert
    expect(sut).toBe('"H","Col"\n"v","say ""hello"""')
  })

  it('given trailing empty line, when augmenting, then skips it', () => {
    // Arrange
    const csv = '"H"\n"v"\n'

    // Act
    const sut = augment(csv, { X: '1' })

    // Assert
    expect(sut).toBe('"H","X"\n"v","1"')
  })
})
