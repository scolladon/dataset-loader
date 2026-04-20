import { describe, expect, it } from 'vitest'
import { parseCsvHeader } from '../../../src/domain/column-name.js'

describe('parseCsvHeader', () => {
  it('given empty string, when parsing, then returns empty array', () => {
    // Arrange / Act
    const sut = parseCsvHeader('')

    // Assert
    expect(sut).toEqual([])
  })

  it('given a simple comma-separated header, when parsing, then returns fields in order', () => {
    // Arrange / Act
    const sut = parseCsvHeader('a,b,c')

    // Assert
    expect(sut).toEqual(['a', 'b', 'c'])
  })

  it('given a BOM-prefixed header, when parsing, then strips the BOM', () => {
    // Arrange / Act
    const sut = parseCsvHeader('\uFEFFa,b')

    // Assert
    expect(sut).toEqual(['a', 'b'])
  })

  it('given a trailing carriage return, when parsing, then strips the CR', () => {
    // Arrange / Act
    const sut = parseCsvHeader('a,b\r')

    // Assert
    expect(sut).toEqual(['a', 'b'])
  })

  it('given surrounding whitespace in fields, when parsing, then trims fields', () => {
    // Arrange / Act
    const sut = parseCsvHeader(' a , b ,  c  ')

    // Assert
    expect(sut).toEqual(['a', 'b', 'c'])
  })

  it('given quoted fields, when parsing, then unwraps the quotes', () => {
    // Arrange / Act
    const sut = parseCsvHeader('"a","b","c"')

    // Assert
    expect(sut).toEqual(['a', 'b', 'c'])
  })

  it('given a quoted field containing a comma, when parsing, then preserves the comma inside the cell', () => {
    // Arrange / Act
    const sut = parseCsvHeader('"col, with comma",b')

    // Assert
    expect(sut).toEqual(['col, with comma', 'b'])
  })

  it('given a quoted field with escaped quote, when parsing, then unescapes the inner quote', () => {
    // Arrange / Act
    const sut = parseCsvHeader('"a""b",c')

    // Assert
    expect(sut).toEqual(['a"b', 'c'])
  })

  it('given empty cells between commas, when parsing, then filters them out', () => {
    // Arrange / Act
    const sut = parseCsvHeader('a,,b')

    // Assert
    expect(sut).toEqual(['a', 'b'])
  })

  it('given a trailing comma, when parsing, then filters the phantom empty cell', () => {
    // Arrange / Act
    const sut = parseCsvHeader('a,b,')

    // Assert
    expect(sut).toEqual(['a', 'b'])
  })

  it('given BOM + CR + quoted + whitespace combined, when parsing, then applies every normalization', () => {
    // Arrange / Act
    const sut = parseCsvHeader('\uFEFF "a" , "b, c" ,d\r')

    // Assert
    expect(sut).toEqual(['a', 'b, c', 'd'])
  })
})
