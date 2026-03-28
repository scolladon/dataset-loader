import { describe, expect, it } from 'vitest'
import { Watermark } from '../../../src/domain/watermark.js'

describe('Watermark', () => {
  it('given valid ISO 8601 with Z, when creating, then succeeds', () => {
    // Arrange / Act
    const sut = Watermark.fromString('2026-03-01T00:00:00.000Z')

    // Assert
    expect(sut.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given valid ISO 8601 with +0000, when creating, then succeeds', () => {
    // Arrange / Act
    const sut = Watermark.fromString('2026-03-01T00:00:00.000+0000')

    // Assert
    expect(sut.toString()).toBe('2026-03-01T00:00:00.000+0000')
  })

  it('given invalid string, when creating, then throws with message containing the value', () => {
    // Act & Assert
    expect(() => Watermark.fromString('not-a-date')).toThrow(
      "Invalid watermark: 'not-a-date' is not ISO 8601"
    )
  })

  it('given undefined string, when creating, then throws with message containing the value', () => {
    // Act & Assert
    expect(() => Watermark.fromString('undefined')).toThrow(
      "Invalid watermark: 'undefined' is not ISO 8601"
    )
  })

  it('given valid ISO 8601 without milliseconds, when creating, then succeeds', () => {
    // Arrange / Act
    const sut = Watermark.fromString('2026-03-01T00:00:00Z')

    // Assert
    expect(sut.toString()).toBe('2026-03-01T00:00:00Z')
  })

  it('given string with invalid prefix before date, when creating, then throws', () => {
    // Act & Assert — kills missing ^ anchor mutation
    expect(() => Watermark.fromString('x2026-03-01T00:00:00.000Z')).toThrow(
      "Invalid watermark: 'x2026-03-01T00:00:00.000Z' is not ISO 8601"
    )
  })

  it('given string with trailing garbage after timezone, when creating, then throws', () => {
    // Act & Assert — kills missing $ anchor mutation
    expect(() => Watermark.fromString('2026-03-01T00:00:00.000Zextra')).toThrow(
      "Invalid watermark: '2026-03-01T00:00:00.000Zextra' is not ISO 8601"
    )
  })

  it('given watermark, when converting to SOQL literal, then returns the raw value', () => {
    // Arrange
    const sut = Watermark.fromString('2026-03-01T00:00:00.000Z')

    // Assert
    expect(sut.toSoqlLiteral()).toBe('2026-03-01T00:00:00.000Z')
  })
})
