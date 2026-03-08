import { describe, expect, it } from 'vitest'
import { Watermark } from '../../../src/domain/watermark.js'

describe('Watermark', () => {
  it('given valid ISO 8601 with Z, when creating, then succeeds', () => {
    const sut = Watermark.fromString('2026-03-01T00:00:00.000Z')
    expect(sut.toString()).toBe('2026-03-01T00:00:00.000Z')
  })

  it('given valid ISO 8601 with +0000, when creating, then succeeds', () => {
    const sut = Watermark.fromString('2026-03-01T00:00:00.000+0000')
    expect(sut.toString()).toBe('2026-03-01T00:00:00.000+0000')
  })

  it('given invalid string, when creating, then throws', () => {
    expect(() => Watermark.fromString('not-a-date')).toThrow()
  })

  it('given undefined string, when creating, then throws', () => {
    expect(() => Watermark.fromString('undefined')).toThrow()
  })

  it('given watermark, when converting to SOQL literal, then returns the raw value', () => {
    const sut = Watermark.fromString('2026-03-01T00:00:00.000Z')
    expect(sut.toSoqlLiteral()).toBe('2026-03-01T00:00:00.000Z')
  })
})
