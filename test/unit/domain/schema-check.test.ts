import { describe, expect, it } from 'vitest'
import { checkSchemaAlignment } from '../../../src/domain/schema-check.js'

const baseInput = {
  datasetName: 'ALM_X',
  entryLabel: 'entry-1',
}

describe('checkSchemaAlignment', () => {
  it('given identical lists, when checking set-only, then ok with no casing diff', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B', 'C'],
      provided: ['A', 'B', 'C'],
      checkOrder: false,
    })

    // Assert
    expect(sut).toEqual({ ok: true, casingDiff: false })
  })

  it('given identical sets in different order, when checking set-only, then ok', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B', 'C'],
      provided: ['C', 'A', 'B'],
      checkOrder: false,
    })

    // Assert
    expect(sut).toEqual({ ok: true, casingDiff: false })
  })

  it('given identical sets in different order, when checking order, then fail with positional diff', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B', 'C'],
      provided: ['A', 'C', 'B'],
      checkOrder: true,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) {
      expect(sut.reason).toMatch(/Order mismatch/)
      expect(sut.reason).toMatch(/position 1/)
    }
  })

  it('given a missing provided field, when checking, then fail listing missing', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B', 'C'],
      provided: ['A', 'B'],
      checkOrder: false,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) expect(sut.reason).toMatch(/missing.*\bC\b/)
  })

  it('given an extra provided field, when checking, then fail listing extra', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B'],
      provided: ['A', 'B', 'X'],
      checkOrder: false,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) expect(sut.reason).toMatch(/not in dataset.*\bX\b/)
  })

  it('given case-only diff, when checking, then ok with casingDiff true', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['UserId', 'IsActive'],
      provided: ['userid', 'isactive'],
      checkOrder: false,
    })

    // Assert
    expect(sut).toEqual({ ok: true, casingDiff: true })
  })

  it('given both missing and extra fields, when checking, then fail listing both', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B', 'C'],
      provided: ['A', 'X'],
      checkOrder: false,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) {
      expect(sut.reason).toMatch(/missing.*\bB\b.*\bC\b/)
      expect(sut.reason).toMatch(/not in dataset.*\bX\b/)
    }
  })

  it('given order-check failure, when checking, then reason hints at trailing-augment rule', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'OrgId', 'B'],
      provided: ['A', 'B', 'OrgId'],
      checkOrder: true,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) expect(sut.reason).toMatch(/augment.*trailing/i)
  })

  it('given dataset name and entry label, when failing, then both appear in the reason', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      datasetName: 'ALM_USERS',
      entryLabel: 'users-prod',
      expected: ['A'],
      provided: ['B'],
      checkOrder: false,
    })

    // Assert
    expect(sut.ok).toBe(false)
    if (!sut.ok) {
      expect(sut.reason).toContain('ALM_USERS')
      expect(sut.reason).toContain('users-prod')
    }
  })

  it('given identical-with-order check, when checking, then ok with no casing diff', () => {
    // Arrange / Act
    const sut = checkSchemaAlignment({
      ...baseInput,
      expected: ['A', 'B'],
      provided: ['A', 'B'],
      checkOrder: true,
    })

    // Assert
    expect(sut).toEqual({ ok: true, casingDiff: false })
  })
})
