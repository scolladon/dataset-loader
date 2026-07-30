import { describe, expect, it } from 'vitest'
import { DateBounds } from '../../../src/domain/date-bounds.js'
import { ReaderKey } from '../../../src/domain/reader-key.js'

const none = () => DateBounds.none()
const INF_SUFFIX = `\x00[-∞, +∞]`

describe('ReaderKey', () => {
  it('given ELF config, when creating key, then format is elf:sourceOrg:eventType:interval:bounds', () => {
    // Arrange / Act
    const sut = ReaderKey.forElf('prod', 'Login', 'Daily', none())

    // Assert
    expect(sut.toString()).toBe(`elf\x00prod\x00Login\x00Daily${INF_SUFFIX}`)
  })

  it('given SObject config, when creating key, then format encodes all query fields and bounds', () => {
    // Arrange / Act
    const sut = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id', 'Name'],
      'LastModifiedDate',
      undefined,
      undefined,
      none()
    )

    // Assert
    expect(sut.toString()).toBe(
      `sobject\x00prod\x00User\x00Id,Name\x00LastModifiedDate\x00\x000${INF_SUFFIX}`
    )
  })

  it('given SObject config with where and limit, when creating key, then includes them', () => {
    // Arrange / Act
    const sut = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id'],
      'CreatedDate',
      'IsActive = true',
      1000,
      none()
    )

    // Assert
    expect(sut.toString()).toBe(
      `sobject\x00prod\x00User\x00Id\x00CreatedDate\x00IsActive = true\x001000${INF_SUFFIX}`
    )
  })

  it('given two ELF keys with same config, when comparing, then they are equal', () => {
    // Arrange
    const a = ReaderKey.forElf('prod', 'Login', 'Daily', none())
    const b = ReaderKey.forElf('prod', 'Login', 'Daily', none())

    // Assert
    expect(a.toString()).toBe(b.toString())
  })

  it('given two ELF keys with different interval, when comparing, then they differ', () => {
    // Arrange
    const a = ReaderKey.forElf('prod', 'Login', 'Daily', none())
    const b = ReaderKey.forElf('prod', 'Login', 'Hourly', none())

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given two SObject keys with different field order, when comparing, then they differ', () => {
    // Arrange
    const a = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id', 'Name'],
      'LastModifiedDate',
      undefined,
      undefined,
      none()
    )
    const b = ReaderKey.forSObject(
      'prod',
      'User',
      ['Name', 'Id'],
      'LastModifiedDate',
      undefined,
      undefined,
      none()
    )

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given two SObject keys with different bounds, when comparing, then they differ', () => {
    // Arrange
    const sameShape = {
      org: 'prod',
      obj: 'User',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      where: undefined,
      limit: undefined,
    } as const
    const a = ReaderKey.forSObject(
      sameShape.org,
      sameShape.obj,
      sameShape.fields,
      sameShape.dateField,
      sameShape.where,
      sameShape.limit,
      none()
    )
    const b = ReaderKey.forSObject(
      sameShape.org,
      sameShape.obj,
      sameShape.fields,
      sameShape.dateField,
      sameShape.where,
      sameShape.limit,
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given two ELF keys with different bounds, when comparing, then they differ', () => {
    // Arrange
    const a = ReaderKey.forElf('prod', 'Login', 'Daily', none())
    const b = ReaderKey.forElf(
      'prod',
      'Login',
      'Daily',
      DateBounds.from('2026-01-01T00:00:00.000Z', undefined)
    )

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given CSV file path, when creating key, then format is csv-null-filePath', () => {
    // Arrange / Act
    const sut = ReaderKey.forCsv('./data/login-events.csv')

    // Assert
    expect(sut.toString()).toBe('csv\x00./data/login-events.csv')
  })

  it('given two CSV keys with same path, when comparing, then they are equal', () => {
    // Arrange
    const a = ReaderKey.forCsv('./data/login-events.csv')
    const b = ReaderKey.forCsv('./data/login-events.csv')

    // Assert
    expect(a.toString()).toBe(b.toString())
  })

  it('given two CSV keys with different paths, when comparing, then they differ', () => {
    // Arrange
    const a = ReaderKey.forCsv('./data/a.csv')
    const b = ReaderKey.forCsv('./data/b.csv')

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  // Discrimination guards: ReaderKey is a de-duplication key in the pipeline;
  // if a StringLiteral mutation collapses any axis into a constant, two
  // distinct configs would silently share the same reader. Each case must
  // produce a different key.
  describe('discrimination axes', () => {
    it.each([
      [
        'sourceOrg',
        ReaderKey.forElf('prod', 'Login', 'Daily', none()),
        ReaderKey.forElf('sandbox', 'Login', 'Daily', none()),
      ],
      [
        'eventType',
        ReaderKey.forElf('prod', 'Login', 'Daily', none()),
        ReaderKey.forElf('prod', 'Logout', 'Daily', none()),
      ],
    ])(
      'given two ELF keys that differ only by %s, when comparing, then they differ',
      (_axis, a, b) => {
        expect(a.toString()).not.toBe(b.toString())
      }
    )

    it.each([
      [
        'sourceOrg',
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
        ReaderKey.forSObject(
          'sandbox',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
      ],
      [
        'sobject',
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
        ReaderKey.forSObject(
          'prod',
          'Contact',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
      ],
      [
        'dateField',
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'CreatedDate',
          undefined,
          undefined,
          none()
        ),
      ],
      [
        'where',
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          'IsActive = true',
          undefined,
          none()
        ),
      ],
      [
        'queryLimit',
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          undefined,
          none()
        ),
        ReaderKey.forSObject(
          'prod',
          'Account',
          ['Id'],
          'LastModifiedDate',
          undefined,
          1000,
          none()
        ),
      ],
    ])(
      'given two SObject keys that differ only by %s, when comparing, then they differ',
      (_axis, a, b) => {
        expect(a.toString()).not.toBe(b.toString())
      }
    )
  })
})
