import { describe, expect, it } from 'vitest'
import { ReaderKey } from '../../../src/domain/reader-key.js'

describe('ReaderKey', () => {
  it('given ELF config, when creating key, then format is elf:sourceOrg:eventType:interval', () => {
    // Arrange / Act
    const sut = ReaderKey.forElf('prod', 'Login', 'Daily')

    // Assert
    expect(sut.toString()).toBe('elf\u0000prod\u0000Login\u0000Daily')
  })

  it('given SObject config, when creating key, then format encodes all query fields', () => {
    // Arrange / Act
    const sut = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id', 'Name'],
      'LastModifiedDate',
      undefined,
      undefined
    )

    // Assert
    expect(sut.toString()).toBe(
      'sobject\u0000prod\u0000User\u0000Id,Name\u0000LastModifiedDate\u0000\u00000'
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
      1000
    )

    // Assert
    expect(sut.toString()).toBe(
      'sobject\u0000prod\u0000User\u0000Id\u0000CreatedDate\u0000IsActive = true\u00001000'
    )
  })

  it('given two ELF keys with same config, when comparing, then they are equal', () => {
    // Arrange
    const a = ReaderKey.forElf('prod', 'Login', 'Daily')
    const b = ReaderKey.forElf('prod', 'Login', 'Daily')

    // Assert
    expect(a.toString()).toBe(b.toString())
  })

  it('given two ELF keys with different interval, when comparing, then they differ', () => {
    // Arrange
    const a = ReaderKey.forElf('prod', 'Login', 'Daily')
    const b = ReaderKey.forElf('prod', 'Login', 'Hourly')

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
      undefined
    )
    const b = ReaderKey.forSObject(
      'prod',
      'User',
      ['Name', 'Id'],
      'LastModifiedDate',
      undefined,
      undefined
    )

    // Assert
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given CSV file path, when creating key, then format is csv-null-filePath', () => {
    // Arrange / Act
    const sut = ReaderKey.forCsv('./data/login-events.csv')

    // Assert
    expect(sut.toString()).toBe('csv\u0000./data/login-events.csv')
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
})
