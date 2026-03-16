import { describe, expect, it } from 'vitest'
import { ReaderKey } from '../../../src/domain/reader-key.js'

describe('ReaderKey', () => {
  it('given ELF config, when creating key, then format is elf:sourceOrg:eventType:interval', () => {
    const sut = ReaderKey.forElf('prod', 'Login', 'Daily')
    expect(sut.toString()).toBe('elf\u0000prod\u0000Login\u0000Daily')
  })

  it('given SObject config, when creating key, then format encodes all query fields', () => {
    const sut = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id', 'Name'],
      'LastModifiedDate',
      undefined,
      undefined
    )
    expect(sut.toString()).toBe(
      'sobject\u0000prod\u0000User\u0000Id,Name\u0000LastModifiedDate\u0000\u00000'
    )
  })

  it('given SObject config with where and limit, when creating key, then includes them', () => {
    const sut = ReaderKey.forSObject(
      'prod',
      'User',
      ['Id'],
      'CreatedDate',
      'IsActive = true',
      1000
    )
    expect(sut.toString()).toBe(
      'sobject\u0000prod\u0000User\u0000Id\u0000CreatedDate\u0000IsActive = true\u00001000'
    )
  })

  it('given two ELF keys with same config, when comparing, then they are equal', () => {
    const a = ReaderKey.forElf('prod', 'Login', 'Daily')
    const b = ReaderKey.forElf('prod', 'Login', 'Daily')
    expect(a.toString()).toBe(b.toString())
  })

  it('given two ELF keys with different interval, when comparing, then they differ', () => {
    const a = ReaderKey.forElf('prod', 'Login', 'Daily')
    const b = ReaderKey.forElf('prod', 'Login', 'Hourly')
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given two SObject keys with different field order, when comparing, then they differ', () => {
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
    expect(a.toString()).not.toBe(b.toString())
  })

  it('given CSV file path, when creating key, then format is csv-null-filePath', () => {
    const sut = ReaderKey.forCsv('./data/login-events.csv')
    expect(sut.toString()).toBe('csv\u0000./data/login-events.csv')
  })

  it('given two CSV keys with same path, when comparing, then they are equal', () => {
    const a = ReaderKey.forCsv('./data/login-events.csv')
    const b = ReaderKey.forCsv('./data/login-events.csv')
    expect(a.toString()).toBe(b.toString())
  })

  it('given two CSV keys with different paths, when comparing, then they differ', () => {
    const a = ReaderKey.forCsv('./data/a.csv')
    const b = ReaderKey.forCsv('./data/b.csv')
    expect(a.toString()).not.toBe(b.toString())
  })
})
