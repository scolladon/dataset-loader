import { describe, expect, it } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'

describe('DatasetKey', () => {
  it('given file target, when creating from entry with targetFile, then toString returns file-namespaced key', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })

    // Assert
    expect(sut.toString()).toBe('file:./output/login.csv')
  })

  it('given file target, when accessing name, then returns the file path', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })

    // Assert
    expect(sut.name).toBe('./output/login.csv')
  })

  it('given file target, when accessing org, then returns undefined', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })

    // Assert
    expect(sut.org).toBeUndefined()
  })

  it('given two file keys with same path, when comparing toString, then they are equal', () => {
    // Arrange
    const key1 = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
    const key2 = DatasetKey.fromEntry({ targetFile: './output/login.csv' })

    // Assert
    expect(key1.toString()).toBe(key2.toString())
  })

  it('given empty targetOrg with targetDataset, when creating from entry, then throws with message', () => {
    // Act & Assert
    expect(() =>
      DatasetKey.fromEntry({ targetOrg: '', targetDataset: 'LoginEvents' })
    ).toThrow('targetOrg must not be empty')
  })

  it('given org target, when creating from entry with targetOrg and targetDataset, then toString returns org-namespaced key', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({
      targetOrg: 'my-org',
      targetDataset: 'LoginEvents',
    })

    // Assert
    expect(sut.toString()).toBe('org:my-org:LoginEvents')
  })

  it('given org target, when accessing org, then returns the targetOrg', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({
      targetOrg: 'ana',
      targetDataset: 'MyDS',
    })

    // Assert
    expect(sut.org).toBe('ana')
  })

  it('given org target, when accessing name, then returns the targetDataset', () => {
    // Arrange / Act
    const sut = DatasetKey.fromEntry({
      targetOrg: 'ana',
      targetDataset: 'MyDS',
    })

    // Assert
    expect(sut.name).toBe('MyDS')
  })

  it('given two org keys with same values, when comparing toString, then they are equal', () => {
    // Arrange
    const a = DatasetKey.fromEntry({ targetOrg: 'ana', targetDataset: 'DS' })
    const b = DatasetKey.fromEntry({ targetOrg: 'ana', targetDataset: 'DS' })

    // Assert
    expect(a.toString()).toBe(b.toString())
  })
})
