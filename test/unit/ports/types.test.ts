import { describe, expect, it } from 'vitest'
import {
  type EntryShape,
  formatErrorMessage,
  isCsv,
  isElf,
  isSObject,
  SF_IDENTIFIER_PATTERN,
  SkipDatasetError,
  SOQL_RELATIONSHIP_PATH_PATTERN,
} from '../../../src/ports/types.js'

describe('SF_IDENTIFIER_PATTERN', () => {
  it('given valid identifier starting with letter, when testing, then matches', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('Account')

    // Assert
    expect(result).toBe(true)
  })

  it('given valid identifier starting with underscore, when testing, then matches', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('_CustomField')

    // Assert
    expect(result).toBe(true)
  })

  it('given identifier with letters numbers underscores, when testing, then matches', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('My_Custom_Object_2')

    // Assert
    expect(result).toBe(true)
  })

  it('given identifier starting with number, when testing, then does not match', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('2Account')

    // Assert
    expect(result).toBe(false)
  })

  it('given empty string, when testing, then does not match', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('')

    // Assert
    expect(result).toBe(false)
  })

  it('given identifier with spaces, when testing, then does not match', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('My Object')

    // Assert
    expect(result).toBe(false)
  })

  it('given identifier with special characters, when testing, then does not match', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('Account-Name')

    // Assert
    expect(result).toBe(false)
  })

  it('given single letter, when testing, then matches', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('a')

    // Assert
    expect(result).toBe(true)
  })

  it('given single underscore, when testing, then matches', () => {
    // Arrange
    const sut = SF_IDENTIFIER_PATTERN

    // Act
    const result = sut.test('_')

    // Assert
    expect(result).toBe(true)
  })
})

describe('SOQL_RELATIONSHIP_PATH_PATTERN', () => {
  it('given simple field name, when testing, then matches', () => {
    expect(SOQL_RELATIONSHIP_PATH_PATTERN.test('Name')).toBe(true)
  })

  it('given relationship path, when testing, then matches', () => {
    expect(SOQL_RELATIONSHIP_PATH_PATTERN.test('Account.Name')).toBe(true)
  })

  it('given string starting with digit before valid path, when testing, then does not match', () => {
    // Kills missing ^ anchor mutation: without ^, '123Account' matches 'Account' in the middle
    expect(SOQL_RELATIONSHIP_PATH_PATTERN.test('123Account')).toBe(false)
  })

  it('given empty string, when testing, then does not match', () => {
    expect(SOQL_RELATIONSHIP_PATH_PATTERN.test('')).toBe(false)
  })
})

describe('SkipDatasetError', () => {
  it('given SkipDatasetError, when constructed, then name is SkipDatasetError', () => {
    // Arrange / Act
    const sut = new SkipDatasetError('skip this dataset')

    // Assert — kills 'SkipDatasetError' string literal mutation
    expect(sut.name).toBe('SkipDatasetError')
  })

  it('given SkipDatasetError, when constructed, then message is preserved', () => {
    const sut = new SkipDatasetError('skip reason')
    expect(sut.message).toBe('skip reason')
  })

  it('given SkipDatasetError, when instanceof checked, then is an Error', () => {
    const sut = new SkipDatasetError('test')
    expect(sut instanceof Error).toBe(true)
  })
})

describe('isElf', () => {
  it('given ELF entry, when checking, then returns true', () => {
    // Arrange
    const entry: EntryShape = {
      sourceOrg: 'org',
      eventLog: 'Login',
      interval: 'Daily',
    }

    // Act
    const sut = isElf(entry)

    // Assert
    expect(sut).toBe(true)
  })

  it('given SObject entry, when checking, then returns false', () => {
    // Arrange
    const entry: EntryShape = { sourceOrg: 'org', sObject: 'Account' }

    // Act
    const sut = isElf(entry)

    // Assert
    expect(sut).toBe(false)
  })
})

describe('isSObject', () => {
  it('given SObject entry, when checking, then returns true', () => {
    // Arrange
    const entry: EntryShape = { sourceOrg: 'org', sObject: 'Account' }

    // Act
    const sut = isSObject(entry)

    // Assert
    expect(sut).toBe(true)
  })

  it('given CSV entry, when checking, then returns false', () => {
    // Arrange
    const entry: EntryShape = { csvFile: './data.csv' }

    // Act
    const sut = isSObject(entry)

    // Assert
    expect(sut).toBe(false)
  })
})

describe('isCsv', () => {
  it('given CSV entry, when checking, then returns true', () => {
    // Arrange
    const entry: EntryShape = { csvFile: './data.csv' }

    // Act
    const sut = isCsv(entry)

    // Assert
    expect(sut).toBe(true)
  })

  it('given ELF entry, when checking, then returns false', () => {
    // Arrange
    const entry: EntryShape = {
      sourceOrg: 'org',
      eventLog: 'Login',
      interval: 'Daily',
    }

    // Act
    const sut = isCsv(entry)

    // Assert
    expect(sut).toBe(false)
  })
})

describe('formatErrorMessage', () => {
  it('given Error instance, when formatting, then returns error message', () => {
    // Arrange
    const error = new Error('something went wrong')

    // Act
    const sut = formatErrorMessage(error)

    // Assert
    expect(sut).toBe('something went wrong')
  })

  it('given string, when formatting, then returns unknown error', () => {
    // Arrange
    const error = 'a string error'

    // Act
    const sut = formatErrorMessage(error)

    // Assert
    expect(sut).toBe('unknown error')
  })

  it('given null, when formatting, then returns unknown error', () => {
    // Arrange
    const error = null

    // Act
    const sut = formatErrorMessage(error)

    // Assert
    expect(sut).toBe('unknown error')
  })

  it('given undefined, when formatting, then returns unknown error', () => {
    // Arrange
    const error = undefined

    // Act
    const sut = formatErrorMessage(error)

    // Assert
    expect(sut).toBe('unknown error')
  })

  it('given number, when formatting, then returns unknown error', () => {
    // Arrange
    const error = 42

    // Act
    const sut = formatErrorMessage(error)

    // Assert
    expect(sut).toBe('unknown error')
  })
})
