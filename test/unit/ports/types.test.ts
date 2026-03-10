import { describe, expect, it } from 'vitest'
import {
  formatErrorMessage,
  SF_IDENTIFIER_PATTERN,
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
