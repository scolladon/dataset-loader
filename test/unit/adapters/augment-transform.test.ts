import { describe, expect, it } from 'vitest'
import {
  buildAugmentHeaderSuffix,
  buildAugmentSuffix,
} from '../../../src/adapters/pipeline/augment-transform.js'

describe('buildAugmentSuffix', () => {
  it('given empty columns, when building suffix, then returns empty string', () => {
    // Arrange / Act
    const sut = buildAugmentSuffix({})

    // Assert
    expect(sut).toBe('')
  })

  it('given single column, when building suffix, then returns comma-prefixed quoted value', () => {
    // Arrange / Act
    const sut = buildAugmentSuffix({ Org: 'prod' })

    // Assert
    expect(sut).toBe(',"prod"')
  })

  it('given value with quotes, when building suffix, then escapes quotes', () => {
    // Arrange / Act
    const sut = buildAugmentSuffix({ Name: 'O"Brien' })

    // Assert
    expect(sut).toBe(',"O""Brien"')
  })

  it('given multiple columns, when building suffix, then joins all values', () => {
    // Arrange / Act
    const sut = buildAugmentSuffix({ A: '1', B: '2' })

    // Assert
    expect(sut).toBe(',"1","2"')
  })
})

describe('buildAugmentHeaderSuffix', () => {
  it('given empty columns, when building header suffix, then returns empty string', () => {
    // Arrange / Act
    const sut = buildAugmentHeaderSuffix({})

    // Assert
    expect(sut).toBe('')
  })

  it('given columns with values, when building header suffix, then returns comma-separated column names without quoting', () => {
    // Arrange / Act
    const sut = buildAugmentHeaderSuffix({
      SourceOrg: '00D123',
      Interval: 'Hourly',
    })

    // Assert
    expect(sut).toBe(',SourceOrg,Interval')
  })
})
