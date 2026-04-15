import { describe, expect, it } from 'vitest'
import { Watermark } from '../../../src/domain/watermark.js'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'
import { WatermarkStore } from '../../../src/domain/watermark-store.js'

describe('WatermarkStore', () => {
  const elfEntry = {
    sourceOrg: 'src',
    eventLog: 'Login',
    interval: 'Daily',
  }

  it('given empty record, when creating store, then get returns undefined', () => {
    // Arrange / Act
    const sut = WatermarkStore.fromRecord({})

    // Assert
    expect(sut.get(WatermarkKey.fromEntry(elfEntry))).toBeUndefined()
  })

  it('given record with entry, when creating store, then get returns watermark', () => {
    // Arrange / Act
    const sut = WatermarkStore.fromRecord({
      'src:elf:Login:Daily': '2026-03-01T00:00:00.000Z',
    })

    // Assert
    expect(sut.get(WatermarkKey.fromEntry(elfEntry))?.toString()).toBe(
      '2026-03-01T00:00:00.000Z'
    )
  })

  it('given store, when setting a value, then returns new store with updated value', () => {
    // Arrange
    const original = WatermarkStore.fromRecord({})
    const key = WatermarkKey.fromEntry(elfEntry)
    const watermark = Watermark.fromString('2026-03-01T00:00:00.000Z')

    // Act
    const sut = original.set(key, watermark)

    // Assert
    expect(sut.get(key)!.toString()).toBe('2026-03-01T00:00:00.000Z')
    expect(original.get(key)).toBeUndefined()
  })

  it('given store with values, when converting to record, then returns plain object', () => {
    // Arrange
    const store = WatermarkStore.fromRecord({
      'src:elf:Login:Daily': '2026-03-01T00:00:00.000Z',
    })

    // Act
    const sut = store.toRecord()

    // Assert
    expect(sut).toEqual({ 'src:elf:Login:Daily': '2026-03-01T00:00:00.000Z' })
  })

  it('given empty factory, when creating, then toRecord returns empty object', () => {
    // Arrange / Act
    const sut = WatermarkStore.empty()

    // Assert
    expect(sut.toRecord()).toEqual({})
  })
})
