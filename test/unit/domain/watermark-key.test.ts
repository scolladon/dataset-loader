import { describe, expect, it } from 'vitest'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'

describe('WatermarkKey', () => {
  it('given ELF entry, when creating key, then format is sourceOrg:elf:eventLog:interval', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    })

    // Assert
    expect(sut.toString()).toBe('src:elf:Login:Daily')
  })

  it('given SObject entry, when creating key, then format is sourceOrg:sobject:sObject', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      sObject: 'Account',
    })

    // Assert
    expect(sut.toString()).toBe('src:sobject:Account')
  })

  it('given ELF entry with name, when creating key, then uses name as key', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
      name: 'my-login-loader',
    })

    // Assert
    expect(sut.toString()).toBe('my-login-loader')
  })

  it('given SObject entry with name, when creating key, then uses name as key', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      sObject: 'Account',
      name: 'account-sync',
    })

    // Assert
    expect(sut.toString()).toBe('account-sync')
  })

  it('given two ELF keys with same sourceOrg/eventLog/interval, when comparing toString, then they are equal', () => {
    // Arrange
    const a = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    })
    const b = WatermarkKey.fromEntry({
      sourceOrg: 'src',
      eventLog: 'Login',
      interval: 'Daily',
    })

    // Assert
    expect(a.toString()).toBe(b.toString())
  })

  it('given CSV entry without name, when creating key, then format is csv-colon-csvFile', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      csvFile: './data/login.csv',
    })

    // Assert
    expect(sut.toString()).toBe('csv:./data/login.csv')
  })

  it('given CSV entry with name, when creating key, then uses name as key', () => {
    // Arrange / Act
    const sut = WatermarkKey.fromEntry({
      csvFile: './data/login.csv',
      name: 'login-data',
    })

    // Assert
    expect(sut.toString()).toBe('login-data')
  })
})
