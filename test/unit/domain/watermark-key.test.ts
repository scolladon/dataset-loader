import { describe, expect, it } from 'vitest'
import { WatermarkKey } from '../../../src/domain/watermark-key.js'

describe('WatermarkKey', () => {
  it('given ELF entry, when creating key, then format is sourceOrg:elf:eventType:interval', () => {
    const sut = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS',
      eventType: 'Login',
      interval: 'Daily',
    })
    expect(sut.toString()).toBe('src:elf:Login:Daily')
  })

  it('given SObject entry, when creating key, then format is sourceOrg:sobject:sobject', () => {
    const sut = WatermarkKey.fromEntry({
      type: 'sobject',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS',
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
    })
    expect(sut.toString()).toBe('src:sobject:Account')
  })

  it('given ELF entry with name, when creating key, then uses name as key', () => {
    const sut = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS',
      eventType: 'Login',
      interval: 'Daily',
      name: 'my-login-loader',
    })
    expect(sut.toString()).toBe('my-login-loader')
  })

  it('given SObject entry with name, when creating key, then uses name as key', () => {
    const sut = WatermarkKey.fromEntry({
      type: 'sobject',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS',
      sobject: 'Account',
      fields: ['Id'],
      dateField: 'LastModifiedDate',
      name: 'account-sync',
    })
    expect(sut.toString()).toBe('account-sync')
  })

  it('given two keys with same value, when comparing toString, then they are equal', () => {
    const a = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS',
      eventType: 'Login',
      interval: 'Daily',
    })
    const b = WatermarkKey.fromEntry({
      type: 'elf',
      sourceOrg: 'src',
      analyticOrg: 'ana',
      dataset: 'DS2',
      eventType: 'Login',
      interval: 'Daily',
    })
    expect(a.toString()).toBe(b.toString())
  })
})
