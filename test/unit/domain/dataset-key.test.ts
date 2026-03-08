import { describe, expect, it } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'

describe('DatasetKey', () => {
  it('given entry, when creating key, then combines analyticOrg and dataset', () => {
    const sut = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'MyDS' })
    expect(sut.toString()).toBe('ana:MyDS')
  })

  it('given key, when accessing org, then returns analyticOrg', () => {
    const sut = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'MyDS' })
    expect(sut.org).toBe('ana')
  })

  it('given key, when accessing name, then returns dataset', () => {
    const sut = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'MyDS' })
    expect(sut.name).toBe('MyDS')
  })

  it('given two keys with same values, when comparing toString, then they are equal', () => {
    const a = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS' })
    const b = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS' })
    expect(a.toString()).toBe(b.toString())
  })
})
