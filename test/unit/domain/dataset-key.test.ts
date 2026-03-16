import { describe, expect, it } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'

describe('DatasetKey', () => {
  describe('Given file target (no analyticOrg)', () => {
    describe('When creating from entry without analyticOrg', () => {
      it('Then toString returns file-namespaced key', () => {
        const sut = DatasetKey.fromEntry({ dataset: './output/login.csv' })
        expect(sut.toString()).toBe('file:./output/login.csv')
      })

      it('Then name returns the file path', () => {
        const sut = DatasetKey.fromEntry({ dataset: './output/login.csv' })
        expect(sut.name).toBe('./output/login.csv')
      })

      it('Then org returns undefined', () => {
        const sut = DatasetKey.fromEntry({ dataset: './output/login.csv' })
        expect(sut.org).toBeUndefined()
      })

      it('Then two keys with same path have equal toString()', () => {
        const key1 = DatasetKey.fromEntry({ dataset: './output/login.csv' })
        const key2 = DatasetKey.fromEntry({ dataset: './output/login.csv' })
        expect(key1.toString()).toBe(key2.toString())
      })

      it('Then fromEntry throws when analyticOrg is empty string', () => {
        expect(() =>
          DatasetKey.fromEntry({ analyticOrg: '', dataset: 'LoginEvents' })
        ).toThrow('analyticOrg must not be empty')
      })
    })
  })

  describe('Given org target (analyticOrg present)', () => {
    describe('When creating from entry with analyticOrg', () => {
      it('Then toString returns org-namespaced key', () => {
        const sut = DatasetKey.fromEntry({
          analyticOrg: 'my-org',
          dataset: 'LoginEvents',
        })
        expect(sut.toString()).toBe('org:my-org:LoginEvents')
      })

      it('Then org returns the analyticOrg', () => {
        const sut = DatasetKey.fromEntry({
          analyticOrg: 'ana',
          dataset: 'MyDS',
        })
        expect(sut.org).toBe('ana')
      })

      it('Then name returns the dataset', () => {
        const sut = DatasetKey.fromEntry({
          analyticOrg: 'ana',
          dataset: 'MyDS',
        })
        expect(sut.name).toBe('MyDS')
      })

      it('Given two keys with same values, when comparing toString, then they are equal', () => {
        const a = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS' })
        const b = DatasetKey.fromEntry({ analyticOrg: 'ana', dataset: 'DS' })
        expect(a.toString()).toBe(b.toString())
      })
    })
  })
})
