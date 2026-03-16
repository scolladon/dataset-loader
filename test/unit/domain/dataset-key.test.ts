import { describe, expect, it } from 'vitest'
import { DatasetKey } from '../../../src/domain/dataset-key.js'

describe('DatasetKey', () => {
  describe('Given file target (no targetOrg)', () => {
    describe('When creating from entry with targetFile', () => {
      it('Then toString returns file-namespaced key', () => {
        const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
        expect(sut.toString()).toBe('file:./output/login.csv')
      })

      it('Then name returns the file path', () => {
        const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
        expect(sut.name).toBe('./output/login.csv')
      })

      it('Then org returns undefined', () => {
        const sut = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
        expect(sut.org).toBeUndefined()
      })

      it('Then two keys with same path have equal toString()', () => {
        const key1 = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
        const key2 = DatasetKey.fromEntry({ targetFile: './output/login.csv' })
        expect(key1.toString()).toBe(key2.toString())
      })

      it('Then fromEntry throws when targetOrg is empty string', () => {
        expect(() =>
          DatasetKey.fromEntry({ targetOrg: '', targetDataset: 'LoginEvents' })
        ).toThrow('targetOrg must not be empty')
      })
    })
  })

  describe('Given org target (targetOrg present)', () => {
    describe('When creating from entry with targetOrg and targetDataset', () => {
      it('Then toString returns org-namespaced key', () => {
        const sut = DatasetKey.fromEntry({
          targetOrg: 'my-org',
          targetDataset: 'LoginEvents',
        })
        expect(sut.toString()).toBe('org:my-org:LoginEvents')
      })

      it('Then org returns the targetOrg', () => {
        const sut = DatasetKey.fromEntry({
          targetOrg: 'ana',
          targetDataset: 'MyDS',
        })
        expect(sut.org).toBe('ana')
      })

      it('Then name returns the targetDataset', () => {
        const sut = DatasetKey.fromEntry({
          targetOrg: 'ana',
          targetDataset: 'MyDS',
        })
        expect(sut.name).toBe('MyDS')
      })

      it('Given two keys with same values, when comparing toString, then they are equal', () => {
        const a = DatasetKey.fromEntry({
          targetOrg: 'ana',
          targetDataset: 'DS',
        })
        const b = DatasetKey.fromEntry({
          targetOrg: 'ana',
          targetDataset: 'DS',
        })
        expect(a.toString()).toBe(b.toString())
      })
    })
  })
})
