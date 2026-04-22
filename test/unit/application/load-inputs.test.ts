import { describe, expect, it } from 'vitest'
import {
  EMPTY_RESULT,
  parseLoadInputs,
  type RawLoadFlags,
} from '../../../src/application/load-inputs.js'

const ISO_JAN = '2026-01-01T00:00:00.000Z'
const ISO_MAR = '2026-03-01T00:00:00.000Z'

const minimalFlags: RawLoadFlags = {
  'config-file': 'cfg.json',
  'state-file': '.state.json',
  audit: false,
  'dry-run': false,
}

describe('parseLoadInputs', () => {
  it('given minimal flags, when parsing, then all fields populate with empty bounds', () => {
    const sut = parseLoadInputs(minimalFlags)
    expect(sut.configPath).toBe('cfg.json')
    expect(sut.statePath).toBe('.state.json')
    expect(sut.audit).toBe(false)
    expect(sut.dryRun).toBe(false)
    expect(sut.entryFilter).toBeUndefined()
    expect(sut.bounds.isEmpty()).toBe(true)
  })

  it('given audit flag, when parsing, then audit is true', () => {
    expect(parseLoadInputs({ ...minimalFlags, audit: true }).audit).toBe(true)
  })

  it('given dry-run flag, when parsing, then dryRun is true', () => {
    expect(parseLoadInputs({ ...minimalFlags, 'dry-run': true }).dryRun).toBe(
      true
    )
  })

  it('given entry filter, when parsing, then entryFilter surfaces', () => {
    expect(
      parseLoadInputs({ ...minimalFlags, entry: 'logins' }).entryFilter
    ).toBe('logins')
  })

  it('given valid --start-date and --end-date, when parsing, then bounds are populated', () => {
    const sut = parseLoadInputs({
      ...minimalFlags,
      'start-date': ISO_JAN,
      'end-date': ISO_MAR,
    })
    expect(sut.bounds.isEmpty()).toBe(false)
    expect(sut.bounds.hasStart()).toBe(true)
    expect(sut.bounds.hasEnd()).toBe(true)
  })

  it('given malformed --start-date, when parsing, then propagates the DateBounds error', () => {
    expect(() =>
      parseLoadInputs({ ...minimalFlags, 'start-date': 'not-a-date' })
    ).toThrow(/--start-date.*not-a-date/)
  })

  it('given --start-date > --end-date, when parsing, then propagates the ordering error', () => {
    expect(() =>
      parseLoadInputs({
        ...minimalFlags,
        'start-date': ISO_MAR,
        'end-date': ISO_JAN,
      })
    ).toThrow(/must be <=/)
  })
})

describe('EMPTY_RESULT', () => {
  it('given the shared empty result, when read, then all counts are zero', () => {
    expect(EMPTY_RESULT).toEqual({
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: 0,
      groupsUploaded: 0,
    })
  })
})
