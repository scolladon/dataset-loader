import { Messages } from '@salesforce/core'
import { beforeAll, describe, expect, it } from 'vitest'
import { loadDatasetLoadMessages } from '../../../src/ports/messages.js'

describe('MessagesPort (SfMessages adapter)', () => {
  beforeAll(() => {
    Messages.importMessagesDirectoryFromMetaUrl(
      new URL('../../../package.json', import.meta.url).toString()
    )
  })

  it('given dataset.load bundle, when getSummary, then returns non-empty string', () => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getSummary().length).toBeGreaterThan(0)
  })

  it('given dataset.load bundle, when getExamples, then returns at least one example', () => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getExamples().length).toBeGreaterThan(0)
  })

  it('given flag key start-date, when getFlagSummary, then returns a summary mentioning ISO-8601', () => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getFlagSummary('start-date')).toContain('ISO-8601')
  })

  it('given error key config-load-failed with a substitution, when getError, then interpolates %s', () => {
    const sut = loadDatasetLoadMessages()
    const msg = sut.getError('config-load-failed', 'bad path')
    expect(msg).toContain('bad path')
    expect(msg).not.toContain('%s')
  })

  it('given error key entry-not-found with entry name, when getError, then surfaces the entry name', () => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getError('entry-not-found', 'my-entry')).toContain('my-entry')
  })

  it('given error key no-target-port with org name, when getError, then surfaces the org name', () => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getError('no-target-port', 'prod-org')).toContain('prod-org')
  })
})
