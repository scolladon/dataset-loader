import { Messages } from '@salesforce/core'
import { beforeAll, describe, expect, it } from 'vitest'
import {
  type ErrorKey,
  type FlagKey,
  loadDatasetLoadMessages,
} from '../../../src/ports/messages.js'

describe('MessagesPort (SfMessages adapter)', () => {
  beforeAll(() => {
    Messages.importMessagesDirectoryFromMetaUrl(
      new URL('../../../package.json', import.meta.url).toString()
    )
  })

  it('given dataset.load bundle, when getSummary, then returns the command description', () => {
    // Assert — matches the exact summary in messages/dataset.load.md. Kills
    // mutations that swap the bundle key (e.g. returning the flag summary).
    const sut = loadDatasetLoadMessages()
    expect(sut.getSummary()).toBe(
      'Load Event Log Files and SObject data into CRM Analytics datasets'
    )
  })

  it('given dataset.load bundle, when getExamples, then returns 3 example invocations', () => {
    // Assert — exact count and a distinctive substring on each example so
    // mutations to the example list are detected.
    const sut = loadDatasetLoadMessages()
    const examples = sut.getExamples()
    expect(examples).toHaveLength(3)
    expect(examples[0]).toContain('<%= config.bin %>')
    expect(examples[1]).toContain('--dry-run')
    expect(examples[2]).toContain('--start-date')
  })

  // Parameterized coverage of every FlagKey. A mutation swapping the key
  // prefix in `getFlagSummary(flag) => bundle.getMessage(\`flags.${flag}.summary\`)`
  // would fail for at least one key here.
  it.each<[FlagKey, string]>([
    ['config-file', 'config'],
    ['state-file', 'watermark'],
    ['audit', 'Pre-flight'],
    ['dry-run', 'plan'],
    ['entry', 'entry'],
    ['start-date', 'ISO-8601'],
    ['end-date', 'ISO-8601'],
  ])('given flag key %s, when getFlagSummary, then surfaces a distinctive substring', (key, marker) => {
    const sut = loadDatasetLoadMessages()
    expect(sut.getFlagSummary(key)).toContain(marker)
  })

  // Parameterized coverage of every ErrorKey. Validates both the bundle
  // lookup and the %s substitution path.
  it.each<[ErrorKey, readonly string[], readonly string[]]>([
    ['config-load-failed', ['bad path'], ['bad path', 'loading failed']],
    ['entry-not-found', ['my-entry'], ["'my-entry'", 'not found']],
    ['entry-not-found.hint-missing-names', [], ['"name" field']],
    ['no-source-port', ['prod'], ["'prod'", 'No SF connection']],
    ['no-target-port', ['analytic'], ["'analytic'", 'target org']],
  ])('given error key %s, when getError, then renders the template with substitutions', (key, subs, markers) => {
    const sut = loadDatasetLoadMessages()
    const msg = sut.getError(key, ...subs)
    for (const marker of markers) {
      expect(msg).toContain(marker)
    }
    // No un-interpolated placeholders remain.
    expect(msg).not.toContain('%s')
  })
})
