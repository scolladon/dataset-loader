import { Messages } from '@salesforce/core'

// Typed accessor over the oclif / @salesforce/core Messages bundle. Keeps the
// runtime modules (flags parser, pipeline runner, dry-run renderer, etc.) from
// depending directly on `Messages.loadMessages` — they receive a `MessagesPort`
// via constructor injection instead. This makes them unit-testable against a
// fake bundle and centralises the set of message keys the command knows about.

export interface MessagesPort {
  getSummary(): string
  getExamples(): string[]
  getFlagSummary(flag: FlagKey): string
  getError(key: ErrorKey, ...substitutions: readonly string[]): string
}

export type FlagKey =
  | 'config-file'
  | 'state-file'
  | 'audit'
  | 'dry-run'
  | 'entry'
  | 'start-date'
  | 'end-date'

export type ErrorKey =
  | 'config-load-failed'
  | 'entry-not-found'
  | 'entry-not-found.hint-missing-names'
  | 'no-source-port'
  | 'no-target-port'
  | 'unknown-entry-kind'

// Resolve the dataset-loader messages bundle. The caller is expected to have
// already invoked `Messages.importMessagesDirectoryFromMetaUrl(import.meta.url)`
// once at process start (the command module handles that).
export function loadDatasetLoadMessages(): MessagesPort {
  const bundle = Messages.loadMessages('dataset-loader', 'dataset.load')
  return {
    getSummary: () => bundle.getMessage('summary'),
    getExamples: () => bundle.getMessages('examples'),
    getFlagSummary: flag => bundle.getMessage(`flags.${flag}.summary`),
    getError: (key, ...substitutions) =>
      bundle.getMessage(`errors.${key}`, [...substitutions]),
  }
}
