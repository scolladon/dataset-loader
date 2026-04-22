import {
  type CsvEntry,
  type ElfEntry,
  type ResolvedEntry,
  type SObjectEntry,
} from '../../src/adapters/config-loader.js'
import { type LoggerPort } from '../../src/ports/types.js'

// Shared fixtures for tests of modules under src/application/. Keeps the
// three runner test files (audit-runner, dry-run-renderer, warnings) from
// redefining the same logger harness and entry literals.

export const sobjectEntry: SObjectEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  sObject: 'Account',
  fields: ['Id'],
  dateField: 'LastModifiedDate',
  name: 'accounts',
}

export const sobjectEntryOverwrite: SObjectEntry = {
  ...sobjectEntry,
  operation: 'Overwrite',
}

export const elfEntry: ElfEntry = {
  sourceOrg: 'src',
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  eventLog: 'Login',
  interval: 'Daily',
  name: 'logins',
}

export const csvEntry: CsvEntry = {
  targetOrg: 'ana',
  targetDataset: 'DS',
  operation: 'Append',
  csvFile: './fake.csv',
  name: 'csv-only',
}

export function resolvedOf(
  entry: SObjectEntry | ElfEntry | CsvEntry,
  index = 0
): ResolvedEntry {
  return { entry, index, augmentColumns: {} }
}

// In-memory LoggerPort that captures every info/warn call. `debug` is a no-op
// because no code path under review emits debug messages — keeps the fixture
// tight without changing the observable semantics.
export function makeCaptureLogger(): {
  logger: LoggerPort
  logs: string[]
  warns: string[]
} {
  const logs: string[] = []
  const warns: string[] = []
  const logger: LoggerPort = {
    info: (m: string) => logs.push(m),
    warn: (m: string) => warns.push(m),
    debug: (_m: string) => {
      /* no-op: no production code under test emits debug */
    },
  }
  return { logger, logs, warns }
}
