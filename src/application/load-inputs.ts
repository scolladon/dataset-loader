import { DateBounds } from '../domain/date-bounds.js'

// Four-count summary returned by every dispatch path (audit / dry-run /
// pipeline). Shared here so the runners and the command share a single
// result shape.
export interface DatasetLoadResult {
  readonly entriesProcessed: number
  readonly entriesSkipped: number
  readonly entriesFailed: number
  readonly groupsUploaded: number
}

export const EMPTY_RESULT: DatasetLoadResult = {
  entriesProcessed: 0,
  entriesSkipped: 0,
  entriesFailed: 0,
  groupsUploaded: 0,
}

// Fully-parsed CLI inputs for `sf dataset load`. Produced by
// `parseLoadInputs` from the raw oclif flag record and consumed by the
// dispatch switch in load.ts (audit → AuditRunner, dry-run →
// DryRunRenderer, real → PipelineRunner).
export interface LoadInputs {
  readonly configPath: string
  readonly statePath: string
  readonly audit: boolean
  readonly dryRun: boolean
  readonly entryFilter: string | undefined
  readonly bounds: DateBounds
}

// Shape of the oclif flag record for `sf dataset load`. Declared here so
// the parser is independent of the SfCommand class and can be unit-tested
// with a plain object.
export interface RawLoadFlags {
  readonly 'config-file': string
  readonly 'state-file': string
  readonly audit: boolean
  readonly 'dry-run': boolean
  readonly entry?: string
  readonly 'start-date'?: string
  readonly 'end-date'?: string
}

// Pure parser: converts the raw oclif flags into a `LoadInputs`. Throws
// the underlying `DateBounds.from` error unchanged when --start-date /
// --end-date are malformed — the caller (load.ts) is responsible for
// surfacing it through `this.error()`.
export function parseLoadInputs(flags: RawLoadFlags): LoadInputs {
  return {
    configPath: flags['config-file'],
    statePath: flags['state-file'],
    audit: flags.audit,
    dryRun: flags['dry-run'],
    entryFilter: flags.entry,
    bounds: DateBounds.from(flags['start-date'], flags['end-date']),
  }
}
