import {
  type AuditOutcome,
  type ReaderKind,
  type SalesforcePort,
} from '../../ports/types.js'

export interface AuditCheck {
  readonly org: string
  readonly label: string
  readonly execute: () => Promise<AuditOutcome>
}

export interface AuditEntry {
  readonly sourceOrg: string
  readonly targetOrg?: string
  readonly targetDataset?: string
  readonly readerKind: ReaderKind
  readonly sObject?: string
  readonly readerFields?: readonly string[]
  readonly augmentColumns: Readonly<Record<string, string>>
  readonly eventType?: string
  readonly interval?: string
  readonly csvFile?: string
}

export interface AuditContext {
  // Strategies bind to a single org for `evaluate`, but cross-org lookups exist
  // (e.g. schemaAlignment runs against the target org but ELF metadata lives
  // on the source org). Strategies look up the right port from this map.
  readonly sfPorts: ReadonlyMap<string, SalesforcePort>
}

interface AuditCheckStrategyBase<Payload> {
  readonly select: (
    entry: AuditEntry
  ) => readonly { org: string; key: string }[]
  readonly label: (org: string, key: string) => string
  readonly evaluate: (
    sfPort: SalesforcePort,
    key: string,
    payload: Payload,
    ctx: AuditContext
  ) => Promise<AuditOutcome>
}

// A strategy describes how one audit concern is selected, aggregated, labelled,
// and evaluated. `Payload` is the per-key data that `evaluate` receives:
// - default `Payload = AuditEntry`: the first entry seen for the dedup key
//   (today's behaviour for permission/connectivity strategies). `merge` is
//   optional in this case — its omission means "first-entry-wins".
// - custom `Payload`: `merge` is REQUIRED (the type system enforces this), so
//   the strategy always specifies how entries combine into the payload that
//   `evaluate` will receive (e.g. union of reader fields for FLS probes).
export type AuditCheckStrategy<Payload = AuditEntry> = [Payload] extends [
  AuditEntry,
]
  ? AuditCheckStrategyBase<AuditEntry> & {
      readonly merge?: (
        existing: AuditEntry | undefined,
        entry: AuditEntry
      ) => AuditEntry
    }
  : AuditCheckStrategyBase<Payload> & {
      readonly merge: (
        existing: Payload | undefined,
        entry: AuditEntry
      ) => Payload
    }

export const pass = (): AuditOutcome => ({ kind: 'pass' })
export const fail = (message: string): AuditOutcome => ({
  kind: 'fail',
  message,
})
export const warn = (message: string): AuditOutcome => ({
  kind: 'warn',
  message,
})

// Shared selector for dataset-scoped strategies — emits one check per
// (targetOrg, targetDataset) pair, skipping entries without either.
// targetDataset values are validated against SF_IDENTIFIER_PATTERN at config
// parse boundary.
export const selectByDataset: AuditCheckStrategy['select'] = e =>
  e.targetOrg && e.targetDataset
    ? [{ org: e.targetOrg, key: e.targetDataset }]
    : []
