import { vi } from 'vitest'
import { type AuditEntry } from '../../src/domain/audit/audit-strategy.js'
import { type LoggerPort, type SalesforcePort } from '../../src/ports/types.js'

// Adapter: pre-existing tests build entries with `{ isElf, sourceOrg, ... }`.
// The `AuditEntry` shape (readerKind, augmentColumns, ...) is shipped for the
// schemaAlignment strategy. This helper preserves the legacy test inputs
// without rewriting every test body.
export function auditEntryOf(o: {
  isElf?: boolean
  sourceOrg: string
  targetOrg?: string
  sObject?: string
  targetDataset?: string
  readerFields?: readonly string[]
}): AuditEntry {
  return {
    readerKind: o.isElf ? 'elf' : o.sObject ? 'sobject' : 'csv',
    sourceOrg: o.sourceOrg,
    targetOrg: o.targetOrg,
    targetDataset: o.targetDataset,
    sObject: o.sObject,
    readerFields: o.sObject ? (o.readerFields ?? ['Id']) : undefined,
    augmentColumns: {},
    eventType: o.isElf ? 'EventType' : undefined,
    interval: o.isElf ? 'Daily' : undefined,
  }
}

export function createMockSfPort(
  queryResult: 'ok' | 'fail' | 'empty' = 'ok'
): SalesforcePort {
  const queryFns = {
    ok: vi.fn(async () => ({
      totalSize: 1,
      done: true,
      records: [{ Id: '001' }],
    })),
    fail: vi.fn(async () => {
      throw new Error('access denied')
    }),
    empty: vi.fn(async () => ({
      totalSize: 0,
      done: true,
      records: [],
    })),
  }
  return {
    query: queryFns[queryResult],
    queryMore: vi.fn(),
    getBlob: vi.fn(),
    getBlobStream: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    del: vi.fn(),
    apiVersion: '62.0',
  }
}

export function createMockLogger(): LoggerPort {
  return { info: vi.fn(), warn: vi.fn(), debug: vi.fn() }
}
