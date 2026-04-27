import {
  type ConfigEntry,
  isCsvEntry,
  isElfEntry,
  type ResolvedEntry,
} from '../adapters/config-loader.js'
import { type AuditEntry } from '../domain/audit/audit-strategy.js'
import { buildAuditChecks, runAudit } from '../domain/audit/runner.js'
import { type LoggerPort, type SalesforcePort } from '../ports/types.js'
import { type DatasetLoadResult, EMPTY_RESULT } from './load-inputs.js'

// Runs the `--audit` dispatch: maps resolved entries to AuditEntry shapes,
// executes the audit domain service, and sets the process exit code when
// any check fails. Emits all output through the injected LoggerPort; the
// caller is responsible for inspecting the returned result for reporting.
export class AuditRunner {
  constructor(private readonly logger: LoggerPort) {}

  async run(
    entries: readonly ResolvedEntry[],
    sfPorts: ReadonlyMap<string, SalesforcePort>
  ): Promise<DatasetLoadResult> {
    const auditEntries = entries.map(({ entry, augmentColumns }) =>
      this.buildAuditEntry(entry, augmentColumns)
    )
    this.logger.info('Audit — pre-flight checks:')
    const checks = buildAuditChecks(auditEntries, sfPorts)
    const auditResult = await runAudit(checks, this.logger)
    if (!auditResult.passed) process.exitCode = 2
    // `runAudit` returns a pass/fail boolean rather than a per-entry count,
    // so `entriesFailed` here is a 0/1 presence flag rather than the real
    // number of failing entries. Preserved as the command's existing shape.
    return {
      ...EMPTY_RESULT,
      entriesFailed: auditResult.passed ? 0 : 1,
    }
  }

  private buildAuditEntry(
    entry: ConfigEntry,
    augmentColumns: Readonly<Record<string, string>>
  ): AuditEntry {
    if (isCsvEntry(entry)) {
      return {
        readerKind: 'csv',
        sourceOrg: '<csv>',
        targetOrg: entry.targetOrg,
        targetDataset: entry.targetDataset,
        augmentColumns,
        csvFile: entry.csvFile,
      }
    }
    if (isElfEntry(entry)) {
      return {
        readerKind: 'elf',
        sourceOrg: entry.sourceOrg,
        targetOrg: entry.targetOrg,
        targetDataset: entry.targetDataset,
        augmentColumns,
        eventType: entry.eventLog,
        interval: entry.interval,
      }
    }
    // After CSV and ELF, the ConfigEntry union has narrowed to SObjectEntry.
    return {
      readerKind: 'sobject',
      sourceOrg: entry.sourceOrg,
      targetOrg: entry.targetOrg,
      targetDataset: entry.targetDataset,
      augmentColumns,
      sObject: entry.sObject,
      readerFields: entry.fields,
    }
  }
}
