import {
  type ConfigEntry,
  isCsvEntry,
  isElfEntry,
  type ResolvedEntry,
} from '../adapters/config-loader.js'
import { CsvBootstrapProvider } from '../adapters/readers/csv-bootstrap-provider.js'
import { ElfBootstrapProvider } from '../adapters/readers/elf-bootstrap-provider.js'
import { SObjectBootstrapProvider } from '../adapters/readers/sobject-bootstrap-provider.js'
import { type AuditEntry } from '../domain/audit/audit-strategy.js'
import { buildAuditChecks, runAudit } from '../domain/audit/runner.js'
import {
  type BootstrapMetadataProvider,
  type LoggerPort,
  type SalesforcePort,
} from '../ports/types.js'
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
    const checks = buildAuditChecks(
      auditEntries,
      sfPorts,
      buildBootstrapProviderFor(sfPorts)
    )
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
        overrideMetadata: entry.overrideMetadata,
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
        overrideMetadata: entry.overrideMetadata,
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
      overrideMetadata: entry.overrideMetadata,
    }
  }
}

// Adapter-aware factory that the domain audit consumes via AuditContext.
// Lives in the application layer because domain code cannot reach adapter
// constructors. The audit only ever calls this on entries that have a target
// dataset (selectByDataset filters out entries without one), so the returned
// providers are always meaningful.
function buildBootstrapProviderFor(
  sfPorts: ReadonlyMap<string, SalesforcePort>
): (entry: AuditEntry) => BootstrapMetadataProvider {
  return (entry: AuditEntry): BootstrapMetadataProvider => {
    // The `??` fallbacks satisfy the AuditEntry type (kind-fields are all
    // optional on the union). `buildAuditEntry` always sets the field
    // appropriate to the entry's `readerKind`, so the fallback branches are
    // unreachable in practice.
    /* v8 ignore next */
    const datasetName = entry.targetDataset ?? ''
    if (entry.readerKind === 'csv') {
      return new CsvBootstrapProvider({
        /* v8 ignore next */
        filePath: entry.csvFile ?? '',
        datasetName,
        augmentColumns: entry.augmentColumns,
      })
    }
    const srcPort = sfPorts.get(entry.sourceOrg)
    /* v8 ignore next 4 — defensive: authConnectivity audit rejects entries
       lacking a source port before schemaAlignment ever runs */
    if (!srcPort) {
      throw new Error(
        `No source SalesforcePort for org '${entry.sourceOrg}' — cannot build bootstrap provider`
      )
    }
    if (entry.readerKind === 'elf') {
      return new ElfBootstrapProvider(srcPort, {
        /* v8 ignore next */
        eventType: entry.eventType ?? '',
        /* v8 ignore next */
        interval: (entry.interval ?? 'Daily') as 'Daily' | 'Hourly',
        datasetName,
        augmentColumns: entry.augmentColumns,
      })
    }
    return new SObjectBootstrapProvider(srcPort, {
      /* v8 ignore next */
      sobject: entry.sObject ?? '',
      /* v8 ignore next */
      fields: entry.readerFields ?? [],
      datasetName,
      augmentColumns: entry.augmentColumns,
    })
  }
}
