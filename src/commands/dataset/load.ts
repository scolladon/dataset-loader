import { Messages, Org } from '@salesforce/core'
import { Flags, SfCommand } from '@salesforce/sf-plugins-core'
import {
  type ConfigEntry,
  entryLabel,
  isCsvEntry,
  isElfEntry,
  isSObjectEntry,
  parseConfig,
  type ResolvedEntry,
  resolveConfig,
} from '../../adapters/config-loader.js'
import { buildAugmentHeaderSuffix } from '../../adapters/pipeline/augment-transform.js'
import { ProgressReporter } from '../../adapters/progress-reporter.js'
import { CsvReader } from '../../adapters/readers/csv-reader.js'
import { ElfReader } from '../../adapters/readers/elf-reader.js'
import { SObjectReader } from '../../adapters/readers/sobject-reader.js'
import { SalesforceClient } from '../../adapters/sf-client.js'
import { FileStateManager } from '../../adapters/state-manager.js'
import { DatasetWriterFactory } from '../../adapters/writers/dataset-writer.js'
import { FileWriterFactory } from '../../adapters/writers/file-writer.js'
import {
  type AuditEntry,
  buildAuditChecks,
  runAudit,
} from '../../domain/auditor.js'
import { DatasetKey } from '../../domain/dataset-key.js'
import { DateBounds } from '../../domain/date-bounds.js'
import { resolveProvidedFields } from '../../domain/field-resolver.js'
import { executePipeline, type PipelineEntry } from '../../domain/pipeline.js'
import { ReaderKey } from '../../domain/reader-key.js'
import { WatermarkKey } from '../../domain/watermark-key.js'
import { type WatermarkStore } from '../../domain/watermark-store.js'
import {
  loadDatasetLoadMessages,
  type MessagesPort,
} from '../../ports/messages.js'
import {
  type AlignmentSpec,
  type CreateWriterPort,
  formatErrorMessage,
  type LoggerPort,
  type ReaderPort,
  type SalesforcePort,
  type StatePort,
} from '../../ports/types.js'
import { computeWarnings, dryRunAnnotation } from './warnings.js'

Messages.importMessagesDirectoryFromMetaUrl(import.meta.url)
const messagesPort: MessagesPort = loadDatasetLoadMessages()

interface DatasetLoadResult {
  entriesProcessed: number
  entriesSkipped: number
  entriesFailed: number
  groupsUploaded: number
}

// Output of the synchronous first pass of pipeline-entry construction.
// The second (async) pass enriches this with an AlignmentSpec before
// handing it to the pipeline as a full PipelineEntry.
interface PipelineEntrySlot {
  readonly resolvedEntry: ResolvedEntry
  readonly index: number
  readonly readerKey: ReaderKey
  readonly fetcher: ReaderPort
  readonly augmentColumns: Record<string, string>
}

export default class DatasetLoad extends SfCommand<DatasetLoadResult> {
  public static readonly summary = messagesPort.getSummary()
  public static readonly examples = messagesPort.getExamples()

  public static readonly flags = {
    'config-file': Flags.file({
      char: 'c',
      summary: messagesPort.getFlagSummary('config-file'),
      default: 'dataset-load.config.json',
    }),
    'state-file': Flags.file({
      char: 's',
      summary: messagesPort.getFlagSummary('state-file'),
      default: '.dataset-load.state.json',
    }),
    audit: Flags.boolean({
      summary: messagesPort.getFlagSummary('audit'),
      default: false,
    }),
    'dry-run': Flags.boolean({
      summary: messagesPort.getFlagSummary('dry-run'),
      default: false,
    }),
    entry: Flags.string({
      summary: messagesPort.getFlagSummary('entry'),
    }),
    'start-date': Flags.string({
      summary: messagesPort.getFlagSummary('start-date'),
    }),
    'end-date': Flags.string({
      summary: messagesPort.getFlagSummary('end-date'),
    }),
  }

  public async run(): Promise<DatasetLoadResult> {
    const { flags } = await this.parse(DatasetLoad)
    const sfPorts = new Map<string, SalesforcePort>()
    const logger = this.createLogger()

    const bounds = this.parseBounds(flags['start-date'], flags['end-date'])

    const resolvedEntries = await this.loadAndResolveConfig(
      flags['config-file'],
      sfPorts
    )

    const filtered = this.filterByEntry(resolvedEntries, flags['entry'])

    if (flags['audit']) return this.handleAudit(filtered, sfPorts, logger)

    const state = new FileStateManager(flags['state-file'])
    const watermarks = await state.read()

    if (flags['dry-run']) return this.handleDryRun(filtered, watermarks, bounds)

    return this.handlePipeline(
      filtered,
      sfPorts,
      watermarks,
      state,
      logger,
      bounds
    )
  }

  private parseBounds(
    start: string | undefined,
    end: string | undefined
  ): DateBounds {
    try {
      return DateBounds.from(start, end)
    } catch (err) {
      // this.error is `never`-returning but TypeScript doesn't always infer
      // that in all code paths, so we also throw to make the control flow
      // explicit for the caller.
      this.error(formatErrorMessage(err))
    }
  }

  private createLogger(): LoggerPort {
    return {
      info: (msg: string) => this.log(msg),
      warn: (msg: string) => this.warn(msg),
      debug: (msg: string) => this.debug(msg),
    }
  }

  private async ensureSfPort(
    orgAlias: string,
    sfPorts: Map<string, SalesforcePort>
  ): Promise<void> {
    if (sfPorts.has(orgAlias)) return
    const org = await Org.create({ aliasOrUsername: orgAlias })
    sfPorts.set(orgAlias, new SalesforceClient(org.getConnection()))
  }

  private async loadAndResolveConfig(
    configPath: string,
    sfPorts: Map<string, SalesforcePort>
  ): Promise<ResolvedEntry[]> {
    try {
      const config = await parseConfig(configPath)
      const allOrgs = new Set<string>()
      for (const e of config.entries) {
        if (!isCsvEntry(e)) allOrgs.add(e.sourceOrg)
        if (e.targetOrg) allOrgs.add(e.targetOrg)
      }
      const ensurePromises: Promise<void>[] = []
      for (const alias of allOrgs) {
        ensurePromises.push(this.ensureSfPort(alias, sfPorts))
      }
      await Promise.all(ensurePromises)
      return await resolveConfig(config, sfPorts)
    } catch (error) {
      this.error(
        messagesPort.getError('config-load-failed', formatErrorMessage(error))
      )
    }
  }

  private filterByEntry(
    entries: ResolvedEntry[],
    entryName: string | undefined
  ): ResolvedEntry[] {
    if (entryName === undefined) return entries

    const hasAnyName = entries.some(e => e.entry.name)
    const filtered: ResolvedEntry[] = []
    for (const e of entries) {
      if (e.entry.name === entryName) filtered.push(e)
    }
    if (filtered.length === 0) {
      const hint = hasAnyName
        ? ''
        : ` ${messagesPort.getError('entry-not-found.hint-missing-names')}`
      this.error(
        `${messagesPort.getError('entry-not-found', entryName)}${hint}`
      )
    }
    return filtered
  }

  private async handleAudit(
    entries: ResolvedEntry[],
    sfPorts: Map<string, SalesforcePort>,
    logger: LoggerPort
  ): Promise<DatasetLoadResult> {
    const auditEntries = entries.map(({ entry, augmentColumns }) =>
      this.buildAuditEntry(entry, augmentColumns)
    )
    logger.info('Audit — pre-flight checks:')
    const checks = buildAuditChecks(auditEntries, sfPorts)
    const auditResult = await runAudit(checks, logger)
    if (!auditResult.passed) process.exitCode = 2
    return {
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: auditResult.passed ? 0 : 1,
      groupsUploaded: 0,
    }
  }

  private buildAuditEntry(
    entry: ConfigEntry,
    augmentColumns: Record<string, string>
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
    if (isSObjectEntry(entry)) {
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
    /* v8 ignore next 2 -- exhaustive discriminator; unreachable */
    throw new Error(messagesPort.getError('unknown-entry-kind'))
  }

  private handleDryRun(
    entries: ResolvedEntry[],
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): DatasetLoadResult {
    for (const msg of computeWarnings(entries, watermarks, bounds)) {
      this.warn(msg)
    }
    this.log('Dry run — planned entries:')
    if (bounds.isEmpty()) {
      for (const { entry } of entries) {
        const wk = WatermarkKey.fromEntry(entry)
        const wm = watermarks.get(wk)?.toString() ?? '(none)'
        const dk = DatasetKey.fromEntry(entry)
        this.log(`  ${entryLabel(entry)} → ${dk.toString()} (watermark: ${wm})`)
      }
      return {
        entriesProcessed: 0,
        entriesSkipped: 0,
        entriesFailed: 0,
        groupsUploaded: 0,
      }
    }
    this.log(`Configured window: ${bounds.toString()}`)
    this.log('')
    for (const { entry } of entries) {
      this.renderDryRunEntry(entry, watermarks, bounds)
    }
    return {
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: 0,
      groupsUploaded: 0,
    }
  }

  private renderDryRunEntry(
    entry: ConfigEntry,
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): void {
    const dk = DatasetKey.fromEntry(entry)
    this.log(`  ${entryLabel(entry)} → ${dk.toString()}`)
    if (isCsvEntry(entry)) {
      this.log('    watermark: n/a (CSV entry — bounds do not apply)')
      return
    }
    const wm = watermarks.get(WatermarkKey.fromEntry(entry))
    this.log(`    watermark: ${wm?.toString() ?? '(none)'}`)
    const dateField = isElfEntry(entry) ? 'LogDate' : entry.dateField
    const lower = bounds.lowerConditionFor(dateField, wm)
    const upper = bounds.upperConditionFor(dateField)
    const conds = [lower, upper].filter(Boolean)
    /* v8 ignore next -- unreachable: this render path is only called from
       handleDryRun when `!bounds.isEmpty()`, which means --start-date or
       --end-date is set. --start-date set → lower = `dateField >= start`
       (defined); --end-date set → upper = `dateField <= end` (defined).
       At least one is always present, so conds.length >= 1. */
    // Stryker disable next-line ConditionalExpression: equivalent mutant —
    // this branch is structurally unreachable (see v8 ignore rationale above).
    if (conds.length === 0) return
    const soql = conds.join(' AND ')
    const annotation = dryRunAnnotation(entry, wm, bounds)
    this.log(`    effective: ${soql}${annotation}`)
  }

  private async handlePipeline(
    entries: ResolvedEntry[],
    sfPorts: Map<string, SalesforcePort>,
    watermarks: WatermarkStore,
    state: StatePort,
    logger: LoggerPort,
    bounds: DateBounds
  ): Promise<DatasetLoadResult> {
    for (const msg of computeWarnings(entries, watermarks, bounds)) {
      this.warn(msg)
    }
    // Two-pass entry build: sync dedupe of readers (avoids Map races under
    // concurrent awaits), then async resolution of providedFields per entry.
    const sharedReaders = new Map<string, ReaderPort>()
    const pass1 = entries.map(resolvedEntry =>
      this.buildPipelineEntryStatic(
        resolvedEntry,
        sfPorts,
        sharedReaders,
        bounds
      )
    )
    const pipelineEntries: PipelineEntry[] = await Promise.all(
      pass1.map(slot => this.resolveAlignment(slot, sfPorts))
    )

    const result = await executePipeline({
      entries: pipelineEntries,
      watermarks,
      createWriter: this.createWriterFactory(sfPorts),
      state,
      progress: new ProgressReporter(),
      logger,
    })

    this.log(
      `Done: ${result.entriesProcessed} processed, ${result.entriesSkipped} skipped, ${result.entriesFailed} failed, ${result.groupsUploaded} groups uploaded`
    )
    if (result.exitCode > 0) process.exitCode = result.exitCode

    return {
      entriesProcessed: result.entriesProcessed,
      entriesSkipped: result.entriesSkipped,
      entriesFailed: result.entriesFailed,
      groupsUploaded: result.groupsUploaded,
    }
  }

  // Pass 1 — synchronous: derive the reader key, dedup the fetcher, and
  // collect the entry data. No async work: mutating sharedReaders is safe
  // because the caller does not await between iterations.
  private buildPipelineEntryStatic(
    resolvedEntry: ResolvedEntry,
    sfPorts: Map<string, SalesforcePort>,
    sharedReaders: Map<string, ReaderPort>,
    bounds: DateBounds
  ): PipelineEntrySlot {
    const { entry, index, augmentColumns } = resolvedEntry

    if (isCsvEntry(entry)) {
      const readerKey = ReaderKey.forCsv(entry.csvFile)
      const cacheKey = readerKey.toString()
      const existing = sharedReaders.get(cacheKey)
      const fetcher: ReaderPort = existing ?? new CsvReader(entry.csvFile)
      if (!existing) sharedReaders.set(cacheKey, fetcher)
      return { resolvedEntry, index, readerKey, fetcher, augmentColumns }
    }

    const srcPort = sfPorts.get(entry.sourceOrg)
    if (!srcPort)
      throw new Error(messagesPort.getError('no-source-port', entry.sourceOrg))
    const readerKey = this.createReaderKey(entry, bounds)
    const readerCacheKey = readerKey.toString()
    const fetcher = this.getOrCreateReader(
      readerCacheKey,
      sharedReaders,
      entry,
      srcPort,
      bounds
    )
    return { resolvedEntry, index, readerKey, fetcher, augmentColumns }
  }

  // Pass 2 — async: resolve the source field list (ELF: LogFileFieldNames
  // query; CSV: first-line read) and build the AlignmentSpec. Runs in
  // parallel across entries; no shared mutable state touched.
  private async resolveAlignment(
    slot: PipelineEntrySlot,
    sfPorts: Map<string, SalesforcePort>
  ): Promise<PipelineEntry> {
    const { resolvedEntry, index, readerKey, fetcher, augmentColumns } = slot
    const { entry } = resolvedEntry
    const providedFields = await resolveProvidedFields(entry, fetcher, sfPorts)
    const readerKind = isCsvEntry(entry)
      ? ('csv' as const)
      : isElfEntry(entry)
        ? ('elf' as const)
        : ('sobject' as const)
    const label = entryLabel(entry)
    const alignment: AlignmentSpec = {
      readerKind,
      entryLabel: label,
      providedFields,
      augmentColumns,
    }
    return {
      index,
      label,
      readerKey,
      watermarkKey: WatermarkKey.fromEntry(entry),
      datasetKey: DatasetKey.fromEntry(entry),
      operation: entry.operation,
      augmentColumns,
      fetcher,
      alignment,
      header: async () =>
        (await fetcher.header()) + buildAugmentHeaderSuffix(augmentColumns),
    }
  }

  // Entries sharing the same readerKey share one ReaderPort instance so that header() returns
  // a valid value for all of them after the shared fetch() call.
  private getOrCreateReader(
    cacheKey: string,
    cache: Map<string, ReaderPort>,
    entry: ConfigEntry,
    srcPort: SalesforcePort,
    bounds: DateBounds
  ): ReaderPort {
    const existing = cache.get(cacheKey)
    if (existing) return existing
    const reader = this.createFetcher(entry, srcPort, bounds)
    cache.set(cacheKey, reader)
    return reader
  }

  private createReaderKey(entry: ConfigEntry, bounds: DateBounds): ReaderKey {
    if (isElfEntry(entry)) {
      return ReaderKey.forElf(
        entry.sourceOrg,
        entry.eventLog,
        entry.interval,
        bounds
      )
    }
    if (isCsvEntry(entry)) {
      return ReaderKey.forCsv(entry.csvFile)
    }
    return ReaderKey.forSObject(
      entry.sourceOrg,
      entry.sObject,
      entry.fields,
      entry.dateField,
      entry.where,
      entry.limit,
      bounds
    )
  }

  private createFetcher(
    entry: ConfigEntry,
    sfPort: SalesforcePort,
    bounds: DateBounds
  ): ReaderPort {
    if (isCsvEntry(entry)) {
      return new CsvReader(entry.csvFile)
    }
    if (isElfEntry(entry)) {
      return new ElfReader(sfPort, entry.eventLog, entry.interval, bounds)
    }
    return new SObjectReader(sfPort, {
      sobject: entry.sObject,
      fields: entry.fields,
      dateField: entry.dateField,
      where: entry.where,
      queryLimit: entry.limit,
      bounds,
    })
  }

  private createWriterFactory(
    sfPorts: Map<string, SalesforcePort>
  ): CreateWriterPort {
    return {
      create(dataset, operation, listener, headerProvider, alignment) {
        if (dataset.org) {
          const sfPort = sfPorts.get(dataset.org)
          if (!sfPort) {
            throw new Error(
              messagesPort.getError('no-target-port', dataset.org)
            )
          }
          return new DatasetWriterFactory(sfPort).create(
            dataset,
            operation,
            listener,
            headerProvider,
            alignment
          )
        }
        return new FileWriterFactory().create(
          dataset,
          operation,
          listener,
          headerProvider,
          alignment
        )
      },
    }
  }
}
