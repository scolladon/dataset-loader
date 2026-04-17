import { Org } from '@salesforce/core'
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
import { buildAuditChecks, runAudit } from '../../domain/auditor.js'
import { DatasetKey } from '../../domain/dataset-key.js'
import { executePipeline, type PipelineEntry } from '../../domain/pipeline.js'
import { ReaderKey } from '../../domain/reader-key.js'
import { WatermarkKey } from '../../domain/watermark-key.js'
import { type WatermarkStore } from '../../domain/watermark-store.js'
import {
  type CreateWriterPort,
  formatErrorMessage,
  type LoggerPort,
  type ReaderPort,
  type SalesforcePort,
  type StatePort,
} from '../../ports/types.js'

interface DatasetLoadResult {
  entriesProcessed: number
  entriesSkipped: number
  entriesFailed: number
  groupsUploaded: number
}

export default class DatasetLoad extends SfCommand<DatasetLoadResult> {
  public static readonly summary =
    'Load Event Log Files and SObject data into CRM Analytics datasets'
  public static readonly examples = [
    '<%= config.bin %> <%= command.id %>',
    '<%= config.bin %> <%= command.id %> --config-file my-config.json --dry-run',
  ]

  public static readonly flags = {
    'config-file': Flags.file({
      char: 'c',
      summary: 'Path to config JSON',
      default: 'dataset-load.config.json',
    }),
    'state-file': Flags.file({
      char: 's',
      summary: 'Path to watermark state file',
      default: '.dataset-load.state.json',
    }),
    audit: Flags.boolean({
      summary: 'Pre-flight checks only (auth, connectivity, permissions)',
      default: false,
    }),
    'dry-run': Flags.boolean({
      summary: 'Show plan without executing',
      default: false,
    }),
    entry: Flags.string({
      summary: 'Process only the entry with this name',
    }),
  }

  public async run(): Promise<DatasetLoadResult> {
    const { flags } = await this.parse(DatasetLoad)
    const sfPorts = new Map<string, SalesforcePort>()
    const logger = this.createLogger()

    const resolvedEntries = await this.loadAndResolveConfig(
      flags['config-file'],
      sfPorts
    )

    const filtered = this.filterByEntry(resolvedEntries, flags['entry'])

    if (flags['audit']) return this.handleAudit(filtered, sfPorts, logger)

    const state = new FileStateManager(flags['state-file'])
    const watermarks = await state.read()

    if (flags['dry-run']) return this.handleDryRun(filtered, watermarks)

    return this.handlePipeline(filtered, sfPorts, watermarks, state, logger)
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
      this.error(`Config loading failed: ${formatErrorMessage(error)}`)
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
        : ' Ensure your config entries have a "name" field.'
      this.error(`Entry '${entryName}' not found.${hint}`)
    }
    return filtered
  }

  private async handleAudit(
    entries: ResolvedEntry[],
    sfPorts: Map<string, SalesforcePort>,
    logger: LoggerPort
  ): Promise<DatasetLoadResult> {
    const orgEntries: {
      isElf: boolean
      sourceOrg: string
      targetOrg?: string
      sObject?: string
      targetDataset?: string
    }[] = []
    for (const { entry } of entries) {
      if (isCsvEntry(entry)) continue
      orgEntries.push({
        isElf: isElfEntry(entry),
        sourceOrg: entry.sourceOrg,
        targetOrg: entry.targetOrg,
        sObject: isSObjectEntry(entry) ? entry.sObject : undefined,
        targetDataset: entry.targetDataset,
      })
    }
    logger.info('Audit — pre-flight checks:')
    const checks = buildAuditChecks(orgEntries, sfPorts)
    const auditResult = await runAudit(checks, logger)
    if (!auditResult.passed) process.exitCode = 2
    return {
      entriesProcessed: 0,
      entriesSkipped: 0,
      entriesFailed: auditResult.passed ? 0 : 1,
      groupsUploaded: 0,
    }
  }

  private handleDryRun(
    entries: ResolvedEntry[],
    watermarks: WatermarkStore
  ): DatasetLoadResult {
    this.log('Dry run — planned entries:')
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

  private async handlePipeline(
    entries: ResolvedEntry[],
    sfPorts: Map<string, SalesforcePort>,
    watermarks: WatermarkStore,
    state: StatePort,
    logger: LoggerPort
  ): Promise<DatasetLoadResult> {
    const sharedReaders = new Map<string, ReaderPort>()
    const pipelineEntries: PipelineEntry[] = []
    for (const resolvedEntry of entries) {
      pipelineEntries.push(
        this.buildPipelineEntry(resolvedEntry, sfPorts, sharedReaders)
      )
    }

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

  private buildPipelineEntry(
    resolvedEntry: ResolvedEntry,
    sfPorts: Map<string, SalesforcePort>,
    sharedReaders: Map<string, ReaderPort>
  ): PipelineEntry {
    const { entry, index, augmentColumns } = resolvedEntry

    if (isCsvEntry(entry)) {
      const readerKey = ReaderKey.forCsv(entry.csvFile)
      const cacheKey = readerKey.toString()
      const existing = sharedReaders.get(cacheKey)
      const fetcher: ReaderPort = existing ?? new CsvReader(entry.csvFile)
      if (!existing) sharedReaders.set(cacheKey, fetcher)
      return {
        index,
        label: entryLabel(entry),
        readerKey,
        watermarkKey: WatermarkKey.fromEntry(entry),
        datasetKey: DatasetKey.fromEntry(entry),
        operation: entry.operation,
        augmentColumns,
        fetcher,
        header: async () =>
          (await fetcher.header()) + buildAugmentHeaderSuffix(augmentColumns),
      }
    }

    // Salesforce-based entries (ELF / SObject) — existing logic below, unchanged
    const srcPort = sfPorts.get(entry.sourceOrg)
    if (!srcPort)
      throw new Error(`No SF connection for org '${entry.sourceOrg}'`)
    const readerKey = this.createReaderKey(entry)
    const readerCacheKey = readerKey.toString()
    const fetcher = this.getOrCreateReader(
      readerCacheKey,
      sharedReaders,
      entry,
      srcPort
    )
    return {
      index,
      label: entryLabel(entry),
      readerKey,
      watermarkKey: WatermarkKey.fromEntry(entry),
      datasetKey: DatasetKey.fromEntry(entry),
      operation: entry.operation,
      augmentColumns,
      fetcher,
      header: async () => {
        const raw = await fetcher.header()
        return raw + buildAugmentHeaderSuffix(augmentColumns)
      },
    }
  }

  // Entries sharing the same readerKey share one ReaderPort instance so that header() returns
  // a valid value for all of them after the shared fetch() call.
  private getOrCreateReader(
    cacheKey: string,
    cache: Map<string, ReaderPort>,
    entry: ConfigEntry,
    srcPort: SalesforcePort
  ): ReaderPort {
    const existing = cache.get(cacheKey)
    if (existing) return existing
    const reader = this.createFetcher(entry, srcPort)
    cache.set(cacheKey, reader)
    return reader
  }

  private createReaderKey(entry: ConfigEntry): ReaderKey {
    if (isElfEntry(entry)) {
      return ReaderKey.forElf(entry.sourceOrg, entry.eventLog, entry.interval)
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
      entry.limit
    )
  }

  private createFetcher(
    entry: ConfigEntry,
    sfPort: SalesforcePort
  ): ReaderPort {
    if (isCsvEntry(entry)) {
      return new CsvReader(entry.csvFile)
    }
    if (isElfEntry(entry)) {
      return new ElfReader(sfPort, entry.eventLog, entry.interval)
    }
    return new SObjectReader(sfPort, {
      sobject: entry.sObject,
      fields: entry.fields,
      dateField: entry.dateField,
      where: entry.where,
      queryLimit: entry.limit,
    })
  }

  private createWriterFactory(
    sfPorts: Map<string, SalesforcePort>
  ): CreateWriterPort {
    return {
      create(dataset, operation, listener, headerProvider) {
        if (dataset.org) {
          const sfPort = sfPorts.get(dataset.org)
          if (!sfPort) {
            throw new Error(
              `No authenticated connection for target org '${dataset.org}'`
            )
          }
          return new DatasetWriterFactory(sfPort).create(
            dataset,
            operation,
            listener,
            headerProvider
          )
        }
        return new FileWriterFactory().create(
          dataset,
          operation,
          listener,
          headerProvider
        )
      },
    }
  }
}
