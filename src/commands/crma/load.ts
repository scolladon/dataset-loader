import { Org } from '@salesforce/core'
import { Flags, SfCommand } from '@salesforce/sf-plugins-core'
import {
  type ConfigEntry,
  entryLabel,
  parseConfig,
  type ResolvedEntry,
  resolveConfig,
} from '../../adapters/config-loader.js'
import { ElfFetcher } from '../../adapters/elf-fetcher.js'
import { ProgressReporter } from '../../adapters/progress-reporter.js'
import { SalesforceClient } from '../../adapters/sf-client.js'
import { SObjectFetcher } from '../../adapters/sobject-fetcher.js'
import { FileStateManager } from '../../adapters/state-manager.js'
import { UploadSinkFactory } from '../../adapters/upload-sink.js'
import { buildAuditChecks, runAudit } from '../../domain/auditor.js'
import { DatasetKey } from '../../domain/dataset-key.js'
import { executePipeline, type PipelineEntry } from '../../domain/pipeline.js'
import { WatermarkKey } from '../../domain/watermark-key.js'
import { type WatermarkStore } from '../../domain/watermark-store.js'
import {
  type CreateUploaderPort,
  type EntryType,
  type FetchPort,
  formatErrorMessage,
  type LoggerPort,
  type SalesforcePort,
  type StatePort,
} from '../../ports/types.js'

export interface CrmaLoadResult {
  entriesProcessed: number
  entriesSkipped: number
  entriesFailed: number
  groupsUploaded: number
}

export default class CrmaLoad extends SfCommand<CrmaLoadResult> {
  public static readonly summary =
    'Load Event Log Files and SObject data into CRMA datasets'
  public static readonly examples = [
    '<%= config.bin %> <%= command.id %>',
    '<%= config.bin %> <%= command.id %> --config-file my-config.json --dry-run',
  ]

  public static readonly flags = {
    'config-file': Flags.file({
      char: 'c',
      summary: 'Path to config JSON',
      default: 'crma-load.config.json',
    }),
    'state-file': Flags.file({
      char: 's',
      summary: 'Path to watermark state file',
      default: '.crma-load.state.json',
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

  public async run(): Promise<CrmaLoadResult> {
    const { flags } = await this.parse(CrmaLoad)
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
        allOrgs.add(e.sourceOrg)
        allOrgs.add(e.analyticOrg)
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
  ): Promise<CrmaLoadResult> {
    const auditEntries: {
      type: EntryType
      sourceOrg: string
      analyticOrg: string
    }[] = []
    for (const { entry } of entries) {
      auditEntries.push({
        type: entry.type,
        sourceOrg: entry.sourceOrg,
        analyticOrg: entry.analyticOrg,
      })
    }
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

  private handleDryRun(
    entries: ResolvedEntry[],
    watermarks: WatermarkStore
  ): CrmaLoadResult {
    this.log('Dry run — planned entries:')
    for (const { entry } of entries) {
      const wk = WatermarkKey.fromEntry(entry)
      const wm = watermarks.get(wk)?.toString() ?? '(none)'
      this.log(
        `  ${entryLabel(entry)} → ${entry.analyticOrg}:${entry.dataset} (watermark: ${wm})`
      )
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
  ): Promise<CrmaLoadResult> {
    const pipelineEntries: PipelineEntry[] = []
    for (const resolvedEntry of entries) {
      pipelineEntries.push(this.buildPipelineEntry(resolvedEntry, sfPorts))
    }

    const createUploader: CreateUploaderPort = {
      create(dataset, operation, listener) {
        const sfPort = sfPorts.get(dataset.org)
        if (!sfPort)
          throw new Error(`No SF connection for org '${dataset.org}'`)
        const factory = new UploadSinkFactory(sfPort)
        return factory.create(dataset, operation, listener)
      },
    }

    const result = await executePipeline({
      entries: pipelineEntries,
      watermarks,
      createUploader,
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
    sfPorts: Map<string, SalesforcePort>
  ): PipelineEntry {
    const { entry, index, augmentColumns } = resolvedEntry
    const srcPort = sfPorts.get(entry.sourceOrg)
    if (!srcPort)
      throw new Error(`No SF connection for org '${entry.sourceOrg}'`)
    return {
      index,
      label: entryLabel(entry),
      watermarkKey: WatermarkKey.fromEntry(entry),
      datasetKey: DatasetKey.fromEntry(entry),
      operation: entry.operation,
      augmentColumns,
      fetcher: this.createFetcher(entry, srcPort),
    }
  }

  private createFetcher(entry: ConfigEntry, sfPort: SalesforcePort): FetchPort {
    if (entry.type === 'elf') {
      return new ElfFetcher(sfPort, entry.eventType, entry.interval)
    }
    return new SObjectFetcher(sfPort, {
      sobject: entry.sobject,
      fields: entry.fields,
      dateField: entry.dateField,
      where: entry.where,
      queryLimit: entry.limit,
    })
  }
}
