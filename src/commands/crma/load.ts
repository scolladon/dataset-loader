import { Org } from '@salesforce/core'
import { Flags, SfCommand } from '@salesforce/sf-plugins-core'
import {
  type ConfigEntry,
  parseConfig,
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
import {
  type CreateUploaderPort,
  type LoggerPort,
  type Operation,
  type SalesforcePort,
} from '../../ports/types.js'

function entryLabel(entry: ConfigEntry): string {
  if (entry.name) return entry.name
  return entry.type === 'elf'
    ? `elf:${entry.eventType}`
    : `sobject:${entry.sobject}`
}

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

    const getSfPort = async (orgAlias: string): Promise<SalesforcePort> => {
      if (!sfPorts.has(orgAlias)) {
        const org = await Org.create({ aliasOrUsername: orgAlias })
        sfPorts.set(orgAlias, new SalesforceClient(org.getConnection()))
      }
      return sfPorts.get(orgAlias)!
    }

    const logger: LoggerPort = {
      info: msg => this.log(msg),
      warn: msg => this.warn(msg),
      debug: msg => this.debug(msg),
    }

    let resolvedEntries
    try {
      const config = await parseConfig(flags['config-file'])
      const allOrgs = new Set<string>()
      for (const e of config.entries) {
        allOrgs.add(e.sourceOrg)
        allOrgs.add(e.analyticOrg)
      }
      await Promise.all([...allOrgs].map(alias => getSfPort(alias)))
      resolvedEntries = await resolveConfig(config, sfPorts)
    } catch (error) {
      this.error(
        `Config loading failed: ${error instanceof Error ? error.message : error}`
      )
    }

    if (flags['entry'] !== undefined) {
      const hasAnyName = resolvedEntries.some(e => e.entry.name)
      resolvedEntries = resolvedEntries.filter(
        e => e.entry.name === flags['entry']
      )
      if (resolvedEntries.length === 0) {
        const hint = hasAnyName
          ? ''
          : ' Ensure your config entries have a "name" field.'
        this.error(`Entry '${flags['entry']}' not found.${hint}`)
      }
    }

    if (flags['audit']) {
      const auditEntries = resolvedEntries.map(({ entry }) => ({
        type: entry.type,
        sourceOrg: entry.sourceOrg,
        analyticOrg: entry.analyticOrg,
      }))
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

    const state = new FileStateManager(flags['state-file'])
    const watermarks = await state.read()

    if (flags['dry-run']) {
      this.log('Dry run — planned entries:')
      for (const { entry } of resolvedEntries) {
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

    const pipelineEntries: PipelineEntry[] = resolvedEntries.map(
      ({ entry, index, augmentColumns }) => {
        const srcPort = sfPorts.get(entry.sourceOrg)!
        return {
          index,
          label: entryLabel(entry),
          watermarkKey: WatermarkKey.fromEntry(entry),
          datasetKey: DatasetKey.fromEntry(entry),
          operation: entry.operation as Operation,
          augmentColumns,
          fetcher:
            entry.type === 'elf'
              ? new ElfFetcher(srcPort, entry.eventType, entry.interval)
              : new SObjectFetcher(
                  srcPort,
                  entry.sobject,
                  entry.fields,
                  entry.dateField!,
                  entry.where,
                  entry.limit
                ),
        }
      }
    )

    const createUploader: CreateUploaderPort = {
      create(dataset, operation) {
        const factory = new UploadSinkFactory(sfPorts.get(dataset.org)!)
        return factory.create(dataset, operation)
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
}
