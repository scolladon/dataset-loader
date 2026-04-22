import { Messages, Org } from '@salesforce/core'
import { Flags, SfCommand } from '@salesforce/sf-plugins-core'
import {
  isCsvEntry,
  parseConfig,
  type ResolvedEntry,
  resolveConfig,
} from '../../adapters/config-loader.js'
import { ProgressReporter } from '../../adapters/progress-reporter.js'
import { SalesforceClient } from '../../adapters/sf-client.js'
import { FileStateManager } from '../../adapters/state-manager.js'
import { DatasetWriterFactory } from '../../adapters/writers/dataset-writer.js'
import { FileWriterFactory } from '../../adapters/writers/file-writer.js'
import { AuditRunner } from '../../application/audit-runner.js'
import { DryRunRenderer } from '../../application/dry-run-renderer.js'
import {
  type DatasetLoadResult,
  type LoadInputs,
  parseLoadInputs,
  type RawLoadFlags,
} from '../../application/load-inputs.js'
import { PipelineRunner } from '../../application/pipeline-runner.js'
import {
  loadDatasetLoadMessages,
  type MessagesPort,
} from '../../ports/messages.js'
import {
  type CreateWriterPort,
  formatErrorMessage,
  type LoggerPort,
  type SalesforcePort,
} from '../../ports/types.js'

Messages.importMessagesDirectoryFromMetaUrl(import.meta.url)
const messagesPort: MessagesPort = loadDatasetLoadMessages()

// Thin composition root: parses flags, loads the config, filters entries,
// and dispatches to the appropriate runner. All orchestration is in the
// runner classes; this file wires ports to them and handles error
// reporting through `this.error` (oclif) and `this.log/warn/debug`
// (bridged to the LoggerPort).
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
    const inputs = this.parseInputs(flags as RawLoadFlags)
    const sfPorts = new Map<string, SalesforcePort>()
    const logger = this.createLogger()

    const resolvedEntries = await this.loadAndResolveConfig(
      inputs.configPath,
      sfPorts
    )
    const filtered = this.filterByEntry(resolvedEntries, inputs.entryFilter)

    if (inputs.audit) {
      return new AuditRunner(logger).run(filtered, sfPorts)
    }

    const state = new FileStateManager(inputs.statePath)
    const watermarks = await state.read()

    if (inputs.dryRun) {
      return new DryRunRenderer(logger).render(
        filtered,
        watermarks,
        inputs.bounds
      )
    }

    return new PipelineRunner({
      logger,
      messages: messagesPort,
      createWriter: this.createWriterFactory(sfPorts),
      progress: new ProgressReporter(),
    }).run(filtered, sfPorts, watermarks, state, inputs.bounds)
  }

  private parseInputs(flags: RawLoadFlags): LoadInputs {
    try {
      return parseLoadInputs(flags)
    } catch (err) {
      // `this.error` is `never`-returning but TypeScript doesn't always
      // infer that in all code paths, so throw-fallthrough is avoided by
      // returning after.
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
