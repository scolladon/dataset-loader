import { Messages, Org } from '@salesforce/core'
import { Flags, SfCommand } from '@salesforce/sf-plugins-core'
import {
  type ConfigEntry,
  type ElfEntry,
  entryLabel,
  isCsvEntry,
  isElfEntry,
  isSObjectEntry,
  parseConfig,
  type ResolvedEntry,
  resolveConfig,
  type SObjectEntry,
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
import { parseCsvHeader } from '../../domain/column-name.js'
import { DatasetKey } from '../../domain/dataset-key.js'
import { DateBounds } from '../../domain/date-bounds.js'
import { executePipeline, type PipelineEntry } from '../../domain/pipeline.js'
import { ReaderKey } from '../../domain/reader-key.js'
import { type Watermark } from '../../domain/watermark.js'
import { WatermarkKey } from '../../domain/watermark-key.js'
import { type WatermarkStore } from '../../domain/watermark-store.js'
import {
  type AlignmentSpec,
  type CreateWriterPort,
  formatErrorMessage,
  type LoggerPort,
  type ReaderPort,
  type SalesforcePort,
  type StatePort,
} from '../../ports/types.js'

Messages.importMessagesDirectoryFromMetaUrl(import.meta.url)
const messages = Messages.loadMessages('dataset-loader', 'dataset.load')

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
  public static readonly summary = messages.getMessage('summary')
  public static readonly examples = messages.getMessages('examples')

  public static readonly flags = {
    'config-file': Flags.file({
      char: 'c',
      summary: messages.getMessage('flags.config-file.summary'),
      default: 'dataset-load.config.json',
    }),
    'state-file': Flags.file({
      char: 's',
      summary: messages.getMessage('flags.state-file.summary'),
      default: '.dataset-load.state.json',
    }),
    audit: Flags.boolean({
      summary: messages.getMessage('flags.audit.summary'),
      default: false,
    }),
    'dry-run': Flags.boolean({
      summary: messages.getMessage('flags.dry-run.summary'),
      default: false,
    }),
    entry: Flags.string({
      summary: messages.getMessage('flags.entry.summary'),
    }),
    'start-date': Flags.string({
      summary: messages.getMessage('flags.start-date.summary'),
    }),
    'end-date': Flags.string({
      summary: messages.getMessage('flags.end-date.summary'),
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
    // SObject
    const so = entry as { sObject: string; fields: readonly string[] }
    return {
      readerKind: 'sobject',
      sourceOrg: entry.sourceOrg,
      targetOrg: entry.targetOrg,
      targetDataset: entry.targetDataset,
      augmentColumns,
      sObject: so.sObject,
      readerFields: so.fields,
    }
  }

  private handleDryRun(
    entries: ResolvedEntry[],
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): DatasetLoadResult {
    this.emitBoundsWarnings(entries, watermarks, bounds)
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
       handleDryRun when `!bounds.isEmpty()`, which means SD or ED is set.
       SD set → lower = `DF >= SD` (defined); ED set → upper = `DF <= ED`
       (defined). At least one is always present, so conds.length >= 1. */
    // Stryker disable next-line ConditionalExpression: equivalent mutant —
    // this branch is structurally unreachable (see v8 ignore rationale above).
    if (conds.length === 0) return
    const soql = conds.join(' AND ')
    const annotation = this.dryRunAnnotation(entry, wm, bounds)
    this.log(`    effective: ${soql}${annotation}`)
  }

  // Precondition: `entry` is non-CSV (caller `renderDryRunEntry` returns
  // early on CSV before reaching here).
  private dryRunAnnotation(
    entry: ElfEntry | SObjectEntry,
    wm: Watermark | undefined,
    bounds: DateBounds
  ): string {
    if (bounds.rewindsBelow(wm)) {
      return '  (REWIND: --start-date before watermark — watermark may regress)'
    }
    if (bounds.leavesHoleAbove(wm)) {
      return '  (HOLE: --start-date after watermark — records in the gap will never be back-filled)'
    }
    if (bounds.matchesWatermark(wm) && entry.operation === 'Append') {
      return '  (BOUNDARY: --start-date equals watermark — boundary record will be re-appended (duplicate))'
    }
    if (bounds.endsBeforeWatermark(wm)) {
      return '  (EMPTY: end-date before watermark — no records will load)'
    }
    return ''
  }

  private emitBoundsWarnings(
    entries: ResolvedEntry[],
    watermarks: WatermarkStore,
    bounds: DateBounds
  ): void {
    // Stryker disable next-line ConditionalExpression: equivalent mutant.
    // Removing this early-return yields observationally identical behavior
    // when bounds are empty: with no startAt/endAt, all four inner
    // predicates (rewindsBelow / leavesHoleAbove / matchesWatermark /
    // endsBeforeWatermark) always return false, and Zod guarantees
    // `entries.length >= 1` so the `nonCsv.length === 0` branch cannot
    // falsely fire a "no effect" warning. The early-return is retained
    // for readability and to short-circuit the no-op hot path.
    if (bounds.isEmpty()) return
    const nonCsv = entries.filter(({ entry }) => !isCsvEntry(entry))
    if (nonCsv.length === 0) {
      this.warn(
        '--start-date / --end-date provided but all selected entries are CSV; bounds have no effect. CSV entries are streamed in full.'
      )
      return
    }
    for (const { entry } of nonCsv) {
      const wm = watermarks.get(WatermarkKey.fromEntry(entry))
      const label = entryLabel(entry)
      if (bounds.rewindsBelow(wm)) {
        this.warn(
          `[${label}] REWIND: --start-date is before watermark ${wm}; previously-loaded records will be re-loaded; watermark may regress.`
        )
      } else if (bounds.leavesHoleAbove(wm)) {
        this.warn(
          `[${label}] HOLE: --start-date is after watermark ${wm}; records between the watermark and --start-date will be skipped this run AND by subsequent incremental runs (watermark will jump past the gap as soon as any in-window record loads).`
        )
      } else if (bounds.matchesWatermark(wm) && entry.operation === 'Append') {
        this.warn(
          `[${label}] BOUNDARY: --start-date equals watermark ${wm}; under operation Append the boundary record will be appended again (duplicate row). Bump --start-date past the watermark, or use operation Overwrite.`
        )
      } else if (bounds.endsBeforeWatermark(wm)) {
        // Only reachable when SD is absent: `DateBounds.from` enforces
        // SD <= ED, so when SD is set the SD-branches above always fire
        // first (covering SD < WM, SD == WM, and SD > WM) or the window
        // is non-empty (ED >= SD implies ED >= WM when SD >= WM).
        this.warn(
          `[${label}] EMPTY: --end-date is before watermark ${wm}; query window is empty — no records will load. To replay this range, use a separate --state-file (see RUN_BOOK).`
        )
      }
    }
  }

  private async handlePipeline(
    entries: ResolvedEntry[],
    sfPorts: Map<string, SalesforcePort>,
    watermarks: WatermarkStore,
    state: StatePort,
    logger: LoggerPort,
    bounds: DateBounds
  ): Promise<DatasetLoadResult> {
    this.emitBoundsWarnings(entries, watermarks, bounds)
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
      throw new Error(`No SF connection for org '${entry.sourceOrg}'`)
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
    const providedFields = await this.resolveProvidedFields(
      entry,
      fetcher,
      sfPorts
    )
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

  private async resolveProvidedFields(
    entry: ConfigEntry,
    fetcher: ReaderPort,
    sfPorts: Map<string, SalesforcePort>
  ): Promise<readonly string[]> {
    if (isSObjectEntry(entry)) {
      return (entry as { fields: readonly string[] }).fields
    }
    if (isCsvEntry(entry)) {
      // Reuse the same CsvReader built in pass 1 — header() is memoised per
      // instance (csv-reader.ts:14), so a pipeline run triggers only one fs read.
      return parseCsvHeader(await fetcher.header())
    }
    // ELF: query LogFileFieldNames for the most recent file of this type.
    // Empty result (no prior blob) → empty list; writer & audit WARN.
    if (!isElfEntry(entry)) {
      /* v8 ignore next 2 -- exhaustive discriminator; unreachable */
      throw new Error('unknown entry kind')
    }
    const elfEntry = entry as {
      sourceOrg: string
      eventLog: string
      interval: string
    }
    const srcPort = sfPorts.get(elfEntry.sourceOrg)
    /* v8 ignore next 2 -- srcPort presence is validated in buildPipelineEntryStatic */
    if (!srcPort)
      throw new Error(`No SF connection for org '${elfEntry.sourceOrg}'`)
    // Safe interpolation: eventLog is constrained by SF_IDENTIFIER_PATTERN
    // (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`) at config parse (config-loader.ts:177),
    // interval is `z.enum(['Daily','Hourly'])` (config-loader.ts:178) — both
    // exclude the single-quote character that would enable SOQL injection.
    //
    // Errors (permissions, connectivity) are swallowed here: the audit phase
    // is the authoritative place to surface them. Returning empty
    // providedFields lets the writer-init short-circuit the schema check
    // and lets the subsequent fetch() fail per-entry instead of killing
    // the whole run.
    let raw: string | null | undefined
    try {
      const result = await srcPort.query<{
        LogFileFieldNames: string | null
      }>(
        `SELECT LogFileFieldNames FROM EventLogFile WHERE EventType = '${elfEntry.eventLog}' AND Interval = '${elfEntry.interval}' ORDER BY LogDate DESC LIMIT 1`
      )
      raw = result.records[0]?.LogFileFieldNames
    } catch {
      return []
    }
    return raw ? parseCsvHeader(raw) : []
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
              `No authenticated connection for target org '${dataset.org}'`
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
