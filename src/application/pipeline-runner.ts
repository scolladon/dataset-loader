import {
  type ElfEntry,
  entryLabel,
  isCsvEntry,
  isElfEntry,
  type ResolvedEntry,
  type SObjectEntry,
} from '../adapters/config-loader.js'
import { buildAugmentHeaderSuffix } from '../adapters/pipeline/augment-transform.js'
import { CsvReader } from '../adapters/readers/csv-reader.js'
import { ElfReader } from '../adapters/readers/elf-reader.js'
import { SObjectReader } from '../adapters/readers/sobject-reader.js'
import { DatasetKey } from '../domain/dataset-key.js'
import { type DateBounds } from '../domain/date-bounds.js'
import { resolveProvidedFields } from '../domain/field-resolver.js'
import { executePipeline, type PipelineEntry } from '../domain/pipeline.js'
import { ReaderKey } from '../domain/reader-key.js'
import { WatermarkKey } from '../domain/watermark-key.js'
import { type WatermarkStore } from '../domain/watermark-store.js'
import { type MessagesPort } from '../ports/messages.js'
import {
  type AlignmentSpec,
  type CreateWriterPort,
  type LoggerPort,
  type ProgressPort,
  type ReaderPort,
  type SalesforcePort,
  type StatePort,
} from '../ports/types.js'
import { type DatasetLoadResult } from './load-inputs.js'
import { computeWarnings } from './warnings.js'

// Inputs required by PipelineRunner at construction time. Keeps the port
// wiring (SalesforcePort lookup, writer factory, progress reporter) in the
// command layer; the runner itself is port-agnostic.
export interface PipelineRunnerDeps {
  readonly logger: LoggerPort
  readonly messages: MessagesPort
  readonly createWriter: CreateWriterPort
  readonly progress: ProgressPort
}

// Output of pass 1 — synchronous reader-cache build. Pass 2 enriches each
// slot with an AlignmentSpec before the PipelineEntry is handed to the
// domain pipeline.
interface PipelineEntrySlot {
  readonly resolvedEntry: ResolvedEntry
  readonly index: number
  readonly readerKey: ReaderKey
  readonly fetcher: ReaderPort
  readonly augmentColumns: Record<string, string>
}

// Runs the real (non-dry) dispatch: emits warnings, builds the pipeline
// entries in two passes (sync reader-cache then async alignment), executes
// the domain pipeline, logs the summary and sets the exit code.
export class PipelineRunner {
  constructor(private readonly deps: PipelineRunnerDeps) {}

  async run(
    entries: readonly ResolvedEntry[],
    sfPorts: ReadonlyMap<string, SalesforcePort>,
    watermarks: WatermarkStore,
    state: StatePort,
    bounds: DateBounds
  ): Promise<DatasetLoadResult> {
    for (const msg of computeWarnings(entries, watermarks, bounds)) {
      this.deps.logger.warn(msg)
    }
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
      createWriter: this.deps.createWriter,
      state,
      progress: this.deps.progress,
      logger: this.deps.logger,
    })

    this.deps.logger.info(
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
    sfPorts: ReadonlyMap<string, SalesforcePort>,
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
    /* v8 ignore next 4 -- srcPort presence is guaranteed by the
       loadAndResolveConfig pre-pass in load.ts (all entries' sourceOrg
       aliases are ensured before PipelineRunner.run is called). */
    if (!srcPort)
      throw new Error(
        this.deps.messages.getError('no-source-port', entry.sourceOrg)
      )
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
    sfPorts: ReadonlyMap<string, SalesforcePort>
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

  // Entries sharing the same readerKey share one ReaderPort instance so that
  // header() returns a valid value for all of them after the shared fetch() call.
  private getOrCreateReader(
    cacheKey: string,
    cache: Map<string, ReaderPort>,
    entry: ElfEntry | SObjectEntry,
    srcPort: SalesforcePort,
    bounds: DateBounds
  ): ReaderPort {
    const existing = cache.get(cacheKey)
    if (existing) return existing
    const reader = this.createFetcher(entry, srcPort, bounds)
    cache.set(cacheKey, reader)
    return reader
  }

  // CSV entries take a separate branch in `buildPipelineEntryStatic`, so
  // these two helpers are narrowed to the non-CSV union — callers can't
  // pass a CsvEntry by accident and the dead CSV case is gone.
  private createReaderKey(
    entry: ElfEntry | SObjectEntry,
    bounds: DateBounds
  ): ReaderKey {
    if (isElfEntry(entry)) {
      return ReaderKey.forElf(
        entry.sourceOrg,
        entry.eventLog,
        entry.interval,
        bounds
      )
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
    entry: ElfEntry | SObjectEntry,
    sfPort: SalesforcePort,
    bounds: DateBounds
  ): ReaderPort {
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
}
