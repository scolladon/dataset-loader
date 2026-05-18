import { readFile } from 'node:fs/promises'
import {
  type AuditOutcome,
  type QueryResult,
  type SalesforcePort,
  SkipDatasetError,
} from '../../../ports/types.js'
import { parseCsvHeader } from '../../column-name.js'
import { checkSchemaAlignment } from '../../schema-check.js'
import { buildSObjectRowProjection } from '../../sobject-row-projection.js'
import {
  type AuditCheckStrategy,
  type AuditContext,
  type AuditEntry,
  fail,
  pass,
  selectByDataset,
  warn,
} from '../audit-strategy.js'

// `evaluate` is a short-circuiting pipeline: each stage either returns
// a terminal AuditOutcome (pass/warn/fail) or yields the state the next
// stage needs. `StageResult<T>` carries that "continue with T or terminate"
// alternative and lets each stage stay independently named & testable.
type StageResult<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly outcome: AuditOutcome }

const stage = <T>(value: T): StageResult<T> => ({ ok: true, value })
const halt = <T>(outcome: AuditOutcome): StageResult<T> => ({
  ok: false,
  outcome,
})

export const schemaAlignment: AuditCheckStrategy = {
  select: selectByDataset,
  label: (org, key) => `${org}: dataset '${key}' schema alignment`,
  evaluate: async (sfPort, key, entry, ctx) => {
    // Order matters: providedFields-first lets ELF (no prior log) and CSV
    // (missing file) short-circuit to their precise WARN/FAIL before we
    // attempt synthesis. Otherwise the bootstrap path would throw
    // `ELF bootstrap requires …` and surface as a generic synthesis FAIL,
    // losing the original audit's actionable message.
    const providedFields = await requireProvidedFields(entry, ctx)
    if (!providedFields.ok) return providedFields.outcome
    const datasetFields = await resolveDatasetFields(sfPort, key, entry, ctx)
    if (!datasetFields.ok) return datasetFields.outcome
    return runSchemaChecks(
      entry,
      key,
      datasetFields.value,
      providedFields.value
    )
  },
}

// Decision matrix (mirrors `docs/design/2026-05-18-bootstrap-new-dataset-upload.md`
// §Audit integration):
//
//   metadata absent  | any override   → synthesised
//   metadata present | override=false → existing  (current behaviour)
//   metadata present | override=true  → synthesised
//
// Graceful degradation: if no provider is wired into the audit context (e.g.
// legacy callers, audit-only tests), the synthesised branch returns pass() so
// the audit silently steps aside — the writer's own resolveMetadata chain
// remains the source of truth for the load itself.
async function resolveDatasetFields(
  sfPort: SalesforcePort,
  key: string,
  entry: AuditEntry,
  ctx: AuditContext
): Promise<StageResult<readonly string[]>> {
  const metadata = await fetchMetadata(sfPort, key)
  const useSynthesised = !metadata || entry.overrideMetadata === true
  if (useSynthesised) {
    return resolveSynthesisedDatasetFields(entry, ctx, key)
  }
  return requireDatasetFields(metadata as string, key)
}

async function resolveSynthesisedDatasetFields(
  entry: AuditEntry,
  ctx: AuditContext,
  key: string
): Promise<StageResult<readonly string[]>> {
  if (!ctx.bootstrapProviderFor) return halt(pass())
  try {
    const schema = await ctx.bootstrapProviderFor(entry).buildSourceSchema()
    return stage(schema.fields.map(f => f.name))
  } catch (err: unknown) {
    /* v8 ignore next -- providers throw Error subclasses only; defensive */
    const message = err instanceof Error ? err.message : String(err)
    return halt(
      fail(
        `Bootstrap synthesis failed for dataset '${key}' (audit pre-flight): ${message}`
      )
    )
  }
}

function requireDatasetFields(
  metadata: string,
  key: string
): StageResult<readonly string[]> {
  const datasetFields = extractDatasetFields(metadata)
  if (!datasetFields) {
    return halt(
      fail(
        `Dataset '${key}' metadata has no objects[0].fields; cannot enforce column alignment`
      )
    )
  }
  return stage(datasetFields)
}

async function requireProvidedFields(
  entry: AuditEntry,
  ctx: AuditContext
): Promise<StageResult<readonly string[]>> {
  const providedFields = await resolveProvidedFields(entry, ctx)
  if (providedFields === 'warn:no-prior-elf') {
    return halt(
      warn(
        `No prior EventLogFile for ${entry.eventType}/${entry.interval}; schema check skipped`
      )
    )
  }
  if (providedFields === 'fail:csv-missing') {
    return halt(
      fail(`CSV file '${entry.csvFile}' could not be read for schema check`)
    )
  }
  return stage(providedFields)
}

function runSchemaChecks(
  entry: AuditEntry,
  datasetName: string,
  datasetFields: readonly string[],
  providedFields: readonly string[]
): AuditOutcome {
  const overlap = checkAugmentOverlap(entry, datasetName, providedFields)
  if (overlap) return overlap
  if (entry.readerKind === 'sobject') {
    return runSObjectCheck(entry, datasetName, datasetFields, providedFields)
  }
  return runOrderedCheck(entry, datasetName, datasetFields, providedFields)
}

function checkAugmentOverlap(
  entry: AuditEntry,
  datasetName: string,
  providedFields: readonly string[]
): AuditOutcome | null {
  const augmentKeys = Object.keys(entry.augmentColumns)
  const overlap = detectOverlap(providedFields, augmentKeys)
  if (overlap.length === 0) return null
  return fail(
    `Schema overlap for dataset '${datasetName}': augment columns also present as reader fields: [${overlap.join(', ')}]`
  )
}

function runOrderedCheck(
  entry: AuditEntry,
  datasetName: string,
  datasetFields: readonly string[],
  providedFields: readonly string[]
): AuditOutcome {
  const provided = [...providedFields, ...Object.keys(entry.augmentColumns)]
  // datasetName IS the entry's targetDataset (by construction of
  // selectByDataset, which keys the dedup slot on entry.targetDataset).
  const result = checkSchemaAlignment({
    datasetName,
    entryLabel: datasetName,
    expected: datasetFields,
    provided,
    checkOrder: true,
  })
  if (!result.ok) return fail(result.reason)
  if (result.casingDiff) {
    return warn(
      `Schema casing differs from dataset '${datasetName}' metadata; dataset will keep its canonical casing`
    )
  }
  return pass()
}

function runSObjectCheck(
  entry: AuditEntry,
  datasetName: string,
  datasetFields: readonly string[],
  providedFields: readonly string[]
): AuditOutcome {
  // datasetName IS the entry's targetDataset (by construction of
  // selectByDataset, which keys the dedup slot on entry.targetDataset).
  try {
    buildSObjectRowProjection({
      datasetName,
      entryLabel: datasetName,
      readerFields: providedFields,
      augmentColumns: entry.augmentColumns,
      datasetFields,
    })
  } catch (err) {
    // buildSObjectRowProjection throws only SkipDatasetError. Any other
    // error is a plumbing bug — rethrow to let buildChecks surface it.
    /* v8 ignore next */
    if (!(err instanceof SkipDatasetError)) throw err
    return fail(err.message)
  }
  // Casing WARN for SObject: set-only match, but raw case-sensitive sets differ
  const exactProvided = new Set([
    ...providedFields.map(f => f.replace(/\./g, '_')),
    ...Object.keys(entry.augmentColumns),
  ])
  const missesCase = datasetFields.some(n => !exactProvided.has(n))
  if (missesCase) {
    return warn(
      `Schema casing differs from dataset '${datasetName}' metadata; dataset will keep its canonical casing`
    )
  }
  return pass()
}

function detectOverlap(
  readerFields: readonly string[],
  augmentKeys: readonly string[]
): string[] {
  const readerNormalized = new Set(
    readerFields.map(f => f.replace(/\./g, '_').toLowerCase())
  )
  return augmentKeys.filter(k =>
    readerNormalized.has(k.replace(/\./g, '_').toLowerCase())
  )
}

async function resolveProvidedFields(
  entry: AuditEntry,
  ctx: AuditContext
): Promise<readonly string[] | 'warn:no-prior-elf' | 'fail:csv-missing'> {
  switch (entry.readerKind) {
    case 'sobject':
      // readerFields is always populated for SObject entries (commands layer
      // sets it from config.fields). Fallback is defensive.
      /* v8 ignore next */
      return entry.readerFields ?? []
    case 'elf':
      return resolveElfHeaderFields(entry, ctx)
    case 'csv':
      return resolveCsvHeaderFields(entry)
  }
}

async function resolveElfHeaderFields(
  entry: AuditEntry,
  ctx: AuditContext
): Promise<readonly string[] | 'warn:no-prior-elf'> {
  const sourcePort = ctx.sfPorts.get(entry.sourceOrg)
  if (!sourcePort) return 'warn:no-prior-elf'
  // Safe interpolation: eventType is SF_IDENTIFIER_PATTERN-constrained,
  // interval is z.enum(['Daily','Hourly']) — both validated at config
  // parse (config-loader.ts:175-180). Neither admits single quotes.
  const result = await sourcePort.query<{ LogFileFieldNames: string | null }>(
    `SELECT LogFileFieldNames FROM EventLogFile WHERE EventType = '${entry.eventType}' AND Interval = '${entry.interval}' ORDER BY LogDate DESC LIMIT 1`
  )
  const raw = result.records[0]?.LogFileFieldNames
  if (!raw) return 'warn:no-prior-elf'
  return parseCsvHeader(raw)
}

async function resolveCsvHeaderFields(
  entry: AuditEntry
): Promise<readonly string[] | 'fail:csv-missing'> {
  if (!entry.csvFile) return 'fail:csv-missing'
  try {
    const content = await readFile(entry.csvFile, 'utf-8')
    const firstLine = content.split('\n', 1)[0]
    return parseCsvHeader(firstLine)
  } catch {
    return 'fail:csv-missing'
  }
}

function extractDatasetFields(metadataJson: string): readonly string[] | null {
  try {
    const parsed: unknown = JSON.parse(metadataJson)
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))
      return null
    const objects = (parsed as { objects?: unknown }).objects
    if (!Array.isArray(objects) || objects.length === 0) return null
    const obj0 = objects[0] as { fields?: unknown }
    if (!Array.isArray(obj0.fields) || obj0.fields.length === 0) return null
    const names: string[] = []
    for (const f of obj0.fields) {
      const name = (f as { fullyQualifiedName?: unknown }).fullyQualifiedName
      if (typeof name !== 'string' || name.length === 0) return null
      names.push(name)
    }
    return names
    /* v8 ignore next 3 -- JSON parse failures surface as null; defensive */
  } catch {
    return null
  }
}

async function fetchMetadata(
  sfPort: SalesforcePort,
  datasetName: string
): Promise<string | null> {
  const result: QueryResult<{ MetadataJson: string | null }> =
    await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${datasetName}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
  if (result.records.length === 0 || !result.records[0].MetadataJson)
    return null
  const blob = await sfPort.getBlob(result.records[0].MetadataJson)
  return typeof blob === 'string' ? blob : JSON.stringify(blob)
}
