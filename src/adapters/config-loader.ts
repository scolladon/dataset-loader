import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { z } from 'zod'
import { DatasetKey } from '../domain/dataset-key.js'
import {
  type CsvShape,
  type ElfShape,
  isCsv,
  isElf,
  isSObject,
  type Operation,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
  type SObjectShape,
  SOQL_RELATIONSHIP_PATH_PATTERN,
} from '../ports/types.js'

interface BaseEntry {
  readonly name?: string
  readonly sourceOrg: string
  readonly targetOrg?: string
  readonly targetDataset?: string
  readonly targetFile?: string
  readonly operation: Operation
  readonly augmentColumns?: Record<string, string>
}

export interface ElfEntry extends BaseEntry, ElfShape {
  interval: 'Daily' | 'Hourly'
}

export interface SObjectEntry extends BaseEntry, SObjectShape {
  fields: string[]
  dateField: string
  where?: string
  limit?: number
}

export interface CsvEntry extends CsvShape {
  targetOrg?: string
  targetDataset?: string
  targetFile?: string
  operation: Operation
  augmentColumns?: Record<string, string>
}

export type ConfigEntry = ElfEntry | SObjectEntry | CsvEntry

export const isElfEntry = isElf<ConfigEntry>
export const isSObjectEntry = isSObject<ConfigEntry>
export const isCsvEntry = isCsv<ConfigEntry>

interface Config {
  entries: ConfigEntry[]
}

export interface ResolvedEntry {
  entry: ConfigEntry
  index: number
  augmentColumns: Record<string, string>
}

const MUSTACHE_TOKEN = /\{\{([\w.]+)\}\}/g
const MUSTACHE_TARGETORG = /\{\{targetOrg\./
const MUSTACHE_SOURCEORG = /\{\{sourceOrg\./

const sfIdentifier = z
  .string()
  .regex(SF_IDENTIFIER_PATTERN, 'Must be a valid Salesforce identifier')
const soqlRelationshipPath = z
  .string()
  .regex(
    SOQL_RELATIONSHIP_PATH_PATTERN,
    'Must be a valid SOQL field or relationship path (e.g. Name, Owner.Name)'
  )
const datasetColumnName = z
  .string()
  .regex(
    /^[a-zA-Z_][a-zA-Z0-9_.]*$/,
    'Must be a valid dataset column name (letters, digits, underscores, dots)'
  )
const orgAlias = z
  .string()
  .regex(/^[a-zA-Z0-9_.-]+$/, 'Must be a valid org alias (no colons)')

const entryName = z
  .string()
  .regex(/^[a-zA-Z0-9_-]+$/, 'Must be alphanumeric, hyphens, or underscores')

function validateTargetFields(
  entry: { targetOrg?: string; targetDataset?: string; targetFile?: string },
  ctx: z.RefinementCtx
): void {
  const hasDatasetTarget = !!entry.targetOrg
  const hasFile = !!entry.targetFile

  if (hasDatasetTarget && hasFile) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Cannot specify both targetOrg and targetFile',
    })
  }
  if (!hasDatasetTarget && !hasFile) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Either targetOrg+targetDataset or targetFile must be specified',
    })
  }
  if (hasDatasetTarget && !entry.targetDataset) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'targetDataset is required when targetOrg is set',
      path: ['targetDataset'],
    })
  }
  if (!hasDatasetTarget && entry.targetDataset) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'targetDataset requires targetOrg to be set',
      path: ['targetDataset'],
    })
  }
}

function rejectAugmentColumns(
  columns: Record<string, string>,
  predicate: (value: string) => boolean,
  messageFor: (key: string) => string,
  ctx: z.RefinementCtx
): void {
  for (const [key, value] of Object.entries(columns)) {
    if (predicate(String(value))) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: messageFor(key),
        path: ['augmentColumns', key],
      })
    }
  }
}

const targetFields = {
  targetOrg: orgAlias.optional(),
  targetDataset: sfIdentifier.optional(),
  targetFile: z
    .string()
    .min(1)
    .refine(p => path.isAbsolute(p) || !path.normalize(p).startsWith('..'), {
      message: 'targetFile must not traverse parent directories',
    })
    .optional(),
  operation: z.enum(['Append', 'Overwrite']).default('Append'),
  augmentColumns: z.record(datasetColumnName, z.string()).optional(),
} as const

const baseEntrySchema = z
  .object({
    name: entryName.optional(),
    sourceOrg: orgAlias,
    ...targetFields,
  })
  .superRefine((entry, ctx) => {
    validateTargetFields(entry, ctx)
    if (!entry.targetOrg && entry.augmentColumns) {
      rejectAugmentColumns(
        entry.augmentColumns,
        value => MUSTACHE_TARGETORG.test(value),
        key =>
          `augmentColumns['${key}'] uses {{targetOrg.*}} which is not allowed for file-target entries (targetOrg is absent)`,
        ctx
      )
    }
  })

const elfEntrySchema = baseEntrySchema
  .extend({
    eventLog: sfIdentifier,
    interval: z.enum(['Daily', 'Hourly']),
  })
  .strict()

// Deny-list for user-supplied WHERE clause: block statement separators, comment
// markers, control characters (incl. DEL and Unicode line/paragraph
// separators), and unbalanced parentheses (which otherwise let a payload break
// out of the `(${where})` wrapping). Defense-in-depth against SOQL payloads
// even though the config is considered a trusted input.
// biome-ignore lint/suspicious/noControlCharactersInRegex: intentionally blocks ASCII control chars
const FORBIDDEN_WHERE_CHARS = /[;`\\\x00-\x1f\x7f\u2028\u2029]/
const FORBIDDEN_WHERE_SEQUENCES = /\/\*|\*\/|--/
function parensAreBalanced(v: string): boolean {
  let depth = 0
  for (const ch of v) {
    if (ch === '(') depth++
    else if (ch === ')' && --depth < 0) return false
  }
  return depth === 0
}
const whereClause = z
  .string()
  .refine(v => !FORBIDDEN_WHERE_CHARS.test(v), {
    message:
      'where clause contains forbidden characters (; ` \\, control chars, or Unicode separators)',
  })
  .refine(v => !FORBIDDEN_WHERE_SEQUENCES.test(v), {
    message: 'where clause contains forbidden comment markers (/*, */, --)',
  })
  .refine(parensAreBalanced, {
    message:
      'where clause has unbalanced parentheses — would break out of the AND-wrapping and broaden the filter',
  })

const sobjectEntrySchema = baseEntrySchema
  .extend({
    sObject: sfIdentifier,
    fields: z.array(soqlRelationshipPath).min(1),
    dateField: sfIdentifier.default('LastModifiedDate'),
    // Trust boundary: where clause is user-supplied SOQL interpolated directly
    // into queries. Narrowed via whereClause to reject statement separators
    // and comment markers; config file is still a trusted input.
    where: whereClause.optional(),
    limit: z.number().int().positive().optional(),
  })
  .strict()

const csvEntrySchema = z
  .object({
    name: entryName.optional(),
    csvFile: z
      .string()
      .min(1)
      .refine(p => path.isAbsolute(p) || !path.normalize(p).startsWith('..'), {
        message: 'csvFile must not traverse parent directories',
      }),
    ...targetFields,
  })
  .strict()
  .superRefine((entry, ctx) => {
    validateTargetFields(entry, ctx)
    if (entry.augmentColumns) {
      rejectAugmentColumns(
        entry.augmentColumns,
        value =>
          MUSTACHE_SOURCEORG.test(value) || MUSTACHE_TARGETORG.test(value),
        key =>
          `augmentColumns['${key}'] uses a dynamic expression which is not allowed for csv entries`,
        ctx
      )
    }
  })

const DISCRIMINATOR_FIELDS = ['eventLog', 'sObject', 'csvFile'] as const

const entrySchema = z
  .unknown()
  .superRefine((val, ctx) => {
    if (typeof val !== 'object' || val === null || Array.isArray(val)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'Entry must be an object',
      })
      return
    }
    const obj = val as Record<string, unknown>
    const present = DISCRIMINATOR_FIELDS.filter(k => k in obj)
    if (present.length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message:
          'Entry must have one of: eventLog (ELF), sObject (SObject), or csvFile (CSV)',
      })
    }
    if (present.length > 1) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Entry must have exactly one of eventLog, sObject, or csvFile — found: ${present.join(', ')}`,
      })
    }
  })
  .pipe(z.union([elfEntrySchema, sobjectEntrySchema, csvEntrySchema]))

const configSchema = z.object({
  entries: z.array(entrySchema).min(1),
})

interface OrgInfo {
  Id: string
  Name: string
}

function collectUniqueOrgs(entries: ConfigEntry[]): Set<string> {
  const orgs = new Set<string>()
  for (const entry of entries) {
    if (isCsvEntry(entry)) continue
    const columns = entry.augmentColumns ?? {}
    for (const value of Object.values(columns)) {
      if (MUSTACHE_SOURCEORG.test(value)) orgs.add(entry.sourceOrg)
      if (MUSTACHE_TARGETORG.test(value) && entry.targetOrg)
        orgs.add(entry.targetOrg)
    }
  }
  return orgs
}

function groupEntriesByDatasetKey(
  entries: ConfigEntry[]
): Map<
  string,
  { datasetKey: DatasetKey; ops: { operation: string; indices: number[] }[] }
> {
  const groups = new Map<
    string,
    { datasetKey: DatasetKey; ops: { operation: string; indices: number[] }[] }
  >()

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]
    const datasetKey = DatasetKey.fromEntry(entry)
    const mapKey = datasetKey.toString()
    const operation = entry.operation

    if (!groups.has(mapKey)) {
      groups.set(mapKey, { datasetKey, ops: [] })
    }
    const group = groups.get(mapKey)!
    const existing = group.ops.find(g => g.operation === operation)
    if (existing) {
      existing.indices.push(i)
    } else {
      group.ops.push({ operation, indices: [i] })
    }
  }

  return groups
}

function validateOperationConsistency(entries: ConfigEntry[]): void {
  const groups = groupEntriesByDatasetKey(entries)

  for (const [, { datasetKey, ops }] of groups) {
    if (ops.length > 1) {
      const details = ops
        .map(o => `'${o.operation}' (entries ${o.indices.join(', ')})`)
        .join(' vs ')
      throw new Error(
        `Entries target dataset '${datasetKey.toString()}' but specify conflicting operations: ${details}`
      )
    }
  }
}

function validateNameUniqueness(entries: ConfigEntry[]): void {
  const seen = new Map<string, number>()
  for (let i = 0; i < entries.length; i++) {
    const name = entries[i].name
    if (!name) continue
    const previousIndex = seen.get(name)
    if (previousIndex !== undefined) {
      throw new Error(
        `Duplicate entry name '${name}' at indices ${previousIndex} and ${i}`
      )
    }
    seen.set(name, i)
  }
}

function validateAugmentColumnConsistency(entries: ConfigEntry[]): void {
  const groups = groupEntriesByDatasetKey(entries)

  for (const [, { datasetKey, ops }] of groups) {
    const groupEntries = ops.flatMap(o => o.indices).map(i => entries[i])
    if (groupEntries.length < 2) continue

    const firstKeys = Object.keys(groupEntries[0].augmentColumns ?? {})
      .sort()
      .join(',')
    for (const entry of groupEntries.slice(1)) {
      const keys = Object.keys(entry.augmentColumns ?? {})
        .sort()
        .join(',')
      if (firstKeys !== keys) {
        throw new Error(
          `Entries targeting '${datasetKey.toString()}' have different augment column names: [${firstKeys}] vs [${keys}]`
        )
      }
    }
  }
}

function validateSObjectFieldConsistency(entries: ConfigEntry[]): void {
  const groups = groupEntriesByDatasetKey(entries)

  for (const [, { datasetKey, ops }] of groups) {
    const sobjectEntries = ops
      .flatMap(o => o.indices)
      .map(i => entries[i])
      .filter((e): e is SObjectEntry => isSObjectEntry(e))
    if (sobjectEntries.length < 2) continue

    const firstFields = [...sobjectEntries[0].fields].sort()
    for (const entry of sobjectEntries.slice(1)) {
      const fields = [...entry.fields].sort()
      if (firstFields.join(',') !== fields.join(',')) {
        throw new Error(
          `SObject entries targeting '${datasetKey.toString()}' have different fields: [${firstFields}] vs [${fields}]`
        )
      }
    }
  }
}

function resolveAugmentColumnsForEntry(
  columns: Record<string, string> | undefined,
  entry: ElfEntry | SObjectEntry,
  orgInfos: Map<string, OrgInfo>
): Record<string, string> {
  if (!columns) return {}
  const resolved: Record<string, string> = {}

  const getOrgInfo = (alias: string): OrgInfo => {
    const info = orgInfos.get(alias)
    /* v8 ignore next -- orgInfos is built from the same entries; missing alias is a programming error */
    if (!info) throw new Error(`Org info not resolved for '${alias}'`)
    return info
  }

  for (const [key, value] of Object.entries(columns)) {
    resolved[key] = value.replace(MUSTACHE_TOKEN, (_, token: string) => {
      if (token === 'sourceOrg.Id') return getOrgInfo(entry.sourceOrg).Id
      if (token === 'sourceOrg.Name') return getOrgInfo(entry.sourceOrg).Name
      if (token === 'targetOrg.Id') return getOrgInfo(entry.targetOrg!).Id
      if (token === 'targetOrg.Name') return getOrgInfo(entry.targetOrg!).Name
      throw new Error(`Unknown mustache token: {{${token}}}`)
    })
  }
  return resolved
}

export async function parseConfig(configPath: string): Promise<Config> {
  const raw = await readFile(configPath, 'utf-8')
  const config: Config = configSchema.parse(JSON.parse(raw))
  validateOperationConsistency(config.entries)
  validateNameUniqueness(config.entries)
  validateAugmentColumnConsistency(config.entries)
  validateSObjectFieldConsistency(config.entries)
  return config
}

export async function resolveConfig(
  config: Config,
  sfPorts: ReadonlyMap<string, SalesforcePort>
): Promise<ResolvedEntry[]> {
  const orgsToResolve = collectUniqueOrgs(config.entries)
  const orgInfos = new Map<string, OrgInfo>()

  await Promise.all(
    [...orgsToResolve].map(async alias => {
      const sfPort = sfPorts.get(alias)
      if (!sfPort)
        throw new Error(`No authenticated connection for org '${alias}'`)
      const result = await sfPort.query<OrgInfo>(
        'SELECT Id, Name FROM Organization LIMIT 1'
      )
      if (result.records.length === 0)
        throw new Error(`Organization query returned no records for '${alias}'`)
      orgInfos.set(alias, result.records[0])
    })
  )

  return config.entries.map((entry, index) => ({
    entry,
    index,
    augmentColumns: isCsvEntry(entry)
      ? (entry.augmentColumns ?? {})
      : resolveAugmentColumnsForEntry(entry.augmentColumns, entry, orgInfos),
  }))
}

/* v8 ignore start -- the throw and the csv=false branch are unreachable: Zod rejects any entry not matching one of the three discriminators */
export function entryLabel(entry: ConfigEntry): string {
  if (entry.name) return entry.name
  if (isElfEntry(entry)) return `elf:${entry.eventLog}`
  if (isSObjectEntry(entry)) return `sobject:${entry.sObject}`
  if (isCsvEntry(entry)) return `csv:${entry.csvFile}`
  throw new Error(`Unknown entry shape`)
}
/* v8 ignore stop */
