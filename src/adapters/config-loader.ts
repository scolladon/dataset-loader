import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { z } from 'zod'
import { DatasetKey } from '../domain/dataset-key.js'
import {
  type EntryType,
  type Operation,
  type SalesforcePort,
  SF_IDENTIFIER_PATTERN,
} from '../ports/types.js'

export interface BaseEntry {
  name?: string
  sourceOrg: string
  targetOrg?: string
  targetDataset?: string
  targetFile?: string
  operation: Operation
  augmentColumns?: Record<string, string>
}

export interface ElfEntry extends BaseEntry {
  type: Extract<EntryType, 'elf'>
  eventType: string
  interval: 'Daily' | 'Hourly'
}

export interface SObjectEntry extends BaseEntry {
  type: Extract<EntryType, 'sobject'>
  sobject: string
  fields: string[]
  dateField: string
  where?: string
  limit?: number
}

export interface CsvEntry {
  name?: string
  type: Extract<EntryType, 'csv'>
  sourceFile: string
  targetOrg?: string
  targetDataset?: string
  targetFile?: string
  operation: Operation
  augmentColumns?: Record<string, string>
}

export type ConfigEntry = ElfEntry | SObjectEntry | CsvEntry

export interface Config {
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
const crmaColumnName = z
  .string()
  .regex(
    /^[a-zA-Z_][a-zA-Z0-9_.]*$/,
    'Must be a valid CRMA column name (letters, digits, underscores, dots)'
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
  const hasCrma = !!entry.targetOrg
  const hasFile = !!entry.targetFile

  if (hasCrma && hasFile) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Cannot specify both targetOrg and targetFile',
    })
  }
  if (!hasCrma && !hasFile) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'Either targetOrg+targetDataset or targetFile must be specified',
    })
  }
  if (hasCrma && !entry.targetDataset) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'targetDataset is required when targetOrg is set',
      path: ['targetDataset'],
    })
  }
  if (!hasCrma && entry.targetDataset) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: 'targetDataset requires targetOrg to be set',
      path: ['targetDataset'],
    })
  }
}

const baseEntrySchema = z
  .object({
    name: entryName.optional(),
    sourceOrg: orgAlias,
    targetOrg: orgAlias.optional(),
    targetDataset: sfIdentifier.optional(),
    targetFile: z.string().min(1).optional(),
    operation: z.enum(['Append', 'Overwrite']).default('Append'),
    augmentColumns: z.record(crmaColumnName, z.string()).optional(),
  })
  .superRefine((entry, ctx) => {
    validateTargetFields(entry, ctx)
    if (!entry.targetOrg && entry.augmentColumns) {
      for (const [key, value] of Object.entries(entry.augmentColumns)) {
        if (MUSTACHE_TARGETORG.test(String(value))) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `augmentColumns['${key}'] uses {{targetOrg.*}} which is not allowed for file-target entries (targetOrg is absent)`,
            path: ['augmentColumns', key],
          })
        }
      }
    }
  })

const elfEntrySchema = baseEntrySchema.extend({
  type: z.literal('elf'),
  eventType: sfIdentifier,
  interval: z.enum(['Daily', 'Hourly']),
})

const sobjectEntrySchema = baseEntrySchema.extend({
  type: z.literal('sobject'),
  sobject: sfIdentifier,
  fields: z.array(sfIdentifier).min(1),
  dateField: sfIdentifier.default('LastModifiedDate'),
  // Trust boundary: where clause is user-supplied SOQL interpolated directly into queries.
  // The config file is a trusted input — do not construct it from untrusted sources.
  where: z.string().optional(),
  limit: z.number().int().positive().optional(),
})

const csvEntrySchema = z
  .object({
    name: entryName.optional(),
    type: z.literal('csv'),
    sourceFile: z
      .string()
      .min(1)
      .refine(p => path.isAbsolute(p) || !path.normalize(p).startsWith('..'), {
        message: 'sourceFile must not traverse parent directories',
      }),
    targetOrg: orgAlias.optional(),
    targetDataset: sfIdentifier.optional(),
    targetFile: z.string().min(1).optional(),
    operation: z.enum(['Append', 'Overwrite']).default('Append'),
    augmentColumns: z.record(crmaColumnName, z.string()).optional(),
  })
  .strict()
  .superRefine((entry, ctx) => {
    validateTargetFields(entry, ctx)
    if (entry.augmentColumns) {
      for (const [key, value] of Object.entries(entry.augmentColumns)) {
        if (
          MUSTACHE_SOURCEORG.test(String(value)) ||
          MUSTACHE_TARGETORG.test(String(value))
        ) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `augmentColumns['${key}'] uses a dynamic expression which is not allowed for csv entries`,
            path: ['augmentColumns', key],
          })
        }
      }
    }
  })

const configSchema = z.object({
  entries: z
    .array(
      z.discriminatedUnion('type', [
        elfEntrySchema,
        sobjectEntrySchema,
        csvEntrySchema,
      ])
    )
    .min(1),
})

interface OrgInfo {
  Id: string
  Name: string
}

function collectUniqueOrgs(entries: ConfigEntry[]): Set<string> {
  const orgs = new Set<string>()
  for (const entry of entries) {
    if (entry.type === 'csv') continue
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

function resolveAugmentColumnsForEntry(
  columns: Record<string, string> | undefined,
  entry: ElfEntry | SObjectEntry,
  orgInfos: Map<string, OrgInfo>
): Record<string, string> {
  if (!columns) return {}
  const resolved: Record<string, string> = {}

  const getOrgInfo = (alias: string): OrgInfo => {
    const info = orgInfos.get(alias)
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
    augmentColumns:
      entry.type === 'csv'
        ? (entry.augmentColumns ?? {})
        : resolveAugmentColumnsForEntry(entry.augmentColumns, entry, orgInfos),
  }))
}

export function entryLabel(entry: ConfigEntry): string {
  if (entry.name) return entry.name
  if (entry.type === 'elf') return `elf:${entry.eventType}`
  if (entry.type === 'sobject') return `sobject:${entry.sobject}`
  return `csv:${entry.sourceFile}`
}
