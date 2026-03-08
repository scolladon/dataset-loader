import { readFile } from 'node:fs/promises'
import { z } from 'zod'
import {
  type EntryType,
  type Operation,
  type SalesforcePort,
} from '../ports/types.js'

export interface BaseEntry {
  name?: string
  sourceOrg: string
  analyticOrg: string
  dataset: string
  operation?: Operation
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
  dateField?: string
  where?: string
  limit?: number
}

export type ConfigEntry = ElfEntry | SObjectEntry

export interface Config {
  entries: ConfigEntry[]
}

export interface ResolvedEntry {
  entry: ConfigEntry
  index: number
  augmentColumns: Record<string, string>
}

const DYNAMIC_EXPRESSIONS = [
  '$sourceOrg.Id',
  '$sourceOrg.Name',
  '$analyticOrg.Id',
  '$analyticOrg.Name',
] as const

const sfIdentifier = z
  .string()
  .regex(/^[a-zA-Z_][a-zA-Z0-9_]*$/, 'Must be a valid Salesforce identifier')
const orgAlias = z
  .string()
  .regex(/^[a-zA-Z0-9_.-]+$/, 'Must be a valid org alias (no colons)')

const entryName = z
  .string()
  .regex(/^[a-zA-Z0-9_-]+$/, 'Must be alphanumeric, hyphens, or underscores')

const baseEntrySchema = z.object({
  name: entryName.optional(),
  sourceOrg: orgAlias,
  analyticOrg: orgAlias,
  dataset: sfIdentifier,
  operation: z.enum(['Append', 'Overwrite']).default('Append'),
  augmentColumns: z.record(sfIdentifier, z.string()).optional(),
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

const configSchema = z.object({
  entries: z
    .array(z.discriminatedUnion('type', [elfEntrySchema, sobjectEntrySchema]))
    .min(1),
})

interface OrgInfo {
  Id: string
  Name: string
}

function collectUniqueOrgs(entries: ConfigEntry[]): Set<string> {
  const orgs = new Set<string>()
  for (const entry of entries) {
    const columns = entry.augmentColumns ?? {}
    for (const value of Object.values(columns)) {
      if (DYNAMIC_EXPRESSIONS.some(expr => value === expr)) {
        if (value.startsWith('$sourceOrg.')) orgs.add(entry.sourceOrg)
        if (value.startsWith('$analyticOrg.')) orgs.add(entry.analyticOrg)
      }
    }
  }
  return orgs
}

function validateOperationConsistency(entries: ConfigEntry[]): void {
  const groups = new Map<string, { operation: string; indices: number[] }[]>()

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]
    const key = `${entry.analyticOrg}:${entry.dataset}`
    const operation = entry.operation!

    if (!groups.has(key)) {
      groups.set(key, [])
    }
    const group = groups.get(key)!
    const existing = group.find(g => g.operation === operation)
    if (existing) {
      existing.indices.push(i)
    } else {
      group.push({ operation, indices: [i] })
    }
  }

  for (const [key, ops] of groups) {
    if (ops.length > 1) {
      const details = ops
        .map(o => `'${o.operation}' (entries ${o.indices.join(', ')})`)
        .join(' vs ')
      const [analyticOrg] = key.split(':')
      const dataset = key.slice(analyticOrg.length + 1)
      throw new Error(
        `Entries target dataset '${dataset}' on org '${analyticOrg}' but specify conflicting operations: ${details}`
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
  entry: ConfigEntry,
  orgInfos: Map<string, OrgInfo>
): Record<string, string> {
  if (!columns) return {}
  const resolved: Record<string, string> = {}

  for (const [key, value] of Object.entries(columns)) {
    if (value === '$sourceOrg.Id')
      resolved[key] = orgInfos.get(entry.sourceOrg)!.Id
    else if (value === '$sourceOrg.Name')
      resolved[key] = orgInfos.get(entry.sourceOrg)!.Name
    else if (value === '$analyticOrg.Id')
      resolved[key] = orgInfos.get(entry.analyticOrg)!.Id
    else if (value === '$analyticOrg.Name')
      resolved[key] = orgInfos.get(entry.analyticOrg)!.Name
    else resolved[key] = value
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
    augmentColumns: resolveAugmentColumnsForEntry(
      entry.augmentColumns,
      entry,
      orgInfos
    ),
  }))
}

export async function loadConfig(
  configPath: string,
  sfPorts: ReadonlyMap<string, SalesforcePort>
): Promise<ResolvedEntry[]> {
  const config = await parseConfig(configPath)
  return resolveConfig(config, sfPorts)
}
