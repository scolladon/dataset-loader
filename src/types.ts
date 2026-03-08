export interface BaseEntry {
  sourceOrg: string
  analyticOrg: string
  dataset: string
  operation?: 'Append' | 'Overwrite'
  augmentColumns?: Record<string, string>
}

export interface ElfEntry extends BaseEntry {
  type: 'elf'
  eventType: string
  interval: 'Daily' | 'Hourly'
}

export interface SObjectEntry extends BaseEntry {
  type: 'sobject'
  sobject: string
  fields: string[]
  dateField?: string
  where?: string
}

export type ConfigEntry = ElfEntry | SObjectEntry

export interface Config {
  entries: ConfigEntry[]
}

export interface ResolvedAugmentColumns {
  [key: string]: string
}

export interface ResolvedEntry {
  entry: ConfigEntry
  index: number
  augmentColumns: ResolvedAugmentColumns
}

export interface FetchResult {
  csv: string
  newWatermark: string
}

export interface GroupInput {
  key: string
  csv: string
  operation: 'Append' | 'Overwrite'
}

export interface GroupResult {
  csv: string
  operation: 'Append' | 'Overwrite'
}

export interface StateFile {
  watermarks: Record<string, string>
}

export function watermarkKey(entry: ConfigEntry): string {
  if (entry.type === 'elf') {
    return `${entry.sourceOrg}:elf:${entry.eventType}:${entry.interval}`
  }
  return `${entry.sourceOrg}:sobject:${entry.sobject}`
}

export function groupKey(entry: ConfigEntry): string {
  return `${entry.analyticOrg}:${entry.dataset}`
}
