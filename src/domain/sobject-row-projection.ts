import { type ProjectionLayout, SkipDatasetError } from '../ports/types.js'
import { csvQuote } from './csv-quote.js'
import { formatSchemaMismatch } from './schema-check.js'

interface SObjectProjectionInput {
  readonly datasetName: string
  readonly entryLabel: string
  readonly readerFields: readonly string[]
  readonly augmentColumns: Readonly<Record<string, string>>
  readonly datasetFields: readonly string[]
}

const translateDots = (name: string): string => name.replace(/\./g, '_')
const normalize = (name: string): string => translateDots(name).toLowerCase()

export function buildSObjectRowProjection(
  input: SObjectProjectionInput
): ProjectionLayout {
  const readerNames = input.readerFields.map(translateDots)
  const augmentKeys = Object.keys(input.augmentColumns).map(translateDots)

  rejectOverlap(input, readerNames, augmentKeys)

  const datasetIndexByName = buildDatasetIndex(input.datasetFields)
  const provided = [...readerNames, ...augmentKeys]
  rejectMismatch(input, datasetIndexByName, provided)

  const outputIndex = new Int32Array(readerNames.length)
  for (let i = 0; i < readerNames.length; i++) {
    outputIndex[i] = lookup(datasetIndexByName, readerNames[i])
  }

  const augmentSlots = Object.entries(input.augmentColumns).map(
    ([key, value]) => ({
      pos: lookup(datasetIndexByName, translateDots(key)),
      quoted: csvQuote(value),
    })
  )

  return {
    targetSize: input.datasetFields.length,
    augmentSlots,
    outputIndex,
  }
}

function rejectOverlap(
  input: SObjectProjectionInput,
  readerNames: readonly string[],
  augmentKeys: readonly string[]
): void {
  const readerSet = new Set(readerNames.map(n => n.toLowerCase()))
  const overlap = augmentKeys.filter(k => readerSet.has(k.toLowerCase()))
  if (overlap.length === 0) return
  throw new SkipDatasetError(
    `Schema overlap for dataset '${input.datasetName}' (entry '${input.entryLabel}'):\n` +
      `  augment columns also present as reader fields: [${overlap.join(', ')}]`
  )
}

function rejectMismatch(
  input: SObjectProjectionInput,
  datasetIndexByName: ReadonlyMap<string, number>,
  provided: readonly string[]
): void {
  const providedNormalized = new Set(provided.map(n => n.toLowerCase()))
  const missing = input.datasetFields.filter(
    name => !providedNormalized.has(name.toLowerCase())
  )
  const extra = provided.filter(
    name => !datasetIndexByName.has(normalize(name))
  )
  if (missing.length === 0 && extra.length === 0) return

  throw new SkipDatasetError(
    formatSchemaMismatch(input.datasetName, input.entryLabel, missing, extra)
  )
}

function buildDatasetIndex(
  datasetFields: readonly string[]
): ReadonlyMap<string, number> {
  const map = new Map<string, number>()
  for (let i = 0; i < datasetFields.length; i++) {
    map.set(datasetFields[i].toLowerCase(), i)
  }
  return map
}

function lookup(
  datasetIndexByName: ReadonlyMap<string, number>,
  name: string
): number {
  const idx = datasetIndexByName.get(name.toLowerCase())
  /* v8 ignore next 3 -- rejectMismatch already guarantees presence; defensive */
  if (idx === undefined) {
    throw new Error(`internal: name '${name}' not in dataset index`)
  }
  return idx
}
