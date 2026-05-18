// Reader-agnostic intermediate representation of a source schema and the
// CRMA InsightsExternalData MetadataJson shape we serialise to.
//
// Two layers on purpose:
//
//   SourceSchema (this file)   ← what each reader can describe about itself
//        │
//        ▼  metadata-synthesizer.synthesise()
//   CrmaMetadataJson (this file)  ← the wire format CRMA expects
//
// Keeping them separate lets each reader produce a SourceSchema in a way
// that's natural for its source (Describe API, LogFileFieldNames, CSV header)
// and keeps the synthesiser a pure, exhaustively-testable mapping with no
// knowledge of any reader.

export type CrmaFieldType = 'Text' | 'Numeric' | 'Date'

export interface SourceField {
  // CRMA-legal column name. Dots in SObject relationship paths must be
  // translated to underscores BEFORE landing here (the SObject provider
  // does that — the synthesiser never rewrites names).
  readonly name: string
  readonly label: string
  readonly type: CrmaFieldType
  // Numeric only:
  readonly precision?: number
  readonly scale?: number
  // Date only:
  readonly format?: string
  // Lets a provider mark a column as derived from an augment constant, so
  // downstream consumers (dry-run renderer, audit) can distinguish reader
  // fields from augment fields without re-parsing config.
  readonly origin: 'reader' | 'augment'
}

export interface SourceSchema {
  readonly datasetName: string
  readonly label: string
  readonly fields: readonly SourceField[]
}

// Subset of the CRMA MetadataJson contract we actually emit. The CRMA schema
// accepts more (canBeIndexed, isMultiValue, multiValueSeparator,
// fiscalMonthOffset, etc.) but those are not required for a bootstrap and can
// be added incrementally without breaking the wire format.
interface CrmaFileFormat {
  readonly charsetName: 'UTF-8'
  readonly fieldsDelimitedBy: ','
  readonly fieldsEnclosedBy: '"'
  readonly linesTerminatedBy: '\n'
  readonly numberOfLinesToIgnore: 0
}

export interface CrmaField {
  readonly name: string
  readonly fullyQualifiedName: string
  readonly label: string
  readonly type: CrmaFieldType
  readonly precision?: number
  readonly scale?: number
  readonly format?: string
  readonly defaultValue?: string
}

export interface CrmaObject {
  readonly connector: string
  readonly fullyQualifiedName: string
  readonly label: string
  readonly name: string
  readonly fields: readonly CrmaField[]
}

export interface CrmaMetadataJson {
  readonly fileFormat: CrmaFileFormat
  readonly objects: readonly CrmaObject[]
}
