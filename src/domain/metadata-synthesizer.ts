// Pure mapping: SourceSchema -> CRMA MetadataJson string.
// No I/O, no SF dependency, fully deterministic. Per-reader providers
// (sobject/elf/csv) own SourceSchema construction; this module is the
// SINGLE place that knows the CRMA wire format.
//
// Wire-grounded constants below are anchored to specific probes that
// exercised CRMA v67.0 in the spike. Re-verify before bumping any default
// — drift between the comment and CRMA's actual rejection message is the
// kind of silent gap that loses days.

import {
  type CrmaField,
  type CrmaMetadataJson,
  type CrmaObject,
  type SourceField,
  type SourceSchema,
} from './metadata-types.js'

const SF_IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/

// ── Numeric ──────────────────────────────────────────────────────────
// CRMA requires precision in [1, 18] AND a defaultValue on every Numeric
// field (FIELD_INTEGRITY_EXCEPTION otherwise). SF Describe sometimes
// returns precision 0 for standard int fields (e.g. User.NumberOfFailedLogins);
// those land outside CRMA's range, so we coerce out-of-range precision to
// the safe maximum.
const DEFAULT_NUMERIC_PRECISION = 18
const DEFAULT_NUMERIC_SCALE = 0
const DEFAULT_NUMERIC_DEFAULT_VALUE = '0'
const CRMA_MIN_PRECISION = 1
const CRMA_MAX_PRECISION = 18

// ── Date ─────────────────────────────────────────────────────────────
// `defaultValue` is NOT required for Date (CRMA accepts empty cells as
// null). We deliberately do NOT emit one — if emitted it must parse against
// the declared `format` token-for-token (a mismatch fails the whole load).
//
// CRMA's `XXX` format token is NARROWER than Java SimpleDateFormat:
//   ✅ ±hhmm (e.g. +0000, +0200)  ← what SF native datetime emits
//   ❌ ±hh:mm (e.g. +00:00)
//   ❌ Z literal
// So this default is safe for SF datetime → CSV pipelines; CSV files from
// other systems may need a different format string.
//
// Malformed Date cells FAIL THE ENTIRE BATCH — no silent-null, no partial
// success. Downstream features that validate source data may be warranted.
const DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

export class SynthesizeError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SynthesizeError'
  }
}

export function synthesise(schema: SourceSchema): string {
  validateSchema(schema)
  const obj: CrmaObject = {
    connector: 'DatasetLoader',
    fullyQualifiedName: schema.datasetName,
    label: schema.label,
    name: schema.datasetName,
    fields: schema.fields.map(toCrmaField),
  }
  const metadata: CrmaMetadataJson = {
    fileFormat: {
      charsetName: 'UTF-8',
      fieldsDelimitedBy: ',',
      fieldsEnclosedBy: '"',
      linesTerminatedBy: '\n',
      numberOfLinesToIgnore: 0,
    },
    objects: [obj],
  }
  return JSON.stringify(metadata)
}

function validateSchema(schema: SourceSchema): void {
  if (!SF_IDENTIFIER_PATTERN.test(schema.datasetName)) {
    throw new SynthesizeError(
      `Invalid dataset name '${schema.datasetName}' — must be a valid Salesforce identifier`
    )
  }
  if (schema.fields.length === 0) {
    throw new SynthesizeError(
      `Cannot synthesise metadata for dataset '${schema.datasetName}': no fields`
    )
  }
  const seen = new Set<string>()
  for (const f of schema.fields) {
    if (!SF_IDENTIFIER_PATTERN.test(f.name)) {
      throw new SynthesizeError(
        `Invalid field name '${f.name}' — dotted SObject paths must be translated to underscores by the provider`
      )
    }
    const key = f.name.toLowerCase()
    if (seen.has(key)) {
      throw new SynthesizeError(
        `Duplicate field '${f.name}' in dataset '${schema.datasetName}' (case-insensitive)`
      )
    }
    seen.add(key)
  }
}

function toCrmaField(f: SourceField): CrmaField {
  const base = {
    name: f.name,
    fullyQualifiedName: f.name,
    label: f.label,
    type: f.type,
  } as const

  if (f.type === 'Numeric') {
    const rawPrecision = f.precision ?? DEFAULT_NUMERIC_PRECISION
    const precision =
      rawPrecision >= CRMA_MIN_PRECISION && rawPrecision <= CRMA_MAX_PRECISION
        ? rawPrecision
        : DEFAULT_NUMERIC_PRECISION
    const scale = Math.min(f.scale ?? DEFAULT_NUMERIC_SCALE, precision)
    return {
      ...base,
      precision,
      scale,
      defaultValue: DEFAULT_NUMERIC_DEFAULT_VALUE,
    }
  }

  if (f.type === 'Date') {
    return {
      ...base,
      format: f.format ?? DEFAULT_DATETIME_FORMAT,
    }
  }

  return base
}
