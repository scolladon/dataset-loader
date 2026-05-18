import {
  type CrmaFieldType,
  type SourceField,
  type SourceSchema,
} from '../../domain/metadata-types.js'
import { appendAugmentFields } from '../../domain/source-schema-builder.js'
import type {
  BootstrapMetadataProvider,
  DescribeField,
  SObjectDescribe,
} from '../../ports/types.js'

// Subset of the port we depend on. Narrowing keeps the test surface tight —
// the provider only needs Describe access, not the full SalesforcePort.
export interface SObjectDescribePort {
  describe(sobjectName: string): Promise<SObjectDescribe>
}

export interface SObjectBootstrapInput {
  readonly sobject: string
  readonly fields: readonly string[]
  readonly datasetName: string
  readonly augmentColumns: Readonly<Record<string, string>>
}

// Per-build cache so the same sObject described N times costs 1 request.
// Each provider instance gets its own cache — short-lived; the SalesforceClient
// has its own longer-lived cache backing the underlying describe() call.
export class SObjectBootstrapProvider implements BootstrapMetadataProvider {
  private readonly cache = new Map<string, SObjectDescribe>()

  constructor(
    private readonly port: SObjectDescribePort,
    private readonly input: SObjectBootstrapInput
  ) {}

  async buildSourceSchema(): Promise<SourceSchema> {
    const fields: SourceField[] = []
    for (const path of this.input.fields) {
      fields.push(await this.resolveField(path))
    }
    appendAugmentFields(fields, this.input.augmentColumns)
    return {
      datasetName: this.input.datasetName,
      label: this.input.datasetName,
      fields,
    }
  }

  private async describe(name: string): Promise<SObjectDescribe> {
    const cached = this.cache.get(name)
    if (cached) return cached
    const fresh = await this.port.describe(name)
    this.cache.set(name, fresh)
    return fresh
  }

  private async resolveField(path: string): Promise<SourceField> {
    const segments = path.split('.')
    let currentSobject = this.input.sobject
    for (let i = 0; i < segments.length - 1; i++) {
      const seg = segments[i]
      const sd = await this.describe(currentSobject)
      const rel = sd.fields.find(
        f => (f.relationshipName ?? '').toLowerCase() === seg.toLowerCase()
      )
      if (!rel || !rel.referenceTo || rel.referenceTo.length === 0) {
        throw new Error(
          `Relationship '${seg}' not found on '${currentSobject}' (path '${path}' on root '${this.input.sobject}')`
        )
      }
      currentSobject = rel.referenceTo[0]
    }
    const leafSegment = segments[segments.length - 1]
    const leafSd = await this.describe(currentSobject)
    const leaf = leafSd.fields.find(
      f => f.name.toLowerCase() === leafSegment.toLowerCase()
    )
    if (!leaf) {
      throw new Error(
        `Field '${path}' not found on root sObject '${this.input.sobject}': leaf '${leafSegment}' missing on '${currentSobject}'`
      )
    }
    return toSourceField(path, leaf)
  }
}

function toSourceField(path: string, f: DescribeField): SourceField {
  const crmaType = mapSfTypeToCrma(f.type)
  const columnName = path.replace(/\./g, '_')
  const base: SourceField = {
    name: columnName,
    label: path,
    type: crmaType,
    origin: 'reader',
  }
  if (crmaType === 'Numeric') {
    return { ...base, precision: f.precision, scale: f.scale }
  }
  if (crmaType === 'Date') {
    return { ...base, format: dateFormatFor(f.type) }
  }
  return base
}

// SF type → CRMA type mapping. Conservative: any unrecognised type lands as
// Text. Wrong-type-as-Text is recoverable (rebuild the dataset via
// overrideMetadata); wrong-type-as-Numeric/Date is not.
function mapSfTypeToCrma(sfType: string): CrmaFieldType {
  switch (sfType) {
    case 'int':
    case 'long':
    case 'double':
    case 'currency':
    case 'percent':
      return 'Numeric'
    case 'date':
    case 'datetime':
      return 'Date'
    default:
      // string, textarea, id, reference, email, phone, url, picklist,
      // multipicklist, combobox, encryptedstring, boolean, time, base64,
      // anyType, address, location, and any future SF types.
      return 'Text'
  }
}

function dateFormatFor(sfType: string): string {
  // `date` is date-only on the wire; `datetime` matches SF's native
  // datetime serialisation (`+0000` offset — the form CRMA's `XXX` token
  // accepts). See domain/metadata-synthesizer.ts comments for the wire probe
  // that grounded this choice.
  return sfType === 'date' ? 'yyyy-MM-dd' : "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
}
