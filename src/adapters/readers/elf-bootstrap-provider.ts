import { parseCsvHeader } from '../../domain/column-name.js'
import { type SourceSchema } from '../../domain/metadata-types.js'
import {
  appendAugmentFields,
  readerTextField,
} from '../../domain/source-schema-builder.js'
import type {
  BootstrapMetadataProvider,
  QueryResult,
  SalesforcePort,
} from '../../ports/types.js'

export interface ElfBootstrapInput {
  readonly eventType: string
  readonly interval: 'Daily' | 'Hourly'
  readonly datasetName: string
  readonly augmentColumns: Readonly<Record<string, string>>
}

interface EventLogFileHeaderRecord {
  LogFileFieldNames: string | null
}

// ELF columns are textual on the wire (every EventLogFile cell arrives as a
// string regardless of semantic). Mapping every ELF column to CRMA `Text` is
// the safe canonical bootstrap default — users wanting typed columns can
// `overrideMetadata: true` after hand-tuning the dataset in Analytics Studio,
// or wait for a sidecar feature.
export class ElfBootstrapProvider implements BootstrapMetadataProvider {
  constructor(
    private readonly sfPort: SalesforcePort,
    private readonly input: ElfBootstrapInput
  ) {}

  async buildSourceSchema(): Promise<SourceSchema> {
    const header = await this.queryLatestHeader()
    if (!header) {
      throw new Error(
        `ELF bootstrap requires at least one historical log for EventType='${this.input.eventType}' Interval='${this.input.interval}' — no prior EventLogFile row exists`
      )
    }
    const columns = parseCsvHeader(header)
    const fields = columns.map(readerTextField)
    appendAugmentFields(fields, this.input.augmentColumns)
    return {
      datasetName: this.input.datasetName,
      label: this.input.datasetName,
      fields,
    }
  }

  private async queryLatestHeader(): Promise<string | null> {
    // Safe interpolation: eventType is sfIdentifier and interval is
    // z.enum(['Daily','Hourly']) — both validated by the config-loader
    // before they reach here. Neither admits a single quote.
    const soql = `SELECT LogFileFieldNames FROM EventLogFile WHERE EventType = '${this.input.eventType}' AND Interval = '${this.input.interval}' ORDER BY LogDate DESC LIMIT 1`
    const result: QueryResult<EventLogFileHeaderRecord> =
      await this.sfPort.query(soql)
    return result.records[0]?.LogFileFieldNames ?? null
  }
}
