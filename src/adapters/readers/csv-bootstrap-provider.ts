import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import { parseCsvHeader } from '../../domain/column-name.js'
import { type SourceSchema } from '../../domain/metadata-types.js'
import {
  appendAugmentFields,
  readerTextField,
} from '../../domain/source-schema-builder.js'
import type { BootstrapMetadataProvider } from '../../ports/types.js'

export interface CsvBootstrapInput {
  readonly filePath: string
  readonly datasetName: string
  readonly augmentColumns: Readonly<Record<string, string>>
}

// CSV bootstrap is always all-Text. Source-side type inference is a UX trap
// (locale-dependent dates, ambiguous nullability, leading zeros). Wrong-type
// at the dataset level is unrecoverable without rebuild; staying Text means
// users can opt into typed schemas later via overrideMetadata when CRMA's
// per-column types matter for their dashboards.
export class CsvBootstrapProvider implements BootstrapMetadataProvider {
  constructor(private readonly input: CsvBootstrapInput) {}

  async buildSourceSchema(): Promise<SourceSchema> {
    const header = await this.readHeaderLine()
    const columns = parseCsvHeader(header)
    if (columns.length === 0) {
      throw new Error(
        `CSV bootstrap requires at least one header line in '${this.input.filePath}'`
      )
    }
    const fields = columns.map(readerTextField)
    appendAugmentFields(fields, this.input.augmentColumns)
    return {
      datasetName: this.input.datasetName,
      label: this.input.datasetName,
      fields,
    }
  }

  // Reads only the first line — never loads the rest of the file.
  private async readHeaderLine(): Promise<string> {
    const stream = createReadStream(this.input.filePath)
    const rl = createInterface({ input: stream, crlfDelay: Infinity })
    try {
      for await (const line of rl) {
        return line
      }
      return ''
    } finally {
      rl.close()
      stream.destroy()
    }
  }
}
