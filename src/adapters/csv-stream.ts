import { type Readable } from 'node:stream'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify/sync'

const CSV_OPTIONS = { quoted: true, quoted_empty: true } as const

function serializeRow(fields: readonly string[]): string {
  return stringify([fields as string[]], CSV_OPTIONS)
}

export class CsvStream {
  private _headersEmitted = false

  get headersEmitted(): boolean {
    return this._headersEmitted
  }

  async *transform(
    streams: AsyncIterable<Readable>,
    augmentColumns: Record<string, string>
  ): AsyncGenerator<string> {
    const augmentKeys = Object.keys(augmentColumns)
    const augmentValues = augmentKeys.map(k => augmentColumns[k])

    for await (const stream of streams) {
      const parser = parse({ relax_column_count: true })
      stream.pipe(parser)

      try {
        let isFirstRow = true

        for await (const row of parser) {
          const record = row as string[]

          if (isFirstRow) {
            isFirstRow = false

            if (!this._headersEmitted) {
              const augmentedHeader = [...record, ...augmentKeys]
              yield serializeRow(augmentedHeader)
              this._headersEmitted = true
            }

            continue
          }

          const augmentedRow = [...record, ...augmentValues]
          yield serializeRow(augmentedRow)
        }
      } finally {
        parser.destroy()
        stream.destroy()
      }
    }
  }
}
