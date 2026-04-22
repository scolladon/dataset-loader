import {
  type ConfigEntry,
  isCsvEntry,
  isElfEntry,
  isSObjectEntry,
} from '../adapters/config-loader.js'
import { type ReaderPort, type SalesforcePort } from '../ports/types.js'
import { parseCsvHeader } from './column-name.js'

// Resolves the source-column list ("provided fields") for an entry, used by
// the audit phase and the pipeline alignment builder.
//
// - SObject: returns the config-declared field list directly.
// - CSV: reuses the already-built CsvReader (header() is memoised, so a full
//   pipeline run triggers at most one filesystem read).
// - ELF: queries LogFileFieldNames from the most recent EventLogFile of the
//   given type/interval. Errors are swallowed — audit is the authoritative
//   place to surface auth / connectivity problems. Returning an empty list
//   lets the writer-init short-circuit the schema check and defers the
//   per-entry failure to the subsequent fetch() call.
//
// Safe interpolation note: ELF's `eventLog` is pattern-validated by
// SF_IDENTIFIER_PATTERN (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`) at config parse and
// `interval` is a Zod enum of {Daily, Hourly} — both exclude the single
// quote that would enable SOQL injection.
export async function resolveProvidedFields(
  entry: ConfigEntry,
  fetcher: ReaderPort,
  sfPorts: ReadonlyMap<string, SalesforcePort>
): Promise<readonly string[]> {
  if (isSObjectEntry(entry)) {
    return entry.fields
  }
  if (isCsvEntry(entry)) {
    return parseCsvHeader(await fetcher.header())
  }
  /* v8 ignore next 3 -- exhaustive discriminator; unreachable */
  if (!isElfEntry(entry)) {
    throw new Error('unknown entry kind')
  }
  const srcPort = sfPorts.get(entry.sourceOrg)
  /* v8 ignore next 2 -- srcPort presence is validated before this call runs */
  if (!srcPort) throw new Error(`No SF connection for org '${entry.sourceOrg}'`)
  let raw: string | null | undefined
  try {
    const result = await srcPort.query<{
      LogFileFieldNames: string | null
    }>(
      `SELECT LogFileFieldNames FROM EventLogFile WHERE EventType = '${entry.eventLog}' AND Interval = '${entry.interval}' ORDER BY LogDate DESC LIMIT 1`
    )
    raw = result.records[0]?.LogFileFieldNames
  } catch {
    return []
  }
  return raw ? parseCsvHeader(raw) : []
}
