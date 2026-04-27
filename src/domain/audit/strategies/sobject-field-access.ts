import { type AuditCheckStrategy, pass } from '../audit-strategy.js'

// Custom payload type: the union of all reader fields that any entry reads
// from this (sourceOrg, sObject) pair. The merge step accumulates fields
// across entries so a single SOQL probe covers every column the running user
// must be able to read.
type SObjectFieldsPayload = readonly string[]

export const sobjectFieldAccess: AuditCheckStrategy<SObjectFieldsPayload> = {
  select: e => (e.sObject ? [{ org: e.sourceOrg, key: e.sObject }] : []),
  merge: (existing, entry) => {
    // SObject entries always carry readerFields (commands layer); the
    // existing-undefined branch only fires for the first contributing
    // entry, which the test suite covers.
    /* v8 ignore next */
    const fields = entry.readerFields ?? []
    return unionPreserveOrder(existing ?? [], fields)
  },
  label: (org, key) => `${org}: ${key} read access`,
  // sObject values are validated against SF_IDENTIFIER_PATTERN, and reader
  // fields against SOQL_RELATIONSHIP_PATH_PATTERN, at config parse boundary
  // (config-loader.ts) — safe to interpolate.
  evaluate: async (sfPort, sObject, fields) => {
    await sfPort.query(buildFlsProbeSoql(sObject, fields))
    return pass()
  },
}

function buildFlsProbeSoql(
  sObject: string,
  fields: SObjectFieldsPayload
): string {
  /* v8 ignore next 2 -- SObject entries always carry readerFields (commands layer); fallback is defensive */
  const projection = fields.length > 0 ? fields.join(', ') : 'Id'
  return `SELECT ${projection} FROM ${sObject} LIMIT 1 WITH SECURITY_ENFORCED`
}

function unionPreserveOrder(
  base: readonly string[],
  extra: readonly string[]
): readonly string[] {
  const seen = new Set(base)
  const merged = [...base]
  for (const f of extra) {
    if (seen.has(f)) continue
    seen.add(f)
    merged.push(f)
  }
  return merged
}
