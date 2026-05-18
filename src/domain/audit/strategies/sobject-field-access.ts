import { type AuditCheckStrategy, pass, warn } from '../audit-strategy.js'

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
    // Stryker disable next-line ArrayDeclaration: equivalent — readerFields is guaranteed by upstream.
    /* v8 ignore next */
    const fields = entry.readerFields ?? []
    return unionPreserveOrder(existing ?? [], fields)
  },
  label: (org, key) => `${org}: ${key} read access`,
  // sObject values are validated against SF_IDENTIFIER_PATTERN, and reader
  // fields against SOQL_RELATIONSHIP_PATH_PATTERN, at config parse boundary
  // (config-loader.ts) — safe to interpolate.
  evaluate: async (sfPort, sObject, fields) => {
    try {
      await sfPort.query(buildFlsProbeSoql(sObject, fields, true))
      return pass()
    } catch (err) {
      if (!isSecurityEnforcedUnsupported(err)) throw err
      // Some entities (e.g. User, UserLogin) reject WITH SECURITY_ENFORCED.
      // Re-probe without it so we still verify the entity and field set are
      // queryable, and surface a WARN so callers know FLS wasn't enforced.
      await sfPort.query(buildFlsProbeSoql(sObject, fields, false))
      return warn(
        `FLS not enforced for ${sObject}: WITH SECURITY_ENFORCED unsupported on this entity`
      )
    }
  },
}

function buildFlsProbeSoql(
  sObject: string,
  fields: SObjectFieldsPayload,
  enforceFls: boolean
): string {
  // Stryker disable next-line all: equivalent — defensive `:'Id'` branch is unreachable per v8 ignore.
  /* v8 ignore next 2 -- SObject entries always carry readerFields (commands layer); fallback is defensive */
  const projection = fields.length > 0 ? fields.join(', ') : 'Id'
  const fls = enforceFls ? ' WITH SECURITY_ENFORCED' : ''
  return `SELECT ${projection} FROM ${sObject}${fls} LIMIT 1`
}

function isSecurityEnforcedUnsupported(err: unknown): boolean {
  return (
    err instanceof Error &&
    err.message.includes('SECURITY_ENFORCED not allowed in this context')
  )
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
