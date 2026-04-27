import { type QueryResult } from '../../../ports/types.js'
import {
  type AuditCheckStrategy,
  fail,
  pass,
  selectByDataset,
} from '../audit-strategy.js'

export const datasetReady: AuditCheckStrategy = {
  select: selectByDataset,
  label: (org, key) => `${org}: dataset '${key}' ready`,
  evaluate: async (sfPort, key) => {
    // Fast path: verify at least one completed-status record exists. The
    // actual metadata blob is fetched by schemaAlignment when it runs.
    const result: QueryResult<unknown> = await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${key}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    return result.records.length > 0
      ? pass()
      : fail(`Dataset '${key}' has no prior metadata`)
  },
}
