import { type QueryResult } from '../../../ports/types.js'
import {
  type AuditCheckStrategy,
  pass,
  selectByDataset,
  warn,
} from '../audit-strategy.js'

// Bootstrap is always-on: when no prior completed load exists, the writer
// synthesises metadata and creates the dataset on first POST. The audit
// surfaces this as a visible WARN rather than a hard FAIL so a typo in
// `targetDataset` is loud (printed next to its peers in the dry-run output)
// but doesn't block intentional first-time uploads. See
// `docs/design/2026-05-18-bootstrap-new-dataset-upload.md` §Audit integration.
export const datasetReady: AuditCheckStrategy = {
  select: selectByDataset,
  label: (org, key) => `${org}: dataset '${key}' ready`,
  evaluate: async (sfPort, key) => {
    const result: QueryResult<unknown> = await sfPort.query(
      `SELECT MetadataJson FROM InsightsExternalData WHERE EdgemartAlias = '${key}' AND Status IN ('Completed', 'CompletedWithWarnings') ORDER BY CreatedDate DESC LIMIT 1`
    )
    return result.records.length > 0
      ? pass()
      : warn(`Dataset '${key}' will be created on first load (bootstrap)`)
  },
}
