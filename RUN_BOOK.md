# Run Book: CRMA Data Loader

## Overview

The CRMA Data Loader is an SF CLI plugin that extracts Salesforce Event Log Files (ELF) and SObject data from source orgs and writes them either into CRM Analytics datasets (CRMA target) or to local CSV files (file target). It runs outside the Salesforce platform to avoid governor limits, uses a streaming pipeline for memory-bounded processing, supports parallel fetching with gzip compression for CRMA uploads, and tracks ingestion progress through a JSON-based watermark system. See the [README](README.md) for full command usage and config format.

## Prerequisites & Setup

### Required Tools

| Tool | Purpose | Version Check |
| --- | --- | --- |
| `sf` | Salesforce CLI — org authentication | `sf --version` |
| `node` | Node.js runtime (>= 18) | `node --version` |

### Installation

```bash
cd /path/to/crma-data-loader
npm install
npm run build
sf plugins link .
```

Verify the plugin is registered:

```bash
sf crma load --help
```

### Org Authentication

Authenticate both the source org (where data lives) and the analytic org (where CRMA datasets live):

```bash
sf org login web --alias my-source-org
sf org login web --alias my-analytic-org
```

Verify each alias:

```bash
sf org display --target-org my-source-org
sf org display --target-org my-analytic-org
```

### Required Permissions

| Org | Required Permissions |
| --- | --- |
| **Source org** (ELF) | `API Enabled`, `View Event Log Files` (`ViewEventLogFiles`) |
| **Source org** (SObject) | `API Enabled` |
| **Analytic org** | `API Enabled`, `Upload External Data to CRM Analytics` (`InsightsAppUploadUser`) |

Assign via Permission Set or Profile. The `sf` CLI alias must authenticate as a user with these permissions.

### Config File Setup

Create `crma-load.config.json` at the project root. See the [README](README.md#config-format) for the full schema. Minimal example:

```json
{
  "entries": [
    {
      "type": "elf",
      "eventType": "Login",
      "interval": "Daily",
      "sourceOrg": "my-source-org",
      "targetOrg": "my-analytic-org",
      "targetDataset": "Login_my_source_org"
    }
  ]
}
```

## Scheduling with Cron

### Recommended Cron Entry

Run daily at 6:00 AM (after Salesforce generates daily log files around 3:00 AM):

```
0 6 * * * cd /path/to/crma-data-loader && sf crma load >> /var/log/crma-loader.log 2>&1
```

For JSON-structured logs:

```
0 6 * * * cd /path/to/crma-data-loader && sf crma load --json >> /var/log/crma-loader.json 2>&1
```

## Running Manually

### Pre-flight Validation

Check auth, connectivity, InsightsExternalData write permissions (analytic orgs), and ViewEventLogFiles access (ELF source orgs) without touching data:

```bash
sf crma load --audit
```

### Dry Run

Preview what would be fetched and uploaded:

```bash
sf crma load --dry-run
```

### Full Run

Process all entries:

```bash
sf crma load
```

With a custom config:

```bash
sf crma load --config-file path/to/config.json --state-file path/to/state.json
```

### Single Entry

Test one entry by its name:

```bash
sf crma load --entry login-events
```

### JSON Output

Get structured output for scripting:

```bash
sf crma load --json
```

Returns:

```json
{
  "status": 0,
  "result": {
    "entriesProcessed": 3,
    "entriesSkipped": 1,
    "entriesFailed": 0,
    "groupsUploaded": 2
  }
}
```

## Monitoring

### Progress Bars

During execution, a main progress bar tracks overall entry progress, with per-group sub-bars showing real-time fetch and upload stats:

```
Processing ████████████████████░░░░░░░░░░░░░░░░░░░░ 2/5 items | 3s elapsed
  ElfDS1 (06V000000000001) — 2 files, 4992 rows → 1 part
  AcctDS (06V000000000002) — 1 file, 150 rows → 0 parts
```

Progress bars are displayed only in TTY mode and suppressed with `--json`.

### Output Summary

On completion, the command logs:

```
Done: 3 processed, 1 skipped, 0 failed, 2 groups uploaded
```

| Metric | Meaning |
|--------|---------|
| processed | Entries that fetched and streamed data successfully |
| skipped | Entries with no new records since last watermark |
| failed | Entries that encountered errors |
| groups uploaded | Distinct CRMA upload jobs completed |

## Troubleshooting

### "No new records, skipping"

- **Symptom**: Log shows `No new records, skipping` and the watermark does not change.
- **Cause**: No records exist newer than the current watermark. Normal if no new data since the last run.
- **Resolution**: No action required. For ELF entries, verify the `interval` matches your license — `Hourly` requires Shield/Event Monitoring Analytics. Check `eventType` matches a valid [EventLogFile EventType](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile.htm).

### "Config loading failed"

- **Symptom**: Command exits immediately with a config validation error.
- **Cause**: Invalid JSON, missing required fields, or conflicting operations in a group.
- **Resolution**: Check the error message — Zod provides specific validation details. Ensure entries targeting the same `(targetOrg, targetDataset)` or `targetFile` use the same `operation`.

### Auth Errors (401/403)

- **Symptom**: Fetch or upload fails with HTTP 401 or 403.
- **Cause**: Expired access token (401) or missing permissions (403).
- **Resolution**:
  ```bash
  # Re-authenticate
  sf org login web --alias <alias>

  # Verify
  sf crma load --audit
  ```
  For headless environments, use `sf org login jwt`. For 403, verify permissions in the [Prerequisites](#required-permissions) section.

### HTTP 429 (Rate Limit)

- **Symptom**: Requests are retried (logged as warnings).
- **Cause**: Salesforce API rate limit exceeded.
- **Resolution**: The plugin automatically retries with exponential backoff (up to 3 attempts per request). If persistent, reduce the number of entries or stagger runs.

### Upload Failures

- **Symptom**: `Upload failed` warning. Watermarks for the affected group are not advanced.
- **Cause**: Network issue or transient API failure during upload.
- **Resolution**: Re-run — watermarks are only advanced on success, so the same data will be re-fetched and re-uploaded.

### "No existing metadata for dataset, skipping"

- **Symptom**: Log shows `No existing metadata for dataset '<name>', skipping` and the entry is skipped.
- **Cause**: The CRMA target dataset has no prior completed upload, so metadata cannot be resolved. This only affects CRMA targets (`targetOrg` set); file targets are always writable.
- **Resolution**: Create the dataset manually via the CRMA UI (Analytics Studio > Data Manager) or perform a one-time dataflow upload first, then re-run.

### "field-count and header's column-count do not match"

- **Symptom**: CRMA reports `field-count, N, and header's column-count, 1, do not match` after upload.
- **Cause**: The dataset metadata is missing `fieldsEnclosedBy: '"'`, so CRMA cannot parse the quoted CSV correctly.
- **Resolution**: Delete the dataset in the CRMA UI, remove its watermark from `.crma-load.state.json`, and re-upload with corrected metadata.

### Dataset Processing Stuck

- **Symptom**: Data does not appear in CRMA after a successful upload.
- **Cause**: Salesforce processing is delayed or stuck.
- **Resolution**:
  1. Query upload status:
     ```bash
     sf data query --query "SELECT Id, Status, StatusMessage FROM InsightsExternalData WHERE EdgemartAlias='<dataset>' ORDER BY CreatedDate DESC LIMIT 5" --target-org <analytic_org>
     ```
  2. If stuck (not `Completed` or `Failed`), abort:
     ```bash
     sf api request rest --method PATCH --body '{"Action":"Delete"}' /services/data/v65.0/sobjects/InsightsExternalData/<record_id> --target-org <analytic_org>
     ```
  3. Re-run the loader.

### Watermark Not Advancing

- **Symptom**: State file watermarks remain unchanged after runs.
- **Cause**: Fetch or upload is failing before watermark update. Watermarks only advance after successful upload of the entire group.
- **Resolution**: Check command output for fetch or upload error messages. Run with `--entry <name>` to isolate the problem entry.

## Disaster Recovery

### Resetting a Watermark

To re-fetch from scratch:

1. Open `.crma-load.state.json`
2. Delete the key for the target entry (or delete the entire file for a full reset)
3. Re-run `sf crma load`

Note: ELF entries without a watermark fetch only the latest record (bootstrap mode). SObject entries without a watermark fetch all matching records — use the `limit` config field to cap initial loads.

To re-ingest from a specific point, set the watermark to an ISO 8601 date before the desired start (both `Z` and `+0000` offsets are accepted):

```json
{
  "watermarks": {
    "my-source-org:elf:Login:Daily": "2026-01-01T00:00:00.000+0000"
  }
}
```

### Recovering a Broken Dataset

If a dataset has corrupted metadata or an incompatible schema:

1. Delete the dataset in the CRMA UI (Analytics Studio > Data Manager > Datasets)
2. Remove the corresponding watermark(s) from `.crma-load.state.json`
3. Run `sf crma load`

Re-create the dataset via the CRMA UI or a one-time dataflow, then re-run the loader. The loader requires existing metadata from a prior completed upload.

### Expired or Rotated Auth

For interactive environments:

```bash
sf org login web --alias <alias>
```

For headless/CI environments:

```bash
sf org login jwt --client-id <client_id> --jwt-key-file <key_file> --username <username> --alias <alias>
```

Verify:

```bash
sf crma load --audit
```

### Rebuilding Config

If `crma-load.config.json` is lost, recreate from the [README config format](README.md#config-format). The state file (`.crma-load.state.json`) is independent — existing watermarks will continue working with a new config.

## Multi-Environment Setup

Use separate config and state files per environment:

```bash
sf crma load -c configs/prod.json -s state/prod.state.json
sf crma load -c configs/staging.json -s state/staging.state.json
```

## Reference

### Exit Codes

| Code | Meaning |
| --- | --- |
| `0` | All entries processed successfully |
| `1` | Partial success (some entries failed, some succeeded) |
| `2` | Fatal error (config invalid, all entries failed, or audit failure) |

### Resilience

| Feature | Behavior |
| --- | --- |
| Per-org concurrency | 25 concurrent API calls per org via `p-limit` (shared across all entries for that org) |
| Entry processing | Concurrent entries within each dataset group |
| HTTP 429 retry | Exponential backoff, 3 attempts max (1s, 2s, 4s) |
| Partial failure | Failed entries don't block others; watermarks only advance on success |
| Atomic state writes | Temp file + rename prevents corruption on crash |
| Streaming | Memory-bounded processing via async iterables; CRMA targets use chunked uploads (10 MB gzip-compressed parts), file targets stream directly to disk |
