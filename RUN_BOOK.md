<!-- markdownlint-disable MD013 -- operational run-book uses long single-line commands and log excerpts -->
# Run Book: Dataset Loader

## Table of Contents

- [Overview](#overview)
- [Prerequisites & Setup](#prerequisites--setup)
- [Scheduling with Cron](#scheduling-with-cron)
- [Running Manually](#running-manually)
- [Date-Bounded Loads (`--start-date` / `--end-date`)](#date-bounded-loads---start-date----end-date)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Disaster Recovery](#disaster-recovery)
- [Multi-Environment Setup](#multi-environment-setup)
- [Reference](#reference)

## Overview

The Dataset Loader is an SF CLI plugin that extracts Salesforce Event Log Files (ELF) and SObject data from source orgs and writes them either into CRM Analytics datasets (CRM Analytics target) or to local CSV files (file target). It runs outside the Salesforce platform to avoid governor limits, uses a streaming pipeline for memory-bounded processing, supports parallel fetching with gzip compression for CRM Analytics uploads, and tracks ingestion progress through a JSON-based watermark system. See the [README](README.md) for full command usage and config format.

## Prerequisites & Setup

### Required Tools

| Tool   | Purpose                             | Version Check    |
|--------|-------------------------------------|------------------|
| `sf`   | Salesforce CLI — org authentication | `sf --version`   |
| `node` | Node.js runtime (>= 18)             | `node --version` |

### Installation

```bash
cd /path/to/dataset-loader
npm install
npm run build
sf plugins link .
```

Verify the plugin is registered:

```bash
sf dataset load --help
```

### Org Authentication

Authenticate both the source org (where data lives) and the analytic org (where CRM Analytics datasets live):

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

| Org                      | Required Permissions                                                             |
|--------------------------|----------------------------------------------------------------------------------|
| **Source org** (ELF)     | `API Enabled`, `View Event Log Files` (`ViewEventLogFiles`)                      |
| **Source org** (SObject) | `API Enabled`, plus **Read** on every SObject listed in any entry's `sObject` field (including standard objects like `User`, `UserLogin`, etc.). License-gated objects (Event Monitoring, etc.) need the relevant feature license. |
| **Analytic org**         | `API Enabled`, `Upload External Data to CRM Analytics` (`InsightsAppUploadUser`) |

Assign via Permission Set or Profile. The `sf` CLI alias must authenticate as a user with these permissions.

### Config File Setup

Create `dataset-load.config.json` at the project root. See the [README](README.md#config-format) for the full schema. Minimal example:

```json
{
  "entries": [
    {
      "eventLog": "Login",
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

```bash
0 6 * * * cd /path/to/dataset-loader && sf dataset load >> /var/log/dataset-loader.log 2>&1
```

For JSON-structured logs:

```bash
0 6 * * * cd /path/to/dataset-loader && sf dataset load --json >> /var/log/dataset-loader.json 2>&1
```

## Running Manually

### Pre-flight Validation

Check auth, connectivity, InsightsExternalData write permissions (analytic orgs), and ViewEventLogFiles access (ELF source orgs) without touching data:

```bash
sf dataset load --audit
```

### Dry Run

Preview what would be fetched and uploaded:

```bash
sf dataset load --dry-run
```

### Full Run

Process all entries:

```bash
sf dataset load
```

With a custom config:

```bash
sf dataset load --config-file path/to/config.json --state-file path/to/state.json
```

### Single Entry

Test one entry by its name:

```bash
sf dataset load --entry login-events
```

### JSON Output

Get structured output for scripting:

```bash
sf dataset load --json
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

```bash
Processing ████████████████████░░░░░░░░░░░░░░░░░░░░ 2/5 items | 3s elapsed
  ElfDS1 (06V000000000001) — 2 files, 4992 rows → 1 part
  AcctDS (06V000000000002) — 1 file, 150 rows → 0 parts
```

Progress bars are displayed only in TTY mode and suppressed with `--json`.

### Output Summary

On completion, the command logs:

```bash
Done: 3 processed, 1 skipped, 0 failed, 2 groups uploaded
```

| Metric          | Meaning                                             |
|-----------------|-----------------------------------------------------|
| processed       | Entries that fetched and streamed data successfully |
| skipped         | Entries with no new records since last watermark    |
| failed          | Entries that encountered errors                     |
| groups uploaded | Distinct CRM Analytics upload jobs completed        |

## Troubleshooting

### "No new records, skipping"

- **Symptom**: Log shows `No new records, skipping` and the watermark does not change.
- **Cause**: No records exist newer than the current watermark. Normal if no new data since the last run.
- **Resolution**: No action required. For ELF entries, verify the `interval` matches your license — `Hourly` requires Shield/Event Monitoring Analytics. Check `eventLog` matches a valid [EventLogFile EventType](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile.htm).

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
  sf dataset load --audit
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

### Audit FAIL: "{org}: {sObject} read access"

- **Symptom**: `[FAIL] <org>: <sObject> read access: ...` in audit output. Often paired with a Salesforce error like `INSUFFICIENT_ACCESS`, `INVALID_TYPE`, or `sObject type '<name>' is not supported`.
- **Cause**: The authenticated user lacks read permission on the queried SObject in the source org. The `sobjectReadAccess` strategy issues `SELECT Id FROM <sObject> LIMIT 1`; anything that rejects this query surfaces here — CRUD-level denial, a missing profile assignment, a standard object disabled for the user's license, or a typo in the config's `sObject` value.
- **Resolution**:
  1. Confirm the SObject name is spelled correctly (case-sensitive API name, including `__c` suffix for custom objects).
  2. In Setup → Users, find the CLI user and verify their Profile and assigned Permission Sets grant **Read** on the object.
  3. For standard objects controlled by license (e.g. `UserLogin`, `Audit*`, event-monitoring objects), the user needs the relevant feature license — check via "View Setup and Configuration" + the object-specific license (e.g. Event Monitoring).
  4. Re-run `sf dataset load --audit` — the check should flip to `[PASS]`.
- **Related**: `API Enabled` must also be on the user's profile (required for any SOQL call); verified by the `auth and connectivity` audit check, which fails earlier if missing.

### "No existing metadata for dataset, skipping"

- **Symptom**: Log shows `No existing metadata for dataset '<name>', skipping` and the entry is skipped.
- **Cause**: The CRM Analytics target dataset has no prior completed upload, so metadata cannot be resolved. This only affects CRM Analytics targets (`targetOrg` set); file targets are always writable.
- **Resolution**: Create the dataset manually via the CRM Analytics UI (Analytics Studio > Data Manager) or perform a one-time dataflow upload first, then re-run.

### "Schema mismatch for dataset" (audit FAIL or writer SkipDatasetError)

- **Symptom**: `Schema mismatch for dataset '<name>' (entry '<label>'): expected by dataset, missing from input: [...]` or `provided by input, not in dataset: [...]`.
- **Cause**: Your source columns don't match the dataset's canonical column set. For SObject entries, the config `fields` (after `.` → `_` translation) plus `augmentColumns` keys must equal the dataset metadata's `objects[0].fields[*].fullyQualifiedName` set, case-insensitively. For ELF/CSV, the same check applies against `LogFileFieldNames` (ELF) or the CSV file header.
- **Resolution**:
  - If a field is missing from the source: add it to `fields` (SObject) or re-extract the source (ELF/CSV).
  - If a field is extra: remove it from `fields` or from `augmentColumns`.
  - If the dataset has columns you didn't intend: recreate the dataset from a corrected initial load. The dataset's metadata freezes on first successful load; the loader will not add or remove columns.

### "Order mismatch for dataset" (ELF/CSV only)

- **Symptom**: `Order mismatch for dataset '<name>' (entry '<label>'): position N: dataset expects 'A', input provides 'B'`.
- **Cause**: The source column **set** matches the dataset's, but their **order** differs. ELF and CSV rows are streamed as-is — we don't reorder them at runtime because parsing every CSV line would double the hot-path cost. CRM Analytics interprets rows by position, so any order diff corrupts every row in exactly the way your dataflow digest reports: values shift into wrong columns. (SObject entries are safe: the loader reorders per-row at runtime.)
- **Resolution**:
  - ELF: the dataset metadata must match the current `LogFileFieldNames` order for the EventType. If Salesforce reordered the event schema in a release, recreate the dataset from a fresh initial load.
  - CSV: either reorder the source file's columns to match the dataset, or recreate the dataset from the source file.
  - Augment columns (if declared) must appear at the **trailing** positions of the dataset's metadata; that's a constraint of the append-suffix middleware ELF/CSV use.

### Audit WARN: "no prior EventLogFile" / "casing differs"

- **Symptom**: Audit line logs `[WARN]` rather than `[PASS]` or `[FAIL]`.
- **Cause**: Non-blocking advisory:
  - `No prior EventLogFile for <type>/<interval>; schema check skipped` — there's no blob to compare against. A real run would load zero rows anyway (no data to check).
  - `Schema casing differs from dataset metadata; dataset will keep its canonical casing` — source column names differ only in letter case from the dataset's; CRM Analytics keeps the dataset's canonical casing, so this is harmless but flagged for visibility.
- **Resolution**: No action required. WARN does not set a non-zero exit code.

### "Cannot share SObject reader across sinks with divergent projections"

- **Symptom**: `Cannot share SObject reader across sinks with divergent projections; split the config entries so their readerKeys differ`. Every entry in the bundle is reported as failed.
- **Cause**: Two SObject entries share a reader (identical `sourceOrg`, `sObject`, `fields`, `dateField`, `where`, `limit`) but target datasets whose metadata column orders differ. The pipeline can't satisfy both with a single reader stream.
- **Resolution**: Perturb one entry's `ReaderKey` so the readers are distinct — change `dateField`, add a trivial `where` filter, or adjust `limit`. This yields two independent readers, each with its own projection.

### "field-count and header's column-count do not match"

- **Symptom**: CRM Analytics reports `field-count, N, and header's column-count, 1, do not match` after upload.
- **Cause**: The dataset metadata is missing `fieldsEnclosedBy: '"'`, so CRM Analytics cannot parse the quoted CSV correctly.
- **Resolution**: Delete the dataset in the CRM Analytics UI, remove its watermark from `.dataset-load.state.json`, and re-upload with corrected metadata.

### Dataset Processing Stuck

- **Symptom**: Data does not appear in CRM Analytics after a successful upload.
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

1. Open `.dataset-load.state.json`
2. Delete the key for the target entry (or delete the entire file for a full reset)
3. Re-run `sf dataset load`

Note: ELF entries without a watermark fetch every available log file ascending — use `--start-date` or pre-seed the state file to cap initial loads. SObject entries without a watermark fetch all matching records — use the `limit` config field or `--start-date` on the CLI to cap.

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

1. Delete the dataset in the CRM Analytics UI (Analytics Studio > Data Manager > Datasets)
2. Remove the corresponding watermark(s) from `.dataset-load.state.json`
3. Run `sf dataset load`

Re-create the dataset via the CRM Analytics UI or a one-time dataflow, then re-run the loader. The loader requires existing metadata from a prior completed upload.

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
sf dataset load --audit
```

### Rebuilding Config

If `dataset-load.config.json` is lost, recreate from the [README config format](README.md#config-format). The state file (`.dataset-load.state.json`) is independent — existing watermarks will continue working with a new config.

## Multi-Environment Setup

Use separate config and state files per environment:

```bash
sf dataset load -c configs/prod.json -s state/prod.state.json
sf dataset load -c configs/staging.json -s state/staging.state.json
```

## Date-Bounded Loads (`--start-date` / `--end-date`)

Run-scoped date bounds via CLI flags. Applies to SObject and ELF
entries; CSV ignores the flags. `--start-date` always overrides the watermark
when set. See the README's "Date bounds" section for the full
semantics; this section covers operational recipes.

### Pattern 1 — Backfill a past window (isolated from main state)

Use when: replaying records in a date range without affecting the
main state file's watermark. Avoids REWIND regression on the
production run.

```bash
cp .dataset-load.state.json .dataset-load.backfill.state.json
sf dataset load \
    --state-file .dataset-load.backfill.state.json \
    --start-date 2026-01-01T00:00:00.000Z \
    --end-date   2026-01-31T23:59:59.999Z
rm .dataset-load.backfill.state.json
```

The main state file is untouched; incremental runs resume where
they were. Do **not** merge the backfill state file back into main.

### Pattern 2 — Fill a HOLE left by a previous `--start-date` after watermark run

Use when: a previous run with `--start-date > watermark` skipped
records in the gap `(previous-watermark, previous-start-date)` and
you want those records loaded. The main watermark has jumped past
the gap, so a naive incremental run will never pick them up.

```bash
# 1. Identify the previous watermark and the --start-date used in the
#    offending run (from the HOLE warning or prior logs).
# 2. Copy the main state file.
cp .dataset-load.state.json .dataset-load.hole-fill.state.json
# 3. Edit the copy: reset the entry's watermark to the previous
#    watermark. The WatermarkKey for SObject is
#    `<sourceOrg>:sobject:<sObject>`, for ELF it's
#    `<sourceOrg>:elf:<eventLog>:<interval>`, or the entry's `name`
#    if set.
# 4. Run with the hole-fill state file, capping at the offending
#    --start-date value.
sf dataset load \
    --state-file .dataset-load.hole-fill.state.json \
    --end-date <previous-start-date>
# 5. Discard the hole-fill state file.
rm .dataset-load.hole-fill.state.json
```

Caveat: the gap records must still exist in the source system. If
deleted since the offending run, there is no recovery — the HOLE
warning at the time was the only chance to notice.

### Warning reference

At most one warning per entry when bounds are set:

| Warning  | Condition                              | Consequence |
|----------|-----------------------------------------|-------------|
| REWIND   | `--start-date < watermark`              | Previously-loaded records re-loaded; watermark may regress. |
| HOLE     | `--start-date > watermark`              | Records in the gap skipped; not back-filled by future incremental runs. |
| BOUNDARY | `--start-date == watermark`, Append     | Boundary record appended a second time (duplicate row). Silent under Overwrite. |
| EMPTY    | `--end-date < watermark`, no `--start-date` | Query window is empty; no records will load. |

Invalid input (bad ISO format, bad calendar date like `2026-02-30`,
or `start-date > end-date`) aborts the run before any mode branch —
surfaces identically under `--audit`, `--dry-run`, or a real run.

### ELF first-run behaviour

On a first ELF run (no watermark, no `--start-date`), the reader
loads every available log file ascending. To cap initial load size,
pass `--start-date <recent-iso>` or pre-seed the state file with a
recent `LogDate`.

## Reference

### Exit Codes

| Code | Meaning                                                            |
|------|--------------------------------------------------------------------|
| `0`  | All entries processed successfully                                 |
| `1`  | Partial success (some entries failed, some succeeded)              |
| `2`  | Fatal error (config invalid, all entries failed, or audit failure) |

### Resilience

| Feature             | Behavior                                                                                                                                                     |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Per-org concurrency | 25 concurrent API calls per org via `p-limit` (shared across all entries for that org)                                                                       |
| Entry processing    | Concurrent entries within each dataset group                                                                                                                 |
| HTTP 429 retry      | Exponential backoff, 3 attempts max (1s, 2s, 4s)                                                                                                             |
| Partial failure     | Failed entries don't block others; watermarks only advance on success                                                                                        |
| Atomic state writes | Temp file + rename prevents corruption on crash                                                                                                              |
| Streaming           | Memory-bounded processing via async iterables; CRM Analytics targets use chunked uploads (10 MB gzip-compressed parts), file targets stream directly to disk |
