<!-- markdownlint-disable MD013 MD033 MD040 -- long table rows, intentional <details> blocks, and oclif-generated command-usage fences without language tags -->
# Dataset Loader

[![Performance](https://img.shields.io/badge/Performance-Dashboard-58a6ff)](https://scolladon.github.io/dataset-loader/dev/bench/runtime/)

SF CLI plugin that loads Salesforce [Event Log Files](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile.htm) (ELF), SObject data, and CSV files into CRM Analytics datasets or local files using the [Analytics External Data API](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_ext_data.meta/bi_dev_guide_ext_data/bi_ext_data_object_externaldata.htm).

<p align="center">
  <img src="resources/combined_pipeline.gif" alt="Pipeline demo" />
</p>

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Config Reference](#config-reference)
- [Command Reference](#sf-dataset-load)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Prerequisites

- [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) (`sf`) with authenticated orgs
- Node.js >= 18

<details>
<summary>Required Permissions</summary>

| Org                      | Permissions                                                                      |
|--------------------------|----------------------------------------------------------------------------------|
| **Source org** (ELF)     | `API Enabled`, `View Event Log Files` (`ViewEventLogFiles`)                      |
| **Source org** (SObject) | `API Enabled`                                                                    |
| **Analytic org**         | `API Enabled`, `Upload External Data to CRM Analytics` (`InsightsAppUploadUser`) |

</details>

## Installation

```bash
sf plugins install dataset-loader
```

## Quick Start

```bash
# 1. Create a config file
touch dataset-load.config.json
```

```json
{
  "entries": [
    {
      "eventLog": "LightningPageView",
      "interval": "Daily",
      "sourceOrg": "my-source-org",
      "targetOrg": "my-analytic-org",
      "targetDataset": "LightningPageView_dataset_APIName"
    },
    {
      "csvFile": "./data/accounts-export.csv",
      "targetOrg": "my-analytic-org",
      "targetDataset": "ImportedAccounts"
    }
  ]
}
```

```bash
# 2. Verify auth and permissions
sf dataset load --audit

# 3. Preview the plan
sf dataset load --dry-run

# 4. Run
sf dataset load
```

> **CRM Analytics targets** — the dataset must already exist with at least one prior completed upload. Create it via the CRM Analytics UI or a one-time dataflow before the first load.

> **File targets** — omit `targetOrg` and set `targetFile` to a local file path. The file is created automatically.

> **Column alignment** — CRM Analytics ingests rows by position. `--audit` verifies your source columns match the dataset's metadata before any upload runs. SObject entries are auto-reordered at runtime; ELF/CSV source order must already match the dataset.

## Config Reference

### Common Fields

All entry types share these output fields:

| Field            | Required | Description                                                                         |
|------------------|----------|-------------------------------------------------------------------------------------|
| `name`           | no       | Optional entry identifier. Used as watermark key override and for --entry filtering |
| `targetOrg`      | no       | SF CLI alias of the CRM Analytics org. Omit to write to a local file instead        |
| `targetDataset`  | no       | CRM Analytics dataset API name. Required when `targetOrg` is set                    |
| `targetFile`     | no       | Local file path to write output. Required when `targetOrg` is omitted               |
| `operation`      | no       | `"Append"` (default) or `"Overwrite"`                                               |
| `augmentColumns` | no       | Extra columns to append to every row (see the Augment Columns details below)        |

> **Type inference:** Entry type is inferred from shape — entries with `eventLog` are ELF, entries with `sObject` are SObject, entries with `csvFile` are CSV.

### ELF Entry

```json
{
  "sourceOrg": "source-org-alias",
  "targetOrg": "analytic-org-alias",
  "targetDataset": "ALM_LightningPageView",
  "eventLog": "LightningPageView",
  "interval": "Daily",
  "augmentColumns": { "OrgId": "{{sourceOrg.Id}}" }
}
```

| Field       | Required | Description                                                  |
|-------------|----------|--------------------------------------------------------------|
| `sourceOrg` | yes      | SF CLI alias of the org containing EventLogFiles             |
| `eventLog`  | yes      | EventLogFile type (e.g. `Login`, `LightningPageView`, `API`) |
| `interval`  | yes      | `"Daily"` or `"Hourly"` (Hourly requires Shield license)     |

### SObject Entry

```json
{
  "sourceOrg": "source-org-alias",
  "targetOrg": "analytic-org-alias",
  "targetDataset": "ALM_Accounts",
  "sObject": "Account",
  "fields": ["Id", "Name", "Industry", "CreatedDate"],
  "where": "Industry != null",
  "augmentColumns": { "OrgId": "{{sourceOrg.Id}}" }
}
```

| Field       | Required | Description                                                |
|-------------|----------|------------------------------------------------------------|
| `sourceOrg` | yes      | SF CLI alias of the source org                             |
| `sObject`   | yes      | SObject API name (e.g. `Account`, `Opportunity`)           |
| `fields`    | yes      | Array of field API names to query                          |
| `dateField` | no       | Field used for watermarking (default: `LastModifiedDate`)  |
| `where`     | no       | Additional SOQL WHERE clause                               |
| `limit`     | no       | Max number of records to fetch (appends `LIMIT n` to SOQL) |

### CSV Entry

```json
{
  "csvFile": "./data/accounts-export.csv",
  "targetOrg": "analytic-org-alias",
  "targetDataset": "ALM_ImportedAccounts",
  "augmentColumns": { "Source": "ManualExport" }
}
```

| Field     | Required | Description                        |
|-----------|----------|------------------------------------|
| `csvFile` | yes      | Path to the local CSV file to load |

> **Note:** CSV entries only support static `augmentColumns` values. Dynamic `{{sourceOrg.*}}` / `{{targetOrg.*}}` expressions are not supported.

<details>
<summary>Augment Columns</summary>

Append static or dynamic columns to every row. Values support mustache-style `{{token}}` interpolation, including mixed with static text (e.g. `"PROD-{{sourceOrg.Name}}"`):

| Token                | Resolves to                                             |
|----------------------|---------------------------------------------------------|
| `{{sourceOrg.Id}}`   | 18-char Organization Id of the source org               |
| `{{sourceOrg.Name}}` | Organization Name of the source org                     |
| `{{targetOrg.Id}}`   | 18-char Organization Id of the target CRM Analytics org |
| `{{targetOrg.Name}}` | Organization Name of the target CRM Analytics org       |
| Any other string     | Used as-is (static value)                               |

Column names may contain dots (e.g. `"Org.Name"`) as CRM Analytics supports dotted dimension names.

</details>

<details>
<summary>Grouping</summary>

Entries targeting the same destination are merged into a single write job:

- **CRM Analytics targets**: same `(targetOrg, targetDataset)` → single `InsightsExternalData` upload
- **File targets**: same `targetFile` path → single file write

All entries in a group must use the same `operation`.

</details>

### State File & Watermarks

Watermarks are stored in a separate state file (`.dataset-load.state.json` by default; override with `--state-file`) to keep config declarative:

```json
{
  "watermarks": {
    "source-org-alias:elf:LightningPageView:Daily": "2026-03-05T00:00:00.000+0000",
    "source-org-alias:sobject:Account": "2026-03-05T14:30:00.000+0000"
  }
}
```

Watermark keys: `{sourceOrg}:elf:{eventLog}:{interval}`, `{sourceOrg}:sobject:{sObject}`, or `csv:{csvFile}`. Set `name` on an entry to use it as the key instead — lets you rename source orgs or change event types without losing watermark history.

First-run behaviour (no watermark yet):

- **ELF** — fetches every available log file ascending. Pass `--start-date` to cap the initial pull (see [Advanced Usage](#advanced-usage)).
- **SObject** — fetches all matching records. Use `limit` in the config or `--start-date` on the CLI to cap.
- **Subsequent runs** — fetch incrementally from the stored watermark.

<!-- commands -->
* [`sf dataset load`](#sf-dataset-load)

## `sf dataset load`

Load Event Log Files and SObject data into CRM Analytics datasets

```
USAGE
  $ sf dataset load [--json] [--flags-dir <value>] [-c <value>] [-s <value>] [--audit] [--dry-run] [--entry
    <value>] [--start-date <value>] [--end-date <value>]

FLAGS
  -c, --config-file=<value>  [default: dataset-load.config.json] Path to config JSON
  -s, --state-file=<value>   [default: .dataset-load.state.json] Path to watermark state file
      --audit                Pre-flight checks only (auth, connectivity, permissions)
      --dry-run              Show plan without executing
      --end-date=<value>     Load only records with dateField/LogDate <= this ISO-8601 datetime (ignored for CSV
                             entries)
      --entry=<value>        Process only the entry with this name
      --start-date=<value>   Load only records with dateField/LogDate >= this ISO-8601 datetime (ignored for CSV
                             entries)

GLOBAL FLAGS
  --flags-dir=<value>  Import flag values from a directory.
  --json               Format output as json.

EXAMPLES
  $ sf dataset load

  $ sf dataset load --config-file my-config.json --dry-run

  $ sf dataset load --start-date 2026-01-01T00:00:00.000Z --end-date 2026-01-31T23:59:59.999Z
```

_See code: [src/commands/dataset/load.ts](https://github.com/scolladon/dataset-loader/blob/main/src/commands/dataset/load.ts)_
<!-- commandsstop -->

## Advanced Usage

Everything about `--start-date` and `--end-date` — the flags that let you override or narrow the default incremental window. Both take ISO-8601 datetimes (e.g. `2026-01-15T00:00:00.000Z`), both are **inclusive**, and both are **ignored for CSV entries**.

The baseline is still incremental: the loader tracks a per-entry watermark in the state file and, by default, pulls rows strictly greater than it. The flags below are escape hatches.

### No flags — normal incremental

```bash
sf dataset load
```

Produces `WHERE dateField > <watermark>` (or no filter on the very first run for that entry). This is what you run in cron.

> ⚠️ **First-run volumes.** On a brand-new entry (no watermark yet, no `--start-date`), every row available in the source is pulled. For high-history orgs this can be a lot — especially for ELF, where it means *every log file ever*. Use **"Load from a cutoff"** below on the first run if that's a concern.

### Load from a cutoff — `--start-date` only

"Start loading from this date onward; skip older history."

```bash
sf dataset load --start-date 2026-01-01T00:00:00.000Z
```

```json
// .dataset-load.state.json — either missing or empty
{ "watermarks": {} }
```

Emits `WHERE dateField >= 2026-01-01T00:00:00.000Z`. After the run, the watermark advances to the last record's dateField and subsequent incremental runs work normally from there.

Typical use: onboarding a new entry without dragging in years of history; capping the initial ELF pull.

### Load up to a date — `--end-date` only

"Cap the load at this date."

```bash
sf dataset load --end-date 2026-03-31T23:59:59.999Z
```

Emits `WHERE dateField > <watermark> AND dateField <= 2026-03-31T23:59:59.999Z`. Useful for end-of-quarter archive runs. **The current watermark must be before `--end-date`** — otherwise the window is empty and you'll see an **EMPTY** warning.

### Load a specific window — both flags

"Load just the dates in this window."

```bash
sf dataset load \
  --start-date 2026-01-01T00:00:00.000Z \
  --end-date   2026-01-31T23:59:59.999Z
```

Emits `WHERE dateField >= 2026-01-01T00:00:00.000Z AND dateField <= 2026-01-31T23:59:59.999Z` — the watermark is overridden on the lower side.

This is the general "backfill / replay" shape. If the window is in the past of your current watermark, use the pattern below to avoid regressing the main watermark.

### Safe past-window backfill

Use when: redoing a range in the past without disturbing the production watermark. Pattern: isolate with a throwaway state file.

```bash
# 1. Copy the main state file.
cp .dataset-load.state.json .dataset-load.backfill.state.json

# 2. Run the backfill against the copy.
sf dataset load \
  --state-file .dataset-load.backfill.state.json \
  --start-date 2026-01-01T00:00:00.000Z \
  --end-date   2026-01-31T23:59:59.999Z

# 3. Discard the copy.
rm .dataset-load.backfill.state.json
```

Main state file is untouched; the next scheduled run resumes where it was. See [RUN_BOOK.md](RUN_BOOK.md) for the full recovery runbook.

### Ad-hoc export to a local CSV (forensics / compliance)

Pull a specific date range to a local file without touching CRM Analytics or the main state.

```bash
sf dataset load \
  --config-file forensic.config.json \
  --state-file /tmp/forensic.state.json \
  --start-date 2026-03-15T13:00:00.000Z \
  --end-date   2026-03-15T16:00:00.000Z
```

```json
// forensic.config.json — `targetFile` instead of `targetOrg` = write to local CSV
{
  "entries": [
    {
      "name": "accounts-incident",
      "sourceOrg": "prod",
      "targetFile": "/tmp/accounts-around-incident.csv",
      "sObject": "Account",
      "fields": ["Id", "Name", "LastModifiedDate"]
    }
  ]
}
```

Dedicated state file + `targetFile` = zero impact on main state and no CRM Analytics write.

### Warnings & gotchas

The command prints at most one warning per entry before running. Each tells you about a non-obvious consequence of the flag combination:

| Warning | Trigger | What it means | What to do |
|---|---|---|---|
| **REWIND** | `--start-date` < watermark | Previously-loaded records will be re-loaded; watermark may regress. | If deliberate, expected. To preserve the main watermark, use the **Safe past-window backfill** pattern above. Under `Append`, records in the window are duplicated. |
| **HOLE** | `--start-date` > watermark | Records with dateField in the gap will be skipped this run AND by subsequent incremental runs (the watermark jumps past the gap). | See [RUN_BOOK.md](RUN_BOOK.md) for the HOLE recovery recipe. |
| **BOUNDARY** | `--start-date` == watermark, `Append` | One boundary record gets appended again (duplicate row). | Bump `--start-date` by 1 ms. Silent under `Overwrite` (wholesale replace is idempotent). |
| **EMPTY** | `--end-date` < watermark (no `--start-date`) | Query window is empty; no records will load. | Drop `--end-date` or advance it. |

The `Append` vs `Overwrite` distinction matters most for **bounded past-window runs on the main dataset**: `Append` creates duplicates for any records already loaded in that window; `Overwrite` replaces the *entire* dataset with just the window (usually not what you want). Use the **Safe past-window backfill** pattern in either case.

## Troubleshooting

See [RUN_BOOK.md](RUN_BOOK.md) for operational recipes: scheduling with cron, recovery from bad state, pattern for filling a past-window backfill without disturbing main state, and pattern for recovering from a `HOLE` warning.

<details>
<summary>Exit Codes</summary>

| Code | Meaning                                                            |
|------|--------------------------------------------------------------------|
| `0`  | All entries processed successfully                                 |
| `1`  | Partial success (some entries failed, some succeeded)              |
| `2`  | Fatal error (config invalid, all entries failed, or audit failure) |

</details>

<details>
<summary>How It Works</summary>

1. **Parse config** — validate JSON with Zod, check operation consistency across groups
2. **Resolve** — authenticate orgs, resolve mustache tokens in augmentColumns
3. **Audit** (optional) — verify connectivity, EventLogFile access, InsightsExternalData write access, and dataset **schema alignment** (source columns match the target dataset's metadata)
4. **Execute pipeline** — group entries by dataset, stream through Reader → Projection (SObject, reorders to dataset column order) → Augment (ELF/CSV) → Writer
5. **Write** — CRM Analytics: gzip-compress, base64-encode, split into 10 MB parts, upload via InsightsExternalData API. File: stream rows directly to a local CSV
6. **Update watermarks** — only for entries whose group uploaded successfully

</details>

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and [DESIGN.md](DESIGN.md) for architecture details.
