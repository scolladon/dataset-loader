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

> **Note:**
>
> - **CRM Analytics targets** — the dataset must already exist with at least one prior completed upload. Create it via the CRM Analytics UI or a one-time dataflow before the first load.
> - **File targets** — omit `targetOrg` and set `targetFile` to a local file path. The file is created automatically.
> - **Column alignment** — CRM Analytics ingests rows by position. `--audit` verifies that your source columns (SObject config `fields`, ELF `LogFileFieldNames`, or CSV header) match the dataset's metadata column set (and order for ELF/CSV) before any upload runs. SObject entries are automatically reordered at runtime to match the dataset's column order; ELF/CSV source order must already match the dataset and is enforced at audit time.

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

<details>
<summary>State File & Watermarks</summary>

Watermarks are stored in a separate state file (`.dataset-load.state.json`) to keep config declarative:

```json
{
  "watermarks": {
    "source-org-alias:elf:LightningPageView:Daily": "2026-03-05T00:00:00.000+0000",
    "source-org-alias:sobject:Account": "2026-03-05T14:30:00.000+0000"
  }
}
```

Watermark keys: `{sourceOrg}:elf:{eventLog}:{interval}`, `{sourceOrg}:sobject:{sObject}`, or `csv:{csvFile}`.

> Set `name` on an entry to use it as the watermark key instead of the auto-generated one. This lets you rename source orgs or change event types without losing watermark history.

- **First ELF run** (no watermark): fetches only the latest record (bootstrap mode)
- **First SObject run** (no watermark): fetches all matching records (use `limit` to cap)
- **Subsequent runs**: fetch incrementally from the stored watermark

</details>

<!-- commands -->
* [`sf dataset load`](#sf-dataset-load)

## `sf dataset load`

Load Event Log Files and SObject data into CRM Analytics datasets

```
USAGE
  $ sf dataset load [--json] [--flags-dir <value>] [-c <value>] [-s <value>] [--audit] [--dry-run] [--entry
    <value>] [--start-date <iso>] [--end-date <iso>]

FLAGS
  -c, --config-file=<value>  [default: dataset-load.config.json] Path to config JSON
  -s, --state-file=<value>   [default: .dataset-load.state.json] Path to watermark state file
      --audit                Pre-flight checks only (auth, connectivity, permissions)
      --dry-run              Show plan without executing
      --entry=<value>        Process only the entry with this name
      --start-date=<iso>     Load only records with dateField/LogDate >= this ISO-8601 datetime
                             (ignored for CSV entries). SD always overrides the watermark when set.
      --end-date=<iso>       Load only records with dateField/LogDate <= this ISO-8601 datetime
                             (ignored for CSV entries).

GLOBAL FLAGS
  --flags-dir=<value>  Import flag values from a directory.
  --json               Format output as json.

EXAMPLES
  $ sf dataset load

  $ sf dataset load --config-file my-config.json --dry-run

  $ sf dataset load --start-date 2026-01-01T00:00:00.000Z --end-date 2026-01-31T23:59:59.999Z
```

### Date bounds (`--start-date` / `--end-date`)

Both flags take strict ISO-8601 datetimes (e.g. `2026-01-15T00:00:00.000Z`). Format is validated via regex; calendar validity is enforced via round-trip UTC-components check (so `2026-02-30T…` is rejected up front).

| Flags                                  | SObject / ELF filter                           |
|----------------------------------------|-----------------------------------------------|
| neither                                | `dateField > watermark` (first run loads everything available) |
| `--start-date SD` only                 | `dateField >= SD` (SD overrides watermark)    |
| `--end-date ED` only                   | `dateField > watermark AND dateField <= ED`   |
| `--start-date SD` + `--end-date ED`    | `dateField >= SD AND dateField <= ED`         |

On a first ELF run (no watermark, no `--start-date`), the reader loads every available log file ascending. To cap the initial load, pass `--start-date <recent-iso>` or pre-seed `.dataset-load.state.json` with a recent `LogDate`.

**SD always wins when set.** The command emits warnings when SD interacts non-trivially with the existing watermark:

- **REWIND** — `SD < watermark`: previously-loaded records will be re-loaded; watermark may regress.
- **HOLE** — `SD > watermark`: records in the gap will be skipped **and never back-filled** by subsequent incremental runs (the watermark jumps past the gap). See `RUN_BOOK.md` for the HOLE recovery pattern.
- **BOUNDARY** — `SD == watermark` under `operation: Append`: the boundary record gets appended again (duplicate row). Silent under `operation: Overwrite`.
- **EMPTY** — `ED < watermark` with no `SD`: query window is empty; no records will load.

CSV entries ignore both flags. If the run filters to CSV-only entries, a single "no effect" warning is emitted.

_See code: [src/commands/dataset/load.ts](https://github.com/scolladon/dataset-loader/blob/main/src/commands/dataset/load.ts)_
<!-- commandsstop -->

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
