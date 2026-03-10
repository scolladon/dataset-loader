# CRMA Data Loader

SF CLI plugin that loads Salesforce [Event Log Files](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_eventlogfile.htm) (ELF) and SObject data into CRM Analytics datasets using the [Analytics External Data API](https://developer.salesforce.com/docs/atlas.en-us.bi_dev_guide_ext_data.meta/bi_dev_guide_ext_data/bi_ext_data_object_externaldata.htm).

## Prerequisites

- [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) (`sf`) with authenticated orgs
- Node.js >= 18

### Required Permissions

| Org | Permissions |
| --- | --- |
| **Source org** (ELF) | `API Enabled`, `View Event Log Files` (`ViewEventLogFiles`) |
| **Source org** (SObject) | `API Enabled` |
| **Analytic org** | `API Enabled`, `Upload External Data to CRM Analytics` (`InsightsAppUploadUser`) |

## Installation

```bash
npm install
npm run build
sf plugins link .
```

## Quick Start

```bash
# 1. Create a config file
cat crma-load.config.json
```

```json
{
  "entries": [
    {
      "type": "elf",
      "eventType": "LightningPageView",
      "interval": "Daily",
      "sourceOrg": "my-source-org",
      "analyticOrg": "my-analytic-org",
      "dataset": "LightningPageView_my_source_org"
    }
  ]
}
```

```bash
# 2. Verify auth and permissions
sf crma load --audit

# 3. Preview the plan
sf crma load --dry-run

# 4. Run
sf crma load
```

The target dataset must already exist in CRMA with at least one prior completed upload (so that metadata is available). Create the dataset manually via the CRMA UI or a one-time dataflow before the first load.

<!-- commands -->
* [`sf crma load`](#sf-crma-load)

## `sf crma load`

Load Event Log Files and SObject data into CRMA datasets

```
USAGE
  $ sf crma load [--json] [--flags-dir <value>] [-c <value>] [-s <value>] [--audit] [--dry-run] [--entry
    <value>]

FLAGS
  -c, --config-file=<value>  [default: crma-load.config.json] Path to config JSON
  -s, --state-file=<value>   [default: .crma-load.state.json] Path to watermark state file
      --audit                Pre-flight checks only (auth, connectivity, permissions)
      --dry-run              Show plan without executing
      --entry=<value>        Process only the entry with this name

GLOBAL FLAGS
  --flags-dir=<value>  Import flag values from a directory.
  --json               Format output as json.

EXAMPLES
  $ sf crma load

  $ sf crma load --config-file my-config.json --dry-run
```

_See code: [src/commands/crma/load.ts](https://github.com/scolladon/crma-data-loader/blob/main/src/commands/crma/load.ts)_
<!-- commandsstop -->

### Exit Codes

| Code | Meaning |
| --- | --- |
| `0` | All entries processed successfully |
| `1` | Partial success (some entries failed, some succeeded) |
| `2` | Fatal error (config invalid, all entries failed, or audit failure) |

## Config Format

### Config File (`crma-load.config.json`)

```json
{
  "entries": [
    {
      "type": "elf",
      "sourceOrg": "source-org-alias",
      "analyticOrg": "analytic-org-alias",
      "dataset": "ALM_LightningPageView",
      "eventType": "LightningPageView",
      "interval": "Daily",
      "operation": "Append",
      "augmentColumns": {
        "OrgId": "$sourceOrg.Id",
        "OrgName": "$sourceOrg.Name",
        "Environment": "Production"
      }
    },
    {
      "type": "sobject",
      "sourceOrg": "source-org-alias",
      "analyticOrg": "analytic-org-alias",
      "dataset": "ALM_Accounts",
      "sobject": "Account",
      "fields": ["Id", "Name", "Industry", "CreatedDate"],
      "dateField": "LastModifiedDate",
      "where": "Industry != null",
      "limit": 100,
      "operation": "Overwrite",
      "augmentColumns": {
        "OrgId": "$sourceOrg.Id"
      }
    }
  ]
}
```

#### ELF Entry Fields

| Field | Required | Description |
| --- | --- | --- |
| `type` | yes | `"elf"` |
| `sourceOrg` | yes | SF CLI alias of the org containing EventLogFiles |
| `analyticOrg` | yes | SF CLI alias of the CRMA org |
| `dataset` | yes | Target CRMA dataset API name (`EdgemartAlias`) |
| `eventType` | yes | EventLogFile type (e.g. `Login`, `LightningPageView`, `API`) |
| `interval` | yes | `"Daily"` or `"Hourly"` (Hourly requires Shield license) |
| `operation` | no | `"Append"` (default) or `"Overwrite"` |
| `augmentColumns` | no | Extra columns to append (see below) |

#### SObject Entry Fields

| Field | Required | Description |
| --- | --- | --- |
| `type` | yes | `"sobject"` |
| `sourceOrg` | yes | SF CLI alias of the source org |
| `analyticOrg` | yes | SF CLI alias of the CRMA org |
| `dataset` | yes | Target CRMA dataset API name |
| `sobject` | yes | SObject API name (e.g. `Account`, `Opportunity`) |
| `fields` | yes | Array of field API names to query |
| `dateField` | no | Field used for watermarking (default: `LastModifiedDate`) |
| `where` | no | Additional SOQL WHERE clause |
| `limit` | no | Max number of records to fetch (appends `LIMIT n` to SOQL) |
| `operation` | no | `"Append"` (default) or `"Overwrite"` |
| `augmentColumns` | no | Extra columns to append (see below) |

#### Augment Columns

Append static or dynamic columns to every fetched row:

| Expression | Resolves to |
| --- | --- |
| `$sourceOrg.Id` | 18-char Organization Id of the source org |
| `$sourceOrg.Name` | Organization Name of the source org |
| `$analyticOrg.Id` | 18-char Organization Id of the analytic org |
| `$analyticOrg.Name` | Organization Name of the analytic org |
| Any other string | Used as-is (static value) |

#### Grouping

Entries targeting the same `(analyticOrg, dataset)` are merged into a single upload job. All entries in a group must use the same `operation`.

### State File (`.crma-load.state.json`)

Watermarks are stored separately from config to keep config declarative:

```json
{
  "watermarks": {
    "source-org-alias:elf:LightningPageView:Daily": "2026-03-05T00:00:00.000+0000",
    "source-org-alias:sobject:Account": "2026-03-05T14:30:00.000+0000"
  }
}
```

Watermark keys: `{sourceOrg}:elf:{eventType}:{interval}` or `{sourceOrg}:sobject:{sobject}`.

Without a state file, the first ELF run fetches only the latest record (bootstrap mode). SObject entries fetch all matching records on first run (use the `limit` field to cap this). Subsequent runs fetch incrementally from the stored watermark.

## How It Works

1. **Parse config** — validate JSON with Zod, check operation consistency across groups
2. **Resolve** — authenticate orgs, resolve dynamic expressions (`$sourceOrg.Id`, etc.)
3. **Audit** (optional) — verify connectivity, EventLogFile access, InsightsExternalData write access
4. **Execute pipeline** — group entries by dataset, stream through Fetcher → Augment → GzipChunkingWritable
5. **Upload** — batch gzip compress, base64 encode, split into 10 MB parts, upload concurrently via InsightsExternalData API
6. **Update watermarks** — only for entries whose group uploaded successfully

## Development

```bash
npm install          # Install dependencies
npm run build        # Compile TypeScript
npm test             # Run all tests
npm run test:watch   # Watch mode
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and [DESIGN.md](DESIGN.md) for architecture details.
