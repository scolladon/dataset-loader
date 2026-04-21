<!-- markdownlint-disable MD013 MD033 -- long table rows and <summary>/<details> architecture-doc patterns -->
# Design Document: Dataset Loader — SF CLI Plugin

## Problem Statement

Organizations need to load Salesforce Event Log Files (ELF) and SObject data into CRM Analytics datasets for long-term analysis. The data may come from multiple source orgs and target multiple analytic orgs, requiring column augmentation (e.g., OrgId), parallel fetching, grouped uploads, and incremental watermark-based loading.

## Why an SF CLI Plugin (TypeScript)

### Previous approaches rejected

**Apex (platform-native):** Abandoned due to governor limits — 12 MB heap ceiling, callout-after-DML restriction, and the 50-upload-per-dataset-per-24h limit create a hard throughput cap (~600 MB/dataset/day). See [Appendix: Why Not Apex](#appendix-why-not-apex).

**Shell scripts (v1):** The initial working implementation used bash scripts (`load-elf.sh`, `run-all.sh`, `lib/*.sh`) with BATS tests. While functional for ELF-only loading, extending to SObject support, dynamic column augmentation, and multi-org grouped uploads would make the bash codebase fragile and hard to test. TypeScript provides type safety, async/await, and the `@salesforce/core` ecosystem.

**SFDMU with custom Add-Ons:** Evaluated as an alternative leveraging the existing [SFDMU plugin](https://help.sfdmu.com/) with its [Custom Add-On API](https://help.sfdmu.com/custom-add-on-api/sfdmu-run/). Rejected for these reasons:

| Concern                  | SFDMU Limitation                                                                                                                                                                                                 |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **ELF blob access**      | Cannot download `EventLogFile.LogFile` blobs — the Add-On API has no mechanism for arbitrary blob field access                                                                                                   |
| **CRM Analytics upload** | InsightsExternalData lifecycle (header + compressed base64 parts + process trigger) is not a CRUD operation and cannot be expressed as SFDMU object operations                                                   |
| **Org model**            | Strictly 2-org (source → target). This project requires N orgs (multiple sources + multiple analytics per entry)                                                                                                 |
| **Add-On API surface**   | No authenticated Salesforce connection exposed to add-on code — arbitrary REST/SOQL calls require managing your own auth ([GitHub issue #787](https://github.com/forcedotcom/SFDX-Data-Move-Utility/issues/787)) |
| **Concurrency control**  | SFDMU controls its own parallelism internally; per-org `p-limit(25)` semaphores are not possible                                                                                                                 |
| **Grouping**             | No equivalent to merging CSVs from multiple entries targeting the same `(targetOrg, targetDataset)`                                                                                                              |

SFDMU is the right tool for SObject-to-SObject migration, but the ELF download → augment → group → CRM Analytics upload pipeline is outside its design scope.

## Architecture: Hexagonal with Streaming Pipeline

The codebase follows **Hexagonal Architecture** (Ports & Adapters) with a streaming pipeline for memory-bounded data processing.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  Command Layer — Composition Root                                          │
│  commands/dataset/load.ts                                                     │
│  Decomposed into focused methods: config loading, SF port creation,        │
│  audit, dry-run, pipeline execution, reader factory                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  Domain Layer — Pure Business Logic                                        │
│  domain/                                                                   │
│  Value objects (Watermark, WatermarkKey, DatasetKey, WatermarkStore)       │
│  Services (Pipeline, Auditor)                                            │
│  No infrastructure dependencies                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Ports Layer — Contracts                                                   │
│  ports/types.ts                                                            │
│  Interfaces consumed by domain, implemented by adapters                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  Adapters Layer — Infrastructure                                           │
│  adapters/                                                                 │
│  Salesforce API, file system, config validation, CLI progress             │
│  Each adapter implements one or more ports                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```text
src/
├── commands/dataset/
│   └── load.ts              # SF CLI command — composition root
├── domain/
│   ├── pipeline.ts               # Core orchestration engine
│   ├── auditor.ts                # Pre-flight checks (auth, access, schema alignment)
│   ├── column-name.ts            # parseCsvHeader: BOM/quote/CR/whitespace-safe CSV header parser
│   ├── csv-quote.ts              # OWASP-safe CSV cell quoting
│   ├── schema-check.ts           # Set + order comparison of column lists (case-insensitive)
│   ├── sobject-row-projection.ts # Builds ProjectionLayout for SObject runtime row reordering
│   ├── watermark.ts              # Value object: ISO 8601 timestamp
│   ├── watermark-key.ts          # Value object: entry identifier
│   ├── watermark-store.ts        # Immutable watermark map
│   └── dataset-key.ts            # Value object: (targetOrg, targetDataset/targetFile) target identity
├── ports/
│   └── types.ts             # Port interfaces + shared types (SF_IDENTIFIER_PATTERN, formatErrorMessage, EntryShape)
└── adapters/
    ├── sf-client.ts           # Salesforce REST API client
    ├── config-loader.ts       # Config parsing & validation (Zod)
    ├── state-manager.ts       # Watermark file persistence
    ├── progress-reporter.ts   # CLI progress bar
    ├── readers/
    │   ├── elf-reader.ts      # EventLogFile reader (yields CSV lines)
    │   ├── sobject-reader.ts  # SObject query reader (yields CSV lines; applies ProjectionLayout when provided)
    │   └── csv-reader.ts      # Local CSV file reader
    ├── writers/
    │   ├── dataset-writer.ts  # CRM Analytics upload (InsightsExternalData)
    │   └── file-writer.ts     # Local file writer (CSV output)
    └── pipeline/
        ├── async-channel.ts       # Bounded async channel (multi-producer, single-consumer)
        ├── augment-transform.ts   # Appends extra columns to CSV lines
        ├── fan-in-stream.ts       # Merges N writer slots into one downstream chunker
        ├── fan-out-transform.ts   # Tees a stream to multiple writable channels
        └── row-counter.ts         # PassThrough that counts rows for progress

test/
├── unit/
│   ├── domain/              # Domain value object & service tests
│   ├── adapters/            # Adapter tests with mocked ports
│   └── ports/               # Shared type & utility tests
├── nut/                     # Node Unit Tests (CLI integration)
├── fixtures/                # Shared test helpers (FakeConnectionBuilder)
└── manual/                  # Manual test scenarios
```

## Ports

All cross-boundary communication goes through port interfaces defined in `ports/types.ts`. This module also exports shared domain types and utilities used across layers.

| Port               | Purpose                                                                                                                                 | Adapter                                     |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| `SalesforcePort`   | SOQL queries, blob downloads, REST CRUD                                                                                                 | `SalesforceClient`                          |
| `ReaderPort`       | Fetch data as `AsyncIterable<string[]>` CSV line batches. SObject readers implement the optional `project(layout)` method to reorder cells per-row into the dataset's canonical column order | `ElfReader`, `SObjectReader`                |
| `CreateWriterPort` | Factory for writers per dataset (accepts `ProgressListener`, `HeaderProvider`, and an optional `AlignmentSpec` describing the source columns + augment columns for schema-alignment enforcement) | `DatasetWriterFactory`, `FileWriterFactory` |
| `Writer`           | Init returns `{ chunker, datasetFields? }` — `datasetFields` is the metadata's canonical column order when alignment enforcement is active (DatasetWriter only); pipeline builds a per-entry `ProjectionLayout` from it. Also: finalize, abort, skip | `DatasetWriter`, `FileWriter`               |
| `ProgressListener` | Callbacks for sink creation and part upload events                                                                                      | Wired in pipeline                           |
| `HeaderProvider`   | Deferred header resolution (used by `FileWriter` to write header on first row)                                                          | `createHeaderProvider` closure (domain)     |
| `StatePort`        | Read/write watermark persistence                                                                                                        | `FileStateManager`                          |
| `ProgressPort`     | Progress bar lifecycle, creates `PhaseProgress` with `GroupTracker` per dataset                                                         | `ProgressReporter`                          |
| `GroupTracker`     | Per-group real-time tracking of parentId, files, rows, and parts                                                                        | `ProgressReporter` (closure)                |
| `LoggerPort`       | Structured logging                                                                                                                      | SF CLI logger                               |

**Shared types and utilities** in `ports/types.ts`:

- `SF_IDENTIFIER_PATTERN` — Regex for valid Salesforce identifiers, shared by config validation and dataset-writer
- `formatErrorMessage(error)` — Safe error-to-string conversion used across all error handlers
- `EntryShape`, `ElfShape`, `SObjectShape`, `CsvShape` — Entry shape types; entry type is inferred from field presence (`eventLog` for ELF, `sObject` for SObject, `csvFile` for CSV) rather than an explicit `type` discriminator
- `isElfShape()`, `isSObjectShape()`, `isCsvShape()` — Type guard functions for shape-based discrimination
- `Operation`, `WatermarkEntry` — Shared type aliases
- `ReaderKind` — `'sobject' | 'elf' | 'csv'` — discriminator used by `AlignmentSpec` and `AuditEntry`
- `SkipDatasetError` — Thrown by `Writer.init()` to signal that a dataset should be silently skipped
- `ProjectionLayout` — `{ targetSize, augmentSlots, outputIndex }` — describes a positional remapping from reader-cell order to dataset-metadata order. Built per-entry by the pipeline from the writer's `datasetFields` + the entry's `AlignmentSpec`; consumed by `SObjectReader.project()`
- `AlignmentSpec` — `{ readerKind, entryLabel, providedFields, augmentColumns }` — passed to the writer factory. `providedFields` are the source columns: SObject config fields (dotted), ELF `LogFileFieldNames`, or CSV file header
- `AuditOutcome` — Discriminated union `{ kind: 'pass' | 'warn' | 'fail', message? }` returned by every audit strategy; `runAudit` treats WARN as non-blocking

## Domain Value Objects

All value objects are **immutable** and **self-validating** (Object Calisthenics):

- **Watermark** — Wraps ISO 8601 timestamp string. Validates format on construction. Converts to SOQL literal via `toSoqlLiteral()`.
- **WatermarkKey** — Composite key: `{sourceOrg}:elf:{eventLog}:{interval}`, `{sourceOrg}:sobject:{sObject}`, or `csv:{csvFile}`. When `name` is set on an entry, it is used as the watermark key instead of the auto-generated one. Static factory `fromEntry()`.
- **DatasetKey** — Composite key: `org:{targetOrg}:{targetDataset}` (CRM Analytics target) or `file:{targetFile}` (file target). Used for grouping entries into write jobs.
- **WatermarkStore** — Immutable map from WatermarkKey → Watermark. `set()` returns a new store instance.
- **DateBounds** — Run-scoped CLI `--start-date` / `--end-date` window. Two-layer validation (ISO regex via `Watermark.fromString` + round-trip UTC-components calendar check). Exposes `lowerConditionFor(dateField, wm)` / `upperConditionFor(dateField)` for reader SOQL assembly and `rewindsBelow(wm)` / `leavesHoleAbove(wm)` / `matchesWatermark(wm)` / `endsBeforeWatermark(wm)` predicates used by the command layer to emit REWIND / HOLE / BOUNDARY / EMPTY warnings. Rule: **SD always wins when set** (CLI overrides watermark on the lower bound); watermark fills in only when SD is absent.

## Data Flow

```text
Config File (JSON)
  │
  ▼
parseConfig() ─── Zod validation, operation consistency check
  │
  ▼
resolveConfig() ─── Query orgs for mustache tokens ({{sourceOrg.Id}}, etc.)
  │
  ▼
executePipeline()
  │
  ├── Group entries by DatasetKey (targetOrg + targetDataset, or targetFile)
  │
  ├── For each group (parallel via Promise.all with .catch()):
  │   │
  │   ├── Init Writer (CRM Analytics: queries metadata, validates alignment, returns datasetFields; File: opens stream)
  │   │   └── Throws SkipDatasetError on no-metadata, set/order mismatch, or augment-vs-reader overlap
  │   ├── Returns shared chunker Writable + (for datasets) the canonical column order
  │   │
  │   ├── For each reader bundle (entries sharing same reader + watermark):
  │   │   │
  │   │   ├── Build per-entry ProjectionLayout from slot.datasetFields + entry.alignment
  │   │   ├── Fan-out constraint: reject whole bundle if viable layouts diverge
  │   │   ├── Call reader.project(layout) once (SObject only) before fetch
  │   │   ├── Single entry: Fetch via ReaderPort → [Projection baked in for SObject | AugmentTransform for ELF/CSV] → chunker
  │   │   └── Multiple entries: FanOutTransform tees the reader stream to each entry's channel
  │   │
  │   ├── End chunker stream and await completion
  │   ├── Finalize Writer
  │   │   └── CRM Analytics: PATCH InsightsExternalData with Action='Process'
  │   │   └── File: close WriteStream
  │   │
  │   └── Update watermarks for successful entries
  │
  └── Persist watermark store to state file
```

## Streaming Architecture

Data never fully materializes in memory. The pipeline streams through three layers:

1. **Readers** yield `AsyncIterable<string[]>` — batches of 2000 CSV lines (header-stripped for ELF)
   - ELF: concurrent blob stream downloads via `getBlobStream()`; all blobs in a page are fetched in parallel; batches are pushed to a bounded `AsyncChannel<string[]>` which applies backpressure when the queue is full; strips header row from each blob
   - SObject: SOQL query with prefetched next page, yields CSV lines via a hand-rolled `csvQuote` helper (csv-stringify parity, plus OWASP formula-injection guard that TAB-prefixes leading `= + - @ |` etc.)
2. **AugmentTransform + RowCounter** — Transform stream that appends augment columns to each batch; PassThrough that counts rows for progress tracking
3. **FanOutTransform** — When multiple entries share the same reader (same org + eventLog/sObject + watermark), a single fetch is teed to N channels via `promiseWrite`, one per entry
4. **Writer chunker** — Writable stream returned by `Writer.init()`:
   - CRM Analytics (`GzipChunkingWritable`): batch gzip-compresses (512KB flush threshold), splits at 10 MB base64 part boundaries, uploads parts concurrently up to `UPLOAD_HIGH_WATER` (= 25) in-flight
   - File (`FileWriter` internal): streams CSV rows directly to a `fs.WriteStream`; writes header on first row via `HeaderProvider`

Key types:

```typescript
interface FetchResult {
  readonly lines: AsyncIterable<string[]>
  readonly watermark: () => Watermark | undefined
  readonly fileCount: () => number
}

interface ReaderPort {
  fetch(watermark?: Watermark): Promise<FetchResult>
  header(): Promise<string>
}

interface Writer {
  init(): Promise<Writable>
  finalize(): Promise<WriterResult>
  abort(): Promise<void>
  skip(): Promise<void>
}
```

## Parallelism Model

| Scope                        | Strategy                                                                  | Limit                                                                       |
|------------------------------|---------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| Dataset groups               | `Promise.all` with `.catch()` wrappers                                    | Unbounded                                                                   |
| Entries within a group       | Concurrent via `Promise.all`                                              | Unbounded (Node.js stream backpressure serializes writes to shared chunker) |
| Part uploads within a group  | Bounded in-flight set (`pendingUploads`) with `Promise.race` backpressure | `UPLOAD_HIGH_WATER` = 25 (CRM Analytics only)                               |
| ELF blob downloads           | Concurrent within each page; pages chained sequentially                   | All blobs pre-fetched per page                                              |
| Salesforce API calls per org | `pLimit` semaphore                                                        | 25 concurrent                                                               |
| HTTP 429 responses           | Exponential backoff retry                                                 | 3 attempts (1s, 2s, 4s)                                                     |

Error isolation: a failed entry aborts the entire group (sink is aborted, all entries in the group keep old watermarks). A failed group does not affect other groups.

## Upload Lifecycle

```text
1. POST InsightsExternalData
   ├── EdgemartAlias, Format=Csv, Operation, MetadataJson (base64)
   └── Action='None'

2. POST InsightsExternalDataPart (one per 10 MB base64 chunk)
   ├── InsightsExternalDataId (parent reference)
   ├── PartNumber (sequential)
   └── DataFile (base64-encoded gzip)

3. PATCH InsightsExternalData
   ├── Action='Process' (triggers CRM Analytics ingestion)
   └── Mode='Incremental' (faster processing)
```

Metadata JSON is reused from the most recent completed upload for the dataset. If no prior upload exists, the group is skipped with a `SkipDatasetError`. The `numberOfLinesToIgnore` field is normalized to `0` since fetchers strip headers before streaming.

## Column Alignment

CRM Analytics ingests CSV rows **by position**, not by name. The dataset's metadata defines the canonical column order (`objects[0].fields[*].fullyQualifiedName`); the uploaded payload must emit cells in that exact order. Without explicit alignment, any drift between the source's column order and the dataset's column order silently corrupts every row — values land in the wrong columns and surface only as downstream type-parse errors in the dataflow digest.

The loader enforces alignment in two places:

1. **Audit-time (`--audit`)** — the `schemaAlignment` strategy fetches the dataset's `MetadataJson`, resolves the source's column list (SObject config `fields`, ELF `LogFileFieldNames`, or CSV file header), and compares them via `checkSchemaAlignment`:
   - **SObject** — set-only check (case-insensitive, with `.`→`_` translation). Order is intentionally ignored because SObject rows are reordered at runtime.
   - **ELF / CSV** — set **and** order check. ELF/CSV rows are streamed as-is (no per-row reorder) to keep the hot path cheap; any order drift fails the audit with a positional diff.
   - **Augment overlap** — fails if an `augmentColumns` key also appears in the reader's source columns (the combined provided list would otherwise have duplicates).

2. **Writer init** — `DatasetWriter.init()` reruns the same checks against the freshly-fetched metadata at pipeline start. If anything has drifted between audit and run, the offending entry is skipped per-group (preserving cross-dataset isolation).

For **SObject runtime reordering**, the pipeline builds a `ProjectionLayout` per entry from the writer's returned `datasetFields` plus that entry's `AlignmentSpec`:

```ts
interface ProjectionLayout {
  targetSize: number                                  // dataset column count
  augmentSlots: ReadonlyArray<{ pos: number; quoted: string }>  // pre-quoted augment cells at their target positions
  outputIndex: Int32Array                             // reader-cell index → target position
}
```

`SObjectReader.project(layout)` is called once per reader before the first `fetch()`. The hot path branches once on layout presence; the per-row cost is identical to the pre-fix implementation (`N` cell writes + one join) but with augment cells baked in at their target positions instead of appended as a suffix.

Two entries targeting the same dataset with different augment **values** (e.g. different `sourceOrg.Id`) each get their own layout — structural fields (`outputIndex`, positions) are shared via the writer's `datasetFields`, but `augmentSlots[i].quoted` carries per-entry values.

**Fan-out constraint**: multiple sinks sharing an SObject reader must produce identical projection layouts (structural equality on `targetSize`, `outputIndex`, and augment pos+quoted pairs). Divergent layouts reject the whole bundle with a structured `SkipDatasetError`, leaving the user to split the config entries so their `ReaderKey`s differ.

## Components

| Component              | Path                                     | Responsibility                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|------------------------|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SalesforceClient`     | `adapters/sf-client.ts`                  | Per-org REST client with `p-limit(25)` concurrency, gzip headers, HTTP 429 retry with exponential backoff (3 attempts max). Error formatting extracts Salesforce error details from `error.data` array.                                                                                                                                                                                                                                                                                                    |
| `ElfReader`            | `adapters/readers/elf-reader.ts`         | Queries EventLogFile, concurrently downloads blob streams via `getBlobStream()`. Each blob processor accumulates lines into batches of 2000 and pushes them to a shared `AsyncChannel<string[]>`, which provides backpressure and serializes delivery to the consumer. Strips CSV header from each blob. Validates `eventLog` and `interval` format on construction. Always loads ascending (`ORDER BY LogDate ASC`) with no `LIMIT`; CLI `--start-date` / `--end-date` flags map to additional `LogDate >= SD` / `LogDate <= ED` conditions via the `DateBounds` value object. |
| `SObjectReader`        | `adapters/readers/sobject-reader.ts`     | Builds SOQL with watermark/where/limit, prefetches next page while yielding current results as CSV lines (serialized via the shared `csvQuote` helper — `{quoted:true, quoted_empty:true}` parity plus an OWASP formula-injection guard that TAB-prefixes leading `= + - @ \|`). Auto-appends `dateField` to SELECT if not in `fields`. Field accessors are precomputed at construction (flat vs dotted paths). CLI `--start-date` / `--end-date` flags map via `DateBounds` to `dateField >= SD` / `dateField <= ED` clauses; SD always overrides the watermark's strict-greater clause when set. |
| `CsvReader`            | `adapters/readers/csv-reader.ts`         | Local CSV file reader. Reads a file line by line and yields batches of CSV lines.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `AugmentTransform`     | `adapters/pipeline/augment-transform.ts` | Transform stream that appends augment column values (CSV-quoted) to each line.                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `FanInStream`          | `adapters/pipeline/fan-in-stream.ts`     | Merges N writer slots into one downstream chunker. Each slot is a `Writable` created via `createSlot()`; when the last slot closes (via `close` event, which fires on both finish and destroy), `downstream.end()` is called automatically. Provides backpressure by delegating each slot write to the downstream write callback.                                                                                                                                                                          |
| `FanOutTransform`      | `adapters/pipeline/fan-out-transform.ts` | Transform stream that tees each chunk to N `PassThrough` channels concurrently via `Promise.all`. Used when multiple entries share the same reader (same org + type + watermark). Channels that error are removed from the active set.                                                                                                                                                                                                                                                                     |
| `RowCounter`           | `adapters/pipeline/row-counter.ts`       | PassThrough stream that counts rows for progress tracking via `GroupTracker.addRows()`.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `AsyncChannel<T>`      | `adapters/pipeline/async-channel.ts`     | Bounded multi-producer, single-consumer async queue. Producers call `push()` and await backpressure when the queue reaches `highWater`. Consumer iterates with `for await`. `close()` and `fail(err)` are idempotent; `push()` after either rejects (no silent data loss). If the consumer breaks early, the async iterator's `return()` cancels the channel and releases any stalled producers.                                                                                                           |
| `DatasetWriterFactory` | `adapters/writers/dataset-writer.ts`     | Creates `DatasetWriter` instances per CRM Analytics dataset. `DatasetWriter.init()` queries existing metadata (required), normalizes `numberOfLinesToIgnore` to 0, creates the parent record, and returns a `GzipChunkingWritable`. The writable batch-compresses with 512KB flush threshold, splits at 10 MB base64 boundaries, and keeps at most `UPLOAD_HIGH_WATER` (= 25) part uploads in-flight via `Promise.race` backpressure.                                                                      |
| `FileWriterFactory`    | `adapters/writers/file-writer.ts`        | Creates `FileWriter` instances per output file path. `FileWriter.init()` opens a `fs.WriteStream` (append or overwrite). Header is written on first row via `HeaderProvider.resolveHeader()`. `skip()` deletes the file on overwrite.                                                                                                                                                                                                                                                                      |
| `ConfigLoader`         | `adapters/config-loader.ts`              | Reads JSON config, Zod schema validation, operation consistency checks, mustache token resolution (`{{sourceOrg.Id}}`, etc.)                                                                                                                                                                                                                                                                                                                                                                               |
| `FileStateManager`     | `adapters/state-manager.ts`              | Atomic read/write of watermark state file (temp file + rename, mode `0o600`)                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `ProgressReporter`     | `adapters/progress-reporter.ts`          | CLI progress display using `cli-progress` MultiBar. Main bar tracks entries, per-group sub-bars show real-time files fetched, rows processed, and parts uploaded. No-op for zero-length phases. Includes workaround for cli-progress non-TTY `bar.start()` bug.                                                                                                                                                                                                                                            |
| `Pipeline`             | `domain/pipeline.ts`                     | Core orchestration: `executePipeline` (entry point). Three phases: (1) init one `WriterSlot` + `FanInStream` per `DatasetGroup`; (2) process reader bundles concurrently — single-entry path pipes directly, multi-entry path fans out via `FanOutTransform`; (3) await each `FanInStream`-owned chunker drain, then finalize/abort each writer. `DatasetGroup` is a pure data class; `createHeaderProvider` creates the deferred header closure.                                                          |
| `Auditor`              | `domain/auditor.ts`                      | Builds and runs pre-flight checks (`authConnectivity`, `elfAccess`, `insightsAccess`, `sobjectReadAccess`, `datasetReady`, `schemaAlignment`). Deduplicates by `(org, key)` across entries. `MetadataJson` is memoised per `(org, dataset)` within a single audit run. `runAudit` executes all checks via `Promise.allSettled`, logs `[PASS]`/`[WARN]`/`[FAIL]` per check, and returns `passed: boolean` (WARN is non-blocking). |
| Domain helpers         | `domain/{column-name,schema-check,sobject-row-projection,csv-quote}.ts` | Pure utilities: `parseCsvHeader` (CSV-safe header tokenizer), `checkSchemaAlignment` (set + order comparison), `buildSObjectRowProjection` (ProjectionLayout builder with overlap/set rejection), `csvQuote` (OWASP-safe cell quoting). |

## Config Validation

Config is validated at load time using Zod schemas:

- Entry type inference from field presence (`eventLog` for ELF, `sObject` for SObject, `csvFile` for CSV)
- Org alias format (`[a-zA-Z0-9_.-]+`)
- Salesforce identifier format (`[a-zA-Z_][a-zA-Z0-9_]*`)
- At least one entry required
- Operation consistency: all entries targeting the same `(targetOrg, targetDataset)` or `targetFile` must use the same operation
- User-supplied `where` clause: deny-list for statement separators (`;`), SOQL/SQL comment markers (`/*`, `*/`, `--`), control characters (C0 + DEL + U+2028/U+2029), and a string-aware balanced-parens check that prevents paren-escape filter broadening (`1=1) OR (1=1`) while allowing legitimate `'foo)bar'` and `\'`-escaped quotes inside string literals
- `augmentColumns`: column names restricted to `[a-zA-Z_][a-zA-Z0-9_.]*` (excludes formula-initiating chars); values are CSV-quoted with TAB-prefix guard on leading `= + - @ \|`

## State Management

Watermarks are persisted atomically using a temp-file-and-rename strategy:

1. Write to `.tmp-<uuid>.json` (mode `0o600`)
2. Rename to target path
3. Clean up temp file on error

The state file is only updated after successful upload, ensuring no data is skipped on retry.

## Resilience

| Feature             | Behavior                                                                               |
|---------------------|----------------------------------------------------------------------------------------|
| Per-org concurrency | 25 concurrent API calls per org via `p-limit` (shared across all entries for that org) |
| HTTP 429 retry      | Exponential backoff, 3 attempts max (1s, 2s, 4s)                                       |
| Partial failure     | Failed entries don't block others; watermarks only advance on success                  |
| Atomic state writes | Temp file + rename prevents corruption on crash                                        |
| Streaming           | Memory-bounded processing regardless of dataset size                                   |
| SSRF guard          | `queryMore` compares `URL(nextRecordsUrl).origin` to `instanceUrl.origin` and rejects off-origin, userinfo-bypass, protocol-relative, and malformed URLs before the bearer token is sent |

## Error Handling

| Scenario                     | Behavior                                                          |
|------------------------------|-------------------------------------------------------------------|
| Config validation fails      | Fail fast, list all validation issues (Zod)                       |
| Operation conflict in group  | Fail fast with message listing conflicting entries and operations |
| Org auth expired/missing     | Fail for that org's entries, continue others                      |
| Fetch failure (single entry) | Skip entry, log error, watermark unchanged                        |
| Upload failure (group)       | Abort sink, all entries in group keep old watermarks              |
| No new records               | Skip silently (info log), watermark unchanged                     |
| HTTP 429 (rate limit)        | Retry with exponential backoff (3 attempts max)                   |

**Exit codes**: 0 = all success, 1 = partial success, 2 = fatal error

## Dependencies

| Package                       | Purpose                                        |
|-------------------------------|------------------------------------------------|
| `@salesforce/sf-plugins-core` | SfCommand base class, standard flags           |
| `@salesforce/core`            | Connection, Org authentication                 |
| `zod`                         | Config schema validation                       |
| `p-limit`                     | Per-org concurrency limiting (25 slots)        |
| `cli-progress`                | CLI progress bar with ETA                      |
| `vitest`                      | Test framework                                 |

## Design Principles

- **Hexagonal Architecture** — Domain isolated from infrastructure via ports
- **Object Calisthenics** — Applied to domain value objects (immutability, wrapping primitives, small classes)
- **Functional Programming** — Pure transformations in the data pipeline (`queryPages`)
- **Streaming** — Async iterables and chunk-based processing for bounded memory usage
- **Fail-fast** — Zod validation, constructor-time checks, audit mode
- **Composition over inheritance** — Adapters composed via dependency injection in the command layer

## Appendix: Why Not Apex

The initial Apex-based approach was abandoned due to multiple Salesforce platform limitations:

1. **Callout-after-DML restriction**: InsightsExternalData upload lifecycle requires multiple sequential API calls; mixing with DML for watermark tracking creates irreconcilable transaction boundary conflicts.
2. **12 MB heap limit**: EventLogFile CSVs can easily exceed this. The entire CSV must be held in memory for base64 encoding.
3. **50 uploads per dataset per 24 hours**: Combined with heap limit, theoretical max throughput is ~600 MB/dataset/day — insufficient for active orgs.
4. **Configuration overhead**: Cross-org callouts require Connected App, Auth Provider, and Named Credentials per environment.
