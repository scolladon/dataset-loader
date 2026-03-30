# Design Document: Dataset Loader — SF CLI Plugin

## Problem Statement

Organizations need to load Salesforce Event Log Files (ELF) and SObject data into CRM Analytics datasets for long-term analysis. The data may come from multiple source orgs and target multiple analytic orgs, requiring column augmentation (e.g., OrgId), parallel fetching, grouped uploads, and incremental watermark-based loading.

## Why an SF CLI Plugin (TypeScript)

### Previous approaches rejected

**Apex (platform-native):** Abandoned due to governor limits — 12 MB heap ceiling, callout-after-DML restriction, and the 50-upload-per-dataset-per-24h limit create a hard throughput cap (~600 MB/dataset/day). See [Appendix: Why Not Apex](#appendix-why-not-apex).

**Shell scripts (v1):** The initial working implementation used bash scripts (`load-elf.sh`, `run-all.sh`, `lib/*.sh`) with BATS tests. While functional for ELF-only loading, extending to SObject support, dynamic column augmentation, and multi-org grouped uploads would make the bash codebase fragile and hard to test. TypeScript provides type safety, async/await, and the `@salesforce/core` ecosystem.

**SFDMU with custom Add-Ons:** Evaluated as an alternative leveraging the existing [SFDMU plugin](https://help.sfdmu.com/) with its [Custom Add-On API](https://help.sfdmu.com/custom-add-on-api/sfdmu-run/). Rejected for these reasons:

| Concern | SFDMU Limitation |
|---------|-----------------|
| **ELF blob access** | Cannot download `EventLogFile.LogFile` blobs — the Add-On API has no mechanism for arbitrary blob field access |
| **CRM Analytics upload** | InsightsExternalData lifecycle (header + compressed base64 parts + process trigger) is not a CRUD operation and cannot be expressed as SFDMU object operations |
| **Org model** | Strictly 2-org (source → target). This project requires N orgs (multiple sources + multiple analytics per entry) |
| **Add-On API surface** | No authenticated Salesforce connection exposed to add-on code — arbitrary REST/SOQL calls require managing your own auth ([GitHub issue #787](https://github.com/forcedotcom/SFDX-Data-Move-Utility/issues/787)) |
| **Concurrency control** | SFDMU controls its own parallelism internally; per-org `p-limit(25)` semaphores are not possible |
| **Grouping** | No equivalent to merging CSVs from multiple entries targeting the same `(analyticOrg, dataset)` |

SFDMU is the right tool for SObject-to-SObject migration, but the ELF download → augment → group → CRM Analytics upload pipeline is outside its design scope.

## Architecture: Hexagonal with Streaming Pipeline

The codebase follows **Hexagonal Architecture** (Ports & Adapters) with a streaming pipeline for memory-bounded data processing.

```
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

```
src/
├── commands/dataset/
│   └── load.ts              # SF CLI command — composition root
├── domain/
│   ├── pipeline.ts          # Core orchestration engine
│   ├── auditor.ts           # Pre-flight permission checks
│   ├── watermark.ts         # Value object: ISO 8601 timestamp
│   ├── watermark-key.ts     # Value object: entry identifier
│   ├── watermark-store.ts   # Immutable watermark map
│   └── dataset-key.ts       # Value object: (targetOrg, targetDataset/targetFile) target identity
├── ports/
│   └── types.ts             # Port interfaces + shared types (SF_IDENTIFIER_PATTERN, formatErrorMessage, EntryShape)
└── adapters/
    ├── sf-client.ts           # Salesforce REST API client
    ├── config-loader.ts       # Config parsing & validation (Zod)
    ├── state-manager.ts       # Watermark file persistence
    ├── progress-reporter.ts   # CLI progress bar
    ├── readers/
    │   ├── elf-reader.ts      # EventLogFile reader (yields CSV lines)
    │   ├── sobject-reader.ts  # SObject query reader (yields CSV lines)
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

| Port | Purpose | Adapter |
|------|---------|---------|
| `SalesforcePort` | SOQL queries, blob downloads, REST CRUD | `SalesforceClient` |
| `ReaderPort` | Fetch data as `AsyncIterable<string[]>` CSV line batches | `ElfReader`, `SObjectReader` |
| `CreateWriterPort` | Factory for writers per dataset (accepts `ProgressListener` for progress callbacks and `HeaderProvider` for deferred header resolution) | `DatasetWriterFactory`, `FileWriterFactory` |
| `Writer` | Init writable stream, finalize, abort on error, skip on no data | `DatasetWriter`, `FileWriter` |
| `ProgressListener` | Callbacks for sink creation and part upload events | Wired in pipeline |
| `HeaderProvider` | Deferred header resolution (used by `FileWriter` to write header on first row) | `createHeaderProvider` closure (domain) |
| `StatePort` | Read/write watermark persistence | `FileStateManager` |
| `ProgressPort` | Progress bar lifecycle, creates `PhaseProgress` with `GroupTracker` per dataset | `ProgressReporter` |
| `GroupTracker` | Per-group real-time tracking of parentId, files, rows, and parts | `ProgressReporter` (closure) |
| `LoggerPort` | Structured logging | SF CLI logger |

**Shared types and utilities** in `ports/types.ts`:
- `SF_IDENTIFIER_PATTERN` — Regex for valid Salesforce identifiers, shared by config validation and dataset-writer
- `formatErrorMessage(error)` — Safe error-to-string conversion used across all error handlers
- `EntryShape`, `ElfShape`, `SObjectShape` — Entry type discriminated unions used by watermark-key and config-loader
- `EntryType`, `Operation`, `WatermarkEntry` — Shared type aliases
- `SkipDatasetError` — Thrown by `Writer.init()` to signal that a dataset should be silently skipped

## Domain Value Objects

All value objects are **immutable** and **self-validating** (Object Calisthenics):

- **Watermark** — Wraps ISO 8601 timestamp string. Validates format on construction. Converts to SOQL literal via `toSoqlLiteral()`.
- **WatermarkKey** — Composite key: `{sourceOrg}:elf:{eventType}:{interval}` or `{sourceOrg}:sobject:{sobject}`. Static factory `fromEntry()`.
- **DatasetKey** — Composite key: `org:{targetOrg}:{targetDataset}` (CRM Analytics target) or `file:{targetFile}` (file target). Used for grouping entries into write jobs.
- **WatermarkStore** — Immutable map from WatermarkKey → Watermark. `set()` returns a new store instance.

## Data Flow

```
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
  │   ├── Init Writer (CRM Analytics: queries metadata, creates parent record; File: opens stream)
  │   │   └── Throws SkipDatasetError if no existing CRM Analytics metadata found
  │   ├── Returns shared chunker Writable for the group
  │   │
  │   ├── For each reader bundle (entries sharing same reader + watermark):
  │   │   │
  │   │   ├── Single entry: Fetch via ReaderPort → AugmentTransform → RowCounter → chunker
  │   │   └── Multiple entries: FanOutTransform tees the reader stream to each entry's channel
  │   │       └── Each channel: AugmentTransform → RowCounter → its group's chunker
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
   - SObject: SOQL query with prefetched next page, yields CSV lines via csv-stringify
2. **AugmentTransform + RowCounter** — Transform stream that appends augment columns to each batch; PassThrough that counts rows for progress tracking
3. **FanOutTransform** — When multiple entries share the same reader (same org + eventType/sobject + watermark), a single fetch is teed to N channels via `promiseWrite`, one per entry
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

| Scope | Strategy | Limit |
|-------|----------|-------|
| Dataset groups | `Promise.all` with `.catch()` wrappers | Unbounded |
| Entries within a group | Concurrent via `Promise.all` | Unbounded (Node.js stream backpressure serializes writes to shared chunker) |
| Part uploads within a group | Bounded in-flight set (`pendingUploads`) with `Promise.race` backpressure | `UPLOAD_HIGH_WATER` = 25 (CRM Analytics only) |
| ELF blob downloads | Concurrent within each page; pages chained sequentially | All blobs pre-fetched per page |
| Salesforce API calls per org | `pLimit` semaphore | 25 concurrent |
| HTTP 429 responses | Exponential backoff retry | 3 attempts (1s, 2s, 4s) |

Error isolation: a failed entry aborts the entire group (sink is aborted, all entries in the group keep old watermarks). A failed group does not affect other groups.

## Upload Lifecycle

```
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

## Components

| Component | Path | Responsibility |
|-----------|------|----------------|
| `SalesforceClient` | `adapters/sf-client.ts` | Per-org REST client with `p-limit(25)` concurrency, gzip headers, HTTP 429 retry with exponential backoff (3 attempts max). Error formatting extracts Salesforce error details from `error.data` array. |
| `ElfReader` | `adapters/readers/elf-reader.ts` | Queries EventLogFile, concurrently downloads blob streams via `getBlobStream()`. Each blob processor accumulates lines into batches of 2000 and pushes them to a shared `AsyncChannel<string[]>`, which provides backpressure and serializes delivery to the consumer. Strips CSV header from each blob. Validates `eventType` and `interval` format on construction. Bootstrap mode (no watermark): fetches only the latest record (`LIMIT 1 DESC`) to establish a watermark without bulk-loading history. |
| `SObjectReader` | `adapters/readers/sobject-reader.ts` | Builds SOQL with watermark/where/limit, prefetches next page while yielding current results as CSV lines (serialized via csv-stringify). Auto-appends `dateField` to SELECT if not in `fields`. |
| `CsvReader` | `adapters/readers/csv-reader.ts` | Local CSV file reader. Reads a file line by line and yields batches of CSV lines. |
| `AugmentTransform` | `adapters/pipeline/augment-transform.ts` | Transform stream that appends augment column values (CSV-quoted) to each line. |
| `FanInStream` | `adapters/pipeline/fan-in-stream.ts` | Merges N writer slots into one downstream chunker. Each slot is a `Writable` created via `createSlot()`; when the last slot closes (via `close` event, which fires on both finish and destroy), `downstream.end()` is called automatically. Provides backpressure by delegating each slot write to the downstream write callback. |
| `FanOutTransform` | `adapters/pipeline/fan-out-transform.ts` | Transform stream that tees each chunk to N `PassThrough` channels concurrently via `Promise.all`. Used when multiple entries share the same reader (same org + type + watermark). Channels that error are removed from the active set. |
| `RowCounter` | `adapters/pipeline/row-counter.ts` | PassThrough stream that counts rows for progress tracking via `GroupTracker.addRows()`. |
| `AsyncChannel<T>` | `adapters/pipeline/async-channel.ts` | Bounded multi-producer, single-consumer async queue. Producers call `push()` and await backpressure when the queue reaches `highWater`. Consumer iterates with `for await`. `close()` signals end-of-stream; `fail(err)` propagates an error to both producers and consumer. |
| `DatasetWriterFactory` | `adapters/writers/dataset-writer.ts` | Creates `DatasetWriter` instances per CRM Analytics dataset. `DatasetWriter.init()` queries existing metadata (required), normalizes `numberOfLinesToIgnore` to 0, creates the parent record, and returns a `GzipChunkingWritable`. The writable batch-compresses with 512KB flush threshold, splits at 10 MB base64 boundaries, and keeps at most `UPLOAD_HIGH_WATER` (= 25) part uploads in-flight via `Promise.race` backpressure. |
| `FileWriterFactory` | `adapters/writers/file-writer.ts` | Creates `FileWriter` instances per output file path. `FileWriter.init()` opens a `fs.WriteStream` (append or overwrite). Header is written on first row via `HeaderProvider.resolveHeader()`. `skip()` deletes the file on overwrite. |
| `ConfigLoader` | `adapters/config-loader.ts` | Reads JSON config, Zod schema validation, operation consistency checks, mustache token resolution (`{{sourceOrg.Id}}`, etc.) |
| `FileStateManager` | `adapters/state-manager.ts` | Atomic read/write of watermark state file (temp file + rename, mode `0o600`) |
| `ProgressReporter` | `adapters/progress-reporter.ts` | CLI progress display using `cli-progress` MultiBar. Main bar tracks entries, per-group sub-bars show real-time files fetched, rows processed, and parts uploaded. No-op for zero-length phases. Includes workaround for cli-progress non-TTY `bar.start()` bug. |
| `Pipeline` | `domain/pipeline.ts` | Core orchestration: `executePipeline` (entry point). Three phases: (1) init one `WriterSlot` + `FanInStream` per `DatasetGroup`; (2) process reader bundles concurrently — single-entry path pipes directly, multi-entry path fans out via `FanOutTransform`; (3) await each `FanInStream`-owned chunker drain, then finalize/abort each writer. `DatasetGroup` is a pure data class; `createHeaderProvider` creates the deferred header closure. |
| `Auditor` | `domain/auditor.ts` | Builds and runs pre-flight checks decomposed into `buildAuthChecks`, `buildElfChecks`, `buildInsightsChecks`. Deduplicates checks across entries. `runAudit` executes all checks via `Promise.allSettled` with `.catch()` to preserve check labels on rejection. |

## Config Validation

Config is validated at load time using Zod schemas:

- Entry type discrimination (`elf` vs `sobject`)
- Org alias format (`[a-zA-Z0-9_.-]+`)
- Salesforce identifier format (`[a-zA-Z_][a-zA-Z0-9_]*`)
- At least one entry required
- Operation consistency: all entries targeting the same `(targetOrg, targetDataset)` or `targetFile` must use the same operation

## State Management

Watermarks are persisted atomically using a temp-file-and-rename strategy:

1. Write to `.tmp-<uuid>.json` (mode `0o600`)
2. Rename to target path
3. Clean up temp file on error

The state file is only updated after successful upload, ensuring no data is skipped on retry.

## Resilience

| Feature | Behavior |
|---------|----------|
| Per-org concurrency | 25 concurrent API calls per org via `p-limit` (shared across all entries for that org) |
| HTTP 429 retry | Exponential backoff, 3 attempts max (1s, 2s, 4s) |
| Partial failure | Failed entries don't block others; watermarks only advance on success |
| Atomic state writes | Temp file + rename prevents corruption on crash |
| Streaming | Memory-bounded processing regardless of dataset size |

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Config validation fails | Fail fast, list all validation issues (Zod) |
| Operation conflict in group | Fail fast with message listing conflicting entries and operations |
| Org auth expired/missing | Fail for that org's entries, continue others |
| Fetch failure (single entry) | Skip entry, log error, watermark unchanged |
| Upload failure (group) | Abort sink, all entries in group keep old watermarks |
| No new records | Skip silently (info log), watermark unchanged |
| HTTP 429 (rate limit) | Retry with exponential backoff (3 attempts max) |

**Exit codes**: 0 = all success, 1 = partial success, 2 = fatal error

## Dependencies

| Package | Purpose |
|---------|---------|
| `@salesforce/sf-plugins-core` | SfCommand base class, standard flags |
| `@salesforce/core` | Connection, Org authentication |
| `zod` | Config schema validation |
| `p-limit` | Per-org concurrency limiting (25 slots) |
| `csv-stringify` | CSV serialization (SObject fetcher row output) |
| `cli-progress` | CLI progress bar with ETA |
| `vitest` | Test framework |

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
