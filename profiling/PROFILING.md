# CRMA ELF Loader — Profiling Guide

## Directory contents

```
profiling/
  PROFILING.md              this file
  profiling.config.json     baseline config for reproducible runs
  profiling.state.json      baseline state (watermarks as of 2026-03-15)
  reset.sh                  reset script — run between every profiling run
```

CPU/heap profile outputs go in `profiles/` (git-ignored, created on demand).

---

## Scenario

**8 entries**, 3 source orgs (`xrmru`, `alm-prod`, `alm-dev`), 1 target CRMA org (`alm-devops`).

**Reader bundles** (grouped by ReaderKey + watermark):

| Bundle | Source | Type | Watermark | Fan-out |
|--------|--------|------|-----------|---------|
| pageviews-xrmru | xrmru | ELF LightningPageView Daily | 2026-03-15 | solo → Test_LightningPageView |
| pageviews-prod | alm-prod | ELF LightningPageView Daily | 2026-03-15 | solo → Test_LightningPageView |
| pageviews-prod-all + prod-file | alm-prod | ELF LightningPageView Daily | none | fan-out → AllLightningPageView.csv + ProdLightningPageView.csv |
| pageviews-xrmru-all | xrmru | ELF LightningPageView Daily | none | solo → AllLightningPageView.csv |
| users-xrmru + xrmru-file | xrmru | SObject User | none | fan-out → Test_User + XrmruUser.csv |
| users-dev | alm-dev | SObject User | none | solo → Test_User |

**Bottleneck bundle**: `alm-prod ELF LightningPageView Daily wm=2026-03-15` → ~11 days of incremental data, **1.84M lines**, ~40 gzip parts uploaded.

---

## Pre-run setup

```bash
# Copy profiling baseline config over the active config
cp profiling/profiling.config.json crma-load.config.json

# Ensure profiles/ output directory exists
mkdir -p profiles
```

The config copy is only needed if `crma-load.config.json` has been modified since the baseline was created.

---

## Between-run reset

**Always run this between profiling runs** — otherwise the state watermarks advance and subsequent runs fetch less data, making results incomparable.

```bash
bash profiling/reset.sh
```

This:
1. Restores `.crma-load.state.json` from `profiling/state.json`
2. Removes generated CSV output files: `AllLightningPageView.csv`, `ProdLightningPageView.csv`, `XrmruUser.csv`

---

## Run commands

### Wall-clock baseline (3 runs, reset between each)

```bash
bash profiling/reset.sh && time sf crma load
bash profiling/reset.sh && time sf crma load
bash profiling/reset.sh && time sf crma load
```

Record real time from each. If variance > 20%, network noise is high — take 5 runs and drop the outlier.

### CPU profile

```bash
bash profiling/reset.sh
mkdir -p profiles
NODE_OPTIONS="--cpu-prof --cpu-prof-dir=./profiles --cpu-prof-interval=100" sf crma load
```

Opens as `profiles/CPU.*.cpuprofile` — load in **Chrome DevTools → Performance → Load profile**.

Focus on:
- `streams` — Node.js stream machinery (nextTick, clearBuffer, writeOrBuffer)
- `zlib` — gzip compression (runs on libuv thread pool, explains >100% CPU)
- GC — heap allocation churn
- `writeToAgg`, `writeSeq`, async iterator steps — ElfReader O(n) chain

### Heap profile

```bash
bash profiling/reset.sh
mkdir -p profiles
NODE_OPTIONS="--heap-prof --heap-prof-dir=./profiles" sf crma load
```

Opens as `profiles/Heap.*.heapprofile` — load in **Chrome DevTools → Memory → Load allocation profile**.

Focus on:
- `Buffer[]` in `GzipChunkState.chunks` — up to 10MB per in-flight chunk
- `uploadPromises[]` in `GzipChunkingWritable` — grows until `_final`
- Closure chains from `writeSeq` in ElfReader
- PassThrough channel buffers

### Memory polling (temporary instrumentation — add to `src/domain/pipeline.ts`)

Add inside `executePipeline`, before the Phase 1 loop:

```typescript
let peakRss = 0
const memInterval = setInterval(() => {
  const m = process.memoryUsage()
  peakRss = Math.max(peakRss, m.rss)
  process.stderr.write(
    `[MEM] rss=${(m.rss / 1024 / 1024).toFixed(1)}MB heap=${(m.heapUsed / 1024 / 1024).toFixed(1)}MB ext=${(m.external / 1024 / 1024).toFixed(1)}MB\n`
  )
}, 2000)
```

Add after `await input.state.write(store)`:

```typescript
clearInterval(memInterval)
const mFinal = process.memoryUsage()
process.stderr.write(
  `[MEM] peak_rss=${(peakRss / 1024 / 1024).toFixed(1)}MB final_rss=${(mFinal.rss / 1024 / 1024).toFixed(1)}MB final_heap=${(mFinal.heapUsed / 1024 / 1024).toFixed(1)}MB\n`
)
```

Remove after use. Do not commit.

### Phase timing (temporary instrumentation — add to `src/domain/pipeline.ts`)

In `executePipeline`:

```typescript
const t0 = performance.now()
// ... Phase 1 loop ...
const t1 = performance.now()
input.logger.info(`[PIPELINE] Phase1 init: ${(t1 - t0).toFixed(0)}ms`)

// ... Phase 2 Promise.all ...
const t2 = performance.now()
input.logger.info(`[PIPELINE] Phase2 bundles: ${(t2 - t1).toFixed(0)}ms`)

// ... Phase 3 Promise.all ...
const t3 = performance.now()
input.logger.info(`[PIPELINE] Phase3 finalize: ${(t3 - t2).toFixed(0)}ms`)
input.logger.info(`[PIPELINE] Total pipeline: ${(t3 - t0).toFixed(0)}ms`)
```

In `processBundleEntries`:

```typescript
const bundleLabel = `${bundle.readerKey.toString().replace(/\0/g, ':').substring(0, 60)} wm=${bundle.watermark?.toString() ?? 'none'}`
const tb = performance.now()
// ... processing ...
const te = performance.now()
input.logger.info(`[BUNDLE] ${bundleLabel} | entries=${active.length} | ${(te - tb).toFixed(0)}ms`)
```

Remove after use. Do not commit.

### Network timing (temporary instrumentation — add to `src/adapters/sf-client.ts`)

Wrap each method's limiter call to capture queue wait and request duration:

```typescript
// In each method (query, post, patch, etc.) — example for post:
post<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const t0 = performance.now()
  return this.limiter(() => {
    const t1 = performance.now()
    return withRetry(
      () => this.connection.request<T>({ ... }).then(result => {
        const t2 = performance.now()
        process.stderr.write(`[SF] POST ${path.substring(0, 60)} wait=${(t1-t0).toFixed(0)}ms req=${(t2-t1).toFixed(0)}ms\n`)
        return result
      }),
      this.baseDelay
    )
  })
}
```

Remove after use. Do not commit.

---

## Results — 2026-03-26

### Baseline (3 runs)

| Run | Wall clock |
|-----|-----------|
| 1 | 57s |
| 2 | 61s |
| 3 | 62s |

### Phase breakdown

| Phase | Time | % |
|-------|------|---|
| Phase 1 (writer init + metadata query) | ~200ms | <1% |
| Phase 2 (bundle processing) | ~56s | 96% |
| Phase 3 (PATCH Action:Process) | ~300ms | <1% |

Phase 2 is dominated by one bundle: **alm-prod ELF LightningPageView wm=2026-03-15** (50–56s, 98% of Phase 2). All other bundles complete in under 1s.

### CPU profile summary

```
Total sampled: 47,491ms   Wall clock: ~59s
Idle (I/O + async scheduling): 26,984ms (56.8%)

Active CPU breakdown:
  streams    :  5,095ms  (24.8%)
  gc         :  3,113ms  (15.2%)
  zlib       :  3,078ms  (15.0%)
  other      :  7,220ms  (35.2%)
  async iter :    413ms
```

### Memory observations

- Peak RSS: **667MB** during Phase 2 (alm-prod bundle processing)
- Final RSS after finalize: **192MB**
- No permanent leak — buffers released after `Promise.all(uploadPromises)` drains
- 46 upload promises accumulated at `_final` for 40 parts (minor double-counting from flush boundary)

### Hypotheses

| # | Hypothesis | Result |
|---|-----------|--------|
| H1 | Network I/O dominates (>80% wall clock) | PARTIAL — 56.8% idle, but async overhead is comparable |
| H2 | pLimit(25) saturated on target org | REJECTED — wait times 0–5ms throughout |
| H3 | Base64 encoding of 10MB chunks is CPU-expensive | REJECTED — 0.3–0.9ms per chunk, negligible |
| H4 | uploadPromises[] grows unbounded | PARTIAL — 46 promises at drain, no permanent leak |
| H5 | Gzip flush threshold (64KB) causes event loop churn | PARTIAL — fix applied (→512KB), 13% fewer parts, no wall-clock gain |
| H6 | Fan-out backpressure couples fastest to slowest | CONFIRMED CORRECT — working as designed |
| H7 | writeSeq promise chain leaks closures | NOT CONFIRMED — no accumulation in heap profile |
| H9 | O(n) async overhead for 1.84M lines is the real bottleneck | **CONFIRMED — primary finding** |

### Primary bottleneck (H9): O(n) async chain in ElfReader

Each of the 1.84M lines traverses multiple async boundaries in `ElfReader.fetch()`:

```
readline 'line' event
  → await writeToAgg(line)        1.84M chained .then() links
    → aggStream.write(chunk)      PassThrough intermediate hop
      → for await (line of aggStream)   1 async iteration step per line
        → yield line
          → pipeline: augment → counter → forwarder → chunker._write()
```

This produces ~9M Promise resolutions + microtask flushes. The `streams` CPU cost (5,095ms, 24.8% of active) is the Node.js stream machinery processing each line individually through `nextTick`, `clearBuffer`, `writeOrBuffer`, `onwrite`.

Observed throughput: **38,333 lines/sec** → 48s for 1.84M lines. This matches wall clock.

The `writeSeq` chain exists to serialize concurrent blob writes and prevent MaxListeners overflow — it's architecturally correct but adds per-line Promise overhead.

---

## Improvement recommendations

### Applied

- **FLUSH_THRESHOLD 64KB → 512KB** (`src/adapters/dataset-writer.ts`)
  - Effect: 13% fewer gzip parts (46 → 40), compression ratio slightly better
  - Wall-clock impact: within network variance (no measurable gain)
  - Keep: reduces CRMA API round-trips at no cost

### Recommended (ordered by expected impact)

**1. Batch lines before entering the pipeline** *(highest ROI)*

Replace per-line yields with batches of N lines (e.g., 1000). This collapses ~1.84M Promise resolutions to ~1,840 batch operations — roughly 1000x fewer microtask flushes in the data path. Requires the augment/counter/forwarder/chunker pipeline to accept `string[]` instead of `string`.

**2. Eliminate the PassThrough aggregation hop in ElfReader**

The `aggStream` PassThrough + `writeSeq` chain adds a full stream buffer layer. For the serialization purpose, a simpler async mutex (a single `Promise`-based lock variable) achieves the same MaxListeners protection without the extra stream hop. This removes one async boundary per line.

**3. Replace readline with manual newline splitting**

`readline.createInterface` emits one 'line' event per line — each is a `nextTick`-scheduled callback. Processing larger chunks from stream `data` events and splitting manually reduces scheduling overhead for high-volume blobs.

**4. Add upload backpressure to GzipChunkingWritable** *(robustness)*

Currently `uploadPromises[]` grows without bound until `_final`. For very large datasets this could accumulate many in-flight requests. Add a high-water mark: pause `_write` when in-flight count exceeds N, resume when some complete.
