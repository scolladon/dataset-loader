# Dataset Loader — Profiling Guide

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

**8 entries**, 3 source orgs (`xrmru`, `alm-prod`, `alm-dev`), 1 target CRM Analytics org (`alm-devops`).

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
cp profiling/profiling.config.json dataset-load.config.json

# Ensure profiles/ output directory exists
mkdir -p profiles
```

The config copy is only needed if `dataset-load.config.json` has been modified since the baseline was created.

---

## Between-run reset

**Always run this between profiling runs** — otherwise the state watermarks advance and subsequent runs fetch less data, making results incomparable.

```bash
bash profiling/reset.sh
```

This:
1. Restores `.dataset-load.state.json` from `profiling/state.json`
2. Removes generated CSV output files: `AllLightningPageView.csv`, `ProdLightningPageView.csv`, `XrmruUser.csv`

---

## Run commands

### Wall-clock baseline (3 runs, reset between each)

```bash
bash profiling/reset.sh && time sf dataset load
bash profiling/reset.sh && time sf dataset load
bash profiling/reset.sh && time sf dataset load
```

Record real time from each. If variance > 20%, network noise is high — take 5 runs and drop the outlier.

### CPU profile

```bash
bash profiling/reset.sh
mkdir -p profiles
NODE_OPTIONS="--cpu-prof --cpu-prof-dir=./profiles --cpu-prof-interval=100" sf dataset load
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
NODE_OPTIONS="--heap-prof --heap-prof-dir=./profiles" sf dataset load
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

In `processBundle`:

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

- **FLUSH_THRESHOLD 64KB → 512KB** (`src/adapters/writers/dataset-writer.ts`)
  - Effect: 13% fewer gzip parts (46 → 40), compression ratio slightly better
  - Wall-clock impact: within network variance (no measurable gain)
  - Keep: reduces CRM Analytics API round-trips at no cost

### Recommended (ordered by expected impact)

**1. Batch lines before entering the pipeline** *(highest ROI)*

Replace per-line yields with batches of N lines (e.g., 1000). This collapses ~1.84M Promise resolutions to ~1,840 batch operations — roughly 1000x fewer microtask flushes in the data path. Requires the augment/counter/forwarder/chunker pipeline to accept `string[]` instead of `string`.

**2. Eliminate the PassThrough aggregation hop in ElfReader**

The `aggStream` PassThrough + `writeSeq` chain adds a full stream buffer layer. For the serialization purpose, a simpler async mutex (a single `Promise`-based lock variable) achieves the same MaxListeners protection without the extra stream hop. This removes one async boundary per line.

**3. Replace readline with manual newline splitting**

`readline.createInterface` emits one 'line' event per line — each is a `nextTick`-scheduled callback. Processing larger chunks from stream `data` events and splitting manually reduces scheduling overhead for high-volume blobs.

**4. Add upload backpressure to GzipChunkingWritable** *(robustness)*

Currently `uploadPromises[]` grows without bound until `_final`. For very large datasets this could accumulate many in-flight requests. Add a high-water mark: pause `_write` when in-flight count exceeds N, resume when some complete.

---

## Post-optimization results — 2026-03-26

### Applied improvements (branch `perf/reduce-async-overhead`)

| # | Change | File(s) |
|---|--------|---------|
| 1 | Replace `readline` with manual chunk splitting in ElfReader | `src/adapters/readers/elf-reader.ts` |
| 2 | Batch lines (2000) through the entire pipeline (`AsyncIterable<string[]>`) | `src/ports/types.ts`, all adapters |
| 3 | Eliminate `aggStream` PassThrough hop — replaced with `AsyncChannel<string[]>` | `src/adapters/readers/elf-reader.ts`, `src/adapters/pipeline/async-channel.ts` |
| 4 | Upload backpressure in `GzipChunkingWritable` (`UPLOAD_HIGH_WATER = DEFAULT_CONCURRENCY = 25`) | `src/adapters/writers/dataset-writer.ts`, `src/adapters/sf-client.ts` |
| 5 | `FLUSH_THRESHOLD` 64KB → 512KB (already on main, carried forward) | `src/adapters/writers/dataset-writer.ts` |

### Verification (3 runs, same scenario, same state reset procedure)

| Run | Before | After |
|-----|--------|-------|
| 1   | 57s    | 58s   |
| 2   | 61s    | 60s   |
| 3   | 62s    | 56s   |
| **Avg** | **~60s** | **~58s** |

### Conclusion

**Wall-clock improvement: negligible (~2s / ~3%)**, within network variance.

The optimizations correctly targeted the O(n) async overhead identified in profiling, but the measured data reveals the bottleneck was not in JS-land as much as the CPU profile suggested. The 56.8% idle time is the Salesforce API network latency — there is no client-side optimization that can reduce it.

The improvements are still **kept** because they:
- Reduce GC pressure (~1.84M fewer Promise allocations per run)
- Eliminate stream machinery overhead (PassThrough, `writeSeq` chain)
- Cap concurrent in-flight uploads at `DEFAULT_CONCURRENCY` to bound heap pressure
- Improve code clarity (`AsyncChannel` vs `aggStream + writeSeq`)

**True bottleneck**: Salesforce API throughput for the ~40 `InsightsExternalDataPart` POST calls and the ~11 days of ELF blob downloads. No client-side optimization can overcome network latency.

---

## Post-optimization results — 2026-03-27

### Applied improvements (branch `refactor/batch-middleware`)

| # | Change | File(s) |
|---|--------|---------|
| 1 | Remove 2 stream hops per entry (AugmentTransform + RowCounter) via `FanInStream` slot `BatchMiddleware` chain | `src/adapters/pipeline/fan-in-stream.ts`, `src/domain/pipeline.ts` |
| 2 | Row counting moved into writers via `ProgressListener.onRowsWritten` | `src/adapters/writers/dataset-writer.ts`, `src/adapters/writers/file-writer.ts` |
| 3 | Config validation: augment column names + SObject fields consistency per DatasetKey | `src/adapters/config-loader.ts` |
| 4 | `createGzip({ level: constants.Z_BEST_SPEED })` — compression level 6 → 1 | `src/adapters/writers/dataset-writer.ts` |
| 5 | Remove `FLUSH_THRESHOLD` periodic gzip flush block (wouldExceed handles boundary) | `src/adapters/writers/dataset-writer.ts` |

### Verification (3 runs, same scenario, same state reset procedure)

| Run | Before (2026-03-26) | After |
|-----|---------------------|-------|
| 1   | 58s                 | 37s   |
| 2   | 60s                 | 36s   |
| 3   | 56s                 | 34s   |
| **Avg** | **~58s**        | **~35.6s** |

### Conclusion

**Wall-clock improvement: ~22s / ~39% faster.**

The primary driver was `Z_BEST_SPEED` gzip compression (change #4). The CPU profile had shown zlib at 15% of active CPU (3,078ms / ~59s run), suggesting gzip was a minor cost — but that underestimated the serialization effect: the gzip thread pool was blocking the upload pipeline. At level 1, compression throughput is ~3x faster, chunks complete sooner, and S3 uploads start earlier, reducing pipeline stall time significantly.

The part count increased from ~40 to ~45–48 (less compression = larger gzip output per chunk = more part boundaries hit), but the faster per-part throughput more than compensates.

The stream hop removals (changes #1, #2) reduced GC pressure and async scheduling overhead but do not show a measurable wall-clock delta independently — their contribution is absorbed into the overall improvement.

**Updated bottleneck understanding**: Gzip CPU was a meaningful serialization bottleneck for this workload (1.84M lines, ~40 parts). `Z_BEST_SPEED` is the correct operating point for this use case — upload throughput is the constraint, not compression ratio.

---

## Gzip level investigation — 2026-03-27

### Hypothesis

Level 1 (`Z_BEST_SPEED`) produces ~22% more compressed output than level 5, resulting in ~46 parts vs ~40. Fewer parts = fewer Salesforce API round-trips. Can level 5 recover part count and reduce wall clock despite the added CPU cost?

### Compression benchmark (10MB real ELF LightningPageView data)

| Level | Time/chunk | Output | Ratio | vs Level 1 |
|-------|-----------|--------|-------|------------|
| 1 | 26ms | 1,582KB | 15.5% | baseline |
| 2 | 26ms | 1,517KB | 14.8% | −4% output, same time |
| 3 | 29ms | 1,470KB | 14.4% | −7% output, +10% time |
| 4 | 45ms | 1,339KB | 13.1% | −15% output, +72% time |
| **5** | **54ms** | **1,228KB** | **12.0%** | **−22% output, +106% time** |
| 6 | 67ms | 1,197KB | 11.7% | −24% output, +156% time |

Levels 1–3 are essentially the same speed. Level 4 is the first meaningful compression jump (+15% smaller output). Level 5 gives 22% smaller output at 2× the CPU cost. Level 6 adds nothing over level 5.

### Level 5 wall-clock results (3 runs, same scenario)

| Run | Level 1 (baseline) | Level 5 | Parts (L5) |
|-----|-------------------|---------|------------|
| 1 | 37s | 44s | 36–38 |
| 2 | 36s | 47s | 36–37 |
| 3 | 34s | 46s | 36–37 |
| **Avg** | **~35.6s** | **~45.5s** | **~36** |

### Conclusion

**Level 5 is ~10s slower than level 1 despite producing 22% fewer bytes and 10 fewer parts.**

Root cause: at level 5 (54ms/chunk), gzip compression on the libuv thread pool serializes the upload pipeline. Each 10MB part takes 54ms to compress before its upload can begin. With 36 chunks sequential in the gzip thread pool, the cumulative gzip delay (~1,944ms) adds visible pipeline stall time that exceeds the network savings from fewer parts.

At level 1 (26ms/chunk), gzip completes fast enough that the libuv thread pool keeps up with the JS pipeline — uploads start almost immediately after each chunk is written. At level 5, gzip becomes the rate limiter between chunk completion and upload start.

The Salesforce API bottleneck is **per-request latency**, not **bandwidth** — uploading 46 small-compressed parts concurrently is faster than uploading 36 larger-compressed parts because latency (not bytes) dominates. Fewer parts helps only if bandwidth is the bottleneck.

**`Z_BEST_SPEED` (level 1) is confirmed not optimal vs level 5.** Reverted to level 1, then tested level 3 — see below.

### Level 3 wall-clock results (3 runs)

| Run | Level 1 | Level 3 | Level 5 | Parts (L3) |
|-----|---------|---------|---------|------------|
| 1 | 37s | 33s | 44s | 42–45 |
| 2 | 36s | 39s | 47s | 42–45 |
| 3 | 34s | 36s | 46s | 42–43 |
| **Avg** | **~35.6s** | **~36s** | **~45.5s** | **~42–44** |

### Verdict: level 3 is the sweet spot

Level 3 and level 1 are **statistically identical in wall clock** (within network jitter of ±3s). Level 3 however produces ~7% smaller compressed output (14.4% vs 15.5% ratio) at the same CPU cost (29ms vs 26ms per 10MB chunk), yielding ~4 fewer parts per run (~42 vs ~46).

The benefit is not wall-clock time but **API call efficiency**: 4 fewer `InsightsExternalDataPart` POST requests per run reduces Salesforce API rate limit consumption. At identical throughput this is a free win.

Level 5 is clearly worse: 2× compression CPU (54ms/chunk) creates pipeline backpressure that adds ~10s wall clock even though it reduces part count further.

**Final choice: level 3.** Applied and committed.

---

## CPU profile analysis — 2026-03-27

Run with `--cpu-prof-interval=100` covering the full CLI invocation (wall clock ~35s).

**JS thread active time: ~1,934ms** (V8 CPU profiler measures JS-thread CPU only; network I/O is invisible — the process is parked in the event loop during the ~33s of Salesforce API calls).

### JS CPU breakdown

| Category | Time | % of JS CPU |
|----------|------|------------|
| Idle (event loop between microtasks) | 872ms | 45% |
| CLI startup (sf + dep loading) | 410ms | 21% |
| Pino logger shutdown (thread-stream flush) | 230ms | 12% |
| HTTP / TLS / jsforce | 203ms | 10% |
| Domain code (elf-reader, pipeline, writers) | 169ms | 9% |
| GC | 24ms | 1.2% |
| zlib | **8ms** | **0.4%** |
| Streams / promises | 6ms | 0.3% |

### Comparison with 2026-03-26 baseline profile

| Category | Before (per-line pipeline) | After (batch pipeline) |
|----------|--------------------------|----------------------|
| zlib | 3,078ms (15% active) | **8ms (0.4%)** |
| streams | 5,095ms (24.8% active) | 6ms |
| GC | 3,113ms | 24ms |
| Domain code | ~413ms async iter | 169ms |

`Z_BEST_SPEED` + 2000-line batching effectively eliminated gzip and stream overhead. All remaining JS CPU is startup, shutdown, and framework overhead — not domain code.

### True wall-clock cost breakdown (35s)

```
~33.0s  Salesforce API network I/O        (94%)  ← irreducible
 ~0.9s  Event loop idle between tasks      (2.5%)
 ~0.4s  CLI startup (sf + deps)            (1.2%)
 ~0.2s  Pino shutdown (thread-stream flush)(0.7%) ← investigated below
 ~0.2s  HTTP/TLS/jsforce init              (0.6%)
 ~0.0s  zlib, streams, GC, domain code    (<0.2%)
```

### Pino shutdown investigation (230ms)

The `thread-stream` shutdown is triggered by `@salesforce/core` Logger. In production (no `SF_DISABLE_LOG_FILE=true`), the Logger creates a pino transport pipeline:

```
pino → transformStream (pino-abstract-transport worker) → rotating file (~/.sf/sf-YYYY-MM-DD.log)
```

The transport runs in a worker thread (`thread-stream`). On process exit, `signal-exit` → `on-exit-leak-free` → `pino/transport.autoEnd` synchronously joins the worker thread — blocking the event loop for ~230ms while the worker flushes and terminates.

**Can the plugin fix this?** No. The Logger singleton is initialized by the sf CLI before any command `run()` is called. By the time plugin code executes, the thread-stream transport is already running. Setting `SF_DISABLE_LOG_FILE=true` in the environment before launch eliminates it (uses in-memory logger instead), but this is a user/environment concern, not plugin-controllable.

**Is it worth fixing at the sf CLI level?** The 230ms is invisible against network variance (1–3s jitter). It is a fixed constant per process invocation regardless of data volume. Not a target for further optimization.

---

## Post-optimization results — 2026-03-28

### Applied improvements (branch `refactor/performance-tweaks`)

| # | Change | File(s) |
|---|--------|---------|
| 1 | Extract `_write` nested closures into named prototype methods (`writeBatch`, `rotateIfNeeded`, `finishAndRotate`) | `src/adapters/writers/dataset-writer.ts` |

Note: `denque` ring buffer was implemented and reverted — see [Denque investigation](#denque-investigation) below.

### Wall-clock results (3 runs)

| Run | Before (2026-03-27) | After |
|-----|---------------------|-------|
| 1   | ~35s                | 46s   |
| 2   | ~36s                | —     |
| 3   | ~34s                | —     |

Network degraded during this session (~40s → ~50s per run on the 2026-03-27 baseline when re-measured). The 46s run is **within network variance** — wall clock is irreducible network I/O.

Data also grew: state watermark advanced from 2026-03-27 baseline (1.84M rows, ~11 days) to 2.25M rows (~13 days from 2026-03-15), adding ~7% more data and 7 additional parts (42 → 49).

### Instrumented run results (phase + network + memory timing)

**Phase breakdown:**

| Phase | Time | % of pipeline |
|-------|------|---------------|
| Phase 1 (writer init + metadata queries) | 270ms | 0.6% |
| Phase 2 (bundle processing) | 44,281ms | 96.2% |
| Phase 3 (finalize — drain + PATCH) | 1,487ms | 3.2% |
| **Total pipeline** | **46,038ms** | — |

**Network call latencies (wait = time in pLimit queue, req = actual HTTP round-trip):**

| Operation | Count | Wait (ms) | Req avg (ms) | Req range (ms) |
|-----------|-------|-----------|--------------|---------------|
| GET query (SOQL metadata) | 8 | 0–3 | 193 | 47–331 |
| GET blob (metadata JSON) | 2 | 0 | 46 | 41–50 |
| GET stream (ELF blobs, small files) | 14 | 0–5 | 302 | 192–399 |
| GET stream (ELF blobs, large files) | 11 | 0–5 | 1,571 | 1,372–1,781 |
| POST InsightsExternalData | 2 | 0 | 193 | 170–215 |
| POST InsightsExternalDataPart | 50 | 0–16 | 891 | 319–1,803 |
| PATCH InsightsExternalData | 2 | 0 | 982 | 845–1,118 |

**pLimit saturation**: wait times 0–16ms throughout. pLimit(25) is **never a bottleneck** — the 16ms peak was a momentary burst where all 25 slots were occupied simultaneously.

**Memory:**

| Metric | Value |
|--------|-------|
| Peak RSS | 763.0 MB |
| Final RSS (end of pipeline) | 752.0 MB |
| Peak heap | ~318 MB |
| Peak external (Node Buffers) | ~155 MB |

External buffers peak at ~155MB = gzip chunks (up to 10MB × 25 concurrent) + HTTP response buffers.

### Key findings

**Network dominates absolutely.** 50 parts × 891ms avg ÷ 25 concurrent = ~1.8s minimum upload time if perfectly parallelized. But parts are produced sequentially (gzip one chunk at a time) so they stagger out over ~44s. The 11 large ELF blobs also stagger: 11 × 1,571ms serial = ~17s of just blob downloading for the bottleneck bundle.

**Two bottlenecks in Phase 2:**
1. **ELF blob download rate** — 11 large blobs at 1,372–1,781ms each (serial within each bundle). These drive when parts become available for upload.
2. **Upload round-trip latency** — 891ms per part, 50 parts, up to 25 concurrent. With staggered production, actual throughput is limited by the pipeline.

**pLimit(25) is appropriately sized.** Wait times confirm headroom even at peak.

**Method extraction (closure → prototype):** no measurable wall-clock impact, as expected. The optimization is GC-pressure/allocation quality, not throughput.

---

## Denque investigation — 2026-03-28 {#denque-investigation}

### Hypothesis

`Array.shift()` in `AsyncChannel` is O(n) — for each dequeue, V8 reindexes all remaining elements. `Denque` (ring buffer, 5M weekly downloads, used by ioredis/bull) offers O(1) push/shift. With 2.25M batches per run, replacing the array should reduce JS CPU.

### Micro-benchmark result

Tight push/shift loop, 1M ops, after JIT warmup:

| Implementation | Time |
|----------------|------|
| Denque | 18.6ms |
| Array | 57.7ms |

3× speedup in isolation.

### Real-workload result

| Run | main (Array) | denque |
|-----|-------------|--------|
| 1 | ~40s | 66s |
| 2 | ~42s | 67s |

+26s wall clock (+~8–10s user CPU after controlling for network variance).

### Root cause: V8 cannot optimize Denque

Three compounding factors:

1. **Built-in intrinsics**: `Array.shift()` is a V8 C++ built-in. V8's JIT inlines it and applies SIMD-assisted memory moves for element reindexing. `Denque.shift()` is a JavaScript prototype method in an external CJS module — V8 cannot inline it into the hot `AsyncChannel` loop. Each call crosses a function boundary that prevents the JIT from seeing the loop as a single compilation unit.

2. **HOLEY_ELEMENTS**: Denque v2.1.0 does `this._storage[head] = undefined` after each shift. This creates a sparse internal array that V8 classifies as `HOLEY_ELEMENTS` instead of `PACKED_ELEMENTS`. Holey arrays require slower GC scanning and lose the fast-path element access of packed arrays.

3. **Micro-benchmark deception**: The 3× result measured both implementations in isolation after JIT warmup on tight loops. In production, V8's type feedback system marks the `AsyncChannel` hot path as "requires generic object handling" instead of "known Array fast path", which degrades the surrounding pipeline code. The deoptimization propagates outward, adding ~8–10s of user CPU across 2.25M batches.

### Conclusion

`Array.shift()` is O(n) in theory but for queue depths of 0–16 items, n is tiny and V8's native implementation beats a "theoretically faster" ring buffer that V8 cannot optimize. Denque was reverted. The Array-based queue is the correct choice for this workload.

---

## AsyncChannel queue — further alternatives considered

### Local ring buffer

A ring buffer implemented directly in `AsyncChannel` (no external dependency) avoids the "external module" deoptimization. However the HOLEY_ELEMENTS problem persists: a ring buffer must clear vacated slots (`buf[head] = undefined`) to release references, which transitions V8's internal array from `PACKED_ELEMENTS` to `HOLEY_ELEMENTS`. Skipping the clear would avoid this but requires a type-unsafe sentinel and leaks references until the slot is overwritten. Not worth the complexity for a zero wall-clock payoff.

### Linked list with head/tail pointers

A singly-linked list avoids both Denque failure modes:

- No slot clearing on dequeue (`head = head.next` — old node becomes unreachable and is GC'd naturally) → no HOLEY_ELEMENTS transition
- Local TypeScript code in the same compilation unit → V8 can inline push/shift
- Node objects `{ value: T, next: Node | null }` are monomorphic (fixed hidden class) → no hidden-class deoptimization

The trade-off is one heap allocation per `push()`:

```typescript
push(item: T): ... {
  const node = { value: item, next: null }  // GC allocation per push
  if (this.tail) this.tail.next = node
  else this.head = node
  this.tail = node
}
```

With ~1,125 pushes per run (2.25M rows ÷ 2000 batch size), this adds ~1,125 short-lived object allocations — negligible (GC measured at 24ms total for the entire run).

**The ceiling is the same.** Even granting that a linked list would not regress like Denque, the absolute savings are unmeasurable:

```
AsyncChannel contributes a fraction of domain code (169ms total)
Linked list vs Array.shift() on depth 0–3: saves ~20–30ms
Wall clock: 46,000ms
Impact: <0.1% — below network jitter floor (±1–3s)
```

### Decision: keep Array

The Array-based queue is the correct long-term choice — not because it is theoretically fastest, but because:

1. The operation is not the bottleneck (94% of wall clock is Salesforce API I/O)
2. V8 already fast-paths `Array.shift()` for small packed arrays via native intrinsics
3. Both alternatives add code complexity for an unmeasurable gain
4. The linked list is the theoretically cleanest O(1) option and would not regress — but "would not make things worse" is not sufficient justification when the gain is zero

