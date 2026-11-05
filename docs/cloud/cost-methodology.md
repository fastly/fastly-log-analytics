# Cost methodology

How this install reports and estimates cost. Admins see it on `/usage`; anyone can use the public `/calculator`. **Usage is not an invoice.** Fastly bills by contract, and nothing here calls a Fastly billing API.

Spec: `docs/superpowers/specs/2026-10-30-operational-logging-and-telemetry-design.md` §7.

## Measured vs. estimated

Every figure on `/usage` is labelled.

| Figure | Label | Source |
|---|---|---|
| Class A / Class B / CDN-hit operation counts | measured | `fos_usage` rows recorded at the S3 and `/fos` chokepoints |
| Dollars for those operations | estimated | `est_cost_usd`, priced at the `UsageLogging` rates |
| CDN hit ratio on `/fos` reads | measured | `fos_usage` GETs by `cdn_cache` |
| Compute requests and vCPU ms by route and cron kind | measured | `api_telemetry`, `cron_runs`, `cron_ticks` (`vcpu_ms`) |
| Billed excess Compute vCPU ms | estimated | per-execution excess CPU above `compute_vcpu_free_ms_per_request`, rounded to the nearest `compute_vcpu_rounding_ms` |
| KV and Cache API operations | measured counts | not priced |
| Storage by dataset | estimated | storage ledger (below) |
| Retention lag | measured | time since the last completed retention sweep |

The **Estimated Incurred Cost** card prices every product the install uses: FOS storage and operations, delivery (front VCL service requests and bandwidth plus Compute bandwidth), Compute requests and vCPU on both the app and worker services, KV Class A/B operations, Secret Store secrets and Config Store ($0, package entitlement). Delivery, Compute and KV figures come from the Fastly Stats API (admin only); without them Compute and delivery fall back to logged values and KV and secrets show n/a. Compute vCPU is priced per Compute execution using the contract CPU terms (`round(max(0, vcpu_ms_per_request - compute_vcpu_free_ms_per_request) / compute_vcpu_rounding_ms) × compute_vcpu_rounding_ms`, charged at `compute_vcpu_excess_rate_per_second / 1000` per excess ms); for logged telemetry, idle clock ticks (`idle_ticks` / `idle_vcpu_ms`) and active cron/worker executions are evaluated separately so sub-20 ms idle ticks do not dilute active conversion or maintenance runs. KV storage size and Cache API operations are not priced.

## Rates

`UsageLogging` (`app/src/auth/settings.rs`) is the single source of rates; defaults are list/contract price: Class A $0.0025 per 1,000 ($2.50 per million), Class B $0.004 per 10,000 ($0.40 per million), storage $0.02/GB-month, egress $0.12/GB, CDN requests $0.01 per 10k, Compute $0.50 per million requests plus contract CPU terms (`compute_vcpu_free_ms_per_request = 20` ms included per request, `compute_vcpu_excess_rate_per_second = $0.00005`/s on excess CPU—equivalent to $0.05 per million excess ms—and `compute_vcpu_rounding_ms = 10` ms nearest-increment rounding per request), KV Class A $6.50 and Class B $0.55 per million, secrets $5/month each, `min_billed_days` 30. Account-level free tiers (first 1M CDN requests, first 10 secrets) are not subtracted, while the per-request Compute CPU allowance (`compute_vcpu_free_ms_per_request`) and nearest-increment rounding (`compute_vcpu_rounding_ms`) are applied per Compute request in both `/usage` and `/calculator`. Recording is on by default; when `enabled` is false `fos_usage` is not recorded and `/usage` says how to turn it on. The calculator works per million operations, so one conversion helper turns per-1k and per-10k rates into per-million (`lib/usage/summary.ts`).

## Operation classes

PUT, LIST, DELETE and `DeleteObjects` are Class A. GET and HEAD are Class B. A CDN hit costs no Object Storage operation, and a MISS through `/fos` costs one Class B. Retention and compaction deletes and LISTs therefore cost Class A even though they remove data.

## Storage ledger and minimum billing

There is no recursive storage walk. Estimated storage per dataset is the last reconcile snapshot plus PUT bytes minus `bytes_deleted` recorded after the snapshot. "Reconcile storage" re-lists each dataset and records a new snapshot as a `storage_reconcile` run.

Storage cost per kind is `S_k × $0.02 × max(retention_k, min_billed_days) / 30` with `min_billed_days = 30`. A compacted-away hourly file still bills its minimum 30 days (see the lifecycle guide cited under Historical), so compaction cuts operations and listing cost, not the minimum storage charge.

## Shield trade-off

Every `flp` execution runs at the IAD shield. That adds latency for far-away callers but concentrates cache lookups and collapses CDN fills, which lowers Class B reads. The effect is visible as the CDN hit ratio.

## Overhead of the logging system itself

A quiet install (defaults) adds roughly 93 Class A and 168 Class B operations a day, about $0.00030/day at list price (FOS operations only), plus retention LIST and delete batches at $0.0000025 each. Storage and egress follow the formulas above. These are estimates from Spec §7.1 until measured on a live install. `/usage` shows the measured figures under "Logging overhead" (calls from `log_flush`, `log_compact` and `retention_sweep`).

## Calculator

Public and fully working without sign-in; `/usage` embeds it as the Cost Estimator. Like FLA, it extrapolates a month from the logged service's traffic and logging settings rather than pricing hand-entered monthly quantities. Logged lines a day are requests (or edge requests when edge-only) × sample rate; raw files a day are the expected files when those lines spread over `files_per_period` writing nodes each period, `P × N × (1 − e^(−L/P/N))`. Each product's monthly cost is 30 × (fixed per day + per raw file × files + per 1,000 lines × lines). Every Compute vCPU component (app fixed, worker conversion, summary bundles, split raw files, relayed fan-out, RUM and compaction) is priced per execution using the contract CPU formula: divide daily vCPU ms by the line's daily execution count, subtract `compute_vcpu_free_ms_per_request` (20 ms default), round excess ms to the nearest `compute_vcpu_rounding_ms` (10 ms default), multiply by daily executions × 30 days, and price at `compute_vcpu_excess_rate_per_second / 1000`. Admins get the install's measured values: traffic from the stats API over the last 3 complete UTC days (`/api/admin/usage/logged-traffic`), and unit costs from up to 7 days of Compute billing (divided by the hours Fastly actually reported), `ingest_files`, `request_ingest` cron runs, `fos_usage` and the latest storage reconcile. Billed KV and front CDN requests are split into a per-file default and a fixed remainder, because those operations are not logged per caller. See [`docs/pages/calculator.md`](pages/calculator.md).

### Summary bundles (always on)

The ingest worker writes one summary Parquet next to each window file (Task 22.1), and a maintenance stage merges a closed hour's summaries into an hour bundle and hours into a day bundle (Task 22.2). The calculator prices them on their own "Summary bundles" line:

- **Window files a day** `W = window_files_per_file × F`. It is measured as `request_ingest` runs' `parquet_files_written` ÷ converted raw files; the default 0.125 is one window file per worker execution at the reference install's measured 0.125 worker requests per raw file.
- **Class A:** one PUT per window summary, 24 hour bundles and 1 day bundle a day (`W + 25`).
- **Class B and front requests:** the hour bundles read every window summary through `/fos`, and the day bundle reads 24 hour bundles (`W + 24`), each counted as a front request and a Class B miss.
- **Storage:** `S(L/W) × W + 24 × S(L/24) + S(L)` bytes a day, retained like the request logs (`max(retention, min billed days)`), where `S(r) = 23 KB + ceiling × r / (r + 2,600)`.
  - The 23 KB footer, the 2,600-row half point and the 380 KB default ceiling are a fit on a native ZSTD(1) run of `write_summary` over 1–40,000 synthetic rows with about 30 fields (23 KB at 1 row, 39 KB at 100, 128 KB at 1,000, 223 KB at 5,000, 310 KB at 20,000 and 359 KB at 40,000).
  - High-cardinality services can reach a few MB per window. The worst case is 42 fields × 2,000 top values plus 42 × 500 HyperLogLog sketches of 258 B each (≈5.4 MB), so the ceiling is an editable unit.
  - Hour and day bundles keep 500 values per field, so the same curve over-states them.
- **Worker CPU:** `summary_vcpu_ms_per_1k_lines × L / 1000` total daily vCPU ms across `W` window-summary executions, priced per execution with the contract CPU allowance (`compute_vcpu_free_ms_per_request`) and nearest-increment rounding (`compute_vcpu_rounding_ms`). Task 22.4 measured 79.7 ms per 1,000 rows over 34 `request_ingest` runs (267,300 rows) on 2026-11-01. Total worker CPU was 228.7 ms per 1,000 rows, so the remainder was about 149 ms. Subsequent inline-summary tuning changed the default assumptions to 32 ms for summaries and 64 ms for other worker work; these are not a fresh live measurement. The measured prefill partitions the billed worker total: subtract the estimated summary share, capped at that total, before pricing the worker line. It does not add the summary share to an already inclusive bill; the summary share remains labelled as an assumption.
- **Maintenance CPU and repeated partial I/O:** these belong to the existing fixed daily inputs, not the worker's per-row rate. Live `summary_bundle` runs spent 197-795 ms of `elapsed_vcpu_ms()` per turn, independently of wall time, and repeatedly rewrote 426-491 KB partials. The default adds a busy-backlog allowance of 720 turns/day (one per 120 s): 795 ms vCPU (the observed maximum of 197-795 ms, not a mean), two LISTs, one partial PUT, one delete, one additional partial GET/front request and 500 KB delivered per turn. Source-window reads and completed-bundle writes remain in the summary formulas above. App execution count is unchanged because maintenance runs inside an already counted clock tick.
  - This adds 572,400 vCPU ms, 2,880 Class A operations, 720 Class B misses/front requests and 0.36 GB delivered per day. At list rates it adds about $2.40/month before partial storage. Measured app billing and FOS fixed-operation totals replace these defaults; maintenance is not added again to those inclusive totals.
  - Replaced partials still incur Object Storage's minimum duration. The editable `Partial bundles rewritten (GB/day)` starts at 0.36 GB/day and adds `0.36 × min_billed_days` GB-months to summaries, not telemetry. At 30 days this adds $0.216/month. It stays a planning allowance because current inventory does not include deleted, minimum-billed objects.
  - These assumptions budget cost, not successful convergence. Hour/day closing remains blocked under the 50 ms execution requirement (remediation Task 22.2). Reduce the fixed allowances and rewritten-partial input when the backlog clears; no live proof of that exists yet.

### Compaction (optional, off by default)

A closed hour's window files below the target size can be merged (Task 23). FOS bills every object for at least the minimum billed days, so merged-away sources still cost their full month. Option A in the remediation plan expects that to add about 100% to Parquet storage at 30-day retention and about 33% at 90 days. At high request rates window files are already 10–25 MB, so compaction mainly helps low and medium traffic.

The calculator's "Compact small Parquet files" toggle and target size use the per-service setting `compaction = { "mode": "off" | "size_target", "size_target_mb": 64 }`, default `off`. With an admin's measured data, the toggle starts from the logged service's setting when `/api/admin/usage/logged-traffic` reports it in `logging.compaction`; otherwise it starts off. The calculator only displays that setting and never writes it.

Each hour holds `W/24` files of `L × parquet_bytes_per_row / W` bytes. `compactionPlan` merges them level by level:

- Each execution merges up to 13 sources: 4 secret reads, create, complete and 2 calls per source fit the 32-call budget.
- The output stays within the target size and about 250 MB, the heap and 60 s an execution can stream.
- Compacts that are still below the target become sources at the next level, until the hour holds `ceil(hour bytes / target)` files or no two files fit in one output.

Priced on the "Compaction" line, per day:

- **Compute:** one request per execution, plus `compact_vcpu_ms_per_mb` × MB copied across `executionsPerDay` executions (priced per execution after subtracting `compute_vcpu_free_ms_per_request` and rounding excess ms to the nearest `compute_vcpu_rounding_ms`).
  - The default of 22 ms per MB is the pre-summary worker rate (97 ms per 1,000 lines before the 64 + 32 = 96 ms worker+summary split and Task 22.4's 228.7 ms measurement, ÷ 0.194 MB of Parquet per 1,000 lines ≈ 500 ms per MB written) × 0.044.
  - 0.044 is the native ratio, per output MB, of copying encoded column chunks with `append_column` (3.7 ms per MB for 13 × 42 KB sources) to converting and encoding them (83 ms per MB at 300-row windows).
  - Large sources copy at 0.6 ms per MB, so the default is conservative.
- **Class A:** per execution, a multipart create, a complete, one part per source and a `DeleteObjects` after the reader grace; plus one LIST per closed hour.
- **Class B and front requests:** one source read through `/fos` per source, counted as a miss.
- **KV:** per execution, a queue write and the delete-queue removal (Class A ×2) and the queue read (Class B).
- **Minimum-billed storage:** every merged-away byte, including intermediate compacts, is billed `min_billed_days`: `deleted GB a day × min_billed_days`. The surviving compacts replace the sources' retained bytes, so retention storage is unchanged. One level adds 100% at 30-day retention and 33% at 90 days; each further level adds the same again.
- **Offset:** `parquet hours read per day` (default 24, each closed hour fetched once) × the files each hour no longer has. Each saved `/api/file` read is an app request, a front request and a Class B miss. Panels served by summary bundles don't fetch files, so they don't count.

### RUM (optional, off without pageviews)

The tracker batches every metric and error of a pageview into one `POST /rum-beacon` per flush (Task 26.1): a 1.5 s debounce, or immediately on `pagehide`/hidden. Each flush is one lean log line, about 380 raw bytes for nine metrics, where the per-metric beacons with default field groups cost about 1,165 B × 9 lines. The tracker draws once per browser session against `rum.sample_rate` and `rum.error_sample_rate`, and unsampled sessions send nothing. `rum_ingest` runs once per `max(5, rum.log_period)` seconds, independent of the request stream (Task 26.4).

Priced on the "RUM" line, per day:

- **Beacons:** `pageviews × sample rate × beacons per pageview`, each a CDN request on the logged service and one raw line. The default is 1 beacon per pageview; long-lived pages flush more than once.
- **Storage:** raw lines × `rum_gz_bytes_per_line` (default 95, the zstd-compressed positional-array size of a ~380 B line), billed like request raw files; rows (`rows_per_line`, default 6) × `rum_parquet_bytes_per_row` (default 30), kept `max(rum retention, min billed days)`.
- **Per raw file:** raw RUM files a day use the request formula with the RUM period, priced at the request per-file units (Class A/B, front requests, worker requests, KV).
- **Runs:** one LIST of the raw RUM prefix (Class A) per due run. CPU is the request rate per 1,000 lines.

An admin's prefill sets the period, sample rate and retention from `logging` in `/api/admin/usage/logged-traffic`, and measures rows per line, Parquet bytes per row and pageviews (lines a day ÷ (sample rate × beacons per pageview)) from `rum_ingest` cron runs. Tracker and Faro bundle downloads are not priced; browsers cache them.


### Ingest capacity (Task 25.3)

The calculator's "Ingest capacity" panel (`app/web/lib/usage/capacity.ts`) checks whether request ingest keeps up with the service's average logged traffic. It is a planning model, not a measurement; Task 25.4 must confirm it live.

Limits, mirrored from `app/src/ingest`:

| Limit | Value | Source |
|---|---|---|
| Worker calls per tick | 8 | `coordinator.rs` `MAX_FANOUT`, bounded in practice by the 32-call budget |
| Shards per relayed call | 0–16 (`ingest_relay`) | `worker.rs` `MAX_RELAY_SHARDS` |
| Objects per shard | 22 | `worker.rs` `SHARD_MAX_OBJECTS` |
| Compressed bytes per shard | 8 MiB | `coordinator.rs` `SHARD_MAX_GZ_BYTES` |
| Decompressed bytes per shard / segment | 24 MiB | `mod.rs` `MAX_BATCH_DECOMPRESSED_BYTES` |
| Compressed bytes one segment reads | 24 MiB | `mod.rs` `SEGMENT_READ_LIMIT` |
| Decompressed bytes one object can be segmented over | 512 MiB | `convert.rs` `MAX_OBJECT_DECOMPRESSED_BYTES` |
| Raw keys planned per tick | 200 | `mod.rs` `LIST_PAGE` |
| Segments one object is cut into at a time | 22 | `coordinator.rs` `MAX_OBJECT_SEGMENTS` (512 MiB ÷ 24 MiB); a smaller `ingest_segment_kib` is raised to keep within it |
| Saved plan, with the results its pending shards may add | 2 MiB | `coordinator.rs` `PLAN_BYTES`, the SDK's KV read limit |
| Worker stop | 28 s | `worker.rs` `WORKER_STOP` |

From the calculator inputs (lines L/s, raw files F/s, gzip bytes per line g, assumed 8:1 compression):

- Object size: `G = L·g / F` compressed, `R = 8·G` decompressed. Segments per object `S = ceil(R / 24 MiB)` when R exceeds 2 × 24 MiB and G is at most 24 MiB, else 1 (`coordinator.rs` `segment_count`).
- Objects per shard `k = min(22, floor(24 MiB / R), floor(8 MiB / G))` (at least 1), or 1 for a segmented object.
- Shard time `t = k × 0.15 s GET + lines per shard × worker vCPU per line + 2 × 0.5 s window PUTs`. Worker vCPU per line is the calculator's conversion plus summary units.
- Tick time `T = 5 s` fixed coordinator work and clock gap `+ t`.
- Shards a tick `W = min(8 × max(relay, 1), P(k))`. `P(k)` is how many shards of k objects fit one 2 MiB plan once each reserves room for its worst-case result: up to `min((27 − k) / 2, 12k)` windows, each a window file and a summary entry, plus per object its key, a 512-byte bad-line sample escaped to 3,074 JSON bytes and its window list (`coordinator.rs` `result_reserve`; `ingestShape.ts` `planShards`, assuming 130-byte raw keys). P(1) = 123 and P(22) = 21. In the worked results below the 200-key LIST page or the 24 MiB segment read binds first, so the bound does not change them.
- Utilization `= max(F·S/k × T / W, F × T / 200)`. Over 1 is "over capacity", over 0.8 "near".
- "Not supported" when an object is over 24 MiB compressed or 512 MiB decompressed (shorten the log period), or a full shard needs more than the 28 s worker stop.
- Busiest worker execution vCPU `= min(lines per shard, L × max(log period, T)) × worker vCPU per line + (S − 1) × 24 MiB × 10 ms/MiB`. The second term is the last segment decompressing and skipping every segment before its range (`convert.rs` `read_segment`). Over Fastly's documented 50 ms per execution (`INGEST_LIMITS.executionVcpuMs`) is "Past documented CPU", with the figure in the panel.
- Throughput envelope: the highest rate (sampling, nodes and period unchanged) that is not "not supported" and has utilization ≤ 1. Supported rate: the highest that also keeps the busiest execution within 50 ms vCPU. The panel shows the throughput envelope separately when it is higher.

Worked results (CPU 96 ms/1k lines, the default conversion plus summary units; 109 nodes), supported rate / throughput envelope:

| Setup | Direct (relay 0) | Relay 16 |
|---|---|---|
| Calculator defaults (296 B/line, 300 s period) | ≈ 9 / 7.9k req/s | ≈ 9 / 29.8k req/s |
| 296 B/line, 10 s period | ≈ 258 / 9.5k req/s | ≈ 258 / 232k req/s |
| 88 B/line, 60 s period | ≈ 43 / 23.7k req/s | ≈ 43 / 455k req/s |
| 88 B/line, 300 s period | ≈ 9 / 26k req/s | ≈ 9 / 104k req/s |

**The 100k req/s target is blocked under the documented 50 ms vCPU.** Shard throughput reaches it with relay 16, but each worker execution then converts thousands of lines: at 100k req/s, 88 B/line and a 300 s period the busiest segment needs ≈ 5.1 s vCPU, 1,680 ms of it skipping 7 earlier segments. Even the calculator defaults pass 50 ms at a few hundred req/s. Ingest workers have always used more than 50 ms (`docs/compute-limits-audit.md`: the 50 ms figure is enforced only in compaction and bundles); the throughput envelope is what the pipeline does if Fastly keeps allowing that, which Task 25.4 must measure, not a supported figure. Holding to 50 ms would need far smaller shards, and so many more executions per tick than the 32-call budget dispatches; that is an architecture change, not a tuning step.

Workers check their wall stop every 1 MiB decompressed (`convert.rs` `CHECKPOINT_BYTES`), including the skipped prefix and the measurement of an object too large to convert whole. A segment that reaches the stop answers `worker_budget_stopped`: nothing is written, the raw object stays, and the coordinator counts the failure, logs it as a platform error and retries (after 3, it abandons and replans the plan). The checkpoint does not enforce 50 ms vCPU: that would stop every conversion above a few hundred lines.

At 100k req/s, 88 B/line, a 60 s period gives ≈ 4.6 MiB gzip objects, converted whole; a 300 s period gives ≈ 23 MiB gzip objects split into 8 segments. Objects past 24 MiB gzip need a shorter log period.

A plan is bounded to what the SDK can read back (2 MiB). A new plan keeps the objects whose shards, with every result they may still add, fit; the rest wait for the next plan. A split or re-segment that would pass the bound holds that shard's objects instead (reason `plan_too_large`, logged as a platform error): the commit moves them to quarantine and keeps their raw objects, so none is lost or stranded in an unreadable plan. A worker result larger than its reserve is a shard failure (`worker_result_too_large`). Straggler-sweep replans keep every key, because their sweep cursor has already moved past them.

Unmeasured assumptions that Task 25.4 must check: per-execution worker vCPU against what Fastly actually allows, the 5 s fixed tick, 0.15 s per GET, 0.5 s per window PUT, the 8:1 ratio, the decompress-and-skip CPU of later segments, and the worker → main relay hop latency. Averages hide peaks; traffic above the supported rate drains as backlog afterwards.


### High-RPS ingest costs (Task 25)

The monthly estimate adds a line for each high-RPS path, only when the calculator's inputs trigger it, so a small service without relay prices exactly as before. Each line has an info tip with its formula. They share the capacity model's limits and object shape (`app/web/lib/usage/ingestShape.ts`). F is raw files a day, S segments per file, W window files a day (`window files per file × F`), w windows per shard (`window files per file ÷ worker requests per file`).

- **Split raw files** (S > 1, Task 25.1), per day:
  - Worker requests `F × (S − worker requests per file)`: one worker execution per segment instead of a share of one.
  - Reads `F × (S − 1)`: each segment reads the object through the front service, a front request and a Class B operation each. This assumes every read misses the front cache, an upper bound.
  - Re-downloaded raw bytes `F × (S − 1) × gzip size`, at the CDN egress rate ($0.12/GB list). Each segment downloads the whole compressed object (`mod.rs` `SEGMENT_READ_LIMIT`), so S segments move it S times; the first copy is already in the delivery line, so only S − 1 are added. At 100k req/s, 300 s, 88 B/line that is 760.32 GB/day × 7 = 5,322.24 GB/day, $19,160.06 a month.
  - Skip vCPU `F × 24 MiB × S(S − 1)/2 × 10 ms per MiB`, priced per extra segment execution with the contract CPU allowance and rounding. Segment i decompresses i × 24 MiB before its range (`convert.rs` `read_segment`), so a file's segments skip 0 + 1 + … + (S − 1) segments of text. The 10 ms/MiB (about 100 MiB/s inflate in Wasm) is an assumption, not a measurement.
  - Extra windows `F × (S × w − window files per file)`: each segment writes its own window files. Each extra window is a Parquet PUT and a summary PUT (2 Class A), a summary read (Class B and a front request) and a minimal 23 kB summary stored for the retention.
- **Multipart window uploads** (window over 8 MiB, Task 25.2): window size `= logged lines × Parquet bytes per row ÷ (W + extra windows)`, parts `P = ceil(size / 8 MiB)`. CreateMultipartUpload, P UploadPart and CompleteMultipartUpload replace one PUT, so each window adds `P + 1` Class A. Aborts after a failure aren't priced, like other retries. The worker falls back to a single PUT when its call budget is short; the estimate ignores that.
- **Relayed fan-out** (`ingest_relay` r ≥ 2, Task 25.2): per tick the pending shards `min(shards/s × T, 8 × r)` go out in `ceil(pending / r)` relay calls, when at least 2 are pending. Relay requests a day `= relay calls per tick × 86,400 / T`, with T the capacity model's tick time, each an extra Compute execution on the worker service at an assumed 5 vCPU ms (priced per execution against the contract CPU allowance, so under the default 20 ms per-request allowance only the Compute request fee applies). The relayed shards themselves are the same worker executions as direct dispatch, now on the main service through the peer backend, so they add no requests. Compute-to-Compute traffic has no billed line in this model. Ticks are assumed back to back; a slower clock packs more shards per relay, so this is an upper bound.

The prefill sets r from the install's `ingest_relay`, reported as `logging.ingest_relay` by `GET /api/admin/usage/logged-traffic`.

Monthly deltas at list prices and default units (109 nodes, 88 B/line):

| Load | Relay 0 | Relay 16 |
|---|---|---|
| 10k req/s, 60 s period | — | +$0.13 relay |
| 100k req/s, 60 s period | +$8.83 multipart (5 parts a window) | +$8.83 multipart, +$0.23 relay |
| 100k req/s, 300 s period | +$19,531 segments (8 a file; $19,160 re-downloaded raw, $308 skip vCPU) | same, +$0.27 relay |

Relay is cheap; splitting is dominated by downloading the whole raw file once per segment, then by the assumed skip vCPU, which Task 25.4 should measure. A shorter log period avoids splitting entirely.

---

# Historical

## Prototype calculator methodology (`cost/v4`)

This describes the archived prototype's `GET /api/costs` API, not the current architecture. It is kept for the evidence and dead ends it records.

The calculator keeps three things separate: billing usage, stats priced at list rates, and a traffic estimate. They are not interchangeable. In particular, **usage is not an invoice**.

**Storage duration:** the [Object Storage lifecycle guide](https://www.fastly.com/documentation/guides/platform/object-storage/bucket-lifecycle-management/) explicitly says, “Fastly Object Storage bills for a minimum storage duration of 30 days per object,” including earlier expiration. This primary source was checked on 2026-10-28. The [product summary](https://docs.fastly.com/products/object-storage) separately describes hourly rounding; its omission of the minimum does not overturn the explicit lifecycle guidance. The [pricing page](https://www.fastly.com/pricing) confirms the $0.020/GB-month entry tier.

## What the public API can show

`GET /api/costs` has no caller-selected service, bucket, date range or backend.
Its allowlist contains only the current VCL source/CDN, `flp` and `flp-worker`.
Storage LISTs are restricted to the runtime `raw_prefix` / `out_prefix` in the
two build-configured project buckets, currently `vcl/` and `logs/`.
Retained experiment services and other prefixes are excluded, not deleted.
Only safe bucket labels `raw` and `pq`, configured prefixes and observed
scope/provenance reach the browser, never bucket identities or signing material.

| Number | Provenance | Meaning |
|---|---|---|
| Service billing quantities | **Real billing usage** | Current and previous UTC month from `/billing/v3/service-usage-metrics`. A row must explicitly identify an allowlisted service, requested month and recognized product. Unknown or unattributable rows are omitted. |
| Actual charged USD | **Unavailable** | The usage APIs provide quantities, not service-attributed invoice charges. We do not request account invoices to manufacture a project total. Empty usage does not establish zero spend. |
| Requests, Compute requests, active CPU ms, bandwidth | **Service stats** | `/stats/service/{id}`, daily history plus a bounded hourly tail, clipped separately to both UTC months. Missing metrics remain missing. Current-month data is delayed and provisional. |
| Compute USD | **Derived from stats × list price** | Compute requests × $0.50/million + active CPU ms × $0.05/million. No allowances, discounts, rounding or contractual adjustments. This is not billed spend. |
| Object count and visible bytes | **Live storage observation** | Direct, signed ListObjectsV2. Not a historical storage invoice; deleted objects and incomplete uploads are invisible. |
| Visible-byte storage footprint | **Derived from LIST × list price** | Observed decimal GB × $0.020/GB-month. This is a hypothetical monthly price for the visible footprint, not actual storage charges. |
| Traffic scenario | **Estimate** | The separate model below, for a 30-day steady-state month. |

Account-level usage and invoice endpoints are not requested. Account-wide FOS
and KV usage cannot be attributed to these prefixes or stores; none of those
quantities, names or totals are returned. We do not apportion them by traffic
and call the result billing.

Only explicitly selected stats/products and sanitized issued-call provenance
leave the server. Provenance identifies actual API URLs/functions and requested
windows; storage uses safe bucket aliases/prefixes rather than signed URLs.
Upstream customer names, arbitrary fields, credentials, request headers and
error bodies never do. Errors use fixed safe JSON codes.

A current-service 404 produces explicit coverage warnings and missing metrics
rather than zero. Other available current services remain visible; retained
legacy services are never requested. Authentication failures, upstream outages,
malformed data or exhausted budgets fail the refresh safely.

## Bounded work and cache behavior

One refresh makes at most **27 backend requests**:

- At least three service-usage requests, each covering both months, with at most
  1,000 records per page. Validated continuation cursors are followed while
  reserving work for remaining services, stats and both storage listings.
  Budget exhaustion leaves explicit incomplete billing coverage, never totals.
- At most nine service-stats requests: three services × at most three windows
  across both months.
- At least one direct 1,000-key LIST page per runtime prefix, at most four each.
  With one billing page per service and nine stats calls, two complete first
  LIST pages use `3 + 9 + 2 = 14` calls; eight LIST pages use
  `3 + 9 + 8 = 20`. Additional billing pages share the same 27-call cap;
  these maxima are alternatives, not additive allowances.

There are no automatic network retries. Each upstream body is capped at 2 MiB. A 45-second admission deadline prevents starting more work after that point; an already-running request remains subject to the backend's explicit provisioned timeouts. This is not a hard 45-second cancellation guarantee.

LIST returns `complete`, pages read, observed objects and observed bytes. When the page budget is exhausted, values are **lower bounds**, never bucket totals. The listing is not an atomic snapshot while ingestion or deletion is running. No object keys, continuation tokens or bucket names are published.

## Historical stats windows

The original daily-only request shape did not see same-day usage. Calling this merely an upstream delay was wrong: [Fastly's documented daily buckets](https://www.fastly.com/documentation/reference/api/metrics-stats/historical-stats/) exclude the current UTC day entirely and normally publish around 02:00 the following day. One authorized hourly probe on 2026-10-28 returned actual `flp` usage: 811 Compute requests and 3,041.588 active CPU ms across four samples, despite daily-only snapshots remaining empty. This establishes usage statistics, not charged USD or nonempty billing.

The corrected collector captures its timestamp once and divides the requested two UTC months into disjoint half-open windows. Daily history ends at **the previous UTC midnight**. The hourly tail starts there and ends at `min(month_end, floor(snapshot / 3600) × 3600)`: the last completed UTC hour boundary, never the unfinished current hour. The tail is shorter than 48 hours. Each window is clipped to its month, including December/January and leap-day boundaries. This intentionally keeps yesterday in the hourly tail even before daily publication around 02:00. At most three windows span the two months. During the first hour of a new month, its eligible window is empty and its stats remain null, not invented zeros; the previous month's tail still ends exactly at the month boundary.

API queries use `from=start&to=end-1` because our internal end is exclusive. Returned `start_time` values are filtered to that half-open window; duplicate, misaligned or unfinished in-range buckets still fail safely. No timestamp is rounded or coerced. Empty history does not erase available hourly data, while a missing metric in any nonempty fragment keeps that month's metric null. A 404 for any fragment marks the month unavailable, never a partial month priced as complete. Public per-period `windows` metadata contains numeric `from`, `to_exclusive` and fixed `resolution` values. `eligible_to_exclusive` states the month's coverage cutoff; `incomplete_seconds_excluded` reports how much of the current unfinished hour is deliberately outside that cutoff, not measured publication latency. The existing calculator assumptions/data dialog receives a UTC cutoff and the approximately 15-minute publication-delay notice through `omissions`.

Timestamp rejection codes distinguish `stats_timestamp_missing`, `stats_timestamp_type`, `stats_timestamp_alignment`, `stats_timestamp_overflow`, `stats_timestamp_unfinished` and `stats_timestamp_duplicate`. These classifications do not relax acceptance: strings, fractional numbers, misalignment, overflow, unfinished buckets and duplicates remain rejected in the same order. Only failures emit `cost.stats.parse_failure`, with a fixed safe code, call ordinal, zero-based service index, requested month/granularity, captured snapshot and requested half-open bounds. No upstream values, bodies, IDs, URLs, headers or credentials are logged; unknown diagnostic-code text is replaced with `stats_parse_failure`. Successful parsing adds no diagnostic dump.

Official documentation says hourly buckets normally appear about 15 minutes after the hour ends and unfinished periods are not reported. **Actual source/CDN data contradicted the unfinished-period claim:** the v14 live failure context below proves a 09:00 hourly point was returned before its inferred 10:00 end. We therefore enforce completed-hour request bounds rather than relying on that promise or relaxing the strict parser. This is not real-time coverage: the excluded current hour is unknown/not included, never zero billed spend. Hourly retention is 375 days, enough for both requested months; minute retention is 35 days and would not cover every previous-month range.

The measured full-field hourly response was 14,402 bytes for four rows of 154–172 fields. At those observed sizes, 48 hourly rows project to about 183 KB; the existing **2 MiB per-response guard remains unchanged and fails closed if exceeded**, including schema growth. This is not a guarantee of upstream response size. Fetching an entire 744-hour month would instead project to 2.5–2.7 MiB before envelope overhead. No documented bulk field-selection query exists for this endpoint; the single-field endpoint would add requests and exceed our budget if naively used for all four metrics.

## Shared cache

A versioned KV snapshot shares sanitized results across executions and POPs for **600 seconds**, including safe upstream failures. The current-architecture correction uses the `cost/v4` namespace so prior six-service/bucket-wide results cannot mask it during rollout. A KV `Add`/generation-CAS lease fences concurrent refreshes; its explicit 120-second expiry permits recovery even if KV TTL deletion is delayed. There is no process-memory cache assumption. A Core Cache transaction collapses local concurrent fills with a two-second wait limit; it caches only sanitized responses for the snapshot's remaining lifetime. Busy/cache failures return a retryable safe error, never bypass caching to call the API.

The lifetime starts at refresh completion, not `generated_at`. A refresh already
in flight during retirement can finish afterward, so deletion plus ten minutes
alone does not guarantee a post-retirement snapshot. Verify that a successful
response's `generated_at` is later than the retirement timestamp; a cache miss
alone is insufficient.

The browser uses TanStack Query with a ten-minute stale interval and no automatic error retries or focus refetches. Its refresh button does not bypass the server cache. Responses carry cache headers and a cache outcome; aggregate telemetry records status, backend-call counts and elapsed time, not credentials or upstream bodies.

## Direct-parts traffic estimate

The pure typed model is `app/web/src/routes/calculator/model.ts`. It reuses the measured compression/CPU shapes and list prices from `experiments/archive/capacity-model/model.py` and the existing how-it-works calculator:

| Shape | gzip bytes/row | Parquet bytes/row | CPU ms/1,000 rows |
|---|---:|---:|---:|
| Small, 10 fields | 56 | 72 | 10.9 |
| Typical, 23 fields | 88 | 74 | 32.0 |
| Long values, 25 fields | 311 | 405 | 38.5 |
| Wide, 50 fields | 355 | 457 | 60.4 |
| Very wide, 100 fields | 724 | 902 | 125.1 |

These are measured ingestion inputs, not fresh measurements of the production schema. Synthetic values compress worse than real logs.

The logger period stays at **one minute**, matching this project's current configuration. Old cost tables instead choose a longer logging period when possible. The file-count assumption remains min(lines per period, 52); behavior at sustained high rates is unverified.

For each output window, the model chooses workers using raw-file count, the measured 30-second CPU planning target and a 32-request worker budget. It reserves two worker calls, assumes 16 MB upload parts, and refuses more than 24 workers or 30 coordinator calls. Passing these checks is a planning result, **not measured throughput or a production admission promise**. Multi-level fan-out is outside this estimator.

Workers upload parts of the final Parquet directly, as demonstrated in research §7.11. There is **no staged Parquet copy**, staged-storage charge, server-side copy charge or staged-footer GET. Equal-sized workers and 16 MB parts are assumed. Each worker's trailing part is padded to at least 5 MiB, even after earlier full parts, because the footer is the final part. Padding is retained and charged once in the final file. Actual grouping, schema and multipart splitting can change it. Footer bytes/control CPU are excluded.

Monthly requests are 30 × 86,400 × requests/second. Raw GB is requests × gzip bytes/row ÷ 1e9. Final Parquet GB includes the padding above. Raw storage is modeled for the documented 30-day minimum despite early deletion. Parquet storage uses max(retention days, 30) ÷ 30 × monthly generated GB. This is a **steady-state retention model**, not first-month accrued storage or a measurement of deleted bytes and actual charges. It does not attempt per-object hourly rounding.

Zero traffic creates no raw PUTs, workers or Parquet bytes. Sparse traffic has at most one active window per arriving row; no empty logger objects are invented. The measured tick floor remains 0.8 executions/minute: 34,560/month. Each tick assumes one raw LIST and one KV gate read. Per active window, three KV writes and two additional reads are a planning allowance. DeleteObjects is assumed billed per call, not per key; that is unverified.

Prices are FOS Class A $2.50/million, Class B $0.40/million, storage $0.020/GB-month; Compute $0.50/million requests and $0.05/million vCPU-ms; KV Class A $0.65/100k and Class B $0.55/million. Free tiers and contracts are excluded.

Also excluded: rollup copies, query/UI traffic, failure retries, incomplete-upload charges, CDN delivery/bandwidth, and changes in worker grouping caused by mixed services. Therefore the full traffic estimate should not match the observed **Compute-only** subtotal.

## References and limits of verification

- [Fastly billing usage API](https://www.fastly.com/documentation/reference/api/account/billing-usage-metrics/): consulted before API implementation.
- [Historical stats API](https://www.fastly.com/documentation/reference/api/metrics-stats/historical-stats/): active CPU time is `compute_execution_time_ms`, not wall time.
- [Live verification §6–§7.13](research/07-live-verification.md): ingestion shapes, direct parts and tick rate.
- [Recommendation](recommendation.md): its older tables still include staged storage; they are not the direct-parts estimator.

Live read-only schema checks found `data.details: []` for the allowlisted VCL service's two-month billing response. The account-usage response has `usage[]`; only its schema was inspected. No nonempty service billing fixture or service-attributed charged-USD response has been observed. The calculator does not imply otherwise.

Native privacy/budget tests, estimator tests, integrated Wasm compilation and fixture-backed browser checks have passed. Installed local Viceroy cannot instantiate the integrated SDK's `fastly_http_cache::transaction_choose_stale` import, but the deployed Fastly runtime accepts it; this is a local compatibility limitation, not a deployment blocker.

### Historical pre-current-scope snapshots

The measurements below preserve the earlier six-service/bucket-wide design for
diagnosis. They are not the current calculator's scope or totals. The deployed
v18 browser checks verify only the three current service IDs and actual
`vcl/` / `logs/` prefixes; no historical aggregate is substituted for them.

The lead's initial deployed `/api/costs` response on 2026-10-28 returned 200 with `X-Cost-Cache: miss`. Validation of the saved sanitized body confirmed all six then-allowlisted service IDs, both requested months, null missing-data semantics, CPU/list-price arithmetic, explicit omissions and no account totals. Refresh work was 23 backend calls in 5,338 ms, consistent with 19 billing/stats calls plus four LIST pages.

Both bucket listings completed: raw had 2,868 objects / 560,940,495 bytes across three pages; Parquet had 375 objects / 54,885,218,883 bytes in one page. These are visible project objects, including retained experiments, not historical billed storage.

October had priceable stats for four of six services, totaling **$0.0203885925 in derived Compute list-price costs for that covered subset only**. The newly provisioned `flp` services had no samples in that daily-only response; September had no samples for any project service. All six billing responses had empty published usage rows with no omitted detail rows or continuation. Consequently nonempty service billing remains unverified, and no actual charged USD is established. This body revealed no scope, arithmetic or LIST-completeness bug; the later hourly probe established the avoidable same-day query-shape issue described above.

The lead subsequently verified cold-follower behavior: five simultaneous post-expiry requests yielded one 200 miss and four structured `cost_refresh_busy` responses around two seconds, followed by a fast 200 hit reusing the fresh generation. This does not independently establish external-call counts or cross-POP deduplication. Repeated `generated_at` and identical bodies are consistent with snapshot reuse, but `X-Cost-Cache: miss` can also mean a new POP's Core Cache was filled from the existing KV snapshot; it does not prove an upstream API refresh. Check `cost.external` telemetry to distinguish those cases. No load or deployment was performed by W4. Parent subsequently deployed the daily/hourly correction as v13; its live failure is recorded below.

The first corrected v13 request returned a structured `503 stats_timestamp`, not a usable snapshot. One authorized direct source/CDN daily-history probe returned 200 with no rows. Offline replay of that empty daily shape and the previously captured four-row `flp` hourly shape succeeds; neither reproduces the failure. Diagnosis stopped rather than guessing a timestamp format or relaxing the unfinished-bucket guard. The diagnostics-only follow-up above made the subsequent authorized failure attributable without exposing payloads.

Parent deployed that observability patch as v14, exact SHA `c9fc5a3c0f71`. A single authorized request with a bounded live stdout tail returned a fresh 503/miss `stats_timestamp_unfinished`: call 4, service index 0, October hourly tail, `[2026-10-27 00:00:00, 2026-10-28 09:55:04)` UTC. The parser had already passed integer, alignment, overflow and in-window checks. Those bounds uniquely imply a 09:00 point whose inferred hourly end is 10:00, beyond the captured 09:55:04 snapshot. This conclusion comes from the classified branch and validated bounds, not an additional provider payload capture. The tail was stopped/reaped after 6.060 seconds; raw logs remain private.

Two focused regressions failed before the completed-hour request fix: the observed 09:55:04 snapshot must request an exclusive 09:00 end (`to=08:59:59`), and the first hour of a month must have no eligible current-month window. They now pass alongside strict malformed/unfinished rejection and boundary/dedup tests. The 27-call and 2 MiB guards are unchanged.

After T1 passed, the parent deployed the fix to both services as v15
`5fee35782bb2`. One authorized request returned 200/miss with a fresh
10:19:42 UTC snapshot: all six project services had current-month samples,
including nonnull usage for both new services. The cutoff was 10:00 UTC;
10:19:42 - 10:00:00 = **1,182 excluded seconds**, not zero usage.
The covered derived Compute subtotal was **$0.05364175825**, not billed spend.
Reported refresh work was **27 calls = 7 billing + 18 stats + 2 LIST** in
2,456 ms; these are snapshot telemetry, not independently captured upstream
call counts. Both one-page bucket listings were complete. Public-schema,
privacy, coverage and arithmetic checks passed. Private evidence is
`data/overnight/costs-w4-v15-20261028T101941Z.json`.
Project billing rowsets remain empty and actual charged USD remains null;
nonempty billing is still unvalidated.
