# Performance Load Test Plan

High-fidelity load and performance testing plan for validating the dashboard's read path under extreme production workloads (10K req/s and 1M req/s equivalent), across 1h/12h/1d/7d/30d windows, cold and warm cache, low/med/high cardinality, on local Parquet+Iceberg with a GCP final-validation tier.

---

## 0.2 Live Test Results from 2026-06-09 (with F1/F3 fixes + file:// warehouse)

After the F1, F3, and file://-warehouse fixes landed (see commit history), re-ran the scale test. Backend restarted clean. Iceberg catalogs re-init'd via the new file:// warehouse — `init_iceberg_table` now succeeds where it silently failed before (the bogus FOS endpoint no longer matters because file:// bypasses S3 entirely).

### Measured Performance — After Fixes

| Test | Before (2026-06-08) | After (2026-06-09) | Change |
|---|---|---|---|
| **F1: cold query, never-committed service** | 14.5 s (S3 manifest timeout) | **402 ms** (FAST PATH) | **36× faster** |
| Cold query, Iceberg-committed 36M rows / 1h | n/a (couldn't commit) | 2.4 s (one-shot, includes view rebind) | n/a |
| Cache-bust p50, 36M rows / 1h (buffer-only) | 2.9 s | n/a (now testing Iceberg path) | — |
| **Cache-bust p50, 36M rows / 1h (Iceberg-committed)** | n/a | **1.88 s** | — |
| Cache-bust p95, 36M rows / 1h | 4.4 s | **2.77 s** | **1.6× faster** |
| **F3: 20 concurrent queries — wedge?** | YES (3+ min wedge, HTTP 000) | **NO** (12 × 200 + 8 × 503 within 10s, backend stays responsive) | **fixed** |
| Backend RSS under 20-VU burst | 9 MB (wedged, no work) | 4.4 GB (8 DuckDB conns × ~550 MB each) | new — needs investigation |
| 6h window query (raw scan, no rollups) | n/a | **1.46 s** | — |

### What Changed (Code)

1. **F1 — `backend/core/iceberg.py:_update_iceberg_view_locked`**: added `elif metadata_loc is None` short-circuit. When the local SQLite catalog has no metadata_location for the table, skip the S3 catalog-load+plan_files round-trip entirely. The view is then either built from buffer files (if any) or falls through to the existing "all empty" WHERE-false branch.
2. **F3 — `backend/core/duckdb_pool.py:_Pool.acquire`**: moved `_prepare_checkout` (which calls `update_iceberg_view`) OUT of the `with self._cond:` block. A 14 s view-rebuild no longer holds the threading lock that blocks all other waiters; the documented `max_wait=10s` → 503 fallback now actually fires.
3. **file:// warehouse — `backend/core/iceberg.py`**:
   - Added `_is_local_only_source(source)` helper: returns True when `fos_local_warehouse: true` is in the config, OR when `fos_endpoint == "http://localhost:0"` (the conventional scrub marker from `dev-sandbox-scrub` memory).
   - `_warehouse_uri` returns `file:///cache/{bucket}/iceberg/` for local-only sources.
   - `_get_catalog` skips S3 props when local-only (PyArrowFileIO handles file:// natively).
   - `_read_metadata_pointer` / `_write_metadata_pointer` are no-ops for local-only (the local SQLite catalog already tracks `metadata_location`).
   - `_update_iceberg_view_locked` correctly extracts local paths from `file://` URIs and points the view's `data_dir` at `iceberg/<namespace>/<table>/data/` instead of the FOS-convention `cache/{bucket}/data/`.

### What's Validated End-to-End

- ✅ `init_iceberg_table` against local-only source: writes metadata.json to `cache/{bucket}/iceberg/default/logs/metadata/`, registers metadata_location in SQLite catalog.
- ✅ `commit_buffer` against local-only source: 27 buffer files (37M rows) committed in 132 s, 160 data files landed in `cache/{bucket}/iceberg/default/logs/data/timestamp_hour=*/`.
- ✅ Dashboard read against Iceberg-committed data via file://: returns full row count, 6 debug queries visible (F1 short-circuit didn't break the normal flow), 1.46 s for 6h window / 37M rows.
- ✅ Pool exhaustion fallback (F3): documented 10s wait→503 behavior fires under 20-VU concurrent burst.
- ✅ Backend stays responsive after burst (no wedge).

### New Issues Surfaced

**F6 — `scripts/backfill_rollups.py` rejects services whose display name contains spaces**: `backend/core/rollups.py:_safe_table_for` reads `source.get("name")` and validates it as a SQL identifier. But `name` is the human-readable display name (e.g. `"Load Test 10K RPS"`); the slug is in `service_id`. Result: backfill_rollups silently no-ops on any service whose display name has spaces. Should read `service_id` instead. **Action**: rename the field read to `service_id` in `rollups.py:_safe_table_for`, OR for our dummy configs, set `name == service_id` to work around.

**F7 — Backend RSS climbs to 4.4 GB under 8-way concurrent load against 36M rows**: way over the < 1.5 GB target. 8 DuckDB connections × ~550 MB each. Probably because each connection materializes a large TEMP TABLE for the live-hour path. Worth investigating per-connection memory bound (DuckDB has `memory_limit` PRAGMA which we should set to enforce a ceiling).

**F8 — Successful queries under concurrency contention go from 1.9 s → 16-21 s**. 10× slowdown when 8 connections share resources. This is just normal queueing but worth quantifying for the user-facing perceptual budget. With pool=8 and 20 concurrent users, p95 was 21.6 s. Tuning `DUCKDB_POOL_MAX_SIZE` higher trades higher RSS for lower per-query latency under contention.

### What's NOT Tested Yet (Carry to Day-3)

- 7d / 30d windows (rollup path) — F6 needs fixing first OR rollups need to be hand-synthesized.
- Sustained concurrency at p95 < target (need pool tuning, query optimization, or both).
- Cross-service contention (multiple services queried simultaneously).
- Memory bound under sustained load.

### Carry-over Recommendations

1. **Investigate the 1.9 s baseline for 1h / 36M rows.** 36M / 1.9 s = 19 M rows/sec scan rate. Reading 27 × 32 MB Parquet files from disk + group-by aggregations should be faster on local NVMe. Worth profiling the dominant time in `_debug_queries`.
2. **Set DuckDB `memory_limit` PRAGMA on every pool connection** to prevent the per-conn 550 MB blowup under concurrency.
3. **Fix F6 (backfill_rollups)** before any rollup-path testing.
4. **Pool tuning experiment** (`DUCKDB_POOL_MAX_SIZE` = 16, 32, 64) to find the latency-vs-RSS sweet spot.

---

## 0.5 F9 landed: time-bucketed rollups for the dashboard chart (2026-06-09 evening)

Implemented and committed in `b771d78`: per-minute time-series bundles
(`cache/{bucket}/rollups/hour_bundled/hour=H/time_series.parquet`) with
SUM-aggregatable schema, written alongside `all_fields.parquet`. Reader
in `QueryRunner.try_time_series_from_rollup` short-circuits the dashboard
chart for the four sum/rate metrics (requests, 5xx, 4xx, hit_rate) when
no filters are active.

### Measured — 12h / 1 M rows-per-hour / cache-busted serial / `chart_metric=requests / interval=1 hour`

| Path | wall p50 | wall p95 | `time_series` section | `live_temp_create` |
|---|---|---|---|---|
| Rollup-served (unfiltered) | **500 ms** | 550 ms | **2 ms** | 385–423 ms |
| Raw-served (filter forces fallback) | 23.5 s | 30.2 s | 96–125 ms | 13.5–15.8 s |

**`time_series` cost: 235 ms (§0.3 baseline) → 2 ms (~120× faster).**
Full-request wall is ~3× faster on the unfiltered window, ~50× faster
vs. the same query with a filter that forces wide-temp materialization.
The remaining cost on the unfiltered path is `live_temp_create` — a
narrow temp table for the OTHER aggregations (waf_sig, conn_requests,
top-N field tabs) that still need raw rows. Those are out of F9's scope.

### Correctness check

Same 6 h window queried unfiltered (rollup path) and with a no-op
exclude filter (raw fallback). 5xx rates per hour match to two decimals:

```
2026-06-08T12 → 2.50 vs 2.50    2026-06-08T13 → 2.50 vs 2.50
2026-06-08T14 → 2.52 vs 2.52    2026-06-08T15 → 2.49 vs 2.49
2026-06-08T16 → 2.50 vs 2.50    2026-06-08T17 → 2.48 vs 2.48
```

### Eligibility matrix (verified by `_section_timings` inspection)

| Query | Path taken |
|---|---|
| 1 h / requests / 1 min | rollup (1.2 ms) |
| 6 h / requests / 15 min → 1 min | rollup (2.6 ms) |
| 12 h / requests / 1 h | rollup (3.2 ms) |
| 7 d / requests / 1 h, sparse coverage | raw fallback (gap in rollup files for hours with no data) |
| 12 h / 5xx, 4xx, hit_rate / 1 h | rollup (1.8–3.0 ms each) |
| 12 h / p95_latency, throughput | raw (metric not rollup-supported) |
| 12 h / requests / 1 second | raw (interval not rollup-supported) |
| 12 h filtered / requests | raw (any filter forces raw) |

### Bundle storage

3 KB per service-hour at 1 M rows/hour after ZSTD. 30 days × 1 service
≈ 2 MB — negligible vs. `all_fields.parquet` (~280 KB/hour).

### Carry-over (filed in §15)

- [ ] **Active-hour merge edge case**: when the window crosses a closed
  hour that's missing its rollup (e.g. hours with zero data), the reader
  falls back to a full raw scan rather than serving the covered hours
  from rollup. v1 chose the conservative path to avoid undercounts; a
  follow-up could distinguish "no data" (write a zero-row bundle) from
  "rollup not built yet" so partial coverage still wins.
- [ ] **Percentile metrics still go raw.** p50/p95/p99 latency,
  throughput, req_size, and ttfb-median — adding t-digest sketches to
  the bundle would cover them. Likely separate PR, ~3-5× this PR's
  effort.
- [ ] **`live_temp_create` is the next bottleneck** on the unfiltered
  path now that time_series is fast. Worth profiling which
  aggregations are still served from it and whether they could move to
  bundled-field rollups too.

---

## 0.4 main vs performance-improvement comparison (2026-06-09 late afternoon)

Ran the same probes against a real S3-backed service on this host (real S3-backed service with 241 MB of cached data — works on both branches) after a clean backend restart on each branch. Window: 1h, ~11.9 K rows.

| Probe | main (`9448897`) | performance-improvement (`d281cd5`) | Δ |
|---|---|---|---|
| Cold query (1 shot) | 1171 ms | 1100 ms | -6% |
| Serial cache-bust, p50 (n=5) | 400 ms | 229 ms | **-43%** |
| Serial cache-bust, p95 | 488 ms | 377 ms | -23% |
| 10-VU concurrent burst, all-200 count | 10 / 10 | 10 / 10 | tie |
| 10-VU burst, p50 | 1427 ms | 1559 ms | +9% (noise) |
| 10-VU burst, p95 | 1505 ms | 1613 ms | +7% (noise) |
| Backend RSS post-burst | 1076 MB | 1242 MB | +15% |

### What this tells us

- **Serial cache-bust latency dropped ~43% on perf-improvement** (229 ms vs 400 ms). The most likely contributor is the `_view_cache` fast-path and pool-checkout optimizations that landed earlier on the branch.
- **Concurrent burst latency is statistically the same** at this scale (10 VUs × 11.9 K rows is too small to stress the pool). Both branches finished all 10 requests cleanly.
- **RSS comparable** — neither runs the test with `DUCKDB_POOL_CONN_MEMORY_LIMIT` set; perf-improvement's slight bump is from extra code loaded.

### What this DOESN'T tell us — and why a richer comparison was infeasible

The dramatic perf-improvement wins documented in §0.2 and §0.3 (F1 cold-cache 14.5 s → 402 ms, F3 wedge → clean 503s, file:// commit path) all rely on infrastructure that **does not exist on `main`** (`_is_local_only_source`, the file:// warehouse, the F3 pool-lock release, F7 memory cap). Specifically:

- The `dummy-10k-rps` test service (with `fos_endpoint="http://localhost:0"`) and its 36M-row dataset only function on perf-improvement. Trying to query it on main would hit S3 with the bogus endpoint and either time out or fail — not a meaningful "main is slower" datapoint.
- The 6h filtered-query OOM that surfaced F10 only happened because the F7 cap was applied; main has no cap and would not OOM there.

So the in-scope comparison is limited to a real, S3-backed service that's locally cached on both branches, over modest data volume — which yields a less dramatic but still real ~40% improvement on cache-busted serial reads.

For a stronger comparison we'd need:
1. A service that exists and has the same data on both branches (the locally-cached real service ✓ — used).
2. A dataset large enough to surface scaling wins (11.9 K rows in 1h is too small — would need to ingest more real-or-synthetic data through the proper Iceberg-commit path that works on `main`).

### Conclusion

The §7 first-pass pass/fail targets (p95 < 500 ms for 1h dashboard at 10K-RPS data) are met on perf-improvement at the small (~12 K-row) scale (377 ms p95). At the 36M-row dummy-service scale measured in §0.3, p95 was 2.77 s — still over target. The §0.3 architectural finding (F9 — rollups don't help time_series) is the actual blocker for hitting target at scale, not anything main vs perf-improvement.

---

## 0.3 Live Test Results from 2026-06-09 afternoon (multi-hour + profile)

After committing F6 (rollups slug), F7 (`DUCKDB_POOL_CONN_MEMORY_LIMIT`), and F8 (`DUCKDB_POOL_CONN_THREADS`), and after pushing all four commits, ran the rollup-path validation tests across multiple windows.

### Phase A — Profiled the 1.05 s baseline (36M rows / 1h / cache-bust)

Single-query section breakdown:

| Section | Time | % |
|---|---|---|
| `live_temp_create` (CREATE TEMP TABLE 12 cols × 36M rows) | 769 ms | **73%** |
| `time_series` (per-minute COUNT bucket aggregation) | 235 ms | **22%** |
| `top_n_rollups:dir_enum:n_hour_files` | 66 ms | 6% |
| `top_n_rollups` (Top-K field aggregations) | 12 ms | 1% |
| everything else (16 sections) | < 5 ms | < 1% |

**Scan rate ~47 M rows/sec** (8 threads). Nothing surprising — the temp table is doing real work and the time_series is bounded by row count.

### Phase B — Generated 12 hours × 1M rows + ran backfill_rollups (F6 validation)

- Generated 12h of data with the committed generator + commit_buffer.
- Stopped backend (DuckDB file lock), ran `python scripts/backfill_rollups.py dummy-10k-rps`. **F6 fix confirmed: 924 per-field parquet files produced.**
- Ran `backend.core.rollups.backfill_hour_bundles` separately to produce 14 × `all_fields.parquet` files (3.9 MB total).
- Restarted backend with `DUCKDB_POOL_CONN_MEMORY_LIMIT=1GB`.

### Phase C — Window-size matrix (12 partitions × 1M rows = 12M total)

**Unfiltered queries** (which path was taken):

| Window | wall | total_rows | path | dominant cost |
|---|---|---|---|---|
| 1h | 223 ms | 1M | **TEMP-HOUR** | live_temp_create=120ms |
| 6h | 846 ms | 6M | **TEMP-HOUR** | live_temp_create=722ms |
| 12h | 1.61 s | 12M | **TEMP-HOUR** | live_temp_create=1423ms |
| 7d (sparse) | 1.55 s | 12M | **TEMP-HOUR** | live_temp_create=1347ms |
| 30d (sparse) | 1.72 s | 12M | **TEMP-HOUR** | live_temp_create=1494ms |

**Filtered queries** (`country=US`, ~46% of rows):

| Window | wall | total_rows | path | dominant cost |
|---|---|---|---|---|
| 1h | 1.26 s | 460K | wide-temp + top_n_batch | wide_temp_create=1075ms |
| 6h | 1.92 s | 2.76M | wide-temp + top_n_batch | wide_temp_create=1369ms |
| 12h | 3.71 s | 5.52M | wide-temp + top_n_batch | wide_temp_create=2631ms |

### F9 — **Bundled rollups don't actually accelerate the dominant cost** ⚠

Every unfiltered query — even 30d windows — went through `live_temp_create` (the CREATE TEMP TABLE materialization of raw rows) rather than the bundled `all_fields.parquet` read path. Inspecting `_section_timings`:

- `top_n_rollups` IS being used → only for the Top-K field aggregations (already cheap, ~10 ms).
- `time_series` (per-minute COUNT aggregation) and the bulk temp-table materialization read RAW rows every time.

This means the rollup path only saves work for the field-aggregation portion of the response, not the dominant `live_temp_create` cost. **The system will not scale to a real 30 day × 10K-RPS query** (= 25.9 B rows → estimated ~9 minutes for the temp-table materialization alone with current per-thread throughput, and would OOM well before completing).

**Root cause**: the bundled `all_fields.parquet` format stores Top-K(field, value, count) per hour — not time-series buckets. The time_series aggregation can't be served from this rollup shape; it needs either (a) a separate time-bucketed rollup (per-minute COUNT, p50/p95/p99 latency) or (b) the dashboard to skip time_series for windows beyond a threshold.

**Action**: extend the rollup builder to produce a time-bucketed rollup alongside the field-Top-K, and teach `time_series` to read from it for windows where the per-minute granularity matches the rollup interval. This is the only way to make 7d/30d unfiltered queries scale.

### F10 — `DUCKDB_POOL_CONN_MEMORY_LIMIT=256MB` too low under any real load

Filtered queries against 6M+ rows OOM with the 256 MB cap (DuckDB's `failed to pin block of size 256.0 KiB (244.0 MiB/244.1 MiB used)`). 1 GB is the practical floor for the current workload; that puts the 8-conn-pool RSS ceiling at ~2 GB (still over the §7 1.5 GB target but workable). Best-of-both: bump cap to `1GB` AND set DuckDB `preserve_insertion_order=false` (the error message recommends it) — would let the cap stay lower. Worth a follow-up commit.

### Concurrent-burst comparison (1 GB cap)

| Burst | Dataset | http codes | p50 | p95 | post-burst RSS |
|---|---|---|---|---|---|
| 20-VU concurrent | 1M rows / 1h hour | 20 × 200 | 1.10 s | 1.50 s | 2.0 GB |
| 20-VU concurrent | 36M rows / 1h hour | 8 × 200, 12 × 503 | 20.9 s | 24.6 s | 2.0 GB |

The 36M-row burst still saturates because 8 conns × ~3s/query × queue depth = >10s wait → 503 fires cleanly. The 1M-row case has 12× more throughput because each query is bounded.

### F11 — Pool size 8 is the new bottleneck under burst (now that the wedge is fixed)

503s under 36M-row burst aren't a bug; they're working as designed. But they're the visible symptom of the pool-tuning trade-off. For interactive dashboard use (small windows, low concurrency) pool=8 is fine. For burst-test or "10 analysts hit the dashboard at the same time on a big window" pool=8 → most see 503. The §8.13 main-branch comparison is the next step to put a number on what's normal.

### Carry-over

- [ ] **F9 — extend rollup builder to produce time-bucketed rollups** (per-minute COUNT + p-quantile latencies). Without this, multi-day unfiltered queries always linear-scan raw and won't scale past ~hour scale at 10K-RPS.
- [ ] **F10 — make `preserve_insertion_order` configurable on read connections** (env var, default false for pool connections). Would let the memory cap stay tighter.
- [ ] **F11 — document recommended `DUCKDB_POOL_MAX_SIZE` per use case** in §13 after the main-branch comparison gives baseline numbers.
- [ ] **F6 follow-up — call `backfill_hour_bundles` automatically after `backfill_rollups`** (currently two separate steps; users will forget the second one).

---

## 0.1 Live Test Results from 2026-06-08 evening

A first scale test was executed using a minimum-viable generator (`/tmp/loadtest_generator.py` — uncommitted) writing synthetic Parquet directly to `cache/dummy-10k-rps-logs/buffer/` (no Iceberg commit, no S3). All queries against the running local backend at `127.0.0.1:18002` with `x-fastly-service-id: dummy-10k-rps` header. Cache-bust via ±30s `end_time` jitter (defeats the 30s `BoundedTTLCache`).

### Measured Performance

| Dataset (1h window) | Rows | Disk | Cold (s) | Warm cache-bust p50 | Max |
|---|---|---|---|---|---|
| Smoke | 100K | 2.3 MB | n/a | n/a | n/a |
| Small | 1M | 21 MB | 7.5 s | ~50ms (TTL cache, not real) | — |
| Medium | 10M | 205 MB | 11.1 s | 445 ms | 548 ms |
| **Primary (10K-RPS × 1h)** | **36M** | **733 MB** | **14.5 s** | **2.9 s** | **4.4 s** |

**Generator throughput**: stable ~270K rows/sec (compute-bound, NumPy + pyarrow vectorized). 36M rows in 93s. Per-row on-disk: ~20 B/row (less than the plan's 80 B/row estimate — the Zipfian categorical data compresses very well; real Fastly logs with higher-entropy URLs and headers would be larger).

**Generator heap**: ~1 GB RSS peak (NOT the < 200 MB plan target — the backend module imports + 500K-row Arrow batches were the cost). Fixing this is a day-2 generator polish item.

**Backend RSS during all tests**: stayed at 9–18 MB. DuckDB's memory management is aggressive — no working-set growth observed up to 36M rows.

### Critical Findings

**F1 — Cold-cache S3-manifest timeout dominates first query of every service (6–14 s wall-clock):** the first query against `dummy-10k-rps` triggered `DuckDB Iceberg View Resolution [SLOW PATH (S3 Read / Manifest Resolve)]` which attempts to read metadata from the bogus `fos_endpoint=http://localhost:0` and hangs until S3 client timeout. Subsequent queries hit the FAST PATH (local cache) at sub-100 ms. **Action**: the iceberg view-builder should detect a never-committed table (no `metadata.json` ever fetched) and skip the manifest-resolve attempt entirely, OR FOS unreachability should fall through to local-buffer-only views immediately. Today it costs every cold query for a local-only test service.

**F2 — Active-hour buffer-scan latency scales linearly with file count and is ~6× over the p95 target:** at 25 buffer files / 733 MB / 36M rows, the active-hour TEMP TABLE read (`backend/repositories/_base.py:480` `read_parquet(buffer_glob, union_by_name=true)`) takes p50 2.9s / max 4.4s cache-busted. The plan target is p95 < 500 ms. The dashboard rebuilds the TEMP table on every cache-busted query — 25-file ZSTD-decompress + UNION ALL + filter-prune is the dominant cost. **Action**: characterize whether row-group statistics are actually being used for the timestamp filter (the buffer files ARE sorted by timestamp per `write_to_buffer` semantics that I matched in the generator). May be a §13.B opportunity — but row-group pruning won't help much when 100% of the rows match the 1h timestamp range.

**F3 — Backend wedges on rapid-fire mixed-endpoint queries; the documented 10s pool-timeout 503 fallback never fired:** after firing ~10 queries in sequence across `/api/dashboard/aggregates`, `/api/dashboard/raw`, `/api/security/aggregates`, `/api/network-health`, `/api/origin/*`, `/api/performance/aggregates`, the backend stopped accepting new connections. 36 ESTABLISHED TCP connections leaked. New requests returned `HTTP 000` after waiting 60 s with the connection unable to complete. **3+ minutes after stopping all clients, the backend still wouldn't respond.** Backend RSS stayed at 9 MB throughout — this is not OOM, it's a pool/lock wedge. The `max_wait=10s` 503 fallback in `backend/core/duckdb_pool.py:140-161` did NOT trigger; queries hung indefinitely instead. **This is a real bug worth filing separately from the load test.**

  - Slowest single endpoints observed before the wedge: `/api/performance/aggregates` timed out at 60s, `/api/origin/slow-urls` 20s, `/api/security/aggregates` 9.8s. These would be the first to expose the wedge under any concurrency.

**F4 — Filter syntax in the original §6 examples was wrong:** the FiltersDict shape is `{"country": {"mode": "include", "values": ["US"]}}` (per `tests/test_smoke_end_to_end.py:181`), NOT `{"country": "US"}`. Plain string values produce a Pydantic `model_attributes_type` validation error. Fixed in §6 below.

**F5 — Response field is `total_rows`, not `total_requests`:** the dashboard aggregates response uses `total_rows` and `total_rows_total`. Earlier draft assumed `total_requests`. The k6 driver assertions need to use `total_rows`.

### What was NOT Tested (blocked by F3)

- Concurrency / `stress` scenario (50 → 200 VUs).
- 7d / 30d windows (would have exercised the bundled rollup path which has no synthetic data yet).
- Filtered-vs-unfiltered comparison at scale.
- Rollup-file-scale across 24/168/720 hour counts.
- Custom-fields casting overhead.

The wedge needs a backend restart and the day-2 work needs to land before any of these can run.

### Recommendations Coming Out of Live Test

1. **Wrap the iceberg view-builder with a "never-committed" short-circuit** (F1). Saves 6–14 s on every cold query for test/dev services.
2. **Investigate the connection-pool wedge** (F3). The `max_wait=10s` 503 fallback is supposed to be the safety net; it's not firing. Possible cause: `_PoolBusy` raising inside a request handler doesn't propagate to a 503; or queries are blocking somewhere outside the pool's `Condition.wait`. Worth filing as a separate bug ticket independent of the load test.
3. **Don't use the buffer path for the test's hot-path measurement** — it lacks partition pruning (UNION ALL over all files every time). The Iceberg-committed path with hour-partitioning is the actual production hot path. Day-2 work needs to figure out how to commit data without a real S3 backend (probably patch the catalog to use `file://` warehouse).
4. **The < 200 MB generator heap target is achievable with two changes**: defer the `from backend.core.iceberg import ...` until after argparse, and use 100K-row batches instead of 500K. Not blocking but worth fixing.

---

## 0. State as of 2026-06-08 (validated against live code + running services)

Plan claims have been validated against the codebase and the live local stack. State at start of day tomorrow:

**Done** (skip in §14):
- ✅ Local backend running at `127.0.0.1:18002` (HTTP 200 from `/api/sources` in ~15 ms).
- ✅ Local frontend running at `127.0.0.1:13002` (HTTP 200 in ~80 ms).
- ✅ `configs/dummy-10k-rps.json` and `configs/dummy-1m-rps.json` exist with `schema_version: 2`, all 12 groups (A–L), provisioning crons disabled, `cdn_url=""`, `fos_endpoint="http://localhost:0"`.
- ✅ Both services appear in `GET /api/sources`.
- ✅ Iceberg catalogs initialized: `cache/dummy-{10k,1m}-rps-logs/iceberg_catalog.db` (20 KB each).
- ✅ Cache directory structure created: `cache/dummy-{10k,1m}-rps-logs/{buffer,data,rollups/{day,hour,hour_bundled}}/`.
- ✅ `DEBUG_RESPONSES` appears enabled (backend returns `_debug_queries` from a plain aggregates POST).
- ✅ `local_rows: 0` — no synthetic data yet (clean slate).

**Not done** (day-1 work):
- ❌ `scripts/loadtest_generator.py` does not exist yet.
- ❌ `scratch/loadtest_k6/` directory and scripts do not exist yet.
- ❌ No baseline numbers captured against `main` or this branch.

**Leftover to investigate**: `configs/huge_load_test.json` exists from an earlier attempt and points at a real (non-local) FOS bucket. Decide tomorrow whether to delete or keep — it's NOT one of the two scrubbed local-only test services and could surprise the test driver if not isolated.

**Validated code claims** (everything else in the plan is referenced against existing files):
- `init_iceberg_table(source, create=True)` at `backend/core/iceberg.py:1267` — takes a single source dict, not a list.
- `commit_buffer(source, progress_callback=None)` at `backend/core/iceberg.py:1708`.
- `_FIELD_ORDER` at `backend/core/iceberg.py:546`.
- `HourTransform()` + `field_id=1000` partition spec at `backend/core/iceberg.py:1351-1355`.
- `BoundedTTLCache(maxsize=500, ttl_seconds=30)` at `backend/repositories/dashboard.py:37-40`. **Confirmed 30s TTL.**
- `DUCKDB_POOL_MAX_SIZE` env var, default 8, in `backend/core/duckdb_pool.py:66`. **Confirmed pool size + override.**
- `execute_top_n_rollups` at `backend/repositories/_base.py:522`, `execute_top_n_batch` at `:891`. **Confirmed rollup-vs-raw dichotomy.**
- `statement_timeout` set via `sql_validator.py:454-456` per connection (DuckDB 0.10+, ms units).
- `_section_timings` / `_debug_queries` / `_debug_calls` / `_is_cached` are real Pydantic `serialization_alias` fields at `backend/models/common.py:157-165`. **Confirmed response shape.**
- `starlette_compress.CompressMiddleware` at `backend/main.py:51`. **Confirmed Brotli/zstd/gzip middleware on this branch.**
- `tests/test_performance_smoke.py` exists.
- Loopback bypass in `backend/utils/remote_access.py:93-94`.
- `PRESETS["all"]` at `backend/core/log_fields.py:1252` is `["A"…"L"]`. (Existing dummy configs use `groups: [A..L]` directly with `preset: "standard"` — this is fine, the explicit `groups` array wins.)

**Code mismatches in plan that have been corrected below**:
1. `load_config()` takes a single `service_id` arg — not a zero-arg call returning `.sources`. Bootstrap snippet rewritten in §4.
2. `scripts/backfill_rollups.py` CLI is positional `service_id` only, not `--service ... --start ...`. Fixed in §5.B and §8.6.
3. ~~Endpoints take `source_name` in JSON body, NOT `x-fastly-service-id` header.~~ **Reverted after live testing 2026-06-08 evening**: the actual convention IS the `x-fastly-service-id` (or `x-service-id`) header, dispatched by `backend/utils/remote_access.py:522`. The body field `source_name` is silently ignored — request falls back to `get_active_service_id()` which returns the alphabetically-first service. This bit during live testing: a query with `"source_name": "dummy-10k-rps"` in the body and no header hit the wrong (alphabetically-first) service and returned 0 rows because that service genuinely has no data in the queried window. **Always send the header.**
4. `mock_data.py` is at `tests/utils/mock_data.py`, not `tests/mock_data.py`. Fixed in §5. Also the file does not contain a hardcoded ASN constant list — the seed pool will need to be defined inside the generator itself.
5. `chart_interval` is set in `frontend/app/dashboard/page.tsx:280,297` (and `frontend/app/charts/page.tsx:66`), not `frontend/components/ReportLayout.tsx`. Reference corrected in §6.

---

## 1. Goal & Scope

Validate that the dashboard **read path** remains usable when a single customer service represents log traffic at **10,000 req/s** or **1,000,000 req/s**, across windows **1h / 12h / 1d / 7d / 30d**, both **cold-cache** and **warm-cache**, across **low / medium / high** cardinality datasets. SUT is `127.0.0.1:18002` (backend); frontend at `127.0.0.1:13002`.

**Endpoints under test**:
- `/api/dashboard/aggregates`, `/api/dashboard/raw`, `/api/dashboard/field-values`
- `/api/security/aggregates`
- `/api/network-health`
- `/api/origin/timeseries`, `/api/origin/slow-urls`
- `/api/performance/aggregates`

Local first, then GCP final validation per §10.

**Explicitly NOT tested here**:
- Cloud FOS ingest throughput (synthetic data bypasses `read_json_auto()`).
- JSON-parse cost in `backend/core/ingest.py`.
- Cron scheduler under load (covered by `tests/test_scheduler_apscheduler_stress.py`).
- Auth / RemoteAccess middleware (127.0.0.1 bypass per `backend/utils/remote_access.py`).
- `/api/query` user-SQL endpoint.
- Admin / CRUD pages.
- Orphan-file cleanup.

---

## 2. Volume Math (80 B/row, ZSTD-3)

Real runs up to **864 M rows** (10K-RPS × 1d) on raw-scan paths. Mathematically scaled rollups beyond — no statistical extrapolation across orders of magnitude (DuckDB has spill cliffs and hash-resize discontinuities that break smooth fits).

| RPS | Window | Rows | Raw on-disk | Local SSD (1 TB) verdict |
|---|---|---|---|---|
| 10,000 | 1h | 36 M | 2.9 GB | yes |
| 10,000 | 12h | 432 M | 34.5 GB | yes |
| 10,000 | 1d | 864 M | 69.1 GB | yes — **primary raw dataset** |
| 10,000 | 7d | 6.05 B | 484 GB | borderline — rollups-only beyond 3d |
| 10,000 | 30d | 25.9 B | 2.07 TB | **NO** — rollups-only |
| 1,000,000 | 1h | 3.6 B | 288 GB | **NO at full** — keep 100 M-row active hour (8 GB) |
| 1,000,000 | 12h | 43.2 B | 3.46 TB | **NO** — rollups-only |
| 1,000,000 | 1d | 86.4 B | 6.91 TB | **NO** — rollups-only |
| 1,000,000 | 7d | 604.8 B | 48.4 TB | **NO** — rollups-only |
| 1,000,000 | 30d | 2.59 T | 207 TB | **NO** — rollups-only, projected only |

**Rollup-only budget**: Top-K=500 × 40 fields × 720 hours × ~80 B ≈ **~1.2 GB per service for full 30d**, independent of source RPS.

**Critical insight**: the dashboard's unfiltered fast-path reads bundled `all_fields.parquet` rollup files whose size is identical whether the underlying hour had 36 M or 3.6 B rows. Genuine 1M-RPS query stress comes from:
1. **Active-hour queries**: the live TEMP TABLE direct-scan of the current hour (keep up to 100 M rows, ~8 GB).
2. **Filtered queries**: dashboard filters bypass rollups and fall back to raw-scans (tested up to 864 M raw-row partitions).

---

## 3. Cardinality Profiles (Orthogonal Complexity Axis)

Zipfian distribution (skew = 1.1) over per-profile pools. Higher cardinality directly stresses DuckDB hash tables and group-by aggregations.

| Profile | distinct URLs | distinct IPs | distinct UAs | distinct JA3/JA4 | distinct ASNs |
|---|---|---|---|---|---|
| `low` | 100 | 1,000 | 50 | 20 | 10 |
| `med` (default) | 50,000 | 100,000 | 5,000 | 500 | 100 |
| `high` | 5,000,000 | 10,000,000 | 500,000 | 50,000 | 1,000 |

`low` and `high` get focused spot-checks at the 1d × 10K-RPS scale only.

---

## 4. Dummy Service Configs

Two local-only services. `backend/config.py:load_config` is mtime-based and `backend/scheduler.py:_sync_jobs` picks them up on next cycle — no backend restart.

### `configs/dummy-10k-rps.json`

```json
{
  "service_id": "dummy-10k-rps",
  "name": "Load Test 10K RPS",
  "access_level": "owner",
  "fos_bucket": "dummy-10k-rps-logs",
  "fos_endpoint": "http://localhost:0",
  "fos_region": "us-east-1",
  "fos_access_key_id": "dummy",
  "fos_secret_access_key": "dummy",
  "cdn_url": "",
  "log_fields": {
    "schema_version": 2,
    "preset": "standard",
    "groups": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"],
    "field_overrides": {},
    "custom_fields": []
  },
  "provisioning": {
    "cron_sync":    { "enabled": false, "interval_mins": 60, "log_enabled": false },
    "cron_compact": { "enabled": false, "interval_mins": 60, "log_enabled": false },
    "cron_ngwaf":   { "enabled": false, "interval_mins": 60, "log_enabled": false }
  }
}
```

### `configs/dummy-1m-rps.json`
Same shape, swap `service_id` / `name` / `fos_bucket` to `dummy-1m-rps` / `"Load Test 1M RPS"` / `dummy-1m-rps-logs`.

**Why all 12 groups (A–L)**: group `L` gates origin fields (`backend/provision/fastly_api.py:75`); without it `/api/origin/*` returns empty. All groups on so all code paths are exercised.

**Scrub compliance**: `cdn_url=""`, `fos_endpoint="http://localhost:0"`, all `provisioning.*` crons disabled (top-level `cron_*` are ignored; only `provisioning.*` is read by the scheduler).

### Cache directories (must exist before generator runs):

```
cache/dummy-10k-rps-logs/{buffer,data,rollups/hour,rollups/hour_bundled,rollups/day}/
cache/dummy-1m-rps-logs/{buffer,data,rollups/hour,rollups/hour_bundled,rollups/day}/
```

### Iceberg catalog bootstrap (already done as of 2026-06-08):

```bash
python -c "from backend.core.iceberg import init_iceberg_table; from backend.config import load_config; [init_iceberg_table(load_config(sid)) for sid in ('dummy-10k-rps', 'dummy-1m-rps')]"
```

`load_config(service_id)` takes a single service_id; it does NOT return a top-level config with a `.sources` attribute. If the catalog files (`cache/{bucket}/iceberg_catalog.db`) already exist (they do), `init_iceberg_table` is idempotent.

### How files reach the dashboard

This codebase has **no automatic glob-fallback** to `read_parquet('cache/.../data/**/*.parquet')` that bypasses the catalog. Verified at `backend/core/iceberg.py:9` and `backend/repositories/_base.py:480-483`:

- Files in `cache/{bucket}/buffer/` are read via `read_parquet(buffer_glob)` automatically — no catalog registration needed for buffer staging.
- Files in `cache/{bucket}/data/` MUST be registered via PyIceberg's `Table.append()` (Layer A handles this) OR staged in `buffer/` and committed via `commit_buffer()`.

If you only drop files into `data/` without catalog registration, the dashboard view is empty.

---

## 5. Synthetic Data Generator (`scripts/loadtest_generator.py`)

Three subcommands. Strictly bounded to **< 200 MB heap RAM** by streaming 1M-row Arrow blocks through `pyarrow.parquet.ParquetWriter` — a single PyArrow table holding 6 B rows would OOM.

### A. Subcommand: `generate-raw`

```bash
python scripts/loadtest_generator.py generate-raw \
  --service dummy-10k-rps \
  --start "2026-06-07T00:00:00Z" --hours 24 \
  --rps 10000 --cardinality med \
  --target-file-mb 128 --threads 8 \
  [--with-custom-fields]
```

- **Vectorization**: builds Arrow batches of 1M rows using Zipfian lookups over preallocated cardinality arrays.
- **Memory ceiling**: `< 200 MB` heap by streaming blocks through `ParquetWriter`. No full-dataset table in memory.
- **Output**: ~128 MB Parquet files matching `_FIELD_ORDER` in `backend/core/iceberg.py`, written into `cache/{bucket}/data/timestamp_hour=YYYY-MM-DD-HH/`, then registered via `Table.append()`.
- **`--with-custom-fields`**: appends 2 user-defined custom fields with mixed types to exercise dynamic-casting overhead in the query path.
- **Throughput target**: ≥1 M rows/sec (NumPy/Arrow vectorized; per `tests/test_performance_smoke.py` precedent). At 80 B/row that's ~80 MB/s — within NVMe envelope. 864 M rows (1d × 10K RPS) ≈ 15 min; 6 B rows (7d) ≈ 100 min.
- **Buffer-path validation**: for the first hour of `dummy-10k-rps`, write to `cache/{bucket}/buffer/` and run `commit_buffer('dummy-10k-rps')` manually to exercise the atomic commit path once.

**Per-row distributions** (full 80-field schema):

| Field | Distribution |
|---|---|
| `timestamp` | uniform across hour partition, ms granularity (TIMESTAMPTZ) |
| `status` | 90% in {200,204,304}, 5% redirects/client errors, 3% server errors, 2% NGWAF-blocked {406,429} |
| `ip` | 80% IPv4 / 20% IPv6, Zipfian over profile pool |
| `country` | weighted US 35% / DE 8% / GB 7% / JP 6% / BR 5% / 5×3% / 17% long tail across 50 codes |
| `city`/`region`/`lat`/`lon`/`metro` | correlated with `country` from fixed 200-city pool |
| `asn` | Zipfian over profile pool; seed list defined inside the generator (e.g. 7922 Comcast, 3320 DTAG, 15169 Google, 16509 AWS, 8075 Microsoft, 13335 Cloudflare, plus per-profile padding to reach the target distinct count). `tests/utils/mock_data.py` does NOT have a hardcoded ASN constant — don't try to import one. |
| `host` | 1–3 per service, 80/15/5 split |
| `url` | 70% Zipfian from profile pool, 20% `/api/...`, 10% long-tail random with query strings |
| `method` | GET 88% / POST 8% / HEAD 2% / OPTIONS+PUT+DELETE 2% |
| `proto` | HTTP/2 70% / HTTP/1.1 20% / HTTP/3 10% |
| `cache` | HIT 60% / MISS 25% / PASS 10% / ERROR 3% / HIT-CLUSTER 2% |
| `resp_bytes` | log-normal, median 8 KB, P99 5 MB |
| `req_bytes` | log-normal, median 1.2 KB, P99 50 KB |
| `elapsed`/`ttfb` | log-normal, median 25 ms, P95 250 ms, P99 1.2 s, 0.5% > 10 s |
| `tcp_rtt`/`rtt_min`/`rtt_var` | log-normal, correlated with `country` |
| `ploss`/`retrans` | mostly 0; 1% > 5 |
| `pop` | weighted across 50 POPs by country correlation |
| `backend`/`edge` | small fixed pools (5 backends, 50 edges) |
| `tls`/`ja3`/`ja4` | from cardinality profile pools (Zipfian) |
| `ua` | Chrome 60% / Safari 15% / Firefox 8% / bots 10% / 7% long tail |
| `waf*` | 95% null, 4% log-only, 1% blocked |
| `q_rtt*`/`q_lost`/`q_cwnd` | populated only for HTTP/3 rows |
| `ottfb`/`ottlb`/`ost`/`obytes`/`oip`/`oretries` | populated only when `cache IN ('MISS','PASS','ERROR')` |
| `_source_file` | `synthetic://{service}/{hour}/{batch}.parquet` |

### B. Subcommand: `generate-rollups`

```bash
python scripts/loadtest_generator.py generate-rollups \
  --service dummy-1m-rps \
  --mode [build | synth]
```

- **`--mode build`**: runs production `scripts/backfill_rollups.py <service_id>` (positional arg, no flags — verified) over synthetic raw Parquet files. Populates both `rollups/hour/field=*/hour=*/compacted_*.parquet` AND the bundled `rollups/hour_bundled/hour=H/all_fields.parquet`. Used for **rollup-build-path validation** on `dummy-10k-rps` 1d. The script reads the entire ingested range from the metadata DB — no `--start`/`--end` filtering.
- **`--mode synth`**: writes the **bundled** file directly at `cache/{bucket}/rollups/hour_bundled/hour=YYYY-MM-DD-HH/all_fields.parquet` with rows of `(field, value, count)` drawn from the same Zipfian as raw, **counts scaled to claimed RPS**. Total: 40 fields × 720 hours × ~5 KB ≈ **140 MB per service**. Also writes per-field files at `rollups/hour/field=*/hour=*/` for code paths that still expect them.

### C. Subcommand: `register-metadata`

For each Parquet file written to `buffer/` or `data/`, INSERT into `data/services/{service_id}.metadata.db` `ingested_files` (`source_file`, `row_count`, `file_size`, `file_date`). Skips dedup-on-LIST machinery, prevents recovery code confusion. For buffer-path files, also writes `.consumed-<ts>` tombstone sidecars.

---

## 6. Load Test Scenarios (`k6`)

**Tool justification**: existing `scratch/profile.js` is Playwright-based (cold-path single-user) — wrong shape for sustained concurrent load. k6 has native Brotli/zstd/gzip support (exercises `starlette-compress` on this branch), outputs p50/p95/p99 natively, and stateless JSON POST matches actual backend usage per `frontend/types/api.generated.ts`. Playwright layered on top only for full-page TTI as a separate test.

**Driver scripts**: new dir `scratch/loadtest_k6/{smoke,baseline,realistic,stress,cache_bust,mixed_filter,rollup_file_scale}.js`.

**Backend POST endpoints** (`http://127.0.0.1:18002`, JSON body). **Service is identified via the `x-fastly-service-id` HTTP header** (dispatched at `backend/utils/remote_access.py:522`). Omitting it falls back to the alphabetically-first service per `get_active_service_id()` — silent footgun that returns 0 rows for a wrong-service query. Every request must include `-H 'x-fastly-service-id: dummy-10k-rps'` (or `dummy-1m-rps`):

| Endpoint | Body skeleton | Page simulated |
|---|---|---|
| `/api/dashboard/aggregates` | `{start_time, end_time, filters:{}, chart_interval, chart_metric:"requests"}` | `/dashboard` time-series |
| `/api/dashboard/raw` | `{start_time, end_time, filters:{}, page:1, limit:50, sort:[...]}` | `/dashboard` raw logs table |
| `/api/dashboard/field-values` | `{start_time, end_time, field:"country", limit:100}` | dashboard filter pickers |
| `/api/security/aggregates` | `{start_time, end_time, filters:{}}` | `/security` |
| `/api/network-health` | `{start_time, end_time, filters:{}, metric:"health_score", bucket_seconds, top_n:30}` | `/network` |
| `/api/origin/timeseries` | `{start_time, end_time, filters:{}, percentile:"p95"}` | `/origin` |
| `/api/origin/slow-urls` | `{start_time, end_time, filters:{}, limit:50}` | `/origin` slow URLs |
| `/api/performance/aggregates` | `{start_time, end_time, filters:{}}` | `/performance` |

**Per-window params** (matching what the frontend sends — `chart_interval` at `frontend/app/dashboard/page.tsx:280,297`; `bucket_seconds` for `/network` is the dashboard's network-health page parameter):

| Window | `chart_interval` | `bucket_seconds` (network) |
|---|---|---|
| 1h | `1 minute` | 60 |
| 12h | `5 minutes` | 300 |
| 1d | `15 minutes` | 900 |
| 7d | `1 hour` | 3600 |
| 30d | `1 hour` | 3600 |

**Scenarios**:

| Name | VUs | Duration | Endpoint mix | Cache | Filters |
|---|---|---|---|---|---|
| `smoke` | 1 | 60 s | sequential 8 endpoints × 5 windows | warm | 0% |
| `baseline-cold` | 5 | 5 min | weighted (60% aggregates, 30% raw, 10% other) | **cold** (drop OS page cache + restart backend before each window) | 0% |
| `baseline-warm` | 5 | 5 min | same | warm | 0% |
| `realistic` | 50 ramp 60 s, hold 10 min | weighted, **70% unfiltered + 30% filtered (country=US or status=4xx)** | mixed | 30% |
| `cache-bust` | 50 | 10 min | weighted, each request shifts `end_time` ±1 random s | forced cold at TTL layer | 30% |
| `stress` | 200 ramp 2 min, hold 5 min | weighted | mixed | 30% |
| `active-hour-stress` | 50 | 5 min | `/api/dashboard/aggregates` window=1h only, against `dummy-1m-rps` active-hour | cache-bust | 0% |
| `rollup-file-scale` | 5 sequential | one shot per partition count | `/api/dashboard/aggregates` only, window varied to hit 24 / 168 / 720 bundled-hour files | cold | 0% |

The filtered mix in `realistic` is mandatory — unfiltered queries hit bundled rollups (`backend/repositories/_base.py:644-671`); filtered queries fall back to `execute_top_n_batch` against base table. A test without filters misses the raw-scan path entirely.

---

## 7. Metrics & Pass/Fail Criteria

### Metrics to Track

| Metric | Source |
|---|---|
| Request p50/p95/p99 latency | k6 `http_req_duration` |
| Backend per-phase timing | Response body `_section_timings` (requires `DEBUG_RESPONSES=1` at backend startup) |
| Per-query DuckDB time | Response body `_debug_queries[].time_ms` |
| FOS/S3 call count | Response body `_debug_calls` — must be **zero** for local test |
| SQLite metadata-DB op count + ms | `GET /api/debug/recent-sqlite` ring buffer (1000-entry limit — pull and reset between scenarios) |
| Cache hit ratio | Response body `_is_cached` — count true/false |
| Backend RSS / CPU | `ps -o rss,pcpu -p $(pgrep -f 'uvicorn backend.main')`, sampled every 5 s by sidecar |
| ASGI/Uvicorn CPU vs DuckDB CPU split | sample `ps` for uvicorn worker + DuckDB child threads separately; distinguishes Brotli/zstd/gzip compression CPU saturation from DuckDB engine CPU |
| Pool exhaustion | k6 503 count + backend logs grep for "pool timeout" (per `backend/core/duckdb_pool.py:max_wait=10s`) |
| Error rate | k6 `http_req_failed` |
| Compression ratio | k6 `data_received` with `Accept-Encoding: br,zstd,gzip` vs identity |
| DuckDB temp-dir spill bytes | `du -sb $(duckdb temp_dir)` sampled every 5 s; correlate with hash-resize cliffs |
| Parquet file enumeration time | `_debug_queries` entries containing `glob` or `iceberg_scan` — break out as separate metric |
| Disk read bytes/sec | `iostat -d 1` on data volume during each scenario |

### Pass/Fail Performance Targets (50 VUs, med cardinality, cache-bust unless noted)

| Dataset | Endpoint | Window | p95 target | p99 target |
|---|---|---|---|---|
| `dummy-10k-rps` | `/api/dashboard/aggregates` | 1h | **< 500 ms** | < 1 s |
| `dummy-10k-rps` | `/api/dashboard/aggregates` | 1d | **< 800 ms** | < 1.5 s |
| `dummy-10k-rps` | `/api/dashboard/aggregates` | 7d | **< 1.5 s** | < 3 s |
| `dummy-10k-rps` | `/api/dashboard/aggregates` | 30d | **< 2.5 s** | < 5 s |
| `dummy-10k-rps` | `/api/dashboard/raw` | any | **< 1 s** | < 2 s |
| `dummy-10k-rps` | `/api/security/aggregates` | 7d | **< 2 s** | < 4 s |
| `dummy-10k-rps` | `/api/network-health` | 1d | **< 1.5 s** | < 3 s |
| `dummy-1m-rps` (rollups) | `/api/dashboard/aggregates` | 7d | **< 2 s** | < 4 s |
| `dummy-1m-rps` (rollups) | `/api/dashboard/aggregates` | 30d | **< 3 s** | < 6 s |
| `dummy-1m-rps` active hour | `/api/dashboard/aggregates` | 1h (cache-bust) | **< 2 s** | < 4 s |
| any | any | any | **error rate < 0.1%** at 50 VUs, < 1% at 200 VUs |
| any | any | any | Backend RSS **< 1.5 GB** sustained |
| `low` vs `high` cardinality | `/api/dashboard/aggregates` | 1d | **high p95 ≤ 2× low p95** | — |
| Brotli vs identity | any | any | **identity p95 ≤ Brotli p95 + 50 ms** (otherwise compression is the bottleneck, not DuckDB) | — |

### Hard Failure Conditions (Stop the test)

- Backend RSS > 1.5 GB sustained > 30 s.
- Any single query > 30 s (would hit `backend/utils/sql_validator.py` `statement_timeout` — recurring kills = design bug).
- p99 for 1h dashboard query > 5 s at baseline (5 VUs) = regression vs `performance-improvement` branch goal.
- Backend process dies or hangs.

---

## 8. Sequencing & Execution Runbook

Strict order; each step gates the next. Steps marked ✅ are already done as of 2026-06-08 (see §0).

1. **Build the generator.** Implement `scripts/loadtest_generator.py` with the three subcommands and < 200 MB memory ceiling. No tests run until this exists.
2. ✅ **Bootstrap.** ~~Write both `configs/dummy-*.json`; verify `/api/sources`; create cache directories; initialize Iceberg tables.~~ **Already done.**
3. **Generator dry-run.** `python scripts/loadtest_generator.py generate-raw --service dummy-10k-rps --hours 1 --rps 10000 --cardinality med`. Then `curl -X POST .../api/dashboard/aggregates` (see §14.9 for full command) — inspect response shape. Stop and debug if numbers look wrong. Confirm heap stays under 200 MB via `/usr/bin/time -v`.
4. **Smoke** (k6, 1 VU, 60 s) against `dummy-10k-rps` 1h. Confirm telemetry parsing works.
5. **Grow to 12h.** Re-run smoke. Latency should be ~same as 1h (partition pruning).
6. **Grow to 1d** (69 GB). Run `python scripts/backfill_rollups.py dummy-10k-rps` (positional service_id, no flags) — this is the build-path validation and writes the bundled `all_fields.parquet`. Then run `baseline-cold` and `baseline-warm` across all 8 endpoints × 5 windows. **Capture as reference numbers** for everything that follows.
7. **Run `realistic` and `cache-bust`** against `dummy-10k-rps` 1d. Anything > 3× baseline at this concurrency = pool/contention issue.
8. **Run `stress` (200 VUs).** Expect 503s; verify clean failure mode, no RSS blowup.
9. **Run `rollup-file-scale`** with windows chosen to hit 24 / 168 / 720 bundled-hour files. Isolates manifest/enumeration overhead.
10. **Cardinality spot-check.** Regenerate `dummy-10k-rps` 1d at `--cardinality low` and `--cardinality high`. Re-run `realistic`. Compare against med-cardinality baseline.
11. **Custom-fields overhead.** Regenerate one hour with `--with-custom-fields`. Re-run smoke + targeted aggregates. Document dynamic-casting overhead.
12. **Switch to `dummy-1m-rps`.** Populate synthetic bundled rollups for all five windows + 100 M-row active hour. Re-run `baseline-cold`, `realistic`, and `active-hour-stress`.
13. **Compare against `main`.** Stash, checkout `main`, re-run steps 6–7. Quantifies `performance-improvement` branch delta.
14. **GCP validation (final).** See §10. Default to Tier 2 (GCS-backed) unless Tier 4 already covered the matrix.

---

## 9. Resource Feasibility & Fallback

| Item | Size | Local | GCP n2-standard-16 |
|---|---|---|---|
| Both services' rollups, all windows (30d) | ~280 MB | trivial | trivial |
| `dummy-10k-rps` raw 1d (864 M) | 69 GB | yes | yes |
| `dummy-10k-rps` raw 3d (2.6 B) | 207 GB | yes (if free) | yes |
| `dummy-10k-rps` raw 7d (6.05 B) | 484 GB | **fills SSD** — downsample to 3d | yes |
| `dummy-1m-rps` active hour (100 M) | 8 GB | yes | yes |
| Backend + DuckDB working set | ≤ 1.5 GB RSS local / 48 GB ceiling on GCP |
| `dummy-1m-rps` × 30d raw (2.59 T / 207 TB) | not stored anywhere | rollups-only | rollups-only or Tier 2 GCS |

**Fallback hierarchy** (apply in order if local can't hold):
1. Drop `dummy-10k-rps` raw from 7d to 3d (rollups still cover query windows).
2. Shrink `dummy-1m-rps` active hour from 100 M to 30 M rows.
3. Rollups-only for both services. Document that filtered-query realism is degraded.

The 1M-RPS × 30d × raw cell is not testable anywhere reasonable — it is rollups-only by design.

---

## 10. Testing at Real Scale (GCP)

Four tiers, ordered by cost and architectural realism. **Read §11 first** so the cost numbers below aren't misread as production storage cost.

### 10.1 Tier 1 — Single-VM huge-disk (TEST cost only, not production storage)

Brute-force: one VM with enough attached storage to hold 207 TB of raw rows.

- **VM**: `n2-highmem-128` (128 vCPU, 864 GB RAM, ~$5.40/hr in us-central1).
- **Storage**: 5 × Hyperdisk Extreme @ 64 TB RAID-0 = 320 TB usable, ~25 GB/s aggregate read. ~$0.125/GB-month = **~$40,000/month** for the disks alone, ~$1,300/day prorated.
- **Generation time**: 207 TB ÷ 5 GB/s ≈ **12 hours**.
- **Total cost for a 5-day test window** (stand up → generate → test → tear down): **~$7,500**.

Use this **only if a stakeholder demands seeing real numbers against 207 TB of physical raw rows**. One-shot validation cost, not recurring.

### 10.2 Tier 2 — Object-storage-backed (matches production architecture) — **recommended default**

What production actually looks like at 1M RPS. Put Parquet in a GCS bucket; DuckDB reads via httpfs (already wired into `backend/core/iceberg.py:update_iceberg_view`).

- **VM**: `n2-standard-16` (16 vCPU, 64 GB RAM, ~$0.78/hr, ~$19/day).
- **Storage**: GCS Standard bucket, single-region. **207 TB × $0.020/GB-month = ~$4,140/month**, prorated to ~$138/day. Same-region egress: free.
- **Total cost for a 5-day test window**: **~$800–1,000**.
- **GCS latency** (~5–10 ms per file vs local NVMe ~0.1 ms) is *more* realistic than local SSD — production with FOS exhibits the same floor.
- **Generation**: `scripts/loadtest_generator.py --output-gcs gs://loadtest-dummy-1m/...`. PyArrow writes Parquet directly to GCS.
- **Iceberg catalog**: leave on VM's local disk (SQLite). Metadata-only, < 100 MB even for 207 TB of data files.

### 10.3 Tier 3 — Mirror a real high-volume production service

Highest fidelity. If any customer's existing service is already 100K-RPS-plus:
- Snapshot the FOS bucket for that service into a test bucket (read-only copy).
- Capture real production query traffic (`/api/debug/recent-sqlite` ring + LB access logs).
- Replay captured traffic against the new-branch code reading the snapshot.

No synthetic generator. No cardinality model to defend. Setup cost: ~2–3 days of plumbing. Use once before any prod deploy that touches the hot query path.

### 10.4 Tier 4 — Skip 1M-RPS raw entirely (architecturally honest answer)

**The 1M-RPS-against-raw-rows case never actually exists as a single dashboard query in production.** The architecture in `_base.py:execute_top_n_rollups` already routes:

- **Unfiltered queries** → bundled `all_fields.parquet` rollup files (size independent of source RPS).
- **Active-hour queries** → live TEMP TABLE from current hour's raw rows (bounded by 1 hour, regardless of long-term RPS).
- **Filtered queries** → fall back to raw scan, with partition pruning to the requested window.

So the three real code paths a 1M-RPS service would hit are all testable without storing 207 TB:

1. Rollups path: rollups-only dataset (~280 MB total for both services × 30d).
2. Live active-hour path: 100 M-row active hour (~8 GB).
3. Filtered raw path: 10K-RPS × 7d (484 GB) — exercises file counts and partition pruning. Query cost at this scale is bounded by *what the partition pruner has to look at*, not long-term volume.

A scaled-up 1M-RPS filtered raw query that scans 7d of partitions touches the same number of partitions as the 10K-RPS test (168 hourly partitions), just with bigger files. The "bigger files" latency multiplier is measurable from existing tests — not a separate test scenario.

**Recommendation**: Tier 4 covers the actual production code paths. Tier 2 (~$1K per cycle) is the next-best validation if anyone wants to see real query latencies against full data volume. Tier 1 ($7.5K one-shot) for political-cover demos only. Tier 3 if a real high-volume customer exists.

### 10.5 GCP runbook (Tier 2, recommended)

1. **Provision**: `gcloud compute instances create loadtest-1m --machine-type=n2-standard-16 --image-family=debian-12 --boot-disk-size=200GB --zone=us-central1-a`
2. **Bucket**: `gsutil mb -c STANDARD -l us-central1 gs://loadtest-dummy-1m-$(date +%s)`
3. **Clone & install**: `git clone … && pip install -e .` plus DuckDB httpfs extension if not in lockfile.
4. **Env**: `GOOGLE_APPLICATION_CREDENTIALS`, `DUCKDB_THREADS=16`, `DUCKDB_MEMORY_LIMIT=48GB`, `DEBUG_RESPONSES=1`.
5. **Configure dummy services for GCS**: same `configs/dummy-1m-rps.json` but with `fos_bucket=loadtest-dummy-1m-…` and `fos_endpoint=https://storage.googleapis.com`. Keep scrub fields and `schema_version: 2` + groups A–L.
6. **Generate** with `--output-gcs`: start with 1h of `dummy-1m-rps` raw (3.6 B rows, ~288 GB) to validate the GCS write path. Then 100 M-row active hour. Then rollups-only for the rest of 30d.
7. **Run scenarios** unchanged from §6 — same k6 scripts, pointed at the VM's external IP.
8. **Compare against local-SSD `dummy-10k-rps` baselines**. Two key questions:
   - Does GCS-vs-SSD inflate p95 by an acceptable multiplier (target: < 3×)?
   - Do absolute p95 numbers at GCS-backed 1M-RPS still meet §7 pass/fail?
9. **Tear down immediately** — leaving the VM and bucket running is the only way this test gets expensive.

---

## 11. Production Storage Economics

The Tier 1 "$7,500" figure is for a **5-day validation test using Hyperdisk Extreme** (premium SSD-class block storage). **No one stores logs that way in production**, like no one stores library archives in hotel suites. For context, here's what 207 TB actually costs to keep on different tiers:

| Tier | $/GB-month | 207 TB / month | Realistic use |
|---|---|---|---|
| Hyperdisk Extreme | $0.125 | ~$40,000 | Active OLTP workload, never log archival |
| Standard PD | $0.040 | ~$8,000 | Hot scratch space |
| **GCS Standard** | **$0.020** | **~$4,140** | **Typical 30d-rolling log retention** |
| GCS Nearline | $0.010 | ~$2,070 | 30d+ infrequent access |
| GCS Coldline | $0.004 | ~$830 | 90d+ rare access |
| GCS Archive | $0.0012 | ~$250 | 365d+ compliance retention |

**Ongoing storage of 1M-RPS logs at 30d retention on GCS Standard is ~$4K/month**, not $7.5K. Add maybe $1–5K/month for ingestion + query compute. Total realistic production cost: **$5–10K/month**, with significant savings if older data tiers to Nearline/Coldline.

This codebase reads from **Fastly Object Storage (FOS)**, not GCS directly. FOS pricing is in the same ballpark. The end user of this dashboard typically doesn't pay storage at all; it's bundled into Fastly log-delivery pricing.

---

## 12. Risks & Unknowns

1. **Unfiltered-only queries are unrealistically rosy.** `_base.py:execute_top_n_rollups` reads bundled rollups for unfiltered; adding a filter falls back to `execute_top_n_batch` against base table. **Mitigation**: 30% filtered mix in `realistic` and `cache-bust`.
2. **30 s `BoundedTTLCache` in `backend/repositories/dashboard.py` will dominate.** Without cache-bust, 50 VUs serve from cache. **Mitigation**: `cache-bust` mandatory; warm scenario characterizes the cache-hit path.
3. **DuckDB pool default = 8; 50 VUs queue.** `backend/core/duckdb_pool.py:max_wait=10s` 503s after 10 s. Test with `DUCKDB_POOL_MAX_SIZE=16` and `=32` to find right default.
4. **Iceberg catalog SQLite write contention.** Read-only test mostly avoids, but per-connection `iceberg.py:update_iceberg_view` on every `get_connection()` reads catalog metadata — needs verification.
5. **Synthetic distributions may diverge from real Fastly logs.** Top-K cost is cardinality-sensitive. **Mitigation**: cardinality matrix as orthogonal axis; consider seeding Zipfian from a real-service rollup sample (frequencies only, no PII).
6. **6 B-row generation takes ~100 min and contends with backend.** Run generation with backend stopped; start backend for test phase only.
7. **No `main`-branch baseline yet.** All targets are first-pass guesses. Step 13 produces the real baseline; adjust targets after.
8. **Frontend TTI not measured by k6.** The 86-card lazy-mount cascade in `frontend/app/dashboard/page.tsx` is invisible to API timing. Run Playwright via `scratch/profile.js` (with `BASE_URL=http://127.0.0.1:13002`) as a separate test post-API-validation.
9. **`commit_buffer` cron path is disabled in our configs.** Intentional, but means we won't catch ingest regressions. Existing `tests/test_performance_smoke.py` covers; flag in results.
10. **`/api/debug/recent-sqlite` ring is 1000 entries.** Pull and reset between scenarios; or stream to disk via sidecar.
11. **DuckDB spill-to-disk cliffs and hash-resize spikes** between 10 M and 100 M temp-table rows. Captured via spill-bytes metric; expect non-linear latency jumps.
12. **Parquet metadata enumeration regime change** between 24 and 720 bundled-hour files. `rollup-file-scale` scenario measures explicitly.
13. **`starlette-compress` Brotli CPU may saturate the ASGI worker before DuckDB saturates.** Compression-CPU metric distinguishes; Brotli-vs-identity pass/fail row in §7 catches this.
14. **Custom fields trigger dynamic casting** which can be 2–10× slower than fixed-schema columns. `--with-custom-fields` + step 11 measures the overhead before it surprises a customer.

---

## 13. Codebase Scale & Architectural Hardening Insights

To prepare the codebase to handle 10K/1M RPS at real customer deployments, these are recommended changes — surfaced by the load test plan but worth tracking independently.

### A. Memory-Bounded Ingestion Catch-up Guard

- **Problem**: if a high-volume service loses network connection to FOS for 12 hours, a backlog of ~400 M rows accumulates. Upon reconnection, the ingestion cron downloads a massive batch of `.gz` files. Loading them all in one DuckDB transaction and calling `_fetched.to_arrow_table()` (verified at `backend/core/ingest.py:689` and `:847`) will exhaust RAM and crash the ASGI worker.
- **Remedy**: modify `backend/core/ingest.py` to check the cumulative compressed size of downloaded `.gz` files per chunk. If total compressed size exceeds **200 MB** (~25 M rows), split the chunk into smaller sub-batches and ingest sequentially.

### B. Sorted-Parquet Row-Group Statistics (Filtered Scan Acceleration)

- **Problem**: when a user executes a filtered dashboard query, DuckDB scans raw hourly partitions. If rows in the Parquet files are unordered, DuckDB cannot utilize row-group min/max metadata and must scan every row-group.
- **Remedy**: ensure both the synthetic generator AND the production writer sort raw logs by `timestamp` and sub-sort by high-cardinality/frequently-filtered dimensions (`country`, `status`) before writing to Parquet. Enables DuckDB to skip up to 90% of row-groups during filtered queries.

### C. Concurrency-Optimized Connection Pool Tuning

- **Problem**: the default connection pool size of 8 in `backend/core/duckdb_pool.py` causes queue timeouts (503 errors) during concurrent stress tests (50+ VUs).
- **Remedy**: configure runtime pool size to scale with workload. Under heavy query volumes, set `DUCKDB_POOL_MAX_SIZE=32` or `64` and tune DuckDB query thread limits (`SET threads = ...`) to prevent context-switching penalties.

### D. Physical Scratch Mount Configuration (Temp Disk Spill)

- **Problem**: during complex group-by queries over high-cardinality data, DuckDB spills intermediate tables to disk. If no temporary directory is configured, DuckDB spills to virtual systems (RAM-backed `/tmp` mounts in some cloud platforms), leading to silent OOM crashes.
- **Remedy**: enforce a production setting that explicitly binds DuckDB's `temp_directory` to a high-speed, physically mounted NVMe SSD scratch path.

---

## 14. Tomorrow's Runbook — Concrete Day-1 Steps

Steps 1–5 of the original "Concrete First Steps" are **already done** (see §0). What remains, in execution order:

### 14.1 Cleanup decision (5 min, blocks generator)

Decide what to do with the leftover `configs/huge_load_test.json` (and its corresponding `cache/<bucket>/` directory). It points at a real (non-local) FOS bucket and was created in an earlier attempt. Options:

- **Delete** if it was abandoned: `rm configs/huge_load_test.json && rm -rf cache/<bucket>/` (also confirm via `/api/sources` it disappears).
- **Keep but isolate** if it's a real service: add it to a "do-not-target" list in the k6 driver.

### 14.2 Verify the backend's `DEBUG_RESPONSES` mode (2 min)

```bash
curl -s -X POST http://127.0.0.1:18002/api/dashboard/aggregates \
  -H 'content-type: application/json' \
  -d '{"source_name":"dummy-10k-rps","start_time":"2026-06-08T00:00:00Z","end_time":"2026-06-08T01:00:00Z","filters":{},"chart_interval":"1 minute","chart_metric":"requests"}' \
  | jq 'keys'
```

Confirm the response includes `_debug_queries`, `_debug_calls`, `_is_cached`, `_section_timings`. If any are missing, restart the backend with `DEBUG_RESPONSES=1` in the env before continuing.

### 14.3 Build `scripts/loadtest_generator.py` (the day's main work)

Three subcommands per §5. Minimum viable v1:

- `generate-raw` first, with `--cardinality med` only (skip low/high spot-checks for v1).
- `< 200 MB` heap ceiling enforced via `pyarrow.parquet.ParquetWriter` with `write_batch(arrow_batch)` in 1M-row chunks.
- Output ~128 MB Parquet files under `cache/{bucket}/data/timestamp_hour=YYYY-MM-DD-HH/`, then call `pyiceberg.Table.append([data_files])` to register.
- Defer `generate-rollups` and `register-metadata` to day-2 once `generate-raw` works end-to-end.

Smoke target: generate 1 hour at 10K-RPS (36M rows, ~2.9 GB) in under 60 seconds, < 200 MB RSS.

### 14.4 Scaffold k6 driver dir

```bash
mkdir -p scratch/loadtest_k6
touch scratch/loadtest_k6/{smoke,baseline_cold,baseline_warm,realistic,stress,cache_bust,mixed_filter,rollup_file_scale}.js
```

Write `smoke.js` only on day-1. The others are templates filled in days 2–3.

### 14.5 Generator dry-run + sanity check (gates everything else)

```bash
/usr/bin/time -l python scripts/loadtest_generator.py generate-raw \
  --service dummy-10k-rps \
  --start "2026-06-08T00:00:00Z" --hours 1 \
  --rps 10000 --cardinality med \
  --target-file-mb 128 --threads 8
```

(Note: `/usr/bin/time -l` on macOS instead of `-v`.) Confirm `maximum resident set size` reports under ~200 MB. Then validate the data is queryable:

```bash
curl -s -X POST http://127.0.0.1:18002/api/dashboard/aggregates \
  -H 'content-type: application/json' \
  -d '{"source_name":"dummy-10k-rps","start_time":"2026-06-08T00:00:00Z","end_time":"2026-06-08T01:00:00Z","filters":{},"chart_interval":"1 minute","chart_metric":"requests"}' \
  | jq '.total_requests, ._section_timings, ._debug_queries | length'
```

Expect `total_requests ≈ 36000000`. If 0, the generator probably wrote files to `data/` without calling `Table.append()` — files in `data/` MUST be registered in the Iceberg catalog (see §4 "How files reach the dashboard").

### 14.6 Smoke (k6, 1 VU, 60s)

`k6 run scratch/loadtest_k6/smoke.js` against the 1h dataset. Sequential pass through all 8 endpoints. Captures response shapes and validates the harness end-to-end before adding concurrency.

### 14.7 Stop-and-review checkpoint

Before scaling to 12h/1d, review:
- Generator wall-clock matched expectations (~15s for 36M rows at 1M rows/sec, ≤60s acceptable)?
- Generator RSS stayed under 200 MB?
- All 8 endpoints returned valid responses in smoke?
- `_debug_calls` showed zero FOS calls (local-only path)?

If any of these fail, fix before proceeding to step §8.5 (grow to 12h) — the issue gets harder to debug at scale.

### 14.8 Day-1 done; day-2 picks up at §8 step 5

Day-2 work: §8.5–8.8 (12h → 1d → backfill_rollups → realistic + cache-bust → stress).
Day-3+: §8.9–8.14 (rollup-file-scale, cardinality, custom-fields, switch to 1m-rps, main-branch comparison, GCP).

### 14.9 Tier choice for final GCP validation (§10)

Default to **Tier 4** (skip 1M-RPS raw entirely) for the first pass — covers all three real production code paths with zero GCP spend. Spin up Tier 2 (~$1K) only if a stakeholder asks for end-to-end 1M-RPS-volume numbers after the local results come in.

---

## 15. Open Todos (carry forward)

**New from 2026-06-08 evening live test (§0.1)**:
- [ ] **CRITICAL: backend wedge after rapid-fire queries (F3)** — pool/lock issue where `max_wait=10s` 503 never fires and the backend stops accepting connections. Reproduce in isolation, file as separate bug, fix before any stress/concurrency scenario.
- [ ] **Cold-cache S3-manifest timeout for never-committed services (F1)** — 6–14 s on first query of every dev/test service. View-builder should short-circuit when no commit has ever happened. Real-time savings on every dashboard cold-load.
- [ ] **Buffer-path UNION-ALL scan is the hot path for our test (F2)** — getting Iceberg-committed data to a local-only backend requires patching the catalog warehouse to `file://` instead of `s3://`. Without this, all "scale" testing is testing the buffer path, not the production hot path.
- [ ] **Restart the local backend** to clear the wedged state (36 leaked TCP connections, can't service new requests as of session end).
- [ ] **Generator: trim heap to < 200 MB** by lazy-importing backend modules and using 100K-row batches (currently ~1 GB peak).

**Pre-existing**:
- [ ] **Decide on `huge_load_test` cleanup** (§14.1).
- [ ] **Verify whether buffered ingest path sorts rows by timestamp** before writing Parquet (relevant to §13.B). **Confirmed YES** — `backend/core/iceberg.py:1678` `write_to_buffer` sorts by `(timestamp ASC, ip ASC)`. The generator should match (it currently sorts by timestamp only — add ip as secondary sort key).
- [ ] **Pull a frequency sample from a real production service's rollups** to seed the Zipfian distributions more accurately (no PII, just `(value, count)` pairs). Risk #5 in §12.
- [ ] **Pick a sustained `DUCKDB_POOL_MAX_SIZE`** after the stress scenario runs — test 8/16/32 and decide. (Blocked by F3.)
- [ ] **Run the `main`-branch comparison** (§8.13) to convert the §7 first-pass targets into evidence-based targets.
- [ ] **Write `docs/performance_load_test_results.md`** once tests run — keep results separate from this plan.

---

*Note: Changes in §0 and §14–15 were made on 2026-06-08 after validating plan claims against `backend/core/iceberg.py`, `backend/config.py`, `backend/core/duckdb_pool.py`, `backend/repositories/dashboard.py`, `backend/repositories/_base.py`, `backend/models/common.py`, `backend/utils/sql_validator.py`, `backend/main.py`, `backend/core/log_fields.py`, `scripts/backfill_rollups.py`, and live probes against the running backend/frontend.*
