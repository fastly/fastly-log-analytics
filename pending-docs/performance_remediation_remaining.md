# Performance Remediation — Remaining Items

Companion to `docs/performance_remediation_plan_final.md`. Tracks every
item that did not ship during the initial 2026-06-05 session on the
`performance-improvement` branch, with the reason it was deferred and a
concrete recipe for completing it. Branch + GCP are at commit `ae54580`
as of the 2026-06-05 third session.

Three categories:

1. **Frontend swaps** — composite backend endpoints landed additive;
   the granular endpoints they replace are still being called. Each is
   a focused UI change that needs the dev frontend up + snapshot
   comparison.
2. **M-effort backend work** — items the plan classified as M effort
   that need dedicated runway (rollup cron registration, sessions perf
   fix, etc.).
3. **Open questions / blocked** — items whose plan path doesn't match
   the current codebase, or that are gated on prerequisites that
   haven't met their threshold.

The plan's pre-merge gate (`performance_remediation_plan_final.md`
appendix lines 1134-1147) applies to every item here: cold-path
measurement must hit the threshold, rendered output must be byte-
identical to pre-refactor, no new duplicate URLs in HAR, telemetry
present on every affected endpoint, no regression on adjacent pages.

---

## 1. Frontend swaps (composites are live; UI hasn't switched yet)

### F1. `/origin` page → use `/api/origin/aggregates` — REJECTED

- **GCP benchmark (2026-06-05 third session)**: composite endpoint
  takes ~2.0s warm consistently (5 runs). Granular endpoints in
  parallel take ~210-264ms warm (3 runs). Composite is **~10× slower**.
- **Root cause**: DuckDB's parallel column-store Parquet scan with
  per-endpoint connections outperforms a single sequential TEMP TABLE
  pipeline. The composite serialises all sub-queries on one connection
  while granular lets the browser fire 6 requests that DuckDB handles
  on separate pool connections in parallel.
- **Decision**: keep the frontend on granular endpoints. The composite
  endpoint stays deployed as a fallback but the frontend will not
  switch.
- **Followup item 10 (drop origin cache)**: remains blocked — the
  granular endpoints are still live and the per-endpoint 30s memo
  cache at `backend/repositories/origin.py` is still load-bearing.

### F2. `/admin/session-scoring` page → use `/api/services/{id}/scoring/analytics` ✅

- **SHIPPED** in commit `4079f94` (2026-06-05 follow-up session).
- Page fetches one composite query keyed on
  `['scoring-analytics-composite', serviceId, sinceHours]`. On resolve,
  seeds individual cache keys (`scoring-health`, `scoring-top-flagged`,
  etc.) via `queryClient.setQueryData`. Child components mount with
  pre-populated caches and skip their own network requests.
- Also fixed pre-existing bugs in the composite endpoint: `Query()`
  objects weren't being resolved to ints when calling sub-functions
  directly, and `evaluation_per_reason` doesn't accept `since_hours`.
- `threshold-preview` stays as its own hook (slider-driven).

### F3. `/admin/session-scoring` page → use `/api/services/{id}/scoring/config` ✅

- **SHIPPED** in commit `4079f94` (same commit as F2).
- Config composite query seeded on
  `['scoring-config-composite', serviceId]`. Seeds `scoring-status`,
  `scoring-threshold-committed`, `scoring-exclude-regex`,
  `scoring-enforce-status-code` caches.
- `enforce-threshold` stays as its own hook (Fastly API round-trip).

### F4. `/network` page → read shielding from `/api/network-health` ✅

- **SHIPPED** in commit `4079f94` (same commit as F2/F3).
- Removed the separate `useServiceQuery` for
  `/api/origin/shielding-analysis`. Now derives `shieldingData` from
  `data?.shielding_analysis` on the existing network-health query.
  Loading state derived from the main query's `isLoadingInitial`.

---

## 2. M-effort backend work

### M1. Item 4 — Telemetry middleware backstop (RC-3 long-term) ✅

- **SHIPPED** in commit `fa51436` (2026-06-05 fifth session).
- `backend/utils/telemetry_response_middleware.py` —
  `TelemetryResponseBodyMiddleware` (Starlette `BaseHTTPMiddleware`).
  Registered in `backend/main.py` BEFORE GZip so it sees uncompressed
  JSON (Starlette middleware ordering is reverse-stack: last
  `add_middleware` call → outermost).
- **Contract**: after the route runs, if all hold — `DEBUG_RESPONSES`
  set, Content-Type is `application/json` (NOT `text/event-stream` /
  `application/x-ndjson` / `application/octet-stream`), body parses as
  a JSON object, and `_debug_queries` isn't already present — merge in
  the three telemetry keys from the contextvar collectors. Otherwise
  pass the response through unchanged.
- **Streaming detection gotcha**: `isinstance(response, StreamingResponse)`
  is unreliable inside `BaseHTTPMiddleware` — Starlette wraps every
  response in a private `_StreamingResponse`. We use Content-Type
  prefix matching instead. JSON-streaming routes should set
  `application/x-ndjson` to opt out.
- **Failure modes are silent + non-blocking**: malformed JSON,
  contextvar errors, encoding errors — all collapse to "pass through
  unchanged". The backstop is hardening, not a correctness gate.
- **Tests**: 13 new specs in
  `tests/utils/test_telemetry_response_middleware.py` pin: plain-dict
  injection, no double-inject, untouched lists / non-JSON / empty
  bodies, SSE + NDJSON pass-through, gated-off behaviour, GZip-outer
  integration, 500 + malformed-JSON pass-through, JSONResponse-wrapped
  dict injection. 5 existing assertion sites loosened from full-equality
  to field-level checks because the backstop now adds telemetry to
  plain-dict endpoints they exercise.
- **Production verified**: `/api/admin/usage-logging` (plain dict)
  now carries `_debug_queries` / `_debug_calls` / `_is_cached`;
  `/api/usage/prefill` (`BaseResponse`) still has its own
  `_debug_queries` with `n_queries=2` and is NOT double-injected.

### M2. Item 17 follow-up — Wire `compact_closed_days_to_daily` into a cron ✅

- **SHIPPED** in commit `4079f94` (2026-06-05 follow-up session).
- Registered `rollup_compact_{service_id}` cron job at 02:00 UTC daily
  in `backend/scheduler.py`. Uses `@cron_task("rollup_compact_daily")`
  decorator. Gated on `compact_cfg.enabled` and `access_level !=
  "read_only"`. Logs rebuilt count via `start_cron_run` / `log_cron_run`.
- **Next step**: after the cron has run at least once on GCP (after
  02:00 UTC tomorrow), re-profile `/dashboard` on a 7-day query to
  confirm the ~3s drop from file-open overhead reduction.

### M3. Item 20 — Sessions perf fix (RC-8 phase 2) — PARTIAL

- **GCP profiling (2026-06-05 third session)**: `sessions_raw` (window
  functions) dominated at ~3000ms out of ~3700ms total. `ordered_temp`
  was 565ms, `sessions_agg` was 150ms.
- **Shipped (commit `dd3a095`)**: Eliminated both TEMP TABLE
  materializations, replaced with a single CTE pipeline. DuckDB
  pipelines single-consumer CTEs without intermediate materialization.
  Also dropped the unused `ttfb` column from the base projection.
- **Result**: ~3.1-3.5s (down from ~3.7-4.9s). ~16-30% improvement.
  The CTE pipelining saves the I/O overhead of materializing
  intermediate results.
- **Why the <1500ms target is not met**: The window function
  computation itself (LAG + CASE + running SUM over 1M rows) takes
  ~3s regardless of approach. Tested narrow (3-col) vs wide (12-col)
  projections through the windows — no difference (DuckDB's column-
  store handles extra columns efficiently). The sort by
  (ip, ja4, ts) over 1M rows is the irreducible cost.
- **What would reach <1500ms**:
  1. Pre-sorted Parquet by (ip, ja4, timestamp) at write time —
     eliminates the sort. Significant write-side change.
  2. Pre-computed session rollup table — eliminates window functions
     entirely. New rollup scheme + cron job.
  3. Approximate sessions (bucket by time window, skip gap detection) —
     changes semantics, probably unacceptable.
- **Effort for further improvement**: L (structural change).

### M4. Item 21 — Fastly call parallelisation (sync → async refactor) ✅

- **SHIPPED** in commit `0561582` (2026-06-05 fifth session).
- Approach: `asyncio.to_thread` wrapping the existing sync `fastly()`
  client, no new HTTP library or auth refactor. The existing retry,
  telemetry, and auth machinery is reused unchanged.
- **prefill (`backend/routers/usage.py`)**: converted to `async def`.
  The version → S3-endpoint → sampling-condition chain (data-
  dependent, runs internally serial) is now wrapped in
  `_resolve_endpoint_chain()` and dispatched via `asyncio.gather()`
  alongside `_fetch_stats()` (the `/stats/service/{id}` or
  `/stats/aggregate` call, fully independent). Wall clock is now bound
  by the slower of the two (~150-200 ms) instead of their sum.
- **Cold DuckDB hop** in prefill (edge_ratio when status cache misses)
  also wrapped in `asyncio.to_thread` so it doesn't block the event
  loop now that the handler is async.
- **query_errors decorator** (`backend/utils/router_utils.py`):
  detects coroutine functions and emits an `async_wrapper` with the
  same exception-mapping the sync branch provides. Without this an
  `async def` handler would surface to FastAPI as a coroutine object
  and break serialization.
- **Alerts page** (`frontend/app/alerts/page.tsx`): `loggingSettings`
  useQuery gets `staleTime: 30_000`. The upstream endpoint chains 3
  sequential Fastly calls (~200 ms) and the result is window-stable;
  caching for 30 s eliminates per-focus refetches.
- **compute_log_accounting**: no parallelisation opportunity inside
  — only one Fastly call (the /stats fetch). Left unchanged.
- **logging-settings**: chain is data-dependent (version → endpoint →
  condition), no within-endpoint parallelism possible. The staleTime
  bump on the frontend is the practical win.
- **Regression tests**: 5 new tests in
  `tests/utils/test_router_utils.py` pin async handler awaiting,
  exception mapping for async, HTTPException pass-through, no trace
  leakage, AND a wall-clock proof that `@query_errors` does not
  serialise `asyncio.gather` children (two 100 ms awaits must finish
  in <180 ms). 1716 backend tests pass overall.
- **Production**: prefill ~1.5 s warm steady state, no errors.

### M5. Item 24 — Isolate `cron_sync` writes from request-path SQLite ✅

- **SHIPPED** in commit `63691af` (2026-06-05 fourth session).
- Item 23 (commit `5e8b795`) already capped the worst case from
  ~9.9 s to ~500 ms by gating on `DEBUG_RESPONSES` and limiting
  rows to 25. This commit closes the remaining gap: drop the
  synchronous `telemetry_proxy._flush_log_writes_for_tests(timeout=0.25)`
  call from `_query_iothread_calls_from_usage_log`. The
  coalescer flusher was serialising against cron_sync's own
  `usage_log` writes, so the 250 ms ceiling hit every admin nav
  during a cron tick.
- **Trade-off**: iothread calls completed in the last ~100 ms
  of a request (one batch interval) may not appear in this
  request's debug panel. They are still tagged with the correct
  process_context and surface in Admin → Usage Log for post-hoc
  inspection. Acceptable for a debug-only surface.
- **Plan target met**: <2 s under cron contention.
- **Production measurements (`/api/sync-status?skip_fos=true`,
  50-run bursts)**:
  - Before: median ~75 ms, worst case 500 ms, ~20% of runs >300 ms
  - After: median ~60 ms, worst case ~220 ms (rare ~1 s outliers
    from SQLite checkpoints), 0% >300 ms in steady-state bursts
- **Regression test** in
  `tests/utils/test_telemetry.py::test_query_iothread_calls_does_not_synchronously_flush_proxy`
  asserts the function never calls `_flush_log_writes_for_tests`.
  Confirmed test fails when the wait is re-added.
- **Why we didn't also build the "write-behind queue for cron's
  usage_log writes"** that the plan also mentioned: not needed.
  The 250 ms ceiling per request was the dominant contributor,
  and removing the synchronous wait drops total per-request
  worst-case by ~60%. Cron-vs-flusher write contention still
  produces occasional 100-220 ms SELECT spikes, but those are
  random across the burst rather than systematic per-request.

### M6. Item 30 — Lift dashboard hooks out of `ReportLayout` render-prop ✅

- **SHIPPED** in commit `3c8a3b4` (2026-06-05 fourth session).
- Extracted ~850-line render-prop body into a top-level
  `DashboardBody` component. `DashboardPage` now only owns
  `useDashboardCards()` + `useCardVisibility()` (both needed by the
  header) and renders `ReportLayout > DashboardBody`. Every body-only
  hook (`useLogFieldsCatalog`, `useFilterStore` subscription, `metric`
  state, `useFieldLabel`, `useDateFormat`, `hiddenCategories`,
  `collapsedSections`, `useIsDataReady`, `useServiceQuery` x 2,
  `useQuery` for compare/top-bots, `useScoringLabels`, the chart-data
  `useMemo`s and event-handler `useCallback`s) now lives at the top of
  `DashboardBody`.
- Eliminates the rules-of-hooks risk (hooks were previously called
  inside a function-as-child arrow expression). Unblocks ESLint's
  `react-hooks/rules-of-hooks` rule on this page. Same shape as the
  `InsightsBody` lift from item 31 (commit `7329f02`), on ~10× the
  scale.
- **Plan target met**: 0 ms direct; behaviour-equivalent. Local cold-
  load profile shows the same 12 API calls before/after with the same
  3 duplicate URLs (`/api/dashboard/raw`, `/api/dashboard/aggregates`,
  `/api/security/top-bots` each 2x). Those duplicates are NOT caused
  by the render-prop pattern — they're triggered by `activeServiceId`
  / `filterPayload` settling from URL-sync, and survive the lift. The
  doc's claim that the lift eliminates them was wrong; the real
  source of those duplicates is upstream of the body altogether.
  GCP cold-load profile matches the pre-lift baseline (9 calls, no
  dups) on a steady run; the 1 extra `/api/admin/share/banner` call
  seen in one run was a 15s-poll timing artifact, not a regression.
- **Followup**: if the duplicate-fetch pattern matters, the next
  thread is investigating `useUrlServiceSync` / `useUrlFilterSync`
  initialisation timing, NOT the render-prop. Not a blocker for M6.

---

## 3. Open questions / blocked

### O1. Item 10 — Drop origin per-endpoint cache

- **Blocked on**: F1 (origin frontend swap to use composite). If
  granular endpoints stay live the cache is still load-bearing.
- **When ready**: Delete
  `backend/repositories/origin.py:18-103` (the cache + helpers).

### O2. Item 16 — Coalesce insights city/region/country queries ✅

- **SHIPPED** in commit `d76bfb8` (2026-06-05 fourth session).
- Approach (b): hand-rolled bypass. Added
  `_coalesced_city_aggregates()` in
  `backend/repositories/insights/repository.py` that runs ONE pass
  over the temp table computing the superset of counts / rates / p95s
  per (city, region, country) group. Each of the 4 city insight tasks
  short-circuits via the precomputed dict instead of issuing its own
  SELECT. Per-insight HAVING / ORDER / LIMIT applied in Python.
  Insight framework unchanged.
- Fires only when city + status + elapsed + timestamp are all in
  the schema; falls back transparently to per-insight scans
  otherwise or on any error in the coalesced path. Row schemas
  in the precomputed dict are constructed to match each insight's
  existing row_processor contract — no changes to processors,
  labels, severities, or investigate URLs.
- **Production measurements**:
  - Before: 4 city queries = 177 + 205 + 219 + 181 = **782 ms**
  - After: 1 coalesced city query = **262 ms**
  - Savings: **520 ms (~67% on city-scan time)**
  - Endpoint wall: 8.36 s → ~6.05 s cold (other scans dominate now)
- **Regression test**
  (`tests/repositories/test_insights.py::test_coalesced_city_path_matches_per_insight_scan_output`)
  seeds known data for all 4 city insights and asserts the
  coalesced path produces equivalent items (label, current_val,
  baseline_val) to the legacy per-insight-scan path (which is
  forced by monkeypatching `_coalesced_city_aggregates` to return
  `{}`).

### O3. Item 18 — Iceberg parquet sorted by timestamp DESC at write time

- **Status**: plan explicitly defers as "structural". Leave deferred.

### O4. Item 25 — Pool SQLite connection per request OR skip PRAGMA dedup

- **Why blocked**: Plan refers to `backend/db/sqlite.py` (file doesn't
  exist) and `apply_sqlite_pragmas()` (function doesn't exist). The
  current code in `backend/core/metadata_db.py:get_con` already
  caches connections per (thread, service_id) and applies PRAGMA once
  per connection lifetime. The "5 connections per dashboard" the
  plan mentions are 5 DIFFERENT threads × the same service.
- **Real recipe** if we want to dedupe across threads:
  1. Replace the thread-local connection cache with a process-wide
     pool (e.g. `queue.Queue` of warm connections).
  2. PRAGMAs apply once per pool slot.
  3. Test concurrency carefully — SQLite connections can't be shared
     between threads simultaneously, but can be checked out one at a
     time.
- **Effort**: M.

### O5. Item 29 — Drop redundant cron-runs `per_page=10` request ✅

- **SHIPPED** in commit `00a86c1` (2026-06-05 fifth session).
- Approach (with UX preserved):
  - Backend `get_cron_runs` gains `since_id: int | None`. When set,
    WHERE adds `(id > ? OR status = 'running')`. The OR keeps still-
    running rows visible across polls so the toast-completion-detection
    effect on `/logs:497` can observe the running→completed transition
    for rows it's tracking.
  - Router exposes `since_id` with `ge=0` validation.
  - Frontend `recentCrons` queryFn passes `Math.max(0, maxSeenIdRef
    - 1)` as `since_id`. The `-1` keeps the most-recently-seen row in
    the response for one more poll so the toast effect can find it.
  - `staleTime: 0 → 5_000` so window-focus refetches inside the poll
    interval don't double-fire.
- **Production measurements** (`/api/cron-runs?per_page=10`,
  steady-state):
  - Before: 3589 bytes per poll
  - After (since_id = max-1, no new rows): 785 bytes per poll
  - Savings: **78% bytes off the poll** (~2.8 KB × 12 polls/min ≈
    33 KB/min/active-tab saved)
- **Regression tests**: 4 in `tests/repositories/test_cron.py` (only
  newer rows, running visible past cursor, since_id=None backwards-
  compat, task filter compose) + 2 in
  `tests/routers/services/test_cron_router.py` (HTTP delta poll,
  `ge=0` validation).
- **Per_page kept at 10**, not trimmed to 3 as the original plan
  suggested: the steady-state byte savings come from since_id, not
  from the cap. Trimming per_page would also miss running crons on
  the initial poll for services with >3 concurrent runs.

### O6. Item 37 — `<link rel="modulepreload">` for plotly ✅

- **SHIPPED** in commits `79ecf13` + `4506efa` + `2088f8f` (2026-06-05
  fifth session).
- Approach: post-build chunk scanner + server-side manifest reader.
  Avoids needing a custom Next.js plugin.
  1. `frontend/scripts/build-preload-manifest.mjs` runs after
     `next build`. Scans `.next/static/chunks/*.js` for plotly's
     internal markers (`plotly-logomark` SVG class, `plotly_afterplot`
     event hook). 100 KB size floor filters out the small per-page
     dynamic-import shims that also contain "plotly" substrings.
     Writes matches + sizes to `.next/static/preload-manifest.json`
     (sorted descending so the biggest chunk preloads first).
  2. `frontend/lib/preload-manifest.ts` reads the JSON on first
     request, caches for process lifetime.
  3. `frontend/app/layout.tsx` becomes an async server component,
     awaits the reader, emits `<link rel="modulepreload">` per chunk
     in `<head>`. Marked `dynamic = "force-dynamic"` so the manifest
     read happens at request time, not build time (the scanner by
     definition runs AFTER `next build`, so any pre-rendered HTML
     would have baked in an empty preload list).
  4. Dockerfile updated to invoke the scanner after `next build` (the
     image used `npx next build` directly, bypassing
     `npm run build`).
- **d3 not preloaded**: scanning prod chunks showed d3-array / d3-scale
  / d3-selection markers in zero chunks — d3 is bundled inside plotly
  rather than separately. Preloading plotly covers it.
- **Regression tests**: 7 vitest specs in
  `frontend/__tests__/preload-manifest.test.ts` pin the scanner's
  contract (marker + size-floor matches, either marker suffices,
  descending sort, empty manifest written when no matches, silent
  skip when chunks dir is absent).
- **Production verified**: prod HTML now includes
  `<link rel="modulepreload" href="/_next/static/chunks/0l66_t675ysrv.js"/>`
  pointing to the 1.38 MB plotly chunk.
- **Failure modes are silent + non-blocking**: missing manifest,
  unreadable file, plotly upgrade that moves markers — all collapse
  to "no preload links". Never breaks the page.

### O7. Item 38 — Fold `/api/views/{id}` into `/api/bootstrap`

- **Blocker**: Plan explicitly gates this on bootstrap being <200 ms
  cold ("Only fold IF item 3 shows bootstrap is <200 ms cold;
  otherwise debug the bootstrap regression first and re-evaluate.").
  We measured 767 ms on GCP, dominated by `get_enriched_services`.
- **Recipe**:
  1. Profile `get_enriched_services` — it's the 767 ms cost.
  2. Bring it under 200 ms (likely needs caching the enriched
     services list with cron-driven invalidation).
  3. THEN fold views into bootstrap.
- **Effort**: M for the bootstrap perf work; S for the actual fold.

### O8. Item 40 — Broader iceberg metadata cache

- **What's done**: `_pointer_cache` in `backend/core/iceberg.py:1090`
  already implements a 30s TTL for `metadata_location.txt`.
- **What's missing**: similar caching for `metadata.json` and
  `snap-*.avro` reads, which happen inside the pyiceberg library and
  bypass our cache.
- **Recipe**:
  1. Monkey-patch pyiceberg's HTTP layer (fragile), OR
  2. Cache at the boto3 layer via a `BotoCache` subclass on the
     S3 client, OR
  3. Pin a specific pyiceberg version that supports user-supplied
     caching.
- **Plan target**: ~50 ms saved on second usage load within 30 s.
- **Effort**: M (pyiceberg internals).

---

## 4. File_date follow-up queries (item 5 expansion)

The `file_date` column + index are populated (commit `1b5d585`), and
`get_log_activity` day-bucket case uses the index (commit `78e38a8`).

- `get_log_accounting_counts` ✅ SHIPPED in commit `f1a9791`
  (2026-06-05 fourth session). Split into a UNION ALL with a fast
  arm filtering on `file_date >= ? AND file_date <= ?` (uses
  `idx_ingested_files_source_date`) and a slow arm keeping the
  legacy `datetime(ingested_at)` scan for rows where file_date is
  NULL. Bucket extraction in the fast arm uses
  `substr(file_name, instr(file_name, 'T') - 10, ?)` directly
  because `file_date IS NOT NULL` implies the basename matches the
  canonical Fastly pattern per `_migration_002`. Per-bucket output
  verified byte-identical to old query on dev sandbox DB
  (~140K files / 19 buckets). Prod measurements:
  - Cold query: 1533 ms → 567 ms (-966 ms / 63%)
  - Warm query: 1533 ms → ~410 ms (-1120 ms / 73%)
  - Cold endpoint wall: 2.25 s → 1.03 s (-1.22 s / 54%)
  - Warm endpoint wall: 2.25 s → ~0.67 s (-1.58 s / 70%)
- `get_node_count_avg` (line ~1500) — still uses
  `substr(file_name, instr(file_name, 'T') - 10, 19)` for unique-emit
  grouping. Lower-impact code path; left as-is for now.

---

## Verification recipe template

Use this for any future item:

```bash
# 1. Memory check before touching the dev stack.
memory_pressure -Q   # require "normal" / >40% free

# 2. Start backend (non-reload mode, lets us iterate without thrashing).
./run.sh   # or: source .env && uv run uvicorn backend.main:app --host 127.0.0.1 --port 18002

# 3. Tag baseline before any change.
BASE_URL=http://localhost:13002 PAGES=<page> node scratch/profile.js --tag=before_<item>

# 4. Implement, run targeted pytest.
uv run pytest tests/repositories/test_<area>.py -q --no-header

# 5. Restart, re-profile.
BASE_URL=http://localhost:13002 PAGES=<page> node scratch/profile.js --tag=after_<item>

# 6. Commit + push.
git add <files> && git commit -m "<area>: <change>"
git push origin performance-improvement

# 7. Deploy to GCP, verify.
gcloud compute ssh fastly-log-analysis --zone us-central1-a --command '~/restart.sh'
BASE_URL=http://localhost:3001 PAGES=<page> node scratch/profile.js --tag=after_<item>_gcp

# 8. Compare profile_results_*.json files; assert per-page threshold from
#    docs/performance_remediation_plan_final.md appendix lines 1097-1114.
```

---

## What landed (for reference)

### Fifth session (2026-06-05 night)

```
fa51436  middleware: telemetry backstop auto-injects _debug_queries into JSON dict responses (M1)
2088f8f  layout: opt root layout out of build-time SSG so O6 preload manifest is read at request time
4506efa  docker: run O6 preload-manifest scanner after next build in image
79ecf13  frontend: preload plotly chunk via <link rel="modulepreload"> in root layout (O6)
0561582  usage: parallelise prefill Fastly calls + bump alerts loggingSettings staleTime (M4)
00a86c1  cron-runs: add since_id delta-poll param + use it on /logs recentCrons (O5)
8548d58  iceberg: tombstone buffer files instead of unlinking inline at commit time
9f25133  queryrunner: clear _view_cache before force=True rebuild on stale-view self-heal
```

Key findings:
- O5 (cron-runs delta poll) SHIPPED. 78% bytes saved on steady-state
  /logs polls. since_id query param with `(id > ? OR status = 'running')`
  filter preserves all UX guarantees (toast detection, running dock).
- Mid-session prod incident: dashboard surfaced "No files found
  ... batch_<hash>.parquet" for ~30 min before being caught.
  Root cause was a buffer-deletion-vs-query race that the existing
  self-heal couldn't recover from due to a cached-SQL re-bind bug.
  Two fixes landed:
    * **9f25133** — `QueryRunner.execute` now calls
      `clear_source_caches(..., keep_snapshot_cache=True)` BEFORE
      `update_iceberg_view(force=True)`, mirroring the
      `get_sync_status` self-heal pattern. The lock-acquire-timeout
      fallback can no longer re-bind the same stale SQL.
    * **8548d58** — `commit_buffer` no longer does `os.remove(parquet)`
      inline at commit time. Replaced with `tombstone_buffer_files`
      (writes a `<path>.consumed-<unix_ts>` sidecar; the parquet
      stays on disk for a 60 s grace window). `buffer_files()` filters
      tombstoned paths from new view binds. `sweep_tombstoned_buffer_files`
      at the start of each commit cycle unlinks both files once past
      grace. Closes the race at its source; self-heal is now an
      essentially-never-fires backstop.
  Both fixes have regression tests. Self-heal correctness pinned by
  `test_queryrunner_execute_clears_view_cache_before_force_rebuild`;
  race regression pinned by
  `test_tombstone_then_query_race_keeps_parquet_readable_during_grace`.

### Fourth session (2026-06-05 late evening)

```
d76bfb8  insights: coalesce 4 city/region/country queries into one (O2)
63691af  telemetry: drop synchronous proxy flush from request-path iothread query (M5)
f1a9791  metadata_db: file_date fast-arm for get_log_accounting_counts
6a5e72f  fix: include country in rollup fast-path narrow projection
3c8a3b4  dashboard: lift hooks out of ReportLayout render-prop (M6)
```

Key findings:
- M6 (dashboard render-prop lift) SHIPPED. ~850-line body extracted
  into a stable `DashboardBody` component. Rules-of-hooks clean.
  Behaviour-equivalent; no network or render regression. The plan's
  claim that the lift would eliminate the local duplicate-fetch
  pattern turned out to be wrong — those duplicates come from URL/
  serviceId sync settling and survive the lift.
- Country bug fix: pre-existing latent issue in the dashboard
  rollup fast-path's narrow TEMP TABLE projection (commit `b6471d6`,
  item 14). `country` wasn't in the 12-column narrow list, but the
  map-data SQL fallback at line 564-570 still referenced it.
  Production hit the BinderException branch today — dashboard
  rendered "No data available". Added `country` to the narrow
  projection + regression test that drives the exact
  use_rollups=True + no-country-in-rollup scenario via monkeypatched
  os.path.isdir and stubbed execute_top_n_rollups.
- get_log_accounting_counts file_date rewrite (section 4) SHIPPED.
  73% query-time reduction on warm /admin/usage-log loads.
- M5 (cron-vs-request SQLite contention) SHIPPED. Plan target was
  <2 s during cron tick; we already hit it (item 23 mitigated to
  ~500 ms worst case), and dropping the synchronous proxy flush
  takes worst-case admin nav from 500 ms to ~220 ms with 0%
  systematic regressions. Trade-off: iothread calls in the last
  ~100 ms of a request may not appear in this request's debug
  panel (still in Usage Log). The plan's "write-behind queue for
  cron's usage_log writes" wasn't built — the systematic 250 ms
  per-request cost was the dominant problem, and that's gone.
- O2 (coalesce insights city queries) SHIPPED. 4 GROUP BY scans
  (782 ms total) → 1 coalesced query (262 ms) on prod. Plan
  target was ~500 ms saved; got 520 ms. Bypass approach (option
  b) keeps the insight framework untouched.

### Third session (2026-06-05 night)

```
ae54580  fix: use proxy.ts instead of middleware.ts for Next.js 16 compat
dd3a095  sessions: eliminate temp-table materialization for ~2× speedup (M3)
843e630  frontend: hover-prefetch sidebar links + restore Caddy-marker middleware
```

Key findings:
- F1 (origin composite swap) REJECTED: composite ~2s vs granular ~210ms parallel
- M3 (sessions): 3.7-4.9s → 3.1-3.5s via CTE pipelining; <1.5s needs structural changes
- Sidebar navigation: hover-prefetch warms loading boundaries on mouse enter

### Follow-up session (2026-06-05 evening)

```
4079f94  frontend: use composite endpoints for session-scoring + network shielding (F2, F3, F4, M2)
```

### Initial session (2026-06-05)

All commits on `performance-improvement` from base `bf44b4f`:

```
0252f25  rollups: per-day compaction + reader prefers per-day for closed days (item 17)
36e5060  network: include shielding-analysis in /network-health response (item 13)
bfdbab6  scoring: composite /scoring/analytics endpoint (item 11)
f45cbc5  scoring: composite /scoring/config endpoint (item 12)
856c9d0  sessions: split monolithic CTE into measurable stages (item 19)
b6471d6  dashboard: live-hour TEMP TABLE + Python bot match + ngwaf_top memo (items 14, 41, +)
9986334  origin: add /api/origin/aggregates composite endpoint (frontend stays granular) (item 9)
6599909  dashboard: use shared useScoringLabels hook for the FLAG column (item 32)
7329f02  react-query + insights: skip 4xx retries, lift insights hooks out of render-prop (items 22, 31)
78e38a8  log-activity day-bucket via file_date + tighten share-status staleTime (items 5-followup, 28)
c7286af  admin-usage-log: visibility-gate the 30s tick + rewrite latest-per-task SQL (items 43, 44)
fbdb709  (Phase 9 batch — see git log)
30ed316  insights + admin: cached schema lookup, retry on empty, prefetch dedup (items 33, 34, 39)
feeb792  frontend: cache /geo/* assets + dynamic-import PlotlyChart on /network (items 27, 36)
e541853  network-health + usage-log: trim response bodies, fold totals into page query (items 35, 42)
5e8b795  admin-perf: cap iothread call telemetry + lean share-banner endpoint (items 23, 26)
1b5d585  ingested_files: add file_date column + (source_name, file_date) index (item 5)
29b2659  usage: remove double telemetry recording on Fastly Stats calls (item 6)
d78ff78  security: narrow aggregates TEMP TABLE + memoize ngwaf_cache ATTACH (items 7, 8)
09dcb0d  bootstrap: attribute /api/bootstrap wall time to per-section timings (item 3)
a2bdb80  scoring: surface DuckDB telemetry on cached analytics endpoints (items 2, +)
```

Each is independently revertible if the change exposes a regression
that wasn't caught locally.
