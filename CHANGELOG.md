# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Cleanup

Post-2.0.0 cleanup sweep applying an in-tree audit's recommendations.
The pattern across the work was the same on every front: kill the dual
maintenance that survived the package carve-up.

- **Three SQLite pools collapse into one.** `metadata.base`,
  `metadata.usage_log_db`, and `share_db.connection` all owned
  identical thread-local pool machinery (same module globals, same
  PRAGMAs, same init lock). They now share `ThreadLocalPool` in
  `backend/core/sqlite_pool.py`. share_db queries flow through
  `InstrumentedConnection` for the first time — they now appear in
  the Live Query Monitor under `service=__global_share__`.
- **Origin summary's per-query templates collapse into one path.**
  `TEMP_SUMMARY_ROLLUP` + `TEMP_SUMMARY_BY_EDGE` are gone; the live
  and TEMP-table paths both use `SUMMARY_GROUPING_SETS` through a
  shared `_shape_summary` helper that reads rows by column name
  (`cursor.description` dict access) instead of positional indices.
- **Cron job tails consolidated.** Five `finally:` blocks ending in
  the same `if run_id: update_cron_duration ... except: pass`
  boilerplate route through `finalize_cron_duration`. The 16+
  `load_config / 404` preambles funnel through `load_service_config`.
  Three `start_cron_run → spawn-thread → 503` triples collapse into
  one `start_or_resume_cron`. Per-hour bundle walks
  (`collect_hourly_bundle_paths`) and the two cross-package migration
  runners (`run_pending_migrations`) get the same treatment.
- **Mixins + helpers for the small repeated shapes.**
  `LogExtentsMixin` (`earliest_log_at` + `latest_log_at`),
  `OkResponse` (`ok: bool = True`), `_atomic_write_json`,
  `_get_cfg_field`, `client_ip`, `shim_attr`, plus iceberg
  `_iceberg_root_prefix` + `_metadata_pointer_candidates`.
- **`fetch_service_name` now routes through the shared `fastly()`
  client** instead of an inline urllib body. Adds a `timeout` keyword
  to `fastly()` (default 30 s preserves the existing behavior of the
  ~50 other call sites) and the name-fetch call site pins
  `timeout=10` + `max_retries=1` so the cold-path tail caps at ~21 s
  vs the client default of ~127 s. Caller is behind a 300 s name
  cache so steady-state cost is unchanged.
- **`_run_falco_lint` absorbs the falco subprocess plumbing** shared
  by `vcl_utils.lint_log_format` (logging-endpoint VCL check) and
  `vcl_validator.lint_vcl` (scoring-snippet VCL check). Each caller
  keeps its own falco-not-available handling, timeout budget, and
  output parser — the helper only owns the tempfile lifecycle,
  `subprocess.run` invocation, and tempfile-path redaction. The two
  use cases stay distinct on purpose (logging is best-effort, scoring
  is a security boundary).

### Fixed

- `start_proxy_server` race that surfaced as
  "proxy server is not running" when N reader threads called
  `get_connection` simultaneously on a cold process. Concurrent
  first-callers now serialise the thread-start decision and wait
  on `_READY` outside the lock so every caller reads `_PORT` after
  the server has bound.
- `get_metadata_storage_stats` + `cleanup_metadata` silently
  ignored the `usage_log` table on every fresh service after
  the v2.0 per-service-file split — the helpers still read
  `metadata.db`. Routed through `usage_log_db` so admin storage
  stats and the retention cleanup job actually see the rows.
- `sync.py` cron tail used to emit a misleading
  "View refresh + warm: Xms" status event even on failure (the
  success log sat outside the try/except). The shared
  `refresh_view_and_warm_pool` puts the success log inside the
  try/except so failure means no event.
- `start_cron_run` non-sync task types fell back to
  `cron_compact.log_retention_days` via a buggy ternary; the
  promoted `_TASK_TO_CRON_KEY` mapping plus a default 7-day
  fallback gets the correct retention applied per task.
- `query_instrumentation._safe_weakref` silently no-op'd the
  memory probe when wrapping non-weakref-able cursors; promoted
  the registry-version's strong-ref-closure fallback so the probe
  always tracks.
- `local_compaction` hour-tier tests were flaky on any clock more
  than 30 days past the hardcoded sample dates — the fixture now
  pins both `_DAILY_TIER_AGE_DAYS` and `_WEEKLY_TIER_AGE_DAYS` so
  neither tier sweeps the test partitions out from under the
  assertions.

### Removed

- `backend/utils/retry.py`, `backend/utils/cdn.py`,
  `backend/core/settings.py` (Path-B removal of three migration
  scaffolds that never adopted in tree). `pydantic-settings`
  dropped from `pyproject.toml` + `uv.lock` (was the sole
  consumer).
- Legacy `usage_log` DDL + 3 triggers + 4 indexes in
  `metadata.base._SCHEMA` (the table moved to its own per-service
  file pre-2.0). `migrate_from_metadata_db` and
  `_migration_003_rebuild_usage_log_hourly_summary` deleted.
- Scrypt passcode verify path + `PASSCODE_DEFAULT_ALGO_KEY` +
  `_migration_003_passcode_algo_marker` (cutover happened
  pre-2.0; fresh installs have no scrypt rows).
- `TunnelState.use_tunnel` + `tunnel_url` + the
  `share_admin` response keys that exposed them (always
  False/None since v2.0 deleted the SSH path).
- Per-checkin `_cleanup_temp_tables` sweep in `duckdb_pool` —
  the "safety net" was unreachable because the failure path
  discards the connection before the sweep can run.

## [2.0.0] - 2026-06-12

Architecture cleanup release. The post-`v1.2.0` perf branch closed the
worst read-path latency by stacking remediation on top of an
architecture that wasn't designed for the workload; this release pays
that down. The largest backend files were carved into per-concern
packages, telemetry moved to OpenTelemetry + structlog, tenancy got a
typed `RequestContext` boundary, frontend hydration warm-up hacks were
replaced with policy, and the test + type gates ratcheted to a level
that catches regressions on the way in. Composite endpoints land as a
hard cutover — frontend + backend ship together, granular endpoints
deleted.

### Architecture

- **`backend/core/iceberg.py` (4,232 LOC)** → `iceberg/` package
  (`view`, `catalog`, `warehouse`, `manifest`, `fs`, `_core`,
  `buffer`, `ddl`, `snapshot_cache`, `dedup`, …). Custom
  `FosFsspecFileIO(FsspecFileIO)` + `CachedFosS3FileSystem(S3FileSystem)`
  subclasses replace 5 of the 6 historical `s3fs` monkeypatches;
  only the `ThreadPoolExecutor.submit` ContextVar wrapper remains
  (see [MONKEYPATCHES.md](MONKEYPATCHES.md)).
- **`backend/scheduler.py` (2,843 LOC)** → `backend/cron/` package
  with `scheduler`, `decorators`, and per-job modules under
  `cron/jobs/` (`sync`, `commit`, `compaction`, `optimize`, `expire`,
  `metadata`, `gap_heal`, `rollup_compact_daily`). The scheduler
  picks the **separate-pool** isolation strategy based on Phase 1
  thread-wait telemetry; the deferred-view-cache-invalidation hack
  is gone.
- **`backend/core/metadata_db.py` (3,168 LOC)** → `backend/core/metadata/`
  package with concern-partitioned mixins (`base`, `alerts`, `views`,
  `ingest_log`, `cron_log`, `asn_cache`, `usage_log`, `reconciliation`,
  `state`). `metadata_db.py` becomes a thin backward-compatible shim.
- **`backend/utils/tunnel.py` (1,022 LOC)** → `backend/utils/tunnel/`
  package (`manager`, `session`, `rate_limiter`, `state`,
  `fingerprint`). The SSH-to-localhost.run path is **deleted entirely**
  (~400 lines): no more SSH subprocess + sleep-listener + reconnect
  state machine. Direct-mode only; production has always used direct.
- **`backend/core/share_db.py` (1,312 LOC)** → `backend/core/share_db/`
  package (`connection`, `schema`, `invites`, `sessions`, `audit`,
  `passcode`, `tos`, `settings`). `argon2-cffi` replaces `scrypt` for
  passcode hashing.
- **`backend/routers/admin.py` (1,650 LOC)** → `backend/routers/admin/`
  package (14 sub-modules: `pop_locations`, `ingest`, `trees`,
  `downloads`, `sync_status`, `compaction`, `health`,
  `log_accounting`, `iceberg`, `bot_sources` + shared
  `_helpers` / `_dir_size` / `_router`).
- **`backend/core/rollups.py` (2,045 LOC)** → `backend/core/rollups/`
  package (8 sub-modules: `_common`, `time_series`, `sessions`,
  `hour_bundles`, `day_bundles`, `recompute`, `wellknown_bots`).
- **`RequestContext` replaces `AnalyticsDeps`** ([`backend/core/request_context.py`](backend/core/request_context.py)).
  Tenancy is enforced at context construction; routes never parse a
  `service_id` from a path param. The security-load-bearing private
  `read_only` attribute is now structurally unexposable as a query
  param.
- **Composite endpoints + hard cutover** — `dashboard/bundle`,
  `security/bundle`, `network/bundle` ship together with the frontend
  swap. Granular per-card endpoints deleted, `_meta_con` parallel path
  dropped, `is_cached/_is_cached` alias collapsed,
  `AnalyticsDeps = RequestContext` shim removed. Top-5 backend files
  now ≤ 1,461 LOC; no backend file > 1,500.

### Telemetry, observability

- **OpenTelemetry** (`opentelemetry-api/sdk` +
  `fastapi`/`botocore`/`aiohttp` instrumentors) replaces the four
  fragmented custom telemetry surfaces. Console exporter ships by
  default; backends (Jaeger / Tempo / Honeycomb / …) are a
  deploy-config decision, not part of this release.
- **`structlog`** wires `trace_id` + `span_id` into structured log
  output via a custom processor.
- **`process_context_scope` + `_ACTIVE_CONTEXTS` mirror kept** at
  [`backend/utils/telemetry.py`](backend/utils/telemetry.py). OTel context
  propagation uses Python ContextVars under the hood, which inherit
  the cross-thread limitation (fsspec iothread, pyiceberg
  ThreadPoolExecutor) the manual mirror was built to solve; removing
  the mirror would re-introduce the ~80%-NULL telemetry bucket
  observed on 2026-05-20. Docstring + plan entry document the
  reasoning.
- **`RequestTelemetry`** thin wrapper owns section spans, query
  attribution, call log, and the custom `app.thread_wait_ms` metric
  that fed the Phase 6 separate-pool decision.

### Reliability, perf

- **`aiodns` + `asyncio.gather` + bulk-transaction sqlite writes** in
  [`backend/utils/rdns_cache.py`](backend/utils/rdns_cache.py) replace the
  serial-blocking `socket.gethostbyaddr` loop that wedged the sync
  worker for minutes on bulk lookups.
- **`tenacity`** decorator-based retry replaces ad-hoc try/except loops
  for Fastly API + NGWAF + SQLite WAL-busy paths; centralised policy
  on `Settings`.
- **`pydantic-settings`** centralises env-var reads + boot validation
  (the "TRUSTED_PROXY_IPS required in prod" gate is now a pydantic
  validator).
- **`cachetools`** replaces `bounded_cache` / `rdns_cache` /
  `ngwaf_bot_cache` in-process LRU/TTL implementations.
- **Structured `.tf.json`** generation replaces f-string HCL +
  `_hcl_escape` regex (`backend/utils/terraform_gen.py`), eliminating
  the custom-HCL escaping injection vector.
- **`orjson` via FastAPI `ORJSONResponse`** for ~5–10× faster JSON
  serialisation on composite endpoint payloads.
- **`rich` + `typer`** for the provision CLI; `httpx` everywhere
  except `telemetry_proxy.py` (which stays on `aiohttp` for the proxy
  server role).
- **`nuqs`** as the URL state source on the frontend, replacing the
  custom Zustand/Effect sync hooks that produced hydration desync on
  refresh.
- **`session_scoring._cached`** clears `_inflight` on the cache-hit
  path too, not only on producer-path teardown — concurrent callers
  on a hot cache key no longer leak the inflight registration when
  the producer finishes before they wake up.
- **`iceberg/buffer.tombstone_buffer_files`** logs + skips on
  marker-write failure (the immediate-`os.remove` fallback re-opened
  the in-flight-query race the tombstone grace window exists to
  close). Pair regression test pins the contract.
- **`DROP TABLE IF EXISTS` identifier quoting** at 11 temp-table
  cleanup sites so the drop tolerates reserved keywords / hyphenated
  service slugs that would otherwise raise.

### Trust topology, middleware

- **Middleware order asserted at boot AND in tests** — the
  multi-paragraph prose comments in `main.py` were replaced with
  one-line `# INVARIANT` markers + a boot-time crash if
  `app.user_middleware` doesn't match the declared tuple. Snapshot
  tests cover Caddy + docker-compose middleware order too.
- **`@pytest.mark.security_regression` marker + monotonic-count CI
  gate** (floor: 24, from `audit-findings/`). Every test covering a
  verified security fix carries the mark; a refactor cannot silently
  drop coverage of a known fix.
- **Trust-topology snapshot tests** pin Caddy `@from_fastly` matcher,
  XFF forwarding, `/share-login` rate-limit, and the backend
  `--forwarded-allow-ips=127.0.0.1` flags.
- **`raise_internal(logger, exc, code, status)`** replaces
  `raise HTTPException(detail={"error": str(e)})` at every backend
  except site that previously echoed the original exception message
  to the client. Detail is now `{"error": <code>, "error_id": <8-hex>}`;
  the full exception lands in the server log with the same
  `error_id` so operators triage without the upstream body / token
  fragments leaking on the wire.
- **`escape_sql_literal`** applied at every `read_parquet()` /
  `glob()` site that interpolates a computed path. Closes the
  injection surface a partially-validated path could open through
  DuckDB's `read_parquet()` glob expansion.
- **Caddy container drops privileges** — `caddy/Dockerfile` adds
  `USER caddy` (the base image ships the user). Caddy is the only
  externally-facing socket and binds nothing below port 1024, so
  there's no reason to keep `root` in the runtime.

### Frontend

- **RSC/CSR boundary** documented in `app/_routing.md`. The
  hidden-Plotly + hidden-MapLibre + `setTimeout` warm-up hacks are
  dropped; replaced with `modulepreload` + the styledata-event swap
  pattern.
- **16 frontend files > 500 LOC split.** `ProvisionWizard.tsx`
  (3,582 LOC) → `wizard/steps/*` + `state.ts` + `api.ts`;
  `app/logs/page.tsx` (2,136 LOC) → `_sections/*` + `_state.ts`.
  `app/admin`, `app/dashboard`, `app/alerts`, `app/security`, etc.
  all post-split < 500. **No frontend file > 499 LOC.**
- **Live Query Monitor** — live-first sort, peak-memory column,
  keyboard shortcuts, URL-persisted filters, per-run inline expand
  for ×N cron-grouped rows, ≥ 30 s stuck-query pulse, copy-SQL,
  sound notification removed.
- **Operations Overview cards** on the admin landing page surface
  ingest gap + live query activity + slow-query count so the things
  operators actually care about don't live three clicks deep.
  Tone-coded (default → attention → warning → critical) so a
  sustained_loss event jumps out.
- **Stable React keys on dynamic lists** — `DebugPanel`, `CronLiveLog`,
  the network metro leaderboard, the query toolbar, and the
  custom-field drawer now key off a stable identity instead of array
  index. `useSSE` attaches a monotonic `_id` to each line so
  append-only feeds (cron progress, query streams) keep stable keys
  across re-renders.
- **Accessibility pass** — `FieldGroups` and `FileBrowser` disclosure
  widgets are real `<button>`s with `aria-expanded`; `SSEModal` uses
  the base-ui `Dialog` render prop instead of a non-keyboard `<div>`
  wrapper; per-row "view audit logs" buttons carry an `aria-label`
  that includes the row's email so screen readers don't read 20
  identical "View" buttons in a row.
- **`fetchWithTimeout` helper** (30 s default; heartbeat tightens to
  10 s) applied to `share-login`, `acknowledge`, and
  `useAnalystHeartbeat` so a hung request surfaces as an error
  instead of an infinite spinner.

### Quality gates

- **Backend coverage gate `--cov-fail-under` 78 → 85** (final actual
  85.05 %). Per-module test waves cover every cleanup-touched module
  + the post-split `rollups/` and `admin/` packages.
- **Frontend coverage gate `coverage.thresholds.lines` 44 → 58**
  (final actual 61.66 %).
- **`tool.mypy.overrides` `ignore_errors` list: 36 modules → 0.**
  Every backend module type-checks under default settings. Three real
  bugs surfaced + fixed during the burndown
  (`repositories/network.py:260` was passing the DuckDB connection
  where `get_asn_names` expected `service_id`;
  `routers/share_auth.py:125,203` had an `iso_z_now() and 24*60*60`
  cookie `max_age` expression where the `and` was a no-op leftover;
  `routers/admin.py` shadowed loop variable that defeated narrowing).
- **mypy per-module strict block: 19 modules opted in**
  (`disallow_untyped_defs` + `disallow_incomplete_defs` +
  `check_untyped_defs` + `warn_return_any` + `warn_unused_ignores`).
  Live-query-monitor surface + every module the v2.0 waves added
  tests for. Full mypy: 221 source files clean.
- **Load-harness CI step**: `scripts/emit_perf_latest.py` runs a
  100K-row synthetic DuckDB workload (~2 s wall); `scripts/perf_gate.sh`
  fails on > 50 % regression vs `tests/perf/baseline.json`. Production
  targets (≤ 2,800 / ≤ 1,900 ms on 36 M rows) documented in
  `baseline.json` `production_targets_comment` and validated by the
  manual `scripts/dev/loadtest_probe.sh`, not the CI gate (GH Actions
  runner variance is too high).

### Operations, portability

- **VM-agnostic deploy runbooks** at
  [`docs/deploy/`](docs/deploy/): `aws_ec2.md`, `azure_vm.md`,
  `gce.md`, `generic_linux.md`. Storage stays Fastly Object Storage
  (S3-compatible API; boto3 keeps working). GCE-specific wording in
  comments renamed to "cloud" / "VM" (the link-local
  169.254.169.254 metadata IP is identical on AWS + GCE; the SSRF
  gate works on both).
- **`scripts/refresh_fastly_cidrs.py`** pulls
  `api.fastly.com/public-ip-list` and rewrites the Caddy
  `@from_fastly` block. Manual or cron-scheduled.

### Breaking

- **Composite-endpoint cutover.** The granular per-card endpoints
  (`/api/dashboard/aggregates`, `/api/dashboard/raw`,
  `/api/dashboard/top_n`, etc.) are **deleted**; callers must use the
  composite (`/api/dashboard/bundle`). External integrators were
  notified 24–48 h ahead.
- **`AnalyticsDeps`** alias for `RequestContext` is removed.
- **`is_cached` / `_is_cached`** alias on `BaseResponse` is removed
  (`is_cached` is the canonical name).
- **SSH-to-localhost.run analyst sharing** is removed. The laptop-
  admin tunnel use case is no longer supported; production has always
  been direct-mode against the Fastly+Caddy public URL.

[2.0.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v2.0.0

## [1.2.0] - 2026-06-09

Dashboard performance overhaul plus capability-focused security hardening. Cold and warm dashboard loads drop from seconds to sub-second on large services; sustained concurrent load no longer wedges the backend. Read-path I/O is structurally cut by a per-service DuckDB connection pool, a per-minute time-series rollup bundle, size-capped bin-packing local compaction, composite endpoints that collapse multi-card admin pages into one request, and a frontend pre-warm / hover-prefetch pattern that makes navigation feel instant. Security hardening tightens cross-tenant boundaries, closes a ContextVar propagation hole in the s3fs proxy hook, removes a secret-in-URL leak on downloads, and adds strict validation across the destructive-op surface.

### Performance

Structural:

- **Per-minute time-series rollup bundle** (`backend/core/rollups.py`) precomputes a hour-bundled per-minute aggregate for the dashboard chart, eliminating the wide Iceberg scan on chart render. Generated alongside the existing Top-N rollups.
- **Per-day compaction tier for rollups** — closed days are compacted into per-day parquet files; the reader prefers the per-day file and falls back to hourly only for the current day, cutting file-handle pressure on long-running services.
- **Size-capped bin-packing local compaction** ([backend/core/local_compaction.py](backend/core/local_compaction.py)) replaces single-file daily/weekly rollups with sequential bin-packing capped at `_MAX_PARTITION_BYTES` (default 256 MB). Hourly partitions older than 7 days bin-pack into daily files; daily files older than 30 days bin-pack into weekly files. DuckDB query parallelism is preserved on multi-month services where the prior single-file approach degraded to scan-of-one-huge-file.
- **DuckDB connection-pool tuning knobs** — `DUCKDB_POOL_CONN_MEMORY_LIMIT` and `DUCKDB_POOL_CONN_THREADS` env vars cap per-pool-connection memory and thread count so 8 concurrent queries don't oversubscribe physical cores or balloon RSS. Pool view-binding moved outside the `Condition` lock to eliminate a deadlock under stale-Iceberg-snapshot reload.
- **Composite read endpoints** collapse multi-card mounts into single requests:
  - `POST /api/scoring/dashboard` (8 per-card requests → 1)
  - `GET /api/scoring/analytics` and `GET /api/scoring/config`
  - `GET /api/network-health` now includes shielding analysis
  - `POST /api/origin/aggregates` (new) batches the origin page's per-card queries
  Per-card endpoints stay mounted for back-compat; the frontend opts into composite where it makes sense.
- **Parquet ingest sort key** changed to `(timestamp, ip)` so sessions queries can stream-merge on `ip` instead of materialising a temp table — ~2× speedup on sessions dashboards.
- **`ingested_files.file_date` column + `(source_name, file_date)` index** added via numbered SQLite migration. The log-accounting fast path uses the index to bucket by day without scanning every row; `metadata_db.get_node_count_avg` and `get_log_accounting_counts` split on it.
- **Iceberg commit hygiene** — buffer files are tombstoned and removed on the next pass instead of unlinked inline at commit time, removing a commit-path stall. `optimize_table` adds `union_by_name` + retry-on-CAS-conflict to silence the nightly schema-evolution warning.
- **Bootstrap stale-while-revalidate** — `/api/bootstrap` returns cached dir-stats immediately and refreshes in the background; views are folded into the response so the admin page doesn't issue a follow-up.

Tuning:

- Dashboard live-hour TEMP TABLE shared across CTEs; Python-side bot match + memoised `ngwaf_top` cut DuckDB round-trips.
- Insights coalesce four city/region/country queries into one and four URL-keyed insights into one CTE (Option C pattern).
- Sessions split the monolithic CTE into measurable stages and eliminate the temp-table materialisation on the hot path.
- Origin summary combines two sequential scans into one via `GROUPING SETS`.
- Cron-runs `since_id` delta-poll param + frontend wiring on `/logs recentCrons` so the page only fetches new events.
- Admin usage-log visibility-gates its 30s tick and rewrites the latest-per-task SQL to skip the full join.
- Admin shielding banner endpoint trimmed; share-status `staleTime` tightened.
- Bot-source cache: 60s TTL on the recursive cache-dir `scandir` (was 200–1500 ms per `/api/bootstrap`).
- React-Query: skip 4xx retries; hooks lifted out of insights / ReportLayout render-props so each page mount re-uses one query instance instead of re-mounting on every parent render.

Frontend:

- **`starlette-compress` replaces `GZipMiddleware`** — backend now negotiates `br` / `zstd` / `gzip` (was gzip-only). Modern browsers get brotli; rendered-text payloads drop ~25 % on the wire.
- **Keep-alive on Next.js http/undici global agents** so the proxy reuses TCP connections to the FastAPI backend instead of new-handshake-per-request.
- **Pre-warm + lazy-mount pattern** — plotly + maplibre-gl + `world.geojson` are pre-warmed on `AppLayout` mount via hidden one-point charts; the visible chart hydrates from the warm module cache instead of triggering a fresh import on first render. `LazyMount` + `PlotlyChart` start `visible=false` to avoid the hydration-mismatch warning that came with the prior eager-mount pattern.
- **Hover-prefetch sidebar links** so the destination's data warms before the click commits.
- **Per-insight skeleton cards on first paint**; full skeleton rendered from `CARD_CATEGORIES` on the dashboard.
- **Modulepreload for the plotly chunk** via a build-time-generated preload manifest (`scripts/build-preload-manifest.mjs` + `lib/preload-manifest.ts`); restores plotly's preload without re-introducing the nav-lag the first attempt caused.
- **Drop `force-dynamic`** on routes that don't need it; root layout opts out of build-time SSG so the preload manifest is read at request time.
- **`/geo/*` static assets cached aggressively**; `PlotlyChart` dynamic-import on `/network`.
- **`SystemHealthCard` polling moved to 1 s** for live attack/load feedback now that the endpoint is cheap.
- **`useNowMs` reuse** — multiple visible-tick components (countdowns, "X seconds ago") share one interval.
- **Map style-data listener** replaces a 100 ms `setTimeout` poll.

### Reliability

- **Multi-worker login loop fixed** — `tunnel.py` now rehydrates a share session on-demand from SQLite when an in-memory cache miss happens on a different uvicorn worker. Previously, login on worker A would loop because worker B couldn't see the freshly-minted session.
- **DuckDB lock conflict resolved** between the connection pool and cron writes — `get_connection` forces `read_only=False` so pool readers and cron writers no longer trip DuckDB's "different configuration" error on the same file.
- **Stale-view self-heal** — `QueryRunner` clears `_view_cache` before the `force=True` rebuild on the post-empty recovery path so the next query doesn't see the stale schema.
- **Iceberg s3fs proxy hook** falls back to the process-global source so the hook always registers, even when the ContextVar is empty (e.g. cold-start LIST before any `_get_catalog` has fired).
- **Top-N current-hour merge** — a silent `ImportError` was dropping the current-hour merge; restored with an explicit fail-loud import.
- **Rollup compaction** — `run_id` threaded through the error branch and the compaction step now uses an in-memory DuckDB so a corrupted on-disk catalog can't wedge the cron.
- **Dashboard response cache** — write to `is_cached` (not the aliased `_is_cached`) so Pydantic doesn't drop the flag on serialise.
- **Dashboard cache hit rate** — disabled the 30 s response-level cache that was masking the rollup wins for fast-changing queries.
- **Usage-log rollup drift** — reconcile cycle changed from DELETE+INSERT to UPSERT so concurrent flushes can't lose rows.
- **Botnet insight investigate link** filters only the queried column, not all of them.
- **`expire_snapshots`** updated for pyiceberg 0.11.1 API and now emits `cron_runs` telemetry.
- **Proxy compatibility** — switched from `middleware.ts` to `proxy.ts` for Next.js 16; restored the Caddy-marker middleware that the upgrade broke.
- **Telemetry response middleware backstop** ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py)) auto-injects `_debug_queries` / `_debug_calls` / `_is_cached` into JSON-dict responses that bypassed `BaseResponse.with_telemetry`, so newly-added endpoints don't silently blank the Debug Panel.

### Security

Capability-focused hardening across the backend and frontend trust boundaries.

- **Cross-tenant ContextVar leak in the s3fs proxy hook** closed. PyIceberg writes parquet via a `ThreadPoolExecutor`; ContextVars don't propagate to executor workers by default, so the prior fix used an endpoint-keyed global registry that was vulnerable to overwrite when two tenants shared an endpoint URL. Replaced with a global `ThreadPoolExecutor.submit` monkeypatch that wraps the callable in `contextvars.copy_context()` — matches asyncio's `loop.run_in_executor` semantics. Documented in [MONKEYPATCHES.md](MONKEYPATCHES.md) §6.
- **Path-param service-scope desync** — analyst sessions could supply a `service_id` path param that didn't match their session scope on a handful of mutation endpoints. Centralised the check via a router-utils helper invoked on every scoped route.
- **Secret-in-URL leak on downloads** — the download endpoint previously embedded the shared CDN secret in the redirect URL where it could land in browser history / referrer headers. Switched to a signed short-lived bearer that's stripped before the redirect.
- **Strict input validation** on the destructive-op surface — provision teardown, NGWAF workspace mutations, scoring threshold + enforce-status-code + recv-exclusion-regex changes — runs through length caps, character allowlists, and (where applicable) `falco` static analysis before any VCL ships.
- **CSRF gates** — moved GET→POST on `logging-settings/update` and sibling state-changing endpoints that were addressable via GET.
- **Authorisation tightening** — share-admin endpoints reject the Caddy-marker header from non-Caddy paths; `claim_token` path consolidated under a single atomic UPDATE so concurrent claims can't both succeed.
- **Cross-tenant cache audit** — re-verified that every per-tenant cache key includes `service_id`; closed two missing entries on insights and origin paths.
- **Thread leak fix** — the share-login flow was leaking a daemon thread per failed login on multi-worker setups; the new on-demand SQLite rehydration replaces the thread entirely.
- **Terms-of-service bypass** — share-login `/acknowledge` now fetches the active TOS version and refuses acknowledgement of a stale one; frontend was sending a hardcoded version.
- **Telemetry-proxy diagnostics** for silent 400s (`Missing X-Fos-Target`) and unclassified `list_objects_v2` calls; preserve `Content-Type` so downstream compression always fires; preserve multi-valued response headers.

### Tests

- 3500+ backend tests (+450).
- 290+ frontend vitest tests (+25).
- New coverage: `tests/core/test_duckdb_pool.py`, `test_local_compaction.py`, `test_rollups_compaction.py`, `test_rollups_hour_bundling.py`, `test_iceberg_helpers.py`, `tests/services/test_service_manager.py`, `tests/utils/test_sql_validator.py`, `test_telemetry_response_middleware.py`, `test_router_utils.py`, `test_state_sync.py`, `test_terraform_gen.py`, plus router coverage for the new composite endpoints and the destructive-op-auth surface.
- `make ci` green: lint + format + mypy + pytest + vcl-test + verify-deps + typecheck-frontend + test-frontend + osv + secret-scan.

### Infrastructure

- **Synthetic load generator** ([scripts/loadtest_generator.py](scripts/loadtest_generator.py)) and **read-path probe** ([scripts/dev/loadtest_probe.sh](scripts/dev/loadtest_probe.sh)) for reproducible perf measurement against local Parquet+Iceberg.
- **Two-pass next build** in the frontend Dockerfile so SSG sees the correct plotly chunk hashes; preload-manifest scanner runs after `next build` to capture them.

### Documentation

- `AGENTS.md` — added Key Systems entries for the DuckDB connection pool, the hourly Top-N rollup pipeline, and the response telemetry middleware. Updated the local-compaction section to reflect the bin-packing tiers.
- `MONKEYPATCHES.md` — documents the new `ThreadPoolExecutor.submit` patch.

[1.2.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v1.2.0

## [1.1.0] - 2026-06-03

Edge session scoring. Every request is classified in real-time at the edge by a Fastly Compute service that runs an L1 (cookie compliance + timing rules) + L2 (PageRank-trained transition matrix) scorer, returning a combined 0-100 score that lands in DuckDB for analyst review. Operators can label sessions, watch live ROC-AUC, retrain the matrix, roll back to a prior matrix, rotate the AES cookie key, and push a hard enforcement threshold that rejects flagged requests at the edge with an operator-chosen HTTP status code (default 429).

### Highlights

- **Edge scoring** — Fastly Compute scorer + 6-snippet VCL preflight pattern (recv/pass/fetch/deliver/miss/enforce), AES-GCM-encrypted session cookie carrying rotating sid + transition state, `fastly.ddos_detected` gate so Compute is bypassed under L7 attack.
- **Admin UI** at `/admin/session-scoring` — StatusPanel with live AUC against accumulated labels, ScoringHealthCard with fire rate / score distribution / top reasons / matrix-staleness alert, ThresholdSlider with counterfactual flag/pass preview + precision/recall + commit-threshold persistence, RocPrCurves with ROC + Precision-Recall plots, TopFlaggedTable + LabelsTab with click-to-view-events per sid, RetrainButton (DuckDB traces → train.py → publish matrix to FOS), SinceHoursPicker driving all six cards on one shared time window.
- **Labels CRUD** — POST/PATCH/DELETE per-sid labels (good/bad/neutral) feed `evaluate_from_persisted_scores` to compute live ROC-AUC. Min-samples gate (≥3 per class) prevents noisy display.
- **ROC + PR curves** + per-reason AUC breakdown (split by L1/L2 rule: cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Composite `/scoring/dashboard`** endpoint collapses the 8 per-card requests into one in-flight-collapsed payload; the existing per-card endpoints stay mounted for back-compat.
- **`edge_score_reason` virtual field** — CSV-split via DuckDB `unnest(string_split(...))`, top-N cards + click-to-filter same as NGWAF signals.
- **FOS matrix persistence** — `enable_scoring` publishes the trained matrix to FOS; backend auto-fetches on startup (no more per-host scp).
- **Matrix version history + rollback** — every publish snapshots the prior matrix to `iceberg/meta/scoring_matrix_history/{version}.json`; new `/scoring/matrix-versions` lists them and `/scoring/matrix-versions/{v}/restore?confirm=true` copies a historical matrix back. AUC reflects the rollback immediately; Wasm at edge keeps the embedded matrix until `deploy_wasm.sh` re-runs (deploy_hint surfaced).
- **Threshold enforcement (live blocking)** — operator commits a threshold, scorer reads it from `scoring_config` ConfigStore, emits `X-Edge-Score-Enforce: 1` when score≥threshold, the new `Session Scoring - Enforce` VCL snippet rejects those requests on the post-scoring restart. Effective at the edge within seconds. Confirm-dialog-gated PUT endpoint + LIVE warning chip in the slider UI. The response code defaults to 429 (Too Many Requests) and is operator-overridable per-service via a new `Enforce response code` selector (403 / 429 / 451 / 503; backend accepts any 4xx/5xx) — picks land via a focused `update_enforce_status_code` orchestrator that swaps only the enforce snippet (~5–10s end-to-end vs. the full enable_scoring flow). Audit-logged as `scoring_enforce_status_code_changed`.
- **URL exclusion regex override** — operator-tunable per-service regex for "which URLs bypass the scorer". Defaults to the built-in static-asset extension list; the new `ExcludeRegexCard` on the Session Scoring page accepts a custom regex (e.g. exclude `/healthz`, exclude entire path prefixes, scope scoring to specific traffic). The PUT endpoint validates input through three layers before any VCL ships: (1) input policy — length cap, no quote / control chars, must compile under Python's `re`; (2) [falco](https://github.com/ysugimoto/falco) static analysis on the assembled recv snippet (catches regex+VCL composition errors that slip past Python's compiler); (3) Fastly's own VCL compiler at activate time. A focused `update_recv_exclusion_regex` orchestrator clones the active version, swaps only the recv snippet, and activates — ~5–15s end-to-end vs. the full enable_scoring flow. Confirm-dialog-gated. Audit-logged as `scoring_exclude_regex_changed`. Falco shipped in the backend Docker image; production sets `SCORING_REQUIRE_FALCO=1` so a missing binary fails closed instead of degrading to input-policy-only.
- **AES key rotation** — `POST /scoring/rotate-key` mints a fresh 32-byte key, moves the prior to `previous_key_hex` (grace slot — Rust cookie codec falls back to it so in-flight cookies keep decoding through one rotation cycle).
- **Cookie lifecycle bounds** — `SESSION_IDLE_EXPIRE_S` (30 min) + `SESSION_HARD_CAP_S` (24h) in the Rust scorer mint a fresh sid when either threshold is exceeded. Stolen cookies can't replay beyond their window; long-running sessions stop biasing the L1 variance estimator.
- **Per-reason AUC breakdown UI** — `PerReasonAucCard` renders AUC split by which L1/L2 rule fired (cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Operator audit log** — new `scoring_audit` table + `/scoring/audit` endpoint records every scoring_enabled, scoring_disabled, threshold_committed/cleared/enforced, matrix_retrained/restored, key_rotated event with actor + timestamp + details. Per-host, never mirrored via state_sync.

### Reliability

- **Cron-progress reliability** — `end_progress` auto-emits `done` when the last event isn't terminal; `list_active_runs` triple-guards (last-event filter + 5-min staleness + DB-status cross-check via `get_cron_run_status`); `reap_zombie_runs` called from every cron-tick cleanup. Fixed a production incident where 382 stale "sync" entries piled up on the System Health card.
- **state_sync merge guards** — `import_admin_state` no longer overwrites scoring `custom_fields` with stale FOS payloads (root cause of a production data-loss incident); sibling fixes in `cli.handle_update_logs`, `provision.write_service_config`, and `api_service_log_fields_set` close every "remote-overwrites-code-managed-state" path.
- **Defense-in-depth** — `enable_scoring` rollback + `disable_scoring` final-save reload cfg right before writing to close the 30-120s race window where concurrent writers got clobbered.
- **Per-key in-flight collapse** in `_cached` so the dashboard's 8-card mount no longer queues queries behind one global lock.

### Performance

Structural:

- **DuckDB connection pool** (`backend/core/duckdb_pool.py`) replaces per-request connection setup; eliminates the per-request DuckDB initialisation cost on hot paths.
- **Hourly Top-N rollup pipeline** (`backend/core/rollups.py` + `scripts/backfill_rollups.py`) precomputes the dashboard's most-asked aggregates; cold-load dashboard scans drop from seconds to tens of ms.
- **Bounded cache primitive** (`backend/utils/bounded_cache.py`, 13-test `tests/utils/test_bounded_cache.py`) replaces several previously-unbounded dict caches across the request path (also referenced under Security → `_StaticAssetLimiter` and the analytics cache in `session_scoring._cached`).

Tuning:

- `security/top-bots` consolidated UA + NGWAF onto one temp table (was 2 independent Iceberg scans per dashboard mount).
- `dashboard/raw` uses `get_source_extent` for cached steady-state extent.
- `usage/prefill` cached-status fast path skips DuckDB hop when the sync cron has populated it.
- `get_enriched_services` 60s TTL cache on the recursive cache-dir `scandir` (was 200-1500ms per `/api/bootstrap`).
- `loading.tsx` Suspense skeletons + dynamic imports (LabelsTab, ChoroplethMap) cut admin-page click lag.

### Cleanup

- Dropped dead `@daypicker/react` dep + dead `frontend/components/ui/calendar.tsx`.
- Collapsed 7-site `cleanup_progress + reap` boilerplate into `cleanup_progress_and_reap()` helper.
- Refactored `security.py`'s ad-hoc temp-table to use the existing `QueryRunner.temp_table()` context manager.
- Narrowed `get_cron_run_status` exception scope to `sqlite3.Error` with DEBUG log so future triage isn't flying blind.

### Security

Capability-focused hardening across the FastAPI backend, Fastly VCL, Next.js frontend, and Rust scorer. All changes deployed and verified.

- **Trust-boundary normalisation**:
  - uvicorn runs with `--proxy-headers --forwarded-allow-ips=127.0.0.1` so `request.client.host` is the real client IP via Caddy's authoritative XFF rewrite.
  - `is_request_remote()` reads `request.client.host` instead of the forgeable Host header; in-app leftmost-XFF parsing is gone.
  - Caddyfile gates `Fastly-Client-IP → X-Forwarded-For` rewrite on `remote_ip` matching Fastly edge ranges. Startup assertion on `TRUSTED_PROXY_IPS` / `UVICORN_FORWARDED_ALLOW_IPS` + integration test prevent silent regression.
  - Next.js `/admin` middleware gates on the Caddy-injected `X-Proxied-By-Caddy: true` marker instead of the forgeable Host header.
- **Destructive-op auth**:
  - `/api/provision/teardown` validates a caller-supplied Fastly token via `/tokens/self` for the `global` scope before any destructive op; never falls back to server-stored credentials. Frontend TeardownDialog prompts admin for the token.
  - `/api/provision/ngwaf-workspaces` token-gated (constant-time stored-key match OR validated `global`-scope token); NGWAF workspace mutation enforces analyst-session scope.
- **DuckDB user-SQL safety**:
  - New `backend/utils/sql_validator.py` enforces a statement-type whitelist + recursive parse-tree walker with catalog blocklist (`duckdb_*` / `pg_*` prefixes, `information_schema` / `pg_catalog` / `system` schemas, non-`main` catalogs) + function denylist (`read_csv` / `read_parquet` / `iceberg_scan` / `glob` / `lsdir` / `getenv` / `current_setting` / `duckdb_secrets` / postgres / sqlite / mysql scanners) + fail-closed parse + audit logging + perf budget. Replaces a regex-based blocklist that missed `read_csv_auto`, `information_schema`, `duckdb_secrets`, `INSTALL/LOAD`, and `getenv`.
  - `escape_sql_literal` helper applied at four ingest call sites; characterisation tests cover the PoC payload + multi-byte UTF-8 + backslash + empty + long-with-many-quotes.
  - `time_range` validated via `dateutil.isoparse` before SQL interpolation.
  - `get_con` / `get_meta_con` dropped the auto-query-param `read_only` flag.
- **VCL header & cache discipline**:
  - `vcl_recv` preamble unsets every internal `x-of-*` / `x-fos-edge-data` / `x-is-cluster-fetch` / `X-Edge-*` header on the inbound request.
  - Origin-metric VCL fields: numeric regex gates + `json.escape` on string values (log-injection).
  - VCL ua/referer keeps its `substr` cap.
  - Fastly `vcl_hash` now keys on the full `req.url` (path + query), not just `req.url.path` — closes cross-query cache poisoning. Auth `key` querystring is already stripped earlier so no secrets leak into cache keys.
- **Cross-tenant scope enforcement**:
  - `/api/alerts/*` and `/api/views/*` enforce analyst-session scope on every read and mutation; pre-flight scope check on PATCH / DELETE via new `get_alert_by_id` / `get_view_by_id` helpers so unauthorised mutations never land.
  - `/api/sources`, `/api/log-fields/catalog`, NGWAF workspace listing — analyst-scope filtering.
  - Cache-layer audit confirmed every per-tenant cache (`session_scoring._cached`, iceberg, bot_sources) includes `service_id` in the key.
- **Path-traversal cages**:
  - `/api/download` path traversal: `realpath` + `commonpath` cage.
  - Cache cleanup rejects bucket separators + `realpath` cage.
  - `service_id` alphanumeric/dash/underscore validation in path helpers.
- **Secret & data hygiene**:
  - `claim_token` TOCTOU → atomic UPDATE with rowcount check.
  - `share_db` quarantine narrowed to actual SQLite corruption signatures (was wiping the DB on transient `OperationalError`).
  - Email-enumeration timing equalised via dummy scrypt on miss.
  - `validate_session` re-syncs `pii_policy` / window / `service_ids` on every call so admin permission edits take effect immediately.
  - `_StaticAssetLimiter` bounded at 10 k tracked IPs.
  - `logging-settings/update` moved GET → POST/PATCH (CSRF).
  - `query_errors` decorator logs traceback server-side, never in the response body; sweep fixture asserts no `trace` key leaks from any route.
- **SSH host-key pinning**: `configs/ssh_known_hosts` pinned, source-controlled, and gitignore-excepted; tunnel manager refuses to start when the file is missing (fail-safe; no TOFU fallback).
- **Scorer signal tightening**: Python + Rust parity — `L1_SCORE_COOKIE_TAMPERED = 100` (was capped at 75 with missing/expired); `L1_ROBOTIC_DWELL_LOW_S 0.5 → 0.20` (closes the 0.20s–0.50s robotic-bot threshold gap). Tracked follow-up sliding-window mean (needs cookie-schema v3) — partial mitigations via `SESSION_IDLE_EXPIRE_S=30 min` + `SESSION_HARD_CAP_S=24h` + session-max scoring bound the practical attack window.

### Tests

- 3070 backend tests
- 65 scorer Rust tests (+8)
- 265 frontend vitest tests (+13)
- `make ci` green: lint + format + mypy + pytest + vcl-test + verify-deps + typecheck-frontend + test-frontend + osv.

### Infrastructure

- Backend Docker image: `python:3.12-slim-bullseye` → `python:3.12-slim-bookworm` (cuts CVE-laden Debian 11 base; remaining 13 high CVEs are deep-dependency / OpenSSL CVEs every major Python base inherits). Frontend image's api-schema stage bumped to match.
- Backend image now ships [`falco`](https://github.com/ysugimoto/falco) v2.3.0 (Fastly VCL static analyser) — required by the scoring-recv-snippet validator.
- **Secret scanning** — [`gitleaks`](https://github.com/gitleaks/gitleaks) v8.30.1 wired in three places: `.pre-commit-config.yaml` (blocks accidentally-staged credentials at commit time), `make secret-scan` Makefile target chained into `make ci`, and a dedicated step in `.github/workflows/ci.yml` (fails the build on any non-allowlisted finding). Configuration in `.gitleaks.toml` extends the built-in ruleset and adds path allowlists for tracked test fixtures, Rust lockfile checksums, the public SSH host key, and (for working-tree-only scans) the gitignored real-config / `.next/` / `data/system/` directories. Verified clean against the full branch history. Policy + suppression playbook documented in **AGENTS.md** §Secrets.
- **CDN cache-key hardening** — `backend/core/fastly/utils.py` `vcl_recv` now runs `querystring.filter_except` to drop all non-S3-API query parameters (caller-injected tracking params, marketing UTMs, session IDs) BEFORE the cache lookup, followed by `querystring.sort` to canonicalise the remaining param order. Composes with the `vcl_hash` fix: untrusted params can no longer fracture the cache OR leak the auth `key` into the cache key.
- Dependency freshness sweep on all four ecosystems:
  - **Python:** `aiohttp 3.13.5 → 3.14.0`, `cfn-lint 1.51.2 → 1.51.4`, `distlib 0.4.0 → 0.4.1`, `filelock 3.29.0 → 3.29.1`, `idna 3.17 → 3.18`, `joserfc 1.6.8 → 1.7.0`.
  - **Frontend:** `@tanstack/react-query 5.100.14 → 5.101.0` (+ devtools), `@types/react 19.2.15 → 19.2.16`, `react/react-dom` resolved to `19.2.7` via the existing `^19.2.5` range. `next` + `eslint-config-next` stay pinned at `16.2.6`.
  - **Rust:** `bitflags 2.11.1 → 2.12.1`.
  - **Deferred (major bumps reserved for 1.2):** TypeScript 5.9 → 6.0 (compiler-API breaking changes); Fastly Rust SDK 0.11 → 0.12 (Compute@Edge API changes); jsdom / eslint / vitest where we're already ahead of the npm "latest" tag.

### Known limitations

- Rate limiting at the edge is NOT included. The DDoS gate (`fastly.ddos_detected`) handles attack-scale traffic by bypassing Compute; sustained-low-rate abuse is left to the operator's existing WAF/NGWAF policies. A future rate-limiting feature is tracked separately.
- When a matrix is rolled back via the UI, the edge Wasm continues to use its embedded matrix until `scripts/scoring/deploy_wasm.sh` re-runs. The Restore endpoint returns a `deploy_hint` with the exact command. See `docs/session_scoring_runbook.md`.

[1.1.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v1.1.0

## [1.0.0] - 2026-06-01

Initial public release. Self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to Fastly Object Storage.

### Highlights

- **Apache Iceberg data lake** in Fastly Object Storage — ACID-compliant log storage, safe for concurrent readers and writers, with automated compaction and snapshot expiration.
- **Automated provisioning** — guided wizard (and equivalent `backend/provision.py` CLI) creates the FOS bucket, scoped access key, CDN-fronting Fastly Delivery service, and the logging endpoint on your VCL service. Auto-rollback on failure.
- **Crash-safe ingestion** — buffered locally, atomically committed; interrupted imports never corrupt the table.
- **CDN-accelerated reads** — every FOS data read goes through a Fastly Delivery service for free egress and edge caching.
- **Multi-source support** — analyze logs from multiple Fastly services side by side, each with its own DuckDB engine and Iceberg table.
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations across every dimension, paginated raw-log viewer with click-to-filter.
- **Insights** — automated anomaly detection for error spikes, regional traffic surges, new IPs, WAF signal changes, cache efficiency collapses, and latency regressions.
- **Usage & Cost** — live storage breakdown, FOS Class A / B operation counts, period totals, and an interactive cost estimator pre-filled from your traffic stats.
- **Log-line accounting** — reconciles Fastly's authoritative `/stats/service/{id}` counter against locally-ingested rows bucket-by-bucket and surfaces sustained pipeline loss.
- **Configurable log fields** — thirteen built-in field groups (HTTP, network, geo, TLS, NGWAF, QUIC/HTTP3, origin metrics, etc.) plus arbitrary custom VCL fields with auto-generated Edge Data Capture snippets.
- **Alerts** — threshold-based, webhook-delivered, with optional comparison-period evaluation and per-status-code scope.
- **Two collaboration modes** — invite analysts to run an independent copy (durable JSON-config join with read-only FOS credentials), or share your running instance live via three sharing modes: SSH reverse tunnel via localhost.run, your own hostname, or your own public IP. Per-analyst passcode invites, optional IP allowlist, optional expiry, and instant single-invite or sever-all revoke. Per-mode trust-model trade-offs are documented in [SECURITY.md](SECURITY.md#live-dashboard-sharing--trust-model).
- **Field-size guard** — warns when your selected log fields approach Fastly's ~8 KB log-format limit.

See [docs/features.md](docs/features.md) for the full feature reference.

[1.0.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/1.0.0
