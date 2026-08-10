# Implementation Handoff: RUM-to-DuckDB & Apache Iceberg Migration

> **You are implementing this end to end.** Not a design review, not a subset,
> not a proof of concept. You will read the codebase, write the code, run the
> gates, deploy to the GCE production VM, and verify the result on the live SE
> demo site. You are done when RUM data is flowing through DuckDB + Iceberg in
> production and you have pasted the evidence.

---

## 0. Before you write a single line

### 0.1 Read these, completely, in this order

1. **[`local-docs/RUM_DUCKDB_ICEBERG_TRANSITION_PLAN.md`](./RUM_DUCKDB_ICEBERG_TRANSITION_PLAN.md)** — the authoritative spec. Every phase, every corrected file path, every resolved decision. This prompt is only its checklist; **where they differ, the plan wins.**
2. **[`AGENTS.md`](../AGENTS.md)** — end to end. Re-read the *Traps & Gotchas* section before every change. Most regressions in this repo are re-discoveries of a documented trap.
3. **[`CLAUDE.md`](../CLAUDE.md)** — commands, conventions, CI ratchets.
4. **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)** — system design.

Then read the actual code for every file the plan names. **Do not implement from
the plan's line numbers alone** — they were verified at `d44f8444`, and the
branch moves. Open the file, find the symbol, confirm the signature.

### 0.2 Load these skills when you reach the matching work

| Work | Skill |
|---|---|
| Any backend endpoint the frontend consumes | `add-analytics-endpoint-e2e` |
| Editing generated Fastly VCL / log fields (Phase 0) | `vcl-change-and-verify`, `falco` |
| Red or flaky backend tests | `backend-test-triage` |
| Before every commit/push | `infra-leak-sweep` |
| Deploying and verifying | `deploy-to-gce-and-verify` |
| Writing tests | `realistic-testing` |
| Any rollup work (only if Phase 3 baselines justify it) | `add-topn-rollup` |

### 0.3 The three ways this migration ships broken while looking green

Internalize these. Each has produced a "done" that was 90% wrong.

1. **`sync_data` not parameterized.** RUM commits to FOS successfully, the cron logs success, every test passes — and the query returns almost nothing, because the stitched view only ever sees un-committed buffer files. See plan Phase 1.6.
2. **`_align_to_schema` defaulting to the log schema.** `write_to_buffer` calls it with `target_schema=None` (`buffer.py:438`), which resolves the *log* schema. Every RUM column is silently written as null. Invisible until someone queries it. See plan Phase 1.4.
3. **Ripping out the `/rum-beacon` VCL.** The plan tells you to delete the *route handler*. It does **not** tell you to touch the edge. Read §0.4 before going near it.

### 0.4 How a beacon actually reaches FOS — read before touching anything RUM

1. The Faro tracker JS (`backend/provision/rum_assets.py:33`) POSTs to `/rum-beacon`.
2. At the edge, `vcl_recv` intercepts that path, copies fields into `x-fos-edge-data:*` headers, and returns **`error 611 "No Content"`** — a synthetic 204 (`backend/provision/declarative/generators.py:190-212`).
3. **The origin is never contacted.** There is no round-trip.
4. `rum_log_condition` (`req.url.path == "/rum-beacon"`) routes it to the RUM log endpoint → `.gz` in FOS. `log_analytics_condition` excludes it from CDN logs (`reconciler.py:728-775`).

**Therefore:** `POST /rum-beacon` in `backend/routers/rum.py:254` is **already dead code in production**. Deleting the route handler requires **zero VCL changes and zero tracker changes**. Do **not** remove the `vcl_recv` interception, the `error 611` synthetic, `rum_log_condition`, or the tracker's `/rum-beacon` path. Removing any of those kills RUM collection entirely.

---

## 1. Non-negotiable execution rules

1. **Work the phases in order (0 → 6).** Each phase has a validation block. Run it, read the output, fix what's red, and only then move on. Do not batch phases.
2. **Every design question is already answered** — plan §7. They are decisions, not options. If you think one is wrong, say so in one sentence and implement it anyway; do not silently substitute your own.
3. **Paths and buffers:**
   - Raw RUM `.gz`: `s3://{bucket}/{prefix}raw_rum/` (moved in Phase 0).
   - RUM buffer: `cache/{bucket}/buffer/{table_name}/`.
   - RUM mirror: `cache/{bucket}/data_{table_name}/`.
   - **Log paths `cache/{bucket}/buffer/` and `cache/{bucket}/data/` do not change.** Altering them orphans existing on-disk state.
   - Buffer ≠ mirror. The buffer is pre-commit, drained by `commit_buffer`, never tier-compacted. The mirror is committed data from `sync_data`, and is what `compact_local_partitions` operates on.
4. **Connection checkout is a source-dict change, not a new argument.** `get_connection` has no `service_id` and no `db_type` (`backend/core/duckdb.py:904`). Routers use `ctx.con` via `backend/deps.py:_ConnectionHolder` → `duckdb_pool.checkout_connection(source)` (`deps.py:128`). Build a RUM-variant source (`duckdb_path` → `.rum.duckdb`, `name` → `{name}::rum`); the suffix gives pool-key, view-cache and lock isolation for free.
5. **No stubs, no placeholders, no `TODO`.** Every file you write must be complete and functional. `rum_commit.py` is currently a placeholder — that is exactly what you are replacing, not a pattern to copy.
6. **No error suppression.** No `try/except: pass`, no `|| true`, no swallowing a failure to make a gate green. Trace to root cause. (The existing per-line parse-failure `warning` in `rum_ingest.py` is a deliberate ingest-robustness choice and may stay; new query-path code may not do this.)
7. **Clean up after yourself.** Delete any scratch script, fixture, or debug file you create. Strip every `print` / `console.log` / debugger before you call a phase done.
8. **Stay in scope.** Do not reformat, refactor, or "improve" code outside the change. Unrelated lines in modified files stay untouched.
9. **Commit by pathspec, never `git add -A`.** Other sessions may be working on this branch; uncommitted changes that aren't yours are not your concern. `frontend/openapi.json` and `frontend/types/api.generated.ts` may already be modified by someone else — leave them unless *your* change regenerated them.
10. **Report honestly.** If a test fails, paste the failure. If you skipped something, say so. Never claim committed / deployed / tested without having done it.

---

## 2. Phase checklist

Full task detail lives in the plan. This is the tracking list.

### Phase 0 — RUM teardown & prefix move
The SE demo service is the only deployment using RUM and its **RUM state is disposable**. **CDN request logs must survive** (plan §7, decision 13). Hard cutover, no dual-read.

- [ ] Tear down RUM: `disable_rum(..., remove_cloud_files=True)` (`backend/provision/rum_orchestrator_v2.py:560`) — deletes `raw/rum/` and strips the RUM VCL/log endpoint.
- [ ] Purge local RUM state: `rum_beacons` rows, `ingested_files WHERE file_name LIKE '%/raw/rum/%'`, and the `ingested_files_summary` rollup entry.
- [ ] Move the producer: `generators.py:525` → `{state.fos_prefix}/raw_rum/%Y/%m/%d/%H/rum_log_%M.json.gz`.
- [ ] Update consumers: `rum_ingest.py:54,139`; `routers/usage.py:496` (billing); `rum_orchestrator_v2.py:452,659`; `provision/orchestrator.py:760`; the three test files.
- [ ] **Only after confirming `raw/rum/` is empty**, remove the now-dead guards: `ingest.py:631` `exclude_prefix_subpath`, `metadata/ingest_log.py:244-251` self-heal, the `delete_fos_prefix` exclude.
- [ ] Re-enable RUM; confirm beacons land under `raw_rum/`.
- [ ] **Do not touch** the beacon interception, `error 611`, `rum_log_condition`, or the tracker path.

```bash
uv run pytest tests/backend/provision/declarative/test_generators.py \
              tests/utils/test_fos_setup.py tests/core/test_rum_ingest.py
```
Verify: zero keys under `{prefix}raw/rum/`, new keys under `{prefix}raw_rum/`, CDN log ingest unaffected, generated VCL still contains `error 611`.

### Phase 1 — Storage infrastructure & connection isolation
- [ ] **Migration `010`** (not 009 — slot 9 is taken). Add `table_name` to `ingested_files`; **rebuild `ingest_in_flight`** with PK `(buffer_filename, table_name)` — a bare `buffer_filename` PK (`metadata/base.py:295`) makes the vitals manifest overwrite the errors manifest, since the deterministic name hashes the source file list. Update `base.py`'s DDL to match. **No data backfill** (Phase 0 wiped it). **Do not drop `rum_beacons`** — `base.py:442-449` recreates it every open.
- [ ] Metadata: `table_name` on `record_in_flight`, `clear_in_flight`, **`list_in_flight`**, `insert_ingested_files`, `get_ingested_filenames`. Per-table `ingested_files_summary`. `_ingested_filenames_cache` keyed `(service_id, table_name)`.
- [ ] Pool: `rum_source_for(src)`; verify `warm_pool_*`, `reset_pool_for_service`, `get_all_stats`; fix `_safe_buffer_mtime` (`duckdb_pool.py:221`); add `.rum.duckdb` to `duckdb_recycle._sources_by_db_path()` (`:105`).
- [ ] Iceberg core: `_table_identifier`, `_buffer_dir`, the schema getters, **`_align_to_schema` passed explicitly**, all of `buffer.py`'s table-blind helpers, `update_iceberg_view` + its hard-coded `table_name = 'logs'` catalog lookup (`view.py:754`), `_init_iceberg_table_locked`'s partition spec.
- [ ] `execute_with_stale_view_retry`: keyword-only `*, table_name="logs"` — it forwards `**kwargs` **to `fn`** (`view.py:181`), so a bare kwarg raises `TypeError`.
- [ ] View naming: RUM views are literally `client_vitals` / `client_errors`; do **not** run them through `_safe_table_name`.
- [ ] **`sync_data` (`sync.py:89`) + its destination dir.** Failure mode #1 in §0.3.
- [ ] New `backend/core/iceberg/rum_schema.py` — **not** `field_registry.py` (that's the VCL catalog).

```bash
uv run pytest tests/core/test_metadata_db_migrations.py tests/core/test_duckdb_pool.py \
              tests/core/test_duckdb_helpers.py tests/core/test_duckdb_recycle.py \
              tests/core/test_iceberg.py tests/core/test_iceberg_view_branches.py \
              tests/core/test_iceberg_helpers.py tests/core/test_iceberg_sync_branches.py
uv run mypy backend/    # fastest way to find every missed table_name call site
```
Must also assert: composite-key coexistence, `list_in_flight` filtering, `_align_to_schema` round-trips **non-null**, `sync_data` leaves `cache/{bucket}/data/` untouched.

### Phase 2 — Ingestion & PyIceberg buffering
- [ ] `rum_ingest.py`: list `raw_rum/`; **preserve the generator event contract** (`started` / `file_done` / `error` / `cleanup_done` / `done` — `started` is what fires `_reconcile_faro_bundle`, `rum_sync.py:186-224`); reuse the parameterized `_recover_in_flight` (`ingest.py:355`), don't reimplement; keep the pathname fallback and the cross-service filter; **fan out to both tables**; `float64` / `int32` / `timestamp("us", tz="UTC")`; reuse `_deterministic_buffer_name`; update the stale module docstring.
- [ ] `rum_commit.py`: **replace the placeholder.** `commit_buffer` both tables → `sync_data` both tables. Add `_BOTO3_CALLER_HINT = "rum_commit"`. Fix the status string `"done"` → `"success"`.
- [ ] ~~Cron registration~~ — **already done** (`scheduler.py:780-838`), intervals from `rum.sync_interval_seconds` / `rum.commit_interval_mins`, **not `log_period`**. Verify only.

```bash
uv run pytest tests/core/test_rum_ingest.py tests/cron/ tests/core/test_iceberg_buffer_branches.py
```
`tests/core/test_rum_ingest.py` asserts on SQLite rows today — **rewrite it** against Parquet output. Derive fixture keys from the producer, never the reader. Also verify by hand: crash-after-`write_to_buffer` recovery doesn't cross tables; a dual-output chunk leaves two in-flight rows; `_reconcile_faro_bundle` still fires.

### Phase 3 — Repositories & router cutover
- [ ] `repositories/rum.py`: fix `rum_cid` → `cid` (`:90`, `:117-123`); `get_error_rate_trend` denominator → `COUNT(DISTINCT req_id)`; drop stale stub docstrings; extract SQL to `_sql/`.
- [ ] `routers/rum.py`: replace `/rum/analytics` (`:349`) wholesale — do not adapt the 150-line Python aggregation; `/rum/beacon-health` (`:197`) → `MAX(timestamp)` + last successful `rum_sync`; `/rum/live-events` (`:905`) → DuckDB; **delete `POST /rum-beacon` (`:254`)** per §0.4; bind the RUM source via a dedicated dependency.
- [ ] `routers/bootstrap.py:420-428` → DuckDB, same change, or the nav badge zeroes.
- [ ] `make gen-types` if any response model changed.

```bash
uv run pytest tests/backend/routers/test_rum_analytics.py tests/repositories/
```
That test seeds `rum_beacons` directly — rewrite against Parquet fixtures. **There is no `tests/api/` directory.**

- [ ] **Record measured baselines.** Capture `section_timings` for every RUM endpoint at 1d/7d/30d on dev and **write the numbers into the transition plan**. There is no `<50ms` target. These numbers gate the rollup decision.

### Phase 4 — Lifecycle, compaction & admin
- [ ] `compact_local_partitions` (`:158`) — **not** `compact_local_buffer`, and it works on the **mirror**, not the buffer. Parameterize the root to `cache/{bucket}/data_{table_name}/`. Keep the active-hour guard and the publish lock. Use 128 MB, matching `optimize_table`'s default.
- [ ] **Rollups: DEFERRED.** Do not build them. Gate on Phase 3 baselines.
- [ ] `service_manager.py`: teardown purges `.rum.duckdb`, RUM buffer **and** mirror dirs, unregisters both crons.
- [ ] **Delete Data → two controls.** "Delete Log Data" scopes the FOS purge to `iceberg/default/logs/` and metadata `DELETE`s to `table_name='logs'`. "Delete RUM Data" scopes to the two RUM tables + `.rum.duckdb` + RUM buffer/mirror. Each gets an optional "also delete raw files" checkbox, **default off**, mirroring `delete_raw_logs` and its warning (`reset.py:179-183`). Both hold the per-service lock for the full run.
- [ ] Admin iceberg router: `table_name` as a validated `Literal`, not a bare `str`. `make gen-types` after.

```bash
uv run pytest tests/core/test_local_compaction.py tests/core/test_local_compaction_branches.py \
              tests/services/test_service_manager.py tests/routers/
make gen-types && make openapi-drift
```
Assert logs-side compaction is **byte-identical** to before, and that each Delete control leaves the other domain's FOS keys *and* metadata rows intact.

### Phase 5 — Query console & frontend
- [ ] `QueryRequest.dataset: Literal["logs","client_vitals","client_errors"] = "logs"`; bind the RUM source in `query.py` (context construction, not a `db_type=` arg).
- [ ] **Security — do not ship the dataset switch without this** (plan §6):
  - Add `cid` to `SESSION_ID_KEYS` (`backend/core/share_db/validation.py:35`).
  - `req_id` stays **visible** — over-masking is also a failure.
  - Worst Sessions groups on `sha256(salt || cid)`; salt = `secrets.token_hex(32)`, stored as `rum.cid_salt` in the service config, generated at RUM enable, **never logged, never returned by any API**.
  - Route the RUM datasets through `_rebind_table_to_window_view` (`repositories/query.py:184`) so `MAX_ANALYST_QUERY_SPAN` clamps them.
  - Add the probes from plan §6.4 to the security regression suite.
  - **Invoke the `security-rbac-expert` agent on this change before merge.** Mandatory — it touches PII masking and a pseudonymous identifier.
- [ ] Frontend dataset toggle; autocomplete from the RUM schema constants; `frontend/app/query/` bypasses `ReportShell` — audit callers.

```bash
uv run pytest tests/routers/test_query_router.py tests/repositories/test_query.py tests/security/
make gen-types && make openapi-drift && make security-regression
cd frontend && npm run test && npm run lint
```
Live analyst probes: `SELECT cid, 'x' || cid, split_part(cid,'-',1), CAST(cid AS BLOB) FROM client_vitals` → all `[redacted]`; over-span query clamped; `req_id` still visible; salt absent from every response.

### Phase 6 — Decommission the SQLite prototype
**Not in the same deploy as Phase 3.** Phases 0–5 keep the SQLite path alive and are revertible; this is the point of no return.
- [ ] Remove the `rum_beacons` DDL from `metadata/base.py:442-449` (table **and** index) — until it's gone, any `DROP` is undone on the next open.
- [ ] Migration `011`: `DROP TABLE IF EXISTS rum_beacons;`
- [ ] Delete `normalize_rum_beacons_timestamps` and every remaining reference in `routers/rum.py` and `routers/bootstrap.py`.
- [ ] `rg -n "rum_beacons" backend/ tests/ frontend/` returns nothing.
- [ ] Confirm `/rum-beacon` is gone from the router and **still present** in the VCL and tracker.

---

## 3. Pre-deploy gate

```bash
make verify     # == make ci && make e2e — mirrors every gating workflow
```

| Gate | Value | Note |
|---|---|---|
| Backend coverage | **86** | `Makefile:225`. The `95` figure applies only to the `backend/provision/declarative` sub-target (`Makefile:58`) |
| Security regression | **206** floor | `scripts/check_security_regression_count.sh:23` — never lower |
| ESLint | **824** ceiling | `scripts/check_eslint_count.sh:51` — drive down, never raise |
| Import contracts | `make import-contracts` | `core ↛ routers` — RUM cron/ingest must not import routers |
| OpenAPI drift | `make openapi-drift` | Regen runs in the **pre-push** hook; commit the drift or the push fails |

Red suite? Run `uv run pytest` first — system `pip` drifts off `uv.lock`; `uv sync` if needed. A red suite on a current checkout is a **real regression**, not pre-existing drift. Use the `backend-test-triage` skill.

Before pushing: run the **`infra-leak-sweep`** skill. `github.com/fastly/fastly-log-analytics` is **public** — no bucket names, Fastly service IDs, GCE hostnames, or `X-Edge-Shield-Auth` values in any tracked file. `local-docs/` is local-only.

---

## 4. Verify on dev, then deploy to GCE

Use the **`deploy-to-gce-and-verify`** skill as the runbook. Summary:

### 4.1 Dev first (required)
```bash
./run.sh --dev      # backend :18002, frontend :13002
curl -s http://127.0.0.1:18002/api/health?deep=1
```
- Admin UI: `http://localhost:13002/admin` — analyst flow: `http://localhost:13002/share-login`
- **Dev runs `FLA_DEV_NO_CRONS=1`** against the same FOS bucket as prod. Cron/ingest changes **cannot** be confirmed by waiting for a tick on dev — exercise them via the HTTP path, or accept they are prod-first. Never enable crons on dev against the shared bucket.
- Never use `:8000` to check dev — that is the **prod tunnel** (`run.sh:56` refuses it).

### 4.2 Deploy
```bash
gcloud compute ssh <INSTANCE> --zone=<ZONE> --command='~/restart.sh'
```
`restart.sh` does `git pull` + `docker compose -f docker-compose.prod.yml --build` + healthcheck. If you force-pushed, pre-flight with `git fetch && git reset --hard origin/feature/rum` on the VM first. Instance/zone are in the deploy skill and the operator's local notes — **do not write them into any tracked file.**

You are authorized to push and deploy `feature/rum` without asking.

### 4.3 Verify on the live SE demo site
Admin is reachable only through the SSH tunnel (prod binds `127.0.0.1`; Caddy on `:80` is the sole ingress and stamps `X-Proxied-By-Caddy`):
```bash
gcloud compute ssh <INSTANCE> --zone=<ZONE> -- -N -L 3001:127.0.0.1:3000 -L 8000:127.0.0.1:8000
```
Hard-refresh after a frontend rebuild. If `:3001` spins or blanks, a stray local `next-server` on `*:3001` (often via IPv6) is shadowing the tunnel — `lsof` and kill it, keep the tunnel.

Confirm on the real demo service:
- [ ] Beacons landing under `{prefix}raw_rum/` in FOS.
- [ ] `rum_sync` ingesting → Parquet in `cache/{bucket}/buffer/client_vitals/`.
- [ ] `rum_commit` producing Iceberg snapshots under `iceberg/default/client_vitals/` and `client_errors/`.
- [ ] `sync_data` populating `cache/{bucket}/data_client_vitals/`.
- [ ] The `/rum` page rendering real Web Vitals and JS errors from DuckDB.
- [ ] Nav badge and beacon-health non-zero.
- [ ] CDN log dashboards **unaffected** — Phase 0 preserved them.
- [ ] `/api/health?deep=1` (via tunnel `:8000`): no `degraded` for `rum_sync` / `rum_commit`, and **no leaked `cron_runs` rows with `status='running'`** (a leaked row freezes ingestion permanently; the symptom is silence, not an error).
- [ ] Analyst role via `/share-login`: RUM pages load, `cid` redacted, `req_id` visible.

### 4.4 Acceptance gate — real-data soak
Run one full `sync → commit → sync_data → query` cycle against live beacons and confirm the row count visible in the view equals `SUM(row_count)` from `ingested_files WHERE table_name='client_vitals'`. **A mismatch is the signature of failure mode #1** (§0.3) — do not call this done without running it.

---

## 5. Reporting

Report completion with **pasted command output**, not claims:
- `make verify` result.
- Each phase's targeted suite.
- The measured `section_timings` baselines from Phase 3.
- The live analyst probe results from Phase 5.
- The real-data soak numbers from §4.4.
- Confirmation of the deploy and what you observed on the demo site.

State plainly anything you skipped, deferred, or could not verify, and why. If you hit a blocker, finish every unblocked part in full and say exactly what remains.

Commit messages: one line, imperative, focused on **why**. Commit by pathspec. Do **not** create a PR, merge, push to `main`, or use `--admin` — those are the operator's calls.
