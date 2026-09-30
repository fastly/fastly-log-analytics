# Current Project State and Current Goals

## Current release

The active development branch is `release/v3.0.0-beta3`. It is a clean,
single-commit continuation from `v2.4.1` containing the completed v3 work.
Version 3 adds the **High-Scale** architecture while preserving the existing
**Standard** architecture.

- **Standard:** single-host or local deployment using synchronous ingestion,
  local Parquet buffers, DuckDB/DuckLake, and per-service operational metadata.
- **High-Scale:** distributed Celery/RedBeat ingestion backed by Postgres,
  Valkey/Redis, and a shared DuckLake catalog. The serving tier remains
  single-pod; only ingestion scales horizontally.

Deployment-specific service IDs, hostnames, project names, credentials, and
cluster namespaces are intentionally absent from this public document. They
belong in ignored `configs/*.json`, `.env`, or operator-local documentation.

## Current goal

Complete and verify the v3 ingestion and analytics contracts without regressing
Standard mode. Every audited feature must work across:

- Standard and High-Scale deployment modes where supported.
- Admin access.
- Analyst Path A for supported Standard-mode flows.
- Analyst Path B remote-share access.

All I/O must retain structured telemetry, query attribution, cron progress, and
Fastly Object Storage usage/cost attribution. Verification must cover behavior,
RBAC, data freshness, failure handling, and user-visible status.

## Working protocol

1. Work on one cron job or one page per implementation session.
2. Read the authoritative specification, `AGENTS.md`, and relevant ADRs first.
3. Resolve behavior and update documentation before implementation.
4. Add regression tests for every non-trivial change.
5. Run focused tests, then `make ci`.
6. Use the canonical multi-environment deployment and verification tooling.
7. Keep deployment-private values outside the tracked tree.

## Phase 0: Environment health baseline

Before auditing or implementing any cron job or page, confirm that all four
deployment environments are currently healthy, running real services with
real traffic, and free of errors:

- Local Standard
- Local High-Scale
- GCE Remote Standard
- Elevation Remote High-Scale

Use the live deploy status page (`scripts/dev/report_server.py`, served at
`http://127.0.0.1:41705/`, always resolving to `reports/deploys/current/`) as
the source of truth. It exposes, per environment:

- Live port/credentials/bootstrap reachability panels (polled every 4s).
- A "Monitored Logs & Exception Dumps" panel that live-tails each container's
  backend and frontend log stream and filters for errors/warnings, independent
  of whether a deploy is actively running.

Run `export MONITOR_MINUTES=1 && ./scripts/dev/deploy_test_all.sh` (per the
canonical multi-tier deployment mandate) to produce/refresh a report, then
review `http://127.0.0.1:41705/` for each environment:

- No error/warning entries in the monitored backend or frontend logs that are
  not already known, documented follow-ups (e.g. the Local High-Scale
  ClickHouse `MEMORY_LIMIT_EXCEEDED` follow-up noted below).
- Each environment's dashboard loads with real, non-zero data (not a mock or
  stale bootstrap).
- Credentials/FOS/CDN status panels show healthy, not expired/401.

Any error found here that is new (not already a documented known-follow-up)
must be triaged and, if it blocks correctness of the cron/page under audit or
is a shared-code defect, fixed before continuing — per the working protocol's
allowance for shared-code fixes found along the way. If it's unrelated and
non-blocking, document it as a new known follow-up in this file and move on;
do not silently ignore it.

This phase is a recurring gate, not a one-time step: re-run it at the start of
each new cron/page work session, since environments can drift (credential
rotation, container restarts, upstream Fastly changes) between sessions.

### Phase 0 baseline result — 2026-09-22

The canonical deployment run completed for commit `2fa7e4434583` and produced
`reports/deploys/2026-09-22-12-37-03/`. All four ports responded, all four
credential panels reported Fastly API/FOS/CDN checks as healthy, and all four
dashboard endpoints exposed real non-zero rows. The gate nevertheless failed
because the monitored logs and end-to-end checks found the following new
correctness blockers:

- **Standard-mode FOS/DuckLake reads are unauthorized or inconsistent.** Local
  Standard and GCE Remote Standard emitted repeated FOS `401 Unauthorized`
  downloads. Local High-Scale also emitted DuckLake `404 NoSuchKey` reads.
  Local Standard and GCE Remote Standard logged buffer commits with schema
  mismatches (`116 columns but 97 values were supplied`) and/or
  `Catalog Error: Schema with name lake does not exist!`. These are not the
  documented Local High-Scale ClickHouse memory follow-up and must be resolved
  before cron/page work resumes.
- **GCE Remote Standard dashboard verification is degraded.** Its 30-day
  dashboard request returned `503`, and deep health repeatedly returned `503`
  while the ingestion/commit errors were active.
- **Local Standard freshness/RUM verification is incomplete.** The request
  header remained `Never`, and the RUM page had no Web Vitals data during the
  verification window; this is likely downstream of the FOS read failures but
  needs confirmation after the storage issue is fixed.

The following deployment-log entries were transient rollout noise rather than
confirmed runtime blockers: frontend SSR `ECONNREFUSED`/remote frontend
`ECONNRESET` while backends restarted, and the remote-standard SSH tunnel's
`Address already in use` messages because an existing tunnel already owned
ports 3001/8001. The existing tunnel made the endpoints reachable, but the
deployment tooling should report reuse explicitly instead of treating the bind
attempt as a clean new tunnel.

The known Local High-Scale ClickHouse `MEMORY_LIMIT_EXCEEDED` follow-up remains
unchanged and was not re-triaged here. The deployment also reported a
temporary Elevation scheduler age of 56 seconds and Celery queue depth of 221;
the subsequent dashboard/RUM checks passed, so this is a monitoring follow-up,
not yet a correctness failure.

## Phase 2: Background Tasks and Cron Jobs

The cron inventory contains 24 active logical jobs. Request and RUM discovery
share one ingestion outcome contract where their data paths are analogous.

1. `log_discovery_{id}` — request-log discovery, download, conversion, and
   High-Scale dispatch.
2. `log_commit_{id}` — Standard request-buffer commit to DuckLake.
3. `local_compact_{id}` — local hourly and daily/weekly compaction.
4. `partial_hour_merge_{id}` — active partial-hour merge.
5. `rollup_heal_{id}` — missing hourly rollup repair.
6. `rollup_compact_{id}` — daily rollup consolidation.
7. `optimize_{id}` — DuckLake inlined-data flush and file rewrite.
8. `expire_{id}` — retention, snapshot cleanup, and local cache purge.
9. `full_sync_{id}` — periodic ingestion reconciliation.
10. `gap_heal_{id}` — bounded ingest-gap discovery.
11. `metadata_cleanup_{id}` — operational metadata maintenance.
12. `alerts_evaluation_{id}` — alert rule evaluation.
13. `insights_prewarmer_{id}` — adaptive insight prewarming.
14. `sync_metadata_{id}` — Analyst Path A state synchronization.
15. `ledger_sweep_{id}` — High-Scale request-ledger recovery.
16. `rum_discovery_{id}` — Standard and High-Scale RUM discovery variants.
17. `rum_commit_{id}` — Standard-only RUM commit.
18. `ledger_rum_sweep_{id}` — High-Scale RUM-ledger recovery.
19. `metric_snapshot` — system-vitals snapshots.
20. `rdns_enrichment` — reverse-DNS enrichment.
21. `bot_data_refresh` — bot intelligence refresh.
22. `ngwaf_sync_{id}` — NGWAF request ingestion.
23. `share_audit_purge` — remote-share audit retention.
24. `duckdb_recycle` — DuckDB pool recycling.

The detailed inventory and per-job specifications live under
[`docs/cron/`](docs/cron/).

## Current work item

**Cron 1: `log_discovery_{service_id}`**

The design is approved and implementation is in progress. Do not move to Cron 2
until Cron 1 is implemented and verified in both deployment modes with Admin,
Analyst Path A, and Analyst Path B access checks.

### Cron 1 implementation decisions — 2026-09-29

- **Polling choice:** Preserve `log_period` and existing per-service
  `cron_sync.interval_mins` / `interval_seconds` as the baseline schedule and
  operator's freshness-vs-FOS-cost control. Add a per-service polling mode:
  Regular (default, one discovery pass per scheduled tick) or Adaptive
  (opt-in, at most two 3-second follow-up passes after finding new objects,
  within a 20-second tick budget; stop on an empty pass). Adaptive can increase
  FOS LIST charges and the UI must say so. Do not mutate the scheduler interval
  during a tick.
- **Freshness scope:** Optimize time until data is queryable from the active
  serving instance (Admin / Analyst Path B). Analyst Path A's independent
  snapshot-metadata sync is outside Cron 1's ≤10-second target. No universal
  ≤10-second guarantee is made when configured polling cadence or processing
  delay cannot meet it.
- **Commit boundary:** Do not commit to DuckLake on every discovery tick just
  to improve serving freshness. Standard serves local buffered rows; the
  scheduled commit remains separate. High-Scale conversion/publication workers
  own publication and acknowledgement.
- **High-Scale recovery boundary:** Keep the request discovery scan at its
  existing five recent minute prefixes. The separate `ledger_sweep` performs
  a four-hour FOS-vs-ledger diff, so Cron 1 must not duplicate that wider LIST
  on every tick. `provisioning.cron_sync.lookback_minutes` in the old job doc
  is stale and is not an implemented setting.
- **Counter persistence:** Persist the ten stable outcome counters as an
  `outcome_counters` JSON object in each request/RUM `cron_runs` record. Keep
  existing scalar fields populated under their current contracts.
- **Quarantine upgrade:** No v3.0 deployment is running. v2 deployments must
  be torn down before upgrading; the legacy FOS-backed quarantine API and
  request-line uploader have been removed without v2 row migration or
  compatibility shims.

### Cron 1 development checkpoint — 2026-09-29

- Implemented `cron_runs.outcome_counters` JSON persistence in the Postgres
  schema and `log_cron_run`; the focused metadata CRUD suite and Postgres DDL
  test pass.
- Implemented the local exact-byte quarantine evidence store and
  `quarantine_evidence` Postgres metadata table. Per-service captures use a
  PostgreSQL advisory lock, enforce the shared FIFO cap, preserve SHA-256 and
  bounded error metadata, and report capture failures to the ingest caller.
  Service log reset also removes the new metadata and local evidence files.
  Focused evidence, reset, buffer-corruption quarantine, metadata-schema, Ruff, and format
  checks pass. Capture also converts per-service metadata-lock acquisition
  failures into an explicit `quarantine_capture_failures` result; its regression
  test passes.
- Updated the Cron 1 design and implementation plan with the approved polling,
  active-serving freshness, Path A scope, commit-boundary, and greenfield
  upgrade decisions.
- **Recovery scope resolved:** Preserve the five-prefix discovery scan; the
  four-hour ledger-sweep FOS diff is the older-file catch-up path. Corrected
  the stale `lookback_minutes` claim in the Cron 1 job reference; no new
  lookback setting or extra per-tick LIST is planned.
- **High-Scale counter attribution and Standard RUM:** Added additive
  `originating_task` / `originating_run_id` ledger fields. Request/RUM
  discovery, full-sync catch-up, and both ledger sweeps persist the exact
  originating run on newly inserted objects; ON CONFLICT rediscovery does not
  overwrite it. Focused request/RUM discovery and sweep tests pass. Celery
  workers read this identity and call the new transactional per-object
  outcome-snapshot helper. The helper replaces an object's prior
  contribution (rather than incrementing blindly), updates the exact
  originating run's JSON counters and legacy row-count scalars, and makes
  data-plane failures visible as run errors. This supports late raw-deletion
  results revising the originating object's counters instead of double-
  counting or changing the commit run. High-Scale request/RUM conversion and
  delayed raw-delete paths now write idempotent object outcomes. Standard RUM
  captures malformed beacon lines as exact-byte local evidence, preserves
  valid neighbors, and persists the same ten counters; malformed records and
  corrupt containers make the run an error. Standard RUM's age-based raw
  retention cleanup is separate from current-run object outcomes; cleanup
  deletion failures are logged but do not alter those counters. Faro-only
  failures yield a warning, while RUM data-plane errors remain errors. A
  wrapper regression test now prevents Faro warnings from downgrading an
  ingestion error. Standard request ingest now
  persists its counters through the cron-run adapter and attributes async and
  inline deletion failures to the exact source keys, so only affected objects
  are classified as failed. A regression test also pins the cron wrapper's
  error status and counter persistence. A DuckDB diagnostic reread failure now
  falls back to scanning already-downloaded gzip files, preserving healthy
  neighboring rows and malformed-row accounting. Focused request/RUM/
  quarantine/metadata/cron suites pass, including this fallback regression.
  Ruff and backend mypy pass. Do not infer outcomes from discovered counts or
  the latest service run.
- Polling-mode configuration and UI are implemented; Regular remains the
  default, while Adaptive performs bounded follow-up discovery and discloses
  increased potential FOS LIST costs. Focused tests cover Regular default,
  Adaptive follow-up aggregation, empty-pass termination, the two-pass/time
  cap, High-Scale discovery, settings round-trip, and the selector interaction.
- The quarantine admin surface now reads the per-item evidence store rather
  than legacy FOS file records. It provides filtered/paginated evidence
  metadata, summary counts, exact-byte download capped at 50 MiB, and purge-one
  or purge-all. Endpoint dependencies reject Analyst Path B, and the source
  `access_level` guard rejects Analyst Path A. Backend router/evidence tests
  and frontend component tests pass. Backend mypy/Ruff, frontend TypeScript,
  ESLint, and formatting checks pass; generated OpenAPI types are current.
  `make openapi-drift` sees the expected generated OpenAPI diff because this
  work remains uncommitted; it compares against `git diff`, not just the live
  backend schema.
- Polling-mode and RUM outcome semantics are complete. The unused legacy
  FOS-backed request-line quarantine helper and its obsolete tests were removed;
  the legacy metadata table remains only for DuckLake buffer-file corruption.
  Cron documentation has been corrected to describe PostgreSQL usage-log
  storage and the local per-item quarantine store; the design no longer
  asserts unchanged background-tab polling behavior.
- The freshness benchmark now reads each target's service ID, backend URL,
  and CDN URL from `FLA_FRESHNESS_<ENV>_*` variables and admin auth from
  `REMOTE_ADMIN_TOKEN`, `ADMIN_SHARED_SECRET`, or `ADMIN_TOKEN`. It reports
  configured Regular/Adaptive cadence, probe-send-to-serving visibility, and
  discovery-completion-to-serving delay; High-Scale delay includes worker
  queue/publication. It does not retrieve FOS object `LastModified` or
  correlate aggregate changes to the probe marker, so results must not be
  labeled object-to-serving latency.
  **Verification update — 2026-09-29:** The first sequential `make -j1 ci`
  run completed with 7,914 passed, 96 skipped, and five failures. Three
  failures were directly related to this work: the route-table tripwire needed
  a placeholder for the new quarantine `item_id` parameter; the changelog
  breaking-path check required a note for the two intentionally removed
  legacy quarantine endpoints; and a batch-conversion test still asserted the
  removed FOS quarantine behavior. The route-tripwire and breaking-path checks
  now pass together in isolation. The batch test now asserts local exact-byte
  evidence and passes in a serial focused run; the two rollup/scoring tests
  that had xdist worker aborts also pass in that run.

  A subsequent `make test-ci` completed with 7,911 passed, 96 skipped, and
  eight failures in `tests/routers/test_admin_log_accounting.py`; each reported
  a row-count or gap mismatch. The module passes serially and with four xdist
  workers, identifying the full-auto failures as shared test-data collisions
  under high worker fan-out. A later bounded full backend run passed 7,918
  tests and skipped 96, with one remaining assertion in
  `tests/core/test_lake_info.py`: it assumed two physical DuckLake files for a
  two-row tiny commit. The test now asserts only a non-empty file/row count
  (which permits inlined or coalesced data) while preserving the exact
  two-row calendar check; it passes in isolation.

  **Verification update — 2026-09-30:** `PYTEST_XDIST_AUTO_NUM_WORKERS=4
  make -j1 ci` did not actually bound the Makefile's backend run:
  `test-ci` explicitly invokes `pytest -n auto`, which started workers through
  `gw9`. That run finished with 7,912 passed, 96 skipped, and seven xdist
  worker crashes (including a Python segmentation fault); the seven affected
  test node IDs passed together with `uv run pytest -n 0`.

  The initial clean-coverage run with explicit `-n 4` lost one xdist worker;
  its 83% report was incomplete. The default PostgreSQL endpoint is an
  unverified SSH forward, so subsequent runs use an isolated disposable
  PostgreSQL container on `127.0.0.1:15432` instead. Its non-durable settings
  prevent the test-database checkpoint stalls seen with the first disposable
  container; they are strictly for tests. Terraform tests pass there.
  A clean bounded-worker run on that container covered 55,804 statements,
  missing 8,348 (above the 85% floor), with 7,959 passing and 96 skipped,
  but two pre-existing expiry unit tests hit a closed pooled connection after
  other tests. Those tests now mock cron-run persistence along with their
  already mocked maintenance call and pass both serially and under xdist.
  The final clean `-n 4` run passed 7,961 tests with 96 skipped; the
  Terraform append passed both tests and the combined coverage was 85.02%
  (8,357 misses out of 55,804 statements), above the 85% floor. The final
  quarantine review found that a failed file unlink discarded the
  evidence metadata while leaving an orphan file; eviction now fails the
  capture and purge reports an error instead of losing the retry path.
  Focused evidence/router regressions pass and coverage appended after this
  fix remains above the floor (85.03%). The request/RUM error,
  corrupt-gzip evidence, High-Scale attribution, and
  scheduled/manual range regression tests pass individually.

  Next.js was updated from 16.3.4 to 16.3.7 to address the critical
  `GHSA-vcvr-r3jv-pc5j` advisory. OSV now reports no critical findings;
  the frontend build, backend lint/format/mypy/import contracts, frontend
  TypeScript/ESLint (831), security regression, secret scan, VCL/scorer,
  deployment validation, and generated OpenAPI drift checks pass. Frontend
  coverage passed (1,380 passed, 12 skipped), as did post-upgrade Chromium
  E2E (92 passed, 4 skipped). The synthetic performance gate remains red on
  this shared workstation: cold runs had one 90 ms or 98 ms sample among
  otherwise 8-18 ms samples, above its 52 ms ceiling. Its query, generator,
  and baseline are unchanged in this work; this measurement does not
  establish a Cron 1 regression, but the local `make ci` gate cannot be
  reported green. Approved live verification remains outstanding. Do not infer outcomes from
  discovered counts or the latest service run.

The approved request/RUM-aligned ingestion contract is:

- Valid records continue processing when another record in the source is
  malformed.
- Each malformed line is retained as one exact-byte diagnostic evidence item.
- A corrupt gzip container is retained as one complete evidence item.
- Evidence is stored locally under
  `data/services/{service_id}/quarantine/`, outside web/static roots.
- Metadata records source type, original FOS key, line ordinal, byte
  offset/length when known, normalized error category, bounded error text, and
  SHA-256.
- Quarantine is diagnostic evidence, not a re-ingest queue.
- One shared cap of 1,000 items per service applies to request and RUM evidence.
- There is no age-based expiry. New writes immediately evict the oldest items
  above the cap.
- Eviction failures are recorded while other eligible evictions continue.
- FOS source deletion is attempted after processing even when quarantine
  capture fails.
- Record failures, quarantine-capture failures, and source-deletion failures
  make the ingestion run `error`.
- Source-deletion failures increment both `objects_failed` and
  `source_delete_failures`.
- Request and RUM paths emit the same zero-filled outcome counters:
  `valid_records`, `malformed_records`, `corrupt_containers`,
  `quarantine_capture_failures`, `source_delete_failures`, `cap_evictions`,
  `objects_processed`, `objects_successful`, `objects_partial`, and
  `objects_failed`.
- High-Scale workers own conversion, validation, quarantine capture, durable
  publication, and acknowledgement. Ledger sweeps repair/redispatch work and
  run a separate four-hour FOS-vs-ledger discovery diff.
- Faro bundle reconciliation is the only intentionally RUM-specific warning
  condition. RUM data-plane failures remain errors.
- Quarantine APIs and UI are Admin-only. Both analyst paths are denied by the
  backend regardless of UI visibility.

Authoritative specifications:

- [`docs/cron/jobs/log-discovery.md`](docs/cron/jobs/log-discovery.md)
- [`docs/cron/jobs/rum-sync.md`](docs/cron/jobs/rum-sync.md)
- [`docs/cron/jobs/rum-discovery.md`](docs/cron/jobs/rum-discovery.md)
- [`docs/cron/jobs/rum-commit.md`](docs/cron/jobs/rum-commit.md)
- [`docs/cron/jobs/ledger-rum-sweep.md`](docs/cron/jobs/ledger-rum-sweep.md)

### Beta3 environment rebuild checkpoint

The disposable test services for Local Standard, Local High-Scale, and remote
High-Scale have been freshly reprovisioned with maximum real log-field
collection, RUM enabled, full sampling, non-edge-only capture, and short log
delivery periods. The production-like Standard demo service remains preserved.

Local Standard serving is repaired after the rebuild: its stale local DuckLake
catalog was reset so readers no longer chase pre-teardown FOS objects, and
`/api/query` plus `/api/log-extents` now return fresh request rows/extents.

High-Scale request-facts serving is working for fresh tagged traffic. The
generic status/extents surface must be backed by ClickHouse-visible facts for
High-Scale services, not by stale config status; this is now covered by focused
regression tests and verified in Local High-Scale. Remote High-Scale still needs
the same code deployed and rechecked.

**Known follow-up (not fixed this session, out of scope for the log-discovery
cron work item):** Local High-Scale's `/dashboard` intermittently shows
"Failed to load dashboard data. unhandled_error". Root-caused to
`backend/high_scale/dashboard.py`'s `bundle()` routing `/api/dashboard/bundle`
through ClickHouse for any service resolved by `HighScaleServiceRegistry`
(confirmed via `_debug_queries` showing `"engine": "ClickHouse"` SQL against
`request_facts`/`request_aggregates`), and the local `fla-hs-clickhouse-1`
container's background MergeTree merge hitting
`MEMORY_LIMIT_EXCEEDED (5.40 GiB)` in
`/var/log/clickhouse-server/clickhouse-server.err.log`, which fails whatever
concurrent SELECT the dashboard bundle issued. Not a credentials problem —
raising `max_server_memory_usage` / tuning `max_bytes_to_merge_at_max_space_in_pool`
or reducing retained history in the local ClickHouse container is the likely
fix. Needs its own dedicated session per the "one cron/page per session" rule.

## Phase 3: Pages

After all cron jobs are implemented and verified, audit pages one at a time
using the specifications under [`docs/pages/`](docs/pages/). Each page must
preserve shared layout/filter/time-range behavior, Standard/High-Scale parity,
role boundaries, telemetry completeness, and documented performance budgets.

Known page-parity investigation after the environment rebuild:

- Compare the **Traffic over Time** chart in Standard mode with both High-Scale
  deployments. High-Scale currently renders bars too close together, without
  the visual spacing present in Standard mode.
- Hold the selected time range and bucket interval constant while comparing the
  modes. Determine whether the difference comes from bucket density, omitted
  zero-value buckets, timestamp/bucket alignment, response shape, or frontend
  Plotly configuration.
- Standard and High-Scale may load data differently, but they must produce the
  same chart semantics and visual spacing for equivalent data. Fix the shared
  contract or rendering path rather than accepting data-source differences as
  the explanation.

## Completion gate

The current item is complete only after focused tests, `make ci`, and end-to-end
validation across the required environments and roles are green. Update this
document when advancing to the next cron or page.
