# ClickHouse serving prototype: baseline and bounded replay

## Current state — experiment closed, dashboard prototype rejected

Task 12 selects the rejection branch of [ADR-20](../adr/20-clickhouse-serving-plane.md).
Production `/api/dashboard/aggregates` and `/api/dashboard/bundle` call the
existing DuckLake repository only, even with `CLICKHOUSE_ENABLED=true`.
There is no separate serving flag. Bounded opt-in FOS/Postgres/ClickHouse
index, schema, export/replay, observability and admin APIs remain available.
The archived `backend/repositories/clickhouse_dashboard.py` evaluator can be
called directly for experimental library comparisons; no live endpoint imports it.

This is a rejection of this prototype, not a universal ClickHouse verdict.
It did not qualify as a coexisting serving accelerator.

| Terminal gate | Measured outcome |
|---|---|
| Serial dashboard latency | Hybrid p50 5.681 s (roughly 6 s), DuckLake 0.793 s (roughly 0.8 s); p95 6.629 vs 1.055 s |
| Concurrency / request errors | Hybrid 8: 20 errors in **32 admitted samples**, 32 not admitted; 32: 58/64 errors; 64: 64/64 errors |
| Saturation qualification | Hybrid 8/32/64 memory-aborted; errors were 45-second deadlines, not successful completed 64-request qualification |
| Selected correctness / recovery | Passed within the bounded corpus; 184,374 rows rebuilt from 19 retained artifacts in 20.202443 s with exact digest |
| Docker serving acceptance | **Failed** latency/error/saturation gates |
| Task 11 Kubernetes | **Not implemented: Docker prerequisite failed, as required** |
| Task 12 decision | Complete: remove live hybrid routing, retain evidence tooling |

Tasks 1–10 produced the evidence below. Only the tested correctness/recovery
gates passed, not the entire serving acceptance. The local restart and live
probes completed, and full CI passed after repairing the test-isolation races.
See the verification record below rather than treating the earlier
phase-specific gate results as current.

## Final restart and live probes, 2026-09-08

The final Docker backend was rebuilt with all three overlays, leaving workers,
other services, and persistent volumes intact. Runtime inspection confirmed
`INGEST_MODE=celery`, `SERVING_MODE=durable`, `CLICKHOUSE_ENABLED=true`, and
`OTEL_EXPORTER=otlp`. ClickHouse enablement controls tooling, not dashboard routing.

After the final source changes and full-CI attempt, the requested command ran:

```bash
./run.sh --stop && ./run.sh --dev --no-reload --no-refresh
```

The gitignored local `.env` had explicitly enabled crons. It was changed to
`FLA_DEV_NO_CRONS=1` before restart to avoid competing ingestion against shared
FOS. The dev backend and frontend remain running on 18002 and 13002.
This dev instance is not the durable Docker comparison: ClickHouse is disabled,
and its local view returned zero rows for the frozen window.

| Probe | Final observation |
|---|---|
| Backend liveness and deep health | HTTP 200, `status=ok`, on both dev and Docker |
| Frontend `/dashboard` | HTTP 200 on both; Chromium rendered the main region and Dashboard heading with no page exceptions, 5xx responses, or missing-service state |
| Durable dashboard fixed corpus | HTTP 200; all 184,374 rows, 13 buckets, country/map/connection counts match the oracle; no `engine:hybrid` telemetry |
| Enabled admin ClickHouse status | HTTP 200, schema 1, healthy matching target, active dataset unexpired; no pending, claimed, or failed publications |
| Admin replay dry run | HTTP 200; plans 19 artifacts, attempts/publishes zero, does not activate |
| Disabled dev status/replay | Status explicitly disabled with unknown lag; replay HTTP 409 with canonical `detail.error=clickhouse_disabled` |
| Query monitor | HTTP 200; completed history includes ClickHouse, DuckDB, Postgres, SQLite on Docker and DuckDB/SQLite on dev |
| Pool snapshot | HTTP 200; final observed pools idle with zero saturation/drain rejects; Docker 1 allocated/1 idle, dev 3 allocated/3 idle, maximum 8 each |
| Authenticated ClickHouse health | Successful authenticated health query |
| Fresh publication metrics | Samples younger than 30 seconds: `app_clickhouse_up=1`, actionable publication lag 0 seconds |

Pool observations are not a serving-capacity claim. The existing durable
request path deliberately bypasses the reusable DuckDB pool
(`backend/deps.py`); other consumers can still create pooled connections.
Zero actionable publication lag means no outstanding bounded publication,
not up-to-date continuous indexing. The frozen dataset's event coverage is
older than current time and its existing expiry still applies.

An earlier full-CI attempt stopped in backend tests: 7,386 passed, 87 skipped,
two failed (`test_writer_then_reader_release_path` and
`test_release_pg_thread_connection_only_affects_calling_thread`). A preceding
one-worker run passed all 7,388 backend tests; subsequent runs are not a
substitute for a green end-to-end invocation. Prototype-specific canonical
error-envelope and frontend MSW coverage failures exposed during these runs
were fixed and their focused tests passed. No gates were weakened.

Both failing tests initially passed in isolation, and their two complete
modules passed 42/42. At that point the tests and underlying connection modules
matched release merge-base `80011e35`, but provenance did not resolve the
test-isolation defects. The test repairs now scope injected connection failures
to the intended database, count retries on the owning test thread, and check
Postgres returns by connection identity. Deliberately injected unrelated work
must not affect those assertions. A separate fresh-interpreter test pins the
real process-wide counter across 800 concurrent increments and reset.
All 43 affected tests pass. The subsequent
`PYTEST_XDIST_AUTO_NUM_WORKERS=1 make ci` completed successfully: 7,389 backend
tests passed, 87 skipped; the separate Terraform selection passed two tests;
combined backend coverage was 86.20%. Every remaining gate passed, and the full
Chromium/Firefox/WebKit matrix finished with 204 passed and 12 skipped, without
retries. No thresholds or assertions were weakened.

### Post-merge verification

The work was committed, then `origin/main` at `821011a2` was merged in
`b584ef48`, including the usage-chart and Playwright readiness/timeout changes.
The merged branch passed a second full
`PYTEST_XDIST_AUTO_NUM_WORKERS=1 make ci` invocation. Its browser matrix had
203 passes, one Firefox admin-SSE test passing on retry, and 12 skips. That
remaining browser flake is distinct from the repaired backend isolation races.

The exact dev restart command above was repeated after that gate, and the
Docker backend was rebuilt from the merged branch. Both backends again returned
healthy liveness/deep-health responses. The durable fixed corpus still matched
all 184,374 rows without hybrid routing; replay dry run attempted zero of its
19 planned artifacts. Fresh authenticated ClickHouse health and actionable-lag
metrics remained 1 and 0 respectively. The merged dev Dashboard and Usage pages
rendered in Chromium with no page exceptions or 5xx responses.

The later pool sample had two idle dev connections and no saturation/drain
rejects; the durable backend's pool list was empty, consistent with its
unpooled request path. These smoke probes do not demonstrate 10,000 API
requests/second or sustained ingestion at 10,000 log events/second.

### Earlier independent gate evidence

All 18 remaining gate targets passed separately, including typechecks, lints,
import contracts, security, OpenAPI drift, synthetic performance, and deploy
validation. Generated contracts were temporarily staged for the regeneration
comparison, remained byte-identical, and were returned to their unstaged state.
The preceding successful coverage phases measured backend coverage 86.23%
and 1,336 passing frontend tests with all frontend coverage floors satisfied.

An earlier `make e2e` matrix exited zero across Chromium, Firefox, and WebKit:
203 passed, one passed on retry, 12 skipped. The retry was the Chromium
merged-admin-SSE request assertion; the earlier trends contrast failure did
not reproduce. E2E logs also contained a metric-snapshot SQLite corruption
error whose cause was not established; browser success does not resolve that
separate warning. No thresholds, assertions, or unrelated UI were changed.

## Scope

This runbook records Task 1's DuckLake baseline, Tasks 4–6's explicit
snapshot export/replay workflow, Task 7's historical opt-in hybrid dashboard adapter,
Task 8's query/publication observability, Task 9's admin controls, and
Task 10's measured Docker benchmark and stopped-ClickHouse recovery exercise.
It does not demonstrate horizontal serving capacity.
[ADR-20](../adr/20-clickhouse-serving-plane.md) defines the selected boundary.
The existing ingest ledger and raw-deletion rules are unchanged.

The historical comparison was explicitly **hybrid**: ClickHouse owned time-bucketed
request counts and the selected country top-N; DuckLake still owns
`conn_requests`, map data, remaining response metadata, and request-context
connection acquisition. `sections=["core","topten"]` does not mean only two
queries: `core` also requests the map and connection histogram. Preserve the
whole response and tenant/time-clamp behavior.

## Frozen fixture and request

The sanitized fixture alias is `local-durable-one-service`. Discover the sole
service through `GET http://127.0.0.1/api/services`, then keep
`services[0].service_id` in memory. Never paste its ID, credentials, source paths,
or raw debug SQL into reports.

The comparison backend was rebuilt from checkout `cf623a66`, **including the
existing uncommitted `pg_schema.py` and `metric_snapshots.py` changes**. This is
not a pristine-commit benchmark. Runtime inspection confirmed
`INGEST_MODE=celery`, `SERVING_MODE=durable`, Postgres operational metadata, and
a Postgres DuckLake catalog. Requests used the local Docker stack through Caddy
on port 80, not a separate development server.

POST `/api/dashboard/bundle?service_id=<in-memory-service-id>` with
`Content-Type: application/json` and `x-debug-responses: 1`:

```json
{
  "start_time": "2026-09-01T00:00:00Z",
  "end_time": "2026-09-02T00:00:00Z",
  "fields": ["country"],
  "sections": ["core", "topten"],
  "chart_interval": "1 hour",
  "chart_metric": "requests",
  "filters": {}
}
```

Use absolute UTC bounds: omit `range_token` and `anchor`. This is an admin
request, with no analyst clamp or masking. The selected field is `country`;
the source must also expose `timestamp`, `ip`, `url`, and `conn_requests`.
No config writes or field-enablement changes are required.

**The existing API's end bound is inclusive (`<=`), not half-open.** There are
zero rows at September 2 00:00:00 UTC in this fixture, so the measured result
also equals the September 1 half-open daily count. Do not silently change that
boundary convention in the future adapter.

Use `/api/query` with `{"sql": "...", "dataset": "logs"}` to establish the
independent oracle:

```sql
SELECT count(*) AS rows, min(timestamp) AS earliest, max(timestamp) AS latest
FROM logs;

SELECT count(*) AS rows
FROM logs
WHERE timestamp >= TIMESTAMPTZ '2026-09-01T00:00:00Z'
  AND timestamp <= TIMESTAMPTZ '2026-09-02T00:00:00Z'
  AND ip IS NOT NULL AND ip != ''
  AND url != '/rum-beacon'
  AND url NOT LIKE '/rum-beacon' || chr(63) || '%';
```

The second query includes the dashboard's default exclusions even though
`filters={}`. SQL three-valued logic also excludes null URLs. Verify raw-window,
eligible-window, null/empty-country, null-URL, null-connection-counter, and
exact-end-boundary counts before comparing engines.

| Dataset check | Observed |
|---|---:|
| Actual `count(*) FROM logs`, before **and** after the run | 190,334 |
| September 1 raw window / eligible window | 184,374 / 184,374 |
| Rows excluded by dashboard defaults in that window | 0 |
| Rows exactly at the end bound | 0 |
| Null country / empty country / null URL / null conn_requests | 0 / 0 / 0 / 0 |
| September 2 / 3 / 4 / 7 daily source counts | 950 / 2,227 / 505 / 2,278 |
| September 6 daily source count | 0 — unsuitable as the benchmark window |
| Earliest event | 2026-09-01 03:28:03 UTC |
| Latest event, before **and** after | 2026-09-07 19:41:25 UTC |

`aggregates.total_rows_total` returned **217,342** from cached summary state.
That is not the measured dataset size and is not evidence of additional
queryable rows. Keep this discrepancy visible; do not “correct” it in one
engine to claim parity. The window's `total_rows` is independently verified.
Cached summary extents are likewise not the freshness oracle.

## Expected response and comparison tolerances

Each of the 30 measured responses passed these semantic checks:

- `aggregates` is an object; `top_bots` is explicitly `null`.
- `aggregates.total_rows == 184374`, `interval == "1 hour"`,
  `metric == "requests"`.
- `aggregates.data` has exactly `country` and `conn_requests`.
  Country has `top=[{"value":"US","count":184374,"label":null}]`,
  `total=184374`. Connection reuse has
  `top=[{"value":"1","count":184374,"label":null}]`, `total=184374`.
- `map_data == [{"country":"US","count":184374}]`.
- The time series has 13 ascending UTC buckets; every point has
  `category=null` and `baseline=null`. Request-count `value` is serialized as
  a float but is an **exact integer aggregate**, not an approximate metric.

All buckets below are on September 1, 2026, at the indicated UTC hour:

| Hour | Requests | Hour | Requests |
|---|---:|---|---:|
| 03 | 7,907 | 10 | 18,469 |
| 04 | 17,903 | 11 | 18,081 |
| 05 | 18,416 | 12 | 18,252 |
| 06 | 18,091 | 13 | 4,095 |
| 07 | 18,254 | 15 | 5,374 |
| 08 | 18,187 | 16 | 3,593 |
| 09 | 17,752 | | |

The sum is 184,374. Empty hours, including 14:00, are **omitted**, not
zero-filled. Build the independent chart oracle with
`time_bucket(INTERVAL '1 hour', timestamp), count(*)`, the same predicate,
and `GROUP BY 1 ORDER BY 1`.

Comparison policy:

1. Integer totals, bucket counts, dimensions, nulls, labels, and interval/metric
   strings match exactly. Missing, null, empty string, and zero are distinct.
2. For future genuinely floating-point aggregates, require
   `abs(actual - expected) <= max(1e-9, 1e-6 * abs(expected))`.
   NaN/infinity never pass; null matches only null. No float tolerance applies
   to this corpus's request counts.
3. Compare timestamps as timezone-aware instants and require UTC buckets in
   ascending order. Preserve connection buckets in lower-bound order
   (`1`, `2–5`, `6–20`, `21+`), retaining the en dashes.
4. Country top-N is descending by count, capped at 10. Current live SQL orders
   by `field, c DESC` **without a value tiebreaker**. Equal-count members may
   permute; at a tied cutoff, any eligible members of that tie may fill the
   remaining slots, but all strictly higher-count members must be present.
   Do not claim deterministic ties. This fixture has one country, so it
   exercises neither tied ordering nor cutoff selection.
5. The durable path's map query has no `ORDER BY`; compare country/count
   membership, not array order. There is only one map row here.
6. Retain response structure and metadata ownership. Do not compare volatile
   timing/debug fields or internal `where_clause`/SQL text as semantic results.
   The future hybrid slice leaves global summary fields DuckLake-owned.

This is a narrow, low-cardinality performance corpus, not proof of arbitrary
filters, ties, null dimensions, pagination, analyst RBAC, or timezone parity.
Those cases need additional fixtures before the adapter ships.

## Measured baseline

Measurement interval: **2026-09-07 22:33:54.952899–22:34:19.751599 UTC**.
Three warm-up requests preceded 30 sequential measured requests at concurrency
one. Python `urllib.request` made a fresh HTTP connection per request; no client
pool, retries, sleep, or concurrent load was added.

Latency uses `time.perf_counter()` from request dispatch through complete body
read and JSON decoding, before semantic checks. Wall time includes the
measurement loop and its checks. Quantiles use linear interpolation at
`(n - 1) * p` in sorted samples. Achieved RPS is successful measured requests
divided by wall time, not an extrapolated concurrency figure.

| Metric | DuckLake baseline |
|---|---:|
| Dataset / eligible window rows | 190,334 / 184,374 |
| Warm-ups / measured attempts / concurrency | 3 / 30 / 1 |
| Wall time | 24.798434208030812 s |
| Achieved successful query throughput | 1.2097537993057923 requests/s |
| p50 | 803.6019370192662 ms |
| p95 | 979.9141249619423 ms |
| p99 | 1,006.2040426721796 ms |
| HTTP or semantic errors | 0 |
| Cached responses | 0 of 30 |
| Debug queries per measured response | 7 |
| Max observed ledger discovery-to-commit lag | 128 s |
| ClickHouse ingest lag / rebuild time | `null` / `null` — not deployed |

`DASHBOARD_CACHE_TTL=0`; every response and every recorded debug query reported
`is_cached=false`. This does **not** mean storage/connection/OS caches were
cold: these are warmed endpoint measurements, with debug telemetry enabled.
Observed sections included `wide_temp_create`, `top_n_batch`,
`conn_requests`, `time_series`, and `map_data`: DuckDB executed the queries
against the durable DuckLake view, not local rollup files. `_debug_queries`
exposes SQL, duration, and cache state, **not an engine tag**; engine ownership
comes from the verified runtime mode and repository path, not an invented
telemetry field.

Freshness was observed at **2026-09-07 22:34:21.612814 UTC**:

- Actual data event watermark: **19:41:25 UTC**.
- Latest committed ledger timestamp: **19:43:20 UTC**.
- Latest DuckLake publication marker: **19:50:11.937041 UTC**.
- Lag scope: all **1,544 committed ledger rows for this one service**, each
  with discovery and commit timestamps, measured as
  `max(committed_at - discovered_at)`. This is not the event watermark's age,
  not a p95 ingest SLA, and not a claim that every historical log row has a
  corresponding ledger row. Stored Postgres `REAL` epoch timestamps have coarse
  precision; the 128-second value is the observed ledger resolution.

The source count, min/max timestamp, and eligible-window count were checked
before and after the run and unchanged. This is observed stability, **not**
a pinned catalog snapshot or immutable replay manifest. Recheck these
watermarks for every later comparison; if they change, recapture both engines
against an equivalent fixture instead of comparing mismatched datasets.

## Repeat the measurement

1. Verify the recorded image/working-tree provenance and runtime modes. Do not
   restart into an unrelated dev server or modify capture/retention settings.
2. Discover the sole service in memory; submit the exact request above through
   Caddy. Enable the same debug header for both engines.
3. Query the dataset extents and counts. Assert the frozen fixture counts;
   derive independent hourly, country, and connection-histogram expectations
   with the dashboard exclusions. Do not use cached summary counts.
4. Execute three warm-ups, then at least 30 requests serially. Time every
   attempt, fully consume the response, and check its semantic shape. Count
   HTTP failures **and** semantic failures as errors; never retry or drop slow
   samples. Retain full-precision latencies but no raw debug payloads.
5. Requery source extents/counts and reject a comparison whose watermarks
   changed. Read the Postgres ledger for the same in-memory service identity:

   ```sql
   SELECT count(*), count(discovered_at), count(committed_at),
          max(committed_at - discovered_at),
          max(committed_at), max(published_at)
   FROM ingest_ledger
   WHERE service_id = %s AND status = 'committed';
   ```

   Bind the service ID as a parameter. Require nonempty complete timestamp
   coverage; unknown lag is not zero. Open the connection using the existing
   `METADATA_DSN` environment variable inside the backend container without
   printing it. Do not dump ledger object names.

6. Serialize the report using the interface below. Preserve unsuccessful runs
   as reports with nonzero errors; they are not acceptable parity baselines.

For reference, the quantile calculation is:

```python
import math

def quantile(samples_ms, p):
    samples = sorted(samples_ms)
    index = (len(samples) - 1) * p
    lo, hi = math.floor(index), math.ceil(index)
    return samples[lo] + (samples[hi] - samples[lo]) * (index - lo)
```

## Report interface and verification

`tests/load/clickhouse_serving_contract.py` exposes:

- `BenchmarkReport.model_validate(mapping)` for Python values, with aware
  `datetime` objects.
- `BenchmarkReport.model_validate_json(json_text)` for JSON reports, with
  ISO-8601 timestamps.
- `report.model_dump_json()` for serialization.
- `Freshness` for the required timestamp subrecord.

Required root fields are `schema_version` (1), `engine` (`ducklake` or
`clickhouse_hybrid`), `dataset_size_rows`, `eligible_window_rows`,
`concurrency`, `warmup_requests`, `measured_requests`, `elapsed_seconds`,
`achieved_throughput_rps`, `p50_ms`, `p95_ms`, `p99_ms`,
`ingest_lag_seconds`, `error_count`, `freshness`,
`clickhouse_ingest_lag_seconds`, and `clickhouse_rebuild_seconds`.

`freshness` requires `observed_at`, `event_watermark`, `window_start`,
`window_end`, `last_commit_at`, and `last_publication_at`. All must be
timezone-aware; the window must be increasing and closed by the observation
time. Publication may lag the latest commit.

The two ClickHouse-only fields are required **nullable** measurements:
explicit `null` means unavailable, not omitted or zero. DuckLake reports
reject non-null values in those fields. A future hybrid report may retain
null until a measurement exists. No ClickHouse success is implied.

The schema rejects missing/extra fields, booleans or strings used as numbers,
fractional counts, negative/nonfinite measurements, unordered quantiles,
impossible counts, and inconsistent throughput. RPS must equal
`(measured_requests - error_count) / elapsed_seconds` within relative `1e-6`
or absolute `1e-9` rounding tolerance. Quantiles include failed attempts.
Keep unrounded values in machine-readable reports.

```bash
uv run pytest tests/load/test_clickhouse_serving_contract.py -o addopts='-q'
uv run ruff format --check tests/load/clickhouse_serving_contract.py \
  tests/load/test_clickhouse_serving_contract.py
uv run ruff check tests/load/clickhouse_serving_contract.py \
  tests/load/test_clickhouse_serving_contract.py
```

Task 1's focused validation passed. Tasks 2–8 are implemented; the later
sections cover the hybrid adapter and observability. No ClickHouse admin API
is installed.

## Explicit export and replay

These commands run on the private Compose network. They do not expose a
ClickHouse port or restart the baseline backend. Set `SERVICE_ID` privately
from the configured logging service; do not paste it into this document.

```bash
make clickhouse-prototype-up
make clickhouse-prototype-schema

# Preview only: no source connection, manifest mutation, FOS write or usage flush.
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml \
  run --rm --no-deps clickhouse-schema-init \
  python -m scripts.clickhouse_replay --service-id "$SERVICE_ID" \
  --export --start 2026-09-01T00:00:00Z --end 2026-09-02T00:00:00Z \
  --max-rows 1000000 --batch-rows 10000 --ttl-hours 24
```

Add `--apply` to export. Save the returned `dataset_id` as `DATASET_ID`.
The command requires a configured admin service, Postgres metadata and a
Postgres DuckLake catalog. Dry-run validates arguments, not connectivity or
source row counts.

```bash
# New generation, including every artifact published to earlier generations.
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml \
  run --rm --no-deps clickhouse-schema-init \
  python -m scripts.clickhouse_replay --service-id "$SERVICE_ID" \
  --rebuild "$DATASET_ID" --limit 100 --apply

# If the artifact limit left it incomplete, resume the returned generation.
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml \
  run --rm --no-deps clickhouse-schema-init \
  python -m scripts.clickhouse_replay --service-id "$SERVICE_ID" \
  --resume "$GENERATION" --limit 100 --apply
```

`--limit` is **artifacts**, 1–1000. It never authorizes partial activation.
`--rebuild` always creates a new generation; `--resume` retries unpublished
members of the specified generation. An insert failure exits nonzero, retains
the manifest, and records the sanitized exception class. A lost acknowledgement
does not count as publication. Discover an interrupted generation through
`clickhouse_generations`, scoped by the service ID, then resume it.

### Storage and visibility contract

| Component | Contract |
|---|---|
| `clickhouse_datasets` | Immutable service, source catalog identity, derived table, snapshot version, inclusive UTC bounds, expiry, expected rows and canonical digest |
| `clickhouse_artifacts` | Immutable Parquet URI, SHA-256, byte size, canonical digest, ordinal start and row count |
| `clickhouse_generations` | Dataset membership, actual ClickHouse table UUID, selection revision and lifecycle |
| `clickhouse_publications` | Per-generation/artifact claims, lease fence/expiry, attempts, sanitized errors and verified digest |
| `clickhouse_service_selection` | One active generation per service, updated under a Postgres lock/revision check |
| `clickhouse_schema_state` | Validated fact-table UUID and schema version |

These are real psycopg transactions from the existing Postgres pool.
The SQLite-shaped metadata wrapper is deliberately not used: its `commit()`
is a no-op. SQLite production configuration is rejected.

The exporter reads `lake.<derived-table> AT (VERSION => <snapshot>)` through
one ordered Arrow reader, inside a source transaction. Ordering includes every
exported dimension and the ingest-stamped `_source_file` when present
(`source_file` is also recognized). Legacy rows without either field carry
an empty source identity. Identical rows still receive
distinct stable ordinals, so replay retains legitimate duplicates.

The canonical payload is a UTF-8 JSON array per row, followed by a newline:
`source_identity, row_ordinal, timestamp, country, ip, url, conn_requests`.
JSON uses compact separators; UTC timestamps have six fractional digits;
null and empty strings remain distinct. Artifact and dataset canonical
digests hash those bytes in ordinal order. The separate content hash covers
the complete Parquet bytes.

Artifacts live only in the configured service bucket under
`<prefix>/clickhouse-prototype/<hashed-service>/<dataset>/<sha256>.parquet`,
outside `ducklake/`. Uploads use conditional creation and verify HEAD size and
hash metadata. Downloads verify the full content hash before decoding.
Postgres membership is sealed only after every artifact has uploaded.
No raw logs, source Parquet or ingest-ledger rows are deleted or changed.

Bounds are 1,000,000 rows per dataset, 10,000 rows and 16 MiB per artifact,
64 KiB per canonical row, and at most 1,000 rows per ClickHouse insert chunk.
An oversized row or dataset aborts rather than truncating coverage.
The exporter also caps canonical artifact buffers below the Parquet byte
limit and opens DuckDB with 512 MiB, two threads and disk spilling disabled.

Expiry defaults to 24 hours, never longer. Positive customer
`provisioning.cron_sync.data_retention_days` caps it at the coverage start
plus that retention period. A zero customer setting means unlimited source
retention, **not** unlimited prototype retention. Expired datasets cannot
serve or replay. The operator owns removal of expired prototype objects and
retired ClickHouse partitions; no automatic cleanup job or bucket lifecycle
change is installed in this slice. Failed exports can leave unreferenced
objects carrying `expires-at` metadata. Expiry is an eligibility gate, not a
claim that physical data has already been removed.

The fact table is `ReplacingMergeTree`, partitioned by `(service_id,
generation)` and sorted by `(service_id, generation, batch_id, row_ordinal)`.
This finite prototype favors isolated rebuilds, not unlimited generation
growth. Every verification and future serving query must use `FINAL`.
Publication verifies ordered values, not just counts or a stored row hash.
Activation verifies the complete dataset and exact manifest membership.

### Interfaces for the next task

Dataclasses and methods are defined in `backend/core/clickhouse_manifest.py`,
`clickhouse_publication.py`, and `clickhouse_export.py`:

```python
DurableBatch(artifact: Artifact, rows: tuple[tuple, ...])
PublicationResult(batch_id: str, status: str, row_count: int = 0, fence: int | None = None)
ReplayResult(generation: str, attempted: int, published: int, activated: bool)

build_batch_id(service_id: str, source_identity: str, content_hash: str) -> str
PgManifest.claim(service_id: str, generation: str, batch_id: str, *, lease_seconds: int = 120) -> int | None
publish_batch(service_id: str, batch: DurableBatch, *, generation: str,
              store: PgManifest | None = None, client: ClickHouseClient | None = None) -> PublicationResult
replay_unpublished(service_id: str, *, generation: str, loader: Callable[[Artifact], DurableBatch],
                   limit: int = 100, store: PgManifest | None = None,
                   client: ClickHouseClient | None = None) -> ReplayResult
full_rebuild(service_id: str, dataset_id: str, *, loader: Callable[[Artifact], DurableBatch],
             limit: int = 100, store: PgManifest | None = None,
             client: ClickHouseClient | None = None) -> ReplayResult
export_snapshot(service_id: str, *, start: datetime, end: datetime, max_rows: int = 1000000,
                batch_rows: int = 10000, ttl_hours: float = 24) -> Dataset
readiness(service_id: str, *, start: datetime, end: datetime, source_snapshot: int,
          catalog_identity: str, source_table: str, store: PgManifest | None = None,
          client: ClickHouseClient | None = None) -> Readiness
```

`readiness` returns a reason: `no_active_generation`,
`expired_or_unsupported_dataset`, `outside_coverage`,
`source_snapshot_mismatch`, `target_unverified`, or `ready`.
The eligible result carries dataset metadata and generation.
It currently verifies the complete bounded target on each call: correct but
not a performance optimization. The adapter does not turn that into a cached
startup-green flag; an empty replacement volume or truncated generation must
never produce a zero-row success.

**Hybrid prerequisite:** pass the snapshot actually bound to the request's
DuckLake view. Querying the current maximum version beside a mutable `logs`
view does not establish equality. The adapter pins a unique request-local
normalized view with `AT (VERSION => snapshot)` before combining engines.
Catalog identity hashes the
credential-free source location and Postgres database/catalog-table OIDs;
the per-service table name is a separate equality condition.

### Verification evidence, 2026-09-07

Task 4 passed 77 focused tests, then initialized the real schemas twice.
Task 5 passed nine real-engine scenarios before Task 6 began. The completed
slice passed 115 focused tests and 12 explicit Postgres/ClickHouse integration
tests. These cover lost acknowledgement, partial inserts, a crash before the
manifest update, stale/expired claims, exact-content mismatch with equal
counts, atomic rollback, competing generations, empty target, replacement
table UUID, expiry, source mismatch and a tiny mocked-FOS exporter.
Unit tests also pin expiry crossing during target verification; an initially
valid dataset must still be unexpired when readiness returns.

The actual September 1 baseline exported **184,374 rows into 19 artifacts**
at source snapshot **27,777**, totaling **1,599,557 Parquet bytes**. All 184,374
rows retain their ingest-stamped source-object attribution. Full ordered
verification matched canonical SHA-256
`2ec73693b464fccaddc5446a60f76488c77b7e8f6628b541d7b971c15784936c`.
A second full generation loaded all 19 artifacts with `DUCKLAKE_CATALOG`
unset, proving rebuild from retained FOS artifacts plus Postgres, without
reopening the source catalog. Both generations verified exactly; the newer
one became active. The dataset expires at **2026-09-08 23:36:16 UTC**.
This proves replay correctness, not dashboard latency or capacity.

The final `PYTEST_XDIST_AUTO_NUM_WORKERS=4 make ci` run passed backend tests
(7,209 passed, 83 skipped, **86.12%** coverage), frontend unit tests
(1,335 passed, 12 skipped), and the lint/type/schema/security/Rust gates.
Four workers were used after the automatic-worker run crashed in the existing
DuckLake attach-concurrency test; that test passed independently and in the
four-worker run.

**Full CI is not green:** cross-browser E2E finished with 202 passed,
12 skipped, one flaky WebKit dashboard navigation timeout, and one failed
Chromium `/admin/trends` accessibility test. The failing loading-label
contrast was **4.36:1**, below **4.5:1**, on six nodes. The failure also
reproduced on its retry. No UI change was made for this backend slice, and no
commit was created. Playwright retained the failure traces under
`frontend/test-results/`.

```bash
uv run pytest tests/core/test_clickhouse_client.py tests/core/test_clickhouse_schema.py \
  tests/core/test_clickhouse_publication.py tests/core/test_clickhouse_export.py \
  tests/core/test_clickhouse_manifest.py \
  tests/core/test_v3_boot_migration.py tests/test_clickhouse_compose.py -o addopts='-q'

# Tests are mounted because the production backend image intentionally omits them.
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml \
  run --rm --no-deps -e RUN_CLICKHOUSE_INTEGRATION=1 \
  -v "$PWD/tests:/app/tests:ro" clickhouse-schema-init \
  python -m unittest tests.core.test_clickhouse_integration -v
```

## Task 7: historical hybrid dashboard experiment (route removed)

**Archived instructions and measurements.** Task 12 removed the dispatcher.
The configuration below now enables diagnostic tooling only; it cannot enable
dashboard serving. The historical HTTP 503/fallback behavior below belongs to
the removed route, not the current dashboard. Do not re-enable it to repeat
endpoint measurements. Use the recorded checkout/image plus working-tree diff
as historical evidence, or call the retained evaluator directly offline and
label those results as library comparisons, not endpoint benchmarks.

The existing `/api/dashboard/aggregates` and `/api/dashboard/bundle` handlers
dispatch only after `RequestContext` has enforced service access and clamped
the request window. No new endpoint or public response field was added.

An eligible request uses the exact body under **Frozen fixture and request**:
`fields=["country"]`, `sections=["core","topten"]`, `chart_metric="requests"`,
`chart_interval="1 hour"`, `filters={}`, with explicit inclusive UTC bounds
inside the sealed dataset. The adapter also accepts `1 second`, `1 minute`,
and `1 day`. An equivalent standalone `/aggregates` call without `sections`
is supported when all aggregate flags are on. A `/bundle` request without
`sections` still requests bots and is deliberately ineligible.

All other fields, nonempty filters (including country filters), other metrics
or intervals, source-level `time_range` scopes, non-durable connections, and
unsupported schemas stay on the existing DuckLake path. Invalid API filter
models remain rejected by Pydantic. Coverage, expiry, catalog/table/snapshot
mismatch, absent active generation, and healthy-but-unverified target data
also produce explicit DuckLake fallback reasons.

### Runtime configuration

Build the backend with Task 7 code before enabling it. The running baseline
backend was **not** restarted or enabled during Task 7 verification.
The local multipod + observability + prototype compose overlays supply:

```text
INGEST_MODE=celery
SERVING_MODE=durable
CLICKHOUSE_ENABLED=true
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_SECURE=false
CLICKHOUSE_DATABASE=fla_prototype
CLICKHOUSE_USER=fla_prototype
CLICKHOUSE_PASSWORD=prototype_local_only
```

The last three values are the overlay's **local-only defaults**, not production
credentials. Keep the existing `DUCKLAKE_CATALOG` and `METADATA_DSN` Postgres
settings and configured FOS credentials. Do not print or copy them into reports.

```bash
CLICKHOUSE_ENABLED=true docker compose \
  -f docker-compose.multipod.yml \
  -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml \
  up -d --no-deps --build backend
```

Disable with the same command and `CLICKHOUSE_ENABLED=false` (no rebuild needed
for rollback). Disabled requests create no ClickHouse client or socket.
The container service name `clickhouse` resolves only on the compose network.
Do not publish its HTTP port just to enable the backend.

### Correctness and operational limits

Every aggregate reads `log_facts FINAL`, constrained by service and verified
generation. This deduplicates retry copies immediately, without waiting for
background merges. The predicate matches the dashboard defaults: non-null,
nonempty IP; non-null URL excluding `/rum-beacon` and `/rum-beacon?…`; inclusive
UTC bounds. Country's denominator is `count(country)` (empty strings count);
top-N excludes null and empty values and retains the existing non-deterministic
tie contract. Bucket timestamps stay UTC; empty buckets are omitted.

The request-local DuckLake view uses the existing field normalization and
source-clamp helper. It never replaces a shared service view or updates global
view caches. A read transaction and explicit `AT VERSION` keep remaining
DuckLake sections on the same logical snapshot even across concurrent commits.
The selected country top-N and time-series SQL are **not** also run in DuckDB.
DuckLake still materializes the filtered input for map, connection histogram,
and counts, and still supplies cached global summary metadata. Its existing
counts also detect a target emptied between readiness and aggregate execution.

Transport/query failures during readiness or aggregation return sanitized
HTTP **503** (`hybrid_unavailable`), not a zero-shaped success. A healthy but
empty/replaced/truncated target fails readiness and falls back with
`target_unverified`. No readiness result is cached.

Inspect existing `_section_timings` for `engine:hybrid`,
`clickhouse:readiness`, and `clickhouse:time_series_country`; ineligible requests
have `engine:<reason>`. Structured `clickhouse.dashboard_dispatch` events carry
the engine and reason; existing ClickHouse debug-call telemetry records each
operation. No new observability service, metric family, or dashboard was added.

### Task 7 verification evidence

- Focused backend suite: **202 passed, 1 skipped** across the adapter,
  dashboard repository/router, and ClickHouse client/publication/export/
  manifest/schema tests. Targeted mypy and Ruff passed; both import contracts
  passed.
- Real ClickHouse test: inserted two physical copies of nine synthetic rows
  in one operation, then queried immediately. All four supported intervals
  matched the expected counts, including null/empty dimensions, null URLs/IPs,
  beacon exclusion, and the microsecond after the inclusive end boundary.
  The test cleans up only its UUID-scoped placeholder service.
  The complete real Postgres/ClickHouse integration suite also passed:
  **13 tests**, including the existing publication/replay cases.
- Real in-memory DuckLake tests verify an `AT VERSION` view stays frozen while
  another connection commits, normalization remains intact, and the original
  service view sees the commit after request cleanup. Router tests retain the
  existing analyst clamp and sanitized 503 envelope.
- One isolated repository-level comparison against the sealed actual FOS
  corpus passed full response parity: **184,374 window rows**, **13 buckets**,
  country/map/connection histogram unchanged, cached global total **217,342**
  unchanged. Source snapshot remains **27,777**. This used the current source
  mounted into a one-off container, not a replacement running backend.
  A final call through the real dashboard router and response model against
  the same corpus also passed with `engine:hybrid`.
- That single comparison measured **648.12 ms** DuckLake repository time versus
  **4,615.06 ms** hybrid, including **4,180.22 ms** fresh readiness verification
  and **65.46 ms** selected ClickHouse aggregates. These are one-shot timings,
  **not** endpoint p50/p95, throughput, or a Task 9 benchmark. The prototype is
  slower with the intentionally full target proof. Do not claim a speedup or
  replace that proof with a cached green flag.

**Full CI is not green.** `PYTEST_XDIST_AUTO_NUM_WORKERS=4 make ci` stopped
in its backend phase with **7,241 passed, 84 skipped, one failed**:
`tests/core/test_duckdb_concurrency.py::test_writer_then_reader_release_path`
observed one lock retry where it expected zero. That test passed when rerun
alone through `uv run pytest`. No connection-pool/concurrency code was changed
to mask the failure, and the later full-CI gates were not reached in this run.
Logs are retained locally in `cache/task7-ci.log`; focused final-source
validation and the real-engine results above are separate from that CI result.

This records the state at Task 7, before the later admin controls and measured
rejection. The frozen dataset expires at the time recorded above; after expiry,
export and verify a new bounded generation before using the retained evaluator.

## Task 8: query and publication observability

Use **all three overlays**, in this order: multipod, observability, ClickHouse.
`CLICKHOUSE_COMPOSE` in the Makefile now extends `COMPOSE_STACK`; the schema/
replay container also explicitly exports OTLP. Omitting observability silently
leaves the app on its default `OTEL_EXPORTER=none`.

No new exporter is installed. The existing OTel SDK sends metrics every
15 seconds to `http://prometheus:9090/api/v1/otlp/v1/metrics`; Prometheus already
enables `--web.enable-otlp-receiver`. Each SDK gets one `service.instance.id`
resource identity, mapped to Prometheus `instance`, so concurrent replay and
backend processes do not overwrite each other's cumulative counters.
The replay CLI force-flushes before exit, including failures. Backend shutdown
flushes, disables observable callbacks, and closes their dedicated client.

### Metric contract

| OTel instrument | Kind / unit | Prometheus series |
|---|---|---|
| `app.clickhouse_query_duration_ms` | histogram / `ms` | `app_clickhouse_query_duration_ms_milliseconds_bucket` |
| `app.clickhouse_insert_duration_ms` | histogram / `ms` | `app_clickhouse_insert_duration_ms_milliseconds_bucket` |
| `app.clickhouse_queries_total` | counter | `app_clickhouse_queries_total` |
| `app.clickhouse_publications_total` | counter | `app_clickhouse_publications_total` |
| `app.clickhouse_publication_lag_seconds` | observable gauge / `s` | `app_clickhouse_publication_lag_seconds` |
| `app.clickhouse_rows_inserted_total` | counter | `app_clickhouse_rows_inserted_total` |
| `app.clickhouse_bytes_read` | counter / `By` | `app_clickhouse_bytes_read_bytes_total` (recording alias) |
| `app.clickhouse_up` | observable gauge | `app_clickhouse_up` |
| `app.clickhouse_disk_free`, `app.clickhouse_disk_total` | observable gauges / `By` | `app_clickhouse_disk_free_bytes`, `app_clickhouse_disk_total_bytes` |

Prometheus 3.5 translates the byte counter to
`app_clickhouse_bytes_read_total`: it suppresses the repeated `bytes` unit
token. `observability/clickhouse.rules.yml` preserves the requested dashboard
name with a 15-second recording alias. Both names are visible; sum only one.
After installing that rule mount, validate/recreate **Prometheus only**:

```bash
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml run --rm --no-deps \
  --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose -f docker-compose.multipod.yml -f docker-compose.observability.yml \
  -f docker-compose.clickhouse-prototype.yml up -d --no-deps prometheus
```

Preserve existing container configuration and volume bindings before any
recreate. A previously anonymous Prometheus volume contains real history too;
do not replace it with an empty named volume. Task 8 verification migrated
that history into the declared named volume and retained both original data
and the brief new-volume capture.

Query/insert durations include admission wait. Query outcomes are `success`,
`error`, or `timeout`; publications count `published`, `failed`, `stale`, and
`not_claimed` attempts, including failures before acquiring a claim. Counters
are lazy, so an outcome has no series until it occurs. Insert rows count
**acknowledged physical rows, including retry copies**, not unique facts or
published batches. Read bytes come from ClickHouse statistics, not HTTP
response size. No metric labels contain tenants, datasets, batches, query IDs
or SQL. Existing structured events and `record_call.details` retain query IDs
and operation statistics; publication events carry batch IDs.

The existing Active Queries surface shows `ClickHouse` rows with a query ID
and operation summary, not SQL or parameter values. They are deliberately
non-cancellable (`con=None`): interrupting a shared HTTP client could affect
another request. No new query monitor or endpoint was added.

### Cheap backend gauges

The enabled backend registers observable callbacks on the existing metric
reader, not a new scheduler. Four callbacks share one sample for ten seconds.
They use a dedicated one-slot HTTP client with two-second connect/query/
admission bounds and one authenticated internal `system.disks` query for the
default disk. These probes do not enter the query-duration histogram.

Lag is a bounded Postgres aggregate over publication `created_at`: the maximum
age of pending, claimed, or failed work, joined to a **building** generation,
an unexpired dataset, and a service-selection row whose revision still matches
that generation. Superseded, retired, published and expired work is excluded.
No admin request or expensive generation-readiness proof is involved.

An empty successful aggregate reports zero lag. A Postgres error reports
unknown (no observation), never zero. A ClickHouse probe error reports `up=0`
and unknown disk capacity. Grafana rejects gauge samples older than 30 seconds
so Prometheus's normal lookback does not keep an old healthy value on screen.
Disabled metrics are no data, not healthy. Disk values describe shared
filesystem capacity, not the size of the fact table.

The existing `fla-multipod` dashboard (datasource UID `prometheus`) now has
query/insert p95 and p99, query/publication outcomes, actionable lag,
authenticated health, disk capacity, acknowledged rows, scan bytes, and Docker
memory panels. Existing container regexes include ClickHouse. The Docker
exporter already sees the container; its source is unchanged.

### Task 8 verification evidence

- Tests were added and run red before implementation. Focused final-source
  validation passed **197 tests**, Ruff, targeted mypy and both import
  contracts. Real Postgres/ClickHouse integration passed **14 tests**, including
  pending/claimed/failed lag and terminal/expired/revision filtering.
- Only the backend image was rebuilt and the backend recreated with all three
  overlays. Unowned backend environment values were preserved; the worker was
  neither rebuilt nor restarted. It still lacks the observability overlay
  until a separately authorized restart.
- Live Prometheus queries verified every metric family above, the healthy
  byte-name recording rule, `up=1`, zero known actionable backlog, nonzero disk
  capacity and Docker memory. Publication `published`, `failed`, and
  `not_claimed` series were observed; `stale` is covered by the focused tests.
  All eleven ClickHouse dashboard entries loaded in Grafana and every panel's
  PromQL was accepted. Live query p95/p99 were populated; these operation
  histograms are not endpoint benchmark results.
- Three real HTTP corpus probes preserved **184,374 rows and all 13 exact
  buckets**. A further probe confirmed `engine:hybrid`, cached total
  **217,342**, **3,937.26 ms** full readiness and **76.95 ms** selected
  ClickHouse aggregation inside a **5,509.83 ms** request. No proof was
  weakened and no performance improvement is claimed.

Full `make ci` is deferred to the coordinating session for Task 8. The earlier
Chromium `/admin/trends` contrast failure and lock-retry flake remain recorded
above; neither was changed. No commit, new admin API, benchmark, Kubernetes
work, ingest change, or raw-deletion change is part of Task 8.

## Task 9: admin status and bounded replay

These are API-only controls. Both routes stay under the existing
`RemoteAccessMiddleware` admin block: an authenticated analyst gets 403.
There is no new frontend hook or export endpoint.

### Status

`GET /api/admin/clickhouse/status?service_id=<service-id>` requires an existing
configured service. It reports:

- `enabled` and `health` (`ok`, `disabled`, or `unavailable`).
- The verified target's recorded `schema_version`.
- `publication_counts` for pending, claimed, failed, and published records,
  scoped to this service, including historical/retired generations.
- `oldest_pending_age_seconds`: age of the oldest actionable pending/claimed/
  failed publication in a building, unexpired, current-revision generation.
  No actionable backlog is `null`, not an invented zero-age publication.
- `last_published_at`: last successful publication across this service's
  history, not the event watermark or a promise that the target is current.
- `active_generation`: selected generation and dataset references, inclusive
  coverage bounds, expiry, `expired`, `target_matches`, and
  `coverage_age_seconds` (age of the coverage end, not ingest latency).
- `expired_dataset_count`, including expired datasets no longer selected.

Disabled mode returns 200 with `enabled=false`, `health=disabled`, and null
measurements. Enabled dependency failures return 503 with `health=unavailable`;
unobserved values remain null. An incompatible recorded schema also returns
503. Missing service configuration is 404; an unreadable configuration is 503.
`health=ok` proves connectivity/schema health, **not** current data or full
generation readiness. Check the selection's expiry and target-match flags.
Neither responses nor errors expose FOS locations, catalog DSNs, or credentials.

### Replay

`POST /api/admin/clickhouse/replay` accepts:

```json
{
  "service_id": "<service-id>",
  "dataset_id": "<dataset-id>",
  "dry_run": true,
  "limit": 1
}
```

Replace `dataset_id` with `generation` to resume a building generation.
Exactly one reference is required. The service must exist and have read-write
access. The shared limit range is 1–1000 artifacts, but HTTP rejects values
above **100** with 422. HTTP also refuses datasets containing more than 100
artifacts with 409: final activation verifies the whole dataset, not just the
last request's slice. Use the CLI for larger retained datasets.

Omitting `dry_run` means **true**. Unlike the CLI's argument-only dry-run,
the API validates the service, target schema, scoped dataset/generation,
expiry, selection revision, and artifact counts through read-only metadata
queries. It does not read or write FOS, audit, allocate a generation, claim a
batch, or insert facts. It does not promise the artifact bytes are accessible
or valid; apply verifies those through `FosArtifacts.load`.

Set `dry_run=false` to apply. A dataset reference starts a new generation via
`full_rebuild`; a generation reference calls `replay_unpublished`. Both use the
existing immutable-artifact loader, never an export. The response includes
`operation`, `dataset_id`, `generation`, `artifact_count`, `planned_artifacts`,
`attempted`, `published`, and `activated`. A new-generation dry-run returns
`generation=null`; it does not reserve an ID. Claimed work can make actual
publication smaller than the plan. Partial work is never activated.

These synchronous handlers run in the request thread pool, with 2-second
Postgres pool/statement/lock bounds and existing bounded ClickHouse/FOS I/O.
They schedule no background work. Apply audits requested/completed/failed
actions and safe counts. Disabled replay is 409; missing scoped references
are 404; expired/incompatible/non-resumable references are 409; dependency or
publication failures are sanitized 503. An apply error can leave partial work:
inspect status before retrying, and resume the existing building generation
rather than assuming a failed response rolled back the whole request.

### Task 9 verification evidence

- **188 focused tests passed**, including disabled/unavailable states,
  reference and limit validation, read-only dry-run, actual replay/loader
  code with fake storage, and authenticated analyst 403s with an allowed-route
  positive control. Error telemetry cannot inject FOS URLs on debug opt-in.
- **16 real Postgres/ClickHouse integration tests passed** using the freshly
  rebuilt backend image, including service-scoped status/preview and expired
  selections. The separate schema-init image was stale; use the rebuilt
  backend image for this verification, rather than treating that old image's
  missing module as a source regression.
- Ruff, targeted mypy, both import contracts, and frontend TypeScript checks
  passed. Full OpenAPI JSON and TypeScript types were regenerated and a second
  generation was byte-stable. `make openapi-drift` itself compares to the Git
  index and rejects these expected uncommitted generated changes; no files
  were staged or committed to hide that distinction.
- The backend was rebuilt/recreated with multipod + observability + ClickHouse
  overlays and `CLICKHOUSE_ENABLED=true`; all other configured environment
  values and volume bindings were preserved. The live backend was healthy.
  Status returned 200, schema 1, 76 published records, no actionable backlog,
  and a nonexpired selected generation matching the target.
- Live dry-run against the selected 19-artifact dataset, with `limit=1`,
  returned one planned artifact, zero attempts/publications, and no new
  generation. Before/after fingerprints of all five service-scoped manifest
  relations and the physical fact count matched.

Task 10 and final verification are recorded separately below and at the top
of this runbook.
No raw-deletion, ingest, export, contrast, or lock-retry behavior changed.

## Task 10: measured Docker gate — failed

**Do not proceed to Kubernetes.** The hybrid prototype preserves the tested
results and recovers from a real ClickHouse outage, but fails the Docker latency
and saturation gates. We did not weaken full-generation readiness verification,
change resource limits, or add performance optimizations to obtain a pass.

### Historical bounded comparison procedure

**Not a current hybrid enablement recipe.** These commands record how Task 10
was measured before the dispatcher was removed. Preserve the recorded image,
checkout and uncommitted diff provenance; a pristine checkout alone does not
reproduce that working tree. Do not reintroduce the route to repeat evidence.
The current driver refuses `--engine clickhouse_hybrid` before Docker/network
access and also rejects responses without hybrid ownership. Historical report
schemas and samples remain readable. Current DuckLake runs need no retired
`engine:disabled` marker, but still require uncached query telemetry. They
accept either diagnostic flag state: `CLICKHOUSE_ENABLED=true` no longer
means hybrid serving, and turning off useful status/replay tooling is unnecessary.

`tests/load/clickhouse_serving_benchmark.py` requires both consent flags, a
literal loopback HTTP origin, an explicitly named Docker backend, and an ignored
`local-docs/*.json` output. It verifies the three-overlay Docker configuration,
Celery/durable mode, Postgres configuration, and the requested feature-flag state.
The driver does not change engines, stop services, or mutate storage.

The historical procedure built the backend once and recreated **only backend**, using all three
overlays and `CLICKHOUSE_ENABLED=false` for DuckLake or `true` for hybrid:

```bash
docker compose -f docker-compose.multipod.yml \
  -f docker-compose.observability.yml -f docker-compose.clickhouse-prototype.yml \
  build backend

CLICKHOUSE_ENABLED=false docker compose -f docker-compose.multipod.yml \
  -f docker-compose.observability.yml -f docker-compose.clickhouse-prototype.yml \
  up -d --no-deps --force-recreate --wait backend

make clickhouse-prototype-benchmark BENCHMARK_ARGS="\
--apply --allow-prototype --engine ducklake --base-url http://127.0.0.1 \
--backend-container ${LOCAL_BACKEND_CONTAINER:?} \
--output local-docs/task10-ducklake.json --stage-seconds 600"
```

Repeat the recreate and benchmark with `true`, `--engine clickhouse_hybrid`,
and a different output filename. Do not recreate workers, Postgres, Fastly
Object Storage resources, or volumes. Restore the original enabled state afterward.
After a memory abort, stop admission; a separate higher-concurrency attempt
requires a healthy, freshly recreated backend and its own evidence file.
`--concurrency 32` or `--concurrency 64` selects one such bounded stage.

Both engine runs used the exact frozen bundle, 64 offered attempts per stage,
three serial warm-ups, a 600-second admission ceiling, a 45-second **whole
request** deadline, and at most 64 HTTP connections. A fixed worker count bounds
in-flight work; there is no retry loop or unbounded task queue. This is a
fixed-attempt comparison, **not equal-duration sustained-load testing**: measured
wall times differ and are reported below. Every failed attempt remains in
latency quantiles. RPS is successful responses divided by actual stage wall time.
Requests use fresh connections through Caddy, with debug telemetry enabled.

The backend has **no Docker cgroup memory limit** (`HostConfig.Memory=0`), not
the suspected 2 GiB limit. ClickHouse retains its existing 2 GiB limit. The Docker
VM reports 8,307,617,792 bytes of memory. The driver samples backend
`memory.current` once per second and stops admitting requests at 1,536 MiB;
already-dispatched requests drain through their bounded deadlines. This can
overshoot the threshold between samples. No limit was raised to pass a stage.
Storage/OS caches were warm, response caching was off, and readiness remained
uncached. Backend resets after hybrid saturation are a disclosed recovery step,
not evidence of steady-state high-concurrency capacity.

### Actual results

Measurements ran on September 8, 2026 UTC. All reported runs used the same
backend image (`sha256:d5fcb3cdd12a4f052a9fdbfe4d99dc6ad22fff08be6d85be1c3c4195f9dcf2a5`).
Independent SQL counts before and after **each run** remained 190,334 total,
184,374 eligible, and zero in the September 6 empty window. The event watermark
remained September 7 19:41:25 UTC. A final independent
`ducklake_snapshots('lake')` query returned snapshot 27,777; successful hybrid
requests also required the pinned source snapshot to match the retained dataset.
The cached summary value 217,342 was never used as dataset size.

Latency columns below are **seconds**, including HTTP failures/timeouts.
`Peak` is measured simultaneous client attempts, not a claim of simultaneous
database execution. The higher hybrid rows are saturation results, not passing
latency baselines.

| Engine | Concurrency / peak | Attempts / errors | Wall s | Successful RPS | p50 s | p95 s | p99 s | Max s | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| DuckLake | 1 / 1 | 64 / 0 | 53.021 | 1.2071 | 0.793 | 1.055 | 1.314 | 1.449 | complete |
| Hybrid | 1 / 1 | 64 / 0 | 371.152 | 0.1724 | 5.681 | 6.629 | 6.961 | 7.101 | complete |
| DuckLake | 8 / 8 | 64 / 0 | 34.017 | 1.8814 | 4.329 | 5.510 | 6.423 | 7.020 | complete |
| Hybrid | 8 / 8 | 32 / 20 | 180.019 | 0.0667 | 45.002 | 45.005 | 45.006 | 45.007 | memory abort; 32 not admitted |
| DuckLake | 32 / 32 | 64 / 0 | 34.520 | 1.8540 | 14.713 | 22.677 | 23.551 | 24.134 | complete |
| Hybrid | 32 / 32 | 64 / 58 | 90.034 | 0.0666 | 45.005 | 45.020 | 45.020 | 45.020 | memory abort |
| DuckLake | 64 / 64 | 64 / 0 | 37.421 | 1.7103 | 27.668 | 37.080 | 37.336 | 37.400 | complete |
| Hybrid | 64 / 64 | 64 / 64 | 45.022 | 0.0000 | 45.005 | 45.007 | 45.007 | 45.007 | memory abort |

All 142 measured hybrid errors above were request deadlines, not discarded
samples. Their dispatch and backend read metrics are **unknown**, not proof of
fallback or zero ClickHouse work. An earlier concurrency-32 warm-up returned
one HTTP 503 among three attempts, so that invocation admitted no load and
retained a separate `warmup_failed` report. The later stage is an explicitly
separate attempt, not a silently retried successful sample.

Backend peak memory was 624 / 982 / 1,251 / 1,356 MiB for DuckLake, versus
988 / 1,695 / 1,704 / 1,648 MiB for hybrid at 1 / 8 / 32 / 64. No OOM kill
was needed. The backend was recreated after saturation to abandon residual
server-side work and restore service, with unchanged resource settings.

Every successful frozen-bundle response matched the integer/time-bucket oracle.
DuckLake responses reported `engine:disabled` and no ClickHouse calls. Successful
hybrid responses reported `engine:hybrid`, full readiness timing, and 25
ClickHouse calls. At concurrency 1, readiness alone had a 4,037.315 ms median.
Those 64 responses observed 81,776,832 ClickHouse rows read and 17,984,680,960
bytes read in total. These are scan counters, not dataset size or transferred
payload size. Timeout responses provide no call telemetry; their work is not
included in these observed counters.

### Separately labeled mixed serial correctness corpus

The load table is **only the frozen bundle**. A separate four-request serial
corpus exercised bundle, top-N-only aggregates, time series with map/connection
sections disabled, and the same service's empty September 6 window. This is a
one-sample-per-case correctness probe, not a mixed-load throughput claim.

| Case | DuckLake flag off | Hybrid flag on | Semantic digests |
|---|---|---|---|
| Frozen bundle | disabled / DuckLake | hybrid | identical |
| Top-N-only aggregates | disabled / DuckLake | unsupported_sections / DuckLake | identical |
| Time series plus country card | disabled / DuckLake | unsupported_sections / DuckLake | identical |
| Empty historical window | disabled / DuckLake | outside_coverage / DuckLake | identical |

All eight responses passed. The adapter does **not** accelerate the three
fallback cases. Top-N-only responses retain the existing `"interval":"1 minute"`
metadata while emitting no chart. An initial harness wrongly expected `"1 hour"`
there; that failed corpus artifact is retained, the expectation was corrected,
and the complete DuckLake measurement was rerun. No application behavior changed.

### Real rebuild, outage, deletion gate, and immediate duplicates

A source-mounted schema-init process invoked the existing replay CLI with
`DUCKLAKE_CATALOG` removed. It rebuilt a **new generation**, from all 19 retained
Fastly Object Storage artifacts, not from the source catalog or its current files:

| Recovery measurement | Observed |
|---|---:|
| Rebuild duration, including activation verification | 20.202443 s |
| Rows / artifact bytes / artifacts published | 184,374 / 1,599,557 / 19 |
| Manifest-to-target canonical digest | exact match |
| Duplicate-visible rows in rebuilt generation | 0 |
| New-generation creation → last publication, Postgres timestamps | 16.017697 s |
| Original retained-dataset creation → this publication | 6,660.389378 s |
| Existing source-ledger max discovery → commit lag | 128 s |

The 6,660-second value includes waiting before an explicit replay; it is not
a streaming-ingest SLA. This prototype has no continuous ClickHouse ingest hook.

The outage test used a separate three-row, placeholder-named service manifest
and real retained Fastly Object Storage artifact. Only ClickHouse was stopped;
Docker reported it `exited`. A publication attempt failed with `ClickHouseError`
in **0.282475 s**, leaving the new generation's member **pending**, with
`published_at=NULL`. Failure occurs before claiming when the target is unreachable,
so pending rather than failed is the correct state here.

The real raw-deletion function, Postgres claims, and Fastly Object Storage DELETE
were exercised on **one disposable fixture object**, never a customer raw key:

1. Successful ClickHouse publication with no DuckLake marker deleted **zero**
   objects.
2. During the ClickHouse outage, setting the fixture's valid-shaped DuckLake
   publication marker authorized deletion of **one** fixture raw object.
   This is a controlled metadata-gate test, not a newly executed DuckLake commit.
   Only fixture source/client lookup was supplied by the harness; the deletion
   predicate and raw-deletion implementation were unchanged.
3. The retained artifact still loaded exactly. After ClickHouse restarted,
   replay published and activated the three rows in **0.399639 s**, with an
   exact canonical digest.
4. With table merges temporarily stopped, an explicit identical reinsert produced
   **six physical rows but three logical `FINAL` rows**. Verification still
   matched the manifest. Merges were restarted in a `finally` block.

Fixture Fastly Object Storage objects, ledger rows, manifests and ClickHouse
facts were then removed using exact fixture identities. Existing service data
and the rebuilt generation remain intact. Final source counts, snapshot, hybrid
bundle, API health and admin ClickHouse status were verified; both health/status
requests returned 200.

### Evidence and acceptance

Ignored local artifacts retain every duration/error without service IDs, raw
SQL, credentials, or response bodies:

- `local-docs/task10-ducklake-final.json`
- `local-docs/task10-clickhouse-hybrid.json`
- `local-docs/task10-clickhouse-hybrid-c32-measured.json`
- `local-docs/task10-clickhouse-hybrid-c64.json`
- `local-docs/task10-hybrid-corpus.json`
- `local-docs/task10-recovery.json`, `task10-publication-lag.json`,
  and `task10-final-health.json`
- Earlier DuckLake/corpus and failed-warm-up artifacts remain alongside them.

`Sample`, `Stage`, `RunEvidence`, and `RecoveryEvidence` supplement the unchanged
strict `BenchmarkReport` contract. They retain corpus ownership, all errors,
full-precision samples, peak concurrency/memory, observed rows/bytes, maximum
latency, and recovery measurements. Missing timeout read telemetry is nullable.
Reports are checkpointed after each stage; a failed post-run oracle cannot erase
completed measurements.

Provisional gates: exact selected-corpus semantics; no duplicate-visible replay
rows; classified publication failures without false success; zero request errors
and improved p95 **and** p99 at the intended concurrency. Correctness/recovery
passed within the stated scope. Latency and saturation failed, including a
serial regression from 1.055 s to 6.629 s p95. **Task 11 was not executed:
Docker prerequisite failed, as required.** Task 12 rejected this prototype for
dashboard serving and restored DuckLake-only routing; Docker recovery is not
evidence of scalable serving.

Focused validation: 153 contract/driver tests passed, with targeted Ruff and
mypy passing. Full CI remains the coordinating session's separate final gate;
no commit was created.
