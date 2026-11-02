# High-scale reader parity: schema additions and backfill

The high-scale readers (Origin, CMCD, RUM, Insights, Logs, Sessions, Dashboard) compute real
results instead of returning empty or approximated stand-ins. Three of them need columns the
ClickHouse tables did not carry. New rows get the columns automatically; rows written before
the upgrade keep empty values until they are rebuilt from the FOS archive.

## What was added

| Table | Columns | Used by |
|---|---|---|
| `origin_minute_summary` | `latency_sk_v/_w`, `ttlb_sk_v/_w`, `overhead_sk_v/_w`, `origin_bytes_sk_v/_w` (`Array(Float64)` centroid values, `Array(UInt32)` counts) | Origin summary percentiles |
| `origin_minute_dimensions` | `latency_sk_v`, `latency_sk_w` | Origin timeseries, slow URLs, POP, path, IP health percentiles |
| `cmcd_projection_facts` | `country LowCardinality(String)`, `asn Nullable(UInt32)` | CMCD rebuffer by country / ASN, `country` / `asn` filters |
| `rum_vitals_facts` | `browser`, `os`, `device`, `city`, `region`, `pop`, `tls`, `ttfb` | RUM environments, filters, live events |
| `rum_error_facts` | the same plus `error_line`, `error_col` | RUM environments, exception line/column, filters, live events |

Sketches are log-histograms (geometric bucket ratio 1.02, relative error at most 1%) merged at
read time with `quantilesExactWeightedArray`; counts stay exact. `request_facts`, the publication
table and all other tables are unchanged.

## Applying the schema

`install_clickhouse_schema()` (worker start-up, `backend.high_scale.archive.clickhouse_batch`) is
idempotent: every addition is `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, a metadata-only change
that is safe while ingest is running. Deploy the new backend and workers; no manual DDL is needed.
The ClickHouse client allow-list (`CLICKHOUSE_TABLE_COLUMNS`) ships in the same release.

## What the readers show before the backfill

- **Origin**: percentiles for minutes written before the upgrade are `null` (they have no sketch);
  request counts, miss/pass counts and error rates are exact throughout. A window that spans the
  upgrade computes percentiles over the post-upgrade minutes only.
- **CMCD**: the country and ASN sections only include sessions whose rows carry the new columns.
- **RUM**: older beacons are counted as `Unknown` browser/OS/device, and older exceptions report
  line/column `0`.

Nothing is fabricated for old rows; they simply carry no value.

## Backfill

One script drives every rebuild; the archive in FOS is the source and is never modified.

```
uv run python scripts/backfill_high_scale.py --service-id <SERVICE_ID> \
    --projection origin --replace-existing --days 30
uv run python scripts/backfill_high_scale.py --service-id <SERVICE_ID> \
    --projection cmcd --replace-existing --days 30
uv run python scripts/backfill_high_scale.py --service-id <SERVICE_ID> \
    --projection rum_vitals --replace-existing --days 30
uv run python scripts/backfill_high_scale.py --service-id <SERVICE_ID> \
    --projection rum_errors --replace-existing --days 30
```

`--projection origin|cmcd` rebuild from the request archive; `rum_vitals|rum_errors` replay that
RUM domain's archive through the fact writer (newest day first). `--days` defaults to 30, the
table TTL. `--capacity-json` supplies an optional replay-capacity measurement JSON file.

`--replace-existing` is a high-impact step and is required whenever rows already exist:

- It runs `ALTER TABLE ... DELETE WHERE service_id = ... AND <time column> >= <now - days>`
  synchronously (`mutations_sync=2`) on the selected projection's table(s), then rebuilds.
  A rebuild publishes under new batch ids, so without the purge every request, beacon or session
  would be counted twice.
- Rollback: there is none to apply, because nothing outside the selected table is touched and the
  archive is intact. If a rebuild stops early, re-run the same command (a fresh purge, then a full
  rebuild). Do not pass `--rebuild-id` together with `--replace-existing`: a resumed rebuild skips
  batches it already recorded as visible, which the purge would have just deleted. The script
  rejects the combination.
- Run one projection at a time per service and let each report `full_rebuild_complete: true`
  (exit code 0; 1 means errors were reported, 3 means the capacity claim was invalid).
- Rows from a window the archive does not cover are lost by the purge. Check the archive coverage
  for the requested `--days` first, or narrow `--days` to the covered range.

## Verifying

- Origin: `POST /api/origin/aggregates` shows `_approx: true` with a `_approx_note` that names the
  histogram sketch, and `ottfb_p95_ms` is non-null for a backfilled window.
- CMCD: `POST /api/cmcd/aggregates` returns `rebuffer_by_country` and `rebuffer_by_asn` rows.
- RUM: `GET /api/services/<id>/rum/analytics` returns non-empty `environments` and exception
  line/column; `GET .../rum/live-events` shows real browser/OS/POP values.
- ClickHouse: for each table, `SELECT count() FROM <table> WHERE service_id = '<id>' AND
  <new column> IS NOT NULL AND <new column> != ''` (or `notEmpty(<array column>)`) rises to cover the
  window, and the visible row count per batch matches `high_scale_batch_publications`.

## Reader & Schema Contracts

No `high_scale` reader may return hardcoded empty lists/dicts, a swallowed query failure, or a silently ignored filter/parameter. When ClickHouse lacks data, add it to a projection and rebuild; do not approximate.

- **Percentiles:** never average per-minute percentiles. Origin projection rows carry a log-histogram sketch (`*_sk_v/_sk_w`) merged at read time with `quantilesExactWeightedArray`; responses set `_approx` + `_approx_note` (1% bound); counts stay exact.
- **Filters:** `request_filter_clauses` ([`backend/high_scale/_common.py`](../../backend/high_scale/_common.py)) translates request filters with typed params; unsupported filters raise `ValueError` (HTTP 400), never get dropped. RUM and CMCD have narrower allow-lists for their tables.
- **Dashboard window boundaries:** a sub-minute start or end uses the scoped facts reader (whole-minute projections cannot represent partial minutes); aggregate cache keys retain seconds and fractions; minute-aligned unfiltered windows keep the projection path.
- **Visibility fence:** fact readers require the batch's publication row to be `visible` (`visible_fact_clauses`).
- **Filtered Origin & Performance:** projections are per-minute with no filterable dimensions, so any active filter is served straight from `request_facts` with exact quantiles (`_approx: false`) and the projection's per-row semantics (Origin latency = `ottfb` else `ttfb`, status = `ost`, overhead = `elapsed - ottlb`; stored `performance_minute_dimensions.latency_*_ms` columns hold raw microseconds and the reader divides by 1000). A filter field with no facts mapping raises 400.
- **Security reads:** [`backend/high_scale/security.py`](../../backend/high_scale/security.py) mirrors the repository contract and never returns a hardcoded empty section. `request_facts` has no `waf_req_id`, so NGWAF names are resolved at ingest (`_enrich_high_scale_ngwaf_bots`, before facts publish) and persisted as `ngwaf_bot` / `ngwaf_bot_category` / `ngwaf_wellknown_bot`. `mask_ips` masks `active_clients[].ip` and `top_ips_header[].ip`; the router must pass it and clamp `threat-intel` / `proxies/export` windows.
- **SQL console trust boundary** ([`backend/high_scale/query.py`](../../backend/high_scale/query.py)): never splice scope into user SQL. Service, publication-fence, and window scope are sent via the `additional_table_filters` HTTP setting in `ClickHouseClient.execute(settings=...)` (allow-listed names only); the row cap wraps user SQL as `SELECT * FROM (\n<sql>\n) LIMIT n` plus `max_result_rows`; function calls are checked against `_ALLOWED_FUNCTIONS` (rejecting `remote`, `url`, `file`, `getSetting`, etc.); and the first query per client probes `system.one` to fail closed if the server does not enforce the filter. Analysts get the console only through per-service, per-mask-mode least-privilege ClickHouse identities (`fla_cv_*` views under `SQL SECURITY DEFINER` — see [high-scale-analyst-console.md](high-scale-analyst-console.md)).
- **Adding a column to a projection or fact table** touches: the `.sql` schema (`CREATE` + `ADD COLUMN IF NOT EXISTS`), `CLICKHOUSE_HIGH_SCALE_COLUMNS` / `CLICKHOUSE_HIGH_SCALE_TABLES` in `backend/core/clickhouse/constants.py`, the writer's column lists, the projection builder, a purge + rebuild path in `backend/high_scale/recovery.py` wired into [`scripts/backfill_high_scale.py`](../../scripts/backfill_high_scale.py), the reader, and this runbook.
- **ClickHouse dialect traps** (only visible against a real engine): `formatDateTime` minutes are `%i` (`%M` is the month name); a trailing `ORDER BY`/`LIMIT` after `UNION ALL` binds to the last `SELECT` only (wrap the union); `greatest(UInt64, Float64)` is illegal (`toFloat64`); a `LEFT JOIN` miss yields defaults, not NULL (`ifNull`); 64-bit ints are unquoted only because the client sets `output_format_json_quote_64bit_integers=0`. Real-engine integration tests in `tests/high_scale/` are gated by `RUN_HS_*` env vars.
- **High-scale archive retention:** Raw `.gz` objects are deleted once archived, so the FOS archive (`high-scale/archive/`) is the only copy that can rebuild ClickHouse after a volume loss. `HighScaleArchiveExpirer` ([`backend/high_scale/postgres_control/deletion.py`](../../backend/high_scale/postgres_control/deletion.py), run from `HighScaleDeletionSweeper.sweep`) deletes a manifest's artifact, `.json` manifest, and `.commit` marker (commit marker first) once the manifest is `source_deleted` and its coverage is older than the domain's retention plus a day (`data_retention_days` for `request`, `rum_retention_days` for RUM, `0` keeps forever), then sets `archive_expired_at`; `manifests_covering` skips expired manifests so replay never reads a missing artifact. `retention_seconds` / `retention_deadline` on a manifest only delay raw-source deletion. `/api/usage/prefill` and `/api/usage/current-storage` account for live and recently expired archive storage (`PostgresControlPlane.archive_storage_summary`, `regr_slope` of `artifact_size` on `row_count`).

## Regression gates

- `uv run pytest tests/high_scale` (unit, fake clients).
- `uv run pytest tests/high_scale/test_schema.py tests/high_scale/test_rum.py` validates the
  ClickHouse DDL, archive/manifest parity, and RUM reader/ingest SQL contracts.
