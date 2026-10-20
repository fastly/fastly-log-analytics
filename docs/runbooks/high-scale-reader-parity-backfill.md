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

`install_clickhouse_schema()` (worker start-up, `backend.high_scale.schema_install`) is
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
uv run python scripts/backfill_high_scale_origin.py --service-id <SERVICE_ID> \
    --projection origin --replace-existing --days 30
uv run python scripts/backfill_high_scale_origin.py --service-id <SERVICE_ID> \
    --projection cmcd --replace-existing --days 30
uv run python scripts/backfill_high_scale_origin.py --service-id <SERVICE_ID> \
    --projection rum_vitals --replace-existing --days 30
uv run python scripts/backfill_high_scale_origin.py --service-id <SERVICE_ID> \
    --projection rum_errors --replace-existing --days 30
```

`--projection origin|cmcd` rebuild from the request archive; `rum_vitals|rum_errors` replay that
RUM domain's archive through the fact writer (newest day first). `--days` defaults to 30, the
table TTL. `--capacity-json` behaves as documented in
[high-scale-helper-wiring.md](high-scale-helper-wiring.md).

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

- Origin: `GET /api/origin/aggregates` shows `_approx: true` with a `_approx_note` that names the
  histogram sketch, and `ottfb_p95_ms` is non-null for a backfilled window.
- CMCD: `POST /api/cmcd/aggregates` returns `rebuffer_by_country` and `rebuffer_by_asn` rows.
- RUM: `GET /api/services/<id>/rum/analytics` returns non-empty `environments` and exception
  line/column; `GET .../rum/live-events` shows real browser/OS/POP values.
- ClickHouse: for each table, `SELECT count() FROM <table> WHERE service_id = '<id>' AND
  <new column> IS NOT NULL AND <new column> != ''` (or `notEmpty(<array column>)`) rises to cover the
  window, and the visible row count per batch matches `high_scale_batch_publications`.

## Regression gates

- `uv run pytest tests/high_scale` (unit, fake clients).
- `RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_clickhouse_readers_integration.py`
  and `RUN_RUM_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_rum_integration.py` execute the
  reader SQL on a real ClickHouse engine (disposable `clickhouse-local` processes from an installed Docker
  image; no ports or deployed services). They catch dialect mistakes a fake client cannot.
