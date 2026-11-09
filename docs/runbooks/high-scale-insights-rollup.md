# High-scale Insights rollup: schema and backfill

Every Insights evaluation compares a window against a baseline (168 h by default). Without help,
each detection scans raw `request_facts` over the whole range. `request_insight_hourly` holds
exact hourly request counts per detector key, so 20 count-only detections read their whole
baseline hours from it instead.

## What was added

| Object | Purpose |
|---|---|
| `request_insight_hourly` (`ReplicatedSummingMergeTree`, 30-day TTL) | One row per (service, hour, shape, `batch_id`, `publication_state`, key values) with an exact `request_count` |
| `request_insight_hourly_mv` | Materialized view on `request_facts`; fills the table at insert time |

A shape is the set of fields some detections group or filter on (`ROLLUP_SHAPES` in
[`backend/high_scale/insights/rollup.py`](../../backend/high_scale/insights/rollup.py)): `url_status`, `url_cache`,
`misc` (cache, transport, ost, p_type, method, c_type, c_speed), `geo` (country, city, region,
status), `asn` (asn, p_type), `ua`, `referer`, `waf`. Each raw row adds exactly one count to every
shape.

## Exactness

- Rows keep `batch_id` and `publication_state`, and readers apply the same publication fence as
  `request_facts` (`visible_fact_clauses`), so a rollup row is visible exactly when its source
  batch is. `delete_batch_rows` on `request_facts` deletes the batch from the rollup too.
- Only detections whose baseline is a plain request count per key use the rollup
  (`ROLLUP_DETECTIONS`). `rollup_problems()` rejects anything else: distinct counts, quantiles,
  averages, min/max and sums of anything other than the row count.
- Each evaluation compares per-hour row counts and batch checksums of the raw facts with each
  shape's rollup, and reads from the rollup only for the longest run of whole baseline hours that
  match. The window, partial edge hours, hours with mismatches, and filtered evaluations all read
  raw facts. If the coverage read fails, everything reads raw facts.

Results are identical to the raw-only queries. The gated real-ClickHouse tests prove it
(`RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_insights_rollup.py`).

## Applying the schema

Both statements are `CREATE ... IF NOT EXISTS` in
[`backend/high_scale/sql/request_schema.sql`](../../backend/high_scale/sql/request_schema.sql). They contain no
`POPULATE`, `DROP` or `ALTER`, so they are safe to run from several pods at once. They are applied
by `install_clickhouse_schema()`, which runs from `python -m backend.core.clickhouse`. The backend
and worker deployments do not run it at start-up, so run it once per deploy in a backend pod.

Until the table exists, the coverage read fails and Insights reads raw facts as before.

Once the view exists, every `request_facts` insert also writes the rollup. If the view's insert
fails, the source insert fails with it.

## Backfill

Hours written before the view existed have no rollup rows, so they read raw facts until they are
backfilled. The backfill rebuilds hours from `request_facts` and never touches `request_facts`.

```
uv run python -m backend.high_scale.insights.rollup_backfill --service-id <SERVICE_ID>
uv run python -m backend.high_scale.insights.rollup_backfill --service-id <SERVICE_ID> --apply
```

- **Dry run by default.** The dry run prints each hour's plan, then
  `would rewrite N hour(s) (dry run; add --apply)`.
- **Idempotent per hour.** For each hour, the backfill compares per-batch row counts between raw
  facts and each shape's rollup. It deletes rows only for mismatched or orphaned batches
  (`mutations_sync=2`), then inserts those batches and any missing ones. Hours that already match
  are skipped, so a rerun resumes where the previous one stopped.
- **Bounded.** The backfill processes the newest hour first. Options:
  - `--days` (1–30, default 8) sets how far back to go.
  - `--skip-recent-hours` (default 2) leaves in-flight hours alone.
  - `--batch-size` (1–500, default 100) sets how many batches each insert covers.
  - `--max-hours` stops after that many rewritten hours.

A batch published while the backfill runs only makes that hour mismatch. The hour reads raw facts
until a rerun repairs it.
