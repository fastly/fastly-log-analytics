---
title: "Ingest Accounting: RUM beacon counting, DuckLake active-hour fallback, and table_name/file_date filters"
author: Drew
date: 2026-11-02
branch: v3.0.0-rc1
id: l-d100384d
scope: repo
status: active
src: agent
touches:
  - backend/core/metadata/ingest_log.py
  - backend/routers/admin/health.py
  - backend/routers/admin/log_accounting.py
  - scripts/dev/scale_harness.py
  - tests/core/test_storage.py
  - tests/routers/test_admin_ops_and_storage.py
  - tests/scripts/test_infra_and_probe_helpers.py
---

In backend/routers/admin/log_accounting.py and backend/core/metadata/ingest_log.py, Ingest Accounting must: (1) cast file_date to text (CAST(file_date AS text)) when width=10 (by=day) so PostgreSQL CASE does not fail with DatatypeMismatch (date vs text), (2) filter ingested_files by table_name = 'logs' in get_log_accounting_counts so client_vitals/client_errors files do not inflate request-log file_count/_meta_rows, (3) fall back to raw_bucket_counts on the DuckLake logs view for the active hour when legacy data/timestamp_hour=* parquet files are absent, and (4) include RUM beacon rows (rum_bucket_counts on client_vitals, counting 2 edge hits per non-TLS beacon due to Fastly HTTP->HTTPS 301 redirect) because VCL excludes /rum-beacon from the access log endpoint while Fastly stats API counts them in edge_requests. Tried counting only logs table rows in _duckdb_row_counts_per_bucket and scanning only cache_dir/data/timestamp_hour=*/*.parquet for the active hour: caused a ~4.8% ingest gap on services with RUM synthetic traffic and under-counted active-hour committed DuckLake rows; selecting raw file_date in get_log_accounting_counts width=10 raised psycopg.errors.DatatypeMismatch on by=day.
