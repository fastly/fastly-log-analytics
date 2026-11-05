---
title: Direct Front VCL CDN /api/file serving via flp_cdn cookie and exact ts_ms latest_log_at bounds
author: Drew
date: 2026-11-04
branch: main
id: l-347a2c81
scope: repo
status: active
src: agent
touches:
  - app/src/auth/mod.rs
  - app/src/auth/session.rs
  - app/src/ingest/api.rs
  - app/src/ingest/catalog.rs
  - app/src/main.rs
  - app/src/shell.rs
  - app/tools/deploy_test_all.py
  - app/vcl/front/main.vcl
---

1. /api/file GET|HEAD requests are authenticated and rewritten directly in Front VCL (vcl_recv) to /fos/<sid>/<key> using a short-lived HMAC-SHA256 cookie (flp_cdn) bound to the active service_id, role, scope prefix, and SHA-256 prefix of flp_session, bypassing Compute while preserving segmented caching and S3 SigV4 signing. 2. Concurrent readLogs(kind) Web Worker calls are serialized per kind in app/web/lib/telemetry/client.ts so parallel callers populate CacheStorage once instead of fetching the same ops Parquet files twice. 3. catalog::Statistics tracks earliest_log and latest_log from Parquet ts_ms column statistics (and clamps open window bounds to now) so latest_log_at never reports a future window end time. 4. CostCalculator compaction saved file reads offset only front CDN requests and Class B reads since /api/file no longer invokes Compute.
