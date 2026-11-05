---
title: "Phase 4b /fastly-value latency, rollup schema & zero-CLS optimization"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-042ee718
scope: repo
status: active
src: agent
touches:
  - backend/repositories/value.py
  - backend/core/rollups/network_dims.py
  - backend/routers/performance.py
  - frontend/app/fastly-value/_sections/FastlyValueClient.tsx
  - tests/repositories/test_value_io_pricing.py
---

Phase 4b /fastly-value: (1) Added _fast_utc_bucket_select using date_trunc on UTC TIMESTAMP to avoid DuckDB per-row ICU calendar math on 30d TIMESTAMPTZ scans (10.1s -> <1s). (2) Added proto/tls/is_ipv6 schema support to build_network_summary_bundles and backend/repositories/value.py plus stale all-zero network_summary.parquet bundle fallback so HTTP/3, HTTP/2, TLS, and IPv6 percentages and protocol time series populate on standard parquet schemas. (3) Reused bucketed hit/miss latency from overview/caching rollup or combined raw bucket scan to eliminate redundant performance_latency_ts raw table scan, piggybacked p99_elapsed onto bots_agg_query, and skipped security_top_signals/bots_top_query when counts are 0. (4) Added ResponseMemo (_value_memo) + run_cached_repo_endpoint to /api/value/summary. (5) Added min-h-[820px] tab reservation and matching 2x4 KPI + 2 chart skeleton grid in FastlyValueClient.tsx to eliminate CLS (0.0254 -> 0.0000).
