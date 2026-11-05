---
title: Browser DuckDB-Wasm analytical engine and persistent FileRollup cache
author: Drew
date: 2026-10-31
branch: feat/usage-fla-parity
id: l-daf91386
scope: repo
status: active
src: agent
touches:
  - app/web/lib/dashboard/duckdb.ts
  - app/web/lib/dashboard/engine.ts
  - app/web/lib/dashboard/sources.ts
  - app/web/lib/dashboard/client.ts
  - app/web/e2e/serve.mjs
---

Unfiltered dashboard queries build a per-file FileRollup once when a Parquet file is first decoded and persist it in CacheStorage (flp-parquet-rollup-v1) so range switches and reloads merge file summaries in O(files) without re-decoding Parquet. Filtered queries run SQL against an in-memory DuckDB-Wasm columnar table (requests) held inside a persistent singleton Web Worker (engine.worker.ts). In browser workers, instantiate DuckDB-Wasm with origin-qualified URLs (self.location.origin + '/duckdb/...') because relative paths resolve against the worker script URL, and serve .wasm with Content-Type: application/wasm in both app/src/site.rs and app/web/e2e/serve.mjs so Chromium's WebAssembly.instantiateStreaming succeeds.
