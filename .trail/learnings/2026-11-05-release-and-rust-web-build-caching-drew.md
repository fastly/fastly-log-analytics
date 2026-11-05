---
title: Release and Rust/Web Build Caching and Parallelism
author: Drew
date: 2026-11-05
branch: main
cost: { minutes: 136, tokens: 733705 }
id: l-0ac2c4e5
scope: repo
status: active
src: agent
touches:
  - app/build.rs
  - app/Cargo.toml
  - app/Makefile
  - app/tools/release.py
  - app/tools/test_release.py
  - app/web/package.json
  - app/web/scripts/copy-duckdb-assets.mjs
---

Fixed 6 build bottlenecks in app/tools/release.py, app/build.rs, app/Cargo.toml, app/Makefile, and app/web/scripts/: (1) replaced mtime-based asset precompression in build.rs with content-addressed SHA-256 caching (.raw/.gz/.br) under FLP_ASSET_CACHE_DIR so git archive snapshots do not invalidate precompressed DuckDB WASM and web assets or leave dangling include_bytes! paths; (2) enabled opt-level = 3 for build-override profiles and parallelized asset precompression across up to 8 threads ordered largest-first; (3) removed lto = true and codegen-units = 1 from profile.release so LLVM codegen runs in parallel instead of spending ~208s single-threaded on Fat LTO; (4) stabilized unchanged source file mtimes across git archive snapshots via source-mtimes.json; (5) cached .next/cache, tsconfig.tsbuildinfo, and public/duckdb alongside node_modules keyed by package-lock.json SHA-256; (6) made copy-duckdb-assets.mjs and gen-types.mjs idempotent when inputs are unchanged.
