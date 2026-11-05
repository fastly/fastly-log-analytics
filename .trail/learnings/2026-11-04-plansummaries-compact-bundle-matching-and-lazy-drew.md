---
title: planSummaries compact= bundle matching and lazy microFile OPFS reservation
author: Drew
date: 2026-11-04
branch: main
cost: { minutes: 20, tokens: 120000 }
id: l-22d75728
scope: repo
status: active
src: agent
touches:
  - app/web/lib/dashboard/summaries.ts
  - app/web/lib/dashboard/engine.ts
---

planSummaries in app/web/lib/dashboard/summaries.ts must match single- and multi-pass request compact= files to hour_bundle and day_bundle by exact row count (and sources <= bundle sources), and readSummary in app/web/lib/dashboard/engine.ts should reuse in-memory decodedSummaries and defer microFile OPFS checks until summary decode fallback; otherwise compacted hours fall back to raw compact= downloads and boot 35MB duckdb-eh.wasm on every unfiltered dashboard load.
