---
title: Per-file CacheStorage rollups and bounded memoryRollups prevent O(N^2) Worker hangs
author: Drew
date: 2026-10-31
branch: main
id: l-49523b06
scope: repo
status: active
src: agent
touches:
  - app/web/lib/dashboard/engine.test.ts
  - app/web/lib/dashboard/engine.ts
  - app/web/lib/dashboard/sources.test.ts
  - app/web/lib/dashboard/sources.ts
---

Do not combine per-file FileRollup objects into a single monolithic CacheStorage JSON snapshot (/__flp_rollup_snapshot?v=1) in browserSource or retain all FileRollup objects in pendingRollups/memoryRollups unbounded. On high-cardinality 1.13M-row datasets (376 files, ~617 KB–1.1 MB JSON per FileRollup, ~207 MB total), queueMicrotask snapshot persistence fires after every async CacheStorage match/put macrotask, writing ~39 GB of JSON (O(N^2)) and stalling/OOMing the dashboard Worker. Instead, persist one CacheStorage entry per file key via rollupCacheUrl, bound in-memory rollups by row count (MAX_MEMORY_ROLLUP_ROWS = 200_000), write rollups as each file finishes, and emit onProgress once any file is uncached or elapsed time exceeds 250ms.
