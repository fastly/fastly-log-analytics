---
title: Field search answers from summaries first
author: Drew
date: 2026-11-04
branch: main
cost: { minutes: 1, tokens: 2000 }
id: l-d0f922fe
scope: repo
status: active
src: agent
touches:
  - "app/web/lib/dashboard/engine.ts#searchFieldSummaries"
  - "app/web/lib/dashboard/engine.ts#collectAggregator"
  - app/web/components/dashboard/FieldSearchDialog.tsx
---

Top-10 field search was slow on open because searchFieldValues calls collectAggregator with one field and topK=null, which disables converter summaries (canUseSummaries needs topK and every card field), and the in-memory memo is off for the real browser source (incremental requires !source.openParquet), so it re-read every raw window through DuckDB-Wasm. Hours the dashboard answered from summaries have no cached FileRollup either. searchFieldSummaries answers from hour/day/window summaries plus cached rollups without raw reads and returns partial when a summary truncated the field (bundles keep top 500) or a window has neither; the dialog shows that, runs the exact scan only when partial, loads every value once (query "", unbounded limit) and filters as you type, so keystrokes never rescan.
