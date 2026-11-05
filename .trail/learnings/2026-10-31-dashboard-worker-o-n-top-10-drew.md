---
title: Dashboard worker O(N) top-10 selection with shared Intl.Collator
author: Drew
date: 2026-10-31
branch: main
id: l-27610155
scope: repo
status: active
src: agent
touches:
  - "app/web/lib/dashboard/engine.ts#topEntries"
  - "app/web/lib/dashboard/engine.ts#Aggregator"
---

String.prototype.localeCompare(other, undefined, { numeric: true }) constructs a new ICU Intl.Collator on every call; sorting full high-cardinality frequency Maps ([...counts].sort) in the dashboard worker caused ~57M comparator calls and locked the worker for ~170s after all 222 Parquet files (930k rows) finished downloading. Use a single-pass O(N) top-K selector with a shared module-level Intl.Collator.
