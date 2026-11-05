---
title: High-scale request facts emulation caching and zero-CLS card layout
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-34315527
scope: repo
status: active
src: agent
touches:
  - backend/routers/admin/compaction.py
  - backend/routers/admin/high_scale.py
  - frontend/app/high-scale/request-facts/page.tsx
  - scripts/dev/profile_dashboard.cjs
  - scripts/lib/dashboard_paint.cjs
  - scripts/lib/section_measurements.cjs
  - tests/high_scale/test_query_service.py
  - tests/scripts/analyst_lifecycle_cli.test.cjs
  - tests/scripts/browser_paint.test.cjs
  - tests/scripts/section_measurements.test.cjs
---

## Decided
- In `frontend/app/high-scale/request-facts/page.tsx` (`HighScaleRequestFactsPanel`, `useHighScaleRequestFacts`, `Metadata`), give `high-scale-loading-card` and `high-scale-facts-card` distinct `key` props (`key="loading"` and `key="loaded"`), match their 1-line `<CardHeader>` height, remove `[content-visibility:auto]` from above-the-fold `Metadata` and `high-scale-filters`, and add `placeholderData: (previousData) => previousData` to `useHighScaleRequestFacts` so filter typing and cursor pagination do not tear down the loaded card.
- In `backend/routers/admin/compaction.py` (`fetch_request_facts_emulation`) and `backend/routers/admin/high_scale.py` (`_execute_request_facts_query`), cache table schemas (`_EMULATION_SCHEMA_CACHE`), fallback extents (`_EMULATION_EXTENTS_CACHE`), and responses (`_REQUEST_FACTS_CACHE`), and tighten partition pruning on cursor pagination via `effective_start = max(start, cursor_ts)`.

## Tried and ruled out
- Leaving `[content-visibility:auto]` on `Metadata` (`[contain-intrinsic-size:auto_88px]`, actual 66px) and `high-scale-filters` (`[contain-intrinsic-size:auto_48px]`, actual 32px): because both are above the fold, the browser laid out `high-scale-table-wrapper` at `y: 585` using the intrinsic heights and shifted it up 38px (`y: 547`) 1ms later when layout resolved.
