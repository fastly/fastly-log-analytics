---
title: Logs page TTL caches and zero-CLS reservations
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-f49fd3b0
scope: repo
status: active
src: agent
touches:
  - backend/routers/admin/compaction.py
  - backend/routers/admin/downloads.py
  - backend/routers/admin/sync_status.py
  - backend/routers/services/views.py
  - frontend/app/logs/_sections/LogsClient.tsx
  - frontend/app/logs/_sections/LogsCronConsole.tsx
  - frontend/app/logs/_sections/LogsCronSchedule.tsx
  - frontend/app/logs/_sections/LogsFileBrowser.tsx
  - frontend/components/SessionScoring/StoragePanels.tsx
  - scripts/dev/profile_dashboard.cjs
---

On /logs (Page 16), StorageCalendarResponse is a RootModel mapping date strings to StorageCalendarDay | list[Any], so debug_envelope in storage_calendar_endpoint must keep is_cached=None rather than boolean; also CronScheduleBox NextRunCountdown inside a justify-between flex row causes horizontal CLS ticks unless given a fixed width (w-[62px] text-right shrink-0 tabular-nums), and StorageStatus/StorageCalendar/MetadataStorageCard must be statically imported in LogsClient rather than dynamic(..., ssr: false) without fallback.
