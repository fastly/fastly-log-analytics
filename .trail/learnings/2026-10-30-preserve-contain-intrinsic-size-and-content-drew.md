---
title: "Preserve contain-intrinsic-size and content-visibility:visible on admin reservation containers"
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-8da6e206
scope: repo
status: active
src: agent
touches:
  - frontend/app/admin/queries/_sections/QueryMonitorClient.tsx
  - frontend/app/admin/queries/_sections/QueryMonitorSections.tsx
  - frontend/app/admin/queue/_sections/QueueLedgerInspector.tsx
  - frontend/app/admin/queue/_sections/QueueOverviewPanels.tsx
  - frontend/app/admin/queue/_sections/QueueWorkerAndSchedulePanels.tsx
  - frontend/app/admin/usage-log/_sections/UsageLogClient.tsx
  - frontend/app/admin/usage-log/_sections/UsageLogTableAndCards.tsx
  - frontend/components/ui/skeleton.tsx
  - frontend/e2e/helpers.ts
---

`assertIntrinsicReservations` in `frontend/e2e/helpers.ts` asserts both `expect(classAttr).toContain('contain-intrinsic-size')` and `expect(classAttr).toContain('content-visibility')` on reservation containers in `/admin/queries`, `/admin/queue`, `/admin/trends`, and `/admin/usage-log`. Using `[contain-intrinsic-size:auto_...] [content-visibility:visible]` satisfies the E2E class-attribute contract while keeping `content-visibility: visible` so off-screen/on-screen layout is never deferred and `CLS = 0.0000`.
