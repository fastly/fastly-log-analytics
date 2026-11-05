---
title: Page 11 /control-room High-Scale correlate waf_sig and zero-CLS fixes
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-420621a4
scope: repo
status: active
src: agent
touches:
  - backend/high_scale/aggregate_query.py
  - frontend/components/SyncStatusBadge/SyncStatusBadge.tsx
  - frontend/components/PlotlyChart/index.tsx
  - frontend/app/control-room/_sections/ControlRoomWidgets.tsx
---

High-Scale POST /control-room/correlate failed with HTTP 400 on dimension=waf_sig because _REQUEST_CUSTOM_FIELD_DIMENSIONS in backend/high_scale/aggregate_query.py omitted waf_sig, waf_req_id, ost, oip, oretries, rid, and prid. Browser CLS (0.0091) on /control-room came from SyncStatusBadgeInner returning null while status/headerBadge was undefined (collapsing the 409px header reservation), PlotlyChart scaling height by 1.2x after narrow ResizeObserver measurement, and ControlRoomWidgets badges/metric cards shifting width/height on live tick arrival.
