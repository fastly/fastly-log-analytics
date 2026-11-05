---
title: RUM Live Activity Monitor POP geo and session TTFB fallback
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
cost: { minutes: 6, tokens: 80031 }
id: l-6f49d787
scope: repo
status: active
src: agent
touches:
  - scripts/dev/measure_e2e_freshness.py
  - tests/scripts/test_environments.py
  - scripts/dev/profile_dashboard.cjs
  - tests/scripts/section_measurements.test.cjs
  - scripts/lib/dashboard_card_paint.cjs
  - scripts/lib/dashboard_map_paint.cjs
  - tests/scripts/dashboard_card_paint.test.cjs
  - tests/scripts/dashboard_map_paint.test.cjs
  - frontend/__tests__/components/Map/NetworkMap.test.tsx
  - frontend/components/Map/NetworkMap/MapLayer.tsx
---

HTTP RUM beacons hitting Fastly 301 Force-TLS redirect before priority-10 vcl_recv had pop populated via server.datacenter while city/region/country/tls were empty, and _parse_rum_line / rum_live_events coerced missing geo/ttfb to Unknown/None. Added resolve_pop_geo_fallback + clean_edge_meta_str in backend/utils/pop_utils.py, beacon/session TTFB and query-param fallback in rum_ingest.py + rum/analytics.py + high_scale/rum.py (including treating ClickHouse non-Nullable 0.0 ttfb as None in _ttfb_ms), RUM custom field merge in vcl_builder.py, cleanRumField/formatRumEnvironment in RumTablesSection.tsx, and guarded setPaintProperty('countries', 'fill-color-transition') in MapLayer.tsx against async loadWorldGeoJson resolution.
