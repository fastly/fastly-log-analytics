---
title: "Network map playback, bucket step alignment, ASN filter, per-bucket city limits, and zero-CLS fixes"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-6966d769
scope: repo
status: active
src: agent
touches:
  - backend/repositories/network.py
  - backend/high_scale/network.py
  - frontend/app/network/_sections/NetworkClient.tsx
  - frontend/components/Map/NetworkMap/index.tsx
  - frontend/components/Map/NetworkMap/MapLayer.tsx
---

On /network, map time-range/step/speed/ASN bugs had 6 root causes: (1) NETWORK_MAP_SECTIONS in NetworkClient.tsx omitted 'buckets' so selecting a specific ASN blanked the map with 'No map data available'; (2) SQL.MAP_BY_COUNTRY_BUCKET in backend/repositories/network.py had a global LIMIT 1000 across all buckets instead of QUALIFY ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY reqs DESC) <= 50, starving 7d/30d buckets; (3) try_network_heatmap_from_rollup returned 1-hour rollup rows even when sub-hour map steps (5m/15m) were requested; (4) initial bucketIdx in NetworkMap/index.tsx selected the trailing active-hour bucket even when its cities array was empty; (5) metric was in React Query keyParts and backend cache key causing redundant refetches on client-side color metric changes; (6) StatCard and missing-transport-fields-banner caused 0.0507 CLS.
