---
title: "Price CDN delivery from logged /fos/... storage_usage and computeBandwidthBytes, not front Stats API totals"
author: Drew
date: 2026-11-02
branch: main
id: l-4dac6168
scope: repo
status: active
src: agent
touches:
  - app/web/components/PublicPages.test.tsx
  - app/web/components/usage/UsageChartsGrid.tsx
  - app/web/components/usage/UsageClient.tsx
  - app/web/components/usage/UsagePage.test.tsx
  - app/web/components/usage/UsageStatCards.tsx
  - app/web/lib/usage/estimate.test.ts
  - app/web/lib/usage/estimate.ts
  - app/web/lib/usage/summary.test.ts
  - app/web/lib/usage/summary.ts
---

Fastly Stats API for the front VCL service (cdnRequests / cdnBandwidthBytes) counts all VCL self-healthcheck probes (/_tick and /_flush) and their response headers across POPs (~4.8M reqs/day, ~8-26 GB/day). Both incurredCost (app/web/lib/usage/summary.ts) and measureEstimate (app/web/lib/usage/estimate.ts) must price CDN delivery from logged /fos/... storage_usage (loggedCdnBytes, classB + cdnHits) plus computeBandwidthBytes rather than front's raw Stats API totals.
