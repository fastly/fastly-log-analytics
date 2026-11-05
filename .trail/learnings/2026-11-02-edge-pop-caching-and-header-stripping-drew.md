---
title: Edge POP caching and header stripping for /_tick and /_flush healthcheck probes
author: Drew
date: 2026-11-02
branch: main
id: l-06f19685
scope: repo
status: active
src: agent
touches:
  - app/infra/test_provision.py
  - app/src/main.rs
  - app/src/shell.rs
  - app/vcl/front/main.vcl
  - app/vcl/front/tests/front.test.vcl
  - app/web/components/usage/UsageClient.tsx
---

In app/vcl/front/main.vcl and clock_response (app/src/shell.rs), VCL healthcheck probes on /_tick and /_flush across ~56 edge POPs generated ~404,817 req/hr (~930.7 MB/hr VCL bandwidth) when edge POPs busted their cache key every second (?b=now.sec) and Compute returned the 1,340-byte Content-Security-Policy header + Vary: Cookie. Normalizing req.url = req.url.path at both edge and shield, subtracting shield Age from x-flp-tick-ttl at the edge, stripping security headers and Vary on clock_ack in app/src/main.rs, and plotting fos_usage.bytes_out in app/web/components/usage/UsageClient.tsx lets edge POPs cache for the remaining sleep interval with minimal response headers.
