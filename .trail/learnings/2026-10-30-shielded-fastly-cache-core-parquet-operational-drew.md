---
title: "Shielded fastly::cache::core + Parquet operational logging architecture"
author: Drew
date: 2026-10-30
branch: main
id: l-0b331049
scope: repo
status: active
src: agent
touches:
  - app/infra/provision.py
  - app/vcl/front/main.vcl
  - app/src/ingest/mod.rs
  - app/src/sdk.rs
---

Operational logs (7 LogKind streams: cron_runs, cron_ticks, ingest_files, fos_usage, admin_audit, api_telemetry, platform_errors) use fastly::cache::core at the iad-va-us shield POP for zsh real-time minute buckets (ReplaceStrategy::Wait) and flush closed buckets to per-kind Snappy Parquet files under processed/{kind}/. Shielding backend('flp', ..., shield=True) on flp-front requires moving 'unset resp.http.x-flp-auth-fail;' inside 'if (fastly.ff.visits_this_service == 0)' in app/vcl/front/main.vcl so auth rate-limit signals survive the shield-to-edge hop. Moving per-file ingest history from KV ('ingested:<raw_key>') to Core Cache + Parquet eliminates 16 KV Class A writes per tick (345,600/month).
