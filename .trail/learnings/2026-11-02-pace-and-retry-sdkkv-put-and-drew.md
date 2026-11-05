---
title: "Pace and retry SdkKv::put and admin_cas against KV 1-write/sec per-key limit"
author: Drew
date: 2026-11-02
branch: main
id: l-1ff3d211
scope: repo
status: active
src: agent
touches:
  - app/src/state/fastly.rs
  - app/src/state.rs
  - app/src/sdk.rs
---

SdkKv::put (auth:state) and FastlyRuntime::admin_cas need 1-write/sec rate-limit tracking (@written_ms metadata in SdkKv, _kv_written_ms in admin_cas, plus 1100ms retry on transient KV errors) because Core Cache read-through makes back-to-back admin routes finish in <200ms and hit KV's 1 write/sec per-key limit on auth:state.
