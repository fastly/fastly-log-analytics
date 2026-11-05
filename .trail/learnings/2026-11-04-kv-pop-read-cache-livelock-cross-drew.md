---
title: "KV POP read-cache livelock, cross-execution write pacing, and coordinator/compaction watermark self-collision"
author: Drew
date: 2026-11-04
branch: main
id: l-a77ee71c
scope: repo
status: active
src: agent
touches:
  - app/src/admin/transport.rs
  - app/src/ingest/bundle.rs
  - app/src/ingest/compaction.rs
  - app/src/ingest/coordinator.rs
  - app/src/sdk.rs
  - app/tests/ingest.rs
---

Fastly KV Store caches reads at edge POPs for up to 60s and only invalidates POP caches when a write succeeds (200 OK). A 412 Precondition Failed on if-generation-match does not invalidate the local POP cache, while frequent reads (/\_tick every 5s) keep the stale generation warm. Furthermore, native KV (kv.lookup) and REST KV (admin_get_fresh) format generation tokens differently for the same record (e.g. 784402847958667264 vs 1793781737172001), so comparing generation strings across transports causes false mismatches; always compare _kv_written_ms timestamps instead. In FastlyRuntime (app/src/sdk.rs), Core Cache (flp:kvcoord:v1:{key}) tracks last_write_ms for cross-execution 1.1s write pacing, last_written_value for monotonic value preservation, and dead_generation + dead_since_ms when ItemPreconditionFailed occurs; if a dead generation persists >= 2s across executions and REST does not have a newer _kv_written_ms, admin_cas paces 1.1s and uses InsertMode::Overwrite to force a 200 OK that purges the stuck POP cache. In coordinator::commit (app/src/ingest/coordinator.rs), re-read ingest:watermark after worker_dispatch when extras_sent is true so parallel stage::PATH (request_compact / summary_bundle) watermark bumps do not self-collide, and checkpoint completed worker shard results to ingest:plan on watermark conflict. In request_compact (app/src/ingest/compaction.rs), release lease_until=0 and save completed job state on compact_watermark_conflict.
