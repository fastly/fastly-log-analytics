# ADR-18 — The Serving Tier Is Single-Pod (Open Gap)

**Status:** Proposed — documents a known gap, not a fix. Nothing in this ADR ships a change to the serving tier.
**Decided by:** v3.0.0 scalability review, 2026-09-01/02

## Context

[ADR-15](15-multi-writer-topology.md) and [ADR-16](16-ingest-ledger.md) made the **ingest** tier horizontally scalable: discovery and conversion fan out across Celery workers coordinated by a shared Postgres ledger and an atomic cron lease, and adding worker replicas adds throughput. That work is real and complete.

The **serving** tier — the backend Deployment that answers API requests — did not move with it, and nothing in the docs said so. The v3.0.0 release notes, the chart, and ADR-15's own framing all read as "multi-pod scalable architecture," which is true of ingest and false of serving. A reader who scales `replicaCount` to 2 on the strength of that gets a stack that fails in three independent ways, two of them silently. This ADR names them so the next reader doesn't have to rediscover them from a production incident.

Scope note: this is about running **N backend pods concurrently**. One backend pod plus N workers — the topology `docker-compose.multipod.yml` and the chart defaults actually describe — is supported and is what ADR-15 delivered.

## The three structural blockers

### 1. The per-service `.duckdb` file is process-exclusive

`backend/core/duckdb_pool.py` opens every pooled connection with `read_only=False`, and `get_connection` forces it, so cron write connections never conflict with pool connections *within a process*. Multiple connections to the same file from the **same** process share in-memory database state and are safe. A **second process** is not: DuckDB takes a process-exclusive lock on a read-write single-file database.

So two backend pods sharing the PVC that holds `data/services/{service_id}.duckdb` do not degrade — the second pod's every checkout fails with `Could not set lock on file`. The pool's retry loop classifies that as transient, exhausts `max_retries`, and raises `DBBusyError`, which `deps.py` maps to a 503. The result is one healthy pod and N-1 pods returning 503 on every data request, behind a Service that load-balances evenly across all of them. Users see intermittent total failure, and no single pod's logs explain it.

Read-only attach is not an escape hatch either: the writer path (cron, view rebind, buffer commit) needs write access, and DuckDB does not offer multi-process read-write on one file.

### 2. Pod-local parquet described by shared-Postgres bookkeeping

`local_compacted_files` and `committed_buffers` are metadata tables, so under `METADATA_DSN` they live in **shared** Postgres via `metadata.base.get_con`. The compacted parquet they describe is **pod-local** — it sits in that pod's cache directory on that pod's disk. Shared bookkeeping pointing at unshared files is the whole bug.

The cron lease makes it deterministic rather than merely racy. `local_compact_{id}` acquires a per-`(service, job)` lease (`start_cron_run(src, "local_compact")`, `backend/cron/jobs/compaction.py:86`), so exactly one pod runs local compaction, writes the merged output to its own disk, and registers the merged-away source basenames in shared Postgres. Every **other** pod then reads those basenames back out through `get_locally_compacted_basenames` (`backend/core/iceberg/sync.py:151`) and treats them as intentionally absent locally — `sync.py:170` and `sync.py:437` both exclude them from the "missing — re-download" check. So the other pods never re-download the source files, and never produced the merged output that was supposed to replace them. Those rows are simply gone from their view of the data. No error, no warning; the sync tick reports success.

This is [AGENTS.md](../../AGENTS.md) trap #21's production incident (1.65M → 302K rows on 2026-05-31) replayed with **pods** substituted for **time**. The mechanism is identical: the registry suppresses re-download of files whose merged replacement does not exist. The 2026-05 fix made the merged output survive orphan-cleanup on the one machine that wrote it; it did nothing about a machine that never wrote it, because at the time there was only ever one.

### 3. The cron lease inverts for jobs whose output is pod-local

ADR-15 §4 built the lease as a cross-pod mutex, and for jobs writing to a **shared** target that is exactly right — `optimize` (`optimize.py:41`), `expire_snapshots` (`expire.py:29`), and alert-notification dedup must run once per tick fleet-wide, and the partial unique index on `job_runs` guarantees that.

For jobs whose output is **pod-local**, the same mutex guarantees the opposite of what is needed: it ensures N-1 pods never produce their own copy of output they each require. Every one of these takes the lease:

| Job | Lease site | Pod-local output |
|---|---|---|
| `local_compact` | `compaction.py:86` | merged parquet in the pod's cache |
| `rollup_compact_daily` | `compaction.py:301` | per-day rollup parquet |
| `rollup_hour_heal` | `compaction.py:218` | per-hour rollup parquet |
| `insights_prewarmer` | `insights_prewarmer.py:136` | pod-local warm caches |
| `metadata_sync` | `metadata.py:126` | pod-local caches + FOS state flush |
| `ngwaf_sync` | `metadata.py:388` | pod-local `ngwaf_bot_cache` SQLite |

The rollup jobs are the worst case, because rollups are a *performance* mechanism with a working fallback. One pod ends up with bundled rollups; the rest find no bundle and fall back to scanning raw parquet — the same runtime fallback described in trap #24, which exists precisely because it is much slower than the bundled read. Nothing is wrong-looking on those pods: correct answers, silently and dramatically slower, exactly where the 1M-RPS target needs rollups most. A latency regression that appears on a fraction of requests proportional to `(N-1)/N` and cannot be reproduced by hitting one pod directly is close to the worst possible shape for on-call.

Note the split ADR-15 §3 describes is between RedBeat-routed and APScheduler-routed jobs, and it is correct as designed — these jobs stay on the in-process APScheduler *on purpose*, so they touch the right pod's files. The defect is not the routing. It is that the lease, taken inside the job body, is scoped fleet-wide while the job's effect is scoped to one pod. Correct behavior for this class is a **per-pod** lease (a pod identity in the lease key), or no lease at all, and no such option exists today.

## Options considered

1. **Do nothing, document it.** What this ADR does. The serving tier is single-pod; that is a real constraint of the storage engine, not an oversight to be patched away cheaply.
2. **Make the lease key pod-aware for the pod-local job class.** Smallest change that would let N pods each maintain their own rollups/caches. Does nothing about blockers 1 or 2, so it is necessary-but-insufficient and should not be built first — it would make the fleet *look* healthier while blocker 1 still 503s.
3. **Move serving off the per-service DuckDB file** — query DuckLake/object storage directly from stateless pods with no pod-local `.duckdb`, no pod-local parquet cache, and rollups in shared storage. This is the only option that actually scales the serving tier, and it is a substantial redesign that would supersede large parts of ADR-01, ADR-03 and ADR-06 (writer-driven view warming assumes a warm pod-local file). It is the honest direction, not a follow-up ticket.
4. **Shard by service.** Route each service's traffic to a designated pod via consistent hashing at the ingress, so a `.duckdb` file is only ever opened by one process. Scales the fleet with the number of *services* rather than with per-service load, needs session-independent routing at the ingress, and turns single-pod loss into per-service outage. Plausible middle path; unvalidated.

## Decision

Record the constraint and make it hard to violate by accident. Concretely, alongside this ADR:

- The backend Deployment in `deploy/chart/fastly-log-analytics/` is pinned to one replica and no longer has an HPA. Previously `.Values.autoscaling.enabled` gated the backend HPA **and** the worker KEDA ScaledObject together, so the only lever for scaling the (scalable) worker fleet also scaled the (unscalable) backend to `maxReplicas: 10`. Worker autoscaling stays on that flag; the backend is not offered as a knob, because there is no value of it that works. Pinned by `tests/chart/test_helm.py`.
- `backend/core/duckdb_pool.py` carries a pointer here at the `read_only=False` note, so the next reader of "multi-pod scalable architecture" does not infer that the serving tier scales.

No code change to the serving tier itself. Option 3 is the direction; it needs its own ADR and design.

## Consequences

- `replicaCount` in the chart applies to the stateless frontend only. Scaling the backend requires resolving this ADR first, not editing a value.
- Any future work that says "scale out the backend" must address blockers 1–3 together. Fixing one in isolation makes the failure quieter, not rarer.
- New cron jobs must be classified by **where their output lands**, not only by ADR-15 §3's RedBeat/APScheduler routing. A job taking the shared lease while writing pod-local output is the bug in blocker 3, and it is currently easy to write by copying any existing job body.

## Lower-severity documented gaps (not fixed here)

These are recorded so they are not rediscovered as mysteries. None is worth fixing while the serving tier is single-pod, and each becomes load-bearing the moment it is not.

- **Cron progress is in-process memory.** `backend/cron_progress.py` is a bare module-level dict (`_progress`/`_last_update`/`_run_metadata` guarded by a `threading.Lock`). In external mode the producers are Celery workers and the consumers are backend pods, in different processes entirely — so the admin active-runs panel and the sync-status activity text are blind to all ingest work. They are not wrong, they are empty, which reads as "nothing is running" during an active sync. The durable `cron_runs` rows are unaffected; only the live progress stream is lost.
- **Two stores bypass the Postgres seam entirely.** `backend/core/metric_snapshots.py` (`data/system/system_metrics.db`) and `backend/core/metadata/usage_log_db.py` are plain SQLite with no `pg_connection.is_postgres()` routing, so unlike every other metadata store they stay pod-local under `METADATA_DSN`. Admin System Health trends and the usage log therefore describe whichever pod served the request.
- **`metric_snapshot` takes no lease.** `backend/cron/jobs/metric_snapshot.py` never calls `start_cron_run`, so all N pods sample every 60s and write into a key with no pod dimension — N interleaved series stored as one. Harmless at N=1, meaningless above it. Note this is the *correct* behavior for a pod-local sampler and the wrong storage shape for it; it is blocker 3's mirror image.
- **`committed_buffers` was missing from the Postgres schema.** It is created by SQLite migration 004, not by `_SCHEMA`, and `scripts/setup_pg_schema.py` only translates `_SCHEMA` plus explicit DDL — so Postgres metadata mode had no `committed_buffers` table at all, silently losing the durable checkpoint that stops a crash between `table.append()` and `tombstone_buffer_files()` from re-appending the same rows. `pg_connection._IGNORE_TABLES` already listed the table, i.e. the dialect shim was written assuming it existed. DDL added to `setup_pg_schema.py` alongside this ADR, since `docker-compose.multipod.yml` now runs on Postgres metadata and would otherwise have regressed.
- **`service_id` is not on every metadata table.** `pg_connection`'s module docstring claimed migration 015 added `service_id` to every per-service table; it added it to `cron_runs` and `local_compacted_files` only. `committed_buffers` has no `service_id` and `filter_uncommitted_buffers` applies no such predicate, so that table is cross-tenant by buffer basename under a shared database — safe only because those basenames are uuid-derived. Docstring corrected; the schema was not changed.
- **Chart deployment defaults CrashLoop — CLOSED.** `values.yaml` shipped `config.ingestMode: celery` with `config.ducklakeCatalog: ""` and `secrets.metadataDsn: ""`, so a default `helm install` booted a backend, worker and beat that `validate_ingest_mode()` correctly refused to start. The gate was behaving as designed; the defaults were wrong. Resolved after this ADR was written: the chart now defaults to `config.ingestMode: sync` (single-node, no external datastores, so a bare `helm install` is deployable), renders the worker/beat/KEDA objects only in celery mode, and validates the celery prerequisites in `templates/validate.yaml` — so selecting celery without a Postgres `ducklakeCatalog`/`metadataDsn`/broker is a `helm template` error naming the missing value, not three CrashLoopBackOffs. Pinned by `tests/chart/test_helm.py`.

## Out of scope

- Implementing option 3 (or 2, or 4). Each needs its own ADR.
- Anything about the ingest tier, which scales as ADR-15/ADR-16 describe.

(The chart's `ingestMode`/`ducklakeCatalog` default mismatch was out of scope when this ADR was written and has since been fixed — see the gap list above.)
