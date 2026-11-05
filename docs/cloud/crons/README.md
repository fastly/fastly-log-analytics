# Crons

`flp`'s tick-driven background jobs. Two health-check probes reach Compute about once a second each. On `/_tick` a dispatcher decides from a Core Cache schedule whether ingest, raw delete or a maintenance stage on `flp-worker` is due; on `/_flush` it decides whether the operational-log flush is due. Idle probes do nothing and write nothing.

This directory documents the code on `main` at `1006ff2`, not the plans. Each file cites `file:line`.

## Purpose

`flp` has no server, no scheduler process, and no state that survives between requests. Each "cron" here is one stage run during a single Compute execution: in `crate::admin::record_tick`, in `crate::admin::record_flush`, or in a `flp-worker` execution the tick dispatches. All state lives in KV, Core Cache and FOS.

## Why it exists

Compute only runs when a request arrives. The front VCL service supplies that request itself, so ingestion and housekeeping keep running without outside infrastructure.

## How it works

The clock and the tick orchestration are documented in [clock.md](clock.md). In summary:
- A VCL health check probes `/_tick` every second; VCL caches it for 1 s at the IAD shield, so about one probe a second reaches Compute. Compute answers 200 at once, then calls `crate::admin::record_tick` (`app/src/shell.rs:415-427`).
- `record_tick` claims the Core Cache schedule record `flp-sched-v1` with one atomic replace (`app/src/admin/schedule.rs`). Off-shield, idle and busy ticks stop there with no backend call. A due tick takes a 55 s lease and runs request_ingest (whose dispatch batch can carry one maintenance stage request) → raw_delete → rows → `cron:state` save → lease release.
- Ingest is due once per log period (`max(5, log_period)` s) after the period closes plus a 180 s grace, and on every tick while a backlog remains. Maintenance is due every 120 s. Durable per-job state is in KV `cron:state.jobs`; the cache record mirrors it and is rebuilt after an eviction.
- The maintenance stage runs on `flp-worker` (`/_internal/stage`): the first of retention_sweep → log_compact → delete_history → storage_reconcile, from `cron:state.maint_next`, that has work ([stage-worker.md](stage-worker.md)).
- A second health check probes `/_flush` the same way. `crate::admin::record_flush` claims its own record `flp-flush-sched-v1` and runs log_flush, alone in its execution, at most once a minute when idle and on the next probe while work remains.
- Each execution has 32 backend calls, and KV operations charge the budget too (`admin/transport.rs:35-75`). Each also has 64 Core Cache operations (`CACHE_OP_BUDGET`, `shell.rs:18`).
  - A stage is admitted only if its call need fits the remaining budget.
  - Stage loops stop starting new work 40 s into the execution.
  - Unfinished work resumes from KV on the next tick.
- Limits are audited in [compute-limits-audit.md](../compute-limits-audit.md). Operator procedures are in [runbook.md](../runbook.md).

| Cron | `cron_kind` | Runs when | File |
|---|---|---|---|
| The clock (`/_tick`) | none (idle ticks counted on the next `cron_runs` row) | Every second at the IAD shield; decides what is due | [clock.md](clock.md) |
| Request ingest | `request_ingest` | Once per log period after its grace, and every tick while a backlog remains | [request-ingest.md](request-ingest.md) |
| Ingest worker (`/_internal/ingest`) | none (reported by request_ingest) | Per ingest shard, dispatched by the coordinator to `flp-worker` | [ingest-worker.md](ingest-worker.md) |
| Raw delete | `raw_delete` | After a successful ingest, and every tick while deletes remain queued | [raw-delete.md](raw-delete.md) |
| The flush probe (`/_flush`) → log flush | `log_flush` | Every second at the shield; flushes at most once a minute when idle, back to back while work remains | [log-flush.md](log-flush.md) |
| Stage worker (`/_internal/stage`) | none (each stage writes its own row) | At most once per 120 s, dispatched with the ingest shards (or alone) to `flp-worker` | [stage-worker.md](stage-worker.md) |
| Log compact | `log_compact` | Its round-robin turn on the stage worker, when a day has work | [log-compact.md](log-compact.md) |
| Retention sweep | `retention_sweep` | Its turn, every 6 h and every turn until a sweep completes | [retention-sweep.md](retention-sweep.md) |
| Storage reconcile | `storage_reconcile` | Its turn, only after an admin request | [storage-reconcile.md](storage-reconcile.md) |
| Delete history | `delete_history` | Its turn, while an admin-started job is incomplete | [delete-history.md](delete-history.md) |

Provisioning, reconcile and teardown jobs (`job:<id>`) are not tick stages. They advance only through admin calls to `POST /api/admin/jobs/{id}/step` (`admin/mod.rs:214-223`).

## Roles and permissions

`/_tick` and `/_flush` carry only the front service key; they are limited to GET on the exact path, cached for 1 s and run only on the shield. `/_internal/stage` needs the worker peer key. Every manual control is admin-only. Details are in [clock.md](clock.md) and each cron file.

## Manual triggers and admin controls

`POST /api/cron-runs/trigger` runs a full tick right away (see [clock.md](clock.md)). Per-cron endpoints are listed in each file.

## Telemetry

Runs are recorded in:
- `cron:state.runs` (the last 50 runs, in KV);
- `cron_runs` (one row per stage that ran, with `idle_ticks` and `idle_vcpu_ms` counting the idle probes before it, and `worker_executions` counting the flp-worker requests this execution sent, plus the peer executions its ingest relays started).

Idle probes write nothing. `cron_ticks` is retired.

Field details are in [clock.md](clock.md).

## TODOs

Cross-cutting items (plan checkboxes, the wanted tick view and per-tick counters) are in [clock.md](clock.md). Job-specific items are in each file.

## Code

- `app/src/admin/schedule.rs`: the 1 s dispatcher's claim, due rules and release.
- `app/src/admin/mod.rs:1608-1660`, `1824-2335`: `record_flush`, `record_tick` and `dispatch`.
- `app/src/admin/stage.rs`; `app/src/admin/logs.rs:452-530`: the stage worker and `maintenance_stage`.
