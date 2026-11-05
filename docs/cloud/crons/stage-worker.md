# stage worker (`/_internal/stage`)

Runs one maintenance stage per clock tick in its own `flp-worker` execution. The stages are retention sweep, log compaction, delete-history and storage reconcile. The tick sends the stage request in the same parallel batch as its ingest shard workers, so maintenance costs the tick one backend call.

## Purpose

Keeps maintenance out of the clock execution. The tick's 32 calls go to ingest and raw delete on every bucket, and each maintenance stage gets a full worker execution with its own:
- 32-call budget;
- wall time;
- per-execution Core Cache transaction limit.

## Why it exists

Before F4, maintenance ran at the end of the tick on whatever calls ingest left, then on fixed bucket slots (9c8e1ff) that made ingest skip or reserve calls on maintenance buckets. Live, the flush's catch-up scan in the same execution exhausted Fastly's Core Cache transaction limit (`{"diagnostic":"instance_limit","limit":"cache_transactions"}`). After that, every FOS send failed with `LIMIT_EXCEEDED`, so `retention_sweep` and `log_compact` failed with `backend_internal_error`. The flush now has its own probe ([log-flush.md](log-flush.md)). Maintenance runs here, never in the same execution as the flush.

## How it works

**Dispatch** (`record_tick_inner`, `app/src/admin/mod.rs:1752-1990`)
1. On every non-busy tick with FOS credentials, the tick builds a `StageRequest`:
   - `service_id`, `run_id`, `tick_id` (the tick row's `event_id`, fixed up front), `trigger`, `actor`, `bucket`;
   - `start = cron:state.maint_next`.
2. It passes the request to the ingest coordinator as `Options.extra` (`mod.rs:1840-1860`). The coordinator sends it with the shard workers in one `worker_dispatch` batch. When there is no ingest work, the batch carries only the stage request. Dispatch happens only within the first 10 s of the tick (`DISPATCH_BY`). The coordinator waits for replies until `DRAIN_AT` (32 s).
3. Ingest reserves `RAW_DELETE_CALLS` (5) for raw delete. Raw delete keeps `TICK_TAIL` (4) for the tick row, the `cron:state` CAS and its conflict retry. Shards, the stage request and the coordinator's own calls share the rest. The tick never skips ingest for maintenance.

**Worker** (`handle_worker_stage`, `app/src/admin/stage.rs:76-146`; route in `sdk.rs:1052-1053`)
1. Requires the `x-flp-worker-key` peer secret (`ingest::worker::authorized`), like `/_internal/ingest` and `/_internal/delete`. A bad key gets 403, and a malformed or invalid body gets 400.
2. Loads the installed service; a different `service_id` replies `not_installed` with `next = start`. Reads the FOS credentials.
3. Backdates its wall clock so every storage helper stops starting FOS calls at `STAGE_STOP` (20 s), well before the tick stops waiting.
4. Runs `logs::maintenance_stage` (`admin/logs.rs:473-525`):
   - Tries the `MAINT_STAGES` (`logs.rs:452-457`) in round-robin order from `start`: `retention_sweep`, `log_compact`, `delete_history`, `storage_reconcile`.
   - A stage with nothing to do costs one or two reads and passes the turn on in the same execution.
   - The first stage with work runs, and no other stage runs after it.
   - `next` = the stage after the one that ran, or `start` if none ran.
5. Writes that stage's `cron_runs` row itself, through the KV `logq_{sid}_{bucket}` outbox, with buffer `{run_id}-maint` and `tick_event_id` = the tick's id. The row is recorded exactly once whether or not the tick hears the reply.
6. Replies `{status, ran, next, rows, calls, cache_ops, vcpu_ms}`. `rows` carries the execution's `fos_usage` and `api_telemetry` rows (`telemetry::api::runtime_rows`). The tick writes them into its own buffer. They are best-effort: lost if the tick stops waiting first.

**Round-robin** (`cron:state.maint_next`, `maint_misses`; `admin/mod.rs:2108-2122`)
- A stage reply sets `maint_next = next` and clears `maint_misses`.
- No reply (`worker_deadline`, error or a non-stage body) increments `maint_misses`. After `MAINT_MISS_LIMIT` (2) misses in a row, the pointer moves on by one. A stage whose worker always times out therefore cannot block the others.
- **Fairness bound:** every stage with work runs within 4 answered ticks of the last one, and within 8 ticks when every turn misses twice. The F5 simulation measured at most 2 ticks between turns for delete-history and 1 for reconcile, at 52 raw files per 2-minute period.

**Stage gates** (unchanged from the in-tick maintenance)
- `retention_sweep`: every 6 h (`SWEEP_INTERVAL_S`), and on every turn until a started sweep completes. See [retention-sweep.md](retention-sweep.md).
- `log_compact`: when `compaction_enabled` is on and a day has work. See [log-compact.md](log-compact.md).
- `delete_history`: while an admin-started job is incomplete. See [delete-history.md](delete-history.md).
- `storage_reconcile`: only after an admin request. See [storage-reconcile.md](storage-reconcile.md).

## Roles and permissions

Peer-only: the route exists only on `flp-worker` and needs the `worker_secret` key. `flp` never calls its own domain.

## Manual triggers and admin controls

- `POST /api/cron-runs/trigger` runs a manual tick, which dispatches one stage request like a clock tick.
- The per-stage manual endpoints (sweep, delete-history, reconcile request) still run in the admin request and are documented in each file.

## Telemetry

- `cron_runs` rows written by the worker (`cron_kind` = the stage name; `run_id` `{tick run_id}-{kind}`; `tick_event_id` links them to the dispatching tick).
- `fos_usage` and `api_telemetry` rows returned to the tick.
- The tick's own rows cover only its own stages (`request_ingest`, `raw_delete`).

## Constraints and gotchas

- One stage per tick. A stage that keeps finding work (a long compaction backlog) still yields after its turn.
- The worker's `cache_ops` are reported in the reply. Maintenance makes no Core Cache lookups except its own telemetry inserts, which count against `CACHE_OP_BUDGET` (64).
- Measured in the F5 simulation: at most 30 backend calls and 0 cache ops per stage execution.

## Failure modes and recovery

- **Worker timeout:** the tick counts a miss. The worker may still finish and write its row. Each stage's KV state resumes the work on its next turn.
- **Worker error reply:** counted as a miss (no `next`).
- **Clock stops:** no stage runs. See [clock.md](clock.md).

## Tests

Native only; none is live evidence.
- `app/tests/ingest.rs`: `simulation::every_maintenance_stage_gets_a_turn_within_four_ticks_under_load`, and the stage tests that drive `record_tick` with `worker_dispatch` running `handle_worker_stage` in a fresh execution.

## Code

- `app/src/admin/stage.rs`: request, reply parsing, handler.
- `app/src/admin/logs.rs:440-540`: `TickContext`, `MAINT_STAGES`, `maintenance_stage`.
- `app/src/admin/mod.rs:1509-1527`, `1752-2122`: constants, dispatch and round-robin.
- `app/src/sdk.rs:1052-1053`: worker route.
