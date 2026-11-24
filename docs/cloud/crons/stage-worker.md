# stage worker (`/_internal/stage`)

Runs one maintenance stage per clock tick in its own `flp-worker` execution. `MAINT_STAGES` lists seven stages, in this order: retention sweep, log compaction, delete-history, storage reconcile, summary bundle, request compaction and Delete Data. Request compaction is in the list only for its cron row and schedule entry: it runs as its own compaction job, not in the rotation. The tick sends the stage request in the same parallel batch as its ingest shard workers, so maintenance costs the tick one backend call.

## Purpose

Keeps maintenance out of the clock execution. The tick's 32 calls go to ingest and raw delete on every bucket, and each maintenance stage gets a full worker execution with its own:
- 32-call budget;
- wall time;
- per-execution Core Cache transaction limit.

## Why it exists

Before F4, maintenance ran at the end of the tick on whatever calls ingest left, then on fixed bucket slots (9c8e1ff) that made ingest skip or reserve calls on maintenance buckets. Live, the flush's catch-up scan in the same execution exhausted Fastly's Core Cache transaction limit (`{"diagnostic":"instance_limit","limit":"cache_transactions"}`). After that, every FOS send failed with `LIMIT_EXCEEDED`, so `retention_sweep` and `log_compact` failed with `backend_internal_error`. The flush now has its own probe ([log-flush.md](log-flush.md)). Maintenance runs here, never in the same execution as the flush.

## How it works

**Dispatch** (`dispatch`, `compute/cloud/src/admin/mod.rs:2375-2417`)
1. On every non-busy tick with FOS credentials, the tick builds a `StageRequest`:
   - `service_id`, `run_id`, `tick_id` (the tick row's `event_id`, fixed up front), `trigger`, `actor`, `bucket`;
   - `start = cron:state.maint_next`.
2. It passes the request to the ingest coordinator as `Options.extra` (`mod.rs:2411-2417`). The coordinator sends it with the shard workers in one `worker_dispatch` batch. When there is no ingest work, the batch carries only the stage request. Dispatch happens only within the first 10 s of the tick (`DISPATCH_BY`). The coordinator waits for replies until `DRAIN_AT` (32 s).
3. Ingest reserves `RAW_DELETE_CALLS` (5) for raw delete. Raw delete keeps `TICK_TAIL` (4) for the tick row, the `cron:state` CAS and its conflict retry. Shards, the stage request and the coordinator's own calls share the rest. The tick never skips ingest for maintenance.

**Worker** (`handle_worker_stage`, `compute/cloud/src/admin/stage.rs:84-159`; route in `sdk.rs:2249-2250`)
1. Requires the `x-flp-worker-key` peer secret (`ingest::worker::authorized`), like `/_internal/ingest` and `/_internal/delete`. A bad key gets 403, and a malformed or invalid body gets 400.
2. Loads the installed service; a different `service_id` replies `not_installed` with `next = start`. Reads the FOS credentials.
3. Backdates its wall clock so every storage helper stops starting FOS calls at `STAGE_STOP` (20 s), well before the tick stops waiting.
4. Runs `logs::maintenance_stage` (`admin/logs.rs:736-808`):
   - First advances a pending Delete Data job (`reset::reset_stage`) while within the wall time. That step writes its own `delete_data` row; when it ran, the rotation's `delete_data` entry is skipped this turn.
   - No rotating stage starts once the execution has used more than `ROTATING_VCPU_MS` (300 ms) of vCPU, or once the wall time is spent.
   - Then tries the `MAINT_STAGES` in round-robin order from `start`:
     1. `retention_sweep`
     2. `log_compact`
     3. `delete_history`
     4. `storage_reconcile`
     5. `summary_bundle`
     6. `request_compact`: always skipped here; it runs in the compaction job (`compact_execution`, dispatched as a compact-only `StageRequest`, see [clock.md](clock.md)).
     7. `delete_data`
   - While a Delete Data job is fencing (`reset::fencing`), `retention_sweep`, `storage_reconcile` and `summary_bundle` are skipped.
   - A stage with nothing to do costs one or two reads and passes the turn on in the same execution.
   - The first stage with work runs, and no other rotating stage runs after it.
   - `next` = the stage after the one that ran, or `start` if none ran.
5. Writes that stage's `cron_runs` row itself, through the KV `logq_{sid}_{bucket}` outbox, with buffer `{run_id}-maint` and `tick_event_id` = the tick's id. The row is recorded exactly once whether or not the tick hears the reply.
6. Replies `{status, ran, next, rows, calls, cache_ops, vcpu_ms, bundle_pending, delete_pending}`. `delete_pending` is `true` when the Delete Data step stopped only for its budget; the tick then makes maintenance due again on the next tick. `bundle_pending` is `true` or `false` when `summary_bundle` had its turn (`true` when the run left work and did not end in an error), and `null` otherwise. The tick uses it to bring the next maintenance turn forward to `MAINT_CATCHUP_S` (30 s) while bundles are behind. `rows` carries the execution's `fos_usage` and `api_telemetry` rows (`telemetry::api::runtime_rows`). The tick writes them into its own buffer. They are best-effort: lost if the tick stops waiting first.

**Round-robin** (`cron:state.maint_next`, `maint_misses`; `admin/mod.rs:3111-3125`)
- A stage reply sets `maint_next = next` and clears `maint_misses`.
- No reply (`worker_deadline`, error or a non-stage body) increments `maint_misses`. After `MAINT_MISS_LIMIT` (2) misses in a row, the pointer moves on by one. A stage whose worker always times out therefore cannot block the others.
- **Fairness bound:** the rotation has six runnable entries (`request_compact` is skipped), so every stage with work runs within 6 answered maintenance turns of the last one, and within 12 when every turn misses twice. The soak simulation allows `MAINT_STAGES.len() × MAINT_INTERVAL_S + 1` s (`TURN_S`) for a requested stage. The F5 simulation measured at most 2 ticks between turns for delete-history and 1 for reconcile, at 52 raw files per 2-minute period.

**Stage gates**
- `retention_sweep`: every 6 h (`SWEEP_INTERVAL_S`), and on every turn until a started sweep completes. See [retention-sweep.md](retention-sweep.md).
- `log_compact`: when `compaction_enabled` is on and a day has work. See [log-compact.md](log-compact.md).
- `delete_history`: while an admin-started job is incomplete. See [delete-history.md](delete-history.md).
- `storage_reconcile`: every 6 hours on its own (`RECONCILE_INTERVAL_S`), or after an admin request. See [storage-reconcile.md](storage-reconcile.md).
- `summary_bundle`: while a closed hour or day has window summaries its bundle does not cover, or the open hour has at least `OPEN_BUNDLE_MIN` (6) settled windows its `open_bundle=` snapshot does not cover. Open hours grow incrementally: a turn resumes the hour's `partial=`, folds only the windows settled since (the name-order prefix of windows ending at least `settle_s(log_period)` before the listed raw minute, so windows late runs still write never invalidate it), writes the larger `partial=`, then writes an `open_bundle=` snapshot of that same prefix. No window is folded twice, and readers fetch the few unsettled windows after the snapshot as windows. Its catalog reads and writes use their own admission gate (`catalog::bundles::admitted`, the bundle's `VCPU_STOP_MS` 600 ms) rather than the ingest commit's 150 ms catalog gate, so a turn that starts late in a busy execution still makes progress. A turn that publishes an hour bundle continues with the next fold in the same execution. Resumable `partial=` state is zstd-compressed and split into objects no larger than the 8 MiB CDN read limit (a full day's is about 5.4 MB on the bench fixtures, against 9.5 MB unpacked). A partial's `h` fingerprint also covers the fold's state layout (`Fold::layout`: the request groups and their columns, plus a manual `PARTIAL_VERSION`), so a partial written by a build with another layout is never resumed: its fields would be read by position and misread, or fail to decode every execution. It is not read and goes with the other stale partials at the next write. A partial whose bytes do not decode (truncated or corrupt) is deleted at once and counted in `discarded` rather than reported as an error, and the build resumes from the next smaller partial or starts over. One over the CDN read limit, or missing a part, is reported as `{error}:{file}` and skipped, and deleted when the build writes. A job that fails reports `{error}:{file}` too and does not stop the others: a failing day stays queued while later days and open-hour pre-folds go on, and a closed hour that fails holds the forward cursor but not revisits or days.
  - The same stage then bundles CMCD window summaries (`processed/request/cmcd_summary/…/window=`) into `processed/summary_bundle/cmcd_summary/` with the same machinery (`bundle::cmcd_summary_bundle`, state `summary:cmcd_bundle_state`): `hour_bundle=`, `open_bundle=`, `day_bundle=` and `partial=` files named with the same count, row and fingerprint proof. Bundles merge windows losslessly (`cmcd_summary::merge_windows`: sums, HyperLogLog and histogram unions, sessions by id), so a bundle answers exactly what its windows would. CMCD bundles are not catalogued: `GET /api/files?dataset=cmcd_summary&view=summary` lists each closed hour as its `hour_bundle=` when the bundle names exactly the windows listed no later than it (plus any windows written after it); an hour without one is listed as its newest `open_bundle=` whose count and fingerprint match the name-order prefix of its listed windows (kind `hour`, ending at its last window), plus the windows after that prefix, and as its windows otherwise. A reader that finds a listed bundle gone relists the summary view once (a replaced open snapshot names its successor), then reads the windows. The /streaming rollup asks for that view first and falls back to the per-window listing when a server rejects it; either way it falls back to raw Parquet above 250 files. The two datasets take turns leading: each state records when its dataset last led (`led`), and the one that led less recently goes first with the execution's whole vCPU and call allowance. The other follows in the same execution only when the leader finished without stopping for its vCPU or wall budget and `MIN_BUNDLE_CALLS` are left, so a backlog in either dataset alternates with the other instead of starving it. Both share the execution's admission gates (600 ms fold stop, 630 ms write stop, `MAX_SOURCES` per execution) and report under the same `summary_bundle` row (`details.cmcd`, with `details.cmcd_first` naming the leader). `bundle_pending` is true when either dataset has work left. An open-hour turn stops folding once the next fold and the partial write, at the last fold's measured cost, would end past the write stop, so it still writes the snapshot it folded for; when the snapshot is out of reach this execution its partial still keeps the folds, so a large open hour converges over turns instead of restarting each turn.
- `request_compact`: never in the rotation. Request and RUM compaction run as their own job every `COMPACT_INTERVAL_S` (30 s), or `COMPACT_CATCHUP_S` (10 s) after a run that left work, while compaction is configured. See [clock.md](clock.md).
- `delete_data`: while an admin-started Delete Data job is incomplete (`reset::reset_stage`). It normally runs before the rotation; the rotation's entry runs it only when that first step did not.

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
- `compute/cloud/tests/ingest.rs`: `simulation::every_maintenance_stage_gets_a_turn_within_four_intervals_under_load`, and the stage tests that drive `record_tick` with `worker_dispatch` running `handle_worker_stage` in a fresh execution.

## Code

- `compute/cloud/src/admin/stage.rs`: request, reply parsing, handler.
- `compute/cloud/src/admin/logs.rs:678-808`: `TickContext`, `MAINT_STAGES`, `maintenance_stage`.
- `compute/cloud/src/admin/mod.rs:1701-1727`, `2258-3157`: constants, dispatch and round-robin.
- `compute/cloud/src/sdk.rs:2249-2250`: worker route.
