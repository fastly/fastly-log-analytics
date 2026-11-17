# raw_delete

Deletes raw `.gz` objects only after ingest has queued them as converted. Uses S3 `DeleteObjects`, with an optional `flp-worker` fan-out fallback.

## Purpose

Remove raw logs from `raw/request/` once they exist as Parquet, so that raw storage does not grow without bound. It is the Q17 `delete_after` behaviour.

## Why it exists

The ingest watermark only proves that a key was listed. It does not prove the key was converted. Deleting only from per-run queues written at ingest commit means a raw object is never removed unless its rows are already in `processed/` (spec §5.2, `ingest/mod.rs:8-11`).

## How it works

**Trigger**
- Runs in the clock tick on every non-busy bucket, directly after request_ingest, and only if ingest returned no error (`admin/mod.rs:2732-2799`). It stays in the tick because it must follow the coordinator's `deleteq` ordering.
- Ingest leaves `RAW_DELETE_CALLS` (5) for it on top of the tick reserve, so it is admitted on every tick ingest does not error. It keeps `TICK_TAIL` (6) calls free for the tick row and the `cron:state` CAS (`admin/mod.rs:1696-1707`).
- Usage caller: `cron:raw_delete`.

**Steps** (`retention::raw_delete_with_inline`, `ingest/retention.rs:270-487`)
1. If `service.delete_after` is false, the raw keys are kept, but each queue record and its `inflight_keys` manifests are still deleted, so records do not accumulate (`230-232`).
2. Check `room(5)`: under the 40 s wall guard (`storage.rs:153`, `208-210`), with `reserve + 5` calls left.
3. `kv_list("ingest:deleteq:", limit 20)` (`QUEUES_PER_TICK`). A next cursor sets `more_pending`.
   - If any records are listed, read `ingest:plan` and `ingest:watermark`. The record of the current plan (`plan_id`, or a legacy `run_id`) is skipped unless the watermark's `run_id` names it: a crash or lost CAS between the deleteq write and the watermark commit must not delete raw objects of an uncommitted plan.
4. `admin_get` each queue record `{run_id, keys, sizes, at}` while `room(3)` holds.
5. Record the queued sizes in the usage aggregator, then make one `delete_keys` call over the keys of every loaded queue (`retention.rs:35-49`):
   - **DeleteObjects:** one POST `?delete` per 1,000 keys (`storage.rs:582-638`). A key counts as deleted only if the response confirms it in `<Deleted>`; any other key counts as failed. An error on the first chunk makes the whole call fail. An error on a later chunk marks the remaining keys as failed.
   - **Fallback:** only when DeleteObjects itself errors and `settings.worker_delete_fallback` is true (the default, `telemetry/mod.rs:1254`).
     - `fan_out` sends up to `retention_fanout` calls (default 8, range 1–16), each carrying up to 28 keys (`WORKER_KEYS`), to `flp-worker` at `POST /_internal/delete` with `x-flp-worker-key` (`retention.rs:51-106`, `sdk.rs:1634-1653`).
     - The worker checks the key in constant time and limits deletion to `processed/` or `raw/` under the service prefix. It issues single DELETEs and treats 404 as deleted (`sdk.rs:2228-2275`, `retention.rs:128-202`).
6. For each deleted key, write an `ingest_files` row with `state_version = 2`, `raw_deleted = true` and `raw_deleted_at`.
7. Update each queue:
   - All keys deleted: `kv_delete` the queue record.
   - Some keys deleted: CAS it down to the remaining keys.
   - No keys deleted: leave it unchanged and count the keys as failed (`302-326`).
8. Move held objects to quarantine. Commit also lists every failed or quarantined object of the plan in the queue record's `held` array (`{key, size, status, reason, attempts}`, `coordinator.rs:2182-2212`; legacy `finish_legacy` in `mod.rs`). When `held` is non-empty, raw_delete needs `room(2)`. It then makes a create-only `admin_cas` of `ingest:quarantine:{run_id}` `{run_id, objects, first_seen, at}` before it deletes or CASes the queue (a CASed queue keeps `"held": []`). `first_seen` is the plan's `created_at`. This also happens when `delete_after` is off, and a queue that holds only held objects is still processed (`retention.rs:449-461`). The raw keys themselves are kept; only the admin purge (`POST /api/admin/quarantine/purge`, `ingest/api.rs:1739`) deletes them.

**State and outputs**
- KV: `ingest:deleteq:{run_id}`, consumed and removed by this stage.
- KV: `ingest:quarantine:{run_id}`, created here from `held` and consumed only by `POST /api/admin/quarantine/purge`.
- FOS: raw objects deleted.
- `fos_usage` delete counts and bytes, recorded through `usage.record_deleted` and `note_sizes`.

## Roles and permissions

There is no endpoint. The stage runs from `/_tick` or from the admin manual trigger. The worker endpoint accepts only requests that carry the `worker_secret` peer key (403 otherwise).

## Manual triggers and admin controls

- `POST /api/cron-runs/trigger` runs ingest and then raw_delete.
- `delete_after` is a per-service setting, default `true` (test `services_default_to_delete_after_and_thirty_day_retention`). There is no separate raw-delete endpoint.

## Telemetry

- `cron_runs` row (`cron_kind = raw_delete`).
  - Outcome: `done`, or `partial` when `more_pending`; `error` if the stage fails.
  - Fields: `deleted_files`, `bytes_written` (here, bytes deleted), `backend_requests_used`.
  - When some keys fail, `error = raw_delete_failed_{n}` (`admin/mod.rs:2773-2775`).
- `ingest_files` v2 rows, one per deleted raw key.
- The fallback adds a `retention.delete_objects_failed` entry to a local log vector. raw_delete passes a throwaway `Vec` (`retention.rs:386`), so neither the entry nor worker logs are recorded for this stage.

## Constraints and gotchas

- Call cost per tick:
  - 1 list
  - one GET per queue, up to 20
  - 1 POST for every 1,000 keys
  - 1 delete or CAS per queue that changed
- Each worker fallback call costs 1 call, plus a one-time `worker_secret` read (`sdk.rs:1637`). Each worker execution has its own 32-call budget.
- With 20 queues of up to 16 keys each, one tick can delete about 320 keys.
- If the budget runs out after the delete, a queue that was fully deleted is not updated (`room(1)`, `retention.rs:427-447`). The next tick deletes those keys again. If FOS behaves like S3 and reports missing keys as `<Deleted>` (this has not been verified live), that tick writes a second `ingest_files` v2 row for each one.
- `failed` and `quarantined` raw objects are never deleted by this stage. Their keys travel as `held` and land in `ingest:quarantine:{run_id}`. Because they are at or below the watermark, they are never re-listed or re-ingested.
- Queue records written before `held` existed have no held objects; their failed and quarantined keys appear only in `ingest_files` rows and are not listed by the quarantine view.
- The fallback depends on the `worker_delete_fallback` setting, not on the outcome of Task 0.2. log_compact forces it off (`compact.rs:659-664`).

## Failure modes and recovery

- **DeleteObjects unsupported or failing:** the worker fan-out runs if enabled. Otherwise the stage errors and every queue is kept.
- **Per-key errors:** the queue is CASed down to those keys and retried on a later tick (test `per_key_delete_errors_are_requeued_and_retried`).
- **Ingest error in this tick:** the stage is skipped and resumes on the next tick.
- **`delete_after` toggled off:** queues build up in KV and drain once it is turned back on.

## Tests

`compute/cloud/tests/ingest.rs`:
- `services_default_to_delete_after_and_thirty_day_retention` (5165)
- `raw_delete_removes_only_queued_keys_and_writes_version_two_rows` (5172)
- `raw_delete_keeps_raw_objects_but_consumes_the_queue_when_delete_after_is_off` (5491)
- `per_key_delete_errors_are_requeued_and_retried` (5525)
- `delete_objects_batches_at_one_thousand_keys` (5548)
- `raw_delete_leaves_the_queue_when_the_budget_cannot_cover_it` (5563)
- `unsupported_delete_objects_falls_back_to_bounded_worker_fanout` (6145)
- `worker_delete_authenticates_bounds_and_scopes_keys` (6176)
- `worker_delete_stops_at_the_wall_and_reports_the_rest_failed` (7967)
- `ticks_delete_converted_raw_objects_and_sweep_every_six_hours` (7058)
- `held_objects_move_to_a_quarantine_record_once_their_plan_is_committed` (5657)
- `held_objects_of_an_uncommitted_plan_are_never_quarantined` (5678)
- `a_held_only_queue_is_quarantined_even_when_delete_after_is_off` (5700)
- Quarantine list and purge tests (5711-5937).

## TODOs

- **Capacity (simulation):** raw delete falls behind at about 0.04 req/s, and above about 0.05 req/s raw objects are never deleted, so they keep billing storage. Source: [recommendation.md](../recommendation.md) cost model (2026-10-30).
- Plan Task 0.2 (needs approval) is unchecked. It confirms DeleteObjects with our hand-rolled SigV4 on a disposable bucket. The object-storage-writes research note in the former fastly-logs-parquet repository has earlier evidence: no Content-MD5 needed, 600 keys accepted, 1,001 rejected with 400. That evidence does not cover the current code path.
- The spec (§5.4) enables the fallback only if Task 0.2 fails, but the code enables it by default.
- Fallback and worker logs are discarded for this stage.
- No queue exists for RUM raw objects; that waits on master-plan Task 9.
- Quarantine `attempts` is the plan's lease attempt (1 for legacy plans), not a per-object retry count, because shard failure entries are dropped on success.
- The quarantine purge uses DeleteObjects only; it has no worker fallback.

## Code

- `compute/cloud/src/ingest/retention.rs:24-30`: constants.
- `compute/cloud/src/ingest/retention.rs:35-106`: `delete_keys`, `fan_out`.
- `compute/cloud/src/ingest/retention.rs:128-202`: `handle_worker_delete`, with its 40 s wall stop at `173-176`.
- `compute/cloud/src/ingest/retention.rs:259-487`: `raw_delete` and `raw_delete_with_inline`, with the quarantine move at `449-461`.
- `compute/cloud/src/ingest/mod.rs:125`, `651-659`: `QUARANTINE_PREFIX`, `held_object`.
- `compute/cloud/src/ingest/storage.rs:142-153`, `571-638`: `Store`, `WALL_STOP`, `delete`, `delete_objects`.
- `compute/cloud/src/sdk.rs:1634-1653`, `2228-2275`: `worker_delete` and the worker router.
- `compute/cloud/src/admin/mod.rs:2732-2799`: stage wiring.
