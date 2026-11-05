# raw_delete

Deletes raw `.gz` objects only after ingest has queued them as converted. Uses S3 `DeleteObjects`, with an optional `flp-worker` fan-out fallback.

## Purpose

Remove raw logs from `raw/request/` once they exist as Parquet, so that raw storage does not grow without bound. It is the Q17 `delete_after` behaviour.

## Why it exists

The ingest watermark only proves that a key was listed. It does not prove the key was converted. Deleting only from per-run queues written at ingest commit means a raw object is never removed unless its rows are already in `processed/` (spec §5.2, `ingest/mod.rs:8-10`).

## How it works

**Trigger**
- Runs in the clock tick on every non-busy bucket, directly after request_ingest, and only if ingest returned no error (`admin/mod.rs:1918-1958`). It stays in the tick because it must follow the coordinator's `deleteq` ordering.
- Ingest leaves `RAW_DELETE_CALLS` (5) for it on top of the tick reserve, so it is admitted on every tick ingest does not error. It keeps `TICK_TAIL` (4) calls free for the tick row and the `cron:state` CAS (`admin/mod.rs:1516-1522`).
- Usage caller: `cron:raw_delete`.

**Steps** (`retention::raw_delete`, `ingest/retention.rs:221-329`)
1. If `service.delete_after` is false, the raw keys are kept, but each queue record and its `inflight_keys` manifests are still deleted, so records do not accumulate (`230-232`).
2. Check `room(5)`: under the 40 s wall guard (`storage.rs:127-132`), with `reserve + 5` calls left.
3. `kv_list("ingest:deleteq:", limit 20)` (`QUEUES_PER_TICK`). A next cursor sets `more_pending`.
   - If any records are listed, read `ingest:plan` and `ingest:watermark`. The record of the current plan (`plan_id`, or a legacy `run_id`) is skipped unless the watermark's `run_id` names it: a crash or lost CAS between the deleteq write and the watermark commit must not delete raw objects of an uncommitted plan.
4. `admin_get` each queue record `{run_id, keys, sizes, at}` while `room(3)` holds.
5. Record the queued sizes in the usage aggregator, then make one `delete_keys` call over the keys of every loaded queue (`retention.rs:29-43`):
   - **DeleteObjects:** one POST `?delete` per 1,000 keys (`storage.rs:205-263`). A key counts as deleted only if the response confirms it in `<Deleted>`; any other key counts as failed. An error on the first chunk makes the whole call fail. An error on a later chunk marks the remaining keys as failed.
   - **Fallback:** only when DeleteObjects itself errors and `settings.worker_delete_fallback` is true (the default, `telemetry/mod.rs:816`).
     - `fan_out` sends up to `retention_fanout` calls (default 8, range 1–16), each carrying up to 28 keys (`WORKER_KEYS`), to `flp-worker` at `POST /_internal/delete` with `x-flp-worker-key` (`retention.rs:45-100`, `sdk.rs:728-745`).
     - The worker checks the key in constant time and limits deletion to `processed/` or `raw/` under the service prefix. It issues single DELETEs and treats 404 as deleted (`sdk.rs:905-944`, `retention.rs:122-195`).
6. For each deleted key, write an `ingest_files` row with `state_version = 2`, `raw_deleted = true` and `raw_deleted_at`.
7. Update each queue:
   - All keys deleted: `kv_delete` the queue record.
   - Some keys deleted: CAS it down to the remaining keys.
   - No keys deleted: leave it unchanged and count the keys as failed (`302-326`).
8. Move held objects to quarantine. Commit also lists every failed or quarantined object of the plan in the queue record's `held` array (`{key, size, status, reason, attempts}`, `coordinator.rs:722-746`; legacy `finish` in `mod.rs`). When `held` is non-empty, raw_delete needs `room(2)`. It then makes a create-only `admin_cas` of `ingest:quarantine:{run_id}` `{run_id, objects, first_seen, at}` before it deletes or CASes the queue (a CASed queue keeps `"held": []`). `first_seen` is the plan's `created_at`. This also happens when `delete_after` is off, and a queue that holds only held objects is still processed (`retention.rs:367-395`). The raw keys themselves are kept; only the admin purge deletes them ([logs page](../pages/logs.md), runbook "Quarantine").

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
  - When some keys fail, `error = raw_delete_failed_{n}` (`admin/mod.rs:1744-1762`).
- `ingest_files` v2 rows, one per deleted raw key.
- The fallback adds a `retention.delete_objects_failed` entry to a local log vector. raw_delete passes a throwaway `Vec` (`retention.rs:281`), so neither the entry nor worker logs are recorded for this stage.

## Constraints and gotchas

- Call cost per tick:
  - 1 list
  - one GET per queue, up to 20
  - 1 POST for every 1,000 keys
  - 1 delete or CAS per queue that changed
- Each worker fallback call costs 1 call, plus a one-time `worker_secret` read (`sdk.rs:145-152`). Each worker execution has its own 32-call budget.
- With 20 queues of up to 16 keys each, one tick can delete about 320 keys.
- If the budget runs out after the delete, a queue that was fully deleted is not updated (`room(1)`, `retention.rs:311-314`). The next tick deletes those keys again. If FOS behaves like S3 and reports missing keys as `<Deleted>` (this has not been verified live), that tick writes a second `ingest_files` v2 row for each one.
- `failed` and `quarantined` raw objects are never deleted by this stage. Their keys travel as `held` and land in `ingest:quarantine:{run_id}`. Because they are at or below the watermark, they are never re-listed or re-ingested.
- Queue records written before `held` existed have no held objects; their failed and quarantined keys appear only in `ingest_files` rows and are not listed by the quarantine view.
- The fallback depends on the `worker_delete_fallback` setting, not on the outcome of Task 0.2. log_compact forces it off (`compact.rs:536-614`).

## Failure modes and recovery

- **DeleteObjects unsupported or failing:** the worker fan-out runs if enabled. Otherwise the stage errors and every queue is kept.
- **Per-key errors:** the queue is CASed down to those keys and retried on a later tick (test `per_key_delete_errors_are_requeued_and_retried`).
- **Ingest error in this tick:** the stage is skipped and resumes on the next tick.
- **`delete_after` toggled off:** queues build up in KV and drain once it is turned back on.

## Tests

`app/tests/ingest.rs`:
- `services_default_to_delete_after_and_thirty_day_retention` (1380)
- `raw_delete_removes_only_queued_keys_and_writes_version_two_rows` (1387)
- `raw_delete_is_a_no_op_when_delete_after_is_off` (1416)
- `per_key_delete_errors_are_requeued_and_retried` (1429)
- `delete_objects_batches_at_one_thousand_keys` (1452)
- `raw_delete_leaves_the_queue_when_the_budget_cannot_cover_it` (1467)
- `unsupported_delete_objects_falls_back_to_bounded_worker_fanout` (1612)
- `worker_delete_authenticates_bounds_and_scopes_keys` (1643)
- `worker_delete_stops_at_the_wall_and_reports_the_rest_failed` (3052)
- `ticks_delete_converted_raw_objects_and_sweep_every_six_hours` (2331)
- `held_objects_move_to_a_quarantine_record_once_their_plan_is_committed` (1959)
- `held_objects_of_an_uncommitted_plan_are_never_quarantined` (1980)
- `a_held_only_queue_is_quarantined_even_when_delete_after_is_off` (2002)
- Purge tests (2013-2240) are listed in [logs.md](../pages/logs.md).

## TODOs

- **Capacity (simulation):** raw delete falls behind at about 0.04 req/s, and above about 0.05 req/s raw objects are never deleted, so they keep billing storage. Source: `docs/recommendation.md` cost model (2026-10-30).
- Plan Task 0.2 (needs approval) is unchecked. It confirms DeleteObjects with our hand-rolled SigV4 on a disposable bucket. `docs/research/04-object-storage-writes.md:253-255` has earlier evidence: no Content-MD5 needed, 600 keys accepted, 1,001 rejected with 400. That evidence does not cover the current code path.
- The spec (§5.4) enables the fallback only if Task 0.2 fails, but the code enables it by default.
- Fallback and worker logs are discarded for this stage.
- No queue exists for RUM raw objects; that waits on master-plan Task 9.
- Quarantine `attempts` is the plan's lease attempt (1 for legacy plans), not a per-object retry count, because shard failure entries are dropped on success.
- The quarantine purge uses DeleteObjects only; it has no worker fallback.

## Code

- `app/src/ingest/retention.rs:21-24`: constants.
- `app/src/ingest/retention.rs:29-100`: `delete_keys`, `fan_out`.
- `app/src/ingest/retention.rs:122-200`: `handle_worker_delete`, with its 40 s wall stop at `171-176`.
- `app/src/ingest/retention.rs:230-400`: `raw_delete`, with the quarantine move at `367-395`.
- `app/src/ingest/mod.rs:43`, `290`: `QUARANTINE_PREFIX`, `held_object`.
- `app/src/ingest/storage.rs:118-132`, `194-263`: `Store`, `WALL_STOP`, `delete`, `delete_objects`.
- `app/src/sdk.rs:728-745`, `905-944`: `worker_delete` and the worker router.
- `app/src/admin/mod.rs:1722-1771`: stage wiring.
