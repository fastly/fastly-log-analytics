# delete_history

An admin-started, resumable job that deletes one operational log kind's Parquet history older than N days, or all of it, then purges that kind from the front CDN.

## Purpose

Lets an admin remove operational log history on demand, for example after a noisy incident or before a demo. It covers one `LogKind` under `processed/ops/{kind}/`. Request and RUM data are out of scope.

## Why it exists

Retention only removes data once it ages past a cutoff. The master plan (D1, line 113) says destructive data operations run only on demand and never on a schedule. A deletion can span thousands of objects, more than one Compute execution's 32 calls, so the job persists its own state and the tick finishes whatever the request could not.

## How it works

**Start** (`POST /api/admin/logs/delete-history`, `admin/logs.rs:278-352`)
1. The body is `{kind, older_than_days}`. `kind` is any `LogKind`. `older_than_days` is 1–3650 (`MAX_HISTORY_DAYS`, `29`) or `null`. Anything else returns 400 `invalid_input`.
2. Read KV `logs:delete_history` (`JOB_KEY`, `27`). An incomplete job returns 409 `history_job_in_progress`.
3. Compute the cutoff:
   - with N days: `floor_hour(now − N days)`;
   - with `null`: `floor_hour(now) + 2 h`, so the current and next hour are included.
4. CAS-create the job `{kind, older_than_days, cutoff_ts, status: "deleting", actor, started_at, updated_at, deleted, error}` (`HistoryJob`, `64-75`). A lost CAS also returns 409.
5. Run `step_history` once inside the request, with reserve 0 and a fresh wall clock. Return `{job}`.

**Step** (`step_history`, `logs.rs:356-412`)
1. **`deleting`**, if `room(3)`: run `retention::history_sweep(store, kind, cutoff_ts, …)` (`retention.rs:757-787`).
   - This is the retention core with fixed state key `history:{kind}` and generation `history:{kind}@{cutoff}`. It lists, persists pending keys, then deletes via DeleteObjects or the fallback.
   - When nothing is pending, `kv_delete history:{kind}` and set status `purging`.
2. **`purging`**, if `room(3)`: read config `front_service_id` and the Fastly token secret named by `service.fastly_token_ref`. Then POST `/service/{front}/purge/flp-fos-ops-{kind}` to the Fastly API.
   - 2xx sets status `complete`.
   - Any other status sets `error = purge_status_N` and leaves the job in `purging`.
3. CAS the job only if status, `deleted` or `error` changed.

****Tick continuation** (`logs.rs:1340-1381`)
- Delete-history runs on its round-robin turn in the stage worker ([stage-worker.md](stage-worker.md), `history_stage`, dispatched at `admin/logs.rs:777`).
- If `enough(3)`, read the job. If it is not `complete` and `enough(4)`, run `step_history` with reserve `TAIL` (3).

**State:** KV `logs:delete_history` and `history:{kind}`. FOS objects are deleted. The front-service surrogate key is purged.

## Roles and permissions

Admin only (`/api/admin/*` is admin-gated at `shell.rs:792-806`, routed at `1279-1287`). The start is audited as `log_history_delete_started`, with target `{kind}:older_than_days={N}` or `{kind}:all`. Tick steps are not audited separately.

## Manual triggers and admin controls

- `POST /api/admin/logs/delete-history` starts the job. On Cloud the shared pages reach it through the adapter with no `older_than_days`: the `/logs` purge (`DELETE /api/cron-runs`, `frontend/app/logs/_sections/useLogsPageState.ts:291-294`) starts a `cron_runs` job (`frontend/lib/cloud-engine/adapter.ts:1596-1599`), and the `/admin/usage-log` purge (`DELETE /api/admin/usage-log`) starts a `fos_usage` job (`frontend/lib/cloud-engine/admin-wire.ts:252-254`).
- `GET /api/admin/logs/status` returns the job (`logs.rs:174-187`).
- `POST /api/cron-runs/trigger` advances a running job right away.
- The job cannot be cancelled. Once it completes, a new one can be started.

## Telemetry

- A `cron_runs` row with `cron_kind = "delete_history"`, in buffer `{run_id}-maint`, is written for every tick step. Its fields:
  - `deleted_files`
  - `more_pending`
  - outcome `partial`, `done` or `error`
  - `error`
  - `details_json = {kind, status}`
- The step run inside the request writes no row.

## Constraints and gotchas

**Budget and cost**
- Whenever the turn reaches delete-history, the stage execution spends 1 call reading `logs:delete_history`, even when no job exists.
- A purge costs 1 Fastly API call. Reading the token adds a secret read on first use.

**Wall time**
- `step_history`'s own `room` checks only calls. The sweep inside it also stops at the 40 s wall guard (`retention.rs:767`).
- The Fastly API timeout is clipped to the remaining tick time (`transport::request_timeout`).

**Errors and lost state**
- If the in-request step fails, the response includes the error but the error is not persisted.
- If the purge keeps failing, for example because the token is missing, the job stays `purging`. It retries every tick and blocks any new job.

**Scope**
- Only FOS files and the CDN copy are removed. Rows still buffered in Core Cache or the KV outbox for that kind are untouched. They land in a later flush, and for best-effort kinds `clear-cache` drops them.
- With `null`, newly flushed files in the current hour are deleted too, because the cutoff is now + 2 h. Files flushed after the job completes stay.

**Machinery**
- This is a bespoke KV job. It does not use the `job:<id>` engine (`admin/mod.rs:275-285`, `1222-1373`), and its row is not `cron_kind = job_step`.

## Failure modes and recovery

- **Crash mid-sweep:** `history:{kind}` keeps the pending keys and cursor, and the next tick resumes (`history_sweep_deletes_one_kind_up_to_its_fixed_cutoff_and_resumes`).
- **Purge failure:** retried every tick until 2xx. Fix the token or `front_service_id`.
- **A second request while a job runs:** 409.

## Tests

`compute/cloud/tests/ingest.rs`:
- `history_sweep_deletes_one_kind_up_to_its_fixed_cutoff_and_resumes` (6527)
- `history_sweep_with_a_future_cutoff_clears_the_whole_kind` (6558)
- `delete_history_runs_as_a_resumable_job_ending_in_one_front_purge` (6864)
- `delete_history_refuses_a_second_job_and_resumes_from_the_tick` (6928)

## TODOs

- The spec's `cron_kind` enum (§3.1, line 153) has no `delete_history`. Spec §5.5 expects this work to run as a `job_step` of the job engine. The code uses its own kind and a KV job.
- There is no cancel, and no cap on retries for a failing purge.
- The master-plan "reset logs" job for request data (line 674) is unimplemented. It is on-demand only.
- v3 parity: the reset-logs/quarantine-purge admin actions in v3 (`admin/*`, master plan line 674). The plan covers them as on-demand stepped jobs. No v3 cron doc corresponds to this job.

## Code

- `compute/cloud/src/admin/logs.rs:27-29`: constants.
- `compute/cloud/src/admin/logs.rs:64-75`: `HistoryJob`.
- `compute/cloud/src/admin/logs.rs:278-352`: start endpoint.
- `compute/cloud/src/admin/logs.rs:356-412`: `step_history`.
- `compute/cloud/src/admin/logs.rs:1340-1381`: stage wiring.
- `compute/cloud/src/ingest/retention.rs:757-787`: `history_sweep`.
- `compute/cloud/src/ingest/retention.rs:789-911`: shared `sweep_dataset`.
