# retention_sweep

Deletes whole expired hour folders, and expired day-level `compact=` files, from each processed dataset. Every dataset keeps its own resumable generation in KV.

## Purpose

Applies Q17 retention:
- `log_retention_days` to `processed/request/`
- `rum_retention_days` to `processed/rum/`
- the per-kind operational retention to `processed/ops/{kind}/`

A value of `0` means keep forever.

### Settings

These follow v3. Each install sets them in the provisioning wizard (Storage step, Data Retention Policy) and changes them later in Logging settings (General Settings):

| Setting | Default | Range | UI label |
|---|---|---|---|
| `delete_after` | on | bool | Auto-Delete Raw Logs |
| `log_retention_days` | 30 | 0–3650, 0 = forever | Request Log Retention |
| `rum_retention_days` | 30 | 0–3650, 0 = forever | RUM Telemetry Retention |
| `log_period` | 60 s | 1–3600 s | Log Period |
| `rum_log_period` | 300 s | 1–3600 s | RUM Log Period |

The UI offers v3's choices: 1, 3, 7, 14, 30, 90, 180 or 365 days, or Forever.
- `Provision` (`admin/model.rs:247-253`) accepts the three retention fields and rejects retention above 3,650 days.
- `PATCH /api/admin/services/{sid}` saves them, and `validate_service` (`admin/mod.rs:580`) enforces the same bound.
- Provisioning still stores fixed values (`delete_after: true`, 30, 30 at `admin/mod.rs:896-898`) until it copies the input fields.
- The auto-delete switch appears only when request logging is enabled. Raw RUM deletion does not exist yet.

## Why it exists

Without it, FOS storage would grow without bound. The master plan answers Q17 with a retention sweep, and it replaces v3's `expire` job. The sweep never rewrites files: it removes whole folders, so the listing-based manifests reflect the deletion immediately.

## How it works

**Trigger** (`sweep_stage`, `admin/logs.rs:541-598`)
1. On its round-robin turn in the stage worker ([stage-worker.md](stage-worker.md)), check `enough(5)` (`TAIL 3 + 5` calls). If there is room, read KV `retention:last_sweep` = `{at, complete, next}`.
2. The sweep is due if any of these hold:
   - there is no record;
   - `complete` is false;
   - at least 6 h have passed since `at` (`SWEEP_INTERVAL_S`, `retention.rs:24`).
3. If due, run `retention_sweep_from(store, settings, TAIL, None, next)`. An unfinished sweep resumes at the first unfinished dataset.
4. Afterwards, `note_sweep` CASes `{at, complete: !more_pending, next}`. The CAS result is ignored (`logs.rs:412-420`).

**Datasets** (`datasets`, `retention.rs:355-385`)
- `request` and `rum`, plus `ops:{kind}` for each `LogKind`.
- Ops days come from `retention_days_by_kind`. If a kind has no entry, `default_retention_days` applies: `admin_audit` 365; `cron_runs`, `ingest_files`, `platform_errors` and `fos_usage` 30; `api_telemetry` and `cron_ticks` 14 (`telemetry/mod.rs:138-144`).
- Datasets with 0 days are skipped (`retention.rs:449`).

**Per dataset** (`sweep_dataset`, `retention.rs:522-624`)
1. Read KV `retention:{name}` = `{generation, cutoff_ts, cursor, pending, done, updated}`.
2. Compute `cutoff = floor_hour(now − days)`. The generation is `name@YYYY-MM-DD`, from the cutoff's day.
   - A different generation restarts discovery from the beginning, carrying over any `pending` keys.
3. Delete any `pending` keys first (`delete_keys`: DeleteObjects, with the worker fallback if enabled). Persist the result. Failures stay pending, and the dataset reports itself unfinished.
4. LIST 1,000 keys from `cursor`. A key is expired if its folder ends at or before the cutoff (`folder_end`, `389-415`):
   - an hour folder ends at hour + 1 h;
   - a `compact=` file under `day=` ends at day + 1.
5. Discovery is `done` when there is no next page, or when the page reaches a live folder.
6. Persist the expired keys as `pending` **before** deleting them. Then loop.
   - Each persist is a CAS plus a re-read. A lost CAS raises `retention_state_conflict`.
   - Every step is gated by `room(n)`: the 40 s wall guard plus enough calls.

**Outputs:** FOS objects deleted, `fos_usage` delete records, and `retention:*` KV state.

## Roles and permissions

Admin only. The manual sweep endpoint is audited. Retention days are edited by admins: service fields through the service settings, ops kinds through `PATCH /api/admin/operational-logging`.

## Manual triggers and admin controls

- `POST /api/admin/logs/sweep` (`logs.rs:380-410`):
  - Runs a full sweep from dataset 0, with reserve 0 and a fresh wall clock, then calls `note_sweep`.
  - Returns `{deleted, failed, bytes, more_pending, datasets, backend_calls}`. Returns 502 on error.
  - Audited as `log_retention_sweep_requested`. It writes no `cron_runs` row.
  - Wired to `RetentionCleanup` on `/logs` (`app/web/components/logs/RetentionCleanup.tsx`).
- `GET /api/admin/logs/status` returns `last_sweep` (`logs.rs:148-160`).

## Telemetry

- Each `cron_runs` row (`cron_kind = retention_sweep`, buffer `{run_id}-maint`) records:
  - `deleted_files`
  - `bytes_written`, which here means bytes found expired
  - `more_pending`
  - `details_json = {datasets: [{dataset, retention_days, complete, deleted, failed}], failed}`
- The outcome is `done`, `partial` or `error`. No row is written when the sweep did nothing.
- `report.logs` (`retention.delete_failed` and the worker logs) is not written anywhere.

## Constraints and gotchas

**Cost**
- The `retention:last_sweep` GET costs 1 call in the stage execution whenever the turn reaches the sweep, even when no sweep is due.
- Each page costs 1 LIST, plus a CAS and GET to persist, then 1 delete POST, plus a CAS and GET.

**Cadence**
- A finished generation is not rescanned until the cutoff day changes. Folders that expire later the same UTC day therefore wait until the next day's generation, even though the sweep runs every 6 h.
- While a sweep is incomplete, it runs on every one of its turns: at least once every 4 answered ticks.

**Listing**
- The code lists flat 1,000-key pages. It does not use the delimiter listing of `year=/month=/day=` prefixes that spec §5.3 describes.
- A key outside the `year=/month=/day=` layout is never deleted.

**Settings**
- Ops kinds accept 1–3650 days only, so they cannot be set to keep forever (`telemetry/mod.rs:840-851`). Only the service datasets accept 0.
- `fos_usage` retention uses the `LogKind` default of 30 days. It does not use `UsageLogging.retention_days` (`auth/settings.rs:9`), which spec §5.1 names as its source.

**Fallback**
- If DeleteObjects fails, the worker fallback is used by default. See [raw-delete.md](raw-delete.md).

**Relation to compaction**
- Compaction and the sweep take separate turns in separate executions, so a compaction backlog cannot starve the sweep (test `simulation::every_maintenance_stage_gets_a_turn_within_four_ticks_under_load`).

## Failure modes and recovery

- **Crash between persist and delete:** the next tick deletes the same pending batch again (`sweep_persists_each_batch_before_deleting_and_resumes_after_a_crash`).
- **Delete errors:** the failed keys stay pending, and `complete = false` keeps the sweep running each tick.
- **Shortened retention:** starts a new generation from the beginning.

## Tests

`app/tests/ingest.rs`:
- `sweep_deletes_whole_hour_folders_older_than_each_datasets_cutoff` (1504)
- `sweep_keeps_expired_day_compaction_only_when_its_day_is_still_inside_retention` (1532)
- `sweep_persists_each_batch_before_deleting_and_resumes_after_a_crash` (1549)
- `failed_keys_stay_pending_and_a_completed_generation_is_not_rescanned` (1578)
- `shortening_retention_starts_a_new_generation_from_the_beginning` (1594)
- `sweep_and_reconcile_endpoints_run_on_demand_and_are_audited` (2284)
- `ticks_delete_converted_raw_objects_and_sweep_every_six_hours` (2331)
- `the_retention_sweep_still_runs_on_ticks_with_a_compaction_backlog` (3010)
- `retention_loops_stop_at_the_wall_and_resume` (3026)

## TODOs

- `fos_usage` retention does not follow `UsageLogging.retention_days`, which spec §5.1 requires.
- The sweep's delete-failure logs are discarded.
- A manual sweep (`POST /api/admin/logs/sweep`) writes no `cron_runs` row.
- The spec (§5.3) describes delimiter listing; the code uses flat pages.
- Retention for RUM raw objects does not exist: there is no RUM raw delete queue until master-plan Task 9.
- v3 parity (covered, implemented): `~/Projects/fastly-log-analytics/docs/cron/jobs/expire.md` maps to this sweep (Q17). `metadata-cleanup.md` maps to ops-kind retention plus audit GC in the flush.
- v3 parity (out of scope): `share-audit-purge.md`. The master plan excludes share links (line 400).

## Code

- `app/src/ingest/retention.rs:24`, `332-433`: interval, report, state, `datasets`, `folder_end`, `retention_sweep`.
- `app/src/ingest/retention.rs:436-478`: `retention_sweep_from`.
- `app/src/ingest/retention.rs:522-624`: `sweep_dataset`.
- `app/src/admin/logs.rs:24`, `380-420`, `517-570`: key, endpoint, `note_sweep`, wiring.
- `app/src/telemetry/mod.rs:138-144`, `805-858`: defaults and validation.
