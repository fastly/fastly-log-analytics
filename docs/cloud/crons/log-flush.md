# log_flush

Archives buffered operational log rows (Core Cache minute buckets, KV outboxes, audit keys) to Parquet in FOS. Each archive is one fenced, resumable transaction, run by its own clock probe (`/_flush`) in its own Compute execution.

## Purpose

Moves every operational log kind into `processed/ops/{kind}/…/hour=HH/window=…parquet`, so that `/logs` and `/usage` read history from FOS. The kinds are `cron_runs`, `ingest_files`, `fos_usage`, `api_telemetry`, `platform_errors` and `admin_audit`.

## Why it exists

Compute cannot keep rows in memory between requests. Writing one FOS object per row would cost too many calls. Rows are therefore buffered cheaply where they are produced, then written in batches.

Rows are buffered by tier (`telemetry/mod.rs:126-132`):
- **Cache (best-effort):** Core Cache buckets `flp:log:v1:{kind}:{sid}:{min}:{shard}` (`telemetry/mod.rs:157`).
- **Outbox (durable):** KV batch keys `logq_{sid}_{bucket:010}` for `cron_runs` and `ingest_files`, one per 2-minute bucket (`bucket = now / 120`). The separator is `_`, because Fastly rejects `:` in a KV list prefix.
  - Writers (tick, ingest and stage workers, `/_flush`, admin requests) append through `append_outbox` (`telemetry/mod.rs:463`): GET, then a generation CAS of the merged NDJSON. A lost CAS re-reads, merges and retries at most twice (`OUTBOX_RETRIES`). At most 6 calls are spent per append (`OUTBOX_APPEND_CALLS`).
  - A key is capped at 512 KiB (`OUTBOX_VALUE_CAP`). A full key rolls to `{bucket}_{n}`, up to `_3`.
  - When retries, rollovers or the budget run out, the rows go to a create-only overflow key `{bucket}_x{first_event_id}` in the same bucket, so nothing is lost.
  - The bucket is recomputed on every attempt, so a write never lands in a bucket that had already closed when it started.
  - Per-request `logq:{sid}:{ms}-{id}` keys written before batching have the same value format. The flush and `read_live` list the `logq` prefix and treat them as closed outbox keys of the bucket their timestamp names, so rows already waiting live are archived and then deleted (`legacy_per_request_outbox_keys_are_archived_and_removed`).
  - Each 2-minute bucket should hold one key. `/_flush` keeps 3 calls (`FLUSH_TAIL_CALLS`) so its own row joins the bucket key with a GET and CAS. With 2 it fell back to an overflow key every probe, doubling inflow; live g2 (56b6d24) then held about 214 keys that never fell, because each commit cleared only 2–3. The clock tick keeps the same GET and CAS in `TICK_TAIL`, so catch-up dispatches in one bucket share its key until it fills.
- **Audit:** KV `audit:` keys and `svc:{sid}._audit_outbox`.

## How it works

**Trigger** (the flush probe)
1. The front service's health check `flp_flush` sends `GET /_flush` every 1,000 ms, on dummy backend `flp_flush_clock` (`app/infra/provision.py:196-234`, `313`). Its VCL path is the same as `/_tick`'s: `main.vcl:81-85` rewrites it to `/_flush?b=<now.sec>`, it is shielded at `iad-va-us` and cached for 1 s, so about one probe a second reaches Compute. Compute answers 200 before the work runs (`Shell::clock_ack`). Drafts provisioned before the probe existed get it from `ensure_flush_clock` on the next deploy (`provision.py:222-240`, `537`).
2. `shell.rs:428-440` accepts only the exact path `/_flush` and calls `admin::record_flush` (`admin/mod.rs:1608-1644`), which refuses the worker service like `record_tick` does, and returns `off_shield` with no work off the IAD shield.
3. **Time precheck.** `record_flush` claims its own Core Cache record `flp-flush-sched-v1` (`schedule::claim` with `flush_wanted`). Until `wake_at` the probe only counts an idle tick: no backend call, no KV write. A due probe takes the 55 s lease and runs `flush_once`. The KV `logs:flush_lease` stays the authority over flush transactions; the record only decides when to look.
4. `flush_once` loads the installed service and FOS credentials, then runs `log_flush_reserving` with `FLUSH_TAIL_CALLS` (3) held back for its own `cron_runs` outbox write and the status purge. Usage caller: `cron:log_flush`. It sets `wake_at`: the next probe when the flush left `more_pending` (including a commit that left sources unread, `FlushReport::behind`), 5 s after `busy`, 30 s after an error, otherwise `FLUSH_WAKE_S` (60 s). A run that did nothing writes no row; its idle counters carry to the next row.
5. The flush never runs inside the clock tick. It has the execution's whole 32-call budget and Core Cache operation budget, and never shares an execution with FOS maintenance, whose sends fail once the per-execution Core Cache transaction limit is hit (`{"diagnostic":"instance_limit","limit":"cache_transactions"}`).

**Core Cache operation budget**
- Every Core Cache lookup, replace and purge counts against `CACHE_OP_BUDGET` = 64 per execution (`shell.rs:18-23`, charged in `sdk.rs:65` and the three cache calls). Past it, the call returns an error instead of reaching Fastly. The limit Fastly enforces is undocumented; 64 stays well below the ~480 lookups that hit it live.
- The scan gets `remaining − FLUSH_CACHE_RESERVE (6) − collect reads`, where `collect` keeps 8 reads (32 when pending rows already look due) (`flush.rs:47-51`, `1228-1237`). The 6 reserved ops cover the caller's telemetry bucket replaces and the status purge.
- `collect` and `purge_ready` stop when only the reserve is left (`flush.rs:804`, `1050`). A stopped scan saves its cursor and resumes next probe.

**Steps** (`log_flush_reserving`, `telemetry/flush.rs:1137-1330`)
1. Return `Unprovisioned` if there is no FOS target. Return `Deferred` if fewer than 4 calls remain.
2. Read cursor `logs:cursor:{sid}` and manifest `logs:flush_txn:{sid}`.
   - A new cursor starts at `closed_end − MAX_SCAN_MIN` (30 minutes back), with the `global` sid scanned up to `closed_end` (`flush.rs:1155-1166`). That is exactly the range `read_live` shows without a cursor (`telemetry/mod.rs:631-655`), so nothing a user can see disappears. Rows older than 30 minutes before the first flush are in the 24 h buckets but no reader shows them, so they are not archived.
3. **Uncommitted manifest:** take the lease and resume `run_txn`. A held lease returns `Busy`.
4. **Committed manifest with GC not done:** finish GC (`finish_committed`, `969`), then reload the cursor.
5. **Scan** (`scan`, `220`):
   - Check closed minutes `(scan_min, now−2]`, at most 30 per run (`MAX_SCAN_MIN`), and only whole minutes the cache-op budget covers.
   - Non-empty bucket bodies found by the scan are kept in memory (≤ 16 MiB, `SCAN_KEEP_BYTES`), so a due flush archives them without a second lookup.
   - Each minute costs 3 cache kinds × 4 shards = 12 lookups, plus 12 more for the `global` sid until `global_end`. So the first probes after a fresh cursor scan two minutes each, then later probes four. (`cron_ticks`, the fourth kind, is retired and no longer scanned.)
   - Minutes older than 1440 count as `expired`. A lookup error stops the scan at that minute without saving partial pending entries.
6. **Due check** (`check_due`, `388`) reads 1 page of the `logq` list prefix, keeps this sid's keys and only **closed** buckets: `bucket + 1 < now / 120`, so the current bucket and the one before it are never read (`closed_bucket`, `372`). The extra bucket of grace covers a writer that read its bucket just before it closed and whose CAS lands within the 60 s execution limit, and KV's eventual list consistency. The flush is due when there are rows pending and either:
   - the flush is manual; or
   - the previous flush was cut short (`cursor.behind`), which bypasses the floor; or
   - `flush_min_interval_s` (300) has passed, and either a closed outbox key exists, pending rows ≥ `flush_max_rows` (500) or the oldest row ≥ `flush_max_age_s` (3600). A closed outbox bucket is final and can carry minutes of rows, so it is never held for `flush_max_age_s`; `flush_max_rows` applies to the per-minute cache buckets `pending` counts.

   `behind` is set when `collect` left sources unread (cache-op or call budget, row or group cap, truncated outbox listing). Without it, steady traffic (12–16 non-empty buckets a minute) outran one transaction per 5 minutes and pending grew to `MAX_PENDING_ENTRIES` (regression `every_bucket_busy_for_hours_drains_with_bounded_pending_and_scan_lag`).
7. **Not due:** purge ready hours, then CAS the cursor under the lease if it changed, unless a manifest is open.
8. **Due:**
   - Require `FIXED_CALLS + 1` (19) calls, otherwise `Deferred`.
   - Take the lease, re-check the cursor generation, then `collect` (`820`). The complete pre-check listing is reused, so the outbox is not listed twice.
   - `collect` takes sources round-robin from four classes: pending buckets, closed outbox keys, `audit:` keys after `audit_seq`, and `svc` outbox entries (≤ 200). Each class gets a turn, so a bucket backlog cannot starve the outbox or the audit sources (`a_bucket_backlog_cannot_starve_the_outbox_or_audit_sources`).
   - Each read is admitted only if the rest of the transaction still fits, counted from the actual file groups so far: `POST_COLLECT_CALLS` (7) + one PUT per group + one delete per outbox key + the svc trim (2, only if svc rows were taken) + the purge (2, only if one is ready) + the caller's reserve. Bucket reads also need a kept body or Core Cache ops above `FLUSH_CACHE_RESERVE`.
   - A batch is limited to 6 (kind, hour) groups and 5,000 rows (`MAX_COLLECT_ROWS`, `68`). The rest stays pending and sets `behind`.
9. Build the manifest:
   - `txn_id` (SHA-256, `763`)
   - `notes` rows (`note_rows`, `458`), stamped with the flush's single `now` reading that `collect` also budgeted their file group with, so a run crossing an hour cannot add an unbudgeted PUT
   - idle-tick rollup (`rollup_idle_ticks`, `403`)
   - outputs: one file per (kind, hour), deduplicated on `event_id` (`build_files`, `502`)
   - `next_cursor`

   CAS the manifest (`manifest_conflict` on loss).
10. **`run_txn`** (`777-827`):
    - PUT each file, stopping with `Stopped` once 40 s of wall time have passed (`WALL_STOP`, `29`).
    - Commit by cursor CAS against the manifest's `cursor_generation`. A failed CAS is `flush_fenced`.
    - No separate commit-mark write: a cursor carrying the `txn_id` is the commit, and the load treats such a manifest as committed. The GC's manifest write records it. The 2 calls saved go to outbox deletes.
11. **`finish_committed`** (`1258`). Every step keeps `FLUSH_TAIL_CALLS` back, so `record_flush`'s own row write always fits:
    - Delete the archived outbox keys and trim the `svc` outbox to the newest 50 (`gc`, `1083`; `trim_svc_outbox`, `1122`). The trim is skipped when no svc rows were archived.
    - Audit-key GC per D10 (`1147`): only archived keys older than the `admin_audit` retention, always keeping the newest 50.
    - Core Cache purge `flp-log-{kind}-{sid}-h{hour}` for every hour fully below `cache_min` (`purge_ready`, `1196`). It is skipped when no purge is pending.

**State**
- KV: `logs:cursor:{sid}`, `logs:flush_txn:{sid}`, `logs:flush_lease` (Add-CAS with a 120 s expiry, `695`), `logq_{sid}_*`, `audit:*`.
- Core Cache: buckets and surrogate purges.
- FOS: window Parquet files.

## Roles and permissions

Admin only. The manual flush, clear-cache and settings endpoints go through `shell.rs:778-787` and are audited.

## Manual triggers and admin controls

- `POST /api/admin/logs/flush` (`admin/logs.rs:174-207`) runs `log_flush` with `manual = true` and a fresh wall clock. It ignores the 5-minute floor.
  - Returns `{status, rows, files, purged, outbox_depth}`, or 502 on error.
  - A held lease returns `200 {status: "busy"}`.
  - Audited as `log_flush_requested`. No `cron_runs` row is written.
  - Wired to `/logs` (`app/web/app/logs/page.tsx:117`).
- `POST /api/admin/logs/clear-cache {kind | "all"}` (`logs.rs:209-239`, `flush.rs:1181-1249`) drops unflushed best-effort rows.
  - Errors: 400 `kind_not_clearable` (a durable kind), 409 `flush_in_progress`, 409 `scan_behind`.
  - Audited as `log_cache_cleared`.
- `GET`/`PATCH /api/admin/operational-logging` edits the flush settings (ranges in `telemetry/mod.rs:827-858`). Audited as `operational_logging_updated`.

## Telemetry

- `cron_runs` row with `cron_kind = log_flush` and `run_id` `flush-{now}-{hex4}-log_flush`, trigger and actor `clock`, no `tick_event_id`.
  - Outcome mapping: `Committed` → `done`, or `partial` with `more_pending` when `collect` left sources unread (`behind`); `Busy` → `busy`; `Deferred`/`Stopped`/`GcPending` → `partial`; `NotDue`/`Unprovisioned` → no row.
  - Fields: `rows_written`, `parquet_files_written`, `backend_requests_used`, `cache_lookups`.
- `platform_errors` notes inside the flush: `log_scan_expired`, `log_buffer_full`, and `logq_depth_high` when more than 60 closed outbox keys wait (`OUTBOX_WARN_DEPTH`, two hours of buckets).
  - A quiet service lets about 30 keys build up before the oldest reaches `flush_max_age_s` (3600 s), so 30 is the normal steady depth and only twice that means the flush is not keeping up.
- If `record_flush` cannot write its own `cron_runs` row, it records an `admin_event` `log.outbox_error`. The stage worker does the same.
- The flushed files carry these rows themselves. A flush's own row lands in the next flush.

## Constraints and gotchas

- Call cost:
  - A not-due flush needs 3–4 calls: cursor, manifest and outbox list, plus the cursor CAS when the scan advanced.
  - A due flush needs ≥ 19 calls plus GETs. `run_txn` skips the cursor re-read for a transaction it has just built.
- Scan lookups are Core Cache operations. They do not count against the call budget, but they do count against the 64-op budget. A 30-minute catch-up (480 lookups) used to exhaust the platform's cache-transaction limit and fail every later send in the same execution.
- Wall time is measured from the start of the `/_flush` execution. `Stopped` resumes from the manifest.
- `docs/runbook.md:351` says "Flush wall time 100 s". The code uses 40 s.
- The manual and automatic flushes share the lease. Clear-cache also takes it.
- The row's `cache_lookups` field records every Core Cache operation in the flush (lookups, replaces and purges), not lookups alone.
- **A closed outbox key makes the next flush due.** Before 2026-11-10 `check_due` counted each closed outbox key as one row, and the clock writes about one key per 2-minute bucket, so about 30 keys never reached `flush_max_rows` (500) and the flush waited for `flush_max_age_s` (3600). Live g2 showed `log_flush` rows about every 58 min and `cron_runs/live` reads paging through an hour of outbox under load. Now any closed outbox key is due once `flush_min_interval_s` (300) has passed, so a busy install archives about every 5 minutes, as the design intended, at the cost of one PUT per (kind, hour) group per flush.

## Failure modes and recovery

- **Crash before commit:** the next tick resumes the same manifest and re-PUTs identical keys (`manifest_is_deterministic_and_reused_after_a_failed_put`).
- **Crash after commit:** a cursor carrying the `txn_id` counts as committed, and only GC remains.
- **Stale executor:** fenced by the cursor generation.
- **Failed purge:** the hour stays in `purge_due` and is retried. The 24 h bucket TTL is a backstop.
- **Before FOS is provisioned:** nothing is written. Durable rows wait in KV. Global buckets expire after 24 h.
- `docs/runbook.md:226-236`: never delete a stuck manifest or cursor by hand.

## Tests

`app/src/telemetry/flush/tests.rs`:
- `scan_checks_only_new_closed_minutes_and_checkpoints_once` (293)
- `long_outage_scans_whole_minutes_within_the_cache_op_budget` (333)
- `a_fresh_cursor_looks_back_only_as_far_as_live_reads` (363)
- `due_rules_gate_flush_without_fos_calls` (338)
- `manifest_is_deterministic_and_reused_after_a_failed_put` (389)
- `held_lease_is_busy_and_stale_commit_is_fenced` (420)
- `commit_then_crash_before_gc_does_not_duplicate_rows` (466)
- `wall_time_stop_leaves_manifest_uncommitted_and_resumes` (487)
- `budget_admission_defers_and_splits_large_backlogs` (592)
- `every_bucket_busy_for_hours_drains_with_bounded_pending_and_scan_lag` (1042)
- `a_bucket_backlog_cannot_starve_the_outbox_or_audit_sources` (1095)
- `idle_ticks_roll_up_into_one_row` (538)
- `audit_key_gc_requires_archived_old_and_keeps_newest_fifty` (607)
- `purges_each_flushed_hour_once_after_it_closes_and_retries_on_failure` (687)
- `clear_cache_is_busy_during_a_transaction_and_scan_behind_for_one_kind` (733)
- `collect_caps_rows_per_flush_and_drains_the_rest_next_time` (887)

`app/src/telemetry/tests.rs`: outbox writer tests (CAS retry and merge, rollover, overflow key).

`app/tests/ingest.rs` (`mod simulation`):
- `the_log_flush_commits_and_keeps_up_under_sustained_ingest` drives `/_flush` and `/_tick` every second for 6 hours and checks commits, scan lag and the call and cache-op budgets.
- `two_days_of_1s_ticks_at_52_files_per_period_convert_exactly_without_a_backlog` asserts the outbox stays within 36 keys (30 buckets of `flush_max_age_s` plus slack) each day, drains to ≤ 2 keys at the end, and that audit rows are archived.

## TODOs

- Spec §2.5 differs from the code in three places:
  - The spec says a held lease returns 409; the code returns 200 `busy` (recorded in the master plan).
  - The spec says 100 s of wall time; the code uses 40 s.
  - The spec says stage order ingest → flush → raw_delete; the code runs flush after raw_delete and after the state save.
- `docs/runbook.md:351` still says the flush wall time is 100 s.
- A manual flush (`POST /api/admin/logs/flush`) writes no `cron_runs` row.
- `pending.nonempty` growth is uncapped (`docs/compute-limits-audit.md`).
- Wanted — not yet in a plan task (Drew 2026-10-30): KV operation, Core Cache replace and purge counters recorded per tick, so `/usage` can price them (see [clock.md](clock.md)).
- Live measurement of lookups, replaces, purges, KV ops and vCPU per flush is pending approval (plan Task 11.3).
- v3 parity: v3 `~/Projects/fastly-log-analytics/docs/cron/jobs/metric-snapshot.md` samples host vitals into Postgres. That is not applicable here, because the plan replaces it with `cron_ticks`/`api_telemetry` rows.

## Code

- `app/src/telemetry/flush.rs:29-52`: constants.
- `app/src/telemetry/flush.rs:69-143`: `Cursor`, `Manifest`, `FlushStatus`.
- `app/src/telemetry/flush.rs:220-285`: `scan`, `check_due`.
- `app/src/telemetry/flush.rs:403-564`: rollup, notes, files, lease.
- `app/src/telemetry/flush.rs:678-1004`: collect, `run_txn`, GC, purge, `flush_overdue` (992).
- `app/src/telemetry/flush.rs:1004-1249`: `log_flush`, `clear_cache`.
- `app/src/telemetry/mod.rs:38`, `48`, `126-157`, `795-858`: shards, scan cap, tiers, keys, settings.
- `app/src/admin/mod.rs:1527-1645`: `FLUSH_TAIL_CALLS`, `run_flush_stage`, `record_flush`.
- `app/src/shell.rs:18-23`, `428-440`: `CACHE_OP_BUDGET`, the `/_flush` route.
- `app/src/admin/schedule.rs`: `FLUSH_SCHED_KEY`, `flush_wanted`, `FLUSH_WAKE_S`.
- `app/vcl/front/main.vcl:81-101`, `182-188`; `app/infra/provision.py:196-240`: the probe.
- `app/src/admin/logs.rs:174-239`: flush and clear-cache endpoints.
