# log_compact

Merges a closed UTC day's operational window files into bounded `compact=` chunks, then deletes the merged sources through a KV delete queue.

## Purpose

The flush writes up to 288 window files per kind per day. Compaction cuts that file count so `/logs` history loads quickly. It applies to `processed/ops/{kind}/` only.

## Why it exists

Small files make browser reads slow and LIST-heavy. Request and RUM window files are compacted separately, by the compaction job (`ingest/compaction.rs`, `cron_kind` `request_compact` and `rum_compact`), not by this stage.

## How it works

**Trigger**
- Runs on `flp-worker` as one of the four round-robin maintenance stages (`compact_stage`, `admin/logs.rs:600-646`). See [stage-worker.md](stage-worker.md).
- It takes its turn only when all of these hold:
  - `compaction_enabled` is set (default true);
  - at least `TAIL (3) + MIN_COMPACT_CALLS (6)` = 9 calls remain in the stage execution;
  - secret `cdn_key` exists;
  - the run did some work. An idle run passes the turn to the next stage in the same execution.
- The function is called with reserve `TAIL + 1`.

**Coverage rule** (module doc `compact.rs:1-12`; `analyze`/`classify` `177-261`)
- `compact={a}-{b}-c{nsrc}-n{rows}-{tags}.parquet` lives in the `day=DD/` folder.
- `tags` are the sorted 8-hex FNV-1a hashes of the source window file names (`compact_key`, `104-111`).
- A compact file covers exactly those windows, whether or not they still exist.
- Legacy names without tags fall back to the `[a, b]` range rule. Such a file is valid only while all or none of its `nsrc` windows are listed.
- The browser applies the same rule (`app/web/lib/telemetry/engine.ts:38`).

**Steps** (`log_compact`, `compact.rs:619-733`)
1. Read KV `logs:compact_state`, which holds `done` per kind, `pending` queue ids and `checked` times.
2. For each kind, pick the days to process:
   - New days run from `done + 1` (or `today − min(retention, 30)`) up to `closed_last = (now − 2 h)/day − 1`.
   - The last 2 days are revisited at most every 6 h, to pick up late windows.
3. Process each day with `compact_day` (`400-512`):
   - **List.** Requires `room(6)`. LIST the day folder, 1,000 keys. If there is a next page, the day is `Blocked("compact_day_too_large")`.
   - **Requeue.** Find valid compact files whose sources are not queued. GET each one through the CDN and check its row count (`compact_rows_mismatch` on failure), then re-enqueue its sources.
   - **Size the chunk.** The chunk size is the largest `n ≤ 24` with `room(n + 2)`. `next_chunk` (`343-367`) also caps a chunk at 2 MiB of source bytes and 10,000 rows. A window over those caps stays loose.
   - **Build and write.** GET the sources via the CDN (`compact_source_missing`), merge and dedupe on `event_id`, then make **one PUT** of the chunk.
   - **Queue.** Only after the PUT, add the sources to `logs:compact_delete:{kind}:{day}` (`compact_queue_conflict`).
4. Days that hit the budget stop the run (`more_pending`). A `Blocked` day skips to the next kind.
5. Drain the delete queues (`drain`, `536-614`), unless the run stopped on wall time.
   - Each queue needs `room(3)`.
   - Only this service's `window=`/`compact=` keys in the queue's own day are deleted (`is_compactable`, `528-534`).
   - Deletion uses `delete_keys` with `worker_delete_fallback` forced off.
   - Failed keys are CASed back into the queue.
6. CAS `logs:compact_state` if it changed.

**Stop guards:** 40 s since the tick started (`WALL_STOP`, `41`) and 35 ms vCPU (`VCPU_STOP_MS`, `43`).

## Roles and permissions

There is no endpoint. The stage runs only from the tick. `compaction_enabled` is changed by an admin through `PATCH /api/admin/operational-logging`, which is audited.

## Manual triggers and admin controls

- `POST /api/cron-runs/trigger` dispatches one maintenance stage from `maint_next`, like a clock tick.
- There is no compaction status endpoint. `/logs` shows the `log_compact` runs.

## Telemetry

- A `cron_runs` row (`cron_kind = log_compact`) in buffer `{run_id}-maint` (`logs.rs:486-514`).
- Field mapping: `listed_files` = sources, `parquet_files_written` = chunks, `rows_written`, `bytes_written`, `deleted_files`, `more_pending`. `error` holds the first error.
- Outcome:
  - `done` or `partial` when anything was written or deleted;
  - `error` on an error;
  - `partial` when budget stopped the run;
  - no row when the run was idle.

## Constraints and gotchas

- A full 24-source chunk needs about `nsrc + 4` calls. The chunk is sized to fit the calls that remain, so an idle tick writes small chunks.
- Peak heap is bounded by the byte and row caps: 2 MiB source and 10,000 rows, well under 128 MB (`compact.rs:36-39`). This is not measured live.
- A day with more than 1,000 objects never compacts (`compact_day_too_large`).
- Compacted-away files still bill 30 days (`min_billed_days`, `docs/runbook.md:325`).
- Compaction and retention never run in the same execution. Retention removes a `compact=` file only with its expired day (`retention.rs:389-415`).
- The source comment in `drain` (`compact.rs:544`) says worker fan-out "is not charged to the tick budget". In fact `worker_delete` charges 1 call (`sdk.rs:729`). The fallback is disabled here either way.
- See `docs/compute-limits-audit.md` (Compaction row, "OK") and `docs/runbook.md:307-325`.

## Failure modes and recovery

- **PUT fails:** nothing is queued or deleted (`a_failed_commit_queues_and_deletes_nothing`).
- **Crash after the PUT, before the queue is written:** the next listing finds the valid compact file and requeues its sources (`rerunning_produces_the_same_keys_and_bytes_and_repairs_a_lost_queue`).
- **Wrong row count:** the compact file is ignored and no sources are deleted.
- **Partial source delete:** the compact file stays valid and no row is hidden.

## Tests

- `app/src/telemetry/compact.rs:749`, `764`: unit tests.
- `app/tests/ingest.rs`:
  - `closed_day_is_merged_in_chunks_of_at_most_24_and_deduplicated` (2535)
  - `coverage_is_the_recorded_source_set_and_a_late_window_in_range_is_still_read` (2584)
  - `partial_source_deletes_never_hide_rows_or_invalidate_the_compact` (2602)
  - `a_chunk_is_sized_to_the_calls_that_fit_and_never_exceeds_them` (2644)
  - `a_failed_commit_queues_and_deletes_nothing` (2680)
  - `request_and_rum_files_are_never_touched` (2695)
  - `the_delete_queue_only_removes_this_services_ops_window_and_compact_keys` (2857)
  - `a_window_over_the_row_cap_stays_loose_and_readable` (2938)
  - `compaction_stops_cleanly_at_the_wall_time_limit_and_resumes_next_tick` (2950)
  - `a_tick_with_a_backlog_stays_inside_the_lookup_and_call_limits` (2966)
  - `an_idle_tick_runs_log_compact` (2992)

## TODOs

- The spec says nothing of the round-robin turn, the tag suffix or the size caps. Master plan Goal 3 ("Where the code differs") records all three.
- Idle-tick headroom has not been measured live. Spec §4 says Task 0 measures it, and plan Task 11.3 (approval-gated) must show that "compaction and retention run on idle ticks within budget".
- A day folder with more than 1,000 objects stays blocked indefinitely. No split-listing path exists.
- v3 parity: `~/Projects/fastly-log-analytics/docs/cron/jobs/local-compact.md`, `rollup-compact.md`, `partial-hour-merge.md` and `optimize.md` compact request data on the server. That is intentionally not ported: master plan D1, table line 673, where compaction happens in the browser.

## Code

- `app/src/telemetry/compact.rs:1-43`: design notes and constants.
- `app/src/telemetry/compact.rs:104-261`: key format, `analyze`, `classify`.
- `app/src/telemetry/compact.rs:303-367`: queues, `next_chunk`.
- `app/src/telemetry/compact.rs:400-512`: `compact_day`.
- `app/src/telemetry/compact.rs:528-614`: `is_compactable`, `drain`.
- `app/src/telemetry/compact.rs:619-733`: `log_compact`.
- `app/src/admin/logs.rs:25-27`, `466-519`: constants and wiring.
