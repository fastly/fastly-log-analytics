# Background Job Specification: `commit_{service_id}`

## 1. Overview

- **Scheduler job ID:** `commit_{service_id}`
- **Cron task name:** `commit`
- **Purpose:** Commit Standard-mode Parquet buffer files to DuckLake, or
  compact/finalize already committed ledger data in High-Scale mode.
- **Schedule:** `provisioning.cron_sync.commit_interval_mins`, default 5 minutes,
  minimum 1 minute.
- **Manual trigger:** `POST /api/admin/commit-storage`. Returns `202 Accepted`
  with a `run_id`; the commit runs asynchronously.
- **Cloud copy (Standard):** after a commit that committed files, the new DuckLake Parquet files are pushed to
  `{prefix}/ducklake/` in the service's FOS bucket on a background thread (`schedule_cloud_push`). `optimize`, `expire`
  and the RUM commit trigger the same push, which also deletes keys the catalog no longer references. Restore with
  `python -m backend.provision.cli restore-data --service-id <id>`. See [CONTRIBUTOR_GUIDE.md](../../../CONTRIBUTOR_GUIDE.md) trap 50.

The current job ID is `commit_{service_id}`. The older `log_commit_` label is
not the scheduler ID used by this branch.

## 2. Execution by deployment mode

| Mode | Scheduler and execution | Commit work | Data and metadata |
|---|---|---|---|
| **Standard** | In-process APScheduler | `commit_buffer` drains the local buffer to DuckLake. It uses the per-service commit lock, bounded file chunks, and a separate DuckLake write attach for each chunk. | Rows commit to the configured DuckLake data path. The catalog and operational metadata use PostgreSQL. Consumed local buffer paths are tombstoned and swept after the grace window; they are not immediately unlinked by the commit. |
| **High-Scale (`high_scale`)** | Scheduled on the in-process APScheduler (`commit_{service_id}`) while the cron lease is held. | Worker ingest has already written rows to DuckLake. The commit job calls `merge_lake_files`, which flushes inlined rows to durable Parquet before adjacent-file compaction, then publishes ledger progress. It calls `finalize_committed_raw` afterward; raw deletion obeys its configured grace period. | DuckLake uses the shared PostgreSQL catalog and configured durable data path. The job does not drain Standard buffer files. |

Standard and High-Scale are separate commit paths. Do not route High-Scale
through `commit_buffer` or treat its worker-side ledger writes as equivalent
to Standard buffer commits.

Both paths configure `DATA_INLINING_ROW_LIMIT 0` on every DuckLake attach. This ensures
that small inserts write directly to Parquet data files at commit time rather than
remaining inlined in the metadata catalog, guaranteeing immediate data durability and
natively preventing upstream duckdb/ducklake#1495 across concurrent attachments.

## 3. Scheduling and eligibility

The scheduler registers `commit_{service_id}` at the configured interval with
`max_instances=1`, `coalesce=True`, 30-second jitter, and
`misfire_grace_time=commit_interval_mins * 60`.

The cron skips missing configuration/source and disabled `cron_sync` (unless
forced). A cron-run lease
prevents overlapping runs. Standard mode checks disk space before touching the
buffer. A held Standard per-service commit lock skips the buffer cycle cleanly;
the next scheduled cycle can retry it.

## 4. Standard commit lifecycle

1. Load the service configuration and source, acquire the `commit` cron-run
   lease, and perform the disk-space pre-check.
2. `commit_buffer` acquires the per-service write lock, sweeps expired
   tombstones, and discovers unconsumed buffer Parquets.
3. `_commit_buffer_impl` processes files in bounded chunks. Each chunk gets
   its own DuckLake write attach and transaction. A batch failure falls back
   to per-file commits; unreadable files are quarantined when eligible.
4. A successfully committed chunk remains durable if a later chunk fails.
   Successfully committed buffer paths are tombstoned. Files not committed
   or quarantined remain eligible for a later scheduled cycle; the same tick
   does not retry a failed chunk.
5. Report accurate committed file/row counts. A later-chunk attach failure,
   an unquarantined per-file failure, or a quarantined unreadable file makes
   the cron run `error`, even if other files committed. Preserve partial
   progress in the summary and error detail.
6. When files did commit, refresh and warm the local DuckDB view/pool, refresh
   the cached service status and invalidate stale dashboard cache entries, and
   launch local compaction even if another chunk or file failed. The view/pool
   refresh remains synchronous so durable partial progress is visible before
   commit completion.
7. Empty input remains a successful no-op. The status refresh is not triggered
   when there were no committed files.

## 5. High-Scale commit lifecycle

1. The `commit` job runs inline on the worker and holds its cron-run lease for
   the complete operation.
2. `merge_lake_files(service_id)` obtains DuckLake write admission and a
   read-write attach, calls `ducklake_flush_inlined_data('lake')`, and then
   calls `ducklake_merge_adjacent_files('lake')`. The flush is required for
   durability: merging already-materialized files cannot promote inlined
   catalog rows.
3. The merge path publishes eligible ledger progress only after the flush.
   Schema-mismatch compaction can be skipped only through the existing
   documented branch; other failures propagate and make the cron run `error`.
4. After a successful merge, the job counts ledger files committed since the
   previous completed `commit` run and calls `finalize_committed_raw`. Raw
   `.gz` deletion is downstream of durable DuckLake commit and obeys the raw
   deletion grace period.
5. The job reports success only after merge and finalization complete. A
   failure is recorded on the `commit` cron run; raw files are not finalized
   after a failed merge.

The worker ingest path, not this cron job, commits per-object ledger writes.
The job does not directly transition claimed rows into committed state.

## 6. Roles and manual operation

| Caller | Commit behavior |
|---|---|
| **Admin (`read_write`)** | Background commits are enabled by configuration. An admin can trigger `POST /api/admin/commit-storage`; the response is `202 Accepted` with a pollable run ID. |
| **Analyst (live shared instance)** | Read-only analytics only. Admin commit endpoints remain blocked over the analyst share surface. |

## 7. Telemetry and status

`_run_commit` is wrapped by `@cron_task("cron_commit", job_name="commit")`.
The decorator provides cron telemetry attribution and usage-log flushing.
DuckLake/PostgreSQL operations use the existing instrumented connection paths.
The job records its `cron_runs` status and summary and emits commit progress
events. Partial counts must remain visible in an error summary; a completed
chunk is never presented as if the entire backlog succeeded.

Usage records are written to PostgreSQL `usage_log`. There is no SQLite
`cron_runs` store in the current architecture.

## 8. Failure and retry behavior

- **No buffer files:** successful no-op; no empty DuckLake snapshot is created.
- **Standard lock is held:** skip the cycle; leave files unchanged for a later
  scheduled run.
- **Later chunk attach/commit failure:** stop processing this tick, retain
  earlier durable chunks, report `error`, and retry remaining files on the next
  scheduled run. Do not extend the current write hold with an in-tick retry.
- **Per-file fallback failure:** report the file error and keep that file
  available for a later retry.
- **Unreadable buffer file:** quarantine it, report its count, and record the
  run as `error`. Successfully quarantined data is not silently represented as
  a full-success run.
- **High-Scale merge/finalization failure:** record `error`; do not proceed to
  raw-file finalization after a failed durable merge.
- **Post-commit status refresh failure:** it cannot undo durable commits. The
  failure is attached as a warning to the commit progress; the next commit
  refreshes the status again.

## 9. Verification checklist

### Automated tests

- Standard success, empty input, lock skip, and concurrent service commits.
- Per-file fallback reports actual Parquet row counts.
- Later-chunk and individual-file failures retain completed work, report
  errors, and retry only still-pending files on a later invocation.
- Quarantined files appear in summaries and make the Standard commit run
  `error`.
- Post-commit metadata sync dispatch is per-service coalesced, carries its
  own cron telemetry, and does not extend the commit cron duration.
- Request and RUM cron callers preserve partial counts and do not publish
  incomplete RUM ledger progress.
- High-Scale merge flushes inlined rows before raw finalization and keeps
  its separate failure/retry path.

### Deployment verification

- Deploy only after the change is tested, intentionally committed, and pushed.
- Verify on both Standard and High-Scale topologies.
- Capture commit outcomes and freshness/lag observations throughout the
  verification window for each topology; report maximum and p95 values
  wherever samples support them, not only a point-in-time snapshot.
