# ADR-16 — The Ingest Ledger: State-Machine Discovery and At-Least-Once Convert

**Status:** Accepted
**Decided by:** v3.0.0 scalability rework, `release/v3.0.0` commits `8fa0dfd1`, `0fc69f51`, `bc495144`

## Context

The v2 sync job (ADR-01's live-buffer tier) is a single per-service loop: LIST the FOS `raw/` prefix, download anything new, transform, commit. That loop is inherently single-writer — there is no way to fan a service's ingest work out across many Celery workers without either double-processing files or losing track of what's already been handled. Getting to 100k–1M RPS requires exactly that fan-out: many workers pulling from FOS concurrently for a single high-traffic service.

The missing piece is a shared, durable record of *what state every discovered object is in* — something every worker and the sweeper can agree on without talking to each other directly. That's the ingest ledger.

## Decision

`ingest_ledger` (one row per `(service_id, object_key)`, see schema in `backend/core/metadata/base.py`) is the single source of truth for celery-mode ingest state. Every row moves through this state machine:

```
discovered → claimed → committed
                     ↘ quarantined   (malformed content, all-NULL-row lines)
                     ↘ dead_letter   (repeated convert failures)
```

- **`discover_prefix()`** LISTs a FOS prefix and inserts new object keys as `discovered`. This is the only writer of new rows.
- **A convert task claims a row** (`claimed`, `claimed_by`, `claimed_at` stamped) before doing the actual transform-and-commit work, then marks it `committed` (`committed_at`, `snapshot_ref`) on success, or increments `attempts`/`last_error` and routes to `quarantined`/`dead_letter` on failure. Discovery and the sweeper dispatch **batches** of `LEDGER_CONVERT_BATCH_SIZE` keys (`convert_batch_objects()`, one DuckLake commit for the whole batch — see "Batched convert" below); `convert_object()` remains the single-key form the sweeper's dead-key retries and in-flight messages across a deploy still use.
- **`sweep_ledger_once()` is the crash net**, run on its own RedBeat schedule (`ledger_sweep_{id}`, see [ADR-15](15-multi-writer-topology.md)'s job-prefix split). It performs three independent recoveries every tick:
  1. Rows stuck `claimed` past `LEDGER_RECLAIM_AFTER_S` (worker died mid-convert) are reset to `discovered` — committed *before* re-dispatch, so a slow-but-alive worker's real claim can't race the reset and get silently reverted.
  2. Rows stuck `discovered` past `LEDGER_REDISPATCH_AFTER_S` (their Celery message was lost — broker restart, crash after the DB insert but before the task landed) are re-dispatched.
  3. A lookback-window FOS LIST is diffed against the ledger to catch objects discovery itself never saw (a missed LIST page, a discovery task that crashed before completing).
- **Convert is idempotent by construction**, so at-least-once redelivery (from the sweeper, from Celery's own `acks_late`/`visibility_timeout` dead-worker redelivery) is safe to duplicate — this is what makes the sweeper's re-dispatch safe to be conservative about.

### The lost-message guard

An earlier version of the sweeper unconditionally re-dispatched every stuck/reclaimed row on every tick. In practice this multiplied duplicate messages during any sustained backlog — observed live growing a queue from ~5k pending to 25k+ queued within a few sweep cycles, because each tick re-enqueued rows that were already sitting in the queue, just not yet picked up. The fix: before re-dispatching, check the live depth of `q.ingest` (`backend.celery_status.celery_queue_depths()`). If the queue already holds at least as many messages as the pending backlog, nothing is actually lost — the workers just haven't caught up — and the sweeper skips re-dispatch entirely. Re-dispatch only fires when the queue is demonstrably shallower than the backlog, which is the only condition that actually indicates a lost message. Convert's idempotency is what makes this a "when in doubt, skip" decision rather than a "when in doubt, resend" one — the cost of skipping-when-wrong (next sweep tick catches it) is far lower than the cost of resending-when-wrong (queue bloat that makes depth metrics meaningless).

### Batched convert

Convert was originally per-object: one Celery task, and therefore one DuckLake catalog commit, per discovered `.gz`. Measurement settled where the ceiling actually is. A live test service had 27,613 committed files (avg **665 bytes**, ~7 log lines each, ~44 files/minute) against **27,615 DuckLake snapshots** — exactly one catalog snapshot per file, one Postgres catalog transaction per file, and a snapshot table growing linearly with file count forever. File count is driven by Fastly's per-POP log-delivery fan-out and `log_period`, *not* by RPS (at higher RPS each file simply carries more lines), so Celery dispatch was never the problem: tens of tasks/sec is trivial. The catalog commit was.

`convert_batch_objects()` collapses a batch into **one** transaction — `DELETE … WHERE _source_file IN (…)` then a single `INSERT`, reading the whole batch through one `read_json_auto([…], filename=true)` call and joining that `filename` against a per-connection TEMP table that maps local temp path → `s3://` key. That `filename` join is what preserves per-row `_source_file` attribution, which is non-negotiable: idempotency (DELETE-by-`_source_file`) and the ledger both key off it. Measured: 8 files through the per-file loop produce 8 snapshots; the same 8 through the batch produce **1**, with all rows and all 8 distinct `_source_file` values intact.

The failure semantics split by blast radius. A key that is *gone from FOS* is dead-lettered alone and dropped from the batch — one dead key must never strand its 49 healthy siblings. A failure of the *shared transaction*, by contrast, is the whole batch's: every still-claimed key is routed through `_ledger_record_failure` so none is left stranded in `claimed`. Schema widening runs over the UNION of columns across the whole batch (outside the transaction, best-effort) so a custom field appearing in only one file still widens the table, and quarantine stays attributed per originating file by re-reading only the files that actually contain NULL-timestamp rows.

### Raw-file finalization is decoupled from commit

`finalize_committed_raw()` is the celery-mode counterpart of the sync path's `delete_after` handling. It deletes a raw `.gz` file only after its ledger row has been `committed` for at least `RAW_DELETE_GRACE_S` (10 minutes) — the grace period protects any reader still holding a pre-commit view of that file's data. Deletion is idempotent (`raw_deleted_at` stamped, checked before re-attempting) and honors the service's `provisioning.cron_sync.delete_after` config, matching the v2 sync path's existing contract so celery-mode services don't grow unbounded raw storage.

## Consequences

- Convert granularity is **per batch** (`LEDGER_CONVERT_BATCH_SIZE`, default 50, env-tunable), which is what keeps catalog snapshots proportional to batches rather than to files. Raising the batch size trades fewer snapshots for a larger blast radius on transaction failure and a longer task; lowering it walks back toward one snapshot per file.
- Any new failure mode in `convert_object()` / `convert_batch_objects()` must decide, explicitly, which of `quarantined` / `dead_letter` / plain retry (`attempts`/`last_error`, left `claimed` or reset to `discovered`) it belongs to — there is no silent "log and move on" outcome available; every row must land in one of the ledger's defined states. For the batch form, also decide whether the failure is per-file (isolate and drop the key) or transaction-wide (fail every still-claimed key).
- `discover_prefix()` remains the only row-creation path. A future ingestion entry point that bypasses it (e.g. a bulk backfill tool) must insert `discovered` rows through the same table, not write parquet directly — otherwise the sweeper and every downstream consumer of ledger state has no record the data exists.

## Out of scope

- Per-object retry backoff scheduling (currently: `attempts`/`last_error` are recorded, but backoff timing is Celery's default retry policy, not ledger-driven).
- ~~RUM ingest (`client_vitals`/`client_errors`)~~ — **ported** in commit `587d0b5f`: `backend/cron/jobs/rum_ledger.py` drives beacon discovery/commit through this same ledger, with `rum_discovery_` and `ledger_rum_sweep_` added to `_REDBEAT_JOB_PREFIXES`. The v2 `rum_sync`/`rum_commit` jobs remain as the sync-mode path.
