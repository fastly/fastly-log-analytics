# request_ingest

Converts settled raw gzip NDJSON objects in `raw/request/` into immutable Parquet window files in `processed/request/`, exactly once. A coordinator in the tick plans the work and fans it out to bounded worker executions.

## Purpose

This is the pipeline's only data producer. Fastly S3 logging writes raw objects to FOS, and this stage turns them into the window files that the browser queries.

## Why it exists

Fastly delivers logs only as raw gzip NDJSON. Analytics needs typed, windowed Parquet. Compute keeps nothing between requests, and one execution has only 32 backend calls, so a tick plans a batch, has peer worker executions convert it in shards, and records a durable commit point.

## How it works

Ingest is split between a **coordinator**, which runs inside the tick and owns all KV state, and up to 8 **workers**: peer `flp-worker` executions that each convert one shard. The worker contract is in [ingest-worker.md](ingest-worker.md).

**Trigger**
- Stage 1 of every non-busy tick (clock or manual) for an installed service with FOS credentials (`admin/mod.rs`, `record_tick`).
- `record_tick` calls `ingest::coordinator::coordinate` directly with `Options::request` (`stream` and `table` = `request`), `reserve = RAW_DELETE_CALLS` (5) so raw delete always fits, and one `extra` request: the tick's maintenance stage for `flp-worker` ([stage-worker.md](stage-worker.md)), sent in the same dispatch batch as the shards (`admin/mod.rs:1840-1868`). Usage is tagged `cron:ingest`.
- Ingest never yields a tick to the flush or to maintenance: the flush runs on its own `/_flush` probe ([log-flush.md](log-flush.md)).

**Plan v2** (`ingest:plan`, `coordinator.rs` `Plan`)
- Fields:
  - `version: 2`;
  - a frozen `plan_id` (12 hex, from window, schema, keys and a nonce);
  - `schema` (`convert::schema_hash`) and `window_s`;
  - `raw_keys` and `sizes`;
  - `shards` (id → keys), `results` and `failures`;
  - `attempt`, `lease_until`, `abandoning` and `replans`;
  - `segments` (shard id → decompressed byte range `{start, end?}`) and `segment_bytes`, only when large objects are segmented. Such a plan has `version: 3`.
- Every plan write is a generation CAS. A second write in the same tick first re-reads the plan and checks `plan_id` and `attempt` (`save_fenced`). On any mismatch the tick reports `busy` and writes nothing further.
- A plan whose non-null `version` is not 2 or 3 is left alone (`busy`), so a release never touches a plan from a different version.
- An uncommitted legacy v1 plan (no `version`) is finished once by the old in-tick path (`finish_legacy` → `finish`) before any v2 planning.

**Steps** (`coordinate` → `step`)
1. Read `ingest:plan` and `ingest:watermark`. A plan whose `plan_id` equals the watermark's `run_id` is committed, so a new plan may start. Otherwise the plan is resumed.
2. **New plan** (`plan_new`), started only in the first 10 s of the tick (`DISPATCH_BY`):
   - LIST `raw/<stream>/` after the watermark (at most 200 keys), keeping settled `.gz` keys only (minute + 60 s + `SETTLE_S`).
   - Width = `ceil(max(objects / 22, gz_bytes / 8 MiB))`, clamped to `1..=min(max_width, room) × max(ingest_relay, 1)`.
     - room = remaining calls − 1 (plan CAS) − 1 (`worker_secret` read by `worker_dispatch`) − 3 (writes after dispatch) − `reserve` − `TICK_RESERVE` − extra requests.
   - `pack` deals keys into shards of at most 22 objects (`SHARD_MAX_OBJECTS`, halved per replan) and 8 MiB gzip. It balances by estimated windows so each worker's window PUTs fit its budget.
   - `layout` first cuts a large object into segments: when its compressed size × 8 exceeds 2 × `segment_bytes` (and it is at most 24 MiB compressed), it becomes `ceil(gz × 8 / segment_bytes)` one-key shards, each covering `segment_bytes` of decompressed text and the last one open-ended. A line belongs to the segment holding its first byte, so the segments read every line exactly once. Segments may outnumber the width; the rest dispatch on the next ticks.
   - CAS the plan with `attempt = 1` and `lease_until = now + 90 s` (`LEASE_S`). A lost CAS reports `busy`.
3. **Resume** (`resume`):
   - A live lease (`lease_until > now`) makes the tick `busy`, with no writes.
   - Otherwise set `attempt + 1` and a new lease, then CAS. The lease is 20 s (`COMMIT_LEASE_S`) when every shard already has a result, since only the commit is left; otherwise 90 s.
   - If every shard already has a result, commit with no dispatch, even under another schema: nothing is converted any more.
   - Otherwise, if the plan's schema differs from this release's, mark the plan `abandoning = "schema_changed"` instead of dispatching.
4. **Dispatch** (`dispatch`): send pending shards (no result yet) in one `Runtime::worker_dispatch` batch to `POST /_internal/ingest`.
   - Worker calls = `min(max_width, room)`. With `ingest_relay` = *r* ≥ 2, up to calls × *r* pending shards are packed in order into groups of at most *r* whose relay body fits 60,000 bytes; a group of one is sent directly, a larger one as one `POST /_internal/relay` ([ingest-worker.md](ingest-worker.md)). Relay replies are unpacked per shard; a failed or unanswered relay fails each of its shards and keeps the lease, exactly like an unanswered worker.
   - The deadline is `32 s − elapsed` (`DRAIN_AT`); the worker stop is `min(deadline − 2 s, 28 s)`.
   - Each response is applied:
     - `ok` with a valid result → recorded. Valid means `result.attempt` equals the plan's attempt, there is a manifest, and there is one output per window, all named `-<plan_id>s<shard>-`.
     - `split` → the shard is replaced by halves `<id>a` and `<id>b`. A single-key shard cannot split, so it counts as a failure.
     - `segment` with the object's measured decompressed `total` → a whole or open-segment shard is replaced by segments `<id>r<i>` from its start, the last one open-ended, and the plan becomes `version: 3`.
     - `schema_mismatch` → held for a later tick.
     - error, timeout, or HTTP failure → a failure, with a `platform_errors` row. After 3 failures (`MAX_SHARD_FAILURES`) the plan is marked `abandoning = "shard_failures"`.
5. **Commit** (`commit`), once every shard has a result:
   - Fence: re-read `ingest:plan` and require the same `plan_id` and `attempt` with no `abandoning`. A tick that outlived its lease reports `busy` and writes nothing. A record of the same plan at a *lower* attempt is a stale read (KV answered from before this tick's own write; takeovers only raise the attempt): the tick re-reads once if it has budget and proceeds, since the watermark CAS still guards the commit. `save_fenced` re-reads on a stale answer the same way.
   - Add-CAS `ingest:deleteq:<plan_id>` = `{run_id, keys, sizes, inflight_keys, at}`. It lists converted raw keys and the shard manifests.
   - CAS `ingest:watermark` = `{key: last raw key, run_id: plan_id, at}`. This is the single commit point. If it fails, the tick reports the error or `busy`, emits no telemetry, and re-reads the watermark: the deleteq record is withdrawn only if the watermark does not name this plan (another tick that committed it owns the record). raw_delete never consumes the record of the current uncommitted plan, so a record left behind is harmless.
   - Segment results merge per raw key: the object is converted only if every segment converted, otherwise it is held with its first failing segment's reason. Rows already written by its converted segments stay committed.
   - Only after the commit, write the totals, one `ingest_files` row per raw object (`state_version = 1`), and the bad-line and quarantine `platform_errors` rows.
6. If the plan did not commit, it is saved with the new results (fenced). The lease is released (`lease_until = 0`) when every worker answered, so the next tick can continue at once.
7. **Abandon and replan** (`cleanup_and_replan`), when `abandoning` is set:
   - LIST `processed/_inflight/<plan_id>/` and read each manifest.
   - DeleteObjects every output a manifest names (each validated against the plan's token), then the manifests.
   - A manifest that cannot be parsed, has no `keys`, or names a key outside `processed/<table>/` and the `-<plan_id>s` token is logged (`platform_errors` `ingest_manifest_invalid`). Cleanup then sweeps `processed/<table>/` for the token: one LIST and one DeleteObjects per page, resumable via `sweep_after` in the plan. When every raw key has a minute, the sweep is bounded to the hour folders within 24 h (`ROUTING_TOLERANCE_S`) plus one window of the keys; otherwise it covers the whole table. Manifests are kept until the sweep finishes.
   - Before the replan, delete `ingest:deleteq:<plan_id>`, which a lost commit may have left. Once the plan id changes, raw_delete would otherwise treat that record as committed.
   - Guard: each manifest read needs `reserve + 6` calls left (the read, 2 DeleteObjects, the deleteq delete, and the fenced re-read + CAS).
   - Abandon runs only after the lease has expired, and workers start no PUT within 30 s of it (`WORKER_LEASE_MARGIN_S`), so no late write can land after cleanup.
   - Once all are gone, replan the same raw keys under a new `plan_id` with `replans + 1`. Dispatch happens on the next tick.

**State and outputs**
- KV: `ingest:plan`, `ingest:watermark`, `ingest:deleteq:<plan_id>`.
  - The plan is written at most twice per tick.
  - Writes to the same key are at least 1 s apart, because `admin_cas` waits `kv_write_delay_ms`.
- FOS outputs: `processed/<table>/<hour_dir>window=<start>-<end>-<plan_id>s<shard>-n<rows>.parquet`, exactly one grid window per file. Readers (`ingest/api.rs`) parse the token unchanged.
- FOS intent manifests: `processed/_inflight/<plan_id>/<shard>.json` = `{plan_id, shard, attempt, keys}`.
  - The worker writes its manifest before any output.
  - Manifests sit outside `processed/request/`, so readers never list them.
  - raw_delete removes them with the raw keys.

## Budgets

- Coordinator calls: 1–2 KV reads, at most 1 LIST, 1 plan CAS, 1 `worker_secret` read and 1 dispatch call per worker. Then either a fence read + deleteq + watermark, or a fenced re-read + CAS. The sustained-load test measured **12 calls** with 8 workers before the commit fence and the budgeted secret read added up to 2 more.
- Worker: its own 32 calls per execution. The measured max is **26** (see [ingest-worker.md](ingest-worker.md)).
- Core Cache:
  - Neither side does per-object or per-row cache operations.
  - The worker emits no telemetry; it returns usage and file rows in its response.
  - The coordinator only appends to the request's `LogBuffer`, which the tick flushes with a few bucket inserts per log kind.

## F4 entry point

```rust
pub fn coordinate<R: Runtime>(runtime: &R, service: &Service, access_key: &str, secret_key: &str,
    started: Instant, buffer: &LogBuffer, options: &Options) -> Result<Report, String>
```

- `Options { stream, table, reserve, max_width, extra }`:
  - `reserve`: calls to leave unused, on top of `TICK_RESERVE`.
  - `max_width`: caps the number of shard workers. It comes from config `ingest_fanout` (default 8, clamped 1..=8).
  - `extra: Vec<(path, body)>`: worker requests that ride in the same `worker_dispatch` batch as the shards. If no shard is dispatched, they are sent alone (only in the first 10 s). Their responses are in `Report::extra_results`, in order.
- `Report` keeps the fields `record_tick` reads and adds `shards_dispatched` and `shards_done`.

## Roles and permissions

There are no direct public endpoints. The stage runs from `/_tick` or from the admin-only `POST /api/cron-runs/trigger`. The worker routes `POST /_internal/ingest` and `POST /_internal/ingest-range` are served by the worker service and require `x-flp-worker-key`. The main service also accepts them with that key, only so a worker relay (`POST /_internal/relay` on the worker service) can reach a second worker through the peer; the relay forwards only those two paths, so the chain stops after two hops.

## Manual triggers and admin controls

- `POST /api/cron-runs/trigger` runs a full tick, including this stage, immediately.
- `GET /api/sync-status` (admin) reports the `earliest_log_at`/`latest_log_at` extents and the tick times. Its v3-shaped ingestion fields (`active_run`, `latest_ingested_file_at`) are always `null`.
- Config `ingest_fanout` (1–8, default 8) caps the worker width.
- Config `ingest_relay` (0–16, default 0) sends up to that many shards through each worker call as a relay; 0 or 1 dispatches directly.
- Config `ingest_segment_kib` (64–24576, default 24576) sets the decompressed segment size for request ingest. RUM is never segmented.

## Telemetry

- One `cron_runs` row (`cron_kind = request_ingest`) whenever the stage is not idle.
  - Outcome: `busy` (plan CAS lost, live lease, or fence miss), `partial` (`more_pending`), `done`, or `error`.
  - Fields: `listed_files`, `converted_files`, `lines_read`, `rows_written`, `bad_lines`, `bytes_written`, `parquet_files_written`, `backend_requests_used` (coordinator calls only), `duration_ms`, `vcpu_ms`.
  - `bytes_read` is always 0.
- One `ingest_files` row per raw object, written only after the commit.
- `platform_errors` for bad lines, quarantined objects, worker failures and abandons.
- Worker FOS usage rows (returned in each response) are buffered as `fos_usage` rows by the coordinator.

## Constraints and gotchas

- Throughput: up to 8 shards × 22 objects = 176 settled objects per tick, within the 200-key LIST page. With `ingest_relay` the shard count per tick rises to calls × relay, still within one 200-key LIST page per plan; see `docs/cost-methodology.md` for the capacity envelope. The sustained-load test holds 60 files a minute at one tick every 2 minutes with no growing backlog.
- Exactly once:
  - Output keys carry the plan and shard (`<plan_id>s<shard>`), so a retried shard rewrites identical keys.
  - Split halves write under new ids. This is safe because a shard splits before any PUT, so its parent wrote nothing.
  - A plan whose outputs might be partial is never committed. It is abandoned instead: its manifests name everything it could have written, and those files are deleted before the same raw keys are replanned under a new `plan_id`.
  - Every PUT is checked: the returned ETag must equal the body's MD5, or the output is not recorded.
- The settle rule waits 3 minutes past each minute directory. Objects that land under a minute already behind the watermark are never listed, and no sweep heals these gaps.
- `failed` and `quarantined` raw objects advance the watermark but are never queued for deletion, so they stay in `raw/` indefinitely.
- New plans and dispatches start only in the first 10 s of the tick. Responses are drained until 32 s.

## Failure modes and recovery

- **Crash at any point before the watermark CAS.** The plan, with any recorded results, resumes after the 90 s lease. Unrecorded shards are re-dispatched and rewrite identical keys.
- **Crash between deleteq and watermark.** The retried deleteq Add is create-only, so nothing is replaced. raw_delete skips the record of the plan in `ingest:plan` until the watermark names it, so raw objects are never deleted before their plan commits.
- **Worker timeout, lost reply, or HTTP error.** Counted as a failure and retried next tick; identical keys make the retry idempotent. After 3 failures the plan is abandoned and replanned with smaller shards.
- **Schema change between releases.** The plan is abandoned, its outputs are deleted via the manifests, and the new release replans the keys.
- **Stale coordinator** (its lease expired and another tick resumed). Its fenced write or commit fence fails, and it reports `busy` without writing.
- **Stale worker** (still running when the lease expires). It refuses to start a PUT within 30 s of `lease_until` (error `worker_lease_expired`).
- **Missing worker service or secrets.** `worker_dispatch` returns errors; the shards fail and are retried.

## Tests

`app/tests/ingest.rs`. The fake runtime runs the real worker in-process with a fresh 32-call budget per worker execution, and asserts that each stays within 32.
- `sustained_load_of_sixty_files_a_minute_drains_without_backlog`
- `a_crash_at_any_call_of_the_tick_never_duplicates_or_loses_rows`
- `lost_and_failed_worker_calls_are_retried_without_duplicates`
- `a_shard_with_too_many_windows_for_its_budget_splits_before_writing`
- `a_shard_failing_three_times_abandons_the_plan_and_replans_without_duplicates`
- `a_plan_from_another_schema_is_abandoned_and_its_outputs_replaced`
- `a_live_lease_makes_another_tick_busy_without_writes`
- `an_etag_that_is_not_the_body_md5_is_never_recorded`
- `outputs_are_window_files_named_by_plan_and_shard_and_readers_skip_manifests`
- `worker_ingest_authenticates_validates_and_reports_schema_mismatch`
- `extra_worker_requests_ride_the_same_dispatch_batch`
- `raw_delete_requeues_manifests_it_could_not_delete`

`app/tests/ingest_fanout_faults.rs` uses the same fake with real `fos_sk` envelopes, a charged `worker_secret` read and dispatch hooks:
- `worker_signs_with_the_secret_key_unwrapped_from_its_envelope`, `worker_delete_signs_with_the_secret_key_unwrapped_from_its_envelope`
- `dispatch_budgets_the_worker_secret_read_and_leaves_the_reserve`, `abandon_cleanup_leaves_the_reserve`
- `a_result_from_another_attempt_is_not_recorded`
- `raw_delete_skips_the_queue_of_a_plan_whose_commit_failed`, `a_plan_whose_shards_all_have_results_commits_despite_a_schema_change`
- `a_tick_that_lost_its_lease_does_not_commit`, `a_lost_watermark_race_keeps_the_winners_queue_and_writes_no_rows`
- `a_watermark_conflict_at_commit_releases_the_lease_for_the_next_tick`, `a_stale_plan_read_at_the_commit_fence_does_not_park_the_plan_for_a_lease`
- `an_invalid_manifest_falls_back_to_a_token_sweep`, `a_worker_past_its_lease_writes_nothing`
- The earlier exactly-once, settle, corrupt-object, deletion and legacy-plan tests.

## TODOs

- **Capacity:** worker fan-out (up to 176 objects per tick) and relayed fan-out are tested only in the fake runtime. Live throughput, heap use, worker wall time and the worker → main relay hop (Task 25.4) are unmeasured.
- F4: a tick budget for later stages, `/_internal/stage` for log_flush and maintenance, and round-robin across services.
- No `rum_ingest` stage exists yet. `Options` keeps `stream`/`table` so RUM can reuse the coordinator (plan section 9C).
- Wanted — not yet in a plan task (Drew 2026-10-30): a quarantine inspection view and purge. Failed and quarantined raw objects are never put on the delete queue, so they accumulate in `raw/request/`.
- `bytes_read` is never populated.
- A single object spanning more windows than a worker's budget allows fails its shard, because a one-key shard cannot split, until the plan is abandoned. The heap use of a 48 MiB decode has not been measured.
- `ingest:deleteq:*` records are never removed when `delete_after` is false.
- v3 parity, Wanted — not yet in a plan task (Drew 2026-10-30):
  - `full-sync.md`: a deep re-LIST of `raw/request/**` behind the watermark.
  - `gap-heal.md`: log accounting that compares the Fastly stats API with ingested rows.

## Code

- `app/src/ingest/mod.rs`: design notes, `Report`, `run_capped`, `load`, legacy `finish`.
- `app/src/ingest/coordinator.rs`: `Options`, `Plan`, `pack`, `coordinate`, `resume`, `dispatch`, `commit`, `cleanup_and_replan`.
- `app/src/ingest/worker.rs`: the shard worker ([ingest-worker.md](ingest-worker.md)).
- `app/src/ingest/storage.rs`: `Store::put_verified`, `get_direct`.
- `app/src/ingest/convert.rs`: limits, `schema_hash`, `convert_object`.
- `app/src/sdk.rs`: `FastlyRuntime::worker_dispatch`.
- `app/src/admin/mod.rs`: stage wiring and the `cron_runs` row.
