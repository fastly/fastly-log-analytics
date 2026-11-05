# Delete Data: design

Status: draft for review, 2026-11-04. This is a prerequisite for the fdlt
load harness (`2026-11-04-fdlt-load-harness-design.md`).

## Why

The fdlt load campaign fills the log bucket with millions of synthetic rows,
and they need to be wiped afterwards. fastly-log-analytics has a "Delete Data"
action (`POST /api/admin/reset-logs`) that does exactly this. Drew wants the
same action here, built first, so the harness can use it. It is also useful
on its own: any admin can reset a service's log history to a clean start
without tearing the service down.

## What it does (Drew's decisions)

- It wipes the monitored service's **request and RUM log data** back to an
  empty state.
- It keeps the service configuration, the operational logs (`processed/ops/*`)
  and the admin audit trail.
- It works like FLA's reset:
  - the moment the admin confirms becomes the **reset point**;
  - ingest pauses;
  - everything ingested before the reset point is deleted, along with raw
    files from before it;
  - ingest resumes, so logs that arrived during the wipe are kept.
- Logging is never stopped. Nothing is torn down and no key is revoked.

## Non-goals

- Deleting a time range from the middle of history.
- Deleting operational logs. That is already what delete-history does.
- Touching other installs, or the FOS bucket itself.
- Rewriting teardown.

## Anti-goals

- Leaving ingest paused because a browser tab closed or a step failed.
- Any reader seeing an inconsistent state, such as a catalog head pointing at
  deleted objects (`catalog_object_not_visible`), or a client stalled on
  `catalog_head_behind` because the generation went down.
- Deleted rows coming back: a pre-reset raw file being ingested after the
  reset, or an in-flight plan committing old files into the new head.
- Deleting `processed/ops/*`, which teardown's prefix-wide delete would do.
- Breaking any Compute limit in `AGENTS.md`.

## What "before the reset point" means

Processed data can't be split cleanly by time:

- window files are named by row time;
- one raw object feeds several windows;
- compaction merges windows within an hour;
- summary bundles cover whole hours and days.

So the wipe is defined by ingest state, not by row timestamps:

- **Processed side:** every processed request and RUM object, summary,
  summary bundle, and catalog, source-index, in-flight and compaction-work
  object that exists once ingest has been fenced. All of it came from ingest
  that ran before the reset point.
- **Raw side:** the reset point `T` is rounded down to a minute. Every raw
  request and RUM object whose key minute is before `T` is deleted. Objects
  at `T` or later are kept and ingested.

## How it works

### Phase 1: fence (ingest paused for about 7 minutes)

The fence waits `FENCE_WAIT_S` = the svc read-cache TTL (300s) + the plan lease (90s) + 30s = 420s. Every cached copy of `svc:<id>` must show the marker before leases are checked, because that cache is invalidated only in the POP that wrote it. It gives up at `FENCE_WAIT_S` + 3 maintenance intervals (780s). Logs that arrive during the pause are kept and ingested afterwards.

1. `POST /api/admin/services/{id}/delete-data` with `confirm_service_id` and
   `generation`. It needs the admin role, records the actor, and writes an
   audit entry. It refuses when another job is pending.
2. It sets a `data_reset` marker on `svc:<id>`:
   `{job_id, reset_at: T, phase, started_at}`. `dispatch` already reads that
   record every tick, so checking the marker costs no extra call.

   While the marker is in the `fencing` phase (that is, until the phase 2
   CAS):
   - **Skipped:** request and RUM ingest, `raw_delete`, `retention_sweep`,
     `summary_bundle`, `request_compact` and `storage_reconcile`. The
     `delete_data` stage runs instead.
   - **Gated in the same way:** `handle_worker_stage` (`stage.rs`).
   - **Refused with 409 `data_reset_in_progress`:** logs/sweep, backfill and
     quarantine purge.
3. Wait out and fence the work that is already in flight:
   - the schedule lease (55s);
   - plan leases (90s, with workers stopping PUTs 30s before expiry);
   - the compaction lease (70s).

   Then explicitly abandon:
   - any uncommitted `ingest:plan` or `ingest:rum:plan` (plan id ≠ the
     watermark's `run_id`), deleting its `processed/_inflight/<plan>/`
     outputs;
   - any in-progress multipart compaction upload, which is aborted;
   - `ingest:catalog_preparing`, `ingest:catalog_replacement_preparing` and
     `ingest:source_index_preparing`.

   Fencing works by lease expiry plus these explicit abandons. Nothing that
   started before the marker can commit afterwards.

### Phase 2: reset state, then resume ingest

4. One overwrite CAS of `ingest:watermark`. It is never a `kv_delete`, so the
   generation, `manifest_gen` and the coordination cache stay consistent.
   - `catalog.generation = old + 1`
   - `catalog.manifest` is a new empty Manifest with
     `batch: "reset"`. Add `"reset"` next to `"retention"` in `delta.rs`, so
     old delta walks return `catalog_resync_required`.
   - `recent` holds only the new event, so old SSE cursors get `resync`.
   - `days`, `statistics`, `hour_bundles`, `day_bundles` and `bundle_retired`
     are cleared, and `last_compaction` is set to None.
   - `retirement_grace_until = now + DELETE_GRACE_S` (660s). That also covers
     pinned snapshot cursors (`SNAPSHOT_SECONDS=600`).
   - `manifest_gen` is bumped, which invalidates the Core Cache hour
     listings.
   - The ingest cursor `key` is set to `raw_prefix + minute_dir(T)`.
     `passes` and `swept` are cleared, and the straggler `sweep` starts at
     `T`. `raw_sources`, `raw_scan` and `raw_scan_due` are reset.
     `run_id` stays equal to the last committed plan.

   The same CAS also writes an explicit **ingest floor**:
   `floor_key = raw_prefix + minute_dir(T)`. Every path that finds raw
   candidates rejects keys below the floor:
   - the start-after LIST in `plan_new` (`coordinator.rs:801`);
   - the recent scan (`recent_candidates`, used when `raw_scan_due`), which
     doesn't follow the cursor;
   - the 2h straggler sweep;
   - backfill;
   - RUM planning.

   Relying on the cursor alone isn't enough, because the recent scan can list
   minutes below it. With the floor in place, a pre-reset raw file can never
   be ingested again, even if it is still in the bucket or uploads late (up to
   `log_period + 120s`). The floor stays in the watermark for good; it is
   harmless once old keys are gone. A test pins each of those paths.
5. The same reset applies to `ingest:rum:watermark` and `ingest:rum:plan`.
   These are cleared or reset:
   - `ingest:backfill`, `ingest:deleteq:*` and `ingest:quarantine:*`;
   - `ingest:compact_state` and `summary:bundle_state`;
   - `retention:{request,rum,summary:request,summary_bundle:request}`.

   `retention:last_sweep` and `retention:purge` are left as they are.
   `history:*`, `logs:*`, `cron:state`, `flp-sched-v1` and `job:*` are kept.
6. Purge `flp-fos-<sid>` on the front service. That also evicts cached ops
   objects, which costs a refetch but loses no data. Then purge the status
   cache (`purge_status_cache`). Finally set the marker's
   `phase: "cleanup"`, which **resumes ingest**. From here on, new logs land
   in the new, empty catalog.

### Phase 3: background cleanup (ingest running)

In `cleanup`, every normal stage runs again: ingest, compaction, bundles and
retention all work on the new head. The `delete_data` stage still runs on
every maintenance tick, until the job is complete.

7. After `retirement_grace_until`, delete the old objects:
   - `raw/request/` and `raw/rum/` keys whose minute is before `T`;
   - `processed/request/` and `processed/rum/` objects not referenced by the
     new head;
   - `processed/summary/request/` and `processed/summary_bundle/request/`;
   - `processed/_catalog/`, except the objects the new head references;
   - `processed/_sources/`, `processed/_inflight/` and
     `processed/_compact_work/`.

   Because ingest is running again, new objects keep appearing. Processed file
   names end in a hashed plan id (`processed_key`, `ingest/mod.rs:248`), so the
   name doesn't say whether a file is old or new. Its write time does:
   - The CAS time from step 4 is stored as `reset_cas_at`. Fencing guarantees
     that no old plan writes after it, and new ingest only starts after the
     edge purge that follows it.
   - A processed, summary, bundle or `_catalog` object is deleted only if its
     LIST `LastModified` is earlier than `reset_cas_at`, **and** the new head
     doesn't reference it.
   - A new object is never deleted.

   Raw cleanup of keys below the floor waits until
   `T + log_period + 120s` (settle) has passed, so it catches late uploads
   too. After its LIST, it re-checks once more after another settle interval.

   The deletes use the `sweep_dataset` pattern (LIST 1000, persist pending,
   delete, one CAS on the way out) with `Store::delete_objects`. That is one
   `DeleteObjects` POST per 1000 keys, on the service's permanent FOS key, so
   no fresh Fastly token is needed. Deletes are recorded in `fos_usage` as
   usual. `processed/ops/` is never listed.
8. Run `request_storage_reconcile`, so the Usage page's storage figures match.
   Then clear the marker and mark the job complete in the audit trail.

### Progress and failure

- The `delete_data` stage is driven by the cron: it is added to the
  maintenance stages, the way delete-history is. It never depends on a
  browser tab. Each execution is bounded by calls, vCPU, wall time and heap,
  within the `AGENTS.md` limits, and resumes from KV state.
- **Before the phase 2 CAS,** the job can be cancelled. Cancelling clears the
  marker and ingest resumes with nothing deleted.
- **After the CAS,** the job only moves forward. A failing step retries with
  backoff, and the error shows on the Services row. Ingest is already
  running again in phase 3, so a stuck cleanup never pauses ingest. It only
  leaves old objects in place, and the report counts them.
- If phase 1 can't finish (for example, a lease is never released), it stops
  after a bound (3 tick intervals), clears the marker and fails the job.
  Ingest resumes with nothing deleted.
- Status: `GET /api/admin/services/{id}/delete-data` returns:
  - the phase;
  - counts deleted per prefix, bytes and pending;
  - the last error and timestamps;
  - `messages`: the last 50 `{at, text}` entries, which each step appends.
  The UI polls this, and the harness uses it too.

## UI

- In the admin Services table's "Manage" menu
  (`components/admin/AdminServices.tsx`), add **Delete Data**. It is a
  destructive item placed above "Teardown Service", and it is disabled while
  a job is pending.
- The dialog is modelled on `TeardownDialog`:
  - it explains what is deleted, what is kept, and that the delete can't be
    undone;
  - it states that logs arriving during the wipe are kept;
  - the admin types the **service ID** to confirm, matching this app's
    teardown dialog rather than FLA's name match (my call);
  - once started, it switches to a live execution panel modelled on FLA's
    (`TeardownDialog.tsx` `SSEExecutionSection` in fastly-log-analytics):
    - a phase and progress label;
    - a scrolling list of status messages, such as "Pausing ingest for this
      service…", "Waiting for in-flight ingest to finish (42s left)…",
      "Catalog reset; ingest resumed", "Deleted 12,340 objects under
      processed/request/" and "Storage figures reconciled";
    - a done message: "Log data deleted. New logs are already arriving.";
    - the error text if a step fails.

    The status endpoint returns the job's last 50 messages, each with a
    timestamp, and the dialog polls it every 2s. That replaces FLA's SSE
    stream, because this job runs on the cron, not inside the request.
    Closing the dialog doesn't affect the job, and reopening Delete Data on
    that service shows the running job's panel instead of a new confirmation.
- Update `app/web/openapi.json` for the two routes and regenerate the types
  with `npm run gen:types`. Then pass `tools/check-api-contract.ts`.

## Tests

- **Rust** (`app/tests/service_jobs.rs`, `app/tests/ingest.rs`,
  `app/tests/retention_state.rs`):
  - the marker skips and gates every listed stage and route;
  - fencing waits out leases and abandons in-flight plans, so an in-flight
    plan can't commit after the reset;
  - the head reset raises the generation, and old delta walks and SSE cursors
    resync rather than stall;
  - raw keys before `T` are never ingested after the reset, including a late
    upload named before `T`. Each candidate path is tested separately:
    start-after, recent scan, straggler sweep, backfill and RUM;
  - cleanup never deletes an object the new head references, or one modified
    after `reset_cas_at`, and never touches `processed/ops/`;
  - cancel before the CAS leaves everything unchanged;
  - executions stay within the call, vCPU and heap bounds, and resume
    correctly after interruption at every phase (using the `LaggingKv`
    CAS-lag fixture);
  - the edge purge happens after the reset.
- **Web:**
  - the dialog: the confirm input gates the button, and the dialog shows
    progress;
  - `manifest-delta.test.ts`: the `reset` batch triggers a fresh snapshot.

## Live verification (needs Drew's go)

1. Run it on the test install after a small fdlt smoke run.
2. Confirm these:
   - the dashboard is empty immediately after the reset;
   - new logs show up within the normal ingest delay;
   - the ops logs and audit trail are intact;
   - every prefix has been cleaned up;
   - the Usage page's storage figures were reconciled.

## My calls (for Drew to check)

- Progress is driven by the cron, not the browser.
- Ingest resumes as soon as the catalog and cursor have been reset, and old
  objects are deleted afterwards in the background. The pause is the fencing
  time only.
- The confirm text is the service ID, matching this app's teardown.
- `"reset"` is a new catalog batch name.
- A permanent ingest floor at the reset point is enforced on every
  raw-candidate path. It is a second guard, so the cursor isn't the only
  thing protecting deleted data.
- Old processed objects are identified by their write time
  (`LastModified < reset_cas_at`) plus a check that the new head doesn't
  reference them.
- The endpoints are `POST` and `GET /api/admin/services/{id}/delete-data`.

## CPU budget

The install's CPU limit is 1000ms per execution. It was raised, and the goal
is still to keep usage well below it. Delete Data stops at the same
`VCPU_STOP_MS = 700` that retention uses, leaving headroom for the final CAS
and the response.
