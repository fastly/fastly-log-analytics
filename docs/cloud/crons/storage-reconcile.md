# storage_reconcile

An admin-requested, resumable job that LISTs every dataset with object sizes and records a byte/object snapshot. That snapshot anchors the estimated storage ledger.

## Purpose

Provides an exact-at-listing-time count of bytes and objects for:
- `raw/request/`
- `raw/rum/`
- `processed/request/`
- `processed/rum/`
- `processed/ops/`, with a per-kind breakdown

The `/usage` storage panel adds later `fos_usage` PUT bytes and subtracts deleted bytes on top of the snapshot. The result is labelled "estimated".

## Why it exists

Spec D13 (lines 268, 272, 342) rules out a recursive storage-tree walk on every page load, which would use too many calls and too much CPU for Compute. Instead, storage is a ledger: the last snapshot plus deltas. A full LIST takes many 1,000-key pages, so it runs as a bounded job spread across ticks.

## How it works

**Request** (`POST /api/admin/usage/reconcile-storage`, `admin/logs.rs:422-430`)
- `request_storage_reconcile` (`retention.rs:652-664`) Add-CASes KV `storage:reconcile`:
  - `{requested_at, actor, index: 0, token: null, totals: [(0,0)×5], by_kind: {}}`
- It returns `{started: false}` if a reconcile already exists. Only one runs at a time.

**Step** (`storage_reconcile_step`, `retention.rs:669-753`)
1. Read `storage:reconcile`. If it is absent, return `None` (no row).
2. While `room(3)` holds (the 40 s wall guard plus calls), LIST one 1,000-key page of `{scope}{folder}` for the current `index`. `scope` is the service's `fos_prefix`.
3. Add the sizes and counts to `totals[index]`. For `processed/ops/`, also add them to `by_kind[first path segment]`.
4. Advance `token`, or move to the next dataset. CAS the state after each page. A lost CAS returns `reconcile_state_conflict`.
5. After the last dataset:
   - build `{datasets: {name: {bytes, objects[, by_kind]}}, requested_at, completed_at}`;
   - `kv_delete storage:reconcile`.

**Tick wiring** (`admin/logs.rs:572-597`)
- The step runs on its round-robin turn in the stage worker ([stage-worker.md](stage-worker.md)), when `enough(RECONCILE_CALLS)` holds (`reconcile_stage`, `admin/logs.rs:694-722`).
- It runs with reserve `TAIL`.

**Output:** the snapshot is stored only as `details_json` on a `cron_runs` row with `outcome = done`. No KV or FOS object keeps it.

## Roles and permissions

Admin only (`shell.rs:488-501`, `778-787`). A request that starts a job is audited as `storage_reconcile_requested` with target `storage`. A request that finds one already running is not audited.

## Manual triggers and admin controls

- `POST /api/admin/usage/reconcile-storage` is called from the `/usage` page (`app/web/lib/usage/summary.ts:571-574`) and from `RetentionCleanup` on `/logs`.
- `GET /api/admin/logs/status` returns `storage_reconcile: {requested_at, actor}` while it runs (`logs.rs:148-160`).
- `POST /api/cron-runs/trigger` advances it right away.
- The job cannot be cancelled.

## Telemetry

- Each step writes a `cron_runs` row with `cron_kind = storage_reconcile`, buffer `{run_id}-maint`, caller `cron:storage_reconcile`.
  - `partial` + `more_pending` while unfinished;
  - `done` + the snapshot once complete;
  - `error` on failure.
- The usage summary reads the snapshot from `cron_runs` (`reconcileSnapshot`, `summary.ts:185-206`).

## Constraints and gotchas

**Cost**
- Whenever the turn reaches reconcile, the stage execution spends 1 GET on `storage:reconcile`, even when nothing was requested.
- Each page costs 1 LIST, a CAS and a GET.

**Accuracy**
- The snapshot is not point-in-time. Ingest, flush, compaction and retention keep changing the tree while pages are listed across ticks.
- `by_kind` keys come from FOS keys, so their count is unbounded, and the whole map is rewritten into the KV state on every page. It is listed as a residual in [compute-limits-audit.md](../compute-limits-audit.md) (row 27, residual line 40).

**Where the snapshot lives**
- It exists only in `cron_runs`, so it ages out with that kind's retention (30 days by default).
- It is invisible to `/usage` once it falls outside the queried window.

**Web summary**
- `reconcileSnapshot` picks the newest row with outcome `done` **or `partial`**. Partial rows carry no `details_json` datasets, so a reconcile in progress hides the previous completed snapshot and counts each dataset as 0 + deltas.

**Fan-out**
- Compute cannot call its own domain, and this job never fans out to a peer. Large trees take many ticks.

## Failure modes and recovery

- **Crash between pages:** the KV cursor resumes. A page whose CAS did not land is listed again. Its totals were never persisted, so it is not double counted.
- **CAS conflict or LIST error:** the row is `error`, the state stays, and the next tick retries.
- **Crash after the final LIST:** the last page is not CASed. The key is deleted before the row is buffered, so a crash between the delete and the row write loses the snapshot. Request a new reconcile.
- **Corrupt state** (`reconcile_state_invalid`): every tick fails until the key is deleted by hand. No endpoint clears it.

## Tests

- `app/tests/ingest.rs`:
  - `storage_reconcile_does_nothing_until_requested` (1966)
  - `storage_reconcile_sums_bytes_and_objects_per_dataset_across_resumed_steps` (1974)
  - `sweep_and_reconcile_endpoints_run_on_demand_and_are_audited` (2284)
  - `retention_loops_stop_at_the_wall_and_resume` (3026)
- Web: `app/web/lib/usage/summary.test.ts` and `app/web/components/usage/UsagePage.test.tsx` mock the endpoint.

## TODOs

- `reconcileSnapshot` should select only `done` rows. As written, a newer partial row hides the snapshot.
- Persist the last snapshot somewhere longer-lived than `cron_runs` retention, or document the dependency. The spec does not decide this.
- Bound or cap the `by_kind` cardinality ([compute-limits-audit.md](../compute-limits-audit.md) residual).
- There is no cancel or reset endpoint for a stuck or corrupt reconcile.
- v3 parity: v3's usage-log storage cost and "sync and storage state" views (`~/Projects/fastly-log-analytics/docs/pages/admin/usage-log.md`, `overview.md:27`) map to this ledger and `/usage`. Spec D13 covers this, and it is implemented as an estimate.

## Code

- `app/src/ingest/retention.rs:626-664`: key, dataset list, state, request.
- `app/src/ingest/retention.rs:669-753`: `storage_reconcile_step`.
- `app/src/admin/logs.rs:422-430`: endpoint.
- `app/src/admin/logs.rs:694-722`: stage wiring.
- `app/web/lib/usage/summary.ts:185-206`, `273-285`: snapshot and ledger.
- `docs/runbook.md:327-333`: operator notes.
