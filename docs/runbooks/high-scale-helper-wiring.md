# High-Scale Helper Wiring

Six small modules under `backend/high_scale/` were built ahead of a designed
capacity-guard feature and had no production importer. This runbook names the
production caller for each, the gate that keeps standard mode untouched, and
what each wiring deliberately does **not** do. It extends
[High-Scale Capacity and Recovery](high-scale-capacity-and-recovery.md) and
[High-Scale Request Logs Design](high-scale-request-logs-design.md).

## Ground rules

- Every caller below runs only in the high-scale topology. Backend callers sit
  behind `backend.config.is_high_scale_mode()`; the worker and ingest
  controller are high-scale-only processes. In `DEPLOYMENT_MODE=standard` no
  new code runs, no new query is issued, and every existing response and log
  line is unchanged.
- Nothing here disables ingest, compaction, replay, or deletion. Overload
  still increases lag and backlog; it never samples or discards events (see
  the capacity contract).
- New operator surfaces are admin-only. They live under `/api/admin/`, which
  `RemoteAccessMiddleware` blocks for analyst and remote sessions.

## Summary

| Module | Production caller | Gate | Effect |
|---|---|---|---|
| `disk_guard` | `backend/cron/jobs/metric_snapshot.py::_sample_high_scale_disk` (60 s global sampler) via `backend/high_scale/disk_sampler.py` | `is_high_scale_mode()` | Reads real serving-volume stats, evaluates the mode, logs transitions, exports gauges, surfaces mode in `GET /api/admin/health-snapshot` |
| `retention` | `GET /api/admin/high-scale/retention-recommendation` in `backend/routers/admin/high_scale.py` | `is_high_scale_mode()` | Returns a hot/warm recommendation; an explicit per-service retention setting (`provisioning.cron_sync.data_retention_days`) always wins; never writes config |
| `fairness` | `HighScaleWorkerLoop.run_once` work selection in `backend/high_scale/worker.py` | worker process (`HIGH_SCALE_ENABLED=1`) | Schedules (service, domain) pages through `FairScheduler` with optional per-tick page capacity and per-service weights |
| `projections` | `HighScaleIngestController` request-domain projection step in `backend/high_scale/ingest_controller.py` | high-scale ingest controller | Publishes `project_cmcd` rows to `cmcd_projection_facts` for request events that carry CMCD fields |
| `recovery_capacity` | `rebuild_serving_state_from_fos` / `rebuild_origin_projections_from_fos` in `backend/high_scale/recovery.py`, driven by `scripts/backfill_high_scale_origin.py --capacity-json` | explicit measurement supplied by the operator | Evaluates the recovery claim and records it on the `RecoveryReport`; recovery itself is never blocked |
| `topology` | `HighScaleConfig.validate()` / `HighScaleConfig.topology_profile()` in `backend/high_scale/models.py` (worker startup), `topology_from_environment` in `backend/high_scale/config.py` (health snapshot) | high-scale config | Single source of truth for profile names and replica/Keeper expectations; startup log and health snapshot expose the profile |

## `disk_guard`: periodic sampler, surfaced mode

Design source: the module contract ("the caller, a periodic sampler, mirroring
the standard-mode SystemHealthCard sampler, is responsible for reading real
volume stats") and the 20% protected reserve used by the retention policy.

- The sampler is the existing 60 s `metric_snapshot` global job, the same
  sampler that feeds the standard-mode System Health card. In high-scale mode
  it calls `ClickHouseClient.storage_health()`, which reads the serving
  volume's real `free_space` and `total_space` from `system.disks`, builds a
  `VolumeStats`, and runs `evaluate_disk_state`.
- `HIGH_SCALE_DISK_ONE_BATCH_BYTES` (default `0`) feeds `one_batch_bytes`, so
  the reserve floor is the greater of 20% or one batch.
- The latest state is held in process and returned by
  `GET /api/admin/health-snapshot` as `high_scale.disk` (mode, free fraction,
  reserve bytes, reasoning, sample age). In standard mode the `high_scale`
  key is omitted from the payload, so the response is byte-identical.
- Mode changes are logged once per transition as `high_scale.disk_mode_changed`
  with the old and new mode; warning level and above for `protective` and
  `emergency`. Gauges `high_scale_disk_mode_level` (0 normal, 1 warning,
  2 protective, 3 emergency) and `high_scale_disk_free_fraction` are recorded
  to metric history and the OTel meter.
- **Gating: none.** The capacity and design documents do not list any optional
  work to suspend at `warning`, `protective`, or `emergency`, and the product
  rule is to disable nothing beyond what the documents specify. The wiring
  therefore evaluates and surfaces the mode only. If a later design names
  optional work (for example background rebuilds or cold exports), gate it on
  `latest_disk_snapshot().state.mode` at that call site and document it here.

## `retention`: admin recommendation, never an override

Design source: the storage policy in the module (about 24 h hot, up to 30 days
warm, bounded by a protected reserve) and the request-logs design decision that
ClickHouse TTL follows each service's `data_retention_days`.

- `GET /api/admin/high-scale/retention-recommendation?service_id=...&events_per_second=...`
  with optional `average_row_bytes` (default 500) and `total_bytes`. When
  `total_bytes` is omitted the sampled serving-volume total is used; if neither
  is known the endpoint answers `409 high_scale_volume_unknown`.
- The response (`HighScaleRetentionRecommendationResponse`) carries the
  recommended hot and warm days, the reasoning, the service's explicit
  `provisioning.cron_sync.data_retention_days` (or `null` when never set), the
  `effective_retention_days`, and a `source` of `override` or `recommendation`.
  Any explicit setting always wins (`0` means keep forever, reported as a
  `null` effective window); the recommendation is advisory and nothing is
  persisted.
- Outside high-scale mode the endpoint answers `404 high_scale_not_enabled`.
- Each call logs `high_scale.retention_recommended` with the inputs and result.
- The design decision "no separate hot window" stands: the recommendation is a
  sizing aid for the wizard and operators. It does not change ClickHouse TTL.

## `fairness`: worker work selection

Design source: request-logs design, multi-service fairness ("pages are
scheduled round-robin per service via `backend/high_scale/fairness.py`"), and
the capacity contract ("per-service fairness guarantees minimum progress while
allowing idle capacity to be borrowed").

- `HighScaleWorkerLoop.run_once` builds one `WorkItem` per configured
  (service, domain) page and dispatches them through a persistent
  `FairScheduler`.
- `HIGH_SCALE_PAGES_PER_TICK` caps pages per tick. Unset or `0` means no cap:
  every page runs every tick, in the original service and domain order, so
  behavior is identical to the previous loop. A cap must be at least the
  number of services so every service keeps minimum progress; a smaller value
  fails at startup.
- `HIGH_SCALE_SERVICE_WEIGHTS` (`service=weight,...`, default weight 1) lets a
  dominant service borrow idle capacity. Undispatched pages stay queued and run
  on a later tick, so none is dropped.
- Each tick that defers work logs `high_scale.fair_dispatch` with dispatched
  and deferred counts.

## `projections`: CMCD from request events

Design source: ADR-21 (CMCD analytics are a request-event projection with their
own watermark) and the existing `cmcd_projection_facts` table and CMCD
watermark, which had no writer.

- After the existing request-domain projections, the ingest controller calls
  `project_cmcd` for each decoded event whose `cmcd` mapping is non-empty and
  publishes one `cmcd` batch through the serving publication. Batch identity is
  derived from the request manifest or object id, so a replay is idempotent.
- It is best-effort like the other projections: a failure is logged as
  `high-scale cmcd projection publish failed` and never fails or repeats the
  request ingest. The archive remains the recovery source for any projection
  gap.
- Events without CMCD fields produce no batch and no extra ClickHouse call.

## `recovery_capacity`: the recovery claim gate

Design source: capacity and recovery contract, "Recovery qualification" (a
recovery claim is valid only when the slowest stage sustains the required
replay rate for the target window while the live reservation remains
available) and the stage-by-stage arithmetic.

- `rebuild_serving_state_from_fos` and `rebuild_origin_projections_from_fos`
  accept an optional `RecoveryCapacityMeasurement`. When given, the capacity is
  evaluated before replay and the result is attached to the `RecoveryReport` as
  `capacity` and `recovery_claim_valid` (capacity feasible and rebuild
  complete). Without a measurement both are `None`.
- Recovery is never blocked by an infeasible measurement: restoring data
  matters more than the claim. An infeasible result logs
  `high_scale.recovery_capacity_gate` at warning level naming the limiting
  stage.
- `scripts/backfill_high_scale_origin.py --capacity-json PATH` loads a
  measurement file and exits `3` when the rebuild completed but the claim is
  not valid, so a qualification run can gate on it.

## `topology`: profile validation

Design source: capacity and recovery contract, "Capacity profiles" (the
portable production profile uses two ClickHouse replicas per shard and three
Keeper members; development profiles may use one replica and a reduced Keeper
topology but must not claim production availability).

- `HighScaleConfig.topology_profile()` resolves the profile through
  `build_topology_profile`, and `HighScaleConfig.validate()` uses it instead of
  a duplicated name list, so an unknown profile fails at worker startup.
- The worker logs `high_scale.topology_profile` at startup, and the health
  snapshot reports the profile named by `HIGH_SCALE_PROFILE` under
  `high_scale.topology` (`null`, with a warning log, when the name is unknown).

## Verification

Tests under `tests/high_scale/`, `tests/cron/`, and `tests/routers/` exercise
each real call path (sampler to health snapshot, endpoint over HTTP, worker
loop, ingest controller, recovery function, config validation), not only the
pure helpers. The route-table tripwire needs no allowlist entry for the new
endpoint because `/api/admin/` is blocked for analysts.
