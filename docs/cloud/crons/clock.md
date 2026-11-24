# clock (`/_tick`)

The clock behind every background job. Every Fastly POP sends a health-check probe at the clock interval (half the shortest log period, clamped to 1–30 s — **30 s** for every saved Cloud service, whose log periods are at least 60 s). The IAD shield caches each answer for the sleep TTL Compute returns (up to 60 s while idle, dropping to a 1 s minimum floor only while work is due or a lease is held), so probes do not wake Compute every second when nothing is due. A turn that stops with work left starts the next one itself instead of waiting for a probe. A dispatcher decides, from a Core Cache schedule record, whether any job is due — ingest cadence follows the service's `log_period` (≥ 60 s), not a 1 s schedule. Most ticks are idle: they make no backend call and write nothing. A tick that is due runs ingest and raw delete, and dispatches one maintenance stage to `flp-worker`. A second probe, `/_flush`, runs the operational-log flush in its own execution ([log-flush.md](log-flush.md)).

## Purpose

Supplies the only schedule `flp` has. The dispatcher (`compute/cloud/src/admin/schedule.rs`) triggers:
- ingest, once per log period as soon as the period closes, and on every tick while a backlog remains (coordinator plus up to 8 shard workers). Request ingest has no grace (`INGEST_GRACE_S` is 0); RUM ingest waits `max(rum log_period, 60) + SETTLE_S` (120 s) past its period's close;
- raw delete, after a successful ingest, or on its own while deletes remain queued;
- one maintenance stage on `flp-worker` at most every 120 s (30 s while `summary_bundle` has work left). A pending Delete Data job advances first; then the first of the seven `MAINT_STAGES` with work runs, in round-robin order: `retention_sweep`, `log_compact`, `delete_history`, `storage_reconcile`, `summary_bundle`, `request_compact` (skipped here: it is its own job, below), `delete_data` ([stage-worker.md](stage-worker.md));
- request compaction, as its own compact-only job on `flp-worker`, every 30 s (10 s while it has a backlog) when compaction is configured.

It records only the jobs that ran. It is not a page. `/admin` and `/logs` display it and can trigger it.

## Why it exists

Compute runs only when a request arrives, and there is no server, scheduler process or memory that survives between requests. The front VCL service generates that request itself, so ingestion and housekeeping need no outside infrastructure. Every probe is a billed front request (2 checks × each probing POP × 86,400 ÷ the interval: about 9.1M a day at 1 s, about 305K at 30 s), and so is every Compute execution it causes.

## How it works

**Trigger**
1. The health check `flp_clock` sends `GET /_tick` at `check_interval` and expects 200 (`clock_healthcheck` in `compute/cloud/infra/provision.py`). Provisioning creates it at 1,000 ms; the first reconcile job, and every one after (a log settings save, or a deploy's reconcile), sets `check_interval` on both clock checks to `clock_interval_ms` (`compute/cloud/src/admin/reconcile.rs`): half the shortest of the request and RUM log periods (RUM counted at no less than 5 s), between 1 and 30 s. The reconcile's `front_clock` step clones, updates, validates and activates the front service only when the interval differs, using the install's own token; a dry run reports it as `front_changes`.
   - It is attached to a dummy backend that no request selects; its condition `flp_clock_never` is `"false"` (`provision.py:338-341`, `933`).
2. `main.vcl:171-179` allows only GET (anything else gets 405), drops the query string, marks the request `x-flp-tick` and removes cookies.
3. `main.vcl:299-323` strips cookies and security headers and caches the response with no grace or stale serving: for 1 s, or, when a 200 carries `x-flp-tick-ttl`, for that sleep TTL minus the response's `Age`.
4. `flp_tick` is shielded at `iad-va-us` (`provision.py:219-220`, `334-336`). The POPs probe every 30 s out of step; while idle, the shield caches Compute's sleep TTL (`x-flp-tick-ttl`, up to 60 s) so probes are absorbed at the shield, and while work is due or a lease is held the 1 s TTL floor caps Compute wakes to at most about one per second.
   - **Continuation:** a request that carries `x-flp-continue` and the CDN key in `x-fastly-key` (checked with `digest.secure_is_equal`) is passed past the cache on every hop (`x-flp-tick-pass`, `main.vcl`). Only `flp` holds that key; a client-sent `x-flp-tick-pass` is stripped.
5. The `/_flush` probe (`flp_flush` on backend `flp_flush_clock`) takes the same VCL path with its own key; see [log-flush.md](log-flush.md).
6. **Early answer.** `main.rs` is an undecorated `main` (`Request::from_client`/`send_to_client`). For a front-authorized `GET /_tick` or `/_flush` on `flp`, `Shell::clock_ack` (`shell.rs:422-429`) returns 200 `public, max-age=<ttl>` with `x-flp-tick-ttl: <ttl>`, which is sent before any work runs. The TTL is `clock_ttl_s` (`admin/mod.rs:1571-1589`, via `schedule::next_ttl_s`, `schedule.rs:277-297`): 1 s off the shield, with no cached record, while a lease is held or while work is wanted; otherwise the seconds until the next wake, capped at `REFRESH_S` (60 s). The execution then runs the handler, and logs its status as `work_status` on the `request.end` line. A health check therefore never waits on a long job.
7. `shell.rs:633-642` accepts only the exact path `/_tick` (anything else gets 403) and calls `record_tick(runtime, "clock", "clock")`.
8. **Continue.** After the turn, `Shell::continue_clock` checks `schedule::clock_continue_wanted`: on the shield, the record still wants a run and no lease is held. Then it sends `GET /_tick` through `flp_front` with the CDN key and `x-flp-continue: <depth>` (`FastlyRuntime::continue_clock`, 10 s timeout, logged as `clock.continue`), so the next turn starts at once instead of at the next probe. Depth stops at `MAX_CLOCK_CONTINUATIONS` (20); a failed continuation leaves the next probe to start the turn. `/_flush` continues the same way.

**Dispatch** (`record_tick`, `compute/cloud/src/admin/mod.rs:2083-2222`; `schedule.rs`)
1. Refuse to run on the worker service, or on anything that is not `flp`: `worker_tick_forbidden`, which becomes 403.
2. **Off the shield:** return 200 `off_shield` with no work. Only the IAD execution owns the Core Cache record, and a POP that misses the shield must not run jobs.
3. **Claim.** One Core Cache replace of `flp-sched-v1` (`schedule::claim`, `schedule.rs:570-621`). Concurrent replaces of one key are serialized (`ReplaceStrategy::Wait`), so two ticks from two POPs, or two executions in the same second, cannot both win. The claim:
   - always sets `last_seen`;
   - returns `Busy` while another execution's `lease_until` is in the future;
   - takes the lease (`owner` = this run's id, `lease_until` = now + 55 s) when a job is due, the mirror is older than `REFRESH_S` (60 s), or the record is missing (eviction rebuild);
   - otherwise counts an idle tick: `idle_ticks += 1`, `idle_vcpu_ms += this execution's vCPU`, and returns `Idle`.
   Idle and busy ticks end there: no backend call, no KV write, no log row.
4. **Due-ness** (`schedule::due`, `schedule.rs:367-399`), decided from the record's mirror of KV `cron:state.jobs` plus the service's `log_period`:
   - ingest: `sync_interval = max(5, log_period)` (v3, `MIN_PERIOD_S`). The 5 s floor never binds for a saved service: settings validation rejects a `log_period` (or RUM `log_period`) below `MIN_LOG_PERIOD` (60 s, `log_period_below_minimum` / `rum_log_period_below_minimum`), and loading a service raises an older, smaller value to 60 s. Period *p* is ready when `now - INGEST_GRACE_S ≥ (p+1) × period`, where `INGEST_GRACE_S` is 0: request raw objects are immutable once they are listed, so the schedule adds no partition-closing allowance and ingest polls as soon as the period closes. RUM ingest uses `rum_grace_s` = `max(rum log_period, 60) + SETTLE_S` (120 s) instead. Ingest is due when the newest ready period is past `last_success_period`, or when the last run left `more_pending` (catch-up on every tick until the backlog is gone);
   - raw delete: while the last run left `more_pending`;
   - maintenance: every `MAINT_INTERVAL_S` (120 s), or `MAINT_CATCHUP_S` (30 s) after a turn whose `summary_bundle` left work pending (the stage reply's `bundle_pending`; `schedule::maint_catching_up`). A turn on another stage keeps the previous flag, and a bundle turn that ends with nothing pending or only an error clears it. A Delete Data step that stopped only for its budget (`delete_pending`) makes maintenance due again on the next tick. Each turn runs the first rotating stage with work (the seven `MAINT_STAGES`, of which `request_compact` is always skipped); request compaction is not in the rotation;
   - request compaction: its own job, only while the service has compaction configured: every `COMPACT_INTERVAL_S` (30 s) once caught up, or `COMPACT_CATCHUP_S` (10 s) after a run that left `more_pending`. It runs as a compact-only `StageRequest` (`compact_only: true`) with the whole 20 s stage wall for chained worker passes, dispatched as a second `extra` beside the maintenance stage (or alone), so a compaction backlog drains at the job's own cadence instead of waiting for a maintenance turn;
   - a failed job backs off `RETRY_S` (30 s), a busy one `BUSY_RETRY_S` (5 s).
5. **Run** (`dispatch`, `mod.rs:2258-3157`). The lease holder reads `cron:state` and the installed service, recomputes due-ness from the durable state, and runs:
   - **request_ingest** through `ingest::coordinator::coordinate_reporting` with `reserve = INGEST_RESERVE` (7: `RAW_DELETE_CALLS` plus what `TICK_TAIL` needs beyond the coordinator's own `TICK_RESERVE`) and, when maintenance or compaction is due, `extra` requests: the `StageRequest` for `/_internal/stage`, starting at `cron:state.maint_next`, then the compact-only one. They go out in the same `worker_dispatch` batch as the shard workers, so each costs the tick 1 call. When only they are due, they are sent alone;
   - **raw_delete**, after a successful ingest or when only raw delete is due, keeping `TICK_TAIL` (6) calls: a GET and CAS so the `cron_runs` append joins its bucket's outbox key, its overflow fallback, and `CRON_STATE_CALLS`. With 4, every catch-up dispatch wrote its own overflow key;
   - for manual triggers only, a FOS manifest `meta/cron-runs/dt=…/{run_id}.json`.
6. **Record.** One `cron_runs` row per stage that ran. A due ingest is logged even when it found nothing (`outcome: idle`), so the per-period cadence stays visible; raw delete is logged only when it did something. The first row carries the idle counters handed over by the claim (`idle_ticks`, `idle_vcpu_ms`) and `worker_executions`, the flp-worker requests this execution sent (shards, extras, stage worker and delete fan-out) plus the peer executions each ingest relay started. A relay reports the shards it dispatched; a relay whose reply was lost or that failed counts its whole group, an upper bound because the tick cannot see how many it reached. The stage worker's rows come back in its reply. Rows are appended to the KV outbox, keeping `CRON_STATE_CALLS` (3) for the state save.
7. **Save.** `save_cron_state` CASes `cron:state` (merge and retry once on conflict): `last_tick`, `last_bucket`, `runs` (newest first, capped at 50), `jobs` (per-job `last_run`, `last_success_period`, `retry_after`, `more_pending`, `last_status`, `last_duration_ms`; merged so the later run wins and the ingested period never moves back), and the round-robin. A save error fails the tick instead of being swallowed. The round-robin moves only when the stored `maint_next` still equals the start this tick dispatched: a reply sets `maint_next = next`; two unanswered dispatches in a row move it on by one (`MAINT_MISS_LIMIT`).
8. **Release.** After the KV save, `schedule::release` (`schedule.rs:628-650`) clears the lease only if this run still owns it, stores the new mirror, and returns any idle counters that no row carried. A dispatch that fails before producing a mirror (a `cron:state` read, install load or state save error) sets the record's `retry_after` to now + `RETRY_S`, and `tick_wanted` stays false until then; otherwise the stale mirror would still say its jobs are due and the next 1 s tick would dispatch again. A successful dispatch clears it. KV is written first, so an evicted or expired record can never bring a finished job back.
9. Emit `cron.tick`, then purge the status cache.

**Without Core Cache.** If the claim itself fails, the tick emits `cron.schedule_unavailable` and dispatches from KV alone. Every job keeps its own fence (ingest plan and watermark CAS, the deleteq CAS, the flush KV lease), so a duplicate dispatch finds nothing to do.

**Budget**
- **Backend calls:** 32 per execution (`admin/transport.rs:45-89`).
  - Charged: KV get, CAS, list and delete; FOS requests; Fastly API calls; secret reads; worker calls.
  - Not charged: Core Cache lookups, replaces and purges. They count against the separate `CACHE_OP_BUDGET` (64 per execution, `shell.rs:12-16`), which keeps every execution under Fastly's undocumented Core Cache transaction limit. A running tick uses 4 (claim, release, two telemetry); an idle tick uses 1.
  - Telemetry writes have their own 8-call allowance (`transport.rs:48`, `62-89`).
- **Admission:** `admit_stage` is `remaining >= needed` (`admin/mod.rs:1690-1692`). The coordinator sizes its fan-out so its own calls, the shards, the stage request, `RAW_DELETE_CALLS` and its reserve fit in 32.
- **Wall time:** stage loops stop starting work 40 s after the tick began (`Store::within_wall`, `ingest/storage.rs`). The lease (55 s) covers that plus the tail. The stage worker stops at `STAGE_STOP` (20 s, `admin/stage.rs`); compaction stops at its own `WALL_STOP` and 35 ms vCPU.

**State**
- Core Cache `flp-sched-v1` at IAD (24 h TTL, not durable): `last_seen`, `owner`, `lease_until`, `idle_ticks`, `idle_vcpu_ms`, `refreshed_at`, `retry_after`, and the `mirror` (`configured`, `log_period_s`, `jobs`).
- KV `cron:state`: `last_bucket`, `last_tick` (last *run*, not last probe), `runs`, `jobs`, `maint_next`, `maint_misses`. Each stage's own keys are listed in its file.
- The service's `log_period` (provisioning config, `admin/model.rs`) sets the ingest cadence.

## Roles and permissions

- `/_tick` and `/_flush` need the front service's key (`front_authorized`), like every request. They are bounded by VCL (GET only, cached for at least 1 s unless a continuation carries the CDN key), by the exact-path check, by the shield check and by the service-id check.
- `GET /api/cron-runs`, `POST /api/cron-runs/trigger` and `GET /api/sync-status` are admin-only (`authorize_admin`).

## Manual triggers and admin controls

- `POST /api/cron-runs/trigger` (empty body) runs `record_tick` with `trigger = "manual"` (`shell.rs:979-1000`). It forces the claim, treats ingest, raw delete and (on the shield) maintenance as due, and writes the FOS manifest. It returns 409 `cron_busy` while a scheduled run holds the lease, and 409 when no service is installed. It does not flush; use `POST /api/admin/logs/flush` for that.
  - The web callers are in the Cloud adapter (`frontend/lib/cloud-engine/adapter.ts:944-955`, `2046-2050`): it maps a `POST /api/admin/ingest-logs` with no time range, and `/api/provision/ingest`, onto this trigger.
- `GET /api/cron-runs` returns `cron:state.runs` plus `last_tick_at`, `next_tick_at`, `clock_interval_s` (the probe interval in whole seconds, rounded up) and `clock` (`probe_interval_s`, `log_period_s`, `rum_log_period_s`), which `/logs` shows as "Each POP probes every 30 s (½ of the 60 s request log period)". `last_tick_at` is the schedule record's `last_seen` on the shield, else the last run in KV.
- The per-stage endpoints (flush, sweep, delete-history, clear-cache, reconcile) are documented in each cron file.

## Telemetry

- **Idle ticks write nothing.** They are counted in the schedule record and handed to the next run, which writes them as `idle_ticks` and `idle_vcpu_ms` on its first `cron_runs` row. `/usage` adds them to Compute requests and vCPU, together with one request per distinct clock `run_id` and each row's `worker_executions`. If the record is evicted, the counts since the last run are lost; the run rows are not.
- **`cron_runs`:** one row per stage that ran (`StageRow::json`), including vCPU, calls used, `cache_lookups` (Core Cache operations), `more_pending` and the idle counters. Durable, held in the KV `logq_{sid}_{bucket}` outbox batch keys. The stage worker writes the maintenance stage's row itself; `/_flush` writes the flush row.
- **`cron_ticks`** is retired: nothing writes it since the dispatcher replaced it. Older rows stay readable on `/logs` until retention removes them.
- **`cron:state.runs`:** the last 50 runs in KV (`MAX_HISTORY_ENTRIES`).
- **Other:** `cron.tick` and `cron.schedule_unavailable` admin events; manual ticks write the FOS manifest.

## Constraints and gotchas

- **No memory between ticks:** every resume point is a KV record. The Core Cache record is an accelerator, not a source of truth.
- **Executions:** each probe that reaches Compute is one billed execution, idle or not, and each continuation adds one front request and one execution while a backlog lasts. At one execution a second, `/_tick` would be about 86,400 a day, 2.6 M a month, and `/_flush` doubles that; the sleep TTL cuts this while nothing is due. The probes themselves are billed front requests whatever reaches Compute; the Cost Estimator models them from the log period (`clockIntervalS`, [cost-methodology.md](../cost-methodology.md)). See the cost note in [recommendation.md](../recommendation.md#1-s-clock-executions).
- **Wall time:** the limits assume the 60 s trial limit and a documented 50 ms CPU per execution. Every guard measures from the start of the execution. Nothing enforces 50 ms CPU except the compaction stop.
- **KV writes:** repeated writes to one key within an execution are spaced at least 1.1 s apart (`transport.rs:49-52`).
- **Cadence:** the probe interval is the clock checks' `check_interval`: 1000 ms at provisioning, then `clock_interval_ms` from every reconcile. `CLOCK_INTERVAL_S` (1) remains the shield cache's minimum TTL and the fallback without an installed service. The ingest cadence comes from `log_period` (at least 60 s); the maintenance cadence is `MAINT_INTERVAL_S`. The Config Store keys `grace_s` and `run_interval_s` are legacy: provisioning still writes them (180 and 120) and the setup check still requires them, but nothing reads them, so changing them has no effect. Log rows and the outbox still use 120 s buckets (`ROW_BUCKET_S`).
- **Off-shield probes** return `off_shield`. A POP that cannot reach the shield runs nothing, by design.
- **Stage order:** ingest (with the maintenance stage in its dispatch batch) → raw_delete → rows → state save → release. `TICK_STAGES` is `["request_ingest", "rum_ingest", "raw_delete"]` (`admin/mod.rs:1687`).

## Failure modes and recovery

- **The clock stops:** nothing runs. The "Ingestion clock" system check (`checks.rs:384-400`) reads the schedule record on the shield and fails when no tick arrived within two probe intervals (at least 10 s), reading the interval from the front's `flp_clock` health check. It uses `schedule::check_seen`, not the raw `last_seen`: while no lease is held, the clock counts as seen up to the record's next scheduled wake (capped at `last_seen + REFRESH_S`), because that is exactly how long the shield serves the cached sleep answer without a probe reaching Compute. Once that wake passes with no probe, freshness stays pinned to it, so the check fails about 10 s after a missed wake. While a lease is held, Compute answers with a 1 s TTL, so probes keep refreshing `last_seen`. Off the shield it falls back to `cron:state.last_tick` with a 300 s limit (`DURABLE_CLOCK_LIMIT_S`) and warns. Use the manual trigger until the clock is fixed.
- **Core Cache evicted or unavailable:** the next tick finds no record, takes the lease and rebuilds the mirror from KV; nothing reruns, because the durable job state already records the last runs. If the claim errors, every tick reads KV until the cache recovers.
- **A holder dies mid-run:** its lease expires after 55 s and the next tick takes over. The jobs' own fences stop duplicate work.
- **No service or control plane:** the tick stores an unconfigured mirror and does nothing until the next refresh.
- **Ingest errors:** raw delete is skipped and ingest backs off 30 s. The maintenance stage was already dispatched; the flush is independent.

## Verification checklist

1. "Ingestion clock" passes, and `last_tick_at` on `/api/cron-runs` is seconds old.
2. `request_ingest` rows appear about once per log period, each carrying `idle_ticks` close to the seconds since the previous run.
3. `more_pending` on `cron_runs` clears within a few ticks after load stops, with `clock.continue` log events between the turns of a backlog.
4. Both clock health checks on the front service show `check_interval` equal to `clock_interval_ms` for the current log periods.
5. No `logq_depth_high` warning.
6. The `ingest:watermark` minute advances and the raw backlog drains.
7. A manual trigger writes a manifest under `meta/cron-runs/` and an audit row.

## Tests

Native tests only; none is live evidence.
- `compute/cloud/tests/ingest.rs`:
  - `record_tick_logs_only_runs_carries_idle_ticks_and_keeps_fifty_runs` (2737)
  - `idle_ticks_and_flush_probes_make_no_backend_call_or_kv_write` (3024)
  - `duplicate_and_concurrent_ticks_run_a_due_job_once` (3173)
  - `an_evicted_schedule_is_rebuilt_from_kv_without_rerunning_jobs` (3220)
  - `ingest_is_scheduled_by_the_clamped_log_period_after_its_grace` (3269)
  - `idle_ticks_and_flush_probes_run_an_overdue_flush_sweep_and_reconcile_within_a_bounded_window` (3566)
  - `simulation::*` (from 8564): two simulated days of 1 s `/_tick` and `/_flush` probes at 52 raw files per 60 s log period, with per-execution call and cache-op budgets, exact-row checks and ingest latency
- `compute/cloud/tests/service_jobs.rs`: `shield_ticks_manual_trigger_fos_manifest_and_audit_logs_end_to_end`
- `compute/cloud/src/shell.rs`: `worker_compute_service_rejects_tick_while_coordinator_accepts_it` (early ack), `a_finished_turn_with_work_left_on_the_shield_starts_the_next_turn`, `a_clock_turn_continues_one_level_deeper_up_to_the_cap`
- `compute/cloud/src/admin/schedule.rs`: `a_turn_continues_only_while_the_clock_record_wants_a_run_and_no_lease_is_held`
- `compute/cloud/src/admin/reconcile.rs`: `the_clock_runs_at_half_the_shortest_log_period_between_1_and_30_s`; `compute/cloud/tests/service_jobs/provisioning.rs`: `reconcile_sets_the_front_clock_health_checks_from_the_log_periods`
- `compute/cloud/src/checks.rs`: `clock_allows_two_intervals_of_the_front_clock_health_check`
- `compute/cloud/vcl/front/tests/front.test.vcl`: `clock_continue_with_the_cdn_key_skips_the_cache`, `clock_continue_without_the_cdn_key_stays_cached`, `clock_ignores_a_client_sent_pass_marker`
- `compute/cloud/tests/route_auth.rs`: `tick_requires_exact_path`
- `compute/cloud/vcl/front/tests/front.test.vcl`: the 1 s TTL with zero grace, Compute's sleep TTL and header stripping, the shield `Age` subtraction, the 1 s floor and cached failures (279-364)

## TODOs

- **Capacity (native simulation):** at 52 raw files per 60 s period with 1 s ticks, ingest, raw delete, every maintenance stage and the flush keep up, with every execution within 32 calls and 64 Core Cache operations, and raw objects consumed on the first tick after their period closes (`INGEST_GRACE_S` is 0; the ignored `tests/ingest.rs` soak measured an ingest latency of 1 s, maximum and mean). Live confirmation is pending.
- **Code, spec and plan disagreements**
  - `grace_s` and `run_interval_s` are required at setup but never read (see Cadence above).

**Resource limits**
- Guarded limits enforced in Compute: `MAX_WINDOWS_PER_OBJECT` (12, `ingest/mod.rs`), `MAX_DECOMPRESSED_BYTES` (48 MiB, `ingest/convert.rs`), `MAX_PENDING_ENTRIES` (1,000, `telemetry/flush.rs`), `MAX_RECONCILE_KINDS` (32, `ingest/retention.rs`), and heap snapshots (`heap_mib`, `sdk.rs`).

**Operational telemetry & admin surfaces**
- KV operation, Core Cache replace, and purge counters per tick are recorded on every `cron_runs` row (`kv_ops`, `cache_replaces`, `purges`, `StageRow::json`) and summed by `/usage`.
- Clock health is surfaced as the "Ingestion clock" system check on `/admin` and the Cron Runs tab on `/logs`. Quarantine inspection and purge live in `QuarantineSection` on `/admin` (`frontend/app/admin/page.tsx`).

**Edition scope & parity**
- Cadence: health checks probe every 30 s per POP (`clock_interval_ms`, half the shortest log period clamped to 1–30 s) with shield sleep-TTL caching up to 60 s when idle; ingest follows `sync_interval = max(5, log_period)` (with `log_period` held to at least 60 s, so the 5 s floor never applies).
- Progress updates use polling on Cloud rather than SSE.
- High-Scale-only jobs not applicable on Cloud: `insights-prewarmer`, `alerts-evaluation`, `bot-data-refresh`, `ngwaf-sync`, `rdns-enrichment`, `share-audit-purge`.

## Code

- `compute/cloud/vcl/front/main.vcl:171-179`, `299-323`: method check and query drop, 1 s or sleep TTL.
- `compute/cloud/infra/provision.py:203-253`, `330-346`, `353`, `932-933`: backends, shield and health checks, `clock_interval_s`, conditions.
- `compute/cloud/src/main.rs:3`, `63`: the early clock acknowledgement.
- `compute/cloud/src/shell.rs:411-429`, `633-652`, `979-1000`: `clock_response` and `clock_ack`, `/_tick` and `/_flush`, the manual trigger.
- `compute/cloud/src/admin/schedule.rs`: the claim (`570-621`), due rules (`367-399`), sleep TTL (`277-297`), release (`628-650`) and `last_seen` (`659-683`).
- `compute/cloud/src/admin/mod.rs:1571-1589`, `1690-1731`, `1800-1836`, `2083-2222`, `2258-3157`: `clock_ttl_s`, budget constants, `record_flush`, `record_tick` and `dispatch`.
- `compute/cloud/src/admin/stage.rs`, `compute/cloud/src/admin/logs.rs`: the stage worker and `maintenance_stage`.
- `compute/cloud/src/admin/transport.rs:14-89`: wall timeouts and `BackendBudget`.
