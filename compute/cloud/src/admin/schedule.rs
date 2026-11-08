//! The 1 s dispatcher's schedule.
//!
//! The front service's `/_tick` probe reaches flp about once a second at the IAD shield. Each
//! tick claims the Core Cache schedule record with one serialized replace (`ReplaceStrategy::
//! Wait` queues concurrent replaces of the key, so two ticks cannot both take the lease). The
//! claim either records an idle tick (`last_seen` and the idle counters, nothing else: no
//! backend call and no KV write) or takes the lease (`owner`, `lease_until`) for one execution
//! that then loads the durable state and runs the jobs that are due. The durable truth stays in
//! KV `cron:state` (`CronState::jobs`); the record mirrors it and is rebuilt from it after an
//! eviction. Completion writes KV first and then releases the lease with an owner-checked
//! replace, so an evicted or expired record can never bring a finished job back.
//!
//! Every job keeps its own fencing (ingest plan/watermark CAS, the deleteq CAS, the flush KV
//! lease), so the claim is what keeps idle ticks free; a duplicate dispatch after a lost record
//! finds nothing to do.

use super::model::CronState;
use crate::shell::{CoreCacheEntry, Runtime};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::time::Duration;

/// One record per install (one install logs one service in its own account).
pub const SCHED_KEY: &str = "flp-sched-v1";
/// The `/_flush` probe's own wake record.
pub const FLUSH_SCHED_KEY: &str = "flp-flush-sched-v1";
/// The holder's whole execution: the 40 s work guard, the telemetry and state tail, margin.
pub const LEASE_S: u64 = 55;
/// v3's `sync_interval = max(5, log_period)`.
pub const MIN_PERIOD_S: u64 = 5;
/// Available immutable request objects need no partition-closing allowance.
pub const INGEST_GRACE_S: u64 = 0;

pub fn ingest_grace_s(_log_period_s: u64) -> u64 {
    INGEST_GRACE_S
}
/// One maintenance stage dispatch per this many seconds (the 120 s clock's old cadence).
pub const MAINT_INTERVAL_S: u64 = 120;
/// While request compaction has a backlog, maintenance turns come this often instead. Each turn
/// compacts first, so this sets the backlog's drain rate.
pub const MAINT_CATCHUP_S: u64 = 30;
/// How often an idle schedule re-reads KV, so config changes (log period, a new install) and
/// writes made off the shield reach the mirror without a job running.
pub const REFRESH_S: u64 = 60;
/// Backoff after a failed job; a busy one retries after `BUSY_RETRY_S`.
pub const RETRY_S: u64 = 30;
pub const BUSY_RETRY_S: u64 = 5;
/// Steady-state idle `request_ingest` dispatches buffered in Core Cache per durable KV flush.
pub const IDLE_WRITE_BATCH: u32 = 6;
/// An idle `/_flush` probe wakes a real flush this often; one that left work wakes the next.
pub const FLUSH_WAKE_S: u64 = 60;
const RECORD_TTL: Duration = Duration::from_secs(86_400);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobState {
    /// Attribution from actual execution; legacy global state is not service evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<String>,
    #[serde(default)]
    pub last_run: Option<u64>,
    /// Ingest only: the newest period index (`(t - ingest_grace_s) / period`) fully ingested.
    #[serde(default)]
    pub last_success_period: Option<u64>,
    /// Clamped cadence of `last_success_period`. Legacy or changed-cadence watermarks are
    /// ignored until a successful bounded ingest replaces them; backlog and fencing stay intact.
    #[serde(default)]
    pub last_success_period_s: Option<u64>,
    #[serde(default)]
    pub retry_after: u64,
    /// The last run left work: due again on the next tick (catch-up).
    #[serde(default)]
    pub more_pending: bool,
    #[serde(default)]
    pub last_status: String,
    #[serde(default)]
    pub last_duration_ms: u64,
    /// Consecutive `request_ingest` runs that found no raw files and left no pending work.
    #[serde(default)]
    pub idle_streak: u32,
}

impl JobState {
    fn success_period(&self, log_period_s: u64) -> Option<u64> {
        self.last_success_period
            .filter(|_| self.last_success_period_s == Some(period_s(log_period_s)))
    }

    pub fn record_success(&mut self, period: u64, log_period_s: u64) {
        self.last_success_period = self.success_period(log_period_s).max(Some(period));
        self.last_success_period_s = Some(period_s(log_period_s));
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jobs {
    #[serde(default)]
    pub ingest: JobState,
    #[serde(default)]
    pub raw_delete: JobState,
    #[serde(default)]
    pub maint: JobState,
    /// The RUM stream's ingest, on its own log period.
    #[serde(default)]
    pub rum: JobState,
}

impl Jobs {
    /// The later run wins. Watermarks advance monotonically only within the same known cadence.
    pub fn merge(&mut self, ours: &Jobs) {
        for (mine, theirs) in [
            (&mut self.ingest, &ours.ingest),
            (&mut self.raw_delete, &ours.raw_delete),
            (&mut self.maint, &ours.maint),
            (&mut self.rum, &ours.rum),
        ] {
            let same_cadence = mine.last_success_period_s.is_some()
                && mine.last_success_period_s == theirs.last_success_period_s;
            let success = mine.last_success_period.max(theirs.last_success_period);
            if theirs.last_run >= mine.last_run {
                *mine = theirs.clone();
            }
            if same_cadence {
                mine.last_success_period = success;
            }
        }
    }
}

/// What the dispatcher needs to decide due-ness without reading KV.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mirror {
    pub configured: bool,
    pub log_period_s: u64,
    pub jobs: Jobs,
    /// The RUM endpoint's log period; `0` while RUM is off.
    #[serde(default)]
    pub rum_log_period_s: u64,
}

impl Mirror {
    pub fn from_state(state: &CronState, log_period: Option<u32>) -> Self {
        Self {
            configured: log_period.is_some(),
            log_period_s: log_period.map_or(crate::logging::DEFAULT_LOG_PERIOD as u64, u64::from),
            jobs: state.jobs.clone(),
            rum_log_period_s: 0,
        }
    }

    /// Schedules RUM ingest on `rum_log_period` (`None` while RUM is off).
    pub fn with_rum(mut self, rum_log_period: Option<u32>) -> Self {
        self.rum_log_period_s = rum_log_period.map_or(0, |p| u64::from(p.max(1)));
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredIdleRun {
    pub run: super::model::CronRun,
    pub row: serde_json::Value,
}

/// The Core Cache record. Not durable: everything but `last_seen` and the idle counters is
/// rebuilt from KV.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sched {
    #[serde(default)]
    pub last_seen: u64,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub lease_until: u64,
    #[serde(default)]
    pub idle_ticks: u64,
    #[serde(default)]
    pub idle_vcpu_ms: u64,
    #[serde(default)]
    pub refreshed_at: u64,
    #[serde(default)]
    pub mirror: Mirror,
    /// The flush record only: when an idle `/_flush` probe next starts a real flush.
    #[serde(default)]
    pub wake_at: u64,
    /// The clock record only: a dispatch that failed before writing a mirror holds further
    /// ticks off until then.
    #[serde(default)]
    pub retry_after: u64,
    /// `cron:state` generation from the last durable KV read/write on the shield, so deferred
    /// idle dispatches detect when an external writer (such as `backfill`) updates `cron:state`.
    #[serde(default)]
    pub cron_generation: Option<String>,
    /// Steady-state idle `request_ingest` dispatches buffered in Core Cache until the
    /// [`IDLE_WRITE_BATCH`]-th idle dispatch or the next productive/maintenance/manual dispatch
    /// flushes their `CronRuns` outbox rows and `cron:state` history together.
    #[serde(default)]
    pub deferred_idle: Vec<DeferredIdleRun>,
}

/// Whether the clock record wants a run: past any dispatch backoff, a job is due or the mirror
/// is stale.
pub fn tick_wanted(record: &Sched, now: u64) -> bool {
    now >= record.retry_after
        && (now >= record.refreshed_at + REFRESH_S || due(&record.mirror, now).any())
}

/// When [`tick_wanted`] next becomes `true` for `record`.
pub fn tick_wake_at(record: &Sched) -> u64 {
    let refresh_at = record.refreshed_at.saturating_add(REFRESH_S);
    let due_at = if record.mirror.configured {
        let jobs = &record.mirror.jobs;
        let ingest_at = if jobs.ingest.more_pending || jobs.ingest.last_success_period.is_none() {
            jobs.ingest.retry_after
        } else {
            next_ingest_at(&record.mirror)
        };
        let raw_delete_at = if jobs.raw_delete.more_pending {
            jobs.raw_delete.retry_after
        } else {
            u64::MAX
        };
        let maint_at = if jobs.maint.more_pending {
            jobs.maint.retry_after
        } else {
            jobs.maint
                .last_run
                .map_or(0, |last| last.saturating_add(MAINT_INTERVAL_S))
                .max(jobs.maint.retry_after)
        };
        let rum_at = if mirror_rum_off(&record.mirror) {
            u64::MAX
        } else if jobs.rum.more_pending || jobs.rum.last_success_period.is_none() {
            jobs.rum.retry_after
        } else {
            next_rum_at(&record.mirror)
        };
        ingest_at.min(raw_delete_at).min(maint_at).min(rum_at)
    } else {
        u64::MAX
    };
    refresh_at.min(due_at).max(record.retry_after)
}

/// Whether the flush record wants a run.
pub fn flush_wanted(record: &Sched, now: u64) -> bool {
    now >= record.wake_at
}

/// When [`flush_wanted`] next becomes `true` for `record`.
pub fn flush_wake_at(record: &Sched) -> u64 {
    record.wake_at
}

/// How many seconds the shield may cache a probe response before Compute next wants a wake.
/// Returns `1` off the shield, when no record is cached, while a lease is held, or when work is
/// already due; otherwise sleeps until `wake_at(record)`, capped at [`REFRESH_S`].
pub fn next_ttl_s(
    runtime: &impl Runtime,
    key: &str,
    wanted: fn(&Sched, u64) -> bool,
    wake_at: fn(&Sched) -> u64,
) -> u64 {
    if !runtime.on_log_shield() {
        return 1;
    }
    let now = runtime.now();
    let Some(record) = peek(runtime, key) else {
        return 1;
    };
    if record.lease_until > now || wanted(&record, now) {
        return 1;
    }
    wake_at(&record).saturating_sub(now).clamp(1, REFRESH_S)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Due {
    /// The period index a successful ingest reaches.
    pub ingest: Option<u64>,
    pub raw_delete: bool,
    pub maint: bool,
    /// The RUM period index a successful RUM ingest reaches.
    pub rum: Option<u64>,
}

impl Due {
    pub fn any(&self) -> bool {
        self.ingest.is_some() || self.raw_delete || self.maint || self.rum.is_some()
    }
}

fn mirror_rum_off(mirror: &Mirror) -> bool {
    mirror.rum_log_period_s == 0
}

pub fn period_s(log_period_s: u64) -> u64 {
    log_period_s.max(MIN_PERIOD_S)
}

/// Polling period reached by `now`, not a claim that upstream delivery is complete.
pub fn ready_period(now: u64, log_period_s: u64) -> u64 {
    now.saturating_sub(ingest_grace_s(log_period_s)) / period_s(log_period_s)
}

/// When ingest is next due with no backlog: the end of the next polling period.
pub fn next_ingest_at(mirror: &Mirror) -> u64 {
    let period = period_s(mirror.log_period_s);
    mirror
        .jobs
        .ingest
        .success_period(period)
        .map_or(0, |p| {
            p.saturating_add(1)
                .saturating_mul(period)
                .saturating_add(ingest_grace_s(mirror.log_period_s))
        })
        .max(mirror.jobs.ingest.retry_after)
}

/// When RUM ingest is next due with no backlog, as [`next_ingest_at`] on the RUM period.
pub fn next_rum_at(mirror: &Mirror) -> u64 {
    let period = period_s(mirror.rum_log_period_s);
    mirror
        .jobs
        .rum
        .success_period(period)
        .map_or(0, |p| {
            p.saturating_add(1)
                .saturating_mul(period)
                .saturating_add(rum_grace_s(mirror.rum_log_period_s))
        })
        .max(mirror.jobs.rum.retry_after)
}

fn rum_grace_s(log_period_s: u64) -> u64 {
    crate::ingest::settle_s(u32::try_from(log_period_s).unwrap_or(u32::MAX))
}

pub fn ready_rum_period(now: u64, log_period_s: u64) -> u64 {
    now.saturating_sub(rum_grace_s(log_period_s)) / period_s(log_period_s)
}

pub fn due(mirror: &Mirror, now: u64) -> Due {
    if !mirror.configured {
        return Due::default();
    }
    let jobs = &mirror.jobs;
    let ready = ready_period(now, mirror.log_period_s);
    let ingest_at = if jobs.ingest.more_pending {
        jobs.ingest.retry_after
    } else {
        next_ingest_at(mirror)
    };
    let ingest = (now >= ingest_at
        && (jobs.ingest.more_pending
            || jobs.ingest.success_period(mirror.log_period_s) < Some(ready)))
    .then_some(ready);
    Due {
        ingest,
        raw_delete: jobs.raw_delete.more_pending && now >= jobs.raw_delete.retry_after,
        maint: now >= jobs.maint.retry_after
            && (jobs.maint.more_pending
                || jobs
                    .maint
                    .last_run
                    .is_none_or(|last| now >= last + MAINT_INTERVAL_S)),
        rum: (!mirror_rum_off(mirror) && now >= jobs.rum.retry_after)
            .then(|| ready_rum_period(now, mirror.rum_log_period_s))
            .filter(|ready| {
                jobs.rum.more_pending
                    || jobs.rum.success_period(mirror.rum_log_period_s) < Some(*ready)
            }),
    }
}

/// Read-only projection of the dispatcher's jobs. Rotating stages have no independent timer.
pub fn task_schedules(
    runtime: &impl Runtime,
    state: &CronState,
    service: Option<&super::model::Service>,
) -> Vec<serde_json::Value> {
    use serde_json::json;
    let Some(service) = service else {
        return Vec::new();
    };
    let now = runtime.now();
    let mirror = Mirror::from_state(state, Some(service.log_period))
        .with_rum(service.rum.enabled.then_some(service.rum.log_period));
    let jobs = &mirror.jobs;
    let raw_delete_execution =
        (jobs.raw_delete.service_id.as_deref() == Some(service.service_id.as_str())).then(|| {
            let status = match jobs.raw_delete.last_status.as_str() {
                status @ ("idle" | "done" | "partial" | "busy" | "error" | "missed"
                | "unprovisioned") => status,
                _ => "unknown",
            };
            json!({
                "service_id": service.service_id,
                "last_run": jobs.raw_delete.last_run,
                "status": status,
                "more_pending": jobs.raw_delete.more_pending,
                "retry_after": jobs.raw_delete.retry_after,
            })
        });
    let pending_at = |job: &JobState| job.more_pending.then_some(job.retry_after.max(now));
    let ingest_at = |job: &JobState, next: u64| {
        if job.more_pending {
            job.retry_after.max(now)
        } else {
            next.max(now)
        }
    };
    let maint_at = if jobs.maint.more_pending {
        jobs.maint.retry_after
    } else {
        jobs.maint
            .last_run
            .map_or(0, |last| last.saturating_add(MAINT_INTERVAL_S))
            .max(jobs.maint.retry_after)
    }
    .max(now);
    let entry =
        |task: &str, last: Option<u64>, next: Option<u64>, interval: Option<u64>, basis: &str| {
            json!({
                "task": task,
                "last_run_time": last.map(super::rfc3339_from_secs),
                "next_run_time": next.map(super::rfc3339_from_secs),
                "interval_s": interval,
                "schedule_basis": basis,
            })
        };
    let mut schedules = vec![
        entry(
            super::TICK_STAGES[0],
            jobs.ingest.last_run,
            Some(ingest_at(&jobs.ingest, next_ingest_at(&mirror))),
            Some(period_s(mirror.log_period_s)),
            "log_period",
        ),
        entry(
            super::TICK_STAGES[1],
            jobs.rum.last_run,
            service
                .rum
                .enabled
                .then(|| ingest_at(&jobs.rum, next_rum_at(&mirror))),
            service
                .rum
                .enabled
                .then(|| period_s(mirror.rum_log_period_s)),
            "log_period",
        ),
        entry(
            super::TICK_STAGES[2],
            jobs.raw_delete.last_run,
            pending_at(&jobs.raw_delete),
            None,
            "pending_work",
        ),
        entry(
            "maintenance",
            jobs.maint.last_run,
            Some(maint_at),
            Some(MAINT_INTERVAL_S),
            "dispatcher",
        ),
        entry(
            "log_flush",
            None,
            peek(runtime, FLUSH_SCHED_KEY).map(|record| record.wake_at.max(now)),
            Some(FLUSH_WAKE_S),
            "flush_wake",
        ),
        entry(
            "cloud_ingest_clock",
            state.last_tick,
            next_tick_at(runtime, state.last_tick),
            Some(super::CLOCK_INTERVAL_S),
            "coordinator",
        ),
    ];
    schedules[2]["execution"] = raw_delete_execution.into();
    if !service.rum.enabled {
        schedules[1]["disabled_reason"] = json!("RUM is disabled");
    }
    for task in super::logs::MAINT_STAGES {
        let mut row = entry(task, None, None, None, "maintenance_turn");
        if task == "delete_data" && service.data_reset.is_none() {
            row["disabled_reason"] = json!("No manual data reset is queued");
        }
        if task == "request_compact" && service.compaction.target_bytes().is_none() {
            row["disabled_reason"] = json!("Request compaction is disabled");
        }
        schedules.push(row);
    }
    schedules
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claim {
    /// Nothing due; the tick was counted.
    Idle,
    /// Another execution holds the lease; the tick was counted.
    Busy,
    /// This execution holds the lease. The idle counters are handed to it (and zeroed in the
    /// record) to go on its cron_runs rows, or back via [`release`].
    Run { idle_ticks: u64, idle_vcpu_ms: u64 },
}

fn decode(entry: Option<CoreCacheEntry>) -> Option<Sched> {
    entry.and_then(|(body, _)| serde_json::from_slice(&body).ok())
}

fn encode(record: &Sched) -> CoreCacheEntry {
    (serde_json::to_vec(record).unwrap_or_default(), Vec::new())
}

/// The atomic claim: one serialized replace of `key`. `force` (a manual run) takes a free lease
/// even when `wanted` says nothing is due. A missing record takes the lease to rebuild it.
pub fn claim(
    runtime: &impl Runtime,
    key: &str,
    token: &str,
    force: bool,
    wanted: fn(&Sched, u64) -> bool,
) -> Result<Claim, String> {
    claim_with_record(runtime, key, token, force, wanted).map(|(claim, _)| claim)
}

/// Like [`claim`], also returning the snapshot of the [`Sched`] record written on claim.
pub fn claim_with_record(
    runtime: &impl Runtime,
    key: &str,
    token: &str,
    force: bool,
    wanted: fn(&Sched, u64) -> bool,
) -> Result<(Claim, Sched), String> {
    let now = runtime.now();
    let vcpu = runtime.vcpu_ms();
    let outcome = RefCell::new(Claim::Idle);
    let snapshot = RefCell::new(Sched::default());
    runtime
        .core_cache_replace(key, &[], RECORD_TTL, &|existing| {
            let found = decode(existing);
            let rebuild = found.is_none();
            let mut record = found.unwrap_or_default();
            record.last_seen = now;
            let held = record.lease_until > now && record.owner.as_deref() != Some(token);
            let wanted = force || rebuild || wanted(&record, now);
            *outcome.borrow_mut() = if held {
                Claim::Busy
            } else if wanted {
                record.owner = Some(token.to_owned());
                record.lease_until = now + LEASE_S;
                Claim::Run {
                    idle_ticks: std::mem::take(&mut record.idle_ticks),
                    idle_vcpu_ms: std::mem::take(&mut record.idle_vcpu_ms),
                }
            } else {
                Claim::Idle
            };
            if !matches!(*outcome.borrow(), Claim::Run { .. }) {
                record.idle_ticks += 1;
                record.idle_vcpu_ms += vcpu;
            }
            *snapshot.borrow_mut() = record.clone();
            encode(&record)
        })
        .map_err(|error| format!("schedule_claim_failed:{error:?}"))?;
    Ok((outcome.into_inner(), snapshot.into_inner()))
}

/// Ends a run after its durable write: clears the lease if this execution still holds it and
/// applies `update` (the new mirror or wake time), and hands back idle counters no cron_runs row
/// carried. A lease that expired and was taken over is left to its new holder, whose own
/// release updates the record; so is a record with no owner, which a newer run already released
/// (or an eviction emptied, and the next claim rebuilds).
pub fn release(
    runtime: &impl Runtime,
    key: &str,
    token: &str,
    update: &dyn Fn(&mut Sched),
    idle: (u64, u64),
) -> Result<(), String> {
    let now = runtime.now();
    runtime
        .core_cache_replace(key, &[], RECORD_TTL, &|existing| {
            let mut record = decode(existing).unwrap_or_default();
            if record.owner.as_deref() == Some(token) {
                record.owner = None;
                record.lease_until = 0;
                record.refreshed_at = now;
                update(&mut record);
            }
            record.idle_ticks += idle.0;
            record.idle_vcpu_ms += idle.1;
            encode(&record)
        })
        .map_err(|error| format!("schedule_release_failed:{error:?}"))
}

/// The cached record, read without claiming (status surfaces and the health check).
pub fn peek(runtime: &impl Runtime, key: &str) -> Option<Sched> {
    decode(runtime.core_cache_lookup(key).ok().flatten())
}

/// When the clock was last seen: the IAD record's `last_seen` on the shield, else `None` with
/// the caller falling back to the durable last run.
pub fn last_seen(runtime: &impl Runtime) -> Option<u64> {
    runtime
        .on_log_shield()
        .then(|| peek(runtime, SCHED_KEY))
        .flatten()
        .map(|record| record.last_seen)
}

/// Effective clock freshness for system checks on the shield: while the shield is intentionally
/// caching `/_tick` until its next scheduled wake, the clock is healthy through that wake time;
/// once the wake time passes without a probe, freshness stays pinned to the missed wake so the
/// 10 s staleness check fires on schedule.
pub fn check_seen(runtime: &impl Runtime) -> Option<u64> {
    if !runtime.on_log_shield() {
        return None;
    }
    let now = runtime.now();
    let record = peek(runtime, SCHED_KEY)?;
    if record.lease_until <= record.last_seen {
        let wake = tick_wake_at(&record).min(record.last_seen.saturating_add(REFRESH_S));
        Some(record.last_seen.max(wake.min(now)))
    } else {
        Some(record.last_seen)
    }
}

/// When the clock will next wake on the shield, else `last_tick + 1`.
pub fn next_tick_at(runtime: &impl Runtime, last_tick: Option<u64>) -> Option<u64> {
    if runtime.on_log_shield() {
        if let Some(record) = peek(runtime, SCHED_KEY) {
            let now = runtime.now();
            let wake = if record.lease_until > now || tick_wanted(&record, now) {
                now.saturating_add(1)
            } else {
                tick_wake_at(&record).min(record.last_seen.max(now).saturating_add(REFRESH_S))
            };
            return Some(wake);
        }
    }
    last_tick.map(|ts| ts.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_watermarks_wake_after_backoff_without_losing_backlog_state() {
        let now = 1_800_003_600;
        let state: CronState = serde_json::from_value(serde_json::json!({
            "last_tick": now,
            "last_bucket": now / 120,
            "maint_next": 3,
            "rum_first": true,
            "jobs": {
                "ingest": {
                    "last_run": now,
                    "last_success_period": ready_period(now, 10),
                    "retry_after": now + 30,
                    "last_status": "error"
                },
                "rum": {
                    "last_run": now,
                    "last_success_period": ready_period(now, 10),
                    "retry_after": now + 30
                },
                "raw_delete": {"more_pending": true, "retry_after": now + 40},
                "maint": {"last_run": now}
            }
        }))
        .unwrap();
        let mirror = Mirror::from_state(&state, Some(60)).with_rum(Some(300));
        let record = Sched {
            refreshed_at: now,
            mirror: mirror.clone(),
            ..Default::default()
        };
        assert_eq!(next_ingest_at(&mirror), now + 30);
        assert_eq!(next_rum_at(&mirror), now + 30);
        assert_eq!(tick_wake_at(&record), now + 30);
        assert!(!tick_wanted(&record, now + 29));
        let due = due(&mirror, now + 30);
        assert_eq!(due.ingest, Some(ready_period(now + 30, 60)));
        assert_eq!(due.rum, Some((now + 30 - rum_grace_s(300)) / period_s(300)));
        assert_eq!(mirror.jobs, state.jobs);
        assert!(mirror.jobs.raw_delete.more_pending);
        assert_eq!(state.maint_next, 3);
        assert!(state.rum_first);
    }

    #[test]
    fn durable_merge_does_not_compare_watermarks_from_different_cadences() {
        let old: Jobs = serde_json::from_value(serde_json::json!({
            "ingest": {
                "last_run": 1_800_000_000,
                "last_success_period": 180_000_000,
                "last_success_period_s": 10
            }
        }))
        .unwrap();
        let new: Jobs = serde_json::from_value(serde_json::json!({
            "ingest": {
                "last_run": 1_800_000_060,
                "last_success_period": 30_000_001,
                "last_success_period_s": 60,
                "more_pending": true,
                "retry_after": 1_800_000_090
            }
        }))
        .unwrap();
        for (mut stored, other) in [(old.clone(), new.clone()), (new.clone(), old)] {
            stored.merge(&other);
            assert_eq!(stored.ingest, new.ingest);
        }
    }

    #[test]
    fn tick_wake_at_matches_tick_wanted_boundary() {
        let base = 1_000_000 * 60 + INGEST_GRACE_S;
        let mut record = Sched {
            last_seen: base,
            refreshed_at: base,
            mirror: Mirror {
                configured: true,
                log_period_s: 60,
                jobs: Jobs {
                    ingest: JobState {
                        last_run: Some(base),
                        last_success_period: Some(ready_period(base, 60)),
                        last_success_period_s: Some(60),
                        ..Default::default()
                    },
                    maint: JobState {
                        last_run: Some(base),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                rum_log_period_s: 0,
            },
            ..Default::default()
        };
        let wake = tick_wake_at(&record);
        assert_eq!(wake, base + 60);
        assert!(!tick_wanted(&record, wake - 1));
        assert!(tick_wanted(&record, wake));

        // Longer log period caps idle sleep at REFRESH_S (60s) via refreshed_at + REFRESH_S.
        record.mirror.log_period_s = 300;
        record.mirror.jobs.ingest.last_success_period = Some(ready_period(base, 300));
        record.mirror.jobs.ingest.last_success_period_s = Some(300);
        let wake_refresh = tick_wake_at(&record);
        assert_eq!(wake_refresh, base + REFRESH_S);
        assert!(!tick_wanted(&record, wake_refresh - 1));
        assert!(tick_wanted(&record, wake_refresh));

        // Catch-up (more_pending) wakes immediately (retry_after = 0 <= base).
        record.mirror.jobs.ingest.more_pending = true;
        assert!(tick_wake_at(&record) <= base);
        assert!(tick_wanted(&record, base));
    }
}
