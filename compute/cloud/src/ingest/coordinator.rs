//! Ingest coordinator: plans raw objects into shards, fans them out to peer worker executions
//! (`POST /_internal/ingest`, see [`super::worker`]) and commits the plan once every shard has a
//! verified result.
//!
//! The plan (`ingest:plan`, `version: 2`, or 3 with segment shards) freezes its raw keys, shard membership and `plan_id`;
//! output keys carry `<plan_id>s<shard>`, so a retried shard overwrites its own files. Every plan
//! write after creation is fenced: a tick first takes the lease (`attempt + 1`,
//! `lease_until = now + LEASE_S`) by generation CAS, and later writes re-read the plan and only
//! proceed if the attempt is still theirs. A shard that fails `MAX_SHARD_FAILURES` times (transient
//! 5xx, 429, `SlowDown` and transport failures do not count), or a plan built by a release with another conversion schema, is abandoned: a later tick deletes
//! every output named by the plan's intent manifests, then replans the same raw keys under a new
//! `plan_id`.
//!
//! Core Cache: the coordinator makes none itself; its telemetry rows go to the request's log
//! buffer and are flushed with the rest of the tick (a few bucket inserts per log kind).

use super::{
    convert, finish_legacy, held_object, hour_dir, inflight_prefix, legacy_run_id, quarantined,
    raw_key_minute, sample_json, scope, settle_s,
    storage::{Object, Store},
    summary, watermark_value,
    worker::{self, ObjectResult, ShardRequest, ShardResult, SHARD_MAX_OBJECTS},
    Report, DELETEQ_PREFIX, LIST_PAGE, MAX_SWEPT, MAX_WINDOWS_PER_OBJECT, SWEEP_INTERVAL_S,
    SWEEP_LISTS, SWEEP_S, TICK_RESERVE,
};
use crate::{
    admin::{Record, Service},
    shell::Runtime,
    telemetry::{write_log, LogBuffer, WritableLogKind},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

pub const PLAN_VERSION: u64 = 2;
/// A plan with segment shards; releases without segments see it as another release's plan.
pub const SEGMENT_PLAN_VERSION: u64 = 3;
/// Default and largest decompressed bytes per segment (config `ingest_segment_kib`).
pub const DEFAULT_SEGMENT_BYTES: u64 = worker::MAX_SEGMENT_BYTES;
const MIN_SEGMENT_BYTES: u64 = 64 * 1024;
/// Assumed gzip/zstd ratio of NDJSON logs when pre-cutting an object by its compressed size. A
/// higher real ratio leaves the open last segment to measure and re-cut the rest.
const ESTIMATED_RATIO: u64 = 8;
/// Most segments one object is cut into at a time: the largest object that can be measured, in
/// default-size segments. A smaller `ingest_segment_kib` is raised to keep within it.
pub const MAX_OBJECT_SEGMENTS: u64 =
    convert::MAX_OBJECT_DECOMPRESSED_BYTES.div_ceil(DEFAULT_SEGMENT_BYTES);
/// The SDK reads at most this much of a KV value, so a saved plan, with every result its
/// pending shards may still add, stays within it.
pub const PLAN_BYTES: usize = 2_097_152;
/// JSON one window file or summary entry adds beyond its scope and table.
const OUTPUT_JSON_BYTES: usize = 320;
/// JSON one object result adds beyond its key, bad-line sample and outputs.
const OBJECT_JSON_BYTES: usize = 256;
/// JSON one shard result adds beyond its manifest, objects and outputs, with its `failures` entry.
const RESULT_JSON_BYTES: usize = 320;
/// Room for fields a plan gains while it runs (`abandoning`, `sweep_after`, `sweep_next`).
const PLAN_SLACK_BYTES: usize = 4096;
pub const LEASE_S: u64 = 90;
pub const SHARD_MAX_GZ_BYTES: u64 = 8 * 1024 * 1024;
/// No worker is dispatched once the tick has run this long.
pub const DISPATCH_BY: Duration = Duration::from_secs(10);
/// Workers that have not answered this long after tick start are given up on for this tick.
pub const DRAIN_AT: Duration = Duration::from_secs(32);
/// Stop starting new coordinator dispatches once this much vCPU is spent in the execution.
pub const VCPU_STOP_MS: u64 = 800;
/// Workers stop starting GETs/PUTs this long before the coordinator stops waiting.
const WORKER_MARGIN: Duration = Duration::from_secs(2);
pub const MAX_SHARD_FAILURES: u32 = 3;
pub const DEFAULT_FANOUT: usize = 8;
pub const MAX_FANOUT: usize = 8;
const MANIFEST_LIMIT: usize = 64 * 1024;
/// Calls after the dispatch: results save (re-read + CAS) or commit (fence read, delete queue and
/// watermark).
const AFTER_DISPATCH: u32 = 3;
/// `worker_dispatch` reads the `worker_secret` once per execution; Secret Store reads charge the
/// budget.
const DISPATCH_SECRET: u32 = 1;
/// Calls after the last manifest read of an abandon cleanup: two DeleteObjects, the delete-queue
/// record delete and the fenced save (re-read + CAS).
const CLEANUP_TAIL: u32 = 5;
/// Workers stop starting PUTs this long before their plan's lease expires, so an abandon (which
/// waits for the lease) never races a late write.
pub const WORKER_LEASE_MARGIN_S: u64 = 30;
/// A raw object counts as a straggler only if FOS dated it this much after the LIST that passed
/// its key returned, allowing for clock skew between Compute and FOS.
pub const UPLOAD_SKEW_S: u64 = 30;

pub struct Options {
    /// Raw stream under `raw/<stream>/` and Parquet table under `processed/<table>/`.
    pub stream: String,
    pub table: String,
    /// Backend calls this run must leave unused (on top of `TICK_RESERVE`).
    pub reserve: u32,
    /// Most worker executions dispatched for ingest shards this tick (1..=`MAX_FANOUT`).
    pub max_width: usize,
    /// Extra `(path, body)` worker requests sent in the same dispatch batch as the shards (or
    /// alone when no shard is dispatched); responses land in `Report::extra_results`.
    pub extra: Vec<(String, Value)>,
    /// Whether this step may advance an active `ingest:backfill` job when forward listing is idle.
    pub allow_backfill: bool,
    /// Whether an idle step may run the straggler sweep (at most once per `SWEEP_INTERVAL_S`).
    pub sweep: bool,
    /// Defer writing `ingest:deleteq:<plan_id>` to KV on clean commits and return it in
    /// `Report::pending_deleteq` for same-tick inline raw deletion.
    pub defer_deleteq: bool,
    /// Decompressed bytes per segment of a large raw object; 0 disables segmenting.
    pub segment_bytes: u64,
    /// Shards per worker call relayed through the worker service (`worker::RELAY_PATH`); 0 or 1
    /// sends each shard directly.
    pub relay: usize,
}

impl Options {
    pub fn request(runtime: &impl Runtime) -> Self {
        Self {
            stream: "request".into(),
            table: "request".into(),
            reserve: 0,
            max_width: fanout_width(runtime),
            extra: Vec::new(),
            allow_backfill: false,
            sweep: false,
            defer_deleteq: false,
            segment_bytes: segment_bytes(runtime),
            relay: relay_shards(runtime),
        }
    }

    /// The RUM beacon stream (`raw/rum/` → `processed/rum/client_vitals/` and
    /// `processed/rum/client_errors/`), which keeps its own plan and watermark.
    pub fn rum(runtime: &impl Runtime) -> Self {
        Self {
            stream: super::rum::STREAM.into(),
            table: super::rum::STREAM.into(),
            sweep: true,
            segment_bytes: 0,
            relay: 0,
            ..Self::request(runtime)
        }
    }
}

/// Config `ingest_fanout`, clamped to `1..=MAX_FANOUT`.
pub fn fanout_width(runtime: &impl Runtime) -> usize {
    runtime
        .config("ingest_fanout")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_FANOUT)
        .clamp(1, MAX_FANOUT)
}

/// Config `ingest_segment_kib` in bytes, clamped to `MIN_SEGMENT_BYTES..=DEFAULT_SEGMENT_BYTES`.
pub fn segment_bytes(runtime: &impl Runtime) -> u64 {
    runtime
        .config("ingest_segment_kib")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(DEFAULT_SEGMENT_BYTES, |kib| kib.saturating_mul(1024))
        .clamp(MIN_SEGMENT_BYTES, DEFAULT_SEGMENT_BYTES)
}

/// Config `ingest_relay`, clamped to `0..=worker::MAX_RELAY_SHARDS`; off by default.
pub fn relay_shards(runtime: &impl Runtime) -> usize {
    runtime
        .config("ingest_relay")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(worker::MAX_RELAY_SHARDS)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Plan {
    pub version: u64,
    pub plan_id: String,
    pub nonce: String,
    pub stream: String,
    pub table: String,
    pub schema: String,
    /// The custom columns `schema` covers, frozen with the plan and sent to every shard.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom: Vec<convert::CustomColumn>,
    pub window_s: i64,
    pub raw_keys: Vec<String>,
    pub sizes: Vec<u64>,
    /// Leaf shards only: a split shard is replaced by its halves `<id>a` and `<id>b`.
    pub shards: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub results: BTreeMap<String, ShardResult>,
    #[serde(default)]
    pub failures: BTreeMap<String, u32>,
    /// Shards that failed with `worker_budget_stopped` in this plan.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub budget_stopped: BTreeSet<String>,
    pub attempt: u32,
    pub lease_until: u64,
    #[serde(default)]
    pub more_pending: bool,
    /// Raw objects this plan listed but did not take, or further listing pages: still unconverted
    /// after the commit.
    #[serde(default)]
    pub backlog: bool,
    pub created_at: u64,
    #[serde(default)]
    pub abandoning: Option<String>,
    #[serde(default)]
    pub replans: u32,
    /// Abandon cleanup fell back to sweeping `processed/<table>/` for this plan's token (an intent
    /// manifest was unreadable): the last key listed, `Some("")` before the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sweep_after: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub swept: bool,
    /// A straggler-sweep plan: its keys lie below the watermark and are recorded as swept when
    /// it commits, together with `sweep_next`, the sweep cursor to save.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub straggler: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sweep_next: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent_after: Option<String>,
    /// Segment shards: each holds one raw key and converts only this decompressed range.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub segments: BTreeMap<String, convert::Segment>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub segment_bytes: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

pub fn plan_id(
    stream: &str,
    table: &str,
    schema: &str,
    window_s: i64,
    keys: &[String],
    nonce: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(format!("{stream}|{table}|{schema}|{window_s}|{nonce}|"));
    for key in keys {
        digest.update(key.as_bytes());
        digest.update(b"\n");
    }
    hex::encode(&digest.finalize()[..6])
}

/// Distinct grid windows a shard is expected to touch, from its raw keys' minute directories
/// and the log period each object spans (plus one for rows routed into a neighbouring window).
fn estimated_windows(keys: &[&str], raw_prefix: &str, window_s: i64, period_s: i64) -> usize {
    let mut windows = BTreeSet::new();
    let mut unknown = 0;
    for key in keys {
        match raw_key_minute(key, raw_prefix) {
            Some(minute) => {
                let first = minute.div_euclid(window_s);
                let last = (minute + period_s.max(60) - 1).div_euclid(window_s);
                windows.extend(first..=last);
            }
            None => unknown += 1,
        }
    }
    windows.len() + unknown + 1
}

/// Greedy, key-ordered packing of `objects` into at most `width` shards. Each shard holds at
/// most `SHARD_MAX_OBJECTS >> replans` objects and `SHARD_MAX_GZ_BYTES`, and its objects plus
/// expected windows fit one worker budget. Returns the shards; objects past them are left over.
#[allow(clippy::too_many_arguments)]
fn pack(
    objects: &[(String, u64)],
    width: usize,
    replans: u32,
    raw_prefix: &str,
    window_s: i64,
    period_s: i64,
    balance: bool,
) -> Vec<Vec<(String, u64)>> {
    let cap = (SHARD_MAX_OBJECTS >> replans.min(8)).max(1);
    let gz: u64 = objects.iter().map(|o| o.1).sum();
    let wanted = objects
        .len()
        .div_ceil(cap)
        .max(gz.div_ceil(SHARD_MAX_GZ_BYTES) as usize)
        .clamp(1, width.max(1));
    let per = if balance {
        cap.min(objects.len().div_ceil(wanted)).max(1)
    } else {
        cap
    };
    let mut shards: Vec<Vec<(String, u64)>> = Vec::new();
    let mut current: Vec<(String, u64)> = Vec::new();
    for object in objects {
        let fits = current.is_empty() || {
            let gz: u64 = current.iter().map(|o| o.1).sum::<u64>() + object.1;
            let mut keys: Vec<&str> = current.iter().map(|o| o.0.as_str()).collect();
            keys.push(&object.0);
            current.len() < per
                && gz <= SHARD_MAX_GZ_BYTES
                && estimated_windows(&keys, raw_prefix, window_s, period_s)
                    <= worker::window_room(keys.len())
        };
        if !fits {
            shards.push(std::mem::take(&mut current));
            if shards.len() >= width {
                return shards;
            }
        }
        current.push(object.clone());
    }
    if !current.is_empty() {
        shards.push(current);
    }
    shards
}

/// Segments a raw object of `gz` compressed bytes is cut into up front (0: converted whole).
fn segment_count(gz: u64, segment_bytes: u64) -> u64 {
    let estimate = gz.saturating_mul(ESTIMATED_RATIO);
    if segment_bytes == 0 || gz > super::SEGMENT_READ_LIMIT as u64 || estimate <= 2 * segment_bytes
    {
        return 0;
    }
    estimate.div_ceil(segment_bytes)
}

/// `segment_bytes`, raised (to whole KiB) so `decompressed` bytes take at most
/// `MAX_OBJECT_SEGMENTS` segments.
fn capped_segment_bytes(segment_bytes: u64, decompressed: u64) -> u64 {
    segment_bytes.max(decompressed.div_ceil(MAX_OBJECT_SEGMENTS).div_ceil(1024) * 1024)
}

fn json_len(text: &str) -> usize {
    serde_json::to_string(text).map_or(text.len() * 6 + 2, |json| json.len())
}

/// Most JSON the result of pending shard `id` can add to the plan.
fn result_reserve(plan: &Plan, scope_json: usize, id: &str) -> usize {
    let keys = &plan.shards[id];
    let windows = worker::window_room(keys.len()).min(MAX_WINDOWS_PER_OBJECT * keys.len());
    let output = scope_json + plan.table.len() + OUTPUT_JSON_BYTES;
    let per_object = windows.min(MAX_WINDOWS_PER_OBJECT) * output;
    let objects: usize = keys
        .iter()
        .map(|key| json_len(key) + OBJECT_JSON_BYTES + 6 * worker::SAMPLE_BYTES + 2 + per_object)
        .sum();
    RESULT_JSON_BYTES
        + 2 * id.len()
        + scope_json
        + plan.plan_id.len()
        + 2 * windows * output
        + windows * (super::file_metadata::METADATA_BYTES + 32)
        + objects
}

/// JSON pending shard `id` adds to the plan now and at most once it answers.
fn pending_bytes(plan: &Plan, scope_json: usize, id: &str) -> usize {
    let entry: usize = 4
        + json_len(id)
        + plan.shards[id]
            .iter()
            .map(|k| json_len(k) + 1)
            .sum::<usize>();
    let segment = plan.segments.get(id).map_or(0, |_| json_len(id) + 64);
    entry + segment + result_reserve(plan, scope_json, id)
}

/// The plan's JSON once every pending shard has answered, at most.
fn projected_bytes(plan: &Plan, scope_json: usize) -> usize {
    let pending: usize = plan
        .shards
        .keys()
        .filter(|id| !plan.results.contains_key(*id))
        .map(|id| result_reserve(plan, scope_json, id))
        .sum();
    serde_json::to_vec(plan).map_or(usize::MAX, |json| json.len()) + PLAN_SLACK_BYTES + pending
}

/// Drops the trailing objects (all shards of each) of a new plan until its projected JSON fits
/// `PLAN_BYTES`; keeps at least the first object's shards.
fn fit_plan(plan: &mut Plan, scope_json: usize) {
    let mut ids: Vec<String> = plan.shards.keys().cloned().collect();
    ids.sort_by_key(|id| id.parse::<usize>().unwrap_or(usize::MAX));
    let skeleton = Plan {
        shards: BTreeMap::new(),
        segments: BTreeMap::new(),
        raw_keys: Vec::new(),
        sizes: Vec::new(),
        ..plan.clone()
    };
    let mut total =
        serde_json::to_vec(&skeleton).map_or(usize::MAX, |json| json.len()) + PLAN_SLACK_BYTES;
    let mut objects = 0;
    let mut keep = (0, 0);
    for (i, id) in ids.iter().enumerate() {
        total = total.saturating_add(pending_bytes(plan, scope_json, id));
        for key in &plan.shards[id] {
            if plan.raw_keys.get(objects) == Some(key) {
                total += 2 * json_len(key) + 22;
                objects += 1;
            }
        }
        let boundary = ids
            .get(i + 1)
            .is_none_or(|next| plan.shards[next].first() != plan.shards[id].last());
        if boundary && (total <= PLAN_BYTES || keep.0 == 0) {
            keep = (i + 1, objects);
        }
        if total > PLAN_BYTES && keep.0 > 0 {
            break;
        }
    }
    for id in &ids[keep.0..] {
        plan.shards.remove(id);
        plan.segments.remove(id);
    }
    plan.raw_keys.truncate(keep.1);
    plan.sizes.truncate(keep.1);
}

/// `segment_bytes`-sized segments from `start`, the last one open-ended.
fn cut(start: u64, count: u64, segment_bytes: u64) -> impl Iterator<Item = convert::Segment> {
    (0..count).map(move |i| convert::Segment {
        start: start + i * segment_bytes,
        end: (i + 1 < count).then(|| start + (i + 1) * segment_bytes),
    })
}

struct Layout {
    shards: BTreeMap<String, Vec<String>>,
    segments: BTreeMap<String, convert::Segment>,
    /// Leading objects laid out; the rest are left over.
    taken: usize,
}

/// [`pack`] for objects converted whole; an object large enough to segment becomes one shard per
/// segment. Stops once `width` shards are laid out (a segmented object may pass it).
#[allow(clippy::too_many_arguments)]
fn layout(
    objects: &[(String, u64)],
    width: usize,
    replans: u32,
    raw_prefix: &str,
    window_s: i64,
    period_s: i64,
    balance: bool,
    segment_bytes: u64,
) -> Layout {
    let mut shards: Vec<(Vec<String>, Option<convert::Segment>)> = Vec::new();
    let mut taken = 0;
    while taken < objects.len() && shards.len() < width {
        let (key, gz) = &objects[taken];
        let count = segment_count(*gz, segment_bytes);
        if count > 0 {
            shards.extend(cut(0, count, segment_bytes).map(|s| (vec![key.clone()], Some(s))));
            taken += 1;
            continue;
        }
        let run = objects[taken..]
            .iter()
            .position(|o| segment_count(o.1, segment_bytes) > 0)
            .map_or(objects.len(), |p| taken + p);
        let packed = pack(
            &objects[taken..run],
            width - shards.len(),
            replans,
            raw_prefix,
            window_s,
            period_s,
            balance,
        );
        let packed_objects: usize = packed.iter().map(Vec::len).sum();
        shards.extend(
            packed
                .into_iter()
                .map(|shard| (shard.into_iter().map(|o| o.0).collect(), None)),
        );
        taken += packed_objects;
        if taken < run {
            break;
        }
    }
    let mut out = Layout {
        shards: BTreeMap::new(),
        segments: BTreeMap::new(),
        taken,
    };
    for (index, (keys, segment)) in shards.into_iter().enumerate() {
        let id = index.to_string();
        if let Some(segment) = segment {
            out.segments.insert(id.clone(), segment);
        }
        out.shards.insert(id, keys);
    }
    out
}

fn plan_version(segments: &BTreeMap<String, convert::Segment>) -> u64 {
    if segments.is_empty() {
        PLAN_VERSION
    } else {
        SEGMENT_PLAN_VERSION
    }
}

struct Ctx<'a, 'b, R: Runtime> {
    store: &'a Store<'b, R>,
    buffer: &'a LogBuffer,
    options: &'a Options,
    report: &'a mut Report,
    extras_sent: bool,
    /// Conversion identity: the request catalog plus the service's committed and enabled custom
    /// fields, or the fixed RUM schemas (no custom columns).
    schema_hash: String,
    custom: Vec<convert::CustomColumn>,
    plan_key: String,
    watermark_key: String,
    vcpu_start: u64,
}

impl<R: Runtime> Ctx<'_, '_, R> {
    fn runtime(&self) -> &R {
        self.store.runtime
    }
    fn within_vcpu(&self) -> bool {
        self.runtime().vcpu_ms().saturating_sub(self.vcpu_start) < VCPU_STOP_MS
    }
    fn reserve(&self) -> u32 {
        let extra = self.options.extra.len() as u32;
        self.options.reserve + TICK_RESERVE + extra + if extra > 0 { DISPATCH_SECRET } else { 0 }
    }
    fn elapsed(&self) -> Duration {
        self.runtime().admin_elapsed(self.store.started)
    }
    fn raw_prefix(&self) -> String {
        format!("{}raw/{}/", scope(self.store.service), self.options.stream)
    }
    fn rum(&self) -> bool {
        self.options.stream == super::rum::STREAM
    }
    /// The logging period of this step's stream's endpoint.
    fn log_period(&self) -> u32 {
        if self.rum() {
            self.store.service.rum.log_period
        } else {
            self.store.service.log_period
        }
    }
    fn period_s(&self) -> i64 {
        i64::from(self.log_period())
    }
    fn window_s(&self) -> i64 {
        super::window_seconds_for_period(self.runtime(), self.log_period())
    }
    fn settled(&self, key: &str, raw_prefix: &str, now: u64) -> bool {
        let settle = settle_s(self.log_period());
        raw_key_minute(key, raw_prefix).is_none_or(|minute| minute as u64 + settle <= now)
    }
    fn processed_prefix(&self) -> String {
        format!(
            "{}processed/{}/",
            scope(self.store.service),
            self.options.table
        )
    }
    /// Shard dispatches that fit after `before` more calls, the worker-secret read and the
    /// post-dispatch writes.
    fn width_room(&self, before: u32) -> usize {
        let reserve =
            self.options.reserve + TICK_RESERVE + self.options.extra.len() as u32 + DISPATCH_SECRET;
        self.runtime()
            .admin_remaining_calls()
            .saturating_sub(before + AFTER_DISPATCH + reserve) as usize
    }
}

/// Runs one ingest step for `options.stream` → `options.table`; see the module docs.
pub fn coordinate<R: Runtime>(
    runtime: &R,
    service: &Service,
    access_key: &str,
    secret_key: &str,
    started: Instant,
    buffer: &LogBuffer,
    options: &Options,
) -> Result<Report, String> {
    let (report, result) = coordinate_reporting(
        runtime, service, access_key, secret_key, started, buffer, options,
    );
    result.map(|()| report)
}

/// [`coordinate`], but the report (with `extra_results`) comes back even when the step fails,
/// because the extra requests may have run regardless.
pub fn coordinate_reporting<R: Runtime>(
    runtime: &R,
    service: &Service,
    access_key: &str,
    secret_key: &str,
    started: Instant,
    buffer: &LogBuffer,
    options: &Options,
) -> (Report, Result<(), String>) {
    let before = runtime.admin_remaining_calls();
    let store = Store {
        runtime,
        service,
        access_key: access_key.into(),
        secret_key: secret_key.into(),
        started,
        vcpu_start: runtime.vcpu_ms(),
    };
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("cron:ingest");
    }
    let mut report = Report::default();
    let (schema_hash, custom) = if options.stream == super::rum::STREAM {
        (super::rum::schema_hash().to_owned(), Vec::new())
    } else {
        let schema = convert::RequestSchema::for_fields(&service.log_fields);
        (schema.hash().to_owned(), schema.custom().to_vec())
    };
    let mut ctx = Ctx {
        store: &store,
        buffer,
        options,
        report: &mut report,
        extras_sent: false,
        schema_hash,
        custom,
        plan_key: super::plan_key(&options.stream),
        watermark_key: super::watermark_key(&options.stream),
        vcpu_start: runtime.vcpu_ms(),
    };
    let result = step(&mut ctx);
    if !ctx.extras_sent && !options.extra.is_empty() {
        ctx.report.extra_results = if ctx.elapsed() < DISPATCH_BY {
            runtime.worker_dispatch(&options.extra, DRAIN_AT.saturating_sub(ctx.elapsed()))
        } else {
            options
                .extra
                .iter()
                .map(|_| Err("dispatch_window_passed".into()))
                .collect()
        };
    }
    report.backend_calls = before.saturating_sub(runtime.admin_remaining_calls());
    (report, result)
}

fn step<R: Runtime>(ctx: &mut Ctx<'_, '_, R>) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let record = runtime.admin_get(&ctx.plan_key)?;
    let version = record.as_ref().map(|r| r.value["version"].clone());
    if version.as_ref().is_some_and(|v| {
        !v.is_null() && v.as_u64() != Some(PLAN_VERSION) && v.as_u64() != Some(SEGMENT_PLAN_VERSION)
    }) {
        // Another release owns this plan; replacing it here would duplicate its rows.
        ctx.report.busy = true;
        return Ok(());
    }
    let watermark = runtime.admin_get(&ctx.watermark_key)?;
    let committed = watermark
        .as_ref()
        .and_then(|r| r.value["run_id"].as_str().map(str::to_owned));
    if let Some(record) = &record {
        if let Some(run_id) = legacy_run_id(&record.value) {
            if committed.as_deref() != Some(run_id) {
                return finish_legacy(
                    ctx.store,
                    record.value.clone(),
                    watermark.as_ref(),
                    ctx.buffer,
                    ctx.report,
                );
            }
        } else if !record.value["version"].is_null() {
            let plan: Plan =
                serde_json::from_value(record.value.clone()).map_err(|_| "ingest_plan_invalid")?;
            if committed.as_deref() != Some(plan.plan_id.as_str()) {
                return resume(ctx, plan, &record.generation, watermark.as_ref());
            }
        }
    }
    if !ctx.rum()
        && ctx.options.sweep
        && watermark.as_ref().is_some_and(|record| {
            !record.value["raw_sources"].is_null()
                && record.value["raw_scan_due"] != true
                && runtime.now()
                    >= record.value["sweep"]["at"]
                        .as_u64()
                        .unwrap_or(0)
                        .saturating_add(SWEEP_INTERVAL_S)
        })
    {
        sweep(ctx, record.as_ref(), watermark.as_ref())?;
        ctx.report.more_pending = true;
        return Ok(());
    }
    plan_new(ctx, record.as_ref(), watermark.as_ref())?;
    let report = &ctx.report;
    let idle =
        report.listed == 0 && report.shards_dispatched == 0 && !report.busy && !report.more_pending;
    if ctx.options.sweep && idle {
        if watermark
            .as_ref()
            .is_some_and(|r| r.value["raw_scan_due"] == true)
        {
            let latest = runtime.admin_get(&ctx.watermark_key)?;
            sweep(ctx, record.as_ref(), latest.as_ref())?;
        } else {
            sweep(ctx, record.as_ref(), watermark.as_ref())?;
        }
    }
    Ok(())
}

/// Locks the custom fields a new plan is about to write: each one the schema includes is marked
/// `committed` in `svc:<id>` before any plan names it, so its name and type can no longer change
/// (master plan 8B-R). Returns false, having planned nothing, when the config changed since the
/// tick loaded it, the write lost a race or fewer than `after` calls would remain; a later tick
/// retries.
fn commit_fields<R: Runtime>(ctx: &mut Ctx<'_, '_, R>, after: u32) -> Result<bool, String> {
    let names: BTreeSet<String> = ctx.custom.iter().map(|c| c.name.clone()).collect();
    let pending = |fields: &Value| -> bool {
        fields["custom_fields"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|f| {
                f["committed"] != true && f["name"].as_str().is_some_and(|n| names.contains(n))
            })
    };
    if !pending(&ctx.store.service.log_fields) {
        return Ok(true);
    }
    let runtime = ctx.store.runtime;
    if runtime.admin_remaining_calls() < ctx.reserve() + 2 + after {
        ctx.report.more_pending = true;
        return Ok(false);
    }
    let key = format!("svc:{}", ctx.store.service.service_id);
    let Some(record) = runtime.admin_get(&key)? else {
        ctx.report.busy = true;
        return Ok(false);
    };
    let mut value = record.value;
    if convert::RequestSchema::for_fields(&value["log_fields"]).hash() != ctx.schema_hash {
        ctx.report.busy = true;
        return Ok(false);
    }
    if !pending(&value["log_fields"]) {
        return Ok(true);
    }
    let mut committed = Vec::new();
    if let Some(fields) = value["log_fields"]["custom_fields"].as_array_mut() {
        for field in fields {
            let name = field["name"].as_str().unwrap_or_default().to_owned();
            if field["committed"] != true && names.contains(&name) {
                field["committed"] = true.into();
                committed.push(name);
            }
        }
    }
    let audit = json!({"actor": "system:ingest", "at": runtime.now(),
        "event": "custom_fields.committed", "fields": committed,
        "config_version": value["config_version"]});
    value["_audit"] = audit.clone();
    // Capped like an admin save's outbox; a full one loses only this entry, never the lock.
    match value["_audit_outbox"].as_array_mut() {
        Some(outbox) if outbox.len() < 4096 => outbox.push(audit),
        Some(_) => {}
        None => value["_audit_outbox"] = json!([audit]),
    }
    if !runtime.admin_cas(&key, Some(&record.generation), &value)? {
        ctx.report.busy = true;
        return Ok(false);
    }
    Ok(true)
}

fn new_nonce(runtime: &impl Runtime) -> Result<String, String> {
    Ok(hex::encode(&runtime.random()?[..8]))
}

fn shard_map(shards: Vec<Vec<(String, u64)>>) -> BTreeMap<String, Vec<String>> {
    shards
        .into_iter()
        .enumerate()
        .map(|(index, shard)| (index.to_string(), shard.into_iter().map(|o| o.0).collect()))
        .collect()
}

fn plan_new<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    record: Option<&Record>,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let raw_prefix = ctx.raw_prefix();
    let window_s = ctx.window_s();
    let start_after = watermark.and_then(|r| r.value["key"].as_str().map(str::to_owned));
    let now = runtime.now();
    let mut recent_after = None;
    let mut candidates = Vec::new();
    let recent_due = !ctx.rum()
        && ctx.options.sweep
        && watermark.is_some_and(|r| r.value["raw_scan_due"] == true);
    let page = if recent_due {
        ctx.report.more_pending = true;
        let Some((recent, after)) = recent_candidates(ctx, watermark)? else {
            return Ok(());
        };
        candidates = recent;
        recent_after = Some(after);
        super::storage::Page {
            objects: Vec::new(),
            next_token: None,
            prefixes: Vec::new(),
        }
    } else {
        ctx.store
            .list(&raw_prefix, start_after.as_deref(), None, LIST_PAGE, None)?
    };
    let mut unsettled = false;
    let mut passed: Option<String> = None;
    for object in page.objects {
        if start_after.as_deref() >= Some(object.key.as_str()) {
            continue;
        }
        if !super::is_raw_key(&object.key)
            || super::below_floor(&object.key, watermark.map(|r| &r.value))
        {
            passed = Some(object.key);
            continue;
        }
        if ctx.rum() && !ctx.settled(&object.key, &raw_prefix, now) {
            unsettled = true;
            break;
        }
        candidates.push(object);
    }
    if let (true, Some(last)) = (candidates.is_empty(), passed) {
        // Nothing to plan, but the keys passed would otherwise be listed again on every tick.
        let prev = watermark.map(|r| &r.value);
        let run_id = prev.and_then(|v| v["run_id"].as_str()).unwrap_or_default();
        let value = watermark_value(prev, &last, run_id, now, &[], now);
        if !runtime.admin_cas(
            &ctx.watermark_key,
            watermark.map(|r| r.generation.as_str()),
            &value,
        )? {
            ctx.report.busy = true;
        }
        ctx.report.more_pending = true;
        return Ok(());
    }
    ctx.report.more_pending = (page.next_token.is_some() && !unsettled)
        || recent_after.as_ref().is_some_and(|after| !after.is_empty());
    ctx.report.backlog_listing = page.next_token.is_some() && !unsettled;
    ctx.report.backlog |= ctx.report.backlog_listing;
    if candidates.is_empty() && !ctx.rum() && !ctx.report.more_pending {
        if let Some((recent, after)) = recent_candidates(ctx, watermark)? {
            candidates = recent;
            recent_after = Some(after);
        }
    }
    ctx.report.listed = candidates.len() as u64;
    if candidates.is_empty() {
        if !ctx.report.more_pending && ctx.options.allow_backfill {
            return plan_backfill(ctx, record, watermark);
        }
        return Ok(());
    }
    ctx.report.backlog = true;
    if !commit_fields(ctx, 2 + AFTER_DISPATCH)? {
        return Ok(());
    }
    // The plan CAS comes before the dispatch.
    let width = ctx.width_room(1).min(ctx.options.max_width) * ctx.options.relay.max(1);
    if width == 0 || ctx.elapsed() >= DISPATCH_BY || !ctx.within_vcpu() {
        ctx.report.more_pending = true;
        return Ok(());
    }
    let objects: Vec<(String, u64)> = candidates.into_iter().map(|o| (o.key, o.size)).collect();
    let largest = objects
        .iter()
        .filter(|o| o.1 <= super::SEGMENT_READ_LIMIT as u64)
        .map(|o| o.1.saturating_mul(ESTIMATED_RATIO))
        .max()
        .unwrap_or(0);
    let segment_bytes = match ctx.options.segment_bytes {
        0 => 0,
        bytes => capped_segment_bytes(bytes, largest),
    };
    let laid = layout(
        &objects,
        width,
        0,
        &raw_prefix,
        window_s,
        ctx.period_s(),
        true,
        segment_bytes,
    );
    let chosen = &objects[..laid.taken];
    let nonce = new_nonce(runtime)?;
    let schema = ctx.schema_hash.clone();
    let mut plan = Plan {
        nonce,
        stream: ctx.options.stream.clone(),
        table: ctx.options.table.clone(),
        schema,
        custom: ctx.custom.clone(),
        window_s,
        sizes: chosen.iter().map(|o| o.1).collect(),
        raw_keys: chosen.iter().map(|o| o.0.clone()).collect(),
        shards: laid.shards,
        segments: laid.segments,
        segment_bytes,
        attempt: 1,
        lease_until: now + LEASE_S,
        created_at: now,
        recent_after,
        ..Default::default()
    };
    fit_plan(&mut plan, json_len(&scope(ctx.store.service)));
    if plan.recent_after.is_some() && plan.raw_keys.len() < objects.len() {
        plan.recent_after = plan.raw_keys.last().cloned();
    }
    plan.version = plan_version(&plan.segments);
    plan.plan_id = plan_id(
        &plan.stream,
        &plan.table,
        &plan.schema,
        window_s,
        &plan.raw_keys,
        &plan.nonce,
    );
    ctx.report.more_pending = ctx.report.more_pending || plan.raw_keys.len() < objects.len();
    plan.more_pending = ctx.report.more_pending;
    plan.backlog = ctx.report.backlog_listing || plan.raw_keys.len() < objects.len();
    let saved = runtime.admin_cas(
        &ctx.plan_key,
        record.map(|r| r.generation.as_str()),
        &serde_json::to_value(&plan).map_err(|_| "ingest_encode")?,
    )?;
    if !saved {
        ctx.report.busy = true;
        return Ok(());
    }
    dispatch(ctx, plan, watermark)
}

fn recent_candidates<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    watermark: Option<&Record>,
) -> Result<Option<(Vec<Object>, String)>, String> {
    let Some(watermark) = watermark else {
        return Ok(None);
    };
    if ctx.elapsed() >= DISPATCH_BY || !ctx.within_vcpu() || ctx.width_room(5) == 0 {
        return Ok(None);
    }
    let root = &watermark.value;
    let prefix = ctx.raw_prefix();
    let first_minute = ctx
        .runtime()
        .now()
        .saturating_sub(settle_s(ctx.log_period())) as i64;
    if root["raw_sources"].is_null()
        || root["key"]
            .as_str()
            .and_then(|key| raw_key_minute(key, &prefix))
            .is_none_or(|minute| minute < first_minute - first_minute.rem_euclid(60))
    {
        if root["raw_scan_due"] == true {
            let mut updated = root.clone();
            updated["raw_scan_due"] = false.into();
            if !ctx.runtime().admin_cas(
                &ctx.watermark_key,
                Some(&watermark.generation),
                &updated,
            )? {
                ctx.report.busy = true;
            }
        }
        ctx.report.more_pending = false;
        return Ok(None);
    }
    let floor = format!("{prefix}{}", super::minute_dir(first_minute));
    let after = root["raw_scan"]
        .as_str()
        .filter(|after| *after >= floor.as_str())
        .unwrap_or(&floor);
    let page = ctx
        .store
        .list(&prefix, Some(after), None, LIST_PAGE, None)?;
    let keys = page
        .objects
        .iter()
        .map(|o| o.key.clone())
        .collect::<Vec<_>>();
    let (known, checked) =
        super::source_index::covered(ctx.store, root, &keys, ctx.reserve() + AFTER_DISPATCH + 5)?;
    let complete = checked == keys.len() && page.next_token.is_none();
    let next = if complete {
        String::new()
    } else {
        keys.get(checked.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| after.to_owned())
    };
    let candidates = page
        .objects
        .into_iter()
        .take(checked)
        .filter(|object| {
            super::is_raw_key(&object.key)
                && !known.contains(&object.key)
                && !super::below_floor(&object.key, Some(root))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        let mut updated = root.clone();
        updated["raw_scan"] = next.into();
        if ctx.options.sweep {
            updated["raw_scan_due"] = (!complete).into();
        }
        if updated == *root {
            return Ok(None);
        }
        if !ctx
            .runtime()
            .admin_cas(&ctx.watermark_key, Some(&watermark.generation), &updated)?
        {
            ctx.report.busy = true;
        }
        ctx.report.more_pending = !complete;
        return Ok(None);
    }
    Ok(Some((candidates, next)))
}

fn known_raw_keys<R: Runtime>(runtime: &R, reserve: u32) -> Result<BTreeSet<String>, String> {
    let mut known = BTreeSet::new();
    for prefix in [DELETEQ_PREFIX, super::QUARANTINE_PREFIX] {
        let mut cursor: Option<String> = None;
        for _ in 0..2 {
            if runtime.admin_remaining_calls() <= reserve + 4 {
                break;
            }
            let page = runtime.kv_list(prefix, cursor.as_deref(), 50)?;
            for key in &page.keys {
                if runtime.admin_remaining_calls() <= reserve + 4 {
                    break;
                }
                let Some(rec) = runtime.admin_get(key)? else {
                    continue;
                };
                if let Some(arr) = rec.value["keys"].as_array() {
                    for k in arr.iter().filter_map(Value::as_str) {
                        known.insert(k.to_owned());
                    }
                }
                for field in ["held", "objects"] {
                    if let Some(arr) = rec.value[field].as_array() {
                        for item in arr {
                            if let Some(k) = item["key"].as_str() {
                                known.insert(k.to_owned());
                            }
                        }
                    }
                }
                if let Some(k) = rec.value["raw_key"].as_str() {
                    known.insert(k.to_owned());
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(known)
}

fn plan_backfill<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    record: Option<&Record>,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    if runtime.admin_remaining_calls() <= ctx.reserve() + 4 {
        return Ok(());
    }
    let Some(bf_rec) = runtime.admin_get(super::BACKFILL_KEY)? else {
        return Ok(());
    };
    let Ok(mut job) = serde_json::from_value::<super::BackfillJob>(bf_rec.value.clone()) else {
        return Ok(());
    };
    if job.status != "running" {
        return Ok(());
    }
    let raw_prefix = ctx.raw_prefix();
    let window_s = ctx.window_s();
    let start_after = if job.after_key.is_empty() {
        None
    } else {
        Some(job.after_key.as_str())
    };
    let page = ctx
        .store
        .list(&raw_prefix, start_after, None, LIST_PAGE, None)?;
    let now = runtime.now();
    let mut candidates: Vec<Object> = Vec::new();
    let mut unsettled = false;
    let mut past_end = false;
    let mut last_scanned: Option<String> = None;
    for object in page.objects {
        if !super::is_raw_key(&object.key) || start_after >= Some(object.key.as_str()) {
            continue;
        }
        if super::below_floor(&object.key, watermark.map(|r| &r.value)) {
            last_scanned = Some(object.key);
            continue;
        }
        if !job.to_key.is_empty() && object.key.as_str() >= job.to_key.as_str() {
            past_end = true;
            break;
        }
        if !ctx.settled(&object.key, &raw_prefix, now) {
            unsettled = true;
            break;
        }
        job.scanned += 1;
        last_scanned = Some(object.key.clone());
        candidates.push(object);
    }
    let mut receipts_complete = true;
    if !ctx.rum()
        && !candidates.is_empty()
        && watermark.is_some_and(|record| !record.value["raw_sources"].is_null())
    {
        let keys = candidates
            .iter()
            .map(|object| object.key.clone())
            .collect::<Vec<_>>();
        let (known, checked) = super::source_index::covered(
            ctx.store,
            &watermark.unwrap().value,
            &keys,
            ctx.reserve() + 6,
        )?;
        if checked < candidates.len() {
            receipts_complete = false;
            job.scanned -= (candidates.len() - checked) as u64;
            candidates.truncate(checked);
            last_scanned = candidates.last().map(|object| object.key.clone());
        }
        candidates.retain(|object| !known.contains(&object.key));
    }
    if !candidates.is_empty() {
        let known = known_raw_keys(runtime, ctx.reserve())?;
        candidates.retain(|object| !known.contains(&object.key));
    }
    ctx.report.listed = candidates.len() as u64;
    let page_exhausted = receipts_complete && (page.next_token.is_none() || unsettled || past_end);
    if candidates.is_empty() {
        if let Some(last) = last_scanned {
            job.after_key = last;
        }
        job.updated_at = now;
        if page_exhausted {
            job.status = "complete".into();
        } else {
            ctx.report.more_pending = true;
        }
        let _ = runtime.admin_cas(
            super::BACKFILL_KEY,
            Some(bf_rec.generation.as_str()),
            &serde_json::to_value(&job).map_err(|_| "ingest_encode")?,
        )?;
        return Ok(());
    }
    if !commit_fields(ctx, 3 + AFTER_DISPATCH)? {
        return Ok(());
    }
    ctx.report.backlog = true;
    let width = ctx.width_room(2).min(ctx.options.max_width);
    if width == 0 || ctx.elapsed() >= DISPATCH_BY || !ctx.within_vcpu() {
        ctx.report.more_pending = true;
        return Ok(());
    }
    let objects: Vec<(String, u64)> = candidates.into_iter().map(|o| (o.key, o.size)).collect();
    let shards = pack(
        &objects,
        width,
        0,
        &raw_prefix,
        window_s,
        ctx.period_s(),
        true,
    );
    let taken: usize = shards.iter().map(Vec::len).sum();
    let chosen = &objects[..taken];
    let raw_keys: Vec<String> = chosen.iter().map(|o| o.0.clone()).collect();
    if let Some(last) = raw_keys.last() {
        job.after_key = last.clone();
    }
    job.planned += taken as u64;
    job.updated_at = now;
    if taken == objects.len() && page_exhausted {
        job.status = "complete".into();
    } else {
        ctx.report.more_pending = true;
    }
    let nonce = new_nonce(runtime)?;
    let schema = ctx.schema_hash.clone();
    let plan = Plan {
        version: PLAN_VERSION,
        plan_id: plan_id(
            &ctx.options.stream,
            &ctx.options.table,
            &schema,
            window_s,
            &raw_keys,
            &nonce,
        ),
        nonce,
        stream: ctx.options.stream.clone(),
        table: ctx.options.table.clone(),
        schema,
        custom: ctx.custom.clone(),
        window_s,
        sizes: chosen.iter().map(|o| o.1).collect(),
        raw_keys,
        shards: shard_map(shards),
        attempt: 1,
        lease_until: now + LEASE_S,
        created_at: now,
        ..Default::default()
    };
    let saved = runtime.admin_cas(
        &ctx.plan_key,
        record.map(|r| r.generation.as_str()),
        &serde_json::to_value(&plan).map_err(|_| "ingest_encode")?,
    )?;
    if !saved {
        ctx.report.busy = true;
        return Ok(());
    }
    let _ = runtime.admin_cas(
        super::BACKFILL_KEY,
        Some(bf_rec.generation.as_str()),
        &serde_json::to_value(&job).map_err(|_| "ingest_encode")?,
    )?;
    dispatch(ctx, plan, watermark)
}

/// Whether FOS dated `object` after the LIST that passed its key returned, judged by the first
/// pass whose last key is at or after it. A key no kept pass covers never counts.
fn straggler(object: &Object, passes: &[(String, u64)]) -> bool {
    let Some(pass) = passes.iter().find(|pass| pass.0 >= object.key) else {
        return false;
    };
    object
        .last_modified
        .as_deref()
        .and_then(|at| {
            time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()
        })
        .is_some_and(|at| at.unix_timestamp() > (pass.1 + UPLOAD_SKEW_S) as i64)
}

/// The straggler sweep: at most `SWEEP_LISTS` LISTs over the last `SWEEP_S` of minute
/// directories below the watermark, resuming from the watermark's `sweep` cursor. Objects FOS
/// dated after the LIST that passed their key are planned on their own (`straggler`) and get an
/// `ingest_files` row with status `straggler`; the plan's commit records them as swept and moves
/// the cursor. With nothing found the cursor moves at once.
fn sweep<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    record: Option<&Record>,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let Some(watermark) = watermark else {
        return Ok(());
    };
    let value = &watermark.value;
    let now = runtime.now();
    let mark = value["key"].as_str().unwrap_or_default();
    let last_sweep = value["sweep"]["at"].as_u64().unwrap_or(0);
    // The LISTs and the plan write must still leave room for one shard.
    if mark.is_empty()
        || now < last_sweep + SWEEP_INTERVAL_S
        || ctx.elapsed() >= DISPATCH_BY
        || ctx.width_room(SWEEP_LISTS as u32 + 1) == 0
    {
        return Ok(());
    }
    let passes: Vec<(String, u64)> =
        serde_json::from_value(value["passes"].clone()).unwrap_or_default();
    let swept: Vec<(String, u64)> =
        serde_json::from_value(value["swept"].clone()).unwrap_or_default();
    let indexed = !ctx.rum() && !value["raw_sources"].is_null();
    let swept: BTreeSet<&str> = swept
        .iter()
        .filter(|_| !indexed)
        .map(|entry| entry.0.as_str())
        .collect();
    let raw_prefix = ctx.raw_prefix();
    let lookback = if ctx.rum() || value["raw_sources"].is_null() {
        SWEEP_S
    } else {
        u64::from(ctx.store.service.log_retention_days).max(1) * 86_400
    };
    let floor = if !ctx.rum()
        && !value["raw_sources"].is_null()
        && ctx.store.service.log_retention_days == 0
    {
        raw_prefix.clone()
    } else {
        format!(
            "{raw_prefix}{}",
            hour_dir(now.saturating_sub(lookback) as i64)
        )
    };
    let after = value["sweep"]["after"]
        .as_str()
        .filter(|after| *after > floor.as_str() && *after < mark)
        .unwrap_or(&floor)
        .to_owned();
    let (mut found, mut next, mut done, mut token) = (Vec::new(), after.clone(), false, None);
    for _ in 0..SWEEP_LISTS {
        let page = ctx
            .store
            .list(&raw_prefix, Some(&after), token.as_deref(), LIST_PAGE, None)?;
        for object in page.objects {
            if object.key.as_str() >= mark {
                done = true;
                break;
            }
            next.clone_from(&object.key);
            if super::is_raw_key(&object.key)
                && !super::below_floor(&object.key, Some(value))
                && !swept.contains(object.key.as_str())
                && raw_key_minute(&object.key, &raw_prefix).is_some()
                && (!ctx.rum() && !value["raw_sources"].is_null() || straggler(&object, &passes))
            {
                found.push((object.key, object.size));
            }
        }
        token = page.next_token;
        if done || token.is_none() {
            done = true;
            break;
        }
    }
    if !ctx.rum() && !found.is_empty() {
        let keys = found.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let (known, checked) = super::source_index::covered(
            ctx.store,
            value,
            &keys,
            ctx.reserve() + AFTER_DISPATCH + 5,
        )?;
        if checked < found.len() {
            done = false;
            next = keys
                .get(checked.saturating_sub(1))
                .cloned()
                .unwrap_or_else(|| after.clone());
            found.truncate(checked);
        }
        found.retain(|(key, _)| !known.contains(key));
    }
    let cursor = if done { String::new() } else { next };
    let room = if indexed {
        found.len()
    } else {
        MAX_SWEPT.saturating_sub(swept.len())
    };
    let mut complete = found.len() <= room;
    found.truncate(room);
    if found.is_empty() {
        let mut updated = value.clone();
        updated["sweep"] = json!({"after": if complete { &cursor } else { &after }, "at": now});
        if !runtime.admin_cas(&ctx.watermark_key, Some(&watermark.generation), &updated)? {
            ctx.report.busy = true;
        }
        return Ok(());
    }
    if !commit_fields(ctx, 2 + AFTER_DISPATCH)? {
        return Ok(());
    }
    let window_s = ctx.window_s();
    let width = ctx.width_room(1).min(ctx.options.max_width);
    let shards = pack(
        &found,
        width,
        0,
        &raw_prefix,
        window_s,
        ctx.period_s(),
        true,
    );
    let taken: usize = shards.iter().map(Vec::len).sum();
    let chosen = &found[..taken];
    let nonce = new_nonce(runtime)?;
    let schema = ctx.schema_hash.clone();
    let mut plan = Plan {
        version: PLAN_VERSION,
        nonce,
        stream: ctx.options.stream.clone(),
        table: ctx.options.table.clone(),
        schema,
        custom: ctx.custom.clone(),
        window_s,
        sizes: chosen.iter().map(|o| o.1).collect(),
        raw_keys: chosen.iter().map(|o| o.0.clone()).collect(),
        shards: shard_map(shards),
        attempt: 1,
        lease_until: now + LEASE_S,
        created_at: now,
        straggler: true,
        ..Default::default()
    };
    fit_plan(&mut plan, json_len(&scope(ctx.store.service)));
    let chosen = &found[..plan.raw_keys.len()];
    complete = complete && chosen.len() == found.len();
    plan.sweep_next = Some(if complete { cursor } else { after });
    plan.plan_id = plan_id(
        &plan.stream,
        &plan.table,
        &plan.schema,
        window_s,
        &plan.raw_keys,
        &plan.nonce,
    );
    let saved = runtime.admin_cas(
        &ctx.plan_key,
        record.map(|r| r.generation.as_str()),
        &serde_json::to_value(&plan).map_err(|_| "ingest_encode")?,
    )?;
    if !saved {
        ctx.report.busy = true;
        return Ok(());
    }
    for (key, size) in chosen {
        write_log(
            runtime,
            ctx.buffer,
            WritableLogKind::IngestFiles,
            json!({
                "run_id": plan.plan_id, "dataset": plan.table, "raw_key": key, "raw_size": size,
                "status": "straggler", "state_version": 0, "severity": "warn",
                "reason": "uploaded_after_watermark", "rows": 0, "lines_read": 0,
                "bad_lines": 0, "output_keys_json": "[]", "raw_deleted": false,
            }),
        );
    }
    ctx.report.listed = taken as u64;
    ctx.report.stragglers = taken as u64;
    dispatch(ctx, plan, Some(watermark))
}

fn resume<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    mut plan: Plan,
    generation: &str,
    watermark: Option<&Record>,
) -> Result<(), String> {
    ctx.report.backlog = true;
    let runtime = ctx.store.runtime;
    ctx.report.resumed = true;
    ctx.report.listed = plan.raw_keys.len() as u64;
    let now = runtime.now();
    if plan.lease_until > now {
        ctx.report.busy = true;
        return Ok(());
    }
    plan.attempt += 1;
    plan.lease_until = now + LEASE_S;
    // Every shard already has a verified result: committing needs no conversion, so another
    // schema is no reason to throw the outputs away.
    let complete = plan.shards.keys().all(|id| plan.results.contains_key(id));
    let deciding = plan.abandoning.is_none() && !complete && plan.schema != ctx.schema_hash;
    if deciding {
        // Workers of an earlier attempt may still be running; clean up on a later tick.
        plan.abandoning = Some("schema_changed".into());
        plan.lease_until = 0;
    }
    let saved = runtime.admin_cas(
        &ctx.plan_key,
        Some(generation),
        &serde_json::to_value(&plan).map_err(|_| "ingest_encode")?,
    )?;
    if !saved {
        ctx.report.busy = true;
        return Ok(());
    }
    ctx.report.more_pending = true;
    if deciding {
        write_log(
            runtime,
            ctx.buffer,
            WritableLogKind::PlatformErrors,
            json!({"error_domain": "ingest_plan", "code": "plan_abandoned", "severity": "warn",
                "caller": "cron:ingest", "message": format!("plan {} abandoned: schema_changed", plan.plan_id)}),
        );
        return Ok(());
    }
    if plan.abandoning.is_some() {
        return cleanup_and_replan(ctx, plan, watermark);
    }
    ctx.report.more_pending = plan.more_pending;
    dispatch(ctx, plan, watermark)
}

/// Re-reads the plan and writes `plan` only if this tick still holds the lease for `plan_id`.
fn save_fenced<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    plan_id: &str,
    attempt: u32,
    plan: &Plan,
) -> Result<bool, String> {
    let runtime = ctx.store.runtime;
    let Some(current) = runtime.admin_get(&ctx.plan_key)? else {
        return Ok(false);
    };
    if current.value["plan_id"].as_str() != Some(plan_id)
        || current.value["attempt"].as_u64() != Some(u64::from(attempt))
    {
        ctx.report.busy = true;
        return Ok(false);
    }
    let saved = runtime.admin_cas(
        &ctx.plan_key,
        Some(&current.generation),
        &serde_json::to_value(plan).map_err(|_| "ingest_encode")?,
    )?;
    ctx.report.busy |= !saved;
    Ok(saved)
}

/// Records a shard failure. Only errors that are not transient (see
/// [`super::storage::transient_error`]) count toward `MAX_SHARD_FAILURES`.
fn failure<R: Runtime>(ctx: &Ctx<'_, '_, R>, plan: &mut Plan, shard: &str, error: &str) {
    if error == convert::ConvertError::Stopped.code() {
        plan.budget_stopped.insert(shard.to_owned());
    }
    let exhausted = !super::storage::transient_error(error) && {
        let count = plan.failures.entry(shard.to_owned()).or_default();
        *count += 1;
        *count >= MAX_SHARD_FAILURES
    };
    if exhausted && plan.abandoning.is_none() {
        plan.abandoning = Some("shard_failures".into());
    }
    write_log(
        ctx.runtime(),
        ctx.buffer,
        WritableLogKind::PlatformErrors,
        json!({"error_domain": "ingest_worker", "code": error, "severity": "warn",
            "caller": "cron:ingest",
            "message": format!("plan {} shard {shard} attempt {} failed{}", plan.plan_id, plan.attempt,
                if exhausted { "; plan abandoned" } else { "" })}),
    );
}

fn hold_shard<R: Runtime>(
    ctx: &Ctx<'_, '_, R>,
    plan: &mut Plan,
    id: &str,
    scope: &str,
    reason: &str,
    message: String,
) {
    let objects = plan.shards[id]
        .iter()
        .map(|key| ObjectResult {
            key: key.clone(),
            gz_size: plan
                .raw_keys
                .iter()
                .position(|k| k == key)
                .and_then(|i| plan.sizes.get(i).copied())
                .unwrap_or(0),
            status: "failed".into(),
            reason: Some(reason.into()),
            ..Default::default()
        })
        .collect();
    plan.failures.remove(id);
    plan.budget_stopped.remove(id);
    plan.results.insert(
        id.to_owned(),
        ShardResult {
            attempt: plan.attempt,
            manifest: worker::manifest_key(scope, &plan.plan_id, id),
            objects,
            segment: plan.segments.get(id).copied(),
            ..Default::default()
        },
    );
    write_log(
        ctx.runtime(),
        ctx.buffer,
        WritableLogKind::PlatformErrors,
        json!({"error_domain": "ingest_worker", "code": reason, "severity": "warn",
            "caller": "cron:ingest", "message": message}),
    );
}

/// Takes `next` (`plan` with shard `id` split or resegmented) if its projected JSON fits
/// `PLAN_BYTES`; otherwise shard `id` is held: its objects are recorded as failed
/// (`plan_too_large`), so the commit moves them to quarantine and keeps their raw objects.
fn fit_or_hold<R: Runtime>(
    ctx: &Ctx<'_, '_, R>,
    plan: &mut Plan,
    mut next: Plan,
    id: &str,
    scope: &str,
) {
    if projected_bytes(&next, json_len(scope)) <= PLAN_BYTES {
        next.budget_stopped.remove(id);
        *plan = next;
        return;
    }
    hold_shard(
        ctx,
        plan,
        id,
        scope,
        "plan_too_large",
        format!(
            "plan {} shard {id}: held, its plan would pass {PLAN_BYTES} bytes",
            plan.plan_id
        ),
    );
}

/// Refines or holds shards whose keys exhausted `MAX_SHARD_FAILURES` with `worker_budget_stopped`
/// in `old`: multi-key shards are split to single-key shards when they fit `PLAN_BYTES`, and keys
/// whose shards were already a single object or segment (and cannot be subdivided further) are
/// held with `worker_budget_stopped` so the rest of the plan can commit.
fn replan_budget_stopped<R: Runtime>(
    ctx: &Ctx<'_, '_, R>,
    old: &Plan,
    replanned: &mut Plan,
    scope: &str,
) {
    let stopped_code = convert::ConvertError::Stopped.code();
    let mut stopped_keys = BTreeSet::new();
    let mut unsplit_keys = BTreeSet::new();
    for (id, keys) in &old.shards {
        let exhausted = old.failures.get(id).copied().unwrap_or(0) >= MAX_SHARD_FAILURES
            && (old.budget_stopped.contains(id) || old.abandoning.as_deref() == Some(stopped_code));
        if !exhausted {
            continue;
        }
        stopped_keys.extend(keys.iter().cloned());
        if keys.len() <= 1 {
            unsplit_keys.extend(keys.iter().cloned());
        }
    }
    if stopped_keys.is_empty() {
        return;
    }
    let scope_json = json_len(scope);
    let ids: Vec<String> = replanned.shards.keys().cloned().collect();
    for id in ids {
        let Some(keys) = replanned.shards.get(&id).cloned() else {
            continue;
        };
        if keys.len() <= 1 || !keys.iter().any(|k| stopped_keys.contains(k)) {
            continue;
        }
        let mut next = replanned.clone();
        next.shards.remove(&id);
        for (i, key) in keys.iter().enumerate() {
            next.shards
                .insert(format!("{id}s{i:02}"), vec![key.clone()]);
        }
        if projected_bytes(&next, scope_json) <= PLAN_BYTES {
            *replanned = next;
        } else {
            unsplit_keys.extend(keys);
        }
    }
    let hold_ids: Vec<String> = replanned
        .shards
        .iter()
        .filter(|(_, keys)| keys.iter().any(|k| unsplit_keys.contains(k)))
        .map(|(id, _)| id.clone())
        .collect();
    for id in hold_ids {
        hold_shard(
            ctx,
            replanned,
            &id,
            scope,
            stopped_code,
            format!(
                "plan {} shard {id}: held after {MAX_SHARD_FAILURES} {stopped_code} failures",
                replanned.plan_id
            ),
        );
    }
}

/// A worker result is accepted only if it answers this attempt, covers exactly the shard's raw
/// keys, every output is a window file of this plan and shard, and its summaries (none from a
/// worker that predates them) are one per output, of this plan and shard.
fn valid_result(
    plan: &Plan,
    shard: &str,
    keys: &[String],
    result: &ShardResult,
    processed: &str,
    scope: &str,
) -> bool {
    let token = format!("-{}-n", worker::run_token(&plan.plan_id, shard));
    let objects: Vec<&str> = result.objects.iter().map(|o| o.key.as_str()).collect();
    let outputs: BTreeSet<&str> = result.outputs.iter().map(|o| o.key.as_str()).collect();
    result.attempt == plan.attempt
        && result.segment == plan.segments.get(shard).copied()
        && objects == keys.iter().map(String::as_str).collect::<Vec<_>>()
        && result.manifest == worker::manifest_key(scope, &plan.plan_id, shard)
        && outputs.len() == result.outputs.len()
        && result.outputs.iter().all(|output| {
            output.key.starts_with(processed)
                && output.key.contains(&token)
                && output.md5.len() == 32
                && super::parse_processed(&output.key).is_some_and(|name| {
                    name.end - name.start == plan.window_s && name.rows == Some(output.rows)
                })
        })
        && result.objects.iter().all(|object| {
            object
                .outputs
                .iter()
                .all(|o| outputs.contains(o.key.as_str()))
        })
        && (result.summaries.is_empty() || result.summaries.len() == result.outputs.len())
        && result.summaries.iter().all(|output| {
            output
                .key
                .starts_with(&summary::summary_prefix(scope, &plan.table))
                && output.key.contains(&token)
                && output.md5.len() == 32
        })
}

fn dispatch<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    mut plan: Plan,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let pending: Vec<String> = plan
        .shards
        .keys()
        .filter(|id| !plan.results.contains_key(*id))
        .cloned()
        .collect();
    if pending.is_empty() {
        return commit(ctx, &plan, watermark);
    }
    let calls = ctx.width_room(0).min(ctx.options.max_width);
    let relay = ctx.options.relay.max(1);
    let elapsed = ctx.elapsed();
    if calls == 0 || elapsed >= DISPATCH_BY || (ctx.report.listed > 0 && !ctx.within_vcpu()) {
        if runtime.admin_remaining_calls() >= ctx.options.reserve + TICK_RESERVE + 2 {
            plan.lease_until = 0;
            save_fenced(ctx, &plan.plan_id, plan.attempt, &plan)?;
        }
        ctx.report.more_pending = true;
        return Ok(());
    }
    let deadline = DRAIN_AT.saturating_sub(elapsed);
    let margin = if relay > 1 {
        WORKER_MARGIN + worker::RELAY_MARGIN
    } else {
        WORKER_MARGIN
    };
    let stop_ms = deadline
        .saturating_sub(margin)
        .min(worker::WORKER_STOP)
        .as_millis() as u64;
    let service = ctx.store.service;
    let scope = scope(service);
    let mut batch: Vec<String> = pending.into_iter().take(calls * relay).collect();
    let mut shard_requests: Vec<(String, Value)> = Vec::new();
    for id in &batch {
        let body = ShardRequest {
            service_id: service.service_id.clone(),
            region: service.fos_region.clone(),
            bucket: service.fos_bucket.clone(),
            prefix: scope.clone(),
            plan_id: plan.plan_id.clone(),
            shard: id.clone(),
            attempt: plan.attempt,
            stream: plan.stream.clone(),
            table: plan.table.clone(),
            schema: plan.schema.clone(),
            custom: plan.custom.clone(),
            window_s: plan.window_s,
            keys: plan.shards[id].clone(),
            stop_ms,
            lease_until: plan.lease_until,
            created_at: plan.created_at,
            segment: plan.segments.get(id).copied(),
            segment_bytes: plan.segment_bytes,
        };
        let path = if body.segment.is_some() {
            worker::RANGE_PATH
        } else {
            worker::PATH
        };
        shard_requests.push((
            path.into(),
            serde_json::to_value(body).map_err(|_| "ingest_encode")?,
        ));
    }
    let groups = relay_groups(shard_requests, calls, relay);
    batch.truncate(groups.iter().map(Vec::len).sum());
    let mut requests: Vec<(String, Value)> = Vec::new();
    for group in &groups {
        if let [single] = group.as_slice() {
            requests.push(single.clone());
        } else {
            let relayed: Vec<Value> = group.iter().map(|(p, b)| json!([p, b])).collect();
            requests.push((
                worker::RELAY_PATH.into(),
                json!({"deadline_ms": deadline.as_millis() as u64, "requests": relayed}),
            ));
        }
    }
    requests.extend(ctx.options.extra.iter().cloned());
    let mut responses = runtime.worker_dispatch(&requests, deadline);
    responses.resize_with(requests.len(), || Err("worker_missing".into()));
    ctx.report.extra_results = responses.split_off(groups.len());
    let mut flat = Vec::with_capacity(batch.len());
    for (group, response) in groups.iter().zip(responses) {
        if group.len() == 1 {
            flat.push(response);
            continue;
        }
        ctx.report.worker_executions_metrics.push(
            crate::telemetry::execution::WorkerExecutionMetrics::from_reply(
                "ingest_relay",
                None,
                response.as_ref().ok(),
            ),
        );
        match response {
            Ok(relayed) => {
                ctx.report.worker_vcpu_ms += relayed["vcpu_ms"].as_u64().unwrap_or(0);
                ctx.report.relayed_worker_requests += relayed["worker_requests"]
                    .as_u64()
                    .map_or(group.len() as u32, |n| n as u32);
                for index in 0..group.len() {
                    let item = &relayed["replies"][index];
                    flat.push(match (item.get("reply"), item["error"].as_str()) {
                        (Some(reply), _) => Ok(reply.clone()),
                        (None, Some(error)) => Err(error.to_owned()),
                        (None, None) => Err("relay_reply_missing".into()),
                    });
                }
            }
            Err(error) => {
                ctx.report.relayed_worker_requests += group.len() as u32;
                flat.extend(group.iter().map(|_| Err(error.clone())));
            }
        }
    }
    let responses = flat;
    ctx.extras_sent = true;
    ctx.report.shards_dispatched = batch.len() as u32;
    let processed = format!("{scope}processed/{}/", plan.table);
    let mut answered = true;
    for (id, response) in batch.iter().zip(responses) {
        ctx.report.worker_executions_metrics.push(
            crate::telemetry::execution::WorkerExecutionMetrics::from_reply(
                if ctx.rum() {
                    "rum_ingest"
                } else {
                    "request_ingest"
                },
                Some(id),
                response.as_ref().ok(),
            ),
        );
        let reply = match response {
            Ok(reply) => reply,
            Err(error) => {
                answered = false;
                failure(ctx, &mut plan, id, &error);
                continue;
            }
        };
        ctx.report.shards_done += 1;
        ctx.report.kv_ops += reply["kv_ops"].as_u64().unwrap_or(0) as u32;
        ctx.report.cache_lookups += reply["cache_lookups"].as_u64().unwrap_or(0) as u32;
        ctx.report.cache_replaces += reply["cache_replaces"].as_u64().unwrap_or(0) as u32;
        ctx.report.purges += reply["purges"].as_u64().unwrap_or(0) as u32;
        ctx.report.worker_vcpu_ms += reply["vcpu_ms"].as_u64().unwrap_or(0);
        ctx.report.summary_vcpu_ms += reply["summary_vcpu_ms"].as_u64().unwrap_or(0);
        if let Some(rows) = reply["usage"].as_array() {
            for row in rows {
                write_log(runtime, ctx.buffer, WritableLogKind::FosUsage, row.clone());
            }
        }
        match reply["status"].as_str().unwrap_or_default() {
            "ok" => {
                let keys = plan.shards[id].clone();
                match serde_json::from_value::<ShardResult>(reply["result"].clone()) {
                    Ok(result)
                        if serde_json::to_vec(&result).map_or(usize::MAX, |r| r.len())
                            > result_reserve(&plan, json_len(&scope), id) =>
                    {
                        failure(ctx, &mut plan, id, "worker_result_too_large");
                    }
                    Ok(result) if valid_result(&plan, id, &keys, &result, &processed, &scope) => {
                        plan.failures.remove(id);
                        plan.results.insert(id.clone(), result);
                    }
                    _ => failure(ctx, &mut plan, id, "worker_result_invalid"),
                }
            }
            "split" if plan.shards[id].len() > 1 => {
                let mut next = plan.clone();
                let keys = next.shards.remove(id).unwrap_or_default();
                let (left, right) = keys.split_at(keys.len() / 2);
                next.shards.insert(format!("{id}a"), left.to_vec());
                next.shards.insert(format!("{id}b"), right.to_vec());
                next.failures.remove(id);
                fit_or_hold(ctx, &mut plan, next, id, &scope);
            }
            "segment" => {
                let mut next = plan.clone();
                match resegment(&mut next, id, reply["total"].as_u64()) {
                    Ok(()) => {
                        next.failures.remove(id);
                        fit_or_hold(ctx, &mut plan, next, id, &scope);
                    }
                    Err(error) => failure(ctx, &mut plan, id, error),
                }
            }
            // The worker runs another release; hold the shard until both sides match.
            "schema_mismatch" => {}
            "error" => {
                let error = reply["error"].as_str().unwrap_or("worker_error").to_owned();
                failure(ctx, &mut plan, id, &error);
            }
            _ => failure(ctx, &mut plan, id, "worker_status_invalid"),
        }
    }
    let complete = plan.shards.keys().all(|id| plan.results.contains_key(id));
    if complete && plan.abandoning.is_none() {
        return commit(ctx, &plan, watermark);
    }
    ctx.report.more_pending = true;
    if answered {
        plan.lease_until = 0;
    }
    let (plan_id, attempt) = (plan.plan_id.clone(), plan.attempt);
    save_fenced(ctx, &plan_id, attempt, &plan)?;
    Ok(())
}

/// Packs shard requests, in order, into at most `calls` groups of at most `relay` requests whose
/// relay body stays within `worker::RELAY_BODY_LIMIT`.
fn relay_groups(
    shards: Vec<(String, Value)>,
    calls: usize,
    relay: usize,
) -> Vec<Vec<(String, Value)>> {
    const ENVELOPE: usize = 64;
    let mut groups: Vec<Vec<(String, Value)>> = Vec::new();
    let mut current: Vec<(String, Value)> = Vec::new();
    let mut size = ENVELOPE;
    for (path, body) in shards {
        let len = path.len() + body.to_string().len() + 8;
        if !current.is_empty() && (current.len() == relay || size + len > worker::RELAY_BODY_LIMIT)
        {
            groups.push(std::mem::take(&mut current));
            size = ENVELOPE;
        }
        if groups.len() == calls {
            return groups;
        }
        size += len;
        current.push((path, body));
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Replaces a whole or open-segment shard whose object decompresses to `total` bytes with
/// segments from its start, the last one open-ended.
fn resegment(plan: &mut Plan, id: &str, total: Option<u64>) -> Result<(), &'static str> {
    let start = plan.segments.get(id).map_or(0, |s| s.start);
    let open = plan.segments.get(id).is_none_or(|s| s.end.is_none());
    let (Some(total), true, 1) = (total, open, plan.shards[id].len()) else {
        return Err("worker_status_invalid");
    };
    if plan.segment_bytes == 0 {
        return Err("worker_status_invalid");
    }
    plan.segment_bytes = capped_segment_bytes(plan.segment_bytes, total.saturating_sub(start));
    let count = total.saturating_sub(start).div_ceil(plan.segment_bytes);
    if count < 2 {
        return Err("worker_status_invalid");
    }
    if id.len() + 1 + (count - 1).to_string().len() > 16 {
        return Err("segment_id_too_long");
    }
    let keys = plan.shards.remove(id).unwrap_or_default();
    plan.segments.remove(id);
    for (i, segment) in cut(start, count, plan.segment_bytes).enumerate() {
        let child = format!("{id}r{i}");
        plan.shards.insert(child.clone(), keys.clone());
        plan.segments.insert(child, segment);
    }
    plan.version = SEGMENT_PLAN_VERSION;
    Ok(())
}

/// One result per raw key: a segmented object converted only if every segment did, else it
/// carries its first failing segment's status.
fn merged_objects(plan: &Plan) -> Vec<ObjectResult> {
    let mut merged: Vec<ObjectResult> = Vec::new();
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    for object in plan
        .shards
        .keys()
        .flat_map(|id| plan.results[id].objects.iter())
    {
        let Some(&index) = at.get(&object.key) else {
            at.insert(object.key.clone(), merged.len());
            merged.push(object.clone());
            continue;
        };
        let into = &mut merged[index];
        match (into.status == "converted", object.status == "converted") {
            (true, true) => {
                into.rows += object.rows;
                into.lines_read += object.lines_read;
                into.bad_lines += object.bad_lines;
                if into.bad_sample.is_none() {
                    into.bad_sample.clone_from(&object.bad_sample);
                }
                into.outputs.extend(object.outputs.iter().cloned());
            }
            (true, false) => *into = object.clone(),
            (false, _) => {}
        }
    }
    merged
}

fn commit<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    plan: &Plan,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let refreshed_watermark = if ctx.extras_sent
        && !ctx.options.extra.is_empty()
        && !ctx.rum()
        && runtime.admin_remaining_calls() > ctx.options.reserve + TICK_RESERVE + 4
    {
        runtime.admin_get(&ctx.watermark_key)?
    } else {
        None
    };
    let watermark = refreshed_watermark.as_ref().or(watermark);
    let merged = merged_objects(plan);
    let objects: Vec<&ObjectResult> = merged.iter().collect();
    let converted: Vec<&ObjectResult> = objects
        .iter()
        .copied()
        .filter(|o| o.status == "converted")
        .collect();
    let manifests: Vec<String> = plan
        .shards
        .keys()
        .map(|id| plan.results[id].manifest.clone())
        .collect();
    let indexed_sources = if ctx.rum() {
        None
    } else {
        let base = watermark
            .map(|record| record.value.clone())
            .unwrap_or(Value::Null);
        match super::source_index::prepare_remote(
            ctx.store,
            &base,
            watermark,
            plan,
            ctx.options.reserve + TICK_RESERVE + 2,
            |reply| {
                ctx.report.worker_executions_metrics.push(
                    crate::telemetry::execution::WorkerExecutionMetrics::from_reply(
                        "catalog",
                        None,
                        reply.as_ref().ok(),
                    ),
                );
                let Ok(reply) = reply else { return };
                ctx.report.worker_vcpu_ms += reply["vcpu_ms"].as_u64().unwrap_or(0);
                ctx.report.kv_ops += reply["kv_ops"].as_u64().unwrap_or(0) as u32;
                if let Some(rows) = reply["usage"].as_array() {
                    for row in rows {
                        write_log(runtime, ctx.buffer, WritableLogKind::FosUsage, row.clone());
                    }
                }
            },
        ) {
            Ok(indexed) => Some(indexed["raw_sources"].clone()),
            Err(error) if error == "source_index_pending" => {
                let mut saved = plan.clone();
                saved.lease_until = 0;
                save_fenced(ctx, &plan.plan_id, plan.attempt, &saved)?;
                ctx.report.more_pending = true;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    };
    // Fence: a tick that outlived its lease must not commit over the tick that took it over.
    let current = runtime.admin_get(&ctx.plan_key)?;
    let held = current.as_ref().is_some_and(|record| {
        record.value["plan_id"].as_str() == Some(plan.plan_id.as_str())
            && record.value["attempt"].as_u64() == Some(u64::from(plan.attempt))
            && record.value["abandoning"].is_null()
    });
    if !held {
        ctx.report.busy = true;
        return Ok(());
    }
    let held: Vec<Value> = objects
        .iter()
        .filter(|o| o.status != "converted")
        .map(|o| {
            let reason = o.reason.as_deref().unwrap_or("failed");
            let status = if quarantined(reason) {
                "quarantined"
            } else {
                "failed"
            };
            held_object(&o.key, o.gz_size, status, reason, plan.attempt)
        })
        .collect();
    let deleteq = format!("{DELETEQ_PREFIX}{}", plan.plan_id);
    // Create-only: a retry after a crash finds the same record already there.
    let mut record = json!({
        "run_id": plan.plan_id,
        "keys": converted.iter().map(|o| &o.key).collect::<Vec<_>>(),
        "sizes": converted.iter().map(|o| o.gz_size).collect::<Vec<_>>(),
        "inflight_keys": manifests,
        "held": held,
        "planned_at": plan.created_at,
        "at": runtime.now(),
    });
    if ctx.rum() {
        // raw_delete checks a RUM record against the RUM plan, not the request one.
        record["stream"] = plan.stream.clone().into();
    }
    let defer_deleteq = ctx.options.defer_deleteq
        && held.is_empty()
        && runtime.admin_remaining_calls() >= ctx.reserve() + 2;
    if !defer_deleteq {
        runtime.admin_cas(&deleteq, None, &record)?;
    }
    let last = plan.raw_keys.last().cloned().unwrap_or_default();
    let now = runtime.now();
    let swept: &[String] = if plan.straggler
        && (ctx.rum() || watermark.is_none_or(|record| record.value["raw_sources"].is_null()))
    {
        &plan.raw_keys
    } else {
        &[]
    };
    let mut value = watermark_value(
        watermark.map(|r| &r.value),
        &last,
        &plan.plan_id,
        plan.created_at,
        swept,
        now,
    );
    if let Some(indexed_sources) = indexed_sources {
        value["raw_sources"] = indexed_sources;
    }
    if let Some(after) = &plan.sweep_next {
        value["sweep"] = json!({"after": after, "at": now});
    }
    if let Some(after) = &plan.recent_after {
        value["raw_scan"] = after.clone().into();
    }
    if !ctx.rum() && ctx.options.sweep {
        let first = now.saturating_sub(settle_s(ctx.log_period())) as i64;
        let recent_mark = value["key"]
            .as_str()
            .and_then(|key| raw_key_minute(key, &ctx.raw_prefix()))
            .is_some_and(|minute| minute >= first - first.rem_euclid(60));
        let scan_due = recent_mark
            && !plan.straggler
            && plan
                .recent_after
                .as_ref()
                .is_none_or(|after| !after.is_empty());
        value["raw_scan_due"] = scan_due.into();
        ctx.report.more_pending |= scan_due;
    }
    let late = plan
        .shards
        .keys()
        .flat_map(|id| plan.results[id].outputs.iter())
        .filter_map(|output| super::parse_processed(&output.key))
        .any(|name| {
            let hour = name.start - name.start.rem_euclid(3600);
            super::manifest_hour_closed(hour, ctx.log_period(), now)
        });
    if late {
        value = super::bump_manifest_gen(watermark.map(|r| &r.value), value);
    }
    if !ctx.rum() {
        let files = plan
            .shards
            .keys()
            .flat_map(|id| plan.results[id].outputs.iter().cloned())
            .collect::<Vec<_>>();
        let summaries = plan
            .shards
            .keys()
            .flat_map(|id| plan.results[id].summaries.iter().cloned())
            .collect::<Vec<_>>();
        let prepared = super::catalog::prepare_reserving(
            ctx.store,
            &value,
            &plan.plan_id,
            &plan.schema,
            &files,
            &summaries,
            &converted.iter().map(|o| o.key.clone()).collect::<Vec<_>>(),
            &[],
            ctx.options.reserve + TICK_RESERVE + 2 + u32::from(ctx.options.defer_deleteq),
        );
        value = match prepared {
            Ok(value) => value,
            Err(error) if error == "catalog_prepare_pending" => {
                let mut saved = plan.clone();
                saved.lease_until = 0;
                runtime.admin_cas(
                    &ctx.plan_key,
                    current.as_ref().map(|record| record.generation.as_str()),
                    &serde_json::to_value(saved).map_err(|_| "ingest_encode")?,
                )?;
                ctx.report.more_pending = true;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
    }
    let committed = runtime.admin_cas(
        &ctx.watermark_key,
        watermark.map(|r| r.generation.as_str()),
        &value,
    );
    if !matches!(committed, Ok(true)) {
        // Lost or unknown. If the watermark names this plan, another tick (or this write after
        // all) committed it and owns the queue record; otherwise withdraw it. raw_delete never
        // consumes the record of an uncommitted plan, so leaving it when out of budget is safe.
        ctx.report.busy = true;
        let mut ours = false;
        if !defer_deleteq && runtime.admin_remaining_calls() >= ctx.reserve() + 2 {
            let current_wm = runtime.admin_get(&ctx.watermark_key)?;
            ours = current_wm.as_ref().and_then(|r| r.value["run_id"].as_str())
                == Some(plan.plan_id.as_str());
            if !ours {
                runtime.kv_delete(&deleteq)?;
            }
        }
        if !ours
            && matches!(committed, Ok(false))
            && runtime.admin_remaining_calls() > ctx.reserve()
        {
            let mut saved = plan.clone();
            saved.lease_until = 0;
            if let Ok(encoded) = serde_json::to_value(saved) {
                let _ = runtime.admin_cas(
                    &ctx.plan_key,
                    current.as_ref().map(|record| record.generation.as_str()),
                    &encoded,
                );
            }
            ctx.report.more_pending = true;
        }
        committed?;
        return Ok(());
    }
    if defer_deleteq {
        ctx.report.pending_deleteq = Some((deleteq, record));
    }
    ctx.report.more_pending |= ctx.report.resumed && plan.created_at < now;
    ctx.report.backlog = plan.backlog;
    for id in plan.shards.keys() {
        for output in &plan.results[id].outputs {
            ctx.report.rows += output.rows;
            ctx.report.bytes += output.bytes;
            ctx.report.files.push(output.key.clone());
        }
    }
    for object in objects {
        object_rows(ctx, plan, object);
    }
    Ok(())
}

fn object_rows<R: Runtime>(ctx: &mut Ctx<'_, '_, R>, plan: &Plan, object: &ObjectResult) {
    let runtime = ctx.store.runtime;
    let row = if object.status == "converted" {
        ctx.report.converted += 1;
        ctx.report.lines_read += object.lines_read;
        ctx.report.bad_lines += object.bad_lines;
        if let Some(sample) = &object.bad_sample {
            write_log(
                runtime,
                ctx.buffer,
                WritableLogKind::PlatformErrors,
                json!({
                    "error_domain": "ingest_corrupt", "code": "bad_ndjson_line",
                    "severity": "warn", "caller": "cron:ingest", "object_key": object.key,
                    "message": format!("{} of {} line(s) rejected", object.bad_lines, object.lines_read),
                    "sample_json": sample_json(sample),
                }),
            );
        }
        json!({
            "status": "converted", "rows": object.rows, "lines_read": object.lines_read,
            "bad_lines": object.bad_lines,
            "output_keys_json": json!(object.outputs.iter().map(|o| {
                json!({"key": o.key, "rows": o.rows, "bytes": o.bytes})
            }).collect::<Vec<_>>()).to_string(),
        })
    } else {
        ctx.report.failed += 1;
        let reason = object.reason.as_deref().unwrap_or("failed");
        let corrupt = quarantined(reason);
        if corrupt {
            write_log(
                runtime,
                ctx.buffer,
                WritableLogKind::PlatformErrors,
                json!({
                    "error_domain": "ingest_corrupt", "code": reason, "severity": "warn",
                    "caller": "cron:ingest", "object_key": object.key,
                    "message": if reason == "too_many_windows" {
                        "raw object spans too many windows and was quarantined"
                    } else {
                        "raw object could not be decoded and was quarantined"
                    },
                }),
            );
        }
        json!({
            "status": if corrupt { "quarantined" } else { "failed" },
            "severity": "warn", "reason": reason, "rows": 0, "lines_read": 0,
            "bad_lines": 0, "output_keys_json": "[]",
        })
    };
    let Value::Object(mut row) = row else {
        return;
    };
    row.insert("run_id".into(), plan.plan_id.clone().into());
    row.insert("dataset".into(), plan.table.clone().into());
    row.insert("raw_key".into(), object.key.clone().into());
    row.insert("raw_size".into(), object.gz_size.into());
    row.insert("state_version".into(), 1.into());
    row.insert("raw_deleted".into(), false.into());
    write_log(
        runtime,
        ctx.buffer,
        WritableLogKind::IngestFiles,
        Value::Object(row),
    );
}

/// Output keys named by an intent manifest, or `None` if it is unreadable or names a key outside
/// this plan's outputs (window files under `processed`, summaries under `summaries`).
fn manifest_outputs(
    body: &[u8],
    processed: &str,
    summaries: &str,
    token: &str,
) -> Option<Vec<String>> {
    let intent: Value = serde_json::from_slice(body).ok()?;
    intent["keys"]
        .as_array()?
        .iter()
        .map(|key| {
            key.as_str()
                .filter(|key| {
                    (key.starts_with(processed) || key.starts_with(summaries))
                        && key.contains(token)
                })
                .map(str::to_owned)
        })
        .collect()
}

/// Hour folders of `processed/<table>/` that can hold this plan's windows, as a `start_after`
/// and an exclusive end key; `None` when a raw key has no minute (its rows route by `now`).
fn sweep_range(plan: &Plan, raw_prefix: &str, processed: &str) -> Option<(String, String)> {
    let minutes: Vec<i64> = plan
        .raw_keys
        .iter()
        .map(|key| raw_key_minute(key, raw_prefix))
        .collect::<Option<_>>()?;
    let reach = convert::ROUTING_TOLERANCE_S + plan.window_s;
    let low = minutes.iter().min()? - reach;
    let high = minutes.iter().max()? + 60 + reach;
    Some((
        format!("{processed}{}", super::hour_dir(low)),
        format!("{processed}{}", super::hour_dir(high + 3600)),
    ))
}

/// Deletes every output named by the abandoned plan's intent manifests, then the manifests, and
/// once none are left replans the same raw keys under a new `plan_id`. An unreadable manifest is
/// logged and replaced by a resumable sweep of the plan's hour folders, window files then
/// summaries (RUM: errors then vitals), for its `-<plan_id>s` token. Resumable: whatever is left is retried by the next
/// tick that takes the lease.
fn cleanup_and_replan<R: Runtime>(
    ctx: &mut Ctx<'_, '_, R>,
    mut plan: Plan,
    watermark: Option<&Record>,
) -> Result<(), String> {
    let runtime = ctx.store.runtime;
    let prefix = format!("{}{}/", inflight_prefix(ctx.store.service), plan.plan_id);
    let page = ctx.store.list(&prefix, None, None, LIST_PAGE, None)?;
    let processed = ctx.processed_prefix();
    let summaries = summary::summary_prefix(&scope(ctx.store.service), &ctx.options.table);
    let token = format!("-{}s", plan.plan_id);
    let mut read = Vec::new();
    let mut outputs = Vec::new();
    let mut complete = page.next_token.is_none();
    for object in &page.objects {
        if runtime.admin_remaining_calls() < ctx.reserve() + 1 + CLEANUP_TAIL
            || !ctx.store.within_wall()
        {
            complete = false;
            break;
        }
        if let Some(body) = ctx.store.get_direct(&object.key, MANIFEST_LIMIT)? {
            match manifest_outputs(&body, &processed, &summaries, &token) {
                Some(keys) => outputs.extend(keys),
                None => {
                    write_log(
                        runtime,
                        ctx.buffer,
                        WritableLogKind::PlatformErrors,
                        json!({"error_domain": "ingest_plan", "code": "ingest_manifest_invalid",
                            "severity": "warn", "caller": "cron:ingest", "object_key": object.key,
                            "message": format!("plan {}: unreadable intent manifest; sweeping for its outputs", plan.plan_id)}),
                    );
                    if !plan.swept && plan.sweep_after.is_none() {
                        plan.sweep_after = Some(String::new());
                    }
                }
            }
        }
        read.push(object.key.clone());
    }
    if let Some(mut after) = plan.sweep_after.clone() {
        let raw_prefix = ctx.raw_prefix();
        // The trees sort in order and none contains another (`processed/<table>/` before
        // `processed/summary/`; RUM's `client_errors/` before `client_vitals/`), so the cursor
        // itself says which phase the sweep is in.
        let trees = if ctx.rum() {
            vec![
                format!("{processed}{}/", super::rum::ERRORS),
                format!("{processed}{}/", super::rum::VITALS),
            ]
        } else {
            vec![processed.clone(), summaries.clone()]
        };
        let mut phase = trees
            .iter()
            .rposition(|tree| after.starts_with(tree.as_str()))
            .unwrap_or(0);
        let mut tree = trees[phase].clone();
        let mut range = sweep_range(&plan, &raw_prefix, &tree);
        let mut end = range.as_ref().map(|r| r.1.clone());
        if after.is_empty() {
            after = range.take().map(|r| r.0).unwrap_or_default();
        }
        loop {
            // A LIST and a DeleteObjects per page.
            if runtime.admin_remaining_calls() < ctx.reserve() + 2 + CLEANUP_TAIL
                || !ctx.store.within_wall()
            {
                break;
            }
            let start = (!after.is_empty() && after != tree).then_some(after.as_str());
            let page = ctx.store.list(&tree, start, None, LIST_PAGE, None)?;
            let mut done = page.next_token.is_none();
            let mut found = Vec::new();
            for object in page.objects {
                if end.as_ref().is_some_and(|end| object.key >= *end) {
                    done = true;
                    break;
                }
                if object.key.contains(&token) {
                    found.push(object.key.clone());
                }
                after = object.key;
            }
            if !found.is_empty() && !ctx.store.delete_objects(&found)?.failed.is_empty() {
                // Retry this page next tick.
                break;
            }
            plan.sweep_after = Some(after.clone());
            if done && phase + 1 < trees.len() {
                phase += 1;
                tree = trees[phase].clone();
                let range = sweep_range(&plan, &raw_prefix, &tree);
                end = range.as_ref().map(|r| r.1.clone());
                after = range.map_or_else(|| tree.clone(), |r| r.0);
                plan.sweep_after = Some(after.clone());
                continue;
            }
            if done {
                plan.sweep_after = None;
                plan.swept = true;
                break;
            }
        }
        complete &= plan.swept;
    }
    // Manifests stay until every output they could name is gone.
    let mut cleared = if plan.sweep_after.is_some() {
        Vec::new()
    } else {
        read.clone()
    };
    if !outputs.is_empty() {
        let result = ctx.store.delete_objects(&outputs)?;
        if !result.failed.is_empty() {
            complete = false;
            cleared.clear();
        }
    }
    if !cleared.is_empty() {
        let result = ctx.store.delete_objects(&cleared)?;
        complete &= result.failed.is_empty();
    }
    complete &= cleared.len() == read.len();
    let (old_id, attempt) = (plan.plan_id.clone(), plan.attempt);
    // The replan below writes the service's current schema, so its fields lock first.
    complete = complete && commit_fields(ctx, CLEANUP_TAIL)?;
    if complete {
        // A queue record left by a commit that never landed names these raw keys; it must be
        // gone before the plan id changes, or raw_delete would treat it as committed.
        complete = runtime
            .kv_delete(&format!("{DELETEQ_PREFIX}{old_id}"))
            .is_ok();
    }
    if !complete {
        ctx.report.more_pending = true;
        plan.lease_until = 0;
        save_fenced(ctx, &old_id, attempt, &plan)?;
        return Ok(());
    }
    let raw_prefix = ctx.raw_prefix();
    let objects: Vec<(String, u64)> = plan
        .raw_keys
        .iter()
        .cloned()
        .zip(plan.sizes.iter().copied().chain(std::iter::repeat(0)))
        .collect();
    let replans = plan.replans + 1;
    let laid = layout(
        &objects,
        usize::MAX,
        replans,
        &raw_prefix,
        plan.window_s,
        ctx.period_s(),
        false,
        plan.segment_bytes,
    );
    let nonce = new_nonce(runtime)?;
    let schema = ctx.schema_hash.clone();
    let old = plan.clone();
    let mut replanned = Plan {
        nonce,
        schema,
        custom: ctx.custom.clone(),
        version: plan_version(&laid.segments),
        shards: laid.shards,
        segments: laid.segments,
        results: BTreeMap::new(),
        failures: BTreeMap::new(),
        budget_stopped: BTreeSet::new(),
        attempt,
        lease_until: 0,
        abandoning: None,
        replans,
        sweep_after: None,
        swept: false,
        ..plan
    };
    let scope = scope(ctx.store.service);
    let count = replanned.raw_keys.len();
    fit_plan(&mut replanned, json_len(&scope));
    if replanned.straggler && replanned.raw_keys.len() < count {
        let prior = watermark
            .and_then(|r| r.value["sweep"]["after"].as_str())
            .unwrap_or_default();
        replanned.sweep_next = Some(prior.to_owned());
    }
    replanned.plan_id = plan_id(
        &replanned.stream,
        &replanned.table,
        &replanned.schema,
        replanned.window_s,
        &replanned.raw_keys,
        &replanned.nonce,
    );
    replan_budget_stopped(ctx, &old, &mut replanned, &scope);
    replanned.version = plan_version(&replanned.segments);
    if save_fenced(ctx, &old_id, attempt, &replanned)? {
        write_log(
            runtime,
            ctx.buffer,
            WritableLogKind::PlatformErrors,
            json!({"error_domain": "ingest_plan", "code": "plan_replanned", "severity": "info",
                "caller": "cron:ingest",
                "message": format!("plan {old_id} replaced by {} after cleanup", replanned.plan_id)}),
        );
    }
    ctx.report.more_pending = true;
    Ok(())
}
