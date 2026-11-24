#![allow(dead_code)]
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{FosRequest, FosResponse, Record, Service},
    ingest::{self, api},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime, PREFETCH_UNREAD},
    telemetry::{self, usage::UsageAgg, LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    io::Write,
    time::Instant,
};

pub(super) const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
pub(super) const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
pub(super) const BUDGET: u32 = 32;

/// A multipart upload's key and the parts received.
type Upload = (String, BTreeMap<u32, Vec<u8>>);

pub(super) struct Fake {
    pub(super) kv: RefCell<BTreeMap<String, (u64, Value)>>,
    pub(super) bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    pub(super) calls: Cell<u32>,
    pub(super) secrets_read: RefCell<BTreeSet<String>>,
    pub(super) now: Cell<u64>,
    pub(super) window: RefCell<Option<String>>,
    pub(super) puts: Cell<u32>,
    pub(super) delete_posts: RefCell<Vec<usize>>,
    pub(super) delete_status: Cell<u16>,
    pub(super) worker_calls: RefCell<Vec<usize>>,
    pub(super) fail_delete_keys: RefCell<BTreeSet<String>>,
    pub(super) fail_put_number: Cell<Option<u32>>,
    pub(super) max_list_keys: Cell<Option<usize>>,
    pub(super) fail_watermark: Cell<bool>,
    pub(super) conflict_watermark: Cell<bool>,
    /// Plan writes left whose next read answers with the record they replaced, as an
    /// eventually consistent KV read right after this execution's own write does.
    pub(super) stale_plan_reads: Cell<u32>,
    /// Plan reads after such a write that still answer correctly before the stale one.
    pub(super) stale_plan_skip: Cell<u32>,
    pub(super) previous_plan: RefCell<Option<Record>>,
    /// Watermark writes left whose next read answers with the record they replaced, as an
    /// eventually consistent KV read right after `catalog::bundles::publish` does.
    pub(super) stale_watermark_reads: Cell<u32>,
    pub(super) previous_watermark: RefCell<Option<Record>>,
    pub(super) fail_compact_after_publish: Cell<bool>,
    pub(super) compact_conflict_after: Cell<Option<u32>>,
    pub(super) stale_compact_state: RefCell<Option<Record>>,
    /// Each compaction state write leaves the next fresh read showing the record it replaced.
    pub(super) lag_compact_reads: Cell<bool>,
    /// How many fresh reads after the next compaction state write still show the value it
    /// replaced; later writes lag one read.
    pub(super) lagging_compact_reads: Cell<u32>,
    pub(super) lagging_compact_state: RefCell<Option<Record>>,
    pub(super) lagging_left: Cell<u32>,
    /// The injected compaction state conflict writes another writer's value, not the same one.
    pub(super) foreign_compact_conflict: Cell<bool>,
    /// After this many more compaction state writes, one lands but reports a lost CAS, as when
    /// a write whose first attempt errored after landing is retried against its old generation.
    pub(super) compact_write_lands_unreported: Cell<Option<u32>>,
    pub(super) fail_fresh_compact_state: Cell<bool>,
    /// Errors the next fresh compaction state reads return, one per read.
    pub(super) fresh_compact_state_glitches: RefCell<std::collections::VecDeque<String>>,
    /// Compaction state CAS attempts.
    pub(super) compact_state_writes: Cell<u32>,
    pub(super) cdn_gets: RefCell<Vec<String>>,
    /// GET keys of each `admin_fos_many` round.
    pub(super) fos_rounds: RefCell<Vec<Vec<String>>>,
    pub(super) fos_concurrent: Cell<bool>,
    /// Keys of every direct object GET.
    pub(super) fos_gets: RefCell<Vec<String>>,
    /// Sends `admin_fos_many`'s speculative reads but leaves them unread, as Fastly's runtime
    /// does with CDN reads.
    pub(super) fos_defer: Cell<bool>,
    /// Deferred reads not read yet, with their response limits.
    pub(super) fos_unread: RefCell<BTreeMap<String, usize>>,
    /// Most response-limit bytes ever outstanding in `fos_unread`.
    pub(super) fos_unread_peak: Cell<usize>,
    pub(super) range_reads: RefCell<Vec<(String, u64, usize)>>,
    pub(super) source_bytes: Cell<u64>,
    pub(super) api_calls: RefCell<Vec<(String, String)>>,
    pub(super) api_queries: RefCell<Vec<(String, String)>>,
    pub(super) api_responses: RefCell<std::collections::VecDeque<(u16, Value)>>,
    pub(super) config_overrides: RefCell<BTreeMap<String, Option<String>>>,
    /// Status every Fastly API call answers with.
    pub(super) api_status: Cell<u16>,
    pub(super) kv_reads: RefCell<Vec<String>>,
    pub(super) seq: std::cell::Cell<u64>,
    pub(super) cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    pub(super) vcpu: Cell<u64>,
    pub(super) vcpu_step: Cell<u64>,
    pub(super) elapsed_s: Cell<u64>,
    /// Milliseconds each backend call advances `now` by, and the part of a second not yet added.
    pub(super) call_ms: Cell<u64>,
    pub(super) call_ms_carry: Cell<u64>,
    pub(super) usage: UsageAgg,
    /// Calls the coordinator (main execution) may make before every further call fails, to
    /// simulate the execution dying at that point.
    pub(super) crash_after: Cell<Option<u32>>,
    /// Per dispatched ingest request, in order: how the fake worker transport behaves.
    pub(super) worker_faults: RefCell<std::collections::VecDeque<WorkerFault>>,
    pub(super) worker_max_calls: Cell<u32>,
    pub(super) worker_runs: Cell<u32>,
    /// Worker requests sent by the current main execution, and by every main execution so far.
    pub(super) worker_sent: Cell<u32>,
    pub(super) worker_sent_total: Cell<u32>,
    pub(super) in_worker: Cell<bool>,
    pub(super) bad_etag_for: RefCell<Option<String>>,
    /// Core Cache operations attempted in the current execution, and surrogate keys per entry.
    pub(super) cache_ops: Cell<u32>,
    pub(super) cache_tags: RefCell<BTreeMap<String, Vec<String>>>,
    /// Most backend calls and cache ops any one execution of each kind has used.
    pub(super) peaks: RefCell<BTreeMap<&'static str, (u32, u32)>>,
    /// Most vCPU milliseconds any one execution of each kind has reported.
    pub(super) vcpu_peaks: RefCell<BTreeMap<&'static str, u64>>,
    pub(super) stage_runs: RefCell<Vec<Value>>,
    pub(super) events: RefCell<Vec<Value>>,
    /// A `maint_next` another tick saves while the stage worker runs.
    pub(super) race_maint_next: Cell<Option<u64>>,
    /// `cron:state` CAS attempts that lose to a concurrent writer before succeeding.
    pub(super) race_cron_state: Cell<u32>,
    /// Whether this POP is the log shield (IAD), and `cron:state` CAS writes so far.
    pub(super) shield: Cell<bool>,
    pub(super) cron_cas: Cell<u32>,
    /// The FOS key secrets read as empty: an install not yet provisioned with credentials.
    pub(super) no_fos: Cell<bool>,
    pub(super) fos_envelope: Cell<bool>,
    /// Every `cron:state` read fails.
    pub(super) fail_cron_state: Cell<bool>,
    /// LIST `LastModified` per key; others read as `BASE`.
    pub(super) mtimes: RefCell<BTreeMap<String, i64>>,
    /// What `heap_mib` reports.
    pub(super) heap_mib: Cell<Option<u32>>,
    pub(super) linear_memory_high_water_bytes: Cell<Option<u64>>,
    /// Transient answers the next PUTs (`"PUT"`), CDN GETs (`"GET"`) or `DeleteObjects` requests
    /// (`"DELETE"`) get instead of running,
    /// as `(method, status, Retry-After seconds)`.
    pub(super) transient: RefCell<std::collections::VecDeque<(&'static str, u16, Option<u64>)>>,
    /// Every `admin_wait`, in milliseconds.
    pub(super) waits: RefCell<Vec<u64>>,
    /// The `content_md5` each PUT carried, per key.
    pub(super) put_md5s: RefCell<BTreeMap<String, Option<String>>>,
    /// Open multipart uploads by id: the key and the parts received.
    pub(super) uploads: RefCell<BTreeMap<String, Upload>>,
    pub(super) upload_seq: Cell<u32>,
    /// vCPU milliseconds a GET of a key containing the pattern spends, as reading and decoding
    /// a multi-MB body does live.
    pub(super) get_vcpu: RefCell<Vec<(&'static str, u64)>>,
    /// Simulated `(LIST, DeleteObjects)` milliseconds; when set, `admin_elapsed` is the real
    /// time since `started` plus the FOS time this execution spent, an `admin_fos_many` round
    /// costing only its slowest request.
    pub(super) fos_wall: Cell<Option<(u64, u64)>>,
    pub(super) wall_ms: Cell<u64>,
    pub(super) in_round: Cell<bool>,
    /// The `DeleteObjects` requests each `admin_fos_many` round sent together.
    pub(super) delete_rounds: RefCell<Vec<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorkerFault {
    Ok,
    /// The request never reaches the worker.
    Unreachable,
    /// The worker runs (and writes) but its answer is lost.
    LostReply,
}

impl Fake {
    pub(super) fn spend_get_vcpu(&self, key: &str) {
        for (pattern, ms) in self.get_vcpu.borrow().iter() {
            if key.contains(pattern) {
                self.vcpu.set(self.vcpu.get() + ms);
            }
        }
    }

    pub(super) fn recorded_get(&self, key: &str) -> Option<Vec<u8>> {
        self.fos_gets.borrow_mut().push(key.to_owned());
        self.spend_get_vcpu(key);
        self.bucket.borrow().get(key).cloned()
    }

    pub(super) fn new(now: i64) -> Self {
        Self {
            kv: Default::default(),
            bucket: Default::default(),
            calls: Cell::new(0),
            secrets_read: Default::default(),
            now: Cell::new(now as u64),
            window: Default::default(),
            puts: Cell::new(0),
            delete_posts: Default::default(),
            delete_status: Cell::new(200),
            worker_calls: Default::default(),
            fail_delete_keys: Default::default(),
            fail_put_number: Cell::new(None),
            max_list_keys: Cell::new(None),
            fail_watermark: Cell::new(false),
            conflict_watermark: Cell::new(false),
            stale_plan_reads: Cell::new(0),
            stale_plan_skip: Cell::new(0),
            previous_plan: Default::default(),
            stale_watermark_reads: Cell::new(0),
            previous_watermark: Default::default(),
            fail_compact_after_publish: Cell::new(false),
            compact_conflict_after: Cell::new(None),
            stale_compact_state: Default::default(),
            lag_compact_reads: Cell::new(false),
            lagging_compact_reads: Cell::new(1),
            lagging_compact_state: Default::default(),
            lagging_left: Cell::new(0),
            foreign_compact_conflict: Cell::new(false),
            compact_write_lands_unreported: Cell::new(None),
            fail_fresh_compact_state: Cell::new(false),
            fresh_compact_state_glitches: Default::default(),
            compact_state_writes: Cell::new(0),
            cdn_gets: Default::default(),
            fos_rounds: Default::default(),
            fos_concurrent: Cell::new(true),
            fos_gets: Default::default(),
            fos_defer: Cell::new(false),
            fos_unread: Default::default(),
            fos_unread_peak: Cell::new(0),
            range_reads: Default::default(),
            source_bytes: Cell::new(0),
            api_calls: Default::default(),
            api_queries: Default::default(),
            api_responses: Default::default(),
            config_overrides: Default::default(),
            api_status: Cell::new(200),
            kv_reads: Default::default(),
            seq: Default::default(),
            cache: Default::default(),
            vcpu: Cell::new(0),
            vcpu_step: Cell::new(1),
            elapsed_s: Cell::new(0),
            call_ms: Cell::new(0),
            call_ms_carry: Cell::new(0),
            usage: UsageAgg::default(),
            crash_after: Cell::new(None),
            worker_faults: Default::default(),
            worker_max_calls: Cell::new(0),
            worker_runs: Cell::new(0),
            worker_sent: Cell::new(0),
            worker_sent_total: Cell::new(0),
            in_worker: Cell::new(false),
            bad_etag_for: Default::default(),
            cache_ops: Cell::new(0),
            cache_tags: Default::default(),
            peaks: Default::default(),
            vcpu_peaks: Default::default(),
            stage_runs: Default::default(),
            events: Default::default(),
            race_maint_next: Cell::new(None),
            race_cron_state: Cell::new(0),
            shield: Cell::new(true),
            cron_cas: Cell::new(0),
            no_fos: Cell::new(false),
            fos_envelope: Cell::new(false),
            fail_cron_state: Cell::new(false),
            mtimes: Default::default(),
            heap_mib: Cell::new(None),
            linear_memory_high_water_bytes: Cell::new(None),
            transient: Default::default(),
            waits: Default::default(),
            put_md5s: Default::default(),
            uploads: RefCell::new(BTreeMap::new()),
            upload_seq: Cell::new(0),
            get_vcpu: Default::default(),
            fos_wall: Cell::new(None),
            wall_ms: Cell::new(0),
            in_round: Cell::new(false),
            delete_rounds: Default::default(),
        }
    }
    pub(super) fn fos_ms(&self, request: &FosRequest<'_>) -> u64 {
        let Some((list, delete)) = self.fos_wall.get() else {
            return 0;
        };
        match request.method {
            "POST" if request.query == "delete" => delete,
            "GET" if request.query.contains("list-type") => list,
            _ => 0,
        }
    }
    pub(super) fn take_transient(&self, method: &str) -> Option<FosResponse> {
        let mut queue = self.transient.borrow_mut();
        let (_, status, retry_after) = queue.front().filter(|(m, _, _)| *m == method).copied()?;
        queue.pop_front();
        Some(FosResponse {
            cdn_cache: None,
            retry_after: retry_after.map(std::time::Duration::from_secs),
            status,
            body: if status == 503 {
                b"<Error><Code>SlowDown</Code></Error>".to_vec()
            } else {
                vec![]
            },
        })
    }
    pub(super) fn cache_op(&self) -> Result<(), CoreCacheError> {
        self.cache_ops.set(self.cache_ops.get() + 1);
        if self.cache_ops.get() > fastly_log_analytics::shell::CACHE_OP_BUDGET {
            return Err(CoreCacheError::Failed("cache_op_budget_exhausted".into()));
        }
        Ok(())
    }
    /// Records the current execution's calls and cache ops under `kind`.
    pub(super) fn sent_to_worker(&self) {
        self.worker_sent.set(self.worker_sent.get() + 1);
        if !self.in_worker.get() {
            self.worker_sent_total.set(self.worker_sent_total.get() + 1);
        }
    }
    pub(super) fn note_peak(&self, kind: &'static str) {
        let mut peaks = self.peaks.borrow_mut();
        let peak = peaks.entry(kind).or_default();
        peak.0 = peak.0.max(self.calls.get());
        peak.1 = peak.1.max(self.cache_ops.get());
    }
    /// Runs `body` as a fresh execution with its own call budget, Secret Store reads and cache
    /// ops, then restores the caller's.
    pub(super) fn execution<T>(&self, kind: &'static str, body: impl FnOnce() -> T) -> T {
        let spent = self.calls.replace(0);
        let bytes = self.source_bytes.replace(0);
        let vcpu = self.vcpu.replace(0);
        let ops = self.cache_ops.replace(0);
        let sent = self.worker_sent.replace(0);
        let secrets = std::mem::take(&mut *self.secrets_read.borrow_mut());
        let in_worker = self.in_worker.replace(true);
        let wall = self.wall_ms.replace(0);
        let out = body();
        self.wall_ms.set(wall);
        self.in_worker.set(in_worker);
        self.note_peak(kind);
        let mut peaks = self.vcpu_peaks.borrow_mut();
        let peak = peaks.entry(kind).or_default();
        *peak = (*peak).max(self.vcpu.get());
        drop(peaks);
        self.calls.set(spent);
        self.source_bytes.set(bytes);
        self.vcpu.set(vcpu);
        self.cache_ops.set(ops);
        self.worker_sent.set(sent);
        *self.secrets_read.borrow_mut() = secrets;
        out
    }
    pub(super) fn charge(&self) -> Result<(), String> {
        if self.calls.get() >= BUDGET {
            return Err("backend_budget_exhausted".into());
        }
        if !self.in_worker.get()
            && self
                .crash_after
                .get()
                .is_some_and(|n| self.calls.get() >= n)
        {
            return Err("injected_crash".into());
        }
        self.calls.set(self.calls.get() + 1);
        let ms = self.call_ms_carry.get() + self.call_ms.get();
        self.now.set(self.now.get() + ms / 1000);
        self.call_ms_carry.set(ms % 1000);
        Ok(())
    }
    pub(super) fn new_request(&self) {
        self.calls.set(0);
        self.source_bytes.set(0);
        self.vcpu.set(0);
        self.worker_sent.set(0);
        self.cache_ops.set(0);
        self.wall_ms.set(0);
        self.secrets_read.borrow_mut().clear();
    }
    pub(super) fn keys(&self, prefix: &str) -> Vec<String> {
        self.bucket
            .borrow()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
}

impl SetupStores for Fake {
    fn config_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for Fake {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        if let Some(value) = self.config_overrides.borrow().get(name) {
            return Ok(value.clone());
        }
        Ok(if name == "window_s" {
            self.window.borrow().clone()
        } else if name == "dev_telemetry" {
            Some("1".into())
        } else {
            Some("v".into())
        })
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        if self.secrets_read.borrow_mut().insert(name.into()) {
            self.charge()?;
        }
        if self.no_fos.get() && matches!(name, "ak" | "sk") {
            return Ok(Some(String::new()));
        }
        if self.fos_envelope.get() {
            if name.starts_with("fos_ak.") {
                return Ok(None);
            }
            if name.starts_with("fos_sk.") {
                return Ok(Some(
                    json!({"access_key": "fos-secret", "secret_key": "fos-secret"}).to_string(),
                ));
            }
        }
        Ok(Some(
            match name {
                "cdn_key" => "ck",
                "session_key" => "session-signing-material-for-the-cursor",
                _ => "fos-secret",
            }
            .into(),
        ))
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let n = self.seq.get() + 1;
        self.seq.set(n);
        let mut bytes = [7u8; 32];
        bytes[..8].copy_from_slice(&n.to_be_bytes());
        Ok(bytes)
    }
    fn now(&self) -> u64 {
        self.now.get()
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.charge()?;
        self.kv_reads.borrow_mut().push(key.into());
        if key == "cron:state" && self.fail_cron_state.get() {
            return Err("injected_cron_state_failure".into());
        }
        if key == ingest::compaction::STATE_KEY {
            if let Some(record) = self.stale_compact_state.borrow().as_ref() {
                return Ok(Some(record.clone()));
            }
        }
        if key == ingest::PLAN_KEY && self.previous_plan.borrow().is_some() {
            if self.stale_plan_skip.get() > 0 {
                self.stale_plan_skip.set(self.stale_plan_skip.get() - 1);
            } else if let Some(record) = self.previous_plan.borrow_mut().take() {
                return Ok(Some(record));
            }
        }
        if key == ingest::WATERMARK_KEY {
            if let Some(record) = self.previous_watermark.borrow_mut().take() {
                return Ok(Some(record));
            }
        }
        Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
            generation: g.to_string(),
            value: v.clone(),
        }))
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.charge()?;
        if key == ingest::WATERMARK_KEY && self.fail_watermark.get() {
            return Err("injected_watermark_failure".into());
        }
        if key == ingest::WATERMARK_KEY && self.conflict_watermark.get() {
            let mut kv = self.kv.borrow_mut();
            if let Some((g, v)) = kv.get(key).cloned() {
                kv.insert(key.into(), (g + 1, v));
            }
            return Ok(false);
        }
        if key == ingest::compaction::STATE_KEY {
            self.compact_state_writes
                .set(self.compact_state_writes.get() + 1);
        }
        if key == ingest::compaction::STATE_KEY
            && self.fail_compact_after_publish.get()
            && value["job"].is_null()
            && value["queue"]
                .as_array()
                .is_some_and(|queue| !queue.is_empty())
        {
            return Err("injected_post_publication_failure".into());
        }
        let mut kv = self.kv.borrow_mut();
        if key == ingest::compaction::STATE_KEY {
            if let Some(remaining) = self.compact_conflict_after.get() {
                if remaining == 0 {
                    self.compact_conflict_after.set(None);
                    if let Some((g, mut v)) = kv.get(key).cloned() {
                        if self.foreign_compact_conflict.get() {
                            v["lease_until"] = json!(u64::MAX);
                        }
                        kv.insert(key.into(), (g + 1, v));
                    }
                    return Ok(false);
                }
                self.compact_conflict_after.set(Some(remaining - 1));
            }
        }
        if key == "cron:state" {
            self.cron_cas.set(self.cron_cas.get() + 1);
        }
        if key == "cron:state" && generation.is_some() && self.race_cron_state.get() > 0 {
            self.race_cron_state.set(self.race_cron_state.get() - 1);
            if let Some((g, v)) = kv.get(key).cloned() {
                kv.insert(key.into(), (g + 1, v));
            }
            return Ok(false);
        }
        let current = kv.get(key).map(|(g, _)| g.to_string());
        if current.as_deref() != generation {
            return Ok(false);
        }
        let next = kv.get(key).map_or(1, |(g, _)| g + 1);
        if key == ingest::compaction::STATE_KEY && self.lag_compact_reads.get() {
            *self.lagging_compact_state.borrow_mut() = kv.get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            });
            self.lagging_left.set(self.lagging_compact_reads.replace(1));
        }
        if key == ingest::PLAN_KEY && self.stale_plan_reads.get() > 0 {
            self.stale_plan_reads.set(self.stale_plan_reads.get() - 1);
            *self.previous_plan.borrow_mut() = kv.get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            });
        }
        if key == ingest::WATERMARK_KEY && self.stale_watermark_reads.get() > 0 {
            self.stale_watermark_reads
                .set(self.stale_watermark_reads.get() - 1);
            *self.previous_watermark.borrow_mut() = kv.get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            });
        }
        kv.insert(key.into(), (next, value.clone()));
        if key == ingest::compaction::STATE_KEY {
            match self.compact_write_lands_unreported.get() {
                Some(0) => {
                    self.compact_write_lands_unreported.set(None);
                    return Ok(false);
                }
                Some(n) => self.compact_write_lands_unreported.set(Some(n - 1)),
                None => {}
            }
        }
        Ok(true)
    }
    fn admin_get_fresh(&self, key: &str, token: &str) -> Result<Option<Record>, String> {
        assert_eq!(
            token, "fos-secret",
            "fresh KV confirmation requires the configured API token"
        );
        if key == ingest::compaction::STATE_KEY {
            self.charge()?;
            self.kv_reads.borrow_mut().push(key.into());
            if self.fail_fresh_compact_state.get() {
                return Err("injected_fresh_compact_state_failure".into());
            }
            if let Some(error) = self.fresh_compact_state_glitches.borrow_mut().pop_front() {
                return Err(error);
            }
            let lagging = self.lagging_compact_state.borrow().clone();
            if let Some(record) = lagging {
                let left = self.lagging_left.get().saturating_sub(1);
                self.lagging_left.set(left);
                if left == 0 {
                    self.lagging_compact_state.borrow_mut().take();
                }
                return Ok(Some(record));
            }
            return Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            }));
        }
        self.admin_get(key)
    }
    fn admin_fos_concurrent(&self) -> bool {
        self.fos_concurrent.get()
    }
    fn admin_fos_many(&self, requests: Vec<FosRequest<'_>>) -> Vec<Result<FosResponse, String>> {
        self.fos_rounds
            .borrow_mut()
            .push(requests.iter().map(|r| r.key.to_owned()).collect());
        let deletes = requests
            .iter()
            .filter(|r| r.method == "POST" && r.query == "delete")
            .count();
        if deletes > 0 {
            self.delete_rounds.borrow_mut().push(deletes);
        }
        if !self.fos_defer.get() {
            // A request slower than its timeout still lands, as FOS finishes it server-side,
            // but its caller sees the deadline.
            let waited = |r: &FosRequest<'_>| self.fos_ms(r).min(r.timeout.as_millis() as u64);
            let slowest = requests.iter().map(waited).max().unwrap_or(0);
            self.wall_ms.set(self.wall_ms.get() + slowest);
            self.in_round.set(true);
            let out: Vec<_> = requests
                .into_iter()
                .map(|r| {
                    let late = self.fos_ms(&r) > r.timeout.as_millis() as u64;
                    let result = self.admin_fos(r);
                    if late {
                        Err("backend_deadline".to_owned())
                    } else {
                        result
                    }
                })
                .collect();
            self.in_round.set(false);
            return out;
        }
        let mut requests = requests.into_iter();
        let mut results: Vec<_> = requests
            .next()
            .map(|r| self.admin_fos(r))
            .into_iter()
            .collect();
        for request in requests {
            if !self.fos_unread.borrow().contains_key(request.key) && self.charge().is_ok() {
                self.fos_unread
                    .borrow_mut()
                    .insert(request.key.to_owned(), request.response_limit);
            }
            results.push(Err(PREFETCH_UNREAD.to_owned()));
        }
        let outstanding = self.fos_unread.borrow().values().sum();
        self.fos_unread_peak
            .set(self.fos_unread_peak.get().max(outstanding));
        results
    }
    fn release_unread(&self) {
        self.fos_unread.borrow_mut().clear();
    }
    fn admin_unread(&self, _: &str) -> BTreeMap<String, usize> {
        self.fos_unread.borrow().clone()
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        let resumed = (request.method == "GET")
            .then(|| self.fos_unread.borrow_mut().remove(request.key))
            .flatten();
        if let Some(reserved) = resumed {
            assert!(
                request.response_limit <= reserved,
                "{} resumed at {} bytes past its {reserved} byte reservation",
                request.key,
                request.response_limit
            );
        } else {
            self.charge()?;
        }
        if !self.in_round.get() {
            self.wall_ms.set(self.wall_ms.get() + self.fos_ms(&request));
        }
        let query: BTreeMap<String, String> = url::form_urlencoded::parse(request.query.as_bytes())
            .into_owned()
            .collect();
        let ok = |status: u16, body: String| {
            Ok(FosResponse {
                cdn_cache: None,
                status,
                body: body.into_bytes(),
                retry_after: None,
            })
        };
        if let Some(id) = query.get("uploadId") {
            let mut uploads = self.uploads.borrow_mut();
            let Some((key, parts)) = uploads.get_mut(id) else {
                return ok(404, "<Error><Code>NoSuchUpload</Code></Error>".into());
            };
            assert_eq!(key, request.key);
            return match request.method {
                "PUT" => {
                    if let Some(response) = self.take_transient("PART") {
                        return Ok(response);
                    }
                    let number: u32 = query["partNumber"].parse().unwrap();
                    parts.insert(number, request.body);
                    ok(200, String::new())
                }
                "POST" => {
                    let xml = String::from_utf8(request.body).unwrap();
                    let mut body = Vec::new();
                    let mut sizes = Vec::new();
                    for part in xml.split("<PartNumber>").skip(1) {
                        let number: u32 =
                            part.split("</PartNumber>").next().unwrap().parse().unwrap();
                        let bytes = &parts[&number];
                        sizes.push(bytes.len());
                        body.extend_from_slice(bytes);
                    }
                    if sizes[..sizes.len() - 1]
                        .iter()
                        .any(|&n| n < 5 * 1024 * 1024)
                    {
                        return ok(400, "<Error><Code>EntityTooSmall</Code></Error>".into());
                    }
                    let key = key.clone();
                    uploads.remove(id);
                    self.bucket.borrow_mut().insert(key, body);
                    ok(200, "<CompleteMultipartUploadResult/>".into())
                }
                "DELETE" => {
                    uploads.remove(id);
                    ok(204, String::new())
                }
                _ => ok(405, String::new()),
            };
        }
        if request.method == "POST" && request.query == "uploads" {
            let n = self.upload_seq.get() + 1;
            self.upload_seq.set(n);
            let id = format!("up/{n}+x");
            self.uploads
                .borrow_mut()
                .insert(id.clone(), (request.key.into(), BTreeMap::new()));
            return ok(
                200,
                format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
            );
        }
        match request.method {
            "PUT" => {
                if let Some(response) = self.take_transient("PUT") {
                    return Ok(response);
                }
                let n = self.puts.get() + 1;
                self.puts.set(n);
                if self.fail_put_number.get() == Some(n) {
                    return Err("injected_put_failure".into());
                }
                self.put_md5s
                    .borrow_mut()
                    .insert(request.key.into(), request.content_md5.map(str::to_owned));
                self.bucket
                    .borrow_mut()
                    .insert(request.key.into(), request.body);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: vec![],
                    retry_after: None,
                })
            }
            "GET" if request.query.contains("list-type") => {
                if let Some(response) = self.take_transient("LIST") {
                    return Ok(response);
                }
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: self.list(request.query).into_bytes(),
                    retry_after: None,
                })
            }
            "GET" => Ok(match self.recorded_get(request.key) {
                Some(body) => FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: fastly_log_analytics::admin::transport::read_fos_body(
                        body.as_slice(),
                        200,
                        request.response_limit,
                        &mut 0,
                    )?,
                    retry_after: None,
                },
                None => FosResponse {
                    cdn_cache: None,
                    status: 404,
                    body: vec![],
                    retry_after: None,
                },
            }),
            "HEAD" => ok(
                if self.bucket.borrow().contains_key(request.key) {
                    200
                } else {
                    404
                },
                String::new(),
            ),
            "DELETE" => {
                self.bucket.borrow_mut().remove(request.key);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 204,
                    body: vec![],
                    retry_after: None,
                })
            }
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8(request.body).unwrap();
                let keys: Vec<String> = xml
                    .split("<Key>")
                    .skip(1)
                    .map(|part| part.split("</Key>").next().unwrap().to_owned())
                    .collect();
                self.delete_posts.borrow_mut().push(keys.len());
                if let Some(response) = self.take_transient("DELETE") {
                    return Ok(response);
                }
                if self.delete_status.get() != 200 {
                    return Ok(FosResponse {
                        cdn_cache: None,
                        status: self.delete_status.get(),
                        body: vec![],
                        retry_after: None,
                    });
                }
                let mut out = String::from("<DeleteResult>");
                for key in keys {
                    if self.fail_delete_keys.borrow().contains(&key) {
                        out.push_str(&format!(
                            "<Error><Key>{key}</Key><Code>InternalError</Code></Error>"
                        ));
                    } else {
                        self.bucket.borrow_mut().remove(&key);
                        out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                    }
                }
                out.push_str("</DeleteResult>");
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: out.into_bytes(),
                    retry_after: None,
                })
            }
            _ => Ok(FosResponse {
                cdn_cache: None,
                status: 405,
                body: vec![],
                retry_after: None,
            }),
        }
    }
    fn admin_cdn_read(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max: usize,
        timeout: std::time::Duration,
        range: Option<(u64, u64)>,
    ) -> Result<FosResponse, String> {
        let Some((start, end)) = range else {
            return self.admin_cdn_get(service_id, key, cdn_key, max, timeout);
        };
        self.charge()?;
        assert_eq!(service_id, SID);
        assert_eq!(cdn_key, "ck");
        let length = usize::try_from(end - start + 1).unwrap();
        assert!(length <= max);
        self.range_reads
            .borrow_mut()
            .push((key.into(), start, length));
        self.source_bytes
            .set(self.source_bytes.get() + length as u64);
        self.cdn_gets.borrow_mut().push(key.into());
        let bucket = self.bucket.borrow();
        let body = bucket
            .get(key)
            .and_then(|bytes| bytes.get(start as usize..=end as usize))
            .map(<[u8]>::to_vec);
        Ok(FosResponse {
            status: if body.is_some() { 206 } else { 404 },
            body: body.unwrap_or_default(),
            cdn_cache: None,
            retry_after: None,
        })
    }

    fn admin_cdn_get(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max: usize,
        _timeout: std::time::Duration,
    ) -> Result<FosResponse, String> {
        self.charge()?;
        assert_eq!(cdn_key, "ck");
        assert_eq!(service_id, SID);
        self.cdn_gets.borrow_mut().push(key.into());
        if let Some(response) = self.take_transient("GET") {
            return Ok(response);
        }
        self.spend_get_vcpu(key);
        // As the SDK's CDN read does once a body passes the response limit.
        if self.bucket.borrow().get(key).is_some_and(|b| b.len() > max) {
            return Err("cdn_body_too_large".into());
        }
        self.source_bytes.set(
            self.source_bytes.get()
                + self
                    .bucket
                    .borrow()
                    .get(key)
                    .map_or(0, |bytes| bytes.len() as u64),
        );
        Ok(match self.bucket.borrow().get(key) {
            Some(body) => FosResponse {
                cdn_cache: None,
                status: 200,
                body: body.clone(),
                retry_after: None,
            },
            None => FosResponse {
                cdn_cache: None,
                status: 404,
                body: vec![],
                retry_after: None,
            },
        })
    }
    fn admin_api(
        &self,
        request: fastly_log_analytics::admin::ApiRequest<'_>,
    ) -> Result<fastly_log_analytics::admin::ApiResponse, String> {
        self.charge()?;
        self.api_calls
            .borrow_mut()
            .push((request.method.into(), request.path.into()));
        self.api_queries
            .borrow_mut()
            .push((request.path.into(), request.query.into()));
        let (status, body) = self
            .api_responses
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| (self.api_status.get(), json!({"status": "ok"})));
        Ok(fastly_log_analytics::admin::ApiResponse {
            status,
            body,
            retry_after: None,
        })
    }
    fn admin_remaining_calls(&self) -> u32 {
        BUDGET - self.calls.get()
    }
    fn execution_backend_calls(&self) -> Option<u32> {
        Some(self.calls.get())
    }
    fn execution_vcpu_ms(&self) -> Option<u64> {
        Some(self.vcpu_ms())
    }
    fn admin_elapsed(&self, started: Instant) -> std::time::Duration {
        let base = std::time::Duration::from_secs(self.elapsed_s.get());
        if self.fos_wall.get().is_none() {
            return base;
        }
        base + started.elapsed() + std::time::Duration::from_millis(self.wall_ms.get())
    }
    fn admin_wait(&self, millis: u64) {
        self.waits.borrow_mut().push(millis);
    }
    fn heap_mib(&self) -> Option<u32> {
        self.heap_mib.get()
    }
    fn linear_memory_high_water_bytes(&self) -> Option<u64> {
        self.linear_memory_high_water_bytes.get()
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.set(self.vcpu.get() + self.vcpu_step.get());
        self.vcpu.get()
    }
    fn pop(&self) -> Option<String> {
        Some("IAD".into())
    }
    fn on_log_shield(&self) -> bool {
        self.shield.get()
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn logged_traffic(&self, _: &str, _: u64, _: u64) -> Result<Value, String> {
        Ok(json!({"days": []}))
    }
    fn service_stats(
        &self,
        service_id: &str,
        from: u64,
        to: u64,
        fields: &[&str],
    ) -> Result<Value, String> {
        if service_id != SID {
            return Err("unknown_service".into());
        }
        let value = |field: &str| match field {
            "requests" => 1000,
            "resp_body_bytes" => 9000,
            "resp_header_bytes" => 1000,
            "hits" => 900,
            "edge_requests" => 600,
            "edge_resp_body_bytes" => 5000,
            "edge_resp_header_bytes" => 500,
            "edge_hit_requests" => 540,
            _ => 1,
        };
        let rows = (from..to)
            .step_by(3600)
            .map(|start| {
                let mut row = json!({"start_time": start});
                for field in fields {
                    row[*field] = json!(value(field));
                }
                row
            })
            .collect::<Vec<_>>();
        Ok(json!(rows))
    }
    fn core_cache_replace(
        &self,
        key: &str,
        _surrogate_keys: &[&str],
        _ttl: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        self.cache_op()?;
        let existing = self.cache.borrow().get(key).cloned();
        self.cache.borrow_mut().insert(key.into(), update(existing));
        self.cache_tags.borrow_mut().insert(
            key.into(),
            _surrogate_keys.iter().map(|k| (*k).to_owned()).collect(),
        );
        Ok(())
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        self.cache_op()?;
        Ok(self.cache.borrow().get(key).cloned())
    }
    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.cache_op()?;
        let tagged: Vec<String> = self
            .cache_tags
            .borrow()
            .iter()
            .filter(|(_, tags)| tags.iter().any(|t| t == surrogate))
            .map(|(key, _)| key.clone())
            .collect();
        for key in tagged {
            self.cache.borrow_mut().remove(&key);
            self.cache_tags.borrow_mut().remove(&key);
        }
        Ok(())
    }
    fn worker_requests(&self) -> u32 {
        self.worker_sent.get()
    }
    fn admin_event(&self, event: Value) {
        self.events.borrow_mut().push(event);
    }
    fn cache_ops(&self) -> u32 {
        self.cache_ops.get()
    }
    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        self.charge()?;
        self.kv_reads.borrow_mut().push(format!("list:{prefix}"));
        let kv = self.kv.borrow();
        Ok(KvPage::from_sorted_keys(
            kv.keys().cloned(),
            prefix,
            cursor,
            limit,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.charge()?;
        self.kv.borrow_mut().remove(key);
        Ok(())
    }
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let bad = self
            .bad_etag_for
            .borrow()
            .as_deref()
            .is_some_and(|pattern| request.key.contains(pattern));
        let tag = if bad {
            "\"00000000000000000000000000000000\"".to_owned()
        } else {
            format!("\"{}\"", hex::encode(Md5::digest(&request.body)))
        };
        self.admin_fos(request)
            .map(|response| (response, Some(tag)))
    }
    fn worker_dispatch(
        &self,
        requests: &[(String, Value)],
        _deadline: std::time::Duration,
    ) -> Vec<Result<Value, String>> {
        let mut out = Vec::new();
        for (path, body) in requests {
            if let Err(error) = self.charge() {
                out.push(Err(error));
                continue;
            }
            self.sent_to_worker();
            if path == fastly_log_analytics::admin::stage::PATH {
                let request = fastly_log_analytics::http::Request {
                    method: "POST".into(),
                    path: path.clone(),
                    headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                    body: body.to_string().into_bytes(),
                };
                let response = self.execution("stage", || {
                    fastly_log_analytics::admin::stage::handle_worker_stage(self, &request)
                });
                let reply: Result<Value, String> = if response.status == 200 {
                    serde_json::from_slice(&response.body).map_err(|e| e.to_string())
                } else {
                    Err(format!("worker_http_{}", response.status))
                };
                if let Ok(reply) = &reply {
                    self.stage_runs.borrow_mut().push(reply.clone());
                }
                if let Some(next) = self.race_maint_next.take() {
                    let mut kv = self.kv.borrow_mut();
                    let (generation, mut state) = kv["cron:state"].clone();
                    state["maint_next"] = next.into();
                    kv.insert("cron:state".into(), (generation + 1, state));
                }
                out.push(reply);
                continue;
            }
            if path != ingest::worker::PATH {
                out.push(Ok(json!({"path": path, "echo": body})));
                continue;
            }
            let fault = self
                .worker_faults
                .borrow_mut()
                .pop_front()
                .unwrap_or(WorkerFault::Ok);
            if fault == WorkerFault::Unreachable {
                out.push(Err("worker_deadline".into()));
                continue;
            }
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: path.clone(),
                headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                body: body.to_string().into_bytes(),
            };
            // A fresh execution: its own 32-call budget and Secret Store reads.
            let kind = if body["maintenance"] == "request_compact" {
                "request_compact_worker"
            } else if body["maintenance"] == "source_index" {
                "source_index_worker"
            } else {
                "ingest_worker"
            };
            let (response, used) = self.execution(kind, || {
                let response = ingest::worker::handle_worker_ingest(self, &request);
                (response, self.calls.get())
            });
            assert!(used <= BUDGET, "worker used {used} calls");
            self.worker_max_calls
                .set(self.worker_max_calls.get().max(used));
            self.worker_runs.set(self.worker_runs.get() + 1);
            if fault == WorkerFault::LostReply {
                out.push(Err("worker_deadline".into()));
            } else if response.status != 200 {
                out.push(Err(format!("worker_http_{}", response.status)));
            } else {
                out.push(serde_json::from_slice(&response.body).map_err(|e| e.to_string()));
            }
        }
        out
    }
    fn worker_delete(&self, body: &Value) -> Result<Value, String> {
        self.charge()?;
        self.sent_to_worker();
        self.worker_calls
            .borrow_mut()
            .push(body["keys"].as_array().map_or(0, Vec::len));
        let request = fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/_internal/delete".into(),
            headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
            body: body.to_string().into_bytes(),
        };
        let spent = self.calls.replace(0);
        let response = ingest::retention::handle_worker_delete(self, &request);
        self.calls.set(spent);
        if response.status != 200 {
            return Err(format!("worker_http_{}", response.status));
        }
        serde_json::from_slice(&response.body).map_err(|e| e.to_string())
    }
}

impl Fake {
    pub(super) fn list(&self, query: &str) -> String {
        LISTS.with(|n| n.set(n.get() + 1));
        let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(q.get("list-type").map(String::as_str), Some("2"));
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let max: usize = q
            .get("max-keys")
            .and_then(|m| m.parse().ok())
            .unwrap_or(1000);
        let max = self.max_list_keys.get().map_or(max, |cap| max.min(cap));
        let after = q
            .get("continuation-token")
            .or_else(|| q.get("start-after"))
            .cloned();
        let delimiter = q.get("delimiter").cloned();
        let mut contents = String::new();
        let mut prefixes = BTreeSet::new();
        let (mut count, mut truncated, mut last) = (0, false, String::new());
        for (key, body) in self.bucket.borrow().iter() {
            if !key.starts_with(&prefix) || after.as_ref().is_some_and(|a| key <= a) {
                continue;
            }
            if let Some(d) = &delimiter {
                if let Some(i) = key[prefix.len()..].find(d.as_str()) {
                    prefixes.insert(key[..prefix.len() + i + d.len()].to_owned());
                    continue;
                }
            }
            if count == max {
                truncated = true;
                break;
            }
            count += 1;
            last = key.clone();
            let modified = self.mtimes.borrow().get(key).map_or_else(
                || "2027-01-15T08:00:00.000Z".to_owned(),
                |t| ingest::rfc3339(*t),
            );
            contents.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>{modified}</LastModified></Contents>",
                body.len()
            ));
        }
        let prefixes: String = prefixes
            .iter()
            .map(|p| format!("<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>"))
            .collect();
        let token = if truncated {
            format!("<NextContinuationToken>{last}</NextContinuationToken>")
        } else {
            String::new()
        };
        format!(
            "<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>{truncated}</IsTruncated>{contents}{prefixes}{token}</ListBucketResult>"
        )
    }
}

pub(super) fn service() -> Service {
    serde_json::from_value(json!({
        "service_id": SID, "name": "n", "status": "active", "created_at": 0, "updated_at": 0,
        "config_version": 1, "logging_enabled": true, "logging_endpoint_name": "e",
        "log_period": 60, "sample_rate": 1, "edge_only": false, "custom_condition": "",
        "fos_bucket": "fos-abcdefghijklmnopqrst12-logs", "fos_prefix": "", "fos_region": "us-east-1",
        "fos_endpoint": "x", "log_fields": [], "field_limits": {}, "fastly_token_ref": "t",
        "fos_access_key_ref": "ak", "fos_secret_key_ref": "sk", "owner": "o",
        "bucket_adopted": false, "bucket_checked": false, "last_activated_version": null,
        "last_reconcile": null, "pending_job_id": null
    }))
    .unwrap()
}

pub(super) fn iso(seconds: i64) -> String {
    ingest::rfc3339(seconds).replace("Z", "+0000")
}

pub(super) fn raw_key(minute_start: i64, name: &str) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(minute_start).unwrap();
    format!(
        "raw/request/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/{name}.zst",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute()
    )
}

pub(super) fn gz(lines: &[String]) -> Vec<u8> {
    let mut text = Vec::new();
    text.write_all(lines.join("\n").as_bytes()).unwrap();
    text.write_all(b"\n").unwrap();
    zstd::stream::encode_all(text.as_slice(), 1).unwrap()
}

pub(super) fn line(at: i64, id: &str) -> String {
    json!({"timestamp": iso(at), "ip": id, "status": 200}).to_string()
}

pub(super) fn put_raw(fake: &Fake, minute_start: i64, name: &str, lines: &[String]) -> String {
    let key = raw_key(minute_start, name);
    fake.bucket.borrow_mut().insert(key.clone(), gz(lines));
    key
}

pub(super) fn tick(fake: &Fake) -> Result<ingest::Report, String> {
    tick_with_service(fake, &service())
}

pub(super) fn tick_with_service(fake: &Fake, service: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run(fake, service, "ak", "sk", Instant::now(), &buffer);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn rows(fake: &Fake, kind: LogKind) -> Vec<Value> {
    fake.new_request();
    telemetry::read_live(fake, SID, kind, None, Some(2000), None)
        .unwrap()
        .rows
}

/// Cron rows wherever they are: flushed to ops files or still in the live outbox. A flush moves
/// rows out of the live read, and closed outbox buckets flush within the interval floor.
pub(super) fn cron_runs(fake: &Fake) -> Vec<Value> {
    let flushed: Vec<Vec<u8>> = fake
        .bucket
        .borrow()
        .iter()
        .filter(|(key, _)| key.contains("processed/ops/cron_runs/"))
        .map(|(_, bytes)| bytes.clone())
        .collect();
    let mut out = Vec::new();
    for batch in flushed.iter().flat_map(|bytes| batches(bytes)) {
        let text = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (kind, outcome) = (text("cron_kind"), text("outcome"));
        let deleted = batch
            .column_by_name("deleted_files")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        out.extend((0..batch.num_rows()).map(|i| {
            json!({
                "cron_kind": kind.value(i),
                "outcome": outcome.value(i),
                "deleted_files": (!deleted.is_null(i)).then(|| deleted.value(i)),
            })
        }));
    }
    out.extend(rows(fake, LogKind::CronRuns));
    out
}

pub(super) fn batches(bytes: &[u8]) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

pub(super) fn processed_ips(fake: &Fake) -> Vec<String> {
    let mut ips = Vec::new();
    let keys = fake.keys("processed/request/");
    for key in fastly_log_analytics::telemetry::compact::visible(&keys, &[]) {
        for batch in batches(&fake.bucket.borrow()[&key]) {
            let col = batch
                .column_by_name("ip")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            ips.extend((0..col.len()).map(|i| col.value(i).to_owned()));
        }
    }
    ips.sort();
    ips
}

pub(super) fn ms_column(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i)))
        .collect()
}

pub(super) fn backfill(fake: &Fake, to_key: &str) -> ingest::Report {
    let job = ingest::BackfillJob {
        status: "running".into(),
        actor: "admin".into(),
        from_ts: None,
        to_ts: None,
        after_key: String::new(),
        to_key: to_key.into(),
        scanned: 0,
        planned: 0,
        started_at: fake.now.get(),
        updated_at: fake.now.get(),
    };
    fake.kv.borrow_mut().insert(
        ingest::BACKFILL_KEY.into(),
        (1, serde_json::to_value(job).unwrap()),
    );
    fake.new_request();
    let buffer = LogBuffer::new(SID, "backfill");
    let mut options = ingest::coordinator::Options::request(fake);
    options.allow_backfill = true;
    ingest::coordinator::coordinate(
        fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap()
}

pub(super) fn tick_service(fake: &Fake, service: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run(fake, service, "ak", "sk", Instant::now(), &buffer);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn sweep_tick(fake: &Fake) -> ingest::Report {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let mut opts = ingest::coordinator::Options::request(fake);
    opts.sweep = true;
    let report = ingest::coordinator::coordinate(
        fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &opts,
    )
    .unwrap();
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn raw_floor(stream: &str, minute_start: i64) -> String {
    format!("raw/{stream}/{}", ingest::minute_dir(minute_start))
}

pub(super) fn set_floor(fake: &Fake, watermark_key: &str, floor: &str) {
    let mut kv = fake.kv.borrow_mut();
    let (generation, mut value) = kv.get(watermark_key).cloned().unwrap_or((0, json!({})));
    value[ingest::FLOOR_FIELD] = floor.into();
    kv.insert(watermark_key.into(), (generation + 1, value));
}

pub(super) fn floor_of(fake: &Fake, watermark_key: &str) -> Option<String> {
    fake.kv.borrow()[watermark_key].1[ingest::FLOOR_FIELD]
        .as_str()
        .map(str::to_owned)
}

pub(super) fn queued_keys(fake: &Fake) -> Vec<String> {
    let mut keys: Vec<String> = deleteq(fake)
        .iter()
        .flat_map(|queue| queue["keys"].as_array().cloned().unwrap_or_default())
        .filter_map(|key| key.as_str().map(str::to_owned))
        .collect();
    keys.sort();
    keys
}

pub(super) fn deleteq(fake: &Fake) -> Vec<Value> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with("ingest:deleteq:"))
        .map(|(_, (_, v))| v.clone())
        .collect()
}

pub(super) fn tick_runtime(fake: &Fake) {
    let sid = SID;
    fake.kv.borrow_mut().insert(
        "admin:install".into(),
        (1, json!({"service_id": sid, "job_id": null, "fence": 0})),
    );
    fake.kv.borrow_mut().insert(
        format!("svc:{sid}"),
        (1, serde_json::to_value(service()).unwrap()),
    );
}

pub(super) fn run_tick(fake: &Fake) -> Value {
    fake.new_request();
    let value = fastly_log_analytics::admin::record_tick(fake, "clock", "clock").unwrap();
    fake.note_peak("tick");
    value
}

/// The front service's `/_flush` probe: one flp execution running only the log flush.
pub(super) fn run_flush(fake: &Fake) -> Value {
    fake.new_request();
    let value = fastly_log_analytics::admin::record_flush(fake).unwrap();
    fake.note_peak("flush");
    value
}

pub(super) fn ingest_runs(fake: &Fake) -> usize {
    rows(fake, LogKind::CronRuns)
        .iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .count()
}

pub(super) fn outbox_keys(fake: &Fake) -> Vec<String> {
    fake.kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with(&format!("logq_{SID}_")))
        .cloned()
        .collect()
}

pub(super) fn seed_files(fake: &Fake, count: i64) {
    for i in 0..count {
        let start = BASE + i * 60;
        let key = ingest::processed_key(
            "processed/request/",
            start,
            start + 60,
            "abcdef012345",
            i as u64 + 1,
        );
        fake.bucket.borrow_mut().insert(key, vec![1, 2, 3]);
    }
    let keys = fake.keys("processed/request/");
    for batch in keys.chunks(128) {
        publish_files(fake, batch, &[], &[]);
    }
}

pub(super) fn files_page(fake: &Fake, query: &str) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    let page = api::files(fake, &service(), query, Instant::now());
    fake.release_unread();
    page
}

pub(super) fn publish_files(
    fake: &Fake,
    keys: &[String],
    summaries: &[String],
    replaces: &[String],
) -> Value {
    let outputs = |keys: &[String]| {
        keys.iter()
            .map(|key| ingest::worker::Output {
                key: key.clone(),
                rows: ingest::parse_processed(key)
                    .and_then(|name| name.rows)
                    .or_else(|| telemetry::compact::parse_compact_name(key).map(|name| name.rows))
                    .unwrap_or(1),
                bytes: fake
                    .bucket
                    .borrow()
                    .get(key)
                    .map_or(0, |bytes| bytes.len() as u64),
                ..Default::default()
            })
            .collect::<Vec<_>>()
    };
    publish_outputs(fake, &outputs(keys), &outputs(summaries), replaces)
}

pub(super) fn publish_outputs(
    fake: &Fake,
    files: &[ingest::worker::Output],
    summaries: &[ingest::worker::Output],
    replaces: &[String],
) -> Value {
    let step = fake.vcpu_step.replace(0);
    fake.vcpu.set(0);
    for _ in 0..100 {
        fake.new_request();
        let record = fake.admin_get(ingest::WATERMARK_KEY).unwrap();
        let previous = record
            .as_ref()
            .map(|r| r.value.clone())
            .unwrap_or_else(|| json!({}));
        match ingest::catalog::prepare(
            &store(fake, &service()),
            &previous,
            "fixture-commit",
            "schema",
            files,
            summaries,
            &[],
            replaces,
        ) {
            Ok(value) => {
                assert!(fake
                    .admin_cas(
                        ingest::WATERMARK_KEY,
                        record.as_ref().map(|r| r.generation.as_str()),
                        &value
                    )
                    .unwrap());
                fake.vcpu_step.set(step);
                fake.vcpu.set(0);
                fake.new_request();
                return value;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("fixture publication failed: {error}"),
        }
    }
    panic!("fixture catalog publication did not make progress");
}

pub(super) fn catalog_keys(fake: &Fake, query: &str) -> (u64, BTreeSet<String>) {
    let mut next = None::<String>;
    let mut keys = BTreeSet::new();
    let mut generation = None;
    for _ in 0..200 {
        let query = next.as_ref().map_or_else(
            || query.to_owned(),
            |cursor| format!("{query}&cursor={cursor}"),
        );
        let page = files_page(fake, &query).unwrap();
        let current = page["catalog_generation"].as_u64().unwrap();
        assert_eq!(*generation.get_or_insert(current), current);
        for file in page["files"].as_array().unwrap() {
            assert!(keys.insert(file["key"].as_str().unwrap().to_owned()));
        }
        next = page["next_cursor"].as_str().map(str::to_owned);
        if next.is_none() {
            assert_eq!(page["complete"], true);
            return (current, keys);
        }
    }
    panic!("catalog pagination did not make bounded progress");
}

/// Twelve committed hours, then an open hour still taking a two-minute window per commit;
/// returns the files query and every published file and summary key.
pub(super) fn publish_active_day(fake: &Fake) -> (String, BTreeSet<String>, BTreeSet<String>) {
    use fastly_log_analytics::ingest::summary;
    let open = BASE - BASE.rem_euclid(3600);
    let mut want_files = BTreeSet::new();
    let mut want_summaries = BTreeSet::new();
    // Twelve committed hours, then an open hour still taking a two-minute window per commit.
    for hour in (0..13).map(|h| open - (12 - h) * 3600) {
        let windows = if hour == open { 30 } else { 4 };
        let (mut hour_files, mut hour_sums) = (Vec::new(), Vec::new());
        for w in 0..windows {
            let start = hour + w * 120;
            let file = ingest::processed_key("processed/request/", start, start + 120, "abc", 3);
            let sum =
                summary::summary_key("processed/summary/request/", start, start + 120, "p1s0", 3);
            fake.bucket.borrow_mut().insert(file.clone(), vec![1]);
            fake.bucket.borrow_mut().insert(sum.clone(), vec![1]);
            want_files.insert(file.clone());
            want_summaries.insert(sum.clone());
            if hour == open {
                publish_files(fake, &[file], &[sum], &[]);
            } else {
                hour_files.push(file);
                hour_sums.push(sum);
            }
        }
        if hour != open {
            publish_files(fake, &hour_files, &hour_sums, &[]);
        }
    }
    let query = format!(
        "summaries=1&from={}&to={}",
        iso_url(open - 12 * 3600),
        iso_url(open + 3600)
    );
    (query, want_files, want_summaries)
}

/// Every page of `query`'s file walk: the files, summary files and page count.
pub(super) fn walk_files(fake: &Fake, query: &str) -> (BTreeSet<String>, BTreeSet<String>, usize) {
    let (mut files, mut summaries, mut next) = (BTreeSet::new(), BTreeSet::new(), None::<String>);
    let mut pages = 0;
    fake.fos_rounds.borrow_mut().clear();
    loop {
        let query = next
            .as_ref()
            .map_or_else(|| query.to_owned(), |c| format!("{query}&cursor={c}"));
        let page = files_page(fake, &query).unwrap();
        pages += 1;
        for (field, out) in [("files", &mut files), ("summary_files", &mut summaries)] {
            for file in page[field].as_array().unwrap() {
                assert!(out.insert(file["key"].as_str().unwrap().to_owned()));
            }
        }
        next = page["next_cursor"].as_str().map(str::to_owned);
        if next.is_none() {
            return (files, summaries, pages);
        }
    }
}

/// A walk of `query` with `fork=1`, following every fork's cursor chain: the files, summary
/// files and page kinds, failing on a key two forks both return.
pub(super) struct Forked {
    pub(super) files: BTreeSet<String>,
    pub(super) summaries: BTreeSet<String>,
    pub(super) forks: usize,
    /// Backend calls the setup page made.
    pub(super) setup_calls: u32,
    /// Concurrent object-read rounds the setup page made.
    pub(super) setup_rounds: usize,
    /// Pages of the longest fork chain, the walk's critical path after setup.
    pub(super) longest: usize,
    /// Object-read rounds of the slowest fork chain.
    pub(super) longest_rounds: usize,
}

pub(super) fn walk_forked(fake: &Fake, query: &str) -> Forked {
    fake.fos_rounds.borrow_mut().clear();
    let setup = files_page(fake, &format!("{query}&fork=1")).unwrap();
    let setup_calls = fake.calls.get();
    let setup_rounds = fake.fos_rounds.borrow().len();
    let generation = setup["catalog_generation"].clone();
    let forks: Vec<String> = setup["forks"]
        .as_array()
        .expect("a forked listing")
        .iter()
        .map(|c| c.as_str().unwrap().to_owned())
        .collect();
    assert!(setup["next_cursor"].is_null());
    assert_eq!(setup["complete"], false);
    let (mut files, mut summaries, mut longest) = (BTreeSet::new(), BTreeSet::new(), 0);
    for (field, out) in [("files", &mut files), ("summary_files", &mut summaries)] {
        for file in setup[field].as_array().unwrap() {
            assert!(out.insert(file["key"].as_str().unwrap().to_owned()));
        }
    }
    let mut longest_rounds = 0;
    for fork in &forks {
        let mut cursor = fork.clone();
        let mut pages = 0;
        fake.fos_rounds.borrow_mut().clear();
        loop {
            let page = files_page(fake, &format!("{query}&cursor={cursor}")).unwrap();
            pages += 1;
            assert_eq!(page["catalog_generation"], generation);
            assert!(page.get("forks").is_none_or(Value::is_null));
            for (field, out) in [("files", &mut files), ("summary_files", &mut summaries)] {
                for file in page[field].as_array().unwrap() {
                    let key = file["key"].as_str().unwrap().to_owned();
                    assert!(out.insert(key.clone()), "{key} listed twice");
                }
            }
            match page["next_cursor"].as_str() {
                Some(next) => cursor = next.to_owned(),
                None => {
                    assert_eq!(page["complete"], true);
                    break;
                }
            }
        }
        longest = longest.max(pages);
        longest_rounds = longest_rounds.max(fake.fos_rounds.borrow().len());
    }
    Forked {
        files,
        summaries,
        forks: forks.len(),
        setup_calls,
        setup_rounds,
        longest,
        longest_rounds,
    }
}

pub(super) fn iso_url(seconds: i64) -> String {
    ingest::rfc3339(seconds).replace(':', "%3A")
}

thread_local! {
    /// LIST requests the fake bucket answered on this test's thread.
    static LISTS: Cell<u32> = const { Cell::new(0) };
}

pub(super) fn lists() -> u32 {
    LISTS.with(Cell::get)
}

/// A request `compact=` key in `start`'s hour folder covering `sources`.
pub(super) fn request_compact_key(sources: &[String]) -> String {
    let names: Vec<_> = sources
        .iter()
        .map(|k| ingest::parse_processed(k).unwrap())
        .collect();
    let a = names.iter().map(|n| n.start).min().unwrap();
    let b = names.iter().map(|n| n.end).max().unwrap();
    let rows = names.iter().map(|n| n.rows.unwrap()).sum();
    let prefix = format!("processed/request/{}", ingest::hour_dir(a));
    fastly_log_analytics::telemetry::compact::compact_key(
        &prefix, a as u64, b as u64, sources, rows,
    )
}

pub(super) fn request_windows(fake: &Fake) -> Vec<String> {
    fake.keys("processed/request/")
        .into_iter()
        .filter(|k| k.contains("/window="))
        .collect()
}

pub(super) fn admin_page(
    fake: &Fake,
    raw: Option<bool>,
    query: &str,
) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    match raw {
        Some(raw) => api::tree(fake, &service(), query, Instant::now(), raw),
        None => api::ingested_files(fake, &service(), query, Instant::now()),
    }
}

pub(super) fn cursor_query(cursor: &str) -> String {
    format!("cursor={cursor}")
}

pub(super) fn store<'a>(fake: &'a Fake, service: &'a Service) -> ingest::storage::Store<'a, Fake> {
    ingest::storage::Store {
        runtime: fake,
        service,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    }
}

pub(super) fn delete_tick(fake: &Fake, service: &Service) -> ingest::retention::DeleteReport {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "del");
    let report =
        ingest::retention::raw_delete(&store(fake, service), &buffer, 4, &Default::default())
            .unwrap();
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn converted_fixture(fake: &Fake, count: i64) -> Vec<String> {
    let keys = (0..count)
        .map(|m| put_raw(fake, BASE + m * 60, "a", &[line(BASE + m * 60, "a")]))
        .collect();
    tick(fake).unwrap();
    keys
}

pub(super) fn quarantine_records(fake: &Fake) -> Vec<Value> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::QUARANTINE_PREFIX))
        .map(|(_, (_, v))| v.clone())
        .collect()
}

/// One committed plan with a corrupt object and a good one; returns (bad, good).
pub(super) fn quarantined_fixture(fake: &Fake) -> (String, String) {
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"not gzip".to_vec());
    let good = put_raw(fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    tick(fake).unwrap();
    (bad, good)
}

pub(super) fn seed_quarantine(fake: &Fake, run: &str, keys: &[String]) {
    let objects: Vec<Value> = keys
        .iter()
        .map(|key| {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1, 2, 3]);
            json!({"key": key, "size": 3, "status": "quarantined", "reason": "corrupt_gzip",
                "attempts": 2})
        })
        .collect();
    fake.kv.borrow_mut().insert(
        format!("{}{run}", ingest::QUARANTINE_PREFIX),
        (
            1,
            json!({"run_id": run, "objects": objects, "first_seen": BASE, "at": BASE + 10}),
        ),
    );
}

pub(super) fn set_watermark(fake: &Fake, key: &str) {
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (1, json!({"key": key, "run_id": "seeded", "at": BASE})),
    );
}

pub(super) fn quarantine_page(fake: &Fake, query: &str) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    api::quarantine(fake, &service(), query)
}

pub(super) fn purge_as(
    fake: &Fake,
    actor: &str,
    body: Value,
) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "purge");
    let result = api::quarantine_purge(
        fake,
        &service(),
        actor,
        body.to_string().as_bytes(),
        &buffer,
        Instant::now(),
    );
    assert!(
        fake.calls.get() + api::PURGE_RESERVE <= BUDGET,
        "a purge step used {} calls",
        fake.calls.get()
    );
    telemetry::flush_request_buffer(fake, &buffer);
    result.map(|purge| purge.body)
}

pub(super) fn purge(fake: &Fake, mut body: Value) -> Result<Value, (u16, &'static str)> {
    let issued = purge_as(fake, "u-admin", body.clone())?;
    assert_eq!(issued["status"], "confirm_required");
    body["confirm"] = issued["confirm_token"].clone();
    purge_as(fake, "u-admin", body)
}

pub(super) const DAY: i64 = 86_400;

pub(super) fn put_processed(fake: &Fake, prefix: &str, hour_start: i64, name: &str) -> String {
    let key = format!(
        "{prefix}{}window={name}.parquet",
        ingest::hour_dir(hour_start)
    );
    fake.bucket.borrow_mut().insert(key.clone(), vec![0; 10]);
    key
}

pub(super) fn sweep(
    fake: &Fake,
    service: &Service,
    settings: &telemetry::OperationalLogSettings,
) -> ingest::retention::SweepReport {
    fake.new_request();
    ingest::retention::retention_sweep(&store(fake, service), settings, 4, None).unwrap()
}

pub(super) fn sweep_request(fake: &Fake) -> ingest::retention::SweepReport {
    fake.new_request();
    let svc = service();
    ingest::retention::retention_sweep(&store(fake, &svc), &Default::default(), 4, Some("request"))
        .unwrap()
}

pub(super) fn ops_parquet(fake: &Fake, kind: LogKind, hour_start: i64, rows: &[Value]) -> String {
    let key = format!(
        "processed/ops/{}/{}window=1-2-abc-n{}.parquet",
        kind.as_str(),
        ingest::hour_dir(hour_start),
        rows.len()
    );
    let bytes = telemetry::schema::to_parquet(kind, rows).unwrap();
    fake.bucket.borrow_mut().insert(key.clone(), bytes);
    key
}

pub(super) fn files_for(fake: &Fake, dataset: &str) -> Result<Value, (u16, &'static str)> {
    files_page(
        fake,
        &format!(
            "from={}&to={}&dataset={dataset}",
            iso_url(BASE),
            iso_url(BASE + 3600)
        ),
    )
}

pub(super) fn reconcile(fake: &Fake) -> Option<ingest::retention::ReconcileReport> {
    fake.new_request();
    ingest::retention::storage_reconcile_step(&store(fake, &service()), 0).unwrap()
}

pub(super) fn patch_settings(
    fake: &Fake,
    patch: Value,
) -> Result<(telemetry::OperationalLogSettings, Vec<String>), String> {
    fake.new_request();
    telemetry::api::patch_operational_settings(fake, &patch)
}

pub(super) fn logs_call(
    fake: &Fake,
    method: &str,
    path: &str,
    body: Value,
) -> (u16, Value, Option<(String, String)>) {
    fake.new_request();
    let request = fastly_log_analytics::http::Request {
        method: method.into(),
        path: path.into(),
        headers: Default::default(),
        body: if body.is_null() {
            vec![]
        } else {
            body.to_string().into_bytes()
        },
    };
    let outcome = fastly_log_analytics::admin::logs::handle(fake, "admin-1", &request);
    let body = serde_json::from_slice(&outcome.response.body).unwrap_or(Value::Null);
    (outcome.response.status, body, outcome.audit)
}

/// 2026-07-23T10:00:00Z
pub(super) const STATS_HOUR: u64 = 1_784_800_800;

pub(super) fn history_service(fake: &Fake) {
    tick_runtime(fake);
}

pub(super) fn plan(fake: &Fake) -> Option<Value> {
    fake.kv
        .borrow()
        .get(ingest::PLAN_KEY)
        .map(|(_, value)| value.clone())
}

pub(super) fn committed(fake: &Fake) -> bool {
    let kv = fake.kv.borrow();
    let watermark = kv
        .get(ingest::WATERMARK_KEY)
        .map(|(_, v)| v["run_id"].clone());
    kv.get(ingest::PLAN_KEY)
        .is_none_or(|(_, p)| watermark.as_ref() == Some(&p["plan_id"]))
}

/// Ticks two minutes apart (the clock bucket), so a lease left by the previous tick has expired.
pub(super) fn tick_later(fake: &Fake) -> Result<ingest::Report, String> {
    fake.now.set(fake.now.get() + 120);
    tick(fake)
}

pub(super) fn seed(fake: &Fake, minutes: i64, per_minute: i64) -> Vec<String> {
    let mut expected = Vec::new();
    for m in 0..minutes {
        let minute = BASE + m * 60;
        for i in 0..per_minute {
            let id = format!("m{m:03}o{i:03}");
            put_raw(
                fake,
                minute,
                &format!("o{i:03}"),
                &[line(minute + i % 60, &id)],
            );
            expected.push(id);
        }
    }
    expected.sort();
    expected
}

pub(super) fn worker_call(fake: &Fake, keys: &[String], stop_ms: u64) -> Value {
    fake.new_request();
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": keys, "stop_ms": stop_ms, "created_at": BASE,
    });
    let response = ingest::worker::handle_worker_ingest(
        fake,
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: ingest::worker::PATH.into(),
            headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
            body: body.to_string().into_bytes(),
        },
    );
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

pub(super) fn custom_service() -> Service {
    let mut svc = service();
    let mut fields = json!({"groups": ["A"], "field_overrides": {}, "custom_fields": [
        {"name": "tenant", "vcl": "req.http.x-tenant", "parquet_type": "Utf8",
         "enabled": true, "committed": true},
        {"name": "score", "vcl": "req.http.x-score", "parquet_type": "Int64",
         "enabled": true, "committed": false},
        {"name": "old_flag", "vcl": "req.http.x-old", "parquet_type": "Boolean",
         "enabled": false, "committed": true},
        {"name": "draft", "vcl": "req.http.x-draft", "parquet_type": "Utf8",
         "enabled": false, "committed": false},
    ]});
    let cmcd = fastly_log_analytics::logging::cmcd::Config {
        enabled: true,
        ..Default::default()
    };
    fastly_log_analytics::logging::cmcd::reconcile_fields(&mut fields, &cmcd, None, false).unwrap();
    svc.log_fields = fields;
    svc.cmcd = cmcd;
    svc
}

pub(super) fn store_service(fake: &Fake, svc: &Service) {
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
}

pub(super) fn rum_key(minute_start: i64, name: &str) -> String {
    raw_key(minute_start, name).replacen("raw/request/", "raw/rum/", 1)
}

pub(super) fn put_rum(fake: &Fake, minute_start: i64, name: &str, lines: &[String]) -> String {
    let key = rum_key(minute_start, name);
    fake.bucket.borrow_mut().insert(key.clone(), gz(lines));
    key
}

pub(super) fn rum_beacon(at: i64, metric: &str, value: &str) -> String {
    json!({
        "timestamp": iso(at), "ts_ms": at * 1000 + 250, "method": "GET",
        "url": format!("/rum?rum_metric_name={metric}&rum_metric_value={value}&rum_metric_rating=good&cid=c1&rum_pathname=/home"),
        "referer": "https://ex.com/r", "city": "Paris", "country": "FR", "pop": "CDG",
        "tls": "1.3", "ttfb": 0.012, "fastly_req_id": "r1", "service_id": "",
        "rum_metric_name": "", "rum_metric_value": null, "rum_error_message": "",
        "rum_body": "",
    })
    .to_string()
}

pub(super) fn rum_tick(fake: &Fake, service: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let options = ingest::coordinator::Options::rum(fake);
    let report = ingest::coordinator::coordinate(
        fake,
        service,
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    );
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn rum_service() -> Service {
    let mut svc = serde_json::to_value(service()).unwrap();
    svc["rum"] = json!({"enabled": true, "endpoint_name": "rum", "log_period": 60});
    serde_json::from_value(svc).unwrap()
}

pub(super) fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i).to_owned()))
        .collect()
}

pub(super) fn floats(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i)))
        .collect()
}

pub(super) fn ints32(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();
    col.values().to_vec()
}

/// One batched beacon as the lean RUM log format writes it.
pub(super) fn rum_batch(at: i64, query: &str) -> String {
    json!({
        "ts_ms": at * 1000 + 250, "service_id": SID, "fastly_req_id": "r1",
        "url": format!("/rum-beacon?service_id={SID}&{query}"),
        "city": "Paris", "region": "IDF", "country": "FR", "pop": "CDG", "tls": "1.3",
        "ttfb": 0.012,
    })
    .to_string()
}

/// Renders a compact log format the way Fastly would: every `%{expr}V` becomes the value of the
/// custom field `expr` reads, or an empty string (quoted) or `null` (bare) for anything else.
pub(super) fn render_compact_line(format: &str, values: &[(&str, &str)]) -> String {
    let mut line = String::new();
    let mut rest = format;
    while let Some(start) = rest.find("%{") {
        let end = start + rest[start..].find("}V").expect("unterminated expression");
        let expr = &rest[start + 2..end];
        let quoted = rest[..start].ends_with('"');
        line.push_str(&rest[..start]);
        let value = values
            .iter()
            .find(|(name, _)| expr.contains(&format!("-data:{name},")))
            .map(|(_, value)| *value);
        line.push_str(value.unwrap_or(if quoted { "" } else { "null" }));
        rest = &rest[end + 2..];
    }
    line.push_str(rest);
    line
}

/// Generates `fields`' compact log format, renders one line of it with `values`, and converts
/// it the way a worker does: with the schema rebuilt from the custom columns its shard carries.
pub(super) fn convert_compact_line(fields: &Value, values: &[(&str, &str)]) -> RecordBatch {
    let format = fastly_log_analytics::logging::generate_compact_log_format(fields).unwrap();
    let line = render_compact_line(&format, values);
    serde_json::from_str::<Value>(&line).expect("rendered line is JSON");
    let zstd_bytes = zstd::stream::encode_all(format!("{line}\n").as_bytes(), 3).unwrap();
    let planned = ingest::convert::RequestSchema::for_fields(fields);
    let shard_custom = serde_json::from_value(json!(planned.custom())).unwrap();
    let schema = ingest::convert::RequestSchema::new(shard_custom);
    assert_eq!(schema.hash(), planned.hash());
    let rows = ingest::convert::convert_object_capped(
        &zstd_bytes,
        300,
        BASE,
        ingest::convert::MAX_DECOMPRESSED_BYTES,
        &schema,
    )
    .unwrap();
    assert_eq!((rows.rows, rows.bad_lines), (1, 0));
    rows.windows.into_values().next().unwrap()
}

pub(super) fn text_at(batch: &RecordBatch, name: &str) -> Option<String> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<StringArray>().unwrap();
    column.is_valid(0).then(|| column.value(0).to_owned())
}

pub(super) fn number_at(batch: &RecordBatch, name: &str) -> Option<i64> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    column.is_valid(0).then(|| column.value(0))
}
