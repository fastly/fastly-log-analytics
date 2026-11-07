//! `summary_bundle` keeps up with the window summaries ingest writes at a 10 s log period with
//! up to 48 summaries a minute (about 2,900 an hour), under a runtime that charges backend calls
//! like Compute and vCPU like the live stage: a window fold, a partial decode and an encode cost
//! tens to hundreds of milliseconds.

use fastly_log_analytics::{
    admin::{
        schedule::{self, JobState, Jobs, Mirror},
        FosRequest, FosResponse, Record, Service,
    },
    ingest::{self, bundle, convert, summary},
    setup::SetupStores,
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    time::Instant,
};

const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
const HOUR: i64 = 3600;
const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
const BUDGET: u32 = 32;
/// What `admin::logs::bundle_stage` keeps back: the cron_runs tail plus one.
const RESERVE: u32 = 4;
const PERIOD_S: i64 = 10;
const PLANS: u32 = 6;
const SHARDS: u32 = 8;
const WINDOWS: &str = "processed/summary/request/";
const BUNDLES: &str = "processed/summary_bundle/request/";
/// Live-like vCPU: every call, a fetched summary's decode and fold, and a written file's encode.
const CALL_MS: u64 = 3;
const FOLD_MS: u64 = 30;
const PARTIAL_FOLD_MS: u64 = 90;
const ENCODE_MS: u64 = 250;
/// The per-execution vCPU stop gate (`bundle::VCPU_STOP_MS`) plus one fold, encode, and call tail.
const VCPU_BUDGET_MS: u64 = bundle::VCPU_STOP_MS + FOLD_MS + ENCODE_MS + 3 * CALL_MS;

#[derive(Default)]
struct Fake {
    now: Cell<i64>,
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    calls: Cell<u32>,
    vcpu: Cell<u64>,
    gets: Cell<u64>,
    puts: Cell<u64>,
    lists: Cell<u64>,
}

impl Fake {
    fn new_execution(&self) {
        self.calls.set(0);
        self.vcpu.set(0);
    }

    fn charge(&self, vcpu: u64) -> Result<(), String> {
        if self.calls.get() >= BUDGET {
            return Err("backend_budget_exhausted".into());
        }
        self.calls.set(self.calls.get() + 1);
        self.vcpu.set(self.vcpu.get() + CALL_MS + vcpu);
        Ok(())
    }

    fn list(&self, query: &str) -> String {
        let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let max: usize = q
            .get("max-keys")
            .and_then(|m| m.parse().ok())
            .unwrap_or(1000);
        let after = q
            .get("continuation-token")
            .or_else(|| q.get("start-after"))
            .cloned();
        let delimiter = q.get("delimiter").cloned();
        let (mut contents, mut prefixes) = (String::new(), std::collections::BTreeSet::new());
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
            contents.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>2027-01-15T08:00:00.000Z</LastModified></Contents>",
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

    fn keys(&self, marker: &str) -> Vec<String> {
        self.bucket
            .borrow()
            .keys()
            .filter(|k| k.rsplit('/').next().unwrap().starts_with(marker))
            .cloned()
            .collect()
    }
}

impl SetupStores for Fake {
    fn config_present(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

fn ok(status: u16, body: Vec<u8>) -> Result<FosResponse, String> {
    Ok(FosResponse {
        cdn_cache: None,
        status,
        body,
        retry_after: None,
    })
}

impl Runtime for Fake {
    fn config(&self, _name: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn secret(&self, _name: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([7; 32])
    }
    fn now(&self) -> u64 {
        self.now.get() as u64
    }
    fn checks(&self, _front_rl: bool) -> Result<Value, String> {
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
    fn admin_remaining_calls(&self) -> u32 {
        BUDGET - self.calls.get()
    }
    fn admin_elapsed(&self, _: Instant) -> std::time::Duration {
        std::time::Duration::ZERO
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.get()
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.charge(0)?;
        Ok(self.kv.borrow().get(key).map(|(generation, value)| Record {
            generation: generation.to_string(),
            value: value.clone(),
        }))
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.charge(0)?;
        let mut kv = self.kv.borrow_mut();
        let current = kv.get(key).map(|(g, _)| *g);
        if current.map(|g| g.to_string()).as_deref() != generation {
            return Ok(false);
        }
        kv.insert(key.into(), (current.map_or(1, |g| g + 1), value.clone()));
        Ok(true)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        match request.method {
            "GET" if request.query.contains("list-type") => {
                self.charge(0)?;
                self.lists.set(self.lists.get() + 1);
                ok(200, self.list(request.query).into_bytes())
            }
            "PUT" => {
                self.charge(ENCODE_MS)?;
                self.puts.set(self.puts.get() + 1);
                self.bucket
                    .borrow_mut()
                    .insert(request.key.into(), request.body);
                ok(200, Vec::new())
            }
            "POST" if request.query == "delete" => {
                self.charge(0)?;
                let xml = String::from_utf8(request.body).unwrap();
                let mut out = String::from("<DeleteResult>");
                for part in xml.split("<Key>").skip(1) {
                    let key = part.split("</Key>").next().unwrap();
                    self.bucket.borrow_mut().remove(key);
                    out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                }
                out.push_str("</DeleteResult>");
                ok(200, out.into_bytes())
            }
            other => panic!("unexpected FOS {other} {}", request.key),
        }
    }
    fn admin_cdn_get(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        _max: usize,
        _timeout: std::time::Duration,
    ) -> Result<FosResponse, String> {
        let partial = key.rsplit('/').next().unwrap().starts_with("partial=");
        self.charge(if partial { PARTIAL_FOLD_MS } else { FOLD_MS })?;
        assert_eq!((service_id, cdn_key), (SID, "ck"));
        self.gets.set(self.gets.get() + 1);
        match self.bucket.borrow().get(key) {
            Some(body) => ok(200, body.clone()),
            None => ok(404, Vec::new()),
        }
    }
}

fn service() -> Service {
    serde_json::from_value(json!({
        "service_id": SID, "name": "n", "status": "active", "created_at": 0, "updated_at": 0,
        "config_version": 1, "logging_enabled": true, "logging_endpoint_name": "e",
        "log_period": PERIOD_S, "sample_rate": 1, "edge_only": false, "custom_condition": "",
        "fos_bucket": "fos-abcdefghijklmnopqrst12-logs", "fos_prefix": "", "fos_region": "us-east-1",
        "fos_endpoint": "x", "log_fields": [], "field_limits": {}, "fastly_token_ref": "t",
        "fos_access_key_ref": "ak", "fos_secret_key_ref": "sk", "owner": "o",
        "bucket_adopted": false, "bucket_checked": false, "last_activated_version": null,
        "last_reconcile": null, "pending_job_id": null
    }))
    .unwrap()
}

/// A small window summary; `seed` varies its values.
fn summary_body(start: i64, seed: u32) -> Vec<u8> {
    let lines: Vec<String> = (0..4)
        .map(|i| {
            json!({
                "timestamp": ingest::rfc3339(start + i * 7),
                "ip": format!("10.0.{seed}.{i}"),
                "status": ([200, 404, 503][(i as usize + seed as usize) % 3]),
                "url": format!("/p{}", (seed * 3 + i as u32) % 17),
                "resp_bytes": 100 + i,
            })
            .to_string()
        })
        .collect();
    let encoded = zstd::stream::encode_all((lines.join("\n") + "\n").as_bytes(), 1).unwrap();
    let rows = convert::convert_object(&encoded, 60, start).unwrap();
    let batch = rows.windows.into_values().next().unwrap();
    summary::write_summary(&[&batch], start).unwrap()
}

/// Writes the `PLANS * SHARDS` window summaries of the minute at `start`.
fn minute(fake: &Fake, start: i64, bodies: &[Vec<u8>]) {
    for plan in 0..PLANS {
        for shard in 0..SHARDS {
            let key =
                summary::summary_key(WINDOWS, start, start + 60, &format!("p{plan}s{shard}"), 4);
            let body = bodies[(plan * SHARDS + shard) as usize % bodies.len()].clone();
            fake.bucket.borrow_mut().insert(key, body);
        }
    }
}

fn watermark(fake: &Fake, at: i64) {
    let t = time::OffsetDateTime::from_unix_timestamp(at - at % 60).unwrap();
    let key = format!(
        "raw/request/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/zzz.zst",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute()
    );
    fake.kv
        .borrow_mut()
        .insert(ingest::WATERMARK_KEY.into(), (1, json!({ "key": key })));
}

fn run(fake: &Fake, svc: &Service) -> bundle::BundleReport {
    run_with_cpu(fake, svc, 0)
}

fn run_with_cpu(fake: &Fake, svc: &Service, initial_cpu: u64) -> bundle::BundleReport {
    fake.new_execution();
    fake.vcpu.set(initial_cpu);
    let store = ingest::storage::Store {
        runtime: fake,
        service: svc,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    let report = bundle::summary_bundle(&store, "ck", RESERVE).unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        fake.calls.get() + RESERVE <= BUDGET,
        "{} calls",
        fake.calls.get()
    );
    assert!(fake.vcpu.get() <= 1000, "{} ms total CPU", fake.vcpu.get());
    report
}

fn hour_bundle(fake: &Fake, hour: i64) -> Option<String> {
    let marker = format!("hour_bundle={:02}-", hour.rem_euclid(86_400) / HOUR);
    fake.keys(&marker)
        .into_iter()
        .find(|k| k.starts_with(BUNDLES) && k.contains(&bundle::day_dir(hour)))
}

fn bodies() -> Vec<Vec<u8>> {
    (0..8).map(|seed| summary_body(BASE, seed)).collect()
}

#[test]
fn prior_execution_cpu_prevents_the_first_fold_without_losing_inputs() {
    let fake = Fake::default();
    let svc = service();
    minute(&fake, BASE, &bodies());
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    let original = fake.bucket.borrow().clone();
    let report = run_with_cpu(&fake, &svc, 710);
    assert!(report.more_pending);
    assert_eq!(report.stopped, Some("vcpu"));
    assert_eq!(fake.gets.get(), 0);
    assert_eq!(fake.puts.get(), 0);
    assert_eq!(*fake.bucket.borrow(), original);
}

#[test]
fn precharged_resume_keeps_the_partial_and_finishes_exactly_across_slices() {
    let fake = Fake::default();
    let svc = service();
    minute(&fake, BASE, &bodies());
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    let sources: Vec<Vec<u8>> = fake.bucket.borrow().values().cloned().collect();
    let expected = bundle::merge(
        &sources.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        BASE,
        bundle::Unit::Hour,
    )
    .unwrap();
    let first = run_with_cpu(&fake, &svc, 550);
    assert_eq!(first.partials, 1);
    let partials = fake.keys("partial=");
    assert_eq!(partials.len(), 1);
    let partial = fake.bucket.borrow()[&partials[0]].clone();
    let gets = fake.gets.get();
    let puts = fake.puts.get();

    let deferred = run_with_cpu(&fake, &svc, 650);
    assert_eq!(deferred.stopped, Some("vcpu"));
    assert!(deferred.more_pending);
    assert_eq!(fake.gets.get(), gets + 1);
    assert_eq!(fake.puts.get(), puts);
    assert_eq!(fake.bucket.borrow()[&partials[0]], partial);
    for _ in 0..20 {
        run(&fake, &svc);
        if hour_bundle(&fake, BASE).is_some() {
            break;
        }
    }
    let key = hour_bundle(&fake, BASE).expect("durable slices finish the hour");
    assert_eq!(fake.bucket.borrow()[&key], expected);
    assert!(fake.keys("partial=").is_empty());
}

#[test]
fn a_fully_covered_partial_defers_final_serialization_after_precharged_decode() {
    let fake = Fake::default();
    let svc = service();
    let body = summary_body(BASE, 0);
    fake.bucket.borrow_mut().insert(
        summary::summary_key(WINDOWS, BASE, BASE + 60, "one", 4),
        body.clone(),
    );
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    let first = run_with_cpu(&fake, &svc, 640);
    assert_eq!(first.partials, 1, "{first:?}");
    let partials = fake.keys("partial=");
    let puts = fake.puts.get();
    let second = run_with_cpu(&fake, &svc, 650);
    assert_eq!(second.stopped, Some("vcpu"));
    assert!(second.more_pending);
    assert_eq!(fake.puts.get(), puts);
    assert_eq!(fake.keys("partial="), partials);
    run(&fake, &svc);
    let key = hour_bundle(&fake, BASE).expect("final serialization resumes separately");
    assert_eq!(
        fake.bucket.borrow()[&key],
        bundle::merge(&[&body], BASE, bundle::Unit::Hour).unwrap()
    );
}

#[test]
fn a_closed_hour_of_2880_windows_bundles_in_bounded_executions() {
    let fake = Fake::default();
    let svc = service();
    let bodies = bodies();
    for m in 0..60 {
        minute(&fake, BASE + m * 60, &bodies);
    }
    let now = BASE + 2 * HOUR;
    fake.now.set(now);
    watermark(&fake, now);
    let (mut runs, mut peak_vcpu, mut peak_bytes) = (0, 0, 0);
    while hour_bundle(&fake, BASE).is_none() {
        runs += 1;
        assert!(runs <= 200, "not bundled after {runs} executions");
        let report = run(&fake, &svc);
        peak_vcpu = peak_vcpu.max(fake.vcpu.get());
        peak_bytes = peak_bytes.max(report.bytes_written);
        assert!(report.sources >= 16 || report.hours == 1, "{report:?}");
    }
    let key = hour_bundle(&fake, BASE).unwrap();
    assert!(key.contains("-c2880-n11520-"), "{key}");
    assert!(peak_vcpu <= VCPU_BUDGET_MS, "{peak_vcpu} ms");
    assert!(fake.gets.get() <= 2880 + runs, "{} GETs", fake.gets.get());
    assert_eq!(fake.keys("partial=").len(), 0);
    assert!(peak_bytes < 200_000, "{peak_bytes} B");
}

/// Ten seconds of a 10 s log period: the minutes whose windows ingest has written by `now`.
fn arrive(fake: &Fake, now: i64, written: &mut i64, bodies: &[Vec<u8>]) {
    while *written + 60 + 2 * PERIOD_S <= now {
        minute(fake, *written, bodies);
        *written += 60;
    }
}

#[test]
fn bundling_keeps_up_with_windows_arriving_at_the_live_rate() {
    let fake = Fake::default();
    let svc = service();
    let bodies = bodies();
    let mut mirror = Mirror {
        configured: true,
        log_period_s: PERIOD_S as u64,
        jobs: Jobs::default(),
        rum_log_period_s: 0,
    };
    let (start, end) = (BASE, BASE + 4 * HOUR);
    let mut written = start;
    let (mut runs, mut busy, mut done_at) = (0u64, 0u64, BTreeMap::new());
    let mut now = start;
    while now < end {
        fake.now.set(now);
        arrive(&fake, now, &mut written, &bodies);
        watermark(&fake, now - PERIOD_S);
        if schedule::due(&mirror, now as u64).maint {
            let report = run(&fake, &svc);
            runs += 1;
            busy += fake.vcpu.get();
            mirror.jobs.maint = JobState {
                last_run: Some(now as u64),
                more_pending: report.more_pending,
                ..JobState::default()
            };
            for hour in (start..start + 3 * HOUR).step_by(HOUR as usize) {
                if !done_at.contains_key(&hour) && hour_bundle(&fake, hour).is_some() {
                    done_at.insert(hour, now);
                }
            }
        }
        now += PERIOD_S;
    }
    for hour in (start..start + 3 * HOUR).step_by(HOUR as usize) {
        let closes = hour + HOUR + PERIOD_S + bundle::CLOSE_SLACK_S;
        let at = done_at.get(&hour).copied();
        assert!(
            at.is_some_and(|at| at <= closes + 300),
            "hour {hour} bundled at {at:?}, closes {closes}"
        );
        let key = hour_bundle(&fake, hour).unwrap();
        assert!(key.contains("-c2880-n11520-"), "{key}");
    }
    let windows = (written - start) as u64 / 60 * u64::from(PLANS * SHARDS);
    assert!(
        fake.gets.get() <= windows + runs,
        "{} GETs for {windows} windows in {runs} runs",
        fake.gets.get()
    );
    // Each run folds a batch: about one write per 12 windows, never one per window.
    assert!(fake.puts.get() * 12 <= windows, "{} PUTs", fake.puts.get());
    assert!(busy / runs <= VCPU_BUDGET_MS, "{} ms a run", busy / runs);
}
