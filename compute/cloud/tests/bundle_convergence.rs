//! `summary_bundle` keeps up with the window summaries ingest writes at a 10 s log period with
//! up to 48 summaries a minute (about 2,900 an hour), under a runtime that charges backend calls
//! like Compute and vCPU like the live stage: a window fold, a partial decode and an encode cost
//! tens to hundreds of milliseconds. The catalog test runs a day of a 60 s log period through
//! catalog commits and the maintenance cadence, as the live service does.

use fastly_log_analytics::{
    admin::{
        schedule::{self, JobState, Jobs, Mirror},
        FosRequest, FosResponse, Record, Service,
    },
    ingest::{self, bundle, catalog, convert, summary, worker::Output},
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
/// A `partial=` write: natively its encode and zstd pack take ~1.6 folds and the Parquet
/// finish ~4 (`open_hour_turn_cost`), so this is charged like a partial read, not a bundle.
const PARTIAL_ENCODE_MS: u64 = PARTIAL_FOLD_MS;
/// The deploy-testing `summary_bundle` vCPU budget: the stop gate plus one fold,
/// encode and call tail must fit under it.
const VCPU_BUDGET_MS: u64 = 900;
/// A catalog block, day index or coverage read: a small JSON decode.
const CATALOG_READ_MS: u64 = 2;

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
    /// Overrides [`FOLD_MS`] when set.
    fold_ms: Cell<u64>,
    /// Window summaries fetched to fold, and bundle or partial files encoded.
    folds: Cell<u64>,
    encodes: Cell<u64>,
    /// vCPU charged for backend calls, folds and encodes across every run (startup excluded).
    work: Cell<u64>,
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
        self.work.set(self.work.get() + CALL_MS + vcpu);
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
        Ok(Some("0123456789abcdef0123456789abcdef".into()))
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
            "GET" => {
                self.charge(CATALOG_READ_MS)?;
                match self.bucket.borrow().get(request.key) {
                    Some(body) => ok(200, body.clone()),
                    None => ok(404, Vec::new()),
                }
            }
            "PUT" => {
                // Catalog JSON is cheap to serialize; parquet bundles and partials are encoded.
                let encoded = !request.key.contains("/_catalog/");
                let partial = request
                    .key
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .starts_with("partial=");
                self.charge(match (encoded, partial) {
                    (false, _) => 0,
                    (true, true) => PARTIAL_ENCODE_MS,
                    (true, false) => ENCODE_MS,
                })?;
                self.encodes.set(self.encodes.get() + u64::from(encoded));
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
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let tag = format!("\"{}\"", hex::encode(Md5::digest(&request.body)));
        self.admin_fos(request)
            .map(|response| (response, Some(tag)))
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
        let fold = match self.fold_ms.get() {
            0 => FOLD_MS,
            ms => ms,
        };
        self.charge(if partial { PARTIAL_FOLD_MS } else { fold })?;
        self.folds.set(self.folds.get() + u64::from(!partial));
        assert_eq!((service_id, cdn_key), (SID, "ck"));
        self.gets.set(self.gets.get() + 1);
        match self.bucket.borrow().get(key) {
            Some(body) => ok(200, body.clone()),
            None => ok(404, Vec::new()),
        }
    }
}

fn service() -> Service {
    service_with_period(PERIOD_S)
}

fn service_with_period(log_period: i64) -> Service {
    serde_json::from_value(json!({
        "service_id": SID, "name": "n", "status": "active", "created_at": 0, "updated_at": 0,
        "config_version": 1, "logging_enabled": true, "logging_endpoint_name": "e",
        "log_period": log_period, "sample_rate": 1, "edge_only": false, "custom_condition": "",
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

/// The raw object key of the minute holding `at`, as the ingest watermark names it.
fn raw_key(at: i64) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(at - at % 60).unwrap();
    format!(
        "raw/request/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/zzz.zst",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute()
    )
}

fn watermark(fake: &Fake, at: i64) {
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (1, json!({ "key": raw_key(at) })),
    );
}

fn store<'a>(fake: &'a Fake, svc: &'a Service) -> ingest::storage::Store<'a, Fake> {
    ingest::storage::Store {
        runtime: fake,
        service: svc,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    }
}

fn run(fake: &Fake, svc: &Service) -> bundle::BundleReport {
    run_with_cpu(fake, svc, 0)
}

fn run_with_cpu(fake: &Fake, svc: &Service, initial_cpu: u64) -> bundle::BundleReport {
    fake.new_execution();
    fake.vcpu.set(initial_cpu);
    let report = bundle::summary_bundle(&store(fake, svc), "ck", RESERVE).unwrap();
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
    let first = run_with_cpu(&fake, &svc, bundle::VCPU_STOP_MS - 150);
    assert_eq!(first.partials, 1);
    let partials = fake.keys("partial=");
    assert_eq!(partials.len(), 1);
    let partial = fake.bucket.borrow()[&partials[0]].clone();
    let gets = fake.gets.get();
    let puts = fake.puts.get();

    let deferred = run_with_cpu(&fake, &svc, bundle::VCPU_STOP_MS - 50);
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
    let first = run_with_cpu(&fake, &svc, bundle::VCPU_STOP_MS - 20);
    assert_eq!(first.partials, 1, "{first:?}");
    let partials = fake.keys("partial=");
    let puts = fake.puts.get();
    let second = run_with_cpu(&fake, &svc, bundle::VCPU_STOP_MS - 50);
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
        compaction: false,
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
    // Each run folds a batch and writes its state and the open snapshot: about two writes per
    // 16 windows, never one per window.
    eprintln!(
        "live rate: {} PUTs for {windows} windows, {} ms a run, {} ms of work",
        fake.puts.get(),
        busy / runs,
        fake.work.get()
    );
    assert!(fake.puts.get() * 8 <= windows, "{} PUTs", fake.puts.get());
    assert!(busy / runs <= VCPU_BUDGET_MS, "{} ms a run", busy / runs);
}

/// A 60 s log period writes this many window summaries a minute, about 180 an hour as live.
const LIVE_PLANS: u32 = 3;
const LIVE_PERIOD_S: i64 = 60;
/// vCPU a stage execution has spent before `summary_bundle` starts (worker start, the reset
/// check, stages that passed the turn). Live, about half of the runs arrive past 150 ms.
const STARTUP_MS: [u64; 4] = [60, 120, 170, 230];
/// Other maintenance stages with work, taking their turns in the rotation (live, the bundle
/// stage got about one maintenance execution in four).
const BUSY_STAGES: u32 = 3;
/// A closed hour's catalog bundle is published within this long after the hour ends. The hour
/// only closes `CLOSE_SLACK_S` plus the log period and commit lag (about 12 minutes) after its end.
const PUBLISHED_WITHIN_S: i64 = 30 * 60;

/// Writes the window summaries of the minute at `start` and commits them, with one processed
/// file each, to the catalog the way an ingest commit does, then moves the watermark to it.
fn commit_minute(fake: &Fake, svc: &Service, start: i64, bodies: &[Vec<u8>]) {
    let (mut files, mut summaries) = (Vec::new(), Vec::new());
    for plan in 0..LIVE_PLANS {
        let id = format!("p{plan}");
        let key = summary::summary_key(WINDOWS, start, start + 60, &id, 4);
        let body = bodies[(start / 60 + i64::from(plan)) as usize % bodies.len()].clone();
        summaries.push(Output {
            key: key.clone(),
            rows: 4,
            bytes: body.len() as u64,
            ..Default::default()
        });
        fake.bucket.borrow_mut().insert(key, body);
        files.push(Output {
            key: ingest::processed_key("processed/request/", start, start + 60, &id, 4),
            rows: 4,
            bytes: 1000,
            ..Default::default()
        });
    }
    for _ in 0..50 {
        fake.new_execution();
        let record = fake.kv.borrow().get(ingest::WATERMARK_KEY).cloned();
        let previous = record
            .as_ref()
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| json!({}));
        match catalog::prepare(
            &store(fake, svc),
            &previous,
            "commit",
            "schema",
            &files,
            &summaries,
            &[],
            &[],
        ) {
            Ok(mut value) => {
                value["key"] = raw_key(start).into();
                let generation = record.map_or(1, |(generation, _)| generation + 1);
                fake.kv
                    .borrow_mut()
                    .insert(ingest::WATERMARK_KEY.into(), (generation, value));
                return;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("commit failed: {error}"),
        }
    }
    panic!("commit made no progress");
}

fn catalog_head(fake: &Fake) -> catalog::Head {
    fake.kv
        .borrow()
        .get(ingest::WATERMARK_KEY)
        .map_or_else(catalog::Head::default, |(_, value)| {
            catalog::head(value).unwrap()
        })
}

/// The windows a published catalog hour bundle covers.
fn covered(fake: &Fake, reference: &catalog::Reference) -> usize {
    catalog::bundles::parse(&fake.bucket.borrow()[&reference.key])
        .unwrap()
        .windows
        .len()
}

#[test]
fn a_day_of_60s_windows_converges_to_catalog_hour_bundles() {
    let fake = Fake::default();
    let svc = service_with_period(LIVE_PERIOD_S);
    let bodies = bodies();
    let mut mirror = Mirror {
        configured: true,
        log_period_s: LIVE_PERIOD_S as u64,
        jobs: Jobs::default(),
        rum_log_period_s: 0,
        compaction: false,
    };
    let (start, end) = (BASE, BASE + 24 * HOUR);
    let mut written = start;
    let (mut slots, mut runs, mut busy) = (0u32, 0usize, 0u64);
    let mut published = BTreeMap::new();
    let mut now = start;
    while now < end + HOUR {
        fake.now.set(now);
        // Ingest commits a minute's windows about two log periods after the minute starts, and
        // keeps going past the day so its last hour closes.
        while written + 2 * LIVE_PERIOD_S <= now {
            commit_minute(&fake, &svc, written, &bodies);
            written += 60;
        }
        if schedule::due(&mirror, now as u64).maint {
            // The rotation: the bundle stage's turn, then the other busy stages' turns.
            let bundle_pending = (slots % (BUSY_STAGES + 1) == 0).then(|| {
                let report = run_with_cpu(&fake, &svc, STARTUP_MS[runs % STARTUP_MS.len()]);
                runs += 1;
                busy += fake.vcpu.get();
                report.more_pending
            });
            slots += 1;
            // What the dispatcher records for the maintenance job after an answered execution.
            let pending =
                schedule::maint_catching_up(mirror.jobs.maint.more_pending, bundle_pending);
            mirror.jobs.maint = JobState {
                last_run: Some(now as u64),
                more_pending: pending,
                retry_after: if pending {
                    now as u64 + schedule::MAINT_CATCHUP_S
                } else {
                    0
                },
                ..JobState::default()
            };
            for (hour, reference) in catalog_head(&fake).hour_bundles {
                published.entry(hour).or_insert((now, reference));
            }
        }
        now += 10;
    }
    let windows = 60 * LIVE_PLANS as usize;
    let late: Vec<String> = (start..end)
        .step_by(HOUR as usize)
        .filter_map(|hour| match published.get(&hour) {
            Some((at, reference))
                if *at <= hour + HOUR + PUBLISHED_WITHIN_S
                    && covered(&fake, reference) == windows =>
            {
                None
            }
            Some((at, reference)) => Some(format!(
                "{}h: {} windows at +{}s",
                (hour - start) / HOUR,
                covered(&fake, reference),
                at - hour - HOUR
            )),
            None => Some(format!("{}h: none", (hour - start) / HOUR)),
        })
        .collect();
    assert!(late.is_empty(), "late after {runs} runs: {late:?}");
    let head = catalog_head(&fake);
    for hour in (start..end).step_by(HOUR as usize) {
        assert_eq!(covered(&fake, &head.hour_bundles[&hour]), windows);
    }
    eprintln!(
        "day: vCPU mean {} over {runs} runs; {} folds for {} windows, {} writes, {} ms of work",
        busy / runs as u64,
        fake.folds.get(),
        windows * 24,
        fake.encodes.get(),
        fake.work.get()
    );
    assert!(
        fake.folds.get() as usize <= windows * 24 * 11 / 10,
        "{} folds for {} windows",
        fake.folds.get(),
        windows * 24
    );
    assert!(
        busy / runs as u64 <= VCPU_BUDGET_MS,
        "{} ms a run",
        busy / runs as u64
    );
}

/// Live 2-minute windows: each gets a summary per shard from the run that read each of the
/// three log periods its lines land in, so its files keep arriving for three minutes after it
/// ends, under random run ids that sort anywhere among the window's earlier files.
const OPEN_WINDOW_S: i64 = 120;
const OPEN_RUN_SHARDS: u32 = 1;
const OPEN_RUN_LAGS: [i64; 3] = [60, 120, 180];
/// What a browser fetches for the open hour once it is this far in: one open bundle plus the
/// windows written since the last snapshot was built and published (at most and on average).
/// Before open-hour partials stopped at settled windows it was 79 and 33.
const OPEN_HOUR_FILES: usize = 30;
const OPEN_HOUR_MEAN_FILES: usize = 16;
const OPEN_SETTLED_FROM_S: i64 = 20 * 60;

/// Commits the summaries `runs` (window start, run id) wrote, one processed file each, and
/// moves the watermark to the raw minute that run read.
fn commit_runs(
    fake: &Fake,
    svc: &Service,
    runs: &[(i64, String)],
    read_minute: i64,
    bodies: &[Vec<u8>],
) -> Vec<String> {
    let (mut files, mut summaries, mut keys) = (Vec::new(), Vec::new(), Vec::new());
    for (start, run) in runs {
        for shard in 0..OPEN_RUN_SHARDS {
            let id = format!("{run}s{shard}");
            let key = summary::summary_key(WINDOWS, *start, start + OPEN_WINDOW_S, &id, 4);
            let body = bodies[(*start / 60) as usize % bodies.len()].clone();
            summaries.push(Output {
                key: key.clone(),
                rows: 4,
                bytes: body.len() as u64,
                ..Default::default()
            });
            fake.bucket.borrow_mut().insert(key.clone(), body);
            keys.push(key);
            files.push(Output {
                key: ingest::processed_key(
                    "processed/request/",
                    *start,
                    start + OPEN_WINDOW_S,
                    &id,
                    4,
                ),
                rows: 4,
                bytes: 1000,
                ..Default::default()
            });
        }
    }
    for _ in 0..50 {
        fake.new_execution();
        let record = fake.kv.borrow().get(ingest::WATERMARK_KEY).cloned();
        let previous = record
            .as_ref()
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| json!({}));
        match catalog::prepare(
            &store(fake, svc),
            &previous,
            "commit",
            "schema",
            &files,
            &summaries,
            &[],
            &[],
        ) {
            Ok(mut value) => {
                value["key"] = raw_key(read_minute).into();
                let generation = record.map_or(1, |(generation, _)| generation + 1);
                fake.kv
                    .borrow_mut()
                    .insert(ingest::WATERMARK_KEY.into(), (generation, value));
                return keys;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("commit failed: {error}"),
        }
    }
    panic!("commit made no progress");
}

/// Open bundles the catalog lists that are not byte-for-byte a one-shot merge of the windows
/// they claim to cover, in file-name order; `checked` skips the ones already compared.
fn inexact_open_bundles(
    fake: &Fake,
    checked: &mut std::collections::BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (hour, reference) in catalog_head(fake).open_bundles {
        if !checked.insert(reference.key.clone()) {
            continue;
        }
        let bucket = fake.bucket.borrow();
        let coverage = catalog::bundles::parse(&bucket[&reference.key]).unwrap();
        let bodies: Vec<&[u8]> = coverage.windows.iter().map(|k| &bucket[k][..]).collect();
        let expected = bundle::merge(&bodies, hour, bundle::Unit::Hour).unwrap();
        if bucket[&coverage.file.key] != expected {
            out.push(format!("{}: {} windows", coverage.file.key, bodies.len()));
        }
    }
    out
}

/// The files a summary listing hands a browser for the open hour: its published open bundle
/// plus every committed window the bundle does not cover, else every window.
fn open_hour_files(
    fake: &Fake,
    hour: i64,
    committed: &std::collections::BTreeSet<String>,
) -> usize {
    let head = catalog_head(fake);
    let Some(reference) = head.open_bundles.get(&hour) else {
        return committed.len();
    };
    let coverage = catalog::bundles::parse(&fake.bucket.borrow()[&reference.key]).unwrap();
    assert!(
        coverage.windows.is_subset(committed),
        "an open bundle covers only committed windows"
    );
    1 + committed.difference(&coverage.windows).count()
}

#[test]
fn the_open_hour_stays_a_handful_of_files_while_late_runs_keep_arriving() {
    let fake = Fake::default();
    let svc = service_with_period(LIVE_PERIOD_S);
    let bodies = bodies();
    let mut mirror = Mirror {
        configured: true,
        log_period_s: LIVE_PERIOD_S as u64,
        jobs: Jobs::default(),
        rum_log_period_s: 0,
        compaction: false,
    };
    let (start, end) = (BASE, BASE + 6 * HOUR);
    let mut committed: BTreeMap<i64, std::collections::BTreeSet<String>> = BTreeMap::new();
    let (mut slots, mut runs, mut busy, mut peak) = (0u32, 0usize, 0u64, 0u64);
    let mut samples: Vec<(i64, usize)> = Vec::new();
    let mut checked = std::collections::BTreeSet::new();
    let mut now = start;
    while now < end {
        fake.now.set(now);
        if now % 60 == 0 && now >= start + 60 {
            // The run at `now` read the raw minute before it: its lines land in windows that
            // ended up to three periods ago.
            let mut due = Vec::new();
            for lag in OPEN_RUN_LAGS {
                let window_end = now - lag;
                if window_end > start && window_end % OPEN_WINDOW_S == 0 {
                    let id = format!(
                        "{:012x}",
                        (now as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 16
                    );
                    due.push((window_end - OPEN_WINDOW_S, id));
                }
            }
            for key in commit_runs(&fake, &svc, &due, now - 60, &bodies) {
                let hour = key
                    .rsplit('/')
                    .next()
                    .and_then(|name| name.strip_prefix("summary="))
                    .and_then(|rest| rest.split('-').next())
                    .and_then(|s| s.parse::<i64>().ok())
                    .map(|s| s - s.rem_euclid(HOUR))
                    .unwrap();
                committed.entry(hour).or_default().insert(key);
            }
        }
        if schedule::due(&mirror, now as u64).maint {
            let bundle_pending = (slots % (BUSY_STAGES + 1) == 0).then(|| {
                let report = run_with_cpu(&fake, &svc, STARTUP_MS[runs % STARTUP_MS.len()]);
                runs += 1;
                busy += fake.vcpu.get();
                peak = peak.max(fake.vcpu.get());
                report.more_pending
            });
            slots += 1;
            let pending =
                schedule::maint_catching_up(mirror.jobs.maint.more_pending, bundle_pending);
            mirror.jobs.maint = JobState {
                last_run: Some(now as u64),
                more_pending: pending,
                retry_after: if pending {
                    now as u64 + schedule::MAINT_CATCHUP_S
                } else {
                    0
                },
                ..JobState::default()
            };
        }
        let hour = now - now.rem_euclid(HOUR);
        if now - hour >= OPEN_SETTLED_FROM_S && now % 60 == 30 && hour > start {
            let files = committed
                .get(&hour)
                .map_or(0, |windows| open_hour_files(&fake, hour, windows));
            samples.push((now - start, files));
        }
        let inexact = inexact_open_bundles(&fake, &mut checked);
        assert!(inexact.is_empty(), "+{}s: {inexact:?}", now - start);
        now += 10;
    }
    let worst = samples.iter().map(|(_, n)| *n).max().unwrap();
    let mean = samples.iter().map(|(_, n)| *n).sum::<usize>() / samples.len();
    let over: Vec<&(i64, usize)> = samples
        .iter()
        .filter(|(_, n)| *n > OPEN_HOUR_FILES)
        .collect();
    assert!(
        over.is_empty(),
        "open hour over {OPEN_HOUR_FILES} files (mean {mean}, worst {worst}) at {} of {} samples: {:?}",
        over.len(),
        samples.len(),
        &over[..over.len().min(12)]
    );
    assert!(mean <= OPEN_HOUR_MEAN_FILES, "open hour mean {mean} files");
    assert!(
        busy / runs as u64 <= VCPU_BUDGET_MS,
        "{} ms a run",
        busy / runs as u64
    );
    assert!(peak <= 1000, "{peak} ms peak");
    // Open bundles grow from the settled windows already folded into a partial, so each window
    // is folded about once, not again for every snapshot taken while it was still recent.
    let windows: usize = committed.values().map(|w| w.len()).sum();
    assert!(
        fake.folds.get() as usize <= windows * 11 / 10,
        "{} folds for {windows} windows",
        fake.folds.get()
    );
    eprintln!(
        "open hour files: mean {mean}, worst {worst}; vCPU mean {} peak {peak}; {} folds, {} writes, {} ms of work",
        busy / runs as u64,
        fake.folds.get(),
        fake.encodes.get(),
        fake.work.get()
    );
}

/// Live Wasm folds a window in about 50 ms, so a fold started just under the stop gate ends
/// past the write gate. A run that folds must still write what it folded (a snapshot, a partial
/// or the bundle) instead of throwing the folds away and repeating them next run.
#[test]
fn a_run_that_folds_windows_always_writes_them() {
    let fake = Fake::default();
    fake.fold_ms.set(50);
    let svc = service_with_period(LIVE_PERIOD_S);
    let bodies = bodies();
    let (start, end) = (BASE, BASE + 4 * HOUR);
    let mut written = start;
    let (mut runs, mut wasted, mut folds, mut busy) = (0usize, Vec::new(), 0u64, 0u64);
    let mut now = start;
    while now < end {
        fake.now.set(now);
        while written + 2 * LIVE_PERIOD_S <= now {
            commit_minute(&fake, &svc, written, &bodies);
            written += 60;
        }
        if now % 120 == 0 {
            let (f, e) = (fake.folds.get(), fake.encodes.get());
            run_with_cpu(&fake, &svc, STARTUP_MS[runs % STARTUP_MS.len()]);
            runs += 1;
            busy += fake.vcpu.get();
            let run_folds = fake.folds.get() - f;
            folds += run_folds;
            if run_folds > 0 && fake.encodes.get() == e {
                wasted.push(format!("+{}s: {run_folds} folds", now - start));
            }
        }
        now += 10;
    }
    let windows = (written - start) as u64 / 60 * u64::from(LIVE_PLANS);
    eprintln!(
        "{folds} folds for {windows} windows, {} ms a run, {} wasted runs",
        busy / runs as u64,
        wasted.len()
    );
    assert!(
        wasted.is_empty(),
        "{} of {runs} runs wasted their folds: {:?}",
        wasted.len(),
        &wasted[..wasted.len().min(12)]
    );
}

/// Every open, hour and day bundle the catalog lists, as `kind@hour: missing object` when its
/// coverage or its file is gone.
fn dangling(fake: &Fake) -> Vec<String> {
    let head = catalog_head(fake);
    let bucket = fake.bucket.borrow();
    let mut out = Vec::new();
    for (kind, map) in [
        ("open", &head.open_bundles),
        ("hour", &head.hour_bundles),
        ("day", &head.day_bundles),
    ] {
        for (start, reference) in map {
            let file = bucket
                .get(&reference.key)
                .map(|body| catalog::bundles::parse(body).unwrap().file.key);
            if !file.as_ref().is_some_and(|key| bucket.contains_key(key)) {
                out.push(format!("{kind}@{}: {file:?}", (start - BASE) / HOUR));
            }
        }
    }
    out
}

/// Runs the bundle stage every maintenance tick from `from` to `to`, committing each minute's
/// windows as ingest does, and asserts after every run that the catalog lists no deleted bundle.
fn run_catalog(fake: &Fake, svc: &Service, from: i64, to: i64, written: &mut i64) {
    let bodies = bodies();
    let mut now = from;
    while now < to {
        fake.now.set(now);
        while *written + 2 * LIVE_PERIOD_S <= now {
            commit_minute(fake, svc, *written, &bodies);
            *written += 60;
        }
        if now % 60 == 0 {
            run(fake, svc);
            let dangling = dangling(fake);
            assert!(
                dangling.is_empty(),
                "+{}s: catalog lists deleted bundles {dangling:?}",
                now - BASE
            );
        }
        now += 10;
    }
}

fn bundle_state(fake: &Fake) -> Value {
    fake.kv.borrow()[bundle::STATE_KEY].1.clone()
}

#[test]
fn a_lost_hour_publication_never_deletes_the_open_bundle_the_catalog_lists() {
    let fake = Fake::default();
    let svc = service_with_period(LIVE_PERIOD_S);
    let mut written = BASE;
    // Into the hour's close, until its hour bundle is built and waits to be published.
    let mut now = BASE;
    run_catalog(&fake, &svc, now, BASE + HOUR - 600, &mut written);
    assert!(catalog_head(&fake).open_bundles.contains_key(&BASE));
    now = BASE + HOUR - 600;
    while bundle_state(&fake)["hours"][BASE.to_string()].is_null()
        || bundle_state(&fake)["publication"].is_null()
    {
        run_catalog(&fake, &svc, now, now + 60, &mut written);
        now += 60;
        assert!(now < BASE + 2 * HOUR, "the hour bundle was never built");
    }
    // The publication never lands (superseded, rejected or lost to a crash).
    let mut state = bundle_state(&fake);
    state["publication"] = Value::Null;
    fake.kv.borrow_mut().get_mut(bundle::STATE_KEY).unwrap().1 = state;
    run_catalog(&fake, &svc, now, now + HOUR, &mut written);
    assert!(
        catalog_head(&fake).hour_bundles.contains_key(&BASE),
        "the hour is rebuilt and published"
    );
}

#[test]
fn a_catalog_bundle_whose_object_is_missing_is_dropped_on_the_next_run() {
    let fake = Fake::default();
    let svc = service_with_period(LIVE_PERIOD_S);
    let mut written = BASE;
    run_catalog(&fake, &svc, BASE, BASE + 2 * HOUR - 600, &mut written);
    let head = catalog_head(&fake);
    let (hour_ref, open_ref) = (
        &head.hour_bundles[&BASE],
        &head.open_bundles[&(BASE + HOUR)],
    );
    for reference in [hour_ref, open_ref] {
        let coverage = catalog::bundles::parse(&fake.bucket.borrow()[&reference.key]).unwrap();
        fake.bucket.borrow_mut().remove(&coverage.file.key);
    }
    assert_eq!(dangling(&fake).len(), 2);
    // A run after the last check's interval drops them with what its bundling left over.
    let mut now = BASE + 2 * HOUR - 600 + bundle::HEAL_INTERVAL_S;
    for _ in 0..5 {
        fake.now.set(now);
        run(&fake, &svc);
        if dangling(&fake).is_empty() {
            break;
        }
        now += 60;
    }
    assert_eq!(dangling(&fake), Vec::<String>::new());
    // The dropped hour is rebuilt from its windows.
    run_catalog(&fake, &svc, now + 60, now + 1800, &mut written);
    assert!(catalog_head(&fake).hour_bundles.contains_key(&BASE));
}

const CMCD_WINDOWS: &str = "processed/request/cmcd_summary/";
const CMCD_BUNDLES: &str = "processed/summary_bundle/cmcd_summary/";

/// A small CMCD window summary; `seed` varies its sessions, bitrates and countries.
fn cmcd_body(start: i64, seed: u32) -> Vec<u8> {
    use arrow_array::{Int64Array, StringArray, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("geo_country", DataType::Utf8, true),
        Field::new("client_asn", DataType::Int64, true),
        Field::new("cmcd_sid", DataType::Utf8, true),
        Field::new("cmcd_br", DataType::Int64, true),
    ]));
    let n = 4i64;
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                TimestampMillisecondArray::from(
                    (0..n).map(|i| start * 1000 + i * 7000).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| Some(["US", "GB", "DE"][(i as usize + seed as usize) % 3]))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| Some(64_500 + (seed as i64 + i) % 5))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| Some(format!("s{}", (seed as i64 * 3 + i) % 11)))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| Some(1000 + 500 * ((seed as i64 + i) % 4)))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ingest::cmcd_summary::write_cmcd_summary(&[&batch], start).unwrap()
}

/// Writes two shards' CMCD window summaries for each minute of the hour at `hour`.
fn cmcd_hour(fake: &Fake, hour: i64, minutes: i64) -> Vec<String> {
    let mut keys = Vec::new();
    for m in 0..minutes {
        for shard in 0..2 {
            let start = hour + m * 60;
            let key = ingest::cmcd_summary::cmcd_summary_key(
                CMCD_WINDOWS,
                start,
                start + 60,
                &format!("p{m}s{shard}"),
                4,
            );
            let body = cmcd_body(start, (m * 2 + shard) as u32 % 8);
            fake.bucket.borrow_mut().insert(key.clone(), body);
            keys.push(key);
        }
    }
    keys
}

fn run_cmcd(fake: &Fake, svc: &Service) -> bundle::BundleReport {
    fake.new_execution();
    let report = bundle::cmcd_summary_bundle(&store(fake, svc), "ck", RESERVE).unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        fake.calls.get() + RESERVE <= BUDGET,
        "{} calls",
        fake.calls.get()
    );
    assert!(fake.vcpu.get() <= 1000, "{} ms total CPU", fake.vcpu.get());
    report
}

/// The CMCD merge of `keys`' bodies in file-name order, as the stage folds them.
fn cmcd_expected(fake: &Fake, keys: &[String]) -> Vec<u8> {
    let mut sorted: Vec<&String> = keys.iter().collect();
    sorted.sort_by_key(|k| k.rsplit('/').next().unwrap().to_owned());
    let bucket = fake.bucket.borrow();
    let bodies: Vec<&[u8]> = sorted.iter().map(|k| bucket[*k].as_slice()).collect();
    ingest::cmcd_summary::merge_windows(&bodies).unwrap()
}

#[test]
fn cmcd_summaries_bundle_into_exact_hour_and_open_bundles_in_bounded_executions() {
    let fake = Fake::default();
    let svc = service();
    let closed = cmcd_hour(&fake, BASE, 60);
    let open = cmcd_hour(&fake, BASE + HOUR, 20);
    fake.now.set(BASE + HOUR + 30 * 60);
    watermark(&fake, fake.now.get());
    let request_state = fake.kv.borrow().get(bundle::STATE_KEY).cloned();
    let mut runs = 0;
    while runs < 40 {
        runs += 1;
        run_cmcd(&fake, &svc);
        let cmcd = |marker: &str| {
            fake.keys(marker)
                .iter()
                .any(|k| k.starts_with(CMCD_BUNDLES))
        };
        // The open hour waits for an execution that wrote nothing else.
        if cmcd("hour_bundle=08-") && cmcd("open_bundle=09-") {
            break;
        }
    }
    let hour_key = fake
        .keys("hour_bundle=08-")
        .into_iter()
        .find(|k| k.starts_with(CMCD_BUNDLES))
        .expect("the closed CMCD hour is bundled");
    assert!(hour_key.contains("hour_bundle=08-c120-n480-"), "{hour_key}");
    assert_eq!(
        fake.bucket.borrow()[&hour_key],
        cmcd_expected(&fake, &closed)
    );
    assert!(fake
        .keys("hour_bundle=")
        .iter()
        .all(|k| k.starts_with(CMCD_BUNDLES)));
    assert!(fake
        .keys("partial=")
        .iter()
        .all(|k| !k.contains("hour=08/")));
    // The open hour's snapshot covers exactly the name-order prefix it names.
    let open_key = fake
        .keys("open_bundle=09-")
        .into_iter()
        .find(|k| k.starts_with(CMCD_BUNDLES))
        .expect("the open CMCD hour gets a snapshot");
    let name = bundle::parse_name(&open_key).unwrap();
    let mut sorted = open.clone();
    sorted.sort_by_key(|k| k.rsplit('/').next().unwrap().to_owned());
    assert_eq!(
        fake.bucket.borrow()[&open_key],
        cmcd_expected(&fake, &sorted[..name.count])
    );
    assert!(name.count >= 6, "{open_key}");
    assert!(runs < 40, "converges");
    // The request bundler's state is untouched.
    assert_eq!(
        fake.kv.borrow().get(bundle::STATE_KEY).cloned(),
        request_state
    );
    assert!(fake.kv.borrow().contains_key(bundle::CMCD_STATE_KEY));
}

/// CMCD windows are listed from the bucket, not a catalog head: a bundle of pre-reset windows
/// written during Delete Data cleanup would postdate the reset and outlive it.
#[test]
fn cmcd_bundling_waits_for_a_delete_data_job_to_finish() {
    use fastly_log_analytics::admin::{DataReset, ResetPhase};
    let fake = Fake::default();
    let mut svc = service();
    cmcd_hour(&fake, BASE, 12);
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    svc.data_reset = Some(DataReset {
        job_id: "job1".into(),
        reset_at: (BASE + HOUR) as u64,
        phase: ResetPhase::Cleanup,
        started_at: (BASE + HOUR) as u64,
        actor: "tester".into(),
        rum_held: false,
    });
    for _ in 0..4 {
        run_cmcd(&fake, &svc);
    }
    assert!(
        fake.keys(CMCD_BUNDLES).is_empty(),
        "{:?}",
        fake.keys(CMCD_BUNDLES)
    );
    assert!(!fake.kv.borrow().contains_key(bundle::CMCD_STATE_KEY));
    svc.data_reset = None;
    for _ in 0..8 {
        run_cmcd(&fake, &svc);
    }
    assert!(fake
        .keys("hour_bundle=08-")
        .iter()
        .any(|k| k.starts_with(CMCD_BUNDLES)));
}

#[test]
fn stage_reserve_bounds_backend_calls_and_vcpu_per_execution() {
    let fake = Fake::default();
    let svc = service();
    let bodies = bodies();
    // 48 window summaries (1 minute of 6 plans x 8 shards) in a closed hour.
    minute(&fake, BASE, &bodies);
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    let sources: Vec<Vec<u8>> = fake.bucket.borrow().values().cloned().collect();
    let expected = bundle::merge(
        &sources.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        BASE,
        bundle::Unit::Hour,
    )
    .unwrap();

    let (mut runs, mut peak_calls, mut peak_vcpu) = (0, 0, 0);
    while hour_bundle(&fake, BASE).is_none() {
        runs += 1;
        assert!(runs <= 20, "not bundled after {runs} executions");
        fake.new_execution();
        // Simulate 2 startup backend calls before bundle_stage enters.
        fake.calls.set(2);
        let report =
            bundle::summary_bundle(&store(&fake, &svc), "ck", bundle::STAGE_CALL_RESERVE).unwrap();
        assert!(report.errors.is_empty(), "{report:?}");
        peak_calls = peak_calls.max(fake.calls.get());
        peak_vcpu = peak_vcpu.max(fake.vcpu.get());
    }
    let key = hour_bundle(&fake, BASE).unwrap();
    assert_eq!(fake.bucket.borrow()[&key], expected);
    assert!(peak_calls <= 22, "peak_calls={peak_calls} exceeded 22");
    assert!(peak_vcpu <= 600, "peak_vcpu={peak_vcpu}ms exceeded 600ms");
}

const RUM_BUNDLES: &str = "processed/summary_bundle/rum/";

fn rum_body(start: i64, seed: u32) -> Vec<u8> {
    use arrow_array::{
        builder::{Float64Builder, Int32Builder, StringBuilder, TimestampMillisecondBuilder},
        RecordBatch,
    };
    use std::sync::Arc;

    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut name = StringBuilder::new();
    let mut value = Float64Builder::new();
    let mut rating = StringBuilder::new();
    let mut pathname = StringBuilder::new();
    let mut browser = StringBuilder::new();
    let mut os = StringBuilder::new();
    let mut device = StringBuilder::new();
    let mut cid = StringBuilder::new();
    let mut req_id = StringBuilder::new();
    let mut city = StringBuilder::new();
    let mut region = StringBuilder::new();
    let mut country = StringBuilder::new();
    let mut pop = StringBuilder::new();
    let mut tls = StringBuilder::new();
    let mut resource_url = StringBuilder::new();
    let mut asn = StringBuilder::new();
    let mut ttfb = Float64Builder::new();
    let mut sample_rate = Float64Builder::new();

    for (m_name, m_val) in [
        ("LCP", 1500.0 + f64::from(seed * 10)),
        ("FCP", 900.0 + f64::from(seed * 5)),
        ("TTFB", 300.0 + f64::from(seed * 2)),
    ] {
        ts.append_value(start * 1000);
        name.append_value(m_name);
        value.append_value(m_val);
        rating.append_value("good");
        pathname.append_value("/home");
        browser.append_value("Chrome");
        os.append_value("macOS");
        device.append_value("desktop");
        cid.append_value(format!("c{seed}"));
        req_id.append_null();
        city.append_null();
        region.append_null();
        country.append_value("US");
        pop.append_null();
        tls.append_null();
        resource_url.append_null();
        asn.append_null();
        ttfb.append_null();
        sample_rate.append_value(1.0);
    }
    let vitals = RecordBatch::try_new(
        ingest::rum::vitals_schema(),
        vec![
            Arc::new(ts.finish()),
            Arc::new(name.finish()),
            Arc::new(value.finish()),
            Arc::new(rating.finish()),
            Arc::new(pathname.finish()),
            Arc::new(browser.finish()),
            Arc::new(os.finish()),
            Arc::new(device.finish()),
            Arc::new(cid.finish()),
            Arc::new(req_id.finish()),
            Arc::new(city.finish()),
            Arc::new(region.finish()),
            Arc::new(country.finish()),
            Arc::new(pop.finish()),
            Arc::new(tls.finish()),
            Arc::new(resource_url.finish()),
            Arc::new(asn.finish()),
            Arc::new(ttfb.finish()),
            Arc::new(sample_rate.finish()),
        ],
    )
    .unwrap();

    let errors = RecordBatch::try_new(
        ingest::rum::errors_schema(),
        vec![
            Arc::new(
                TimestampMillisecondBuilder::new()
                    .with_timezone("UTC")
                    .finish(),
            ),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(Int32Builder::new().finish()),
            Arc::new(Int32Builder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(StringBuilder::new().finish()),
            Arc::new(Float64Builder::new().finish()),
            Arc::new(Float64Builder::new().finish()),
        ],
    )
    .unwrap();
    ingest::rum_summary::write_summary(&[&vitals], &[&errors], start).unwrap()
}

fn rum_hour(fake: &Fake, hour: i64, minutes: usize) -> Vec<String> {
    let prefix = ingest::rum_summary::prefix("");
    let mut keys = Vec::new();
    for m in 0..minutes {
        let start = hour + (m as i64) * 60;
        let key = ingest::rum_summary::key(&prefix, start, start + 60, &format!("p{m}s0"), 3);
        let body = rum_body(start, m as u32 % 8);
        fake.bucket.borrow_mut().insert(key.clone(), body);
        keys.push(key);
    }
    keys
}

fn rum_expected(fake: &Fake, keys: &[String]) -> Vec<u8> {
    let mut sorted: Vec<&String> = keys.iter().collect();
    sorted.sort_by_key(|k| k.rsplit('/').next().unwrap().to_owned());
    let bucket = fake.bucket.borrow();
    let bodies: Vec<&[u8]> = sorted.iter().map(|k| bucket[*k].as_slice()).collect();
    ingest::rum_summary::merge_windows(&bodies).unwrap()
}

#[test]
fn rum_summaries_bundle_into_exact_hour_and_open_bundles_and_list_via_summary_view() {
    let fake = Fake::default();
    let svc = service();
    let closed = rum_hour(&fake, BASE, 12);
    let open = rum_hour(&fake, BASE + HOUR, 10);
    fake.now.set(BASE + HOUR + 30 * 60);
    watermark(&fake, fake.now.get());

    let mut runs = 0;
    while runs < 30 {
        runs += 1;
        fake.new_execution();
        let report = bundle::rum_summary_bundle(&store(&fake, &svc), "ck", RESERVE).unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let rum_has = |marker: &str| fake.keys(marker).iter().any(|k| k.starts_with(RUM_BUNDLES));
        if rum_has("hour_bundle=08-") && rum_has("open_bundle=09-") {
            break;
        }
    }
    let hour_key = fake
        .keys("hour_bundle=08-")
        .into_iter()
        .find(|k| k.starts_with(RUM_BUNDLES))
        .expect("closed RUM hour is bundled");
    assert!(hour_key.contains("hour_bundle=08-c12-n36-"), "{hour_key}");
    assert_eq!(
        fake.bucket.borrow()[&hour_key],
        rum_expected(&fake, &closed)
    );

    let open_key = fake
        .keys("open_bundle=09-")
        .into_iter()
        .find(|k| k.starts_with(RUM_BUNDLES))
        .expect("open RUM hour gets a snapshot");
    let name = bundle::parse_name(&open_key).unwrap();
    let mut sorted = open.clone();
    sorted.sort_by_key(|k| k.rsplit('/').next().unwrap().to_owned());
    assert_eq!(
        fake.bucket.borrow()[&open_key],
        rum_expected(&fake, &sorted[..name.count])
    );
    assert!(fake.kv.borrow().contains_key(bundle::RUM_STATE_KEY));

    // `GET /api/files?dataset=rum&view=summary` returns the hour_bundle instead of 12 per-minute summaries.
    fake.new_execution();
    let query = format!(
        "dataset=rum&view=summary&from={}&to={}",
        ingest::rfc3339(BASE),
        ingest::rfc3339(BASE + HOUR)
    );
    let resp = ingest::api::files(&fake, &svc, &query, std::time::Instant::now()).unwrap();
    let listed: Vec<&str> = resp["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["key"].as_str())
        .collect();
    assert_eq!(listed, vec![hour_key.as_str()]);
    assert_eq!(resp["complete"], true);
}

#[test]
fn rum_bundling_runs_in_bundle_datasets_and_waits_for_delete_data() {
    use fastly_log_analytics::admin::{DataReset, ResetPhase};
    let fake = Fake::default();
    let mut svc = service();
    rum_hour(&fake, BASE, 6);
    fake.now.set(BASE + 2 * HOUR);
    watermark(&fake, fake.now.get());
    svc.data_reset = Some(DataReset {
        job_id: "job1".into(),
        reset_at: (BASE + HOUR) as u64,
        phase: ResetPhase::Cleanup,
        started_at: (BASE + HOUR) as u64,
        actor: "tester".into(),
        rum_held: false,
    });
    fake.new_execution();
    let reports = bundle::bundle_datasets(&store(&fake, &svc), "ck", RESERVE, 4).unwrap();
    assert!(reports.rum.is_none());
    assert!(fake.keys(RUM_BUNDLES).is_empty());

    svc.data_reset = None;
    for _ in 0..8 {
        fake.new_execution();
        let reports = bundle::bundle_datasets(&store(&fake, &svc), "ck", RESERVE, 4).unwrap();
        if let Some(Ok(r)) = reports.rum {
            assert!(r.errors.is_empty(), "{:?}", r.errors);
        }
        if fake
            .keys("hour_bundle=08-")
            .iter()
            .any(|k| k.starts_with(RUM_BUNDLES))
        {
            break;
        }
    }
    assert!(fake
        .keys("hour_bundle=08-")
        .iter()
        .any(|k| k.starts_with(RUM_BUNDLES)));
}
