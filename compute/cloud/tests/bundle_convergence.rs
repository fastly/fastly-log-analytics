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
            "GET" => {
                self.charge(CATALOG_READ_MS)?;
                match self.bucket.borrow().get(request.key) {
                    Some(body) => ok(200, body.clone()),
                    None => ok(404, Vec::new()),
                }
            }
            "PUT" => {
                // Catalog JSON is cheap to serialize; parquet bundles and partials are encoded.
                self.charge(if request.key.contains("/_catalog/") {
                    0
                } else {
                    ENCODE_MS
                })?;
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
    let first = run_with_cpu(&fake, &svc, 450);
    assert_eq!(first.partials, 1);
    let partials = fake.keys("partial=");
    assert_eq!(partials.len(), 1);
    let partial = fake.bucket.borrow()[&partials[0]].clone();
    let gets = fake.gets.get();
    let puts = fake.puts.get();

    let deferred = run_with_cpu(&fake, &svc, 550);
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
    let first = run_with_cpu(&fake, &svc, 580);
    assert_eq!(first.partials, 1, "{first:?}");
    let partials = fake.keys("partial=");
    let puts = fake.puts.get();
    let second = run_with_cpu(&fake, &svc, 550);
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
    // Each run folds a batch: about one write per 12 windows, never one per window.
    assert!(fake.puts.get() * 12 <= windows, "{} PUTs", fake.puts.get());
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
    assert!(
        busy / runs as u64 <= VCPU_BUDGET_MS,
        "{} ms a run",
        busy / runs as u64
    );
}
