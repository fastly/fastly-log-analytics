use super::support::*;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use bytes::Bytes;
use fastly_log_analytics::{
    ingest::{self, api},
    telemetry::{LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

#[test]
fn sustained_load_of_sixty_files_a_minute_drains_without_backlog() {
    let fake = Fake::new(BASE);
    let mut expected = Vec::new();
    let mut max_calls = 0;
    let mut produced_until = BASE;
    for round in 0..12 {
        // Two minutes of 60 files each arrive between ticks.
        for _ in 0..2 {
            for i in 0..60 {
                let id = format!("t{produced_until}i{i:02}");
                put_raw(
                    &fake,
                    produced_until,
                    &format!("f{i:02}"),
                    &[
                        line(produced_until + i, &id),
                        line(produced_until + i, &format!("{id}b")),
                    ],
                );
                expected.push(id.clone());
                expected.push(format!("{id}b"));
            }
            produced_until += 60;
        }
        fake.now.set(produced_until as u64);
        let report = tick(&fake).unwrap();
        assert!(!report.busy, "round {round}");
        max_calls = max_calls.max(report.backend_calls);
        // Only objects that have not settled yet (the last three minutes) may wait.
        let watermark = fake
            .kv
            .borrow()
            .get(ingest::WATERMARK_KEY)
            .map(|(_, v)| v["key"].as_str().unwrap().to_owned())
            .unwrap_or_default();
        let unconverted = fake
            .keys("raw/request/")
            .into_iter()
            .filter(|key| key.as_str() > watermark.as_str())
            .count();
        assert!(
            unconverted <= 3 * 60,
            "round {round}: backlog {unconverted}"
        );
    }
    for _ in 0..3 {
        let report = tick_later(&fake).unwrap();
        max_calls = max_calls.max(report.backend_calls);
    }
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    assert!(committed(&fake));
    assert!(
        max_calls + ingest::TICK_RESERVE <= BUDGET,
        "coordinator {max_calls}"
    );
    assert!(fake.worker_max_calls.get() <= BUDGET);
    let files = rows(&fake, LogKind::IngestFiles);
    assert_eq!(files.len(), expected.len() / 2);
}

#[test]
fn a_crash_at_any_call_of_the_tick_never_duplicates_or_loses_rows() {
    for lost in [false, true] {
        let mut crash = 0;
        loop {
            let fake = Fake::new(BASE + 7200);
            let expected = seed(&fake, 3, 20);
            if lost {
                // One shard's answer is lost, so the tick saves partial results instead.
                fake.worker_faults
                    .borrow_mut()
                    .push_back(WorkerFault::LostReply);
            }
            fake.crash_after.set(Some(crash));
            let crashed = tick(&fake);
            let finished = crashed.is_ok() && fake.calls.get() < crash;
            fake.crash_after.set(None);
            for _ in 0..6 {
                tick_later(&fake).unwrap();
            }
            assert!(committed(&fake), "lost {lost} crash {crash}");
            assert_eq!(processed_ips(&fake), expected, "lost {lost} crash {crash}");
            // Telemetry of a tick that dies after its commit is lost with it, never doubled.
            let files = rows(&fake, LogKind::IngestFiles);
            let distinct: BTreeSet<&str> = files
                .iter()
                .map(|r| r["raw_key"].as_str().unwrap())
                .collect();
            assert_eq!(distinct.len(), files.len(), "lost {lost} crash {crash}");
            if finished {
                break;
            }
            crash += 1;
        }
        assert!(crash > 8, "every call index of a full tick was crashed");
    }
}

#[test]
fn lost_and_failed_worker_calls_are_retried_without_duplicates() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 3, 20);
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::LostReply,
        WorkerFault::Unreachable,
        WorkerFault::Ok,
    ]);
    let first = tick(&fake).unwrap();
    assert!(first.more_pending);
    assert_eq!(first.converted, 0);
    assert_eq!((first.shards_dispatched, first.shards_done), (3, 1));
    // An unanswered worker may still be writing: the lease holds until it expires.
    assert!(tick(&fake).unwrap().busy);
    let second = tick_later(&fake).unwrap();
    assert!(second.resumed);
    assert_eq!(second.shards_dispatched, 2);
    assert_eq!(second.converted, 60);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_shard_with_too_many_windows_for_its_budget_splits_before_writing() {
    let fake = Fake::new(BASE + 7200);
    let mut expected = Vec::new();
    for i in 0..10i64 {
        // All in one minute directory, but each spans 6 windows; 10 objects reach 15 windows,
        // more than the 8 windows (two PUTs each) one 10-object worker has room for.
        let lines: Vec<String> = (0..6)
            .map(|w| {
                let id = format!("o{i}w{w:02}");
                expected.push(id.clone());
                line(BASE + (i + w) * 60, &id)
            })
            .collect();
        put_raw(&fake, BASE, &format!("o{i}"), &lines);
    }
    expected.sort();
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(
        fake.keys("processed/").is_empty(),
        "nothing written before the split"
    );
    let shards = plan(&fake).unwrap()["shards"].clone();
    assert_eq!(
        shards.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["0a", "0b"]
    );
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    let token = format!("{}s0-", plan(&fake).unwrap()["plan_id"].as_str().unwrap());
    assert!(fake
        .keys("processed/request/")
        .iter()
        .all(|k| !k.contains(&token)));
}

#[test]
fn a_shard_failing_three_times_abandons_the_plan_and_replans_without_duplicates() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    // The first attempt writes its files but the answer is lost; two more attempts never arrive.
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::LostReply,
        WorkerFault::Unreachable,
        WorkerFault::Unreachable,
    ]);
    tick(&fake).unwrap();
    let stale = fake.keys("processed/request/");
    assert!(!stale.is_empty());
    assert!(!fake.keys("processed/_inflight/").is_empty());
    let old = plan(&fake).unwrap();
    tick_later(&fake).unwrap();
    tick_later(&fake).unwrap();
    assert_eq!(plan(&fake).unwrap()["abandoning"], "shard_failures");
    // The deciding tick leaves cleanup to a later tick; that one removes every stale file.
    let cleanup = tick_later(&fake).unwrap();
    assert_eq!(cleanup.converted, 0);
    assert!(fake.keys("processed/").is_empty());
    let replanned = plan(&fake).unwrap();
    assert_ne!(replanned["plan_id"], old["plan_id"]);
    assert_eq!(replanned["raw_keys"], old["raw_keys"]);
    assert_eq!(replanned["replans"], 1);
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    assert!(stale.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

#[test]
fn a_plan_from_another_schema_is_abandoned_and_its_outputs_replaced() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    let stale = fake.keys("processed/request/");
    assert!(!stale.is_empty());
    {
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::PLAN_KEY).unwrap();
        entry.1["schema"] = "0000000000000000".into();
    }
    let deciding = tick_later(&fake).unwrap();
    assert_eq!(deciding.shards_dispatched, 0);
    assert_eq!(plan(&fake).unwrap()["abandoning"], "schema_changed");
    tick_later(&fake).unwrap();
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_leftover_deleteq_of_an_abandoned_plan_is_removed_before_the_replan() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    let old = plan(&fake).unwrap();
    // A commit whose queue write landed but whose watermark CAS was lost.
    let leftover = format!("ingest:deleteq:{}", old["plan_id"].as_str().unwrap());
    fake.kv.borrow_mut().insert(
        leftover.clone(),
        (1, json!({"run_id": old["plan_id"], "keys": old["raw_keys"], "sizes": [], "inflight_keys": []})),
    );
    fake.kv.borrow_mut().get_mut(ingest::PLAN_KEY).unwrap().1["schema"] = "0000000000000000".into();
    tick_later(&fake).unwrap();
    tick_later(&fake).unwrap();
    assert!(!fake.kv.borrow().contains_key(&leftover));
    assert_ne!(plan(&fake).unwrap()["plan_id"], old["plan_id"]);
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    assert_eq!(deleteq(&fake).len(), 1, "only the committed plan's queue");
}

#[test]
fn a_live_lease_makes_another_tick_busy_without_writes() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 3);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::Unreachable);
    tick(&fake).unwrap();
    let before = fake.kv.borrow()[ingest::PLAN_KEY].clone();
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S - 1);
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert_eq!(fake.kv.borrow()[ingest::PLAN_KEY], before);
    assert_eq!(report.shards_dispatched, 0);
}

#[test]
fn a_watermark_conflict_at_commit_releases_the_lease_for_the_next_tick() {
    // Compaction commits to the same watermark key from its own execution, so a tick whose
    // shards are all done can lose the watermark CAS. That tick must hand the plan back with
    // its lease released: the next tick commits the finished results instead of sitting out
    // LEASE_S as busy.
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 3);
    fake.conflict_watermark.set(true);
    let lost = tick(&fake).unwrap();
    assert!(lost.busy);
    assert_eq!(lost.converted, 0);
    let saved = plan(&fake).expect("the finished plan is kept for the next tick");
    assert_eq!(
        saved["lease_until"], 0,
        "lease released on watermark conflict"
    );
    assert_eq!(saved["results"].as_object().unwrap().len(), 1);
    // A resumed plan with every shard done goes straight to commit; losing there too must
    // release the lease the resume just took.
    let lost_again = tick(&fake).unwrap();
    assert!(lost_again.busy);
    assert_eq!(lost_again.shards_dispatched, 0);
    let saved = plan(&fake).unwrap();
    assert_eq!(
        saved["lease_until"], 0,
        "lease released on a resumed commit conflict"
    );
    fake.conflict_watermark.set(false);
    let next = tick(&fake).unwrap();
    assert!(!next.busy, "the next tick must not wait out the lease");
    assert_eq!(next.converted, 3);
    assert_eq!(next.shards_dispatched, 0, "finished shards are not re-run");
}

#[test]
fn a_stale_plan_read_at_the_commit_fence_does_not_park_the_plan_for_a_lease() {
    // Seen live: a resumed, fully converted plan takes a fresh lease, then the fence re-read
    // answers with the record from before that write (KV is eventually consistent). The
    // attempt there is lower than ours, which cannot be a takeover; a takeover bumps it.
    // Treating it as one left the plan parked behind a 90 s lease, three times in 15 minutes.
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 3);
    fake.conflict_watermark.set(true);
    let lost = tick(&fake).unwrap();
    assert!(lost.busy);
    assert_eq!(plan(&fake).unwrap()["lease_until"], 0);
    fake.conflict_watermark.set(false);
    // The catalog worker reads the plan first; the fence read is the one answered stale.
    fake.stale_plan_reads.set(1);
    fake.stale_plan_skip.set(1);
    let resumed = tick(&fake).unwrap();
    assert_eq!(resumed.shards_dispatched, 0);
    assert_eq!(
        resumed.converted, 3,
        "a stale fence read must not block the commit"
    );
    assert!(!resumed.busy);
    assert!(committed(&fake), "the finished plan is committed");
    fake.now.set(fake.now.get() + 10);
    let next = tick(&fake).unwrap();
    assert!(!next.busy, "nothing is parked behind a lease");
}

#[test]
fn an_uncommitted_v1_plan_is_finished_once_in_tick_before_v2_planning() {
    let fake = Fake::new(BASE + 7200);
    let old = put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
    let new = put_raw(&fake, BASE + 60, "new", &[line(BASE + 60, "new")]);
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (
            3,
            json!({"run_id": "abcdefabcdef", "window_s": 60, "schema": "v3",
                "raw_keys": [old], "created_at": 1}),
        ),
    );
    let first = tick(&fake).unwrap();
    assert!(first.resumed);
    assert_eq!(first.converted, 1);
    assert_eq!(fake.worker_runs.get(), 0, "v1 plans finish in-tick");
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"],
        "abcdefabcdef"
    );
    let second = tick(&fake).unwrap();
    assert_eq!(second.converted, 1);
    assert_eq!(plan(&fake).unwrap()["version"], 2);
    assert_eq!(plan(&fake).unwrap()["raw_keys"], json!([new]));
    assert_eq!(processed_ips(&fake), ["new", "old"]);
}

#[test]
fn an_etag_that_is_not_the_body_md5_is_never_recorded() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 1, 3);
    *fake.bad_etag_for.borrow_mut() = Some(".parquet".into());
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    let saved = plan(&fake).unwrap();
    assert!(saved["results"].as_object().unwrap().is_empty());
    assert_eq!(saved["failures"]["0"], 1);
    assert!(rows(&fake, LogKind::PlatformErrors)
        .iter()
        .any(|r| r["code"] == "etag_mismatch"));
    *fake.bad_etag_for.borrow_mut() = None;
    assert_eq!(tick_later(&fake).unwrap().converted, 3);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn outputs_are_window_files_named_by_plan_and_shard_and_readers_skip_manifests() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 2, 2);
    tick(&fake).unwrap();
    let plan_id = plan(&fake).unwrap()["plan_id"].as_str().unwrap().to_owned();
    for key in fake.keys("processed/request/") {
        let name = ingest::parse_processed(&key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert!(key.contains(&format!("-{plan_id}s0-n")), "{key}");
    }
    let manifests = fake.keys("processed/_inflight/");
    assert_eq!(manifests, [format!("processed/_inflight/{plan_id}/0.json")]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let listing = files_page(&fake, &query).unwrap();
    assert_eq!(listing["files"].as_array().unwrap().len(), 2);
    assert!(!listing.to_string().contains("_inflight"));
}

#[test]
fn worker_ingest_authenticates_validates_and_reports_schema_mismatch() {
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": [key], "stop_ms": 20_000,
    });
    let call = |header: &str, body: &Value| {
        fake.new_request();
        ingest::worker::handle_worker_ingest(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".to_owned(), header.to_owned())].into(),
                body: body.to_string().into_bytes(),
            },
        )
    };
    assert_eq!(call("wrong", &body).status, 403);
    for (field, value) in [
        ("keys", json!(["processed/request/x.zst"])),
        ("keys", json!([])),
        ("plan_id", json!("../x")),
        ("prefix", json!("/abs/")),
        ("shard", json!("A")),
    ] {
        let mut bad = body.clone();
        bad[field] = value;
        assert_eq!(call("fos-secret", &bad).status, 400, "{field}");
    }
    let mut other = body.clone();
    other["schema"] = "ffffffffffffffff".into();
    let reply: Value = serde_json::from_slice(&call("fos-secret", &other).body).unwrap();
    assert_eq!(reply["status"], "schema_mismatch");
    assert!(fake.keys("processed/").is_empty());
    let reply: Value = serde_json::from_slice(&call("fos-secret", &body).body).unwrap();
    assert_eq!(reply["status"], "ok");
    assert!(reply["calls"].as_u64().unwrap() <= u64::from(BUDGET));
    assert_eq!(reply["result"]["objects"][0]["status"], "converted");
    let summary = reply["summary_vcpu_ms"].as_u64().unwrap();
    assert!(summary > 0 && summary < reply["vcpu_ms"].as_u64().unwrap());
    assert!(fake.kv.borrow().is_empty(), "the worker writes no KV");
}

#[test]
fn worker_routes_untimestamped_rows_by_the_plan_time_so_retries_write_the_same_keys() {
    let fake = Fake::new(BASE + 7200);
    let key = "raw/request/odd/object.zst".to_owned();
    let untimed = json!({"ip": "a", "status": 200}).to_string();
    fake.bucket.borrow_mut().insert(key.clone(), gz(&[untimed]));
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": [key], "stop_ms": 20_000, "created_at": BASE,
    });
    let mut written = Vec::new();
    for now in [BASE + 7200, BASE + 7200 + 3600] {
        fake.now.set(now as u64);
        fake.new_request();
        let response = ingest::worker::handle_worker_ingest(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                body: body.to_string().into_bytes(),
            },
        );
        let reply: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(reply["status"], "ok", "{reply}");
        written.push(fake.keys("processed/request/"));
    }
    assert_eq!(written[0].len(), 1);
    assert_eq!(written[0], written[1]);
    let name = ingest::parse_processed(&written[0][0]).unwrap();
    assert_eq!(name.start, BASE - BASE.rem_euclid(60));
}

#[test]
fn extra_worker_requests_ride_the_same_dispatch_batch() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 2);
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let options = ingest::coordinator::Options {
        extra: vec![("/_internal/echo".into(), json!({"stage": "log_flush"}))],
        ..ingest::coordinator::Options::request(&fake)
    };
    let report = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap();
    assert_eq!(report.converted, 2);
    assert_eq!(report.extra_results.len(), 1);
    assert_eq!(
        report.extra_results[0].as_ref().unwrap()["path"],
        "/_internal/echo"
    );
    // Nothing to ingest: the extras still go out, alone.
    fake.new_request();
    let alone = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap();
    assert_eq!(alone.converted, 0);
    assert!(alone.extra_results[0].is_ok());
}

#[test]
fn raw_delete_requeues_manifests_it_could_not_delete() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let manifest = fake.keys("processed/_inflight/")[0].clone();
    fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
    let report = delete_tick(&fake, &service());
    assert_eq!(report.deleted, 2);
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["keys"], json!([]));
    assert_eq!(queued[0]["inflight_keys"], json!([manifest]));
    fake.fail_delete_keys.borrow_mut().clear();
    delete_tick(&fake, &service());
    assert!(deleteq(&fake).is_empty());
    assert!(fake.keys("processed/_inflight/").is_empty());
    assert_eq!(processed_ips(&fake), ["a", "a"]);
}

/// F5 and the 1 s dispatcher: the front service's `/_tick` and `/_flush` probes reach flp every
/// second, fast-forwarded over a 52-files-per-period load with a 60 s log period.
mod simulation {
    use super::*;
    use fastly_log_analytics::admin::logs::MAINT_STAGES;
    use fastly_log_analytics::admin::schedule::MAINT_INTERVAL_S;
    use fastly_log_analytics::ingest::retention::SWEEP_INTERVAL_S;
    use std::collections::VecDeque;

    const PERIOD_S: i64 = 60;
    const WINDOW_S: i64 = 120;
    const FILES_PER_PERIOD: usize = 52;
    const LINES_PER_FILE: usize = 2;
    /// A requested maintenance stage runs within this many seconds: one stage dispatch every
    /// `MAINT_INTERVAL_S`, round-robin over the stages, plus a tick.
    const TURN_S: i64 = MAINT_STAGES.len() as i64 * MAINT_INTERVAL_S as i64 + 1;
    const RECONCILE_KEY: &str = "storage:reconcile";
    const HISTORY_KEY: &str = "logs:delete_history";
    /// A fresh cursor starts `MAX_SCAN_MIN` back with the `global` sid as well, then scans
    /// three minutes a flush; it has caught up by then.
    const WARMUP_S: i64 = 3 * 3600;
    /// Outbox keys left at the end of a day: one per 2-minute bucket, waiting up to the default
    /// `flush_max_age_s` (an hour, 30 buckets) when nothing else makes the flush due, plus the
    /// two buckets inside the read grace and the odd rollover or overflow key.
    const OUTBOX_BOUND: usize = 3600 / 120 + 6;
    /// Period close until its raw objects are converted and deleted. Request ingest has no
    /// grace (`INGEST_GRACE_S` is 0), so this is normally the first tick after the close (1 s
    /// here); the bound is loose and fails only when a period waits about two minutes.
    const LATENCY_BOUND_S: i64 = 125;

    #[derive(Default)]
    struct Day {
        raw_pending: usize,
        logq_pending: usize,
        flush_commits: usize,
        ops_files: usize,
        /// Most minutes the flush cursor trailed the clock after any flush past the warm-up.
        max_scan_lag_min: u64,
        /// Executions that ran a job, and probes that ended at the schedule claim.
        tick_runs: usize,
        idle_ticks: usize,
        flush_runs: usize,
        /// Seconds from a period's close until its raw objects were converted and deleted.
        max_latency_s: i64,
        latency_sum_s: i64,
        latency_n: i64,
    }

    #[derive(Default)]
    struct Sim {
        days: Vec<Day>,
        /// Stage-worker turns by stage, as simulated seconds.
        turns: BTreeMap<String, Vec<i64>>,
        /// Most seconds a requested reconcile or history job waited for its first turn.
        max_wait: BTreeMap<&'static str, i64>,
        /// Most seconds the completed-sweep marker aged.
        max_sweep_age_s: i64,
        expected: Vec<String>,
        /// Admin audit rows written, from `audit:` keys and the service's audit outbox.
        audits: usize,
    }

    /// Durable outbox keys of this service, whatever their layout.
    fn outbox_depth(fake: &Fake) -> usize {
        fake.kv
            .borrow()
            .keys()
            .filter(|k| k.starts_with(&format!("logq_{SID}_")))
            .count()
    }

    /// One admin login (`audit:<seq>`) and one service config change (svc audit outbox).
    fn audit(fake: &Fake, seq: usize) {
        let now = fake.now.get();
        let mut kv = fake.kv.borrow_mut();
        kv.insert(
            format!("audit:{seq:020}"),
            (1, json!({"time": now, "actor": "admin", "action": "login"})),
        );
        let (generation, svc) = kv.get_mut(&format!("svc:{SID}")).unwrap();
        *generation += 1;
        if !svc["_audit_outbox"].is_array() {
            svc["_audit_outbox"] = json!([]);
        }
        svc["_audit_outbox"]
            .as_array_mut()
            .unwrap()
            .push(json!({"actor": "admin", "at": now, "event": "config", "config_version": seq}));
    }

    fn archived_audits(fake: &Fake) -> usize {
        fake.keys("processed/ops/admin_audit/")
            .iter()
            .flat_map(|key| batches(&fake.bucket.borrow()[key]))
            .map(|batch| batch.num_rows())
            .sum()
    }

    fn start(fake: &Fake) {
        tick_runtime(fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["log_period"] = json!(PERIOD_S);
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        *fake.window.borrow_mut() = Some(WINDOW_S.to_string());
    }

    /// One log period: Fastly writes `FILES_PER_PERIOD` objects for the period just closed.
    /// Returns one of them, to time the period's conversion.
    fn deliver(fake: &Fake, period_n: usize, expected: &mut Vec<String>) -> String {
        let period = fake.now.get() as i64 - PERIOD_S;
        let mut first = String::new();
        for file in 0..FILES_PER_PERIOD {
            let lines: Vec<String> = (0..LINES_PER_FILE)
                .map(|n| {
                    let ip = format!("{period_n:05}-{file:02}-{n}");
                    expected.push(ip.clone());
                    line(period + (file as i64 % PERIOD_S), &ip)
                })
                .collect();
            let key = put_raw(fake, period, &format!("t{period_n:05}f{file:02}"), &lines);
            if file == 0 {
                first = key;
            }
        }
        first
    }

    type KvRecord = Option<(u64, Value)>;

    fn record(fake: &Fake, key: &str) -> KvRecord {
        fake.kv.borrow().get(key).cloned()
    }

    fn simulate(fake: &Fake, seconds: i64) -> Sim {
        start(fake);
        // Start on a period boundary, as Fastly's log periods are aligned.
        fake.now
            .set(fake.now.get() / PERIOD_S as u64 * PERIOD_S as u64);
        let mut sim = Sim::default();
        // A requested stage's KV record changes on its first turn.
        let mut asked: BTreeMap<&'static str, (i64, KvRecord)> = BTreeMap::new();
        let begun = fake.now.get() as i64;
        let cursor_key = format!("logs:cursor:{SID}");
        let mut txn = Value::Null;
        let mut day = Day::default();
        let mut waiting: VecDeque<(i64, String)> = VecDeque::new();
        for second in 1..=seconds {
            fake.now.set(fake.now.get() + 1);
            let now = fake.now.get() as i64;
            if second % PERIOD_S == 0 {
                let period_n = (second / PERIOD_S) as usize;
                waiting.push_back((now, deliver(fake, period_n, &mut sim.expected)));
                if period_n % 30 == 7 {
                    audit(fake, period_n);
                    sim.audits += 2;
                }
            }
            if second % DAY == DAY / 3 {
                fake.new_request();
                assert!(ingest::retention::request_storage_reconcile(fake, "admin").unwrap());
                asked.insert(RECONCILE_KEY, (second, record(fake, RECONCILE_KEY)));
            }
            if second % DAY == DAY / 2 {
                fake.kv.borrow_mut().insert(
                    HISTORY_KEY.into(),
                    (
                        1,
                        json!({"kind": "cron_runs", "older_than_days": 30,
                               "cutoff_ts": (now - 30 * DAY) / 3600 * 3600,
                               "status": "deleting", "actor": "admin", "started_at": now,
                               "updated_at": now, "deleted": 0, "error": null}),
                    ),
                );
                asked.insert(HISTORY_KEY, (second, record(fake, HISTORY_KEY)));
            }
            let before = fake.stage_runs.borrow().len();
            let report = run_tick(fake);
            assert!(report["ok"] == true, "{report}");
            if report.get("run").is_some() {
                day.tick_runs += usize::from(!report["run"].is_null());
                for run in &fake.stage_runs.borrow()[before..] {
                    if let Some(ran) = run["ran"].as_str() {
                        sim.turns.entry(ran.to_owned()).or_default().push(second);
                    }
                }
                while waiting
                    .front()
                    .is_some_and(|(_, key)| !fake.bucket.borrow().contains_key(key))
                {
                    let (closed, _) = waiting.pop_front().unwrap();
                    let latency = now - closed;
                    day.max_latency_s = day.max_latency_s.max(latency);
                    day.latency_sum_s += latency;
                    day.latency_n += 1;
                }
                let changed: Vec<&'static str> = asked
                    .iter()
                    .filter(|(key, (_, seen))| record(fake, key) != *seen)
                    .map(|(key, _)| *key)
                    .collect();
                for key in changed {
                    asked.remove(key);
                }
            } else {
                assert_eq!(fake.calls.get(), 0, "an idle tick made a backend call");
                day.idle_ticks += 1;
            }
            for (key, (at, _)) in &asked {
                let wait = sim.max_wait.entry(key).or_default();
                *wait = (*wait).max(second - *at);
            }
            if second % 60 == 0 {
                let marker = record(fake, "retention:last_sweep").map_or(Value::Null, |r| r.1);
                let swept = marker["at"].as_i64().unwrap_or(begun);
                sim.max_sweep_age_s = sim.max_sweep_age_s.max(now - swept);
            }

            let flushed = run_flush(fake);
            if flushed.get("configured").is_some() {
                day.flush_runs += 1;
                let cursor = record(fake, &cursor_key);
                let committed = cursor
                    .as_ref()
                    .map_or(Value::Null, |c| c.1["txn_id"].clone());
                if !committed.is_null() && committed != txn {
                    day.flush_commits += 1;
                    txn = committed;
                }
                let scan_min = cursor.map_or(0, |c| c.1["scan_min"].as_u64().unwrap_or(0));
                if second >= WARMUP_S {
                    day.max_scan_lag_min = day
                        .max_scan_lag_min
                        .max((now as u64 / 60).saturating_sub(scan_min));
                }
            } else {
                assert_eq!(
                    fake.calls.get(),
                    0,
                    "an idle flush probe made a backend call"
                );
            }
            if second % DAY == 0 || second == seconds {
                day.raw_pending = fake.keys("raw/").len();
                day.logq_pending = outbox_depth(fake);
                day.ops_files = fake.keys("processed/ops/").len();
                sim.days.push(std::mem::take(&mut day));
            }
        }
        sim
    }

    fn report(fake: &Fake, sim: &Sim) {
        for (n, day) in sim.days.iter().enumerate() {
            eprintln!(
                "day {n}: raw pending {}, logq {}, flush commits {}, max scan lag {} min, \
                 ops files {}, tick runs {}, idle ticks {}, flush runs {}, \
                 ingest latency max {} s mean {} s",
                day.raw_pending,
                day.logq_pending,
                day.flush_commits,
                day.max_scan_lag_min,
                day.ops_files,
                day.tick_runs,
                day.idle_ticks,
                day.flush_runs,
                day.max_latency_s,
                day.latency_sum_s / day.latency_n.max(1),
            );
        }
        for (kind, (calls, ops)) in fake.peaks.borrow().iter() {
            eprintln!("{kind}: max {calls} backend calls, {ops} cache ops");
        }
        for (stage, turns) in &sim.turns {
            eprintln!("{stage}: {} turns", turns.len());
        }
        eprintln!(
            "max waits {:?} s, max sweep age {} s",
            sim.max_wait, sim.max_sweep_age_s
        );
    }

    fn assert_within_budgets(fake: &Fake) {
        for (kind, (calls, ops)) in fake.peaks.borrow().iter() {
            assert!(*calls <= BUDGET, "{kind} used {calls} calls");
            assert!(
                *ops <= fastly_log_analytics::shell::CACHE_OP_BUDGET,
                "{kind} used {ops} cache ops"
            );
        }
    }

    #[test]
    fn the_log_flush_commits_and_keeps_up_under_sustained_ingest() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, DAY / 4);
        report(&fake, &sim);
        let day = &sim.days[0];
        assert!(
            day.flush_commits >= 10,
            "{} flush commits",
            day.flush_commits
        );
        assert!(
            day.max_scan_lag_min <= 10,
            "flush lag {} min",
            day.max_scan_lag_min
        );
        assert_within_budgets(&fake);
    }

    #[test]
    #[ignore = "soak: run with `make -C app soak`"]
    fn every_maintenance_stage_gets_a_turn_within_four_intervals_under_load() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, DAY);
        report(&fake, &sim);
        for key in [RECONCILE_KEY, HISTORY_KEY] {
            let wait = sim.max_wait.get(key).copied().unwrap_or(i64::MAX);
            assert!(wait <= TURN_S, "{key} waited {wait} s");
        }
        assert!(
            sim.max_sweep_age_s <= SWEEP_INTERVAL_S as i64 + TURN_S + 60,
            "the sweep went {} s without completing",
            sim.max_sweep_age_s
        );
        for stage in MAINT_STAGES.iter().filter(|stage| **stage != "delete_data") {
            assert!(sim.turns.contains_key(*stage), "{stage} never ran");
        }
    }

    #[test]
    #[ignore = "soak: run with `make -C app soak`"]
    fn two_days_of_1s_ticks_at_52_files_per_period_convert_exactly_without_a_backlog() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, 2 * DAY);
        report(&fake, &sim);
        assert_within_budgets(&fake);
        for (n, day) in sim.days.iter().enumerate() {
            assert!(
                day.raw_pending <= 3 * FILES_PER_PERIOD,
                "backlog on day {n}: {}",
                day.raw_pending
            );
            assert!(day.flush_commits > 0, "no flush committed on day {n}");
            assert!(
                day.max_latency_s <= LATENCY_BOUND_S,
                "ingest latency on day {n}: {} s",
                day.max_latency_s
            );
            // About one run per period plus maintenance every 120 s; every other tick is idle.
            assert!(
                day.tick_runs <= (DAY / PERIOD_S + DAY / MAINT_INTERVAL_S as i64) as usize * 2,
                "{} tick runs on day {n}",
                day.tick_runs
            );
            assert!(
                day.logq_pending <= OUTBOX_BOUND,
                "outbox depth on day {n}: {}",
                day.logq_pending
            );
        }
        assert!(sim.days.windows(2).all(|d| d[1].ops_files > d[0].ops_files));
        // Drain: no new deliveries until the last period is converted and its raws deleted.
        for _ in 0..300 {
            fake.now.set(fake.now.get() + 1);
            run_tick(&fake);
            run_flush(&fake);
        }
        assert!(fake.keys("raw/").is_empty());
        let mut expected = sim.expected;
        expected.sort();
        let got = processed_ips(&fake);
        assert_eq!(got.len(), expected.len(), "missing or duplicate rows");
        assert!(got == expected, "the Parquet rows differ from the input");
        // Past the default flush_max_age_s, only the flush's own rows since its last commit remain
        // queued: each commit's row waits for the next one, at most the max age later.
        for _ in 0..3600 + 600 {
            fake.now.set(fake.now.get() + 1);
            run_flush(&fake);
        }
        let now = fake.now.get();
        let left: Vec<(String, Value)> = fake
            .kv
            .borrow()
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("logq_{SID}_")))
            .map(|(k, v)| (k.clone(), v.1.clone()))
            .collect();
        assert!(left.len() <= 3, "the outbox drains: {} keys", left.len());
        for (key, value) in &left {
            for line in value["ndjson"].as_str().unwrap().lines() {
                let row: Value = serde_json::from_str(line).unwrap();
                assert_eq!(row["cron_kind"], "log_flush", "{key} holds {line}");
                assert!(
                    row["bucket"].as_u64().unwrap() * 120 + 3600 + 240 >= now,
                    "{key} is stale"
                );
            }
        }
        assert_eq!(
            archived_audits(&fake),
            sim.audits,
            "every audit row archived once"
        );
    }
}

/// Task 22.2: closed hours' window summaries merge into hour bundles, closed days into day
/// bundles, bounded per execution and resumable.
mod summary_bundles {
    use super::*;
    use fastly_log_analytics::ingest::{bundle, convert, summary};
    use serde_json::Map;

    const HOUR: i64 = 3600;
    const WINDOWS: &str = "processed/summary/request/";
    const BUNDLES: &str = "processed/summary_bundle/request/";

    fn pick<T: Copy>(values: &[T], i: i64) -> T {
        values[i as usize % values.len()]
    }

    fn line(at_ms: i64, i: i64) -> String {
        let mut row = json!({
            "timestamp": time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(at_ms) * 1_000_000)
                .unwrap()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            "status": pick(&[200, 200, 404, 503, 401, 0], i),
            "cache": pick(&["HIT", "MISS", "PASS", "HIT-CLUSTER", "SYNTH"], i),
            "resp_bytes": 100 + i * 7,
            "elapsed": 5_000 + i * 13,
            "ttfb": 0.001 * (i % 17) as f64,
            "ottfb": 2_000 + i,
            "ottlb": 4_000 + i * 2,
            "waf": i % 5 == 0,
            "waf_resp": pick(&[406, 200], i),
            "waf_sig": pick(&["a, b", "", "c"], i),
            "country": pick(&["US", "DE", "JP"], i),
            "asn": pick(&[13335, 15169], i),
            "tcp_rtt": i % 50,
            "edge": i % 4 != 0,
            "ua": format!("ua{}", i % 5),
            "pop": pick(&["IAD", "LHR", "NRT"], i),
            "ost": pick(&[200, 502], i),
            "ploss": (i % 10) as f64 / 100.0,
            "url": if i % 13 == 0 { "/login".to_string() } else { format!("/p{}", i % 23) },
        });
        if i % 11 != 0 {
            row["ip"] = format!("10.0.{}.{}", i % 3, i % 41).into();
        }
        if i % 9 != 0 {
            row["ja4"] = pick(&["t13d", "t12d"], i).into();
        }
        row.to_string()
    }

    /// Writes the summary of `n` rows in the minute window at `start`; returns its batch.
    fn window(fake: &Fake, start: i64, shard: u32, n: i64) -> RecordBatch {
        let lines: Vec<String> = (0..n)
            .map(|i| {
                line(
                    start * 1000 + i * 977 % 60_000,
                    i + start + i64::from(shard) * 7,
                )
            })
            .collect();
        let rows = convert::convert_object(&gz(&lines), 60, start).unwrap();
        let batch = rows.windows.into_values().next().unwrap();
        let key = summary::summary_key(
            WINDOWS,
            start,
            start + 60,
            &format!("p1s{shard}"),
            batch.num_rows() as u64,
        );
        fake.bucket.borrow_mut().insert(
            key.clone(),
            summary::write_summary(&[&batch], start).unwrap(),
        );
        let bytes = fake.bucket.borrow()[&key].len() as u64;
        publish_outputs(
            fake,
            &[],
            &[ingest::worker::Output {
                key: key.clone(),
                rows: batch.num_rows() as u64,
                bytes,
                ..Default::default()
            }],
            &[],
        );
        batch
    }

    fn watermark(fake: &Fake, at: i64) {
        let previous = fake.kv.borrow().get(ingest::WATERMARK_KEY).cloned();
        let mut value = previous
            .as_ref()
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| json!({}));
        value["key"] = raw_key(at - at % 60, "zzz").into();
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (previous.map_or(1, |(generation, _)| generation + 1), value),
        );
    }

    fn run(fake: &Fake) -> bundle::BundleReport {
        fake.new_request();
        let svc = service();
        let report = bundle::summary_bundle(&store(fake, &svc), "ck", 1).unwrap();
        assert!(fake.calls.get() <= BUDGET, "{} calls", fake.calls.get());
        report
    }

    fn settle(fake: &Fake) -> u32 {
        for n in 1..=40 {
            if !run(fake).more_pending {
                return n;
            }
        }
        panic!("bundling did not settle");
    }

    fn read(bytes: &[u8]) -> Vec<Map<String, Value>> {
        use arrow_array::{BinaryArray, BooleanArray, Float64Array};
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
        let mut out = Vec::new();
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let mut row = Map::new();
                for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                    if column.is_null(i) {
                        continue;
                    }
                    let any = column.as_any();
                    let value: Value = if let Some(a) = any.downcast_ref::<StringArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<Float64Array>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<BooleanArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
                        hex::encode(a.value(i)).into()
                    } else {
                        panic!("unexpected column type {}", field.name());
                    };
                    row.insert(field.name().clone(), value);
                }
                out.push(row);
            }
        }
        out
    }

    /// Bundle keys whose file name starts with `marker`.
    fn of(fake: &Fake, marker: &str) -> Vec<String> {
        fake.keys(BUNDLES)
            .into_iter()
            .filter(|k| k.rsplit('/').next().unwrap().starts_with(marker))
            .collect()
    }

    fn one_of(fake: &Fake, marker: &str) -> String {
        let keys = of(fake, marker);
        assert_eq!(keys.len(), 1, "{keys:?}");
        keys[0].clone()
    }

    type Rows = Vec<Map<String, Value>>;

    /// Pivot slice rows in a stable order, and the other rows in file order.
    fn split_slices(rows: Rows) -> (Rows, Rows) {
        let (mut slices, rest): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| {
            let b = r["bundle"].as_str().unwrap();
            b == "pivot_totals" || b.starts_with("slice_")
        });
        let sort_key = |r: &Map<String, Value>| {
            ["bundle", "pivot", "pivot_value", "field", "bucket", "value"]
                .map(|k| r.get(k).map(Value::to_string).unwrap_or_default())
        };
        slices.sort_by_key(sort_key);
        (slices, rest)
    }

    /// Row-by-row equality; floats to 1e-9 relative, and a merged sketch's estimate stands in
    /// for the exact distinct count a single-pass summary records.
    fn assert_same(got: &[Map<String, Value>], want: &[Map<String, Value>]) {
        assert_eq!(got.len(), want.len());
        for (n, (g, w)) in got.iter().zip(want).enumerate() {
            let mut w = w.clone();
            if let (Some(sketch), true) = (
                w.get("ip_sketch").and_then(Value::as_str),
                w.contains_key("ip_count_observed"),
            ) {
                let hll = summary::Hll::from_bytes(&hex::decode(sketch).unwrap()).unwrap();
                w.insert("ip_count_observed".into(), (hll.count() as i64).into());
            }
            assert_eq!(
                g.keys().collect::<Vec<_>>(),
                w.keys().collect::<Vec<_>>(),
                "row {n}"
            );
            for (k, a) in g {
                let b = &w[k];
                if a.is_f64() && b.is_f64() {
                    let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
                    assert!(
                        (a - b).abs() <= 1e-9 * b.abs().max(1.0),
                        "row {n} {k}: {a} != {b}"
                    );
                } else {
                    assert_eq!(a, b, "row {n} {k}: {g:?}");
                }
            }
        }
    }

    fn seed_hour(fake: &Fake, hour: i64, minutes: i64) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        for m in 0..minutes {
            batches.push(window(fake, hour + m * 60, 0, 20 + m));
            if m == 5 {
                batches.push(window(fake, hour + m * 60, 1, 9));
            }
        }
        batches
    }

    /// Every `view=summary` page for `[from, to)` with `spare` calls left per execution, as
    /// `(kind, key, rows)` in order; asserts no key repeats.
    fn summary_view(fake: &Fake, from: i64, to: i64, spare: u32) -> Vec<(String, String, u64)> {
        let base = format!("view=summary&from={}&to={}", iso_url(from), iso_url(to));
        let (mut cursor, mut out) = (None::<String>, Vec::new());
        for _ in 0..50 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            fake.new_request();
            fake.calls.set(BUDGET - spare);
            let page = api::files(fake, &service(), &query, Instant::now()).unwrap();
            assert_eq!(page["approximate"]["top_k"]["hour"], bundle::HOUR_TOP_K);
            for f in page["files"].as_array().unwrap() {
                let key = f["key"].as_str().unwrap().to_owned();
                assert!(out.iter().all(|(_, k, _)| *k != key), "{key} repeated");
                out.push((
                    f["kind"].as_str().unwrap().to_owned(),
                    key,
                    f["rows"].as_u64().unwrap(),
                ));
            }
            if page["complete"] == true {
                assert!(page["next_cursor"].is_null());
                return out;
            }
            cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        }
        panic!("summary view did not complete");
    }

    fn kinds(view: &[(String, String, u64)]) -> Vec<&str> {
        view.iter().map(|(kind, _, _)| kind.as_str()).collect()
    }

    fn day_rows(fake: &Fake) -> u64 {
        fake.keys(WINDOWS)
            .iter()
            .filter(|k| k.contains("/day=15/"))
            .map(|k| {
                k.rsplit_once("-n")
                    .unwrap()
                    .1
                    .trim_end_matches(".parquet")
                    .parse::<u64>()
                    .unwrap()
            })
            .sum()
    }

    #[test]
    fn summary_bundles_do_not_suppress_window_fallbacks_for_fractional_range_boundaries() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        settle(&fake);
        let from = ingest::rfc3339(BASE).replace('Z', ".500Z");
        let page = files_page(
            &fake,
            &format!("view=summary&from={from}&to={}", iso_url(BASE + HOUR)),
        )
        .unwrap();
        let files = page["files"].as_array().unwrap();
        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|file| file["kind"] == "window"));
        window(&fake, BASE + HOUR, 0, 1);
        let to = ingest::rfc3339(BASE + HOUR).replace('Z', ".500Z");
        let page = files_page(
            &fake,
            &format!("view=summary&from={}&to={to}", iso_url(BASE)),
        )
        .unwrap();
        let files = page["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["kind"], "hour");
        assert_eq!(files[1]["kind"], "window");
    }

    #[test]
    fn catalog_delta_reconciles_the_summary_boundary_without_replaying_raw_history() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        settle(&fake);
        window(&fake, BASE + HOUR, 0, 20);
        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"]
            .as_u64()
            .unwrap();
        let old = summary_view(&fake, BASE, BASE + HOUR, 30);
        assert_eq!(kinds(&old), ["hour"]);
        let query=format!("summaries=1&from={}&to={}&since_generation={generation}&previous_from={}&previous_to={}",
            iso_url(BASE+60),iso_url(BASE+HOUR+60),iso_url(BASE),iso_url(BASE+HOUR));
        let page = files_page(&fake, &query).unwrap();
        assert_eq!(page["complete"], true);
        assert_eq!(page["changes"].as_array().unwrap().len(), 0);
        assert_eq!(page["files"].as_array().unwrap().len(), 0);
        assert_eq!(page["summary_partitions"].as_array().unwrap().len(), 1);
        let summaries = page["summary_files"].as_array().unwrap();
        assert_eq!(summaries.len(), 3);
        assert!(summaries.iter().all(|file| file["kind"] == "window"));
        assert_eq!(
            summaries
                .iter()
                .map(|file| file["rows"].as_u64().unwrap())
                .sum::<u64>(),
            63
        );
    }

    #[test]
    fn catalog_retention_preserves_a_pinned_bundle_reader_through_the_retirement_grace() {
        let day = BASE - BASE.rem_euclid(86_400);
        let fake = Fake::new(day + 2 * 86_400 - 10);
        watermark(&fake, day + 2 * 86_400 - 10);
        seed_hour(&fake, BASE, 2);
        settle(&fake);
        let query = format!(
            "view=summary&from={}&to={}",
            iso_url(day),
            iso_url(day + 86_400)
        );
        fake.new_request();
        fake.calls.set(BUDGET - 5);
        let first = api::files(&fake, &service(), &query, Instant::now()).unwrap();
        assert_eq!(first["complete"], false);
        let cursor = first["next_cursor"].as_str().unwrap();
        let canonical = one_of(&fake, "day_bundle=");
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.now.set((day + 2 * 86_400 + 10) as u64);
        fake.new_request();
        ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
            .unwrap();
        assert!(fake.bucket.borrow().contains_key(&canonical));
        fake.new_request();
        let pinned = api::files(
            &fake,
            &svc,
            &format!("{query}&cursor={cursor}"),
            Instant::now(),
        )
        .unwrap();
        assert_eq!(pinned["catalog_generation"], first["catalog_generation"]);
        assert_eq!(pinned["files"][0]["key"], canonical);
        let current = summary_view(&fake, day, day + 86_400, 30);
        assert!(current.is_empty());
        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        for _ in 0..10 {
            fake.new_request();
            let report = ingest::retention::retention_sweep(
                &store(&fake, &svc),
                &Default::default(),
                4,
                None,
            )
            .unwrap();
            if !report.more_pending {
                break;
            }
        }
        assert!(!fake.bucket.borrow().contains_key(&canonical));
    }

    #[test]
    fn v2_summary_catalog_serves_committed_day_coverage_and_late_windows_exactly_once() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 2);
        seed_hour(&fake, BASE + HOUR, 1);
        settle(&fake);
        let day = BASE - BASE.rem_euclid(86_400);
        let before = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&before), ["day"]);
        assert_eq!(before.iter().map(|f| f.2).sum::<u64>(), 61);
        window(&fake, BASE + 120, 9, 7);
        let after = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&after), ["day", "window"]);
        assert_eq!(after.iter().map(|f| f.2).sum::<u64>(), 68);
        let requests: i64 = after
            .iter()
            .flat_map(|(_, key, _)| read(&fake.bucket.borrow()[key]))
            .filter(|row| row["bundle"] == "overview")
            .map(|row| row["requests"].as_i64().unwrap())
            .sum();
        assert_eq!(requests, 68);
        let partial = summary_view(&fake, BASE + 60, BASE + HOUR, 30);
        assert_eq!(kinds(&partial), ["window", "window"]);
        assert_eq!(partial.iter().map(|f| f.2).sum::<u64>(), 28);
    }

    #[test]
    fn summary_view_uses_committed_bundles_without_duplicate_window_coverage() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let open = window(&fake, BASE + 16 * HOUR + 60, 0, 5);
        let day = BASE - 8 * HOUR;
        let view = summary_view(&fake, day, BASE + 17 * HOUR, 30);
        assert_eq!(kinds(&view), ["day", "window"]);
        assert!(view
            .iter()
            .any(|entry| entry.1 == one_of(&fake, "day_bundle=")));
        assert_eq!(
            view.iter().map(|entry| entry.2).sum::<u64>(),
            day_rows(&fake) + open.num_rows() as u64
        );

        // Forks of a whole-day summary view list its day bundle once, beside the same windows.
        for (from, to) in [
            (day, BASE + 17 * HOUR),
            (day, day + 86_400),
            (BASE, BASE + 9 * HOUR),
        ] {
            let query = format!("view=summary&from={}&to={}", iso_url(from), iso_url(to));
            let sequential: BTreeSet<String> = summary_view(&fake, from, to, 30)
                .into_iter()
                .map(|entry| entry.1)
                .collect();
            let forked = walk_forked(&fake, &query);
            assert!(forked.forks >= 2);
            assert_eq!(forked.files, sequential);
        }

        // A range that starts mid-day reads the hours inside it, never the day bundle.
        let view = summary_view(&fake, BASE + HOUR, BASE + 2 * HOUR, 30);
        assert_eq!(kinds(&view), ["hour"]);
        assert!(view
            .iter()
            .all(|entry| entry.1.contains("/hour_bundle=09-")));
    }

    #[test]
    fn summary_view_and_inline_metadata_keep_late_committed_windows_exact_without_list() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        window(&fake, BASE + 30 * 60, 0, 13);
        let late = fake
            .keys(WINDOWS)
            .into_iter()
            .find(|k| k.contains(&format!("summary={}-", BASE + 30 * 60)))
            .unwrap();
        fake.mtimes
            .borrow_mut()
            .insert(late.clone(), BASE + 20 * HOUR);
        let view = summary_view(&fake, BASE - 8 * HOUR, BASE + 16 * HOUR, 30);
        assert_eq!(kinds(&view), ["day", "window"]);
        assert!(view.iter().any(|entry| entry.1 == late));
        assert_eq!(view.iter().map(|v| v.2).sum::<u64>(), day_rows(&fake));

        // An hour cut by the range is read per window, only those overlapping it.
        let view = summary_view(&fake, BASE + 3 * 60 + 30, BASE + 2 * HOUR, 30);
        let windows: Vec<&str> = view
            .iter()
            .filter(|v| v.0 == "window" && v.1.contains("/hour=08/"))
            .map(|v| v.1.as_str())
            .collect();
        assert_eq!(view.iter().filter(|entry| entry.0 == "hour").count(), 1);
        assert!(windows.iter().all(|k| k.contains("/hour=08/")));
        assert_eq!(
            windows.len(),
            fake.keys(WINDOWS)
                .iter()
                .filter(|k| k.contains("/hour=08/summary="))
                .filter(|k| {
                    let end: i64 = k
                        .split("summary=")
                        .nth(1)
                        .unwrap()
                        .split('-')
                        .nth(1)
                        .unwrap()
                        .parse()
                        .unwrap();
                    end > BASE + 3 * 60 + 30
                })
                .count()
        );

        let view = summary_view(&fake, BASE + HOUR, BASE + HOUR + 2 * 60, 30);
        assert_eq!(kinds(&view), ["window", "window"]);
        fake.new_request();
        let inline = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + HOUR + 2 * 60)
            ),
            Instant::now(),
        )
        .unwrap();
        let summary_files = inline["summary_files"].as_array().unwrap();
        assert_eq!(summary_files.len(), 2, "{summary_files:?}");
        assert_eq!(summary_files[0]["kind"], "window");
        assert!(summary_files[0]["key"]
            .as_str()
            .unwrap()
            .contains("/hour=09/summary="));

        for k in fake.keys(WINDOWS) {
            let req_key =
                k.replacen(WINDOWS, "processed/request/", 1)
                    .replacen("/summary=", "/window=", 1);
            fake.bucket.borrow_mut().insert(req_key, vec![1]);
        }
        fake.new_request();
        let lists_before = super::lists();
        let inline2 = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + 2 * HOUR)
            ),
            Instant::now(),
        )
        .unwrap();
        let lists_used = super::lists() - lists_before;
        assert_eq!(
            lists_used, 0,
            "catalog discovery must not LIST physical objects"
        );
        let sf2 = inline2["summary_files"].as_array().unwrap();
        assert_eq!(sf2.len(), 1);
        assert_eq!(sf2[0]["kind"], "hour");
        assert!(sf2[0]["key"].as_str().unwrap().contains("/hour_bundle=09-"));

        fake.cache.borrow_mut().clear();
        fake.new_request();
        let lists_before_cold = super::lists();
        let inline3 = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + 2 * HOUR)
            ),
            Instant::now(),
        )
        .unwrap();
        let lists_used_cold = super::lists() - lists_before_cold;
        assert_eq!(
            lists_used_cold, 0,
            "cold discovery must use the same committed snapshot"
        );
        assert_eq!(inline3["summary_files"], inline2["summary_files"]);
    }

    #[test]
    fn a_closing_hour_bundles_windows_committed_after_its_open_hour_snapshot() {
        let fake = Fake::new(BASE + 30 * 60);
        for m in 0..50 {
            window(&fake, BASE + m * 30, 0, 5);
        }
        watermark(&fake, BASE + 30 * 60);
        // The open hour pre-folds part of its 50 windows and keeps its catalog snapshot.
        assert!(run(&fake).more_pending);
        for m in 50..70 {
            window(&fake, BASE + m * 30, 0, 5);
        }
        fake.now.set((BASE + 2 * HOUR) as u64);
        watermark(&fake, BASE + 2 * HOUR);
        settle(&fake);
        // Only the complete hour is ever published, not one built from the open-hour snapshot.
        assert!(one_of(&fake, "hour_bundle=").contains("hour_bundle=08-c70-"));
        assert!(fake.keys(BUNDLES).iter().all(|k| !k.contains("-c50-")));
    }

    #[test]
    fn summary_bundle_closes_an_idle_hour_once_sweep_confirms_no_pending_raw_files() {
        let fake = Fake::new(BASE + 2 * HOUR);
        seed_hour(&fake, BASE, 4);
        // Last raw key arrived mid-hour at BASE + 20 * 60; without a completed sweep, hour=08 stays
        // open even after wall-clock passes BASE + 2 * HOUR.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (1, json!({"key": raw_key(BASE + 20 * 60, "zzz")})),
        );
        settle(&fake);
        assert!(of(&fake, "hour_bundle=").is_empty());

        // Once straggler sweep completes past the hour close slack, hour=08 closes and bundles.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                2,
                json!({
                    "key": raw_key(BASE + 20 * 60, "zzz"),
                    "sweep": {"after": "", "at": BASE + 2 * HOUR}
                }),
            ),
        );
        settle(&fake);
        assert!(one_of(&fake, "hour_bundle=").contains("hour_bundle=08-c4-"));
    }

    #[test]
    fn summary_view_pages_at_hour_boundaries_within_the_call_budget() {
        let fake = Fake::new(BASE);
        let day = BASE - 8 * HOUR;
        let mut keys = BTreeSet::new();
        for i in 0..2_600 {
            let start = day + i * 30;
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            keys.insert(key);
        }
        for d in 1..5 {
            let start = day + d * 24 * HOUR;
            let key = summary::summary_key(WINDOWS, start, start + 60, "p1s0", 2);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            keys.insert(key);
        }
        publish_files(&fake, &[], &keys.iter().cloned().collect::<Vec<_>>(), &[]);
        let view = summary_view(&fake, day, day + 5 * 24 * HOUR, 9);
        assert!(view.iter().all(|v| v.0 == "window"));
        let got: BTreeSet<String> = view.into_iter().map(|v| v.1).collect();
        assert_eq!(got, keys);

        fake.new_request();
        fake.calls.set(BUDGET - 3);
        let query = format!(
            "view=summary&from={}&to={}",
            iso_url(day),
            iso_url(day + HOUR)
        );
        assert_eq!(
            api::files(&fake, &service(), &query, Instant::now())
                .unwrap_err()
                .0,
            503
        );
        assert!(fake.calls.get() <= BUDGET);
        let raw = format!("from={}&to={}", iso_url(day), iso_url(day + HOUR));
        fake.new_request();
        let page = api::files(
            &fake,
            &service(),
            &format!("{raw}&view=summary&dataset=rum"),
            Instant::now(),
        );
        assert_eq!(page.unwrap_err().0, 400);
    }

    #[test]
    fn summary_view_reads_only_the_requested_committed_hour_without_listing() {
        let fake = Fake::new(BASE);
        for i in 0..1_920 {
            let start = BASE + i * 30;
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key, vec![1]);
        }
        publish_files(&fake, &[], &fake.keys(WINDOWS), &[]);
        let before = super::lists();
        let view = summary_view(&fake, BASE + HOUR, BASE + 2 * HOUR, 30);
        assert_eq!(view.len(), 120);
        assert_eq!(super::lists() - before, 0);
    }

    #[test]
    fn summary_view_reads_immutable_catalogs_until_a_commit_moves_the_generation() {
        let fake = Fake::new(BASE + 3 * HOUR);
        let put = |start: i64| {
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            publish_files(&fake, &[], &[key], &[]);
        };
        for i in 0..120 {
            put(BASE + i * 30);
        }
        let first = summary_view(&fake, BASE, BASE + 2 * HOUR, 30);
        assert_eq!(first.len(), 120);
        let before = super::lists();
        assert_eq!(summary_view(&fake, BASE, BASE + 2 * HOUR, 30), first);
        assert_eq!(super::lists() - before, 0);

        put(BASE + 45 * 60 + 15);
        assert_eq!(summary_view(&fake, BASE, BASE + 2 * HOUR, 30).len(), 121);
    }

    #[test]
    fn a_closed_hour_bundle_equals_the_summary_of_all_its_rows() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        let batches = seed_hour(&fake, BASE, 11);
        seed_hour(&fake, BASE + 2 * HOUR, 3);
        settle(&fake);
        let key = one_of(&fake, "hour_bundle=");
        assert!(
            key.starts_with(&format!("{BUNDLES}sha256="))
                && key.contains("/year=2027/month=01/day=15/hour_bundle=08-c12-n"),
            "{key}"
        );
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        let want = read(&summary::write_summary(&refs, BASE).unwrap());
        let got = read(&fake.bucket.borrow()[&key]);
        // Bundles write pivot slices as their own trailing row group; windows interleave them.
        let (got_slices, got) = split_slices(got);
        let (want_slices, want) = split_slices(want);
        assert!(!want_slices.is_empty());
        assert_same(&got, &want);
        assert_same(&got_slices, &want_slices);
        let name = bundle::parse_name(&key).unwrap();
        assert_eq!((name.unit, name.count), (8, 12));
        assert_eq!(
            name.rows,
            batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()
        );
        assert!(
            of(&fake, "hour_bundle=")
                .iter()
                .all(|k| !k.contains("hour_bundle=10")),
            "the open hour stays per-window"
        );
        assert!(of(&fake, "day_bundle=").is_empty(), "the day is still open");
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
    }

    #[test]
    fn a_resumed_build_matches_a_one_shot_merge_and_reruns_reproduce_the_same_bytes() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 50);
        let first = run(&fake);
        assert!(first.more_pending);
        assert!(
            fake.keys(WINDOWS)
                .iter()
                .any(|k| k.contains("/hour=08/partial=c")),
            "an unfinished hour leaves a partial to resume from"
        );
        assert!(settle(&fake) >= 2);
        let key = one_of(&fake, "hour_bundle=");
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
        let sources: Vec<Vec<u8>> = fake
            .keys(WINDOWS)
            .iter()
            .map(|k| fake.bucket.borrow()[k].clone())
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(Vec::as_slice).collect();
        let one_shot = bundle::merge(&refs, BASE, bundle::Unit::Hour).unwrap();
        let bytes = fake.bucket.borrow()[&key].clone();
        assert_eq!(bytes, one_shot);

        fake.kv.borrow_mut().remove(bundle::STATE_KEY);
        fake.bucket.borrow_mut().remove(&key);
        settle(&fake);
        assert_eq!(one_of(&fake, "hour_bundle="), key);
        assert_eq!(fake.bucket.borrow()[&key], bytes);
    }

    #[test]
    fn a_full_partial_defers_final_parquet_write_when_vcpu_exceeds_write_budget_and_ignores_legacy_partials(
    ) {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 7);
        fake.new_request();
        fake.calls.set(BUDGET - 10);
        let prime = bundle::summary_bundle(&store(&fake, &service()), "ck", 1).unwrap();
        assert!(prime.more_pending);
        assert_eq!(prime.partials, 1);
        // The committed inventory and first fold are durable. The remaining seven folds
        // cross the 630ms write cutoff without crossing the 600ms fold cutoff first.
        fake.vcpu_step.set(72);
        let first = run(&fake);
        assert!(first.more_pending);
        assert_eq!(first.partials, 1);
        assert_eq!(first.stopped, Some("vcpu"));
        let partials: Vec<String> = fake
            .keys(WINDOWS)
            .into_iter()
            .filter(|k| k.contains("/hour=08/partial=c8-"))
            .collect();
        assert_eq!(partials.len(), 1, "{partials:?}");
        assert!(of(&fake, "hour_bundle=").is_empty());

        // Turn 2 decodes partial=c8 and writes the final hour_bundle in its own execution.
        let second = run(&fake);
        assert_eq!(second.hours, 1);
        assert_eq!(second.sources, 1);
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
        let key = one_of(&fake, "hour_bundle=");
        let want = fake.bucket.borrow()[&key].clone();
        fake.vcpu_step.set(1);
        assert_eq!(run(&fake).hours, 1);

        // If a legacy non-PARTIAL_MAGIC partial= exists, summary_bundle ignores and deletes it.
        fake.kv.borrow_mut().remove(bundle::STATE_KEY);
        fake.bucket.borrow_mut().remove(&key);
        let legacy_partial = partials[0].replace("partial=c8-", "partial=c1-");
        fake.bucket
            .borrow_mut()
            .insert(legacy_partial.clone(), b"PAR1legacy".to_vec());
        settle(&fake);
        assert!(!fake.bucket.borrow().contains_key(&legacy_partial));
        assert_eq!(fake.bucket.borrow()[&key], want);
    }

    #[test]
    fn a_day_partial_past_the_read_limit_is_reported_and_the_day_rebuilds_without_it() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let day = one_of(&fake, "day_bundle=");
        let want = fake.bucket.borrow()[&day].clone();
        let mut hours: Vec<String> = of(&fake, "hour_bundle=")
            .iter()
            .map(|k| k.rsplit('/').next().unwrap().to_owned())
            .collect();
        hours.sort();
        // An unpacked partial that grew past the 8 MiB CDN read limit, so every resume failed.
        let stuck = format!(
            "{BUNDLES}year=2027/month=01/day=15/partial=c1-n1-h{:016x}.parquet",
            bundle::fingerprint([hours[0].as_str()])
        );
        let mut body = b"FLPB\x03".to_vec();
        body.resize(9 << 20, 0);
        fake.bucket.borrow_mut().insert(stuck.clone(), body);
        fake.kv.borrow_mut().remove(bundle::STATE_KEY);
        fake.bucket.borrow_mut().remove(&day);

        let mut errors = Vec::new();
        for _ in 0..40 {
            let report = run(&fake);
            errors.extend(report.errors);
            if !report.more_pending {
                break;
            }
        }
        assert!(
            errors
                .iter()
                .any(|e| e.starts_with("cdn_body_too_large:partial=c1-")),
            "{errors:?}"
        );
        assert!(!fake.bucket.borrow().contains_key(&stuck));
        assert_eq!(one_of(&fake, "day_bundle="), day);
        assert_eq!(fake.bucket.borrow()[&day], want);
    }

    #[test]
    fn a_day_that_cannot_be_built_does_not_hold_back_later_days() {
        let fake = Fake::new(BASE + 41 * HOUR);
        watermark(&fake, BASE + 41 * HOUR);
        seed_hour(&fake, BASE, 3);
        seed_hour(&fake, BASE + 24 * HOUR, 3);
        for _ in 0..40 {
            run(&fake);
            if of(&fake, "hour_bundle=").len() == 2 {
                break;
            }
        }
        assert!(of(&fake, "day_bundle=").is_empty());
        let unreadable = of(&fake, "hour_bundle=")
            .into_iter()
            .find(|k| k.contains("/day=15/"))
            .unwrap();
        fake.bucket
            .borrow_mut()
            .insert(unreadable, vec![0; 9 << 20]);

        let mut errors = Vec::new();
        for _ in 0..20 {
            errors.extend(run(&fake).errors);
        }
        let days = of(&fake, "day_bundle=");
        assert_eq!(days.len(), 1, "{days:?} {errors:?}");
        assert!(days[0].contains("/day=16/"), "{days:?}");
        assert!(
            errors
                .iter()
                .any(|e| e.starts_with("cdn_body_too_large:hour_bundle=08-")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_late_window_rebuilds_its_hour_and_day_and_retires_the_old_bundles_after_grace() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let old_hour = of(&fake, "hour_bundle=")
            .into_iter()
            .find(|k| k.contains("hour_bundle=08-"))
            .unwrap();
        let old_day = one_of(&fake, "day_bundle=");
        assert!(
            old_day.starts_with(&format!("{BUNDLES}sha256="))
                && old_day.contains("/year=2027/month=01/day=15/day_bundle=c2-n"),
            "{old_day}"
        );
        let day_rows = read(&fake.bucket.borrow()[&old_day]);
        assert_eq!(
            day_rows
                .iter()
                .filter(|r| r["bundle"] == "overview")
                .count(),
            2,
            "a day keeps one overview row per hour"
        );

        let late = window(&fake, BASE + 30 * 60, 0, 13);
        fake.now.set((BASE + 24 * HOUR) as u64);
        watermark(&fake, BASE + 24 * HOUR);
        settle(&fake);
        let hours: Vec<String> = of(&fake, "hour_bundle=")
            .into_iter()
            .filter(|k| k.contains("hour_bundle=08-"))
            .collect();
        assert_eq!(
            hours.len(),
            2,
            "the replaced bundle waits out the reader grace"
        );
        let new_hour = hours.iter().find(|k| **k != old_hour).unwrap().clone();
        assert!(new_hour.contains("hour_bundle=08-c8-"), "{new_hour}");
        let new_day = of(&fake, "day_bundle=")
            .into_iter()
            .find(|k| *k != old_day)
            .expect("the day bundle is rebuilt");
        let overview: i64 = read(&fake.bucket.borrow()[&new_day])
            .iter()
            .filter(|r| r["bundle"] == "overview")
            .map(|r| r["requests"].as_i64().unwrap())
            .sum();
        let old_total: i64 = day_rows
            .iter()
            .filter(|r| r["bundle"] == "overview")
            .map(|r| r["requests"].as_i64().unwrap())
            .sum();
        assert_eq!(overview, old_total + late.num_rows() as i64);

        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        settle(&fake);
        let bucket = fake.bucket.borrow();
        assert!(!bucket.contains_key(&old_hour) && !bucket.contains_key(&old_day));
        assert!(bucket.contains_key(&new_hour) && bucket.contains_key(&new_day));
        drop(bucket);
        let day = BASE - BASE.rem_euclid(86_400);
        let visible = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&visible), ["day"]);
        assert_eq!(visible[0].1, new_day);
    }

    #[test]
    fn floor_key_survives_bundle_publish_and_cleanup() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        let floor = raw_floor("request", BASE);
        set_floor(&fake, ingest::WATERMARK_KEY, &floor);
        seed_hour(&fake, BASE, 6);
        settle(&fake);
        let old_hour = of(&fake, "hour_bundle=")
            .into_iter()
            .find(|k| k.contains("hour_bundle=08-"))
            .unwrap();
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        window(&fake, BASE + 30 * 60, 0, 13);
        fake.now.set((BASE + 24 * HOUR) as u64);
        watermark(&fake, BASE + 24 * HOUR);
        settle(&fake);
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        settle(&fake);
        assert!(!fake.bucket.borrow().contains_key(&old_hour));
        assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
    }

    #[test]
    fn reader_views_use_a_bundle_and_read_only_the_windows_it_does_not_cover() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 4);
        settle(&fake);
        let svc = service();
        let s = store(&fake, &svc);
        let list = |prefix: &str| s.list(prefix, None, None, 1000, None).unwrap().objects;
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let bundles = list(BUNDLES);
        let view = bundle::hour_view(8, &windows, &bundles);
        assert_eq!(
            view.bundle.as_deref(),
            Some(one_of(&fake, "hour_bundle=").as_str())
        );
        assert!(view.windows.is_empty());

        let late = summary::summary_key(WINDOWS, BASE + 600, BASE + 660, "p2s0", 1);
        fake.bucket.borrow_mut().insert(late.clone(), vec![0]);
        fake.mtimes
            .borrow_mut()
            .insert(late.clone(), BASE + 4 * HOUR);
        for b in &bundles {
            fake.mtimes
                .borrow_mut()
                .insert(b.key.clone(), BASE + 3 * HOUR);
        }
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let bundles = list(BUNDLES);
        let view = bundle::hour_view(8, &windows, &bundles);
        assert!(view.bundle.is_some());
        assert_eq!(view.windows, std::slice::from_ref(&late));

        // A window gone from the listing invalidates the bundle: read every window instead.
        let gone = windows.iter().find(|o| o.key != late).unwrap().key.clone();
        fake.bucket.borrow_mut().remove(&gone);
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let view = bundle::hour_view(8, &windows, &bundles);
        assert!(view.bundle.is_none());
        assert_eq!(view.windows.len(), 4);
    }

    #[test]
    fn retention_sweeps_hour_and_day_bundles_with_request_retention() {
        let fake = Fake::new(BASE + 40 * DAY);
        let day = |offset: i64| {
            let t = ingest::hour_dir(BASE + offset * DAY);
            t.rsplit_once("hour=").unwrap().0.to_owned()
        };
        let old = [
            format!(
                "{BUNDLES}{}hour_bundle=08-c1-n1-h0000000000000001.parquet",
                day(0)
            ),
            format!("{BUNDLES}{}partial=c1-n1-h0000000000000001.parquet", day(0)),
            format!(
                "{BUNDLES}{}day_bundle=c1-n1-h0000000000000001.parquet",
                day(0)
            ),
            format!(
                "{BUNDLES}sha256={}/{}day_bundle=c1-n1-h0000000000000001.parquet",
                "f".repeat(64),
                day(0)
            ),
        ];
        let fresh = [
            format!(
                "{BUNDLES}{}hour_bundle=08-c1-n1-h0000000000000001.parquet",
                day(20)
            ),
            format!(
                "{BUNDLES}{}day_bundle=c1-n1-h0000000000000001.parquet",
                day(20)
            ),
            format!(
                "{BUNDLES}sha256={}/{}day_bundle=c1-n1-h0000000000000001.parquet",
                "0".repeat(64),
                day(20)
            ),
        ];
        for key in old.iter().chain(&fresh) {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let svc = service();
        let mut report = sweep(&fake, &svc, &Default::default());
        let mut datasets = report.datasets.clone();
        while report.more_pending {
            fake.new_request();
            report = ingest::retention::retention_sweep_from(
                &store(&fake, &svc),
                &Default::default(),
                4,
                None,
                report.resume_from,
            )
            .unwrap();
            datasets.extend(report.datasets.clone());
        }
        assert!(
            datasets
                .iter()
                .any(|d| d.to_string().contains("summary_bundle")),
            "{datasets:?}"
        );
        let bucket = fake.bucket.borrow();
        for key in &old {
            assert!(!bucket.contains_key(key), "{key}");
        }
        for key in &fresh {
            assert!(bucket.contains_key(key), "{key}");
        }
    }

    #[test]
    fn the_tick_runs_summary_bundle_as_a_maintenance_stage() {
        let fake = Fake::new(BASE + 3 * HOUR);
        tick_runtime(&fake);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE + 3 * HOUR, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            watermark(&fake, fake.now.get() as i64);
            run_tick(&fake);
        }
        let kinds: Vec<String> = rows(&fake, LogKind::CronRuns)
            .iter()
            .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
            .collect();
        assert!(kinds.contains(&"summary_bundle".to_owned()), "{kinds:?}");
        assert!(of(&fake, "hour_bundle=")
            .iter()
            .any(|k| k.contains("hour_bundle=08-c3-")));
    }

    const CMCD_WINDOWS: &str = "processed/request/cmcd_summary/";
    const CMCD_BUNDLES: &str = "processed/summary_bundle/cmcd_summary/";

    /// Writes a CMCD window summary of `n` rows for the minute at `start`; returns its key.
    fn cmcd_window(fake: &Fake, start: i64, shard: u32, n: i64) -> String {
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("cmcd_sid", DataType::Utf8, true),
            Field::new("cmcd_br", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(
                    TimestampMillisecondArray::from(
                        (0..n)
                            .map(|i| start * 1000 + i * 977 % 60_000)
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                std::sync::Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| Some(format!("s{}", i % 3)))
                        .collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(Int64Array::from(
                    (0..n).map(|i| Some(1000 + i * 10)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let key = ingest::cmcd_summary::cmcd_summary_key(
            CMCD_WINDOWS,
            start,
            start + 60,
            &format!("p1s{shard}"),
            n as u64,
        );
        fake.bucket.borrow_mut().insert(
            key.clone(),
            ingest::cmcd_summary::write_cmcd_summary(&[&batch], start).unwrap(),
        );
        key
    }

    fn settle_cmcd(fake: &Fake) {
        let svc = service();
        for _ in 0..40 {
            fake.new_request();
            let report = bundle::cmcd_summary_bundle(&store(fake, &svc), "ck", 1).unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            if !report.more_pending {
                return;
            }
        }
        panic!("CMCD bundling did not settle");
    }

    fn cmcd_view(fake: &Fake, from: i64, to: i64) -> Vec<(String, String, u64)> {
        let page = files_page(
            fake,
            &format!(
                "dataset=cmcd_summary&view=summary&from={}&to={}",
                iso_url(from),
                iso_url(to)
            ),
        )
        .unwrap();
        assert_eq!(page["complete"], true, "{page}");
        page["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                (
                    f["kind"].as_str().unwrap().to_owned(),
                    f["key"].as_str().unwrap().to_owned(),
                    f["rows"].as_u64().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn cmcd_summary_view_reads_hour_bundles_plus_windows_written_after_them() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        let windows: Vec<String> = (0..4)
            .map(|m| cmcd_window(&fake, BASE + m * 60, 0, 5))
            .collect();
        settle_cmcd(&fake);
        let bundles: Vec<String> = fake
            .keys(CMCD_BUNDLES)
            .into_iter()
            .filter(|k| k.contains("hour_bundle=08-c4-n20-"))
            .collect();
        assert_eq!(bundles.len(), 1, "{:?}", fake.keys(CMCD_BUNDLES));

        let view = cmcd_view(&fake, BASE, BASE + HOUR);
        assert_eq!(view, [("hour".to_owned(), bundles[0].clone(), 20)]);

        // A window written after the bundle is read on top of it.
        let late = cmcd_window(&fake, BASE + 600, 1, 2);
        fake.mtimes
            .borrow_mut()
            .insert(late.clone(), BASE + 2 * HOUR);
        let view = cmcd_view(&fake, BASE, BASE + HOUR);
        assert_eq!(
            view,
            [
                ("hour".to_owned(), bundles[0].clone(), 20),
                ("window".to_owned(), late.clone(), 2)
            ]
        );

        // A window the bundle does not name but listed no later than it breaks the proof, so
        // every window is read instead.
        fake.mtimes.borrow_mut().remove(&late);
        let view = cmcd_view(&fake, BASE, BASE + HOUR);
        assert!(view.iter().all(|v| v.0 == "window"), "{view:?}");
        assert_eq!(view.len(), 5);

        // A range starting inside the hour reads its overlapping windows, not the bundle.
        let view = cmcd_view(&fake, BASE + 120, BASE + HOUR);
        let keys: Vec<&str> = view.iter().map(|v| v.1.as_str()).collect();
        assert_eq!(
            keys,
            [windows[2].as_str(), windows[3].as_str(), late.as_str()]
        );

        // The raw CMCD listing is unchanged.
        let raw = files_page(
            &fake,
            &format!(
                "dataset=cmcd_summary&from={}&to={}",
                iso_url(BASE),
                iso_url(BASE + HOUR)
            ),
        )
        .unwrap();
        assert_eq!(raw["files"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn cmcd_summary_view_reads_the_open_bundle_plus_the_windows_after_it() {
        let now = BASE + 25 * 60;
        let fake = Fake::new(now);
        watermark(&fake, now);
        let mut windows: Vec<String> = (0..20)
            .map(|m| cmcd_window(&fake, BASE + m * 60, 0, 5))
            .collect();
        settle_cmcd(&fake);
        // Windows written after the snapshot.
        windows.extend((20..23).map(|m| cmcd_window(&fake, BASE + m * 60, 0, 5)));
        let open: Vec<String> = fake
            .keys(CMCD_BUNDLES)
            .into_iter()
            .filter(|k| k.contains("open_bundle=08-"))
            .collect();
        assert_eq!(open.len(), 1, "{:?}", fake.keys(CMCD_BUNDLES));
        let covered = bundle::parse_name(&open[0]).unwrap().count;
        assert!((6..=20).contains(&covered), "{}", open[0]);

        // The open bundle stands in for the settled windows it names; the rest are read raw.
        let view = cmcd_view(&fake, BASE, now);
        let mut expected = vec![("hour".to_owned(), open[0].clone(), covered as u64 * 5)];
        expected.extend(
            windows[covered..]
                .iter()
                .map(|k| ("window".to_owned(), k.clone(), 5)),
        );
        assert_eq!(view, expected);

        // A late window sorting inside the bundle's prefix breaks the proof: every window is read.
        let late = cmcd_window(&fake, BASE, 1, 2);
        let view = cmcd_view(&fake, BASE, now);
        assert!(view.iter().all(|v| v.0 == "window"), "{view:?}");
        assert_eq!(view.len(), 24);
        assert!(view.iter().any(|v| v.1 == late));
    }

    #[test]
    fn the_tick_bundles_cmcd_summaries_after_request_summaries() {
        let fake = Fake::new(BASE + 3 * HOUR);
        tick_runtime(&fake);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        for m in 0..3 {
            cmcd_window(&fake, BASE + m * 60, 0, 4);
        }
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE + 3 * HOUR, "complete": true})),
        );
        for _ in 0..24 {
            fake.now.set(fake.now.get() + 120);
            watermark(&fake, fake.now.get() as i64);
            run_tick(&fake);
        }
        assert!(of(&fake, "hour_bundle=")
            .iter()
            .any(|k| k.contains("hour_bundle=08-c3-")));
        assert!(
            fake.keys(CMCD_BUNDLES)
                .iter()
                .any(|k| k.contains("hour_bundle=08-c3-n12-")),
            "{:?}",
            fake.keys(CMCD_BUNDLES)
        );
    }

    #[test]
    fn a_request_bundle_backlog_does_not_starve_cmcd_bundles() {
        let fake = Fake::new(BASE + 3 * HOUR);
        tick_runtime(&fake);
        watermark(&fake, BASE + 3 * HOUR);
        // Each request hour takes more than one execution's folds and calls.
        seed_hour(&fake, BASE, 50);
        seed_hour(&fake, BASE + HOUR, 50);
        for m in 0..3 {
            cmcd_window(&fake, BASE + m * 60, 0, 4);
        }
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE + 3 * HOUR, "complete": true})),
        );
        for _ in 0..60 {
            fake.now.set(fake.now.get() + 120);
            watermark(&fake, fake.now.get() as i64);
            run_tick(&fake);
            if fake
                .keys(CMCD_BUNDLES)
                .iter()
                .any(|k| k.contains("hour_bundle=08-c3-n12-"))
            {
                assert!(
                    !of(&fake, "hour_bundle=")
                        .iter()
                        .any(|k| k.contains("hour_bundle=09-")),
                    "CMCD waited for the request backlog to clear"
                );
                return;
            }
        }
        panic!("CMCD bundling never ran: {:?}", fake.keys(CMCD_BUNDLES));
    }

    #[test]
    fn summary_bundle_work_left_brings_the_next_maintenance_tick_forward() {
        use fastly_log_analytics::admin::schedule;
        let fake = Fake::new(BASE + 3 * HOUR);
        tick_runtime(&fake);
        watermark(&fake, BASE + 3 * HOUR);
        // More windows than one execution folds.
        seed_hour(&fake, BASE, 40);
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE + 3 * HOUR, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            watermark(&fake, fake.now.get() as i64);
            let before = fake.stage_runs.borrow().len();
            run_tick(&fake);
            if fake.stage_runs.borrow()[before..]
                .iter()
                .any(|reply| reply["ran"] == "summary_bundle")
            {
                let maint = fake.kv.borrow()["cron:state"].1["jobs"]["maint"].clone();
                assert_eq!(maint["more_pending"], true, "{maint}");
                assert_eq!(
                    maint["retry_after"],
                    fake.now.get() + schedule::MAINT_CATCHUP_S,
                    "{maint}"
                );
                return;
            }
        }
        panic!("the rotation never reached summary_bundle");
    }

    #[test]
    fn summary_bundle_tolerates_stale_watermark_read_after_publish() {
        // An open-hour bundle is built on tick 1 and published on tick 2. Right after
        // `catalog::bundles::publish` commits the new catalog generation to KV, Fastly KV Store
        // eventual consistency can still serve the previous `WATERMARK_KEY` record to the same
        // execution's subsequent `admin_get(WATERMARK_KEY)`. Building the next open bundle must
        // carry forward the just-published catalog generation so tick 3 does not reject it with
        // `catalog_bundle_not_active`.
        let fake = Fake::new(BASE + 1200);
        watermark(&fake, BASE + 1200);
        for i in 0..8 {
            window(&fake, BASE + i * 60, i as u32, 3);
        }
        let first = run(&fake);
        assert!(first.errors.is_empty(), "tick 1 errors: {:?}", first.errors);
        assert!(
            first.more_pending,
            "tick 1 should have a pending open-bundle publication"
        );

        // Add more windows in the same open hour so tick 2 builds a second open bundle right
        // after publishing the first one.
        for i in 8..16 {
            window(&fake, BASE + i * 60, i as u32, 3);
        }
        watermark(&fake, BASE + 1200);
        fake.stale_watermark_reads.set(1);

        let second = run(&fake);
        assert!(
            second.errors.is_empty(),
            "tick 2 errors: {:?}",
            second.errors
        );

        // Tick 3 publishes the second open bundle against the now-propagated watermark.
        let third = run(&fake);
        assert!(
            third.errors.is_empty(),
            "tick 3 should not fail with catalog_bundle_not_active: {:?}",
            third.errors
        );
    }

    #[test]
    fn summary_bundle_drops_superseded_publication_without_error() {
        // Tick 1 builds an open-hour bundle and saves `state.publication` in KV `STATE_KEY`.
        // If KV eventual consistency later serves that stale `STATE_KEY` after a newer open
        // bundle (higher catalog generation) has already been published for the same hour,
        // `summary_bundle` should drop the superseded publication, record an observability
        // event, and NOT fail the cron stage with `catalog_bundle_not_active`.
        let fake = Fake::new(BASE + 1200);
        watermark(&fake, BASE + 1200);
        for i in 0..8 {
            window(&fake, BASE + i * 60, i as u32, 3);
        }
        let first = run(&fake);
        assert!(first.errors.is_empty(), "tick 1 errors: {:?}", first.errors);
        let stale_state = fake
            .kv
            .borrow()
            .get(bundle::STATE_KEY)
            .cloned()
            .expect("bundle state saved after tick 1");

        // Advance with a second open bundle so `WATERMARK_KEY` moves to a newer generation for
        // `open_bundles[BASE]`.
        for i in 8..16 {
            window(&fake, BASE + i * 60, i as u32, 3);
        }
        watermark(&fake, BASE + 1200);
        let second = run(&fake);
        assert!(
            second.errors.is_empty(),
            "tick 2 errors: {:?}",
            second.errors
        );
        let third = run(&fake);
        assert!(third.errors.is_empty(), "tick 3 errors: {:?}", third.errors);
        // Tick 2 pre-folds a partial and tick 3 encodes the snapshot (one encode per tick), so
        // tick 4 publishes it.
        let fourth = run(&fake);
        assert!(
            fourth.errors.is_empty(),
            "tick 4 errors: {:?}",
            fourth.errors
        );

        // Simulate another POP reading the stale `STATE_KEY` from tick 1 (holding the old
        // `state.publication` whose `built_generation` is now behind `WATERMARK_KEY`).
        let current_gen = fake.kv.borrow()[bundle::STATE_KEY].0;
        fake.kv
            .borrow_mut()
            .insert(bundle::STATE_KEY.into(), (current_gen + 1, stale_state.1));
        fake.events.borrow_mut().clear();

        let replay = run(&fake);
        assert!(
            replay.errors.is_empty(),
            "superseded publication must not fail summary_bundle: {:?}",
            replay.errors
        );
        assert!(
            fake.events
                .borrow()
                .iter()
                .any(|e| e["event"] == "catalog.bundle_publication_superseded"),
            "expected catalog.bundle_publication_superseded event, got: {:?}",
            fake.events.borrow()
        );
    }

    fn stale_open_publication_closed_into_an_hour(fake: &Fake) -> (Value, Value) {
        watermark(fake, BASE + 1200);
        for i in 0..8 {
            window(fake, BASE + i * 60, i as u32, 3);
        }
        let first = run(fake);
        assert!(first.errors.is_empty(), "tick 1 errors: {:?}", first.errors);
        let stale_state = fake
            .kv
            .borrow()
            .get(bundle::STATE_KEY)
            .cloned()
            .expect("bundle state saved after tick 1");
        for i in 8..16 {
            window(fake, BASE + i * 60, i as u32, 3);
        }
        watermark(fake, BASE + 1200);
        let second = run(fake);
        assert!(
            second.errors.is_empty(),
            "tick 2 errors: {:?}",
            second.errors
        );
        let third = run(fake);
        assert!(third.errors.is_empty(), "tick 3 errors: {:?}", third.errors);
        let open_reference = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["open_bundles"]
            [&BASE.to_string()]
            .clone();

        fake.now.set((BASE + 2 * HOUR) as u64);
        watermark(fake, BASE + 2 * HOUR);
        settle(fake);
        assert!(
            fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["hour_bundles"]
                .get(BASE.to_string())
                .is_some(),
            "hour closure should publish a real closed-hour reference"
        );

        (stale_state.1, open_reference)
    }

    #[test]
    fn summary_bundle_reports_open_publication_closed_into_an_hour() {
        let fake = Fake::new(BASE + 1200);
        let (stale_state, _) = stale_open_publication_closed_into_an_hour(&fake);
        let publication_file = stale_state["publication"]["file"]["key"].clone();
        let state_generation = fake.kv.borrow()[bundle::STATE_KEY].0;
        fake.kv.borrow_mut().insert(
            bundle::STATE_KEY.into(),
            (state_generation + 1, stale_state),
        );
        fake.events.borrow_mut().clear();

        let replay = run(&fake);
        assert!(
            replay.errors.is_empty(),
            "closed-hour supersession must not fail summary_bundle: {:?}",
            replay.errors
        );
        let events = fake.events.borrow();
        let event = events
            .iter()
            .find(|event| event["event"] == "catalog.bundle_publication_superseded")
            .expect("superseded publication admin event");
        assert_eq!(event["reason"], "hour_bundle_closed");
        assert_eq!(event["publication_file"], publication_file);
    }

    #[test]
    fn summary_bundle_keeps_unproven_closed_hour_references_as_errors() {
        for invalid in [
            "stale",
            "above_head",
            "coverage_not_older",
            "corrupt",
            "wrong_kind",
        ] {
            let fake = Fake::new(BASE + 1200);
            let (stale_state, open_reference) = stale_open_publication_closed_into_an_hour(&fake);
            let built_generation = stale_state["publication"]["built_generation"]
                .as_u64()
                .expect("publication built generation");
            {
                let (watermark_generation, mut watermark_value) =
                    fake.kv.borrow()[ingest::WATERMARK_KEY].clone();
                let head_generation = watermark_value["catalog"]["generation"]
                    .as_u64()
                    .expect("catalog head generation");
                let reference = watermark_value["catalog"]["hour_bundles"]
                    .get_mut(BASE.to_string())
                    .expect("closed-hour reference");
                match invalid {
                    "stale" => reference["generation"] = built_generation.into(),
                    "above_head" => reference["generation"] = (head_generation + 1).into(),
                    "coverage_not_older" => {
                        let key = reference["key"].as_str().expect("coverage key").to_owned();
                        let reference_generation = reference["generation"]
                            .as_u64()
                            .expect("reference generation");
                        let mut coverage: Value =
                            serde_json::from_slice(&fake.bucket.borrow()[&key])
                                .expect("closed coverage JSON");
                        coverage["built_generation"] = reference_generation.into();
                        fake.bucket.borrow_mut().insert(
                            key,
                            serde_json::to_vec(&coverage).expect("serialize closed coverage"),
                        );
                    }
                    "corrupt" => {
                        let key = reference["key"].as_str().expect("coverage key");
                        fake.bucket
                            .borrow_mut()
                            .insert(key.into(), b"not json".to_vec());
                    }
                    "wrong_kind" => *reference = open_reference.clone(),
                    _ => unreachable!(),
                }
                fake.kv.borrow_mut().insert(
                    ingest::WATERMARK_KEY.into(),
                    (watermark_generation + 1, watermark_value),
                );
            }
            let state_generation = fake.kv.borrow()[bundle::STATE_KEY].0;
            fake.kv.borrow_mut().insert(
                bundle::STATE_KEY.into(),
                (state_generation + 1, stale_state),
            );
            fake.events.borrow_mut().clear();

            let replay = run(&fake);
            assert_eq!(
                replay.errors,
                ["catalog_bundle_not_active"],
                "{invalid} closed-hour reference"
            );
            assert!(
                !fake
                    .events
                    .borrow()
                    .iter()
                    .any(|event| event["event"] == "catalog.bundle_publication_superseded"),
                "{invalid} reference must not be reported as superseded"
            );
        }
    }

    #[test]
    fn summary_bundle_keeps_real_catalog_publication_failures_as_errors() {
        let fake = Fake::new(BASE + 1200);
        watermark(&fake, BASE + 1200);
        for i in 0..8 {
            window(&fake, BASE + i * 60, i as u32, 3);
        }
        let first = run(&fake);
        assert!(first.errors.is_empty(), "tick 1 errors: {:?}", first.errors);
        fake.kv.borrow_mut().remove(ingest::WATERMARK_KEY);
        fake.events.borrow_mut().clear();

        let replay = run(&fake);
        assert_eq!(replay.errors, ["catalog_bundle_not_active"]);
        assert!(
            !fake
                .events
                .borrow()
                .iter()
                .any(|event| event["event"] == "catalog.bundle_publication_superseded"),
            "real catalog publication errors must not be reported as superseded"
        );
    }

    fn assert_w6_layout(bytes: &[u8]) {
        use arrow_array::StringArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::collections::BTreeSet;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec())).unwrap();
        let meta = builder.metadata().clone();
        let kv = meta
            .file_metadata()
            .key_value_metadata()
            .expect("kv metadata");
        let summary_ver = kv
            .iter()
            .find(|e| e.key == "flp_summary")
            .and_then(|e| e.value.as_deref());
        assert_eq!(summary_ver, Some("3"));

        let layout_raw = kv
            .iter()
            .find(|e| e.key == "flp_summary_layout")
            .and_then(|e| e.value.as_deref())
            .expect("flp_summary_layout kv metadata");
        let layout: Value = serde_json::from_str(layout_raw).expect("valid layout json");
        assert_eq!(layout["v"], 1);
        let rg_entries = layout["rg"].as_array().expect("rg array");
        assert_eq!(
            rg_entries.len(),
            meta.num_row_groups(),
            "layout entries must match row group count"
        );
        assert!(meta.num_row_groups() > 1, "expected multiple row groups");

        let mut seen_groups = BTreeSet::new();
        let mut total_rows = 0usize;
        for (rg_idx, rg_entry) in rg_entries.iter().enumerate() {
            let declared: BTreeSet<String> = rg_entry
                .as_array()
                .expect("entry array")
                .iter()
                .map(|v| v.as_str().expect("group string").to_owned())
                .collect();
            assert!(
                !declared.is_empty(),
                "row group {rg_idx} has empty layout entry"
            );

            let rg_meta = meta.row_group(rg_idx);
            assert!(rg_meta.num_rows() > 0, "row group {rg_idx} is empty");
            total_rows += rg_meta.num_rows() as usize;

            for col in rg_meta.columns() {
                let name = col.column_descr().name();
                if matches!(name, "bundle" | "bucket" | "field") {
                    assert!(
                        col.statistics().is_some(),
                        "row group {rg_idx} column {name} must keep statistics"
                    );
                } else {
                    assert!(
                        col.statistics().is_none(),
                        "row group {rg_idx} column {name} must drop statistics"
                    );
                }
            }

            let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
                .unwrap()
                .with_row_groups(vec![rg_idx])
                .build()
                .unwrap();
            let mut actual_groups = BTreeSet::new();
            for batch in reader {
                let batch = batch.unwrap();
                let bundles = batch
                    .column_by_name("bundle")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let b = bundles.value(row);
                    let g = if b == "dim_totals" {
                        values.value(row)
                    } else {
                        b
                    };
                    actual_groups.insert(g.to_owned());
                }
            }
            for g in &actual_groups {
                assert!(
                    declared.contains(g),
                    "row group {rg_idx} contains undeclared group {g} (declared: {declared:?})"
                );
                assert!(
                    seen_groups.insert(g.clone()),
                    "group {g} appears in multiple row groups"
                );
            }
        }
        assert_eq!(
            total_rows,
            meta.file_metadata().num_rows() as usize,
            "row group counts sum to file rows"
        );
    }

    #[test]
    fn hour_bundle_has_one_row_group_per_bundle_value() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 11);
        settle(&fake);
        let key = one_of(&fake, "hour_bundle=");
        let bytes = fake.bucket.borrow()[&key].clone();
        assert_w6_layout(&bytes);
    }

    #[test]
    fn layout_kv_matches_the_footer() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 11);
        settle(&fake);
        let key = one_of(&fake, "hour_bundle=");
        let bytes = fake.bucket.borrow()[&key].clone();
        assert_w6_layout(&bytes);
    }

    #[test]
    fn day_bundle_layout_survives_refold() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let day = one_of(&fake, "day_bundle=");
        let bytes = fake.bucket.borrow()[&day].clone();
        assert_w6_layout(&bytes);
    }

    #[test]
    fn open_bundle_equals_hour_bundle_of_its_windows() {
        let fake = Fake::new(BASE + 30 * 60);
        watermark(&fake, BASE + 30 * 60);
        for m in 0..8 {
            window(&fake, BASE + m * 30, (m * 10) as u32, 5);
        }
        settle(&fake);
        let open_key = one_of(&fake, "open_bundle=");
        let name = bundle::parse_name(&open_key).unwrap();
        assert_eq!(name.kind, bundle::NameKind::Open);
        assert_eq!((name.unit, name.count), (8, 8));
        assert_eq!(name.end_ms, Some((BASE + 7 * 30 + 60) * 1000));
        let sources: Vec<Vec<u8>> = fake
            .keys(WINDOWS)
            .into_iter()
            .filter(|k| k.contains("/summary="))
            .map(|k| fake.bucket.borrow()[&k].clone())
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(Vec::as_slice).collect();
        let one_shot = bundle::merge(&refs, BASE, bundle::Unit::Hour).unwrap();
        let bytes = fake.bucket.borrow()[&open_key].clone();
        assert_eq!(bytes, one_shot);
        assert_w6_layout(&bytes);
        let view = summary_view(&fake, BASE, BASE + HOUR, 30);
        assert_eq!(view, vec![("hour".into(), open_key, 40)]);
    }

    #[test]
    fn open_bundle_never_seeds_a_resume() {
        let fake = Fake::new(BASE + 30 * 60);
        watermark(&fake, BASE + 30 * 60);
        for m in 0..8 {
            window(&fake, BASE + m * 60, (m * 100) as u32, 100);
        }
        settle(&fake);
        let first_open = one_of(&fake, "open_bundle=");
        assert_eq!(bundle::parse_name(&first_open).unwrap().count, 8);
        for key in fake
            .keys(WINDOWS)
            .into_iter()
            .filter(|k| k.contains("partial="))
        {
            fake.bucket.borrow_mut().remove(&key);
        }

        for m in 8..14 {
            window(&fake, BASE + m * 60, (m * 100) as u32, 100);
        }
        fake.now.set((BASE + 2 * HOUR) as u64);
        watermark(&fake, BASE + 2 * HOUR);
        settle(&fake);

        let closed_key = one_of(&fake, "hour_bundle=");
        let sources: Vec<Vec<u8>> = fake
            .keys(WINDOWS)
            .into_iter()
            .filter(|k| k.contains("/summary="))
            .map(|k| fake.bucket.borrow()[&k].clone())
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(Vec::as_slice).collect();
        let one_shot = bundle::merge(&refs, BASE, bundle::Unit::Hour).unwrap();
        assert_eq!(fake.bucket.borrow()[&closed_key], one_shot);

        fake.now
            .set((BASE + 2 * HOUR + bundle::RETIRE_GRACE_S + 1) as u64);
        settle(&fake);
        assert!(of(&fake, "open_bundle=").is_empty());
    }
}

/// Native decode cost per decompressed MB, for comparing converter changes (wasm differs).
/// Run with `cargo test --release --test ingest convert_cpu_per_mb -- --ignored --nocapture`.
#[test]
#[ignore]
fn convert_cpu_per_mb() {
    use ingest::convert::{columns, Kind};
    let lines: Vec<String> = (0..12_000i64)
        .map(|i| {
            let mut row = serde_json::Map::new();
            for column in columns() {
                let value = match (column.id, column.kind) {
                    ("timestamp", _) => json!(iso(BASE + i % 60)),
                    ("ts_ms", _) => json!((BASE + i % 60) * 1000 + i % 1000),
                    ("url", _) => json!(format!("/assets/{i}/image-{}.jpg?w=640&q=80", i % 97)),
                    (_, Kind::Utf8) => json!(format!("value-{}-\u{e9}", i % 113)),
                    (_, Kind::Int64) => json!(i % 5000),
                    (_, Kind::Float64) => json!((i % 1000) as f64 / 7.0),
                    (_, Kind::Bool) => json!(i % 2 == 0),
                    (_, Kind::TimestampMs) => json!(iso(BASE)),
                };
                row.insert(column.id.into(), value);
            }
            Value::Object(row).to_string()
        })
        .collect();
    let data = gz(&lines);
    let (mut decode, mut write) = (f64::MAX, f64::MAX);
    let mut decompressed = 0;
    for _ in 0..5 {
        let started = Instant::now();
        let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
        decode = decode.min(started.elapsed().as_secs_f64() * 1000.0);
        let started = Instant::now();
        let bytes =
            ingest::convert::write_parquet(&rows.windows.values().collect::<Vec<_>>()).unwrap();
        write = write.min(started.elapsed().as_secs_f64() * 1000.0);
        decompressed = rows.decompressed;
        assert!(!bytes.is_empty());
    }
    let mb = decompressed as f64 / (1024.0 * 1024.0);
    println!(
        "{mb:.1} MiB: decode {:.2} ms/MiB, write {:.2} ms/MiB",
        decode / mb,
        write / mb
    );
}

#[test]
fn worker_retries_one_transient_get_or_put_honouring_retry_after() {
    let fake = Fake::new(BASE + 7200);
    let keys = [put_raw(&fake, BASE, "a", &[line(BASE, "a")])];
    let clean = worker_call(&fake, &keys, 20_000);
    assert_eq!(clean["status"], "ok", "{clean}");

    fake.transient.borrow_mut().push_back(("GET", 429, None));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert_eq!(reply["calls"], clean["calls"].as_u64().unwrap() + 1);
    assert_eq!(fake.waits.borrow().len(), 1);

    fake.waits.borrow_mut().clear();
    fake.transient.borrow_mut().push_back(("PUT", 503, Some(2)));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert_eq!(*fake.waits.borrow(), [2000]);

    // One spare call: a second transient answer is final.
    fake.waits.borrow_mut().clear();
    fake.transient
        .borrow_mut()
        .extend([("PUT", 503, None), ("PUT", 500, None)]);
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "error");
    assert_eq!(reply["error"], "put_http_500");
    assert_eq!(fake.waits.borrow().len(), 1);

    // A Retry-After past the worker's stop is not waited out.
    fake.waits.borrow_mut().clear();
    fake.transient
        .borrow_mut()
        .push_back(("PUT", 503, Some(25)));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["error"], "put_http_503");
    assert!(fake.waits.borrow().is_empty());
}

#[test]
fn transient_worker_errors_do_not_count_toward_abandoning_the_plan() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 1, 3);
    for _ in 0..ingest::coordinator::MAX_SHARD_FAILURES + 1 {
        fake.transient
            .borrow_mut()
            .extend([("PUT", 503, None), ("PUT", 503, None)]);
        let report = tick_later(&fake).unwrap();
        assert_eq!(report.converted, 0);
        let plan = plan(&fake).unwrap();
        assert!(plan["abandoning"].is_null(), "{plan}");
        assert!(
            plan["failures"].as_object().is_none_or(|f| f.is_empty()),
            "{plan}"
        );
    }
    assert_eq!(tick_later(&fake).unwrap().converted, 3);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn worker_splits_when_the_heap_nears_its_limit() {
    let fake = Fake::new(BASE + 7200);
    let a = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let b = put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    fake.heap_mib.set(Some(120));
    let reply = worker_call(&fake, &[a.clone(), b], 20_000);
    assert_eq!(reply["status"], "split", "{reply}");
    assert!(fake.keys("processed/").is_empty());
    // A lone object cannot split further, so it still converts.
    let reply = worker_call(&fake, &[a], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
}

#[test]
fn worker_puts_carry_content_md5() {
    use base64::Engine;
    use md5::{Digest, Md5};
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    assert_eq!(worker_call(&fake, &[key], 20_000)["status"], "ok");
    let md5s = fake.put_md5s.borrow();
    assert_eq!(md5s.len(), 3, "manifest, window file and summary");
    for (key, md5) in md5s.iter() {
        let body = &fake.bucket.borrow()[key];
        let want = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
        assert_eq!(md5.as_deref(), Some(want.as_str()), "{key}");
    }
}

#[test]
fn per_object_bytes_are_each_objects_row_share_of_the_window_file() {
    let fake = Fake::new(BASE + 7200);
    let a = put_raw(&fake, BASE, "a", &[line(BASE, "a1")]);
    let b = put_raw(
        &fake,
        BASE,
        "b",
        &[line(BASE, "b1"), line(BASE + 1, "b2"), line(BASE + 2, "b3")],
    );
    let reply = worker_call(&fake, &[a, b], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    let result = &reply["result"];
    let size = result["outputs"][0]["bytes"].as_u64().unwrap();
    let shares: Vec<u64> = result["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["outputs"][0]["bytes"].as_u64().unwrap())
        .collect();
    assert_eq!(shares, [size / 4, size * 3 / 4]);
}

#[test]
fn a_shard_holds_about_24_mib_decompressed_but_a_lone_larger_object_converts() {
    let fake = Fake::new(BASE + 7200);
    let big = |at: i64, tag: &str, n: usize| -> Vec<String> {
        (0..n)
            .map(|i| line(at, &format!("{tag}{i}-{}", "x".repeat(200_000))))
            .collect()
    };
    // About 13 MiB each: together past the batch cap, each alone well under it.
    let a = put_raw(&fake, BASE, "a", &big(BASE, "a", 68));
    let b = put_raw(&fake, BASE + 60, "b", &big(BASE + 60, "b", 68));
    let reply = worker_call(&fake, &[a, b], 20_000);
    assert_eq!(reply["status"], "split", "{reply}");
    // About 30 MiB: over the batch cap, but the only object of its shard.
    let c = put_raw(&fake, BASE + 120, "c", &big(BASE + 120, "c", 155));
    let reply = worker_call(&fake, &[c], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
}

/// Summary files per grid window under `processed/summary/request/`.
fn summaries_per_window(fake: &Fake) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for key in fake.keys("processed/summary/request/") {
        let Some(name) = key
            .rsplit('/')
            .next()
            .and_then(|n| n.strip_prefix("summary="))
        else {
            continue;
        };
        let mut parts = name.splitn(3, '-');
        let window = format!("{}-{}", parts.next().unwrap(), parts.next().unwrap());
        *out.entry(window).or_default() += 1;
    }
    out
}

#[test]
fn a_burst_backlog_writes_one_summary_file_per_closed_window() {
    use ingest::coordinator::{self, Options};
    // Eight one-minute windows of 20 objects each land at once; runs of at most two shards
    // drain them over several runs.
    let fake = Fake::new(BASE);
    let mut expected = Vec::new();
    for minute in 0..8 {
        let at = BASE + minute * 60;
        for i in 0..20 {
            let id = format!("m{minute:02}i{i:02}");
            put_raw(&fake, at, &format!("f{i:02}"), &[line(at + 1 + i, &id)]);
            expected.push(id);
        }
    }
    fake.now.set((BASE + 8 * 60 + 600) as u64);
    let options = Options {
        max_width: 2,
        ..Options::request(&fake)
    };
    let (mut runs, mut max_calls) = (0, 0);
    loop {
        fake.new_request();
        let report = coordinator::coordinate(
            &fake,
            &service(),
            "ak",
            "sk",
            Instant::now(),
            &LogBuffer::new(SID, "burst"),
            &options,
        )
        .unwrap();
        assert!(!report.busy);
        max_calls = max_calls.max(fake.calls.get());
        runs += 1;
        if !report.more_pending {
            break;
        }
        assert!(runs < 20, "burst did not drain");
        fake.now.set(fake.now.get() + 10);
    }
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    assert!(max_calls <= BUDGET, "coordinator {max_calls}");
    assert!(fake.worker_max_calls.get() <= BUDGET);
    let per_window = summaries_per_window(&fake);
    assert_eq!(per_window.len(), 8, "{per_window:?}");
    assert!(
        per_window.values().all(|&files| files == 1),
        "summary files per window after {runs} runs: {per_window:?}"
    );
}

#[test]
fn coex_iter24_02_retention_sweeps_expired_cmcd_summary_windows_and_skips_prefix_on_request_sweep()
{
    let fake = Fake::new(BASE + 40 * DAY);
    let expired_hour = ingest::hour_dir(BASE);
    let fresh_hour = ingest::hour_dir(BASE + 20 * DAY);

    let expired_cmcd_win = format!(
        "processed/request/cmcd_summary/{expired_hour}window={BASE}-{}-r1-n5.parquet",
        BASE + 60
    );
    let fresh_cmcd_win = format!(
        "processed/request/cmcd_summary/{fresh_hour}window={}-{}-r1-n5.parquet",
        BASE + 20 * DAY,
        BASE + 20 * DAY + 60
    );
    let expired_req = format!(
        "processed/request/{expired_hour}window={BASE}-{}-r1.parquet",
        BASE + 60
    );
    let fresh_req = format!(
        "processed/request/{fresh_hour}window={}-{}-r1.parquet",
        BASE + 20 * DAY,
        BASE + 20 * DAY + 60
    );

    for k in [&expired_cmcd_win, &fresh_cmcd_win, &expired_req, &fresh_req] {
        fake.bucket.borrow_mut().insert((*k).clone(), vec![1, 2, 3]);
    }
    // Also seed >1000 live cmcd_summary objects so a `"request"` sweep that didn't skip past
    // `cmcd_summary/` on initial `None` cursor would waste an entire LIST page scanning `cmcd_summary/`!
    for idx in 0..1005u32 {
        fake.bucket.borrow_mut().insert(
            format!(
                "processed/request/cmcd_summary/{fresh_hour}window={}-{}-r{idx}-n1.parquet",
                BASE + 20 * DAY,
                BASE + 20 * DAY + 60
            ),
            vec![7],
        );
    }

    let svc = service();
    // 1. Single-step `"request"` retention sweep must skip past `cmcd_summary/` on initial cursor and delete `expired_req` in 1 LIST!
    fake.new_request();
    let lists_before = lists();
    let req_only = ingest::retention::retention_sweep(
        &store(&fake, &svc),
        &Default::default(),
        4,
        Some("request"),
    )
    .unwrap();
    let req_lists = lists() - lists_before;
    assert_eq!(
        req_only.deleted, 1,
        "expected expired_req deleted: {req_only:?}"
    );
    assert_eq!(
        req_lists, 1,
        "request sweep must skip cmcd_summary/ via start_after instead of paging >1000 cmcd_summary keys"
    );
    assert!(!fake.bucket.borrow().contains_key(&expired_req));
    assert!(fake.bucket.borrow().contains_key(&fresh_req));

    // 2. Full `retention_sweep` must include `summary:cmcd_summary`, deleting `expired_cmcd_win` while keeping `fresh_cmcd_win`!
    fake.new_request();
    let mut report = sweep(&fake, &svc, &Default::default());
    let mut datasets = report.datasets.clone();
    while report.more_pending {
        fake.new_request();
        report = ingest::retention::retention_sweep_from(
            &store(&fake, &svc),
            &Default::default(),
            4,
            None,
            report.resume_from,
        )
        .unwrap();
        datasets.extend(report.datasets.clone());
    }
    assert!(
        datasets
            .iter()
            .any(|d| d["dataset"] == "summary:cmcd_summary"),
        "expected summary:cmcd_summary in retention datasets: {datasets:?}"
    );
    assert!(
        !fake.bucket.borrow().contains_key(&expired_cmcd_win),
        "expired cmcd_summary window Parquet must be deleted by retention_sweep"
    );
    assert!(
        fake.bucket.borrow().contains_key(&fresh_cmcd_win),
        "fresh cmcd_summary window Parquet must survive retention_sweep"
    );
}
