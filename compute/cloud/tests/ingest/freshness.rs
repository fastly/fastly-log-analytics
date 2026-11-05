use super::*;
use ingest::coordinator::{self, Options};
use sha2::{Digest, Sha256};

pub(super) fn finish_tick(fake: &Fake) -> Value {
    let mut result = run_tick(fake);
    for _ in 0..10 {
        let pending = fastly_log_analytics::admin::schedule::peek(
            fake,
            fastly_log_analytics::admin::schedule::SCHED_KEY,
        )
        .is_some_and(|record| {
            [
                &record.mirror.jobs.ingest,
                &record.mirror.jobs.rum,
                &record.mirror.jobs.raw_delete,
            ]
            .iter()
            .any(|job| job.more_pending && job.retry_after <= fake.now.get() + 1)
        });
        if !pending {
            return result;
        }
        fake.now.set(fake.now.get() + 1);
        result = run_tick(fake);
        assert!(fake.calls.get() <= 32);
    }
    panic!("scheduled publication did not finish bounded continuations");
}

fn ingest_available(fake: &Fake, svc: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let report = coordinator::coordinate(
        fake,
        svc,
        "ak",
        "sk",
        Instant::now(),
        &LogBuffer::new(SID, "freshness"),
        &Options::request(fake),
    );
    assert!(fake.calls.get() <= 32, "coordinator backend-call budget");
    assert!(
        fake.worker_max_calls.get() <= 32,
        "worker backend-call budget"
    );
    assert!(fake.cache_ops.get() <= 32, "cache-operation budget");
    report
}

fn discover_ips(fake: &Fake) -> Vec<String> {
    let query = format!("from={}&to={}", iso_url(BASE - 60), iso_url(BASE + 180));
    let (_, keys) = catalog_keys(fake, &query);
    let mut ips = Vec::new();
    for key in keys {
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

#[test]
fn ten_second_available_object_with_enveloped_worker_credentials_commits() {
    let fake = Fake::new(BASE + 10);
    fake.fos_envelope.set(true);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "envelope", &[line(BASE + 1, "enveloped")]);

    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(discover_ips(&fake), ["enveloped"]);
}

#[test]
fn ten_second_available_objects_commit_and_recent_late_keys_remain_exact() {
    let fake = Fake::new(BASE + 10);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "z", &[line(BASE + 1, "first")]);
    let first = ingest_available(&fake, &svc).unwrap();
    assert_eq!(
        first.converted, 1,
        "available immutable object must not wait 120s"
    );
    assert_eq!(discover_ips(&fake), ["first"]);

    fake.now.set((BASE + 20) as u64);
    let late = put_raw(&fake, BASE, "a", &[line(BASE + 2, "same-minute")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 15);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(discover_ips(&fake), ["first", "same-minute"]);

    fake.now.set((BASE + 70) as u64);
    let late = put_raw(&fake, BASE, "b", &[line(BASE + 3, "previous-minute")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 65);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(
        discover_ips(&fake),
        ["first", "previous-minute", "same-minute"]
    );
    for now in [80, 190, 310] {
        fake.now.set((BASE + now) as u64);
        assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 0);
    }
    assert_eq!(
        discover_ips(&fake),
        ["first", "previous-minute", "same-minute"]
    );
}

#[test]
fn ten_second_available_object_lost_worker_reply_retries_without_duplicate_commit() {
    let fake = Fake::new(BASE + 10);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "z", &[line(BASE + 1, "retried")]);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 0);
    assert_eq!(discover_ips(&fake), Vec::<String>::new());
    fake.now.set((BASE + 10) as u64 + coordinator::LEASE_S + 1);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(discover_ips(&fake), ["retried"]);
    fake.now.set(fake.now.get() + 10);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 0);
    assert_eq!(discover_ips(&fake), ["retried"]);
}

#[test]
fn ten_second_clock_commits_available_objects_within_one_poll_period() {
    let fake = Fake::new(BASE + 10);
    tick_runtime(&fake);
    let mut svc = service();
    svc.log_period = 10;
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
    put_raw(&fake, BASE, "available", &[line(BASE + 1, "clock")]);
    let available_at = fake.now.get();
    let mut committed_at = None;
    for elapsed in 0..=10 {
        fake.now.set(available_at + elapsed);
        run_tick(&fake);
        assert!(fake.calls.get() <= 32);
        assert!(fake.worker_max_calls.get() <= 32);
        let committed = fake
            .kv
            .borrow()
            .get(ingest::WATERMARK_KEY)
            .is_some_and(|(_, root)| root["catalog"]["generation"].as_u64().unwrap_or(0) > 0);
        if committed {
            committed_at = Some(fake.now.get());
            break;
        }
    }
    let committed_at = committed_at.expect("available-object to committed catalog exceeded 10s");
    assert!(committed_at - available_at <= 10);
    assert_eq!(discover_ips(&fake), ["clock"]);
}

#[test]
fn failed_root_publication_does_not_publish_receipts_or_duplicate_a_retry() {
    let fake = Fake::new(BASE + 10);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "z", &[line(BASE + 1, "root-retry")]);
    fake.fail_watermark.set(true);
    assert_eq!(
        ingest_available(&fake, &svc).unwrap_err(),
        "injected_watermark_failure"
    );
    assert_eq!(discover_ips(&fake), Vec::<String>::new());
    fake.fail_watermark.set(false);
    fake.now.set(fake.now.get() + coordinator::LEASE_S + 1);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(discover_ips(&fake), ["root-retry"]);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 0);
    assert_eq!(discover_ips(&fake), ["root-retry"]);
}

fn pending_source_request(fake: &Fake, svc: &Service) -> fastly_log_analytics::http::Request {
    let kv = fake.kv.borrow();
    let plan = &kv[ingest::PLAN_KEY].1;
    let previous = kv.get(ingest::WATERMARK_KEY);
    let root = previous
        .map(|(_, root)| root.clone())
        .unwrap_or(Value::Null);
    let body = json!({
        "maintenance": "source_index", "service_id": svc.service_id,
        "region": svc.fos_region, "bucket": svc.fos_bucket, "prefix": ingest::scope(svc),
        "plan_id": plan["plan_id"], "attempt": plan["attempt"],
        "base_generation": previous.map(|(generation, _)| generation.to_string()),
        "base_identity": hex::encode(Sha256::digest(serde_json::to_vec(&root).unwrap())),
        "root": root, "keys": plan["raw_keys"], "stop_ms": 31_000,
    });
    fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: ingest::worker::PATH.into(),
        headers: [("x-flp-worker-key".into(), "fos-secret".into())].into(),
        body: body.to_string().into_bytes(),
    }
}

fn helper_response(fake: &Fake, request: &fastly_log_analytics::http::Request) -> Value {
    let response = fake.execution("source_index_worker", || {
        let response = ingest::worker::handle_worker_ingest(fake, request);
        assert!(fake.calls.get() <= 32);
        response
    });
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

#[test]
fn source_index_peer_requires_existing_worker_auth_and_rejects_stale_root() {
    let fake = Fake::new(BASE + 10);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "a", &[line(BASE, "private-ip")]);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    ingest_available(&fake, &svc).unwrap();
    let request = pending_source_request(&fake, &svc);
    let unauthorized = fastly_log_analytics::http::Request {
        method: request.method.clone(),
        path: request.path.clone(),
        headers: Default::default(),
        body: request.body.clone(),
    };
    let response = fake.execution("source_index_worker", || {
        ingest::worker::handle_worker_ingest(&fake, &unauthorized)
    });
    assert_eq!(response.status, 403);
    assert_eq!(String::from_utf8(response.body).unwrap(), "Forbidden");
    assert!(fake.keys("processed/_sources/").is_empty());

    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (1, json!({"run_id": "competing-root"})),
    );
    let before = fake.kv.borrow().clone();
    let stale = helper_response(&fake, &request);
    assert_eq!(stale["status"], "stale");
    assert_eq!(stale["noop_reason"], "source_index_base_mismatch");
    let metrics = fastly_log_analytics::telemetry::execution::WorkerExecutionMetrics::from_reply(
        "catalog",
        None,
        Some(&stale),
    );
    assert_eq!(
        serde_json::to_value(metrics).unwrap()["noop_reason"],
        "source_index_base_mismatch"
    );
    assert_eq!(*fake.kv.borrow(), before);
    assert!(fake.keys("processed/_sources/").is_empty());

    let retry = pending_source_request(&fake, &svc);
    assert_eq!(helper_response(&fake, &retry)["status"], "ok");
}

#[test]
fn source_index_peer_does_not_classify_a_superseded_plan_as_a_base_mismatch() {
    let fake = Fake::new(BASE + 10);
    let svc = service();
    put_raw(&fake, BASE, "a", &[line(BASE, "private-ip")]);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    ingest_available(&fake, &svc).unwrap();
    let request = pending_source_request(&fake, &svc);
    fake.kv.borrow_mut().get_mut(ingest::PLAN_KEY).unwrap().1["attempt"] = 2.into();
    let before = fake.kv.borrow().clone();

    let stale = helper_response(&fake, &request);
    assert_eq!(stale["status"], "stale");
    assert!(stale.get("noop_reason").is_none());
    assert_eq!(*fake.kv.borrow(), before);
    assert!(fake.keys("processed/_sources/").is_empty());
}

#[test]
fn source_index_peer_checkpoint_resumes_without_publishing_a_root() {
    let fake = Fake::new(BASE + 10);
    let svc = service();
    for minute in 0..10 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "row")],
        );
    }
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    ingest_available(&fake, &svc).unwrap();
    let request = pending_source_request(&fake, &svc);
    let mut finished = false;
    for _ in 0..10 {
        let reply = helper_response(&fake, &request);
        assert!(reply["calls"].as_u64().unwrap() <= 32);
        assert!(!fake.kv.borrow().contains_key(ingest::WATERMARK_KEY));
        if reply["status"] == "ok" {
            finished = true;
            break;
        }
        assert_eq!(reply["status"], "pending");
    }
    assert!(finished, "bounded peer work must make durable progress");
    assert!(!fake.keys("processed/_sources/").is_empty());
}

#[test]
fn lost_source_index_peer_reply_reuses_immutable_receipts_on_retry() {
    let fake = Fake::new(BASE + 10);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE, "a", &[line(BASE + 1, "source-retry")]);
    fake.worker_faults
        .borrow_mut()
        .extend([WorkerFault::Ok, WorkerFault::LostReply]);
    let pending = ingest_available(&fake, &svc).unwrap();
    assert!(pending.more_pending);
    assert_eq!(discover_ips(&fake), Vec::<String>::new());
    let receipts = fake.keys("processed/_sources/");
    assert!(!receipts.is_empty());
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    assert_eq!(fake.keys("processed/_sources/"), receipts);
    assert_eq!(discover_ips(&fake), ["source-retry"]);
}

#[test]
fn scheduled_forward_traffic_does_not_starve_a_same_minute_late_object() {
    let fake = Fake::new(BASE + 10);
    tick_runtime(&fake);
    let mut svc = service();
    svc.log_period = 10;
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
    put_raw(&fake, BASE, "z", &[line(BASE + 1, "initial")]);
    for now in 10..20 {
        fake.now.set((BASE + now) as u64);
        run_tick(&fake);
        assert!(fake.calls.get() <= 32);
    }
    let late = put_raw(&fake, BASE, "a", &[line(BASE + 2, "late")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 15);
    put_raw(&fake, BASE, "zz", &[line(BASE + 11, "forward")]);
    for now in 20..=30 {
        fake.now.set((BASE + now) as u64);
        run_tick(&fake);
        assert!(fake.calls.get() <= 32);
    }
    assert_eq!(discover_ips(&fake), ["forward", "initial", "late"]);
}

#[test]
fn retained_prefix_sweep_uses_receipts_not_upload_skew_after_the_hot_window() {
    let fake = Fake::new(BASE + 239);
    let mut svc = service();
    svc.log_period = 10;
    let mut options = Options::request(&fake);
    options.sweep = true;
    let run = |fake: &Fake| {
        fake.new_request();
        let report = coordinator::coordinate(
            fake,
            &svc,
            "ak",
            "sk",
            Instant::now(),
            &LogBuffer::new(SID, "retained-freshness"),
            &options,
        )
        .unwrap();
        assert!(fake.calls.get() <= 32);
        assert!(fake.worker_max_calls.get() <= 32);
        report
    };
    put_raw(&fake, BASE, "z", &[line(BASE, "retained")]);
    assert_eq!(run(&fake).converted, 1);
    fake.now.set((BASE + 245) as u64);
    let late = put_raw(&fake, BASE, "a", &[line(BASE + 1, "outside-hot")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 245);
    for now in 245..=255 {
        fake.now.set((BASE + now) as u64);
        run(&fake);
        if discover_ips(&fake).len() == 2 {
            break;
        }
    }
    assert_eq!(discover_ips(&fake), ["outside-hot", "retained"]);
    fake.now.set((BASE + 400) as u64);
    run(&fake);
    fake.now.set((BASE + 530) as u64);
    run(&fake);
    assert_eq!(discover_ips(&fake), ["outside-hot", "retained"]);
}

fn scheduled_ingest(fake: &Fake, svc: &Service) -> ingest::Report {
    fake.new_request();
    let mut options = Options::request(fake);
    options.sweep = true;
    let report = coordinator::coordinate(
        fake,
        svc,
        "ak",
        "sk",
        Instant::now(),
        &LogBuffer::new(SID, "scheduled-freshness"),
        &options,
    )
    .unwrap();
    assert!(fake.calls.get() <= 32);
    assert!(fake.worker_max_calls.get() <= 32);
    report
}

#[test]
fn retained_sweep_makes_progress_under_continuous_forward_arrivals() {
    let fake = Fake::new(BASE + 600);
    let mut svc = service();
    svc.log_period = 10;
    put_raw(&fake, BASE + 600, "first", &[line(BASE + 600, "initial")]);
    scheduled_ingest(&fake, &svc);
    let late = put_raw(&fake, BASE, "late", &[line(BASE, "older-late")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 601);
    for now in (610..=1000).step_by(10) {
        fake.now.set((BASE + now) as u64);
        put_raw(
            &fake,
            BASE + now,
            &format!("forward-{now}"),
            &[line(BASE + now, "forward")],
        );
        scheduled_ingest(&fake, &svc);
        if processed_ips(&fake).iter().any(|ip| ip == "older-late") {
            return;
        }
    }
    panic!("retained-prefix ingestion starved behind ongoing forward/hot arrivals");
}

#[test]
fn disabled_retention_does_not_exclude_a_raw_prefix_older_than_one_day() {
    let now = BASE + 3 * DAY;
    let fake = Fake::new(now);
    let mut svc = service();
    svc.log_period = 10;
    svc.log_retention_days = 0;
    put_raw(&fake, now, "first", &[line(now, "initial")]);
    scheduled_ingest(&fake, &svc);
    put_raw(&fake, BASE, "old-late", &[line(BASE, "historical")]);
    for step in 1..=4 {
        fake.now.set((now + step * 120) as u64);
        scheduled_ingest(&fake, &svc);
    }
    assert_eq!(processed_ips(&fake), ["historical", "initial"]);
}

#[test]
fn retained_raw_backfill_reuses_receipts_after_delete_queue_consumption() {
    let fake = Fake::new(BASE + 3600);
    let mut svc = service();
    svc.delete_after = false;
    let raw = put_raw(&fake, BASE, "a", &[line(BASE, "retained-row")]);
    assert_eq!(ingest_available(&fake, &svc).unwrap().converted, 1);
    delete_tick(&fake, &svc);
    assert!(deleteq(&fake).is_empty());
    assert!(fake.bucket.borrow().contains_key(&raw));
    let report = backfill(&fake, "");
    assert_eq!((report.converted, report.failed), (0, 0));
    assert_eq!(discover_ips(&fake), ["retained-row"]);
}
