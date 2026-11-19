use super::support::*;
use fastly_log_analytics::{
    ingest::worker,
    telemetry::{self, LogKind},
};
use serde_json::{json, Value};

#[test]
fn relayed_fanout_runs_more_shards_than_worker_calls() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    let report = tick(&fake).unwrap();
    assert_eq!(report.shards_dispatched, 8, "{}", plan(&fake));
    assert_eq!(fake.relay_runs.get(), 2);
    let catalog_workers = report
        .worker_executions_metrics
        .iter()
        .filter(|sample| sample.kind == "catalog")
        .count() as u32;
    assert_eq!(
        fake.worker_runs.get(),
        report.shards_dispatched + catalog_workers
    );
    assert_eq!(
        fake.paths
            .borrow()
            .iter()
            .filter(|p| *p == worker::RELAY_PATH)
            .count(),
        2
    );
    assert!(committed(&fake));
    drain(&fake, 4);
    assert_eq!(processed_ips(&fake), sorted(expected));
}

#[test]
fn capacity_telemetry_retains_relay_and_lost_child_execution_coverage() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults.borrow_mut().push_back(Fault::LostReply);
    let report = tick(&fake).unwrap();
    let value = serde_json::to_value(&report).unwrap();
    let samples = value["worker_executions_metrics"].as_array().unwrap();
    assert_eq!(
        samples
            .iter()
            .filter(|s| s["kind"] == "ingest_relay")
            .count(),
        2
    );
    assert_eq!(
        samples
            .iter()
            .filter(|s| s["kind"] == "request_ingest")
            .count(),
        8
    );
    assert_eq!(samples.iter().filter(|s| s["answered"] == false).count(), 5);
    for sample in samples.iter().filter(|s| s["answered"] == false) {
        assert!(sample["vcpu_ms"].is_null(), "{sample}");
        assert!(sample["calls"].is_null(), "{sample}");
        assert!(sample["heap_mib"].is_null(), "{sample}");
    }
    assert!(samples.iter().all(|s| s["heap_mib"].is_null()));
}

#[test]
fn cron_runs_count_the_workers_each_relay_dispatched_even_when_its_reply_is_lost() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults.borrow_mut().push_back(Fault::LostReply);
    fake.kv.borrow_mut().insert(
        "admin:install".into(),
        (1, json!({"service_id": SID, "job_id": null, "fence": 0})),
    );
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(service()).unwrap()),
    );
    fake.new_request();
    fastly_log_analytics::admin::record_tick(&fake, "clock", "clock").unwrap();
    assert_eq!(fake.relay_runs.get(), 2, "{:?}", fake.paths.borrow());
    assert_eq!(fake.worker_runs.get(), 8);
    fake.new_request();
    let logged: i64 = telemetry::read_live(&fake, SID, LogKind::CronRuns, None, Some(2000), None)
        .unwrap()
        .rows
        .iter()
        .map(|r| r["worker_executions"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(
        logged,
        fake.paths.borrow().len() as i64,
        "{:?}",
        fake.paths.borrow()
    );
}

#[test]
fn without_relay_the_width_stays_the_worker_call_count() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    let report = tick(&fake).unwrap();
    assert_eq!(report.shards_dispatched, 2);
    assert_eq!(fake.relay_runs.get(), 0);
    drain(&fake, 12);
    assert_eq!(processed_ips(&fake), sorted(expected));
}

#[test]
fn a_failed_or_lost_relay_retries_its_shards_without_loss_or_duplicates() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults
        .borrow_mut()
        .extend([Fault::Unreachable, Fault::LostReply]);
    tick(&fake).unwrap();
    assert!(!committed(&fake));
    assert_eq!(fake.worker_runs.get(), 4);
    let first = plan(&fake);
    assert_eq!(
        first["results"].as_object().map_or(0, |r| r.len()),
        0,
        "{first}"
    );
    assert_ne!(first["lease_until"], 0);
    drain(&fake, 6);
    let got = processed_ips(&fake);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, sorted(expected));
}


#[test]
fn a_relay_forwards_only_shard_requests_within_its_bounds() {
    let fake = Fake::new();
    let shard = serde_json::to_value(shard_request(&[raw_key(BASE, "x", "zst")])).unwrap();
    for path in [worker::RELAY_PATH, "/_internal/delete", "/_internal/stage"] {
        let response = relay_request(&fake, json!([[worker::PATH, shard], [path, shard]]));
        assert_eq!(response.status, 400, "{path}");
    }
    let many: Vec<Value> = (0..=worker::MAX_RELAY_SHARDS)
        .map(|_| json!([worker::PATH, shard]))
        .collect();
    assert_eq!(relay_request(&fake, Value::from(many)).status, 400);
    assert!(fake.paths.borrow().is_empty());
    let mut unauthorized = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: worker::RELAY_PATH.into(),
        headers: Default::default(),
        body: b"{}".to_vec(),
    };
    assert_eq!(
        worker::handle_worker_relay(&fake, &unauthorized).status,
        403
    );
    unauthorized
        .headers
        .insert("x-flp-worker-key".into(), "wk".into());
    assert_eq!(
        worker::handle_worker_relay(&fake, &unauthorized).status,
        400
    );
}
