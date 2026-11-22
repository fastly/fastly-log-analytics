use super::{freshness, support::*};
use fastly_log_analytics::{
    ingest::{self, api},
    telemetry::{self, LogKind},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

#[test]
fn sweep_deletes_whole_hour_folders_older_than_each_datasets_cutoff() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old_req = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let new_req = put_processed(&fake, "processed/request/", BASE + 20 * DAY, "1-2-a-n1");
    let old_rum = put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    let old_ops = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let new_ops = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 39 * DAY,
        "1-2-a-n1",
    );
    let mut svc = service();
    svc.rum_retention_days = 0;
    let mut settings = telemetry::OperationalLogSettings::default();
    settings
        .retention_days_by_kind
        .insert("cron_ticks".into(), 7);
    let report = sweep(&fake, &svc, &settings);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old_req) && bucket.contains_key(&new_req));
    assert!(bucket.contains_key(&old_rum), "0 days keeps forever");
    assert!(!bucket.contains_key(&old_ops) && bucket.contains_key(&new_ops));
    assert_eq!(report.deleted, 2);
    assert!(report.bytes >= 20);
}

#[test]
fn sweep_purges_the_service_edge_cache_only_after_deleting() {
    let fake = Fake::new(BASE + 40 * DAY);
    let fresh = put_processed(&fake, "processed/request/", BASE + 39 * DAY, "1-2-a-n1");
    sweep_request(&fake);
    assert!(fake.bucket.borrow().contains_key(&fresh));
    assert!(
        fake.api_calls.borrow().is_empty(),
        "nothing deleted, nothing purged"
    );

    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.now.set(fake.now.get() + DAY as u64);
    let report = sweep_request(&fake);
    assert_eq!(report.deleted, 1);
    assert!(!report.more_pending);
    assert_eq!(
        *fake.api_calls.borrow(),
        vec![(
            "POST".to_string(),
            format!("/service/v/purge/flp-fos-{SID}")
        )]
    );
    assert!(!fake.kv.borrow().contains_key("retention:purge"));
}

#[test]
fn failed_edge_purge_is_retried_by_the_next_sweep() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.api_status.set(503);
    let first = sweep_request(&fake);
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(
        first.more_pending,
        "a pending purge keeps the sweep running"
    );
    assert!(fake.kv.borrow().contains_key("retention:purge"));

    fake.api_status.set(200);
    let second = sweep_request(&fake);
    assert_eq!(second.deleted, 0);
    assert!(!second.more_pending);
    assert_eq!(fake.api_calls.borrow().len(), 2);
    assert!(!fake.kv.borrow().contains_key("retention:purge"));
    sweep_request(&fake);
    assert_eq!(fake.api_calls.borrow().len(), 2);
}

#[test]
fn sweep_without_room_to_purge_leaves_the_purge_pending() {
    let fake = Fake::new(BASE + 40 * DAY);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let svc = service();
    // Catalog read, state get, list, mark, delete and state CAS fit; purge does not.
    fake.new_request();
    fake.calls.set(BUDGET - 4 - 6);
    let report = ingest::retention::retention_sweep(
        &store(&fake, &svc),
        &Default::default(),
        4,
        Some("request"),
    )
    .unwrap();
    assert_eq!(report.deleted, 1);
    assert!(report.more_pending);
    assert!(fake.api_calls.borrow().is_empty());
    assert!(fake.kv.borrow().contains_key("retention:purge"));
}

#[test]
fn sweep_keeps_expired_day_compaction_only_when_its_day_is_still_inside_retention() {
    let fake = Fake::new(BASE + 40 * DAY);
    let day = |offset: i64| {
        let t = ingest::hour_dir(BASE + offset * DAY);
        t.rsplit_once("hour=").unwrap().0.to_owned()
    };
    let old = format!("processed/request/{}compact=a.parquet", day(0));
    let fresh = format!("processed/request/{}compact=b.parquet", day(20));
    for key in [&old, &fresh] {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    sweep(&fake, &service(), &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(fake.bucket.borrow().contains_key(&fresh));
}

#[test]
fn sweep_persists_each_batch_before_deleting_and_resumes_after_a_crash() {
    let fake = Fake::new(BASE + 40 * DAY);
    let keys: Vec<String> = (0..3)
        .map(|h| put_processed(&fake, "processed/request/", BASE + h * 3600, "1-2-a-n1"))
        .collect();
    let svc = service();
    // Catalog read, list, state get and persistence fit; delete does not.
    fake.new_request();
    fake.calls.set(BUDGET - 4 - 5);
    let first = ingest::retention::retention_sweep(
        &store(&fake, &svc),
        &Default::default(),
        4,
        Some("request"),
    )
    .unwrap();
    assert_eq!(first.deleted, 0);
    assert!(fake.delete_posts.borrow().is_empty());
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert_eq!(state["pending"].as_array().unwrap().len(), 3);
    let second = sweep(&fake, &svc, &Default::default());
    assert_eq!(second.deleted, 3);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert!(state["pending"].as_array().unwrap().is_empty());
    assert_eq!(state["done"], true);
}

#[test]
fn failed_keys_stay_pending_and_a_completed_generation_is_not_rescanned() {
    let fake = Fake::new(BASE + 40 * DAY);
    let key = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.fail_delete_keys.borrow_mut().insert(key.clone());
    let svc = service();
    let first = sweep(&fake, &svc, &Default::default());
    assert_eq!((first.deleted, first.failed), (0, 1));
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(sweep(&fake, &svc, &Default::default()).deleted, 1);
    fake.delete_posts.borrow_mut().clear();
    let again = sweep(&fake, &svc, &Default::default());
    assert_eq!(again.deleted, 0);
    assert!(!fake.bucket.borrow().contains_key(&key));
}

#[test]
fn shortening_retention_starts_a_new_generation_from_the_beginning() {
    let fake = Fake::new(BASE + 40 * DAY);
    let mid = put_processed(&fake, "processed/request/", BASE + 20 * DAY, "1-2-a-n1");
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let mut svc = service();
    sweep(&fake, &svc, &Default::default());
    assert!(fake.bucket.borrow().contains_key(&mid));
    let before = fake.kv.borrow()["retention:request"].1["generation"].clone();
    svc.log_retention_days = 10;
    sweep(&fake, &svc, &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&mid));
    assert_ne!(
        fake.kv.borrow()["retention:request"].1["generation"],
        before
    );
}

#[test]
fn unsupported_delete_objects_falls_back_to_bounded_worker_fanout() {
    let fake = Fake::new(BASE + 40 * DAY);
    fake.delete_status.set(501);
    let keys: Vec<String> = (0..70)
        .map(|i| {
            put_processed(
                &fake,
                "processed/request/",
                BASE + i * 60,
                &format!("{i}-a-n1"),
            )
        })
        .collect();
    let mut settings = telemetry::OperationalLogSettings {
        retention_fanout: 2,
        ..Default::default()
    };
    let report = sweep(&fake, &service(), &settings);
    assert_eq!(*fake.worker_calls.borrow(), vec![28, 28]);
    assert_eq!(report.deleted, 56);
    assert_eq!(report.failed, 14);
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert_eq!(state["pending"].as_array().unwrap().len(), 14);
    assert!(report.more_pending);
    settings.retention_fanout = 8;
    fake.worker_calls.borrow_mut().clear();
    assert_eq!(sweep(&fake, &service(), &settings).deleted, 14);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

#[test]
fn worker_delete_authenticates_bounds_and_scopes_keys() {
    let fake = Fake::new(BASE);
    let in_scope = "p/processed/request/year=2020/month=01/day=01/hour=00/window=a.parquet";
    fake.bucket.borrow_mut().insert(in_scope.into(), vec![1]);
    let call = |key: Option<&str>, body: Value| {
        let mut headers = BTreeMap::new();
        if let Some(key) = key {
            headers.insert("x-flp-worker-key".to_owned(), key.to_owned());
        }
        ingest::retention::handle_worker_delete(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: "/_internal/delete".into(),
                headers,
                body: body.to_string().into_bytes(),
            },
        )
    };
    let base = |keys: Vec<String>| {
        json!({"service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
               "prefix": "p/", "keys": keys})
    };
    assert_eq!(call(None, base(vec![in_scope.into()])).status, 403);
    assert_eq!(call(Some("wrong"), base(vec![in_scope.into()])).status, 403);
    let many: Vec<String> = (0..29)
        .map(|i| format!("p/processed/request/{i}.parquet"))
        .collect();
    assert_eq!(call(Some("fos-secret"), base(many)).status, 400);
    for bad in [
        "other/processed/request/a.parquet",
        "p/processed/../secret",
        "p/svc/a",
    ] {
        assert_eq!(
            call(Some("fos-secret"), base(vec![bad.into()])).status,
            400,
            "{bad}"
        );
    }
    assert!(fake.bucket.borrow().contains_key(in_scope));
    let ok = call(Some("fos-secret"), base(vec![in_scope.into()]));
    assert_eq!(ok.status, 200);
    let body: Value = serde_json::from_slice(&ok.body).unwrap();
    assert_eq!(body["deleted"], json!([in_scope]));
    assert!(body["logs"].is_array());
    assert!(!fake.bucket.borrow().contains_key(in_scope));
}

#[test]
fn ingested_files_reports_the_latest_state_per_raw_key_from_outbox_then_parquet() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    fake.fail_delete_keys.borrow_mut().insert(keys[2].clone());
    delete_tick(&fake, &service());
    let old = raw_key(BASE - 7200, "old");
    ops_parquet(
        &fake,
        LogKind::IngestFiles,
        BASE - 7200,
        &[
            json!({"timestamp": (BASE - 7200) * 1000, "event_id": "a", "service_id": SID,
                   "severity": "info", "run_id": "r0", "dataset": "request", "raw_key": old,
                   "raw_size": 9, "status": "converted", "state_version": 1, "rows": 4,
                   "raw_deleted": false}),
            json!({"timestamp": (BASE - 7000) * 1000, "event_id": "b", "service_id": SID,
                   "severity": "info", "run_id": "r1", "dataset": "request", "raw_key": keys[0],
                   "raw_size": 9, "status": "converted", "state_version": 1, "rows": 1,
                   "raw_deleted": false}),
        ],
    );
    let page = admin_page(&fake, None, "").unwrap();
    let files = page["files"].as_array().unwrap();
    assert_eq!(files.len(), 4);
    let by_key = |key: &str| files.iter().find(|f| f["key"] == key).unwrap().clone();
    assert_eq!(by_key(&keys[0])["raw_deleted"], true);
    assert_eq!(by_key(&keys[1])["raw_deleted"], true);
    assert_eq!(by_key(&keys[2])["raw_deleted"], false);
    assert_eq!(by_key(&old)["raw_deleted"], false);
    assert_eq!(by_key(&old)["status"], "converted");
    let minute = by_key(&keys[0]);
    assert_eq!(minute["start"], ingest::rfc3339(BASE));
    assert_eq!(minute["end"], ingest::rfc3339(BASE + 60));
    assert_eq!(minute["rows"], 1);
    assert!(minute["size"].as_u64().unwrap() > 0);
    assert!(minute["last_modified"].is_string());
    assert_eq!(
        (page["complete"].clone(), page["next_cursor"].clone()),
        (json!(true), Value::Null)
    );
    assert_eq!(page["skipped_objects"], 0);
    assert!(fake
        .cdn_gets
        .borrow()
        .iter()
        .any(|k| k.starts_with("processed/ops/ingest_files/")));
}

/// Live, one archived page held ~27k `ingest_files` rows over ~10k raw keys, and filling the
/// `rows`/`size` a deleted state inherits rescanned every row per file: the request hit the
/// 60 s Compute limit and the Ingestion History tab showed a 503.
#[test]
fn ingested_files_inherits_rows_and_size_in_one_pass_over_a_large_archived_page() {
    let fake = Fake::new(BASE + 3600);
    let keys = 20_000;
    let mut rows = Vec::with_capacity(keys * 3);
    for n in 0..keys {
        let raw = raw_key(BASE - 7200, &format!("f{n:05}"));
        let at = (BASE - 7200) * 1000 + n as i64;
        for (version, status, deleted) in [
            (1, "converted", false),
            (2, "deleting", false),
            (3, "deleted", true),
        ] {
            let mut row = json!({"timestamp": at + version, "event_id": format!("{n}-{version}"),
                   "service_id": SID, "severity": "info", "run_id": "r0", "dataset": "request",
                   "raw_key": raw, "status": status, "state_version": version,
                   "raw_deleted": deleted});
            if version == 1 {
                row["raw_size"] = json!(9);
                row["rows"] = json!(n + 1);
            }
            rows.push(row);
        }
    }
    ops_parquet(&fake, LogKind::IngestFiles, BASE - 7200, &rows);
    let started = Instant::now();
    let page = admin_page(&fake, None, "").unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
    let files = page["files"].as_array().unwrap();
    assert_eq!(files.len(), keys);
    let last = files
        .iter()
        .find(|f| f["key"].as_str().unwrap().contains("f19999"))
        .unwrap();
    assert_eq!(last["status"], "deleted");
    assert_eq!(last["rows"], 20_000);
    assert_eq!(last["size"], 9);
}

#[test]
fn ingested_files_splits_large_pages_within_budget_and_resumes_via_cursor() {
    let fake = Fake::new(BASE + 7200);
    // Write 3 archived ingest_files Parquet objects with 2,500 distinct raw keys each (7,500 total),
    // exceeding the per-page row/byte budget so a single Compute request stops early instead of
    // decoding 10 large compacted files into WASM memory at once.
    for batch in 0..3 {
        let mut rows = Vec::with_capacity(2_500);
        let at_s = BASE - 10_800 + batch * 3600;
        for n in 0..2_500 {
            let raw = raw_key(at_s, &format!("b{batch}-f{n:04}"));
            rows.push(json!({
                "timestamp": at_s * 1000 + n as i64,
                "event_id": format!("{batch}-{n}"),
                "service_id": SID,
                "severity": "info",
                "run_id": format!("r{batch}"),
                "dataset": "request",
                "raw_key": raw,
                "raw_size": 42,
                "status": "converted",
                "state_version": 1,
                "rows": n + 1,
                "raw_deleted": false
            }));
        }
        ops_parquet(&fake, LogKind::IngestFiles, at_s, &rows);
    }

    let first = admin_page(&fake, None, "").unwrap();
    assert_eq!(
        first["complete"], false,
        "large ingest_files listing must paginate before decoding all objects in one WASM call"
    );
    assert!(first["next_cursor"].is_string());

    let mut seen = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 10, "pagination must terminate");
        let query = cursor
            .as_deref()
            .map(|c| format!("cursor={c}"))
            .unwrap_or_default();
        let page = admin_page(&fake, None, &query).unwrap();
        for file in page["files"].as_array().unwrap() {
            seen.insert(file["key"].as_str().unwrap().to_owned());
        }
        if page["complete"] == true {
            assert!(page["next_cursor"].is_null());
            break;
        }
        cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
    }
    assert!(pages >= 2);
    assert_eq!(seen.len(), 7_500);
}

#[test]
fn files_dataset_selects_request_rum_or_one_ops_kind() {
    let fake = Fake::new(BASE);
    for (prefix, name) in [
        ("processed/request/", 1),
        ("processed/rum/client_vitals/", 2),
        ("processed/ops/cron_ticks/", 3),
        ("processed/ops/platform_errors/", 4),
    ] {
        let (start, end) = if prefix.contains("ops") {
            (BASE / 60 + name, BASE / 60 + name)
        } else {
            (BASE + name, BASE + name + 60)
        };
        put_processed(&fake, prefix, BASE, &format!("{start}-{end}-a-n{name}"));
    }
    publish_files(&fake, &fake.keys("processed/request/"), &[], &[]);
    let keys = |dataset: &str| -> Vec<String> {
        files_for(&fake, dataset).unwrap()["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect()
    };
    assert!(keys("rum")[0].starts_with("processed/rum/client_vitals/"));
    assert!(keys("ops:cron_ticks")[0].starts_with("processed/ops/cron_ticks/"));
    assert_eq!(keys("ops:cron_ticks").len(), 1);
    assert!(keys("request")[0].starts_with("processed/request/"));
    for bad in [
        "ops",
        "ops:",
        "ops:nope",
        "ops:../x",
        "raw",
        "ops:cron_ticks/x",
    ] {
        assert_eq!(files_for(&fake, bad).unwrap_err().0, 400, "{bad}");
    }
    let no_param = files_page(
        &fake,
        &format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600)),
    )
    .unwrap();
    assert!(no_param["files"][0]["key"]
        .as_str()
        .unwrap()
        .starts_with("processed/request/"));
}

#[test]
fn files_cursor_is_bound_to_its_dataset() {
    let fake = Fake::new(BASE);
    for i in 0..600 {
        put_processed(
            &fake,
            "processed/rum/client_errors/",
            BASE,
            &format!("{}-{}-a-n1", BASE + i, BASE + i + 60),
        );
    }
    let first = files_for(&fake, "rum").unwrap();
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    for dataset in ["ops:cron_ticks", "request"] {
        let query = format!(
            "from={}&to={}&dataset={dataset}&cursor={cursor}",
            iso_url(BASE),
            iso_url(BASE + 3600)
        );
        assert_eq!(files_page(&fake, &query).unwrap_err().0, 400, "{dataset}");
    }
}

#[test]
fn file_reads_request_rum_and_ops_roots_only() {
    let fake = Fake::new(BASE);
    let get = |k: &str| {
        fake.new_request();
        api::file(&fake, &service(), &format!("key={k}"), None)
            .map(|upstream| fastly_log_analytics::shell::fetch_upstream(&fake, &upstream).body)
    };
    for good in [
        "processed/request/year=2027/a.parquet",
        "processed/rum/year=2027/a.parquet",
        "processed/ops/cron_ticks/year=2027/a.parquet",
    ] {
        fake.bucket.borrow_mut().insert(good.into(), vec![7]);
        assert_eq!(get(good).unwrap(), vec![7], "{good}");
        assert!(api::valid_processed_key(&service(), good));
    }
    for bad in [
        "processed/other/a.parquet",
        "processed/a.parquet",
        "processed/rum/../request/a.parquet",
        "processed/ops//a.parquet",
        "processed/ops/a.txt",
        "processed/rumx/a.parquet",
    ] {
        assert_eq!(get(bad).unwrap_err().0, 400, "{bad}");
    }
    assert!(api::is_ops_key(
        &service(),
        "processed/ops/cron_ticks/a.parquet"
    ));
    assert!(!api::is_ops_key(&service(), "processed/rum/a.parquet"));
}

#[test]
fn rum_file_listing_pages_through_both_rum_tables() {
    let fake = Fake::new(BASE);
    let hour = BASE - BASE.rem_euclid(3600);
    let mut expected = std::collections::BTreeSet::new();
    for (table, count) in [("client_errors", 3), ("client_vitals", api::FILES_PAGE + 3)] {
        for i in 0..count {
            let key = ingest::processed_key(
                &format!("processed/rum/{table}/"),
                hour,
                hour + 60,
                &format!("source{i:04}"),
                1,
            );
            fake.bucket.borrow_mut().insert(key.clone(), vec![1, 2, 3]);
            expected.insert(key);
        }
    }
    let query = format!(
        "from={}&to={}&dataset=rum",
        iso_url(hour),
        iso_url(hour + 3600)
    );
    let mut listed = std::collections::BTreeSet::new();
    let mut page = files_page(&fake, &query).unwrap();
    for _ in 0..10 {
        for file in page["files"].as_array().unwrap() {
            assert!(listed.insert(file["key"].as_str().unwrap().to_owned()));
        }
        if page["complete"] == true {
            break;
        }
        let cursor = page["next_cursor"].as_str().unwrap().to_owned();
        page = files_page(&fake, &format!("{query}&cursor={cursor}")).unwrap();
    }
    assert_eq!(page["complete"], true);
    assert_eq!(listed, expected);
}

#[test]
fn storage_tree_can_list_the_ops_and_rum_roots() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    let ops = {
        fake.new_request();
        api::tree(&fake, &service(), "dataset=ops", Instant::now(), false).unwrap()
    };
    assert_eq!(ops["root"], "processed/ops/");
    assert_eq!(ops["nodes"][0]["name"], "cron_ticks");
    fake.new_request();
    let rum = api::tree(
        &fake,
        &service(),
        "dataset=rum&prefix=year%3D2027%2F",
        Instant::now(),
        false,
    )
    .unwrap();
    assert_eq!(rum["root"], "processed/rum/");
    fake.new_request();
    assert_eq!(
        api::tree(&fake, &service(), "dataset=nope", Instant::now(), false)
            .unwrap_err()
            .0,
        400
    );
    fake.new_request();
    assert_eq!(
        api::tree(&fake, &service(), "dataset=ops", Instant::now(), true)
            .unwrap_err()
            .0,
        400
    );
}

#[test]
fn storage_tree_replays_the_same_cursor_after_a_failed_list() {
    let fake = Fake::new(BASE);
    for i in 0..=1000 {
        fake.bucket
            .borrow_mut()
            .insert(format!("processed/request/file-{i:04}.parquet"), vec![1]);
    }
    let first = admin_page(&fake, Some(false), "dataset=request").unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let query = format!("dataset=request&{}", cursor_query(cursor));
    fake.transient
        .borrow_mut()
        .push_back(("LIST", 503, Some(1)));

    assert_eq!(admin_page(&fake, Some(false), &query).unwrap_err().0, 502);

    let retried = admin_page(&fake, Some(false), &query).unwrap();
    let first_keys: Vec<_> = first["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["key"].as_str().unwrap())
        .collect();
    let retry_keys: Vec<_> = retried["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["key"].as_str().unwrap())
        .collect();
    assert_eq!(
        first_keys
            .iter()
            .chain(&retry_keys)
            .collect::<Vec<_>>()
            .len(),
        1001
    );
    let first_keys: BTreeSet<_> = first_keys.into_iter().collect();
    let retry_keys: BTreeSet<_> = retry_keys.into_iter().collect();
    assert!(first_keys.is_disjoint(&retry_keys));
    assert_eq!(retried["complete"], true);
}

#[test]
fn history_sweep_deletes_one_kind_up_to_its_fixed_cutoff_and_resumes() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let mid = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 30 * DAY,
        "1-2-a-n1",
    );
    let other = put_processed(&fake, "processed/ops/cron_runs/", BASE, "1-2-a-n1");
    let svc = service();
    let cutoff = BASE + 20 * DAY;
    fake.new_request();
    let report = ingest::retention::history_sweep(
        &store(&fake, &svc),
        LogKind::CronTicks,
        cutoff,
        &Default::default(),
        0,
    )
    .unwrap();
    assert!(!report.more_pending);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old));
    assert!(bucket.contains_key(&mid) && bucket.contains_key(&other));
    drop(bucket);
    assert!(fake.kv.borrow().contains_key("history:cron_ticks"));
    assert!(!fake.kv.borrow().contains_key("retention:ops:cron_ticks"));
}

#[test]
fn history_sweep_with_a_future_cutoff_clears_the_whole_kind() {
    let fake = Fake::new(BASE + 40 * DAY);
    let keys: Vec<String> = (0..3)
        .map(|h| {
            put_processed(
                &fake,
                "processed/ops/fos_usage/",
                BASE + 39 * DAY + h * 3600,
                "1-2-a-n1",
            )
        })
        .collect();
    fake.new_request();
    ingest::retention::history_sweep(
        &store(&fake, &service()),
        LogKind::FosUsage,
        BASE + 41 * DAY,
        &Default::default(),
        0,
    )
    .unwrap();
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

#[test]
fn storage_reconcile_does_nothing_until_requested() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    assert!(reconcile(&fake).is_none());
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
}

#[test]
fn storage_reconcile_folds_excess_ops_kind_folders_into_other() {
    let fake = Fake::new(BASE);
    let total = ingest::retention::MAX_RECONCILE_KINDS + 5;
    for i in 0..total {
        put_processed(
            &fake,
            &format!("processed/ops/kind{i:03}/"),
            BASE,
            "1-2-a-n1",
        );
    }
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let mut done = None;
    for _ in 0..8 {
        if let Some(snapshot) = reconcile(&fake).and_then(|r| r.snapshot) {
            done = Some(snapshot);
            break;
        }
    }
    let by_kind = done.expect("completes")["datasets"]["pq_ops"]["by_kind"].clone();
    let by_kind = by_kind.as_object().unwrap();
    assert_eq!(by_kind.len(), ingest::retention::MAX_RECONCILE_KINDS + 1);
    assert_eq!(by_kind["other"]["objects"], 5);
}

#[test]
fn storage_reconcile_persists_its_cursor_once_per_step() {
    // A KV read right after a write can still return the old generation, so a step that
    // re-read its own write to CAS again lost that CAS and stalled as `busy` after one page.
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let generation = || fake.kv.borrow()["storage:reconcile"].0;
    let before = generation();
    fake.new_request();
    fake.calls.set(BUDGET - 8);
    let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
        .unwrap()
        .unwrap();
    assert!(report.snapshot.is_none(), "budget stops it mid-way");
    assert_eq!(generation(), before + 1);
    let state = fake.kv.borrow()["storage:reconcile"].1.clone();
    assert!(state["index"].as_u64().unwrap() >= 3, "{state}");
}

#[test]
fn storage_reconcile_sums_bytes_and_objects_per_dataset_across_resumed_steps() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/request/", BASE + 3600, "1-2-a-n1");
    put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    fake.bucket.borrow_mut().insert(
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.zst".into(),
        vec![0; 5],
    );
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    assert!(
        !ingest::retention::request_storage_reconcile(&fake, "admin").unwrap(),
        "a running reconcile is not restarted"
    );
    // Two LIST pages per request: each step advances the persisted cursor.
    let mut done = None;
    for _ in 0..8 {
        fake.new_request();
        fake.calls.set(BUDGET - 4);
        let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
            .unwrap()
            .unwrap();
        if let Some(snapshot) = report.snapshot {
            done = Some(snapshot);
            break;
        }
    }
    let snapshot = done.expect("completes");
    assert_eq!(
        snapshot["datasets"]["pq_request"],
        json!({"bytes": 20, "objects": 2})
    );
    assert_eq!(
        snapshot["datasets"]["pq_rum"],
        json!({"bytes": 10, "objects": 1})
    );
    assert_eq!(snapshot["datasets"]["pq_ops"]["objects"], 1);
    assert_eq!(
        snapshot["datasets"]["pq_ops"]["by_kind"]["cron_ticks"]["bytes"],
        10
    );
    assert_eq!(
        snapshot["datasets"]["raw_request"],
        json!({"bytes": 5, "objects": 1})
    );
    assert_eq!(
        snapshot["datasets"]["raw_rum"],
        json!({"bytes": 0, "objects": 0})
    );
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
    assert!(reconcile(&fake).is_none());
}

#[test]
fn operational_settings_patch_merges_validates_persists_and_reports_changes() {
    let fake = Fake::new(BASE);
    let (saved, changed) = patch_settings(
        &fake,
        json!({"flush_max_rows": 1000, "retention_days_by_kind": {"cron_ticks": 3}}),
    )
    .unwrap();
    assert_eq!(changed, vec!["flush_max_rows", "retention_days_by_kind"]);
    assert_eq!(saved.flush_max_rows, 1000);
    assert_eq!(saved.retention_days_by_kind["cron_ticks"], 3);
    assert_eq!(
        saved.retention_days_by_kind["cron_runs"], 30,
        "other kinds keep their days"
    );
    assert_eq!(telemetry::api::operational_settings(&fake), saved);
    let (_, none) = patch_settings(&fake, json!({"flush_max_rows": 1000})).unwrap();
    assert!(none.is_empty(), "an unchanged value is not reported");
    for bad in [
        json!({"nope": 1}),
        json!({"log_idle_ticks": true}),
        json!({"flush_max_rows": 10}),
        json!({"flush_max_rows": "many"}),
        json!({"retention_days_by_kind": {"fos_usage": 5}}),
        json!({"retention_days_by_kind": {"cron_ticks": 0}}),
        json!({"retention_fanout": 17}),
        json!([1]),
    ] {
        assert!(patch_settings(&fake, bad.clone()).is_err(), "{bad}");
    }
    assert_eq!(telemetry::api::operational_settings(&fake), saved);
}

#[test]
fn an_invalid_stored_settings_record_falls_back_to_defaults() {
    let fake = Fake::new(BASE);
    fake.kv.borrow_mut().insert(
        "settings:operational_logs".into(),
        (1, json!({"flush_max_rows": 1, "retention_fanout": 99})),
    );
    assert_eq!(
        telemetry::api::operational_settings(&fake),
        telemetry::OperationalLogSettings::default()
    );
}

#[test]
fn logged_traffic_reports_the_compaction_setting() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let (status, body, _) = logs_call(&fake, "GET", "/api/admin/usage/logged-traffic", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["logging"]["compaction"],
        json!({"mode": "off", "size_target_mb": 128})
    );
}

#[test]
fn logged_traffic_reports_the_ingest_relay_setting() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let (status, body, _) = logs_call(&fake, "GET", "/api/admin/usage/logged-traffic", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["logging"]["ingest_relay"], json!(0));
}

#[test]
fn service_stats_sums_the_logged_service_fields_per_whole_hour() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let from_ms = (STATS_HOUR + 1200) * 1000;
    let to_ms = (STATS_HOUR + 3 * 3600 - 1200) * 1000;
    let path = format!("/api/admin/usage/service-stats?from={from_ms}&to={to_ms}");
    let (status, body, audit) = logs_call(&fake, "GET", &path, Value::Null);
    assert_eq!(status, 200, "{body}");
    assert!(audit.is_none());
    assert_eq!(
        body,
        json!({
            "service_id": SID, "from": STATS_HOUR + 3600, "to": STATS_HOUR + 2 * 3600,
            "sample_rate": 1, "edge_only": false,
            "hours": [{"start_time": STATS_HOUR + 3600, "requests": 1000, "bandwidth_bytes": 10000, "hits": 900}],
        })
    );
}

#[test]
fn service_stats_reads_edge_fields_when_logging_is_edge_only() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let mut svc = serde_json::to_value(service()).unwrap();
    svc["edge_only"] = json!(true);
    fake.kv.borrow_mut().insert(format!("svc:{SID}"), (2, svc));
    let path = format!(
        "/api/admin/usage/service-stats?from={}&to={}",
        STATS_HOUR * 1000,
        (STATS_HOUR + 3600) * 1000
    );
    let (status, body, _) = logs_call(&fake, "GET", &path, Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["edge_only"], json!(true));
    assert_eq!(
        body["hours"],
        json!([{"start_time": STATS_HOUR, "requests": 600, "bandwidth_bytes": 5500, "hits": 540}])
    );
}

#[test]
fn service_stats_rejects_bad_ranges_and_needs_an_installed_service() {
    let fake = Fake::new(BASE);
    let ok = format!(
        "/api/admin/usage/service-stats?from={}&to={}",
        STATS_HOUR * 1000,
        (STATS_HOUR + 3600) * 1000
    );
    assert_eq!(logs_call(&fake, "GET", &ok, Value::Null).0, 409);
    tick_runtime(&fake);
    let too_long = format!(
        "/api/admin/usage/service-stats?from={}&to={}",
        STATS_HOUR * 1000,
        (STATS_HOUR + 32 * 86_400) * 1000
    );
    for path in [
        "/api/admin/usage/service-stats?from=5000".to_string(),
        format!(
            "/api/admin/usage/service-stats?from={}&to={}",
            STATS_HOUR * 1000,
            STATS_HOUR * 1000
        ),
        too_long,
    ] {
        let (status, body, _) = logs_call(&fake, "GET", &path, Value::Null);
        assert_eq!(
            (status, &body["error"]),
            (400, &json!("invalid_range")),
            "{path}"
        );
    }
}

#[test]
fn platform_usage_fetches_all_sections_independently_without_leaking_identifiers() {
    let fake = Fake::new(BASE);
    fake.config_overrides.borrow_mut().extend([
        ("flp_service_id".into(), Some("app-private-id".into())),
        ("worker_service_id".into(), Some("worker-private-id".into())),
        (
            "ngwaf_workspace_id".into(),
            Some("workspace-private-id".into()),
        ),
    ]);
    fake.api_responses.borrow_mut().extend([
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "object_storage_bytes_count": 1_073_741_824,
                "object_storage_buckets_count": 2
            }], "next_cursor": null}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 2,
                "compute_request_time_billed_ms": 50,
                "compute_service_bereq_error": 1,
                "compute_cache_operations_count": 3,
                "compute_sandboxes": 4,
                "compute_service_resource_limits_error": 5
            }], "next_cursor": null}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 7,
                "compute_request_time_billed_ms": 100,
                "compute_service_bereq_error": 6,
                "compute_cache_operations_count": 8,
                "compute_sandboxes": 9,
                "compute_service_resource_limits_error": 10
            }], "next_cursor": null}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "ngwaf_requests_total_count": 20,
                "ngwaf_requests_allowed_count": 11,
                "ngwaf_requests_blocked_count": 3,
                "ngwaf_requests_challenged_count": 2,
                "ngwaf_requests_logged_count": 2,
                "ngwaf_requests_timeout_count": 1,
                "ngwaf_requests_unknown_count": 1
            }], "next_cursor": null}),
        ),
    ]);

    let (status, body, audit) = logs_call(
        &fake,
        "GET",
        "/api/admin/usage/platform?from=0&to=3600000",
        Value::Null,
    );

    assert_eq!(status, 200, "{body}");
    assert!(audit.is_none());
    assert_eq!(body["source"], "fastly");
    assert_eq!(body["storage"]["bytes"], 1_073_741_824u64);
    assert_eq!(body["compute"]["requests"], 9);
    assert_eq!(body["compute"]["resource_errors"], 15);
    assert_eq!(body["ngwaf"]["blocked"], 3);
    let wire = body.to_string();
    assert!(!wire.contains("private-id"));
    assert!(!wire.contains("fos-secret"));
    assert!(!wire.contains("next_cursor"));
    let queries = fake.api_queries.borrow();
    assert_eq!(queries.len(), 4);
    assert!(queries[0].0.contains("/aggregate/daily"));
    assert!(queries[1].0.contains("/entities/app-private-id/hourly"));
    assert!(queries[2].0.contains("/entities/worker-private-id/hourly"));
    assert!(queries[3]
        .0
        .contains("/entities/workspace-private-id/hourly"));
    assert!(queries[1].1.contains("compute_requests"));
    assert!(queries[1].1.contains("compute_service_bereq_error"));
}

#[test]
fn platform_usage_reports_not_configured_ngwaf_without_fetching_it() {
    let fake = Fake::new(BASE);
    fake.config_overrides
        .borrow_mut()
        .insert("ngwaf_workspace_id".into(), None);
    fake.api_responses.borrow_mut().extend([
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "object_storage_bytes_count": 1
            }]}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 1,
                "compute_request_time_billed_ms": 50
            }]}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 2,
                "compute_request_time_billed_ms": 100
            }]}),
        ),
    ]);

    let (status, body, _) = logs_call(
        &fake,
        "GET",
        "/api/admin/usage/platform?from=0&to=3600000",
        Value::Null,
    );

    assert_eq!(status, 200, "{body}");
    assert_eq!(body["source"], "fastly");
    assert_eq!(body["ngwaf"]["status"], "not_configured");
    assert_eq!(fake.api_queries.borrow().len(), 3);
}

#[test]
fn platform_usage_keeps_compute_after_a_storage_source_failure() {
    let fake = Fake::new(BASE);
    fake.config_overrides
        .borrow_mut()
        .insert("ngwaf_workspace_id".into(), None);
    fake.api_responses.borrow_mut().extend([
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-02T00:00:00Z",
                "object_storage_bytes_count": 1
            }]}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 1,
                "compute_request_time_billed_ms": 50
            }]}),
        ),
        (
            200,
            json!({"data": [{
                "timestamp": "1970-01-01T00:00:00Z",
                "compute_requests": 2,
                "compute_request_time_billed_ms": 100
            }]}),
        ),
    ]);

    let (status, body, _) = logs_call(
        &fake,
        "GET",
        "/api/admin/usage/platform?from=0&to=3600000",
        Value::Null,
    );

    assert_eq!(status, 200, "{body}");
    assert_eq!(body["source"], "partial");
    assert_eq!(body["storage"]["status"], "unavailable");
    assert_eq!(body["compute"]["status"], "ok");
    assert_eq!(body["compute"]["requests"], 3);
    assert_eq!(fake.api_queries.borrow().len(), 3);
}

#[test]
fn platform_usage_rejects_bad_ranges_before_making_platform_calls() {
    let fake = Fake::new(BASE);

    for path in [
        "/api/admin/usage/platform?from=0",
        "/api/admin/usage/platform?from=1000&to=1000",
        "/api/admin/usage/platform?from=0&to=2764800000",
    ] {
        let (status, body, _) = logs_call(&fake, "GET", path, Value::Null);
        assert_eq!((status, &body["error"]), (400, &json!("invalid_range")));
    }
    assert!(fake.api_queries.borrow().is_empty());
}

#[test]
fn geography_fetches_daily_region_metrics_once_from_the_front_service() {
    let fake = Fake::new(BASE);
    fake.config_overrides
        .borrow_mut()
        .insert("front_service_id".into(), Some("front-private-id".into()));
    fake.api_responses.borrow_mut().push_back((
        200,
        json!({"data": [
            {"timestamp":"1970-01-01T00:00:00Z","region":"europe","requests":7,"bandwidth":700},
            {"timestamp":"1970-01-01T00:00:00Z","region":"usa","requests":3,"bandwidth":300}
        ]}),
    ));

    let (status, body, audit) = logs_call(
        &fake,
        "GET",
        "/api/admin/usage/geography?start=0&end=86400000&group_by=region",
        Value::Null,
    );

    assert_eq!(status, 200, "{body}");
    assert!(audit.is_none());
    assert_eq!(body["source"], "fastly");
    assert_eq!(body["requests"], 10);
    assert_eq!(body["bandwidth_bytes"], 1_000);
    assert_eq!(body["data"][0]["location"], "europe");
    let calls = fake.api_queries.borrow();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].0.contains("/entities/front-private-id/daily"));
    assert!(calls[0].1.contains("metric=requests,bandwidth"));
    assert!(calls[0].1.contains("group_by=region"));
    assert!(!body.to_string().contains("front-private-id"));
}

#[test]
fn geography_defaults_to_region_and_rejects_bad_group_without_fetching() {
    let fake = Fake::new(BASE);
    fake.config_overrides
        .borrow_mut()
        .insert("front_service_id".into(), Some("front-id".into()));
    fake.api_responses.borrow_mut().push_back((
        200,
        json!({"data": [{
            "timestamp":"1970-01-01T00:00:00Z","region":"europe","requests":1,"bandwidth":2
        }]}),
    ));

    let (status, body, _) = logs_call(
        &fake,
        "GET",
        "/api/admin/usage/geography?start=0&end=86400000&group_by=country",
        Value::Null,
    );
    assert_eq!(
        (status, body["error"].clone()),
        (400, json!("invalid_group"))
    );
    assert!(fake.api_queries.borrow().is_empty());

    assert_eq!(
        logs_call(
            &fake,
            "GET",
            "/api/admin/usage/geography?start=0&end=86400000",
            Value::Null,
        )
        .0,
        200
    );
    assert!(fake.api_queries.borrow()[0].1.contains("group_by=region"));
}

#[test]
fn geography_sanitizes_unavailable_sources_and_parser_failures() {
    for configured in [false, true] {
        let fake = Fake::new(BASE);
        if configured {
            fake.config_overrides
                .borrow_mut()
                .insert("front_service_id".into(), Some("front-private-id".into()));
            fake.api_responses.borrow_mut().push_back((
                200,
                json!({"data": [{"timestamp":"bad","region":"secret-location"}],
                    "next_cursor":"secret-cursor"}),
            ));
        }

        let (status, body, _) = logs_call(
            &fake,
            "GET",
            "/api/admin/usage/geography?start=0&end=86400000&group_by=datacenter",
            Value::Null,
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["source"], "unavailable");
        assert_eq!(body["data"], json!([]));
        let wire = body.to_string();
        assert!(!wire.contains("private-id"));
        assert!(!wire.contains("secret-location"));
        assert!(!wire.contains("secret-cursor"));
    }
}

#[test]
fn logs_endpoints_get_and_patch_settings_with_an_audit_note() {
    let fake = Fake::new(BASE);
    let (status, body, audit) =
        logs_call(&fake, "GET", "/api/admin/operational-logging", Value::Null);
    assert_eq!(status, 200);
    assert_eq!(body["settings"]["flush_max_rows"], 500);
    assert_eq!(body["defaults"]["flush_max_rows"], 500);
    assert!(audit.is_none());
    let (status, body, audit) = logs_call(
        &fake,
        "PATCH",
        "/api/admin/operational-logging",
        json!({"flush_max_rows": 750, "compaction_enabled": false}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["settings"]["flush_max_rows"], 750);
    assert_eq!(
        body["changed"],
        json!(["compaction_enabled", "flush_max_rows"])
    );
    assert_eq!(
        audit,
        Some((
            "operational_logging_updated".into(),
            "compaction_enabled,flush_max_rows".into()
        ))
    );
    let (status, body, audit) = logs_call(
        &fake,
        "PATCH",
        "/api/admin/operational-logging",
        json!({"flush_max_rows": 1}),
    );
    assert_eq!((status, body["detail"].as_str().is_some()), (400, true));
    assert!(
        audit.is_none(),
        "a rejected patch is not audited as applied"
    );
    assert_eq!(
        logs_call(&fake, "PUT", "/api/admin/operational-logging", json!({})).0,
        404
    );
}

#[test]
fn logs_clear_cache_accepts_a_best_effort_kind_or_all_and_rejects_durable_kinds() {
    let fake = Fake::new(BASE);
    for bad in [
        json!({}),
        json!({"kind": "admin_audit"}),
        json!({"kind": "cron_runs"}),
        json!({"kind": "cron_ticks"}),
        json!({"kind": "nope"}),
        json!({"kind": 3}),
    ] {
        let (status, _, audit) =
            logs_call(&fake, "POST", "/api/admin/logs/clear-cache", bad.clone());
        assert_eq!(status, 400, "{bad}");
        assert!(audit.is_none());
    }
}

#[test]
fn delete_history_runs_as_a_resumable_job_ending_in_one_front_purge() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let old = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let new = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 39 * DAY,
        "1-2-a-n1",
    );
    let other = put_processed(&fake, "processed/ops/cron_runs/", BASE, "1-2-a-n1");
    let (status, body, audit) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_ticks", "older_than_days": 10}),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["job"]["status"], "complete");
    assert_eq!(
        audit,
        Some((
            "log_history_delete_started".into(),
            "cron_ticks:older_than_days=10".into()
        ))
    );
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old) && bucket.contains_key(&new) && bucket.contains_key(&other));
    drop(bucket);
    assert_eq!(
        *fake.api_calls.borrow(),
        vec![(
            "POST".to_string(),
            "/service/v/purge/flp-fos-ops-cron_ticks".to_string()
        )]
    );
    let (_, again, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_ticks"}),
    );
    assert_eq!(
        again["job"]["status"], "complete",
        "a finished job can be replaced"
    );
    assert!(
        !fake.bucket.borrow().contains_key(&new),
        "no older_than_days clears everything"
    );
    for bad in [
        json!({}),
        json!({"kind": "nope"}),
        json!({"kind": "cron_ticks", "older_than_days": -1}),
        json!({"kind": "cron_ticks", "older_than_days": 99999}),
    ] {
        assert_eq!(
            logs_call(&fake, "POST", "/api/admin/logs/delete-history", bad).0,
            400
        );
    }
}

#[test]
fn delete_history_refuses_a_second_job_and_resumes_from_the_tick() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let seeded: Vec<String> = (0..3)
        .map(|h| {
            put_processed(
                &fake,
                "processed/ops/cron_ticks/",
                BASE + h * 3600,
                "1-2-a-n1",
            )
        })
        .collect();
    // A starved request records the job but cannot finish it.
    fake.new_request();
    fake.calls.set(BUDGET - 10);
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: "/api/admin/logs/delete-history".into(),
        headers: Default::default(),
        body: json!({"kind": "cron_ticks"}).to_string().into_bytes(),
    };
    let first = fastly_log_analytics::admin::logs::handle(&fake, "a", &request);
    let body: Value = serde_json::from_slice(&first.response.body).unwrap();
    assert_ne!(body["job"]["status"], "complete");
    let (status, other, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_runs"}),
    );
    assert_eq!(
        (status, other["detail"].clone()),
        (409, json!("history_job_in_progress"))
    );
    for _ in 0..16 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(seeded.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    let (_, status_body, _) = logs_call(&fake, "GET", "/api/admin/logs/status", Value::Null);
    assert_eq!(status_body["delete_history"]["status"], "complete");
    // The retention sweep also expires these and purges the whole service once.
    let purges = fake.api_calls.borrow().clone();
    assert_eq!(
        purges
            .iter()
            .filter(|(_, p)| p.contains("/purge/flp-fos-ops-"))
            .count(),
        1
    );
    assert!(purges.len() <= 2);
}

#[test]
fn sweep_and_reconcile_endpoints_run_on_demand_and_are_audited() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    for idx in 0..25 {
        put_processed(&fake, "processed/request/", BASE, &format!("1-2-b-n{idx}"));
    }
    let (status, body, audit) = logs_call(&fake, "POST", "/api/admin/logs/sweep", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        audit,
        Some(("log_retention_sweep_requested".into(), "retention".into()))
    );
    assert!(body["deleted"].as_u64().unwrap_or(0) >= 1, "{body}");
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(
        fake.kv.borrow().contains_key("retention:last_sweep"),
        "sweep must reserve budget to persist retention:last_sweep"
    );
    assert!(
        fastly_log_analytics::shell::Runtime::admin_remaining_calls(&fake) >= 6,
        "sweep must reserve >= 6 calls for ProjectedStore::record_admin_action"
    );
    let mut completed = body["more_pending"] != true;
    for _ in 0..8 {
        if completed {
            break;
        }
        let (s, next_body, _) = logs_call(&fake, "POST", "/api/admin/logs/sweep", Value::Null);
        assert_eq!(s, 200, "{next_body}");
        assert!(
            fastly_log_analytics::shell::Runtime::admin_remaining_calls(&fake) >= 6,
            "resumed sweep must also reserve >= 6 calls for ProjectedStore::record_admin_action"
        );
        completed = next_body["more_pending"] != true;
    }
    assert!(
        completed,
        "manual sweep must resume and finish across calls"
    );
    let (status, body, audit) = logs_call(
        &fake,
        "POST",
        "/api/admin/usage/reconcile-storage",
        Value::Null,
    );
    assert_eq!((status, body["started"].clone()), (200, json!(true)));
    assert_eq!(
        audit,
        Some(("storage_reconcile_requested".into(), "storage".into()))
    );
    let (_, again, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/usage/reconcile-storage",
        Value::Null,
    );
    assert_eq!(again["started"], false);
    for _ in 0..6 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    let runs = rows(&fake, LogKind::CronRuns);
    let reconcile: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "storage_reconcile" && r["outcome"] == "done")
        .collect();
    assert_eq!(reconcile.len(), 1);
    let details: Value =
        serde_json::from_str(reconcile[0]["details_json"].as_str().unwrap()).unwrap();
    assert_eq!(details["datasets"]["pq_request"]["objects"], 0, "{details}");
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
}

#[test]
fn ticks_delete_converted_raw_objects_and_sweep_every_six_hours() {
    let fake = Fake::new(BASE + 40 * DAY);
    tick_runtime(&fake);
    let done: serde_json::Map<String, Value> = LogKind::ALL
        .iter()
        .map(|k| (k.as_str().to_owned(), json!((BASE + 40 * DAY) / DAY)))
        .collect();
    fake.kv
        .borrow_mut()
        .insert("logs:compact_state".into(), (1, json!({"done": done})));
    let raws: Vec<String> = (0..2)
        .map(|m| {
            put_raw(
                &fake,
                BASE + 40 * DAY - 3600 + m * 60,
                "a",
                &[line(BASE + 40 * DAY - 3600 + m * 60, "a")],
            )
        })
        .collect();
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let first = freshness::finish_tick(&fake);
    for _ in 0..16 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
    }
    assert!(raws.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    assert_eq!(
        first["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|run| run["deleted_raw_files"].as_u64().unwrap())
            .sum::<u64>(),
        2
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let del: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "raw_delete")
        .collect();
    assert_eq!(del.len(), 1);
    assert_eq!(del[0]["deleted_files"], 2);
    let sweeps: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "retention_sweep")
        .collect();
    assert!(
        !sweeps.is_empty(),
        "a sweep spanning ticks reports each leg"
    );
    assert!(
        sweeps
            .iter()
            .map(|r| r["deleted_files"].as_i64().unwrap())
            .sum::<i64>()
            >= 1
    );
    assert!(!fake.bucket.borrow().contains_key(&old));
    let marker = || fake.kv.borrow()["retention:last_sweep"].1.clone();
    assert_eq!(marker()["complete"], true);
    let swept_at = marker()["at"].as_u64().unwrap();
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(
        marker()["at"].as_u64().unwrap(),
        swept_at,
        "not due again for six hours"
    );
    fake.now.set(fake.now.get() + 6 * 3600);
    for _ in 0..4 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(marker()["at"].as_u64().unwrap() >= swept_at + 6 * 3600);
}

mod compaction {
    use super::*;
    use fastly_log_analytics::telemetry::compact::{
        classify, compact_key, log_compact, CompactReport,
    };

    const KIND: &str = "api_telemetry";

    fn day_start() -> i64 {
        BASE - 8 * 3600 - DAY
    }

    fn day_dir(day_start: i64) -> String {
        ingest::hour_dir(day_start)
            .rsplit_once("hour=")
            .unwrap()
            .0
            .to_owned()
    }

    fn ops_prefix(kind: &str) -> String {
        format!("processed/ops/{kind}/")
    }

    fn row(ts_ms: i64, id: &str) -> Value {
        json!({"timestamp": ts_ms, "event_id": id, "service_id": SID, "severity": "info",
               "method": "GET", "status_code": 200})
    }

    fn start_min(index: i64) -> i64 {
        day_start() / 60 + index * 5
    }

    fn window_key(prefix: &str, index: i64) -> String {
        let start = start_min(index);
        let hour_ts = (start * 60).div_euclid(3600) * 3600;
        format!(
            "{prefix}{}window={start}-{}-t{index:03}-n3.parquet",
            ingest::hour_dir(hour_ts),
            start + 1
        )
    }

    fn put_window(fake: &Fake, kind: &str, index: i64) -> String {
        let key = window_key(&ops_prefix(kind), index);
        let ts = start_min(index) * 60_000;
        let rows = [
            row(ts, &format!("w{index}-a")),
            row(ts + 1000, &format!("w{index}-b")),
            row(ts + 2000, "dup"),
        ];
        let parsed = LogKind::parse(kind).unwrap();
        let bytes = telemetry::schema::to_parquet(parsed, &rows).unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    fn seed_windows(fake: &Fake, count: i64) -> Vec<String> {
        (0..count).map(|i| put_window(fake, KIND, i)).collect()
    }

    fn seed_state(fake: &Fake) {
        let today = day_start().div_euclid(DAY);
        let map: serde_json::Map<String, Value> = LogKind::ALL
            .iter()
            .map(|k| {
                let done = if k.as_str() == KIND { today - 1 } else { today };
                (k.as_str().to_owned(), json!(done))
            })
            .collect();
        let mut checked = serde_json::Map::new();
        for kind in LogKind::ALL.iter().filter(|k| k.as_str() != KIND) {
            for day in today - 2..=today {
                let t = time::OffsetDateTime::from_unix_timestamp(day * DAY).unwrap();
                checked.insert(
                    format!(
                        "{}:{:04}-{:02}-{:02}",
                        kind.as_str(),
                        t.year(),
                        t.month() as u8,
                        t.day()
                    ),
                    json!(BASE),
                );
            }
        }
        fake.kv.borrow_mut().insert(
            "logs:compact_state".into(),
            (1, json!({"done": map, "checked": checked})),
        );
    }

    fn compact(fake: &Fake, reserve: u32) -> CompactReport {
        compact_with(fake, &Default::default(), reserve)
    }

    fn compact_with(
        fake: &Fake,
        settings: &telemetry::OperationalLogSettings,
        reserve: u32,
    ) -> CompactReport {
        fake.new_request();
        log_compact(&store(fake, &service()), "ck", settings, reserve).unwrap()
    }

    fn compacts(fake: &Fake, kind: &str) -> Vec<String> {
        fake.keys(&ops_prefix(kind))
            .into_iter()
            .filter(|k| k.contains("/compact="))
            .collect()
    }

    fn queue_key() -> String {
        let t = time::OffsetDateTime::from_unix_timestamp(day_start()).unwrap();
        format!(
            "logs:compact_delete:{KIND}:{:04}-{:02}-{:02}",
            t.year(),
            t.month() as u8,
            t.day()
        )
    }

    fn decode(fake: &Fake, key: &str) -> Vec<Value> {
        let bytes = fake.bucket.borrow()[key].clone();
        telemetry::schema::from_parquet(LogKind::ApiTelemetry, bytes).unwrap()
    }

    fn settle(fake: &Fake) {
        for _ in 0..30 {
            let report = compact(fake, 3);
            if !report.more_pending && report.chunks == 0 {
                return;
            }
        }
        panic!("compaction did not settle");
    }

    #[test]
    fn closed_day_is_merged_in_chunks_of_at_most_24_and_deduplicated() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 50);
        settle(&fake);
        let dir = format!("{}{}", ops_prefix(KIND), day_dir(day_start()));
        let found = compacts(&fake, KIND);
        let shape = |i: usize, a: i64, b: i64, tail: &str| {
            let name = found[i].strip_prefix(&dir).unwrap();
            assert!(
                name.starts_with(&format!("compact={a}-{b}-{tail}-")),
                "{name}"
            );
        };
        assert_eq!(found.len(), 3);
        shape(0, start_min(0), start_min(23), "c24-n49");
        shape(1, start_min(24), start_min(47), "c24-n49");
        shape(2, start_min(48), start_min(49), "c2-n5");
        let expected = found.clone();
        let rows = decode(&fake, &expected[0]);
        let ids: BTreeSet<_> = rows
            .iter()
            .map(|r| r["event_id"].as_str().unwrap())
            .collect();
        assert_eq!((rows.len(), ids.len()), (49, 49));
        let remaining: Vec<_> = fake
            .keys(&ops_prefix(KIND))
            .into_iter()
            .filter(|k| k.contains("/window="))
            .collect();
        assert!(
            remaining.is_empty(),
            "sources are deleted after the commits"
        );
    }

    fn dir() -> &'static str {
        "processed/ops/api_telemetry/year=2027/month=01/day=14/"
    }

    fn w(hour: u32, start: u64) -> String {
        format!(
            "{}hour={hour:02}/window={start}-{}-x-n1.parquet",
            dir(),
            start + 1
        )
    }

    #[test]
    fn coverage_is_the_recorded_source_set_and_a_late_window_in_range_is_still_read() {
        let sources = [w(0, 100), w(0, 150), w(1, 200)];
        let c = compact_key(dir(), 100, 200, &sources, 9);
        let late = w(0, 120);
        let mut keys = sources.to_vec();
        keys.extend([late.clone(), w(1, 201), w(2, 99), c.clone()]);
        let listing = classify(&keys);
        assert_eq!(listing.valid, vec![c]);
        assert!(listing.ignored.is_empty());
        assert_eq!(listing.covered, sources.to_vec());
        assert_eq!(
            listing.read,
            vec![late, w(1, 201), w(2, 99)],
            "only recorded sources are skipped"
        );
    }

    #[test]
    fn partial_source_deletes_never_hide_rows_or_invalidate_the_compact() {
        let sources = [w(0, 100), w(0, 150), w(1, 200)];
        let c = compact_key(dir(), 100, 200, &sources, 9);
        let partial = classify(&[w(0, 150), c.clone()]);
        assert_eq!(partial.valid, vec![c.clone()]);
        assert_eq!(partial.covered, vec![w(0, 150)]);
        assert!(partial.read.is_empty() && partial.ignored.is_empty());
        let after_delete = classify(std::slice::from_ref(&c));
        assert_eq!(after_delete.valid, vec![c]);
    }

    #[test]
    fn a_legacy_compact_name_without_a_source_set_keeps_the_range_rule() {
        let c = format!("{}compact=100-200-c3-n9.parquet", dir());
        let partial = classify(&[w(0, 100), w(0, 150), c.clone()]);
        assert_eq!(partial.ignored, vec![c.clone()]);
        assert_eq!(partial.read, vec![w(0, 100), w(0, 150)]);
        let full = classify(&[w(0, 100), w(0, 150), w(1, 200), c.clone()]);
        assert!(full.read.is_empty() && full.valid == vec![c]);
    }

    #[test]
    fn one_chunk_is_written_when_only_one_fits_and_the_next_tick_continues() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 48);
        let first = compact(&fake, 3);
        assert_eq!((first.chunks, first.sources), (1, 24));
        assert!(first.more_pending);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert_eq!(
            listing.read,
            sources[24..].to_vec(),
            "uncovered windows stay readable"
        );
        let second = compact(&fake, 3);
        assert_eq!(second.chunks, 1);
        assert_eq!(compacts(&fake, KIND).len(), 2);
    }

    #[test]
    fn a_chunk_is_sized_to_the_calls_that_fit_and_never_exceeds_them() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 24);
        // state read, list and queue read leave 29; reserve 21 leaves room for 6 GETs, a PUT and the insert.
        let small = compact(&fake, 21);
        assert_eq!((small.chunks, small.sources), (1, 6));
        assert!(small.more_pending);
        let none = compact(&fake, 30);
        assert_eq!(none.chunks, 0);
        assert!(none.more_pending);
        assert!(fake.calls.get() <= BUDGET);
    }

    #[test]
    fn sources_are_queued_after_the_commit_and_deleted_on_a_later_tick() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 24);
        compact(&fake, 3);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        let queued = fake.kv.borrow()[&queue_key()].1["keys"].clone();
        assert_eq!(queued.as_array().unwrap().len(), 24);
        assert!(fake.delete_posts.borrow().is_empty());
        let later = compact(&fake, 3);
        assert_eq!(later.deleted, 24);
        assert_eq!(*fake.delete_posts.borrow(), vec![24]);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert_eq!(compacts(&fake, KIND).len(), 1);
    }

    #[test]
    fn a_failed_commit_queues_and_deletes_nothing() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 5);
        fake.fail_put_number.set(Some(1));
        let report = compact(&fake, 3);
        assert_eq!(report.chunks, 0);
        assert!(!report.errors.is_empty());
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert!(compacts(&fake, KIND).is_empty());
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        assert_eq!(compact(&fake, 3).chunks, 1, "the next tick retries");
    }

    #[test]
    fn request_and_rum_files_are_never_touched() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 3);
        let mut guarded = Vec::new();
        for root in ["processed/request/", "processed/rum/"] {
            for index in 0..3 {
                let key = window_key(root, index);
                fake.bucket.borrow_mut().insert(key.clone(), vec![1; 10]);
                guarded.push(key);
            }
        }
        settle(&fake);
        let bucket = fake.bucket.borrow();
        assert!(guarded.iter().all(|k| bucket.get(k) == Some(&vec![1; 10])));
        assert!(!bucket
            .keys()
            .any(|k| !k.starts_with("processed/ops/") && k.contains("compact=")));
    }

    #[test]
    fn rerunning_produces_the_same_keys_and_bytes_and_repairs_a_lost_queue() {
        let run = || {
            let fake = Fake::new(BASE);
            seed_state(&fake);
            seed_windows(&fake, 24);
            compact(&fake, 3);
            fake
        };
        let (a, b) = (run(), run());
        assert_eq!(compacts(&a, KIND), compacts(&b, KIND));
        let key = &compacts(&a, KIND)[0];
        assert_eq!(a.bucket.borrow()[key], b.bucket.borrow()[key]);

        // Crash between the PUT and the queue insert: no queue, state not saved.
        a.kv.borrow_mut().remove(&queue_key());
        a.kv.borrow_mut().remove("logs:compact_state");
        seed_state(&a);
        compact(&a, 3);
        assert_eq!(compacts(&a, KIND).len(), 1, "no second compact file");
        compact(&a, 3);
        assert!(!a.bucket.borrow().keys().any(|k| k.contains("/window=")));
    }

    #[test]
    fn a_compact_with_the_wrong_row_count_never_causes_deletes() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 3);
        let bad = format!(
            "{}{}compact={}-{}-c3-n99.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start_min(0),
            start_min(2)
        );
        let one = telemetry::schema::to_parquet(LogKind::ApiTelemetry, &[row(1, "x")]).unwrap();
        fake.bucket.borrow_mut().insert(bad, one);
        compact(&fake, 3);
        compact(&fake, 3);
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn disabled_setting_makes_no_calls() {
        let fake = Fake::new(BASE);
        seed_windows(&fake, 5);
        let settings = telemetry::OperationalLogSettings {
            compaction_enabled: false,
            ..Default::default()
        };
        let report = compact_with(&fake, &settings, 3);
        assert_eq!((fake.calls.get(), report.chunks), (0, 0));
    }

    fn listed_files(fake: &Fake, from: i64, to: i64) -> Vec<Value> {
        let base = format!(
            "from={}&to={}&dataset=ops:{KIND}",
            iso_url(from),
            iso_url(to)
        );
        let (mut cursor, mut files) = (None::<String>, Vec::new());
        for _ in 0..10 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            let page = files_page(fake, &query).unwrap();
            files.extend(page["files"].as_array().unwrap().iter().cloned());
            match page["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return files,
            }
        }
        panic!("files did not complete");
    }

    fn keys_of(files: &[Value]) -> Vec<String> {
        let mut keys: Vec<String> = files
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect();
        keys.sort();
        keys
    }

    #[test]
    fn files_lists_each_compact_once_in_place_of_its_sources_and_keeps_late_windows() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 30);
        assert_eq!(compact(&fake, 3).chunks, 1);
        let first = compacts(&fake, KIND);
        assert!(sources[..24]
            .iter()
            .all(|k| fake.bucket.borrow().contains_key(k)));
        // `from` lies past hour 00, so an hour-prefix start-after would skip the day's compacts.
        let (from, to) = (day_start() + 5400, day_start() + 3 * 3600);
        let files = listed_files(&fake, from, to);
        let mut expected = first.clone();
        expected.extend(sources[24..].iter().cloned());
        expected.sort();
        assert_eq!(keys_of(&files), expected, "pending sources stay hidden");
        let entry = files.iter().find(|f| f["key"] == first[0]).unwrap();
        assert_eq!(entry["start"], ingest::rfc3339(start_min(0) * 60));
        assert_eq!(entry["end"], ingest::rfc3339(day_start() + 2 * 3600));
        assert_eq!(
            entry["span"],
            json!([
                ingest::rfc3339(start_min(0) * 60),
                ingest::rfc3339(start_min(23) * 60)
            ])
        );
        assert_eq!(
            (entry["rows"].clone(), entry["sources"].clone()),
            (json!(49), json!(24))
        );

        settle(&fake);
        let late = window_key(&ops_prefix(KIND), 25).replace("-t025-", "-late-");
        fake.bucket.borrow_mut().insert(late.clone(), vec![1]);
        let all = compacts(&fake, KIND);
        assert_eq!(all.len(), 2);
        let mut expected = all.clone();
        expected.push(late);
        expected.sort();
        assert_eq!(keys_of(&listed_files(&fake, from, to)), expected);
    }

    #[test]
    fn files_hides_sources_listed_on_a_later_page_than_their_compact() {
        let fake = Fake::new(BASE);
        let prefix = format!("{}{}", ops_prefix(KIND), day_dir(day_start()));
        let first = day_start() / 60;
        let filler: Vec<String> = (0..api::FILES_PAGE as i64 + 20)
            .map(|i| format!("{prefix}hour=00/window={first}-{first}-f{i:04}-n1.parquet"))
            .collect();
        let late = day_start() / 60 + 23 * 60;
        let sources: Vec<String> = (0..3)
            .map(|i| {
                format!(
                    "{prefix}hour=23/window={}-{}-s{i}-n1.parquet",
                    late + i,
                    late + i
                )
            })
            .collect();
        let compact = compact_key(&prefix, late as u64, late as u64 + 2, &sources, 3);
        for key in filler.iter().chain(&sources).chain([&compact]) {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let mut expected = filler.clone();
        expected.push(compact);
        expected.sort();
        assert_eq!(
            keys_of(&listed_files(&fake, day_start(), day_start() + DAY)),
            expected
        );
    }

    fn ingest_row(id: &str, raw: &str, ts_s: i64) -> Value {
        json!({"timestamp": ts_s * 1000, "event_id": id, "service_id": SID,
               "severity": "info", "run_id": "r", "dataset": "request", "raw_key": raw,
               "raw_size": 9, "status": "converted", "state_version": 1, "rows": 2,
               "raw_deleted": false})
    }

    fn put_ingest(fake: &Fake, key: &str, rows: &[Value]) {
        let bytes = telemetry::schema::to_parquet(LogKind::IngestFiles, rows).unwrap();
        fake.bucket.borrow_mut().insert(key.to_owned(), bytes);
    }

    #[test]
    fn ingested_files_reads_compacts_instead_of_their_sources_and_keeps_late_windows() {
        let fake = Fake::new(BASE);
        let prefix = format!("processed/ops/ingest_files/{}", day_dir(day_start()));
        let minute = day_start() / 60;
        let window = |hour: i64, tag: &str| {
            let m = minute + hour * 60;
            format!("{prefix}hour={hour:02}/window={m}-{m}-{tag}-n1.parquet")
        };
        let raws: Vec<String> = (0..3)
            .map(|i| raw_key(day_start() + i * 3600, &format!("r{i}")))
            .collect();
        let rows: Vec<Value> = (0..3)
            .map(|i| ingest_row(&format!("e{i}"), &raws[i as usize], day_start() + i * 3600))
            .collect();
        let sources = [window(0, "a"), window(1, "b")];
        for (key, row) in sources.iter().zip(&rows) {
            put_ingest(&fake, key, std::slice::from_ref(row));
        }
        let compact = compact_key(&prefix, minute as u64, minute as u64 + 60, &sources, 2);
        put_ingest(&fake, &compact, &rows[..2]);
        let late = window(2, "c");
        put_ingest(&fake, &late, &rows[2..]);

        let page = admin_page(&fake, None, "").unwrap();
        let mut keys: Vec<&str> = page["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        keys.sort();
        let mut want: Vec<&str> = raws.iter().map(String::as_str).collect();
        want.sort();
        assert_eq!(keys, want, "every raw file once");
        assert_eq!(page["skipped_objects"], 0);
        let read: Vec<String> = fake
            .cdn_gets
            .borrow()
            .iter()
            .filter(|k| k.starts_with("processed/ops/ingest_files/"))
            .cloned()
            .collect();
        assert_eq!(read, vec![compact.clone(), late.clone()]);

        for key in &sources {
            fake.bucket.borrow_mut().remove(key);
        }
        let page = admin_page(&fake, None, "").unwrap();
        assert_eq!(page["files"].as_array().unwrap().len(), 3);
        assert_eq!(page["skipped_objects"], 0);
    }

    #[test]
    fn a_day_is_closed_two_hours_after_midnight_and_a_fresh_install_scans_back() {
        let fake = Fake::new(BASE - 7 * 3600);
        seed_windows(&fake, 5);
        for _ in 0..12 {
            compact(&fake, 3);
        }
        assert!(
            compacts(&fake, KIND).is_empty(),
            "01:00 UTC is inside the grace period"
        );
        fake.now.set(BASE as u64);
        for _ in 0..12 {
            compact(&fake, 3);
        }
        assert_eq!(
            compacts(&fake, KIND).len(),
            1,
            "no saved state: scans the lookback window"
        );
    }

    #[test]
    fn retention_removes_a_compact_file_with_its_expired_day() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 24);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        fake.now.set((BASE + 40 * DAY) as u64);
        let mut settings = telemetry::OperationalLogSettings::default();
        settings.retention_days_by_kind.insert(KIND.into(), 14);
        sweep(&fake, &service(), &settings);
        assert!(compacts(&fake, KIND).is_empty());
    }

    fn put_late(fake: &Fake, start: i64, tag: &str) -> String {
        let key = format!(
            "{}{}hour=00/window={start}-{}-late{tag}-n1.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start + 1
        );
        let bytes = telemetry::schema::to_parquet(
            LogKind::ApiTelemetry,
            &[row(start * 60_000, &format!("late-{tag}"))],
        )
        .unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    #[test]
    fn late_windows_inside_or_after_a_compact_are_compacted_on_a_later_revisit() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 6);
        settle(&fake);
        let first = compacts(&fake, KIND);
        assert_eq!(first.len(), 1);
        let inside = put_late(&fake, start_min(2), "a");
        let after = put_late(&fake, start_min(5) + 2, "b");
        settle(&fake);
        assert_eq!(compacts(&fake, KIND), first, "not revisited immediately");
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert_eq!(listing.valid, first);
        assert_eq!(listing.read, vec![inside.clone(), after.clone()]);
        fake.now.set(fake.now.get() + 7 * 3600);
        settle(&fake);
        let all = compacts(&fake, KIND);
        assert_eq!(all.len(), 2);
        assert!(all.contains(&first[0]));
        let late: Vec<_> = all.iter().filter(|k| **k != first[0]).collect();
        let ids: BTreeSet<String> = decode(&fake, late[0])
            .iter()
            .map(|r| r["event_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            ids,
            BTreeSet::from(["late-a".to_owned(), "late-b".to_owned()])
        );
        assert!(!fake.bucket.borrow().contains_key(&inside));
        assert!(!fake.bucket.borrow().contains_key(&after));
    }

    #[test]
    fn the_delete_queue_only_removes_this_services_ops_window_and_compact_keys() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let good = put_window(&fake, KIND, 0);
        let other_kind = format!(
            "processed/ops/cron_runs/{}",
            &good[good.find("year=").unwrap()..]
        );
        let foreign = [
            "raw/request/x.gz".to_owned(),
            "processed/request/year=2027/month=01/day=14/hour=00/window=1-2-a-n1.parquet".to_owned(),
            "other-scope/processed/ops/api_telemetry/year=2027/month=01/day=14/hour=00/window=1-2-a-n1.parquet".to_owned(),
            format!("{}../x.parquet", ops_prefix(KIND)),
            format!("{}notes.txt", ops_prefix(KIND)),
            other_kind,
        ];
        for key in &foreign {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let mut keys = vec![good.clone()];
        keys.extend(foreign.iter().cloned());
        fake.kv.borrow_mut().insert(
            queue_key(),
            (
                1,
                json!({"keys": keys, "sizes": vec![1; keys.len()], "at": 1}),
            ),
        );
        let mut state = json!({"done": {}, "pending": [format!("{KIND}:{}", queue_key().rsplit(':').next().unwrap())]});
        state["done"] = fake.kv.borrow()["logs:compact_state"].1["done"].clone();
        fake.kv
            .borrow_mut()
            .insert("logs:compact_state".into(), (2, state));
        let report = compact(&fake, 3);
        assert_eq!(report.deleted, 1);
        assert!(!fake.bucket.borrow().contains_key(&good));
        for key in &foreign {
            assert!(fake.bucket.borrow().contains_key(key), "{key} was deleted");
        }
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
    }

    fn put_declared(fake: &Fake, index: i64, declared_rows: u64) -> String {
        let start = start_min(index);
        let key = format!(
            "{}{}hour=00/window={start}-{}-big{index:03}-n{declared_rows}.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start + 1
        );
        let bytes = telemetry::schema::to_parquet(
            LogKind::ApiTelemetry,
            &[row(start * 60_000, &format!("big-{index}"))],
        )
        .unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    #[test]
    fn a_large_day_is_split_by_the_row_cap_across_ticks_and_loses_nothing() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources: Vec<String> = (0..6).map(|i| put_declared(&fake, i, 4000)).collect();
        let first = compact(&fake, 22);
        assert_eq!((first.chunks, first.sources), (1, 2));
        assert!(first.more_pending);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 3);
        let ids: BTreeSet<String> = compacts(&fake, KIND)
            .iter()
            .flat_map(|k| decode(&fake, k))
            .map(|r| r["event_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids.len(), 6);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn a_window_over_the_row_cap_stays_loose_and_readable() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let huge = put_declared(&fake, 0, 20_000);
        let rest: Vec<String> = (1..4).map(|i| put_declared(&fake, i, 10)).collect();
        settle(&fake);
        assert!(fake.bucket.borrow().contains_key(&huge));
        assert!(rest.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
        assert_eq!(classify(&fake.keys(&ops_prefix(KIND))).read, vec![huge]);
    }

    #[test]
    fn compaction_stops_cleanly_at_the_wall_time_limit_and_resumes_next_tick() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 6);
        fake.elapsed_s.set(41);
        let stopped = compact(&fake, 3);
        assert_eq!((stopped.chunks, stopped.stopped), (0, Some("wall_time")));
        assert!(stopped.more_pending);
        assert_eq!(fake.puts.get(), 0);
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        fake.elapsed_s.set(0);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 1);
    }

    #[test]
    fn a_tick_with_a_backlog_stays_inside_the_lookup_and_call_limits() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        for _ in 0..6 {
            fake.now.set(fake.now.get() + 120);
            fake.kv_reads.borrow_mut().clear();
            run_tick(&fake);
            assert!(fake.kv_reads.borrow().len() <= 32);
            assert!(fake.calls.get() <= BUDGET);
        }
    }

    fn seed_idle_backlog(fake: &Fake) {
        tick_runtime(fake);
        seed_state(fake);
        seed_windows(fake, 4);
    }

    fn cron_kinds(fake: &Fake) -> Vec<String> {
        rows(fake, LogKind::CronRuns)
            .iter()
            .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn an_idle_tick_runs_log_compact() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        assert!(cron_kinds(&fake).contains(&"log_compact".to_owned()));
        assert!(!compacts(&fake, KIND).is_empty());
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert!(listing.read.len() <= 1, "{:?}", listing.read);
    }

    #[test]
    fn the_retention_sweep_still_runs_on_ticks_with_a_compaction_backlog() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        let old = put_processed(&fake, "processed/request/", BASE - 40 * DAY, "1-2-a-n1");
        for _ in 0..8 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        let kinds = cron_kinds(&fake);
        assert!(kinds.contains(&"retention_sweep".to_owned()), "{kinds:?}");
        assert!(!fake.bucket.borrow().contains_key(&old));
        assert!(kinds.contains(&"log_compact".to_owned()), "{kinds:?}");
    }
}

#[test]
fn retention_loops_stop_at_the_wall_and_resume() {
    let fake = Fake::new(BASE + 40 * DAY);
    let queued = converted_fixture(&fake, 2);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let svc = service();
    fake.elapsed_s.set(41);
    let deleted = delete_tick(&fake, &svc);
    assert_eq!((deleted.deleted, deleted.more_pending), (0, true));
    assert!(queued.iter().all(|k| fake.bucket.borrow().contains_key(k)));
    sweep(&fake, &svc, &Default::default());
    assert!(fake.bucket.borrow().contains_key(&old));
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    fake.new_request();
    let step = ingest::retention::storage_reconcile_step(&store(&fake, &svc), 0)
        .unwrap()
        .unwrap();
    assert!(step.snapshot.is_none());
    assert!(fake.kv.borrow().contains_key("storage:reconcile"));
    fake.elapsed_s.set(0);
    assert_eq!(delete_tick(&fake, &svc).deleted, 2);
    sweep(&fake, &svc, &Default::default());
    assert!(
        fake.bucket.borrow().contains_key(&old),
        "catalog readers retain their grace"
    );
    fake.now
        .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S);
    sweep(&fake, &svc, &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&old));
}

#[test]
fn worker_delete_stops_at_the_wall_and_reports_the_rest_failed() {
    let fake = Fake::new(BASE);
    let svc = service();
    let keys: Vec<String> = (0..3)
        .map(|i| put_processed(&fake, "processed/request/", BASE + i * 3600, "1-2-a-n1"))
        .collect();
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: "/_internal/delete".into(),
        headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
        body: json!({
            "service_id": svc.service_id, "region": svc.fos_region,
            "bucket": svc.fos_bucket, "prefix": ingest::scope(&svc), "keys": keys,
        })
        .to_string()
        .into_bytes(),
    };
    fake.elapsed_s.set(41);
    let response = ingest::retention::handle_worker_delete(&fake, &request);
    let reply: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(reply["deleted"].as_array().unwrap().len(), 0);
    assert_eq!(reply["failed"].as_array().unwrap().len(), 3);
    assert!(keys.iter().all(|k| fake.bucket.borrow().contains_key(k)));
}
