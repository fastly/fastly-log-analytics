use super::{freshness, support::*};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{Record, Service},
    ingest::{self},
    shell::Runtime,
    telemetry::{self, LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

#[test]
fn service_schema_adds_committed_and_enabled_custom_fields_in_name_order() {
    use ingest::convert::{CustomColumn, Kind, RequestSchema};
    let base = RequestSchema::for_fields(&service().log_fields);
    assert!(base.custom().is_empty());
    assert_eq!(base.hash(), ingest::convert::schema_hash());
    let schema = RequestSchema::for_fields(&custom_service().log_fields);
    let names: Vec<&str> = schema.custom().iter().map(|c| c.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert!(names.contains(&"tenant") && names.contains(&"score") && names.contains(&"old_flag"));
    assert!(
        !names.contains(&"draft"),
        "a never-committed disabled field has no column"
    );
    assert_eq!(names.iter().filter(|n| n.starts_with("cmcd_")).count(), 14);
    let kind = |name: &str| {
        schema
            .custom()
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .kind
    };
    assert_eq!(kind("score"), Kind::Int64);
    assert_eq!(kind("old_flag"), Kind::Bool);
    assert_eq!(kind("cmcd_br"), Kind::Int64);
    assert_eq!(kind("cmcd_sid"), Kind::Utf8);
    assert_ne!(schema.hash(), base.hash());
    let retyped = RequestSchema::new(
        schema
            .custom()
            .iter()
            .map(|c| CustomColumn {
                name: c.name.clone(),
                kind: if c.name == "score" {
                    Kind::Utf8
                } else {
                    c.kind
                },
                slot_only: c.slot_only,
            })
            .collect(),
    );
    assert_ne!(
        retyped.hash(),
        schema.hash(),
        "a type change is a new schema"
    );
    assert_eq!(
        RequestSchema::new(schema.custom().iter().rev().cloned().collect()).hash(),
        schema.hash(),
        "field order in the config does not matter"
    );
}

#[test]
fn custom_and_cmcd_fields_are_written_as_typed_columns_and_committed() {
    let fake = Fake::new(BASE + 3600);
    let svc = custom_service();
    store_service(&fake, &svc);
    let row = json!({"timestamp": iso(BASE), "ip": "a", "status": 200, "tenant": "acme",
        "score": 42, "cmcd_br": 3000, "cmcd_sid": "s-1", "cmcd_bs": "1", "draft": "x"});
    put_raw(&fake, BASE, "a", &[row.to_string()]);
    assert_eq!(tick_service(&fake, &svc).unwrap().converted, 1);
    let schema = ingest::convert::RequestSchema::for_fields(&svc.log_fields);
    let keys = fake.keys("processed/request/year=");
    assert_eq!(keys.len(), 1);
    assert_eq!(fake.keys("processed/request/cmcd_summary/").len(), 1);
    let batch = &batches(&fake.bucket.borrow()[&keys[0]])[0];
    let text = |name: &str| {
        let col = batch.column_by_name(name).unwrap();
        col.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    };
    let int = |name: &str| {
        let col = batch.column_by_name(name).unwrap();
        col.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    };
    assert_eq!(text("tenant"), "acme");
    assert_eq!(int("score"), 42);
    assert_eq!(int("cmcd_br"), 3000);
    assert_eq!(int("cmcd_bs"), 1);
    assert_eq!(text("cmcd_sid"), "s-1");
    assert!(batch.column_by_name("old_flag").unwrap().is_null(0));
    assert!(batch.column_by_name("draft").is_none());
    assert_eq!(plan(&fake).unwrap()["schema"], schema.hash());
    let stored = &fake.kv.borrow()[&format!("svc:{SID}")].1;
    let score = stored["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "score")
        .unwrap()
        .clone();
    assert_eq!(score["committed"], true, "a planned field is locked");
    let draft = stored["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "draft")
        .unwrap()
        .clone();
    assert_ne!(draft["committed"], true);
    assert!(stored["_audit_outbox"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["event"] == "custom_fields.committed"));
}

#[test]
fn a_custom_field_type_change_abandons_the_open_plan() {
    let fake = Fake::new(BASE + 7200);
    let svc = custom_service();
    store_service(&fake, &svc);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick_service(&fake, &svc).unwrap();
    let mut changed = custom_service();
    for field in changed.log_fields["custom_fields"].as_array_mut().unwrap() {
        if field["name"] == "draft" {
            field["enabled"] = true.into();
        }
    }
    store_service(&fake, &changed);
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
    tick_service(&fake, &changed).unwrap();
    assert_eq!(plan(&fake).unwrap()["abandoning"], "schema_changed");
}

#[test]
fn worker_rejects_custom_columns_that_do_not_match_the_schema_hash() {
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let schema = ingest::convert::RequestSchema::for_fields(&custom_service().log_fields);
    let mut body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "custom": schema.custom(), "window_s": 60, "keys": [key], "stop_ms": 20_000,
    });
    let call = |body: &Value| -> Value {
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
        serde_json::from_slice(&response.body).unwrap()
    };
    assert_eq!(call(&body)["status"], "schema_mismatch");
    body["schema"] = schema.hash().into();
    let reply = call(&body);
    assert_eq!(reply["status"], "ok", "{reply}");
    let output = reply["result"]["outputs"][0]["key"].as_str().unwrap();
    let batch = &batches(&fake.bucket.borrow()[output])[0];
    assert!(batch.column_by_name("cmcd_sid").is_some());
}

#[test]
fn rum_beacons_split_into_vitals_and_errors_like_v3() {
    let faro = json!({
        "meta": {"browser": {"name": "Firefox", "mobile": true}, "os": {"name": "Android"},
                 "page": {"url": "https://ex.com/p/x?q=1"}},
        "measurements": [{"type": "web-vitals", "values": {"cls": 0.1, "delta": 0.1},
                          "context": {"rating": "good"}}],
        "exceptions": [{"value": "TypeError", "stacktrace": {"frames": [
            {"filename": "app.js", "lineno": 10, "colno": 5}]}}],
    });
    let mut faro_line: Value = serde_json::from_str(&rum_beacon(BASE + 7, "", "")).unwrap();
    faro_line["url"] = "/rum".into();
    faro_line["rum_body"] = faro.to_string().into();
    let mut error_line: Value = serde_json::from_str(&rum_beacon(BASE + 3, "", "")).unwrap();
    error_line["rum_error_message"] = "boom".into();
    error_line["url"] = "/rum".into();
    let mut options_line: Value = serde_json::from_str(&rum_beacon(BASE, "LCP", "1")).unwrap();
    options_line["method"] = "OPTIONS".into();
    let mut other_line: Value = serde_json::from_str(&rum_beacon(BASE, "LCP", "1")).unwrap();
    other_line["service_id"] = "OTHER".into();
    let data = gz(&[
        rum_beacon(BASE + 5, "LCP", "1234.5"),
        error_line.to_string(),
        faro_line.to_string(),
        options_line.to_string(),
        other_line.to_string(),
        json!({"method": "GET", "url": "/rum?rum_metric_name=LCP"}).to_string(),
        "{not json".into(),
    ]);
    let rows = ingest::rum::convert_object(&data, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!(
        (rows.lines_read, rows.bad_lines, rows.rows),
        (7, 2, 4),
        "OPTIONS and other-service beacons are skipped, not bad"
    );
    assert!(rows.bad_sample.is_some());
    let vitals = &rows.windows[&BASE];
    assert_eq!(vitals.schema(), ingest::rum::vitals_schema());
    assert_eq!(
        ms_column(vitals, "timestamp"),
        [Some((BASE + 5) * 1000 + 250), Some((BASE + 7) * 1000 + 250)]
    );
    assert_eq!(
        strings(vitals, "metric_name"),
        [Some("LCP".into()), Some("cls".into())]
    );
    assert_eq!(floats(vitals, "metric_value"), [Some(1234.5), Some(0.1)]);
    assert_eq!(
        strings(vitals, "metric_rating"),
        [Some("good".into()), Some("good".into())]
    );
    assert_eq!(
        strings(vitals, "pathname"),
        [Some("/home".into()), Some("/p/x".into())]
    );
    assert_eq!(
        strings(vitals, "browser"),
        [Some("Unknown".into()), Some("Firefox".into())]
    );
    assert_eq!(
        strings(vitals, "os"),
        [Some("Unknown".into()), Some("Android".into())]
    );
    assert_eq!(
        strings(vitals, "device"),
        [Some("Unknown".into()), Some("Mobile".into())]
    );
    assert_eq!(strings(vitals, "cid")[0].as_deref(), Some("c1"));
    assert_eq!(strings(vitals, "req_id")[0].as_deref(), Some("r1"));
    assert_eq!(strings(vitals, "city")[0].as_deref(), Some("Paris"));
    assert_eq!(floats(vitals, "ttfb")[0], Some(0.012));
    let errors = &rows.errors[&BASE];
    assert_eq!(errors.schema(), ingest::rum::errors_schema());
    assert_eq!(
        strings(errors, "error_message"),
        [Some("boom".into()), Some("TypeError".into())]
    );
    assert_eq!(
        strings(errors, "error_file"),
        [Some("unknown.js".into()), Some("app.js".into())]
    );
    assert_eq!(ints32(errors, "error_line"), [0, 10]);
    assert_eq!(ints32(errors, "error_col"), [0, 5]);
    assert_eq!(
        strings(errors, "pathname"),
        [Some("/r".into()), Some("/p/x".into())],
        "a beacon without rum_pathname falls back to its referer's path"
    );
}

#[test]
fn compact_beacon_extracts_browser_os_device_and_defaults_missing_to_unknown() {
    let compact = gz(&[
        rum_batch(
            BASE + 8,
            "cid=c1&p=%2Fshop&m=LCP:1500:good&b=Safari&o=iOS&d=Mobile",
        ),
        rum_batch(BASE + 9, "cid=c2&p=%2Fshop&m=LCP:1600:good"),
    ]);
    let compact_rows = ingest::rum::convert_object(&compact, 60, BASE, 1 << 20, SID).unwrap();
    let compact_vitals = &compact_rows.windows[&BASE];
    assert_eq!(
        strings(compact_vitals, "browser"),
        [Some("Safari".into()), Some("Unknown".into())]
    );
    assert_eq!(
        strings(compact_vitals, "os"),
        [Some("iOS".into()), Some("Unknown".into())]
    );
    assert_eq!(
        strings(compact_vitals, "device"),
        [Some("Mobile".into()), Some("Unknown".into())]
    );
}

#[test]
fn one_batched_beacon_expands_into_a_row_per_metric_and_error() {
    let data = gz(&[
        rum_batch(
            BASE + 5,
            "cid=c%2C1&p=%2Fshop%2Fa%3Ab&m=LCP:1200:good,FCP:400.5:needs-improvement,ttfb:85:,event_a%3Ab:1:\
             &e=Type%7CError%3A%20x|app.js|10|5&e=boom|||",
        ),
        rum_beacon(BASE + 6, "INP", "40"),
    ]);
    let rows = ingest::rum::convert_object(&data, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!((rows.lines_read, rows.bad_lines, rows.rows), (2, 0, 7));
    let vitals = &rows.windows[&BASE];
    assert_eq!(
        strings(vitals, "metric_name"),
        ["LCP", "FCP", "ttfb", "event_a:b", "INP"].map(|s| Some(s.into()))
    );
    assert_eq!(
        floats(vitals, "metric_value"),
        [Some(1200.0), Some(400.5), Some(85.0), Some(1.0), Some(40.0)]
    );
    assert_eq!(
        strings(vitals, "metric_rating"),
        ["good", "needs-improvement", "", "", "good"].map(|s| Some(s.into()))
    );
    assert_eq!(
        strings(vitals, "pathname"),
        ["/shop/a:b", "/shop/a:b", "/shop/a:b", "/shop/a:b", "/home"].map(|s| Some(s.into()))
    );
    assert_eq!(
        strings(vitals, "cid"),
        ["c,1", "c,1", "c,1", "c,1", "c1"].map(|s| Some(s.into()))
    );
    assert_eq!(strings(vitals, "req_id")[0].as_deref(), Some("r1"));
    assert_eq!(strings(vitals, "region")[0].as_deref(), Some("IDF"));
    let errors = &rows.errors[&BASE];
    assert_eq!(
        strings(errors, "error_message"),
        [Some("Type|Error: x".into()), Some("boom".into())]
    );
    assert_eq!(
        strings(errors, "error_file"),
        [Some("app.js".into()), Some("unknown.js".into())]
    );
    assert_eq!(ints32(errors, "error_line"), [10, 0]);
    assert_eq!(ints32(errors, "error_col"), [5, 0]);
    assert_eq!(
        strings(errors, "pathname"),
        [Some("/shop/a:b".into()), Some("/shop/a:b".into())]
    );
}

#[test]
fn vcl_batched_query_string_beacons_split_metrics_and_errors_and_decode_percent() {
    let mut batched_via_raw_query: Value =
        serde_json::from_str(&rum_beacon(BASE + 1, "", "")).unwrap();
    batched_via_raw_query["url"] = "/rum-beacon".into();
    batched_via_raw_query["rum_raw_query"] =
        "/rum-beacon?rum_metric_name=ttfb&rum_metric_value=205.3&rum_metric_rating=good\
         &rum_metric_name=lcp&rum_metric_value=584&rum_metric_rating=good\
         &rum_metric_name=truncated_name&rum_metric_value=notanumber\
         &rum_error_message=Uncaught%20RangeError%3A%20boom&rum_error_file=app.js&rum_error_line=27&rum_error_col=9\
         &cid=c-batched&rum_pathname=%2Frum-demo%2F"
            .into();
    batched_via_raw_query["rum_pathname"] = "%2Frum-demo%2F".into();

    let mut query_only_err: Value = serde_json::from_str(&rum_beacon(BASE + 2, "", "")).unwrap();
    query_only_err["url"] =
        "/rum-beacon?rum_error_message=ReferenceError%3A%20x&rum_pathname=%2Frum-demo%2Fpage2&cid=c-err".into();

    let mut vcl_encoded_err: Value = serde_json::from_str(&rum_beacon(BASE + 3, "", "")).unwrap();
    vcl_encoded_err["url"] = "/rum-beacon".into();
    vcl_encoded_err["rum_error_message"] = "Uncaught%20Error%3A%20y".into();
    vcl_encoded_err["rum_error_file"] = "https%3A%2F%2Fexample.test%2Fapp.js".into();
    vcl_encoded_err["rum_pathname"] = "%2Frum-demo%2Fpage3".into();

    let data = gz(&[
        batched_via_raw_query.to_string(),
        query_only_err.to_string(),
        vcl_encoded_err.to_string(),
    ]);
    let rows = ingest::rum::convert_object(&data, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!((rows.lines_read, rows.bad_lines, rows.rows), (3, 0, 5));

    let vitals = &rows.windows[&BASE];
    assert_eq!(
        strings(vitals, "metric_name"),
        [Some("ttfb".into()), Some("lcp".into())]
    );
    assert_eq!(floats(vitals, "metric_value"), [Some(205.3), Some(584.0)]);
    assert_eq!(
        strings(vitals, "pathname"),
        [Some("/rum-demo/".into()), Some("/rum-demo/".into())]
    );
    assert_eq!(
        strings(vitals, "cid"),
        [Some("c-batched".into()), Some("c-batched".into())]
    );

    let errors = &rows.errors[&BASE];
    assert_eq!(
        strings(errors, "error_message"),
        [
            Some("Uncaught RangeError: boom".into()),
            Some("ReferenceError: x".into()),
            Some("Uncaught Error: y".into()),
        ]
    );
    assert_eq!(
        strings(errors, "error_file"),
        [
            Some("app.js".into()),
            Some("unknown.js".into()),
            Some("https://example.test/app.js".into()),
        ]
    );
    assert_eq!(ints32(errors, "error_line"), [27, 0, 0]);
    assert_eq!(ints32(errors, "error_col"), [9, 0, 0]);
    assert_eq!(
        strings(errors, "pathname"),
        [
            Some("/rum-demo/".into()),
            Some("/rum-demo/page2".into()),
            Some("/rum-demo/page3".into()),
        ]
    );
}

#[test]
fn the_rum_stream_converts_into_both_tables_under_its_own_plan_keys() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    let mut error_line: Value = serde_json::from_str(&rum_beacon(BASE + 70, "", "")).unwrap();
    error_line["rum_error_message"] = "boom".into();
    let raw = [
        put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]),
        put_rum(
            &fake,
            BASE + 60,
            "b",
            &[rum_beacon(BASE + 65, "INP", "40"), error_line.to_string()],
        ),
    ];
    put_raw(&fake, BASE, "req", &[line(BASE, "q")]);
    let report = rum_tick(&fake, &svc).unwrap();
    assert_eq!((report.converted, report.rows), (2, 3));
    let vitals = fake.keys("processed/rum/client_vitals/");
    let errors = fake.keys("processed/rum/client_errors/");
    assert_eq!(
        (vitals.len(), errors.len()),
        (2, 1),
        "{vitals:?} {errors:?}"
    );
    for key in vitals.iter().chain(&errors) {
        let name = ingest::parse_processed(key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert!(
            key.contains("year=2027/month=01/day=15/hour=08/window="),
            "{key}"
        );
    }
    assert!(
        fake.keys("processed/summary/").is_empty(),
        "RUM writes no summaries"
    );
    assert!(
        fake.keys("processed/request/").is_empty(),
        "the request stream is untouched"
    );
    let kv = fake.kv.borrow();
    assert!(!kv.contains_key(ingest::PLAN_KEY) && !kv.contains_key(ingest::WATERMARK_KEY));
    assert_eq!(
        kv[&ingest::watermark_key("rum")].1["key"].as_str(),
        Some(raw[1].as_str())
    );
    assert!(kv.contains_key(&ingest::plan_key("rum")));
    drop(kv);
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["stream"], "rum");
    assert_eq!(queued[0]["keys"], json!(raw));
    assert_eq!(rum_tick(&fake, &svc).unwrap().converted, 0);
    let deleted = delete_tick(&fake, &svc);
    assert_eq!(deleted.deleted, 2);
    assert!(fake.keys("raw/rum/").is_empty());
    assert_eq!(fake.keys("raw/request/").len(), 1);
}

#[test]
fn rum_commits_fold_running_extents_into_the_watermark_for_the_sync_badge() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    assert_eq!(
        ingest::rum_extents(&fake).unwrap(),
        None,
        "nothing before the first commit"
    );
    put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]);
    rum_tick(&fake, &svc).unwrap();
    let first = ingest::rum_extents(&fake).unwrap().unwrap();
    assert_eq!(first["total_rows"], 1);
    assert_eq!(first["latest_log_at"], ingest::rfc3339(BASE + 60));
    assert_eq!(first["last_sync_at"], ingest::rfc3339(BASE + 3600));
    fake.now.set(BASE as u64 + 3660);
    put_rum(
        &fake,
        BASE + 120,
        "b",
        &[
            rum_beacon(BASE + 125, "INP", "40"),
            rum_beacon(BASE + 130, "CLS", "0.1"),
        ],
    );
    rum_tick(&fake, &svc).unwrap();
    let second = ingest::rum_extents(&fake).unwrap().unwrap();
    assert_eq!(second["total_rows"], 3, "rows accumulate across commits");
    assert_eq!(second["latest_log_at"], ingest::rfc3339(BASE + 180));
    assert_eq!(second["last_sync_at"], ingest::rfc3339(BASE + 3660));
    let kv = fake.kv.borrow();
    let stats = &kv[&ingest::watermark_key("rum")].1[ingest::RUM_STATS_FIELD];
    assert_eq!(stats["rows"], 3);
}

#[test]
fn raw_delete_keeps_the_raw_keys_of_an_uncommitted_rum_plan() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    let key = put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]);
    let mut kv = fake.kv.borrow_mut();
    kv.insert(
        ingest::plan_key("rum"),
        (1, json!({"plan_id": "aaaaaaaaaaaa", "version": 2})),
    );
    kv.insert(
        ingest::watermark_key("rum"),
        (1, json!({"run_id": "bbbbbbbbbbbb", "key": ""})),
    );
    kv.insert(
        format!("{}aaaaaaaaaaaa", ingest::DELETEQ_PREFIX),
        (
            1,
            json!({"run_id": "aaaaaaaaaaaa", "stream": "rum", "keys": [key], "sizes": [1],
                   "inflight_keys": [], "held": [], "planned_at": 0, "at": 0}),
        ),
    );
    drop(kv);
    let report = delete_tick(&fake, &svc);
    assert_eq!(report.deleted, 0);
    assert_eq!(fake.keys("raw/rum/").len(), 1);
    fake.kv
        .borrow_mut()
        .get_mut(&ingest::watermark_key("rum"))
        .unwrap()
        .1["run_id"] = "aaaaaaaaaaaa".into();
    assert_eq!(delete_tick(&fake, &svc).deleted, 1);
}

#[test]
fn logging_period_changes_resume_both_streams_after_a_successful_ten_second_schedule() {
    use fastly_log_analytics::admin::schedule::{ready_period, ready_rum_period};

    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.log_period = 10;
    svc.rum.log_period = 10;
    store_service(&fake, &svc);
    run_tick(&fake);
    for kind in ["ingest", "rum"] {
        assert_eq!(
            fake.kv.borrow()["cron:state"].1["jobs"][kind]["last_success_period"],
            if kind == "rum" {
                ready_rum_period(fake.now.get(), 60)
            } else {
                ready_period(fake.now.get(), 60)
            },
            "initial successful clamped 60s {kind} schedule"
        );
    }

    for (index, (request_period, rum_period)) in
        [(120, 300), (300, 120), (60, 60)].into_iter().enumerate()
    {
        svc.log_period = request_period;
        svc.rum.log_period = rum_period;
        store_service(&fake, &svc);
        fake.now.set(fake.now.get() + 60);
        let at = BASE + index as i64 * 60;
        let name = format!("period-{index}");
        put_raw(&fake, at, &name, &[line(at, &name)]);
        put_rum(&fake, at, &name, &[rum_beacon(at, "LCP", "100")]);
        for _ in 0..8 {
            freshness::finish_tick(&fake);
            let jobs = serde_json::to_value(
                fastly_log_analytics::admin::schedule::peek(
                    &fake,
                    fastly_log_analytics::admin::schedule::SCHED_KEY,
                )
                .unwrap()
                .mirror
                .jobs,
            )
            .unwrap();
            if jobs["ingest"]["last_success_period"]
                == ready_period(fake.now.get(), u64::from(request_period))
                && jobs["rum"]["last_success_period"]
                    == ready_rum_period(fake.now.get(), u64::from(rum_period))
            {
                break;
            }
            fake.now.set(fake.now.get() + 1);
        }
        let jobs = serde_json::to_value(
            fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror
            .jobs,
        )
        .unwrap();
        for (kind, period) in [("ingest", request_period), ("rum", rum_period)] {
            assert_eq!(
                jobs[kind]["last_success_period"],
                if kind == "rum" {
                    ready_rum_period(fake.now.get(), u64::from(period))
                } else {
                    ready_period(fake.now.get(), u64::from(period))
                },
                "{kind} must use the changed {period}s cadence"
            );
            assert_eq!(jobs[kind]["last_success_period_s"], period);
        }
        assert_eq!(processed_ips(&fake).len(), index + 1);
        assert_eq!(fake.keys("processed/rum/client_vitals/").len(), index + 1);

        let next = fake.now.get() + 1;
        let expected = fastly_log_analytics::admin::schedule::due(
            &fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror,
            next,
        );
        fake.now.set(next);
        run_tick(&fake);
        let after = serde_json::to_value(
            fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror
            .jobs,
        )
        .unwrap();
        assert_eq!(
            after["ingest"]["last_run"],
            if expected.ingest.is_some() {
                json!(next)
            } else {
                jobs["ingest"]["last_run"].clone()
            },
            "a drained stream must follow its configured polling period"
        );
        assert_eq!(
            after["rum"]["last_run"],
            if expected.rum.is_some() {
                json!(next)
            } else {
                jobs["rum"]["last_run"].clone()
            }
        );
    }
}

#[test]
fn legacy_scheduler_state_migrates_after_cache_eviction_and_a_cas_conflict() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.log_period = 10;
    svc.rum.log_period = 10;
    store_service(&fake, &svc);
    run_tick(&fake);
    {
        let mut kv = fake.kv.borrow_mut();
        let jobs = &mut kv.get_mut("cron:state").unwrap().1["jobs"];
        for kind in ["ingest", "rum"] {
            jobs[kind]
                .as_object_mut()
                .unwrap()
                .remove("last_success_period_s");
        }
    }
    fake.cache.borrow_mut().clear();
    fake.race_cron_state.set(1);
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    for kind in ["ingest", "rum"] {
        assert_eq!(
            fake.kv.borrow()["cron:state"].1["jobs"][kind]["last_success_period_s"],
            60,
            "{kind} must persist its clamped cadence even after a CAS retry"
        );
    }
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    assert_eq!(fake.calls.get(), 0, "migration must not repeat every tick");
}

#[test]
fn rum_ingest_runs_only_when_its_own_log_period_is_due() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.rum.log_period = 300;
    store_service(&fake, &svc);
    let count = |fake: &Fake, kind: &str| {
        rows(fake, LogKind::CronRuns)
            .into_iter()
            .filter(|r| r["cron_kind"] == kind)
            .count()
    };
    run_tick(&fake);
    assert_eq!(count(&fake, "rum_ingest"), 1, "the first RUM period is due");
    let mut rum_ticks = Vec::new();
    for minute in 1..=10 {
        fake.now.set((BASE + 3600 + minute * 60) as u64);
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "q")],
        );
        let before = count(&fake, "rum_ingest");
        run_tick(&fake);
        if count(&fake, "rum_ingest") > before {
            rum_ticks.push(minute);
        }
    }
    assert_eq!(count(&fake, "request_ingest"), 11);
    assert_eq!(rum_ticks.len(), 2, "every 5 minutes: {rum_ticks:?}");
    assert_eq!(rum_ticks[1] - rum_ticks[0], 5);
}

#[test]
fn a_tick_ingests_the_rum_stream_after_requests_and_a_deferred_rum_goes_first_next() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    store_service(&fake, &rum_service());
    let rum_first = |fake: &Fake| fake.kv.borrow()["cron:state"].1["rum_first"].clone();
    let rum_rows = |fake: &Fake| -> Vec<Value> {
        rows(fake, LogKind::CronRuns)
            .into_iter()
            .filter(|r| r["cron_kind"] == "rum_ingest")
            .collect()
    };
    put_raw(&fake, BASE, "a", &[line(BASE, "q")]);
    put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]);
    run_tick(&fake);
    assert_eq!(processed_ips(&fake), ["q"]);
    assert!(
        fake.keys("processed/rum/").is_empty(),
        "after a converting request step only the RUM listing fits the budget"
    );
    let rum = rum_rows(&fake);
    assert!(rum.len() <= 1);
    if let Some(stage) = rum.first() {
        assert_eq!(stage["more_pending"], true);
    }
    assert_eq!(rum_first(&fake), true);

    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "r")]);
    put_rum(
        &fake,
        BASE + 60,
        "b",
        &[rum_beacon(BASE + 65, "LCP", "100")],
    );
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(
        fake.keys("processed/rum/client_vitals/").len(),
        2,
        "the deferred stream goes first"
    );
    assert_eq!(rum_first(&fake), false);
    fake.now.set(fake.now.get() + 120);
    freshness::finish_tick(&fake);
    assert_eq!(processed_ips(&fake), ["q", "r"]);

    put_rum(
        &fake,
        BASE + 120,
        "c",
        &[rum_beacon(BASE + 125, "LCP", "100")],
    );
    fake.now.set(fake.now.get() + 120);
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let before = rum_rows(&fake).len();
    run_tick(&fake);
    assert_eq!(
        rum_rows(&fake).len(),
        before,
        "no second step after the dispatch window"
    );
    assert_eq!(rum_first(&fake), true, "a skipped RUM step goes first next");
}

#[test]
fn zstd_positional_array_raw_logs_and_period_aligned_windows_round_trip() {
    let fake = Fake::new(BASE + 3600);
    let mut svc = service();
    svc.log_period = 300;

    let fmt = fastly_log_analytics::logging::generate_compact_log_format(&json!({
        "preset": "full",
        "fields": ingest::convert::columns()
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>()
    }))
    .unwrap();
    assert!(fmt.starts_with("[null,") && fmt.ends_with(']'));

    let make_array_line = |ts_ms: i64, ip: &str, host: &str, status: i64| {
        let mut arr = vec![Value::Null; ingest::convert::columns().len()];
        for (idx, col) in ingest::convert::columns().iter().enumerate() {
            match col.id {
                "ts_ms" => arr[idx] = json!(ts_ms),
                "ip" => arr[idx] = json!(ip),
                "host" => arr[idx] = json!(host),
                "status" => arr[idx] = json!(status),
                "cache" => arr[idx] = json!("HIT"),
                "method" => arr[idx] = json!("GET"),
                _ => {}
            }
        }
        serde_json::to_string(&arr).unwrap()
    };

    let raw_lines = [
        make_array_line((BASE + 200) * 1000, "198.51.100.2", "b.example.com", 200),
        make_array_line((BASE + 50) * 1000, "198.51.100.1", "a.example.com", 404),
        make_array_line((BASE + 10) * 1000, "198.51.100.3", "a.example.com", 200),
    ];
    let payload = raw_lines.join("\n") + "\n";
    let zstd_bytes = zstd::stream::encode_all(payload.as_bytes(), 3).unwrap();
    let zst_key = raw_key(BASE, "batch");
    fake.bucket.borrow_mut().insert(zst_key, zstd_bytes);

    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run(&fake, &svc, "ak", "sk", Instant::now(), &buffer).unwrap();
    assert_eq!((report.rows, report.bad_lines), (3, 0));
    let keys = fake.keys("processed/request/");
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].contains(&format!("/window={}-{}-", BASE, BASE + 300)),
        "expected 300s window file, got {:?}",
        keys
    );
    let summary_keys = fake.keys("processed/summary/");
    assert_eq!(summary_keys.len(), 1);
    assert!(
        summary_keys[0].contains(&format!("/summary={}-{}-", BASE, BASE + 300)),
        "expected 300s summary file, got {:?}",
        summary_keys
    );
    // Verify low-cardinality sort order: ("a.example.com", 200) then ("a.example.com", 404) then ("b.example.com", 200)
    let ordered_ips: Vec<String> = batches(&fake.bucket.borrow()[&keys[0]])
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("ip")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..col.len()).map(move |i| col.value(i).to_owned())
        })
        .collect();
    assert_eq!(
        ordered_ips,
        ["198.51.100.3", "198.51.100.1", "198.51.100.2"]
    );
}

#[test]
fn compact_log_format_custom_fields_land_in_their_own_columns() {
    let mut fields = json!({
        "preset": "full",
        "fields": ingest::convert::columns().iter().map(|c| c.id).collect::<Vec<_>>(),
        "custom_fields": [
            {"name": "lt_run_id", "vcl": "req.http.x-lt-run-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
            {"name": "lt_request_id", "vcl": "req.http.x-lt-request-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
        ],
    });
    let cmcd = fastly_log_analytics::logging::cmcd::Config {
        enabled: true,
        ..Default::default()
    };
    fastly_log_analytics::logging::cmcd::reconcile_fields(&mut fields, &cmcd, None, false).unwrap();
    let batch = convert_compact_line(
        &fields,
        &[
            ("lt_run_id", "run-7"),
            ("lt_request_id", "req-9"),
            ("cmcd_sid", "sess-3"),
            ("cmcd_br", "1200"),
        ],
    );
    assert_eq!(text_at(&batch, "lt_run_id").as_deref(), Some("run-7"));
    assert_eq!(text_at(&batch, "lt_request_id").as_deref(), Some("req-9"));
    assert_eq!(text_at(&batch, "cmcd_sid").as_deref(), Some("sess-3"));
    assert_eq!(number_at(&batch, "cmcd_br"), Some(1200));
    assert_eq!(number_at(&batch, "cmcd_bl"), None);
    assert_eq!(text_at(&batch, "_source_file"), None);
    assert_eq!(number_at(&batch, "requests"), None);
}

#[test]
fn compact_log_format_custom_field_of_unsupported_type_keeps_its_slot() {
    let fields = json!({
        "preset": "full",
        "fields": ingest::convert::columns().iter().map(|c| c.id).collect::<Vec<_>>(),
        "custom_fields": [
            {"name": "event_at", "vcl": "req.http.x-event-at", "parquet_type": "TIMESTAMP",
             "enabled": true, "committed": true},
            {"name": "lt_run_id", "vcl": "req.http.x-lt-run-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
        ],
    });
    let batch = convert_compact_line(
        &fields,
        &[("event_at", "2027-01-15T08:00:00Z"), ("lt_run_id", "run-7")],
    );
    assert!(batch.column_by_name("event_at").is_none());
    assert_eq!(text_at(&batch, "lt_run_id").as_deref(), Some("run-7"));
}

#[test]
fn rum_zstd_positional_array_and_sorted_parquet_round_trip() {
    let fake = Fake::new(BASE + 3600);
    let mut svc = rum_service();
    svc.rum.log_period = 300;

    let fmt = fastly_log_analytics::logging::rum::generate_compact_rum_log_format().unwrap();
    assert!(fmt.starts_with('[') && fmt.ends_with(']'));

    let make_rum_array = |ts_ms: i64, req_id: &str, query: &str| {
        json!([
            ts_ms,
            SID,
            req_id,
            format!("/rum-beacon?service_id={SID}&{query}"),
            "Paris",
            "IDF",
            "FR",
            "CDG",
            "1.3",
            0.012
        ])
        .to_string()
    };

    let raw_lines = [
        make_rum_array((BASE + 200) * 1000, "r-lcp", "m=LCP:1200:good"),
        make_rum_array(
            (BASE + 50) * 1000,
            "r-cls",
            "m=CLS:0.05:good&e=boom|app.js|10|2",
        ),
    ];
    let payload = raw_lines.join("\n") + "\n";
    let zstd_bytes = zstd::stream::encode_all(payload.as_bytes(), 3).unwrap();
    let zst_key = "raw/rum/year=2027/month=01/day=15/hour=08/minute=00/rum-batch.zst".to_string();
    fake.bucket.borrow_mut().insert(zst_key, zstd_bytes);

    let report = rum_tick(&fake, &svc).unwrap();
    assert_eq!((report.rows, report.bad_lines), (3, 0));
    let vitals_keys = fake.keys("processed/rum/client_vitals/");
    let errors_keys = fake.keys("processed/rum/client_errors/");
    assert_eq!(vitals_keys.len(), 1);
    assert_eq!(errors_keys.len(), 1);
    assert!(
        vitals_keys[0].contains(&format!("/window={}-{}-", BASE, BASE + 300)),
        "expected 300s RUM vitals window file, got {:?}",
        vitals_keys
    );
    let vitals_batch = &batches(&fake.bucket.borrow()[&vitals_keys[0]])[0];
    // Sorted by metric_name ascending: CLS before LCP
    assert_eq!(
        strings(vitals_batch, "metric_name"),
        [Some("CLS".into()), Some("LCP".into())]
    );
    assert_eq!(
        strings(vitals_batch, "req_id"),
        [Some("r-cls".into()), Some("r-lcp".into())]
    );
    let vitals_bytes = Bytes::from(fake.bucket.borrow()[&vitals_keys[0]].clone());
    let builder = ParquetRecordBatchReaderBuilder::try_new(vitals_bytes).unwrap();
    let rg = builder.metadata().row_group(0);
    let col_meta = |name: &str| {
        rg.columns()
            .iter()
            .find(|c| c.column_descr().name() == name)
            .unwrap()
    };
    assert!(col_meta("timestamp")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::DELTA_BINARY_PACKED));
    assert!(col_meta("metric_value")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::BYTE_STREAM_SPLIT));
    assert!(col_meta("ttfb")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::BYTE_STREAM_SPLIT));
    assert!(col_meta("req_id").dictionary_page_offset().is_none());
}

/// Task 23: size-aware compaction of closed request hours.
mod request_compaction {
    use super::*;
    use arrow_array::BinaryArray;
    use fastly_log_analytics::ingest::compaction::{self, Report, DELETE_GRACE_S};
    use std::sync::Arc;

    const HOUR: i64 = BASE - BASE.rem_euclid(3600);
    const NOW: i64 = HOUR + 4 * 3600;
    const TARGET: u64 = 64 << 20;
    const ROOT: &str = "processed/request/";

    fn window(fake: &Fake, start: i64, ips: &[&str]) -> String {
        let lines: Vec<String> = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| line(start + i as i64, ip))
            .collect();
        let rows = ingest::convert::convert_object(&gz(&lines), 60, start).unwrap();
        let bytes = ingest::convert::write_parquet(&[&rows.windows[&start]]).unwrap();
        let key = ingest::processed_key(ROOT, start, start + 60, "abcdef012345", ips.len() as u64);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    /// A window file whose schema differs from the request schema: one binary column.
    fn odd_window(fake: &Fake, start: i64, values: &[Vec<u8>]) -> String {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "blob",
            arrow_schema::DataType::Binary,
            false,
        )]));
        let column = BinaryArray::from_iter_values(values.iter());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let key =
            ingest::processed_key(ROOT, start, start + 60, "abcdef012345", values.len() as u64);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    fn run(fake: &Fake, target: u64) -> Report {
        fake.new_request();
        fake.vcpu.set(0);
        let service = service();
        compaction::request_compact(&store(fake, &service), "ck", target, 0).unwrap()
    }

    #[test]
    fn a_lagging_checkpoint_save_does_not_spend_the_final_state_write() {
        let fake = Fake::new(NOW);
        seed(&fake, 60);
        fake.lag_compact_reads.set(true);
        let service = service();
        for (step, conflict, lag) in [(0, true, 1), (107, false, 3), (129, false, 4)] {
            fake.now.set(fake.now.get() + step);
            fake.conflict_watermark.set(conflict);
            fake.lagging_compact_reads.set(lag);
            fake.new_request();
            let report =
                compaction::request_compact_pass(&store(&fake, &service), "ck", TARGET, 1, None)
                    .unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(fake.calls.get() < BUDGET);
        }
    }

    /// Passes under random watermark contention and KV API lag stop within their calls instead
    /// of failing: a write they can't confirm is left to the next pass.
    #[test]
    fn passes_under_kv_lag_and_contention_stay_within_their_calls() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rnd = |m: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % m
        };
        let service = service();
        for trial in 0..96 {
            let fake = Fake::new(NOW);
            seed(&fake, [3, 8, 33, 60, 90][trial % 5]);
            fake.lag_compact_reads.set(true);
            for pass in 0..30 {
                fake.conflict_watermark.set(rnd(3) == 0);
                fake.lagging_compact_reads.set(rnd(6) as u32);
                fake.new_request();
                let report = compaction::request_compact_pass(
                    &store(&fake, &service),
                    "ck",
                    TARGET,
                    1,
                    None,
                )
                .unwrap_or_else(|error| panic!("trial {trial} pass {pass}: {error}"));
                assert!(
                    report.errors.is_empty(),
                    "trial {trial} pass {pass}: {:?}",
                    report.errors
                );
                assert!(
                    fake.calls.get() < BUDGET,
                    "trial {trial} pass {pass} spent its tail"
                );
                fake.now.set(fake.now.get() + 1 + rnd(400));
            }
        }
    }

    fn finish(fake: &Fake, target: u64) -> Report {
        for _ in 0..64 {
            let report = run(fake, target);
            if report.compacts > 0 || !report.more_pending {
                return report;
            }
        }
        panic!("resumable compaction failed to finish a bounded job");
    }

    #[test]
    fn persisted_deletion_queue_accounts_exact_source_bytes_after_a_complete_delete() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 3);
        let expected: u64 = sources
            .iter()
            .map(|key| fake.bucket.borrow()[key].len() as u64)
            .sum();
        finish(&fake, TARGET);
        fake.usage
            .drain_rows(&fastly_log_analytics::auth::settings::UsageLogging::default());
        fake.now
            .set(fake.now.get() + compaction::DELETE_GRACE_S + 1);

        let report = run(&fake, TARGET);

        assert_eq!(report.deleted, 3);
        let rows = fake
            .usage
            .drain_rows(&fastly_log_analytics::auth::settings::UsageLogging::default());
        assert_eq!(
            rows.iter()
                .map(|row| row["bytes_deleted"].as_u64().unwrap())
                .sum::<u64>(),
            expected
        );
        assert!(rows
            .iter()
            .filter(|row| row["op"] == "DELETE_OBJECTS")
            .all(|row| row["bytes_deleted_complete"] == true));
    }

    #[test]
    fn partial_compaction_delete_accounts_only_confirmed_source_bytes_without_recounting() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 3);
        let source_sizes: BTreeMap<String, u64> = sources
            .iter()
            .map(|key| (key.clone(), fake.bucket.borrow()[key].len() as u64))
            .collect();
        finish(&fake, TARGET);
        let failed = sources[1].clone();
        fake.fail_delete_keys.borrow_mut().insert(failed.clone());
        fake.usage
            .drain_rows(&fastly_log_analytics::auth::settings::UsageLogging::default());
        fake.now
            .set(fake.now.get() + compaction::DELETE_GRACE_S + 1);

        let partial = run(&fake, TARGET);
        let rows = fake
            .usage
            .drain_rows(&fastly_log_analytics::auth::settings::UsageLogging::default());
        let failed_bytes = source_sizes[&failed];
        let confirmed_bytes = sources
            .iter()
            .filter(|key| **key != failed)
            .map(|key| source_sizes[key])
            .sum::<u64>();

        assert_eq!((partial.deleted, partial.failed), (2, 1));
        assert_eq!(
            rows.iter()
                .map(|row| row["bytes_deleted"].as_u64().unwrap())
                .sum::<u64>(),
            confirmed_bytes
        );
        assert!(rows
            .iter()
            .filter(|row| row["op"] == "DELETE_OBJECTS")
            .all(|row| row["bytes_deleted_complete"] == true));
        fake.fail_delete_keys.borrow_mut().clear();
        assert_eq!(run(&fake, TARGET).deleted, 1);
        let rows = fake
            .usage
            .drain_rows(&fastly_log_analytics::auth::settings::UsageLogging::default());
        assert_eq!(
            rows.iter()
                .map(|row| row["bytes_deleted"].as_u64().unwrap())
                .sum::<u64>(),
            failed_bytes
        );
    }

    pub(super) fn compacts(fake: &Fake) -> Vec<String> {
        fake.keys(ROOT)
            .into_iter()
            .filter(|k| k.contains("/compact="))
            .collect()
    }

    fn column(bytes: &[u8], name: &str) -> Vec<String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let values = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            out.extend(values.iter().map(|v| v.unwrap_or_default().to_owned()));
        }
        out
    }

    fn manifest_gen(fake: &Fake) -> u64 {
        fake.kv
            .borrow()
            .get(ingest::WATERMARK_KEY)
            .and_then(|(_, v)| v["manifest_gen"].as_u64())
            .unwrap_or(0)
    }

    pub(super) fn seed(fake: &Fake, count: i64) -> Vec<String> {
        (0..count)
            .map(|i| window(fake, HOUR + i * 60, &[&format!("a{i}"), &format!("b{i}")]))
            .collect()
    }

    /// Request files `/api/files` lists from `from` to `to`, the count a browser range scan
    /// registers.
    fn visible(fake: &Fake, from: i64, to: i64) -> usize {
        let now = fake.now.get();
        let query = format!("from={}&to={}", iso_url(from), iso_url(to));
        let count = catalog_keys(fake, &query).1.len();
        fake.now.set(now);
        count
    }

    /// Bursty ingest: `per_minute` small window files a minute for `hours`, compaction turns on
    /// the 30 s maintenance cadence chaining passes as `request_compact_stage` does. Returns the
    /// most files a 48 h browser range held at any quarter hour and the count when ingest ends.
    fn burst(per_minute: u64, hours: i64) -> (usize, usize) {
        let start = HOUR + 3600;
        let fake = Fake::new(start);
        let service = service();
        let lines = [line(start, "a"), line(start + 1, "b")];
        let rows = ingest::convert::convert_object(&gz(&lines), 60, start).unwrap();
        let bytes = ingest::convert::write_parquet(&[&rows.windows[&start]]).unwrap();
        let mut peak = 0;
        let mut serial = 0u64;
        let end = start + hours * 3600;
        let mut t = start;
        while t < end {
            t += 30;
            fake.now.set(t as u64);
            if t % 60 == 0 {
                let window = t - 60;
                let keys: Vec<String> = (0..per_minute)
                    .map(|_| {
                        serial += 1;
                        let key = ingest::processed_key(
                            ROOT,
                            window,
                            window + 60,
                            &format!("{serial:012x}"),
                            2,
                        );
                        fake.bucket.borrow_mut().insert(key.clone(), bytes.clone());
                        key
                    })
                    .collect();
                publish_files(&fake, &keys, &[], &[]);
                fake.now.set(t as u64);
            }
            fake.new_request();
            let due = fake
                .admin_get(ingest::WATERMARK_KEY)
                .unwrap()
                .map(|record| ingest::catalog::head(&record.value).unwrap())
                .is_some_and(|head| head.compaction_due || head.next_compaction_check <= t as u64);
            if due {
                let id = format!("turn{t}");
                for pass in 1..=8u32 {
                    fake.new_request();
                    fake.vcpu.set(0);
                    let report = compaction::request_compact_pass(
                        &store(&fake, &service),
                        "ck",
                        TARGET,
                        1,
                        Some(compaction::Turn {
                            id: &id,
                            pass,
                            last: pass == 8,
                        }),
                    )
                    .unwrap();
                    assert!(report.errors.is_empty(), "{:?}", report.errors);
                    let blocked = matches!(
                        report.stopped.as_deref(),
                        Some("lease_held" | "lease_conflict" | "state_lag")
                    );
                    if !report.more_pending || blocked {
                        break;
                    }
                }
                fake.now.set(t as u64);
            }
            if t % 900 == 0 {
                peak = peak.max(visible(&fake, t - 48 * 3600, t));
            }
        }
        (peak, visible(&fake, end - 48 * 3600, end))
    }

    /// A load test's burst, ~1,440 window files an hour for six hours, stays far below the
    /// browser's 256-file range budget for a 24 h window and its 24 h baseline.
    #[test]
    fn bursty_ingest_keeps_a_48_hour_range_far_below_the_browser_file_budget() {
        let (peak, last) = burst(24, 6);
        eprintln!("48h visible files: peak {peak}, at end {last}");
        assert!(peak <= 64, "peak {peak} visible files in 48 h");
        assert!(last <= 64, "{last} visible files in 48 h");
    }

    #[test]
    fn small_sources_merge_into_shared_row_groups_without_losing_rows() {
        let fake = Fake::new(NOW);
        seed(&fake, 24);
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 24);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        let bytes = fake.bucket.borrow()[&outputs[0]].clone();
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
        let metadata = reader.metadata().clone();
        assert_eq!(metadata.file_metadata().num_rows(), 48);
        // One row group per source would cap an hour's compact at the footer limit long
        // before its byte target; merged groups keep a load hour to one or two files.
        assert!(
            metadata.num_row_groups() < 6,
            "{} row groups",
            metadata.num_row_groups()
        );
        for group in metadata.row_groups() {
            assert!(!group.sorting_columns().unwrap_or(&Vec::new()).is_empty());
        }
        let mut values = column(&bytes, "ip");
        values.sort();
        let mut expected: Vec<String> = (0..24)
            .flat_map(|i| [format!("a{i}"), format!("b{i}")])
            .collect();
        expected.sort();
        assert_eq!(values, expected);
    }

    /// `count` window files of `rows` distinct clients from `from`, five shards per minute as
    /// a load hour's ingest writes them, each minute published in its own commit.
    fn load_windows(fake: &Fake, from: i64, count: i64, rows: i64) -> Vec<String> {
        let mut all = Vec::new();
        for minute in 0..(count + 4) / 5 {
            let start = from + minute * 60;
            let mut keys = Vec::new();
            for shard in 0..(count - minute * 5).min(5) {
                let lines: Vec<String> = (0..rows)
                    .map(|n| {
                        let ip = format!("10.{}.{shard}.{n}", minute % 256);
                        line(start + n % 60, &ip)
                    })
                    .collect();
                let converted = ingest::convert::convert_object(&gz(&lines), 60, start).unwrap();
                let bytes = ingest::convert::write_parquet(&[&converted.windows[&start]]).unwrap();
                let shard_id = format!("abcdef01234{shard}");
                let key = ingest::processed_key(ROOT, start, start + 60, &shard_id, rows as u64);
                fake.bucket.borrow_mut().insert(key.clone(), bytes);
                keys.push(key);
            }
            let now = fake.now.replace((start + 120) as u64);
            publish_files(fake, &keys, &[], &[]);
            fake.now.set(now);
            all.extend(keys);
        }
        all
    }

    /// Pass `pass` of maintenance turn `turn`, 20 s into the stage's 40 s of wall time.
    fn turn_pass(fake: &Fake, turn: &str, pass: u32) -> Report {
        try_turn_pass(fake, turn, pass).unwrap()
    }

    fn try_turn_pass(fake: &Fake, turn: &str, pass: u32) -> Result<Report, String> {
        fake.new_request();
        fake.vcpu.set(0);
        fake.elapsed_s.set(20);
        let service = service();
        let store = store(fake, &service);
        let turn = compaction::Turn {
            id: turn,
            pass,
            last: false,
        };
        let report = compaction::request_compact_pass(&store, "ck", TARGET, 1, Some(turn));
        fake.elapsed_s.set(0);
        report
    }

    /// A maintenance turn's chained passes hand the lease on rather than each taking it with
    /// its own state write, and a pass that won't complete its job records the checkpoint in its
    /// final state write. That leaves two passes enough calls to upload a 32-source compact.
    #[test]
    fn a_turn_uploads_a_load_hours_32_source_compact_in_two_passes() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        let first = turn_pass(&fake, "turn-1", 1);
        assert_eq!((first.compacts, first.bytes_written), (0, 0), "{first:?}");
        assert!(first.more_pending);
        let second = turn_pass(&fake, "turn-1", 2);
        assert!(second.bytes_written > 0, "{second:?}");
        let third = turn_pass(&fake, "turn-1", 3);
        assert_eq!((third.compacts, third.sources), (1, 32), "{third:?}");
    }

    /// The handed-on lease admits only the same turn's next pass; another turn waits for it to
    /// lapse, which it does within the stage's wall time.
    #[test]
    fn a_handed_on_lease_admits_only_the_same_turn() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        assert!(turn_pass(&fake, "turn-1", 1).more_pending);
        for (turn, pass) in [("turn-2", 2), ("turn-1", 1), ("turn-1", 3)] {
            let other = turn_pass(&fake, turn, pass);
            assert_eq!(other.stopped.as_deref(), Some("lease_held"), "{other:?}");
        }
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let lease = state["lease_until"].as_u64().unwrap();
        assert!(lease <= fake.now.get() + 30, "{state}");
        fake.now.set(lease + 1);
        let later = turn_pass(&fake, "turn-2", 1);
        assert_eq!(later.stopped, None, "{later:?}");
        assert!(later.bytes_written > 0, "{later:?}");
    }

    /// A handed-on lease runs from when it is handed on, not from when the handing pass began.
    /// Otherwise it lapses while the next pass still works, an overlapping turn takes it, and
    /// the next pass's state write then fails with `compact_state_conflict`.
    #[test]
    fn a_handed_on_lease_covers_the_next_pass_after_a_long_handing_pass() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        let began = fake.now.get();
        fake.call_ms.set(300);
        assert!(turn_pass(&fake, "turn-1", 1).more_pending);
        fake.call_ms.set(0);
        let handed = fake.now.get();
        assert!(handed >= began + 6, "pass 1 took {} s", handed - began);
        let left_by_pass_1 = fake.kv.borrow()[compaction::STATE_KEY].clone();
        // Pass 2 starts right after pass 1 with the 20 s of wall time pass 1 had left. Another
        // turn's cron arrives near the end of that wall time.
        fake.now.set(handed + 18);
        let other = turn_pass(&fake, "turn-2", 1);
        // Pass 2 read the state before that turn arrived.
        *fake.lagging_compact_state.borrow_mut() = Some(Record {
            generation: left_by_pass_1.0.to_string(),
            value: left_by_pass_1.1,
        });
        fake.now.set(handed + 1);
        let second = try_turn_pass(&fake, "turn-1", 2);
        let second = second.unwrap();
        assert_eq!(second.stopped, None, "{second:?}");
        assert!(second.bytes_written > 0, "{second:?}");
        assert_eq!(other.stopped.as_deref(), Some("lease_held"), "{other:?}");
    }

    /// While an hour still has window files to merge, new compacts take only windows; the
    /// hour's compacts are merged together once, after its windows are done. So a row is
    /// rewritten at most twice instead of once per compact the hour grows by.
    #[test]
    fn an_hours_compact_is_not_rewritten_with_each_new_batch_of_windows() {
        let fake = Fake::new(NOW);
        let windows = load_windows(&fake, HOUR, 96, 10);
        let mut rewritten = 0;
        for _ in 0..64 {
            let report = run(&fake, TARGET);
            rewritten += report.rows;
            if !report.more_pending {
                break;
            }
        }
        let outputs = compacts(&fake);
        let live: Vec<_> = fake
            .keys(ROOT)
            .into_iter()
            .filter(|key| windows.contains(key) || key.contains("/compact="))
            .collect();
        assert_eq!(outputs.len(), 4, "{live:?}");
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let listed = catalog_keys(&fake, &query).1;
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert!(rewritten <= 2 * 960, "{rewritten} rows rewritten");
    }

    /// A pass that reads a job's last source completes it without first checkpointing it.
    /// Native time for each pass of a load hour's job: 32 windows of 400 distinct clients each,
    /// published in one commit as the catalog's compaction keeps them. Compute runs WebAssembly,
    /// so expect a small multiple of these numbers there.
    #[test]
    #[ignore = "timing: cargo test --release --test ingest pass_timing -- --ignored --nocapture"]
    fn pass_timing_for_a_load_hour_job() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 400);
        for pass in 1..=16 {
            let began = Instant::now();
            let report = run(&fake, TARGET);
            eprintln!(
                "pass {pass}: {:.1} ms, {} sources, {} rows, {} compacts, stopped {:?}",
                began.elapsed().as_secs_f64() * 1000.0,
                report.sources,
                report.rows,
                report.compacts,
                report.stopped,
            );
            if report.compacts > 0 {
                return;
            }
        }
        panic!("the job did not finish");
    }

    #[test]
    fn floor_key_survives_compaction_and_catalog_retirement() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let floor = raw_floor("request", HOUR);
        set_floor(&fake, ingest::WATERMARK_KEY, &floor);
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 2);
        assert_eq!(compacts(&fake).len(), 1);
        for _ in 0..8 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].0;
        fake.now.set((NOW + 30 * 86_400) as u64);
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.new_request();
        assert!(ingest::catalog::retire(&store(&fake, &svc)).unwrap() > 0);
        assert!(fake.kv.borrow()[ingest::WATERMARK_KEY].0 > generation);
        assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
    }

    #[test]
    fn stale_compaction_state_does_not_prevent_resuming_the_current_generation() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let state = serde_json::to_value(compaction::State::default()).unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (2, state.clone()));
        *fake.stale_compact_state.borrow_mut() = Some(Record {
            generation: "1".into(),
            value: state,
        });
        let report = run(&fake, TARGET);
        assert_eq!(report.stopped, None);
        assert!(report.errors.is_empty());
        assert!(fake.kv.borrow()[compaction::STATE_KEY].0 > 2);
        let report = if report.compacts > 0 {
            report
        } else {
            finish(&fake, TARGET)
        };
        assert_eq!(report.sources, 2);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        let mut values = column(&fake.bucket.borrow()[&outputs[0]], "ip");
        values.sort();
        assert_eq!(values, ["a0", "a1", "b0", "b1"]);
    }

    #[test]
    fn fresh_compaction_state_preserves_an_owner_hidden_by_a_stale_lookup() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let active = serde_json::to_value(compaction::State {
            lease_until: NOW as u64 + 70,
            ..Default::default()
        })
        .unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (2, active.clone()));
        *fake.stale_compact_state.borrow_mut() = Some(Record {
            generation: "1".into(),
            value: serde_json::to_value(compaction::State::default()).unwrap(),
        });
        let before = fake.bucket.borrow().clone();
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert_eq!(report.stopped.as_deref(), Some("lease_held"));
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY], (2, active));
        assert_eq!(*fake.bucket.borrow(), before);
    }

    #[test]
    fn unavailable_fresh_compaction_state_fails_before_lease_or_object_mutation() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let before = fake.bucket.borrow().clone();
        fake.fail_fresh_compact_state.set(true);
        fake.new_request();
        let error =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap_err();
        assert_eq!(error, "injected_fresh_compact_state_failure");
        assert!(!fake.kv.borrow().contains_key(compaction::STATE_KEY));
        assert_eq!(*fake.bucket.borrow(), before);
        assert!(fake.uploads.borrow().is_empty());
    }

    #[test]
    fn a_compaction_state_body_cut_short_once_is_read_again() {
        let fake = Fake::new(NOW);
        seed(&fake, 24);
        fake.fresh_compact_state_glitches
            .borrow_mut()
            .push_back("backend_json_invalid".into());
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 24);
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn a_compaction_state_body_unreadable_twice_fails_before_any_mutation() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let before = fake.bucket.borrow().clone();
        for _ in 0..2 {
            fake.fresh_compact_state_glitches
                .borrow_mut()
                .push_back("backend_body_unavailable".into());
        }
        fake.new_request();
        let error =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap_err();
        assert_eq!(error, "backend_body_unavailable");
        assert!(!fake.kv.borrow().contains_key(compaction::STATE_KEY));
        assert_eq!(*fake.bucket.borrow(), before);
        assert!(fake.uploads.borrow().is_empty());
    }

    #[test]
    fn a_compaction_state_read_that_is_not_a_state_once_is_read_again() {
        let fake = Fake::new(NOW);
        seed(&fake, 24);
        *fake.lagging_compact_state.borrow_mut() = Some(Record {
            generation: "9".into(),
            value: json!({"upload": 5}),
        });
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 24);
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn compaction_state_conflict_before_lease_is_deferred_without_io() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let state = compaction::State {
            upload: Some(compaction::Upload {
                key: format!("{ROOT}prior.parquet"),
                id: "prior-upload".into(),
            }),
            ..Default::default()
        };
        let state = serde_json::to_value(state).unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (1, state.clone()));
        fake.uploads.borrow_mut().insert(
            "prior-upload".into(),
            (format!("{ROOT}prior.parquet"), BTreeMap::new()),
        );
        let bucket_before = fake.bucket.borrow().clone();
        let uploads_before = fake.uploads.borrow().clone();
        fake.compact_conflict_after.set(Some(0));
        fake.new_request();
        let report =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap();
        assert!(report.more_pending);
        assert!(report.errors.is_empty());
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["stopped"], "lease_conflict");
        assert_eq!(value["lease_generation"], "1");
        assert_eq!(report.compacts, 0);
        assert_eq!(*fake.bucket.borrow(), bucket_before);
        assert_eq!(*fake.uploads.borrow(), uploads_before);
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY], (2, state));
        assert_eq!(finish(&fake, TARGET).sources, 2);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        assert_eq!(column(&fake.bucket.borrow()[&outputs[0]], "ip").len(), 4);
    }

    #[test]
    fn compaction_state_conflict_after_lease_still_fails() {
        let fake = Fake::new(NOW);
        fake.kv.borrow_mut().insert(
            compaction::STATE_KEY.into(),
            (
                1,
                serde_json::to_value(compaction::State::default()).unwrap(),
            ),
        );
        fake.compact_conflict_after.set(Some(1));
        fake.foreign_compact_conflict.set(true);
        fake.new_request();
        let error =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap_err();
        assert_eq!(error, "compact_state_conflict");
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY].0, 3);
        assert!(
            fake.waits.borrow().is_empty(),
            "another writer's value is not waited on"
        );
    }

    /// A state write can land and still report a lost CAS: the KV insert's first attempt
    /// errors after landing and its retry fails the precondition against that write. The pass
    /// finds its own value stored and goes on, whichever of its writes it was.
    #[test]
    fn a_state_write_that_landed_but_reported_a_conflict_is_not_a_conflict() {
        for nth in 0..4 {
            let fake = Fake::new(NOW);
            seed(&fake, 2);
            fake.kv.borrow_mut().insert(
                compaction::STATE_KEY.into(),
                (
                    1,
                    serde_json::to_value(compaction::State::default()).unwrap(),
                ),
            );
            fake.compact_write_lands_unreported.set(Some(nth));
            let report = finish(&fake, TARGET);
            assert_eq!(
                fake.compact_write_lands_unreported.get(),
                None,
                "write {nth} was injected"
            );
            assert!(report.errors.is_empty(), "write {nth}: {:?}", report.errors);
            assert_eq!(report.sources, 2, "write {nth}: {report:?}");
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            assert_eq!(
                state["lease_until"], 0,
                "write {nth}: the pass released its lease"
            );
        }
    }

    #[test]
    fn a_lagging_read_of_the_pass_own_state_write_is_not_a_conflict() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.lag_compact_reads.set(true);
        let report = finish(&fake, TARGET);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.sources, 2);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(state["lease_until"], 0, "the pass released its lease");
    }

    /// Once the backlog drains a pass takes about a second, so its next write follows its
    /// lease write before the KV API shows the lease write; the pass waits that lag out.
    #[test]
    fn a_pass_own_state_write_lagging_for_several_reads_is_not_a_conflict() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.kv.borrow_mut().insert(
            compaction::STATE_KEY.into(),
            (
                1,
                serde_json::to_value(compaction::State::default()).unwrap(),
            ),
        );
        fake.lag_compact_reads.set(true);
        fake.lagging_compact_reads.set(5);
        let report = finish(&fake, TARGET);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.sources, 2);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(state["lease_until"], 0, "the pass released its lease");
        assert!(!fake.waits.borrow().is_empty(), "the lag is waited out");
    }

    /// The KV API can lag a pass's own write past every read-back. The write landed, so this is
    /// no conflict: the pass stops with more pending, and once its lease lapses the next pass
    /// finishes the work.
    #[test]
    fn a_pass_own_state_write_lagging_past_every_read_back_stops_with_more_pending() {
        for sources in [0, 2] {
            let fake = Fake::new(NOW);
            seed(&fake, sources);
            fake.kv.borrow_mut().insert(
                compaction::STATE_KEY.into(),
                (
                    1,
                    serde_json::to_value(compaction::State::default()).unwrap(),
                ),
            );
            fake.lag_compact_reads.set(true);
            fake.lagging_compact_reads.set(40);
            fake.new_request();
            let report = compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0)
                .unwrap_or_else(|error| panic!("{sources} sources: {error}"));
            assert_eq!(report.stopped.as_deref(), Some("state_lag"), "{report:?}");
            assert!(report.more_pending, "{sources} sources: {report:?}");
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            fake.lag_compact_reads.set(false);
            fake.lagging_compact_state.borrow_mut().take();
            fake.now.set(fake.now.get() + 71);
            let report = finish(&fake, TARGET);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(compacts(&fake).len(), usize::from(sources > 0));
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            assert_eq!(state["lease_until"], 0, "the lease was released");
        }
    }

    #[test]
    fn adaptive_policy_compacts_current_hour_and_revisits_late_inputs() {
        let fake = Fake::new(HOUR + 1800);
        seed(&fake, 2);
        assert_eq!(finish(&fake, TARGET).sources, 2);
        window(&fake, HOUR + 180, &["late-a"]);
        window(&fake, HOUR + 240, &["late-b"]);
        fake.now.set((HOUR + 3600) as u64);
        assert_eq!(
            finish(&fake, TARGET).sources,
            2,
            "late windows merge on their own"
        );
        assert_eq!(compacts(&fake).len(), 2);
        fake.now.set((HOUR + 3600) as u64 + compaction::MIN_AGE_S);
        assert_eq!(
            finish(&fake, TARGET).sources,
            2,
            "the closed hour's compacts merge"
        );
        assert_eq!(compacts(&fake).len(), 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        assert_eq!(
            column(&fake.bucket.borrow()[active.first().unwrap()], "ip").len(),
            6
        );
    }

    #[test]
    fn compaction_cpu_admission_does_not_bypass_the_first_job() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.vcpu_step.set(45);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert!(report.more_pending);
        assert_eq!(report.stopped.as_deref(), Some("vcpu_limit"));
        assert!(compacts(&fake).is_empty());
    }

    #[test]
    fn compaction_cpu_budget_excludes_vcpu_spent_before_the_pass() {
        // Live workers reach the pass with 45-60 ms of execution vCPU already spent.
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.vcpu_step.set(0);
        let mut report = Report::default();
        for _ in 0..16 {
            fake.new_request();
            fake.vcpu.set(60);
            report =
                compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap();
            if report.compacts > 0 || !report.more_pending {
                break;
            }
        }
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.compacts, 1, "stopped: {:?}", report.stopped);
        assert_eq!(compacts(&fake).len(), 1);
    }

    /// Restoring a job's checkpoint (its buffered output and footer, several MB live) is the
    /// fixed cost of resuming it. Counted against the pass's vCPU stop, it spent the whole
    /// budget live, so a job whose sources were all read never finished: every pass restored
    /// it, stopped before the upload and saved it again, and compaction stalled behind it.
    #[test]
    fn a_resumed_job_finishes_when_restoring_its_checkpoint_spends_the_vcpu_budget() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.vcpu_step.set(0);
        fake.get_vcpu.borrow_mut().extend([
            ("window=", 25),
            ("_compact_work/", compaction::VCPU_STOP_MS + 5),
        ]);
        let first = run(&fake, TARGET);
        assert_eq!(first.compacts, 0, "{first:?}");
        assert!(first.more_pending);
        assert_eq!(first.stopped.as_deref(), Some("vcpu_limit"), "{first:?}");
        let job = fake.kv.borrow()[compaction::STATE_KEY].1["job"].clone();
        assert_eq!(job["next"], 2, "{job}");
        assert!(job["checkpoint"].is_string(), "{job}");
        let mut report = Report::default();
        for _ in 0..4 {
            report = run(&fake, TARGET);
            if report.compacts > 0 {
                break;
            }
        }
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!((report.compacts, report.sources), (1, 2), "{report:?}");
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn interrupted_compaction_resumes_without_reading_checkpointed_sources() {
        let fake = Fake::new(NOW);
        // Two 6 MiB sources fill the pass's source-byte budget, so the third waits for a resume.
        let sources = (0..3)
            .map(|i| odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 6 << 20]]))
            .collect::<Vec<_>>();
        fake.vcpu_step.set(3);
        let first = run(&fake, TARGET);
        assert_eq!(first.compacts, 0);
        assert!(first.more_pending);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let completed = state["job"]["next"].as_u64().unwrap() as usize;
        assert!((1..sources.len()).contains(&completed));
        assert!(state["job"]["checkpoint"].is_string());
        let checkpoint = state["job"]["checkpoint"].as_str().unwrap();
        assert!(fake.bucket.borrow()[checkpoint].len() > 2 * 1024 * 1024);
        assert_eq!(fake.uploads.borrow().len(), 1);
        fake.vcpu_step.set(0);
        fake.cdn_gets.borrow_mut().clear();
        let next = run(&fake, TARGET);
        assert!(next.errors.is_empty(), "{:?}", next.errors);
        assert_eq!(next.compacts, 1);
        for key in &sources[..completed] {
            assert!(
                !fake.cdn_gets.borrow().contains(key),
                "checkpointed source was reread: {key}"
            );
        }
        let key = &compacts(&fake)[0];
        let batches = batches(&fake.bucket.borrow()[key]);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let blobs = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            (0..3).map(|i| vec![i as u8; 6 << 20]).collect::<Vec<_>>()
        );
        assert!(fake.uploads.borrow().is_empty());
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn missing_work_checkpoint_aborts_explicitly_without_publishing_or_deleting_sources() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 4);
        fake.vcpu_step.set(3);
        assert!(run(&fake, TARGET).more_pending);
        let key = fake.kv.borrow()[compaction::STATE_KEY].1["job"]["checkpoint"]
            .as_str()
            .unwrap()
            .to_owned();
        fake.bucket.borrow_mut().remove(&key);
        fake.vcpu_step.set(0);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert!(report
            .errors
            .iter()
            .any(|e| e == "compact_checkpoint_missing"));
        assert!(fake.uploads.borrow().is_empty());
        assert!(compacts(&fake).is_empty());
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
        assert_eq!(manifest_gen(&fake), 0);
    }

    #[test]
    fn a_missing_upload_restarts_from_committed_sources_without_losing_rows() {
        for final_part in [false, true] {
            let fake = Fake::new(NOW);
            let sources = if final_part {
                seed(&fake, 3)
            } else {
                (0..3)
                    .map(|i| odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 6 << 20]]))
                    .collect::<Vec<_>>()
            };
            if final_part {
                fake.transient.borrow_mut().push_back(("PART", 503, None));
                let mut failure = None;
                for _ in 0..8 {
                    fake.new_request();
                    match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                        Ok(report) => assert!(report.more_pending),
                    }
                }
                assert_eq!(failure.as_deref(), Some("multipart_part_http_503"));
            } else {
                fake.vcpu_step.set(3);
                assert!(run(&fake, TARGET).more_pending);
            }
            let job = fake.kv.borrow()[compaction::STATE_KEY].1["job"].clone();
            assert_eq!(job["next"].as_u64() == Some(3), final_part);
            let checkpoint = job["checkpoint"].as_str().unwrap().to_owned();
            fake.uploads.borrow_mut().clear();
            fake.now.set(fake.now.get() + 70);
            fake.vcpu_step.set(0);
            let report = run(&fake, TARGET);
            assert_eq!(report.compacts, 0);
            assert!(report.more_pending);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(manifest_gen(&fake), 0);
            assert!(compacts(&fake).is_empty());
            assert!(sources
                .iter()
                .all(|key| fake.bucket.borrow().contains_key(key)));
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            assert!(state["job"].is_null());
            assert_eq!(state["skip"], json!([]));
            assert!(fake.calls.get() <= 32);

            assert_eq!(finish(&fake, TARGET).rows, if final_part { 6 } else { 3 });
            let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
            let active = catalog_keys(&fake, &query).1;
            assert_eq!(active.len(), 1);
            let output = fake.bucket.borrow()[active.first().unwrap()].clone();
            if final_part {
                assert_eq!(column(&output, "ip"), ["a0", "b0", "a1", "b1", "a2", "b2"]);
            } else {
                let values = batches(&output)
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .iter()
                            .map(|value| value.unwrap().to_vec())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(values, (0..3).map(|i| vec![i; 6 << 20]).collect::<Vec<_>>());
            }
            run(&fake, TARGET);
            assert!(!fake.bucket.borrow().contains_key(&checkpoint));
            assert!(fake.uploads.borrow().is_empty());
        }
    }

    #[test]
    fn an_unclassified_part_404_keeps_the_checkpoint_and_fails_explicitly() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        fake.transient.borrow_mut().push_back(("PART", 404, None));
        let mut failure = None;
        for _ in 0..8 {
            fake.new_request();
            match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                Err(error) => {
                    failure = Some(error);
                    break;
                }
                Ok(report) => assert!(report.more_pending),
            }
        }
        assert_eq!(failure.as_deref(), Some("multipart_part_http_404"));
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"]["checkpoint"].is_string());
        assert_eq!(fake.uploads.borrow().len(), 1);
        assert_eq!(manifest_gen(&fake), 0);
    }

    #[test]
    fn a_missing_range_copy_upload_does_not_skip_the_valid_source() {
        let fake = Fake::new(NOW);
        odd_window(&fake, HOUR, &[vec![1; 10 << 20]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 << 20]]);
        assert_eq!(finish(&fake, TARGET).rows, 2);
        let source = compacts(&fake)[0].clone();
        odd_window(&fake, HOUR + 120, &[vec![3; 10 << 20]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        fake.vcpu_step.set(3);
        let mut interrupted = false;
        for _ in 0..16 {
            run(&fake, TARGET);
            let job = fake.kv.borrow()[compaction::STATE_KEY].1["job"].clone();
            if job["copying"]["footer"].is_string() {
                interrupted = true;
                break;
            }
        }
        assert!(interrupted);
        fake.uploads.borrow_mut().clear();
        fake.vcpu_step.set(0);
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert!(state["job"].is_null());
        assert_eq!(state["skip"], json!([]));
        assert!(fake.bucket.borrow().contains_key(&source));
        assert_eq!(finish(&fake, TARGET).rows, 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let output = fake.bucket.borrow()[active.first().unwrap()].clone();
        let blobs = batches(&output)
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            vec![vec![1; 10 << 20], vec![2; 10 << 20], vec![3; 10 << 20]]
        );
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn compaction_heap_admission_leaves_sources_unchanged() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 4);
        fake.heap_mib.set(Some(96));
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert_eq!(report.compacts, 0);
        assert!(fake.cdn_gets.borrow().is_empty());
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
    }

    #[test]
    fn larger_compacts_merge_again_with_bounded_range_reads() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        odd_window(&fake, HOUR, &[vec![1; 10 * 1024 * 1024]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 * 1024 * 1024]]);
        assert_eq!(finish(&fake, TARGET).rows, 2);
        let first = compacts(&fake)[0].clone();
        assert!(fake.bucket.borrow()[&first].len() > compaction::MAX_SOURCE_BYTES as usize);
        odd_window(&fake, HOUR + 120, &[vec![3; 10 * 1024 * 1024]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        assert_eq!(finish(&fake, TARGET).rows, 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let bytes = fake.bucket.borrow()[active.first().unwrap()].clone();
        assert_eq!(
            batches(&bytes)
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            3
        );
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn an_open_hours_compacts_merge_together_once_they_pile_up() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        // `NOW` is on the hour: this hour is still open for the whole test.
        let hour = NOW;
        let mut second = 0;
        for round in 0..compaction::COMPACT_TRIGGER {
            for _ in 0..compaction::FILE_TRIGGER {
                odd_window(&fake, hour + second, &[vec![round as u8; 16]]);
                second += 1;
            }
            fake.now.set((hour + second + 60) as u64);
            assert!(
                finish(&fake, TARGET).compacts >= 1,
                "round {round} merges its windows"
            );
        }
        let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1.len(),
            compaction::COMPACT_TRIGGER
        );
        assert!(
            ((fake.now.get() as i64) + compaction::MIN_AGE_S as i64)
                < hour + 3600 + compaction::MIN_AGE_S as i64,
            "hour still open"
        );
        let report = finish(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert_eq!(
            report.rows,
            (compaction::COMPACT_TRIGGER * compaction::FILE_TRIGGER) as u64
        );
        assert_eq!(catalog_keys(&fake, &query).1.len(), 1);
    }

    #[test]
    fn a_failed_range_copy_part_keeps_every_unacknowledged_byte_for_retry() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        odd_window(&fake, HOUR, &[vec![1; 10 << 20]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 << 20]]);
        finish(&fake, TARGET);
        odd_window(&fake, HOUR + 120, &[vec![3; 10 << 20]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        fake.transient.borrow_mut().push_back(("PART", 503, None));
        let report = finish(&fake, TARGET);
        assert_eq!(report.rows, 3);
        assert!(fake.transient.borrow().is_empty());
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let actual = batches(&fake.bucket.borrow()[active.first().unwrap()]);
        let blobs = actual
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            vec![vec![1; 10 << 20], vec![2; 10 << 20], vec![3; 10 << 20]]
        );
    }

    #[test]
    fn a_128_mib_target_admits_a_job_larger_than_one_execution_and_checkpoints_it() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        for i in 0..9 {
            odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 15 * 1024 * 1024]]);
        }
        let target = 128 << 20;
        let mut planned_large = false;
        let mut published = false;
        let mut executions = 0;
        for _ in 0..64 {
            let report = run(&fake, target);
            executions += 1;
            assert!(fake.calls.get() <= 32);
            assert!(fake.source_bytes.get() <= compaction::MAX_EXECUTION_SOURCE_BYTES);
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            if let Some(sources) = state["job"]["sources"].as_array() {
                let bytes = sources
                    .iter()
                    .map(|source| source["size"].as_u64().unwrap())
                    .sum::<u64>();
                assert!(bytes <= target);
                planned_large |= bytes > 100 << 20;
            }
            if report.compacts > 0 {
                assert!(report.bytes_written > 100 << 20);
                published = true;
                break;
            }
        }
        assert!(
            planned_large,
            "job admission must not charge all future reads to this execution"
        );
        assert!(published);
        assert!(executions > 1);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 2);
        let rows = active
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(rows, 9);
    }

    #[test]
    fn a_crash_after_catalog_cas_recovers_publication_and_original_cleanup_grace() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let sources = seed(&fake, 5);
        fake.fail_compact_after_publish.set(true);
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                Err(error) if error == "injected_post_publication_failure" => {
                    failed = true;
                    break;
                }
                Ok(_) => {}
                Err(error) => panic!("unexpected failure: {error}"),
            }
        }
        assert!(failed);
        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"].clone();
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let committed = catalog_keys(&fake, &query).1;
        assert_eq!(committed.len(), 1);
        fake.fail_compact_after_publish.set(false);
        fake.now.set((NOW as u64) + 120);
        let recovered = run(&fake, TARGET);
        assert!(recovered.errors.is_empty());
        assert_eq!(
            fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"],
            generation
        );
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null());
        assert_eq!(
            fake.kv.borrow()[compaction::STATE_KEY].1["queue"][0]["at"],
            NOW
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S - 1);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|key| !fake.bucket.borrow().contains_key(key)));
        assert_eq!(catalog_keys(&fake, &query).1, committed);
    }

    #[test]
    fn retention_during_a_durable_job_abandons_obsolete_publication_and_resumes() {
        let fake = Fake::new(NOW);
        seed(&fake, 5);
        fake.vcpu_step.set(3);
        assert!(run(&fake, TARGET).more_pending);
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_object());
        fake.vcpu_step.set(0);
        fake.now.set((NOW + 30 * 86_400) as u64);
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.new_request();
        ingest::catalog::retire(&store(&fake, &svc)).unwrap();
        let mut abandoned = false;
        for _ in 0..32 {
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= 32);
            abandoned |= report
                .errors
                .iter()
                .any(|error| error == "catalog_replacement_not_active");
            if fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null() {
                break;
            }
        }
        assert!(abandoned);
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null());
        let now = fake.now.get() as i64;
        window(&fake, now - 2400, &["new-a"]);
        window(&fake, now - 2340, &["new-b"]);
        assert_eq!(finish(&fake, TARGET).sources, 2);
    }

    #[test]
    fn stale_retry_after_intervening_compaction_never_deletes_live_committed_compact() {
        let fake = Fake::new(NOW);
        let _sources = seed(&fake, 5);
        fake.fail_compact_after_publish.set(true);
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                Err(error) if error == "injected_post_publication_failure" => {
                    failed = true;
                    break;
                }
                Ok(_) => {}
                Err(error) => panic!("unexpected failure: {error}"),
            }
        }
        assert!(failed);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let committed = catalog_keys(&fake, &query).1;
        assert_eq!(committed.len(), 1);
        let committed_key = committed.iter().next().unwrap().clone();

        fake.fail_compact_after_publish.set(false);
        let mut stale_state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        fake.kv.borrow_mut().remove(compaction::STATE_KEY);

        window(&fake, HOUR + 3600, &["b1"]);
        window(&fake, HOUR + 3660, &["b2"]);
        fake.now.set((NOW + 7200) as u64);
        assert_eq!(finish(&fake, TARGET).sources, 2);

        stale_state["lease_until"] = json!(0);
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (999, stale_state));
        for step in 1..=4 {
            fake.now.set((NOW + 7200 + step * 120) as u64);
            run(&fake, TARGET);
        }
        assert!(
            fake.bucket.borrow().contains_key(&committed_key),
            "live committed compact was deleted: {committed_key}"
        );
        assert_eq!(catalog_keys(&fake, &query).1, committed);
    }

    #[test]
    fn aged_mergeable_sources_survive_catalog_page_and_execution_boundaries() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let a = window(&fake, HOUR, &["a"]);
        let b = window(&fake, HOUR + 60, &["b"]);
        let mut outputs = (0..1002)
            .map(|i| ingest::worker::Output {
                key: ingest::processed_key(ROOT, HOUR, HOUR + 60, &format!("full{i:04}"), 1),
                rows: 1,
                bytes: TARGET,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        outputs[1] = ingest::worker::Output {
            key: a.clone(),
            rows: 1,
            bytes: fake.bucket.borrow()[&a].len() as u64,
            ..Default::default()
        };
        outputs[1001] = ingest::worker::Output {
            key: b.clone(),
            rows: 1,
            bytes: fake.bucket.borrow()[&b].len() as u64,
            ..Default::default()
        };
        publish_outputs(&fake, &outputs, &[], &[]);
        fake.now.set((NOW + compaction::MIN_AGE_S as i64) as u64);
        let report = finish(&fake, TARGET);
        assert_eq!((report.compacts, report.sources, report.rows), (1, 2, 2));
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn compaction_paginates_beyond_one_thousand_files_without_losing_coverage() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let original = window(&fake, HOUR, &["x"]);
        let bytes = fake.bucket.borrow()[&original].clone();
        let mut keys = Vec::new();
        for i in 0..1004 {
            let key = ingest::processed_key(ROOT, HOUR, HOUR + 60, &format!("legacy{i:04}"), 1);
            fake.bucket.borrow_mut().insert(key.clone(), bytes.clone());
            keys.push(key);
        }
        publish_files(&fake, &keys, &[], &[]);
        fake.now.set(fake.now.get() + compaction::MIN_AGE_S);
        let mut complete = false;
        for _ in 0..500 {
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= 32);
            if !report.more_pending {
                complete = true;
                break;
            }
        }
        assert!(
            complete,
            "the paginated inventory must make durable progress: compacts={} state={}",
            compacts(&fake).len(), fake.kv.borrow().get(compaction::STATE_KEY)
                .map(|r| json!({"job":r.1["job"]["next"],"skip":r.1["skip"].as_array().map(Vec::len),
                    "cursor":r.1["catalog_cursor"],"objects":r.1["catalog_objects"].as_array().map(Vec::len)}))
                .unwrap_or_default()
        );
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(
            active.len() <= 1005usize.div_ceil(compaction::MAX_SOURCES) + 1,
            "{} files remain after bounded-footer compaction",
            active.len()
        );
        let windows = active
            .iter()
            .filter(|key| ingest::parse_processed(key).is_some())
            .collect::<Vec<_>>();
        assert!(
            windows.len() <= 1,
            "remaining={windows:?}, skip={}",
            fake.kv.borrow()[compaction::STATE_KEY].1["skip"]
        );
        let rows = active
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|batch| batch.num_rows() as u64)
                    .sum::<u64>()
            })
            .sum::<u64>();
        assert_eq!(rows, 1005);
    }

    #[test]
    fn merges_a_closed_hours_small_windows_and_deletes_them_after_the_grace() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 5);
        let open = window(&fake, NOW - 600, &["open"]);
        let report = finish(&fake, TARGET);
        assert_eq!((report.compacts, report.sources, report.rows), (1, 5, 10));
        let made = compacts(&fake);
        assert_eq!(made.len(), 1);
        assert!(made[0].starts_with(&format!("{ROOT}{}catalog1/", ingest::hour_dir(HOUR))));
        let name = fastly_log_analytics::telemetry::compact::parse_compact_name(&made[0]).unwrap();
        assert_eq!(
            (name.a, name.b, name.sources, name.rows),
            (HOUR as u64, (HOUR + 300) as u64, 5, 10)
        );
        let mut ips = column(&fake.bucket.borrow()[&made[0]], "ip");
        ips.sort();
        let mut expected: Vec<String> = (0..5)
            .flat_map(|i| [format!("a{i}"), format!("b{i}")])
            .collect();
        expected.sort();
        assert_eq!(ips, expected);
        assert_eq!(
            manifest_gen(&fake),
            1,
            "readers' hour caches are invalidated"
        );
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(NOW));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([made[0].clone(), open.clone()])
        );

        fake.now.set((NOW as u64) + DELETE_GRACE_S - 1);
        run(&fake, TARGET);
        assert!(
            sources.iter().all(|k| fake.bucket.borrow().contains_key(k)),
            "inside the grace"
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        let report = run(&fake, TARGET);
        assert_eq!(report.deleted, 5);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
        assert!(fake.bucket.borrow().contains_key(&open));
        assert!(fake.bucket.borrow().contains_key(&made[0]));
        assert_eq!(
            fake.kv.borrow()[compaction::STATE_KEY].1["queue"],
            json!([])
        );
    }

    mod committed_catalog {
        use super::*;
        use fastly_log_analytics::ingest::catalog;

        fn publish(fake: &Fake, batch: &str) -> Value {
            fake.new_request();
            let service = service();
            let previous = fake
                .admin_get(ingest::WATERMARK_KEY)
                .unwrap()
                .map(|r| r.value)
                .unwrap_or_else(|| json!({}));
            let value = catalog::prepare(
                &store(fake, &service),
                &previous,
                batch,
                "schema-v1",
                &[],
                &[],
                &[],
                &[],
            )
            .unwrap();
            let generation = fake
                .kv
                .borrow()
                .get(ingest::WATERMARK_KEY)
                .map(|r| r.0.to_string());
            assert!(fake
                .admin_cas(ingest::WATERMARK_KEY, generation.as_deref(), &value)
                .unwrap());
            value
        }

        #[test]
        fn finite_events_replay_only_committed_catalog_generations() {
            let fake = Fake::new(BASE);
            let service = service();
            let ready = catalog::events(&fake, &service, "").unwrap();
            assert!(ready.contains("retry: 30000\nid: "));
            assert!(ready.contains("event: ready\n"));
            let before = format!("{SID}:0");
            publish(&fake, "batch-a");
            publish(&fake, "batch-b");
            fake.new_request();
            let replay = catalog::events(&fake, &service, &format!("cursor={before}")).unwrap();
            assert!(replay.contains("event: commit\n"));
            assert!(replay.contains("\"generation\":2"));
            fake.new_request();
            let unchanged = catalog::events(&fake, &service, &format!("cursor={SID}:2")).unwrap();
            assert!(!unchanged.contains("event:"));
            assert!(unchanged.contains("retry: 30000\n"));
            fake.new_request();
            let resync = catalog::events(&fake, &service, "cursor=other-install:1").unwrap();
            assert!(resync.contains("event: resync\n"));
        }

        #[test]
        fn uncommitted_manifest_is_not_a_visible_commit_and_retry_is_immutable() {
            let fake = Fake::new(BASE);
            let service = service();
            let store = store(&fake, &service);
            let first =
                catalog::prepare(&store, &json!({}), "batch-a", "v1", &[], &[], &[], &[]).unwrap();
            let second =
                catalog::prepare(&store, &json!({}), "batch-a", "v1", &[], &[], &[], &[]).unwrap();
            assert_eq!(first, second);
            fake.new_request();
            let event = catalog::events(&fake, &service, "").unwrap();
            assert!(event.contains("\"generation\":0"));
            assert!(!event.contains("event: commit"));
        }

        #[test]
        fn events_expired_cursor_requires_resync_and_retry_tracks_logging_period() {
            let fake = Fake::new(BASE);
            for n in 0..40 {
                publish(&fake, &format!("batch-{n}"));
            }
            fake.new_request();
            let mut service = service();
            service.log_period = 3;
            let event = catalog::events(&fake, &service, &format!("cursor={SID}:0")).unwrap();
            assert!(event.contains("event: resync\n"));
            assert!(event.contains("retry: 1500\n"));
            service.log_period = 1;
            fake.new_request();
            assert!(catalog::events(&fake, &service, "")
                .unwrap()
                .contains("retry: 1000\n"));
            service.log_period = 120;
            fake.new_request();
            assert!(catalog::events(&fake, &service, "")
                .unwrap()
                .contains("retry: 30000\n"));
        }

        #[test]
        fn a_transiently_stale_head_never_regresses_an_acknowledged_generation() {
            let fake = Fake::new(BASE);
            publish(&fake, "batch-a");
            fake.new_request();
            let event = catalog::events(&fake, &service(), &format!("cursor={SID}:2")).unwrap();
            assert!(!event.contains("event:"));
            assert!(event.contains("catalog head behind cursor"));
        }

        #[test]
        fn actual_ingestion_preserves_the_ordered_catalog_across_watermark_commits() {
            let fake = Fake::new(BASE + 600);
            put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
            tick(&fake).unwrap();
            let first = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"].clone();
            assert_eq!(first["generation"], 1);
            put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
            tick_later(&fake).unwrap();
            let next = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"].clone();
            assert_eq!(next["generation"], 2);
            let manifest: catalog::Manifest =
                serde_json::from_slice(&fake.bucket.borrow()[next["manifest"].as_str().unwrap()])
                    .unwrap();
            assert_eq!(manifest.previous.as_deref(), first["manifest"].as_str());
            assert_eq!(manifest.rows, 1);
            assert_eq!(manifest.files.len(), 1);
            assert_eq!(manifest.sources.len(), 1);
            assert!(!manifest.files[0].md5.is_empty());
            assert!(fake.bucket.borrow().contains_key(&manifest.files[0].key));
            let metadata = manifest.files[0]
                .metadata
                .as_ref()
                .expect("standard files retain usable metadata");
            assert!(!metadata.row_groups.is_empty());
            for column in metadata.row_groups.iter().flat_map(|group| &group.columns) {
                assert!(column.offset + column.length < manifest.files[0].bytes);
            }
        }
    }

    #[test]
    fn is_idempotent() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        finish(&fake, TARGET);
        let after_first = fake.keys(ROOT);
        let generation = manifest_gen(&fake);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert_eq!(fake.keys(ROOT), after_first);
        assert_eq!(manifest_gen(&fake), generation);
    }

    #[test]
    fn keeps_windows_at_or_above_the_target_and_groups_up_to_it() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 6);
        let size = fake.bucket.borrow()[&sources[0]].len() as u64;
        // Room for two windows per compact; a window at the target is never merged.
        let big = odd_window(&fake, HOUR + 3000, &[vec![0; (2 * size + 1) as usize]]);
        for _ in 0..64 {
            if !run(&fake, 2 * size + 1).more_pending {
                break;
            }
        }
        // Merged windows rewrite smaller than their sources, so compacts can merge again;
        // every active compact still stays within the target.
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(active.contains(&big));
        let made: Vec<&String> = active.iter().filter(|k| k.contains("/compact=")).collect();
        assert_eq!(active.len(), made.len() + 1);
        assert!((1..6).contains(&made.len()), "{made:?}");
        let mut ips = Vec::new();
        for key in made {
            assert!(fake.bucket.borrow()[key].len() as u64 <= 2 * size + 1);
            ips.extend(column(&fake.bucket.borrow()[key], "ip"));
        }
        assert_eq!(ips.len(), 12);
    }

    #[test]
    fn a_crash_between_the_upload_and_the_queue_is_repaired_by_the_next_run() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 3);
        fake.fail_watermark.set(true);
        let service = service();
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            if compaction::request_compact(&store(&fake, &service), "ck", TARGET, 0).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert_eq!(compacts(&fake).len(), 1);
        assert_eq!(manifest_gen(&fake), 0);
        fake.fail_watermark.set(false);
        fake.now.set(fake.now.get() + 70);
        let report = run(&fake, TARGET);
        assert_eq!(
            report.compacts, 1,
            "the completed upload is recovered without copying sources"
        );
        assert_eq!(compacts(&fake).len(), 1);
        assert_eq!(manifest_gen(&fake), 1);
        fake.now.set(fake.now.get() + DELETE_GRACE_S);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn watermark_conflict_after_merge_defers_saves_completed_job_and_releases_lease_immediately() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        fake.conflict_watermark.set(true);
        let service = service();
        let mut deferred = None;
        for _ in 0..32 {
            fake.new_request();
            let report =
                compaction::request_compact(&store(&fake, &service), "ck", TARGET, 0).unwrap();
            if report.stopped.as_deref() == Some("watermark_conflict") {
                deferred = Some(report);
                break;
            }
        }
        // Ingest publishes to the same watermark many times a second; losing the CAS race is
        // contention the next pass resolves, not a failure.
        let deferred = deferred.expect("a pass deferred on the watermark conflict");
        assert!(deferred.more_pending);
        assert!(deferred.errors.is_empty());
        assert_eq!(deferred.compacts, 0);
        assert_eq!(deferred.sources, 0);
        let raw_state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let saved: compaction::State = serde_json::from_value(raw_state.clone()).unwrap();
        assert_eq!(saved.lease_until, 0, "lease released on watermark conflict");
        assert_eq!(
            raw_state["job"]["complete"],
            json!(true),
            "completed merge job preserved in state"
        );
        // Immediate retry without advancing clock or re-reading source windows commits the compact.
        fake.conflict_watermark.set(false);
        fake.cdn_gets.borrow_mut().clear();
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert!(
            fake.cdn_gets.borrow().is_empty(),
            "no source re-reads needed after checkpointed watermark conflict"
        );
        assert_eq!(manifest_gen(&fake), 1);
    }

    #[test]
    fn a_window_arriving_after_its_hours_compact_stays_visible() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        finish(&fake, TARGET);
        let late = window(&fake, HOUR + 1200, &["late"]);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([compacts(&fake)[0].clone(), late])
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        finish(&fake, TARGET);
        let active = catalog_keys(&fake, &query).1;
        let ips = active
            .iter()
            .flat_map(|key| column(&fake.bucket.borrow()[key], "ip"))
            .collect::<Vec<_>>();
        assert_eq!(ips.len(), 7);
        assert_eq!(ips.iter().filter(|ip| *ip == "late").count(), 1);
    }

    #[test]
    fn a_schema_mismatch_leaves_the_odd_window_uncompacted() {
        let fake = Fake::new(NOW);
        let first = seed(&fake, 2);
        let odd = odd_window(&fake, HOUR + 150, &[vec![1, 2, 3]]);
        let rest: Vec<String> = (3..5)
            .map(|i| window(&fake, HOUR + i * 60, &[&format!("c{i}")]))
            .collect();
        for _ in 0..64 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        let mut covered = first.clone();
        covered.extend(rest);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(active.contains(&odd));
        assert!(covered.iter().all(|key| !active.contains(key)));
        assert_eq!(
            active
                .iter()
                .filter(|key| ingest::parse_processed(key).is_some())
                .cloned()
                .collect::<Vec<_>>(),
            vec![odd]
        );
        let rows: usize = compacts(&fake)
            .iter()
            .map(|k| column(&fake.bucket.borrow()[k], "ip").len())
            .sum();
        assert_eq!(rows, 6);
    }

    #[test]
    fn streams_a_large_compact_as_multipart_parts() {
        let fake = Fake::new(NOW);
        let blob = |seed: u8| -> Vec<u8> {
            (0..3 * 1024 * 1024u32)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed)
                .collect()
        };
        let sources: Vec<String> = (0..4)
            .map(|i| odd_window(&fake, HOUR + i * 60, &[blob(i as u8)]))
            .collect();
        let report = finish(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert_eq!(fake.upload_seq.get(), 1, "one multipart upload");
        assert!(fake.uploads.borrow().is_empty());
        let made = compacts(&fake);
        let bytes = fake.bucket.borrow()[&made[0]].clone();
        assert!(bytes.len() > 12 * 1024 * 1024);
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let mut blobs = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .clone();
            blobs.extend(column.iter().map(|v| v.unwrap().to_vec()));
        }
        assert_eq!(blobs, (0..4).map(|i| blob(i as u8)).collect::<Vec<_>>());
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([made[0].clone()])
        );
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
    }

    pub(super) fn stage_turn(fake: &Fake, start: usize, compact_only: bool) -> Value {
        let body = json!({
            "service_id": SID,
            "run_id": "run-1",
            "tick_id": "",
            "trigger": "clock",
            "actor": "system",
            "bucket": 0,
            "start": start,
            "compact_only": compact_only,
        });
        fake.new_request();
        let path = fastly_log_analytics::admin::stage::PATH.to_owned();
        fake.worker_dispatch(&[(path, body)], std::time::Duration::from_secs(30))
            .remove(0)
            .unwrap()
    }

    fn compaction_service(fake: &Fake) {
        tick_runtime(fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
    }

    /// Request compaction is its own job: a maintenance turn never runs it, whichever stage the
    /// rotation starts from, and a compact-only execution runs nothing but it.
    #[test]
    fn request_compaction_runs_on_its_own_execution_not_in_the_rotation() {
        for start in [0, 5] {
            let fake = Fake::new(NOW);
            compaction_service(&fake);
            seed(&fake, 40);
            let reply = stage_turn(&fake, start, false);
            assert_eq!(reply["ran"], "retention_sweep", "{reply}");
            assert_eq!(reply["next"], 1, "{reply}");
            assert!(compacts(&fake).is_empty(), "{reply}");
            let reply = stage_turn(&fake, start, true);
            assert_eq!(reply["ran"], "request_compact", "{reply}");
            assert!(reply.get("next").is_none(), "{reply}");
            assert!(!compacts(&fake).is_empty(), "{reply}");
            let rows = rows(&fake, LogKind::CronRuns);
            assert!(
                rows.iter().any(|row| row["cron_kind"] == "request_compact"),
                "{rows:?}"
            );
        }
    }

    /// Live KV paces writes to one key about a second apart, so the turn's passes go only as
    /// fast as their state writes. Chained passes hand the lease on, and only a pass completing
    /// its job saves the state before its final write.
    #[test]
    fn a_maintenance_turns_passes_write_the_state_about_once_each() {
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        load_windows(&fake, HOUR, 128, 50);
        let reply = stage_turn(&fake, 0, true);
        let rows = rows(&fake, LogKind::CronRuns);
        let row = rows
            .iter()
            .find(|row| row["cron_kind"] == "request_compact")
            .expect("a request_compact row");
        let details: Value = serde_json::from_str(row["details_json"].as_str().unwrap()).unwrap();
        let passes = details["passes"].as_u64().unwrap() as u32;
        let jobs = compacts(&fake).len() as u32;
        assert!(passes > 1 && jobs > 1, "{reply}");
        let writes = fake.compact_state_writes.get();
        assert!(
            writes <= passes + 2 * jobs + 1,
            "{writes} writes in {passes} passes, {jobs} jobs"
        );
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(
            state["lease_until"], 0,
            "the last pass released its lease: {state}"
        );
    }

    /// Every pass is its own worker execution, so the row's `vcpu_ms` is the admin execution's
    /// own time (what the per-execution budget applies to), and the passes' worker time sits in
    /// `details.worker_vcpu_ms` as the ingest rows report it.
    #[test]
    fn a_compaction_rows_vcpu_is_the_admin_executions_not_the_sum_of_its_passes() {
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        load_windows(&fake, HOUR, 128, 50);
        fake.vcpu_step.set(45);
        stage_turn(&fake, 0, true);
        let rows = rows(&fake, LogKind::CronRuns);
        let row = rows
            .iter()
            .find(|row| row["cron_kind"] == "request_compact")
            .expect("a request_compact row");
        let details: Value = serde_json::from_str(row["details_json"].as_str().unwrap()).unwrap();
        let passes = details["passes"].as_u64().unwrap();
        let worker = details["worker_vcpu_ms"].as_u64().unwrap();
        let own = details["stage_execution_metrics"]["vcpu_ms"]
            .as_u64()
            .unwrap();
        assert!(passes > 1, "{details}");
        assert!(worker >= passes * 45, "{details}");
        assert_eq!(row["vcpu_ms"].as_u64().unwrap(), own, "{row}");
        assert!(row["vcpu_ms"].as_u64().unwrap() < worker, "{row}");
    }

    /// The tick dispatches the compaction job beside the maintenance stage. A run that leaves a
    /// backlog brings the job's next run forward to `COMPACT_CATCHUP_S` without touching the
    /// maintenance cadence; a caught-up job waits `COMPACT_INTERVAL_S`.
    #[test]
    fn a_compaction_backlog_brings_the_jobs_next_run_forward() {
        use fastly_log_analytics::admin::schedule::{COMPACT_CATCHUP_S, COMPACT_INTERVAL_S};
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        seed(&fake, 230);
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(runs.len(), 2, "a maintenance turn and a compaction run");
        assert_ne!(runs[0]["ran"], "request_compact", "{}", runs[0]);
        assert_eq!(runs[1]["ran"], "request_compact", "{}", runs[1]);
        assert_eq!(runs[1]["compact_pending"], true, "{}", runs[1]);
        let jobs = fake.kv.borrow()["cron:state"].1["jobs"].clone();
        assert_eq!(jobs["compact"]["more_pending"], true, "{jobs}");
        assert_eq!(
            jobs["compact"]["retry_after"],
            fake.now.get() + COMPACT_CATCHUP_S
        );
        assert_eq!(jobs["maint"]["more_pending"], false, "{jobs}");
        assert_eq!(jobs["maint"]["retry_after"], 0, "{jobs}");

        fake.now.set(fake.now.get() + COMPACT_CATCHUP_S);
        let made = compacts(&fake).len();
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(runs.len(), 3, "only the compaction job came back early");
        assert_eq!(runs[2]["ran"], "request_compact", "{}", runs[2]);
        assert!(compacts(&fake).len() > made, "{}", runs[2]);

        while fake.kv.borrow()["cron:state"].1["jobs"]["compact"]["more_pending"] == true {
            fake.now.set(fake.now.get() + COMPACT_CATCHUP_S);
            run_tick(&fake);
        }
        let caught_up = fake.stage_runs.borrow().len();
        fake.now.set(fake.now.get() + COMPACT_INTERVAL_S - 1);
        run_tick(&fake);
        assert_eq!(fake.stage_runs.borrow().len(), caught_up, "not due yet");
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(
            runs.len(),
            caught_up + 1,
            "{COMPACT_INTERVAL_S} s after the last run"
        );
        assert_eq!(runs[caught_up]["status"], "ok");
        assert!(
            runs[caught_up]["ran"].is_null() || runs[caught_up]["ran"] == "request_compact",
            "{}",
            runs[caught_up]
        );
    }

    /// Maintenance rides the ingest tick's dispatch, so a plan is in flight for nearly every
    /// turn under load. The tick re-reads the watermark before its commit, so both land.
    #[test]
    fn compacts_alongside_the_dispatching_ticks_in_flight_plan() {
        let fake = Fake::new(NOW);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        let sources = seed(&fake, 3);
        let period = NOW - 180;
        put_raw(&fake, period, "fresh", &[line(period, "fresh")]);
        let stage = serde_json::to_value(fastly_log_analytics::admin::stage::StageRequest {
            service_id: SID.into(),
            run_id: "run-1".into(),
            tick_id: String::new(),
            trigger: "clock".into(),
            actor: "system".into(),
            bucket: 0,
            start: 5,
            compact_only: true,
        })
        .unwrap();
        fake.new_request();
        let buffer = LogBuffer::new(SID, "req");
        let options = ingest::coordinator::Options {
            extra: vec![(fastly_log_analytics::admin::stage::PATH.into(), stage)],
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
        assert_eq!(report.converted, 1, "{report:?}");
        assert!(!report.busy);
        let made = compacts(&fake);
        assert_eq!(made.len(), 1, "{:?}", report.extra_results);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(NOW));
        let (_, catalog) = catalog_keys(&fake, &query);
        assert!(catalog.contains(&made[0]));
        assert!(sources.iter().all(|key| !catalog.contains(key)));
        assert!(
            catalog
                .iter()
                .any(|key| !key.contains("/compact=") && !sources.contains(key)),
            "{catalog:?}"
        );
    }

    /// A window file with enough wide string columns that a few of them fill a compact's footer.
    fn wide_window(fake: &Fake, start: i64) -> String {
        let names = (0..800).map(|c| format!("c{c:03}")).collect::<Vec<_>>();
        let schema = Arc::new(arrow_schema::Schema::new(
            names
                .iter()
                .map(|name| arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, false))
                .chain([arrow_schema::Field::new(
                    "blob",
                    arrow_schema::DataType::Binary,
                    false,
                )])
                .collect::<Vec<_>>(),
        ));
        // A binary column is outside the window writer's types, so these one-row files are
        // copied as they are and each keeps its own footer entry.
        let columns = names
            .iter()
            .map(|name| {
                Arc::new(arrow_array::StringArray::from(vec![format!(
                    "{start}-{name}-{}",
                    "x".repeat(96)
                )])) as arrow_array::ArrayRef
            })
            .chain([Arc::new(BinaryArray::from_iter_values([b"x"])) as arrow_array::ArrayRef])
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let key = ingest::processed_key(ROOT, start, start + 60, "abcdef012345", 1);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    #[test]
    fn a_full_compact_footer_regroups_without_an_error_and_is_remembered() {
        let fake = Fake::new(NOW);
        let sources = (0..2)
            .flat_map(|h| (0..8).map(move |i| HOUR + h * 3600 + i * 60))
            .map(|start| wide_window(&fake, start))
            .collect::<Vec<_>>();
        let mut regroups = 0;
        for _ in 0..64 {
            let report = run(&fake, TARGET);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            regroups += usize::from(report.stopped.as_deref() == Some("footer_limit"));
            if !report.more_pending {
                break;
            }
        }
        assert_eq!(
            regroups, 1,
            "the learned group size must outlive the commit"
        );
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(
            state["skip"],
            json!([]),
            "a full compact is left alone, not skipped"
        );
        let compacts = compacts(&fake);
        assert!(compacts.len() >= 4, "{compacts:?}");
        let rows = compacts
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(rows, sources.len());
    }

    #[test]
    fn a_learned_group_limit_never_leaves_a_closed_hour_in_pieces() {
        // Live: `group_limit: 33` with every compact at weight 32 left hours of four-minute
        // files that could never pair, so a 6 h window stayed at 100+ files.
        let fake = Fake::new(NOW);
        for i in 0..4 {
            window(&fake, HOUR + i * 60, &["x"]);
        }
        let settle = |fake: &Fake| {
            for _ in 0..64 {
                let report = run(fake, TARGET);
                assert!(report.errors.is_empty(), "{:?}", report.errors);
                if !report.more_pending && report.compacts == 0 {
                    break;
                }
            }
            let query = format!("from={}&to={}", iso_url(HOUR), iso_url(NOW));
            catalog_keys(fake, &query).1
        };
        fake.kv.borrow_mut().insert(
            compaction::STATE_KEY.into(),
            (1, json!({ "group_limit": 2 })),
        );
        let pieces = settle(&fake);
        assert_eq!(pieces.len(), 2, "{pieces:?}");
        assert!(pieces.iter().all(|key| key.contains("-c2-n2-")));
        {
            let mut kv = fake.kv.borrow_mut();
            let state = &mut kv.get_mut(compaction::STATE_KEY).unwrap().1;
            state["group_limit"] = json!(3);
        }
        let whole = settle(&fake);
        assert_eq!(whole.len(), 1, "{whole:?}");
        assert!(
            whole.iter().next().unwrap().contains("-c2-n4-"),
            "{whole:?}"
        );
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(state["skip"], json!([]));
        assert_eq!(
            state["group_limit"],
            json!(0),
            "a limit a pair outgrew is forgotten"
        );
    }

    #[test]
    fn a_pass_spends_its_spare_calls_reading_sources() {
        let fake = Fake::new(NOW);
        for i in 0..32 {
            window(&fake, HOUR + i * 60, &["x"]);
        }
        let mut passes = 0;
        let report = loop {
            passes += 1;
            fake.new_request();
            fake.vcpu.set(0);
            let report =
                compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 1).unwrap();
            assert!(fake.calls.get() <= BUDGET);
            if report.compacts > 0 || passes > 16 {
                break report;
            }
        };
        assert_eq!(report.sources, 32);
        assert!(
            passes <= 3,
            "{passes} worker passes for one 32-source compact"
        );
    }

    #[test]
    fn stays_within_the_backend_call_budget() {
        let fake = Fake::new(NOW);
        for hour in 0..3 {
            for i in 0..30 {
                window(&fake, HOUR + hour * 3600 - 2 * 86_400 + i * 60, &["x"]);
            }
        }
        let mut runs = 0;
        loop {
            runs += 1;
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= BUDGET);
            if !report.more_pending || runs > 100 {
                break;
            }
        }
        assert!(runs <= 100);
        let query = format!("from={}&to={}", iso_url(HOUR - 2 * 86_400), iso_url(HOUR));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 3);
        assert!(active.iter().all(|key| key.contains("/compact=")));
        assert_eq!(
            active
                .iter()
                .map(|key| column(&fake.bucket.borrow()[key], "ip").len())
                .sum::<usize>(),
            90
        );
    }

    #[test]
    fn large_multi_hour_catalog_reserves_budget_to_release_lease_and_compact() {
        let fake = Fake::new(NOW);
        // 30 distinct hours with 2 windows each creates >30 catalog blocks, which previously
        // caused `catalog::discovery::page` to exhaust the 32-call budget inside `Run::scan`,
        // fail the final state write with `call_budget`, and strand a 70 s lease with 0 compacts.
        for hour in 0..30 {
            for i in 0..2 {
                window(&fake, HOUR + hour * 3600 - 3 * 86_400 + i * 60, &["x"]);
            }
        }
        let mut compacted_total = 0u32;
        for _ in 0..40 {
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= BUDGET);
            assert_ne!(
                report.stopped.as_deref(),
                Some("lease_held"),
                "pass must never strand its 70s lease on call_budget exhaustion"
            );
            let state: compaction::State =
                serde_json::from_value(fake.kv.borrow()[compaction::STATE_KEY].1.clone()).unwrap();
            assert_eq!(
                state.lease_until, 0,
                "standalone pass must always keep enough call budget to release its lease"
            );
            compacted_total += report.compacts;
            if !report.more_pending {
                break;
            }
        }
        assert!(
            compacted_total >= 1,
            "multi-hour catalog must make compaction progress instead of livelocking"
        );
    }

    #[test]
    fn clean_closed_hours_and_days_skip_block_walks_until_late_commit() {
        let fake = Fake::new(NOW);
        let old_day_base = HOUR - 3 * 86_400;
        for h in 0..12 {
            window(&fake, old_day_base + h * 3600, &["x"]);
        }
        for _ in 0..20 {
            let report = run(&fake, TARGET);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            if !report.more_pending {
                break;
            }
        }
        let state: compaction::State =
            serde_json::from_value(fake.kv.borrow()[compaction::STATE_KEY].1.clone()).unwrap();
        assert!(
            !state.clean_hours.is_empty() || !state.clean_days.is_empty(),
            "settled closed hours must be recorded as clean"
        );

        // Next scan sees all 12 closed hours clean (and promotes closed days to clean_days),
        // then subsequent idle scans skip them without fetching any catalog block.
        let promote = run(&fake, TARGET);
        assert!(promote.errors.is_empty(), "{:?}", promote.errors);
        assert!(!promote.more_pending);
        let state: compaction::State =
            serde_json::from_value(fake.kv.borrow()[compaction::STATE_KEY].1.clone()).unwrap();
        assert!(
            !state.clean_days.is_empty(),
            "a closed day whose hours are all clean must be cached in clean_days"
        );

        let idle = run(&fake, TARGET);
        assert!(idle.errors.is_empty(), "{:?}", idle.errors);
        assert!(!idle.more_pending);
        assert_eq!(idle.compacts, 0);
        assert!(
            fake.calls.get() <= 10,
            "clean closed days must be skipped without walking blocks, used {} calls",
            fake.calls.get()
        );

        // A late-arriving window into one of those cached closed hours bumps that hour's
        // generation and day index key, so compaction automatically re-enters and merges it.
        window(&fake, old_day_base + 120, &["late"]);
        let mut compacted = 0u32;
        for _ in 0..20 {
            let report = run(&fake, TARGET);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            compacted += report.compacts;
            if !report.more_pending && report.compacts == 0 {
                break;
            }
        }
        assert_eq!(
            compacted, 1,
            "late commit into a clean closed hour must invalidate its cache entry and compact"
        );
    }

    fn ticks(fake: &Fake, enabled: bool) -> Vec<String> {
        tick_runtime(fake);
        if enabled {
            let mut svc = serde_json::to_value(service()).unwrap();
            svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
            fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        }
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": NOW, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(fake);
        }
        rows(fake, LogKind::CronRuns)
            .iter()
            .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn compaction_peer_accepts_a_serialized_service_with_a_nonempty_install_prefix() {
        let fake = Fake::new(NOW);
        let mut svc = service();
        svc.fos_prefix = "campaign/capacity".into();
        svc.compaction =
            serde_json::from_value(json!({"mode": "size_target", "size_target_mb": 64})).unwrap();
        assert!(fastly_log_analytics::admin::valid_prefix(&svc.fos_prefix));
        let request = fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: ingest::worker::PATH.into(),
            headers: [("x-flp-worker-key".into(), "fos-secret".into())].into(),
            body: json!({"maintenance": "request_compact", "service": svc})
                .to_string()
                .into_bytes(),
        };
        let response = ingest::worker::handle_worker_ingest(&fake, &request);
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let reply: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(reply["status"], "ok");
        assert!(reply["execution_metrics"]["calls"].as_u64().unwrap() > 0);
    }

    #[test]
    fn compaction_peer_rejects_traversal_and_ambiguous_install_prefixes() {
        for prefix in [
            "../campaign",
            "/campaign",
            "campaign//capacity",
            "campaign/./capacity",
        ] {
            let fake = Fake::new(NOW);
            let mut svc = service();
            svc.fos_prefix = prefix.into();
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".into(), "fos-secret".into())].into(),
                body: json!({"maintenance": "request_compact", "service": svc})
                    .to_string()
                    .into_bytes(),
            };
            assert_eq!(
                ingest::worker::handle_worker_ingest(&fake, &request).status,
                400,
                "{prefix}"
            );
            assert!(
                fake.kv_reads.borrow().is_empty(),
                "invalid scopes cannot touch storage"
            );
        }
    }

    #[test]
    fn an_idle_tick_runs_request_compaction_when_the_service_enables_it() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        let kinds = ticks(&fake, true);
        assert!(kinds.contains(&"request_compact".to_owned()), "{kinds:?}");
        assert_eq!(
            compacts(&fake).len(),
            1,
            "{:?}; {:?}",
            rows(&fake, LogKind::CronRuns)
                .iter()
                .filter(|row| row["cron_kind"] == "request_compact")
                .collect::<Vec<_>>(),
            fake.kv.borrow().get(compaction::STATE_KEY)
        );
        let records = rows(&fake, LogKind::CronRuns);
        let details: Value = records
            .iter()
            .find_map(|row| {
                let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
                details["worker_executions_metrics"]
                    .as_array()?
                    .iter()
                    .any(|sample| sample["kind"] == "request_compact")
                    .then_some(details)
            })
            .unwrap();
        let samples = details["worker_executions_metrics"].as_array().unwrap();
        assert_eq!(
            samples.len() as u64,
            details["workers_dispatched"].as_u64().unwrap()
        );
        assert_eq!(details["workers_answered"], details["workers_dispatched"]);
        let child = samples
            .iter()
            .find(|sample| sample["kind"] == "request_compact")
            .unwrap();
        assert_eq!(child["status"], "ok");
        assert!(child["calls"].as_u64().unwrap() > 0);
        assert!(samples.iter().any(|sample| sample["kind"] == "maintenance"));
    }

    #[test]
    fn one_maintenance_turn_chains_compaction_passes_until_a_compact_commits() {
        let fake = Fake::new(NOW);
        seed(&fake, 32);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": NOW, "complete": true})),
        );
        let first = (0..16)
            .find_map(|_| {
                fake.now.set(fake.now.get() + 120);
                run_tick(&fake);
                rows(&fake, LogKind::CronRuns)
                    .into_iter()
                    .find(|row| row["cron_kind"] == "request_compact")
            })
            .expect("a request_compact turn");
        assert_eq!(first["parquet_files_written"], 1, "{first}");
        assert_eq!(first["listed_files"], 32, "{first}");
        assert_eq!(compacts(&fake).len(), 1);
        let details: Value = serde_json::from_str(first["details_json"].as_str().unwrap()).unwrap();
        assert!(details["passes"].as_u64().unwrap() > 1, "{details}");
    }

    #[test]
    fn a_compaction_turn_starts_no_further_pass_without_wall_time_for_one() {
        let fake = Fake::new(NOW);
        seed(&fake, 32);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        let turn = |fake: &Fake| {
            fake.new_request();
            let body = json!({"service_id": SID, "run_id": "run1", "tick_id": "", "trigger": "clock",
                "actor": "clock", "bucket": 0, "start": 5, "compact_only": true});
            let path = fastly_log_analytics::admin::stage::PATH.to_owned();
            let reply = fake.worker_dispatch(&[(path, body)], std::time::Duration::from_secs(32));
            assert_eq!(reply[0].as_ref().unwrap()["status"], "ok");
        };
        let wall = ingest::storage::WALL_STOP.as_secs();
        fake.elapsed_s.set(wall - 4);
        turn(&fake);
        assert_eq!(fake.worker_runs.get(), 1);
        assert!(compacts(&fake).is_empty());
        fake.elapsed_s.set(0);
        turn(&fake);
        assert!(fake.worker_runs.get() > 2);
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn compaction_off_by_default_costs_nothing() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        let kinds = ticks(&fake, false);
        assert!(!kinds.contains(&"request_compact".to_owned()), "{kinds:?}");
        assert!(compacts(&fake).is_empty());
        assert!(!fake
            .kv_reads
            .borrow()
            .iter()
            .any(|k| k == compaction::STATE_KEY));
    }

    #[test]
    fn request_compaction_respects_cpu_wall_and_call_budgets_without_changing_other_stages() {
        assert_eq!(ingest::bundle::VCPU_STOP_MS, 600);
        assert_eq!(ingest::bundle::WRITE_VCPU_STOP_MS, 630);
        assert_eq!(compaction::VCPU_STOP_MS, 40);
        assert_eq!(fastly_log_analytics::telemetry::compact::VCPU_STOP_MS, 700);
        assert_eq!(
            fastly_log_analytics::telemetry::flush::FLUSH_VCPU_SOFT_MS,
            700
        );
        assert_eq!(ingest::coordinator::VCPU_STOP_MS, 800);
        assert_eq!(ingest::retention::VCPU_STOP_MS, 700);

        let fake = Fake::new(NOW + 4 * 3600);
        for h in 0..3 {
            let start = HOUR + h * 3600;
            window(&fake, start + 60, &["198.51.100.1"]);
            window(&fake, start + 120, &["198.51.100.2"]);
        }

        // Wall-clock gate (40s < 60s runtime limit) halts work before starting and resumes once
        // elapsed time resets for the next execution.
        fake.elapsed_s.set(40);
        let wall_blocked = run(&fake, TARGET);
        assert_eq!(wall_blocked.compacts, 0);
        assert!(wall_blocked.more_pending);
        assert!(fake.calls.get() <= BUDGET);

        fake.elapsed_s.set(0);
        fake.vcpu_step.set(750);
        let first = run(&fake, TARGET);
        assert_eq!(
            first.compacts, 0,
            "CPU admission applies before the first merge"
        );
        assert!(
            first.more_pending,
            "remaining hours stay pending for next run"
        );
        assert!(
            fake.calls.get() <= BUDGET,
            "{} calls > 32",
            fake.calls.get()
        );

        fake.vcpu_step.set(1);
        for _ in 0..16 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        assert_eq!(compacts(&fake).len(), 3);
        assert!(fake.calls.get() <= BUDGET);
    }
}

mod rum_compaction {
    use super::*;
    use fastly_log_analytics::ingest::compaction::{self, Dataset, Report, DELETE_GRACE_S};

    const HOUR: i64 = BASE - BASE.rem_euclid(3600);
    const NOW: i64 = HOUR + 4 * 3600;
    const TARGET: u64 = 64 << 20;
    const VITALS: &str = "client_vitals";
    const ERRORS: &str = "client_errors";
    const PLAN: &str = "abcdef012345";

    fn root(table: &str) -> String {
        format!("processed/rum/{table}/")
    }

    /// A `table` window file written by shard 0 of `plan`, one beacon row per value.
    fn window_of(fake: &Fake, table: &str, start: i64, values: &[&str], plan: &str) -> String {
        let lines: Vec<String> = values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let at = start + i as i64;
                if table == ERRORS {
                    let mut line: Value = serde_json::from_str(&rum_beacon(at, "", "")).unwrap();
                    line["rum_error_message"] = (*value).into();
                    line.to_string()
                } else {
                    rum_beacon(at, "LCP", value)
                }
            })
            .collect();
        let rows = ingest::rum::convert_object(&gz(&lines), 60, start, 1 << 20, SID).unwrap();
        let batch = if table == ERRORS {
            &rows.errors[&start]
        } else {
            &rows.windows[&start]
        };
        let bytes = ingest::convert::write_parquet(&[batch]).unwrap();
        let key = ingest::processed_key(
            &root(table),
            start,
            start + 60,
            &format!("{plan}s0"),
            values.len() as u64,
        );
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    fn window(fake: &Fake, table: &str, start: i64, values: &[&str]) -> String {
        window_of(fake, table, start, values, PLAN)
    }

    fn seed(fake: &Fake, table: &str, count: i64) -> Vec<String> {
        (0..count)
            .map(|i| window(fake, table, HOUR + i * 60, &["1", "2"]))
            .collect()
    }

    fn run(fake: &Fake, table: &'static str) -> Report {
        fake.new_request();
        fake.vcpu.set(0);
        let service = service();
        compaction::compact_pass(
            &store(fake, &service),
            "ck",
            TARGET,
            0,
            None,
            Dataset::Rum(table),
        )
        .unwrap()
    }

    /// Runs passes until one leaves nothing pending, adding up their counts.
    fn settle(fake: &Fake, table: &'static str) -> Report {
        let mut total = Report::default();
        for _ in 0..64 {
            let report = run(fake, table);
            total.compacts += report.compacts;
            total.sources += report.sources;
            total.rows += report.rows;
            total.deleted += report.deleted;
            if !report.more_pending {
                return total;
            }
        }
        panic!("RUM compaction did not settle");
    }

    fn compacts(fake: &Fake, table: &str) -> Vec<String> {
        fake.keys(&root(table))
            .into_iter()
            .filter(|k| k.contains("/compact="))
            .collect()
    }

    /// Every file `/api/files?dataset=rum` lists from `from` to `to`, with its rows.
    fn listed(fake: &Fake, from: i64, to: i64) -> BTreeMap<String, u64> {
        let query = format!("from={}&to={}&dataset=rum", iso_url(from), iso_url(to));
        let mut out = BTreeMap::new();
        let mut page = files_page(fake, &query).unwrap();
        loop {
            for file in page["files"].as_array().unwrap() {
                let key = file["key"].as_str().unwrap().to_owned();
                assert!(out.insert(key, file["rows"].as_u64().unwrap()).is_none());
            }
            if page["complete"] == true {
                return out;
            }
            let cursor = page["next_cursor"].as_str().unwrap().to_owned();
            page = files_page(fake, &format!("{query}&cursor={cursor}")).unwrap();
        }
    }

    fn total(listed: &BTreeMap<String, u64>) -> u64 {
        listed.values().sum()
    }

    fn parquet_rows(fake: &Fake, key: &str) -> i64 {
        let bytes = fake.bucket.borrow()[key].clone();
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .metadata()
            .file_metadata()
            .num_rows()
    }

    /// Closed-hour RUM windows merge into a compact in their own hour folder (RUM has no
    /// catalog), which `/api/files` lists in their place with the same rows. The windows stay
    /// readable through the grace, and the RUM watermark is never touched.
    #[test]
    fn closed_hour_rum_windows_compact_in_their_hour_folder_and_list_once() {
        let fake = Fake::new(NOW);
        let vitals = seed(&fake, VITALS, 10);
        let error = window(&fake, ERRORS, HOUR, &["boom"]);
        assert_eq!(total(&listed(&fake, HOUR, HOUR + 3600)), 21);
        let watermark = fake.kv.borrow().get(&ingest::watermark_key("rum")).cloned();
        let report = settle(&fake, VITALS);
        assert_eq!((report.compacts, report.sources, report.rows), (1, 10, 20));
        let made = compacts(&fake, VITALS);
        assert_eq!(made.len(), 1);
        let folder = format!("{}{}", root(VITALS), ingest::hour_dir(HOUR));
        assert!(
            made[0].starts_with(&format!("{folder}compact=")),
            "{made:?}"
        );
        assert_eq!(parquet_rows(&fake, &made[0]), 20);
        let after = listed(&fake, HOUR, HOUR + 3600);
        assert_eq!(
            after.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([made[0].clone(), error])
        );
        assert_eq!(total(&after), 21);
        assert!(vitals
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
        assert!(!fake.kv.borrow().contains_key(compaction::STATE_KEY));
        assert_eq!(
            fake.kv.borrow().get(&ingest::watermark_key("rum")).cloned(),
            watermark
        );
        assert!(compacts(&fake, ERRORS).is_empty());
        fake.now.set(fake.now.get() + DELETE_GRACE_S);
        assert_eq!(settle(&fake, VITALS).deleted, 10);
        assert!(vitals
            .iter()
            .all(|key| !fake.bucket.borrow().contains_key(key)));
        assert_eq!(listed(&fake, HOUR, HOUR + 3600), after);
    }

    /// Each RUM table and the request dataset have their own state record, so a lease or job
    /// held on one never stalls another.
    #[test]
    fn each_rum_table_and_requests_keep_their_own_compaction_state() {
        let fake = Fake::new(NOW);
        seed(&fake, VITALS, 4);
        seed(&fake, ERRORS, 4);
        let held = json!({"lease_until": u64::MAX});
        for key in [compaction::STATE_KEY, Dataset::Rum(VITALS).state_key()] {
            fake.kv
                .borrow_mut()
                .insert(key.to_owned(), (1, held.clone()));
        }
        assert_eq!(run(&fake, VITALS).stopped.as_deref(), Some("lease_held"));
        let report = settle(&fake, ERRORS);
        assert_eq!((report.compacts, report.sources), (1, 4));
        assert_eq!(compacts(&fake, ERRORS).len(), 1);
        assert!(compacts(&fake, VITALS).is_empty());
        for key in [compaction::STATE_KEY, Dataset::Rum(VITALS).state_key()] {
            assert_eq!(fake.kv.borrow()[key].1, held, "{key}");
        }
        assert!(fake
            .kv
            .borrow()
            .contains_key(Dataset::Rum(ERRORS).state_key()));
    }

    /// An abandoned plan's cleanup sweeps the windows its run token names, so a window of the
    /// uncommitted RUM plan, or one written within the minimum age (whose plan may not have
    /// been seen yet), never joins a compact. Aged windows of an hour still open do.
    #[test]
    fn uncommitted_and_young_rum_windows_wait_while_aged_ones_of_the_open_hour_compact() {
        let fake = Fake::new(NOW);
        for i in 0..3 {
            window(&fake, VITALS, HOUR + i * 60, &["1"]);
        }
        let uncommitted: Vec<String> = (3..6)
            .map(|i| window_of(&fake, VITALS, HOUR + i * 60, &["1"], "fedcba987654"))
            .collect();
        fake.kv.borrow_mut().insert(
            ingest::plan_key("rum"),
            (1, json!({"plan_id": "fedcba987654"})),
        );
        fake.kv
            .borrow_mut()
            .insert(ingest::watermark_key("rum"), (1, json!({"run_id": PLAN})));
        let open = NOW - 3600;
        for i in 0..2 {
            window(&fake, VITALS, open + i * 60, &["1"]);
        }
        let young: Vec<String> = (2..5)
            .map(|i| {
                let key = window(&fake, VITALS, open + i * 60, &["1"]);
                fake.mtimes.borrow_mut().insert(key.clone(), NOW - 60);
                key
            })
            .collect();
        let report = settle(&fake, VITALS);
        assert_eq!((report.compacts, report.sources), (2, 5));
        let made = compacts(&fake, VITALS);
        let mut expected: BTreeSet<String> = made.into_iter().collect();
        expected.extend(uncommitted);
        expected.extend(young);
        let listed = listed(&fake, HOUR, NOW);
        assert_eq!(listed.keys().cloned().collect::<BTreeSet<_>>(), expected);
        assert_eq!(total(&listed), 11);
    }

    /// A closed hour's compacts merge into one, which hides them. A compact is deleted only
    /// once its own sources are, so no row is ever listed twice, even when a delete fails.
    #[test]
    fn a_closed_hours_rum_compacts_merge_and_outlive_their_sources() {
        let fake = Fake::new(NOW);
        let windows = seed(&fake, VITALS, 40);
        let report = settle(&fake, VITALS);
        assert_eq!(report.compacts, 3, "32 windows, then 8, then both compacts");
        assert_eq!(compacts(&fake, VITALS).len(), 3);
        let after = listed(&fake, HOUR, HOUR + 3600);
        assert_eq!(after.len(), 1, "{after:?}");
        assert_eq!(total(&after), 80);
        let merged = after.keys().next().unwrap().clone();
        assert_eq!(parquet_rows(&fake, &merged), 80);
        fake.fail_delete_keys
            .borrow_mut()
            .insert(windows[0].clone());
        fake.now.set(fake.now.get() + DELETE_GRACE_S);
        for _ in 0..2 {
            run(&fake, VITALS);
            assert_eq!(listed(&fake, HOUR, HOUR + 3600), after);
            assert_eq!(compacts(&fake, VITALS).len(), 3);
        }
        fake.fail_delete_keys.borrow_mut().clear();
        for _ in 0..2 {
            run(&fake, VITALS);
        }
        assert_eq!(fake.keys(&root(VITALS)), vec![merged]);
        assert_eq!(listed(&fake, HOUR, HOUR + 3600), after);
    }

    /// A compact whose state was lost after it was published is found by its tags: its
    /// sources are queued again rather than compacted a second time.
    #[test]
    fn sources_of_a_rum_compact_whose_state_was_lost_are_queued_not_compacted_again() {
        let fake = Fake::new(NOW);
        seed(&fake, VITALS, 40);
        settle(&fake, VITALS);
        let made = compacts(&fake, VITALS);
        let after = listed(&fake, HOUR, HOUR + 3600);
        fake.kv
            .borrow_mut()
            .remove(Dataset::Rum(VITALS).state_key());
        assert_eq!(settle(&fake, VITALS).compacts, 0);
        assert_eq!(compacts(&fake, VITALS), made);
        fake.now.set(fake.now.get() + DELETE_GRACE_S);
        for _ in 0..2 {
            run(&fake, VITALS);
        }
        assert_eq!(fake.keys(&root(VITALS)).len(), 1);
        assert_eq!(listed(&fake, HOUR, HOUR + 3600), after);
    }

    /// A listing page that resumes inside an hour still applies that hour's compacts, which sort
    /// before its windows, so the windows they cover never come back on a later page.
    #[test]
    fn a_rum_listing_resumed_inside_an_hour_still_hides_compacted_windows() {
        let fake = Fake::new(NOW);
        let windows: Vec<String> = (0..502)
            .map(|i| {
                let token = format!("{PLAN}s0");
                ingest::processed_key(&root(VITALS), HOUR + i, HOUR + i + 60, &token, 1)
            })
            .collect();
        for key in &windows {
            fake.bucket.borrow_mut().insert(key.clone(), Vec::new());
        }
        let folder = format!("{}{}", root(VITALS), ingest::hour_dir(HOUR));
        let covered = &windows[500..];
        let compact = telemetry::compact::compact_key(
            &folder,
            (HOUR + 500) as u64,
            (HOUR + 561) as u64,
            covered,
            2,
        );
        fake.bucket.borrow_mut().insert(compact.clone(), Vec::new());
        let listed = listed(&fake, HOUR, HOUR + 3600);
        assert_eq!(listed.len(), 501);
        assert!(listed.contains_key(&compact));
        assert!(!covered.iter().any(|key| listed.contains_key(key)));
    }

    fn compaction_service(fake: &Fake, rum: bool) {
        tick_runtime(fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        if rum {
            svc["rum"] = json!({"enabled": true, "endpoint_name": "rum", "log_period": 60});
        }
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
    }

    /// The compaction execution runs RUM passes after the request ones, in their own
    /// `rum_compact` row, and reads no RUM compaction state while RUM is off.
    #[test]
    fn the_compaction_execution_compacts_rum_tables_when_rum_is_enabled() {
        let fake = Fake::new(NOW);
        compaction_service(&fake, false);
        seed(&fake, VITALS, 4);
        request_compaction::seed(&fake, 40);
        let reply = request_compaction::stage_turn(&fake, 0, true);
        assert!(!request_compaction::compacts(&fake).is_empty(), "{reply}");
        assert!(compacts(&fake, VITALS).is_empty(), "{reply}");
        let rum_state = |key: &String| key.starts_with("ingest:compact_state:rum");
        assert!(!fake.kv_reads.borrow().iter().any(rum_state));
        compaction_service(&fake, true);
        seed(&fake, ERRORS, 4);
        let reply = request_compaction::stage_turn(&fake, 0, true);
        assert_eq!(compacts(&fake, VITALS).len(), 1, "{reply}");
        assert_eq!(compacts(&fake, ERRORS).len(), 1, "{reply}");
        let rows = rows(&fake, LogKind::CronRuns);
        let row = rows
            .iter()
            .find(|row| row["cron_kind"] == "rum_compact")
            .unwrap_or_else(|| panic!("{rows:?}"));
        assert_eq!(row["parquet_files_written"], 2, "{row}");
    }

    /// One table's backlog never takes every pass of an execution from the other due table.
    #[test]
    fn a_rum_tables_backlog_leaves_passes_for_the_other_table() {
        let fake = Fake::new(NOW);
        compaction_service(&fake, true);
        for hour in 0..3 {
            for i in 0..40 {
                window(&fake, ERRORS, HOUR + hour * 3600 + i * 60, &["1"]);
            }
        }
        seed(&fake, VITALS, 4);
        let reply = request_compaction::stage_turn(&fake, 0, true);
        assert!(!compacts(&fake, ERRORS).is_empty(), "{reply}");
        assert_eq!(compacts(&fake, VITALS).len(), 1, "{reply}");
        // Its share's last pass hands its lease to no further pass.
        let state = fake.kv.borrow()["ingest:compact_state:rum:client_errors"]
            .1
            .clone();
        assert!(state["handoff"].is_null(), "{state}");
    }
}

mod delete_data_gate {
    use super::*;
    use fastly_log_analytics::admin::{reset::JOB_KEY, DataReset, ResetPhase};

    fn mark(fake: &Fake, phase: Option<ResetPhase>) {
        let status = match phase {
            Some(ResetPhase::Cleanup) => "cleanup",
            _ => "fencing",
        };
        let mut svc = rum_service();
        svc.data_reset = phase.map(|phase| DataReset {
            job_id: "job1".into(),
            reset_at: BASE as u64,
            phase,
            started_at: BASE as u64,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(fake, &svc);
        // A fence still waiting out its leases for the whole scenario, so the job stays put.
        let started_at = fake.now.get() + 100_000;
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": status, "started_at": started_at}),
            ),
        );
    }

    fn scenario(phase: Option<ResetPhase>, ticks: usize) -> Vec<String> {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        mark(&fake, phase.clone());
        put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
        put_rum(
            &fake,
            BASE + 60,
            "ok",
            &[rum_beacon(BASE + 60, "LCP", "123")],
        );
        for _ in 0..ticks {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        let kinds: Vec<String> = rows(&fake, LogKind::CronRuns)
            .iter()
            .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
            .collect();
        if phase == Some(ResetPhase::Fencing) {
            assert_eq!(fake.keys("raw/request/").len(), 1, "{kinds:?}");
        }
        kinds
    }

    fn count(kinds: &[String], kind: &str) -> usize {
        kinds.iter().filter(|k| *k == kind).count()
    }

    #[test]
    fn fencing_marker_skips_ingest_rum_and_raw_delete_but_runs_delete_data() {
        let control = scenario(None, 4);
        assert!(count(&control, "request_ingest") > 0, "{control:?}");
        assert!(count(&control, "rum_ingest") > 0, "{control:?}");
        let kinds = scenario(Some(ResetPhase::Fencing), 4);
        for kind in ["request_ingest", "rum_ingest", "raw_delete"] {
            assert_eq!(count(&kinds, kind), 0, "{kind}: {kinds:?}");
        }
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
    }

    #[test]
    fn fencing_marker_skips_request_data_maintenance_stages() {
        let control = scenario(None, 16);
        assert!(count(&control, "retention_sweep") > 0, "{control:?}");
        let kinds = scenario(Some(ResetPhase::Fencing), 16);
        for kind in [
            "retention_sweep",
            "summary_bundle",
            "request_compact",
            "storage_reconcile",
        ] {
            assert_eq!(count(&kinds, kind), 0, "{kind}: {kinds:?}");
        }
    }

    #[test]
    fn delete_data_stage_runs_every_maintenance_tick_until_complete() {
        let kinds = scenario(Some(ResetPhase::Fencing), 2);
        assert_eq!(count(&kinds, "delete_data"), 2, "{kinds:?}");
    }

    #[test]
    fn cleanup_phase_holds_rum_ingest_until_its_cursor_reset_lands() {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let hold = |rum_held: bool| {
            let mut svc = rum_service();
            svc.data_reset = Some(DataReset {
                job_id: "job1".into(),
                reset_at: BASE as u64,
                phase: ResetPhase::Cleanup,
                started_at: BASE as u64,
                actor: "tester".into(),
                rum_held,
            });
            store_service(&fake, &svc);
        };
        hold(true);
        // A job the marker doesn't own: only the marker's gate is under test.
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (1, json!({"job_id": "other", "status": "complete"})),
        );
        put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
        put_rum(
            &fake,
            BASE + 60,
            "ok",
            &[rum_beacon(BASE + 60, "LCP", "123")],
        );
        let ticks = |n: usize| {
            for _ in 0..n {
                fake.now.set(fake.now.get() + 120);
                run_tick(&fake);
            }
            rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect::<Vec<String>>()
        };
        let kinds = ticks(4);
        assert!(count(&kinds, "request_ingest") > 0, "{kinds:?}");
        assert_eq!(count(&kinds, "rum_ingest"), 0, "{kinds:?}");
        assert!(!fake.keys("processed/request/").is_empty(), "{kinds:?}");
        assert!(fake.keys("processed/rum/").is_empty(), "{kinds:?}");
        assert_eq!(fake.keys("raw/rum/").len(), 1);
        hold(false);
        let kinds = ticks(4);
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
    }

    /// RUM floats in every processed RUM file.
    fn rum_values(fake: &Fake) -> Vec<f64> {
        let mut values = Vec::new();
        for key in fake.keys("processed/rum/") {
            for batch in batches(&fake.bucket.borrow()[&key]) {
                if batch.column_by_name("metric_value").is_some() {
                    values.extend(floats(&batch, "metric_value").into_iter().flatten());
                }
            }
        }
        values.sort_by(f64::total_cmp);
        values
    }

    #[test]
    fn a_held_rum_stream_resumes_from_the_floor_and_never_converts_pre_reset_beacons() {
        // Reset well before the clock, so new objects (LIST mtime `BASE`) postdate the CAS.
        let reset_at = BASE - 1200;
        let cas_at = reset_at + 30;
        let fake = Fake::new(BASE);
        tick_runtime(&fake);
        let mut svc = rum_service();
        // A long request settle keeps cleanup from deleting the raw keys during the test.
        svc.log_period = 1800;
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at: reset_at as u64,
            phase: ResetPhase::Cleanup,
            started_at: reset_at as u64,
            actor: "tester".into(),
            rum_held: true,
        });
        store_service(&fake, &svc);
        let minute = |t: i64| ingest::minute_dir(t);
        let floor = format!("raw/request/{}", minute(reset_at));
        // The post-CAS bound resumed request ingest; the RUM cursor reset never landed.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                1,
                json!({"key": floor, "floor_key": floor, "at": cas_at, "passes": [], "swept": [],
                "sweep": {"after": floor, "at": cas_at}, "manifest_gen": 1}),
            ),
        );
        fake.kv.borrow_mut().insert(
            "ingest:rum:watermark".into(),
            (
                1,
                json!({"key": format!("raw/rum/{}", minute(reset_at - 600)), "at": 1,
                "passes": [], "swept": []}),
            ),
        );
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "cleanup", "reset_at": reset_at,
                "started_at": reset_at, "resetting_at": cas_at - 2, "reset_cas_at": cas_at,
                "steps_done": ["keys", "deleteq", "quarantine"]}),
            ),
        );
        put_rum(
            &fake,
            reset_at - 60,
            "old",
            &[rum_beacon(reset_at - 30, "LCP", "111")],
        );
        put_rum(
            &fake,
            reset_at + 60,
            "new",
            &[rum_beacon(reset_at + 90, "LCP", "222")],
        );
        let mut kinds = Vec::new();
        for _ in 0..6 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
            kinds = rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect();
        }
        let svc: Service =
            serde_json::from_value(fake.kv.borrow()[&format!("svc:{SID}")].1.clone()).unwrap();
        assert!(
            !fastly_log_analytics::admin::reset::rum_held(&svc),
            "{kinds:?}"
        );
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
        assert_eq!(rum_values(&fake), [222.0], "{kinds:?}");
    }

    #[test]
    fn cleanup_phase_runs_normal_stages_again() {
        let kinds = scenario(Some(ResetPhase::Cleanup), 16);
        assert!(count(&kinds, "request_ingest") > 0, "{kinds:?}");
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
        assert!(count(&kinds, "retention_sweep") > 0, "{kinds:?}");
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
    }

    #[test]
    fn delete_data_and_a_rotating_stage_share_one_execution_vcpu_allowance() {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let reset_at = BASE as u64 + 540;
        let cas_at = reset_at + 30;
        let mut svc = rum_service();
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at,
            phase: ResetPhase::Cleanup,
            started_at: reset_at,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(&fake, &svc);
        let floor = format!("raw/request/{}", ingest::minute_dir(reset_at as i64));
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                1,
                json!({"key": floor, "floor_key": floor, "at": cas_at, "passes": [], "swept": [],
                "sweep": {"after": floor, "at": cas_at}, "manifest_gen": 1}),
            ),
        );
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "cleanup", "reset_at": reset_at,
                "started_at": reset_at, "resetting_at": cas_at, "reset_cas_at": cas_at,
                "steps_done": ["rum_watermark", "rum_released", "keys", "deleteq", "quarantine"]}),
            ),
        );
        // Pre-reset objects (LIST mtime `BASE`) for cleanup, and old ops logs for the others.
        for i in 0..6000 {
            fake.bucket.borrow_mut().insert(
                format!("processed/request/day=1/old-{i:05}.parquet"),
                vec![1],
            );
        }
        for day in 1..9 {
            for hour in 0..24 {
                fake.bucket.borrow_mut().insert(
                    format!(
                        "processed/ops/cron_runs/year=2026/month=12/day={day:02}/hour={hour:02}/p.parquet"
                    ),
                    vec![1],
                );
            }
        }
        fake.vcpu_step.set(60);
        let mut kinds = Vec::new();
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
            kinds = rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect();
        }
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
        let peak = fake.vcpu_peaks.borrow().get("stage").copied().unwrap_or(0);
        assert!(
            peak <= 1000,
            "stage execution used {peak}ms vCPU: {kinds:?}"
        );
    }

    const OLD: &str = "processed/request/day=1/old-";

    /// A Delete Data job in cleanup past its retirement grace, with `objects` pre-reset
    /// processed files to delete.
    fn cleanup_fixture(objects: usize) -> Fake {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let reset_at = BASE as u64 + 540;
        let cas_at = reset_at + 30;
        let mut svc = rum_service();
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at,
            phase: ResetPhase::Cleanup,
            started_at: reset_at,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(&fake, &svc);
        let floor = format!("raw/request/{}", ingest::minute_dir(reset_at as i64));
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                1,
                json!({"key": floor, "floor_key": floor, "at": cas_at, "passes": [], "swept": [],
                "sweep": {"after": floor, "at": cas_at}, "manifest_gen": 1}),
            ),
        );
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "cleanup", "reset_at": reset_at,
                "started_at": reset_at, "resetting_at": cas_at, "reset_cas_at": cas_at,
                "steps_done": ["rum_watermark", "rum_released", "keys", "deleteq", "quarantine"]}),
            ),
        );
        for i in 0..objects {
            fake.bucket
                .borrow_mut()
                .insert(format!("{OLD}{i:05}.parquet"), vec![1, 2]);
        }
        fake
    }

    fn job(fake: &Fake) -> Value {
        fake.kv.borrow()[JOB_KEY].1.clone()
    }

    fn deleted(job: &Value) -> u64 {
        job["deleted"]
            .as_object()
            .map_or(0, |map| map.values().filter_map(Value::as_u64).sum())
    }

    fn left(fake: &Fake) -> usize {
        fake.keys(OLD).len()
    }

    fn maint_more_pending(fake: &Fake) -> Value {
        fake.kv.borrow()["cron:state"].1["jobs"]["maint"]["more_pending"].clone()
    }

    #[test]
    fn delete_data_sends_a_rounds_deletes_together_and_ticks_again_at_once_while_objects_remain() {
        use fastly_log_analytics::admin::schedule::{peek, tick_wake_at, SCHED_KEY};
        let n = 25_000;
        let fake = cleanup_fixture(n);
        // Half a second per LIST and four seconds per round of DeleteObjects requests.
        fake.fos_wall.set(Some((500, 4_000)));
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        let first = n - left(&fake);
        assert!(first >= 5_000, "the first step deleted {first} objects");
        let rounds = fake.delete_rounds.borrow().clone();
        assert!(rounds.iter().any(|&sent| sent >= 5), "{rounds:?}");
        assert!(fake.delete_posts.borrow().iter().all(|&keys| keys <= 1000));
        assert_eq!(maint_more_pending(&fake), true);
        let record = peek(&fake, SCHED_KEY).unwrap();
        assert!(tick_wake_at(&record) <= fake.now.get());
        let mut ticks = 1;
        while left(&fake) > 0 {
            assert!(
                ticks < 6,
                "{} objects left after {ticks} ticks",
                left(&fake)
            );
            fake.now.set(fake.now.get() + 1);
            run_tick(&fake);
            ticks += 1;
        }
        let job = job(&fake);
        assert_eq!(deleted(&job), n as u64, "{job}");
        assert_eq!(job["bytes"], 2 * n as u64, "{job}");
        assert_eq!(job["pending"], 0, "{job}");
        assert!(job["error"].is_null(), "{job}");
        assert!(job["delete_ms"].as_u64().unwrap() >= 4_000, "{job}");
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        assert_eq!(maint_more_pending(&fake), false);
    }

    #[test]
    fn delete_data_keeps_a_partly_failed_rounds_cursor_and_counts_each_object_once() {
        let n = 3_000;
        let fake = cleanup_fixture(n);
        fake.fail_delete_keys
            .borrow_mut()
            .insert(format!("{OLD}01500.parquet"));
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert_eq!(*fake.delete_rounds.borrow(), vec![3]);
        assert_eq!(left(&fake), 1);
        let failed = job(&fake);
        assert_eq!(
            failed["error"], "delete_failed: 1 objects under processed/request/",
            "{failed}"
        );
        assert_eq!(failed["pending"], 1, "{failed}");
        assert_eq!(deleted(&failed), n as u64 - 1, "{failed}");
        assert_eq!(
            failed["cursors"]["processed/request/"],
            format!("{OLD}00999.parquet"),
            "{failed}"
        );
        assert_eq!(maint_more_pending(&fake), false);
        fake.fail_delete_keys.borrow_mut().clear();
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert_eq!(left(&fake), 0);
        let job = job(&fake);
        assert_eq!(deleted(&job), n as u64, "{job}");
        assert_eq!(job["pending"], 0, "{job}");
    }

    /// A round's `DeleteObjects` outlasting the step's remaining wall time is the step's budget
    /// running out, not a failure: the step stops with more pending and the next one lists
    /// the prefix again.
    #[test]
    fn delete_data_stops_with_more_pending_when_a_rounds_deletes_outlast_the_step() {
        let n = 1_500;
        let fake = cleanup_fixture(n);
        fake.fos_wall.set(Some((500, 50_000)));
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert!(!fake.delete_rounds.borrow().is_empty());
        let stopped = job(&fake);
        assert_eq!(stopped["status"], "cleanup", "{stopped}");
        assert!(stopped["error"].is_null(), "{stopped}");
        assert!(
            stopped["cursors"]["processed/request/"].is_null(),
            "{stopped}"
        );
        assert_eq!(maint_more_pending(&fake), true);
        fake.fos_wall.set(None);
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        assert_eq!(left(&fake), 0);
        let job = job(&fake);
        assert!(job["error"].is_null(), "{job}");
        assert_eq!(job["pending"], 0, "{job}");
    }

    #[test]
    fn delete_data_sends_a_slowed_down_batch_again_after_its_retry_after() {
        let fake = cleanup_fixture(2_000);
        fake.transient
            .borrow_mut()
            .push_back(("DELETE", 503, Some(1)));
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert_eq!(*fake.delete_rounds.borrow(), vec![2, 1]);
        assert!(fake.waits.borrow().contains(&1000), "{:?}", fake.waits);
        assert_eq!(left(&fake), 0);
        let job = job(&fake);
        assert!(job["error"].is_null(), "{job}");
        assert_eq!(deleted(&job), 2_000, "{job}");
    }

    #[test]
    fn delete_data_reports_a_persistent_delete_failure_and_resumes_later() {
        let n = 1_500;
        let fake = cleanup_fixture(n);
        fake.delete_status.set(503);
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert_eq!(*fake.delete_rounds.borrow(), vec![2, 2]);
        assert_eq!(left(&fake), n);
        let failed = job(&fake);
        assert_eq!(failed["status"], "cleanup", "{failed}");
        assert_eq!(failed["error"], "delete_objects_http_503", "{failed}");
        assert_eq!(failed["pending"], n as u64, "{failed}");
        assert_eq!(deleted(&failed), 0, "{failed}");
        assert_eq!(maint_more_pending(&fake), false);
        fake.delete_status.set(200);
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        assert_eq!(left(&fake), 0);
        let job = job(&fake);
        assert_eq!(deleted(&job), n as u64, "{job}");
        assert_eq!(job["pending"], 0, "{job}");
    }
}

mod delete_data_reset {
    use super::*;
    use fastly_log_analytics::admin::{self, reset::JOB_KEY, DataReset, ResetPhase};
    use ingest::catalog;

    const RESET_AT: u64 = BASE as u64 + 600;

    /// Runs a Delete Data job's "resetting" step at `RESET_AT`, as the maintenance stage would.
    fn reset(fake: &Fake) {
        let mut svc = rum_service();
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at: RESET_AT,
            phase: ResetPhase::Fencing,
            started_at: RESET_AT,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(fake, &svc);
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "resetting", "reset_at": RESET_AT,
                    "started_at": RESET_AT}),
            ),
        );
        fake.new_request();
        admin::reset::advance(&store(fake, &svc)).unwrap().unwrap();
        let job = fake.kv.borrow()[JOB_KEY].1.clone();
        assert_eq!(job["status"], "cleanup", "{job}");
        assert!(fake.calls.get() <= BUDGET);
    }

    fn publish_two(fake: &Fake) {
        for (n, name) in ["a", "b"].iter().enumerate() {
            let start = BASE + n as i64 * 60;
            let key = ingest::processed_key("processed/request/", start, start + 60, name, 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            publish_files(fake, &[key], &[], &[]);
        }
    }

    /// The ips of every file the current catalog head's recent commits added.
    fn catalog_ips(fake: &Fake) -> Vec<String> {
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let head = catalog::head(&watermark).unwrap();
        let mut ips = Vec::new();
        for event in &head.recent {
            let body = fake.bucket.borrow()[event.manifest.as_ref().unwrap()].clone();
            let manifest: catalog::Manifest = serde_json::from_slice(&body).unwrap();
            for file in &manifest.files {
                for batch in batches(&fake.bucket.borrow()[&file.key]) {
                    ips.extend(strings(&batch, "ip").into_iter().flatten());
                }
            }
        }
        ips.sort();
        ips
    }

    #[test]
    fn reset_head_raises_generation_and_empties_catalog() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let prev = catalog::head(&watermark).unwrap();
        assert!(!prev.days.is_empty() && !prev.statistics.is_empty());
        let now = (BASE + 3600) as u64;
        let (head, manifest) = catalog::reset_head(&prev, now, SID);
        assert_eq!(head.generation, prev.generation + 1);
        assert_eq!(head.recent.len(), 1);
        assert_eq!(head.recent[0].generation, head.generation);
        assert_eq!(head.recent[0].cursor, format!("{SID}:{}", head.generation));
        assert!(head.days.is_empty());
        assert!(head.statistics.is_empty());
        assert!(head.hour_bundles.is_empty() && head.day_bundles.is_empty());
        assert!(head.bundle_retired.is_empty());
        assert!(head.last_compaction.is_none());
        assert_eq!(
            head.retirement_grace_until,
            now + ingest::compaction::DELETE_GRACE_S
        );
        assert_eq!(manifest.batch, "reset");
        assert_eq!(manifest.generation, head.generation);
        assert_eq!(manifest.service_id, SID);
        assert!(manifest.files.is_empty() && manifest.summaries.is_empty());
        assert!(manifest.previous.is_none());
        let mut value = watermark;
        value["catalog"] = serde_json::to_value(&head).unwrap();
        assert_eq!(catalog::head(&value).unwrap().generation, head.generation);
    }

    #[test]
    fn delta_walk_over_reset_manifest_requires_resync() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        reset(&fake);
        let window = format!(
            "from={}&to={}&previous_from={}&previous_to={}",
            iso_url(BASE),
            iso_url(BASE + 3600),
            iso_url(BASE),
            iso_url(BASE + 3600)
        );
        for base in [1, 2] {
            assert_eq!(
                files_page(&fake, &format!("{window}&since_generation={base}")).unwrap_err(),
                (409, "catalog_resync_required"),
                "base {base}"
            );
        }
    }

    #[test]
    fn sse_cursor_from_before_reset_gets_resync() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        reset(&fake);
        fake.new_request();
        let stale = catalog::events(&fake, &service(), &format!("cursor={SID}:2")).unwrap();
        assert!(stale.contains("event: resync\n"), "{stale}");
        assert!(stale.contains(&format!("id: {SID}:3\n")), "{stale}");
        fake.new_request();
        let current = catalog::events(&fake, &service(), &format!("cursor={SID}:3")).unwrap();
        assert!(!current.contains("event:"), "{current}");
    }

    #[test]
    fn reset_clears_committed_plans_so_no_pre_reset_raw_key_is_ingested_again() {
        let fake = Fake::new(BASE + 600);
        put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
        tick(&fake).unwrap();
        let committed = fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].clone();
        assert_eq!(
            fake.kv.borrow()[ingest::PLAN_KEY].1["plan_id"],
            committed,
            "a committed plan record stays in place and names pre-reset raw keys"
        );
        fake.kv.borrow_mut().insert(
            ingest::plan_key("rum"),
            (
                1,
                json!({"version": 3, "plan_id": "rum-old", "lease_until": 0}),
            ),
        );
        reset(&fake);
        for key in [ingest::plan_key("request"), ingest::plan_key("rum")] {
            assert!(!fake.kv.borrow().contains_key(&key), "{key}");
        }
        // The pre-reset raw object is still in the bucket and a late one lands below the floor.
        put_raw(&fake, BASE + 60, "late", &[line(BASE + 60, "late")]);
        put_raw(
            &fake,
            RESET_AT as i64,
            "new",
            &[line(RESET_AT as i64, "new")],
        );
        for _ in 0..6 {
            tick_later(&fake).unwrap();
        }
        assert_eq!(catalog_ips(&fake), vec!["new".to_owned()]);
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        assert_ne!(watermark["run_id"], committed);
        assert!(watermark["catalog"]["generation"].as_u64().unwrap() > 2);
    }

    #[test]
    fn a_commit_whose_watermark_read_predates_the_reset_cannot_land_after_it() {
        let fake = Fake::new(BASE + 600);
        put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
        tick(&fake).unwrap();
        // A coordinator read this watermark before the reset CAS.
        fake.new_request();
        let stale = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
        reset(&fake);
        let after = fake.kv.borrow()[ingest::WATERMARK_KEY].clone();
        assert!(
            after.0 > stale.generation.parse::<u64>().unwrap(),
            "the reset is an overwrite CAS, never a delete and re-create"
        );
        let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "stale", 1);
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        let output = ingest::worker::Output {
            key,
            rows: 1,
            bytes: 1,
            ..Default::default()
        };
        fake.new_request();
        let value = catalog::prepare(
            &store(&fake, &service()),
            &stale.value,
            "stale-plan",
            "schema-v1",
            &[output],
            &[],
            &[],
            &[],
        )
        .unwrap();
        assert!(!fake
            .admin_cas(ingest::WATERMARK_KEY, Some(&stale.generation), &value)
            .unwrap());
        assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY], after);
    }
}
