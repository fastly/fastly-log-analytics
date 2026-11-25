use super::{freshness, support::*};
use arrow_array::{cast::AsArray, Array, Int64Array, StringArray, TimestampMillisecondArray};
use bytes::Bytes;
use fastly_log_analytics::{
    ingest::{self},
    telemetry::{self, usage::UsageAgg, LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

#[test]
fn a_tick_that_arrives_with_execution_vcpu_already_spent_still_commits() {
    // Live ticks reach the catalog commit with 40-50 ms of execution vCPU behind them (tick
    // startup, plan parsing, the source-index dispatch). The catalog's admission gate must not
    // turn that into a tick that re-plans forever and never writes a row.
    let fake = Fake::new(BASE + 3600);
    for (m, name) in ["a", "b", "c"].iter().enumerate() {
        let minute = BASE + m as i64 * 60;
        let lines: Vec<String> = (0..4)
            .map(|i| line(minute + i * 10, &format!("{name}{i}")))
            .collect();
        put_raw(&fake, minute, name, &lines);
    }
    let mut report = None;
    for _ in 0..4 {
        fake.new_request();
        fake.vcpu.set(45);
        let buffer = LogBuffer::new(SID, "req");
        let r = ingest::run(&fake, &service(), "ak", "sk", Instant::now(), &buffer).unwrap();
        let committed = fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].is_string();
        report = Some(r);
        if committed {
            break;
        }
    }
    let report = report.unwrap();
    assert_eq!((report.converted, report.rows), (3, 12));
    assert_eq!(fake.keys("processed/request/").len(), 3);
    assert!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].is_string());
}

#[test]
fn canonical_fixture_has_cross_stack_ingest_parity() {
    const FIXTURE: &str = include_str!("../../../../tests/fixtures/canonical_ingest.ndjson");

    let fake = Fake::new(BASE + 3600);
    let mut fixture_service = service();
    fixture_service.log_fields = json!({
        "groups": [],
        "field_overrides": {},
        "custom_fields": [
            {"name": "tenant", "duckdb_type": "VARCHAR", "enabled": true, "committed": true},
            {"name": "score", "duckdb_type": "BIGINT", "enabled": true, "committed": true},
            {"name": "optional", "duckdb_type": "BOOLEAN", "enabled": true, "committed": true},
            {"name": "nullable", "duckdb_type": "VARCHAR", "enabled": true, "committed": true}
        ]
    });
    let lines: Vec<String> = FIXTURE.lines().map(str::to_owned).collect();
    put_raw(&fake, BASE, "canonical", &lines);

    let report = tick_with_service(&fake, &fixture_service).unwrap();
    assert_eq!(
        (
            report.converted,
            report.failed,
            report.rows,
            report.bad_lines,
            report.lines_read
        ),
        (1, 0, 5, 2, 7)
    );

    let visible =
        fastly_log_analytics::telemetry::compact::visible(&fake.keys("processed/request/"), &[]);
    assert_eq!(visible.len(), 2);
    let persisted_before: BTreeMap<_, _> = visible
        .iter()
        .map(|key| (key.clone(), fake.bucket.borrow()[key].clone()))
        .collect();
    type FixtureRow = (
        String,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<bool>,
        Option<String>,
    );
    let mut assigned = BTreeMap::<i64, Vec<FixtureRow>>::new();
    for key in &visible {
        let start = ingest::parse_processed(key).unwrap().start;
        for batch in batches(&fake.bucket.borrow()[key]) {
            let ids = batch.column_by_name("rid").unwrap().as_string::<i32>();
            let status = batch
                .column_by_name("status")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let timestamp = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let tenant = batch.column_by_name("tenant").unwrap();
            assert_eq!(tenant.data_type(), &arrow_schema::DataType::Utf8);
            let tenant = tenant.as_any().downcast_ref::<StringArray>().unwrap();
            let score = batch.column_by_name("score").unwrap();
            assert_eq!(score.data_type(), &arrow_schema::DataType::Int64);
            let score = score.as_any().downcast_ref::<Int64Array>().unwrap();
            let optional = batch.column_by_name("optional").unwrap();
            assert_eq!(optional.data_type(), &arrow_schema::DataType::Boolean);
            let optional = optional
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            let nullable = batch.column_by_name("nullable").unwrap();
            assert_eq!(nullable.data_type(), &arrow_schema::DataType::Utf8);
            let nullable = nullable.as_any().downcast_ref::<StringArray>().unwrap();
            assigned
                .entry(start)
                .or_default()
                .extend((0..batch.num_rows()).map(|i| {
                    (
                        ids.value(i).to_owned(),
                        status.iter().nth(i).flatten(),
                        timestamp.is_valid(i).then(|| timestamp.value(i)),
                        tenant.is_valid(i).then(|| tenant.value(i).to_owned()),
                        score.is_valid(i).then(|| score.value(i)),
                        optional.is_valid(i).then(|| optional.value(i)),
                        nullable.is_valid(i).then(|| nullable.value(i).to_owned()),
                    )
                }));
        }
    }
    assert_eq!(assigned[&BASE].len(), 4);
    assert_eq!(assigned[&(BASE + 60)].len(), 1);
    assert_eq!(
        assigned[&BASE]
            .iter()
            .map(
                |(id, status, timestamp, tenant, score, optional, nullable)| {
                    (
                        id.as_str(),
                        *status,
                        *timestamp,
                        tenant.as_deref(),
                        *score,
                        *optional,
                        nullable.as_deref(),
                    )
                }
            )
            .collect::<Vec<_>>(),
        [
            (
                "canonical-stable",
                Some(200),
                Some(BASE * 1000 + 1_000),
                None,
                None,
                None,
                None,
            ),
            (
                "canonical-stable",
                Some(200),
                Some(BASE * 1000 + 2_000),
                None,
                None,
                None,
                None,
            ),
            (
                "canonical-start",
                Some(503),
                Some(BASE * 1000),
                None,
                None,
                None,
                None,
            ),
            (
                "canonical-null",
                None,
                Some(BASE * 1000 + 59_000),
                None,
                None,
                None,
                None,
            ),
        ]
    );
    assert_eq!(
        assigned[&BASE]
            .iter()
            .filter(|(id, ..)| id == "canonical-stable")
            .count(),
        2
    );
    assert_eq!(
        assigned[&(BASE + 60)],
        [(
            "canonical-boundary".into(),
            Some(204),
            Some(BASE * 1000 + 60_000),
            Some("alpha".into()),
            Some(7),
            None,
            None,
        )]
    );

    let (mut requests, mut bandwidth) = (0, 0);
    for key in fake.keys("processed/summary/request/") {
        for batch in batches(&fake.bucket.borrow()[&key]) {
            let bundles = batch.column_by_name("bundle").unwrap().as_string::<i32>();
            let request_counts = batch
                .column_by_name("requests")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let byte_counts = batch
                .column_by_name("total_bandwidth_bytes")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..batch.num_rows() {
                if bundles.value(i) == "overview" {
                    requests += request_counts.value(i);
                    bandwidth += byte_counts.value(i);
                }
            }
        }
    }
    assert_eq!((requests, bandwidth), (5, 70));
    assert_eq!(
        tick_with_service(&fake, &fixture_service)
            .unwrap()
            .converted,
        0
    );
    let visible_after =
        fastly_log_analytics::telemetry::compact::visible(&fake.keys("processed/request/"), &[]);
    let persisted_after: BTreeMap<_, _> = visible_after
        .iter()
        .map(|key| (key.clone(), fake.bucket.borrow()[key].clone()))
        .collect();
    assert_eq!(persisted_after, persisted_before);
}

#[test]
fn canonical_fixture_six_paginated_objects_interrupted_publication_parity() {
    const FIXTURE: &str = include_str!("../../../../tests/fixtures/canonical_ingest.ndjson");

    let fake = Fake::new(BASE + 3600);
    fake.max_list_keys.set(Some(2));
    let mut fixture_service = service();
    fixture_service.log_fields = json!({
        "groups": [],
        "field_overrides": {},
        "custom_fields": [
            {"name": "tenant", "duckdb_type": "VARCHAR", "enabled": true, "committed": true},
            {"name": "score", "duckdb_type": "BIGINT", "enabled": true, "committed": true},
            {"name": "optional", "duckdb_type": "BOOLEAN", "enabled": true, "committed": true},
            {"name": "nullable", "duckdb_type": "VARCHAR", "enabled": true, "committed": true}
        ]
    });

    let lines: Vec<String> = FIXTURE.lines().map(str::to_owned).collect();
    assert_eq!(lines.len(), 7);
    let groups: [&[String]; 6] = [
        &lines[0..1],
        &lines[1..2],
        &lines[2..3],
        &lines[3..4],
        &lines[4..6],
        &lines[6..7],
    ];
    for (idx, group) in groups.iter().enumerate() {
        put_raw(&fake, BASE, &format!("p{}", idx + 1), group);
    }

    let mut total_rows = 0u64;
    let mut total_bad_lines = 0u64;
    let mut total_lines_read = 0u64;

    // Page 1: interrupt at archive/worker PUT, then recover.
    fake.fail_put_number.set(Some(1));
    let failed_p1 = tick_with_service(&fake, &fixture_service).unwrap();
    assert!(failed_p1.more_pending);
    assert_eq!(failed_p1.converted, 0);
    fake.fail_put_number.set(None);
    let ok_p1 = tick_with_service(&fake, &fixture_service).unwrap();
    assert_eq!(ok_p1.converted, 2);
    total_rows += ok_p1.rows;
    total_bad_lines += ok_p1.bad_lines;
    total_lines_read += ok_p1.lines_read;

    // Page 2: interrupt coordinator mid-tick (manifest/plan phase), then recover after lease expiry.
    fake.crash_after.set(Some(5));
    assert!(tick_with_service(&fake, &fixture_service).is_err());
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
    fake.crash_after.set(None);
    let ok_p2 = tick_with_service(&fake, &fixture_service).unwrap();
    assert!(ok_p2.resumed);
    assert_eq!(ok_p2.converted, 2);
    total_rows += ok_p2.rows;
    total_bad_lines += ok_p2.bad_lines;
    total_lines_read += ok_p2.lines_read;

    // Page 3: interrupt at watermark commit (serving), then recover after lease expiry.
    fake.fail_watermark.set(true);
    let failed_p3 = tick_with_service(&fake, &fixture_service);
    assert!(failed_p3.is_err());
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
    fake.fail_watermark.set(false);
    let mut ok_p3 = tick_with_service(&fake, &fixture_service).unwrap();
    assert!(ok_p3.resumed);
    assert_eq!(ok_p3.converted, 2);
    total_rows += ok_p3.rows;
    total_bad_lines += ok_p3.bad_lines;
    total_lines_read += ok_p3.lines_read;

    while ok_p3.more_pending {
        ok_p3 = tick_with_service(&fake, &fixture_service).unwrap();
        total_rows += ok_p3.rows;
        total_bad_lines += ok_p3.bad_lines;
        total_lines_read += ok_p3.lines_read;
    }

    assert_eq!((total_rows, total_bad_lines, total_lines_read), (5, 2, 7));

    // Idempotent replay after full completion writes nothing new.
    let replay = tick_with_service(&fake, &fixture_service).unwrap();
    assert_eq!(replay.converted, 0);

    let visible =
        fastly_log_analytics::telemetry::compact::visible(&fake.keys("processed/request/"), &[]);
    let mut visible_rows = 0usize;
    let mut status_counts: BTreeMap<Option<i64>, usize> = BTreeMap::new();
    let mut count_5xx = 0usize;
    let mut total_bytes = 0i64;
    let mut distinct_ips: BTreeSet<String> = BTreeSet::new();
    for key in &visible {
        for batch in batches(&fake.bucket.borrow()[key]) {
            visible_rows += batch.num_rows();
            let status = batch
                .column_by_name("status")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let resp_bytes = batch
                .column_by_name("resp_bytes")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let client_ip = batch
                .column_by_name("client_ip")
                .and_then(|col| col.as_any().downcast_ref::<StringArray>());
            for i in 0..batch.num_rows() {
                let s = status.iter().nth(i).flatten();
                *status_counts.entry(s).or_default() += 1;
                if s.is_some_and(|code| (500..=599).contains(&code)) {
                    count_5xx += 1;
                }
                total_bytes += resp_bytes.iter().nth(i).flatten().unwrap_or(0);
                if let Some(ip_col) = client_ip {
                    if ip_col.is_valid(i) && !ip_col.value(i).is_empty() {
                        distinct_ips.insert(ip_col.value(i).to_owned());
                    }
                }
            }
        }
    }
    assert_eq!(visible_rows, 5);
    assert_eq!(
        status_counts,
        BTreeMap::from([(None, 1), (Some(200), 2), (Some(204), 1), (Some(503), 1)])
    );
    assert_eq!(count_5xx, 1);
    assert_eq!(distinct_ips.len(), 0);
    assert_eq!(total_bytes, 70);

    let (mut ov_requests, mut ov_bandwidth, mut ov_ips) = (0i64, 0i64, 0i64);
    let (mut ts_requests, mut ts_5xx, mut ts_bytes) = (0i64, 0i64, 0i64);
    for key in fake.keys("processed/summary/request/") {
        for batch in batches(&fake.bucket.borrow()[&key]) {
            let bundles = batch.column_by_name("bundle").unwrap().as_string::<i32>();
            let req_col = batch
                .column_by_name("requests")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let bw_col = batch
                .column_by_name("total_bandwidth_bytes")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let ip_col = batch
                .column_by_name("ip_count_observed")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let s5xx_col = batch
                .column_by_name("status_5xx")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            let bytes_col = batch
                .column_by_name("resp_bytes_sum")
                .unwrap()
                .as_primitive::<arrow_array::types::Int64Type>();
            for i in 0..batch.num_rows() {
                match bundles.value(i) {
                    "overview" => {
                        ov_requests += req_col.value(i);
                        ov_bandwidth += bw_col.value(i);
                        ov_ips += ip_col.value(i);
                    }
                    "time_series" => {
                        ts_requests += req_col.value(i);
                        ts_5xx += s5xx_col.value(i);
                        ts_bytes += bytes_col.value(i);
                    }
                    _ => {}
                }
            }
        }
    }
    assert_eq!((ov_requests, ov_bandwidth, ov_ips), (5, 70, 0));
    assert_eq!((ts_requests, ts_5xx, ts_bytes), (5, 1, 70));
}

#[test]
fn every_row_lands_exactly_once_and_routes_by_window() {
    let fake = Fake::new(BASE + 3600);
    let mut expected = Vec::new();
    for (m, name) in ["a", "b", "c"].iter().enumerate() {
        let minute = BASE + m as i64 * 60;
        let lines: Vec<String> = (0..4)
            .map(|i| {
                let id = format!("{name}{i}");
                expected.push(id.clone());
                line(minute + i * 10, &id)
            })
            .collect();
        put_raw(&fake, minute, name, &lines);
    }
    let report = tick(&fake).unwrap();
    assert_eq!((report.converted, report.rows), (3, 12));
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    let files = fake.keys("processed/request/");
    assert_eq!(files.len(), 3, "one file per 60s window: {files:?}");
    let summaries = fake.keys("processed/summary/request/");
    let mut paired: Vec<String> = summaries
        .iter()
        .map(|k| {
            k.replace("processed/summary/request/", "processed/request/")
                .replace("/summary=", "/window=")
        })
        .collect();
    paired.sort();
    assert_eq!(paired, files, "one summary beside each window");
    for key in &files {
        let name = ingest::parse_processed(key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert_eq!(name.rows, Some(4));
        assert!(key.contains("year=2027/month=01/day=15/hour=08/window="));
        for batch in batches(&fake.bucket.borrow()[key]) {
            let ts = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            assert!((0..ts.len()).all(|i| {
                let s = ts.value(i) / 1000;
                s >= name.start && s < name.end
            }));
        }
    }
    let again = tick(&fake).unwrap();
    assert_eq!(again.converted, 0);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn convert_object_counts_every_non_empty_line() {
    let data = gz(&[
        line(BASE, "a"),
        String::new(),
        "{not json".into(),
        line(BASE, "b"),
    ]);
    let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
    assert_eq!((rows.rows, rows.bad_lines, rows.lines_read), (2, 1, 3));
}

#[test]
fn rows_route_by_millisecond_ts_ms_and_fall_back_to_timestamp() {
    let at = |s: i64, ms: i64, ip: &str| {
        json!({"timestamp": iso(s), "ts_ms": s * 1000 + ms, "ip": ip}).to_string()
    };
    // ts_ms wins over a disagreeing timestamp; a legacy line without ts_ms uses timestamp.
    let lying = json!({"timestamp": iso(BASE + 61), "ts_ms": BASE * 1000 + 999, "ip": "c"});
    let data = gz(&[
        at(BASE + 5, 250, "a"),
        line(BASE + 70, "legacy"),
        lying.to_string(),
    ]);
    let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
    assert_eq!(
        rows.windows.keys().copied().collect::<Vec<_>>(),
        [BASE, BASE + 60]
    );
    let first = &rows.windows[&BASE];
    assert_eq!(
        ms_column(first, "ts_ms"),
        [Some((BASE + 5) * 1000 + 250), Some(BASE * 1000 + 999)]
    );
    let second = &rows.windows[&(BASE + 60)];
    assert_eq!(ms_column(second, "ts_ms"), [None]);
    assert_eq!(ms_column(second, "timestamp"), [Some((BASE + 70) * 1000)]);
}

#[test]
fn window_files_are_sorted_by_time_and_declare_sorting_columns() {
    let row = |s: i64, ms: Option<i64>, ip: &str| match ms {
        Some(ms) => json!({"timestamp": iso(s), "ts_ms": s * 1000 + ms, "ip": ip, "status": 200})
            .to_string(),
        None => line(s, ip),
    };
    let one = ingest::convert::convert_object(
        &gz(&[
            row(BASE + 40, Some(5), "d"),
            row(BASE + 2, None, "legacy-late"),
            row(BASE + 10, Some(900), "b"),
            row(BASE + 1, None, "legacy-early"),
        ]),
        60,
        BASE,
    )
    .unwrap();
    let two = ingest::convert::convert_object(
        &gz(&[row(BASE + 10, Some(100), "a"), row(BASE + 30, Some(0), "c")]),
        60,
        BASE,
    )
    .unwrap();
    let batches_in = [&one.windows[&BASE], &two.windows[&BASE]];
    let bytes = ingest::convert::write_parquet(&batches_in).unwrap();
    assert_eq!(bytes, ingest::convert::write_parquet(&batches_in).unwrap());
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
    let schema = builder.parquet_schema().clone();
    for group in builder.metadata().row_groups() {
        let sorting: Vec<(String, bool, bool)> = group
            .sorting_columns()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    schema.column(c.column_idx as usize).name().to_owned(),
                    c.descending,
                    c.nulls_first,
                )
            })
            .collect();
        assert_eq!(
            sorting,
            [
                ("host".into(), false, false),
                ("status".into(), false, false),
                ("cache".into(), false, false),
                ("method".into(), false, false),
                ("ts_ms".into(), false, false),
                ("timestamp".into(), false, false)
            ]
        );
    }
    let ips: Vec<String> = batches(&bytes)
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
    assert_eq!(ips, ["a", "b", "c", "d", "legacy-early", "legacy-late"]);
}

#[test]
fn parquet_carries_schema_metadata_and_nulls_mismatched_values() {
    let fake = Fake::new(BASE + 3600);
    let bad = json!({"timestamp": iso(BASE), "ip": "x", "status": "oops", "unknown_key": 5});
    put_raw(&fake, BASE, "m", &[bad.to_string(), "{not json".into()]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.rows, report.bad_lines), (1, 1));
    let key = &fake.keys("processed/request/")[0];
    let bytes = fake.bucket.borrow()[key].clone();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
    let meta = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    assert!(meta
        .iter()
        .any(|kv| kv.key == "flp_schema" && kv.value.as_deref() == Some("v3")));
    assert!(builder.schema().column_with_name("unknown_key").is_none());
    let batch = batches(&bytes).remove(0);
    let status = batch
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(status.is_null(0));
}

#[test]
fn window_size_setting_groups_rows() {
    let fake = Fake::new(BASE + 3600);
    *fake.window.borrow_mut() = Some("300".into());
    let lines: Vec<String> = (0..5)
        .map(|i| line(BASE + i * 60, &format!("r{i}")))
        .collect();
    put_raw(&fake, BASE, "w", &lines);
    tick(&fake).unwrap();
    let files = fake.keys("processed/request/");
    assert_eq!(files.len(), 1);
    let name = ingest::parse_processed(&files[0]).unwrap();
    assert_eq!(
        (name.start, name.end, name.rows),
        (BASE, BASE + 300, Some(5))
    );
}

#[test]
fn crash_before_commit_retries_without_duplicates() {
    for crash in ["put", "watermark"] {
        let fake = Fake::new(BASE + 3600);
        let mut expected = Vec::new();
        for m in 0..3i64 {
            let id = format!("r{m}");
            expected.push(id.clone());
            put_raw(&fake, BASE + m * 60, "x", &[line(BASE + m * 60, &id)]);
        }
        if crash == "put" {
            // PUT 1 is the worker's intent manifest; PUT 2 its first window file. The worker
            // fails, the tick doesn't: the shard is retried by the next tick.
            fake.fail_put_number.set(Some(2));
            assert!(tick(&fake).unwrap().more_pending);
        } else {
            fake.fail_watermark.set(true);
            assert!(tick(&fake).is_err());
            assert!(deleteq(&fake).is_empty(), "uncommitted queue withdrawn");
            fake.now
                .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
        }
        fake.fail_put_number.set(None);
        fake.fail_watermark.set(false);
        let retry = tick(&fake).unwrap();
        assert!(retry.resumed, "{crash}");
        assert_eq!(processed_ips(&fake), expected, "{crash}");
        assert_eq!(fake.keys("processed/request/").len(), 3, "{crash}");
        assert_eq!(fake.keys("processed/summary/request/").len(), 3, "{crash}");
        assert_eq!(tick(&fake).unwrap().converted, 0);
        assert_eq!(processed_ips(&fake), expected, "{crash}");
        assert!(!fake.kv.borrow().keys().any(|k| k.starts_with("ingested:")));
    }
}

#[test]
fn a_stage_reply_counts_as_a_turn_even_when_the_ingest_step_fails() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "x", &[line(BASE, "r0")]);
    fake.fail_watermark.set(true);
    run_tick(&fake);
    let runs = fake.stage_runs.borrow().clone();
    assert_eq!(runs.len(), 1, "the stage ran in the ingest batch");
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(state["maint_misses"], 0, "not counted as a miss");
    assert_eq!(state["maint_next"], runs[0]["next"]);
}

#[test]
fn a_stale_stage_reply_never_moves_maint_next_backwards() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    let start = fake.kv.borrow()["cron:state"].1["maint_next"]
        .as_u64()
        .unwrap();
    let raced = (start + 2) % 4;
    fake.race_maint_next.set(Some(raced));
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(fake.stage_runs.borrow().len(), 2);
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(
        state["maint_next"], raced,
        "the concurrent writer's value stands"
    );
}

#[test]
fn a_cron_state_save_that_loses_twice_is_an_error_not_silence() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    fake.race_cron_state.set(2);
    fake.now.set(fake.now.get() + 120);
    fake.new_request();
    let result = fastly_log_analytics::admin::record_tick(&fake, "clock", "clock");
    assert!(result.is_err(), "{result:?}");
}

#[test]
fn backfill_retained_quarantine_is_not_attempted_again_after_queue_consumption() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    assert!(deleteq(&fake).is_empty());
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    let reads = fake.cdn_gets.borrow().len();
    let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();

    let report = backfill(&fake, "");

    assert_eq!(
        (report.converted, report.failed, report.shards_dispatched),
        (0, 0, 0)
    );
    assert_eq!(fake.cdn_gets.borrow().len(), reads);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert!(!fake.bucket.borrow().contains_key(&good));
    assert_eq!(processed_ips(&fake), vec!["good".to_owned()]);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1, watermark);
    let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
    assert_eq!(
        (job["scanned"].as_u64(), job["planned"].as_u64()),
        (Some(1), Some(0))
    );
    assert_eq!(job["after_key"], bad);
    assert_eq!(job["status"], "complete");
}

#[test]
fn backfill_ineligible_pages_do_not_list_or_read_known_records() {
    for case in ["empty", "invalid", "unsettled", "to_key"] {
        let fake = Fake::new(BASE + 3600);
        let key = match case {
            "empty" => None,
            "invalid" => Some("raw/request/invalid.txt".to_owned()),
            "unsettled" => Some(raw_key(BASE + 3600, "fresh")),
            _ => Some(raw_key(BASE, "boundary")),
        };
        if let Some(key) = &key {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        set_watermark(&fake, &raw_key(BASE + 7200, "watermark"));
        for prefix in [ingest::DELETEQ_PREFIX, ingest::QUARANTINE_PREFIX] {
            fake.kv
                .borrow_mut()
                .insert(format!("{prefix}probe"), (1, json!({"keys": []})));
        }
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let report = backfill(
            &fake,
            if case == "to_key" {
                key.as_deref().unwrap()
            } else {
                ""
            },
        );
        assert_eq!(
            (report.listed, report.converted, report.shards_dispatched),
            (0, 0, 0),
            "{case}"
        );
        assert_eq!(report.backend_calls, 6, "{case}");
        assert_eq!(
            fake.kv_reads
                .borrow()
                .iter()
                .filter(|key| {
                    key.contains(ingest::DELETEQ_PREFIX) || key.contains(ingest::QUARANTINE_PREFIX)
                })
                .count(),
            0,
            "{case}: no known-key LIST or record GET is needed"
        );
        assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1, watermark);
        let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
        assert_eq!(job["status"], "complete", "{case}");
        assert_eq!(job["after_key"], "", "{case}");
        assert_eq!(job["scanned"], 0, "{case}");
        assert_eq!(job["planned"], 0, "{case}");
    }
}

#[test]
fn backfill_eligible_candidates_read_fresh_known_records_and_keep_legacy_shapes() {
    for shape in ["keys", "held", "raw_key", "objects"] {
        let fake = Fake::new(BASE + 3600);
        let key = put_raw(&fake, BASE, "known", &[line(BASE, "duplicate")]);
        set_watermark(&fake, &raw_key(BASE + 60, "watermark"));
        let value = match shape {
            "keys" => json!({"keys": [key]}),
            "held" => json!({"held": [{"key": key}]}),
            "raw_key" => json!({"raw_key": key}),
            _ => json!({"objects": [{"key": key}]}),
        };
        let record_key = format!("{}known", ingest::QUARANTINE_PREFIX);
        fake.kv.borrow_mut().insert(record_key.clone(), (2, value));
        let report = backfill(&fake, "");
        assert_eq!(
            (report.converted, report.shards_dispatched),
            (0, 0),
            "{shape}"
        );
        assert_eq!(report.backend_calls, 9, "{shape}");
        let reads = fake.kv_reads.borrow();
        assert_eq!(
            reads
                .iter()
                .filter(|key| key.as_str() == "list:ingest:deleteq:")
                .count(),
            1
        );
        assert_eq!(
            reads
                .iter()
                .filter(|key| key.as_str() == "list:ingest:quarantine:")
                .count(),
            1
        );
        assert_eq!(reads.iter().filter(|key| *key == &record_key).count(), 1);
        let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
        assert_eq!(job["after_key"], key);
        assert_eq!(
            (job["scanned"].as_u64(), job["planned"].as_u64()),
            (Some(1), Some(0))
        );
    }
}

#[test]
fn backfill_deep_relist_converts_missed_raw_files_without_regressing_watermark() {
    let fake = Fake::new(BASE + 3600);
    // Normal tick ingests minute +120 ("late") first, advancing watermark past BASE ("missed").
    let late_key = put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "ip_late")]);
    let r1 = tick(&fake).unwrap();
    assert_eq!(r1.converted, 1);
    let wm_before = fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(wm_before, late_key);

    // A historical raw file arrives at BASE (behind watermark). Normal forward tick skips it.
    let _missed_key = put_raw(&fake, BASE, "missed", &[line(BASE, "ip_missed")]);
    assert_eq!(tick(&fake).unwrap().converted, 0);

    // Enqueue a deep re-LIST backfill job.
    let job = ingest::BackfillJob {
        status: "running".into(),
        actor: "admin".into(),
        from_ts: None,
        to_ts: None,
        after_key: String::new(),
        to_key: String::new(),
        scanned: 0,
        planned: 0,
        started_at: fake.now.get(),
        updated_at: fake.now.get(),
    };
    fake.kv.borrow_mut().insert(
        ingest::BACKFILL_KEY.into(),
        (1, serde_json::to_value(&job).unwrap()),
    );

    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let mut opts = ingest::coordinator::Options::request(&fake);
    opts.allow_backfill = true;
    let r2 = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &opts,
    )
    .unwrap();
    assert_eq!(r2.converted, 1);
    assert_eq!(
        processed_ips(&fake),
        vec!["ip_late".to_owned(), "ip_missed".to_owned()]
    );
    let wm_after = fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        wm_after, wm_before,
        "watermark must not regress when backfilling older keys"
    );
}

#[test]
fn a_newer_plan_version_is_left_alone() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE, "x", &[line(BASE, "r0")]);
    let newer = json!({"version": 4, "plan_id": "p", "shards": []});
    fake.kv
        .borrow_mut()
        .insert("ingest:plan".into(), (7, newer.clone()));
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert_eq!(report.converted, 0);
    assert_eq!(fake.kv.borrow()["ingest:plan"], (7, newer));
    assert!(fake.keys("processed/request/").is_empty());
}

#[test]
fn ingest_plans_and_dispatches_nothing_past_the_dispatch_deadline() {
    let fake = Fake::new(BASE + 7200);
    for i in 0..4 {
        let at = BASE + i * 60;
        put_raw(&fake, at, "o", &[line(at, &format!("w{i}"))]);
    }
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(first.more_pending);
    assert!(!fake.kv.borrow().contains_key(ingest::PLAN_KEY));
    assert_eq!(fake.worker_runs.get(), 0);
    fake.elapsed_s.set(0);
    for _ in 0..4 {
        tick(&fake).unwrap();
    }
    assert_eq!(processed_ips(&fake), ["w0", "w1", "w2", "w3"]);
}

#[test]
fn backlog_names_unconverted_raw_only() {
    let fake = Fake::new(BASE + 7200);
    for i in 0..4 {
        let at = BASE + i * 60;
        put_raw(&fake, at, "o", &[line(at, &format!("w{i}"))]);
    }
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let deferred = tick(&fake).unwrap();
    assert!(deferred.more_pending && deferred.backlog);
    fake.elapsed_s.set(0);
    let mut last = deferred;
    for _ in 0..8 {
        last = tick(&fake).unwrap();
        if processed_ips(&fake).len() == 4 {
            break;
        }
    }
    assert_eq!(processed_ips(&fake), ["w0", "w1", "w2", "w3"]);
    assert!(
        !last.backlog,
        "commit of the last raw object leaves no backlog"
    );
    // Recent-window scans and sweeps keep the scheduler ticking but name no unconverted raw.
    for _ in 0..3 {
        let idle = tick(&fake).unwrap();
        assert_eq!(idle.converted, 0);
        assert!(!idle.backlog);
    }
}

#[test]
fn immutable_requests_commit_before_the_closed_partition_allowance() {
    let fake = Fake::new(BASE + 60 + ingest::SETTLE_S as i64 - 1);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(processed_ips(&fake), ["a"]);
    fake.now.set(BASE as u64 + 60 + ingest::SETTLE_S);
    assert_eq!(tick(&fake).unwrap().converted, 0);
}

#[test]
fn settle_waits_for_the_log_period_so_a_late_upload_is_still_planned() {
    let mut svc = service();
    svc.log_period = 300;
    let fake = Fake::new(BASE + 60 + ingest::SETTLE_S as i64);
    put_raw(&fake, BASE, "b", &[line(BASE, "b")]);
    tick_service(&fake, &svc).unwrap();
    // Another edge node's 300 s batch for the same minute lands 200 s after the minute closed,
    // under a key that sorts before the first one.
    fake.now.set(BASE as u64 + 60 + 200);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    tick_service(&fake, &svc).unwrap();
    fake.now.set(BASE as u64 + 300 + ingest::SETTLE_S);
    for _ in 0..3 {
        tick_service(&fake, &svc).unwrap();
    }
    assert_eq!(processed_ips(&fake), ["a", "b"]);
}

#[test]
fn a_list_page_without_gz_keys_still_advances_the_watermark() {
    let fake = Fake::new(BASE + 3600);
    let mut last = String::new();
    for i in 0..ingest::LIST_PAGE {
        last = raw_key(BASE, &format!("n{i:03}")).replace(".zst", ".json");
        fake.bucket
            .borrow_mut()
            .insert(last.clone(), b"{}".to_vec());
    }
    put_raw(&fake, BASE + 60, "z", &[line(BASE + 60, "z")]);
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(first.more_pending);
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"].as_str(),
        Some(last.as_str())
    );
    tick(&fake).unwrap();
    assert_eq!(processed_ips(&fake), ["z"]);
}

#[test]
fn the_sweep_ingests_an_object_uploaded_after_the_watermark_passed_its_key_once() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    let seen = put_raw(&fake, BASE, "c", &[line(BASE, "c")]);
    fake.mtimes.borrow_mut().insert(seen, BASE + 3590);
    assert_eq!(tick(&fake).unwrap().converted, 2);
    let late = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.mtimes.borrow_mut().insert(late.clone(), BASE + 3700);
    fake.now.set(BASE as u64 + 3800);
    assert_eq!(tick(&fake).unwrap().converted, 0);

    let report = sweep_tick(&fake);
    assert_eq!((report.stragglers, report.converted), (1, 1));
    assert!(report.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    assert_eq!(processed_ips(&fake), ["a", "b", "c"]);
    let files = rows(&fake, LogKind::IngestFiles);
    let mut statuses: Vec<&str> = files
        .iter()
        .filter(|row| row["raw_key"] == late.as_str())
        .map(|row| row["status"].as_str().unwrap())
        .collect();
    statuses.sort_unstable();
    assert_eq!(statuses, ["converted", "straggler"]);
    let latest = telemetry::latest_ingest_state(&files);
    let state = latest.iter().find(|row| row["raw_key"] == late.as_str());
    assert_eq!(state.unwrap()["status"], "converted");

    for _ in 0..3 {
        fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
        let again = sweep_tick(&fake);
        assert_eq!((again.stragglers, again.converted), (0, 0));
    }
    assert_eq!(processed_ips(&fake), ["a", "b", "c"]);
}

#[test]
fn the_sweep_lists_at_most_two_pages_a_tick_and_resumes_from_its_cursor() {
    let fake = Fake::new(BASE + 3600);
    let pages = ingest::SWEEP_LISTS * ingest::LIST_PAGE;
    for i in 0..pages + 20 {
        let key = raw_key(BASE, &format!("n{i:04}")).replace(".zst", ".json");
        fake.bucket.borrow_mut().insert(key, b"{}".to_vec());
    }
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    while tick(&fake).unwrap().more_pending {}
    assert_eq!(processed_ips(&fake), ["b"]);
    let late = put_raw(&fake, BASE, "z", &[line(BASE, "z")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 3700);
    fake.now.set(BASE as u64 + 3800);

    let first = sweep_tick(&fake);
    assert_eq!(first.stragglers, 0);
    assert!(first.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    let cursor = fake.kv.borrow()[ingest::WATERMARK_KEY].1["sweep"]["after"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        cursor,
        raw_key(BASE, &format!("n{:04}", pages - 1)).replace(".zst", ".json")
    );

    fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    let second = sweep_tick(&fake);
    assert_eq!((second.stragglers, second.converted), (1, 1));
    assert_eq!(processed_ips(&fake), ["b", "z"]);
}

#[test]
fn below_floor_compares_keys_only_against_a_string_floor() {
    let floor = raw_floor("request", BASE + 60);
    let below = raw_key(BASE, "a");
    let at = raw_key(BASE + 60, "a");
    let watermark = json!({ "floor_key": floor });
    assert!(ingest::below_floor(&below, Some(&watermark)));
    assert!(!ingest::below_floor(&at, Some(&watermark)));
    assert!(!ingest::below_floor(&below, None));
    assert!(!ingest::below_floor(&below, Some(&json!({}))));
    assert!(!ingest::below_floor(
        &below,
        Some(&json!({ "floor_key": 7 }))
    ));
}

#[test]
fn floor_blocks_start_after_listing_below_reset_minute() {
    let fake = Fake::new(BASE + 3600);
    let reset = BASE + 600;
    put_raw(&fake, reset - 60, "old", &[line(reset - 60, "old")]);
    let new = put_raw(&fake, reset, "new", &[line(reset, "new")]);
    let floor = raw_floor("request", reset);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    for _ in 0..8 {
        if !tick(&fake).unwrap().more_pending {
            break;
        }
    }
    assert_eq!(processed_ips(&fake), ["new"]);
    assert_eq!(queued_keys(&fake), std::slice::from_ref(&new));
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"].as_str(),
        Some(new.as_str())
    );
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_recent_scan_candidates() {
    let reset = BASE + 600;
    let fake = Fake::new(reset + 120);
    put_raw(&fake, reset - 60, "old", &[line(reset - 60, "old")]);
    let fresh = put_raw(&fake, reset, "z", &[line(reset, "z")]);
    let floor = raw_floor("request", reset);
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (
            1,
            json!({"key": raw_key(reset, "a"), "run_id": "abcdefabcdef", "raw_sources": {},
                   "raw_scan_due": true, "floor_key": floor}),
        ),
    );
    for _ in 0..6 {
        sweep_tick(&fake);
        fake.now.set(fake.now.get() + 1);
    }
    assert_eq!(processed_ips(&fake), ["z"]);
    assert_eq!(queued_keys(&fake), [fresh]);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_straggler_sweep() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    let seen = put_raw(&fake, BASE, "c", &[line(BASE, "c")]);
    fake.mtimes.borrow_mut().insert(seen, BASE + 3590);
    assert_eq!(tick(&fake).unwrap().converted, 2);
    let floor = raw_floor("request", BASE + 60);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    let late = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 3700);
    fake.now.set(BASE as u64 + 3800);
    assert_eq!(tick(&fake).unwrap().converted, 0);
    for _ in 0..3 {
        let report = sweep_tick(&fake);
        assert_eq!((report.stragglers, report.converted), (0, 0));
        fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    }
    assert_eq!(processed_ips(&fake), ["b", "c"]);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_backfill() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "ip_late")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    let floor = raw_floor("request", BASE + 60);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    put_raw(&fake, BASE, "missed", &[line(BASE, "ip_missed")]);
    let report = backfill(&fake, "");
    assert_eq!((report.listed, report.converted), (0, 0));
    assert_eq!(processed_ips(&fake), ["ip_late"]);
    assert_eq!(
        fake.kv.borrow()[ingest::BACKFILL_KEY].1["status"],
        "complete"
    );
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_rum_planning() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    put_rum(&fake, BASE, "old", &[rum_beacon(BASE + 5, "LCP", "100")]);
    let new = put_rum(
        &fake,
        BASE + 60,
        "new",
        &[rum_beacon(BASE + 65, "LCP", "200")],
    );
    let key = ingest::watermark_key("rum");
    let floor = raw_floor("rum", BASE + 60);
    set_floor(&fake, &key, &floor);
    for _ in 0..6 {
        if !rum_tick(&fake, &svc).unwrap().more_pending {
            break;
        }
    }
    assert_eq!(queued_keys(&fake), std::slice::from_ref(&new));
    assert_eq!(fake.kv.borrow()[&key].1["key"].as_str(), Some(new.as_str()));
    fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    rum_tick(&fake, &svc).unwrap();
    assert_eq!(queued_keys(&fake), [new]);
    assert_eq!(floor_of(&fake, &key), Some(floor));
}

#[test]
fn floor_key_survives_every_watermark_writer() {
    let fake = Fake::new(BASE + 600);
    let floor = raw_floor("request", BASE);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    let wm = || fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();

    // A legacy v1 plan's `finish`.
    let old = put_raw(&fake, BASE + 300, "old", &[line(BASE + 300, "old")]);
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (
            3,
            json!({"run_id": "abcdefabcdef", "window_s": 60, "schema": "v3",
                "raw_keys": [old], "created_at": 1}),
        ),
    );
    assert!(tick(&fake).unwrap().resumed);
    assert_eq!(wm()["run_id"], "abcdefabcdef");
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // A plan and its commit.
    let new = put_raw(&fake, BASE + 360, "new", &[line(BASE + 360, "new")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(wm()["key"].as_str(), Some(new.as_str()));
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `plan_new` passing a page without raw keys.
    let passed = raw_key(BASE + 420, "n").replace(".zst", ".json");
    fake.bucket
        .borrow_mut()
        .insert(passed.clone(), b"{}".to_vec());
    tick(&fake).unwrap();
    assert_eq!(wm()["key"].as_str(), Some(passed.as_str()));
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // The straggler sweep moving its cursor with nothing found.
    fake.now.set(BASE as u64 + 720);
    sweep_tick(&fake);
    assert_eq!(wm()["sweep"]["at"], BASE + 720);
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `recent_candidates` clearing a due scan whose cursor isn't recent.
    {
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::WATERMARK_KEY).unwrap();
        entry.0 += 1;
        entry.1["raw_sources"] = json!({});
        entry.1["raw_scan_due"] = true.into();
    }
    sweep_tick(&fake);
    assert_eq!(wm()["raw_scan_due"], false);
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `recent_candidates` finishing a recent scan with no candidates.
    {
        let now = fake.now.get() as i64;
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::WATERMARK_KEY).unwrap();
        entry.0 += 1;
        entry.1["key"] = raw_key(now - now.rem_euclid(60) - 60, "zzz").into();
        entry.1["raw_scan_due"] = true.into();
        entry.1["raw_scan"] = "".into();
    }
    sweep_tick(&fake);
    assert_eq!(wm()["raw_scan_due"], false);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn tick_is_bounded_and_continues_next_tick() {
    let fake = Fake::new(BASE + 7200);
    let capacity = ingest::coordinator::MAX_FANOUT * ingest::worker::SHARD_MAX_OBJECTS;
    let mut expected = Vec::new();
    for i in 0..capacity + 40 {
        let id = format!("r{i:03}");
        expected.push(id.clone());
        let minute = BASE + (i / 25) as i64 * 60;
        put_raw(&fake, minute, &format!("o{i:03}"), &[line(minute, &id)]);
    }
    expected.sort();
    let first = tick(&fake).unwrap();
    assert!(first.converted as usize <= capacity);
    assert!(first.converted > 0);
    assert!(first.more_pending);
    assert!(first.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    for _ in 0..6 {
        tick(&fake).unwrap();
    }
    assert_eq!(processed_ips(&fake), expected);
    assert!(fake.worker_max_calls.get() <= BUDGET);
}

#[test]
fn corrupt_objects_are_recorded_and_skipped() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.failed, report.converted), (1, 1));
    assert_eq!(processed_ips(&fake), vec!["good"]);
    assert_eq!(tick(&fake).unwrap().listed, 0);
}

#[test]
fn an_object_spanning_too_many_windows_is_quarantined_without_blocking_others() {
    let fake = Fake::new(BASE + 7200);
    let spread: Vec<String> = (0..ingest::MAX_WINDOWS_PER_OBJECT as i64 + 1)
        .map(|i| line(BASE + i * 60, &format!("w{i}")))
        .collect();
    put_raw(&fake, BASE, "spread", &spread);
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.failed, report.converted), (1, 1));
    assert_eq!(processed_ips(&fake), vec!["good"]);
    let errors = rows(&fake, LogKind::PlatformErrors);
    assert!(errors
        .iter()
        .any(|r| r["code"] == "too_many_windows" && r["error_domain"] == "ingest_corrupt"));
    let files = rows(&fake, LogKind::IngestFiles);
    let spread_row = files
        .iter()
        .find(|r| r["raw_key"].as_str().unwrap().ends_with("spread.zst"))
        .unwrap();
    assert_eq!(spread_row["status"], "quarantined");
    assert_eq!(tick(&fake).unwrap().listed, 0);
    assert!(deleteq(&fake)
        .iter()
        .all(|q| q["keys"].to_string().contains("ok.zst")
            && !q["keys"].to_string().contains("spread")));
}

#[test]
fn cumulative_decompressed_bytes_per_tick_are_capped_and_the_rest_defers() {
    let fake = Fake::new(BASE + 7200);
    let big = |at: i64, tag: &str| -> Vec<String> {
        (0..130)
            .map(|i| line(at, &format!("{tag}{i}-{}", "x".repeat(200_000))))
            .collect()
    };
    for i in 0..3 {
        put_raw(
            &fake,
            BASE + i * 60,
            "big",
            &big(BASE + i * 60, &format!("b{i}")),
        );
    }
    // One shard cannot hold all three in memory: it splits before any PUT, until each half fits.
    let mut converted = 0;
    for _ in 0..4 {
        let report = tick(&fake).unwrap();
        assert_eq!(report.failed, 0);
        converted += report.converted;
    }
    assert_eq!(converted, 3);
    assert_eq!(fake.keys("processed/request/").len(), 3);
    assert!(fake.keys("processed/_inflight/").is_empty() || !deleteq(&fake).is_empty());
}

#[test]
fn converted_raw_keys_including_fresh_objects_are_queued_but_failed_objects_are_not() {
    let fake = Fake::new(BASE + 3600);
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"not gzip".to_vec());
    let good = put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let fresh = put_raw(&fake, BASE + 3590, "new", &[line(BASE + 3590, "fresh")]);
    tick(&fake).unwrap();
    let queues = deleteq(&fake);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["keys"], json!([good, fresh]));
    assert_eq!(queues[0]["sizes"].as_array().unwrap().len(), 2);
    assert!(!fake.kv.borrow().keys().any(|k| k.starts_with("ingested:")));
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&bad) && bucket.contains_key(&good) && bucket.contains_key(&fresh));
    drop(bucket);
    assert_eq!(processed_ips(&fake), vec!["fresh", "good"]);
}

#[test]
fn a_tick_never_reads_or_lists_the_old_ledger() {
    let fake = Fake::new(BASE + 7200);
    for minute in 0..6 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "a")],
        );
    }
    for _ in 0..3 {
        tick(&fake).unwrap();
    }
    assert!(fake
        .kv_reads
        .borrow()
        .iter()
        .all(|k| !k.contains("ingested:") && !k.contains("ingest:sweep")));
    assert!(fake.kv.borrow().get("ingest:sweep").is_none());
}

#[test]
fn watermark_alone_is_not_proof_for_deletion() {
    let fake = Fake::new(BASE + 7200);
    let old = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    put_raw(&fake, BASE + 60, "a", &[line(BASE + 60, "b")]);
    tick(&fake).unwrap();
    let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
    assert!(watermark["key"].as_str().unwrap() >= old.as_str());
    fake.kv
        .borrow_mut()
        .retain(|k, _| !k.starts_with("ingest:deleteq:"));
    for _ in 0..3 {
        tick(&fake).unwrap();
    }
    assert_eq!(fake.keys("raw/request/").len(), 2);
}

#[test]
fn backlog_below_the_watermark_drains_across_ticks_without_reingesting() {
    let fake = Fake::new(BASE + 7200);
    for minute in 0..40 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "a")],
        );
    }
    let mut converted = 0;
    for _ in 0..12 {
        converted += tick(&fake).unwrap().converted;
    }
    assert_eq!(converted, 40);
    assert_eq!(processed_ips(&fake).len(), 40);
    let queued: usize = deleteq(&fake)
        .iter()
        .map(|q| q["keys"].as_array().unwrap().len())
        .sum();
    assert_eq!(queued, 40);
}

#[test]
fn ingest_files_rows_are_one_per_raw_object_in_one_outbox_key() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    let good = put_raw(
        &fake,
        BASE + 60,
        "ok",
        &[line(BASE + 60, "g"), "{oops".into(), line(BASE + 60, "h")],
    );
    let report = tick(&fake).unwrap();
    assert_eq!(
        (report.lines_read, report.rows, report.bad_lines),
        (3, 2, 1)
    );
    assert_eq!(report.lines_read, report.rows + report.bad_lines);
    let outbox: Vec<String> = fake
        .kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with(&format!("logq_{SID}_")))
        .cloned()
        .collect();
    assert_eq!(outbox.len(), 1);
    let rows = rows(&fake, LogKind::IngestFiles);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["state_version"] == 1));
    let converted = rows.iter().find(|r| r["raw_key"] == good.as_str()).unwrap();
    assert_eq!(converted["status"], "converted");
    assert_eq!(converted["rows"], 2);
    assert_eq!(converted["bad_lines"], 1);
    assert_eq!(converted["lines_read"], 3);
    let text = [line(BASE + 60, "g"), "{oops".into(), line(BASE + 60, "h")]
        .join("\n")
        .len()
        + 1;
    assert_eq!(
        converted["raw_bytes"], text,
        "decompressed bytes, beside raw_size, give the real ratio"
    );
    let outputs: Value =
        serde_json::from_str(converted["output_keys_json"].as_str().unwrap()).unwrap();
    assert_eq!(outputs.as_array().unwrap().len(), 1);
    let failed = rows.iter().find(|r| r["status"] != "converted").unwrap();
    assert_eq!(failed["status"], "quarantined");
    assert_eq!(failed["reason"], "corrupt_gzip");
}

#[test]
fn corrupt_gzip_and_bad_lines_emit_bounded_platform_errors() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    let huge = format!("{{broken {}", "x".repeat(10_000));
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "g"), huge]);
    tick(&fake).unwrap();
    let errors = rows(&fake, LogKind::PlatformErrors);
    let codes: BTreeSet<&str> = errors.iter().filter_map(|r| r["code"].as_str()).collect();
    assert_eq!(codes, BTreeSet::from(["bad_ndjson_line", "corrupt_gzip"]));
    for row in &errors {
        assert_eq!(row["error_domain"], "ingest_corrupt");
        assert!(row["sample_json"].as_str().is_none_or(|s| s.len() <= 2048));
    }
    let bad = errors
        .iter()
        .find(|r| r["code"] == "bad_ndjson_line")
        .unwrap();
    assert!(bad["sample_json"].as_str().unwrap().contains("broken"));
}

#[test]
fn bad_line_samples_stay_valid_json_within_the_field_cap() {
    let fake = Fake::new(BASE + 3600);
    let dense = format!("{{{}", "\"\\\u{1}é".repeat(1500));
    let long = format!("{{broken {}", "x".repeat(10_000));
    put_raw(&fake, BASE + 60, "a", &[dense]);
    put_raw(&fake, BASE + 120, "b", &[long]);
    tick(&fake).unwrap();
    let errors = rows(&fake, LogKind::PlatformErrors);
    let samples: Vec<_> = errors
        .iter()
        .filter(|r| r["code"] == "bad_ndjson_line")
        .collect();
    assert_eq!(samples.len(), 2);
    for row in samples {
        let text = row["sample_json"].as_str().unwrap();
        assert!(text.len() <= 2048, "{}", text.len());
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert!(parsed["line"].as_str().is_some_and(|l| l.starts_with('{')));
    }
}

#[test]
fn cron_metrics_count_successful_raw_objects_without_quarantined_objects() {
    for kind in ["request_ingest", "rum_ingest"] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let bad = if kind == "rum_ingest" {
            store_service(&fake, &rum_service());
            put_rum(
                &fake,
                BASE + 60,
                "ok",
                &[rum_beacon(BASE + 60, "LCP", "123")],
            );
            rum_key(BASE, "bad")
        } else {
            put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
            raw_key(BASE, "bad")
        };
        fake.bucket.borrow_mut().insert(bad, b"not gzip".to_vec());
        for _ in 0..8 {
            run_tick(&fake);
            if rows(&fake, LogKind::CronRuns)
                .iter()
                .any(|row| row["cron_kind"] == kind && row["converted_files"] == 1)
            {
                break;
            }
            fake.now.set(fake.now.get() + 1);
        }
        let runs = rows(&fake, LogKind::CronRuns);
        let run = runs
            .iter()
            .find(|r| r["cron_kind"] == kind && r["converted_files"] == 1)
            .unwrap();
        assert_eq!(run["listed_files"], 2, "{kind}");
        assert_eq!(run["converted_files"], 1, "{kind}");
        assert_eq!(run["rows_written"], 1, "{kind}");
        assert_eq!(
            runs.iter()
                .filter(|row| row["cron_kind"] == kind)
                .filter_map(|row| row["converted_files"].as_u64())
                .sum::<u64>(),
            1
        );
    }
}

#[test]
fn cron_metrics_leave_unmeasured_bytes_null_but_preserve_successful_zero_counts() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    let runs = rows(&fake, LogKind::CronRuns);
    let run = runs
        .iter()
        .find(|r| r["cron_kind"] == "request_ingest")
        .unwrap();
    assert_eq!(run["outcome"], "idle");
    assert_eq!(run.get("bytes_read"), Some(&Value::Null));
    for field in [
        "listed_files",
        "converted_files",
        "deleted_files",
        "lines_read",
        "rows_written",
        "bad_lines",
        "bytes_written",
        "parquet_files_written",
    ] {
        assert_eq!(run[field], 0, "{field}");
    }
    let parquet =
        telemetry::schema::to_parquet(LogKind::CronRuns, std::slice::from_ref(run)).unwrap();
    let archived = batches(&parquet);
    assert!(archived[0].column_by_name("bytes_read").unwrap().is_null(0));
    let decoded = telemetry::schema::from_parquet(LogKind::CronRuns, parquet).unwrap();
    assert_eq!(decoded[0].get("bytes_read"), None);
    assert_eq!(decoded[0]["rows_written"], 0);
}

#[test]
fn cron_metrics_errors_leave_unknown_counts_null_and_keep_reported_progress() {
    for partial in [false, true] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        if partial {
            put_raw(&fake, BASE, "ok", &[line(BASE, "ok")]);
            fake.fail_watermark.set(true);
        } else {
            fake.kv.borrow_mut().insert(
                ingest::PLAN_KEY.into(),
                (1, json!({"version": ingest::coordinator::PLAN_VERSION})),
            );
        }
        freshness::finish_tick(&fake);
        let runs = rows(&fake, LogKind::CronRuns);
        let run = runs
            .iter()
            .find(|r| r["cron_kind"] == "request_ingest" && r["outcome"] == "error")
            .unwrap();
        assert_eq!(run["outcome"], "error", "{run}");
        assert_eq!(
            run.get("listed_files"),
            Some(&if partial { json!(1) } else { Value::Null })
        );
        for field in [
            "converted_files",
            "deleted_files",
            "lines_read",
            "rows_written",
            "bad_lines",
            "bytes_read",
            "bytes_written",
            "parquet_files_written",
        ] {
            assert_eq!(run.get(field), Some(&Value::Null), "{field}");
        }
        assert!(run["backend_requests_used"].as_u64().unwrap() > 0);
        let parquet =
            telemetry::schema::to_parquet(LogKind::CronRuns, std::slice::from_ref(run)).unwrap();
        let archived = batches(&parquet);
        assert!(archived[0]
            .column_by_name("rows_written")
            .unwrap()
            .is_null(0));
        let decoded = telemetry::schema::from_parquet(LogKind::CronRuns, parquet).unwrap();
        assert_eq!(decoded[0].get("rows_written"), None);
        assert_eq!(decoded[0]["listed_files"], run["listed_files"]);
    }
}

#[test]
fn record_tick_logs_only_runs_carries_idle_ticks_and_keeps_fifty_runs() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    let committed_at = fake.kv.borrow()["cron:state"].1["last_ingest_at"].clone();
    assert!(
        rows(&fake, LogKind::CronTicks).is_empty(),
        "cron_ticks is retired"
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let deferred_before = fastly_log_analytics::admin::schedule::peek(
        &fake,
        fastly_log_analytics::admin::schedule::SCHED_KEY,
    )
    .unwrap()
    .deferred_idle
    .len();
    let ingest_runs: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    assert_eq!(
        ingest_runs
            .iter()
            .filter(|run| run["converted_files"] == 1)
            .count(),
        1
    );
    assert!(ingest_runs
        .iter()
        .all(|run| run["vcpu_ms"].as_i64().unwrap() > 0));
    let converted = ingest_runs
        .iter()
        .find(|run| {
            run["details_json"].as_str().is_some_and(|details| {
                serde_json::from_str::<Value>(details).unwrap()["summary_vcpu_ms"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
            })
        })
        .unwrap();
    let details: Value = serde_json::from_str(converted["details_json"].as_str().unwrap()).unwrap();
    let worker = details["worker_vcpu_ms"].as_u64().unwrap();
    let summary = details["summary_vcpu_ms"].as_u64().unwrap();
    assert!(summary > 0 && summary < worker, "{details}");

    for _ in 0..30 {
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        assert_eq!(fake.calls.get(), 0, "an idle tick makes no backend call");
    }
    assert_eq!(rows(&fake, LogKind::CronRuns).len(), runs.len());
    fake.now.set(fake.now.get() + 60);
    run_tick(&fake);
    let mut carried: Vec<_> = rows(&fake, LogKind::CronRuns)
        .into_iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    carried.extend(
        fastly_log_analytics::admin::schedule::peek(
            &fake,
            fastly_log_analytics::admin::schedule::SCHED_KEY,
        )
        .unwrap()
        .deferred_idle
        .into_iter()
        .map(|run| run.row),
    );
    assert_eq!(
        carried.len(),
        ingest_runs.len() + deferred_before + 1,
        "a scheduled ingest is logged even when idle"
    );
    let idle = carried
        .iter()
        .find(|r| r["outcome"] == "idle" && r["idle_ticks"] == 30)
        .unwrap();
    assert_eq!(idle["idle_ticks"], 30);
    assert!(idle["idle_vcpu_ms"].as_i64().unwrap() > 0);

    for _ in 0..55 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
    }
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(state["runs"].as_array().unwrap().len(), 50);
    assert!(
        state["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["converted_raw_files"] == 0),
        "later runs have pushed the converting run out of the ring buffer"
    );
    assert_eq!(
        state["last_ingest_at"], committed_at,
        "the last ingest outlives the ring buffer"
    );
    assert!(fastly_log_analytics::admin::last_ingest_at(&fake, Some(SID)).is_some());
    assert_eq!(
        fastly_log_analytics::admin::last_ingest_at(&fake, Some("OtherServiceId0000000000")),
        None,
        "another service must not inherit this service's last ingest"
    );
    assert_eq!(
        fastly_log_analytics::admin::last_ingest_at(&fake, None),
        None
    );
}

#[test]
fn capacity_telemetry_preserves_worker_costs_and_missing_replies() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(37));
    for i in 0..(2 * ingest::worker::SHARD_MAX_OBJECTS + 1) {
        put_raw(&fake, BASE, &format!("sample-{i}"), &[line(BASE, "a")]);
    }
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::Ok,
        WorkerFault::LostReply,
        WorkerFault::Ok,
    ]);
    fake.transient.borrow_mut().push_back(("GET", 503, None));
    let report = tick(&fake).unwrap();
    let value = serde_json::to_value(&report).unwrap();
    let samples = value["worker_executions_metrics"].as_array().unwrap();
    let shards: Vec<_> = samples
        .iter()
        .filter(|sample| sample["kind"] == "request_ingest")
        .collect();
    assert_eq!(shards.len(), report.shards_dispatched as usize);
    assert_eq!(shards.iter().filter(|s| s["answered"] == true).count(), 2);
    let missing = shards.iter().find(|s| s["answered"] == false).unwrap();
    for field in [
        "calls",
        "vcpu_ms",
        "summary_vcpu_ms",
        "duration_ms",
        "heap_mib",
    ] {
        assert!(missing[field].is_null(), "{missing}");
    }
    let measured: Vec<_> = shards.iter().filter(|s| s["answered"] == true).collect();
    assert_ne!(measured[0]["calls"], measured[1]["calls"], "{samples:?}");
    assert!(measured.iter().all(|s| s["heap_mib"] == 37));
    assert_eq!(
        samples
            .iter()
            .filter_map(|s| s["vcpu_ms"].as_u64())
            .sum::<u64>(),
        report.worker_vcpu_ms
    );
}

#[test]
fn capacity_telemetry_keeps_tick_execution_separate_from_worker_totals() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.heap_mib.set(Some(29));
    fake.linear_memory_high_water_bytes
        .set(Some(80 * 1024 * 1024));
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    run_tick(&fake);
    let runs = rows(&fake, LogKind::CronRuns);
    let details: Value = runs
        .iter()
        .find_map(|row| {
            let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
            details.get("execution_metrics").map(|_| details.clone())
        })
        .unwrap();
    assert_eq!(
        details["execution_metrics"]["scope"],
        "tick_before_telemetry_flush"
    );
    assert_eq!(details["execution_metrics"]["heap_mib"], 29);
    assert_eq!(
        details["execution_metrics"]["linear_memory_high_water_bytes"],
        80 * 1024 * 1024
    );
    assert_eq!(details["workers_dispatched"], fake.worker_sent_total.get());
    assert_eq!(details["workers_answered"], details["workers_dispatched"]);
    let samples = details["worker_executions_metrics"].as_array().unwrap();
    assert_eq!(samples.len(), fake.worker_sent_total.get() as usize);
    assert!(samples.iter().any(|sample| sample["kind"] == "maintenance"));
    assert!(samples
        .iter()
        .all(|sample| sample["linear_memory_high_water_bytes"] == 80 * 1024 * 1024));
    assert!(details["execution_metrics"]["vcpu_ms"].as_u64().unwrap() > 0);
    let stage: Value = serde_json::from_str(
        runs.iter()
            .find(|r| r["cron_kind"] == "request_ingest")
            .unwrap()["details_json"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(stage["stage_execution_metrics"]["heap_mib"] == 29);
}

#[test]
fn capacity_telemetry_reports_explicit_zero_fanout_without_inventing_heap() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.shield.set(false);
    let reply = fastly_log_analytics::admin::record_tick(&fake, "manual", "admin").unwrap();
    assert_eq!(reply["execution_metrics"]["scope"], "tick_end");
    assert!(reply["execution_metrics"]["heap_mib"].is_null());
    assert_eq!(
        reply["execution_metrics"].get("linear_memory_high_water_bytes"),
        Some(&Value::Null)
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let details: Value = runs
        .iter()
        .find_map(|row| {
            let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
            details.get("execution_metrics").map(|_| details.clone())
        })
        .unwrap();
    assert_eq!(details["workers_dispatched"], 0);
    assert_eq!(details["workers_answered"], 0);
    assert_eq!(details["worker_executions_metrics"], json!([]));
    assert_eq!(details["root_run_id"], reply["run"]["id"]);
}

#[test]
fn manual_run_manifest_records_when_the_run_finished() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.call_ms.set(1500);
    fake.new_request();
    let started = fake.now.get();
    fastly_log_analytics::admin::record_tick(&fake, "manual", "admin").unwrap();
    let keys = fake.keys("meta/cron-runs/");
    assert_eq!(keys.len(), 1, "{keys:?}");
    let manifest: Value = serde_json::from_slice(&fake.bucket.borrow()[&keys[0]]).unwrap();
    let at = fastly_log_analytics::admin::rfc3339_from_secs;
    let duration_s = manifest["duration_ms"].as_u64().unwrap() / 1000;
    assert!(duration_s > 0, "{manifest}");
    assert_eq!(manifest["started_at"], at(started), "{manifest}");
    assert_eq!(
        manifest["finished_at"],
        at(started + duration_s),
        "{manifest}"
    );
}

#[test]
fn capacity_telemetry_keeps_maintenance_only_worker_coverage_in_root_stdout() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    fake.now.set(fake.now.get() + 1);
    {
        let mut kv = fake.kv.borrow_mut();
        let state = kv.get_mut("cron:state").unwrap();
        state.0 += 1;
        state.1["jobs"]["maint"]["more_pending"] = true.into();
    }
    fake.cache.borrow_mut().clear();
    fake.events.borrow_mut().clear();
    run_tick(&fake);
    let events = fake.events.borrow();
    let event = events
        .iter()
        .find(|event| event["event"] == "cron.execution")
        .unwrap();
    assert_eq!(event["workers_dispatched"], 1);
    assert_eq!(event["workers_answered"], 1);
    assert_eq!(event["worker_executions_metrics"][0]["kind"], "maintenance");
    assert_eq!(event["execution_metrics"]["scope"], "tick_end");
    assert_eq!(event["root_run_id"], event["run_id"]);
}

#[test]
fn cron_runs_count_every_worker_execution_the_tick_dispatched() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
        put_raw(&fake, BASE + i as i64, name, &[line(BASE + i as i64, name)]);
    }
    for _ in 0..6 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    let sent = fake.worker_sent_total.get();
    assert!(sent > 0, "the ticks dispatched worker executions");
    let logged: i64 = rows(&fake, LogKind::CronRuns)
        .iter()
        .map(|r| r["worker_executions"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(
        logged,
        i64::from(sent),
        "each worker execution is logged once"
    );
}

#[test]
fn idle_ticks_and_flush_probes_make_no_backend_call_or_kv_write() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    run_flush(&fake);
    for _ in 0..20 {
        fake.now.set(fake.now.get() + 1);
        let kv = fake.kv.borrow().clone();
        let tick = run_tick(&fake);
        assert_eq!(tick["outcome"], "idle", "{tick}");
        assert_eq!(fake.calls.get(), 0);
        assert_eq!(fake.cache_ops.get(), 1, "one schedule claim");
        let flush = run_flush(&fake);
        assert_eq!(flush["outcome"], "idle", "{flush}");
        assert_eq!(fake.calls.get(), 0);
        assert_eq!(fake.cache_ops.get(), 1);
        assert!(*fake.kv.borrow() == kv, "an idle probe wrote KV");
    }
}

#[test]
fn catch_up_dispatches_in_one_bucket_share_its_outbox_key() {
    let fake = Fake::new(BASE + 3 * 3600);
    tick_runtime(&fake);
    // A few ticks of backlog: every dispatch spends ingest and raw delete down to their reserves,
    // and all of their rows fit one outbox key.
    seed(&fake, 9, 50);
    // Start on an outbox bucket boundary so every dispatch below falls in the same bucket.
    fake.now.set(fake.now.get() / 120 * 120);
    let mut dispatches = 0;
    while !fake.keys("raw/").is_empty() {
        assert!(dispatches < 20, "the catch-up did not finish");
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        dispatches += 1;
    }
    assert!(dispatches >= 3, "a {dispatches}-tick catch-up");
    let keys = outbox_keys(&fake);
    assert_eq!(keys.len(), 1, "{dispatches} dispatches made keys {keys:?}");
}

#[test]
fn a_committed_flush_left_behind_runs_again_on_the_next_probe() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let bucket = (BASE as u64 + 3600) / 120 - 40;
    for i in 0..40 {
        let line = json!({"kind": "ingest_files", "timestamp": (bucket * 120) * 1000 + i,
                          "event_id": format!("f{i:02}"), "service_id": SID,
                          "severity": "info", "raw_key": "k"});
        fake.kv.borrow_mut().insert(
            telemetry::outbox_key(SID, bucket, Some(&format!("x{i:02}"))),
            (1, json!({"ndjson": line.to_string()})),
        );
    }
    let first = run_flush(&fake);
    assert_eq!(first["outcome"], "partial", "{first}");
    assert_eq!(first["more_pending"], true, "{first}");
    let left = outbox_keys(&fake).len();
    assert!(left > 0, "the backlog outlasts one flush");
    fake.now.set(fake.now.get() + 1);
    let next = run_flush(&fake);
    assert_ne!(next["outcome"], "idle", "{next}");
    assert!(outbox_keys(&fake).len() < left);
}

#[test]
fn an_install_without_fos_credentials_backs_off_instead_of_dispatching_every_second() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.no_fos.set(true);
    run_tick(&fake);
    let writes = fake.cron_cas.get();
    for _ in 0..20 {
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
    }
    assert_eq!(
        fake.cron_cas.get(),
        writes,
        "an unprovisioned install wrote cron:state on a 1 s tick"
    );
}

#[test]
fn a_failing_dispatch_backs_off_instead_of_retrying_every_second() {
    use fastly_log_analytics::admin::schedule::RETRY_S;
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    // The cached mirror now says ingest is due again when the next period closes.
    put_raw(&fake, BASE + 3600, "b", &[line(BASE + 3600, "b")]);
    fake.now.set(fake.now.get() + 300);
    fake.kv_reads.borrow_mut().clear();
    fake.fail_cron_state.set(true);
    let dispatches = || {
        fake.kv_reads
            .borrow()
            .iter()
            .filter(|key| *key == "cron:state")
            .count()
    };
    let tick = || {
        fake.new_request();
        fastly_log_analytics::admin::record_tick(&fake, "clock", "clock")
    };
    let start = fake.now.get();
    for _ in 0..20 {
        let _ = tick();
        fake.now.set(fake.now.get() + 1);
    }
    assert_eq!(dispatches(), 1, "dispatches in 20 one-second ticks");
    fake.now.set(start + RETRY_S);
    assert!(tick().is_err());
    assert_eq!(dispatches(), 2, "retried once the backoff passed");
    fake.fail_cron_state.set(false);
    fake.now.set(start + 2 * RETRY_S);
    tick().unwrap();
    freshness::finish_tick(&fake);
    assert_eq!(processed_ips(&fake), ["a", "b"]);
}

#[test]
fn off_shield_probes_return_without_any_call_or_cache_op() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.shield.set(false);
    for probe in [run_tick, run_flush] {
        let value = probe(&fake);
        assert_eq!(value["outcome"], "off_shield", "{value}");
        assert_eq!((fake.calls.get(), fake.cache_ops.get()), (0, 0));
    }
    assert_eq!(fake.keys("raw/").len(), 1);
    assert!(fake.cache.borrow().is_empty());
}

#[test]
fn duplicate_and_concurrent_ticks_run_a_due_job_once() {
    use fastly_log_analytics::admin::schedule::{self, Claim, SCHED_KEY};
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    // Another execution holds the lease: ticks and manual runs back off without a call.
    fake.new_request();
    let held = schedule::claim(&fake, SCHED_KEY, "other", false, schedule::tick_wanted).unwrap();
    assert!(matches!(held, Claim::Run { .. }));
    assert_eq!(run_tick(&fake)["outcome"], "busy");
    assert_eq!(fake.calls.get(), 0);
    fake.new_request();
    assert_eq!(
        fastly_log_analytics::admin::record_tick(&fake, "manual", "admin"),
        Err("cron_busy".into())
    );
    assert_eq!(ingest_runs(&fake), 0);
    // The holder's lease lapses: the next tick takes over, and the stale holder's release
    // leaves the new owner's lease alone.
    fake.now.set(fake.now.get() + schedule::LEASE_S + 1);
    fake.new_request();
    let taken = schedule::claim(&fake, SCHED_KEY, "next", false, schedule::tick_wanted).unwrap();
    assert!(matches!(taken, Claim::Run { .. }));
    schedule::release(&fake, SCHED_KEY, "other", &|_| {}, (0, 0)).unwrap();
    let record = schedule::peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(record.owner.as_deref(), Some("next"));
    schedule::release(&fake, SCHED_KEY, "next", &|_| {}, (0, 0)).unwrap();
    // A stale holder releasing after that cannot overwrite the new owner's record.
    let released = schedule::peek(&fake, SCHED_KEY).unwrap();
    fake.now.set(fake.now.get() + 1);
    let stale = |record: &mut schedule::Sched| record.wake_at = 1;
    schedule::release(&fake, SCHED_KEY, "other", &stale, (0, 0)).unwrap();
    assert_eq!(schedule::peek(&fake, SCHED_KEY).unwrap(), released);
    // Two ticks in the same second, once the record is due a refresh: one ingest.
    fake.now.set(fake.now.get() + schedule::REFRESH_S);
    let cas = fake.cron_cas.get();
    run_tick(&fake);
    assert_eq!(fake.cron_cas.get(), cas + 1, "one cron:state write per run");
    freshness::finish_tick(&fake);
    let completed = ingest_runs(&fake);
    run_tick(&fake);
    assert_eq!(fake.calls.get(), 0);
    assert_eq!(ingest_runs(&fake), completed);
    assert_eq!(processed_ips(&fake), ["a"]);
}

#[test]
fn an_evicted_schedule_is_rebuilt_from_kv_without_rerunning_jobs() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    let completed = ingest_runs(&fake);
    fake.now.set(fake.now.get() + 1);
    fake.cache.borrow_mut().clear();
    let kv = fake.kv.borrow().clone();
    let rebuilt = run_tick(&fake);
    assert!(rebuilt["run"].is_null(), "{rebuilt}");
    assert!(fake.calls.get() <= 3, "{} calls", fake.calls.get());
    assert!(*fake.kv.borrow() == kv, "the rebuild wrote KV");
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    assert_eq!(
        fake.calls.get(),
        0,
        "the rebuilt record serves the next tick"
    );
    assert_eq!(ingest_runs(&fake), completed);
    assert_eq!(processed_ips(&fake), ["a"]);
}

#[test]
fn a_backlog_is_caught_up_on_consecutive_seconds() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let expected = seed(&fake, 10, 60);
    let mut runs = Vec::new();
    for second in 0..120 {
        fake.now.set(fake.now.get() + 1);
        if run_tick(&fake).get("run").is_some() {
            runs.push(second);
        }
        if fake.keys("raw/").is_empty() {
            break;
        }
    }
    assert!(fake.keys("raw/").is_empty(), "backlog left after {runs:?}");
    assert!(runs.len() > 2, "{runs:?}");
    assert!(
        runs.windows(2).all(|w| w[1] - w[0] <= 1),
        "catch-up waits: {runs:?}"
    );
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn ingest_is_scheduled_by_the_clamped_log_period_after_its_grace() {
    use fastly_log_analytics::admin::schedule::{
        due, next_ingest_at, period_s, ready_period, Mirror, INGEST_GRACE_S,
    };
    assert_eq!(period_s(1), 5);
    assert_eq!(period_s(5), 5);
    assert_eq!(period_s(60), 60);
    let mut mirror = Mirror {
        configured: true,
        log_period_s: 60,
        ..Default::default()
    };
    let now = 1_000_000 * 60 + INGEST_GRACE_S;
    assert_eq!(ready_period(now, 60), 1_000_000);
    mirror.jobs.ingest.record_success(1_000_000, 60);
    mirror.jobs.maint.last_run = Some(now);
    assert!(!due(&mirror, now).any());
    assert_eq!(next_ingest_at(&mirror), now + 60);
    assert!(!due(&mirror, now + 59).any());
    assert_eq!(due(&mirror, now + 60).ingest, Some(1_000_001));
    mirror.jobs.ingest.more_pending = true;
    assert_eq!(due(&mirror, now + 1).ingest, Some(1_000_000), "catch-up");
    mirror.jobs.ingest.retry_after = now + 30;
    assert!(due(&mirror, now + 29).ingest.is_none(), "error backoff");
    mirror.log_period_s = 2;
    mirror.jobs.ingest = Default::default();
    mirror.jobs.ingest.record_success(ready_period(now, 2), 2);
    assert_eq!(
        next_ingest_at(&mirror),
        (ready_period(now, 2) + 1) * 5 + INGEST_GRACE_S
    );
    mirror.configured = false;
    assert!(!due(&mirror, now + 3600).any());
}

#[test]
fn request_ingest_keeps_the_logging_cadence_after_consecutive_idles() {
    use fastly_log_analytics::admin::schedule::{next_ingest_at, peek, tick_wake_at, SCHED_KEY};

    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = service();
    svc.log_period = 60;
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let first = freshness::finish_tick(&fake);
    assert!(
        first["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["outcome"] == "done"),
        "{first}"
    );
    let record = peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(
        record.mirror.jobs.ingest.idle_streak, 1,
        "the resumed snapshot is followed by one empty forward poll"
    );
    // Hold maintenance dispatch and straggler sweep past the 4-step 60s cadence window so
    // consecutive pure request_ingest idle polls are exercised uninterrupted (matching
    // steady_state_idle_dispatches_batch_kv_writes_and_reconcile_on_flush).
    {
        let mut kv = fake.kv.borrow_mut();
        if let Some((_, cron_val)) = kv.get_mut("cron:state") {
            cron_val["jobs"]["maint"]["retry_after"] = json!((BASE + 7200) as u64);
        }
        if let Some((_, wm_val)) = kv.get_mut(ingest::WATERMARK_KEY) {
            wm_val["sweep"]["at"] = json!((BASE + 7200) as u64);
        }
    }
    assert!(tick_wake_at(&record) <= fake.now.get() + 60);
    let total_ingest_runs = |fake: &Fake| {
        ingest_runs(fake) + peek(fake, SCHED_KEY).map_or(0, |s| s.deferred_idle.len())
    };
    for step in 0..4 {
        let next = next_ingest_at(&peek(&fake, SCHED_KEY).unwrap().mirror);
        let advance_s = next - fake.now.get();
        assert!((1..=60).contains(&advance_s));
        let before_ingest_runs = total_ingest_runs(&fake);
        for dt in 1..advance_s {
            fake.now.set(fake.now.get() + 1);
            run_tick(&fake);
            assert_eq!(
                total_ingest_runs(&fake),
                before_ingest_runs,
                "step {step} (+{dt}s) ran request_ingest early"
            );
        }
        fake.now.set(fake.now.get() + 1);
        let ran = freshness::finish_tick(&fake);
        assert!(
            (before_ingest_runs + 1..=before_ingest_runs + 2).contains(&total_ingest_runs(&fake))
        );
        assert_eq!(ran["run"]["outcome"], "idle", "step {step}: {ran}");
        let record = peek(&fake, SCHED_KEY).unwrap();
        assert_eq!(record.mirror.jobs.ingest.idle_streak, step + 2);
        let now = fake.now.get();
        assert!((now + 1..=now + 60).contains(&next_ingest_at(&record.mirror)));
    }

    put_raw(&fake, BASE + 120, "b", &[line(BASE + 120, "b")]);
    fake.now
        .set(next_ingest_at(&peek(&fake, SCHED_KEY).unwrap().mirror));
    let productive = freshness::finish_tick(&fake);
    assert_eq!(
        productive["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|run| run["converted_raw_files"].as_u64().unwrap())
            .sum::<u64>(),
        2,
        "{productive}"
    );
    assert_eq!(processed_ips(&fake), ["a", "b"]);
    let record = peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(record.mirror.jobs.ingest.idle_streak, 1);
    assert_eq!(record.mirror.jobs.ingest.retry_after, 0);
    assert!(record.deferred_idle.is_empty());
    assert!((fake.now.get() + 1..=fake.now.get() + 60).contains(&tick_wake_at(&record)));
}

#[test]
fn steady_state_idle_dispatches_batch_kv_writes_and_reconcile_on_flush() {
    use fastly_log_analytics::admin::schedule::{peek, IDLE_WRITE_BATCH, SCHED_KEY};
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    // Hold maintenance stage dispatch beyond the window so 6 consecutive pure request_ingest idle
    // dispatches are exercised uninterrupted.
    fake.kv.borrow_mut().insert(
        "cron:state".into(),
        (
            1,
            json!({
                "jobs": {
                    "maint": {
                        "retry_after": (BASE + 7200) as u64
                    }
                }
            }),
        ),
    );
    // First idle dispatch establishes the schedule and idle_streak = 1 in KV.
    let first = run_tick(&fake);
    assert_eq!(first["run"]["outcome"], "idle", "{first}");
    assert_eq!(ingest_runs(&fake), 1);
    let cas_before = fake.cron_cas.get();
    let outbox_before = rows(&fake, LogKind::CronRuns).len();

    // Next 5 steady-state idle dispatches (with 3 non-due probe ticks before each) buffer in
    // Core Cache with zero `cron:state` CAS writes and zero new `logq` rows.
    for i in 1..(IDLE_WRITE_BATCH as usize) {
        for _ in 0..3 {
            fake.now.set(fake.now.get() + 1);
            assert_eq!(run_tick(&fake)["outcome"], "idle");
        }
        fake.now.set(fake.now.get() + 60);
        let tick = run_tick(&fake);
        assert_eq!(tick["run"]["outcome"], "idle", "batch step {i}: {tick}");
        assert_eq!(
            fake.cron_cas.get(),
            cas_before,
            "steady-state idle step {i} should defer cron:state CAS"
        );
        assert_eq!(
            rows(&fake, LogKind::CronRuns).len(),
            outbox_before,
            "steady-state idle step {i} should defer logq write"
        );
        let sched = peek(&fake, SCHED_KEY).unwrap();
        assert_eq!(sched.deferred_idle.len(), i);
    }

    // The 6th steady-state idle dispatch reaches IDLE_WRITE_BATCH (6) and flushes all 6 runs in a
    // single `cron:state` CAS write and a single `logq` append, preserving every run's idle_ticks
    // and idle_vcpu_ms diagnostics.
    for _ in 0..3 {
        fake.now.set(fake.now.get() + 1);
        assert_eq!(run_tick(&fake)["outcome"], "idle");
    }
    fake.now.set(fake.now.get() + 60);
    let sixth = run_tick(&fake);
    assert_eq!(sixth["run"]["outcome"], "idle", "{sixth}");
    assert_eq!(
        fake.cron_cas.get(),
        cas_before + 1,
        "batch of 6 steady-state idle dispatches writes cron:state once"
    );
    let sched = peek(&fake, SCHED_KEY).unwrap();
    assert!(sched.deferred_idle.is_empty());

    let first_run_id = format!("{}-request_ingest", first["run"]["id"].as_str().unwrap());
    let cron_runs: Vec<_> = rows(&fake, LogKind::CronRuns)
        .into_iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    assert_eq!(cron_runs.len(), 1 + IDLE_WRITE_BATCH as usize);
    let batched: Vec<_> = cron_runs
        .iter()
        .filter(|r| r["run_id"].as_str() != Some(first_run_id.as_str()))
        .collect();
    assert_eq!(batched.len(), IDLE_WRITE_BATCH as usize);
    let batch_idle_ticks: i64 = batched
        .iter()
        .map(|r| r["idle_ticks"].as_i64().unwrap())
        .sum();
    assert_eq!(
        batch_idle_ticks,
        (IDLE_WRITE_BATCH as i64) * 3,
        "all 18 intermediate idle probe ticks reconcile across the 6 batched runs"
    );
    assert!(batched
        .iter()
        .all(|r| r["idle_ticks"] == 3 && r["idle_vcpu_ms"].as_i64().unwrap() > 0));

    let state = &fake.kv.borrow()["cron:state"].1;
    assert_eq!(
        state["runs"].as_array().unwrap().len(),
        1 + IDLE_WRITE_BATCH as usize,
        "cron:state ring buffer receives all 6 batched runs on flush"
    );
}

#[test]
fn rum_ingest_is_due_by_its_own_log_period_independent_of_requests() {
    use fastly_log_analytics::admin::schedule::{due, ready_period, tick_wake_at, Mirror, Sched};
    let now = 1_000_000 * 300 + ingest::settle_s(300);
    let mut mirror = Mirror {
        configured: true,
        log_period_s: 60,
        ..Default::default()
    };
    mirror.jobs.ingest.record_success(ready_period(now, 60), 60);
    mirror.jobs.maint.last_run = Some(now);
    assert_eq!(due(&mirror, now).rum, None, "RUM off");

    mirror = mirror.with_rum(Some(300));
    assert_eq!(mirror.rum_log_period_s, 300);
    assert_eq!(due(&mirror, now).rum, Some(1_000_000), "first RUM run");
    mirror.jobs.rum.record_success(1_000_000, 300);
    assert!(!due(&mirror, now).any());
    // Requests come due every minute without RUM, and RUM only after its own period.
    assert_eq!(due(&mirror, now + 60).rum, None);
    assert!(due(&mirror, now + 60).ingest.is_some());
    assert_eq!(due(&mirror, now + 300).rum, Some(1_000_001));
    mirror
        .jobs
        .ingest
        .record_success(ready_period(now + 400, 60), 60);
    mirror.jobs.maint.last_run = Some(now + 290);
    let record = Sched {
        refreshed_at: now + 290,
        mirror: mirror.clone(),
        ..Default::default()
    };
    assert_eq!(tick_wake_at(&record), now + 300);
    mirror.jobs.rum.more_pending = true;
    assert_eq!(due(&mirror, now + 1).rum, Some(1_000_000), "catch-up");
    mirror.jobs.rum.retry_after = now + 30;
    assert_eq!(due(&mirror, now + 29).rum, None, "error backoff");
    assert_eq!(mirror.with_rum(None).rum_log_period_s, 0);
}

#[test]
fn schedule_job_merge_keeps_the_later_run_and_the_furthest_period() {
    use fastly_log_analytics::admin::schedule::{JobState, Jobs};
    let mut stored = Jobs {
        ingest: JobState {
            last_run: Some(20),
            last_success_period: Some(9),
            last_success_period_s: Some(60),
            ..Default::default()
        },
        ..Default::default()
    };
    let ours = Jobs {
        ingest: JobState {
            last_run: Some(10),
            last_success_period: Some(11),
            last_success_period_s: Some(60),
            more_pending: true,
            ..Default::default()
        },
        maint: JobState {
            last_run: Some(30),
            ..Default::default()
        },
        ..Default::default()
    };
    stored.merge(&ours);
    assert_eq!(stored.ingest.last_run, Some(20));
    assert!(!stored.ingest.more_pending);
    assert_eq!(stored.ingest.last_success_period, Some(11));
    assert_eq!(stored.maint.last_run, Some(30));
}

#[test]
fn stages_run_in_budget_order_and_admit_only_when_everything_fits() {
    use fastly_log_analytics::admin::{admit_stage, TICK_STAGES};
    assert_eq!(TICK_STAGES, ["request_ingest", "rum_ingest", "raw_delete"]);
    assert!(admit_stage(10, 10));
    assert!(!admit_stage(9, 10));
}

#[test]
fn maintenance_takes_a_storage_snapshot_without_an_admin_request() {
    let fake = Fake::new(BASE + 40 * DAY);
    tick_runtime(&fake);
    for _ in 0..12 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
    }
    let kinds = cron_runs(&fake)
        .iter()
        .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"storage_reconcile".to_owned()), "{kinds:?}");
    assert!(fake.kv.borrow()["storage:reconcile"].1["completed_at"].is_u64());
}

#[test]
fn idle_ticks_and_flush_probes_run_an_overdue_flush_sweep_and_reconcile_within_a_bounded_window() {
    let fake = Fake::new(BASE + 40 * DAY);
    tick_runtime(&fake);
    let done: serde_json::Map<String, Value> = LogKind::ALL
        .iter()
        .map(|k| (k.as_str().to_owned(), json!((BASE + 40 * DAY) / DAY)))
        .collect();
    fake.kv
        .borrow_mut()
        .insert("logs:compact_state".into(), (1, json!({"done": done})));
    let now_ms = (BASE + 40 * DAY) * 1000;
    for i in 0..3i64 {
        let line = json!({"kind":"ingest_files","timestamp":now_ms - 7_200_000 + i,
                          "event_id":format!("f{i}"),"service_id":SID,"severity":"info","raw_key":"k"});
        fake.kv.borrow_mut().insert(
            format!("logq_{SID}_{:010}_{i:02}", (now_ms / 1000 - 7_200) / 120),
            (1, json!({"ndjson": line.to_string()})),
        );
    }
    fake.kv.borrow_mut().insert(
        format!("logs:cursor:{SID}"),
        (
            1,
            json!({"scan_min": 1, "cache_min": 1, "pending": {"rows": 3,
                   "oldest_min": (BASE + 40 * DAY) as u64 / 60 - 120, "nonempty": []}}),
        ),
    );
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    for _ in 0..8 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        run_flush(&fake);
    }
    assert!(
        fake.bucket
            .borrow()
            .keys()
            .any(|k| k.contains("processed/ops/")),
        "the overdue flush committed ops files"
    );
    let kinds = cron_runs(&fake)
        .iter()
        .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"storage_reconcile".to_owned()), "{kinds:?}");
    // With nothing expired the sweep still logs its run, so its six-hour cadence stays visible.
    let sweeps: Vec<Value> = cron_runs(&fake)
        .into_iter()
        .filter(|r| r["cron_kind"] == "retention_sweep")
        .collect();
    assert!(
        sweeps
            .iter()
            .any(|r| r["outcome"] == "done" && r["deleted_files"] == 0),
        "{sweeps:?}"
    );
    let ran: Vec<Value> = fake
        .stage_runs
        .borrow()
        .iter()
        .map(|r| r["ran"].clone())
        .collect();
    assert!(ran.contains(&json!("retention_sweep")), "{ran:?}");
    assert!(fake.kv.borrow()["retention:last_sweep"].1["complete"] == true);
}

#[test]
fn fos_usage_aggregates_per_op_dataset_caller_status_and_cache_state() {
    use fastly_log_analytics::auth::settings::UsageLogging;
    let usage = UsageAgg::default();
    usage.set_caller("cron:ingest");
    for _ in 0..3 {
        usage.record_fos(
            "PUT",
            "processed/request/a.parquet",
            "",
            Some(200),
            10,
            100,
            0,
        );
    }
    usage.record_fos(
        "GET",
        "",
        "list-type=2&prefix=raw%2Frequest%2F",
        Some(200),
        5,
        0,
        50,
    );
    usage.note_sizes([("raw/request/a.gz", 777u64)]);
    usage.record_fos("DELETE", "raw/request/a.gz", "", Some(204), 4, 0, 0);
    usage.record_cdn("raw/request/b.gz", Some(200), Some("HIT"), 2, 900);
    usage.record_cdn("raw/request/c.gz", Some(200), Some("MISS"), 20, 900);
    usage.record_cdn("raw/request/d.gz", Some(200), None, 20, 900);
    let rows = usage.drain_rows(&UsageLogging::default());
    let find = |op: &str, cache: &str| {
        rows.iter()
            .find(|r| r["op"] == op && r["cdn_cache"] == cache)
            .unwrap_or_else(|| panic!("{op} {cache}: {rows:?}"))
    };
    let put = find("PUT", "DIRECT");
    assert_eq!(
        (
            put["op_count"].as_i64(),
            put["op_class"].as_str(),
            put["dataset"].as_str()
        ),
        (Some(3), Some("A"), Some("pq_request"))
    );
    assert_eq!(put["bytes_in"], 300);
    assert!((put["est_cost_usd"].as_f64().unwrap() - 3.0 * 0.0025 / 1000.0).abs() < 1e-12);
    let list = find("LIST", "DIRECT");
    assert_eq!(
        (list["op_class"].as_str(), list["dataset"].as_str()),
        (Some("A"), Some("raw_request"))
    );
    let delete = find("DELETE", "DIRECT");
    assert_eq!(delete["op_class"], "A");
    assert_eq!(delete["bytes_deleted"], 777);
    assert_eq!(delete["bytes_deleted_complete"], true);
    let hit = find("GET", "HIT");
    assert_eq!(hit["op_class"], "CDN_HIT");
    assert_eq!(hit["est_cost_usd"].as_f64(), Some(0.0));
    let miss = rows
        .iter()
        .find(|r| r["op"] == "GET" && r["cdn_cache"] == "MISS")
        .unwrap();
    assert_eq!(miss["op_class"], "B");
    assert_eq!(miss["op_count"], 2);
    assert!((miss["est_cost_usd"].as_f64().unwrap() - 2.0 * 0.004 / 10_000.0).abs() < 1e-12);
    assert!(rows.iter().all(|r| r["caller"] == "cron:ingest"));
    assert!(usage.drain_rows(&UsageLogging::default()).is_empty());
}

#[test]
fn raw_reads_use_the_cdn_path() {
    let fake = Fake::new(BASE + 3600);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    tick(&fake).unwrap();
    assert_eq!(*fake.cdn_gets.borrow(), vec![key]);
}
