use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use bytes::Bytes;
use fastly_log_analytics::ingest::{
    convert,
    hist::Hist,
    summary::{self, Hll},
};
use md5::{Digest, Md5};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Compression as Codec};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
const WINDOW: i64 = 300;

fn iso(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

/// Deterministic rows over five minutes, with every value class the summary treats specially.
fn fixture() -> Vec<Value> {
    let caches = [
        Value::from("HIT"),
        "HIT-STALE".into(),
        "HIT-CLUSTER".into(),
        "MISS".into(),
        "PASS".into(),
        "SYNTH".into(),
        "ERROR".into(),
        Value::Null,
    ];
    let statuses = [
        Value::from(200),
        200.into(),
        404.into(),
        503.into(),
        0.into(),
        Value::Null,
    ];
    let sigs = ["a, b,,c", "", "b", "c ,a"];
    (0..400i64)
        .map(|i| {
            let country = ["US", "DE", "JP"][i as usize % 3];
            let asn = [13335, 15169][i as usize % 2];
            let pop = ["IAD", "LHR", "NRT", ""][i as usize % 4];
            let ost: Value = if i % 2 == 0 {
                Value::Null
            } else {
                [200, 502][i as usize % 4 / 2].into()
            };
            let mut row = json!({
                "timestamp": iso(BASE * 1000 + i * 731 % (WINDOW * 1000)),
                "status": statuses[i as usize % statuses.len()],
                "cache": caches[i as usize % caches.len()],
                "resp_bytes": 100 + i * 7,
                "elapsed": 5_000 + i * 13,
                "ottfb": if i % 3 == 0 { Value::Null } else { (2_000 + i).into() },
                "ottlb": if i % 4 == 0 { Value::Null } else { (4_000 + i * 2).into() },
                "waf": i % 5 == 0,
                "waf_resp": if i % 2 == 0 { 406 } else { 200 },
                "waf_sig": sigs[i as usize % sigs.len()],
                "country": country,
                "asn": asn,
                "tcp_rtt": if i % 6 == 0 { Value::Null } else { (i % 50).into() },
                "edge": i % 4 != 0,
                "ua": format!("ua{}", i % 5),
                "tls_ciphers_sha": format!("c{}", i % 3),
                "pop": pop,
                "ost": ost,
                "ploss": (i % 10) as f64 / 100.0,
                "age": (i % 7) as f64 + 0.5,
            });
            if i % 11 != 0 {
                row["ip"] = format!("10.0.0.{}", i % 37).into();
            }
            if i % 9 != 0 {
                row["ja4"] = ["t13d", "t12d"][i as usize % 2].into();
            }
            if i % 13 != 0 {
                row["ttfb"] = (0.001 * (i % 17) as f64).into();
            }
            row
        })
        .collect()
}

mod common;

fn batch(rows: &[Value]) -> RecordBatch {
    let gz = common::zstd_ndjson(rows);
    let converted = convert::convert_object(&gz, WINDOW, BASE).unwrap();
    assert_eq!(converted.windows.len(), 1);
    converted.windows.into_values().next().unwrap()
}

fn read(bytes: &[u8]) -> Vec<Map<String, Value>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
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
                let value = if let Some(a) = any.downcast_ref::<StringArray>() {
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

fn bundle<'a>(rows: &'a [Map<String, Value>], name: &str) -> Vec<&'a Map<String, Value>> {
    rows.iter().filter(|r| r["bundle"] == name).collect()
}

fn s<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(Value::as_str)
}

fn i(row: &Value, key: &str) -> Option<i64> {
    row.get(key).and_then(Value::as_i64)
}

fn mi(row: &Map<String, Value>, key: &str) -> Option<i64> {
    row.get(key).and_then(Value::as_i64)
}

fn mf(row: &Map<String, Value>, key: &str) -> Option<f64> {
    row.get(key).and_then(Value::as_f64)
}

fn f(row: &Value, key: &str) -> Option<f64> {
    row.get(key).and_then(Value::as_f64)
}

#[test]
fn asset_urls_rows_match_the_raw_tables() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000), "url": "/app.js", "asset_type": "JavaScript",
            "content_encoding": "identity", "cache": "MISS", "resp_bytes": 100,
            "status": 200, "ttl": 0}),
        json!({"timestamp": iso(BASE * 1000 + 1), "path": "/app.js", "asset_type": "JavaScript",
            "content_encoding": "GZIP", "cache": "MISS", "resp_bytes": 200,
            "status": 500, "ttl": 7200}),
        json!({"timestamp": iso(BASE * 1000 + 2), "url": "/index.html", "asset_type": "HTML",
            "content_encoding": Value::Null, "cache": "HIT", "resp_bytes": 300,
            "status": 200, "ttl": 0}),
        json!({"timestamp": iso(BASE * 1000 + 3), "url": "/style.css", "asset_type": "CSS",
            "content_encoding": "", "cache": "MISS-CLUSTER", "resp_bytes": 400,
            "status": 404, "ttl": 1800}),
        json!({"timestamp": iso(BASE * 1000 + 4), "url": "/data.json", "asset_type": "JSON",
            "content_encoding": "IDENTITY", "cache": Value::Null, "resp_bytes": 500,
            "status": 200, "ttl": 0}),
    ];
    let b = custom_batch(
        &rows,
        &[
            ("asset_type", convert::Kind::Utf8),
            ("content_encoding", convert::Kind::Utf8),
            ("ttl", convert::Kind::Float64),
        ],
    );
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());
    let rows = bundle(&out, "asset_urls");
    let large: BTreeMap<_, _> = rows
        .iter()
        .filter(|r| r["field"] == "large")
        .map(|r| {
            (
                s(&Value::Object((*r).clone()), "value").unwrap().to_owned(),
                *r,
            )
        })
        .collect();
    assert_eq!(large.len(), 2);
    assert_eq!(
        mi(
            large["[\"/app.js\",\"JavaScript\",\"identity\"]"],
            "requests"
        ),
        Some(1)
    );
    assert_eq!(
        mi(
            large["[\"/app.js\",\"JavaScript\",\"identity\"]"],
            "resp_bytes_sum"
        ),
        Some(100)
    );
    assert_eq!(
        mi(
            large["[\"/app.js\",\"JavaScript\",\"identity\"]"],
            "status_min"
        ),
        Some(200)
    );
    assert!(!large.keys().any(|k| k.contains("unknown")));

    let low: BTreeMap<_, _> = rows
        .iter()
        .filter(|r| r["field"] == "low_ttl")
        .map(|r| {
            (
                s(&Value::Object((*r).clone()), "value").unwrap().to_owned(),
                *r,
            )
        })
        .collect();
    assert_eq!(low.len(), 3);
    assert_eq!(
        mf(low["[\"/app.js\",\"JavaScript\",\"MISS\"]"], "ttl_sum"),
        Some(0.0)
    );
    assert_eq!(
        mf(low["[\"/style.css\",\"CSS\",\"MISS-CLUSTER\"]"], "ttl_sum"),
        Some(1800.0)
    );
}

fn close(a: &Value, b: f64) {
    let a = a.as_f64().unwrap();
    assert!((a - b).abs() <= 1e-6 * b.abs().max(1.0), "{a} != {b}");
}

fn fla_hit(cache: Option<&str>) -> bool {
    matches!(cache, Some("HIT" | "HIT-STALE" | "HIT-CLUSTER"))
}

fn minute_of(row: &Value) -> i64 {
    let at = time::OffsetDateTime::parse(
        s(row, "timestamp").unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (at.unix_timestamp_nanos() / 1_000_000) as i64 / 60_000 * 60_000
}

fn ts_ms(row: &Value) -> i64 {
    let at = time::OffsetDateTime::parse(
        s(row, "timestamp").unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (at.unix_timestamp_nanos() / 1_000_000) as i64
}

#[test]
fn summary_matches_a_brute_force_computation_over_the_rows() {
    let rows = fixture();
    let b = batch(&rows);
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());

    // time_series: the dashboard's minute buckets (hit is any HIT*, 5xx is 500-599, 4xx any
    // other status >= 400).
    let mut minutes: BTreeMap<i64, [f64; 8]> = BTreeMap::new();
    for row in &rows {
        let m = minutes.entry(minute_of(row)).or_default();
        let status = i(row, "status");
        m[0] += 1.0;
        let s5 = status.is_some_and(|s| (500..=599).contains(&s));
        m[1] += f64::from(u8::from(!s5 && status.is_some_and(|s| s >= 400)));
        m[2] += f64::from(u8::from(s5));
        m[3] += f64::from(u8::from(
            s(row, "cache").is_some_and(|c| c.starts_with("HIT")),
        ));
        m[4] += f64::from(u8::from(s(row, "cache").is_some()));
        m[5] += i(row, "resp_bytes").unwrap() as f64;
        if let Some(t) = f(row, "ttfb") {
            m[6] += t;
            m[7] += 1.0;
        }
    }
    let series = bundle(&out, "time_series");
    assert_eq!(series.len(), minutes.len());
    for (row, (minute, m)) in series.iter().zip(&minutes) {
        assert_eq!(row["bucket"], *minute);
        for (k, col) in [
            "requests",
            "status_4xx",
            "status_5xx",
            "hits",
            "cache_total",
            "resp_bytes_sum",
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(row[*col].as_i64().unwrap() as f64, m[k], "{col}");
        }
        close(&row["ttfb_sum"], m[6]);
        assert_eq!(row["ttfb_count"].as_i64().unwrap() as f64, m[7]);
    }

    // overview: fla's hit includes HIT-CLUSTER.
    let ov = bundle(&out, "overview");
    assert_eq!(ov.len(), 1);
    let ov = ov[0];
    let count = |pred: &dyn Fn(&Value) -> bool| rows.iter().filter(|r| pred(r)).count() as i64;
    let sum = |pred: &dyn Fn(&Value) -> bool, val: &dyn Fn(&Value) -> Option<f64>| -> f64 {
        rows.iter().filter(|r| pred(r)).filter_map(val).sum()
    };
    let cache = |r: &Value| s(r, "cache").map(str::to_owned);
    assert_eq!(ov["bucket"], BASE * 1000);
    assert_eq!(ov["requests"], rows.len() as i64);
    assert_eq!(ov["hit_requests"], count(&|r| fla_hit(s(r, "cache"))));
    assert_eq!(
        ov["hit_requests"],
        series
            .iter()
            .map(|r| r["hits"].as_i64().unwrap())
            .sum::<i64>()
    );
    assert_eq!(
        ov["miss_requests"],
        count(&|r| cache(r).as_deref() == Some("MISS"))
    );
    assert_eq!(
        ov["pass_requests"],
        count(&|r| cache(r).as_deref() == Some("PASS"))
    );
    assert_eq!(
        ov["synth_requests"],
        count(&|r| cache(r).as_deref() == Some("SYNTH"))
    );
    assert_eq!(
        ov["origin_requests"],
        count(&|r| matches!(s(r, "cache"), Some("MISS" | "PASS" | "SYNTH" | "ERROR")))
    );
    assert_eq!(
        ov["bandwidth_saved_bytes"].as_i64().unwrap() as f64,
        sum(&|r| fla_hit(s(r, "cache")), &|r| f(r, "resp_bytes"))
    );
    assert_eq!(
        ov["total_bandwidth_bytes"].as_i64().unwrap() as f64,
        sum(&|_| true, &|r| f(r, "resp_bytes"))
    );
    assert_eq!(
        ov["threats_blocked"],
        count(&|r| r["waf"] == true && i(r, "waf_resp") == Some(406))
    );
    assert_eq!(
        (
            ov["shield_hit_requests"].clone(),
            ov["shield_total_requests"].clone()
        ),
        (json!(0), json!(0))
    );
    close(
        &ov["hit_elapsed_sum"],
        sum(&|r| fla_hit(s(r, "cache")), &|r| f(r, "elapsed")),
    );
    close(
        &ov["miss_ttfb_sum"],
        sum(&|r| cache(r).as_deref() == Some("MISS"), &|r| f(r, "ttfb")),
    );
    assert_eq!(
        ov["miss_ttfb_count"],
        count(&|r| cache(r).as_deref() == Some("MISS") && r.get("ttfb").is_some())
    );
    let both = |r: &Value| r.get("ttfb").is_some() && r.get("elapsed").is_some();
    let ottfb_ms = |r: &Value| f(r, "ottfb").map_or(0.0, |v| v / 1000.0);
    let ottlb_ms = |r: &Value| f(r, "ottlb").map_or(0.0, |v| v / 1000.0);
    assert_eq!(ov["waterfall_count"], count(&both));
    close(
        &ov["edge_processing_sum"],
        sum(&both, &|r| {
            Some((f(r, "ttfb")? * 1000.0 - ottfb_ms(r)).max(0.0))
        }),
    );
    close(&ov["origin_wait_sum"], sum(&both, &|r| Some(ottfb_ms(r))));
    close(
        &ov["origin_download_sum"],
        sum(&both, &|r| Some((ottlb_ms(r) - ottfb_ms(r)).max(0.0))),
    );
    close(
        &ov["client_download_sum"],
        sum(&both, &|r| {
            Some((f(r, "elapsed")? / 1000.0 - ottlb_ms(r).max(f(r, "ttfb")? * 1000.0)).max(0.0))
        }),
    );
    let ips: BTreeSet<&str> = rows.iter().filter_map(|r| s(r, "ip")).collect();
    assert_eq!(ov["ip_count_observed"], ips.len() as i64);
    let mut hll = Hll::default();
    ips.iter()
        .for_each(|ip| hll.add_hash(summary::hll_hash(ip)));
    assert_eq!(ov["ip_sketch"], hex::encode(hll.to_bytes()));

    // all_fields: CAST AS VARCHAR without NULL, '' kept as the dashboard counts it, exact
    // floats for age, split+trim without empty tokens for waf_sig_ind.
    let mut expected: BTreeMap<&str, BTreeMap<String, i64>> = BTreeMap::new();
    for row in &rows {
        let mut add = |field: &'static str, value: String| {
            *expected.entry(field).or_default().entry(value).or_default() += 1;
        };
        for field in ["cache", "status", "edge", "pop", "asn", "ip"] {
            match row.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::String(v)) => add(field, v.clone()),
                Some(v) => add(field, v.to_string()),
            }
        }
        add("age", f(row, "age").unwrap().to_string());
        for token in s(row, "waf_sig").unwrap().split(',') {
            if !token.trim().is_empty() {
                add("waf_sig_ind", token.trim().to_owned());
            }
        }
    }
    let fields = bundle(&out, "all_fields");
    let totals = bundle(&out, "field_totals");
    // The catalog fields and the referral stems.
    assert_eq!(totals.len(), summary::FIELDS.len() + 1);
    for (field, values) in &expected {
        let got: BTreeMap<String, i64> = fields
            .iter()
            .filter(|r| r["field"] == *field)
            .map(|r| {
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["count"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(&got, values, "{field}");
        let total = totals.iter().find(|r| r["field"] == *field).unwrap();
        assert_eq!(total["count"], values.values().sum::<i64>());
        assert_eq!(total["truncated"], false);
    }
    assert!(expected["edge"].contains_key("true"));
    assert!(expected["pop"].contains_key("") && expected["age"].contains_key("0.5"));
    assert!(expected["waf_sig_ind"].keys().eq(["a", "b", "c"]));

    // all_fields_ip: only the fields a page reads sketches of, with exact distinct IPs per
    // value and the sketch of exactly those IPs.
    let ip_rows = bundle(&out, "all_fields_ip");
    assert!(
        ip_rows
            .iter()
            .all(|r| summary::IP_SKETCH_FIELDS.contains(&r["field"].as_str().unwrap())),
        "unread sketches"
    );
    for row in ip_rows.iter().filter(|r| r["field"] == "tls_ciphers_sha") {
        let value = row["value"].as_str().unwrap();
        let ips: BTreeSet<&str> = rows
            .iter()
            .filter(|r| s(r, "tls_ciphers_sha") == Some(value))
            .filter_map(|r| s(r, "ip"))
            .collect();
        let mut hll = Hll::default();
        ips.iter()
            .for_each(|ip| hll.add_hash(summary::hll_hash(ip)));
        assert_eq!(row["ip_count_observed"], ips.len() as i64);
        assert_eq!(row["ip_sketch"], hex::encode(hll.to_bytes()));
        assert_eq!(row["sample_capped"], false);
    }
    assert_eq!(
        ip_rows
            .iter()
            .filter(|r| r["field"] == "tls_ciphers_sha")
            .count(),
        3
    );

    // sessions: per (ip, ja4) over rows with an IP.
    let mut sessions: BTreeMap<(String, Option<String>), Vec<&Value>> = BTreeMap::new();
    for row in rows.iter().filter(|r| r.get("ip").is_some()) {
        let key = (
            s(row, "ip").unwrap().to_owned(),
            s(row, "ja4").map(str::to_owned),
        );
        sessions.entry(key).or_default().push(row);
    }
    let got = bundle(&out, "sessions");
    assert_eq!(got.len(), sessions.len());
    for row in got {
        let key = (
            row["ip"].as_str().unwrap().to_owned(),
            row.get("ja4").and_then(Value::as_str).map(str::to_owned),
        );
        let group = &sessions[&key];
        assert_eq!(row["req_count"], group.len() as i64);
        assert_eq!(
            row["first_ts"],
            group.iter().map(|r| ts_ms(r)).min().unwrap()
        );
        assert_eq!(
            row["last_ts"],
            group.iter().map(|r| ts_ms(r)).max().unwrap()
        );
        assert_eq!(
            row["country"],
            group.iter().filter_map(|r| s(r, "country")).min().unwrap()
        );
        assert_eq!(
            row["ua_min"],
            group.iter().filter_map(|r| s(r, "ua")).min().unwrap()
        );
        assert_eq!(
            row["rtt_count"],
            group
                .iter()
                .filter(|r| r.get("tcp_rtt").is_some_and(|v| !v.is_null()))
                .count() as i64
        );
        assert_eq!(
            row["edge_count"],
            group.iter().filter(|r| r["edge"] == true).count() as i64
        );
        assert_eq!(
            row["shield_count"],
            group.iter().filter(|r| r["edge"] == false).count() as i64
        );
        assert_eq!(
            row["total_bytes"],
            group.iter().filter_map(|r| i(r, "resp_bytes")).sum::<i64>()
        );
    }

    // dims: additive per-dimension metrics; empty pop is skipped.
    let dims = bundle(&out, "dims");
    for row in dims.iter().filter(|r| r["field"] == "pop") {
        let group: Vec<&Value> = rows
            .iter()
            .filter(|r| s(r, "pop") == row["value"].as_str())
            .collect();
        assert_eq!(row["requests"], group.len() as i64);
        assert_eq!(
            row["errors"],
            group
                .iter()
                .filter(|r| i(r, "status").is_some_and(|s| s >= 400 || s == 0))
                .count() as i64
        );
        let lat: Vec<f64> = group
            .iter()
            .filter_map(|r| f(r, "ottfb").or_else(|| f(r, "ttfb").map(|t| t * 1e6)))
            .collect();
        assert_eq!(row["lat_us_count"], lat.len() as i64);
        close(&row["lat_us_sum"], lat.iter().sum());
        assert_eq!(
            row["ost_5xx_count"],
            group
                .iter()
                .filter(|r| i(r, "ost").is_some_and(|o| o >= 500))
                .count() as i64
        );
    }
    assert_eq!(dims.iter().filter(|r| r["field"] == "pop").count(), 3);
}

#[test]
fn top_k_keeps_the_most_frequent_values_and_totals_stay_exact() {
    let mut rows = Vec::new();
    for n in 0..(summary::TOP_K as i64 + 50) {
        let repeat = if n < 3 { 5 } else { 1 };
        for _ in 0..repeat {
            rows.push(json!({"timestamp": iso(BASE * 1000), "url": format!("/u{n:05}")}));
        }
    }
    let b = batch(&rows);
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());
    let urls: Vec<_> = bundle(&out, "all_fields")
        .into_iter()
        .filter(|r| r["field"] == "url")
        .collect();
    assert_eq!(urls.len(), summary::TOP_K);
    assert_eq!(urls[0]["value"], "/u00000");
    assert_eq!(urls[0]["count"], 5);
    // Ties break by value, so the cut is deterministic.
    assert_eq!(urls[3]["value"], "/u00003");
    assert_eq!(
        urls.last().unwrap()["value"],
        format!("/u{:05}", summary::TOP_K - 1)
    );
    let total = bundle(&out, "field_totals")
        .into_iter()
        .find(|r| r["field"] == "url")
        .unwrap();
    assert_eq!(total["count"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    let ip_spread = bundle(&out, "all_fields_ip");
    assert!(
        ip_spread.iter().all(|r| r["field"] != "url"),
        "no IPs, no sketch"
    );
}

fn custom_batch(rows: &[Value], custom: &[(&str, convert::Kind)]) -> RecordBatch {
    let schema = convert::RequestSchema::new(
        custom
            .iter()
            .map(|(name, kind)| convert::CustomColumn {
                name: (*name).into(),
                kind: *kind,
                slot_only: false,
            })
            .collect(),
    );
    let mut text = Vec::new();
    for row in rows {
        writeln!(text, "{row}").unwrap();
    }
    let gz = zstd::stream::encode_all(text.as_slice(), 1).unwrap();
    let converted = convert::convert_object_capped(&gz, WINDOW, BASE, u64::MAX, &schema).unwrap();
    converted.windows.into_values().next().unwrap()
}

#[test]
fn custom_columns_are_summarized_but_system_columns_are_not() {
    use convert::Kind;
    let custom = [
        ("tenant", Kind::Utf8),
        ("score", Kind::Int64),
        ("ratio", Kind::Float64),
        ("flag", Kind::Bool),
        ("empty", Kind::Utf8),
        ("cmcd_br", Kind::Int64),
        ("rum_lcp", Kind::Float64),
        ("edge_x", Kind::Utf8),
        ("fastly_req_id", Kind::Utf8),
        ("lt_run_id", Kind::Utf8),
        ("lt_request_id", Kind::Utf8),
    ];
    let rows: Vec<Value> = (0..6i64)
        .map(|i| {
            let tenant = ["acme", "acme", "beta"][i as usize % 3];
            json!({"timestamp": iso(BASE * 1000 + i), "ip": format!("10.0.0.{i}"),
                "tenant": tenant, "score": i % 2,
                "ratio": 0.5, "flag": i % 2 == 0, "cmcd_br": 3000, "rum_lcp": 1.5,
                "edge_x": "e", "fastly_req_id": format!("r{i}"),
                "lt_run_id": "run-7", "lt_request_id": format!("q{i}")})
        })
        .collect();
    let b = custom_batch(&rows, &custom);
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());
    let totals: BTreeMap<String, i64> = bundle(&out, "field_totals")
        .into_iter()
        .map(|r| {
            (
                r["field"].as_str().unwrap().to_owned(),
                r["count"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(totals["tenant"], 6);
    assert_eq!(totals["score"], 6);
    assert_eq!(totals["ratio"], 6);
    assert_eq!(totals["flag"], 6);
    assert_eq!(
        totals["empty"], 0,
        "an all-null custom column still gets a total"
    );
    for system in [
        "cmcd_br",
        "rum_lcp",
        "edge_x",
        "fastly_req_id",
        "lt_run_id",
        "lt_request_id",
        "ts_ms",
    ] {
        assert!(!totals.contains_key(system), "{system} is not a card");
    }
    let values = |field: &str| -> BTreeMap<String, i64> {
        bundle(&out, "all_fields")
            .into_iter()
            .filter(|r| r["field"] == field)
            .map(|r| {
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["count"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    assert_eq!(
        values("tenant"),
        BTreeMap::from([("acme".into(), 4), ("beta".into(), 2)])
    );
    assert_eq!(
        values("score"),
        BTreeMap::from([("0".into(), 3), ("1".into(), 3)])
    );
    assert_eq!(
        values("flag"),
        BTreeMap::from([("false".into(), 3), ("true".into(), 3)])
    );
    assert_eq!(values("ratio"), BTreeMap::from([("0.5".into(), 6)]));
    assert!(!bundle(&out, "all_fields_ip")
        .iter()
        .any(|r| r["field"] == "tenant"));
}

#[test]
fn bundles_keep_a_custom_field_only_when_every_source_summarized_it() {
    use fastly_log_analytics::ingest::bundle;
    let rows = |offset: i64| -> Vec<Value> {
        (0..3i64)
            .map(|i| json!({"timestamp": iso((BASE + offset) * 1000 + i), "tenant": "acme"}))
            .collect()
    };
    let with = |offset: i64| {
        let b = custom_batch(&rows(offset), &[("tenant", convert::Kind::Utf8)]);
        summary::write_summary(&[&b], BASE + offset).unwrap()
    };
    let without = summary::write_summary(&[&batch(&rows(WINDOW))], BASE + WINDOW).unwrap();
    let total = |bytes: &[u8], field: &str| -> Option<i64> {
        bundle(&read(bytes), "field_totals")
            .into_iter()
            .find(|r| r["field"] == field)
            .map(|r| r["count"].as_i64().unwrap())
    };
    let full = bundle::merge(&[&with(0), &with(WINDOW)], BASE, bundle::Unit::Hour).unwrap();
    assert_eq!(total(&full, "tenant"), Some(6));
    let mixed = bundle::merge(&[&with(0), &without], BASE, bundle::Unit::Hour).unwrap();
    assert_eq!(
        total(&mixed, "tenant"),
        None,
        "a partial count would read as exact"
    );
    assert_eq!(total(&mixed, "url"), Some(0), "catalog fields are kept");
    let day = bundle::merge(&[&full, &mixed], BASE, bundle::Unit::Day).unwrap();
    assert_eq!(total(&day, "tenant"), None);
}

/// `bytes` with its `all_fields_ip` rows relabelled to `field`, like a summary from before
/// sketches were limited to [`summary::IP_SKETCH_FIELDS`].
fn with_sketches_for(bytes: &[u8], field: &str) -> Vec<u8> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let schema = builder.schema().clone();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_key_value_metadata(kv)
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (name, fields) = (col("bundle"), col("field"));
        let relabelled: StringArray = (0..batch.num_rows())
            .map(|i| {
                if name.value(i) == "all_fields_ip" {
                    Some(field.to_owned())
                } else {
                    (!fields.is_null(i)).then(|| fields.value(i).to_owned())
                }
            })
            .collect();
        let at = schema.index_of("field").unwrap();
        let mut columns = batch.columns().to_vec();
        columns[at] = std::sync::Arc::new(relabelled);
        writer
            .write(&RecordBatch::try_new(schema.clone(), columns).unwrap())
            .unwrap();
    }
    writer.into_inner().unwrap()
}

#[test]
fn bundles_keep_only_the_ip_sketches_a_page_reads() {
    use fastly_log_analytics::ingest::bundle::{self as bundles, Unit};
    let w1 = summary::write_summary(&[&batch(&fixture())], BASE).unwrap();
    let sketched = |out: &[Map<String, Value>]| -> BTreeSet<String> {
        bundle(out, "all_fields_ip")
            .into_iter()
            .map(|r| r["field"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(
        sketched(&read(&w1)),
        BTreeSet::from(["tls_ciphers_sha".to_owned()])
    );
    let old = with_sketches_for(&w1, "country");
    assert_eq!(
        sketched(&read(&old)),
        BTreeSet::from(["country".to_owned()])
    );
    let hour = read(&bundles::merge(&[&old, &w1], BASE, Unit::Hour).unwrap());
    assert_eq!(
        sketched(&hour),
        BTreeSet::from(["tls_ciphers_sha".to_owned()])
    );
    let tls = |out: &[Map<String, Value>]| -> Vec<(String, i64)> {
        bundle(out, "all_fields_ip")
            .into_iter()
            .map(|r| {
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["count"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    assert_eq!(tls(&hour), tls(&read(&w1)));
}

#[test]
fn rows_without_a_timestamp_are_left_out() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000 + 1), "cache": "HIT", "ip": "a"}),
        json!({"cache": "HIT", "ip": "b"}),
    ];
    let b = batch(&rows);
    assert_eq!(b.num_rows(), 2);
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());
    assert_eq!(bundle(&out, "overview")[0]["requests"], 1);
    assert_eq!(bundle(&out, "sessions").len(), 1);
}

/// Rows for the v3 rows: network, latency, bot, header and asset columns, with nulls and
/// out-of-range values.
fn fixture_v3(offset_ms: i64, n: i64) -> Vec<Value> {
    (0..n)
        .map(|i| {
            let k = i as usize;
            let mut row = json!({
                "timestamp": iso(BASE * 1000 + offset_ms + i * 977 % (WINDOW * 1000)),
                "status": ([200, 503, 500, 599, 600, 404, 200][k % 7]),
                "cache": (["HIT", "miss", "PASS", "HIT-STALE"][k % 4]),
                "resp_bytes": 100 + i * 3,
                "elapsed": ([40, 5_000, 120_000, 70_000_000][k % 4] + i),
                "rid": format!("req-{offset_ms}-{i}"),
                "url": (["/app.js", "/s.css?v=1", "/f.woff2", "/index.html", "/x.JS"][k % 5]),
                "req_header_bytes": ([100, 256, 257, 5000, 40_000][k % 5]),
            });
            if !k.is_multiple_of(5) {
                row["proto"] = ["HTTP/2", "HTTP/1.1", "HTTP/3", "HTTP/2"][k % 4].into();
            }
            if !k.is_multiple_of(6) {
                row["country"] = ["US", "DE", "JP"][k % 3].into();
            }
            if !k.is_multiple_of(4) {
                row["city"] = ["Ashburn", "Berlin"][k % 2].into();
                row["lat"] = (10.0 + (k % 3) as f64).into();
                row["lon"] = (-5.0 - (k % 2) as f64).into();
            }
            if !k.is_multiple_of(7) {
                row["asn"] = [13335, 15169, 16509][k % 3].into();
            }
            if !k.is_multiple_of(3) {
                row["tcp_rtt"] = (1_000 + (k % 50) * 900).into();
                row["rtt_var"] = (100 + k % 9).into();
            }
            if !k.is_multiple_of(8) {
                row["ploss"] = ((k % 10) as f64 / 100.0).into();
            }
            if !k.is_multiple_of(13) {
                row["ttfb"] = (0.000_05 + 0.003 * (k % 17) as f64).into();
            }
            if k.is_multiple_of(2) {
                row["ottfb"] = (2_000 + i * 11).into();
            }
            if k.is_multiple_of(3) {
                row["ottlb"] = (4_000 + i * 13).into();
            }
            if k.is_multiple_of(9) {
                row["resp_header_content_encoding"] = "GZIP".into();
            }
            if !k.is_multiple_of(5) {
                row["edge"] = k.is_multiple_of(2).into();
            }
            if !k.is_multiple_of(4) {
                row["pop"] = ["ATL", "", "FRA"][k % 3].into();
            }
            if k.is_multiple_of(2) {
                row["obytes"] = (500 + i * 7).into();
            }
            if !k.is_multiple_of(3) {
                row["delivery_rate"] = (1_000_000 + i * 1000).into();
            }
            if k % 5 != 1 {
                row["ost"] = [200, 503, 404, 599][k % 4].into();
            }
            row["waf_sig"] = [
                "VERIFIED-BOT.googlebot",
                "verified-bot.bingbot",
                "x",
                "VERIFIED-BOT.googlebot,y",
            ][k % 4]
                .into();
            row
        })
        .collect()
}

fn meta(bytes: &[u8], key: &str) -> Option<String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.clone())
}

fn hist_cell(row: &Map<String, Value>, col: &str) -> Option<Hist> {
    row.get(col)
        .map(|v| Hist::from_bytes(&hex::decode(v.as_str().unwrap()).unwrap()).unwrap())
}

fn hist_of(values: impl IntoIterator<Item = i64>) -> Option<Hist> {
    let mut h = Hist::default();
    values.into_iter().for_each(|v| h.add(v));
    (!h.is_empty()).then_some(h)
}

fn fnv(s: &str) -> i64 {
    let h = s.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (h >> 1) as i64
}

fn ttfb_us(r: &Value) -> Option<i64> {
    f(r, "ttfb").map(|t| (t * 1e6).round() as i64)
}

/// The /network keys and sums for one group of rows.
fn check_net_group(row: &Map<String, Value>, group: &[&Value], what: &str) {
    let n = |pred: &dyn Fn(&Value) -> bool| group.iter().filter(|r| pred(r)).count() as i64;
    assert_eq!(row["requests"], group.len() as i64, "{what}");
    assert_eq!(
        row["status_5xx"],
        n(&|r| i(r, "status").is_some_and(|s| (500..=599).contains(&s))),
        "{what}"
    );
    assert_eq!(
        row["rtt_sum"],
        group.iter().filter_map(|r| i(r, "tcp_rtt")).sum::<i64>(),
        "{what}"
    );
    assert_eq!(
        row["rtt_count"],
        n(&|r| i(r, "tcp_rtt").is_some()),
        "{what}"
    );
    close(
        &row["ploss_sum"],
        group.iter().filter_map(|r| f(r, "ploss")).sum(),
    );
    assert_eq!(
        row["ploss_count"],
        n(&|r| f(r, "ploss").is_some()),
        "{what}"
    );
    assert_eq!(
        hist_cell(row, "ttfb_hist"),
        hist_of(group.iter().filter_map(|r| ttfb_us(r))),
        "{what}"
    );
    assert_eq!(
        hist_cell(row, "rtt_hist"),
        hist_of(group.iter().filter_map(|r| i(r, "tcp_rtt"))),
        "{what}"
    );
    assert_eq!(
        hist_cell(row, "ottfb_hist"),
        hist_of(group.iter().filter_map(|r| i(r, "ottfb"))),
        "{what}"
    );
    assert_eq!(
        hist_cell(row, "ottlb_hist"),
        hist_of(group.iter().filter_map(|r| i(r, "ottlb"))),
        "{what}"
    );
}

fn net_key(r: &Value, dim: &str) -> String {
    let or = |k: &str, d: &str| s(r, k).unwrap_or(d).to_owned();
    match dim {
        "proto" => or("proto", "Other"),
        "country" => or("country", "Unknown"),
        "asn" => i(r, "asn").unwrap_or(0).to_string(),
        "city" => {
            serde_json::to_string(&[or("country", "Unknown"), or("city", "Unknown")]).unwrap()
        }
        _ => unreachable!(),
    }
}

#[test]
fn v3_rows_match_a_brute_force_computation_over_the_rows() {
    let rows = fixture_v3(0, 700);
    let b = batch(&rows);
    let bytes = summary::write_summary(&[&b], BASE).unwrap();
    assert_eq!(meta(&bytes, "flp_summary").as_deref(), Some("3"));
    let out = read(&bytes);

    // overview carries all five latency histograms.
    let ov = bundle(&out, "overview")[0];
    assert_eq!(
        hist_cell(ov, "ttfb_hist"),
        hist_of(rows.iter().filter_map(ttfb_us))
    );
    assert_eq!(
        hist_cell(ov, "elapsed_hist"),
        hist_of(rows.iter().filter_map(|r| i(r, "elapsed")))
    );
    let elapsed = hist_cell(ov, "elapsed_hist").unwrap();
    assert!(elapsed.under > 0 && elapsed.over > 0);

    // dims: the /network keys (null is Unknown, 0 or Other), coalesced RTT and loss, 5xx.
    let dims = bundle(&out, "dims");
    let totals = bundle(&out, "dim_totals");
    for dim in ["proto", "country", "asn", "city"] {
        let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for r in &rows {
            groups.entry(net_key(r, dim)).or_default().push(r);
        }
        let got: Vec<_> = dims.iter().filter(|r| r["field"] == dim).collect();
        assert_eq!(got.len(), groups.len(), "{dim}");
        for row in got {
            let value = row["value"].as_str().unwrap();
            check_net_group(row, &groups[value], &format!("{dim}={value}"));
            assert_eq!(
                row["rttvar_count"],
                groups[value]
                    .iter()
                    .filter(|r| i(r, "rtt_var").is_some())
                    .count() as i64
            );
            assert_eq!(row["retrans_count"], 0);
            assert!(row.get("cwnd_count").is_none_or(|c| c == 0));
            if dim == "city" {
                let geo: Vec<_> = groups[value]
                    .iter()
                    .filter_map(|r| Some((f(r, "lat")?, f(r, "lon")?)))
                    .collect();
                assert_eq!(row["geo_count"], geo.len() as i64);
                close(&row["lat_sum"], geo.iter().map(|g| g.0).sum());
                close(&row["lon_sum"], geo.iter().map(|g| g.1).sum());
            }
        }
        let total = totals
            .iter()
            .find(|r| r["value"] == "dims" && r["field"] == dim)
            .unwrap();
        assert_eq!(total["requests"], rows.len() as i64);
        assert_eq!(total["truncated"], false);
    }
    assert!(dims
        .iter()
        .any(|r| r["field"] == "city" && r["value"] == r#"["Unknown","Unknown"]"#));

    // series5: overall, proto and ASN per five minutes.
    let series = bundle(&out, "series5");
    let overall: Vec<_> = series.iter().filter(|r| r["field"] == "overall").collect();
    assert_eq!(overall.len(), 1);
    assert_eq!(overall[0]["bucket"], BASE * 1000);
    let all: Vec<&Value> = rows.iter().collect();
    check_net_group(overall[0], &all, "overall");
    assert_eq!(
        hist_cell(overall[0], "elapsed_hist"),
        hist_of(rows.iter().filter_map(|r| i(r, "elapsed")))
    );
    for dim in ["proto", "asn"] {
        let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for r in &rows {
            groups.entry(net_key(r, dim)).or_default().push(r);
        }
        let got: Vec<_> = series.iter().filter(|r| r["field"] == dim).collect();
        assert_eq!(got.len(), groups.len(), "{dim}");
        for row in got {
            let value = row["value"].as_str().unwrap();
            check_net_group(row, &groups[value], &format!("series5 {dim}={value}"));
        }
    }

    // bins: request header sizes and verified bots per five minutes.
    let bins = bundle(&out, "bins");
    let mut want: BTreeMap<(String, String), i64> = BTreeMap::new();
    for r in &rows {
        let h = i(r, "req_header_bytes").unwrap();
        let label = match h {
            ..=256 => "0-256B",
            257..=1024 => "256B-1KB",
            1025..=4096 => "1KB-4KB",
            4097..=8192 => "4KB-8KB",
            8193..=32768 => "8KB-32KB",
            _ => ">32KB",
        };
        *want
            .entry(("req_header_size".into(), label.into()))
            .or_default() += 1;
        let sig = s(r, "waf_sig").unwrap();
        if sig.to_uppercase().contains("VERIFIED-BOT.") {
            *want
                .entry(("verified_bot".into(), sig.replace("VERIFIED-BOT.", "")))
                .or_default() += 1;
        }
    }
    let got: BTreeMap<(String, String), i64> = bins
        .iter()
        .map(|r| {
            assert_eq!(r["bucket"], BASE * 1000);
            (
                (
                    r["field"].as_str().unwrap().to_owned(),
                    r["value"].as_str().unwrap().to_owned(),
                ),
                r["requests"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(got, want);
    assert!(got.contains_key(&("verified_bot".into(), "verified-bot.bingbot".into())));

    // assets: derived type, upper cache, lower encoding, the /assets sums.
    let mut want: BTreeMap<String, (i64, i64, f64)> = BTreeMap::new();
    for r in &rows {
        let url = s(r, "url").unwrap().to_lowercase();
        let kind = if url.ends_with(".js") {
            "JavaScript"
        } else if url.ends_with(".css") {
            "CSS"
        } else if url.contains(".woff") {
            "Font"
        } else {
            "Other"
        };
        let key = serde_json::to_string(&[
            kind.to_owned(),
            s(r, "cache").unwrap().to_uppercase(),
            s(r, "resp_header_content_encoding")
                .unwrap_or("unknown")
                .to_lowercase(),
        ])
        .unwrap();
        let ttfb_ms = f(r, "ttfb").map_or(f(r, "elapsed").unwrap() / 1000.0, |t| t * 1000.0);
        let e = want.entry(key).or_default();
        e.0 += 1;
        e.1 += i(r, "resp_bytes").unwrap();
        e.2 += ttfb_ms;
    }
    let assets = bundle(&out, "assets");
    assert_eq!(assets.len(), want.len());
    for row in assets {
        let (n, b, t) = want[row["value"].as_str().unwrap()];
        assert_eq!(row["requests"], n);
        assert_eq!(row["resp_bytes_sum"], b);
        close(&row["ttfb_sum"], t);
        assert_eq!(row["origin_requests"], 0);
    }

    // sample: the 300 rows with a TTFB whose request id hashes lowest.
    let mut keyed: Vec<(i64, &Value)> = rows
        .iter()
        .filter(|r| r.get("ttfb").is_some())
        .map(|r| (fnv(s(r, "rid").unwrap()), r))
        .collect();
    keyed.sort_by_key(|k| k.0);
    keyed.truncate(summary::SAMPLE_K);
    let sample = bundle(&out, "sample");
    assert_eq!(sample.len(), summary::SAMPLE_K);
    for (row, (key, r)) in sample.iter().zip(&keyed) {
        assert_eq!(row["sample_key"], *key);
        assert_eq!(row["bucket"], ts_ms(r));
        assert_eq!(row["value"], s(r, "cache").unwrap());
        close(&row["ttfb_sum"], f(r, "ttfb").unwrap());
        assert_eq!(row["elapsed_us"], i(r, "elapsed").unwrap());
        assert_eq!(row.get("rtt_sum").and_then(Value::as_i64), i(r, "tcp_rtt"));
    }
}

/// `COALESCE(ottfb, ttfb * 1e6)` in whole µs: /origin's latency.
fn origin_us(r: &Value) -> Option<i64> {
    i(r, "ottfb").or_else(|| ttfb_us(r))
}

fn leg_of(r: &Value) -> &'static str {
    match r.get("edge").and_then(Value::as_bool) {
        Some(true) => "true",
        Some(false) => "false",
        None => "",
    }
}

type Row = Map<String, Value>;

/// The `name` rows of `field` by value, and the field's `dim_totals` row.
fn group_rows<'a>(out: &'a [Row], name: &str, field: &str) -> (BTreeMap<String, &'a Row>, &'a Row) {
    let rows = bundle(out, name)
        .into_iter()
        .filter(|r| r["field"] == field)
        .map(|r| (r["value"].as_str().unwrap().to_owned(), r))
        .collect();
    let total = bundle(out, "dim_totals")
        .into_iter()
        .find(|r| r["value"] == name && r["field"] == field)
        .unwrap_or_else(|| panic!("{name}/{field} dim_totals"));
    (rows, total)
}

fn groups_by<'a>(
    rows: impl IntoIterator<Item = &'a Value>,
    key: impl Fn(&Value) -> Option<String>,
) -> BTreeMap<String, Vec<&'a Value>> {
    let mut out: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for r in rows {
        if let Some(k) = key(r) {
            out.entry(k).or_default().push(r);
        }
    }
    out
}

#[test]
fn transport_origin_and_url_rows_match_a_brute_force_computation() {
    let rows = fixture_v3(0, 700);
    let out = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());

    // transport: /network transport's sums over every row, and per protocol.
    let by_proto = groups_by(&rows, |r| {
        Some(s(r, "proto").unwrap_or("Unknown").to_owned())
    });
    let all: Vec<&Value> = rows.iter().collect();
    for (field, groups) in [
        ("overall", BTreeMap::from([(String::new(), all.clone())])),
        ("proto", by_proto),
    ] {
        let (got, total) = group_rows(&out, "transport", field);
        assert_eq!(total["requests"], rows.len() as i64, "{field}");
        assert_eq!(total["truncated"], false);
        assert_eq!(got.len(), groups.len(), "{field}");
        for (value, group) in &groups {
            let row = got[value];
            let what = format!("transport {field}={value}");
            assert_eq!(row["requests"], group.len() as i64, "{what}");
            assert_eq!(
                row["resp_bytes_sum"],
                group.iter().filter_map(|r| i(r, "resp_bytes")).sum::<i64>(),
                "{what}"
            );
            assert_eq!(
                hist_cell(row, "rtt_hist"),
                hist_of(group.iter().filter_map(|r| i(r, "tcp_rtt"))),
                "{what}"
            );
            close(
                &row["ploss_sum"],
                group.iter().filter_map(|r| f(r, "ploss")).sum(),
            );
            assert_eq!(
                row["ploss_count"],
                group.iter().filter(|r| f(r, "ploss").is_some()).count() as i64
            );
            let rates: Vec<f64> = group.iter().filter_map(|r| f(r, "delivery_rate")).collect();
            close(&row["delivery_rate_sum"], rates.iter().sum());
            assert_eq!(row["delivery_rate_count"], rates.len() as i64, "{what}");
        }
    }

    // origin: over rows with an origin latency; overall carries every column, legs and POPs
    // requests and latency.
    let lat_rows: Vec<&Value> = rows.iter().filter(|r| origin_us(r).is_some()).collect();
    assert!(lat_rows.len() < rows.len());
    let (got, total) = group_rows(&out, "origin", "overall");
    assert_eq!(total["requests"], lat_rows.len() as i64);
    let row = got[""];
    let n = |pred: &dyn Fn(&Value) -> bool| lat_rows.iter().filter(|r| pred(r)).count() as i64;
    assert_eq!(row["requests"], lat_rows.len() as i64);
    let cache = |r: &Value, p: &str| s(r, "cache").is_some_and(|c| c.starts_with(p));
    assert_eq!(row["miss_requests"], n(&|r| cache(r, "MISS")));
    assert_eq!(row["pass_requests"], n(&|r| cache(r, "PASS")));
    assert!(row["pass_requests"].as_i64().unwrap() > 0);
    assert_eq!(
        row["ost_5xx_count"],
        n(&|r| i(r, "ost").is_some_and(|s| (500..600).contains(&s)))
    );
    assert_eq!(row["ost_total_count"], n(&|r| i(r, "ost").is_some()));
    assert_eq!(
        hist_cell(row, "ttfb_hist"),
        hist_of(lat_rows.iter().filter_map(|r| origin_us(r)))
    );
    assert_eq!(
        hist_cell(row, "ottlb_hist"),
        hist_of(lat_rows.iter().filter_map(|r| i(r, "ottlb")))
    );
    let overhead = hist_cell(row, "overhead_hist").unwrap();
    assert_eq!(
        Some(overhead.clone()),
        hist_of(
            lat_rows
                .iter()
                .filter_map(|r| Some(i(r, "elapsed")? - i(r, "ottlb")?))
        )
    );
    assert!(overhead.under > 0, "negative overheads stay counted");
    assert_eq!(
        hist_cell(row, "obytes_hist"),
        hist_of(lat_rows.iter().filter_map(|r| i(r, "obytes")))
    );
    for (field, groups) in [
        (
            "edge",
            groups_by(lat_rows.iter().copied(), |r| Some(leg_of(r).to_owned())),
        ),
        (
            "pop",
            groups_by(lat_rows.iter().copied(), |r| {
                s(r, "pop").filter(|p| !p.is_empty()).map(str::to_owned)
            }),
        ),
    ] {
        let (got, total) = group_rows(&out, "origin", field);
        assert_eq!(
            total["requests"],
            groups.values().map(Vec::len).sum::<usize>() as i64,
            "{field}"
        );
        assert_eq!(got.len(), groups.len(), "{field}");
        for (value, group) in &groups {
            let row = got[value];
            assert_eq!(row["requests"], group.len() as i64, "{field}={value}");
            assert_eq!(
                hist_cell(row, "ttfb_hist"),
                hist_of(group.iter().filter_map(|r| origin_us(r))),
                "{field}={value}"
            );
            assert!(row.get("miss_requests").is_none() && row.get("ottlb_hist").is_none());
        }
    }
    assert_eq!(
        group_rows(&out, "origin", "edge")
            .0
            .keys()
            .collect::<Vec<_>>(),
        ["", "false", "true"]
    );

    // urls: origin latency and elapsed per URL, elapsed per ASN.
    for (field, groups) in [
        ("url", groups_by(&rows, |r| s(r, "url").map(str::to_owned))),
        (
            "asn",
            groups_by(&rows, |r| i(r, "asn").map(|a| a.to_string())),
        ),
    ] {
        let (got, total) = group_rows(&out, "urls", field);
        assert_eq!(
            total["requests"],
            groups.values().map(Vec::len).sum::<usize>() as i64
        );
        assert_eq!(got.len(), groups.len(), "{field}");
        for (value, group) in &groups {
            let row = got[value];
            let what = format!("urls {field}={value}");
            assert_eq!(row["requests"], group.len() as i64, "{what}");
            let elapsed: Vec<i64> = group.iter().filter_map(|r| i(r, "elapsed")).collect();
            close(&row["elapsed_sum"], elapsed.iter().sum::<i64>() as f64);
            assert_eq!(row["elapsed_count"], elapsed.len() as i64, "{what}");
            assert_eq!(hist_cell(row, "elapsed_hist"), hist_of(elapsed), "{what}");
            let lat = if field == "url" {
                hist_of(group.iter().filter_map(|r| origin_us(r)))
            } else {
                None
            };
            assert_eq!(hist_cell(row, "ttfb_hist"), lat, "{what}");
        }
    }

    // series5 origin: per leg and five minutes, origin latency and OTTLB.
    let legs = groups_by(&rows, |r| Some(leg_of(r).to_owned()));
    let series: Vec<_> = bundle(&out, "series5")
        .into_iter()
        .filter(|r| r["field"] == "origin")
        .collect();
    assert_eq!(series.len(), legs.len());
    for row in series {
        let group = &legs[row["value"].as_str().unwrap()];
        assert_eq!(row["bucket"], BASE * 1000);
        assert_eq!(row["requests"], group.len() as i64);
        assert_eq!(
            hist_cell(row, "ttfb_hist"),
            hist_of(group.iter().filter_map(|r| origin_us(r)))
        );
        assert_eq!(
            hist_cell(row, "ottlb_hist"),
            hist_of(group.iter().filter_map(|r| i(r, "ottlb")))
        );
    }
}

#[test]
fn new_groups_mark_presence_without_data_and_cut_urls_to_the_top_k() {
    // No origin columns at all: every new field still has its dim_totals row.
    let rows: Vec<Value> = (0..summary::DIM_K as i64 + 5)
        .map(|i| json!({"timestamp": iso(BASE * 1000 + i), "url": format!("/u/{i}")}))
        .chain((0..3).map(|i| json!({"timestamp": iso(BASE * 1000 + i), "url": "/hot"})))
        .collect();
    let out = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());
    for (group, field) in [
        ("transport", "overall"),
        ("transport", "proto"),
        ("origin", "overall"),
        ("origin", "edge"),
        ("origin", "pop"),
        ("urls", "asn"),
        ("series5", "origin"),
        ("header_ip", "ip"),
        ("bot_ua", "ua"),
        ("tls_fp", "fp"),
        ("tls_fp", "fp_country"),
        ("value", "cache"),
        ("value", "io_pair"),
        ("proxy", "sample"),
    ] {
        let (_, total) = group_rows(&out, group, field);
        let want = match (group, field) {
            ("transport", _) | ("urls", "url") | ("series5", _) | ("value", "cache") => {
                rows.len() as i64
            }
            _ => 0,
        };
        assert_eq!(total["requests"], want, "{group}/{field}");
        assert_eq!(total["truncated"], false, "{group}/{field}");
    }
    let (urls, total) = group_rows(&out, "urls", "url");
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    assert_eq!(urls.len(), summary::DIM_K);
    assert_eq!(urls["/hot"]["requests"], 3);
    assert!(group_rows(&out, "origin", "overall").0[""]
        .get("ttfb_hist")
        .is_none());
}

/// `fixture_v3` rows with the /network quality columns: regions, and the client-only loss and RTT
/// variance custom fields on some rows.
fn quality_rows() -> Vec<Value> {
    fixture_v3(0, 700)
        .into_iter()
        .enumerate()
        .map(|(k, mut row)| {
            if k % 5 != 2 {
                row["region"] = ["VA", "CA", "HE"][k % 3].into();
            }
            if k % 4 == 1 {
                row["client_ploss"] = (0.001 * (k % 7) as f64).into();
            }
            if k % 6 == 2 {
                row["client_tcpi_rttvar"] = (300.0 + (k % 11) as f64).into();
            }
            row
        })
        .collect()
}

#[test]
fn city_series_and_quality_rows_match_a_brute_force_computation() {
    let rows = quality_rows();
    let client = [
        ("client_ploss", convert::Kind::Float64),
        ("client_tcpi_rttvar", convert::Kind::Float64),
    ];
    let out = read(&summary::write_summary(&[&custom_batch(&rows, &client)], BASE).unwrap());
    let step = |r: &Value| {
        let at = time::OffsetDateTime::parse(
            s(r, "timestamp").unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        (at.unix_timestamp() * 1000).div_euclid(300_000) * 300_000
    };

    // series5 city: the map's cells, per five minutes and `[COALESCE(country), COALESCE(city)]`.
    let cells = groups_by(&rows, |r| {
        let city = s(r, "city")?;
        Some(json!([step(r), [r.get("country"), city]]).to_string())
    });
    let got: Vec<_> = bundle(&out, "series5")
        .into_iter()
        .filter(|r| r["field"] == "city")
        .collect();
    let (_, total) = group_rows(&out, "series5", "city");
    let with_city = rows.iter().filter(|r| s(r, "city").is_some()).count();
    assert!(with_city < rows.len());
    assert_eq!(total["requests"], with_city as i64);
    assert_eq!(total["truncated"], false);
    assert_eq!(got.len(), cells.len());
    for row in got {
        let value: Value = serde_json::from_str(row["value"].as_str().unwrap()).unwrap();
        let key = json!([row["bucket"], value]).to_string();
        let group = &cells[&key];
        assert_eq!(row["requests"], group.len() as i64, "{key}");
        assert_eq!(
            row["status_5xx"],
            group
                .iter()
                .filter(|r| i(r, "status").is_some_and(|s| (500..600).contains(&s)))
                .count() as i64
        );
        close(
            &row["ploss_sum"],
            group
                .iter()
                .filter_map(|r| f(r, "client_ploss").or_else(|| f(r, "ploss")))
                .sum(),
        );
        let geo: Vec<&&Value> = group.iter().filter(|r| f(r, "lat").is_some()).collect();
        assert_eq!(row["geo_count"], geo.len() as i64);
        close(
            &row["lat_sum"],
            geo.iter().filter_map(|r| f(r, "lat")).sum(),
        );
        close(
            &row["lon_sum"],
            geo.iter().filter_map(|r| f(r, "lon")).sum(),
        );
        assert_eq!(
            hist_cell(row, "rtt_hist"),
            hist_of(group.iter().filter_map(|r| i(r, "tcp_rtt"))),
            "{key}"
        );
        assert!(row.get("ttfb_hist").is_none() && row.get("rtt_sum").is_none());
    }
    assert!(
        cells.keys().any(|k| k.contains("[null,")),
        "a city without a country"
    );

    // quality overall: every row's request and its throughput where elapsed > 0.
    let (got, total) = group_rows(&out, "quality", "overall");
    assert_eq!(total["requests"], rows.len() as i64);
    let row = got[""];
    assert_eq!(row["requests"], rows.len() as i64);
    let tp: Vec<f64> = rows
        .iter()
        .filter_map(|r| {
            let e = i(r, "elapsed").filter(|e| *e > 0)?;
            Some(i(r, "resp_bytes").unwrap_or(0) as f64 * 8.0 / e as f64)
        })
        .collect();
    assert_eq!(row["throughput_count"], tp.len() as i64);
    close(&row["throughput_sum"], tp.iter().sum());

    // quality region: requests and RTT per `[country, COALESCE(region, 'Unknown')]`.
    let regions = groups_by(&rows, |r| {
        Some(json!([s(r, "country")?, s(r, "region").unwrap_or("Unknown")]).to_string())
    });
    let (got, total) = group_rows(&out, "quality", "region");
    assert_eq!(
        total["requests"],
        regions.values().map(Vec::len).sum::<usize>() as i64
    );
    assert_eq!(got.len(), regions.len());
    assert!(got.keys().any(|k| k.contains("Unknown")));
    for (value, group) in &regions {
        let row = got[value];
        assert_eq!(row["requests"], group.len() as i64, "{value}");
        let rtt: Vec<i64> = group.iter().filter_map(|r| i(r, "tcp_rtt")).collect();
        assert_eq!(row["rtt_sum"], rtt.iter().sum::<i64>(), "{value}");
        assert_eq!(row["rtt_count"], rtt.len() as i64, "{value}");
    }

    // quality asn: only the client-only loss and RTT variance, over rows that carry them.
    let asns = groups_by(&rows, |r| {
        (f(r, "client_ploss").is_some() || f(r, "client_tcpi_rttvar").is_some())
            .then(|| i(r, "asn").unwrap_or(0).to_string())
    });
    let (got, total) = group_rows(&out, "quality", "asn");
    assert_eq!(
        total["requests"],
        asns.values().map(Vec::len).sum::<usize>() as i64
    );
    assert_eq!(got.len(), asns.len());
    for (value, group) in &asns {
        let row = got[value];
        let sum = |k: &str| group.iter().filter_map(|r| f(r, k)).sum::<f64>();
        let count = |k: &str| group.iter().filter(|r| f(r, k).is_some()).count() as i64;
        assert_eq!(row["requests"], group.len() as i64, "{value}");
        close(&row["ploss_sum"], sum("client_ploss"));
        assert_eq!(row["ploss_count"], count("client_ploss"), "{value}");
        close(&row["rttvar_sum"], sum("client_tcpi_rttvar"));
        assert_eq!(row["rttvar_count"], count("client_tcpi_rttvar"), "{value}");
    }
}

#[test]
fn city_series_cut_to_the_top_k_and_quality_asn_present_without_client_columns() {
    let n = summary::SERIES_CITY_K as i64 + 5;
    let rows: Vec<Value> = (0..n)
        .map(|i| json!({"timestamp": iso(BASE * 1000 + i), "city": format!("c{i}"), "country": "US"}))
        .chain((0..3).map(|i| json!({"timestamp": iso(BASE * 1000 + i), "city": "Hot"})))
        .collect();
    let out = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());
    let (_, total) = group_rows(&out, "series5", "city");
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    let cities: Vec<_> = bundle(&out, "series5")
        .into_iter()
        .filter(|r| r["field"] == "city")
        .collect();
    assert_eq!(cities.len(), summary::SERIES_CITY_K);
    assert!(cities
        .iter()
        .any(|r| r["value"] == r#"[null,"Hot"]"# && r["requests"] == 3));
    // No client columns: `asn` keeps its presence row and no values.
    for (field, requests, values) in [
        ("overall", rows.len(), 1),
        ("asn", 0, 0),
        ("region", n as usize, 1),
    ] {
        let (got, total) = group_rows(&out, "quality", field);
        assert_eq!(total["requests"], requests as i64, "{field}");
        assert_eq!(got.len(), values, "{field}");
    }
    assert_eq!(
        group_rows(&out, "quality", "region").0[r#"["US","Unknown"]"#]["requests"],
        n
    );
}

/// `bytes` as a summary written before the transport, origin, urls, quality and city rows existed.
fn without_new_rows(bytes: &[u8]) -> Vec<u8> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let schema = builder.schema().clone();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_key_value_metadata(kv)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (name, field, value) = (col("bundle"), col("field"), col("value"));
        let new = |i: usize| {
            let g = if name.value(i) == "dim_totals" {
                value.value(i)
            } else {
                name.value(i)
            };
            let f = (!field.is_null(i)).then(|| field.value(i));
            matches!(
                g,
                "transport"
                    | "origin"
                    | "urls"
                    | "quality"
                    | "header_ip"
                    | "bot_ua"
                    | "tls_fp"
                    | "value"
                    | "proxy"
            ) || (g == "series5" && matches!(f, Some("origin" | "city")))
                || f == Some(summary::REF_STEM)
        };
        let mut start = None;
        for i in 0..=batch.num_rows() {
            let keep = i < batch.num_rows() && !new(i);
            match (keep, start) {
                (true, None) => start = Some(i),
                (false, Some(at)) => {
                    writer.write(&batch.slice(at, i - at)).unwrap();
                    start = None;
                }
                _ => {}
            }
        }
    }
    writer.into_inner().unwrap()
}

#[test]
fn bundles_drop_new_rows_unless_every_source_has_them() {
    use fastly_log_analytics::ingest::bundle::{self as bundles, Unit};
    let (b1, b2) = (
        batch(&fixture_sv(0, 400)),
        batch(&fixture_sv(WINDOW * 1000, 500)),
    );
    let w1 = summary::write_summary(&[&b1], BASE).unwrap();
    let w2 = summary::write_summary(&[&b2], BASE + WINDOW).unwrap();
    let old = without_new_rows(&w1);
    let stripped = read(&old);
    assert!(bundle(&stripped, "transport").is_empty());
    assert_eq!(bundle(&stripped, "dims"), bundle(&read(&w1), "dims"));

    let new_fields = |out: &[Map<String, Value>]| -> BTreeSet<(String, String)> {
        bundle(out, "dim_totals")
            .into_iter()
            .map(|r| {
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["field"].as_str().unwrap().to_owned(),
                )
            })
            .filter(|(g, f)| {
                matches!(
                    g.as_str(),
                    "transport"
                        | "origin"
                        | "urls"
                        | "quality"
                        | "header_ip"
                        | "bot_ua"
                        | "tls_fp"
                        | "value"
                        | "proxy"
                ) || (g == "series5" && matches!(f.as_str(), "origin" | "city"))
            })
            .collect()
    };
    let stems = |out: &[Map<String, Value>]| {
        ["all_fields", "field_totals"]
            .into_iter()
            .flat_map(|b| bundle(out, b))
            .filter(|r| r["field"] == summary::REF_STEM)
            .count()
    };
    let both = read(&bundles::merge(&[&w1, &w2], BASE, Unit::Hour).unwrap());
    assert_eq!(new_fields(&both).len(), 24);
    assert!(stems(&both) > 1);

    let mixed_bytes = bundles::merge(&[&old, &w2], BASE, Unit::Hour).unwrap();
    let mixed = read(&mixed_bytes);
    assert!(new_fields(&mixed).is_empty());
    assert_eq!(stems(&mixed), 0);
    for name in [
        "transport",
        "origin",
        "urls",
        "quality",
        "header_ip",
        "bot_ua",
        "tls_fp",
        "value",
        "proxy",
    ] {
        assert!(bundle(&mixed, name).is_empty(), "{name}");
    }
    assert!(!bundle(&mixed, "series5")
        .iter()
        .any(|r| r["field"] == "origin" || r["field"] == "city"));
    assert!(bundle(&both, "series5")
        .iter()
        .any(|r| r["field"] == "city"));
    assert!(bundle(&both, "quality")
        .iter()
        .any(|r| r["field"] == "overall"));
    // Older fields still merge both sources.
    assert_eq!(keyed(&mixed, "dims"), keyed(&both, "dims"));

    // A day over an hour that lacks them lacks them too; over full hours it keeps them.
    let full_hour = bundles::merge(&[&w1, &w2], BASE, Unit::Hour).unwrap();
    let day = read(&bundles::merge(&[&mixed_bytes, &full_hour], BASE, Unit::Day).unwrap());
    assert!(new_fields(&day).is_empty());
    assert_eq!(stems(&day), 0);
    let day = read(&bundles::merge(&[&full_hour], BASE, Unit::Day).unwrap());
    assert_eq!(new_fields(&day).len(), 24);
    assert_eq!(stems(&day), stems(&both));
}

#[test]
fn series5_keeps_the_top_asns_and_marks_the_cut() {
    let k = summary::SERIES_ASN_K as i64;
    let mut rows = Vec::new();
    for asn in 1..=k + 10 {
        for _ in 0..asn {
            rows.push(json!({"timestamp": iso(BASE * 1000 + asn), "asn": asn}));
        }
    }
    let b = batch(&rows);
    let out = read(&summary::write_summary(&[&b], BASE).unwrap());
    let asns: BTreeSet<i64> = bundle(&out, "series5")
        .iter()
        .filter(|r| r["field"] == "asn")
        .map(|r| r["value"].as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(asns, (11..=k + 10).collect());
    let total = bundle(&out, "dim_totals")
        .into_iter()
        .find(|r| r["value"] == "series5" && r["field"] == "asn")
        .unwrap();
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    assert_eq!(
        bundle(&out, "dims")
            .iter()
            .filter(|r| r["field"] == "asn")
            .count() as i64,
        k + 10,
        "dims keep DIM_K"
    );
}

/// The rows of `bundle_name` keyed by `(field, value, bucket)`, without the bundle column.
fn keyed(
    out: &[Map<String, Value>],
    bundle_name: &str,
) -> BTreeMap<(String, String, i64), Map<String, Value>> {
    bundle(out, bundle_name)
        .into_iter()
        .map(|r| {
            let key = (
                r.get("field")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                r.get("value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                r["bucket"].as_i64().unwrap(),
            );
            (key, r.clone())
        })
        .collect()
}

fn same_rows(a: &Map<String, Value>, b: &Map<String, Value>, what: &str) {
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "{what}"
    );
    for (k, v) in a {
        match (v, &b[k]) {
            (Value::Number(x), Value::Number(y)) if x.is_f64() || y.is_f64() => {
                close(v, y.as_f64().unwrap());
                let _ = x;
            }
            (x, y) => assert_eq!(x, y, "{what} {k}"),
        }
    }
}

#[test]
fn hour_bundles_merge_v3_rows_like_one_window_over_all_rows() {
    use fastly_log_analytics::ingest::bundle::{self as bundles, Unit};
    let first = fixture_sv(0, 400);
    let second = fixture_sv(WINDOW * 1000, 500);
    let (b1, b2) = (batch(&first), batch(&second));
    let whole = read(&summary::write_summary(&[&b1, &b2], BASE).unwrap());
    let w1 = summary::write_summary(&[&b1], BASE).unwrap();
    let w2 = summary::write_summary(&[&b2], BASE + WINDOW).unwrap();
    let hour_bytes = bundles::merge(&[&w1, &w2], BASE, Unit::Hour).unwrap();
    assert_eq!(meta(&hour_bytes, "flp_summary").as_deref(), Some("3"));
    let hour = read(&hour_bytes);
    for name in [
        "dims",
        "series5",
        "bins",
        "assets",
        "transport",
        "origin",
        "urls",
        "header_ip",
        "bot_ua",
        "tls_fp",
        "value",
        "proxy",
        "dim_totals",
    ] {
        let (a, b) = (keyed(&whole, name), keyed(&hour, name));
        assert_eq!(
            a.keys().collect::<Vec<_>>(),
            b.keys().collect::<Vec<_>>(),
            "{name}"
        );
        for (key, row) in &a {
            same_rows(row, &b[key], &format!("{name} {key:?}"));
        }
    }
    assert_eq!(
        keyed(&hour, "series5")
            .keys()
            .filter(|k| k.0 == "overall")
            .map(|k| k.2)
            .collect::<Vec<_>>(),
        [BASE * 1000, (BASE + WINDOW) * 1000]
    );
    let sample = |out: &[Map<String, Value>]| -> Vec<i64> {
        bundle(out, "sample")
            .iter()
            .map(|r| r["sample_key"].as_i64().unwrap())
            .collect()
    };
    assert_eq!(sample(&hour), sample(&whole));
    assert_eq!(sample(&hour).len(), summary::SAMPLE_K);
    let stems = |out: &[Map<String, Value>]| -> BTreeMap<String, Value> {
        bundle(out, "all_fields")
            .into_iter()
            .filter(|r| r["field"] == summary::REF_STEM)
            .map(|r| (r["value"].as_str().unwrap().to_owned(), r["count"].clone()))
            .collect()
    };
    assert!(!stems(&hour).is_empty());
    assert_eq!(stems(&hour), stems(&whole));

    // A day bundle keeps hour buckets for the timed rows.
    let day = read(&bundles::merge(&[&hour_bytes], BASE, Unit::Day).unwrap());
    let overall: Vec<_> = bundle(&day, "series5")
        .into_iter()
        .filter(|r| r["field"] == "overall")
        .collect();
    assert_eq!(overall.len(), 1);
    assert_eq!(overall[0]["bucket"], BASE * 1000);
    assert_eq!(overall[0]["requests"], 900);
    let mut both = hist_cell(
        &keyed(&hour, "series5")[&("overall".into(), String::new(), BASE * 1000)],
        "ttfb_hist",
    )
    .unwrap();
    both.merge(
        &hist_cell(
            &keyed(&hour, "series5")[&("overall".into(), String::new(), (BASE + WINDOW) * 1000)],
            "ttfb_hist",
        )
        .unwrap(),
    );
    assert_eq!(hist_cell(overall[0], "ttfb_hist"), Some(both));
    assert_eq!(sample(&day), sample(&hour));
}

/// `fixture_v3` rows with the /security and /fastly-value columns: IPs (some missing or ''),
/// human, bot and fetcher agents, referrals, TLS fingerprints, proxy types, bot names, Image
/// Optimizer formats and bytes (an output of 0), RTT minimums (some with a TCP RTT, for tunnel,
/// WiFi and direct rows) and every cache class.
fn fixture_sv(offset_ms: i64, n: i64) -> Vec<Value> {
    let uas = [
        Value::from("Mozilla/5.0"),
        "Googlebot/2.1".into(),
        "ChatGPT-User/1.0".into(),
        "perplexity-user".into(),
        "".into(),
        "curl claude-externalfetcher".into(),
        "HeadlessChrome".into(),
        Value::Null,
    ];
    let referers = [
        Value::from("https://www.example.com/a"),
        "http://news.site.org/x".into(),
        "-".into(),
        "".into(),
        Value::Null,
        "HTTPS://Foo.Bar:443/".into(),
    ];
    let mut rows = fixture_v3(offset_ms, n);
    for (k, row) in rows.iter_mut().enumerate() {
        if k % 11 != 0 {
            row["ip"] = if k % 23 == 1 {
                "".into()
            } else {
                format!("10.1.0.{}", k % 37).into()
            };
        }
        row["ua"] = uas[k % uas.len()].clone();
        row["referer"] = referers[k % referers.len()].clone();
        match k % 5 {
            0 => row["ja3"] = format!("ja3-{}", k % 7).into(),
            1 => row["ja3"] = "".into(),
            _ => {}
        }
        if k % 3 != 2 {
            row["ja4"] = format!("t13d{}", k % 4).into();
        }
        if k % 4 != 3 {
            row["p_type"] = ["", "VPN", "TOR"][k % 3].into();
        }
        if k % 6 != 5 {
            row["_bot_name"] = ["", "googlebot", "bingbot"][k % 3].into();
        }
        if k % 4 == 1 {
            row["io_input_format"] = ["jpeg", "png", ""][k / 4 % 3].into();
            row["io_output_format"] = ["webp", "avif", ""][k / 12 % 3].into();
            row["io_input_bytes"] = (10_000 + k * 31).into();
            row["io_output_bytes"] = if k % 24 == 1 { 0 } else { 2_000 + k * 7 }.into();
        }
        if k % 3 == 0 {
            row["rtt_min"] = (50 + k % 90).into();
            if k % 2 == 0 {
                row["tcp_rtt"] = (400 + (k % 7) * 600).into();
            }
        }
        match k % 9 {
            0 => row["cache"] = "SYNTH".into(),
            3 => row["cache"] = "".into(),
            4 => row["cache"] = "ERROR".into(),
            6 => row["cache"] = Value::Null,
            _ => {}
        }
        if k % 7 == 0 {
            row["waf_sig"] = "a, b,,c".into();
        }
    }
    rows
}

type Want = BTreeMap<(String, String, String, i64), Map<String, Value>>;

/// The expected row of `group`/`field`/`value` at `bucket`, created empty.
fn want_row<'a>(
    want: &'a mut Want,
    group: &str,
    field: &str,
    value: &str,
    bucket: i64,
) -> &'a mut Map<String, Value> {
    want.entry((group.into(), field.into(), value.into(), bucket))
        .or_insert_with(|| {
            let mut m = Map::new();
            m.insert("bundle".into(), group.into());
            m.insert("bucket".into(), bucket.into());
            m.insert("field".into(), field.into());
            m.insert("value".into(), value.into());
            m
        })
}

/// Adds `v` to `row[col]`, as an integer when both are.
fn add_to(row: &mut Map<String, Value>, col: &str, v: Value) {
    let sum = match (row.get(col), &v) {
        (None, _) => v,
        (Some(Value::Number(a)), Value::Number(b)) if a.is_i64() && b.is_i64() => {
            (a.as_i64().unwrap() + b.as_i64().unwrap()).into()
        }
        (Some(a), b) => (a.as_f64().unwrap() + b.as_f64().unwrap()).into(),
    };
    row.insert(col.into(), sum);
}

fn same_map(a: &Map<String, Value>, b: &Map<String, Value>, what: &str) {
    let keys = |m: &Map<String, Value>| m.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(keys(a), keys(b), "{what}");
    for (k, v) in a {
        match (v, &b[k]) {
            (Value::Number(_), Value::Number(y)) => close(v, y.as_f64().unwrap()),
            (x, y) => assert_eq!(x, y, "{what} {k}"),
        }
    }
}

/// /fastly-value's cache class.
fn value_class(cache: Option<&str>) -> &'static str {
    let upper = cache.unwrap_or("").to_uppercase();
    match ["HIT", "MISS", "PASS", "SYNTH"]
        .into_iter()
        .find(|p| upper.starts_with(p))
    {
        Some(p) => p,
        None if upper.is_empty() => "",
        None => "OTHER",
    }
}

fn non_empty<'a>(r: &'a Value, key: &str) -> Option<&'a str> {
    s(r, key).filter(|v| !v.is_empty())
}

/// The /security and /fastly-value rows and their `dim_totals` the brute-force way.
fn security_value_want(rows: &[Value], bucket: i64) -> (Want, BTreeMap<(String, String), i64>) {
    let mut want = Want::new();
    let mut totals: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut total = |g: &str, f: &str, n: i64| {
        *totals.entry((g.into(), f.into())).or_default() += n;
    };
    for (g, f) in [
        ("header_ip", "ip"),
        ("bot_ua", "ua"),
        ("tls_fp", "fp"),
        ("tls_fp", "fp_country"),
        ("proxy", "sample"),
    ]
    .into_iter()
    .chain(summary::VALUE_FIELDS.iter().map(|f| ("value", *f)))
    {
        total(g, f, 0);
    }
    for r in rows {
        let ts = ts_ms(r);
        let day = ts.div_euclid(86_400_000) * 86_400_000;
        let ip = non_empty(r, "ip");
        if let (Some(ip), Some(h)) = (ip, i(r, "req_header_bytes")) {
            total("header_ip", "ip", 1);
            let row = want_row(&mut want, "header_ip", "ip", ip, bucket);
            let max = i(&Value::Object(row.clone()), "value_max").unwrap_or(i64::MIN);
            row.insert("value_max".into(), max.max(h).into());
        }
        if let Some(ua) = non_empty(r, "ua") {
            total("bot_ua", "ua", 1);
            let row = want_row(&mut want, "bot_ua", "ua", ua, bucket);
            add_to(row, "resp_bytes_sum", i(r, "resp_bytes").unwrap().into());
            add_to(row, "requests", 1.into());
            add_to(row, "count", i64::from(ip.is_some()).into());
        }
        if let Some(fp) = non_empty(r, "ja3").or_else(|| non_empty(r, "ja4")) {
            total("tls_fp", "fp", 1);
            let row = want_row(&mut want, "tls_fp", "fp", fp, bucket);
            add_to(row, "requests", 1.into());
            add_to(
                row,
                "count",
                i64::from(non_empty(r, "p_type").is_some()).into(),
            );
            if let Some(country) = s(r, "country") {
                total("tls_fp", "fp_country", 1);
                let key = json!([fp, country]).to_string();
                let row = want_row(&mut want, "tls_fp", "fp_country", &key, bucket);
                add_to(row, "requests", 1.into());
            }
        }

        let mut value = |field: &str, v: &str, cols: &[(&str, Value)]| {
            total("value", field, 1);
            let row = want_row(&mut want, "value", field, v, day);
            add_to(row, "requests", 1.into());
            for (col, n) in cols {
                add_to(row, col, n.clone());
            }
        };
        let class = value_class(s(r, "cache"));
        let bytes = i(r, "resp_bytes").unwrap();
        value(
            "cache",
            class,
            &[
                ("resp_bytes_sum", bytes.into()),
                ("count", 1.into()),
                ("elapsed_sum", (i(r, "elapsed").unwrap() as f64).into()),
                ("elapsed_count", 1.into()),
            ],
        );
        let edge = r.get("edge").and_then(Value::as_bool);
        if edge == Some(false) {
            value("shield", class, &[]);
        }
        let output = non_empty(r, "io_output_format");
        if output.is_some() {
            let leg = edge.map_or("", |e| if e { "true" } else { "false" });
            value(
                "io",
                leg,
                &[("resp_bytes_sum", bytes.into()), ("count", 1.into())],
            );
        }
        let io = i(r, "io_input_bytes").zip(i(r, "io_output_bytes"));
        if let Some((a, b)) = io {
            value(
                "io_both",
                "",
                &[("io_in_sum", a.into()), ("io_out_sum", b.into())],
            );
        }
        if let Some(name) = non_empty(r, "_bot_name") {
            value("bot_name", name, &[]);
        }
        for token in s(r, "waf_sig").unwrap_or("").split(',') {
            if !token.is_empty() {
                value("waf_signal", token, &[]);
            }
        }
        if let (Some(input), Some(output)) = (non_empty(r, "io_input_format"), output) {
            let ratio = io
                .filter(|(_, b)| *b != 0)
                .map(|(a, b)| a as f64 / b as f64);
            value(
                "io_pair",
                &json!([input, output]).to_string(),
                &[
                    ("ratio_sum", ratio.unwrap_or(0.0).into()),
                    ("ratio_count", i64::from(ratio.is_some()).into()),
                ],
            );
        }

        if let Some(rtt) = i(r, "rtt_min") {
            total("proxy", "sample", 1);
            let key = format!("{:019}", fnv(s(r, "rid").unwrap()));
            let row = want_row(&mut want, "proxy", "sample", &key, bucket);
            row.insert("requests".into(), 1.into());
            row.insert("rtt_min".into(), (rtt as f64).into());
            for (col, from) in [
                ("ip", "ip"),
                ("asn", "asn"),
                ("tcp_rtt", "tcp_rtt"),
                ("lat_sum", "lat"),
                ("lon_sum", "lon"),
                ("pop", "pop"),
                ("country", "country"),
                ("label", "city"),
            ] {
                match r.get(from) {
                    None | Some(Value::Null) => {}
                    Some(Value::Number(n)) if col != "asn" => {
                        row.insert(col.into(), n.as_f64().unwrap().into());
                    }
                    Some(v) => {
                        row.insert(col.into(), v.clone());
                    }
                }
            }
        }
    }
    (want, totals)
}

#[test]
fn security_and_value_rows_match_a_brute_force_computation() {
    let rows = fixture_sv(0, 400);
    let out = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());
    let (want, totals) = security_value_want(&rows, BASE * 1000);
    let groups = ["header_ip", "bot_ua", "tls_fp", "value", "proxy"];
    let got: Want = out
        .iter()
        .filter(|r| groups.contains(&r["bundle"].as_str().unwrap()))
        .map(|r| {
            let key = (
                r["bundle"].as_str().unwrap().to_owned(),
                r["field"].as_str().unwrap().to_owned(),
                r["value"].as_str().unwrap().to_owned(),
                r["bucket"].as_i64().unwrap(),
            );
            (key, r.clone())
        })
        .collect();
    assert_eq!(
        got.keys().collect::<Vec<_>>(),
        want.keys().collect::<Vec<_>>()
    );
    for (key, row) in &want {
        same_map(&got[key], row, &format!("{key:?}"));
    }
    for g in groups {
        assert!(got.keys().any(|k| k.0 == g), "{g} has rows");
    }
    let got_totals: BTreeMap<(String, String), i64> = bundle(&out, "dim_totals")
        .into_iter()
        .filter(|r| groups.contains(&r["value"].as_str().unwrap()))
        .map(|r| {
            assert_eq!(r["truncated"], false);
            (
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["field"].as_str().unwrap().to_owned(),
                ),
                r["requests"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(got_totals, totals);

    // Referral stems are an all_fields field, with the catalog fields' IP counts.
    let mut stems: BTreeMap<String, i64> = BTreeMap::new();
    for r in &rows {
        if let Some(stem) = summary::ref_stem(s(r, "ip"), s(r, "ua"), s(r, "referer")) {
            *stems.entry(stem).or_default() += 1;
        }
    }
    assert!(stems.len() >= 4, "{stems:?}");
    let got: BTreeMap<String, i64> = bundle(&out, "all_fields")
        .into_iter()
        .filter(|r| r["field"] == summary::REF_STEM)
        .map(|r| {
            (
                r["value"].as_str().unwrap().to_owned(),
                r["count"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(got, stems);
    let total = bundle(&out, "field_totals")
        .into_iter()
        .find(|r| r["field"] == summary::REF_STEM)
        .unwrap();
    assert_eq!(total["count"], stems.values().sum::<i64>());
}

#[test]
fn referral_stems_follow_the_raw_regexes() {
    let ip = Some("10.0.0.1");
    for (ua, referer, want) in [
        ("Mozilla", "https://www.example.com/a", Some("example")),
        ("Mozilla", "http://news.site.org/x", Some("news")),
        ("Mozilla", "-", None),
        ("Mozilla", "", None),
        ("ChatGPT-User/1.0", "", Some("chatgpt")),
        ("perplexity-user", "-", Some("perplexity")),
        ("Googlebot", "https://g.com/", None),
        ("HeadlessChrome", "https://g.com/", None),
        ("Mozilla", "https://www./", Some("www")),
        ("Mozilla", "HTTPS://Foo.Bar:443/", Some("foo")),
        ("Mozilla", "ftp://x https://y.z/", Some("y")),
        ("Mozilla", "https:///nohost http://late.io", Some("late")),
        ("x -user", "https://r.com", Some("r")),
        ("abc-userx", "", Some("abc")),
        ("a.b-externalfetcher", "", Some("b")),
        ("-user", "", None),
        ("Mozilla", "https://www.ex ample.com", Some("ex")),
        ("Mozilla", "https://wwwx.com", Some("wwwx")),
        ("Mozilla", "http://www.:8080", Some("www")),
        ("Mozilla", "no scheme here", None),
        ("MeinBot-User", "https://x.com", None),
    ] {
        assert_eq!(
            summary::ref_stem(ip, Some(ua), Some(referer)).as_deref(),
            want,
            "{ua:?} {referer:?}"
        );
    }
    for ip in [None, Some("")] {
        assert_eq!(
            summary::ref_stem(ip, Some("Mozilla"), Some("https://a.com")),
            None
        );
    }
    assert_eq!(
        summary::ref_stem(ip, None, Some("https://a.com")).as_deref(),
        Some("a")
    );
}

#[test]
fn security_and_value_groups_cut_to_their_top_k() {
    let header_k = summary::HEADER_IP_K as i64;
    let mut rows: Vec<Value> = (0..header_k + 20)
        .map(|n| {
            json!({
                "timestamp": iso(BASE * 1000 + n),
                "ip": format!("10.2.{}.{}", n / 256, n % 256),
                "req_header_bytes": 100 + n * 10,
                "rid": format!("r{n}"),
                "rtt_min": 5,
                "_bot_name": format!("bot{n:04}"),
            })
        })
        .collect();
    let extra = summary::SAMPLE_K as i64 + summary::VALUE_TOP_K as i64;
    rows.extend((0..extra).map(|n| {
        json!({
            "timestamp": iso(BASE * 1000 + n),
            "rid": format!("s{n}"),
            "rtt_min": 7,
            "_bot_name": if n < 50 { "bot0000".to_owned() } else { format!("bot{n:04}") },
        })
    }));
    let out = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());

    let (ips, total) = group_rows(&out, "header_ip", "ip");
    assert_eq!(total["requests"], header_k + 20);
    assert_eq!(total["truncated"], true);
    let kept: BTreeSet<i64> = ips
        .values()
        .map(|r| r["value_max"].as_i64().unwrap())
        .collect();
    assert_eq!(kept, (20..header_k + 20).map(|n| 100 + n * 10).collect());

    let (sample, total) = group_rows(&out, "proxy", "sample");
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    let mut keys: Vec<i64> = rows.iter().map(|r| fnv(s(r, "rid").unwrap())).collect();
    keys.sort_unstable();
    keys.truncate(summary::SAMPLE_K);
    assert_eq!(
        sample.keys().cloned().collect::<Vec<_>>(),
        keys.iter().map(|k| format!("{k:019}")).collect::<Vec<_>>()
    );

    let bots: Vec<&Map<String, Value>> = bundle(&out, "value")
        .into_iter()
        .filter(|r| r["field"] == "bot_name")
        .collect();
    assert_eq!(bots.len(), summary::VALUE_TOP_K);
    assert_eq!(bots[0]["value"], "bot0000");
    let total = bundle(&out, "dim_totals")
        .into_iter()
        .find(|r| r["value"] == "value" && r["field"] == "bot_name")
        .unwrap();
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
}

#[test]
fn hll_matches_the_reference_vectors() {
    // Vectors from an independent Python HLL++ (p=12, sparse p'=25, fla's md5 hash and rank),
    // shared with frontend/lib/cloud-engine/dashboard/hll.test.ts.
    let sketch = |items: &mut dyn Iterator<Item = String>| {
        let mut h = Hll::default();
        items.for_each(|ip| h.add_hash(summary::hll_hash(&ip)));
        h
    };
    let one = sketch(&mut std::iter::once("192.0.2.1".to_owned()));
    assert_eq!(hex::encode(one.to_bytes()), "020c0001c5f6e48903");
    assert_eq!(one.count(), 1);

    let few = sketch(&mut (0..20).map(|i| format!("10.0.0.{i}")));
    let bytes = few.to_bytes();
    assert_eq!(bytes.len(), 85);
    assert_eq!(
        hex::encode(Md5::digest(&bytes)),
        "39c35cb9e9b2f005a5373f90038ca841"
    );
    assert_eq!(few.count(), 20);

    let many = sketch(&mut (0..2000).map(|i| format!("10.0.{}.{}", i / 256, i % 256)));
    let bytes = many.to_bytes();
    assert_eq!(bytes.len(), 3 + 4096);
    assert_eq!(
        &bytes[..12],
        &hex::decode("020c01030000010101000200").unwrap()[..]
    );
    assert_eq!(
        hex::encode(Md5::digest(&bytes)),
        "1c52cca58c4d6b0afa94b13423e57317"
    );
    assert_eq!(many.count(), 1977);

    let mut merged = Hll::from_bytes(&one.to_bytes()).unwrap();
    merged.merge(&many);
    let mut both = many.clone();
    both.add_hash(summary::hll_hash("192.0.2.1"));
    assert_eq!(merged.to_bytes(), both.to_bytes());
    assert!(Hll::from_bytes(&bytes[..4098]).is_none());
}

#[test]
fn summaries_are_reproducible_and_zstd_compressed() {
    let rows = fixture();
    let b = batch(&rows);
    let first = summary::write_summary(&[&b], BASE).unwrap();
    let again = summary::write_summary(&[&batch(&rows)], BASE).unwrap();
    assert_eq!(first, again);
    for bytes in [first, convert::write_parquet(&[&b]).unwrap()] {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
        for group in builder.metadata().row_groups() {
            for column in group.columns() {
                assert!(
                    matches!(column.compression(), Codec::ZSTD(level) if level.compression_level() == 1),
                    "{:?}",
                    column.compression()
                );
            }
        }
    }
    let key = summary::summary_key(
        &summary::summary_prefix("", "request"),
        BASE,
        BASE + WINDOW,
        "p1s0",
        400,
    );
    assert_eq!(
        key,
        "processed/summary/request/year=2027/month=01/day=15/hour=08/summary=1800000000-1800000300-p1s0-n400.parquet"
    );
}

#[test]
fn ins_url_counts_status_ge_500_and_case_sensitive_cache_prefixes() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000), "url": "/a", "status": 503, "cache": "HIT"}),
        json!({"timestamp": iso(BASE * 1000 + 1), "url": "/a", "status": 600, "cache": "hit-stale"}),
        json!({"timestamp": iso(BASE * 1000 + 2), "url": "/a", "status": 200, "cache": "HIT-CLUSTER"}),
        json!({"timestamp": iso(BASE * 1000 + 3), "url": "/a", "status": 200, "cache": "MISS-CLUSTER"}),
        json!({"timestamp": iso(BASE * 1000 + 4), "url": "/a", "status": 200, "cache": "PASS-FOO"}),
        json!({"timestamp": iso(BASE * 1000 + 5), "url": Value::Null, "status": 503, "cache": "HIT"}),
    ];
    let b = batch(&rows);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());
    let total = summary_rows
        .iter()
        .find(|r| r["bundle"] == "dim_totals" && r["field"] == "url" && r["value"] == "ins_url")
        .expect("ins_url dim_totals row");
    assert_eq!(total["requests"], 5);
    assert_eq!(total["truncated"], false);

    let url_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "ins_url" && r["field"] == "url")
        .collect();
    assert_eq!(url_rows.len(), 1);
    let row = url_rows[0];
    assert_eq!(row["value"], "/a");
    assert_eq!(row["requests"], 5);
    assert_eq!(row["status_5xx"], 2);
    assert_eq!(row["hit_requests"], 2);
    assert_eq!(row["miss_requests"], 1);
    assert_eq!(row["pass_requests"], 1);
}

#[test]
fn ins_ip_counts_denied_auth_and_404_per_ip() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000), "ip": "1.1.1.1", "url": "/LOGIN", "status": 401}),
        json!({"timestamp": iso(BASE * 1000 + 1), "ip": "1.1.1.1", "url": "/missing", "status": 404}),
        json!({"timestamp": iso(BASE * 1000 + 2), "ip": "2.2.2.2", "url": "/login", "status": 200}),
        json!({"timestamp": iso(BASE * 1000 + 3), "ip": "3.3.3.3", "url": "/x", "status": 403}),
        json!({"timestamp": iso(BASE * 1000 + 4), "ip": "4.4.4.4", "url": Value::Null, "status": 401}),
        json!({"timestamp": iso(BASE * 1000 + 5), "ip": "", "url": "/missing", "status": 404}),
        json!({"timestamp": iso(BASE * 1000 + 6), "ip": Value::Null, "url": "/missing", "status": 404}),
    ];
    let b = batch(&rows);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());
    let total = summary_rows
        .iter()
        .find(|r| r["bundle"] == "dim_totals" && r["field"] == "ip" && r["value"] == "ins_ip")
        .expect("ins_ip dim_totals row");
    assert_eq!(total["requests"], 2);
    assert_eq!(total["truncated"], false);

    let ip_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "ins_ip" && r["field"] == "ip")
        .collect();
    assert_eq!(ip_rows.len(), 1);
    let row = ip_rows[0];
    assert_eq!(row["value"], "1.1.1.1");
    assert_eq!(row["requests"], 2);
    assert_eq!(row["errors"], 1);
    assert_eq!(row["count"], 1);
}

/// Writes `frontend/lib/cloud-engine/dashboard/__fixtures__/summaries/`: raw window Parquet,
/// their window summaries, hour and day bundles and `index.json`, for the dashboard's
/// summary-vs-raw tests.
/// Run with `cargo test --test summary -- --ignored write_dashboard_fixtures`.
#[test]
#[ignore = "regenerates the web dashboard fixtures"]
fn write_dashboard_fixtures() {
    let rows: Vec<Value> = (0..1600i64)
        .map(|i| {
            let ms = BASE * 1000 + (i * 4_513) % (2 * 3600 * 1000);
            let in_first_hour = ms < (BASE + 3600) * 1000;
            let url = if i % 3 == 0 {
                format!("/hot/{}", i % 7)
            } else if in_first_hour {
                format!("/p/{}", i % 620)
            } else {
                format!("/q/{}", i % 40)
            };
            let k = i as usize;
            let timestamp: Value = if i % 401 == 7 {
                Value::Null
            } else {
                iso(ms).into()
            };
            let status = [200, 200, 304, 404, 503, 0][k % 6];
            let cache = ["HIT", "hit-stale", "HIT-CLUSTER", "MISS", "PASS", ""][k % 6];
            let host = ["a.example", "b.example", ""][k % 3];
            let method = ["GET", "GET", "POST"][k % 3];
            let country = ["US", "DE", "JP", "BR"][k % 4];
            let asn = [13335, 15169, 16509][k % 3];
            let pop = ["IAD", "LHR", "NRT", ""][k % 4];
            let mut row = json!({
                "timestamp": timestamp,
                "status": status,
                "cache": cache,
                "resp_bytes": 100 + i * 7,
                "host": host,
                "url": url,
                "method": method,
                "country": country,
                "asn": asn,
                "pop": pop,
                "edge": i % 4 != 0,
                "is_ipv6": i % 5 == 0,
                "ttl": (i % 4) as f64 * 1.5,
                "age": (i % 7) as f64 + 0.25,
                "ua": format!("ua{}", i % 9),
            });
            if i % 17 != 0 {
                row["ip"] = format!("10.{}.{}.{}", i % 3, (i / 7) % 40, i % 23).into();
            }
            row
        })
        .collect();
    write_summary_fixtures("dashboard/__fixtures__/summaries", &rows, &[]);
}

/// Writes `frontend/lib/cloud-engine/__fixtures__/network-summaries/`: rows carrying the
/// /network columns (RTT, loss, geo, ASN, protocol, TTFB), with their summaries and bundles,
/// for the /network rollup-vs-raw parity tests.
/// Run with `cargo test --test summary -- --ignored write_network_fixtures`.
#[test]
#[ignore = "regenerates the web /network fixtures"]
fn write_network_fixtures() {
    const CITIES: [(&str, &str, &str, f64, f64); 6] = [
        ("US", "Ashburn", "VA", 39.04, -77.49),
        ("US", "Seattle", "WA", 47.61, -122.33),
        ("DE", "Frankfurt", "HE", 50.11, 8.68),
        ("JP", "Tokyo", "13", 35.68, 139.69),
        ("BR", "Sao Paulo", "SP", -23.55, -46.63),
        ("GB", "London", "ENG", 51.51, -0.13),
    ];
    let rows: Vec<Value> = (0..1600i64)
        .map(|i| {
            let ms = BASE * 1000 + (i * 4_513) % (2 * 3600 * 1000);
            let k = i as usize;
            let (country, city, region, lat, lon) = CITIES[k % 6];
            let mut row = json!({
                "timestamp": iso(ms),
                "status": ([200, 200, 304, 404, 503, 500, 0, 302][k % 8]),
                "cache": (["HIT", "MISS", "PASS", "HIT-STALE", "MISS"][k % 5]),
                "resp_bytes": 100 + i * 7,
                "pop": (["IAD", "LHR", "NRT", "FRA", "GRU"][(k / 3) % 5]),
                "country": country,
                "region": region,
                "asn": ([13335, 15169, 16509, 7922, 3320, 2516][(k / 2) % 6]),
                "proto": (["2", "3", "1.1"][k % 3]),
                "ttfb": 0.000_5 * ((i * 13) % 400) as f64,
                "tcp_rtt": 2_000 + (i * 37) % 40_000 + 9_000 * (k % 6) as i64,
                "ploss": 0.002 * (i % 5) as f64,
                "rtt_var": 500 + i % 700,
                "retrans": i % 4,
                "delivery_rate": 100_000 + i * 31,
                "elapsed": (i * 97) % 300_000,
            });
            // Nulls exercise each coalesce: unknown city and country, ASN 0, protocol
            // default and absent RTT, loss, elapsed and timestamp.
            if i % 11 == 0 {
                row["city"] = Value::Null;
            } else {
                row["city"] = city.into();
                row["lat"] = lat.into();
                row["lon"] = lon.into();
            }
            if i % 13 == 0 {
                row["country"] = Value::Null;
            }
            if i % 19 == 0 {
                row["asn"] = Value::Null;
            }
            if i % 23 == 0 {
                row["proto"] = Value::Null;
            }
            if i % 7 == 0 {
                row["tcp_rtt"] = Value::Null;
            }
            if i % 9 == 0 {
                row["ploss"] = Value::Null;
            }
            if i % 17 == 0 {
                row["elapsed"] = Value::Null;
            }
            if i % 3 == 0 {
                row["client_ploss"] = (0.001 * (i % 6) as f64).into();
            }
            if i % 4 == 1 {
                row["client_tcpi_rttvar"] = (250 + (i * 3) % 900).into();
            }
            if i % 401 == 7 {
                row["timestamp"] = Value::Null;
            }
            row
        })
        .collect();
    let client = [
        ("client_ploss", convert::Kind::Float64),
        ("client_tcpi_rttvar", convert::Kind::Float64),
    ];
    write_summary_fixtures("__fixtures__/network-summaries", &rows, &client);
}

/// Writes `frontend/lib/cloud-engine/__fixtures__/origin-summaries/`: rows carrying the
/// /origin and /performance columns (origin IP, status, timings and bytes, TTL, elapsed, ASN), with
/// their summaries and bundles, for the rollup-vs-raw parity tests.
/// Run with `cargo test --test summary -- --ignored write_origin_fixtures`.
#[test]
#[ignore = "regenerates the web /origin and /performance fixtures"]
fn write_origin_fixtures() {
    const TTLS: [f64; 9] = [0.0, 30.0, 59.5, 60.0, 300.0, 3600.0, 7200.0, 86_400.0, 1e6];
    const OIPS: [&str; 6] = [
        "10.0.0.1",
        "10.0.0.2",
        "10.0.0.3",
        "192.0.2.7",
        "-",
        "2001:db8::1",
    ];
    let rows: Vec<Value> = (0..1600i64)
        .map(|i| {
            let ms = BASE * 1000 + (i * 4_513) % (2 * 3600 * 1000);
            let k = i as usize;
            let cache = ["HIT", "MISS", "PASS", "HIT-STALE", "MISS"][k % 5];
            let mut row = json!({
                "timestamp": iso(ms),
                "status": ([200, 200, 304, 404, 503, 500, 200][k % 7]),
                "cache": cache,
                "resp_bytes": 100 + i * 7,
                "pop": (["IAD", "LHR", "NRT", "FRA", ""][(k / 3) % 5]),
                "edge": i % 4 != 0,
                "url": format!("/p/{}", i % 40),
                "ttfb": 0.000_05 + 0.000_2 * ((i * 13) % 400) as f64,
                "elapsed": 1_000 + (i * 97) % 300_000,
                "asn": ([13335, 15169, 16509, 7922][(k / 5) % 4]),
            });
            // Only origin fetches carry the origin columns, as on Fastly: HITs have none.
            if !cache.starts_with("HIT") {
                let ottfb = 1_500 + (i * 211) % 180_000;
                row["oip"] = OIPS[(k / 2) % 6].into();
                row["ost"] = ([200, 200, 503, 500, 404, 200, 302, 502][(k / 3) % 8]).into();
                row["ottfb"] = ottfb.into();
                row["obytes"] = (200 + (i * 61) % 50_000).into();
                if i % 6 != 0 {
                    let ottlb = ottfb + (i * 53) % 90_000;
                    row["ottlb"] = ottlb.into();
                    // Fastly's elapsed time spans the origin fetch, so CDN overhead is positive.
                    row["elapsed"] = (ottlb + 200 + (i * 37) % 20_000).into();
                }
            }
            if i % 9 == 0 {
                row["ttfb"] = Value::Null;
            }
            if i % 11 == 0 {
                row["elapsed"] = Value::Null;
            }
            if i % 17 == 0 {
                row["edge"] = Value::Null;
            }
            if i % 7 != 0 {
                row["ttl"] = TTLS[k % 9].into();
            }
            if i % 401 == 7 {
                row["timestamp"] = Value::Null;
            }
            row
        })
        .collect();
    write_summary_fixtures("__fixtures__/origin-summaries", &rows, &[]);
}

/// Writes `frontend/lib/cloud-engine/__fixtures__/security-value-summaries/`: rows carrying the
/// /security and /fastly-value columns (user agents, referers, TLS fingerprints, proxy types, bot
/// names, image-optimizer bytes, RTTs, WAF signals) over two hours, with their summaries and
/// bundles, for the rollup-vs-raw parity tests.
/// Run with `cargo test --test summary -- --ignored write_security_value_fixtures`.
#[test]
#[ignore = "regenerates the web /security and /fastly-value fixtures"]
fn write_security_value_fixtures() {
    let mut rows = fixture_sv(0, 1600);
    for (i, row) in rows.iter_mut().enumerate() {
        let ms = BASE * 1000 + (i as i64 * 4_513) % (2 * 3600 * 1000);
        row["timestamp"] = iso(ms).into();
        row["rid"] = format!("sv-{i}").into();
    }
    write_summary_fixtures("__fixtures__/security-value-summaries", &rows, &[]);
}

/// Writes `frontend/lib/cloud-engine/{rel}/`: 15-minute raw windows of `rows`, their window
/// summaries, hour bundles (and each hour's bundle without its last window), the day bundle
/// and `index.json`.
fn write_summary_fixtures(rel: &str, rows: &[Value], custom: &[(&str, convert::Kind)]) {
    use fastly_log_analytics::ingest::bundle::{self, Unit};
    const FIXTURE_WINDOW: i64 = 900;
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../frontend/lib/cloud-engine")
        .join(rel);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut text = Vec::new();
    for row in rows {
        writeln!(text, "{row}").unwrap();
    }
    let gz = zstd::stream::encode_all(text.as_slice(), 1).unwrap();
    let schema = convert::RequestSchema::new(
        custom
            .iter()
            .map(|(name, kind)| convert::CustomColumn {
                name: (*name).into(),
                kind: *kind,
                slot_only: false,
            })
            .collect(),
    );
    let converted = convert::convert_object_capped(
        &gz,
        FIXTURE_WINDOW,
        BASE + 3600,
        convert::MAX_DECOMPRESSED_BYTES,
        &schema,
    )
    .unwrap();
    let summaries = summary::summary_prefix("", "request");
    let bundles = bundle::bundle_prefix("", "request");
    let mut index = Vec::new();
    let mut save = |key: &str, bytes: &[u8], extra: Value| {
        let file = key.rsplit('/').next().unwrap().to_owned();
        std::fs::write(dir.join(&file), bytes).unwrap();
        let mut entry = json!({"key": key, "file": file, "size": bytes.len()});
        entry
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        index.push(entry);
    };
    let mut by_hour: BTreeMap<i64, Vec<(String, Vec<u8>)>> = BTreeMap::new();
    for (start, batch) in &converted.windows {
        let end = start + FIXTURE_WINDOW;
        let n = batch.num_rows() as u64;
        let raw = format!(
            "processed/request/{}window={start}-{end}-p1s0-n{n}.parquet",
            fastly_log_analytics::ingest::hour_dir(*start)
        );
        save(
            &raw,
            &convert::write_parquet(&[batch]).unwrap(),
            json!({"kind": "raw", "start": start, "end": end, "rows": n}),
        );
        let key = summary::summary_key(&summaries, *start, end, "p1s0", n);
        let bytes = summary::write_summary(&[batch], *start).unwrap();
        save(
            &key,
            &bytes,
            json!({"kind": "window", "start": start, "end": end, "rows": n, "sources": 1}),
        );
        by_hour
            .entry(start - start.rem_euclid(3600))
            .or_default()
            .push((key, bytes));
    }
    let mut hour_keys = Vec::new();
    let mut hour_bytes = Vec::new();
    for (hour, windows) in &by_hour {
        let keys: Vec<String> = windows.iter().map(|(k, _)| k.clone()).collect();
        let sources: Vec<&[u8]> = windows.iter().map(|(_, b)| b.as_slice()).collect();
        let rows: u64 = keys
            .iter()
            .map(|k| {
                k.rsplit_once("-n")
                    .unwrap()
                    .1
                    .trim_end_matches(".parquet")
                    .parse::<u64>()
                    .unwrap()
            })
            .sum();
        let key = bundle::hour_bundle_key(&bundles, *hour, &keys);
        let bytes = bundle::merge(&sources, *hour, Unit::Hour).unwrap();
        save(
            &key,
            &bytes,
            json!({"kind": "hour", "start": hour, "end": hour + 3600, "rows": rows, "sources": keys.len()}),
        );
        if keys.len() > 1 {
            let early = &keys[..keys.len() - 1];
            let key = bundle::hour_bundle_key(&bundles, *hour, early);
            let rows: u64 = early
                .iter()
                .map(|k| {
                    k.rsplit_once("-n")
                        .unwrap()
                        .1
                        .trim_end_matches(".parquet")
                        .parse::<u64>()
                        .unwrap()
                })
                .sum();
            let bytes = bundle::merge(&sources[..sources.len() - 1], *hour, Unit::Hour).unwrap();
            save(
                &key,
                &bytes,
                json!({"kind": "hour", "start": hour, "end": hour + 3600, "rows": rows, "sources": early.len(), "early": true}),
            );
        }
        hour_keys.push(key);
        hour_bytes.push(bytes);
    }
    let day = BASE - BASE.rem_euclid(86_400);
    let key = bundle::day_bundle_key(&bundles, day, &hour_keys);
    let sources: Vec<&[u8]> = hour_bytes.iter().map(Vec::as_slice).collect();
    let bytes = bundle::merge(&sources, day, Unit::Day).unwrap();
    let rows: u64 = converted
        .windows
        .values()
        .map(|b| b.num_rows() as u64)
        .sum();
    save(
        &key,
        &bytes,
        json!({"kind": "day", "start": day, "end": day + 86_400, "rows": rows, "sources": converted.windows.len()}),
    );
    std::fs::write(
        dir.join("index.json"),
        serde_json::to_string_pretty(&index).unwrap() + "\n",
    )
    .unwrap();
}
