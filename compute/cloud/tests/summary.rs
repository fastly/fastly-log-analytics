use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use bytes::Bytes;
use fastly_log_analytics::ingest::{
    convert,
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

fn f(row: &Value, key: &str) -> Option<f64> {
    row.get(key).and_then(Value::as_f64)
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
    assert_eq!(totals.len(), summary::FIELDS.len());
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

    // all_fields_ip: exact distinct IPs per value and the sketch of exactly those IPs.
    let ip_rows = bundle(&out, "all_fields_ip");
    for row in ip_rows.iter().filter(|r| r["field"] == "country") {
        let value = row["value"].as_str().unwrap();
        let ips: BTreeSet<&str> = rows
            .iter()
            .filter(|r| s(r, "country") == Some(value))
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
        ip_rows.iter().filter(|r| r["field"] == "country").count(),
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
    assert!(bundle(&out, "all_fields_ip")
        .iter()
        .any(|r| r["field"] == "tenant" && r["value"] == "acme"));
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

#[test]
fn hll_matches_fla_hll_byte_for_byte() {
    // Vectors from fla v3 backend/utils/hll.py at 2c75f058 (p=8, md5 hash).
    let mut one = Hll::default();
    one.add_hash(summary::hll_hash("192.0.2.1"));
    let bytes = one.to_bytes();
    assert_eq!(bytes.len(), 258);
    assert_eq!(&bytes[..2], &[1, 8]);
    let set: Vec<(usize, u8)> = bytes[2..]
        .iter()
        .enumerate()
        .filter(|(_, r)| **r != 0)
        .map(|(i, r)| (i, *r))
        .collect();
    assert_eq!(set, [(98, 2)]);
    assert_eq!(one.count(), 1);

    let mut many = Hll::default();
    for i in 0..1000 {
        many.add_hash(summary::hll_hash(&format!("10.0.{}.{}", i / 256, i % 256)));
    }
    let bytes = many.to_bytes();
    assert_eq!(&bytes[..12], &[1, 8, 7, 3, 2, 7, 6, 2, 1, 2, 6, 6]);
    assert_eq!(
        hex::encode(Md5::digest(&bytes)),
        "f004a78139a91300de8fe67bc3f91ef6"
    );
    assert_eq!(many.count(), 1060);

    let mut merged = Hll::from_bytes(&one.to_bytes()).unwrap();
    merged.merge(&many);
    let mut both = many.clone();
    both.add_hash(summary::hll_hash("192.0.2.1"));
    assert_eq!(merged.to_bytes(), both.to_bytes());
    assert!(Hll::from_bytes(&bytes[..257]).is_none());
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

/// Writes `web/lib/dashboard/__fixtures__/summaries/`: raw window Parquet, their window
/// summaries, hour and day bundles and `index.json`, for the dashboard's summary-vs-raw tests.
/// Run with `cargo test --test summary -- --ignored write_dashboard_fixtures`.
#[test]
#[ignore = "regenerates the web dashboard fixtures"]
fn write_dashboard_fixtures() {
    use fastly_log_analytics::ingest::bundle::{self, Unit};
    const FIXTURE_WINDOW: i64 = 900;
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("web/lib/dashboard/__fixtures__/summaries");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
    let mut text = Vec::new();
    for row in &rows {
        writeln!(text, "{row}").unwrap();
    }
    let gz = zstd::stream::encode_all(text.as_slice(), 1).unwrap();
    let converted = convert::convert_object(&gz, FIXTURE_WINDOW, BASE + 3600).unwrap();
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
