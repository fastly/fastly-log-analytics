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
    let first = fixture_v3(0, 400);
    let second = fixture_v3(WINDOW * 1000, 500);
    let (b1, b2) = (batch(&first), batch(&second));
    let whole = read(&summary::write_summary(&[&b1, &b2], BASE).unwrap());
    let w1 = summary::write_summary(&[&b1], BASE).unwrap();
    let w2 = summary::write_summary(&[&b2], BASE + WINDOW).unwrap();
    let hour_bytes = bundles::merge(&[&w1, &w2], BASE, Unit::Hour).unwrap();
    assert_eq!(meta(&hour_bytes, "flp_summary").as_deref(), Some("3"));
    let hour = read(&hour_bytes);
    for name in ["dims", "series5", "bins", "assets", "dim_totals"] {
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
    write_summary_fixtures("dashboard/__fixtures__/summaries", &rows);
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
            });
            // Nulls exercise each coalesce: unknown city and country, ASN 0, protocol
            // default and absent RTT, loss and timestamp.
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
            if i % 401 == 7 {
                row["timestamp"] = Value::Null;
            }
            row
        })
        .collect();
    write_summary_fixtures("__fixtures__/network-summaries", &rows);
}

/// Writes `frontend/lib/cloud-engine/{rel}/`: 15-minute raw windows of `rows`, their window
/// summaries, hour bundles (and each hour's bundle without its last window), the day bundle
/// and `index.json`.
fn write_summary_fixtures(rel: &str, rows: &[Value]) {
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
