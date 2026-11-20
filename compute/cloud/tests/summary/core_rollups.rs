use super::support::*;
use fastly_log_analytics::ingest::{
    convert,
    summary::{self, Hll},
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

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

    // all_fields: CAST AS VARCHAR without NULL or empty strings; ttl/age round to whole seconds
    // like the v2 dashboard, and waf_sig_ind splits and trims without empty tokens.
    let mut expected: BTreeMap<&str, BTreeMap<String, i64>> = BTreeMap::new();
    for row in &rows {
        let mut add = |field: &'static str, value: String| {
            *expected.entry(field).or_default().entry(value).or_default() += 1;
        };
        for field in ["cache", "status", "edge", "pop", "asn", "ip"] {
            match row.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::String(v)) if !v.is_empty() => add(field, v.clone()),
                Some(Value::String(_)) => {}
                Some(v) => add(field, v.to_string()),
            }
        }
        add("age", f(row, "age").unwrap().round().to_string());
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
    assert!(!expected["pop"].contains_key(""));
    assert!(expected["age"].contains_key("1"));
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
