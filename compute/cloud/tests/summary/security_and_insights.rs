use super::{
    support::*,
    v3_network_origin::{fnv, group_rows},
};
use bytes::Bytes;
use fastly_log_analytics::ingest::{
    convert,
    summary::{self, Hll},
};
use md5::{Digest, Md5};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Compression as Codec};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

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

#[test]
fn ins_lat_keys_city_by_region_and_country() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000), "city": "Springfield", "region": "IL", "country": "US", "pop": "ORD", "elapsed": 20_000}),
        json!({"timestamp": iso(BASE * 1000 + 1), "city": "Springfield", "region": "MO", "country": "US", "pop": "ORD", "elapsed": 40_000}),
        json!({"timestamp": iso(BASE * 1000 + 2), "city": "Springfield", "region": Value::Null, "country": Value::Null, "pop": "DFW", "elapsed": 30_000}),
        json!({"timestamp": iso(BASE * 1000 + 3), "city": "", "region": "IL", "country": "US", "pop": "", "elapsed": 50_000}),
        json!({"timestamp": iso(BASE * 1000 + 4), "city": Value::Null, "region": "IL", "country": "US", "pop": Value::Null, "elapsed": 60_000}),
    ];
    let b = batch(&rows);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());

    let city_total = summary_rows
        .iter()
        .find(|r| r["bundle"] == "dim_totals" && r["field"] == "city" && r["value"] == "ins_lat")
        .expect("ins_lat city dim_totals row");
    assert_eq!(city_total["requests"], 3);
    assert_eq!(city_total["truncated"], false);

    let city_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "ins_lat" && r["field"] == "city")
        .collect();
    assert_eq!(city_rows.len(), 3);
    let keys: std::collections::BTreeSet<&str> = city_rows
        .iter()
        .map(|r| r["value"].as_str().unwrap())
        .collect();
    assert!(keys.contains(r#"["Springfield","IL","US"]"#));
    assert!(keys.contains(r#"["Springfield","MO","US"]"#));
    assert!(keys.contains(r#"["Springfield",null,null]"#));
    for r in &city_rows {
        assert_eq!(r["requests"], 1);
        assert_eq!(r["elapsed_count"], 1);
    }

    let pop_total = summary_rows
        .iter()
        .find(|r| r["bundle"] == "dim_totals" && r["field"] == "pop" && r["value"] == "ins_lat")
        .expect("ins_lat pop dim_totals row");
    assert_eq!(pop_total["requests"], 3);
    assert_eq!(pop_total["truncated"], false);

    let pop_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "ins_lat" && r["field"] == "pop")
        .collect();
    assert_eq!(pop_rows.len(), 2);
    let ord = pop_rows.iter().find(|r| r["value"] == "ORD").unwrap();
    assert_eq!(ord["requests"], 2);
    assert_eq!(ord["elapsed_count"], 2);
    let dfw = pop_rows.iter().find(|r| r["value"] == "DFW").unwrap();
    assert_eq!(dfw["requests"], 1);
    assert_eq!(dfw["elapsed_count"], 1);
}

#[test]
fn ins_asn_counts_proxy_and_hosting() {
    let rows = vec![
        json!({"timestamp": iso(BASE * 1000), "asn": 15169, "p_type": "hosting"}),
        json!({"timestamp": iso(BASE * 1000 + 1), "asn": 15169, "p_type": "HOSTING"}),
        json!({"timestamp": iso(BASE * 1000 + 2), "asn": 15169, "p_type": ""}),
        json!({"timestamp": iso(BASE * 1000 + 3), "asn": 15169, "p_type": Value::Null}),
        json!({"timestamp": iso(BASE * 1000 + 4), "asn": 13335, "p_type": ""}),
        json!({"timestamp": iso(BASE * 1000 + 5), "asn": Value::Null, "p_type": "hosting"}),
    ];
    let b = batch(&rows);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());
    let total = summary_rows
        .iter()
        .find(|r| r["bundle"] == "dim_totals" && r["field"] == "asn" && r["value"] == "ins_asn")
        .expect("ins_asn dim_totals row");
    assert_eq!(total["requests"], 2);
    assert_eq!(total["truncated"], false);

    let asn_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "ins_asn" && r["field"] == "asn")
        .collect();
    assert_eq!(asn_rows.len(), 1);
    let row = asn_rows[0];
    assert_eq!(row["value"], "15169");
    assert_eq!(row["requests"], 2);
    assert_eq!(row["count"], 1);
}

#[test]
fn filtered_rollups_counts_low_card_filters() {
    let rows = vec![
        json!({
            "timestamp": iso(BASE * 1000),
            "status": 200,
            "cache": "HIT",
            "pop": "IAD",
            "country": "US",
            "proto": "HTTP/2",
            "method": "GET",
            "content_type": "Application/JSON; charset=utf-8",
            "edge": true,
            "resp_bytes": 1000,
            "ttfb": 0.010,
            "elapsed": 25_000,
            "tcp_rtt": 12_000,
            "ottfb": 5_000,
            "ottlb": 8_000,
            "ost": 200,
        }),
        json!({
            "timestamp": iso(BASE * 1000 + 1000),
            "status": 503,
            "cache": "MISS",
            "pop": "LHR",
            "country": "GB",
            "proto": "HTTP/1.1",
            "method": "POST",
            "content_type": "text/html",
            "edge": false,
            "resp_bytes": 500,
            "ttfb": 0.050,
            "elapsed": 80_000,
            "tcp_rtt": 30_000,
            "ottfb": 40_000,
            "ottlb": 60_000,
            "ost": 502,
        }),
    ];
    let b = custom_batch(&rows, &[("content_type", convert::Kind::Utf8)]);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());

    for field in summary::FILTERED_ROLLUP_FIELDS {
        let total = summary_rows
            .iter()
            .find(|r| {
                r["bundle"] == "dim_totals"
                    && r["field"] == field
                    && r["value"] == "filtered_rollups"
            })
            .unwrap_or_else(|| panic!("missing filtered_rollups dim_totals for {field}"));
        assert_eq!(total["requests"], 2);
        assert_eq!(total["truncated"], false);
    }

    let ct_json = summary_rows
        .iter()
        .find(|r| {
            r["bundle"] == "filtered_rollups"
                && r["field"] == "content_type"
                && r["value"] == "application/json"
        })
        .expect("normalized content_type row");
    assert_eq!(ct_json["requests"], 1);
    assert_eq!(ct_json["count"], 1);
    assert_eq!(ct_json["hit_requests"], 1);
    assert_eq!(ct_json["cache_hits"], 1);
    assert_eq!(ct_json["miss_requests"], 0);
    assert_eq!(ct_json["resp_bytes_sum"], 1000);
    assert_eq!(ct_json["ost_total_count"], 1);
    assert_eq!(ct_json["ost_5xx_count"], 0);
    assert!(hist_cell(ct_json, "ttfb_hist").is_some());
    assert!(hist_cell(ct_json, "elapsed_hist").is_some());
    assert!(hist_cell(ct_json, "rtt_hist").is_some());
    assert!(hist_cell(ct_json, "ottfb_hist").is_some());
    assert!(hist_cell(ct_json, "ottlb_hist").is_some());

    let edge_false = summary_rows
        .iter()
        .find(|r| {
            r["bundle"] == "filtered_rollups" && r["field"] == "edge" && r["value"] == "false"
        })
        .expect("edge=false row");
    assert_eq!(edge_false["requests"], 1);
    assert_eq!(edge_false["errors"], 1);
    assert_eq!(edge_false["status_5xx"], 1);
    assert_eq!(edge_false["miss_requests"], 1);
    assert_eq!(edge_false["ost_5xx_count"], 1);

    // Truncation test: 20 distinct methods > cap 16 -> truncated = true
    let many: Vec<Value> = (0..20)
        .map(|i| json!({"timestamp": iso(BASE * 1000 + i), "method": format!("M{i:02}")}))
        .collect();
    let mb = batch(&many);
    let mrows = read(&summary::write_summary(&[&mb], BASE).unwrap());
    let mtotal = mrows
        .iter()
        .find(|r| {
            r["bundle"] == "dim_totals"
                && r["field"] == "method"
                && r["value"] == "filtered_rollups"
        })
        .expect("method dim_totals");
    assert_eq!(mtotal["requests"], 20);
    assert_eq!(mtotal["truncated"], true);
    let kept = mrows
        .iter()
        .filter(|r| r["bundle"] == "filtered_rollups" && r["field"] == "method")
        .count();
    assert_eq!(kept, 16);
}

#[test]
fn shield_routes_join_edge_rid_to_shield_prid_within_a_window() {
    let rows = vec![
        // One edge row joined to two shield rows (many-to-many), one with negative delta.
        json!({"timestamp": iso(BASE * 1000), "edge": true, "rid": "r1", "pop": "EWR", "ottfb": 50_000}),
        json!({"timestamp": iso(BASE * 1000 + 1), "edge": false, "prid": "r1", "pop": "IAD", "ottfb": 20_000}),
        json!({"timestamp": iso(BASE * 1000 + 2), "edge": false, "prid": "r1", "pop": "ORD", "ottfb": 60_000}),
        // Shield row with no edge.
        json!({"timestamp": iso(BASE * 1000 + 3), "edge": false, "prid": "r_orphan", "pop": "IAD", "ottfb": 10_000}),
        // ttfb-only rows (seconds -> *1e6 fallback).
        json!({"timestamp": iso(BASE * 1000 + 4), "edge": true, "rid": "r2", "pop": "LHR", "ttfb": 0.080}),
        json!({"timestamp": iso(BASE * 1000 + 5), "edge": false, "prid": "r2", "pop": "AMS", "ttfb": 0.030}),
    ];
    let b = batch(&rows);
    let summary_rows = read(&summary::write_summary(&[&b], BASE).unwrap());
    let total = summary_rows
        .iter()
        .find(|r| {
            r["bundle"] == "dim_totals" && r["field"] == "route" && r["value"] == "shield_routes"
        })
        .expect("shield_routes dim_totals row");
    assert_eq!(total["requests"], 3);
    assert_eq!(total["truncated"], false);

    let route_rows: Vec<&Map<String, Value>> = summary_rows
        .iter()
        .filter(|r| r["bundle"] == "shield_routes" && r["field"] == "route")
        .collect();
    assert_eq!(route_rows.len(), 3);

    let ewr_iad = route_rows
        .iter()
        .find(|r| r["value"] == "[\"EWR\",\"IAD\"]")
        .unwrap();
    assert_eq!(ewr_iad["requests"], 1);
    assert_eq!(ewr_iad["errors"], 0);
    let h = hist_cell(ewr_iad, "overhead_hist").unwrap();
    assert_eq!((h.min, h.max, h.count()), (30_000, 30_000, 1));

    let ewr_ord = route_rows
        .iter()
        .find(|r| r["value"] == "[\"EWR\",\"ORD\"]")
        .unwrap();
    assert_eq!(ewr_ord["requests"], 1);
    assert_eq!(ewr_ord["errors"], 1);
    let h = hist_cell(ewr_ord, "overhead_hist").unwrap();
    assert_eq!((h.min, h.max, h.count()), (-10_000, -10_000, 1));

    let lhr_ams = route_rows
        .iter()
        .find(|r| r["value"] == "[\"LHR\",\"AMS\"]")
        .unwrap();
    assert_eq!(lhr_ams["requests"], 1);
    assert_eq!(lhr_ams["errors"], 0);
    let h = hist_cell(lhr_ams, "overhead_hist").unwrap();
    assert_eq!((h.min, h.max, h.count()), (50_000, 50_000, 1));
}
