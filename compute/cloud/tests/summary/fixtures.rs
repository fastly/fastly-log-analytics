use super::support::*;
use fastly_log_analytics::ingest::{convert, summary};
use serde_json::{json, Value};
use std::{collections::BTreeMap, io::Write};

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
            if i < 40 {
                let pair = i / 2;
                let is_edge = i % 2 == 0;
                row["edge"] = is_edge.into();
                if is_edge {
                    row["rid"] = format!("shield-{pair}").into();
                    row["ttfb"] = if pair == 0 { 0.01 } else { 0.05 }.into();
                } else {
                    row["prid"] = format!("shield-{pair}").into();
                    row["ttfb"] = if pair == 0 { 0.03 } else { 0.02 }.into();
                }
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
            if i < 40 {
                let pair = i / 2;
                let is_edge = i % 2 == 0;
                row["edge"] = is_edge.into();
                if is_edge {
                    row["rid"] = format!("shield-{pair}").into();
                    row["ottfb"] = if pair == 0 { 10_000 } else { 50_000 }.into();
                } else {
                    row["prid"] = format!("shield-{pair}").into();
                    row["ottfb"] = if pair == 0 { 30_000 } else { 20_000 }.into();
                }
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
