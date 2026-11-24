use super::support::*;
use arrow_array::{Array, StringArray};
use bytes::Bytes;
use fastly_log_analytics::ingest::{convert, summary};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn fnv(s: &str) -> i64 {
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

    // assets, by High-Scale's rules: URL-extension type, upper cache, normalized encoding,
    // compressed and compressible flags, then the /assets sums.
    let mut want: BTreeMap<String, (i64, i64, f64)> = BTreeMap::new();
    for r in &rows {
        let url = s(r, "url").unwrap().to_lowercase();
        let ends = |exts: &[&str]| exts.iter().any(|e| url.ends_with(e));
        let kind = if ends(&[".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"]) {
            "Images"
        } else if ends(&[
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".csv", ".zip",
            ".tar", ".gz", ".tgz", ".rar", ".7z",
        ]) {
            "Documents"
        } else if ends(&[".js", ".mjs", ".css"]) {
            "JavaScript/CSS"
        } else if ends(&[".woff", ".woff2", ".ttf", ".otf", ".eot"]) {
            "Fonts"
        } else if ends(&[".m3u8", ".ts", ".mp4", ".m4s", ".webm"]) {
            "Video"
        } else {
            "API/Dynamic"
        };
        let raw = s(r, "resp_header_content_encoding").unwrap_or("");
        let lower = raw.to_lowercase();
        let encoding = if raw.is_empty() {
            "uncompressed".to_owned()
        } else if lower.contains("br") {
            "br".to_owned()
        } else if lower.contains("gzip") {
            "gzip".to_owned()
        } else {
            raw.to_owned()
        };
        let compressed = u8::from(["gzip", "br", "deflate"].contains(&lower.as_str()));
        let compressible = u8::from(ends(&[".js", ".mjs", ".css", ".svg", ".html", ".json"]));
        let key = serde_json::to_string(&(
            kind,
            s(r, "cache").unwrap().to_uppercase(),
            encoding,
            compressed,
            compressible,
        ))
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
pub(super) fn group_rows<'a>(
    out: &'a [Row],
    name: &str,
    field: &str,
) -> (BTreeMap<String, &'a Row>, &'a Row) {
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

    // series1 origin: the same per leg and minute, for /origin's sub-five-minute series.
    let minute = |r: &Value| {
        let at = time::OffsetDateTime::parse(
            s(r, "timestamp").unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        (at.unix_timestamp() * 1000).div_euclid(60_000) * 60_000
    };
    let cells = groups_by(&rows, |r| Some(format!("{}|{}", minute(r), leg_of(r))));
    assert!(cells.len() > legs.len());
    let (_, total) = group_rows(&out, "series1", "origin");
    assert_eq!(total["requests"], rows.len() as i64);
    assert_eq!(total["truncated"], false);
    let series: Vec<_> = bundle(&out, "series1");
    assert_eq!(series.len(), cells.len());
    for row in series {
        assert_eq!(row["field"], "origin");
        let key = format!("{}|{}", row["bucket"], row["value"].as_str().unwrap());
        let group = &cells[&key];
        assert_eq!(row["requests"], group.len() as i64, "{key}");
        assert_eq!(
            hist_cell(row, "ttfb_hist"),
            hist_of(group.iter().filter_map(|r| origin_us(r))),
            "{key}"
        );
        assert_eq!(
            hist_cell(row, "ottlb_hist"),
            hist_of(group.iter().filter_map(|r| i(r, "ottlb"))),
            "{key}"
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
        ("series1", "origin"),
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
            ("transport", _)
            | ("urls", "url")
            | ("series5" | "series1", _)
            | ("value", "cache") => rows.len() as i64,
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
                || g == "series1"
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
                    || g == "series1"
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
    assert_eq!(new_fields(&both).len(), 25);
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
    assert!(bundle(&mixed, "series1").is_empty());
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
        "series1",
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
    // Minute rows stay in hour bundles: a day is never read at minute buckets.
    assert!(!bundle(&hour, "series1").is_empty());
    assert!(bundle(&day, "series1").is_empty());
    assert!(!bundle(&day, "dim_totals")
        .iter()
        .any(|r| r["value"] == "series1"));
}
