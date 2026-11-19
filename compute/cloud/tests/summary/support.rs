#![allow(dead_code)]
use super::common;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use bytes::Bytes;
use fastly_log_analytics::ingest::{convert, hist::Hist};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Map, Value};
use std::io::Write;

pub(super) const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
pub(super) const WINDOW: i64 = 300;

pub(super) fn iso(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

/// Deterministic rows over five minutes, with every value class the summary treats specially.
pub(super) fn fixture() -> Vec<Value> {
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

pub(super) fn batch(rows: &[Value]) -> RecordBatch {
    let gz = common::zstd_ndjson(rows);
    let converted = convert::convert_object(&gz, WINDOW, BASE).unwrap();
    assert_eq!(converted.windows.len(), 1);
    converted.windows.into_values().next().unwrap()
}

pub(super) fn read(bytes: &[u8]) -> Vec<Map<String, Value>> {
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

pub(super) fn bundle<'a>(
    rows: &'a [Map<String, Value>],
    name: &str,
) -> Vec<&'a Map<String, Value>> {
    rows.iter().filter(|r| r["bundle"] == name).collect()
}

pub(super) fn s<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(Value::as_str)
}

pub(super) fn i(row: &Value, key: &str) -> Option<i64> {
    row.get(key).and_then(Value::as_i64)
}

pub(super) fn mi(row: &Map<String, Value>, key: &str) -> Option<i64> {
    row.get(key).and_then(Value::as_i64)
}

pub(super) fn mf(row: &Map<String, Value>, key: &str) -> Option<f64> {
    row.get(key).and_then(Value::as_f64)
}

pub(super) fn f(row: &Value, key: &str) -> Option<f64> {
    row.get(key).and_then(Value::as_f64)
}

pub(super) fn close(a: &Value, b: f64) {
    let a = a.as_f64().unwrap();
    assert!((a - b).abs() <= 1e-6 * b.abs().max(1.0), "{a} != {b}");
}

pub(super) fn fla_hit(cache: Option<&str>) -> bool {
    matches!(cache, Some("HIT" | "HIT-STALE" | "HIT-CLUSTER"))
}

pub(super) fn minute_of(row: &Value) -> i64 {
    let at = time::OffsetDateTime::parse(
        s(row, "timestamp").unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (at.unix_timestamp_nanos() / 1_000_000) as i64 / 60_000 * 60_000
}

pub(super) fn ts_ms(row: &Value) -> i64 {
    let at = time::OffsetDateTime::parse(
        s(row, "timestamp").unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (at.unix_timestamp_nanos() / 1_000_000) as i64
}

pub(super) fn custom_batch(rows: &[Value], custom: &[(&str, convert::Kind)]) -> RecordBatch {
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

pub(super) fn with_sketches_for(bytes: &[u8], field: &str) -> Vec<u8> {
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

pub(super) fn fixture_v3(offset_ms: i64, n: i64) -> Vec<Value> {
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

pub(super) fn meta(bytes: &[u8], key: &str) -> Option<String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.clone())
}

pub(super) fn hist_cell(row: &Map<String, Value>, col: &str) -> Option<Hist> {
    row.get(col)
        .map(|v| Hist::from_bytes(&hex::decode(v.as_str().unwrap()).unwrap()).unwrap())
}

pub(super) fn hist_of(values: impl IntoIterator<Item = i64>) -> Option<Hist> {
    let mut h = Hist::default();
    values.into_iter().for_each(|v| h.add(v));
    (!h.is_empty()).then_some(h)
}

pub(super) fn quality_rows() -> Vec<Value> {
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

pub(super) fn fixture_sv(offset_ms: i64, n: i64) -> Vec<Value> {
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
