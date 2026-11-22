use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use fastly_log_analytics::ingest::{cmcd_summary, convert, hist::Hist, hll::Hll};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Map, Value};
use std::sync::Arc;

mod common;

const BASE: i64 = 1_800_000_000;
const WINDOW: i64 = 300;

fn iso(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

fn read_rows(bytes: &[u8]) -> Vec<Map<String, Value>> {
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

fn raw_batch(rows: &[Value]) -> RecordBatch {
    let gz = common::zstd_ndjson(rows);
    let converted = convert::convert_object(&gz, WINDOW, BASE).unwrap();
    converted.windows.into_values().next().unwrap()
}

fn hll_count(hex_str: &str) -> u64 {
    let bytes = hex::decode(hex_str).unwrap();
    Hll::from_bytes(&bytes).unwrap().count()
}

fn hist_from_hex(hex_str: &str) -> Hist {
    let bytes = hex::decode(hex_str).unwrap();
    Hist::from_bytes(&bytes).unwrap()
}

#[test]
fn no_cmcd_columns_writes_no_file() {
    let batch = raw_batch(&[
        json!({"timestamp": iso(BASE * 1000), "status": 200, "url": "/a"}),
        json!({"timestamp": iso(BASE * 1000 + 1000), "status": 404, "url": "/b"}),
    ]);
    assert!(!cmcd_summary::batches_have_cmcd(&[&batch]));
}

#[test]
fn cmcd_summary_matches_cmcd_ts_aggregates() {
    // Window 1: mixed ot ('v', 'a', ''), bs true/false/null, tb=0 excluded from util, su rows,
    // plus session 'span' starting here and continuing in Window 2.
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("geo_country", DataType::Utf8, true),
        Field::new("client_asn", DataType::Int64, true),
        Field::new("as_name", DataType::Utf8, true),
        Field::new("ttfb_ms", DataType::Float64, true),
        Field::new("cmcd_sid", DataType::Utf8, true),
        Field::new("cmcd_cid", DataType::Utf8, true),
        Field::new("cmcd_br", DataType::Int64, true),
        Field::new("cmcd_tb", DataType::Int64, true),
        Field::new("cmcd_bl", DataType::Int64, true),
        Field::new("cmcd_bs", DataType::Boolean, true),
        Field::new("cmcd_mtp", DataType::Int64, true),
        Field::new("cmcd_ot", DataType::Utf8, true),
        Field::new("cmcd_sf", DataType::Utf8, true),
        Field::new("cmcd_su", DataType::Boolean, true),
    ]));

    let t0 = BASE * 1000;
    let w1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    t0 + 1_000,
                    t0 + 10_000,
                    t0 + 20_000,
                    t0 + 30_000,
                    t0 + 40_000,
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec![
                Some("US"),
                Some("US"),
                Some("CA"),
                Some("US"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(15169),
                Some(15169),
                Some(13335),
                Some(15169),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("Google"),
                Some("Google"),
                Some("Cloudflare"),
                Some("Google"),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(20.0),
                None,
                Some(50.0),
                None,
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("s1"),
                Some("s1"),
                Some("s2"),
                Some("span"),
                Some("s3"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("movie"),
                Some("movie"),
                Some("live"),
                Some("movie"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(2000),
                Some(4000),
                Some(128),
                Some(3000),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(4000),
                Some(0),
                Some(4000),
                Some(6000),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1000),
                Some(3000),
                Some(9000),
                Some(2000),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(5000),
                Some(6000),
                Some(500),
                Some(8000),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("v"),
                Some("v"),
                Some("a"),
                Some("v"),
                Some(""),
            ])),
            Arc::new(StringArray::from(vec![
                Some("h"),
                Some("h"),
                Some("d"),
                Some("h"),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                None,
            ])),
        ],
    )
    .unwrap();

    // Window 2: 'span' session continues 310s after its first request in Window 1.
    let t1 = (BASE + WINDOW) * 1000;
    let w2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![t1 + 40_000]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec![Some("US")])),
            Arc::new(Int64Array::from(vec![Some(15169)])),
            Arc::new(StringArray::from(vec![Some("Google")])),
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("span")])),
            Arc::new(StringArray::from(vec![Some("movie")])),
            Arc::new(Int64Array::from(vec![Some(3000)])),
            Arc::new(Int64Array::from(vec![Some(6000)])),
            Arc::new(Int64Array::from(vec![Some(4000)])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
            Arc::new(Int64Array::from(vec![Some(7000)])),
            Arc::new(StringArray::from(vec![Some("v")])),
            Arc::new(StringArray::from(vec![Some("h")])),
            Arc::new(BooleanArray::from(vec![Some(false)])),
        ],
    )
    .unwrap();

    assert!(cmcd_summary::batches_have_cmcd(&[&w1]));
    let bytes1 = cmcd_summary::write_cmcd_summary(&[&w1], BASE).unwrap();
    let bytes2 = cmcd_summary::write_cmcd_summary(&[&w2], BASE + WINDOW).unwrap();
    let rows1 = read_rows(&bytes1);
    let rows2 = read_rows(&bytes2);

    let bucket1 = rows1.iter().find(|r| r["kind"] == "bucket").unwrap();
    assert_eq!(bucket1["reqs"], 5);
    assert_eq!(bucket1["rebuffer_events"], 1);
    assert_eq!(bucket1["has_bs"], 3);
    assert_eq!(bucket1["has_su"], 3);
    assert_eq!(bucket1["has_ot"], 4);
    assert_eq!(bucket1["has_bl"], 4);
    assert_eq!(bucket1["has_br"], 4);
    assert_eq!(bucket1["has_mtp"], 4);
    assert_eq!(bucket1["has_cid"], 4);
    // Video rows: s1(2000, 4000, 1000, bs=false), s1(4000, 0, 3000, bs=true), span(3000, 6000, 2000, bs=false).
    assert_eq!(bucket1["v_br_n"], 3);
    assert_eq!(bucket1["v_br_sum"].as_f64().unwrap(), 9000.0);
    assert_eq!(bucket1["v_bl_n"], 3);
    assert_eq!(bucket1["v_bl_sum"].as_f64().unwrap(), 6000.0);
    // tb=0 on the second row is excluded from util_n: (2000/4000 + 3000/6000) = 1.0 over 2 rows.
    assert_eq!(bucket1["util_n"], 2);
    assert!((bucket1["util_sum"].as_f64().unwrap() - 1.0).abs() < 1e-9);
    // Startup rows: row 0 (bl=1000, ttfb=20.0) and row 2 (bl=9000, ttfb=50.0).
    assert_eq!(bucket1["startup_requests"], 2);
    assert_eq!(bucket1["su_bl_n"], 2);
    assert_eq!(bucket1["su_bl_sum"].as_f64().unwrap(), 10000.0);
    assert_eq!(bucket1["su_ttfb_n"], 2);
    assert_eq!(bucket1["su_ttfb_sum"].as_f64().unwrap(), 70.0);
    assert_eq!(hll_count(bucket1["sid_hll"].as_str().unwrap()), 4);
    assert_eq!(hll_count(bucket1["rebuffer_sid_hll"].as_str().unwrap()), 1);
    let v_mtp = hist_from_hex(bucket1["v_mtp_hist"].as_str().unwrap());
    assert_eq!(v_mtp.count(), 3);

    // Session spanning two windows: merge first_ts (min) and last_ts (max).
    let s_w1 = rows1
        .iter()
        .find(|r| r["kind"] == "session" && r["key"] == "span")
        .unwrap();
    let s_w2 = rows2
        .iter()
        .find(|r| r["kind"] == "session" && r["key"] == "span")
        .unwrap();
    let first_ts = s_w1["first_ts"]
        .as_i64()
        .unwrap()
        .min(s_w2["first_ts"].as_i64().unwrap());
    let last_ts = s_w1["last_ts"]
        .as_i64()
        .unwrap()
        .max(s_w2["last_ts"].as_i64().unwrap());
    assert_eq!(last_ts - first_ts, 310_000);

    // Object types: 'v' (3), 'a' (1); empty '' is excluded.
    let ot_v = rows1
        .iter()
        .find(|r| r["kind"] == "ot" && r["key"] == "v")
        .unwrap();
    assert_eq!(ot_v["reqs"], 3);
    assert!(!rows1.iter().any(|r| r["kind"] == "ot" && r["key"] == ""));
}

/// A CMCD window summary over `(offset_ms, sid, br, country, asn, as_name)` rows from `start_s`.
fn cmcd_window(start_s: i64, rows: &[(i64, &str, i64, &str, i64, &str)]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("geo_country", DataType::Utf8, true),
        Field::new("client_asn", DataType::Int64, true),
        Field::new("as_name", DataType::Utf8, true),
        Field::new("cmcd_sid", DataType::Utf8, true),
        Field::new("cmcd_br", DataType::Int64, true),
        Field::new("cmcd_ot", DataType::Utf8, true),
    ]));
    let ts: Vec<i64> = rows.iter().map(|r| start_s * 1000 + r.0).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(ts).with_timezone("UTC")),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.3)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.4)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.5)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.2)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|_| Some("v")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    cmcd_summary::write_cmcd_summary(&[&batch], start_s).unwrap()
}

fn find<'a>(rows: &'a [Map<String, Value>], kind: &str, key: &str) -> &'a Map<String, Value> {
    rows.iter()
        .find(|r| r["kind"] == kind && r.get("key").and_then(Value::as_str) == Some(key))
        .unwrap_or_else(|| panic!("no {kind} {key}"))
}

#[test]
fn bundle_merge_is_the_lossless_union_of_its_windows() {
    let w1 = cmcd_window(
        BASE,
        &[
            (1_000, "s1", 2000, "US", 15169, "Google"),
            (2_000, "s2", 4000, "CA", 13335, "Cloudflare"),
        ],
    );
    let w2 = cmcd_window(
        BASE,
        &[
            (3_000, "s1", 3000, "US", 15169, "Alphabet"),
            (4_000, "s3", 1000, "US", 15169, "Google"),
        ],
    );
    let w3 = cmcd_window(BASE + WINDOW, &[(1_000, "s1", 5000, "US", 15169, "Google")]);
    let wide: Vec<(i64, String)> = (0..cmcd_summary::SESSION_K as i64 + 1)
        .map(|i| (i, format!("w{i:05}")))
        .collect();
    let w4 = cmcd_window(
        BASE + 2 * WINDOW,
        &wide
            .iter()
            .map(|(i, sid)| (*i, sid.as_str(), 100, "DE", 3320, "DTAG"))
            .collect::<Vec<_>>(),
    );
    let merged = cmcd_summary::merge_windows(&[&w1, &w2, &w3, &w4]).unwrap();
    let rows = read_rows(&merged);

    let first = rows
        .iter()
        .find(|r| r["kind"] == "bucket" && r["bucket"] == BASE * 1000)
        .unwrap();
    assert_eq!(first["reqs"], 4);
    assert_eq!(first["br_n"], 4);
    assert_eq!(first["br_sum"].as_f64().unwrap(), 10_000.0);
    assert_eq!(hll_count(first["sid_hll"].as_str().unwrap()), 3);
    assert_eq!(first["min_ts"], BASE * 1000 + 1_000);
    assert_eq!(first["max_ts"], BASE * 1000 + 4_000);
    let second = rows
        .iter()
        .find(|r| r["kind"] == "bucket" && r["bucket"] == (BASE + WINDOW) * 1000)
        .unwrap();
    assert_eq!(second["reqs"], 1);

    let s1 = find(&rows, "session", "s1");
    assert_eq!(s1["reqs"], 3);
    assert_eq!(s1["first_ts"], BASE * 1000 + 1_000);
    assert_eq!(s1["last_ts"], (BASE + WINDOW) * 1000 + 1_000);
    assert_eq!(find(&rows, "country", "US")["reqs"], 4);
    assert_eq!(find(&rows, "country", "CA")["reqs"], 1);
    let google = find(&rows, "asn", "15169");
    assert_eq!(google["reqs"], 4);
    assert_eq!(google["label"], "Alphabet");
    assert_eq!(
        find(&rows, "ot", "v")["reqs"],
        5 + cmcd_summary::SESSION_K as i64 + 1
    );
    // The wide window kept SESSION_K of its sessions; the bundle keeps them and its flag.
    assert_eq!(
        rows.iter().filter(|r| r["kind"] == "session").count(),
        3 + cmcd_summary::SESSION_K
    );
    assert_eq!(find(&rows, "totals", "session")["truncated"], true);
    assert_eq!(find(&rows, "totals", "country")["truncated"], false);

    // Every per-row request count is the sum over the windows, as a reader merging them sees.
    let reqs = |bytes: &[u8], kind: &str| -> i64 {
        read_rows(bytes)
            .iter()
            .filter(|r| r["kind"] == kind)
            .map(|r| r["reqs"].as_i64().unwrap())
            .sum()
    };
    for kind in ["bucket", "session", "content", "country", "asn", "ot", "sf"] {
        let windows: i64 = [&w1, &w2, &w3, &w4].iter().map(|w| reqs(w, kind)).sum();
        assert_eq!(reqs(&merged, kind), windows, "{kind}");
    }
    // The same windows in any order give the same bytes.
    assert_eq!(
        cmcd_summary::merge_windows(&[&w4, &w3, &w1, &w2]).unwrap(),
        merged
    );
    assert_eq!(
        cmcd_summary::merge_windows(&[&merged]).unwrap(),
        merged,
        "a bundle folds back to itself"
    );
}
