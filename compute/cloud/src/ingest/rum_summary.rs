//! Per-window RUM summary (`processed/rum/summary/`), summarizing `client_vitals` and
//! `client_errors` batches for `rum.ts` `queryRumAnalytics`.

use super::{
    convert,
    hist::Hist,
    hll::{self, Hll},
};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
    },
    Array, ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::file::metadata::KeyValue;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub const VERSION: &str = "1";
pub const BUCKET_MS: i64 = 300_000;
pub const PAGE_K: usize = 500;
pub const ENV_K: usize = 200;
pub const ERROR_K: usize = 500;

pub fn prefix(scope: &str) -> String {
    format!("{scope}processed/rum/summary/")
}

pub fn key(prefix: &str, start: i64, end: i64, run_id: &str, rows: u64) -> String {
    super::summary::summary_key(prefix, start, end, run_id, rows)
}

pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("kind", DataType::Utf8, false),
                Field::new(
                    "bucket",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                ),
                Field::new("key", DataType::Utf8, true),
                Field::new("count", DataType::Int64, true),
                Field::new("good", DataType::Int64, true),
                Field::new("poor", DataType::Int64, true),
                Field::new("value_hist", DataType::Binary, true),
                Field::new("lcp_hist", DataType::Binary, true),
                Field::new("cls_hist", DataType::Binary, true),
                Field::new("inp_hist", DataType::Binary, true),
                Field::new("cid_hll", DataType::Binary, true),
                Field::new("msg_hll", DataType::Binary, true),
                Field::new("min_line", DataType::Int64, true),
                Field::new("min_col", DataType::Int64, true),
                Field::new(
                    "last_ts",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                ),
                Field::new("truncated", DataType::Boolean, true),
            ]))
        })
        .clone()
}

fn thresholds(metric: &str) -> (f64, f64) {
    match metric {
        "LCP" => (2500.0, 4000.0),
        "CLS" => (0.1, 0.25),
        "INP" => (200.0, 500.0),
        "FCP" => (1800.0, 3000.0),
        "TTFB" => (800.0, 1800.0),
        _ => (2500.0, 4000.0),
    }
}

fn scale_metric(metric: &str, value: f64) -> i64 {
    let scaled = if metric == "CLS" {
        value * 1_000_000.0
    } else {
        value * 1_000.0
    };
    scaled.round() as i64
}

fn col_str<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

fn col_f64<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Float64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
}

fn col_i32<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
}

fn col_ts<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a TimestampMillisecondArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>())
}

fn get_str(col: Option<&StringArray>, row: usize) -> Option<&str> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

fn get_f64(col: Option<&Float64Array>, row: usize) -> Option<f64> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
        .filter(|v| v.is_finite())
}

fn get_i32(col: Option<&Int32Array>, row: usize) -> Option<i32> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

fn get_ts(col: Option<&TimestampMillisecondArray>, row: usize) -> Option<i64> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

#[derive(Default)]
struct MetricAgg {
    count: i64,
    good: i64,
    poor: i64,
    hist: Hist,
}

#[derive(Default)]
struct VitalsTotalAgg {
    count: i64,
    cid_hll: Hll,
}

#[derive(Default)]
struct PageAgg {
    count: i64,
    lcp: Hist,
    cls: Hist,
    inp: Hist,
}

#[derive(Default)]
struct ErrorsTotalAgg {
    count: i64,
    cid_hll: Hll,
    msg_hll: Hll,
}

#[derive(Default)]
struct ErrorAgg {
    count: i64,
    min_line: Option<i64>,
    min_col: Option<i64>,
    cid_hll: Hll,
    last_ts: Option<i64>,
}

struct Out {
    kind: StringBuilder,
    bucket: TimestampMillisecondBuilder,
    key: StringBuilder,
    count: Int64Builder,
    good: Int64Builder,
    poor: Int64Builder,
    value_hist: BinaryBuilder,
    lcp_hist: BinaryBuilder,
    cls_hist: BinaryBuilder,
    inp_hist: BinaryBuilder,
    cid_hll: BinaryBuilder,
    msg_hll: BinaryBuilder,
    min_line: Int64Builder,
    min_col: Int64Builder,
    last_ts: TimestampMillisecondBuilder,
    truncated: BooleanBuilder,
}

impl Out {
    fn new() -> Self {
        Self {
            kind: StringBuilder::new(),
            bucket: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            key: StringBuilder::new(),
            count: Int64Builder::new(),
            good: Int64Builder::new(),
            poor: Int64Builder::new(),
            value_hist: BinaryBuilder::new(),
            lcp_hist: BinaryBuilder::new(),
            cls_hist: BinaryBuilder::new(),
            inp_hist: BinaryBuilder::new(),
            cid_hll: BinaryBuilder::new(),
            msg_hll: BinaryBuilder::new(),
            min_line: Int64Builder::new(),
            min_col: Int64Builder::new(),
            last_ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            truncated: BooleanBuilder::new(),
        }
    }

    fn finish(mut self) -> Result<RecordBatch, String> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.kind.finish()),
            Arc::new(self.bucket.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.count.finish()),
            Arc::new(self.good.finish()),
            Arc::new(self.poor.finish()),
            Arc::new(self.value_hist.finish()),
            Arc::new(self.lcp_hist.finish()),
            Arc::new(self.cls_hist.finish()),
            Arc::new(self.inp_hist.finish()),
            Arc::new(self.cid_hll.finish()),
            Arc::new(self.msg_hll.finish()),
            Arc::new(self.min_line.finish()),
            Arc::new(self.min_col.finish()),
            Arc::new(self.last_ts.finish()),
            Arc::new(self.truncated.finish()),
        ];
        RecordBatch::try_new(schema(), arrays).map_err(|_| "rum_summary_batch".into())
    }
}

fn push_opt_bin(builder: &mut BinaryBuilder, bytes: Option<&[u8]>) {
    match bytes {
        Some(b) => builder.append_value(b),
        None => builder.append_null(),
    }
}

fn top_k<K: Ord + Clone, V>(
    map: BTreeMap<K, V>,
    k: usize,
    count_of: impl Fn(&V) -> i64,
) -> (Vec<(K, V)>, bool) {
    let truncated = map.len() > k;
    let mut items: Vec<(K, V)> = map.into_iter().collect();
    items.sort_by(|(ka, va), (kb, vb)| {
        (Reverse(count_of(va)), ka).cmp(&(Reverse(count_of(vb)), kb))
    });
    items.truncate(k);
    (items, truncated)
}

/// Builds the `rum_summary` Parquet bytes for one window `[start, end)` (seconds).
pub fn write_summary(
    vitals: &[&RecordBatch],
    errors: &[&RecordBatch],
    start: i64,
) -> Result<Vec<u8>, String> {
    let default_bucket = start * 1000;
    let mut metrics: BTreeMap<(i64, String), MetricAgg> = BTreeMap::new();
    let mut vitals_totals: BTreeMap<i64, VitalsTotalAgg> = BTreeMap::new();
    let mut pages: BTreeMap<String, PageAgg> = BTreeMap::new();
    let mut envs: [BTreeMap<String, i64>; 3] = Default::default();

    for batch in vitals {
        let ts = col_ts(batch, "timestamp");
        let name = col_str(batch, "metric_name");
        let value = col_f64(batch, "metric_value");
        let pathname = col_str(batch, "pathname");
        let browser = col_str(batch, "browser");
        let os = col_str(batch, "os");
        let device = col_str(batch, "device");
        let cid = col_str(batch, "cid");

        for row in 0..batch.num_rows() {
            let row_ts = get_ts(ts, row).unwrap_or(default_bucket);
            let bucket5 = row_ts - row_ts.rem_euclid(BUCKET_MS);
            let vt = vitals_totals.entry(bucket5).or_default();
            vt.count += 1;
            if let Some(c) = get_str(cid, row) {
                vt.cid_hll.add_hash(hll::hash(c));
            }

            let upper = get_str(name, row).unwrap_or("").to_ascii_uppercase();
            let val = get_f64(value, row);
            let m = metrics.entry((bucket5, upper.clone())).or_default();
            m.count += 1;
            if let Some(v) = val {
                let (good_bound, poor_bound) = thresholds(&upper);
                if v <= good_bound {
                    m.good += 1;
                }
                if v > poor_bound {
                    m.poor += 1;
                }
                m.hist.add(scale_metric(&upper, v));
            }

            let path = get_str(pathname, row).unwrap_or("/");
            let p = pages.entry(path.to_owned()).or_default();
            p.count += 1;
            if let Some(v) = val {
                match upper.as_str() {
                    "LCP" => p.lcp.add(scale_metric("LCP", v)),
                    "CLS" => p.cls.add(scale_metric("CLS", v)),
                    "INP" => p.inp.add(scale_metric("INP", v)),
                    _ => {}
                }
            }

            for (idx, col) in [browser, os, device].into_iter().enumerate() {
                let v = get_str(col, row).unwrap_or("Unknown");
                *envs[idx].entry(v.to_owned()).or_default() += 1;
            }
        }
    }

    let mut errors_totals: BTreeMap<i64, ErrorsTotalAgg> = BTreeMap::new();
    let mut error_rows: BTreeMap<(String, String), ErrorAgg> = BTreeMap::new();

    for batch in errors {
        let ts = col_ts(batch, "timestamp");
        let msg = col_str(batch, "error_message");
        let file = col_str(batch, "error_file");
        let line = col_i32(batch, "error_line");
        let col = col_i32(batch, "error_col");
        let cid = col_str(batch, "cid");

        for row in 0..batch.num_rows() {
            let row_ts = get_ts(ts, row);
            let bucket5 = row_ts
                .map(|t| t - t.rem_euclid(BUCKET_MS))
                .unwrap_or(default_bucket - default_bucket.rem_euclid(BUCKET_MS));
            let et = errors_totals.entry(bucket5).or_default();
            et.count += 1;
            if let Some(c) = get_str(cid, row) {
                et.cid_hll.add_hash(hll::hash(c));
            }
            if let Some(m) = get_str(msg, row) {
                et.msg_hll.add_hash(hll::hash(m));
            }

            let err_msg = get_str(msg, row).unwrap_or("Unknown Error");
            let err_file = get_str(file, row).unwrap_or("");
            let entry = error_rows
                .entry((err_msg.to_owned(), err_file.to_owned()))
                .or_default();
            entry.count += 1;
            if let Some(l) = get_i32(line, row) {
                let l = i64::from(l);
                entry.min_line = Some(entry.min_line.map_or(l, |cur| cur.min(l)));
            }
            if let Some(c) = get_i32(col, row) {
                let c = i64::from(c);
                entry.min_col = Some(entry.min_col.map_or(c, |cur| cur.min(c)));
            }
            if let Some(c) = get_str(cid, row) {
                entry.cid_hll.add_hash(hll::hash(c));
            }
            if let Some(t) = row_ts {
                entry.last_ts = Some(entry.last_ts.map_or(t, |cur| cur.max(t)));
            }
        }
    }

    let mut out = Out::new();

    for ((bucket5, metric), agg) in metrics {
        out.kind.append_value("metric");
        out.bucket.append_value(bucket5);
        out.key.append_value(&metric);
        out.count.append_value(agg.count);
        out.good.append_value(agg.good);
        out.poor.append_value(agg.poor);
        let blob = agg.hist.to_bytes();
        push_opt_bin(&mut out.value_hist, blob.as_deref());
        out.lcp_hist.append_null();
        out.cls_hist.append_null();
        out.inp_hist.append_null();
        out.cid_hll.append_null();
        out.msg_hll.append_null();
        out.min_line.append_null();
        out.min_col.append_null();
        out.last_ts.append_null();
        out.truncated.append_null();
    }

    for (bucket5, agg) in vitals_totals {
        out.kind.append_value("vitals_total");
        out.bucket.append_value(bucket5);
        out.key.append_null();
        out.count.append_value(agg.count);
        out.good.append_null();
        out.poor.append_null();
        out.value_hist.append_null();
        out.lcp_hist.append_null();
        out.cls_hist.append_null();
        out.inp_hist.append_null();
        out.cid_hll.append_value(agg.cid_hll.to_bytes());
        out.msg_hll.append_null();
        out.min_line.append_null();
        out.min_col.append_null();
        out.last_ts.append_null();
        out.truncated.append_null();
    }

    let (kept_pages, pages_truncated) = top_k(pages, PAGE_K, |p| p.count);
    for (path, agg) in kept_pages {
        out.kind.append_value("page");
        out.bucket.append_null();
        out.key.append_value(&path);
        out.count.append_value(agg.count);
        out.good.append_null();
        out.poor.append_null();
        out.value_hist.append_null();
        let lcp = agg.lcp.to_bytes();
        let cls = agg.cls.to_bytes();
        let inp = agg.inp.to_bytes();
        push_opt_bin(&mut out.lcp_hist, lcp.as_deref());
        push_opt_bin(&mut out.cls_hist, cls.as_deref());
        push_opt_bin(&mut out.inp_hist, inp.as_deref());
        out.cid_hll.append_null();
        out.msg_hll.append_null();
        out.min_line.append_null();
        out.min_col.append_null();
        out.last_ts.append_null();
        out.truncated.append_value(pages_truncated);
    }

    for (dim, map) in ["browser", "os", "device"].into_iter().zip(envs) {
        let (kept_env, env_truncated) = top_k(map, ENV_K, |&c| c);
        for (value, count) in kept_env {
            let key = serde_json::to_string(&[dim, value.as_str()]).expect("strings serialize");
            out.kind.append_value("env");
            out.bucket.append_null();
            out.key.append_value(&key);
            out.count.append_value(count);
            out.good.append_null();
            out.poor.append_null();
            out.value_hist.append_null();
            out.lcp_hist.append_null();
            out.cls_hist.append_null();
            out.inp_hist.append_null();
            out.cid_hll.append_null();
            out.msg_hll.append_null();
            out.min_line.append_null();
            out.min_col.append_null();
            out.last_ts.append_null();
            out.truncated.append_value(env_truncated);
        }
    }

    for (bucket5, agg) in errors_totals {
        out.kind.append_value("errors_total");
        out.bucket.append_value(bucket5);
        out.key.append_null();
        out.count.append_value(agg.count);
        out.good.append_null();
        out.poor.append_null();
        out.value_hist.append_null();
        out.lcp_hist.append_null();
        out.cls_hist.append_null();
        out.inp_hist.append_null();
        out.cid_hll.append_value(agg.cid_hll.to_bytes());
        out.msg_hll.append_value(agg.msg_hll.to_bytes());
        out.min_line.append_null();
        out.min_col.append_null();
        out.last_ts.append_null();
        out.truncated.append_null();
    }

    let (kept_errors, errors_truncated) = top_k(error_rows, ERROR_K, |e| e.count);
    for ((message, file), agg) in kept_errors {
        let key =
            serde_json::to_string(&[message.as_str(), file.as_str()]).expect("strings serialize");
        out.kind.append_value("error");
        out.bucket.append_null();
        out.key.append_value(&key);
        out.count.append_value(agg.count);
        out.good.append_null();
        out.poor.append_null();
        out.value_hist.append_null();
        out.lcp_hist.append_null();
        out.cls_hist.append_null();
        out.inp_hist.append_null();
        out.cid_hll.append_value(agg.cid_hll.to_bytes());
        out.msg_hll.append_null();
        match agg.min_line {
            Some(l) => out.min_line.append_value(l),
            None => out.min_line.append_null(),
        }
        match agg.min_col {
            Some(c) => out.min_col.append_value(c),
            None => out.min_col.append_null(),
        }
        match agg.last_ts {
            Some(t) => out.last_ts.append_value(t),
            None => out.last_ts.append_null(),
        }
        out.truncated.append_value(errors_truncated);
    }

    let batch = out.finish()?;
    convert::encode_record_batch(
        schema(),
        &batch,
        vec![KeyValue::new("flp_rum_summary".into(), VERSION.to_string())],
    )
}
