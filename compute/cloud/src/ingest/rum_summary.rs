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
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::metadata::KeyValue};
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

/// Accumulator that merges `rum_summary` window files (or bundles) into an hour, open-hour or
/// day bundle with the same `flp_rum_summary` schema and version.
#[derive(Default)]
pub struct BundleAcc {
    metrics: BTreeMap<(i64, String), MetricAgg>,
    vitals_totals: BTreeMap<i64, VitalsTotalAgg>,
    pages: BTreeMap<String, PageAgg>,
    pages_truncated: bool,
    envs: [BTreeMap<String, i64>; 3],
    envs_truncated: [bool; 3],
    errors_totals: BTreeMap<i64, ErrorsTotalAgg>,
    error_rows: BTreeMap<(String, String), ErrorAgg>,
    errors_truncated: bool,
    sampled: bool,
}

fn col_i64<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
}

fn col_bin<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a BinaryArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
}

fn col_bool<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a BooleanArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
}

fn get_i64(col: Option<&Int64Array>, row: usize) -> Option<i64> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

fn get_bin(col: Option<&BinaryArray>, row: usize) -> Option<&[u8]> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

fn get_bool(col: Option<&BooleanArray>, row: usize) -> Option<bool> {
    col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
}

fn merge_hist_opt(target: &mut Hist, blob: Option<&[u8]>) -> Result<(), String> {
    if let Some(bytes) = blob {
        if !target.merge_bytes(bytes) {
            return Err("rum_summary_hist_invalid".into());
        }
    }
    Ok(())
}

fn merge_hll_opt(target: &mut Hll, blob: Option<&[u8]>) -> Result<(), String> {
    if let Some(bytes) = blob {
        if !target.merge_bytes(bytes) {
            return Err("rum_summary_sketch_invalid".into());
        }
    }
    Ok(())
}

impl BundleAcc {
    /// Folds one `rum_summary` Parquet object (window summary or bundle) into this accumulator.
    pub fn fold(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|_| "rum_summary_unreadable")?;
        let version = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| {
                kv.iter()
                    .find(|entry| entry.key == "flp_rum_summary")
                    .and_then(|entry| entry.value.as_deref())
            });
        if version != Some(VERSION) {
            return Err("rum_summary_version_unsupported".into());
        }
        let reader = builder.build().map_err(|_| "rum_summary_unreadable")?;
        for batch in reader {
            let batch = batch.map_err(|_| "rum_summary_unreadable")?;
            let kinds = col_str(&batch, "kind").ok_or("rum_summary_unreadable")?;
            let buckets = col_ts(&batch, "bucket");
            let keys = col_str(&batch, "key");
            let counts = col_i64(&batch, "count");
            let goods = col_i64(&batch, "good");
            let poors = col_i64(&batch, "poor");
            let value_hists = col_bin(&batch, "value_hist");
            let lcp_hists = col_bin(&batch, "lcp_hist");
            let cls_hists = col_bin(&batch, "cls_hist");
            let inp_hists = col_bin(&batch, "inp_hist");
            let cid_hlls = col_bin(&batch, "cid_hll");
            let msg_hlls = col_bin(&batch, "msg_hll");
            let min_lines = col_i64(&batch, "min_line");
            let min_cols = col_i64(&batch, "min_col");
            let last_ts_col = col_ts(&batch, "last_ts");
            let truncated = col_bool(&batch, "truncated");

            for row in 0..batch.num_rows() {
                if kinds.is_null(row) {
                    continue;
                }
                match kinds.value(row) {
                    "metric" => {
                        let Some(bucket5) = get_ts(buckets, row) else {
                            continue;
                        };
                        let Some(metric) = get_str(keys, row) else {
                            continue;
                        };
                        let entry = self
                            .metrics
                            .entry((bucket5, metric.to_owned()))
                            .or_default();
                        entry.count += get_i64(counts, row).unwrap_or(0);
                        entry.good += get_i64(goods, row).unwrap_or(0);
                        entry.poor += get_i64(poors, row).unwrap_or(0);
                        merge_hist_opt(&mut entry.hist, get_bin(value_hists, row))?;
                    }
                    "vitals_total" => {
                        let Some(bucket5) = get_ts(buckets, row) else {
                            continue;
                        };
                        let entry = self.vitals_totals.entry(bucket5).or_default();
                        entry.count += get_i64(counts, row).unwrap_or(0);
                        merge_hll_opt(&mut entry.cid_hll, get_bin(cid_hlls, row))?;
                    }
                    "page" => {
                        let path = get_str(keys, row).unwrap_or("/");
                        if get_bool(truncated, row) == Some(true) {
                            self.pages_truncated = true;
                        }
                        let entry = self.pages.entry(path.to_owned()).or_default();
                        entry.count += get_i64(counts, row).unwrap_or(0);
                        merge_hist_opt(&mut entry.lcp, get_bin(lcp_hists, row))?;
                        merge_hist_opt(&mut entry.cls, get_bin(cls_hists, row))?;
                        merge_hist_opt(&mut entry.inp, get_bin(inp_hists, row))?;
                    }
                    "env" => {
                        let Some(raw_key) = get_str(keys, row) else {
                            continue;
                        };
                        let Ok([dim, val]) = serde_json::from_str::<[String; 2]>(raw_key) else {
                            continue;
                        };
                        let idx = match dim.as_str() {
                            "browser" => 0,
                            "os" => 1,
                            "device" => 2,
                            _ => continue,
                        };
                        if get_bool(truncated, row) == Some(true) {
                            self.envs_truncated[idx] = true;
                        }
                        *self.envs[idx].entry(val).or_default() +=
                            get_i64(counts, row).unwrap_or(0);
                    }
                    "errors_total" => {
                        let Some(bucket5) = get_ts(buckets, row) else {
                            continue;
                        };
                        let entry = self.errors_totals.entry(bucket5).or_default();
                        entry.count += get_i64(counts, row).unwrap_or(0);
                        merge_hll_opt(&mut entry.cid_hll, get_bin(cid_hlls, row))?;
                        merge_hll_opt(&mut entry.msg_hll, get_bin(msg_hlls, row))?;
                    }
                    "error" => {
                        let Some(raw_key) = get_str(keys, row) else {
                            continue;
                        };
                        let Ok([message, file]) = serde_json::from_str::<[String; 2]>(raw_key)
                        else {
                            continue;
                        };
                        if get_bool(truncated, row) == Some(true) {
                            self.errors_truncated = true;
                        }
                        let entry = self.error_rows.entry((message, file)).or_default();
                        entry.count += get_i64(counts, row).unwrap_or(0);
                        if let Some(l) = get_i64(min_lines, row) {
                            entry.min_line = Some(entry.min_line.map_or(l, |cur| cur.min(l)));
                        }
                        if let Some(c) = get_i64(min_cols, row) {
                            entry.min_col = Some(entry.min_col.map_or(c, |cur| cur.min(c)));
                        }
                        merge_hll_opt(&mut entry.cid_hll, get_bin(cid_hlls, row))?;
                        if let Some(t) = get_ts(last_ts_col, row) {
                            entry.last_ts = Some(entry.last_ts.map_or(t, |cur| cur.max(t)));
                        }
                    }
                    "sampled" => {
                        self.sampled = true;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Trims top-k maps to the schema caps so folding and resuming from a partial give identical
    /// state and bytes.
    pub fn prune(&mut self) {
        if self.pages.len() > PAGE_K {
            let (kept, trunc) = top_k(std::mem::take(&mut self.pages), PAGE_K, |p| p.count);
            self.pages = kept.into_iter().collect();
            self.pages_truncated |= trunc;
        }
        for idx in 0..3 {
            if self.envs[idx].len() > ENV_K {
                let (kept, trunc) = top_k(std::mem::take(&mut self.envs[idx]), ENV_K, |&c| c);
                self.envs[idx] = kept.into_iter().collect();
                self.envs_truncated[idx] |= trunc;
            }
        }
        if self.error_rows.len() > ERROR_K {
            let (kept, trunc) = top_k(std::mem::take(&mut self.error_rows), ERROR_K, |e| e.count);
            self.error_rows = kept.into_iter().collect();
            self.errors_truncated |= trunc;
        }
    }

    /// Encodes the accumulated state as a `rum_summary` Parquet file.
    pub fn write(&self) -> Result<Vec<u8>, String> {
        let mut out = Out::new();

        for ((bucket5, metric), agg) in &self.metrics {
            out.kind.append_value("metric");
            out.bucket.append_value(*bucket5);
            out.key.append_value(metric);
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

        for (bucket5, agg) in &self.vitals_totals {
            out.kind.append_value("vitals_total");
            out.bucket.append_value(*bucket5);
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

        let pages_truncated = self.pages_truncated || self.pages.len() > PAGE_K;
        let mut pages: Vec<(&String, &PageAgg)> = self.pages.iter().collect();
        pages.sort_by(|(ka, va), (kb, vb)| (Reverse(va.count), ka).cmp(&(Reverse(vb.count), kb)));
        pages.truncate(PAGE_K);
        for (path, agg) in pages {
            out.kind.append_value("page");
            out.bucket.append_null();
            out.key.append_value(path);
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

        for (idx, dim) in ["browser", "os", "device"].into_iter().enumerate() {
            let map = &self.envs[idx];
            let env_truncated = self.envs_truncated[idx] || map.len() > ENV_K;
            let mut items: Vec<(&String, &i64)> = map.iter().collect();
            items.sort_by(|(ka, va), (kb, vb)| (Reverse(**va), ka).cmp(&(Reverse(**vb), kb)));
            items.truncate(ENV_K);
            for (value, &count) in items {
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

        for (bucket5, agg) in &self.errors_totals {
            out.kind.append_value("errors_total");
            out.bucket.append_value(*bucket5);
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

        let errors_truncated = self.errors_truncated || self.error_rows.len() > ERROR_K;
        let mut error_items: Vec<(&(String, String), &ErrorAgg)> = self.error_rows.iter().collect();
        error_items
            .sort_by(|(ka, va), (kb, vb)| (Reverse(va.count), ka).cmp(&(Reverse(vb.count), kb)));
        error_items.truncate(ERROR_K);
        for ((message, file), agg) in error_items {
            let key = serde_json::to_string(&[message.as_str(), file.as_str()])
                .expect("strings serialize");
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

        if self.sampled {
            out.kind.append_value("sampled");
            out.bucket.append_null();
            out.key.append_null();
            out.count.append_value(1);
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
            out.truncated.append_null();
        }

        let batch = out.finish()?;
        convert::encode_record_batch(
            schema(),
            &batch,
            vec![KeyValue::new("flp_rum_summary".into(), VERSION.to_string())],
        )
    }
}

/// Merges RUM window summaries (or bundles) the way `bundle::rum_summary_bundle` does, folding
/// each in order and pruning between sources.
pub fn merge_windows(sources: &[&[u8]]) -> Result<Vec<u8>, String> {
    let mut acc = BundleAcc::default();
    for source in sources {
        acc.fold((*source).to_vec())?;
        acc.prune();
    }
    acc.write()
}

/// Builds the `rum_summary` Parquet bytes for one window `[start, end)` (seconds).
pub fn write_summary(
    vitals: &[&RecordBatch],
    errors: &[&RecordBatch],
    start: i64,
) -> Result<Vec<u8>, String> {
    let default_bucket = start * 1000;
    let mut acc = BundleAcc::default();

    for batch in vitals {
        let ts = col_ts(batch, "timestamp");
        let name = col_str(batch, "metric_name");
        let value = col_f64(batch, "metric_value");
        let pathname = col_str(batch, "pathname");
        let browser = col_str(batch, "browser");
        let os = col_str(batch, "os");
        let device = col_str(batch, "device");
        let cid = col_str(batch, "cid");
        let sample_rate = col_f64(batch, "sample_rate");

        for row in 0..batch.num_rows() {
            // A resource timing measures one request, not the page view: it is read raw.
            if get_str(name, row) == Some("ttlb") {
                continue;
            }
            if get_f64(sample_rate, row).is_some_and(|sr| sr > 0.0 && sr < 1.0) {
                acc.sampled = true;
            }
            let row_ts = get_ts(ts, row).unwrap_or(default_bucket);
            let bucket5 = row_ts - row_ts.rem_euclid(BUCKET_MS);
            let vt = acc.vitals_totals.entry(bucket5).or_default();
            vt.count += 1;
            if let Some(c) = get_str(cid, row) {
                vt.cid_hll.add_hash(hll::hash(c));
            }

            let upper = get_str(name, row).unwrap_or("").to_ascii_uppercase();
            let val = get_f64(value, row);
            let m = acc.metrics.entry((bucket5, upper.clone())).or_default();
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
            let p = acc.pages.entry(path.to_owned()).or_default();
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
                *acc.envs[idx].entry(v.to_owned()).or_default() += 1;
            }
        }
    }

    for batch in errors {
        let ts = col_ts(batch, "timestamp");
        let msg = col_str(batch, "error_message");
        let file = col_str(batch, "error_file");
        let line = col_i32(batch, "error_line");
        let col = col_i32(batch, "error_col");
        let cid = col_str(batch, "cid");
        let sample_rate = col_f64(batch, "sample_rate");

        for row in 0..batch.num_rows() {
            if get_f64(sample_rate, row).is_some_and(|sr| sr > 0.0 && sr < 1.0) {
                acc.sampled = true;
            }
            let row_ts = get_ts(ts, row);
            let bucket5 = row_ts
                .map(|t| t - t.rem_euclid(BUCKET_MS))
                .unwrap_or(default_bucket - default_bucket.rem_euclid(BUCKET_MS));
            let et = acc.errors_totals.entry(bucket5).or_default();
            et.count += 1;
            if let Some(c) = get_str(cid, row) {
                et.cid_hll.add_hash(hll::hash(c));
            }
            if let Some(m) = get_str(msg, row) {
                et.msg_hll.add_hash(hll::hash(m));
            }

            let err_msg = get_str(msg, row).unwrap_or("Unknown Error");
            let err_file = get_str(file, row).unwrap_or("");
            let entry = acc
                .error_rows
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

    acc.write()
}
