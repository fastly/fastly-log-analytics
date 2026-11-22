//! Per-window CMCD summary Parquet, written by the ingest worker next to CMCD-bearing request
//! windows at `processed/<table>/cmcd_summary/<window naming>`.
//!
//! Keeps CMCD metrics out of the main summary's 128-column budget and hour-merge path.

use super::convert::{self, FastMap};
use super::hist::Hist;
use super::hll::Hll;
use arrow_array::{
    builder::{BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::file::metadata::KeyValue;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub const CMCD_SUMMARY_VERSION: &str = "1";
pub const SERIES_STEP_MS: i64 = super::summary::SERIES_STEP_MS;
pub const SESSION_K: usize = 2000;
pub const CONTENT_K: usize = 500;
pub const COUNTRY_K: usize = 300;
pub const ASN_K: usize = 500;

pub fn cmcd_summary_prefix(scope: &str, table: &str) -> String {
    format!("{scope}processed/{table}/cmcd_summary/")
}

pub fn cmcd_summary_key(prefix: &str, start: i64, end: i64, run_id: &str, rows: u64) -> String {
    super::processed_key(prefix, start, end, run_id, rows)
}

/// True when any batch carries at least one non-null value in `cmcd_sid`, `cmcd_br`, `cmcd_bl`,
/// or `cmcd_ot` (the raw CMCD base filter).
pub fn batches_have_cmcd(batches: &[&RecordBatch]) -> bool {
    const MARKERS: [&str; 4] = ["cmcd_sid", "cmcd_br", "cmcd_bl", "cmcd_ot"];
    batches.iter().any(|batch| {
        MARKERS.iter().any(|name| {
            batch
                .column_by_name(name)
                .is_some_and(|col| col.null_count() < col.len())
        })
    })
}

pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("kind", DataType::Utf8, false),
                Field::new("bucket", DataType::Int64, true),
                Field::new("key", DataType::Utf8, true),
                Field::new("label", DataType::Utf8, true),
                Field::new("truncated", DataType::Boolean, true),
                Field::new("reqs", DataType::Int64, true),
                Field::new("sid_hll", DataType::Binary, true),
                Field::new("rebuffer_events", DataType::Int64, true),
                Field::new("rebuffer_sid_hll", DataType::Binary, true),
                Field::new("has_bs", DataType::Int64, true),
                Field::new("has_su", DataType::Int64, true),
                Field::new("has_ot", DataType::Int64, true),
                Field::new("has_bl", DataType::Int64, true),
                Field::new("has_br", DataType::Int64, true),
                Field::new("has_mtp", DataType::Int64, true),
                Field::new("has_cid", DataType::Int64, true),
                Field::new("br_sum", DataType::Float64, true),
                Field::new("br_n", DataType::Int64, true),
                Field::new("tb_sum", DataType::Float64, true),
                Field::new("tb_n", DataType::Int64, true),
                Field::new("bl_sum", DataType::Float64, true),
                Field::new("bl_n", DataType::Int64, true),
                Field::new("mtp_sum", DataType::Float64, true),
                Field::new("mtp_n", DataType::Int64, true),
                Field::new("v_br_sum", DataType::Float64, true),
                Field::new("v_br_n", DataType::Int64, true),
                Field::new("v_tb_sum", DataType::Float64, true),
                Field::new("v_tb_n", DataType::Int64, true),
                Field::new("v_bl_sum", DataType::Float64, true),
                Field::new("v_bl_n", DataType::Int64, true),
                Field::new("util_sum", DataType::Float64, true),
                Field::new("util_n", DataType::Int64, true),
                Field::new("util_all_sum", DataType::Float64, true),
                Field::new("util_all_n", DataType::Int64, true),
                Field::new("startup_requests", DataType::Int64, true),
                Field::new("su_bl_sum", DataType::Float64, true),
                Field::new("su_bl_n", DataType::Int64, true),
                Field::new("su_ttfb_sum", DataType::Float64, true),
                Field::new("su_ttfb_n", DataType::Int64, true),
                Field::new("v_bl_hist", DataType::Binary, true),
                Field::new("bl_hist", DataType::Binary, true),
                Field::new("v_mtp_hist", DataType::Binary, true),
                Field::new("mtp_hist", DataType::Binary, true),
                Field::new("min_ts", DataType::Int64, true),
                Field::new("max_ts", DataType::Int64, true),
                Field::new("first_ts", DataType::Int64, true),
                Field::new("last_ts", DataType::Int64, true),
            ]))
        })
        .clone()
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch.column_by_name(name)?.as_any().downcast_ref()
}

fn get_str(col: Option<&StringArray>, i: usize) -> Option<&str> {
    let arr = col?;
    arr.is_valid(i).then(|| arr.value(i))
}

fn get_non_empty_str(col: Option<&StringArray>, i: usize) -> Option<&str> {
    get_str(col, i).filter(|s| !s.is_empty())
}

enum NumCol<'a> {
    Int(&'a Int64Array),
    Float(&'a Float64Array),
}

impl<'a> NumCol<'a> {
    fn new(batch: &'a RecordBatch, name: &str) -> Option<Self> {
        let any = batch.column_by_name(name)?.as_any();
        if let Some(a) = any.downcast_ref::<Int64Array>() {
            Some(Self::Int(a))
        } else {
            any.downcast_ref::<Float64Array>().map(Self::Float)
        }
    }

    fn get(&self, i: usize) -> Option<f64> {
        match self {
            Self::Int(a) => a.is_valid(i).then(|| a.value(i) as f64),
            Self::Float(a) => a.is_valid(i).then(|| a.value(i)).filter(|v| v.is_finite()),
        }
    }

    fn get_i64(&self, i: usize) -> Option<i64> {
        match self {
            Self::Int(a) => a.is_valid(i).then(|| a.value(i)),
            Self::Float(a) => a
                .is_valid(i)
                .then(|| a.value(i))
                .filter(|v| v.is_finite())
                .map(|v| v.round() as i64),
        }
    }
}

enum BoolCol<'a> {
    Bool(&'a BooleanArray),
    Int(&'a Int64Array),
}

impl<'a> BoolCol<'a> {
    fn new(batch: &'a RecordBatch, name: &str) -> Option<Self> {
        let any = batch.column_by_name(name)?.as_any();
        if let Some(a) = any.downcast_ref::<BooleanArray>() {
            Some(Self::Bool(a))
        } else {
            any.downcast_ref::<Int64Array>().map(Self::Int)
        }
    }

    fn get(&self, i: usize) -> Option<bool> {
        match self {
            Self::Bool(a) => a.is_valid(i).then(|| a.value(i)),
            Self::Int(a) => a.is_valid(i).then(|| a.value(i) != 0),
        }
    }
}

#[derive(Default)]
struct BucketAgg {
    reqs: i64,
    sid_hll: Hll,
    rebuffer_events: i64,
    rebuffer_sid_hll: Hll,
    has_bs: i64,
    has_su: i64,
    has_ot: i64,
    has_bl: i64,
    has_br: i64,
    has_mtp: i64,
    has_cid: i64,
    br_sum: f64,
    br_n: i64,
    tb_sum: f64,
    tb_n: i64,
    bl_sum: f64,
    bl_n: i64,
    mtp_sum: f64,
    mtp_n: i64,
    v_br_sum: f64,
    v_br_n: i64,
    v_tb_sum: f64,
    v_tb_n: i64,
    v_bl_sum: f64,
    v_bl_n: i64,
    util_sum: f64,
    util_n: i64,
    util_all_sum: f64,
    util_all_n: i64,
    startup_requests: i64,
    su_bl_sum: f64,
    su_bl_n: i64,
    su_ttfb_sum: f64,
    su_ttfb_n: i64,
    v_bl_hist: Hist,
    bl_hist: Hist,
    v_mtp_hist: Hist,
    mtp_hist: Hist,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

struct SessionAgg {
    first_ts: i64,
    last_ts: i64,
    reqs: i64,
}

#[derive(Default)]
struct ContentAgg {
    reqs: i64,
    sid_hll: Hll,
    rebuffer_sid_hll: Hll,
    has_bs: i64,
    v_br_sum: f64,
    v_br_n: i64,
    br_sum: f64,
    br_n: i64,
    v_bl_sum: f64,
    v_bl_n: i64,
    bl_sum: f64,
    bl_n: i64,
}

#[derive(Default)]
struct CountryAgg {
    reqs: i64,
    sid_hll: Hll,
    rebuffer_events: i64,
    rebuffer_sid_hll: Hll,
    has_bs: i64,
    v_mtp_hist: Hist,
    mtp_hist: Hist,
}

#[derive(Default)]
struct AsnAgg {
    reqs: i64,
    label: Option<String>,
    sid_hll: Hll,
    rebuffer_events: i64,
    rebuffer_sid_hll: Hll,
    has_bs: i64,
    v_br_sum: f64,
    v_br_n: i64,
    br_sum: f64,
    br_n: i64,
    v_bl_sum: f64,
    v_bl_n: i64,
    bl_sum: f64,
    bl_n: i64,
    mtp_sum: f64,
    mtp_n: i64,
}

fn hll_opt(hll: &Hll) -> Option<Vec<u8>> {
    match hll {
        Hll::Sparse(entries) if entries.is_empty() => None,
        _ => Some(hll.to_bytes()),
    }
}

fn hist_opt(hist: &Hist) -> Option<Vec<u8>> {
    hist.to_bytes()
}

fn scaled_hist_val(v: f64) -> i64 {
    (v * 1000.0).round() as i64
}

fn top_keys<K: Ord + Clone>(
    items: impl Iterator<Item = (K, i64)>,
    limit: usize,
) -> (Vec<K>, usize, bool) {
    let mut ranked: Vec<(Reverse<i64>, K)> = items.map(|(k, reqs)| (Reverse(reqs), k)).collect();
    let total = ranked.len();
    let truncated = total > limit;
    if truncated {
        ranked.select_nth_unstable(limit);
        ranked.truncate(limit);
    }
    ranked.sort_unstable();
    (
        ranked.into_iter().map(|(_, k)| k).collect(),
        total,
        truncated,
    )
}

#[derive(Default)]
struct RowOut {
    kind: String,
    bucket: Option<i64>,
    key: Option<String>,
    label: Option<String>,
    truncated: Option<bool>,
    reqs: Option<i64>,
    sid_hll: Option<Vec<u8>>,
    rebuffer_events: Option<i64>,
    rebuffer_sid_hll: Option<Vec<u8>>,
    has_bs: Option<i64>,
    has_su: Option<i64>,
    has_ot: Option<i64>,
    has_bl: Option<i64>,
    has_br: Option<i64>,
    has_mtp: Option<i64>,
    has_cid: Option<i64>,
    br_sum: Option<f64>,
    br_n: Option<i64>,
    tb_sum: Option<f64>,
    tb_n: Option<i64>,
    bl_sum: Option<f64>,
    bl_n: Option<i64>,
    mtp_sum: Option<f64>,
    mtp_n: Option<i64>,
    v_br_sum: Option<f64>,
    v_br_n: Option<i64>,
    v_tb_sum: Option<f64>,
    v_tb_n: Option<i64>,
    v_bl_sum: Option<f64>,
    v_bl_n: Option<i64>,
    util_sum: Option<f64>,
    util_n: Option<i64>,
    util_all_sum: Option<f64>,
    util_all_n: Option<i64>,
    startup_requests: Option<i64>,
    su_bl_sum: Option<f64>,
    su_bl_n: Option<i64>,
    su_ttfb_sum: Option<f64>,
    su_ttfb_n: Option<i64>,
    v_bl_hist: Option<Vec<u8>>,
    bl_hist: Option<Vec<u8>>,
    v_mtp_hist: Option<Vec<u8>>,
    mtp_hist: Option<Vec<u8>>,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
}

pub fn write_cmcd_summary(
    batches: &[&RecordBatch],
    window_start_s: i64,
) -> Result<Vec<u8>, String> {
    let mut buckets: BTreeMap<i64, BucketAgg> = BTreeMap::new();
    let mut sessions: FastMap<String, SessionAgg> = FastMap::default();
    let mut contents: FastMap<String, ContentAgg> = FastMap::default();
    let mut countries: FastMap<String, CountryAgg> = FastMap::default();
    let mut asns: FastMap<i64, AsnAgg> = FastMap::default();
    let mut ots: BTreeMap<String, i64> = BTreeMap::new();
    let mut sfs: BTreeMap<String, (i64, Hll)> = BTreeMap::new();

    for batch in batches {
        let ts_ms_col = NumCol::new(batch, "ts_ms");
        let ts_col: Option<&TimestampMillisecondArray> = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref());

        let sid_col = str_col(batch, "cmcd_sid");
        let cid_col = str_col(batch, "cmcd_cid");
        let br_col = NumCol::new(batch, "cmcd_br");
        let tb_col = NumCol::new(batch, "cmcd_tb");
        let bl_col = NumCol::new(batch, "cmcd_bl");
        let bs_col = BoolCol::new(batch, "cmcd_bs");
        let mtp_col = NumCol::new(batch, "cmcd_mtp");
        let ot_col = str_col(batch, "cmcd_ot");
        let sf_col = str_col(batch, "cmcd_sf");
        let su_col = BoolCol::new(batch, "cmcd_su");

        let geo_country_col = str_col(batch, "geo_country");
        let country_col = str_col(batch, "country");
        let client_asn_col = NumCol::new(batch, "client_asn");
        let asn_col = NumCol::new(batch, "asn");
        let as_name_col = str_col(batch, "as_name");

        let ttfb_ms_col = NumCol::new(batch, "ttfb_ms");
        let ttfb_col = NumCol::new(batch, "ttfb");
        let time_elapsed_usec_col = NumCol::new(batch, "time_elapsed_usec");
        let elapsed_col = NumCol::new(batch, "elapsed");

        for i in 0..batch.num_rows() {
            let sid = get_str(sid_col, i);
            let br = br_col.as_ref().and_then(|c| c.get(i));
            let bl = bl_col.as_ref().and_then(|c| c.get(i));
            let ot = get_str(ot_col, i);
            if sid.is_none() && br.is_none() && bl.is_none() && ot.is_none() {
                continue;
            }

            let ts_ms = ts_ms_col
                .as_ref()
                .and_then(|c| c.get_i64(i))
                .or_else(|| ts_col.and_then(|c| c.is_valid(i).then(|| c.value(i))))
                .unwrap_or(window_start_s * 1000);
            let bucket5 = ts_ms - ts_ms.rem_euclid(SERIES_STEP_MS);

            let tb = tb_col.as_ref().and_then(|c| c.get(i));
            let bs = bs_col.as_ref().and_then(|c| c.get(i));
            let mtp = mtp_col.as_ref().and_then(|c| c.get(i));
            let su = su_col.as_ref().and_then(|c| c.get(i));
            let cid = get_non_empty_str(cid_col, i);
            let sf = get_non_empty_str(sf_col, i);
            let ot_non_empty = ot.filter(|s| !s.is_empty());
            let is_video = ot_non_empty == Some("v");
            let is_rebuf = bs == Some(true);
            let is_su = su == Some(true);
            let sid_non_empty = sid.filter(|s| !s.is_empty());
            let sid_hash = sid_non_empty.map(super::hll::hash);

            let b = buckets.entry(bucket5).or_default();
            b.reqs += 1;
            b.min_ts = Some(b.min_ts.map_or(ts_ms, |m| m.min(ts_ms)));
            b.max_ts = Some(b.max_ts.map_or(ts_ms, |m| m.max(ts_ms)));
            if let Some(h) = sid_hash {
                b.sid_hll.add_hash(h);
                if is_rebuf {
                    b.rebuffer_sid_hll.add_hash(h);
                }
            }
            if is_rebuf {
                b.rebuffer_events += 1;
            }
            if bs.is_some() {
                b.has_bs += 1;
            }
            if su.is_some() {
                b.has_su += 1;
            }
            if ot_non_empty.is_some() {
                b.has_ot += 1;
            }
            if bl.is_some() {
                b.has_bl += 1;
            }
            if br.is_some() {
                b.has_br += 1;
            }
            if mtp.is_some() {
                b.has_mtp += 1;
            }
            if cid.is_some() {
                b.has_cid += 1;
            }
            if let Some(v) = br {
                b.br_sum += v;
                b.br_n += 1;
                if is_video {
                    b.v_br_sum += v;
                    b.v_br_n += 1;
                }
            }
            if let Some(v) = tb {
                b.tb_sum += v;
                b.tb_n += 1;
                if is_video {
                    b.v_tb_sum += v;
                    b.v_tb_n += 1;
                }
            }
            if let Some(v) = bl {
                b.bl_sum += v;
                b.bl_n += 1;
                let hv = scaled_hist_val(v);
                b.bl_hist.add(hv);
                if is_video {
                    b.v_bl_sum += v;
                    b.v_bl_n += 1;
                    b.v_bl_hist.add(hv);
                }
            }
            if let Some(v) = mtp {
                b.mtp_sum += v;
                b.mtp_n += 1;
                let hv = scaled_hist_val(v);
                b.mtp_hist.add(hv);
                if is_video {
                    b.v_mtp_hist.add(hv);
                }
            }
            if let (Some(br_v), Some(tb_v)) = (br, tb) {
                if tb_v > 0.0 {
                    let ratio = br_v / tb_v;
                    b.util_all_sum += ratio;
                    b.util_all_n += 1;
                    if is_video {
                        b.util_sum += ratio;
                        b.util_n += 1;
                    }
                }
            }
            if is_su {
                b.startup_requests += 1;
                if let Some(v) = bl {
                    b.su_bl_sum += v;
                    b.su_bl_n += 1;
                }
                let ttfb_ms = ttfb_ms_col
                    .as_ref()
                    .and_then(|c| c.get(i))
                    .or_else(|| ttfb_col.as_ref().and_then(|c| c.get(i)).map(|v| v * 1000.0))
                    .or_else(|| {
                        time_elapsed_usec_col
                            .as_ref()
                            .and_then(|c| c.get(i))
                            .map(|v| v / 1000.0)
                    })
                    .or_else(|| {
                        elapsed_col
                            .as_ref()
                            .and_then(|c| c.get(i))
                            .map(|v| v / 1000.0)
                    });
                if let Some(v) = ttfb_ms {
                    b.su_ttfb_sum += v;
                    b.su_ttfb_n += 1;
                }
            }

            if let Some(s) = sid_non_empty {
                sessions
                    .entry(s.to_owned())
                    .and_modify(|agg| {
                        agg.first_ts = agg.first_ts.min(ts_ms);
                        agg.last_ts = agg.last_ts.max(ts_ms);
                        agg.reqs += 1;
                    })
                    .or_insert(SessionAgg {
                        first_ts: ts_ms,
                        last_ts: ts_ms,
                        reqs: 1,
                    });
            }

            if let Some(c) = cid {
                let agg = contents.entry(c.to_owned()).or_default();
                agg.reqs += 1;
                if let Some(h) = sid_hash {
                    agg.sid_hll.add_hash(h);
                    if is_rebuf {
                        agg.rebuffer_sid_hll.add_hash(h);
                    }
                }
                if bs.is_some() {
                    agg.has_bs += 1;
                }
                if let Some(v) = br {
                    agg.br_sum += v;
                    agg.br_n += 1;
                    if is_video {
                        agg.v_br_sum += v;
                        agg.v_br_n += 1;
                    }
                }
                if let Some(v) = bl {
                    agg.bl_sum += v;
                    agg.bl_n += 1;
                    if is_video {
                        agg.v_bl_sum += v;
                        agg.v_bl_n += 1;
                    }
                }
            }

            if let Some(country) =
                get_non_empty_str(geo_country_col, i).or_else(|| get_non_empty_str(country_col, i))
            {
                let agg = countries.entry(country.to_owned()).or_default();
                agg.reqs += 1;
                if let Some(h) = sid_hash {
                    agg.sid_hll.add_hash(h);
                    if is_rebuf {
                        agg.rebuffer_sid_hll.add_hash(h);
                    }
                }
                if is_rebuf {
                    agg.rebuffer_events += 1;
                }
                if bs.is_some() {
                    agg.has_bs += 1;
                }
                if let Some(v) = mtp {
                    let hv = scaled_hist_val(v);
                    agg.mtp_hist.add(hv);
                    if is_video {
                        agg.v_mtp_hist.add(hv);
                    }
                }
            }

            if let Some(asn) = client_asn_col
                .as_ref()
                .and_then(|c| c.get_i64(i))
                .or_else(|| asn_col.as_ref().and_then(|c| c.get_i64(i)))
            {
                let agg = asns.entry(asn).or_default();
                agg.reqs += 1;
                if let Some(name) = get_non_empty_str(as_name_col, i) {
                    match &agg.label {
                        Some(cur) if cur.as_str() <= name => {}
                        _ => agg.label = Some(name.to_owned()),
                    }
                }
                if let Some(h) = sid_hash {
                    agg.sid_hll.add_hash(h);
                    if is_rebuf {
                        agg.rebuffer_sid_hll.add_hash(h);
                    }
                }
                if is_rebuf {
                    agg.rebuffer_events += 1;
                }
                if bs.is_some() {
                    agg.has_bs += 1;
                }
                if let Some(v) = br {
                    agg.br_sum += v;
                    agg.br_n += 1;
                    if is_video {
                        agg.v_br_sum += v;
                        agg.v_br_n += 1;
                    }
                }
                if let Some(v) = bl {
                    agg.bl_sum += v;
                    agg.bl_n += 1;
                    if is_video {
                        agg.v_bl_sum += v;
                        agg.v_bl_n += 1;
                    }
                }
                if let Some(v) = mtp {
                    agg.mtp_sum += v;
                    agg.mtp_n += 1;
                }
            }

            if let Some(o) = ot_non_empty {
                *ots.entry(o.to_owned()).or_default() += 1;
            }
            if let Some(s) = sf {
                let entry = sfs.entry(s.to_owned()).or_default();
                entry.0 += 1;
                if let Some(h) = sid_hash {
                    entry.1.add_hash(h);
                }
            }
        }
    }

    let mut rows: Vec<RowOut> = Vec::new();
    for (bucket, b) in buckets {
        rows.push(RowOut {
            kind: "bucket".into(),
            bucket: Some(bucket),
            reqs: Some(b.reqs),
            sid_hll: hll_opt(&b.sid_hll),
            rebuffer_events: Some(b.rebuffer_events),
            rebuffer_sid_hll: hll_opt(&b.rebuffer_sid_hll),
            has_bs: Some(b.has_bs),
            has_su: Some(b.has_su),
            has_ot: Some(b.has_ot),
            has_bl: Some(b.has_bl),
            has_br: Some(b.has_br),
            has_mtp: Some(b.has_mtp),
            has_cid: Some(b.has_cid),
            br_sum: Some(b.br_sum),
            br_n: Some(b.br_n),
            tb_sum: Some(b.tb_sum),
            tb_n: Some(b.tb_n),
            bl_sum: Some(b.bl_sum),
            bl_n: Some(b.bl_n),
            mtp_sum: Some(b.mtp_sum),
            mtp_n: Some(b.mtp_n),
            v_br_sum: Some(b.v_br_sum),
            v_br_n: Some(b.v_br_n),
            v_tb_sum: Some(b.v_tb_sum),
            v_tb_n: Some(b.v_tb_n),
            v_bl_sum: Some(b.v_bl_sum),
            v_bl_n: Some(b.v_bl_n),
            util_sum: Some(b.util_sum),
            util_n: Some(b.util_n),
            util_all_sum: Some(b.util_all_sum),
            util_all_n: Some(b.util_all_n),
            startup_requests: Some(b.startup_requests),
            su_bl_sum: Some(b.su_bl_sum),
            su_bl_n: Some(b.su_bl_n),
            su_ttfb_sum: Some(b.su_ttfb_sum),
            su_ttfb_n: Some(b.su_ttfb_n),
            v_bl_hist: hist_opt(&b.v_bl_hist),
            bl_hist: hist_opt(&b.bl_hist),
            v_mtp_hist: hist_opt(&b.v_mtp_hist),
            mtp_hist: hist_opt(&b.mtp_hist),
            min_ts: b.min_ts,
            max_ts: b.max_ts,
            ..Default::default()
        });
    }

    let (kept_sessions, total_sessions, trunc_sessions) =
        top_keys(sessions.iter().map(|(k, v)| (k.clone(), v.reqs)), SESSION_K);
    rows.push(RowOut {
        kind: "totals".into(),
        key: Some("session".into()),
        truncated: Some(trunc_sessions),
        reqs: Some(total_sessions as i64),
        ..Default::default()
    });
    for k in kept_sessions {
        let s = &sessions[&k];
        rows.push(RowOut {
            kind: "session".into(),
            key: Some(k),
            reqs: Some(s.reqs),
            first_ts: Some(s.first_ts),
            last_ts: Some(s.last_ts),
            ..Default::default()
        });
    }

    let (kept_contents, total_contents, trunc_contents) =
        top_keys(contents.iter().map(|(k, v)| (k.clone(), v.reqs)), CONTENT_K);
    rows.push(RowOut {
        kind: "totals".into(),
        key: Some("content".into()),
        truncated: Some(trunc_contents),
        reqs: Some(total_contents as i64),
        ..Default::default()
    });
    for k in kept_contents {
        let c = &contents[&k];
        rows.push(RowOut {
            kind: "content".into(),
            key: Some(k),
            reqs: Some(c.reqs),
            sid_hll: hll_opt(&c.sid_hll),
            rebuffer_sid_hll: hll_opt(&c.rebuffer_sid_hll),
            has_bs: Some(c.has_bs),
            v_br_sum: Some(c.v_br_sum),
            v_br_n: Some(c.v_br_n),
            br_sum: Some(c.br_sum),
            br_n: Some(c.br_n),
            v_bl_sum: Some(c.v_bl_sum),
            v_bl_n: Some(c.v_bl_n),
            bl_sum: Some(c.bl_sum),
            bl_n: Some(c.bl_n),
            ..Default::default()
        });
    }

    let (kept_countries, total_countries, trunc_countries) = top_keys(
        countries.iter().map(|(k, v)| (k.clone(), v.reqs)),
        COUNTRY_K,
    );
    rows.push(RowOut {
        kind: "totals".into(),
        key: Some("country".into()),
        truncated: Some(trunc_countries),
        reqs: Some(total_countries as i64),
        ..Default::default()
    });
    for k in kept_countries {
        let c = &countries[&k];
        rows.push(RowOut {
            kind: "country".into(),
            key: Some(k),
            reqs: Some(c.reqs),
            sid_hll: hll_opt(&c.sid_hll),
            rebuffer_events: Some(c.rebuffer_events),
            rebuffer_sid_hll: hll_opt(&c.rebuffer_sid_hll),
            has_bs: Some(c.has_bs),
            v_mtp_hist: hist_opt(&c.v_mtp_hist),
            mtp_hist: hist_opt(&c.mtp_hist),
            ..Default::default()
        });
    }

    let (kept_asns, total_asns, trunc_asns) =
        top_keys(asns.iter().map(|(k, v)| (*k, v.reqs)), ASN_K);
    rows.push(RowOut {
        kind: "totals".into(),
        key: Some("asn".into()),
        truncated: Some(trunc_asns),
        reqs: Some(total_asns as i64),
        ..Default::default()
    });
    for k in kept_asns {
        let a = &asns[&k];
        rows.push(RowOut {
            kind: "asn".into(),
            key: Some(k.to_string()),
            label: a.label.clone(),
            reqs: Some(a.reqs),
            sid_hll: hll_opt(&a.sid_hll),
            rebuffer_events: Some(a.rebuffer_events),
            rebuffer_sid_hll: hll_opt(&a.rebuffer_sid_hll),
            has_bs: Some(a.has_bs),
            v_br_sum: Some(a.v_br_sum),
            v_br_n: Some(a.v_br_n),
            br_sum: Some(a.br_sum),
            br_n: Some(a.br_n),
            v_bl_sum: Some(a.v_bl_sum),
            v_bl_n: Some(a.v_bl_n),
            bl_sum: Some(a.bl_sum),
            bl_n: Some(a.bl_n),
            mtp_sum: Some(a.mtp_sum),
            mtp_n: Some(a.mtp_n),
            ..Default::default()
        });
    }

    for (k, reqs) in ots {
        rows.push(RowOut {
            kind: "ot".into(),
            key: Some(k),
            reqs: Some(reqs),
            ..Default::default()
        });
    }

    for (k, (reqs, hll)) in sfs {
        rows.push(RowOut {
            kind: "sf".into(),
            key: Some(k),
            reqs: Some(reqs),
            sid_hll: hll_opt(&hll),
            ..Default::default()
        });
    }

    let batch = build_batch(&rows)?;
    convert::encode_record_batch(
        schema(),
        &batch,
        vec![KeyValue::new(
            "flp_cmcd_summary".to_string(),
            CMCD_SUMMARY_VERSION.to_string(),
        )],
    )
}

fn build_batch(rows: &[RowOut]) -> Result<RecordBatch, String> {
    let n = rows.len();
    let mut kind = StringBuilder::with_capacity(n, n * 8);
    let mut bucket = Int64Builder::with_capacity(n);
    let mut key = StringBuilder::with_capacity(n, n * 16);
    let mut label = StringBuilder::with_capacity(n, n * 16);
    let mut truncated = BooleanBuilder::with_capacity(n);
    let mut reqs = Int64Builder::with_capacity(n);
    let mut sid_hll = BinaryBuilder::with_capacity(n, n * 32);
    let mut rebuffer_events = Int64Builder::with_capacity(n);
    let mut rebuffer_sid_hll = BinaryBuilder::with_capacity(n, n * 32);
    let mut has_bs = Int64Builder::with_capacity(n);
    let mut has_su = Int64Builder::with_capacity(n);
    let mut has_ot = Int64Builder::with_capacity(n);
    let mut has_bl = Int64Builder::with_capacity(n);
    let mut has_br = Int64Builder::with_capacity(n);
    let mut has_mtp = Int64Builder::with_capacity(n);
    let mut has_cid = Int64Builder::with_capacity(n);
    let mut br_sum = Float64Builder::with_capacity(n);
    let mut br_n = Int64Builder::with_capacity(n);
    let mut tb_sum = Float64Builder::with_capacity(n);
    let mut tb_n = Int64Builder::with_capacity(n);
    let mut bl_sum = Float64Builder::with_capacity(n);
    let mut bl_n = Int64Builder::with_capacity(n);
    let mut mtp_sum = Float64Builder::with_capacity(n);
    let mut mtp_n = Int64Builder::with_capacity(n);
    let mut v_br_sum = Float64Builder::with_capacity(n);
    let mut v_br_n = Int64Builder::with_capacity(n);
    let mut v_tb_sum = Float64Builder::with_capacity(n);
    let mut v_tb_n = Int64Builder::with_capacity(n);
    let mut v_bl_sum = Float64Builder::with_capacity(n);
    let mut v_bl_n = Int64Builder::with_capacity(n);
    let mut util_sum = Float64Builder::with_capacity(n);
    let mut util_n = Int64Builder::with_capacity(n);
    let mut util_all_sum = Float64Builder::with_capacity(n);
    let mut util_all_n = Int64Builder::with_capacity(n);
    let mut startup_requests = Int64Builder::with_capacity(n);
    let mut su_bl_sum = Float64Builder::with_capacity(n);
    let mut su_bl_n = Int64Builder::with_capacity(n);
    let mut su_ttfb_sum = Float64Builder::with_capacity(n);
    let mut su_ttfb_n = Int64Builder::with_capacity(n);
    let mut v_bl_hist = BinaryBuilder::with_capacity(n, n * 32);
    let mut bl_hist = BinaryBuilder::with_capacity(n, n * 32);
    let mut v_mtp_hist = BinaryBuilder::with_capacity(n, n * 32);
    let mut mtp_hist = BinaryBuilder::with_capacity(n, n * 32);
    let mut min_ts = Int64Builder::with_capacity(n);
    let mut max_ts = Int64Builder::with_capacity(n);
    let mut first_ts = Int64Builder::with_capacity(n);
    let mut last_ts = Int64Builder::with_capacity(n);

    for r in rows {
        kind.append_value(&r.kind);
        bucket.append_option(r.bucket);
        key.append_option(r.key.as_deref());
        label.append_option(r.label.as_deref());
        truncated.append_option(r.truncated);
        reqs.append_option(r.reqs);
        sid_hll.append_option(r.sid_hll.as_deref());
        rebuffer_events.append_option(r.rebuffer_events);
        rebuffer_sid_hll.append_option(r.rebuffer_sid_hll.as_deref());
        has_bs.append_option(r.has_bs);
        has_su.append_option(r.has_su);
        has_ot.append_option(r.has_ot);
        has_bl.append_option(r.has_bl);
        has_br.append_option(r.has_br);
        has_mtp.append_option(r.has_mtp);
        has_cid.append_option(r.has_cid);
        br_sum.append_option(r.br_sum);
        br_n.append_option(r.br_n);
        tb_sum.append_option(r.tb_sum);
        tb_n.append_option(r.tb_n);
        bl_sum.append_option(r.bl_sum);
        bl_n.append_option(r.bl_n);
        mtp_sum.append_option(r.mtp_sum);
        mtp_n.append_option(r.mtp_n);
        v_br_sum.append_option(r.v_br_sum);
        v_br_n.append_option(r.v_br_n);
        v_tb_sum.append_option(r.v_tb_sum);
        v_tb_n.append_option(r.v_tb_n);
        v_bl_sum.append_option(r.v_bl_sum);
        v_bl_n.append_option(r.v_bl_n);
        util_sum.append_option(r.util_sum);
        util_n.append_option(r.util_n);
        util_all_sum.append_option(r.util_all_sum);
        util_all_n.append_option(r.util_all_n);
        startup_requests.append_option(r.startup_requests);
        su_bl_sum.append_option(r.su_bl_sum);
        su_bl_n.append_option(r.su_bl_n);
        su_ttfb_sum.append_option(r.su_ttfb_sum);
        su_ttfb_n.append_option(r.su_ttfb_n);
        v_bl_hist.append_option(r.v_bl_hist.as_deref());
        bl_hist.append_option(r.bl_hist.as_deref());
        v_mtp_hist.append_option(r.v_mtp_hist.as_deref());
        mtp_hist.append_option(r.mtp_hist.as_deref());
        min_ts.append_option(r.min_ts);
        max_ts.append_option(r.max_ts);
        first_ts.append_option(r.first_ts);
        last_ts.append_option(r.last_ts);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(kind.finish()),
        Arc::new(bucket.finish()),
        Arc::new(key.finish()),
        Arc::new(label.finish()),
        Arc::new(truncated.finish()),
        Arc::new(reqs.finish()),
        Arc::new(sid_hll.finish()),
        Arc::new(rebuffer_events.finish()),
        Arc::new(rebuffer_sid_hll.finish()),
        Arc::new(has_bs.finish()),
        Arc::new(has_su.finish()),
        Arc::new(has_ot.finish()),
        Arc::new(has_bl.finish()),
        Arc::new(has_br.finish()),
        Arc::new(has_mtp.finish()),
        Arc::new(has_cid.finish()),
        Arc::new(br_sum.finish()),
        Arc::new(br_n.finish()),
        Arc::new(tb_sum.finish()),
        Arc::new(tb_n.finish()),
        Arc::new(bl_sum.finish()),
        Arc::new(bl_n.finish()),
        Arc::new(mtp_sum.finish()),
        Arc::new(mtp_n.finish()),
        Arc::new(v_br_sum.finish()),
        Arc::new(v_br_n.finish()),
        Arc::new(v_tb_sum.finish()),
        Arc::new(v_tb_n.finish()),
        Arc::new(v_bl_sum.finish()),
        Arc::new(v_bl_n.finish()),
        Arc::new(util_sum.finish()),
        Arc::new(util_n.finish()),
        Arc::new(util_all_sum.finish()),
        Arc::new(util_all_n.finish()),
        Arc::new(startup_requests.finish()),
        Arc::new(su_bl_sum.finish()),
        Arc::new(su_bl_n.finish()),
        Arc::new(su_ttfb_sum.finish()),
        Arc::new(su_ttfb_n.finish()),
        Arc::new(v_bl_hist.finish()),
        Arc::new(bl_hist.finish()),
        Arc::new(v_mtp_hist.finish()),
        Arc::new(mtp_hist.finish()),
        Arc::new(min_ts.finish()),
        Arc::new(max_ts.finish()),
        Arc::new(first_ts.finish()),
        Arc::new(last_ts.finish()),
    ];
    RecordBatch::try_new(schema(), columns).map_err(|e| format!("cmcd_summary batch: {e}"))
}

/// Summed integer columns of a merged row, after `reqs`.
const INT_SUMS: [&str; 20] = [
    "rebuffer_events",
    "has_bs",
    "has_su",
    "has_ot",
    "has_bl",
    "has_br",
    "has_mtp",
    "has_cid",
    "br_n",
    "tb_n",
    "bl_n",
    "mtp_n",
    "v_br_n",
    "v_tb_n",
    "v_bl_n",
    "util_n",
    "util_all_n",
    "startup_requests",
    "su_bl_n",
    "su_ttfb_n",
];
const FLOAT_SUMS: [&str; 11] = [
    "br_sum",
    "tb_sum",
    "bl_sum",
    "mtp_sum",
    "v_br_sum",
    "v_tb_sum",
    "v_bl_sum",
    "util_sum",
    "util_all_sum",
    "su_bl_sum",
    "su_ttfb_sum",
];
const HLLS: [&str; 2] = ["sid_hll", "rebuffer_sid_hll"];
const HISTS: [&str; 4] = ["v_bl_hist", "bl_hist", "v_mtp_hist", "mtp_hist"];
const MINS: [&str; 2] = ["min_ts", "first_ts"];
const MAXES: [&str; 2] = ["max_ts", "last_ts"];

/// Memory bounds of a bundle accumulator per row kind: far above what an hour of window
/// summaries keeps (`SESSION_K`, `CONTENT_K`, ... per window), so they bind only on a runaway
/// hour or day. Past one, the rows with the fewest requests go and `totals.truncated` is set.
pub const BUNDLE_SESSION_CAP: usize = 100_000;
const BUNDLE_ROW_CAP: usize = 20_000;

/// One merged non-session row.
#[derive(Default, Clone)]
struct MergeRow {
    label: Option<String>,
    reqs: Option<i64>,
    ints: [Option<i64>; INT_SUMS.len()],
    floats: [Option<f64>; FLOAT_SUMS.len()],
    hlls: [Option<Hll>; HLLS.len()],
    hists: [Option<Hist>; HISTS.len()],
    mins: [Option<i64>; MINS.len()],
    maxes: [Option<i64>; MAXES.len()],
}

/// `(kind, bucket, key)` of a merged row.
type RowKey = (String, Option<i64>, Option<String>);

/// Lossless merge of CMCD window summaries into an hour or day bundle, in the window schema:
/// a reader merging the bundle gets what it gets merging its windows, since every metric it
/// reads is a sum, a min/max, a sketch union or a histogram sum per `(kind, bucket, key)`, and
/// `asn` labels keep the smallest non-empty one as readers do. `totals` rows keep `truncated`
/// set when any source (or a cap here) dropped rows of that kind.
#[derive(Default, Clone)]
pub struct BundleAcc {
    rows: BTreeMap<RowKey, MergeRow>,
    /// `(reqs, first_ts, last_ts)` per session id.
    sessions: BTreeMap<String, (i64, Option<i64>, Option<i64>)>,
    /// `(truncated, reqs)` per `totals` key.
    totals: BTreeMap<String, (bool, i64)>,
}

fn add<T: std::ops::Add<Output = T> + Copy>(into: &mut Option<T>, v: Option<T>) {
    if let Some(v) = v {
        *into = Some(into.map_or(v, |a| a + v));
    }
}

struct Read<'a> {
    batch: &'a RecordBatch,
}

impl<'a> Read<'a> {
    fn col<T: 'static>(&self, name: &str) -> Option<&'a T> {
        self.batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<T>()
    }
    fn i(&self, name: &str) -> Option<&'a Int64Array> {
        self.col(name)
    }
}

fn opt_i(col: Option<&Int64Array>, i: usize) -> Option<i64> {
    col.and_then(|a| a.is_valid(i).then(|| a.value(i)))
}

fn opt_f(col: Option<&Float64Array>, i: usize) -> Option<f64> {
    col.and_then(|a| a.is_valid(i).then(|| a.value(i)))
}

fn opt_bin(col: Option<&arrow_array::BinaryArray>, i: usize) -> Option<&[u8]> {
    col.and_then(|a| a.is_valid(i).then(|| a.value(i)))
}

impl BundleAcc {
    /// Folds one window summary or bundle.
    pub fn fold(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .map_err(|_| "cmcd_summary_unreadable")?
            .build()
            .map_err(|_| "cmcd_summary_unreadable")?;
        for batch in reader {
            let batch = batch.map_err(|_| "cmcd_summary_unreadable")?;
            self.fold_batch(&batch)?;
        }
        Ok(())
    }

    fn fold_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let r = Read { batch };
        let kind = r
            .col::<StringArray>("kind")
            .ok_or("cmcd_summary_unreadable")?;
        let key = r.col::<StringArray>("key");
        let label = r.col::<StringArray>("label");
        let truncated = r.col::<BooleanArray>("truncated");
        let bucket = r.i("bucket");
        let reqs = r.i("reqs");
        let ints: Vec<_> = INT_SUMS.iter().map(|c| r.i(c)).collect();
        let floats: Vec<_> = FLOAT_SUMS
            .iter()
            .map(|c| r.col::<Float64Array>(c))
            .collect();
        let hlls: Vec<_> = HLLS
            .iter()
            .map(|c| r.col::<arrow_array::BinaryArray>(c))
            .collect();
        let hists: Vec<_> = HISTS
            .iter()
            .map(|c| r.col::<arrow_array::BinaryArray>(c))
            .collect();
        let mins: Vec<_> = MINS.iter().map(|c| r.i(c)).collect();
        let maxes: Vec<_> = MAXES.iter().map(|c| r.i(c)).collect();
        for i in 0..batch.num_rows() {
            if !kind.is_valid(i) {
                continue;
            }
            let k = kind.value(i);
            let key_at = get_str(key, i);
            match k {
                "totals" => {
                    let Some(name) = key_at else { continue };
                    let t = self.totals.entry(name.to_owned()).or_default();
                    t.0 |= truncated.is_some_and(|a| a.is_valid(i) && a.value(i));
                    t.1 = t.1.max(opt_i(reqs, i).unwrap_or(0));
                }
                "session" => {
                    let Some(sid) = key_at else { continue };
                    let s = self
                        .sessions
                        .entry(sid.to_owned())
                        .or_insert((0, None, None));
                    s.0 += opt_i(reqs, i).unwrap_or(0);
                    if let Some(f) = opt_i(mins[1], i) {
                        s.1 = Some(s.1.map_or(f, |a| a.min(f)));
                    }
                    if let Some(l) = opt_i(maxes[1], i) {
                        s.2 = Some(s.2.map_or(l, |a| a.max(l)));
                    }
                }
                _ => {
                    let row = self
                        .rows
                        .entry((k.to_owned(), opt_i(bucket, i), key_at.map(str::to_owned)))
                        .or_default();
                    if let Some(l) = get_str(label, i) {
                        let better = match row.label.as_deref() {
                            None => true,
                            Some(cur) => !l.is_empty() && (cur.is_empty() || l < cur),
                        };
                        if better {
                            row.label = Some(l.to_owned());
                        }
                    }
                    add(&mut row.reqs, opt_i(reqs, i));
                    for (n, col) in ints.iter().enumerate() {
                        add(&mut row.ints[n], opt_i(*col, i));
                    }
                    for (n, col) in floats.iter().enumerate() {
                        add(&mut row.floats[n], opt_f(*col, i));
                    }
                    for (n, col) in hlls.iter().enumerate() {
                        if let Some(blob) = opt_bin(*col, i) {
                            let hll = row.hlls[n].get_or_insert_with(Hll::default);
                            if !hll.merge_bytes(blob) {
                                return Err("cmcd_summary_sketch_invalid".into());
                            }
                        }
                    }
                    for (n, col) in hists.iter().enumerate() {
                        if let Some(blob) = opt_bin(*col, i) {
                            let hist = row.hists[n].get_or_insert_with(Hist::default);
                            if !hist.merge_bytes(blob) {
                                return Err("cmcd_summary_hist_invalid".into());
                            }
                        }
                    }
                    for (n, col) in mins.iter().enumerate() {
                        if let Some(v) = opt_i(*col, i) {
                            row.mins[n] = Some(row.mins[n].map_or(v, |a| a.min(v)));
                        }
                    }
                    for (n, col) in maxes.iter().enumerate() {
                        if let Some(v) = opt_i(*col, i) {
                            row.maxes[n] = Some(row.maxes[n].map_or(v, |a| a.max(v)));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies the memory caps: per kind, the rows with the most requests (then the smallest key)
    /// stay. Buckets are bounded by time and never cut.
    pub fn prune(&mut self) {
        if self.sessions.len() > BUNDLE_SESSION_CAP {
            let mut ranked: Vec<(Reverse<i64>, String)> = self
                .sessions
                .iter()
                .map(|(k, s)| (Reverse(s.0), k.clone()))
                .collect();
            ranked.select_nth_unstable(BUNDLE_SESSION_CAP);
            for (_, k) in &ranked[BUNDLE_SESSION_CAP..] {
                self.sessions.remove(k);
            }
            self.totals.entry("session".into()).or_default().0 = true;
        }
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for (kind, _, _) in self.rows.keys() {
            *counts.entry(kind.as_str()).or_default() += 1;
        }
        let over: Vec<String> = counts
            .into_iter()
            .filter(|(kind, n)| *kind != "bucket" && *n > BUNDLE_ROW_CAP)
            .map(|(kind, _)| kind.to_owned())
            .collect();
        for kind in over {
            let mut ranked: Vec<(Reverse<i64>, RowKey)> = self
                .rows
                .iter()
                .filter(|(k, _)| k.0 == kind)
                .map(|(k, row)| (Reverse(row.reqs.unwrap_or(0)), k.clone()))
                .collect();
            ranked.select_nth_unstable(BUNDLE_ROW_CAP);
            for (_, k) in &ranked[BUNDLE_ROW_CAP..] {
                self.rows.remove(k);
            }
            self.totals.entry(kind).or_default().0 = true;
        }
    }

    /// The merged rows as a CMCD summary Parquet file.
    pub fn write(&self) -> Result<Vec<u8>, String> {
        let mut kept: BTreeMap<&str, i64> = BTreeMap::new();
        for (kind, _, _) in self.rows.keys() {
            *kept.entry(kind.as_str()).or_default() += 1;
        }
        kept.insert("session", self.sessions.len() as i64);
        let mut out = Vec::with_capacity(self.rows.len() + self.sessions.len() + 8);
        for (name, (truncated, reqs)) in &self.totals {
            out.push(RowOut {
                kind: "totals".into(),
                key: Some(name.clone()),
                truncated: Some(*truncated),
                reqs: Some((*reqs).max(kept.get(name.as_str()).copied().unwrap_or(0))),
                ..Default::default()
            });
        }
        for (sid, (reqs, first, last)) in &self.sessions {
            out.push(RowOut {
                kind: "session".into(),
                key: Some(sid.clone()),
                reqs: Some(*reqs),
                first_ts: *first,
                last_ts: *last,
                ..Default::default()
            });
        }
        for ((kind, bucket, key), row) in &self.rows {
            let mut o = RowOut {
                kind: kind.clone(),
                bucket: *bucket,
                key: key.clone(),
                label: row.label.clone(),
                reqs: row.reqs,
                ..Default::default()
            };
            let [rebuffer_events, has_bs, has_su, has_ot, has_bl, has_br, has_mtp, has_cid, br_n, tb_n, bl_n, mtp_n, v_br_n, v_tb_n, v_bl_n, util_n, util_all_n, startup_requests, su_bl_n, su_ttfb_n] =
                row.ints;
            (o.rebuffer_events, o.has_bs, o.has_su, o.has_ot, o.has_bl) =
                (rebuffer_events, has_bs, has_su, has_ot, has_bl);
            (o.has_br, o.has_mtp, o.has_cid) = (has_br, has_mtp, has_cid);
            (o.br_n, o.tb_n, o.bl_n, o.mtp_n) = (br_n, tb_n, bl_n, mtp_n);
            (o.v_br_n, o.v_tb_n, o.v_bl_n) = (v_br_n, v_tb_n, v_bl_n);
            (o.util_n, o.util_all_n, o.startup_requests) = (util_n, util_all_n, startup_requests);
            (o.su_bl_n, o.su_ttfb_n) = (su_bl_n, su_ttfb_n);
            let [br_sum, tb_sum, bl_sum, mtp_sum, v_br_sum, v_tb_sum, v_bl_sum, util_sum, util_all_sum, su_bl_sum, su_ttfb_sum] =
                row.floats;
            (o.br_sum, o.tb_sum, o.bl_sum, o.mtp_sum) = (br_sum, tb_sum, bl_sum, mtp_sum);
            (o.v_br_sum, o.v_tb_sum, o.v_bl_sum) = (v_br_sum, v_tb_sum, v_bl_sum);
            (o.util_sum, o.util_all_sum) = (util_sum, util_all_sum);
            (o.su_bl_sum, o.su_ttfb_sum) = (su_bl_sum, su_ttfb_sum);
            let hll = |h: &Option<Hll>| h.as_ref().and_then(hll_opt);
            let hist = |h: &Option<Hist>| h.as_ref().and_then(hist_opt);
            (o.sid_hll, o.rebuffer_sid_hll) = (hll(&row.hlls[0]), hll(&row.hlls[1]));
            (o.v_bl_hist, o.bl_hist) = (hist(&row.hists[0]), hist(&row.hists[1]));
            (o.v_mtp_hist, o.mtp_hist) = (hist(&row.hists[2]), hist(&row.hists[3]));
            (o.min_ts, o.first_ts) = (row.mins[0], row.mins[1]);
            (o.max_ts, o.last_ts) = (row.maxes[0], row.maxes[1]);
            out.push(o);
        }
        let batch = build_batch(&out)?;
        convert::encode_record_batch(
            schema(),
            &batch,
            vec![KeyValue::new(
                "flp_cmcd_summary".to_string(),
                CMCD_SUMMARY_VERSION.to_string(),
            )],
        )
    }
}

/// Merges CMCD window summaries (or bundles) the way `bundle::cmcd_summary_bundle` does,
/// folding in file order with the caps applied after each fold.
pub fn merge_windows(sources: &[&[u8]]) -> Result<Vec<u8>, String> {
    let mut acc = BundleAcc::default();
    for source in sources {
        acc.fold(source.to_vec())?;
        acc.prune();
    }
    acc.write()
}
