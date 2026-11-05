//! Per-window summary Parquet, written by the ingest worker next to each window file.
//!
//! One long-format file per window, rows tagged by `bundle`. Shapes and definitions mirror the
//! fla v3 hourly rollups (`backend/core/rollups/` at 2c75f058): `time_series`, `overview`,
//! `all_fields` (+ `field_totals`), `all_fields_ip`, `sessions` and additive `dims`. Every value
//! is an additive sum or count, a min/max, or an HLL sketch, so windows merge into hours and days.

use super::convert::{self, FastMap};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    },
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use md5::{Digest, Md5};
use parquet::file::metadata::KeyValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub const SUMMARY_VERSION: &str = "2";
/// Values kept per field per window in `all_fields`.
pub const TOP_K: usize = 2000;
/// Values per field that also get an IP-spread sketch (fla's hourly `TOP_K`).
pub const IP_TOP_K: usize = 500;
/// fla's `IP_SAMPLE_CAP`: `sample_capped` is set at or above this many distinct IPs.
pub const IP_SAMPLE_CAP: u64 = 5000;
pub const SESSION_K: usize = 2000;
pub const DIM_K: usize = 2000;
pub const HLL_VERSION: u8 = 1;
pub const HLL_P: u8 = 8;
pub(super) const HLL_M: usize = 1 << HLL_P;

/// Dashboard card fields (`web/lib/dashboard/cards.ts` CARD_FIELDS) plus origin fields and the
/// fla virtual field `waf_sig_ind`.
pub const FIELDS: &[&str] = &[
    "ip",
    "asn",
    "host",
    "url",
    "method",
    "status",
    "cache",
    "proto",
    "ua",
    "referer",
    "resp_header_content_encoding",
    "ttl",
    "age",
    "hits",
    "digest",
    "city",
    "region",
    "country",
    "metro",
    "transport",
    "c_speed",
    "c_type",
    "pop",
    "backend",
    "edge",
    "server_region",
    "tls",
    "is_ipv6",
    "conn_requests",
    "waf",
    "waf_resp",
    "waf_ms",
    "p_type",
    "p_desc",
    "ja3",
    "ja4",
    "tls_ciphers_sha",
    "io_input_format",
    "io_output_format",
    "oip",
    "ost",
    "waf_sig_ind",
];

pub const DIMS: &[&str] = &["pop", "oip", "edge", "backend", "country", "region", "asn"];

/// `{scope}processed/summary/{table}/`: a tree apart from the window files, so their readers'
/// LIST caps and `window=` parsing never see summaries.
pub fn summary_prefix(scope: &str, table: &str) -> String {
    format!("{scope}processed/summary/{table}/")
}

pub fn summary_key(prefix: &str, start: i64, end: i64, run_id: &str, rows: u64) -> String {
    format!(
        "{prefix}{}summary={start}-{end}-{run_id}-n{rows}.parquet",
        super::hour_dir(start)
    )
}

/// fla `backend/utils/hll.py`: md5 of the UTF-8 item, first 8 bytes little-endian.
pub fn hll_hash(item: &str) -> u64 {
    let digest = Md5::digest(item.as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("md5 is 16 bytes"))
}

#[derive(Clone)]
pub struct Hll([u8; HLL_M]);

impl Default for Hll {
    fn default() -> Self {
        Self([0; HLL_M])
    }
}

impl Hll {
    #[inline]
    fn bucket_rank(h: u64) -> (u8, u8) {
        let width = 64 - u32::from(HLL_P);
        let bucket = (h >> width) as u8;
        let observed = h & ((1u64 << width) - 1);
        let bits = 64 - observed.leading_zeros();
        let rank = if bits <= 1 { width } else { width + 1 - bits } as u8;
        (bucket, rank)
    }

    #[inline]
    fn add_bucket_rank(&mut self, bucket: u8, rank: u8) {
        let b = usize::from(bucket);
        if rank > self.0[b] {
            self.0[b] = rank;
        }
    }

    pub fn add_hash(&mut self, h: u64) {
        let (bucket, rank) = Self::bucket_rank(h);
        self.add_bucket_rank(bucket, rank);
    }

    pub fn merge(&mut self, other: &Hll) {
        for (a, b) in self.0.iter_mut().zip(other.0.iter()) {
            *a = (*a).max(*b);
        }
    }

    pub fn merge_bytes(&mut self, blob: &[u8]) {
        if blob.len() == 2 + HLL_M && blob[0] == HLL_VERSION && blob[1] == HLL_P {
            for (a, &b) in self.0.iter_mut().zip(&blob[2..]) {
                *a = (*a).max(b);
            }
        }
    }

    pub(super) fn registers(&self) -> &[u8; HLL_M] {
        &self.0
    }

    pub(super) fn from_registers(registers: [u8; HLL_M]) -> Self {
        Self(registers)
    }

    /// `[version, p, registers...]`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + HLL_M);
        out.extend([HLL_VERSION, HLL_P]);
        out.extend_from_slice(&self.0);
        out
    }

    pub fn from_bytes(blob: &[u8]) -> Option<Self> {
        if blob.len() != 2 + HLL_M || blob[0] != HLL_VERSION || blob[1] != HLL_P {
            return None;
        }
        let mut registers = [0; HLL_M];
        registers.copy_from_slice(&blob[2..]);
        Some(Self(registers))
    }

    pub fn count(&self) -> u64 {
        let m = HLL_M as f64;
        let sum: f64 = self.0.iter().map(|&r| 2f64.powi(-i32::from(r))).sum();
        let mut estimate = 0.7213 / (1.0 + 1.079 / m) * m * m / sum;
        if estimate <= 2.5 * m {
            let zeros = self.0.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                estimate = m * (m / zeros as f64).ln();
            }
        }
        let two64 = 2f64.powi(64);
        if estimate > two64 / 30.0 {
            estimate = -two64 * (1.0 - estimate / two64).ln();
        }
        compute_common::num::round_ties_even(estimate) as u64
    }
}

#[derive(Clone, Copy)]
pub(super) enum Ty {
    Utf8,
    I64,
    F64,
    Bool,
    Ts,
    Bin,
}

macro_rules! columns {
    ($($variant:ident = ($name:literal, $ty:ident)),+ $(,)?) => {
        #[derive(Clone, Copy)]
        #[repr(usize)]
        pub(super) enum C { $($variant),+ }
        pub(super) const COLS: &[(&str, Ty)] = &[$(($name, Ty::$ty)),+];
    };
}

columns! {
    Bundle = ("bundle", Utf8),
    Bucket = ("bucket", Ts),
    FieldName = ("field", Utf8),
    Value = ("value", Utf8),
    Count = ("count", I64),
    Truncated = ("truncated", Bool),
    Requests = ("requests", I64),
    Status4xx = ("status_4xx", I64),
    Status5xx = ("status_5xx", I64),
    Hits = ("hits", I64),
    CacheTotal = ("cache_total", I64),
    RespBytesSum = ("resp_bytes_sum", I64),
    TtfbSum = ("ttfb_sum", F64),
    TtfbCount = ("ttfb_count", I64),
    HitRequests = ("hit_requests", I64),
    MissRequests = ("miss_requests", I64),
    PassRequests = ("pass_requests", I64),
    SynthRequests = ("synth_requests", I64),
    OriginRequests = ("origin_requests", I64),
    BandwidthSaved = ("bandwidth_saved_bytes", I64),
    TotalBandwidth = ("total_bandwidth_bytes", I64),
    ShieldHits = ("shield_hit_requests", I64),
    ShieldTotal = ("shield_total_requests", I64),
    ThreatsBlocked = ("threats_blocked", I64),
    HitElapsedSum = ("hit_elapsed_sum", F64),
    HitElapsedCount = ("hit_elapsed_count", I64),
    MissElapsedSum = ("miss_elapsed_sum", F64),
    MissElapsedCount = ("miss_elapsed_count", I64),
    HitTtfbSum = ("hit_ttfb_sum", F64),
    HitTtfbCount = ("hit_ttfb_count", I64),
    MissTtfbSum = ("miss_ttfb_sum", F64),
    MissTtfbCount = ("miss_ttfb_count", I64),
    WaterfallCount = ("waterfall_count", I64),
    EdgeProcessingSum = ("edge_processing_sum", F64),
    OriginWaitSum = ("origin_wait_sum", F64),
    OriginDownloadSum = ("origin_download_sum", F64),
    ClientDownloadSum = ("client_download_sum", F64),
    IpSketch = ("ip_sketch", Bin),
    IpCountObserved = ("ip_count_observed", I64),
    SampleCapped = ("sample_capped", Bool),
    Ip = ("ip", Utf8),
    Ja4 = ("ja4", Utf8),
    FirstTs = ("first_ts", Ts),
    LastTs = ("last_ts", Ts),
    ReqCount = ("req_count", I64),
    Country = ("country", Utf8),
    Asn = ("asn", I64),
    Reqs4xx = ("reqs_4xx", I64),
    Reqs5xx = ("reqs_5xx", I64),
    TotalBytes = ("total_bytes", I64),
    RttSum = ("rtt_sum", I64),
    RttCount = ("rtt_count", I64),
    EdgeCount = ("edge_count", I64),
    ShieldCount = ("shield_count", I64),
    UaMin = ("ua_min", Utf8),
    EdgeSidMax = ("edge_sid_max", Utf8),
    CmcdCount = ("cmcd_count", I64),
    Errors = ("errors", I64),
    CacheHits = ("cache_hits", I64),
    LatUsSum = ("lat_us_sum", F64),
    LatUsCount = ("lat_us_count", I64),
    Ost5xxCount = ("ost_5xx_count", I64),
    OstTotalCount = ("ost_total_count", I64),
    PlossSum = ("ploss_sum", F64),
    PlossCount = ("ploss_count", I64),
}

pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(
                COLS.iter()
                    .map(|(name, ty)| {
                        let data_type = match ty {
                            Ty::Utf8 => DataType::Utf8,
                            Ty::I64 => DataType::Int64,
                            Ty::F64 => DataType::Float64,
                            Ty::Bool => DataType::Boolean,
                            Ty::Ts => {
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                            }
                            Ty::Bin => DataType::Binary,
                        };
                        Field::new(*name, data_type, *name != "bundle")
                    })
                    .collect::<Vec<_>>(),
            ))
        })
        .clone()
}

#[derive(Clone, Copy, Default)]
pub(super) enum V<'a> {
    #[default]
    Null,
    S(&'a str),
    I(i64),
    F(f64),
    B(bool),
    Bin(&'a [u8]),
}

enum Col {
    Utf8(StringBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Ts(TimestampMillisecondBuilder),
    Bin(BinaryBuilder),
}

pub(super) struct Out {
    cols: Vec<Col>,
    /// Each column's length; a column behind `rows` owes nulls, appended in one run before its
    /// next value or at finish.
    lens: [usize; NCOLS],
    rows: usize,
}

const _: () = assert!(NCOLS <= 128, "Out::push tracks columns in a u128");

impl Col {
    fn append_nulls(&mut self, n: usize) {
        match self {
            Col::Utf8(b) => b.append_nulls(n),
            Col::I64(b) => b.append_nulls(n),
            Col::F64(b) => b.append_nulls(n),
            Col::Bool(b) => b.append_nulls(n),
            Col::Ts(b) => b.append_nulls(n),
            Col::Bin(b) => b.append_nulls(n),
        }
    }
}

const NCOLS: usize = COLS.len();

impl Out {
    pub(super) fn new() -> Self {
        Self {
            cols: COLS
                .iter()
                .map(|(_, ty)| match ty {
                    Ty::Utf8 => Col::Utf8(StringBuilder::new()),
                    Ty::I64 => Col::I64(Int64Builder::new()),
                    Ty::F64 => Col::F64(Float64Builder::new()),
                    Ty::Bool => Col::Bool(BooleanBuilder::new()),
                    Ty::Ts => Col::Ts(TimestampMillisecondBuilder::new().with_timezone("UTC")),
                    Ty::Bin => Col::Bin(BinaryBuilder::new()),
                })
                .collect(),
            lens: [0; NCOLS],
            rows: 0,
        }
    }

    /// Appends one row; a column given twice keeps its last value, and a missing or mistyped value
    /// is null.
    pub(super) fn push(&mut self, values: &[(C, V<'_>)]) {
        let mut seen = 0u128;
        for &(c, value) in values.iter().rev() {
            let c = c as usize;
            if seen & (1 << c) != 0 {
                continue;
            }
            seen |= 1 << c;
            let owed = self.rows - self.lens[c];
            let pushed = match (&mut self.cols[c], value) {
                (Col::Utf8(b), V::S(s)) => {
                    b.append_nulls(owed);
                    b.append_value(s);
                    true
                }
                (Col::I64(b), V::I(i)) => {
                    b.append_nulls(owed);
                    b.append_value(i);
                    true
                }
                (Col::F64(b), V::F(f)) => {
                    b.append_nulls(owed);
                    b.append_value(f);
                    true
                }
                (Col::Bool(b), V::B(x)) => {
                    b.append_nulls(owed);
                    b.append_value(x);
                    true
                }
                (Col::Ts(b), V::I(i)) => {
                    b.append_nulls(owed);
                    b.append_value(i);
                    true
                }
                (Col::Bin(b), V::Bin(x)) => {
                    b.append_nulls(owed);
                    b.append_value(x);
                    true
                }
                _ => false,
            };
            if pushed {
                self.lens[c] = self.rows + 1;
            }
        }
        self.rows += 1;
    }

    pub(super) fn finish(mut self) -> Result<RecordBatch, String> {
        for (col, len) in self.cols.iter_mut().zip(self.lens) {
            col.append_nulls(self.rows - len);
        }
        let arrays: Vec<ArrayRef> = self
            .cols
            .iter_mut()
            .map(|col| -> ArrayRef {
                match col {
                    Col::Utf8(b) => Arc::new(b.finish()),
                    Col::I64(b) => Arc::new(b.finish()),
                    Col::F64(b) => Arc::new(b.finish()),
                    Col::Bool(b) => Arc::new(b.finish()),
                    Col::Ts(b) => Arc::new(b.finish()),
                    Col::Bin(b) => Arc::new(b.finish()),
                }
            })
            .collect();
        RecordBatch::try_new(schema(), arrays).map_err(|_| "summary_batch".into())
    }
}

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Option<&'a T> {
    batch.column_by_name(name)?.as_any().downcast_ref::<T>()
}

fn get_s(array: Option<&StringArray>, i: usize) -> Option<&str> {
    array.filter(|a| a.is_valid(i)).map(|a| a.value(i))
}

fn get_i(array: Option<&Int64Array>, i: usize) -> Option<i64> {
    array.filter(|a| a.is_valid(i)).map(|a| a.value(i))
}

fn get_f(array: Option<&Float64Array>, i: usize) -> Option<f64> {
    array.filter(|a| a.is_valid(i)).map(|a| a.value(i))
}

fn get_b(array: Option<&BooleanArray>, i: usize) -> Option<bool> {
    array.filter(|a| a.is_valid(i)).map(|a| a.value(i))
}

const NO_IP: u32 = u32::MAX;

/// Window-wide dictionary of distinct IP hashes (`u64`) mapped to dense `u32` IDs and their
/// precomputed HLL `(bucket, rank)` pairs, avoiding repeated MD5 hashing and 8-byte sorting across
/// the 42 `all_fields` passes.
#[derive(Default)]
struct IpTable<'a> {
    by_str: FastMap<&'a str, u32>,
    by_hash: FastMap<u64, u32>,
    entries: Vec<(u8, u8)>,
}

impl<'a> IpTable<'a> {
    fn intern(&mut self, ip: &'a str) -> u32 {
        if let Some(&id) = self.by_str.get(ip) {
            return id;
        }
        let h = hll_hash(ip);
        let id = *self.by_hash.entry(h).or_insert_with(|| {
            let next = self.entries.len() as u32;
            self.entries.push(Hll::bucket_rank(h));
            next
        });
        self.by_str.insert(ip, id);
        id
    }

    fn sketch_all(&self) -> (Hll, u64) {
        let mut hll = Hll::default();
        for &(bucket, rank) in &self.entries {
            hll.add_bucket_rank(bucket, rank);
        }
        (hll, self.entries.len() as u64)
    }
}

/// The columns one batch contributes to the summary.
struct Rows<'a> {
    ts: &'a TimestampMillisecondArray,
    live_rows: Vec<usize>,
    ip: Option<&'a StringArray>,
    ip_id: Vec<u32>,
    status: Option<&'a Int64Array>,
    cache: Option<&'a StringArray>,
    resp_bytes: Option<&'a Int64Array>,
    ttfb: Option<&'a Float64Array>,
    elapsed: Option<&'a Int64Array>,
    ottfb: Option<&'a Int64Array>,
    ottlb: Option<&'a Int64Array>,
    waf: Option<&'a BooleanArray>,
    waf_resp: Option<&'a Int64Array>,
    ja4: Option<&'a StringArray>,
    country: Option<&'a StringArray>,
    asn: Option<&'a Int64Array>,
    tcp_rtt: Option<&'a Int64Array>,
    edge: Option<&'a BooleanArray>,
    ua: Option<&'a StringArray>,
    ost: Option<&'a Int64Array>,
    ploss: Option<&'a Float64Array>,
    batch: &'a RecordBatch,
}

impl<'a> Rows<'a> {
    fn new(batch: &'a RecordBatch, ip_table: &mut IpTable<'a>) -> Option<Self> {
        let ts: &TimestampMillisecondArray = col(batch, "timestamp")?;
        let live_rows: Vec<usize> = (0..ts.len()).filter(|&i| ts.is_valid(i)).collect();
        let ip: Option<&StringArray> = col(batch, "ip");
        let mut ip_id = vec![NO_IP; batch.num_rows()];
        if let Some(ip_arr) = ip {
            for &i in &live_rows {
                if ip_arr.is_valid(i) {
                    ip_id[i] = ip_table.intern(ip_arr.value(i));
                }
            }
        }
        Some(Self {
            ts,
            live_rows,
            ip,
            ip_id,
            status: col(batch, "status"),
            cache: col(batch, "cache"),
            resp_bytes: col(batch, "resp_bytes"),
            ttfb: col(batch, "ttfb"),
            elapsed: col(batch, "elapsed"),
            ottfb: col(batch, "ottfb"),
            ottlb: col(batch, "ottlb"),
            waf: col(batch, "waf"),
            waf_resp: col(batch, "waf_resp"),
            ja4: col(batch, "ja4"),
            country: col(batch, "country"),
            asn: col(batch, "asn"),
            tcp_rtt: col(batch, "tcp_rtt"),
            edge: col(batch, "edge"),
            ua: col(batch, "ua"),
            ost: col(batch, "ost"),
            ploss: col(batch, "ploss"),
            batch,
        })
    }

    /// Rows fla's `WHERE timestamp >= start AND timestamp < end` keeps: a null timestamp is out.
    #[inline]
    fn live(&self) -> impl Iterator<Item = usize> + '_ {
        self.live_rows.iter().copied()
    }
}

/// A grouped value. Strings borrow from the batch; numbers and booleans render at write time.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Key<'a> {
    S(&'a str),
    I(i64),
    B(bool),
    /// `f64` bits, with -0.0 stored as 0.0.
    F(u64),
}

impl<'a> Key<'a> {
    fn render(&self) -> Cow<'a, str> {
        match self {
            Key::S(s) => Cow::Borrowed(s),
            Key::I(i) => Cow::Owned(i.to_string()),
            Key::B(b) => Cow::Owned(b.to_string()),
            Key::F(bits) => Cow::Owned(f64::from_bits(*bits).to_string()),
        }
    }
}

/// Sorts `items` by `order` and keeps the first `keep`, sorting only those.
fn rank<T>(items: &mut Vec<T>, order: impl Fn(&T, &T) -> std::cmp::Ordering, keep: usize) {
    if keep == 0 {
        items.clear();
        return;
    }
    if items.len() > keep {
        items.select_nth_unstable_by(keep - 1, &order);
        items.truncate(keep);
    }
    items.sort_unstable_by(order);
}

/// The `keep` most counted values, ties by rendered value ascending.
/// Drains `map`, which keeps its capacity for the next field.
fn top<'a, T>(
    map: &mut FastMap<Key<'a>, T>,
    count: impl Fn(&T) -> i64,
    keep: usize,
) -> Vec<(Cow<'a, str>, T)> {
    let mut ranked: Vec<_> = map.drain().map(|(k, v)| (k.render(), v)).collect();
    rank(
        &mut ranked,
        |a, b| count(&b.1).cmp(&count(&a.1)).then_with(|| a.0.cmp(&b.0)),
        keep,
    );
    ranked
}

/// One column resolved and downcast once per batch outside the row loop. All-null columns resolve
/// to `None` so callers skip scanning the batch entirely.
enum FieldCol<'a> {
    WafSig(&'a StringArray),
    S(&'a StringArray),
    I(&'a Int64Array),
    B(&'a BooleanArray),
    F(&'a Float64Array),
}

impl<'a> FieldCol<'a> {
    fn resolve(batch: &'a RecordBatch, field: &str) -> Option<Self> {
        if field == "waf_sig_ind" {
            let a = col::<StringArray>(batch, "waf_sig")?;
            return (a.null_count() < a.len()).then_some(Self::WafSig(a));
        }
        let array = batch.column_by_name(field)?;
        if array.null_count() >= array.len() {
            return None;
        }
        let any = array.as_any();
        if let Some(a) = any.downcast_ref::<StringArray>() {
            Some(Self::S(a))
        } else if let Some(a) = any.downcast_ref::<Int64Array>() {
            Some(Self::I(a))
        } else if let Some(a) = any.downcast_ref::<BooleanArray>() {
            Some(Self::B(a))
        } else {
            any.downcast_ref::<Float64Array>().map(Self::F)
        }
    }

    /// Emits each key of row `i` as the dashboard counts them: NULL is skipped, '' is kept unless
    /// `skip_empty` (fla's dims), and floats keep their exact value (`f64` Display, which
    /// round-trips). `waf_sig_ind` is `waf_sig` split on ',' and trimmed, without empty tokens.
    #[inline]
    fn for_each_key(&self, i: usize, skip_empty: bool, mut on_key: impl FnMut(Key<'a>)) {
        match self {
            Self::WafSig(a) => {
                if a.is_valid(i) {
                    for s in a
                        .value(i)
                        .split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        on_key(Key::S(s));
                    }
                }
            }
            Self::S(a) => {
                if a.is_valid(i) {
                    let s = a.value(i);
                    if !(skip_empty && s.is_empty()) {
                        on_key(Key::S(s));
                    }
                }
            }
            Self::I(a) => {
                if a.is_valid(i) {
                    on_key(Key::I(a.value(i)));
                }
            }
            Self::B(a) => {
                if a.is_valid(i) {
                    on_key(Key::B(a.value(i)));
                }
            }
            Self::F(a) => {
                if a.is_valid(i) {
                    let v = a.value(i);
                    if v.is_finite() {
                        on_key(Key::F(if v == 0.0 { 0.0f64 } else { v }.to_bits()));
                    }
                }
            }
        }
    }
}

fn hit(cache: Option<&str>) -> bool {
    matches!(cache, Some("HIT" | "HIT-STALE" | "HIT-CLUSTER"))
}

fn ts_hit(cache: Option<&str>) -> bool {
    matches!(cache, Some("HIT" | "HIT-STALE"))
}

/// The dashboard's `isCdnHit`: any state whose uppercase starts with `HIT`.
fn dashboard_hit(cache: Option<&str>) -> bool {
    cache.is_some_and(|c| {
        c.get(..3).is_some_and(|p| p.eq_ignore_ascii_case("HIT"))
            || (!c.is_ascii() && c.to_uppercase().starts_with("HIT"))
    })
}

#[derive(Default)]
struct Minute {
    requests: i64,
    s4: i64,
    s5: i64,
    hits: i64,
    cache_total: i64,
    resp_bytes: i64,
    ttfb_sum: f64,
    ttfb_count: i64,
}

#[derive(Default)]
struct Overview {
    requests: i64,
    hit: i64,
    miss: i64,
    pass: i64,
    synth: i64,
    origin: i64,
    saved: i64,
    total_bytes: i64,
    threats: i64,
    hit_elapsed: (f64, i64),
    miss_elapsed: (f64, i64),
    hit_ttfb: (f64, i64),
    miss_ttfb: (f64, i64),
    waterfall: i64,
    edge_processing: f64,
    origin_wait: f64,
    origin_download: f64,
    client_download: f64,
}

#[derive(Default)]
struct Session<'a> {
    first: i64,
    last: i64,
    reqs: i64,
    country: Option<&'a str>,
    asn: Option<i64>,
    r4: i64,
    r5: i64,
    bytes: i64,
    rtt: (i64, i64),
    edge: i64,
    shield: i64,
    ua: Option<&'a str>,
}

fn min_opt<T: PartialOrd + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if y < x { y } else { x }),
        (x, None) => x,
        (None, y) => y,
    }
}

#[derive(Default)]
struct Dim {
    requests: i64,
    errors: i64,
    hits: i64,
    bytes: i64,
    lat: (f64, i64),
    ost5: i64,
    ost: i64,
    rtt: (i64, i64),
    ploss: (f64, i64),
}

#[derive(Default)]
struct FieldAgg {
    count: i64,
    ip_rows: i64,
    ips: Vec<u32>,
}

fn sketch_ids(ids: &mut Vec<u32>, entries: &[(u8, u8)]) -> (Hll, u64) {
    ids.sort_unstable();
    ids.dedup();
    let mut hll = Hll::default();
    for &id in ids.iter() {
        let (bucket, rank) = entries[id as usize];
        hll.add_bucket_rank(bucket, rank);
    }
    (hll, ids.len() as u64)
}

/// The service's custom card fields: batch columns outside the catalog, minus system-managed
/// CMCD, RUM, edge and request-id columns, which never get a dashboard card (fla v3).
fn custom_fields(batches: &[&RecordBatch]) -> Vec<String> {
    let catalog: Vec<&str> = convert::columns().iter().map(|c| c.id).collect();
    let mut names: Vec<String> = Vec::new();
    for b in batches {
        for f in b.schema().fields() {
            let name = f.name().as_str();
            if !catalog.contains(&name)
                && !FIELDS.contains(&name)
                && name != "fastly_req_id"
                && !["cmcd_", "edge_", "rum_", "_"]
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
                && !names.iter().any(|n| n == name)
            {
                names.push(f.name().clone());
            }
        }
    }
    names.sort_unstable();
    names
}

/// Builds the summary for the rows of one window `[start, end)` (seconds). Deterministic: the
/// same batches in the same order give the same bytes. Heap: one field's value map at a time
/// (about rows x 48 bytes plus 4 bytes per IP row), plus at most `IP_TOP_K` 258-byte sketches
/// per field and `TOP_K` rows per field in the output builders.
pub fn write_summary(batches: &[&RecordBatch], start: i64) -> Result<Vec<u8>, String> {
    let bucket = start * 1000;
    let mut ip_table = IpTable::default();
    let rows: Vec<Rows> = batches
        .iter()
        .filter_map(|b| Rows::new(b, &mut ip_table))
        .collect();
    let mut out = Out::new();

    let mut ov = Overview::default();
    let mut minutes: BTreeMap<i64, Minute> = BTreeMap::new();
    for r in &rows {
        for i in r.live() {
            let status = get_i(r.status, i);
            let cache = get_s(r.cache, i);
            let bytes = get_i(r.resp_bytes, i);
            let ttfb = get_f(r.ttfb, i);
            let elapsed = get_i(r.elapsed, i);
            let m = minutes.entry(r.ts.value(i).div_euclid(60_000)).or_default();
            m.requests += 1;
            let s5 = status.is_some_and(|s| (500..=599).contains(&s));
            m.s4 += i64::from(!s5 && status.is_some_and(|s| s >= 400));
            m.s5 += i64::from(s5);
            m.hits += i64::from(dashboard_hit(cache));
            m.cache_total += i64::from(cache.is_some());
            m.resp_bytes += bytes.unwrap_or(0);
            if let Some(t) = ttfb {
                m.ttfb_sum += t;
                m.ttfb_count += 1;
            }

            ov.requests += 1;
            let is_hit = hit(cache);
            ov.hit += i64::from(is_hit);
            ov.miss += i64::from(cache == Some("MISS"));
            ov.pass += i64::from(cache == Some("PASS"));
            ov.synth += i64::from(cache == Some("SYNTH"));
            ov.origin += i64::from(matches!(cache, Some("MISS" | "PASS" | "SYNTH" | "ERROR")));
            if is_hit {
                ov.saved += bytes.unwrap_or(0);
            }
            ov.total_bytes += bytes.unwrap_or(0);
            ov.threats +=
                i64::from(get_b(r.waf, i) == Some(true) && get_i(r.waf_resp, i) == Some(406));
            if let Some(e) = elapsed {
                if is_hit {
                    ov.hit_elapsed.0 += e as f64;
                    ov.hit_elapsed.1 += 1;
                } else if cache == Some("MISS") {
                    ov.miss_elapsed.0 += e as f64;
                    ov.miss_elapsed.1 += 1;
                }
            }
            if let Some(t) = ttfb {
                if is_hit {
                    ov.hit_ttfb.0 += t;
                    ov.hit_ttfb.1 += 1;
                } else if cache == Some("MISS") {
                    ov.miss_ttfb.0 += t;
                    ov.miss_ttfb.1 += 1;
                }
            }
            if let (Some(t), Some(e)) = (ttfb, elapsed) {
                let ottfb_ms = get_i(r.ottfb, i).map_or(0.0, |v| v as f64 / 1000.0);
                let ottlb_ms = get_i(r.ottlb, i).map_or(0.0, |v| v as f64 / 1000.0);
                let ttfb_ms = t * 1000.0;
                ov.waterfall += 1;
                ov.edge_processing += (ttfb_ms - ottfb_ms).max(0.0);
                ov.origin_wait += ottfb_ms;
                ov.origin_download += (ottlb_ms - ottfb_ms).max(0.0);
                ov.client_download += (e as f64 / 1000.0 - ottlb_ms.max(ttfb_ms)).max(0.0);
            }
        }
    }
    let (ip_hll, ip_distinct) = ip_table.sketch_all();
    let ip_blob = ip_hll.to_bytes();
    out.push(&[
        (C::Bundle, V::S("overview")),
        (C::Bucket, V::I(bucket)),
        (C::Requests, V::I(ov.requests)),
        (C::HitRequests, V::I(ov.hit)),
        (C::MissRequests, V::I(ov.miss)),
        (C::PassRequests, V::I(ov.pass)),
        (C::SynthRequests, V::I(ov.synth)),
        (C::OriginRequests, V::I(ov.origin)),
        (C::BandwidthSaved, V::I(ov.saved)),
        (C::TotalBandwidth, V::I(ov.total_bytes)),
        (C::ShieldHits, V::I(0)),
        (C::ShieldTotal, V::I(0)),
        (C::ThreatsBlocked, V::I(ov.threats)),
        (C::HitElapsedSum, V::F(ov.hit_elapsed.0)),
        (C::HitElapsedCount, V::I(ov.hit_elapsed.1)),
        (C::MissElapsedSum, V::F(ov.miss_elapsed.0)),
        (C::MissElapsedCount, V::I(ov.miss_elapsed.1)),
        (C::HitTtfbSum, V::F(ov.hit_ttfb.0)),
        (C::HitTtfbCount, V::I(ov.hit_ttfb.1)),
        (C::MissTtfbSum, V::F(ov.miss_ttfb.0)),
        (C::MissTtfbCount, V::I(ov.miss_ttfb.1)),
        (C::WaterfallCount, V::I(ov.waterfall)),
        (C::EdgeProcessingSum, V::F(ov.edge_processing)),
        (C::OriginWaitSum, V::F(ov.origin_wait)),
        (C::OriginDownloadSum, V::F(ov.origin_download)),
        (C::ClientDownloadSum, V::F(ov.client_download)),
        (C::IpSketch, V::Bin(&ip_blob)),
        (C::IpCountObserved, V::I(ip_distinct as i64)),
        (C::SampleCapped, V::B(ip_distinct >= IP_SAMPLE_CAP)),
    ]);
    for (minute, m) in &minutes {
        out.push(&[
            (C::Bundle, V::S("time_series")),
            (C::Bucket, V::I(minute * 60_000)),
            (C::Requests, V::I(m.requests)),
            (C::Status4xx, V::I(m.s4)),
            (C::Status5xx, V::I(m.s5)),
            (C::Hits, V::I(m.hits)),
            (C::CacheTotal, V::I(m.cache_total)),
            (C::RespBytesSum, V::I(m.resp_bytes)),
            (C::TtfbSum, V::F(m.ttfb_sum)),
            (C::TtfbCount, V::I(m.ttfb_count)),
        ]);
    }
    drop(minutes);

    let custom = custom_fields(batches);
    let mut aggs: FastMap<Key, FieldAgg> = FastMap::default();
    for field in FIELDS
        .iter()
        .copied()
        .chain(custom.iter().map(String::as_str))
    {
        for r in &rows {
            let Some(col) = FieldCol::resolve(r.batch, field) else {
                continue;
            };
            for i in r.live() {
                let ip_id = r.ip_id[i];
                col.for_each_key(i, false, |key| {
                    let agg = aggs.entry(key).or_default();
                    agg.count += 1;
                    if ip_id != NO_IP {
                        agg.ip_rows += 1;
                        if agg.ips.last() != Some(&ip_id) {
                            agg.ips.push(ip_id);
                        }
                    }
                });
            }
        }
        let total: i64 = aggs.values().map(|a| a.count).sum();
        let truncated = aggs.len() > TOP_K;
        let mut ranked = top(&mut aggs, |a| a.count, TOP_K);
        out.push(&[
            (C::Bundle, V::S("field_totals")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Count, V::I(total)),
            (C::Truncated, V::B(truncated)),
        ]);
        for (value, agg) in &ranked {
            out.push(&[
                (C::Bundle, V::S("all_fields")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(field)),
                (C::Value, V::S(value)),
                (C::Count, V::I(agg.count)),
            ]);
        }
        for (value, agg) in ranked.iter_mut().take(IP_TOP_K) {
            if agg.ip_rows == 0 {
                continue;
            }
            let (hll, observed) = sketch_ids(&mut agg.ips, &ip_table.entries);
            out.push(&[
                (C::Bundle, V::S("all_fields_ip")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(field)),
                (C::Value, V::S(value)),
                (C::Count, V::I(agg.ip_rows)),
                (C::IpSketch, V::Bin(&hll.to_bytes())),
                (C::IpCountObserved, V::I(observed as i64)),
                (C::SampleCapped, V::B(observed >= IP_SAMPLE_CAP)),
            ]);
        }
    }

    drop(aggs);
    drop(ip_table);
    let mut sessions: FastMap<(&str, Option<&str>), Session> = FastMap::default();
    for r in &rows {
        for i in r.live() {
            let Some(ip) = get_s(r.ip, i) else { continue };
            let ts = r.ts.value(i);
            let s = sessions
                .entry((ip, get_s(r.ja4, i)))
                .or_insert_with(|| Session {
                    first: ts,
                    last: ts,
                    ..Default::default()
                });
            s.first = s.first.min(ts);
            s.last = s.last.max(ts);
            s.reqs += 1;
            s.country = min_opt(s.country, get_s(r.country, i));
            s.asn = min_opt(s.asn, get_i(r.asn, i));
            let status = get_i(r.status, i);
            s.r4 += i64::from(status.is_some_and(|x| (400..=499).contains(&x)));
            s.r5 += i64::from(status.is_some_and(|x| x >= 500));
            s.bytes += get_i(r.resp_bytes, i).unwrap_or(0);
            if let Some(rtt) = get_i(r.tcp_rtt, i) {
                s.rtt.0 += rtt;
                s.rtt.1 += 1;
            }
            match get_b(r.edge, i) {
                Some(true) => s.edge += 1,
                Some(false) => s.shield += 1,
                None => {}
            }
            s.ua = min_opt(s.ua, get_s(r.ua, i));
        }
    }
    let mut sessions: Vec<_> = sessions.into_iter().collect();
    rank(
        &mut sessions,
        |a, b| b.1.reqs.cmp(&a.1.reqs).then_with(|| a.0.cmp(&b.0)),
        SESSION_K,
    );
    for ((ip, ja4), s) in &sessions {
        out.push(&[
            (C::Bundle, V::S("sessions")),
            (C::Bucket, V::I(bucket)),
            (C::Ip, V::S(ip)),
            (C::Ja4, ja4.map_or(V::Null, V::S)),
            (C::FirstTs, V::I(s.first)),
            (C::LastTs, V::I(s.last)),
            (C::ReqCount, V::I(s.reqs)),
            (C::Country, s.country.map_or(V::Null, V::S)),
            (C::Asn, s.asn.map_or(V::Null, V::I)),
            (C::Reqs4xx, V::I(s.r4)),
            (C::Reqs5xx, V::I(s.r5)),
            (C::TotalBytes, V::I(s.bytes)),
            (C::RttSum, V::I(s.rtt.0)),
            (C::RttCount, V::I(s.rtt.1)),
            (C::EdgeCount, V::I(s.edge)),
            (C::ShieldCount, V::I(s.shield)),
            (C::UaMin, s.ua.map_or(V::Null, V::S)),
            (C::EdgeSidMax, V::Null),
            (C::CmcdCount, V::I(0)),
        ]);
    }
    drop(sessions);

    for dim in DIMS {
        let mut dims: FastMap<Key, Dim> = FastMap::default();
        for r in &rows {
            let Some(col) = FieldCol::resolve(r.batch, dim) else {
                continue;
            };
            for i in r.live() {
                col.for_each_key(i, true, |key| {
                    let d = dims.entry(key).or_default();
                    let status = get_i(r.status, i);
                    d.requests += 1;
                    d.errors += i64::from(status.is_some_and(|s| s >= 400 || s == 0));
                    d.hits += i64::from(ts_hit(get_s(r.cache, i)));
                    d.bytes += get_i(r.resp_bytes, i).unwrap_or(0);
                    let lat = get_i(r.ottfb, i)
                        .map(|v| v as f64)
                        .or_else(|| get_f(r.ttfb, i).map(|t| t * 1e6));
                    if let Some(lat) = lat {
                        d.lat.0 += lat;
                        d.lat.1 += 1;
                    }
                    if let Some(ost) = get_i(r.ost, i) {
                        d.ost += 1;
                        d.ost5 += i64::from(ost >= 500);
                    }
                    if let Some(rtt) = get_i(r.tcp_rtt, i) {
                        d.rtt.0 += rtt;
                        d.rtt.1 += 1;
                    }
                    if let Some(p) = get_f(r.ploss, i) {
                        d.ploss.0 += p;
                        d.ploss.1 += 1;
                    }
                });
            }
        }
        let ranked = top(&mut dims, |d| d.requests, DIM_K);
        drop(dims);
        for (value, d) in &ranked {
            out.push(&[
                (C::Bundle, V::S("dims")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(dim)),
                (C::Value, V::S(value)),
                (C::Requests, V::I(d.requests)),
                (C::Errors, V::I(d.errors)),
                (C::CacheHits, V::I(d.hits)),
                (C::RespBytesSum, V::I(d.bytes)),
                (C::LatUsSum, V::F(d.lat.0)),
                (C::LatUsCount, V::I(d.lat.1)),
                (C::Ost5xxCount, V::I(d.ost5)),
                (C::OstTotalCount, V::I(d.ost)),
                (C::RttSum, V::I(d.rtt.0)),
                (C::RttCount, V::I(d.rtt.1)),
                (C::PlossSum, V::F(d.ploss.0)),
                (C::PlossCount, V::I(d.ploss.1)),
            ]);
        }
    }

    let batch = out.finish()?;
    convert::encode_record_batch(
        schema(),
        &batch,
        vec![KeyValue::new(
            "flp_summary".into(),
            SUMMARY_VERSION.to_string(),
        )],
    )
}
