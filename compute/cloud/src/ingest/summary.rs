//! Per-window summary Parquet, written by the ingest worker next to each window file.
//!
//! One long-format file per window, rows tagged by `bundle`. Shapes and definitions mirror the
//! fla v3 hourly rollups (`backend/core/rollups/` at 2c75f058): `time_series`, `overview`,
//! `all_fields` (+ `field_totals`), `all_fields_ip`, `sessions` and additive `dims`. Every value
//! is an additive sum or count, a min/max, or an HLL sketch, so windows merge into hours and days.

use super::convert::{self, FastMap};
use super::hist::{self, Hist};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    },
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::file::metadata::KeyValue;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub const SUMMARY_VERSION: &str = "3";
/// Values kept per field per window in `all_fields`.
pub const TOP_K: usize = 2000;
/// Values per field that also get an IP-spread sketch (fla's hourly `TOP_K`).
pub const IP_TOP_K: usize = 500;
/// fla's `IP_SAMPLE_CAP`: `sample_capped` is set at or above this many distinct IPs.
pub const IP_SAMPLE_CAP: u64 = 5000;
pub const SESSION_K: usize = 2000;
pub const DIM_K: usize = 2000;
/// ASNs kept in `series5` per window and hour bundle: 4x the 30 a day bundle writes.
pub const SERIES_ASN_K: usize = 120;
/// `sample` rows per window and bundle.
pub const SAMPLE_K: usize = 300;
/// `series5` and `bins` bucket width in windows and hour bundles; day bundles use an hour.
pub const SERIES_STEP_MS: i64 = 300_000;
pub use super::hll::{P as HLL_P, VERSION as HLL_VERSION};

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

/// `country`, `asn`, `city` and `proto` use the /network keys (a null is `Unknown`, `0` or
/// `Other`; `city` is the JSON `[country, city]`); the others skip null and ''.
pub const DIMS: &[&str] = &[
    "pop", "oip", "edge", "backend", "country", "region", "asn", "city", "proto",
];

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

pub use super::hll::{hash as hll_hash, Hll};

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
    TtfbHist = ("ttfb_hist", Bin),
    ElapsedHist = ("elapsed_hist", Bin),
    RttHist = ("rtt_hist", Bin),
    OttfbHist = ("ottfb_hist", Bin),
    OttlbHist = ("ottlb_hist", Bin),
    Label = ("label", Utf8),
    CwndSum = ("cwnd_sum", F64),
    CwndCount = ("cwnd_count", I64),
    RttvarSum = ("rttvar_sum", F64),
    RttvarCount = ("rttvar_count", I64),
    RetransCount = ("retrans_count", I64),
    LatSum = ("lat_sum", F64),
    LonSum = ("lon_sum", F64),
    GeoCount = ("geo_count", I64),
    SampleKey = ("sample_key", I64),
    ElapsedUs = ("elapsed_us", I64),
}

/// The histogram columns, in [`Lat`] order.
pub(super) const HIST_COLS: [C; 5] = [
    C::TtfbHist,
    C::ElapsedHist,
    C::RttHist,
    C::OttfbHist,
    C::OttlbHist,
];
/// The latencies `dims` keep (no elapsed): indexes into [`HIST_COLS`].
const DIM_HISTS: [usize; 4] = [0, 2, 3, 4];

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
/// precomputed HLL sparse entries, avoiding repeated MD5 hashing and 8-byte sorting across
/// the 42 `all_fields` passes.
#[derive(Default)]
struct IpTable<'a> {
    by_str: FastMap<&'a str, u32>,
    by_hash: FastMap<u64, u32>,
    entries: Vec<u32>,
}

impl<'a> IpTable<'a> {
    fn intern(&mut self, ip: &'a str) -> u32 {
        if let Some(&id) = self.by_str.get(ip) {
            return id;
        }
        let h = hll_hash(ip);
        let id = *self.by_hash.entry(h).or_insert_with(|| {
            let next = self.entries.len() as u32;
            self.entries.push(super::hll::entry(h));
            next
        });
        self.by_str.insert(ip, id);
        id
    }

    fn sketch_all(&self) -> (Hll, u64) {
        (
            Hll::from_entries(self.entries.clone()),
            self.entries.len() as u64,
        )
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
    net: NetCols<'a>,
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
            net: NetCols::new(batch),
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

/// A numeric column of any type (a string parses like DuckDB's `TRY_CAST`), scaled.
#[derive(Clone, Copy)]
enum AnyNum<'a> {
    I(&'a Int64Array),
    F(&'a Float64Array),
    S(&'a StringArray),
}

/// `COALESCE` over numeric columns, each resolved once per batch; absent and all-null columns
/// drop out.
struct Num<'a>(Vec<(AnyNum<'a>, f64)>);

impl<'a> Num<'a> {
    fn new(batch: &'a RecordBatch, names: &[(&str, f64)]) -> Self {
        Self(
            names
                .iter()
                .filter_map(|&(name, scale)| {
                    let a = batch.column_by_name(name)?;
                    if a.null_count() >= a.len() {
                        return None;
                    }
                    let any = a.as_any();
                    let col = if let Some(x) = any.downcast_ref::<Int64Array>() {
                        AnyNum::I(x)
                    } else if let Some(x) = any.downcast_ref::<Float64Array>() {
                        AnyNum::F(x)
                    } else {
                        AnyNum::S(any.downcast_ref::<StringArray>()?)
                    };
                    Some((col, scale))
                })
                .collect(),
        )
    }

    #[inline]
    fn get(&self, i: usize) -> Option<f64> {
        self.0.iter().find_map(|&(col, scale)| {
            let v = match col {
                AnyNum::I(a) => a.is_valid(i).then(|| a.value(i) as f64),
                AnyNum::F(a) => a.is_valid(i).then(|| a.value(i)),
                AnyNum::S(a) => a
                    .is_valid(i)
                    .then(|| a.value(i).trim().parse::<f64>().ok())
                    .flatten(),
            }?;
            v.is_finite().then_some(v * scale)
        })
    }
}

/// `COALESCE` over string columns; with `skip_empty`, `NULLIF(col, '')` each.
struct Str<'a>(Vec<&'a StringArray>);

impl<'a> Str<'a> {
    fn new(batch: &'a RecordBatch, names: &[&str]) -> Self {
        Self(
            names
                .iter()
                .filter_map(|name| col::<StringArray>(batch, name))
                .filter(|a| a.null_count() < a.len())
                .collect(),
        )
    }

    #[inline]
    fn get(&self, i: usize, skip_empty: bool) -> Option<&'a str> {
        self.0.iter().find_map(|a| {
            let v = a.is_valid(i).then(|| a.value(i))?;
            (!(skip_empty && v.is_empty())).then_some(v)
        })
    }
}

/// The batch columns behind the v3 rows, with the dashboard's aliases: the /network,
/// /performance, /security and /assets queries' `COALESCE`s.
struct NetCols<'a> {
    country: Str<'a>,
    city: Str<'a>,
    proto: Str<'a>,
    as_name: Str<'a>,
    asn: Num<'a>,
    rtt: Num<'a>,
    ploss: Num<'a>,
    cwnd: Num<'a>,
    rttvar: Num<'a>,
    retrans: Num<'a>,
    lat: Num<'a>,
    lon: Num<'a>,
    header: Num<'a>,
    waf_sig: Option<&'a StringArray>,
    asset_type: Str<'a>,
    content_type: Str<'a>,
    url: Str<'a>,
    cache: Str<'a>,
    encoding: Str<'a>,
    body: Num<'a>,
    ttfb_ms: Num<'a>,
    origin_ms: Num<'a>,
    req_id: Str<'a>,
}

impl<'a> NetCols<'a> {
    fn new(batch: &'a RecordBatch) -> Self {
        Self {
            country: Str::new(batch, &["geo_country", "country"]),
            city: Str::new(batch, &["geo_city", "city"]),
            proto: Str::new(batch, &["protocol", "proto"]),
            as_name: Str::new(batch, &["as_name"]),
            asn: Num::new(batch, &[("client_asn", 1.0), ("asn", 1.0)]),
            rtt: Num::new(
                batch,
                &[
                    ("client_rtt_us", 1.0),
                    ("client_tcpi_rtt", 1.0),
                    ("tcp_rtt", 1.0),
                    ("tcpi_rtt", 1.0),
                ],
            ),
            ploss: Num::new(batch, &[("client_ploss", 1.0), ("ploss", 1.0)]),
            cwnd: Num::new(batch, &[("client_cwnd", 1.0)]),
            rttvar: Num::new(
                batch,
                &[
                    ("client_tcpi_rttvar", 1.0),
                    ("rtt_var", 1.0),
                    ("tcpi_rtt_var", 1.0),
                ],
            ),
            retrans: Num::new(batch, &[("client_tcpi_delta_retrans", 1.0)]),
            lat: Num::new(batch, &[("lat", 1.0)]),
            lon: Num::new(batch, &[("lon", 1.0)]),
            header: Num::new(batch, &[("req_header_bytes", 1.0)]),
            waf_sig: col(batch, "waf_sig"),
            asset_type: Str::new(batch, &["asset_type"]),
            content_type: Str::new(batch, &["content_type"]),
            url: Str::new(batch, &["url", "path"]),
            cache: Str::new(batch, &["cache_status", "cache"]),
            encoding: Str::new(batch, &["content_encoding", "resp_header_content_encoding"]),
            body: Num::new(batch, &[("resp_body_size", 1.0), ("resp_bytes", 1.0)]),
            ttfb_ms: Num::new(
                batch,
                &[
                    ("ttfb_ms", 1.0),
                    ("ttfb", 1000.0),
                    ("time_to_first_byte_ns", 1e-6),
                    ("time_elapsed_usec", 1e-3),
                    ("elapsed", 1e-3),
                ],
            ),
            origin_ms: Num::new(batch, &[("origin_Whole_ms", 1.0), ("origin_time_ms", 1.0)]),
            req_id: Str::new(batch, &["fastly_req_id", "rid"]),
        }
    }
}

/// Latencies in µs, in [`HIST_COLS`] order: TTFB, elapsed, RTT, origin TTFB, origin TTLB, each
/// with its [`hist::slot`].
type Lat = [Option<(i64, u8)>; 5];

/// One row's values for the v3 rows, computed once and shared by every group it lands in.
struct Net<'a> {
    status: Option<i64>,
    s5: bool,
    hit: bool,
    bytes: i64,
    lat_us: Option<f64>,
    ost: Option<i64>,
    rtt: Option<i64>,
    ploss: Option<f64>,
    cwnd: Option<f64>,
    rttvar: Option<f64>,
    retrans: bool,
    geo: Option<(f64, f64)>,
    as_name: Option<&'a str>,
    country: &'a str,
    city: &'a str,
    proto: &'a str,
    asn: i64,
    lat: Lat,
}

/// TTFB seconds as whole µs, the histograms' unit.
fn ttfb_us(ttfb: Option<f64>) -> Option<i64> {
    ttfb.map(|t| (t * 1e6).round() as i64)
}

fn lat(v: Option<i64>) -> Option<(i64, u8)> {
    v.map(|v| (v, hist::slot(v)))
}

fn add_lat(hists: &mut [Hist], lat: &Lat, which: &[usize]) {
    for (h, &k) in hists.iter_mut().zip(which) {
        if let Some((v, slot)) = lat[k] {
            h.add_slot(v, slot);
        }
    }
}

#[derive(Default)]
struct Dim<'a> {
    requests: i64,
    errors: i64,
    hits: i64,
    bytes: i64,
    lat: (f64, i64),
    ost5: i64,
    ost: i64,
    rtt: (i64, i64),
    ploss: (f64, i64),
    s5: i64,
    cwnd: (f64, i64),
    rttvar: (f64, i64),
    retrans: i64,
    geo: Option<(f64, f64, i64)>,
    label: Option<&'a str>,
    hists: [Hist; 4],
}

impl<'a> Dim<'a> {
    fn add(&mut self, n: &Net<'a>, geo: bool, label: bool) {
        self.requests += 1;
        self.errors += i64::from(n.status.is_some_and(|s| s >= 400 || s == 0));
        self.hits += i64::from(n.hit);
        self.bytes += n.bytes;
        if let Some(l) = n.lat_us {
            self.lat.0 += l;
            self.lat.1 += 1;
        }
        if let Some(ost) = n.ost {
            self.ost += 1;
            self.ost5 += i64::from(ost >= 500);
        }
        if let Some(r) = n.rtt {
            self.rtt.0 += r;
            self.rtt.1 += 1;
        }
        if let Some(p) = n.ploss {
            self.ploss.0 += p;
            self.ploss.1 += 1;
        }
        self.s5 += i64::from(n.s5);
        if let Some(c) = n.cwnd {
            self.cwnd.0 += c;
            self.cwnd.1 += 1;
        }
        if let Some(v) = n.rttvar {
            self.rttvar.0 += v;
            self.rttvar.1 += 1;
        }
        self.retrans += i64::from(n.retrans);
        if geo {
            let g = self.geo.get_or_insert((0.0, 0.0, 0));
            if let Some((la, lo)) = n.geo {
                g.0 += la;
                g.1 += lo;
                g.2 += 1;
            }
        }
        if label {
            self.label = min_opt(self.label, n.as_name);
        }
        add_lat(&mut self.hists, &n.lat, &DIM_HISTS);
    }
}

#[derive(Default)]
struct Series<'a> {
    requests: i64,
    s5: i64,
    bytes: i64,
    rtt: (i64, i64),
    ploss: (f64, i64),
    label: Option<&'a str>,
    hists: [Hist; 5],
}

impl<'a> Series<'a> {
    fn add(&mut self, n: &Net<'a>, label: bool) {
        self.requests += 1;
        self.s5 += i64::from(n.s5);
        self.bytes += n.bytes;
        if let Some(r) = n.rtt {
            self.rtt.0 += r;
            self.rtt.1 += 1;
        }
        if let Some(p) = n.ploss {
            self.ploss.0 += p;
            self.ploss.1 += 1;
        }
        if label {
            self.label = min_opt(self.label, n.as_name);
        }
        add_lat(&mut self.hists, &n.lat, &[0, 1, 2, 3, 4]);
    }
}

#[derive(Default)]
struct Asset {
    requests: i64,
    bytes: i64,
    ttfb_ms: f64,
    origin_ms: f64,
    origin_requests: i64,
}

/// A `dims` key: a card-style value, or a `city` within its country.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum DimKey<'a> {
    K(Key<'a>),
    City(&'a str, &'a str),
}

impl<'a> DimKey<'a> {
    fn render(&self) -> Cow<'a, str> {
        match self {
            Self::K(k) => k.render(),
            Self::City(country, city) => {
                Cow::Owned(serde_json::to_string(&[country, city]).expect("strings serialize"))
            }
        }
    }
}

/// `security.ts` `buildReqHeaderBucketCaseSql`.
fn header_bin(bytes: f64) -> &'static str {
    match bytes {
        b if b <= 256.0 => "0-256B",
        b if b <= 1024.0 => "256B-1KB",
        b if b <= 4096.0 => "1KB-4KB",
        b if b <= 8192.0 => "4KB-8KB",
        b if b <= 32768.0 => "8KB-32KB",
        _ => ">32KB",
    }
}

fn ends_with_ci(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

fn contains_ci(s: &str, needle: &str) -> bool {
    s.as_bytes()
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
}

fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// `assets.ts` `DERIVED_ASSET_CTE`'s `derived_asset_type`.
fn asset_type<'a>(
    declared: Option<&'a str>,
    content_type: Option<&str>,
    url: Option<&str>,
) -> &'a str {
    if let Some(t) = declared {
        return t;
    }
    let ct = content_type.unwrap_or("");
    let url = url.unwrap_or("");
    if contains_ci(ct, "javascript") || ends_with_ci(url, ".js") {
        "JavaScript"
    } else if contains_ci(ct, "text/css") || ends_with_ci(url, ".css") {
        "CSS"
    } else if starts_with_ci(ct, "image/") {
        "Image"
    } else if starts_with_ci(ct, "font/") || contains_ci(url, ".woff") {
        "Font"
    } else if starts_with_ci(ct, "video/") || starts_with_ci(ct, "audio/") {
        "Media"
    } else if contains_ci(ct, "application/json") {
        "JSON"
    } else if contains_ci(ct, "text/html") {
        "HTML"
    } else {
        "Other"
    }
}

/// Verified bots as /security counts them: `waf_sig` containing `VERIFIED-BOT.` in any case,
/// named by the signature with each exact `VERIFIED-BOT.` removed.
fn verified_bot(sig: &str) -> Option<Cow<'_, str>> {
    if !contains_ci(sig, "VERIFIED-BOT.") {
        return None;
    }
    Some(if sig.contains("VERIFIED-BOT.") {
        Cow::Owned(sig.replace("VERIFIED-BOT.", ""))
    } else {
        Cow::Borrowed(sig)
    })
}

/// The sample order: FNV-1a 64 of the request id (or of the row's timestamp, URL, IP and
/// position), halved to fit an `i64`.
fn sample_key(
    req_id: Option<&str>,
    ts: i64,
    url: Option<&str>,
    ip: Option<&str>,
    at: usize,
) -> i64 {
    let h = match req_id {
        Some(id) => compute_common::crypto::fnv1a64(id.bytes()),
        None => compute_common::crypto::fnv1a64(
            format!("{ts}|{}|{}|{at}", url.unwrap_or(""), ip.unwrap_or("")).into_bytes(),
        ),
    };
    (h >> 1) as i64
}

/// The `keep` keys of `totals` with the most requests, ties by rendered value ascending.
fn top_keys<K: Copy>(
    totals: impl Iterator<Item = (K, i64, String)>,
    keep: usize,
) -> Vec<(K, String)> {
    let mut ranked: Vec<(K, i64, String)> = totals.collect();
    rank(
        &mut ranked,
        |a, b| b.1.cmp(&a.1).then_with(|| a.2.cmp(&b.2)),
        keep,
    );
    ranked.into_iter().map(|(k, _, s)| (k, s)).collect()
}

#[derive(Default)]
struct FieldAgg {
    count: i64,
    ip_rows: i64,
    ips: Vec<u32>,
}

fn sketch_ids(ids: &mut Vec<u32>, entries: &[u32]) -> (Hll, u64) {
    ids.sort_unstable();
    ids.dedup();
    let hll = Hll::from_entries(ids.iter().map(|&id| entries[id as usize]).collect());
    (hll, ids.len() as u64)
}

/// The service's custom card fields: batch columns outside the catalog, minus system-managed
/// CMCD, RUM, edge, load-test (`lt_`) and request-id columns, which never get a dashboard card
/// (fla v3).
fn custom_fields(batches: &[&RecordBatch]) -> Vec<String> {
    let catalog: Vec<&str> = convert::columns().iter().map(|c| c.id).collect();
    let mut names: Vec<String> = Vec::new();
    for b in batches {
        for f in b.schema().fields() {
            let name = f.name().as_str();
            if !catalog.contains(&name)
                && !FIELDS.contains(&name)
                && name != "fastly_req_id"
                && !["cmcd_", "edge_", "rum_", "lt_", "_"]
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

fn push_hists<'b>(row: &mut Vec<(C, V<'b>)>, cols: &[C], blobs: &'b [Option<Vec<u8>>]) {
    for (&c, blob) in cols.iter().zip(blobs) {
        if let Some(b) = blob {
            row.push((c, V::Bin(b)));
        }
    }
}

fn opt_s(v: Option<&str>) -> V<'_> {
    v.map_or(V::Null, V::S)
}

/// `s` in ASCII upper (or lower) case, borrowed when it already is.
fn ascii_case(s: &str, upper: bool) -> Cow<'_, str> {
    let wrong = |b: &u8| {
        if upper {
            b.is_ascii_lowercase()
        } else {
            b.is_ascii_uppercase()
        }
    };
    if !s.bytes().any(|b| wrong(&b)) {
        Cow::Borrowed(s)
    } else if upper {
        Cow::Owned(s.to_ascii_uppercase())
    } else {
        Cow::Owned(s.to_ascii_lowercase())
    }
}

/// One `dim_totals` row: every request a field of `group` counted, and whether rows were cut.
fn push_total(out: &mut Out, bucket: i64, group: &str, field: &str, total: i64, truncated: bool) {
    out.push(&[
        (C::Bundle, V::S("dim_totals")),
        (C::Bucket, V::I(bucket)),
        (C::FieldName, V::S(field)),
        (C::Value, V::S(group)),
        (C::Requests, V::I(total)),
        (C::Truncated, V::B(truncated)),
    ]);
}

/// `(bucket, value, row)` triples of one field.
type TimedRows<'a, T> = Vec<(i64, Cow<'a, str>, T)>;

/// Timed rows of one field kept to its `keep` values with the most requests over the window.
fn top_timed<'a, T>(
    rows: FastMap<(i64, Cow<'a, str>), T>,
    requests: impl Fn(&T) -> i64,
    keep: usize,
) -> (TimedRows<'a, T>, i64, bool) {
    let mut totals: FastMap<Cow<'a, str>, i64> = FastMap::default();
    for ((_, v), t) in &rows {
        *totals.entry(v.clone()).or_default() += requests(t);
    }
    let total = totals.values().sum();
    let truncated = totals.len() > keep;
    let kept = top_keys(
        totals.into_iter().map(|(v, n)| ((), n, v.into_owned())),
        keep,
    );
    let order: FastMap<&str, usize> = kept
        .iter()
        .enumerate()
        .map(|(at, (_, v))| (v.as_str(), at))
        .collect();
    let mut out: Vec<(usize, i64, Cow<'a, str>, T)> = rows
        .into_iter()
        .filter_map(|((b, v), t)| Some((*order.get(v.as_ref())?, b, v, t)))
        .collect();
    out.sort_unstable_by_key(|r| (r.0, r.1));
    (
        out.into_iter().map(|(_, b, v, t)| (b, v, t)).collect(),
        total,
        truncated,
    )
}

fn series_row<'b>(out: &mut Out, bucket: i64, field: &str, value: &str, s: &Series<'b>) {
    let blobs = s.hists.each_ref().map(Hist::to_bytes);
    let mut row = vec![
        (C::Bundle, V::S("series5")),
        (C::Bucket, V::I(bucket)),
        (C::FieldName, V::S(field)),
        (C::Value, V::S(value)),
        (C::Requests, V::I(s.requests)),
        (C::Status5xx, V::I(s.s5)),
        (C::RespBytesSum, V::I(s.bytes)),
        (C::RttSum, V::I(s.rtt.0)),
        (C::RttCount, V::I(s.rtt.1)),
        (C::PlossSum, V::F(s.ploss.0)),
        (C::PlossCount, V::I(s.ploss.1)),
        (C::Label, opt_s(s.label)),
    ];
    push_hists(&mut row, &HIST_COLS, &blobs);
    out.push(&row);
}

/// The v3 rows (`dims`, `series5`, `bins`, `assets`, `sample` and their `dim_totals`) in one
/// pass over the rows.
fn write_v3(rows: &[Rows], bucket: i64, out: &mut Out) {
    const FIELD_DIMS: [&str; 5] = ["pop", "oip", "edge", "backend", "region"];
    let mut dims: Vec<FastMap<DimKey, Dim>> = DIMS.iter().map(|_| FastMap::default()).collect();
    let dim_at = |name: &str| DIMS.iter().position(|d| *d == name).expect("a dim");
    let (country_at, asn_at, city_at, proto_at) = (
        dim_at("country"),
        dim_at("asn"),
        dim_at("city"),
        dim_at("proto"),
    );
    let field_at = FIELD_DIMS.map(dim_at);
    let mut overall: BTreeMap<i64, Series> = BTreeMap::new();
    let mut by_proto: FastMap<(i64, Cow<str>), Series> = FastMap::default();
    let mut by_asn: FastMap<(i64, Cow<str>), Series> = FastMap::default();
    let mut header_bins: FastMap<(i64, Cow<str>), i64> = FastMap::default();
    let mut bot_bins: FastMap<(i64, Cow<str>), i64> = FastMap::default();
    let mut assets: FastMap<(&str, Cow<str>, Cow<str>), Asset> = FastMap::default();
    // Max-heap of the smallest sample keys: (key, ts, batch, row).
    let mut sample: std::collections::BinaryHeap<(i64, i64, usize, usize)> =
        std::collections::BinaryHeap::with_capacity(SAMPLE_K + 1);

    for (at, r) in rows.iter().enumerate() {
        let c = &r.net;
        let field_cols = FIELD_DIMS.map(|d| FieldCol::resolve(r.batch, d));
        for i in r.live() {
            let ts = r.ts.value(i);
            let step = ts.div_euclid(SERIES_STEP_MS) * SERIES_STEP_MS;
            let status = get_i(r.status, i);
            let cache = get_s(r.cache, i);
            let ttfb = get_f(r.ttfb, i);
            let elapsed = get_i(r.elapsed, i);
            let ottfb = get_i(r.ottfb, i);
            let rtt = c.rtt.get(i).map(|v| v.round() as i64);
            let as_name = c.as_name.get(i, false);
            let n = Net {
                status,
                s5: status.is_some_and(|s| (500..=599).contains(&s)),
                hit: ts_hit(cache),
                bytes: get_i(r.resp_bytes, i).unwrap_or(0),
                lat_us: ottfb.map(|v| v as f64).or_else(|| ttfb.map(|t| t * 1e6)),
                ost: get_i(r.ost, i),
                rtt,
                ploss: c.ploss.get(i),
                cwnd: c.cwnd.get(i),
                rttvar: c.rttvar.get(i),
                retrans: c.retrans.get(i).is_some_and(|v| v > 0.0),
                geo: c.lat.get(i).zip(c.lon.get(i)),
                as_name,
                country: c.country.get(i, false).unwrap_or("Unknown"),
                city: c.city.get(i, false).unwrap_or("Unknown"),
                proto: c.proto.get(i, false).unwrap_or("Other"),
                asn: c.asn.get(i).map_or(0, |v| v as i64),
                lat: [
                    lat(ttfb_us(ttfb)),
                    lat(elapsed),
                    lat(rtt),
                    lat(ottfb),
                    lat(get_i(r.ottlb, i)),
                ],
            };

            for (k, col) in field_at.iter().zip(&field_cols) {
                if let Some(col) = col {
                    col.for_each_key(i, true, |key| {
                        dims[*k]
                            .entry(DimKey::K(key))
                            .or_default()
                            .add(&n, false, false);
                    });
                }
            }
            dims[country_at]
                .entry(DimKey::K(Key::S(n.country)))
                .or_default()
                .add(&n, false, false);
            dims[asn_at]
                .entry(DimKey::K(Key::I(n.asn)))
                .or_default()
                .add(&n, false, true);
            dims[city_at]
                .entry(DimKey::City(n.country, n.city))
                .or_default()
                .add(&n, true, false);
            dims[proto_at]
                .entry(DimKey::K(Key::S(n.proto)))
                .or_default()
                .add(&n, false, false);

            overall.entry(step).or_default().add(&n, false);
            by_proto
                .entry((step, Cow::Borrowed(n.proto)))
                .or_default()
                .add(&n, false);
            by_asn
                .entry((step, Cow::Owned(n.asn.to_string())))
                .or_default()
                .add(&n, true);

            if let Some(h) = c.header.get(i) {
                *header_bins
                    .entry((step, Cow::Borrowed(header_bin(h))))
                    .or_default() += 1;
            }
            if let Some(bot) = get_s(c.waf_sig, i).and_then(verified_bot) {
                *bot_bins.entry((step, bot)).or_default() += 1;
            }

            let url = c.url.get(i, false);
            let kind = asset_type(c.asset_type.get(i, true), c.content_type.get(i, false), url);
            let a = assets
                .entry((
                    kind,
                    ascii_case(c.cache.get(i, false).unwrap_or("OTHER"), true),
                    ascii_case(c.encoding.get(i, true).unwrap_or("unknown"), false),
                ))
                .or_default();
            a.requests += 1;
            a.bytes += c.body.get(i).map_or(0, |v| v as i64);
            a.ttfb_ms += c.ttfb_ms.get(i).unwrap_or(0.0);
            let origin = c.origin_ms.get(i).unwrap_or(0.0);
            a.origin_ms += origin;
            a.origin_requests += i64::from(origin > 0.0);

            if ttfb.is_some() {
                let key = sample_key(c.req_id.get(i, false), ts, url, get_s(r.ip, i), i);
                let entry = (key, ts, at, i);
                if sample.len() < SAMPLE_K {
                    sample.push(entry);
                } else if sample.peek().is_some_and(|top| entry < *top) {
                    sample.pop();
                    sample.push(entry);
                }
            }
        }
    }

    for (k, dim) in DIMS.iter().enumerate() {
        let map = std::mem::take(&mut dims[k]);
        let total: i64 = map.values().map(|d| d.requests).sum();
        let truncated = map.len() > DIM_K;
        let mut ranked: Vec<(DimKey, i64, String)> = Vec::new();
        let mut values: FastMap<DimKey, Dim> = map;
        for (key, d) in &values {
            ranked.push((*key, d.requests, key.render().into_owned()));
        }
        let kept = top_keys(ranked.into_iter(), DIM_K);
        push_total(out, bucket, "dims", dim, total, truncated);
        for (key, value) in &kept {
            let d = values.remove(key).expect("a ranked key");
            let blobs = d.hists.each_ref().map(Hist::to_bytes);
            let mut row = vec![
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
                (C::Status5xx, V::I(d.s5)),
                (C::CwndSum, V::F(d.cwnd.0)),
                (C::CwndCount, V::I(d.cwnd.1)),
                (C::RttvarSum, V::F(d.rttvar.0)),
                (C::RttvarCount, V::I(d.rttvar.1)),
                (C::RetransCount, V::I(d.retrans)),
                (C::Label, opt_s(d.label)),
            ];
            if let Some((la, lo, n)) = d.geo {
                row.extend([
                    (C::LatSum, V::F(la)),
                    (C::LonSum, V::F(lo)),
                    (C::GeoCount, V::I(n)),
                ]);
            }
            let cols = DIM_HISTS.map(|h| HIST_COLS[h]);
            push_hists(&mut row, &cols, &blobs);
            out.push(&row);
        }
    }
    drop(dims);

    let total: i64 = overall.values().map(|s| s.requests).sum();
    push_total(out, bucket, "series5", "overall", total, false);
    for (step, s) in &overall {
        series_row(out, *step, "overall", "", s);
    }
    drop(overall);
    let (proto, total, truncated) = top_timed(by_proto, |s| s.requests, DIM_K);
    push_total(out, bucket, "series5", "proto", total, truncated);
    for (step, value, s) in &proto {
        series_row(out, *step, "proto", value, s);
    }
    drop(proto);
    let (asn, total, truncated) = top_timed(by_asn, |s| s.requests, SERIES_ASN_K);
    push_total(out, bucket, "series5", "asn", total, truncated);
    for (step, value, s) in &asn {
        series_row(out, *step, "asn", value, s);
    }
    drop(asn);

    for (field, bins) in [("req_header_size", header_bins), ("verified_bot", bot_bins)] {
        let (kept, total, truncated) = top_timed(bins, |n| *n, DIM_K);
        push_total(out, bucket, "bins", field, total, truncated);
        for (step, value, n) in &kept {
            out.push(&[
                (C::Bundle, V::S("bins")),
                (C::Bucket, V::I(*step)),
                (C::FieldName, V::S(field)),
                (C::Value, V::S(value)),
                (C::Requests, V::I(*n)),
            ]);
        }
    }

    let total: i64 = assets.values().map(|a| a.requests).sum();
    let truncated = assets.len() > DIM_K;
    let kept = top_keys(
        assets.iter().map(|(k, a)| {
            let value = serde_json::to_string(&[k.0, k.1.as_ref(), k.2.as_ref()])
                .expect("strings serialize");
            (k, a.requests, value)
        }),
        DIM_K,
    );
    push_total(out, bucket, "assets", "asset", total, truncated);
    for (key, value) in &kept {
        let a = &assets[*key];
        out.push(&[
            (C::Bundle, V::S("assets")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("asset")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(a.requests)),
            (C::RespBytesSum, V::I(a.bytes)),
            (C::TtfbSum, V::F(a.ttfb_ms)),
            (C::OriginWaitSum, V::F(a.origin_ms)),
            (C::OriginRequests, V::I(a.origin_requests)),
        ]);
    }
    drop(kept);
    drop(assets);

    for (key, ts, at, i) in sample.into_sorted_vec() {
        let r = &rows[at];
        out.push(&[
            (C::Bundle, V::S("sample")),
            (C::Bucket, V::I(ts)),
            (C::Value, opt_s(get_s(r.cache, i))),
            (C::TtfbSum, get_f(r.ttfb, i).map_or(V::Null, V::F)),
            (C::ElapsedUs, get_i(r.elapsed, i).map_or(V::Null, V::I)),
            (
                C::RttSum,
                r.net.rtt.get(i).map_or(V::Null, |v| V::I(v.round() as i64)),
            ),
            (C::SampleKey, V::I(key)),
        ]);
    }
}

/// Builds the summary for the rows of one window `[start, end)` (seconds). Deterministic: the
/// same batches in the same order give the same bytes. Heap: one field's value map at a time
/// (about rows x 48 bytes plus 4 bytes per IP row), plus at most `IP_TOP_K` sketches per field
/// (4 bytes per distinct IP while sparse, 4 KiB once dense past 1,024) and `TOP_K` rows per field
/// in the output builders.
pub fn write_summary(batches: &[&RecordBatch], start: i64) -> Result<Vec<u8>, String> {
    let bucket = start * 1000;
    let mut ip_table = IpTable::default();
    let rows: Vec<Rows> = batches
        .iter()
        .filter_map(|b| Rows::new(b, &mut ip_table))
        .collect();
    let mut out = Out::new();

    let mut ov = Overview::default();
    let mut ov_hists: [Hist; 5] = Default::default();
    let mut minutes: BTreeMap<i64, Minute> = BTreeMap::new();
    for r in &rows {
        for i in r.live() {
            let status = get_i(r.status, i);
            let cache = get_s(r.cache, i);
            let bytes = get_i(r.resp_bytes, i);
            let ttfb = get_f(r.ttfb, i);
            let elapsed = get_i(r.elapsed, i);
            let ottfb = get_i(r.ottfb, i);
            let ottlb = get_i(r.ottlb, i);
            let rtt = r.net.rtt.get(i).map(|v| v.round() as i64);
            for (h, v) in ov_hists
                .iter_mut()
                .zip([ttfb_us(ttfb), elapsed, rtt, ottfb, ottlb])
            {
                if let Some(v) = v {
                    h.add(v);
                }
            }
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
                let ottfb_ms = ottfb.map_or(0.0, |v| v as f64 / 1000.0);
                let ottlb_ms = ottlb.map_or(0.0, |v| v as f64 / 1000.0);
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
    let mut row = vec![
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
    ];
    let blobs = ov_hists.each_ref().map(Hist::to_bytes);
    push_hists(&mut row, &HIST_COLS, &blobs);
    out.push(&row);
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

    write_v3(&rows, bucket, &mut out);

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
