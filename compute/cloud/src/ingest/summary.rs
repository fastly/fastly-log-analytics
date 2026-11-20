//! Per-window summary Parquet, written by the ingest worker next to each window file.
//!
//! One long-format file per window, rows tagged by `bundle`. Shapes and definitions mirror the
//! fla v3 hourly rollups (`backend/core/rollups/` at 2c75f058): `time_series`, `overview`,
//! `all_fields` (+ `field_totals`), `all_fields_ip` (only for [`IP_SKETCH_FIELDS`]), `sessions` and additive `dims`, plus the
//! Cloud-only `transport`, `origin`, `urls` and `quality` groups and `series5` `origin` and
//! `city` fields (/network transport, quality and map, /origin and /performance rankings), which
//! always write their `dim_totals` rows as a presence mark. Every
//! value is an additive sum or count, a min/max, a histogram or an HLL sketch, so windows merge
//! into hours and days.

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
pub const INS_URL_K: usize = 500;
pub const INS_IP_K: usize = 500;
pub const INS_ASN_K: usize = 500;
pub const ASSET_URL_K: usize = 500;
pub const SHIELD_ROUTE_K: usize = 500;
/// ASNs kept in `series5` per window and hour bundle: 4x the 30 a day bundle writes.
pub const SERIES_ASN_K: usize = 120;
/// Cities kept in `series5` per window and hour bundle (the /network map): 4x the 50 a day
/// bundle writes.
pub const SERIES_CITY_K: usize = 200;
/// `sample` rows per window and bundle.
pub const SAMPLE_K: usize = 300;
/// `header_ip` IPs kept per window and bundle by largest request header: max-merged, the top 10
/// /security shows stay exact.
pub const HEADER_IP_K: usize = 100;
/// `tls_fp` fingerprints and `[fingerprint, country]` pairs kept per window and bundle
/// (/security threat intel shows 50 fingerprints).
pub const TLS_FP_K: usize = 500;
/// `value` bot names, WAF signals and Image Optimizer format pairs kept per window and bundle
/// (/fastly-value shows 20 of each).
pub const VALUE_TOP_K: usize = 200;
/// The `all_fields` field of /security bot efficiency's referral stems (see [`ref_stem`]).
pub const REF_STEM: &str = "_ref_stem";
/// The fields whose `all_fields_ip` distinct-IP sketches a page reads: /security's TLS
/// fingerprints and bot efficiency's referral stems. Others would be unread bytes.
pub const IP_SKETCH_FIELDS: [&str; 2] = ["tls_ciphers_sha", REF_STEM];
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

/// Low-cardinality single-field filter dimensions written into `filtered_rollups`.
pub const FILTERED_ROLLUP_FIELDS: [&str; 8] = [
    "status",
    "cache",
    "pop",
    "country",
    "proto",
    "method",
    "content_type",
    "edge",
];

pub fn filtered_rollup_cap(field: &str) -> usize {
    match field {
        "status" => 64,
        "cache" | "proto" | "method" => 16,
        "pop" => 200,
        "country" => 250,
        "content_type" => 100,
        "edge" => 2,
        _ => 16,
    }
}

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
    DeliveryRateSum = ("delivery_rate_sum", F64),
    DeliveryRateCount = ("delivery_rate_count", I64),
    ElapsedSum = ("elapsed_sum", F64),
    ElapsedCount = ("elapsed_count", I64),
    ObytesHist = ("obytes_hist", Bin),
    OverheadHist = ("overhead_hist", Bin),
    ThroughputSum = ("throughput_sum", F64),
    ThroughputCount = ("throughput_count", I64),
    ValueMax = ("value_max", I64),
    IoInSum = ("io_in_sum", I64),
    IoOutSum = ("io_out_sum", I64),
    RatioSum = ("ratio_sum", F64),
    RatioCount = ("ratio_count", I64),
    RttMin = ("rtt_min", F64),
    TcpRtt = ("tcp_rtt", F64),
    Pop = ("pop", Utf8),
    StatusMin = ("status_min", I64),
    TtlSum = ("ttl_sum", F64),
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

    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
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

fn dashboard_key<'a>(field: &str, key: Key<'a>) -> Option<Key<'a>> {
    match key {
        Key::S("") => None,
        Key::F(bits) if field == "ttl" || field == "age" => {
            Some(Key::I(f64::from_bits(bits).round() as i64))
        }
        Key::I(n) if field == "conn_requests" => {
            if n <= 0 {
                None
            } else {
                Some(Key::S(match n {
                    1 => "1",
                    2..=5 => "2\u{2013}5",
                    6..=20 => "6\u{2013}20",
                    _ => "21+",
                }))
            }
        }
        _ => Some(key),
    }
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
    ttl: Num<'a>,
    ttfb_ms: Num<'a>,
    origin_ms: Num<'a>,
    req_id: Str<'a>,
    rid: Str<'a>,
    prid: Str<'a>,
    /// /network transport's `COALESCE(proto, protocol, 'Unknown')`, RTT and delivery rate.
    transport_proto: Str<'a>,
    transport_rtt: Num<'a>,
    delivery: Num<'a>,
    obytes: Num<'a>,
    /// /network quality's `COALESCE(geo_region, region)`, client-only loss and RTT variance, and
    /// throughput elapsed `COALESCE(time_elapsed_usec, elapsed)`.
    region: Str<'a>,
    client_ploss: Num<'a>,
    client_rttvar: Num<'a>,
    tp_elapsed: Num<'a>,
    /// /security proxies' `COALESCE(rtt_min, tcpi_min_rtt)` and `COALESCE(tcp_rtt, tcpi_rtt)`,
    /// and the plain columns its rows and threat intel read.
    rtt_min: Num<'a>,
    proxy_rtt: Num<'a>,
    pop: Str<'a>,
    plain_city: Str<'a>,
    plain_region: Str<'a>,
    plain_url: Str<'a>,
    referer: Str<'a>,
    ja3: Str<'a>,
    p_type: Str<'a>,
    /// /fastly-value's bot name and Image Optimizer columns.
    bot_name: Str<'a>,
    in_format: Str<'a>,
    out_format: Str<'a>,
    io_in: Num<'a>,
    io_out: Num<'a>,
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
            ttl: Num::new(batch, &[("ttl", 1.0)]),
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
            rid: Str::new(batch, &["rid"]),
            prid: Str::new(batch, &["prid"]),
            transport_proto: Str::new(batch, &["proto", "protocol"]),
            transport_rtt: Num::new(
                batch,
                &[
                    ("tcp_rtt", 1.0),
                    ("client_rtt_us", 1.0),
                    ("client_tcpi_rtt", 1.0),
                    ("tcpi_rtt", 1.0),
                ],
            ),
            delivery: Num::new(
                batch,
                &[("delivery_rate", 1.0), ("tcpi_delivery_rate", 1.0)],
            ),
            obytes: Num::new(batch, &[("obytes", 1.0)]),
            region: Str::new(batch, &["geo_region", "region"]),
            client_ploss: Num::new(batch, &[("client_ploss", 1.0)]),
            client_rttvar: Num::new(batch, &[("client_tcpi_rttvar", 1.0)]),
            tp_elapsed: Num::new(batch, &[("time_elapsed_usec", 1.0), ("elapsed", 1.0)]),
            rtt_min: Num::new(batch, &[("rtt_min", 1.0), ("tcpi_min_rtt", 1.0)]),
            proxy_rtt: Num::new(batch, &[("tcp_rtt", 1.0), ("tcpi_rtt", 1.0)]),
            pop: Str::new(batch, &["pop"]),
            plain_city: Str::new(batch, &["city"]),
            plain_region: Str::new(batch, &["region"]),
            plain_url: Str::new(batch, &["url"]),
            referer: Str::new(batch, &["referer"]),
            ja3: Str::new(batch, &["ja3"]),
            p_type: Str::new(batch, &["p_type"]),
            bot_name: Str::new(batch, &["_bot_name"]),
            in_format: Str::new(batch, &["io_input_format"]),
            out_format: Str::new(batch, &["io_output_format"]),
            io_in: Num::new(batch, &[("io_input_bytes", 1.0)]),
            io_out: Num::new(batch, &[("io_output_bytes", 1.0)]),
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
struct FilteredAgg {
    requests: i64,
    errors: i64,
    s5: i64,
    cache_hits: i64,
    hit_requests: i64,
    miss_requests: i64,
    pass_requests: i64,
    bytes: i64,
    lat: (f64, i64),
    ost5: i64,
    ost: i64,
    rtt: (i64, i64),
    ploss: (f64, i64),
    cwnd: (f64, i64),
    rttvar: (f64, i64),
    retrans: i64,
    elapsed: (f64, i64),
    delivery: (f64, i64),
    hists: [Hist; 5],
}

impl FilteredAgg {
    fn add(&mut self, n: &Net, cache: Option<&str>, elapsed: Option<i64>, delivery: Option<f64>) {
        self.requests += 1;
        self.errors += i64::from(n.status.is_some_and(|s| s >= 400 || s == 0));
        self.s5 += i64::from(n.s5);
        self.cache_hits += i64::from(n.hit);
        self.hit_requests += i64::from(hit(cache));
        self.miss_requests += i64::from(cache.is_some_and(|s| s.starts_with("MISS")));
        self.pass_requests += i64::from(cache.is_some_and(|s| s.starts_with("PASS")));
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
        if let Some(c) = n.cwnd {
            self.cwnd.0 += c;
            self.cwnd.1 += 1;
        }
        if let Some(v) = n.rttvar {
            self.rttvar.0 += v;
            self.rttvar.1 += 1;
        }
        self.retrans += i64::from(n.retrans);
        if let Some(e) = elapsed {
            self.elapsed.0 += e as f64;
            self.elapsed.1 += 1;
        }
        if let Some(d) = delivery {
            self.delivery.0 += d;
            self.delivery.1 += 1;
        }
        add_lat(&mut self.hists, &n.lat, &[0, 1, 2, 3, 4]);
    }

    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str) {
        let blobs = self.hists.each_ref().map(Hist::to_bytes);
        let mut row = vec![
            (C::Bundle, V::S("filtered_rollups")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::Count, V::I(self.requests)),
            (C::Errors, V::I(self.errors)),
            (C::Status5xx, V::I(self.s5)),
            (C::CacheHits, V::I(self.cache_hits)),
            (C::HitRequests, V::I(self.hit_requests)),
            (C::MissRequests, V::I(self.miss_requests)),
            (C::PassRequests, V::I(self.pass_requests)),
            (C::RespBytesSum, V::I(self.bytes)),
            (C::LatUsSum, V::F(self.lat.0)),
            (C::LatUsCount, V::I(self.lat.1)),
            (C::Ost5xxCount, V::I(self.ost5)),
            (C::OstTotalCount, V::I(self.ost)),
            (C::RttSum, V::I(self.rtt.0)),
            (C::RttCount, V::I(self.rtt.1)),
            (C::PlossSum, V::F(self.ploss.0)),
            (C::PlossCount, V::I(self.ploss.1)),
            (C::CwndSum, V::F(self.cwnd.0)),
            (C::CwndCount, V::I(self.cwnd.1)),
            (C::RttvarSum, V::F(self.rttvar.0)),
            (C::RttvarCount, V::I(self.rttvar.1)),
            (C::RetransCount, V::I(self.retrans)),
            (C::ElapsedSum, V::F(self.elapsed.0)),
            (C::ElapsedCount, V::I(self.elapsed.1)),
            (C::DeliveryRateSum, V::F(self.delivery.0)),
            (C::DeliveryRateCount, V::I(self.delivery.1)),
        ];
        push_hists(&mut row, &HIST_COLS, &blobs);
        out.push(&row);
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

#[derive(Default)]
struct AssetUrl {
    large_requests: i64,
    large_bytes: i64,
    large_status: Option<i64>,
    low_requests: i64,
    low_ttl: f64,
}

/// A `transport` row: /network transport's sums over every row.
#[derive(Default)]
struct Transport {
    requests: i64,
    bytes: i64,
    rtt: Hist,
    ploss: (f64, i64),
    delivery: (f64, i64),
}

impl Transport {
    fn add(&mut self, bytes: i64, rtt: Option<(i64, u8)>, ploss: Option<f64>, rate: Option<f64>) {
        self.requests += 1;
        self.bytes += bytes;
        if let Some((v, slot)) = rtt {
            self.rtt.add_slot(v, slot);
        }
        if let Some(p) = ploss {
            self.ploss.0 += p;
            self.ploss.1 += 1;
        }
        if let Some(r) = rate {
            self.delivery.0 += r;
            self.delivery.1 += 1;
        }
    }

    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str) {
        let rtt = self.rtt.to_bytes();
        let mut row = vec![
            (C::Bundle, V::S("transport")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::RespBytesSum, V::I(self.bytes)),
            (C::PlossSum, V::F(self.ploss.0)),
            (C::PlossCount, V::I(self.ploss.1)),
            (C::DeliveryRateSum, V::F(self.delivery.0)),
            (C::DeliveryRateCount, V::I(self.delivery.1)),
        ];
        push_hists(&mut row, &[C::RttHist], std::slice::from_ref(&rtt));
        out.push(&row);
    }
}

/// A `series5` `city` cell: the /network map's requests, 5xx, loss, coordinates and RTT per
/// bucket and `[COALESCE(geo_country, country), COALESCE(geo_city, city)]`.
#[derive(Default)]
struct MapCell {
    requests: i64,
    s5: i64,
    ploss: (f64, i64),
    geo: (f64, f64, i64),
    rtt: Hist,
}

impl MapCell {
    fn add(&mut self, n: &Net<'_>) {
        self.requests += 1;
        self.s5 += i64::from(n.s5);
        if let Some(p) = n.ploss {
            self.ploss.0 += p;
            self.ploss.1 += 1;
        }
        if let Some((la, lo)) = n.geo {
            self.geo.0 += la;
            self.geo.1 += lo;
            self.geo.2 += 1;
        }
        if let Some((v, slot)) = n.lat[2] {
            self.rtt.add_slot(v, slot);
        }
    }

    fn push(&self, out: &mut Out, bucket: i64, value: &str) {
        let rtt = self.rtt.to_bytes();
        let mut row = vec![
            (C::Bundle, V::S("series5")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("city")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::Status5xx, V::I(self.s5)),
            (C::PlossSum, V::F(self.ploss.0)),
            (C::PlossCount, V::I(self.ploss.1)),
            (C::LatSum, V::F(self.geo.0)),
            (C::LonSum, V::F(self.geo.1)),
            (C::GeoCount, V::I(self.geo.2)),
        ];
        push_hists(&mut row, &[C::RttHist], std::slice::from_ref(&rtt));
        out.push(&row);
    }
}

/// A `quality` row: /network quality's sums the other rows lack. `overall` counts every row and
/// its throughput; `region` requests and RTT per `[country, region]`; `asn` the client-only loss
/// and RTT variance, over rows that carry either.
#[derive(Default)]
struct Quality {
    requests: i64,
    rtt: (i64, i64),
    rttvar: (f64, i64),
    ploss: (f64, i64),
    throughput: (f64, i64),
}

impl Quality {
    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str) {
        out.push(&[
            (C::Bundle, V::S("quality")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::RttSum, V::I(self.rtt.0)),
            (C::RttCount, V::I(self.rtt.1)),
            (C::RttvarSum, V::F(self.rttvar.0)),
            (C::RttvarCount, V::I(self.rttvar.1)),
            (C::PlossSum, V::F(self.ploss.0)),
            (C::PlossCount, V::I(self.ploss.1)),
            (C::ThroughputSum, V::F(self.throughput.0)),
            (C::ThroughputCount, V::I(self.throughput.1)),
        ]);
    }
}

/// One row's /origin values, over rows with an origin latency (`COALESCE(ottfb, ttfb * 1e6)`).
struct OriginRow {
    lat: (i64, u8),
    miss: bool,
    pass: bool,
    ost: Option<i64>,
    ottlb: Option<i64>,
    overhead: Option<i64>,
    obytes: Option<i64>,
}

/// An `origin` row: the /origin summary's counts and histograms (`overall`), or requests and
/// latency per path leg (`edge`) or POP (`pop`).
#[derive(Default)]
struct Origin {
    requests: i64,
    miss: i64,
    pass: i64,
    ost5: i64,
    ost: i64,
    lat: Hist,
    ottlb: Hist,
    overhead: Hist,
    obytes: Hist,
}

impl Origin {
    fn add(&mut self, o: &OriginRow, full: bool) {
        self.requests += 1;
        self.lat.add_slot(o.lat.0, o.lat.1);
        if !full {
            return;
        }
        self.miss += i64::from(o.miss);
        self.pass += i64::from(o.pass);
        if let Some(s) = o.ost {
            self.ost += 1;
            self.ost5 += i64::from((500..600).contains(&s));
        }
        for (h, v) in [
            (&mut self.ottlb, o.ottlb),
            (&mut self.overhead, o.overhead),
            (&mut self.obytes, o.obytes),
        ] {
            if let Some(v) = v {
                h.add(v);
            }
        }
    }

    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str, full: bool) {
        let mut row = vec![
            (C::Bundle, V::S("origin")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
        ];
        if full {
            row.extend([
                (C::MissRequests, V::I(self.miss)),
                (C::PassRequests, V::I(self.pass)),
                (C::Ost5xxCount, V::I(self.ost5)),
                (C::OstTotalCount, V::I(self.ost)),
            ]);
        }
        let blobs = [&self.lat, &self.ottlb, &self.overhead, &self.obytes].map(Hist::to_bytes);
        push_hists(
            &mut row,
            &[C::TtfbHist, C::OttlbHist, C::OverheadHist, C::ObytesHist],
            &blobs,
        );
        out.push(&row);
    }
}

/// A `urls` row: per URL or ASN, origin latency for /origin's slow URLs and elapsed time for
/// /performance's rankings.
#[derive(Default)]
struct UrlAgg {
    requests: i64,
    lat: Hist,
    elapsed: Hist,
    elapsed_sum: f64,
}

impl UrlAgg {
    fn add(&mut self, lat: Option<(i64, u8)>, elapsed: Option<(i64, u8)>) {
        self.requests += 1;
        if let Some((v, slot)) = lat {
            self.lat.add_slot(v, slot);
        }
        if let Some((v, slot)) = elapsed {
            self.elapsed.add_slot(v, slot);
            self.elapsed_sum += v as f64;
        }
    }

    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str) {
        let mut row = vec![
            (C::Bundle, V::S("urls")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::ElapsedSum, V::F(self.elapsed_sum)),
            (C::ElapsedCount, V::I(self.elapsed.count() as i64)),
        ];
        let blobs = [&self.lat, &self.elapsed].map(Hist::to_bytes);
        push_hists(&mut row, &[C::TtfbHist, C::ElapsedHist], &blobs);
        out.push(&row);
    }
}

/// A `series5` `origin` row per path leg: origin latency and OTTLB for /origin's series.
#[derive(Default)]
struct OriginSeries {
    requests: i64,
    lat: Hist,
    ottlb: Hist,
}

/// `requests` per value of one untimed field, cut to the `keep` with the most: `(total,
/// truncated)` and the kept values in order.
fn top_untimed<'a, T>(
    map: FastMap<Cow<'a, str>, T>,
    requests: impl Fn(&T) -> i64,
    keep: usize,
) -> (Vec<(Cow<'a, str>, T)>, i64, bool) {
    let total = map.values().map(&requests).sum();
    let truncated = map.len() > keep;
    let mut ranked: Vec<(Cow<'a, str>, T)> = map.into_iter().collect();
    rank(
        &mut ranked,
        |a, b| {
            requests(&b.1)
                .cmp(&requests(&a.1))
                .then_with(|| a.0.cmp(&b.0))
        },
        keep,
    );
    (ranked, total, truncated)
}

/// `true`, `false` or '' (null): the /origin path leg.
fn leg(edge: Option<bool>) -> &'static str {
    match edge {
        Some(true) => "true",
        Some(false) => "false",
        None => "",
    }
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

/// `s` lowercased like DuckDB's `lower`, borrowed when it already is.
fn lower(s: &str) -> Cow<'_, str> {
    if s.is_ascii() {
        ascii_case(s, false)
    } else {
        Cow::Owned(s.to_lowercase())
    }
}

/// /security bot efficiency's user agents that never count as human referrals.
const NOT_HUMAN: [&str; 9] = [
    "bot",
    "crawler",
    "spider",
    "slurp",
    "archiver",
    "scraper",
    "externalagent",
    "extended",
    "headless",
];

/// `regexp_extract(ua_l, '([a-z0-9]+)-(?:user|externalfetcher)', 1)`: the first run of
/// `[a-z0-9]` followed by `-user` or `-externalfetcher`, else ''.
fn agent_stem(ua_l: &str) -> &str {
    let b = ua_l.as_bytes();
    let word = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let mut i = 0;
    while i < b.len() {
        if !word(b[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < b.len() && word(b[i]) {
            i += 1;
        }
        if b[i..].starts_with(b"-user") || b[i..].starts_with(b"-externalfetcher") {
            return &ua_l[start..i];
        }
    }
    ""
}

/// `regexp_extract(ref_l, 'https?://(?:www\.)?([^/.:;\s]+)', 1)` (RE2, leftmost first): the host's
/// first label after the first `http://` or `https://` that has one, `www.` skipped when a label
/// follows it, else ''.
fn referer_stem(ref_l: &str) -> &str {
    let b = ref_l.as_bytes();
    let label = |from: usize| {
        let end = b[from..]
            .iter()
            .position(|c| {
                matches!(
                    c,
                    b'/' | b'.' | b':' | b';' | b' ' | b'\t' | b'\n' | 0x0c | b'\r'
                )
            })
            .map_or(b.len(), |n| from + n);
        &ref_l[from..end]
    };
    for p in 0..b.len() {
        let after = if b[p..].starts_with(b"https://") {
            p + 8
        } else if b[p..].starts_with(b"http://") {
            p + 7
        } else {
            continue;
        };
        if b[after..].starts_with(b"www.") {
            let stem = label(after + 4);
            if !stem.is_empty() {
                return stem;
            }
        }
        let stem = label(after);
        if !stem.is_empty() {
            return stem;
        }
    }
    ""
}

/// /security bot efficiency's referral stem of one row (`buildSecurityBotEfficiencySql`
/// `ref_extracted`), or `None` when the row is left out: no IP, no referral (a referer other than
/// '' and '-', or an agent naming `-user` or `-externalfetcher`), a non-human agent, or no stem.
pub fn ref_stem(ip: Option<&str>, ua: Option<&str>, referer: Option<&str>) -> Option<String> {
    if ip.unwrap_or("").is_empty() {
        return None;
    }
    let ua_l = lower(ua.unwrap_or(""));
    let referer = referer.unwrap_or("");
    let fetcher = ua_l.contains("-user") || ua_l.contains("-externalfetcher");
    if (!fetcher && matches!(referer, "" | "-")) || NOT_HUMAN.iter().any(|w| ua_l.contains(w)) {
        return None;
    }
    let stem = Some(agent_stem(&ua_l))
        .filter(|s| fetcher && !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| referer_stem(&lower(referer)).to_owned());
    (!stem.is_empty()).then_some(stem)
}

/// The live rows of one batch with a [`ref_stem`], and their stems.
fn ref_stems(r: &Rows) -> Vec<(usize, String)> {
    let referer = &r.net.referer;
    r.live()
        .filter_map(|i| {
            let stem = ref_stem(get_s(r.ip, i), get_s(r.ua, i), referer.get(i, false))?;
            Some((i, stem))
        })
        .collect()
}

/// /fastly-value's cache class of `UPPER(COALESCE(NULLIF(cache_status, ''), cache, ''))`: `HIT`,
/// `MISS`, `PASS` or `SYNTH` by prefix, '' when empty, else `OTHER`.
fn cache_class(cache: &str) -> &'static str {
    const CLASSES: [&str; 4] = ["HIT", "MISS", "PASS", "SYNTH"];
    if cache.is_empty() {
        return "";
    }
    let found = if cache.is_ascii() {
        CLASSES.into_iter().find(|p| starts_with_ci(cache, p))
    } else {
        let upper = cache.to_uppercase();
        CLASSES.into_iter().find(|p| upper.starts_with(p))
    };
    found.unwrap_or("OTHER")
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

#[inline]
fn count_key<'a>(aggs: &mut FastMap<Key<'a>, FieldAgg>, key: Key<'a>, ip_id: u32) {
    let agg = aggs.entry(key).or_default();
    agg.count += 1;
    if ip_id != NO_IP {
        agg.ip_rows += 1;
        if agg.ips.last() != Some(&ip_id) {
            agg.ips.push(ip_id);
        }
    }
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

/// `(sample_key, timestamp, batch, row)` of a `sample` row.
type SampleAt = (i64, i64, usize, usize);

/// The v3 rows (`dims`, `series5`, `bins`, `assets`, `transport`, `origin`, `urls`, `quality`
/// and their `dim_totals`) in one pass over the rows, which also picks the `sample` rows, returned
/// in order for [`write_sample`] to write after the other groups.
fn write_v3<'a>(
    rows: &[Rows<'a>],
    bucket: i64,
    out: &mut Out,
) -> (
    Vec<SampleAt>,
    [FastMap<Cow<'a, str>, FilteredAgg>; FILTERED_ROLLUP_FIELDS.len()],
) {
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
    let mut sample: std::collections::BinaryHeap<SampleAt> =
        std::collections::BinaryHeap::with_capacity(SAMPLE_K + 1);
    let mut transport = Transport::default();
    let mut by_transport: FastMap<Cow<str>, Transport> = FastMap::default();
    let mut origin_all = Origin::default();
    let mut origin_legs: FastMap<Cow<str>, Origin> = FastMap::default();
    let mut origin_pops: FastMap<Cow<str>, Origin> = FastMap::default();
    let mut origin_series: FastMap<(i64, Cow<str>), OriginSeries> = FastMap::default();
    let mut by_url: FastMap<Cow<str>, UrlAgg> = FastMap::default();
    let mut by_raw_asn: FastMap<Cow<str>, UrlAgg> = FastMap::default();
    let mut map_cells: FastMap<(i64, Option<&str>, &str), MapCell> = FastMap::default();
    let mut quality = Quality::default();
    let mut quality_regions: FastMap<(&str, &str), Quality> = FastMap::default();
    let mut quality_asns: FastMap<i64, Quality> = FastMap::default();
    let mut filtered: [FastMap<Cow<str>, FilteredAgg>; FILTERED_ROLLUP_FIELDS.len()] =
        Default::default();

    for (at, r) in rows.iter().enumerate() {
        let c = &r.net;
        let field_cols = FIELD_DIMS.map(|d| FieldCol::resolve(r.batch, d));
        let filter_cols = FILTERED_ROLLUP_FIELDS.map(|f| {
            if f == "content_type" {
                None
            } else {
                FieldCol::resolve(r.batch, f)
            }
        });
        let (url_col, asn_col, pop_col) = (
            FieldCol::resolve(r.batch, "url"),
            FieldCol::resolve(r.batch, "asn"),
            FieldCol::resolve(r.batch, "pop"),
        );
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
            let (country, city) = (c.country.get(i, false), c.city.get(i, false));
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
                country: country.unwrap_or("Unknown"),
                city: city.unwrap_or("Unknown"),
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
            if let Some(city) = city {
                map_cells.entry((step, country, city)).or_default().add(&n);
            }

            quality.requests += 1;
            if let Some(e) = c.tp_elapsed.get(i).filter(|e| *e > 0.0) {
                quality.throughput.0 += c.body.get(i).unwrap_or(0.0) * 8.0 / e;
                quality.throughput.1 += 1;
            }
            if let Some(country) = country {
                let q = quality_regions
                    .entry((country, c.region.get(i, false).unwrap_or("Unknown")))
                    .or_default();
                q.requests += 1;
                if let Some(r) = n.rtt {
                    q.rtt.0 += r;
                    q.rtt.1 += 1;
                }
            }
            let (client_ploss, client_rttvar) = (c.client_ploss.get(i), c.client_rttvar.get(i));
            if client_ploss.is_some() || client_rttvar.is_some() {
                let q = quality_asns.entry(n.asn).or_default();
                q.requests += 1;
                if let Some(p) = client_ploss {
                    q.ploss.0 += p;
                    q.ploss.1 += 1;
                }
                if let Some(v) = client_rttvar {
                    q.rttvar.0 += v;
                    q.rttvar.1 += 1;
                }
            }

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

            let ottlb = get_i(r.ottlb, i);
            let origin_lat = lat(n.lat_us.map(|v| v.round() as i64));
            let edge = get_b(r.edge, i);
            let elapsed_lat = n.lat[1];
            let (body, t_rtt, rate) = (
                c.body.get(i).map_or(0, |v| v as i64),
                lat(c.transport_rtt.get(i).map(|v| v.round() as i64)),
                c.delivery.get(i),
            );
            transport.add(body, t_rtt, n.ploss, rate);
            by_transport
                .entry(Cow::Borrowed(
                    c.transport_proto.get(i, false).unwrap_or("Unknown"),
                ))
                .or_default()
                .add(body, t_rtt, n.ploss, rate);
            for (k, col) in filter_cols.iter().enumerate() {
                if let Some(col) = col {
                    col.for_each_key(i, true, |key| {
                        filtered[k]
                            .entry(key.render())
                            .or_default()
                            .add(&n, cache, elapsed, rate);
                    });
                }
            }
            if let Some(raw_ct) = c.content_type.get(i, true) {
                let base = raw_ct.split(';').next().unwrap_or(raw_ct).trim();
                if !base.is_empty() {
                    filtered[6]
                        .entry(ascii_case(base, false))
                        .or_default()
                        .add(&n, cache, elapsed, rate);
                }
            }
            if let Some(latency) = origin_lat {
                let o = OriginRow {
                    lat: latency,
                    miss: cache.is_some_and(|s| s.starts_with("MISS")),
                    pass: cache.is_some_and(|s| s.starts_with("PASS")),
                    ost: n.ost,
                    ottlb,
                    overhead: elapsed.zip(ottlb).map(|(e, l)| e - l),
                    obytes: c.obytes.get(i).map(|v| v.round() as i64),
                };
                origin_all.add(&o, true);
                origin_legs
                    .entry(Cow::Borrowed(leg(edge)))
                    .or_default()
                    .add(&o, false);
                if let Some(col) = &pop_col {
                    col.for_each_key(i, true, |key| {
                        origin_pops.entry(key.render()).or_default().add(&o, false);
                    });
                }
            }
            let s = origin_series
                .entry((step, Cow::Borrowed(leg(edge))))
                .or_default();
            s.requests += 1;
            if let Some((v, slot)) = origin_lat {
                s.lat.add_slot(v, slot);
            }
            if let Some((v, slot)) = n.lat[4] {
                s.ottlb.add_slot(v, slot);
            }
            if let Some(col) = &url_col {
                col.for_each_key(i, false, |key| {
                    by_url
                        .entry(key.render())
                        .or_default()
                        .add(origin_lat, elapsed_lat);
                });
            }
            if let Some(col) = &asn_col {
                col.for_each_key(i, false, |key| {
                    by_raw_asn
                        .entry(key.render())
                        .or_default()
                        .add(None, elapsed_lat);
                });
            }

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
    let cells: FastMap<(i64, Cow<str>), MapCell> = map_cells
        .into_iter()
        .map(|((step, country, city), cell)| {
            let key = serde_json::to_string(&(country, city)).expect("strings serialize");
            ((step, Cow::Owned(key)), cell)
        })
        .collect();
    let (cities, total, truncated) = top_timed(cells, |c| c.requests, SERIES_CITY_K);
    push_total(out, bucket, "series5", "city", total, truncated);
    for (step, value, cell) in &cities {
        cell.push(out, *step, value);
    }
    drop(cities);
    let (legs, total, truncated) = top_timed(origin_series, |s| s.requests, DIM_K);
    push_total(out, bucket, "series5", "origin", total, truncated);
    for (step, value, s) in &legs {
        let blobs = [&s.lat, &s.ottlb].map(Hist::to_bytes);
        let mut row = vec![
            (C::Bundle, V::S("series5")),
            (C::Bucket, V::I(*step)),
            (C::FieldName, V::S("origin")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(s.requests)),
        ];
        push_hists(&mut row, &[C::TtfbHist, C::OttlbHist], &blobs);
        out.push(&row);
    }
    drop(legs);

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

    push_total(
        out,
        bucket,
        "transport",
        "overall",
        transport.requests,
        false,
    );
    transport.push(out, bucket, "overall", "");
    let (kept, total, truncated) = top_untimed(by_transport, |t| t.requests, DIM_K);
    push_total(out, bucket, "transport", "proto", total, truncated);
    for (value, t) in &kept {
        t.push(out, bucket, "proto", value);
    }
    drop(kept);

    push_total(out, bucket, "origin", "overall", origin_all.requests, false);
    origin_all.push(out, bucket, "overall", "", true);
    for (field, map) in [("edge", origin_legs), ("pop", origin_pops)] {
        let (kept, total, truncated) = top_untimed(map, |o| o.requests, DIM_K);
        push_total(out, bucket, "origin", field, total, truncated);
        for (value, o) in &kept {
            o.push(out, bucket, field, value, false);
        }
    }

    for (field, map) in [("url", by_url), ("asn", by_raw_asn)] {
        let (kept, total, truncated) = top_untimed(map, |u| u.requests, DIM_K);
        push_total(out, bucket, "urls", field, total, truncated);
        for (value, u) in &kept {
            u.push(out, bucket, field, value);
        }
    }

    push_total(out, bucket, "quality", "overall", quality.requests, false);
    quality.push(out, bucket, "overall", "");
    let regions: FastMap<Cow<str>, Quality> = quality_regions
        .into_iter()
        .map(|(key, q)| {
            let key = serde_json::to_string(&key).expect("strings serialize");
            (Cow::Owned(key), q)
        })
        .collect();
    let asns: FastMap<Cow<str>, Quality> = quality_asns
        .into_iter()
        .map(|(asn, q)| (Cow::Owned(asn.to_string()), q))
        .collect();
    for (field, map) in [("asn", asns), ("region", regions)] {
        let (kept, total, truncated) = top_untimed(map, |q| q.requests, DIM_K);
        push_total(out, bucket, "quality", field, total, truncated);
        for (value, q) in &kept {
            q.push(out, bucket, field, value);
        }
    }

    (sample.into_sorted_vec(), filtered)
}

fn write_filtered_rollups(
    mut filtered: [FastMap<Cow<'_, str>, FilteredAgg>; FILTERED_ROLLUP_FIELDS.len()],
    bucket: i64,
    out: &mut Out,
) {
    for (k, &field) in FILTERED_ROLLUP_FIELDS.iter().enumerate() {
        let map = std::mem::take(&mut filtered[k]);
        let (kept, total, truncated) = top_untimed(map, |a| a.requests, filtered_rollup_cap(field));
        push_total(out, bucket, "filtered_rollups", field, total, truncated);
        for (value, agg) in &kept {
            agg.push(out, bucket, field, value);
        }
    }
}

fn write_asset_urls(rows: &[Rows], bucket: i64, out: &mut Out) {
    type AssetUrlKey<'a> = (Cow<'a, str>, Cow<'a, str>, Cow<'a, str>);
    let mut large: FastMap<AssetUrlKey<'_>, AssetUrl> = FastMap::default();
    let mut low: FastMap<AssetUrlKey<'_>, AssetUrl> = FastMap::default();
    for r in rows {
        let c = &r.net;
        for i in r.live() {
            let asset_url: Cow<'_, str> = Cow::Owned(c.url.get(i, false).unwrap_or("/").to_owned());
            let kind: Cow<'_, str> = Cow::Borrowed(asset_type(
                c.asset_type.get(i, true),
                c.content_type.get(i, false),
                Some(asset_url.as_ref()),
            ));
            let encoding: Cow<'_, str> = Cow::Owned(
                ascii_case(c.encoding.get(i, true).unwrap_or("unknown"), false).into_owned(),
            );
            let normalized_cache: Cow<'_, str> =
                Cow::Owned(ascii_case(c.cache.get(i, false).unwrap_or("OTHER"), true).into_owned());
            let large_key = (asset_url.clone(), kind.clone(), encoding);
            if matches!(large_key.2.as_ref(), "none" | "identity")
                && matches!(large_key.1.as_ref(), "JavaScript" | "CSS" | "HTML" | "JSON")
            {
                let entry = large.entry(large_key).or_default();
                entry.large_requests += 1;
                entry.large_bytes += c.body.get(i).unwrap_or(0.0) as i64;
                if let Some(status) = get_i(r.status, i) {
                    entry.large_status =
                        Some(entry.large_status.map_or(status, |min| min.min(status)));
                }
            }
            if !normalized_cache.starts_with("HIT") {
                if let Some(ttl) = c.ttl.get(i).filter(|ttl| *ttl < 3600.0) {
                    let entry = low.entry((asset_url, kind, normalized_cache)).or_default();
                    entry.low_requests += 1;
                    entry.low_ttl += ttl;
                }
            }
        }
    }
    for (field, map) in [("large", large), ("low_ttl", low)] {
        let total: i64 = map
            .values()
            .map(|a| {
                if field == "large" {
                    a.large_requests
                } else {
                    a.low_requests
                }
            })
            .sum();
        let mut ranked: Vec<_> = map.into_iter().collect();
        let truncated = ranked.len() > ASSET_URL_K;
        rank(
            &mut ranked,
            |a, b| {
                let ar = if field == "large" {
                    a.1.large_requests
                } else {
                    a.1.low_requests
                };
                let br = if field == "large" {
                    b.1.large_requests
                } else {
                    b.1.low_requests
                };
                br.cmp(&ar).then_with(|| a.0.cmp(&b.0))
            },
            ASSET_URL_K,
        );
        push_total(out, bucket, "asset_urls", field, total, truncated);
        for ((url, kind, value), a) in ranked {
            let key = serde_json::to_string(&[url.as_ref(), kind.as_ref(), value.as_ref()])
                .expect("strings serialize");
            let mut row = vec![
                (C::Bundle, V::S("asset_urls")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(field)),
                (C::Value, V::S(&key)),
                (
                    C::Requests,
                    V::I(if field == "large" {
                        a.large_requests
                    } else {
                        a.low_requests
                    }),
                ),
            ];
            if field == "large" {
                row.extend([
                    (C::RespBytesSum, V::I(a.large_bytes)),
                    (C::StatusMin, a.large_status.map_or(V::Null, V::I)),
                ]);
            } else {
                row.push((C::TtlSum, V::F(a.low_ttl)));
            }
            out.push(&row);
        }
    }
}

fn write_sample(rows: &[Rows], sample: Vec<SampleAt>, out: &mut Out) {
    for (key, ts, at, i) in sample {
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

/// The `value` fields, in write order.
pub const VALUE_FIELDS: [&str; 7] = [
    "cache",
    "shield",
    "io",
    "io_both",
    "bot_name",
    "waf_signal",
    "io_pair",
];

/// Values kept per `value` field per window and bundle.
pub(super) fn value_keep(field: &str) -> usize {
    match field {
        "bot_name" | "waf_signal" | "io_pair" => VALUE_TOP_K,
        _ => DIM_K,
    }
}

const DAY_MS: i64 = 86_400_000;

/// A `bot_ua` or `tls_fp` value: requests, response bytes and the requests with an IP (`bot_ua`)
/// or a proxy type (`tls_fp`).
#[derive(Default)]
struct Tally {
    requests: i64,
    bytes: i64,
    count: i64,
}

/// A `value` row: one value of a /fastly-value field on one UTC day.
#[derive(Default)]
struct ValueAgg {
    requests: i64,
    bytes: (i64, i64),
    elapsed: (f64, i64),
    io: (i64, i64),
    ratio: (f64, i64),
}

impl ValueAgg {
    fn add_body(&mut self, body: Option<f64>) {
        self.requests += 1;
        if let Some(b) = body {
            self.bytes.0 += b as i64;
            self.bytes.1 += 1;
        }
    }

    fn push(&self, out: &mut Out, day: i64, field: &str, value: &str) {
        let mut row = vec![
            (C::Bundle, V::S("value")),
            (C::Bucket, V::I(day)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
        ];
        match field {
            "cache" => row.extend([
                (C::RespBytesSum, V::I(self.bytes.0)),
                (C::Count, V::I(self.bytes.1)),
                (C::ElapsedSum, V::F(self.elapsed.0)),
                (C::ElapsedCount, V::I(self.elapsed.1)),
            ]),
            "io" => row.extend([
                (C::RespBytesSum, V::I(self.bytes.0)),
                (C::Count, V::I(self.bytes.1)),
            ]),
            "io_both" => row.extend([
                (C::IoInSum, V::I(self.io.0)),
                (C::IoOutSum, V::I(self.io.1)),
            ]),
            "io_pair" => row.extend([
                (C::RatioSum, V::F(self.ratio.0)),
                (C::RatioCount, V::I(self.ratio.1)),
            ]),
            _ => {}
        }
        out.push(&row);
    }
}

/// The /security and /fastly-value rows in one pass over the rows, each with its `dim_totals`:
/// - `header_ip` `ip`: the largest request header (`value_max`) per non-empty IP, top
///   [`HEADER_IP_K`] (/security top IPs by header size);
/// - `bot_ua` `ua`: response bytes, requests and requests with a non-empty IP per non-empty user
///   agent, top [`DIM_K`] by bytes (/security bot efficiency and well-known bots);
/// - `tls_fp` `fp`: requests and proxied requests per `COALESCE(NULLIF(ja3, ''), NULLIF(ja4, ''))`,
///   and `fp_country` the requests per JSON `[fp, country]`, each top [`TLS_FP_K`] (/security
///   threat intel);
/// - `value`: per UTC day, [`VALUE_FIELDS`] (/fastly-value caching, performance, security, bots
///   and Image Optimizer);
/// - `proxy` `sample`: the [`SAMPLE_K`] rows with an RTT minimum and the smallest `sample_key`
///   (the zero-padded value), with /security proxies' columns.
fn write_ext(rows: &[Rows], bucket: i64, out: &mut Out) {
    let mut header_ips: FastMap<Cow<str>, i64> = FastMap::default();
    let mut header_rows = 0i64;
    let mut uas: FastMap<Cow<str>, Tally> = FastMap::default();
    let mut fps: FastMap<Cow<str>, Tally> = FastMap::default();
    let mut fp_countries: FastMap<(&str, &str), Tally> = FastMap::default();
    let mut values: [FastMap<(i64, Cow<str>), ValueAgg>; 7] = Default::default();
    let mut pairs: FastMap<(i64, &str, &str), ValueAgg> = FastMap::default();
    let mut proxy: std::collections::BinaryHeap<(i64, usize, usize)> =
        std::collections::BinaryHeap::with_capacity(SAMPLE_K + 1);
    let mut proxy_rows = 0i64;
    for (at, r) in rows.iter().enumerate() {
        let c = &r.net;
        for i in r.live() {
            let day = r.ts.value(i).div_euclid(DAY_MS) * DAY_MS;
            let ip = get_s(r.ip, i).filter(|s| !s.is_empty());
            if let (Some(ip), Some(h)) = (ip, c.header.get(i)) {
                header_rows += 1;
                let h = h as i64;
                header_ips
                    .entry(Cow::Borrowed(ip))
                    .and_modify(|m| *m = (*m).max(h))
                    .or_insert(h);
            }
            if let Some(ua) = get_s(r.ua, i).filter(|s| !s.is_empty()) {
                let t = uas.entry(Cow::Borrowed(ua)).or_default();
                t.requests += 1;
                t.bytes += get_i(r.resp_bytes, i).unwrap_or(0);
                t.count += i64::from(ip.is_some());
            }
            let fp = c
                .ja3
                .get(i, true)
                .or_else(|| get_s(r.ja4, i).filter(|s| !s.is_empty()));
            if let Some(fp) = fp {
                let t = fps.entry(Cow::Borrowed(fp)).or_default();
                t.requests += 1;
                t.count += i64::from(c.p_type.get(i, true).is_some());
                if let Some(country) = get_s(r.country, i) {
                    fp_countries.entry((fp, country)).or_default().requests += 1;
                }
            }

            let class = c.cache.get(i, true).map_or("", cache_class);
            let body = c.body.get(i);
            let edge = get_b(r.edge, i);
            let v = values[0].entry((day, Cow::Borrowed(class))).or_default();
            v.add_body(body);
            if let Some(e) = c.tp_elapsed.get(i) {
                v.elapsed.0 += e;
                v.elapsed.1 += 1;
            }
            if edge == Some(false) {
                values[1]
                    .entry((day, Cow::Borrowed(class)))
                    .or_default()
                    .requests += 1;
            }
            let out_format = c.out_format.get(i, true);
            if out_format.is_some() {
                values[2]
                    .entry((day, Cow::Borrowed(leg(edge))))
                    .or_default()
                    .add_body(body);
            }
            let io = c.io_in.get(i).zip(c.io_out.get(i));
            if let Some((io_in, io_out)) = io {
                let v = values[3].entry((day, Cow::Borrowed(""))).or_default();
                v.requests += 1;
                v.io.0 += io_in as i64;
                v.io.1 += io_out as i64;
            }
            if let Some(name) = c.bot_name.get(i, true) {
                values[4]
                    .entry((day, Cow::Borrowed(name)))
                    .or_default()
                    .requests += 1;
            }
            if let Some(sig) = get_s(c.waf_sig, i) {
                for signal in sig.split(',').filter(|s| !s.is_empty()) {
                    values[5]
                        .entry((day, Cow::Borrowed(signal)))
                        .or_default()
                        .requests += 1;
                }
            }
            if let (Some(input), Some(output)) = (c.in_format.get(i, true), out_format) {
                let v = pairs.entry((day, input, output)).or_default();
                v.requests += 1;
                if let Some((io_in, io_out)) = io.filter(|(_, o)| *o != 0.0) {
                    v.ratio.0 += io_in / io_out;
                    v.ratio.1 += 1;
                }
            }

            if c.rtt_min.get(i).is_some() {
                proxy_rows += 1;
                let key = sample_key(
                    c.req_id.get(i, false),
                    r.ts.value(i),
                    c.url.get(i, false),
                    get_s(r.ip, i),
                    i,
                );
                let entry = (key, at, i);
                if proxy.len() < SAMPLE_K {
                    proxy.push(entry);
                } else if proxy.peek().is_some_and(|top| entry < *top) {
                    proxy.pop();
                    proxy.push(entry);
                }
            }
        }
    }

    let (kept, _, truncated) = top_untimed(header_ips, |m| *m, HEADER_IP_K);
    push_total(out, bucket, "header_ip", "ip", header_rows, truncated);
    for (ip, max) in &kept {
        out.push(&[
            (C::Bundle, V::S("header_ip")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("ip")),
            (C::Value, V::S(ip)),
            (C::ValueMax, V::I(*max)),
        ]);
    }
    drop(kept);

    let total = uas.values().map(|t| t.requests).sum();
    let (kept, _, truncated) = top_untimed(uas, |t| t.bytes, DIM_K);
    push_total(out, bucket, "bot_ua", "ua", total, truncated);
    for (ua, t) in &kept {
        out.push(&[
            (C::Bundle, V::S("bot_ua")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("ua")),
            (C::Value, V::S(ua)),
            (C::RespBytesSum, V::I(t.bytes)),
            (C::Requests, V::I(t.requests)),
            (C::Count, V::I(t.count)),
        ]);
    }
    drop(kept);

    let countries: FastMap<Cow<str>, Tally> = fp_countries
        .into_iter()
        .map(|(key, t)| {
            let key = serde_json::to_string(&key).expect("strings serialize");
            (Cow::Owned(key), t)
        })
        .collect();
    for (field, map) in [("fp", fps), ("fp_country", countries)] {
        let (kept, total, truncated) = top_untimed(map, |t| t.requests, TLS_FP_K);
        push_total(out, bucket, "tls_fp", field, total, truncated);
        for (value, t) in &kept {
            let count = if field == "fp" {
                V::I(t.count)
            } else {
                V::Null
            };
            out.push(&[
                (C::Bundle, V::S("tls_fp")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(field)),
                (C::Value, V::S(value)),
                (C::Requests, V::I(t.requests)),
                (C::Count, count),
            ]);
        }
    }

    values[6] = pairs
        .into_iter()
        .map(|((day, input, output), v)| {
            let key = serde_json::to_string(&(input, output)).expect("strings serialize");
            ((day, Cow::Owned(key)), v)
        })
        .collect();
    for (field, map) in VALUE_FIELDS.iter().zip(values) {
        let (kept, total, truncated) = top_timed(map, |v| v.requests, value_keep(field));
        push_total(out, bucket, "value", field, total, truncated);
        for (day, value, v) in &kept {
            v.push(out, *day, field, value);
        }
    }

    push_total(
        out,
        bucket,
        "proxy",
        "sample",
        proxy_rows,
        proxy_rows > SAMPLE_K as i64,
    );
    let mut last = None;
    for (key, at, i) in proxy.into_sorted_vec() {
        // Rows sharing a key (a repeated request id) are one row, as a bundle merges them.
        if last.replace(key) == Some(key) {
            continue;
        }
        let (r, c) = (&rows[at], &rows[at].net);
        let num = |v: Option<f64>| v.map_or(V::Null, V::F);
        out.push(&[
            (C::Bundle, V::S("proxy")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("sample")),
            (C::Value, V::S(&format!("{key:019}"))),
            (C::Requests, V::I(1)),
            (C::Ip, opt_s(get_s(r.ip, i))),
            (C::Asn, get_i(r.asn, i).map_or(V::Null, V::I)),
            (C::RttMin, num(c.rtt_min.get(i))),
            (C::TcpRtt, num(c.proxy_rtt.get(i))),
            (C::LatSum, num(c.lat.get(i))),
            (C::LonSum, num(c.lon.get(i))),
            (C::Pop, opt_s(c.pop.get(i, false))),
            (C::Country, opt_s(get_s(r.country, i))),
            (C::Label, opt_s(c.plain_city.get(i, false))),
        ]);
    }
}

const AUTH_TOKENS: [&str; 21] = [
    "login",
    "signin",
    "sign-in",
    "log-in",
    "auth",
    "oauth",
    "openid",
    "token",
    "sso",
    "saml",
    "session",
    "account",
    "password",
    "passwd",
    "passwrd",
    "credential",
    "xmlrpc",
    "mfa",
    "otp",
    "2fa",
    "verify",
];

#[derive(Default)]
struct InsUrlAgg {
    requests: i64,
    s5: i64,
    hit: i64,
    miss: i64,
    pass: i64,
}

#[derive(Default)]
struct InsIpAgg {
    denied: i64,
    c404: i64,
}

/// The /insights baseline rows (`ins_url` and `ins_ip`), in [`GROUPS`] order.
fn write_insights(rows: &[Rows], bucket: i64, out: &mut Out) {
    let mut ins_urls: FastMap<Cow<str>, InsUrlAgg> = FastMap::default();
    let mut ins_ips: FastMap<Cow<str>, InsIpAgg> = FastMap::default();
    for r in rows {
        let url_col = FieldCol::resolve(r.batch, "url");
        for i in r.live() {
            let status = get_i(r.status, i);
            let cache = get_s(r.cache, i);
            if let Some(col) = &url_col {
                let s5 = i64::from(status.is_some_and(|s| s >= 500));
                let hit = i64::from(cache.is_some_and(|c| c.starts_with("HIT")));
                let miss = i64::from(cache.is_some_and(|c| c.starts_with("MISS")));
                let pass = i64::from(cache.is_some_and(|c| c.starts_with("PASS")));
                col.for_each_key(i, false, |key| {
                    let u = ins_urls.entry(key.render()).or_default();
                    u.requests += 1;
                    u.s5 += s5;
                    u.hit += hit;
                    u.miss += miss;
                    u.pass += pass;
                });
            }
            let Some(ip) = get_s(r.ip, i).filter(|s| !s.is_empty()) else {
                continue;
            };
            let Some(status) = status else {
                continue;
            };
            if status == 404 {
                ins_ips.entry(Cow::Borrowed(ip)).or_default().c404 += 1;
            } else if matches!(status, 401 | 403) {
                // ASCII case fold differs from RE2's Unicode `i` only for U+212A and U+017F.
                if r.net
                    .plain_url
                    .get(i, true)
                    .is_some_and(|url| AUTH_TOKENS.iter().any(|tok| contains_ci(url, tok)))
                {
                    ins_ips.entry(Cow::Borrowed(ip)).or_default().denied += 1;
                }
            }
        }
    }

    let (kept, total, truncated) = top_untimed(ins_urls, |u| u.requests, INS_URL_K);
    push_total(out, bucket, "ins_url", "url", total, truncated);
    for (value, u) in &kept {
        out.push(&[
            (C::Bundle, V::S("ins_url")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("url")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(u.requests)),
            (C::Status5xx, V::I(u.s5)),
            (C::HitRequests, V::I(u.hit)),
            (C::MissRequests, V::I(u.miss)),
            (C::PassRequests, V::I(u.pass)),
        ]);
    }

    let (kept, total, truncated) = top_untimed(ins_ips, |x| x.denied + x.c404, INS_IP_K);
    push_total(out, bucket, "ins_ip", "ip", total, truncated);
    for (value, x) in &kept {
        out.push(&[
            (C::Bundle, V::S("ins_ip")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("ip")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(x.denied + x.c404)),
            (C::Errors, V::I(x.denied)),
            (C::Count, V::I(x.c404)),
        ]);
    }
}

#[derive(Default)]
struct InsLatAgg {
    requests: i64,
    elapsed: Hist,
}

impl InsLatAgg {
    fn add(&mut self, elapsed: Option<(i64, u8)>) {
        self.requests += 1;
        if let Some((v, slot)) = elapsed {
            self.elapsed.add_slot(v, slot);
        }
    }

    fn push(&self, out: &mut Out, bucket: i64, field: &str, value: &str) {
        let blobs = [self.elapsed.to_bytes()];
        let mut row = vec![
            (C::Bundle, V::S("ins_lat")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S(field)),
            (C::Value, V::S(value)),
            (C::Requests, V::I(self.requests)),
            (C::ElapsedCount, V::I(self.elapsed.count() as i64)),
        ];
        push_hists(&mut row, &[C::ElapsedHist], &blobs);
        out.push(&row);
    }
}

#[derive(Default)]
struct InsAsnAgg {
    requests: i64,
    hosting: i64,
}

/// The /insights latency and ASN hosting baseline rows (`ins_lat` and `ins_asn`), appended after
/// `asset_urls` in [`GROUPS`] order.
fn write_insights_lat_asn(rows: &[Rows], bucket: i64, out: &mut Out) {
    let mut cities: FastMap<(&str, Option<&str>, Option<&str>), InsLatAgg> = FastMap::default();
    let mut pops: FastMap<Cow<str>, InsLatAgg> = FastMap::default();
    let mut asns: FastMap<Cow<str>, InsAsnAgg> = FastMap::default();
    for r in rows {
        let asn_col = FieldCol::resolve(r.batch, "asn");
        for i in r.live() {
            let elapsed_lat = lat(get_i(r.elapsed, i));
            if let Some(city) = r.net.plain_city.get(i, true) {
                let region = r.net.plain_region.get(i, false);
                let country = get_s(r.country, i);
                cities
                    .entry((city, region, country))
                    .or_default()
                    .add(elapsed_lat);
            }
            if let Some(pop) = r.net.pop.get(i, true) {
                pops.entry(Cow::Borrowed(pop)).or_default().add(elapsed_lat);
            }
            if let Some(col) = &asn_col {
                if let Some(p_type) = r.net.p_type.get(i, true) {
                    let hosting = i64::from(p_type == "hosting");
                    col.for_each_key(i, false, |key| {
                        let a = asns.entry(key.render()).or_default();
                        a.requests += 1;
                        a.hosting += hosting;
                    });
                }
            }
        }
    }

    let cities_json: FastMap<Cow<str>, InsLatAgg> = cities
        .into_iter()
        .map(|(k, v)| {
            (
                Cow::Owned(serde_json::to_string(&k).expect("strings serialize")),
                v,
            )
        })
        .collect();
    let (kept_cities, total, truncated) = top_untimed(cities_json, |x| x.requests, SERIES_CITY_K);
    push_total(out, bucket, "ins_lat", "city", total, truncated);
    for (value, x) in &kept_cities {
        x.push(out, bucket, "city", value);
    }

    let (kept_pops, total, truncated) = top_untimed(pops, |x| x.requests, DIM_K);
    push_total(out, bucket, "ins_lat", "pop", total, truncated);
    for (value, x) in &kept_pops {
        x.push(out, bucket, "pop", value);
    }

    let (kept_asns, total, truncated) = top_untimed(asns, |x| x.requests, INS_ASN_K);
    push_total(out, bucket, "ins_asn", "asn", total, truncated);
    for (value, x) in &kept_asns {
        out.push(&[
            (C::Bundle, V::S("ins_asn")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("asn")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(x.requests)),
            (C::Count, V::I(x.hosting)),
        ]);
    }
}

#[derive(Default)]
struct ShieldRouteAgg {
    requests: i64,
    overhead: Hist,
    errors: i64,
}

fn write_shield_routes(rows: &[Rows], bucket: i64, out: &mut Out) {
    let mut shields: FastMap<&str, Vec<(&str, Option<i64>)>> = FastMap::default();
    for r in rows {
        for i in r.live() {
            if leg(get_b(r.edge, i)) != "false" {
                continue;
            }
            let Some(prid) = r.net.prid.get(i, true) else {
                continue;
            };
            let pop = r.net.pop.get(i, false).unwrap_or("");
            let us = get_i(r.ottfb, i).or_else(|| ttfb_us(get_f(r.ttfb, i)));
            shields.entry(prid).or_default().push((pop, us));
        }
    }

    let mut routes: FastMap<Cow<str>, ShieldRouteAgg> = FastMap::default();
    for r in rows {
        for i in r.live() {
            if leg(get_b(r.edge, i)) != "true" {
                continue;
            }
            let Some(rid) = r.net.rid.get(i, true) else {
                continue;
            };
            let Some(matched) = shields.get(rid) else {
                continue;
            };
            let e_pop = r.net.pop.get(i, false).unwrap_or("");
            let e_us = get_i(r.ottfb, i).or_else(|| ttfb_us(get_f(r.ttfb, i)));
            for &(s_pop, s_us) in matched {
                let key = serde_json::to_string(&(e_pop, s_pop)).expect("strings serialize");
                let agg = routes.entry(Cow::Owned(key)).or_default();
                agg.requests += 1;
                if let (Some(e), Some(s)) = (e_us, s_us) {
                    let delta = e - s;
                    agg.overhead.add(delta);
                    if delta < 0 {
                        agg.errors += 1;
                    }
                }
            }
        }
    }

    let (kept, total, truncated) = top_untimed(routes, |x| x.requests, SHIELD_ROUTE_K);
    push_total(out, bucket, "shield_routes", "route", total, truncated);
    for (value, x) in &kept {
        let mut row = vec![
            (C::Bundle, V::S("shield_routes")),
            (C::Bucket, V::I(bucket)),
            (C::FieldName, V::S("route")),
            (C::Value, V::S(value)),
            (C::Requests, V::I(x.requests)),
            (C::Errors, V::I(x.errors)),
        ];
        let blobs = [&x.overhead].map(Hist::to_bytes);
        push_hists(&mut row, &[C::OverheadHist], &blobs);
        out.push(&row);
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
    let stems: Vec<Vec<(usize, String)>> = rows.iter().map(ref_stems).collect();
    let mut extra: Vec<&str> = custom.iter().map(String::as_str).collect();
    extra.push(REF_STEM);
    extra.sort_unstable();
    let mut aggs: FastMap<Key, FieldAgg> = FastMap::default();
    for field in FIELDS.iter().copied().chain(extra) {
        let sketched = IP_SKETCH_FIELDS.contains(&field);
        for (r, stems) in rows.iter().zip(&stems) {
            if field == REF_STEM {
                for (i, stem) in stems {
                    count_key(&mut aggs, Key::S(stem), r.ip_id[*i]);
                }
                continue;
            }
            let Some(col) = FieldCol::resolve(r.batch, field) else {
                continue;
            };
            for i in r.live() {
                let ip_id = if sketched { r.ip_id[i] } else { NO_IP };
                col.for_each_key(i, true, |key| {
                    if let Some(key) = dashboard_key(field, key) {
                        count_key(&mut aggs, key, ip_id);
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

    let (sample, filtered) = write_v3(&rows, bucket, &mut out);
    write_ext(&rows, bucket, &mut out);
    write_insights(&rows, bucket, &mut out);
    write_asset_urls(&rows, bucket, &mut out);
    write_insights_lat_asn(&rows, bucket, &mut out);
    write_shield_routes(&rows, bucket, &mut out);
    write_filtered_rollups(filtered, bucket, &mut out);
    write_sample(&rows, sample, &mut out);

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
