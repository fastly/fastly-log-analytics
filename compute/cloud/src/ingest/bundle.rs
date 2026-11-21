//! Hour and day summary bundles (Task 22.2): the `summary_bundle` maintenance stage merges a
//! closed hour's per-window summaries ([`super::summary`]) into one hour bundle, then a closed
//! day's hour bundles into one day bundle, like fla v3's `hour_bundled`/`day_bundled` rollups.
//! The open hour stays per-window; the browser merges it.
//!
//! Layout, under `{scope}processed/`:
//! - windows: `summary/{table}/year=Y/month=M/day=D/hour=HH/summary={a}-{b}-{plan}s{shard}-n{rows}.parquet`
//! - hour: `summary_bundle/{table}/year=Y/month=M/day=D/hour_bundle=HH-c{n}-n{rows}-h{fp}.parquet`
//! - day: `summary_bundle/{table}/year=Y/month=M/day=D/day_bundle=c{n}-n{rows}-h{fp}.parquet`
//! - resumable partials (never read by readers): `partial=c{k}-n{rows}-h{fp}.parquet` in the
//!   hour's window folder (hour builds) or the day's `summary_bundle` folder (day builds). The
//!   state is zstd-compressed and split so no object passes `CDN_READ_LIMIT`: the head holds the
//!   first slice, `partial=c{k}-n{rows}-h{fp}-p{i}.parquet` the rest. A partial that cannot be
//!   read is reported and skipped, and goes with the other stale partials at the next write.
//!
//! A day's hour and day bundles share one folder, so one LIST resolves a closed day and one
//! retention dataset covers both.
//!
//! `c` counts the sources, `n` sums their `-n` rows, and `fp` is [`fingerprint`] over the sorted
//! source file names, so a name says exactly which sources it covers and the same sources always
//! give the same key and bytes. Hour sources are window summaries; day sources are hour bundles.
//!
//! Merge: sums add, min/max keep min/max, HLL sketches max-merge, `sessions` merge by
//! `(ip, ja4)`, `time_series` sums per minute and `overview` keeps one row per hour. Sources fold
//! one at a time in name order and the state is cut after each fold to `summary::TOP_K` values per
//! field (sketches for the top `summary::IP_TOP_K`, only for `summary::IP_SKETCH_FIELDS`; older
//! sources' other sketches are dropped), `SESSION_K` sessions and `DIM_K` values per
//! dim, so a build resumed from a partial gives the same bytes as one that never stopped. The
//! bundle keeps the top [`HOUR_TOP_K`]/[`DAY_TOP_K`] values per field by `(count desc, value
//! asc)`; `field_totals.truncated` marks any field that lost values.
//!
//! v3 groups (`dims`, `series5`, `bins`, `assets`, `transport`, `origin`, `urls`, `quality`) sum per
//! value, histograms add bucket-wise,
//! and `series5`/`bins` buckets floor to 5 minutes (hour) or an hour (day). Folding keeps 4x
//! candidates (`DIM_K`, `summary::SERIES_ASN_K`, `summary::SERIES_CITY_K`) by summed requests.
//! An hour bundle writes all of them, because day bundles fold hour bundles; a day bundle writes
//! its top [`DAY_TOP_K`] ([`SERIES_ASN_TOP_K`] for `series5` ASNs, [`SERIES_CITY_TOP_K`] for
//! `series5` cities). `dim_totals.truncated` marks a group field that
//! lost values, whose counts are then lower bounds. Fields added after version 3 shipped
//! (`transport`, `origin`, `urls`, `quality` and `series5` `origin` and `city`) are written only
//! when every source
//! carried their `dim_totals` row, so a bundle over older windows lacks them and readers fall
//! back to raw. The /security and /fastly-value groups (`header_ip` keeps the max per IP,
//! `bot_ua`, `tls_fp` and per-day `value` sum, `proxy` keeps one row per sample key and its
//! `summary::SAMPLE_K` smallest) are gated the same way, as is the `_ref_stem` field, a
//! non-catalog field. `sample` keeps the
//! `summary::SAMPLE_K` rows with the smallest `sample_key`. A source of another summary version
//! makes the bundle `mixed`, which readers treat as unknown.

use super::{
    hist::Hist,
    hour_dir, raw_key_minute, raw_prefix, scope,
    storage::{Object, Store, CDN_READ_LIMIT},
    summary::{self, Hll, Out, Ty, C, COLS, DIMS, FIELDS, V},
    WATERMARK_KEY,
};
use crate::shell::Runtime;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::metadata::KeyValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
};

/// Values kept per field in an hour bundle (fla `TOP_K`).
pub const HOUR_TOP_K: usize = 500;
/// Values kept per field in a day bundle (fla `DAY_BUNDLE_TOP_K`).
pub const DAY_TOP_K: usize = 500;
/// ASNs a day bundle writes per `series5` bucket set: the 30 a page shows by default.
pub const SERIES_ASN_TOP_K: usize = 30;
/// Cities a day bundle writes in `series5` (the /network map).
pub const SERIES_CITY_TOP_K: usize = 50;
/// Sources folded per execution, which bounds heap and time per build step.
pub const MAX_SOURCES: usize = 24;
/// Minimum unfolded window summaries in an open hour before pre-folding a `partial=` batch.
const OPEN_BATCH_SOURCES: usize = 18;
/// Minimum new closed windows in an open hour before writing an `open_bundle=` Parquet snapshot.
const OPEN_BUNDLE_MIN: usize = 6;
/// An hour is closed once ingest has listed raw objects this long past its end plus the log
/// period, so its windows are all written (late ones are caught by the revisit).
pub const CLOSE_SLACK_S: i64 = 600;
/// A fresh state starts this many days back (or the request retention, if shorter).
pub const LOOKBACK_DAYS: i64 = 7;
/// Bundled hours this far behind the forward cursor are re-listed for late windows ...
pub const REVISIT_S: i64 = 48 * 3600;
/// ... this often.
pub const REVISIT_INTERVAL_S: i64 = 6 * 3600;
/// A replaced bundle is deleted this long after its replacement is written, so a reader that
/// listed it can still read it.
pub const RETIRE_GRACE_S: i64 = 600;
pub const STATE_KEY: &str = "summary:bundle_state";
const TABLE: &str = "request";
const HOUR_S: i64 = 3600;
const DAY_S: i64 = 86_400;
/// Stop starting work this far into the execution; the Compute limit is 60 s.
const WALL_STOP: std::time::Duration = std::time::Duration::from_secs(40);
/// Stop folding once this much vCPU is spent; a v3 partial encode (~250ms) must still fit under 900ms.
pub const VCPU_STOP_MS: u64 = 600;
/// Start a final serialization (~250ms plus the tail) only below this, keeping it under 900ms.
pub const WRITE_VCPU_STOP_MS: u64 = 630;
const PARTIAL_MAGIC: &[u8; 5] = b"FLPB\x09";
/// A packed partial: [`PACKED_MAGIC`], the object count (u32 LE) and the raw length (u64 LE),
/// then the first slice of the zstd-compressed [`PARTIAL_MAGIC`] body; the rest follows in
/// [`part_key`] objects, each at most the CDN read limit.
const PACKED_MAGIC: &[u8; 5] = b"FLPB\x04";
const PACKED_HEAD: usize = PACKED_MAGIC.len() + 4 + 8;
/// A raw partial is a few times its bundle; larger means a corrupt length.
const PARTIAL_RAW_MAX: u64 = 256 << 20;

/// Compresses an encoded partial and splits it into objects of at most `limit` bytes, head first.
fn pack_partial(raw: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, String> {
    let packed = zstd::bulk::compress(raw, 1).map_err(|_| "partial_encode_failed")?;
    let mut rest = packed.as_slice();
    let mut objects = Vec::new();
    let mut head = Vec::with_capacity(limit.min(PACKED_HEAD + rest.len()));
    head.extend_from_slice(PACKED_MAGIC);
    head.extend_from_slice(&[0; 4]);
    head.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    let mut object = head;
    loop {
        let take = rest.len().min(limit - object.len());
        object.extend_from_slice(&rest[..take]);
        rest = &rest[take..];
        objects.push(object);
        if rest.is_empty() {
            break;
        }
        object = Vec::with_capacity(limit.min(rest.len()));
    }
    let count = u32::try_from(objects.len()).map_err(|_| "partial_encode_failed")?;
    objects[0][PACKED_MAGIC.len()..PACKED_MAGIC.len() + 4].copy_from_slice(&count.to_le_bytes());
    Ok(objects)
}

/// The object count of a packed partial head.
fn packed_parts(head: &[u8]) -> Option<usize> {
    if head.len() < PACKED_HEAD || !head.starts_with(PACKED_MAGIC) {
        return None;
    }
    let count = u32::from_le_bytes(head[PACKED_MAGIC.len()..][..4].try_into().ok()?);
    usize::try_from(count).ok().filter(|n| *n > 0)
}

/// The encoded partial from all of a packed partial's objects, head first.
fn unpack_partial(objects: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let head = objects.first().ok_or("partial_unreadable")?;
    if packed_parts(head) != Some(objects.len()) {
        return Err("partial_unreadable".into());
    }
    let raw_len = u64::from_le_bytes(
        head[PACKED_MAGIC.len() + 4..PACKED_HEAD]
            .try_into()
            .unwrap(),
    );
    if raw_len > PARTIAL_RAW_MAX {
        return Err("partial_unreadable".into());
    }
    let mut packed = head[PACKED_HEAD..].to_vec();
    for object in &objects[1..] {
        packed.extend_from_slice(object);
    }
    let raw =
        zstd::bulk::decompress(&packed, raw_len as usize).map_err(|_| "partial_unreadable")?;
    if raw.len() as u64 != raw_len || !raw.starts_with(PARTIAL_MAGIC) {
        return Err("partial_unreadable".into());
    }
    Ok(raw)
}

/// Object `i` (from 1) of the packed partial at `head`; [`parse_name`] rejects it.
fn part_key(head: &str, i: usize) -> String {
    format!("{}-p{i}.parquet", head.trim_end_matches(".parquet"))
}

/// A partial that can never be read as it is, so a build skips it rather than retrying it.
fn unreadable(error: &str) -> bool {
    matches!(error, "cdn_body_too_large" | "get_body_too_large")
}

/// `{scope}processed/summary_bundle/{table}/`, holding hour and day bundles.
pub fn bundle_prefix(scope: &str, table: &str) -> String {
    format!("{scope}processed/summary_bundle/{table}/")
}

/// FNV-1a 64 over each file name followed by `\n`, in the order given (callers sort).
pub fn fingerprint<'a>(names: impl IntoIterator<Item = &'a str>) -> u64 {
    compute_common::crypto::fnv1a64(
        names
            .into_iter()
            .flat_map(|name| name.bytes().chain(*b"\n")),
    )
}

fn file_name(key: &str) -> &str {
    super::file_name(key)
}

/// `year=Y/month=M/day=D/` of `ts`.
pub fn day_dir(ts: i64) -> String {
    super::day_dir(ts)
}

/// Rows from a source name's `-n{rows}` suffix; 0 when absent.
fn name_rows(key: &str) -> u64 {
    super::parse_name_rows(key).unwrap_or(0)
}

/// Sorted file names of `keys`.
fn names(keys: &[String]) -> Vec<&str> {
    let mut names: Vec<&str> = keys.iter().map(|k| file_name(k)).collect();
    names.sort_unstable();
    names
}

fn covering(names: &[&str], rows: u64) -> String {
    format!(
        "c{}-n{rows}-h{:016x}.parquet",
        names.len(),
        fingerprint(names.iter().copied())
    )
}

/// The hour bundle covering the window summaries `windows` of the hour at `hour_start`.
pub fn hour_bundle_key(hour_prefix: &str, hour_start: i64, windows: &[String]) -> String {
    let rows = windows.iter().map(|k| name_rows(k)).sum();
    format!(
        "{hour_prefix}{}hour_bundle={:02}-{}",
        day_dir(hour_start),
        hour_start.rem_euclid(DAY_S) / HOUR_S,
        covering(&names(windows), rows)
    )
}

/// The open-hour partial bundle covering `windows` up to `end_ms` (milliseconds).
pub fn open_bundle_key(
    hour_prefix: &str,
    hour_start: i64,
    end_ms: i64,
    windows: &[String],
) -> String {
    let rows = windows.iter().map(|k| name_rows(k)).sum();
    format!(
        "{hour_prefix}{}open_bundle={:02}-{end_ms}-{}",
        day_dir(hour_start),
        hour_start.rem_euclid(DAY_S) / HOUR_S,
        covering(&names(windows), rows)
    )
}

/// The day bundle covering the hour bundles `hours` of the day at `day_start`.
pub fn day_bundle_key(day_prefix: &str, day_start: i64, hours: &[String]) -> String {
    let rows = hours.iter().map(|k| name_rows(k)).sum();
    format!(
        "{day_prefix}{}day_bundle={}",
        day_dir(day_start),
        covering(&names(hours), rows)
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameKind {
    Hour,
    Open,
    Day,
    Partial,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleName {
    pub kind: NameKind,
    /// The hour (`HH`) of an hour/open bundle, the day of month of a day bundle, 0 for a partial.
    pub unit: u32,
    /// End timestamp in milliseconds for `NameKind::Open`.
    pub end_ms: Option<i64>,
    pub count: usize,
    pub rows: u64,
    pub fp: u64,
}

/// Parses an hour bundle, open bundle, day bundle or partial key.
pub fn parse_name(key: &str) -> Option<BundleName> {
    let name = file_name(key).strip_suffix(".parquet")?;
    let (kind, unit, end_ms, rest) = if let Some(rest) = name.strip_prefix("hour_bundle=") {
        let (hh, rest) = rest.split_once('-')?;
        (NameKind::Hour, hh.parse().ok()?, None, rest)
    } else if let Some(rest) = name.strip_prefix("open_bundle=") {
        let (hh, rest) = rest.split_once('-')?;
        let (end_ms_str, rest) = rest.split_once('-')?;
        let end_ms: i64 = end_ms_str.parse().ok()?;
        (NameKind::Open, hh.parse().ok()?, Some(end_ms), rest)
    } else if let Some(rest) = name.strip_prefix("day_bundle=") {
        let day = key.rsplit('/').nth(1)?.strip_prefix("day=")?.parse().ok()?;
        (NameKind::Day, day, None, rest)
    } else {
        (NameKind::Partial, 0, None, name.strip_prefix("partial=")?)
    };
    let mut parts = rest.split('-');
    let count = parts.next()?.strip_prefix('c')?.parse().ok()?;
    let rows = parts.next()?.strip_prefix('n')?.parse().ok()?;
    let fp = u64::from_str_radix(parts.next()?.strip_prefix('h')?, 16).ok()?;
    if parts.next().is_some() || (matches!(kind, NameKind::Hour | NameKind::Open) && unit > 23) {
        return None;
    }
    Some(BundleName {
        kind,
        unit,
        end_ms,
        count,
        rows,
        fp,
    })
}

/// What a reader reads for one closed hour.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HourView {
    /// The hour bundle that covers the listed windows it was built from.
    pub bundle: Option<String>,
    /// Window summaries to read as well: every window without a valid bundle, else the late
    /// windows written after it.
    pub windows: Vec<String>,
}

fn newest_first(a: &Object, b: &Object) -> Ordering {
    b.last_modified
        .cmp(&a.last_modified)
        .then_with(|| b.key.cmp(&a.key))
}

/// Resolves hour `hour` from a LIST of its window folder (`windows`) and a LIST of its day's
/// `summary_bundle` folder (`bundles`). A bundle is valid when the windows listed no later than
/// its `LastModified` are exactly its sources (`c` and `fp`); later windows are read on top. With
/// no valid bundle every window is read.
pub fn hour_view(hour: u32, windows: &[Object], bundles: &[Object]) -> HourView {
    let windows: Vec<&Object> = windows
        .iter()
        .filter(|o| file_name(&o.key).starts_with("summary="))
        .collect();
    let mut candidates: Vec<&Object> = bundles
        .iter()
        .filter(|o| parse_name(&o.key).is_some_and(|n| n.kind == NameKind::Hour && n.unit == hour))
        .collect();
    candidates.sort_by(|a, b| newest_first(a, b));
    for bundle in candidates {
        let name = parse_name(&bundle.key).expect("filtered");
        let (covered, late): (Vec<&Object>, Vec<&Object>) =
            windows
                .iter()
                .partition(|w| match (&w.last_modified, &bundle.last_modified) {
                    (Some(w), Some(b)) => w <= b,
                    _ => true,
                });
        let mut covered: Vec<&str> = covered.iter().map(|o| file_name(&o.key)).collect();
        covered.sort_unstable();
        if covered.len() == name.count && fingerprint(covered.iter().copied()) == name.fp {
            return HourView {
                bundle: Some(bundle.key.clone()),
                windows: late.iter().map(|o| o.key.clone()).collect(),
            };
        }
    }
    HourView {
        bundle: None,
        windows: windows.iter().map(|o| o.key.clone()).collect(),
    }
}

/// The current hour bundle per hour of a `summary_bundle` day folder listing: the newest by
/// `LastModified` (a replaced bundle lingers for [`RETIRE_GRACE_S`]).
pub fn current_hours(listing: &[Object]) -> BTreeMap<u32, Object> {
    let mut out: BTreeMap<u32, Object> = BTreeMap::new();
    for o in listing {
        let Some(name) = parse_name(&o.key).filter(|n| n.kind == NameKind::Hour) else {
            continue;
        };
        match out.get(&name.unit) {
            Some(cur) if newest_first(o, cur) != Ordering::Less => {}
            _ => {
                out.insert(name.unit, o.clone());
            }
        }
    }
    out
}

/// The day bundle that covers exactly the current hour bundles of its day, if any, from a LIST
/// of the day's `summary_bundle` folder.
pub fn day_view(listing: &[Object]) -> Option<String> {
    let current = current_hours(listing);
    let mut names: Vec<&str> = current.values().map(|o| file_name(&o.key)).collect();
    names.sort_unstable();
    let fp = fingerprint(names.iter().copied());
    let mut days: Vec<&Object> = listing
        .iter()
        .filter(|o| parse_name(&o.key).is_some_and(|n| n.kind == NameKind::Day))
        .collect();
    days.sort_by(|a, b| newest_first(a, b));
    days.into_iter()
        .find(|o| parse_name(&o.key).is_some_and(|n| n.count == names.len() && n.fp == fp))
        .map(|o| o.key.clone())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    Hour,
    Day,
}

impl Unit {
    fn top_k(self) -> usize {
        match self {
            Unit::Hour => HOUR_TOP_K,
            Unit::Day => DAY_TOP_K,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Unit::Hour => "hour",
            Unit::Day => "day",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Null,
    I(i64),
    F(f64),
    S(String),
    /// A non-empty latency histogram.
    H(Box<Hist>),
}

#[derive(Clone, Copy)]
enum Op {
    Sum,
    Min,
    Max,
    /// Histograms add bucket by bucket.
    Merge,
}

use Op::{Max, Merge, Min, Sum};

const TS_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::Status4xx, Sum),
    (C::Status5xx, Sum),
    (C::Hits, Sum),
    (C::CacheTotal, Sum),
    (C::RespBytesSum, Sum),
    (C::TtfbSum, Sum),
    (C::TtfbCount, Sum),
];

const OV_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::HitRequests, Sum),
    (C::MissRequests, Sum),
    (C::PassRequests, Sum),
    (C::SynthRequests, Sum),
    (C::OriginRequests, Sum),
    (C::BandwidthSaved, Sum),
    (C::TotalBandwidth, Sum),
    (C::ShieldHits, Sum),
    (C::ShieldTotal, Sum),
    (C::ThreatsBlocked, Sum),
    (C::HitElapsedSum, Sum),
    (C::HitElapsedCount, Sum),
    (C::MissElapsedSum, Sum),
    (C::MissElapsedCount, Sum),
    (C::HitTtfbSum, Sum),
    (C::HitTtfbCount, Sum),
    (C::MissTtfbSum, Sum),
    (C::MissTtfbCount, Sum),
    (C::WaterfallCount, Sum),
    (C::EdgeProcessingSum, Sum),
    (C::OriginWaitSum, Sum),
    (C::OriginDownloadSum, Sum),
    (C::ClientDownloadSum, Sum),
    (C::TtfbHist, Merge),
    (C::ElapsedHist, Merge),
    (C::RttHist, Merge),
    (C::OttfbHist, Merge),
    (C::OttlbHist, Merge),
];

const SESSION_COLS: &[(C, Op)] = &[
    (C::FirstTs, Min),
    (C::LastTs, Max),
    (C::ReqCount, Sum),
    (C::Country, Min),
    (C::Asn, Min),
    (C::Reqs4xx, Sum),
    (C::Reqs5xx, Sum),
    (C::TotalBytes, Sum),
    (C::RttSum, Sum),
    (C::RttCount, Sum),
    (C::EdgeCount, Sum),
    (C::ShieldCount, Sum),
    (C::UaMin, Min),
    (C::EdgeSidMax, Max),
    (C::CmcdCount, Sum),
];
/// Index of `req_count` in `SESSION_COLS`.
const SESSION_REQS: usize = 2;

const DIM_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::Errors, Sum),
    (C::CacheHits, Sum),
    (C::RespBytesSum, Sum),
    (C::LatUsSum, Sum),
    (C::LatUsCount, Sum),
    (C::Ost5xxCount, Sum),
    (C::OstTotalCount, Sum),
    (C::RttSum, Sum),
    (C::RttCount, Sum),
    (C::PlossSum, Sum),
    (C::PlossCount, Sum),
    (C::Status5xx, Sum),
    (C::CwndSum, Sum),
    (C::CwndCount, Sum),
    (C::RttvarSum, Sum),
    (C::RttvarCount, Sum),
    (C::RetransCount, Sum),
    (C::LatSum, Sum),
    (C::LonSum, Sum),
    (C::GeoCount, Sum),
    (C::Label, Min),
    (C::TtfbHist, Merge),
    (C::RttHist, Merge),
    (C::OttfbHist, Merge),
    (C::OttlbHist, Merge),
];

const SERIES_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::Status5xx, Sum),
    (C::RespBytesSum, Sum),
    (C::RttSum, Sum),
    (C::RttCount, Sum),
    (C::PlossSum, Sum),
    (C::PlossCount, Sum),
    (C::LatSum, Sum),
    (C::LonSum, Sum),
    (C::GeoCount, Sum),
    (C::Label, Min),
    (C::TtfbHist, Merge),
    (C::ElapsedHist, Merge),
    (C::RttHist, Merge),
    (C::OttfbHist, Merge),
    (C::OttlbHist, Merge),
];

const BIN_COLS: &[(C, Op)] = &[(C::Requests, Sum)];

const TRANSPORT_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::RespBytesSum, Sum),
    (C::PlossSum, Sum),
    (C::PlossCount, Sum),
    (C::DeliveryRateSum, Sum),
    (C::DeliveryRateCount, Sum),
    (C::RttHist, Merge),
];

const ORIGIN_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::MissRequests, Sum),
    (C::PassRequests, Sum),
    (C::Ost5xxCount, Sum),
    (C::OstTotalCount, Sum),
    (C::TtfbHist, Merge),
    (C::OttlbHist, Merge),
    (C::OverheadHist, Merge),
    (C::ObytesHist, Merge),
];

const URL_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::ElapsedSum, Sum),
    (C::ElapsedCount, Sum),
    (C::TtfbHist, Merge),
    (C::ElapsedHist, Merge),
];

const QUALITY_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::RttSum, Sum),
    (C::RttCount, Sum),
    (C::RttvarSum, Sum),
    (C::RttvarCount, Sum),
    (C::PlossSum, Sum),
    (C::PlossCount, Sum),
    (C::ThroughputSum, Sum),
    (C::ThroughputCount, Sum),
];

const ASSET_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::RespBytesSum, Sum),
    (C::TtfbSum, Sum),
    (C::OriginWaitSum, Sum),
    (C::OriginRequests, Sum),
];
const ASSET_URL_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::RespBytesSum, Sum),
    (C::StatusMin, Min),
    (C::TtlSum, Sum),
];

const HEADER_IP_COLS: &[(C, Op)] = &[(C::ValueMax, Max)];

const BOT_UA_COLS: &[(C, Op)] = &[(C::RespBytesSum, Sum), (C::Requests, Sum), (C::Count, Sum)];

const TLS_FP_COLS: &[(C, Op)] = &[(C::Requests, Sum), (C::Count, Sum)];

const VALUE_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::RespBytesSum, Sum),
    (C::Count, Sum),
    (C::ElapsedSum, Sum),
    (C::ElapsedCount, Sum),
    (C::IoInSum, Sum),
    (C::IoOutSum, Sum),
    (C::RatioSum, Sum),
    (C::RatioCount, Sum),
];

/// One row per sample key (`value`); `requests` is always 1, so ranking keeps the smallest keys.
/// Floats have no `Min`, so they keep the first source's value, like the strings' `Min` keeps
/// one of equal rows.
const PROXY_COLS: &[(C, Op)] = &[
    (C::Requests, Min),
    (C::Ip, Min),
    (C::Asn, Min),
    (C::RttMin, Min),
    (C::TcpRtt, Min),
    (C::LatSum, Min),
    (C::LonSum, Min),
    (C::Pop, Min),
    (C::Country, Min),
    (C::Label, Min),
];

const SAMPLE_COLS: &[(C, Op)] = &[
    (C::Value, Min),
    (C::TtfbSum, Sum),
    (C::ElapsedUs, Sum),
    (C::RttSum, Sum),
];

/// `ins_url` (`url` field): `Status5xx` is `status >= 500`; `HitRequests`/`MissRequests`/
/// `PassRequests` use case-sensitive `starts_with` on the raw `cache` column.
const INS_URL_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::Status5xx, Sum),
    (C::HitRequests, Sum),
    (C::MissRequests, Sum),
    (C::PassRequests, Sum),
];

/// `ins_ip` (`ip` field): `Errors` = denied auth (status 401/403 and auth URL), `Count` = 404s,
/// `Requests` = `Errors + Count`.
const INS_IP_COLS: &[(C, Op)] = &[(C::Requests, Sum), (C::Errors, Sum), (C::Count, Sum)];

/// `ins_lat` (`city` and `pop` fields): /insights latency regression baselines (`ElapsedHist` in µs).
const INS_LAT_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::ElapsedCount, Sum),
    (C::ElapsedHist, Merge),
];

/// `ins_asn` (`asn` field): `Requests` = rows with non-empty `p_type`, `Count` =
/// `p_type == "hosting"`.
const INS_ASN_COLS: &[(C, Op)] = &[(C::Requests, Sum), (C::Count, Sum)];

/// `shield_routes` (`route` field): window-local join of edge `rid` to shield `prid`;
/// `OverheadHist` holds `(edge_ottfb_us - shield_ottfb_us)` and `Errors` counts negative deltas.
const SHIELD_ROUTES_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::OverheadHist, Merge),
    (C::Errors, Sum),
];

const FILTERED_ROLLUPS_COLS: &[(C, Op)] = &[
    (C::Requests, Sum),
    (C::Count, Sum),
    (C::Errors, Sum),
    (C::Status5xx, Sum),
    (C::CacheHits, Sum),
    (C::HitRequests, Sum),
    (C::MissRequests, Sum),
    (C::PassRequests, Sum),
    (C::RespBytesSum, Sum),
    (C::LatUsSum, Sum),
    (C::LatUsCount, Sum),
    (C::Ost5xxCount, Sum),
    (C::OstTotalCount, Sum),
    (C::RttSum, Sum),
    (C::RttCount, Sum),
    (C::PlossSum, Sum),
    (C::PlossCount, Sum),
    (C::CwndSum, Sum),
    (C::CwndCount, Sum),
    (C::RttvarSum, Sum),
    (C::RttvarCount, Sum),
    (C::RetransCount, Sum),
    (C::ElapsedSum, Sum),
    (C::ElapsedCount, Sum),
    (C::DeliveryRateSum, Sum),
    (C::DeliveryRateCount, Sum),
    (C::TtfbHist, Merge),
    (C::ElapsedHist, Merge),
    (C::RttHist, Merge),
    (C::OttfbHist, Merge),
    (C::OttlbHist, Merge),
];

/// A bundle of `(field, value)` rows that sum per value (and, when `timed`, per bucket), cut to
/// the values with the most requests (`cols[0]`) and counted in `dim_totals`.
struct Group {
    name: &'static str,
    cols: &'static [(C, Op)],
    timed: bool,
    /// Field write order; others follow by name.
    fields: &'static [&'static str],
    /// Fields newer than some version-3 summaries: written only when every source has them
    /// (a `dim_totals` row), so a mix with older sources reads as absent, not as a partial sum.
    gated: &'static [&'static str],
}

const GROUPS: [Group; 20] = [
    Group {
        name: "dims",
        cols: DIM_COLS,
        timed: false,
        fields: DIMS,
        gated: &[],
    },
    Group {
        name: "series5",
        cols: SERIES_COLS,
        timed: true,
        fields: &["overall", "proto", "asn", "city", "origin"],
        gated: &["origin", "city"],
    },
    Group {
        name: "bins",
        cols: BIN_COLS,
        timed: true,
        fields: &["req_header_size", "verified_bot"],
        gated: &[],
    },
    Group {
        name: "assets",
        cols: ASSET_COLS,
        timed: false,
        fields: &["asset"],
        gated: &[],
    },
    Group {
        name: "transport",
        cols: TRANSPORT_COLS,
        timed: false,
        fields: &["overall", "proto"],
        gated: &["overall", "proto"],
    },
    Group {
        name: "origin",
        cols: ORIGIN_COLS,
        timed: false,
        fields: &["overall", "edge", "pop"],
        gated: &["overall", "edge", "pop"],
    },
    Group {
        name: "urls",
        cols: URL_COLS,
        timed: false,
        fields: &["url", "asn"],
        gated: &["url", "asn"],
    },
    Group {
        name: "quality",
        cols: QUALITY_COLS,
        timed: false,
        fields: &["overall", "asn", "region"],
        gated: &["overall", "asn", "region"],
    },
    Group {
        name: "header_ip",
        cols: HEADER_IP_COLS,
        timed: false,
        fields: &["ip"],
        gated: &["ip"],
    },
    Group {
        name: "bot_ua",
        cols: BOT_UA_COLS,
        timed: false,
        fields: &["ua"],
        gated: &["ua"],
    },
    Group {
        name: "tls_fp",
        cols: TLS_FP_COLS,
        timed: false,
        fields: &["fp", "fp_country"],
        gated: &["fp", "fp_country"],
    },
    Group {
        name: "value",
        cols: VALUE_COLS,
        timed: true,
        fields: &summary::VALUE_FIELDS,
        gated: &summary::VALUE_FIELDS,
    },
    Group {
        name: "proxy",
        cols: PROXY_COLS,
        timed: false,
        fields: &["sample"],
        gated: &["sample"],
    },
    Group {
        name: "ins_url",
        cols: INS_URL_COLS,
        timed: false,
        fields: &["url"],
        gated: &["url"],
    },
    Group {
        name: "ins_ip",
        cols: INS_IP_COLS,
        timed: false,
        fields: &["ip"],
        gated: &["ip"],
    },
    Group {
        name: "asset_urls",
        cols: ASSET_URL_COLS,
        timed: false,
        fields: &["large", "low_ttl"],
        gated: &["large", "low_ttl"],
    },
    Group {
        name: "ins_lat",
        cols: INS_LAT_COLS,
        timed: false,
        fields: &["city", "pop"],
        gated: &["city", "pop"],
    },
    Group {
        name: "ins_asn",
        cols: INS_ASN_COLS,
        timed: false,
        fields: &["asn"],
        gated: &["asn"],
    },
    Group {
        name: "shield_routes",
        cols: SHIELD_ROUTES_COLS,
        timed: false,
        fields: &["route"],
        gated: &["route"],
    },
    Group {
        name: "filtered_rollups",
        cols: FILTERED_ROLLUPS_COLS,
        timed: false,
        fields: &summary::FILTERED_ROLLUP_FIELDS,
        gated: &summary::FILTERED_ROLLUP_FIELDS,
    },
];

impl Group {
    /// Candidate values kept per field while folding: 4x what a final bundle writes.
    fn keep(&self, field: &str) -> usize {
        match (self.name, field) {
            ("series5", "asn") => summary::SERIES_ASN_K,
            ("series5", "city") => summary::SERIES_CITY_K,
            ("header_ip", _) => summary::HEADER_IP_K,
            ("tls_fp", _) => summary::TLS_FP_K,
            ("value", field) => summary::value_keep(field),
            ("asset_urls", _) => summary::ASSET_URL_K,
            ("proxy", _) => summary::SAMPLE_K,
            ("ins_url", _) => summary::INS_URL_K,
            ("ins_ip", _) => summary::INS_IP_K,
            ("ins_lat", "city") => summary::SERIES_CITY_K,
            ("ins_asn", _) => summary::INS_ASN_K,
            ("shield_routes", _) => summary::SHIELD_ROUTE_K,
            ("filtered_rollups", field) => summary::filtered_rollup_cap(field),
            _ => summary::DIM_K,
        }
    }

    /// Values a final bundle writes per field: an hour keeps the folding candidates, since day
    /// bundles fold hour bundles; a day writes the planned K.
    fn written(&self, field: &str, unit: Unit) -> usize {
        match unit {
            Unit::Hour => self.keep(field),
            Unit::Day if self.name == "series5" && field == "asn" => SERIES_ASN_TOP_K,
            Unit::Day if self.name == "series5" && field == "city" => SERIES_CITY_TOP_K,
            Unit::Day if self.name == "proxy" => summary::SAMPLE_K,
            Unit::Day if self.name == "ins_lat" => self.keep(field),
            Unit::Day => self.keep(field).min(DAY_TOP_K),
        }
    }
}

fn merge_cell(into: &mut Cell, from: Cell, op: Op) {
    match (into, from, op) {
        (_, Cell::Null, _) => {}
        (into @ Cell::Null, from, _) => *into = from,
        (Cell::I(a), Cell::I(b), Sum) => *a = a.saturating_add(b),
        (Cell::F(a), Cell::F(b), Sum) => *a += b,
        (Cell::I(a), Cell::I(b), Min) => *a = (*a).min(b),
        (Cell::I(a), Cell::I(b), Max) => *a = (*a).max(b),
        (Cell::S(a), Cell::S(b), Min) if b < *a => *a = b,
        (Cell::S(a), Cell::S(b), Max) if b > *a => *a = b,
        (Cell::H(a), Cell::H(b), Merge) => a.merge(&b),
        _ => {}
    }
}

fn int(cell: &Cell) -> i64 {
    match cell {
        Cell::I(i) => *i,
        _ => 0,
    }
}

#[derive(Clone, Copy)]
enum TypedCol<'a> {
    None,
    Utf8(&'a StringArray),
    I64(&'a Int64Array),
    Ts(&'a TimestampMillisecondArray),
    F64(&'a Float64Array),
    Bool(&'a BooleanArray),
    Bin(&'a BinaryArray),
}

/// A source's columns, pre-downcast once per `RecordBatch` by `C` index.
struct Cols<'a>([TypedCol<'a>; COLS.len()]);

impl<'a> Cols<'a> {
    fn new(batch: &'a RecordBatch) -> Self {
        let mut cols = [TypedCol::None; COLS.len()];
        for (idx, (name, _)) in COLS.iter().enumerate() {
            let Some(arr) = batch.column_by_name(name) else {
                continue;
            };
            let any = arr.as_any();
            cols[idx] = if let Some(a) = any.downcast_ref::<StringArray>() {
                TypedCol::Utf8(a)
            } else if let Some(a) = any.downcast_ref::<Int64Array>() {
                TypedCol::I64(a)
            } else if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
                TypedCol::Ts(a)
            } else if let Some(a) = any.downcast_ref::<Float64Array>() {
                TypedCol::F64(a)
            } else if let Some(a) = any.downcast_ref::<BooleanArray>() {
                TypedCol::Bool(a)
            } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
                TypedCol::Bin(a)
            } else {
                TypedCol::None
            };
        }
        Self(cols)
    }

    #[inline]
    fn s(&self, c: C, i: usize) -> Option<&'a str> {
        match self.0[c as usize] {
            TypedCol::Utf8(a) if a.is_valid(i) => Some(a.value(i)),
            _ => None,
        }
    }

    #[inline]
    fn i(&self, c: C, i: usize) -> Option<i64> {
        match self.0[c as usize] {
            TypedCol::I64(a) if a.is_valid(i) => Some(a.value(i)),
            TypedCol::Ts(a) if a.is_valid(i) => Some(a.value(i)),
            _ => None,
        }
    }

    #[inline]
    fn f(&self, c: C, i: usize) -> Option<f64> {
        match self.0[c as usize] {
            TypedCol::F64(a) if a.is_valid(i) => Some(a.value(i)),
            _ => None,
        }
    }

    #[inline]
    fn b(&self, c: C, i: usize) -> bool {
        match self.0[c as usize] {
            TypedCol::Bool(a) if a.is_valid(i) => a.value(i),
            _ => false,
        }
    }

    #[inline]
    fn bin(&self, c: C, i: usize) -> Option<&'a [u8]> {
        match self.0[c as usize] {
            TypedCol::Bin(a) if a.is_valid(i) => Some(a.value(i)),
            _ => None,
        }
    }

    #[inline]
    fn cell(&self, c: C, i: usize) -> Cell {
        match COLS[c as usize].1 {
            Ty::Utf8 => self.s(c, i).map_or(Cell::Null, |s| Cell::S(s.to_owned())),
            Ty::I64 | Ty::Ts => self.i(c, i).map_or(Cell::Null, Cell::I),
            Ty::F64 => self.f(c, i).map_or(Cell::Null, Cell::F),
            Ty::Bin => self.hist(c, i).map_or(Cell::Null, |h| Cell::H(Box::new(h))),
            Ty::Bool => Cell::Null,
        }
    }

    /// A histogram cell; an empty or unreadable one is null.
    fn hist(&self, c: C, i: usize) -> Option<Hist> {
        self.bin(c, i)
            .and_then(Hist::from_bytes)
            .filter(|h| !h.is_empty())
    }

    fn cells(&self, cols: &[(C, Op)], i: usize) -> Vec<Cell> {
        cols.iter().map(|(c, _)| self.cell(*c, i)).collect()
    }

    fn merge_into(&self, into: &mut [Cell], cols: &[(C, Op)], i: usize) {
        for (cell, (c, op)) in into.iter_mut().zip(cols) {
            match COLS[*c as usize].1 {
                Ty::I64 | Ty::Ts => {
                    if let Some(b) = self.i(*c, i) {
                        merge_cell(cell, Cell::I(b), *op);
                    }
                }
                Ty::F64 => {
                    if let Some(b) = self.f(*c, i) {
                        merge_cell(cell, Cell::F(b), *op);
                    }
                }
                Ty::Utf8 => {
                    if let Some(b) = self.s(*c, i) {
                        match (cell, *op) {
                            (cell @ Cell::Null, _) => *cell = Cell::S(b.to_owned()),
                            (Cell::S(a), Min) if b < a.as_str() => {
                                a.clear();
                                a.push_str(b);
                            }
                            (Cell::S(a), Max) if b > a.as_str() => {
                                a.clear();
                                a.push_str(b);
                            }
                            _ => {}
                        }
                    }
                }
                Ty::Bin => match cell {
                    Cell::H(a) => {
                        if let Some(blob) = self.bin(*c, i) {
                            a.merge_bytes(blob);
                        }
                    }
                    cell => {
                        if let Some(h) = self.hist(*c, i) {
                            *cell = Cell::H(Box::new(h));
                        }
                    }
                },
                Ty::Bool => {}
            }
        }
    }
}

#[derive(Default)]
struct FieldVal {
    count: i64,
    ip_rows: i64,
    sketch: Option<Box<Hll>>,
    capped: bool,
}

#[derive(Default)]
struct FieldAcc {
    total: i64,
    truncated: bool,
    values: HashMap<String, FieldVal>,
    /// Folded sources (weighted like `Acc::sources`) that summarized this field.
    sources: u64,
}

type SessionKey = (String, Option<String>);

/// One field of a [`Group`]: per value, per bucket (0 when untimed), the summed cells.
#[derive(Default)]
struct GroupAcc {
    /// Requests of every value, including cut ones.
    total: i64,
    truncated: bool,
    values: HashMap<String, BTreeMap<i64, Vec<Cell>>>,
    /// Folded sources (weighted like `Acc::sources`) with this field's `dim_totals` row.
    sources: u64,
}

impl GroupAcc {
    fn requests(rows: &BTreeMap<i64, Vec<Cell>>) -> i64 {
        rows.values().map(|c| int(&c[0])).sum()
    }

    /// Values by `(requests desc, value asc)`.
    fn ranked(&self) -> Vec<(&String, &BTreeMap<i64, Vec<Cell>>)> {
        let mut ranked: Vec<_> = self
            .values
            .iter()
            .map(|(v, rows)| (Self::requests(rows), v, rows))
            .collect();
        ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        ranked.into_iter().map(|(_, v, rows)| (v, rows)).collect()
    }
}

/// `(sample_key, timestamp ms, cells)`.
type SampleRow = (i64, i64, Vec<Cell>);

/// The merge state of a bundle under construction.
struct Acc {
    /// Per hour bucket (ms): sums, IP sketch, `sample_capped` of any source.
    overview: BTreeMap<i64, (Vec<Cell>, Hll, bool)>,
    minutes: BTreeMap<i64, Vec<Cell>>,
    fields: BTreeMap<String, FieldAcc>,
    sessions: HashMap<SessionKey, Vec<Cell>>,
    /// Per [`GROUPS`] entry, per field.
    groups: [BTreeMap<String, GroupAcc>; GROUPS.len()],
    /// The lowest [`summary::SAMPLE_K`] sample rows, by `(key, timestamp)`.
    sample: Vec<SampleRow>,
    /// A source of another summary version was folded in: the bundle is written as `mixed`,
    /// which readers skip.
    foreign: bool,
    /// Bucket width (ms) of timed group rows: five minutes in an hour bundle, an hour in a day
    /// bundle.
    step: i64,
    /// Window summaries folded in, counting a resumed partial as the sources it covers.
    sources: u64,
}

impl Default for Acc {
    fn default() -> Self {
        Self::new(Unit::Hour)
    }
}

/// `(count desc, value asc)`.
fn by_count<T>(a: &(String, T), b: &(String, T), count: impl Fn(&T) -> i64) -> Ordering {
    count(&b.1).cmp(&count(&a.1)).then_with(|| a.0.cmp(&b.0))
}

/// Keeps the first `k` of `items` by `order`, in no particular order; true if any were cut.
fn keep_top<T>(items: &mut Vec<T>, k: usize, order: impl FnMut(&T, &T) -> Ordering) -> bool {
    if items.len() <= k {
        return false;
    }
    if k > 0 {
        items.select_nth_unstable_by(k - 1, order);
    }
    items.truncate(k);
    true
}

impl Acc {
    fn new(unit: Unit) -> Self {
        Self {
            overview: BTreeMap::new(),
            minutes: BTreeMap::new(),
            fields: BTreeMap::new(),
            sessions: HashMap::new(),
            groups: Default::default(),
            sample: Vec::new(),
            foreign: false,
            step: match unit {
                Unit::Hour => summary::SERIES_STEP_MS,
                Unit::Day => HOUR_S * 1000,
            },
            sources: 0,
        }
    }

    fn group_acc(&mut self, group: usize, field: &str) -> &mut GroupAcc {
        let fields = &mut self.groups[group];
        if !fields.contains_key(field) {
            fields.insert(field.to_owned(), GroupAcc::default());
        }
        fields.get_mut(field).expect("inserted")
    }

    /// Folds one source that stands for `weight` window summaries (a resumed partial covers
    /// several; a day bundle's hour bundle counts as one).
    fn fold(&mut self, bytes: Vec<u8>, weight: u64) -> Result<(), String> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .map_err(|_| "summary_unreadable".to_string())?;
        let version = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| kv.iter().find(|kv| kv.key == "flp_summary"))
            .and_then(|kv| kv.value.as_deref());
        self.foreign |= version != Some(summary::SUMMARY_VERSION);
        let reader = builder
            .build()
            .map_err(|_| "summary_unreadable".to_string())?;
        let mut seen = BTreeSet::new();
        let mut seen_groups = BTreeSet::new();
        for batch in reader {
            let batch = batch.map_err(|_| "summary_unreadable".to_string())?;
            let cols = Cols::new(&batch);
            for i in 0..batch.num_rows() {
                if cols.s(C::Bundle, i) == Some("field_totals") {
                    let f = cols.s(C::FieldName, i).unwrap_or_default();
                    if !seen.contains(f) {
                        seen.insert(f.to_owned());
                    }
                }
                if cols.s(C::Bundle, i) == Some("dim_totals") {
                    let g = cols.s(C::Value, i).unwrap_or_default();
                    if let Some(g) = GROUPS.iter().position(|x| x.name == g) {
                        seen_groups
                            .insert((g, cols.s(C::FieldName, i).unwrap_or_default().to_owned()));
                    }
                }
                self.fold_row(&cols, i);
            }
        }
        for field in seen {
            let acc = self.fields.entry(field).or_default();
            acc.sources = acc.sources.saturating_add(weight);
        }
        for (g, field) in seen_groups {
            let acc = self.group_acc(g, &field);
            acc.sources = acc.sources.saturating_add(weight);
        }
        self.sources = self.sources.saturating_add(weight);
        Ok(())
    }

    /// A gated field (see [`Group::gated`]) that some source lacks is left out.
    fn group_covered(&self, group: &Group, field: &str, acc: &GroupAcc) -> bool {
        !group.gated.contains(&field) || acc.sources >= self.sources
    }

    /// A custom field that some source never summarized (it predates the field or the
    /// summarizer) would merge to a partial count, so it is left out and readers fall back to
    /// raw. Catalog fields are in every version-3 summary.
    fn covered(&self, field: &str, acc: &FieldAcc) -> bool {
        FIELDS.contains(&field) || acc.sources >= self.sources
    }

    fn field_acc(&mut self, field: &str) -> &mut FieldAcc {
        if !self.fields.contains_key(field) {
            self.fields.insert(field.to_owned(), FieldAcc::default());
        }
        self.fields.get_mut(field).expect("inserted")
    }

    fn fold_row(&mut self, cols: &Cols<'_>, i: usize) {
        let f_str = cols.s(C::FieldName, i).unwrap_or_default();
        let v_str = cols.s(C::Value, i).unwrap_or_default();
        match cols.s(C::Bundle, i) {
            Some("overview") => {
                let hour = cols
                    .i(C::Bucket, i)
                    .unwrap_or_default()
                    .div_euclid(HOUR_S * 1000)
                    * HOUR_S
                    * 1000;
                let entry = self
                    .overview
                    .entry(hour)
                    .or_insert_with(|| (vec![Cell::Null; OV_COLS.len()], Hll::default(), false));
                cols.merge_into(&mut entry.0, OV_COLS, i);
                if let Some(blob) = cols.bin(C::IpSketch, i) {
                    entry.1.merge_bytes(blob);
                }
                entry.2 |= cols.b(C::SampleCapped, i);
            }
            Some("time_series") => {
                let minute = cols.i(C::Bucket, i).unwrap_or_default();
                let entry = self
                    .minutes
                    .entry(minute)
                    .or_insert_with(|| vec![Cell::Null; TS_COLS.len()]);
                cols.merge_into(entry, TS_COLS, i);
            }
            Some("field_totals") => {
                let acc = self.field_acc(f_str);
                acc.total = acc
                    .total
                    .saturating_add(cols.i(C::Count, i).unwrap_or_default());
                acc.truncated |= cols.b(C::Truncated, i);
            }
            Some("all_fields") => {
                let acc = self.field_acc(f_str);
                let count = cols.i(C::Count, i).unwrap_or_default();
                if let Some(v) = acc.values.get_mut(v_str) {
                    v.count = v.count.saturating_add(count);
                } else {
                    acc.values.insert(
                        v_str.to_owned(),
                        FieldVal {
                            count,
                            ..FieldVal::default()
                        },
                    );
                }
            }
            Some("all_fields_ip") => {
                if !summary::IP_SKETCH_FIELDS.contains(&f_str) {
                    return;
                }
                let acc = self.field_acc(f_str);
                let v = if let Some(v) = acc.values.get_mut(v_str) {
                    v
                } else {
                    acc.values.entry(v_str.to_owned()).or_default()
                };
                v.ip_rows = v
                    .ip_rows
                    .saturating_add(cols.i(C::Count, i).unwrap_or_default());
                let sketch = v.sketch.get_or_insert_with(Box::default);
                if let Some(blob) = cols.bin(C::IpSketch, i) {
                    sketch.merge_bytes(blob);
                }
                v.capped |= cols.b(C::SampleCapped, i);
            }
            Some("sessions") => {
                let Some(ip) = cols.s(C::Ip, i) else { return };
                let key = (ip.to_owned(), cols.s(C::Ja4, i).map(str::to_owned));
                match self.sessions.get_mut(&key) {
                    Some(cells) => cols.merge_into(cells, SESSION_COLS, i),
                    None => {
                        self.sessions.insert(key, cols.cells(SESSION_COLS, i));
                    }
                }
            }
            Some("dim_totals") => {
                let Some(g) = GROUPS.iter().position(|g| g.name == v_str) else {
                    return;
                };
                let acc = self.group_acc(g, f_str);
                acc.total = acc
                    .total
                    .saturating_add(cols.i(C::Requests, i).unwrap_or_default());
                acc.truncated |= cols.b(C::Truncated, i);
            }
            Some("sample") => {
                self.sample.push((
                    cols.i(C::SampleKey, i).unwrap_or_default(),
                    cols.i(C::Bucket, i).unwrap_or_default(),
                    cols.cells(SAMPLE_COLS, i),
                ));
            }
            Some(name) => {
                let Some(g) = GROUPS.iter().position(|g| g.name == name) else {
                    return;
                };
                let group = &GROUPS[g];
                let bucket = if group.timed {
                    let step = self.step;
                    cols.i(C::Bucket, i).unwrap_or_default().div_euclid(step) * step
                } else {
                    0
                };
                let acc = self.group_acc(g, f_str);
                let rows = match acc.values.get_mut(v_str) {
                    Some(rows) => rows,
                    None => acc.values.entry(v_str.to_owned()).or_default(),
                };
                match rows.get_mut(&bucket) {
                    Some(cells) => cols.merge_into(cells, group.cols, i),
                    None => {
                        rows.insert(bucket, cols.cells(group.cols, i));
                    }
                }
            }
            None => {}
        }
    }

    /// The cut applied after every fold, so the state depends only on the sources and their
    /// order, never on where an execution stopped.
    fn prune(&mut self) {
        for acc in self.fields.values_mut() {
            if acc.values.len() > summary::TOP_K {
                let mut values: Vec<(String, FieldVal)> = acc.values.drain().collect();
                keep_top(&mut values, summary::TOP_K, |a, b| {
                    by_count(a, b, |v| v.count)
                });
                acc.values.extend(values);
                acc.truncated = true;
            }
            if acc.values.len() > summary::IP_TOP_K {
                let mut sketched: Vec<(&String, &mut FieldVal)> = acc
                    .values
                    .iter_mut()
                    .filter(|(_, v)| v.sketch.is_some())
                    .collect();
                if sketched.len() > summary::IP_TOP_K {
                    sketched.select_nth_unstable_by(summary::IP_TOP_K - 1, |a, b| {
                        b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0))
                    });
                    for (_, v) in &mut sketched[summary::IP_TOP_K..] {
                        v.ip_rows = 0;
                        v.sketch = None;
                        v.capped = false;
                    }
                }
            }
        }
        if self.sessions.len() > summary::SESSION_K {
            let mut sessions: Vec<(SessionKey, Vec<Cell>)> = self.sessions.drain().collect();
            keep_top(&mut sessions, summary::SESSION_K, session_order);
            self.sessions.extend(sessions);
        }
        for (group, fields) in GROUPS.iter().zip(&mut self.groups) {
            for (field, acc) in fields.iter_mut() {
                let keep = group.keep(field);
                if acc.values.len() > keep {
                    let mut values: Vec<(String, BTreeMap<i64, Vec<Cell>>)> =
                        acc.values.drain().collect();
                    keep_top(&mut values, keep, |a, b| by_count(a, b, GroupAcc::requests));
                    acc.values.extend(values);
                    acc.truncated = true;
                }
            }
        }
        if self.sample.len() > summary::SAMPLE_K {
            self.sample.sort_by_key(|r| (r.0, r.1));
            self.sample.truncate(summary::SAMPLE_K);
        }
    }

    /// Encodes the full intermediate `Acc` state in a compact binary format (`PARTIAL_MAGIC`) so
    /// resuming across ticks takes `< 1 ms` of vCPU instead of a 73-column ZSTD Parquet round-trip.
    fn encode_partial(&self) -> Vec<u8> {
        fn put_u32(out: &mut Vec<u8>, v: u32) {
            out.extend_from_slice(&v.to_le_bytes());
        }
        fn put_u64(out: &mut Vec<u8>, v: u64) {
            out.extend_from_slice(&v.to_le_bytes());
        }
        fn put_i64(out: &mut Vec<u8>, v: i64) {
            out.extend_from_slice(&v.to_le_bytes());
        }
        fn put_str(out: &mut Vec<u8>, s: &str) {
            put_u32(out, s.len() as u32);
            out.extend_from_slice(s.as_bytes());
        }
        fn put_cells(out: &mut Vec<u8>, cells: &[Cell]) {
            put_u32(out, cells.len() as u32);
            for c in cells {
                match c {
                    Cell::Null => out.push(0),
                    Cell::I(i) => {
                        out.push(1);
                        put_i64(out, *i);
                    }
                    Cell::F(f) => {
                        out.push(2);
                        out.extend_from_slice(&f.to_le_bytes());
                    }
                    Cell::S(s) => {
                        out.push(3);
                        put_str(out, s);
                    }
                    Cell::H(h) => match h.to_bytes() {
                        Some(bytes) => {
                            out.push(4);
                            put_u32(out, bytes.len() as u32);
                            out.extend_from_slice(&bytes);
                        }
                        None => out.push(0),
                    },
                }
            }
        }
        fn put_hll(out: &mut Vec<u8>, hll: &Hll) {
            let bytes = hll.to_bytes();
            put_u32(out, bytes.len() as u32);
            out.extend_from_slice(&bytes);
        }

        let mut out = Vec::with_capacity(64 * 1024);
        out.extend_from_slice(PARTIAL_MAGIC);
        put_u64(&mut out, self.sources);

        put_u32(&mut out, self.overview.len() as u32);
        for (hour, (cells, hll, capped)) in &self.overview {
            put_i64(&mut out, *hour);
            put_cells(&mut out, cells);
            put_hll(&mut out, hll);
            out.push(u8::from(*capped));
        }

        put_u32(&mut out, self.minutes.len() as u32);
        for (minute, cells) in &self.minutes {
            put_i64(&mut out, *minute);
            put_cells(&mut out, cells);
        }

        put_u32(&mut out, self.fields.len() as u32);
        for (field, acc) in &self.fields {
            put_str(&mut out, field);
            put_i64(&mut out, acc.total);
            out.push(u8::from(acc.truncated));
            put_u64(&mut out, acc.sources);
            put_u32(&mut out, acc.values.len() as u32);
            for (val, v) in &acc.values {
                put_str(&mut out, val);
                put_i64(&mut out, v.count);
                put_i64(&mut out, v.ip_rows);
                out.push(u8::from(v.capped));
                match &v.sketch {
                    Some(hll) => {
                        out.push(1);
                        put_hll(&mut out, hll);
                    }
                    None => out.push(0),
                }
            }
        }

        put_u32(&mut out, self.sessions.len() as u32);
        for ((ip, ja4), cells) in &self.sessions {
            put_str(&mut out, ip);
            match ja4 {
                Some(j) => {
                    out.push(1);
                    put_str(&mut out, j);
                }
                None => out.push(0),
            }
            put_cells(&mut out, cells);
        }

        for fields in &self.groups {
            put_u32(&mut out, fields.len() as u32);
            for (field, acc) in fields {
                put_str(&mut out, field);
                put_i64(&mut out, acc.total);
                out.push(u8::from(acc.truncated));
                put_u64(&mut out, acc.sources);
                put_u32(&mut out, acc.values.len() as u32);
                for (val, rows) in &acc.values {
                    put_str(&mut out, val);
                    put_u32(&mut out, rows.len() as u32);
                    for (bucket, cells) in rows {
                        put_i64(&mut out, *bucket);
                        put_cells(&mut out, cells);
                    }
                }
            }
        }

        put_u32(&mut out, self.sample.len() as u32);
        for (key, ts, cells) in &self.sample {
            put_i64(&mut out, *key);
            put_i64(&mut out, *ts);
            put_cells(&mut out, cells);
        }
        out.push(u8::from(self.foreign));
        out
    }

    fn decode_partial(bytes: &[u8], unit: Unit) -> Result<Self, String> {
        struct Cur<'a>(&'a [u8]);
        impl<'a> Cur<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
                if self.0.len() < n {
                    return Err("partial_unreadable".to_string());
                }
                let (head, tail) = self.0.split_at(n);
                self.0 = tail;
                Ok(head)
            }
            fn u8(&mut self) -> Result<u8, String> {
                Ok(self.take(1)?[0])
            }
            fn u32(&mut self) -> Result<u32, String> {
                Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
            }
            fn u64(&mut self) -> Result<u64, String> {
                Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            fn i64(&mut self) -> Result<i64, String> {
                Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            fn str(&mut self) -> Result<String, String> {
                let len = self.u32()? as usize;
                let s = std::str::from_utf8(self.take(len)?)
                    .map_err(|_| "partial_unreadable".to_string())?;
                Ok(s.to_owned())
            }
            fn cells(&mut self) -> Result<Vec<Cell>, String> {
                let len = self.u32()? as usize;
                if len > self.0.len() {
                    return Err("partial_unreadable".to_string());
                }
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    let cell = match self.u8()? {
                        0 => Cell::Null,
                        1 => Cell::I(self.i64()?),
                        2 => Cell::F(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
                        3 => Cell::S(self.str()?),
                        4 => {
                            let len = self.u32()? as usize;
                            Cell::H(Box::new(
                                Hist::from_bytes(self.take(len)?)
                                    .ok_or_else(|| "partial_unreadable".to_string())?,
                            ))
                        }
                        _ => return Err("partial_unreadable".to_string()),
                    };
                    out.push(cell);
                }
                Ok(out)
            }
            fn hll(&mut self) -> Result<Hll, String> {
                let len = self.u32()? as usize;
                Hll::from_bytes(self.take(len)?).ok_or_else(|| "partial_unreadable".to_string())
            }
        }

        let Some(rest) = bytes.strip_prefix(PARTIAL_MAGIC) else {
            return Err("partial_unreadable".to_string());
        };
        let mut cur = Cur(rest);
        let sources = cur.u64()?;

        let ov_len = cur.u32()? as usize;
        let mut overview = BTreeMap::new();
        for _ in 0..ov_len {
            let hour = cur.i64()?;
            let cells = cur.cells()?;
            let hll = cur.hll()?;
            let capped = cur.u8()? != 0;
            overview.insert(hour, (cells, hll, capped));
        }

        let min_len = cur.u32()? as usize;
        let mut minutes = BTreeMap::new();
        for _ in 0..min_len {
            let minute = cur.i64()?;
            let cells = cur.cells()?;
            minutes.insert(minute, cells);
        }

        let fld_len = cur.u32()? as usize;
        let mut fields = BTreeMap::new();
        for _ in 0..fld_len {
            let field = cur.str()?;
            let total = cur.i64()?;
            let truncated = cur.u8()? != 0;
            let f_sources = cur.u64()?;
            let val_len = cur.u32()? as usize;
            if val_len > cur.0.len() / 22 {
                return Err("partial_unreadable".to_string());
            }
            let mut values = HashMap::with_capacity(val_len);
            for _ in 0..val_len {
                let val = cur.str()?;
                let count = cur.i64()?;
                let ip_rows = cur.i64()?;
                let capped = cur.u8()? != 0;
                let sketch = if cur.u8()? != 0 {
                    Some(Box::new(cur.hll()?))
                } else {
                    None
                };
                values.insert(
                    val,
                    FieldVal {
                        count,
                        ip_rows,
                        sketch,
                        capped,
                    },
                );
            }
            fields.insert(
                field,
                FieldAcc {
                    total,
                    truncated,
                    values,
                    sources: f_sources,
                },
            );
        }

        let ses_len = cur.u32()? as usize;
        if ses_len > cur.0.len() / 9 {
            return Err("partial_unreadable".to_string());
        }
        let mut sessions = HashMap::with_capacity(ses_len);
        for _ in 0..ses_len {
            let ip = cur.str()?;
            let ja4 = if cur.u8()? != 0 {
                Some(cur.str()?)
            } else {
                None
            };
            let cells = cur.cells()?;
            sessions.insert((ip, ja4), cells);
        }

        let mut acc = Self::new(unit);
        for fields in &mut acc.groups {
            let f_len = cur.u32()? as usize;
            for _ in 0..f_len {
                let field = cur.str()?;
                let total = cur.i64()?;
                let truncated = cur.u8()? != 0;
                let sources = cur.u64()?;
                let v_len = cur.u32()? as usize;
                if v_len > cur.0.len() / 8 {
                    return Err("partial_unreadable".to_string());
                }
                let mut values = HashMap::with_capacity(v_len);
                for _ in 0..v_len {
                    let val = cur.str()?;
                    let r_len = cur.u32()? as usize;
                    let mut rows = BTreeMap::new();
                    for _ in 0..r_len {
                        let bucket = cur.i64()?;
                        rows.insert(bucket, cur.cells()?);
                    }
                    values.insert(val, rows);
                }
                fields.insert(
                    field,
                    GroupAcc {
                        total,
                        truncated,
                        values,
                        sources,
                    },
                );
            }
        }

        let s_len = cur.u32()? as usize;
        for _ in 0..s_len {
            let key = cur.i64()?;
            let ts = cur.i64()?;
            acc.sample.push((key, ts, cur.cells()?));
        }
        acc.foreign = cur.u8()? != 0;
        if !cur.0.is_empty() {
            return Err("partial_unreadable".to_string());
        }

        Ok(Self {
            overview,
            minutes,
            fields,
            sessions,
            sources,
            ..acc
        })
    }

    /// A final bundle of `unit` or a partial (`None`: the whole state, flags as folded).
    fn write(&self, start: i64, unit: Option<Unit>, label: &str) -> Result<Vec<u8>, String> {
        let bucket = start * 1000;
        let fin = unit.is_some();
        let top_k = unit.map(Unit::top_k);
        let mut out = Out::new();
        let mut groups_out: Vec<arrow_array::RecordBatch> = Vec::new();
        let mut layout_rg: Vec<&'static [&'static str]> = Vec::new();
        let flush_group = |out: &mut Out,
                           groups: &'static [&'static str],
                           groups_out: &mut Vec<arrow_array::RecordBatch>,
                           layout_rg: &mut Vec<&'static [&'static str]>|
         -> Result<(), String> {
            if !out.is_empty() {
                let batch = std::mem::replace(out, Out::new()).finish()?;
                groups_out.push(batch);
                layout_rg.push(groups);
            }
            Ok(())
        };
        let row = |out: &mut Out, head: &[(C, V<'_>)], cols: &[(C, Op)], cells: &[Cell]| {
            let blobs: Vec<Option<Vec<u8>>> = cells
                .iter()
                .map(|cell| match cell {
                    Cell::H(h) => h.to_bytes(),
                    _ => None,
                })
                .collect();
            let mut values: Vec<(C, V<'_>)> = head.to_vec();
            values.extend(
                cols.iter()
                    .zip(cells)
                    .zip(&blobs)
                    .map(|(((c, _), cell), blob)| {
                        (
                            *c,
                            match cell {
                                Cell::Null => V::Null,
                                Cell::I(i) => V::I(*i),
                                Cell::F(f) => V::F(*f),
                                Cell::S(s) => V::S(s),
                                Cell::H(_) => blob.as_deref().map_or(V::Null, V::Bin),
                            },
                        )
                    }),
            );
            out.push(&values);
        };
        for (hour, (cells, hll, capped)) in &self.overview {
            let observed = fin.then(|| hll.count());
            let blob = hll.to_bytes();
            row(
                &mut out,
                &[
                    (C::Bundle, V::S("overview")),
                    (C::Bucket, V::I(*hour)),
                    (C::IpSketch, V::Bin(&blob)),
                    (
                        C::IpCountObserved,
                        observed.map_or(V::Null, |o| V::I(o as i64)),
                    ),
                    (
                        C::SampleCapped,
                        V::B(*capped || observed.is_some_and(|o| o >= summary::IP_SAMPLE_CAP)),
                    ),
                ],
                OV_COLS,
                cells,
            );
        }
        for (minute, cells) in &self.minutes {
            row(
                &mut out,
                &[(C::Bundle, V::S("time_series")), (C::Bucket, V::I(*minute))],
                TS_COLS,
                cells,
            );
        }
        flush_group(
            &mut out,
            &["overview", "time_series"],
            &mut groups_out,
            &mut layout_rg,
        )?;
        let ordered = |names: Vec<&String>, known: &[&str]| -> Vec<String> {
            let mut out: Vec<String> = known
                .iter()
                .filter(|k| names.iter().any(|n| n == *k))
                .map(|k| (*k).to_owned())
                .collect();
            let mut rest: Vec<String> = names
                .into_iter()
                .filter(|n| !known.contains(&n.as_str()))
                .cloned()
                .collect();
            rest.sort_unstable();
            out.extend(rest);
            out
        };
        for field in ordered(self.fields.keys().collect(), FIELDS) {
            let acc = &self.fields[&field];
            if !self.covered(&field, acc) {
                continue;
            }
            let mut ranked: Vec<(&String, &FieldVal)> = acc.values.iter().collect();
            ranked.sort_unstable_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
            let cut = top_k.unwrap_or(ranked.len());
            let truncated = acc.truncated || ranked.len() > cut;
            ranked.truncate(cut);
            out.push(&[
                (C::Bundle, V::S("field_totals")),
                (C::Bucket, V::I(bucket)),
                (C::FieldName, V::S(&field)),
                (C::Count, V::I(acc.total)),
                (C::Truncated, V::B(truncated)),
            ]);
            for (value, v) in &ranked {
                out.push(&[
                    (C::Bundle, V::S("all_fields")),
                    (C::Bucket, V::I(bucket)),
                    (C::FieldName, V::S(&field)),
                    (C::Value, V::S(value)),
                    (C::Count, V::I(v.count)),
                ]);
            }
            let sketched = if fin { summary::IP_TOP_K } else { ranked.len() };
            for (value, v) in ranked.iter().take(sketched) {
                let Some(hll) = &v.sketch else { continue };
                let observed = fin.then(|| hll.count());
                out.push(&[
                    (C::Bundle, V::S("all_fields_ip")),
                    (C::Bucket, V::I(bucket)),
                    (C::FieldName, V::S(&field)),
                    (C::Value, V::S(value)),
                    (C::Count, V::I(v.ip_rows)),
                    (C::IpSketch, V::Bin(&hll.to_bytes())),
                    (
                        C::IpCountObserved,
                        observed.map_or(V::Null, |o| V::I(o as i64)),
                    ),
                    (
                        C::SampleCapped,
                        V::B(v.capped || observed.is_some_and(|o| o >= summary::IP_SAMPLE_CAP)),
                    ),
                ]);
            }
        }
        flush_group(
            &mut out,
            &["field_totals", "all_fields", "all_fields_ip"],
            &mut groups_out,
            &mut layout_rg,
        )?;
        let mut sessions: Vec<(&SessionKey, &Vec<Cell>)> = self.sessions.iter().collect();
        sessions.sort_unstable_by(|a, b| {
            int(&b.1[SESSION_REQS])
                .cmp(&int(&a.1[SESSION_REQS]))
                .then_with(|| a.0.cmp(b.0))
        });
        sessions.truncate(summary::SESSION_K);
        for ((ip, ja4), cells) in sessions {
            row(
                &mut out,
                &[
                    (C::Bundle, V::S("sessions")),
                    (C::Bucket, V::I(bucket)),
                    (C::Ip, V::S(ip)),
                    (C::Ja4, ja4.as_deref().map_or(V::Null, V::S)),
                ],
                SESSION_COLS,
                cells,
            );
        }
        flush_group(&mut out, &["sessions"], &mut groups_out, &mut layout_rg)?;
        for (group, fields) in GROUPS.iter().zip(&self.groups) {
            for field in ordered(fields.keys().collect(), group.fields) {
                let acc = &fields[&field];
                if !self.group_covered(group, &field, acc) {
                    continue;
                }
                let mut ranked = acc.ranked();
                let cut = unit.map_or(ranked.len(), |u| group.written(&field, u));
                let truncated = acc.truncated || ranked.len() > cut;
                ranked.truncate(cut);
                out.push(&[
                    (C::Bundle, V::S("dim_totals")),
                    (C::Bucket, V::I(bucket)),
                    (C::FieldName, V::S(&field)),
                    (C::Value, V::S(group.name)),
                    (C::Requests, V::I(acc.total)),
                    (C::Truncated, V::B(truncated)),
                ]);
                for (value, rows) in ranked {
                    for (b, cells) in rows {
                        row(
                            &mut out,
                            &[
                                (C::Bundle, V::S(group.name)),
                                (C::Bucket, V::I(if group.timed { *b } else { bucket })),
                                (C::FieldName, V::S(&field)),
                                (C::Value, V::S(value)),
                            ],
                            group.cols,
                            cells,
                        );
                    }
                }
            }
            match group.name {
                "dims" => flush_group(&mut out, &["dims"], &mut groups_out, &mut layout_rg)?,
                "series5" => flush_group(&mut out, &["series5"], &mut groups_out, &mut layout_rg)?,
                "origin" => flush_group(
                    &mut out,
                    &["bins", "assets", "transport", "origin"],
                    &mut groups_out,
                    &mut layout_rg,
                )?,
                _ => {}
            }
        }
        let mut sample: Vec<&SampleRow> = self.sample.iter().collect();
        sample.sort_by_key(|r| (r.0, r.1));
        for (key, ts, cells) in sample.into_iter().take(summary::SAMPLE_K) {
            row(
                &mut out,
                &[
                    (C::Bundle, V::S("sample")),
                    (C::Bucket, V::I(*ts)),
                    (C::SampleKey, V::I(*key)),
                ],
                SAMPLE_COLS,
                cells,
            );
        }
        flush_group(
            &mut out,
            &[
                "urls",
                "quality",
                "header_ip",
                "bot_ua",
                "tls_fp",
                "value",
                "proxy",
                "ins_url",
                "ins_ip",
                "asset_urls",
                "ins_lat",
                "ins_asn",
                "shield_routes",
                "filtered_rollups",
                "sample",
            ],
            &mut groups_out,
            &mut layout_rg,
        )?;
        let batch_refs: Vec<&arrow_array::RecordBatch> = groups_out.iter().collect();
        let layout_json = serde_json::json!({ "v": 1, "rg": layout_rg }).to_string();
        super::convert::write_summary_batches(
            summary::schema(),
            &batch_refs,
            vec![
                KeyValue::new(
                    "flp_summary".into(),
                    if self.foreign {
                        "mixed".to_owned()
                    } else {
                        summary::SUMMARY_VERSION.to_string()
                    },
                ),
                KeyValue::new(
                    "flp_bundle".into(),
                    if fin {
                        label.to_owned()
                    } else {
                        format!("{label}_partial")
                    },
                ),
                KeyValue::new("flp_summary_layout".into(), layout_json),
            ],
        )
    }
}

fn session_order(a: &(SessionKey, Vec<Cell>), b: &(SessionKey, Vec<Cell>)) -> Ordering {
    int(&b.1[SESSION_REQS])
        .cmp(&int(&a.1[SESSION_REQS]))
        .then_with(|| a.0.cmp(&b.0))
}

/// Merges `sources` (window summaries for an hour, hour bundles for a day), in the order given,
/// into the bundle starting at `start` (seconds). The stage folds sources in file-name order.
pub fn merge(sources: &[&[u8]], start: i64, unit: Unit) -> Result<Vec<u8>, String> {
    let mut acc = Acc::new(unit);
    for source in sources {
        acc.fold(source.to_vec(), 1)?;
        acc.prune();
    }
    acc.write(start, Some(unit), unit.label())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BundleReport {
    /// Hour and day bundles written.
    pub hours: u32,
    pub days: u32,
    /// Partials written by builds that will resume next execution.
    pub partials: u32,
    /// Sources (and partials) read and merged.
    pub sources: u64,
    pub rows: u64,
    pub bytes_written: u64,
    /// Replaced bundles and finished partials deleted.
    pub deleted: u64,
    pub more_pending: bool,
    /// `wall_time` or `vcpu` when a time limit ended the run early.
    pub stopped: Option<&'static str>,
    pub backend_calls: u32,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Default, Clone, PartialEq, Debug)]
struct State {
    /// First hour (start, seconds) the forward pass has not bundled yet.
    next_hour: Option<i64>,
    /// Current hour bundle per hour, while its day may still be (re)built or revisited.
    hours: BTreeMap<i64, String>,
    /// Current open-hour partial bundle per open hour.
    #[serde(default)]
    open_bundles: BTreeMap<i64, String>,
    /// Current day bundle per day, while it may be revisited.
    days: BTreeMap<i64, String>,
    /// Days to (re)build once the forward pass is past them.
    dirty: BTreeSet<i64>,
    /// Next hour of an unfinished revisit pass.
    revisit: Option<i64>,
    revisited_at: i64,
    /// Replaced bundles and when they may be deleted.
    retire: BTreeMap<String, i64>,
    #[serde(default)]
    inventory: Option<super::catalog::bundles::Inventory>,
    #[serde(default)]
    publication: Option<super::catalog::bundles::Coverage>,
    #[serde(default)]
    catalog_hours: BTreeMap<i64, (super::catalog::Reference, String)>,
}

enum Step {
    Done,
    Pending,
}

struct Ctx<'s, 'a, R: Runtime> {
    store: &'s Store<'a, R>,
    cdn_key: &'s str,
    reserve: u32,
    windows: String,
    bundles: String,
    now: i64,
    /// Hours with a window folder, per day listed this execution.
    present: HashMap<i64, BTreeSet<i64>>,
    report: BundleReport,
    catalog: super::catalog::Head,
    publication: Option<super::catalog::bundles::Coverage>,
}

/// A partial's state and object count, `Err` when it cannot be read, `None` when out of calls.
type Resume = Option<Result<(Acc, usize), String>>;

struct Job<'j> {
    sources: Vec<String>,
    partials: Vec<String>,
    final_key: Option<&'j str>,
    folder: String,
    start: i64,
    unit: Unit,
    coverage: Option<super::catalog::bundles::Coverage>,
}

impl<R: Runtime> Ctx<'_, '_, R> {
    /// `closed` asks for every window of a closed hour, so an open-hour snapshot is retaken.
    fn catalog_windows(
        &self,
        state: &mut State,
        hour: i64,
        closed: bool,
    ) -> Result<Option<Vec<String>>, String> {
        if state
            .inventory
            .as_ref()
            .is_none_or(|inventory| inventory.hour != hour || closed && !inventory.closed)
        {
            let current = if let Some(key) = self.catalog.days.get(&(hour - hour.rem_euclid(DAY_S)))
            {
                if !self.room(3) {
                    return Ok(None);
                }
                let day: super::catalog::Day = super::catalog::read(self.store, key)?;
                day.hours.get(&hour).cloned()
            } else {
                None
            };
            state.inventory = Some(super::catalog::bundles::Inventory {
                hour,
                generation: self.catalog.generation,
                current,
                windows: BTreeSet::new(),
                closed,
            });
        }
        let inventory = state
            .inventory
            .as_mut()
            .ok_or("catalog_preparation_invalid")?;
        if !super::catalog::bundles::collect(self.store, inventory, self.reserve + 2)? {
            return Ok(None);
        }
        Ok(Some(inventory.windows.iter().cloned().collect()))
    }

    fn room(&self, need: u32) -> bool {
        self.store.runtime.admin_remaining_calls() >= self.reserve + need
    }

    fn stop(&mut self) -> bool {
        let runtime = self.store.runtime;
        let reason = if runtime.admin_elapsed(self.store.started) >= WALL_STOP {
            Some("wall_time")
        } else if runtime.vcpu_ms() >= VCPU_STOP_MS {
            Some("vcpu")
        } else {
            None
        };
        if reason.is_some() {
            self.report.stopped = reason;
        }
        reason.is_some()
    }

    fn write_stop(&self) -> bool {
        let runtime = self.store.runtime;
        runtime.admin_elapsed(self.store.started) >= WALL_STOP
            || runtime.vcpu_ms() >= WRITE_VCPU_STOP_MS
    }

    fn list_all(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<Option<Vec<Object>>, String> {
        let (mut out, mut token) = (Vec::new(), None::<String>);
        loop {
            if !self.room(1) {
                return Ok(None);
            }
            let page = self
                .store
                .list(prefix, None, token.as_deref(), 1000, delimiter)?;
            out.extend(page.objects);
            out.extend(page.prefixes.into_iter().map(|key| Object {
                key,
                size: 0,
                last_modified: None,
            }));
            match page.next_token {
                Some(next) => token = Some(next),
                None => return Ok(Some(out)),
            }
        }
    }

    /// Hours of `day` that have a window folder; `None` when out of calls.
    fn present(&mut self, day: i64) -> Result<Option<&BTreeSet<i64>>, String> {
        if !self.present.contains_key(&day) {
            if self.catalog.version == super::catalog::VERSION {
                if !self.room(3) {
                    return Ok(None);
                }
                let hours = if let Some(key) = self.catalog.days.get(&day) {
                    let index: super::catalog::Day = super::catalog::read(self.store, key)?;
                    index.hours.keys().copied().collect()
                } else {
                    BTreeSet::new()
                };
                self.present.insert(day, hours);
                return Ok(self.present.get(&day));
            }
            let day_rel = day_dir(day);
            let prefix = format!("{}{day_rel}", self.windows);
            let Some(listed) = self.list_all(&prefix, Some("/"))? else {
                return Ok(None);
            };
            let hours: BTreeSet<i64> = listed
                .iter()
                .filter_map(|o| {
                    o.key
                        .strip_prefix(&prefix)?
                        .strip_prefix("hour=")?
                        .trim_end_matches('/')
                        .parse::<i64>()
                        .ok()
                })
                .filter(|h| (0..24).contains(h))
                .map(|h| day + h * HOUR_S)
                .collect();
            let empty = hours.is_empty();
            self.present.insert(day, hours);
            if empty {
                let month_rel = &day_rel[..day_rel.len() - "day=00/".len()];
                let month_prefix = format!("{}{month_rel}", self.windows);
                if let Some(month_listed) = self.list_all(&month_prefix, Some("/"))? {
                    let present_days: BTreeSet<&str> = month_listed
                        .iter()
                        .filter_map(|o| o.key.strip_prefix(&self.windows))
                        .collect();
                    for offset in -31..=31 {
                        let other = day + offset * DAY_S;
                        let other_rel = day_dir(other);
                        if other_rel.starts_with(month_rel)
                            && !present_days.contains(other_rel.as_str())
                        {
                            self.present.entry(other).or_default();
                        }
                    }
                }
            }
        }
        Ok(self.present.get(&day))
    }

    /// Folds what the job's best partial and its remaining sources allow this execution, then
    /// writes the bundle or a partial to resume from (and for open hours, an `open_bundle=`
    /// Parquet snapshot every [`OPEN_BUNDLE_MIN`] new closed windows).
    fn build(
        &mut self,
        job: Job<'_>,
        mut open: Option<(&mut State, usize)>,
    ) -> Result<Step, String> {
        let mut sources = job.sources;
        sources.sort_unstable_by(|a, b| file_name(a).cmp(file_name(b)));
        let names: Vec<&str> = sources.iter().map(|k| file_name(k)).collect();
        let mut candidates: Vec<(usize, &String)> = job
            .partials
            .iter()
            .filter_map(|key| {
                let name = parse_name(key)?;
                (name.kind == NameKind::Partial
                    && name.count > 0
                    && name.count <= names.len()
                    && fingerprint(names[..name.count].iter().copied()) == name.fp)
                    .then_some((name.count, key))
            })
            .collect();
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        let covered = candidates.first().map_or(0, |(count, _)| *count);
        let need_partial =
            job.final_key.is_none() && sources.len().saturating_sub(covered) >= OPEN_BATCH_SOURCES;
        let need_open = open
            .as_ref()
            .is_some_and(|(_, oc)| sources.len().saturating_sub(*oc) >= OPEN_BUNDLE_MIN);
        if job.final_key.is_none() {
            if !need_partial && !need_open {
                return Ok(Step::Done);
            }
            let need = if need_partial {
                OPEN_BATCH_SOURCES as u32 + 3
            } else {
                (sources.len().saturating_sub(covered)).min(MAX_SOURCES) as u32 + 4
            };
            if !self.room(need) {
                return Ok(Step::Pending);
            }
        }
        let mut acc = Acc::new(job.unit);
        let mut done = 0;
        let mut extra = 0;
        for (count, key) in candidates {
            if !self.room(3) {
                return Ok(Step::Pending);
            }
            match self.read_partial(key, job.unit)? {
                Some(Ok((state, parts))) => {
                    acc = state;
                    done = count;
                    extra = parts - 1;
                    self.report.sources += 1;
                    break;
                }
                // A partial that cannot be read is skipped for the next one, or a fresh build,
                // and deleted with the other stale partials once this build writes.
                Some(Err(error)) => self
                    .report
                    .errors
                    .push(format!("{error}:{}", file_name(key))),
                None => return Ok(Step::Pending),
            }
        }
        let resumed = done;
        let tail = if job.partials.is_empty() { 3 } else { 4 } + extra as u32;
        while done < sources.len() {
            if done - resumed >= MAX_SOURCES || self.stop() {
                break;
            }
            if !self.room(tail) {
                break;
            }
            let source = file_name(&sources[done]);
            let Some(bytes) = self
                .store
                .get(self.cdn_key, &sources[done], CDN_READ_LIMIT)
                .map_err(|e| format!("{e}:{source}"))?
            else {
                // Removed since the LIST (an abandoned plan): relist next execution.
                self.report
                    .errors
                    .push(format!("summary_vanished:{source}"));
                break;
            };
            acc.fold(bytes, 1).map_err(|e| format!("{e}:{source}"))?;
            acc.prune();
            done += 1;
            self.report.sources += 1;
        }
        let rows: u64 = sources[..done].iter().map(|k| name_rows(k)).sum();
        let defer_final = job.final_key.is_some() && done == sources.len() && self.write_stop();
        if defer_final && self.report.stopped.is_none() {
            self.report.stopped = Some("vcpu");
        }
        let mut wrote_partial = false;
        let (key, mut objects, is_final) = match job
            .final_key
            .filter(|_| done == sources.len() && !defer_final)
        {
            Some(final_key) => {
                let body = acc.write(job.start, Some(job.unit), job.unit.label())?;
                (Some(final_key.to_owned()), vec![body], true)
            }
            None if (job.final_key.is_some() || need_partial || need_open) && done > resumed => {
                let objects = pack_partial(&acc.encode_partial(), CDN_READ_LIMIT)?;
                if !self.room(objects.len() as u32) {
                    return Ok(Step::Pending);
                }
                let key = format!("{}partial={}", job.folder, covering(&names[..done], rows));
                wrote_partial = true;
                (Some(key), objects, false)
            }
            None if job.final_key.is_none() => (None, Vec::new(), false),
            None => return Ok(Step::Pending),
        };
        if let Some(key) = key {
            self.report.bytes_written += objects.iter().map(|o| o.len() as u64).sum::<u64>();
            // Continuation objects go first, so a head never names a part that was not written.
            for (i, part) in objects.drain(1..).enumerate() {
                self.store.put(&part_key(&key, i + 1), part)?;
            }
            let body = objects.pop().ok_or("bundle_body_missing")?;
            if is_final {
                if let Some(mut coverage) = job.coverage {
                    let hash = hex::encode(Sha256::digest(&body));
                    let relative = key
                        .strip_prefix(&self.bundles)
                        .ok_or("catalog_bundle_key_invalid")?;
                    let canonical = format!("{}sha256={hash}/{relative}", self.bundles);
                    let bytes = body.len() as u64;
                    let md5 = self.store.put_verified(&canonical, body)?;
                    coverage.file = super::worker::Output {
                        key: canonical,
                        rows,
                        bytes,
                        md5,
                        metadata: None,
                    };
                    self.publication = Some(coverage);
                } else {
                    self.store.put(&key, body)?;
                }
            } else {
                self.store.put(&key, body)?;
            }
            let parts = format!("{}-p", key.trim_end_matches(".parquet"));
            let stale: Vec<String> = job
                .partials
                .into_iter()
                .filter(|p| *p != key && !p.starts_with(&parts))
                .collect();
            if !stale.is_empty() && self.room(1) {
                match self.store.delete_objects(&stale) {
                    Ok(result) => self.report.deleted += result.deleted.len() as u64,
                    Err(error) => self.report.errors.push(error),
                }
            }
        }
        if is_final {
            self.report.rows += rows;
            return Ok(Step::Done);
        }
        if wrote_partial {
            self.report.partials += 1;
        }
        if let Some((state, open_covered)) = open.as_mut() {
            if done.saturating_sub(*open_covered) >= OPEN_BUNDLE_MIN && !self.write_stop() {
                let end_ms = sources[..done]
                    .iter()
                    .filter_map(|k| super::catalog::file_bounds(k))
                    .map(|(_, end)| end * 1000)
                    .max()
                    .unwrap_or(job.start * 1000);
                let open_key = open_bundle_key(&self.bundles, job.start, end_ms, &sources[..done]);
                let body = acc.write(job.start, Some(Unit::Hour), Unit::Hour.label())?;
                self.report.bytes_written += body.len() as u64;
                if self.catalog.version == super::catalog::VERSION {
                    let hash = hex::encode(Sha256::digest(&body));
                    let relative = open_key
                        .strip_prefix(&self.bundles)
                        .ok_or("catalog_bundle_key_invalid")?;
                    let canonical = format!("{}sha256={hash}/{relative}", self.bundles);
                    let bytes = body.len() as u64;
                    let md5 = self.store.put_verified(&canonical, body)?;
                    let old = state.open_bundles.insert(job.start, canonical.clone());
                    self.retire(state, old, Some(&canonical));
                    self.publication = Some(super::catalog::bundles::Coverage {
                        file: super::worker::Output {
                            key: canonical,
                            rows,
                            bytes,
                            md5,
                            metadata: None,
                        },
                        start: job.start,
                        end: job.start + HOUR_S,
                        windows: sources[..done].iter().cloned().collect(),
                        hours: BTreeMap::new(),
                        built_generation: self.catalog.generation,
                    });
                } else {
                    self.store.put(&open_key, body)?;
                    let old = state.open_bundles.insert(job.start, open_key.clone());
                    self.retire(state, old, Some(&open_key));
                }
            }
            if self.publication.is_some()
                || (need_partial && sources.len().saturating_sub(done) >= OPEN_BATCH_SOURCES)
                || (need_open && done < sources.len())
            {
                Ok(Step::Pending)
            } else {
                Ok(Step::Done)
            }
        } else {
            Ok(Step::Pending)
        }
    }

    /// The state saved in the partial at `key` and how many objects hold it; `Some(Err)` when it
    /// can never be read as stored, `None` when out of calls.
    fn read_partial(&self, key: &str, unit: Unit) -> Result<Resume, String> {
        let get = |key: &str| match self.store.get(self.cdn_key, key, CDN_READ_LIMIT) {
            Ok(Some(bytes)) => Ok(Ok(bytes)),
            Ok(None) => Ok(Err("partial_vanished".to_owned())),
            Err(error) if unreadable(&error) => Ok(Err(error)),
            Err(error) => Err(error),
        };
        let head = match get(key)? {
            Ok(head) => head,
            Err(error) => return Ok(Some(Err(error))),
        };
        let (raw, parts) = if head.starts_with(PARTIAL_MAGIC) {
            (head, 1)
        } else if let Some(parts) = packed_parts(&head) {
            let mut objects = vec![head];
            for i in 1..parts {
                if !self.room(3) {
                    return Ok(None);
                }
                match get(&part_key(key, i))? {
                    Ok(part) => objects.push(part),
                    Err(error) => return Ok(Some(Err(error))),
                }
            }
            match unpack_partial(&objects) {
                Ok(raw) => (raw, parts),
                Err(error) => return Ok(Some(Err(error))),
            }
        } else {
            return Ok(Some(Err("partial_unreadable".into())));
        };
        Ok(Some(
            Acc::decode_partial(&raw, unit).map(|acc| (acc, parts)),
        ))
    }

    fn retire(&self, state: &mut State, old: Option<String>, new: Option<&str>) {
        if let Some(old) = old.filter(|old| Some(old.as_str()) != new) {
            state.retire.insert(old, self.now + RETIRE_GRACE_S);
        }
    }

    /// Brings hour `hour`'s bundle up to date with its windows.
    fn hour(&mut self, state: &mut State, hour: i64) -> Result<Step, String> {
        let day = hour.div_euclid(DAY_S) * DAY_S;
        let had = state.hours.get(&hour).cloned();
        let present = match self.present(day)? {
            Some(hours) => hours.contains(&hour),
            None => return Ok(Step::Pending),
        };
        if !present && had.is_none() {
            return Ok(Step::Done);
        }
        let folder = format!("{}{}", self.windows, hour_dir(hour));
        let listing_prefix = if self.catalog.version == super::catalog::VERSION {
            format!("{folder}partial=")
        } else {
            folder.clone()
        };
        let Some(listed) = self.list_all(&listing_prefix, None)? else {
            return Ok(Step::Pending);
        };
        let (mut windows, mut partials) = (Vec::new(), Vec::new());
        for o in listed {
            let name = file_name(&o.key);
            if name.starts_with("summary=") {
                windows.push(o.key);
            } else if name.starts_with("partial=") {
                partials.push(o.key);
            }
        }
        if self.catalog.version == super::catalog::VERSION {
            let Some(committed) = self.catalog_windows(state, hour, true)? else {
                return Ok(Step::Pending);
            };
            windows = committed;
        }
        if windows.is_empty() {
            if had.is_some() {
                self.retire(state, had, None);
                state.hours.remove(&hour);
                state.dirty.insert(day);
            }
            return Ok(Step::Done);
        }
        let key = hour_bundle_key(&self.bundles, hour, &windows);
        if had.as_deref() == Some(key.as_str()) {
            state.inventory = None;
            return Ok(Step::Done);
        }
        let coverage = (self.catalog.version == super::catalog::VERSION).then(|| {
            super::catalog::bundles::Coverage {
                file: Default::default(),
                start: hour,
                end: hour + HOUR_S,
                windows: windows.iter().cloned().collect(),
                hours: BTreeMap::new(),
                built_generation: self.catalog.generation,
            }
        });
        let step = self.build(
            Job {
                sources: windows,
                partials,
                final_key: Some(&key),
                folder,
                start: hour,
                unit: Unit::Hour,
                coverage,
            },
            None,
        )?;
        if let Step::Done = step {
            self.retire(state, had, Some(&key));
            let prev_open = state.open_bundles.remove(&hour);
            self.retire(state, prev_open, Some(&key));
            state.hours.insert(hour, key);
            state.dirty.insert(day);
            self.report.hours += 1;
            if self.publication.is_some() {
                return Ok(Step::Pending);
            }
            state.inventory = None;
        }
        Ok(step)
    }

    /// Pre-folds full batches of an open hour's windows into a resumable `partial=` without
    /// writing `hour_bundle=`, so closing the hour only has the final tail left to fold.
    fn open_hour(&mut self, state: &mut State, hour: i64) -> Result<Step, String> {
        let day = hour.div_euclid(DAY_S) * DAY_S;
        if let Some(hours) = self.present.get(&day) {
            if !hours.contains(&hour) {
                return Ok(Step::Done);
            }
        }
        let folder = format!("{}{}", self.windows, hour_dir(hour));
        let listing_prefix = if self.catalog.version == super::catalog::VERSION {
            format!("{folder}partial=")
        } else {
            folder.clone()
        };
        let Some(listed) = self.list_all(&listing_prefix, None)? else {
            return Ok(Step::Pending);
        };
        let (mut windows, mut partials) = (Vec::new(), Vec::new());
        for o in listed {
            let name = file_name(&o.key);
            if name.starts_with("summary=") {
                windows.push(o.key);
            } else if name.starts_with("partial=") {
                partials.push(o.key);
            }
        }
        if self.catalog.version == super::catalog::VERSION {
            let Some(committed) = self.catalog_windows(state, hour, false)? else {
                return Ok(Step::Pending);
            };
            windows = committed;
        }
        let mut sorted_names: Vec<&str> = windows.iter().map(|k| file_name(k)).collect();
        sorted_names.sort_unstable();
        let open_covered = state
            .open_bundles
            .get(&hour)
            .and_then(|key| parse_name(key))
            .filter(|name| {
                name.kind == NameKind::Open
                    && name.count > 0
                    && name.count <= sorted_names.len()
                    && fingerprint(sorted_names[..name.count].iter().copied()) == name.fp
            })
            .map_or(0, |name| name.count);
        let step = self.build(
            Job {
                sources: windows,
                partials,
                final_key: None,
                folder,
                start: hour,
                unit: Unit::Hour,
                coverage: None,
            },
            Some((state, open_covered)),
        )?;
        if matches!(step, Step::Done) {
            state.inventory = None;
        }
        Ok(step)
    }

    /// Brings day `day`'s bundle up to date with its hour bundles.
    fn day(&mut self, state: &mut State, day: i64) -> Result<Step, String> {
        let mut sources: Vec<String> = state
            .hours
            .range(day..day + DAY_S)
            .map(|(_, key)| key.clone())
            .collect();
        let mut committed_hours = BTreeMap::new();
        if self.catalog.version == super::catalog::VERSION {
            sources.clear();
            for (hour, reference) in self.catalog.hour_bundles.range(day..day + DAY_S) {
                if state
                    .catalog_hours
                    .get(hour)
                    .is_none_or(|(saved, _)| saved != reference)
                {
                    if !self.room(3) || !super::catalog::bundles::admitted(self.store) {
                        return Ok(Step::Pending);
                    }
                    let coverage = super::catalog::bundles::load(self.store, reference)?;
                    state
                        .catalog_hours
                        .insert(*hour, (reference.clone(), coverage.file.key));
                }
                sources.push(state.catalog_hours[hour].1.clone());
                committed_hours.insert(*hour, reference.clone());
            }
        }
        let had = state.days.get(&day).cloned();
        if sources.is_empty() {
            self.retire(state, had, None);
            state.days.remove(&day);
            return Ok(Step::Done);
        }
        let key = day_bundle_key(&self.bundles, day, &sources);
        if had.as_deref() == Some(key.as_str()) {
            return Ok(Step::Done);
        }
        let folder = format!("{}{}", self.bundles, day_dir(day));
        let Some(listed) = self.list_all(&folder, None)? else {
            return Ok(Step::Pending);
        };
        let partials = listed
            .into_iter()
            .map(|o| o.key)
            .filter(|k| file_name(k).starts_with("partial="))
            .collect();
        let step = self.build(
            Job {
                sources,
                partials,
                final_key: Some(&key),
                folder,
                start: day,
                unit: Unit::Day,
                coverage: (self.catalog.version == super::catalog::VERSION).then(|| {
                    super::catalog::bundles::Coverage {
                        file: Default::default(),
                        start: day,
                        end: day + DAY_S,
                        windows: BTreeSet::new(),
                        hours: committed_hours,
                        built_generation: self.catalog.generation,
                    }
                }),
            },
            None,
        )?;
        if let Step::Done = step {
            self.retire(state, had, Some(&key));
            state.days.insert(day, key);
            self.report.days += 1;
            if self.publication.is_some() {
                return Ok(Step::Pending);
            }
        }
        Ok(step)
    }

    /// Deletes replaced bundles whose grace has passed.
    fn delete_retired(&mut self, state: &mut State) {
        let due: Vec<String> = state
            .retire
            .iter()
            .filter(|(key, at)| **at <= self.now && key.starts_with(&self.bundles))
            .map(|(key, _)| key.clone())
            .collect();
        if due.is_empty() {
            return;
        }
        if !self.room(1) {
            self.report.more_pending = true;
            return;
        }
        match self.store.delete_objects(&due) {
            Ok(result) => {
                self.report.deleted += result.deleted.len() as u64;
                for key in result.deleted {
                    state.retire.remove(&key);
                }
            }
            Err(error) => self.report.errors.push(error),
        }
    }
}

/// Bundles closed hours (forward from the last one done), then closed days, then every
/// `REVISIT_INTERVAL_S` re-lists the last `REVISIT_S` of bundled hours and rebuilds any with late
/// or removed windows (and their days), and finally deletes replaced bundles past their grace.
/// `reserve` calls stay free for the caller; it must be at least 1, which the state write uses.
pub fn summary_bundle<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    reserve: u32,
) -> Result<BundleReport, String> {
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let scope = scope(store.service);
    let mut ctx = Ctx {
        store,
        cdn_key,
        reserve,
        windows: summary::summary_prefix(&scope, TABLE),
        bundles: bundle_prefix(&scope, TABLE),
        now: runtime.now() as i64,
        present: HashMap::new(),
        report: BundleReport::default(),
        catalog: super::catalog::Head::default(),
        publication: None,
    };
    if ctx.stop() || !ctx.room(2) {
        ctx.report.more_pending = true;
        return Ok(ctx.report);
    }
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("cron:summary_bundle");
    }
    let record = runtime.admin_get(STATE_KEY)?;
    let mut state: State = record
        .as_ref()
        .and_then(|r| serde_json::from_value(r.value.clone()).ok())
        .unwrap_or_default();
    let original = state.clone();
    let now = ctx.now;
    // A bundle built last execution is published first, then the run goes on with the watermark
    // read after that commit. Publishing is idempotent, so a run that ends before saving the
    // state publishes the same coverage again next time.
    if let Some(publication) = state.publication.as_ref() {
        match super::catalog::bundles::publish(store, publication, reserve + 1) {
            Ok(true) => {
                if publication.end - publication.start == DAY_S {
                    ctx.report.days += 1;
                } else {
                    ctx.report.hours += 1;
                }
                state.publication = None;
                state.inventory = None;
            }
            Ok(false) => {
                ctx.report.more_pending = true;
                return Ok(ctx.report);
            }
            Err(error) if error == "catalog_bundle_not_active" => {
                ctx.report.errors.push(error);
                state.publication = None;
                state.inventory = None;
            }
            Err(error) => return Err(error),
        }
    }
    if !ctx.room(2) {
        // Out of calls right after publishing: save that and resume next execution.
        if state != original {
            let _ = runtime.admin_cas(
                STATE_KEY,
                record.as_ref().map(|r| r.generation.as_str()),
                &serde_json::to_value(&state).map_err(|_| "bundle_state_encode")?,
            )?;
        }
        ctx.report.more_pending = true;
        ctx.report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
        return Ok(ctx.report);
    }
    let watermark = runtime.admin_get(WATERMARK_KEY)?;
    ctx.catalog = super::catalog::head(
        watermark
            .as_ref()
            .map(|r| &r.value)
            .unwrap_or(&serde_json::Value::Null),
    )?;
    ctx.report.deleted += super::catalog::bundles::cleanup(store, watermark.as_ref(), reserve + 1)?;
    let swept_at = watermark
        .as_ref()
        .filter(|r| r.value["sweep"]["after"].as_str() == Some(""))
        .and_then(|r| r.value["sweep"]["at"].as_i64())
        .unwrap_or(0);
    let closed = watermark
        .as_ref()
        .and_then(|r| r.value["key"].as_str())
        .and_then(|key| raw_key_minute(key, &raw_prefix(store.service)))
        .map(|minute| {
            (minute + 60).max(swept_at).min(now)
                - i64::from(store.service.log_period)
                - CLOSE_SLACK_S
        });
    let today = now.div_euclid(DAY_S) * DAY_S;
    let retention = i64::from(store.service.log_retention_days);
    let lookback = if retention == 0 {
        LOOKBACK_DAYS
    } else {
        retention.min(LOOKBACK_DAYS)
    };
    let floor = today - lookback * DAY_S;
    if state.next_hour.is_none() {
        state.revisited_at = now;
    }
    let mut next = state.next_hour.unwrap_or(floor).max(floor);
    state.next_hour = Some(next);

    let result = (|| -> Result<(), String> {
        let ctx = &mut ctx;
        let state = &mut state;
        let mut held = false;
        if let Some(closed) = closed {
            while next + HOUR_S <= closed {
                if ctx.report.stopped.is_some() || (ctx.report.bytes_written > 0 && ctx.stop()) {
                    ctx.report.more_pending = true;
                    return Ok(());
                }
                match ctx.hour(state, next) {
                    Ok(Step::Done) => {}
                    Ok(Step::Pending) => {
                        ctx.report.more_pending = true;
                        return Ok(());
                    }
                    // The cursor cannot pass an hour it could not bundle, but bundled hours and
                    // their days still get revisited and built.
                    Err(error) => {
                        ctx.report.errors.push(error);
                        ctx.report.more_pending = true;
                        held = true;
                        break;
                    }
                }
                next += HOUR_S;
                state.next_hour = Some(next);
                if next.rem_euclid(DAY_S) == 0 {
                    state.dirty.insert(next - DAY_S);
                }
            }
        }
        let revisit_from = (next - REVISIT_S).max(floor);
        if state.revisit.is_some() || now - state.revisited_at >= REVISIT_INTERVAL_S {
            let mut hour = state.revisit.unwrap_or(revisit_from).max(revisit_from);
            while hour < next {
                if ctx.report.stopped.is_some() || (ctx.report.bytes_written > 0 && ctx.stop()) {
                    state.revisit = Some(hour);
                    ctx.report.more_pending = true;
                    return Ok(());
                }
                match ctx.hour(state, hour) {
                    Ok(Step::Done) => {}
                    Ok(Step::Pending) => {
                        state.revisit = Some(hour);
                        ctx.report.more_pending = true;
                        return Ok(());
                    }
                    Err(error) => {
                        ctx.report.errors.push(error);
                        ctx.report.more_pending = true;
                    }
                }
                hour += HOUR_S;
            }
            state.revisit = None;
            state.revisited_at = now;
        }
        for day in state.dirty.clone() {
            if day + DAY_S > next {
                continue;
            }
            if ctx.report.stopped.is_some() || (ctx.report.bytes_written > 0 && ctx.stop()) {
                ctx.report.more_pending = true;
                return Ok(());
            }
            match ctx.day(state, day) {
                Ok(Step::Done) => {
                    state.dirty.remove(&day);
                }
                Ok(Step::Pending) => {
                    ctx.report.more_pending = true;
                    return Ok(());
                }
                // The day stays dirty for the next execution; later days go on.
                Err(error) => {
                    ctx.report.errors.push(error);
                    ctx.report.more_pending = true;
                }
            }
        }
        if closed.is_some() && !held {
            let mut open = next;
            while open <= now {
                if ctx.report.stopped.is_some() || ctx.report.bytes_written > 0 {
                    break;
                }
                match ctx.open_hour(state, open) {
                    Ok(Step::Done) => {}
                    Ok(Step::Pending) => {
                        ctx.report.more_pending = true;
                        return Ok(());
                    }
                    Err(error) => {
                        ctx.report.errors.push(error);
                        ctx.report.more_pending = true;
                    }
                }
                open += HOUR_S;
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        ctx.report.errors.push(error);
        ctx.report.more_pending = true;
    }
    if let Some(publication) = ctx.publication.take() {
        state.publication = Some(publication);
    }
    if ctx.report.stopped != Some("wall_time") {
        ctx.delete_retired(&mut state);
    }
    let keep_from = (next - REVISIT_S).max(floor).div_euclid(DAY_S) * DAY_S - DAY_S;
    let dirty = state.dirty.clone();
    state
        .hours
        .retain(|h, _| *h >= keep_from || dirty.contains(&(h.div_euclid(DAY_S) * DAY_S)));
    state
        .open_bundles
        .retain(|h, _| *h >= keep_from || dirty.contains(&(h.div_euclid(DAY_S) * DAY_S)));
    state.days.retain(|d, _| *d >= keep_from);
    state.catalog_hours.retain(|h, _| *h >= keep_from);
    if state != original {
        let value = serde_json::to_value(&state).map_err(|_| "bundle_state_encode")?;
        if !runtime.admin_cas(
            STATE_KEY,
            record.as_ref().map(|r| r.generation.as_str()),
            &value,
        )? {
            ctx.report.more_pending = true;
        }
    }
    ctx.report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
    Ok(ctx.report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::convert;

    fn window(offset: i64, custom: bool) -> Vec<u8> {
        let base = 1_800_000_000 + offset;
        let mut plain = Vec::new();
        for i in 0..3 {
            plain.extend_from_slice(
                serde_json::json!({"timestamp": time::OffsetDateTime::from_unix_timestamp(base + i).unwrap().format(&time::format_description::well_known::Rfc3339).unwrap(), "tenant": "acme"})
                    .to_string()
                    .as_bytes(),
            );
            plain.push(b'\n');
        }
        let gz = zstd::stream::encode_all(plain.as_slice(), 1).unwrap();
        let schema = convert::RequestSchema::new(if custom {
            vec![convert::CustomColumn {
                name: "tenant".into(),
                kind: convert::Kind::Utf8,
                slot_only: false,
            }]
        } else {
            Vec::new()
        });
        let rows = convert::convert_object_capped(&gz, 300, base, u64::MAX, &schema).unwrap();
        let batch = rows.windows.into_values().next().unwrap();
        summary::write_summary(&[&batch], base).unwrap()
    }

    fn tenant_total(bytes: Vec<u8>) -> Option<i64> {
        let mut acc = Acc::default();
        acc.fold(bytes, 1).unwrap();
        acc.fields
            .get("tenant")
            .filter(|f| acc.covered("tenant", f))
            .map(|f| f.total)
    }

    #[test]
    fn resumed_partials_keep_custom_field_coverage() {
        for (sources, want) in [
            ([true, true, true], Some(9)),
            ([true, true, false], None),
            ([false, true, true], None),
            ([true, false, true], None),
        ] {
            let windows: Vec<Vec<u8>> = sources
                .iter()
                .enumerate()
                .map(|(i, custom)| window(i as i64 * 300, *custom))
                .collect();
            let mut first = Acc::default();
            for w in &windows[..2] {
                first.fold(w.clone(), 1).unwrap();
            }
            let partial = first.write(1_800_000_000, None, "hour").unwrap();
            let mut resumed = Acc::default();
            resumed.fold(partial, 2).unwrap();
            resumed.fold(windows[2].clone(), 1).unwrap();
            let fin = resumed
                .write(1_800_000_000, Some(Unit::Hour), "hour")
                .unwrap();
            let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
            let one_shot = merge(&refs, 1_800_000_000, Unit::Hour).unwrap();
            assert_eq!(tenant_total(fin.clone()), want, "{sources:?}");
            assert_eq!(fin, one_shot, "{sources:?}");
        }
    }

    /// A window summary over rows with the v3 columns; ASNs past `SERIES_ASN_K` force cuts.
    fn rich_window(offset: i64) -> Vec<u8> {
        let base = 1_800_000_000 + offset;
        let fmt = &time::format_description::well_known::Rfc3339;
        let mut plain = Vec::new();
        for i in 0..400_i64 {
            let ts = time::OffsetDateTime::from_unix_timestamp(base + i % 300).unwrap();
            let row = serde_json::json!({
                "timestamp": ts.format(fmt).unwrap(),
                "status": ([200, 503, 404][(i % 3) as usize]),
                "cache": (["HIT", "MISS"][(i % 2) as usize]),
                "ttfb": 0.001 * (i % 40) as f64,
                "elapsed": 1_000 + i * 37,
                "ottfb": 500 + i,
                "tcp_rtt": 2_000 + i % 90,
                "ploss": 0.01,
                "asn": i % 150 + offset,
                "proto": (["HTTP/2", "HTTP/3"][(i % 2) as usize]),
                "country": (["US", "DE"][(i % 2) as usize]),
                // 60 cities per window, 240 over an hour's four: past `SERIES_CITY_K`.
                "city": format!("c{}", i % 60 + offset / 5),
                "lat": 1.5,
                "lon": -2.5,
                "waf_sig": (["VERIFIED-BOT.googlebot", "x"][(i % 2) as usize]),
                "req_header_bytes": 100 * (i % 50),
                "url": (["/a.js", "/b.css", "/c"][(i % 3) as usize]),
                "rid": format!("r{offset}-{i}"),
            });
            plain.extend_from_slice(row.to_string().as_bytes());
            plain.push(b'\n');
        }
        let zst = zstd::stream::encode_all(plain.as_slice(), 1).unwrap();
        let rows = convert::convert_object(&zst, 300, base).unwrap();
        let batch = rows.windows.into_values().next().unwrap();
        summary::write_summary(&[&batch], base).unwrap()
    }

    fn flp_summary(bytes: &[u8]) -> Option<String> {
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
            .unwrap()
            .metadata()
            .file_metadata()
            .key_value_metadata()?
            .iter()
            .find(|kv| kv.key == "flp_summary")?
            .value
            .clone()
    }

    #[test]
    fn binary_partials_resume_v3_state_to_the_same_bytes() {
        let windows: Vec<Vec<u8>> = (0..4).map(|i| rich_window(i * 300)).collect();
        let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
        for unit in [Unit::Hour, Unit::Day] {
            let mut first = Acc::new(unit);
            for w in &windows[..2] {
                first.fold(w.clone(), 1).unwrap();
                first.prune();
            }
            let partial = first.encode_partial();
            let mut resumed = Acc::decode_partial(&partial, unit).unwrap();
            assert_eq!(resumed.encode_partial().len(), partial.len());
            for w in &windows[2..] {
                resumed.fold(w.clone(), 1).unwrap();
                resumed.prune();
            }
            let fin = resumed
                .write(1_800_000_000, Some(unit), unit.label())
                .unwrap();
            assert_eq!(fin, merge(&refs, 1_800_000_000, unit).unwrap());
            assert_eq!(flp_summary(&fin).as_deref(), Some("3"));
        }
        let mut acc = Acc::new(Unit::Hour);
        for w in &windows {
            acc.fold(w.clone(), 1).unwrap();
            acc.prune();
        }
        let asn = &acc.groups[1]["asn"];
        assert_eq!(asn.values.len(), summary::SERIES_ASN_K);
        assert!(asn.truncated);
        assert_eq!(asn.total, 1600);
        assert_eq!(acc.sample.len(), summary::SAMPLE_K);
        assert!(acc
            .sample
            .windows(2)
            .all(|w| (w[0].0, w[0].1) <= (w[1].0, w[1].1)));
        assert!(Acc::decode_partial(&acc.encode_partial()[..40], Unit::Hour).is_err());
    }

    #[test]
    fn packed_partials_split_into_objects_under_the_limit_and_resume_to_the_same_bytes() {
        let windows: Vec<Vec<u8>> = (0..4).map(|i| rich_window(i * 300)).collect();
        let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
        let mut first = Acc::new(Unit::Day);
        for w in &windows[..2] {
            first.fold(w.clone(), 1).unwrap();
            first.prune();
        }
        let raw = first.encode_partial();
        let objects = pack_partial(&raw, 4096).unwrap();
        assert!(objects.len() > 2, "{} objects", objects.len());
        assert!(objects.iter().all(|o| o.len() <= 4096));
        assert!(objects.iter().map(Vec::len).sum::<usize>() < raw.len());
        assert_eq!(packed_parts(&objects[0]), Some(objects.len()));
        assert_eq!(packed_parts(&raw), None);
        assert_eq!(unpack_partial(&objects).unwrap(), raw);
        assert!(unpack_partial(&objects[..objects.len() - 1]).is_err());

        let mut resumed =
            Acc::decode_partial(&unpack_partial(&objects).unwrap(), Unit::Day).unwrap();
        for w in &windows[2..] {
            resumed.fold(w.clone(), 1).unwrap();
            resumed.prune();
        }
        let fin = resumed
            .write(1_800_000_000, Some(Unit::Day), Unit::Day.label())
            .unwrap();
        assert_eq!(fin, merge(&refs, 1_800_000_000, Unit::Day).unwrap());

        let head = "p/year=2027/month=01/day=15/partial=c2-n9-h00000000000000ab.parquet";
        let part = part_key(head, 1);
        assert_eq!(
            part,
            "p/year=2027/month=01/day=15/partial=c2-n9-h00000000000000ab-p1.parquet"
        );
        assert_eq!(parse_name(&part), None);
    }

    #[test]
    fn binary_partials_keep_gated_field_coverage() {
        let windows: Vec<Vec<u8>> = (0..2).map(|i| rich_window(i * 300)).collect();
        let has = |bytes: Vec<u8>| {
            let mut acc = Acc::default();
            acc.fold(bytes, 1).unwrap();
            let series = GROUPS.iter().position(|x| x.name == "series5").unwrap();
            ["transport", "origin", "urls", "quality", "city"].map(|g| match g {
                "city" => acc.groups[series].contains_key("city"),
                _ => {
                    let at = GROUPS.iter().position(|x| x.name == g).unwrap();
                    !acc.groups[at].is_empty()
                }
            })
        };
        let mut first = Acc::default();
        first.fold(windows[0].clone(), 1).unwrap();
        // A source from before the transport, origin, urls, quality and city rows.
        first.sources += 1;
        let mut resumed = Acc::decode_partial(&first.encode_partial(), Unit::Hour).unwrap();
        resumed.fold(windows[1].clone(), 1).unwrap();
        let fin = resumed
            .write(1_800_000_000, Some(Unit::Hour), "hour")
            .unwrap();
        assert_eq!(has(fin), [false; 5]);
        let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
        assert_eq!(
            has(merge(&refs, 1_800_000_000, Unit::Hour).unwrap()),
            [true; 5]
        );
    }

    #[test]
    fn hours_keep_the_candidate_margin_and_days_write_the_planned_k() {
        use arrow_array::{Array, BooleanArray, StringArray};
        // `series5` values of `want` and whether the bundle flags that group field as cut.
        let read = |bundle: &[u8], want: &str| {
            let mut asns = BTreeSet::new();
            let mut flagged = None;
            for batch in
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bundle.to_vec()))
                    .unwrap()
                    .build()
                    .unwrap()
            {
                let batch = batch.unwrap();
                let col = |name: &str| batch.column_by_name(name).unwrap().clone();
                let (kind, field, value, cut) =
                    (col("bundle"), col("field"), col("value"), col("truncated"));
                let kind = kind.as_any().downcast_ref::<StringArray>().unwrap();
                let field = field.as_any().downcast_ref::<StringArray>().unwrap();
                let value = value.as_any().downcast_ref::<StringArray>().unwrap();
                let cut = cut.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..batch.num_rows() {
                    if field.is_null(i) || field.value(i) != want {
                        continue;
                    }
                    match kind.value(i) {
                        "series5" => {
                            asns.insert(value.value(i).to_owned());
                        }
                        "dim_totals" if value.value(i) == "series5" => flagged = Some(cut.value(i)),
                        _ => {}
                    }
                }
            }
            (asns.len(), flagged)
        };
        let windows: Vec<Vec<u8>> = (0..4).map(|i| rich_window(i * 300)).collect();
        let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
        // A day folds hour bundles, so an hour must keep the 4x margin for the day's ranking.
        let hour = merge(&refs, 1_800_000_000, Unit::Hour).unwrap();
        assert_eq!(read(&hour, "asn"), (summary::SERIES_ASN_K, Some(true)));
        assert_eq!(read(&hour, "city"), (summary::SERIES_CITY_K, Some(true)));
        let day = merge(&[&hour], 1_800_000_000, Unit::Day).unwrap();
        assert_eq!(read(&day, "asn"), (SERIES_ASN_TOP_K, Some(true)));
        assert_eq!(read(&day, "city"), (SERIES_CITY_TOP_K, Some(true)));
    }

    /// Every resumed execution decodes the partial first and encodes it last, on top of the
    /// folds: print that cost with an hour's worth of state (ASN dims past `DIM_K`).
    #[test]
    #[ignore = "measurement; run with --release --ignored --nocapture"]
    fn partial_round_trip_cost() {
        let mut acc = Acc::new(Unit::Hour);
        for i in 0..60 {
            acc.fold(rich_window(i * 60), 1).unwrap();
            acc.prune();
        }
        assert_eq!(acc.groups[0]["asn"].values.len(), summary::DIM_K);
        let t = std::time::Instant::now();
        let partial = acc.encode_partial();
        let encode = t.elapsed();
        let t = std::time::Instant::now();
        let back = Acc::decode_partial(&partial, Unit::Hour).unwrap();
        let decode = t.elapsed();
        assert_eq!(back.encode_partial().len(), partial.len());
        println!(
            "partial {} B: encode {:.1} ms, decode {:.1} ms",
            partial.len(),
            encode.as_secs_f64() * 1000.0,
            decode.as_secs_f64() * 1000.0
        );
    }

    #[test]
    fn a_source_of_another_version_marks_the_bundle_mixed() {
        let v3 = rich_window(0);
        let batch = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(v3.clone()))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let v2 = convert::write_with_metadata(
            summary::schema(),
            &[&batch],
            vec![KeyValue::new("flp_summary".into(), "2".to_owned())],
        )
        .unwrap();
        let start = 1_800_000_000;
        let clean = merge(&[&v3, &v3], start, Unit::Hour).unwrap();
        assert_eq!(flp_summary(&clean).as_deref(), Some("3"));
        let mixed = merge(&[&v3, &v2], start, Unit::Hour).unwrap();
        assert_eq!(flp_summary(&mixed).as_deref(), Some("mixed"));
        let day = merge(&[&clean, &mixed], start, Unit::Day).unwrap();
        assert_eq!(flp_summary(&day).as_deref(), Some("mixed"));

        let mut acc = Acc::new(Unit::Hour);
        acc.fold(v2, 1).unwrap();
        let resumed = Acc::decode_partial(&acc.encode_partial(), Unit::Hour).unwrap();
        assert!(resumed.foreign);
        let partial = acc.write(start, None, "hour").unwrap();
        assert_eq!(flp_summary(&partial).as_deref(), Some("mixed"));
    }

    #[test]
    fn names_round_trip_and_fingerprints_follow_the_sorted_sources() {
        let windows = vec![
            "p/year=2027/month=01/day=15/hour=08/summary=1800000060-1800000120-p1s0-n3.parquet"
                .to_owned(),
            "p/year=2027/month=01/day=15/hour=08/summary=1800000000-1800000060-p1s0-n2.parquet"
                .to_owned(),
        ];
        let key = hour_bundle_key("h/", 1_800_000_000, &windows);
        let name = parse_name(&key).unwrap();
        assert_eq!(
            (name.kind, name.unit, name.count, name.rows),
            (NameKind::Hour, 8, 2, 5)
        );
        let mut reversed = windows.clone();
        reversed.reverse();
        assert_eq!(hour_bundle_key("h/", 1_800_000_000, &reversed), key);
        let day = day_bundle_key("d/", 1_799_971_200, std::slice::from_ref(&key));
        assert!(day.starts_with("d/year=2027/month=01/day=15/day_bundle=c1-n5-h"));
        let name = parse_name(&day).unwrap();
        assert_eq!((name.kind, name.unit, name.count), (NameKind::Day, 15, 1));
        assert_eq!(
            parse_name("x/partial=c1-n5-h00000000000000ff.parquet")
                .unwrap()
                .fp,
            255
        );
        assert!(parse_name("x/summary=1-2-p1s0-n1.parquet").is_none());
    }

    #[test]
    fn decode_partial_rejects_oversized_element_counts_without_oom_and_preserves_valid_roundtrip() {
        use std::alloc::{GlobalAlloc, Layout, System};

        thread_local! {
            static ALLOC_CAP: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
        }

        struct CappedAlloc;

        // SAFETY: Delegates directly to `std::alloc::System` with unmodified `layout`/`ptr`
        // arguments when within the thread-local test cap, or returns `null_mut()` per the
        // `GlobalAlloc` contract when a single allocation exceeds the Wasm memory ceiling.
        unsafe impl GlobalAlloc for CappedAlloc {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                if ALLOC_CAP.with(|cap| layout.size() > cap.get()) {
                    return std::ptr::null_mut();
                }
                // SAFETY: Caller upholds `GlobalAlloc::alloc` preconditions (`layout.size() > 0`).
                unsafe { System.alloc(layout) }
            }
            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                if ALLOC_CAP.with(|cap| layout.size() > cap.get()) {
                    return std::ptr::null_mut();
                }
                // SAFETY: Caller upholds `GlobalAlloc::alloc_zeroed` preconditions.
                unsafe { System.alloc_zeroed(layout) }
            }
            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                if ALLOC_CAP.with(|cap| new_size > cap.get()) {
                    return std::ptr::null_mut();
                }
                // SAFETY: Caller upholds `GlobalAlloc::realloc` preconditions.
                unsafe { System.realloc(ptr, layout, new_size) }
            }
            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                // SAFETY: Caller upholds `GlobalAlloc::dealloc` preconditions.
                unsafe { System.dealloc(ptr, layout) }
            }
        }

        #[global_allocator]
        static TEST_ALLOC: CappedAlloc = CappedAlloc;

        struct CapGuard;
        impl Drop for CapGuard {
            fn drop(&mut self) {
                ALLOC_CAP.with(|cap| cap.set(usize::MAX));
            }
        }
        ALLOC_CAP.with(|cap| cap.set(64 * 1024 * 1024));
        let _guard = CapGuard;

        for count in [0x4000_0000u32, u32::MAX] {
            // 1. Cur::cells (via overview[0].cells)
            let mut raw_cells = PARTIAL_MAGIC.to_vec();
            raw_cells.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_cells.extend_from_slice(&1u32.to_le_bytes()); // ov_len = 1
            raw_cells.extend_from_slice(&0i64.to_le_bytes()); // hour = 0
            raw_cells.extend_from_slice(&count.to_le_bytes()); // cells len
            assert_eq!(
                Acc::decode_partial(&raw_cells, Unit::Hour).err().as_deref(),
                Some("partial_unreadable")
            );

            // 2. fields -> val_len
            let mut raw_val = PARTIAL_MAGIC.to_vec();
            raw_val.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_val.extend_from_slice(&0u32.to_le_bytes()); // ov_len = 0
            raw_val.extend_from_slice(&0u32.to_le_bytes()); // min_len = 0
            raw_val.extend_from_slice(&1u32.to_le_bytes()); // fld_len = 1
            raw_val.extend_from_slice(&1u32.to_le_bytes()); // field str len = 1
            raw_val.push(b'f');
            raw_val.extend_from_slice(&1i64.to_le_bytes()); // total
            raw_val.push(0); // truncated
            raw_val.extend_from_slice(&1u64.to_le_bytes()); // f_sources
            raw_val.extend_from_slice(&count.to_le_bytes()); // val_len
            assert_eq!(
                Acc::decode_partial(&raw_val, Unit::Hour).err().as_deref(),
                Some("partial_unreadable")
            );

            // 3. sessions -> ses_len
            let mut raw_ses = PARTIAL_MAGIC.to_vec();
            raw_ses.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_ses.extend_from_slice(&0u32.to_le_bytes()); // ov_len = 0
            raw_ses.extend_from_slice(&0u32.to_le_bytes()); // min_len = 0
            raw_ses.extend_from_slice(&0u32.to_le_bytes()); // fld_len = 0
            raw_ses.extend_from_slice(&count.to_le_bytes()); // ses_len
            assert_eq!(
                Acc::decode_partial(&raw_ses, Unit::Hour).err().as_deref(),
                Some("partial_unreadable")
            );

            // 4. groups[0] -> v_len
            let mut raw_grp = PARTIAL_MAGIC.to_vec();
            raw_grp.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_grp.extend_from_slice(&0u32.to_le_bytes()); // ov_len = 0
            raw_grp.extend_from_slice(&0u32.to_le_bytes()); // min_len = 0
            raw_grp.extend_from_slice(&0u32.to_le_bytes()); // fld_len = 0
            raw_grp.extend_from_slice(&0u32.to_le_bytes()); // ses_len = 0
            raw_grp.extend_from_slice(&1u32.to_le_bytes()); // groups[0] f_len = 1
            raw_grp.extend_from_slice(&1u32.to_le_bytes()); // field str len = 1
            raw_grp.push(b'g');
            raw_grp.extend_from_slice(&1i64.to_le_bytes()); // total
            raw_grp.push(0); // truncated
            raw_grp.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_grp.extend_from_slice(&count.to_le_bytes()); // v_len
            assert_eq!(
                Acc::decode_partial(&raw_grp, Unit::Hour).err().as_deref(),
                Some("partial_unreadable")
            );
        }

        // Valid empty & populated partial round-trips remain intact
        let empty_acc = Acc::new(Unit::Hour);
        let empty_encoded = empty_acc.encode_partial();
        let empty_decoded = Acc::decode_partial(&empty_encoded, Unit::Hour).unwrap();
        assert_eq!(empty_decoded.encode_partial(), empty_encoded);

        let mut populated = Acc::new(Unit::Hour);
        populated.fold(rich_window(0), 1).unwrap();
        let pop_encoded = populated.encode_partial();
        let pop_decoded = Acc::decode_partial(&pop_encoded, Unit::Hour).unwrap();
        assert_eq!(pop_decoded.encode_partial().len(), pop_encoded.len());
    }
}
