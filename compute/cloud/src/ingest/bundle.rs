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
//!   hour's window folder (hour builds) or the day's `summary_bundle` folder (day builds).
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
//! field (sketches for the top `summary::IP_TOP_K`), `SESSION_K` sessions and `DIM_K` values per
//! dim, so a build resumed from a partial gives the same bytes as one that never stopped. The
//! bundle keeps the top [`HOUR_TOP_K`]/[`DAY_TOP_K`] values per field by `(count desc, value
//! asc)`; `field_totals.truncated` marks any field that lost values.

use super::{
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
/// Sources folded per execution, which bounds heap and time per build step.
pub const MAX_SOURCES: usize = 24;
/// Minimum unfolded window summaries in an open hour before pre-folding a `partial=` batch.
const OPEN_BATCH_SOURCES: usize = 18;
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
/// Stop folding once this much vCPU is spent in the execution.
pub const VCPU_STOP_MS: u64 = 700;
/// Leave 320ms of this installation's 1000ms CPU allowance for final serialization and the tail.
pub const WRITE_VCPU_STOP_MS: u64 = 680;
const PARTIAL_MAGIC: &[u8; 5] = b"FLPB\x01";

/// `{scope}processed/summary_bundle/{table}/`, holding hour and day bundles.
pub fn bundle_prefix(scope: &str, table: &str) -> String {
    format!("{scope}processed/summary_bundle/{table}/")
}

/// FNV-1a 64 over each file name followed by `\n`, in the order given (callers sort).
pub fn fingerprint<'a>(names: impl IntoIterator<Item = &'a str>) -> u64 {
    compute_common::crypto::fnv1a64(
        names
            .into_iter()
            .flat_map(|name| name.bytes().chain([b'\n'])),
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
    Day,
    Partial,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleName {
    pub kind: NameKind,
    /// The hour (`HH`) of an hour bundle, the day of month of a day bundle, 0 for a partial.
    pub unit: u32,
    pub count: usize,
    pub rows: u64,
    pub fp: u64,
}

/// Parses an hour bundle, day bundle or partial key.
pub fn parse_name(key: &str) -> Option<BundleName> {
    let name = file_name(key).strip_suffix(".parquet")?;
    let (kind, unit, rest) = if let Some(rest) = name.strip_prefix("hour_bundle=") {
        let (hh, rest) = rest.split_once('-')?;
        (NameKind::Hour, hh.parse().ok()?, rest)
    } else if let Some(rest) = name.strip_prefix("day_bundle=") {
        let day = key.rsplit('/').nth(1)?.strip_prefix("day=")?.parse().ok()?;
        (NameKind::Day, day, rest)
    } else {
        (NameKind::Partial, 0, name.strip_prefix("partial=")?)
    };
    let mut parts = rest.split('-');
    let count = parts.next()?.strip_prefix('c')?.parse().ok()?;
    let rows = parts.next()?.strip_prefix('n')?.parse().ok()?;
    let fp = u64::from_str_radix(parts.next()?.strip_prefix('h')?, 16).ok()?;
    if parts.next().is_some() || (kind == NameKind::Hour && unit > 23) {
        return None;
    }
    Some(BundleName {
        kind,
        unit,
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
}

#[derive(Clone, Copy)]
enum Op {
    Sum,
    Min,
    Max,
}

use Op::{Max, Min, Sum};

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
];

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
            Ty::Bool | Ty::Bin => Cell::Null,
        }
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
                Ty::Bool | Ty::Bin => {}
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

/// The merge state of a bundle under construction.
#[derive(Default)]
struct Acc {
    /// Per hour bucket (ms): sums, IP sketch, `sample_capped` of any source.
    overview: BTreeMap<i64, (Vec<Cell>, Hll, bool)>,
    minutes: BTreeMap<i64, Vec<Cell>>,
    fields: BTreeMap<String, FieldAcc>,
    sessions: HashMap<SessionKey, Vec<Cell>>,
    dims: BTreeMap<String, HashMap<String, Vec<Cell>>>,
    /// Window summaries folded in, counting a resumed partial as the sources it covers.
    sources: u64,
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
    /// Folds one source that stands for `weight` window summaries (a resumed partial covers
    /// several; a day bundle's hour bundle counts as one).
    fn fold(&mut self, bytes: Vec<u8>, weight: u64) -> Result<(), String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .and_then(|builder| builder.build())
            .map_err(|_| "summary_unreadable".to_string())?;
        let mut seen = BTreeSet::new();
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
                self.fold_row(&cols, i);
            }
        }
        for field in seen {
            let acc = self.fields.entry(field).or_default();
            acc.sources = acc.sources.saturating_add(weight);
        }
        self.sources = self.sources.saturating_add(weight);
        Ok(())
    }

    /// A custom field that some source never summarized (it predates the field or the
    /// summarizer) would merge to a partial count, so it is left out and readers fall back to
    /// raw. Catalog fields are in every version-2 summary.
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
            Some("dims") => {
                if !self.dims.contains_key(f_str) {
                    self.dims.insert(f_str.to_owned(), HashMap::new());
                }
                let dim = self.dims.get_mut(f_str).expect("inserted");
                match dim.get_mut(v_str) {
                    Some(cells) => cols.merge_into(cells, DIM_COLS, i),
                    None => {
                        dim.insert(v_str.to_owned(), cols.cells(DIM_COLS, i));
                    }
                }
            }
            _ => {}
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
        for dim in self.dims.values_mut() {
            if dim.len() > summary::DIM_K {
                let mut values: Vec<(String, Vec<Cell>)> = dim.drain().collect();
                keep_top(&mut values, summary::DIM_K, |a, b| {
                    by_count(a, b, |c| int(&c[0]))
                });
                dim.extend(values);
            }
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
                }
            }
        }
        fn put_hll(out: &mut Vec<u8>, hll: &Hll) {
            let regs = hll.registers();
            let nz = regs.iter().filter(|&&r| r != 0).count();
            out.extend_from_slice(&(nz as u16).to_le_bytes());
            for (idx, &r) in regs.iter().enumerate() {
                if r != 0 {
                    out.push(idx as u8);
                    out.push(r);
                }
            }
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

        put_u32(&mut out, self.dims.len() as u32);
        for (field, dim) in &self.dims {
            put_str(&mut out, field);
            put_u32(&mut out, dim.len() as u32);
            for (val, cells) in dim {
                put_str(&mut out, val);
                put_cells(&mut out, cells);
            }
        }
        out
    }

    fn decode_partial(bytes: &[u8]) -> Result<Self, String> {
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
            fn u16(&mut self) -> Result<u16, String> {
                Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
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
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    let cell = match self.u8()? {
                        0 => Cell::Null,
                        1 => Cell::I(self.i64()?),
                        2 => Cell::F(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
                        3 => Cell::S(self.str()?),
                        _ => return Err("partial_unreadable".to_string()),
                    };
                    out.push(cell);
                }
                Ok(out)
            }
            fn hll(&mut self) -> Result<Hll, String> {
                let nz = self.u16()? as usize;
                if nz > summary::HLL_M {
                    return Err("partial_unreadable".to_string());
                }
                let mut regs = [0u8; summary::HLL_M];
                for _ in 0..nz {
                    let idx = self.u8()? as usize;
                    let r = self.u8()?;
                    regs[idx] = r;
                }
                Ok(Hll::from_registers(regs))
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

        let dim_len = cur.u32()? as usize;
        let mut dims = BTreeMap::new();
        for _ in 0..dim_len {
            let field = cur.str()?;
            let v_len = cur.u32()? as usize;
            let mut dim = HashMap::with_capacity(v_len);
            for _ in 0..v_len {
                let val = cur.str()?;
                let cells = cur.cells()?;
                dim.insert(val, cells);
            }
            dims.insert(field, dim);
        }

        Ok(Self {
            overview,
            minutes,
            fields,
            sessions,
            dims,
            sources,
        })
    }

    /// A final bundle (`top_k`) or a partial (`None`: the whole state, flags as folded).
    fn write(&self, start: i64, top_k: Option<usize>, label: &str) -> Result<Vec<u8>, String> {
        let bucket = start * 1000;
        let fin = top_k.is_some();
        let mut out = Out::new();
        let row = |out: &mut Out, head: &[(C, V<'_>)], cols: &[(C, Op)], cells: &[Cell]| {
            let mut values: Vec<(C, V<'_>)> = head.to_vec();
            values.extend(cols.iter().zip(cells).map(|((c, _), cell)| {
                (
                    *c,
                    match cell {
                        Cell::Null => V::Null,
                        Cell::I(i) => V::I(*i),
                        Cell::F(f) => V::F(*f),
                        Cell::S(s) => V::S(s),
                    },
                )
            }));
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
        for dim in ordered(self.dims.keys().collect(), DIMS) {
            let mut ranked: Vec<(&String, &Vec<Cell>)> = self.dims[&dim].iter().collect();
            ranked.sort_unstable_by(|a, b| {
                int(&b.1[0]).cmp(&int(&a.1[0])).then_with(|| a.0.cmp(b.0))
            });
            ranked.truncate(summary::DIM_K);
            for (value, cells) in ranked {
                row(
                    &mut out,
                    &[
                        (C::Bundle, V::S("dims")),
                        (C::Bucket, V::I(bucket)),
                        (C::FieldName, V::S(&dim)),
                        (C::Value, V::S(value)),
                    ],
                    DIM_COLS,
                    cells,
                );
            }
        }
        let batch = out.finish()?;
        super::convert::write_with_metadata(
            summary::schema(),
            &[&batch],
            vec![
                KeyValue::new("flp_summary".into(), summary::SUMMARY_VERSION.to_string()),
                KeyValue::new(
                    "flp_bundle".into(),
                    if fin {
                        label.to_owned()
                    } else {
                        format!("{label}_partial")
                    },
                ),
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
    let mut acc = Acc::default();
    for source in sources {
        acc.fold(source.to_vec(), 1)?;
        acc.prune();
    }
    acc.write(start, Some(unit.top_k()), unit.label())
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
    fn catalog_windows(&self, state: &mut State, hour: i64) -> Result<Option<Vec<String>>, String> {
        if state
            .inventory
            .as_ref()
            .is_none_or(|inventory| inventory.hour != hour)
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
    /// writes the bundle or a partial to resume from.
    fn build(&mut self, job: Job<'_>) -> Result<Step, String> {
        let mut sources = job.sources;
        sources.sort_unstable_by(|a, b| file_name(a).cmp(file_name(b)));
        let names: Vec<&str> = sources.iter().map(|k| file_name(k)).collect();
        let best = job
            .partials
            .iter()
            .filter_map(|key| {
                let name = parse_name(key)?;
                (name.count > 0
                    && name.count <= names.len()
                    && fingerprint(names[..name.count].iter().copied()) == name.fp)
                    .then_some((name.count, key))
            })
            .max();
        let covered = best.map_or(0, |(count, _)| count);
        if job.final_key.is_none() {
            if sources.len().saturating_sub(covered) < OPEN_BATCH_SOURCES {
                return Ok(Step::Done);
            }
            if !self.room(OPEN_BATCH_SOURCES as u32 + 3) {
                return Ok(Step::Pending);
            }
        }
        let mut acc = Acc::default();
        let mut done = 0;
        if let Some((count, key)) = best {
            if !self.room(3) {
                return Ok(Step::Pending);
            }
            if let Some(bytes) = self.store.get(self.cdn_key, key, CDN_READ_LIMIT)? {
                if bytes.starts_with(PARTIAL_MAGIC) {
                    acc = Acc::decode_partial(&bytes)?;
                    done = count;
                    self.report.sources += 1;
                }
            }
        }
        let resumed = done;
        let tail = if job.partials.is_empty() { 3 } else { 4 };
        while done < sources.len() {
            if done - resumed >= MAX_SOURCES || self.stop() {
                break;
            }
            if !self.room(tail) {
                break;
            }
            let Some(bytes) = self
                .store
                .get(self.cdn_key, &sources[done], CDN_READ_LIMIT)?
            else {
                // Removed since the LIST (an abandoned plan): relist next execution.
                self.report
                    .errors
                    .push(format!("summary_vanished:{}", file_name(&sources[done])));
                break;
            };
            acc.fold(bytes, 1)?;
            acc.prune();
            done += 1;
            self.report.sources += 1;
        }
        let rows: u64 = sources[..done].iter().map(|k| name_rows(k)).sum();
        let defer_final = job.final_key.is_some() && done == sources.len() && self.write_stop();
        if defer_final && self.report.stopped.is_none() {
            self.report.stopped = Some("vcpu");
        }
        let (key, body, is_final) = match job
            .final_key
            .filter(|_| done == sources.len() && !defer_final)
        {
            Some(final_key) => {
                let body = acc.write(job.start, Some(job.unit.top_k()), job.unit.label())?;
                (final_key.to_owned(), body, true)
            }
            None if done > resumed => {
                let body = acc.encode_partial();
                let key = format!("{}partial={}", job.folder, covering(&names[..done], rows));
                (key, body, false)
            }
            None => return Ok(Step::Pending),
        };
        self.report.bytes_written += body.len() as u64;
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
        let stale: Vec<String> = job.partials.into_iter().filter(|p| *p != key).collect();
        if !stale.is_empty() && self.room(1) {
            match self.store.delete_objects(&stale) {
                Ok(result) => self.report.deleted += result.deleted.len() as u64,
                Err(error) => self.report.errors.push(error),
            }
        }
        if is_final {
            self.report.rows += rows;
            Ok(Step::Done)
        } else {
            self.report.partials += 1;
            if job.final_key.is_none() && sources.len().saturating_sub(done) < OPEN_BATCH_SOURCES {
                Ok(Step::Done)
            } else {
                Ok(Step::Pending)
            }
        }
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
            let Some(committed) = self.catalog_windows(state, hour)? else {
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
        let step = self.build(Job {
            sources: windows,
            partials,
            final_key: Some(&key),
            folder,
            start: hour,
            unit: Unit::Hour,
            coverage,
        })?;
        if let Step::Done = step {
            self.retire(state, had, Some(&key));
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
            let Some(committed) = self.catalog_windows(state, hour)? else {
                return Ok(Step::Pending);
            };
            windows = committed;
        }
        let step = self.build(Job {
            sources: windows,
            partials,
            final_key: None,
            folder,
            start: hour,
            unit: Unit::Hour,
            coverage: None,
        })?;
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
                    if !self.room(3) || !super::catalog::admitted(self.store) {
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
        let step = self.build(Job {
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
        })?;
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
    let watermark = runtime.admin_get(WATERMARK_KEY)?;
    ctx.catalog = super::catalog::head(
        watermark
            .as_ref()
            .map(|r| &r.value)
            .unwrap_or(&serde_json::Value::Null),
    )?;
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
        let _ = runtime.admin_cas(
            STATE_KEY,
            record.as_ref().map(|r| r.generation.as_str()),
            &serde_json::to_value(&state).map_err(|_| "bundle_state_encode")?,
        )?;
        ctx.report.more_pending = true;
        ctx.report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
        return Ok(ctx.report);
    }
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
        if let Some(closed) = closed {
            while next + HOUR_S <= closed {
                if ctx.report.stopped.is_some() || (ctx.report.bytes_written > 0 && ctx.stop()) {
                    ctx.report.more_pending = true;
                    return Ok(());
                }
                if let Step::Pending = ctx.hour(state, next)? {
                    ctx.report.more_pending = true;
                    return Ok(());
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
                if let Step::Pending = ctx.hour(state, hour)? {
                    state.revisit = Some(hour);
                    ctx.report.more_pending = true;
                    return Ok(());
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
            match ctx.day(state, day)? {
                Step::Done => {
                    state.dirty.remove(&day);
                }
                Step::Pending => {
                    ctx.report.more_pending = true;
                    return Ok(());
                }
            }
        }
        if closed.is_some() {
            let mut open = next;
            while open <= now {
                if ctx.report.stopped.is_some() || ctx.report.bytes_written > 0 {
                    break;
                }
                if let Step::Pending = ctx.open_hour(state, open)? {
                    ctx.report.more_pending = true;
                    return Ok(());
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
                .write(1_800_000_000, Some(Unit::Hour.top_k()), "hour")
                .unwrap();
            let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
            let one_shot = merge(&refs, 1_800_000_000, Unit::Hour).unwrap();
            assert_eq!(tenant_total(fin.clone()), want, "{sources:?}");
            assert_eq!(fin, one_shot, "{sources:?}");
        }
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
}
