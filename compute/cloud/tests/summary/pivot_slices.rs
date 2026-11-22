//! Pivot slices: the dashboard rollups of the rows with one pivot value, which must equal the
//! dashboard rollups of a summary written over only those rows.
use super::support::*;
use fastly_log_analytics::ingest::{
    bundle::{self, Unit},
    convert,
    summary::{self, Hll},
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// The dashboard view of a summary: per minute series, non-empty field totals, field values and
/// the distinct-IP estimate.
#[derive(Debug, PartialEq)]
struct View {
    minutes: BTreeMap<i64, Vec<Value>>,
    totals: BTreeMap<String, (i64, bool)>,
    values: BTreeMap<(String, String), i64>,
    ips: u64,
}

const TS: [&str; 8] = [
    "requests",
    "status_4xx",
    "status_5xx",
    "hits",
    "cache_total",
    "resp_bytes_sum",
    "ttfb_sum",
    "ttfb_count",
];

fn hll(row: &Map<String, Value>) -> Option<Hll> {
    row.get("ip_sketch")
        .and_then(Value::as_str)
        .and_then(|h| Hll::from_bytes(&hex::decode(h).unwrap()))
}

fn ts_cells(row: &Map<String, Value>) -> Vec<Value> {
    TS.iter()
        .map(|c| match row.get(*c) {
            Some(Value::Number(n)) if n.is_f64() => json!(format!("{:.9}", n.as_f64().unwrap())),
            Some(v) => v.clone(),
            None => Value::Null,
        })
        .collect()
}

fn base_view(rows: &[Map<String, Value>]) -> View {
    let mut view = View {
        minutes: BTreeMap::new(),
        totals: BTreeMap::new(),
        values: BTreeMap::new(),
        ips: 0,
    };
    let mut ips = Hll::default();
    for r in rows {
        match r["bundle"].as_str().unwrap() {
            "time_series" => {
                view.minutes.insert(mi(r, "bucket").unwrap(), ts_cells(r));
            }
            "field_totals" if mi(r, "count") != Some(0) => {
                view.totals.insert(
                    r["field"].as_str().unwrap().into(),
                    (mi(r, "count").unwrap(), r["truncated"] == true),
                );
            }
            "all_fields" => {
                view.values.insert(
                    (
                        r["field"].as_str().unwrap().into(),
                        r["value"].as_str().unwrap().into(),
                    ),
                    mi(r, "count").unwrap(),
                );
            }
            "overview" => ips.merge(&hll(r).unwrap()),
            _ => {}
        }
    }
    view.ips = ips.count();
    view
}

fn slice_view(rows: &[Map<String, Value>], pivot: &str, value: &str) -> View {
    let mut view = View {
        minutes: BTreeMap::new(),
        totals: BTreeMap::new(),
        values: BTreeMap::new(),
        ips: 0,
    };
    let mut ips = Hll::default();
    for r in rows.iter().filter(|r| {
        r.get("pivot") == Some(&json!(pivot)) && r.get("pivot_value") == Some(&json!(value))
    }) {
        match r["bundle"].as_str().unwrap() {
            "slice_ts" => {
                view.minutes.insert(mi(r, "bucket").unwrap(), ts_cells(r));
            }
            "slice_totals" => {
                view.totals.insert(
                    r["field"].as_str().unwrap().into(),
                    (mi(r, "count").unwrap(), r["truncated"] == true),
                );
            }
            "slice_fields" => {
                view.values.insert(
                    (
                        r["field"].as_str().unwrap().into(),
                        r["value"].as_str().unwrap().into(),
                    ),
                    mi(r, "count").unwrap(),
                );
            }
            "slice_overview" => ips.merge(&hll(r).unwrap()),
            _ => {}
        }
    }
    view.ips = ips.count();
    view
}

fn pivot_total<'a>(rows: &'a [Map<String, Value>], pivot: &str) -> &'a Map<String, Value> {
    rows.iter()
        .find(|r| r["bundle"] == "pivot_totals" && r.get("pivot") == Some(&json!(pivot)))
        .expect("pivot_totals row")
}

const CUSTOM: &[(&str, convert::Kind)] = &[("tenant", convert::Kind::Utf8)];

fn rows_with_tenant() -> Vec<Value> {
    fixture()
        .into_iter()
        .enumerate()
        .map(|(i, mut r)| {
            if i % 5 != 0 {
                r["tenant"] = ["acme", "globex"][i % 2].into();
            }
            r
        })
        .collect()
}

fn status_of(row: &Value) -> Option<i64> {
    row.get("status").and_then(Value::as_i64)
}

#[test]
fn window_status_slices_equal_the_summary_of_their_rows() {
    let rows = rows_with_tenant();
    let all = read(&summary::write_summary(&[&custom_batch(&rows, CUSTOM)], BASE).unwrap());

    let total = pivot_total(&all, "status");
    let non_null = rows.iter().filter(|r| status_of(r).is_some()).count() as i64;
    assert!(
        non_null < rows.len() as i64,
        "the fixture has null statuses"
    );
    assert_eq!(total["count"], non_null);
    assert_eq!(total["truncated"], false);

    let mut values: Vec<i64> = rows.iter().filter_map(status_of).collect();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values, [0, 200, 404, 503]);
    for status in values {
        let subset: Vec<Value> = rows
            .iter()
            .filter(|r| status_of(r) == Some(status))
            .cloned()
            .collect();
        let only = read(&summary::write_summary(&[&custom_batch(&subset, CUSTOM)], BASE).unwrap());
        let want = base_view(&only);
        assert!(want.totals.contains_key("tenant"));
        assert!(want.ips > 0);
        assert_eq!(
            slice_view(&all, "status", &status.to_string()),
            want,
            "status {status}"
        );
    }
    assert_eq!(slice_view(&all, "status", "301"), base_view(&[]));
}

#[test]
fn pivot_values_past_the_cap_are_cut_and_counted() {
    let n = summary::PIVOT_K + 6;
    let rows: Vec<Value> = (0..n)
        .flat_map(|v| {
            // Value v has v + 1 rows, so the cut keeps the most frequent.
            (0..=v).map(move |k| {
                json!({"timestamp": iso(BASE * 1000 + (v * 100 + k) as i64), "status": 100 + v})
            })
        })
        .collect();
    let all = read(&summary::write_summary(&[&batch(&rows)], BASE).unwrap());
    let total = pivot_total(&all, "status");
    assert_eq!(total["count"], rows.len() as i64);
    assert_eq!(total["truncated"], true);
    let mut kept: Vec<String> = all
        .iter()
        .filter(|r| r["bundle"] == "slice_overview")
        .map(|r| r["pivot_value"].as_str().unwrap().to_owned())
        .collect();
    kept.sort_unstable();
    let mut want: Vec<String> = (n - summary::PIVOT_K..n)
        .map(|v| (100 + v).to_string())
        .collect();
    want.sort_unstable();
    assert_eq!(kept, want);
}

/// Three window summaries over `rows` (round robin) restricted to `keep`; empty windows are left
/// out, as the summarizer writes none for them.
fn windows_of(rows: &[Value], keep: impl Fn(&Value) -> bool) -> Vec<Vec<u8>> {
    (0..3)
        .filter_map(|w| {
            let subset: Vec<Value> = rows
                .iter()
                .enumerate()
                .filter(|(i, r)| i % 3 == w && keep(r))
                .map(|(_, r)| r.clone())
                .collect();
            (!subset.is_empty())
                .then(|| summary::write_summary(&[&custom_batch(&subset, CUSTOM)], BASE).unwrap())
        })
        .collect()
}

#[test]
fn bundle_status_slices_equal_the_bundle_of_their_rows() {
    let rows = rows_with_tenant();
    let all = windows_of(&rows, |_| true);
    let refs: Vec<&[u8]> = all.iter().map(Vec::as_slice).collect();
    for unit in [Unit::Hour, Unit::Day] {
        let merged = read(&bundle::merge(&refs, BASE, unit).unwrap());
        let total = pivot_total(&merged, "status");
        assert_eq!(
            total["count"],
            rows.iter().filter(|r| status_of(r).is_some()).count() as i64
        );
        for status in [0, 200, 404, 503] {
            let only = windows_of(&rows, |r| status_of(r) == Some(status));
            let only_refs: Vec<&[u8]> = only.iter().map(Vec::as_slice).collect();
            let want = base_view(&read(&bundle::merge(&only_refs, BASE, unit).unwrap()));
            assert!(want.totals.contains_key("tenant"));
            assert_eq!(
                slice_view(&merged, "status", &status.to_string()),
                want,
                "{unit:?} status {status}"
            );
        }
    }
}

#[test]
fn bundles_drop_slices_unless_every_source_wrote_them() {
    let rows = rows_with_tenant();
    let windows = windows_of(&rows, |_| true);
    let mut refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
    let old = without_slices(&windows[1]);
    refs[1] = &old;
    let merged = read(&bundle::merge(&refs, BASE, Unit::Hour).unwrap());
    assert!(merged
        .iter()
        .all(|r| !r["bundle"].as_str().unwrap().starts_with("slice_")
            && r["bundle"] != "pivot_totals"));
    assert!(merged.iter().any(|r| r["bundle"] == "all_fields"));
}

/// `bytes` without its pivot rows, as a summary written before slices existed.
fn without_slices(bytes: &[u8]) -> Vec<u8> {
    use arrow_array::{Array, StringArray};
    use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes)).unwrap();
    let schema = builder.schema().clone();
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_key_value_metadata(kv)
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    for b in builder.build().unwrap() {
        let b = b.unwrap();
        let kind = b
            .column_by_name("bundle")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keep = |i: usize| {
            kind.is_null(i)
                || !(kind.value(i).starts_with("slice_") || kind.value(i) == "pivot_totals")
        };
        let mut i = 0;
        while i < b.num_rows() {
            let start = i;
            while i < b.num_rows() && keep(i) == keep(start) {
                i += 1;
            }
            if keep(start) {
                writer.write(&b.slice(start, i - start)).unwrap();
            }
        }
    }
    writer.close().unwrap();
    out
}
