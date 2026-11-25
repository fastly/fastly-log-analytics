use arrow_array::{
    builder::{Float64Builder, Int32Builder, StringBuilder, TimestampMillisecondBuilder},
    Array, BinaryArray, Int64Array, RecordBatch, StringArray,
};
use bytes::Bytes;
use fastly_log_analytics::ingest::{
    hist::Hist,
    hll::Hll,
    rum::{errors_schema, vitals_schema},
    rum_summary,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{collections::BTreeMap, sync::Arc};

const BASE_S: i64 = 1_800_000_000;
const BASE_MS: i64 = BASE_S * 1000;

fn make_vitals(rows: &[(&str, f64, &str, Option<&str>)]) -> RecordBatch {
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut name = StringBuilder::new();
    let mut value = Float64Builder::new();
    let mut rating = StringBuilder::new();
    let mut pathname = StringBuilder::new();
    let mut browser = StringBuilder::new();
    let mut os = StringBuilder::new();
    let mut device = StringBuilder::new();
    let mut cid = StringBuilder::new();
    let mut req_id = StringBuilder::new();
    let mut city = StringBuilder::new();
    let mut region = StringBuilder::new();
    let mut country = StringBuilder::new();
    let mut pop = StringBuilder::new();
    let mut tls = StringBuilder::new();
    let mut resource_url = StringBuilder::new();
    let mut asn = StringBuilder::new();
    let mut ttfb = Float64Builder::new();
    let mut sample_rate = Float64Builder::new();

    for (i, &(m_name, m_val, path, m_cid)) in rows.iter().enumerate() {
        ts.append_value(BASE_MS + (i as i64) * 1000);
        name.append_value(m_name);
        value.append_value(m_val);
        rating.append_value("good");
        pathname.append_value(path);
        browser.append_value("Chrome");
        os.append_value("macOS");
        device.append_value("desktop");
        match m_cid {
            Some(c) => cid.append_value(c),
            None => cid.append_null(),
        }
        req_id.append_null();
        city.append_null();
        region.append_null();
        country.append_value("US");
        pop.append_null();
        tls.append_null();
        resource_url.append_null();
        asn.append_null();
        ttfb.append_null();
        sample_rate.append_value(1.0);
    }

    RecordBatch::try_new(
        vitals_schema(),
        vec![
            Arc::new(ts.finish()),
            Arc::new(name.finish()),
            Arc::new(value.finish()),
            Arc::new(rating.finish()),
            Arc::new(pathname.finish()),
            Arc::new(browser.finish()),
            Arc::new(os.finish()),
            Arc::new(device.finish()),
            Arc::new(cid.finish()),
            Arc::new(req_id.finish()),
            Arc::new(city.finish()),
            Arc::new(region.finish()),
            Arc::new(country.finish()),
            Arc::new(pop.finish()),
            Arc::new(tls.finish()),
            Arc::new(resource_url.finish()),
            Arc::new(asn.finish()),
            Arc::new(ttfb.finish()),
            Arc::new(sample_rate.finish()),
        ],
    )
    .unwrap()
}

fn make_errors(rows: &[(&str, &str, i32, i32, Option<&str>)]) -> RecordBatch {
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut message = StringBuilder::new();
    let mut file = StringBuilder::new();
    let mut line = Int32Builder::new();
    let mut col = Int32Builder::new();
    let mut pathname = StringBuilder::new();
    let mut browser = StringBuilder::new();
    let mut os = StringBuilder::new();
    let mut device = StringBuilder::new();
    let mut cid = StringBuilder::new();
    let mut req_id = StringBuilder::new();
    let mut city = StringBuilder::new();
    let mut region = StringBuilder::new();
    let mut country = StringBuilder::new();
    let mut pop = StringBuilder::new();
    let mut tls = StringBuilder::new();
    let mut resource_url = StringBuilder::new();
    let mut asn = StringBuilder::new();
    let mut ttfb = Float64Builder::new();
    let mut sample_rate = Float64Builder::new();

    for (i, &(msg, f, l, c, m_cid)) in rows.iter().enumerate() {
        ts.append_value(BASE_MS + (i as i64) * 1000);
        message.append_value(msg);
        file.append_value(f);
        line.append_value(l);
        col.append_value(c);
        pathname.append_value("/home");
        browser.append_value("Chrome");
        os.append_value("macOS");
        device.append_value("desktop");
        match m_cid {
            Some(c_id) => cid.append_value(c_id),
            None => cid.append_null(),
        }
        req_id.append_null();
        city.append_null();
        region.append_null();
        country.append_value("US");
        pop.append_null();
        tls.append_null();
        resource_url.append_null();
        asn.append_null();
        ttfb.append_null();
        sample_rate.append_value(1.0);
    }

    RecordBatch::try_new(
        errors_schema(),
        vec![
            Arc::new(ts.finish()),
            Arc::new(message.finish()),
            Arc::new(file.finish()),
            Arc::new(line.finish()),
            Arc::new(col.finish()),
            Arc::new(pathname.finish()),
            Arc::new(browser.finish()),
            Arc::new(os.finish()),
            Arc::new(device.finish()),
            Arc::new(cid.finish()),
            Arc::new(req_id.finish()),
            Arc::new(city.finish()),
            Arc::new(region.finish()),
            Arc::new(country.finish()),
            Arc::new(pop.finish()),
            Arc::new(tls.finish()),
            Arc::new(resource_url.finish()),
            Arc::new(asn.finish()),
            Arc::new(ttfb.finish()),
            Arc::new(sample_rate.finish()),
        ],
    )
    .unwrap()
}

fn read_all(bytes: Vec<u8>) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn rum_summary_counts_metrics_and_ratings_like_rum_ts() {
    let key = rum_summary::key(
        &rum_summary::prefix("svc/"),
        BASE_S,
        BASE_S + 300,
        "p1s0",
        10,
    );
    assert!(key.starts_with("svc/processed/rum/summary/year="));
    assert!(key.contains("/summary=1800000000-1800000300-p1s0-n10.parquet"));

    let vitals = make_vitals(&[
        // LCP boundaries: 2500 good, 4000 not poor, 4001 poor
        ("lcp", 2500.0, "/a", Some("s1")),
        ("LCP", 4000.0, "/a", Some("s2")),
        ("LCP", 4001.0, "/a", Some("s1")),
        // CLS boundaries: 0 (p50 = 0), 0.1 good, 0.25 not poor, 0.26 poor
        ("CLS", 0.0, "/a", Some("s1")),
        ("CLS", 0.0, "/b", Some("s2")),
        ("CLS", 0.0, "/b", Some("s2")),
        ("CLS", 0.1, "/b", Some("s3")),
        ("CLS", 0.25, "/b", Some("s4")),
        ("CLS", 0.26, "/b", None),
        // Unknown metric uses ELSE thresholds (2500 / 4000)
        ("CUSTOM", 2500.0, "/c", Some("s1")),
        ("CUSTOM", 4000.0, "/c", Some("s2")),
        ("CUSTOM", 4001.0, "/c", Some("s3")),
    ]);
    let errors = make_errors(&[
        ("TypeError", "app.js", 20, 5, Some("s2")),
        ("TypeError", "app.js", 10, 3, Some("s3")),
        ("RangeError", "lib.js", 1, 1, Some("s2")),
    ]);

    let bytes = rum_summary::write_summary(&[&vitals], &[&errors], BASE_S).unwrap();
    let batches = read_all(bytes);

    let mut metrics: BTreeMap<String, (i64, i64, i64, Hist)> = BTreeMap::new();
    let mut vitals_total: Option<(i64, u64)> = None;
    let mut errors_total: Option<(i64, u64, u64)> = None;
    let mut top_error: Option<(i64, i64, i64, u64)> = None;

    for batch in &batches {
        let kinds = batch
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let goods = batch
            .column_by_name("good")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let poors = batch
            .column_by_name("poor")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_hists = batch
            .column_by_name("value_hist")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let cid_hlls = batch
            .column_by_name("cid_hll")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let msg_hlls = batch
            .column_by_name("msg_hll")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let min_lines = batch
            .column_by_name("min_line")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let min_cols = batch
            .column_by_name("min_col")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            match kinds.value(i) {
                "metric" => {
                    let h = Hist::from_bytes(value_hists.value(i)).unwrap();
                    metrics.insert(
                        keys.value(i).to_owned(),
                        (counts.value(i), goods.value(i), poors.value(i), h),
                    );
                }
                "vitals_total" => {
                    let hll = Hll::from_bytes(cid_hlls.value(i)).unwrap();
                    vitals_total = Some((counts.value(i), hll.count()));
                }
                "errors_total" => {
                    let c_hll = Hll::from_bytes(cid_hlls.value(i)).unwrap();
                    let m_hll = Hll::from_bytes(msg_hlls.value(i)).unwrap();
                    errors_total = Some((counts.value(i), c_hll.count(), m_hll.count()));
                }
                "error" if keys.value(i) == r#"["TypeError","app.js"]"# => {
                    let c_hll = Hll::from_bytes(cid_hlls.value(i)).unwrap();
                    top_error = Some((
                        counts.value(i),
                        min_lines.value(i),
                        min_cols.value(i),
                        c_hll.count(),
                    ));
                }
                _ => {}
            }
        }
    }

    let (lcp_cnt, lcp_good, lcp_poor, _) = &metrics["LCP"];
    assert_eq!((*lcp_cnt, *lcp_good, *lcp_poor), (3, 1, 1));

    let (cls_cnt, cls_good, cls_poor, cls_hist) = &metrics["CLS"];
    assert_eq!((*cls_cnt, *cls_good, *cls_poor), (6, 4, 1));
    assert_eq!(cls_hist.quantile(0.5), Some(0.0));

    let (custom_cnt, custom_good, custom_poor, _) = &metrics["CUSTOM"];
    assert_eq!((*custom_cnt, *custom_good, *custom_poor), (3, 1, 1));

    assert_eq!(vitals_total, Some((12, 4)));
    assert_eq!(errors_total, Some((3, 2, 2)));
    assert_eq!(top_error, Some((2, 10, 3, 2)));
}

#[test]
fn resource_timings_do_not_count_as_page_vitals() {
    let page = [("LCP", 1200.0, "/home", Some("c1"))];
    let with_resource = [
        ("LCP", 1200.0, "/home", Some("c1")),
        ("ttlb", 850.0, "/home", Some("c1")),
    ];
    let errors = make_errors(&[]);
    let plain = rum_summary::write_summary(&[&make_vitals(&page)], &[&errors], BASE_S).unwrap();
    let mixed =
        rum_summary::write_summary(&[&make_vitals(&with_resource)], &[&errors], BASE_S).unwrap();
    assert_eq!(plain, mixed);
}

#[test]
fn rum_summary_merge_windows_preserves_fcp_ttfb_buckets_and_sampled_flag() {
    let w1_vitals = make_vitals(&[
        ("LCP", 1800.0, "/home", Some("c1")),
        ("FCP", 1200.0, "/home", Some("c1")),
        ("TTFB", 400.0, "/home", Some("c1")),
    ]);
    let w1_errors = make_errors(&[("Boom", "app.js", 12, 4, Some("c1"))]);
    let w2_vitals = make_vitals(&[
        ("LCP", 3200.0, "/about", Some("c2")),
        ("FCP", 2100.0, "/about", Some("c2")),
        ("TTFB", 900.0, "/about", Some("c2")),
    ]);
    let w2_errors = make_errors(&[("Boom", "app.js", 8, 2, Some("c2"))]);

    let b1 = rum_summary::write_summary(&[&w1_vitals], &[&w1_errors], BASE_S).unwrap();
    let b2 = rum_summary::write_summary(&[&w2_vitals], &[&w2_errors], BASE_S + 60).unwrap();
    let merged = rum_summary::merge_windows(&[&b1, &b2]).unwrap();
    let combined =
        rum_summary::write_summary(&[&w1_vitals, &w2_vitals], &[&w1_errors, &w2_errors], BASE_S)
            .unwrap();
    assert_eq!(merged, combined);

    // A sampled batch emits a `sampled` row that survives `merge_windows`.
    let mut ts = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut name = StringBuilder::new();
    let mut value = Float64Builder::new();
    let mut rating = StringBuilder::new();
    let mut pathname = StringBuilder::new();
    let mut browser = StringBuilder::new();
    let mut os = StringBuilder::new();
    let mut device = StringBuilder::new();
    let mut cid = StringBuilder::new();
    let mut req_id = StringBuilder::new();
    let mut city = StringBuilder::new();
    let mut region = StringBuilder::new();
    let mut country = StringBuilder::new();
    let mut pop = StringBuilder::new();
    let mut tls = StringBuilder::new();
    let mut resource_url = StringBuilder::new();
    let mut asn = StringBuilder::new();
    let mut ttfb = Float64Builder::new();
    let mut sample_rate = Float64Builder::new();
    ts.append_value(BASE_MS);
    name.append_value("FCP");
    value.append_value(1500.0);
    rating.append_value("good");
    pathname.append_value("/sampled");
    browser.append_value("Firefox");
    os.append_value("Linux");
    device.append_value("desktop");
    cid.append_value("c3");
    req_id.append_null();
    city.append_null();
    region.append_null();
    country.append_value("US");
    pop.append_null();
    tls.append_null();
    resource_url.append_null();
    asn.append_null();
    ttfb.append_null();
    sample_rate.append_value(0.5);
    let sampled_batch = RecordBatch::try_new(
        vitals_schema(),
        vec![
            Arc::new(ts.finish()),
            Arc::new(name.finish()),
            Arc::new(value.finish()),
            Arc::new(rating.finish()),
            Arc::new(pathname.finish()),
            Arc::new(browser.finish()),
            Arc::new(os.finish()),
            Arc::new(device.finish()),
            Arc::new(cid.finish()),
            Arc::new(req_id.finish()),
            Arc::new(city.finish()),
            Arc::new(region.finish()),
            Arc::new(country.finish()),
            Arc::new(pop.finish()),
            Arc::new(tls.finish()),
            Arc::new(resource_url.finish()),
            Arc::new(asn.finish()),
            Arc::new(ttfb.finish()),
            Arc::new(sample_rate.finish()),
        ],
    )
    .unwrap();
    let empty_err = make_errors(&[]);
    let sampled_bytes =
        rum_summary::write_summary(&[&sampled_batch], &[&empty_err], BASE_S).unwrap();
    let merged_sampled = rum_summary::merge_windows(&[&b1, &sampled_bytes]).unwrap();
    let has_sampled = read_all(merged_sampled).iter().any(|batch| {
        let kinds = batch
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows()).any(|i| kinds.value(i) == "sampled")
    });
    assert!(has_sampled, "merged bundle must preserve sampled marker");
}
