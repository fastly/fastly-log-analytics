//! Summary cost on realistic shapes: window write time and size, and the hour and day bundle
//! merges the `summary_bundle` stage runs under `VCPU_STOP_MS`. Native timings; Compute's Wasm
//! runs slower, so the bounds leave headroom.
//!
//! `cargo test --release --test summary_bench -- --ignored --nocapture` prints the numbers.

use arrow_array::RecordBatch;
use fastly_log_analytics::ingest::{
    bundle::{self, Unit},
    convert, summary,
};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

mod common;

const BASE: i64 = 1_800_000_000;
/// Window summaries per hour bundle execution (`bundle::MAX_SOURCES`).
const SOURCES: usize = 24;

fn iso(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// Zipf-like: small values far more often than large ones, in `0..n`.
    fn skewed(&mut self, n: u64) -> u64 {
        let u = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((u * u * u) * n as f64) as u64
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.skewed(items.len() as u64) as usize]
    }
}

/// `n` rows over `[start, start + 120 s)` shaped like production traffic: skewed IPs, ASNs,
/// cities and URLs, nulls where the edge leaves fields unset.
fn rows(seed: u64, start: i64, n: usize) -> Vec<Value> {
    let mut r = Rng(seed);
    let countries = ["US", "DE", "GB", "JP", "BR", "IN", "FR", "CA", "AU", "NL"];
    let protos = ["HTTP/2", "HTTP/1.1", "HTTP/3"];
    let caches = ["HIT", "MISS", "PASS", "HIT-STALE", "ERROR"];
    let pops = [
        "IAD", "FRA", "LHR", "NRT", "GRU", "BOM", "CDG", "YYZ", "SYD", "AMS",
    ];
    let ext = [".js", ".css", ".png", ".woff2", ".html", ".json", ""];
    (0..n)
        .map(|i| {
            let country = r.pick(&countries);
            let asn = 1000 + r.skewed(3000);
            let mut row = json!({
                "timestamp": iso(start * 1000 + (r.next() % 120_000) as i64),
                "ip": format!("10.{}.{}.{}", r.skewed(40), r.skewed(250), r.next() % 250),
                "status": ([200, 200, 200, 304, 404, 500, 503][r.skewed(7) as usize]),
                "cache": r.pick(&caches),
                "resp_bytes": 200 + r.skewed(2_000_000),
                "ttfb": (50 + r.skewed(2_000_000)) as f64 / 1e6,
                "elapsed": 100 + r.skewed(5_000_000),
                "url": format!("/p/{}{}", r.skewed(20_000), r.pick(&ext)),
                "country": country,
                "city": format!("{country}-{}", r.skewed(150)),
                "region": format!("{country}-r{}", r.skewed(20)),
                "lat": (r.next() % 180) as f64 - 90.0,
                "lon": (r.next() % 360) as f64 - 180.0,
                "asn": asn,
                "proto": r.pick(&protos),
                "pop": r.pick(&pops),
                "tcp_rtt": 500 + r.skewed(300_000),
                "rtt_var": 100 + r.skewed(50_000),
                "ploss": (r.skewed(100) as f64) / 1000.0,
                "req_header_bytes": 150 + r.skewed(40_000),
                "rid": format!("{seed:x}-{i:x}"),
                "ua": format!("ua-{}", r.skewed(500)),
                "ja4": format!("t13d{}", r.skewed(300)),
            });
            if r.next().is_multiple_of(3) {
                row["ottfb"] = (1000 + r.skewed(1_000_000)).into();
                row["ottlb"] = (2000 + r.skewed(2_000_000)).into();
                row["backend"] = format!("origin-{}", r.skewed(4)).into();
            }
            if r.next().is_multiple_of(20) {
                row["waf_sig"] = ["VERIFIED-BOT.googlebot", "VERIFIED-BOT.bingbot", "SQLI"]
                    [r.skewed(3) as usize]
                    .into();
            }
            row
        })
        .collect()
}

fn batch(rows: &[Value], start: i64) -> RecordBatch {
    let zst = common::zstd_ndjson(rows);
    let converted = convert::convert_object(&zst, 300, start).unwrap();
    converted.windows.into_values().next().unwrap()
}

/// The fastest of five runs: this machine is shared, so the minimum is the stable figure.
fn time<T>(mut f: impl FnMut() -> T) -> (T, Duration) {
    let mut best = Duration::MAX;
    let mut out = None;
    for _ in 0..5 {
        let t = Instant::now();
        out = Some(f());
        best = best.min(t.elapsed());
    }
    (out.expect("ran"), best)
}

#[test]
#[ignore = "measurement; run with --release --ignored --nocapture"]
fn summary_window_and_bundle_cost() {
    // Phase −1's busiest hour: ~170k requests over ~210 two-minute shard windows, ~800 rows
    // each. `FLA_BENCH_ROWS` overrides it to stress one execution with denser windows.
    let per_window: usize = std::env::var("FLA_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(800);
    let batches: Vec<RecordBatch> = (0..SOURCES)
        .map(|w| {
            let start = BASE + w as i64 * 150;
            batch(&rows(w as u64 + 1, start, per_window), start)
        })
        .collect();
    let mut windows = Vec::new();
    let mut write = Duration::ZERO;
    for (w, b) in batches.iter().enumerate() {
        let (bytes, t) = time(|| summary::write_summary(&[b], BASE + w as i64 * 150 / 300 * 300));
        write += t;
        windows.push(bytes.unwrap());
    }
    let refs: Vec<&[u8]> = windows.iter().map(Vec::as_slice).collect();
    let window_bytes: usize = windows.iter().map(Vec::len).sum();
    let (hour, merge) = time(|| bundle::merge(&refs, BASE, Unit::Hour).unwrap());
    let hours: Vec<Vec<u8>> = (0..24).map(|_| hour.clone()).collect();
    let hour_refs: Vec<&[u8]> = hours.iter().map(Vec::as_slice).collect();
    let (day, day_merge) = time(|| bundle::merge(&hour_refs, BASE, Unit::Day).unwrap());
    println!(
        "summary {}: window write {:.1} ms avg ({} rows), window {} B avg; \
         hour merge of {SOURCES} {:.1} ms -> {} B; day merge of 24 hours {:.1} ms -> {} B",
        summary::SUMMARY_VERSION,
        write.as_secs_f64() * 1000.0 / SOURCES as f64,
        per_window,
        window_bytes / SOURCES,
        merge.as_secs_f64() * 1000.0,
        hour.len(),
        day_merge.as_secs_f64() * 1000.0,
        day.len(),
    );
    // The stage checks VCPU_STOP_MS (700 ms of Wasm vCPU) between sources. A full batch of
    // windows should fit one execution with room for Wasm's slowdown and the write, and one hour
    // bundle fold must fit the 300 ms left under Compute's 1,000 ms limit; a day whose 24 hours
    // do not fit one execution resumes in the next.
    if !cfg!(debug_assertions) && per_window <= 800 {
        assert!(merge < Duration::from_millis(200), "hour merge {merge:?}");
        let per_hour = day_merge / 24;
        assert!(
            per_hour < Duration::from_millis(30),
            "day merge {per_hour:?} per hour"
        );
    }
}
