//! Runs the generated RUM tracker under Node with a fake browser and a fake Faro SDK.

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use fastly_log_analytics::{ingest, logging::rum};
use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

const SID: &str = "svc1";
const BASE: i64 = 1_767_268_800;

const HARNESS: &str = r#"
var requests = [], timers = new Map(), nextTimer = 1, listeners = {}, initialized = null, scripts = 0;
var store = {};
if (PRESET_DRAW !== null) store.rum_sample = String(PRESET_DRAW);
globalThis.window = globalThis;
window.sessionStorage = {
  getItem: function(k) { return k in store ? store[k] : null; },
  setItem: function(k, v) { store[k] = String(v); }
};
window.location = { pathname: '/shop/a:b', href: 'https://shop.example/shop/a:b' };
var perfObservers = {};
globalThis.PerformanceObserver = function(callback) { this.callback = callback; };
PerformanceObserver.prototype.observe = function(options) {
  (perfObservers[options.type] = perfObservers[options.type] || []).push(this.callback);
};
function perf(type, entries) {
  (perfObservers[type] || []).forEach(function(callback) {
    callback({ getEntries: function() { return entries; } });
  });
}
function res(name, rid, timing) {
  var entry = { name: name, serverTiming: rid ? [{ name: 'rid', description: rid }] : [] };
  Object.keys(timing).forEach(function(k) { entry[k] = timing[k]; });
  return entry;
}
function on(type, fn) { (listeners[type] = listeners[type] || []).push(fn); }
function fire(type) { (listeners[type] || []).forEach(function(fn) { fn(); }); }
window.addEventListener = on;
globalThis.document = {
  cookie: 'x=1; rum_cid=c%2C1', visibilityState: 'visible',
  head: { appendChild: function() { scripts++; } },
  createElement: function() { return {}; },
  addEventListener: on
};
globalThis.setTimeout = function(fn, ms) { var id = nextTimer++; timers.set(id, fn); return id; };
globalThis.clearTimeout = function(id) { timers.delete(id); };
function runTimers() { var due = Array.from(timers.values()); timers.clear(); due.forEach(function(fn) { fn(); }); }
globalThis.fetch = function(url, init) {
  requests.push({ via: 'fetch', url: url, method: init && init.method });
  return Promise.resolve({});
};
Object.defineProperty(globalThis, 'navigator', { configurable: true, writable: true, value: {
  sendBeacon: function(url) { requests.push({ via: 'beacon', url: url }); return true; }
}});
window.Faro = {
  initializeFaro: function(config) { initialized = config; },
  WebVitalsInstrumentation: function() {},
  ErrorsInstrumentation: function() {}
};
function send(item) { return initialized.beforeSend(item); }
function vital(name, value, rating) {
  var values = { delta: 1 }; values[name] = value;
  send({ type: 'measurement', payload: { type: 'web-vitals', values: values, context: { rating: rating } } });
}
"#;

fn run(config: &rum::Config, draw: Option<f64>, scenario: &str) -> Value {
    run_with(config, draw, "", scenario)
}

/// `prelude` runs after the fake browser is set up and before the tracker loads.
fn run_with(config: &rum::Config, draw: Option<f64>, prelude: &str, scenario: &str) -> Value {
    let tracker = rum::generate_rum_tracker_js(SID, config);
    let draw = draw.map_or("null".to_owned(), |d| d.to_string());
    let script = format!(
        "var PRESET_DRAW = {draw};\n{HARNESS}\n{prelude}\n{tracker}\n{scenario}\n\
         process.stdout.write(JSON.stringify({{ requests: requests, initialized: !!initialized, \
         scripts: scripts, draw: store.rum_sample }}));"
    );
    let mut child = Command::new("node")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("node is required to test the RUM tracker");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

fn urls(result: &Value) -> Vec<String> {
    result["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["url"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn a_pageview_flush_sends_every_metric_and_error_in_one_beacon_the_ingester_expands() {
    let result = run(
        &rum::Config::default(),
        None,
        r#"
        vital('fcp', 412, 'good');
        vital('lcp', 1234.5, 'needs-improvement');
        send({ type: 'event', payload: { name: 'faro.performance.navigation',
               attributes: { ttfb: 85, duration: 1503 } } });
        send({ type: 'exception', payload: { value: 'Boom: x|y',
               stacktrace: { frames: [{ filename: 'app.js', lineno: 10, colno: 5 }] } } });
        fetch('/rum-beacon?service_id=svc1', { method: 'POST', body: '{}' });
        if (requests.length !== 0) throw new Error('sent before the debounce');
        runTimers();
        vital('inp', 40, 'good');
        fire('pagehide');
        "#,
    );
    assert_eq!(result["initialized"], true);
    let requests = result["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(
        (requests[0]["via"].as_str(), requests[0]["method"].as_str()),
        (Some("fetch"), Some("POST"))
    );
    assert_eq!(requests[1]["via"], "beacon");
    let urls = urls(&result);
    assert!(
        urls[0].starts_with("/rum-beacon?service_id=svc1&"),
        "{}",
        urls[0]
    );

    let lines: Vec<String> = urls
        .iter()
        .enumerate()
        .map(|(i, url)| {
            json!({"ts_ms": (BASE + i as i64) * 1000, "service_id": SID, "url": url,
                   "pop": "CDG", "country": "FR"})
            .to_string()
        })
        .collect();
    let encoded = zstd::stream::encode_all(lines.join("\n").as_bytes(), 1).unwrap();
    let rows = ingest::rum::convert_object(&encoded, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!((rows.lines_read, rows.bad_lines, rows.rows), (2, 0, 6));
    let vitals = &rows.windows[&BASE];
    assert_eq!(
        strings(vitals, "metric_name"),
        ["fcp", "lcp", "ttfb", "duration", "inp"]
    );
    assert_eq!(
        floats(vitals, "metric_value"),
        [412.0, 1234.5, 85.0, 1503.0, 40.0]
    );
    assert_eq!(
        strings(vitals, "metric_rating"),
        ["good", "needs-improvement", "", "", "good"]
    );
    assert!(strings(vitals, "pathname").iter().all(|p| p == "/shop/a:b"));
    assert!(strings(vitals, "cid").iter().all(|c| c == "c,1"));
    assert_eq!(floats(vitals, "sample_rate"), [1.0; 5]);
    let errors = &rows.errors[&BASE];
    assert_eq!(strings(errors, "error_message"), ["Boom: x|y"]);
    assert_eq!(strings(errors, "error_file"), ["app.js"]);
    assert_eq!(floats(errors, "sample_rate"), [1.0]);
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch.column_by_name(name).unwrap();
    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
    (0..array.len())
        .map(|i| array.value(i).to_owned())
        .collect()
}

fn floats(batch: &RecordBatch, name: &str) -> Vec<f64> {
    let column = batch.column_by_name(name).unwrap();
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..array.len()).map(|i| array.value(i)).collect()
}

#[test]
fn a_large_batch_splits_into_beacons_under_the_url_budget() {
    let result = run(
        &rum::Config::default(),
        None,
        r#"
        for (var i = 0; i < 400; i++) send({ type: 'event', payload: { name: 'clicked_button_' + i } });
        runTimers();
        "#,
    );
    let urls = urls(&result);
    assert!(urls.len() > 1, "{}", urls.len());
    assert!(
        urls.iter().all(|u| u.len() <= 7_500),
        "{:?}",
        urls.iter().map(String::len).collect::<Vec<_>>()
    );
    let metrics: usize = urls
        .iter()
        .map(|u| u.split("&m=").nth(1).unwrap().split(',').count())
        .sum();
    assert_eq!(metrics, 400);
}

#[test]
fn sampling_is_decided_once_per_session_and_errors_use_their_own_rate() {
    let config = rum::Config {
        sample_rate: 0.25,
        error_sample_rate: 0.5,
        ..rum::Config::default()
    };
    let scenario = r#"
        if (initialized) {
          vital('fcp', 412, 'good');
          send({ type: 'exception', payload: { value: 'Boom' } });
          fire('pagehide');
        }
    "#;
    let unsampled = run(&config, Some(0.7), scenario);
    assert_eq!(unsampled["initialized"], false);
    assert_eq!(unsampled["scripts"], 0);
    assert!(urls(&unsampled).is_empty());

    let errors_only = urls(&run(&config, Some(0.3), scenario));
    assert_eq!(errors_only.len(), 1);
    assert!(
        errors_only[0].contains("&sr=0.25&esr=0.5"),
        "{}",
        errors_only[0]
    );
    assert!(errors_only[0].contains("&e=Boom") && !errors_only[0].contains("&m="));

    let both = urls(&run(&config, Some(0.1), scenario));
    assert!(both[0].contains("&m=fcp:412:good") && both[0].contains("&e=Boom"));

    let fresh = run(&config, None, "");
    let draw: f64 = fresh["draw"].as_str().unwrap().parse().unwrap();
    assert!((0.0..1.0).contains(&draw));
}

/// The High-Scale generator (`backend/provision/rum_assets.py`) is pinned to the same file by
/// `test_generate_rum_tracker_js_matches_cloud_generator`, so both editions ship one tracker.
#[test]
fn the_default_tracker_matches_the_shared_parity_fixture() {
    assert_eq!(
        rum::generate_rum_tracker_js(SID, &rum::Config::default()),
        include_str!("rum_tracker_parity.js")
    );
}

/// The High-Scale generators are pinned to the same files by
/// `test_rum_log_format_and_beacon_extraction_match_the_cloud_generators`.
#[test]
fn the_beacon_log_format_and_extraction_match_the_shared_parity_fixtures() {
    assert_eq!(
        rum::generate_rum_log_format().unwrap(),
        include_str!("rum_log_format_parity.txt").trim_end()
    );
    assert_eq!(
        rum::beacon_field_extraction_vcl(),
        include_str!("rum_beacon_extraction_parity.vcl").trim_end()
    );
}

const RESOURCE_SCENARIO: &str = r#"
    perf('resource', [
      res('https://shop.example/img/hero.jpg?v=2', 'rid00001',
          { requestStart: 100, responseEnd: 950.4, transferSize: 5000 }),
      res('https://cdn.shop.example/video/seg1.ts', 'rid00002',
          { requestStart: 0, fetchStart: 200, responseEnd: 2200, transferSize: 900000 }),
      res('https://shop.example/app.js', 'ridstale',
          { requestStart: 10, responseEnd: 11, transferSize: 0 }),
      res('https://other.example/pixel.gif', null,
          { requestStart: 10, responseEnd: 40, transferSize: 300 }),
      res('https://shop.example/rum-beacon?service_id=svc1', 'ridbeacn',
          { requestStart: 10, responseEnd: 40, transferSize: 300 })
    ]);
    perf('navigation', [
      res('https://shop.example/shop/a:b', 'rid00000',
          { requestStart: 5, responseEnd: 180, transferSize: 20000, startTime: 0, loadEventEnd: 1503.2 })
    ]);
    runTimers();
"#;

#[test]
fn a_sampled_pageview_reports_client_ttlb_only_for_requests_that_reached_the_edge() {
    let config = rum::Config {
        resource_sample_rate: 0.5,
        ..rum::Config::default()
    };
    let result = run_with(
        &config,
        None,
        "Math.random = function() { return 0.2; };",
        RESOURCE_SCENARIO,
    );
    let urls = urls(&result);
    assert_eq!(urls.len(), 1, "{urls:?}");
    assert!(urls[0].contains("&rsr=0.5"), "{}", urls[0]);
    // The browser-cached file, the third-party file without a request ID and the beacon
    // itself are all absent.
    assert!(
        urls[0].contains(
            "&r=rid00001:%2Fimg%2Fhero.jpg:850,rid00002:%2Fvideo%2Fseg1.ts:2000,\
             rid00000:%2Fshop%2Fa%3Ab:175"
        ),
        "{}",
        urls[0]
    );
    assert!(urls[0].contains("&m=page_load:1503:"), "{}", urls[0]);

    let line = json!({"ts_ms": BASE * 1000, "service_id": SID, "url": urls[0],
                      "fastly_req_id": "beaconid", "asn": 7922, "pop": "CDG", "country": "FR"})
    .to_string();
    let encoded = zstd::stream::encode_all(line.as_bytes(), 1).unwrap();
    let rows = ingest::rum::convert_object(&encoded, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!((rows.lines_read, rows.bad_lines, rows.rows), (1, 0, 4));
    let vitals = &rows.windows[&BASE];
    assert_eq!(
        strings(vitals, "metric_name"),
        ["page_load", "ttlb", "ttlb", "ttlb"]
    );
    assert_eq!(
        floats(vitals, "metric_value"),
        [1503.0, 850.0, 2000.0, 175.0]
    );
    assert_eq!(
        strings(vitals, "req_id"),
        ["beaconid", "rid00001", "rid00002", "rid00000"]
    );
    assert_eq!(
        strings(vitals, "resource_url"),
        ["", "/img/hero.jpg", "/video/seg1.ts", "/shop/a:b"]
    );
    assert!(strings(vitals, "pathname").iter().all(|p| p == "/shop/a:b"));
    assert!(strings(vitals, "asn").iter().all(|a| a == "7922"));
    assert_eq!(floats(vitals, "sample_rate"), [1.0, 0.5, 0.5, 0.5]);
}

#[test]
fn resource_timing_is_skipped_when_unsampled_or_off_and_capped_per_pageview() {
    let config = rum::Config {
        resource_sample_rate: 0.5,
        ..rum::Config::default()
    };
    let unsampled = urls(&run_with(
        &config,
        Some(0.1),
        "Math.random = function() { return 0.9; };",
        RESOURCE_SCENARIO,
    ));
    assert_eq!(unsampled.len(), 1, "{unsampled:?}");
    assert!(
        unsampled[0].contains("&m=page_load:1503:") && !unsampled[0].contains("&r="),
        "{}",
        unsampled[0]
    );

    let off = rum::Config {
        capture_resources: false,
        resource_sample_rate: 1.0,
        ..rum::Config::default()
    };
    let off = urls(&run(&off, Some(0.1), RESOURCE_SCENARIO));
    assert!(!off[0].contains("&r="), "{}", off[0]);

    let all = rum::Config {
        resource_sample_rate: 1.0,
        ..rum::Config::default()
    };
    let capped = urls(&run(
        &all,
        Some(0.1),
        r#"
        var many = [];
        for (var i = 0; i < 80; i++) {
          many.push(res('https://shop.example/a' + i + '.js', 'rid' + i,
                        { requestStart: 1, responseEnd: 9, transferSize: 10 }));
        }
        perf('resource', many);
        runTimers();
        "#,
    ));
    let reported = |urls: &[String]| -> usize {
        urls.iter()
            .filter_map(|u| u.split("&r=").nth(1))
            .map(|r| r.split('&').next().unwrap().split(',').count())
            .sum()
    };
    assert_eq!(reported(&capped), 50);

    // A long-lived page (a video session) keeps reporting: the budget refills one request every
    // five seconds, so later downloads are measured at a bounded rate.
    let long_session = urls(&run_with(
        &all,
        Some(0.1),
        "var NOW = 1000000; Date.now = function() { return NOW; };",
        r#"
        function burst(prefix, n) {
          var many = [];
          for (var i = 0; i < n; i++) {
            many.push(res('https://shop.example/' + prefix + i + '.ts', prefix + i,
                          { requestStart: 1, responseEnd: 9, transferSize: 10 }));
          }
          perf('resource', many);
          runTimers();
        }
        burst('a', 80);
        NOW += 60000;
        burst('b', 30);
        "#,
    ));
    assert_eq!(reported(&long_session), 50 + 12);
    assert!(
        long_session.iter().any(|u| u.contains("b11:"))
            && !long_session.iter().any(|u| u.contains("b12:"))
    );
}

#[test]
fn sec20d_validate_service_id_and_js_escaping_prevent_script_breakout() {
    for valid in ["svc1", "svc-1", "fixtureService", "AbCdEfGhIjKlMnOpQrSt12"] {
        assert!(
            rum::validate_service_id(valid).is_ok(),
            "expected valid service_id: {valid}"
        );
    }
    for invalid in [
        "",
        &"a".repeat(65),
        "svc'1",
        "svc*/<script>",
        "svc/1",
        "svc 1",
    ] {
        assert!(
            rum::validate_service_id(invalid).is_err(),
            "expected invalid service_id: {invalid}"
        );
    }

    let hostile = "svc'; globalThis.pwned = true; */ </script>";
    let tracker = rum::generate_rum_tracker_js(hostile, &rum::Config::default());
    assert!(!tracker.contains("</script>"));
    assert!(!tracker.contains("/* Faro Web SDK wrapper for service"));
    assert!(tracker
        .contains(r#"var SERVICE_ID = "svc'; globalThis.pwned = true; */ \u003c/script\u003e";"#));
    assert!(tracker.contains("'?service_id=' + enc(SERVICE_ID)"));

    let script = format!(
        "var PRESET_DRAW = 0.1;\nglobalThis.pwned = false;\n{HARNESS}\n{tracker}\n\
         vital('fcp', 100, 'good'); runTimers();\n\
         process.stdout.write(JSON.stringify({{ pwned: globalThis.pwned, urls: requests.map(r => r.url) }}));"
    );
    let mut child = Command::new("node")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("node is required");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(parsed["pwned"], false);
    let first_url = parsed["urls"][0].as_str().unwrap();
    assert!(
        first_url.contains("service_id=svc'%3B%20globalThis.pwned%20%3D%20true"),
        "{first_url}"
    );
}
