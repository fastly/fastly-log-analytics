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
window.location = { pathname: '/shop/a:b' };
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
    let tracker = rum::generate_rum_tracker_js(SID, config);
    let draw = draw.map_or("null".to_owned(), |d| d.to_string());
    let script = format!(
        "var PRESET_DRAW = {draw};\n{HARNESS}\n{tracker}\n{scenario}\n\
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
