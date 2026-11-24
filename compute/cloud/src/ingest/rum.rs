//! RUM beacon conversion: the `raw/rum/` stream into `processed/rum/client_vitals/` and
//! `processed/rum/client_errors/` window files, through the same coordinator, worker shards and
//! window naming as requests (no summaries). A port of v3's `_parse_rum_line` and
//! `extract_metrics_from_faro_payload`; `timestamp` is milliseconds (from `ts_ms` when logged)
//! where v3 kept microseconds of the second-precision `timestamp`.

use super::convert::{self, ConvertError, ObjectRows};
use arrow_array::{
    builder::{Float64Builder, Int32Builder, StringBuilder, TimestampMillisecondBuilder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub const STREAM: &str = "rum";
pub const VITALS: &str = "client_vitals";
pub const ERRORS: &str = "client_errors";
const SCHEMA_VERSION: &str = "rum-v1";
const TAIL: [&str; 11] = [
    "pathname", "browser", "os", "device", "cid", "req_id", "city", "region", "country", "pop",
    "tls",
];

fn timestamp() -> Field {
    Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        true,
    )
}

fn with_tail(mut fields: Vec<Field>) -> SchemaRef {
    fields.extend(
        TAIL.iter()
            .map(|name| Field::new(*name, DataType::Utf8, true)),
    );
    fields.push(Field::new("ttfb", DataType::Float64, true));
    fields.push(Field::new("sample_rate", DataType::Float64, true));
    Arc::new(Schema::new(fields))
}

pub fn vitals_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            with_tail(vec![
                timestamp(),
                Field::new("metric_name", DataType::Utf8, true),
                Field::new("metric_value", DataType::Float64, true),
                Field::new("metric_rating", DataType::Utf8, true),
            ])
        })
        .clone()
}

pub fn errors_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            with_tail(vec![
                timestamp(),
                Field::new("error_message", DataType::Utf8, true),
                Field::new("error_file", DataType::Utf8, true),
                Field::new("error_line", DataType::Int32, true),
                Field::new("error_col", DataType::Int32, true),
            ])
        })
        .clone()
}

/// Identity of the RUM conversion, which plans and workers must agree on (like
/// [`convert::RequestSchema::hash`]).
pub fn schema_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(format!(
            "{SCHEMA_VERSION}|{}|{}|{}|",
            convert::ROUTING_TOLERANCE_S,
            convert::MAX_LINE_BYTES,
            convert::MAX_DECOMPRESSED_BYTES
        ));
        for schema in [vitals_schema(), errors_schema()] {
            for field in schema.fields() {
                digest.update(format!("{}:{};", field.name(), field.data_type()));
            }
            digest.update("|");
        }
        hex::encode(&digest.finalize()[..8])
    })
}

/// Python truthiness for the JSON values a beacon line carries.
fn truthy(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    })
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The first truthy of `keys` in `object`, as text.
fn first(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| truthy(object.get(*key)))
        .map(text)
}

/// v3's `safe_float`: numbers, booleans and numeric strings; anything else is `None`.
fn float(value: Option<&Value>) -> Option<f64> {
    let parsed = match value? {
        Value::Number(n) => n.as_f64(),
        Value::Bool(b) => Some(f64::from(u8::from(*b))),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    };
    parsed.filter(|v| v.is_finite())
}

/// v3's `safe_int` (default 0), within the column's `int32`.
fn int(value: Option<&Value>) -> i32 {
    let parsed = match value {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)),
        Some(Value::Bool(b)) => Some(i64::from(*b)),
        Some(Value::String(s)) => s.trim().parse().ok(),
        _ => None,
    };
    parsed.and_then(|v| i32::try_from(v).ok()).unwrap_or(0)
}

/// `urllib.parse.urlparse(url).path`.
fn url_path(url: &str) -> &str {
    let url = url.split(['#', '?']).next().unwrap_or_default();
    let rest = match url.split_once(':') {
        Some((scheme, rest))
            if scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) =>
        {
            rest
        }
        _ => url,
    };
    match rest.strip_prefix("//") {
        Some(authority) => authority.find('/').map_or("", |i| &authority[i..]),
        None => rest,
    }
}

/// `parse_qs(urlparse(url).query)[name][0]`, skipping blank values like `parse_qs` does.
fn query_param(url: &str, name: &str) -> Option<String> {
    let query = url.split('#').next()?.split_once('?')?.1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, value)| key == name && !value.is_empty())
        .map(|(_, value)| value.into_owned())
}

#[derive(Default)]
struct Metric {
    name: Option<String>,
    value: Option<Value>,
    rating: Option<String>,
    pathname: Option<String>,
    cid: Option<String>,
    browser: Option<String>,
    os: Option<String>,
    device: Option<String>,
    error: Option<(String, String, i32, i32)>,
    sample_rate: Option<f64>,
}

fn faro_metrics(payload: &Map<String, Value>, line: &Map<String, Value>) -> Vec<Metric> {
    let object = |value: Option<&Value>| value.and_then(Value::as_object).cloned();
    let meta = object(payload.get("meta")).unwrap_or_default();
    let browser = object(meta.get("browser")).unwrap_or_default();
    let os = object(meta.get("os")).unwrap_or_default();
    let page = truthy(payload.get("page"))
        .or_else(|| truthy(meta.get("page")))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let browser_name = first(&browser, &["name"])
        .or_else(|| first(line, &["browser"]))
        .unwrap_or_else(|| "Chrome".into());
    let os_name = first(&os, &["name"])
        .or_else(|| first(line, &["os"]))
        .unwrap_or_else(|| "macOS".into());
    let device = if truthy(browser.get("mobile")).is_some() {
        "Mobile"
    } else {
        "Desktop"
    };
    let url = first(&page, &["url"])
        .or_else(|| first(line, &["url"]))
        .unwrap_or_default();
    let pathname = if url.is_empty() {
        "/".to_owned()
    } else {
        url_path(&url).to_owned()
    };
    let cid = first(line, &["rum_cid", "cid"]).unwrap_or_default();
    let entry = |name: String, value: Option<Value>, rating: String| Metric {
        name: Some(name),
        value,
        rating: Some(rating),
        pathname: Some(pathname.clone()),
        cid: Some(cid.clone()),
        browser: Some(browser_name.clone()),
        os: Some(os_name.clone()),
        device: Some(device.to_owned()),
        error: None,
        sample_rate: None,
    };
    let list = |key: &str| {
        payload
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for m in list("measurements").iter().filter_map(Value::as_object) {
        if m.get("type").and_then(Value::as_str) != Some("web-vitals") {
            continue;
        }
        let rating = m
            .get("context")
            .and_then(Value::as_object)
            .and_then(|c| first(c, &["rating"]))
            .or_else(|| {
                m.get("meta")
                    .and_then(Value::as_object)
                    .and_then(|c| first(c, &["rating"]))
            })
            .unwrap_or_default();
        for (k, v) in m
            .get("values")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            if k != "delta" {
                out.push(entry(k.clone(), Some(v.clone()), rating.clone()));
            }
        }
    }
    for e in list("events").iter().filter_map(Value::as_object) {
        let Some(name) = first(e, &["name"]) else {
            continue;
        };
        if name == "faro.performance.navigation" {
            let values = truthy(e.get("attributes"))
                .or_else(|| truthy(e.get("values")))
                .and_then(Value::as_object);
            for (k, v) in values.into_iter().flatten() {
                out.push(entry(k.clone(), Some(v.clone()), String::new()));
            }
        } else {
            out.push(entry(
                format!("event_{name}"),
                Some(1.0.into()),
                String::new(),
            ));
        }
    }
    for log in list("logs").iter().filter_map(Value::as_object) {
        let level = first(log, &["level"]).unwrap_or_else(|| "info".into());
        out.push(entry("log".into(), Some(1.0.into()), level));
    }
    for exception in list("exceptions").iter().filter_map(Value::as_object) {
        let message =
            first(exception, &["value", "message"]).unwrap_or_else(|| "Unknown error".into());
        let frame = exception
            .get("stacktrace")
            .and_then(Value::as_object)
            .and_then(|s| s.get("frames"))
            .and_then(Value::as_array)
            .and_then(|frames| frames.first())
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut metric = entry("exception".into(), None, String::new());
        metric.error = Some((
            message,
            first(&frame, &["filename"]).unwrap_or_else(|| "unknown.js".into()),
            int(truthy(frame.get("lineno"))),
            int(truthy(frame.get("colno"))),
        ));
        out.push(metric);
    }
    if out.is_empty() {
        out.push(entry("pageview".into(), Some(1.0.into()), String::new()));
    }
    out
}

/// A `rum_body` holding a Faro payload, possibly JSON-encoded twice.
fn faro_payload(body: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str(body).ok()? {
        Value::String(inner) => serde_json::from_str(&inner).ok(),
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn query_metric(line: &Map<String, Value>) -> Metric {
    let mut name = first(line, &["rum_metric_name"]);
    let mut value = line.get("rum_metric_value").cloned();
    let mut rating = first(line, &["rum_metric_rating"]);
    let mut cid = first(line, &["rum_cid", "cid"]);
    let mut pathname = first(line, &["rum_pathname"]);
    let url = first(line, &["url", "rum_raw_query"]).unwrap_or_default();
    if !url.is_empty() {
        name = name.or_else(|| query_param(&url, "rum_metric_name"));
        if matches!(&value, None | Some(Value::Null))
            || value.as_ref().and_then(Value::as_str) == Some("")
        {
            value = query_param(&url, "rum_metric_value")
                .map(Value::String)
                .or(value);
        }
        rating = rating.or_else(|| query_param(&url, "rum_metric_rating"));
        cid = cid.or_else(|| query_param(&url, "cid"));
        pathname = pathname.or_else(|| query_param(&url, "rum_pathname"));
    }
    if pathname.is_none() {
        pathname = first(line, &["referer"])
            .map(|referer| url_path(&referer).to_owned())
            .filter(|p| !p.is_empty());
    }
    let error = first(line, &["rum_error_message"]).map(|message| {
        (
            message,
            first(line, &["rum_error_file"]).unwrap_or_else(|| "unknown.js".into()),
            int(line.get("rum_error_line")),
            int(line.get("rum_error_col")),
        )
    });
    Metric {
        name,
        value,
        rating,
        pathname: Some(pathname.unwrap_or_else(|| "/".into()).replace("//", "/")),
        cid: Some(cid.unwrap_or_default()),
        browser: first(line, &["browser"]),
        os: first(line, &["os"]),
        device: first(line, &["device"]),
        error,
        sample_rate: None,
    }
}

fn decode(component: &str) -> String {
    percent_encoding::percent_decode_str(component)
        .decode_utf8_lossy()
        .into_owned()
}

/// A batched beacon (`m=name:value:rating,…`, `e=message|file|line|col`, one `e` per error):
/// every queued metric and error of one page flush. Separators are split before decoding, as
/// the tracker percent-encodes each component. `None` for a legacy one-metric beacon.
fn batch_metrics(line: &Map<String, Value>) -> Option<Vec<Metric>> {
    let url = first(line, &["url"])?;
    let query = url.split('#').next()?.split_once('?')?.1;
    let params: Vec<(&str, &str)> = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect();
    if !params.iter().any(|(key, _)| matches!(*key, "m" | "e")) {
        return None;
    }
    let param = |name: &str| {
        params
            .iter()
            .find(|(key, value)| *key == name && !value.is_empty())
            .map(|(_, value)| decode(value))
    };
    let pathname = param("p").unwrap_or_else(|| "/".into());
    let cid = param("cid").unwrap_or_default();
    let rate = |name: &str| {
        param(name)
            .and_then(|rate| rate.parse::<f64>().ok())
            .filter(|rate| *rate > 0.0 && *rate <= 1.0)
    };
    let (sample_rate, error_sample_rate) = (rate("sr"), rate("esr"));
    let entry = |name: String, value: Option<Value>, rating: String| Metric {
        name: Some(name),
        value,
        rating: Some(rating),
        pathname: Some(pathname.clone()),
        cid: Some(cid.clone()),
        sample_rate,
        ..Metric::default()
    };
    let mut out = Vec::new();
    for (key, value) in &params {
        match *key {
            "m" => {
                for metric in value.split(',').filter(|m| !m.is_empty()) {
                    let mut parts = metric.split(':').map(decode);
                    let name = parts.next().unwrap_or_default();
                    let value = parts.next().map(Value::String);
                    out.push(entry(name, value, parts.next().unwrap_or_default()));
                }
            }
            "e" => {
                let mut parts = value.split('|').map(decode);
                let message = parts.next().filter(|m| !m.is_empty());
                let Some(message) = message else {
                    continue;
                };
                let file = parts
                    .next()
                    .filter(|f| !f.is_empty())
                    .unwrap_or_else(|| "unknown.js".into());
                let mut number = || int(parts.next().map(Value::String).as_ref());
                let (error_line, error_col) = (number(), number());
                let mut metric = entry("exception".into(), None, String::new());
                metric.error = Some((message, file, error_line, error_col));
                metric.sample_rate = error_sample_rate;
                out.push(metric);
            }
            _ => {}
        }
    }
    Some(out)
}

struct Tail {
    strings: [StringBuilder; 11],
    ttfb: Float64Builder,
    sample_rate: Float64Builder,
}

impl Tail {
    fn new() -> Self {
        Self {
            strings: std::array::from_fn(|_| StringBuilder::new()),
            ttfb: Float64Builder::new(),
            sample_rate: Float64Builder::new(),
        }
    }
    /// Unsampled (legacy) beacons count as a 1.0 sample rate.
    fn push(&mut self, values: &[String; 11], ttfb: Option<f64>, sample_rate: Option<f64>) {
        for (builder, value) in self.strings.iter_mut().zip(values) {
            builder.append_value(value);
        }
        self.ttfb.append_option(ttfb);
        self.sample_rate.append_value(sample_rate.unwrap_or(1.0));
    }
    fn finish(mut self, mut arrays: Vec<ArrayRef>) -> Vec<ArrayRef> {
        for builder in &mut self.strings {
            arrays.push(Arc::new(builder.finish()));
        }
        arrays.push(Arc::new(self.ttfb.finish()));
        arrays.push(Arc::new(self.sample_rate.finish()));
        arrays
    }
}

struct Vitals {
    timestamp: TimestampMillisecondBuilder,
    name: StringBuilder,
    value: Float64Builder,
    rating: StringBuilder,
    tail: Tail,
}

struct Errors {
    timestamp: TimestampMillisecondBuilder,
    message: StringBuilder,
    file: StringBuilder,
    line: Int32Builder,
    col: Int32Builder,
    tail: Tail,
}

fn utc() -> TimestampMillisecondBuilder {
    TimestampMillisecondBuilder::new().with_timezone("UTC")
}

impl Vitals {
    fn new() -> Self {
        Self {
            timestamp: utc(),
            name: StringBuilder::new(),
            value: Float64Builder::new(),
            rating: StringBuilder::new(),
            tail: Tail::new(),
        }
    }
    fn finish(mut self) -> Result<RecordBatch, ConvertError> {
        let head: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.rating.finish()),
        ];
        RecordBatch::try_new(vitals_schema(), self.tail.finish(head))
            .map_err(|_| ConvertError::Corrupt)
    }
}

impl Errors {
    fn new() -> Self {
        Self {
            timestamp: utc(),
            message: StringBuilder::new(),
            file: StringBuilder::new(),
            line: Int32Builder::new(),
            col: Int32Builder::new(),
            tail: Tail::new(),
        }
    }
    fn finish(mut self) -> Result<RecordBatch, ConvertError> {
        let head: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.message.finish()),
            Arc::new(self.file.finish()),
            Arc::new(self.line.finish()),
            Arc::new(self.col.finish()),
        ];
        RecordBatch::try_new(errors_schema(), self.tail.finish(head))
            .map_err(|_| ConvertError::Corrupt)
    }
}

struct Row {
    metric: Metric,
    tail: [String; 11],
}

enum Parsed {
    Rows(i64, Option<f64>, Vec<Row>),
    Skip,
    Bad,
}

const COMPACT_RUM_KEYS: [&str; 10] = [
    "ts_ms",
    "service_id",
    "fastly_req_id",
    "url",
    "city",
    "region",
    "country",
    "pop",
    "tls",
    "ttfb",
];

fn parse_line(bytes: &[u8], service_id: &str) -> Parsed {
    let line = match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Object(map)) => map,
        Ok(Value::Array(values))
            if !values.is_empty() && values.len() <= COMPACT_RUM_KEYS.len() =>
        {
            COMPACT_RUM_KEYS
                .iter()
                .copied()
                .zip(values)
                .map(|(k, v)| (k.to_owned(), v))
                .collect()
        }
        _ => return Parsed::Bad,
    };
    if first(&line, &["service_id", "rum_service_id"]).is_some_and(|sid| sid != service_id) {
        return Parsed::Skip;
    }
    if line.get("method").and_then(Value::as_str) == Some("OPTIONS")
        || truthy(line.get("is_healthcheck")).is_some()
        || line
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(|p| matches!(p, "/health" | "/ping" | "/favicon.ico"))
    {
        return Parsed::Skip;
    }
    let ms = line.get("ts_ms").and_then(Value::as_i64).or_else(|| {
        line.get("timestamp")
            .and_then(Value::as_str)
            .and_then(convert::parse_ms)
    });
    let Some(ms) = ms else {
        return Parsed::Bad;
    };
    let metrics = line
        .get("rum_body")
        .and_then(Value::as_str)
        .filter(|body| !body.is_empty())
        .and_then(faro_payload)
        .map(|payload| faro_metrics(&payload, &line))
        .or_else(|| batch_metrics(&line))
        .unwrap_or_else(|| vec![query_metric(&line)]);
    let or = |value: &Option<String>, key: &str, default: &str| {
        value
            .clone()
            .filter(|v| !v.is_empty())
            .or_else(|| first(&line, &[key]))
            .unwrap_or_else(|| default.into())
    };
    let rows = metrics
        .into_iter()
        .map(|metric| {
            let tail = [
                metric
                    .pathname
                    .clone()
                    .filter(|p| !p.is_empty())
                    .unwrap_or_else(|| "/".into()),
                or(&metric.browser, "browser", "Chrome"),
                or(&metric.os, "os", "macOS"),
                or(&metric.device, "device", "Desktop"),
                metric.cid.clone().unwrap_or_default(),
                first(&line, &["fastly_req_id"]).unwrap_or_default(),
                first(&line, &["city", "geo_city"]).unwrap_or_default(),
                first(&line, &["region", "geo_region"]).unwrap_or_default(),
                first(&line, &["country", "geo_country_code"]).unwrap_or_default(),
                first(&line, &["pop", "server_pop"]).unwrap_or_default(),
                first(&line, &["tls", "tls_version"]).unwrap_or_default(),
            ];
            Row { metric, tail }
        })
        .collect();
    let ttfb = float(truthy(line.get("ttfb")).or_else(|| truthy(line.get("time_to_first_byte"))));
    Parsed::Rows(ms, ttfb, rows)
}

/// Converts one raw RUM object into `client_vitals` (`ObjectRows::windows`) and `client_errors`
/// (`ObjectRows::errors`) rows by window. Beacons of another service, `OPTIONS` preflights and
/// health checks are skipped; a line that is not a JSON object or has no time is a bad line,
/// and the rest of the object still converts.
pub fn convert_object(
    gzip: &[u8],
    window_s: i64,
    object_time_s: i64,
    max_decompressed: u64,
    service_id: &str,
) -> Result<ObjectRows, ConvertError> {
    let mut vitals: BTreeMap<i64, Vitals> = BTreeMap::new();
    let mut errors: BTreeMap<i64, Errors> = BTreeMap::new();
    let (mut rows, mut lines_read, mut bad_lines) = (0u64, 0u64, 0u64);
    let mut bad_sample = None;
    let decompressed = convert::read_lines(gzip, max_decompressed, |line| {
        lines_read += 1;
        let parsed = line
            .good()
            .map_or(Parsed::Bad, |bytes| parse_line(bytes, service_id));
        let (ms, ttfb, parsed_rows) = match parsed {
            Parsed::Rows(ms, ttfb, parsed_rows) => (ms, ttfb, parsed_rows),
            Parsed::Skip => return,
            Parsed::Bad => {
                bad_lines += 1;
                bad_sample.get_or_insert_with(|| convert::sample_of(line.bytes()));
                return;
            }
        };
        let start = convert::window_of(convert::route(Some(ms), object_time_s), window_s);
        for Row { metric, tail } in parsed_rows {
            rows += 1;
            if let Some((message, file, error_line, error_col)) = metric.error {
                let e = errors.entry(start).or_insert_with(Errors::new);
                e.timestamp.append_value(ms);
                e.message.append_value(message);
                e.file.append_value(file);
                e.line.append_value(error_line);
                e.col.append_value(error_col);
                e.tail.push(&tail, ttfb, metric.sample_rate);
            } else {
                let v = vitals.entry(start).or_insert_with(Vitals::new);
                v.timestamp.append_value(ms);
                v.name
                    .append_value(metric.name.as_deref().unwrap_or("unknown"));
                v.value
                    .append_value(float(metric.value.as_ref()).unwrap_or(0.0));
                v.rating.append_value(metric.rating.unwrap_or_default());
                v.tail.push(&tail, ttfb, metric.sample_rate);
            }
        }
    })?;
    Ok(ObjectRows {
        windows: vitals
            .into_iter()
            .map(|(start, b)| b.finish().map(|batch| (start, batch)))
            .collect::<Result<_, _>>()?,
        errors: errors
            .into_iter()
            .map(|(start, b)| b.finish().map(|batch| (start, batch)))
            .collect::<Result<_, _>>()?,
        rows,
        lines_read,
        bad_lines,
        bad_sample,
        decompressed,
    })
}
