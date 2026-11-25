use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Vendored Faro Web SDK bundles, newest first. A service pins the version stored in its
/// `svc:<id>` config; only an admin settings change moves it.
pub const SUPPORTED_FARO_VERSIONS: &[&str] = &["2.13.0", "2.10.0"];
/// The version new services start on.
pub const DEFAULT_FARO_VERSION: &str = "2.13.0";
/// The version every service used before versions were selectable, so a stored RUM config
/// without one keeps it instead of silently upgrading.
pub const LEGACY_FARO_VERSION: &str = "2.10.0";
pub const DEFAULT_RUM_ENDPOINT_NAME: &str = "Fastly RUM Logs";
pub const DEFAULT_RUM_LOG_PERIOD: u32 = 300;
pub const TRACKER_OBJECT_KEY: &str = "rum/rum-tracker.js";

/// `package/dist/bundle/faro-web-sdk.iife.js` from the `@grafana/faro-web-sdk` npm tarball,
/// verified against the registry's sha512 integrity before vendoring.
const FARO_BUNDLES: &[(&str, &[u8], &str)] = &[
    (
        "2.13.0",
        include_bytes!("../../../../backend/static/faro-web-sdk-v2.13.0.iife.js"),
        "85741c45fd1fd4875c5f25a95a7940e1171ea11e308a83d88fd5f3a5dcab8c1c",
    ),
    (
        "2.10.0",
        include_bytes!("../../../../backend/static/faro-web-sdk-v2.10.0.iife.js"),
        "cd02768dda8e6dc88b558bc288e1ec2ec416f6ec8f7cce04782d52e70c478b69",
    ),
];

pub const RUM_FIELD_NAMES: &[&str] = &[
    "service_id",
    "rum_cid",
    "fastly_req_id",
    "rum_metric_name",
    "rum_metric_value",
    "rum_metric_rating",
    "rum_error_message",
    "rum_error_stack",
    "rum_trace_id",
    "rum_span_id",
    "rum_pathname",
    "rum_connection_speed",
    "rum_dns_ms",
    "rum_tcp_ms",
    "rum_tls_ms",
    "rum_ttfb_ms",
    "rum_raw_query",
    "rum_body",
];

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub endpoint_name: String,
    pub log_period: u32,
    pub custom_condition: String,
    #[serde(default = "legacy_faro_version")]
    pub faro_version: String,
    pub capture_vitals: bool,
    pub capture_performance: bool,
    pub capture_errors: bool,
    pub capture_events: bool,
    /// Share of browser sessions that send vitals, performance and events, in [0.01, 1].
    pub sample_rate: f64,
    /// Share of browser sessions that send errors, in [0.01, 1].
    pub error_sample_rate: f64,
    /// Report client time-to-last-byte for the requests a page view made through this service.
    pub capture_resources: bool,
    /// Share of page views that report resource timing, in [0.01, 1].
    pub resource_sample_rate: f64,
    /// Page origins allowed to read cross-host `Server-Timing: rid` headers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timing_allow_origins: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint_name: DEFAULT_RUM_ENDPOINT_NAME.into(),
            log_period: DEFAULT_RUM_LOG_PERIOD,
            custom_condition: String::new(),
            faro_version: DEFAULT_FARO_VERSION.into(),
            capture_vitals: true,
            capture_performance: true,
            capture_errors: true,
            capture_events: true,
            sample_rate: 1.0,
            error_sample_rate: 1.0,
            capture_resources: true,
            resource_sample_rate: 0.1,
            timing_allow_origins: Vec::new(),
        }
    }
}

pub fn shield_for_region(region: &str) -> &'static str {
    crate::catalog::data()["fos_regions"][region]
        .as_str()
        .unwrap_or("iad-va-us")
}

fn legacy_faro_version() -> String {
    LEGACY_FARO_VERSION.into()
}

pub fn faro_bundle_bytes(version: &str) -> Result<&'static [u8], String> {
    FARO_BUNDLES
        .iter()
        .find(|(v, _, _)| *v == version)
        .map(|(_, bytes, _)| *bytes)
        .ok_or_else(|| format!("Unsupported Faro Web SDK version: {version}"))
}

pub fn faro_bundle_sha256(version: &str) -> Option<&'static str> {
    FARO_BUNDLES
        .iter()
        .find(|(v, _, _)| *v == version)
        .map(|(_, _, sha256)| *sha256)
}

pub fn faro_object_key(version: &str) -> String {
    format!("rum/faro-web-sdk-v{version}.iife.js")
}

pub fn validate_faro_version(version: &str) -> Result<(), String> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()))
    {
        return Err(format!(
            "faro_version must be a plain X.Y.Z version string; got {version:?}"
        ));
    }
    Ok(())
}

pub fn validate_shield_pop(raw: &str) -> Result<&str, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 64
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(format!("invalid shield_pop: {raw:?}"));
    }
    Ok(trimmed)
}

pub fn validate_custom_condition(condition: &str) -> Result<(), String> {
    super::validate_vcl_condition(condition)?;
    let trimmed = condition.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    // Check for req.url or req.url.path predicate
    if contains_req_url_predicate(trimmed) {
        return Err(
            "A condition on req.url / req.url.path cannot work here: it is AND-ed onto req.url.path == \"/rum-beacon\", so the path is always \"/rum-beacon\". Such a condition is either a no-op or silently disables all RUM logging. To exclude beacons by page URL, filter in the tracker — the page URL is in the beacon's POST body, which VCL cannot read.".into()
        );
    }
    Ok(())
}

fn contains_req_url_predicate(s: &str) -> bool {
    let unquoted = super::scan_vcl_delimiters(s).unwrap_or_else(|_| s.to_owned());
    let lower = super::normalize_vcl_dots(&unquoted.to_ascii_lowercase());
    let bytes = lower.as_bytes();
    let needle = b"req.url";
    let mut idx = 0;
    while idx + needle.len() <= bytes.len() {
        if let Some(pos) = lower[idx..].find("req.url") {
            let start = idx + pos;
            let before_ok = super::is_vcl_token_start(bytes, start)
                && !(start >= 2
                    && bytes[start - 1] == b'.'
                    && (bytes[start - 2].is_ascii_alphanumeric()
                        || matches!(bytes[start - 2], b'_' | b'-')));
            let end = start + needle.len();
            let rest = &lower[end..];
            let after_end = if rest.starts_with(".path") {
                end + 5
            } else {
                end
            };
            let after_ok = after_end >= bytes.len()
                || !(bytes[after_end].is_ascii_alphanumeric()
                    || bytes[after_end] == b'_'
                    || bytes[after_end] == b'.');
            if before_ok && after_ok {
                return true;
            }
            idx = start + needle.len();
        } else {
            break;
        }
    }
    false
}

pub fn validate_config(config: &Config) -> Result<(), String> {
    if !(1..=3600).contains(&config.log_period) {
        return Err("rum.log_period must be in range [1, 3600]".into());
    }
    if config.endpoint_name.is_empty()
        || config.endpoint_name.len() > 80
        || config.endpoint_name.chars().any(|c| c.is_control())
    {
        return Err("rum.endpoint_name is invalid".into());
    }
    if ![
        config.sample_rate,
        config.error_sample_rate,
        config.resource_sample_rate,
    ]
    .iter()
    .all(|rate| (0.01..=1.0).contains(rate))
    {
        return Err("rum sample rates must be in range [0.01, 1]".into());
    }
    validate_faro_version(&config.faro_version)?;
    if !SUPPORTED_FARO_VERSIONS.contains(&config.faro_version.as_str()) {
        return Err("rum.faro_version is unsupported".into());
    }
    validate_timing_allow_origins(&config.timing_allow_origins)?;
    validate_custom_condition(&config.custom_condition)
}

pub const MAX_TIMING_ALLOW_ORIGINS: usize = 20;

fn valid_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && !host.contains("..")
        && host
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric())
        && host
            .bytes()
            .last()
            .is_some_and(|b| b.is_ascii_alphanumeric())
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
}

pub fn valid_timing_allow_origin(origin: &str) -> bool {
    let Some(rest) = origin.strip_prefix("https://") else {
        return false;
    };
    if origin.contains("..") {
        return false;
    }
    let host = match rest.rsplit_once(':') {
        Some((h, port))
            if !port.is_empty() && port.len() <= 5 && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            h
        }
        Some(_) => return false,
        None => rest,
    };
    valid_hostname(host)
}

pub fn validate_timing_allow_origins(origins: &[String]) -> Result<Vec<String>, String> {
    if origins.len() > MAX_TIMING_ALLOW_ORIGINS {
        return Err("rum timing_allow_origins must contain at most 20 origins".into());
    }
    let mut out = Vec::with_capacity(origins.len());
    let mut seen = std::collections::BTreeSet::new();
    for origin in origins {
        if !valid_timing_allow_origin(origin) {
            return Err(format!(
                "rum timing_allow_origins entries must be https://host[:port] origins; got {origin:?}"
            ));
        }
        if seen.insert(origin.clone()) {
            out.push(origin.clone());
        }
    }
    Ok(out)
}

pub fn service_timing_origins(domains: &[Value]) -> Vec<String> {
    let mut hosts: Vec<String> = domains
        .iter()
        .filter_map(|d| d["name"].as_str().or_else(|| d.as_str()))
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|h| valid_hostname(h))
        .collect();
    hosts.sort();
    hosts.dedup();
    hosts
        .into_iter()
        .take(MAX_TIMING_ALLOW_ORIGINS)
        .map(|host| format!("https://{host}"))
        .collect()
}

pub fn build_rum_log_condition(custom_condition: &str) -> Result<String, String> {
    validate_custom_condition(custom_condition)?;
    let base = "req.url.path == \"/rum-beacon\" && req.method == \"POST\" && fastly.ff.visits_this_service == 0 && req.restarts == 0";
    let trimmed = custom_condition.trim();
    if trimmed.is_empty() {
        Ok(base.into())
    } else {
        Ok(format!("{base} && ({trimmed})"))
    }
}

/// Fields the request capture adds while RUM is on: only the beacon's request ID, which the
/// lean RUM log format reads from an extracted header.
pub fn rum_custom_fields() -> Value {
    json!([
        {
            "name": "fastly_req_id",
            "label": "RUM Request ID",
            "description": "Per-request correlation key (minted at edge) for joining client-reported TTFB with server-side metrics.",
            "vcl_log_expression": "req.http.x-fos-edge-data:fastly_req_id",
            "collection_stage": "beacon",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 12,
            "enabled": true
        }
    ])
}

/// Edge fields of the RUM stream, by catalog id: exactly what `ingest::rum` reads besides the
/// beacon URL.
/// The beacon's client network, read at the edge so client timings group by ASN without a join.
const RUM_ASN_VALUE: &str = "%{if(client.as.number > 0, \"\" + client.as.number, \"null\")}V";
const RUM_EDGE_FIELDS: [&str; 6] = ["city", "region", "country", "pop", "tls", "ttfb"];

/// The lean RUM log format: the edge fields the RUM parser reads and the beacon URL (whose query
/// string carries every metric and error) once. A deliberate difference from v3, which logged
/// the request groups A/C/D/I, the query string three times and an always-empty request body.
pub fn generate_rum_log_format() -> Result<String, String> {
    let catalog_vcl = |id: &str| {
        crate::catalog::data()["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|field| field["id"] == id)
            .and_then(|field| field["vcl"].as_str())
            .map(str::to_owned)
            .ok_or_else(|| format!("catalog field {id} has no VCL"))
    };
    let mut parts = vec![
        catalog_vcl("ts_ms")?,
        "\"service_id\":\"%{req.service_id}V\"".to_owned(),
        "\"fastly_req_id\":\"%{json.escape(req.http.x-fos-edge-data:fastly_req_id)}V\"".to_owned(),
        "\"url\":\"%{json.escape(substr(req.url, 0, 8192))}V\"".to_owned(),
    ];
    for id in RUM_EDGE_FIELDS {
        parts.push(catalog_vcl(id)?);
    }
    parts.push(format!("\"asn\":{RUM_ASN_VALUE}"));
    Ok(format!("{{{}}}", parts.join(",")))
}

/// Compact positional JSON array format for the RUM stream (`[ts_ms, service_id, fastly_req_id,
/// url, city, region, country, pop, tls, ttfb]`), omitting repeated JSON field keys on every line.
pub fn generate_compact_rum_log_format() -> Result<String, String> {
    let catalog_val = |id: &str| {
        crate::catalog::data()["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|field| field["id"] == id)
            .and_then(|field| field["vcl"].as_str())
            .and_then(|vcl| vcl.split_once(':'))
            .map(|(_, val)| val.to_owned())
            .ok_or_else(|| format!("catalog field {id} has no VCL"))
    };
    let mut parts = vec![
        catalog_val("ts_ms")?,
        "\"%{req.service_id}V\"".to_owned(),
        "\"%{json.escape(req.http.x-fos-edge-data:fastly_req_id)}V\"".to_owned(),
        "\"%{json.escape(substr(req.url, 0, 8192))}V\"".to_owned(),
    ];
    for id in RUM_EDGE_FIELDS {
        parts.push(catalog_val(id)?);
    }
    parts.push(RUM_ASN_VALUE.to_owned());
    Ok(format!("[{}]", parts.join(",")))
}

pub fn beacon_field_extraction_vcl() -> &'static str {
    super::templates()["rum_beacon_extraction"]
        .as_str()
        .expect("rum_beacon_extraction template must exist in templates.json")
}

pub fn beacon_response_vcl() -> &'static str {
    super::templates()["rum_beacon_response"]
        .as_str()
        .expect("rum_beacon_response template must exist in templates.json")
}

pub fn asset_fetch_vcl(shield_pop: &str, faro_version: Option<&str>) -> Result<String, String> {
    let trimmed_shield = shield_pop.trim();
    let backend_str = if !trimmed_shield.is_empty() && !trimmed_shield.eq_ignore_ascii_case("none")
    {
        let valid_pop = validate_shield_pop(trimmed_shield)?;
        let shield_var = format!("ssl_shield_{}", valid_pop.replace('-', "_"));
        format!("fastly.try_select_shield({shield_var}, F_fos_origin)")
    } else {
        "F_fos_origin".to_string()
    };

    let path_condition = match faro_version.filter(|v| !v.is_empty()) {
        None => "req.url.path == \"/js/rum.js\"".to_string(),
        Some(ver) => {
            validate_faro_version(ver)?;
            "(req.url.path == \"/js/rum.js\" || req.url.path == \"/js/faro-sdk.js\")".to_string()
        }
    };

    let base_tpl = super::templates()["rum_asset_fetch"]
        .as_str()
        .expect("rum_asset_fetch template must exist in templates.json");
    Ok(base_tpl
        .replace(
            "(req.url.path == \"/js/rum.js\" || req.url.path == \"/js/faro-sdk.js\")",
            &path_condition,
        )
        .replace(
            "fastly.try_select_shield(ssl_shield_iad_va_us, F_fos_origin)",
            &backend_str,
        ))
}

pub fn sigv4_sign_vcl(faro_version: Option<&str>) -> Result<String, String> {
    let (path_condition, faro_rewrite) = match faro_version.filter(|v| !v.is_empty()) {
        None => ("req.url.path == \"/js/rum.js\"".to_string(), String::new()),
        Some(ver) => {
            validate_faro_version(ver)?;
            (
                "(req.url.path == \"/js/rum.js\" || req.url.path == \"/js/faro-sdk.js\")"
                    .to_string(),
                format!(
                    "\n\n    # If the user-facing path is /js/faro-sdk.js, point to the pinned Faro Web SDK object in FOS\n    if (bereq.url.path == \"/js/faro-sdk.js\") {{\n        set bereq.url = \"/rum/faro-web-sdk-v{ver}.iife.js\";\n    }}"
                ),
            )
        }
    };

    let base_tpl = super::templates()["rum_sigv4_sign"]
        .as_str()
        .expect("rum_sigv4_sign template must exist in templates.json");
    let default_faro_block = format!(
        "\n\n    # If the user-facing path is /js/faro-sdk.js, point to the pinned Faro Web SDK object in FOS\n    if (bereq.url.path == \"/js/faro-sdk.js\") {{\n        set bereq.url = \"/rum/faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js\";\n    }}"
    );
    Ok(base_tpl
        .replace(
            "(req.url.path == \"/js/rum.js\" || req.url.path == \"/js/faro-sdk.js\")",
            &path_condition,
        )
        .replace(&default_faro_block, &faro_rewrite))
}

pub fn fetch_caching_vcl(faro_version: Option<&str>) -> Result<String, String> {
    let mut body = "# Cache RUM tracker JS at the edge; browsers get a short TTL so a
# Surrogate-Key purge of \"rum-js\" becomes visible to clients quickly.
if (req.url.path == \"/js/rum.js\") {
    if (beresp.status == 200) {
        set beresp.ttl = 86400s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = \"rum-js\";
        set beresp.http.Surrogate-Control = \"max-age=86400\";
        set beresp.http.Cache-Control = \"public, max-age=300\";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"
    .to_string();

    if let Some(ver) = faro_version.filter(|v| !v.is_empty()) {
        validate_faro_version(ver)?;
        let faro_fetch = super::templates()["rum_faro_fetch"]
            .as_str()
            .expect("rum_faro_fetch template must exist in templates.json");
        body.push_str("\n\n");
        body.push_str(faro_fetch);
    }

    Ok(body)
}

pub fn error_beacon_vcl() -> &'static str {
    super::templates()["rum_error"]
        .as_str()
        .expect("rum_error template must exist in templates.json")
}

/// Tells the browser which edge request it just made (`Server-Timing: rid`), so the tracker can
/// report client time-to-last-byte against that request's log line.
pub fn request_timing_vcl() -> &'static str {
    super::templates()["rum_deliver_timing"]
        .as_str()
        .expect("rum_deliver_timing template must exist in templates.json")
}

/// Extends [`request_timing_vcl`] with `Timing-Allow-Origin` for the service's own hostnames.
pub fn request_timing_vcl_with_origins(origins: &[String]) -> Result<String, String> {
    let valid = validate_timing_allow_origins(origins)?;
    let mut out = request_timing_vcl().to_owned();
    if !valid.is_empty() {
        out.push_str(&format!(
            "\n# Without this the browser hides the request ID and transfer size of cross-host files.\n\
if (fastly.ff.visits_this_service == 0 && req.url.path != \"/rum-beacon\" && !resp.http.Timing-Allow-Origin) {{\n    \
set resp.http.Timing-Allow-Origin = \"{}\";\n}}",
            valid.join(", ")
        ));
    }
    Ok(out)
}

pub fn validate_service_id(service_id: &str) -> Result<(), String> {
    if service_id.is_empty()
        || service_id.len() > 64
        || !service_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err("invalid RUM service_id".into());
    }
    Ok(())
}

fn js_string_literal(value: &str) -> String {
    serde_json::to_string(value)
        .expect("string serialization cannot fail")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// The browser tracker. Faro collects; this wrapper drops Faro's own sends and batches every
/// metric and error of a pageview into one `POST /rum-beacon` per flush (debounced, or on
/// hide/pagehide via `sendBeacon`), split only when a URL would exceed `MAX_URL_CHARS`.
/// Sampling is decided once per browser session. Resource timing is sampled per page view and
/// read from the browser's own timing entries: only requests that carry this service's request
/// ID (`Server-Timing: rid`) and a non-zero transfer size, so browser-cache hits, third-party
/// files and the beacon itself are never reported. A page view reports up to 50 requests at once
/// and then one more every five seconds.
pub fn generate_rum_tracker_js(_service_id: &str, config: &Config) -> String {
    let beacon_endpoint_js = js_string_literal("/rum-beacon");
    let vitals = config.capture_vitals;
    let performance = config.capture_performance;
    let errors = config.capture_errors;
    let events = config.capture_events;
    let sample_rate = config.sample_rate;
    let error_sample_rate = config.error_sample_rate;
    let resources = config.capture_resources;
    let resource_sample_rate = config.resource_sample_rate;
    format!(
        r#"/* Faro Web SDK wrapper */
(function() {{
  var BEACON_ENDPOINT = {beacon_endpoint_js};
  var CAPTURE_VITALS = {vitals};
  var CAPTURE_PERFORMANCE = {performance};
  var CAPTURE_ERRORS = {errors};
  var CAPTURE_EVENTS = {events};
  var SAMPLE_RATE = {sample_rate};
  var ERROR_SAMPLE_RATE = {error_sample_rate};
  var CAPTURE_RESOURCES = {resources};
  var RESOURCE_SAMPLE_RATE = {resource_sample_rate};
  var MAX_RESOURCES = 50;
  var RESOURCE_REFILL_MS = 5000;
  var FLUSH_DELAY_MS = 1500;
  var MAX_URL_CHARS = 7500;

  var draw = NaN;
  try {{
    draw = parseFloat(window.sessionStorage.getItem('rum_sample'));
  }} catch (e) {{}}
  if (!(draw >= 0 && draw < 1)) {{
    draw = Math.random();
    try {{
      window.sessionStorage.setItem('rum_sample', String(draw));
    }} catch (e) {{}}
  }}
  var SAMPLED = draw < SAMPLE_RATE;
  var ERRORS_SAMPLED = CAPTURE_ERRORS && draw < ERROR_SAMPLE_RATE;
  var RESOURCES_SAMPLED = CAPTURE_RESOURCES && typeof PerformanceObserver === 'function' &&
    Math.random() < RESOURCE_SAMPLE_RATE;
  if (!SAMPLED && !ERRORS_SAMPLED && !RESOURCES_SAMPLED) return;

  var batch = {{ path: null, metrics: [], resources: [], errors: [] }};
  var flushTimer = null;

  function enc(value) {{
    return encodeURIComponent(String(value));
  }}

  function queueItem(item) {{
    if (!item || !item.payload) return;
    try {{
      var metrics = [];
      var error = null;
      if (item.type === 'measurement') {{
        if (!SAMPLED || !CAPTURE_VITALS) return;
        var m = item.payload;
        if (m.type === 'web-vitals') {{
          var values = m.values || {{}};
          var rating = (m.context && m.context.rating) || (m.meta && m.meta.rating) || '';
          Object.keys(values).forEach(function(k) {{
            if (k !== 'delta') {{
              metrics.push(enc(k) + ':' + enc(values[k]) + ':' + enc(rating));
            }}
          }});
        }}
      }} else if (item.type === 'event') {{
        if (!SAMPLED) return;
        var e = item.payload;
        if (e.name === 'faro.performance.navigation') {{
          if (!CAPTURE_PERFORMANCE) return;
          var attrs = e.attributes || e.values || {{}};
          Object.keys(attrs).forEach(function(k) {{
            metrics.push(enc(k) + ':' + enc(attrs[k]) + ':');
          }});
        }} else if (e.name && e.name.indexOf('faro.') !== 0) {{
          if (!CAPTURE_EVENTS) return;
          metrics.push(enc('event_' + e.name) + ':1:');
        }}
      }} else if (item.type === 'exception') {{
        if (!ERRORS_SAMPLED) return;
        var exc = item.payload;
        var message = exc.value || exc.message || '';
        if (!message) return;
        var frame = (exc.stacktrace && exc.stacktrace.frames && exc.stacktrace.frames[0]) || {{}};
        error = [message.slice(0, 500), frame.filename || '', frame.lineno || 0, frame.colno || 0]
          .map(enc).join('|');
      }}
      if (metrics.length === 0 && !error) return;
      var path = window.location.pathname;
      if (batch.path !== null && batch.path !== path) flushBatch(false);
      batch.path = path;
      batch.metrics = batch.metrics.concat(metrics);
      if (error) batch.errors.push(error);
      scheduleFlush();
    }} catch (e) {{
      console.error('RUM: failed to queue beacon item:', e);
    }}
  }}

  function queueTiming(metrics, resources) {{
    if (metrics.length === 0 && resources.length === 0) return;
    var path = window.location.pathname;
    if (batch.path !== null && batch.path !== path) flushBatch(false);
    batch.path = path;
    batch.metrics = batch.metrics.concat(metrics);
    batch.resources = batch.resources.concat(resources);
    scheduleFlush();
  }}

  // One `rid:path:ms` per request that reached the edge: request start to last byte.
  function resourceTiming(entry) {{
    if (!(entry.transferSize > 0) || !(entry.responseEnd > 0)) return null;
    var rid = '';
    (entry.serverTiming || []).forEach(function(t) {{
      if (t.name === 'rid' && t.description) rid = t.description;
    }});
    if (!rid) return null;
    var path;
    try {{
      path = new URL(entry.name, window.location.href).pathname;
    }} catch (e) {{
      return null;
    }}
    if (path === BEACON_ENDPOINT) return null;
    var ttlb = entry.responseEnd - (entry.requestStart > 0 ? entry.requestStart : entry.fetchStart || 0);
    if (!(ttlb >= 0)) return null;
    return enc(rid) + ':' + enc(path) + ':' + Math.round(ttlb);
  }}

  // A page view may report MAX_RESOURCES requests at once, then one more per RESOURCE_REFILL_MS,
  // so a long session (video) keeps measuring later downloads at a bounded rate.
  var resourceBudget = MAX_RESOURCES;
  var resourceBudgetAt = Date.now();
  function takeResourceBudget() {{
    var refill = Math.floor((Date.now() - resourceBudgetAt) / RESOURCE_REFILL_MS);
    if (refill > 0) {{
      resourceBudget = Math.min(MAX_RESOURCES, resourceBudget + refill);
      resourceBudgetAt += refill * RESOURCE_REFILL_MS;
    }}
    if (resourceBudget <= 0) return false;
    resourceBudget--;
    return true;
  }}

  function observeTiming(type) {{
    try {{
      new PerformanceObserver(function(list) {{
        try {{
          var metrics = [];
          var resources = [];
          list.getEntries().forEach(function(entry) {{
            if (type === 'navigation' && SAMPLED && CAPTURE_PERFORMANCE && entry.loadEventEnd > 0) {{
              metrics.push('page_load:' + Math.round(entry.loadEventEnd - (entry.startTime || 0)) + ':');
            }}
            if (!RESOURCES_SAMPLED) return;
            var timing = resourceTiming(entry);
            if (timing && takeResourceBudget()) resources.push(timing);
          }});
          queueTiming(metrics, resources);
        }} catch (e) {{
          console.error('RUM: failed to queue timing entries:', e);
        }}
      }}).observe({{ type: type, buffered: true }});
    }} catch (e) {{}}
  }}
  if (typeof PerformanceObserver === 'function') {{
    if (RESOURCES_SAMPLED || (SAMPLED && CAPTURE_PERFORMANCE)) observeTiming('navigation');
    if (RESOURCES_SAMPLED) observeTiming('resource');
  }}

  function scheduleFlush() {{
    if (flushTimer !== null) clearTimeout(flushTimer);
    var hidden = typeof document !== 'undefined' && document.visibilityState === 'hidden';
    flushTimer = setTimeout(function() {{
      flushTimer = null;
      flushBatch(hidden);
    }}, hidden ? 0 : FLUSH_DELAY_MS);
  }}

  function detectEnv() {{
    var ua = (typeof navigator !== 'undefined' && navigator.userAgent) || '';
    var ual = ua.toLowerCase();
    var uad = (typeof navigator !== 'undefined' && navigator.userAgentData) || null;
    function has(s) {{ return ua.indexOf(s) !== -1; }}
    function hasL(s) {{ return ual.indexOf(s) !== -1; }}
    var b = '';
    if (has('Edg/')) b = 'Edge';
    else if (has('Firefox/')) b = 'Firefox';
    else if (has('Chrome/') && !has('Edg/')) b = 'Chrome';
    else if (has('Safari/') && !has('Chrome/')) b = 'Safari';
    var o = '';
    if (has('iPhone') || has('iPad') || has('iPod')) o = 'iOS';
    else if (has('Android')) o = 'Android';
    else if (has('Mac OS X') || has('Macintosh') || (uad && uad.platform === 'macOS')) o = 'macOS';
    else if (has('Windows') || (uad && uad.platform === 'Windows')) o = 'Windows';
    else if (has('Linux') || (uad && uad.platform === 'Linux')) o = 'Linux';
    var d = (hasL('ipad') || hasL('tablet')) ? 'Tablet' : ((uad && uad.mobile) || hasL('mobi') || hasL('android') || hasL('iphone') ? 'Mobile' : (ua || uad ? 'Desktop' : ''));
    return {{ b: b, o: o, d: d }};
  }}

  function beaconUrls() {{
    var cid = '';
    var cookieMatch = document.cookie.match(/(?:^|; )rum_cid=([^;]+)/);
    if (cookieMatch) {{
      try {{
        cid = decodeURIComponent(cookieMatch[1]);
      }} catch (e) {{
        cid = cookieMatch[1];
      }}
    }}
    var env = detectEnv();
    var head = BEACON_ENDPOINT + '?' + (cid ? 'cid=' + enc(cid) + '&' : '') +
      'p=' + enc(batch.path) + '&sr=' + SAMPLE_RATE + '&esr=' + ERROR_SAMPLE_RATE +
      (RESOURCES_SAMPLED ? '&rsr=' + RESOURCE_SAMPLE_RATE : '') +
      (env.b ? '&b=' + enc(env.b) : '') +
      (env.o ? '&o=' + enc(env.o) : '') +
      (env.d ? '&d=' + enc(env.d) : '');
    var urls = [];
    var url = head;
    var openList = '';
    // `m` and `r` are comma-separated lists; every `e` is its own parameter.
    function append(part, key) {{
      var isList = key !== 'e';
      var piece = isList && openList === key ? ',' + part : '&' + key + '=' + part;
      if (url !== head && url.length + piece.length > MAX_URL_CHARS) {{
        urls.push(url);
        url = head;
        piece = '&' + key + '=' + part;
      }}
      url += piece;
      openList = isList ? key : '';
    }}
    batch.metrics.forEach(function(m) {{ append(m, 'm'); }});
    batch.resources.forEach(function(r) {{ append(r, 'r'); }});
    batch.errors.forEach(function(e) {{ append(e, 'e'); }});
    if (url !== head) urls.push(url);
    return urls;
  }}

  function flushBatch(useBeacon) {{
    if (flushTimer !== null) {{
      clearTimeout(flushTimer);
      flushTimer = null;
    }}
    if (batch.metrics.length === 0 && batch.resources.length === 0 && batch.errors.length === 0) return;
    var urls = beaconUrls();
    batch = {{ path: null, metrics: [], resources: [], errors: [] }};
    urls.forEach(function(url) {{
      if (useBeacon && originalSendBeacon) {{
        originalSendBeacon.call(navigator, url, '');
      }} else if (originalFetch) {{
        originalFetch.call(window, url, {{ method: 'POST', body: '', keepalive: true }})
          .catch(function() {{}});
      }}
    }});
  }}

  // Faro's own transport sends are dropped: the batch above carries the data.
  var originalSendBeacon = null;
  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {{
    originalSendBeacon = navigator.sendBeacon;
    navigator.sendBeacon = function(url, data) {{
      if (typeof url === 'string' && url.indexOf('/rum-beacon') !== -1) {{
        return true;
      }}
      return originalSendBeacon.call(this, url, data);
    }};
  }}

  var originalFetch = null;
  if (typeof window.fetch === 'function') {{
    originalFetch = window.fetch;
    window.fetch = function(input, init) {{
      if (typeof input === 'string' && input.indexOf('/rum-beacon') !== -1) {{
        return Promise.resolve(new Response(null, {{ status: 204, statusText: 'No Content' }}));
      }}
      return originalFetch.call(this, input, init);
    }};
  }}

  if (typeof XMLHttpRequest === 'function') {{
    var originalOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(method, url) {{
      if (typeof url === 'string' && url.indexOf('/rum-beacon') !== -1) {{
        this._rumBeacon = true;
      }}
      return originalOpen.apply(this, arguments);
    }};

    var originalSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function(body) {{
      if (this._rumBeacon) {{
        var self = this;
        setTimeout(function() {{
          Object.defineProperty(self, 'readyState', {{ value: 4, writable: true }});
          Object.defineProperty(self, 'status', {{ value: 204, writable: true }});
          if (typeof self.onreadystatechange === 'function') {{
            self.onreadystatechange();
          }}
          if (typeof self.onload === 'function') {{
            self.onload();
          }}
        }}, 0);
        return;
      }}
      return originalSend.apply(this, arguments);
    }};
  }}

  document.addEventListener('visibilitychange', function() {{
    if (document.visibilityState === 'hidden') flushBatch(true);
  }});
  window.addEventListener('pagehide', function() {{
    flushBatch(true);
  }});

  // Check if Faro is present (either via window.Faro or window.GrafanaFaroWebSdk)
  var Faro = window.Faro || window.GrafanaFaroWebSdk;

  if (!SAMPLED && !ERRORS_SAMPLED) {{
    // Resource timing alone reads the browser's own entries and needs no SDK.
  }} else if (typeof Faro === 'undefined') {{
    var script = document.createElement('script');
    script.src = '/js/faro-sdk.js';
    script.async = true;
    script.onload = function() {{
      Faro = window.Faro || window.GrafanaFaroWebSdk;
      initializeFaro();
    }};
    script.onerror = function() {{
      console.error('Failed to load Faro SDK');
    }};
    document.head.appendChild(script);
  }} else {{
    initializeFaro();
  }}

  function initializeFaro() {{
    if (typeof Faro === 'undefined' || typeof Faro.initializeFaro !== 'function') {{
      console.error('Faro SDK not available or initializeFaro not a function');
      return;
    }}

    var instrumentations = [];

    if (SAMPLED && Faro.WebVitalsInstrumentation) {{
      instrumentations.push(new Faro.WebVitalsInstrumentation());
    }}
    if (SAMPLED && Faro.ViewInstrumentation) {{
      instrumentations.push(new Faro.ViewInstrumentation());
    }}
    var ErrorInstrumentationCtor = Faro.ErrorsInstrumentation || Faro.ErrorInstrumentation;
    if (ErrorInstrumentationCtor) {{
      instrumentations.push(new ErrorInstrumentationCtor());
    }} else {{
      console.warn('Faro SDK: no error instrumentation export found (checked ErrorsInstrumentation, ErrorInstrumentation) — JS error tracking disabled');
    }}

    var config = {{
      app: {{
        name: 'rum-app',
        version: '1.0.0'
      }},
      url: BEACON_ENDPOINT,
      instrumentations: instrumentations,
      samplingRate: 1,
      batching: {{
        enabled: true,
        sendTimeout: 10,
        itemLimit: 1
      }},
      beforeSend: function(event) {{
        if (!event) return null;
        if (event.type) {{
          queueItem(event);
        }} else {{
          (event.measurements || []).forEach(function(payload) {{
            queueItem({{ type: 'measurement', payload: payload }});
          }});
          (event.events || []).forEach(function(payload) {{
            queueItem({{ type: 'event', payload: payload }});
          }});
          (event.exceptions || []).forEach(function(payload) {{
            queueItem({{ type: 'exception', payload: payload }});
          }});
        }}
        return null;
      }}
    }};

    try {{
      Faro.initializeFaro(config);
    }} catch (e) {{
      console.error('Failed to initialize Faro:', e);
    }}
  }}
}})();
"#
    )
}
