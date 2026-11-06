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

pub fn validate_custom_condition(condition: &str) -> Result<(), String> {
    let trimmed = condition.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if trimmed.len() > 2048 {
        return Err("custom_condition exceeds maximum length of 2048 characters".into());
    }
    if trimmed.contains(['\n', '\r', ';', '{', '}']) {
        return Err("custom_condition contains forbidden characters".into());
    }
    if trimmed.contains('\'') {
        return Err("VCL string literals use double quotes, not single quotes.".into());
    }
    // Check for req.url or req.url.path predicate
    if contains_req_url_predicate(trimmed) {
        return Err(
            "A condition on req.url / req.url.path cannot work here: it is AND-ed onto req.url.path == \"/rum-beacon\", so the path is always \"/rum-beacon\". Such a condition is either a no-op or silently disables all RUM logging. To exclude beacons by page URL, filter in the tracker — the page URL is in the beacon's POST body, which VCL cannot read.".into()
        );
    }
    let mut depth: i32 = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    for ch in trimmed.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_quotes = !in_quotes;
            continue;
        }
        if !in_quotes {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth < 0 {
                    return Err("custom_condition has unbalanced parentheses".into());
                }
            }
        }
    }
    if in_quotes || depth != 0 {
        return Err("custom_condition has unbalanced quotes or parentheses".into());
    }
    Ok(())
}

fn contains_req_url_predicate(s: &str) -> bool {
    let bytes = s.as_bytes();
    let needle = b"req.url";
    let mut idx = 0;
    while idx + needle.len() <= bytes.len() {
        if let Some(pos) = s[idx..].find("req.url") {
            let start = idx + pos;
            let before_ok = start == 0
                || !(bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
            let end = start + needle.len();
            let rest = &s[end..];
            let after_end = if let Some(stripped) = rest.strip_prefix(".path") {
                end + 5 + (rest.len() - stripped.len() - 5)
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
    if ![config.sample_rate, config.error_sample_rate]
        .iter()
        .all(|rate| (0.01..=1.0).contains(rate))
    {
        return Err("rum sample rates must be in range [0.01, 1]".into());
    }
    validate_faro_version(&config.faro_version)?;
    if !SUPPORTED_FARO_VERSIONS.contains(&config.faro_version.as_str()) {
        return Err("rum.faro_version is unsupported".into());
    }
    validate_custom_condition(&config.custom_condition)
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
    let backend_str = if !shield_pop.is_empty() && !shield_pop.eq_ignore_ascii_case("none") {
        let shield_var = format!("ssl_shield_{}", shield_pop.replace('-', "_"));
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
    let faro_rewrite = match faro_version.filter(|v| !v.is_empty()) {
        None => String::new(),
        Some(ver) => {
            validate_faro_version(ver)?;
            format!(
                "\n\n    # If the user-facing path is /js/faro-sdk.js, point to the pinned Faro Web SDK object in FOS\n    if (bereq.url.path == \"/js/faro-sdk.js\") {{\n        set bereq.url = \"/rum/faro-web-sdk-v{ver}.iife.js\";\n    }}"
            )
        }
    };

    let base_tpl = super::templates()["rum_sigv4_sign"]
        .as_str()
        .expect("rum_sigv4_sign template must exist in templates.json");
    let default_faro_block = format!(
        "\n\n    # If the user-facing path is /js/faro-sdk.js, point to the pinned Faro Web SDK object in FOS\n    if (bereq.url.path == \"/js/faro-sdk.js\") {{\n        set bereq.url = \"/rum/faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js\";\n    }}"
    );
    Ok(base_tpl.replace(&default_faro_block, &faro_rewrite))
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

/// The browser tracker. Faro collects; this wrapper drops Faro's own sends and batches every
/// metric and error of a pageview into one `POST /rum-beacon` per flush (debounced, or on
/// hide/pagehide via `sendBeacon`), split only when a URL would exceed `MAX_URL_CHARS`.
/// Sampling is decided once per browser session.
pub fn generate_rum_tracker_js(service_id: &str, config: &Config) -> String {
    let vitals = config.capture_vitals;
    let performance = config.capture_performance;
    let errors = config.capture_errors;
    let events = config.capture_events;
    let sample_rate = config.sample_rate;
    let error_sample_rate = config.error_sample_rate;
    format!(
        r#"/* Faro Web SDK wrapper for service {service_id} */
(function() {{
  var SERVICE_ID = '{service_id}';
  var BEACON_ENDPOINT = '/rum-beacon';
  var CAPTURE_VITALS = {vitals};
  var CAPTURE_PERFORMANCE = {performance};
  var CAPTURE_ERRORS = {errors};
  var CAPTURE_EVENTS = {events};
  var SAMPLE_RATE = {sample_rate};
  var ERROR_SAMPLE_RATE = {error_sample_rate};
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
  if (!SAMPLED && !ERRORS_SAMPLED) return;

  var batch = {{ path: null, metrics: [], errors: [] }};
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

  function scheduleFlush() {{
    if (flushTimer !== null) clearTimeout(flushTimer);
    var hidden = typeof document !== 'undefined' && document.visibilityState === 'hidden';
    flushTimer = setTimeout(function() {{
      flushTimer = null;
      flushBatch(hidden);
    }}, hidden ? 0 : FLUSH_DELAY_MS);
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
    var head = BEACON_ENDPOINT + '?service_id=' + SERVICE_ID + (cid ? '&cid=' + enc(cid) : '') +
      '&p=' + enc(batch.path) + '&sr=' + SAMPLE_RATE + '&esr=' + ERROR_SAMPLE_RATE;
    var urls = [];
    var url = head;
    var openMetrics = false;
    function append(part, isMetric) {{
      var piece = isMetric && openMetrics ? ',' + part : (isMetric ? '&m=' : '&e=') + part;
      if (url !== head && url.length + piece.length > MAX_URL_CHARS) {{
        urls.push(url);
        url = head;
        openMetrics = false;
        piece = (isMetric ? '&m=' : '&e=') + part;
      }}
      url += piece;
      openMetrics = isMetric;
    }}
    batch.metrics.forEach(function(m) {{ append(m, true); }});
    batch.errors.forEach(function(e) {{ append(e, false); }});
    if (url !== head) urls.push(url);
    return urls;
  }}

  function flushBatch(useBeacon) {{
    if (flushTimer !== null) {{
      clearTimeout(flushTimer);
      flushTimer = null;
    }}
    if (batch.metrics.length === 0 && batch.errors.length === 0) return;
    var urls = beaconUrls();
    batch = {{ path: null, metrics: [], errors: [] }};
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

  if (typeof Faro === 'undefined') {{
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
      url: BEACON_ENDPOINT + '?service_id=' + SERVICE_ID,
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
