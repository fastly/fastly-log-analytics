"""RUM assets & Faro SDK management."""

import hashlib
import json
import logging
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from backend import config as svcconfig
from backend.core.faro_versions import fetch_faro_bundle
from backend.core.fastly.client import is_mock_mode, region_endpoint
from backend.utils.http import make_client

logger = logging.getLogger(__name__)


def sign_fos_request(
    method: str,
    url: str,
    headers: dict | None,
    body: bytes,
    access_key_id: str,
    secret_access_key: str,
    region: str = "us-east-1",
) -> dict[str, str]:
    """Sign a FOS request using AWS SigV4."""
    if headers is None:
        headers = {}

    if "Host" not in headers and "host" not in headers:
        headers["Host"] = urlparse(url).netloc

    credentials = Credentials(access_key_id, secret_access_key)
    aws_req = AWSRequest(method=method, url=url, headers=headers, data=body)
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers


# Faro bundles are stored per-version so a mid-upgrade failure never leaves
# the currently-served bundle half-overwritten; cleanup sweeps this prefix.
FARO_KEY_PREFIX = "rum/faro-web-sdk-v"
FARO_KEY_SUFFIX = ".iife.js"

_S3_LIST_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

# Sentinel distinguishing "caller passed no expectation" (write unconditionally
# — the default, used by explicit operator actions like enable_rum/
# upgrade_faro_version) from "caller expects the pin to still be None" (a
# real, meaningful expectation) in download_and_upload_faro's
# expected_current_version compare-and-set guard. `None` itself can't serve
# as that sentinel because it's also a legitimate expected value (an
# unpinned service).
_NO_EXPECTATION = object()


class _FaroPinChanged(Exception):
    """The persisted Faro pin no longer matches the version this sync started from."""

    def __init__(self, found: object) -> None:
        super().__init__(f"faro pin is now {found!r}")
        self.found = found


def _faro_fos_key(version: str) -> str:
    return f"{FARO_KEY_PREFIX}{version}{FARO_KEY_SUFFIX}"


def faro_bundle_intact(cfg: dict, pinned_version: str) -> bool:
    """Cheap SigV4 HEAD check: True only if FOS already holds the pinned
    bundle with an ETag matching the stored content hash.

    Any failure (missing FOS creds, no stored hash, 404/non-200, mismatched
    ETag, network error) returns False. This is the shared home for the
    check: the cron's every-tick integrity check
    (``backend/cron/jobs/rum_sync.py::_reconcile_faro_bundle``) and the
    tracker-publish readiness gate (``faro_tracker_ready`` below, used by
    ``upload_rum_tracker_js``) both need it, and it must live where neither
    ``backend/provision`` nor ``backend/cron`` imports the other — putting
    it in ``rum_assets.py`` (already imported by both) avoids a
    provision-to-cron import edge. Must stay cheap (no unpkg traffic) and
    must never raise.

    Compares against ``faro_fos_etag_md5`` (not ``faro_content_hash``): a
    single-part PUT's S3/FOS ``ETag`` is protocol-mandated MD5 of the
    object bytes, so this check needs an MD5 value specifically —
    independent of whatever algorithm ``faro_content_hash`` (the
    content-drift marker ``detect_faro_version_change`` reads) uses.
    """
    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")
    stored_hash = (cfg.get("rum") or {}).get("faro_fos_etag_md5")

    if not all([access_key, secret_key, bucket]) or not stored_hash:
        return False

    if access_key is None or secret_key is None:
        raise RuntimeError("access_key / secret_key is unexpectedly None")

    fos_key = f"{FARO_KEY_PREFIX}{pinned_version}{FARO_KEY_SUFFIX}"
    fos_host = region_endpoint(region)
    fos_url = f"https://{fos_host}/{bucket}/{fos_key}"

    try:
        headers = sign_fos_request(
            method="HEAD",
            url=fos_url,
            headers={},
            body=b"",
            access_key_id=access_key,
            secret_access_key=secret_key,
            region=region,
        )
        with make_client() as client:
            try:
                response = client.head(fos_url, headers=headers, timeout=10.0)
            except httpx.TimeoutException:
                response = client.head(fos_url, headers=headers, timeout=10.0)
        if response.status_code != 200:
            return False
        etag = response.headers.get("ETag", "").strip('"')
        return etag == stored_hash
    except Exception as e:
        logger.warning(
            "Faro FOS integrity HEAD check failed, treating as needs-restore: %s: %s",
            type(e).__name__,
            e,
        )
        return False


def faro_tracker_ready(cfg: dict) -> tuple[bool, str]:
    """True only if the faro-referencing tracker is safe to publish.

    ``generate_rum_tracker_js`` references ``/js/faro-sdk.js``
    unconditionally, but that route only serves real content once (1) a
    Faro version is pinned in config and (2) that version's bundle actually
    exists in FOS. Activation succeeding is NOT sufficient on its own — an
    activation can complete with no faro route at all if ``faro_version``
    was never pinned. Publishing the tracker without both conditions
    strands it pointing at a route that falls through to the origin's
    generic 2-byte "OK" response, so Faro never initializes and no beacons
    are collected — this is the exact live-production failure this check
    exists to prevent.

    Returns ``(ready, reason)``: ``reason`` is a human-readable explanation
    when not ready, empty string when ready.
    """
    rum_cfg = cfg.get("rum") or {}
    pinned_version = rum_cfg.get("faro_version")
    if not pinned_version:
        return False, "no Faro version pinned (cfg['rum']['faro_version'] is unset)"
    if not faro_bundle_intact(cfg, pinned_version):
        return False, f"Faro bundle v{pinned_version} is not present/intact in FOS"
    return True, ""


def _js_json_literal(value: str) -> str:
    """Serialize *value* as an HTML-safe JS string literal, byte-identical to the Cloud generator."""
    return (
        json.dumps(str(value), ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


DEFAULT_RESOURCE_SAMPLE_RATE = 0.1
MIN_RUM_SAMPLE_RATE = 0.01


def _js_rate_literal(value: object, *, default: float) -> str:
    """Coerce *value* to a JS number literal in [0.01, 1]; anything else becomes ``default``."""
    rate = default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        candidate = float(value)
        if MIN_RUM_SAMPLE_RATE <= candidate <= 1.0:
            rate = candidate
    text = repr(rate)
    return text[:-2] if text.endswith(".0") else text


def _js_bool_literal(value: object, *, default: bool = True) -> str:
    """Coerce *value* strictly to the JS boolean literal ``'true'`` or ``'false'``."""
    if value is None:
        return "true" if default else "false"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "false" if value.strip().lower() in {"false", "0", "no", "off", ""} else "true"
    return "true" if bool(value) else "false"


def generate_rum_tracker_js(service_id: str, beacon_endpoint: str = "/rum-beacon") -> str:
    """Generate the Faro Web SDK wrapper JS.

    This creates a minimal loader that:
    1. Loads the self-hosted Faro Web SDK bundle from this service's own
       domain — always the relative, first-party ``/js/faro-sdk.js``, never
       a third-party CDN. That path is served from FOS via the RUM asset
       fetch VCL (backend/core/fastly/client.py), which routes it
       to the pinned bundle uploaded by ``download_and_upload_faro``.
    2. Initializes it with the service's beacon endpoint
    3. Sends beacons back to the origin

    The beacon endpoint defaults to /rum-beacon
    but can be customized if needed.

    This body is identical regardless of which Faro version is pinned (the
    URL is always the constant "/js/faro-sdk.js"), so it never needs
    re-uploading on a version bump — only once, the first time RUM is
    enabled for a service, which ``upload_rum_tracker_js``'s MD5-vs-ETag
    check already handles idempotently.
    """
    beacon_url = str(beacon_endpoint).replace("{service_id}", str(service_id))

    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    capture_vitals_js = _js_bool_literal(rum_cfg.get("capture_vitals", True), default=True)
    capture_performance_js = _js_bool_literal(rum_cfg.get("capture_performance", True), default=True)
    capture_errors_js = _js_bool_literal(rum_cfg.get("capture_errors", True), default=True)
    capture_events_js = _js_bool_literal(rum_cfg.get("capture_events", True), default=True)
    capture_resources_js = _js_bool_literal(rum_cfg.get("capture_resources", True), default=True)
    sample_rate_js = _js_rate_literal(rum_cfg.get("sample_rate"), default=1.0)
    error_sample_rate_js = _js_rate_literal(rum_cfg.get("error_sample_rate"), default=1.0)
    resource_sample_rate_js = _js_rate_literal(
        rum_cfg.get("resource_sample_rate"), default=DEFAULT_RESOURCE_SAMPLE_RATE
    )

    service_id_js = _js_json_literal(service_id)
    beacon_url_js = _js_json_literal(beacon_url)

    return f"""/* Faro Web SDK wrapper */
(function() {{
  var SERVICE_ID = {service_id_js};
  var BEACON_ENDPOINT = {beacon_url_js};
  var CAPTURE_VITALS = {capture_vitals_js};
  var CAPTURE_PERFORMANCE = {capture_performance_js};
  var CAPTURE_ERRORS = {capture_errors_js};
  var CAPTURE_EVENTS = {capture_events_js};
  var SAMPLE_RATE = {sample_rate_js};
  var ERROR_SAMPLE_RATE = {error_sample_rate_js};
  var CAPTURE_RESOURCES = {capture_resources_js};
  var RESOURCE_SAMPLE_RATE = {resource_sample_rate_js};
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
    var head = BEACON_ENDPOINT + '?service_id=' + enc(SERVICE_ID) + (cid ? '&cid=' + enc(cid) : '') +
      '&p=' + enc(batch.path) + '&sr=' + SAMPLE_RATE + '&esr=' + ERROR_SAMPLE_RATE +
      (RESOURCES_SAMPLED ? '&rsr=' + RESOURCE_SAMPLE_RATE : '');
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
      url: BEACON_ENDPOINT + '?service_id=' + enc(SERVICE_ID),
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
"""


def upload_rum_tracker_js(
    service_id: str,
    token: str,
    *,
    overwrite: bool = True,
    status_cb=None,
) -> dict[str, Any]:
    """Generate and upload rum-tracker.js to the service's FOS bucket.

    Gated on ``faro_tracker_ready``: the tracker unconditionally references
    ``/js/faro-sdk.js``, so publishing it before a Faro version is pinned
    AND its bundle genuinely exists in FOS would strand it pointing at a
    dead route (no CDN fallback exists by design). When not ready, this is
    a deliberate no-op — whatever tracker is already in FOS (or none, for a
    brand-new service) is left untouched, which is strictly better than
    publishing one that can never initialize Faro.

    Returns:
        {
            "path": "rum/rum-tracker.js",
            "bytes_uploaded": int,
            "fos_key": str (s3://bucket/rum/rum-tracker.js),
            "skipped": bool (present and True only when readiness failed),
        }
    """
    from backend.provision.cli import info, ok, warn

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"No config for service {service_id}")

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")

    if not all([access_key, secret_key, bucket]):
        raise RuntimeError(f"Service {service_id} missing FOS credentials (access_key, secret_key, bucket)")

    if access_key is None or secret_key is None:
        raise RuntimeError("access_key / secret_key is unexpectedly None")

    fos_key = "rum/rum-tracker.js"
    fos_host = region_endpoint(region)
    fos_url = f"https://{fos_host}/{bucket}/{fos_key}"

    ready, reason = faro_tracker_ready(cfg)
    if not ready:
        msg = (
            f"Skipping RUM tracker JS upload for {service_id}: {reason} — "
            "publishing now would strand the tracker at a route with no bundle behind it"
        )
        warn(msg)
        if status_cb:
            status_cb(f"⚠️  Skipping RUM tracker JS upload: {reason}")
        return {
            "path": fos_key,
            "bytes_uploaded": 0,
            "fos_key": f"s3://{bucket}/{fos_key}",
            "skipped": True,
        }

    # Generate the JS file first so we can check if it differs
    js_content = generate_rum_tracker_js(service_id)
    js_bytes = js_content.encode("utf-8")

    # Compared against the FOS/S3 ETag header below, which is protocol-
    # mandated MD5 of the object bytes for a single-part PUT — not a
    # security use of the hash, just matching S3's ETag semantics.
    local_md5 = hashlib.md5(js_bytes, usedforsecurity=False).hexdigest()

    if not (is_mock_mode() and bucket == "mock_bucket"):
        try:
            # Check if file already exists and is up to date via HEAD request
            with make_client() as client:
                head_headers = sign_fos_request(
                    method="HEAD",
                    url=fos_url,
                    headers={},
                    body=b"",
                    access_key_id=access_key,
                    secret_access_key=secret_key,
                    region=region,
                )
                head_response = client.head(fos_url, headers=head_headers, timeout=10.0)
                if head_response.status_code == 200:
                    remote_etag = head_response.headers.get("ETag", "").strip('"')
                    if remote_etag == local_md5:
                        ok(f"RUM tracker JS already present and up to date in {fos_key} (MD5 matches)")
                        if status_cb:
                            status_cb("✅ RUM tracker JS already present and up to date")
                        return {
                            "path": fos_key,
                            "bytes_uploaded": 0,
                            "fos_key": f"s3://{bucket}/{fos_key}",
                        }
                    else:
                        info(
                            f"RUM tracker JS out of date in {fos_key} (local MD5 {local_md5} != remote ETag {remote_etag})"
                        )
                        if status_cb:
                            status_cb("🔄 RUM tracker JS has changed, updating...")
        except Exception:
            # HEAD failed (file doesn't exist or network issue) — proceed to upload
            pass

    info(f"Uploading RUM tracker JS to FOS bucket {bucket}")
    if status_cb:
        status_cb("⏳ Uploading RUM tracker JS…")

    # Build headers with SigV4 signing
    headers = {
        "Content-Type": "application/javascript",
        "Content-Length": str(len(js_bytes)),
    }

    try:
        if is_mock_mode() and bucket == "mock_bucket":
            ok(f"[MOCK] Mock uploaded RUM tracker JS ({len(js_bytes)} bytes) to {fos_key}")
            if status_cb:
                status_cb(f"✅ RUM tracker JS uploaded ({len(js_bytes)} bytes)")
        else:
            # Sign the request
            signed_headers = sign_fos_request(
                method="PUT",
                url=fos_url,
                headers=headers,
                body=js_bytes,
                access_key_id=access_key,
                secret_access_key=secret_key,
                region=region,
            )

            # Upload to FOS
            with make_client() as client:
                response = client.put(
                    fos_url,
                    content=js_bytes,
                    headers=signed_headers,
                    timeout=30.0,
                )
                response.raise_for_status()

            ok(f"Uploaded {len(js_bytes)} bytes to {fos_key}")
            if status_cb:
                status_cb(f"✅ RUM tracker JS uploaded ({len(js_bytes)} bytes)")

        return {
            "path": fos_key,
            "bytes_uploaded": len(js_bytes),
            "fos_key": f"s3://{bucket}/{fos_key}",
        }

    except Exception as exc:
        fail_msg = f"Failed to upload RUM tracker JS: {exc}"
        warn(fail_msg)
        if status_cb:
            status_cb(f"❌ {fail_msg}")
        raise RuntimeError(fail_msg) from exc


def delete_rum_tracker_js(
    service_id: str,
    token: str,
    *,
    status_cb=None,
) -> None:
    """Delete rum-tracker.js from FOS bucket (cleanup on disable)."""
    from backend.provision.cli import info, ok, warn

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        warn(f"No config for service {service_id} — skipping JS cleanup")
        return

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")

    if not all([access_key, secret_key, bucket]):
        warn(f"Service {service_id} missing FOS credentials — skipping JS cleanup")
        return

    if access_key is None or secret_key is None:
        raise RuntimeError("access_key / secret_key is unexpectedly None")

    info(f"Deleting RUM tracker JS from FOS bucket {bucket}")
    if status_cb:
        status_cb("⏳ Deleting RUM tracker JS…")

    fos_key = "rum/rum-tracker.js"
    fos_host = region_endpoint(region)
    fos_url = f"https://{fos_host}/{bucket}/{fos_key}"

    try:
        # Sign and delete
        headers: dict[str, str] = {}
        signed_headers = sign_fos_request(
            method="DELETE",
            url=fos_url,
            headers=headers,
            body=b"",
            access_key_id=access_key,
            secret_access_key=secret_key,
            region=region,
        )

        with make_client() as client:
            response = client.delete(
                fos_url,
                headers=signed_headers,
                timeout=30.0,
            )
            # 204 (deleted) and 404 (already absent) are both success
            if response.status_code not in (204, 404):
                response.raise_for_status()

        ok(f"Deleted {fos_key} from FOS")
        if status_cb:
            status_cb("✅ RUM tracker JS deleted")

    except Exception as exc:
        warn(f"Failed to delete RUM tracker JS: {exc}")
        if status_cb:
            status_cb(f"⚠️  Could not delete JS: {exc}")


async def download_and_upload_faro(
    service_id: str,
    version: str,
    token: str,
    *,
    status_cb=None,
    expected_current_version: Any = _NO_EXPECTATION,
) -> dict[str, Any]:
    """Download a pinned Faro Web SDK version and upload it to the service's FOS bucket.

    Persists two separate hashes to ``cfg["rum"]``, each read by a different
    consumer:
      - ``faro_content_hash`` (sha256): our own content-drift marker, read
        back by ``detect_faro_version_change`` to decide whether upstream
        re-released this version string with different bytes.
      - ``faro_fos_etag_md5`` (MD5): read by ``faro_bundle_intact`` above,
        used both by the cron's cheap FOS HEAD integrity check
        (``backend/cron/jobs/rum_sync.py::_reconcile_faro_bundle``) and by
        ``faro_tracker_ready``'s publish-readiness gate. It compares this
        hash against the object's S3/FOS ``ETag`` header — a
        single-part PUT's ETag is protocol-mandated MD5 of the bytes, so
        that comparison needs an MD5 specifically, independent of whichever
        algorithm ``faro_content_hash`` uses.

    ``expected_current_version`` guards against a cron/upgrade race (F-5
    audit finding): the RUM sync cron calls this to *restore* or *re-sync*
    whatever version is CURRENTLY pinned — never to change the pin — but the
    download from unpkg + upload to FOS between reading that pinned version
    and persisting the config below can take long enough for an operator's
    concurrent ``upgrade_faro_version`` call to land in between. Without a
    guard, the cron's write lands last and reverts ``cfg["rum"]["faro_version"]``
    back to the version it originally read — which then makes
    ``cleanup_old_faro_versions(keep_current=True)`` (run right after a
    successful upgrade) compute its "keep" key from the reverted, stale
    version and delete the FOS object the live VCL actually routes to.
    Pass the version the caller observed before starting this (possibly
    slow) download+upload; immediately before persisting, config is
    reloaded fresh and the write is skipped (bytes are still uploaded to
    this version's own FOS key — that part is unaffected) if the stored pin
    no longer matches. Left at its default (no expectation) — the
    unconditional, "this IS the new pin" behavior — for explicit operator
    actions (``enable_rum``, ``upgrade_faro_version``), which must always win.

    Returns:
        {
            "version": str,
            "path": "rum/faro-web-sdk-v{version}.iife.js",
            "bytes_uploaded": int,
            "content_hash": str (sha256 hex digest),
            "fos_key": str (s3://bucket/path),
            "config_updated": bool (False only when the compare-and-set
                guard above skipped the config write),
        }
    """
    from backend.provision.cli import info, ok, warn

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"No config for service {service_id}")

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")

    if not all([access_key, secret_key, bucket]):
        raise RuntimeError(f"Service {service_id} missing FOS credentials (access_key, secret_key, bucket)")

    if access_key is None or secret_key is None:
        raise RuntimeError("access_key / secret_key is unexpectedly None")

    info(f"Downloading Faro Web SDK v{version}")
    if status_cb:
        status_cb(f"⏳ Downloading Faro Web SDK v{version}…")

    bundle = await fetch_faro_bundle(version)
    content_hash = hashlib.sha256(bundle).hexdigest()
    # S3/FOS ETag for a single-part PUT is protocol-mandated MD5 of the
    # object bytes — stored separately (not a security use of the hash)
    # so the cron's ETag-based integrity check can keep comparing
    # like-for-like regardless of the algorithm above.
    etag_md5 = hashlib.md5(bundle, usedforsecurity=False).hexdigest()

    fos_key = _faro_fos_key(version)
    fos_host = region_endpoint(region)
    fos_url = f"https://{fos_host}/{bucket}/{fos_key}"

    headers = {
        "Content-Type": "application/javascript",
        "Content-Length": str(len(bundle)),
    }

    info(f"Uploading Faro Web SDK v{version} to FOS bucket {bucket}")
    if status_cb:
        status_cb(f"⏳ Uploading Faro Web SDK v{version}…")

    try:
        if is_mock_mode() and bucket == "mock_bucket":
            ok(f"[MOCK] Mock uploaded Faro Web SDK v{version} ({len(bundle)} bytes) to {fos_key}")
            if status_cb:
                status_cb(f"✅ Faro Web SDK v{version} uploaded ({len(bundle)} bytes)")
        else:
            signed_headers = sign_fos_request(
                method="PUT",
                url=fos_url,
                headers=headers,
                body=bundle,
                access_key_id=access_key,
                secret_access_key=secret_key,
                region=region,
            )

            with make_client() as client:
                response = client.put(
                    fos_url,
                    content=bundle,
                    headers=signed_headers,
                    timeout=30.0,
                )
                response.raise_for_status()

            ok(f"Uploaded Faro Web SDK v{version} ({len(bundle)} bytes) to {fos_key}")
            if status_cb:
                status_cb(f"✅ Faro Web SDK v{version} uploaded ({len(bundle)} bytes)")

    except Exception as exc:
        fail_msg = f"Failed to upload Faro Web SDK v{version}: {exc}"
        warn(fail_msg)
        if status_cb:
            status_cb(f"❌ {fail_msg}")
        raise RuntimeError(fail_msg) from exc

    # Reload fresh immediately before persisting (F-5): the download+upload
    # above may have taken long enough for a concurrent, explicit pin change
    # to land. See the expected_current_version note in this function's
    # docstring.
    def _commit_pin(target_cfg: dict) -> None:
        rum_cfg = dict(target_cfg.get("rum") or {})
        if expected_current_version is not _NO_EXPECTATION and rum_cfg.get("faro_version") != expected_current_version:
            raise _FaroPinChanged(rum_cfg.get("faro_version"))
        rum_cfg["faro_version"] = version
        rum_cfg["faro_content_hash"] = content_hash
        rum_cfg["faro_fos_etag_md5"] = etag_md5
        target_cfg["rum"] = rum_cfg

    try:
        # Compare-and-set under the config lock: the pin check and the write see the same config.
        if svcconfig.mutate_config(service_id, _commit_pin) is None:
            # The service was deleted mid-sync; recreating its config from the stale copy would resurrect it.
            warn(f"Service {service_id} no longer has a config; Faro v{version} uploaded but pin not persisted")
    except _FaroPinChanged as changed:
        warn(
            f"Faro pin for {service_id} changed concurrently while syncing v{version} "
            f"(expected {expected_current_version!r}, found {changed.found!r}); "
            "bundle bytes uploaded to their own FOS key but config left untouched"
        )
        if status_cb:
            status_cb(f"⚠️  Faro pin changed concurrently; v{version} uploaded but config not overwritten")
        return {
            "version": version,
            "path": fos_key,
            "bytes_uploaded": len(bundle),
            "content_hash": content_hash,
            "fos_key": f"s3://{bucket}/{fos_key}",
            "config_updated": False,
        }

    return {
        "version": version,
        "path": fos_key,
        "bytes_uploaded": len(bundle),
        "content_hash": content_hash,
        "fos_key": f"s3://{bucket}/{fos_key}",
        "config_updated": True,
    }


async def detect_faro_version_change(service_id: str, version: str) -> bool:
    """True if ``version`` (or its upstream bundle content) differs from what's stored.

    Covers three drift cases: never provisioned, an explicit version bump,
    and an upstream re-release of the *same* version string (security patch
    pushed without a version bump) — the last one requires re-downloading
    the bundle to compare hashes.
    """
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum")
    if not rum_cfg:
        return True

    stored_version = rum_cfg.get("faro_version")
    stored_hash = rum_cfg.get("faro_content_hash")

    if stored_version != version or not stored_hash:
        return True

    bundle = await fetch_faro_bundle(version)
    current_hash = hashlib.sha256(bundle).hexdigest()
    return current_hash != stored_hash


async def cleanup_old_faro_versions(service_id: str, *, keep_current: bool = True) -> None:
    """Delete stale ``rum/faro-web-sdk-v*.iife.js`` objects from FOS.

    Best-effort: this runs after a successful upgrade, so a cleanup failure
    (listing error, transient FOS blip) must never surface to the caller —
    it would otherwise turn a successful upgrade into a reported failure.
    """
    from backend.provision.cli import info, ok, warn

    try:
        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return

        access_key = cfg.get("fos_access_key_id")
        secret_key = cfg.get("fos_secret_access_key")
        bucket = cfg.get("fos_bucket")
        region = cfg.get("fos_region", "us-east-1")

        if not all([access_key, secret_key, bucket]):
            return

        if access_key is None or secret_key is None:
            raise RuntimeError("access_key / secret_key is unexpectedly None")

        keep_version = (cfg.get("rum") or {}).get("faro_version") if keep_current else None
        keep_key = _faro_fos_key(keep_version) if keep_version else None

        fos_host = region_endpoint(region)
        list_url = f"https://{fos_host}/{bucket}?list-type=2&prefix={quote(FARO_KEY_PREFIX, safe='')}"

        with make_client() as client:
            list_headers = sign_fos_request(
                method="GET",
                url=list_url,
                headers={},
                body=b"",
                access_key_id=access_key,
                secret_access_key=secret_key,
                region=region,
            )
            list_response = client.get(list_url, headers=list_headers, timeout=10.0)
            list_response.raise_for_status()

            root = ET.fromstring(list_response.content)
            keys = [
                el.text for el in root.findall(f".//{{{_S3_LIST_XMLNS}}}Contents/{{{_S3_LIST_XMLNS}}}Key") if el.text
            ]

            for key in keys:
                if key == keep_key:
                    continue

                obj_url = f"https://{fos_host}/{bucket}/{key}"
                del_headers = sign_fos_request(
                    method="DELETE",
                    url=obj_url,
                    headers={},
                    body=b"",
                    access_key_id=access_key,
                    secret_access_key=secret_key,
                    region=region,
                )
                del_response = client.delete(obj_url, headers=del_headers, timeout=10.0)
                if del_response.status_code not in (204, 404):
                    del_response.raise_for_status()
                info(f"Deleted old Faro bundle {key}")

        ok("Cleaned up old Faro Web SDK versions")

    except Exception as exc:
        warn(f"Failed to clean up old Faro versions: {exc}")
