"""RUM asset generation and FOS upload (JS tracker, etc.)."""

from __future__ import annotations

import hashlib
import logging
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote

import certifi
import httpx

from backend import config as svcconfig
from backend.core.faro_versions import fetch_faro_bundle
from backend.core.fastly.utils import region_endpoint
from backend.utils.fos_signing import sign_fos_request

logger = logging.getLogger(__name__)

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

    assert access_key is not None and secret_key is not None

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
        with httpx.Client(verify=certifi.where()) as client:
            response = client.head(fos_url, headers=headers, timeout=10.0)
        if response.status_code != 200:
            return False
        etag = response.headers.get("ETag", "").strip('"')
        return etag == stored_hash
    except Exception:
        logger.warning("Faro FOS integrity HEAD check failed (treating as needs-restore)", exc_info=True)
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


def generate_rum_tracker_js(service_id: str, beacon_endpoint: str = "/rum-beacon") -> str:
    """Generate the Faro Web SDK wrapper JS.

    This creates a minimal loader that:
    1. Loads the self-hosted Faro Web SDK bundle from this service's own
       domain — always the relative, first-party ``/js/faro-sdk.js``, never
       a third-party CDN. That path is served from FOS via the RUM asset
       fetch VCL (backend/core/fastly/rum_provisioning.py), which routes it
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
    beacon_url = beacon_endpoint.format(service_id=service_id)

    return f"""/* Faro Web SDK wrapper for service {service_id} */
(function() {{
  var SERVICE_ID = '{service_id}';
  var BEACON_ENDPOINT = '{beacon_url}';

  // Monkeypatch fetch and XMLHttpRequest to enrich RUM beacon query parameters from JSON bodies.
  // This allows edge VCL (which cannot read request bodies) to capture and log critical metrics.
  function enrichRumUrl(url, bodyStr) {{
    if (typeof url !== 'string' || url.indexOf('/rum-beacon') === -1 || !bodyStr) {{
      return url;
    }}
    try {{
      var payload = JSON.parse(bodyStr);
      var metricName = '';
      var metricValue = '';
      var metricRating = '';

      // 1. Extract Web Vitals measurement
      if (payload.measurements && Array.isArray(payload.measurements)) {{
        var webVitals = payload.measurements.filter(function(m) {{ return m.type === 'web-vitals'; }});
        if (webVitals.length > 0) {{
          var values = webVitals[0].values || {{}};
          metricName = Object.keys(values)[0] || '';
          metricValue = values[metricName] || '';
          metricRating = (webVitals[0].meta && webVitals[0].meta.rating) || '';
        }}
      }}

      // 2. Extract Faro performance navigation timing
      if (!metricName && payload.events && Array.isArray(payload.events)) {{
        var navs = payload.events.filter(function(e) {{ return e.name === 'faro.performance.navigation'; }});
        if (navs.length > 0) {{
          var attrs = navs[0].attributes || navs[0].values || {{}};
          var plt = attrs.pageLoadTime !== undefined ? attrs.pageLoadTime : attrs.duration;
          if (plt !== undefined) {{
            metricName = 'pageLoadTime';
            metricValue = plt;
          }}
        }}
      }}

      // 3. Extract JS exception message
      var errorMessage = '';
      if (payload.exceptions && Array.isArray(payload.exceptions)) {{
        var exc = payload.exceptions[0];
        errorMessage = exc.value || exc.message || '';
      }}

      var extra = '';
      if (metricName) {{
        extra += '&rum_metric_name=' + encodeURIComponent(metricName) + '&rum_metric_value=' + encodeURIComponent(metricValue);
        if (metricRating) {{
          extra += '&rum_metric_rating=' + encodeURIComponent(metricRating);
        }}
      }}
      if (errorMessage) {{
        extra += '&rum_error_message=' + encodeURIComponent(errorMessage);
      }}

      // Extract rum_cid session cookie
      var cid = '';
      var cookieMatch = document.cookie.match(/(?:^|; )rum_cid=([^;]+)/);
      if (cookieMatch) {{
        cid = cookieMatch[1];
      }}
      if (cid) {{
        extra += '&cid=' + encodeURIComponent(cid);
      }}

      extra += '&rum_pathname=' + encodeURIComponent(window.location.pathname);
      return url + extra;
    }} catch (e) {{
      console.error('RUM: URL enrichment failed:', e);
      return url;
    }}
  }}

  // Intercept window.fetch
  if (typeof window.fetch === 'function') {{
    var originalFetch = window.fetch;
    window.fetch = function(input, init) {{
      if (typeof input === 'string' && input.indexOf('/rum-beacon') !== -1) {{
        var body = init && init.body;
        if (body && typeof body === 'string') {{
          input = enrichRumUrl(input, body);
        }}
      }}
      return originalFetch.apply(this, arguments);
    }};
  }}

  // Intercept XMLHttpRequest
  if (typeof XMLHttpRequest === 'function') {{
    var originalOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(method, url) {{
      if (typeof url === 'string' && url.indexOf('/rum-beacon') !== -1) {{
        this._rumBeacon = true;
        this._rumUrl = url;
      }}
      return originalOpen.apply(this, arguments);
    }};

    var originalSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function(body) {{
      if (this._rumBeacon && typeof body === 'string') {{
        try {{
          var enrichedUrl = enrichRumUrl(this._rumUrl, body);
          if (enrichedUrl !== this._rumUrl) {{
            originalOpen.call(this, 'POST', enrichedUrl, true);
          }}
        }} catch (e) {{
          console.error('RUM: XHR URL enrichment failed:', e);
        }}
      }}
      return originalSend.apply(this, arguments);
    }};
  }}

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

    // Only enable Web Vitals and Error tracking to reduce beacon volume
    // Default getWebInstrumentations() includes performance resource tracking
    // which sends one beacon per resource (50-100+ beacons per page load)
    if (Faro.WebVitalsInstrumentation) {{
      instrumentations.push(new Faro.WebVitalsInstrumentation());
    }}
    // Real export is "ErrorsInstrumentation" (plural) in every SDK version
    // we've checked (1.19.0, 2.9.0), but this probe used to look for the
    // singular "ErrorInstrumentation" and silently found nothing — no
    // exception, no warning, just no JS error tracking. Try the plural
    // first, fall back to the singular in case a future release renames it
    // back, and warn loudly if neither is found so this class of bug is
    // visible instead of silent.
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
      // Sampling: default to 100% (track all sessions)
      // Can be configured per-service via rum_sampling_rate in config
      // Examples: 0.1 = 10%, 0.5 = 50%, 1.0 = 100%
      samplingRate: 1.0,
      // Filter to only send important events (drop resource tracking, session events)
      beforeSend: function(event) {{
        if (!event) {{
          return null;
        }}

        // 1. If it's a batch payload structure (older Faro/fallback check)
        if (event.events && Array.isArray(event.events)) {{
          event.events = event.events.filter(function(e) {{
            var name = e.name || '';
            return name === 'faro.exception' || name === 'faro.performance.navigation';
          }});
        }}
        if (event.measurements && Array.isArray(event.measurements)) {{
          event.measurements = event.measurements.filter(function(m) {{
            return m.type === 'web-vitals';
          }});
        }}
        if (event.events || event.measurements) {{
          var hasData = (event.events && event.events.length > 0) || (event.measurements && event.measurements.length > 0);
          return hasData ? event : null;
        }}

        // 2. Otherwise, treat as Faro's standard individual transport item (Faro v1/v2 spec)
        if (event.type === 'event') {{
          var name = (event.payload && event.payload.name) || '';
          if (name === 'faro.exception' || name === 'faro.performance.navigation') {{
            return event;
          }}
          return null;
        }}

        if (event.type === 'measurement') {{
          var mType = (event.payload && event.payload.type) || '';
          if (mType === 'web-vitals') {{
            return event;
          }}
          return null;
        }}

        if (event.type === 'exception') {{
          return event;
        }}

        // For other types (logs, traces etc.), default to returning event
        return event;
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
    from backend.provision.utils import info, ok, warn

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"No config for service {service_id}")

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")

    if not all([access_key, secret_key, bucket]):
        raise RuntimeError(f"Service {service_id} missing FOS credentials (access_key, secret_key, bucket)")

    assert access_key is not None and secret_key is not None

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

    import hashlib

    # Compared against the FOS/S3 ETag header below, which is protocol-
    # mandated MD5 of the object bytes for a single-part PUT — not a
    # security use of the hash, just matching S3's ETag semantics.
    local_md5 = hashlib.md5(js_bytes, usedforsecurity=False).hexdigest()

    try:
        # Check if file already exists and is up to date via HEAD request
        with httpx.Client(verify=certifi.where()) as client:
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
        with httpx.Client(verify=certifi.where()) as client:
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
    from backend.provision.utils import info, ok, warn

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

    assert access_key is not None and secret_key is not None

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

        with httpx.Client(verify=certifi.where()) as client:
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
    from backend.provision.utils import info, ok, warn

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"No config for service {service_id}")

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    bucket = cfg.get("fos_bucket")
    region = cfg.get("fos_region", "us-east-1")

    if not all([access_key, secret_key, bucket]):
        raise RuntimeError(f"Service {service_id} missing FOS credentials (access_key, secret_key, bucket)")

    assert access_key is not None and secret_key is not None

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
        signed_headers = sign_fos_request(
            method="PUT",
            url=fos_url,
            headers=headers,
            body=bundle,
            access_key_id=access_key,
            secret_access_key=secret_key,
            region=region,
        )

        with httpx.Client(verify=certifi.where()) as client:
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
    fresh_cfg = svcconfig.load_config(service_id) or cfg
    rum_cfg = dict(fresh_cfg.get("rum") or {})
    if expected_current_version is not _NO_EXPECTATION and rum_cfg.get("faro_version") != expected_current_version:
        warn(
            f"Faro pin for {service_id} changed concurrently while syncing v{version} "
            f"(expected {expected_current_version!r}, found {rum_cfg.get('faro_version')!r}); "
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

    rum_cfg["faro_version"] = version
    rum_cfg["faro_content_hash"] = content_hash
    rum_cfg["faro_fos_etag_md5"] = etag_md5
    fresh_cfg["rum"] = rum_cfg
    svcconfig.save_config(service_id, fresh_cfg)

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
    from backend.provision.utils import info, ok, warn

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

        assert access_key is not None and secret_key is not None

        keep_version = (cfg.get("rum") or {}).get("faro_version") if keep_current else None
        keep_key = _faro_fos_key(keep_version) if keep_version else None

        fos_host = region_endpoint(region)
        list_url = f"https://{fos_host}/{bucket}?list-type=2&prefix={quote(FARO_KEY_PREFIX, safe='')}"

        with httpx.Client(verify=certifi.where()) as client:
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
