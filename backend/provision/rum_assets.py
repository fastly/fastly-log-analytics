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


def _faro_fos_key(version: str) -> str:
    return f"{FARO_KEY_PREFIX}{version}{FARO_KEY_SUFFIX}"


def generate_rum_tracker_js(service_id: str, beacon_endpoint: str = "/rum-beacon") -> str:
    """Generate the Faro Web SDK wrapper JS.

    This creates a minimal loader that:
    1. Loads the Faro Web SDK from a CDN
    2. Initializes it with the service's beacon endpoint
    3. Sends beacons back to the origin

    The beacon endpoint defaults to /rum-beacon
    but can be customized if needed.
    """
    beacon_url = beacon_endpoint.format(service_id=service_id)

    return f"""/* Faro Web SDK wrapper for service {service_id} */
(function() {{
  var SERVICE_ID = '{service_id}';
  var BEACON_ENDPOINT = '{beacon_url}';

  // Check if Faro is present (either via window.Faro or window.GrafanaFaroWebSdk)
  var Faro = window.Faro || window.GrafanaFaroWebSdk;

  if (typeof Faro === 'undefined') {{
    var script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/@grafana/faro-web-sdk@^1/dist/bundle/faro-web-sdk.iife.js';
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
    if (Faro.ErrorInstrumentation) {{
      instrumentations.push(new Faro.ErrorInstrumentation());
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
        // Filter events to keep only exceptions and performance navigation
        // Drop: session_start, faro.performance.resource (50-100+ per page), and other non-critical events
        if (event.events && Array.isArray(event.events)) {{
          event.events = event.events.filter(function(e) {{
            var name = e.name || '';
            // Keep only exceptions and navigation (drop resource tracking and session events)
            return name === 'faro.exception' || name === 'faro.performance.navigation';
          }});
        }}

        // Keep only web-vitals measurements (LCP, CLS, INP, FCP, TTFB)
        if (event.measurements && Array.isArray(event.measurements)) {{
          event.measurements = event.measurements.filter(function(m) {{
            return m.type === 'web-vitals';
          }});
        }}

        // Only send beacons that have meaningful data after filtering
        var hasData = (event.events && event.events.length > 0) || (event.measurements && event.measurements.length > 0);
        return hasData ? event : null;
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

    Returns:
        {
            "path": "rum/rum-tracker.js",
            "bytes_uploaded": int,
            "fos_key": str (s3://bucket/rum/rum-tracker.js)
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

    # Generate the JS file first so we can check if it differs
    js_content = generate_rum_tracker_js(service_id)
    js_bytes = js_content.encode("utf-8")

    import hashlib

    local_md5 = hashlib.md5(js_bytes).hexdigest()

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
) -> dict[str, Any]:
    """Download a pinned Faro Web SDK version and upload it to the service's FOS bucket.

    Persists the version + content hash to ``cfg["rum"]`` so later reconcile
    passes can detect drift (upstream re-release, admin-requested upgrade)
    without re-downloading on every check.

    Returns:
        {
            "version": str,
            "path": "rum/faro-web-sdk-v{version}.iife.js",
            "bytes_uploaded": int,
            "content_hash": str (MD5 hex digest),
            "fos_key": str (s3://bucket/path),
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
    content_hash = hashlib.md5(bundle).hexdigest()

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

    rum_cfg = dict(cfg.get("rum") or {})
    rum_cfg["faro_version"] = version
    rum_cfg["faro_content_hash"] = content_hash
    cfg["rum"] = rum_cfg
    svcconfig.save_config(service_id, cfg)

    return {
        "version": version,
        "path": fos_key,
        "bytes_uploaded": len(bundle),
        "content_hash": content_hash,
        "fos_key": f"s3://{bucket}/{fos_key}",
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
    current_hash = hashlib.md5(bundle).hexdigest()
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
