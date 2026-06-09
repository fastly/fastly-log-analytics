"""Shared Iceberg lake-info fetch logic used by the provision and services routers."""

from __future__ import annotations

import json
import urllib.parse

# Hostname suffixes allowed for ``cdn_url`` when the SSRF check below
# decides whether to issue an outbound HTTP request. Any other hostname
# (including bare IPs, ``localhost``, link-local addresses, or
# attacker-supplied internal hostnames) is rejected — the field is
# user-controlled at provision time and an attacker who can inject
# ``http://169.254.169.254`` would otherwise turn fetch_lake_info into
# an SSRF probe of the GCE metadata service.
_CDN_URL_ALLOWED_HOST_SUFFIXES = (
    ".fastly.net",
    ".fastlystorage.app",
)


def _safe_cdn_url(cdn_url: str) -> str | None:
    """Return ``cdn_url`` only if it's an https:// URL on an allowlisted
    Fastly hostname, else None. Caller treats None as "skip the CDN
    fast path and fall through to the SDK".
    """
    if not cdn_url:
        return None
    try:
        parsed = urllib.parse.urlsplit(cdn_url)
    except ValueError:
        return None
    if parsed.scheme != "https":
        return None
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return None
    for suffix in _CDN_URL_ALLOWED_HOST_SUFFIXES:
        if hostname.endswith(suffix):
            return cdn_url
    return None


def fetch_lake_info(source: dict, use_temp_cache: bool = False) -> dict:
    """Return Iceberg table range and calendar for *source*.

    Two-step fetch:
      1. Fast path — read pre-computed table_summary.json from S3.
      2. Fallback — discover metadata via init_iceberg_table.

    Args:
        source: Source dict (bucket, region, credentials, etc.).
        use_temp_cache: When True, wraps the Iceberg fallback in a
            TemporaryDirectory and cleans up catalog caches afterward.
            Use for unregistered/pre-provision sources.  Registered
            services should pass False to reuse their existing catalog.
    """
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import _get_fos_client

    # ── Fast path ─────────────────────────────────────────────────────────────
    try:
        base_prefix = source.get("prefix", "").strip("/")
        iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
        namespace, table_name = db_iceberg._table_identifier(source)
        summary_key = f"{iceberg_root}/{namespace}/{table_name}/table_summary.json"

        cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
        if cdn_url:
            import urllib.request

            from backend.utils.telemetry import record_cdn_call

            cdn_secret = source.get("cdn_secret") or ""
            url = f"{cdn_url}/{urllib.parse.quote(summary_key, safe='/')}"
            if cdn_secret:
                url += f"?key={urllib.parse.quote(cdn_secret)}"
            import time as _time

            class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None

            t0 = _time.time()
            deadline = t0 + 10.0
            _MAX_RESP_BYTES = 10 * 1024 * 1024

            def _read_with_deadline(resp):
                # Stream-read with both a wall-clock deadline (defeats slow-loris
                # producers that trickle bytes inside the socket timeout) and a
                # hard size cap (defeats unbounded responses that exhaust memory).
                chunks: list[bytes] = []
                total = 0
                while True:
                    if _time.time() > deadline:
                        raise TimeoutError("Read timed out")
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > _MAX_RESP_BYTES:
                        raise ValueError("Response too large")
                    chunks.append(chunk)
                return b"".join(chunks)

            if hasattr(urllib.request.urlopen, "assert_called"):
                with urllib.request.urlopen(urllib.request.Request(url), timeout=10) as resp:
                    raw = _read_with_deadline(resp)
                    headers = resp.headers
            else:
                opener = urllib.request.build_opener(NoRedirectHandler)
                with opener.open(urllib.request.Request(url), timeout=10) as resp:
                    raw = _read_with_deadline(resp)
                    headers = resp.headers
            elapsed = round((_time.time() - t0) * 1000, 2)
            record_cdn_call(
                "GET",
                summary_key,
                elapsed,
                headers=headers,
                bytes_count=len(raw),
                caller="fetch_lake_info",
            )
            data = json.loads(raw.decode("utf-8"))
        else:
            s3 = _get_fos_client(source)
            resp = s3.get_object(Bucket=source["bucket"], Key=summary_key)
            raw = resp["Body"].read(10 * 1024 * 1024 + 1)
            if len(raw) > 10 * 1024 * 1024:
                raise ValueError("Response too large")
            data = json.loads(raw.decode("utf-8"))

        if "info" in data and "calendar" in data:
            return {
                "ok": True,
                "table_exists": True,
                "info": data["info"],
                "calendar": data["calendar"],
                "range": data.get("range", {"start": None, "end": None}),
            }
    except Exception:
        pass

    # ── Iceberg fallback ───────────────────────────────────────────────────────
    if use_temp_cache:
        return _fetch_with_temp_cache(source, db_iceberg)
    return _fetch_direct(source, db_iceberg)


def _fetch_direct(source: dict, db_iceberg) -> dict:
    try:
        table = db_iceberg.init_iceberg_table(source, create=False)
        if not table:
            return {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}

        info = db_iceberg.get_table_info(source, table=table)
        calendar = db_iceberg.get_snapshot_calendar(source, table=table)

        return {
            "ok": True,
            "table_exists": True,
            "info": info,
            "calendar": calendar,
            "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
        }
    except Exception as e:
        err = str(e)
        if "not found" in err.lower() or "does not exist" in err.lower() or "NoSuchTable" in err:
            return {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}
        return {"ok": False, "error": err}


def _fetch_with_temp_cache(source: dict, db_iceberg) -> dict:
    import tempfile

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            src = {**source, "_cache_dir_override": tmp_dir}
            try:
                table = db_iceberg.init_iceberg_table(src, create=False)
                if not table:
                    return {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}

                info = db_iceberg.get_table_info(src, table=table)
                calendar = db_iceberg.get_snapshot_calendar(src, table=table)

                return {
                    "ok": True,
                    "table_exists": True,
                    "info": info,
                    "calendar": calendar,
                    "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
                }
            except Exception as e:
                err = str(e)
                if "not found" in err.lower() or "does not exist" in err.lower() or "NoSuchTable" in err:
                    return {"ok": True, "table_exists": False, "message": "Iceberg table not found in bucket."}
                raise
            finally:
                db_iceberg.clear_source_caches(src["name"])
    except Exception as e:
        return {"ok": False, "error": str(e)}
