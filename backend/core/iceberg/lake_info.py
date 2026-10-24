"""Shared DuckLake lake-info fetch logic used by the provision and services routers."""

from __future__ import annotations

import urllib.parse

# Hostname suffixes allowed for ``cdn_url`` when the SSRF check below
# decides whether to issue an outbound HTTP request. Any other hostname
# (including bare IPs, ``localhost``, link-local addresses, or
# attacker-supplied internal hostnames) is rejected — the field is
# user-controlled at provision time and an attacker who can inject
# ``http://169.254.169.254`` would otherwise turn fetch_lake_info into
# an SSRF probe of the cloud metadata service (same link-local IP on
# AWS, GCE, and Azure).
_CDN_URL_ALLOWED_HOST_SUFFIXES = (
    ".fastly.net",
    ".fastlystorage.app",
)


def _safe_cdn_url(cdn_url: str) -> str | None:
    """Return ``cdn_url`` only if it's an https:// URL on an allowlisted
    Fastly hostname, else None.
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
    """Return DuckLake table range and calendar for *source*.

    Args:
        source: Source dict (bucket, region, credentials, etc.).
        use_temp_cache: When True, wraps the DuckLake query in a
            TemporaryDirectory and cleans up source caches afterward.
            Use for unregistered/pre-provision sources.  Registered
            services should pass False to reuse their existing catalog.
    """
    from backend.core import iceberg as db_iceberg

    if use_temp_cache:
        return _fetch_with_temp_cache(source, db_iceberg)
    return _fetch_direct(source, db_iceberg)


def _fetch_direct(source: dict, db_iceberg) -> dict:
    """DuckLake-native fallback (v3 write-path cutover).

    Existence is checked via ``ducklake_table_exists`` — a row in
    ``ducklake_table_info`` — rather than a pyiceberg ``load_table``. A
    file-count-based existence check would be wrong here: small commits
    land "inlined" directly in the catalog metadata with zero physical
    files (see ``iceberg/_ducklake.py`` module docstring), so a table with
    real committed rows can legitimately have zero files.
    """
    try:
        with db_iceberg.ducklake_session(source):
            if not db_iceberg.ducklake_table_exists(source):
                return {"ok": True, "table_exists": False, "message": "DuckLake table not found."}

            info = db_iceberg.get_table_info(source)
            if "error" in info:
                # get_table_info never raises (see its docstring) — it reports
                # attach/scan failures via this key instead, so surface them
                # the same way an exception would have: ok=False, not a false
                # "table not found".
                return {"ok": False, "error": info["error"]}
            calendar = db_iceberg.get_snapshot_calendar(source)

            return {
                "ok": True,
                "table_exists": True,
                "info": info,
                "calendar": calendar,
                "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _fetch_with_temp_cache(source: dict, db_iceberg) -> dict:
    """Same DuckLake-native fallback as :func:`_fetch_direct`, but scoped to
    a throwaway ``_cache_dir_override`` and cleared via
    ``clear_source_caches`` afterward — used for unregistered/pre-provision
    sources so a failed provisioning attempt doesn't leak an in-memory
    catalog cache entry."""
    import tempfile

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            src = {**source, "_cache_dir_override": tmp_dir}
            if "iceberg_metadata_location" in source:
                src["iceberg_metadata_location"] = source["iceberg_metadata_location"]
            try:
                with db_iceberg.ducklake_session(src):
                    if not db_iceberg.ducklake_table_exists(src):
                        return {"ok": True, "table_exists": False, "message": "DuckLake table not found."}

                    info = db_iceberg.get_table_info(src)
                    if "error" in info:
                        return {"ok": False, "error": info["error"]}
                    calendar = db_iceberg.get_snapshot_calendar(src)

                    return {
                        "ok": True,
                        "table_exists": True,
                        "info": info,
                        "calendar": calendar,
                        "range": {"start": info.get("min_timestamp"), "end": info.get("max_timestamp")},
                    }
            finally:
                db_iceberg.clear_source_caches(src["name"])
    except Exception as e:
        return {"ok": False, "error": str(e)}
