import json
import logging

from backend import config as svcconfig
from backend.core.duckdb import _get_fos_client, get_source_for_service

logger = logging.getLogger(__name__)


def get_admin_state_key(source: dict) -> str:
    base_prefix = source.get("prefix", "").strip("/")
    iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
    return f"{iceberg_root}/meta/admin_state.json"


def export_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source or source.get("access_level") == "read_only":
        return

    try:
        from backend.core import metadata_db

        state: dict = {
            "_audit_logs": metadata_db.export_audit(service_id, limit=200),
            "_views": metadata_db.list_views(service_id),
        }

        # Export custom_fields from the service config file
        cfg = svcconfig.load_config(service_id)
        if cfg:
            from backend.core import log_fields as _lf

            lf = _lf.get_lf_config(cfg)
            state["custom_fields"] = lf.get("custom_fields", [])

        # Upload to FOS
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_admin_state_key(source)

        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
        logger.debug(f"[state_sync] Exported admin state to {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to export admin state: {e}")


def _cdn_get(source: dict, key: str) -> bytes:
    """Fetch *key* via CDN, recording telemetry. Raises on any HTTP error."""
    import time
    import urllib.error
    import urllib.parse
    import urllib.request

    from backend.utils.telemetry import record_cdn_call

    cdn_url = (source.get("cdn_url") or "").rstrip("/")
    cdn_secret = source.get("cdn_secret") or ""
    url = f"{cdn_url}/{urllib.parse.quote(key, safe='/')}"
    if cdn_secret:
        url += f"?key={urllib.parse.quote(cdn_secret)}"
    req = urllib.request.Request(url)
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read()
        headers = resp.headers
    elapsed = round((time.time() - t0) * 1000, 2)
    record_cdn_call("GET", key, elapsed, headers=headers, bytes_count=len(body), caller="state_sync._cdn_get")
    return body


def import_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source:
        return

    try:
        import urllib.error

        bucket = source["bucket"]
        key = get_admin_state_key(source)
        cdn_url = (source.get("cdn_url") or "").rstrip("/")

        if cdn_url:
            try:
                body = _cdn_get(source, key)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return
                raise
            state = json.loads(body.decode("utf-8"))
        else:
            s3 = _get_fos_client(source)
            try:
                resp = s3.get_object(Bucket=bucket, Key=key)
            except s3.exceptions.NoSuchKey:
                return
            state = json.loads(resp["Body"].read().decode("utf-8"))

        from backend.core import metadata_db

        metadata_db.replace_audit_for_service(service_id, state.get("_audit_logs", []))
        metadata_db.replace_views_for_service(service_id, state.get("_views", []))

        # Merge custom_fields into the local service config so the analyst's
        # UI catalog matches what the admin has defined.
        if "custom_fields" in state:
            cfg = svcconfig.load_config(service_id)
            if cfg is not None:
                from backend.core import log_fields as _lf

                lf = _lf.get_lf_config(cfg)
                lf["custom_fields"] = state["custom_fields"]
                cfg["log_fields"] = lf
                svcconfig.save_config(service_id, cfg)

        logger.debug(f"[state_sync] Imported admin state from {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to import admin state: {e}")
