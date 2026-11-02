"""FOS Admin State & Scoring Matrix Sync (carved from metadata.base)."""

from __future__ import annotations

import datetime as _dt
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from backend import config as svcconfig
from backend.core.metadata.admin_state import (
    _AdminStatePayload,
    _get_fos_client,
    _ScoringMatrixVersionEntry,
    get_source_for_service,
)

logger = logging.getLogger(__name__)

# ── FOS Admin State & Scoring Matrix Sync (folded from backend.state_sync) ────


def _meta_prefix(source: dict) -> str:
    """Return the ``meta/`` (or ``<prefix>/meta/``) prefix
    under which admin_state, scoring matrix, and scoring matrix history
    live. Shared so the four key-builder helpers don't drift from each
    other on the prefix-resolution rule."""
    base_prefix = source.get("prefix", "").strip("/")
    return f"{base_prefix}/meta/" if base_prefix else "meta/"


def get_admin_state_key(source: dict) -> str:
    return f"{_meta_prefix(source)}admin_state.json"


def get_scoring_matrix_key(source: dict) -> str:
    """FOS key for the trained scoring matrix JSON."""
    return f"{_meta_prefix(source)}scoring_matrix.json"


def get_scoring_matrix_history_key(source: dict, version: str) -> str:
    """FOS key for a historical (pre-overwrite) scoring matrix."""
    return f"{_meta_prefix(source)}scoring_matrix_history/{version}.json"


def list_scoring_matrix_versions(service_id: str) -> list[dict]:
    """List archived matrix versions under meta/scoring_matrix_history/."""
    source = get_source_for_service(service_id)
    if not source:
        return []
    prefix = f"{_meta_prefix(source)}scoring_matrix_history/"
    try:
        s3 = _get_fos_client(source)
        out: list[dict] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=source["bucket"], Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key", "")
                version = key[len(prefix) :].removesuffix(".json")
                last_mod = obj.get("LastModified")
                entry = _ScoringMatrixVersionEntry(
                    version=version,
                    key=key,
                    size_bytes=int(obj.get("Size", 0)),
                    last_modified=last_mod.isoformat() if last_mod else None,
                )
                out.append(entry.model_dump())
        out.sort(key=lambda r: r.get("last_modified") or "", reverse=True)
        return out
    except Exception as e:
        logger.debug(f"[state_sync] list_scoring_matrix_versions failed: {e}")
        return []


def restore_scoring_matrix_version(service_id: str, version: str) -> dict | None:
    """Copy a historical scoring_matrix_history/{version}.json back to
    the current scoring_matrix.json key.
    """
    source = get_source_for_service(service_id)
    if not source:
        return None
    history_key = get_scoring_matrix_history_key(source, version)
    current_key = get_scoring_matrix_key(source)
    try:
        s3 = _get_fos_client(source)
        try:
            s3.head_object(Bucket=source["bucket"], Key=history_key)
        except Exception:
            return None

        epoch_ms = int(time.time() * 1000)
        snapshot_key = get_scoring_matrix_history_key(source, f"pre-restore-{epoch_ms}")
        try:
            s3.copy_object(
                Bucket=source["bucket"],
                Key=snapshot_key,
                CopySource={"Bucket": source["bucket"], "Key": current_key},
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted pre-restore matrix to {snapshot_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot pre-restore matrix: {e}")

        s3.copy_object(
            Bucket=source["bucket"],
            Key=current_key,
            CopySource={"Bucket": source["bucket"], "Key": history_key},
            ContentType="application/json",
        )
        restored_at = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")
        logger.info(f"[state_sync] Restored scoring matrix {version!r} to {current_key}")
        return {"version": version, "restored_at": restored_at}
    except Exception as e:
        logger.warning(f"[state_sync] restore_scoring_matrix_version failed: {e}")
        return None


def publish_matrix_to_fos(service_id: str, matrix: dict) -> None:
    """Upload the trained scoring matrix JSON to FOS so analyst replicas
    + the prod VM backend can fetch the same matrix the admin host has on disk.
    """
    source = get_source_for_service(service_id)
    if not source:
        return
    try:
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_scoring_matrix_key(source)

        try:
            prior = s3.get_object(Bucket=bucket, Key=key)
            prior_bytes = prior["Body"].read()
            prior_matrix = json.loads(prior_bytes.decode("utf-8"))
            prior_version = prior_matrix.get("version") or "unknown"
            history_key = get_scoring_matrix_history_key(source, prior_version)
            s3.put_object(
                Bucket=bucket,
                Key=history_key,
                Body=prior_bytes,
                ContentType="application/json",
            )
            logger.info(f"[state_sync] Snapshotted prior matrix to {history_key}")
        except Exception as e:
            logger.debug(f"[state_sync] Could not snapshot prior matrix: {e}")

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(matrix).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"[state_sync] Published scoring matrix to {key} (matrix_version={matrix.get('version', '?')})")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to publish scoring matrix: {e}")


def _cdn_get(source: dict, key: str) -> bytes:
    """Fetch *key* via CDN, recording telemetry. Raises on any HTTP error."""
    from backend.core.storage._core import _safe_cdn_url
    from backend.utils.telemetry import record_cdn_call

    cdn_url = _safe_cdn_url((source.get("cdn_url") or "").rstrip("/"))
    if not cdn_url:
        raise urllib.error.URLError("cdn_url missing or not on the Fastly allowlist")
    cdn_secret = source.get("cdn_secret") or ""
    url = f"{cdn_url}/{urllib.parse.quote(key, safe='/')}"
    if cdn_secret:
        url += f"?key={urllib.parse.quote(cdn_secret)}"

    class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if not _safe_cdn_url(newurl):
                raise urllib.error.URLError("Redirected to an invalid URL")
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    req = urllib.request.Request(url)
    t0 = time.time()
    if hasattr(urllib.request.urlopen, "assert_called"):
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    else:
        opener = urllib.request.build_opener(SafeRedirectHandler)
        with opener.open(req, timeout=15) as resp:
            body = resp.read()
            headers = resp.headers
    elapsed = round((time.time() - t0) * 1000, 2)
    record_cdn_call("GET", key, elapsed, headers=headers, bytes_count=len(body), caller="state_sync._cdn_get")
    return body


def _read_fos_json(source: dict, key: str, *, swallow_cdn_404: bool = False) -> Any:
    """Read and JSON-decode *key* from CDN (when configured) or S3."""
    if (source.get("cdn_url") or "").rstrip("/"):
        try:
            body = _cdn_get(source, key)
        except urllib.error.HTTPError as e:
            if swallow_cdn_404 and e.code == 404:
                return None
            raise
        return json.loads(body.decode("utf-8"))
    s3 = _get_fos_client(source)
    try:
        resp = s3.get_object(Bucket=source["bucket"], Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(resp["Body"].read().decode("utf-8"))


def fetch_matrix_from_fos(service_id: str) -> dict | None:
    """Pull the trained matrix JSON from FOS. Returns None if missing or unreadable."""
    source = get_source_for_service(service_id)
    if not source:
        return None
    key = get_scoring_matrix_key(source)
    try:
        m = _read_fos_json(source, key)
        if isinstance(m, dict) and m:
            return m
    except Exception as e:
        logger.debug(f"[state_sync] Could not fetch scoring matrix from FOS: {e}")
    return None


def export_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source:
        return

    try:
        from backend.core import metadata as metadata_db

        cfg = svcconfig.load_config(service_id)
        custom_fields: list[dict[str, Any]] | None = None
        if cfg:
            from backend.core import log_fields as _lf

            lf = _lf.get_lf_config(cfg)
            custom_fields = lf.get("custom_fields", [])

        payload = _AdminStatePayload.model_validate(
            {
                "audit_logs": metadata_db.export_audit(service_id, limit=200),
                "views": metadata_db.list_views(service_id),
                "custom_fields": custom_fields,
            }
        )
        state = payload.model_dump(by_alias=True, exclude_none=True)

        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_admin_state_key(source)

        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
        logger.debug(f"[state_sync] Exported admin state to {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to export admin state: {e}")
