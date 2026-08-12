import json
import logging
import time

from backend import config as svcconfig
from backend.core.duckdb import _get_fos_client, get_source_for_service

logger = logging.getLogger(__name__)


def _iceberg_meta_prefix(source: dict) -> str:
    """Return the ``iceberg/meta/`` (or ``<prefix>/iceberg/meta/``) prefix
    under which admin_state, scoring matrix, and scoring matrix history
    live. Shared so the four key-builder helpers don't drift from each
    other on the prefix-resolution rule."""
    base_prefix = source.get("prefix", "").strip("/")
    iceberg_root = f"{base_prefix}/iceberg" if base_prefix else "iceberg"
    return f"{iceberg_root}/meta/"


def get_admin_state_key(source: dict) -> str:
    return f"{_iceberg_meta_prefix(source)}admin_state.json"


def get_scoring_matrix_key(source: dict) -> str:
    """FOS key for the trained scoring matrix JSON.

    Separate from admin_state because the matrix is a build artifact (gitignored,
    not in admin_state.custom_fields). Lives under the same iceberg/meta/ prefix
    so analyst hosts read the same blob the admin host wrote.
    """
    return f"{_iceberg_meta_prefix(source)}scoring_matrix.json"


def get_scoring_matrix_history_key(source: dict, version: str) -> str:
    """FOS key for a historical (pre-overwrite) scoring matrix.

    Lives under ``iceberg/meta/scoring_matrix_history/{version}.json``
    so the operator can list past matrices and roll back to a known-good
    one if a fresh retrain regresses AUC.
    """
    return f"{_iceberg_meta_prefix(source)}scoring_matrix_history/{version}.json"


def list_scoring_matrix_versions(service_id: str) -> list[dict]:
    """List archived matrix versions under iceberg/meta/scoring_matrix_history/.

    Returns ``[{"version": "...", "key": "...", "size_bytes": int,
    "last_modified": "<iso>"}, ...]`` sorted by last_modified descending.
    Best-effort: returns empty list on any S3 error.
    """
    source = get_source_for_service(service_id)
    if not source:
        return []
    prefix = f"{_iceberg_meta_prefix(source)}scoring_matrix_history/"
    try:
        s3 = _get_fos_client(source)
        out: list[dict] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=source["bucket"], Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key", "")
                # Strip prefix + .json suffix to recover the version string.
                version = key[len(prefix) :].removesuffix(".json")
                last_mod = obj.get("LastModified")
                out.append(
                    {
                        "version": version,
                        "key": key,
                        "size_bytes": int(obj.get("Size", 0)),
                        "last_modified": last_mod.isoformat() if last_mod else None,
                    }
                )
        out.sort(key=lambda r: r.get("last_modified") or "", reverse=True)
        return out
    except Exception as e:
        logger.debug(f"[state_sync] list_scoring_matrix_versions failed: {e}")
        return []


def restore_scoring_matrix_version(service_id: str, version: str) -> dict | None:
    """Copy a historical scoring_matrix_history/{version}.json back to
    the current scoring_matrix.json key. The next backend that calls
    fetch_matrix_from_fos sees the restored matrix.

    Returns ``{"version": "...", "restored_at": "<iso>"}`` on success,
    None when the version doesn't exist. Caller is responsible for
    busting analytics caches + deleting the local matrix.json so
    _load_matrix's resolution-order step 1 doesn't shadow the restored
    FOS matrix.

    Live Wasm at the edge still uses its previously-embedded matrix —
    a full restore-to-edge requires re-running deploy_wasm.sh.
    """
    import datetime as _dt

    source = get_source_for_service(service_id)
    if not source or source.get("access_level") == "read_only":
        return None
    history_key = get_scoring_matrix_history_key(source, version)
    current_key = get_scoring_matrix_key(source)
    try:
        s3 = _get_fos_client(source)
        try:
            s3.head_object(Bucket=source["bucket"], Key=history_key)
        except Exception:
            return None  # version doesn't exist

        # SNAPSHOT-BEFORE-OVERWRITE: copy the current live matrix to the
        # history prefix BEFORE the restore copy_object overwrites it.
        # Without this, a bad restore (operator picks the wrong version)
        # is irreversible because the only copy of the prior-live matrix
        # was the one we're about to clobber. Best-effort: NoSuchKey (no
        # prior current) is silent; other failures log at DEBUG and do
        # NOT block the restore — the operator's active intent wins.
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

    Without this, every fresh container needs the matrix scp'd in
    manually (which is how the AUC field got bootstrapped the first
    time). With this, calling enable_scoring or retrain on any
    read_write host pushes the matrix to FOS exactly once, and every
    other host's ``fetch_matrix_from_fos`` picks it up.

    Idempotent — calling with the same matrix overwrites the prior copy.
    Silent no-op on read_only sources (analyst pods don't write back).
    """
    source = get_source_for_service(service_id)
    if not source or source.get("access_level") == "read_only":
        return
    try:
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        key = get_scoring_matrix_key(source)

        # SNAPSHOT-BEFORE-OVERWRITE: copy the current matrix (if any) to
        # the history prefix BEFORE the new put_object. Lets the
        # operator roll back to a known-good matrix if a fresh retrain
        # regresses AUC. Best-effort: history-snapshot failure (no
        # prior current, permission edge case) does NOT block the
        # publish — the operator's active intent always wins.
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
            # NoSuchKey on first-ever publish is expected and silent;
            # other failures log at DEBUG so we know about them without
            # spamming the operator.
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


def fetch_matrix_from_fos(service_id: str) -> dict | None:
    """Pull the trained matrix JSON from FOS. Returns None if missing
    (no admin host has published it yet) or unreadable.

    Read-side path uses CDN when configured (cdn_url + cdn_secret) so
    analyst hosts don't burn a Class B FOS op on every backend restart;
    falls back to S3 GetObject when the CDN isn't wired.
    """
    source = get_source_for_service(service_id)
    if not source:
        return None
    key = get_scoring_matrix_key(source)
    try:
        if source.get("cdn_url"):
            body = _cdn_get(source, key)
            m = json.loads(body.decode("utf-8"))
        else:
            s3 = _get_fos_client(source)
            try:
                resp = s3.get_object(Bucket=source["bucket"], Key=key)
            except s3.exceptions.NoSuchKey:
                return None
            m = json.loads(resp["Body"].read().decode("utf-8"))
        if isinstance(m, dict) and m:
            return m
    except Exception as e:
        logger.debug(f"[state_sync] Could not fetch scoring matrix from FOS: {e}")
    return None


def export_admin_state(service_id: str):
    source = get_source_for_service(service_id)
    if not source or source.get("access_level") == "read_only":
        return

    try:
        from backend.core import metadata as metadata_db

        state: dict = {
            "_audit_logs": metadata_db.export_audit(service_id, limit=200),
            "_views": metadata_db.list_views(service_id),
        }

        # Export custom_fields from the service config file
        cfg = svcconfig.load_config(service_id)
        if cfg:
            from backend.core import field_registry as _lf

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

    from backend.core.iceberg.lake_info import _safe_cdn_url
    from backend.utils.telemetry import record_cdn_call

    # SSRF guard: ``cdn_url`` is user-supplied at provision time. Reject
    # anything that isn't an https Fastly hostname so the helper can't be
    # turned into an outbound HTTP probe of internal services (cloud
    # metadata at 169.254.169.254 on AWS/GCE/Azure, peer VMs, link-local
    # addresses).
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

        from backend.core import metadata as metadata_db

        # NON-DESTRUCTIVE on read_only analyst hosts: the analyst pod
        # writes views and audit_logs locally (the routers have no
        # access_level gate), and export_admin_state refuses to push from
        # read_only sources — so the old wholesale replace_*_for_service
        # silently wiped analyst writes on every metadata_sync cron tick
        # with no chance of recovery. On read_only hosts we upsert/merge
        # so local rows survive. On read_write hosts the original
        # replace_* still runs — the writer is the source of truth there.
        is_read_only = (source or {}).get("access_level") == "read_only"
        if is_read_only:
            metadata_db.upsert_views_for_service(service_id, state.get("_views", []))
            metadata_db.merge_audit_for_service(service_id, state.get("_audit_logs", []))
        else:
            metadata_db.replace_audit_for_service(service_id, state.get("_audit_logs", []))
            metadata_db.replace_views_for_service(service_id, state.get("_views", []))

        # Merge custom_fields into the local service config so the analyst's
        # UI catalog matches what the admin has defined.
        #
        # WHY THIS IS A MERGE (not an overwrite): scoring is enabled by code
        # that injects 8 well-known custom_fields (edge_score, edge_score_l1,
        # edge_score_l2, edge_cookie_compliance, edge_score_reason, edge_sid,
        # edge_score_rtt_us, edge_score_exec_us) via _SCORING_CUSTOM_FIELDS.
        # If the FOS-stored admin_state.json
        # predates scoring enablement (or was last written by a host that
        # didn't have scoring), an unconditional overwrite silently strips
        # those fields on every metadata_sync tick — which makes ingest
        # write all-NULL values for the scoring columns even though Fastly
        # is still emitting the data correctly. The 2026-06-02 production
        # incident was exactly this. When scoring is enabled in the local
        # cfg, ALWAYS re-inject the canonical list from code; the code is
        # the source of truth, not whatever happens to be in FOS.
        if "custom_fields" in state:
            cfg = svcconfig.load_config(service_id)
            if cfg is not None:
                from backend.core import field_registry as _lf
                from backend.provision.system_fields import (
                    reconcile_system_custom_fields,
                    system_feature_flags,
                )

                lf = _lf.get_lf_config(cfg)
                remote_fields = list(state["custom_fields"])

                # Strip any system-named entries the remote might carry
                # (stale, partial, or just plain different) and re-add the
                # canonical entries from code for whichever features are
                # enabled locally. CMCD is included for the same reason
                # scoring is — see backend/provision/system_fields.py.
                scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
                remote_fields = reconcile_system_custom_fields(
                    remote_fields,
                    scoring_enabled=scoring_enabled,
                    cmcd_enabled=cmcd_enabled,
                )

                lf["custom_fields"] = remote_fields
                cfg["log_fields"] = lf
                svcconfig.save_config(service_id, cfg)

        logger.debug(f"[state_sync] Imported admin state from {key}")
    except Exception as e:
        logger.warning(f"[state_sync] Failed to import admin state: {e}")
