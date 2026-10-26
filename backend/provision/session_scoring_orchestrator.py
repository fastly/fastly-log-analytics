"""End-to-end ``enable_scoring`` / ``disable_scoring`` for a single
customer's logging service.

This is the user-facing "turn on session scoring" flow. It composes the
existing primitives:

  - ``ensure_scoring_service`` / ``delete_scoring_service`` (Compute
    service + ConfigStores + AES key + resource links, in
    backend/provision/session_scoring_setup.py)
  - ``scripts/scoring/deploy_wasm.sh`` (build + push the Wasm)
  - ``ensure_vcl_snippet`` + ``ensure_condition`` (Fastly idempotent
    helpers from backend/core/fastly/service.py)
  - ``update_logging_endpoint`` (regenerate log format + push, from
    backend/provision/fastly_api.py)

The VCL mutation follows the same proven pattern as
``ensure_logging_endpoint`` ([backend/provision/fastly_api.py:636](backend/provision/fastly_api.py#L636)):
    get_active → clone → mutate draft → validate → activate
    → on any exception, re-activate the prior version (leave the draft
      dangling for debug) and re-raise.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend import config as svcconfig
from backend.core.fastly.client import (
    ensure_vcl_snippet,
    fastly,
    fastly_raw,
    get_active_version,
    list_vcl_snippets,
)
from backend.provision.session_scoring_setup import (
    MATRIX_KEY_NAME,
    MATRIX_RESOURCE_LINK_NAME,
    delete_scoring_service,
    ensure_scoring_service,
)
from backend.provision.utils import BOLD, _c, draft_version, fail, info, ok, rollback_to_version, warn

logger = logging.getLogger(__name__)

# Locations of the matrix files relative to repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_MATRIX_PATH = _REPO_ROOT / "compute" / "scorer" / "matrix.json"
# Prebuilt, matrix-less Wasm package. Built off-VM (Makefile `scorer-package`
# / deploy_wasm.sh) and committed; the Dockerfile COPYies it into the image so
# the backend can deploy it via the Fastly API with no toolchain. The matrix
# is no longer embedded — it's served from the scoring_matrix KV Store.
_SCORER_PACKAGE = _REPO_ROOT / "compute" / "scorer" / "pkg" / "session-scorer.tar.gz"


def _tenant_matrix_path(logging_service_id: str) -> Path:
    """Per-tenant matrix path used by retrain / boot / deploy.

    Mirrors what ``_load_matrix`` checks first in
    [backend/routers/session_scoring.py](../routers/session_scoring.py)
    so writers and readers agree on the same location.
    """
    return _MATRIX_PATH.with_name(f"{_MATRIX_PATH.stem}_{logging_service_id}{_MATRIX_PATH.suffix}")


def _resolve_tenant_matrix_for_deploy(logging_service_id: str) -> Path | None:
    """Return a local path containing this tenant's trained matrix, or None.

    Resolution order:
      1. ``matrix_{sid}.json`` already on local disk → use as-is.
      2. Fetch from FOS (``iceberg/meta/scoring_matrix.json``) and
         materialise to the tenant path → use that.
      3. Nothing trained anywhere → return None (caller deploys the
         empty default and L2 self-disables, same as pre-fix behaviour).

    NOT consulted: the legacy shared ``matrix.json`` path. Embedding
    that would re-open the cross-tenant leak audit finding #005
    closed — service B's wasm would carry whichever tenant last
    retrained before #005 shipped.
    """
    import json as _json

    tenant_path = _tenant_matrix_path(logging_service_id)
    if tenant_path.exists():
        return tenant_path

    try:
        from backend.state_sync import fetch_matrix_from_fos

        matrix = fetch_matrix_from_fos(logging_service_id)
    except Exception:
        logger.debug("fetch_matrix_from_fos failed during deploy resolve", exc_info=True)
        matrix = None

    if not matrix or matrix.get("vocab_size", 0) <= 0:
        return None

    tenant_path.parent.mkdir(parents=True, exist_ok=True)
    with tenant_path.open("w") as f:
        _json.dump(matrix, f)
    return tenant_path


# Custom-field definitions the orchestrator adds/removes when enabling/
# disabling scoring. Kept as a single source of truth so disable_scoring
# can find them by name to undo cleanly.
# vcl_log_expression points at req.http.x-fos-edge-data:edge_* subfields
# (NOT the source req.http.x-edge-data:* subfields). Why: subfield writes
# in vcl_recv propagate to the log emitter; writes anywhere else don't.
# Our session_scoring recv snippet (pass 2) copies x-edge-data:* into
# x-fos-edge-data:edge_* exactly so this log format can read them.
# stage="deliver" is kept so the field shows up in the right tab in the
# UI; the value is actually populated in recv pass 2 via the manual
# promotion in session_scoring_vcl.recv_snippet.
_SCORING_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": "edge_score",
        "label": "Edge Score",
        "description": "Combined session-anomaly score (0–100, quantized to nearest 5) from the edge scorer.",
        "vcl_log_expression": "req.http.x-edge-score:score",
        "collection_stage": "deliver",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 4,
        "enabled": True,
    },
    {
        "name": "edge_score_l1",
        "label": "Edge Score (Layer 1)",
        "description": "Layer-1 (universal behavioral) score contribution.",
        "vcl_log_expression": "req.http.x-edge-score:l1",
        "collection_stage": "deliver",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 4,
        "enabled": True,
    },
    {
        "name": "edge_score_l2",
        "label": "Edge Score (Layer 2)",
        "description": "Layer-2 (route transition) score contribution.",
        "vcl_log_expression": "req.http.x-edge-score:l2",
        "collection_stage": "deliver",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 4,
        "enabled": True,
    },
    {
        "name": "edge_cookie_compliance",
        "label": "Cookie Compliance",
        "description": "ok | missing | tampered | unknown.",
        "vcl_log_expression": "req.http.x-edge-score:compliance",
        "collection_stage": "deliver",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 10,
        "enabled": True,
    },
    {
        "name": "edge_score_reason",
        "label": "Score Reason",
        "description": "Comma-separated list of fired scoring rules.",
        "vcl_log_expression": "req.http.x-edge-score:reason",
        "collection_stage": "deliver",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 60,
        "enabled": True,
    },
    {
        "name": "edge_sid",
        "label": "Session ID",
        "description": (
            "12-hex-char rotating session id from the edge scorer cookie. "
            "Empty when the inbound request had no valid cookie. Used as "
            "the key for admin session labels (good / bad / neutral)."
        ),
        "vcl_log_expression": "req.http.x-edge-score:sid",
        "collection_stage": "deliver",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 12,
        "enabled": True,
    },
    {
        "name": "edge_score_rtt_us",
        "label": "Scorer Round-Trip (µs)",
        "description": (
            "Edge-observed round-trip latency for the scoring sub-fetch, in "
            "microseconds (a time.elapsed.usec diff in VCL). Includes network "
            "+ Compute cold-start + Wasm exec. Fail-open / timeout rows show "
            "≈the backend timeout budget — this is the number to compare "
            "against connect/first_byte_timeout when tuning fail-open rate."
        ),
        "vcl_log_expression": "req.http.x-edge-score:rtt",
        "collection_stage": "deliver",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 6,
        "enabled": True,
    },
    {
        "name": "edge_score_exec_us",
        "label": "Scorer Exec (µs)",
        "description": (
            "Scorer-reported Wasm execution time in microseconds "
            "(X-Edge-Score-Exec-Us response header). Successful (HTTP 200) "
            "scoring rows only; NULL on fail-open rows since the scorer never "
            "responded. Isolates compute cost from the round-trip's network "
            "+ cold-start component."
        ),
        "vcl_log_expression": "req.http.x-edge-score:exec",
        "collection_stage": "deliver",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "edge_matrix_version",
        "label": "Matrix Version",
        "description": (
            "Transition-matrix version (YYYY-MM-DD-<suffix>) that scored this "
            "request, from the scorer's X-Edge-Matrix-Version response header. "
            "Lets a logged edge score be correlated to the matrix (and rollback) "
            "that produced it. Empty on cookie-missing requests (L2 is skipped "
            "so no matrix is loaded) and NULL on fail-open rows."
        ),
        "vcl_log_expression": "req.http.x-edge-score:matrix",
        "collection_stage": "deliver",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 18,
        "enabled": True,
    },
]
_SCORING_FIELD_NAMES = {cf["name"] for cf in _SCORING_CUSTOM_FIELDS}


def merge_scoring_custom_fields(custom_fields: list[dict] | None) -> list[dict]:
    """Return ``custom_fields`` with the canonical scoring fields re-applied.

    Drops any existing entries whose name collides with a scoring field, then
    appends fresh copies of ``_SCORING_CUSTOM_FIELDS`` (code is the source of
    truth). Kept-first-then-canonical ordering is preserved. Single source of
    truth for the merge idiom shared by ``_add_scoring_custom_fields``,
    ``orchestrator``, and the ``cli``.

    DEPRECATED: Use get_scoring_fields() for new code. This function is kept
    for backward compatibility during the transition away from persisting
    system fields in config files.
    """
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    return kept + [dict(cf) for cf in _SCORING_CUSTOM_FIELDS]


# Canonical inputs for the VCL drift fingerprint. We hash the snippets the
# *current generator code* produces using a constant secret + default config
# so that operator-tunable changes (exclude_url_regex, enforce_status_code,
# key rotation) DON'T register as drift — only a change to the snippet
# generation code itself does. The real secret/regex/status are pushed by
# their own endpoints, which already redeploy the affected snippet.
_VCL_FINGERPRINT_SECRET = "drift-fingerprint"


def _package_sha(path: Path = _SCORER_PACKAGE) -> str | None:
    """sha256 of the committed Wasm package bytes — the exact bytes the
    backend uploads on deploy. Returns None if the file is missing/unreadable
    (a fresh checkout without the prebuilt package)."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def scorer_vcl_fingerprint(logging_service_id: str) -> str:
    """Content hash of the scoring VCL the current generator produces for
    this service, using canonical inputs (see _VCL_FINGERPRINT_SECRET). A
    change to generate_scoring_vcl shifts this hash; operator config tweaks
    do not."""
    snippets = generate_scoring_vcl(logging_service_id, _VCL_FINGERPRINT_SECRET)
    return hashlib.sha256(json.dumps(snippets, sort_keys=True).encode()).hexdigest()


def shipped_scorer_identity(logging_service_id: str) -> dict[str, str | None]:
    """Identity of the scorer build the backend would deploy *right now*:
    the committed Wasm package sha + the VCL generator fingerprint. The
    status endpoint compares this to the {deployed_package_sha,
    deployed_vcl_sha} stamped at the last enable to flag edge drift."""
    return {
        "package_sha": _package_sha(),
        "vcl_sha": scorer_vcl_fingerprint(logging_service_id),
    }


def _live_package_files_hash(scoring_service_id: str, version: int, token: str) -> str:
    """The edge's OWN integrity hash for a Compute version's package
    (``files_hash``, falling back to the deprecated ``hashsum``). Empty string
    when unavailable. Lets a redeploy detect a package that changed at the edge
    out from under us (e.g. a manual deploy) without trusting only the cfg
    stamp — we compare the live value against the one we stamped at our last
    deploy. Fastly's hash format is undocumented/deprecated and is NOT the
    sha256 of the tarball bytes, so we never compute it locally; we only ever
    compare edge-stamped value to edge-live value."""
    try:
        pkg = fastly("GET", f"/service/{scoring_service_id}/version/{version}/package", token=token) or {}
    except RuntimeError:
        return ""
    meta = (pkg.get("metadata") if isinstance(pkg, dict) else None) or {}
    return meta.get("files_hash") or meta.get("hashsum") or ""


def _deploy_wasm_package(
    scoring_service_id: str,
    matrix_store_id: str,
    token: str,
    status_cb=None,
    *,
    prior_package_sha: str = "",
    prior_files_hash: str = "",
) -> dict[str, Any]:
    """Deploy the prebuilt, matrix-less Wasm package to the scoring service
    via the Fastly API — no Rust/Fastly-CLI toolchain required (the backend
    container has neither). Replaces the old ``deploy_wasm.sh`` subprocess.

    clone latest version → ensure the scoring_matrix KV resource link is
    present on the draft (idempotent; a fresh service inherits it from v1,
    a pre-KV service gets it added here) → upload the committed package →
    activate. The matrix itself is seeded separately into KV by
    ``_write_matrix_to_kv``; this package reads it at runtime.
    """
    info("Deploying prebuilt Wasm package to the scoring Compute service")

    if not _SCORER_PACKAGE.exists():
        raise RuntimeError(
            f"prebuilt scorer package not found at {_SCORER_PACKAGE} — "
            "the Docker image builds it from source (backend/Dockerfile scorer-builder stage), so "
            "rebuild the image; outside Docker run `make scorer-package`"
        )

    # Smart-redeploy skip: if the committed build is unchanged since our last
    # deploy AND the live edge package still matches what we last stamped from
    # the edge, there is nothing to upload — reuse the active version instead of
    # bumping a no-op Compute version. ``get_active_version`` is None for a fresh
    # v1-draft service, so a first enable always deploys.
    # Pass _SCORER_PACKAGE explicitly (not _package_sha's def-time default) so
    # this reads the same module global the upload below uses — keeps the skip
    # sha equal to the sha we'd stamp, and honours a patched package path.
    committed_sha = _package_sha(_SCORER_PACKAGE)
    active = get_active_version(scoring_service_id, token)
    if active is not None and committed_sha and committed_sha == prior_package_sha:
        live_files_hash = _live_package_files_hash(scoring_service_id, active, token)
        if live_files_hash and live_files_hash == prior_files_hash:
            ok(f"Scorer package unchanged at the edge (v{active}) — skipping Wasm upload")
            if status_cb:
                status_cb(f"✅ [Compute] Package already current (v{active}) — skipped upload")
            return {"version": active, "sha": committed_sha, "files_hash": live_files_hash, "skipped": True}

    if status_cb:
        status_cb("⏳ [Compute] Uploading scorer Wasm package (no build)…")

    # Clone the highest existing version → editable draft (uniform for both a
    # fresh v1-draft service and an existing active one; the clone inherits
    # domain/backend/resource-links/package).
    versions = fastly("GET", f"/service/{scoring_service_id}/version", token=token) or []
    base = max(int(v["number"]) for v in versions)
    # Same sequence as before the helper: no /validate call, no rollback (``base`` is the
    # highest version, which for a fresh service is a never-active v1 draft, so there is no
    # known-good version to fall back to), and the activate call passes ``expect_empty``.
    with draft_version(
        scoring_service_id,
        token,
        base_version=base,
        call=fastly,
        validate=False,
        expect_empty_activate=True,
        rollback=False,
    ) as new_ver:
        # Ensure the matrix KV store is linked on this draft. 409/conflict means
        # it was already inherited from the cloned version — that's success.
        if matrix_store_id:
            try:
                fastly(
                    "POST",
                    f"/service/{scoring_service_id}/version/{new_ver}/resource",
                    {"name": MATRIX_RESOURCE_LINK_NAME, "resource_id": matrix_store_id},
                    token=token,
                )
            except RuntimeError as exc:
                if not ("409" in str(exc) or "conflict" in str(exc).lower() or "already" in str(exc).lower()):
                    raise

        # Upload the package (multipart/form-data, field "package") then activate.
        pkg_bytes = _SCORER_PACKAGE.read_bytes()
        boundary = "----fos-scorer-package-boundary"
        body = (
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="package"; filename="session-scorer.tar.gz"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode()
            + pkg_bytes
            + f"\r\n--{boundary}--\r\n".encode()
        )
        fastly_raw(
            "PUT",
            f"/service/{scoring_service_id}/version/{new_ver}/package",
            body,
            content_type=f"multipart/form-data; boundary={boundary}",
            token=token,
        )
    ok(f"Deployed scorer package + activated v{new_ver}")
    if status_cb:
        status_cb(f"✅ [Compute] Activated scorer service v{new_ver}")
    # Return the new Compute version + the sha of the bytes we just uploaded +
    # the edge's own files_hash for the freshly-activated version. The sha is
    # enable_scoring's deployed_package_sha drift stamp (the /scoring/status
    # check compares it to the currently-committed package); files_hash is
    # stamped so a later redeploy can ask the edge "did the live package change
    # since we deployed?" and skip a no-op upload. The version is surfaced in the
    # two-phase SSE log so the operator sees which Compute version went live.
    return {
        "version": new_ver,
        "sha": hashlib.sha256(pkg_bytes).hexdigest(),
        "files_hash": _live_package_files_hash(scoring_service_id, new_ver, token),
        "skipped": False,
    }


def _write_matrix_to_kv(
    matrix_store_id: str,
    logging_service_id: str,
    token: str,
    status_cb=None,
) -> None:
    """Write the tenant's trained matrix into the scoring_matrix KV Store via
    the Fastly API. Resolves the matrix with the existing tenant-scoped chain
    (local ``matrix_{sid}.json`` → FOS). No trained matrix → leave the key
    absent so the scorer's L2 stays disabled (same as the old empty-default
    deploy). This is also exactly what retrain calls to push a new matrix —
    no rebuild, takes effect as Wasm instances recycle.
    """
    if not matrix_store_id:
        warn("no scoring_matrix store id — skipping matrix upload (L2 stays disabled)")
        return
    matrix_path = _resolve_tenant_matrix_for_deploy(logging_service_id)
    if matrix_path is None:
        info("no trained matrix for this tenant — L2 disabled until first retrain")
        return
    # Convert the on-disk JSON matrix to the compact FSM1 binary the Rust scorer
    # reads from KV (~5.6× smaller; decodes with no HashMap build on cold start —
    # Fastly Compute is instance-per-request, so every L2 request is cold). The
    # JSON on disk / FOS is unchanged (Python eval still reads it).
    import json as _json

    from backend.scoring.matrix import serialize_kv

    with matrix_path.open() as f:
        data = serialize_kv(_json.load(f))
    if status_cb:
        status_cb("⏳ [Compute] Writing trained matrix to KV…")
    fastly_raw(
        "PUT",
        f"/resources/stores/kv/{matrix_store_id}/keys/{MATRIX_KEY_NAME}",
        data,
        content_type="application/octet-stream",
        token=token,
    )
    ok(f"Wrote matrix to KV ({len(data)} bytes FSM1) from {matrix_path.name}")


def _remove_scoring_backend(logging_service_id: str, version: int, token: str) -> None:
    """Remove the scoring backend (idempotent — 404 is fine)."""
    encoded = urllib.parse.quote(SCORING_BACKEND_API_NAME, safe="")
    try:
        fastly(
            "DELETE",
            f"/service/{logging_service_id}/version/{version}/backend/{encoded}",
            token=token,
            expect_empty=True,
        )
        ok(f"Removed scoring backend {SCORING_BACKEND_API_NAME}")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("Scoring backend already absent")
        else:
            raise


def _remove_scoring_snippets(logging_service_id: str, version: int, token: str) -> None:
    """Delete the six scoring snippets by name (idempotent)."""
    present = set(list_vcl_snippets(logging_service_id, version, token))
    for name in scoring_snippet_names():
        if name not in present:
            continue
        encoded = urllib.parse.quote(name, safe="")
        try:
            fastly(
                "DELETE",
                f"/service/{logging_service_id}/version/{version}/snippet/{encoded}",
                token=token,
                expect_empty=True,
            )
            ok(f"Removed snippet {name}")
        except RuntimeError as exc:
            if "404" in str(exc):
                continue
            raise


def _add_scoring_custom_fields(cfg: dict) -> dict:
    """Merge the 6 scoring custom_fields into cfg.log_fields.custom_fields.
    Existing fields with the same name are replaced (idempotent re-runs
    pick up any tuning we've done to bytes_estimate / label / etc.)."""
    cfg.setdefault("log_fields", {})
    cfg["log_fields"].setdefault("custom_fields", [])
    cfg["log_fields"]["custom_fields"] = merge_scoring_custom_fields(cfg["log_fields"]["custom_fields"])
    return cfg


def _remove_scoring_custom_fields(cfg: dict) -> dict:
    """Strip the 6 scoring custom_fields from cfg, leaving any others
    untouched."""
    if "log_fields" not in cfg or "custom_fields" not in cfg["log_fields"]:
        return cfg
    cfg["log_fields"]["custom_fields"] = [
        cf for cf in cfg["log_fields"]["custom_fields"] if cf.get("name") not in _SCORING_FIELD_NAMES
    ]
    return cfg


def _publish_scoring_fos_side_effects(logging_service_id: str) -> None:
    """Best-effort republish of the custom_fields list (admin_state.json) + the
    trained matrix to FOS so read-only analyst hosts converge on the next
    import tick. Never fatal — both legs swallow their own errors. Called on
    both the deploy and the redeploy-no-op paths so a skipped redeploy still
    keeps FOS consistent."""
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(logging_service_id)
        ok("Published custom_fields to FOS admin_state.json")
    except Exception as exc:
        warn(f"Could not export admin_state to FOS (non-fatal): {exc}")

    try:
        from backend.state_sync import publish_matrix_to_fos

        tenant_matrix_path = _tenant_matrix_path(logging_service_id)
        if tenant_matrix_path.exists():
            import json as _json

            with tenant_matrix_path.open() as f:
                matrix = _json.load(f)
            publish_matrix_to_fos(logging_service_id, matrix)
            ok(f"Published scoring matrix to FOS (version={matrix.get('version', '?')})")
    except Exception as exc:
        warn(f"Could not publish scoring matrix to FOS (non-fatal): {exc}")


def _delete_scoring_matrix_from_fos(logging_service_id: str) -> None:
    """Inverse of publish_matrix_to_fos: remove the scoring-matrix artifacts a
    disable would otherwise leave behind — the FOS-published current matrix +
    its version history, and the local tenant matrix file. Best-effort; never
    fatal. Without this a future re-enable silently re-adopts the stale matrix
    via fetch_matrix_from_fos / _resolve_tenant_matrix_for_deploy."""
    # Local tenant matrix file (compute/scorer/matrix_{sid}.json).
    try:
        _tenant_matrix_path(logging_service_id).unlink(missing_ok=True)
    except OSError as exc:
        warn(f"Could not remove local tenant matrix file (non-fatal): {exc}")

    # FOS objects: the current matrix + everything under the history prefix.
    try:
        from backend.core.duckdb import _get_fos_client, get_source_for_service
        from backend.core.ingest import _delete_objects_robust
        from backend.state_sync import get_scoring_matrix_key

        source = get_source_for_service(logging_service_id)
        if not source or source.get("access_level") == "read_only":
            return
        s3 = _get_fos_client(source)
        bucket = source["bucket"]
        current_key = get_scoring_matrix_key(source)  # {prefix}/iceberg/meta/scoring_matrix.json
        keys = [current_key]
        history_prefix = f"{current_key.rsplit('/', 1)[0]}/scoring_matrix_history/"
        try:
            paginator = s3.get_paginator("list_objects_v2", caller_hint="scoring_matrix_cleanup")
            for page in paginator.paginate(Bucket=bucket, Prefix=history_prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        except Exception as exc:  # noqa: BLE001 — history listing is best-effort
            warn(f"Could not list scoring matrix history for cleanup (non-fatal): {exc}")
        deleted = _delete_objects_robust(s3, bucket, keys)
        ok(f"Removed {deleted} scoring matrix object(s) from FOS")
    except Exception as exc:  # noqa: BLE001 — FOS cleanup must never break disable
        warn(f"Could not delete scoring matrix from FOS (non-fatal): {exc}")


def enable_scoring(
    logging_service_id: str,
    token: str,
    *,
    status_cb=None,
) -> dict[str, Any]:
    """Provision (or reuse) the Compute scoring service, deploy the Wasm,
    then mutate the customer's VCL service to call it via the restart
    pattern.

    Re-running while scoring is already enabled is the supported REDEPLOY
    path: it re-uploads the committed Wasm package (new Compute version +
    activate) and clones + regenerates + activates the logging VCL. Only the
    identity-style ensure_* helpers (service / stores / backend) are no-ops on
    the happy path — the package upload and VCL activation always run, which is
    how a new scorer build or VCL change reaches the edge.

    Returns:
        {
          "scoring_service_id":      "...",
          "scoring_service_name":    "Session Scoring Service for {id}",
          "scoring_domain":          "fos-...-session-scorer.edgecompute.app",
          "scoring_keys_store_id":   "...",
          "scoring_config_store_id": "...",
          "aes_key_hex":             "..." (only on first creation),
          "logging_service_active_version": int  (post-activate),
        }
    """
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    # ── Stage 1: Compute scoring service + AES key + ConfigStores. ──────────
    info(f"Enabling session scoring for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Enabling session scoring for {logging_service_id}...")
        # Two-phase orientation so the SSE viewer can tell the redeploy does
        # BOTH halves — the Compute scorer service AND the logging service's
        # VCL + log fields. Before this the two phases were indistinguishable
        # in the modal and operators couldn't tell what each step touched.
        status_cb("Redeploy runs two phases: (1) Compute scorer service, then (2) logging service (VCL + log fields).")
        status_cb("▸ Phase 1/2 — Compute scorer service")

    # On a re-run we lose `aes_key_hex` and `request_secret` from
    # ensure_scoring_service (they're write-only in the ConfigStore).
    # Preserve whatever the prior provision stashed in cfg so VCL
    # generation still has the secret available. If neither has one
    # (e.g. the scoring service was provisioned before the secret
    # feature existed), generate a fresh one and PATCH it into the
    # ConfigStore so this enable is self-healing.
    prior_scoring = cfg.get("scoring") or {}

    scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
    scoring_service_id = scoring_meta["scoring_service_id"]
    scoring_domain = scoring_meta["scoring_domain"]
    # Source of truth is the store (ensure_scoring_service now reads it back on
    # reuse); cfg is only a fallback, and a fresh secret is generated if there
    # is genuinely none anywhere.
    request_secret = scoring_meta.get("request_secret") or prior_scoring.get("request_secret") or ""
    if not request_secret:
        import secrets as _secrets

        request_secret = _secrets.token_hex(32)
        info("Generated fresh request_secret (none in store or cfg)")

    # ALWAYS upsert the resolved secret to the store — not just when healing an
    # empty one. The store and the VCL-embedded X-Edge-Scorer-Auth MUST be
    # identical or the scorer 401s every request (100% fail-open). Previously
    # the upsert only ran on the empty path, so a non-empty cfg secret got
    # baked into the VCL without being written to the store, and any drift
    # between them went uncorrected. This guarantees store == VCL on every run.
    keys_store_id = scoring_meta.get("scoring_keys_store_id") or prior_scoring.get("scoring_keys_store_id")
    if not keys_store_id:
        raise RuntimeError("Cannot sync request_secret: no scoring_keys_store_id available.")
    try:
        fastly(
            "POST",
            f"/resources/stores/config/{keys_store_id}/item",
            {"item_key": "request_secret", "item_value": request_secret},
            token=token,
        )
    except RuntimeError:
        fastly(
            "PATCH",
            f"/resources/stores/config/{keys_store_id}/item/request_secret",
            {"item_value": request_secret},
            token=token,
        )
    info("Synced request_secret to scoring_keys store (store == VCL)")

    # ── Stage 2: deploy the prebuilt Wasm package + seed the matrix KV. ─────
    # No toolchain on the backend: the matrix-less package is committed +
    # shipped in the image and pushed via the Fastly API; the trained matrix
    # is written to the scoring_matrix KV Store the scorer reads at runtime.
    matrix_store_id = (
        scoring_meta.get("scoring_matrix_store_id") or (cfg.get("scoring") or {}).get("scoring_matrix_store_id") or ""
    )
    deployed = _deploy_wasm_package(
        scoring_service_id,
        matrix_store_id,
        token,
        status_cb=status_cb,
        # Smart-redeploy: skip the upload when the committed build AND the live
        # edge package are both unchanged since our last deploy.
        prior_package_sha=prior_scoring.get("deployed_package_sha", ""),
        prior_files_hash=prior_scoring.get("deployed_package_files_hash", ""),
    )
    deployed_package_sha = deployed["sha"]
    deployed_files_hash = deployed.get("files_hash", "")
    compute_ver = deployed["version"]
    _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)

    # ── Stage 3: write scoring metadata into the LOGGING service config. ────
    # Preserve operator-tunable overrides across re-enables — the previous
    # implementation replaced the entire ``scoring`` block, silently wiping
    # the operator's per-service exclude_url_regex and enforce_status_code.
    # Pull them off the pre-existing block before the replace.
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    def _apply_scoring(target: dict) -> None:
        # Applied on a fresh read under the config lock: Stages 1-2 above ran for minutes, and
        # only the ``scoring`` block + scoring custom_fields are ours to write. The operator
        # overrides are re-read here so a change made mid-deploy is not reverted.
        fresh_prior = target.get("scoring") or {}
        target["scoring"] = _build_scoring_block(fresh_prior)
        _add_scoring_custom_fields(target)

    def _build_scoring_block(prior_scoring: dict) -> dict[str, Any]:
        block = {
            "enabled": True,
            "scoring_service_id": scoring_service_id,
            "scoring_service_name": scoring_meta["scoring_service_name"],
            "scoring_domain": scoring_domain,
            "scoring_keys_store_id": scoring_meta["scoring_keys_store_id"],
            "scoring_config_store_id": scoring_meta["scoring_config_store_id"],
            "scoring_matrix_store_id": matrix_store_id or scoring_meta.get("scoring_matrix_store_id", ""),
            # Stash the secret here so re-runs of enable_scoring can recover
            # it (the ConfigStore is write-only from our perspective). The
            # config file is gitignored under /configs/* so this never leaks.
            "request_secret": request_secret,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            # Identity of the scorer build we just pushed to the edge. /scoring/status
            # recomputes these from the committed package + VCL generator and flags
            # drift when they differ, so the operator knows a redeploy is needed.
            "deployed_package_sha": deployed_package_sha,
            # The edge's own integrity hash for the live Compute package, stamped so
            # a later redeploy can ask the edge "did the live package change since
            # we deployed?" (manual-deploy drift) and skip a no-op upload.
            "deployed_package_files_hash": deployed_files_hash,
            "deployed_vcl_sha": scorer_vcl_fingerprint(logging_service_id),
            # First-enable defaults: persist the actual values so the admin UI
            # shows what's actually in use (no empty-as-sentinel cleverness)
            # and so a future change to the bundled default doesn't silently
            # alter the per-service behaviour.
            "exclude_url_regex": prior_scoring.get("exclude_url_regex") or DEFAULT_ASSET_EXT_REGEX,
        }
        # Preserve any operator-set enforce_status_code override across the
        # block replace; absence means "use the bundled default 429" — there's
        # no need to materialise the default since the enforce snippet's
        # default arg already covers it.
        if prior_scoring.get("enforce_status_code") is not None:
            block["enforce_status_code"] = prior_scoring["enforce_status_code"]
        return block

    # The scoring custom_fields are added so update_logging_endpoint picks them up.
    if svcconfig.mutate_config(logging_service_id, _apply_scoring) is None:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")
    n_scoring = len(_SCORING_FIELD_NAMES)
    ok(f"Stashed scoring metadata + {n_scoring} custom_fields into service config")

    # ── Phase 2/2: Logging Service (Declarative VCL + log fields) ───────────
    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")

    if status_cb:
        status_cb("▸ Phase 2/2 — Logging service (VCL + log fields) via Declarative Reconciler")

    try:
        from backend.provision.declarative.reconciler import reconcile_vcl_state

        res = reconcile_vcl_state(logging_service_id, token, status_cb=status_cb)
        if res.error:
            raise RuntimeError(f"Reconciliation failed: {res.error}")

        new_ver = res.activated_version or active_ver
        if status_cb:
            status_cb(f"✅ Done — Compute scorer v{compute_ver} + logging VCL v{new_ver} are now live.")

        scoring_meta["logging_service_active_version"] = new_ver
        _publish_scoring_fos_side_effects(logging_service_id)
        return scoring_meta

    except Exception as exc:
        fail(f"enable_scoring failed: {exc}")

        # Also revert the on-disk config so a retry starts from clean state.
        def _revert(target: dict) -> None:
            target.pop("scoring", None)
            _remove_scoring_custom_fields(target)

        svcconfig.mutate_config(logging_service_id, _revert)
        raise


def disable_scoring(
    logging_service_id: str,
    token: str,
    *,
    status_cb=None,
) -> None:
    """Tear down session scoring for this customer.

    Reverse of enable_scoring: clone active VCL → remove the 6 scoring
    snippets + scoring backend → strip the 6 custom_fields → regenerate
    log format → validate → activate → delete the scoring Compute
    service + ConfigStores. Idempotent — 404s tolerated everywhere."""
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        warn("Session scoring is not enabled for this service — nothing to disable")
        if status_cb:
            status_cb("✅ Session scoring already disabled.")
        return

    scoring_service_id = scoring.get("scoring_service_id", "")
    scoring_keys_store_id = scoring.get("scoring_keys_store_id", "")
    scoring_config_store_id = scoring.get("scoring_config_store_id", "")
    scoring_matrix_store_id = scoring.get("scoring_matrix_store_id", "")

    info(f"Disabling session scoring for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Disabling session scoring for {logging_service_id}...")

    # ── Stage 1: clone active version. ──────────────────────────────────────
    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Stage 4 (validate + activate) is owned by draft_version. ───────────
    with draft_version(
        logging_service_id,
        token,
        base_version=active_ver,
        call=fastly,
        comment=f"Disable session scoring {ts}",
        on_failure=lambda exc: fail(f"disable_scoring VCL phase failed: {exc}"),
    ) as new_ver:
        # ── Stage 2: remove scoring VCL bits. ───────────────────────────────
        _remove_scoring_snippets(logging_service_id, new_ver, token)
        _remove_scoring_backend(logging_service_id, new_ver, token)

        # ── Stage 3: drop the 6 custom_fields + regen log format. ───────────
        def _strip_fields(target: dict) -> None:
            _remove_scoring_custom_fields(target)

        stripped = svcconfig.mutate_config(logging_service_id, _strip_fields)
        if stripped is not None:
            cfg = stripped

        from backend.core.fastly.client import list_s3_endpoints
        from backend.provision.fastly_api import generate_capture_vcl, load_log_format

        capture = generate_capture_vcl(cfg.get("log_fields"))
        # Re-install (or remove if no fields left) the capture VCL.
        ensure_vcl_snippet(
            "Fastly Log Analysis Capture",
            "recv",
            capture["recv"],
            1,
            logging_service_id,
            new_ver,
            token,
        )
        # Re-install the low-priority Reset Client IP snippet alongside it so the
        # edge-only Fastly-Client-IP reset stays in place after scoring teardown.
        if "recv_reset" in capture:
            ensure_vcl_snippet(
                "Fastly Log Analysis Reset Client IP",
                "recv",
                capture["recv_reset"],
                -100,
                logging_service_id,
                new_ver,
                token,
            )

        endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
        existing_endpoints = list_s3_endpoints(logging_service_id, new_ver, token)
        if endpoint_name in existing_endpoints:
            new_format = load_log_format(cfg.get("log_fields"))
            encoded = urllib.parse.quote(endpoint_name, safe="")
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{new_ver}/logging/s3/{encoded}",
                {"format": new_format, "format_version": 2},
                token=token,
            )

    ok(f"Logging service version {new_ver} active (scoring stripped)")

    # ── Stage 5: tear down the Compute service + stores. ───────────────────
    failed_stores = delete_scoring_service(
        scoring_service_id,
        scoring_keys_store_id=scoring_keys_store_id,
        scoring_config_store_id=scoring_config_store_id,
        scoring_matrix_store_id=scoring_matrix_store_id,
        token=token,
        status_cb=status_cb,
    )

    # ── Stage 5.5: remove the FOS-published matrix + local tenant file. ─────
    # The config still exists here (unlike the full-teardown path), so the FOS
    # credentials resolve normally. Best-effort — never aborts the disable.
    _delete_scoring_matrix_from_fos(logging_service_id)

    # ── Stage 6: clear the scoring block from config. ──────────────────────
    # The Fastly + Compute teardown stages above can take 60-120s, so only the
    # ``scoring`` key is dropped, on a fresh read under the config lock: a stale
    # snapshot would clobber concurrent writer mutations (metadata_sync tick,
    # custom_fields PATCH, ngwaf_workspace_id update).
    svcconfig.mutate_config(logging_service_id, lambda target: target.pop("scoring", None))

    # Publish the new custom_fields list (now without scoring) so analyst
    # boxes stop seeing the scoring entries on their next metadata_sync.
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(logging_service_id)
    except Exception as exc:
        warn(f"Could not export admin_state to FOS after disable (non-fatal): {exc}")

    # Surface any store that genuinely failed to delete (its id is now gone from
    # cfg, so it can't be auto-retried — the operator must hand-clean it).
    for label, store_id in failed_stores:
        warn(f"Scoring {label} store {store_id} was NOT deleted — remove it manually in Fastly.")
        if status_cb:
            status_cb(f"⚠️ Scoring {label} store {store_id} not deleted — remove it manually.")

    if status_cb:
        status_cb("✅ Session scoring disabled.")
    ok("Session scoring disabled")


def teardown_scoring_resources(
    logging_service_id: str,
    scoring_meta: dict,
    token: str,
    *,
    status_cb=None,
) -> list[tuple[str, str]]:
    """Tear down scoring for a service that is being FULLY DESTROYED.

    Unlike ``disable_scoring`` this NEVER touches the service config (the
    teardown path deletes ``configs/{sid}.json`` before this runs, so there's
    nothing to load/pop/save) and never re-exports admin_state (the whole
    service is going away). It only does the two irreducible Fastly-side jobs:

      1. Strip the scoring VCL (the 6 scoring snippets + the ``session_scorer``
         backend) from the customer's still-live logging service on a fresh
         cloned+activated version, so no dangling backend/snippets reference a
         deleted Compute service. Skipped cleanly if the logging service has no
         active version (already gone).
      2. Delete the owned resources via ``delete_scoring_service`` (Compute
         service first, then both ConfigStores, then the matrix KV key+store) —
         AFTER the scoring-stripped version is active, so the live edge never
         references a backend whose host is already deleted.

    ``scoring_meta`` is the ``cfg['scoring']`` block carried in the teardown
    ``state`` dict. Idempotent / 404-tolerant throughout. Returns the list of
    ``(label, store_id)`` pairs that genuinely failed to delete (non-404) so the
    caller can surface them for manual cleanup; empty on full success.
    """
    info(f"Tearing down session scoring for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Tearing down session scoring for {logging_service_id}...")

    # ── Stage 1: strip the scoring VCL from the still-live logging service. ──
    # MUST succeed before Stage 2 deletes the Compute service. A dangling
    # ``session_scorer`` backend on an ACTIVE version points at a deleted host,
    # so every request the customer's service pre-flights to the scorer fails —
    # an outage. This used to be best-effort ("the operator cares most about not
    # paying for an orphaned Compute service"), which has the trade backwards:
    # an orphaned Compute service costs pennies and is trivially deleted later,
    # a dangling backend on live traffic is an incident. Fastly 500s on these
    # calls are real (observed 2026-08-12), so this path is reachable.
    #
    # get_active_version returns None if the service is already gone — that is
    # NOT a failure, there is nothing left to dereference.
    strip_ok = False
    try:
        active_ver = get_active_version(logging_service_id, token)
        if active_ver is None:
            if status_cb:
                status_cb("Logging service has no active version — skipping VCL strip.")
        else:
            ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            # rollback=False: nothing went live until the final activate, so a failure
            # leaves the current version serving; teardown aborts below instead.
            with draft_version(
                logging_service_id,
                token,
                base_version=active_ver,
                call=fastly,
                comment=f"Teardown: strip session scoring {ts}",
                rollback=False,
            ) as new_ver:
                _remove_scoring_snippets(logging_service_id, new_ver, token)
                _remove_scoring_backend(logging_service_id, new_ver, token)
            ok(f"Logging service version {new_ver} active (scoring VCL stripped)")
        strip_ok = True
    except Exception as exc:
        warn(f"Could not strip scoring VCL during teardown: {exc}")
        if status_cb:
            status_cb(f"❌ Could not strip scoring VCL: {exc}")

    if not strip_ok:
        # ABORT before deleting the Compute service. Deleting it now would leave
        # the customer's ACTIVE version pre-flighting to a backend whose host no
        # longer exists. The Compute service is left in place deliberately: it
        # is cheap, harmless and re-deletable, whereas a dangling backend on
        # live traffic is an outage. Re-run teardown once the cause is cleared.
        raise RuntimeError(
            "Aborting scoring teardown: the scoring VCL could not be stripped from "
            f"{logging_service_id}, so its active version may still route to the scorer. "
            "The Compute service was NOT deleted (deleting it now would break live traffic). "
            "Resolve the error above and re-run teardown."
        )

    # ── Stage 2: delete the owned Compute service + stores. ─────────────────
    failed = delete_scoring_service(
        scoring_meta.get("scoring_service_id", ""),
        scoring_keys_store_id=scoring_meta.get("scoring_keys_store_id", ""),
        scoring_config_store_id=scoring_meta.get("scoring_config_store_id", ""),
        scoring_matrix_store_id=scoring_meta.get("scoring_matrix_store_id", ""),
        token=token,
        status_cb=status_cb,
    )
    return failed or []


def _clone_swap_snippet(
    sid: str,
    token: str,
    *,
    active_ver: int,
    comment: str,
    snippet_name: str,
    vcl_type: str,
    body: str,
    priority: int,
) -> int:
    """Clone ``active_ver`` → set its comment → install ONE VCL snippet →
    validate → activate, returning the new active version.

    Shared lifecycle for ``update_recv_exclusion_regex`` /
    ``update_enforce_status_code``: same clone/comment/ensure/validate/
    activate call sequence, only the snippet identity + comment differ.
    Emits NO log lines (callers keep their own info()/ok()/fail()) and does
    NOT roll back (``rollback=False``) — the caller owns the rollback so it can
    log fail() in the exact pre-rollback order the originals used. Raises
    ``RuntimeError`` on a non-``ok`` validate result, matching the inline form.
    """
    with draft_version(
        sid,
        token,
        base_version=active_ver,
        call=fastly,
        comment=comment,
        rollback=False,
    ) as new_ver:
        ensure_vcl_snippet(
            snippet_name,
            vcl_type,
            body,
            priority,
            sid,
            new_ver,
            token,
        )
    return new_ver


def _update_live_scoring_snippet(
    logging_service_id: str,
    token: str,
    *,
    fn_name: str,
    setting_label: str,
    scoring_key: str,
    scoring_value: Any,
    snippet_name: str,
    vcl_type: str,
    body: str,
    priority: int,
    info_label: str,
    comment_fn: Callable[[str], str],
    ok_suffix: str,
) -> int:
    def _mutate(target: dict) -> None:
        scoring = target.get("scoring") or {}
        if not scoring.get("enabled"):
            raise RuntimeError(
                f"Session scoring is not enabled for {logging_service_id}; "
                f"run enable_scoring first before customising the {setting_label}."
            )
        scoring[scoring_key] = scoring_value
        target["scoring"] = scoring

    if svcconfig.mutate_config(logging_service_id, _mutate) is None:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")

    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    info(f"Cloning version {active_ver} to update {info_label}")
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        new_ver = _clone_swap_snippet(
            logging_service_id,
            token,
            active_ver=active_ver,
            comment=comment_fn(ts),
            snippet_name=snippet_name,
            vcl_type=vcl_type,
            body=body,
            priority=priority,
        )
        ok(f"Logging service version {new_ver} active ({ok_suffix})")
        return new_ver
    except Exception as exc:
        fail(f"{fn_name} failed: {exc}")
        rollback_to_version(logging_service_id, token, active_ver, call=fastly)
        raise


def update_recv_exclusion_regex(
    logging_service_id: str,
    token: str,
    *,
    new_regex: str,
) -> dict[str, Any]:
    """Re-publish ONLY the recv VCL snippet with a new exclusion regex.

    Lighter-weight than running ``enable_scoring`` end-to-end: we keep
    the existing Compute service / ConfigStores / Wasm / log-format
    untouched, and ONLY clone the active VCL version → swap the recv
    snippet body → activate. Takes ~5-10s in practice.

    ``new_regex`` is the operator's pre-validated regex string (already
    passed through ``backend.utils.vcl_validator.validate_url_exclusion_regex``
    + falco lint by the API layer). Empty string means "use the default"
    and persists as ``None`` in cfg so a future default change auto-picks-up.

    Returns:
        {
          "effective_regex": str,              # what got interpolated
          "is_default": bool,
          "logging_service_active_version": int,  # post-activate
        }

    Raises ``RuntimeError`` on any Fastly API failure; the rollback path
    re-activates the prior version so the service is never left in an
    inconsistent state.
    """
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        recv_snippet,
        resolve_exclude_url_regex,
    )

    cleaned = (new_regex or "").strip()
    effective_regex = resolve_exclude_url_regex(cleaned or None)
    is_default = effective_regex == DEFAULT_ASSET_EXT_REGEX
    new_recv_body = recv_snippet(logging_service_id, exclude_url_regex=cleaned or None)

    new_ver = _update_live_scoring_snippet(
        logging_service_id,
        token,
        fn_name="update_recv_exclusion_regex",
        setting_label="recv exclusion regex",
        scoring_key="exclude_url_regex",
        scoring_value=cleaned or None,
        snippet_name=SCORING_RECV_NAME,
        vcl_type="recv",
        body=new_recv_body,
        priority=SCORING_SNIPPET_PRIORITY,
        info_label="recv-snippet exclusion regex",
        comment_fn=lambda ts: f"Update scoring recv exclusion regex {ts}",
        ok_suffix="recv exclusion regex updated",
    )

    return {
        "effective_regex": effective_regex,
        "is_default": is_default,
        "logging_service_active_version": new_ver,
    }


def update_enforce_status_code(
    logging_service_id: str,
    token: str,
    *,
    new_status_code: int | None,
) -> dict[str, Any]:
    """Re-publish ONLY the enforce VCL snippet with a new status code.

    Mirrors ``update_recv_exclusion_regex``: clone the active version,
    swap the enforce snippet body, validate, activate. Takes ~5-10s.

    ``new_status_code`` is the operator's pre-validated int (400-599) or
    ``None`` to reset to the default 429. The PUT endpoint validates the
    range BEFORE calling here; this function defends with
    ``resolve_enforce_status_code`` but trusts its caller.

    Returns:
        {
          "effective_status_code": int,
          "is_default": bool,
          "logging_service_active_version": int,
        }

    Raises ``RuntimeError`` on any Fastly API failure; the rollback path
    re-activates the prior version so the service is never left in an
    inconsistent state.
    """
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ENFORCE_STATUS_CODE,
        enforce_snippet,
        resolve_enforce_status_code,
    )

    effective_code = resolve_enforce_status_code(new_status_code)
    is_default = effective_code == DEFAULT_ENFORCE_STATUS_CODE
    new_enforce_body = enforce_snippet(effective_code)

    new_ver = _update_live_scoring_snippet(
        logging_service_id,
        token,
        fn_name="update_enforce_status_code",
        setting_label="enforce status code",
        scoring_key="enforce_status_code",
        scoring_value=None if is_default else effective_code,
        snippet_name=SCORING_ENFORCE_NAME,
        vcl_type="recv",
        body=new_enforce_body,
        priority=SCORING_SNIPPET_PRIORITY + 1,
        info_label=f"enforce-snippet status code → {effective_code}",
        comment_fn=lambda ts: f"Update scoring enforce status code → {effective_code} ({ts})",
        ok_suffix=f"enforce status code → {effective_code}",
    )

    return {
        "effective_status_code": effective_code,
        "is_default": is_default,
        "logging_service_active_version": new_ver,
    }


# ============================================================================
# Session Scoring VCL Generation (folded from session_scoring_vcl.py)
# ============================================================================

"""VCL snippet generator for the session-scoring restart pattern.

Adapted from the canonical Fastly preflight pattern (fiddle 4b1a74ee).
Six snippets — recv / pass / fetch / miss / deliver / enforce — coordinate to:

  1. recv:    on first pass, route to the scorer Compute backend with
              X-Edge-Scoring-Pass=1, return(pass).
  2. pass:    inject the auth + service-id headers on bereq for the
              upcoming scorer sub-fetch (pass is the correct subroutine
              for bereq header mutations under return(pass)).
  3. fetch:   when the backend is the scorer, return(deliver) to skip
              cache + go straight to deliver with the scorer response.
  4. deliver: pass-1 captures the eight scorer values (score, l1, l2,
              compliance, reason, sid, enforce, exec) + the VCL-computed
              round-trip (rtt) + the rotated Set-Cookie into subfields of
              req.http.x-edge-score (single consolidated header — ten
              subfields total), unsets the resp.http.x-edge-* leaks, and
              issues a naked `restart`. pass-2 emits the rotated cookie via
              `add resp.http.Set-Cookie` (additive — preserves origin cookies).
  5. miss:    unset bereq.http.x-edge-score + X-Edge-Scoring-Pass so
              neither leaks to the real origin on pass 2.
  6. enforce: on the post-scoring restart, error 429 when the scorer
              emitted X-Edge-Score-Enforce=1 (operator committed an
              enforce_threshold and the request's score met it).

**Storage strategy.** All eight scoring values (plus the VCL-computed
``rtt`` and the rotated Set-Cookie) are stored as SUBFIELDS of
``req.http.x-edge-score`` — single consolidated header keeps the
per-request header budget small. Log format reads the subfields via
``req.http.x-edge-score:score`` etc.

**Why restart from vcl_deliver.** Empirically (v440), req.http
modifications made in vcl_fetch before `return(restart)` are invisible
to Fastly's log-format evaluator. Restarting from vcl_deliver after
writing the subfields is the working pattern.

Fail-open contract: any error reaching the scorer (5xx, timeout, DNS
failure) sets ``req.http.x-edge-score:score = "0"`` and
``req.http.x-edge-score:compliance = "unknown"`` so the request flows
normally to origin and the log line still has populated score fields
(vs. NULLs that look like a misconfiguration).
"""


from backend.utils.vcl_utils import VclSafetyError, validate_quoted_literal


def _assert_vcl_string_safe(value: str, *, field: str, allow_backslash: bool = False) -> str:
    """Raise ``ValueError`` if ``value`` can't be safely embedded in a VCL
    double-quoted string literal; return it unchanged otherwise. See the module
    note above — this is a defense-in-depth guard at the generation boundary, not
    the primary validation (that lives in the persisted-write + Fastly /validate
    path). ``allow_backslash`` for regex inputs that legitimately contain ``\\``.

    Delegates to the unified :mod:`backend.utils.vcl_safety` char policy so the
    forbidden-char rules can't drift from the other generators/validators.
    ``VclSafetyError`` subclasses ``ValueError`` so existing callers catching
    ``ValueError`` are unaffected."""
    try:
        return validate_quoted_literal(value, allow_backslash=allow_backslash, field=field)
    except VclSafetyError as exc:
        raise ValueError(str(exc)) from exc


# Backend name handling. Fastly's API creates a backend whose VCL-visible
# name is "F_" + the raw name you submitted. So:
#   - SCORING_BACKEND_API_NAME is what we POST to /backend  → "session_scorer"
#   - SCORING_BACKEND_VCL_NAME is what VCL sees             → "F_session_scorer"
SCORING_BACKEND_API_NAME = "session_scorer"
SCORING_BACKEND_VCL_NAME = f"F_{SCORING_BACKEND_API_NAME}"

# Snippet names. Stable string constants so disable_scoring can find and
# remove the exact snippets by name. Fastly only accepts
# [A-Za-z0-9_. -] in snippet names — no colons, slashes, or other
# punctuation.
SCORING_RECV_NAME = "Session Scoring - Recv"
SCORING_PASS_NAME = "Session Scoring - Pass"
SCORING_FETCH_NAME = "Session Scoring - Fetch"
SCORING_DELIVER_NAME = "Session Scoring - Deliver"
SCORING_MISS_NAME = "Session Scoring - Miss"
SCORING_ENFORCE_NAME = "Session Scoring - Enforce"

# Snippet priority — lower runs first. 100 is the "after everything
# else" slot used by most user snippets on this service.
SCORING_SNIPPET_PRIORITY = 100


# Default asset-extension regex: requests whose URL matches this regex
# bypass the scorer entirely. Static assets carry no session signal and
# routing them through Compute is wasted cost + capacity.
#
# This is the DEFAULT. Operators can override it per-service via the
# Session Scoring admin page; the operator-supplied value lives in the
# service config under ``scoring.exclude_url_regex`` and is interpolated
# into the recv snippet by ``recv_snippet`` below. An empty / unset
# override falls back to this default.
DEFAULT_ASSET_EXT_REGEX = (
    # Anchored at the start AND restricted to the path segment via
    # ``[^?#;]*`` (any non-``?``, ``#``, or ``;`` chars). Without the anchor + path-only
    # restriction, ``/api/login?file=.png`` would also match — the
    # extension test would see ``.png`` in the query string and skip
    # scoring entirely, letting an attacker bypass session scoring on
    # any dynamic endpoint by appending an asset extension to the
    # query string. The fix bounds the match to the URL path.
    r"^[^?#;]*"
    r"\.(aif|aiff|au|avi|bin|bmp|cab|carb|cct|cdf|class|css|dcr|doc|"
    r"dtd|exe|flv|gcf|gff|gif|grv|hdml|hqx|ico|ini|jpeg|jpg|js|mov|"
    r"mp3|mp4|nc|pct|pdf|png|ppc|pws|svg|swa|swf|txt|vbs|w32|wav|"
    r"wbmp|wml|wmlc|wmls|wmlsc|xsd|zip|webp|woff|woff2|ttf|bz2|gz|"
    r"tgz|tar|lzma|rar|war|bz|7z|ts|m3u8)($|\?|#)"
)


def resolve_exclude_url_regex(operator_override: str | None) -> str:
    """Pick between the operator's override and the built-in default.

    Empty / None / whitespace-only → default. The operator-facing API
    interprets the empty string as "I want the default" — same shape
    as Pydantic optional-field handling.
    """
    if operator_override is None:
        return DEFAULT_ASSET_EXT_REGEX
    cleaned = operator_override.strip()
    return cleaned or DEFAULT_ASSET_EXT_REGEX


# Default HTTP status code returned by the enforce snippet when the scorer
# flags a request. Operator-overridable via cfg.scoring.enforce_status_code;
# bake-into-VCL at deploy so each change does a snippet swap (see
# update_enforce_status_code orchestrator) rather than needing a
# ConfigStore-to-VCL binding for a value that changes rarely.
DEFAULT_ENFORCE_STATUS_CODE = 429

# Allowed range — anything outside 4xx/5xx makes no sense for "reject".
_ENFORCE_STATUS_CODE_MIN = 400
_ENFORCE_STATUS_CODE_MAX = 599


def enforce_reason_phrase(status_code: int) -> str:
    """HTTP reason phrase for the enforce snippet's synthetic body.

    Delegates to Python's ``http.HTTPStatus`` so any IANA-registered code
    yields its canonical phrase (403 → "Forbidden", 451 → "Unavailable
    For Legal Reasons", 511 → "Network Authentication Required", …).
    Non-standard codes the operator might pick (419, 444, 530, 599) fall
    back to ``"Blocked"`` — keeps the synthetic body meaningful even when
    the stdlib map doesn't know the code."""
    import http

    try:
        return http.HTTPStatus(status_code).phrase
    except ValueError:
        return "Blocked"


def resolve_enforce_status_code(operator_override: int | None) -> int:
    """Pick the effective enforce status code. None / out-of-range → default.

    The PUT endpoint validates the operator's input before persistence,
    so out-of-range here means a stale or corrupted cfg — fall back to
    the safe default rather than baking a nonsensical code into VCL."""
    if operator_override is None:
        return DEFAULT_ENFORCE_STATUS_CODE
    try:
        code = int(operator_override)
    except (TypeError, ValueError):
        return DEFAULT_ENFORCE_STATUS_CODE
    if not (_ENFORCE_STATUS_CODE_MIN <= code <= _ENFORCE_STATUS_CODE_MAX):
        return DEFAULT_ENFORCE_STATUS_CODE
    return code


def recv_snippet(
    logging_service_id: str,
    *,
    exclude_url_regex: str | None = None,
) -> str:
    """vcl_recv snippet: at the EDGE on the first pass (no shield hop yet
    AND req.restarts == 0 AND scoring-pass marker not set AND URL doesn't
    match the exclusion regex), route to the scorer Compute backend with
    X-Edge-Scoring-Pass=1, `return(pass)`. After the scoring restart
    completes (req.restarts == 1) and the score was captured, re-enable
    shielding for the real-origin pass so the cached object can be
    served from the shield POP normally.

    ``exclude_url_regex`` is the operator-supplied regex of URLs to
    SKIP from scoring. None or "" falls back to ``DEFAULT_ASSET_EXT_REGEX``.
    The caller (orchestrator) is responsible for having validated the
    regex via backend.utils.vcl_validator BEFORE getting here — this
    function trusts its input and string-substitutes verbatim into the
    VCL boolean expression.

    The edge/shield boundary is detected with
    ``fastly.ff.visits_this_service == 0`` — true only at the first Fastly
    POP to handle the request (the true edge). A client cannot forge it:
    each ``Fastly-FF`` entry is a salted hash only genuine Fastly hops can
    produce, so an inbound ``Fastly-FF`` header doesn't move this counter.
    Stays correct even when the nearest POP is a shield (that POP is simply
    the first hop, so the counter is still 0 there).

    Note: `logging_service_id` is kept as an argument for symmetry with
    peer snippet generators."""
    _ = logging_service_id
    effective_regex = resolve_exclude_url_regex(exclude_url_regex)
    # EC-07: belt-and-suspenders — reject a regex that would break out of the VCL
    # string literal it's substituted into below (regex allows backslash).
    _assert_vcl_string_safe(effective_regex, field="exclude_url_regex", allow_backslash=True)
    return f"""# Session Scoring: client-edge header scrub (anti-spoofing).
# Edge-only (fastly.ff.visits_this_service == 0, unforgeable) so any
# client-supplied X-Edge-* gets stripped before it can be forged into a
# clean score.
if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{
  unset req.http.X-Edge-Scoring-Pass;
  unset req.http.X-Edge-Score;
  unset req.http.X-Edge-Score-Reason;
  unset req.http.X-Edge-Score-Enforce;
  unset req.http.X-Edge-Sid;
  unset req.http.X-Edge-Score-Set-Cookie;
  # Defense-in-depth for the L2 skip-gram anchor. The scorer already forces
  # prev_anchor to None (compute/scorer/src/main.rs), but a client-supplied
  # X-Edge-Prev-Anchor must never reach Compute regardless: a SEEN
  # high-probability anchor would otherwise raise the L2 transition prob and
  # depress the sequence-anomaly score (the audit F009 evasion class, which
  # the codebase already closed for the sibling X-Edge-Prev-Route header).
  unset req.http.X-Edge-Prev-Anchor;
  # NGWAF skip-inspection bypass guard. Only WE may set
  # x-sigsci-skip-inspection-once (below, on the scoring sub-fetch) — strip any
  # client-supplied copy here so a client can never skip the Next-Gen WAF.
  # NGWAF's own ngwaf_config_init recv code also strips it on sampled requests
  # (and runs before this snippet), but unset here too for defense in depth.
  unset req.http.x-sigsci-skip-inspection-once;
}}

# Session Scoring: route the first-pass dynamic request to the scorer.
# Edge-only — fastly.ff.visits_this_service == 0 is true only at the true
# edge; at a shield POP it is > 0, so the shield skips this block and the
# real-origin pass is served from the shield normally. Unforgeable, so a
# client cannot fake having already transited our edge.
#
# DDoS bypass (fastly.ddos_detected): when Fastly's L7 DDoS detection
# flags this request, do NOT route to Compute. Two reasons:
#   1. Cost ceiling — under attack, Compute invocations scale linearly
#      with attack volume. Skipping flagged requests caps the blast
#      radius while NGWAF / Fastly's mitigation handles the actual block.
#   2. Signal quality — the scorer's L2 transition matrix learns from
#      benign traffic shapes; feeding attack traffic in pollutes the
#      matrix even though those scores wouldn't be acted on.
# See: https://www.fastly.com/documentation/reference/vcl/variables/miscellaneous/fastly-ddos-detected/
if (fastly.ff.visits_this_service == 0 && req.restarts == 0 && req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected && std.tolower(req.url) !~ "{effective_regex}") {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  # Skip NGWAF inspection on the scoring sub-fetch ONLY. The scorer is
  # payload-agnostic — it strips the query string and scores the path — so
  # inspecting this preflight hop is pure cost, and worse: NGWAF 406s attack
  # URLs here, turning every blocked request into a scorer fail-open
  # (compute-unavailable-406) even though the scorer never sees the payload.
  # NGWAF's edge_security (vcl_miss/pass, priority 150) reads this on bereq
  # AFTER our Session Scoring - Pass (priority 100), skips inspection, and
  # self-unsets its bereq copy. The real-origin pass after the restart stays
  # fully inspected — the persisted req.http copy is unset on the restart
  # path below.
  set req.http.x-sigsci-skip-inspection-once = "true";
  set req.http.X-Edge-Scoring-Pass = "1";
  # Stamp the round-trip start (µs since request receipt) so pass-1 deliver
  # can compute edge-observed scorer latency (edge_score_rtt_us). Mirrors
  # the x-of-start TTFB/TTLB idiom in fastly_api.generate_capture_vcl —
  # that timer deliberately SKIPS the scoring sub-fetch, so this stamp is
  # the only place the scorer leg is timed.
  set req.http.x-edge-score-t0 = time.elapsed.usec;
  # PASS — skip cache for the scoring sub-fetch. On the post-restart
  # pass the scoring snippet doesn't re-fire because X-Edge-Scoring-Pass
  # got unset in pass-1 deliver and req.restarts is now 1.
  return(pass);
}}

# Post-scoring restart: we captured the score in pass-1 deliver and the
# request flow is now headed for the real origin. Without this block,
# the previous `return(pass)` would have permanently disabled shielding
# for this request — re-enable it so the real-origin fetch can land on
# the shield POP normally. `var.fastly_req_do_shield` is the magic
# variable Fastly's auto-generated main VCL reads to decide whether to
# shield the request.
if (req.restarts == 1 && req.http.x-edge-score) {{
  set var.fastly_req_do_shield = true;
}}
if (req.restarts == 1) {{
  # Real-origin pass — ensure NGWAF inspects it. We set
  # x-sigsci-skip-inspection-once on the scoring sub-fetch (restarts == 0);
  # edge_security unsets only its bereq copy, so the req.http value persists
  # across the restart. Unset it here so attack traffic to the real origin is
  # never skipped — even on scorer fail-open.
  unset req.http.x-sigsci-skip-inspection-once;
}}"""


def pass_snippet(logging_service_id: str, request_secret: str) -> str:
    """vcl_pass snippet: when this is the scoring sub-fetch (backend ==
    scorer), rewrite bereq.method to GET so the client request body stays
    buffered at the edge for the restarts == 1 origin pass (paired with the
    service's preflight body preservation pragmas), and inject the auth +
    service-id headers on bereq for the upcoming sub-fetch. Also unset
    bereq.http.x-edge-score so any attacker-supplied inbound x-edge-score
    doesn't get echoed into the scorer's view of the request."""
    _assert_vcl_string_safe(request_secret, field="request_secret")  # EC-07
    return f"""# Session Scoring: inject auth + service-id on the scorer sub-fetch.
# vcl_pass is the right subroutine for bereq mutations when recv used
# return(pass).
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  # Rewrite the preflight sub-fetch to GET so Fastly does not stream the
  # inbound request body to the Compute scorer, preserving the body for
  # the restarts == 1 origin pass.
  set bereq.method = "GET";
  set bereq.http.X-Edge-Service-Id = "{logging_service_id}";
  # Shared-secret header — the scorer compares this to the
  # request_secret ConfigStore entry and 401s on mismatch. Embedded
  # literally in VCL which is compiled and never sent to clients.
  set bereq.http.X-Edge-Scorer-Auth = "{request_secret}";
  # X-Edge-Scoring-Pass is an internal marker; the scorer doesn't need
  # to see it and we don't want it polluting any downstream telemetry.
  unset bereq.http.X-Edge-Scoring-Pass;
}}
# Strip any inbound x-edge-score header an attacker may have set; the
# real one is built by us in vcl_deliver after the scorer responds.
unset bereq.http.x-edge-score;"""


def fetch_snippet() -> str:
    """vcl_fetch snippet: when the backend is the scorer, return(deliver)
    so the response goes straight to deliver without any cache-related
    handling. (return(pass) in recv already prevents caching, but
    return(deliver) here is the canonical preflight-pattern shape and
    avoids any weird interactions with beresp's TTL.)"""
    return f"""# Session Scoring: skip cache handling for the scorer sub-fetch.
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  return(deliver);
}}"""


def deliver_snippet() -> str:
    """vcl_deliver snippet — the heart of the pattern.

    PASS 1 (X-Edge-Scoring-Pass == "1"): scorer's response is in
    resp.http.x-edge-*. Stash the eight scorer values (score, l1, l2,
    compliance, reason, sid, enforce, exec) into req.http.x-edge-score
    subfields (single consolidated header), compute the round-trip (rtt)
    subfield from the recv x-edge-score-t0 stamp, stash Set-Cookie into a
    :set-cookie subfield (ten subfields total), scrub the
    resp.http.x-edge-* headers (anti-leak), then naked `restart`.

    PASS 2 (X-Edge-Scoring-Pass already gone): the stashed cookie gets
    emitted via `add resp.http.Set-Cookie` (additive — preserves any
    Set-Cookie the real origin set).

    The subfield writes in pass-1 deliver propagate to vcl_log via the
    req.http persistence across restart. The log format reads
    req.http.x-edge-score:score etc."""
    return """# Session Scoring: pass-1 stash + naked restart; pass-2 emit cookie.

# ── PASS 1: capture scorer response into req.http.x-edge-score subfields ──
if (req.http.X-Edge-Scoring-Pass == "1") {
  unset req.http.X-Edge-Scoring-Pass;
  # Edge-observed scorer round-trip (µs): diff time.elapsed against the
  # x-edge-score-t0 stamp set in recv just before the sub-fetch. Computed
  # for BOTH success and fail-open (outside the status branch) so timed-out
  # rows record ≈the timeout budget — exactly the signal for tuning it.
  # Same std.atoi(time.elapsed.usec) idiom as the TTFB/TTLB capture VCL.
  if (req.http.x-edge-score-t0 != "") {
    declare local var.rtt INTEGER;
    set var.rtt = std.atoi(time.elapsed.usec);
    set var.rtt -= std.atoi(req.http.x-edge-score-t0);
    # Stringify with the `"" +` idiom: assigning an INTEGER var straight to a
    # header SUBFIELD lands empty (unlike a plain header, which coerces). Every
    # other working subfield is set from a string; match that.
    set req.http.x-edge-score:rtt = "" + var.rtt;
  }
  if (resp.status == 200) {
    set req.http.x-edge-score:score = resp.http.x-edge-score;
    set req.http.x-edge-score:l1 = resp.http.x-edge-score-l1;
    set req.http.x-edge-score:l2 = resp.http.x-edge-score-l2;
    set req.http.x-edge-score:compliance = resp.http.X-Edge-Cookie-Compliance;
    set req.http.x-edge-score:reason = resp.http.x-edge-score-reason;
    # Hex-encoded 6-byte session id. Used by the admin labeling UI to
    # target individual sessions; the scorer issues a fresh sid when
    # the inbound cookie is missing/tampered.
    set req.http.x-edge-score:sid = resp.http.x-edge-sid;
    # Enforcement signal — set by the Rust scorer when the operator
    # has committed enforce_threshold to the scoring_config ConfigStore
    # AND the request's score met it. Captured here so the recv-
    # restart-2 Enforce snippet can read it via subfield.
    set req.http.x-edge-score:enforce = resp.http.x-edge-score-enforce;
    # Scorer-reported Wasm execution time (µs). Only present on 200s —
    # fail-open rows leave edge_score_exec_us NULL since the scorer never
    # responded. Lets us split round-trip latency into compute vs network.
    set req.http.x-edge-score:exec = resp.http.x-edge-score-exec-us;
    # Matrix version (YYYY-MM-DD-<suffix>) that scored this request. Captured
    # into the log subfield BEFORE the anti-leak unset below so a logged edge
    # score can be correlated to the matrix version (and rollback) that produced
    # it (EC-03). Empty on cookie-missing requests (L2 skipped → no matrix load).
    set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;
  } else {
    # Scorer returned non-200 — fail open. No cookie to rotate.
    set req.http.x-edge-score:score = "0";
    set req.http.x-edge-score:l1 = "0";
    set req.http.x-edge-score:l2 = "0";
    set req.http.x-edge-score:compliance = "unknown";
    set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;
  }
  # Stash the rotated cookie as a subfield too; pass-2 reads it back
  # and emits via add resp.http.Set-Cookie.
  set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;
  # Anti-leak: strip the scorer's resp.http.x-edge-* headers so they
  # don't reach the client even if the restart path were to short-
  # circuit somehow.
  unset resp.http.x-edge-score;
  unset resp.http.x-edge-score-l1;
  unset resp.http.x-edge-score-l2;
  unset resp.http.x-edge-score-reason;
  unset resp.http.x-edge-sid;
  unset resp.http.X-Edge-Cookie-Compliance;
  unset resp.http.X-Edge-Matrix-Version;
  unset resp.http.x-edge-score-enforce;
  unset resp.http.x-edge-score-exec-us;
  restart;
}

# ── PASS 2: real origin response — emit the rotated cookie additively ──
# Only emit at the EDGE (fastly.ff.visits_this_service == 0). At a shield
# POP the counter is > 0, so the shield skips this block and the cookie is
# emitted exactly once, at the edge, to the client. Unforgeable, so no
# attacker-induced duplicate Set-Cookie emission.
if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {
  add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;
}"""


def enforce_snippet(status_code: int = DEFAULT_ENFORCE_STATUS_CODE) -> str:
    """vcl_recv snippet that errors ``status_code`` when the scorer flagged the
    request as over-threshold.

    Fires on req.restarts == 1 (after the scoring sub-fetch + restart)
    when the deliver pass-1 snippet captured ``X-Edge-Score-Enforce: 1``
    from the scorer's response. The scorer only emits that header when
    the operator has committed an enforce_threshold value via the
    admin UI AND the request's score met it.

    Edge-only — fastly.ff.visits_this_service == 0 (unforgeable) so shield
    hops don't double-enforce. ``error <status_code>`` instead of a `synth`
    keeps the door open for a custom vcl_error page later.

    ``status_code`` defaults to 429 (Too Many Requests). Operators can
    override via cfg.scoring.enforce_status_code; valid range 400-599.
    The reason phrase is auto-mapped via ``enforce_reason_phrase``."""
    code = resolve_enforce_status_code(status_code)
    reason = enforce_reason_phrase(code)
    return (
        f"# Session Scoring: enforce committed threshold by erroring flagged requests.\n"
        f"# Status code ({code} {reason}) is operator-configurable via\n"
        f"# cfg.scoring.enforce_status_code; the update_enforce_status_code\n"
        f"# orchestrator swaps this snippet on change. Default 429.\n"
        f"# Fires only on the post-scoring restart (req.restarts == 1) when the\n"
        f"# deliver pass-1 captured X-Edge-Score-Enforce=1 from the scorer.\n"
        f"# Edge-only (fastly.ff.visits_this_service == 0, unforgeable) so shield hops don't double-enforce.\n"
        f'if (fastly.ff.visits_this_service == 0 && req.restarts == 1 && req.http.x-edge-score:enforce == "1") {{\n'
        f'  error {code} "{reason}";\n'
        f"}}"
    )


def miss_snippet() -> str:
    """vcl_miss snippet: defensive unsets. Strip inbound x-edge-score
    (attacker could try to forge it) and X-Edge-Scoring-Pass (don't
    leak the internal marker to the real origin on pass-2 fetch)."""
    return """# Session Scoring: strip internal scoring headers before forwarding to
# the real origin. x-edge-score could be attacker-supplied; the
# X-Edge-Scoring-Pass marker is internal-only.
if (req.backend.is_origin) {
  unset bereq.http.x-edge-score;
  unset bereq.http.X-Edge-Scoring-Pass;
}"""


def generate_scoring_vcl(
    logging_service_id: str,
    request_secret: str,
    *,
    exclude_url_regex: str | None = None,
    enforce_status_code: int | None = None,
) -> dict[str, str]:
    """Return a {snippet_name: vcl_body} dict for all six snippets.

    Caller passes each (name, body) pair to ``ensure_vcl_snippet``
    individually so the existing idempotent diff-and-update path
    handles re-deploys cleanly.

    ``request_secret`` is the shared secret VCL embeds in the
    X-Edge-Scorer-Auth header so the scoring Compute service can
    reject requests that didn't originate from this VCL service.

    ``exclude_url_regex`` is the operator's per-service override of the
    URL-exclusion regex used by recv_snippet. None / "" → default.
    Pre-validated by backend.utils.vcl_validator at the API layer
    before reaching this function.

    ``enforce_status_code`` is the operator's per-service override of the
    HTTP status code the enforce snippet returns when the scorer flags
    a request. None / out-of-range → default (429).
    """
    return {
        SCORING_RECV_NAME: recv_snippet(logging_service_id, exclude_url_regex=exclude_url_regex),
        SCORING_PASS_NAME: pass_snippet(logging_service_id, request_secret),
        SCORING_FETCH_NAME: fetch_snippet(),
        SCORING_DELIVER_NAME: deliver_snippet(),
        SCORING_MISS_NAME: miss_snippet(),
        SCORING_ENFORCE_NAME: enforce_snippet(resolve_enforce_status_code(enforce_status_code)),
    }


def scoring_snippet_names() -> list[str]:
    """Names of the snippets we install. Used by disable_scoring to find
    and remove them by name from the cloned VCL version."""
    return [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]


import sys as _sys

_self_mod = _sys.modules[__name__]
_sys.modules["backend.provision.session_scoring_vcl"] = _self_mod
_prov_pkg = _sys.modules.get("backend.provision")
if _prov_pkg is not None:
    setattr(_prov_pkg, "session_scoring_vcl", _self_mod)
_backend_pkg = _sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.setdefault("backend.provision.session_scoring_vcl", __name__)
