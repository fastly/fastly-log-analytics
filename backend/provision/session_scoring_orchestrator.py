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
from pathlib import Path
from typing import Any

from backend import config as svcconfig
from backend.core.fastly.client import fastly, fastly_raw
from backend.core.fastly.service import (
    ensure_vcl_snippet,
    get_active_version,
    list_vcl_snippets,
)
from backend.provision.session_scoring_setup import (
    MATRIX_KEY_NAME,
    MATRIX_RESOURCE_LINK_NAME,
    delete_scoring_service,
    ensure_scoring_service,
)
from backend.provision.session_scoring_vcl import (
    SCORING_BACKEND_API_NAME,
    SCORING_DELIVER_NAME,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_FETCH_PRIORITY,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
    SCORING_SNIPPET_PRIORITY,
    generate_scoring_vcl,
    scoring_snippet_names,
)
from backend.provision.utils import BOLD, _c, fail, info, ok, warn

logger = logging.getLogger(__name__)

# Locations of the matrix files relative to repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_MATRIX_PATH = _REPO_ROOT / "compute" / "scorer" / "matrix.json"
_DEPLOY_WASM_SCRIPT = _REPO_ROOT / "scripts" / "scoring" / "deploy_wasm.sh"
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


def _deploy_wasm_package(
    scoring_service_id: str,
    matrix_store_id: str,
    token: str,
    status_cb=None,
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
    if status_cb:
        status_cb("⏳ [Compute] Uploading scorer Wasm package (no build)…")

    if not _SCORER_PACKAGE.exists():
        raise RuntimeError(
            f"prebuilt scorer package not found at {_SCORER_PACKAGE} — "
            "run `make scorer-package` (or scripts/scoring/deploy_wasm.sh build) "
            "off-VM and commit compute/scorer/pkg/session-scorer.tar.gz"
        )

    # Clone the highest existing version → editable draft (uniform for both a
    # fresh v1-draft service and an existing active one; the clone inherits
    # domain/backend/resource-links/package).
    versions = fastly("GET", f"/service/{scoring_service_id}/version", token=token) or []
    base = max(int(v["number"]) for v in versions)
    # Fastly's clone route is PUT (matches every other clone in this codebase).
    clone = fastly("PUT", f"/service/{scoring_service_id}/version/{base}/clone", token=token)
    new_ver = int(clone["number"])

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
    fastly("PUT", f"/service/{scoring_service_id}/version/{new_ver}/activate", token=token, expect_empty=True)
    ok(f"Deployed scorer package + activated v{new_ver}")
    if status_cb:
        status_cb(f"✅ [Compute] Activated scorer service v{new_ver}")
    # Return the new Compute version + the sha of the bytes we just uploaded.
    # The sha becomes enable_scoring's deployed_package_sha drift stamp (the
    # /scoring/status check compares it to the currently-committed package to
    # tell the operator when the edge is stale); the version is surfaced in the
    # two-phase SSE log so the operator can see exactly which Compute version
    # went live alongside the logging VCL version.
    return {"version": new_ver, "sha": hashlib.sha256(pkg_bytes).hexdigest()}


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


def _add_scoring_backend(
    logging_service_id: str,
    version: int,
    scoring_domain: str,
    token: str,
) -> None:
    """Add the scoring Compute service as a backend on the cloned VCL
    version. Backend name is the constant from session_scoring_vcl so the
    recv snippet can reference it by name."""
    payload = {
        "name": SCORING_BACKEND_API_NAME,
        "address": scoring_domain,
        "port": 443,
        "use_ssl": True,
        "ssl_cert_hostname": scoring_domain,
        "ssl_sni_hostname": scoring_domain,
        # The Fastly Compute service routes by Host header. Without
        # override_host, the upstream Host arrives as the customer's
        # domain (e.g. www.example.com) and the scorer's
        # edgecompute.app service can't dispatch it — TLS SNI matches
        # but the Host header doesn't. Forcing it to the scoring
        # domain fixes routing.
        "override_host": scoring_domain,
        # The edgecompute.app cert is from Fastly's internal CA and may not
        # validate cleanly when one Fastly service backends to another. Both
        # ends are inside Fastly's network so we trade strict verification
        # for reliability — security is not at risk because the path never
        # leaves Fastly's edge.
        "ssl_check_cert": False,
        "auto_loadbalance": False,
        # Wasm execution is ~1ms warm and intra-Fastly network adds ~5-20ms,
        # so the timeout is a CEILING, not the time a typical request waits.
        # It only bites cold-start Compute instances (and Fastly Compute is
        # instance-per-request, so every request is cold). first_byte_timeout
        # lowered 200→100ms (2026-06-22): successful-scoring RTT sits at
        # p50 ~5ms / p99 ~18ms / p99.9 ~40ms (max ~830ms), so 100ms clears the
        # steady-state distribution with wide headroom. The 200ms ceiling had
        # been rescuing a small cold-slow-instantiation band (~100-200ms RTT,
        # order ~30 scorings/day) — those now fail open instead. That trade is
        # accepted: the dominant fail-open class is sandbox-creation contention
        # from scheduled scanner bursts, which rejects connection-level (or
        # hangs well past any 100-200ms boundary) and is unaffected by this
        # knob, so the wider ceiling bought tail-latency for real users without
        # reducing the visible error spikes. connect_timeout stays 100ms (the
        # fast rejects are connection-level, not connect timeouts). Tuned from
        # the edge_score_rtt_us / edge_score_exec_us columns (per-request, see
        # session_scoring_vcl + the scorer's X-Edge-Score-Exec-Us header).
        "connect_timeout": 100,
        "first_byte_timeout": 100,
        "between_bytes_timeout": 200,
    }
    # Idempotent: if the backend already exists, PUT-update it when the
    # config has drifted (e.g. we tuned the timeouts). POST a new one
    # only when it's missing. Without the PUT path, re-running enable
    # on a version with an existing backend would silently keep stale
    # timeouts in place.
    existing_match = None
    try:
        existing = (
            fastly(
                "GET",
                f"/service/{logging_service_id}/version/{version}/backend",
                token=token,
            )
            or []
        )
        for b in existing:
            if b.get("name") == SCORING_BACKEND_API_NAME:
                existing_match = b
                break
    except RuntimeError:
        pass

    if existing_match is not None:
        drift = any(existing_match.get(k) != v for k, v in payload.items() if k in existing_match)
        if not drift:
            ok(f"Scoring backend already current on version {version}")
            return
        encoded = urllib.parse.quote(SCORING_BACKEND_API_NAME, safe="")
        fastly(
            "PUT",
            f"/service/{logging_service_id}/version/{version}/backend/{encoded}",
            payload,
            token=token,
        )
        ok(f"Updated scoring backend {SCORING_BACKEND_API_NAME} (drifted settings)")
        return

    fastly(
        "POST",
        f"/service/{logging_service_id}/version/{version}/backend",
        payload,
        token=token,
    )
    ok(f"Added scoring backend {SCORING_BACKEND_API_NAME} ({scoring_domain})")


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
    deployed = _deploy_wasm_package(scoring_service_id, matrix_store_id, token, status_cb=status_cb)
    deployed_package_sha = deployed["sha"]
    compute_ver = deployed["version"]
    _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)

    # ── Stage 3: write scoring metadata into the LOGGING service config. ────
    # Preserve operator-tunable overrides across re-enables — the previous
    # implementation replaced the entire ``scoring`` block, silently wiping
    # the operator's per-service exclude_url_regex and enforce_status_code.
    # Pull them off the pre-existing block before the replace.
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    prior_scoring = cfg.get("scoring") or {}
    cfg["scoring"] = {
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
        cfg["scoring"]["enforce_status_code"] = prior_scoring["enforce_status_code"]
    # Add the scoring custom_fields so update_logging_endpoint picks them up.
    _add_scoring_custom_fields(cfg)
    svcconfig.save_config(logging_service_id, cfg)
    n_scoring = len(_SCORING_FIELD_NAMES)
    ok(f"Stashed scoring metadata + {n_scoring} custom_fields into service config")

    # ── Stage 4: clone the LOGGING service's active VCL version. ────────────
    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    info(f"Logging service active version: {active_ver}")
    if status_cb:
        status_cb("▸ Phase 2/2 — Logging service (VCL + log fields)")
        status_cb(f"⏳ [Logging] Cloning active version {active_ver}…")
    clone = fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{active_ver}/clone",
        token=token,
    )
    new_ver = int(clone["number"])
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{new_ver}",
        {"comment": f"Enable session scoring (scorer={scoring_service_id}) {ts}"},
        token=token,
    )
    ok(f"Draft version: {new_ver}")

    try:
        # ── Stage 5: add the scoring Compute service as a backend. ──────────
        _add_scoring_backend(logging_service_id, new_ver, scoring_domain, token)

        # ── Stage 6: install the six scoring VCL snippets. ──────────────────
        info("Installing 6 scoring VCL snippets (recv / pass / fetch / deliver / miss / enforce)")
        if status_cb:
            status_cb("⏳ [Logging] Installing scoring VCL snippets…")
        # Pick up the operator's overrides (if any) so a re-enable carries
        # the customised exclusion regex AND enforce-status-code forward.
        # None / "" / out-of-range → defaults.
        scoring_cfg = cfg.get("scoring") or {}
        exclude_url_regex = scoring_cfg.get("exclude_url_regex")
        enforce_status_code = scoring_cfg.get("enforce_status_code")
        vcl_snippets = generate_scoring_vcl(
            logging_service_id,
            request_secret,
            exclude_url_regex=exclude_url_regex,
            enforce_status_code=enforce_status_code,
        )
        for snip_name, vcl_type, prio in (
            (SCORING_RECV_NAME, "recv", SCORING_SNIPPET_PRIORITY),
            (SCORING_PASS_NAME, "pass", SCORING_SNIPPET_PRIORITY),
            # Fetch gets priority 1 so `return(deliver)` for the scorer
            # backend fires before any other fetch-stage snippet runs.
            (SCORING_FETCH_NAME, "fetch", SCORING_FETCH_PRIORITY),
            (SCORING_DELIVER_NAME, "deliver", SCORING_SNIPPET_PRIORITY),
            (SCORING_MISS_NAME, "miss", SCORING_SNIPPET_PRIORITY),
            # Enforce snippet runs at recv-restart-2 (priority 101 — after
            # the main Recv routing block) and 429s requests the scorer
            # flagged via X-Edge-Score-Enforce. Off by default — fires
            # only when the operator commits an enforce_threshold via
            # the admin UI.
            (SCORING_ENFORCE_NAME, "recv", SCORING_SNIPPET_PRIORITY + 1),
        ):
            ensure_vcl_snippet(
                snip_name,
                vcl_type,
                vcl_snippets[snip_name],
                prio,
                logging_service_id,
                new_ver,
                token,
            )
        ok("Installed 6 scoring VCL snippets")

        # ── Stage 7: regenerate the capture-VCL + log format for the
        #            6 new custom_fields. update_logging_endpoint handles
        #            both: it diffs the format, pushes the new one, and
        #            re-runs ensure_vcl_snippet for capture snippets so
        #            the new deliver-stage capture VCL gets installed.
        info("Regenerating log format + capture VCL for scoring fields")
        if status_cb:
            status_cb("⏳ [Logging] Updating log format to include score fields…")
        # update_logging_endpoint targets the active version by default.
        # We want it to write to OUR draft, so we pass a hint via the cfg
        # — but update_logging_endpoint doesn't accept a version arg. So
        # we call it after activate, which means it'd create yet another
        # version. To avoid that double-activation, we manually install
        # the capture snippets on the draft here via the shared helper
        # (which also installs the Origin Error snippet that an earlier
        # inline copy of this logic was silently missing).
        from backend.provision.fastly_api import install_capture_snippets, resolve_shield_secret

        install_capture_snippets(
            logging_service_id,
            new_ver,
            cfg.get("log_fields"),
            token,
            cluster_secret=resolve_shield_secret(cfg),
            # Scoring is being enabled here, so the capture must be restart-tolerant
            # (the scorer restarts the request; gating on req.restarts == 0 would
            # leave scored rows with no url/ua/geo and edge != true → Fire Rate 0%).
            scoring_enabled=True,
        )

        # Update the logging endpoint's format string on the draft version.
        # The existing s3 logging endpoint must already exist (it was
        # provisioned at setup). We PUT to update its format.
        from backend.core.fastly.service import list_s3_endpoints
        from backend.provision.fastly_api import load_log_format

        endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
        existing_endpoints = list_s3_endpoints(logging_service_id, new_ver, token)
        if endpoint_name not in existing_endpoints:
            warn(
                f"Logging endpoint {endpoint_name!r} not found on draft v{new_ver} — "
                "skipping format update. Score fields will land in resp headers but not the log line."
            )
        else:
            new_format = load_log_format(cfg.get("log_fields"))
            encoded = urllib.parse.quote(endpoint_name, safe="")
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{new_ver}/logging/s3/{encoded}",
                {"format": new_format, "format_version": 2},
                token=token,
            )
            ok(f"Updated logging endpoint format to include {len(_SCORING_FIELD_NAMES)} score fields")

            # Make the log-sampling response_condition restart-tolerant. Scoring
            # restarts the request (req.restarts 0→1) to run the scorer sub-fetch,
            # so the final logged pass is req.restarts == 1. The condition set at
            # provision gates on req.restarts == 0, which drops EVERY scored
            # request from the logs (the scorer's edge_score never reaches the log
            # line → Fire Rate 0%). Rewrite the edge clause to match the edge hop
            # by visits_this_service alone (restart-invariant). See
            # backend.provision.fastly_api._log_sampling_edge_clause.
            from backend.core.fastly.service import ensure_condition, find_condition

            ep = fastly(
                "GET",
                f"/service/{logging_service_id}/version/{new_ver}/logging/s3/{encoded}",
                token=token,
            )
            cond_name = ep.get("response_condition")
            if cond_name:
                cur_cond = find_condition(cond_name, logging_service_id, new_ver, token)
                cur_stmt = (cur_cond or {}).get("statement", "")
                fixed_stmt = cur_stmt.replace(
                    "(req.restarts == 0 && fastly.ff.visits_this_service == 0)",
                    "fastly.ff.visits_this_service == 0",
                )
                if cur_cond and fixed_stmt != cur_stmt:
                    ensure_condition(cond_name, fixed_stmt, "RESPONSE", logging_service_id, new_ver, token)
                    ok("Made log-sampling condition restart-tolerant — scored requests now log")

        # ── Stage 8: validate ──────────────────────────────────────────────
        info(f"Validating draft version {new_ver}")
        if status_cb:
            status_cb(f"⏳ [Logging] Validating draft version {new_ver}…")
        result = fastly(
            "GET",
            f"/service/{logging_service_id}/version/{new_ver}/validate",
            token=token,
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
        ok("Draft validated")

        # ── Stage 9: activate ──────────────────────────────────────────────
        info(f"Activating version {new_ver}")
        if status_cb:
            status_cb(f"⏳ [Logging] Activating version {new_ver}…")
        fastly(
            "PUT",
            f"/service/{logging_service_id}/version/{new_ver}/activate",
            token=token,
        )
        ok(f"Version {new_ver} active")
        if status_cb:
            status_cb(f"✅ Done — Compute scorer v{compute_ver} + logging VCL v{new_ver} are now live.")

        scoring_meta["logging_service_active_version"] = new_ver

        # Publish the new custom_fields list to FOS's admin_state.json so
        # read_only analyst hosts (and the prod VM backend) pick them up
        # on their next import_admin_state tick. Without this, a stale
        # admin_state.json from before scoring was enabled would silently
        # strip our 6 custom_fields on every metadata_sync — exactly the
        # 2026-06-02 incident that motivated the import_admin_state merge
        # fix in backend/state_sync.py.
        try:
            from backend.state_sync import export_admin_state

            export_admin_state(logging_service_id)
            ok("Published custom_fields to FOS admin_state.json")
        except Exception as exc:
            warn(f"Could not export admin_state to FOS (non-fatal): {exc}")

        # Also publish the trained scoring matrix to FOS so analyst hosts
        # (and any fresh backend container) see the exact same matrix
        # that's currently embedded in the deployed Wasm. Without this,
        # the /scoring/evaluation endpoint falls back to the default-empty
        # matrix on read_only hosts and reports AUC ≈ 0.5 even though the
        # live scorer is using a real trained one.
        #
        # Reads from the tenant-scoped path (audit finding #005); the
        # legacy shared ``matrix.json`` would have either been empty
        # (post-#005 retrains all write tenant-scoped) or held another
        # tenant's matrix (pre-#005 leftovers from a prior retrain on
        # this host).
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

        return scoring_meta

    except Exception as exc:
        # ── Stage 10: rollback ─────────────────────────────────────────────
        fail(f"enable_scoring failed: {exc}")
        info(f"Rolling back — re-activating version {active_ver}")
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        # Also revert the on-disk config so a retry starts from clean state.
        #
        # DEFENSE-IN-DEPTH: re-load cfg here instead of trusting the in-
        # memory copy from line ~381. The Fastly stages above can take
        # 30-60s; during that window a concurrent writer (metadata_sync
        # tick re-injecting scoring fields, an admin PATCHing log_fields,
        # an ngwaf_workspace_id update) may have mutated configs/<sid>.json.
        # Writing the stale snapshot back wholesale would clobber those
        # concurrent changes. Re-reading + mutating + saving means we
        # only touch the scoring-related keys our rollback is supposed
        # to revert.
        try:
            fresh = svcconfig.load_config(logging_service_id) or cfg
        except Exception:
            fresh = cfg
        fresh.pop("scoring", None)
        _remove_scoring_custom_fields(fresh)
        svcconfig.save_config(logging_service_id, fresh)
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
    clone = fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{active_ver}/clone",
        token=token,
    )
    new_ver = int(clone["number"])
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{logging_service_id}/version/{new_ver}",
        {"comment": f"Disable session scoring {ts}"},
        token=token,
    )

    try:
        # ── Stage 2: remove scoring VCL bits. ───────────────────────────────
        _remove_scoring_snippets(logging_service_id, new_ver, token)
        _remove_scoring_backend(logging_service_id, new_ver, token)

        # ── Stage 3: drop the 6 custom_fields + regen log format. ───────────
        _remove_scoring_custom_fields(cfg)
        svcconfig.save_config(logging_service_id, cfg)

        from backend.core.fastly.service import list_s3_endpoints
        from backend.provision.fastly_api import generate_capture_vcl, load_log_format, resolve_shield_secret

        capture = generate_capture_vcl(cfg.get("log_fields"), cluster_secret=resolve_shield_secret(cfg))
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

        # ── Stage 4: validate + activate. ──────────────────────────────────
        result = fastly(
            "GET",
            f"/service/{logging_service_id}/version/{new_ver}/validate",
            token=token,
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
        fastly(
            "PUT",
            f"/service/{logging_service_id}/version/{new_ver}/activate",
            token=token,
        )
        ok(f"Logging service version {new_ver} active (scoring stripped)")
    except Exception as exc:
        fail(f"disable_scoring VCL phase failed: {exc}")
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        raise

    # ── Stage 5: tear down the Compute service + stores. ───────────────────
    delete_scoring_service(
        scoring_service_id,
        scoring_keys_store_id=scoring_keys_store_id,
        scoring_config_store_id=scoring_config_store_id,
        scoring_matrix_store_id=scoring_matrix_store_id,
        token=token,
        status_cb=status_cb,
    )

    # ── Stage 6: clear the scoring block from config. ──────────────────────
    # DEFENSE-IN-DEPTH: re-load cfg right before the final save. The
    # Fastly + Compute teardown stages above can take 60-120s; the
    # in-memory cfg loaded at line ~644 is a stale snapshot that would
    # clobber any concurrent writer mutations (metadata_sync tick,
    # custom_fields PATCH, ngwaf_workspace_id update). Same load-mutate-
    # save-just-the-target-keys pattern as the enable_scoring rollback.
    try:
        fresh = svcconfig.load_config(logging_service_id) or cfg
    except Exception:
        fresh = cfg
    fresh.pop("scoring", None)
    svcconfig.save_config(logging_service_id, fresh)

    # Publish the new custom_fields list (now without scoring) so analyst
    # boxes stop seeing the scoring entries on their next metadata_sync.
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(logging_service_id)
    except Exception as exc:
        warn(f"Could not export admin_state to FOS after disable (non-fatal): {exc}")

    if status_cb:
        status_cb("✅ Session scoring disabled.")
    ok("Session scoring disabled")


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
    NOT roll back — the caller owns the rollback so it can log fail() in the
    exact pre-rollback order the originals used. Raises ``RuntimeError`` on a
    non-``ok`` validate result, matching the inline form.
    """
    clone = fastly("PUT", f"/service/{sid}/version/{active_ver}/clone", token=token)
    new_ver = int(clone["number"])
    fastly(
        "PUT",
        f"/service/{sid}/version/{new_ver}",
        {"comment": comment},
        token=token,
    )
    ensure_vcl_snippet(
        snippet_name,
        vcl_type,
        body,
        priority,
        sid,
        new_ver,
        token,
    )
    result = fastly("GET", f"/service/{sid}/version/{new_ver}/validate", token=token)
    if result.get("status") != "ok":
        raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
    fastly(
        "PUT",
        f"/service/{sid}/version/{new_ver}/activate",
        token=token,
    )
    return new_ver


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
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise RuntimeError(
            f"Session scoring is not enabled for {logging_service_id}; "
            "run enable_scoring first before customising the recv exclusion regex."
        )
    request_secret = scoring.get("request_secret")
    if not request_secret:
        raise RuntimeError(
            "Cannot re-publish recv snippet without request_secret in cfg; "
            "the snippet bodies for peer snippets depend on it. Re-run enable_scoring."
        )

    # Persist the override first — that way even if the Fastly activation
    # below fails, a future enable_scoring run picks up the new value.
    # None is the canonical "use default" representation so the JSON cfg
    # file doesn't end up with an empty-string sentinel.
    cleaned = (new_regex or "").strip()
    scoring["exclude_url_regex"] = cleaned or None
    cfg["scoring"] = scoring
    svcconfig.save_config(logging_service_id, cfg)

    # Generate the recv snippet body with the new regex.
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ASSET_EXT_REGEX,
        recv_snippet,
        resolve_exclude_url_regex,
    )

    effective_regex = resolve_exclude_url_regex(cleaned or None)
    is_default = effective_regex == DEFAULT_ASSET_EXT_REGEX
    new_recv_body = recv_snippet(logging_service_id, request_secret, exclude_url_regex=cleaned or None)

    # Clone → swap → activate.
    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    info(f"Cloning version {active_ver} to update recv-snippet exclusion regex")
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        new_ver = _clone_swap_snippet(
            logging_service_id,
            token,
            active_ver=active_ver,
            comment=f"Update scoring recv exclusion regex {ts}",
            snippet_name=SCORING_RECV_NAME,
            vcl_type="recv",
            body=new_recv_body,
            priority=SCORING_SNIPPET_PRIORITY,
        )
        ok(f"Logging service version {new_ver} active (recv exclusion regex updated)")
    except Exception as exc:
        fail(f"update_recv_exclusion_regex failed: {exc}")
        # Re-activate the prior version so the service isn't left on the
        # half-updated draft. Best-effort — if this fails too, the draft
        # is left for the operator to clean up manually.
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        raise

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
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")
    scoring = cfg.get("scoring") or {}
    if not scoring.get("enabled"):
        raise RuntimeError(
            f"Session scoring is not enabled for {logging_service_id}; "
            "run enable_scoring first before customising the enforce status code."
        )

    # Persist the override first so a future enable_scoring re-bake also
    # picks it up even if the activation below fails. None is the canonical
    # "use default" representation (mirrors exclude_url_regex shape).
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ENFORCE_STATUS_CODE,
        enforce_snippet,
        resolve_enforce_status_code,
    )

    effective_code = resolve_enforce_status_code(new_status_code)
    is_default = effective_code == DEFAULT_ENFORCE_STATUS_CODE
    scoring["enforce_status_code"] = None if is_default else effective_code
    cfg["scoring"] = scoring
    svcconfig.save_config(logging_service_id, cfg)

    # 034: enforce_snippet now bakes the request_secret into its shield-auth
    # boundary check. Re-publishing without the secret would emit invalid
    # VCL — fail loudly here rather than letting the activation fail later.
    request_secret = scoring.get("request_secret")
    if not request_secret:
        raise RuntimeError(
            "Cannot re-publish enforce snippet without request_secret in cfg; "
            "run enable_scoring first or restore scoring.request_secret."
        )
    new_enforce_body = enforce_snippet(request_secret, effective_code)

    # Clone → swap → activate.
    active_ver = get_active_version(logging_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Logging service {logging_service_id} has no active version")
    info(f"Cloning version {active_ver} to update enforce-snippet status code → {effective_code}")
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        new_ver = _clone_swap_snippet(
            logging_service_id,
            token,
            active_ver=active_ver,
            comment=f"Update scoring enforce status code → {effective_code} ({ts})",
            snippet_name=SCORING_ENFORCE_NAME,
            vcl_type="recv",
            body=new_enforce_body,
            priority=SCORING_SNIPPET_PRIORITY + 1,
        )
        ok(f"Logging service version {new_ver} active (enforce status code → {effective_code})")
    except Exception as exc:
        fail(f"update_enforce_status_code failed: {exc}")
        # Re-activate the prior version so the service isn't left on the
        # half-updated draft. Best-effort.
        try:
            fastly(
                "PUT",
                f"/service/{logging_service_id}/version/{active_ver}/activate",
                token=token,
            )
        except RuntimeError:
            pass
        raise

    return {
        "effective_status_code": effective_code,
        "is_default": is_default,
        "logging_service_active_version": new_ver,
    }
