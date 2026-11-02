"""End-to-end enable/disable/teardown flows and live snippet updates for session scoring."""

from __future__ import annotations

import datetime as _dt
import urllib.parse
from collections.abc import Callable
from typing import Any

import backend.config as svcconfig
from backend.core.fastly.client import ensure_vcl_snippet, fastly, get_active_version
from backend.provision.cli import BOLD, _c, draft_version, fail, info, ok, rollback_to_version, warn
from backend.provision.session_scoring.stores import (
    _delete_scoring_matrix_from_fos,
    _publish_scoring_fos_side_effects,
    _write_matrix_to_kv,
    rotate_aes_key,
)
from backend.provision.session_scoring.wasm_deploy import (
    DEFAULT_ASSET_EXT_REGEX,
    DEFAULT_ENFORCE_STATUS_CODE,
    SCORING_ENFORCE_NAME,
    SCORING_RECV_NAME,
    SCORING_SNIPPET_PRIORITY,
    _deploy_wasm_package,
    _remove_scoring_backend,
    _remove_scoring_snippets,
    delete_scoring_service,
    enforce_snippet,
    ensure_scoring_service,
    recv_snippet,
    resolve_enforce_status_code,
    resolve_exclude_url_regex,
    scorer_vcl_fingerprint,
)

__all__ = [
    "_SCORING_CUSTOM_FIELDS",
    "_SCORING_FIELD_NAMES",
    "_add_scoring_custom_fields",
    "_clone_swap_snippet",
    "_remove_scoring_custom_fields",
    "_update_live_scoring_snippet",
    "disable_scoring",
    "enable_scoring",
    "merge_scoring_custom_fields",
    "rotate_aes_key",
    "teardown_scoring_resources",
    "update_enforce_status_code",
    "update_recv_exclusion_regex",
]

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
    """Return ``custom_fields`` with the canonical scoring fields re-applied."""
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    return kept + [dict(cf) for cf in _SCORING_CUSTOM_FIELDS]


def _add_scoring_custom_fields(cfg: dict) -> dict:
    """Merge the 6 scoring custom_fields into cfg.log_fields.custom_fields.     Existing fields with the same name are replaced (idempotent re-runs     pick up any tuning we've done to bytes_estimate / label / etc.)."""
    cfg.setdefault("log_fields", {})
    cfg["log_fields"].setdefault("custom_fields", [])
    cfg["log_fields"]["custom_fields"] = merge_scoring_custom_fields(cfg["log_fields"]["custom_fields"])
    return cfg


def _remove_scoring_custom_fields(cfg: dict) -> dict:
    """Strip the 6 scoring custom_fields from cfg, leaving any others     untouched."""
    if "log_fields" not in cfg or "custom_fields" not in cfg["log_fields"]:
        return cfg
    cfg["log_fields"]["custom_fields"] = [
        cf for cf in cfg["log_fields"]["custom_fields"] if cf.get("name") not in _SCORING_FIELD_NAMES
    ]
    return cfg


def enable_scoring(logging_service_id: str, token: str, *, status_cb=None) -> dict[str, Any]:
    """Provision (or reuse) the Compute scoring service, deploy the Wasm,     then mutate the customer's VCL service to call it via the restart     pattern."""
    cfg = svcconfig.load_config(logging_service_id)
    if not cfg:
        raise RuntimeError(f"No config found for logging service {logging_service_id}")
    # ── Stage 1: Compute scoring service + AES key + ConfigStores. ──────────
    info(f"Enabling session scoring for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Enabling session scoring for {logging_service_id}...")
        status_cb("Redeploy runs two phases: (1) Compute scorer service, then (2) logging service (VCL + log fields).")
        status_cb("▸ Phase 1/2 — Compute scorer service")
    prior_scoring = cfg.get("scoring") or {}
    scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
    scoring_service_id = scoring_meta["scoring_service_id"]
    scoring_domain = scoring_meta["scoring_domain"]
    request_secret = scoring_meta.get("request_secret") or prior_scoring.get("request_secret") or ""
    if not request_secret:
        import secrets as _secrets

        request_secret = _secrets.token_hex(32)
        info("Generated fresh request_secret (none in store or cfg)")
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
    matrix_store_id = (
        scoring_meta.get("scoring_matrix_store_id") or (cfg.get("scoring") or {}).get("scoring_matrix_store_id") or ""
    )
    deployed = _deploy_wasm_package(
        scoring_service_id,
        matrix_store_id,
        token,
        status_cb=status_cb,
        prior_package_sha=prior_scoring.get("deployed_package_sha", ""),
        prior_files_hash=prior_scoring.get("deployed_package_files_hash", ""),
    )
    deployed_package_sha = deployed["sha"]
    deployed_files_hash = deployed.get("files_hash", "")
    compute_ver = deployed["version"]
    _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)

    # ── Stage 3: write scoring metadata into the LOGGING service config. ────
    def _apply_scoring(target: dict) -> None:
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
            "request_secret": request_secret,
            "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            "deployed_package_sha": deployed_package_sha,
            "deployed_package_files_hash": deployed_files_hash,
            "deployed_vcl_sha": scorer_vcl_fingerprint(logging_service_id),
            "exclude_url_regex": prior_scoring.get("exclude_url_regex") or DEFAULT_ASSET_EXT_REGEX,
        }
        if prior_scoring.get("enforce_status_code") is not None:
            block["enforce_status_code"] = prior_scoring["enforce_status_code"]
        return block

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

        def _revert(target: dict) -> None:
            target.pop("scoring", None)
            _remove_scoring_custom_fields(target)

        svcconfig.mutate_config(logging_service_id, _revert)
        raise


def disable_scoring(logging_service_id: str, token: str, *, status_cb=None) -> None:
    """Tear down session scoring for this customer."""
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
        ensure_vcl_snippet(
            "Fastly Log Analysis Capture", "recv", capture["recv"], 1, logging_service_id, new_ver, token
        )
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
    _delete_scoring_matrix_from_fos(logging_service_id)
    # ── Stage 6: clear the scoring block from config. ──────────────────────
    svcconfig.mutate_config(logging_service_id, lambda target: target.pop("scoring", None))
    try:
        from backend.core.metadata.base import export_admin_state

        export_admin_state(logging_service_id)
    except Exception as exc:
        warn(f"Could not export admin_state to FOS after disable (non-fatal): {exc}")
    for label, store_id in failed_stores:
        warn(f"Scoring {label} store {store_id} was NOT deleted — remove it manually in Fastly.")
        if status_cb:
            status_cb(f"⚠️ Scoring {label} store {store_id} not deleted — remove it manually.")
    if status_cb:
        status_cb("✅ Session scoring disabled.")
    ok("Session scoring disabled")


def teardown_scoring_resources(
    logging_service_id: str, scoring_meta: dict, token: str, *, status_cb=None
) -> list[tuple[str, str]]:
    """Tear down scoring for a service that is being FULLY DESTROYED."""
    info(f"Tearing down session scoring for {_c(BOLD, logging_service_id)}")
    if status_cb:
        status_cb(f"⏳ Tearing down session scoring for {logging_service_id}...")
    # ── Stage 1: strip the scoring VCL from the still-live logging service. ──
    strip_ok = False
    try:
        active_ver = get_active_version(logging_service_id, token)
        if active_ver is None:
            if status_cb:
                status_cb("Logging service has no active version — skipping VCL strip.")
        else:
            ts = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
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
    sid: str, token: str, *, active_ver: int, comment: str, snippet_name: str, vcl_type: str, body: str, priority: int
) -> int:
    """Clone ``active_ver`` → set its comment → install ONE VCL snippet →     validate → activate, returning the new active version."""
    with draft_version(sid, token, base_version=active_ver, call=fastly, comment=comment, rollback=False) as new_ver:
        ensure_vcl_snippet(snippet_name, vcl_type, body, priority, sid, new_ver, token)
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


def update_recv_exclusion_regex(logging_service_id: str, token: str, *, new_regex: str) -> dict[str, Any]:
    """Re-publish ONLY the recv VCL snippet with a new exclusion regex."""
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
    return {"effective_regex": effective_regex, "is_default": is_default, "logging_service_active_version": new_ver}


def update_enforce_status_code(logging_service_id: str, token: str, *, new_status_code: int | None) -> dict[str, Any]:
    """Re-publish ONLY the enforce VCL snippet with a new status code."""
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
