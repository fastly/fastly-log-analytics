# ruff: noqa: T201
"""Option B: Unified, production-grade Declarative Multi-Service IaC Engine.

Coordinates state reconciliation across Fastly KV Stores, Compute Scorer services,
Active HTTP Readiness probing, and VCL logging services in a single transactional control loop.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from backend.core.fastly.mock_fixtures import is_mock_mode
from backend.provision.declarative.generators import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
)
from backend.provision.declarative.reconciler import (
    _bootstrap_featurestate_from_fastly,
    reconcile_vcl_state,
)
from backend.provision.declarative.state import FeatureState
from backend.provision.fastly_api import find_service_by_name
from backend.provision.session_scoring_orchestrator import (
    _SCORER_PACKAGE,
    _deploy_wasm_package,
    _remove_scoring_custom_fields,
    _write_matrix_to_kv,
)
from backend.provision.session_scoring_setup import (
    MATRIX_STORE_NAME_TEMPLATE,
    _find_kv_store,
    delete_scoring_service,
    ensure_scoring_service,
)

logger = logging.getLogger(__name__)


# ── Unified Declarative Resource Schemas ─────────────────────────────────────


class DeclarativeKVStore(BaseModel):
    name: str
    keys: dict[str, str] = Field(default_factory=dict)
    link_to_services: list[str] = Field(default_factory=list)


class DeclarativeComputeService(BaseModel):
    name: str
    package_path: str
    domains: list[str] = Field(default_factory=list)
    linked_kv_stores: list[str] = Field(default_factory=list)


class DeclarativeVCLService(BaseModel):
    name: str
    custom_fields: list[dict[str, Any]] = Field(default_factory=list)
    logging_endpoints: list[dict[str, Any]] = Field(default_factory=list)
    backends: list[dict[str, Any]] = Field(default_factory=list)
    snippets: list[dict[str, Any]] = Field(default_factory=list)


class UnifiedDesiredState(BaseModel):
    service_id: str
    vcl_service: DeclarativeVCLService
    compute_scorer: DeclarativeComputeService | None = None
    kv_stores: dict[str, DeclarativeKVStore] = Field(default_factory=dict)


# ── Core Helper Functions ───────────────────────────────────────────────────


def build_desired_state(logging_service_id: str, token: str) -> UnifiedDesiredState:
    """Build the complete, unified desired infrastructure state from configuration."""
    config_path = Path(f"configs/{logging_service_id}.json")
    if not config_path.exists():
        state = _bootstrap_featurestate_from_fastly(logging_service_id, token)
    else:
        cfg = json.loads(config_path.read_text())
        state = FeatureState.from_config(cfg)

    # Compile the VCL logging service definitions
    vcl_service = DeclarativeVCLService(
        name=state.logging_endpoint_name,
        custom_fields=state.log_fields.custom_fields,
        logging_endpoints=[ep if isinstance(ep, dict) else ep.to_dict() for ep in desired_logging_endpoints(state)],
        backends=[b if isinstance(b, dict) else b.to_dict() for b in desired_backends(state)],
        snippets=[s if isinstance(s, dict) else s.to_dict() for s in desired_snippets(state)],
    )

    # Compute scorer & KV store compiled structures
    compute_scorer = None
    kv_stores = {}

    if state.scoring.enabled:
        store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
        compute_scorer = DeclarativeComputeService(
            name=f"Session Scoring Service for {logging_service_id}",
            package_path=str(_SCORER_PACKAGE),
            domains=[state.scoring.domain],
            linked_kv_stores=[store_name],
        )

        # Build keys dictionary for matrix
        keys = {}
        from backend.provision.session_scoring_orchestrator import _resolve_tenant_matrix_for_deploy
        from backend.scoring.matrix import serialize_kv

        matrix_path = _resolve_tenant_matrix_for_deploy(logging_service_id)
        if matrix_path and matrix_path.exists():
            with matrix_path.open() as f:
                serialized = serialize_kv(json.load(f))
                keys["matrix"] = serialized.hex()

        kv_stores[store_name] = DeclarativeKVStore(
            name=store_name,
            keys=keys,
            link_to_services=[compute_scorer.name],
        )

    return UnifiedDesiredState(
        service_id=logging_service_id,
        vcl_service=vcl_service,
        compute_scorer=compute_scorer,
        kv_stores=kv_stores,
    )


def discover_current_state(logging_service_id: str, token: str) -> dict[str, Any]:
    """Retrieve the active live infrastructure matching names in our scope."""
    kv_stores = {}
    store_name = MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id)
    store = _find_kv_store(store_name, token)
    if store:
        kv_stores[store_name] = store

    compute_name = f"Session Scoring Service for {logging_service_id}"
    compute_service = find_service_by_name(compute_name, token)

    return {
        "kv_stores": kv_stores,
        "compute_scorer": compute_service,
    }


def print_speculative_execution_plan(desired: UnifiedDesiredState, current: dict[str, Any]) -> None:
    """Print a highly detailed, developer-friendly execution diff plan."""
    print("\n" + "=" * 55)
    print("=== DECLARATIVE IaC SPECULATIVE EXECUTION PLAN ===")
    print("=" * 55)
    print(f"Logging Service ID: {desired.service_id}")
    print()

    print("Phase 1: Storage Layer (KV Stores)")
    for name, store_def in desired.kv_stores.items():
        if name not in current["kv_stores"]:
            print(f"  + [CREATE] KV Store: '{name}'")
            for k in store_def.keys:
                print(f"    + [SYNC] Write Key: '{k}' (binary payload)")
        else:
            print(f"  ~ [REUSE] KV Store: '{name}'")
            for k in store_def.keys:
                print(f"    ~ [SYNC] Rewrite/Sync Key: '{k}'")

    for name in current["kv_stores"]:
        if name not in desired.kv_stores:
            print(f"  - [DELETE] KV Store: '{name}'")

    print()
    print("Phase 2: Compute Layer (Serverless Scorer)")
    if desired.compute_scorer:
        if not current["compute_scorer"]:
            print(f"  + [CREATE] Compute Service: '{desired.compute_scorer.name}'")
            print(f"    + [DOMAIN] Route: '{desired.compute_scorer.domains[0]}'")
            print(f"    + [PACKAGE] Deploy prebuilt WASM from '{desired.compute_scorer.package_path}'")
            print(f"    + [LINK] Associate resource: '{desired.compute_scorer.linked_kv_stores[0]}'")
        else:
            print(
                f"  ~ [REUSE] Compute Service: '{desired.compute_scorer.name}' (ID: {current['compute_scorer'].get('id')})"
            )
            print("    ~ [PACKAGE] Verify and deploy newer WASM package version if needed")
    elif current["compute_scorer"]:
        print(
            f"  - [DELETE] Compute Service: '{current['compute_scorer'].get('name')}' (ID: {current['compute_scorer'].get('id')})"
        )

    print()
    print("Phase 2.5: DNS & Readiness Probe Guard")
    if desired.compute_scorer:
        print(f"  * [PROBE] Check readiness of domain 'https://{desired.compute_scorer.domains[0]}/health'")

    print()
    print("Phase 3: VCL Edge Routing Layer")
    print("  ~ [RECONCILE] Generate snippets, custom fields, and backends dynamically")
    print("  ~ [ACTIVATE] Commit configuration to VCL edge and trigger version activation")
    print("=" * 55 + "\n")


def verify_compute_scorer_readiness(
    compute_def: DeclarativeComputeService,
    token: str,
    status_cb: Callable[[str], None] | None = None,
) -> None:
    """Probes the Compute Scorer domain to guarantee DNS and Edge Routing are active."""
    import httpx

    if is_mock_mode():
        if status_cb:
            status_cb("🔍 [Mock Mode] Skipping active HTTP readiness probe for Compute Scorer.")
        return

    domain = compute_def.domains[0]
    url = f"https://{domain}/health"
    max_retries = 10
    backoff = 1.0

    for i in range(max_retries):
        if status_cb:
            status_cb(f"🔍 Issuing HTTP probe to {url} (attempt {i + 1}/{max_retries})...")
        try:
            # We pass a sentinel auth header to signify a probe roundtrip.
            response = httpx.get(url, headers={"X-Edge-Scorer-Auth": "probe-sentinel"}, timeout=2.0)
            # Accept any standard status code (e.g. 200, 401, 404) as routing confirmation from Wasm
            if response.status_code in (200, 401, 404):
                if status_cb:
                    status_cb(f"✅ Compute Scorer is hot (status {response.status_code}).")
                return
        except Exception as e:
            if status_cb:
                status_cb(f"⚠️ Probe {i + 1} failed: {type(e).__name__}. Retrying in {backoff:.1f}s...")

        time.sleep(backoff)
        backoff = min(5.0, backoff * 1.5)

    raise RuntimeError(f"Readiness probe failed: Scorer service at {url} did not respond within timeout window.")


# ── Primary Orchestration Loop ───────────────────────────────────────────────


def reconcile_infrastructure(
    logging_service_id: str,
    token: str,
    dry_run: bool = False,
    status_cb: Callable[[str], None] | None = None,
) -> None:
    """The central unified IaC control loop managing all multi-service resource operations."""
    # 1. Build desired and discover current state
    desired = build_desired_state(logging_service_id, token)

    if status_cb:
        status_cb("⏳ Discovering active edge resources on Fastly account...")
    current = discover_current_state(logging_service_id, token)

    # 2. Speculative Execution Diff Plan
    if dry_run:
        print_speculative_execution_plan(desired, current)
        return

    # Transactional rollback: load & save the previous config to restore in case of failure
    config_path = Path(f"configs/{logging_service_id}.json")
    if not config_path.exists():
        raise RuntimeError(f"Configuration file configs/{logging_service_id}.json is required for live deployment.")

    orig_config_content = config_path.read_text()
    cfg = json.loads(orig_config_content)

    try:
        # ── Phase 1 & Phase 2: Create & Populate Storage + Compute Scorer ─────
        scoring_meta: dict[str, Any] = {}
        if desired.compute_scorer:
            if status_cb:
                status_cb("⏳ Phase 1: Reconciling edge KV Stores...")
                status_cb("⏳ Phase 2: Reconciling Compute Scorer Service and uploading WASM package...")

            # ensure_scoring_service idempotently handles Service creation, domain mapping, placeholder backends,
            # Keys & Config ConfigStores, AES Keys, and resource links.
            scoring_meta = ensure_scoring_service(logging_service_id, token, status_cb=status_cb)
            scoring_service_id = scoring_meta["scoring_service_id"]
            matrix_store_id = scoring_meta.get("scoring_matrix_store_id", "")

            # Sync matrix binary keys to KV store
            _write_matrix_to_kv(matrix_store_id, logging_service_id, token, status_cb=status_cb)

            # Upload the WASM Compute package
            package_meta = _deploy_wasm_package(
                scoring_service_id,
                matrix_store_id,
                token,
                status_cb=status_cb,
            )

            # Update the configuration dictionary with populated IDs & metadata
            cfg["scoring"] = {
                "enabled": True,
                "scoring_service_id": scoring_service_id,
                "scoring_service_name": scoring_meta.get("scoring_service_name", ""),
                "scoring_domain": scoring_meta.get("scoring_domain", ""),
                "scoring_keys_store_id": scoring_meta.get("scoring_keys_store_id", ""),
                "scoring_config_store_id": scoring_meta.get("scoring_config_store_id", ""),
                "scoring_matrix_store_id": matrix_store_id,
                "request_secret": scoring_meta.get("request_secret", cfg.get("scoring", {}).get("request_secret", "")),
                "aes_key_hex": scoring_meta.get("aes_key_hex", "") or cfg.get("scoring", {}).get("aes_key_hex", ""),
                "deployed_package_sha": package_meta["sha"],
                "deployed_package_files_hash": package_meta["files_hash"],
            }

            # ── Phase 2.5: Active HTTP Readiness Probe ───────────────────────
            if status_cb:
                status_cb("⏳ Phase 2.5: Issuing HTTP readiness checks on deployed domain...")
            verify_compute_scorer_readiness(desired.compute_scorer, token, status_cb=status_cb)

        elif current["compute_scorer"]:
            # If we are disabling scoring, we clean up the configuration *before* syncing VCL
            if status_cb:
                status_cb("⏳ Phase 4: Executing safe reverse-topological teardown of disabled assets...")
                status_cb("⏳ Stripping VCL scoring backend and snippets first...")

            # Clean up the JSON configuration log-fields
            cfg = _remove_scoring_custom_fields(cfg)
            if "scoring" in cfg:
                cfg["scoring"]["enabled"] = False

        # ── Phase 3: VCL Logging Sync ────────────────────────────────────────
        if status_cb:
            status_cb("⏳ Phase 3: Syncing VCL Edge Logging Configurations, Custom Fields, and Backends...")

        # Update JSON config on disk before triggering VCL reconciliation so the reconciler sees the live state
        config_path.write_text(json.dumps(cfg, indent=2))

        reconcile_vcl_state(logging_service_id, token, dry_run=False, status_cb=status_cb)

        # ── Phase 4: Deleting Scorer Resources (Only if we were disabling them) ──
        if not desired.compute_scorer and current["compute_scorer"]:
            if status_cb:
                status_cb("⏳ Sleeping/Draining edge requests for 5 seconds to ensure clean cutover...")
            time.sleep(5.0)

            if status_cb:
                status_cb("⏳ Deleting serverless Compute Scorer service and resources last...")

            old_scoring_service_id = current["compute_scorer"].get("id")
            old_matrix_store_id = (
                current["kv_stores"].get(MATRIX_STORE_NAME_TEMPLATE.format(sid=logging_service_id), {}).get("id", "")
            )

            # Look up prior config store IDs from original config to ensure they are deleted
            prior_scoring = json.loads(orig_config_content).get("scoring") or {}

            delete_scoring_service(
                scoring_service_id=old_scoring_service_id,
                scoring_keys_store_id=prior_scoring.get("scoring_keys_store_id", ""),
                scoring_config_store_id=prior_scoring.get("scoring_config_store_id", ""),
                scoring_matrix_store_id=old_matrix_store_id,
                token=token,
                status_cb=status_cb,
            )

            # Completely clear out the scoring configuration on full teardown success
            if "scoring" in cfg:
                cfg.pop("scoring", None)
            config_path.write_text(json.dumps(cfg, indent=2))

        if status_cb:
            status_cb("✅ Infrastructure state reconciliation completed successfully.")

    except Exception as err:
        logger.error("IaC reconciliation failure, executing transactional configuration rollback", exc_info=True)
        if status_cb:
            status_cb(f"❌ Error during reconciliation: {err}. Rolling back to previous configuration...")
        # Restore the original config file content to disk
        config_path.write_text(orig_config_content)
        raise err
