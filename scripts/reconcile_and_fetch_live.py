import json
import os
import shutil
import sys
from pathlib import Path

# Adjust path to import backend modules
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.core.fastly.service import get_generated_vcl
from backend.provision.declarative.fastly_integration import activate_version, fetch_logging_endpoints
from backend.provision.declarative.generators import (
    desired_backends,
    desired_dictionaries,
    desired_logging_endpoints,
    desired_snippets,
)
from backend.provision.declarative.reconciler import (
    MANAGED_BACKEND_NAMES,
    MANAGED_DICTIONARY_NAMES,
    _apply_diff,
    _clone_active_version,
    _fetch_backends,
    _fetch_dictionaries,
    _fetch_logging_endpoints,
    _fetch_snippets,
    _validate_draft,
    acquire_vcl_lock,
    compute_diff,
)
from backend.provision.declarative.state import FeatureState


def build_scenario_config(base_cfg: dict, base_id: int, cmcd_enabled: bool) -> dict:
    cfg = json.loads(json.dumps(base_cfg))  # deep copy

    # Base service setup
    cfg["service_id"] = "rmWzCRA0lkAOs9Gnxvohs4"
    cfg["log_period"] = 60
    cfg["sample_rate"] = 100
    cfg["edge_only"] = True
    cfg["custom_condition"] = ""

    # Helper variables
    logging_enabled = True
    rum_enabled = False
    scoring_enabled = False
    groups = ["A", "B", "C", "D", "F", "G", "K", "L"]

    if base_id == 1:
        logging_enabled = True
        rum_enabled = False
        scoring_enabled = False
        groups = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"]

    elif base_id == 2:
        logging_enabled = False
        rum_enabled = True
        scoring_enabled = False
        groups = []

    elif base_id == 3:
        logging_enabled = True
        rum_enabled = True
        scoring_enabled = False
        groups = ["A", "B", "C", "D", "F", "G", "K", "L"]

    elif base_id == 4:
        logging_enabled = True
        rum_enabled = False
        scoring_enabled = True
        groups = ["A", "B", "C", "D", "F", "G", "K", "L"]

    elif base_id == 5:
        logging_enabled = True
        rum_enabled = True
        scoring_enabled = True
        groups = ["A", "B", "C", "D", "F", "G", "K", "L"]

    cfg["logging_enabled"] = logging_enabled
    cfg["rum_enabled"] = rum_enabled
    cfg["rum"] = {"enabled": rum_enabled}
    cfg["log_fields"]["groups"] = groups

    cfg["cmcd"] = {
        "enabled": cmcd_enabled,
        "mode": "headers" if cmcd_enabled else "query_string",
        "version": 2 if cmcd_enabled else 1,
    }

    if scoring_enabled:
        cfg["scoring"] = {
            "enabled": True,
            "domain": "fos-rmwzcra0lkaos9gnxvohs4-session-scorer.edgecompute.app",
            "request_secret": "my-test-secret-4321",
            "exclude_url_regex": "^/healthz.*",
            "enforce_status_code": 429,
        }
    else:
        cfg["scoring"] = {"enabled": False, "domain": "", "request_secret": "", "exclude_url_regex": ""}

    if "custom_fields" in cfg["log_fields"]:
        for field in cfg["log_fields"]["custom_fields"]:
            name = field["name"]
            if name.startswith("cmcd_"):
                field["enabled"] = cmcd_enabled
            elif name.startswith("edge_"):
                field["enabled"] = scoring_enabled
            elif name.startswith("rum_") or name.startswith("rum"):
                field["enabled"] = rum_enabled

    return cfg


def run_draft_only_reconciliation(service_id: str, token: str) -> int:
    """Creates, configures, and validates a draft version on Fastly, but does NOT activate it."""
    with acquire_vcl_lock(service_id):
        # 1. Fetch current active state from Fastly
        print("Fetching active configuration state from Fastly...")
        current_snippets = _fetch_snippets(service_id, token)
        current_endpoints = _fetch_logging_endpoints(service_id, token)
        current_backends = _fetch_backends(service_id, token)
        current_backends = [b for b in current_backends if b.name in MANAGED_BACKEND_NAMES]
        current_dictionaries = _fetch_dictionaries(service_id, token)
        current_dictionaries = [d for d in current_dictionaries if d.name in MANAGED_DICTIONARY_NAMES]

        # 2. Build desired state from config file
        config_path = Path(f"configs/{service_id}.json")
        cfg = json.loads(config_path.read_text())
        desired_state = FeatureState.from_config(cfg)

        # 3. Compute VCL/endpoint diff
        print("Computing VCL configuration difference...")
        desired_snippets_list = desired_snippets(desired_state)
        desired_endpoints_list = desired_logging_endpoints(desired_state)
        desired_backends_list = desired_backends(desired_state)
        desired_dictionaries_list = desired_dictionaries(desired_state)

        diff = compute_diff(
            current_snippets=current_snippets,
            desired_snippets=desired_snippets_list,
            current_endpoints=current_endpoints,
            desired_endpoints=desired_endpoints_list,
            current_backends=current_backends,
            desired_backends=desired_backends_list,
            current_dictionaries=current_dictionaries,
            desired_dictionaries=desired_dictionaries_list,
        )

        print("DEBUG DIFF:")
        print("  - Current snippets count:", len(current_snippets))
        print("  - Desired snippets count:", len(desired_snippets_list))
        print("  - Current endpoints:", [e.name for e in current_endpoints])
        print("  - Desired endpoints:", [e.name for e in desired_endpoints_list])
        print("  - Endpoints to remove:", diff.endpoints_to_remove)

        # 4. Clone active version to draft
        print("Cloning active version to a new draft version on Fastly...")
        draft_version = _clone_active_version(service_id, token)
        print(f"Created draft version: {draft_version}")

        # 5. Apply diff on draft
        print("Applying snippets and VCL configuration to draft version...")
        _apply_diff(
            service_id,
            token,
            draft_version,
            diff,
            desired_endpoints=desired_endpoints_list,
            desired_state=desired_state,
            status_cb=print,
        )

        # 6. Validate draft with Fastly's validation API
        print("Validating draft configuration...")
        validation_errors = _validate_draft(service_id, token, draft_version)
        if validation_errors:
            print(f"Validation failed for draft version {draft_version}!")
            raise RuntimeError(f"VCL validation failed: {validation_errors}")

        print(f"✓ Draft version {draft_version} validated successfully!")

        print(f"Activating version {draft_version} on Fastly...")
        activate_version(service_id, draft_version, token)
        print(f"✓ Version {draft_version} activated and is now live!")

        return draft_version


def main():
    service_id = "rmWzCRA0lkAOs9Gnxvohs4"
    config_path = Path(__file__).resolve().parent.parent / "configs" / f"{service_id}.json"
    backup_path = Path(__file__).resolve().parent.parent / "configs" / f"{service_id}.json.bak"

    if not config_path.exists():
        print(f"Error: Base configuration file not found at {config_path}")
        sys.exit(1)

    if not backup_path.exists():
        shutil.copy(config_path, backup_path)
        print(f"Backed up base configuration to {backup_path}")
    else:
        print(f"Using existing backup configuration at {backup_path}")

    with open(backup_path) as f:
        base_cfg = json.load(f)

    token = base_cfg.get("fastly_api_key")
    if not token:
        print("Error: fastly_api_key is missing from configuration!")
        sys.exit(1)

    out_dir = Path(__file__).resolve().parent.parent / "vcl_reviews"
    out_dir.mkdir(exist_ok=True)

    scenarios = [
        # Scenario 5 (CMCD Off)
        {
            "id": 5,
            "desc": "Scenario 5: Log Analytics + RUM + Session Scoring, CMCD Off",
            "base_id": 5,
            "cmcd_enabled": False,
        },
    ]

    success_versions = {}

    try:
        for scen in scenarios:
            scenario_num = scen["id"]
            desc = scen["desc"]
            base_id = scen["base_id"]
            cmcd_enabled = scen["cmcd_enabled"]

            print("\n" + "=" * 80)
            print(f"GENERATING DRAFT VERSION FOR {desc.upper()}...")
            print("=" * 80)

            # 1. Build and write scenario config
            scenario_cfg = build_scenario_config(base_cfg, base_id, cmcd_enabled)
            with open(config_path, "w") as f:
                json.dump(scenario_cfg, f, indent=2)

            # 2. Run the draft-only reconciler
            draft_version = run_draft_only_reconciliation(service_id, token)
            success_versions[desc] = draft_version

            # 3. Pull actual compiled VCL and logging configs via API
            print(f"Retrieving fully compiled generated VCL from Fastly API for draft version {draft_version}...")
            live_vcl = get_generated_vcl(service_id, draft_version, token)
            live_endpoints = fetch_logging_endpoints(service_id, draft_version, token)

            # 4. Compile live VCL Output
            output_lines = []
            output_lines.append("=" * 80)
            output_lines.append("GATSBY SITE PRODUCTION VCL REVIEW (LIVE RECONCILED & ACTIVATED ON FASTLY)")
            output_lines.append(f"SCENARIO: {desc.upper()}")
            output_lines.append(f"Service ID: {service_id}")
            output_lines.append(f"Fastly Active Version ID (VALIDATED & ACTIVATED): {draft_version}")
            output_lines.append("=" * 80)
            output_lines.append("")

            # Log formats
            state = FeatureState.from_config(scenario_cfg)
            if state.logging_enabled:
                from backend.provision.declarative.generators import generate_log_format

                output_lines.append("--- LOG FORMAT (VCL FORMAT STRING) ---")
                output_lines.append(generate_log_format(state))
                output_lines.append("")

            # Logging endpoints
            if live_endpoints:
                output_lines.append("--- LIVE LOGGING ENDPOINTS (PULLED VIA FASTLY API) ---")
                for ep in live_endpoints:
                    output_lines.append(f"Endpoint Name: {ep.name}")
                    output_lines.append(f"Type: {ep.endpoint_type}")
                    output_lines.append(f"Path: {ep.path}")
                    output_lines.append(f"Period: {ep.period}s")
                    output_lines.append(f"Condition: {ep.response_condition}")
                    output_lines.append("")

            # Live compiled VCL
            if live_vcl:
                output_lines.append("--- FULLY COMPILED GENERATED VCL (PULLED VIA FASTLY API) ---")
                output_lines.append(live_vcl)
                output_lines.append("")

            # 5. Append JSON config at the bottom
            output_lines.append("")
            output_lines.append("=" * 80)
            output_lines.append("ACTIVE CONFIGURATION JSON AT THE TIME OF DEPLOYMENT")
            output_lines.append("=" * 80)
            output_lines.append(json.dumps(scenario_cfg, indent=2))

            # Save to text file
            safe_name = (
                desc.lower().replace(":", "").replace("+", "plus").replace(" ", "_").replace("(", "").replace(")", "")
            )
            filename = out_dir / f"{safe_name}.txt"
            with open(filename, "w") as f:
                f.write("\n".join(output_lines))

            print(f"Saved live pulled fully compiled VCL for Scenario {scenario_num} to {filename}")

    finally:
        # Restore the original base configuration
        if backup_path.exists():
            shutil.copy(backup_path, config_path)
            os.remove(backup_path)
            print("\nRestored original base configuration locally.")

    print("\nLive deployment scenarios completed successfully!")
    print(f"Generated draft versions summary: {success_versions}")


if __name__ == "__main__":
    main()
