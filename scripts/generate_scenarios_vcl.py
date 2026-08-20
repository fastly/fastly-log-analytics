import json
import sys
from pathlib import Path

# Adjust path to import backend modules
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.provision.declarative.generators import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
    generate_log_format,
    logging_service_snippets,
)
from backend.provision.declarative.state import FeatureState


def build_scenario_state(base_cfg: dict, scenario_num: int) -> FeatureState:
    cfg = json.loads(json.dumps(base_cfg))  # deep copy

    # Defaults across scenarios
    cfg["service_id"] = "rmWzCRA0lkAOs9Gnxvohs4"
    cfg["log_period"] = 60
    cfg["sample_rate"] = 100
    cfg["edge_only"] = True
    cfg["custom_condition"] = ""

    if scenario_num == 1:
        # Scenario 1: Deploy log analytics only with all fields enabled.
        cfg["logging_enabled"] = True
        cfg["rum_enabled"] = False
        cfg["scoring"] = {"enabled": False, "domain": "", "request_secret": "", "exclude_url_regex": ""}
        cfg["log_fields"]["groups"] = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L"]
        # Keep CMCD enabled or disabled? The prompt asks for log analytics only with all fields enabled.
        # Let's keep CMCD enabled if it's considered part of logging/all fields, but keep it matching base config.
        # Actually, let's keep CMCD enabled as in base config since it adds CMCD custom fields.
        cfg["cmcd"] = {"enabled": True, "mode": "headers", "version": 2}

    elif scenario_num == 2:
        # Scenario 2: Deploy RUM only
        cfg["logging_enabled"] = False
        cfg["rum_enabled"] = True
        cfg["scoring"] = {"enabled": False, "domain": "", "request_secret": "", "exclude_url_regex": ""}
        cfg["cmcd"] = {"enabled": False, "mode": "query_string", "version": 1}
        cfg["log_fields"]["groups"] = []

    elif scenario_num == 3:
        # Scenario 3: Deploy log analytics + RUM
        cfg["logging_enabled"] = True
        cfg["rum_enabled"] = True
        cfg["scoring"] = {"enabled": False, "domain": "", "request_secret": "", "exclude_url_regex": ""}
        cfg["cmcd"] = {"enabled": True, "mode": "headers", "version": 2}
        # Use standard active groups from the base config
        cfg["log_fields"]["groups"] = ["A", "B", "C", "D", "F", "G", "K", "L"]

    elif scenario_num == 4:
        # Scenario 4: Deploy log analytics + session scoring
        cfg["logging_enabled"] = True
        cfg["rum_enabled"] = False
        cfg["scoring"] = {
            "enabled": True,
            "domain": "fos-rmwzcra0lkaos9gnxvohs4-session-scorer.edgecompute.app",
            "request_secret": "my-test-secret-4321",
            "exclude_url_regex": "^/healthz.*",
            "enforce_status_code": 429,
        }
        cfg["cmcd"] = {"enabled": True, "mode": "headers", "version": 2}
        # Use standard active groups from the base config
        cfg["log_fields"]["groups"] = ["A", "B", "C", "D", "F", "G", "K", "L"]

    elif scenario_num == 5:
        # Scenario 5: Deploy log analytics + RUM + session scoring
        cfg["logging_enabled"] = True
        cfg["rum_enabled"] = True
        cfg["scoring"] = {
            "enabled": True,
            "domain": "fos-rmwzcra0lkaos9gnxvohs4-session-scorer.edgecompute.app",
            "request_secret": "my-test-secret-4321",
            "exclude_url_regex": "^/healthz.*",
            "enforce_status_code": 429,
        }
        cfg["cmcd"] = {"enabled": True, "mode": "headers", "version": 2}
        # Use standard active groups from the base config
        cfg["log_fields"]["groups"] = ["A", "B", "C", "D", "F", "G", "K", "L"]

    else:
        raise ValueError(f"Unknown scenario number {scenario_num}")

    return FeatureState.from_config(cfg)


def format_vcl_snippet(snippet) -> str:
    return f'"""\nSnippet Name: {snippet.name}\nSubroutine: {snippet.subroutine}\nPriority: {snippet.priority}\n"""\n{snippet.body}\n'


def main():
    config_path = Path(__file__).resolve().parent.parent / "configs" / "rmWzCRA0lkAOs9Gnxvohs4.json"
    if not config_path.exists():
        print(f"Error: Base configuration file not found at {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        base_cfg = json.load(f)

    out_dir = Path(__file__).resolve().parent.parent / "vcl_reviews"
    out_dir.mkdir(exist_ok=True)

    scenarios_desc = {
        1: "Scenario 1: Deploy log analytics only with all fields enabled",
        2: "Scenario 2: Deploy RUM only",
        3: "Scenario 3: Deploy log analytics + RUM",
        4: "Scenario 4: Deploy log analytics + session scoring",
        5: "Scenario 5: Deploy log analytics + RUM + session scoring",
    }

    for scenario_num, desc in scenarios_desc.items():
        print(f"Generating VCL for {desc}...")
        try:
            state = build_scenario_state(base_cfg, scenario_num)
        except Exception as e:
            print(f"Error building state for Scenario {scenario_num}: {e}")
            import traceback

            traceback.print_exc()
            continue

        # Compile VCL Outputs
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"GATSBY SITE PRODUCTION VCL REVIEW - {desc.upper()}")
        output_lines.append(f"Service ID: {state.service_id}")
        output_lines.append("=" * 80)
        output_lines.append("")

        # 1. Log Formats
        if state.logging_enabled:
            output_lines.append("--- LOG FORMAT (VCL FORMAT STRING) ---")
            output_lines.append(generate_log_format(state))
            output_lines.append("")

        # 2. Desired Log Endpoints
        endpoints = desired_logging_endpoints(state)
        if endpoints:
            output_lines.append("--- LOGGING ENDPOINTS ---")
            for ep in endpoints:
                output_lines.append(f"Endpoint Name: {ep.name}")
                output_lines.append(f"Type: {ep.endpoint_type}")
                output_lines.append(f"Path: {ep.path}")
                output_lines.append(f"Period: {ep.period}s")
                output_lines.append(f"Condition: {ep.response_condition}")
                output_lines.append("")

        # 3. Desired Backends
        backends = desired_backends(state)
        if backends:
            output_lines.append("--- BACKENDS DEPLOYED ---")
            for b in backends:
                output_lines.append(f"Backend Name: {b.name}")
                output_lines.append(f"Address: {b.address}:{b.port}")
                if b.ssl_hostname:
                    output_lines.append(f"SSL Hostname: {b.ssl_hostname}")
                if b.shield:
                    output_lines.append(f"Shield POP: {b.shield}")
                output_lines.append("")

        # 4. Desired Consolidated Snippets
        output_lines.append("--- CONSOLIDATED VCL SUBROUTINES ---")
        for snip in desired_snippets(state):
            output_lines.append(format_vcl_snippet(snip))
            output_lines.append("-" * 40)

        # 5. Logging Service Capture Snippets
        capture_snips = logging_service_snippets(state)
        if capture_snips:
            output_lines.append("--- LOGGING SERVICE CAPTURE SNIPPETS ---")
            for snip in capture_snips:
                output_lines.append(format_vcl_snippet(snip))
                output_lines.append("-" * 40)

        # Write to txt file
        safe_name = desc.lower().replace(":", "").replace("+", "plus").replace(" ", "_")
        filename = out_dir / f"{safe_name}.txt"
        with open(filename, "w") as f:
            f.write("\n".join(output_lines))

        print(f"Saved: {filename}")


if __name__ == "__main__":
    main()
