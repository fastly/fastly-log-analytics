# ruff: noqa: T201
"""Declarative VCL snippet, logging endpoint, backend, and dictionary builders."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from backend.core.fastly.client import (
    RUM_ASSET_FETCH_NAME,
    RUM_FARO_FETCH_NAME,
    generate_rum_asset_fetch_vcl,
    generate_rum_recv_parts,
    generate_rum_vcl,
)
from backend.provision import cli as log_paths
from backend.provision.cli import VclBuilder
from backend.provision.declarative.models import (
    Backend,
    DiffResult,
    FeatureState,
    LoggingEndpoint,
    ServiceDictionary,
    VCLSnippet,
)
from backend.provision.fastly_api import generate_capture_vcl
from backend.provision.session_scoring_orchestrator import (
    SCORING_BACKEND_API_NAME,
    generate_scoring_vcl,
)

_FORMAT_KEY_RE = re.compile(r'"([A-Za-z0-9_]+)"\s*:')


def _format_field_names(endpoints: list[LoggingEndpoint]) -> set[str]:
    fields: set[str] = set()
    for ep in endpoints:
        if ep.format_string:
            fields.update(_FORMAT_KEY_RE.findall(ep.format_string))
    return fields


def build_rum_log_condition(desired_state: FeatureState | None) -> str:
    """Build the VCL statement for the RUM beacon log-emission response condition."""
    statement = (
        'req.url.path == "/rum-beacon" && req.method == "POST" '
        "&& fastly.ff.visits_this_service == 0 && req.restarts == 0"
    )
    if (custom := getattr(desired_state, "rum_custom_condition", None)) and custom.strip():
        statement += f" && ({custom.strip()})"
    return statement


def _state_log_fields_dict(state: FeatureState) -> dict[str, Any]:
    return {
        "groups": state.log_fields.groups,
        "custom_fields": state.log_fields.custom_fields,
        "field_overrides": state.log_fields.field_overrides,
    }


def logging_service_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return capture snippets for the logging service."""
    if not state.logging_enabled:
        return []
    capture_vcl_dict = generate_capture_vcl(
        _state_log_fields_dict(state),
        scoring_enabled=state.scoring.enabled,
        rum_enabled=state.rum_enabled,
        scoring_exclude_url_regex=state.scoring.exclude_url_regex,
        cmcd_enabled=state.cmcd.enabled,
        cmcd_mode=state.cmcd.mode,
        cmcd_version=state.cmcd.version,
    )
    subroutine_map = {
        "recv": ("Fastly Log Analysis Capture", 1),
        "recv_reset": ("Fastly Log Analysis Reset Client IP", -100),
        "miss": ("Fastly Log Analysis Miss", 10),
        "pass": ("Fastly Log Analysis Pass", 10),
        "fetch": ("Fastly Log Analysis Origin Fetch", 10),
        "deliver": ("Fastly Log Analysis Origin Deliver", 10),
        "error": ("Fastly Log Analysis Origin Error", 10),
    }
    return [
        VCLSnippet(
            name=snippet_name,
            priority=priority,
            body=capture_vcl_dict[subroutine_key],
            subroutine="vcl_recv" if subroutine_key == "recv_reset" else f"vcl_{subroutine_key}",
        )
        for subroutine_key, (snippet_name, priority) in subroutine_map.items()
        if subroutine_key in capture_vcl_dict
    ]


def desired_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return the 5 consolidated snippets (vcl_recv, vcl_miss, vcl_fetch, vcl_deliver, vcl_error)."""
    snippets: list[VCLSnippet] = []
    for subroutine in ("vcl_recv", "vcl_miss", "vcl_pass", "vcl_fetch", "vcl_deliver", "vcl_error"):
        body = generate_consolidated_snippet(state, subroutine)
        snippets.append(
            VCLSnippet(name=f"Fastly Log Analytics - {subroutine}", priority=10, body=body, subroutine=subroutine)
        )
        declared_vars = re.findall(r"^\s*declare\s+local\s+var\.(\w+)", body, re.MULTILINE)
        duplicates = [v for v in set(declared_vars) if declared_vars.count(v) > 1]
        if duplicates:
            raise RuntimeError(f"{subroutine}: duplicate var declarations {duplicates}")
    return snippets


def generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for a single subroutine."""
    if subroutine != "vcl_recv":
        return _legacy_generate_consolidated_snippet(state, subroutine)
    sections: list[str] = []
    edge_first_hop_statements: list[str] = []
    log_fields_dict = _state_log_fields_dict(state)
    if state.logging_enabled:
        from backend.provision.fastly_api import get_scrub_vcl_statements

        edge_first_hop_statements.extend(
            [
                "  # Section 6: Log Field Capture (vcl_recv)",
                "  # [security] strip client-supplied internal-routing headers",
                *(f"  {scrub}" for scrub in get_scrub_vcl_statements(log_fields_dict)),
            ]
        )
    if state.rum_enabled:
        edge_first_hop_statements.extend(
            [
                "  # Mint a per-request ID for RUM beacons on fresh requests",
                "  set req.http.x-rum-req-id = randomstr(12);",
            ]
        )
    if state.rum_enabled and state.logging_enabled:
        rum_extraction, _ = generate_rum_recv_parts()
        edge_first_hop_statements.extend(
            [
                "  # Section 5: RUM field extraction (vcl_recv)",
                '  if (req.url.path == "/rum-beacon") {',
                *(f"    {line}" for line in rum_extraction.splitlines()),
                "  }",
            ]
        )
    if state.logging_enabled:
        from backend.provision.fastly_api import get_capture_vcl_statements

        edge_first_hop_statements.append("")
        if state.cmcd.enabled:
            from backend.provision.cmcd_fields import generate_cmcd_vcl

            cmcd_body = next(iter(generate_cmcd_vcl(mode=state.cmcd.mode, version=state.cmcd.version).values()))
            edge_first_hop_statements.append("  # Section 3: CMCD Extraction (vcl_recv) — before field capture")
            edge_first_hop_statements.extend(f"  {line}" for line in cmcd_body.splitlines() if line.strip())
        edge_first_hop_statements.append("  # Capture edge data for logging before shielding or backend fetch")
        edge_first_hop_statements.extend(f"  {cap}" for cap in get_capture_vcl_statements(log_fields_dict))
    if state.rum_enabled:
        _, rum_response = generate_rum_recv_parts()
        edge_first_hop_statements.extend(
            [
                "  # Section 5: RUM (vcl_recv)",
                '  if (req.url.path == "/rum-beacon") {',
                *(f"    {line}" for line in rum_response.splitlines()),
                "  }",
            ]
        )
    if state.scoring.enabled:
        from backend.provision.session_scoring_orchestrator import SCORING_BACKEND_VCL_NAME, resolve_exclude_url_regex

        effective_regex = resolve_exclude_url_regex(state.scoring.exclude_url_regex)
        rum_asset_guard = (
            ' && req.url.path != "/js/rum.js" && req.url.path != "/js/faro-sdk.js"' if state.rum_enabled else ""
        )
        edge_first_hop_statements.extend(
            [
                "",
                "  # Section 4: Session Scoring (vcl_recv)",
                "  # Session Scoring: route the first-pass dynamic request to the scorer.",
                "  # Edge-only — fastly.ff.visits_this_service == 0 is true only at the true edge.",
                *(
                    ["  # RUM assets we serve ourselves are never scored — see rum_asset_guard in generators.py."]
                    if rum_asset_guard
                    else []
                ),
                f'  if (req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected{rum_asset_guard}'
                f' && std.tolower(req.url) !~ "{effective_regex}") {{',
                f"    set req.backend = {SCORING_BACKEND_VCL_NAME};",
                "    # Skip NGWAF inspection on the scoring sub-fetch ONLY.",
                '    set req.http.x-sigsci-skip-inspection-once = "true";',
                '    set req.http.X-Edge-Scoring-Pass = "1";',
                "    # Stamp the round-trip start so deliver can compute latency.",
                "    set req.http.x-edge-score-t0 = time.elapsed.usec;",
                "    # PASS — skip cache for the scoring sub-fetch.",
                "    return(pass);",
                "  }",
            ]
        )
    if edge_first_hop_statements:
        body = VclBuilder.join_lines(edge_first_hop_statements)
        sections.append(f"if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{\n{body}\n}}")
    if state.rum_enabled:
        from backend.core.fastly.client import SHIELD_MAP

        shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
        sections.append(generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)[RUM_ASSET_FETCH_NAME])
    if state.scoring.enabled:
        sections.append(_generate_scoring_section_vcl(state, "vcl_recv"))
    return VclBuilder.join_sections(sections)


def _legacy_generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for other subroutines using legacy multi-section append logic."""
    sections: list[str] = []
    if state.logging_enabled:
        capture_snippets = generate_capture_vcl(
            _state_log_fields_dict(state),
            scoring_enabled=state.scoring.enabled,
            rum_enabled=state.rum_enabled,
            scoring_exclude_url_regex=state.scoring.exclude_url_regex,
            cmcd_enabled=state.cmcd.enabled,
            cmcd_mode=state.cmcd.mode,
            cmcd_version=state.cmcd.version,
        )
        capture_vcl = capture_snippets.get(subroutine.replace("vcl_", ""), "")
        if capture_vcl.strip():
            sections.append(f"# Section 6: Log Field Capture ({subroutine})\n{capture_vcl}")
    if state.scoring.enabled:
        sections.append(_generate_scoring_section_vcl(state, subroutine))
    if state.rum_enabled:
        sections.append(_generate_rum_section_vcl(state, subroutine))
    return VclBuilder.join_sections(sections)


def _generate_scoring_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 4: Session Scoring (if enabled)."""
    if not state.scoring.enabled:
        return ""
    scoring_vcl_dict = generate_scoring_vcl(
        logging_service_id=state.service_id,
        request_secret=state.scoring.request_secret,
        exclude_url_regex=state.scoring.exclude_url_regex,
        enforce_status_code=state.scoring.enforce_status_code,
    )
    snippet_map = {
        "vcl_deliver": "Session Scoring - Deliver",
        "vcl_fetch": "Session Scoring - Fetch",
        "vcl_pass": "Session Scoring - Pass",
    }
    bodies = [scoring_vcl_dict[snippet_map[subroutine]]] if snippet_map.get(subroutine) in scoring_vcl_dict else []
    if subroutine == "vcl_recv":
        if "Session Scoring - Enforce" in scoring_vcl_dict:
            bodies.append(scoring_vcl_dict["Session Scoring - Enforce"])
        bodies.append(
            "# Session Scoring: post-scoring restart shielding & NGWAF restore.\n"
            "if (req.restarts == 1 && req.http.x-edge-score) {\n"
            "  set var.fastly_req_do_shield = true;\n"
            "}\n"
            "if (req.restarts == 1) {\n"
            "  unset req.http.x-sigsci-skip-inspection-once;\n"
            "}"
        )
    return f"# Section 4: Session Scoring ({subroutine})\n" + "\n\n".join(bodies) if bodies else ""


def _generate_rum_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 5: RUM (if enabled, includes Phase 1 + Phase 3 asset-fetch)."""
    if not state.rum_enabled:
        return ""
    from backend.core.fastly.client import SHIELD_MAP

    rum_vcl_dict = generate_rum_vcl(state.service_id, state.faro_version)
    shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
    asset_fetch_dict = generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)
    if subroutine == "vcl_recv":
        sections = ([rum_vcl_dict["RUM - Recv"]] if "RUM - Recv" in rum_vcl_dict else []) + [
            asset_fetch_dict["RUM - Asset fetch FOS"]
        ]
        return f"# Section 5: RUM ({subroutine})\n" + "\n\n".join(sections)
    if subroutine == "vcl_deliver":
        return f"# Section 5: RUM ({subroutine})\n{rum_vcl_dict['RUM - Set cookies']}" if state.scoring.enabled else ""
    if subroutine == "vcl_miss":
        return f"# Section 5: RUM ({subroutine})\n{asset_fetch_dict['RUM - Asset fetch SigV4 signing']}"
    if subroutine == "vcl_error":
        from backend.core.fastly.client import _generate_vcl_error_handler

        return f"# Section 5: RUM ({subroutine})\n{_generate_vcl_error_handler()}"
    if subroutine == "vcl_fetch":
        body = """# Cache RUM tracker JS at the edge; browsers get a short TTL so a
# Surrogate-Key purge of "rum-js" becomes visible to clients quickly.
if (req.url.path == "/js/rum.js") {
    if (beresp.status == 200) {
        set beresp.ttl = 86400s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-js";
        set beresp.http.Surrogate-Control = "max-age=86400";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""
        if RUM_FARO_FETCH_NAME in asset_fetch_dict:
            body = body + "\n\n" + asset_fetch_dict[RUM_FARO_FETCH_NAME]
        return f"# Section 5: RUM ({subroutine})\n{body}"
    return ""


def generate_log_format(state: FeatureState) -> str:
    """Generate complete log format VCL expression."""
    from backend.provision.fastly_api import load_log_format
    from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

    custom_fields = [cf for cf in state.log_fields.custom_fields if cf.get("name") not in _RUM_FIELD_NAMES]
    return load_log_format(
        {
            "groups": state.log_fields.groups,
            "custom_fields": custom_fields,
            "field_overrides": state.log_fields.field_overrides,
        }
    )


def desired_logging_endpoints(state: FeatureState) -> list[LoggingEndpoint]:
    """Return desired logging endpoints (main S3 + secondary RUM if enabled)."""
    endpoints: list[LoggingEndpoint] = []
    if state.logging_enabled:
        endpoints.append(
            LoggingEndpoint(
                name=state.logging_endpoint_name,
                endpoint_type="s3",
                path=log_paths.analytics_log_path(),
                period=state.log_period,
                response_condition="log_analytics_condition",
                format_string=generate_log_format(state),
                placement=None,
                response_object_name="",
            )
        )
    if state.rum_enabled:
        from backend.provision.fastly_api import load_log_format
        from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

        rum_fields = [f for f in state.log_fields.custom_fields if f.get("name") in _RUM_FIELD_NAMES]
        endpoints.append(
            LoggingEndpoint(
                name=state.rum_endpoint_name,
                endpoint_type="s3",
                path=log_paths.rum_log_path(),
                period=state.rum_log_period if state.rum_log_period is not None else state.log_period,
                response_condition="rum_log_condition",
                format_string=load_log_format({"custom_fields": rum_fields}),
                placement=None,
                response_object_name="",
            )
        )
    return endpoints


def desired_backends(state: FeatureState) -> list[Backend]:
    """Return desired backends (session_scorer, fos_origin, rum_collector)."""
    backends: list[Backend] = []
    if state.scoring.enabled:
        backends.append(
            Backend(
                name=SCORING_BACKEND_API_NAME,
                address=state.scoring.domain,
                port=443,
                ssl_check_cert=False,
                ssl_hostname=state.scoring.domain,
                connect_timeout=100,
                first_byte_timeout=100,
                between_bytes_timeout=200,
                auto_loadbalance=False,
                request_condition="fastly_log_analytics_false",
            )
        )
    if state.rum_enabled:
        from backend.core.fastly.client import SHIELD_MAP

        shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
        backends.append(
            Backend(
                name="fos_origin",
                address=state.fos_endpoint,
                port=443,
                ssl_check_cert=True,
                ssl_hostname=state.fos_endpoint,
                use_ssl=True,
                override_host=state.fos_endpoint,
                auto_loadbalance=False,
                shield=shield_pop if shield_pop.lower() != "none" else "",
                request_condition="fastly_log_analytics_false",
            )
        )
    return backends


def desired_dictionaries(state: FeatureState) -> list[ServiceDictionary]:
    """Return desired Fastly service dictionaries (fos_credentials, etc)."""
    if not state.rum_enabled:
        return []
    items = (
        {
            "access_key": state.fos_access_key_id,
            "secret_key": state.fos_secret_access_key,
            "bucket": state.fos_bucket,
            "region": state.fos_region,
        }
        if state.fos_access_key_id
        else {}
    )
    return [ServiceDictionary(name="fos_credentials", write_only=True, items=items)]


def _is_managed_snippet(name: str) -> bool:
    return name.startswith("Fastly Log Analytics - ") or _is_legacy_snippet(name)


def _is_legacy_snippet(name: str) -> bool:
    if name.startswith("Fastly Log Analytics - "):
        return False
    return any(
        name.startswith(p) for p in ("Fastly Log Analysis", "Fastly Log Analytics", "RUM -", "Session Scoring", "CMCD")
    )


def _detect_and_queue_legacy_cleanup(
    current_snippets: list[VCLSnippet], diff: DiffResult, status_cb: Callable[[str], None] | None = None
) -> None:
    if legacy := [s for s in current_snippets if _is_legacy_snippet(s.name)]:
        if status_cb:
            status_cb(
                f"⏳ Detected {len(legacy)} legacy VCL snippet(s) from prior deployment — will remove during reconciliation…"
            )
        for snippet in legacy:
            if snippet.name not in diff.snippets_to_remove:
                diff.snippets_to_remove.append(snippet.name)
