"""Pure VCL and logging configuration generation from FeatureState.

This module contains pure functions that assemble VCL snippets in a deterministic,
feature-complete manner. No side effects — only return generated content.
"""

from __future__ import annotations

import re

from backend.core.fastly.rum_provisioning import (
    RUM_ASSET_FETCH_NAME,
    RUM_FARO_FETCH_NAME,
    generate_rum_asset_fetch_vcl,
    generate_rum_vcl,
)
from backend.provision import log_paths
from backend.provision.declarative.diff import Backend, LoggingEndpoint, ServiceDictionary, VCLSnippet
from backend.provision.declarative.state import FeatureState
from backend.provision.fastly_api import generate_capture_vcl
from backend.provision.session_scoring_vcl import (
    SCORING_BACKEND_API_NAME,
    generate_scoring_vcl,
)


def logging_service_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return capture snippets for the logging service.

    The logging service needs capture snippets for all stages (recv, miss, pass,
    and optionally fetch, deliver, error when Group L is enabled). The recv snippet
    is the primary one that triggers logging ("Fastly Log Analysis Capture").

    Args:
        state: Feature state.

    Returns:
        List of logging-service-specific VCL snippets.
    """
    if not state.logging_enabled:
        return []

    log_fields_dict = {
        "groups": state.log_fields.groups,
        "custom_fields": state.log_fields.custom_fields,
        "field_overrides": state.log_fields.field_overrides,
    }
    capture_vcl_dict = generate_capture_vcl(
        log_fields_dict,
        scoring_enabled=state.scoring.enabled,
        rum_enabled=state.rum_enabled,
        scoring_exclude_url_regex=state.scoring.exclude_url_regex,
        cmcd_enabled=state.cmcd.enabled,
        cmcd_mode=state.cmcd.mode,
        cmcd_version=state.cmcd.version,
    )

    snippets: list[VCLSnippet] = []

    # Map subroutine names to snippet names and priorities
    # Main capture snippet has priority 1 (higher = earlier); reset is -100
    subroutine_map = {
        "recv": ("Fastly Log Analysis Capture", 1),
        "recv_reset": ("Fastly Log Analysis Reset Client IP", -100),
        "miss": ("Fastly Log Analysis Miss", 10),
        "pass": ("Fastly Log Analysis Pass", 10),
        "fetch": ("Fastly Log Analysis Origin Fetch", 10),
        "deliver": ("Fastly Log Analysis Origin Deliver", 10),
        "error": ("Fastly Log Analysis Origin Error", 10),
    }

    # Create snippets for all available subroutines
    for subroutine_key, (snippet_name, priority) in subroutine_map.items():
        if subroutine_key in capture_vcl_dict:
            # Map subroutine keys to vcl_ prefixed names
            if subroutine_key == "recv_reset":
                vcl_subroutine = "vcl_recv"
            else:
                vcl_subroutine = f"vcl_{subroutine_key}"

            snippets.append(
                VCLSnippet(
                    name=snippet_name,
                    priority=priority,
                    body=capture_vcl_dict[subroutine_key],
                    subroutine=vcl_subroutine,
                )
            )

    return snippets


def desired_snippets(state: FeatureState) -> list[VCLSnippet]:
    """Return the 5 consolidated snippets (vcl_recv, vcl_miss, vcl_fetch, vcl_deliver, vcl_error).

    Assembles RUM + Scoring + CMCD fragments in deterministic order:
    Section 1: Variable declarations
    Section 2: Security header & client IP scrubbing
    Section 3: Common Media Client Data (CMCD) extraction (if enabled)
    Section 4: Session Scoring request-restart hook (if enabled)
    Section 5: RUM beacon interception & asset routing (if enabled)

    Args:
        state: Feature state (controls which features are enabled).

    Returns:
        List of 5 consolidated VCL snippets with priority=10.

    Assertions:
        - No duplicate variable declarations in any subroutine.
        - All referenced log fields are available.
    """
    subroutines = ["vcl_recv", "vcl_miss", "vcl_pass", "vcl_fetch", "vcl_deliver", "vcl_error"]
    snippets = []

    for subroutine in subroutines:
        body = generate_consolidated_snippet(state, subroutine)
        snippet = VCLSnippet(
            name=f"Fastly Log Analytics - {subroutine}",
            priority=10,
            body=body,
            subroutine=subroutine,
        )
        snippets.append(snippet)

        # Assertion: no duplicate variable declarations
        var_decl_pattern = r"^\s*declare\s+local\s+var\.(\w+)"
        declared_vars = re.findall(var_decl_pattern, body, re.MULTILINE)
        duplicates = [v for v in set(declared_vars) if declared_vars.count(v) > 1]
        assert not duplicates, f"{subroutine}: duplicate var declarations {duplicates}"

    return snippets


def generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for a single subroutine."""
    if subroutine != "vcl_recv":
        # Keep legacy behavior for other subroutines
        return _legacy_generate_consolidated_snippet(state, subroutine)

    sections: list[str] = []

    # 1. Zone 1: At the Top at the Edge
    edge_first_hop_statements: list[str] = []

    # A. Header scrubbing first (Label as Section 6)
    if state.logging_enabled:
        from backend.provision.fastly_api import get_scrub_vcl_statements

        log_fields_dict = {
            "groups": state.log_fields.groups,
            "custom_fields": state.log_fields.custom_fields,
            "field_overrides": state.log_fields.field_overrides,
        }
        edge_first_hop_statements.append("  # Section 6: Log Field Capture (vcl_recv)")
        edge_first_hop_statements.append("  # [security] strip client-supplied internal-routing headers")
        for scrub in get_scrub_vcl_statements(log_fields_dict):
            edge_first_hop_statements.append(f"  {scrub}")

    # B. Minting request ID (if RUM enabled)
    if state.rum_enabled:
        edge_first_hop_statements.append("  # Mint a per-request ID for RUM beacons on fresh requests")
        edge_first_hop_statements.append("  set req.http.x-rum-req-id = randomstr(12);")

    # C. Edge capture only (if logging enabled)
    # MUST go before RUM beacon interception so the beacon inherits the geolocation / network headers!
    if state.logging_enabled:
        from backend.provision.fastly_api import get_capture_vcl_statements

        log_fields_dict = {
            "groups": state.log_fields.groups,
            "custom_fields": state.log_fields.custom_fields,
            "field_overrides": state.log_fields.field_overrides,
        }
        edge_first_hop_statements.append("")

        # CMCD extraction MUST precede the capture statements: capture promotes
        # req.http.x-cmcd:<key> into req.http.x-fos-edge-data:cmcd_<key>, which
        # is what the log format reads. Extracting after it copies empty
        # strings — the 2026-08 CMCD outage, silent because the extraction still
        # works (it strips ?CMCD= from the cache key) and the columns exist.
        if state.cmcd.enabled:
            from backend.provision.cmcd_vcl import generate_cmcd_vcl

            cmcd_vcl_dict = generate_cmcd_vcl(mode=state.cmcd.mode, version=state.cmcd.version)
            cmcd_body = next(iter(cmcd_vcl_dict.values()))
            edge_first_hop_statements.append("  # Section 3: CMCD Extraction (vcl_recv) — before field capture")
            for line in cmcd_body.splitlines():
                if line.strip():
                    edge_first_hop_statements.append(f"  {line}")

        edge_first_hop_statements.append("  # Capture edge data for logging before shielding or backend fetch")
        for cap in get_capture_vcl_statements(log_fields_dict):
            edge_first_hop_statements.append(f"  {cap}")

    # D. Beacon interception POST to /rum-beacon (Section 5 RUM)
    # Placed AFTER the standard captures block!
    if state.rum_enabled:
        edge_first_hop_statements.append("  # Section 5: RUM (vcl_recv)")
        edge_first_hop_statements.append("  # Handle RUM beacon POST to /rum-beacon")
        edge_first_hop_statements.append('  if (req.url.path == "/rum-beacon") {')
        edge_first_hop_statements.append("      # Extract the essential fields from querystring:")
        edge_first_hop_statements.append("      # - cid: session ID from rum_cid cookie (set in deliver)")
        edge_first_hop_statements.append("      # - req: per-request ID (minted in recv)")
        edge_first_hop_statements.append("      # - raw query: complete set of event_N_* params, parsed during ingest")
        edge_first_hop_statements.append(
            '      set req.http.x-fos-edge-data:rum_cid = querystring.get(req.url, "cid");'
        )
        edge_first_hop_statements.append(
            '      set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");'
        )
        edge_first_hop_statements.append('      if (req.http.x-fos-edge-data:fastly_req_id == "") {')
        edge_first_hop_statements.append(
            "          set req.http.x-fos-edge-data:fastly_req_id = req.http.Fastly-Request-ID;"
        )
        edge_first_hop_statements.append("      }")
        edge_first_hop_statements.append("      set req.http.x-fos-edge-data:rum_raw_query = req.url;")
        edge_first_hop_statements.append("      set req.http.x-fos-edge-data:rum_body = req.body;")
        edge_first_hop_statements.append(
            "      # Mark beacon to skip S3 logging (already logged separately to metadata DB)"
        )
        edge_first_hop_statements.append('      set req.http.x-skip-rum-logging = "1";')
        edge_first_hop_statements.append("      # Synthetic 204 response (no origin round-trip needed)")
        edge_first_hop_statements.append('      error 611 "No Content";')
        edge_first_hop_statements.append("  }")

    # E. Session Scoring first-pass routing (Section 4 Session Scoring)
    # Placed at the very end of the consolidated block!
    if state.scoring.enabled:
        from backend.provision.session_scoring_vcl import SCORING_BACKEND_VCL_NAME, resolve_exclude_url_regex

        effective_regex = resolve_exclude_url_regex(state.scoring.exclude_url_regex)

        # The two RUM scripts we host are excluded structurally, not via
        # exclude_url_regex. DEFAULT_ASSET_EXT_REGEX does already match
        # ".js", but that default is operator-overridable — a narrower
        # override would silently start routing our own asset requests
        # through the scorer. These paths are infrastructure, not scoreable
        # visitor navigation, so the guard must not depend on operator
        # config. Exact path equality (not a regex) mirrors how the
        # asset-fetch routing itself tests these paths, so the two can't
        # drift apart.
        #
        # This is also a correctness fix, not just noise reduction: the
        # scoring block ends in return(pass), which exits vcl_recv before
        # the RUM asset routing below it ever runs. Without this guard a
        # GET /js/rum.js pays a full scorer round-trip and only reaches FOS
        # on the post-scoring restart.
        rum_asset_guard = ""
        if state.rum_enabled:
            rum_asset_guard = ' && req.url.path != "/js/rum.js" && req.url.path != "/js/faro-sdk.js"'

        edge_first_hop_statements.append("")
        edge_first_hop_statements.append("  # Section 4: Session Scoring (vcl_recv)")
        edge_first_hop_statements.append("  # Session Scoring: route the first-pass dynamic request to the scorer.")
        edge_first_hop_statements.append(
            "  # Edge-only — fastly.ff.visits_this_service == 0 is true only at the true edge."
        )
        if rum_asset_guard:
            edge_first_hop_statements.append(
                "  # RUM assets we serve ourselves are never scored — see rum_asset_guard in generators.py."
            )
        edge_first_hop_statements.append(
            f'  if (req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected{rum_asset_guard}'
            f' && std.tolower(req.url) !~ "{effective_regex}") {{'
        )
        edge_first_hop_statements.append(f"    set req.backend = {SCORING_BACKEND_VCL_NAME};")
        edge_first_hop_statements.append("    # Skip NGWAF inspection on the scoring sub-fetch ONLY.")
        edge_first_hop_statements.append('    set req.http.x-sigsci-skip-inspection-once = "true";')
        edge_first_hop_statements.append('    set req.http.X-Edge-Scoring-Pass = "1";')
        edge_first_hop_statements.append("    # Stamp the round-trip start so deliver can compute latency.")
        edge_first_hop_statements.append("    set req.http.x-edge-score-t0 = time.elapsed.usec;")
        edge_first_hop_statements.append("    # PASS — skip cache for the scoring sub-fetch.")
        edge_first_hop_statements.append("    return(pass);")
        edge_first_hop_statements.append("  }")

    # Now wrap all edge_first_hop_statements in a SINGLE condition block!
    if edge_first_hop_statements:
        body = "\n".join(edge_first_hop_statements)
        sections.append(f"if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{\n{body}\n}}")

    # B. RUM Phase 3 Routing (asset GET /js/rum.js [+ /js/faro-sdk.js])
    # Sourced from backend.core.fastly.rum_provisioning so there is a single
    # place that defines this routing — see that module's
    # _generate_asset_fetch_vcl for the shield/backend-selection logic and
    # the faro_version handling.
    if state.rum_enabled:
        from backend.core.fastly.utils import SHIELD_MAP

        shield_pop = state.cdn_shield or SHIELD_MAP.get(state.fos_region, "iad-va-us")
        asset_fetch_dict = generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)
        sections.append(asset_fetch_dict[RUM_ASSET_FETCH_NAME])

    # C. Session Scoring Enforcement (Section 4)
    if state.scoring.enabled:
        scoring_body = _generate_scoring_section_vcl(state, "vcl_recv")
        sections.append(scoring_body)

    return "\n\n".join(s for s in sections if s.strip())


def _legacy_generate_consolidated_snippet(state: FeatureState, subroutine: str) -> str:
    """Generate consolidated VCL for other subroutines using legacy multi-section append logic."""
    sections: list[str] = []

    # Section 1: Variable declarations (if any feature needs them)
    var_decls: list[str] = []
    sections.append("\n".join(var_decls) if var_decls else "")

    # Section 6: Standard & Custom Field Captures (if logging enabled)
    if state.logging_enabled:
        from backend.provision.fastly_api import generate_capture_vcl

        log_fields_dict = {
            "groups": state.log_fields.groups,
            "custom_fields": state.log_fields.custom_fields,
            "field_overrides": state.log_fields.field_overrides,
        }
        capture_snippets = generate_capture_vcl(
            log_fields_dict,
            scoring_enabled=state.scoring.enabled,
            rum_enabled=state.rum_enabled,
            scoring_exclude_url_regex=state.scoring.exclude_url_regex,
            cmcd_enabled=state.cmcd.enabled,
            cmcd_mode=state.cmcd.mode,
            cmcd_version=state.cmcd.version,
        )
        sub_key = subroutine.replace("vcl_", "")
        capture_vcl = capture_snippets.get(sub_key, "")
        if capture_vcl.strip():
            sections.append(f"# Section 6: Log Field Capture ({subroutine})\n{capture_vcl}")

    # Section 2: Security header & client IP scrubbing (always)
    if subroutine == "vcl_recv":
        sections.append(_generate_security_headers_vcl())

    # Section 3: CMCD Extraction (if enabled)
    if state.cmcd.enabled:
        cmcd_body = _generate_cmcd_section_vcl(state, subroutine)
        sections.append(cmcd_body)

    # Section 4: Session Scoring (if enabled)
    if state.scoring.enabled:
        scoring_body = _generate_scoring_section_vcl(state, subroutine)
        sections.append(scoring_body)

    # Section 5: RUM (if enabled)
    if state.rum_enabled:
        rum_body = _generate_rum_section_vcl(state, subroutine)
        sections.append(rum_body)

    # Join non-empty sections with blank lines
    consolidated = "\n\n".join(s for s in sections if s.strip())
    return consolidated


def _generate_security_headers_vcl() -> str:
    """Section 2: Security header & client IP scrubbing (consolidated into Section 6)."""
    return ""


def _generate_cmcd_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 3: CMCD extraction (if enabled, runs in vcl_recv).

    Under declarative consolidation, CMCD extraction is generated natively
    inside generate_capture_vcl (Section 6) so that it is nested inside the
    unforgeable edge-detect block. Returning empty here avoids duplicate code.
    """
    return ""


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

    # Map subroutine names to snippet names from the generator
    snippet_map = {
        "vcl_deliver": "Session Scoring - Deliver",
        "vcl_fetch": "Session Scoring - Fetch",
        "vcl_pass": "Session Scoring - Pass",
    }

    bodies = []
    snippet_name = snippet_map.get(subroutine)
    if snippet_name and snippet_name in scoring_vcl_dict:
        bodies.append(scoring_vcl_dict[snippet_name])

    # Enforce snippet is also a recv snippet and should run after Recv
    if subroutine == "vcl_recv":
        if "Session Scoring - Enforce" in scoring_vcl_dict:
            bodies.append(scoring_vcl_dict["Session Scoring - Enforce"])

        # Post-scoring restart logic
        restart_vcl = (
            "# Session Scoring: post-scoring restart shielding & NGWAF restore.\n"
            "if (req.restarts == 1 && req.http.x-edge-score) {\n"
            "  set var.fastly_req_do_shield = true;\n"
            "}\n"
            "if (req.restarts == 1) {\n"
            "  unset req.http.x-sigsci-skip-inspection-once;\n"
            "}"
        )
        bodies.append(restart_vcl)

    if bodies:
        body = "\n\n".join(bodies)
        return f"# Section 4: Session Scoring ({subroutine})\n{body}"

    return ""


def _generate_rum_section_vcl(state: FeatureState, subroutine: str) -> str:
    """Section 5: RUM (if enabled, includes Phase 1 + Phase 3 asset-fetch)."""
    if not state.rum_enabled:
        return ""

    rum_vcl_dict = generate_rum_vcl(state.service_id, state.faro_version)
    shield_pop = state.cdn_shield
    if not shield_pop:
        from backend.core.fastly.utils import SHIELD_MAP

        shield_pop = SHIELD_MAP.get(state.fos_region, "iad-va-us")
    asset_fetch_dict = generate_rum_asset_fetch_vcl(shield_pop, state.faro_version)

    # Map subroutine names to snippet names from the generator
    # Phase 1: recv + deliver
    # Phase 3: asset-fetch + sigv4 signing + error handler
    snippet_map = {
        "vcl_recv": "RUM - Recv",
        "vcl_deliver": "RUM - Set cookies",
        "vcl_miss": "RUM - Asset fetch SigV4 signing",
        "vcl_error": "RUM - Recv",  # placeholder, will be overridden below
    }

    sections = []

    # Handle vcl_recv: include asset-fetch logic and RUM-Recv beacon logic
    if subroutine == "vcl_recv":
        if "RUM - Recv" in rum_vcl_dict:
            sections.append(rum_vcl_dict["RUM - Recv"])
        sections.append(asset_fetch_dict["RUM - Asset fetch FOS"])
        body = "\n\n".join(sections)
        return f"# Section 5: RUM ({subroutine})\n{body}"

    # Handle vcl_deliver: just deliver logic (only relevant if scoring is also enabled to get session ID)
    if subroutine == "vcl_deliver":
        if not state.scoring.enabled:
            return ""
        body = rum_vcl_dict["RUM - Set cookies"]
        return f"# Section 5: RUM ({subroutine})\n{body}"

    # Handle vcl_miss: include SigV4 signing logic (only needed in vcl_miss, never vcl_pass)
    if subroutine == "vcl_miss":
        body = asset_fetch_dict["RUM - Asset fetch SigV4 signing"]
        return f"# Section 5: RUM ({subroutine})\n{body}"

    # Handle vcl_error: include error handler for synthetic 611 response
    if subroutine == "vcl_error":
        from backend.core.fastly.rum_provisioning import _generate_vcl_error_handler

        body = _generate_vcl_error_handler()
        return f"# Section 5: RUM ({subroutine})\n{body}"

    # Handle vcl_fetch: long edge TTL, short browser TTL, tagged for purging.
    #
    # The ?v= hash in the embed snippet (frontend/app/rum/_sections/
    # RumPageClient.tsx) is derived from the service id, NOT the tracker
    # contents, so the URL is stable across tracker updates. The previous
    # "max-age=86400, public, immutable" therefore meant a browser kept
    # serving a stale tracker for up to 24h after a reconcile, and no purge
    # could reach it — purging only clears the edge. Same decoupling as the
    # Faro bundle: Surrogate-Control (edge-only) long, Cache-Control
    # (browser-visible) short, no `immutable`.
    #
    # Gated on beresp.status == 200 for the same reason /js/faro-sdk.js is
    # (the F-3 audit finding): without the check, a transient FOS 403/404 —
    # mid-upload, or a bucket-policy blip — gets cached at the edge for a
    # full day, so every visitor loads a broken tracker until the TTL
    # expires or someone notices and purges.
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
    """Generate complete log format VCL expression.

    Includes all standard Fastly fields plus all custom fields from state,
    excluding RUM-specific fields which are exclusively logged on the secondary RUM endpoint.

    Args:
        state: Feature state.

    Returns:
        VCL log format expression.

    Assertions:
        - All referenced custom fields are defined.
    """
    from backend.provision.fastly_api import load_log_format
    from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

    # Exclude RUM beacon fields from the main requests log format to prevent bloat
    custom_fields = [cf for cf in state.log_fields.custom_fields if cf.get("name") not in _RUM_FIELD_NAMES]

    log_fields_dict = {
        "groups": state.log_fields.groups,
        "custom_fields": custom_fields,
        "field_overrides": state.log_fields.field_overrides,
    }
    return load_log_format(log_fields_dict)


def desired_logging_endpoints(state: FeatureState) -> list[LoggingEndpoint]:
    """Return desired logging endpoints (main S3 + secondary RUM if enabled).

    Args:
        state: Feature state.

    Returns:
        List of logging endpoints to deploy.
    """
    endpoints: list[LoggingEndpoint] = []

    # Main S3 logging endpoint
    # Note: response_condition must be "true" or a named condition name, not a VCL expression.
    if state.logging_enabled:
        log_format = generate_log_format(state)
        endpoints.append(
            LoggingEndpoint(
                name=state.logging_endpoint_name,
                endpoint_type="s3",
                path=log_paths.analytics_log_path(state.fos_prefix),
                period=state.log_period,
                response_condition="log_analytics_condition",
                format_string=log_format,
                placement=None,
                response_object_name="",
            )
        )

    # Secondary RUM endpoint (if RUM enabled)
    if state.rum_enabled:
        from backend.provision.fastly_api import load_log_format
        from backend.provision.rum_orchestrator_v2 import _RUM_FIELD_NAMES

        rum_fields = [f for f in state.log_fields.custom_fields if f.get("name") in _RUM_FIELD_NAMES]
        rum_log_fields = {"custom_fields": rum_fields}
        rum_format = load_log_format(rum_log_fields)

        endpoints.append(
            LoggingEndpoint(
                name=state.rum_endpoint_name,
                endpoint_type="s3",
                path=log_paths.rum_log_path(state.fos_prefix),
                period=state.log_period,
                response_condition="rum_log_condition",
                format_string=rum_format,
                placement=None,
                response_object_name="",
            )
        )

    return endpoints


def desired_backends(state: FeatureState) -> list[Backend]:
    """Return desired backends (session_scorer, fos_origin, rum_collector).

    Args:
        state: Feature state.

    Returns:
        List of backends to deploy.
    """
    backends: list[Backend] = []

    # Session Scoring backend (if scoring enabled)
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

    # RUM asset backend (if RUM enabled)
    if state.rum_enabled:
        from backend.core.fastly.utils import SHIELD_MAP

        shield_pop = state.cdn_shield
        if not shield_pop:
            shield_pop = SHIELD_MAP.get(state.fos_region, "iad-va-us")
        shield_val = shield_pop if shield_pop.lower() != "none" else ""

        # fos_origin backend for fetching rum-tracker.js
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
                shield=shield_val,
                request_condition="fastly_log_analytics_false",
            )
        )

    return backends


def desired_dictionaries(state: FeatureState) -> list[ServiceDictionary]:
    """Return desired Fastly service dictionaries (fos_credentials, etc).

    The fos_credentials dictionary is needed when:
    - RUM is enabled (for asset-fetch SigV4 signing), OR
    - Logging is enabled (for origin S3 backend credentials)

    Args:
        state: Feature state.

    Returns:
        List of dictionaries to deploy.
    """
    dicts: list[ServiceDictionary] = []

    if state.rum_enabled:
        items = {}
        if state.fos_access_key_id:
            items = {
                "access_key": state.fos_access_key_id,
                "secret_key": state.fos_secret_access_key,
                "bucket": state.fos_bucket,
                "region": state.fos_region,
            }
        dicts.append(
            ServiceDictionary(
                name="fos_credentials",
                write_only=True,
                items=items,
            )
        )

    return dicts
