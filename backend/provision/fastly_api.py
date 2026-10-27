import datetime
import json
import os
import re
import shutil
import sys
import urllib.parse
from typing import Any

from backend.core import log_fields as fc
from backend.core import log_fields as lf
from backend.core import vcl_generator as vcl_utils
from backend.core.fastly.client import (
    _FARO_VERSION_RE,
    _RUM_PATH_PREDICATE,
    FASTLY_LOG_FIELDS,
    RUM_ASSET_FETCH_NAME,
    RUM_DELIVER_SET_COOKIE_NAME,
    RUM_FARO_FETCH_NAME,
    RUM_RECV_NAME,
    RUM_SIGV4_SIGN_NAME,
    SHIELD_MAP,
    _assert_faro_version_safe,
    fastly,
    generate_rum_asset_fetch_vcl,
    generate_rum_recv_parts,
    generate_rum_vcl,
    get_active_version,
    get_generated_vcl,
    list_s3_endpoints,
    load_vcl,
    parse_period,
    region_endpoint,
    render_vcl_sigv4_signing_block,
    validate_rum_custom_condition,
)
from backend.provision.cli import BOLD, VclBuilder, _c, draft_version, fail, info, ok, warn

__all__ = [
    "FASTLY_LOG_FIELDS",
    "RUM_ASSET_FETCH_NAME",
    "RUM_DELIVER_SET_COOKIE_NAME",
    "RUM_FARO_FETCH_NAME",
    "RUM_RECV_NAME",
    "RUM_SIGV4_SIGN_NAME",
    "SHIELD_MAP",
    "_assert_faro_version_safe",
    "_FARO_VERSION_RE",
    "_RUM_PATH_PREDICATE",
    "generate_rum_asset_fetch_vcl",
    "generate_rum_recv_parts",
    "generate_rum_vcl",
    "load_vcl",
    "parse_period",
    "region_endpoint",
    "render_vcl_sigv4_signing_block",
    "validate_rum_custom_condition",
]

SESSION_COOKIE_ALLOWLIST: tuple[str, ...] = (
    "sessionid",
    "PHPSESSID",
    "JSESSIONID",
    "ASP.NET_SessionId",
    "connect.sid",
    "session",
    "sid",
)


def _session_cookie_hash_vcl() -> str:
    """Return the VCL expression that hashes the first-present session cookie."""
    sel = '""'
    for name in reversed(SESSION_COOKIE_ALLOWLIST):
        sf = f'subfield(req.http.Cookie, "{name}", ";")'
        sel = f'if({sf} != "", digest.hash_sha256({sf}), {sel})'
    return sel


EDGE_DATA_MAPPING = {
    "host": "req.http.Host",
    "ip": "client.ip",
    "country": "client.geo.country_code",
    "city": "client.geo.city",
    "region": "client.geo.region",
    "lat": 'if(client.geo.country_code != "?", "" + client.geo.latitude, "")',
    "lon": 'if(client.geo.country_code != "?", "" + client.geo.longitude, "")',
    "metro": 'if(client.geo.metro_code > 0, "" + client.geo.metro_code, "")',
    "asn": 'if(client.as.number > 0, "" + client.as.number, "")',
    "ja3": "tls.client.ja3_md5",
    "ja4": "tls.client.ja4",
    "tls": 'if(tls.client.protocol != "", regsub(tls.client.protocol, "^TLSv", ""), "null")',
    "rtt": 'if(client.socket.tcpi_rtt > 0, "" + client.socket.tcpi_rtt, "")',
    "ua": "req.http.User-Agent",
    "referer": "req.http.Referer",
    "transport": "transport.type",
    "ploss": 'if(client.socket.ploss > 0, "" + client.socket.ploss, "")',
    "rtt_min": 'if(client.socket.tcpi_min_rtt > 0, "" + client.socket.tcpi_min_rtt, "")',
    "rtt_var": 'if(client.socket.tcpi_rttvar > 0, "" + client.socket.tcpi_rttvar, "")',
    "retrans": 'if(client.socket.tcpi_delta_retrans > 0, "" + client.socket.tcpi_delta_retrans, "null")',
    "bw": 'if(transport.bw_estimate > 0, "" + transport.bw_estimate, "null")',
    "c_speed": fc.vcl_encode_chain("client.geo.conn_speed", fc.CONN_SPEED_ENCODE),
    "c_type": 'if(client.geo.conn_type == "?", "", client.geo.conn_type)',
    "p_type": fc.vcl_encode_chain("client.geo.proxy_type", fc.PROXY_TYPE_ENCODE),
    "p_desc": fc.vcl_encode_chain("client.geo.proxy_description", fc.PROXY_DESC_ENCODE),
    "q_rtt": 'if(transport.type == "quic", "" + quic.rtt.smoothed, "null")',
    "q_rtt_var": 'if(transport.type == "quic", "" + quic.rtt.variance, "null")',
    "q_lost": 'if(transport.type == "quic", "" + quic.num_packets.lost, "null")',
    "q_cwnd": 'if(transport.type == "quic", "" + quic.cc.cwnd, "null")',
    "srv_region": "server.region",
    "is_ipv6": 'if(req.is_ipv6, "1", "0")',
    "conn_reqs": 'if(client.requests > 0, "" + client.requests, "null")',
    "del_rate": 'if(client.socket.tcpi_delivery_rate > 0, "" + client.socket.tcpi_delivery_rate, "null")',
    "data_segs": 'if(client.socket.tcpi_data_segs_out > 0, "" + client.socket.tcpi_data_segs_out, "null")',
    "tls_csha": "tls.client.ciphers_list_sha",
    "cookie_session": _session_cookie_hash_vcl(),
}

_SCRUB_HEADERS = (
    "x-is-cluster-fetch",
    "x-fos-edge-data",
    "x-fos-origin-data",
    "x-of-start",
    "x-of-ttfb",
    "x-of-ttlb",
    "x-of-connect",
    "x-of-ost",
    "x-of-oip",
    "x-of-oretries",
    "x-of-status",
    "x-edge-req-id",
    "x-fos-io-ifsz",
    "x-fos-io-ofsz",
    "x-fos-io-ifmt",
    "x-fos-io-ofmt",
    "X-Edge-Scoring-Pass",
    "x-edge-score",
    "X-Edge-Score",
    "X-Edge-Score-Reason",
    "X-Edge-Score-Enforce",
    "X-Edge-Sid",
    "X-Edge-Score-Set-Cookie",
    "X-Edge-Prev-Anchor",
    "x-sigsci-skip-inspection-once",
)


def _resolve_enabled_custom_fields(log_fields_config: dict | None) -> list[dict]:
    return sorted(
        [cf for cf in (log_fields_config or {}).get("custom_fields", []) if cf.get("enabled", True)],
        key=lambda x: x["name"],
    )


def get_scrub_vcl_statements(log_fields_config: dict | None) -> list[str]:
    """Generate pure VCL statements to strip client-supplied internal headers."""
    lines = [f"unset req.http.{h};" for h in _SCRUB_HEADERS]
    for cf in _resolve_enabled_custom_fields(log_fields_config):
        name = cf["name"]
        for prefix in ("x-fos-edge-data", "x-fos-origin-data", "x-edge-score"):
            lines.append(f"unset req.http.{prefix}:{name};")
    return lines


def get_capture_vcl_statements(log_fields_config: dict | None) -> list[str]:
    """Generate pure VCL statements to capture standard and custom edge fields."""
    log_fields_config = log_fields_config or {}
    from backend.core.log_fields import get_required_edge_headers

    required = set(get_required_edge_headers(log_fields_config)) - {"ip"}
    group_l = "L" in (log_fields_config.get("groups") or [])
    limits = log_fields_config.get("field_limits") or {}
    custom_edge = [
        cf for cf in _resolve_enabled_custom_fields(log_fields_config) if cf.get("collection_stage", "edge") == "edge"
    ]

    lines = [
        "if (req.http.Fastly-Client-IP) {",
        "  set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;",
        "} else {",
        "  set req.http.x-fos-edge-data:ip = client.ip;",
        "}",
    ]
    for key in sorted(required):
        if key in EDGE_DATA_MAPPING:
            expr = EDGE_DATA_MAPPING[key]
            if key in ("ua", "referer"):
                expr = f"substr({expr}, 0, {limits.get(key, 1000)})"
            lines.append(f"set req.http.x-fos-edge-data:{key} = {expr};")

    for cf in custom_edge:
        lines.append(f"set req.http.x-fos-edge-data:{cf['name']} = {cf['vcl_log_expression']};")

    if group_l:
        lines.append("set req.http.x-req-id = randomstr(8);")

    return lines


def generate_capture_vcl(
    log_fields_config: dict | None,
    scoring_enabled: bool = False,
    rum_enabled: bool = False,
    scoring_exclude_url_regex: str | None = None,
    cmcd_enabled: bool = False,
    cmcd_mode: str = "query_string",
    cmcd_version: int = 1,
) -> dict[str, str]:
    """Return dict of VCL snippets keyed by subroutine name."""
    log_fields_config = log_fields_config or {}
    raw_required = lf.get_required_edge_headers(log_fields_config)
    group_l = "L" in (log_fields_config.get("groups") or [])
    group_m = "M" in (log_fields_config.get("groups") or [])

    enabled_custom = _resolve_enabled_custom_fields(log_fields_config)
    custom_origin = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "origin"]
    custom_deliver = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "deliver"]

    edge_detect = "fastly.ff.visits_this_service == 0"
    scrub_guard = f"req.restarts == 0 && {edge_detect}"

    scrub_lines = [
        "# [security] strip client-supplied internal-routing headers",
        f"if ({scrub_guard}) {{",
    ]
    for statement in get_scrub_vcl_statements(log_fields_config):
        scrub_lines.append(f"  {statement}")

    if rum_enabled:
        scrub_lines.append("  # Mint a per-request ID for RUM beacons on fresh requests")
        scrub_lines.append("  set req.http.x-rum-req-id = randomstr(12);")

    # CMCD extraction MUST precede capture statements so x-cmcd:<key> is populated before copy.
    if cmcd_enabled:
        from backend.provision.orchestrator import generate_cmcd_vcl

        cmcd_body = next(iter(generate_cmcd_vcl(mode=cmcd_mode, version=cmcd_version).values()))
        cmcd_lines = ["  # Section 3: CMCD Extraction (vcl_recv) — must precede field capture"]
        for line in cmcd_body.splitlines():
            if line.strip():
                cmcd_lines.append(f"  {line}")
            elif cmcd_lines and cmcd_lines[-1] != "":
                cmcd_lines.append("")
        if cmcd_lines and cmcd_lines[-1] == "":
            cmcd_lines.pop()
        scrub_lines.extend(cmcd_lines)

    if rum_enabled:
        rum_extraction, _ = generate_rum_recv_parts()
        scrub_lines.append("  # Section 5: RUM field extraction (vcl_recv)")
        scrub_lines.append('  if (req.url.path == "/rum-beacon") {')
        for line in rum_extraction.splitlines():
            scrub_lines.append(f"    {line}")
        scrub_lines.append("  }")

    for statement in get_capture_vcl_statements(log_fields_config):
        scrub_lines.append(f"  {statement}")

    if rum_enabled:
        _, rum_response = generate_rum_recv_parts()
        scrub_lines.append("  # Section 5: RUM (vcl_recv)")
        scrub_lines.append('  if (req.url.path == "/rum-beacon") {')
        for line in rum_response.splitlines():
            scrub_lines.append(f"    {line}")
        scrub_lines.append("  }")

    if scoring_enabled:
        from backend.provision.session_scoring_orchestrator import SCORING_BACKEND_VCL_NAME, resolve_exclude_url_regex

        effective_regex = resolve_exclude_url_regex(scoring_exclude_url_regex)
        scrub_lines.extend(
            [
                "  # Section 4: Session Scoring (vcl_recv)",
                "  # Session Scoring: route the first-pass dynamic request to the scorer.",
                "  # Edge-only — fastly.ff.visits_this_service == 0 is true only at the true",
                "  # edge; at a shield POP it is > 0, so the shield skips this block and the",
                "  # real-origin pass is served from the shield normally. Unforgeable, so a",
                "  # client cannot fake having already transited our edge.",
                "  #",
                "  # DDoS bypass (fastly.ddos_detected): when Fastly's L7 DDoS detection",
                "  # flags this request, do NOT route to Compute. Two reasons:",
                "  #   1. Cost ceiling — under attack, Compute invocations scale linearly",
                "  #      with attack volume. Skipping flagged requests caps the blast",
                "  #      radius while NGWAF / Fastly's mitigation handles the actual block.",
                "  #   2. Signal quality — the scorer's L2 transition matrix learns from",
                "  #      benign traffic shapes; feeding attack traffic in pollutes the",
                "  #      matrix even though those scores wouldn't be acted on.",
                "  # See: https://www.fastly.com/documentation/reference/vcl/variables/miscellaneous/fastly-ddos-detected/",
                f'  if (req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected && std.tolower(req.url) !~ "{effective_regex}") {{',
                f"    set req.backend = {SCORING_BACKEND_VCL_NAME};",
                "    # Skip NGWAF inspection on the scoring sub-fetch ONLY. The scorer is",
                "    # payload-agnostic — it strips the query string and scores the path — so",
                "    # inspecting this preflight hop is pure cost, and worse: NGWAF 406s attack",
                "    # URLs here, turning every blocked request into a scorer fail-open",
                "    # (compute-unavailable-406) even though the scorer never sees the payload.",
                "    # NGWAF's edge_security (vcl_miss/pass, priority 150) reads this on bereq",
                "    # AFTER our Session Scoring - Pass (priority 100), skips inspection, and",
                "    # self-unsets its bereq copy. The real-origin pass after the restart stays",
                "    # fully inspected — the persisted req.http copy is unset on the restart",
                "    # path below.",
                '    set req.http.x-sigsci-skip-inspection-once = "true";',
                '    set req.http.X-Edge-Scoring-Pass = "1";',
                "    # Stamp the round-trip start (µs since request receipt) so pass-1 deliver",
                "    # can compute edge-observed scorer latency (edge_score_rtt_us). Mirrors",
                "    # the x-of-start TTFB/TTLB idiom in fastly_api.generate_capture_vcl —",
                "    # that timer deliberately SKIPS the scoring sub-fetch, so this stamp is",
                "    # the only place the scorer leg is timed.",
                "    set req.http.x-edge-score-t0 = time.elapsed.usec;",
                "    # PASS — skip cache for the scoring sub-fetch. On the post-restart",
                "    # pass the scoring snippet doesn't re-fire because X-Edge-Scoring-Pass",
                "    # got unset in pass-1 deliver and req.restarts is now 1.",
                "    return(pass);",
                "  }",
            ]
        )

    scrub_lines.append("}")
    recv_vcl = VclBuilder.join_lines(scrub_lines)

    base_unset_lines = ["if (req.backend.is_origin) {", "  unset bereq.http.x-fos-edge-data;"]
    if scoring_enabled:
        base_unset_lines.extend(["  unset bereq.http.x-edge-score;", "  unset bereq.http.X-Edge-Scoring-Pass;"])
    base_unset_lines.append("}")
    base_unset = VclBuilder.join_lines(base_unset_lines) + "\n"

    def _group_l_timing(label: str) -> str:
        return (
            f"\n# [group-L] Record timing start for {label} fetch\n"
            'if (req.http.X-Edge-Scoring-Pass != "1") {\n'
            "set req.http.x-of-start = time.elapsed.usec;\n"
            "unset bereq.http.x-of-start;\n"
            'set bereq.http.x-is-cluster-fetch = "1";\n'
            "if (req.http.x-edge-req-id) {\n"
            "  set bereq.http.x-edge-req-id = req.http.x-edge-req-id;\n"
            "} else if (req.http.x-req-id) {\n"
            "  set bereq.http.x-edge-req-id = req.http.x-req-id;\n"
            "}\n"
            "unset bereq.http.x-req-id;\n}\n"
        )

    miss_vcl = base_unset + _group_l_timing("origin") if group_l else base_unset
    pass_vcl = base_unset + _group_l_timing("PASS") if group_l else base_unset
    snippets: dict[str, str] = {"recv": recv_vcl, "miss": miss_vcl, "pass": pass_vcl}

    if "ip" in raw_required:
        snippets["recv_reset"] = (
            "# [security] Drop a client-supplied Fastly-Client-IP at the true edge so a\n"
            "# spoofed value cannot poison the captured client IP. Operator VCL running\n"
            "# after this (priority above -100, before the priority-1 capture) may set\n"
            "# the real source IP; the ip capture falls back to client.ip otherwise.\n"
            f"if ({scrub_guard}) {{\n"
            "  unset req.http.Fastly-Client-IP;\n"
            "}"
        )

    if group_l or custom_origin:
        fetch_lines = []
        if group_l:
            fetch_lines.append(
                "# [group-L] Record TTFB and capture origin metadata\n"
                'if (req.http.X-Edge-Scoring-Pass != "1" && req.http.x-of-start != "") {\n'
                "  declare local var.fetch_ttfb INTEGER;\n"
                "  set var.fetch_ttfb = std.atoi(time.elapsed.usec);\n"
                "  set var.fetch_ttfb -= std.atoi(req.http.x-of-start);\n"
                "  set req.http.x-of-ttfb = var.fetch_ttfb;\n"
                "  set req.http.x-of-connect  = beresp.handshake_time_to_origin_ms;\n"
                "  set req.http.x-of-status   = beresp.status;\n"
                "  set req.http.x-of-oip      = beresp.backend.ip;\n"
                "  set req.http.x-of-oretries = req.restarts;\n"
                "}"
            )
        if custom_origin:
            fetch_lines.append("# --- Custom Origin Fields Start ---")
            fetch_lines.append("if (req.backend.is_origin) {")
            for cf in custom_origin:
                fetch_lines.append(f"  set beresp.http.x-fos-origin-data:{cf['name']} = {cf['vcl_log_expression']};")
            fetch_lines.append("}")
            fetch_lines.append("# --- Custom Origin Fields End ---")
        snippets["fetch"] = VclBuilder.join_lines(fetch_lines)

    if group_l:
        snippets["error"] = VclBuilder.join_lines(
            [
                "# [group-L] Capture timing for failed origin fetches\n"
                'if (req.http.X-Edge-Scoring-Pass != "1" && req.http.x-of-start != "") {\n'
                "  declare local var.error_ttfb INTEGER;\n"
                "  set var.error_ttfb = std.atoi(time.elapsed.usec);\n"
                "  set var.error_ttfb -= std.atoi(req.http.x-of-start);\n"
                "  set req.http.x-of-ttfb = var.error_ttfb;\n"
                "  set req.http.x-of-status   = obj.status;\n"
                "  set req.http.x-of-oip      = req.backend.ip;\n"
                "  set req.http.x-of-oretries = req.restarts;\n"
                "}"
            ]
        )

    if group_l or group_m or custom_origin or custom_deliver:
        deliver_lines = []
        if group_l:
            _l_hdrs = (
                ("x-of-ttfb", "resp.http.x-of-ttfb"),
                ("x-of-ttlb", "resp.http.x-of-ttlb"),
                ("x-of-connect", "resp.http.x-of-connect"),
                ("x-of-status", "resp.http.x-of-status"),
                ("x-of-oip", "resp.http.x-of-oip"),
                ("x-of-oretries", "req.restarts"),
            )
            _l_promote = "\n".join(
                f'if (req.http.{h} == "" && resp.http.{h} != "") {{ set req.http.{h} = {src}; }}' for h, src in _l_hdrs
            )
            _l_unset = "\n".join(f"  unset resp.http.{h};" for h, _ in _l_hdrs)
            _l_forward = "\n".join(
                f'  if (req.http.{h} != "") {{ set resp.http.{h} = req.http.{h}; }}' for h, _ in _l_hdrs
            )
            deliver_lines.append(
                "# [group-L] Record TTLB, capture bytes, strip all internal headers\n"
                'if (req.http.X-Edge-Scoring-Pass != "1" && req.http.x-of-start != "") {\n'
                "  declare local var.ttlb INTEGER;\n"
                "  set var.ttlb = std.atoi(time.elapsed.usec);\n"
                "  set var.ttlb -= std.atoi(req.http.x-of-start);\n"
                "  set req.http.x-of-ttlb = var.ttlb;\n"
                "}\n"
                "unset resp.http.x-of-start;\n"
                "\n"
                "# Promote upstream metrics to req so this node can log them if it didn't generate its own\n"
                f"{_l_promote}\n"
                "\n"
                'if (req.http.x-is-cluster-fetch != "1") {\n'
                "  # Returning to client: strip internal metrics from response\n"
                f"{_l_unset}\n"
                "} else {\n"
                "  # Returning to internal cluster node: ensure metrics are attached to response\n"
                f"{_l_forward}\n"
                "}\n"
                "unset resp.http.x-edge-req-id;"
            )

        if group_m:
            _m_stats = (("ifsz", "[0-9]+"), ("ofsz", "[0-9]+"), ("ifmt", "[a-zA-Z0-9]+"), ("ofmt", "[a-zA-Z0-9]+"))
            _m_hdrs = tuple(f"x-fos-io-{s}" for s, _ in _m_stats)
            _m_promote = "\n".join(
                f'if (req.http.{h} == "" && resp.http.{h} != "") {{ set req.http.{h} = resp.http.{h}; }}'
                for h in _m_hdrs
            )
            _m_extract = "\n".join(
                f'  if (resp.http.Fastly-Io-Transform-Stats ~ "{s}=") {{\n'
                f'    set req.http.x-fos-io-{s} = regsub(resp.http.Fastly-Io-Transform-Stats, ".*{s}=({pat}).*", "\\1");\n'
                "  }"
                for s, pat in _m_stats
            )
            _m_unset = "\n".join(f"  unset resp.http.{h};" for h in _m_hdrs)
            _m_forward = "\n".join(
                f'  if (req.http.{h} != "") {{ set resp.http.{h} = req.http.{h}; }}' for h in _m_hdrs
            )
            deliver_lines.append(
                "# [group-M] Extract IO metrics from Fastly-Io-Transform-Stats\n"
                f"{_m_promote}\n"
                'if (resp.http.Fastly-Io-Transform-Stats != "") {\n'
                f"{_m_extract}\n"
                "}\n"
                'if (req.http.x-is-cluster-fetch != "1") {\n'
                f"{_m_unset}\n"
                "} else {\n"
                f"{_m_forward}\n"
                "}"
            )

        if custom_origin:
            deliver_lines.append("# --- Custom Origin Fields Start ---")
            for cf in custom_origin:
                name = cf["name"]
                deliver_lines.append(f'if (resp.http.x-fos-origin-data:{name} != "") {{')
                if cf.get("origin_log_frequency", "all") == "miss_pass":
                    deliver_lines.append('  if (fastly_info.state !~ "HIT") {')
                    deliver_lines.append(
                        f"    set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"
                    )
                    deliver_lines.append("  }")
                else:
                    deliver_lines.append(
                        f"  set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"
                    )
                deliver_lines.append("}")
                deliver_lines.append('if (req.http.x-is-cluster-fetch != "1") {')
                deliver_lines.append(f"  unset resp.http.x-fos-origin-data:{name};")
                deliver_lines.append("}")
            deliver_lines.append("# --- Custom Origin Fields End ---")

        if custom_deliver:
            deliver_lines.append("# --- Custom Deliver Fields Start ---")
            for cf in custom_deliver:
                name = cf["name"]
                deliver_lines.append(f'if ({cf["vcl_log_expression"]} != "") {{')
                deliver_lines.append(f"  set req.http.x-fos-edge-data:{name} = {cf['vcl_log_expression']};")
                deliver_lines.append("}")
            deliver_lines.append("# --- Custom Deliver Fields End ---")

        snippets["deliver"] = VclBuilder.join_lines(deliver_lines)

    return snippets


def load_log_format(log_fields_config: dict = None) -> str:
    """Return the log format as a single-line string, defaulting to standard groups when unset."""
    cfg = dict(log_fields_config or {})
    if not cfg.get("groups") and not cfg.get("preset"):
        cfg["groups"] = lf.PRESETS["standard"]["groups"]
        cfg.setdefault("field_overrides", {})
    return lf.generate_log_format(cfg)


FASTLY_LOG_FORMAT_SAFE_MAX = int(os.environ.get("FASTLY_LOG_FORMAT_SAFE_MAX") or "12000")


def validate_log_format(log_fields_config: dict = None) -> list[str]:
    """Validate the generated log format and VCL snippets for syntax errors."""
    try:
        raw = load_log_format(log_fields_config)
    except Exception as e:
        return [f"Could not generate log format: {e}"]

    if len(raw) > FASTLY_LOG_FORMAT_SAFE_MAX:
        custom_fields = (log_fields_config or {}).get("custom_fields", [])
        custom_chars = sum(
            len(cf.get("vcl_log_expression", "")) + len(cf.get("name", "")) + 5
            for cf in custom_fields
            if cf.get("enabled", True)
        )
        builtin_chars = len(raw) - custom_chars
        return [
            f"LOG_FORMAT_TOO_LONG: Log format is {len(raw)} chars; "
            f"Fastly's limit is ~16384 (safe max: {FASTLY_LOG_FORMAT_SAFE_MAX}). "
            f"Remove fields or shorten VCL expressions. "
            f"(Built-in fields: ~{builtin_chars} chars, Custom fields: ~{custom_chars} chars)"
        ]

    if shutil.which("falco"):
        valid, message = vcl_utils.lint_log_format(raw, generate_capture_vcl(log_fields_config))
        return [] if valid else [message]

    return _validate_log_format_regex(raw)


def _validate_log_format_regex(raw: str) -> list[str]:
    """Regex-based fallback log format checks."""
    errors = []
    for expr in re.findall(r"%\{(.*?)\}V", raw, re.DOTALL):
        bare_cond = re.search(r"\bif\s*\(\s*[><=!]", expr)
        if bare_cond:
            snippet = expr.strip()[:80].replace("\n", " ")
            op = bare_cond.group().split("(", 1)[1].strip()
            errors.append(
                f"VCL syntax error — if() condition missing left-hand side "
                f"(if({op}...) — did you forget the variable name?) "
                f"in: %{{{snippet}}}V"
            )

        for m in re.finditer(r"\bif\s*\(", expr):
            depth, closed = 0, False
            for ch in expr[m.start() :]:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        closed = True
                        break
            if not closed:
                snippet = expr.strip()[:80].replace("\n", " ")
                errors.append(f"VCL syntax error — unclosed if() parenthesis in: %{{{snippet}}}V")
                break
    return errors


_RATELIMIT_OPT_IN_RE = re.compile(r"ratelimit_opt_in\s+true\b")
_FIX_UNSENT_BODY_DRAIN_RE = re.compile(r"fix_unsent_body_drain\s+true\b")
_NO_BODY_IF_BEREQ_GET_OR_HEAD_RE = re.compile(r"no_body_if_bereq_is_get_or_head\s+true\b")


def _fetch_active_generated_vcl(service_id: str, token: str) -> str | None:
    """Return the active version's compiled VCL for ``service_id``, or ``None``."""
    if not service_id:
        return None
    try:
        active_ver = get_active_version(service_id, token)
        if active_ver is None:
            return None
        return get_generated_vcl(service_id, active_ver, token) or None
    except Exception:  # noqa: BLE001
        return None


def account_has_rate_limiting(token: str, *service_ids: str) -> bool | None:
    """Proactively detect whether the Fastly account has edge rate limiting."""
    for service_id in service_ids:
        content = _fetch_active_generated_vcl(service_id, token)
        if content:
            return bool(_RATELIMIT_OPT_IN_RE.search(content))
    return None


def service_has_scoring_pragmas(service_id: str, token: str) -> bool | None:
    """Detect whether ``service_id`` has both preflight body preservation pragmas enabled."""
    if not service_id or not token:
        return None
    content = _fetch_active_generated_vcl(service_id, token)
    if not content:
        return None
    return bool(_FIX_UNSENT_BODY_DRAIN_RE.search(content) and _NO_BODY_IF_BEREQ_GET_OR_HEAD_RE.search(content))


def ensure_cdn_service(
    cfg: dict, fos_access_key: str, fos_secret_key: str, token: str, status_cb=None, on_created=None
) -> dict:
    """Create and activate the CDN VCL service fronting FOS via declarative reconciler."""
    from backend import config as _svcconfig
    from backend.provision.declarative.reconciler import reconcile_cdn_service_state

    logging_service_id = cfg.get("logging_service_id") or cfg.get("service_id")
    if not logging_service_id:
        raise ValueError("logging_service_id or service_id must be provided in config.")

    config_path = _svcconfig.config_path(logging_service_id)

    def _merge_inputs(disk_cfg: dict) -> None:
        disk_cfg.update(cfg)
        disk_cfg["fos_access_key_id"] = fos_access_key
        disk_cfg["fos_secret_access_key"] = fos_secret_key

    with _svcconfig.config_lock(logging_service_id):
        if _svcconfig.mutate_config(logging_service_id, _merge_inputs) is None:
            new_cfg: dict = {}
            _merge_inputs(new_cfg)
            _svcconfig.save_config(logging_service_id, new_cfg)

    def _read_cdn_sid() -> tuple[dict, str]:
        updated = json.loads(config_path.read_text())
        return updated, updated.get("fos_proxy", {}).get("service_id") or updated.get("cdn_service_id", "")

    try:
        reconcile_cdn_service_state(
            logging_service_id=logging_service_id,
            token=token,
            status_cb=status_cb,
            activate=True,
        )
    except Exception:
        if config_path.exists():
            try:
                _, cdn_sid = _read_cdn_sid()
                if on_created and cdn_sid:
                    on_created(cdn_sid)
            except Exception:
                pass
        raise

    updated_cfg, cdn_sid = _read_cdn_sid()
    if on_created and cdn_sid:
        on_created(cdn_sid)

    return {
        "id": cdn_sid,
        "name": updated_cfg.get("cdn_service_name") or f"Log Analysis CDN Service for {logging_service_id}",
        "rate_limiting": account_has_rate_limiting(token, logging_service_id),
    }


def redeploy_cdn_vcl(cdn_service_id: str, token: str, rate_limiting: bool = True, status_cb=None):
    """Clone active version and redeploy the main VCL snippet via declarative reconciler."""
    from backend import config as _svcconfig
    from backend.config import list_configs
    from backend.provision.declarative.reconciler import reconcile_cdn_service_state

    logging_service_id = next(
        (
            c["service_id"]
            for c in list_configs()
            if c.get("cdn_service_id") == cdn_service_id
            or (c.get("fos_proxy") or {}).get("service_id") == cdn_service_id
        ),
        None,
    )
    if not logging_service_id:
        raise ValueError(f"Could not map cdn_service_id '{cdn_service_id}' to a configured logging_service_id.")

    if (
        _svcconfig.mutate_config(
            logging_service_id,
            lambda cfg: cfg.setdefault("fos_proxy", {}).__setitem__("rate_limiting_enabled", rate_limiting),
        )
        is None
    ):
        raise FileNotFoundError(_svcconfig.config_path(logging_service_id))

    result = reconcile_cdn_service_state(
        logging_service_id=logging_service_id, token=token, status_cb=status_cb, activate=True
    )
    return result.activated_version


def delete_cdn_service(service_id: str, name: str, token: str, status_cb=None):
    """Delete the analytics-owned log-fronting CDN VCL service."""
    label = f"{name} ({service_id})"
    info(f"Deleting analytics-owned log-fronting CDN service {_c(BOLD, label)}…")
    if status_cb:
        status_cb(f"⏳ Deleting analytics-owned CDN service {label} (not your origin service)...")
    try:
        for v in fastly("GET", f"/service/{service_id}/version", token=token):
            if v.get("active"):
                if status_cb:
                    status_cb(f"⏳ Deactivating version {v['number']} of {service_id}...")
                fastly("PUT", f"/service/{service_id}/version/{v['number']}/deactivate", token=token)
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("CDN service already deleted")
            return

    try:
        fastly("DELETE", f"/service/{service_id}", token=token, expect_empty=True)
        ok(f"Analytics-owned CDN service {label} deleted")
        if status_cb:
            status_cb(f"✅ Analytics-owned CDN service {service_id} deleted.")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok(f"Analytics-owned CDN service {service_id} already deleted")
        else:
            raise exc


def _log_sampling_edge_clause(scoring_enabled: bool) -> str:
    """Edge-hop predicate for the 'Log Sampling' response_condition."""
    return "fastly.ff.visits_this_service == 0"


_CAPTURE_SNIPPET_PREFIX = "Fastly Log Analytics"
_RUM_SNIPPET_NAMES = frozenset(
    {"RUM - Recv", "RUM - Set cookies", "RUM - Asset fetch FOS", "RUM - Asset fetch SigV4 signing"}
)


def remove_logging_endpoint(
    service_id: str,
    endpoint_name: str,
    token: str,
    status_cb=None,
    *,
    remove_logging: bool = True,
    remove_rum: bool = True,
):
    """Safely remove the logging endpoint + all analytics-owned VCL from the active version."""
    from backend.provision.declarative.reconciler import MANAGED_BACKEND_NAMES, MANAGED_DICTIONARY_NAMES
    from backend.provision.orchestrator import cmcd_snippet_names
    from backend.provision.session_scoring_orchestrator import scoring_snippet_names

    info(f"Checking active version of service {_c(BOLD, service_id)}…")
    if status_cb:
        status_cb(f"🔍 Checking active version of service {service_id}...")
    active_ver = get_active_version(service_id, token)
    if active_ver is None:
        warn(f"Service {service_id} has no active version — skipping.")
        return
    ok(f"Active version: {active_ver}")

    existing = list_s3_endpoints(service_id, active_ver, token)
    eps_list = []
    if remove_logging:
        eps_list.extend(["Fastly Log Analytics", "Fastly Object Storage Logs", endpoint_name])
    if remove_rum:
        eps_list.extend(["Fastly RUM Logs", "Fastly RUM Object Storage Logs"])

    endpoints_to_delete = [ep for ep in dict.fromkeys(eps_list) if ep in existing]
    if not endpoints_to_delete:
        ok(f"No matching logging endpoints found on version {active_ver} — checking snippets anyway")

    info(f"Cloning version {active_ver} → new draft…")
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _log_failure(exc: Exception) -> None:
        fail(str(exc))
        info(f"Rolling back — re-activating version {active_ver}...")

    with draft_version(
        service_id,
        token,
        base_version=active_ver,
        call=fastly,
        comment=f"Deactivating Fastly Object Storage logging at {ts}",
        comment_before_activate=True,
        status_cb=info,
        validate_msg="Validating draft version…",
        activate_msg="Activating version {ver}…",
        on_failure=_log_failure,
    ) as new_ver:
        ok(f"Draft version: {new_ver}")

        def _delete_draft_resource(res_path: str, name: str) -> bool:
            encoded = urllib.parse.quote(name, safe="")
            try:
                fastly(
                    "DELETE",
                    f"/service/{service_id}/version/{new_ver}/{res_path}/{encoded}",
                    token=token,
                    expect_empty=True,
                )
                return True
            except RuntimeError as exc:
                if "404" not in str(exc):
                    raise exc
                return False

        for ep in endpoints_to_delete:
            info(f"Removing '{_c(BOLD, ep)}' from draft…")
            if _delete_draft_resource("logging/s3", ep):
                ok(f"Logging endpoint '{ep}' removed from draft")

        info("Removing VCL snippets…")
        owned_exact: set[str] = set()
        if remove_logging:
            owned_exact.update(scoring_snippet_names())
            owned_exact.update(cmcd_snippet_names())
        if remove_rum:
            owned_exact.update(_RUM_SNIPPET_NAMES)

        removed_names: list[str] = []
        try:
            present = fastly("GET", f"/service/{service_id}/version/{new_ver}/snippet", token=token)
        except Exception as e:
            warn(f"Could not list snippets on draft {new_ver}: {e}")
            present = []

        for snip in present:
            s_name = snip.get("name") or ""
            if not ((remove_logging and s_name.startswith(_CAPTURE_SNIPPET_PREFIX)) or s_name in owned_exact):
                continue
            info(f"Removing snippet '{_c(BOLD, s_name)}' from draft…")
            if _delete_draft_resource("snippet", s_name):
                removed_names.append(s_name)

        ok(f"VCL snippets removed from draft ({len(removed_names)}): {', '.join(removed_names) or 'none matched'}")
        leftovers = [
            s.get("name")
            for s in present
            if (s.get("name") or "").startswith(_CAPTURE_SNIPPET_PREFIX) and s.get("name") not in removed_names
        ]
        if leftovers:
            warn(f"Capture snippets still on draft {new_ver} after removal: {leftovers}")
        elif not removed_names and present:
            warn(
                f"No analytics-owned snippets matched on draft {new_ver}. "
                f"Snippets present: {[s.get('name') for s in present]} — "
                "if any of those are ours, teardown left VCL behind."
            )

        managed_cond_names = {"Log Sampling", "log_analytics_condition", "rum_log_condition"}
        for res_path, plural, label, allowed_names in (
            ("backend", "backends", "backend", MANAGED_BACKEND_NAMES),
            ("dictionary", "dictionaries", "dictionary", MANAGED_DICTIONARY_NAMES),
            ("condition", "conditions", "condition", managed_cond_names),
        ):
            try:
                for item in fastly("GET", f"/service/{service_id}/version/{new_ver}/{res_path}", token=token):
                    item_name = item.get("name")
                    if item_name in allowed_names:
                        info(f"Removing managed {label} '{item_name}' from draft…")
                        encoded_name = urllib.parse.quote(item_name, safe="")
                        fastly(
                            "DELETE",
                            f"/service/{service_id}/version/{new_ver}/{res_path}/{encoded_name}",
                            token=token,
                            expect_empty=True,
                        )
                        ok(f"Managed {label} '{item_name}' removed from draft")
            except Exception as e:
                warn(f"Failed to check/remove {plural}: {e}")

    ok(f"Version {new_ver} is now active")


def update_logging_endpoint(cfg: dict, token: str):
    """Safely update the log configuration of an existing FOS endpoint using the declarative reconciler."""
    service_id = cfg["logging_service_id"]
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    sample_rate, edge_only, period, path = (
        cfg.get("sample_rate"),
        cfg.get("edge_only"),
        cfg.get("log_period"),
        cfg.get("fos_path"),
    )
    total_steps = 5
    yield {"type": "progress", "current": 0, "total": total_steps}

    from backend import config as _svcconfig
    from backend.provision.orchestrator import reconcile_cfg_system_custom_fields

    try:
        service_cfg = _svcconfig.load_config(service_id)
    except Exception as e:
        raise RuntimeError(f"Service config for {service_id} not found: {e}")
    if service_cfg is None:
        raise RuntimeError(f"Service config for {service_id} is empty or None")

    cmcd_enabled_req, cmcd_mode_req, cmcd_version_req = (
        cfg.get("cmcd_enabled"),
        cfg.get("cmcd_mode"),
        cfg.get("cmcd_version"),
    )

    def _apply_requested_changes(target: dict) -> None:
        old_cmcd = target.get("cmcd") or {}
        cmcd_was_enabled = bool(old_cmcd.get("enabled"))

        if cmcd_enabled_req is not None:
            from backend.provision.orchestrator import _remove_cmcd_custom_fields, reconcile_cmcd_custom_fields

            want_enabled = bool(cmcd_enabled_req)
            new_mode = cmcd_mode_req or old_cmcd.get("mode", "query_string")
            new_version = int(cmcd_version_req) if cmcd_version_req is not None else old_cmcd.get("version", 1)

            if want_enabled and not cmcd_was_enabled:
                target["cmcd"] = {
                    "enabled": True,
                    "mode": new_mode,
                    "version": new_version,
                    "enabled_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
                }
                lf_cfg = target.setdefault("log_fields", {})
                lf_cfg["custom_fields"] = reconcile_cmcd_custom_fields(lf_cfg.get("custom_fields"), enabled=True)
            elif not want_enabled and cmcd_was_enabled:
                target.pop("cmcd", None)
                _remove_cmcd_custom_fields(target)
            elif want_enabled and cmcd_was_enabled:
                if new_mode != old_cmcd.get("mode") or new_version != old_cmcd.get("version"):
                    target["cmcd"] = {**old_cmcd, "mode": new_mode, "version": new_version}

        prov = target.setdefault("provisioning", {})
        if sample_rate is not None:
            prov["sample_rate"] = int(sample_rate)
        if edge_only is not None:
            prov["edge_only"] = bool(edge_only)
        if period is not None:
            target["log_period"] = int(period)
        if endpoint_name is not None:
            prov["endpoint_name"] = endpoint_name
        if cfg.get("log_fields") is not None:
            incoming_lf = dict(cfg["log_fields"])
            if not incoming_lf.get("custom_fields"):
                existing_custom = (target.get("log_fields") or {}).get("custom_fields")
                if existing_custom:
                    incoming_lf["custom_fields"] = list(existing_custom)
            target["log_fields"] = incoming_lf

        reconcile_cfg_system_custom_fields(target)
        if path is not None:
            target["fos_path"] = path

    if _svcconfig.mutate_config(service_id, _apply_requested_changes) is None:
        raise RuntimeError(f"Service config for {service_id} is empty or None")

    try:
        detected_rl = account_has_rate_limiting(token, service_id)
        current_rl = ((_svcconfig.load_config(service_id) or {}).get("provisioning") or {}).get("rate_limiting")
        if detected_rl is not None and current_rl != detected_rl:
            _svcconfig.mutate_config(
                service_id,
                lambda target: target.setdefault("provisioning", {}).__setitem__("rate_limiting", detected_rl),
            )
            msg = (
                "ℹ️ Edge rate limiting is now available on this account — redeploy the CDN service to enable it."
                if detected_rl
                else "ℹ️ Edge rate limiting is not available on this account."
            )
            yield {"type": "status", "message": msg}
    except Exception:
        pass

    yield {"type": "progress", "current": 1, "total": total_steps}

    from backend.provision.cli import run_with_events
    from backend.provision.declarative.reconciler import reconcile_vcl_state

    yield {"type": "status", "message": f"🔍 Starting declarative reconciliation for service {service_id}..."}
    try:
        result = yield from run_with_events(reconcile_vcl_state, service_id, token, dry_run=False)
        if result.error:
            raise RuntimeError(result.error)

        yield {"type": "progress", "current": 5, "total": total_steps}
        yield {
            "type": "done",
            "message": f"Service updated successfully to version {result.activated_version}.",
            "version": result.activated_version,
            "changed": bool(result.changes_applied),
        }
    except Exception as err:
        raise RuntimeError(f"Declarative reconciliation failed: {err}")


# ── CDN Snippets ──────────────────────────────────────────────────────────────

_CDN_RECV_SWR_SNIPPET_NAME = "cdn-swr-shield-disable"
_CDN_RECV_SWR_SNIPPET = """\
  if (fastly.ff.visits_this_service > 1) {
    set req.max_stale_while_revalidate = 0s;
  }"""

_CDN_FETCH_GENERATION_SNIPPET_NAME = "cdn-race-condition-generation"
_CDN_FETCH_GENERATION_SNIPPET = """\
  if (req.backend.is_origin) {
    set beresp.http.X-Shield-Generation = req.vcl.generation;
  } else {
    declare local var.local INTEGER;
    set var.local = std.atoi(req.vcl.generation);
    if (std.atoi(beresp.http.X-Shield-Generation) < var.local) {
      set beresp.ttl = 1s;
    }
    set beresp.http.X-Edge-Generation = req.vcl.generation;
  }"""

_CDN_NO_CACHE_404_SNIPPET_NAME = "cdn-no-cache-404"
_CDN_NO_CACHE_404_SNIPPET = """\
  if ((req.method == "GET" || req.method == "HEAD") && beresp.status == 404) {
    set beresp.cacheable = false;
    set beresp.ttl = 0s;
    set beresp.stale_while_revalidate = 0s;
    set beresp.stale_if_error = 0s;
  }"""

_CDN_STREAMING_BYPASS_SNIPPET_NAME = "cdn-streaming-bypass"
_CDN_STREAMING_BYPASS_SNIPPET = """\
  # [streaming] Bypass compression and enable streaming for Server-Sent Events (SSE)
  if (beresp.http.Content-Type ~ "^text/event-stream" || req.url ~ "stream") {
    set beresp.do_stream = true;
    set beresp.gzip = false;
    set beresp.brotli = false;
    set beresp.ttl = 0s;
    set beresp.cacheable = false;
  }"""

_CDN_SNIPPETS = [
    (_CDN_STREAMING_BYPASS_SNIPPET_NAME, "fetch", _CDN_STREAMING_BYPASS_SNIPPET, 5),
    (_CDN_RECV_SWR_SNIPPET_NAME, "recv", _CDN_RECV_SWR_SNIPPET, 10),
    (_CDN_FETCH_GENERATION_SNIPPET_NAME, "fetch", _CDN_FETCH_GENERATION_SNIPPET, 20),
    (_CDN_NO_CACHE_404_SNIPPET_NAME, "fetch", _CDN_NO_CACHE_404_SNIPPET, 30),
]


# ── Remote Frontend Sharing Domain (merged from sharing_domain.py) ────────────


def _fastly_api_request(method: str, path: str, *, token: str, json_data: Any = None, step: str = "") -> Any:
    """Perform a request to the Fastly API via the shared client."""
    try:
        return fastly(method, path, body=json_data, token=token)
    except RuntimeError as exc:
        raise RuntimeError(f"Fastly API error during {step}: {exc}") from exc


def deploy_remote_frontend(
    service_name: str,
    domain_name: str,
    origin_host: str,
    origin_port: int,
    use_ssl: bool,
    token: str,
    override_host: str | None = None,
) -> dict[str, Any]:
    """Deploy a remote frontend service to Fastly."""
    service_data = _fastly_api_request(
        "POST", "/service", token=token, json_data={"name": service_name, "type": "vcl"}, step="Create Service"
    )
    service_id = service_data.get("id")
    if not service_id:
        raise RuntimeError("Failed to retrieve service ID from response during Create Service")

    version_data = _fastly_api_request(
        "POST", f"/service/{service_id}/version", token=token, step="Verify Draft Version"
    )
    version = version_data.get("number")
    if version is None:
        raise RuntimeError("Failed to retrieve version number from response during Verify Draft Version")

    _fastly_api_request(
        "POST",
        f"/service/{service_id}/version/{version}/domain",
        token=token,
        json_data={"name": domain_name},
        step="Attach Domain",
    )

    backend_payload: dict[str, Any] = {
        "name": "gce_vm_origin",
        "address": origin_host,
        "port": origin_port,
        "use_ssl": use_ssl,
        "ssl_check_cert": False,
    }
    if override_host:
        backend_payload["override_host"] = override_host
    if use_ssl:
        sni_host = override_host or origin_host
        backend_payload["ssl_sni_hostname"] = sni_host
        backend_payload["ssl_cert_hostname"] = sni_host
    _fastly_api_request(
        "POST",
        f"/service/{service_id}/version/{version}/backend",
        token=token,
        json_data=backend_payload,
        step="Attach Backend Origin",
    )
    _fastly_api_request(
        "PUT", f"/service/{service_id}/version/{version}/activate", token=token, step="Activate Version"
    )

    return {
        "service_id": service_id,
        "version": version,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }


def delete_remote_frontend(remote_service_id: str, token: str) -> None:
    """Deactivate and delete the remote frontend service on Fastly."""
    try:
        for v in _fastly_api_request("GET", f"/service/{remote_service_id}/version", token=token, step="Get Versions"):
            if v.get("active"):
                ver_num = v["number"]
                _fastly_api_request(
                    "PUT",
                    f"/service/{remote_service_id}/version/{ver_num}/deactivate",
                    token=token,
                    step=f"Deactivate Version {ver_num}",
                )
    except Exception as exc:
        if "404" not in str(exc):
            raise exc

    try:
        _fastly_api_request("DELETE", f"/service/{remote_service_id}", token=token, step="Delete Service")
    except Exception as exc:
        if "404" not in str(exc):
            raise exc


sys.modules.setdefault("backend.provision.sharing_domain", sys.modules[__name__])
