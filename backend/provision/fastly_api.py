import datetime
import re
import shutil
import sys
import urllib.parse
from typing import Any

from backend.core import field_registry as fc
from backend.core import field_registry as lf
from backend.core.fastly.client import (
    fastly,
    get_active_version,
    get_generated_vcl,
    list_s3_endpoints,
)
from backend.core.fastly.rum_provisioning import generate_rum_recv_parts
from backend.provision.utils import BOLD, VclBuilder, _c, draft_version, fail, info, ok, warn
from backend.utils import vcl_utils

# ── VCL Edge Data Mapping ───────────────────────────────────────────────────

# Ordered allowlist of common framework session-cookie names. The edge captures
# a SHA-256 hash of the FIRST cookie in this list that is actually present on
# the request (privacy: the raw cookie value never leaves Fastly). There is no
# single cross-framework session cookie name, so a curated allowlist is the most
# defensible generic approach. ``subfield()`` does EXACT key matching (and
# handles dotted names like ``connect.sid``), so it is safe against prefix
# false-matches that a naive ``Cookie ~ "sid=(...)"`` regex would hit. Ordered
# by framework popularity — first present wins.
SESSION_COOKIE_ALLOWLIST: tuple[str, ...] = (
    "sessionid",  # Django
    "PHPSESSID",  # PHP
    "JSESSIONID",  # Java / Spring / Tomcat
    "ASP.NET_SessionId",  # ASP.NET
    "connect.sid",  # Express / Node
    "session",  # Flask (default), generic
    "sid",  # generic
)


def _session_cookie_hash_vcl() -> str:
    """Return the VCL expression that hashes the first-present session cookie.

    Builds a right-folded nested ``if()`` over ``SESSION_COOKIE_ALLOWLIST``:
    each branch tests one cookie and, when present, hashes THAT cookie in the
    ``if()``'s VALUE (true) position — ``if(<cookie> != "", hash(<cookie>),
    <next branch>)`` — falling through to ``""`` when none are present.

    The hash MUST live in the value branch, not wrapped around the whole
    selection. Fastly's VCL compiler rejects an ``if()`` function nested inside
    another ``if()``'s CONDITION (``if`` is a reserved keyword there — real
    ``/validate`` errors with "Syntax error in condition"; falco / ``make
    vcl-test`` are more lenient and do NOT catch it). The old
    ``if(<nested-if select chain> != "", hash(...), "")`` form did exactly that.
    Same limitation is documented in ``backend/core/log_fields.py`` for the
    deliver-stage numeric flattening.

    An empty cookie is never hashed (a per-branch ``!= ""`` guard precedes each
    hash), so a cookie-less request stays ``""`` rather than collapsing to the
    constant SHA-256 of "" and poisoning the harvesting insight. The hash is
    computed at the true edge (the ``x-fos-edge-data`` capture is edge-gated),
    so the raw cookie is never logged and never forwarded past Fastly.
    """
    sel = '""'
    for name in reversed(SESSION_COOKIE_ALLOWLIST):
        sf = f'subfield(req.http.Cookie, "{name}", ";")'
        sel = f'if({sf} != "", digest.hash_sha256({sf}), {sel})'
    return sel


# Maps x-fos-edge-data subfield keys to the VCL expressions used to capture them.
# Only headers present in this mapping will be captured at the edge.
EDGE_DATA_MAPPING = {
    "host": "req.http.Host",
    # Prefer an operator-supplied Fastly-Client-IP over the raw socket IP.
    # When the service sits behind another proxy/CDN, operator VCL higher in
    # vcl_recv may rewrite req.http.Fastly-Client-IP to the true source IP
    # (and set client.geo.ip_override accordingly); in that case client.ip is
    # the intermediary, not the real client. Fall back to the socket IP when
    # the header is unset/empty. Both if() branches must be STRING, so the
    # fallback uses "" + client.ip to stringify the IP type.
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
    # Group C — Infrastructure (captured at edge for shield-safe accuracy)
    "srv_region": "server.region",
    "is_ipv6": 'if(req.is_ipv6, "1", "0")',
    "conn_reqs": 'if(client.requests > 0, "" + client.requests, "null")',
    # Group G — Network Quality Deep (socket variables only valid at true edge PoP)
    "del_rate": 'if(client.socket.tcpi_delivery_rate > 0, "" + client.socket.tcpi_delivery_rate, "null")',
    "data_segs": 'if(client.socket.tcpi_data_segs_out > 0, "" + client.socket.tcpi_data_segs_out, "null")',
    # Group H — Security: TLS Fingerprinting (TLS state only valid at true edge PoP)
    "tls_csha": "tls.client.ciphers_list_sha",
    # Group H — Security: hashed session-cookie id (PRIVACY: hashed at edge; the
    # raw cookie value is never logged or forwarded). See SESSION_COOKIE_ALLOWLIST.
    "cookie_session": _session_cookie_hash_vcl(),
}


def get_scrub_vcl_statements(log_fields_config: dict | None) -> list[str]:
    """Generate pure VCL statements to strip client-supplied internal headers."""
    log_fields_config = log_fields_config or {}
    enabled_custom = sorted(
        [cf for cf in log_fields_config.get("custom_fields", []) if cf.get("enabled", True)],
        key=lambda x: x["name"],
    )
    lines = [
        "unset req.http.x-is-cluster-fetch;",
        "unset req.http.x-fos-edge-data;",
        "unset req.http.x-fos-origin-data;",
        "unset req.http.x-of-start;",
        "unset req.http.x-of-ttfb;",
        "unset req.http.x-of-ttlb;",
        "unset req.http.x-of-connect;",
        "unset req.http.x-of-ost;",
        "unset req.http.x-of-oip;",
        "unset req.http.x-of-oretries;",
        "unset req.http.x-of-status;",
        "unset req.http.x-edge-req-id;",
        "unset req.http.x-fos-io-ifsz;",
        "unset req.http.x-fos-io-ofsz;",
        "unset req.http.x-fos-io-ifmt;",
        "unset req.http.x-fos-io-ofmt;",
        "unset req.http.X-Edge-Scoring-Pass;",
        "unset req.http.x-edge-score;",
        "unset req.http.X-Edge-Score;",
        "unset req.http.X-Edge-Score-Reason;",
        "unset req.http.X-Edge-Score-Enforce;",
        "unset req.http.X-Edge-Sid;",
        "unset req.http.X-Edge-Score-Set-Cookie;",
        "unset req.http.X-Edge-Prev-Anchor;",
        "unset req.http.x-sigsci-skip-inspection-once;",
    ]
    if enabled_custom:
        for cf in enabled_custom:
            name = cf["name"]
            lines.append(f"unset req.http.x-fos-edge-data:{name};")
            lines.append(f"unset req.http.x-fos-origin-data:{name};")
            lines.append(f"unset req.http.x-edge-score:{name};")
    return lines


def get_capture_vcl_statements(log_fields_config: dict | None) -> list[str]:
    """Generate pure VCL statements to capture standard and custom edge fields."""
    log_fields_config = log_fields_config or {}
    from backend.core.log_fields import get_required_edge_headers

    raw_required = get_required_edge_headers(log_fields_config)
    required = set(raw_required)
    required.discard("ip")
    group_l = "L" in (log_fields_config.get("groups") or [])
    limits = log_fields_config.get("field_limits") or {}
    enabled_custom = sorted(
        [cf for cf in log_fields_config.get("custom_fields", []) if cf.get("enabled", True)],
        key=lambda x: x["name"],
    )
    custom_edge = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "edge"]

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
            if key == "ua":
                limit = limits.get("ua", 1000)
                expr = f"substr({expr}, 0, {limit})"
            elif key == "referer":
                limit = limits.get("referer", 1000)
                expr = f"substr({expr}, 0, {limit})"
            lines.append(f"set req.http.x-fos-edge-data:{key} = {expr};")

    if custom_edge:
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
    """Return dict of VCL snippets keyed by subroutine name.

    Always returns "recv", "miss", and "pass". When group L (Origin Metrics)
    is enabled, also returns "fetch", "error", and "deliver".

    ``log_fields_config`` accepts ``None`` because most callers pass the
    raw ``cfg.get("log_fields")`` value; coerced to ``{}`` at the top so
    downstream calls don't have to repeat the None-check.
    """
    log_fields_config = log_fields_config or {}
    raw_required = lf.get_required_edge_headers(log_fields_config)
    required = set(raw_required)
    required.discard("ip")
    group_l = "L" in (log_fields_config.get("groups") or [])
    group_m = "M" in (log_fields_config.get("groups") or [])
    limits = log_fields_config.get("field_limits") or {}

    enabled_custom = sorted(
        [cf for cf in (log_fields_config or {}).get("custom_fields", []) if cf.get("enabled", True)],
        key=lambda x: x["name"],
    )

    custom_edge = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "edge"]
    custom_origin = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "origin"]
    # "deliver" stage: capture from response headers in vcl_deliver and
    # promote into req.http.x-fos-edge-data:* so the same log-format
    # consumer that handles edge-stage fields picks them up. Used by the
    # session-scoring integration to capture X-Edge-Score* response headers
    # from the scorer Compute backend.
    custom_deliver = [cf for cf in enabled_custom if cf.get("collection_stage", "edge") == "deliver"]

    # Security: scrub internal-routing headers a client could spoof.
    # The cluster-fetch / edge-data headers are set by THIS service's own
    # snippets on the origin-bound bereq (vcl_miss / vcl_pass) and must
    # never appear on an inbound req. Without this scrub, a client header
    # like ``x-is-cluster-fetch: 1`` makes the conditional in vcl_deliver
    # incorrectly classify the response as internal-cluster traffic and
    # SKIP the "strip internal headers" cleanup — leaking origin-side
    # metric headers (x-of-oip = origin backend IP, x-of-ttfb, etc.) to
    # the client. Run BEFORE the edge-capture conditional so even
    # configurations without any group-L / custom fields get the scrub.
    # 020: Build scrub as a list so we can append per-custom-field
    # unsets. ``unset req.http.x-fos-edge-data;`` strips the bare
    # header but does NOT strip arbitrary subfield variants
    # (``req.http.x-fos-edge-data:my_field``) on Fastly VCL — those
    # are independent header slots once the colon-subfield syntax is
    # in play. A client that knows a custom-field name (and they often
    # leak through CSP, error pages, or just by being mentioned in
    # public docs) can pre-set ``x-fos-edge-data:<field>`` and have
    # the log line read the spoofed value instead of the edge-captured
    # one. Per-name scrubs close the gap.
    # Edge-hop detection (restart-invariant). ``fastly.ff.visits_this_service``
    # is 0 only at the first Fastly POP to handle the request (the true edge);
    # a client cannot forge it (each Fastly-FF entry is a salted hash only
    # genuine Fastly hops can produce), so it can't fake having already transited
    # our edge. Stays correct even when the nearest POP is a shield — that POP is
    # simply the first hop, so visits_this_service is still 0 there. This mirrors
    # the CDN fronting service (core/fastly/utils.py) and the scoring VCL.
    edge_detect = "fastly.ff.visits_this_service == 0"
    # SCRUB runs only on the FIRST edge pass: it strips client-forged headers
    # before anything trusts them. It must stay req.restarts == 0 so it does not
    # re-run on the post-scoring-restart pass (where it would risk wiping the
    # score headers the scoring deliver snippet stashed).
    scrub_guard = f"req.restarts == 0 && {edge_detect}"
    # CAPTURE must run on the FINAL edge pass. Session scoring restarts the
    # request (req.restarts 0→1); gating capture on req.restarts == 0 leaves
    # scored requests with no url/ua/geo and edge != true, and EVERY scoring view
    # filters WHERE edge = true → Fire Rate 0%. When scoring is enabled, drop the
    # restarts clause so the post-restart edge pass captures; edge_detect alone
    # still scopes it to the edge hop, and the first-pass scrub already removed
    # any client-forged values so capturing on the later pass is safe.
    capture_guard = edge_detect

    scrub_lines = [
        "# [security] strip client-supplied internal-routing headers",
        f"if ({scrub_guard}) {{",
    ]
    for statement in get_scrub_vcl_statements(log_fields_config):
        scrub_lines.append(f"  {statement}")

    if rum_enabled:
        scrub_lines.append("  # Mint a per-request ID for RUM beacons on fresh requests")
        scrub_lines.append("  set req.http.x-rum-req-id = randomstr(12);")

    # CMCD extraction MUST be emitted BEFORE the capture statements below.
    # Capture copies ``req.http.x-cmcd:<key>`` into
    # ``req.http.x-fos-edge-data:cmcd_<key>`` (that promoted header is what the
    # log format reads). Extracting afterwards copies EMPTY strings, so every
    # cmcd_* column logs empty while the extraction itself works perfectly —
    # provable at the edge, because it still strips ?CMCD= from the cache key.
    # That silent ordering inversion is the 2026-08 CMCD-collection outage; it
    # survived a month because nothing errors and the columns exist.
    if cmcd_enabled:
        from backend.provision.cmcd_fields import generate_cmcd_vcl

        cmcd_vcl_dict = generate_cmcd_vcl(mode=cmcd_mode, version=cmcd_version)
        cmcd_body = next(iter(cmcd_vcl_dict.values()))
        cmcd_lines = [
            "  # Section 3: CMCD Extraction (vcl_recv) — must precede field capture",
        ]
        for line in cmcd_body.splitlines():
            if line.strip():
                cmcd_lines.append(f"  {line}")
            else:
                # Avoid consecutive empty lines to keep it clean, but keep a single empty line if there's code above
                if cmcd_lines and cmcd_lines[-1] != "":
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
        from backend.provision.session_scoring_vcl import SCORING_BACKEND_VCL_NAME, resolve_exclude_url_regex

        effective_regex = resolve_exclude_url_regex(scoring_exclude_url_regex)
        routing_lines = [
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
        scrub_lines.extend(routing_lines)

    scrub_lines.append("}")
    recv_vcl = VclBuilder.join_lines(scrub_lines)

    # miss and pass: unset edge headers + optional group-L timing
    base_unset_lines = ["if (req.backend.is_origin) {", "  unset bereq.http.x-fos-edge-data;"]
    if scoring_enabled:
        base_unset_lines.append("  unset bereq.http.x-edge-score;")
        base_unset_lines.append("  unset bereq.http.X-Edge-Scoring-Pass;")
    base_unset_lines.append("}")
    base_unset = VclBuilder.join_lines(base_unset_lines) + "\n"

    # Session-scoring services route the first-pass request to the scorer
    # Compute backend via `return(pass)` in vcl_recv. That triggers the
    # PASS subroutine for the scorer fetch, which would otherwise capture
    # x-of-start AT THE SCORER FETCH TIME — polluting the eventual TTFB/
    # TTLB numbers with scorer-leg latency. The X-Edge-Scoring-Pass=="1"
    # marker (set by session_scoring_vcl.recv_snippet just before the
    # `return(pass)`) is our discriminator. Non-scoring services never set
    # this header, so the guard is always true and timing fires normally.
    _scoring_guard_open = 'if (req.http.X-Edge-Scoring-Pass != "1") {\n'
    _scoring_guard_close = "}\n"

    def _group_l_timing(label: str) -> str:
        return (
            f"\n# [group-L] Record timing start for {label} fetch\n"
            + _scoring_guard_open
            + "set req.http.x-of-start = time.elapsed.usec;\n"
            "unset bereq.http.x-of-start;\n"
            'set bereq.http.x-is-cluster-fetch = "1";\n'
            "if (req.http.x-edge-req-id) {\n"
            "  set bereq.http.x-edge-req-id = req.http.x-edge-req-id;\n"
            "} else if (req.http.x-req-id) {\n"
            "  set bereq.http.x-edge-req-id = req.http.x-req-id;\n"
            "}\n"
            "unset bereq.http.x-req-id;\n" + _scoring_guard_close
        )

    if group_l:
        miss_vcl = base_unset + _group_l_timing("origin")
        pass_vcl = base_unset + _group_l_timing("PASS")
    else:
        miss_vcl = base_unset
        pass_vcl = base_unset

    snippets: dict[str, str] = {"recv": recv_vcl, "miss": miss_vcl, "pass": pass_vcl}

    # Reset any client-supplied Fastly-Client-IP at the true edge so a spoofed
    # inbound header cannot poison the captured client IP. This is its OWN
    # snippet installed at priority -100 (see CAPTURE_SNIPPET_PLAN) — ahead of
    # operator VCL and the priority-1 capture — so operator VCL may rewrite
    # Fastly-Client-IP to the real source IP *after* this runs (e.g. a service
    # behind a fronting proxy that carries the origin client in X-Source-Ip).
    # The "ip" capture expression (EDGE_DATA_MAPPING) then trusts a present
    # Fastly-Client-IP and falls back to client.ip otherwise. Gated on the same
    # first-edge-pass guard as the header scrub so it neither runs at a shield
    # POP (which must keep the forwarded value) nor re-wipes after a
    # session-scoring restart. Only emitted when the client IP is edge-captured.
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
                # Skip the scoring sub-fetch — we want TTFB for the real
                # origin, not the scorer Compute backend.
                'if (req.http.X-Edge-Scoring-Pass != "1" && req.http.x-of-start != "") {\n'
                "  declare local var.fetch_ttfb INTEGER;\n"
                "  set var.fetch_ttfb = std.atoi(time.elapsed.usec);\n"
                "  set var.fetch_ttfb -= std.atoi(req.http.x-of-start);\n"
                "  set req.http.x-of-ttfb = var.fetch_ttfb;\n"
                # Backend connect/handshake latency (TCP+TLS) to origin/shield,
                # distinct from TTFB. beresp.* is only readable in vcl_fetch, so
                # this is the one place it can be captured; it stays null on the
                # error path (vcl_error has no beresp) and on HITs.
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
                # Capture into beresp so it gets cached with the object
                fetch_lines.append(f"  set beresp.http.x-fos-origin-data:{cf['name']} = {cf['vcl_log_expression']};")
            fetch_lines.append("}")
            fetch_lines.append("# --- Custom Origin Fields End ---")
        snippets["fetch"] = VclBuilder.join_lines(fetch_lines)

    if group_l:
        error_lines = [
            "# [group-L] Capture timing for failed origin fetches\n"
            # Skip the scoring sub-fetch — a scorer error is fail-open
            # handled by our session-scoring snippet and shouldn't
            # pollute the customer's origin-error telemetry.
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
        snippets["error"] = VclBuilder.join_lines(error_lines)

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
                # Skip scoring sub-fetch — don't capture scorer-leg TTLB
                # into the real-request's telemetry.
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
            _m_stats = (
                ("ifsz", "[0-9]+"),
                ("ofsz", "[0-9]+"),
                ("ifmt", "[a-zA-Z0-9]+"),
                ("ofmt", "[a-zA-Z0-9]+"),
            )
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
                freq = cf.get("origin_log_frequency", "all")
                deliver_lines.append(f'if (resp.http.x-fos-origin-data:{name} != "") {{')

                if freq == "miss_pass":
                    deliver_lines.append('  if (fastly_info.state !~ "HIT") {')
                    deliver_lines.append(
                        f"    set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"
                    )
                    deliver_lines.append("  }")
                else:
                    # Promote to req so it's available in vcl_log
                    deliver_lines.append(
                        f"  set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"
                    )

                deliver_lines.append("}")
                # Strip from client responses; keep in cluster responses so the edge node can read and cache it
                deliver_lines.append('if (req.http.x-is-cluster-fetch != "1") {')
                deliver_lines.append(f"  unset resp.http.x-fos-origin-data:{name};")
                deliver_lines.append("}")
            deliver_lines.append("# --- Custom Origin Fields End ---")

        if custom_deliver:
            # Deliver-stage fields read from the RESPONSE headers
            # (e.g. resp.http.X-Edge-Score after a Compute scorer sub-fetch
            # returned). The expression in vcl_log_expression points at the
            # ``req.http.*`` slot the upstream snippet copied it into — same
            # final namespace as edge fields, just captured a stage later in
            # the request lifecycle.
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
    """Return the log format as a single-line string.

    A config that carries custom_fields but no groups/preset MUST still get the
    standard groups. This happens when scoring is enabled on a service that was
    provisioned with an empty log_fields (which relied on the standard-preset
    fallback): adding the scoring custom_fields makes log_fields truthy, so a
    plain ``log_fields_config or {standard}`` fallback never fires and
    url/ua/geo/edge silently drop out of the log format (every non-base field
    vanishes, leaving only base + scoring fields).
    """
    cfg = dict(log_fields_config or {})
    if not cfg.get("groups") and not cfg.get("preset"):
        cfg["groups"] = lf.PRESETS["standard"]["groups"]
        cfg.setdefault("field_overrides", {})
    return lf.generate_log_format(cfg)


import os

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
        vcl_snippets = generate_capture_vcl(log_fields_config)
        valid, message = vcl_utils.lint_log_format(raw, vcl_snippets)
        if not valid:
            return [message]
        return []

    return _validate_log_format_regex(raw)


def _validate_log_format_regex(raw: str) -> list[str]:
    """Regex-based fallback log format checks."""
    errors = []
    expressions = re.findall(r"%\{(.*?)\}V", raw, re.DOTALL)

    for expr in expressions:
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
            rest = expr[m.start() :]
            depth = 0
            closed = False
            for ch in rest:
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


# Account-level edge rate-limiting entitlement. Fastly stamps this pragma into
# the COMPILED (generated) VCL of every VCL service on an entitled account —
# independent of whether that service's source VCL declares any ratecounters.
# Matched on the literal `true` value so a `ratelimit_opt_in false` line (or the
# pragma being absent) reads as "not entitled".
_RATELIMIT_OPT_IN_RE = re.compile(r"ratelimit_opt_in\s+true\b")


def _fetch_active_generated_vcl(service_id: str, token: str) -> str | None:
    """Return the active version's compiled VCL for ``service_id``, or ``None``."""
    if not service_id:
        return None
    try:
        active_ver = get_active_version(service_id, token)
        if active_ver is None:
            return None
        content = get_generated_vcl(service_id, active_ver, token)
    except Exception:  # noqa: BLE001 — detection is best-effort; never break a deploy
        return None
    return content or None


def account_has_rate_limiting(token: str, *service_ids: str) -> bool | None:
    """Proactively detect whether the Fastly account has edge rate limiting.

    Edge rate limiting (``ratecounter`` / ``penaltybox``) is an ACCOUNT-level
    entitlement; Fastly injects ``pragma optional_param ratelimit_opt_in true;``
    into the generated VCL of every VCL service on an entitled account, so
    probing any one VCL service the account owns is sufficient. Tries each id in
    order and returns on the first that yields parseable generated VCL:

    * ``True``  — the pragma is present (account is entitled).
    * ``False`` — generated VCL was read but the pragma is absent/false.
    * ``None``  — no probe was conclusive (every id was a Compute/wasm service
      with no generated VCL, lacked an active version, or errored). Callers
      treat ``None`` as "unknown — don't disable rate limiting; the reactive
      :func:`_validate_with_ratelimit_fallback` remains the backstop".

    Pass more than one id (e.g. the CDN service AND the customer's logging
    service) so a wasm/no-active-version probe falls through to a VCL sibling.
    Fully best-effort: any unexpected error on a probe drops to the next id.
    """
    for service_id in service_ids:
        content = _fetch_active_generated_vcl(service_id, token)
        if not content:
            continue
        return bool(_RATELIMIT_OPT_IN_RE.search(content))
    return None


# Service-level VCL pragmas required for Session Scoring's preflight sub-fetch +
# restart flow so POST/PUT/PATCH request bodies are neither streamed on the
# GET-rewritten scorer preflight pass nor drained across the restart to origin.
_FIX_UNSENT_BODY_DRAIN_RE = re.compile(r"fix_unsent_body_drain\s+true\b")
_NO_BODY_IF_BEREQ_GET_OR_HEAD_RE = re.compile(r"no_body_if_bereq_is_get_or_head\s+true\b")


def service_has_scoring_pragmas(service_id: str, token: str) -> bool | None:
    """Detect whether ``service_id`` has both preflight body preservation
    pragmas enabled in its compiled VCL.

    Returns:
    * ``True``  — both ``fix_unsent_body_drain true`` and
      ``no_body_if_bereq_is_get_or_head true`` are present in the active
      version's generated VCL.
    * ``False`` — generated VCL was read, and at least one pragma is absent or
      ``false``.
    * ``None``  — inconclusive (missing ``service_id``/``token``, no active
      version, empty generated VCL, or Fastly API error).
    """
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
    import json

    from backend import config as _svcconfig
    from backend.provision.declarative.reconciler import reconcile_cdn_service_state
    from backend.provision.fastly_api import account_has_rate_limiting

    logging_service_id = cfg.get("logging_service_id") or cfg.get("service_id")
    if not logging_service_id:
        raise ValueError("logging_service_id or service_id must be provided in config.")

    config_path = _svcconfig.config_path(logging_service_id)

    def _merge_inputs(disk_cfg: dict) -> None:
        # Merge top-level or nested fields from cfg and inputs
        for k, v in cfg.items():
            disk_cfg[k] = v

        disk_cfg["fos_access_key_id"] = fos_access_key
        disk_cfg["fos_secret_access_key"] = fos_secret_key

    # Write updated config to disk (under the config lock; a first-time provision has no file yet)
    with _svcconfig.config_lock(logging_service_id):
        if _svcconfig.mutate_config(logging_service_id, _merge_inputs) is None:
            new_cfg: dict = {}
            _merge_inputs(new_cfg)
            _svcconfig.save_config(logging_service_id, new_cfg)

    # Run the declarative reconciler for CDN service
    try:
        result = reconcile_cdn_service_state(
            logging_service_id=logging_service_id,
            token=token,
            status_cb=status_cb,
            activate=True,
        )
    except Exception as e:
        if config_path.exists():
            try:
                updated_cfg = json.loads(config_path.read_text())
                cdn_sid = updated_cfg.get("fos_proxy", {}).get("service_id") or updated_cfg.get("cdn_service_id", "")
                if on_created and cdn_sid:
                    on_created(cdn_sid)
            except Exception:
                pass
        raise e

    # Load updated config from disk to return the expected dict structure
    updated_cfg = json.loads(config_path.read_text())
    cdn_sid = updated_cfg.get("fos_proxy", {}).get("service_id") or updated_cfg.get("cdn_service_id", "")

    if on_created and cdn_sid:
        on_created(cdn_sid)

    # Detect rate limiting entitlement
    detected_rl = account_has_rate_limiting(token, logging_service_id)

    return {
        "id": cdn_sid,
        "name": updated_cfg.get("cdn_service_name") or f"Log Analysis CDN Service for {logging_service_id}",
        "rate_limiting": detected_rl,
    }


def redeploy_cdn_vcl(cdn_service_id: str, token: str, rate_limiting: bool = True, status_cb=None):
    """Clone active version and redeploy the main VCL snippet via declarative reconciler."""

    from backend import config as _svcconfig
    from backend.config import list_configs
    from backend.provision.declarative.reconciler import reconcile_cdn_service_state

    logging_service_id = None
    for c in list_configs():
        fos_proxy_cfg = c.get("fos_proxy", {})
        if c.get("cdn_service_id") == cdn_service_id or fos_proxy_cfg.get("service_id") == cdn_service_id:
            logging_service_id = c["service_id"]
            break

    if not logging_service_id:
        raise ValueError(f"Could not map cdn_service_id '{cdn_service_id}' to a configured logging_service_id.")

    # We update the rate_limiting_enabled on disk config if different from input rate_limiting
    def _set_rate_limiting(cfg: dict) -> None:
        cfg.setdefault("fos_proxy", {})["rate_limiting_enabled"] = rate_limiting

    if _svcconfig.mutate_config(logging_service_id, _set_rate_limiting) is None:
        raise FileNotFoundError(_svcconfig.config_path(logging_service_id))

    result = reconcile_cdn_service_state(
        logging_service_id=logging_service_id,
        token=token,
        status_cb=status_cb,
        activate=True,
    )

    return result.activated_version


def delete_cdn_service(service_id: str, name: str, token: str, status_cb=None):
    """Delete the analytics-owned log-fronting CDN VCL service.

    ``name`` is a caller-supplied label only — the deletion targets
    ``service_id``. The teardown path used to pass the CUSTOMER's service
    display name here, so the log read
    ``Deleting CDN service 'www.drew-michael.com'`` and looked for all the world
    like the customer's own site was being destroyed (2026-08-13). Callers now
    pass the analytics naming convention (``Log Analysis CDN Service for <id>``)
    and every message below carries the id it is actually acting on.
    """
    label = f"{name} ({service_id})"
    info(f"Deleting analytics-owned log-fronting CDN service {_c(BOLD, label)}…")
    if status_cb:
        status_cb(f"⏳ Deleting analytics-owned CDN service {label} (not your origin service)...")
    try:
        versions = fastly("GET", f"/service/{service_id}/version", token=token)
        for v in versions:
            if v.get("active"):
                if status_cb:
                    status_cb(f"⏳ Deactivating version {v['number']} of {service_id}...")
                fastly("PUT", f"/service/{service_id}/version/{v['number']}/deactivate", token=token)
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("CDN service already deleted")
            return
        pass

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
    """Edge-hop predicate for the 'Log Sampling' response_condition.

    Session scoring restarts the request (req.restarts 0→1) to run the scorer
    sub-fetch, so the FINAL logged pass (the one carrying the real response +
    the captured edge_score subfields) runs at req.restarts == 1. ``vcl_log``
    fires once, at that final pass — so gating the log on ``req.restarts == 0``
    drops EVERY scored request from the logs (edge_score never reaches the log
    line → Fire Rate 0%). ``fastly.ff.visits_this_service == 0`` identifies the
    edge hop restart-invariantly, so the post-restart edge pass still logs.
    """
    return "fastly.ff.visits_this_service == 0"


# Snippet-ownership attribution for teardown. Every name we install is either
# in the capture family (this prefix, across all naming generations) or comes
# from a canonical generator list, so teardown can't drift from provisioning.
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
    from backend.provision.cmcd_fields import cmcd_snippet_names
    from backend.provision.session_scoring_vcl import scoring_snippet_names

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
        eps_list.extend(
            [
                "Fastly Log Analytics",
                "Fastly Object Storage Logs",
                endpoint_name,
            ]
        )
    if remove_rum:
        eps_list.extend(
            [
                "Fastly RUM Logs",
                "Fastly RUM Object Storage Logs",
            ]
        )

    endpoints_to_delete = [ep for ep in dict.fromkeys(eps_list) if ep in existing]

    if not endpoints_to_delete:
        ok(f"No matching logging endpoints found on version {active_ver} — checking snippets anyway")

    info(f"Cloning version {active_ver} → new draft…")
    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _draft_status(msg: str) -> None:
        info(msg)

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
        status_cb=_draft_status,
        validate_msg="Validating draft version…",
        activate_msg="Activating version {ver}…",
        on_failure=_log_failure,
    ) as new_ver:
        ok(f"Draft version: {new_ver}")

        for ep in endpoints_to_delete:
            info(f"Removing '{_c(BOLD, ep)}' from draft…")
            encoded = urllib.parse.quote(ep, safe="")
            try:
                fastly(
                    "DELETE",
                    f"/service/{service_id}/version/{new_ver}/logging/s3/{encoded}",
                    token=token,
                    expect_empty=True,
                )
                ok(f"Logging endpoint '{ep}' removed from draft")
            except RuntimeError as exc:
                if "404" not in str(exc):
                    raise exc

        # ENUMERATE, don't guess. This used to blind-DELETE a hardcoded list of
        # snippet names; every 404 was swallowed, so when the names on the
        # service didn't match the list it reported success having removed
        # NOTHING — leaving the full capture VCL live on a service the operator
        # believed was torn down (observed 2026-08-13: "removed 0 active
        # snippets" while endpoints/conditions/dictionaries were correctly
        # removed, because those three already used the list-then-match pattern
        # below). The list was also missing ``- vcl_pass``, so it stranded a
        # snippet even on a service whose names DID match.
        info("Removing VCL snippets…")
        owned_exact = set()
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
            # Attribution, deliberately narrow. ``Fastly Log Analytics`` covers
            # every generation of the capture snippets (legacy "… Capture" and
            # current "… - vcl_recv"). The rest come from the canonical
            # generators so this can't drift from what provisioning installs.
            # NOTE: do NOT prefix-match "Session " — a customer's own
            # "Session Tracking - *" snippets live alongside our
            # "Session Scoring - *" ones and must never be touched.
            should_remove = False
            if remove_logging and s_name.startswith(_CAPTURE_SNIPPET_PREFIX):
                should_remove = True
            elif s_name in owned_exact:
                should_remove = True

            if not should_remove:
                continue
            info(f"Removing snippet '{_c(BOLD, s_name)}' from draft…")
            try:
                encoded_s = urllib.parse.quote(s_name, safe="")
                fastly(
                    "DELETE",
                    f"/service/{service_id}/version/{new_ver}/snippet/{encoded_s}",
                    token=token,
                    expect_empty=True,
                )
                removed_names.append(s_name)
            except RuntimeError as exc:
                if "404" not in str(exc):
                    raise exc

        ok(f"VCL snippets removed from draft ({len(removed_names)}): {', '.join(removed_names) or 'none matched'}")
        # Never let "removed nothing" pass silently again.
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

        from backend.provision.declarative.reconciler import (
            MANAGED_BACKEND_NAMES,
            MANAGED_DICTIONARY_NAMES,
        )

        managed_cond_names = {"Log Sampling", "log_analytics_condition", "rum_log_condition"}
        for res_path, plural, label, allowed_names in (
            ("backend", "backends", "backend", MANAGED_BACKEND_NAMES),
            ("dictionary", "dictionaries", "dictionary", MANAGED_DICTIONARY_NAMES),
            ("condition", "conditions", "condition", managed_cond_names),
        ):
            try:
                items = fastly("GET", f"/service/{service_id}/version/{new_ver}/{res_path}", token=token)
                for item in items:
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
    sample_rate = cfg.get("sample_rate")
    edge_only = cfg.get("edge_only")
    period = cfg.get("log_period")
    path = cfg.get("fos_path")

    total_steps = 5
    yield {"type": "progress", "current": 0, "total": total_steps}

    from backend import config as _svcconfig
    from backend.provision.cmcd_fields import reconcile_cfg_system_custom_fields

    try:
        service_cfg = _svcconfig.load_config(service_id)
    except Exception as e:
        raise RuntimeError(f"Service config for {service_id} not found: {e}")
    if service_cfg is None:
        raise RuntimeError(f"Service config for {service_id} is empty or None")

    cmcd_enabled_req = cfg.get("cmcd_enabled")
    cmcd_mode_req = cfg.get("cmcd_mode")
    cmcd_version_req = cfg.get("cmcd_version")

    def _apply_requested_changes(target: dict) -> None:
        # Runs on the freshly-read config under the config lock, so only the keys this flow
        # owns are touched and a concurrent writer's changes survive.
        old_cmcd = target.get("cmcd") or {}
        cmcd_was_enabled = bool(old_cmcd.get("enabled"))

        if cmcd_enabled_req is not None:
            import datetime as _dt

            from backend.provision.cmcd_fields import (
                _remove_cmcd_custom_fields,
                reconcile_cmcd_custom_fields,
            )

            want_enabled = bool(cmcd_enabled_req)
            new_mode = cmcd_mode_req or old_cmcd.get("mode", "query_string")
            new_version = int(cmcd_version_req) if cmcd_version_req is not None else old_cmcd.get("version", 1)

            if want_enabled and not cmcd_was_enabled:
                target["cmcd"] = {
                    "enabled": True,
                    "mode": new_mode,
                    "version": new_version,
                    "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
                }
                lf_cfg = target.setdefault("log_fields", {})
                lf_cfg["custom_fields"] = reconcile_cmcd_custom_fields(lf_cfg.get("custom_fields"), enabled=True)
            elif not want_enabled and cmcd_was_enabled:
                target.pop("cmcd", None)
                _remove_cmcd_custom_fields(target)
            elif want_enabled and cmcd_was_enabled:
                if new_mode != old_cmcd.get("mode") or new_version != old_cmcd.get("version"):
                    target["cmcd"] = {
                        **old_cmcd,
                        "mode": new_mode,
                        "version": new_version,
                    }

        # Update state fields in nested provisioning block
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
            # MERGE GUARD (sibling of the cli.py + api_service_log_fields_set
            # guards): callers hand us a log_fields built from groups alone
            # (_build_log_fields_config returns no custom_fields key), so a
            # wholesale assign strips the user's custom_fields AND the
            # system-managed scoring/CMCD entries. reconcile_vcl_state then
            # regenerates the Fastly log format from this config, so the strip
            # reaches the edge: the extraction VCL keeps running and nothing
            # logs its output. Treat "absent OR empty" as "no change".
            incoming_lf = dict(cfg["log_fields"])
            if not incoming_lf.get("custom_fields"):
                existing_custom = (target.get("log_fields") or {}).get("custom_fields")
                if existing_custom:
                    incoming_lf["custom_fields"] = list(existing_custom)
            target["log_fields"] = incoming_lf

        # Re-assert the system-managed custom fields against the FINAL feature
        # state (post-merge above, post-CMCD-request handling earlier). Keyed on
        # state rather than on a transition so a reconcile that changes nothing
        # about CMCD still converges, and a disable strips the fields.
        reconcile_cfg_system_custom_fields(target)
        if path is not None:
            target["fos_path"] = path

    if _svcconfig.mutate_config(service_id, _apply_requested_changes) is None:
        raise RuntimeError(f"Service config for {service_id} is empty or None")

    # Refresh account level rate-limiting best effort (Fastly call: no config lock held)
    try:
        detected_rl = account_has_rate_limiting(token, service_id)
        current_rl = ((_svcconfig.load_config(service_id) or {}).get("provisioning") or {}).get("rate_limiting")
        if detected_rl is not None and current_rl != detected_rl:

            def _set_rate_limiting(target: dict) -> None:
                target.setdefault("provisioning", {})["rate_limiting"] = detected_rl

            _svcconfig.mutate_config(service_id, _set_rate_limiting)
            if detected_rl:
                yield {
                    "type": "status",
                    "message": "ℹ️ Edge rate limiting is now available on this account — redeploy the CDN service to enable it.",
                }
            else:
                yield {"type": "status", "message": "ℹ️ Edge rate limiting is not available on this account."}
    except Exception:
        pass

    # Yield initial progress
    yield {"type": "progress", "current": 1, "total": total_steps}

    from backend.provision.declarative.reconciler import reconcile_vcl_state
    from backend.provision.utils import run_with_events

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


# ── Iceberg Metadata Snippet ──────────────────────────────────────────────────

_ICEBERG_METADATA_SNIPPET_NAME = "iceberg-metadata-pointer-ttl"
_ICEBERG_METADATA_SNIPPET = """\
  if (req.url ~ "metadata_location\\.txt($|\\?)") {
    set beresp.http.Surrogate-Key = "iceberg-metadata-pointer";
    set beresp.ttl = 10s;
    set beresp.stale_while_revalidate = 5s;
    set beresp.stale_if_error = 60s;
  }
  if (req.url ~ "table_summary\\.json($|\\?)") {
    set beresp.http.Surrogate-Key = "iceberg-table-summary";
    set beresp.ttl = 300s;
    set beresp.stale_while_revalidate = 60s;
    set beresp.stale_if_error = 3600s;
  }"""

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

# PyIceberg's commit lifecycle does HEAD-before-PUT on new metadata paths
# (the CAS atomicity check). That HEAD legitimately 404s. If Fastly caches
# the 404, the PUT lands in R2 via FOS native but the CDN keeps serving the
# cached 404 to every subsequent commit's load_table — the cron stays
# broken until TTL expires. Applies to ALL GET/HEAD 404s on this service,
# not just iceberg paths, since any read-then-write pattern hits the same
# trap. Priority 30 so it runs last and wins over any ttl set upstream.
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
    (_ICEBERG_METADATA_SNIPPET_NAME, "fetch", _ICEBERG_METADATA_SNIPPET, 10),
    (_CDN_RECV_SWR_SNIPPET_NAME, "recv", _CDN_RECV_SWR_SNIPPET, 10),
    (_CDN_FETCH_GENERATION_SNIPPET_NAME, "fetch", _CDN_FETCH_GENERATION_SNIPPET, 20),
    (_CDN_NO_CACHE_404_SNIPPET_NAME, "fetch", _CDN_NO_CACHE_404_SNIPPET, 30),
]


# ── Remote Frontend Sharing Domain (merged from sharing_domain.py) ────────────


def _fastly_api_request(
    method: str,
    path: str,
    *,
    token: str,
    json_data: Any = None,
    step: str = "",
) -> Any:
    """Perform a request to the Fastly API via the shared client.

    Delegates transport to ``backend.core.fastly.client.fastly`` so this
    module inherits its retry/backoff, OTel telemetry, and mock-mode support
    instead of hand-rolling a second ``requests``-based client. ``path`` is a
    Fastly API path (e.g. ``/service``); the client pins the ``api.fastly.com``
    base and ``Fastly-Key`` auth. Returns the parsed JSON body (``dict`` or
    ``list``). Errors are re-raised with the human-readable ``step`` for
    provisioning diagnostics.
    """
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
    """Deploy a remote frontend service to Fastly.

    Coordinates with the Fastly REST API to register a new proxy/reverse-proxy
    service pointing to our GCE VM origin.

    Steps executed sequentially:
    1. Create Service: POST https://api.fastly.com/service
       with JSON {"name": service_name, "type": "vcl"}
    2. Verify Draft Version: POST https://api.fastly.com/service/{service_id}/version
       to generate active draft Version 1. Retrieve the version `number`.
    3. Attach Domain: POST https://api.fastly.com/service/{service_id}/version/{version}/domain
       with JSON {"name": domain_name}
    4. Attach Backend Origin: POST https://api.fastly.com/service/{service_id}/version/{version}/backend
       with JSON payload {"name": "gce_vm_origin", "address": origin_host, "port": origin_port, "use_ssl": use_ssl, "ssl_check_cert": False}
    5. Activate Version: PUT https://api.fastly.com/service/{service_id}/version/{version}/activate

    Args:
        service_name: Name of the service to create.
        domain_name: Domain name to attach.
        origin_host: Backend host address.
        origin_port: Backend port.
        use_ssl: Whether to use SSL for the backend origin.
        token: Fastly API token.
        override_host: Optional host header override for the backend origin.

    Returns:
        dict: A dictionary containing:
            - service_id: The ID of the created service.
            - version: The version number.
            - domain_name: The domain name.
            - origin_host: The backend origin host.

    Raises:
        RuntimeError: If any of the Fastly API calls fail.
    """
    # Step 1: Create Service
    step = "Create Service"
    service_payload = {"name": service_name, "type": "vcl"}
    service_data = _fastly_api_request("POST", "/service", token=token, json_data=service_payload, step=step)
    service_id = service_data.get("id")
    if not service_id:
        raise RuntimeError(f"Failed to retrieve service ID from response during {step}")

    # Step 2: Verify Draft Version
    step = "Verify Draft Version"
    version_data = _fastly_api_request("POST", f"/service/{service_id}/version", token=token, step=step)
    version = version_data.get("number")
    if version is None:
        raise RuntimeError(f"Failed to retrieve version number from response during {step}")

    # Step 3: Attach Domain
    step = "Attach Domain"
    domain_payload = {"name": domain_name}
    _fastly_api_request(
        "POST",
        f"/service/{service_id}/version/{version}/domain",
        token=token,
        json_data=domain_payload,
        step=step,
    )

    # Step 4: Attach Backend Origin
    step = "Attach Backend Origin"
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
        step=step,
    )

    # Step 5: Activate Version
    step = "Activate Version"
    _fastly_api_request("PUT", f"/service/{service_id}/version/{version}/activate", token=token, step=step)

    return {
        "service_id": service_id,
        "version": version,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }


def delete_remote_frontend(remote_service_id: str, token: str) -> None:
    """Deactivate and delete the remote frontend service on Fastly."""
    # Step 1: Get versions of the service to deactivate any active ones
    try:
        versions = _fastly_api_request("GET", f"/service/{remote_service_id}/version", token=token, step="Get Versions")
        for v in versions:
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

    # Step 2: Delete the service
    try:
        _fastly_api_request("DELETE", f"/service/{remote_service_id}", token=token, step="Delete Service")
    except Exception as exc:
        if "404" not in str(exc):
            raise exc


sys.modules.setdefault("backend.provision.sharing_domain", sys.modules[__name__])
