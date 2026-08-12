import datetime
import re
import shutil
import urllib.parse

from backend.core import field_registry as lf
from backend.core.fastly.client import fastly
from backend.core.fastly.service import (
    ensure_condition,
    ensure_vcl_snippet,
    find_service_by_name,
    get_active_version,
    get_generated_vcl,
    list_s3_endpoints,
)
from backend.core.fastly.utils import SHIELD_MAP, load_vcl, region_endpoint
from backend.provision.utils import BOLD, _c, fail, info, ok, warn
from backend.utils import field_codes as fc
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


# Capture-snippet install plan — the single source of truth shared by the live
# provisioning path (``install_capture_snippets``) and the Terraform generator
# (``backend.utils.terraform_gen``): which generated snippet lands in which
# subroutine, at what priority, and whether it is always present.
#   (content_key, snippet_name, subroutine, priority, required)
#   * content_key — key in the dict returned by ``generate_capture_vcl``
#   * priority    — lower runs first (Fastly default 100). "Reset Client IP"
#     uses -100 so it runs ahead of operator VCL and the priority-1 capture.
#   * required    — core phases are always generated; optional phases are
#     guarded by ``content_key in snippets``.
CAPTURE_SNIPPET_PLAN: tuple[tuple[str, str, str, int, bool], ...] = (
    ("recv_reset", "Fastly Log Analytics Reset Client IP", "recv", -100, False),
    ("recv", "Fastly Log Analytics Capture", "recv", 1, True),
    ("miss", "Fastly Log Analytics Miss", "miss", 100, True),
    ("pass", "Fastly Log Analytics Pass", "pass", 100, True),
    ("fetch", "Fastly Log Analytics Origin Fetch", "fetch", 100, False),
    ("deliver", "Fastly Log Analytics Origin Deliver", "deliver", 100, False),
    ("error", "Fastly Log Analytics Origin Error", "error", 100, False),
)


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

    for statement in get_capture_vcl_statements(log_fields_config):
        scrub_lines.append(f"  {statement}")

    if rum_enabled:
        rum_lines = [
            "  # Section 5: RUM (vcl_recv)",
            "  # Handle RUM beacon POST to /rum-beacon",
            '  if (req.url.path == "/rum-beacon") {',
            "      # Extract the essential fields from querystring:",
            "      # - cid: session ID from rum_cid cookie (set in deliver)",
            "      # - req: per-request ID (minted in recv)",
            "      # - raw query: complete set of event_N_* params, parsed during ingest",
            '      set req.http.x-fos-edge-data:rum_cid = querystring.get(req.url, "cid");',
            '      set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");',
            '      if (req.http.x-fos-edge-data:fastly_req_id == "") {',
            "          set req.http.x-fos-edge-data:fastly_req_id = req.http.Fastly-Request-ID;",
            "      }",
            "      set req.http.x-fos-edge-data:rum_raw_query = req.url;",
            "      set req.http.x-fos-edge-data:rum_body = req.body;",
            "",
            "      # Mark beacon to skip S3 logging (already logged separately to metadata DB)",
            '      set req.http.x-skip-rum-logging = "1";',
            "",
            "      # Synthetic 204 response (no origin round-trip needed)",
            '      error 611 "No Content";',
            "  }",
        ]
        scrub_lines.extend(rum_lines)

    if cmcd_enabled:
        from backend.provision.cmcd_vcl import generate_cmcd_vcl

        cmcd_vcl_dict = generate_cmcd_vcl(mode=cmcd_mode, version=cmcd_version)
        cmcd_body = next(iter(cmcd_vcl_dict.values()))
        cmcd_lines = [
            "  # Section 3: CMCD Extraction (vcl_recv)",
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
    recv_vcl = "\n".join(scrub_lines)

    # miss and pass: unset edge headers + optional group-L timing
    base_unset_lines = ["if (req.backend.is_origin) {", "  unset bereq.http.x-fos-edge-data;"]
    if scoring_enabled:
        base_unset_lines.append("  unset bereq.http.x-edge-score;")
        base_unset_lines.append("  unset bereq.http.X-Edge-Scoring-Pass;")
    base_unset_lines.append("}")
    base_unset = "\n".join(base_unset_lines) + "\n"

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

    if group_l:
        miss_vcl = base_unset + (
            "\n# [group-L] Record timing start for origin fetch\n"
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
        pass_vcl = base_unset + (
            "\n# [group-L] Record timing start for PASS fetch\n"
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
        snippets["fetch"] = "\n".join(fetch_lines)

        error_lines = []
        if group_l:
            error_lines.append(
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
            )
        snippets["error"] = "\n".join(error_lines)

    if group_l or group_m or custom_origin or custom_deliver:
        deliver_lines = []
        if group_l:
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
                'if (req.http.x-of-ttfb == "" && resp.http.x-of-ttfb != "") { set req.http.x-of-ttfb = resp.http.x-of-ttfb; }\n'
                'if (req.http.x-of-ttlb == "" && resp.http.x-of-ttlb != "") { set req.http.x-of-ttlb = resp.http.x-of-ttlb; }\n'
                'if (req.http.x-of-connect == "" && resp.http.x-of-connect != "") { set req.http.x-of-connect = resp.http.x-of-connect; }\n'
                'if (req.http.x-of-status == "" && resp.http.x-of-status != "") { set req.http.x-of-status = resp.http.x-of-status; }\n'
                'if (req.http.x-of-oip == "" && resp.http.x-of-oip != "") { set req.http.x-of-oip = resp.http.x-of-oip; }\n'
                'if (req.http.x-of-oretries == "" && resp.http.x-of-oretries != "") { set req.http.x-of-oretries = req.restarts; }\n'
                "\n"
                'if (req.http.x-is-cluster-fetch != "1") {\n'
                "  # Returning to client: strip internal metrics from response\n"
                "  unset resp.http.x-of-ttfb;\n"
                "  unset resp.http.x-of-ttlb;\n"
                "  unset resp.http.x-of-connect;\n"
                "  unset resp.http.x-of-status;\n"
                "  unset resp.http.x-of-oip;\n"
                "  unset resp.http.x-of-oretries;\n"
                "} else {\n"
                "  # Returning to internal cluster node: ensure metrics are attached to response\n"
                '  if (req.http.x-of-ttfb != "") { set resp.http.x-of-ttfb = req.http.x-of-ttfb; }\n'
                '  if (req.http.x-of-ttlb != "") { set resp.http.x-of-ttlb = req.http.x-of-ttlb; }\n'
                '  if (req.http.x-of-connect != "") { set resp.http.x-of-connect = req.http.x-of-connect; }\n'
                '  if (req.http.x-of-status != "") { set resp.http.x-of-status = req.http.x-of-status; }\n'
                '  if (req.http.x-of-oip != "") { set resp.http.x-of-oip = req.http.x-of-oip; }\n'
                '  if (req.http.x-of-oretries != "") { set resp.http.x-of-oretries = req.http.x-of-oretries; }\n'
                "}\n"
                "unset resp.http.x-edge-req-id;"
            )

        if group_m:
            deliver_lines.append(
                "# [group-M] Extract IO metrics from Fastly-Io-Transform-Stats\n"
                'if (req.http.x-fos-io-ifsz == "" && resp.http.x-fos-io-ifsz != "") '
                "{ set req.http.x-fos-io-ifsz = resp.http.x-fos-io-ifsz; }\n"
                'if (req.http.x-fos-io-ofsz == "" && resp.http.x-fos-io-ofsz != "") '
                "{ set req.http.x-fos-io-ofsz = resp.http.x-fos-io-ofsz; }\n"
                'if (req.http.x-fos-io-ifmt == "" && resp.http.x-fos-io-ifmt != "") '
                "{ set req.http.x-fos-io-ifmt = resp.http.x-fos-io-ifmt; }\n"
                'if (req.http.x-fos-io-ofmt == "" && resp.http.x-fos-io-ofmt != "") '
                "{ set req.http.x-fos-io-ofmt = resp.http.x-fos-io-ofmt; }\n"
                'if (resp.http.Fastly-Io-Transform-Stats != "") {\n'
                '  if (resp.http.Fastly-Io-Transform-Stats ~ "ifsz=") {\n'
                "    set req.http.x-fos-io-ifsz = regsub(resp.http.Fastly-Io-Transform-Stats, "
                '".*ifsz=([0-9]+).*", "\\1");\n'
                "  }\n"
                '  if (resp.http.Fastly-Io-Transform-Stats ~ "ofsz=") {\n'
                "    set req.http.x-fos-io-ofsz = regsub(resp.http.Fastly-Io-Transform-Stats, "
                '".*ofsz=([0-9]+).*", "\\1");\n'
                "  }\n"
                '  if (resp.http.Fastly-Io-Transform-Stats ~ "ifmt=") {\n'
                "    set req.http.x-fos-io-ifmt = regsub(resp.http.Fastly-Io-Transform-Stats, "
                '".*ifmt=([a-zA-Z0-9]+).*", "\\1");\n'
                "  }\n"
                '  if (resp.http.Fastly-Io-Transform-Stats ~ "ofmt=") {\n'
                "    set req.http.x-fos-io-ofmt = regsub(resp.http.Fastly-Io-Transform-Stats, "
                '".*ofmt=([a-zA-Z0-9]+).*", "\\1");\n'
                "  }\n"
                "}\n"
                'if (req.http.x-is-cluster-fetch != "1") {\n'
                "  unset resp.http.x-fos-io-ifsz;\n"
                "  unset resp.http.x-fos-io-ofsz;\n"
                "  unset resp.http.x-fos-io-ifmt;\n"
                "  unset resp.http.x-fos-io-ofmt;\n"
                "} else {\n"
                '  if (req.http.x-fos-io-ifsz != "") '
                "{ set resp.http.x-fos-io-ifsz = req.http.x-fos-io-ifsz; }\n"
                '  if (req.http.x-fos-io-ofsz != "") '
                "{ set resp.http.x-fos-io-ofsz = req.http.x-fos-io-ofsz; }\n"
                '  if (req.http.x-fos-io-ifmt != "") '
                "{ set resp.http.x-fos-io-ifmt = req.http.x-fos-io-ifmt; }\n"
                '  if (req.http.x-fos-io-ofmt != "") '
                "{ set resp.http.x-fos-io-ofmt = req.http.x-fos-io-ofmt; }\n"
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

        snippets["deliver"] = "\n".join(deliver_lines)

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


def install_capture_snippets(
    service_id: str,
    version: int,
    log_fields_config: dict | None,
    token: str,
    scoring_enabled: bool = False,
) -> None:
    # Delete legacy/historical snippet names to prevent collisions during compilation
    legacy_snippet_names = [
        "Fastly Log Analytics - vcl_recv",
        "Fastly Log Analytics - vcl_miss",
        "Fastly Log Analytics - vcl_pass",
        "Fastly Log Analytics - vcl_fetch",
        "Fastly Log Analytics - vcl_error",
        "Fastly Log Analytics - vcl_deliver",
    ]
    try:
        current_snippets = fastly("GET", f"/service/{service_id}/version/{version}/snippet", token=token)
        for s in current_snippets:
            s_name = s.get("name")
            if s_name in legacy_snippet_names:
                encoded_name = urllib.parse.quote(s_name, safe="")
                try:
                    fastly("DELETE", f"/service/{service_id}/version/{version}/snippet/{encoded_name}", token=token)
                    info(f"Deleted legacy snippet: {s_name}")
                except Exception as ex:
                    warn(f"Failed to delete legacy snippet {s_name}: {ex}")
    except Exception as e:
        warn(f"Failed to fetch current snippets for legacy cleanup: {e}")

    snippets = generate_capture_vcl(log_fields_config, scoring_enabled=scoring_enabled)
    # CAPTURE_SNIPPET_PLAN is the single source of truth: required phases
    # ("recv", "miss", "pass") are always generated; optional phases
    # ("recv_reset", "fetch", "deliver", "error") only exist for certain
    # configs and are guarded by `content_key in snippets`.
    for content_key, snip_name, subroutine, priority, required in CAPTURE_SNIPPET_PLAN:
        if not required and content_key not in snippets:
            continue
        ensure_vcl_snippet(
            snip_name,
            subroutine,
            snippets[content_key],
            priority,
            service_id,
            version,
            token,
        )


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
        if not service_id:
            continue
        try:
            active_ver = get_active_version(service_id, token)
            if active_ver is None:
                continue
            content = get_generated_vcl(service_id, active_ver, token)
        except Exception:  # noqa: BLE001 — detection is best-effort; never break a deploy
            continue
        if not content:
            continue
        return bool(_RATELIMIT_OPT_IN_RE.search(content))
    return None


def _validate_with_ratelimit_fallback(svc_id, ver, token, *, status_cb=None, ok_msg=None, ok_fallback_msg=None):
    """Validate ``svc_id``/``ver`` and, if it fails on a missing rate-limiting
    feature (ratecounter / penaltybox / ratelimit), redeploy the main VCL
    without rate limiting and re-validate. Returns the final validate result.

    Shared by ``ensure_cdn_service`` and ``redeploy_cdn_vcl`` — both ran the
    identical GET-validate → keyword-sniff → no-ratelimit redeploy → re-validate
    sequence. Raises ``RuntimeError`` only for the no-ratelimit fallback that
    STILL fails to validate (its caller-specific message). A non-rate-limit
    validation failure is NOT raised here — the helper returns the not-ok
    result so each caller keeps its own distinct final raise / status branch.
    ``ok_msg`` / ``ok_fallback_msg`` let a caller emit its success log (the
    initial-ok and rate-limiting-disabled messages differ per caller); omit
    them to stay silent on the ok path.
    """
    result = fastly("GET", f"/service/{svc_id}/version/{ver}/validate", token=token)

    if result.get("status") != "ok":
        errors = result.get("errors") or result.get("msg") or result
        errors_str = str(errors).lower()
        if any(kw in errors_str for kw in ("ratecounter", "penaltybox", "ratelimit")):
            warn("Rate limiting not available on this account — redeploying without it")
            if status_cb:
                status_cb("⚠️ Rate limiting unavailable; redeploying without it...")
            vcl_no_rl = load_vcl(rate_limiting=False)
            fastly("PUT", f"/service/{svc_id}/version/{ver}/vcl/main", {"content": vcl_no_rl}, token=token)
            result = fastly("GET", f"/service/{svc_id}/version/{ver}/validate", token=token)
            if result.get("status") != "ok":
                raise RuntimeError(f"VCL validation failed (no-ratelimit fallback): {result.get('errors')}")
            if ok_fallback_msg:
                ok(ok_fallback_msg)
        return result

    if ok_msg:
        ok(ok_msg)
    return result


def ensure_cdn_service(
    cfg: dict, fos_access_key: str, fos_secret_key: str, token: str, status_cb=None, on_created=None
) -> dict:
    """Create and activate the CDN VCL service fronting FOS."""
    name = cfg["cdn_service_name"]
    domain = cfg["cdn_url"].replace("https://", "")
    region = cfg["fos_region"]
    fos_host = region_endpoint(region)
    shield_pop = cfg.get("cdn_shield")
    if not shield_pop:
        shield_pop = SHIELD_MAP.get(region, "iad-va-us")
    shield_enabled = shield_pop.lower() != "none"

    existing = find_service_by_name(name, token)
    if existing:
        raise RuntimeError(
            f"CDN service '{name}' already exists. Please delete it from Fastly or use a different name."
        )

    info(f"Creating CDN VCL service {_c(BOLD, name)}…")
    if status_cb:
        status_cb(f"⏳ Creating CDN service '{name}'...")
    svc = fastly("POST", "/service", {"name": name, "type": "vcl"}, token=token)
    svc_id = svc["id"]
    # Hand the new service id to the caller IMMEDIATELY. Everything below (domain,
    # backend, dictionaries, VCL, validation, activation) can still fail, and the
    # orchestrator otherwise only records cdn_service_id AFTER this function
    # returns — so a failure here would orphan a CDN service the rollback can't
    # see, blocking re-provision with "CDN service already exists".
    if on_created:
        on_created(svc_id)
    v = 1
    ok(f"Service created  (id: {svc_id})")

    logging_service_id = cfg.get("logging_service_id", "")
    service_comment = (
        f"CDN fronting service for the Fastly Object Storage log bucket associated with "
        f"service {logging_service_id}. Provides authenticated read access to stored log "
        f"files for the Fastly Log Analysis tool."
    )
    fastly("PUT", f"/service/{svc_id}", {"comment": service_comment}, token=token)

    info(f"Adding domain {_c(BOLD, domain)}…")
    if status_cb:
        status_cb(f"⏳ Adding domain {domain}...")
    fastly(
        "POST", f"/service/{svc_id}/version/{v}/domain", {"name": domain, "comment": "Log Analysis CDN"}, token=token
    )
    ok("Domain added")

    backend_payload = {
        "name": "fos_origin",
        "address": fos_host,
        "port": 443,
        "use_ssl": True,
        "ssl_cert_hostname": fos_host,
        "ssl_sni_hostname": fos_host,
        "auto_loadbalance": False,
        "connect_timeout": 5000,
        "first_byte_timeout": 60000,
        "between_bytes_timeout": 30000,
    }

    if shield_enabled:
        backend_payload["shield"] = shield_pop
        info(f"Adding backend → {_c(BOLD, fos_host)} (Shield POP: {_c(BOLD, shield_pop)})…")
        if status_cb:
            status_cb(f"⏳ Adding backend {fos_host} shielded at {shield_pop}...")
    else:
        info(f"Adding backend → {_c(BOLD, fos_host)} (Shield disabled)…")
        if status_cb:
            status_cb(f"⏳ Adding backend {fos_host} (no shield)...")

    fastly("POST", f"/service/{svc_id}/version/{v}/backend", backend_payload, token=token)

    info("Configuring edge dictionary for FOS credentials…")
    if status_cb:
        status_cb("⏳ Configuring edge dictionary for credentials...")
    dict_resp = fastly(
        "POST",
        f"/service/{svc_id}/version/{v}/dictionary",
        {"name": "fos_credentials", "write_only": True},
        token=token,
    )
    dict_id = dict_resp["id"]

    fastly(
        "POST",
        f"/service/{svc_id}/dictionary/{dict_id}/item",
        {"item_key": "access_key", "item_value": fos_access_key},
        token=token,
    )
    fastly(
        "POST",
        f"/service/{svc_id}/dictionary/{dict_id}/item",
        {"item_key": "secret_key", "item_value": fos_secret_key},
        token=token,
    )
    fastly(
        "POST",
        f"/service/{svc_id}/dictionary/{dict_id}/item",
        {"item_key": "bucket", "item_value": cfg["fos_bucket_name"]},
        token=token,
    )
    fastly(
        "POST",
        f"/service/{svc_id}/dictionary/{dict_id}/item",
        {"item_key": "region", "item_value": region},
        token=token,
    )
    ok("FOS credentials configured")

    info("Configuring edge dictionary for CDN auth secret…")
    if status_cb:
        status_cb("⏳ Configuring CDN auth dictionary...")
    auth_dict_resp = fastly(
        "POST", f"/service/{svc_id}/version/{v}/dictionary", {"name": "cdn_auth", "write_only": True}, token=token
    )
    fastly(
        "POST",
        f"/service/{svc_id}/dictionary/{auth_dict_resp['id']}/item",
        {"item_key": "secret", "item_value": cfg["cdn_secret"]},
        token=token,
    )
    ok("CDN auth dictionary configured")

    # Proactively detect whether the account has edge rate limiting so we upload
    # the correct VCL on the first try, instead of relying on the reactive
    # validate fallback (which still backstops a ``None``/unknown result). The
    # ratecounter/penaltybox entitlement is account-level, so the customer's
    # existing logging service is a valid probe.
    detected_rl = account_has_rate_limiting(token, logging_service_id)
    rate_limiting = True if detected_rl is None else detected_rl
    if detected_rl is False and status_cb:
        status_cb("ℹ️ Edge rate limiting unavailable on this account; deploying CDN without it...")

    info("Uploading custom VCL…")
    if status_cb:
        status_cb("⏳ Uploading custom VCL...")
    vcl = load_vcl(rate_limiting=rate_limiting)
    fastly("POST", f"/service/{svc_id}/version/{v}/vcl", {"name": "main", "content": vcl, "main": True}, token=token)
    ok("VCL uploaded")

    info("Deploying CDN VCL snippets…")
    if status_cb:
        status_cb("⏳ Deploying CDN VCL snippets...")
    for snip_name, stype, snip_content, priority in _CDN_SNIPPETS:
        ensure_vcl_snippet(snip_name, stype, snip_content, priority=priority, service_id=svc_id, version=v, token=token)
    ok("VCL snippets deployed")

    info("Validating service version…")
    if status_cb:
        status_cb("⏳ Validating service configuration...")
    result = _validate_with_ratelimit_fallback(
        svc_id,
        v,
        token,
        status_cb=status_cb,
        ok_msg="Version validated",
        ok_fallback_msg="Version validated (rate limiting disabled)",
    )
    if result.get("status") != "ok":
        errors = result.get("errors") or result.get("msg") or result
        raise RuntimeError(f"VCL validation failed: {errors}")

    info("Activating CDN service…")
    if status_cb:
        status_cb("⏳ Activating CDN service...")
    fastly("PUT", f"/service/{svc_id}/version/{v}/activate", token=token)
    ok(f"CDN service active  (version {v})")
    if status_cb:
        status_cb("✅ CDN service active.")

    # ``rate_limiting`` is the account entitlement (bool) or None when detection
    # was inconclusive; the orchestrator persists it to provisioning.rate_limiting
    # only when conclusive (None leaves the default-True read path intact).
    return {"id": svc_id, "name": name, "rate_limiting": detected_rl}


def redeploy_cdn_vcl(cdn_service_id: str, token: str, rate_limiting: bool = True, status_cb=None):
    """Clone active version and redeploy the main VCL snippet."""
    if status_cb:
        status_cb(f"🔍 Checking active version of CDN service {cdn_service_id}...")
    active_ver = get_active_version(cdn_service_id, token)
    if active_ver is None:
        raise RuntimeError(f"CDN service {cdn_service_id} has no active version.")

    if status_cb:
        status_cb(f"🔄 Cloning version {active_ver}...")
    clone = fastly("PUT", f"/service/{cdn_service_id}/version/{active_ver}/clone", token=token)
    new_ver = clone["number"]

    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{cdn_service_id}/version/{new_ver}",
        {"comment": f"VCL update via Fastly Log Analysis at {ts}"},
        token=token,
    )

    if status_cb:
        status_cb(f"⏳ Uploading VCL to version {new_ver}...")
    vcl_content = load_vcl(rate_limiting=rate_limiting)
    fastly(
        "PUT",
        f"/service/{cdn_service_id}/version/{new_ver}/vcl/main",
        {"content": vcl_content},
        token=token,
    )

    # Reconcile CDN snippets on the new version before validation.
    # ensure_vcl_snippet is idempotent (diffs by content/type/priority), so
    # this is safe to call on every redeploy. Without it, snippet-only
    # changes (like cdn-no-cache-404 added for the 2026-05-19 commit-cron
    # negative-cache outage) never reach the live service.
    if status_cb:
        status_cb("⏳ Reconciling CDN VCL snippets...")
    for snip_name, stype, snip_content, priority in _CDN_SNIPPETS:
        ensure_vcl_snippet(
            snip_name,
            stype,
            snip_content,
            priority=priority,
            service_id=cdn_service_id,
            version=new_ver,
            token=token,
        )

    if status_cb:
        status_cb("⏳ Validating...")
    result = _validate_with_ratelimit_fallback(cdn_service_id, new_ver, token, status_cb=status_cb)

    if result.get("status") == "ok":
        if status_cb:
            status_cb(f"🚀 Activating version {new_ver}...")
        fastly("PUT", f"/service/{cdn_service_id}/version/{new_ver}/activate", token=token)
        return new_ver
    else:
        raise RuntimeError(f"Validation failed: {result}")


def delete_cdn_service(service_id: str, name: str, token: str, status_cb=None):
    """Delete the CDN VCL service."""
    info(f"Deleting CDN service {_c(BOLD, name)}  ({service_id})…")
    if status_cb:
        status_cb(f"⏳ Deleting CDN service '{name}'...")
    try:
        versions = fastly("GET", f"/service/{service_id}/version", token=token)
        for v in versions:
            if v.get("active"):
                if status_cb:
                    status_cb(f"⏳ Deactivating version {v['number']}...")
                fastly("PUT", f"/service/{service_id}/version/{v['number']}/deactivate", token=token)
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("CDN service already deleted")
            return
        pass

    try:
        fastly("DELETE", f"/service/{service_id}", token=token, expect_empty=True)
        ok("CDN service deleted")
        if status_cb:
            status_cb("✅ CDN service deleted.")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("CDN service already deleted")
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


def ensure_logging_endpoint(cfg: dict, fos_access_key: str, fos_secret_key: str, token: str, status_cb=None) -> int:
    """Safely add a logging endpoint to the target service's active version."""
    service_id = cfg["logging_service_id"]
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    region = cfg["fos_region"]
    bucket = cfg["fos_bucket_name"]
    prefix = (cfg.get("fos_prefix") or "").strip("/")
    period = cfg["log_period"]
    path = f"/{prefix}/raw/%Y-%m-%d/%H/" if prefix else "/raw/%Y-%m-%d/%H/"

    info(f"Checking active version of service {_c(BOLD, service_id)}…")
    if status_cb:
        status_cb(f"🔍 Checking active version of service {service_id}...")
    active_ver = get_active_version(service_id, token)
    if active_ver is None:
        raise RuntimeError(f"Service {service_id} has no active version.")
    ok(f"Active version: {active_ver}")

    existing = list_s3_endpoints(service_id, active_ver, token)
    if endpoint_name in existing:
        ok(f"Logging endpoint '{endpoint_name}' already present on version {active_ver}")
        if status_cb:
            status_cb(f"✅ Logging endpoint already present on version {active_ver}.")
        return active_ver

    info(f"Cloning version {active_ver} → new draft…")
    if status_cb:
        status_cb(f"🔄 Cloning version {active_ver} to create a new draft...")
    clone = fastly("PUT", f"/service/{service_id}/version/{active_ver}/clone", token=token)
    new_ver = clone["number"]

    ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fastly(
        "PUT",
        f"/service/{service_id}/version/{new_ver}",
        {"comment": f"Provisioning Fastly Object Storage logging at {ts}"},
        token=token,
    )
    ok(f"Draft version: {new_ver}")

    try:
        info(f"Adding Fastly Object Storage logging endpoint '{_c(BOLD, endpoint_name)}'…")
        if status_cb:
            status_cb(f"➕ Adding logging endpoint '{endpoint_name}' to draft...")

        # Prefer nested provisioning.* fields, fall back to legacy flat fields for backward compat
        prov = cfg.get("provisioning", {})
        sample_rate = prov.get("sample_rate") if "sample_rate" in prov else cfg.get("sample_rate", 100)
        sample_rate = int(sample_rate) if sample_rate is not None else 100
        edge_only = prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False)
        edge_only = bool(edge_only)
        # ``.get(k, "")`` only defaults when the key is ABSENT; the /execute API
        # passes custom_condition=None explicitly, so guard with ``or ""``.
        custom_condition = prov.get("custom_condition") if "custom_condition" in prov else cfg.get("custom_condition")
        custom_condition = (custom_condition or "").strip()

        scoring_enabled = bool((cfg.get("scoring") or {}).get("enabled"))
        cond_parts = ["!segmented_caching.is_inner_req"]
        if edge_only:
            cond_parts.append(_log_sampling_edge_clause(scoring_enabled))
        if sample_rate < 100:
            cond_parts.append(f"randombool({sample_rate}, 100)")
        if custom_condition:
            cond_parts.append(f"({custom_condition})")

        cond_stmt = " && ".join(cond_parts)
        cond_name = "Log Sampling"
        ensure_condition(cond_name, cond_stmt, "RESPONSE", service_id, new_ver, token)
        resp_condition = cond_name

        log_format = load_log_format(cfg.get("log_fields"))
        payload = {
            "name": endpoint_name,
            "bucket_name": bucket,
            "domain": region_endpoint(region),
            "access_key": fos_access_key,
            "secret_key": fos_secret_key,
            "path": path,
            "period": period,
            "format_version": 2,
            "format": log_format,
            "gzip_level": 9,
            "message_type": "blank",
            "timestamp_format": "%Y-%m-%dT%H:%M:%S.000",
            "compression_codec": None,
            "server_side_encryption": None,
            "public_key": None,
        }
        if resp_condition:
            payload["response_condition"] = resp_condition

        fastly("POST", f"/service/{service_id}/version/{new_ver}/logging/s3", payload, token=token)

        info("Deploying VCL snippets to capture edge values…")
        if status_cb:
            status_cb("⏳ Deploying VCL snippets to capture edge values...")

        install_capture_snippets(
            service_id,
            new_ver,
            cfg.get("log_fields"),
            token,
            scoring_enabled=bool((cfg.get("scoring") or {}).get("enabled")),
        )

        cmcd_block = cfg.get("cmcd") or {}
        if cmcd_block.get("enabled"):
            from backend.provision.cmcd_vcl import (
                CMCD_SNIPPET_NAME,
                CMCD_SNIPPET_PRIORITY,
                generate_cmcd_vcl,
            )

            vcl_snips = generate_cmcd_vcl(
                mode=cmcd_block.get("mode", "query_string"),
                version=cmcd_block.get("version", 1),
            )
            ensure_vcl_snippet(
                CMCD_SNIPPET_NAME,
                "recv",
                vcl_snips[CMCD_SNIPPET_NAME],
                CMCD_SNIPPET_PRIORITY,
                service_id,
                new_ver,
                token,
            )

        ok("Logging endpoint and VCL snippets added to draft")

        info("Validating draft version…")
        if status_cb:
            status_cb("⏳ Validating draft configuration...")
        result = fastly("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
        ok("Draft validated")

        info("Activating version {new_ver}…")
        if status_cb:
            status_cb(f"⏳ Activating version {new_ver}...")
        fastly("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token)
        ok(f"Version {new_ver} is now active")
        if status_cb:
            status_cb(f"✅ Version {new_ver} is now active.")
        return new_ver

    except Exception as exc:
        fail(str(exc))
        info(f"Rolling back — re-activating version {active_ver}...")
        try:
            fastly("PUT", f"/service/{service_id}/version/{active_ver}/activate", token=token)
        except RuntimeError:
            pass
        raise


def remove_logging_endpoint(service_id: str, endpoint_name: str, token: str, status_cb=None):
    """Safely remove the logging endpoint from the active version."""
    info(f"Checking active version of service {_c(BOLD, service_id)}…")
    if status_cb:
        status_cb(f"🔍 Checking active version of service {service_id}...")
    active_ver = get_active_version(service_id, token)
    if active_ver is None:
        warn(f"Service {service_id} has no active version — skipping.")
        return
    ok(f"Active version: {active_ver}")

    existing = list_s3_endpoints(service_id, active_ver, token)
    endpoints_to_delete = [
        ep
        for ep in [
            "Fastly Log Analytics",
            "Fastly Object Storage Logs",
            "Fastly RUM Logs",
            "Fastly RUM Object Storage Logs",
            endpoint_name,
        ]
        if ep in existing
    ]

    if not endpoints_to_delete:
        ok(f"No matching logging endpoints found on version {active_ver} — checking snippets anyway")

    info(f"Cloning version {active_ver} → new draft…")
    clone = fastly("PUT", f"/service/{service_id}/version/{active_ver}/clone", token=token)
    new_ver = clone["number"]
    ok(f"Draft version: {new_ver}")

    try:
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

        info("Removing VCL snippets…")
        target_snippets = [
            "Fastly Log Analytics Capture",
            "Fastly Log Analytics Miss",
            "Fastly Log Analytics Pass",
            "Fastly Log Analytics Origin Fetch",
            "Fastly Log Analytics Origin Error",
            "Fastly Log Analytics Origin Deliver",
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_fetch",
            "Fastly Log Analytics - vcl_deliver",
            "Fastly Log Analytics - vcl_error",
        ]
        removed_count = 0
        for snippet_name in target_snippets:
            try:
                encoded_s = urllib.parse.quote(snippet_name, safe="")
                fastly(
                    "DELETE",
                    f"/service/{service_id}/version/{new_ver}/snippet/{encoded_s}",
                    token=token,
                    expect_empty=True,
                )
                removed_count += 1
            except RuntimeError as exc:
                if "404" not in str(exc):
                    raise exc
        ok(f"VCL snippets removed from draft (removed {removed_count} active snippets)")

        # Clean up any managed backends
        try:
            backends = fastly("GET", f"/service/{service_id}/version/{new_ver}/backend", token=token)
            managed_backend_names = {"session_scorer", "fos_origin", "F_fos_origin", "F_F_fos_origin", "rum_collector"}
            for b in backends:
                b_name = b.get("name")
                if b_name in managed_backend_names:
                    info(f"Removing managed backend '{b_name}' from draft…")
                    encoded_b = urllib.parse.quote(b_name, safe="")
                    fastly(
                        "DELETE",
                        f"/service/{service_id}/version/{new_ver}/backend/{encoded_b}",
                        token=token,
                        expect_empty=True,
                    )
                    ok(f"Managed backend '{b_name}' removed from draft")
        except Exception as e:
            warn(f"Failed to check/remove backends: {e}")

        # Clean up any managed dictionaries
        try:
            dicts = fastly("GET", f"/service/{service_id}/version/{new_ver}/dictionary", token=token)
            managed_dict_names = {"fos_credentials"}
            for d in dicts:
                d_name = d.get("name")
                if d_name in managed_dict_names:
                    info(f"Removing managed dictionary '{d_name}' from draft…")
                    encoded_d = urllib.parse.quote(d_name, safe="")
                    fastly(
                        "DELETE",
                        f"/service/{service_id}/version/{new_ver}/dictionary/{encoded_d}",
                        token=token,
                        expect_empty=True,
                    )
                    ok(f"Managed dictionary '{d_name}' removed from draft")
        except Exception as e:
            warn(f"Failed to check/remove dictionaries: {e}")

        # Clean up any managed conditions
        try:
            conds = fastly("GET", f"/service/{service_id}/version/{new_ver}/condition", token=token)
            managed_cond_names = {"Log Sampling", "log_analytics_condition", "rum_log_condition"}
            for c in conds:
                c_name = c.get("name")
                if c_name in managed_cond_names:
                    info(f"Removing managed condition '{c_name}' from draft…")
                    encoded_c = urllib.parse.quote(c_name, safe="")
                    fastly(
                        "DELETE",
                        f"/service/{service_id}/version/{new_ver}/condition/{encoded_c}",
                        token=token,
                        expect_empty=True,
                    )
                    ok(f"Managed condition '{c_name}' removed from draft")
        except Exception as e:
            warn(f"Failed to check/remove conditions: {e}")

        info("Validating draft version…")
        result = fastly("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
        if result.get("status") != "ok":
            raise RuntimeError(f"Validation failed: {result.get('errors') or result}")
        ok("Draft validated")

        ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        fastly(
            "PUT",
            f"/service/{service_id}/version/{new_ver}",
            {"comment": f"Deactivating Fastly Object Storage logging at {ts}"},
            token=token,
        )
        info(f"Activating version {new_ver}…")
        fastly("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token)
        ok(f"Version {new_ver} is now active")

    except Exception as exc:
        fail(str(exc))
        info(f"Rolling back — re-activating version {active_ver}...")
        try:
            fastly("PUT", f"/service/{service_id}/version/{active_ver}/activate", token=token)
        except RuntimeError:
            pass
        raise


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
    from backend.provision.system_fields import reconcile_cfg_system_custom_fields

    try:
        service_cfg = _svcconfig.load_config(service_id)
    except Exception as e:
        raise RuntimeError(f"Service config for {service_id} not found: {e}")
    if service_cfg is None:
        raise RuntimeError(f"Service config for {service_id} is empty or None")

    # Handle CMCD request merging/removal
    cmcd_enabled_req = cfg.get("cmcd_enabled")
    cmcd_mode_req = cfg.get("cmcd_mode")
    cmcd_version_req = cfg.get("cmcd_version")
    old_cmcd = service_cfg.get("cmcd") or {}
    cmcd_was_enabled = bool(old_cmcd.get("enabled"))
    cmcd_changed = False

    if cmcd_enabled_req is not None:
        import datetime as _dt

        from backend.provision.cmcd_fields import merge_cmcd_custom_fields
        from backend.provision.cmcd_orchestrator import _remove_cmcd_custom_fields

        want_enabled = bool(cmcd_enabled_req)
        new_mode = cmcd_mode_req or old_cmcd.get("mode", "query_string")
        new_version = int(cmcd_version_req) if cmcd_version_req is not None else old_cmcd.get("version", 1)

        if want_enabled and not cmcd_was_enabled:
            service_cfg["cmcd"] = {
                "enabled": True,
                "mode": new_mode,
                "version": new_version,
                "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            }
            lf_cfg = service_cfg.setdefault("log_fields", {})
            lf_cfg["custom_fields"] = merge_cmcd_custom_fields(lf_cfg.get("custom_fields"))
            cmcd_changed = True
        elif not want_enabled and cmcd_was_enabled:
            service_cfg.pop("cmcd", None)
            _remove_cmcd_custom_fields(service_cfg)
            cmcd_changed = True
        elif want_enabled and cmcd_was_enabled:
            if new_mode != old_cmcd.get("mode") or new_version != old_cmcd.get("version"):
                service_cfg["cmcd"] = {
                    **old_cmcd,
                    "mode": new_mode,
                    "version": new_version,
                }
                cmcd_changed = True

    # Update state fields in nested provisioning block
    prov = service_cfg.setdefault("provisioning", {})
    if sample_rate is not None:
        prov["sample_rate"] = int(sample_rate)
    if edge_only is not None:
        prov["edge_only"] = bool(edge_only)
    if period is not None:
        service_cfg["log_period"] = int(period)
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
            existing_custom = (service_cfg.get("log_fields") or {}).get("custom_fields")
            if existing_custom:
                incoming_lf["custom_fields"] = list(existing_custom)
        service_cfg["log_fields"] = incoming_lf

    # Re-assert the system-managed custom fields against the FINAL feature
    # state (post-merge above, post-CMCD-request handling earlier). Keyed on
    # state rather than on a transition so a reconcile that changes nothing
    # about CMCD still converges, and a disable strips the fields.
    reconcile_cfg_system_custom_fields(service_cfg)
    if path is not None:
        service_cfg["fos_path"] = path

    # Save configuration
    _svcconfig.save_config(service_id, service_cfg)

    # Refresh account level rate-limiting best effort
    try:
        detected_rl = account_has_rate_limiting(token, service_id)
        prov = service_cfg.setdefault("provisioning", {})
        if detected_rl is not None and prov.get("rate_limiting") != detected_rl:
            prov["rate_limiting"] = detected_rl
            _svcconfig.save_config(service_id, service_cfg)
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

    import queue
    import threading

    events: queue.Queue = queue.Queue()

    # Define status callback wrapper to yield status updates to caller
    def status_callback(msg: str) -> None:
        import sys

        sys.stdout.write(f"  →  {msg}\n")
        sys.stdout.flush()
        events.put({"type": "status", "message": msg})

    from backend.provision.declarative.reconciler import reconcile_vcl_state

    yield {"type": "status", "message": f"🔍 Starting declarative reconciliation for service {service_id}..."}
    try:
        result_container = []
        error_container = []

        def worker():
            try:
                res = reconcile_vcl_state(service_id, token, dry_run=False, status_cb=status_callback)
                result_container.append(res)
            except Exception as e:
                error_container.append(e)
            finally:
                events.put(None)

        t = threading.Thread(target=worker)
        t.start()

        while True:
            try:
                evt = events.get(timeout=0.1)
                if evt is None:
                    break
                yield evt
            except queue.Empty:
                continue

        t.join()

        if error_container:
            raise error_container[0]

        result = result_container[0]
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

_CDN_SNIPPETS = [
    (_ICEBERG_METADATA_SNIPPET_NAME, "fetch", _ICEBERG_METADATA_SNIPPET, 10),
    (_CDN_RECV_SWR_SNIPPET_NAME, "recv", _CDN_RECV_SWR_SNIPPET, 10),
    (_CDN_FETCH_GENERATION_SNIPPET_NAME, "fetch", _CDN_FETCH_GENERATION_SNIPPET, 20),
    (_CDN_NO_CACHE_404_SNIPPET_NAME, "fetch", _CDN_NO_CACHE_404_SNIPPET, 30),
]
