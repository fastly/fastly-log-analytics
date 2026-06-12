"""VCL snippet generator for the session-scoring restart pattern.

Adapted from the canonical Fastly preflight pattern (fiddle 4b1a74ee).
Six snippets — recv / pass / fetch / miss / deliver / enforce — coordinate to:

  1. recv:    on first pass, route to the scorer Compute backend with
              X-Edge-Scoring-Pass=1, return(pass).
  2. pass:    inject the auth + service-id headers on bereq for the
              upcoming scorer sub-fetch (pass is the correct subroutine
              for bereq header mutations under return(pass)).
  3. fetch:   when the backend is the scorer, return(deliver) to skip
              cache + go straight to deliver with the scorer response.
  4. deliver: pass-1 captures all seven scorer values (score, l1, l2,
              compliance, reason, sid, enforce) + the rotated Set-Cookie
              into subfields of req.http.x-edge-score (single consolidated
              header — eight subfields total), unsets the resp.http
              .x-edge-* leaks, and issues a naked `restart`. pass-2 emits
              the rotated cookie via `add resp.http.Set-Cookie` (additive
              — preserves any origin cookies).
  5. miss:    unset bereq.http.x-edge-score + X-Edge-Scoring-Pass so
              neither leaks to the real origin on pass 2.
  6. enforce: on the post-scoring restart, error 429 when the scorer
              emitted X-Edge-Score-Enforce=1 (operator committed an
              enforce_threshold and the request's score met it).

**Storage strategy.** All seven scoring values (plus the rotated
Set-Cookie) are stored as SUBFIELDS of ``req.http.x-edge-score`` —
single consolidated header keeps the per-request header budget small.
Log format reads the subfields via ``req.http.x-edge-score:score`` etc.

**Why restart from vcl_deliver.** Empirically (v440), req.http
modifications made in vcl_fetch before `return(restart)` are invisible
to Fastly's log-format evaluator. Restarting from vcl_deliver after
writing the subfields is the working pattern.

Fail-open contract: any error reaching the scorer (5xx, timeout, DNS
failure) sets ``req.http.x-edge-score:score = "0"`` and
``req.http.x-edge-score:compliance = "unknown"`` so the request flows
normally to origin and the log line still has populated score fields
(vs. NULLs that look like a misconfiguration).
"""

from __future__ import annotations

# Backend name handling. Fastly's API creates a backend whose VCL-visible
# name is "F_" + the raw name you submitted. So:
#   - SCORING_BACKEND_API_NAME is what we POST to /backend  → "session_scorer"
#   - SCORING_BACKEND_VCL_NAME is what VCL sees             → "F_session_scorer"
SCORING_BACKEND_API_NAME = "session_scorer"
SCORING_BACKEND_VCL_NAME = f"F_{SCORING_BACKEND_API_NAME}"

# Snippet names. Stable string constants so disable_scoring can find and
# remove the exact snippets by name. Fastly only accepts
# [A-Za-z0-9_. -] in snippet names — no colons, slashes, or other
# punctuation.
SCORING_RECV_NAME = "Session Scoring - Recv"
SCORING_PASS_NAME = "Session Scoring - Pass"
SCORING_FETCH_NAME = "Session Scoring - Fetch"
SCORING_DELIVER_NAME = "Session Scoring - Deliver"
SCORING_MISS_NAME = "Session Scoring - Miss"
SCORING_ENFORCE_NAME = "Session Scoring - Enforce"

# Snippet priority — lower runs first. 100 is the "after everything
# else" slot used by most user snippets on this service.
SCORING_SNIPPET_PRIORITY = 100

# vcl_fetch needs a low priority specifically — when the backend is
# the scorer, we want `return(deliver)` to fire IMMEDIATELY, before
# any other fetch-stage snippet (group-L timing, custom origin field
# captures, etc.) gets a chance to run against the scorer's response.
# Priority 1 puts us first in the fetch subroutine.
SCORING_FETCH_PRIORITY = 1


# Default asset-extension regex: requests whose URL matches this regex
# bypass the scorer entirely. Static assets carry no session signal and
# routing them through Compute is wasted cost + capacity.
#
# This is the DEFAULT. Operators can override it per-service via the
# Session Scoring admin page; the operator-supplied value lives in the
# service config under ``scoring.exclude_url_regex`` and is interpolated
# into the recv snippet by ``recv_snippet`` below. An empty / unset
# override falls back to this default.
DEFAULT_ASSET_EXT_REGEX = (
    # Anchored at the start AND restricted to the path segment via
    # ``[^?#;]*`` (any non-``?``, ``#``, or ``;`` chars). Without the anchor + path-only
    # restriction, ``/api/login?file=.png`` would also match — the
    # extension test would see ``.png`` in the query string and skip
    # scoring entirely, letting an attacker bypass session scoring on
    # any dynamic endpoint by appending an asset extension to the
    # query string. The fix bounds the match to the URL path.
    r"^[^?#;]*"
    r"\.(aif|aiff|au|avi|bin|bmp|cab|carb|cct|cdf|class|css|dcr|doc|"
    r"dtd|exe|flv|gcf|gff|gif|grv|hdml|hqx|ico|ini|jpeg|jpg|js|mov|"
    r"mp3|mp4|nc|pct|pdf|png|ppc|pws|svg|swa|swf|txt|vbs|w32|wav|"
    r"wbmp|wml|wmlc|wmls|wmlsc|xsd|zip|webp|woff|woff2|ttf|bz2|gz|"
    r"tgz|tar|lzma|rar|war|bz|7z|ts|m3u8)($|\?|#)"
)

# Backwards-compat alias for tests / external callers that referenced
# the old name before the override path landed.
_ASSET_EXT_REGEX = DEFAULT_ASSET_EXT_REGEX


def resolve_exclude_url_regex(operator_override: str | None) -> str:
    """Pick between the operator's override and the built-in default.

    Empty / None / whitespace-only → default. The operator-facing API
    interprets the empty string as "I want the default" — same shape
    as Pydantic optional-field handling.
    """
    if operator_override is None:
        return DEFAULT_ASSET_EXT_REGEX
    cleaned = operator_override.strip()
    return cleaned or DEFAULT_ASSET_EXT_REGEX


# Default HTTP status code returned by the enforce snippet when the scorer
# flags a request. Operator-overridable via cfg.scoring.enforce_status_code;
# bake-into-VCL at deploy so each change does a snippet swap (see
# update_enforce_status_code orchestrator) rather than needing a
# ConfigStore-to-VCL binding for a value that changes rarely.
DEFAULT_ENFORCE_STATUS_CODE = 429

# Allowed range — anything outside 4xx/5xx makes no sense for "reject".
_ENFORCE_STATUS_CODE_MIN = 400
_ENFORCE_STATUS_CODE_MAX = 599


def enforce_reason_phrase(status_code: int) -> str:
    """HTTP reason phrase for the enforce snippet's synthetic body.

    Delegates to Python's ``http.HTTPStatus`` so any IANA-registered code
    yields its canonical phrase (403 → "Forbidden", 451 → "Unavailable
    For Legal Reasons", 511 → "Network Authentication Required", …).
    Non-standard codes the operator might pick (419, 444, 530, 599) fall
    back to ``"Blocked"`` — keeps the synthetic body meaningful even when
    the stdlib map doesn't know the code."""
    import http

    try:
        return http.HTTPStatus(status_code).phrase
    except ValueError:
        return "Blocked"


def resolve_enforce_status_code(operator_override: int | None) -> int:
    """Pick the effective enforce status code. None / out-of-range → default.

    The PUT endpoint validates the operator's input before persistence,
    so out-of-range here means a stale or corrupted cfg — fall back to
    the safe default rather than baking a nonsensical code into VCL."""
    if operator_override is None:
        return DEFAULT_ENFORCE_STATUS_CODE
    try:
        code = int(operator_override)
    except (TypeError, ValueError):
        return DEFAULT_ENFORCE_STATUS_CODE
    if not (_ENFORCE_STATUS_CODE_MIN <= code <= _ENFORCE_STATUS_CODE_MAX):
        return DEFAULT_ENFORCE_STATUS_CODE
    return code


def recv_snippet(
    logging_service_id: str,
    request_secret: str,
    *,
    exclude_url_regex: str | None = None,
) -> str:
    """vcl_recv snippet: at the EDGE on the first pass (no shield hop yet
    AND req.restarts == 0 AND scoring-pass marker not set AND URL doesn't
    match the exclusion regex), route to the scorer Compute backend with
    X-Edge-Scoring-Pass=1, `return(pass)`. After the scoring restart
    completes (req.restarts == 1) and the score was captured, re-enable
    shielding for the real-origin pass so the cached object can be
    served from the shield POP normally.

    ``exclude_url_regex`` is the operator-supplied regex of URLs to
    SKIP from scoring. None or "" falls back to ``DEFAULT_ASSET_EXT_REGEX``.
    The caller (orchestrator) is responsible for having validated the
    regex via backend.utils.vcl_validator BEFORE getting here — this
    function trusts its input and string-substitutes verbatim into the
    VCL boolean expression.

    ``request_secret`` is also baked into the edge/shield boundary check
    (``req.http.X-Edge-Shield-Auth != "{request_secret}"``). The original
    boundary used ``fastly.ff.visits_this_service == 0``, which an
    attacker could flip by setting their own ``Fastly-FF`` header
    (Fastly's edge propagates the value verbatim to the next hop). The
    secret-comparison form fails closed: only the edge's own pass/miss
    subroutines (which know the secret because it's literally baked into
    their VCL bodies) can set the header to a value that satisfies the
    check.

    Note: `logging_service_id` is kept as an argument for symmetry with
    peer snippet generators."""
    _ = logging_service_id
    effective_regex = resolve_exclude_url_regex(exclude_url_regex)
    return f"""# Session Scoring: client-edge header scrub (anti-spoofing).
# Edge-only — see X-Edge-Shield-Auth note below — so any client-supplied
# X-Edge-* gets stripped before it can be forged into a clean score.
# Also strip the X-Edge-Shield-Auth header itself so a client cannot
# pre-set it and skip our edge-only protections.
if (req.restarts == 0 && req.http.X-Edge-Shield-Auth != "{request_secret}") {{
  unset req.http.X-Edge-Shield-Auth;
  unset req.http.X-Edge-Scoring-Pass;
  unset req.http.X-Edge-Score;
  unset req.http.X-Edge-Score-Reason;
  unset req.http.X-Edge-Score-Enforce;
  unset req.http.X-Edge-Sid;
  unset req.http.X-Edge-Score-Set-Cookie;
}}

# Session Scoring: route the first-pass dynamic request to the scorer.
# Edge-only — the pass/miss snippets set X-Edge-Shield-Auth on the
# bereq going to the shield, so the shield's vcl_recv reads back a
# matching secret and skips this block. An attacker who tries to spoof
# the boundary by sending their own Fastly-FF header cannot satisfy the
# secret comparison, so the edge-only logic still runs on their hop and
# they get scored / scrubbed normally.
#
# DDoS bypass (fastly.ddos_detected): when Fastly's L7 DDoS detection
# flags this request, do NOT route to Compute. Two reasons:
#   1. Cost ceiling — under attack, Compute invocations scale linearly
#      with attack volume. Skipping flagged requests caps the blast
#      radius while NGWAF / Fastly's mitigation handles the actual block.
#   2. Signal quality — the scorer's L2 transition matrix learns from
#      benign traffic shapes; feeding attack traffic in pollutes the
#      matrix even though those scores wouldn't be acted on.
# See: https://www.fastly.com/documentation/reference/vcl/variables/miscellaneous/fastly-ddos-detected/
if (req.http.X-Edge-Shield-Auth != "{request_secret}" && req.restarts == 0 && req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected && std.tolower(req.url) !~ "{effective_regex}") {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  set req.http.X-Edge-Scoring-Pass = "1";
  # PASS — skip cache for the scoring sub-fetch. On the post-restart
  # pass the scoring snippet doesn't re-fire because X-Edge-Scoring-Pass
  # got unset in pass-1 deliver and req.restarts is now 1.
  return(pass);
}}

# Post-scoring restart: we captured the score in pass-1 deliver and the
# request flow is now headed for the real origin. Without this block,
# the previous `return(pass)` would have permanently disabled shielding
# for this request — re-enable it so the real-origin fetch can land on
# the shield POP normally. `var.fastly_req_do_shield` is the magic
# variable Fastly's auto-generated main VCL reads to decide whether to
# shield the request.
if (req.restarts == 1 && req.http.x-edge-score) {{
  set var.fastly_req_do_shield = true;
}}"""


def pass_snippet(logging_service_id: str, request_secret: str) -> str:
    """vcl_pass snippet: when this is the scoring sub-fetch (backend ==
    scorer), inject the auth + service-id headers on bereq for the
    upcoming sub-fetch. Also unset bereq.http.x-edge-score so any
    attacker-supplied inbound x-edge-score doesn't get echoed into the
    scorer's view of the request.

    Also stamps ``bereq.http.X-Edge-Shield-Auth = "{request_secret}"``
    on every pass — this is what the shield POP's vcl_recv reads back to
    decide "skip edge-only blocks because this hop already ran them on
    the edge". An attacker who tries to spoof Fastly-FF cannot satisfy
    the shield-auth comparison because the secret is only ever set by
    pass_snippet / miss_snippet (compiled into our VCL, never sent to
    clients)."""
    return f"""# Session Scoring: inject auth + service-id on the scorer sub-fetch.
# vcl_pass is the right subroutine for bereq mutations when recv used
# return(pass).
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  set bereq.http.X-Edge-Service-Id = "{logging_service_id}";
  # Shared-secret header — the scorer compares this to the
  # request_secret ConfigStore entry and 401s on mismatch. Embedded
  # literally in VCL which is compiled and never sent to clients.
  set bereq.http.X-Edge-Scorer-Auth = "{request_secret}";
  # X-Edge-Scoring-Pass is an internal marker; the scorer doesn't need
  # to see it and we don't want it polluting any downstream telemetry.
  unset bereq.http.X-Edge-Scoring-Pass;
}}
# Strip any inbound x-edge-score header an attacker may have set; the
# real one is built by us in vcl_deliver after the scorer responds.
unset bereq.http.x-edge-score;
# Shield-auth marker — the shield POP's vcl_recv reads this back via
# req.http.X-Edge-Shield-Auth and skips the edge-only branches when it
# matches. Unspoofable from outside because the secret is baked into
# the compiled VCL and never returned to clients.
set bereq.http.X-Edge-Shield-Auth = "{request_secret}";"""


def fetch_snippet() -> str:
    """vcl_fetch snippet: when the backend is the scorer, return(deliver)
    so the response goes straight to deliver without any cache-related
    handling. (return(pass) in recv already prevents caching, but
    return(deliver) here is the canonical preflight-pattern shape and
    avoids any weird interactions with beresp's TTL.)"""
    return f"""# Session Scoring: skip cache handling for the scorer sub-fetch.
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  return(deliver);
}}"""


def deliver_snippet(request_secret: str) -> str:
    """vcl_deliver snippet — the heart of the pattern.

    PASS 1 (X-Edge-Scoring-Pass == "1"): scorer's response is in
    resp.http.x-edge-*. Stash all seven scorer values (score, l1, l2,
    compliance, reason, sid, enforce) into req.http.x-edge-score
    subfields (single consolidated header), stash Set-Cookie into a
    :set-cookie subfield (eight subfields total), scrub the
    resp.http.x-edge-* headers (anti-leak), then naked `restart`.

    PASS 2 (X-Edge-Scoring-Pass already gone): the stashed cookie gets
    emitted via `add resp.http.Set-Cookie` (additive — preserves any
    Set-Cookie the real origin set).

    The subfield writes in pass-1 deliver propagate to vcl_log via the
    req.http persistence across restart. The log format reads
    req.http.x-edge-score:score etc."""
    return f"""# Session Scoring: pass-1 stash + naked restart; pass-2 emit cookie.

# ── PASS 1: capture scorer response into req.http.x-edge-score subfields ──
if (req.http.X-Edge-Scoring-Pass == "1") {{
  unset req.http.X-Edge-Scoring-Pass;
  if (resp.status == 200) {{
    set req.http.x-edge-score:score = resp.http.x-edge-score;
    set req.http.x-edge-score:l1 = resp.http.x-edge-score-l1;
    set req.http.x-edge-score:l2 = resp.http.x-edge-score-l2;
    set req.http.x-edge-score:compliance = resp.http.X-Edge-Cookie-Compliance;
    set req.http.x-edge-score:reason = resp.http.x-edge-score-reason;
    # Hex-encoded 6-byte session id. Used by the admin labeling UI to
    # target individual sessions; the scorer issues a fresh sid when
    # the inbound cookie is missing/tampered.
    set req.http.x-edge-score:sid = resp.http.x-edge-sid;
    # Enforcement signal — set by the Rust scorer when the operator
    # has committed enforce_threshold to the scoring_config ConfigStore
    # AND the request's score met it. Captured here so the recv-
    # restart-2 Enforce snippet can read it via subfield.
    set req.http.x-edge-score:enforce = resp.http.x-edge-score-enforce;
  }} else {{
    # Scorer returned non-200 — fail open. No cookie to rotate.
    set req.http.x-edge-score:score = "0";
    set req.http.x-edge-score:l1 = "0";
    set req.http.x-edge-score:l2 = "0";
    set req.http.x-edge-score:compliance = "unknown";
    set req.http.x-edge-score:reason = "compute-unavailable";
  }}
  # Stash the rotated cookie as a subfield too; pass-2 reads it back
  # and emits via add resp.http.Set-Cookie.
  set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;
  # Anti-leak: strip the scorer's resp.http.x-edge-* headers so they
  # don't reach the client even if the restart path were to short-
  # circuit somehow.
  unset resp.http.x-edge-score;
  unset resp.http.x-edge-score-l1;
  unset resp.http.x-edge-score-l2;
  unset resp.http.x-edge-score-reason;
  unset resp.http.x-edge-sid;
  unset resp.http.X-Edge-Cookie-Compliance;
  unset resp.http.X-Edge-Matrix-Version;
  unset resp.http.x-edge-score-enforce;
  restart;
}}

# ── PASS 2: real origin response — emit the rotated cookie additively ──
# Only emit at the EDGE. We detect "this is the edge hop" via the
# absence of the shield-auth secret on req.http.X-Edge-Shield-Auth;
# the shield POP receives that header from us (set in pass/miss), so
# the shield sees the match and skips this block. A spoofed
# Fastly-FF header cannot fake the secret, so attacker-induced
# duplicate Set-Cookie emission is no longer possible.
if (req.http.X-Edge-Shield-Auth != "{request_secret}" && req.http.x-edge-score:set-cookie != "") {{
  add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;
}}"""


def enforce_snippet(request_secret: str, status_code: int = DEFAULT_ENFORCE_STATUS_CODE) -> str:
    """vcl_recv snippet that errors ``status_code`` when the scorer flagged the
    request as over-threshold.

    Fires on req.restarts == 1 (after the scoring sub-fetch + restart)
    when the deliver pass-1 snippet captured ``X-Edge-Score-Enforce: 1``
    from the scorer's response. The scorer only emits that header when
    the operator has committed an enforce_threshold value via the
    admin UI AND the request's score met it.

    Edge-only — the shield-auth secret comparison replaces the original
    ``fastly.ff.visits_this_service == 0`` check, which an attacker
    could flip by sending their own ``Fastly-FF`` header.
    ``error <status_code>`` instead of a `synth` keeps the door open
    for a custom vcl_error page later.

    ``status_code`` defaults to 429 (Too Many Requests). Operators can
    override via cfg.scoring.enforce_status_code; valid range 400-599.
    The reason phrase is auto-mapped via ``enforce_reason_phrase``."""
    code = resolve_enforce_status_code(status_code)
    reason = enforce_reason_phrase(code)
    return (
        f"# Session Scoring: enforce committed threshold by erroring flagged requests.\n"
        f"# Status code ({code} {reason}) is operator-configurable via\n"
        f"# cfg.scoring.enforce_status_code; the update_enforce_status_code\n"
        f"# orchestrator swaps this snippet on change. Default 429.\n"
        f"# Fires only on the post-scoring restart (req.restarts == 1) when the\n"
        f"# deliver pass-1 captured X-Edge-Score-Enforce=1 from the scorer.\n"
        f"# Edge-only (unspoofable shield-auth comparison) so shield hops don't double-enforce.\n"
        f'if (req.http.X-Edge-Shield-Auth != "{request_secret}" && req.restarts == 1 && req.http.x-edge-score:enforce == "1") {{\n'
        f'  error {code} "{reason}";\n'
        f"}}"
    )


def miss_snippet(request_secret: str) -> str:
    """vcl_miss snippet: defensive unsets. Strip inbound x-edge-score
    (attacker could try to forge it) and X-Edge-Scoring-Pass (don't
    leak the internal marker to the real origin on pass-2 fetch).

    Also stamps ``bereq.http.X-Edge-Shield-Auth = "{request_secret}"``
    on every miss-driven backend fetch so the shield POP's vcl_recv
    sees the secret and skips edge-only blocks. Without this, only
    pass-driven fetches set the marker, and a cacheable miss flow
    would leave the marker unset on the shield hop — re-triggering
    the edge-only branches and double-scoring."""
    return f"""# Session Scoring: strip internal scoring headers before forwarding to
# the real origin. x-edge-score could be attacker-supplied; the
# X-Edge-Scoring-Pass marker is internal-only.
unset bereq.http.x-edge-score;
unset bereq.http.X-Edge-Scoring-Pass;
# Shield-auth marker — match pass_snippet so the shield POP recognises
# this hop as edge-originated and skips re-running edge-only blocks.
set bereq.http.X-Edge-Shield-Auth = "{request_secret}";"""


def generate_scoring_vcl(
    logging_service_id: str,
    request_secret: str,
    *,
    exclude_url_regex: str | None = None,
    enforce_status_code: int | None = None,
) -> dict[str, str]:
    """Return a {snippet_name: vcl_body} dict for all six snippets.

    Caller passes each (name, body) pair to ``ensure_vcl_snippet``
    individually so the existing idempotent diff-and-update path
    handles re-deploys cleanly.

    ``request_secret`` is the shared secret VCL embeds in the
    X-Edge-Scorer-Auth header so the scoring Compute service can
    reject requests that didn't originate from this VCL service.

    ``exclude_url_regex`` is the operator's per-service override of the
    URL-exclusion regex used by recv_snippet. None / "" → default.
    Pre-validated by backend.utils.vcl_validator at the API layer
    before reaching this function.

    ``enforce_status_code`` is the operator's per-service override of the
    HTTP status code the enforce snippet returns when the scorer flags
    a request. None / out-of-range → default (429).
    """
    return {
        SCORING_RECV_NAME: recv_snippet(logging_service_id, request_secret, exclude_url_regex=exclude_url_regex),
        SCORING_PASS_NAME: pass_snippet(logging_service_id, request_secret),
        SCORING_FETCH_NAME: fetch_snippet(),
        SCORING_DELIVER_NAME: deliver_snippet(request_secret),
        SCORING_MISS_NAME: miss_snippet(request_secret),
        SCORING_ENFORCE_NAME: enforce_snippet(request_secret, resolve_enforce_status_code(enforce_status_code)),
    }


def scoring_snippet_names() -> list[str]:
    """Names of the snippets we install. Used by disable_scoring to find
    and remove them by name from the cloned VCL version."""
    return [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]
