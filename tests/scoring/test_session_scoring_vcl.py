"""Tests for backend.provision.session_scoring_vcl — Six snippets
(recv/pass/fetch/deliver/miss/enforce) that wire the customer's VCL
service to the scoring Compute backend via the canonical Fastly
preflight pattern (fiddle 4b1a74ee).

These tests pin the exact subroutine assignments and key VCL idioms so
any unintentional drift surfaces immediately."""

from __future__ import annotations

import pytest

from backend.provision.session_scoring_vcl import (
    SCORING_BACKEND_API_NAME,
    SCORING_BACKEND_VCL_NAME,
    SCORING_DELIVER_NAME,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
    SCORING_SNIPPET_PRIORITY,
    deliver_snippet,
    enforce_snippet,
    fetch_snippet,
    generate_scoring_vcl,
    miss_snippet,
    pass_snippet,
    recv_snippet,
    scoring_snippet_names,
)

# Note on secrets: ONLY pass_snippet embeds the shared request_secret now
# (X-Edge-Scorer-Auth — the genuine scorer-backend auth; the scorer 401s on
# mismatch), so secret-bearing tests pass it as a literal to pass_snippet /
# generate_scoring_vcl. Edge detection across ALL snippets is the unforgeable
# fastly.ff.visits_this_service == 0 check, NOT a shield-auth secret comparison.

# ── Snippet-name constants are stable ────────────────────────────────────────


def test_snippet_names_match_constants():
    """disable_scoring searches by these exact names — if they drift the
    teardown won't find anything to remove."""
    assert scoring_snippet_names() == [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]


def test_priority_constant():
    assert SCORING_SNIPPET_PRIORITY == 100


def test_fetch_priority_runs_first():
    """SCORING_FETCH gets a much lower priority so its `return(deliver)`
    fires before any other fetch-stage snippet (group-L timing capture,
    custom origin field captures, etc.) gets a chance to run against
    the scorer's response."""
    from backend.provision.session_scoring_vcl import SCORING_FETCH_PRIORITY

    assert SCORING_FETCH_PRIORITY < SCORING_SNIPPET_PRIORITY
    assert SCORING_FETCH_PRIORITY <= 1


def test_backend_name_constant():
    """Fastly's API auto-prefixes user backend names with "F_" inside VCL,
    so the API-side name ("session_scorer") maps to the VCL-side name
    ("F_session_scorer"). Hard-coding both prevents silent drift."""
    assert SCORING_BACKEND_API_NAME == "session_scorer"
    assert SCORING_BACKEND_VCL_NAME == "F_session_scorer"


# ── recv snippet ─────────────────────────────────────────────────────────────


def test_recv_routes_first_pass_to_scoring_backend():
    vcl = recv_snippet("MyServiceId")
    assert f"set req.backend = {SCORING_BACKEND_VCL_NAME};" in vcl
    # First-pass guard: the unforgeable fastly.ff.visits_this_service == 0 check
    # is true only at the true edge (the first Fastly POP), and a client cannot
    # forge it. req.restarts == 0 prevents a re-fire after our own deliver-side
    # restart; X-Edge-Scoring-Pass != "1" is belt-and-suspenders for unrelated
    # VCL restarts.
    assert "fastly.ff.visits_this_service == 0" in vcl
    assert "req.restarts == 0" in vcl
    assert 'req.http.X-Edge-Scoring-Pass != "1"' in vcl
    # The retired shield-auth edge-detection mechanism must NOT appear.
    assert "X-Edge-Shield-Auth" not in vcl


def test_recv_snippet_unsets_client_supplied_headers():
    """Defense-in-depth: at the client-edge boundary (the unforgeable
    fastly.ff.visits_this_service == 0 first edge pass AND req.restarts == 0),
    strip every X-Edge-* header a client could have set. Without this, an
    attacker could forge:
      - X-Edge-Scoring-Pass=1   → bypass scoring entirely
      - X-Edge-Score=0          → forge a clean score that subfields propagate
      - X-Edge-Score-Enforce=0  → suppress enforcement of a real-score block
      - X-Edge-Sid=...          → impersonate another session
      - X-Edge-Score-Reason=... → poison reason-attribution telemetry
      - X-Edge-Score-Set-Cookie=... → smuggle Set-Cookie into the response

    The unsets MUST be gated by the client-edge boundary
    (``req.restarts == 0 && fastly.ff.visits_this_service == 0``). Shield hops
    must NOT scrub (the edge node legitimately set those headers on pass-1
    deliver before the restart — at a shield POP visits_this_service > 0), and
    post-scoring restarts (req.restarts == 1) must NOT scrub either (deliver
    wrote them deliberately for the enforce snippet + log line).
    """
    vcl = recv_snippet("Svc")
    # Each of the client-controllable scoring headers must be unset.
    # NB: X-Edge-Score (no subfield syntax) — the bare header, not the
    # subfield-bearing one used internally. X-Edge-Shield-Auth is retired and
    # no longer scrubbed (it is not used for edge detection or anything else).
    required_unsets = (
        "unset req.http.X-Edge-Scoring-Pass",
        "unset req.http.X-Edge-Score",
        "unset req.http.X-Edge-Sid",
        "unset req.http.X-Edge-Score-Enforce",
        "unset req.http.X-Edge-Score-Reason",
        "unset req.http.X-Edge-Score-Set-Cookie",
        # L2 skip-gram anchor: a client-supplied SEEN high-probability anchor
        # would raise the L2 transition prob and depress the anomaly score
        # (audit F009 evasion class). Must be scrubbed at the edge boundary.
        "unset req.http.X-Edge-Prev-Anchor",
    )
    for unset in required_unsets:
        assert unset in vcl, f"missing client-edge scrub: {unset!r}"
    # The retired shield-auth header must not be scrubbed (or referenced).
    assert "X-Edge-Shield-Auth" not in vcl
    # The unsets must live inside the client-edge boundary if-block —
    # i.e. each unset's position must come AFTER the boundary guard.
    boundary_idx = vcl.find("req.restarts == 0 && fastly.ff.visits_this_service == 0")
    assert boundary_idx != -1, "client-edge boundary guard missing from recv"
    for unset in required_unsets:
        assert vcl.find(unset) > boundary_idx, (
            f"{unset!r} appears BEFORE the client-edge boundary guard; "
            "an unguarded unset would also strip headers we set "
            "deliberately on the post-scoring restart path."
        )


def test_recv_re_enables_shielding_post_scoring_restart():
    """After the scoring restart (req.restarts == 1) and assuming the
    score was captured (req.http.x-edge-score header is set with
    subfields), set the fastly_req_do_shield variable so the real-origin
    fetch can land on the shield POP normally. Without this, the prior
    `return(pass)` would have permanently disabled shielding for this
    request."""
    vcl = recv_snippet("Svc")
    assert "req.restarts == 1 && req.http.x-edge-score" in vcl
    assert "set var.fastly_req_do_shield = true;" in vcl


def test_recv_stamps_rtt_start_before_pass():
    """recv stamps time.elapsed.usec into x-edge-score-t0 just before the
    scorer sub-fetch so pass-1 deliver can diff it into edge_score_rtt_us.
    Must be set before return(pass)."""
    vcl = recv_snippet("Svc")
    assert "set req.http.x-edge-score-t0 = time.elapsed.usec;" in vcl
    assert vcl.index("set req.http.x-edge-score-t0") < vcl.index("return(pass);")


def test_recv_sets_scoring_pass_marker_then_pass():
    """The X-Edge-Scoring-Pass marker is the discriminator pass/fetch/
    deliver snippets all read. recv sets it just before return(pass)."""
    vcl = recv_snippet("Svc")
    assert 'set req.http.X-Edge-Scoring-Pass = "1";' in vcl
    assert "return(pass);" in vcl


def test_recv_does_not_set_auth_or_service_id_headers():
    """Auth + service-id headers move to vcl_pass (the canonical place
    for bereq mutations when recv used return(pass))."""
    vcl = recv_snippet("MyServiceId")
    # These are set on bereq in pass, not on req in recv
    assert "X-Edge-Scorer-Auth" not in vcl
    assert "X-Edge-Service-Id" not in vcl


def test_recv_skips_scoring_when_ddos_detected():
    """When Fastly's L7 DDoS detection flags the request, do NOT route to
    Compute. Two reasons: (a) cost ceiling — Compute invocations scale
    linearly with attack volume, so skipping flagged requests caps blast
    radius while NGWAF / Fastly mitigation handles the actual block; and
    (b) signal quality — the L2 matrix learns from benign traffic shapes
    so feeding attack traffic would pollute the matrix even though the
    scores wouldn't be acted on. The gate is `!fastly.ddos_detected`
    inside the existing first-pass `if`, so all the other gates still
    fire too (edge-only, restarts==0, etc.)."""
    vcl = recv_snippet("Svc")
    assert "!fastly.ddos_detected" in vcl
    # Must appear inside the SAME if block as the other recv gates — not
    # in a separate clause that could route some attack requests to
    # Compute when the other gates also pass. The edge-boundary gate is
    # the unforgeable fastly.ff.visits_this_service == 0 check.
    assert "fastly.ff.visits_this_service == 0" in vcl
    # Sanity: the ddos check sits between the X-Edge-Scoring-Pass guard
    # and the asset-extension guard, so the conjunction is intact.
    pass_marker_idx = vcl.find('req.http.X-Edge-Scoring-Pass != "1"')
    ddos_idx = vcl.find("!fastly.ddos_detected")
    asset_idx = vcl.find("std.tolower(req.url) !~")
    assert pass_marker_idx < ddos_idx < asset_idx, "ddos gate must be in the recv first-pass conjunction"


def test_recv_does_not_set_dead_prev_route_header():
    """The old `set req.http.X-Edge-Prev-Route = req.http.X-Edge-Last-Route;`
    line was broken — req.http doesn't persist between client requests, so
    X-Edge-Last-Route was always empty. Compute scorer now reads
    prev_route from the encrypted cookie state instead."""
    vcl = recv_snippet("Svc")
    assert "set req.http.X-Edge-Prev-Route" not in vcl
    assert "set req.http.X-Edge-Last-Route" not in vcl


def test_recv_sets_ngwaf_skip_inspection_on_scoring_route():
    """The scoring sub-fetch must skip NGWAF inspection. NGWAF's edge_security
    (vcl_miss/pass) 406s attack URLs, and because that runs on the scoring
    sub-fetch it turns every WAF block into a scorer fail-open
    (compute-unavailable-406) even though the scorer strips the query string
    and never sees the payload. Setting x-sigsci-skip-inspection-once = "true"
    when we route to the scorer makes NGWAF skip THAT hop only.

    Must be set inside the first-pass route block: after `set req.backend`
    (so it only applies when we actually route to Compute) and before
    `return(pass)` (so it lands on the sub-fetch's bereq)."""
    vcl = recv_snippet("Svc")
    assert 'set req.http.x-sigsci-skip-inspection-once = "true";' in vcl
    backend_idx = vcl.find(f"set req.backend = {SCORING_BACKEND_VCL_NAME};")
    skip_idx = vcl.find('set req.http.x-sigsci-skip-inspection-once = "true";')
    pass_idx = vcl.find("return(pass);")
    assert backend_idx != -1 and skip_idx != -1 and pass_idx != -1
    assert backend_idx < skip_idx < pass_idx, (
        "x-sigsci-skip-inspection-once must be set inside the scoring route "
        "block: after `set req.backend` and before `return(pass)`"
    )


def test_recv_strips_client_supplied_skip_inspection_header():
    """x-sigsci-skip-inspection-once is a REQUEST header — a client who sets
    it could skip the WAF entirely. Strip any client-supplied copy in the
    client-edge scrub block (gated by the unforgeable edge boundary,
    restarts==0) so only our own scoring-route set survives."""
    vcl = recv_snippet("Svc")
    assert "unset req.http.x-sigsci-skip-inspection-once;" in vcl
    # Must sit after the client-edge boundary guard (i.e. inside the
    # client-edge scrub), like the other anti-spoofing unsets.
    boundary_idx = vcl.find("req.restarts == 0 && fastly.ff.visits_this_service == 0")
    assert boundary_idx != -1
    assert vcl.find("unset req.http.x-sigsci-skip-inspection-once;") > boundary_idx


def test_recv_unsets_skip_inspection_on_real_origin_restart():
    """req.http persists across the restart and edge_security unsets only its
    bereq copy, so the value we set on the scoring sub-fetch would otherwise
    leak into the real-origin pass and skip the WAF on attack traffic. The
    restart block (req.restarts == 1) must unset it so the real origin is
    always inspected. There are TWO unsets total: one in the client-edge
    scrub (restarts==0) and one on the restart path (restarts==1)."""
    vcl = recv_snippet("Svc")
    assert vcl.count("unset req.http.x-sigsci-skip-inspection-once;") == 2
    restart_idx = vcl.find("req.restarts == 1 && req.http.x-edge-score")
    assert restart_idx != -1
    # The second unset lives in the restart block (after the restart guard).
    assert vcl.find("unset req.http.x-sigsci-skip-inspection-once;", restart_idx) > restart_idx


# ── pass snippet ─────────────────────────────────────────────────────────────


def test_pass_injects_auth_and_service_id_on_scoring_backend():
    """When the upcoming sub-fetch is the scorer, set the auth + service-
    id headers on bereq. These were in recv (on req) before — moved to
    pass per the canonical preflight pattern (fiddle 4b1a74ee)."""
    vcl = pass_snippet("MyServiceId", "deadbeef1234567890")
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in vcl
    assert 'set bereq.http.X-Edge-Service-Id = "MyServiceId";' in vcl
    assert 'set bereq.http.X-Edge-Scorer-Auth = "deadbeef1234567890";' in vcl


def test_pass_unsets_internal_scoring_pass_header_on_bereq():
    """The X-Edge-Scoring-Pass marker is internal to our VCL — don't
    leak it to the scorer (or any backend) on the sub-fetch."""
    vcl = pass_snippet("Svc", "secret")
    assert "unset bereq.http.X-Edge-Scoring-Pass;" in vcl


def test_pass_strips_inbound_x_edge_score_anti_poisoning():
    """An attacker could try to POST/GET with an x-edge-score header
    they made up. Stripping it on bereq ensures the scorer (or any
    downstream backend) never sees client-supplied score data."""
    vcl = pass_snippet("Svc", "secret")
    assert "unset bereq.http.x-edge-score;" in vcl


def test_pass_different_secret_produces_different_vcl():
    """ensure_vcl_snippet's idempotency-by-diff must actually update
    when the secret changes."""
    a = pass_snippet("Svc", "secret_a")
    b = pass_snippet("Svc", "secret_b")
    assert a != b
    assert "secret_a" in a and "secret_a" not in b
    assert "secret_b" in b and "secret_b" not in a


# ── fetch snippet ────────────────────────────────────────────────────────────


def test_fetch_returns_deliver_for_scoring_backend():
    """When the backend is the scorer, return(deliver) so the response
    goes straight to deliver without any cache-related handling. This
    is the canonical preflight-pattern shape."""
    vcl = fetch_snippet()
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in vcl
    assert "return(deliver);" in vcl


# ── deliver snippet — the heart of the pattern ───────────────────────────────


def test_deliver_pass_1_captures_six_subfields_on_x_edge_score():
    """Pass 1: capture the scorer's resp.http.x-edge-* headers into
    SUBFIELDS of a single consolidated header `req.http.x-edge-score`.
    Short subfield names (score/l1/l2/compliance/reason/sid) keep the
    per-request header budget small."""
    vcl = deliver_snippet()
    for sub, src in (
        ("score", "x-edge-score"),
        ("l1", "x-edge-score-l1"),
        ("l2", "x-edge-score-l2"),
        ("compliance", "X-Edge-Cookie-Compliance"),
        ("reason", "x-edge-score-reason"),
        ("sid", "x-edge-sid"),
    ):
        assert f"set req.http.x-edge-score:{sub} = resp.http.{src};" in vcl


def test_deliver_fail_open_on_non_200_sets_zeros():
    """Any non-200 from the scorer (5xx/timeout) → score=0, compliance=
    unknown, reason=compute-unavailable in the subfields. Real request
    still serves; log line gets populated zeros, not NULLs."""
    vcl = deliver_snippet()
    assert "} else {" in vcl
    assert 'set req.http.x-edge-score:score = "0";' in vcl
    assert 'set req.http.x-edge-score:compliance = "unknown";' in vcl
    assert 'set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;' in vcl


def test_deliver_pass_1_only_fires_under_scoring_pass_marker():
    """X-Edge-Scoring-Pass discriminates pass-1 (scoring sub-fetch)
    from pass-2 (real origin). Pass-1 unsets the marker before issuing
    the naked restart, so pass-2 sees it gone."""
    vcl = deliver_snippet()
    assert 'if (req.http.X-Edge-Scoring-Pass == "1")' in vcl
    assert "unset req.http.X-Edge-Scoring-Pass;" in vcl


def test_deliver_pass_1_stashes_cookie_as_subfield():
    """The rotated cookie from the scorer gets stashed in a subfield
    too so it can be re-emitted in pass-2 with `add resp.http.Set-Cookie`."""
    vcl = deliver_snippet()
    assert "set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;" in vcl


def test_deliver_uses_naked_restart_not_return_restart():
    """The canonical preflight pattern uses a bare `restart;` from
    vcl_deliver (NOT `return(restart);`). The two have the same effect
    in this position, but the bare form matches the documented Fastly
    example."""
    vcl = deliver_snippet()
    assert "\n  restart;\n" in vcl
    assert "return(restart)" not in vcl


def test_deliver_pass_1_strips_scorer_response_headers():
    """The scorer's resp.http.x-edge-* headers must NOT reach the
    client. Even though the restart short-circuits to a different
    response, defense-in-depth says unset them in case routing changes."""
    vcl = deliver_snippet()
    for header in (
        "x-edge-score",
        "x-edge-score-l1",
        "x-edge-score-l2",
        "x-edge-score-reason",
        "x-edge-sid",
        "X-Edge-Cookie-Compliance",
        "X-Edge-Matrix-Version",
    ):
        assert f"unset resp.http.{header};" in vcl


def test_deliver_pass_2_emits_cookie_additively_at_edge_only():
    """Pass 2 (real origin response): `add resp.http.Set-Cookie` (not
    `set`) so any origin-issued Set-Cookie survives. Gated on the
    unforgeable fastly.ff.visits_this_service == 0 edge check so only the edge
    node emits — shield nodes (visits_this_service > 0) would otherwise produce
    a duplicate Set-Cookie when the request hops shield → edge."""
    vcl = deliver_snippet()
    assert 'if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {' in vcl
    assert "X-Edge-Shield-Auth" not in vcl
    assert "add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;" in vcl
    # Specifically NOT `set` (which would overwrite origin's cookie).
    assert "set resp.http.Set-Cookie =" not in vcl


def test_deliver_computes_rtt_for_both_branches():
    """Edge round-trip (edge_score_rtt_us) is computed from the recv
    x-edge-score-t0 stamp via the std.atoi(time.elapsed.usec) diff idiom,
    OUTSIDE the resp.status branch so fail-open/timeout rows also record
    it (they sit ≈the timeout budget)."""
    vcl = deliver_snippet()
    assert "declare local var.rtt INTEGER;" in vcl
    assert "set var.rtt = std.atoi(time.elapsed.usec);" in vcl
    assert "set var.rtt -= std.atoi(req.http.x-edge-score-t0);" in vcl
    # Stringified — INTEGER var → subfield lands empty without the "" + coercion.
    assert 'set req.http.x-edge-score:rtt = "" + var.rtt;' in vcl
    # Computed before the success/fail-open split so both branches inherit it.
    assert vcl.index("set req.http.x-edge-score:rtt") < vcl.index("if (resp.status == 200)")


def test_deliver_captures_exec_subfield_and_strips_header():
    """Scorer-reported Wasm exec time is captured into the exec subfield on
    200, and the source response header is unset in the anti-leak list."""
    vcl = deliver_snippet()
    assert "set req.http.x-edge-score:exec = resp.http.x-edge-score-exec-us;" in vcl
    assert "unset resp.http.x-edge-score-exec-us;" in vcl


# ── miss snippet ─────────────────────────────────────────────────────────────


def test_miss_unsets_inbound_score_header_anti_poisoning():
    """Defense in depth: when forwarding to the real origin on pass-2
    miss, strip any x-edge-score the client might have injected so it
    can't poison the origin's view of the request."""
    vcl = miss_snippet()
    assert "unset bereq.http.x-edge-score;" in vcl


def test_miss_unsets_scoring_pass_marker():
    """The internal X-Edge-Scoring-Pass marker stays internal — don't
    forward it to the real origin."""
    vcl = miss_snippet()
    assert "unset bereq.http.X-Edge-Scoring-Pass;" in vcl


# ── generate_scoring_vcl: integration shape ──────────────────────────────────


def test_generate_returns_six_snippets_keyed_by_name():
    snippets = generate_scoring_vcl("Svc", "test_secret_abc123")
    assert set(snippets.keys()) == {
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    }


def test_enforce_snippet_errors_429_on_enforce_subfield():
    """Enforce snippet fires only on req.restarts==1 + edge-only,
    issues `error 429 "Too Many Requests"` so vcl_error can hook in a
    custom page later. The three guard conditions MUST appear in the
    documented order (edge-only → restart-once → enforce-subfield) so
    the conjunction reads top-to-bottom the same way the design doc
    describes it; reordering it would still compile but obscures the
    intent and breaks grep-by-pattern in ops runbooks.

    The edge-only check is the unforgeable ``fastly.ff.visits_this_service == 0``
    (the retired shield-auth secret variant is gone).
    """
    vcl = enforce_snippet()
    # (a) Conjunction order — find the `if (` predicate (skip past any
    #     comment lines that mention the same tokens) then verify the
    #     three guards appear in order inside that predicate.
    if_idx = vcl.find("if (")
    assert if_idx != -1, "enforce snippet missing top-level if predicate"
    predicate_tail = vcl[if_idx:]
    edge_idx = predicate_tail.find("fastly.ff.visits_this_service == 0")
    restart_idx = predicate_tail.find("req.restarts == 1")
    enforce_idx = predicate_tail.find('req.http.x-edge-score:enforce == "1"')
    assert edge_idx != -1, "fastly.ff edge guard missing from if predicate"
    assert restart_idx != -1, "req.restarts == 1 guard missing from if predicate"
    assert enforce_idx != -1, "enforce subfield guard missing from if predicate"
    assert edge_idx < restart_idx < enforce_idx, (
        "enforce snippet guards must appear in order: "
        "fastly.ff.visits_this_service == 0, req.restarts == 1, "
        'req.http.x-edge-score:enforce == "1"'
    )
    # And the three must be joined by `&&` (single if-conjunction),
    # not separate nested ifs that could fire independently.
    between_edge_restart = predicate_tail[edge_idx:restart_idx]
    between_restart_enforce = predicate_tail[restart_idx:enforce_idx]
    assert "&&" in between_edge_restart, "edge → restart guards must be &&-joined"
    assert "&&" in between_restart_enforce, "restart → enforce guards must be &&-joined"
    # (b) Action is `error 429 "Too Many Requests"` — full literal so the
    #     status code AND the reason phrase are both pinned.
    assert 'error 429 "Too Many Requests"' in vcl
    # The retired shield-auth edge-detection mechanism must NOT appear.
    assert "X-Edge-Shield-Auth" not in vcl


def test_enforce_snippet_parameterized_status_code():
    """Operator-overridable status code: the enforce snippet should accept
    any 4xx/5xx code and emit the matching reason phrase. Out-of-range
    inputs and None fall back to the default 429."""
    from backend.provision.session_scoring_vcl import (
        DEFAULT_ENFORCE_STATUS_CODE,
        enforce_reason_phrase,
        enforce_snippet,
        resolve_enforce_status_code,
    )

    assert DEFAULT_ENFORCE_STATUS_CODE == 429
    assert resolve_enforce_status_code(None) == 429
    assert resolve_enforce_status_code(403) == 403
    assert resolve_enforce_status_code(99) == 429  # below min → default
    assert resolve_enforce_status_code(600) == 429  # above max → default
    assert resolve_enforce_status_code("not-an-int") == 429
    # Reason phrases come from Python's stdlib ``http.HTTPStatus`` so any
    # IANA-registered 4xx/5xx code yields its canonical phrase. Non-standard
    # codes (419, 444, 530, 599) fall back to "Blocked" — HTTPStatus raises
    # ValueError on them.
    assert enforce_reason_phrase(403) == "Forbidden"
    assert enforce_reason_phrase(429) == "Too Many Requests"
    assert enforce_reason_phrase(451) == "Unavailable For Legal Reasons"
    assert enforce_reason_phrase(503) == "Service Unavailable"
    assert enforce_reason_phrase(511) == "Network Authentication Required"
    assert enforce_reason_phrase(599) == "Blocked"
    assert enforce_reason_phrase(444) == "Blocked"

    # Default still bakes "error 429 Too Many Requests" (backward compat).
    assert 'error 429 "Too Many Requests"' in enforce_snippet()

    # Common overrides each emit code + standard reason phrase.
    for code, phrase in [
        (403, "Forbidden"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
    ]:
        snippet = enforce_snippet(code)
        assert f'error {code} "{phrase}"' in snippet, f'enforce_snippet({code}) should emit `error {code} "{phrase}"`'
        # Guard conditions must stay exactly the same regardless of code.
        # Edge boundary is the unforgeable fastly.ff.visits_this_service == 0 check.
        assert "fastly.ff.visits_this_service == 0" in snippet
        assert "req.restarts == 1" in snippet
        assert 'req.http.x-edge-score:enforce == "1"' in snippet

    # Unusual but IANA-registered code (418 is registered in RFC 2324).
    assert 'error 418 "I\'m a Teapot"' in enforce_snippet(418)
    # Non-standard code → generic "Blocked" reason.
    assert 'error 599 "Blocked"' in enforce_snippet(599)

    # Out-of-range input → falls back to default (defense in depth).
    assert 'error 429 "Too Many Requests"' in enforce_snippet(99)
    assert 'error 429 "Too Many Requests"' in enforce_snippet(600)


def test_generate_scoring_vcl_threads_enforce_status_code():
    """generate_scoring_vcl must thread enforce_status_code through to the
    enforce snippet so enable_scoring's per-service override actually
    reaches the baked VCL."""
    from backend.provision.session_scoring_vcl import (
        SCORING_ENFORCE_NAME,
        generate_scoring_vcl,
    )

    snippets = generate_scoring_vcl("svc-x", "secret-y", enforce_status_code=451)
    assert 'error 451 "Unavailable For Legal Reasons"' in snippets[SCORING_ENFORCE_NAME]

    # None → default 429.
    snippets_default = generate_scoring_vcl("svc-x", "secret-y", enforce_status_code=None)
    assert 'error 429 "Too Many Requests"' in snippets_default[SCORING_ENFORCE_NAME]


def test_deliver_captures_enforce_header():
    """Deliver pass-1 must capture the scorer's X-Edge-Score-Enforce
    header into a subfield so the Enforce recv snippet can read it
    across the restart."""
    vcl = deliver_snippet()
    assert "set req.http.x-edge-score:enforce = resp.http.x-edge-score-enforce;" in vcl
    # And anti-leak unset must include it too.
    assert "unset resp.http.x-edge-score-enforce;" in vcl


def test_generate_bakes_service_id_into_pass_snippet():
    """Service id (for AAD binding) is set on bereq in vcl_pass, not in
    vcl_recv. Deliver/fetch/miss don't need it."""
    snippets = generate_scoring_vcl("UniqueServiceXYZ", "test_secret_abc123")
    assert "UniqueServiceXYZ" in snippets[SCORING_PASS_NAME]
    assert "UniqueServiceXYZ" not in snippets[SCORING_RECV_NAME]
    assert "UniqueServiceXYZ" not in snippets[SCORING_FETCH_NAME]
    assert "UniqueServiceXYZ" not in snippets[SCORING_DELIVER_NAME]
    assert "UniqueServiceXYZ" not in snippets[SCORING_MISS_NAME]


def test_generate_bakes_secret_into_pass_snippet_only():
    """The shared request_secret travels via the X-Edge-Scorer-Auth header that
    pass_snippet sets on bereq — the scorer 401s on mismatch. With the
    X-Edge-Shield-Auth edge-detection mechanism retired, pass_snippet is now the
    ONLY snippet that embeds the secret; recv/fetch/deliver/miss/enforce key
    their edge check on the unforgeable fastly.ff.visits_this_service == 0 and
    never reference the secret."""
    snippets = generate_scoring_vcl("Svc", "deadbeef1234")
    assert "deadbeef1234" in snippets[SCORING_PASS_NAME]
    # The secret is embedded ONLY in pass_snippet now.
    assert "deadbeef1234" not in snippets[SCORING_RECV_NAME]
    assert "deadbeef1234" not in snippets[SCORING_FETCH_NAME]
    assert "deadbeef1234" not in snippets[SCORING_DELIVER_NAME]
    assert "deadbeef1234" not in snippets[SCORING_MISS_NAME]
    assert "deadbeef1234" not in snippets[SCORING_ENFORCE_NAME]


def test_generate_is_pure_no_randomness():
    """Two calls with the same args return identical strings — needed
    for the ensure_vcl_snippet diff to recognize 'already up to date'
    and skip a redundant API call on re-deploy."""
    a = generate_scoring_vcl("Svc", "test_secret_abc123")
    b = generate_scoring_vcl("Svc", "test_secret_abc123")
    assert a == b


# ── EC-07: generator-local VCL string-injection guard ────────────────────────


def test_request_secret_with_quote_is_rejected():
    """EC-07: a request_secret containing a double-quote would terminate the VCL
    string literal it's substituted into (== "{secret}") and could inject VCL.
    The generator rejects it (belt-and-suspenders; real secrets are
    secrets.token_hex). The secret is now embedded ONLY by pass_snippet
    (X-Edge-Scorer-Auth), so that snippet refuses — as does the top-level
    generator which threads the secret through to pass_snippet. recv/deliver/
    miss/enforce no longer take the secret at all, so there is nothing to inject
    there."""
    bad = 'abc" || req.http.x-evil == "1'
    callers = (
        lambda: pass_snippet("Svc", bad),
        lambda: generate_scoring_vcl("Svc", bad),
    )
    for call in callers:
        with pytest.raises(ValueError, match="request_secret"):
            call()


def test_exclude_url_regex_with_quote_is_rejected_but_backslash_is_allowed():
    """EC-07: the recv exclusion regex is interpolated into a VCL string literal
    too. A double-quote (or control char) is rejected, but a backslash MUST be
    allowed — real regexes use ``\\.`` (the default asset regex does)."""
    with pytest.raises(ValueError, match="exclude_url_regex"):
        recv_snippet("Svc", exclude_url_regex=r'\.(png)"; bad')
    # A normal backslash-bearing regex generates cleanly (no false positive).
    body = recv_snippet("Svc", exclude_url_regex=r"^/skip/.*\.(png|jpg)$")
    assert r"\.(png|jpg)" in body


def test_default_secret_and_regex_generate_cleanly():
    """The guard never fires on the real default inputs (hex secret + the bundled
    asset-extension regex, which contains backslashes)."""
    body = generate_scoring_vcl("Svc", "a" * 64)  # hex-shaped secret
    assert body  # no raise
