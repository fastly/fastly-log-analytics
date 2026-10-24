from __future__ import annotations

"""Tests for backend.provision.session_scoring_vcl — Six snippets
(recv/pass/fetch/deliver/miss/enforce) that wire the customer's VCL
service to the scoring Compute backend via the canonical Fastly
preflight pattern (fiddle 4b1a74ee).

These tests pin the exact subroutine assignments and key VCL idioms so
any unintentional drift surfaces immediately."""

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
    id headers on bereq and rewrite bereq.method to GET so inbound POST/PUT
    bodies are preserved at the edge for the restarts == 1 origin pass."""
    vcl = pass_snippet("MyServiceId", "deadbeef1234567890")
    assert f"if (req.backend == {SCORING_BACKEND_VCL_NAME})" in vcl
    assert 'set bereq.method = "GET";' in vcl
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
    for caller_fn in callers:
        with pytest.raises(ValueError, match="request_secret"):
            caller_fn()


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


# ── Merged from test_scoring_vcl_l2_gaps.py ──────────────────────────────────────────────────────

"""Edge-scorer wiring guards in the generated scoring VCL.

1. No snippet supplies ``X-Edge-Matrix-Age-Days`` (or any client-spoofable
   scoring-control header) on the scorer bereq. L2's contribution to the
   enforced score is now gated by the operator's explicit opt-in, read
   SERVER-SIDE by the scorer from the scoring_config ConfigStore
   (``l2_enforce_enabled`` + the ``l2_enabled_at`` fade-in anchor; see
   ``compute/scorer/src/main.rs::load_l2_enforce_enabled`` /
   ``load_l2_days_since_optin``) — deliberately NOT from a request header. A
   client-supplied scoring-age/weight header would let an attacker zero out
   their own L2 contribution to the enforced ``X-Edge-Score`` (the F009 evasion
   class already closed for ``X-Edge-Prev-Route`` / ``X-Edge-Prev-Anchor``).
   This test guards against a future change re-introducing that spoofable path.

2. The deliver snippet captures the scorer's ``X-Edge-Matrix-Version`` response
   header into the consolidated ``req.http.x-edge-score`` log subfield BEFORE the
   anti-leak unset, so a logged edge score can be correlated to the matrix
   version that produced it (EC-03). This test pins that the capture is present
   AND that the anti-leak unset still runs.

Mirrors the existing ``test_recv_does_not_set_dead_prev_route_header`` pattern in
tests/scoring/test_session_scoring_vcl.py (asserting a header is intentionally
absent).
"""


_SVC = "svc-l2-gap-test"
_SECRET = "deadbeef1234567890abcdefdeadbeef"


def test_no_snippet_sets_matrix_age_days_header():
    """L2's enforcement gate (opt-in flag + fade-in anchor) is read SERVER-SIDE
    by the scorer from scoring_config, never from a client-controllable header.
    Pin that no generated snippet supplies X-Edge-Matrix-Age-Days on the scorer
    bereq: a request-header age/weight would let a client zero out their own L2
    contribution (the F009 evasion class). If a snippet ever sets it, this trips
    so the spoofable path is rejected — the gate must stay config-store-only."""
    snippets = generate_scoring_vcl(_SVC, _SECRET)
    offenders = {
        name: body for name, body in snippets.items() if "Matrix-Age-Days" in body or "matrix-age-days" in body.lower()
    }
    assert not offenders, (
        "A scoring snippet now supplies X-Edge-Matrix-Age-Days — L2's enforcement "
        "gate must be derived server-side from the scoring_config ConfigStore, NOT "
        "a client-spoofable header (which would depress the L2 contribution). "
        f"Remove it. Snippets: {list(offenders)}"
    )


def test_deliver_captures_matrix_version_before_stripping_it():
    """EC-03: deliver now captures the scorer's X-Edge-Matrix-Version into the
    consolidated x-edge-score:matrix log subfield BEFORE the anti-leak unset, so
    a logged edge score can be correlated to the matrix version that produced it.
    Pin that BOTH the capture and the anti-leak unset are present, and that the
    capture comes first (capturing after the unset would log an empty value)."""
    body = deliver_snippet()
    capture = "set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;"
    unset = "unset resp.http.X-Edge-Matrix-Version;"
    assert capture in body, "deliver must capture matrix-version into a log subfield"
    assert unset in body, "deliver must still anti-leak unset the scorer header"
    assert body.index(capture) < body.index(unset), (
        "the capture must run BEFORE the anti-leak unset, else the subfield logs empty"
    )
    # Sanity: capture rides the same consolidated x-edge-score subfield bundle as
    # the other scorer outputs.
    for captured in ("x-edge-score:score", "x-edge-score:reason", "x-edge-score:exec"):
        assert captured in body


# ── Merged from test_session_scoring_setup.py ──────────────────────────────────────────────────────

"""Tests for backend.provision.session_scoring_setup.

The provisioner is a thin orchestrator over Fastly's HTTP API — every test
here pins one assertion about WHAT the Fastly API gets called with (URL,
method, JSON body), and a few about the idempotency contract. Real Fastly
roundtrips live in the manual deploy script."""

from unittest.mock import MagicMock, call, patch

from backend.provision import session_scoring_setup as sss
from backend.provision.session_scoring_setup import (
    COMPUTE_MANAGE_URL,
    CONFIG_RESOURCE_LINK_NAME,
    CONFIG_STORE_MANAGE_URL,
    CURRENT_KEY_HEX,
    DEBUG_LOG_DEFAULT,
    DEBUG_LOG_KEY,
    KEYS_RESOURCE_LINK_NAME,
    KV_STORE_MANAGE_URL,
    MATRIX_RESOURCE_LINK_NAME,
    SCORING_ENABLED_AT_KEY,
    EntitlementError,
    delete_scoring_service,
    ensure_scoring_service,
)

LOG_SVC = "TestLogSvcABC123"
TOKEN = "FAKE_TOKEN"


# ── Name + domain templates ──────────────────────────────────────────────────


def test_service_name_pattern():
    assert sss._scoring_service_name("ABC123") == "Session Scoring Service for ABC123"


def test_domain_lowercases_service_id():
    # Customer-facing domain must be RFC-1035 lowercase even if the
    # logging service id is mixed-case.
    d = sss._scoring_domain("MixedCaseSvcId001")
    assert d == "fos-mixedcasesvcid001-session-scorer.edgecompute.app"
    assert d == d.lower()


# ── ensure_scoring_service: create-from-scratch path ─────────────────────────


def _fastly_mock_for_create():
    """A MagicMock that returns sensible defaults for each API call the
    create-path makes. The side_effect is keyed off the (method, path)
    tuple so test assertions can verify call order without coupling to
    side-effect ordering."""
    responses = {
        ("GET", "/service"): [],  # no pre-existing services
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("POST", "/service/SCORING_SVC_ID/version/1/domain"): {},
        ("POST", "/service/SCORING_SVC_ID/version/1/backend"): {},
        ("POST", "/resources/stores/config"): None,  # set below per call
        ("POST", "/resources/stores/config/KEYS_STORE_ID/item"): {},
        ("POST", "/resources/stores/config/CFG_STORE_ID/item"): {},
        ("POST", "/service/SCORING_SVC_ID/version/1/resource"): {},
        # Matrix KV store: not found on first list, then created.
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
    }

    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return responses.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_creates_full_stack_when_nothing_exists():
    fastly_mock = _fastly_mock_for_create()
    # KV-Store entitlement probe uses fos_setup.product_enabled (a separate
    # `fastly` binding), so patch it True or it leaks a real api.fastly.com call.
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    # Returned dict contains all the IDs the caller needs to stash.
    assert result["scoring_service_id"] == "SCORING_SVC_ID"
    assert result["scoring_service_name"] == f"Session Scoring Service for {LOG_SVC}"
    assert result["scoring_domain"] == f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app"
    assert result["scoring_keys_store_id"] == "KEYS_STORE_ID"
    assert result["scoring_config_store_id"] == "CFG_STORE_ID"
    assert result["scoring_matrix_store_id"] == "MATRIX_STORE_ID"
    # Real AES key — 64 hex chars (32 bytes).
    assert len(result["aes_key_hex"]) == 64
    int(result["aes_key_hex"], 16)  # valid hex

    # Verify the API was called with the right shapes (subset of all calls).
    calls = fastly_mock.call_args_list
    # 1. List services (idempotency probe).
    assert call("GET", "/service", token=TOKEN) in calls
    # 2. Create scoring service as wasm type.
    create_svc_call = next(c for c in calls if c.args[:2] == ("POST", "/service"))
    assert create_svc_call.args[2] == {"name": f"Session Scoring Service for {LOG_SVC}", "type": "wasm"}
    # 3. Add domain.
    domain_call = next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/domain")
    assert domain_call.args[2]["name"].endswith("session-scorer.edgecompute.app")
    # 4. Add placeholder backend.
    backend_call = next(c for c in calls if c.args[1] == "/service/SCORING_SVC_ID/version/1/backend")
    assert backend_call.args[2]["name"] == "placeholder_origin"
    # 5. Two stores created.
    store_creates = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config")]
    assert len(store_creates) == 2
    assert any("scoring_keys_" in c.args[2]["name"] for c in store_creates)
    assert any("scoring_config_" in c.args[2]["name"] for c in store_creates)
    # 6. AES key uploaded to keys store.
    key_upload = next(c for c in calls if c.args[1].endswith("/KEYS_STORE_ID/item"))
    assert key_upload.args[2]["item_key"] == CURRENT_KEY_HEX
    assert len(key_upload.args[2]["item_value"]) == 64
    # 7. Debug toggle uploaded to config store, default "0".
    debug_upload = next(c for c in calls if c.args[1].endswith("/CFG_STORE_ID/item"))
    assert debug_upload.args[2] == {"item_key": DEBUG_LOG_KEY, "item_value": DEBUG_LOG_DEFAULT}
    # 7b. Layer-2 warm-up anchor seeded into the config store with an epoch value
    #     (EC-01). It rides the same POST .../CFG_STORE_ID/item path as the debug
    #     toggle, so select it by item_key.
    cfg_item_posts = [c for c in calls if c.args[:2] == ("POST", "/resources/stores/config/CFG_STORE_ID/item")]
    anchor_upload = next(c for c in cfg_item_posts if c.args[2]["item_key"] == SCORING_ENABLED_AT_KEY)
    assert anchor_upload.args[2]["item_value"].isdigit(), "anchor must be epoch seconds"
    assert int(anchor_upload.args[2]["item_value"]) > 0
    # 8. Resource links created with the short names the Wasm expects
    #    (keys + config ConfigStores + the matrix KV store).
    link_calls = [c for c in calls if c.args[1].endswith("/version/1/resource")]
    assert len(link_calls) == 3
    link_names = {c.args[2]["name"] for c in link_calls}
    assert link_names == {KEYS_RESOURCE_LINK_NAME, CONFIG_RESOURCE_LINK_NAME, MATRIX_RESOURCE_LINK_NAME}


# ── ensure_scoring_service: entitlement gating + rollback ────────────────────


def _create_side_effect(*, fail_on=None, fail_exc=None):
    """`fastly` mock for the create path that ALSO answers the rollback
    (delete_scoring_service) calls, so a mid-build failure can be asserted to
    tear everything down. Raises ``fail_exc`` whenever it sees the
    ``(method, path)`` tuple ``fail_on``."""
    config_store_responses = iter(
        [
            {"id": "KEYS_STORE_ID", "name": "scoring_keys_SCORING_SVC_ID"},
            {"id": "CFG_STORE_ID", "name": "scoring_config_SCORING_SVC_ID"},
        ]
    )
    base = {
        ("GET", "/service"): [],  # idempotency probe: nothing exists
        ("POST", "/service"): {"id": "SCORING_SVC_ID", "name": "x", "type": "wasm"},
        ("GET", "/resources/stores/kv"): [],
        ("POST", "/resources/stores/kv"): {"id": "MATRIX_STORE_ID", "name": "scoring_matrix_SCORING_SVC_ID"},
        # rollback (delete_scoring_service): no active versions → straight to deletes.
        ("GET", "/service/SCORING_SVC_ID/version"): [],
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if fail_on and (method, path) == fail_on:
            raise fail_exc
        if (method, path) == ("POST", "/resources/stores/config"):
            return next(config_store_responses)
        return base.get((method, path), {})

    return MagicMock(side_effect=side_effect)


def test_ensure_raises_when_kv_store_not_enabled():
    """KV Store is the only required product with a status endpoint, so it's a
    pre-flight check BEFORE any resource is created — a missing entitlement
    must cost zero cleanup (no service ever gets made)."""
    fastly_mock = _create_side_effect()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=False),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "kv_store_not_enabled"
    assert ei.value.link == KV_STORE_MANAGE_URL
    # Nothing was created — the wasm service POST is never reached.
    assert not any(c.args[:2] == ("POST", "/service") for c in fastly_mock.call_args_list)


def test_ensure_maps_compute_create_4xx_to_compute_not_enabled():
    """A 4xx on the wasm `POST /service` means Compute isn't enabled. It's the
    first durable resource, so there's nothing to roll back."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/service"),
        fail_exc=RuntimeError("HTTP 403 Forbidden"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "compute_not_enabled"
    assert ei.value.link == COMPUTE_MANAGE_URL
    # Service create failed → no rollback delete attempted.
    assert not any(c.args[:2] == ("DELETE", "/service/SCORING_SVC_ID") for c in fastly_mock.call_args_list)


def test_ensure_maps_config_store_4xx_to_config_store_not_enabled_and_rolls_back():
    """A 4xx on the Config Store create means it isn't enabled for Compute. By
    then the wasm service exists, so it must be rolled back."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/resources/stores/config"),
        fail_exc=RuntimeError("HTTP 400 Bad Request"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(EntitlementError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    assert ei.value.code == "config_store_not_enabled"
    assert ei.value.link == CONFIG_STORE_MANAGE_URL
    # Rollback tore down the just-created wasm service.
    assert any(c.args[:2] == ("DELETE", "/service/SCORING_SVC_ID") for c in fastly_mock.call_args_list)


def test_ensure_rolls_back_everything_on_late_non_entitlement_failure():
    """A failure AFTER the stores are created (here, the first resource-link
    POST) must roll back the service AND all three stores, and re-raise the
    original error unchanged (not mis-mapped to an entitlement error)."""
    fastly_mock = _create_side_effect(
        fail_on=("POST", "/service/SCORING_SVC_ID/version/1/resource"),
        fail_exc=RuntimeError("HTTP 500 Internal Server Error"),
    )
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        with pytest.raises(RuntimeError) as ei:
            ensure_scoring_service(LOG_SVC, TOKEN)

    # Original error preserved — NOT converted to an EntitlementError.
    assert not isinstance(ei.value, EntitlementError)
    assert "HTTP 5" in str(ei.value)
    deletes = {c.args[:2] for c in fastly_mock.call_args_list if c.args[0] == "DELETE"}
    assert ("DELETE", "/service/SCORING_SVC_ID") in deletes
    assert ("DELETE", "/resources/stores/config/KEYS_STORE_ID") in deletes
    assert ("DELETE", "/resources/stores/config/CFG_STORE_ID") in deletes
    assert ("DELETE", "/resources/stores/kv/MATRIX_STORE_ID") in deletes


# ── ensure_scoring_service: idempotent reuse path ────────────────────────────


def test_ensure_reuses_existing_service():
    """If the scoring service already exists, ensure_ returns its IDs
    without creating new stores or rotating the AES key."""
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        # Matrix KV store already exists for this service → no self-heal create.
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        # request_secret is read back from the store (source of truth) on reuse.
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": "stored_secret_" + "z" * 50}
        # Warm-up anchor already present → preserved, no re-seed (write-once).
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            return {"item_key": SCORING_ENABLED_AT_KEY, "item_value": "1700000000"}
        # We should NOT see any POSTs.
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    assert result["scoring_keys_store_id"] == "EXISTING_KEYS"
    assert result["scoring_config_store_id"] == "EXISTING_CFG"
    assert result["scoring_matrix_store_id"] == "EXISTING_MATRIX"
    # request_secret is read back from the store (source of truth) so the
    # caller embeds the store's actual secret into the VCL — no cfg/store drift.
    assert result["request_secret"] == "stored_secret_" + "z" * 50
    # Critical: we did NOT rotate the AES key on a reuse.
    assert result["aes_key_hex"] == ""


def test_ensure_reuse_heals_missing_warmup_anchor():
    """A service provisioned before EC-01 has no scoring_enabled_at anchor.
    Re-enabling it must SEED the anchor (write-once) so the service's advisory
    readiness clock starts from now — not turn L2 on with a step change. Pins that
    the reuse path POSTs the anchor exactly when the GET 404s."""
    existing = {"id": "EXISTING_SVC_ID", "name": f"Session Scoring Service for {LOG_SVC}"}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service"):
            return [existing]
        if (method, path) == ("GET", "/resources/stores/config"):
            return [
                {"id": "EXISTING_KEYS", "name": "scoring_keys_EXISTING_SVC_ID"},
                {"id": "EXISTING_CFG", "name": "scoring_config_EXISTING_SVC_ID"},
            ]
        if (method, path) == ("GET", "/resources/stores/kv"):
            return [{"id": "EXISTING_MATRIX", "name": "scoring_matrix_EXISTING_SVC_ID"}]
        if (method, path) == ("GET", "/resources/stores/config/EXISTING_KEYS/item/request_secret"):
            return {"item_key": "request_secret", "item_value": "s" * 64}
        # Anchor item MISSING on this legacy service → GET 404s, heal POSTs it.
        if (method, path) == ("GET", f"/resources/stores/config/EXISTING_CFG/item/{SCORING_ENABLED_AT_KEY}"):
            raise RuntimeError("404 not found")
        if (method, path) == ("POST", "/resources/stores/config/EXISTING_CFG/item"):
            return {}
        raise AssertionError(f"unexpected API call: {method} {path}")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = ensure_scoring_service(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == "EXISTING_SVC_ID"
    anchor_post = next(
        c for c in fastly_mock.call_args_list if c.args[:2] == ("POST", "/resources/stores/config/EXISTING_CFG/item")
    )
    assert anchor_post.args[2]["item_key"] == SCORING_ENABLED_AT_KEY
    assert anchor_post.args[2]["item_value"].isdigit()


# ── ensure_scoring_service: status callback wiring ───────────────────────────


def test_ensure_emits_status_callbacks_during_creation():
    fastly_mock = _fastly_mock_for_create()
    status_cb = MagicMock()
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("backend.provision.session_scoring_setup.product_enabled", return_value=True),
    ):
        ensure_scoring_service(LOG_SVC, TOKEN, status_cb=status_cb)
    # At least the "ensuring", "created service", "domain", "stores", "linked"
    # phases each emit a callback.
    assert status_cb.call_count >= 4
    msgs = " ".join(str(c.args[0]) for c in status_cb.call_args_list)
    assert "scoring service" in msgs.lower()
    assert "domain" in msgs.lower()
    assert "config stores" in msgs.lower()


# ── delete_scoring_service: teardown path ────────────────────────────────────


def test_delete_runs_full_teardown_in_correct_order():
    """Order is: deactivate active versions → delete service → delete each
    config store. Versions must be deactivated first or service-delete 409s;
    service must die before stores or the resource-link blocks store-delete."""
    versions = [{"number": 1, "active": True}, {"number": 2, "active": False}]
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path))
        if (method, path) == ("GET", "/service/SVC_X/version"):
            return versions
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        delete_scoring_service(
            "SVC_X",
            scoring_keys_store_id="KEYS_X",
            scoring_config_store_id="CFG_X",
            scoring_matrix_store_id="MATRIX_X",
            token=TOKEN,
        )

    # Ordering invariant: deactivate happens before DELETE /service/SVC_X,
    # which happens before either store delete.
    deactivate_idx = calls_in_order.index(("PUT", "/service/SVC_X/version/1/deactivate"))
    svc_delete_idx = calls_in_order.index(("DELETE", "/service/SVC_X"))
    keys_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/KEYS_X"))
    cfg_delete_idx = calls_in_order.index(("DELETE", "/resources/stores/config/CFG_X"))
    assert deactivate_idx < svc_delete_idx < keys_delete_idx
    assert svc_delete_idx < cfg_delete_idx
    # Inactive version (#2) does NOT get a deactivate call.
    assert ("PUT", "/service/SVC_X/version/2/deactivate") not in calls_in_order
    # Matrix KV store torn down too: empty the key first, then the store,
    # both after the service is gone (resource link would otherwise block it).
    kv_key_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X/keys/matrix"))
    kv_store_idx = calls_in_order.index(("DELETE", "/resources/stores/kv/MATRIX_X"))
    assert svc_delete_idx < kv_key_idx < kv_store_idx


def test_delete_skips_already_deleted_service_silently():
    """404s on the version-list call short-circuit; the function returns
    normally (idempotent teardown). Stores still get tried."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/MISSING/version"):
            raise RuntimeError("404 not found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        # Should not raise.
        delete_scoring_service("MISSING", token=TOKEN)


def test_delete_with_empty_service_id_is_noop():
    """Common case: called from teardown when the logging service was never
    scoring-enabled. Must not call Fastly at all."""
    fastly_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        delete_scoring_service("", token=TOKEN)
    fastly_mock.assert_not_called()


def test_delete_tolerates_404_on_store_delete():
    """A store that was already manually deleted shouldn't fail the teardown
    (idempotency contract)."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if path.startswith("/resources/stores/config/MISSING_STORE"):
            raise RuntimeError("404 not found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        # Should not raise even though one of the store deletes 404s.
        delete_scoring_service(
            "SVC",
            scoring_keys_store_id="MISSING_STORE",
            scoring_config_store_id="ALSO_MISSING",
            token=TOKEN,
        )


def test_delete_surfaces_non_404_store_failure():
    """A non-404 store-delete failure must NOT raise (one stuck store can't block
    the rest of teardown) but MUST be returned + surfaced via status_cb so the
    operator can hand-clean it — otherwise the id is silently dropped from cfg
    and becomes an un-retryable orphan."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        if method == "DELETE" and path.startswith("/resources/stores/config/STUCK"):
            raise RuntimeError("500 internal error")  # non-404 → genuine failure
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    msgs = []
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        failed = delete_scoring_service(
            "SVC",
            scoring_keys_store_id="STUCK",
            scoring_config_store_id="FINE",
            token=TOKEN,
            status_cb=msgs.append,
        )

    # The stuck store is reported (label, id); the healthy one is not.
    assert ("scoring_keys", "STUCK") in failed
    assert all(store_id != "FINE" for _, store_id in failed)
    # And a manual-cleanup message reached the stream.
    assert any("STUCK" in m and "manual" in m.lower() for m in msgs)


def test_delete_returns_empty_list_on_clean_teardown():
    """Full-success teardown returns an empty failed-id list."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SVC/version"):
            return []
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        failed = delete_scoring_service(
            "SVC",
            scoring_keys_store_id="K",
            scoring_config_store_id="C",
            scoring_matrix_store_id="M",
            token=TOKEN,
        )
    assert failed == []


# ── Merged from test_session_scoring_rotate_key.py ──────────────────────────────────────────────────────

"""Tests for backend.provision.session_scoring_setup.rotate_aes_key.

The rotator's contract: pull current_key_hex out of the scoring_keys
ConfigStore, slot it into previous_key_hex, generate a fresh 32-byte hex
key, write it as the new current_key_hex. Each test pins one axis of
that contract — first-rotation cold start, second-rotation steady state,
upsert fallback when PATCH 404s, input validation, and HTTP error
propagation. Mocking pattern mirrors test_session_scoring_setup.py:
patch the module-level ``fastly`` symbol with a MagicMock whose
``side_effect`` is keyed off ``(method, path)``."""


from backend.provision.session_scoring_setup import (
    PREVIOUS_KEY_HEX,
    rotate_aes_key,
)

KEYS_STORE_ID = "KEYS_STORE_XYZ"
TOKEN = "FAKE_TOKEN"

CURRENT_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{CURRENT_KEY_HEX}"
PREVIOUS_ITEM_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item/{PREVIOUS_KEY_HEX}"
ITEM_COLLECTION_PATH = f"/resources/stores/config/{KEYS_STORE_ID}/item"


def _hex64(byte: int) -> str:
    """Helper: produce a deterministic 64-char hex string for fixtures."""
    return f"{byte:02x}" * 32


# ── Happy paths ──────────────────────────────────────────────────────────────


def test_first_rotation_moves_current_into_previous_and_writes_fresh_current():
    """Cold start: scoring_keys has current_key_hex only (no previous_key_hex
    yet — that slot is created on first rotation). The old current must
    be PATCHed/POSTed into the previous slot, and a brand-new 32-byte
    key written as the new current."""
    old_current = _hex64(0xAB)
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        # PATCH on previous_key_hex 404s — slot doesn't exist yet.
        if method == "PATCH" and path == PREVIOUS_ITEM_PATH:
            raise RuntimeError("HTTP 404 PATCH /.../previous_key_hex\n    not found")
        # POST upsert fallback succeeds, as does PATCH on the existing
        # current_key_hex slot.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # Returned dict: new 64-char hex current, old value preserved as
    # previous, ISO-8601 rotation timestamp.
    assert len(result["current_key_hex"]) == 64
    int(result["current_key_hex"], 16)  # valid hex
    assert result["previous_key_hex"] == old_current
    assert result["current_key_hex"] != old_current
    assert "T" in result["rotated_at"]  # ISO 8601

    # The previous slot was written with the OLD current value (PATCH 404
    # → POST upsert).
    prev_writes = [
        c
        for c in calls_in_order
        if c[1] == PREVIOUS_ITEM_PATH
        or c[1] == ITEM_COLLECTION_PATH
        and c[2]
        and c[2].get("item_key") == PREVIOUS_KEY_HEX
    ]
    # Either form of write (PATCH or POST-collection) should carry the old_current value.
    prev_values = [
        (c[2] or {}).get("item_value")
        for c in calls_in_order
        if (c[1] == PREVIOUS_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == PREVIOUS_KEY_HEX)
    ]
    assert old_current in prev_values, f"previous slot was never written with old current: {calls_in_order}"

    # The current slot was overwritten with the NEW key.
    cur_writes = [
        (c[2] or {}).get("item_value")
        for c in calls_in_order
        if (c[1] == CURRENT_ITEM_PATH and c[0] == "PATCH")
        or (c[1] == ITEM_COLLECTION_PATH and c[0] == "POST" and (c[2] or {}).get("item_key") == CURRENT_KEY_HEX)
    ]
    assert result["current_key_hex"] in cur_writes

    # Both keys must be 64 hex chars AND distinct.
    assert len(old_current) == 64
    assert len(result["current_key_hex"]) == 64
    assert old_current != result["current_key_hex"]


def test_second_rotation_overwrites_previous_with_old_current():
    """Steady state: both current_key_hex and previous_key_hex exist in the
    store. After rotation: previous = old-current; current = brand-new
    key. The old previous is dropped (one grace level by design)."""
    old_current = _hex64(0xCD)
    old_previous = _hex64(0xEF)  # this value should be DISCARDED
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": old_current}
        # Both slots already exist — PATCH succeeds on both.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert result["previous_key_hex"] == old_current
    assert result["current_key_hex"] != old_current
    assert result["current_key_hex"] != old_previous  # not somehow recycled

    # The previous slot was PATCHed with old_current (not the prior previous).
    prev_patches = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == PREVIOUS_ITEM_PATH]
    assert len(prev_patches) == 1
    assert prev_patches[0][2] == {"item_value": old_current}

    # The current slot was PATCHed with the new key.
    cur_patches = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]
    assert len(cur_patches) == 1
    assert cur_patches[0][2] == {"item_value": result["current_key_hex"]}

    # The dropped old_previous value MUST NOT appear in any write body —
    # it should be irretrievable after this rotation.
    written_values = [(c[2] or {}).get("item_value") for c in calls_in_order if c[0] in ("PATCH", "POST") and c[2]]
    assert old_previous not in written_values


# ── PATCH-then-POST upsert fallback ──────────────────────────────────────────


def test_patch_404_falls_back_to_post_for_current_key_creation():
    """Edge case: the current_key_hex slot itself is missing (extreme
    cold start, e.g. store was wiped). PATCH on current 404s → POST
    upsert is tried with item_key=current_key_hex."""
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            # No current key in the store at all.
            raise RuntimeError("HTTP 404 GET /.../current_key_hex\n    not found")
        if method == "PATCH":
            raise RuntimeError("HTTP 404 PATCH\n    item does not exist")
        # POST always succeeds.
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # prev_value defaults to "" when GET failed, so previous slot is not
    # written at all on this code path.
    assert result["previous_key_hex"] == ""

    # The current key was POSTed (not PATCHed — that 404'd).
    post_creates = [c for c in calls_in_order if c[0] == "POST" and c[1] == ITEM_COLLECTION_PATH]
    assert any(c[2] == {"item_key": CURRENT_KEY_HEX, "item_value": result["current_key_hex"]} for c in post_creates), (
        f"no POST upsert for current_key_hex: {calls_in_order}"
    )

    # And a PATCH was attempted FIRST (the fallback contract).
    patch_attempts = [c for c in calls_in_order if c[0] == "PATCH" and c[1] == CURRENT_ITEM_PATH]
    assert len(patch_attempts) == 1
    assert patch_attempts[0][2] == {"item_value": result["current_key_hex"]}


# ── Input validation ────────────────────────────────────────────────────────


def test_empty_store_id_raises_value_error():
    """Caller bug: rotate without a store id. Must fail fast before any
    Fastly API call — calling the API with an empty path would build a
    malformed URL and waste a round trip."""
    fastly_mock = MagicMock()
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(ValueError, match="scoring_keys_store_id is required"):
            rotate_aes_key("", token=TOKEN)
    fastly_mock.assert_not_called()


def test_empty_token_propagates_http_401_as_runtime_error():
    """Empty token reaches the Fastly client, which sends an empty
    Fastly-Key header → 401. The GET is wrapped in try/except so prev_value
    falls back to ""; the subsequent PATCH-then-POST attempts on
    current_key_hex both 401 and the POST surfaces as RuntimeError
    (no inner handler swallows it)."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        # Every call comes back as 401 because the token is empty.
        raise RuntimeError("HTTP 401 unauthorized\n    invalid token")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="401"):
            rotate_aes_key(KEYS_STORE_ID, token="")


def test_401_from_fastly_surfaces_as_runtime_error():
    """An explicit auth failure (bad-but-non-empty token) must NOT be
    silently swallowed by the PATCH→POST fallback. The first PATCH-then-POST
    pair on the current slot should bubble the RuntimeError up to the
    caller — losing the rotation silently would leave the cookies-issued-now
    cohort un-decodable on the next deploy."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET":
            # GET is in a try/except — return a value so we get past it.
            return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x11)}
        # All writes (PATCH, POST) 401.
        raise RuntimeError("HTTP 401 unauthorized\n    bad token")

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="401"):
            rotate_aes_key(KEYS_STORE_ID, token="bad-token")


# ── Cryptographic-quality keys ──────────────────────────────────────────────


def test_two_successive_rotations_produce_different_current_keys():
    """``secrets.token_hex(32)`` must be the source, not anything
    predictable. Two back-to-back rotations should produce two distinct
    64-char hex strings. (Birthday-paradox collision probability at
    256 bits of entropy is ~2^-128 — effectively impossible.)"""

    state = {"current": _hex64(0x01)}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": state["current"]}
        if method == "PATCH" and path == CURRENT_ITEM_PATH and body:
            # Track what the rotator wrote so the next GET reflects it.
            state["current"] = body["item_value"]
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch("backend.provision.session_scoring_setup.fastly", fastly_mock):
        first = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)
        second = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    # Distinct keys, each 64 hex chars (full 32 bytes of entropy).
    assert len(first["current_key_hex"]) == 64
    assert len(second["current_key_hex"]) == 64
    assert first["current_key_hex"] != second["current_key_hex"], (
        "two successive rotations produced identical keys — RNG is broken or stubbed"
    )

    # The second rotation's previous_key_hex should equal the first's
    # current_key_hex (proper key-history chaining).
    assert second["previous_key_hex"] == first["current_key_hex"]


# ── Module-level reference smoke check ──────────────────────────────────────


def test_rotate_aes_key_uses_secrets_token_hex():
    """Belt-and-suspenders: patch secrets.token_hex inside the module
    namespace and verify the rotator actually consults it (so a future
    refactor to a weaker RNG would fail this test loudly)."""
    sentinel = "ff" * 32
    calls_in_order: list = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        calls_in_order.append((method, path, body))
        if (method, path) == ("GET", CURRENT_ITEM_PATH):
            return {"item_key": CURRENT_KEY_HEX, "item_value": _hex64(0x22)}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    # The function imports ``secrets`` locally inside its body, so we
    # patch the module attribute that the import resolves against.
    with (
        patch("backend.provision.session_scoring_setup.fastly", fastly_mock),
        patch("secrets.token_hex", return_value=sentinel) as token_hex_mock,
    ):
        result = rotate_aes_key(KEYS_STORE_ID, token=TOKEN)

    assert result["current_key_hex"] == sentinel
    token_hex_mock.assert_called_with(32)
    # Sanity: module reference still intact after the patch.
    assert sss.CURRENT_KEY_HEX == "current_key_hex"
