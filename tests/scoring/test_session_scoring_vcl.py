"""Tests for backend.provision.session_scoring_vcl — Six snippets
(recv/pass/fetch/deliver/miss/enforce) that wire the customer's VCL
service to the scoring Compute backend via the canonical Fastly
preflight pattern (fiddle 4b1a74ee).

These tests pin the exact subroutine assignments and key VCL idioms so
any unintentional drift surfaces immediately."""

from __future__ import annotations

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

# Test value for the shared request_secret. recv/pass/miss/deliver/enforce
# all bake this into the VCL body now; an attacker who could guess it could
# spoof the edge/shield boundary check (034).
_TEST_SECRET = "test_secret_abc123"

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
    vcl = recv_snippet("MyServiceId", _TEST_SECRET)
    assert f"set req.backend = {SCORING_BACKEND_VCL_NAME};" in vcl
    # First-pass guard (034 — replaces the spoofable visits_this_service
    # check): the shield-auth secret comparison is true at the edge (no
    # secret stamped yet) and false on the shield (pass/miss snippets
    # stamped it). req.restarts == 0 prevents a re-fire after our own
    # deliver-side restart; X-Edge-Scoring-Pass != "1" is
    # belt-and-suspenders for unrelated VCL restarts.
    assert f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"' in vcl
    assert "req.restarts == 0" in vcl
    assert 'req.http.X-Edge-Scoring-Pass != "1"' in vcl
    # The old, spoofable boundary must NOT come back.
    assert "fastly.ff.visits_this_service == 0" not in vcl


def test_recv_snippet_unsets_client_supplied_headers():
    """Defense-in-depth: at the client-edge boundary (no shield hop yet
    AND req.restarts == 0), strip every X-Edge-* header a client could
    have set. Without this, an attacker could forge:
      - X-Edge-Scoring-Pass=1   → bypass scoring entirely
      - X-Edge-Score=0          → forge a clean score that subfields propagate
      - X-Edge-Score-Enforce=0  → suppress enforcement of a real-score block
      - X-Edge-Sid=...          → impersonate another session
      - X-Edge-Score-Reason=... → poison reason-attribution telemetry
      - X-Edge-Score-Set-Cookie=... → smuggle Set-Cookie into the response
      - X-Edge-Shield-Auth=...  → spoof the edge/shield boundary check

    The unsets MUST be gated by the shield-auth boundary
    (``req.http.X-Edge-Shield-Auth != "{request_secret}" && req.restarts
    == 0``). Shield hops must NOT scrub (the edge node legitimately set
    those headers on pass-1 deliver before the restart), and
    post-scoring restarts (req.restarts == 1) must NOT scrub either
    (deliver wrote them deliberately for the enforce snippet + log line).
    """
    vcl = recv_snippet("Svc", _TEST_SECRET)
    # Each of the six client-controllable scoring headers must be unset.
    # NB: X-Edge-Score (no subfield syntax) — the bare header, not the
    # subfield-bearing one used internally. X-Edge-Shield-Auth must
    # also be in the scrub list so a client cannot pre-set it.
    required_unsets = (
        "unset req.http.X-Edge-Shield-Auth",
        "unset req.http.X-Edge-Scoring-Pass",
        "unset req.http.X-Edge-Score",
        "unset req.http.X-Edge-Sid",
        "unset req.http.X-Edge-Score-Enforce",
        "unset req.http.X-Edge-Score-Reason",
        "unset req.http.X-Edge-Score-Set-Cookie",
    )
    for unset in required_unsets:
        assert unset in vcl, f"missing client-edge scrub: {unset!r}"
    # The unsets must live inside the client-edge boundary if-block —
    # i.e. each unset's position must come AFTER the boundary guard.
    boundary_idx = vcl.find(f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"')
    assert boundary_idx != -1, "shield-auth boundary guard missing from recv"
    for unset in required_unsets:
        assert vcl.find(unset) > boundary_idx, (
            f"{unset!r} appears BEFORE the shield-auth boundary guard; "
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
    vcl = recv_snippet("Svc", "test_secret_abc123")
    assert "req.restarts == 1 && req.http.x-edge-score" in vcl
    assert "set var.fastly_req_do_shield = true;" in vcl


def test_recv_sets_scoring_pass_marker_then_pass():
    """The X-Edge-Scoring-Pass marker is the discriminator pass/fetch/
    deliver snippets all read. recv sets it just before return(pass)."""
    vcl = recv_snippet("Svc", "test_secret_abc123")
    assert 'set req.http.X-Edge-Scoring-Pass = "1";' in vcl
    assert "return(pass);" in vcl


def test_recv_does_not_set_auth_or_service_id_headers():
    """Auth + service-id headers move to vcl_pass (the canonical place
    for bereq mutations when recv used return(pass))."""
    vcl = recv_snippet("MyServiceId", "test_secret_abc123")
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
    vcl = recv_snippet("Svc", _TEST_SECRET)
    assert "!fastly.ddos_detected" in vcl
    # Must appear inside the SAME if block as the other recv gates — not
    # in a separate clause that could route some attack requests to
    # Compute when the other gates also pass. The edge-boundary gate is
    # now the shield-auth secret comparison (034).
    assert f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"' in vcl
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
    vcl = recv_snippet("Svc", "test_secret_abc123")
    assert "set req.http.X-Edge-Prev-Route" not in vcl
    assert "set req.http.X-Edge-Last-Route" not in vcl


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
    vcl = deliver_snippet(_TEST_SECRET)
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
    vcl = deliver_snippet(_TEST_SECRET)
    assert "} else {" in vcl
    assert 'set req.http.x-edge-score:score = "0";' in vcl
    assert 'set req.http.x-edge-score:compliance = "unknown";' in vcl
    assert 'set req.http.x-edge-score:reason = "compute-unavailable";' in vcl


def test_deliver_pass_1_only_fires_under_scoring_pass_marker():
    """X-Edge-Scoring-Pass discriminates pass-1 (scoring sub-fetch)
    from pass-2 (real origin). Pass-1 unsets the marker before issuing
    the naked restart, so pass-2 sees it gone."""
    vcl = deliver_snippet(_TEST_SECRET)
    assert 'if (req.http.X-Edge-Scoring-Pass == "1")' in vcl
    assert "unset req.http.X-Edge-Scoring-Pass;" in vcl


def test_deliver_pass_1_stashes_cookie_as_subfield():
    """The rotated cookie from the scorer gets stashed in a subfield
    too so it can be re-emitted in pass-2 with `add resp.http.Set-Cookie`."""
    vcl = deliver_snippet(_TEST_SECRET)
    assert "set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;" in vcl


def test_deliver_uses_naked_restart_not_return_restart():
    """The canonical preflight pattern uses a bare `restart;` from
    vcl_deliver (NOT `return(restart);`). The two have the same effect
    in this position, but the bare form matches the documented Fastly
    example."""
    vcl = deliver_snippet(_TEST_SECRET)
    assert "\n  restart;\n" in vcl
    assert "return(restart)" not in vcl


def test_deliver_pass_1_strips_scorer_response_headers():
    """The scorer's resp.http.x-edge-* headers must NOT reach the
    client. Even though the restart short-circuits to a different
    response, defense-in-depth says unset them in case routing changes."""
    vcl = deliver_snippet(_TEST_SECRET)
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
    unspoofable shield-auth secret comparison (034) so only the edge
    node emits — shield nodes would otherwise produce a duplicate
    Set-Cookie when the request hops shield → edge."""
    vcl = deliver_snippet(_TEST_SECRET)
    assert f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"' in vcl
    assert "fastly.ff.visits_this_service == 0" not in vcl
    assert 'req.http.x-edge-score:set-cookie != ""' in vcl
    assert "add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;" in vcl
    # Specifically NOT `set` (which would overwrite origin's cookie).
    assert "set resp.http.Set-Cookie =" not in vcl


# ── miss snippet ─────────────────────────────────────────────────────────────


def test_miss_unsets_inbound_score_header_anti_poisoning():
    """Defense in depth: when forwarding to the real origin on pass-2
    miss, strip any x-edge-score the client might have injected so it
    can't poison the origin's view of the request."""
    vcl = miss_snippet(_TEST_SECRET)
    assert "unset bereq.http.x-edge-score;" in vcl


def test_miss_unsets_scoring_pass_marker():
    """The internal X-Edge-Scoring-Pass marker stays internal — don't
    forward it to the real origin."""
    vcl = miss_snippet(_TEST_SECRET)
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

    034: the edge-only check is now the unspoofable shield-auth secret
    comparison rather than ``fastly.ff.visits_this_service == 0``.
    """
    vcl = enforce_snippet(_TEST_SECRET)
    # (a) Conjunction order — find the `if (` predicate (skip past any
    #     comment lines that mention the same tokens) then verify the
    #     three guards appear in order inside that predicate.
    if_idx = vcl.find("if (")
    assert if_idx != -1, "enforce snippet missing top-level if predicate"
    predicate_tail = vcl[if_idx:]
    edge_idx = predicate_tail.find(f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"')
    restart_idx = predicate_tail.find("req.restarts == 1")
    enforce_idx = predicate_tail.find('req.http.x-edge-score:enforce == "1"')
    assert edge_idx != -1, "shield-auth edge guard missing from if predicate"
    assert restart_idx != -1, "req.restarts == 1 guard missing from if predicate"
    assert enforce_idx != -1, "enforce subfield guard missing from if predicate"
    assert edge_idx < restart_idx < enforce_idx, (
        "enforce snippet guards must appear in order: "
        "shield-auth, req.restarts == 1, "
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
    # The old, spoofable boundary must NOT come back.
    assert "fastly.ff.visits_this_service == 0" not in vcl


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
    assert 'error 429 "Too Many Requests"' in enforce_snippet(_TEST_SECRET)

    # Common overrides each emit code + standard reason phrase.
    for code, phrase in [
        (403, "Forbidden"),
        (451, "Unavailable For Legal Reasons"),
        (503, "Service Unavailable"),
    ]:
        snippet = enforce_snippet(_TEST_SECRET, code)
        assert f'error {code} "{phrase}"' in snippet, f'enforce_snippet({code}) should emit `error {code} "{phrase}"`'
        # Guard conditions must stay exactly the same regardless of code.
        # 034: edge boundary is now the shield-auth secret comparison.
        assert f'req.http.X-Edge-Shield-Auth != "{_TEST_SECRET}"' in snippet
        assert "req.restarts == 1" in snippet
        assert 'req.http.x-edge-score:enforce == "1"' in snippet

    # Unusual but IANA-registered code (418 is registered in RFC 2324).
    assert 'error 418 "I\'m a Teapot"' in enforce_snippet(_TEST_SECRET, 418)
    # Non-standard code → generic "Blocked" reason.
    assert 'error 599 "Blocked"' in enforce_snippet(_TEST_SECRET, 599)

    # Out-of-range input → falls back to default (defense in depth).
    assert 'error 429 "Too Many Requests"' in enforce_snippet(_TEST_SECRET, 99)
    assert 'error 429 "Too Many Requests"' in enforce_snippet(_TEST_SECRET, 600)


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
    vcl = deliver_snippet(_TEST_SECRET)
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


def test_generate_bakes_secret_into_pass_snippet():
    """The shared request_secret travels via the X-Edge-Scorer-Auth
    header that pass_snippet sets on bereq — the scorer 401s on
    mismatch. 034 also bakes the same secret into recv/deliver/miss/
    enforce as part of the X-Edge-Shield-Auth boundary check; the
    only snippet that genuinely never sees it is fetch."""
    snippets = generate_scoring_vcl("Svc", "deadbeef1234")
    assert "deadbeef1234" in snippets[SCORING_PASS_NAME]
    # 034: recv/deliver/miss/enforce now also bake the secret into the
    # shield-auth boundary check.
    assert "deadbeef1234" in snippets[SCORING_RECV_NAME]
    assert "deadbeef1234" in snippets[SCORING_DELIVER_NAME]
    assert "deadbeef1234" in snippets[SCORING_MISS_NAME]
    assert "deadbeef1234" in snippets[SCORING_ENFORCE_NAME]
    # Fetch is the one snippet that doesn't reference the secret.
    assert "deadbeef1234" not in snippets[SCORING_FETCH_NAME]


def test_generate_is_pure_no_randomness():
    """Two calls with the same args return identical strings — needed
    for the ensure_vcl_snippet diff to recognize 'already up to date'
    and skip a redundant API call on re-deploy."""
    a = generate_scoring_vcl("Svc", "test_secret_abc123")
    b = generate_scoring_vcl("Svc", "test_secret_abc123")
    assert a == b
