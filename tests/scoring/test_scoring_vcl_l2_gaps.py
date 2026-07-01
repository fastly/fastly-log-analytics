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

from __future__ import annotations

from backend.provision.session_scoring_vcl import (
    deliver_snippet,
    generate_scoring_vcl,
)

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
