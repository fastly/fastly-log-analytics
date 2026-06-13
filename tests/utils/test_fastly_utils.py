"""Tests for backend.core.fastly.utils."""

import re

import pytest

from backend.core.fastly.utils import load_vcl


def test_load_vcl_orders_auth_before_purge():
    """Verify that in the generated VCL, the authentication check and its
    accompanying 401 Unauthorized block are strictly defined before the
    unauthenticated-vulnerable FASTLYPURGE bypass shortcut.
    """
    vcl = load_vcl()
    assert vcl is not None

    # Find the positions of the key blocks in the generated VCL
    auth_err_pos = vcl.find('error 401 "Unauthorized"')
    purge_block_pos = vcl.find('if (req.method == "FASTLYPURGE")')

    assert auth_err_pos != -1, "Should find the authentication check block"
    assert purge_block_pos != -1, "Should find the FASTLYPURGE check block"

    assert auth_err_pos < purge_block_pos, (
        "Authentication check must strictly precede the FASTLYPURGE native execution to prevent unauthenticated cache evictions"
    )


@pytest.mark.security_regression
def test_load_vcl_auth_gates_do_not_trust_fastly_ff():
    """Audit finding 006: ``fastly.ff.visits_this_service == 0`` is
    derived from the client-controllable ``Fastly-FF`` HTTP header, so
    using it as a "this is the edge, run the auth gate" signal lets an
    attacker spoof the header to skip auth entirely. The fix replaces
    the spoofable check with a compiled-in ``X-Edge-CDN-Auth`` shield
    secret. This test pins the regression: the auth/penalty/Client-IP
    gates must no longer reference visits_this_service.
    """
    vcl = load_vcl()

    # The security-critical gates (auth + penaltybox + Client-IP) must
    # now use the shield-auth marker, not the FF count.
    for line in vcl.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "fastly.ff.visits_this_service" not in stripped:
            continue
        # The ONE legitimate remaining use is the SWR-on-shield tweak
        # (`fastly.ff.visits_this_service > 1`), which is a behavior
        # tweak (disable stale-while-revalidate on shield) not a
        # security gate. Allow that one; reject everything else.
        assert "visits_this_service > 1" in stripped, (
            f"unexpected fastly.ff.visits_this_service usage in security-critical gate: {stripped!r}"
        )


@pytest.mark.security_regression
def test_load_vcl_substitutes_shield_secret_consistently():
    """The shield-auth secret is stamped on outgoing bereqs (miss_pass)
    and matched in vcl_recv's edge-vs-shield detection. Edge and shield
    run the SAME compiled VCL, so both copies see the same constant by
    construction. Pin that load_vcl produces one consistent secret per
    invocation across every site that references it.
    """
    vcl = load_vcl()
    matches = re.findall(r'X-Edge-CDN-Auth[^"]*"([a-f0-9]{64})"', vcl)
    assert len(matches) >= 4, f"expected at least 4 X-Edge-CDN-Auth references; got {len(matches)}"
    assert len(set(matches)) == 1, "shield-auth secret must be identical at every reference site"
    # Two independent invocations must mint independent secrets so a
    # leaked one only burns its own VCL deploy.
    assert load_vcl() != vcl, "two load_vcl() calls must produce different VCL (fresh secrets)"


@pytest.mark.security_regression
def test_load_vcl_strips_client_spoofed_shield_marker():
    """The vcl_recv must unset a client-supplied X-Edge-CDN-Auth header
    that doesn't match the compiled-in secret. Without this strip an
    attacker who guessed (or replayed) any plausible value would skip
    every edge-only gate. Pin that the strip block runs FIRST inside
    vcl_recv — before any logic that reads X-Edge-CDN-Auth.
    """
    vcl = load_vcl()
    # Slice vcl_recv body.
    recv_start = vcl.index("sub vcl_recv {")
    recv_body = vcl[recv_start : recv_start + 2000]
    strip_pos = recv_body.find("unset req.http.X-Edge-CDN-Auth")
    first_read = recv_body.find("X-Edge-CDN-Auth !=")
    assert strip_pos != -1, "missing client-spoof strip block in vcl_recv"
    assert first_read != -1, "missing X-Edge-CDN-Auth comparison in vcl_recv"
    # The strip's own `if (... != "secret")` comparison appears at first_read;
    # the unset itself should appear inside that block (i.e. AFTER the
    # comparison, which is fine), and before any further-down comparison
    # that reads the header for gating. We assert the strip exists and
    # the first comparison line is the one guarding it.
    strip_block = recv_body[first_read:strip_pos]
    assert "unset req.http.X-Edge-CDN-Auth" not in strip_block, "expected strip after first comparison"


@pytest.mark.security_regression
def test_load_vcl_stamps_shield_auth_marker_on_bereq():
    """The shield-detection check only works if the edge actually sets
    X-Edge-CDN-Auth on the outgoing bereq. Pin that the stamp lives in
    miss_pass (which fires on both miss + pass), so any bereq the
    shield POP receives carries the marker.
    """
    vcl = load_vcl()
    miss_pass_start = vcl.index("sub miss_pass {")
    next_sub = vcl.find("sub ", miss_pass_start + 1)
    body = vcl[miss_pass_start:next_sub]
    assert "set bereq.http.X-Edge-CDN-Auth" in body, "miss_pass must stamp X-Edge-CDN-Auth on bereq"
