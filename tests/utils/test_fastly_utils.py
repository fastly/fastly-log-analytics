"""Tests for backend.core.fastly.utils."""

import argparse

import pytest

from backend.core.fastly.utils import int_range, load_vcl, parse_period, region_endpoint


def test_load_vcl_orders_auth_before_native_request_handling():
    """Verify the authentication check (and its 401 Unauthorized block) is
    strictly defined before Fastly's native request handling — the
    ``#FASTLY recv`` macro, which is where the FASTLYPURGE method is
    processed. This ordering prevents an unauthenticated client from
    triggering a cache eviction.
    """
    vcl = load_vcl()
    assert vcl is not None

    # FASTLYPURGE is handled by Fastly's native boilerplate (#FASTLY recv),
    # NOT a manual ``return(purge)`` — which is not a valid return action in
    # vcl_recv and is rejected by both falco and the Fastly compiler.
    assert "return(purge)" not in vcl, "vcl_recv must not use the invalid return(purge) action"

    auth_err_pos = vcl.find('error 401 "Unauthorized"')
    fastly_recv_pos = vcl.find("#FASTLY recv")

    assert auth_err_pos != -1, "Should find the authentication check block"
    assert fastly_recv_pos != -1, "Should find the #FASTLY recv macro (native FASTLYPURGE handling)"

    assert auth_err_pos < fastly_recv_pos, (
        "Authentication check must strictly precede Fastly's native request "
        "handling to prevent unauthenticated cache evictions"
    )


@pytest.mark.security_regression
def test_load_vcl_authenticates_only_at_the_edge():
    """The client-facing auth gate (the ``cdn_auth.secret`` ``?key=`` /
    ``x-fastly-key`` check), the Fastly-Client-IP override, and the
    penaltybox enforcement must run only on the first Fastly POP to see
    the request — gated on ``req.restarts == 0 && fastly.ff.visits_this_service == 0``.

    Rationale: a request reaching the shield with ``visits_this_service > 0``
    has already passed this gate at the edge (unauthenticated requests
    ``error 401`` at the edge and are never forwarded), so the shield must
    NOT re-run it — the ``key`` param is stripped before forwarding and a
    re-check would 401 a legitimate request. A client cannot forge
    ``visits_this_service`` (each ``Fastly-FF`` entry is a salted hash only
    genuine Fastly hops produce), so this is a sound trust boundary and the
    old ``X-Edge-CDN-Auth`` shield marker is no longer needed.
    """
    vcl = load_vcl()

    # The marker mechanism must be fully gone.
    assert "X-Edge-CDN-Auth" not in vcl, "X-Edge-CDN-Auth shield marker should be removed"

    # The auth 401 must live inside the edge-only guard.
    recv_start = vcl.index("sub vcl_recv {")
    recv_body = vcl[recv_start : vcl.index("sub vcl_hash")]
    edge_guard = "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {"
    guard_pos = recv_body.find(edge_guard)
    auth_401_pos = recv_body.find('error 401 "Unauthorized"')
    assert guard_pos != -1, "missing edge-only guard (req.restarts == 0 && visits_this_service == 0)"
    assert auth_401_pos != -1, "missing auth 401 block in vcl_recv"
    assert guard_pos < auth_401_pos, "auth gate must be inside the edge-only guard"
    # The auth gate must still check the client key from the cdn_auth dict.
    assert 'table.lookup(cdn_auth, "secret"' in recv_body, "auth gate must check cdn_auth.secret"


@pytest.mark.security_regression
def test_load_vcl_x_amz_date_rounds_seconds_to_minute_boundary():
    """Finding 006: a per-request ``x-amz-date`` makes every concurrent
    request to the same FOS object produce a unique SigV4 signature, which
    defeats Fastly's per-(URL, method) collapsed-forwarding / request
    coalescing — instead of one in-flight fetch serving an entire burst,
    every request forwards in parallel and the origin spend scales with
    incoming RPS instead of the cache-key cardinality.

    Pinning the seconds field to ``00`` in the strftime format string
    collapses all requests within the same UTC minute onto the SAME
    signature, restoring coalescing. Still well inside the SigV4 15-minute
    validity window. (VCL doesn't expose ``%`` or ``/`` operators on
    integers, so format-string rounding is the cleanest expression.)
    """
    vcl = load_vcl()
    amz_lines = [line for line in vcl.splitlines() if "x-amz-date" in line and "strftime" in line]
    assert amz_lines, "x-amz-date assignment line missing from VCL"
    assignment = amz_lines[0]
    # The fix replaces ``%S`` (per-second) with literal ``00`` so all
    # requests within a minute produce the same signed timestamp.
    assert "%H%M00Z" in assignment, (
        f"x-amz-date strftime format must pin seconds to 00 (finding 006); got: {assignment!r}"
    )
    # Negative: the pre-fix per-second ``%S`` form must NOT remain.
    assert 'strftime({"%Y%m%dT%H%M%SZ"}, now);' not in vcl, (
        "unrounded x-amz-date strftime still present — fix not applied"
    )


# ── region_endpoint: FOS host construction ─────────────────────────────────────


@pytest.mark.parametrize(
    "region,expected",
    [
        ("us-east-1", "us-east-1.object.fastlystorage.app"),
        ("eu-central", "eu-central.object.fastlystorage.app"),
    ],
)
def test_region_endpoint(region, expected):
    assert region_endpoint(region) == expected


# ── parse_period: input validation ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("60", 60),
        ("1 minute", 60),
        ("5 minutes", 300),
        ("5m", 300),
        ("5min", 300),
        ("  10  ", 10),
        ("2M", 120),
    ],
)
def test_parse_period_valid_inputs(raw, expected):
    assert parse_period(raw) == expected


@pytest.mark.parametrize("raw", ["", "abc", "12 hours", "5 sec", "minute"])
def test_parse_period_invalid_format_raises(raw):
    with pytest.raises(ValueError, match="Cannot parse period"):
        parse_period(raw)


# ── int_range: argparse type checker ───────────────────────────────────────────


def test_int_range_returns_callable():
    assert callable(int_range(0, 10))


@pytest.mark.parametrize("value", ["10", "15", "20"])
def test_int_range_accepts_values_in_bounds(value):
    checker = int_range(10, 20)
    out = checker(value)
    assert out == int(value)
    assert isinstance(out, int)


@pytest.mark.parametrize("value", ["abc", "1.5", ""])
def test_int_range_rejects_non_integer(value):
    checker = int_range(0, 100)
    with pytest.raises(argparse.ArgumentTypeError, match="Must be an integer"):
        checker(value)


@pytest.mark.parametrize("value", ["5", "9"])
def test_int_range_rejects_below_minimum(value):
    checker = int_range(10, 20)
    with pytest.raises(argparse.ArgumentTypeError, match="Must be between 10 and 20"):
        checker(value)


@pytest.mark.parametrize("value", ["21", "1000"])
def test_int_range_rejects_above_maximum(value):
    checker = int_range(10, 20)
    with pytest.raises(argparse.ArgumentTypeError, match="Must be between 10 and 20"):
        checker(value)


# ── load_vcl: rate_limiting=False strips the rate-limit blocks ─────────────────


def test_load_vcl_with_rate_limiting_false_strips_ratelimit_blocks():
    """When the deploy disables rate limiting (e.g. a service without
    the ratelimit add-on), the #RATELIMIT_BEGIN/END markers and their
    enclosed VCL must be removed — otherwise the compiled VCL would
    reference undefined `ratecounter`/`penaltybox` symbols and fail to
    upload."""
    vcl_with = load_vcl(rate_limiting=True)
    vcl_without = load_vcl(rate_limiting=False)

    assert "#RATELIMIT_BEGIN" in vcl_with
    assert "#RATELIMIT_END" in vcl_with
    assert "ratecounter auth_fail_rc" in vcl_with
    assert "penaltybox auth_fail_pb" in vcl_with

    assert "#RATELIMIT_BEGIN" not in vcl_without
    assert "#RATELIMIT_END" not in vcl_without
    assert "ratecounter auth_fail_rc" not in vcl_without
    assert "penaltybox auth_fail_pb" not in vcl_without
    # The non-ratelimit auth check (the `error 401` outside the marker
    # block) must still be present — only the rate-limit add-ons were
    # stripped.
    assert 'error 401 "Unauthorized"' in vcl_without
