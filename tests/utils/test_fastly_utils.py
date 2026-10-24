"""Consolidated tests for test_fastly_utils."""

from __future__ import annotations

import pytest

from backend.core.fastly.utils import load_vcl, parse_period, region_endpoint


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
        ("eu-central-1", "eu-central-1.object.fastlystorage.app"),
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


# ── Merged from test_fastly_mock_mode.py ──────────────────────────────────────────────

from backend.core.fastly.client import fastly
from backend.core.fastly.mock_fixtures import is_mock_mode, mock_response
from backend.utils.ngwaf import fetch_verified_bots_paged


def test_is_mock_mode_reads_env(monkeypatch):
    monkeypatch.delenv("FASTLY_MOCK_MODE", raising=False)
    assert is_mock_mode() is False
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    assert is_mock_mode() is True
    monkeypatch.setenv("FASTLY_MOCK_MODE", "0")
    assert is_mock_mode() is False


def test_mock_response_post_service_returns_id_and_name():
    out = mock_response("POST", "/service", {"name": "Test CDN"})
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Test CDN"
    assert out["version"] == 1


def test_mock_response_get_service_returns_empty_list():
    # find_service_by_name pages this — empty means "name is novel".
    assert mock_response("GET", "/service") == []


def test_mock_response_unknown_endpoint_returns_ok_envelope():
    # Default catch-all — keeps an unfamiliar journey moving without
    # making up specific fields a future test could false-pin against.
    assert mock_response("GET", "/totally/uncovered/endpoint") == {"ok": True}


def test_mock_response_access_key_create_shape():
    # The mock returns the FLAT shape `ensure_fos_access_key` reads
    # (`key["access_key"]` / `key["secret_key"]`), not the JSON:API
    # `data.attributes.*` wrapper. Mirrors the orchestrator's
    # actual access pattern (backend/provision/fos_setup.py:368-376).
    out = mock_response("POST", "/resources/object-storage/access-keys", {"description": "x"})
    assert out["access_key"] == "AKIA_MOCK"
    assert out["secret_key"] == "SECRET_MOCK"  # gitleaks:allow — canned mock secret, not real
    assert out["description"] == "x"


def test_fastly_client_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode the production fastly() must skip the real urlopen
    path entirely and return the fixture shape. Sanity: any token works."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    out = fastly("POST", "/service", {"name": "Smoke"}, token="any-token")
    assert out["id"] == "mock-svc-id"
    assert out["name"] == "Smoke"


def test_ngwaf_short_circuits_in_mock_mode(monkeypatch):
    """In mock mode fetch_verified_bots_paged yields one empty page and
    stops — no real /ngwaf/v1/workspaces/:id/requests call is made."""
    monkeypatch.setenv("FASTLY_MOCK_MODE", "1")
    pages = list(
        fetch_verified_bots_paged(
            api_key="any",
            workspace_id="ws-mock",
            from_ts="2026-06-01T00:00:00Z",
        )
    )
    assert pages == [([], None, 0)]


# ── Merged from test_refresh_fastly_cidrs.py ──────────────────────────────────────────────

import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "refresh_fastly_cidrs.py"


def _load_module():
    """Load ``refresh_fastly_cidrs`` from scripts/ as an importable module."""
    spec = importlib.util.spec_from_file_location("refresh_fastly_cidrs", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["refresh_fastly_cidrs"] = module
    spec.loader.exec_module(module)
    return module


refresh = _load_module()


# Captured 2026-06-03 Fastly public-ip-list payload (trimmed; ipv6 retained
# so we exercise the "ignore v6" path of the script).
SAMPLE_FASTLY_RESPONSE = {
    "addresses": [
        "151.101.0.0/16",
        "23.235.32.0/20",
        "43.249.72.0/22",
        "199.232.0.0/16",
        "146.75.0.0/17",
    ],
    "ipv6_addresses": [
        "2a04:4e40::/32",
        "2a04:4e42::/32",
    ],
}

SAMPLE_CADDYFILE = """\
:80 {
\timport security_headers

\t@from_fastly_v4 {
\t\tremote_ip 1.1.1.0/24 2.2.2.0/24
\t}

\trequest_header @from_fastly_v4 X-Forwarded-For {http.request.header.Fastly-Client-IP}
}
"""


def test_sort_cidrs_orders_by_octet_then_prefix():
    """Sort must produce a deterministic, human-readable order so refreshes
    with no upstream change are a no-op (idempotency)."""
    out = refresh.sort_cidrs(["199.232.0.0/16", "23.235.32.0/20", "151.101.0.0/16"])
    assert out == ["23.235.32.0/20", "151.101.0.0/16", "199.232.0.0/16"]


def test_rewrite_caddyfile_replaces_remote_ip_line_only():
    """The rewrite must touch only the remote_ip line — surrounding
    Caddyfile bytes (matcher name, braces, other directives) stay verbatim.
    Pinned because a botched rewrite could silently delete the request_header
    line and disable the whole Fastly-Client-IP propagation."""
    cidrs = ["23.235.32.0/20", "151.101.0.0/16"]
    out = refresh.rewrite_caddyfile(SAMPLE_CADDYFILE, cidrs)

    assert "remote_ip 23.235.32.0/20 151.101.0.0/16" in out
    # Original CIDRs are gone.
    assert "1.1.1.0/24" not in out
    # Surrounding lines untouched.
    assert "import security_headers" in out
    assert "request_header @from_fastly_v4 X-Forwarded-For" in out
    # Tab indentation preserved.
    assert "\t\tremote_ip" in out


def test_rewrite_caddyfile_is_idempotent():
    """Running twice with the same CIDR list must converge — second pass
    is a no-op. Guards against the script appending instead of replacing."""
    cidrs = ["23.235.32.0/20", "151.101.0.0/16"]
    once = refresh.rewrite_caddyfile(SAMPLE_CADDYFILE, cidrs)
    twice = refresh.rewrite_caddyfile(once, cidrs)
    assert once == twice


def test_rewrite_caddyfile_raises_when_matcher_missing():
    """If somebody renames the matcher the script must fail loud, not
    silently leave the Caddyfile unchanged."""
    with pytest.raises(RuntimeError, match="@from_fastly_v4"):
        refresh.rewrite_caddyfile(":80 {\n\tlog\n}\n", ["1.1.1.0/24"])


def test_fetch_fastly_cidrs_parses_v4_only_and_sorts():
    """Mocked transport — exercises JSON parsing + sort without network."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == refresh.FASTLY_PUBLIC_IP_LIST
        return httpx.Response(200, content=json.dumps(SAMPLE_FASTLY_RESPONSE).encode())

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        out = refresh.fetch_fastly_cidrs(client=client)

    # Sorted by octet; ipv6 dropped entirely.
    assert out == [
        "23.235.32.0/20",
        "43.249.72.0/22",
        "146.75.0.0/17",
        "151.101.0.0/16",
        "199.232.0.0/16",
    ]


def test_fetch_fastly_cidrs_rejects_empty_v4_list():
    """Empty allow-list would lock the matcher to nothing — refuse loudly
    instead of writing an empty CIDR set into production."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'{"addresses": [], "ipv6_addresses": []}')

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="empty allow-list"):
            refresh.fetch_fastly_cidrs(client=client)


# ── Merged from test_pop_utils.py ──────────────────────────────────────────────

from unittest.mock import patch

import pytest

from backend.utils import pop_utils


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    cache = tmp_path / "pop_locations.json"
    monkeypatch.setattr(pop_utils, "CACHE_FILE", str(cache))
    return cache


# ── fetch_pop_locations ──────────────────────────────────────────────────────


def test_fetch_returns_false_for_empty_api_key(isolated_cache):
    assert pop_utils.fetch_pop_locations("") is False
    assert pop_utils.fetch_pop_locations(None) is False  # type: ignore[arg-type]
    # Nothing was written
    assert not isolated_cache.exists()


def test_fetch_writes_cache_on_success(isolated_cache):
    sample = [{"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}}]

    with patch("backend.core.fastly.client.fastly", return_value=sample):
        ok = pop_utils.fetch_pop_locations("api-key")

    assert ok is True
    assert isolated_cache.exists()
    assert json.loads(isolated_cache.read_text()) == sample


def test_fetch_returns_false_on_network_error(isolated_cache):
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("boom")):
        ok = pop_utils.fetch_pop_locations("api-key")
    assert ok is False
    assert not isolated_cache.exists()


def test_fetch_passes_api_key_to_fastly_client(isolated_cache):
    """The Fastly-Key header is what the API authenticates on; fastly()
    sets it from the token kwarg. Verify the kwarg is threaded through —
    a missing/wrong token returns the API's public dataset (different
    shape) and silently breaks the cache.
    """
    with patch("backend.core.fastly.client.fastly", return_value=[]) as mock_fastly:
        pop_utils.fetch_pop_locations("my-secret-token")

    mock_fastly.assert_called_once_with("GET", "/datacenters", token="my-secret-token")


# ── get_pop_locations ────────────────────────────────────────────────────────


def test_get_returns_empty_when_cache_missing(isolated_cache):
    assert pop_utils.get_pop_locations() == []


def test_get_returns_cached_data(isolated_cache):
    data = [{"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}}]
    isolated_cache.write_text(json.dumps(data))
    assert pop_utils.get_pop_locations() == data


def test_get_returns_empty_for_corrupt_cache(isolated_cache):
    """A truncated or non-JSON file must not raise — just return empty."""
    isolated_cache.write_text("{ not valid json")
    assert pop_utils.get_pop_locations() == []


# ── get_pop_lat_lon_map ──────────────────────────────────────────────────────


def test_lat_lon_map_extracts_coords_per_code(isolated_cache):
    data = [
        {"code": "JFK", "coordinates": {"latitude": 40.6, "longitude": -73.8}},
        {"code": "LHR", "coordinates": {"latitude": 51.5, "longitude": -0.1}},
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"JFK": (40.6, -73.8), "LHR": (51.5, -0.1)}


def test_lat_lon_map_skips_entries_missing_required_fields(isolated_cache):
    """A POP without coordinates (or with missing latitude/longitude) must
    be silently skipped — the alternative is a crash in the consumer."""
    data = [
        {"code": "GOOD", "coordinates": {"latitude": 1.0, "longitude": 2.0}},
        {"code": "NO_COORDS"},  # no coordinates key
        {"code": "PARTIAL", "coordinates": {"latitude": 3.0}},  # missing longitude
        {"coordinates": {"latitude": 4.0, "longitude": 5.0}},  # no code
    ]
    isolated_cache.write_text(json.dumps(data))
    got = pop_utils.get_pop_lat_lon_map()
    assert got == {"GOOD": (1.0, 2.0)}


def test_lat_lon_map_returns_empty_for_non_list_cache(isolated_cache):
    """Defensive: if the cache ever contains a dict instead of a list
    (legacy format, manual edit, etc.), don't crash."""
    isolated_cache.write_text(json.dumps({"some": "object"}))
    assert pop_utils.get_pop_lat_lon_map() == {}


# ── Merged from test_geo.py ──

from backend.utils.countries import format_city_label


class TestFormatCityLabel:
    def test_full_label(self):
        result = format_city_label("Portland", "US", "OR")
        assert result == "Portland, OR, United States"

    def test_no_region(self):
        result = format_city_label("London", "GB")
        assert result in ("London, United Kingdom", "London, GB")

    def test_country_only(self):
        result = format_city_label("", "US")
        assert result in ("United States", "US")

    def test_unknown_country_code_uppercased(self):
        result = format_city_label("Metropolis", "ZZ")
        assert "Metropolis" in result
        assert "ZZ" in result

    def test_all_empty_returns_unknown(self):
        assert format_city_label("", "", "") == "Unknown"

    def test_city_title_cased(self):
        result = format_city_label("new york", "US")
        assert result.startswith("New York")

    def test_region_uppercased(self):
        result = format_city_label("Lyon", "FR", "ara")
        assert ", ARA" in result

    def test_none_like_empty_strings(self):
        # The function coerces falsy to "" via `city = city or ""`
        result = format_city_label("", "", "")
        assert result == "Unknown"

    def test_country_without_city_or_region(self):
        result = format_city_label("", "DE", "")
        assert result in ("Germany", "DE")

    def test_country_code_case_insensitive(self):
        lower = format_city_label("Berlin", "de")
        upper = format_city_label("Berlin", "DE")
        assert lower == upper


# ── Merged from test_provision_utils.py ──────────────────────────────────────────────


import pytest

from backend.provision import utils as pu

# ── _highlight: regex-driven quote highlighting ──────────────────────────────


def test_highlight_wraps_single_quoted_substring_in_magenta():
    out = pu._highlight("created service 'svc-123'")
    assert pu.MAG in out  # magenta colour wraps the quoted name
    assert "svc-123" in out
    assert pu.RST in out


def test_highlight_leaves_already_coloured_strings_alone():
    """If the message already contains ANSI escapes, don't double-wrap.
    Otherwise the second pass would corrupt the colour reset codes."""
    pre_coloured = f"{pu.GRN}already coloured{pu.RST}"
    assert pu._highlight(pre_coloured) == pre_coloured


def test_highlight_handles_message_without_quotes():
    """No quotes → no change other than the wrapping the regex would
    have made. Pinned because the regex must be a no-op, not crash."""
    assert pu._highlight("no quotes here") == "no quotes here"


# ── ask: prompt + default handling ───────────────────────────────────────────


def test_ask_returns_user_input_when_provided():
    with patch("builtins.input", return_value="user-value"):
        assert pu.ask("what?") == "user-value"


def test_ask_returns_default_on_empty_input():
    """Bare Enter → fall back to default. Pinned because provisioning
    relies heavily on ``[default]`` prompts to skip past boilerplate."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("region?", default="us-east-1") == "us-east-1"


def test_ask_returns_empty_string_when_no_default_and_no_input():
    """No default and no input → empty string (not None) so callers can
    safely call ``.strip()``, ``len()``, etc. without a None-check."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("optional?") == ""


def test_ask_strips_whitespace_from_input():
    with patch("builtins.input", return_value="  value  "):
        assert pu.ask("what?") == "value"


# ── ask_yes: Y/n parsing ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "answer,expected",
    [
        ("y", True),
        ("Y", True),
        ("yes", True),
        ("YES", True),
        ("n", False),
        ("N", False),
        ("no", False),
        ("NO", False),
        ("", True),  # default=True (the default default)
    ],
)
def test_ask_yes_with_default_true(answer, expected):
    with patch("builtins.input", return_value=answer):
        assert pu.ask_yes("ok?") is expected


def test_ask_yes_default_false_returns_false_on_empty():
    with patch("builtins.input", return_value=""):
        assert pu.ask_yes("ok?", default=False) is False


def test_ask_yes_non_y_prefix_treated_as_no():
    """Anything not starting with 'y' is false. Pinned because a
    permissive interpretation here would let a typo (``q``, ``ok``)
    accidentally confirm a destructive action."""
    with patch("builtins.input", return_value="maybe"):
        assert pu.ask_yes("destroy?", default=False) is False


# ── ask_int: validation retry loop ───────────────────────────────────────────


def test_ask_int_returns_parsed_integer():
    with patch("builtins.input", return_value="42"):
        assert pu.ask_int("n?", default=10) == 42


def test_ask_int_returns_default_on_empty_input():
    with patch("builtins.input", return_value=""):
        assert pu.ask_int("n?", default=7) == 7


def test_ask_int_retries_on_non_integer():
    """First input is garbage → loop, second is valid → return. The
    operator should never get a Python traceback for a typo."""
    inputs = iter(["abc", "9"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0) == 9


def test_ask_int_rejects_below_min_then_accepts():
    inputs = iter(["3", "10"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, min_val=5) == 10


def test_ask_int_rejects_above_max_then_accepts():
    inputs = iter(["999", "50"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, max_val=100) == 50


# ── Print helpers: structural pinning (no escape-code lock-in) ──────────────


def test_fail_writes_to_stderr(capsys):
    """``fail`` goes to stderr so the CLI ``set -o pipefail`` users
    catch errors. The other helpers default to stdout."""
    pu.fail("boom")
    captured = capsys.readouterr()
    assert "boom" in captured.err
    assert captured.out == ""


def test_ok_writes_to_stdout(capsys):
    pu.ok("done")
    captured = capsys.readouterr()
    assert "done" in captured.out
    assert captured.err == ""


def test_blank_emits_newline(capsys):
    pu.blank()
    assert capsys.readouterr().out == "\n"


def test_step_includes_progress_and_title(capsys):
    pu.step(2, 5, "Configure Fastly")
    out = capsys.readouterr().out
    assert "[2/5]" in out
    assert "Configure Fastly" in out


def test_banner_draws_separator_around_title(capsys):
    pu.banner("Welcome")
    out = capsys.readouterr().out
    assert "Welcome" in out
    # Bar uses U+2501 BOX DRAWINGS HEAVY HORIZONTAL repeated 64 times
    assert "━" * 10 in out


# ── _c: trivial wrapper, exercised here so it's not "uncovered" ─────────────


def test_c_wraps_text_with_colour_and_reset():
    wrapped = pu._c(pu.GRN, "hello")
    assert wrapped.startswith(pu.GRN)
    assert wrapped.endswith(pu.RST)
    assert "hello" in wrapped
