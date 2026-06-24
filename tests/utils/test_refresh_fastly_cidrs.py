"""Tests for ``scripts/refresh_fastly_cidrs.py``.

The script's three pure pieces — fetch (mocked transport), sort, rewrite —
are exercised against a captured Fastly response and a sample Caddyfile
snippet. No real network calls.
"""

from __future__ import annotations

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
