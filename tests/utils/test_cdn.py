"""Tests for backend.utils.cdn.

URL/Request construction for the CDN-fronted ingest path. Two functions
to cover: ``build_cdn_url`` (string concatenation with proper URL
encoding) and ``cdn_request`` (decides between query-param and header
auth). Both are pure — no network, no fixtures.
"""

from __future__ import annotations

import urllib.parse

import pytest

from backend.utils.cdn import build_cdn_url, cdn_request

# ── build_cdn_url ────────────────────────────────────────────────────────────


def test_build_url_appends_key_to_base():
    url = build_cdn_url("https://cdn.example.com", "raw/2026-05-15/00/log.gz")
    assert url == "https://cdn.example.com/raw/2026-05-15/00/log.gz"


def test_build_url_strips_trailing_slash_on_base():
    """A trailing ``/`` on the base must not produce a double slash."""
    url = build_cdn_url("https://cdn.example.com/", "key")
    assert url == "https://cdn.example.com/key"


def test_build_url_encodes_special_characters_in_key():
    """Spaces, plus signs, queries-in-keys must be percent-encoded —
    otherwise the URL parses with a fake ``?`` separator."""
    url = build_cdn_url("https://cdn.example.com", "weird key+with?stuff")
    # The leading slash on the path is preserved (safe="/=" in the impl);
    # spaces become %20, + becomes %2B, ? becomes %3F.
    assert "weird%20key%2Bwith%3Fstuff" in url


def test_build_url_preserves_slashes_in_key():
    """Keys are file paths — slashes are structural, not characters to encode."""
    url = build_cdn_url("https://cdn.example.com", "a/b/c/d.gz")
    assert url == "https://cdn.example.com/a/b/c/d.gz"


def test_build_url_adds_secret_as_key_query_param():
    url = build_cdn_url("https://cdn.example.com", "file.gz", secret="my-secret")
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get("key") == ["my-secret"]


def test_build_url_no_secret_means_no_key_query_param():
    url = build_cdn_url("https://cdn.example.com", "file.gz")
    parsed = urllib.parse.urlparse(url)
    assert parsed.query == ""


def test_build_url_preserves_existing_base_query_params():
    """If the base URL already has ``?foo=bar``, adding the secret must
    NOT clobber it — the impl uses parse_qs / urlencode for safety."""
    url = build_cdn_url("https://cdn.example.com/?foo=bar", "file.gz", secret="s")
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get("foo") == ["bar"]
    assert qs.get("key") == ["s"]


def test_build_url_secret_with_special_chars_is_url_encoded():
    """A secret containing ``&`` or ``=`` would break naive concatenation —
    parse/urlencode round-trip must produce a parseable URL."""
    url = build_cdn_url("https://cdn.example.com", "file.gz", secret="a&b=c")
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    assert qs.get("key") == ["a&b=c"]  # parse_qs decodes it back cleanly


# ── cdn_request ──────────────────────────────────────────────────────────────


def test_request_query_auth_default_path():
    """Default mode: secret on URL as ``?key=`` query param, no header."""
    req = cdn_request("https://cdn.example.com", "file.gz", secret="s")
    assert "key=s" in req.full_url
    assert "x-fastly-key" not in dict(req.header_items())


def test_request_header_auth_mode():
    """``use_header_auth=True`` puts the secret in the ``x-fastly-key``
    header and KEEPS it OUT of the URL — the VCL accepts either."""
    req = cdn_request("https://cdn.example.com", "file.gz", secret="s", use_header_auth=True)
    assert "key=s" not in req.full_url
    headers = {k.lower(): v for k, v in req.header_items()}
    assert headers.get("x-fastly-key") == "s"


def test_request_header_auth_with_no_secret_falls_back_to_query():
    """If header-auth is requested but no secret is supplied, the impl
    falls through to the query path (with empty secret → no key param)."""
    req = cdn_request("https://cdn.example.com", "file.gz", use_header_auth=True)
    assert "key=" not in req.full_url
    assert "x-fastly-key" not in dict(req.header_items())


def test_request_url_is_built_from_base_and_key():
    """End-to-end: the produced URL should match what build_cdn_url
    would have produced — no divergence between the two helpers."""
    expected = build_cdn_url("https://cdn.example.com", "a/b.gz", secret="x")
    req = cdn_request("https://cdn.example.com", "a/b.gz", secret="x")
    assert req.full_url == expected


@pytest.mark.parametrize("use_header_auth", [True, False])
def test_request_no_secret_no_auth_anywhere(use_header_auth):
    """Without a secret AND without header-auth, the request is bare."""
    req = cdn_request("https://cdn.example.com", "file.gz", use_header_auth=use_header_auth)
    assert "key=" not in req.full_url
    assert "x-fastly-key" not in dict(req.header_items())
