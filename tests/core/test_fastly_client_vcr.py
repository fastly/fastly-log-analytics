"""vcrpy-driven Fastly API client tests.

Why: the MagicMock tests in [test_fastly_client.py](test_fastly_client.py)
pin call shape (patch ``urllib.request.urlopen`` and assert on args). A
refactor like ``urlopen(req)`` → ``urlopen(req, timeout=30)`` or wrapping
the call in a retry helper would silently invalidate those mocks. vcrpy
plays back at the socket layer so the transport above it can be
refactored freely.

Cassettes live under ``tests/cassettes/``. These tests run alongside the
MagicMock suite, not as a replacement.

TESTING_PLAN_3 item 8.

Re-record (R-11 freshness gate fires at 90 days):

    uv run pytest tests/core/test_fastly_client_vcr.py --vcr-record=all

The gate lives in ``tests/conftest.py::pytest_sessionstart`` and will
fail collection with a list of stale cassette paths if any one is
older than the threshold.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import vcr

from backend.core.fastly.client import fastly

CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "..", "cassettes")

_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def test_get_service_returns_parsed_json():
    """Happy path: 200 OK with a JSON body. The client must return the
    parsed dict, not the raw bytes."""
    with _my_vcr.use_cassette("fastly_get_service_success.yaml"):
        result = fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real")

    assert result["id"] == "SU3xxxxxxxxxxxxxx0000"
    assert result["name"] == "Test CDN"
    assert result["active_version"]["number"] == 42


def test_503_is_retried_then_succeeds():
    """The client's retry policy: 503 triggers an exponential-backoff
    retry. The cassette has 503 then 200; the call should succeed and
    surface the 200 body. Patch time.sleep so the retry doesn't slow
    the test."""
    # The client delegates retry/backoff to tenacity; patch the sleep tenacity
    # would call so the test doesn't actually wait through the exp-backoff.
    with patch("tenacity.nap.time.sleep"):
        with _my_vcr.use_cassette("fastly_503_then_success.yaml"):
            result = fastly("GET", "/account", token="test-token-not-real")

    assert result == {"id": "acct-1", "name": "Test Account"}


def test_429_is_retried_then_succeeds():
    """429 (rate limit) is treated identically to 5xx by
    ``_is_retryable_fastly_error``. The cassette serves 429 then 200; the
    client must retry and surface the 200 body. This is the wire-level
    pin for the audit recommendation that a transient Fastly rate-limit
    must not abort a multi-step provision flow."""
    with patch("tenacity.nap.time.sleep"):
        with _my_vcr.use_cassette("fastly_429_then_success.yaml"):
            result = fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real")

    assert result["id"] == "SU3xxxxxxxxxxxxxx0000"
    assert result["name"] == "Test CDN"
