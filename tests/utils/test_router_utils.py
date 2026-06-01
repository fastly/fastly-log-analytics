"""Tests for ``backend.utils.router_utils`` — shared router helpers.

Three orthogonal pieces share this file:

1. ``query_errors`` decorator — the exception-to-HTTPException mapper
   wrapping nearly every analytical route. Its mapping (ValueError →
   400, LookupError → 404, other → configurable, HTTPException passed
   through) is the contract the frontend's error UI keys on.

2. ``format_debug_request`` — header obfuscation for the debug panel.
   Pinned so a refactor never starts leaking ``Fastly-Key`` into the
   debug UI (it would otherwise be visible to any analyst).

3. ``sync_admin_state`` + SSE helpers — small but used everywhere.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.utils import router_utils

# ── format_debug_request: sensitive-header obfuscation ───────────────────────


def test_format_debug_request_obfuscates_sensitive_headers():
    """The four sensitive-header names (Fastly-Key, Authorization,
    x-api-key, x-api-token) must be masked except for the last four
    chars. Pinned because leaking these in the debug UI would expose
    customer API credentials to anyone with read-only access."""
    out = router_utils.format_debug_request(
        "GET",
        "https://api.fastly.com/services",
        headers={
            "Fastly-Key": "super-secret-key-1234",
            "Authorization": "Bearer abcd",
            "Accept": "application/json",  # not sensitive
        },
    )
    assert "super-secret-key-1234" not in out
    assert "***1234" in out
    assert "***abcd" in out
    assert "application/json" in out  # non-sensitive headers pass through verbatim


def test_format_debug_request_obfuscates_short_sensitive_value():
    """A sensitive header value < 4 chars → fully masked to ``***``
    (don't leak any chars). Defensive: a 3-char value would otherwise
    slice to '' and surface as ``***`` anyway, but pinning the literal
    forces a regression test if someone changes the slice logic."""
    out = router_utils.format_debug_request("GET", "https://x", headers={"x-api-key": "abc"})
    assert "***\n" in out or out.endswith("***")
    assert "abc" not in out


def test_format_debug_request_obfuscation_is_case_insensitive():
    """Header names are case-insensitive per RFC 7230 — the obfuscator
    must lowercase before checking the deny-list."""
    out = router_utils.format_debug_request("GET", "https://x", headers={"FASTLY-KEY": "supersecret"})
    assert "supersecret" not in out


def test_format_debug_request_renders_query_string():
    out = router_utils.format_debug_request(
        "GET", "https://x", query={"region": "us-east-1", "fastly-key": "secret123"}
    )
    assert "region=us-east-1" in out
    assert "fastly-key=***t123" in out  # obfuscated in query too
    assert "secret123" not in out


def test_format_debug_request_minimal():
    """No headers, no query → just the method+url and the divider."""
    out = router_utils.format_debug_request("POST", "https://api.fastly.com/x")
    assert "POST https://api.fastly.com/x" in out
    assert "--- Request ---" in out


# ── SSE_HEADERS + sse_flush_preamble ────────────────────────────────────────


def test_sse_headers_disable_buffering_for_proxies():
    """``X-Accel-Buffering: no`` is what makes nginx/cloudflare flush
    SSE chunks immediately. Without it the frontend waits for the
    buffer to fill, defeating the "real-time progress" UX."""
    assert router_utils.SSE_HEADERS["X-Accel-Buffering"] == "no"
    assert router_utils.SSE_HEADERS["Content-Type"] == "text/event-stream"
    assert router_utils.SSE_HEADERS["Cache-Control"] == "no-cache"


def test_sse_flush_preamble_emits_count_chunks_of_padding():
    """The preamble pushes ~8KB of comment-line padding through the
    pipe so proxies flush their buffer before the first real event."""
    chunks = list(router_utils.sse_flush_preamble(count=3))
    assert len(chunks) == 3
    for c in chunks:
        assert c.startswith(": ")  # SSE comment prefix
        assert c.endswith("\n\n")


# ── sync_admin_state ─────────────────────────────────────────────────────────


def test_sync_admin_state_skips_when_service_id_is_none():
    """No service_id → no work. Pinned because the routes call this
    with the optional ``service_id_hint`` which may be None for
    cross-service alert mutations."""
    # Should not raise and should not even attempt to import state_sync.
    router_utils.sync_admin_state(None)
    router_utils.sync_admin_state("")  # empty string also skips


def test_sync_admin_state_calls_export_admin_state(monkeypatch):
    calls: list[str] = []

    def fake_export(sid):
        calls.append(sid)

    # The function imports state_sync lazily; patch on the source module
    import backend.state_sync as ss

    monkeypatch.setattr(ss, "export_admin_state", fake_export)

    router_utils.sync_admin_state("svc-1")
    assert calls == ["svc-1"]


def test_sync_admin_state_swallows_all_exceptions(monkeypatch):
    """Fire-and-forget: a sync failure must NEVER bubble back into the
    HTTP request that triggered the mutation. A 500 here would make
    the alert-save UI flash an error even though the alert WAS saved."""
    import backend.state_sync as ss

    def boom(sid):
        raise RuntimeError("S3 unreachable")

    monkeypatch.setattr(ss, "export_admin_state", boom)

    # Must not raise
    router_utils.sync_admin_state("svc-1")


# ── query_errors decorator: exception → HTTPException mapping ───────────────


def test_query_errors_passes_through_httpexception_unchanged():
    """``HTTPException`` is the existing app-layer error type — wrapping
    it would double-encode the response body. The decorator must let
    it propagate as-is."""

    @router_utils.query_errors()
    def handler():
        raise HTTPException(status_code=418, detail={"error": "teapot"})

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 418
    assert exc.value.detail == {"error": "teapot"}


def test_query_errors_maps_value_error_to_400():
    """``ValueError`` (validation, bad arg) → 400 with the error message.
    No traceback in the detail — these are user-facing input errors."""

    @router_utils.query_errors()
    def handler():
        raise ValueError("invalid filter shape")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 400
    assert exc.value.detail == {"error": "invalid filter shape"}
    assert "trace" not in exc.value.detail  # no traceback for 400


def test_query_errors_maps_lookup_error_to_404():
    """``LookupError`` (KeyError / IndexError) → 404. Used by repo
    layers to signal "row not found" without a custom exception type."""

    @router_utils.query_errors()
    def handler():
        raise KeyError("alert-id-nope")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 404


def test_query_errors_maps_unknown_exception_to_configured_status_with_trace():
    """Generic exceptions surface with a full traceback in the detail
    — the debug panel renders this. Pinned because removing the
    traceback would make production debugging much harder."""

    @router_utils.query_errors(status_code=500)
    def handler():
        raise RuntimeError("downstream blew up")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 500
    assert exc.value.detail["error"] == "downstream blew up"
    assert "Traceback" in exc.value.detail["trace"]


def test_query_errors_default_status_code_is_400():
    """Default to 400 (client error) — matches the historic behaviour
    when the decorator was an inline try/except in every route."""

    @router_utils.query_errors()
    def handler():
        raise RuntimeError("bad")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 400


def test_query_errors_preserves_function_metadata_via_wraps():
    """``functools.wraps`` keeps the wrapped function's __name__ and
    docstring — FastAPI uses these to generate OpenAPI op IDs. A
    regression here would rename every wrapped endpoint to ``wrapper``."""

    @router_utils.query_errors()
    def my_endpoint():
        """Docstring for OpenAPI."""
        return 1

    assert my_endpoint.__name__ == "my_endpoint"
    assert my_endpoint.__doc__ == "Docstring for OpenAPI."


def test_query_errors_passes_args_and_kwargs_through():
    @router_utils.query_errors()
    def handler(a, b, *, c):
        return a + b + c

    assert handler(1, 2, c=3) == 6
