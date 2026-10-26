"""Consolidated tests for test_router_utils."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.utils import router_utils
from backend.utils.router_utils import require_fastly_token, resolve_fastly_token, stored_fastly_token

# ── format_debug_request: sensitive-header obfuscation ───────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
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


@pytest.mark.usefixtures("_isolate_module_globals")
def test_format_debug_request_obfuscates_short_sensitive_value():
    """A sensitive header value < 4 chars → fully masked to ``***``
    (don't leak any chars). Defensive: a 3-char value would otherwise
    slice to '' and surface as ``***`` anyway, but pinning the literal
    forces a regression test if someone changes the slice logic."""
    out = router_utils.format_debug_request("GET", "https://x", headers={"x-api-key": "abc"})
    assert "***\n" in out or out.endswith("***")
    assert "abc" not in out


@pytest.mark.usefixtures("_isolate_module_globals")
def test_format_debug_request_obfuscation_is_case_insensitive():
    """Header names are case-insensitive per RFC 7230 — the obfuscator
    must lowercase before checking the deny-list."""
    out = router_utils.format_debug_request("GET", "https://x", headers={"FASTLY-KEY": "supersecret"})
    assert "supersecret" not in out


@pytest.mark.usefixtures("_isolate_module_globals")
def test_format_debug_request_renders_query_string():
    out = router_utils.format_debug_request(
        "GET", "https://x", query={"region": "us-east-1", "fastly-key": "secret123"}
    )
    assert "region=us-east-1" in out
    assert "fastly-key=***t123" in out  # obfuscated in query too
    assert "secret123" not in out


@pytest.mark.usefixtures("_isolate_module_globals")
def test_format_debug_request_minimal():
    """No headers, no query → just the method+url and the divider."""
    out = router_utils.format_debug_request("POST", "https://api.fastly.com/x")
    assert "POST https://api.fastly.com/x" in out
    assert "--- Request ---" in out


@pytest.mark.usefixtures("_isolate_module_globals")
def test_router_utils_redaction_uses_the_shared_salt_registry():
    salt = "test-cid-salt-for-redaction"
    router_utils.register_salt(salt)

    assert router_utils.redact_error_text(f"{salt} /app/data/private.db") == ("[redacted] <path>")


# ── sync_admin_state ─────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sync_admin_state_skips_when_service_id_is_none():
    """No service_id → no work. Pinned because the routes call this
    with the optional ``service_id_hint`` which may be None for
    cross-service alert mutations."""
    # Should not raise and should not even attempt to import state_sync.
    router_utils.sync_admin_state(None)
    router_utils.sync_admin_state("")  # empty string also skips


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sync_admin_state_calls_export_admin_state(monkeypatch):
    calls: list[str] = []

    def fake_export(sid):
        calls.append(sid)

    # The function imports state_sync lazily; patch on the source module
    import backend.state_sync as ss

    monkeypatch.setattr(ss, "export_admin_state", fake_export)

    router_utils.sync_admin_state("svc-1")
    assert calls == ["svc-1"]


@pytest.mark.usefixtures("_isolate_module_globals")
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


@pytest.mark.usefixtures("_isolate_module_globals")
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


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_maps_value_error_to_400():
    """``ValueError`` (validation, bad arg) → 400 with the message under
    ``detail.message`` and the canonical ``bad_request`` error code in
    the machine-code slot. No traceback in the detail — these are
    user-facing input errors."""

    @router_utils.query_errors()
    def handler():
        raise ValueError("invalid filter shape")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 400
    assert exc.value.detail == {"error": "bad_request", "message": "invalid filter shape"}
    assert "trace" not in exc.value.detail  # no traceback for 400


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_maps_lookup_error_to_404():
    """``LookupError`` (KeyError / IndexError) → 404. Used by repo
    layers to signal "row not found" without a custom exception type."""

    @router_utils.query_errors()
    def handler():
        raise KeyError("alert-id-nope")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 404


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_maps_unknown_exception_to_configured_status_without_trace(caplog):
    """Security: generic exceptions surface ONLY a stable machine code
    (``unhandled_error``) + correlation id. The exception message is
    logged server-side via ``raise_internal`` but MUST NOT appear in
    the response body. Pinned because re-introducing ``str(e)`` in the
    detail would leak DuckDB internals, upstream Fastly response bodies,
    and any secret values that landed in the exception message."""
    import logging

    @router_utils.query_errors(status_code=500)
    def handler():
        raise RuntimeError("downstream blew up")

    with caplog.at_level(logging.ERROR), pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 500
    assert exc.value.detail["error"] == "unhandled_error"
    # The original exception message MUST NOT reach the wire — it lives
    # in the server log alongside the error_id for triage.
    assert "downstream blew up" not in str(exc.value.detail)
    assert exc.value.detail.get("error_id"), "raise_internal should attach an error_id"
    assert "trace" not in exc.value.detail, (
        "stack-trace leakage regression — query_errors must not put a 'trace' key in the response detail (security)"
    )


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_default_status_code_is_400():
    """Default to 400 (client error) — matches the historic behaviour
    when the decorator was an inline try/except in every route."""

    @router_utils.query_errors()
    def handler():
        raise RuntimeError("bad")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 400


@pytest.mark.usefixtures("_isolate_module_globals")
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


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_passes_args_and_kwargs_through():
    @router_utils.query_errors()
    def handler(a, b, *, c):
        return a + b + c

    assert handler(1, 2, c=3) == 6


# ── query_errors: async handler support (M4) ─────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_wraps_async_handler_and_returns_value():
    """Async route handlers (introduced with M4 for asyncio.gather
    parallelisation of Fastly calls in usage::prefill) must work with
    the decorator. The wrapper detects coroutine functions and awaits
    them — without this branch, FastAPI would receive a coroutine
    object as the response and fail to serialize it."""
    import asyncio

    @router_utils.query_errors()
    async def handler() -> dict:
        await asyncio.sleep(0)
        return {"ok": True}

    result = asyncio.run(handler())
    assert result == {"ok": True}


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_maps_value_error_in_async_handler_to_400():
    """Same ValueError → 400 mapping that the sync branch provides,
    pinned for the async branch. Without this, an async handler raising
    ValueError would surface as a 500."""
    import asyncio

    @router_utils.query_errors()
    async def handler():
        raise ValueError("bad input")

    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler())
    assert exc.value.status_code == 400
    assert exc.value.detail == {"error": "bad_request", "message": "bad input"}


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_passes_httpexception_through_for_async_handler():
    """An async handler that raises HTTPException itself (e.g. a 502
    from a Fastly call) must NOT be remapped — the original status
    code is what the frontend renders."""
    import asyncio

    @router_utils.query_errors()
    async def handler():
        raise HTTPException(status_code=502, detail={"error": "upstream down"})

    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler())
    assert exc.value.status_code == 502
    assert exc.value.detail == {"error": "upstream down"}


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_maps_unknown_exception_in_async_handler_to_configured_status(caplog):
    """An async handler raising a generic Exception is mapped to the
    decorator's configured status_code. Mirrors the sync branch behavior
    so callers don't need to know whether the handler is async."""
    import asyncio
    import logging

    @router_utils.query_errors(status_code=500)
    async def handler():
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="backend.utils.router_utils"):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(handler())
    assert exc.value.status_code == 500
    assert exc.value.detail["error"] == "unhandled_error"
    assert "boom" not in str(exc.value.detail)
    assert exc.value.detail.get("error_id"), "raise_internal should attach an error_id"
    assert "trace" not in (exc.value.detail or {}), (
        "stack-trace leakage regression for async handlers — query_errors must "
        "not put a 'trace' key in the response detail (security)"
    )


# ── raise_internal: server-log the cause, generic detail on the wire ────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_raise_internal_does_not_leak_exception_string_to_client(caplog):
    """Server-side log captures the traceback (operators can triage); the
    HTTPException detail returned to the client carries ONLY a generic
    ``code`` and an ``error_id`` for correlation. Pinned because the v2.0
    audit found 8 routers that interpolated ``str(e)`` directly into
    HTTPException.detail — when the exception originates in
    ``backend.core.fastly.client.fastly()`` that ``str(e)`` includes
    the upstream Fastly response body (potentially internal hostnames,
    token fragments, etc.). Re-introducing that pattern would re-open
    the leak.
    """
    import logging

    log = logging.getLogger("test.raise_internal")

    leaky = RuntimeError("HTTP 502 GET /tokens/self\n    internal.fastly.svc:5001 timed out")
    with caplog.at_level(logging.ERROR, logger="test.raise_internal"):
        with pytest.raises(HTTPException) as exc:
            router_utils.raise_internal(log, leaky, code="my_endpoint_failed", status=500)

    assert exc.value.status_code == 500
    detail = exc.value.detail or {}
    assert detail.get("error") == "my_endpoint_failed"
    assert "error_id" in detail
    assert len(detail["error_id"]) == 8  # 8-char hex prefix
    # The leaky message MUST NOT be on the wire.
    assert "internal.fastly.svc" not in str(detail)
    assert "502" not in detail.get("error", "")
    # But the operator log MUST have captured the full exception for triage.
    log_text = "\n".join(r.getMessage() for r in caplog.records) + "\n".join(
        str(r.exc_info) for r in caplog.records if r.exc_info
    )
    assert detail["error_id"] in log_text


@pytest.mark.usefixtures("_isolate_module_globals")
def test_raise_internal_chains_original_exception_for_traceback():
    """``raise from`` semantics: the caused-by chain must point at the
    original exception so server logs show the full root cause.
    Without ``from exc`` the operator log would show only the generic
    ``request_failed`` exception, hiding the actual upstream failure."""
    import logging

    log = logging.getLogger("test.raise_internal_chain")
    orig = RuntimeError("root cause")
    try:
        try:
            raise orig
        except RuntimeError as e:
            router_utils.raise_internal(log, e)
    except HTTPException as wrapped:
        # The wrapped exception's __cause__ is the original (via "raise ... from exc")
        assert wrapped.__cause__ is orig


@pytest.mark.usefixtures("_isolate_module_globals")
def test_query_errors_async_branch_preserves_concurrency():
    """The whole point of converting to async: two awaitables started
    via asyncio.gather under @query_errors must run concurrently. If
    the decorator accidentally awaits in a way that serialises them,
    the wall-clock would be ~ sum(sleeps) instead of ~ max(sleeps).
    """
    import asyncio
    import time

    @router_utils.query_errors()
    async def handler():
        async def _slow_a():
            await asyncio.sleep(0.10)
            return "a"

        async def _slow_b():
            await asyncio.sleep(0.10)
            return "b"

        a, b = await asyncio.gather(_slow_a(), _slow_b())
        return {"a": a, "b": b}

    t0 = time.monotonic()
    result = asyncio.run(handler())
    elapsed = time.monotonic() - t0

    assert result == {"a": "a", "b": "b"}
    assert elapsed < 0.5, (
        f"two 100ms awaits under asyncio.gather must run concurrently "
        f"(wall clock should be ~100ms, not ~200ms). Got {elapsed * 1000:.0f}ms — "
        f"the async decorator branch is serialising them."
    )


# ── resolve_fastly_token / require_fastly_token ──────────────────────────────


@pytest.fixture
def configs(monkeypatch):
    store: dict[str, dict | None] = {}
    loaded: list[str] = []

    def _load(service_id):
        loaded.append(service_id)
        return store.get(service_id)

    monkeypatch.setattr("backend.config.load_config", _load)
    monkeypatch.setattr("backend.config.get_active_service_id", lambda *a, **k: "active-svc")
    store["active-svc"] = {"fastly_api_key": "ACTIVE-KEY"}
    store["svc-a"] = {"fastly_api_key": "stored-a"}
    store["svc-nokey"] = {"service_id": "svc-nokey"}
    store["svc-nullkey"] = {"fastly_api_key": None}
    return loaded


@pytest.mark.usefixtures("_isolate_module_globals")
def test_override_wins_without_reading_config(configs) -> None:
    assert resolve_fastly_token("svc-a", "override") == "override"
    assert require_fastly_token("svc-a", "override") == "override"
    assert configs == []


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.parametrize("empty", ["", None])
def test_stored_key_used_when_override_is_empty(configs, empty) -> None:
    assert resolve_fastly_token("svc-a", empty) == "stored-a"
    assert require_fastly_token("svc-a", empty) == "stored-a"


@pytest.mark.usefixtures("_isolate_module_globals")
def test_lenient_variant_returns_empty_string_instead_of_raising(configs) -> None:
    assert resolve_fastly_token("svc-missing") == ""
    assert resolve_fastly_token("svc-nokey") == ""
    assert resolve_fastly_token("svc-nullkey") == ""


@pytest.mark.usefixtures("_isolate_module_globals")
def test_empty_service_id_never_borrows_the_active_services_key(configs) -> None:
    assert resolve_fastly_token("") == ""
    with pytest.raises(HTTPException) as exc:
        require_fastly_token("")
    assert exc.value.status_code == 404


@pytest.mark.usefixtures("_isolate_module_globals")
def test_strict_variant_errors_match_the_historical_rum_bodies(configs) -> None:
    with pytest.raises(HTTPException) as missing:
        require_fastly_token("svc-missing")
    assert (missing.value.status_code, missing.value.detail) == (404, {"error": "No config for service svc-missing"})
    with pytest.raises(HTTPException) as nokey:
        require_fastly_token("svc-nokey")
    assert (nokey.value.status_code, nokey.value.detail) == (400, {"error": "No Fastly API key for service svc-nokey"})


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.parametrize(
    ("cfg", "expected"),
    [
        ({"fastly_api_key": "k"}, "k"),
        ({"fastly_api_key": "  k \n"}, "k"),
        ({"fastly_api_key": None}, ""),
        ({"fastly_api_key": ""}, ""),
        ({}, ""),
        (None, ""),
    ],
)
def test_stored_fastly_token_reads_config_value(cfg, expected) -> None:
    assert stored_fastly_token(cfg) == expected


@pytest.mark.usefixtures("_isolate_module_globals")
def test_token_resolution_never_logs_the_secret(configs, caplog) -> None:
    import logging

    with caplog.at_level(logging.DEBUG):
        resolve_fastly_token("svc-a")
        require_fastly_token("svc-a")
        stored_fastly_token({"fastly_api_key": "stored-a"})
    assert "stored-a" not in caplog.text


# ── Merged from test_sse.py ──────────────────────────────────────────────

from collections.abc import AsyncIterator

from backend.utils.sse import sse_response
from sse_starlette.sse import EventSourceResponse

from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS


async def _stream() -> AsyncIterator[str]:
    yield "data: hello\n\n"


def _headers(resp: EventSourceResponse) -> dict[str, str]:
    return {k.decode().lower(): v.decode() for k, v in resp.raw_headers}


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sse_response_returns_event_source_response():
    resp = sse_response(_stream())
    assert isinstance(resp, EventSourceResponse)


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sse_response_default_ping_is_5():
    resp = sse_response(_stream())
    assert resp._ping_interval == 5


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sse_response_ping_passthrough():
    resp = sse_response(_stream(), ping=11)
    assert resp._ping_interval == 11


@pytest.mark.usefixtures("_isolate_module_globals")
def test_sse_response_applies_passthrough_headers():
    resp = sse_response(_stream())
    headers = _headers(resp)
    for key, value in SSE_PASSTHROUGH_HEADERS.items():
        assert headers[key.lower()] == value


@pytest.mark.usefixtures("_isolate_module_globals")
def test_routers_build_sse_only_via_sse_response():
    from pathlib import Path

    routers = Path(__file__).resolve().parents[2] / "backend" / "routers"
    offenders = [str(p.relative_to(routers)) for p in routers.rglob("*.py") if "EventSourceResponse(" in p.read_text()]
    assert offenders == []


# ── Merged from test_auth.py ──────────────────────────────────────────────

from types import SimpleNamespace

import pytest
from backend.utils.auth import analyst_allowed_services, mask_ips_for, require_owned_row


def _req(analyst_session: object | None) -> SimpleNamespace:
    if isinstance(analyst_session, list):
        analyst_session = SimpleNamespace(service_ids=analyst_session)
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


@pytest.mark.usefixtures("_isolate_module_globals")
def test_analyst_allowed_services_returns_none_for_admin():
    """Admin sessions don't carry an analyst_session; the helper signals
    "no scope filter" via None so caller code can branch on
    ``allowed is not None``."""
    assert analyst_allowed_services(_req(None)) is None


@pytest.mark.usefixtures("_isolate_module_globals")
def test_analyst_allowed_services_returns_set_for_scoped_analyst():
    """An analyst scoped to two services gets a set of those two ids —
    callers use it to filter list/get endpoints."""
    sess = SimpleNamespace(service_ids=["svc-A", "svc-B"])
    assert analyst_allowed_services(_req(sess)) == {"svc-A", "svc-B"}


@pytest.mark.usefixtures("_isolate_module_globals")
def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_none():
    """An analyst session without service_ids (defensive null) gets an
    empty set — NOT None. Returning None would collapse to the admin
    branch and grant unrestricted access; an empty set correctly
    blocks every service."""
    sess = SimpleNamespace(service_ids=None)
    assert analyst_allowed_services(_req(sess)) == set()


@pytest.mark.usefixtures("_isolate_module_globals")
def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_empty():
    """Same defense as the None case for the [] variant."""
    sess = SimpleNamespace(service_ids=[])
    assert analyst_allowed_services(_req(sess)) == set()


@pytest.mark.usefixtures("_isolate_module_globals")
def test_mask_ips_for_admin_is_false():
    """Admin (no analyst session) never masks."""
    assert mask_ips_for(None) is False


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        ({"mask_ips": True}, True),
        ({"mask_ips": False}, False),
        ({}, False),
        (None, False),  # pii_policy revoked to None mid-session (F014 re-sync)
        ("not-a-dict", False),
    ],
)
def test_mask_ips_for_policy_variants(policy, expected):
    """The mask_ips predicate is True only for a dict policy carrying a truthy
    mask_ips. A None / non-dict policy returns False rather than raising — the
    /api/bootstrap path relied on the unguarded form and 500'd when an invite's
    pii_policy was revoked to None mid-session.
    """
    sess = SimpleNamespace(pii_policy=policy)
    assert mask_ips_for(sess) is expected


# ── require_owned_row ────────────────────────────────────────────────────────


class _Fetch:
    def __init__(self, row: dict | None):
        self.row = row
        self.calls: list[tuple[str, str]] = []

    def __call__(self, row_id: str, service_id: str) -> dict | None:
        self.calls.append((row_id, service_id))
        return self.row


def _forbidden(exc: pytest.ExceptionInfo[HTTPException], service: str | None) -> None:
    assert exc.value.status_code == 403
    assert exc.value.detail == {"error": "service_not_authorized", "service": service}


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_admin_passes_without_touching_the_repository() -> None:
    fetch = _Fetch({"service_id": "other"})
    assert require_owned_row(_req(None), "svcA", "row-1", fetch) is None
    assert fetch.calls == []


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_analyst_owning_the_row_passes_and_row_is_looked_up_in_the_addressed_service() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcA"})
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch) == {"id": "row-1", "service_id": "svcA"}
    assert fetch.calls == [("row-1", "svcA")]


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_analyst_addressing_an_out_of_scope_service_is_rejected_before_any_lookup() -> None:
    fetch = _Fetch(None)
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcB", "row-1", fetch)
    _forbidden(exc, "svcB")
    assert fetch.calls == []


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_analyst_in_scope_service_but_row_owned_elsewhere_is_rejected() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcB"})
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch)
    _forbidden(exc, "svcB")


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_row_without_service_id_is_rejected_for_an_analyst() -> None:
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch({"id": "row-1"}))
    _forbidden(exc, None)


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_missing_row_in_scope_passes_so_the_mutation_can_report_its_own_not_found() -> None:
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch(None)) is None


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.security_regression
def test_analyst_with_empty_scope_is_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req([]), "svcA", "row-1", _Fetch(None))
    _forbidden(exc, "svcA")


# ── Merged from test_valkey_publisher.py ──────────────────────────────────────────────

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from backend.utils.valkey_publisher import ValkeyPublisher

from backend.utils import valkey_publisher as vp
from backend.utils.telemetry import _last as _last_error_log


@pytest.fixture()
def _isolate_module_globals():
    """``PUBLISH_STATS``, the rate-limit clock and the cached sync client are
    all module-global — without a reset, assertions here would be cumulative
    and order-dependent (the same test-isolation trap as
    ``_lock_retry_count`` in tests/core/test_duckdb_concurrency.py)."""
    saved_stats = dict(vp.PUBLISH_STATS)
    saved_client = vp._sync_redis
    for k in vp.PUBLISH_STATS:
        vp.PUBLISH_STATS[k] = 0
    _last_error_log.clear()
    vp._sync_redis = None
    yield
    vp.PUBLISH_STATS.clear()
    vp.PUBLISH_STATS.update(saved_stats)
    _last_error_log.clear()
    vp._sync_redis = saved_client


def _fake_sync_client():
    """Sync Redis stand-in whose pipeline records the staged commands."""
    client = MagicMock()
    pipe = MagicMock()
    pipe.staged = []
    pipe.publish.side_effect = lambda ch, msg: pipe.staged.append(("publish", ch, msg))
    pipe.rpush.side_effect = lambda k, v: pipe.staged.append(("rpush", k, v))
    pipe.ltrim.side_effect = lambda k, a, b: pipe.staged.append(("ltrim", k, a, b))
    client.pipeline.return_value = pipe
    client.pipe = pipe
    return client


# ── channel naming (tenant isolation) ────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_channel_is_scoped_by_prefix_and_service():
    """Two services must never share a channel — a collision would fan one
    tenant's events out to another tenant's subscribers."""
    pub = ValkeyPublisher(channel_prefix="sync-status")
    assert pub._channel("svc-a") == "sse:sync-status:svc-a"
    assert pub._channel("svc-b") != pub._channel("svc-a")

    other_feed = ValkeyPublisher(channel_prefix="cron-runs")
    assert other_feed._channel("svc-a") != pub._channel("svc-a")


# ── publish routing ──────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_publish_without_bound_loop_uses_sync_client():
    """The bug this class exists to fix: in a Celery worker ``bind_loop`` is
    never called, and worker-originated cron/sync events were silently
    dropped. No loop bound MUST mean a real sync publish."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(channel_prefix="sync-status")

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})

    assert ("publish", "sse:sync-status:svc-a", json.dumps({"status": "ok"})) in client.pipe.staged
    client.pipe.execute.assert_called_once()
    assert vp.PUBLISH_STATS["published"] == 1
    assert vp.PUBLISH_STATS["publish_errors"] == 0


@pytest.mark.usefixtures("_isolate_module_globals")
def test_publish_failure_counts_and_does_not_raise():
    """A dead backplane must be diagnosable from the counter, and must never
    propagate into the caller — publishers are cron/ingest paths whose real
    work already succeeded."""
    client = MagicMock()
    client.pipeline.side_effect = RuntimeError("valkey down")
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})  # must not raise

    assert vp.PUBLISH_STATS["publish_errors"] == 1
    assert vp.PUBLISH_STATS["published"] == 0


@pytest.mark.usefixtures("_isolate_module_globals")
def test_publish_with_bound_loop_goes_through_the_loop():
    """With a loop bound (the FastAPI process) the publish rides asyncio, not
    the sync client."""
    pub = ValkeyPublisher()
    loop = MagicMock()
    pub._loop = loop
    pub._redis = MagicMock()

    with patch.object(vp, "_get_sync_redis") as sync_client:
        pub.publish("svc-a", {"status": "ok"})

    loop.call_soon_threadsafe.assert_called_once()
    sync_client.assert_not_called()


@pytest.mark.usefixtures("_isolate_module_globals")
def test_publish_falls_back_to_sync_when_loop_is_gone():
    """Loop shutting down must degrade to a sync publish rather than drop the
    event on the floor."""
    client = _fake_sync_client()
    pub = ValkeyPublisher()
    loop = MagicMock()
    loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
    pub._loop = loop
    pub._redis = MagicMock()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})

    assert ("publish", "sse:sync-status:svc-a", json.dumps({"status": "ok"})) in client.pipe.staged
    assert vp.PUBLISH_STATS["published"] == 1


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.asyncio
async def test_async_publish_counts_error_without_raising():
    """The coroutine the loop schedules must swallow-and-count too — an
    exception there would surface as an unretrieved-task warning, not a
    diagnosable counter."""
    pub = ValkeyPublisher()
    redis = MagicMock()
    redis.pipeline.side_effect = RuntimeError("broker gone")
    pub._redis = redis
    pub._loop = asyncio.get_running_loop()

    pub.publish("svc-a", {"status": "ok"})
    # Two hops: call_soon_threadsafe runs the callback that creates the
    # future, then the future itself needs a turn.
    for _ in range(4):
        await asyncio.sleep(0)

    assert vp.PUBLISH_STATS["publish_errors"] == 1


# ── history ──────────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_history_is_not_written_when_disabled():
    """``history_size=0`` (the sync-status feed) must not accumulate keys in
    Valkey — that would be an unbounded memory leak per service."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=0)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert not [c for c in client.pipe.staged if c[0] in ("rpush", "ltrim")]


@pytest.mark.usefixtures("_isolate_module_globals")
def test_history_is_capped_when_enabled():
    """With history on, every publish appends AND trims — the trim is what
    bounds growth."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=5)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert ("rpush", "sse:sync-status:svc-a:history", json.dumps({"n": 1})) in client.pipe.staged
    assert ("ltrim", "sse:sync-status:svc-a:history", -5, -1) in client.pipe.staged


@pytest.mark.usefixtures("_isolate_module_globals")
def test_get_recent_ticks_empty_when_history_disabled():
    pub = ValkeyPublisher(history_size=0)
    with patch.object(vp, "_get_sync_redis") as client:
        assert pub.get_recent_ticks("svc-a") == []
    client.assert_not_called()


@pytest.mark.usefixtures("_isolate_module_globals")
def test_get_recent_ticks_decodes_history():
    client = MagicMock()
    client.lrange.return_value = [json.dumps({"n": 1}), json.dumps({"n": 2})]
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a", count=2) == [{"n": 1}, {"n": 2}]

    client.lrange.assert_called_once_with("sse:sync-status:svc-a:history", -2, -1)


@pytest.mark.usefixtures("_isolate_module_globals")
def test_get_recent_ticks_survives_a_broken_broker():
    """The SSE seed must degrade to "no history" rather than 500 the stream."""
    client = MagicMock()
    client.lrange.side_effect = RuntimeError("valkey down")
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a") == []


# ── subscriber_count (cross-pod) ─────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
def test_subscriber_count_reads_across_pods():
    """This is what suspends the RT poller when nobody is watching — it must
    reflect subscribers on OTHER pods, which is why it goes to Valkey."""
    client = MagicMock()
    client.pubsub_numsub.return_value = [("sse:sync-status:svc-a", 3)]
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 3


@pytest.mark.usefixtures("_isolate_module_globals")
def test_subscriber_count_is_zero_on_error():
    client = MagicMock()
    client.pubsub_numsub.side_effect = RuntimeError("down")
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 0


# ── subscribe ────────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.asyncio
async def test_subscribe_before_bind_loop_raises():
    """Explicit contract: subscribing is only meaningful in the SSE-serving
    process, and a silent no-op here would hang the stream forever."""
    pub = ValkeyPublisher()
    with pytest.raises(RuntimeError, match="bind_loop"):
        await anext(pub.subscribe("svc-a"))


class _FakePubSub:
    """pubsub stand-in yielding a scripted frame sequence."""

    def __init__(self, frames, fail_after=False):
        self._frames = frames
        self._fail_after = fail_after
        self.subscribed: list[str] = []

    async def subscribe(self, channel):
        self.subscribed.append(channel)

    async def unsubscribe(self, channel):
        pass

    async def close(self):
        pass

    async def listen(self):
        for f in self._frames:
            yield f
        if self._fail_after:
            raise RuntimeError("connection lost")


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.asyncio
async def test_subscribe_yields_decoded_payloads_and_skips_control_frames():
    pub = ValkeyPublisher()
    frames = [
        {"type": "subscribe", "data": 1},  # control frame — must be skipped
        {"type": "message", "data": json.dumps({"n": 1})},
        {"type": "message", "data": json.dumps({"n": 2})},
    ]
    fake = _FakePubSub(frames)
    pub._redis = MagicMock()
    pub._redis.pubsub.return_value = fake

    got = []
    async for payload in pub.subscribe("svc-a"):
        got.append(payload)
        if len(got) == 2:
            break

    assert got == [{"n": 1}, {"n": 2}]
    assert fake.subscribed == ["sse:sync-status:svc-a"]


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.asyncio
async def test_subscribe_counts_undecodable_payload_and_keeps_streaming():
    """One poisoned message must not kill the stream — it is counted and the
    next good frame still arrives."""
    pub = ValkeyPublisher()
    frames = [
        {"type": "message", "data": "{not json"},
        {"type": "message", "data": json.dumps({"n": 7})},
    ]
    pub._redis = MagicMock()
    pub._redis.pubsub.return_value = _FakePubSub(frames)

    got = []
    async for payload in pub.subscribe("svc-a"):
        got.append(payload)
        break

    assert got == [{"n": 7}]
    assert vp.PUBLISH_STATS["decode_errors"] == 1


@pytest.mark.usefixtures("_isolate_module_globals")
@pytest.mark.asyncio
async def test_subscribe_reconnects_after_broker_loss():
    """A Valkey restart must reconnect with backoff, not leave the SSE
    generator dead and the client hanging on an open connection."""
    pub = ValkeyPublisher()
    first = _FakePubSub([{"type": "message", "data": json.dumps({"n": 1})}], fail_after=True)
    second = _FakePubSub([{"type": "message", "data": json.dumps({"n": 2})}])
    pub._redis = MagicMock()
    pub._redis.pubsub.side_effect = [first, second]

    sleeps: list[float] = []

    async def _no_sleep(secs):
        sleeps.append(secs)

    got = []
    with patch.object(vp.asyncio, "sleep", _no_sleep):
        async for payload in pub.subscribe("svc-a"):
            got.append(payload)
            if len(got) == 2:
                break

    assert got == [{"n": 1}, {"n": 2}], "must resume delivery on the reconnected subscription"
    assert sleeps, "reconnect must back off rather than hot-loop"
    assert second.subscribed == ["sse:sync-status:svc-a"]


# ── Merged from tests/utils/test_sql_validator.py ──

"""Acceptance tests for the DuckDB user-SQL validator (security).

The Decision B policy is defined in security_remediation_final_v6.md §2 and
implemented in backend/utils/sql_validator.py. This file pins every
acceptance criterion called out in that document so a future refactor
can't silently weaken the policy.

The validator runs against a real DuckDB connection (the same engine the
production app uses), so test coverage is end-to-end on the parse-tree
walker — no mocking the json_serialize_sql output.
"""


import duckdb
import pytest

from backend.utils.sql_validator import (
    MAX_INPUT_BYTES,
    SQLValidationError,
    apply_user_query_limits,
    validate_user_sql,
)


@pytest.fixture
def con():
    """A fresh in-memory DuckDB connection for each test. Sized minimally —
    these tests never execute the SQL, only parse it."""
    c = duckdb.connect(":memory:")
    yield c
    c.close()


# ── ACCEPTANCE: things that should PASS ─────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        # Plain SELECT from a non-blocked table
        "SELECT count(*) FROM fos_view WHERE service_id = 'x' LIMIT 100",
        # CTE alias produces a BASE_TABLE node for `x` — must not be blocked
        "WITH x AS (SELECT count(*) FROM fos_view) SELECT * FROM x",
        # UNION (SET_OPERATION_NODE)
        "SELECT 1 UNION SELECT 2",
        # SELECT with aggregate + GROUP BY
        "SELECT service_id, count(*) FROM logs GROUP BY service_id ORDER BY 2 DESC LIMIT 50",
        # Multiple CTEs
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT a.x + b.y FROM a, b",
    ],
)
def test_valid_user_queries_pass(con, sql):
    result = validate_user_sql(sql, parser_con=con)
    assert result.parse_tree.get("statements"), f"missing statements key: {result.parse_tree}"


# ── ACCEPTANCE: function denylist (file/secret exfil) ───────────────────────


@pytest.mark.parametrize(
    "sql,blocked_function",
    [
        ("SELECT * FROM read_csv_auto('/etc/passwd')", "read_csv_auto"),
        ("SELECT * FROM read_parquet('s3://bucket/key.parquet')", "read_parquet"),
        ("SELECT * FROM read_json('/tmp/whatever.json')", "read_json"),
        ("SELECT * FROM read_text('/etc/passwd')", "read_text"),
        ("SELECT * FROM read_blob('/etc/passwd')", "read_blob"),
        ("SELECT getenv('AWS_SECRET_ACCESS_KEY')", "getenv"),
        ("SELECT current_setting('s3_secret_access_key')", "current_setting"),
        ("SELECT * FROM duckdb_secrets()", "duckdb_secrets"),
        ("SELECT * FROM glob('/etc/*')", "glob"),
        ("SELECT * FROM lsdir('/')", "lsdir"),
        ("SELECT * FROM postgres_scan('host=evil', 'public', 't')", "postgres_scan"),
        ("SELECT * FROM sqlite_scan('/tmp/x.db', 't')", "sqlite_scan"),
        ("SELECT * FROM iceberg_scan('/tmp/iceberg')", "iceberg_scan"),
        # Regression for audit finding 014: parquet_scan / parquet_metadata
        # / parquet_schema / parquet_kv_metadata are DuckDB aliases that
        # bypassed the denylist before. They must be rejected for the same
        # reason as read_parquet (arbitrary path → exfil).
        ("SELECT * FROM parquet_scan('/etc/passwd.parquet')", "parquet_scan"),
        ("SELECT * FROM parquet_metadata('/tmp/x.parquet')", "parquet_metadata"),
        ("SELECT * FROM parquet_schema('/tmp/x.parquet')", "parquet_schema"),
        ("SELECT * FROM parquet_kv_metadata('/tmp/x.parquet')", "parquet_kv_metadata"),
    ],
)
def test_blocked_functions_rejected(con, sql, blocked_function):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"function_denylist:{blocked_function}", (
        f"expected function_denylist:{blocked_function}, got {exc.value.reason}"
    )


# ── REGRESSION (C1): denylist→allowlist for table functions ─────────────────
#
# The pre-fix gate was a function-NAME denylist. DuckDB 1.5.x ships file/
# external-resource table functions that were never in that list, so an
# authenticated analyst could read arbitrary server files and other tenants'
# databases through /api/query. The fix adds (a) a static name-prefix
# denylist and (b) a table-function ALLOWLIST derived from the live engine's
# duckdb_functions(). These tests pin both halves.


@pytest.mark.parametrize(
    "sql,expected_prefix",
    [
        # The exact five functions the old denylist missed (verified file/
        # external-resource readers in DuckDB 1.5.3). Each is now caught by
        # the static prefix layer.
        ("SELECT * FROM sniff_csv('/srv/app/config/other.json')", "sniff_"),
        ("SELECT * FROM read_duckdb('/srv/app/data/metadata.duckdb')", "read_"),
        ("SELECT * FROM parquet_file_metadata('s3://other-tenant/raw/x.parquet')", "parquet_"),
        ("SELECT * FROM parquet_full_metadata('/tmp/x.parquet')", "parquet_"),
        ("SELECT * FROM parquet_bloom_probe('/tmp/x.parquet', 'c', 1)", "parquet_"),
    ],
)
def test_c1_missing_file_readers_rejected_by_prefix(con, sql, expected_prefix):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"function_prefix:{expected_prefix}", (
        f"expected function_prefix:{expected_prefix}, got {exc.value.reason}"
    )


@pytest.mark.parametrize(
    "sql,fn",
    [
        # Dangerous table functions that match NO blocked prefix and were NOT
        # in the explicit denylist — only the allowlist (LAYER 3) closes them.
        # json_execute_serialized_sql would let an analyst run reconstructed
        # SQL; which_secret introspects stored secrets; pragma_* leak engine
        # internals; arrow_scan/pandas_scan reach process memory pointers.
        ("SELECT * FROM which_secret('s3://x', 'a')", "which_secret"),
        ("SELECT * FROM pragma_version()", "pragma_version"),
        ("SELECT * FROM pragma_database_size()", "pragma_database_size"),
        ("SELECT * FROM pragma_table_info('logs')", "pragma_table_info"),
        ("SELECT * FROM summary(logs)", "summary"),
    ],
)
def test_c1_unprefixed_table_functions_rejected_by_allowlist(con, sql, fn):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"function_table_not_allowed:{fn}", (
        f"expected function_table_not_allowed:{fn}, got {exc.value.reason}"
    )


def test_c1_serialized_sql_executor_rejected(con):
    """json_execute_serialized_sql is the highest-severity unprefixed catch:
    it executes a reconstructed statement, which would bypass every other
    parse-time gate. It is scalar+table in this build, so the allowlist
    (not the denylist) is what stops it."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql("SELECT * FROM json_execute_serialized_sql('{}')", parser_con=con)
    assert exc.value.reason == "function_table_not_allowed:json_execute_serialized_sql"


def test_c1_allowlisted_table_functions_pass(con):
    """The vetted I/O-free table functions (and the scalar variants that
    share their names) must still validate — the allowlist must not be a
    blanket table-function ban."""
    for sql in (
        "SELECT * FROM generate_series(1, 10)",
        "SELECT * FROM range(5)",
        "SELECT unnest([1, 2, 3]) AS n",
        "SELECT repeat('-', 10) AS bar",  # scalar repeat shares the table name
        "SELECT n FROM range(3) t(n) WHERE n > 0",
    ):
        result = validate_user_sql(sql, parser_con=con)
        assert result.parse_tree.get("statements"), sql


def test_c1_ordinary_scalar_query_still_passes(con):
    """A representative analytics query (scalar/aggregate functions only)
    must not be caught by the table-function gate."""
    sql = (
        "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n, "
        "regexp_matches(url, 'admin') AS hit, lower(ua) AS ua "
        "FROM logs GROUP BY 1, 3, 4 ORDER BY 2 DESC LIMIT 100"
    )
    result = validate_user_sql(sql, parser_con=con, allowed_tables=frozenset({"logs"}))
    assert result.parse_tree.get("statements")


def test_c1_allowlist_tracks_live_engine_enumeration(con):
    """Belt check: every table-typed function the live build exposes is either
    explicitly allowlisted or rejected by validate_user_sql — there is no
    table function that silently passes. Guards against a future engine build
    adding a file reader that slips the gate."""
    from backend.utils.sql_validator import _ALLOWED_TABLE_FUNCTIONS

    names = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type='table'"
        ).fetchall()
    ]
    assert names, "expected a non-empty table-function enumeration"
    leaked = []
    for name in names:
        if name.lower() in _ALLOWED_TABLE_FUNCTIONS:
            continue
        # Call shape doesn't matter — we assert it is rejected at validation,
        # never executed. A parse error also counts as "did not pass".
        try:
            validate_user_sql(f"SELECT * FROM {name}()", parser_con=con)
        except SQLValidationError:
            continue
        except Exception:
            # Unparseable call shape for this fn — not a validator pass; skip.
            continue
        leaked.append(name)
    assert not leaked, f"table functions passed validation without being allowlisted: {leaked}"


# ── ACCEPTANCE: catalog table prefix blocklist ──────────────────────────────


@pytest.mark.parametrize(
    "sql,blocked_prefix",
    [
        ("SELECT * FROM duckdb_secrets", "duckdb_"),
        ("SELECT * FROM duckdb_settings", "duckdb_"),
        ("SELECT * FROM duckdb_variables", "duckdb_"),
        ("SELECT * FROM duckdb_extensions", "duckdb_"),
        ("SELECT * FROM pg_settings", "pg_"),
    ],
)
def test_blocked_table_prefixes_rejected(con, sql, blocked_prefix):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"catalog_blocklist:table_name_prefix:{blocked_prefix}"


# ── ACCEPTANCE: schema_name blocklist (catches information_schema bypass) ──


@pytest.mark.parametrize(
    "sql,blocked_schema",
    [
        # The v5 regression: information_schema.tables has table_name='tables'
        # which slipped through the prefix-only check. This is THE
        # specific test that pins the v5 → v6 hardening.
        ("SELECT * FROM information_schema.tables", "information_schema"),
        ("SELECT * FROM information_schema.schemata", "information_schema"),
        ("SELECT * FROM information_schema.referential_constraints", "information_schema"),
        ("SELECT * FROM pg_catalog.pg_settings", "pg_catalog"),
    ],
)
def test_blocked_schemas_rejected(con, sql, blocked_schema):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    # pg_catalog.pg_settings has a pg_ prefix too — either rejection
    # message is acceptable as long as the query is blocked.
    assert exc.value.reason.startswith("catalog_blocklist:")


# ── ACCEPTANCE: statement-type whitelist ────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        "ATTACH '/etc/passwd' AS x",
        "INSTALL httpfs",
        "LOAD httpfs",
        "PRAGMA enable_external_access",
        "UPDATE logs SET x = 1",
        "INSERT INTO logs VALUES (1)",
        "DELETE FROM logs",
        "CREATE TABLE evil (x INT)",
        "DROP TABLE logs",
        "ALTER TABLE logs ADD COLUMN y INT",
        "COPY logs TO '/tmp/dump.csv'",
    ],
)
def test_non_select_statements_rejected(con, sql):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    # Some of these (PRAGMA / SET / ATTACH) trigger 'parse_error' if the
    # parser refuses them outright; others land on statement_type:*.
    # Both shapes count as 'rejected' for security purposes.
    assert exc.value.reason.startswith(("statement_type:", "parse_error"))


# ── ACCEPTANCE: multi-statement payloads ────────────────────────────────────


def test_multi_statement_rejected(con):
    sql = "SELECT 1; SELECT 2"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason in ("multi_statement", "no_statements")


def test_injection_classic_attempted(con):
    """The original audit attack vector: prepended ATTACH after the
    legitimate SELECT. Must be caught by either the multi-statement check
    or the statement-type check on the ATTACH branch."""
    sql = "SELECT 1; ATTACH '/etc/passwd' AS x"
    with pytest.raises(SQLValidationError):
        validate_user_sql(sql, parser_con=con)


# ── ACCEPTANCE: input size ceiling ──────────────────────────────────────────


def test_input_too_large_rejected(con):
    huge = "SELECT 1 WHERE 1 IN (" + ",".join(str(i) for i in range(20_000)) + ")"
    assert len(huge.encode()) > MAX_INPUT_BYTES
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(huge, parser_con=con)
    assert exc.value.reason == "input_too_large"


# ── ACCEPTANCE: fail-closed on parse error ──────────────────────────────────


def test_malformed_sql_rejected(con):
    """json_serialize_sql returns {'error': true, ...} for parse failures
    rather than raising. The validator must NOT silently pass these
    through — fail closed."""
    sql = "SELECT FROM WHERE * NOT-VALID"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason.startswith("parse_error")


def test_empty_input_rejected(con):
    """Empty string is technically zero statements — must fail closed."""
    with pytest.raises(SQLValidationError):
        validate_user_sql("", parser_con=con)


def test_whitespace_only_input_rejected(con):
    with pytest.raises(SQLValidationError):
        validate_user_sql("   \n\t  ", parser_con=con)


# ── SHOW_REF reject (audit F-8/9/10) ────────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        "SHOW TABLES",
        "SHOW DATABASES",
        "SHOW SCHEMAS",
        "SHOW ALL TABLES",
        "DESCRIBE logs",
        "SUMMARIZE SELECT 1",
    ],
)
def test_show_ref_statements_rejected(con, sql):
    """SHOW / DESCRIBE / SUMMARIZE all parse as SELECT_NODE with from_table
    of type SHOW_REF — they slip past the statement-type whitelist. Live-
    confirmed that SHOW TABLES leaks foreign service table names from the
    pooled DuckDB connection; SUMMARIZE and DESCRIBE leak schemas. Reject
    them on the user-query path."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == "show_ref"


# ── allowed_tables per-call allowlist (audit F-8/9/10) ──────────────────────


def test_allowed_tables_blocks_foreign_table(con):
    """When allowed_tables is set, any BASE_TABLE name outside the set is
    rejected — this is the actual cross-tenant defense."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "SELECT count(*) FROM logs_test_service",
            parser_con=con,
            allowed_tables=frozenset({"logs", "logs_examplesvcid0000000001"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


def test_allowed_tables_permits_canonical_logs(con):
    """``logs`` is in the allowlist — should pass."""
    result = validate_user_sql(
        "SELECT count(*) FROM logs",
        parser_con=con,
        allowed_tables=frozenset({"logs", "logs_examplesvcid0000000001"}),
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_permits_per_service_table(con):
    """The per-service rewritten table name is in the allowlist — should pass."""
    result = validate_user_sql(
        "SELECT count(*) FROM logs_examplesvcid0000000001",
        parser_con=con,
        allowed_tables=frozenset({"logs", "logs_examplesvcid0000000001"}),
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_case_insensitive(con):
    """Allowlist matches lowercase — caller passes lowercased names."""
    result = validate_user_sql(
        "SELECT count(*) FROM LOGS",
        parser_con=con,
        allowed_tables=frozenset({"logs"}),
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_none_is_backwards_compatible(con):
    """When allowed_tables is None (the default), no per-table allowlist
    is enforced — preserves the existing internal-caller contract."""
    # No allowlist + no other blocklist hit → must pass.
    result = validate_user_sql(
        "SELECT count(*) FROM logs_some_random_service",
        parser_con=con,
        allowed_tables=None,
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_blocks_subselect_foreign_table(con):
    """Walker recurses into sub-selects — the allowlist must apply there too."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "SELECT * FROM (SELECT count(*) FROM logs_other) AS sub",
            parser_con=con,
            allowed_tables=frozenset({"logs"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


def test_allowed_tables_blocks_join_to_foreign_table(con):
    """A JOIN against a foreign table must be rejected."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "SELECT * FROM logs a JOIN logs_other b ON a.id = b.id",
            parser_con=con,
            allowed_tables=frozenset({"logs"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


def test_allowed_tables_permits_cte_alias_reference(con):
    """``WITH x AS (...) SELECT * FROM x`` references ``x`` as a
    BASE_TABLE — without the CTE-alias exception the allowlist would
    wrongly reject the inner reference."""
    result = validate_user_sql(
        "WITH x AS (SELECT count(*) FROM logs) SELECT * FROM x",
        parser_con=con,
        allowed_tables=frozenset({"logs"}),
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_permits_nested_cte_reference(con):
    """Lexically-scoped CTE tracking must still allow nested + multi CTE
    definitions (regression guard against over-restricting the fix below)."""
    for sql in (
        "WITH a AS (WITH b AS (SELECT count(*) FROM logs) SELECT * FROM b) SELECT * FROM a",
        "WITH p AS (SELECT 1 z), q AS (SELECT 2 y) SELECT p.z + q.y FROM p, q",
        # A top-level CTE legitimately covers both UNION branches.
        "WITH t AS (SELECT 1 z) SELECT * FROM t UNION ALL SELECT * FROM t",
    ):
        result = validate_user_sql(sql, parser_con=con, allowed_tables=frozenset({"logs"}))
        assert result.parse_tree.get("statements"), sql


def test_allowed_tables_blocks_cte_shadowing_in_subquery(con):
    """Finding 009 (run 80e9f210): a CTE declared in an INNER subquery must
    not whitelist its name for a SIBLING outer reference, which DuckDB
    resolves to the real base table. The global pre-collection used to leak
    the alias across scopes; lexical scoping closes it."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "SELECT * FROM logs_other, (WITH logs_other AS (SELECT 1) SELECT * FROM logs_other) sub",
            parser_con=con,
            allowed_tables=frozenset({"logs"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


def test_allowed_tables_blocks_schema_qualified_cte_name(con):
    """Finding 017 (run 80e9f210): a same-named CTE must not excuse a
    schema/catalog-qualified reference — ``main.logs_other`` resolves to the
    base table in DuckDB, not the CTE, so it must be rejected even though a
    CTE named ``logs_other`` is in scope."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "WITH logs_other AS (SELECT 1) SELECT * FROM main.logs_other",
            parser_con=con,
            allowed_tables=frozenset({"logs"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


def test_allowed_tables_permits_recursive_cte(con):
    """Recursive CTEs reference their own name inside the body — both
    occurrences must be accepted under the alias-expansion."""
    result = validate_user_sql(
        "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<5) SELECT * FROM x",
        parser_con=con,
        allowed_tables=frozenset({"logs"}),
    )
    assert result.parse_tree.get("statements")


def test_allowed_tables_cte_expansion_does_not_grant_unrelated_table(con):
    """CTE expansion only whitelists the declared alias names — it must
    not accidentally allow a sibling reference to an unrelated foreign
    table in the same query."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(
            "WITH x AS (SELECT 1) SELECT * FROM logs_other",
            parser_con=con,
            allowed_tables=frozenset({"logs"}),
        )
    assert exc.value.reason == "catalog_blocklist:foreign_table"


# ── apply_user_query_limits ─────────────────────────────────────────────────


def test_apply_user_query_limits_sets_memory_limit(con):
    apply_user_query_limits(con, memory_limit="256MB", timeout_seconds=5)
    # The setting is visible via current_setting (note: validator would
    # block this in user SQL — but this is a meta-test on the helper).
    val = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    # DuckDB normalizes "256MB" to bytes/MB depending on version; just
    # ensure something non-empty came back.
    assert val


# ── escape_sql_literal characterization tests (security) ───────────────
#
# These tests target the REAL attack vectors enumerated in the v6
# remediation doc, not just the happy path. They use a real DuckDB
# connection to round-trip the escaped value back through the parser, so
# any future bug in the escape function fails the test instead of silently
# allowing a known attack shape.


from backend.utils.sql_validator import escape_sql_literal  # noqa: E402


def _roundtrip(con: duckdb.DuckDBPyConnection, value: str) -> str:
    """Embed ``value`` in a DuckDB string literal via escape_sql_literal
    and confirm the engine returns it unchanged."""
    sql = f"SELECT '{escape_sql_literal(value)}'"
    return con.execute(sql).fetchone()[0]


def test_escape_simple_string(con):
    assert _roundtrip(con, "raw/2026-01-01/log.json") == "raw/2026-01-01/log.json"


def test_escape_single_quote(con):
    """Bare single quote — the textbook attack character."""
    assert _roundtrip(con, "O'Brien") == "O'Brien"


def test_escape_already_doubled_quote(con):
    """An attacker who pre-doubled their quotes shouldn't double them again."""
    assert _roundtrip(con, "''') OR 1=1 --") == "''') OR 1=1 --"


def test_escape_attack_payload_from_audit(con):
    """The exact payload shape from the audit's proof-of-concept."""
    payload = (
        "raw/inj*'], format='newline_delimited', ignore_errors=true);"
        " CREATE MACRO ignore(records, filename, columns, ignore_errors) AS NULL;"
        " CREATE TABLE pwned AS SELECT 1 AS flag;"
        " SELECT ignore(--"
    )
    assert _roundtrip(con, payload) == payload


def test_escape_backslash_is_literal(con):
    """DuckDB doesn't treat \\ as an escape character in single-quoted
    literals (standard mode), so we don't need to double it. Confirm
    round-trip is identity."""
    assert _roundtrip(con, r"a\nb\tc\\d") == r"a\nb\tc\\d"


def test_escape_control_chars(con):
    """Tabs and newlines pass through unchanged."""
    assert _roundtrip(con, "a\tb\nc") == "a\tb\nc"


def test_escape_multibyte_utf8(con):
    """A historic DuckDB bug treated 0x27 as a quote even when it appeared
    as a continuation byte inside a multi-byte UTF-8 sequence. Modern
    DuckDB handles this correctly because we operate on the Python str
    (not bytes), so only actual U+0027 code points get doubled."""
    # U+0027 is the actual quote; U+2019 is the right single quotation
    # mark (encodes as 0xE2 0x80 0x99 — no 0x27 in the byte stream but
    # similar visual character).
    assert _roundtrip(con, "café'résumé") == "café'résumé"
    assert _roundtrip(con, "smart ’quote") == "smart ’quote"


def test_escape_empty_string(con):
    assert _roundtrip(con, "") == ""


def test_escape_only_quotes(con):
    assert _roundtrip(con, "''''''") == "''''''"


def test_escape_null_byte_raises_in_duckdb(con):
    """DuckDB's parser terminates literals on NUL bytes (treats them as
    a C-style string terminator). S3 object keys cannot contain NUL
    (S3 API rejects them), so this is a non-issue for the production
    callsite — but pinning the behavior here ensures any future fix to
    the escape helper that tries to handle NUL silently doesn't end up
    in a passing test.
    """
    # escape_sql_literal itself returns the NUL unchanged. The DuckDB
    # parser is what fails — that's the layer that would silently
    # truncate a query if we ever interpolated a NUL-bearing string.
    escaped = escape_sql_literal("a\x00b")
    assert "\x00" in escaped
    with pytest.raises(duckdb.ParserException):
        con.execute(f"SELECT '{escaped}'")


def test_escape_long_string_with_many_quotes(con):
    """An attacker padding their payload shouldn't break escaping."""
    value = "x" * 1000 + "'" * 1000 + "y" * 1000
    assert _roundtrip(con, value) == value


def test_escape_rejects_non_string():
    """The helper is str-only — passing bytes or int is a programming
    error and should fail loud."""
    with pytest.raises(TypeError):
        escape_sql_literal(b"bytes")
    with pytest.raises(TypeError):
        escape_sql_literal(42)
    with pytest.raises(TypeError):
        escape_sql_literal(None)


# ── REGRESSION TESTS: BATCH B Hardening ─────────────────────────────────────


def test_query_table_function_blocked(con):
    """Finding 010: Ensure query() table function is blocked."""
    sql = "SELECT * FROM query('SELECT * FROM duckdb_secrets()')"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == "function_denylist:query"


def test_replacement_scan_blocked_by_table_name_characters(con):
    """Finding 029: Ensure table names containing slashes, backslashes, or dots (file paths/replacement scans) are rejected."""
    sql_slash = "SELECT * FROM '/etc/passwd'"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql_slash, parser_con=con)
    assert exc.value.reason == "catalog_blocklist:table_name_path"

    sql_dot = "SELECT * FROM 'data.parquet'"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql_dot, parser_con=con)
    assert exc.value.reason == "catalog_blocklist:table_name_path"


def test_query_table_function_blocked_finding_011(con):
    """Finding 011: Ensure query_table() table function is blocked."""
    sql = "SELECT * FROM query_table('my_table')"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == "function_denylist:query_table"


# ── service_id_from_source tests (merged from test_service_ids.py) ──────────

from backend.config import service_id_from_source  # noqa: E402


def test_service_id_from_source_prefers_explicit_override():
    assert service_id_from_source({"name": "name", "service_id": "id"}, "override") == "override"


def test_service_id_from_source_uses_service_id_by_default():
    assert service_id_from_source({"name": "name", "service_id": "id"}) == "id"


def test_service_id_from_source_can_prefer_name():
    assert service_id_from_source({"name": "name", "service_id": "id"}, prefer_name=True) == "name"


def test_service_id_from_source_falls_back_to_available_value():
    assert service_id_from_source({"name": "name"}) == "name"
    assert service_id_from_source({}) == ""


# ── Merged from tests/utils/test_query_retries.py ──

"""Consolidated tests for test_query_retries."""


import sys
import time
from contextlib import ExitStack, contextmanager

import httpx
import pytest
from backend.core.retry import retry_call

from backend.core import duckdb as core_duckdb
from backend.core.iceberg import execute_with_stale_view_retry, is_stale_view_error
from backend.core.realtime import poller as poller_mod
from backend.repositories._base import QueryRunner

# ── From test_iceberg_self_heal.py ──

"""Tests for the iceberg.execute_with_stale_view_retry helper.

The helper exists so background-job code paths (rdns_cache discovery,
rollups DESCRIBE) — which open raw DuckDB connections instead of going
through QueryRunner — can recover from the same buffer-deletion race
that QueryRunner.execute already handles. Production incident
2026-06-10 (~8 hours of 100%-failing rdns discovery runs spamming
the log) is the regression these tests pin against.
"""


class TestIsStaleViewError:
    @pytest.mark.parametrize(
        "msg",
        [
            'IO Error: No files found that match the pattern "cache/foo/batch_abc.parquet"',
            "Catalog Error: Table with name logs_xyz does not exist",
            "No such file or directory: /tmp/buf.parquet",
            # DuckLake's brief DETACH window during commit/optimize: a
            # reader mid-checkout hits this exact CatalogException text.
            'Catalog Error: Table with name "lake.logs" does not exist because schema "lake" does not exist.',
        ],
    )
    def test_recognises_known_messages(self, msg: str) -> None:
        assert is_stale_view_error(Exception(msg)) is True

    def test_returns_false_for_unrelated_errors(self) -> None:
        assert is_stale_view_error(Exception("Syntax error at line 1")) is False
        assert is_stale_view_error(Exception("Permission denied")) is False
        assert is_stale_view_error(Exception("Connection refused")) is False

    def test_recognises_real_duckdb_exception_from_detached_ducklake_catalog(self) -> None:
        """Not just a string fixture — a REAL DuckDB error from the exact
        DuckLake DETACH-then-query race (commit_batch/optimize briefly
        detach 'lake' on a connection other readers share), confirming the
        matcher isn't accidentally coupled to a specific error phrasing
        that a future DuckDB version could change."""
        import duckdb as _duckdb

        con = _duckdb.connect()
        con.execute("ATTACH ':memory:' AS lake")
        con.execute("CREATE TABLE lake.logs (x INT)")
        con.execute("DETACH lake")
        try:
            con.execute("SELECT * FROM lake.logs")
        except Exception as e:
            assert is_stale_view_error(e) is True
        else:
            pytest.fail("expected querying a detached catalog to raise")


class TestExecuteWithStaleViewRetry:
    def test_passthrough_when_fn_succeeds(self) -> None:
        """No retry / no cache bust when the first call succeeds."""
        con = MagicMock()
        src = {"name": "svc-a"}
        fn = MagicMock(return_value="ok")

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        fn.assert_called_once_with(con)
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_retries_once_after_stale_view_error(self) -> None:
        """First call raises a stale-view error → bust caches + force rebind + retry."""
        con = MagicMock()
        src = {"name": "svc-b"}
        fn = MagicMock(
            side_effect=[
                Exception('IO Error: No files found that match the pattern "buffer/batch_x.parquet"'),
                ["row1", "row2"],
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == ["row1", "row2"]
        assert fn.call_count == 2
        # Both calls receive the same con — i.e. retry isn't allocating a new connection.
        for call in fn.call_args_list:
            assert call.args[0] is con
        mock_clear.assert_called_once_with("svc-b", keep_snapshot_cache=True)
        mock_update.assert_called_once()
        # update_iceberg_view called with (con, src, force=True)
        upd_call = mock_update.call_args
        assert upd_call.args[0] is con
        assert upd_call.args[1] is src
        assert upd_call.kwargs.get("force") is True

    def test_non_stale_error_propagates_without_retry(self) -> None:
        """A non-stale error must surface immediately — don't waste a rebind on a real failure."""
        con = MagicMock()
        src = {"name": "svc-c"}
        fn = MagicMock(side_effect=ValueError("bad SQL"))

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            with pytest.raises(ValueError, match="bad SQL"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 1
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_second_attempt_failure_propagates(self) -> None:
        """If the retry itself fails, propagate — caller chooses fallback (log + skip vs raise).

        Pins the contract so a tempting "also swallow on retry failure" change
        wouldn't slip past silently — the caller-side `except duckdb.Error`
        block stays in charge of the user-visible behaviour.
        """
        con = MagicMock()
        src = {"name": "svc-d"}
        fn = MagicMock(
            side_effect=[
                Exception("No files found"),
                RuntimeError("still broken"),
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            with pytest.raises(RuntimeError, match="still broken"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 2

    def test_passes_through_extra_args_and_kwargs(self) -> None:
        """The helper must forward *args and **kwargs to ``fn``."""
        con = MagicMock()
        src = {"name": "svc-e"}
        fn = MagicMock(return_value=42)

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn, "a", 1, key="value")

        assert result == 42
        fn.assert_called_once_with(con, "a", 1, key="value")

    def test_default_source_key_when_name_missing(self) -> None:
        """``src`` without a ``name`` key falls back to ``"default"`` for the
        cache-bust call — never raises KeyError on the retry path."""
        con = MagicMock()
        src: dict = {}
        fn = MagicMock(side_effect=[Exception("No files found"), "ok"])

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        mock_clear.assert_called_once_with("default", keep_snapshot_cache=True)


class TestConfigureDuckdbS3:
    def test_configure_duckdb_s3_loads_extensions(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()
        configure_duckdb_s3(con)
        calls = [c.args[0] for c in con.execute.call_args_list]
        assert any("LOAD httpfs; LOAD parquet;" in c for c in calls)

    def test_configure_duckdb_s3_installs_httpfs_on_load_failure(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()
        seen: list[str] = []

        def side_effect(sql, *args):
            seen.append(sql)
            if sql == "LOAD httpfs; LOAD parquet;" and seen.count("LOAD httpfs; LOAD parquet;") == 1:
                raise RuntimeError("Extension not installed")

        con.execute.side_effect = side_effect
        configure_duckdb_s3(con)
        assert "INSTALL httpfs; INSTALL parquet;" in seen

    def test_configure_duckdb_s3_reraises_when_install_also_fails(self) -> None:
        from backend.core.iceberg.view import configure_duckdb_s3

        con = MagicMock()

        def side_effect(sql, *args):
            if "httpfs" in sql:
                raise RuntimeError("offline")

        con.execute.side_effect = side_effect
        with pytest.raises(RuntimeError, match="offline"):
            configure_duckdb_s3(con)


# ── From test_retry.py ──


class _Transient(Exception):
    pass


class _Fatal(Exception):
    pass


def _is_transient(exc: BaseException) -> bool:
    return isinstance(exc, _Transient)


def test_success_passthrough_no_sleep() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        return "ok"

    result = retry_call(fn, retry_on=_is_transient, attempts=5, sleep=sleeps.append)

    assert result == "ok"
    assert calls["n"] == 1
    assert sleeps == []


def test_retryable_retried_to_budget_then_reraises() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=4,
            base_delay=1.0,
            multiplier=2.0,
            sleep=sleeps.append,
        )

    # 4 attempts -> 3 sleeps between them
    assert calls["n"] == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_fatal_not_retried() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []


def test_eventual_success_stops_retrying() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("retry me")
        return "done"

    result = retry_call(
        fn,
        retry_on=_is_transient,
        attempts=5,
        base_delay=1.0,
        multiplier=2.0,
        sleep=sleeps.append,
    )

    assert result == "done"
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_max_delay_clamps_backoff() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            base_delay=1.0,
            multiplier=10.0,
            max_delay=5.0,
            sleep=sleeps.append,
        )

    # 1, min(10,5)=5, min(100,5)=5, min(1000,5)=5
    assert sleeps == [1.0, 5.0, 5.0, 5.0]


def test_constant_backoff_via_multiplier_one() -> None:
    sleeps: list[float] = []

    def fn() -> None:
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            multiplier=1.0,
            sleep=sleeps.append,
        )

    assert sleeps == [1.0, 1.0]


def test_before_retry_fires_between_attempts_with_exception() -> None:
    sleeps: list[float] = []
    seen: list[BaseException] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient(f"attempt-{calls['n']}")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=3,
            base_delay=1.0,
            before_retry=seen.append,
            sleep=sleeps.append,
        )

    # Hook runs once per retry (not after the final give-up attempt).
    assert len(seen) == 2
    assert all(isinstance(e, _Transient) for e in seen)
    assert [str(e) for e in seen] == ["attempt-1", "attempt-2"]


def test_before_retry_not_called_on_fatal() -> None:
    seen: list[BaseException] = []

    def fn() -> None:
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=5,
            before_retry=seen.append,
            sleep=lambda _d: None,
        )

    assert seen == []


def test_max_elapsed_budget_stops_early() -> None:
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    # tenacity's stop_after_delay reads real monotonic time, so a real (tiny)
    # sleep is required for the wall-clock budget to advance. Kept small so the
    # test stays fast while still proving the budget cuts off well before the
    # attempt cap.
    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1000,
            base_delay=0.02,
            multiplier=1.0,
            max_elapsed=0.05,
            sleep=time.sleep,
        )

    # Budget cut it off well before the 1000-attempt cap.
    assert calls["n"] < 1000


def test_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        retry_call(lambda: None, retry_on=_is_transient, attempts=0)


def test_attempts_one_calls_once_and_propagates() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=1,
            base_delay=1.0,
            sleep=sleeps.append,
        )

    assert calls["n"] == 1
    assert sleeps == []


# ── From test_retry_sites.py ──


@contextmanager
def _captured_sleeps(*extra_targets: str, only_from: str | None = None):
    """Record sleep delays instead of sleeping.

    ``only_from`` restricts the global ``time.sleep`` capture to calls made
    (within a few frames) from a file whose path ends with that suffix (other threads in the
    process also sleep). Seam targets in ``extra_targets`` record unconditionally.
    """
    delays: list[float] = []

    def _global_sleep(seconds):
        if only_from is None:
            delays.append(seconds)
            return
        frame = sys._getframe(1)
        for _ in range(8):
            if frame is None:
                return
            if frame.f_code.co_filename.endswith(only_from):
                delays.append(seconds)
                return
            frame = frame.f_back

    with ExitStack() as stack:
        stack.enter_context(patch("time.sleep", side_effect=_global_sleep))
        for target in extra_targets:
            stack.enter_context(patch(target, side_effect=delays.append, create=True))
        yield delays


def _Resp(status: int, body: dict | None = None) -> httpx.Response:
    return httpx.Response(status, json=body or {}, request=httpx.Request("GET", "https://rt.example.test/"))


class _ScriptedSession:
    def __init__(self, script: list):
        self.headers: dict[str, str] = {}
        self.script = list(script)
        self.calls = 0

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        step = self.script.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


@pytest.fixture
def _poller_creds(monkeypatch):
    from backend import config as config_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda service_id: "k")
    monkeypatch.setattr(config_mod, "get_fastly_service_id", lambda service_id: "fsid")


def _fetch(session, cursor=0):
    return poller_mod.RealtimePoller()._fetch_realtime(session, "svc", cursor)


def test_poller_retries_503_with_1s_2s_backoff_then_succeeds(_poller_creds):
    session = _ScriptedSession([_Resp(503), _Resp(503), _Resp(200, {"ok": 1})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session) == {"ok": 1}
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_exhausted_retryable_status_raises_http_error_without_final_sleep(_poller_creds):
    session = _ScriptedSession([_Resp(429), _Resp(502), _Resp(500)])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            _fetch(session)
    assert ei.value.response.status_code == 500
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_non_retryable_status_raises_immediately(_poller_creds):
    session = _ScriptedSession([_Resp(404)])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.HTTPStatusError):
            _fetch(session)
    assert session.calls == 1
    assert delays == []


def test_poller_connection_error_retries_then_raises_last(_poller_creds):
    errs = [httpx.ConnectError("a"), httpx.ConnectError("b"), httpx.ConnectError("c")]
    session = _ScriptedSession(errs)
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(httpx.ConnectError, match="c"):
            _fetch(session)
    assert session.calls == 3
    assert delays == [1, 2]


def test_poller_non_longpoll_read_timeout_is_retried(_poller_creds):
    session = _ScriptedSession([httpx.ReadTimeout("timed out"), _Resp(200, {"ok": 2})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session, cursor=0) == {"ok": 2}
    assert delays == [1]


def test_poller_longpoll_read_timeout_short_circuits(_poller_creds):
    session = _ScriptedSession([httpx.ReadTimeout("timed out")])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(poller_mod._LongPollTimeout):
            _fetch(session, cursor=123)
    assert session.calls == 1
    assert delays == []


def test_poller_unrelated_exception_is_not_retried(_poller_creds):
    session = _ScriptedSession([ValueError("boom")])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        with pytest.raises(ValueError):
            _fetch(session)
    assert session.calls == 1
    assert delays == []


def _configure(con) -> None:
    with (
        patch("backend.core.duckdb._load_httpfs"),
        patch("backend.utils.telemetry_proxy.start_proxy_server"),
        patch("backend.utils.telemetry_proxy.proxy_endpoint", return_value="http://127.0.0.1:1"),
    ):
        core_duckdb._configure_fos(con, {"name": "svc-retry", "access_key_id": "a", "secret_access_key": "b"})


def _con_failing_secret(errors: list[BaseException]) -> MagicMock:
    con = MagicMock()
    pending = list(errors)

    def _execute(sql, *args, **kwargs):
        if "CREATE OR REPLACE SECRET" in str(sql) and pending:
            raise pending.pop(0)
        return MagicMock()

    con.execute.side_effect = _execute
    return con


def _secret_calls(con) -> int:
    return sum(1 for c in con.execute.call_args_list if "CREATE OR REPLACE SECRET" in str(c.args[0]))


def test_fos_secret_retries_write_write_conflict_with_rollback_each_attempt():
    con = _con_failing_secret([Exception("Write-Write Conflict on x"), Exception("write-write conflict")])
    with _captured_sleeps() as delays:
        _configure(con)
    assert _secret_calls(con) == 3
    assert con.rollback.call_count == 3
    assert delays == []


def test_fos_secret_third_conflict_is_raised():
    con = _con_failing_secret([Exception("write-write conflict")] * 3)
    with _captured_sleeps():
        with pytest.raises(Exception, match="write-write conflict"):
            _configure(con)
    assert _secret_calls(con) == 3


def test_fos_secret_other_error_raises_immediately():
    con = _con_failing_secret([RuntimeError("bad key")])
    with pytest.raises(RuntimeError, match="bad key"):
        _configure(con)
    assert _secret_calls(con) == 1


def _stale() -> Exception:
    return duckdb.IOException("No files found: batch_x.parquet")


def _runner(side_effects: list) -> QueryRunner:
    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc"})
    runner.con = MagicMock()
    runner.con.execute.side_effect = side_effects
    return runner


def test_execute_with_retry_success_does_not_refresh():
    runner = _runner(["cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    refresh.assert_not_called()


def test_execute_with_retry_stale_then_success_refreshes_once():
    runner = _runner([_stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    assert refresh.call_count == 1
    assert runner.con.execute.call_count == 2


def test_execute_with_retry_non_stale_raises_without_refresh():
    runner = _runner([RuntimeError("syntax")])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        with pytest.raises(RuntimeError, match="syntax"):
            runner.execute_with_retry("SELECT 1")
    refresh.assert_not_called()


def test_execute_with_retry_outer_retry_after_inner_self_heal_exhausted():
    # QueryRunner.execute self-heals once internally; the outer retry only
    # fires when that inner retry also surfaces a stale-view error.
    runner = _runner([_stale(), _stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.execute_with_retry("SELECT 1") == "cur"
    assert refresh.call_count == 2
    assert runner.con.execute.call_count == 3


def test_execute_with_retry_non_stale_failure_after_outer_refresh_returns_none():
    runner = _runner([_stale(), _stale(), RuntimeError("late")])
    with patch("backend.core.iceberg.update_iceberg_view"):
        assert runner.execute_with_retry("SELECT 1") is None
    assert runner.con.execute.call_count == 3


def test_execute_with_retry_refresh_failure_returns_none():
    runner = _runner([_stale()])
    with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("refresh")):
        assert runner.execute_with_retry("SELECT 1") is None
    assert runner.con.execute.call_count == 1


def test_create_temp_table_stale_then_success_is_true_after_one_refresh():
    runner = _runner([_stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is True
    assert refresh.call_count == 1


def test_create_temp_table_outer_retry_after_inner_self_heal_exhausted():
    runner = _runner([_stale(), _stale(), "cur"])
    with patch("backend.core.iceberg.update_iceberg_view") as refresh:
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is True
    assert refresh.call_count == 2


def test_create_temp_table_non_stale_after_outer_refresh_is_false():
    runner = _runner([_stale(), _stale(), RuntimeError("late")])
    with patch("backend.core.iceberg.update_iceberg_view"):
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is False


def test_create_temp_table_refresh_failure_is_false():
    runner = _runner([_stale()])
    with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("refresh")):
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is False


# ── Merged from test_http_make_client.py ──────────────────────────────────────────────

import pytest
from backend.utils.http import make_client


def test_make_client_default_timeout_matches_httpx_default():
    with make_client() as c:
        assert c.timeout == httpx.Timeout(5.0)


def test_make_client_uses_injected_transport_and_timeout():
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True}))
    with make_client(httpx.Timeout(2.0), transport=transport) as c:
        assert c.timeout == httpx.Timeout(2.0)
        assert c.get("https://example.invalid/x").json() == {"ok": True}


@pytest.fixture
def rt_creds(monkeypatch):
    from backend import config as config_mod

    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda sid: "tok-test")
    monkeypatch.setattr(config_mod, "get_fastly_service_id", lambda sid: "FSID")
    monkeypatch.setattr(poller_mod, "_retry_sleep", lambda s: None)


def test_poller_fetch_retries_503_then_succeeds_and_sends_key(rt_creds):
    seen: list[tuple[str, str]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((str(req.url), req.headers["Fastly-Key"]))
        if len(seen) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"Timestamp": 5, "Data": []})

    with make_client(transport=httpx.MockTransport(handler)) as c:
        out = poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 0)

    assert out == {"Timestamp": 5, "Data": []}
    assert len(seen) == 2
    assert seen[0][0] == "https://rt.fastly.com/v1/channel/FSID/ts/h?limit=120"
    assert all(k == "tok-test" for _, k in seen)


def test_poller_fetch_does_not_retry_403(rt_creds):
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(403)

    with make_client(transport=httpx.MockTransport(handler)) as c:
        with pytest.raises(httpx.HTTPStatusError):
            poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 0)
    assert calls == 1


def test_poller_longpoll_read_timeout_raises_gap_signal(rt_creds):
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    with make_client(transport=httpx.MockTransport(handler)) as c:
        with pytest.raises(poller_mod._LongPollTimeout):
            poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 123)
