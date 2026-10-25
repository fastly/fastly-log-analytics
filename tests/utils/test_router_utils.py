"""Consolidated tests for test_router_utils."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.utils import router_utils
from backend.utils.router_utils import require_fastly_token, resolve_fastly_token, stored_fastly_token

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


def test_router_utils_redaction_uses_the_shared_salt_registry():
    salt = "test-cid-salt-for-redaction"
    router_utils.register_salt(salt)

    assert router_utils.redact_error_text(f"{salt} /app/data/private.db") == ("[redacted] <path>")


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


def test_query_errors_maps_lookup_error_to_404():
    """``LookupError`` (KeyError / IndexError) → 404. Used by repo
    layers to signal "row not found" without a custom exception type."""

    @router_utils.query_errors()
    def handler():
        raise KeyError("alert-id-nope")

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == 404


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


# ── query_errors: async handler support (M4) ─────────────────────────────────


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


def test_override_wins_without_reading_config(configs) -> None:
    assert resolve_fastly_token("svc-a", "override") == "override"
    assert require_fastly_token("svc-a", "override") == "override"
    assert configs == []


@pytest.mark.parametrize("empty", ["", None])
def test_stored_key_used_when_override_is_empty(configs, empty) -> None:
    assert resolve_fastly_token("svc-a", empty) == "stored-a"
    assert require_fastly_token("svc-a", empty) == "stored-a"


def test_lenient_variant_returns_empty_string_instead_of_raising(configs) -> None:
    assert resolve_fastly_token("svc-missing") == ""
    assert resolve_fastly_token("svc-nokey") == ""
    assert resolve_fastly_token("svc-nullkey") == ""


def test_empty_service_id_never_borrows_the_active_services_key(configs) -> None:
    assert resolve_fastly_token("") == ""
    with pytest.raises(HTTPException) as exc:
        require_fastly_token("")
    assert exc.value.status_code == 404


def test_strict_variant_errors_match_the_historical_rum_bodies(configs) -> None:
    with pytest.raises(HTTPException) as missing:
        require_fastly_token("svc-missing")
    assert (missing.value.status_code, missing.value.detail) == (404, {"error": "No config for service svc-missing"})
    with pytest.raises(HTTPException) as nokey:
        require_fastly_token("svc-nokey")
    assert (nokey.value.status_code, nokey.value.detail) == (400, {"error": "No Fastly API key for service svc-nokey"})


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


def test_sse_response_returns_event_source_response():
    resp = sse_response(_stream())
    assert isinstance(resp, EventSourceResponse)


def test_sse_response_default_ping_is_5():
    resp = sse_response(_stream())
    assert resp._ping_interval == 5


def test_sse_response_ping_passthrough():
    resp = sse_response(_stream(), ping=11)
    assert resp._ping_interval == 11


def test_sse_response_applies_passthrough_headers():
    resp = sse_response(_stream())
    headers = _headers(resp)
    for key, value in SSE_PASSTHROUGH_HEADERS.items():
        assert headers[key.lower()] == value


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


def test_analyst_allowed_services_returns_none_for_admin():
    """Admin sessions don't carry an analyst_session; the helper signals
    "no scope filter" via None so caller code can branch on
    ``allowed is not None``."""
    assert analyst_allowed_services(_req(None)) is None


def test_analyst_allowed_services_returns_set_for_scoped_analyst():
    """An analyst scoped to two services gets a set of those two ids —
    callers use it to filter list/get endpoints."""
    sess = SimpleNamespace(service_ids=["svc-A", "svc-B"])
    assert analyst_allowed_services(_req(sess)) == {"svc-A", "svc-B"}


def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_none():
    """An analyst session without service_ids (defensive null) gets an
    empty set — NOT None. Returning None would collapse to the admin
    branch and grant unrestricted access; an empty set correctly
    blocks every service."""
    sess = SimpleNamespace(service_ids=None)
    assert analyst_allowed_services(_req(sess)) == set()


def test_analyst_allowed_services_returns_empty_set_when_service_ids_is_empty():
    """Same defense as the None case for the [] variant."""
    sess = SimpleNamespace(service_ids=[])
    assert analyst_allowed_services(_req(sess)) == set()


def test_mask_ips_for_admin_is_false():
    """Admin (no analyst session) never masks."""
    assert mask_ips_for(None) is False


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


@pytest.mark.security_regression
def test_admin_passes_without_touching_the_repository() -> None:
    fetch = _Fetch({"service_id": "other"})
    assert require_owned_row(_req(None), "svcA", "row-1", fetch) is None
    assert fetch.calls == []


@pytest.mark.security_regression
def test_analyst_owning_the_row_passes_and_row_is_looked_up_in_the_addressed_service() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcA"})
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch) == {"id": "row-1", "service_id": "svcA"}
    assert fetch.calls == [("row-1", "svcA")]


@pytest.mark.security_regression
def test_analyst_addressing_an_out_of_scope_service_is_rejected_before_any_lookup() -> None:
    fetch = _Fetch(None)
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcB", "row-1", fetch)
    _forbidden(exc, "svcB")
    assert fetch.calls == []


@pytest.mark.security_regression
def test_analyst_in_scope_service_but_row_owned_elsewhere_is_rejected() -> None:
    fetch = _Fetch({"id": "row-1", "service_id": "svcB"})
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", fetch)
    _forbidden(exc, "svcB")


@pytest.mark.security_regression
def test_row_without_service_id_is_rejected_for_an_analyst() -> None:
    with pytest.raises(HTTPException) as exc:
        require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch({"id": "row-1"}))
    _forbidden(exc, None)


@pytest.mark.security_regression
def test_missing_row_in_scope_passes_so_the_mutation_can_report_its_own_not_found() -> None:
    assert require_owned_row(_req(["svcA"]), "svcA", "row-1", _Fetch(None)) is None


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


@pytest.fixture(autouse=True)
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


def test_channel_is_scoped_by_prefix_and_service():
    """Two services must never share a channel — a collision would fan one
    tenant's events out to another tenant's subscribers."""
    pub = ValkeyPublisher(channel_prefix="sync-status")
    assert pub._channel("svc-a") == "sse:sync-status:svc-a"
    assert pub._channel("svc-b") != pub._channel("svc-a")

    other_feed = ValkeyPublisher(channel_prefix="cron-runs")
    assert other_feed._channel("svc-a") != pub._channel("svc-a")


# ── publish routing ──────────────────────────────────────────────────────────


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


def test_history_is_not_written_when_disabled():
    """``history_size=0`` (the sync-status feed) must not accumulate keys in
    Valkey — that would be an unbounded memory leak per service."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=0)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert not [c for c in client.pipe.staged if c[0] in ("rpush", "ltrim")]


def test_history_is_capped_when_enabled():
    """With history on, every publish appends AND trims — the trim is what
    bounds growth."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=5)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert ("rpush", "sse:sync-status:svc-a:history", json.dumps({"n": 1})) in client.pipe.staged
    assert ("ltrim", "sse:sync-status:svc-a:history", -5, -1) in client.pipe.staged


def test_get_recent_ticks_empty_when_history_disabled():
    pub = ValkeyPublisher(history_size=0)
    with patch.object(vp, "_get_sync_redis") as client:
        assert pub.get_recent_ticks("svc-a") == []
    client.assert_not_called()


def test_get_recent_ticks_decodes_history():
    client = MagicMock()
    client.lrange.return_value = [json.dumps({"n": 1}), json.dumps({"n": 2})]
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a", count=2) == [{"n": 1}, {"n": 2}]

    client.lrange.assert_called_once_with("sse:sync-status:svc-a:history", -2, -1)


def test_get_recent_ticks_survives_a_broken_broker():
    """The SSE seed must degrade to "no history" rather than 500 the stream."""
    client = MagicMock()
    client.lrange.side_effect = RuntimeError("valkey down")
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a") == []


# ── subscriber_count (cross-pod) ─────────────────────────────────────────────


def test_subscriber_count_reads_across_pods():
    """This is what suspends the RT poller when nobody is watching — it must
    reflect subscribers on OTHER pods, which is why it goes to Valkey."""
    client = MagicMock()
    client.pubsub_numsub.return_value = [("sse:sync-status:svc-a", 3)]
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 3


def test_subscriber_count_is_zero_on_error():
    client = MagicMock()
    client.pubsub_numsub.side_effect = RuntimeError("down")
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 0


# ── subscribe ────────────────────────────────────────────────────────────────


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
