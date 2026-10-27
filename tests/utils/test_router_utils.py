"""Consolidated tests for test_router_utils."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import AsyncIterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest
from backend.core.retry import retry_call
from backend.core.storage.view import configure_duckdb_s3
from backend.utils.auth import analyst_allowed_services, mask_ips_for, require_owned_row
from backend.utils.http import make_client
from backend.utils.sql_validator import (
    _ALLOWED_TABLE_FUNCTIONS,
    MAX_INPUT_BYTES,
    SQLValidationError,
    apply_user_query_limits,
    escape_sql_literal,
    validate_user_sql,
)
from backend.utils.sse import sse_response
from backend.utils.valkey_publisher import ValkeyPublisher
from fastapi import HTTPException
from sse_starlette.sse import EventSourceResponse

from backend import config as config_mod
from backend.config import service_id_from_source
from backend.core import duckdb as core_duckdb
from backend.core.realtime import poller as poller_mod
from backend.core.storage import execute_with_stale_view_retry, is_stale_view_error
from backend.repositories._base import QueryRunner
from backend.utils import router_utils
from backend.utils import valkey_publisher as vp
from backend.utils.router_utils import (
    SSE_PASSTHROUGH_HEADERS,
    require_fastly_token,
    resolve_fastly_token,
    stored_fastly_token,
)
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
    # The function imports state_sync lazily; patch on the source module
    import backend.state_sync as ss

    monkeypatch.setattr(ss, "export_admin_state", calls.append)
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


@pytest.mark.parametrize(
    ("raised", "expected_status", "expected_detail"),
    [
        # HTTPException is the existing app-layer error type — wrapping would double-encode.
        (HTTPException(status_code=418, detail={"error": "teapot"}), 418, {"error": "teapot"}),
        # ValueError (validation, bad arg) → 400 with canonical bad_request code and no trace.
        (ValueError("invalid filter shape"), 400, {"error": "bad_request", "message": "invalid filter shape"}),
        # LookupError (KeyError / IndexError) → 404 for repo "row not found" signaling.
        (KeyError("alert-id-nope"), 404, None),
        # Default status_code is 400 (client error) when unconfigured.
        (RuntimeError("bad"), 400, None),
    ],
)
def test_query_errors_sync_exception_mapping(raised, expected_status, expected_detail):
    @router_utils.query_errors()
    def handler():
        raise raised

    with pytest.raises(HTTPException) as exc:
        handler()
    assert exc.value.status_code == expected_status
    if expected_detail is not None:
        assert exc.value.detail == expected_detail
        assert "trace" not in exc.value.detail


@pytest.mark.parametrize("is_async", [False, True])
def test_query_errors_maps_unknown_exception_to_configured_status_without_trace(caplog, is_async):
    """Security: generic exceptions surface ONLY a stable machine code
    (``unhandled_error``) + correlation id for both sync and async handlers.
    The exception message is logged server-side via ``raise_internal`` but
    MUST NOT appear in the response body or include a ``trace`` key."""

    @router_utils.query_errors(status_code=500)
    def sync_handler():
        raise RuntimeError("downstream blew up")

    @router_utils.query_errors(status_code=500)
    async def async_handler():
        raise RuntimeError("downstream blew up")

    with caplog.at_level(logging.ERROR), pytest.raises(HTTPException) as exc:
        if is_async:
            asyncio.run(async_handler())
        else:
            sync_handler()
    assert exc.value.status_code == 500
    assert exc.value.detail["error"] == "unhandled_error"
    assert "downstream blew up" not in str(exc.value.detail)
    assert exc.value.detail.get("error_id"), "raise_internal should attach an error_id"
    assert "trace" not in (exc.value.detail or {}), (
        "stack-trace leakage regression — query_errors must not put a 'trace' key in the response detail (security)"
    )


def test_query_errors_preserves_function_metadata_and_forwards_args():
    """``functools.wraps`` keeps __name__ and __doc__ for FastAPI OpenAPI op IDs
    and forwards positional/keyword arguments unchanged."""

    @router_utils.query_errors()
    def my_endpoint(a, b, *, c):
        """Docstring for OpenAPI."""
        return a + b + c

    assert my_endpoint.__name__ == "my_endpoint"
    assert my_endpoint.__doc__ == "Docstring for OpenAPI."
    assert my_endpoint(1, 2, c=3) == 6


# ── query_errors: async handler support (M4) ─────────────────────────────────


def test_query_errors_wraps_async_handler_and_returns_value():
    """Async route handlers (introduced with M4 for asyncio.gather
    parallelisation of Fastly calls in usage::prefill) must be awaited."""

    @router_utils.query_errors()
    async def handler() -> dict:
        await asyncio.sleep(0)
        return {"ok": True}

    assert asyncio.run(handler()) == {"ok": True}


@pytest.mark.parametrize(
    ("raised", "expected_status", "expected_detail"),
    [
        (ValueError("bad input"), 400, {"error": "bad_request", "message": "bad input"}),
        (HTTPException(status_code=502, detail={"error": "upstream down"}), 502, {"error": "upstream down"}),
    ],
)
def test_query_errors_async_exception_mapping(raised, expected_status, expected_detail):
    """ValueError → 400 and HTTPException passthrough pinned for async handlers."""

    @router_utils.query_errors()
    async def handler():
        raise raised

    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler())
    assert exc.value.status_code == expected_status
    assert exc.value.detail == expected_detail


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
    log = logging.getLogger("test.raise_internal")
    leaky = RuntimeError("HTTP 502 GET /tokens/self\n    internal.fastly.svc:5001 timed out")
    with caplog.at_level(logging.ERROR, logger="test.raise_internal"), pytest.raises(HTTPException) as exc:
        router_utils.raise_internal(log, leaky, code="my_endpoint_failed", status=500)

    assert exc.value.status_code == 500
    detail = exc.value.detail or {}
    assert detail.get("error") == "my_endpoint_failed"
    assert "error_id" in detail and len(detail["error_id"]) == 8  # 8-char hex prefix
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

    @router_utils.query_errors()
    async def handler():
        async def _slow(val: str) -> str:
            await asyncio.sleep(0.10)
            return val

        a, b = await asyncio.gather(_slow("a"), _slow("b"))
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
    store: dict[str, dict | None] = {
        "active-svc": {"fastly_api_key": "ACTIVE-KEY"},
        "svc-a": {"fastly_api_key": "stored-a"},
        "svc-nokey": {"service_id": "svc-nokey"},
        "svc-nullkey": {"fastly_api_key": None},
    }
    loaded: list[str] = []

    def _load(service_id):
        loaded.append(service_id)
        return store.get(service_id)

    monkeypatch.setattr("backend.config.load_config", _load)
    monkeypatch.setattr("backend.config.get_active_service_id", lambda *a, **k: "active-svc")
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
    with caplog.at_level(logging.DEBUG):
        resolve_fastly_token("svc-a")
        require_fastly_token("svc-a")
        stored_fastly_token({"fastly_api_key": "stored-a"})
    assert "stored-a" not in caplog.text


# ── Merged from test_sse.py ──────────────────────────────────────────────


async def _stream() -> AsyncIterator[str]:
    yield "data: hello\n\n"


def _headers(resp: EventSourceResponse) -> dict[str, str]:
    return {k.decode().lower(): v.decode() for k, v in resp.raw_headers}


@pytest.mark.parametrize(("kwargs", "expected_ping"), [({}, 5), ({"ping": 11}, 11)])
def test_sse_response_type_ping_and_passthrough_headers(kwargs, expected_ping):
    resp = sse_response(_stream(), **kwargs)
    assert isinstance(resp, EventSourceResponse) and resp._ping_interval == expected_ping
    headers = _headers(resp)
    for key, value in SSE_PASSTHROUGH_HEADERS.items():
        assert headers[key.lower()] == value


def test_routers_build_sse_only_via_sse_response():
    routers = Path(__file__).resolve().parents[2] / "backend" / "routers"
    offenders = [str(p.relative_to(routers)) for p in routers.rglob("*.py") if "EventSourceResponse(" in p.read_text()]
    assert offenders == []


# ── Merged from test_auth.py ──────────────────────────────────────────────


def _req(analyst_session: object | None) -> SimpleNamespace:
    if isinstance(analyst_session, list):
        analyst_session = SimpleNamespace(service_ids=analyst_session)
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


@pytest.mark.parametrize(
    ("session", "expected"),
    [
        # Admin sessions carry no analyst_session; None signals "no scope filter".
        (None, None),
        # Analyst scoped to two services gets a set of those ids.
        (SimpleNamespace(service_ids=["svc-A", "svc-B"]), {"svc-A", "svc-B"}),
        # Analyst session with None or empty service_ids gets set() (NOT None) to block all services.
        (SimpleNamespace(service_ids=None), set()),
        (SimpleNamespace(service_ids=[]), set()),
    ],
)
def test_analyst_allowed_services(session, expected):
    assert analyst_allowed_services(_req(session)) == expected


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
    assert mask_ips_for(SimpleNamespace(pii_policy=policy)) is expected


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
    assert ValkeyPublisher(channel_prefix="cron-runs")._channel("svc-a") != pub._channel("svc-a")


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
    with patch.object(vp, "_get_sync_redis", return_value=client):
        ValkeyPublisher().publish("svc-a", {"status": "ok"})  # must not raise
    assert vp.PUBLISH_STATS["publish_errors"] == 1
    assert vp.PUBLISH_STATS["published"] == 0


def test_publish_with_bound_loop_goes_through_the_loop():
    """With a loop bound (the FastAPI process) the publish rides asyncio, not
    the sync client."""
    pub = ValkeyPublisher()
    pub._loop = loop = MagicMock()
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
    pub._loop = loop = MagicMock()
    loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
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
    pub._redis = redis = MagicMock()
    redis.pipeline.side_effect = RuntimeError("broker gone")
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
    with patch.object(vp, "_get_sync_redis", return_value=client):
        ValkeyPublisher(history_size=0).publish("svc-a", {"n": 1})
    assert not [c for c in client.pipe.staged if c[0] in ("rpush", "ltrim")]


def test_history_is_capped_when_enabled():
    """With history on, every publish appends AND trims — the trim is what
    bounds growth."""
    client = _fake_sync_client()
    with patch.object(vp, "_get_sync_redis", return_value=client):
        ValkeyPublisher(history_size=5).publish("svc-a", {"n": 1})
    assert ("rpush", "sse:sync-status:svc-a:history", json.dumps({"n": 1})) in client.pipe.staged
    assert ("ltrim", "sse:sync-status:svc-a:history", -5, -1) in client.pipe.staged


def test_get_recent_ticks_empty_when_history_disabled():
    with patch.object(vp, "_get_sync_redis") as client:
        assert ValkeyPublisher(history_size=0).get_recent_ticks("svc-a") == []
    client.assert_not_called()


def test_get_recent_ticks_decodes_history():
    client = MagicMock()
    client.lrange.return_value = [json.dumps({"n": 1}), json.dumps({"n": 2})]
    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert ValkeyPublisher(history_size=10).get_recent_ticks("svc-a", count=2) == [{"n": 1}, {"n": 2}]
    client.lrange.assert_called_once_with("sse:sync-status:svc-a:history", -2, -1)


def test_get_recent_ticks_survives_a_broken_broker():
    """The SSE seed must degrade to "no history" rather than 500 the stream."""
    client = MagicMock()
    client.lrange.side_effect = RuntimeError("valkey down")
    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert ValkeyPublisher(history_size=10).get_recent_ticks("svc-a") == []


# ── subscriber_count (cross-pod) ─────────────────────────────────────────────


@pytest.mark.parametrize(
    ("side_effect", "expected"), [([[("sse:sync-status:svc-a", 3)]], 3), (RuntimeError("down"), 0)]
)
def test_subscriber_count_reads_across_pods_and_defaults_to_zero_on_error(side_effect, expected):
    """Suspends the RT poller when nobody across pods is watching; degrades to 0 on error."""
    client = MagicMock()
    client.pubsub_numsub.side_effect = side_effect
    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert ValkeyPublisher().subscriber_count("svc-a") == expected


# ── subscribe ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_before_bind_loop_raises():
    """Explicit contract: subscribing is only meaningful in the SSE-serving
    process, and a silent no-op here would hang the stream forever."""
    with pytest.raises(RuntimeError, match="bind_loop"):
        await anext(ValkeyPublisher().subscribe("svc-a"))


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


# ── Merged from tests/utils/test_sql_validator.py ──
#
# Acceptance tests for the DuckDB user-SQL validator (security).
# The Decision B policy is defined in security_remediation_final_v6.md §2 and
# implemented in backend/utils/sql_validator.py. This file pins every
# acceptance criterion called out in that document so a future refactor
# can't silently weaken the policy.
# The validator runs against a real DuckDB connection (the same engine the
# production app uses), so test coverage is end-to-end on the parse-tree
# walker — no mocking the json_serialize_sql output.


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
    ("sql", "blocked_function"),
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
    assert exc.value.reason == f"function_denylist:{blocked_function}"


# ── REGRESSION (C1): denylist→allowlist for table functions ─────────────────
#
# The pre-fix gate was a function-NAME denylist. DuckDB 1.5.x ships file/
# external-resource table functions that were never in that list, so an
# authenticated analyst could read arbitrary server files and other tenants'
# databases through /api/query. The fix adds (a) a static name-prefix
# denylist and (b) a table-function ALLOWLIST derived from the live engine's
# duckdb_functions(). These tests pin both halves.


@pytest.mark.parametrize(
    ("sql", "expected_prefix"),
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
    assert exc.value.reason == f"function_prefix:{expected_prefix}"


@pytest.mark.parametrize(
    ("sql", "fn"),
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
    assert exc.value.reason == f"function_table_not_allowed:{fn}"


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
        assert validate_user_sql(sql, parser_con=con).parse_tree.get("statements"), sql


def test_c1_ordinary_scalar_query_still_passes(con):
    """A representative analytics query (scalar/aggregate functions only)
    must not be caught by the table-function gate."""
    sql = (
        "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n, "
        "regexp_matches(url, 'admin') AS hit, lower(ua) AS ua "
        "FROM logs GROUP BY 1, 3, 4 ORDER BY 2 DESC LIMIT 100"
    )
    assert validate_user_sql(sql, parser_con=con, allowed_tables=frozenset({"logs"})).parse_tree.get("statements")


def test_c1_allowlist_tracks_live_engine_enumeration(con):
    """Belt check: every table-typed function the live build exposes is either
    explicitly allowlisted or rejected by validate_user_sql — there is no
    table function that silently passes. Guards against a future engine build
    adding a file reader that slips the gate."""
    rows = con.execute("SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type='table'").fetchall()
    names = [r[0] for r in rows]
    assert names, "expected a non-empty table-function enumeration"
    leaked = []
    for name in names:
        if name.lower() in _ALLOWED_TABLE_FUNCTIONS:
            continue
        # Call shape doesn't matter — we assert it is rejected at validation,
        # never executed. A parse error also counts as "did not pass".
        try:
            validate_user_sql(f"SELECT * FROM {name}()", parser_con=con)
        except Exception:
            continue
        leaked.append(name)
    assert not leaked, f"table functions passed validation without being allowlisted: {leaked}"


# ── ACCEPTANCE: catalog table prefix & schema_name blocklist ────────────────


@pytest.mark.parametrize(
    ("sql", "blocked_prefix"),
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


@pytest.mark.parametrize(
    ("sql", "blocked_schema"),
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


# ── ACCEPTANCE: statement-type whitelist & multi-statement ──────────────────


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


def test_multi_statement_rejected(con):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql("SELECT 1; SELECT 2", parser_con=con)
    assert exc.value.reason in ("multi_statement", "no_statements")


def test_injection_classic_attempted(con):
    """The original audit attack vector: prepended ATTACH after the
    legitimate SELECT. Must be caught by either the multi-statement check
    or the statement-type check on the ATTACH branch."""
    with pytest.raises(SQLValidationError):
        validate_user_sql("SELECT 1; ATTACH '/etc/passwd' AS x", parser_con=con)


# ── ACCEPTANCE: input size ceiling & fail-closed on parse error ─────────────


def test_input_too_large_rejected(con):
    huge = "SELECT 1 WHERE 1 IN (" + ",".join(str(i) for i in range(20_000)) + ")"
    assert len(huge.encode()) > MAX_INPUT_BYTES
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(huge, parser_con=con)
    assert exc.value.reason == "input_too_large"


def test_malformed_sql_rejected(con):
    """json_serialize_sql returns {'error': true, ...} for parse failures
    rather than raising. The validator must NOT silently pass these
    through — fail closed."""
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql("SELECT FROM WHERE * NOT-VALID", parser_con=con)
    assert exc.value.reason.startswith("parse_error")


@pytest.mark.parametrize("sql", ["", "   \n\t  "])
def test_empty_or_whitespace_input_rejected(con, sql):
    """Empty string is technically zero statements — must fail closed."""
    with pytest.raises(SQLValidationError):
        validate_user_sql(sql, parser_con=con)


# ── SHOW_REF reject & allowed_tables per-call allowlist (audit F-8/9/10) ────


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


_DEFAULT_ALLOWED = frozenset({"logs", "logs_examplesvcid0000000001"})


@pytest.mark.parametrize(
    ("sql", "allowed"),
    [
        # ``logs`` is in the allowlist — should pass.
        ("SELECT count(*) FROM logs", _DEFAULT_ALLOWED),
        # The per-service rewritten table name is in the allowlist — should pass.
        ("SELECT count(*) FROM logs_examplesvcid0000000001", _DEFAULT_ALLOWED),
        # Allowlist matches lowercase — caller passes lowercased names.
        ("SELECT count(*) FROM LOGS", frozenset({"logs"})),
        # When allowed_tables is None (the default), no per-table allowlist is enforced.
        ("SELECT count(*) FROM logs_some_random_service", None),
        # ``WITH x AS (...) SELECT * FROM x`` references ``x`` as a BASE_TABLE — CTE-alias exception.
        ("WITH x AS (SELECT count(*) FROM logs) SELECT * FROM x", frozenset({"logs"})),
        # Lexically-scoped CTE tracking must still allow nested + multi CTE definitions.
        ("WITH a AS (WITH b AS (SELECT count(*) FROM logs) SELECT * FROM b) SELECT * FROM a", frozenset({"logs"})),
        ("WITH p AS (SELECT 1 z), q AS (SELECT 2 y) SELECT p.z + q.y FROM p, q", frozenset({"logs"})),
        # A top-level CTE legitimately covers both UNION branches.
        ("WITH t AS (SELECT 1 z) SELECT * FROM t UNION ALL SELECT * FROM t", frozenset({"logs"})),
        # Recursive CTEs reference their own name inside the body — both occurrences accepted.
        (
            "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<5) SELECT * FROM x",
            frozenset({"logs"}),
        ),
    ],
)
def test_allowed_tables_permits_valid_queries(con, sql, allowed):
    assert validate_user_sql(sql, parser_con=con, allowed_tables=allowed).parse_tree.get("statements")


@pytest.mark.parametrize(
    ("sql", "allowed"),
    [
        # When allowed_tables is set, any BASE_TABLE name outside the set is rejected (cross-tenant defense).
        ("SELECT count(*) FROM logs_test_service", _DEFAULT_ALLOWED),
        # Walker recurses into sub-selects — the allowlist must apply there too.
        ("SELECT * FROM (SELECT count(*) FROM logs_other) AS sub", frozenset({"logs"})),
        # A JOIN against a foreign table must be rejected.
        ("SELECT * FROM logs a JOIN logs_other b ON a.id = b.id", frozenset({"logs"})),
        # Finding 009 (run 80e9f210): a CTE declared in an INNER subquery must not whitelist its name
        # for a SIBLING outer reference, which DuckDB resolves to the real base table.
        ("SELECT * FROM logs_other, (WITH logs_other AS (SELECT 1) SELECT * FROM logs_other) sub", frozenset({"logs"})),
        # Finding 017 (run 80e9f210): a same-named CTE must not excuse a schema/catalog-qualified
        # reference — ``main.logs_other`` resolves to the base table in DuckDB, not the CTE.
        ("WITH logs_other AS (SELECT 1) SELECT * FROM main.logs_other", frozenset({"logs"})),
        # CTE expansion only whitelists the declared alias names — not an unrelated foreign table.
        ("WITH x AS (SELECT 1) SELECT * FROM logs_other", frozenset({"logs"})),
    ],
)
def test_allowed_tables_blocks_foreign_references(con, sql, allowed):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con, allowed_tables=allowed)
    assert exc.value.reason == "catalog_blocklist:foreign_table"


# ── apply_user_query_limits ─────────────────────────────────────────────────


def test_apply_user_query_limits_sets_memory_limit(con):
    apply_user_query_limits(con, memory_limit="256MB", timeout_seconds=5)
    # The setting is visible via current_setting (note: validator would
    # block this in user SQL — but this is a meta-test on the helper).
    assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0]


# ── escape_sql_literal characterization tests (security) ───────────────
#
# These tests target the REAL attack vectors enumerated in the v6
# remediation doc, not just the happy path. They use a real DuckDB
# connection to round-trip the escaped value back through the parser, so
# any future bug in the escape function fails the test instead of silently
# allowing a known attack shape.


def _roundtrip(con: duckdb.DuckDBPyConnection, value: str) -> str:
    """Embed ``value`` in a DuckDB string literal via escape_sql_literal
    and confirm the engine returns it unchanged."""
    return con.execute(f"SELECT '{escape_sql_literal(value)}'").fetchone()[0]


@pytest.mark.parametrize(
    "value",
    [
        "raw/2026-01-01/log.json",
        "O'Brien",  # bare single quote — textbook attack character
        "''') OR 1=1 --",  # already doubled quote
        (
            "raw/inj*'], format='newline_delimited', ignore_errors=true);"
            " CREATE MACRO ignore(records, filename, columns, ignore_errors) AS NULL;"
            " CREATE TABLE pwned AS SELECT 1 AS flag;"
            " SELECT ignore(--"
        ),  # exact payload shape from the audit's proof-of-concept
        r"a\nb\tc\\d",  # backslash is literal in DuckDB standard mode
        "a\tb\nc",  # control chars pass through unchanged
        "café'résumé",  # multibyte UTF-8 with U+0027
        "smart ’quote",  # U+2019 right single quotation mark
        "",  # empty string
        "''''''",  # only quotes
        "x" * 1000 + "'" * 1000 + "y" * 1000,  # long string with many quotes
    ],
)
def test_escape_roundtrip(con, value):
    assert _roundtrip(con, value) == value


def test_escape_null_byte_raises_in_duckdb(con):
    """DuckDB's parser terminates literals on NUL bytes (treats them as
    a C-style string terminator). S3 object keys cannot contain NUL
    (S3 API rejects them), so this is a non-issue for the production
    callsite — but pinning the behavior here ensures any future fix to
    the escape helper that tries to handle NUL silently doesn't end up
    in a passing test.
    """
    escaped = escape_sql_literal("a\x00b")
    assert "\x00" in escaped
    with pytest.raises(duckdb.ParserException):
        con.execute(f"SELECT '{escaped}'")


@pytest.mark.parametrize("bad_input", [b"bytes", 42, None])
def test_escape_rejects_non_string(bad_input):
    """The helper is str-only — passing bytes or int is a programming error and should fail loud."""
    with pytest.raises(TypeError):
        escape_sql_literal(bad_input)


# ── REGRESSION TESTS: BATCH B Hardening ─────────────────────────────────────


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        # Finding 010: Ensure query() table function is blocked.
        ("SELECT * FROM query('SELECT * FROM duckdb_secrets()')", "function_denylist:query"),
        # Finding 029: Ensure table names containing slashes, backslashes, or dots (replacement scans) are rejected.
        ("SELECT * FROM '/etc/passwd'", "catalog_blocklist:table_name_path"),
        ("SELECT * FROM 'data.parquet'", "catalog_blocklist:table_name_path"),
        # Finding 011: Ensure query_table() table function is blocked.
        ("SELECT * FROM query_table('my_table')", "function_denylist:query_table"),
    ],
)
def test_batch_b_hardening_rejections(con, sql, reason):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == reason


# ── service_id_from_source tests (merged from test_service_ids.py) ──────────


@pytest.mark.parametrize(
    ("src", "override", "prefer_name", "expected"),
    [
        ({"name": "name", "service_id": "id"}, "override", False, "override"),
        ({"name": "name", "service_id": "id"}, None, False, "id"),
        ({"name": "name", "service_id": "id"}, None, True, "name"),
        ({"name": "name"}, None, False, "name"),
        ({}, None, False, ""),
    ],
)
def test_service_id_from_source(src, override, prefer_name, expected):
    assert service_id_from_source(src, override, prefer_name=prefer_name) == expected


# ── Merged from tests/utils/test_query_retries.py ───────────────────────────
#
# From the former self-heal test module:
# Tests for the storage.execute_with_stale_view_retry helper.
# The helper exists so background-job code paths (rdns_cache discovery,
# rollups DESCRIBE) — which open raw DuckDB connections instead of going
# through QueryRunner — can recover from the same buffer-deletion race
# that QueryRunner.execute already handles. Production incident
# 2026-06-10 (~8 hours of 100%-failing rdns discovery runs spamming
# the log) is the regression these tests pin against.


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

    @pytest.mark.parametrize("msg", ["Syntax error at line 1", "Permission denied", "Connection refused"])
    def test_returns_false_for_unrelated_errors(self, msg: str) -> None:
        assert is_stale_view_error(Exception(msg)) is False

    def test_recognises_real_duckdb_exception_from_detached_ducklake_catalog(self) -> None:
        """Not just a string fixture — a REAL DuckDB error from the exact
        DuckLake DETACH-then-query race (commit_batch/optimize briefly
        detach 'lake' on a connection other readers share), confirming the
        matcher isn't accidentally coupled to a specific error phrasing
        that a future DuckDB version could change."""
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS lake")
        con.execute("CREATE TABLE lake.logs (x INT)")
        con.execute("DETACH lake")
        try:
            con.execute("SELECT * FROM lake.logs")
        except Exception as e:
            assert is_stale_view_error(e) is True
        else:
            pytest.fail("expected querying a detached catalog to raise")


@contextmanager
def _patch_storage_caches():
    with (
        patch("backend.core.storage._core.clear_source_caches") as mock_clear,
        patch("backend.core.storage._core.update_storage_view") as mock_update,
    ):
        yield mock_clear, mock_update


class TestExecuteWithStaleViewRetry:
    def test_passthrough_when_fn_succeeds(self) -> None:
        """No retry / no cache bust when the first call succeeds."""
        con, fn = MagicMock(), MagicMock(return_value="ok")
        with _patch_storage_caches() as (mock_clear, mock_update):
            assert execute_with_stale_view_retry(con, {"name": "svc-a"}, fn) == "ok"
        fn.assert_called_once_with(con)
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_retries_once_after_stale_view_error(self) -> None:
        """First call raises a stale-view error → bust caches + force rebind + retry."""
        con, src = MagicMock(), {"name": "svc-b"}
        fn = MagicMock(
            side_effect=[
                Exception('IO Error: No files found that match the pattern "buffer/batch_x.parquet"'),
                ["row1", "row2"],
            ]
        )
        with _patch_storage_caches() as (mock_clear, mock_update):
            assert execute_with_stale_view_retry(con, src, fn) == ["row1", "row2"]
        assert fn.call_count == 2 and all(call.args[0] is con for call in fn.call_args_list)
        mock_clear.assert_called_once_with("svc-b", keep_snapshot_cache=True)
        mock_update.assert_called_once_with(con, src, force=True, target_table="logs")

    def test_non_stale_error_propagates_without_retry(self) -> None:
        """A non-stale error must surface immediately — don't waste a rebind on a real failure."""
        con, fn = MagicMock(), MagicMock(side_effect=ValueError("bad SQL"))
        with _patch_storage_caches() as (mock_clear, mock_update), pytest.raises(ValueError, match="bad SQL"):
            execute_with_stale_view_retry(con, {"name": "svc-c"}, fn)
        assert fn.call_count == 1
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_second_attempt_failure_propagates(self) -> None:
        """If the retry itself fails, propagate — caller chooses fallback (log + skip vs raise)."""
        con, fn = MagicMock(), MagicMock(side_effect=[Exception("No files found"), RuntimeError("still broken")])
        with _patch_storage_caches(), pytest.raises(RuntimeError, match="still broken"):
            execute_with_stale_view_retry(con, {"name": "svc-d"}, fn)
        assert fn.call_count == 2

    def test_passes_through_extra_args_and_kwargs(self) -> None:
        """The helper must forward *args and **kwargs to ``fn``."""
        con, fn = MagicMock(), MagicMock(return_value=42)
        with _patch_storage_caches():
            assert execute_with_stale_view_retry(con, {"name": "svc-e"}, fn, "a", 1, key="value") == 42
        fn.assert_called_once_with(con, "a", 1, key="value")

    def test_default_source_key_when_name_missing(self) -> None:
        """``src`` without a ``name`` key falls back to ``"default"`` for the cache-bust call."""
        con, fn = MagicMock(), MagicMock(side_effect=[Exception("No files found"), "ok"])
        with _patch_storage_caches() as (mock_clear, _):
            assert execute_with_stale_view_retry(con, {}, fn) == "ok"
        mock_clear.assert_called_once_with("default", keep_snapshot_cache=True)


class TestConfigureDuckdbS3:
    def test_configure_duckdb_s3_loads_extensions(self) -> None:
        con = MagicMock()
        configure_duckdb_s3(con)
        assert any("LOAD httpfs; LOAD parquet;" in c.args[0] for c in con.execute.call_args_list)

    def test_configure_duckdb_s3_installs_httpfs_on_load_failure(self) -> None:
        con, seen = MagicMock(), []

        def side_effect(sql, *args):
            seen.append(sql)
            if sql == "LOAD httpfs; LOAD parquet;" and seen.count("LOAD httpfs; LOAD parquet;") == 1:
                raise RuntimeError("Extension not installed")

        con.execute.side_effect = side_effect
        configure_duckdb_s3(con)
        assert "INSTALL httpfs; INSTALL parquet;" in seen

    def test_configure_duckdb_s3_reraises_when_install_also_fails(self) -> None:
        con = MagicMock()
        con.execute.side_effect = lambda sql, *args: (
            (_ for _ in ()).throw(RuntimeError("offline")) if "httpfs" in sql else None
        )
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

    assert retry_call(fn, retry_on=_is_transient, attempts=5, sleep=sleeps.append) == "ok"
    assert calls["n"] == 1 and sleeps == []


@pytest.mark.parametrize(
    ("attempts", "multiplier", "max_delay", "expected_sleeps"),
    [
        (4, 2.0, None, [1.0, 2.0, 4.0]),  # 4 attempts -> 3 sleeps between them
        (5, 10.0, 5.0, [1.0, 5.0, 5.0, 5.0]),  # max_delay clamps backoff
        (3, 1.0, None, [1.0, 1.0]),  # constant backoff via multiplier=1.0
        (1, 2.0, None, []),  # attempts=1 calls once and propagates
    ],
)
def test_retryable_exhaustion_and_backoff_shapes(attempts, multiplier, max_delay, expected_sleeps) -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    kwargs = {"max_delay": max_delay} if max_delay is not None else {}
    with pytest.raises(_Transient):
        retry_call(
            fn,
            retry_on=_is_transient,
            attempts=attempts,
            base_delay=1.0,
            multiplier=multiplier,
            sleep=sleeps.append,
            **kwargs,
        )
    assert calls["n"] == attempts
    assert sleeps == expected_sleeps


def test_fatal_not_retried() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(fn, retry_on=_is_transient, attempts=5, base_delay=1.0, sleep=sleeps.append)
    assert calls["n"] == 1 and sleeps == []


def test_eventual_success_stops_retrying() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient("retry me")
        return "done"

    res = retry_call(fn, retry_on=_is_transient, attempts=5, base_delay=1.0, multiplier=2.0, sleep=sleeps.append)
    assert res == "done" and calls["n"] == 3 and sleeps == [1.0, 2.0]


def test_before_retry_fires_between_attempts_with_exception() -> None:
    sleeps: list[float] = []
    seen: list[BaseException] = []
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient(f"attempt-{calls['n']}")

    with pytest.raises(_Transient):
        retry_call(
            fn, retry_on=_is_transient, attempts=3, base_delay=1.0, before_retry=seen.append, sleep=sleeps.append
        )
    # Hook runs once per retry (not after the final give-up attempt).
    assert [str(e) for e in seen] == ["attempt-1", "attempt-2"]
    assert all(isinstance(e, _Transient) for e in seen)


def test_before_retry_not_called_on_fatal() -> None:
    seen: list[BaseException] = []

    def fn() -> None:
        raise _Fatal("nope")

    with pytest.raises(_Fatal):
        retry_call(fn, retry_on=_is_transient, attempts=5, before_retry=seen.append, sleep=lambda _d: None)
    assert seen == []


def test_max_elapsed_budget_stops_early() -> None:
    calls = {"n": 0}

    def fn() -> None:
        calls["n"] += 1
        raise _Transient("boom")

    # tenacity's stop_after_delay reads real monotonic time, so a real (tiny)
    # sleep is required for the wall-clock budget to advance.
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
    assert calls["n"] < 1000


def test_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        retry_call(lambda: None, retry_on=_is_transient, attempts=0)


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
    monkeypatch.setattr(config_mod, "get_fastly_api_key", lambda service_id: "k")
    monkeypatch.setattr(config_mod, "get_fastly_service_id", lambda service_id: "fsid")


def _fetch(session, cursor=0):
    return poller_mod.RealtimePoller()._fetch_realtime(session, "svc", cursor)


def test_poller_retries_503_with_1s_2s_backoff_then_succeeds(_poller_creds):
    session = _ScriptedSession([_Resp(503), _Resp(503), _Resp(200, {"ok": 1})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session) == {"ok": 1}
    assert session.calls == 3 and delays == [1, 2]


def test_poller_exhausted_retryable_status_raises_http_error_without_final_sleep(_poller_creds):
    session = _ScriptedSession([_Resp(429), _Resp(502), _Resp(500)])
    with (
        _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays,
        pytest.raises(httpx.HTTPStatusError) as ei,
    ):
        _fetch(session)
    assert ei.value.response.status_code == 500
    assert session.calls == 3 and delays == [1, 2]


def test_poller_non_longpoll_read_timeout_is_retried(_poller_creds):
    session = _ScriptedSession([httpx.ReadTimeout("timed out"), _Resp(200, {"ok": 2})])
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays:
        assert _fetch(session, cursor=0) == {"ok": 2}
    assert delays == [1]


@pytest.mark.parametrize(
    ("script", "cursor", "exc_type", "expected_calls", "expected_delays"),
    [
        ([_Resp(404)], 0, httpx.HTTPStatusError, 1, []),
        ([httpx.ConnectError("a"), httpx.ConnectError("b"), httpx.ConnectError("c")], 0, httpx.ConnectError, 3, [1, 2]),
        ([httpx.ReadTimeout("timed out")], 123, poller_mod._LongPollTimeout, 1, []),
        ([ValueError("boom")], 0, ValueError, 1, []),
    ],
)
def test_poller_error_and_short_circuit_cases(_poller_creds, script, cursor, exc_type, expected_calls, expected_delays):
    session = _ScriptedSession(script)
    with _captured_sleeps("backend.core.realtime.poller._retry_sleep") as delays, pytest.raises(exc_type):
        _fetch(session, cursor=cursor)
    assert session.calls == expected_calls and delays == expected_delays


def _configure(con) -> None:
    with (
        patch("backend.core.duckdb._load_httpfs"),
        patch("backend.utils.telemetry_proxy.start_proxy_server"),
        patch("backend.utils.telemetry_proxy.proxy_endpoint", return_value="http://127.0.0.1:1"),
    ):
        core_duckdb._configure_fos(con, {"name": "svc-retry", "access_key_id": "a", "secret_access_key": "b"})


def _con_failing_secret(errors: list[BaseException]) -> MagicMock:
    con, pending = MagicMock(), list(errors)

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
    assert _secret_calls(con) == 3 and con.rollback.call_count == 3 and delays == []


def test_fos_secret_third_conflict_is_raised():
    con = _con_failing_secret([Exception("write-write conflict")] * 3)
    with _captured_sleeps(), pytest.raises(Exception, match="write-write conflict"):
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


@pytest.mark.parametrize(
    ("effects", "refresh_err", "expected_out", "expected_refreshes", "expected_calls"),
    [
        (["cur"], None, "cur", 0, 1),
        ([_stale(), "cur"], None, "cur", 1, 2),
        # QueryRunner.execute self-heals once internally; outer retry fires when inner retry also hits stale-view.
        ([_stale(), _stale(), "cur"], None, "cur", 2, 3),
        ([_stale(), _stale(), RuntimeError("late")], None, None, 2, 3),
        ([_stale()], RuntimeError("refresh"), None, 2, 1),
    ],
)
def test_execute_with_retry_cases(effects, refresh_err, expected_out, expected_refreshes, expected_calls):
    runner = _runner(effects)
    with patch("backend.core.storage.update_storage_view", side_effect=refresh_err) as refresh:
        assert runner.execute_with_retry("SELECT 1") == expected_out
    assert refresh.call_count == expected_refreshes
    assert runner.con.execute.call_count == expected_calls


def test_execute_with_retry_non_stale_raises_without_refresh():
    runner = _runner([RuntimeError("syntax")])
    with patch("backend.core.storage.update_storage_view") as refresh, pytest.raises(RuntimeError, match="syntax"):
        runner.execute_with_retry("SELECT 1")
    refresh.assert_not_called()


@pytest.mark.parametrize(
    ("effects", "refresh_err", "expected_out", "expected_refreshes"),
    [
        ([_stale(), "cur"], None, True, 1),
        ([_stale(), _stale(), "cur"], None, True, 2),
        ([_stale(), _stale(), RuntimeError("late")], None, False, 2),
        ([_stale()], RuntimeError("refresh"), False, 2),
    ],
)
def test_create_temp_table_cases(effects, refresh_err, expected_out, expected_refreshes):
    runner = _runner(effects)
    with patch("backend.core.storage.update_storage_view", side_effect=refresh_err) as refresh:
        assert runner.create_temp_table("CREATE TEMP TABLE t AS SELECT 1") is expected_out
    assert refresh.call_count == expected_refreshes


# ── Merged from test_http_make_client.py ──────────────────────────────────────────────


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

    with make_client(transport=httpx.MockTransport(handler)) as c, pytest.raises(httpx.HTTPStatusError):
        poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 0)
    assert calls == 1


def test_poller_longpoll_read_timeout_raises_gap_signal(rt_creds):
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    with make_client(transport=httpx.MockTransport(handler)) as c, pytest.raises(poller_mod._LongPollTimeout):
        poller_mod.RealtimePoller()._fetch_realtime(c, "svc", 123)
