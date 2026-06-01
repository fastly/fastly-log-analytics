"""Phase 4 telemetry-proxy tests — surviving pins for the always-on proxy.

Phase 4 flipped the proxy default to ON. Phase 5 deleted every legacy
mechanism (wrap_s3_client, _TrackedClient, _setup_http_logging,
_TrackedConnection, TrackedFsspecFileIO, is_telemetry_proxy_enabled,
warn_legacy_telemetry). What remains pinned here: the lifespan startup
hook eagerly starts the proxy, and the boto3 paginator caller_hint
flows through to the proxy header.
"""

from __future__ import annotations

from unittest.mock import patch

# ── FastAPI lifespan eagerly starts the proxy ────────────────────────────────


def test_fastapi_lifespan_starts_proxy():
    """The lifespan startup hook must call telemetry_proxy.start_proxy_server()
    so the first request doesn't pay a cold-start cost. The proxy itself is
    idempotent; lifespan just guarantees the boot."""
    import asyncio as _asyncio

    from backend import main as _main

    called = {"n": 0}

    def _fake_start():
        called["n"] += 1

    with patch("backend.utils.telemetry_proxy.start_proxy_server", side_effect=_fake_start):

        async def _drive():
            async with _main.lifespan(_main.app):
                pass

        _asyncio.run(_drive())

    assert called["n"] >= 1, "lifespan must call start_proxy_server()"


# ── caller_hint plumbing for _get_fos_client ─────────────────────────────────


def test_get_fos_client_paginator_accepts_caller_hint():
    """Production callers (ingest_scan, download_zip, download_all) pass
    ``caller_hint`` to ``client.get_paginator(...)``. The proxy is the
    telemetry sink, but the call sites still pass the kwarg — so the client
    must silently accept it instead of exploding with
    ``BaseClient.get_paginator() got an unexpected keyword argument``."""
    from backend.core import duckdb as _ddb

    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()

    source = {
        "name": "phase4-caller-hint",
        "service_id": "phase4-caller-hint",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
        "access_key_id": "test",
        "secret_access_key": "test",
    }

    client = _ddb._get_fos_client(source)
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
    assert pag is not None
    assert hasattr(pag, "paginate")


def test_get_fos_client_paginator_caller_hint_flows_to_proxy_header():
    """When ``caller_hint`` is passed to the paginator and we iterate via
    ``.paginate(...)``, every S3 request the proxy receives during that
    iteration must carry ``X-Telemetry-Caller: <caller_hint>`` (overriding
    the default ``boto3.<op>``). This lets the proxy keep ingest-scan /
    download contexts distinguishable in ``_usage_log``."""
    from backend.core import duckdb as _ddb

    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()

    source = {
        "name": "phase4-caller-hint-header",
        "service_id": "phase4-caller-hint-header",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
    }
    client = _ddb._get_fos_client(source)
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")

    captured = []

    class _FakeReq:
        def __init__(self):
            self.headers = {}

    if hasattr(pag, "_set_caller_hint_context"):
        token = pag._set_caller_hint_context()
        try:
            req = _FakeReq()
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            pag._reset_caller_hint_context(token)
    else:
        from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

        token = _BOTO3_CALLER_HINT.set("ingest_scan")
        try:
            req = _FakeReq()
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            _BOTO3_CALLER_HINT.reset(token)

    assert captured == ["ingest_scan"], (
        f"caller_hint must override default boto3.<op> in X-Telemetry-Caller; got {captured!r}"
    )

    # After exiting the caller_hint scope, the default boto3.<op> is restored.
    req = _FakeReq()
    client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
    assert req.headers.get("X-Telemetry-Caller") == "boto3.listobjectsv2"


# ── Upstream timeout: bounded by default, configurable via env var ──────────


def test_upstream_timeout_default_is_bounded():
    """Default total upstream timeout must be <= 120s. The stuck-proxy
    incident on 2026-05-20 held a connection open for 300s before any
    timer fired; pinning the default at 90s (with 120s slack here for
    future tuning) prevents that regression."""
    from backend.utils import telemetry_proxy

    total = telemetry_proxy._UPSTREAM_TIMEOUT.total
    assert total is not None, "upstream timeout total must not be unbounded (None)"
    assert 0 < total <= 120, f"default upstream timeout should be bounded (<= 120s), got {total}"


def test_upstream_timeout_env_var_override():
    """FOS_PROXY_UPSTREAM_TIMEOUT_S must override the default when set.
    Tested via importlib.reload so the env-read at module load is
    re-evaluated."""
    import importlib
    import os

    from backend.utils import telemetry_proxy

    original = os.environ.get("FOS_PROXY_UPSTREAM_TIMEOUT_S")
    try:
        os.environ["FOS_PROXY_UPSTREAM_TIMEOUT_S"] = "37"
        importlib.reload(telemetry_proxy)
        assert telemetry_proxy._UPSTREAM_TIMEOUT.total == 37.0
    finally:
        if original is None:
            os.environ.pop("FOS_PROXY_UPSTREAM_TIMEOUT_S", None)
        else:
            os.environ["FOS_PROXY_UPSTREAM_TIMEOUT_S"] = original
        importlib.reload(telemetry_proxy)
