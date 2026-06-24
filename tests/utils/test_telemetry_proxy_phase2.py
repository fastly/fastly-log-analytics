"""Phase 2 tests for the telemetry proxy — boto3 routed through the proxy.

Design spec: docs/superpowers/specs/2026-05-19-telemetry-proxy-design.md (§Phase 2)
Plan: docs/superpowers/plans/2026-05-19-telemetry-proxy-phase2.md

These tests live in their own file (not in test_telemetry_proxy.py) so the
Phase 2 fixtures don't pollute the Phase 1 file and so the dual-mode
comparison fixture stays isolated. Shared infrastructure with the Phase 1
file is intentionally duplicated — moving them to a conftest.py would
silently change Phase 1 fixture discovery, which we don't want.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import aiohttp
import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from moto.server import ThreadedMotoServer

from backend.utils import telemetry_proxy


@pytest.fixture
async def proxy_server():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    deadline = asyncio.get_event_loop().time() + 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{telemetry_proxy.proxy_endpoint()}/healthz") as r:
                    if r.status == 200:
                        break
        except (aiohttp.ClientError, RuntimeError):
            pass
        if asyncio.get_event_loop().time() > deadline:
            telemetry_proxy.stop_proxy_server()
            raise RuntimeError("proxy did not become healthy in 5s")
        await asyncio.sleep(0.02)
    try:
        yield telemetry_proxy
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.fixture
def moto_s3_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    seed_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed_client.create_bucket(Bucket="test-bucket")
    try:
        yield endpoint_url, f"{host}:{port}"
    finally:
        server.stop()


# ── Task 2: install_boto3_proxy_hook ─────────────────────────────────────────


async def test_install_boto3_proxy_hook_routes_request_through_proxy(proxy_server, moto_s3_server):
    """The hook is the single integration point between boto3 and the proxy.
    If it doesn't inject X-Fos-Target / X-Telemetry-Service-Id correctly,
    the proxy returns 400 (missing header) and no telemetry row is written.
    Pinning a successful round-trip + exactly one logged row proves the
    hook produces headers the proxy actually accepts."""
    endpoint_url, host_port = moto_s3_server
    source = {
        "name": "phase2-test",
        "service_id": "phase2-test",
        "fos_native_endpoint": f"http://{host_port}",
    }
    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }

    client = boto3.client(
        "s3",
        endpoint_url=proxy_server.proxy_endpoint(),
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    telemetry_proxy.install_boto3_proxy_hook(client, source)

    captured_rows: list[dict] = []
    captured_service: list[str] = []

    def _capture(service_id, rows, process_context=None):
        captured_service.append(service_id)
        captured_rows.extend(rows)

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.core.metadata.log_usage_calls", side_effect=_capture):
            resp = client.head_bucket(Bucket="test-bucket")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) == 1, f"expected 1 telemetry row, got {len(captured_rows)}"
    assert captured_service == ["phase2-test"]
    row = captured_rows[0]
    assert row["method"] == "HEAD"
    assert row["caller"] == "boto3.headbucket"


async def test_install_boto3_proxy_hook_passes_process_context_per_request(proxy_server, moto_s3_server):
    """The boto3 advantage called out in spec §6: per-call ContextVar reads,
    so context set between two requests on the same client is reflected in
    the second request's telemetry row but not the first. Pinning this
    proves we read the ContextVar at request build time, not at hook
    registration time."""
    from backend.utils import telemetry as tlm

    endpoint_url, host_port = moto_s3_server
    source = {
        "name": "phase2-ctx",
        "service_id": "phase2-ctx",
        "fos_native_endpoint": f"http://{host_port}",
    }
    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }
    client = boto3.client(
        "s3",
        endpoint_url=proxy_server.proxy_endpoint(),
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    telemetry_proxy.install_boto3_proxy_hook(client, source)

    contexts_seen: list[str | None] = []

    def _capture(service_id, rows, process_context=None):
        contexts_seen.append(process_context)

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.core.metadata.log_usage_calls", side_effect=_capture):
            tlm._set_process_context_for_tests("cron:sync:phase2-ctx-A")
            client.head_bucket(Bucket="test-bucket")
            tlm._set_process_context_for_tests("cron:compact:phase2-ctx-B")
            client.head_bucket(Bucket="test-bucket")
            telemetry_proxy._flush_log_writes_for_tests()
            tlm._set_process_context_for_tests(None)

    assert contexts_seen == [
        "cron:sync:phase2-ctx-A",
        "cron:compact:phase2-ctx-B",
    ], f"per-request context did not round-trip: {contexts_seen}"


async def test_install_boto3_proxy_hook_falls_back_to_thread_name_when_context_unset(proxy_server, moto_s3_server):
    """When no caller has ever called _set_process_context_for_tests (or it was reset),
    the boto3 hook must still emit *some* context — the thread name. Untagged
    rows in usage_log block cost attribution; on 2026-05-20 we discovered
    426K rows/day landing as NULL because the boto3/s3fs hooks skipped the
    header when the ContextVar was empty. This pin enforces the fallback."""
    from backend.utils import telemetry as tlm

    _endpoint_url, host_port = moto_s3_server
    source = {
        "name": "phase2-fallback",
        "service_id": "phase2-fallback",
        "fos_native_endpoint": f"http://{host_port}",
    }
    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }
    client = boto3.client(
        "s3",
        endpoint_url=proxy_server.proxy_endpoint(),
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    telemetry_proxy.install_boto3_proxy_hook(client, source)

    contexts_seen: list[str | None] = []

    def _capture(service_id, rows, process_context=None):
        contexts_seen.append(process_context)

    # Force-clear both the ContextVar and the process-global fallback so the
    # hook genuinely sees an empty context.
    tlm._PROCESS_CONTEXT.set(None)
    with tlm._LATEST_PROCESS_CONTEXT_LOCK:
        tlm._LATEST_PROCESS_CONTEXT = None

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.core.metadata.log_usage_calls", side_effect=_capture):
            client.head_bucket(Bucket="test-bucket")
            telemetry_proxy._flush_log_writes_for_tests()

    assert len(contexts_seen) == 1
    assert contexts_seen[0] is not None
    assert contexts_seen[0].startswith("untagged:"), f"expected fallback 'untagged:<thread>', got {contexts_seen[0]!r}"


# ── Stream J: route boto3 object downloads through CDN ──────────────────────


class _FakeBoto3Client:
    """Minimal stand-in for a boto3 client: captures the before-send handler
    registered by install_boto3_proxy_hook so each test can invoke it
    directly with a synthetic request + event_name. Avoids spinning up the
    proxy + moto for what is purely a header-injection unit test."""

    def __init__(self):
        self._handler = None
        self.meta = self  # so `client.meta.events.register(...)` resolves here

    @property
    def events(self):
        return self

    def register(self, _signal: str, handler):
        self._handler = handler

    def invoke(self, *, method: str, event_name: str):
        from types import SimpleNamespace

        req = SimpleNamespace(method=method, headers={})
        assert self._handler is not None, "install_boto3_proxy_hook never ran"
        self._handler(request=req, event_name=event_name)
        return req.headers


def _cdn_source():
    return {
        "name": "streamj",
        "service_id": "streamj",
        "fos_native_endpoint": "us-east.object.fastlystorage.app",
        "cdn_url": "https://fos-streamj-logs.global.ssl.fastly.net",
        "cdn_secret": "cdn-secret-abc",
    }


def test_install_boto3_proxy_hook_routes_get_object_to_cdn():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
    assert headers["X-Fos-Target"] == "fos-streamj-logs.global.ssl.fastly.net"
    assert headers["x-fastly-key"] == "cdn-secret-abc"


def test_install_boto3_proxy_hook_routes_head_object_to_cdn():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method="HEAD", event_name="before-send.s3.HeadObject")
    assert headers["X-Fos-Target"] == "fos-streamj-logs.global.ssl.fastly.net"
    assert headers["x-fastly-key"] == "cdn-secret-abc"


def test_install_boto3_proxy_hook_keeps_list_native():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method="GET", event_name="before-send.s3.ListObjectsV2")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app"
    assert "x-fastly-key" not in headers


def test_install_boto3_proxy_hook_keeps_put_native():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method="PUT", event_name="before-send.s3.PutObject")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app"
    assert "x-fastly-key" not in headers


def test_install_boto3_proxy_hook_keeps_delete_native():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method="POST", event_name="before-send.s3.DeleteObjects")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app"
    assert "x-fastly-key" not in headers


def test_install_boto3_proxy_hook_get_with_no_cdn_url_stays_native():
    source = {
        "name": "streamj-nocdn",
        "service_id": "streamj-nocdn",
        "fos_native_endpoint": "us-east.object.fastlystorage.app",
    }
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, source)
    headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app"
    assert "x-fastly-key" not in headers
