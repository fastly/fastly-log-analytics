"""Tests for the local telemetry proxy (backend/utils/telemetry_proxy.py).

Design spec: docs/superpowers/specs/2026-05-19-telemetry-proxy-design.md
Implementation plan: docs/superpowers/plans/2026-05-19-telemetry-proxy-phase1.md
"""

from __future__ import annotations

import asyncio
import tracemalloc
from unittest.mock import AsyncMock, patch

import aiohttp
import boto3
import pytest
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config
from moto.server import ThreadedMotoServer

from backend.utils import telemetry_proxy


def _mock_upstream(status=200, headers=None, chunks=(b"",)):
    """Build a mock aiohttp response context manager that yields `chunks`.

    aiohttp's `session.request(...)` returns a context manager synchronously
    (not a coroutine). The returned object's `__aenter__` is awaited to get
    the actual response, so we build an AsyncMock context manager wrapping
    a response mock with the correct attribute shape.
    """

    async def _gen():
        for c in chunks:
            yield c

    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.headers = headers or {}
    mock_resp.content.iter_chunked = lambda _size: _gen()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_resp
    mock_ctx.__aexit__.return_value = None
    return mock_ctx, mock_resp


@pytest.fixture
async def proxy_server():
    """Start the proxy in a clean state, poll /healthz until ready, yield it,
    then shut down and reset module globals.

    Using a fixture (not bare start/stop calls in each test) eliminates two
    flakiness sources: (1) the asyncio.sleep(0.1) "wait for spin-up" race
    on slow CI runners, and (2) module-global leakage when a test fails
    before its stop_proxy_server() call.
    """
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


async def test_proxy_healthz_returns_200_on_startup(proxy_server):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{proxy_server.proxy_endpoint()}/healthz") as resp:
            assert resp.status == 200
            assert (await resp.text()) == "OK"


async def test_proxy_refuses_request_without_x_fos_target_header(proxy_server):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{proxy_server.proxy_endpoint()}/some/path") as resp:
            assert resp.status == 400
            assert "Missing X-Fos-Target header" in (await resp.text())


async def test_proxy_streams_response_chunks_through_to_client(proxy_server):
    """Bytes pass through unaltered and the upstream URL is reconstructed
    from X-Fos-Target + the incoming path/query."""
    ctx, _ = _mock_upstream(chunks=(b"chunk1", b"chunk2"))
    captured = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        return ctx

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_capture):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/test/stream?key=val"
            async with s.get(url, headers={"X-Fos-Target": "fake.cdn.net"}) as resp:
                assert resp.status == 200
                assert (await resp.read()) == b"chunk1chunk2"

    assert captured["method"] == "GET"
    # url is wrapped in yarl.URL(..., encoded=True) by the proxy so the wire
    # form matches what botocore signed; compare via str() to ignore type.
    assert str(captured["url"]) == "https://fake.cdn.net/test/stream?key=val"


async def test_proxy_handles_upstream_5xx_returns_5xx_to_client(proxy_server):
    ctx, _ = _mock_upstream(status=503)
    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/error"
            async with s.get(url, headers={"X-Fos-Target": "fake.cdn.net"}) as resp:
                assert resp.status == 503


async def test_proxy_strips_hop_by_hop_headers_before_forwarding(proxy_server):
    """RFC 7230 hop-by-hop headers (Connection, TE, etc.) and our internal
    telemetry headers must not be forwarded upstream. Forwarding them can
    confuse the upstream's connection handling or leak internal routing
    metadata."""
    ctx, _ = _mock_upstream(chunks=(b"ok",))
    sent_headers: dict[str, str] = {}

    def _capture(*args, **kwargs):
        sent_headers.update(kwargs.get("headers", {}))
        return ctx

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_capture):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/x"
            async with s.get(
                url,
                headers={
                    "X-Fos-Target": "fake.cdn.net",
                    "X-Telemetry-Caller": "duckdb.httpfs",
                    "Connection": "close",
                    "TE": "trailers",
                    "User-Agent": "myclient/1.0",
                },
            ) as _resp:
                await _resp.read()

    assert "Host" not in sent_headers
    assert "Connection" not in sent_headers
    assert "TE" not in sent_headers
    assert "X-Fos-Target" not in sent_headers
    assert "X-Telemetry-Caller" not in sent_headers
    assert sent_headers.get("User-Agent") == "myclient/1.0"


async def test_proxy_injects_sigv4_authorization_header_for_fos_request(proxy_server):
    """The proxy reads X-Telemetry-Service-Id, loads FOS creds from config,
    and signs the OUTBOUND request with SigV4. Clients hit the proxy
    UNSIGNED — the proxy is the only signer."""
    ctx, _ = _mock_upstream()
    mock_cfg = {
        "fos_access_key_id": "AKIATESTKEY",
        "fos_secret_access_key": "test-secret",
        "fos_region": "us-east-1",
    }
    captured = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        return ctx

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch.object(telemetry_proxy._SESSION, "request", side_effect=_capture):
            async with aiohttp.ClientSession() as s:
                url = f"{proxy_server.proxy_endpoint()}/test.txt"
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                    },
                ) as resp:
                    await resp.read()

    sent = captured["headers"]
    assert "Authorization" in sent
    assert "Credential=AKIATESTKEY" in sent["Authorization"]
    assert "SignedHeaders=" in sent["Authorization"]
    assert "Signature=" in sent["Authorization"]
    assert "X-Amz-Date" in sent
    assert "X-Amz-Content-SHA256" in sent


async def test_proxy_does_not_sign_cdn_routed_requests(proxy_server):
    """Requests routed to the service's cdn_url use ?key=... auth, NOT
    SigV4 — re-signing them would corrupt them."""
    ctx, _ = _mock_upstream()
    mock_cfg = {
        "fos_access_key_id": "AKIATESTKEY",
        "fos_secret_access_key": "test-secret",
        "fos_region": "us-east-1",
        "cdn_url": "https://cdn.example.net",
    }
    captured = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        return ctx

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch.object(telemetry_proxy._SESSION, "request", side_effect=_capture):
            async with aiohttp.ClientSession() as s:
                url = f"{proxy_server.proxy_endpoint()}/file.log.gz?key=abc"
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "cdn.example.net",
                        "X-Telemetry-Service-Id": "test-svc",
                    },
                ) as resp:
                    await resp.read()

    assert "Authorization" not in captured["headers"]


async def test_proxy_preserves_url_encoding_on_outbound_wire_so_sigv4_matches(proxy_server):
    """Regression: Iceberg commit PUTs to data partitions whose object keys
    contain ``=`` (e.g. ``timestamp_hour=2026-05-19-23``) arrive at the proxy
    URL-encoded as ``timestamp_hour%3D2026-05-19-23``.

    botocore's ``S3SigV4Auth`` derives the canonical URI verbatim from
    ``urlsplit(url).path`` — so when ``upstream_url`` contains ``%3D``, it
    signs the request *as if* the path were ``%3D``. But aiohttp's
    ``ClientSession`` defaults to ``URL(str, encoded=False)``, which DECODES
    ``%3D`` back to ``=`` on the outbound wire. R2 then verifies the
    signature against ``=`` (what arrived on the wire) and returns
    ``HTTP 403 'The calculated signature does not match what was provided'``.

    The proxy must put the same form on the wire that it signed. We pin
    this by running a fake HTTP upstream and asserting that the path it
    receives on the wire still contains ``%3D``.
    """
    import socket as _socket

    import yarl
    from aiohttp import web as _web

    received_on_upstream_wire: list[str] = []

    async def _echo(request):
        received_on_upstream_wire.append(request.path_qs)
        return _web.Response(text="ok")

    upstream_app = _web.Application()
    upstream_app.router.add_route("PUT", "/{tail:.*}", _echo)
    upstream_runner = _web.AppRunner(upstream_app)
    await upstream_runner.setup()
    _s = _socket.socket()
    _s.bind(("127.0.0.1", 0))
    upstream_port = _s.getsockname()[1]
    _s.close()
    await _web.TCPSite(upstream_runner, "127.0.0.1", upstream_port).start()

    try:
        mock_cfg = {
            "fos_access_key_id": "AKIATESTKEY",
            "fos_secret_access_key": "test-secret",
            "fos_region": "us-east-1",
        }
        telemetry_proxy._bust_config_cache()
        encoded_path = "/buc/data/timestamp_hour%3D2026-05-19-23/x.parquet"
        # encoded=True tells yarl/aiohttp to send the URL on the wire
        # verbatim, mimicking what aiobotocore (s3fs) does in production.
        client_url = yarl.URL(f"{proxy_server.proxy_endpoint()}{encoded_path}", encoded=True)
        with patch("backend.config.load_config", return_value=mock_cfg):
            async with aiohttp.ClientSession() as s:
                async with s.put(
                    client_url,
                    data=b"payload",
                    headers={
                        "X-Fos-Target": f"http://127.0.0.1:{upstream_port}",
                        "X-Telemetry-Service-Id": "test-svc",
                    },
                ) as resp:
                    await resp.read()
    finally:
        await upstream_runner.cleanup()

    assert received_on_upstream_wire, "fake upstream never received the request"
    # The signature is computed from the URL the proxy *signed*, which
    # contains %3D. The wire to the upstream must therefore also carry
    # %3D — otherwise R2 canonicalizes against `=` and the signature
    # mismatches. This is the production 403 we are pinning.
    assert received_on_upstream_wire[0] == encoded_path, (
        "proxy decoded %3D -> = on the outbound wire, but botocore signed for %3D. "
        f"wire={received_on_upstream_wire[0]!r} expected={encoded_path!r}"
    )


@pytest.fixture
def moto_s3_server():
    """Run a real http moto S3 server on a free port and seed a bucket.

    Unlike the broader `s3_mock` fixture in tests/conftest.py (which uses
    moto's transport patch), this returns a host:port that aiohttp can
    actually connect to — required for the proxy's E2E SigV4 verification.
    """
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


async def test_boto3_unsigned_through_proxy_round_trips_against_moto(proxy_server, moto_s3_server):
    """End-to-end: boto3 UNSIGNED client → proxy (signs) → moto S3 (verifies sig).
    A buggy SigV4 implementation would still produce a header that "looks
    right" in shape but moto would reject with 403. This pins actual
    correctness, not just shape."""
    endpoint_url, host_port = moto_s3_server
    bucket = "test-bucket"
    key = "roundtrip/file.txt"
    body = b"hello from boto3 through the proxy"

    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }

    proxy_url = proxy_server.proxy_endpoint()
    target_header = f"http://{host_port}"

    def _inject_proxy_headers(request, **_kwargs):
        request.headers["X-Fos-Target"] = target_header
        request.headers["X-Telemetry-Service-Id"] = "test-svc"
        request.headers["X-Telemetry-Caller"] = "test-boto3"

    proxied_s3 = boto3.client(
        "s3",
        endpoint_url=proxy_url,
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    proxied_s3.meta.events.register("before-send.s3.*", _inject_proxy_headers)

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        proxied_s3.put_object(Bucket=bucket, Key=key, Body=body)
        got = proxied_s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert got == body
        keys = [o["Key"] for o in proxied_s3.list_objects_v2(Bucket=bucket).get("Contents", [])]
        assert key in keys


async def test_proxy_writes_one_usage_log_row_per_request(proxy_server):
    ctx, _ = _mock_upstream(
        chunks=(b"hello world",),
        headers={"Content-Length": "11", "X-Cache": "HIT"},
    )
    captured_rows = []
    captured_ctx = {"value": None}

    def _capture_calls(service_id, rows, process_context=None):
        captured_rows.extend(rows)
        captured_ctx["value"] = process_context

    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture_calls):
            async with aiohttp.ClientSession() as s:
                url = f"{proxy_server.proxy_endpoint()}/key.parquet"
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                        "X-Telemetry-Caller": "duckdb.httpfs",
                        "X-Telemetry-Context": "api:GET /api/dashboard/aggregates",
                    },
                ) as resp:
                    await resp.read()
            # Flush BEFORE exiting the patch context, otherwise the executor
            # may run after the patch is torn down and the capture stays empty.
            telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) == 1
    row = captured_rows[0]
    assert row["method"] == "GET"
    assert row["path"] == "/key.parquet"
    assert row["bytes"] == 11
    assert row["status"] == "OK"
    assert row["caller"] == "duckdb.httpfs"
    assert row["service"] == "FOS"
    # X-Cache MUST live inside `details` as the FIRST `· `-separated chunk —
    # see backend/core/metadata_db.py:1113 where shield-egress doubling
    # parses it. A separate `x_cache` key would be silently ignored AND the
    # shield-doubling math would under-report by 50%.
    assert row["details"].startswith("HIT"), row["details"]
    # process_context flows through the function arg, not the row dict.
    assert captured_ctx["value"] == "api:GET /api/dashboard/aggregates"


async def test_proxy_translates_fos_list_get_to_list_objects_v2(proxy_server):
    """boto3's list_objects_v2 lands at the proxy as a raw HTTP GET with
    ``?list-type=2&...``. log_usage_calls keys Class A vs Class B off the
    S3 op name (LIST_OBJECTS_V2 = A), so a bare ``GET`` in the row would
    misclassify every LIST as a Class B read. Bug observed in prod:
    ~10k LISTs/day inflating Class B by ~12%.
    """
    ctx, _ = _mock_upstream(
        chunks=(b"<ListBucketResult/>",),
        headers={"Content-Length": "19"},
    )
    captured_rows = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
            async with aiohttp.ClientSession() as s:
                url = (
                    f"{proxy_server.proxy_endpoint()}/bucket"
                    "?list-type=2&prefix=raw%2F&start-after=raw%2F2026-06-08%2F"
                )
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                        "X-Telemetry-Caller": "ingest_scan",
                    },
                ) as resp:
                    await resp.read()
            telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) == 1
    row = captured_rows[0]
    assert row["service"] == "FOS"
    assert row["method"] == "LIST_OBJECTS_V2", row["method"]
    # The raw query string is preserved in path for forensic queries.
    assert "list-type=2" in row["path"]


async def test_proxy_keeps_get_for_non_list_fos_reads(proxy_server):
    """Guardrail for the LIST translation: a plain object GET (no
    ``list-type=`` query) must stay ``GET`` so it lands in Class B."""
    ctx, _ = _mock_upstream(chunks=(b"x" * 32,), headers={"Content-Length": "32"})
    captured_rows = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{proxy_server.proxy_endpoint()}/bucket/key.parquet?versionId=abc",
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                        "X-Telemetry-Caller": "duckdb.httpfs",
                    },
                ) as resp:
                    await resp.read()
            telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) == 1
    assert captured_rows[0]["method"] == "GET", captured_rows[0]["method"]


async def test_proxy_encodes_xcache_chain_in_details_for_shield_doubling(proxy_server):
    """The downstream shield-egress doubling at metadata_db.py:1113 reads
    the first `· `-separated chunk of details and looks for `MISS, MISS`
    / `MISS, PASS`. Without correctly formatting details, CDN egress
    bytes under-report by 50% on every shield miss."""
    ctx, _ = _mock_upstream(
        chunks=(b"x" * 1024,),
        headers={"X-Cache": "MISS, MISS"},
    )
    captured_rows = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    mock_cfg = {
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_region": "us-east-1",
        "cdn_url": "https://cdn.example.net",
    }
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
            with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
                async with aiohttp.ClientSession() as s:
                    url = f"{proxy_server.proxy_endpoint()}/k.parquet"
                    async with s.get(
                        url,
                        headers={
                            "X-Fos-Target": "cdn.example.net",
                            "X-Telemetry-Service-Id": "test-svc",
                            "X-Telemetry-Caller": "duckdb.httpfs",
                        },
                    ) as resp:
                        await resp.read()
                telemetry_proxy._flush_log_writes_for_tests()

    # MISS, MISS chain produces 2 rows (CDN + synth FOS GET_OBJECT). The
    # executor runs them concurrently, so we can't rely on captured_rows[0]
    # being the CDN row — find it explicitly.
    cdn_rows = [r for r in captured_rows if r["service"] == "CDN"]
    assert len(cdn_rows) == 1, [r["service"] for r in captured_rows]
    first_chunk = cdn_rows[0]["details"].split(" · ")[0]
    assert first_chunk == "MISS, MISS", cdn_rows[0]["details"]


async def test_proxy_handles_50_concurrent_requests_without_head_of_line_blocking(proxy_server):
    """50 concurrent requests should complete in roughly the time of one
    request, not 50× one request. A serialized handler would push p99 to
    50× the p50; aiohttp's per-handler concurrency keeps it close to p50."""
    ctx, _ = _mock_upstream(chunks=(b"x" * 1024,))

    # Each concurrent in-flight handler enters the same context manager.
    # AsyncMock context managers are reusable; aiohttp re-iterates the
    # generator on each request, so make sure the mock yields fresh
    # bytes each time by patching `request` with a fresh ctx per call.
    def _per_call(*args, **kwargs):
        fresh, _ = _mock_upstream(chunks=(b"x" * 1024,))
        return fresh

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_per_call):
        url = f"{proxy_server.proxy_endpoint()}/k"
        headers = {"X-Fos-Target": "fake.cdn.net"}
        async with aiohttp.ClientSession() as s:

            async def _one():
                async with s.get(url, headers=headers) as r:
                    await r.read()
                    return r.status

            t0 = asyncio.get_event_loop().time()
            results = await asyncio.gather(*[_one() for _ in range(50)])
            elapsed = asyncio.get_event_loop().time() - t0

    assert all(s == 200 for s in results)
    # Sanity bound — guards against head-of-line blocking, NOT a perf budget.
    # Real value is < 300ms locally; observed 2.15s on a slow CI runner. A
    # genuinely serialized handler would push this to ~25s+ (50× single-
    # request latency), so 5s is still a comfortable canary while
    # absorbing CI variance.
    assert elapsed < 5.0, f"50 concurrent requests took {elapsed:.2f}s — likely serialized"


async def test_proxy_logs_warning_when_dashboard_context_hits_fos(proxy_server, caplog):
    """Cost-guardrail policy: if X-Telemetry-Context starts with 'api:'
    AND the request is GET/HEAD AND the target is FOS (not CDN), emit a
    WARNING with the request path. Do NOT block — make the cost-regression
    class visible (see the iceberg_scan vs read_parquet incident)."""
    import logging

    ctx, _ = _mock_upstream()
    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/k.parquet"
            with caplog.at_level(logging.WARNING, logger="backend.utils.telemetry_proxy"):
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                        "X-Telemetry-Context": "api:POST /api/dashboard/aggregates",
                    },
                ) as resp:
                    await resp.read()
                # Wait for the proxy handler's finally block to finish so
                # the warning is captured before caplog stops listening.
                telemetry_proxy._flush_log_writes_for_tests()

    assert any("dashboard context hitting FOS" in r.message for r in caplog.records), (
        f"expected guardrail warning, got: {[r.message for r in caplog.records]}"
    )


async def test_proxy_per_request_context_headers_land_in_usage_log(proxy_server):
    """Pin the contextual-logging contract: X-Telemetry-Context becomes
    `process_context` on the log row and X-Telemetry-Caller becomes
    `caller`. The plumbing already works (Task 6 wired it) — this test
    exists so the contract can't regress silently."""
    ctx, _ = _mock_upstream()
    captured = {"rows": [], "process_context": None}

    def _capture(service_id, rows, process_context=None):
        captured["rows"].extend(rows)
        captured["process_context"] = process_context

    with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
            async with aiohttp.ClientSession() as s:
                url = f"{proxy_server.proxy_endpoint()}/k"
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "bucket.s3.amazonaws.com",
                        "X-Telemetry-Service-Id": "test-svc",
                        "X-Telemetry-Caller": "duckdb.httpfs",
                        "X-Telemetry-Context": "cron:sync:test-svc",
                    },
                ) as resp:
                    await resp.read()
            telemetry_proxy._flush_log_writes_for_tests()

    assert captured["process_context"] == "cron:sync:test-svc"
    assert captured["rows"][0]["caller"] == "duckdb.httpfs"


async def test_proxy_synthesizes_fos_get_object_for_cdn_full_miss(proxy_server):
    """Fastly behavior on a cache MISS is to fetch the FULL body from FOS
    (regardless of whether the client sent HEAD or GET) so subsequent CDN
    reads hit cache. The real FOS-side op is therefore always GET_OBJECT,
    even for HEAD requests. Preserves the relabel from commit 54a8a95."""
    ctx, _ = _mock_upstream(
        chunks=(b"x" * 512,),
        headers={"X-Cache": "MISS, MISS"},
    )
    captured_rows = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    mock_cfg = {
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_region": "us-east-1",
        "cdn_url": "https://cdn.example.net",
    }
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
            with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
                async with aiohttp.ClientSession() as s:
                    url = f"{proxy_server.proxy_endpoint()}/k.log.gz"
                    # Client sent HEAD — but synth row should still be GET_OBJECT.
                    async with s.head(
                        url,
                        headers={
                            "X-Fos-Target": "cdn.example.net",
                            "X-Telemetry-Service-Id": "test-svc",
                            "X-Telemetry-Caller": "fastly.api",
                        },
                    ) as resp:
                        await resp.read()
                telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) == 2, [r for r in captured_rows]
    services = {r["service"] for r in captured_rows}
    assert services == {"CDN", "FOS"}, services
    synth_row = next(r for r in captured_rows if r["service"] == "FOS")
    assert synth_row["method"] == "GET_OBJECT", synth_row["method"]
    assert synth_row["caller"] == "cdn.miss"
    assert synth_row["path"] == "/k.log.gz"
    assert synth_row["bytes"] == 512


@pytest.mark.parametrize("x_cache", ["HIT", "HIT, HIT", "MISS, HIT"])
async def test_proxy_does_not_synthesize_when_cdn_chain_has_a_hit(proxy_server, x_cache):
    """Anywhere along the chain reaches cache → no FOS access happened.
    HIT, HIT (both POPs hit), MISS, HIT (edge missed but shield hit), and
    bare HIT all bypass FOS — no synth row should appear."""
    ctx, _ = _mock_upstream(chunks=(b"x" * 100,), headers={"X-Cache": x_cache})
    captured_rows = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    mock_cfg = {
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_region": "us-east-1",
        "cdn_url": "https://cdn.example.net",
    }
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
            with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
                async with aiohttp.ClientSession() as s:
                    url = f"{proxy_server.proxy_endpoint()}/k.parquet"
                    async with s.get(
                        url,
                        headers={
                            "X-Fos-Target": "cdn.example.net",
                            "X-Telemetry-Service-Id": "test-svc",
                            "X-Telemetry-Caller": "duckdb.httpfs",
                        },
                    ) as resp:
                        await resp.read()
                telemetry_proxy._flush_log_writes_for_tests()

    # Exactly one row — the CDN row. No synth FOS row.
    assert len(captured_rows) == 1, [r["service"] for r in captured_rows]
    assert captured_rows[0]["service"] == "CDN"


async def test_proxy_writes_actually_persist_to_metadata_db(proxy_server, tmp_path, monkeypatch):
    """End-to-end through the real metadata_db code path: prove the row
    schema is compatible with what log_usage_calls writes to SQLite.
    A future refactor that changes the expected keys would be caught here
    — the mocked-write tests above would still pass."""
    # Route writes to an isolated SQLite directory and force config that
    # makes this look like a CDN call (so shield-doubling fires).
    monkeypatch.setattr("backend.core.metadata_db._DATA_DIR", str(tmp_path))

    mock_cfg = {
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_region": "us-east-1",
        "cdn_url": "https://cdn.example.net",
    }

    ctx, _ = _mock_upstream(
        chunks=(b"x" * 100,),
        headers={"X-Cache": "MISS, MISS"},
    )
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.config.is_usage_logging_enabled", return_value=True):
            with patch.object(telemetry_proxy._SESSION, "request", return_value=ctx):
                async with aiohttp.ClientSession() as s:
                    url = f"{proxy_server.proxy_endpoint()}/k"
                    async with s.get(
                        url,
                        headers={
                            "X-Fos-Target": "cdn.example.net",
                            "X-Telemetry-Service-Id": "real-svc-task6",
                            "X-Telemetry-Caller": "duckdb.httpfs",
                        },
                    ) as resp:
                        await resp.read()
                # Wait for the real metadata_db.log_usage_calls (writing
                # through SQLite) to finish before we read back.
                telemetry_proxy._flush_log_writes_for_tests()

    # A MISS, MISS chain produces TWO rows (CDN + synth FOS GET_OBJECT
    # from Task 7). We assert on the CDN row specifically.
    from backend.core import metadata_db

    con = metadata_db.get_con("real-svc-task6")
    rows = con.execute(
        "SELECT operation_class, operation_type, url, bytes "
        "FROM usage_log WHERE operation_class = 'CDN' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchall()

    assert len(rows) == 1, f"expected the CDN row in usage_log, got {len(rows)} rows"
    op_class, op_type, url, bytes_recorded = rows[0]
    assert op_class == "CDN"
    assert op_type == "GET"
    assert url == "/k"
    # Shield-egress doubling: 100 bytes × 2 = 200 (chain was MISS, MISS).
    # Failing here means either details wasn't encoded with X-Cache as the
    # first `· `-chunk, or service != "CDN".
    assert bytes_recorded == 200, (
        f"shield-egress doubling failed — expected 200 bytes (100×2 for MISS, MISS chain), got {bytes_recorded}."
    )


# ── Streaming follow-up (Phase 1 follow-up plan) ─────────────────────────────


def _seed_moto_object(endpoint_url: str, bucket: str, key: str, body: bytes) -> None:
    seed = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed.put_object(Bucket=bucket, Key=key, Body=body)


def _build_proxied_s3(proxy_url: str, host_port: str, caller: str):
    """Build a boto3 client that talks to the proxy and tags requests with
    the proxy routing headers needed for telemetry capture."""

    def _inject(request, **_kwargs):
        request.headers["X-Fos-Target"] = f"http://{host_port}"
        request.headers["X-Telemetry-Service-Id"] = "test-svc"
        request.headers["X-Telemetry-Caller"] = caller

    client = boto3.client(
        "s3",
        endpoint_url=proxy_url,
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    client.meta.events.register("before-send.s3.*", _inject)
    return client


async def test_proxy_preserves_range_get_byte_ranges(proxy_server, moto_s3_server):
    """A Range: bytes=A-B request must return exactly the requested slice,
    not the whole body. Pins that hop-by-hop stripping doesn't accidentally
    strip Range (Range is a request header, not hop-by-hop)."""
    endpoint_url, host_port = moto_s3_server
    bucket = "test-bucket"
    key = "range/big.bin"
    body = bytes(range(256)) * 4096  # 1MB of repeating 0..255
    _seed_moto_object(endpoint_url, bucket, key, body)

    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-test")

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        resp = proxied.get_object(Bucket=bucket, Key=key, Range="bytes=100-1099")
        chunk = resp["Body"].read()

    assert len(chunk) == 1000
    assert chunk == body[100:1100]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206


async def test_proxy_records_each_range_get_as_separate_row(proxy_server, moto_s3_server):
    """Three Range GETs against the same object produce three telemetry rows
    (one per request) — matches FOS billing, where each Range GET is one
    Class B op even if it's the same key."""
    endpoint_url, host_port = moto_s3_server
    bucket = "test-bucket"
    key = "range/three.bin"
    body = bytes(range(256)) * 4096
    _seed_moto_object(endpoint_url, bucket, key, body)

    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-multi")

    captured_rows: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
            for start, end in [(0, 99), (100, 199), (200, 299)]:
                resp = proxied.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
                resp["Body"].read()
            telemetry_proxy._flush_log_writes_for_tests()

    get_rows = [r for r in captured_rows if r["method"] == "GET"]
    assert len(get_rows) == 3, [r["method"] for r in captured_rows]
    assert all(r["bytes"] == 100 for r in get_rows), [r["bytes"] for r in get_rows]


async def test_proxy_records_multipart_put_per_part(proxy_server, moto_s3_server):
    """A 3-part multipart upload should produce one telemetry row per part
    (UploadPart is a billable Class A op) plus rows for CreateMultipartUpload
    and CompleteMultipartUpload. Pins that the proxy doesn't accidentally
    coalesce multipart traffic into a single row."""
    endpoint_url, host_port = moto_s3_server
    bucket = "test-bucket"
    key = "multipart/big.bin"

    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "multipart-test")

    captured_rows: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    # S3 multipart minimum part size is 5MB (moto enforces). 3 parts × 5MB = 15MB.
    part_bytes = 5 * 1024 * 1024
    parts_data = [b"a" * part_bytes, b"b" * part_bytes, b"c" * part_bytes]

    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=mock_cfg):
        with patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture):
            init = proxied.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = init["UploadId"]
            parts = []
            for i, chunk in enumerate(parts_data, start=1):
                r = proxied.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=i,
                    Body=chunk,
                )
                parts.append({"ETag": r["ETag"], "PartNumber": i})
            proxied.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            telemetry_proxy._flush_log_writes_for_tests()

    # Filter to PUT rows (UploadPart uses PUT) — exactly 3, one per part.
    put_rows = [r for r in captured_rows if r["method"] == "PUT"]
    assert len(put_rows) == 3, (
        f"expected 3 PUT rows (one per UploadPart), got {len(put_rows)}: "
        f"{[(r['method'], r['path'][:80]) for r in captured_rows]}"
    )
    # Each PUT path includes partNumber=N — confirms part identity isn't lost.
    part_paths = sorted(r["path"] for r in put_rows)
    for n, p in zip([1, 2, 3], part_paths):
        assert f"partNumber={n}" in p, f"missing partNumber={n} in {p}"


async def test_proxy_streams_large_response_without_buffering(proxy_server):
    """A 50MB response body should stream through the proxy without
    accumulating in memory. We mock the upstream to yield 64KB chunks
    lazily (never materializing the full body), so the only Python
    allocations during transit are whatever the proxy itself holds in
    memory. A naive `await resp.read()` in the handler would push peak
    above 50MB; correct chunked streaming keeps it well below.

    Note: this test deliberately bypasses moto. moto's werkzeug server
    allocates the full response body in a single bytes object before
    sending — that 50MB allocation would dominate tracemalloc and mask
    whatever the proxy itself does. Mocking the upstream isolates the
    proxy's contribution."""
    body_size = 50 * 1024 * 1024
    chunk_size = 64 * 1024
    n_chunks = body_size // chunk_size

    # Lazy chunk generator: each .__anext__() yields a fresh 64KB bytes
    # object. The full 50MB never exists at once.
    async def _lazy_chunks():
        chunk = b"\x00" * chunk_size
        for _ in range(n_chunks):
            yield chunk

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {"Content-Length": str(body_size)}
    mock_resp.content.iter_chunked = lambda _size: _lazy_chunks()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_resp
    mock_ctx.__aexit__.return_value = None

    received_bytes = 0
    with patch.object(telemetry_proxy._SESSION, "request", return_value=mock_ctx):
        async with aiohttp.ClientSession() as session:
            tracemalloc.start()
            try:
                async with session.get(
                    f"{proxy_server.proxy_endpoint()}/big.bin",
                    headers={"X-Fos-Target": "fake.upstream.net"},
                ) as resp:
                    assert resp.status == 200
                    # Count bytes WITHOUT accumulating — accumulating in the
                    # test would be a 50MB allocation that masks the proxy's.
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        received_bytes += len(chunk)
                _current, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

    assert received_bytes == body_size, f"expected {body_size} bytes, got {received_bytes}"
    # Sanity bound: with no client-side accumulation and a lazy upstream,
    # peak Python heap should stay under a few MB even for a 50MB body.
    # A naive `await upstream_resp.read()` in the proxy handler would
    # spike peak above 50MB. We pick 10MB as a generous ceiling that
    # tolerates aiohttp/botocore internals but catches body buffering.
    peak_mb = peak / (1024 * 1024)
    body_mb = body_size / (1024 * 1024)
    assert peak_mb < 10, (
        f"peak Python heap during {body_mb:.0f}MB download was {peak_mb:.1f}MB — "
        "likely the proxy is buffering the response body instead of streaming"
    )


async def test_proxy_retries_idempotent_get_on_connection_error_and_succeeds(proxy_server):
    """Telemetry 2026-05-20: 605 HTTP 502s in 45 min (18% of pyiceberg.s3fs
    GETs), 97% of them immediately retried by boto3 and succeeded. Root
    cause: aiohttp's pool handed out a connection Fastly had already
    half-closed → "Cannot write to closing transport" → proxy returned 502
    → boto3 retried on a fresh connection → success. We absorb that race
    inside the proxy now so boto3 sees one clean 200 instead of 502+200,
    halving the doubled wire-cost and the latency hit (~570ms per blip)."""
    call_count = {"n": 0}
    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"hello",))

    def _flaky(*_args, **_kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # ServerDisconnectedError is the canonical wrapper for the
            # half-closed-pool race. Raising synchronously from .request()
            # (not from __aenter__) mirrors what aiohttp does when the
            # connector itself can't dial out.
            raise aiohttp.ServerDisconnectedError("simulated pool race")
        return ok_ctx

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_flaky):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/some/key.avro"
            async with s.get(url, headers={"X-Fos-Target": "fake.cdn.net"}) as resp:
                assert resp.status == 200, "internal retry must hide the connection blip from the client"
                assert (await resp.read()) == b"hello"

    assert call_count["n"] == 2, (
        f"expected 1 fail + 1 retry = 2 upstream attempts; got {call_count['n']}. "
        "If retry isn't firing, every Fastly keep-alive race re-doubles boto3's GET cost."
    )


async def test_proxy_does_not_retry_non_idempotent_method_on_connection_error(proxy_server):
    """Replaying a POST/PUT against the upstream could double-write or
    double-charge. The retry path MUST be restricted to GET/HEAD/OPTIONS;
    a connection error on a PUT surfaces as 502 so boto3's idempotency
    logic — which knows the request semantics — decides whether to retry."""
    call_count = {"n": 0}

    def _always_fails(*_args, **_kwargs):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("simulated pool race")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/some/key.avro"
            async with s.put(url, data=b"payload", headers={"X-Fos-Target": "fake.cdn.net"}) as resp:
                assert resp.status == 502, "PUT must surface 502, not silently retry"

    assert call_count["n"] == 1, (
        f"PUT must NOT be retried internally (got {call_count['n']} attempts). "
        "Retrying a non-idempotent request risks double-writes against the upstream."
    )


async def test_proxy_retries_at_most_once_on_repeated_connection_errors(proxy_server):
    """If the upstream is actually down (not a pool blip), retrying forever
    would burn the request's latency budget and mask the outage. One retry
    is enough to absorb a single half-closed-pool race; a second failure
    means something worse, and the 502 should be surfaced."""
    call_count = {"n": 0}

    def _always_fails(*_args, **_kwargs):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("upstream genuinely down")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/some/key.avro"
            async with s.get(url, headers={"X-Fos-Target": "fake.cdn.net"}) as resp:
                assert resp.status == 502

    assert call_count["n"] == 2, (
        f"expected exactly 2 attempts (1 try + 1 retry); got {call_count['n']}. "
        "More than 2 = unbounded retry loop; fewer = retry never fired."
    )


async def test_client_disconnect_mid_stream_is_not_logged_as_upstream_502(proxy_server):
    """Regression for the 2026-05-21 false-positive 502s. The proxy used to
    treat `ConnectionResetError` from `proxy_resp.write(chunk)` (the
    client-facing side) the same as an upstream failure — emitting a row
    tagged HTTP 502. But that exception means the *client* (aiobotocore)
    closed its socket mid-stream; the upstream GET to FOS already completed
    and FOS will bill us for it. Recording a 502 in that case both
    over-counts failures and under-counts billable upstream success.

    cron_compact telemetry on 2026-05-21 showed ~140 of these per tick, all
    spurious — boto3 was retrying its own connection and getting clean 200s
    on the second attempt. The pin: the telemetry row carries the upstream
    status (OK), not a synthesized 502."""
    captured: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        captured.extend(rows)

    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"chunk1", b"chunk2", b"chunk3"))

    # Patch the inbound write so the second chunk hits a "closing" client
    # transport, which is exactly what aiohttp/http_writer raises when
    # aiobotocore drops the connection partway through reading the body.
    write_count = {"n": 0}
    real_write = web.StreamResponse.write

    async def _flaky_write(self, data):
        write_count["n"] += 1
        if write_count["n"] >= 2:
            raise ConnectionResetError("Cannot write to closing transport")
        return await real_write(self, data)

    with (
        patch.object(telemetry_proxy._SESSION, "request", return_value=ok_ctx),
        patch.object(web.StreamResponse, "write", _flaky_write),
        patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture),
    ):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}/some/key.avro"
            # We don't care what the client sees here — the regression is
            # about what the proxy *records*, not what it returns to the
            # client (the client is mid-disconnect by simulation).
            try:
                async with s.get(
                    url,
                    headers={
                        "X-Fos-Target": "fake.cdn.net",
                        "X-Telemetry-Service-Id": "test-svc",
                    },
                ) as resp:
                    try:
                        await resp.read()
                    except aiohttp.ClientError:
                        pass
            except aiohttp.ClientError:
                pass

        telemetry_proxy._flush_log_writes_for_tests()

    # Filter to FOS/CDN-targeted rows (skip any incidental healthz noise).
    proxy_rows = [r for r in captured if r.get("service") in ("FOS", "CDN")]
    assert len(proxy_rows) >= 1, f"expected ≥1 proxy row, got {captured!r}"
    row = proxy_rows[0]
    assert row["status"] == "OK", (
        f"client mid-stream disconnect must NOT be recorded as a 502; got status={row['status']!r}. "
        "Logging a 502 here double-counts in the billing dashboard and hides the upstream "
        "GET that FOS actually served and billed."
    )


def test_proxy_connector_caps_keepalive_below_fastly_default():
    """The aiohttp default of 15s keepalive races Fastly's ~5-10s edge
    keep-alive — that race is what produced the 605 HTTP 502s on
    2026-05-20. The connector must cap idle reuse below any plausible
    upstream keep-alive so a stale pool entry can't outlive the server's
    willingness to accept on it. enable_cleanup_closed is harmless extra
    defense (no-op on Python 3.13.3+ where the underlying CPython bug is
    fixed, but still load-bearing on older Pythons in the field)."""
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    try:
        assert telemetry_proxy._SESSION is not None
        connector = telemetry_proxy._SESSION.connector
        assert connector is not None
        # keepalive_timeout must be strictly less than aiohttp's 15s default
        # AND less than Fastly's lower-bound 5s, so the pool can never offer
        # a connection past the server's keep-alive expiry.
        assert connector._keepalive_timeout < 5.0, (
            f"keepalive_timeout={connector._keepalive_timeout}s is >= 5s; "
            "Fastly edges close idle keep-alives within that window and the "
            "race comes back."
        )
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()
