from __future__ import annotations

import asyncio
import importlib
import io
import logging
import os
import socket
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yarl
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config
from moto.server import ThreadedMotoServer

from backend.core import duckdb as _ddb
from backend.core import iceberg as _ic
from backend.utils import telemetry as tlm
from backend.utils import telemetry_proxy

MOCK_CDN_CFG = {
    "fos_access_key_id": "k",
    "fos_secret_access_key": "s",
    "fos_region": "us-east-1",
    "cdn_url": "https://cdn.example.net",
}
MOCK_FOS_CFG = {
    "fos_access_key_id": "testing",
    "fos_secret_access_key": "testing",
    "fos_region": "us-east-1",
}


@pytest.fixture
async def proxy_server():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    deadline = asyncio.get_event_loop().time() + 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s, s.get(f"{telemetry_proxy.proxy_endpoint()}/healthz") as r:
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


def _mock_upstream(status=200, headers=None, chunks=(b"",)):
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


async def _proxy_call(
    proxy_server,
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    status: int = 200,
    resp_headers: dict[str, str] | None = None,
    chunks: tuple[bytes, ...] = (b"",),
    cfg: dict | None = None,
):
    ctx, _ = _mock_upstream(status=status, headers=resp_headers, chunks=chunks)
    captured_req: dict = {}
    captured_rows: list[dict] = []
    captured_ctx: dict[str, str | None] = {"value": None}

    def _cap_req(*_a, **kw):
        captured_req.update(kw)
        return ctx

    def _cap_log(_sid, rows, process_context=None):
        captured_rows.extend(rows)
        captured_ctx["value"] = process_context

    if cfg is not None:
        telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=cfg) if cfg is not None else patch.dict(os.environ, {}),
        patch.object(telemetry_proxy._SESSION, "request", side_effect=_cap_req),
        patch("backend.core.metadata.log_usage_calls", side_effect=_cap_log),
    ):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}{path}"
            async with s.request(method, url, headers=headers or {}) as resp:
                body = await resp.read()
        telemetry_proxy._flush_log_writes_for_tests()
    return resp.status, body, captured_req, captured_rows, captured_ctx["value"]


async def test_proxy_healthz_returns_200_on_startup(proxy_server):
    async with aiohttp.ClientSession() as s, s.get(f"{proxy_server.proxy_endpoint()}/healthz") as resp:
        assert resp.status == 200 and (await resp.text()) == "OK"


async def test_proxy_refuses_request_without_x_fos_target_header(proxy_server):
    async with aiohttp.ClientSession() as s, s.get(f"{proxy_server.proxy_endpoint()}/some/path") as resp:
        assert resp.status == 400 and "Missing X-Fos-Target header" in (await resp.text())


async def test_proxy_streams_response_chunks_through_to_client(proxy_server):
    st, body, req, _, _ = await _proxy_call(
        proxy_server, "/test/stream?key=val", headers={"X-Fos-Target": "fake.cdn.net"}, chunks=(b"chunk1", b"chunk2")
    )
    assert st == 200 and body == b"chunk1chunk2"
    assert req["method"] == "GET" and str(req["url"]) == "https://fake.cdn.net/test/stream?key=val"


async def test_proxy_handles_upstream_5xx_returns_5xx_to_client(proxy_server):
    st, _, _, _, _ = await _proxy_call(proxy_server, "/error", headers={"X-Fos-Target": "fake.cdn.net"}, status=503)
    assert st == 503


async def test_proxy_strips_hop_by_hop_headers_before_forwarding(proxy_server):
    _, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/x",
        headers={
            "X-Fos-Target": "fake.cdn.net",
            "X-Telemetry-Caller": "duckdb.httpfs",
            "Connection": "close",
            "TE": "trailers",
            "User-Agent": "myclient/1.0",
        },
        chunks=(b"ok",),
    )
    sent = req["headers"]
    for h in ("Host", "Connection", "TE", "X-Fos-Target", "X-Telemetry-Caller"):
        assert h not in sent
    assert sent.get("User-Agent") == "myclient/1.0"


async def test_proxy_does_not_sign_cdn_routed_requests(proxy_server):
    _, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/file.log.gz?key=abc",
        headers={"X-Fos-Target": "cdn.example.net", "X-Telemetry-Service-Id": "test-svc"},
        cfg=MOCK_CDN_CFG,
    )
    assert "Authorization" not in req["headers"]


async def test_proxy_preserves_url_encoding_on_outbound_wire_so_sigv4_matches(proxy_server):
    received_on_upstream_wire: list[str] = []

    async def _echo(request):
        received_on_upstream_wire.append(request.path_qs)
        return web.Response(text="ok")

    upstream_app = web.Application()
    upstream_app.router.add_route("PUT", "/{tail:.*}", _echo)
    upstream_runner = web.AppRunner(upstream_app)
    await upstream_runner.setup()
    s_sock = socket.socket()
    s_sock.bind(("127.0.0.1", 0))
    upstream_port = s_sock.getsockname()[1]
    s_sock.close()
    await web.TCPSite(upstream_runner, "127.0.0.1", upstream_port).start()

    try:
        telemetry_proxy._bust_config_cache()
        encoded_path = "/buc/data/timestamp_hour%3D2026-05-19-23/x.parquet"
        client_url = yarl.URL(f"{proxy_server.proxy_endpoint()}{encoded_path}", encoded=True)
        with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
            async with (
                aiohttp.ClientSession() as s,
                s.put(
                    client_url,
                    data=b"payload",
                    headers={"X-Fos-Target": f"http://127.0.0.1:{upstream_port}", "X-Telemetry-Service-Id": "test-svc"},
                ) as resp,
            ):
                await resp.read()
    finally:
        await upstream_runner.cleanup()

    assert received_on_upstream_wire == [encoded_path]


async def test_boto3_unsigned_through_proxy_round_trips_against_moto(proxy_server, moto_s3_server):
    _, host_port = moto_s3_server
    bucket, key, body = "test-bucket", "roundtrip/file.txt", b"hello from boto3 through the proxy"
    proxied_s3 = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "test-boto3")
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
        proxied_s3.put_object(Bucket=bucket, Key=key, Body=body)
        assert proxied_s3.get_object(Bucket=bucket, Key=key)["Body"].read() == body
        assert key in [o["Key"] for o in proxied_s3.list_objects_v2(Bucket=bucket).get("Contents", [])]


async def test_proxy_writes_one_usage_log_row_per_request(proxy_server):
    _, _, _, rows, pctx = await _proxy_call(
        proxy_server,
        "/key.parquet",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
            "X-Telemetry-Context": "api:GET /api/dashboard/aggregates",
        },
        resp_headers={"Content-Length": "11", "X-Cache": "HIT"},
        chunks=(b"hello world",),
    )
    assert len(rows) == 1
    row = rows[0]
    assert (row["method"], row["path"], row["bytes"], row["status"], row["caller"], row["service"]) == (
        "GET",
        "/key.parquet",
        11,
        "OK",
        "duckdb.httpfs",
        "FOS",
    )
    assert row["details"].startswith("HIT")
    assert pctx == "api:GET /api/dashboard/aggregates"


async def test_proxy_translates_fos_list_get_to_list_objects_v2(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/bucket?list-type=2&prefix=raw%2F&start-after=raw%2F2026-06-08%2F",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "ingest_scan",
        },
        resp_headers={"Content-Length": "19"},
        chunks=(b"<ListBucketResult/>",),
    )
    assert len(rows) == 1 and rows[0]["service"] == "FOS" and rows[0]["method"] == "LIST_OBJECTS_V2"
    assert "list-type=2" in rows[0]["path"]


async def test_proxy_keeps_get_for_non_list_fos_reads(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/bucket/key.parquet?versionId=abc",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"Content-Length": "32"},
        chunks=(b"x" * 32,),
    )
    assert len(rows) == 1 and rows[0]["method"] == "GET"


async def test_proxy_encodes_xcache_chain_in_details_for_shield_doubling(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.parquet",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"X-Cache": "MISS, MISS"},
        chunks=(b"x" * 1024,),
        cfg=MOCK_CDN_CFG,
    )
    cdn_rows = [r for r in rows if r["service"] == "CDN"]
    assert len(cdn_rows) == 1 and cdn_rows[0]["details"].split(" · ")[0] == "MISS, MISS"


async def test_proxy_handles_50_concurrent_requests_without_head_of_line_blocking(proxy_server):
    with patch.object(
        telemetry_proxy._SESSION, "request", side_effect=lambda *a, **kw: _mock_upstream(chunks=(b"x" * 1024,))[0]
    ):
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
    assert all(s == 200 for s in results) and elapsed < 5.0


async def test_proxy_logs_warning_when_dashboard_context_hits_fos(proxy_server, caplog):
    with caplog.at_level(logging.WARNING, logger="backend.utils.telemetry_proxy"):
        await _proxy_call(
            proxy_server,
            "/k.parquet",
            headers={
                "X-Fos-Target": "bucket.s3.amazonaws.com",
                "X-Telemetry-Service-Id": "test-svc",
                "X-Telemetry-Context": "api:POST /api/dashboard/aggregates",
            },
        )
    assert any("dashboard context hitting FOS" in r.message for r in caplog.records)


async def test_proxy_synthesizes_fos_get_object_for_cdn_full_miss(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.log.gz",
        method="HEAD",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "fastly.api",
        },
        resp_headers={"X-Cache": "MISS, MISS"},
        chunks=(b"x" * 512,),
        cfg=MOCK_CDN_CFG,
    )
    assert len(rows) == 2 and {r["service"] for r in rows} == {"CDN", "FOS"}
    synth = next(r for r in rows if r["service"] == "FOS")
    assert (synth["method"], synth["caller"], synth["path"], synth["bytes"]) == (
        "GET_OBJECT",
        "cdn.miss",
        "/k.log.gz",
        512,
    )


@pytest.mark.parametrize("x_cache", ["HIT", "HIT, HIT", "MISS, HIT"])
async def test_proxy_does_not_synthesize_when_cdn_chain_has_a_hit(proxy_server, x_cache):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.parquet",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"X-Cache": x_cache},
        chunks=(b"x" * 100,),
        cfg=MOCK_CDN_CFG,
    )
    assert len(rows) == 1 and rows[0]["service"] == "CDN"


async def test_proxy_writes_actually_persist_to_metadata_db(proxy_server, tmp_path, monkeypatch):
    monkeypatch.setattr("backend.core.metadata._DATA_DIR", str(tmp_path))
    ctx, _ = _mock_upstream(chunks=(b"x" * 100,), headers={"X-Cache": "MISS, MISS"})
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_CDN_CFG),
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch.object(telemetry_proxy._SESSION, "request", return_value=ctx),
    ):
        async with (
            aiohttp.ClientSession() as s,
            s.get(
                f"{proxy_server.proxy_endpoint()}/k",
                headers={
                    "X-Fos-Target": "cdn.example.net",
                    "X-Telemetry-Service-Id": "real-svc-task6",
                    "X-Telemetry-Caller": "duckdb.httpfs",
                },
            ) as resp,
        ):
            await resp.read()
        telemetry_proxy._flush_log_writes_for_tests()

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("real-svc-task6")
    rows = con.execute(
        "SELECT operation_class, operation_type, url, bytes FROM usage_log WHERE operation_class = 'CDN' ORDER BY id DESC LIMIT 1"
    ).fetchall()
    assert [(r["operation_class"], r["operation_type"], r["url"], r["bytes"]) for r in rows] == [
        ("CDN", "GET", "/k", 200)
    ]


async def test_reset_for_tests_stops_log_flusher(proxy_server):
    with patch("backend.core.metadata.log_usage_calls"):
        proxy_server._submit_log_write("test-svc", {"method": "GET"}, None)
        proxy_server._flush_log_writes_for_tests()
    flusher = proxy_server._LOG_FLUSHER_THREAD
    assert flusher is not None and flusher.is_alive()
    proxy_server.stop_proxy_server()
    proxy_server._reset_for_tests()
    assert not flusher.is_alive()


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
    endpoint_url, host_port = moto_s3_server
    body = bytes(range(256)) * 4096
    _seed_moto_object(endpoint_url, "test-bucket", "range/big.bin", body)
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-test")
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
        resp = proxied.get_object(Bucket="test-bucket", Key="range/big.bin", Range="bytes=100-1099")
        chunk = resp["Body"].read()
    assert len(chunk) == 1000 and chunk == body[100:1100] and resp["ResponseMetadata"]["HTTPStatusCode"] == 206


async def test_proxy_records_each_range_get_as_separate_row(proxy_server, moto_s3_server):
    endpoint_url, host_port = moto_s3_server
    _seed_moto_object(endpoint_url, "test-bucket", "range/three.bin", bytes(range(256)) * 4096)
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-multi")
    captured_rows: list[dict] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        for start, end in [(0, 99), (100, 199), (200, 299)]:
            proxied.get_object(Bucket="test-bucket", Key="range/three.bin", Range=f"bytes={start}-{end}")["Body"].read()
        telemetry_proxy._flush_log_writes_for_tests()
    get_rows = [r for r in captured_rows if r["method"] == "GET"]
    assert len(get_rows) == 3 and all(r["bytes"] == 100 for r in get_rows)


async def test_proxy_records_multipart_put_per_part(proxy_server, moto_s3_server):
    _, host_port = moto_s3_server
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "multipart-test")
    captured_rows: list[dict] = []
    part_bytes = 5 * 1024 * 1024
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        upload_id = proxied.create_multipart_upload(Bucket="test-bucket", Key="multipart/big.bin")["UploadId"]
        parts = []
        for i, ch in enumerate([b"a" * part_bytes, b"b" * part_bytes, b"c" * part_bytes], start=1):
            r = proxied.upload_part(
                Bucket="test-bucket", Key="multipart/big.bin", UploadId=upload_id, PartNumber=i, Body=ch
            )
            parts.append({"ETag": r["ETag"], "PartNumber": i})
        proxied.complete_multipart_upload(
            Bucket="test-bucket", Key="multipart/big.bin", UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        telemetry_proxy._flush_log_writes_for_tests()
    put_rows = [r for r in captured_rows if r["method"] == "PUT"]
    assert len(put_rows) == 3
    for n, p in zip([1, 2, 3], sorted(r["path"] for r in put_rows)):
        assert f"partNumber={n}" in p


async def test_proxy_streams_large_response_without_buffering(proxy_server):
    body_size, chunk_size = 50 * 1024 * 1024, 64 * 1024

    async def _lazy_chunks():
        chunk = b"\x00" * chunk_size
        for _ in range(body_size // chunk_size):
            yield chunk

    mock_resp = AsyncMock(status=200, headers={"Content-Length": str(body_size)})
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
                    f"{proxy_server.proxy_endpoint()}/big.bin", headers={"X-Fos-Target": "fake.upstream.net"}
                ) as resp:
                    assert resp.status == 200
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        received_bytes += len(chunk)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
    assert received_bytes == body_size and (peak / (1024 * 1024)) < 10


async def test_proxy_retries_idempotent_get_on_connection_error_and_succeeds(proxy_server):
    call_count = {"n": 0}
    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"hello",))

    def _flaky(*_a, **_kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise aiohttp.ServerDisconnectedError("simulated pool race")
        return ok_ctx

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_flaky):
        async with (
            aiohttp.ClientSession() as s,
            s.get(f"{proxy_server.proxy_endpoint()}/some/key.avro", headers={"X-Fos-Target": "fake.cdn.net"}) as resp,
        ):
            assert resp.status == 200 and (await resp.read()) == b"hello"
    assert call_count["n"] == 2


async def test_proxy_does_not_retry_non_idempotent_method_on_connection_error(proxy_server):
    call_count = {"n": 0}

    def _always_fails(*_a, **_kw):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("simulated pool race")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with (
            aiohttp.ClientSession() as s,
            s.put(
                f"{proxy_server.proxy_endpoint()}/some/key.avro",
                data=b"payload",
                headers={"X-Fos-Target": "fake.cdn.net"},
            ) as resp,
        ):
            assert resp.status == 502
    assert call_count["n"] == 1


async def test_proxy_retries_at_most_once_on_repeated_connection_errors(proxy_server):
    call_count = {"n": 0}

    def _always_fails(*_a, **_kw):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("upstream genuinely down")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with (
            aiohttp.ClientSession() as s,
            s.get(f"{proxy_server.proxy_endpoint()}/some/key.avro", headers={"X-Fos-Target": "fake.cdn.net"}) as resp,
        ):
            assert resp.status == 502
    assert call_count["n"] == 2


async def test_client_disconnect_mid_stream_is_not_logged_as_upstream_502(proxy_server):
    captured: list[dict] = []
    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"chunk1", b"chunk2", b"chunk3"))
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
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured.extend(rows),
        ),
    ):
        async with aiohttp.ClientSession() as s:
            try:
                async with s.get(
                    f"{proxy_server.proxy_endpoint()}/some/key.avro",
                    headers={"X-Fos-Target": "fake.cdn.net", "X-Telemetry-Service-Id": "test-svc"},
                ) as resp:
                    try:
                        await resp.read()
                    except aiohttp.ClientError:
                        pass
            except aiohttp.ClientError:
                pass
        telemetry_proxy._flush_log_writes_for_tests()

    proxy_rows = [r for r in captured if r.get("service") in ("FOS", "CDN")]
    assert len(proxy_rows) >= 1 and proxy_rows[0]["status"] == "OK"


def test_proxy_connector_caps_keepalive_below_fastly_default():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    try:
        assert telemetry_proxy._SESSION is not None and telemetry_proxy._SESSION.connector is not None
        assert telemetry_proxy._SESSION.connector._keepalive_timeout < 5.0
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.mark.parametrize(
    ("target", "cdn", "fos", "expected"),
    [
        ("127.0.0.1", None, None, True),
        ("127.0.0.1:9999", None, None, True),
        ("localhost", None, None, True),
        ("localhost:8080", None, None, True),
        ("::1", None, None, True),
        ("cdn.example.net", "cdn.example.net", "us-east-1.object.fastlystorage.app", True),
        ("cdn.example.org", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("us-east-1.object.fastlystorage.app", None, "us-east-1.object.fastlystorage.app", True),
        ("eu-west-1.object.fastlystorage.app", None, "us-east-1.object.fastlystorage.app", False),
        ("evil.example.com", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("169.254.169.254", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("CDN.EXAMPLE.NET", "cdn.example.net", "us-east-1.object.fastlystorage.app", True),
        ("US-EAST-1.OBJECT.FASTLYSTORAGE.APP", None, "us-east-1.object.fastlystorage.app", True),
    ],
)
def test_is_target_host_allowed(target, cdn, fos, expected):
    assert telemetry_proxy._is_target_host_allowed(target, cdn, fos) is expected


async def test_proxy_rejects_disallowed_x_fos_target_with_400(proxy_server, caplog):
    mock_cfg = {**MOCK_CDN_CFG, "fos_native_endpoint": "us-east-1.object.fastlystorage.app"}
    with caplog.at_level("WARNING", logger=telemetry_proxy.logger.name):
        st, body, req, _, _ = await _proxy_call(
            proxy_server,
            "/some/path",
            headers={"X-Fos-Target": "evil.example.com", "X-Telemetry-Service-Id": "test-svc"},
            cfg=mock_cfg,
        )
    assert st == 400 and "not allowed" in body.decode().lower() and not req
    assert any(
        "rejected disallowed X-Fos-Target" in rec.message and "evil.example.com" in rec.message
        for rec in caplog.records
    )


async def test_proxy_allows_fos_target_matching_configured_fos_endpoint(proxy_server):
    mock_cfg = {**MOCK_FOS_CFG, "fos_endpoint": "us-east-1.object.fastlystorage.app"}
    st, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/k",
        headers={"X-Fos-Target": "us-east-1.object.fastlystorage.app", "X-Telemetry-Service-Id": "test-svc"},
        cfg=mock_cfg,
    )
    assert st == 200 and "Authorization" in req["headers"]


async def test_proxy_skips_gate_when_no_service_id(proxy_server):
    st, _, req, _, _ = await _proxy_call(proxy_server, "/x", headers={"X-Fos-Target": "fake.cdn.net"}, chunks=(b"ok",))
    assert st == 200 and req


def _make_hooked_boto3_client(proxy_server, moto_s3_server, service_id: str):
    _, host_port = moto_s3_server
    source = {"name": service_id, "service_id": service_id, "fos_native_endpoint": f"http://{host_port}"}
    client = boto3.client(
        "s3",
        endpoint_url=proxy_server.proxy_endpoint(),
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    telemetry_proxy.install_boto3_proxy_hook(client, source)
    return client, MOCK_FOS_CFG


async def test_install_boto3_proxy_hook_routes_request_through_proxy(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-test")
    captured_rows: list[dict] = []
    captured_service: list[str] = []

    def _capture(service_id, rows, process_context=None):
        captured_service.append(service_id)
        captured_rows.extend(rows)

    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        resp = client.head_bucket(Bucket="test-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        telemetry_proxy._flush_log_writes_for_tests()
    assert len(captured_rows) == 1 and captured_service == ["phase2-test"]
    assert captured_rows[0]["method"] == "HEAD" and captured_rows[0]["caller"] == "boto3.headbucket"


async def test_install_boto3_proxy_hook_passes_process_context_per_request(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-ctx")
    contexts_seen: list[str | None] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, _r, process_context=None: contexts_seen.append(process_context),
        ),
    ):
        tlm._set_process_context_for_tests("cron:sync:phase2-ctx-A")
        client.head_bucket(Bucket="test-bucket")
        tlm._set_process_context_for_tests("cron:compact:phase2-ctx-B")
        client.head_bucket(Bucket="test-bucket")
        telemetry_proxy._flush_log_writes_for_tests()
        tlm._set_process_context_for_tests(None)
    assert contexts_seen == ["cron:sync:phase2-ctx-A", "cron:compact:phase2-ctx-B"]


async def test_install_boto3_proxy_hook_falls_back_to_thread_name_when_context_unset(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-fallback")
    contexts_seen: list[str | None] = []
    tlm._PROCESS_CONTEXT.set(None)
    with tlm._LATEST_PROCESS_CONTEXT_LOCK:
        tlm._LATEST_PROCESS_CONTEXT = None
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, _r, process_context=None: contexts_seen.append(process_context),
        ),
    ):
        client.head_bucket(Bucket="test-bucket")
        telemetry_proxy._flush_log_writes_for_tests()
    assert len(contexts_seen) == 1 and contexts_seen[0] is not None and contexts_seen[0].startswith("untagged:")


class _FakeBoto3Client:
    def __init__(self):
        self._handler = None
        self.meta = self
        self.events = self

    def register(self, _signal: str, handler):
        self._handler = handler

    def invoke(self, *, method: str, event_name: str, url: str = ""):
        req = SimpleNamespace(method=method, headers={}, url=url)
        assert self._handler is not None
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


@pytest.mark.parametrize(
    ("method", "event_name", "url", "expected_target", "expect_fastly_key"),
    [
        ("GET", "before-send.s3.GetObject", "", "fos-streamj-logs.global.ssl.fastly.net", True),
        (
            "GET",
            "before-send.s3.GetObject",
            "http://127.0.0.1:8000/bucket/raw/request/year=2026/file.gz",
            "fos-streamj-logs.global.ssl.fastly.net",
            True,
        ),
        ("HEAD", "before-send.s3.HeadObject", "", "fos-streamj-logs.global.ssl.fastly.net", True),
        ("GET", "before-send.s3.ListObjectsV2", "", "us-east.object.fastlystorage.app", False),
        ("PUT", "before-send.s3.PutObject", "", "us-east.object.fastlystorage.app", False),
        ("POST", "before-send.s3.DeleteObjects", "", "us-east.object.fastlystorage.app", False),
    ],
)
def test_install_boto3_proxy_hook_routing_by_operation(method, event_name, url, expected_target, expect_fastly_key):
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method=method, event_name=event_name, url=url)
    assert headers["X-Fos-Target"] == expected_target
    assert (headers.get("x-fastly-key") == "cdn-secret-abc") if expect_fastly_key else ("x-fastly-key" not in headers)


def test_install_boto3_proxy_hook_ingest_download_uses_cdn():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    token = telemetry_proxy._BOTO3_CALLER_HINT.set("ingest_download")
    try:
        headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
        assert headers["X-Fos-Target"] == "fos-streamj-logs.global.ssl.fastly.net"
        assert headers["x-fastly-key"] == "cdn-secret-abc"
    finally:
        telemetry_proxy._BOTO3_CALLER_HINT.reset(token)


def test_install_boto3_proxy_hook_get_with_no_cdn_url_stays_native():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(
        client,
        {
            "name": "streamj-nocdn",
            "service_id": "streamj-nocdn",
            "fos_native_endpoint": "us-east.object.fastlystorage.app",
        },
    )
    headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app" and "x-fastly-key" not in headers


def test_sign_request_adds_content_md5_for_bulk_delete(monkeypatch):
    monkeypatch.setattr(telemetry_proxy, "_load_config_cached", lambda sid: MOCK_FOS_CFG)
    signed = telemetry_proxy._sign_request(
        "POST",
        "https://us-east-1.object.fastlystorage.app/my-bucket?delete",
        {},
        b"<Delete><Object><Key>foo</Key></Object></Delete>",
        "svc-test",
    )
    assert ("Content-MD5" in signed or "content-md5" in signed) and "Authorization" in signed


def _get_fos_secret(source: dict, *, run_s3_cfg: bool = False):
    con = duckdb.connect(":memory:")
    try:
        _ddb._configure_fos(con, source)
        if run_s3_cfg:
            _ic.configure_duckdb_s3(con)
        rows = con.execute("SELECT name, type, secret_string FROM duckdb_secrets() WHERE name='fos_proxy'").fetchall()
        ep = con.execute("SELECT current_setting('s3_endpoint')").fetchone()[0] if run_s3_cfg else None
        return rows, ep
    finally:
        con.close()


def test_configure_fos_creates_proxy_secret_no_cdn(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-no-cdn",
            "service_id": "phase3a-no-cdn",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    assert len(rows) == 1 and rows[0][1].upper() == "S3"
    sec = rows[0][2]
    assert proxy_server.proxy_endpoint().replace("http://", "") in sec
    for req in (
        "X-Fos-Target=us-east-1.object.fastlystorage.app",
        "X-Telemetry-Service-Id=phase3a-no-cdn",
        "X-Telemetry-Caller=duckdb.httpfs",
    ):
        assert req in sec
    assert "x-fastly-key" not in sec.lower()


def test_configure_fos_creates_proxy_secret_with_cdn(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-cdn",
            "service_id": "phase3a-cdn",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "cdn.example.com",
            "cdn_url": "https://cdn.example.com",
            "cdn_secret": "fastly-secret-abc",
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    sec = rows[0][2]
    assert "X-Fos-Target=cdn.example.com" in sec
    assert "us-east-1.object.fastlystorage.app" not in sec
    assert "x-fastly-key=fastly-secret-abc" in sec


def test_configure_fos_uses_native_fos_when_edge_only_is_false(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-native-reads",
            "service_id": "phase3a-native-reads",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "cdn.example.com",
            "cdn_url": "https://cdn.example.com",
            "cdn_secret": "fastly-secret-abc",
            "provisioning": {"edge_only": False},
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    sec = rows[0][2]
    assert "X-Fos-Target=us-east-1.object.fastlystorage.app" in sec
    assert "X-Fos-Target=cdn.example.com" not in sec
    assert "x-fastly-key" not in sec.lower()


def test_configure_duckdb_s3_does_not_clobber_proxy_secret(proxy_server):
    rows, ep = _get_fos_secret(
        {
            "name": "phase3a-icestack",
            "service_id": "phase3a-icestack",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "access_key_id": "AKIA-test",
            "secret_access_key": "secret-test",
            "region": "us-east-1",
        },
        run_s3_cfg=True,
    )
    assert len(rows) == 1 and proxy_server.proxy_endpoint().replace("http://", "") in rows[0][2]
    assert ep in ("", None)


async def test_duckdb_read_through_proxy_logs_telemetry(proxy_server, moto_s3_server, tmp_path):
    endpoint_url, host_port = moto_s3_server
    buf = io.BytesIO()
    pq.write_table(pa.table({"x": list(range(8))}), buf)
    _seed_moto_object(endpoint_url, "test-bucket", "hello.parquet", buf.getvalue())

    source = {
        "name": "phase3a-e2e",
        "service_id": "phase3a-e2e",
        "fos_native_endpoint": f"http://{host_port}",
        "endpoint": f"http://{host_port}",
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "region": "us-east-1",
        "duckdb_path": str(tmp_path / "phase3a_e2e.duckdb"),
    }
    captured_rows: list[dict] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        con = _ddb.get_connection(source, skip_view_update=True)
        try:
            (count,) = con.execute("SELECT count(*) FROM read_parquet('s3://test-bucket/hello.parquet')").fetchone()
            assert count == 8
        finally:
            con.close()
        telemetry_proxy._flush_log_writes_for_tests()

    duckdb_rows = [r for r in captured_rows if r.get("caller") == "duckdb.httpfs"]
    assert any("test-bucket" in (r.get("path") or "") and "hello.parquet" in (r.get("path") or "") for r in duckdb_rows)


def test_configure_fos_concurrent_no_write_write_conflict(proxy_server, tmp_path):
    db_path = str(tmp_path / "concurrent_fos.duckdb")
    source = {
        "name": "phase3a-concurrent",
        "service_id": "phase3a-concurrent",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-concurrent",
        "secret_access_key": "secret-concurrent",
        "region": "us-east-1",
    }
    errors: list[Exception] = []

    def _worker(_i: int) -> None:
        try:
            con = duckdb.connect(db_path)
            try:
                _ddb._configure_fos(con, source)
            finally:
                con.close()
        except Exception as e:
            errors.append(e)

    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(_worker, range(32)))
    assert not errors


def test_fastapi_lifespan_starts_proxy():
    from backend import main as _main

    called = {"n": 0}
    with patch(
        "backend.utils.telemetry_proxy.start_proxy_server", side_effect=lambda: called.__setitem__("n", called["n"] + 1)
    ):

        async def _drive():
            async with _main.lifespan(_main.app):
                pass

        asyncio.run(_drive())
    assert called["n"] >= 1


def test_get_fos_client_paginator_accepts_caller_hint():
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()
    client = _ddb._get_fos_client(
        {
            "name": "phase4-caller-hint",
            "service_id": "phase4-caller-hint",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "https://us-east-1.object.fastlystorage.app",
            "access_key_id": "test",
            "secret_access_key": "test",
        }
    )
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
    assert pag is not None and hasattr(pag, "paginate")


def test_get_fos_client_paginator_caller_hint_flows_to_proxy_header():
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()
    client = _ddb._get_fos_client(
        {
            "name": "phase4-caller-hint-header",
            "service_id": "phase4-caller-hint-header",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "https://us-east-1.object.fastlystorage.app",
        }
    )
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
    captured = []
    req = SimpleNamespace(headers={})
    if hasattr(pag, "_set_caller_hint_context"):
        token = pag._set_caller_hint_context()
        try:
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            pag._reset_caller_hint_context(token)
    else:
        token = telemetry_proxy._BOTO3_CALLER_HINT.set("ingest_scan")
        try:
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            telemetry_proxy._BOTO3_CALLER_HINT.reset(token)
    assert captured == ["ingest_scan"]
    req2 = SimpleNamespace(headers={})
    client.meta.events.emit("before-send.s3.ListObjectsV2", request=req2)
    assert req2.headers.get("X-Telemetry-Caller") == "boto3.listobjectsv2"


def test_upstream_timeout_default_is_bounded():
    total = telemetry_proxy._UPSTREAM_TIMEOUT.total
    assert total is not None and 0 < total <= 120


def test_upstream_timeout_env_var_override():
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
