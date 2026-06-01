"""Phase 3a tests for the telemetry proxy — DuckDB httpfs through the proxy.

Design spec: docs/superpowers/specs/2026-05-19-telemetry-proxy-design.md (§Phase 3a)

These tests live in their own file (matching the Phase 2 split) so the
Phase 3a fixtures don't pollute earlier files. Shared infrastructure with
Phase 1/2 is duplicated on purpose — moving it to conftest.py would
silently change discovery elsewhere, which we don't want.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import aiohttp
import boto3
import duckdb
import pytest
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
        yield endpoint_url, f"{host}:{port}", seed_client
    finally:
        server.stop()


# ── Task 1: _configure_fos creates a proxy SECRET (no CDN) ──────────────────


def test_configure_fos_creates_proxy_secret_no_cdn(proxy_server):
    """``_configure_fos`` must create a SECRET named ``fos_proxy`` pointing
    at the local proxy. ENDPOINT is the bare proxy host:port (no scheme),
    USE_SSL is false (proxy is plain HTTP), URL_STYLE is path. The
    EXTRA_HTTP_HEADERS must carry X-Fos-Target (native FOS endpoint),
    X-Telemetry-Service-Id, and X-Telemetry-Caller='duckdb.httpfs'. Without
    these the proxy returns 400 or routes the read to the wrong upstream."""
    from backend.core import duckdb as _ddb

    source = {
        "name": "phase3a-no-cdn",
        "service_id": "phase3a-no-cdn",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-phase3a",
        "secret_access_key": "secret-phase3a",
        "region": "us-east-1",
    }
    con = duckdb.connect(":memory:")
    try:
        _ddb._configure_fos(con, source)

        rows = con.execute("SELECT name, type, secret_string FROM duckdb_secrets() WHERE name='fos_proxy'").fetchall()
        assert len(rows) == 1, f"expected 1 fos_proxy secret, got {len(rows)}: {rows}"
        name, stype, secret_string = rows[0]
        assert stype.upper() == "S3"
        proxy_host_port = proxy_server.proxy_endpoint().replace("http://", "")
        assert proxy_host_port in secret_string, (
            f"secret_string should reference proxy host:port {proxy_host_port}; got: {secret_string}"
        )
        for required in (
            "X-Fos-Target=us-east-1.object.fastlystorage.app",
            "X-Telemetry-Service-Id=phase3a-no-cdn",
            "X-Telemetry-Caller=duckdb.httpfs",
        ):
            assert required in secret_string, (
                f"missing required header in extra_http_headers: {required}\nsecret_string={secret_string}"
            )
        assert "x-fastly-key" not in secret_string.lower(), (
            "no-CDN source must not inject x-fastly-key into the proxy SECRET"
        )
    finally:
        con.close()


# ── Task 2: _configure_fos with CDN: x-fastly-key + CDN host as target ──────


def test_configure_fos_creates_proxy_secret_with_cdn(proxy_server):
    """When source has cdn_url + cdn_secret, the proxy SECRET must:
      - Set X-Fos-Target to the CDN host (so proxy forwards there, and
        proxy's _sign_request skips SigV4 re-signing for CDN targets).
      - Carry x-fastly-key in EXTRA_HTTP_HEADERS (the CDN's auth header).
    Otherwise routing through the proxy would either fail CDN auth or
    incur native-FOS cost on every dashboard read."""
    from backend.core import duckdb as _ddb

    source = {
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
    con = duckdb.connect(":memory:")
    try:
        _ddb._configure_fos(con, source)

        rows = con.execute("SELECT secret_string FROM duckdb_secrets() WHERE name='fos_proxy'").fetchall()
        assert len(rows) == 1
        secret_string = rows[0][0]
        assert "X-Fos-Target=cdn.example.com" in secret_string, (
            f"CDN-configured source must target CDN host, not native FOS\nsecret_string={secret_string}"
        )
        assert "us-east-1.object.fastlystorage.app" not in secret_string, (
            "native FOS endpoint must not leak into headers when CDN is configured"
        )
        assert "x-fastly-key=fastly-secret-abc" in secret_string, (
            f"CDN-configured source must inject x-fastly-key\nsecret_string={secret_string}"
        )
    finally:
        con.close()


# ── Task 5: iceberg.configure_duckdb_s3 does not clobber proxy SECRET ───────


def test_configure_duckdb_s3_does_not_clobber_proxy_secret(proxy_server):
    """iceberg.configure_duckdb_s3 runs immediately after _configure_fos in
    get_connection. Pin that it leaves the fos_proxy SECRET alone (and that
    no SET s3_endpoint is issued either, which would override routing)."""
    from backend.core import duckdb as _ddb
    from backend.core import iceberg as _ic

    source = {
        "name": "phase3a-icestack",
        "service_id": "phase3a-icestack",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-test",
        "secret_access_key": "secret-test",
        "region": "us-east-1",
    }
    con = duckdb.connect(":memory:")
    try:
        _ddb._configure_fos(con, source)
        _ic.configure_duckdb_s3(con)

        rows = con.execute("SELECT secret_string FROM duckdb_secrets() WHERE name='fos_proxy'").fetchall()
        assert len(rows) == 1
        proxy_host_port = proxy_server.proxy_endpoint().replace("http://", "")
        assert proxy_host_port in rows[0][0], "configure_duckdb_s3 must NOT clobber the proxy SECRET"
        ep = con.execute("SELECT current_setting('s3_endpoint')").fetchone()[0]
        assert ep in ("", None), f"configure_duckdb_s3 must NOT SET s3_endpoint; got: {ep!r}"
    finally:
        con.close()


# ── Task 6: end-to-end DuckDB read through proxy logs telemetry ─────────────


def _write_parquet_to_moto(seed_client, bucket: str, key: str, rows: int = 8) -> bytes:
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"x": list(range(rows))})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    body = buf.getvalue()
    seed_client.put_object(Bucket=bucket, Key=key, Body=body)
    return body


async def test_duckdb_read_through_proxy_logs_telemetry(proxy_server, moto_s3_server, tmp_path):
    """Acceptance-level integration: DuckDB issues a real read_parquet
    against moto via the proxy, and the proxy logs at least one usage row
    with caller='duckdb.httpfs' and the bucket/key referenced. If headers,
    signing, or routing are wrong, the read either errors or no row gets
    captured."""
    from backend.core import duckdb as _ddb

    endpoint_url, host_port, seed_client = moto_s3_server
    _write_parquet_to_moto(seed_client, "test-bucket", "hello.parquet", rows=8)

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
    mock_cfg = {
        "fos_access_key_id": "testing",
        "fos_secret_access_key": "testing",
        "fos_region": "us-east-1",
    }

    captured_rows: list[dict] = []

    def _capture(service_id, rows, process_context=None):
        captured_rows.extend(rows)

    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch("backend.core.metadata_db.log_usage_calls", side_effect=_capture),
    ):
        con = _ddb.get_connection(source, skip_view_update=True)
        try:
            (count,) = con.execute("SELECT count(*) FROM read_parquet('s3://test-bucket/hello.parquet')").fetchone()
            assert count == 8, f"expected 8 rows from parquet, got {count}"
        finally:
            con.close()
        telemetry_proxy._flush_log_writes_for_tests()

    assert len(captured_rows) >= 1, (
        f"expected >=1 captured usage row from DuckDB->proxy round-trip, got {len(captured_rows)}"
    )
    duckdb_rows = [r for r in captured_rows if r.get("caller") == "duckdb.httpfs"]
    assert len(duckdb_rows) >= 1, (
        f"expected at least one row with caller='duckdb.httpfs'; got callers={[r.get('caller') for r in captured_rows]}"
    )
    keyed = [
        r for r in duckdb_rows if "test-bucket" in (r.get("path") or "") and "hello.parquet" in (r.get("path") or "")
    ]
    assert len(keyed) >= 1, (
        f"expected a usage row mentioning test-bucket/hello.parquet; got paths={[r.get('path') for r in duckdb_rows]}"
    )


# ── Regression: concurrent _configure_fos must not race on SECRET catalog ───


def test_configure_fos_concurrent_no_write_write_conflict(proxy_server, tmp_path):
    """Two or more connections opened simultaneously on the SAME DuckDB file
    each issue ``CREATE OR REPLACE SECRET fos_proxy`` from _configure_fos.
    DuckDB raises ``TransactionContext Error: Catalog write-write conflict
    on create with "fos_proxy"`` when the writes overlap, which crashes
    dashboard requests and ASGI handlers in production. Pin that concurrent
    _configure_fos calls all succeed (process-level lock serialises them)."""
    from concurrent.futures import ThreadPoolExecutor

    from backend.core import duckdb as _ddb

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

    assert not errors, (
        "concurrent _configure_fos must not raise catalog write-write conflict; "
        f"got {len(errors)} error(s); first: {errors[0]!r}"
    )
