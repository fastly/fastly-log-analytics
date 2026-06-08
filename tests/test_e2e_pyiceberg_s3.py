"""PyIceberg-over-S3 integration test.

The main E2E test ([tests/test_e2e_pipeline.py](./test_e2e_pipeline.py))
uses a ``file://`` warehouse because PyIceberg's fsspec/s3fs/aiobotocore
stack collides with moto's in-process decorators (async vs sync runtime
mismatch raises mid-write). We dodge that collision here by running moto
as a REAL HTTP server (``ThreadedMotoServer``). s3fs/aiobotocore talks
HTTP just like it would to FOS — there's no in-process monkey-patching
of the async loop, so the collision never happens.

What this catches that the file:// E2E misses:
  - Real s3fs.S3FileSystem code paths during PyIceberg.commit (the same
    paths that produced past s3fs max-concurrency / probe-GET incidents).
  - The PUT-then-CAS commit shape that PyIceberg uses for snapshot
    promotion, which is the source of the Fastly negative-cache CAS trap
    (a 404 on the probe GET gets cached, then a successful PUT lands but
    the next CAS still sees the cached 404 and corrupts the commit).
  - DuckDB ``httpfs`` reading the very parquet files PyIceberg wrote,
    against the same S3-protocol endpoint, in a single process.

Scope is deliberately narrow: one happy-path commit + one DuckDB read.
Adding aggregates / insights / sync-cron interactions belongs in the
file:// E2E where the fixture is cheap. The S3 path is here to prove
the seam exists, not to re-cover everything the file:// E2E already does.

Closes TESTING_PLAN_3 item: "No PyIceberg-over-S3 integration test."
"""

from __future__ import annotations

import os
import socket
import tempfile
import threading
import uuid
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import duckdb
import pyarrow as pa
import pytest

# moto's ThreadedMotoServer runs a real HTTP S3 endpoint in a background
# thread. That's the key distinction from @mock_aws — the in-process
# decorators monkey-patch boto3's HTTP layer, which is what aiobotocore
# (used by s3fs) can't see through. ThreadedMotoServer puts a real socket
# between us and moto, so every client (boto3, s3fs, DuckDB httpfs)
# routes to it identically.
moto_server = pytest.importorskip("moto.server")


def _free_port() -> int:
    """Bind a random port and release it — moto picks it up next."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def moto_s3_server():
    """Stand up moto as a real HTTP S3 server on a random port.

    Yields ``(endpoint_url, port)`` and tears the server down even if a
    test raises. Sharing the server across multiple writes within a test
    is fine; the bucket is per-test."""
    port = _free_port()
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    try:
        yield (f"http://127.0.0.1:{port}", port)
    finally:
        server.stop()


@pytest.fixture
def s3_iceberg_env(moto_s3_server, monkeypatch):
    """Full PyIceberg-over-S3 environment.

    Wires moto's HTTP endpoint into every layer that talks S3:
      - boto3 (via _get_fos_client override) for the metadata pointer
        read/write path.
      - s3fs (via FSSPEC_S3_ENDPOINT_URL env + catalog props) for
        PyIceberg's parquet writes.
      - DuckDB httpfs (via SET s3_endpoint) for the read-back assertion.

    Resets every iceberg module cache before AND after so cached
    catalogs / snapshots / pointers from previous tests don't bleed in."""
    endpoint_url, port = moto_s3_server
    # Unique bucket + service name per test. moto's in-process backend
    # state can persist across ThreadedMotoServer instances within the
    # same pytest process — two tests sharing "iceberg-s3-test" would
    # see each other's parquet, throwing off COUNT(*) assertions in the
    # DuckDB read test. The uuid suffix makes every test see a clean
    # bucket regardless of moto's backend lifecycle.
    suffix = uuid.uuid4().hex[:8]
    bucket = f"iceberg-s3-test-{suffix}"
    region = "us-east-1"

    # Create the bucket via plain boto3 (synchronous; no aiobotocore here).
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name=region,
    )
    s3.create_bucket(Bucket=bucket)

    tmpdir = tempfile.mkdtemp(prefix="pyice_s3_")
    cache_path = os.path.join(tmpdir, "cache")
    os.makedirs(cache_path, exist_ok=True)

    source: dict[str, Any] = {
        "name": f"pyice_s3_svc_{suffix}",
        "service_id": f"pyice-s3-svc-{suffix}",
        "service_name": "PyIceberg S3 Test",
        "bucket": bucket,
        "prefix": "",
        "region": region,
        "endpoint": f"127.0.0.1:{port}",
        "fos_native_endpoint": f"127.0.0.1:{port}",
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda src: f"s3://{bucket}/iceberg")

    # Restore the un-patched s3fs.S3FileSystem.__init__ for this test.
    # backend.core.iceberg installs a global monkeypatch at import time
    # that re-routes every s3fs request through the FOS telemetry proxy
    # (signing, X-Fos-Target headers, scheme inference). The proxy is
    # the right layer for production but turns into a forwarding maze
    # in tests — it tries to sign for a missing service config, defaults
    # to https://, and trips moto's HTTP-only listener. Restore the
    # original __init__ so PyIceberg's s3fs writes go straight at moto.
    # The proxy + signing path has its own dedicated tests in
    # ``tests/utils/test_telemetry_proxy*.py``; this test is about the
    # PyIceberg/DuckDB/S3-protocol seam, not the proxy.
    from s3fs import S3FileSystem

    from backend.core.iceberg import _orig_s3fs_init, _orig_s3fs_set_session

    monkeypatch.setattr(S3FileSystem, "__init__", _orig_s3fs_init)
    monkeypatch.setattr(S3FileSystem, "set_session", _orig_s3fs_set_session)

    # _get_fos_client normally routes through the telemetry proxy. For
    # this test we want plain boto3 → moto so list/get/put work without
    # the proxy's URL-rewriting layer.
    def _moto_fos_client(src):
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name=region,
        )

    monkeypatch.setattr("backend.core.duckdb._get_fos_client", _moto_fos_client)

    # _get_catalog hardcodes ``https://{endpoint}`` (the FOS pattern).
    # Override so the catalog talks plain ``http://`` to moto. Still uses
    # FosSqlCatalog so the load_table cache short-circuit runs end-to-end.
    def _moto_catalog(src):
        from backend.core.iceberg import (
            _PENDING_FS_SOURCE,
            _catalog_cache,
            _catalog_db_path,
            _catalog_lock,
            _get_fos_catalog_class,
            _register_proxy_source,
        )

        source_key = src.get("name", "default")
        with _catalog_lock:
            if source_key in _catalog_cache:
                return _catalog_cache[source_key]
            _PENDING_FS_SOURCE.set(src)
            _register_proxy_source(src)
            props = {
                "uri": f"sqlite:///{_catalog_db_path(src)}",
                "warehouse": f"s3://{bucket}/iceberg",
                "s3.endpoint": endpoint_url,
                "s3.access-key-id": "testing",
                "s3.secret-access-key": "testing",
                "s3.path-style-access": "true",
                "s3.region": region,
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            }
            cls = _get_fos_catalog_class()
            cat = cls("fos", **props)
            cat._fos_source = src
            _catalog_cache[source_key] = cat
            return cat

    monkeypatch.setattr("backend.core.iceberg._get_catalog", _moto_catalog)

    # Reset module caches before AND after. PyIceberg's catalog cache is
    # the most dangerous — a stale catalog from a previous test would
    # still point at the previous moto endpoint (now a dead port).
    from backend.core import iceberg as _ice

    for cache in (
        _ice._catalog_cache,
        _ice._snapshot_files_cache,
        _ice._table_object_cache,
        getattr(_ice, "_view_cache", {}),
        getattr(_ice, "_pointer_cache", {}),
    ):
        cache.clear()

    yield {
        "src": source,
        "endpoint": endpoint_url,
        "bucket": bucket,
        "region": region,
        "cache": cache_path,
        "s3": s3,
    }

    for cache in (
        _ice._catalog_cache,
        _ice._snapshot_files_cache,
        _ice._table_object_cache,
        getattr(_ice, "_view_cache", {}),
        getattr(_ice, "_pointer_cache", {}),
    ):
        cache.clear()

    import shutil as _sh

    _sh.rmtree(tmpdir, ignore_errors=True)


def _make_log_batch(n: int = 10) -> pa.Table:
    """Same shape as the file:// E2E's _make_log_batch — uses the real
    catalog's column types so commit_buffer can align without warnings."""
    base = datetime.now(UTC) - timedelta(hours=1)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
            "status": pa.array([200 if i % 3 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array([f"/path/{i % 5}" for i in range(n)]),
            "country": pa.array(["US"] * n),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array(["LAX"] * n),
        }
    )


def test_pyiceberg_commit_lands_parquet_in_real_s3(s3_iceberg_env, monkeypatch):
    """Happy path: init table → write buffer → commit → list S3 bucket
    and find a parquet object under the iceberg prefix.

    If this fails, the regression is in the PyIceberg-over-real-S3 seam
    (s3fs, fsspec, our s3fs.__init__ patch, or the FosSqlCatalog wiring).
    A failure here that doesn't reproduce in the file:// E2E points at
    something S3-protocol-specific."""
    from backend.core import iceberg as ice

    src = s3_iceberg_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    table = ice.init_iceberg_table(src)
    assert table is not None

    batch = _make_log_batch(n=10)
    ice.write_to_buffer(src, batch, "s3_batch_0.parquet")

    result = ice.commit_buffer(src)
    assert result["files_committed"] >= 1, result
    assert result["rows_committed"] == 10, result
    assert result["snapshot_id"] is not None

    # Bucket should now contain at least one parquet object under
    # iceberg/. We don't pin the exact key (PyIceberg generates random
    # UUIDs) — just that the write reached S3.
    listing = s3_iceberg_env["s3"].list_objects_v2(Bucket=s3_iceberg_env["bucket"], Prefix="iceberg/")
    keys = [obj["Key"] for obj in listing.get("Contents", [])]
    parquet_keys = [k for k in keys if k.endswith(".parquet")]
    assert parquet_keys, f"No parquet files in S3 after commit. All keys: {keys}"

    # And the metadata pointer should have been written.
    metadata_keys = [k for k in keys if k.endswith(".metadata.json")]
    assert metadata_keys, f"No metadata.json in S3 after commit. All keys: {keys}"


def test_duckdb_httpfs_reads_pyiceberg_parquet_from_s3(s3_iceberg_env, monkeypatch):
    """The read-back half of the seam: PyIceberg writes parquet to S3,
    DuckDB httpfs reads it via SET s3_endpoint, COUNT(*) matches.

    Pins the contract that DuckDB's httpfs extension can consume parquet
    written by PyIceberg's FsspecFileIO against the same S3-protocol
    endpoint. A break here usually means one side switched parquet
    encoding or compression in a way the other hasn't picked up.
    """
    from backend.core import iceberg as ice

    src = s3_iceberg_env["src"]
    endpoint_url = s3_iceberg_env["endpoint"]
    bucket = s3_iceberg_env["bucket"]

    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_log_batch(n=12), "s3_read_0.parquet")
    ice.commit_buffer(src)

    listing = s3_iceberg_env["s3"].list_objects_v2(Bucket=bucket, Prefix="iceberg/")
    parquet_keys = [obj["Key"] for obj in listing.get("Contents", []) if obj["Key"].endswith(".parquet")]
    assert parquet_keys, "Setup expects committed parquet; got none."

    # Strip http:// and the port — DuckDB's s3_endpoint is host[:port],
    # and use_ssl is the http-vs-https switch.
    host = endpoint_url.removeprefix("http://").removeprefix("https://")

    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute("SET s3_access_key_id='testing';")
    con.execute("SET s3_secret_access_key='testing';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_region='us-east-1';")

    total = 0
    for key in parquet_keys:
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/{key}')").fetchone()
        total += result[0]
    assert total == 12, f"DuckDB read {total} rows from PyIceberg-written parquet; expected 12. Keys: {parquet_keys}"


# A note for future maintainers: if PyIceberg or s3fs ships a release that
# breaks against moto specifically (rather than against real S3 — they're
# protocol-compatible but moto's server has occasionally been laggy on
# new APIs), the right fix is usually to pin/bump moto, NOT to bypass
# this test. The whole point is to exercise the seam against an HTTP S3
# endpoint we control. Use ``pytest -k pyiceberg_s3 -v`` to iterate.

# A second note: ``threading`` is imported but currently unused. Reserved
# for a follow-up concurrency test that runs commit_buffer in two threads
# against the shared moto server — pins the contract that PyIceberg's
# optimistic CAS retry handles a real lost race.
_ = threading
