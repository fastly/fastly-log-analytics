"""Integration tests for adopt_iceberg_to_ducklake — real parquet fixtures,
real DuckDB, real DuckLake catalog (replaces the earlier mock-only test that
asserted on MagicMock call strings and could not catch a broken migration).

The FOS-resident tests are equally real: a ThreadedMotoServer stands in for
Fastly Object Storage, pyiceberg writes an actual s3-warehouse Iceberg table
into it through the production s3fs/telemetry-proxy path, and DuckDB reads
those ``s3://`` objects back through ``get_connection``. Only the object
store is mocked."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.duckdb import get_connection
from backend.core.iceberg._ducklake import ducklake_table_name
from backend.core.iceberg._ducklake_migration import (
    _normalize_data_path,
    adopt_iceberg_to_ducklake,
)


def _make_source(tmp_path, name: str) -> dict:
    cache = tmp_path / f"cache_{name}"
    cache.mkdir(parents=True)
    return {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }


def _arrow_rows(n: int, start_second: int = 0) -> pa.Table:
    base = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(seconds=start_second + i) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.0.0.{start_second + i}" for i in range(n)]),
            "status": pa.array([200] * n),
        }
    )


def _write_legacy_parquet(source: dict, rel_path: str, n: int) -> str:
    """Write a legacy hive-partition parquet the way the old sync path did."""
    path = os.path.join(source["_cache_dir_override"], "data", rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(_arrow_rows(n), path)
    return path


def _lake_count(source: dict) -> int:
    con = get_connection(source)
    try:
        table = ducklake_table_name(source)
        row = con.execute(f'SELECT count(*) FROM lake."{table}"').fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


@pytest.fixture
def migration_source(tmp_path, monkeypatch):
    name = f"mig{uuid.uuid4().hex[:8]}"
    src = _make_source(tmp_path, name)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == name else None)
    return src


# ── FOS-resident legacy table (the durable, system-of-truth source) ──────────


@pytest.fixture
def fos_migration_source(tmp_path, monkeypatch):
    """A cloud-backed source whose legacy Iceberg table lives in object storage.

    ``ThreadedMotoServer`` (not the in-process ``mock_aws``) is required
    because pyiceberg reaches S3 through s3fs/aiobotocore and DuckDB
    through httpfs — both real HTTP clients, neither of which the
    in-process botocore patch intercepts.

    ``telemetry_proxy._load_config_cached`` is stubbed because the proxy
    is the sole SigV4 signer and resolves its keys from the on-disk
    service config, which a synthetic source dict has no entry in
    (unsigned forwarding gets a 403). ``fos_native_endpoint`` is
    deliberately absent from that stub: the proxy's write-verb override
    strips the scheme off it, which would send PUTs to ``https://`` on a
    plain-HTTP moto listener.
    """
    from moto.server import ThreadedMotoServer

    from backend.utils import telemetry_proxy

    server = ThreadedMotoServer(port=0)
    server.start()
    _, port = server.get_host_and_port()
    endpoint = f"http://127.0.0.1:{port}"

    name = f"fos{uuid.uuid4().hex[:8]}"
    bucket = f"bucket-{name}"
    boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="testkey",
        aws_secret_access_key="testsecret",
        region_name="us-east-1",
    ).create_bucket(Bucket=bucket)

    cache = tmp_path / f"cache_{name}"
    cache.mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "bucket": bucket,
        "prefix": "svc",
        "endpoint": endpoint,
        "fos_native_endpoint": endpoint,
        "access_key_id": "testkey",
        "secret_access_key": "testsecret",
        "region": "us-east-1",
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }

    monkeypatch.setattr(
        telemetry_proxy,
        "_load_config_cached",
        lambda sid: {
            "service_id": name,
            "fos_access_key_id": "testkey",
            "fos_secret_access_key": "testsecret",
            "fos_region": "us-east-1",
        },
    )
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == name else None)
    try:
        yield src
    finally:
        server.stop()


def _commit_legacy_iceberg_table(src: dict, table: pa.Table) -> list[str]:
    """Create + append to the legacy pyiceberg table, returning its data files."""
    from backend.core.iceberg import _core as _core_mod

    catalog = _core_mod._get_catalog(src)
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    ice_table = catalog.create_table(("default", "logs"), schema=table.schema)
    ice_table.append(table)

    loaded = catalog.load_table(("default", "logs"))
    snapshot = loaded.current_snapshot()
    assert snapshot is not None
    return [
        entry.data_file.file_path
        for manifest in snapshot.manifests(loaded.io)
        for entry in manifest.fetch_manifest_entry(loaded.io)
        if entry.status.name != "DELETED"
    ]


@pytest.mark.timeout(180)
def test_adopt_registers_fos_resident_iceberg_data_files(fos_migration_source):
    """The new path: enumerate the legacy table's FOS data files and adopt those.

    The pre-fix adopter globbed only local disk, so an operator whose
    ``data_retention_days`` exceeds their ``cache_retention_days`` lost
    visibility of everything outside the local cache window.
    """
    src = fos_migration_source
    fos_files = _commit_legacy_iceberg_table(src, _arrow_rows(6))
    assert fos_files and all(p.startswith("s3://") for p in fos_files)

    res = adopt_iceberg_to_ducklake(src["name"])

    assert res["source"] == "iceberg_table"
    assert res["adopted_files"] == len(fos_files)
    assert res["rows_adopted"] == 6
    assert _lake_count(src) == 6, "FOS-resident rows must be queryable through lake.<table>"


@pytest.mark.timeout(180)
def test_adopt_fos_resident_table_is_idempotent(fos_migration_source):
    """ducklake_add_data_files duplicates rows on a re-add — including for
    ``s3://`` paths, which DuckLake echoes back verbatim rather than as a
    filesystem path. A second run must change nothing."""
    src = fos_migration_source
    fos_files = _commit_legacy_iceberg_table(src, _arrow_rows(5))

    first = adopt_iceberg_to_ducklake(src["name"])
    assert first["rows_adopted"] == 5
    assert _lake_count(src) == 5

    second = adopt_iceberg_to_ducklake(src["name"])
    assert second["adopted_files"] == 0
    assert second["skipped_files"] == len(fos_files)
    assert second["rows_adopted"] == 0
    assert _lake_count(src) == 5, "re-running the migration must not duplicate rows"


@pytest.mark.timeout(180)
def test_adopt_prefers_fos_table_over_the_local_cache_mirror(fos_migration_source):
    """``cache/.../data/`` is a *mirror* of the FOS objects the manifests name.

    Adopting both would double-count every row inside the cache window,
    so the legacy table wins and the local tree is ignored. The local
    fixture here holds different rows precisely so a union would show up
    as an inflated count rather than passing silently.
    """
    src = fos_migration_source
    _commit_legacy_iceberg_table(src, _arrow_rows(4))
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-13/stale.parquet", 7)

    res = adopt_iceberg_to_ducklake(src["name"])

    assert res["source"] == "iceberg_table"
    assert res["rows_adopted"] == 4
    assert _lake_count(src) == 4


@pytest.mark.timeout(180)
def test_adopt_fails_loudly_when_the_legacy_table_is_unreadable(fos_migration_source, monkeypatch):
    """Half-migrated is worse than refused: an existing-but-unreadable
    legacy table must raise, not quietly adopt whatever else is lying
    around locally."""
    from backend.core.iceberg import _core as _core_mod
    from backend.core.iceberg import _ducklake_migration as mig

    src = fos_migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-13/local.parquet", 3)

    monkeypatch.setattr(mig, "_legacy_metadata_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        _core_mod,
        "_load_table_cached",
        lambda *a, **k: (_ for _ in ()).throw(OSError("manifest list unreachable")),
    )
    monkeypatch.setattr(_core_mod, "_try_register_from_fos", lambda *a, **k: None)

    with pytest.raises(RuntimeError, match="could not be loaded"):
        adopt_iceberg_to_ducklake(src["name"])


def test_legacy_metadata_probe_failure_is_loud(fos_migration_source, monkeypatch):
    """ "We could not look" must never be reported as "there is nothing there"."""
    from backend.core.iceberg import _core as _core_mod
    from backend.core.iceberg import _ducklake_migration as mig

    src = fos_migration_source
    monkeypatch.setattr(
        mig,
        "_legacy_metadata_exists",
        lambda *a, **k: (_ for _ in ()).throw(OSError("ListBucket denied")),
    )
    monkeypatch.setattr(
        _core_mod,
        "_load_table_cached",
        lambda *a, **k: (_ for _ in ()).throw(OSError("nope")),
    )
    monkeypatch.setattr(_core_mod, "_try_register_from_fos", lambda *a, **k: None)

    with pytest.raises(RuntimeError, match="could not determine whether a legacy Iceberg table exists"):
        adopt_iceberg_to_ducklake(src["name"])


# ── Path normalisation ───────────────────────────────────────────────────────


def test_normalize_data_path_leaves_object_uris_verbatim():
    """``os.path.abspath('s3://b/k')`` yields ``/cwd/s3:/b/k``.

    That is why the dedupe branches on scheme: DuckLake hands object URIs
    back unchanged, so abspath-ing them makes the "already adopted" set
    never match and every re-run duplicates the whole table.
    """
    uri = "s3://bucket/svc/iceberg/data/f.parquet"
    assert _normalize_data_path(uri) == uri
    assert os.path.abspath(uri) != uri  # documents the footgun being avoided

    assert _normalize_data_path("file:///tmp/a/f.parquet") == "/tmp/a/f.parquet"
    assert _normalize_data_path("/tmp/a/f.parquet") == "/tmp/a/f.parquet"
    assert _normalize_data_path("gs://bucket/f.parquet") == "gs://bucket/f.parquet"


# ── Local-disk legacy parquet (no legacy Iceberg table) ──────────────────────


def test_adopt_registers_legacy_parquet_and_validates_counts(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-13/file2.parquet", 2)

    res = adopt_iceberg_to_ducklake(src["name"])
    assert res["adopted_files"] == 2
    assert res["skipped_files"] == 0
    assert res["rows_adopted"] == 5
    assert res["source"] == "local_dirs"
    assert _lake_count(src) == 5


def test_adopt_is_idempotent(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)

    first = adopt_iceberg_to_ducklake(src["name"])
    assert first["adopted_files"] == 1
    second = adopt_iceberg_to_ducklake(src["name"])
    assert second["adopted_files"] == 0
    assert second["skipped_files"] == 1
    assert second["rows_adopted"] == 0
    assert _lake_count(src) == 3, "re-running the migration must not duplicate rows"


def test_adopt_picks_up_only_new_files_on_rerun(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)
    assert adopt_iceberg_to_ducklake(src["name"])["rows_adopted"] == 3

    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-14/file3.parquet", 4)
    res = adopt_iceberg_to_ducklake(src["name"])
    assert res["adopted_files"] == 1
    assert res["skipped_files"] == 1
    assert res["rows_adopted"] == 4
    assert _lake_count(src) == 7


def test_adopt_no_files_is_a_noop(migration_source):
    res = adopt_iceberg_to_ducklake(migration_source["name"])
    assert res == {
        "adopted_files": 0,
        "skipped_files": 0,
        "rows_adopted": 0,
        "source": "none",
        "candidate_files": 0,
    }


def test_adopt_unknown_service_raises(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    with pytest.raises(ValueError, match="unknown service"):
        adopt_iceberg_to_ducklake("nope")


def test_migrate_admin_endpoint_registered():
    from backend.main import app

    paths = app.openapi()["paths"]
    assert "/api/admin/ducklake/migrate" in paths
    assert "post" in paths["/api/admin/ducklake/migrate"]
