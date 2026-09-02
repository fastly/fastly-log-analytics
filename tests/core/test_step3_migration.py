"""Integration tests for adopt_iceberg_to_ducklake — real parquet fixtures,
real DuckDB, real DuckLake catalog (replaces the earlier mock-only test that
asserted on MagicMock call strings and could not catch a broken migration)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.duckdb import get_connection
from backend.core.iceberg._ducklake import ducklake_table_name
from backend.core.iceberg._ducklake_migration import adopt_iceberg_to_ducklake


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


def _write_legacy_parquet(source: dict, rel_path: str, n: int) -> str:
    """Write a legacy hive-partition parquet the way the old sync path did."""
    base = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    path = os.path.join(source["_cache_dir_override"], "data", rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [base + timedelta(seconds=i) for i in range(n)], type=pa.timestamp("us", tz="UTC")
                ),
                "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
                "status": pa.array([200] * n),
            }
        ),
        path,
    )
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


def test_adopt_registers_legacy_parquet_and_validates_counts(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-13/file2.parquet", 2)

    res = adopt_iceberg_to_ducklake(src["name"])
    assert res == {"adopted_files": 2, "skipped_files": 0, "rows_adopted": 5}
    assert _lake_count(src) == 5


def test_adopt_is_idempotent(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)

    first = adopt_iceberg_to_ducklake(src["name"])
    assert first["adopted_files"] == 1
    second = adopt_iceberg_to_ducklake(src["name"])
    assert second == {"adopted_files": 0, "skipped_files": 1, "rows_adopted": 0}
    assert _lake_count(src) == 3, "re-running the migration must not duplicate rows"


def test_adopt_picks_up_only_new_files_on_rerun(migration_source):
    src = migration_source
    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-12/file1.parquet", 3)
    assert adopt_iceberg_to_ducklake(src["name"])["rows_adopted"] == 3

    _write_legacy_parquet(src, "timestamp_hour=2026-08-30-14/file3.parquet", 4)
    res = adopt_iceberg_to_ducklake(src["name"])
    assert res == {"adopted_files": 1, "skipped_files": 1, "rows_adopted": 4}
    assert _lake_count(src) == 7


def test_adopt_no_files_is_a_noop(migration_source):
    res = adopt_iceberg_to_ducklake(migration_source["name"])
    assert res == {"adopted_files": 0, "skipped_files": 0, "rows_adopted": 0}


def test_adopt_unknown_service_raises(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    with pytest.raises(ValueError, match="unknown service"):
        adopt_iceberg_to_ducklake("nope")


def test_migrate_admin_endpoint_registered():
    from backend.main import app

    paths = app.openapi()["paths"]
    assert "/api/admin/ducklake/migrate" in paths
    assert "post" in paths["/api/admin/ducklake/migrate"]
