"""Shared rollup writer/reader test helpers."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from tests._helpers.locks import noop_lock


def write_hour_all_fields(cache_root: str, hour: str) -> None:
    """Write a sentinel ``all_fields.parquet`` so the backfill walk treats ``hour`` as touched."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "field": pa.array(["x"]),
                "value": pa.array(["y"]),
                "count": pa.array([1], type=pa.int64()),
            }
        ),
        os.path.join(d, "all_fields.parquet"),
    )


def build_writer_patches(cache_root: Any, table: str, con_factory: Callable[[], Any]) -> tuple[Any, ...]:
    """Return the common 5-patch stack for rollup writer tests."""
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con_factory()),
        patch("backend.core.iceberg.view._get_service_lock", noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    )


def write_hour_bundle(hour_dir: str, filename: str, select_sql_values: str, schema_cols: str) -> None:
    """Write a Parquet bundle file from a SQL ``VALUES`` clause."""
    os.makedirs(hour_dir, exist_ok=True)
    wcon = duckdb.connect()
    try:
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {select_sql_values}) AS t({schema_cols})) "
            f"TO '{hour_dir}/{filename}' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def make_stub_runner(
    src: dict[str, Any],
    captured_sql: list[str],
    stub_rows: list[tuple[Any, ...]],
    *,
    table: str = "logs_svc_reader",
) -> Any:
    """Build a ``QueryRunner`` with ``execute()`` stubbed to capture SQL and return canned rows."""
    from backend.repositories._base import QueryRunner

    class _Result:
        def fetchall(self) -> list[tuple[Any, ...]]:
            return stub_rows

        def fetchone(self) -> tuple[Any, ...] | None:
            return stub_rows[0] if stub_rows else None

    class _Conn:
        def execute(self, sql: str, params: Any = None) -> _Result:
            captured_sql.append(sql)
            return _Result()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner._table = table
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner
