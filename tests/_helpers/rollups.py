"""Shared rollup writer/reader test helpers."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME, IP_SPREAD_DIR_NAME

ALL_FIELDS = "all_fields.parquet"
DEFAULT_ANCHOR = datetime(2026, 3, 15, 10, 0, 0, tzinfo=UTC)


@contextmanager
def noop_lock(*_a: Any, **_k: Any) -> Iterator[None]:
    """No-op stand-in for per-service context-manager locks in unit tests."""
    yield


def days_ago_iso(days: int) -> str:
    return (datetime.now(UTC).date() - timedelta(days=days)).strftime("%Y-%m-%d")


def yesterday_iso() -> str:
    return days_ago_iso(1)


def two_days_ago_iso() -> str:
    return days_ago_iso(2)


def three_days_ago_iso() -> str:
    return days_ago_iso(3)


def past_hour(hours_ago: int) -> tuple[str, datetime]:
    """``(partition key "YYYY-MM-DD-HH", hour-aligned datetime)`` for ``hours_ago`` hours back."""
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


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


def build_writer_patches(
    cache_root: Any,
    table: str,
    con_factory: Callable[[], Any],
    *,
    safe_table_target: str = "backend.core.rollups._common._safe_table_for",
) -> tuple[Any, ...]:
    """Return the common 5-patch stack for rollup writer tests."""
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(safe_table_target, return_value=table),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con_factory()),
        patch("backend.core.iceberg.view._get_service_lock", noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    )


@contextmanager
def writer_patches(
    cache_root: Any,
    table: str,
    con: Any,
    *,
    safe_table_target: str = "backend.core.rollups._common._safe_table_for",
) -> Iterator[None]:
    """Context manager entering the 5-patch rollup writer stack."""
    with ExitStack() as stack:
        for p in build_writer_patches(cache_root, table, lambda: con, safe_table_target=safe_table_target):
            stack.enter_context(p)
        yield


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


@dataclass
class RollupGoldenLayout:
    """Unified on-disk rollup directory layout helper for golden characterization suites."""

    root: Path
    anchor: datetime = DEFAULT_ANCHOR
    fns: tuple[str, ...] = ()
    hour_root: Path = field(init=False)
    day_root: Path = field(init=False)
    field_root: Path = field(init=False)
    hour: Path = field(init=False)
    day: Path = field(init=False)
    hour_b: Path = field(init=False)
    day_b: Path = field(init=False)
    bundled: Path = field(init=False)
    perfield: Path = field(init=False)

    def __post_init__(self) -> None:
        r = self.root / "rollups"
        self.hour_root = r / "hour_bundled"
        self.day_root = r / "day_bundled"
        self.field_root = r / "hour"
        self.hour = self.field_root
        self.day = r / "day"
        self.hour_b = self.hour_root
        self.day_b = self.day_root
        self.bundled = self.hour_root
        self.perfield = r / IP_SPREAD_DIR_NAME

    def hkey(self, k: int) -> str:
        return (self.anchor + timedelta(hours=k)).strftime("%Y-%m-%d-%H")

    def hk(self, k: int) -> str:
        return self.hkey(k)

    def dkey(self, k: int) -> str:
        return (self.anchor + timedelta(days=k)).strftime("%Y-%m-%d")

    def dk(self, k: int) -> str:
        return (self.anchor.replace(hour=0) + timedelta(days=k)).strftime("%Y-%m-%d")

    @staticmethod
    def _touch(p: Path) -> None:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")

    def mkroot(self) -> None:
        self.hour_root.mkdir(parents=True, exist_ok=True)
        self.day_root.mkdir(parents=True, exist_ok=True)
        self.field_root.mkdir(parents=True, exist_ok=True)

    def hour_files(self, ks: range | list[int], fns: tuple[str, ...] | None = None) -> None:
        for k in ks:
            d = self.hour_root / f"hour={self.hkey(k)}"
            d.mkdir(parents=True, exist_ok=True)
            for fn in fns or self.fns:
                (d / fn).write_bytes(b"x")

    def day_files(self, ks: range | list[int], variant: str = "std") -> None:
        for k in ks:
            day = self.dkey(k)
            for fn in self.fns:
                if variant == "std" or fn != ALL_FIELDS:
                    d = self.day_root / f"day={day}"
                    d.mkdir(parents=True, exist_ok=True)
                    (d / fn).write_bytes(b"x")
                elif variant == "nested_alt":
                    d = self.day_root / f"day={day}"
                    d.mkdir(parents=True, exist_ok=True)
                    (d / f"day_bundle_{day}.parquet").write_bytes(b"x")
                elif variant == "flat_alt":
                    self.day_root.mkdir(parents=True, exist_ok=True)
                    (self.day_root / f"day_bundle_{day}.parquet").write_bytes(b"x")

    def marker(self, k: int) -> None:
        (self.field_root / "field=requests" / f"hour={self.hkey(k)}").mkdir(parents=True, exist_ok=True)

    def sentinel(self, k: int) -> None:
        d = self.hour_root / f"hour={self.hkey(k)}"
        d.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
        pq.write_table(pa.table({"field": [], "value": [], "count": []}, schema=schema), d / ALL_FIELDS)

    def nonempty_allfields(self, k: int) -> None:
        d = self.hour_root / f"hour={self.hkey(k)}"
        d.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
        pq.write_table(pa.table({"field": ["country"], "value": ["US"], "count": [1]}, schema=schema), d / ALL_FIELDS)

    def field_hours(self, fields: list[str], ks: range | list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.hour / f"field={f}" / f"hour={self.hk(k)}" / fname)

    def field_hour_dirs(self, fields: list[str], ks: list[int]) -> None:
        for f in fields:
            for k in ks:
                (self.hour / f"field={f}" / f"hour={self.hk(k)}").mkdir(parents=True, exist_ok=True)

    def field_days(self, fields: list[str], ks: list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.day / f"field={f}" / f"day={self.dk(k)}" / fname)

    def empty_day_dirs(self, fields: list[str], ks: list[int]) -> None:
        for f in fields:
            for k in ks:
                (self.day / f"field={f}" / f"day={self.dk(k)}").mkdir(parents=True, exist_ok=True)

    def bundled_hours(self, ks: range | list[int]) -> None:
        for k in ks:
            self._touch(self.hour_b / f"hour={self.hk(k)}" / ALL_FIELDS)

    def bundled_days(self, ks: list[int], variant: str = "std") -> None:
        for k in ks:
            day = self.dk(k)
            if variant == "std":
                self._touch(self.day_b / f"day={day}" / ALL_FIELDS)
            elif variant == "nested_alt":
                self._touch(self.day_b / f"day={day}" / f"day_bundle_{day}.parquet")
            elif variant == "flat_alt":
                self._touch(self.day_b / f"day_bundle_{day}.parquet")

    def bundles(self, ks: range | list[int], fname: str = IP_SPREAD_BUNDLE_FILENAME) -> None:
        for k in ks:
            self._touch(self.bundled / f"hour={self.hk(k)}" / fname)

    def perfield_hours(self, fields: list[str], ks: range | list[int], fname: str = "part-0.parquet") -> None:
        for f in fields:
            for k in ks:
                self._touch(self.perfield / f"field={f}" / f"hour={self.hk(k)}" / fname)


def build_rollup_golden_layout(
    root: Path,
    builder: Callable[[RollupGoldenLayout], None] | None = None,
    *,
    anchor: datetime = DEFAULT_ANCHOR,
    fns: tuple[str, ...] = (),
) -> RollupGoldenLayout:
    """Create a ``RollupGoldenLayout`` and optionally apply ``builder`` to populate it."""
    lay = RollupGoldenLayout(root=root, anchor=anchor, fns=fns)
    if builder is not None:
        builder(lay)
    return lay
