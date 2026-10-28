"""Tests for the per-tick recompute / one-shot backfill / retention
cleanup drivers in ``backend.core.rollups.recompute``.

The shared ``_run_per_field_copy`` core is exercised end-to-end against
a real :memory: DuckDB so the COPY+PARTITION_BY+publish-under-lock
sequence + per-field skip rules are all covered.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow.parquet as pq
import pytest
from tests._helpers.rollups import noop_lock as _noop_lock
from tests._helpers.rollups import past_hour as _past_hour
from tests._helpers.rollups import writer_patches

import backend.core.rollups.day_bundles as hour_bundles
import backend.core.rollups.origin_dims as network_health_geo
import backend.core.rollups.origin_dims as network_health_heatmap
import backend.core.rollups.origin_dims as network_quality
import backend.core.rollups.origin_dims as network_rtt
import backend.core.rollups.origin_dims as network_speed
import backend.core.rollups.origin_dims as network_summary
import backend.core.rollups.origin_dims as origin_latency_ts
import backend.core.rollups.origin_dims as origin_summary
import backend.core.rollups.origin_dims as perf_dims
import backend.core.rollups.origin_dims as perf_latency
import backend.core.rollups.origin_dims as pop_health
import backend.core.rollups.origin_dims as slow_urls
import backend.core.rollups.recompute as overview
import backend.core.rollups.recompute as sessions
import backend.core.rollups.recompute as time_series
import backend.core.rollups.security_dims as ngwaf_bots
import backend.core.rollups.security_dims as verified_bots_ts
import backend.core.rollups.security_dims as wellknown_bots
import backend.core.storage._core as _dl
from backend.core import rollups
from backend.core.rollups import day_bundles, origin_dims, security_dims
from backend.core.rollups.recompute import backfill_missing_rum_aggregates, recompute_rum_aggregates, table_exists
from backend.cron.jobs import compaction
from backend.repositories import rum as rum_repo


def _make_table_with_rows(table: str, hour_dt: datetime, rows: list[tuple[str, str]]) -> duckdb.DuckDBPyConnection:
    """Create ``table`` with (timestamp, ip, country) and INSERT rows."""
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, ip VARCHAR, country VARCHAR)")
    for ip, country in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?)",
            [hour_dt + timedelta(minutes=5), ip, country],
        )
    return con


# ── _run_per_field_copy ────────────────────────────────────────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_writes_partitioned_parquet(tmp_path):
    """Happy path: a single field with rows in a closed hour produces a
    per-(field, hour) parquet under rollups/hour/field=ip/hour=H/."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pc"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_pc"

    con = _make_table_with_rows(table, hour_dt, [("1.1.1.1", "US"), ("2.2.2.2", "JP")])
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_per_field_copy("svc-pc", src, table, where_sql, ["ip"])

    # Field tmp dir cleaned up.
    assert not (cache_root / "rollups" / "tmp" / "ip").exists()
    # Per-field parquet published.
    field_dir = cache_root / "rollups" / "hour" / "field=ip"
    assert field_dir.exists()
    hour_dirs = list(field_dir.glob("hour=*"))
    assert len(hour_dirs) == 1
    parquets = list(hour_dirs[0].glob("compacted_*.parquet"))
    assert len(parquets) == 1


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_skips_unsafe_field_name(tmp_path):
    """A field name failing _is_safe_ident must be skipped — defense in
    depth against bypassing _get_fields."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-unsafe"}
    _, hour_dt = _past_hour(2)
    con = _make_table_with_rows("logs_x", hour_dt, [("1.1.1.1", "US")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        # ``"select"`` is alphanumeric so _is_safe_ident passes; we need
        # a value that actually fails the regex.
        recompute._run_per_field_copy("svc-unsafe", src, "logs_x", "1=1", ["bad-field-name!"])

    assert not (cache_root / "rollups" / "hour").exists() or not any((cache_root / "rollups" / "hour").iterdir())


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_skips_field_missing_from_schema(tmp_path):
    """A field name absent from the table's column set is skipped
    (no COPY emitted)."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-missing"}
    _, hour_dt = _past_hour(2)
    con = _make_table_with_rows("logs_x", hour_dt, [("1.1.1.1", "US")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        # `nonexistent` is not in the schema → skipped silently.
        recompute._run_per_field_copy("svc-missing", src, "logs_x", "1=1", ["nonexistent"])

    field_dir = cache_root / "rollups" / "hour" / "field=nonexistent"
    assert not field_dir.exists()


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_skips_virtual_field_with_missing_backing(tmp_path):
    """Virtual fields are gated on their BACKING column. If the backing
    column isn't on the schema, the virtual field is skipped."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    _, hour_dt = _past_hour(2)
    # Table has neither waf_sig (the backing) nor waf_sig_ind (the virtual).
    con = _make_table_with_rows("logs_x", hour_dt, [("1.1.1.1", "US")])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_per_field_copy("svc", {"name": "svc"}, "logs_x", "1=1", ["waf_sig_ind"])

    assert not (cache_root / "rollups" / "hour" / "field=waf_sig_ind").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_describe_failure_returns(tmp_path):
    """DESCRIBE blowing up returns cleanly without writing anything."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    con = duckdb.connect(":memory:")

    def _boom(_c, _src, _fn):
        raise duckdb.Error("synthetic")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=_boom),
    ):
        # Should NOT raise — function logs and returns.
        recompute._run_per_field_copy("svc", {"name": "svc"}, "logs_x", "1=1", ["ip"])

    assert not (cache_root / "rollups" / "hour").exists() or not any((cache_root / "rollups" / "hour").iterdir())


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_copy_failure_cleans_tmp_and_continues(tmp_path):
    """If COPY raises duckdb.Error for one field, its tmp dir is cleaned
    and the next field still runs."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-mixed"}
    _, hour_dt = _past_hour(2)
    real_con = _make_table_with_rows("logs_x", hour_dt, [("1.1.1.1", "US")])

    class _Proxy:
        """Delegating wrapper so we can override .execute (DuckDB's own
        attribute is read-only and resists patch.object)."""

        def __init__(self, con):
            self._con = con
            self._calls = 0

        def execute(self, sql, *args, **kwargs):
            self._calls += 1
            # The DESCRIBE is the first execute call; let it through. The
            # next COPY for field='ip' raises; subsequent COPY for
            # 'country' goes through.
            if self._calls >= 2 and "COPY" in sql and "'ip'" in sql:
                raise duckdb.Error("simulated ip COPY failure")
            return self._con.execute(sql, *args, **kwargs)

        def close(self):
            self._con.close()

    proxy = _Proxy(real_con)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=proxy),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_per_field_copy("svc-mixed", src, "logs_x", "1=1", ["ip", "country"])

    # ip tmp dir cleaned up.
    assert not (cache_root / "rollups" / "tmp" / "ip").exists()
    # country published successfully.
    assert any((cache_root / "rollups" / "hour" / "field=country").glob("hour=*/compacted_*.parquet"))


@pytest.mark.usefixtures("frozen_clock")
def test_run_per_field_copy_self_heals_stale_view_on_copy(tmp_path):
    """A transient DuckLake ``schema "lake" does not exist`` (Trap #35) on
    the per-field COPY must self-heal via ``execute_with_stale_view_retry``
    and still publish the field — not warn + skip it. Mirrors the
    ip_spread SELECT self-heal so a cold-start lake detach race (every
    field's COPY failing in one burst when a concurrent DETACH rips
    ``lake`` out mid-loop) doesn't drop the rollup or flood the deploy
    log-error gate with Catalog Errors."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-copy-heal"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_copy_heal"
    real = _make_table_with_rows(table, hour_dt, [("1.1.1.1", "US"), ("2.2.2.2", "JP")])
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )

    class _FlakyCopyCon:
        """Delegates to a real con but fails the FIRST COPY with a
        lake-detach Catalog Error, succeeding on retry."""

        def __init__(self, inner):
            self._inner = inner
            self._failed_once = False

        def execute(self, sql, *a, **k):
            if "COPY" in sql and not self._failed_once:
                self._failed_once = True
                raise duckdb.CatalogException(
                    'Catalog Error: Table with name "lake.logs_svc_copy_heal" '
                    'does not exist because schema "lake" does not exist.'
                )
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    con = _FlakyCopyCon(real)

    def _retry_double(c, _src, fn):
        from backend.core.storage import is_stale_view_error

        try:
            return fn(c)
        except Exception as e:  # noqa: BLE001
            if not is_stale_view_error(e):
                raise
            return fn(c)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=_retry_double,
        ),
    ):
        recompute._run_per_field_copy("svc-copy-heal", src, table, where_sql, ["ip"])

    # The field published despite the first COPY's transient lake error.
    field_dir = cache_root / "rollups" / "hour" / "field=ip"
    assert field_dir.exists()
    assert list(field_dir.glob("hour=*/compacted_*.parquet"))


# ── _run_ip_spread_per_field ────────────────────────────────────────────────


def _read_ip_spread_rows(field_dir):
    """Helper: read every published parquet under
    ``rollups/hour_ip_spread/field=X/hour=Y/*.parquet`` into a list of
    dicts. Used by the ip_spread tests so they can inspect the actual
    parquet output (sketch bytes, observed counts, capped flags)."""
    import pyarrow.parquet as pq

    rows = []
    for parquet_path in sorted(field_dir.glob("hour=*/compacted_*.parquet")):
        tbl = pq.read_table(str(parquet_path))
        for row in tbl.to_pylist():
            rows.append(row)
    return rows


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_writes_partitioned_parquet_with_hll(tmp_path):
    """Happy path: a single field with distinct IPs in a closed hour
    produces a per-(field, hour) parquet under
    ``rollups/hour_ip_spread/field=X/hour=Y/`` with an HLL BLOB whose
    deserialized count is within HLL error of the input's true distinct."""
    from backend.core.rollups import recompute
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_ipsp"

    # Five distinct IPs against value="JP", three against value="US".
    rows = [
        ("1.1.1.1", "JP"),
        ("2.2.2.2", "JP"),
        ("3.3.3.3", "JP"),
        ("4.4.4.4", "JP"),
        ("5.5.5.5", "JP"),
        ("6.6.6.6", "US"),
        ("7.7.7.7", "US"),
        ("8.8.8.8", "US"),
    ]
    con = _make_table_with_rows(table, hour_dt, rows)
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-ipsp", src, table, where_sql, ["country"])

    # tmp dir cleaned up
    assert not (cache_root / "rollups" / "tmp_ip" / "country").exists()
    # parquet published to the ip_spread tree
    field_dir = cache_root / "rollups" / "hour_ip_spread" / "field=country"
    assert field_dir.exists()
    hour_dirs = list(field_dir.glob("hour=*"))
    assert len(hour_dirs) == 1

    published = _read_ip_spread_rows(field_dir)
    by_value = {r["value"]: r for r in published}
    assert set(by_value) == {"JP", "US"}

    # Round-trip: HLL bytes deserialize to an estimate close to true distinct.
    jp_hll = HyperLogLog.from_bytes(by_value["JP"]["ip_sketch"])
    us_hll = HyperLogLog.from_bytes(by_value["US"]["ip_sketch"])
    # Tolerance is generous: HLL has ~6.5% error at p=8, but tiny
    # cardinalities exercise the linear-counting regime where a 5-IP
    # input often estimates to exactly 5, sometimes to 4 or 6. Pin the
    # documented `ip_count_observed` column at the exact value instead.
    assert by_value["JP"]["ip_count_observed"] == 5
    assert by_value["US"]["ip_count_observed"] == 3
    assert by_value["JP"]["sample_capped"] is False
    assert by_value["US"]["sample_capped"] is False
    # HLL estimate sanity-bound (small-range regime, exact or near-exact).
    assert abs(jp_hll.count() - 5) <= 2
    assert abs(us_hll.count() - 3) <= 2


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_silent_when_no_ip_column(tmp_path):
    """When the schema doesn't carry an ``ip`` column there's no IP
    spread to build — the writer must silently return without raising
    or creating any files in the ip_spread tree. Pinned because the
    security FE already handles 'no ip column' as the empty-card
    case, and a stray error here would surface as a cron warning the
    operator can't act on."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-noip"}
    _, hour_dt = _past_hour(2)

    # Table WITHOUT ip column.
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc_noip (timestamp TIMESTAMPTZ, country VARCHAR)")
    con.execute("INSERT INTO logs_svc_noip VALUES (?, ?)", [hour_dt + timedelta(minutes=5), "JP"])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-noip", src, "logs_svc_noip", "1=1", ["country"])

    assert not (cache_root / "rollups" / "hour_ip_spread").exists() or not any(
        (cache_root / "rollups" / "hour_ip_spread").rglob("*.parquet")
    )


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_skips_virtual_fields(tmp_path):
    """Virtual (CSV-unnest) fields aren't a useful target for IP-spread —
    the unnested signals don't have a 1:1 IP relationship the way raw
    fingerprint columns do. Pinned so a future expansion of the
    virtual-field map can't accidentally start building noisy
    ip_spread parquets for the unnest columns."""
    from backend.core.rollups import recompute
    from backend.core.rollups._common import _VIRTUAL_FIELD_BACKING

    # Confirm the field we're using is registered as virtual.
    assert "waf_sig_ind" in _VIRTUAL_FIELD_BACKING

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-virt"}
    _, hour_dt = _past_hour(2)

    # Table HAS ip + the BACKING column for the virtual field.
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc_virt (timestamp TIMESTAMPTZ, ip VARCHAR, waf_sig VARCHAR)")
    con.execute(
        "INSERT INTO logs_svc_virt VALUES (?, ?, ?)",
        [hour_dt + timedelta(minutes=5), "1.1.1.1", "sig-a,sig-b"],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-virt", src, "logs_svc_virt", "1=1", ["waf_sig_ind"])

    # No parquet should land for the virtual field.
    assert not (cache_root / "rollups" / "hour_ip_spread" / "field=waf_sig_ind").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_marks_capped_when_sample_hits_cap(tmp_path, monkeypatch):
    """When the per-(field, hour, value) distinct IP count meets or
    exceeds IP_SAMPLE_CAP, the ``sample_capped`` flag MUST be True so
    the reader / FE can label the resulting count as approximate.

    Patches the cap down to a small value so the test stays fast — the
    writer's cap behaviour doesn't depend on the absolute value, only
    on whether the observed count meets it."""
    from backend.core.rollups import _common as common_mod
    from backend.core.rollups import recompute

    # Drop the cap so we don't need to materialize 5000 IPs to trigger it.
    monkeypatch.setattr(common_mod, "IP_SAMPLE_CAP", 3, raising=True)
    # Also patch the constant where recompute imports it (recompute pulls
    # IP_SAMPLE_CAP at module import time into its own namespace).
    monkeypatch.setattr(recompute, "IP_SAMPLE_CAP", 3, raising=True)

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-cap"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_cap"

    # 5 distinct IPs for "JP" exceeds cap=3; "US" with 2 IPs stays under.
    rows = [
        ("1.1.1.1", "JP"),
        ("2.2.2.2", "JP"),
        ("3.3.3.3", "JP"),
        ("4.4.4.4", "JP"),
        ("5.5.5.5", "JP"),
        ("9.9.9.9", "US"),
        ("8.8.8.8", "US"),
    ]
    con = _make_table_with_rows(table, hour_dt, rows)
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-cap", src, table, where_sql, ["country"])

    field_dir = cache_root / "rollups" / "hour_ip_spread" / "field=country"
    published = _read_ip_spread_rows(field_dir)
    by_value = {r["value"]: r for r in published}
    assert by_value["JP"]["sample_capped"] is True
    assert by_value["US"]["sample_capped"] is False


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_empty_result_writes_nothing(tmp_path):
    """An hour with rows but ``ip IS NOT NULL`` filtering them all out
    produces zero output rows — the writer must skip writing rather
    than producing an empty parquet that downstream readers would
    have to special-case."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-empty"}
    _, hour_dt = _past_hour(2)

    # All-NULL ip column.
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ip VARCHAR, country VARCHAR)")
    con.execute("INSERT INTO logs_x VALUES (?, ?, ?)", [hour_dt + timedelta(minutes=5), None, "JP"])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-empty", src, "logs_x", "1=1", ["country"])

    # No parquet should land.
    assert not (cache_root / "rollups" / "hour_ip_spread" / "field=country").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_run_ip_spread_per_field_self_heals_stale_view_on_select(tmp_path):
    """A transient DuckLake ``schema "lake" does not exist`` (Trap #35)
    on the per-field ip_spread SELECT must self-heal via
    ``execute_with_stale_view_retry`` and still publish the field —
    not warn + skip it. Mirrors the describe-path self-heal so a
    cold-start lake detach race doesn't drop the rollup (and doesn't
    flood the deploy log-error gate with Catalog Errors)."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-heal"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_heal"
    rows = [("1.1.1.1", "JP"), ("2.2.2.2", "JP"), ("3.3.3.3", "US")]
    real = _make_table_with_rows(table, hour_dt, rows)
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )

    class _FlakySelectCon:
        """Delegates to a real con but fails the FIRST ip_spread SELECT
        with a lake-detach Catalog Error, succeeding on retry."""

        def __init__(self, inner):
            self._inner = inner
            self._failed_once = False

        def execute(self, sql, *a, **k):
            if "ip_sample" in sql and not self._failed_once:
                self._failed_once = True
                raise duckdb.CatalogException(
                    'Catalog Error: Table with name "lake.logs_svc_heal" does '
                    'not exist because schema "lake" does not exist.'
                )
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    con = _FlakySelectCon(real)

    def _retry_double(c, _src, fn):
        from backend.core.storage import is_stale_view_error

        try:
            return fn(c)
        except Exception as e:  # noqa: BLE001
            if not is_stale_view_error(e):
                raise
            return fn(c)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=_retry_double,
        ),
    ):
        recompute._run_ip_spread_per_field("svc-heal", src, table, where_sql, ["country"])

    # The field published despite the first SELECT's transient lake error.
    field_dir = cache_root / "rollups" / "hour_ip_spread" / "field=country"
    assert field_dir.exists()
    assert list(field_dir.glob("hour=*"))


# ── recompute_touched_hours ─────────────────────────────────────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_no_hours_returns_immediately():
    from backend.core.rollups import recompute

    # Should not raise; nothing to do.
    recompute.recompute_touched_hours("svc", {"name": "svc"}, set())


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_skips_active_hour():
    """Active UTC hour must be filtered out before doing any work."""
    from backend.core.rollups import recompute

    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    called = {"n": 0}

    def _fail(*a, **kw):
        called["n"] += 1

    with (
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._run_per_field_copy", side_effect=_fail),
    ):
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {active})

    # Active hour filtered → no work passed downstream.
    assert called["n"] == 0


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_malformed_hour_skipped(tmp_path):
    """Bad hour tokens are logged + dropped; only good hours proceed."""
    from contextlib import ExitStack

    from backend.core.rollups import recompute

    h_good, _ = _past_hour(2)
    captured: list = []

    def _capture(_sid, _src, _table, where_sql, _fields):
        captured.append(where_sql)

    with ExitStack() as stack:
        stack.enter_context(patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"))
        stack.enter_context(patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]))
        stack.enter_context(patch("backend.core.rollups.recompute._run_per_field_copy", side_effect=_capture))
        stack.enter_context(patch("backend.core.rollups.recompute._run_ip_spread_per_field"))
        stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.bundle_hours_ip_spread", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_ngwaf_bots_ts_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.recompute_wellknown_bots_rollup", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_time_series_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_session_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_slow_urls_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_origin_summary_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_network_rtt_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_network_speed_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_verified_bots_ts_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_perf_latency_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_origin_dims_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_origin_latency_ts_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_security_dims_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_security_proxies_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_perf_dims_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_network_heatmap_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_network_geo_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_ngwaf_bots_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_overview_bundles", return_value=0))
        stack.enter_context(patch("backend.core.rollups.recompute.build_network_summary_bundles", return_value=0))

        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h_good, "not-an-hour"})

    assert len(captured) == 1
    # The good hour token must appear in the WHERE clause.
    assert h_good in captured[0]


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_no_safe_table_returns():
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    with (
        patch("backend.core.rollups.recompute._safe_table_for", return_value=None),
        patch(
            "backend.core.rollups.recompute._run_per_field_copy",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_all_active_returns():
    """If every input hour is the active hour, parsed list is empty → return."""
    from backend.core.rollups import recompute

    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    with (
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch(
            "backend.core.rollups.recompute._run_per_field_copy",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {active})


@pytest.mark.usefixtures("frozen_clock")
def test_recompute_touched_hours_swallows_downstream_bundle_errors():
    """Bundle/time_series/sessions failures must NOT propagate — they're
    best-effort optimisations, the per-field rebuild already succeeded."""
    from contextlib import ExitStack

    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    with ExitStack() as stack:
        stack.enter_context(patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"))
        stack.enter_context(patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]))
        stack.enter_context(patch("backend.core.rollups.recompute._run_per_field_copy"))
        # The recompute now also fires _run_ip_spread_per_field for any
        # field whose IP-spread companion rollup exists; stub it so the
        # test doesn't try to acquire a real DuckDB connection.
        stack.enter_context(patch("backend.core.rollups.recompute._run_ip_spread_per_field"))
        stack.enter_context(
            patch("backend.core.rollups.recompute.bundle_hours", side_effect=RuntimeError("bundle-boom"))
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_time_series_bundles",
                side_effect=RuntimeError("ts-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_session_bundles",
                side_effect=RuntimeError("sess-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_slow_urls_bundles",
                side_effect=RuntimeError("su-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_origin_summary_bundles",
                side_effect=RuntimeError("os-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_network_rtt_bundles",
                side_effect=RuntimeError("nr-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_network_speed_bundles",
                side_effect=RuntimeError("ns-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_verified_bots_ts_bundles",
                side_effect=RuntimeError("vbts-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_perf_latency_bundles",
                side_effect=RuntimeError("pl-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_origin_dims_bundles",
                side_effect=RuntimeError("od-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_origin_latency_ts_bundles",
                side_effect=RuntimeError("olts-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_security_dims_bundles",
                side_effect=RuntimeError("sd-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_perf_dims_bundles",
                side_effect=RuntimeError("pd-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_network_heatmap_bundles",
                side_effect=RuntimeError("nhm-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_network_geo_bundles",
                side_effect=RuntimeError("geo-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_ngwaf_bots_bundles",
                side_effect=RuntimeError("ngwaf-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_overview_bundles",
                side_effect=RuntimeError("ov-boom"),
            )
        )
        stack.enter_context(
            patch(
                "backend.core.rollups.recompute.build_network_summary_bundles",
                side_effect=RuntimeError("net-sum-boom"),
            )
        )

        additional_writers = [
            stack.enter_context(patch.object(recompute, name, side_effect=RuntimeError(f"{name}-boom")))
            for name in (
                "bundle_hours_ip_spread",
                "build_security_proxies_bundles",
                "build_ngwaf_bots_ts_bundles",
                "recompute_wellknown_bots_rollup",
            )
        ]
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})
        for writer in additional_writers:
            writer.assert_called_once()


# ── cleanup_old_rollups ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_zero_max_age_disables_cleanup(tmp_path):
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    # Add an old hour dir — it must NOT be deleted when max_age=0.
    old = cache_root / "rollups" / "hour" / "field=ip" / "hour=2020-01-01-00"
    old.mkdir(parents=True)
    (old / "compacted_x.parquet").write_bytes(b"x")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, 0) == 0

    assert old.exists()


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_negative_max_age_disabled(tmp_path):
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, -5) == 0


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_deletes_hours_below_cutoff(tmp_path):
    """Hours strictly older than (now - max_age_days) get deleted; newer
    hours are kept."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()

    now = datetime.now(UTC)
    # 30-day-old hour → must be deleted with max_age=7
    old = (now - timedelta(days=30)).strftime("%Y-%m-%d-%H")
    # 1-day-old hour → must be kept
    young = (now - timedelta(days=1)).strftime("%Y-%m-%d-%H")

    for h in (old, young):
        d = cache_root / "rollups" / "hour" / "field=ip" / f"hour={h}"
        d.mkdir(parents=True)
        (d / f"compacted_{uuid.uuid4().hex[:8]}.parquet").write_bytes(b"x")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        deleted = recompute.cleanup_old_rollups("svc", {"name": "svc"}, max_age_days=7)

    assert deleted == 1
    assert not (cache_root / "rollups" / "hour" / "field=ip" / f"hour={old}").exists()
    assert (cache_root / "rollups" / "hour" / "field=ip" / f"hour={young}").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_sweeps_all_trees(tmp_path):
    """Verifies that cleanup_old_rollups sweeps ip_spread, day, day_bundled,
    and hour_bundled trees."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()

    now = datetime.now(UTC)
    old = (now - timedelta(days=30)).strftime("%Y-%m-%d-%H")
    old_day = old[:10]
    young = (now - timedelta(days=1)).strftime("%Y-%m-%d-%H")
    young_day = young[:10]

    # Create old and young entries across trees
    (cache_root / "rollups" / "hour" / "field=ip" / f"hour={old}").mkdir(parents=True)
    (cache_root / "rollups" / "hour" / "field=ip" / f"hour={young}").mkdir(parents=True)

    (cache_root / "rollups" / "hour_ip_spread" / "field=ip" / f"hour={old}").mkdir(parents=True)
    (cache_root / "rollups" / "hour_ip_spread" / "field=ip" / f"hour={young}").mkdir(parents=True)

    (cache_root / "rollups" / "day" / "field=ip" / f"day={old_day}").mkdir(parents=True)
    (cache_root / "rollups" / "day" / "field=ip" / f"day={young_day}").mkdir(parents=True)

    old_db = cache_root / "rollups" / "day_bundled" / f"day={old_day}"
    old_db.mkdir(parents=True)
    (old_db / "all_fields.parquet").write_bytes(b"x")

    young_db = cache_root / "rollups" / "day_bundled" / f"day={young_day}"
    young_db.mkdir(parents=True)
    (young_db / "all_fields.parquet").write_bytes(b"x")

    old_hb = cache_root / "rollups" / "hour_bundled" / f"hour={old}"
    old_hb.mkdir(parents=True)
    (old_hb / "all_fields.parquet").write_bytes(b"x")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        deleted = recompute.cleanup_old_rollups("svc", {"name": "svc"}, max_age_days=7, hour_bundle_max_age_days=7)

    # 1 from hour, 1 from hour_ip_spread, 1 from day_bundled, 1 from day, 1 from hour_bundled
    assert deleted >= 5
    assert not (cache_root / "rollups" / "hour" / "field=ip" / f"hour={old}").exists()
    assert not (cache_root / "rollups" / "hour_ip_spread" / "field=ip" / f"hour={old}").exists()
    assert not (cache_root / "rollups" / "day" / "field=ip" / f"day={old_day}").exists()
    assert not (cache_root / "rollups" / "day_bundled" / f"day={old_day}").exists()
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={old}").exists()

    # Young preserved
    assert (cache_root / "rollups" / "hour" / "field=ip" / f"hour={young}").exists()
    assert (cache_root / "rollups" / "hour_ip_spread" / "field=ip" / f"hour={young}").exists()
    assert (cache_root / "rollups" / "day_bundled" / f"day={young_day}").exists()


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_no_root_returns_zero(tmp_path):
    from backend.core.rollups import recompute

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, 7) == 0


@pytest.mark.usefixtures("frozen_clock")
def test_cleanup_old_rollups_skips_non_field_entries(tmp_path):
    """Top-level entries that don't start with ``field=`` are ignored
    (so stray files in rollups/hour/ don't crash the walker)."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    rollup_root = cache_root / "rollups" / "hour"
    rollup_root.mkdir(parents=True)
    (rollup_root / "stray.tmp").write_bytes(b"x")
    (rollup_root / "README").write_bytes(b"x")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, 7) == 0

    # Strays preserved.
    assert (rollup_root / "stray.tmp").exists()


# ── _publish_field_partitions ───────────────────────────────────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_publish_field_partitions_overwrites_stale(tmp_path):
    """New per-hour parquets replace old ones in the destination — the
    rename-then-unlink order keeps a concurrent reader from seeing an
    empty dir mid-publish."""
    from backend.core.rollups._common import _publish_field_partitions

    src_root = tmp_path / "tmp_field"
    field = "ip"
    field_dir = src_root / f"field={field}"
    hour_dir = field_dir / "hour=2026-05-15-10"
    hour_dir.mkdir(parents=True)
    (hour_dir / "part-0.parquet").write_bytes(b"new")

    dst_root = tmp_path / "rollups_hour"
    dst_hour_dir = dst_root / f"field={field}" / "hour=2026-05-15-10"
    dst_hour_dir.mkdir(parents=True)
    (dst_hour_dir / "compacted_old.parquet").write_bytes(b"old")

    published = _publish_field_partitions(str(src_root), str(dst_root), field)

    assert published == 1
    # Old file gone, new file present (under compacted_ naming).
    contents = list(dst_hour_dir.glob("*.parquet"))
    assert len(contents) == 1
    assert all("compacted_" in p.name for p in contents)


@pytest.mark.usefixtures("frozen_clock")
def test_publish_field_partitions_no_field_dir_returns_zero(tmp_path):
    """Missing src field dir → nothing to publish."""
    from backend.core.rollups._common import _publish_field_partitions

    assert _publish_field_partitions(str(tmp_path / "missing"), str(tmp_path), "ip") == 0


# ── _get_fields ─────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_get_fields_includes_safe_custom_fields_skips_unsafe():
    """Custom field names that fail the safe-ident regex are skipped
    (logged) instead of crashing or sneaking into SQL."""
    from backend.core.rollups._common import _get_fields

    src = {
        "log_fields": {
            "custom_fields": [
                {"name": "safe_cf", "enabled": True, "show_in_dashboard": True},
                {"name": "bad-with-dash", "enabled": True, "show_in_dashboard": True},
                {"name": "disabled_cf", "enabled": False, "show_in_dashboard": True},
                {"name": "hidden_cf", "enabled": True, "show_in_dashboard": False},
            ]
        }
    }
    fields = _get_fields(src)
    assert "safe_cf" in fields
    assert "bad-with-dash" not in fields
    assert "disabled_cf" not in fields
    assert "hidden_cf" not in fields


@pytest.mark.usefixtures("frozen_clock")
def test_get_fields_excludes_metrics_only_synthetic_fields():
    """Regression test for a live production incident: METRICS-group
    catalog entries (requests, hit_rate, p50_latency, ...) have vcl=None —
    a chart-metric-only concept, never a real per-row column — so including
    them here means the per-field rollup writer tries to SELECT a column
    that doesn't exist in the raw/buffer parquet and errors."""
    from backend.core.rollups._common import _get_fields

    fields = _get_fields({"log_fields": {"custom_fields": []}})

    assert "requests" not in fields
    assert "hit_rate" not in fields
    assert "p95_latency" not in fields
    assert "country" in fields


# ── _build_copy_query and _build_copy_ip_query NULL filters ─────────────────


@pytest.mark.usefixtures("frozen_clock")
def test_build_copy_query_filters_out_nulls_and_empty():
    """Assert that _build_copy_query generates SQL that explicitly filters out
    NULL and empty string values for the rollup field to prevent populating
    the rollups cache with natural nulls of optional fields."""
    from backend.core.rollups._common import _build_copy_query

    sql = _build_copy_query("logs_svc", "cookie_session", "timestamp >= '2026-08-20T00:00:00'")

    # Check that we have our new IS NOT NULL and NULLIF filters on cookie_session
    assert 'AND "cookie_session" IS NOT NULL' in sql
    assert "AND NULLIF(CAST(\"cookie_session\" AS VARCHAR), '') IS NOT NULL" in sql


@pytest.mark.usefixtures("frozen_clock")
def test_build_copy_ip_query_filters_out_nulls_and_empty():
    """Assert that _build_ip_spread_select_query generates SQL that explicitly filters out
    NULL and empty string values for the rollup field, in addition to the standard ip filters."""
    from backend.core.rollups._common import _build_ip_spread_select_query

    sql = _build_ip_spread_select_query("logs_svc", "ja3", "timestamp >= '2026-08-20T00:00:00'")

    assert 'AND "ip" IS NOT NULL' in sql
    assert 'AND "ja3" IS NOT NULL' in sql
    assert "AND NULLIF(CAST(\"ja3\" AS VARCHAR), '') IS NOT NULL" in sql


# ── Merged from tests/core/test_rollups_ts_and_sessions.py ──


# ── Merged from tests/core/test_rollups_sessions.py & test_rollups_time_series.py ──
def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    cols_sql = (
        "timestamp TIMESTAMPTZ, ip VARCHAR, ja4 VARCHAR, country VARCHAR, "
        "asn INTEGER, status INTEGER, resp_bytes BIGINT, tcp_rtt DOUBLE, "
        "edge INTEGER, ua VARCHAR, edge_sid VARCHAR"
    )
    con.execute(f"CREATE TABLE {table} ({cols_sql})")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ip"),
                r.get("ja4"),
                r.get("country"),
                r.get("asn"),
                r.get("status"),
                r.get("resp_bytes"),
                r.get("tcp_rtt"),
                r.get("edge"),
                r.get("ua"),
                r.get("edge_sid"),
            ],
        )


def _seed_logs_rollups_time_series(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("status"), r.get("cache"), r.get("resp_bytes"), r.get("ttfb")],
        )


@pytest.mark.usefixtures("frozen_clock")
def test_build_sessions_writes_aggregates_for_closed_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sess", "service_id": "svc-sess"}
    hour_token, hour_dt = _past_hour(2)
    con = duckdb.connect(":memory:")
    _seed_logs(
        con,
        "logs_svc_sess",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=1),
                "ip": "1.1.1.1",
                "ja4": "ja4-A",
                "country": "US",
                "asn": 100,
                "status": 200,
                "resp_bytes": 500,
                "tcp_rtt": 12.5,
                "edge": 1,
                "ua": "Mozilla/5.0",
                "edge_sid": "sid-1",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=2),
                "ip": "1.1.1.1",
                "ja4": "ja4-A",
                "country": "US",
                "asn": 100,
                "status": 404,
                "resp_bytes": 100,
                "tcp_rtt": 18.0,
                "edge": 1,
                "ua": "Mozilla/5.0",
                "edge_sid": "sid-2",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=5),
                "ip": "2.2.2.2",
                "ja4": "ja4-B",
                "country": "JP",
                "asn": 200,
                "status": 500,
                "resp_bytes": 250,
                "tcp_rtt": 30.0,
                "edge": 0,
                "ua": "curl/8.0",
                "edge_sid": "sid-3",
            },
        ],
    )

    with writer_patches(cache_root, "logs_svc_sess", con):
        assert sessions.build_session_bundles("svc-sess", src, [hour_token]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "sessions.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {
        "bucket",
        "ip",
        "ja4",
        "first_ts",
        "last_ts",
        "req_count",
        "country",
        "asn",
        "reqs_4xx",
        "reqs_5xx",
        "total_bytes",
        "rtt_sum",
        "rtt_count",
        "edge_count",
        "shield_count",
        "ua_min",
        "edge_sid_max",
    }.issubset(set(t.column_names))

    rows_by_ip = {r["ip"]: r for r in t.to_pylist()}
    a, b = rows_by_ip["1.1.1.1"], rows_by_ip["2.2.2.2"]
    assert (a["req_count"], a["reqs_4xx"], a["reqs_5xx"], a["total_bytes"], a["edge_count"], a["shield_count"]) == (
        2,
        1,
        0,
        600,
        2,
        0,
    )
    assert (b["req_count"], b["reqs_5xx"], b["edge_count"], b["shield_count"]) == (1, 1, 0, 1)


@pytest.mark.usefixtures("frozen_clock")
def test_build_sessions_ja4_absent_writes_null(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ja4"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ip VARCHAR)")
    con.execute("INSERT INTO logs_x VALUES (?, ?)", [hour_dt + timedelta(minutes=3), "9.9.9.9"])

    with writer_patches(cache_root, "logs_x", con):
        assert sessions.build_session_bundles("svc-no-ja4", src, [hour_token]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "sessions.parquet"
    rows = pq.read_table(str(bundle)).to_pylist()
    assert len(rows) == 1
    assert rows[0]["ja4"] is None and rows[0]["country"] is None and rows[0]["edge_count"] == 0


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_writes_per_minute_buckets(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ts"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs_rollups_time_series(
        con,
        "logs_ts",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=0, seconds=10),
                "status": 200,
                "cache": "HIT",
                "resp_bytes": 100,
                "ttfb": 0.05,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=0, seconds=20),
                "status": 404,
                "cache": "HIT-STALE",
                "resp_bytes": 80,
                "ttfb": 0.1,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=1),
                "status": 500,
                "cache": "MISS",
                "resp_bytes": 200,
                "ttfb": 0.2,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=2),
                "status": 200,
                "cache": "HIT",
                "resp_bytes": 300,
                "ttfb": 0.15,
            },
        ],
    )

    with writer_patches(cache_root, "logs_ts", con):
        assert time_series.build_time_series_bundles("svc-ts", src, [hour_token]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {
        "bucket",
        "requests",
        "status_4xx",
        "status_5xx",
        "hits",
        "cache_total",
        "resp_bytes_sum",
        "ttfb_sum",
        "ttfb_count",
    }.issubset(set(t.column_names))

    rows = sorted(t.to_pylist(), key=lambda r: r["bucket"])
    assert len(rows) == 3
    assert (
        rows[0]["requests"],
        rows[0]["status_4xx"],
        rows[0]["status_5xx"],
        rows[0]["hits"],
        rows[0]["cache_total"],
    ) == (2, 1, 0, 2, 2)
    assert rows[1]["status_5xx"] == 1
    assert rows[2]["ttfb_count"] == 1 and abs(rows[2]["ttfb_sum"] - 0.15) < 1e-9


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_replaces_an_existing_hour_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ts-rebuild"}
    hour_token, hour_dt = _past_hour(2)
    db_path = tmp_path / "source.duckdb"
    con = duckdb.connect(str(db_path))
    _seed_logs_rollups_time_series(
        con,
        "logs_ts",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=59),
                "status": 200,
                "cache": "MISS",
                "resp_bytes": 100,
                "ttfb": 0.05,
            }
        ],
    )

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    with writer_patches(cache_root, "logs_ts", con):
        assert time_series.build_time_series_bundles("svc-ts-rebuild", src, [hour_token]) == 1
    assert pq.read_table(str(bundle))["requests"].to_pylist() == [1]

    con = duckdb.connect(str(db_path))
    con.execute(
        "INSERT INTO logs_ts VALUES (?, ?, ?, ?, ?)",
        [hour_dt + timedelta(minutes=59, seconds=30), 200, "MISS", 100, 0.05],
    )
    with writer_patches(cache_root, "logs_ts", con):
        assert time_series.build_time_series_bundles("svc-ts-rebuild", src, [hour_token]) == 1
    assert pq.read_table(str(bundle))["requests"].to_pylist() == [2]


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_columns_absent_use_constant_zero(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bare"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_bare (timestamp TIMESTAMPTZ)")
    con.execute("INSERT INTO logs_bare VALUES (?)", [hour_dt + timedelta(minutes=1)])

    with writer_patches(cache_root, "logs_bare", con):
        assert time_series.build_time_series_bundles("svc-bare", src, [hour_token]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    rows = pq.read_table(str(bundle)).to_pylist()
    assert len(rows) == 1
    r = rows[0]
    assert (
        r["requests"],
        r["status_4xx"],
        r["status_5xx"],
        r["hits"],
        r["cache_total"],
        r["resp_bytes_sum"],
        r["ttfb_count"],
    ) == (1, 0, 0, 0, 0, 0, 0)


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.parametrize(
    ("writer_fn", "seed_fn", "missing_col_ddl"),
    [
        (
            sessions.build_session_bundles,
            _seed_logs,
            "CREATE TABLE logs_missing (timestamp TIMESTAMPTZ, status INTEGER)",
        ),
        (
            time_series.build_time_series_bundles,
            _seed_logs_rollups_time_series,
            "CREATE TABLE logs_missing (status INTEGER)",
        ),
    ],
    ids=["sessions", "time_series"],
)
def test_rollup_writer_edge_cases_and_skips(tmp_path, writer_fn, seed_fn, missing_col_ddl):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    hour_token, _ = _past_hour(2)

    # Empty input
    assert writer_fn("svc", src, []) == 0

    # Active hour & malformed hour skipped
    con = duckdb.connect(":memory:")
    seed_fn(con, "logs_x", [])
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        assert writer_fn("svc", src, [active]) == 0
        assert writer_fn("svc", src, ["not-an-hour"]) == 0

    # No safe table
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=None),
    ):
        assert writer_fn("svc", src, [hour_token]) == 0

    # Missing required column (ip for sessions, timestamp for time_series)
    con_missing = duckdb.connect(":memory:")
    con_missing.execute(missing_col_ddl)
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_missing"),
        patch("backend.core.duckdb.get_connection", return_value=con_missing),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _src, fn: fn(c)),
    ):
        assert writer_fn("svc", src, [hour_token]) == 0

    # DESCRIBE failure returns 0 cleanly
    def _boom(_c, _src, _fn):
        raise duckdb.Error("synthetic describe failure")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=_boom),
    ):
        assert writer_fn("svc", src, [hour_token]) == 0


# ── Merged from tests/core/test_rollups_rum.py ──
def _create_rum_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR,
            pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR,
            city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, error_line INT, error_col INT,
            pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR,
            city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE
        )
    """)


def test_recompute_rum_aggregates_idempotency_and_correctness():
    con = duckdb.connect(":memory:")
    _create_rum_tables(con)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hour1, hour2 = now - timedelta(hours=2), now - timedelta(hours=1)

    for row in [
        [
            hour1,
            "LCP",
            2.5,
            "good",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
        [
            hour1,
            "LCP",
            4.5,
            "poor",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user2",
            "req2",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
        [
            hour2,
            "CLS",
            0.05,
            "good",
            "/about",
            "Safari",
            "iOS",
            "mobile",
            "user3",
            "req3",
            "NYC",
            "NY",
            "US",
            "JFK",
            "TLS1.3",
            0.1,
        ],
    ]:
        con.execute("INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

    con.execute(
        "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "ReferenceError",
            "app.js",
            10,
            20,
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )

    recompute_rum_aggregates(con, "svc-rum-test")
    assert table_exists(con, "rum_vitals_aggregates")
    assert table_exists(con, "rum_error_aggregates")

    v_rows = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    dimensions = {r[0] for r in v_rows}
    assert {"browser", "metric_name", "total"}.issubset(dimensions)

    lcp_row = [r for r in v_rows if r[0] == "metric_name" and r[1] == "LCP"][0]
    assert lcp_row[2:6] == (2, 1, 1, 4.0)

    e_rows = con.execute(
        "SELECT dimension, value, error_count, bucket_start FROM rum_error_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    assert len(e_rows) == 3 and e_rows[2][0] == "total" and e_rows[2][2] == 1

    recompute_rum_aggregates(con, "svc-rum-test")
    v_rows_after = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    assert v_rows_after == v_rows


def test_backfill_missing_rum_aggregates_heals_legacy_hour_without_total_beacons():
    con = duckdb.connect(":memory:")
    _create_rum_tables(con)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    legacy_hour, recent_hour, beyond_lookback_hour = (
        now - timedelta(days=12),
        now - timedelta(hours=2),
        now - timedelta(days=40),
    )

    for hr, req_id in [
        (legacy_hour, "req-legacy-1"),
        (legacy_hour, "req-legacy-2"),
        (recent_hour, "req-recent-1"),
        (beyond_lookback_hour, "req-old-1"),
    ]:
        con.execute(
            "INSERT INTO client_vitals VALUES (?, 'LCP', 2.1, 'good', '/home', 'Chrome', 'macOS', 'desktop', 'u1', ?, 'SFO', 'CA', 'US', 'SFO', 'TLS1.3', 0.1)",
            [hr, req_id],
        )

    recompute_rum_aggregates(con, "svc-rum-test")
    con.execute(
        "DELETE FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start IN (?, ?) AND value = 'total_beacons'",
        [legacy_hour, beyond_lookback_hour],
    )

    lcp_cnt = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'metric_name' AND value = 'LCP'",
        [legacy_hour],
    ).fetchone()
    assert lcp_cnt is not None and lcp_cnt[0] == 2
    assert (
        con.execute(
            "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
            [legacy_hour],
        ).fetchone()
        is None
    )

    healed = backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30)
    assert len(healed) == 1

    tb_after = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
        [legacy_hour],
    ).fetchone()
    assert tb_after is not None and tb_after[0] == 2
    assert backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30) == []


def test_rollup_hour_heal_invokes_rum_total_beacons_backfill(monkeypatch, tmp_path):
    sid = "svc-rum-heal-cron"
    src = {
        "name": "rum-heal-svc",
        "service_id": sid,
        "cache_dir": str(tmp_path),
        "_cache_dir_override": str(tmp_path),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: src)
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: str(tmp_path))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **k: 42)
    for target in (
        "backend.core.duckdb.log_cron_run",
        "backend.core.duckdb.finalize_cron_run_if_running",
        "backend.cron.scheduler.start_progress",
        "backend.cron.scheduler.end_progress",
        "backend.cron.scheduler.cleanup_progress_and_reap",
        "backend.cron.scheduler._log_and_add_progress",
    ):
        monkeypatch.setattr(target, lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **k: (True, ""))
    monkeypatch.setattr(
        "backend.config.load_config", lambda s: {"provisioning": {"cron_sync": {}}, "rum": {"enabled": True}}
    )
    monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        lambda *a, **k: {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0},
    )
    monkeypatch.setattr("backend.core.rollups._common._hour_bundled_root", lambda s: str(tmp_path / "hours"))

    fake_con = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.rum_source_for", lambda s: dict(s))
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: fake_con)
    monkeypatch.setattr(_dl, "_ducklake_attach", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.storage.execute_with_stale_view_retry", lambda con, s, fn, **kw: fn(con))

    backfill_calls: list[tuple[object, str, int]] = []
    monkeypatch.setattr(
        "backend.core.rollups.recompute.backfill_missing_rum_aggregates",
        lambda con, service_id, lookback_days=30: backfill_calls.append((con, service_id, lookback_days)) or [],
    )

    res = compaction._run_rollup_hour_heal.__wrapped__(sid, manual=True)
    assert res["status"] == "success"
    assert backfill_calls == [(fake_con, sid, 30)]


def _seeded_file_db(path: Path, hours: int = 4) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    con.execute(
        "CREATE TABLE client_errors (timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, "
        "error_line INT, error_col INT, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    for h in range(hours):
        for i in range(10):
            con.execute(
                "INSERT INTO client_vitals VALUES (?, 'LCP', 1000, 'good', '/p', 'c', 'o', 'd', ?, ?)",
                [now - timedelta(hours=h) + timedelta(seconds=i), f"c{i}", f"r{h}_{i}"],
            )
    return con


def _duplicate_keys(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM rum_vitals_aggregates GROUP BY service_id, bucket_start, dimension, value HAVING COUNT(*) > 1)"
    ).fetchone()[0]


def test_concurrent_recomputes_never_duplicate_aggregate_rows(tmp_path: Path):
    con = _seeded_file_db(tmp_path / "rum.duckdb")
    recompute_rum_aggregates(con, "svc")
    errors: list[BaseException] = []

    def run() -> None:
        try:
            recompute_rum_aggregates(con.cursor(), "svc")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    for _ in range(10):
        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert errors == [] and _duplicate_keys(con) == 0


def test_heal_rebuilds_hours_whose_total_beacons_row_is_duplicated(tmp_path: Path):
    con = _seeded_file_db(tmp_path / "rum.duckdb")
    recompute_rum_aggregates(con, "svc")
    con.execute("INSERT INTO rum_vitals_aggregates SELECT * FROM rum_vitals_aggregates WHERE value = 'total_beacons'")
    assert _duplicate_keys(con) > 0

    healed = backfill_missing_rum_aggregates(con, "svc", lookback_days=30)
    assert healed and _duplicate_keys(con) == 0
    assert (
        con.execute("SELECT SUM(event_count) FROM rum_vitals_aggregates WHERE value = 'total_beacons'").fetchone()[0]
        == 40
    )


# ── Merged from tests/core/test_rum_rollups.py ──
def test_get_beacon_health_and_header_stats(in_memory_duckdb):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    con.execute(
        "CREATE TABLE client_errors (timestamp TIMESTAMPTZ, error_message VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    con.execute(
        "INSERT INTO client_vitals (timestamp, metric_name, metric_value, cid, req_id) VALUES (?, 'LCP', 2.0, 'c1', 'r1')",
        [datetime.now(UTC)],
    )
    recent_count, last_dt = rum_repo.get_beacon_health(con)
    assert recent_count == 1 and last_dt is not None

    distinct_count, newest_dt = rum_repo.get_header_stats(con)
    assert distinct_count == 1 and newest_dt is not None
    assert rum_repo.get_client_vitals_columns(con) == [
        "timestamp",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "cid",
        "req_id",
    ]


# ── Merged from tests/core/test_rollup_writers_golden.py ──
GOLDEN_PATH = Path(__file__).parent / "golden" / "rollup_writers.json"
SERVICE = "svc_golden"
TABLE = "logs_svc_golden"

_LOG_TABLE_SQL = f"""
CREATE TABLE {TABLE} AS
SELECT
  TIMESTAMPTZ '2026-06-13 00:00:00+00' + to_minutes(i::INTEGER) AS timestamp,
  '10.0.' || (i % 5) || '.' || (i % 7) AS ip,
  (['Mozilla/5.0', 'curl/7.68', 'Googlebot/2.1', 'python-requests'])[(i % 4) + 1] AS ua,
  '/p' || (i % 6) AS url,
  (['US', 'JP', 'DE'])[(i % 3) + 1] AS country,
  (['NYC', 'TYO', 'BER'])[(i % 3) + 1] AS city,
  (['NY', 'TK', 'BE'])[(i % 3) + 1] AS region,
  (['501', '392', '999'])[(i % 3) + 1] AS metro,
  40.0 + (i % 3) AS lat,
  -70.0 + (i % 3) AS lon,
  CAST(100 + (i % 3) AS BIGINT) AS asn,
  (['DEN', 'IAD', 'LAX'])[((i // 3) % 3) + 1] AS pop,
  (CASE i % 5 WHEN 0 THEN 200 WHEN 1 THEN 200 WHEN 2 THEN 404 WHEN 3 THEN 500 ELSE 304 END)::INTEGER AS status,
  (['HIT', 'MISS', 'PASS', 'HIT-STALE', 'MISS'])[(i % 5) + 1] AS cache,
  (['GET', 'POST'])[(i % 2) + 1] AS method,
  0.01 + (i % 10) * 0.01 AS ttfb,
  CAST(50000 + (i % 20) * 1000 AS DOUBLE) AS ottfb,
  CAST(80000 + (i % 25) * 1000 AS DOUBLE) AS ottlb,
  CAST(1000 + (i % 40) AS BIGINT) AS obytes,
  CAST(100 + (i % 30) AS DOUBLE) AS elapsed,
  CAST(10000 + (i % 50) * 100 AS BIGINT) AS tcp_rtt,
  CAST(5000 + (i % 7) * 100 AS BIGINT) AS rtt_min,
  CAST(100 + (i % 9) AS BIGINT) AS rtt_var,
  (i % 10) / 100.0 AS ploss,
  CAST(500 + (i % 100) AS BIGINT) AS req_bytes,
  CAST(1000 + (i % 100) AS BIGINT) AS resp_bytes,
  CAST(200 + (i % 30) AS BIGINT) AS req_header_bytes,
  CAST(1 + (i % 25) AS BIGINT) AS conn_requests,
  (['slow', 'med', 'fast'])[(i % 3) + 1] AS c_speed,
  (i % 4) <> 0 AS edge,
  '203.0.113.' || (i % 4) AS oip,
  (CASE i % 3 WHEN 0 THEN 200 WHEN 1 THEN 200 ELSE 500 END)::INTEGER AS ost,
  'ja4_' || (i % 5) AS ja4,
  CASE WHEN i % 7 = 0 THEN NULL ELSE 'sha' || (i % 3) END AS tls_ciphers_sha,
  CAST((i % 5) * 60 AS BIGINT) AS ttl,
  CASE WHEN i % 9 = 0 THEN 'VERIFIED-BOT.google,WAF.x' ELSE NULL END AS waf_sig,
  (i % 11 = 0) AS waf,
  (i % 13 = 0) AS waf_resp,
  (i % 2 = 0) AS is_shield,
  (i % 3 <> 0) AS is_ssl,
  (i % 8 = 0) AS ipv6,
  'HTTP/2' AS protocol,
  'sid' || (i % 12) AS edge_sid,
  'cmcd' || (i % 6) AS cmcd_sid,
  'req' || i AS waf_req_id
FROM range(0, 2880) AS t(i)
"""

_TMP_RE = re.compile(r"(\.tmp_[a-z0-9_]*?)[0-9a-f]{12}(\.parquet)")
_RANDOM_NAME_RE = re.compile(r"(compacted_|part-|data_)[0-9a-f]{12,32}")
_TMP_DIR_RE = re.compile(r"(rollups/tmp/[A-Za-z0-9_]+?)_[0-9a-f]{8}\b")
_PATHS_RE = re.compile(r"read_parquet\(\[([^\]]*)\]")
_SHAPE_RE = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?|-\d{2})?")
SCHEMAS: dict[str, list[str]] = {}
_FIELDS = ["country", "url", "status", "pop", "ua", "cache"]
_BOT_PATTERN = "Googlebot|curl"
_BOT_VERSION = "v-golden"


class _Recorder:
    def __init__(self, con: duckdb.DuckDBPyConnection, log: list[str], close: bool) -> None:
        self._con, self._log, self._close = con, log, close

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(query)
        return self._con.execute(query, *args, **kwargs)

    def close(self) -> None:
        if self._close:
            self._con.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


def _normalise_sql(q: str, root: str) -> str:
    q = _TMP_DIR_RE.sub(r"\1_<id>", _RANDOM_NAME_RE.sub(r"\1<id>", _TMP_RE.sub(r"\1<id>\2", q.replace(root, "<R>"))))
    q = _PATHS_RE.sub(lambda m: "read_parquet([" + ", ".join(sorted(p.strip() for p in m.group(1).split(","))) + "]", q)
    return re.sub(r" ?([()]) ?", r"\1", re.sub(r"\s+", " ", q).strip())


def _digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def _shape(q: str) -> str:
    return _SHAPE_RE.sub("<date>", q)


def _canon(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, list):
        return [_canon(x) for x in v]
    return v


def _snapshot_tree(
    root: Path, file_cache: dict[Path, tuple[int, int, dict[str, Any], list[str]]] | None = None
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = _RANDOM_NAME_RE.sub(r"\1<id>", p.relative_to(root).as_posix())
        if rel.startswith("_scratch"):
            continue
        st = p.stat()
        if p.suffix != ".parquet":
            out[rel] = {"bytes": st.st_size}
            continue
        cached = file_cache.get(p) if file_cache is not None else None
        if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
            entry, schema = cached[2], cached[3]
            SCHEMAS[entry["schema"]] = schema
            out[rel] = entry
            continue
        table = pq.ParquetFile(p).read()
        rows = sorted(json.dumps(_canon(list(r.values())), default=str) for r in table.to_pylist())
        schema = [f"{f.name}:{f.type}" for f in table.schema]
        schema_digest = _digest(json.dumps(schema))
        SCHEMAS[schema_digest] = schema
        entry = {"schema": schema_digest, "rows": len(rows), "sha": _digest("\n".join(rows))}
        if file_cache is not None:
            file_cache[p] = (st.st_mtime_ns, st.st_size, entry, schema)
        out[rel] = entry
    return out


def _writers() -> list[tuple[str, Callable[[str, dict, list[str]], int]]]:
    return [
        ("time_series", time_series.build_time_series_bundles),
        ("sessions", sessions.build_session_bundles),
        ("slow_urls", slow_urls.build_slow_urls_bundles),
        ("origin_summary", origin_summary.build_origin_summary_bundles),
        ("origin_dims", origin_dims.build_origin_dims_bundles),
        ("origin_latency_ts", origin_latency_ts.build_origin_latency_ts_bundles),
        ("network_rtt", network_rtt.build_network_rtt_bundles),
        ("network_speed", network_speed.build_network_speed_bundles),
        ("network_quality", network_quality.build_network_quality_bundles),
        ("network_summary", network_summary.build_network_summary_bundles),
        ("network_heatmap", network_health_heatmap.build_network_heatmap_bundles),
        ("network_geo", network_health_geo.build_network_geo_bundles),
        ("perf_dims", perf_dims.build_perf_dims_bundles),
        ("perf_latency", perf_latency.build_perf_latency_bundles),
        ("security_dims", security_dims.build_security_dims_bundles),
        ("overview", overview.build_overview_bundles),
        ("pop_health", pop_health.build_pop_health_bundles),
        ("verified_bots_ts", verified_bots_ts.build_verified_bots_ts_bundles),
        ("ngwaf_bots", ngwaf_bots.build_ngwaf_bots_bundles),
        ("ngwaf_bots_ts", ngwaf_bots.build_ngwaf_bots_ts_bundles),
    ]


def _compactors() -> list[tuple[str, Callable[..., int]]]:
    d = day_bundles
    return [
        ("origin_summary", d.compact_origin_summary_closed_days_to_daily),
        ("network_rtt", d.compact_network_rtt_closed_days_to_daily),
        ("network_speed", d.compact_network_speed_closed_days_to_daily),
        ("ngwaf_bots", d.compact_ngwaf_bots_closed_days_to_daily),
        ("ngwaf_bots_ts", d.compact_ngwaf_bots_ts_closed_days_to_daily),
        ("verified_bots_ts", d.compact_verified_bots_ts_closed_days_to_daily),
        ("origin_latency_ts", d.compact_origin_latency_ts_closed_days_to_daily),
        ("perf_latency", d.compact_perf_latency_closed_days_to_daily),
        ("origin_dims", d.compact_origin_dims_closed_days_to_daily),
        ("security_dims", d.compact_security_dims_closed_days_to_daily),
        ("perf_dims", d.compact_perf_dims_closed_days_to_daily),
        ("overview", d.compact_overview_closed_days_to_daily),
        ("pop_health", d.compact_pop_health_closed_days_to_daily),
        ("network_quality", d.compact_network_quality_closed_days_to_daily),
    ]


def _closed_hours() -> list[str]:
    return [f"2026-06-{d}-{h:02d}" for d in (13, 14) for h in range(24)]


def _write_ngwaf_cache(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE ngwaf_bots (waf_req_id TEXT, bot_name TEXT, category TEXT)")
    rows = [(f"req{i}", f"bot{i % 3}", None if i % 5 == 0 else f"cat{i % 2}") for i in range(0, 2880, 7)]
    rows.append(("req1", None, "untagged"))
    con.executemany("INSERT INTO ngwaf_bots VALUES (?, ?, ?)", rows)
    con.commit()
    con.close()


def _write_data_partitions(shared: duckdb.DuckDBPyConnection, cache_root: Path, hours: list[str]) -> None:
    for h in hours:
        d = cache_root / "data" / f"timestamp_hour={h}"
        d.mkdir(parents=True)
        start = datetime.strptime(h, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        shared.execute(
            f"COPY (SELECT * FROM {TABLE} WHERE timestamp >= TIMESTAMPTZ '{start.isoformat()}' "
            f"AND timestamp < TIMESTAMPTZ '{(start + timedelta(hours=1)).isoformat()}') "
            f"TO '{d / 'part-0.parquet'}' (FORMAT PARQUET)"
        )


def _plant_stale_wellknown(shared: duckdb.DuckDBPyConnection, cache_root: Path) -> None:
    root = cache_root / "rollups" / "wellknown_bots"
    for hour, wipe in (("2026-06-13-05", True), ("2026-06-01-05", False)):
        d = root / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        if wipe:
            for f in d.glob("*.parquet"):
                f.unlink()
        shared.execute(
            "COPY (SELECT 'curl/7.68' AS ua, '10.0.0.1' AS ip, 3::BIGINT AS request_count, "
            f"'old-version' AS pattern_set_version) TO '{d / 'compacted_aaaaaaaaaaaa.parquet'}' (FORMAT PARQUET)"
        )


def _run_pipeline(tmp_path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    root = str(cache_root)
    log: list[str] = []

    shared, mem_shared = duckdb.connect(":memory:"), duckdb.connect(":memory:")
    for c in (shared, mem_shared):
        c.execute("PRAGMA threads=1")
        c.execute("SET TimeZone='UTC'")
    shared.execute(_LOG_TABLE_SQL)

    ngwaf_db = tmp_path / "ngwaf_bot_cache.db"
    _write_ngwaf_cache(ngwaf_db)

    def _writer_con(*_a: Any, **_k: Any) -> _Recorder:
        cur = shared.cursor()
        cur.execute("SET TimeZone='UTC'")
        return _Recorder(cur, log, close=True)

    def _memory_con(*_a: Any, **_k: Any) -> _Recorder:
        cur = mem_shared.cursor()
        cur.execute("SET TimeZone='UTC'")
        return _Recorder(cur, log, close=True)

    source = {"name": SERVICE, "service_id": SERVICE}
    hours = _closed_hours()
    phases: dict[str, Any] = {}
    texts: dict[str, str] = {}
    seen: dict[str, dict] = {}
    file_cache: dict[Path, tuple[int, int, dict[str, Any], list[str]]] = {}

    def phase(name: str, fn: Callable[[], Any]) -> None:
        log.clear()
        returned = fn()
        tree = _snapshot_tree(cache_root, file_cache)
        delta = {k: v for k, v in tree.items() if seen.get(k) != v}
        removed = sorted(k for k in seen if k not in tree)
        seen.clear()
        seen.update(tree)
        normalised = [_normalise_sql(q, root) for q in log]
        texts.update({_digest(q): _shape(q) for q in normalised})
        phases[name] = {
            "returned": returned,
            "changed": delta,
            "removed": removed,
            "sql": sorted(_digest(q) for q in normalised),
            "sql_count": len(log),
        }

    with (
        patch("backend.core.duckdb._cache_dir", return_value=root),
        patch("backend.core.duckdb.get_connection", side_effect=_writer_con),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_memory_con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=_BOT_VERSION),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=_BOT_PATTERN),
    ):
        phase(
            "per_field_copy",
            lambda: (
                rollups._run_per_field_copy(SERVICE, source, rollups._safe_table_for(source), "1=1", _FIELDS),
                rollups.recompute._run_ip_spread_per_field(
                    SERVICE, source, rollups._safe_table_for(source), "1=1", _FIELDS
                ),
            )[1],
        )
        phase("bundle_hours", lambda: hour_bundles.bundle_hours(SERVICE, source, hours))
        phase("compact_closed_days", lambda: rollups.compact_closed_days_to_daily(SERVICE, source))
        phase("bundle_days", lambda: day_bundles.bundle_days(SERVICE, source, ["2026-06-13", "2026-06-14"]))
        for name, writer in _writers():
            phase(f"hour_writer:{name}", lambda w=writer: w(SERVICE, source, hours))
        for name, compactor in _compactors():
            phase(f"day_compactor:{name}", lambda c=compactor: c(SERVICE, source))
        _write_data_partitions(shared, cache_root, hours)
        seen.update(_snapshot_tree(cache_root, file_cache))
        phase(
            "wellknown_bots:recompute", lambda: wellknown_bots.recompute_wellknown_bots_rollup(SERVICE, source, hours)
        )
        _plant_stale_wellknown(shared, cache_root)
        seen.update(_snapshot_tree(cache_root, file_cache))
        phase("wellknown_bots:backfill", lambda: wellknown_bots.backfill_wellknown_bots_rollup(SERVICE, source))
    mem_shared.close()
    shared.close()
    return phases, texts


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.timeout(600)
def test_writers_match_golden(tmp_path: Path) -> None:
    phases, texts = _run_pipeline(tmp_path)
    got = json.loads(json.dumps(phases, sort_keys=True))

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(
                {"phases": got, "schemas": SCHEMAS, "sql_shapes": sorted(set(texts.values()))}, indent=1, sort_keys=True
            )
            + "\n"
        )
        return

    golden = json.loads(GOLDEN_PATH.read_text())
    want = golden["phases"]
    assert got.keys() == want.keys()
    for name in want:
        if got[name] != want[name]:
            new = [h for h in got[name]["sql"] if h not in want[name]["sql"]]
            detail = "\n".join(f"new statement: {texts.get(h)}" for h in new[:3])
            lines = difflib.unified_diff(
                json.dumps(want[name], indent=1, sort_keys=True).splitlines(),
                json.dumps(got[name], indent=1, sort_keys=True).splitlines(),
                "golden",
                "got",
                lineterm="",
                n=2,
            )
            diff = "\n".join(list(lines)[:60])
            raise AssertionError(f"phase {name!r} drifted from golden\n{detail}\n{diff}")


@pytest.mark.usefixtures("frozen_clock")
def test_shared_rollup_session_materializes_slice_once_across_per_field_and_ip_spread(
    tmp_path: Path,
) -> None:
    """Inside shared_rollup_session, _run_per_field_copy and _run_ip_spread_per_field
    materialize the touched-hours slice once into a session temp table and reuse it across
    all per-field COPY queries instead of rescanning the base logs table 40 times."""
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-slice"}
    _, hour_dt = _past_hour(2)
    table = "logs_svc_slice"

    con = _make_table_with_rows(table, hour_dt, [("1.1.1.1", "US"), ("2.2.2.2", "JP")])
    where_sql = (
        f"timestamp >= '{(hour_dt - timedelta(minutes=1)).isoformat()}' "
        f"AND timestamp < '{(hour_dt + timedelta(hours=1)).isoformat()}'"
    )
    executed_sql: list[str] = []

    class _SpyCon:
        def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
            self._inner = inner

        def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
            executed_sql.append(sql)
            return self._inner.execute(sql, *args, **kwargs)

        def close(self) -> None:
            pass

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    spy_con = _SpyCon(con)
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.duckdb.get_connection", return_value=spy_con),
        patch("backend.core.storage._core._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
        rollups.shared_rollup_session(src),
    ):
        rollups._run_per_field_copy("svc-slice", src, table, where_sql, ["ip", "country"])
        rollups.recompute._run_ip_spread_per_field("svc-slice", src, table, where_sql, ["ip", "country"])
    con.close()

    create_slice_stmts = [s for s in executed_sql if "CREATE OR REPLACE TEMP TABLE _rollup_slice_0" in s]
    assert len(create_slice_stmts) == 1
    copy_stmts = [s for s in executed_sql if s.lstrip().startswith("COPY (")]
    assert len(copy_stmts) == 2
    assert all("FROM _rollup_slice_0" in s for s in copy_stmts)
    slice_scans = [s for s in executed_sql if "FROM _rollup_slice_0" in s]
    assert len(slice_scans) == 4
