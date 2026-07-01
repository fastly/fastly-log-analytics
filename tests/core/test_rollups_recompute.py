"""Tests for the per-tick recompute / one-shot backfill / retention
cleanup drivers in ``backend.core.rollups.recompute``.

The shared ``_run_per_field_copy`` core is exercised end-to-end against
a real :memory: DuckDB so the COPY+PARTITION_BY+publish-under-lock
sequence + per-field skip rules are all covered.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        # ``"select"`` is alphanumeric so _is_safe_ident passes; we need
        # a value that actually fails the regex.
        recompute._run_per_field_copy("svc-unsafe", src, "logs_x", "1=1", ["bad-field-name!"])

    assert not (cache_root / "rollups" / "hour").exists() or not any((cache_root / "rollups" / "hour").iterdir())


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        # `nonexistent` is not in the schema → skipped silently.
        recompute._run_per_field_copy("svc-missing", src, "logs_x", "1=1", ["nonexistent"])

    field_dir = cache_root / "rollups" / "hour" / "field=nonexistent"
    assert not field_dir.exists()


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_per_field_copy("svc", {"name": "svc"}, "logs_x", "1=1", ["waf_sig_ind"])

    assert not (cache_root / "rollups" / "hour" / "field=waf_sig_ind").exists()


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
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_boom),
    ):
        # Should NOT raise — function logs and returns.
        recompute._run_per_field_copy("svc", {"name": "svc"}, "logs_x", "1=1", ["ip"])

    assert not (cache_root / "rollups" / "hour").exists() or not any((cache_root / "rollups" / "hour").iterdir())


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_per_field_copy("svc-mixed", src, "logs_x", "1=1", ["ip", "country"])

    # ip tmp dir cleaned up.
    assert not (cache_root / "rollups" / "tmp" / "ip").exists()
    # country published successfully.
    assert any((cache_root / "rollups" / "hour" / "field=country").glob("hour=*/compacted_*.parquet"))


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-noip", src, "logs_svc_noip", "1=1", ["country"])

    assert not (cache_root / "rollups" / "hour_ip_spread").exists() or not any(
        (cache_root / "rollups" / "hour_ip_spread").rglob("*.parquet")
    )


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-virt", src, "logs_svc_virt", "1=1", ["waf_sig_ind"])

    # No parquet should land for the virtual field.
    assert not (cache_root / "rollups" / "hour_ip_spread" / "field=waf_sig_ind").exists()


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-cap", src, table, where_sql, ["country"])

    field_dir = cache_root / "rollups" / "hour_ip_spread" / "field=country"
    published = _read_ip_spread_rows(field_dir)
    by_value = {r["value"]: r for r in published}
    assert by_value["JP"]["sample_capped"] is True
    assert by_value["US"]["sample_capped"] is False


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        recompute._run_ip_spread_per_field("svc-empty", src, "logs_x", "1=1", ["country"])

    # No parquet should land.
    assert not (cache_root / "rollups" / "hour_ip_spread" / "field=country").exists()


# ── recompute_touched_hours ─────────────────────────────────────────────────


def test_recompute_touched_hours_no_hours_returns_immediately():
    from backend.core.rollups import recompute

    # Should not raise; nothing to do.
    recompute.recompute_touched_hours("svc", {"name": "svc"}, set())


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


def test_recompute_touched_hours_malformed_hour_skipped(tmp_path):
    """Bad hour tokens are logged + dropped; only good hours proceed."""
    from backend.core.rollups import recompute

    h_good, _ = _past_hour(2)
    captured: list = []

    def _capture(_sid, _src, _table, where_sql, _fields):
        captured.append(where_sql)

    with (
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]),
        patch("backend.core.rollups.recompute._run_per_field_copy", side_effect=_capture),
        patch("backend.core.rollups.recompute._run_ip_spread_per_field"),
        patch("backend.core.rollups.recompute.bundle_hours", return_value=0),
        patch("backend.core.rollups.recompute.build_time_series_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_session_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_slow_urls_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_origin_summary_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_network_rtt_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_network_speed_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_verified_bots_ts_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_perf_latency_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_origin_dims_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_origin_latency_ts_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_security_dims_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_perf_dims_bundles", return_value=0),
    ):
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h_good, "not-an-hour"})

    assert len(captured) == 1
    # The good hour token must appear in the WHERE clause.
    assert h_good in captured[0]


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


def test_recompute_touched_hours_swallows_downstream_bundle_errors():
    """Bundle/time_series/sessions failures must NOT propagate — they're
    best-effort optimisations, the per-field rebuild already succeeded."""
    from backend.core.rollups import recompute

    h, _ = _past_hour(2)
    with (
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._get_fields", return_value=["ip"]),
        patch("backend.core.rollups.recompute._run_per_field_copy"),
        # The recompute now also fires _run_ip_spread_per_field for any
        # field whose IP-spread companion rollup exists; stub it so the
        # test doesn't try to acquire a real DuckDB connection.
        patch("backend.core.rollups.recompute._run_ip_spread_per_field"),
        patch("backend.core.rollups.recompute.bundle_hours", side_effect=RuntimeError("bundle-boom")),
        patch(
            "backend.core.rollups.recompute.build_time_series_bundles",
            side_effect=RuntimeError("ts-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_session_bundles",
            side_effect=RuntimeError("sess-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_slow_urls_bundles",
            side_effect=RuntimeError("su-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_origin_summary_bundles",
            side_effect=RuntimeError("os-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_network_rtt_bundles",
            side_effect=RuntimeError("nr-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_network_speed_bundles",
            side_effect=RuntimeError("ns-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_verified_bots_ts_bundles",
            side_effect=RuntimeError("vbts-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_perf_latency_bundles",
            side_effect=RuntimeError("pl-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_origin_dims_bundles",
            side_effect=RuntimeError("od-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_origin_latency_ts_bundles",
            side_effect=RuntimeError("olts-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_security_dims_bundles",
            side_effect=RuntimeError("sd-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_perf_dims_bundles",
            side_effect=RuntimeError("pd-boom"),
        ),
    ):
        # Must not raise.
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})


# ── backfill_rollups + markers ──────────────────────


def test_backfill_rollups_stamps_markers_for_all_fields(tmp_path):
    """``backfill_rollups`` records an ISO timestamp per field in the
    markers JSON. Subsequent backfill_rollups calls with the same field
    set should see no missing fields."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-mark"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._run_per_field_copy"),
        patch("backend.core.rollups.recompute._run_ip_spread_per_field"),
    ):
        recompute.backfill_rollups("svc-mark", src, fields=["ip", "country"])

    markers_path = cache_root / "rollups" / "backfill_markers.json"
    assert markers_path.exists()
    data = json.loads(markers_path.read_text())
    assert "ip" in data and "country" in data
    # Each value is an ISO timestamp.
    datetime.fromisoformat(data["ip"])


def test_backfill_rollups_no_safe_table_returns(tmp_path):
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._safe_table_for", return_value=None),
        patch(
            "backend.core.rollups.recompute._run_per_field_copy",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        recompute.backfill_rollups("svc", {"name": "svc"})


def test_backfill_rollups_empty_field_list_returns(tmp_path):
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._get_fields", return_value=[]),
        patch(
            "backend.core.rollups.recompute._run_per_field_copy",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        # No fields configured at all → return without invoking the COPY.
        recompute.backfill_rollups("svc", {"name": "svc"})


# ── cleanup_old_rollups ─────────────────────────────────────────────────────


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


def test_cleanup_old_rollups_negative_max_age_disabled(tmp_path):
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, -5) == 0


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


def test_cleanup_old_rollups_no_root_returns_zero(tmp_path):
    from backend.core.rollups import recompute

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert recompute.cleanup_old_rollups("svc", {"name": "svc"}, 7) == 0


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


# ── markers _load_markers + _save_markers (atomic write) ─────────────────────


def test_save_markers_writes_atomic_then_replace(tmp_path):
    """_save_markers writes to a tmp path then os.replace — a partial
    file from a crash mid-write must not be visible to readers."""
    from backend.core.rollups._common import _load_markers, _save_markers

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _save_markers({"name": "svc"}, {"ip": "2026-06-12T10:00:00+00:00"})

    # No tmp leftovers in the rollups dir.
    tmp_leftovers = list((cache_root / "rollups").glob("backfill_markers.json.tmp.*"))
    assert tmp_leftovers == []

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        loaded = _load_markers({"name": "svc"})
    assert loaded == {"ip": "2026-06-12T10:00:00+00:00"}


def test_load_markers_handles_missing_file(tmp_path):
    from backend.core.rollups._common import _load_markers

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert _load_markers({"name": "svc"}) == {}


def test_load_markers_handles_corrupt_json(tmp_path):
    """Corrupt markers file → empty dict + warning (never raise)."""
    from backend.core.rollups._common import _load_markers

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    rd = cache_root / "rollups"
    rd.mkdir()
    (rd / "backfill_markers.json").write_text("{not valid json")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert _load_markers({"name": "svc"}) == {}


def test_load_markers_non_dict_payload_returns_empty(tmp_path):
    """A markers file with a non-dict top-level (list, scalar) must not
    poison the caller — return {} instead."""
    from backend.core.rollups._common import _load_markers

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    rd = cache_root / "rollups"
    rd.mkdir()
    (rd / "backfill_markers.json").write_text(json.dumps(["not", "a", "dict"]))

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert _load_markers({"name": "svc"}) == {}


# ── _publish_field_partitions ───────────────────────────────────────────────


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


def test_publish_field_partitions_no_field_dir_returns_zero(tmp_path):
    """Missing src field dir → nothing to publish."""
    from backend.core.rollups._common import _publish_field_partitions

    assert _publish_field_partitions(str(tmp_path / "missing"), str(tmp_path), "ip") == 0


# ── _get_fields ─────────────────────────────────────────────────────────────


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
