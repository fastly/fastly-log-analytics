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
        patch("backend.core.rollups.recompute.bundle_hours", return_value=0),
        patch("backend.core.rollups.recompute.build_time_series_bundles", return_value=0),
        patch("backend.core.rollups.recompute.build_session_bundles", return_value=0),
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
        patch("backend.core.rollups.recompute.bundle_hours", side_effect=RuntimeError("bundle-boom")),
        patch(
            "backend.core.rollups.recompute.build_time_series_bundles",
            side_effect=RuntimeError("ts-boom"),
        ),
        patch(
            "backend.core.rollups.recompute.build_session_bundles",
            side_effect=RuntimeError("sess-boom"),
        ),
    ):
        # Must not raise.
        recompute.recompute_touched_hours("svc", {"name": "svc"}, {h})


# ── backfill_rollups + ensure_field_backfills + markers ──────────────────────


def test_backfill_rollups_stamps_markers_for_all_fields(tmp_path):
    """``backfill_rollups`` records an ISO timestamp per field in the
    markers JSON. Subsequent ensure_field_backfills with the same field
    set should see no missing fields."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-mark"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._safe_table_for", return_value="logs_x"),
        patch("backend.core.rollups.recompute._run_per_field_copy"),
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


def test_ensure_field_backfills_skips_when_all_marked(tmp_path):
    """Every eligible field already has a marker → no backfill triggered."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    markers_dir = cache_root / "rollups"
    markers_dir.mkdir()
    (markers_dir / "backfill_markers.json").write_text(
        json.dumps({"ip": "2026-06-12T10:00:00+00:00", "country": "2026-06-12T10:00:00+00:00"})
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._get_fields", return_value=["ip", "country"]),
        patch(
            "backend.core.rollups.recompute.backfill_rollups",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        recompute.ensure_field_backfills("svc", {"name": "svc"})


def test_ensure_field_backfills_triggers_for_missing_fields(tmp_path):
    """A field present in _get_fields but absent from markers triggers
    backfill_rollups with that subset."""
    from backend.core.rollups import recompute

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    markers_dir = cache_root / "rollups"
    markers_dir.mkdir()
    (markers_dir / "backfill_markers.json").write_text(json.dumps({"ip": "2026-06-12T10:00:00+00:00"}))

    captured: list[list[str]] = []

    def _capture(_sid, _src, fields):
        captured.append(list(fields))

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.recompute._get_fields", return_value=["ip", "country", "url"]),
        patch("backend.core.rollups.recompute.backfill_rollups", side_effect=_capture),
    ):
        recompute.ensure_field_backfills("svc", {"name": "svc"})

    assert captured == [["country", "url"]]


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
