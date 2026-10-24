"""Tests for rollup day-compaction (item 17 / RC-9 / M2).

Two pieces pinned here:

* ``compact_closed_days_to_daily`` (backend/core/rollups.py): correctly
  rolls 24 per-hour parquets into one per-day parquet, AND uses an
  in-memory DuckDB connection so it doesn't contend with uvicorn's
  RW connection on the per-service ``.duckdb`` file. The lock-
  contention bug surfaced on prod 2026-06-06 — the very first
  compaction attempt blocked 5 min on the DuckDB file lock and never
  produced any per-day files.

* ``_run_rollup_compact_daily`` (backend/cron/jobs/compaction.py): passes
  ``run_id`` through both the success AND error branches of
  ``log_cron_run`` so the row started by ``start_cron_run`` is
  UPDATEd in place. Without this fix the running row is orphaned on
  every failure and a SECOND fresh terminal row is INSERTed —
  pre-fix prod had a stuck ``running`` row from a manual one-shot
  trigger because of this exact bug.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.rollups import partial_hour as ph
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.sources import make_source


# ── Merged from tests/core/test_rollups_compaction.py ──
def _write_hour_rollup(buf: str, field: str, hour: str, rows: list[dict]) -> str:
    """Write a per-hour rollup parquet to
    ``<buf>/rollups/hour/field=<field>/hour=<hour>/compacted_<rand>.parquet``
    and return the path."""
    import uuid

    d = os.path.join(buf, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def test_compact_writes_per_day_file_summing_hour_counts(tmp_path):
    """A closed day with multiple per-hour rollup files becomes ONE
    per-day file whose ``count`` column is the SUM of the hour counts
    per (field, value). Pinned because this is the entire reason the
    M2 compaction exists — reduces 24 file-opens to 1 on dashboard
    7-day queries."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-1"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Closed day 2026-06-04 — two hours each with the same (field,value)
        _write_hour_rollup(
            str(cache_root),
            "ua",
            "2026-06-04-10",
            [{"field": "ua", "value": "Mozilla/5.0", "count": 100}],
        )
        _write_hour_rollup(
            str(cache_root),
            "ua",
            "2026-06-04-11",
            [{"field": "ua", "value": "Mozilla/5.0", "count": 250}],
        )
        # Active (today) day must NOT be compacted — still being written.
        from datetime import UTC, datetime

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        _write_hour_rollup(
            str(cache_root),
            "ua",
            f"{today}-12",
            [{"field": "ua", "value": "Mozilla/5.0", "count": 999}],
        )

        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-1", src)

    assert rebuilt == 1, "exactly one (field, day) tuple should be rebuilt — the closed day"
    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    assert day_file.exists(), f"per-day file missing at {day_file}"

    # Read via DuckDB — pyarrow chokes on the dictionary-encoded string
    # columns DuckDB COPY emits; DuckDB's own reader handles them fine.
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            f"SELECT field, value, count FROM read_parquet('{day_file}') ORDER BY field, value"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("ua", "Mozilla/5.0", 350)], (
        f"per-day file should sum the two hour counts (100+250=350); got {rows}"
    )

    # Today's day MUST NOT have a per-day file (active — still being written).
    active_day_file = cache_root / "rollups" / "day" / "field=ua" / f"day={today}" / "compacted.parquet"
    assert not active_day_file.exists(), "active day must be skipped — premature compaction loses data being written"


def test_compact_uses_in_memory_duckdb_not_per_service_file(tmp_path):
    """Regression for the 2026-06-06 prod lock incident: opening the
    per-service ``.duckdb`` file via ``get_connection`` contends with
    uvicorn's RW connection on the SAME file (held for view rebuilds).
    DuckDB doesn't allow mixed RW+RO from one path → ``DBBusyError``.

    The fix is to use ``duckdb.connect(':memory:')`` — the compaction
    only needs DuckDB to run COPY against local parquet files, no
    persistent state required. This test pins that behaviour by
    spying on ``duckdb.connect`` and asserting it was called ONLY
    with ``':memory:'`` (never with a path to the per-service db).
    """
    import duckdb as duckdb_module

    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-mem"}

    _write_hour_rollup(
        str(cache_root),
        "ua",
        "2026-06-04-10",
        [{"field": "ua", "value": "x", "count": 1}],
    )

    connect_calls: list = []
    real_connect = duckdb_module.connect

    def _spy_connect(*args, **kwargs):
        connect_calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.object(duckdb_module, "connect", side_effect=_spy_connect),
    ):
        rollups.compact_closed_days_to_daily("svc-compact-mem", src)

    # At least one connect call must be in-memory. NONE may target the
    # per-service .duckdb path.
    memory_calls = [c for c in connect_calls if c[0] and c[0][0] == ":memory:"]
    path_calls = [c for c in connect_calls if c[0] and isinstance(c[0][0], str) and c[0][0].endswith(".duckdb")]
    assert memory_calls, (
        f"compaction should open at least one ':memory:' DuckDB connection. Got connect calls: {connect_calls}"
    )
    assert not path_calls, (
        f"compaction must NOT open the per-service .duckdb file — it contends with "
        f"uvicorn's RW connection. Got path calls: {path_calls}"
    )


def test_compact_skips_when_per_day_file_is_already_up_to_date(tmp_path):
    """If the per-day parquet's mtime is newer than every constituent
    per-hour parquet, the day is skipped. Pinned because this is the
    cron's idempotency contract — running it every 24h must NOT redo
    work for days that haven't seen new hour rollups."""
    import time

    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-idem"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(
            str(cache_root),
            "ua",
            "2026-06-04-10",
            [{"field": "ua", "value": "y", "count": 5}],
        )
        first = rollups.compact_closed_days_to_daily("svc-compact-idem", src)
        assert first == 1

        # Force the per-day file's mtime forward so it appears newer
        # than the hour file. Real cron behavior matches: COPY writes
        # the day file AFTER reading the hour file, so its mtime is
        # naturally newer.
        day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
        os.utime(str(day_file), (time.time() + 60, time.time() + 60))

        second = rollups.compact_closed_days_to_daily("svc-compact-idem", src)
        assert second == 0, "already-current day must be skipped"


def _write_hour_bundle(buf: str, hour: str, rows: list[dict]) -> str:
    """Write a bundled-hour parquet to
    ``<buf>/rollups/hour_bundled/hour=<hour>/all_fields.parquet`` and
    return the path. Mirrors the schema the hour-bundler produces:
    ``field`` is a regular column (not hive-partitioned)."""
    import uuid as _uuid

    d = os.path.join(buf, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    # The real bundler writes to all_fields.parquet directly. Mirror.
    p = os.path.join(d, "all_fields.parquet")
    # Use a tmp + rename so a half-written file can't trip the reader's
    # ``os.path.isfile`` check during this test.
    tmp = os.path.join(d, f".tmp_{_uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, tmp)
    os.replace(tmp, p)
    return p


def test_compact_falls_back_to_hour_bundled_when_per_field_hours_cleaned_up(tmp_path):
    """Regression: the hour-bundler's _cleanup_per_field_after_bundle sweep
    deletes rollups/hour/field=*/hour=H/ once an hour-bundle is published.
    Before this fix, compact_closed_days_to_daily walked ONLY the per-field
    hour tree — so a closed day whose per-field hours had all been swept
    yielded an EMPTY per-field-per-day file (or none at all). The reader's
    day_covered_by_any_field check then treated the partial day as
    authoritative and bundled-hour data for that day was silently dropped
    via the bundled-hour walk's ``hour[:10] in day_covered_by_any_field``
    skip. Surfaced 2026-06-15 as a ~16% undercount of POSTs on the
    dashboard method panel.

    Fix: the compactor reads BOTH sources and SUMs. Per-field hour files
    (still present for not-yet-bundled hours) UNION ALL'd with
    bundled-hour files filtered to the current field. This test pins the
    bundled-only case — the per-field tree is empty for the closed day,
    so the entire per-field-per-day must come from the bundle.
    """
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-fallback"}

    # Two closed bundled hours on the same day, both holding method+POST
    # rows. NO per-field-per-hour files for this day exist (mimicking
    # post-cleanup state).
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "method", "value": "POST", "count": 1000},
            {"field": "method", "value": "GET", "count": 2000},
            {"field": "country", "value": "US", "count": 3000},
        ],
    )
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-11",
        [
            {"field": "method", "value": "POST", "count": 1500},
            {"field": "method", "value": "GET", "count": 2500},
        ],
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-fallback", src)

    # Both ``method`` and ``country`` should land per-day files for the day.
    assert rebuilt >= 2, f"expected per-day files for method + country; got {rebuilt}"

    import duckdb as _ddb

    con = _ddb.connect(":memory:")
    try:
        method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
        country_day = cache_root / "rollups" / "day" / "field=country" / "day=2026-06-04" / "compacted.parquet"
        assert method_day.exists(), f"method per-day missing at {method_day}"
        assert country_day.exists(), f"country per-day missing at {country_day}"

        method_rows = sorted(con.execute(f"SELECT value, count FROM read_parquet('{method_day}')").fetchall())
        assert method_rows == [("GET", 4500), ("POST", 2500)], (
            f"per-day file must sum bundled-hour counts; got {method_rows}"
        )
        country_rows = con.execute(f"SELECT value, count FROM read_parquet('{country_day}')").fetchall()
        assert country_rows == [("US", 3000)]
    finally:
        con.close()


def test_compact_unions_per_field_hour_and_bundled_hour_sources(tmp_path):
    """When BOTH per-field hour AND hour-bundled data exist for the same
    (field, day) — e.g. some hours bundled-and-cleaned-up, others still
    pending — the compactor must SUM both sources. Without this the
    per-field-per-day would either drop the bundled hours (old code) or
    the still-pending hours (broken alternative)."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-mixed"}

    # Per-field-per-hour for one hour (not yet bundled).
    _write_hour_rollup(
        str(cache_root),
        "method",
        "2026-06-04-12",
        [{"field": "method", "value": "POST", "count": 500}],
    )
    # Hour-bundled for two earlier hours (per-field files already swept).
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [{"field": "method", "value": "POST", "count": 200}],
    )
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-11",
        [{"field": "method", "value": "POST", "count": 300}],
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.compact_closed_days_to_daily("svc-compact-mixed", src)

    import duckdb as _ddb

    con = _ddb.connect(":memory:")
    try:
        method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
        assert method_day.exists()
        rows = con.execute(f"SELECT value, count FROM read_parquet('{method_day}')").fetchall()
        assert rows == [("POST", 1000)], f"per-day must sum 200+300+500; got {rows}"
    finally:
        con.close()


def test_backfill_missing_hour_bundles_detects_gaps_via_view(tmp_path, monkeypatch):
    """Pin the discovery half of the self-heal pass: an hour where the
    iceberg view has rows but no hour_bundled file exists must end up
    in the recompute call's hour set. Models the 2026-06-15 prod gap
    where ingest's "touched hours" report under-reported and 18 hours
    of dashboard data went silently missing.

    Recompute itself opens the per-service .duckdb file (which the
    test sandbox doesn't host); stub recompute_touched_hours so the
    test stays scoped to the discovery + dispatch contract.
    """
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-heal", "service_id": "svc-heal"}

    # Existing bundle for one hour; no bundle for an earlier hour even
    # though the view will have data there.
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [{"field": "method", "value": "GET", "count": 100}],
    )

    def _fake_update_iceberg_view(con, _src):
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_heal AS SELECT * FROM (VALUES "
            "(TIMESTAMP '2026-06-04 09:30:00+00'), "
            "(TIMESTAMP '2026-06-04 09:45:00+00'), "
            "(TIMESTAMP '2026-06-04 10:15:00+00')"
            ") AS t(timestamp)"
        )

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)

    from datetime import UTC, datetime

    class _FrozenNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)

    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)

    captured_hours: list[set[str]] = []

    def _fake_recompute(_sid, _src, hours):
        captured_hours.append(set(hours))

    monkeypatch.setattr("backend.core.rollups.recompute.recompute_touched_hours", _fake_recompute)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-heal", src, lookback_days=2)

    # Discovery: the 09:00 hour had view rows and NO bundle on entry.
    # The 10:00 hour was already bundled. Self-heal must target ONLY
    # the missing hour.
    assert captured_hours == [{"2026-06-04-09"}], f"unexpected dispatch: {captured_hours}"
    assert result["missing"] == 1, f"expected 1 missing; got {result}"


def test_backfill_missing_hour_bundles_heals_partial_bundle_missing_core_fields(tmp_path, monkeypatch):
    """An existing hour bundle that only contains a subset of core fields (e.g.
    only ``url`` after a partial bundle overwrite) must be detected as
    incomplete and included in the ``missing`` set for recompute."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-partial-heal", "service_id": "svc-partial-heal"}

    # Complete bundle for 10:00 (has ip, status, method, pop).
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "ip", "value": "107.170.164.220", "count": 100},
            {"field": "status", "value": "200", "count": 100},
            {"field": "method", "value": "GET", "count": 100},
            {"field": "pop", "value": "IAD", "count": 100},
        ],
    )
    # Partial bundle for 09:00 (missing ip, status, method, pop — only has url).
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-09",
        [{"field": "url", "value": "/only-url", "count": 120}],
    )

    def _fake_update_iceberg_view(con, _src):
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_partial_heal AS SELECT * FROM (VALUES "
            "(TIMESTAMP '2026-06-04 09:30:00+00', '107.170.164.220', 200, 'GET', 'IAD'), "
            "(TIMESTAMP '2026-06-04 10:15:00+00', '107.170.164.220', 200, 'GET', 'IAD')"
            ") AS t(timestamp, ip, status, method, pop)"
        )

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)

    from datetime import UTC, datetime

    class _FrozenNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)

    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)

    captured_hours: list[set[str]] = []

    def _fake_recompute(_sid, _src, hours):
        captured_hours.append(set(hours))

    monkeypatch.setattr("backend.core.rollups.recompute.recompute_touched_hours", _fake_recompute)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-partial-heal", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}], f"partial bundle must be healed; got {captured_hours}"
    assert result["missing"] == 1
    partial_path = cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet"
    assert os.path.getmtime(partial_path) == 0.0, "incomplete bundle mtime must be reset so bundle_hours rebuilds it"


def test_backfill_ignores_rum_child_views_when_finding_request_view(tmp_path, monkeypatch):
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-rum", "service_id": "svc-rum"}

    def _fake_update_iceberg_view(con, _src):
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_rum__client_errors AS "
            "SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)"
        )
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_rum AS "
            "SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:45:00+00')) AS t(timestamp)"
        )

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)

    from datetime import UTC, datetime

    class _FrozenNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)

    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)
    captured_hours: list[set[str]] = []
    monkeypatch.setattr(
        "backend.core.rollups.recompute.recompute_touched_hours",
        lambda _sid, _src, hours: captured_hours.append(set(hours)),
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-rum", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}]
    assert result["missing"] == 1


def test_backfill_missing_hour_bundles_noop_when_complete(tmp_path, monkeypatch):
    """Bundle tree is complete → no dispatch, no log noise, returns 0."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-noop", "service_id": "svc-noop"}

    # Bundle exists for the only hour the view will surface.
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-09",
        [{"field": "method", "value": "GET", "count": 50}],
    )

    def _fake_update_iceberg_view(con, _src):
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_noop AS SELECT * FROM (VALUES "
            "(TIMESTAMP '2026-06-04 09:30:00+00')"
            ") AS t(timestamp)"
        )

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)

    from datetime import UTC, datetime

    class _FrozenNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)

    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)

    dispatched = []
    monkeypatch.setattr(
        "backend.core.rollups.recompute.recompute_touched_hours",
        lambda *args, **kw: dispatched.append(True),
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)

    assert dispatched == [], "must not dispatch when everything is already bundled"
    assert result["missing"] == 0 and result["rebuilt_fields"] == 0 and result["bundled"] == 0
    # First pass stamps empty SENTINEL bundles for the zero-row hours in the
    # 2-day lookback (48 hours minus the one bundled data hour) so the
    # reader can tell verified-empty from writer-gap.
    assert result["stamped_empty"] == 47, f"expected 47 sentinel stamps, got {result}"

    # Second pass is a TRUE no-op: every hour (data or empty) is covered.
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result2 = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)
    assert dispatched == []
    assert result2 == {
        "missing": 0,
        "rebuilt_fields": 0,
        "bundled": 0,
        "stamped_empty": 0,
        "coverage_verified": True,
    }


def test_backfill_rebuilds_empty_sentinel_when_late_rows_arrive(tmp_path, monkeypatch):
    """A verified-empty hour must be rebuilt when durable data arrives later."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-late", "service_id": "svc-late"}
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [])

    def _fake_update_iceberg_view(con, _src):
        con.execute(
            "CREATE OR REPLACE VIEW logs_svc_late AS SELECT * FROM (VALUES "
            "(TIMESTAMP '2026-06-04 09:30:00+00')"
            ") AS t(timestamp)"
        )

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)

    from datetime import UTC, datetime

    class _FrozenNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)

    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)
    dispatched: list[set[str]] = []

    def _fake_recompute(_sid, _src, hours):
        dispatched.append(set(hours))
        field_dir = cache_root / "rollups" / "hour" / "field=method" / "hour=2026-06-04-09"
        field_dir.mkdir(parents=True, exist_ok=True)
        source_path = field_dir / "late.parquet"
        pq.write_table(
            pa.table(
                {
                    "field": ["method"],
                    "value": ["GET"],
                    "count": pa.array([7], type=pa.int64()),
                }
            ),
            source_path,
        )
        os.utime(source_path, (source_path.stat().st_mtime + 2, source_path.stat().st_mtime + 2))
        assert rollups.bundle_hours("svc-late", src, list(hours)) == 1

    monkeypatch.setattr(
        "backend.core.rollups.recompute.recompute_touched_hours",
        _fake_recompute,
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-late", src, lookback_days=2)

    assert dispatched == [{"2026-06-04-09"}]
    assert result["missing"] == 1
    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet"
    assert pq.read_metadata(bundle).num_rows == 1


def test_corrupt_hour_bundle_is_not_treated_as_coverage(tmp_path):
    from backend.core.rollups.recompute import _bundle_metadata

    corrupt = tmp_path / "all_fields.parquet"
    corrupt.write_bytes(b"not parquet")

    assert _bundle_metadata(str(corrupt)) is None


def test_compact_returns_zero_when_rollups_dir_missing(tmp_path):
    """No rollups dir → no work, returns 0. Pinned because a freshly-
    provisioned service has no rollups yet and the cron MUST be a
    no-op rather than crash."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-empty"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-empty", src)

    assert rebuilt == 0


def _mock_rollups(monkeypatch):
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **kw: 0)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )
    monkeypatch.setattr("backend.core.rollups.compact_network_rtt_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_network_speed_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_ngwaf_bots_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_dims_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_latency_ts_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_summary_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_overview_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_perf_latency_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_security_dims_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_security_proxies_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_verified_bots_ts_closed_days_to_daily", lambda *a, **kw: 0)


def test_run_rollup_compact_daily_passes_run_id_on_success(monkeypatch):
    """Success branch must pass ``run_id`` so log_cron_run UPDATEs the
    running row instead of inserting a fresh terminal row. Without
    this, every successful run orphans the 'running' row created by
    start_cron_run."""
    from backend.cron.jobs import compaction as sch

    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 4242)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda sid, src, lookback_days=30: 7)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-ok")

    assert len(log_calls) == 1
    call = log_calls[0]
    assert call["kwargs"].get("run_id") == 4242, (
        f"success branch must pass run_id=4242 to UPDATE the 'running' row. Got kwargs: {call['kwargs']}"
    )
    # Summary should describe the work done; status is positional[3].
    assert call["args"][3] == "success"
    assert "Rebuilt 7" in (call["kwargs"].get("summary") or "")


def test_run_rollup_compact_daily_passes_run_id_on_error(monkeypatch):
    """Error branch MUST also pass ``run_id`` — the bug that this fix
    addresses was that the original code called log_cron_run without
    run_id in the except block, inserting a fresh 'error' row and
    leaving the original 'running' row stuck forever. Pinned with the
    exact prod incident in mind (cron_runs row 103760 on 2026-06-06)."""
    from backend.cron.jobs import compaction as sch

    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 9999)

    def _boom(sid, src, lookback_days=30):
        raise RuntimeError("simulated DB lock")

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", _boom)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-err")

    assert len(log_calls) == 1
    call = log_calls[0]
    assert call["kwargs"].get("run_id") == 9999, (
        f"error branch must pass run_id=9999 so the 'running' row is UPDATEd "
        f"to 'error' instead of orphaned. Got kwargs: {call['kwargs']}. "
        f"The 2026-06-06 prod orphan (row 103760) was caused by this exact bug."
    )
    assert call["args"][3] == "error"
    assert "simulated DB lock" in (call["kwargs"].get("error_message") or "")


def test_compacted_day_file_has_bigint_count_column(tmp_path):
    """The per-day file's ``count`` column MUST be BIGINT to match the
    per-hour files. The reader's UNION ALL of day + hour scans requires
    column-type parity per column. If compaction writes DOUBLE (the
    default DuckDB SUM(BIGINT) sometimes produces in COPY contexts),
    the UNION ALL breaks at plan time and the dashboard top-N tabs go
    blank. Pinned to the 2026-06-06 prod incident.
    """
    import duckdb

    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-bigint"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(
            str(cache_root),
            "ua",
            "2026-06-04-10",
            [{"field": "ua", "value": "Mozilla", "count": 1}],
        )
        rollups.compact_closed_days_to_daily("svc-compact-bigint", src)

    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    con = duckdb.connect(":memory:")
    try:
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{day_file}')").fetchall()
    finally:
        con.close()
    # DESCRIBE returns rows like (column_name, column_type, null, key, default, extra).
    count_col_type = next((row[1] for row in schema if row[0] == "count"), None)
    assert count_col_type == "BIGINT", (
        f"per-day file's count column must be BIGINT (matches per-hour files for UNION ALL); "
        f"got {count_col_type!r}. Schema: {schema}"
    )


def test_mixed_day_and_hour_read_via_union_all_does_not_hit_hive_partition_mismatch(tmp_path):
    """End-to-end regression for the 2026-06-06 reader bug. Per-day files
    live under ``day=YYYY-MM-DD/`` partition; per-hour files live under
    ``hour=YYYY-MM-DD-HH/``. ``read_parquet([mixed_paths], hive_partitioning=1)``
    in a single call rejects with ``Binder Error: Hive partition mismatch
    ... key "day" not found`` because hive_partitioning requires uniform
    partition keys. The fix is two SEPARATE read_parquet calls (one per
    layout) UNION ALL'd; this test simulates the dashboard's actual
    aggregation query against a mixed file set.
    """
    import duckdb

    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-mixed-read"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Closed day 2026-06-04: 1 hour file → compacted to per-day file.
        _write_hour_rollup(
            str(cache_root),
            "ua",
            "2026-06-04-10",
            [{"field": "ua", "value": "Mozilla", "count": 100}],
        )
        rollups.compact_closed_days_to_daily("svc-mixed-read", src)

        # Today (active day): per-hour file remains as-is — NOT compacted.
        from datetime import UTC, datetime

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        _write_hour_rollup(
            str(cache_root),
            "ua",
            f"{today}-12",
            [{"field": "ua", "value": "Mozilla", "count": 50}],
        )

    # Build the mixed file list as the reader would.
    import glob

    day_files = glob.glob(str(cache_root / "rollups" / "day" / "field=ua" / "day=*" / "*.parquet"))
    hour_files = glob.glob(str(cache_root / "rollups" / "hour" / "field=ua" / f"hour={today}-*" / "*.parquet"))
    assert day_files, "test setup: per-day file should exist"
    assert hour_files, "test setup: per-hour file for active day should exist"

    con = duckdb.connect(":memory:")
    try:
        # 1. Single-call mixed read MUST fail with hive partition mismatch —
        #    pins the underlying behaviour we're working around. If this
        #    starts passing, DuckDB has loosened the hive_partitioning
        #    contract and the UNION ALL split is no longer needed.
        all_paths = day_files + hour_files
        paths_sql = ", ".join("'" + p + "'" for p in all_paths)
        with pytest.raises(duckdb.BinderException, match=r"Hive partition mismatch"):
            con.execute(
                f"SELECT field, value, SUM(count) AS c "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=1) "
                f"GROUP BY field, value"
            ).fetchall()

        # 2. Reader's actual UNION ALL shape MUST succeed and aggregate
        #    the SUM across both sources (100 from the closed-day file +
        #    50 from the active-day hour file = 150).
        day_sql = ", ".join("'" + p + "'" for p in day_files)
        hour_sql = ", ".join("'" + p + "'" for p in hour_files)
        rows = con.execute(
            f"SELECT field, value, SUM(count) AS c FROM ("
            f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{day_sql}], hive_partitioning=1)"
            f"  UNION ALL "
            f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{hour_sql}], hive_partitioning=1)"
            f") GROUP BY field, value"
        ).fetchall()
        assert rows == [("ua", "Mozilla", 150)], (
            f"UNION ALL of day + hour scans must sum across both sources; got {rows}"
        )
    finally:
        con.close()


def test_run_rollup_compact_daily_returns_silently_when_start_cron_run_skips(monkeypatch):
    """If ``start_cron_run`` raises RuntimeError (another task is
    busy), the function returns without calling ``log_cron_run`` —
    no row to UPDATE because none was created. Pinned because the
    pre-fix code had the same skip-on-RuntimeError behaviour but a
    careless refactor could accidentally enter the try-block anyway."""
    from backend.cron.jobs import compaction as sch

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})

    def _busy(src, task):
        raise CronLeaseHeldError("another task is running")

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", _busy)
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily",
        lambda sid, src: pytest.fail("must not be called when start_cron_run skips"),
    )
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    sch._run_rollup_compact_daily("svc-run-busy")

    assert log_calls == [], (
        "log_cron_run must NOT be called when start_cron_run raised — there's no running row to update."
    )


# ── Merged from tests/core/test_rollups_day_bundles.py ──
def _write_per_field_day(cache_root: str, field: str, day: str, rows: list[tuple[str, int]]) -> str:
    """Write a per-(field, day) parquet at the expected layout."""
    d = os.path.join(cache_root, "rollups", "day", f"field={field}", f"day={day}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([field] * len(rows)),
            "value": pa.array([r[0] for r in rows]),
            "count": pa.array([r[1] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, p)
    return p


def _past_day(days_ago: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def test_bundle_days_writes_combined_parquet(tmp_path):
    """Per-(field, day) parquets get combined into a single
    day_bundled/day=D/all_fields.parquet with field/value/count columns."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day"}

    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 100), ("/b", 50)])
    _write_per_field_day(str(cache_root), "country", day, [("US", 80), ("JP", 20)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = day_bundles.bundle_days("svc-day", src, [day])

    assert n == 1
    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    rows = list(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("url", "/a", 100) in rows
    assert ("url", "/b", 50) in rows
    assert ("country", "US", 80) in rows
    assert ("country", "JP", 20) in rows


def test_bundle_days_top_k_truncation_with_other_synthetic_row(tmp_path):
    """When a (field, day) has more than DAY_BUNDLE_TOP_K values, the
    bundle keeps top-K AND a synthetic ``__other__`` row that sums the
    long tail — so the dashboard's per-field total stays correct."""
    from backend.core.rollups import day_bundles
    from backend.core.rollups._common import DAY_BUNDLE_TOP_K

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-topk"}
    day = _past_day(2)

    # Write DAY_BUNDLE_TOP_K + 50 values with counts (i+1) so
    # the bottom 50 collapse into __other__.
    rows = [(f"v{i}", i + 1) for i in range(DAY_BUNDLE_TOP_K + 50)]
    _write_per_field_day(str(cache_root), "url", day, rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        n = day_bundles.bundle_days("svc-topk", src, [day])

    assert n == 1
    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    records = t.to_pylist()
    other = [r for r in records if r["value"] == "__other__"]
    assert len(other) == 1, "exactly one __other__ row expected"
    non_other = [r for r in records if r["value"] != "__other__"]
    assert len(non_other) == DAY_BUNDLE_TOP_K, f"top-K must be exactly {DAY_BUNDLE_TOP_K}"
    # __other__ must equal SUM of the bottom 50 counts (1..50 = 1_275).
    assert other[0]["count"] == sum(range(1, 51))


def test_bundle_days_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    """When only one field's per-day parquet is rewritten after the initial
    day bundle swept per-field files, untouched fields in the existing bundle
    must be preserved rather than dropped."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-preserve"}
    day = _past_day(2)

    _write_per_field_day(str(cache_root), "ip", day, [("107.170.164.220", 500)])
    _write_per_field_day(str(cache_root), "url", day, [("/old", 100)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

        # Rewrite ONLY url (ip's per-field file was swept on the first bundle pass)
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/new", 250)])
        future = time.time() + 60
        os.utime(new_p, (future, future))

        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 500) in rows
    assert ("url", "/new", 250) in rows
    assert ("url", "/old", 100) not in rows


def test_bundle_days_skips_active_day(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d")
    _write_per_field_day(str(cache_root), "url", active, [("/x", 1)])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = day_bundles.bundle_days("svc", {"name": "svc"}, [active])

    assert n == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={active}" / "all_fields.parquet").exists()


def test_bundle_days_skips_when_bundle_up_to_date(tmp_path):
    """A bundle older than every source mtime is reused (no rebuild)."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        # Calling again with no source changes → 0 (skipped).
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_rebuilds_when_source_newer_than_bundle(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1

        # Write a new per-field file with future mtime → source > bundle.
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/b", 5)])
        future = time.time() + 60
        os.utime(new_p, (future, future))

        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1


def test_bundle_days_no_per_field_files_skipped(tmp_path):
    """A day with no per-field parquets → nothing to bundle, no entry."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    # Create only the day root, no field subdirs.
    (cache_root / "rollups" / "day").mkdir(parents=True)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_no_day_root_returns_zero(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache_missing"
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_empty_input_returns_zero():
    from backend.core.rollups import day_bundles

    assert day_bundles.bundle_days("svc", {"name": "svc"}, []) == 0


def test_bundle_days_malformed_day_token_skipped(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    (cache_root / "rollups" / "day").mkdir(parents=True)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, ["not-a-day"]) == 0


def test_backfill_day_bundles_discovers_and_caps(tmp_path):
    """backfill walks the per-field-day tree, skips days that already have
    a bundle, respects ``max_days``."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-bf"}

    days = [_past_day(i) for i in range(2, 6)]  # 4 closed days
    for d in days:
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])

    # Pre-seed one bundle so it gets skipped.
    pre_seeded = days[0]
    seeded_dir = cache_root / "rollups" / "day_bundled" / f"day={pre_seeded}"
    seeded_dir.mkdir(parents=True)
    (seeded_dir / "all_fields.parquet").write_bytes(b"present")

    seen: list[list[str]] = []

    def _fake_bundle(_sid, _src, days_in):
        seen.append(list(days_in))
        return len(days_in)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.day_bundles.bundle_days", _fake_bundle),
    ):
        rebuilt = day_bundles.backfill_day_bundles("svc-day-bf", src, max_days=2)

    assert rebuilt == 2
    # Should NOT include the pre-seeded day; should be capped at 2.
    assert len(seen[0]) == 2
    assert pre_seeded not in seen[0]


def test_backfill_day_bundles_no_root_returns_zero(tmp_path):
    from backend.core.rollups import day_bundles

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_backfill_day_bundles_all_present_returns_zero(tmp_path):
    """If every closed day already has a bundle, backfill returns 0
    without invoking the builder."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc"}

    for i in range(2, 4):
        d = _past_day(i)
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
        bd = cache_root / "rollups" / "day_bundled" / f"day={d}"
        bd.mkdir(parents=True)
        (bd / "all_fields.parquet").write_bytes(b"x")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(
            "backend.core.rollups.day_bundles.bundle_days",
            side_effect=AssertionError("must not be invoked"),
        ),
    ):
        assert day_bundles.backfill_day_bundles("svc", src) == 0


def test_link_day_bundle_alias_hardlinks_and_leaves_no_tmp(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")

    _link_day_bundle_alias(str(src), str(dst))

    assert os.path.samefile(src, dst)
    assert dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_never_removes_existing_dst_on_failure(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")

    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=OSError("no copy")),
    ):
        _link_day_bundle_alias(str(src), str(dst))

    assert dst.read_bytes() == b"old"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_copy_fallback_is_atomic_replace(tmp_path) -> None:
    from backend.core.rollups.day_bundles import _link_day_bundle_alias

    d = tmp_path / "alias"
    d.mkdir()
    src = d / "src.parquet"
    dst = d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    seen_during_copy: list[bool] = []
    real_copy = __import__("shutil").copyfile

    def spy_copy(s, d):
        seen_during_copy.append(dst.exists())
        return real_copy(s, d)

    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=spy_copy),
    ):
        _link_day_bundle_alias(str(src), str(dst))

    assert seen_during_copy == [True]
    assert dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


# ── Merged from tests/core/test_rollups_hour_bundling.py ──
def _write_per_field_hour(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    """Write a per-(field, hour) rollup parquet. Returns path."""
    d = os.path.join(cache_root, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    # PER-FIELD layout stores only (value, count) — field and hour come from
    # the hive path. Mirror that here so we test against the real layout.
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _write_per_field_hour_ip_spread(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    """Write a per-(field, hour) IP-spread rollup parquet. Returns path.

    Schema mirrors the live writer's output (see _common.IP_SPREAD_*
    constants + recompute._run_ip_spread_per_field): (value,
    ip_sketch, ip_count_observed, sample_capped) — field+hour come
    from the hive partition path on read."""
    d = os.path.join(cache_root, "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
            "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
            "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def test_bundle_hours_writes_one_parquet_per_hour(tmp_path):
    """Per-(field, hour) parquets get combined into a single
    rollups/hour_bundled/hour=H/all_fields.parquet containing rows for
    all fields. Schema: field, value, count."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-1"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Hour 2026-05-15-10: two fields, multiple rows each.
        _write_per_field_hour(
            str(cache_root),
            "url",
            "2026-05-15-10",
            [
                {"value": "/login", "count": 100},
                {"value": "/api", "count": 75},
            ],
        )
        _write_per_field_hour(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {"value": "US", "count": 80},
                {"value": "JP", "count": 20},
            ],
        )

        n = rollups.bundle_hours("svc-bundle-1", src, ["2026-05-15-10"])

    assert n == 1, f"expected 1 hour bundled; got {n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists(), f"bundled file missing at {bundle}"

    t = pq.read_table(str(bundle))
    # Bundle MUST include field/value/count. DuckDB's COPY may also
    # preserve the hour hive-partition value as an extra column — that's
    # benign (the reader projects only field/value/count via the explicit
    # SELECT list in execute_top_n_rollups).
    assert {"field", "value", "count"}.issubset(set(t.column_names)), (
        f"bundled parquet must carry field+value+count columns; got {t.column_names}"
    )
    rows = list(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("url", "/login", 100) in rows
    assert ("url", "/api", 75) in rows
    assert ("country", "US", 80) in rows
    assert ("country", "JP", 20) in rows


def test_bundle_hours_skips_active_hour(tmp_path):
    """Active (current UTC) hour must not be bundled — its per-field
    parquets are still being written by the post-sync rebuild and
    bundling would race them. The dashboard reader serves the active
    hour live anyway."""
    from datetime import UTC, datetime

    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-active"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", active, [{"value": "/x", "count": 1}])
        n = rollups.bundle_hours("svc-bundle-active", src, [active])

    assert n == 0, "active hour must be skipped"
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={active}" / "all_fields.parquet"
    assert not bundle.exists()


def test_bundle_hours_skips_when_bundle_is_up_to_date(tmp_path):
    """Re-running bundle_hours with no changes to source files must skip
    the rebuild. Without the mtime guard the post-sync hook would
    re-bundle every closed hour on every sync tick — wasted I/O."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-skip"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        n1 = rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"])
        assert n1 == 1

        bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
        mtime_first = os.path.getmtime(bundle)

        # Re-run with no source changes. Bundle must NOT be rebuilt
        # (mtime would jump if it were).
        n2 = rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"])
        assert n2 == 0, f"second run with no source changes should rebuild 0; got {n2}"
        assert os.path.getmtime(bundle) == mtime_first


def test_bundle_hours_rebuilds_when_source_files_newer(tmp_path):
    """If a per-field file is newer than the bundle, the bundle MUST be
    rebuilt — otherwise the bundle would miss a sync's worth of new
    top-K values.

    The replacement per-field write here mirrors production's
    `_run_per_field_copy` behaviour: a touched hour rewrites the per-
    field tree from a full re-scan of base data, so the new write
    contains both the old "/x" row AND the freshly-added "/y" row.
    (Pre-cleanup the test wrote only "/y" and relied on the prior
    bundle's "/x" surviving via the union of bundle + new per-field;
    the per-field-cleanup-after-bundle pass introduced in 2026-06-12
    deletes the per-field tree once bundled, so the rebuild reads only
    what the second per-field write provides — same invariant as
    production.)
    """
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-stale"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

        # Full-replacement per-field write (the production recompute path
        # is non-incremental). Force mtime strictly forward so the
        # freshness check fires regardless of FS timer resolution.
        new_p = _write_per_field_hour(
            str(cache_root),
            "url",
            "2026-05-15-10",
            [{"value": "/x", "count": 1}, {"value": "/y", "count": 2}],
        )
        future = time.time() + 10
        os.utime(new_p, (future, future))

        n = rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])

    assert n == 1, f"newer per-field file must trigger rebuild; got n={n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    values = set(t["value"].to_pylist())
    assert "/y" in values, "newly-written per-field row must appear in the rebuilt bundle"
    assert "/x" in values, "previous-bundle row that the per-field rewrite preserved must be present"


def test_bundle_hours_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    """Regression test for the 2026-10-23 prod incident where hour=2026-10-23-01
    lost ``field='ip'`` (and 29 other fields) because a single-field per-hour
    write triggered ``bundle_hours`` after ``cleanup_per_field_after_bundle``
    had already deleted the other 30 per-field parquets. Re-bundling when only
    a subset of fields have fresh per-field parquets must preserve untouched
    fields from the existing bundle."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-preserve"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "ip", "2026-05-15-10", [{"value": "107.170.164.220", "count": 120}])
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/a", "count": 10}])
        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1

        # Now only url is rewritten (ip's per-field file was swept by the first bundle pass)
        new_p = _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/b", "count": 25}])
        future = time.time() + 10
        os.utime(new_p, (future, future))

        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    t = pq.read_table(str(bundle))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 120) in rows, "untouched field 'ip' must be preserved from existing bundle"
    assert ("url", "/b", 25) in rows, "updated field 'url' must reflect new per-field parquet"
    assert ("url", "/a", 10) not in rows, "updated field 'url' old rows must be replaced"


def test_reader_uses_bundle_when_available_skipping_per_field_files(tmp_path):
    """When a bundled file exists for an hour, the reader's enumeration
    must skip the per-field parquets for that hour (since the bundle
    already covers them) — otherwise data is double-counted."""
    from backend.core import rollups
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-prefer-bundle", "bucket": "b", "prefix": "p"}

    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 50}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-prefer-bundle", src, ["2026-05-15-10"])

        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            # Window includes the bundled hour. should_query_live=False
            # because end_time is well before the active hour.
            rows, _ = runner.execute_top_n_rollups(
                ["url", "country"],
                "2026-05-15T10:00:00",
                "2026-05-15T11:00:00",
                limit=10,
            )

    by_field: dict[str, list[tuple]] = {}
    for f, v, c in rows:
        by_field.setdefault(f, []).append((v, c))
    # Each value must appear EXACTLY ONCE (count == 100/50). If the
    # reader read both bundle AND per-field files, we'd see 200/100.
    assert by_field.get("url") == [("/login", 100)], (
        f"url count must be 100 (single source); got {by_field.get('url')}. Double-count bug?"
    )
    assert by_field.get("country") == [("US", 50)], (
        f"country count must be 50 (single source); got {by_field.get('country')}. Double-count bug?"
    )


def test_reader_falls_back_to_per_field_when_bundle_missing(tmp_path):
    """When NO bundled file exists for an hour (cron hasn't run yet, or
    bundling failed), the reader must fall back to per-field files for
    that hour. Otherwise data for unbundled hours silently disappears."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-fallback", "bucket": "b", "prefix": "p"}

    # Per-field file exists, but NO bundle.
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            rows, _ = runner.execute_top_n_rollups(
                ["url"],
                "2026-05-15T10:00:00",
                "2026-05-15T11:00:00",
                limit=10,
            )

    assert rows == [("url", "/login", 100)], f"reader must fall back to per-field when bundle missing; got {rows}"


def test_reader_mixed_bundled_and_per_field_hours(tmp_path):
    """A query window spanning multiple hours where SOME are bundled
    and others aren't (newly-built bundle backlog) must return the
    correct unioned counts."""
    from backend.core import rollups
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-mixed", "bucket": "b", "prefix": "p"}

    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-11", [{"value": "/login", "count": 50}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        # Bundle ONLY hour 10. Hour 11 stays per-field.
        rollups.bundle_hours("svc-read-mixed", src, ["2026-05-15-10"])

        import duckdb as _ddb

        con = _ddb.connect(":memory:")
        with (
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            runner = QueryRunner(con, src)
            rows, _ = runner.execute_top_n_rollups(
                ["url"],
                "2026-05-15T10:00:00",
                "2026-05-15T12:00:00",
                limit=10,
            )

    # Hour 10 = 100, hour 11 = 50 → total 150. If reader double-counted
    # the bundled hour by also reading per-field, we'd see 250.
    assert rows == [("url", "/login", 150)], f"mixed bundled+per-field union must sum correctly; got {rows}"


def test_bundle_hours_cleans_up_per_field_files_after_publish(tmp_path):
    """After a fresh bundle is published, the per-field per-hour parquet
    files that fed into it are redundant — the reader prefers the bundle
    and the writer's recompute path is non-incremental (rewrites all
    per-field for any touched hour from base data). The cleanup pass
    inside bundle_hours sweeps the per-field dirs to keep the file
    count down on the active-day query window."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-cleanup"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
        _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}])

        n = rollups.bundle_hours("svc-cleanup", src, ["2026-05-15-10"])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists()
    # The per-field/hour dirs for the bundled hour should be gone.
    for f in ("url", "country"):
        per_field_hour_dir = cache_root / "rollups" / "hour" / f"field={f}" / "hour=2026-05-15-10"
        assert not per_field_hour_dir.exists(), f"per-field dir {per_field_hour_dir} must be swept after bundling"


def test_bundle_hours_cleanup_dry_run_logs_but_does_not_unlink(tmp_path, caplog):
    """ROLLUP_CLEANUP_DRY_RUN=1 makes the cleanup pass log the file
    count it WOULD delete without actually unlinking — first-deploy
    safety so an operator can confirm the math before flipping it off."""
    import logging

    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-dry-run"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.dict(os.environ, {"ROLLUP_CLEANUP_DRY_RUN": "1"}),
        caplog.at_level(logging.INFO, logger="backend.core.rollups"),
    ):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-dry-run", src, ["2026-05-15-10"])

    # Per-field dir survives the dry run.
    per_field_hour_dir = cache_root / "rollups" / "hour" / "field=url" / "hour=2026-05-15-10"
    assert per_field_hour_dir.exists()
    # And we logged what we would have deleted.
    assert any("ROLLUP_CLEANUP_DRY_RUN" in r.message for r in caplog.records), (
        "dry-run mode must emit a log line naming the file count it would unlink"
    )


def test_bundle_hours_ip_spread_writes_one_parquet_per_hour(tmp_path):
    """Per-(field, hour) IP-spread parquets get combined into a single
    rollups/hour_bundled/hour=H/all_fields_ip.parquet containing rows
    for all fields. Schema: field, value, ip_sketch, ip_count_observed,
    sample_capped. The HLL ``ip_sketch`` BLOB column must pass through
    byte-identical so the reader's deserialize-and-merge step sees the
    same sketches the writer produced."""
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-bundle"}

    # Build two sketches with known input sets so the test can verify
    # byte-for-byte preservation after the bundle round-trip.
    hll_us = HyperLogLog()
    hll_us.update([f"1.1.1.{i}" for i in range(50)])
    hll_jp = HyperLogLog()
    hll_jp.update([f"2.2.2.{i}" for i in range(30)])
    sketch_us = hll_us.to_bytes()
    sketch_jp = hll_jp.to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {
                    "value": "US",
                    "ip_sketch": sketch_us,
                    "ip_count_observed": 50,
                    "sample_capped": False,
                },
                {
                    "value": "JP",
                    "ip_sketch": sketch_jp,
                    "ip_count_observed": 30,
                    "sample_capped": False,
                },
            ],
        )
        # Second field on the same hour — to confirm the bundle merges
        # multiple field dirs into one file.
        hll_ja3 = HyperLogLog()
        hll_ja3.update([f"3.3.3.{i}" for i in range(10)])
        sketch_ja3 = hll_ja3.to_bytes()
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "ja3",
            "2026-05-15-10",
            [
                {
                    "value": "ja3-abc",
                    "ip_sketch": sketch_ja3,
                    "ip_count_observed": 10,
                    "sample_capped": False,
                },
            ],
        )

        from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

        n = bundle_hours_ip_spread("svc-ipsp-bundle", src, ["2026-05-15-10"])

    assert n == 1, f"expected 1 hour bundled; got {n}"

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields_ip.parquet"
    assert bundle.exists(), f"bundled ip_spread file missing at {bundle}"

    t = pq.read_table(str(bundle))
    assert {"field", "value", "ip_sketch", "ip_count_observed", "sample_capped"}.issubset(set(t.column_names)), (
        f"bundle must carry all five columns; got {t.column_names}"
    )

    # Pull rows into a (field, value) -> row dict for assertion clarity.
    by_key: dict[tuple[str, str], dict] = {}
    for row in t.to_pylist():
        by_key[(row["field"], row["value"])] = row
    assert set(by_key.keys()) == {("country", "US"), ("country", "JP"), ("ja3", "ja3-abc")}

    # HLL sketches must pass through byte-identical so the reader's
    # merge produces the same estimates as if we'd added the inputs
    # directly. This is THE invariant the rollup tree relies on.
    assert by_key[("country", "US")]["ip_sketch"] == sketch_us
    assert by_key[("country", "JP")]["ip_sketch"] == sketch_jp
    assert by_key[("ja3", "ja3-abc")]["ip_sketch"] == sketch_ja3
    assert by_key[("country", "US")]["ip_count_observed"] == 50
    assert by_key[("country", "US")]["sample_capped"] is False

    # And the merged reader-side estimate must be close to the true count.
    # (For a single-hour bundle, the merge is a no-op — verify the
    # sketch deserializes and gives a sensible cardinality estimate.)
    restored = HyperLogLog.from_bytes(by_key[("country", "US")]["ip_sketch"])
    assert abs(restored.count() - 50) <= 10  # generous bound at p=8 small-range


def test_bundle_hours_ip_spread_skips_active_hour(tmp_path):
    """Active hour must be skipped — same race-with-writer reason the
    count bundler skips it."""
    import datetime as _dt

    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-active"}
    active_hour = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d-%H")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            active_hour,
            [
                {
                    "value": "US",
                    "ip_sketch": HyperLogLog().to_bytes(),
                    "ip_count_observed": 0,
                    "sample_capped": False,
                },
            ],
        )
        n = bundle_hours_ip_spread("svc-ipsp-active", src, [active_hour])

    assert n == 0
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={active_hour}" / "all_fields_ip.parquet"
    assert not bundle.exists()


def test_bundle_hours_ip_spread_skips_when_bundle_is_up_to_date(tmp_path):
    """If the bundle file is newer than every per-field source, the
    bundler must skip rebuilding (cheap mtime check, expensive COPY
    avoided). Mirrors bundle_hours' fast-path."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
    from backend.utils.hll import HyperLogLog

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-fresh"}
    hour = "2026-05-15-10"
    sketch = HyperLogLog().to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            hour,
            [{"value": "US", "ip_sketch": sketch, "ip_count_observed": 0, "sample_capped": False}],
        )
        # First call produces the bundle.
        n_first = bundle_hours_ip_spread("svc-ipsp-fresh", src, [hour])
        # Touch the bundle to make it strictly newer than the per-field source.
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour}" / "all_fields_ip.parquet"
        time.sleep(0.05)
        os.utime(bundle, None)
        # Second call should NOT rebuild — the bundle is up-to-date.
        n_second = bundle_hours_ip_spread("svc-ipsp-fresh", src, [hour])

    assert n_first == 1
    assert n_second == 0


def test_bundle_hours_ip_spread_returns_zero_when_no_per_field_files(tmp_path):
    """An hour with NO per-field IP-spread parquets yields a 0 return
    and no bundle file — handles the case where ip_spread writer
    hasn't run yet for any field on that hour."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-empty"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = bundle_hours_ip_spread("svc-ipsp-empty", src, ["2026-05-15-10"])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10").exists()


def test_bundle_hours_ip_spread_returns_zero_when_root_missing(tmp_path):
    """When the ip_spread root dir doesn't exist at all (cold pool, no
    writer tick has run yet), the bundler returns 0 immediately
    without raising an OSError on the listdir."""
    from backend.core.rollups.hour_bundles import bundle_hours_ip_spread

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ipsp-cold"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        n = bundle_hours_ip_spread("svc-ipsp-cold", src, ["2026-05-15-10"])

    assert n == 0


# ── Merged from tests/core/test_rollups_partial_hour.py ──
def _write_hourly_parquet(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        # DuckDB's session TimeZone defaults to the OS locale, not UTC — on a
        # non-UTC dev machine, binding a tz-aware Python datetime into this
        # naive TIMESTAMP column would silently localize it instead of
        # storing the UTC wall-clock value. Match the codebase's own
        # connection convention (backend/core/duckdb.py, recompute.py) so
        # this fixture's data means what the assertions below assume.
        con.execute("SET TimeZone='UTC';")
        con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?)", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _write_multi_field_hourly_parquet(path, rows):
    """rows: list of (timestamp, country, method) tuples."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR, method VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


class TestRollupsPartialHour:
    @pytest.fixture(autouse=True)
    def _patch_cache_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path / src["name"]))
        yield

    def test_watermark_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_watermark(src, "2026-09-09-14") == 0.0

    def test_all_fields_defaults_to_empty(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_all_fields(src, "2026-09-09-14") == []

    def test_total_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_total(src, "2026-09-09-14") == 0

    def test_merge_partial_hour_writes_a_total_row_not_per_field_summable(self, tmp_path):
        """C1 (final whole-branch review, empirically verified): summing
        read_partial_hour_all_fields's rows across every populated field counts
        each request once PER POPULATED FIELD, not once. merge_partial_hour must
        write a separate, correct total that survives this trap."""
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

        now = datetime.now(UTC)
        # 3 requests, each with BOTH country and method populated.
        _write_multi_field_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US", "GET"), (now, "US", "POST"), (now, "CA", "GET")],
        )

        stats = ph.merge_partial_hour("svc-a", src, ["country", "method"])
        assert stats["new_files"] == 1

        # The trap: summing per-field rows gives 3 (country) + 3 (method) = 6,
        # double the real request count.
        per_field_rows = ph.read_partial_hour_all_fields(src, active_hour)
        assert sum(c for _, _, c in per_field_rows) == 6, (
            "sanity check: summing per-field rows should reproduce the trap this test guards against"
        )

        # The real total, read correctly, is 3 — not subject to per-field
        # summation, and not one of the rows read_partial_hour_all_fields
        # returns (it must not leak the synthetic __total__ field into the
        # per-field result either).
        assert ph.read_partial_hour_total(src, active_hour) == 3
        assert all(field != ph.TOTAL_FIELD for field, _, _ in per_field_rows)

    def test_merge_partial_hour_excludes_a_custom_field_named_total_field(self, tmp_path):
        """A custom field literally named "__total__" (admin-controlled, but
        _is_safe_ident's regex would otherwise accept it) must not be merged as
        a per-field row — that would collide with and corrupt the synthetic
        total row's own accounting."""
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")
        now = datetime.now(UTC)
        os.makedirs(hourly_dir, exist_ok=True)
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone='UTC';")
            # A hypothetical custom field literally named "__total__" that
            # HAS its own data column — the case the fix guards against.
            con.execute('CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR, "__total__" VARCHAR)')
            con.executemany("INSERT INTO t VALUES (?, ?, ?)", [(now, "US", "x"), (now, "CA", "y")])
            con.execute(f"COPY t TO '{os.path.join(hourly_dir, 'batch1.parquet')}' (FORMAT PARQUET)")
        finally:
            con.close()

        stats = ph.merge_partial_hour("svc-a", src, ["country", ph.TOTAL_FIELD])
        assert stats["new_files"] == 1
        assert ph.read_partial_hour_total(src, active_hour) == 2
        per_field_rows = ph.read_partial_hour_all_fields(src, active_hour)
        assert dict((v, c) for _, v, c in per_field_rows) == {"US": 1, "CA": 1}

    def test_read_partial_hour_functions_reuse_caller_connection(self, tmp_path, monkeypatch):
        """I3 (final whole-branch review): read_partial_hour_all_fields /
        read_partial_hour_total must reuse the caller's DuckDB connection when
        given one, instead of opening a fresh ``:memory:`` connection per call
        (measured ~16ms overhead per call, paid 3x per dashboard request on the
        hot path this whole feature exists to speed up)."""
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")
        now = datetime.now(UTC)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])

        real_connect = duckdb.connect
        connect_calls = []

        def _tracking_connect(*args, **kwargs):
            connect_calls.append((args, kwargs))
            return real_connect(*args, **kwargs)

        caller_con = duckdb.connect(":memory:")
        # Patch the shared `duckdb` module's `connect` AFTER opening the
        # caller's own connection (so that call isn't itself counted), then any
        # `import duckdb; duckdb.connect(...)` inside partial_hour.py's fallback
        # branch — which must NOT run when `con` is supplied — would show up
        # here too.
        monkeypatch.setattr(duckdb, "connect", _tracking_connect)
        try:
            rows = ph.read_partial_hour_all_fields(src, active_hour, con=caller_con)
            total = ph.read_partial_hour_total(src, active_hour, con=caller_con)
        finally:
            caller_con.close()

        assert dict((v, c) for _, v, c in rows) == {"US": 1, "CA": 1}
        assert total == 2
        assert connect_calls == [], (
            f"expected no fresh duckdb.connect() calls when a connection is supplied, got {connect_calls}"
        )

    def test_merge_partial_hour_total_accumulates_across_ticks(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")
        now = datetime.now(UTC)

        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 2

        time.sleep(0.05)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

        # No-op tick leaves the total unchanged.
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

    def test_merge_partial_hour_is_incremental_and_idempotent(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

        now = datetime.now(UTC)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US"), (now, "US"), (now, "CA")],
        )

        stats1 = ph.merge_partial_hour("svc-a", src, ["country"])
        assert stats1["new_files"] == 1
        rows = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
        assert rows == {"US": 2, "CA": 1}

        # Re-running with no new files is a cheap no-op — result unchanged.
        stats2 = ph.merge_partial_hour("svc-a", src, ["country"])
        assert stats2["new_files"] == 0
        rows_again = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
        assert rows_again == {"US": 2, "CA": 1}

        # A second batch of NEW rows merges in additively — not a full
        # recompute (batch1 is not re-read: the watermark already passed it).
        time.sleep(0.05)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch2.parquet"),
            [(now, "US"), (now, "DE")],
        )
        stats3 = ph.merge_partial_hour("svc-a", src, ["country"])
        assert stats3["new_files"] == 1
        rows_final = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
        assert rows_final == {"US": 3, "CA": 1, "DE": 1}

    def test_merge_partial_hour_excludes_rows_before_hour_start(self, tmp_path):
        """Regression test for the hour-boundary WHERE-clause fix.

        A buffer/active-hour-partition parquet is included by FILE mtime, not
        by row-level timestamp certainty (see _list_new_source_files) — a file
        finalized right at the hour boundary can straddle it, holding some rows
        from the previous hour alongside this hour's rows. merge_partial_hour's
        merge SQL must filter those straddling rows out via an explicit
        ``timestamp >= hour_start AND timestamp < hour_end`` predicate. If that
        predicate ever regressed to an unfiltered ``1=1``, this test would be
        the one to catch it: it plants one row timestamped a few seconds BEFORE
        the active hour's start (deliberately not "now", which is always inside
        the active hour and so can never exercise the filter) inside the SAME
        file as an in-hour row, then asserts the pre-hour row's value never
        appears in the persisted rollup.
        """
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = str(tmp_path / "svc-a")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour}")

        hour_start = datetime.strptime(active_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        just_before_hour = hour_start - timedelta(seconds=5)
        inside_hour = datetime.now(UTC)

        _write_hourly_parquet(
            os.path.join(hourly_dir, "straddling.parquet"),
            [(just_before_hour, "PREV_HOUR_ONLY"), (inside_hour, "US")],
        )

        stats = ph.merge_partial_hour("svc-a", src, ["country"])
        assert stats["new_files"] == 1

        rows = dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour))
        assert "PREV_HOUR_ONLY" not in rows
        assert rows == {"US": 1}

    def test_gc_stale_partial_hours_removes_old_but_not_active(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")

        os.makedirs(ph.partial_hour_dir(src, active_hour))
        os.makedirs(ph.partial_hour_dir(src, stale_hour))

        removed = ph.gc_stale_partial_hours(src, max_age_hours=2)

        assert removed == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))

    def test_gc_stale_partial_hours_ignores_malformed_hour_token(self, tmp_path):
        """A dir whose ``hour=`` suffix isn't a parseable token must be left
        alone (and must not raise) — parse_hour_token(...) is None short-
        circuits before the age comparison ever runs on it."""
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        malformed_dir = os.path.join(ph.partial_hour_root(src), "hour=garbage")

        os.makedirs(ph.partial_hour_dir(src, active_hour))
        os.makedirs(ph.partial_hour_dir(src, stale_hour))
        os.makedirs(malformed_dir)

        removed = ph.gc_stale_partial_hours(src, max_age_hours=2)

        assert removed == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))
        assert os.path.isdir(malformed_dir)
