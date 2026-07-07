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
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


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


# ── compact_closed_days_to_daily ────────────────────────────────────────


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
            "CREATE OR REPLACE VIEW logs_test AS SELECT * FROM (VALUES "
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
            "CREATE OR REPLACE VIEW logs_test AS SELECT * FROM (VALUES "
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
    assert result2 == {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0}


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


# ── _run_rollup_compact_daily — wrapper passes run_id ───────────────────


def test_run_rollup_compact_daily_passes_run_id_on_success(monkeypatch):
    """Success branch must pass ``run_id`` so log_cron_run UPDATEs the
    running row instead of inserting a fresh terminal row. Without
    this, every successful run orphans the 'running' row created by
    start_cron_run."""
    from backend.cron.jobs import compaction as sch

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 4242)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda sid, src: 7)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    sch._run_rollup_compact_daily("svc-run-ok")

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

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 9999)

    def _boom(sid, src):
        raise RuntimeError("simulated DB lock")

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", _boom)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    sch._run_rollup_compact_daily("svc-run-err")

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
        raise RuntimeError("another task is running")

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
