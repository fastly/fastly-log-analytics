"""Tests for rollup day-compaction (item 17 / RC-9 / M2).

Two pieces pinned here:

* ``compact_closed_days_to_daily`` (backend/core/rollups.py): correctly
  rolls 24 per-hour parquets into one per-day parquet, AND uses an
  in-memory DuckDB connection so it doesn't contend with uvicorn's
  RW connection on the per-service ``.duckdb`` file. The lock-
  contention bug surfaced on prod 2026-06-06 — the very first
  compaction attempt blocked 5 min on the DuckDB file lock and never
  produced any per-day files.

* ``_run_rollup_compact_daily`` (backend/scheduler.py): passes
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


def test_backfill_missing_hour_rollups_rebuilds_only_gaps(tmp_path, monkeypatch):
    """The self-healing path catches hours that fell through the
    active-hour skip: all of an hour's data was ingested while the
    hour was still active (so recompute_touched_hours skipped it), and
    no later files arrived to re-trigger a rebuild after the hour
    closed.

    Setup: a base table with rows spanning several closed hours.
    Rollup dir has hour=H-1 already (built normally) but is MISSING
    hour=H-3 (the stranded one). Call backfill_missing_hour_rollups
    and assert: (a) the missing hour gets a per-hour rollup file;
    (b) the already-present hour is not rebuilt; (c) the active hour
    is never rolled up; (d) re-calling is a no-op (idempotent).
    """
    from datetime import UTC, datetime, timedelta

    import duckdb

    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-backfill-missing"}

    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    h_minus_1 = (active_dt - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    h_minus_3 = (active_dt - timedelta(hours=3)).strftime("%Y-%m-%d-%H")
    h_active = active_dt.strftime("%Y-%m-%d-%H")

    # Seed a base table the rollup COPY can read. Use a file-backed
    # DuckDB so the rollup helpers (which open their own connections
    # via get_connection) all see the same data. _get_fields will pull
    # FIELDS from backend.repositories.dashboard; the COPY filters by
    # WHERE timestamp ... AND strftime IN (...), so any subset of fields
    # is fine.
    db_path = str(tmp_path / "test.duckdb")
    seed_con = duckdb.connect(db_path)
    # Row timestamps placed AT THE START of each hour so they land
    # cleanly in the bucket strftime computes. `active - 3h` falls in
    # hour H-3 (the missing-rollup test target), `active - 1h` in H-1
    # (the pre-existing-rollup hour), `active - 5min` in the active
    # hour (must never be rolled up).
    seed_con.execute(
        "CREATE TABLE logs_svc_backfill_missing AS "
        "SELECT "
        f"  TIMESTAMP '{(active_dt - timedelta(hours=3)).isoformat()}' AS timestamp, "
        "  'US' AS country, 200 AS status "
        "UNION ALL SELECT "
        f"  TIMESTAMP '{(active_dt - timedelta(hours=1)).isoformat()}', "
        "  'JP', 404 "
        "UNION ALL SELECT "
        f"  TIMESTAMP '{(active_dt - timedelta(minutes=5)).isoformat()}', "
        "  'DE', 500"
    )
    seed_con.close()  # Release the file lock before the helpers open it.

    # Stub _get_fields to return only `country` (the proxy field check
    # uses fields[0]) — avoids reaching into the full dashboard FIELDS
    # registry and keeps the test self-contained.
    monkeypatch.setattr("backend.core.rollups._get_fields", lambda src: ["country", "status"])
    # Provide a cache_dir + connection factory that all helpers use.
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
    monkeypatch.setattr(
        "backend.core.duckdb.get_connection",
        lambda source, read_only=True: duckdb.connect(db_path, read_only=read_only),
    )
    # execute_with_stale_view_retry inside helpers — short-circuit to a
    # direct execute since there's no iceberg view in the test fixture.
    monkeypatch.setattr(
        "backend.core.iceberg.execute_with_stale_view_retry",
        lambda con, src, fn: fn(con),
    )
    # And bypass _safe_table_for to point at the test table.
    monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_svc_backfill_missing")

    # Pre-seed an existing rollup for h_minus_1 so the test can verify
    # we don't double-build it. Write a parquet with one row.
    pre_existing = cache_root / "rollups" / "hour" / "field=country" / f"hour={h_minus_1}"
    pre_existing.mkdir(parents=True, exist_ok=True)
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table({"value": ["pre_existing"], "count": pa.array([42], type=pa.int64())}),
        str(pre_existing / "compacted_xxx.parquet"),
    )
    pre_existing_mtime = (pre_existing / "compacted_xxx.parquet").stat().st_mtime

    healed = rollups.backfill_missing_hour_rollups("svc-backfill-missing", src, lookback_hours=6)

    # Only ONE closed hour (H-3) has data AND is missing a rollup. H-1
    # has data + a pre-existing rollup → skipped. Other lookback hours
    # (H-2, H-4, H-5, H-6) have no data → not in hours_with_data set →
    # never considered missing. The active hour is excluded by the
    # base-table WHERE bound.
    assert healed == 1, f"expected 1 missing hour with data to be rebuilt; got {healed}"

    # h_minus_3 now has a rollup file (the data-bearing closed hour we set up)
    h_minus_3_dir = cache_root / "rollups" / "hour" / "field=country" / f"hour={h_minus_3}"
    assert h_minus_3_dir.is_dir() and any(
        f.name.endswith(".parquet") for f in h_minus_3_dir.iterdir()
    ), f"missing hour {h_minus_3} should now have a rollup file"

    # The active hour is NEVER rolled up (recompute_touched_hours guard).
    h_active_dir = cache_root / "rollups" / "hour" / "field=country" / f"hour={h_active}"
    assert not h_active_dir.exists(), (
        f"active hour {h_active} must not be rolled up — it's still being written"
    )

    # Second call: idempotent — all closed hours in the lookback now have
    # rollups, so nothing to do. h_minus_3 may have only been built once.
    healed_again = rollups.backfill_missing_hour_rollups("svc-backfill-missing", src, lookback_hours=6)
    assert healed_again == 0, f"second call should be a no-op; got {healed_again} rebuilt"


def test_backfill_missing_hour_rollups_noop_when_no_fields(tmp_path, monkeypatch):
    """If the service has no eligible fields (custom-fields-only and all
    disabled), backfill returns 0 — there's nothing to roll up."""
    from backend.core import rollups

    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-backfill-nofield"}
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
    monkeypatch.setattr("backend.core.rollups._get_fields", lambda src: [])

    healed = rollups.backfill_missing_hour_rollups("svc-backfill-nofield", src, lookback_hours=24)
    assert healed == 0


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
    from backend import scheduler as sch

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
    from backend import scheduler as sch

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
    from backend import scheduler as sch

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
