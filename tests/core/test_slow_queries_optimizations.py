"""Tests for trigger migrations, small cache self-healing, and usage_log optimizations."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from backend.core.metadata import slow_queries, usage_log, usage_log_db
from backend.core.metadata.usage_log import clear_usage_log


def test_usage_log_purging_and_trigger_restricton(tmp_path, monkeypatch):
    """Verify raw log deletes do not decrement hourly summary, but reconciliation deletes do."""
    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import iso_z

    sid = "test_purge_svc"
    usage_log.clear_usage_log(sid)

    old_ts = "2020-05-01T10:00:00Z"
    now_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    now_hour = now_dt.strftime("%Y-%m-%dT%H")
    now_ts_1 = iso_z(now_dt)
    now_ts_2 = iso_z(now_dt + timedelta(minutes=15))

    con = usage_log_db.get_con(sid)
    try:
        # 1. Insert raw logs and reconciliation rows with matching summary records
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (old_ts, sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (now_ts_2, sid, "A", "RECONCILE_A", 50, "fastly.reconciliation"),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, "2020-05-01T10", "A", "CDN", 100, 0, old_ts),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, now_hour, "A", "RECONCILE_A", 50, 0, now_ts_1),
        )
        con.commit()

        # Check summaries aggregated both (150 total)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 2. Delete raw log row (simulating retention purging of old rows)
        usage_log.purge_usage_log(sid, retention_days=30)

        # The summary MUST remain untouched (still 150)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 3. Reconcile fastly stats (simulating gap recompute where Fastly reports 0 -> gap 0, so old 50 removed)
        usage_log.reconcile_fastly_stats(sid, [{"hour_iso": now_ts_1, "class_a": 0, "class_b": 0}])

        # The summary MUST reflect the reconciliation adjustment (the old 50 reconciliation row is gone -> total 100)
        row = con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()


def test_clear_usage_log_wipes_both_tables(tmp_path, monkeypatch):
    """Verify that clear_usage_log explicitly truncates both usage_log and usage_log_hourly_summary."""
    sid = "test_clear_svc"
    con = usage_log_db.get_con(sid)
    try:
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("2026-05-01T10:00:00Z", sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sid, "2026-05-01T10", "A", "CDN", 100, 0, "2026-05-01T10:00:00Z"),
        )
        con.commit()

        # Summaries aggregated
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()

    # Call clear_usage_log
    clear_usage_log(sid)

    # Connect again and verify both are empty
    con = usage_log_db.get_con(sid)
    try:
        assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        assert (
            con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        )
    finally:
        usage_log_db.close_all_connections()


def test_flush_releases_postgres_thread_connection_after_success(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-flush"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    con.executemany.assert_called_once()
    con.commit.assert_called_once_with()
    release.assert_called_once_with()


def test_flush_releases_connection_when_write_fails(monkeypatch):
    con = MagicMock()
    con.executemany.side_effect = RuntimeError("metadata unavailable")
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-flush-error"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()


def test_flush_releases_on_the_short_lived_worker_thread(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    released_on = []

    def release():
        released_on.append(threading.current_thread())

    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-thread"] = [{"query_id": "q1"}]

    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert released_on == [worker]


def test_flush_without_connection_keeps_best_effort_behavior(monkeypatch):
    monkeypatch.setattr(
        slow_queries,
        "get_con",
        MagicMock(side_effect=RuntimeError("metadata unavailable")),
    )
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer.clear()
        slow_queries._buffer["svc-no-connection"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()
