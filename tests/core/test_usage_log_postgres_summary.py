"""Tests for PostgreSQL usage_log hourly summary rollup maintenance.

Verifies that the Python write path (log_usage_calls, log_synthetic_usage,
reconcile_fastly_stats, purge_usage_log, clear_usage_log) correctly maintains
the usage_log_hourly_summary rollup table under PostgreSQL, replacing the
historical SQLite AFTER INSERT / AFTER DELETE triggers.
"""

from __future__ import annotations

from unittest.mock import patch

from backend.core.metadata import usage_log, usage_log_db


def test_log_usage_calls_updates_hourly_summary(monkeypatch):
    sid = "svc_test_summary_1"
    usage_log.clear_usage_log(sid)

    # Patch time to test multi-hour batches
    now_hour_10 = "2026-05-01T10:15:30Z"
    calls_hour_10 = [
        {"service": "FOS", "method": "PutObject", "path": "/f1", "bytes": 100},
        {"service": "FOS", "method": "PutObject", "path": "/f2", "bytes": 200},
        {"service": "FOS", "method": "GetObject", "path": "/f3", "bytes": 50},
        {"service": "CDN", "method": "download", "path": "/c1", "bytes": 1000},
    ]

    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, calls_hour_10)

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes "
        "FROM usage_log_hourly_summary WHERE service_id = ? "
        "ORDER BY hour, operation_class, operation_type",
        (sid,),
    ).fetchall()

    assert len(rows) == 3
    # Hour 10: PutObject (Class A, count 2, bytes 300)
    assert rows[0]["hour"] == "2026-05-01T10"
    assert rows[0]["operation_class"] == "A"
    assert rows[0]["operation_type"] == "PutObject"
    assert rows[0]["count"] == 2
    assert rows[0]["bytes"] == 300

    # Hour 10: GetObject (Class B, count 1, bytes 50)
    assert rows[1]["hour"] == "2026-05-01T10"
    assert rows[1]["operation_class"] == "B"
    assert rows[1]["operation_type"] == "GetObject"
    assert rows[1]["count"] == 1
    assert rows[1]["bytes"] == 50

    # Hour 10: download (Class CDN, count 1, bytes 1000)
    assert rows[2]["hour"] == "2026-05-01T10"
    assert rows[2]["operation_class"] == "CDN"
    assert rows[2]["operation_type"] == "download"
    assert rows[2]["count"] == 1
    assert rows[2]["bytes"] == 1000

    # Second batch in hour 11: also adds to hour 10 to test ON CONFLICT accumulation
    calls_batch_2 = [
        {"service": "FOS", "method": "PutObject", "path": "/f4", "bytes": 400},
    ]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, calls_batch_2)

    row = con.execute(
        "SELECT count, bytes FROM usage_log_hourly_summary "
        "WHERE service_id = ? AND hour = '2026-05-01T10' AND operation_class = 'A' AND operation_type = 'PutObject'",
        (sid,),
    ).fetchone()
    assert row["count"] == 3
    assert row["bytes"] == 700


def test_log_synthetic_usage_updates_hourly_summary():
    sid = "svc_test_summary_2"
    usage_log.clear_usage_log(sid)

    calls = [
        {
            "_timestamp_override": "2026-06-15T08:12:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj1.parquet",
            "bytes": 500,
        },
        {
            "_timestamp_override": "2026-06-15T08:45:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj2.parquet",
            "bytes": 700,
        },
        {
            "_timestamp_override": "2026-06-15T09:05:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj3.parquet",
            "bytes": 300,
        },
    ]

    inserted = usage_log.log_synthetic_usage(sid, calls)
    assert inserted == 3

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes "
        "FROM usage_log_hourly_summary WHERE service_id = ? "
        "ORDER BY hour",
        (sid,),
    ).fetchall()

    assert len(rows) == 2
    assert rows[0]["hour"] == "2026-06-15T08"
    assert rows[0]["operation_class"] == "A"
    assert rows[0]["count"] == 2
    assert rows[0]["bytes"] == 1200

    assert rows[1]["hour"] == "2026-06-15T09"
    assert rows[1]["operation_class"] == "A"
    assert rows[1]["count"] == 1
    assert rows[1]["bytes"] == 300


def test_reconcile_fastly_stats_adjusts_summary():
    sid = "svc_test_summary_3"
    usage_log.clear_usage_log(sid)

    hour = "2026-07-01T14:00:00Z"
    # Seed 10 local Class A operations via log_usage_calls
    calls = [{"service": "FOS", "method": "PutObject", "path": f"/k{i}"} for i in range(10)]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=hour):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    row = con.execute(
        "SELECT count FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert row["count"] == 10

    # Fastly reports 25 Class A operations for the hour -> gap = 15
    hourly_records = [{"hour_iso": hour, "class_a": 25, "class_b": 0}]
    written = usage_log.reconcile_fastly_stats(sid, hourly_records)
    assert written == 1

    # Total in summary should now be 10 + 15 = 25
    sum_row = con.execute(
        "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert sum_row["total"] == 25

    # Re-run reconciliation with 30 Class A operations -> previous gap of 15 is removed, new gap is 20
    hourly_records_updated = [{"hour_iso": hour, "class_a": 30, "class_b": 0}]
    written_2 = usage_log.reconcile_fastly_stats(sid, hourly_records_updated)
    assert written_2 == 1

    sum_row_2 = con.execute(
        "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert sum_row_2["total"] == 30


def test_purge_and_clear_usage_log():
    sid = "svc_test_summary_4"
    usage_log.clear_usage_log(sid)

    now = "2020-01-01T12:00:00Z"
    calls = [{"service": "FOS", "method": "PutObject", "path": "/old"}]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    # Purge old raw logs (retention 1 day)
    usage_log.purge_usage_log(sid, retention_days=1)
    # Raw log deleted
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    # Summary preserved by design
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    # clear_usage_log wipes both
    usage_log.clear_usage_log(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0
