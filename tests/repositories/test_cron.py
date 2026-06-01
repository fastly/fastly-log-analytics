"""Tests for backend.repositories.cron.

Thin wrapper over ``backend.core.metadata_db`` cron-run helpers. Exercises
through real per-service SQLite (autouse ``isolate_metadata_db``).
"""

from __future__ import annotations

from backend.core import metadata_db
from backend.repositories.cron import delete_cron_log, get_cron_logs, purge_cron_logs


def _seed_runs(service_id: str, runs: list[dict]) -> list[int]:
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, '[]', ?)",
            (
                r.get("task", "sync"),
                r.get("started_at", "2026-05-15T00:00:00Z"),
                r.get("duration_s", 1.0),
                r.get("status", "success"),
                r.get("summary", "ok"),
            ),
        )
        ids.append(int(cur.lastrowid or 0))
    con.commit()
    return ids


def test_get_cron_logs_returns_total_and_entries():
    sid = "svc-cron-1"
    _seed_runs(sid, [{"task": "sync"}, {"task": "commit"}, {"task": "sync"}])
    total, entries = get_cron_logs(sid)
    assert total == 3
    assert len(entries) == 3


def test_get_cron_logs_filters_by_task_and_status():
    sid = "svc-cron-2"
    _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "error"},
            {"task": "commit", "status": "success"},
        ],
    )
    total_sync, _ = get_cron_logs(sid, task="sync")
    assert total_sync == 2
    total_err, _ = get_cron_logs(sid, status="error")
    assert total_err == 1


def test_get_cron_logs_pagination():
    sid = "svc-cron-3"
    _seed_runs(sid, [{"task": "sync"} for _ in range(8)])
    total, entries = get_cron_logs(sid, page=2, per_page=3)
    assert total == 8
    assert len(entries) == 3


def test_delete_cron_log_removes_only_target():
    sid = "svc-cron-4"
    [a, b] = _seed_runs(sid, [{"task": "sync"}, {"task": "sync"}])
    delete_cron_log(sid, b)
    con = metadata_db.get_con(sid)
    remaining = [row[0] for row in con.execute("SELECT id FROM cron_runs").fetchall()]
    assert remaining == [a]


def test_delete_cron_log_unknown_id_is_noop():
    sid = "svc-cron-5"
    _seed_runs(sid, [{"task": "sync"}])
    delete_cron_log(sid, 99999999)  # must not raise
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 1


def test_purge_cron_logs_no_filter_clears_all():
    sid = "svc-cron-6"
    _seed_runs(sid, [{"task": "sync"}, {"task": "commit"}])
    purge_cron_logs(sid)
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 0


def test_purge_cron_logs_by_task():
    sid = "svc-cron-7"
    _seed_runs(sid, [{"task": "sync"}, {"task": "sync"}, {"task": "commit"}])
    purge_cron_logs(sid, task="sync")
    con = metadata_db.get_con(sid)
    tasks = [row[0] for row in con.execute("SELECT task FROM cron_runs").fetchall()]
    assert tasks == ["commit"]
