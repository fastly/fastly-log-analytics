"""Tests for backend.repositories.cron.

Thin wrapper over ``backend.core.metadata`` cron-run helpers. Exercises
through real per-service SQLite (autouse ``isolate_metadata_db``).
"""

from __future__ import annotations

from backend.core import metadata as metadata_db
from backend.repositories.cron import delete_cron_log, get_cron_logs, purge_cron_logs


def _seed_runs(service_id: str, runs: list[dict]) -> list[int]:
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, ?, '[]', ?)",
            (
                service_id,
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


def test_get_cron_logs_since_id_returns_only_newer_rows():
    """Delta polling (O5): with since_id set, rows with id <= since_id are
    excluded UNLESS they are still running. Used by /logs `recentCrons`
    poll to make steady-state polls return ~0 rows instead of 10.
    """
    sid = "svc-cron-since-1"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid, since_id=ids[1])
    assert total == 1, "only the third row (id > since_id) should match"
    assert {e["id"] for e in entries} == {ids[2]}


def test_get_cron_logs_since_id_keeps_running_rows_even_if_id_below_cutoff():
    """The poll MUST keep status='running' rows visible across polls even
    after their id <= since_id — otherwise the client's
    `backgroundCronToast` status-update effect can't observe the row's
    eventual completion (it looks the row up by id). The
    `(id > ? OR status = 'running')` clause is what guarantees this.
    """
    sid = "svc-cron-since-2"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "running"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    # Cursor is past the running row's id — it would normally be excluded.
    total, entries = get_cron_logs(sid, since_id=ids[2])
    returned_ids = {e["id"] for e in entries}
    assert ids[0] in returned_ids, (
        "running row must remain in the response even when id <= since_id, "
        "so the toast-completion-detection effect on /logs keeps working"
    )
    assert total == len(returned_ids)


def test_get_cron_logs_since_id_none_returns_all_rows():
    """Backwards-compat: when since_id is None (or omitted), the response
    is unchanged from pre-O5 behaviour — all matching rows up to per_page.
    """
    sid = "svc-cron-since-3"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "success"},
        ],
    )
    total, entries = get_cron_logs(sid)
    assert total == 2
    assert {e["id"] for e in entries} == set(ids)


def test_get_cron_logs_since_id_combines_with_task_filter():
    """since_id + task filter compose: only NEW or RUNNING rows of that
    task are returned. Ensures the main 500-row admin paginator (which
    doesn't pass since_id) is unaffected, while the delta poll can still
    layer a task filter if it ever wants to.
    """
    sid = "svc-cron-since-4"
    ids = _seed_runs(
        sid,
        [
            {"task": "sync", "status": "success"},
            {"task": "commit", "status": "success"},
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "running"},
        ],
    )
    total, entries = get_cron_logs(sid, task="sync", since_id=ids[2])
    returned_ids = {e["id"] for e in entries}
    # ids[2] is excluded (id == since_id), ids[3] is new AND running.
    # ids[1] (commit) is excluded by task filter. ids[0] is sync but old + not running.
    assert returned_ids == {ids[3]}
    assert total == 1
