"""Regression: orphaned 'running' cron rows must be reapable on demand.

Background: ``backend.cron_progress._progress`` is an in-memory dict wiped on
every server restart. Any ``cron_runs`` row still marked ``running`` after
a restart is by definition an orphan — the worker thread that owned it died
with the previous process and the SSE log stream for it will hang.

``reap_running_cron_runs`` must mark all such rows as ``error`` regardless of
how recently they were started. The pre-existing ``start_cron_run`` orphan
reap is age-gated (60 minutes), which is too long for a stuck UI.
"""

from __future__ import annotations

from backend.core import metadata_db


def _seed_running(service_id: str, task: str = "sync") -> int:
    con = metadata_db.get_con(service_id)
    cur = con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, datetime('now'), 0.0, 'running', '[]')",
        (task,),
    )
    con.commit()
    return int(cur.lastrowid or 0)


def test_reap_marks_running_rows_as_error():
    sid = "svc-reap-1"
    rid = _seed_running(sid)

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    con = metadata_db.get_con(sid)
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    assert row["status"] == "error"
    assert "interrupted" in (row["error_message"] or "").lower()


def test_reap_is_no_op_when_nothing_running():
    sid = "svc-reap-2"
    metadata_db.get_con(sid)  # initialise schema
    n = metadata_db.reap_running_cron_runs(sid)
    assert n == 0


def test_reap_does_not_touch_completed_rows():
    sid = "svc-reap-3"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 0
    row = con.execute("SELECT status FROM cron_runs WHERE task = 'sync'").fetchone()
    assert row["status"] == "success"


def test_reap_ignores_age_unlike_start_cron_run():
    """``start_cron_run`` only reaps rows older than 60 min; this reaps any age.

    The whole point of the new function is to wipe state on startup, when even
    a freshly-running row is an orphan because the in-memory dict is gone.
    """
    sid = "svc-reap-4"
    rid = _seed_running(sid)  # started_at = now (well under 60 min)

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    row = metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    assert row["status"] == "error"


def test_reap_custom_reason_is_recorded():
    sid = "svc-reap-5"
    _seed_running(sid)

    metadata_db.reap_running_cron_runs(sid, reason="Pod evicted by k8s")

    row = metadata_db.get_con(sid).execute("SELECT error_message FROM cron_runs WHERE status = 'error'").fetchone()
    assert "k8s" in (row["error_message"] or "")
