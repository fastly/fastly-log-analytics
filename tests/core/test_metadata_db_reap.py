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

import pytest

from backend.core import metadata as metadata_db


def _seed_running(service_id: str, task: str = "sync") -> int:
    con = metadata_db.get_con(service_id)
    cur = con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, ?, datetime('now'), 0.0, 'running', '[]')",
        (service_id, task),
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


def test_reap_clears_legacy_null_service_id_rows():
    """Regression (GCE 2026-09-22): pre-migration 'sync'/'metadata_sync'
    rows stored NULL in the service_id column. ``WHERE service_id = ?`` is
    NULL-unsafe (NULL = 'x' is never true in SQL), so those rows survived
    every restart forever and permanently degraded /api/health?deep=1 with
    "sync stuck running since ...". Since get_con(service_id) already
    scopes the connection to this service's own file, every row in it
    belongs to this service regardless of its service_id column — the
    reap must clear NULL rows too.
    """
    sid = "svc-reap-null-legacy"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (NULL, 'sync', datetime('now'), 0.0, 'running', '[]')"
    )
    con.commit()

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE task = 'sync'").fetchone()
    assert row["status"] == "error"
    assert "interrupted" in (row["error_message"] or "").lower()


def test_reap_custom_reason_is_recorded():
    sid = "svc-reap-5"
    _seed_running(sid)

    metadata_db.reap_running_cron_runs(sid, reason="Pod evicted by k8s")

    row = metadata_db.get_con(sid).execute("SELECT error_message FROM cron_runs WHERE status = 'error'").fetchone()
    assert "k8s" in (row["error_message"] or "")


# ── time-machine: pin the start_cron_run 60-minute orphan boundary ──────────


def _seed_lease(sid: str, task: str, heartbeat_at: float, ttl_s: int = 60) -> None:
    """Insert the ``job_runs`` lease that ``start_cron_run`` consults. The reap is
    heartbeat-driven now: a 'running' cron_runs row is an orphan only when no
    lease row with ``heartbeat_at + lease_ttl_s >= now`` backs it."""
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
        "VALUES (?, ?, ?, ?, ?, 'running')",
        (sid, task, heartbeat_at, heartbeat_at, ttl_s),
    )
    con.commit()


def test_start_cron_run_reaps_rows_exactly_at_lease_expiry():
    """``start_cron_run`` reaps a 'running' cron row once its job_runs lease
    (heartbeat + ttl) has lapsed, and refuses to start while the lease is live.

    Pins the boundary deterministically with time-machine: a lease heartbeating
    at T=0 with a 60 s ttl still blocks a start at T+59 s (the row must stay
    'running'), but is reaped at T+61 s so the new run starts. A different
    task's live lease is never touched (cross-task isolation).
    """
    import datetime as _dt

    import time_machine

    sid = "svc-lease-boundary"
    t0 = _dt.datetime(2026, 6, 16, 12, 0, 0, tzinfo=_dt.UTC)

    old_id = _seed_running_at(sid, "commit", t0)
    _seed_lease(sid, "commit", t0.timestamp(), ttl_s=60)
    cross_id = _seed_running_at(sid, "optimize", t0)
    _seed_lease(sid, "optimize", t0.timestamp() + 3600, ttl_s=60)

    # T+59s: lease still live -> refused, nothing reaped.
    with time_machine.travel(t0 + _dt.timedelta(seconds=59), tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "commit")

    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "running"

    # T+61s: lease lapsed -> the orphan row is reaped and a fresh run starts.
    with time_machine.travel(t0 + _dt.timedelta(seconds=61), tick=False):
        new_id = metadata_db.start_cron_run(sid, "commit")

    assert new_id and new_id != old_id
    old = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (old_id,)).fetchone()
    assert old["status"] == "error"
    assert "heartbeat lost" in (old["error_message"] or "")
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (cross_id,)).fetchone()["status"] == "running", (
        "a different task's live lease must not be reaped by this task's start"
    )


# ── per-task orphan threshold: 'sync' uses a SHORT cutoff (incident 2026-06-19) ─


def _seed_running_at(sid: str, task: str, when) -> int:
    """Insert a 'running' row stamped at the (time-machine) ``when`` instant."""
    import time_machine

    from backend.utils.date_utils import iso_z_now

    with time_machine.travel(when, tick=False):
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (sid, task, iso_z_now()),
        )
        con.commit()
        return int(cur.lastrowid or 0)


def test_sync_orphan_threshold_is_short_and_reaps_at_10_min():
    """A 'sync' row is reaped after the per-task 10-min cutoff (not the
    60-min default) so a stuck/leaked sync can't freeze ingestion for an
    hour. After reaping the only running row, start_cron_run proceeds and
    returns a fresh id (no 'already running')."""
    import datetime as _dt

    import time_machine

    sid = "svc-sync-threshold"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, "sync", t0)

    # 11 min later: the sync row is past the 10-min cutoff → reaped, and a
    # new run starts cleanly.
    with time_machine.travel(t0 + _dt.timedelta(minutes=11), tick=False):
        new_id = metadata_db.start_cron_run(sid, "sync")

    assert new_id and new_id != old_id
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


def test_live_lease_blocks_start_even_when_cron_row_is_old():
    """The orphan decision is the lease heartbeat, not the cron row's age: a
    'sync' row started 11 minutes ago whose lease heartbeated 30 s ago is a
    healthy long-running sync, so a concurrent start is refused. This is what
    stops the heartbeat model from double-running healthy syncs."""
    import datetime as _dt

    import time_machine

    sid = "svc-sync-live-lease"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    row_id = _seed_running_at(sid, "sync", t0)
    now = t0 + _dt.timedelta(minutes=11)
    _seed_lease(sid, "sync", (now - _dt.timedelta(seconds=30)).timestamp(), ttl_s=60)

    with time_machine.travel(now, tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "sync")

    status = metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (row_id,)).fetchone()
    assert status["status"] == "running"


@pytest.mark.parametrize("task", ["sync", "commit"])
def test_lease_less_running_row_is_reaped_immediately_for_any_task(task):
    """A 'running' cron row with NO backing lease is an orphan (its owner died
    without a heartbeat) and is reaped on the next start regardless of age or
    task — there is no per-task 10/60-minute age cutoff any more."""
    import datetime as _dt

    import time_machine

    sid = f"svc-no-lease-{task}"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, task, t0)

    with time_machine.travel(t0 + _dt.timedelta(seconds=5), tick=False):
        new_id = metadata_db.start_cron_run(sid, task)

    con = metadata_db.get_con(sid)
    assert new_id and new_id != old_id
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


# ── finalize_cron_run_if_running: idempotent terminal backstop ───────────────


def test_finalize_flips_running_row_to_error():
    sid = "svc-finalize-1"
    rid = _seed_running(sid, "sync")

    flipped = metadata_db.finalize_cron_run_if_running(
        sid, "sync", rid, summary="no terminal event", error_message="orphaned"
    )

    assert flipped is True
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, error_message, summary FROM cron_runs WHERE id = ?", (rid,))
        .fetchone()
    )
    assert row["status"] == "error"
    assert row["error_message"] == "orphaned"
    assert row["summary"] == "no terminal event"


def test_finalize_is_noop_on_terminal_row():
    """Must not clobber a row that already reached 'success' (the common
    case — finalize runs in a finally AFTER a successful log_cron_run)."""
    sid = "svc-finalize-2"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()
    rid = int(con.execute("SELECT id FROM cron_runs WHERE status='success'").fetchone()["id"])

    flipped = metadata_db.finalize_cron_run_if_running(sid, "sync", rid)

    assert flipped is False
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()["status"] == "success"


def test_finalize_noop_when_run_id_missing():
    sid = "svc-finalize-3"
    metadata_db.get_con(sid)  # init schema
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", None) is False
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", 999999) is False
