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

from backend.core import metadata as metadata_db


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


# ── time-machine: pin the start_cron_run 60-minute orphan boundary ──────────


def test_start_cron_run_reaps_rows_exactly_at_orphan_threshold():
    """``start_cron_run`` reaps rows older than ``_ORPHAN_THRESHOLD_MINS``
    (60 min) but leaves younger rows alone.

    Pinning this boundary deterministically requires time-travel: stamp
    a row's ``started_at`` at T=0, jump 61 min forward, call
    ``start_cron_run`` and verify the OLD row got reaped while a
    YOUNGER (59 min) row stays. Without freezing the clock the test
    either:
      (a) waits 61 real minutes (intolerable), or
      (b) backdates the row insert by ALREADY 61 min, which exercises
          the SQL but bypasses the production code path where the
          ``time_cutoff`` is computed from ``datetime.now(UTC)`` in
          backend/core/metadata/cron_log.py:29.

    time-machine swaps Python's ``datetime.now`` at the C-extension
    layer so the cron_log.py code observes the travelled time without
    any instrumentation of the SUT. Note that SQLite's ``datetime('now')``
    is NOT affected — so we stamp ``started_at`` via Python's
    ``iso_z_now()`` (matches what production cron_log.py does), not via
    raw SQL ``datetime('now')``.
    """
    import datetime as _dt

    import time_machine

    from backend.core.metadata.base import _ORPHAN_THRESHOLD_MINS
    from backend.utils.date_utils import iso_z_now

    sid = "svc-reap-boundary"
    threshold = _dt.timedelta(minutes=_ORPHAN_THRESHOLD_MINS)
    t0 = _dt.datetime(2026, 6, 16, 12, 0, 0, tzinfo=_dt.UTC)

    def _seed_at_python_now(task: str) -> int:
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, 0.0, 'running', '[]')",
            (task, iso_z_now()),
        )
        con.commit()
        return int(cur.lastrowid or 0)

    # The reap is task-scoped (WHERE task = ? AND started_at < cutoff).
    # Use 'commit' here — a task that keeps the 60-min DEFAULT cutoff (the
    # 'sync' task now overrides to 10 min, covered by its own tests below).
    # Seed BOTH boundary rows under the SAME task — what we're really
    # pinning is the timestamp boundary, not cross-task behavior. A third
    # row under a DIFFERENT task proves the reap doesn't cross tasks.

    # T=0 → will be 61 min old after travel.
    with time_machine.travel(t0, tick=False):
        old_id = _seed_at_python_now("commit")
    # T+2min → will be only 59 min old after travel; same task.
    with time_machine.travel(t0 + _dt.timedelta(minutes=2), tick=False):
        young_same_task_id = _seed_at_python_now("commit")
    # T+2min → DIFFERENT task; must NEVER be touched (cross-task isolation).
    with time_machine.travel(t0 + _dt.timedelta(minutes=2), tick=False):
        cross_task_id = _seed_at_python_now("optimize")

    # Travel past the 60-min threshold for the OLD commit row only.
    with time_machine.travel(t0 + threshold + _dt.timedelta(minutes=1), tick=False):
        # ``start_cron_run`` reaps orphans for the SAME task it's
        # starting. The young 'commit' row is exactly 59 min old → keep.
        # The old 'commit' row is 61 min old → reap. The 'optimize' row is
        # a different task → must not be touched even though it would be
        # young anyway.
        # The young commit row is still 'running', so start_cron_run will
        # raise (already running) — catch and inspect cron_runs state.
        with __import__("pytest").raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "commit")

    con = metadata_db.get_con(sid)
    rows = con.execute(
        "SELECT id, task, status FROM cron_runs WHERE id IN (?, ?, ?) ORDER BY id",
        (old_id, young_same_task_id, cross_task_id),
    ).fetchall()
    assert len(rows) == 3
    by_id = {r["id"]: r["status"] for r in rows}
    assert by_id[old_id] == "error", f"commit row at t0 (61 min before reap) should be reaped; got {by_id[old_id]!r}"
    assert by_id[young_same_task_id] == "running", (
        f"commit row at t0+2min (59 min before reap) should NOT be reaped; got {by_id[young_same_task_id]!r}"
    )
    assert by_id[cross_task_id] == "running", (
        f"different-task row should not be touched by commit reap; got {by_id[cross_task_id]!r}"
    )


# ── per-task orphan threshold: 'sync' uses a SHORT cutoff (incident 2026-06-19) ─


def _seed_running_at(sid: str, task: str, when) -> int:
    """Insert a 'running' row stamped at the (time-machine) ``when`` instant."""
    import time_machine

    from backend.utils.date_utils import iso_z_now

    with time_machine.travel(when, tick=False):
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, 0.0, 'running', '[]')",
            (task, iso_z_now()),
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

    from backend.core.metadata.base import _TASK_ORPHAN_THRESHOLD_MINS

    assert _TASK_ORPHAN_THRESHOLD_MINS.get("sync") == 10

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


def test_sync_row_under_threshold_still_blocks():
    """A 'sync' row only 9 min old (< 10-min cutoff) is NOT reaped, so a
    concurrent start is correctly refused — proving the cutoff didn't go
    to zero and start double-running healthy syncs."""
    import datetime as _dt

    import pytest
    import time_machine

    sid = "svc-sync-under"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    _seed_running_at(sid, "sync", t0)

    with time_machine.travel(t0 + _dt.timedelta(minutes=9), tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "sync")


def test_non_sync_task_keeps_60_min_default():
    """Tasks without a per-task override keep the 60-min cutoff: a 'commit'
    row 11 min old is NOT reaped (would be, under sync's 10-min rule)."""
    import datetime as _dt

    import pytest
    import time_machine

    sid = "svc-commit-default"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    _seed_running_at(sid, "commit", t0)

    with time_machine.travel(t0 + _dt.timedelta(minutes=11), tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "commit")


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


# ── _retry_on_locked: transient "database is locked" must not crash a tick ───
#
# Cron jobs (sync/commit/local-compact/metadata_sync) converge on the same
# minute boundary and write one per-service metadata.db. busy_timeout absorbs
# ordinary queueing, but an immediate SQLITE_BUSY (WAL snapshot conflict, or a
# checkpoint that couldn't drain on a full disk — the 2026-06-23 incident)
# surfaces as "database is locked" and used to crash the whole cron tick with a
# traceback. The bookkeeping writes now roll back and retry. (svc names here
# don't matter — these exercise the helper, not real DBs.)


def test_retry_on_locked_returns_value_on_first_success():
    from backend.core.metadata.cron_log import _retry_on_locked

    class _FakeCon:
        def rollback(self):  # pragma: no cover - never called on the happy path
            raise AssertionError("rollback must not run when fn succeeds")

    assert _retry_on_locked(_FakeCon(), lambda: 42) == 42


def test_retry_on_locked_retries_then_succeeds(monkeypatch):
    import sqlite3

    from backend.core.metadata import cron_log

    monkeypatch.setattr(cron_log.time, "sleep", lambda *_a, **_k: None)

    class _FakeCon:
        def __init__(self):
            self.rollbacks = 0

        def rollback(self):
            self.rollbacks += 1

    con = _FakeCon()
    state = {"n": 0}

    def fn():
        state["n"] += 1
        if state["n"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    assert cron_log._retry_on_locked(con, fn) == "ok"
    assert state["n"] == 3
    assert con.rollbacks == 2  # one rollback before each of the two retries


def test_retry_on_locked_reraises_after_exhausting_attempts(monkeypatch):
    import sqlite3

    import pytest

    from backend.core.metadata import cron_log

    monkeypatch.setattr(cron_log.time, "sleep", lambda *_a, **_k: None)

    class _FakeCon:
        def rollback(self):
            pass

    attempts = {"n": 0}

    def always_locked():
        attempts["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="locked"):
        cron_log._retry_on_locked(_FakeCon(), always_locked)
    assert attempts["n"] == cron_log._LOCKED_RETRY_ATTEMPTS


def test_retry_on_locked_does_not_retry_other_operational_errors():
    import sqlite3

    import pytest

    from backend.core.metadata import cron_log

    class _FakeCon:
        def __init__(self):
            self.rollbacks = 0

        def rollback(self):  # pragma: no cover - a non-lock error must not retry
            self.rollbacks += 1

    con = _FakeCon()
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise sqlite3.OperationalError("no such table: cron_runs")

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        cron_log._retry_on_locked(con, fn)
    assert calls["n"] == 1  # immediate re-raise, no retry/backoff
    assert con.rollbacks == 0


def test_start_cron_run_survives_a_transient_lock_without_double_insert(monkeypatch):
    """The exact failure from the incident: the orphan-reap UPDATE at the top
    of start_cron_run raises "database is locked" once. It must retry and still
    insert EXACTLY ONE running row (the rollback means the redo can't duplicate).
    """
    import sqlite3

    from backend.core.metadata import cron_log

    sid = "svc-retry-start"
    con = metadata_db.get_con(sid)  # init schema
    monkeypatch.setattr(cron_log.time, "sleep", lambda *_a, **_k: None)

    orig_execute = con.execute
    calls = {"n": 0}

    def flaky_execute(sql, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:  # the reap UPDATE — first statement of the unit
            raise sqlite3.OperationalError("database is locked")
        return orig_execute(sql, *args, **kwargs)

    monkeypatch.setattr(con, "execute", flaky_execute, raising=False)

    rid = metadata_db.start_cron_run(sid, "sync")

    assert rid
    assert calls["n"] == 4, "expected reap(raises)→reap→count→insert; got a different statement sequence"
    # Read back through the captured real execute (pytest restores the shadow at
    # teardown). Exactly one row proves the rollback+redo didn't double-insert.
    rows = orig_execute("SELECT id, status FROM cron_runs WHERE task = 'sync'").fetchall()
    assert len(rows) == 1, f"transient-lock retry must not double-insert; got {len(rows)} rows"
    assert rows[0]["status"] == "running"
