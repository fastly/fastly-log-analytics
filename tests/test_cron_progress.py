"""Tests for ``backend.cron_progress`` — in-memory cron run progress tracker.

The frontend's sync-status badge polls this via SSE to render "Ingesting
file X of Y…" progress. It's an in-memory module (lost on restart) and
the orphan-reaping logic in ``metadata_db.reap_running_cron_runs`` was
the production fix for the "Loading logs..." stuck-sync bug. This file
pins the in-memory side of that flow.
"""

from __future__ import annotations

import time

import pytest

from backend import cron_progress


@pytest.fixture(autouse=True)
def _reset_progress_state():
    """Clear the module-level dicts between tests — they're process-global.

    Includes ``_terminal_run_ids`` (the cron_runs terminal-state memo added
    in perf commit 2e29ac3). Without clearing it, a test that observed
    run_id=N as terminal would short-circuit subsequent tests that reuse
    the same run_id with a non-terminal mock.
    """
    cron_progress._progress.clear()
    cron_progress._last_update.clear()
    cron_progress._run_metadata.clear()
    cron_progress._terminal_run_ids.clear()
    yield
    cron_progress._progress.clear()
    cron_progress._last_update.clear()
    cron_progress._run_metadata.clear()
    cron_progress._terminal_run_ids.clear()


# ── start_progress + add_progress ────────────────────────────────────────────


def test_start_progress_initialises_empty_event_list():
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    assert cron_progress._progress[1] == []
    meta = cron_progress._run_metadata[1]
    assert meta["service_id"] == "svc-a"
    assert meta["task"] == "sync"
    # started_at must be populated so the System Health card can sort by age
    # and surface "running for 3m". Previously the field was missing and
    # always rendered as null.
    assert isinstance(meta["started_at"], float)
    assert meta["started_at"] <= time.time()


def test_start_progress_is_idempotent():
    """A duplicate ``start_progress(1, ...)`` must NOT clobber existing
    events or metadata — the cron scheduler may call start more than
    once for the same run during retries."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "msg": "one"})
    cron_progress.start_progress(1, service_id="other", task="other")

    assert len(cron_progress._progress[1]) == 1  # Existing event preserved
    assert cron_progress._run_metadata[1]["service_id"] == "svc-a"  # Original metadata preserved


def test_add_progress_appends_event():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.add_progress(1, {"type": "step", "i": 2})

    assert cron_progress._progress[1] == [{"type": "step", "i": 1}, {"type": "step", "i": 2}]


def test_add_progress_silently_drops_unknown_run():
    """``add_progress`` for an unstarted run is a no-op (no KeyError) —
    pinned because callers don't always check the run started first."""
    cron_progress.add_progress(999, {"type": "step"})  # must not raise
    assert 999 not in cron_progress._progress


def test_add_progress_advances_last_update_timestamp(monkeypatch):
    """The TTL-based ``cleanup_progress`` keys on ``_last_update``, so
    every event must refresh the timestamp — otherwise an active run
    that's been emitting events for >1h would still get reaped.

    Drives ``time.time`` with a monotonic step counter instead of
    ``time.sleep(0.01)`` so the assertion is deterministic under loaded
    CI runners (where 10 ms of wall clock isn't guaranteed to elapse
    between calls under preemption).
    """
    ticks = iter([1_700_000_000.0, 1_700_000_001.0])
    monkeypatch.setattr(cron_progress.time, "time", lambda: next(ticks))

    cron_progress.start_progress(1)
    initial_ts = cron_progress._last_update[1]
    cron_progress.add_progress(1, {"type": "step"})
    assert cron_progress._last_update[1] > initial_ts


# ── get_progress ─────────────────────────────────────────────────────────────


def test_get_progress_returns_full_list_when_start_idx_zero():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"i": 1})
    cron_progress.add_progress(1, {"i": 2})

    assert cron_progress.get_progress(1) == [{"i": 1}, {"i": 2}]


def test_get_progress_returns_slice_from_start_idx():
    """The SSE endpoint polls with the last-seen index so it doesn't
    re-send events the client already has."""
    cron_progress.start_progress(1)
    for i in range(5):
        cron_progress.add_progress(1, {"i": i})

    assert cron_progress.get_progress(1, start_idx=3) == [{"i": 3}, {"i": 4}]


def test_get_progress_returns_none_for_unknown_run():
    """Distinct from ``[]`` (which means "no new events yet"); ``None``
    tells the SSE handler to emit a terminal 'run not found' message."""
    assert cron_progress.get_progress(999) is None


def test_get_progress_returns_copy_not_live_reference():
    """The caller iterates the result outside the lock; if we returned
    the live list, a concurrent ``add_progress`` would mutate it
    mid-iteration and crash with RuntimeError."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"i": 1})

    out = cron_progress.get_progress(1)
    cron_progress.add_progress(1, {"i": 2})
    # The earlier slice must not have grown
    assert out == [{"i": 1}]


# ── get_latest_progress_for_service ──────────────────────────────────────────


def test_latest_progress_returns_last_event_with_task_attached():
    cron_progress.start_progress(1, service_id="svc-a", task="sync_logs")
    cron_progress.add_progress(1, {"type": "step", "msg": "downloading"})

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out["type"] == "step"
    assert out["msg"] == "downloading"
    assert out["task"] == "sync_logs"


def test_latest_progress_returns_none_for_service_with_no_runs():
    """Used by the UI on first load — must not crash on an empty store."""
    assert cron_progress.get_latest_progress_for_service("svc-nothing") is None


def test_latest_progress_skips_completed_runs():
    """A run whose last event is ``done`` or ``error`` is finished; the
    UI should poll the cron_runs table for the final outcome, not show
    a stale step message as if the run were still active."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "done", "rows": 100})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


def test_latest_progress_skips_errored_runs():
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "error", "msg": "S3 down"})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


def test_latest_progress_picks_newest_run_when_multiple_active():
    """If two runs for the same service are both active, the higher
    run_id (= newer in our autoincrement scheme) wins. Pinned because
    a regression would let the UI show an old run's step messages."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": "old"})
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "step", "i": "new"})

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out["i"] == "new"


def test_latest_progress_returns_task_only_when_run_has_no_events_yet():
    """A run that just started but hasn't emitted any events yet → return
    ``{"task": ...}`` so the UI shows a "starting" placeholder instead
    of a blank badge."""
    cron_progress.start_progress(7, service_id="svc-a", task="sync_logs")

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out == {"task": "sync_logs"}


def test_latest_progress_ignores_runs_belonging_to_other_services():
    """A run for ``svc-b`` must NOT surface in ``svc-a``'s status badge."""
    cron_progress.start_progress(1, service_id="svc-b", task="sync")
    cron_progress.add_progress(1, {"type": "step"})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


# ── end_progress ─────────────────────────────────────────────────────────────


def test_end_progress_appends_final_event_when_provided():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "step"})
    cron_progress.end_progress(1, {"type": "done", "rows": 42})

    events = cron_progress._progress[1]
    assert events[-1] == {"type": "done", "rows": 42}


def test_end_progress_without_final_event_auto_emits_done_when_last_was_status():
    """REGRESSION: the sync cron path emits status messages (e.g.
    "view refresh: 12ms") as its LAST event before falling through to
    `finally: end_progress(run_id)` with no explicit final event. The
    prior end_progress just updated the timestamp, so list_active_runs'
    `events[-1].type in (done,error)` filter never matched → the run
    showed as in-flight forever (TTL was 1 hour). Production
    accumulated 382 stale entries on the System Health card.

    The fix: when no final_event is given AND the last event isn't
    already terminal, auto-append `{type:"done"}` so list_active_runs
    correctly filters."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "status", "message": "view refresh: 12ms"})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    assert events[-1].get("type") == "done", "end_progress must auto-emit done after a status final"
    # And list_active_runs now filters it out
    assert cron_progress.list_active_runs() == []


def test_end_progress_does_not_double_emit_when_last_was_done():
    """If the caller already added a done event via _log_and_add_progress
    + then called end_progress(run_id), don't append a second done."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "done", "rows": 100})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1, "end_progress must not double-emit done"


def test_end_progress_does_not_double_emit_when_last_was_error():
    """Same as above for error — runs that ended in an error shouldn't
    have a misleading 'done' tacked on by the auto-emit path."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "error", "message": "S3 timeout"})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    error_events = [e for e in events if e.get("type") == "error"]
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(error_events) == 1
    assert len(done_events) == 0


def test_end_progress_with_explicit_final_event_does_not_auto_emit():
    """Caller passing an explicit final_event always wins — the
    auto-emit is only for callers that forgot."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "status", "message": "x"})

    cron_progress.end_progress(1, final_event={"type": "done", "rows": 42})

    events = cron_progress._progress[1]
    assert events[-1] == {"type": "done", "rows": 42}
    # Exactly one done — the explicit one. No auto-emit on top.
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1


def test_end_progress_silently_drops_unknown_run():
    cron_progress.end_progress(999, {"type": "done"})  # must not raise
    assert 999 not in cron_progress._progress


# ── cleanup_progress: TTL eviction ───────────────────────────────────────────


def test_cleanup_progress_removes_entries_older_than_one_hour():
    """The TTL is 3600s; anything older gets dropped. Pinned because a
    leak here would slowly accumulate run state for every cron that
    fires, eventually OOMing the dev server."""
    cron_progress.start_progress(1)
    cron_progress.start_progress(2)

    # Backdate run 1 past the TTL
    cron_progress._last_update[1] = time.time() - 3700

    cron_progress.cleanup_progress()

    assert 1 not in cron_progress._progress
    assert 1 not in cron_progress._last_update
    assert 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._progress  # Recent one survives


def test_cleanup_progress_keeps_entries_at_boundary():
    """An entry exactly at the boundary (3600s ago) should still be kept
    — the check is ``> 3600``, not ``>=``. Pinned so a future > → >=
    refactor is forced through this test."""
    cron_progress.start_progress(1)
    cron_progress._last_update[1] = time.time() - 3500  # well under TTL

    cron_progress.cleanup_progress()
    assert 1 in cron_progress._progress


def test_cleanup_progress_is_safe_on_empty_state():
    cron_progress.cleanup_progress()  # must not raise


# ── Concurrency: lock acquisition under racing writers ──────────────────────


# ── list_active_runs ─────────────────────────────────────────────────────────


def test_list_active_runs_returns_only_runs_without_terminal_event():
    """REGRESSION: the admin health-snapshot endpoint was iterating
    ``_run_metadata`` directly, which kept completed runs visible for an
    hour (the cleanup TTL). The System Health card rendered dozens of
    duplicated "sync · KLJPUtJk" boxes. ``list_active_runs`` filters out
    runs whose last event is ``done``/``error`` so only actually-active
    runs are returned."""
    # Active: started, no terminal event.
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    # Completed: started, last event is done.
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "done", "rows": 100})
    # Errored: started, last event is error.
    cron_progress.start_progress(3, service_id="svc-b", task="metadata_sync")
    cron_progress.add_progress(3, {"type": "error", "msg": "S3 timeout"})
    # Active second service.
    cron_progress.start_progress(4, service_id="svc-b", task="commit")

    out = cron_progress.list_active_runs()
    out_by_id = {entry["run_id"]: entry for entry in out}

    assert 1 in out_by_id
    assert 4 in out_by_id
    assert 2 not in out_by_id, "completed run must be filtered out"
    assert 3 not in out_by_id, "errored run must be filtered out"


def test_list_active_runs_includes_metadata_fields():
    """The admin endpoint needs service_id + task + started_at to render
    the in-flight badge. Pin the shape so a future metadata field rename
    doesn't break the wire format."""
    cron_progress.start_progress(7, service_id="svc-a", task="sync")
    out = cron_progress.list_active_runs()
    assert len(out) == 1
    entry = out[0]
    assert entry["run_id"] == 7
    assert entry["service_id"] == "svc-a"
    assert entry["task"] == "sync"
    assert isinstance(entry["started_at"], float)


def test_list_active_runs_returns_empty_when_no_runs():
    assert cron_progress.list_active_runs() == []


def test_list_active_runs_filters_zombie_runs_older_than_5min():
    """REGRESSION: APScheduler can recycle worker threads mid-cron
    (interpreter shutdown, OOM kill, executor restart) without giving
    the try/finally `end_progress` block a chance to fire. The result
    was 32 stale 'sync' entries on the System Health card from a
    single executor incident on 2026-06-03.

    list_active_runs now treats any run whose _last_update is >5 min
    old as a zombie and filters it out — covers all the paths where
    end_progress never ran."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status", "msg": "x"})
    # Backdate the last update past the staleness band
    cron_progress._last_update[1] = time.time() - 400

    out = cron_progress.list_active_runs()
    assert out == [], "zombie run must be filtered out"


def test_list_active_runs_filters_runs_db_marked_success():
    """REGRESSION (2026-06-03): production saw 13 in-flight 'sync'
    entries in _run_metadata where the corresponding cron_runs row
    was already DB-marked status='success' (dur 2-6s). The watchdog
    thread that runs each cron occasionally abandons the worker on
    timeout-edge or interpreter shutdown without firing the
    try/finally end_progress block. DB log_cron_run writes still land
    (they happen INSIDE the cron function), but the in-memory dict
    is left stale.

    Fix: list_active_runs cross-checks each candidate against
    metadata_db.get_cron_run_status() — if the persisted status is
    terminal, the in-memory ghost is filtered out regardless of its
    event tail."""
    from unittest.mock import patch

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status", "msg": "ingesting"})
    # _last_update is fresh (just added an event) so the staleness
    # filter does NOT kick in — this run looks "active" by the in-memory
    # signal but the DB has already marked it success.

    with patch("backend.core.metadata.get_cron_run_status", return_value="success"):
        out = cron_progress.list_active_runs()
    assert out == [], "DB-success run must be filtered even when in-memory says active"


def test_list_active_runs_does_not_filter_db_missing_or_running():
    """If the DB row is missing (transient) or still 'running', trust
    the in-memory signal. The cross-check is a backstop, not a gate —
    we'd rather show a false in-flight than miss a real one."""
    from unittest.mock import patch

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status"})

    with patch("backend.core.metadata.get_cron_run_status", return_value=None):
        out = cron_progress.list_active_runs()
    assert len(out) == 1
    with patch("backend.core.metadata.get_cron_run_status", return_value="running"):
        out = cron_progress.list_active_runs()
    assert len(out) == 1


def test_reap_zombie_runs_evicts_stale_entries_and_returns_count():
    """reap_zombie_runs actively prunes _run_metadata so callers that
    walk the dict directly (admin.py:210/238/1022 — there's a precedent
    for regressions here) also see the cleaned state."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status"})
    cron_progress._last_update[1] = time.time() - 400  # zombie

    cron_progress.start_progress(2, service_id="svc-a", task="sync")  # active

    n = cron_progress.reap_zombie_runs()
    assert n == 1
    assert 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._run_metadata
    # Zombie got a synthetic error event so SSE subscribers see termination
    last_event = cron_progress._progress[1][-1]
    assert last_event.get("type") == "error"
    assert "zombie" in last_event.get("message", "").lower()


def test_concurrent_add_progress_does_not_lose_events():
    """The shared lock must serialise writers. Two threads each
    appending 100 events → final list has all 200, no torn writes."""
    import threading

    cron_progress.start_progress(1)

    def writer(prefix: str):
        for i in range(100):
            cron_progress.add_progress(1, {"thread": prefix, "i": i})

    t1 = threading.Thread(target=writer, args=("A",))
    t2 = threading.Thread(target=writer, args=("B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(cron_progress._progress[1]) == 200
