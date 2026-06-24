import threading
import time

_progress: dict[int, list[dict]] = {}
_last_update: dict[int, float] = {}
_run_metadata: dict[int, dict] = {}
_lock = threading.Lock()

# Run IDs we've already confirmed are in a terminal DB state. Once a
# cron_runs row reads ``status IN ('success', 'error')`` it never goes
# back, so the SQLite check per run_id per list_active_runs() call is
# pure waste from the second invocation onwards. The /admin page polls
# at 5 s (was 1 s before perf item #12 landed) and the snapshot can
# contain 100+ candidates; the audit measured 422 cron_runs SELECTs
# per page load before this cache went in.
_terminal_run_ids: set[int] = set()
_TERMINAL_CACHE_CAP = 4096


def start_progress(run_id: int | None, service_id: str | None = None, task: str | None = None):
    # run_id can be None when start_cron_run failed to register the run
    # in per-service SQLite (e.g. table write contention). The cron job
    # still runs; progress tracking is a no-op for that run.
    if run_id is None:
        return
    with _lock:
        if run_id not in _progress:
            now = time.time()
            _progress[run_id] = []
            _last_update[run_id] = now
            _run_metadata[run_id] = {
                "service_id": service_id,
                "task": task,
                "started_at": now,
            }


_STALE_AFTER_SECONDS = 300  # 5 min — covers slow syncs, kills zombie entries


def list_active_runs() -> list[dict]:
    """Return metadata for runs that are GENUINELY in flight.

    A run is considered active when ALL of these hold:
      1. It's in ``_run_metadata`` (was started_progress'd)
      2. Its last progress event is NOT terminal (done/error)
      3. Its ``_last_update`` was within the last 5 minutes
      4. The persisted ``cron_runs.status`` is still ``'running'``

    Condition (4) is the DB-truth backstop: when an APScheduler
    watchdog abandons a worker thread (interpreter shutdown, OOM
    kill, executor recycle) or some other path completes ``log_cron_run``
    without firing the in-memory ``end_progress``, the in-memory dict
    falsely shows the run as in-flight even though the DB knows it
    succeeded. Production observed 13+ such ghosts on 2026-06-03
    after a backend restart — DB rows said ``status='success'`` with
    durations of 2-6 seconds while the in-memory dict held them as
    active for 100+ seconds. Cross-checking against the DB gives a
    correct answer regardless of what happened to the in-memory
    state.

    Condition (3) covers the residual: a run whose DB write also got
    skipped (something crashed before ``log_cron_run``). After 5 min
    of zero progress, we declare it a zombie regardless.
    """
    now = time.time()
    with _lock:
        candidates = []
        for run_id, meta in _run_metadata.items():
            events = _progress.get(run_id) or []
            if events and events[-1].get("type") in ("done", "error"):
                continue
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                continue
            candidates.append((run_id, meta))

    # DB cross-check happens OUTSIDE the lock so a slow SQLite call
    # doesn't block other progress operations. Short-circuit on the
    # _terminal_run_ids memo first — terminal status never reverts, so
    # the per-poll cron_runs SELECT only needs to run once per run_id
    # over the process's lifetime (the audit measured 422 of these per
    # /admin load before this).
    out = []
    for run_id, meta in candidates:
        if run_id in _terminal_run_ids:
            continue
        if _db_status_is_terminal(meta.get("service_id"), run_id):
            if len(_terminal_run_ids) < _TERMINAL_CACHE_CAP:
                _terminal_run_ids.add(run_id)
            continue
        entry = {"run_id": run_id}
        entry.update(meta)
        out.append(entry)
    return out


def _db_status_is_terminal(service_id: str | None, run_id: int) -> bool:
    """Return True if the cron_runs row for this run_id has a terminal
    status ('success' or 'error') in per-service SQLite.

    Best-effort: any DB error (missing service, table not yet created,
    SQLite locked) returns False so the in-memory truth still serves
    the badge (we'd rather show one false-in-flight than hide a
    genuinely running one).
    """
    if not service_id:
        return False
    try:
        from backend.core import metadata as metadata_db

        status = metadata_db.get_cron_run_status(service_id, run_id)
        return status in ("success", "error")
    except Exception:
        return False


def reap_zombie_runs() -> int:
    """Eagerly evict zombie run metadata from in-memory state.

    Mirrors list_active_runs' staleness check but actually mutates
    the dicts. Called from the scheduler's per-tick cleanup so
    /admin/health-snapshot doesn't drift by minutes between sync
    ticks. Returns the count evicted for log telemetry.

    Why this and not just rely on cleanup_progress's 1-hour TTL: a
    zombie sync that ran for 2 minutes then died leaves a stale entry
    that's still <1h old. cleanup_progress wouldn't touch it.
    list_active_runs filters the badge but the entry still bloats
    _run_metadata and shows up in any other code path that walks
    the dict (admin.py:210/238/1022 — patched 2026-06-02 but easy
    to regress).
    """
    now = time.time()
    evicted = 0
    with _lock:
        for run_id in list(_run_metadata.keys()):
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                events = _progress.get(run_id) or []
                # Stale + no terminal event = zombie. Append a synthetic
                # error so any SSE subscriber sees the run ended.
                if not events or events[-1].get("type") not in ("done", "error"):
                    _progress.setdefault(run_id, []).append(
                        {"type": "error", "message": "scheduler reaped zombie cron (no progress in 5m)"}
                    )
                _run_metadata.pop(run_id, None)
                evicted += 1
        return evicted


def add_progress(run_id: int | None, event: dict):
    # See start_progress for why run_id can be None — no-op in that case.
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            _progress[run_id].append(event)
            _last_update[run_id] = time.time()


def get_progress(run_id: int | None, start_idx: int = 0, service_id: str | None = None) -> list[dict] | None:
    if run_id is None:
        return None
    with _lock:
        if run_id not in _progress:
            return None
        if service_id and _run_metadata.get(run_id, {}).get("service_id") != service_id:
            return None
        # Return a copy of the slice to avoid race conditions when the caller iterates over it
        return list(_progress[run_id][start_idx:])


def get_latest_progress_for_service(service_id: str) -> dict | None:
    """Return the latest progress event for any active run belonging to this service."""
    with _lock:
        # Find active runs for this service
        active_runs = []
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") == service_id and run_id in _progress:
                events = _progress[run_id]
                # If the last event is "done" or "error", this run is no longer active
                if events and events[-1].get("type") in ("done", "error"):
                    continue
                active_runs.append(run_id)

        if not active_runs:
            return None

        # Take the newest one
        run_id = max(active_runs)
        if _progress[run_id]:
            # Return the last event + the task type
            latest = _progress[run_id][-1].copy()
            latest["task"] = _run_metadata[run_id].get("task")
            return latest
        return {"task": _run_metadata[run_id].get("task")}


def end_progress(run_id: int | None, final_event: dict | None = None):
    """Mark a cron run as ended.

    AUTO-DONE: if no ``final_event`` is provided AND the run's last
    event isn't already a terminal type ("done"/"error"), automatically
    append a ``{"type": "done"}`` event so ``list_active_runs`` can
    filter the run out. Without this, callers that emit only "status"
    events during their lifetime (the sync path's view-refresh message
    is the canonical example) leave the run "active" until the 1-hour
    TTL — accumulating dozens of stale entries on the System Health card.

    Explicit callers that want a richer terminal event can still pass
    ``final_event={"type": "done", "rows": N}`` and the same append path
    runs. The auto-emit only kicks in when the caller forgot.
    """
    # See start_progress for why run_id can be None — no-op in that case.
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            events = _progress[run_id]
            last_type = events[-1].get("type") if events else None
            if final_event:
                _progress[run_id].append(final_event)
            elif last_type not in ("done", "error"):
                _progress[run_id].append({"type": "done"})
            _last_update[run_id] = time.time()


def cleanup_progress_and_reap():
    """Convenience helper that runs cleanup_progress + reap_zombie_runs.

    The two are always called as a pair from every cron entrypoint
    (7 scheduler functions today). Wrapping them prevents the
    common bug where a new cron runner remembers cleanup but forgets
    the reap — leaving zombie entries in the System Health card.

    Returns the reap count for log telemetry; cleanup_progress's
    return value is None.
    """
    cleanup_progress()
    return reap_zombie_runs()


def cleanup_progress():
    now = time.time()
    with _lock:
        expired = [k for k, v in _last_update.items() if now - v > 3600]  # 1 hour TTL
        for k in expired:
            _progress.pop(k, None)
            _last_update.pop(k, None)
            _run_metadata.pop(k, None)


# R-1: register the cross-test leak-surface globals so the autouse
# fixture in tests/conftest.py drains them via CacheRegistry.clear_all()
# rather than hand-clearing each one. _lock is left alone (not a
# cache, not state).
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron_progress._progress", _progress)
_CacheRegistry.register("cron_progress._last_update", _last_update)
_CacheRegistry.register("cron_progress._run_metadata", _run_metadata)
_CacheRegistry.register("cron_progress._terminal_run_ids", _terminal_run_ids)
