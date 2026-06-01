import threading
import time

_progress: dict[int, list[dict]] = {}
_last_update: dict[int, float] = {}
_run_metadata: dict[int, dict] = {}
_lock = threading.Lock()


def start_progress(run_id: int, service_id: str = None, task: str = None):
    with _lock:
        if run_id not in _progress:
            _progress[run_id] = []
            _last_update[run_id] = time.time()
            _run_metadata[run_id] = {"service_id": service_id, "task": task}


def add_progress(run_id: int, event: dict):
    with _lock:
        if run_id in _progress:
            _progress[run_id].append(event)
            _last_update[run_id] = time.time()


def get_progress(run_id: int, start_idx: int = 0) -> list[dict] | None:
    with _lock:
        if run_id not in _progress:
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


def end_progress(run_id: int, final_event: dict | None = None):
    with _lock:
        if run_id in _progress:
            if final_event:
                _progress[run_id].append(final_event)
            _last_update[run_id] = time.time()


def cleanup_progress():
    now = time.time()
    with _lock:
        expired = [k for k, v in _last_update.items() if now - v > 3600]  # 1 hour TTL
        for k in expired:
            _progress.pop(k, None)
            _last_update.pop(k, None)
            _run_metadata.pop(k, None)
