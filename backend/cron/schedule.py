"""Helper that assembles the /api/cron-schedule payload.

Extracted from ``backend.routers.services.core.api_cron_schedule`` so
the /logs cron tile and the bootstrap response can share the same
assembly without a router-to-router import. The route in
``services/core.py`` still owns the URL contract + the short TTL cache;
this module is the pure data builder both call paths use.
"""

from __future__ import annotations


def build_cron_schedule_payload(source: dict) -> dict:
    """Return ``{"schedules": [...]}`` for the given ``source``.

    Pulls APScheduler jobs via ``backend.cron.scheduler.get_scheduler``,
    enriches each with ``latest_cron_per_task`` history from
    ``metadata_db``, and tags the alerts tile with ``disabled_reason``
    when no alerts are configured. Safe to call without a request
    scope — ``source`` is the per-service dict bootstrap already
    resolves via ``_db.get_source_for_service``.
    """
    from backend.core import metadata as metadata_db
    from backend.cron.scheduler import get_scheduler

    sched = get_scheduler()
    service_id = source["name"]
    last_runs: dict[str, dict] = {}
    try:
        per_task = metadata_db.latest_cron_per_task(service_id)
        for task, info in per_task.items():
            last_runs[task] = {
                "last_run_time": info["started_at"],
                "last_run_status": info["status"],
                "last_run_duration_s": info["duration_s"],
                "last_run_summary": info["summary"],
            }
    except Exception:
        pass
    _TASK_MAP = {
        "sync_metadata": "metadata_sync",
        "sync": "sync",
        "rum_sync": "rum_sync",
        "full_sync": "full_sync",
        "gap_heal": "gap_heal",
        "commit": "commit",
        "rum_commit": "rum_commit",
        "optimize": "optimize",
        "local_compact": "local_compact",
        "expire": "expire",
        "alerts_evaluation": "alerts",
        "ngwaf_sync": "ngwaf_sync",
        "metadata_cleanup": "metadata_cleanup",
        "insights_prewarmer": "insights_prewarmer",
    }
    schedules = []
    for job in sched._sched.get_jobs():
        job_id = getattr(job, "id", "")
        if not job_id.endswith(f"_{service_id}"):
            continue
        if job_id.startswith("initial_sync"):
            continue
        task_name = job_id[: -len(f"_{service_id}")]
        db_task = _TASK_MAP.get(task_name)
        if db_task is None:
            continue
        from backend.utils.date_utils import iso_z

        next_run = iso_z(job.next_run_time) if job.next_run_time else None
        schedules.append({"task": db_task, "next_run_time": next_run, **last_runs.get(db_task, {})})
    existing = {s["task"] for s in schedules}
    for task, info in last_runs.items():
        if task not in existing and task in _TASK_MAP.values():
            schedules.append({"task": task, "next_run_time": None, **info})

    # Mark the alerts tile as "No alerts configured" when no alerts exist.
    # Two cases:
    #  1. The cron is unregistered AND there are no historical runs → synthesize
    #     a fresh placeholder so the UI tile doesn't silently vanish.
    #  2. The cron is unregistered but historical runs exist → the loop above
    #     already added an alerts entry with next_run_time=None; tag it with
    #     disabled_reason so the UI renders "No alerts configured" instead of
    #     the ambiguous "Next: Disabled" fallback.
    # Once an alert is created the alerts router calls scheduler.reload(), which
    # re-registers the job and overwrites this placeholder with a live entry.
    try:
        if metadata_db.count_alerts(service_id) == 0:
            alerts_entry = next((s for s in schedules if s["task"] == "alerts"), None)
            if alerts_entry is None:
                schedules.append(
                    {
                        "task": "alerts",
                        "next_run_time": None,
                        "disabled_reason": "no_alerts_configured",
                    }
                )
            elif alerts_entry.get("next_run_time") is None:
                alerts_entry["disabled_reason"] = "no_alerts_configured"
    except Exception:
        pass

    return {"schedules": schedules}
