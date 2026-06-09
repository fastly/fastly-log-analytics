"""Repository for cron run history.

Storage lives in per-service SQLite via ``backend.core.metadata_db``.
"""

from __future__ import annotations

from backend.core import metadata_db


def get_cron_logs(
    service_id: str,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
) -> tuple[int, list[dict]]:
    return metadata_db.get_cron_runs(
        service_id,
        task=task,
        status=status,
        page=page,
        per_page=per_page,
        sort_col=sort_col,
        sort_dir=sort_dir,
        since_id=since_id,
    )


def delete_cron_log(service_id: str, log_id: int) -> None:
    metadata_db.delete_cron_run(service_id, log_id)


def purge_cron_logs(service_id: str, task: str | None = None, days: int | None = None) -> None:
    metadata_db.purge_cron_runs(service_id, task=task, days=days)
