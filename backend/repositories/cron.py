"""Repository for cron run history.

Storage lives in per-service SQLite via ``backend.core.metadata``.
"""

from __future__ import annotations

from backend.core import metadata as metadata_db


def get_cron_logs(
    service_id: str,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
    *,
    skip_total: bool = False,
) -> tuple[int, list[dict]]:
    # Delta polls (since_id is not None) never need the precount — the
    # /logs page only renders `total` on the full-history path. Skip the
    # count(*) when delta-polling so the read isn't competing with the
    # writer-side lock burst that delta polls trigger.
    # The badge consumers (useLastSync — per_page=1, task=sync) also
    # never read `total`; they pass skip_total=true so the count(*)
    # FROM cron_runs WHERE task=? (a 200-330 ms scan on a busy
    # service) drops to a single LIMIT 1 fetch.
    return metadata_db.get_cron_runs(
        service_id,
        task=task,
        status=status,
        page=page,
        per_page=per_page,
        sort_col=sort_col,
        sort_dir=sort_dir,
        since_id=since_id,
        with_total=since_id is None and not skip_total,
    )


def delete_cron_log(service_id: str, log_id: int) -> None:
    metadata_db.delete_cron_run(service_id, log_id)


def purge_cron_logs(service_id: str, task: str | None = None, days: int | None = None) -> None:
    metadata_db.purge_cron_runs(service_id, task=task, days=days)
