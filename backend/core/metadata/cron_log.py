"""Cron-run history + scoring audit in metadata SQLite.

Backs the ``cron_runs`` and ``scoring_audit`` tables. Provides the start /
update / log / purge / reap surface used by the scheduler and the per-task
status summaries used by the sync-status / refresh-config-status endpoints.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta

from backend.core.metadata.base import _ORPHAN_THRESHOLD_MINS, get_con
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def start_cron_run(service_id: str, task: str) -> int:
    """Create a 'running' cron run row, reaping orphans first.

    Raises RuntimeError if a run of the same task is already in progress
    (within the orphan threshold). Returns the new row id.
    """
    con = get_con(service_id)
    started_at = iso_z_now()
    time_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_ORPHAN_THRESHOLD_MINS))

    # Reap orphans first (rows still 'running' but older than the threshold).
    con.execute(
        "UPDATE cron_runs SET status = 'error', "
        "error_message = COALESCE(error_message, 'Process interrupted') "
        "WHERE task = ? AND status = 'running' AND started_at < ?",
        (task, time_cutoff),
    )

    busy = con.execute(
        "SELECT count(*) AS n FROM cron_runs WHERE task = ? AND status = 'running'",
        (task,),
    ).fetchone()
    if busy and busy["n"] > 0:
        con.commit()
        raise RuntimeError(f"Task '{task}' is already running for this service.")

    cur = con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, ?, 0.0, 'running', '[]')",
        (task, started_at),
    )
    con.commit()
    return int(cur.lastrowid or 0)


def log_cron_run(
    service_id: str,
    task: str,
    duration_s: float,
    status: str,
    *,
    error_message: str | None = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list | None = None,
    summary: str | None = None,
    log_output: str | None = None,
    run_id: int | None = None,
) -> None:
    """Update an existing cron_run row by id, or insert a new completed one.

    When ``run_id`` is provided (the common case — start_cron_run created the
    row), this UPDATEs in place. Otherwise INSERTs a fresh terminal row
    (used by paths that didn't go through start_cron_run, e.g. retries).
    """
    con = get_con(service_id)
    started_at = iso_z(datetime.now(UTC) - timedelta(seconds=max(duration_s, 0)))
    keys_json = json.dumps(parquet_keys or [])
    if run_id is not None:
        con.execute(
            """UPDATE cron_runs SET
                duration_s = ?, status = ?, error_message = ?,
                files_downloaded = ?, files_deleted_fos = ?, rows_ingested = ?, corrupt_rows = ?,
                parquet_files_created = ?, parquet_files_optimized = ?,
                parquet_keys = ?, summary = ?, log_output = ?
               WHERE id = ?""",
            (
                duration_s,
                status,
                error_message,
                files_downloaded,
                files_deleted_fos,
                rows_ingested,
                corrupt_rows,
                parquet_files_created,
                parquet_files_optimized,
                keys_json,
                summary,
                log_output,
                run_id,
            ),
        )
    else:
        con.execute(
            """INSERT INTO cron_runs (task, started_at, duration_s, status, error_message,
                files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                parquet_files_created, parquet_files_optimized, parquet_keys, summary, log_output)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task,
                started_at,
                duration_s,
                status,
                error_message,
                files_downloaded,
                files_deleted_fos,
                rows_ingested,
                corrupt_rows,
                parquet_files_created,
                parquet_files_optimized,
                keys_json,
                summary,
                log_output,
            ),
        )
    con.commit()


def update_cron_duration(
    service_id: str,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
) -> None:
    con = get_con(service_id)
    if log_output is None:
        con.execute(
            "UPDATE cron_runs SET duration_s = ? WHERE id = ?",
            (duration_s, run_id),
        )
    else:
        con.execute(
            "UPDATE cron_runs SET duration_s = ?, log_output = ? WHERE id = ?",
            (duration_s, log_output, run_id),
        )
    con.commit()


def delete_cron_run(service_id: str, run_id: int) -> None:
    con = get_con(service_id)
    con.execute("DELETE FROM cron_runs WHERE id = ?", (run_id,))
    con.commit()


def purge_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    days: int | None = None,
) -> None:
    con = get_con(service_id)
    where: list[str] = []
    params: list = []
    if task and task != "all":
        where.append("task = ?")
        params.append(task)
    if days is not None:
        cutoff = iso_z(datetime.now(UTC) - timedelta(days=days))
        where.append("started_at < ?")
        params.append(cutoff)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    con.execute(f"DELETE FROM cron_runs {where_sql}", params)
    con.commit()


def record_scoring_audit(
    service_id: str,
    action: str,
    *,
    actor: str = "operator",
    details: dict | None = None,
) -> None:
    """Append an operator-attribution row to the scoring_audit log.

    Called from every scoring-config-mutating endpoint (enable, disable,
    threshold commit + enforce, retrain, rotate-key, matrix-rollback).
    Best-effort: any SQLite failure is logged at DEBUG and swallowed so
    a busy WAL doesn't block the actual operator action.
    """
    try:
        con = get_con(service_id)
        con.execute(
            "INSERT INTO scoring_audit (service_id, action, actor, details) VALUES (?, ?, ?, ?)",
            (service_id, action, actor, json.dumps(details) if details else None),
        )
        con.commit()
    except sqlite3.Error as e:
        logger.debug("[metadata_db] record_scoring_audit(%s, %s) failed: %s", service_id, action, e)


def list_scoring_audit(
    service_id: str,
    *,
    limit: int = 100,
    since: str | None = None,
) -> list[dict]:
    """Most-recent first. Optional ISO ``since`` timestamp lower bound."""
    try:
        con = get_con(service_id)
        if since:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? AND timestamp >= ? ORDER BY id DESC LIMIT ?",
                (service_id, since, limit),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? ORDER BY id DESC LIMIT ?",
                (service_id, limit),
            ).fetchall()
        out = []
        for r in rows:
            row = dict(r)
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (ValueError, TypeError):
                    pass
            out.append(row)
        return out
    except sqlite3.Error as e:
        logger.debug("[metadata_db] list_scoring_audit(%s) failed: %s", service_id, e)
        return []


def prune_scoring_audit(service_id: str, *, keep_last: int = 10000) -> None:
    """Trim scoring_audit to the most recent ``keep_last`` rows per service.

    Cheap unbounded growth guard — every scoring-config mutation appends
    one row, and the table is only ever read by the admin UI / state_sync
    export which already caps its own page size. Best-effort: any SQLite
    failure is logged at DEBUG and swallowed so trimming never blocks the
    caller (typically a maintenance cron, not the operator hot path).
    """
    try:
        con = get_con(service_id)
        # Tiebreak on id DESC so concurrent inserts that landed in the same
        # `datetime('now')` second are deterministically ordered (otherwise
        # SQLite is free to pick any row from the tied group, which makes
        # prune flaky under burst workloads and breaks reproducibility tests).
        con.execute(
            "DELETE FROM scoring_audit WHERE service_id = ? AND id NOT IN ("
            "SELECT id FROM scoring_audit WHERE service_id = ? ORDER BY timestamp DESC, id DESC LIMIT ?)",
            (service_id, service_id, keep_last),
        )
        con.commit()
    except sqlite3.Error as e:
        logger.debug("[metadata_db] prune_scoring_audit(%s) failed: %s", service_id, e)


def get_cron_run_status(service_id: str, run_id: int) -> str | None:
    """Return the status string for a single cron_runs row, or None if
    the row doesn't exist. Used by cron_progress.list_active_runs to
    cross-check the in-memory state against the DB-of-truth (catches
    abandoned-worker-thread zombies that completed log_cron_run but
    never fired end_progress).

    Narrowed exception scope: catches sqlite3.Error (DB unreachable,
    table missing, locked) and logs at DEBUG so the next 'why isn't
    the cross-check firing?' triage isn't flying blind. Returns None
    on any DB failure so list_active_runs falls back to the in-memory
    signal (we'd rather show a false in-flight than miss a real one).
    """
    try:
        con = get_con(service_id)
        row = con.execute("SELECT status FROM cron_runs WHERE id = ?", (run_id,)).fetchone()
        return row["status"] if row else None
    except sqlite3.Error as e:
        logger.debug("[metadata_db] get_cron_run_status(%s, %s) failed: %s", service_id, run_id, e)
        return None


def get_cron_run_result(service_id: str, run_id: int) -> dict | None:
    """Return ``{status, log_output}`` for a cron_runs row, or ``None`` if
    the row doesn't exist. Used by the SSE progress stream when the
    in-memory progress cache has rolled off (completed/historical runs).

    Distinct from ``get_cron_run_status`` because the SSE stream also
    needs the log_output to replay the run's terminal lines."""
    try:
        con = get_con(service_id)
        row = con.execute("SELECT status, log_output FROM cron_runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return {"status": row["status"], "log_output": row["log_output"]}
    except sqlite3.Error as e:
        logger.debug("[metadata_db] get_cron_run_result(%s, %s) failed: %s", service_id, run_id, e)
        return None


def get_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
    with_total: bool = True,
) -> tuple[int, list[dict]]:
    """Paginated cron run history. Used by repositories/cron.py.

    ``since_id`` enables delta polling: when provided, rows are returned only
    if ``id > since_id`` OR ``status = 'running'``. The ``status = 'running'``
    branch keeps long-lived in-progress runs visible across polls (otherwise
    a sync that started 60 s ago would drop out once its id <= since_id),
    AND keeps the row visible for the single poll where it transitions from
    running to completed (so the client can observe the status change and
    update its toast). Once a row is observed completed (id <= since_id AND
    status != 'running'), it falls out of the response.
    """
    con = get_con(service_id)
    where: list[str] = []
    params: list = []
    if task and task != "all":
        where.append("task = ?")
        params.append(task)
    if status and status != "all":
        where.append("status = ?")
        params.append(status)
    if since_id is not None:
        where.append("(id > ? OR status = 'running')")
        params.append(since_id)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Skip the count(*) precount for delta polls — the frontend cron poll
    # doesn't read total on the since_id branch (only the cron-history
    # page's full-load path uses it), and the writer-side lock contention
    # this query competes with happens precisely when delta polls are
    # firing fastest. Caller opts out via with_total=False.
    if with_total:
        total_row = con.execute(f"SELECT count(*) AS n FROM cron_runs {where_sql}", params).fetchone()
        total = int(total_row["n"]) if total_row else 0
    else:
        total = 0

    valid_sort_cols = {"started_at", "duration_s", "task", "status"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "started_at"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, task, started_at, duration_s, status, error_message,
                   files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                   parquet_files_created, parquet_files_optimized, parquet_keys, summary
            FROM cron_runs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [
        {
            "id": r["id"],
            "task": r["task"],
            "started_at": r["started_at"],
            "duration_s": r["duration_s"],
            "status": r["status"],
            "error_message": r["error_message"],
            "files_downloaded": r["files_downloaded"],
            "files_deleted_fos": r["files_deleted_fos"],
            "rows_ingested": r["rows_ingested"],
            "corrupt_rows": r["corrupt_rows"],
            "parquet_files_created": r["parquet_files_created"],
            "parquet_files_optimized": r["parquet_files_optimized"],
            "parquet_keys": json.loads(r["parquet_keys"] or "[]"),
            "summary": r["summary"],
        }
        for r in rows
    ]
    return total, entries


def latest_cron_per_task(service_id: str) -> dict[str, dict]:
    """Return {task: latest_completed_run_dict} for the sync-status endpoint.

    Single window-function pass: ROW_NUMBER() OVER (PARTITION BY task) keeps
    the latest non-`running` row per task in one scan of the
    `idx_cron_task_started(task, started_at)` index. The previous
    DISTINCT-tasks + correlated-subquery shape did a btree-seek per task,
    taking ~12.9 ms — fast in absolute terms but per-task overhead added
    up on services with many task types. Mirrors the same pattern used
    by `cron_summary_for_tasks` below.
    """
    con = get_con(service_id)
    rows = con.execute(
        """
        SELECT task, started_at, status, duration_s, summary, error_message
        FROM (
            SELECT task, started_at, status, duration_s, summary, error_message,
                   ROW_NUMBER() OVER (
                       PARTITION BY task ORDER BY started_at DESC, id DESC
                   ) AS rn
            FROM cron_runs
            WHERE status != 'running'
        )
        WHERE rn = 1
        """
    ).fetchall()
    return {
        r["task"]: {
            "started_at": r["started_at"],
            "status": r["status"],
            "duration_s": r["duration_s"],
            "summary": r["summary"],
            "error_message": r["error_message"],
        }
        for r in rows
    }


def reap_running_cron_runs(service_id: str, reason: str = "Process interrupted by server restart") -> int:
    """Mark every ``running`` cron row as ``error``, regardless of age.

    Called at backend startup: in-memory progress dicts (``backend.cron_progress``)
    are wiped on every restart, so any row still marked ``running`` in SQLite is
    by definition an orphan — its event stream is gone and the worker thread
    that owned it died with the previous process. Without this reap, the run
    sits in the DB until the next sync of the *same task* triggers
    ``start_cron_run``'s 60-minute orphan cutoff — and in the meantime the UI
    polls ``/api/cron-runs?status=running``, sees the stale row, and mounts a
    ``CronLiveLog`` that hangs on "Loading logs..." until the SSE endpoint
    times out 30 s later.

    Returns the number of rows reaped (0 if none).
    """
    con = get_con(service_id)
    cur = con.execute(
        "UPDATE cron_runs SET status = 'error', error_message = COALESCE(error_message, ?) WHERE status = 'running'",
        (reason,),
    )
    con.commit()
    return int(cur.rowcount or 0)


def cron_busy(service_id: str) -> bool:
    """True if any cron run is currently 'running' within the orphan threshold."""
    con = get_con(service_id)
    time_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_ORPHAN_THRESHOLD_MINS))
    row = con.execute(
        "SELECT count(*) AS n FROM cron_runs WHERE status = 'running' AND started_at > ?",
        (time_cutoff,),
    ).fetchone()
    return bool(row and row["n"] > 0)


def cron_summary_for_tasks(service_id: str, tasks: tuple[str, ...] = ("sync", "commit")) -> dict[str, dict]:
    """For each named task, return the latest run's summary fields. Used by refresh_config_status."""
    if not tasks:
        return {}
    con = get_con(service_id)
    placeholders = ",".join("?" * len(tasks))
    rows = con.execute(
        f"""
        SELECT task, started_at, duration_s, status, error_message, summary
        FROM (
            SELECT task, started_at, duration_s, status, error_message, summary,
                   ROW_NUMBER() OVER (PARTITION BY task ORDER BY started_at DESC) AS rn
            FROM cron_runs
            WHERE task IN ({placeholders})
        )
        WHERE rn = 1
        """,
        tasks,
    ).fetchall()
    return {
        row["task"]: {
            "last_run": row["started_at"],
            "duration_s": row["duration_s"],
            "status": row["status"],
            "error_message": row["error_message"],
            "summary": row["summary"],
        }
        for row in rows
    }
