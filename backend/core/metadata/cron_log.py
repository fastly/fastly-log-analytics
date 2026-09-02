"""Cron-run history + scoring audit in metadata SQLite.

Backs the ``cron_runs`` and ``scoring_audit`` tables. Provides the start /
update / log / purge / reap surface used by the scheduler and the per-task
status summaries used by the sync-status / refresh-config-status endpoints.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import statistics
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from backend.core.metadata.base import get_con, get_con_readonly
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

logger = logging.getLogger(__name__)

# Cron bookkeeping writes converge: sync / commit / local-compact / metadata_sync
# all fire on the same minute boundary and write the one per-service metadata.db.
# ``PRAGMA busy_timeout=30000`` (see sqlite_pool.DEFAULT_PRAGMAS) absorbs ordinary
# lock-queueing, but an *immediate* SQLITE_BUSY bypasses the busy handler — a WAL
# snapshot conflict, or a checkpoint that couldn't drain while the disk was full
# (the 2026-06-23 incident bloated the WAL to 4 MB). Those surface as
# "database is locked" and used to crash the whole cron tick with a traceback.
# They are transient: roll back and retry the whole write unit a few times with
# short backoff. A lock that survives every attempt re-raises (a real problem,
# not contention). See [[colima-disk-full-stalls-ingestion]].
_LOCKED_RETRY_ATTEMPTS = 20
_LOCKED_RETRY_BASE_SLEEP_S = 0.05


def _retry_on_locked[T](con: sqlite3.Connection, fn: Callable[[], T]) -> T:
    """Run ``fn`` (a complete, idempotent write+commit unit), retrying on a
    transient ``OperationalError: database is locked``.

    Each attempt rolls back first so a half-open transaction never leaves a
    stale WAL snapshot pinned for the retry. ``fn`` must be safe to re-run after
    a rollback — the cron writers acquire the write lock on their first
    statement, so a lock failure means nothing was committed and the redo can't
    double-write. Non-lock errors (e.g. the ``RuntimeError`` start_cron_run
    raises when a run is already in progress) propagate immediately.
    """
    for attempt in range(_LOCKED_RETRY_ATTEMPTS):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if (
                "locked" not in str(e).lower()
                and "disk i/o error" not in str(e).lower()
                or attempt == _LOCKED_RETRY_ATTEMPTS - 1
            ):
                raise
            try:
                con.rollback()
            except sqlite3.Error:
                pass
            logger.debug(
                "[cron_log] transient DB lock (attempt %d/%d), retrying: %s", attempt + 1, _LOCKED_RETRY_ATTEMPTS, e
            )
            time.sleep(min(1.0, _LOCKED_RETRY_BASE_SLEEP_S * (2**attempt)))
    # Unreachable: the loop either returns fn()'s value or re-raises on the
    # final attempt. Present so type-checkers see a terminal path.
    raise AssertionError("unreachable")  # pragma: no cover


def start_cron_run(service_id: str, task: str) -> int:
    import time

    con = get_con(service_id)
    started_at = iso_z_now()
    now = time.time()

    def _write() -> int:
        # Reap orphans based on heartbeat.
        # Any cron_runs row that is 'running' but has no active lease is an
        # orphan. Idempotent under concurrent callers (SQLite same-thread;
        # Postgres multi-pod): a second UPDATE hitting the same rows after
        # the first already flipped them just matches zero rows.
        con.execute(
            """UPDATE cron_runs SET status = 'error',
               error_message = COALESCE(error_message, 'Process interrupted (heartbeat lost)')
               WHERE service_id = ? AND task = ? AND status = 'running'
                 AND NOT EXISTS (
                     SELECT 1 FROM job_runs
                     WHERE service_id = cron_runs.service_id
                       AND job_name = cron_runs.task
                       AND status = 'running'
                       AND (heartbeat_at + lease_ttl_s) >= ?
                 )""",
            (service_id, task, now),
        )

        # Also clean up expired job_runs — reaping an EXPIRED lease first is
        # what makes the atomic insert below correct: by the time it runs,
        # any row still literally status='running' is non-expired (had it
        # expired, this statement would already have reaped it), so the
        # unique-index conflict below means a genuinely active lease, not a
        # stale one blocking a legitimate new run.
        con.execute(
            """UPDATE job_runs SET status = 'reaped'
               WHERE service_id = ? AND job_name = ? AND status = 'running'
                 AND (heartbeat_at + lease_ttl_s) < ?""",
            (service_id, task, now),
        )

        # Acquire the lease atomically. idx_job_runs_running_lease (a unique
        # index on (service_id, job_name) WHERE status='running') is the
        # conflict target — ON CONFLICT DO NOTHING means at most one of any
        # number of concurrent callers gets rowcount=1; everyone else gets
        # rowcount=0 and must back off. This replaces a check-then-insert
        # (SELECT count, then INSERT) that only stayed race-free because a
        # single SQLite writer connection serializes both statements inside
        # one transaction — a guarantee autocommit-mode Postgres does NOT
        # give multiple pods racing the same (service_id, task).
        lease_ttl_s = 60
        lease_cur = con.execute(
            "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
            "VALUES (?, ?, ?, ?, ?, 'running') "
            "ON CONFLICT (service_id, job_name) WHERE status = 'running' DO NOTHING",
            (service_id, task, now, now, lease_ttl_s),
        )
        if not lease_cur.rowcount:
            con.commit()
            raise RuntimeError(f"Task '{task}' is already running for this service.")

        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (service_id, task, started_at),
        )
        con.commit()
        return int(cur.lastrowid or 0)

    # Retry the whole reap→guard→insert unit on a transient lock so a
    # converging cron minute can't crash the tick (see _retry_on_locked).
    new_id = _retry_on_locked(con, _write)

    try:
        from backend.utils.cache_registry import CacheRegistry

        CacheRegistry.clear("services.core._cron_schedule_cache")
    except Exception:
        pass

    # Tickle any connected /api/cron-runs/stream subscribers that a run
    # just started. Same defensive contract as the sync-status publish
    # hook (backend/cron/jobs/sync.py): broad except so a publish
    # failure never blocks cron bookkeeping.
    try:
        from backend.cron_runs_publisher import publisher as _cron_runs_publisher

        _cron_runs_publisher.publish(
            service_id,
            {
                "event": "cron_run_changed",
                "run_id": new_id,
                "task": task,
                "status": "running",
                "ts": started_at,
            },
        )
    except Exception:
        logger.exception("[cron_log] cron-runs SSE publish (start) failed for %s/%s", service_id, task)

    return new_id


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

    def _write() -> None:
        import time

        now = time.time()
        con.execute(
            "UPDATE job_runs SET status = ?, finished_at = ? WHERE service_id = ? AND job_name = ? AND status = 'running'",
            (status, now, service_id, task),
        )
        if run_id is not None:
            con.execute(
                """UPDATE cron_runs SET
                    duration_s = ?, status = ?, error_message = ?,
                    files_downloaded = ?, files_deleted_fos = ?, rows_ingested = ?, corrupt_rows = ?,
                    parquet_files_created = ?, parquet_files_optimized = ?,
                    parquet_keys = ?, summary = ?, log_output = ?
                   WHERE id = ? AND service_id = ?""",
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
                    service_id,
                ),
            )
        else:
            con.execute(
                """INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, error_message,
                    files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                    parquet_files_created, parquet_files_optimized, parquet_keys, summary, log_output)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    service_id,
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

    # A leaked terminal write is worse than a leaked start (the row stays
    # 'running' and blocks the task until the orphan cutoff — the 2026-06-19
    # incident), so retry the terminal write on a transient lock too.
    _retry_on_locked(con, _write)

    try:
        from backend.utils.cache_registry import CacheRegistry

        CacheRegistry.clear("services.core._cron_schedule_cache")
    except Exception:
        pass

    # Tickle subscribers that this run reached a terminal status.
    # Broad except so an SSE-channel failure never breaks cron
    # bookkeeping.
    try:
        from backend.cron_runs_publisher import publisher as _cron_runs_publisher

        _cron_runs_publisher.publish(
            service_id,
            {
                "event": "cron_run_changed",
                "run_id": run_id,
                "task": task,
                "status": status,
                "ts": iso_z_now(),
            },
        )
    except Exception:
        logger.exception("[cron_log] cron-runs SSE publish (end) failed for %s/%s", service_id, task)


def finalize_cron_run_if_running(
    service_id: str,
    task: str,
    run_id: int | None,
    *,
    duration_s: float = 0.0,
    summary: str | None = None,
    error_message: str | None = None,
) -> bool:
    """Backstop: finalize a cron_runs row as ``error`` ONLY if it's still
    ``running``. Idempotent — no-op when the row is missing or already
    terminal.

    The normal lifecycle is ``start_cron_run`` (inserts ``running``) →
    ``log_cron_run`` (sets the terminal status). If a job exits between those
    two without recording a status — e.g. an ingest generator that returns no
    terminal ``done`` event, or an exception on a path that skips
    ``log_cron_run`` — the row leaks as ``running`` and blocks every
    subsequent run of the same task until the orphan cutoff. That froze prod
    ingestion ~20 min on 2026-06-19. Call this from a ``finally`` so the row
    can never be orphaned by a code path.

    The UPDATE is guarded with ``AND status = 'running'`` so a row that became
    terminal between the read and the write is never clobbered. Returns True
    iff it flipped the row.
    """
    if run_id is None:
        return False
    con = get_con(service_id)

    def _write() -> bool:
        row = con.execute(
            "SELECT status FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id)
        ).fetchone()
        if not row or row["status"] != "running":
            return False
        cur = con.execute(
            "UPDATE cron_runs SET status = 'error', duration_s = ?, "
            "error_message = COALESCE(error_message, ?), summary = COALESCE(summary, ?) "
            "WHERE id = ? AND service_id = ? AND status = 'running'",
            (max(duration_s, 0.0), error_message, summary, run_id, service_id),
        )
        con.commit()
        return bool(cur.rowcount)

    if not _retry_on_locked(con, _write):
        return False

    try:
        from backend.utils.cache_registry import CacheRegistry

        CacheRegistry.clear("services.core._cron_schedule_cache")
    except Exception:
        pass

    # Tickle subscribers that this run reached a terminal status — same
    # contract (and broad except) as log_cron_run's publish hook.
    try:
        from backend.cron_runs_publisher import publisher as _cron_runs_publisher

        _cron_runs_publisher.publish(
            service_id,
            {
                "event": "cron_run_changed",
                "run_id": run_id,
                "task": task,
                "status": "error",
                "ts": iso_z_now(),
            },
        )
    except Exception:
        logger.exception("[cron_log] cron-runs SSE publish (finalize) failed for %s/%s", service_id, task)
    return True


def update_cron_duration(
    service_id: str,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
) -> None:
    con = get_con(service_id)
    if log_output is None:
        con.execute(
            "UPDATE cron_runs SET duration_s = ? WHERE id = ? AND service_id = ?",
            (duration_s, run_id, service_id),
        )
    else:
        con.execute(
            "UPDATE cron_runs SET duration_s = ?, log_output = ? WHERE id = ? AND service_id = ?",
            (duration_s, log_output, run_id, service_id),
        )
    con.commit()


def delete_cron_run(service_id: str, run_id: int) -> None:
    con = get_con(service_id)
    con.execute("DELETE FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id))
    con.commit()


def purge_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    days: int | None = None,
) -> None:
    con = get_con(service_id)
    where: list[str] = ["service_id = ?"]
    params: list = [service_id]
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
    import contextlib

    try:
        with contextlib.closing(get_con_readonly(service_id)) as con:
            row = con.execute(
                "SELECT status FROM cron_runs WHERE id = ? AND service_id = ?", (run_id, service_id)
            ).fetchone()
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
    import contextlib

    try:
        with contextlib.closing(get_con_readonly(service_id)) as con:
            row = con.execute(
                "SELECT status, log_output FROM cron_runs WHERE id = ? AND service_id = ?",
                (run_id, service_id),
            ).fetchone()
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
    where: list[str] = ["service_id = ?"]
    params: list = [service_id]
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
    # firing fastest. Caller opts out via with_total=False. When skipped,
    # ``total`` is filled in from ``len(entries)`` below so callers (and
    # tests) see a truthful row count for the page they got back, rather
    # than a misleading 0.
    if with_total:
        total_row = con.execute(f"SELECT count(*) AS n FROM cron_runs {where_sql}", params).fetchone()
        total = int(total_row["n"]) if total_row else 0
    else:
        total = None  # filled in after rows are fetched

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
    if total is None:
        total = len(entries)
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
            WHERE service_id = ? AND status != 'running'
        )
        WHERE rn = 1
        """,
        (service_id,),
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


def adaptive_stale_minutes(
    con: sqlite3.Connection,
    *,
    default_minutes: int,
    min_samples: int = 10,
    percentile: float = 0.95,
    safety_multiplier: float = 2.0,
    max_minutes: int = 1440,
) -> int:
    """SRE-22: widen (never narrow) ``default_minutes`` to this service's own
    p95 gap between non-empty ingests, so a low-traffic service's organic
    quiet periods don't trip ``/api/health``'s staleness probe. See
    ``backend.main.health_check``.

    Takes an already-open connection (rather than self-fetching via
    ``get_con(service_id)`` like most of this module) because its only
    caller, ``health_check``, already has one open for the same service —
    and reusing it, rather than a second ``get_con`` call, is what lets a
    test double the caller injects (e.g. an in-memory sqlite fixture) also
    cover this query, instead of silently falling through to the real
    per-service DB.

    Computed from the gaps between consecutive successful ``sync`` runs with
    ``files_downloaded > 0 OR rows_ingested > 0`` in ``cron_runs``. Falls
    back to ``default_minutes`` unchanged when there are fewer than
    ``min_samples`` gaps (new service, or ``cron_runs_days`` retention
    hasn't accumulated enough history yet), or on any error — this must
    never make the probe LESS reliable than the fixed check it widens.
    """
    try:
        rows = con.execute(
            # 'sync' (pre-v3 name) kept so pre-upgrade history still feeds the
            # percentile until enough log_discovery samples accumulate.
            "SELECT started_at FROM cron_runs "
            "WHERE task IN ('sync', 'log_discovery') AND status = 'success' "
            "AND (files_downloaded > 0 OR rows_ingested > 0) "
            "ORDER BY started_at"
        ).fetchall()
        timestamps = [t for t in (parse_iso_utc(r["started_at"]) for r in rows) if t is not None]
        if len(timestamps) < min_samples + 1:
            return default_minutes
        gaps = sorted((b - a).total_seconds() / 60.0 for a, b in zip(timestamps, timestamps[1:]))
        cut_points = statistics.quantiles(gaps, n=100, method="inclusive")
        p_gap = cut_points[min(len(cut_points) - 1, max(0, int(percentile * 100) - 1))]
        return int(min(max_minutes, max(default_minutes, round(p_gap * safety_multiplier))))
    except Exception:
        return default_minutes


def reap_running_cron_runs(service_id: str, reason: str = "Process interrupted by server restart") -> int:
    con = get_con(service_id)
    con.execute(
        "UPDATE job_runs SET status = 'error', detail = ? WHERE service_id = ? AND status = 'running'",
        (reason, service_id),
    )
    cur = con.execute(
        "UPDATE cron_runs SET status = 'error', error_message = COALESCE(error_message, ?) WHERE service_id = ? AND status = 'running'",
        (reason, service_id),
    )
    con.commit()
    return int(cur.rowcount or 0)


def cron_busy(service_id: str) -> bool:
    import time

    con = get_con(service_id)
    now = time.time()
    row = con.execute(
        "SELECT count(*) AS n FROM job_runs WHERE service_id = ? AND status = 'running' AND (heartbeat_at + lease_ttl_s) >= ?",
        (service_id, now),
    ).fetchone()
    return bool(row and row["n"] > 0)


def cron_summary_for_tasks(
    service_id: str, tasks: tuple[str, ...] = ("log_discovery", "log_ingest")
) -> dict[str, dict]:
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
            WHERE service_id = ? AND task IN ({placeholders})
        )
        WHERE rn = 1
        """,
        (service_id, *tasks),
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


def heartbeat_cron_run(service_id: str, task: str) -> None:
    import time

    con = get_con(service_id)
    now = time.time()
    con.execute(
        "UPDATE job_runs SET heartbeat_at = ? WHERE service_id = ? AND job_name = ? AND status = 'running'",
        (now, service_id, task),
    )
    con.commit()


def reap_stale_jobs(service_id: str) -> None:
    import time

    from backend.core.metadata.base import get_con

    con = get_con(service_id)
    now = time.time()

    # Find all running jobs that have expired
    stale_jobs = con.execute(
        "SELECT id, job_name FROM job_runs WHERE service_id = ? AND status = 'running' AND (heartbeat_at + lease_ttl_s) < ?",
        (service_id, now),
    ).fetchall()

    if not stale_jobs:
        return

    for row in stale_jobs:
        job_id = row["id"]
        job_name = row["job_name"]

        con.execute(
            "UPDATE job_runs SET status = 'reaped', detail = 'reaped due to lease expiry' WHERE id = ?", (job_id,)
        )
        # Scope by started_at: only rows that predate the lease expiry are the
        # leaked ones — an unscoped UPDATE would also error a NEWER healthy run
        # of the same task that started after the stale lease expired.
        con.execute(
            "UPDATE cron_runs SET status = 'error', error_message = 'Reaped due to lease expiry' "
            "WHERE service_id = ? AND task = ? AND status = 'running' "
            "AND started_at <= (SELECT strftime('%Y-%m-%dT%H:%M:%SZ', heartbeat_at + lease_ttl_s, 'unixepoch') "
            "                   FROM job_runs WHERE id = ?)",
            (service_id, job_name, job_id),
        )

    con.commit()

    # Re-enqueue idempotent ingest jobs to prevent orphaned-sync-row ingestion
    # stalls — only when a broker is actually configured (in-process mode has
    # no worker; send_task would error against the default localhost broker).
    # Commit the reap FIRST so a broker failure can't roll it back.
    import os

    if os.environ.get("CELERY_BROKER_URL"):
        from backend.celery_app import app

        for row in stale_jobs:
            try:
                if row["job_name"] == "log_discovery":
                    app.send_task("backend.cron.jobs.sync._run_log_discovery_cron_celery", args=[service_id])
                elif row["job_name"] == "log_ingest":
                    app.send_task("backend.cron.jobs.commit._run_log_ingest_celery", args=[service_id])
            except Exception as e:
                logger.warning("[reap_stale_jobs] %s: re-enqueue of %s failed: %s", service_id, row["job_name"], e)
