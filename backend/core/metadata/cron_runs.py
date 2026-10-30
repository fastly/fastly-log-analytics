"""Cron & ingest job run tracking and scoring audit against metadata Postgres."""

from __future__ import annotations

import json
import logging
import statistics
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import psycopg
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter, ValidationError

from backend.core.metadata.pg_pool import (
    get_con,
    get_con_readonly,
)
from backend.utils.date_utils import iso_z, iso_z_now, parse_iso_utc

logger = logging.getLogger(__name__)


class CronLeaseHeldError(RuntimeError):
    """The (service_id, task) lease is held by another run; callers should skip, not fail."""


class _OutcomeCountersModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    valid_records: int = Field(default=0, ge=0)
    malformed_records: int = Field(default=0, ge=0)
    corrupt_containers: int = Field(default=0, ge=0)
    quarantine_capture_failures: int = Field(default=0, ge=0)
    source_delete_failures: int = Field(default=0, ge=0)
    cap_evictions: int = Field(default=0, ge=0)
    objects_processed: int = Field(default=0, ge=0)
    objects_successful: int = Field(default=0, ge=0)
    objects_partial: int = Field(default=0, ge=0)
    objects_failed: int = Field(default=0, ge=0)


OUTCOME_COUNTER_KEYS = tuple(_OutcomeCountersModel.model_fields.keys())


def _normalize_outcome_counters(counters: dict[str, int] | None) -> dict[str, int]:
    values = counters or {}
    unknown = set(values) - set(OUTCOME_COUNTER_KEYS)
    if unknown:
        raise ValueError(f"unknown ingestion outcome counter(s): {', '.join(sorted(unknown))}")
    try:
        return _OutcomeCountersModel.model_validate(values).model_dump()
    except ValidationError as exc:
        loc = exc.errors()[0].get("loc", ("counter",))[0]
        raise ValueError(f"ingestion outcome counter {loc!r} must be a non-negative integer") from exc


def _decode_outcome_counters(raw: str | None) -> dict[str, int]:
    if not raw:
        return _normalize_outcome_counters(None)
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError("invalid outcome_counters JSON in metadata") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("outcome_counters metadata must be a JSON object")
    try:
        return _normalize_outcome_counters(parsed)
    except ValueError as exc:
        raise RuntimeError("invalid outcome_counters values in metadata") from exc


def _begin_metadata_write(con: Any) -> None:
    con.execute("BEGIN")


def _lock_cron_run(con: Any, service_id: str, run_id: int) -> None:
    con.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))",
        (service_id, run_id),
    )


def _clear_cron_schedule_cache() -> None:
    try:
        from backend.utils.bounded_cache import CacheRegistry

        CacheRegistry.clear("services.core._cron_schedule_cache")
    except Exception:
        logger.debug("cron_log: CacheRegistry.clear failed (ignored)", exc_info=True)


def _publish_cron_run_change(
    service_id: str,
    run_id: int,
    task: str,
    status: str,
    *,
    started_at: str | None = None,
    duration_s: float | None = None,
    ts: str | None = None,
) -> None:
    try:
        from backend.cron.scheduler import publisher

        payload: dict[str, Any] = {
            "event": "cron_run_changed",
            "run_id": run_id,
            "task": task,
            "status": status,
            "ts": ts if ts is not None else iso_z_now(),
        }
        if started_at is not None:
            payload["started_at"] = started_at
        if duration_s is not None:
            payload["duration_s"] = duration_s
        publisher.publish(service_id, payload)
    except Exception:
        logger.exception("[cron_log] cron-runs SSE publish failed for %s/%s", service_id, task)


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
            raise CronLeaseHeldError(f"Task '{task}' is already running for this service.")

        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (service_id, task, started_at),
        )
        con.commit()
        return int(cur.lastrowid or 0)

    # Run the whole reap→guard→insert unit so a converging cron minute
    # can't crash the tick.
    new_id = _write()

    _clear_cron_schedule_cache()

    # Tickle any connected /api/cron-runs/stream subscribers that a run
    # just started. Same defensive contract as the sync-status publish
    # hook (backend/cron/jobs/sync.py): broad except so a publish
    # failure never blocks cron bookkeeping.
    _publish_cron_run_change(
        service_id,
        new_id,
        task,
        "running",
        started_at=started_at,
        duration_s=0.0,
        ts=started_at,
    )

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
    outcome_counters: dict[str, int] | None = None,
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
    outcome_counters = _normalize_outcome_counters(outcome_counters) if outcome_counters is not None else None
    final_status = status
    row_started_at = started_at

    def _write() -> None:
        nonlocal final_status, row_started_at
        import time

        final_rows_ingested = rows_ingested
        final_corrupt_rows = corrupt_rows
        _begin_metadata_write(con)
        try:
            if run_id is not None:
                _lock_cron_run(con, service_id, run_id)
                lock_clause = " FOR UPDATE"
                existing = con.execute(
                    "SELECT started_at, status, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ?"
                    + lock_clause,
                    (run_id, service_id),
                ).fetchone()
                if existing and existing["started_at"]:
                    row_started_at = str(existing["started_at"])
                existing_raw_counters = existing["outcome_counters"] if existing else None
                has_existing_counters = bool(existing_raw_counters and json.loads(existing_raw_counters))
                existing_counters = _decode_outcome_counters(existing_raw_counters) if has_existing_counters else None
                current_status = existing["status"] if existing else None
                if outcome_counters is None:
                    merged_counters = existing_counters
                elif existing_counters is not None and current_status == "running":
                    merged_counters = {
                        key: existing_counters[key] + outcome_counters[key] for key in OUTCOME_COUNTER_KEYS
                    }
                else:
                    merged_counters = existing_counters or outcome_counters
                final_status = "error" if current_status == "error" else status
            else:
                merged_counters = outcome_counters

            now = time.time()
            con.execute(
                "UPDATE job_runs SET status = ?, finished_at = ? "
                "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                (final_status, now, service_id, task),
            )
            if run_id is not None:
                persisted_counters = merged_counters
                if persisted_counters is not None:
                    final_rows_ingested = persisted_counters["valid_records"]
                    final_corrupt_rows = (
                        persisted_counters["malformed_records"] + persisted_counters["corrupt_containers"]
                    )
                con.execute(
                    """UPDATE cron_runs SET
                        duration_s = ?, status = ?, error_message = COALESCE(error_message, ?),
                        files_downloaded = ?, files_deleted_fos = ?, rows_ingested = ?, corrupt_rows = ?,
                        parquet_files_created = ?, parquet_files_optimized = ?,
                        parquet_keys = ?, outcome_counters = ?, summary = ?, log_output = ?
                       WHERE id = ? AND service_id = ?""",
                    (
                        duration_s,
                        final_status,
                        error_message,
                        files_downloaded,
                        files_deleted_fos,
                        final_rows_ingested,
                        final_corrupt_rows,
                        parquet_files_created,
                        parquet_files_optimized,
                        keys_json,
                        json.dumps(persisted_counters or {}),
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
                        parquet_files_created, parquet_files_optimized, parquet_keys, outcome_counters, summary, log_output)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        service_id,
                        task,
                        started_at,
                        duration_s,
                        status,
                        error_message,
                        files_downloaded,
                        files_deleted_fos,
                        final_rows_ingested,
                        final_corrupt_rows,
                        parquet_files_created,
                        parquet_files_optimized,
                        keys_json,
                        json.dumps(outcome_counters or {}),
                        summary,
                        log_output,
                    ),
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    # A leaked terminal write is worse than a leaked start (the row stays
    # 'running' and blocks the task until the orphan cutoff — the 2026-06-19
    # incident).
    _write()

    _clear_cron_schedule_cache()

    # Tickle subscribers that this run reached a terminal status.
    # Broad except so an SSE-channel failure never breaks cron
    # bookkeeping.
    if run_id is not None:
        _publish_cron_run_change(
            service_id,
            run_id,
            task,
            final_status,
            started_at=row_started_at,
            duration_s=duration_s,
        )


def _apply_cron_run_outcome_deltas(
    con: Any,
    service_id: str,
    run_id: int,
    expected_task: str | None,
    deltas: dict[str, int],
) -> tuple[str | None, str | None]:
    """Lock ``cron_runs`` row ``run_id``, apply ``deltas`` to ``outcome_counters``,
    and return ``(prev_run_status, new_run_status)``.
    """
    _lock_cron_run(con, service_id, run_id)
    run_row = con.execute(
        "SELECT task, status, outcome_counters FROM cron_runs WHERE id = ? AND service_id = ? FOR UPDATE",
        (run_id, service_id),
    ).fetchone()
    if run_row is None:
        raise RuntimeError(f"originating cron run {run_id} is missing for ledger object")
    if expected_task != run_row["task"]:
        raise RuntimeError(f"ledger origin task does not match cron run {run_id}")
    prev_run_status = str(run_row["status"]) if run_row["status"] is not None else None

    totals = _decode_outcome_counters(run_row["outcome_counters"])
    for key in OUTCOME_COUNTER_KEYS:
        totals[key] += deltas[key]
        if totals[key] < 0:
            raise RuntimeError(f"outcome counter {key!r} would become negative")
    has_failure = any(
        totals[key]
        for key in (
            "malformed_records",
            "corrupt_containers",
            "quarantine_capture_failures",
            "source_delete_failures",
            "objects_partial",
            "objects_failed",
        )
    )
    new_run_status = "error" if has_failure or run_row["status"] == "error" else run_row["status"]
    con.execute(
        "UPDATE cron_runs SET outcome_counters = ?, status = ?, "
        "rows_ingested = ?, corrupt_rows = ?, "
        "error_message = CASE WHEN ? = 'error' "
        "THEN COALESCE(error_message, 'One or more ingestion objects had failures') "
        "ELSE error_message END WHERE id = ? AND service_id = ?",
        (
            json.dumps(totals),
            new_run_status,
            totals["valid_records"],
            totals["malformed_records"] + totals["corrupt_containers"],
            new_run_status,
            run_id,
            service_id,
        ),
    )
    return prev_run_status, new_run_status


def _record_ingest_object_outcome_inner(
    service_id: str,
    object_key: str,
    outcome_counters: dict[str, int],
) -> tuple[bool, tuple[int, str, str] | None]:
    """Commit one ledger object's outcome and return ``(updated, publish_info)``."""

    new_counters = _normalize_outcome_counters(outcome_counters)
    con = get_con(service_id)
    _begin_metadata_write(con)
    try:
        ledger_row = con.execute(
            "SELECT originating_task, originating_run_id, outcome_counters "
            "FROM ingest_ledger WHERE service_id = ? AND object_key = ? FOR UPDATE",
            (service_id, object_key),
        ).fetchone()
        if ledger_row is None:
            con.execute("ROLLBACK")
            return False, None

        task = ledger_row["originating_task"]
        run_id = ledger_row["originating_run_id"]
        previous = _decode_outcome_counters(ledger_row["outcome_counters"])
        prev_run_status: str | None = None
        new_run_status: str | None = None
        if run_id is not None:
            run_id = int(run_id)
            deltas = {key: new_counters[key] - previous[key] for key in OUTCOME_COUNTER_KEYS}
            prev_run_status, new_run_status = _apply_cron_run_outcome_deltas(con, service_id, run_id, task, deltas)

        con.execute(
            "UPDATE ingest_ledger SET outcome_counters = ? WHERE service_id = ? AND object_key = ?",
            (json.dumps(new_counters), service_id, object_key),
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    if run_id is not None and task is not None and new_run_status is not None and new_run_status != prev_run_status:
        return True, (run_id, task, new_run_status)
    return True, None


def record_ingest_object_outcome(
    service_id: str,
    object_key: str,
    outcome_counters: dict[str, int],
    *,
    publish: bool = True,
) -> bool:
    """Replace one ledger object's contribution in its originating cron run.

    The ledger snapshot and aggregated cron counters commit together, so task
    retries and later source-deletion outcomes cannot double-count an object.
    """
    updated, pub_info = _record_ingest_object_outcome_inner(service_id, object_key, outcome_counters)
    if publish and pub_info is not None:
        run_id, task, new_run_status = pub_info
        _publish_cron_run_change(service_id, run_id, task, new_run_status)
    return updated


def record_ingest_batch_outcomes(
    service_id: str,
    outcomes_by_key: dict[str, dict[str, int]],
) -> int:
    """Record ledger object outcomes for a batch of objects in a single
    transaction, coalescing SSE ``cron_run_changed`` publications to at most
    one per originating cron run.

    Without batch-level coalescing, a 50-file ``convert_batch_objects`` call
    emits 50 back-to-back ``cron_run_changed`` events ~25ms apart for the same
    ``log_discovery`` run, driving a refetch + ``net::ERR_ABORTED`` storm on
    connected browsers.
    """
    if not outcomes_by_key:
        return 0
    normalized_by_key: dict[str, dict[str, int]] = {}
    for object_key, counters in outcomes_by_key.items():
        if not object_key:
            raise ValueError("object_key is required")
        normalized_by_key[object_key] = _normalize_outcome_counters(counters)

    con = get_con(service_id)
    _begin_metadata_write(con)
    pending_publishes: dict[tuple[int, str], str] = {}
    try:
        keys = list(normalized_by_key.keys())
        placeholders = ",".join("?" for _ in keys)
        ledger_rows = con.execute(
            f"SELECT object_key, originating_task, originating_run_id, outcome_counters "
            f"FROM ingest_ledger WHERE service_id = ? AND object_key IN ({placeholders}) "
            f"ORDER BY object_key FOR UPDATE",
            (service_id, *keys),
        ).fetchall()
        if not ledger_rows:
            con.execute("ROLLBACK")
            return 0

        deltas_by_run: dict[int, tuple[str | None, dict[str, int]]] = {}
        ledger_updates: list[tuple[str, str, str]] = []
        for ledger_row in ledger_rows:
            obj_key = ledger_row["object_key"]
            new_counters = normalized_by_key[obj_key]
            task = ledger_row["originating_task"]
            run_id = ledger_row["originating_run_id"]
            previous = _decode_outcome_counters(ledger_row["outcome_counters"])
            if run_id is not None:
                run_id_int = int(run_id)
                existing_entry = deltas_by_run.get(run_id_int)
                if existing_entry is None:
                    deltas = {k: 0 for k in OUTCOME_COUNTER_KEYS}
                    deltas_by_run[run_id_int] = (task, deltas)
                else:
                    if existing_entry[0] != task:
                        raise RuntimeError(f"ledger origin task does not match cron run {run_id_int}")
                    deltas = existing_entry[1]
                for key in OUTCOME_COUNTER_KEYS:
                    deltas[key] += new_counters[key] - previous[key]
            ledger_updates.append((json.dumps(new_counters), service_id, obj_key))

        for run_id_int in sorted(deltas_by_run):
            task, deltas = deltas_by_run[run_id_int]
            prev_run_status, new_run_status = _apply_cron_run_outcome_deltas(con, service_id, run_id_int, task, deltas)
            if task is not None and new_run_status is not None and new_run_status != prev_run_status:
                pending_publishes[(run_id_int, task)] = new_run_status

        con.executemany(
            "UPDATE ingest_ledger SET outcome_counters = ? WHERE service_id = ? AND object_key = ?",
            ledger_updates,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    for (run_id_int, task), new_run_status in pending_publishes.items():
        _publish_cron_run_change(service_id, run_id_int, task, new_run_status)
    return len(ledger_updates)


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
        import time

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
        if cur.rowcount:
            con.execute(
                "UPDATE job_runs SET status = 'error', finished_at = ? "
                "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                (time.time(), service_id, task),
            )
        con.commit()
        return bool(cur.rowcount)

    if not _write():
        return False

    _clear_cron_schedule_cache()

    # Tickle subscribers that this run reached a terminal status — same
    # contract (and broad except) as log_cron_run's publish hook.
    _publish_cron_run_change(service_id, run_id, task, "error")
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
    Best-effort: any DB failure is logged at DEBUG and swallowed so a
    transient Postgres blip doesn't block the actual operator action.
    """
    try:
        con = get_con(service_id)
        con.execute(
            "INSERT INTO scoring_audit (service_id, action, actor, details) VALUES (?, ?, ?, ?)",
            (service_id, action, actor, json.dumps(details) if details else None),
        )
        con.commit()
    except psycopg.Error as e:
        logger.debug("[metadata_db] record_scoring_audit(%s, %s) failed: %s", service_id, action, e)


_JSON_ANY_ADAPTER: TypeAdapter[Any] = TypeAdapter(Any)
_JSON_LIST_ADAPTER: TypeAdapter[list[Any]] = TypeAdapter(list[Any])


def _try_parse_json(v: Any) -> Any:
    if isinstance(v, (str, bytes)) and v:
        try:
            return _JSON_ANY_ADAPTER.validate_json(v)
        except (ValueError, TypeError):
            return v
    return v


class _ScoringAuditRowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    details: Annotated[Any, BeforeValidator(_try_parse_json)] = None


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
        return [_ScoringAuditRowModel.model_validate(dict(r)).model_dump() for r in rows]
    except psycopg.Error as e:
        logger.debug("[metadata_db] list_scoring_audit(%s) failed: %s", service_id, e)
        return []


def get_cron_run_status(service_id: str, run_id: int) -> str | None:
    """Return the status string for a single cron_runs row, or None if
    the row doesn't exist. Used by cron_progress.list_active_runs to
    cross-check the in-memory state against the DB-of-truth (catches
    abandoned-worker-thread zombies that completed log_cron_run but
    never fired end_progress).

    Narrowed exception scope: catches psycopg.Error (DB unreachable,
    table missing, connection lost) and logs at DEBUG so the next 'why
    isn't the cross-check firing?' triage isn't flying blind. Returns None
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
    except psycopg.Error as e:
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
    except psycopg.Error as e:
        logger.debug("[metadata_db] get_cron_run_result(%s, %s) failed: %s", service_id, run_id, e)
        return None


def _parse_parquet_keys(v: Any) -> list[Any]:
    if isinstance(v, (str, bytes)):
        return _JSON_LIST_ADAPTER.validate_json(v or "[]")
    if v:
        return list(v)
    return []


class _CronRunEntryModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    parquet_keys: Annotated[
        list[Any],
        BeforeValidator(_parse_parquet_keys),
    ] = Field(default_factory=list)


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
        if "," in task:
            task_list = [t.strip() for t in task.split(",") if t.strip()]
            placeholders = ", ".join("?" for _ in task_list)
            where.append(f"task IN ({placeholders})")
            params.extend(task_list)
        elif task in ("rum_sync", "rum_discovery"):
            where.append("task IN ('rum_sync', 'rum_discovery')")
        else:
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

    entries = [_CronRunEntryModel.model_validate(dict(r)).model_dump() for r in rows]
    if total is None:
        total = len(entries)
    return total, entries


def latest_cron_per_task(service_id: str, exclude_error_messages: tuple[str, ...] = ()) -> dict[str, dict]:
    """Return {task: latest_completed_run_dict} for the sync-status endpoint.

    Single window-function pass: ROW_NUMBER() OVER (PARTITION BY task) keeps
    the latest non-`running` row per task in one scan of the
    `idx_cron_task_started(task, started_at)` index. The previous
    DISTINCT-tasks + correlated-subquery shape did a btree-seek per task,
    taking ~12.9 ms — fast in absolute terms but per-task overhead added
    up on services with many task types. Mirrors the same pattern used
    by `cron_summary_for_tasks` below.

    ``exclude_error_messages`` (default none) drops rows carrying one of
    these exact error_message sentinels BEFORE the per-task ranking, so an
    excluded row cannot become a task's "latest" and cannot mask the real
    terminal run beneath it. The fault-surfacing caller
    (``recent_cron_failures`` in ``routers/admin/health.py``) passes the
    boot-reap sentinel ``"Process interrupted by server restart"`` here, so a
    cron the deploy restarted mid-run isn't reported as an environment fault —
    matching the identical exclusion the deep-health degraded check already
    applies in ``backend/main.py`` (see its comment on boot-reap rows being
    lifecycle artifacts, not failing crons). Default callers are unaffected.
    """
    con = get_con(service_id)

    exclusion_sql = ""
    params: list[Any] = [service_id]
    if exclude_error_messages:
        placeholders = ",".join(["?"] * len(exclude_error_messages))
        exclusion_sql = f" AND COALESCE(error_message, '') NOT IN ({placeholders})"
        params.extend(exclude_error_messages)

    sql = f"""
    WITH RECURSIVE tasks AS (
        (
            SELECT task FROM cron_runs WHERE service_id = ? ORDER BY service_id, task LIMIT 1
        )
        UNION ALL
        SELECT (
            SELECT task FROM cron_runs WHERE service_id = ? AND task > t.task ORDER BY service_id, task LIMIT 1
        )
        FROM tasks t WHERE t.task IS NOT NULL
    )
    SELECT r.task, r.started_at, r.status, r.duration_s, r.summary, r.error_message
    FROM tasks t
    CROSS JOIN LATERAL (
        SELECT task, started_at, status, duration_s, summary, error_message
        FROM cron_runs
        WHERE service_id = ? AND task = t.task AND status != 'running'{exclusion_sql}
        ORDER BY service_id, task, started_at DESC, id DESC LIMIT 1
    ) r WHERE t.task IS NOT NULL
    """
    cte_params: list[Any] = [service_id, service_id, service_id]
    if exclude_error_messages:
        cte_params.extend(exclude_error_messages)
    rows = con.execute(sql, tuple(cte_params)).fetchall()
    latest_by_task: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row)
        task_name = row_dict.pop("task")
        latest_by_task[task_name] = row_dict
    return latest_by_task


def adaptive_stale_minutes(
    con: Any,
    *,
    default_minutes: int,
    service_id: str | None = None,
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
        sid = service_id or getattr(con, "_service_id", None)
        rows = None
        if sid:
            try:
                rows = con.execute(
                    "SELECT started_at FROM cron_runs "
                    "WHERE service_id = ? "
                    "AND task = 'log_discovery' AND status = 'success' "
                    "AND (files_downloaded > 0 OR rows_ingested > 0) "
                    "ORDER BY started_at",
                    (sid,),
                ).fetchall()
            except Exception:
                rows = None
        if rows is None:
            rows = con.execute(
                "SELECT started_at FROM cron_runs "
                "WHERE task = 'log_discovery' AND status = 'success' "
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
        "UPDATE cron_runs SET status = 'error', error_message = COALESCE(error_message, ?) "
        "WHERE service_id = ? AND status = 'running'",
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
