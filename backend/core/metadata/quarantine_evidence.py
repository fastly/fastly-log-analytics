"""Quarantined-file and exact-byte local evidence tracking in the metadata Postgres database."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO

from backend import config
from backend.core.metadata import base as pg_connection
from backend.core.metadata.base import get_con
from backend.utils.telemetry import atomic_write_bytes

logger = logging.getLogger(__name__)

_QUARANTINE_EVIDENCE_COLUMNS = (
    "id",
    "service_id",
    "source_type",
    "original_key",
    "line_ordinal",
    "byte_offset",
    "byte_length",
    "error_category",
    "error_text",
    "sha256",
    "file_path",
    "quarantined_at",
)


def get_quarantine_storage_total(service_id: str) -> int:
    con = get_con(service_id)
    row = con.execute(
        "SELECT coalesce(sum(error_size_bytes), 0) FROM quarantined_files WHERE service_id = ?", (service_id,)
    ).fetchone()
    return int(row[0]) if row else 0


def insert_quarantine_evidence(
    service_id: str,
    source_type: str,
    original_key: str,
    *,
    line_ordinal: int | None,
    byte_offset: int | None,
    byte_length: int,
    error_category: str,
    error_text: str,
    sha256: str,
) -> int:
    con = get_con(service_id)
    row = con.execute(
        """
        INSERT INTO quarantine_evidence
            (service_id, source_type, original_key, line_ordinal, byte_offset,
             byte_length, error_category, error_text, sha256, file_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')
        RETURNING id
        """,
        (
            service_id,
            source_type,
            original_key,
            line_ordinal,
            byte_offset,
            byte_length,
            error_category,
            error_text,
            sha256,
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("quarantine evidence insert returned no item id")
    item_id = int(row[0])
    try:
        con.execute(
            "UPDATE quarantine_evidence SET file_path = ? WHERE service_id = ? AND id = ?",
            (f"{item_id}.dat", service_id, item_id),
        )
        con.commit()
    except Exception:
        con.execute("DELETE FROM quarantine_evidence WHERE service_id = ? AND id = ?", (service_id, item_id))
        con.commit()
        raise
    return item_id


def get_quarantine_evidence(service_id: str, item_id: int) -> dict[str, Any] | None:
    con = get_con(service_id)
    row = con.execute(
        """
        SELECT id, service_id, source_type, original_key, line_ordinal,
               byte_offset, byte_length, error_category, error_text, sha256,
               file_path, quarantined_at
        FROM quarantine_evidence
        WHERE service_id = ? AND id = ?
        """,
        (service_id, item_id),
    ).fetchone()
    if row is None:
        return None
    return dict(zip(_QUARANTINE_EVIDENCE_COLUMNS, row, strict=True))


def list_quarantine_evidence(
    service_id: str,
    limit: int = 100,
    offset: int = 0,
    *,
    oldest_first: bool = False,
    error_category: str | None = None,
    source_type: str | None = None,
) -> list[dict[str, Any]]:
    con = get_con(service_id)
    order = "ASC" if oldest_first else "DESC"
    query = """
        SELECT id, service_id, source_type, original_key, line_ordinal,
               byte_offset, byte_length, error_category, error_text, sha256,
               file_path, quarantined_at
        FROM quarantine_evidence
        WHERE service_id = ?
    """
    params: list[object] = [service_id]
    if error_category is not None:
        query += " AND error_category = ?"
        params.append(error_category)
    if source_type is not None:
        query += " AND source_type = ?"
        params.append(source_type)
    query += f" ORDER BY quarantined_at {order}, id {order} LIMIT ? OFFSET ?"
    params.extend((limit, offset))
    rows = con.execute(query, params).fetchall()
    return [dict(zip(_QUARANTINE_EVIDENCE_COLUMNS, row, strict=True)) for row in rows]


def count_quarantine_evidence(
    service_id: str,
    *,
    error_category: str | None = None,
    source_type: str | None = None,
) -> int:
    con = get_con(service_id)
    query = "SELECT count(*) FROM quarantine_evidence WHERE service_id = ?"
    params: list[object] = [service_id]
    if error_category is not None:
        query += " AND error_category = ?"
        params.append(error_category)
    if source_type is not None:
        query += " AND source_type = ?"
        params.append(source_type)
    row = con.execute(query, params).fetchone()
    return int(row[0]) if row else 0


def get_quarantine_evidence_summary(service_id: str) -> dict[str, Any]:
    con = get_con(service_id)
    row = con.execute(
        """
        SELECT count(*), coalesce(sum(byte_length), 0), min(quarantined_at), max(quarantined_at)
        FROM quarantine_evidence
        WHERE service_id = ?
        """,
        (service_id,),
    ).fetchone()
    grouped = con.execute(
        """
        SELECT source_type, error_category, count(*)
        FROM quarantine_evidence
        WHERE service_id = ?
        GROUP BY source_type, error_category
        """,
        (service_id,),
    ).fetchall()
    categories: dict[str, int] = {}
    source_counts = {"request": 0, "rum": 0}
    for source_type, category, count in grouped:
        source_counts[source_type] = source_counts.get(source_type, 0) + count
        categories[category] = categories.get(category, 0) + count
    return {
        "total_items": row[0],
        "total_bytes": row[1],
        "oldest_at": str(row[2]) if row[2] is not None else None,
        "newest_at": str(row[3]) if row[3] is not None else None,
        "request_items": source_counts["request"],
        "rum_items": source_counts["rum"],
        "category_counts": categories,
    }


get_quarantine_summary = get_quarantine_evidence_summary


def delete_quarantine_evidence(service_id: str, ids: list[int] | None = None) -> int:
    con = get_con(service_id)
    if ids is None:
        cur = con.execute("DELETE FROM quarantine_evidence WHERE service_id = ?", (service_id,))
    elif ids:
        placeholders = ", ".join("?" * len(ids))
        cur = con.execute(
            f"DELETE FROM quarantine_evidence WHERE service_id = ? AND id IN ({placeholders})",
            [service_id, *ids],
        )
    else:
        return 0
    con.commit()
    return int(cur.rowcount)


# ---------------------------------------------------------------------------
# Exact-byte local evidence storage (folded from backend.core.quarantine)
# ---------------------------------------------------------------------------

QUARANTINE_ITEM_CAP = 1_000
_MAX_ERROR_TEXT_LENGTH = 2_000
_service_locks: dict[str, threading.RLock] = {}
_service_locks_guard = threading.Lock()


def _evidence_dir(service_id: str) -> Path:
    services_root = config.SERVICES_DATA_DIR.resolve()
    service_root = (services_root / service_id).resolve()
    if not service_root.is_relative_to(services_root):
        raise ValueError("service_id resolves outside the services data directory")
    evidence_dir = (service_root / "quarantine").resolve()
    if not evidence_dir.is_relative_to(service_root):
        raise ValueError("quarantine directory resolves outside the service data directory")
    evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(evidence_dir, 0o700)
    return evidence_dir


_quarantine_root = _evidence_dir


def _evidence_path(service_id: str, item_id: int) -> Path:
    if item_id <= 0:
        raise ValueError("item_id must be positive")
    return _evidence_dir(service_id) / f"{item_id}.dat"


def _advisory_key(service_id: str) -> int:
    value = int.from_bytes(hashlib.sha256(b"fla-quarantine:" + service_id.encode()).digest()[:8], "big")
    return value - (1 << 64) if value >= (1 << 63) else value


@contextmanager
def _service_lock(service_id: str) -> Iterator[None]:
    with _service_locks_guard:
        lock = _service_locks.setdefault(service_id, threading.RLock())
    with lock:
        with pg_connection.get_pg_pool().connection() as con:
            with con.transaction():
                con.execute("SELECT pg_advisory_xact_lock(%s)", (_advisory_key(service_id),))
                yield


_quarantine_file_lock = _service_lock


def _unlink_evidence(service_id: str, item: dict[str, Any]) -> bool:
    item_id = int(item["id"])
    expected_name = f"{item_id}.dat"
    if item["file_path"] != expected_name:
        logger.error(
            "quarantine.evidence.invalid_path",
            extra={"service_id": service_id, "item_id": item_id},
        )
        return False
    path = _evidence_path(service_id, item_id)
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        logger.exception(
            "quarantine.evidence.unlink_failed",
            extra={"service_id": service_id, "item_id": item_id},
        )
        return False


def capture_evidence(
    service_id: str,
    source_type: str,
    original_key: str,
    payload: bytes,
    *,
    line_ordinal: int | None,
    byte_offset: int | None,
    error_category: str,
    error_text: str,
) -> dict[str, int | str]:
    if source_type not in {"request", "rum"}:
        raise ValueError("source_type must be 'request' or 'rum'")
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if line_ordinal is not None and line_ordinal < 1:
        raise ValueError("line_ordinal must be positive when provided")
    if byte_offset is not None and byte_offset < 0:
        raise ValueError("byte_offset must be non-negative when provided")

    try:
        with _service_lock(service_id):
            return _capture_evidence_locked(
                service_id,
                source_type,
                original_key,
                payload,
                line_ordinal=line_ordinal,
                byte_offset=byte_offset,
                error_category=error_category,
                error_text=error_text,
            )
    except Exception:
        logger.exception(
            "quarantine.evidence.lock_failed",
            extra={"service_id": service_id, "source_type": source_type, "original_key": original_key},
        )
        return {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def _capture_evidence_locked(
    service_id: str,
    source_type: str,
    original_key: str,
    payload: bytes,
    *,
    line_ordinal: int | None,
    byte_offset: int | None,
    error_category: str,
    error_text: str,
) -> dict[str, int | str]:
    item_id: int | None = None
    final_path: Path | None = None
    try:
        evidence_dir = _evidence_dir(service_id)
        item_id = insert_quarantine_evidence(
            service_id,
            source_type,
            original_key,
            line_ordinal=line_ordinal,
            byte_offset=byte_offset,
            byte_length=len(payload),
            error_category=error_category,
            error_text=error_text[:_MAX_ERROR_TEXT_LENGTH],
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        final_path = evidence_dir / f"{item_id}.dat"
        atomic_write_bytes(final_path, payload, prefix=".capture-", fsync=True)

        rows = list_quarantine_evidence(service_id, limit=QUARANTINE_ITEM_CAP + 1, oldest_first=True)
        evicted = rows[: max(0, len(rows) - QUARANTINE_ITEM_CAP)]
        for item in evicted:
            if not _unlink_evidence(service_id, item):
                raise OSError(f"could not delete quarantine evidence {item['id']}")
        evicted_count = delete_quarantine_evidence(service_id, [int(item["id"]) for item in evicted])
        return {
            "id": item_id,
            "cap_evictions": evicted_count,
            "quarantine_capture_failures": 0,
        }
    except Exception:
        logger.exception(
            "quarantine.evidence.capture_failed",
            extra={"service_id": service_id, "source_type": source_type, "original_key": original_key},
        )
        if item_id is not None:
            try:
                delete_quarantine_evidence(service_id, [item_id])
            except Exception:
                logger.exception(
                    "quarantine.evidence.metadata_cleanup_failed",
                    extra={"service_id": service_id, "item_id": item_id},
                )
        if final_path is not None:
            try:
                final_path.unlink(missing_ok=True)
            except OSError:
                logger.exception(
                    "quarantine.evidence.file_cleanup_failed",
                    extra={"service_id": service_id, "item_id": item_id},
                )
        return {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def read_evidence(service_id: str, item_id: int) -> tuple[dict[str, Any], BinaryIO] | None:
    item = get_quarantine_evidence(service_id, item_id)
    if item is None:
        return None
    path = _evidence_path(service_id, item_id)
    if item["file_path"] != path.name:
        raise ValueError(f"invalid evidence path for item {item_id}")
    try:
        return item, path.open("rb")
    except OSError:
        logger.exception("quarantine.evidence.read_failed", extra={"service_id": service_id, "item_id": item_id})
        raise


def purge_evidence(service_id: str, item_id: int | None = None) -> dict[str, int]:
    with _service_lock(service_id):
        if item_id is None:
            items = list_quarantine_evidence(service_id, limit=2**31 - 1)
        else:
            item = get_quarantine_evidence(service_id, item_id)
            items = [item] if item is not None else []
        for item in items:
            if not _unlink_evidence(service_id, item):
                raise OSError(f"could not delete quarantine evidence {item['id']}")
        purged = delete_quarantine_evidence(service_id, [int(item["id"]) for item in items])
        return {"purged": purged}


def record_in_flight(
    service_id: str,
    buffer_filename: str,
    rows: list[tuple[str, int, int | None]],
    table_name: str = "logs",
) -> None:
    """Persist the (file_name, row_count, file_size) tuples that BELONG to a
    buffer Parquet, BEFORE the Parquet is written.

    On crash recovery, ``list_in_flight`` returns these tuples so the sweep
    can promote them into ``ingested_files`` without re-parsing the buffer.
    Upsert semantics: a re-run of the same chunk (same deterministic
    buffer filename) overwrites the prior manifest — never raises.
    """
    con = get_con(service_id)
    con.execute(
        """INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json, started_at, table_name)
           VALUES (?, ?, ?, datetime('now'), ?)
           ON CONFLICT(buffer_filename, table_name) DO UPDATE SET
               source_name = excluded.source_name,
               files_json = excluded.files_json,
               started_at = excluded.started_at""",
        (buffer_filename, service_id, json.dumps(rows), table_name),
    )
    con.commit()


def clear_in_flight(service_id: str, buffer_filename: str, table_name: str = "logs") -> None:
    """Drop the in_flight row for ``buffer_filename`` after its files have
    been committed to ``ingested_files``. Idempotent."""
    con = get_con(service_id)
    con.execute(
        "DELETE FROM ingest_in_flight WHERE source_name = ? AND buffer_filename = ? AND table_name = ?",
        (service_id, buffer_filename, table_name),
    )
    con.commit()


def list_in_flight(service_id: str, table_name: str = "logs") -> list[tuple[str, list[tuple[str, int, int | None]]]]:
    """Return [(buffer_filename, [(file_name, row_count, file_size), ...]), ...]
    for every pending row belonging to this service. Used by the crash-
    recovery sweep at the start of every ingest tick."""
    con = get_con(service_id)
    rows = con.execute(
        "SELECT buffer_filename, files_json FROM ingest_in_flight WHERE source_name = ? AND table_name = ?",
        (service_id, table_name),
    ).fetchall()
    out: list[tuple[str, list[tuple[str, int, int | None]]]] = []
    for r in rows:
        try:
            tuples = [tuple(t) for t in json.loads(r["files_json"] or "[]")]
        except (json.JSONDecodeError, TypeError):
            tuples = []
        out.append((r["buffer_filename"], tuples))
    return out


_RUM_TABLES_SQL = "table_name IN ('client_vitals', 'client_errors')"


def get_rum_beacons_avg_per_day(service_id: str) -> float | None:
    """Mean of per-day RUM beacon rows (client_vitals + client_errors) in ``ingested_files``."""
    con = get_con(service_id)
    row = con.execute(
        f"SELECT AVG(daily_beacons) FROM (SELECT SUM(row_count) as daily_beacons "
        f"FROM ingested_files WHERE source_name = ? AND {_RUM_TABLES_SQL} GROUP BY file_date) sub",
        (service_id,),
    ).fetchone()
    val = row[0]
    return float(val) if val is not None else None


def get_rum_beacons_by_date(service_id: str, start_date: str, end_date: str) -> dict[str, int]:
    """``{file_date: beacon_rows}`` for RUM tables in ``[start_date, end_date)``."""
    con = get_con(service_id)
    rows = con.execute(
        f"SELECT file_date as date, SUM(row_count) as beacon_count FROM ingested_files "
        f"WHERE source_name = ? AND {_RUM_TABLES_SQL} AND file_date >= ? AND file_date < ? "
        "GROUP BY file_date ORDER BY date",
        (service_id, start_date, end_date),
    ).fetchall()
    return {str(row["date"]): row["beacon_count"] for row in rows if row["date"]}


def get_node_count_avg(service_id: str) -> float | None:
    """Average number of files-per-flush, derived from the basename timestamp.

    Used by routers/usage.py prefill estimator. The basename always starts with
    YYYY-MM-DDTHH:MM:SS — the first 'T' in the path is always the timestamp T
    (bucket/prefix segments are lowercase + numeric). Grouping by that 19-char
    substring is equivalent to the prior Python regex over file_name, but runs
    entirely in SQLite instead of dragging every row across the boundary.

    Fast/slow split (mirrors ``get_log_accounting_counts``): the fast arm
    filters on ``file_date IS NOT NULL``, which is covered by the composite
    ``idx_ingested_files_source_date`` index — lets SQLite walk only the
    canonical-basename rows directly via the index instead of scanning the
    full source partition and per-row evaluating ``instr(file_name, 'T')``.
    The slow arm keeps the ``instr`` guard for rows with NULL file_date
    (legacy / test / ad-hoc backfills) so the average stays semantically
    equivalent to the pre-change behavior.
    """
    con = get_con(service_id)
    row = con.execute(
        """SELECT avg(c) AS avg_c FROM (
               -- Fast arm: file_date IS NOT NULL implies the basename matches
               -- the canonical Fastly pattern per _migration_002, so the
               -- substr group-by always succeeds without an instr() guard.
               SELECT count(*) AS c FROM ingested_files
               WHERE source_name = ? AND file_date IS NOT NULL
               GROUP BY substr(file_name, strpos(file_name, 'T') - 10, 19)
               UNION ALL
               -- Slow arm: rows without a parseable basename. Typically
               -- zero rows in prod but kept so test fixtures + legacy
               -- uploads still contribute to the average.
               SELECT count(*) AS c FROM ingested_files
               WHERE source_name = ? AND file_date IS NULL AND strpos(file_name, 'T') >= 11
               GROUP BY substr(file_name, strpos(file_name, 'T') - 10, 19)
           ) AS combined""",
        (service_id, service_id),
    ).fetchone()
    if not row or row["avg_c"] is None:
        return None
    return float(row["avg_c"])
