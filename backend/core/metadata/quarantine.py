"""Quarantined-file tracking in metadata SQLite.

Tracks raw .gz files copied to the ``errors/`` FOS prefix because they
contained corrupt/invalid lines during ingestion. Provides CRUD for the
``quarantined_files`` table — admin listing, summary stats, and auto-purge.
"""

from __future__ import annotations

import json
import logging

from backend.core.metadata.base import get_con

logger = logging.getLogger(__name__)


def insert_quarantined_file(
    service_id: str,
    file_name: str,
    source_name: str,
    fos_key: str,
    error_key: str,
    meta_key: str,
    valid_rows: int,
    corrupt_rows: int,
    file_size_bytes: int | None,
    corrupt_samples: list[str] | None = None,
    reason_counts: dict[str, int] | None = None,
    error_size_bytes: int | None = None,
) -> None:
    con = get_con(service_id)
    con.execute(
        """
        INSERT OR REPLACE INTO quarantined_files
            (file_name, source_name, fos_key, error_key, meta_key,
             valid_rows, corrupt_rows, file_size_bytes, corrupt_samples,
             reason_counts, error_size_bytes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            file_name,
            source_name,
            fos_key,
            error_key,
            meta_key,
            valid_rows,
            corrupt_rows,
            file_size_bytes,
            json.dumps(corrupt_samples or []),
            json.dumps(reason_counts or {}),
            error_size_bytes,
        ),
    )
    con.commit()


def _parse_json_col(raw: str | None, default=None):
    if not raw:
        return default if default is not None else {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else {}


def list_quarantined_files(service_id: str, limit: int = 100, offset: int = 0) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        """
        SELECT id, file_name, source_name, fos_key, error_key, meta_key,
               valid_rows, corrupt_rows, file_size_bytes, corrupt_samples,
               quarantined_at, reason_counts
        FROM quarantined_files
        ORDER BY quarantined_at DESC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    ).fetchall()
    result = []
    for r in rows:
        result.append(
            {
                "id": r[0],
                "file_name": r[1],
                "source_name": r[2],
                "fos_key": r[3],
                "error_key": r[4],
                "meta_key": r[5],
                "valid_rows": r[6],
                "corrupt_rows": r[7],
                "file_size_bytes": r[8],
                "corrupt_samples": _parse_json_col(r[9], default=[]),
                "quarantined_at": r[10],
                "reason_counts": _parse_json_col(r[11] if len(r) > 11 else None, default={}),
            }
        )
    return result


def get_quarantine_summary(service_id: str) -> dict:
    con = get_con(service_id)
    row = con.execute(
        """
        SELECT count(*),
               coalesce(sum(corrupt_rows), 0),
               min(quarantined_at),
               max(quarantined_at)
        FROM quarantined_files
        """
    ).fetchone()
    return {
        "total_files": row[0] if row else 0,
        "total_corrupt_rows": row[1] if row else 0,
        "oldest_at": row[2] if row else None,
        "newest_at": row[3] if row else None,
    }


def get_expired_quarantined_files(service_id: str, retention_days: int = 14) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        f"SELECT id, error_key, meta_key FROM quarantined_files WHERE quarantined_at < datetime('now', '-{retention_days} days')"
    ).fetchall()
    return [{"id": r[0], "error_key": r[1], "meta_key": r[2]} for r in rows]


def get_quarantined_file_by_id(service_id: str, quarantine_id: int) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        """
        SELECT id, file_name, source_name, fos_key, error_key, meta_key,
               valid_rows, corrupt_rows, file_size_bytes, corrupt_samples,
               quarantined_at, reason_counts
        FROM quarantined_files
        WHERE id = ?
        """,
        (quarantine_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "file_name": row[1],
        "source_name": row[2],
        "fos_key": row[3],
        "error_key": row[4],
        "meta_key": row[5],
        "valid_rows": row[6],
        "corrupt_rows": row[7],
        "file_size_bytes": row[8],
        "corrupt_samples": _parse_json_col(row[9], default=[]),
        "quarantined_at": row[10],
        "reason_counts": _parse_json_col(row[11] if len(row) > 11 else None, default={}),
    }


def get_quarantine_storage_total(service_id: str) -> int:
    con = get_con(service_id)
    row = con.execute("SELECT coalesce(sum(error_size_bytes), 0) FROM quarantined_files").fetchone()
    return row[0] if row else 0


def delete_quarantined_rows(service_id: str, ids: list[int]) -> int:
    if not ids:
        return 0
    con = get_con(service_id)
    placeholders = ", ".join("?" * len(ids))
    cur = con.execute(f"DELETE FROM quarantined_files WHERE id IN ({placeholders})", ids)
    con.commit()
    return cur.rowcount
