"""Metadata ledger reset helper for ingest tests."""

from __future__ import annotations

from typing import Any

from backend.core.metadata.base import get_con


def clear_ledger(service_id: str, *, quarantine: bool = False) -> tuple[Any, Any]:
    """Delete the service's ``ingest_ledger`` rows (and, optionally, all quarantine rows); return ``(con, cur)``."""
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    if quarantine:
        con.execute("DELETE FROM quarantine_evidence WHERE service_id=?", (service_id,))
        con.execute("DELETE FROM quarantined_files")
    con.commit()
    return con, cur
