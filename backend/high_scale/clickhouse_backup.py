"""Native ClickHouse incremental backups for the high-scale serving tables.

This is a durability *accelerator*, never the only rebuild source — FOS
archive artifacts remain the authoritative recovery path
(:mod:`backend.high_scale.recovery`). A backup failure must never authorize
raw source deletion; callers integrate this independently of the deletion
controller's grace-period gate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from backend.core.clickhouse_client import CLICKHOUSE_HIGH_SCALE_TABLES

_DESTINATION_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_STATUS_MAP = {
    "BACKUP_CREATED": "completed",
    "RESTORED": "completed",
    "BACKUP_FAILED": "failed",
    "RESTORE_FAILED": "failed",
}


class BackupClient(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> list[dict]: ...


@dataclass(frozen=True)
class BackupReceipt:
    backup_id: str
    status: str
    uncompressed_size: int
    compressed_size: int
    error: str | None


def run_incremental_clickhouse_backup(
    client: BackupClient,
    *,
    table: str,
    destination: str,
) -> BackupReceipt:
    """Issue ``BACKUP TABLE ... TO Disk(destination)`` and report its outcome.

    ``destination`` names a disk configured in the ClickHouse server's own
    ``storage_configuration`` (e.g. an FOS-backed disk) — this function does
    not create or validate that the disk itself exists, only that the name
    is a safe identifier.
    """
    if table not in CLICKHOUSE_HIGH_SCALE_TABLES:
        raise ValueError(f"ClickHouse table {table!r} is not internally allowlisted for backup")
    if not destination or not _DESTINATION_RE.fullmatch(destination):
        raise ValueError("backup destination must be a safe disk identifier")

    rows = client.execute(f"BACKUP TABLE `{table}` TO Disk('{destination}', '{table}.zip')")
    row = rows[0] if rows else {}
    status = _STATUS_MAP.get(str(row.get("status", "")), "failed")
    error = row.get("error") or None
    return BackupReceipt(
        backup_id=str(row.get("id", "")),
        status=status,
        uncompressed_size=int(row.get("uncompressed_size", 0) or 0),
        compressed_size=int(row.get("compressed_size", 0) or 0),
        error=str(error) if error else None,
    )
