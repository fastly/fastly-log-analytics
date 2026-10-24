"""Shared in-memory test doubles for High-Scale tests."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

DATA_PLANE_MODES = ("standard", "high_scale")

LEDGER_STATUSES = (
    "discovered",
    "claimed",
    "appended",
    "archived",
    "acknowledged",
    "source_missing",
    "source_deleted",
)
TERMINAL_STATUSES = {"acknowledged", "source_missing", "source_deleted"}


@dataclass(frozen=True)
class SourceObject:
    object_id: str
    service_id: str
    domain: str
    object_key: str
    checksum: str
    size_bytes: int
    version: str
    status: str
    worker_id: str | None
    lease_generation: int
    lease_until: str | None
    accepted_rows: int
    malformed_rows: int
    archive_manifest_id: str | None
    archive_owner_epoch: int | None
    last_error: str | None


@dataclass(frozen=True)
class ClaimAttempt:
    object_id: str
    claimed: bool
    lease_generation: int
    status: str


class HighScaleLedger:
    """In-memory SQLite test double for HighScaleLedger."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._con = sqlite3.connect(str(path))
        self._con.row_factory = sqlite3.Row
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS source_objects (
                object_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                object_key TEXT NOT NULL,
                checksum TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                version TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                lease_generation INTEGER NOT NULL DEFAULT 0,
                lease_until INTEGER,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                malformed_rows INTEGER NOT NULL DEFAULT 0,
                archive_manifest_id TEXT,
                archive_owner_epoch INTEGER,
                last_error TEXT,
                UNIQUE (service_id, object_key)
            )
            """
        )
        self._con.commit()

    def close(self) -> None:
        self._con.close()

    def discover(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int = 0,
        version: str = "",
    ) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        existing = self._get(object_id)
        if existing is not None:
            if existing.checksum != checksum or (version and existing.version and existing.version != version):
                raise ValueError(f"source object identity changed for {service_id}:{object_key}")
            return existing
        self._con.execute(
            """
            INSERT INTO source_objects (
                object_id, service_id, domain, object_key, checksum, size_bytes, version, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'discovered')
            """,
            (object_id, service_id, domain, object_key, checksum, size_bytes, version),
        )
        self._con.commit()
        return self._require(object_id)

    def claim(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        lease_seconds: int = 300,
        now: datetime | None = None,
    ) -> ClaimAttempt:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        now_dt = _utc(now)
        now_epoch = int(now_dt.timestamp())
        lease_until = int((now_dt + timedelta(seconds=lease_seconds)).timestamp())
        cur = self._con.execute(
            """
            UPDATE source_objects
            SET status = 'claimed',
                worker_id = ?,
                lease_generation = lease_generation + 1,
                lease_until = ?,
                last_error = NULL
            WHERE object_id = ?
              AND status IN ('discovered', 'claimed')
              AND (lease_until IS NULL OR lease_until <= ?)
            """,
            (worker_id, lease_until, object_id, now_epoch),
        )
        self._con.commit()
        updated = self._require(object_id)
        if cur.rowcount == 0 and updated.status not in {"discovered", "claimed"}:
            raise ValueError(f"source object is not claimable: {updated.status}")
        return ClaimAttempt(
            object_id=object_id,
            claimed=cur.rowcount == 1,
            lease_generation=updated.lease_generation,
            status=updated.status,
        )

    def record_counts(
        self,
        service_id: str,
        object_key: str,
        *,
        accepted_rows: int,
        malformed_rows: int,
    ) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        self._con.execute(
            """
            UPDATE source_objects
            SET accepted_rows = ?, malformed_rows = ?
            WHERE object_id = ?
            """,
            (accepted_rows, malformed_rows, object_id),
        )
        self._con.commit()
        return self._require(object_id)

    def mark_appended(self, service_id: str, object_key: str, lease_generation: int) -> SourceObject:
        return self._transition(service_id, object_key, lease_generation, {"claimed"}, "appended")

    def mark_archived(
        self,
        service_id: str,
        object_key: str,
        lease_generation: int,
        archive_manifest_id: str,
        owner_epoch: int,
    ) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.lease_generation != lease_generation or current.status not in {"appended", "archived"}:
            raise ValueError("cannot mark archived with stale lease or invalid status")
        self._con.execute(
            """
            UPDATE source_objects
            SET status = 'archived',
                archive_manifest_id = ?,
                archive_owner_epoch = ?
            WHERE object_id = ?
            """,
            (archive_manifest_id, owner_epoch, object_id),
        )
        self._con.commit()
        return self._require(object_id)

    def acknowledge(self, service_id: str, object_key: str, archive_manifest_id: str) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.status == "acknowledged" and current.archive_manifest_id == archive_manifest_id:
            return current
        if current.status != "archived" or current.archive_manifest_id != archive_manifest_id:
            raise ValueError("cannot acknowledge source before matching archive manifest is recorded")
        self._con.execute(
            """
            UPDATE source_objects
            SET status = 'acknowledged', lease_until = NULL
            WHERE object_id = ?
            """,
            (object_id,),
        )
        self._con.commit()
        return self._require(object_id)

    def mark_failed(self, service_id: str, object_key: str, lease_generation: int, error: str) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.lease_generation != lease_generation or current.status in TERMINAL_STATUSES:
            raise ValueError("cannot mark failed with stale lease or terminal status")
        self._con.execute(
            """
            UPDATE source_objects
            SET status = 'discovered',
                worker_id = NULL,
                lease_until = NULL,
                last_error = ?
            WHERE object_id = ?
            """,
            (error[:500], object_id),
        )
        self._con.commit()
        return self._require(object_id)

    def mark_source_missing(
        self,
        service_id: str,
        object_key: str,
        lease_generation: int | None = None,
        *,
        error: str = "",
    ) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.status == "source_missing":
            return current
        if (lease_generation is not None and current.lease_generation != lease_generation) or (
            current.status in TERMINAL_STATUSES
        ):
            raise ValueError("cannot mark source missing with stale lease or terminal status")
        self._con.execute(
            """
            UPDATE source_objects
            SET status = 'source_missing',
                worker_id = NULL,
                lease_until = NULL,
                last_error = ?
            WHERE object_id = ?
            """,
            (error[:500], object_id),
        )
        self._con.commit()
        return self._require(object_id)

    def authorize_source_delete(
        self,
        service_id: str,
        object_key: str,
        archive_manifest_id: str,
        *,
        current_owner_epoch: int,
    ) -> SourceObject:
        current = self._require(_object_id(service_id, object_key))
        if current.status not in {"acknowledged", "source_deleted"}:
            raise ValueError("source deletion requires acknowledged archive state")
        if current.archive_manifest_id != archive_manifest_id:
            raise ValueError("source deletion manifest does not match ledger")
        if current.archive_owner_epoch != current_owner_epoch:
            raise ValueError("source deletion owner epoch does not match ledger")
        return current

    def mark_source_deleted(self, service_id: str, object_key: str, archive_manifest_id: str) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.status == "source_deleted" and current.archive_manifest_id == archive_manifest_id:
            return current
        if current.status != "acknowledged" or current.archive_manifest_id != archive_manifest_id:
            raise ValueError("cannot mark source deleted before acknowledgement")
        self._con.execute(
            "UPDATE source_objects SET status = 'source_deleted' WHERE object_id = ?",
            (object_id,),
        )
        self._con.commit()
        return self._require(object_id)

    def expired_claims(
        self,
        service_id: str | None = None,
        domain: str | None = None,
        *,
        limit: int | None = None,
        now: datetime | None = None,
    ) -> list[SourceObject]:
        now_epoch = int(_utc(now).timestamp())
        clauses = ["status = 'claimed'", "lease_until IS NOT NULL", "lease_until <= ?"]
        params: list[object] = [now_epoch]
        if service_id is not None:
            clauses.append("service_id = ?")
            params.append(service_id)
        if domain is not None:
            clauses.append("domain = ?")
            params.append(domain)
        sql = f"SELECT * FROM source_objects WHERE {' AND '.join(clauses)} ORDER BY object_key"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._con.execute(sql, tuple(params)).fetchall()
        return [self._row_to_source(row) for row in rows]

    def count(self) -> int:
        row = self._con.execute("SELECT COUNT(*) FROM source_objects").fetchone()
        return int(row[0]) if row else 0

    def source(self, service_id: str, object_key: str) -> SourceObject | None:
        return self._get(_object_id(service_id, object_key))

    def _transition(
        self,
        service_id: str,
        object_key: str,
        lease_generation: int,
        allowed_from: set[str],
        target_status: str,
    ) -> SourceObject:
        object_id = _object_id(service_id, object_key)
        current = self._require(object_id)
        if current.lease_generation != lease_generation or current.status not in allowed_from | {target_status}:
            raise ValueError(f"cannot transition {current.status} to {target_status}")
        self._con.execute(
            "UPDATE source_objects SET status = ? WHERE object_id = ?",
            (target_status, object_id),
        )
        self._con.commit()
        return self._require(object_id)

    def _get(self, object_id: str) -> SourceObject | None:
        row = self._con.execute("SELECT * FROM source_objects WHERE object_id = ?", (object_id,)).fetchone()
        return self._row_to_source(row) if row is not None else None

    def _require(self, object_id: str) -> SourceObject:
        item = self._get(object_id)
        if item is None:
            raise KeyError(f"unknown source object: {object_id}")
        return item

    @staticmethod
    def _row_to_source(row: sqlite3.Row) -> SourceObject:
        lease_until = (
            datetime.fromtimestamp(int(row["lease_until"]), tz=UTC).isoformat()
            if row["lease_until"] is not None
            else None
        )
        return SourceObject(
            object_id=str(row["object_id"]),
            service_id=str(row["service_id"]),
            domain=str(row["domain"]),
            object_key=str(row["object_key"]),
            checksum=str(row["checksum"]),
            size_bytes=int(row["size_bytes"]),
            version=str(row["version"]),
            status=str(row["status"]),
            worker_id=str(row["worker_id"]) if row["worker_id"] is not None else None,
            lease_generation=int(row["lease_generation"]),
            lease_until=lease_until,
            accepted_rows=int(row["accepted_rows"]),
            malformed_rows=int(row["malformed_rows"]),
            archive_manifest_id=str(row["archive_manifest_id"]) if row["archive_manifest_id"] is not None else None,
            archive_owner_epoch=int(row["archive_owner_epoch"]) if row["archive_owner_epoch"] is not None else None,
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
        )


@dataclass(frozen=True)
class OwnershipRecord:
    service_id: str
    current_owner: str
    owner_epoch: int
    source_cursor: str
    rollback_ready: bool
    updated_at: str


class OwnershipStore:
    """In-memory SQLite test double for OwnershipStore."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._con = sqlite3.connect(str(path))
        self._con.row_factory = sqlite3.Row
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS service_ownership (
                service_id TEXT PRIMARY KEY,
                current_owner TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL,
                source_cursor TEXT NOT NULL,
                rollback_ready INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS service_domain_cursors (
                service_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                source_cursor TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (service_id, domain)
            )
            """
        )
        self._con.commit()

    def close(self) -> None:
        self._con.close()

    def initialize(
        self,
        service_id: str,
        *,
        owner: str = "standard",
        source_cursor: str = "",
        rollback_ready: bool = True,
    ) -> OwnershipRecord:
        _validate_owner(owner)
        existing = self.get(service_id)
        if existing is not None:
            return existing
        now = _now_iso()
        self._con.execute(
            """
            INSERT INTO service_ownership (
                service_id, current_owner, owner_epoch, source_cursor, rollback_ready, updated_at
            ) VALUES (?, ?, 1, ?, ?, ?)
            """,
            (service_id, owner, source_cursor, int(rollback_ready), now),
        )
        self._con.commit()
        return self._require(service_id)

    def fence_cutover(
        self,
        service_id: str,
        new_owner: str,
        *,
        expected_epoch: int,
        source_cursor: str,
        rollback_ready: bool = True,
    ) -> OwnershipRecord:
        _validate_owner(new_owner)
        now = _now_iso()
        cur = self._con.execute(
            """
            UPDATE service_ownership
            SET current_owner = ?,
                owner_epoch = owner_epoch + 1,
                source_cursor = ?,
                rollback_ready = ?,
                updated_at = ?
            WHERE service_id = ? AND owner_epoch = ?
            """,
            (new_owner, source_cursor, int(rollback_ready), now, service_id, expected_epoch),
        )
        self._con.commit()
        if cur.rowcount != 1:
            raise ValueError(f"ownership epoch fence failed for {service_id}")
        return self._require(service_id)

    def advance_cursor(
        self,
        service_id: str,
        source_cursor: str,
        *,
        expected_owner: str,
        expected_epoch: int,
        domain: str | None = None,
    ) -> OwnershipRecord:
        _validate_owner(expected_owner)
        now = _now_iso()
        cur = self._con.execute(
            """
            UPDATE service_ownership
            SET source_cursor = ?, updated_at = ?
            WHERE service_id = ? AND current_owner = ? AND owner_epoch = ?
            """,
            (source_cursor, now, service_id, expected_owner, expected_epoch),
        )
        if cur.rowcount == 1 and domain is not None:
            self._con.execute(
                """
                INSERT INTO service_domain_cursors (service_id, domain, source_cursor, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (service_id, domain)
                DO UPDATE SET source_cursor = excluded.source_cursor, updated_at = excluded.updated_at
                """,
                (service_id, domain, source_cursor, now),
            )
        self._con.commit()
        if cur.rowcount != 1:
            raise ValueError(f"cannot advance cursor with stale owner fence for {service_id}")
        return self._require(service_id)

    def source_cursor_for(self, service_id: str, domain: str) -> str:
        row = self._con.execute(
            "SELECT source_cursor FROM service_domain_cursors WHERE service_id = ? AND domain = ?",
            (service_id, domain),
        ).fetchone()
        if row is not None and str(row["source_cursor"]):
            return str(row["source_cursor"])
        return "__initial__"

    def get(self, service_id: str) -> OwnershipRecord | None:
        row = self._con.execute(
            "SELECT * FROM service_ownership WHERE service_id = ?",
            (service_id,),
        ).fetchone()
        if row is None:
            return None
        return OwnershipRecord(
            service_id=str(row["service_id"]),
            current_owner=str(row["current_owner"]),
            owner_epoch=int(row["owner_epoch"]),
            source_cursor=str(row["source_cursor"]),
            rollback_ready=bool(row["rollback_ready"]),
            updated_at=str(row["updated_at"]),
        )

    def _require(self, service_id: str) -> OwnershipRecord:
        record = self.get(service_id)
        if record is None:
            raise KeyError(f"unknown service ownership: {service_id}")
        return record


def _object_id(service_id: str, object_key: str) -> str:
    digest = hashlib.sha256(f"{service_id}:{object_key}".encode()).hexdigest()[:24]
    return f"src-{digest}"


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _validate_owner(owner: str) -> None:
    if owner not in DATA_PLANE_MODES:
        raise ValueError(f"unknown data plane owner: {owner}")


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


# ---------------------------------------------------------------------------
# Shared ClickHouse Container Integration Helpers
# ---------------------------------------------------------------------------

_SQL_ROOT = Path("backend/high_scale/sql")


def free_loopback_port() -> int:
    """Bind an ephemeral loopback port and return the port number."""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def clickhouse_single_node_ddl(sql_root: Path = _SQL_ROOT) -> list[str]:
    """Load High-Scale ClickHouse schema SQL statements rewritten for single-node MergeTree."""
    import re

    statements: list[str] = []
    for name in ("publication_schema.sql", "request_schema.sql", "rum_schema.sql"):
        text = (sql_root / name).read_text()
        text = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            text,
        )
        text = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", text)
        text = re.sub(r"\nTTL [^;]*;", ";", text)
        text = re.sub(r"--[^\n]*", "", text)
        statements += [s.strip() for s in text.split(";") if s.strip()]
    return statements


def wait_for_clickhouse_ready(base_url: str, timeout_s: float = 90.0) -> None:
    """Poll ClickHouse HTTP ``/ping`` until it responds ``Ok.`` or raise ``RuntimeError``."""
    import time

    import httpx

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if httpx.get(f"{base_url}ping", timeout=2).text.strip() == "Ok.":
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise RuntimeError("clickhouse-server did not become ready")
