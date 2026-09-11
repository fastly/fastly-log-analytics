"""Lease-fenced source ledger for the high-scale ingestion plane."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass


class ArchiveNotVerified(RuntimeError):
    """Raised when a source object is not safe to delete."""


class ChecksumMismatch(RuntimeError):
    """Raised when a discovered key changes identity."""


@dataclass(frozen=True)
class SourceObject:
    object_id: str
    service_id: str
    domain: str
    object_key: str
    checksum: str
    size_bytes: int
    version: str | None


@dataclass(frozen=True)
class ClaimResult:
    object_id: str
    claimed: bool
    lease_generation: int


@dataclass(frozen=True)
class DeletionAuthorization:
    object_id: str
    archive_manifest_id: str
    owner_epoch: int


class HighScaleLedger:
    """SQLite-backed control ledger suitable for local tests and single writers."""

    def __init__(self, database: str = ":memory:") -> None:
        self._con = sqlite3.connect(database)
        self._con.row_factory = sqlite3.Row
        self._con.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_objects (
                object_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                object_key TEXT NOT NULL,
                checksum TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                version TEXT,
                status TEXT NOT NULL,
                owner TEXT,
                lease_until REAL,
                lease_generation INTEGER NOT NULL DEFAULT 0,
                archive_manifest_id TEXT,
                owner_epoch INTEGER,
                malformed_rows INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                UNIQUE(service_id, object_key)
            )
            """
        )

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
        version: str | None = None,
    ) -> SourceObject:
        row = self._con.execute(
            "SELECT * FROM source_objects WHERE service_id=? AND object_key=?",
            (service_id, object_key),
        ).fetchone()
        if row:
            if row["checksum"] != checksum or row["version"] != version:
                raise ChecksumMismatch(f"source identity changed for {object_key}")
            return self._source(row)
        object_id = f"{service_id}:{domain}:{checksum}:{object_key}"
        self._con.execute(
            "INSERT INTO source_objects "
            "(object_id,service_id,domain,object_key,checksum,size_bytes,version,status) "
            "VALUES (?,?,?,?,?,?,?, 'discovered')",
            (object_id, service_id, domain, object_key, checksum, size_bytes, version),
        )
        self._con.commit()
        return self._source(
            self._con.execute("SELECT * FROM source_objects WHERE object_id=?", (object_id,)).fetchone()
        )

    def claim(self, object_key: str, owner: str, *, lease_seconds: float = 300.0) -> ClaimResult:
        now = time.time()
        row = self._con.execute(
            "SELECT object_id,status,lease_until,lease_generation FROM source_objects WHERE object_key=?",
            (object_key,),
        ).fetchone()
        if row is None:
            raise KeyError(object_key)
        active = row["status"] == "claimed" and (row["lease_until"] or 0) > now
        if active:
            return ClaimResult(row["object_id"], False, row["lease_generation"])
        generation = int(row["lease_generation"]) + 1
        self._con.execute(
            "UPDATE source_objects SET status='claimed',owner=?,lease_until=?,lease_generation=? WHERE object_id=?",
            (owner, now + lease_seconds, generation, row["object_id"]),
        )
        self._con.commit()
        return ClaimResult(row["object_id"], True, generation)

    def record_counts(self, object_key: str, *, accepted_rows: int, malformed_rows: int) -> None:
        if min(accepted_rows, malformed_rows) < 0:
            raise ValueError("ledger counts must be non-negative")
        self._con.execute(
            "UPDATE source_objects SET accepted_rows=?,malformed_rows=? WHERE object_key=?",
            (accepted_rows, malformed_rows, object_key),
        )
        self._con.commit()

    def mark_appended(self, object_key: str, lease_generation: int) -> None:
        self._transition_claim(object_key, lease_generation, "appended", current_status="claimed")

    def mark_archived(self, object_key: str, lease_generation: int, manifest_id: str, owner_epoch: int) -> None:
        if not manifest_id or owner_epoch < 0:
            raise ValueError("archive manifest and owner epoch are required")
        self._transition_claim(object_key, lease_generation, "archived", current_status="appended")
        self._con.execute(
            "UPDATE source_objects SET archive_manifest_id=?,owner_epoch=? WHERE object_key=?",
            (manifest_id, owner_epoch, object_key),
        )
        self._con.commit()

    def acknowledge(self, object_key: str, manifest_id: str) -> None:
        row = self._con.execute(
            "SELECT status,archive_manifest_id,malformed_rows FROM source_objects WHERE object_key=?",
            (object_key,),
        ).fetchone()
        if row is None:
            raise KeyError(object_key)
        if row["status"] != "archived" or row["archive_manifest_id"] != manifest_id:
            raise ArchiveNotVerified(f"archive is not complete for {object_key}")
        self._con.execute("UPDATE source_objects SET status='acknowledged' WHERE object_key=?", (object_key,))
        self._con.commit()

    def mark_source_deleted(self, object_key: str, manifest_id: str) -> None:
        row = self._con.execute(
            "SELECT status,archive_manifest_id FROM source_objects WHERE object_key=?",
            (object_key,),
        ).fetchone()
        if row is None:
            raise KeyError(object_key)
        if row["status"] == "source_deleted" and row["archive_manifest_id"] == manifest_id:
            return
        if row["status"] != "acknowledged" or row["archive_manifest_id"] != manifest_id:
            raise ArchiveNotVerified(f"source deletion was not acknowledged for {object_key}")
        self._con.execute("UPDATE source_objects SET status='source_deleted' WHERE object_key=?", (object_key,))
        self._con.commit()

    def authorize_source_delete(
        self,
        object_key: str,
        archive_manifest_id: str,
        *,
        current_owner_epoch: int,
    ) -> DeletionAuthorization:
        row = self._con.execute(
            "SELECT object_id,status,archive_manifest_id,owner_epoch FROM source_objects WHERE object_key=?",
            (object_key,),
        ).fetchone()
        if (
            row is None
            or row["status"] not in {"acknowledged", "archived"}
            or row["archive_manifest_id"] != archive_manifest_id
            or row["owner_epoch"] != current_owner_epoch
        ):
            raise ArchiveNotVerified(f"source deletion is not authorized for {object_key}")
        return DeletionAuthorization(row["object_id"], archive_manifest_id, current_owner_epoch)

    def count(self) -> int:
        return int(self._con.execute("SELECT count(*) FROM source_objects").fetchone()[0])

    def _transition_claim(self, object_key: str, generation: int, status: str, *, current_status: str) -> None:
        cur = self._con.execute(
            "UPDATE source_objects SET status=?,lease_until=NULL WHERE object_key=? "
            "AND status=? AND lease_generation=?",
            (status, object_key, current_status, generation),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"stale or missing claim for {object_key}")
        self._con.commit()

    @staticmethod
    def _source(row: sqlite3.Row) -> SourceObject:
        return SourceObject(
            row["object_id"],
            row["service_id"],
            row["domain"],
            row["object_key"],
            row["checksum"],
            row["size_bytes"],
            row["version"],
        )
