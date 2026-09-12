"""Durable owner-epoch fencing for high-scale migration cutovers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class OwnershipRecord:
    service_id: str
    owner_epoch: int
    current_owner: str
    previous_owner: str | None
    source_cursor: str
    drain_complete: bool
    cutover_committed: bool
    rollback_allowed: bool


class OwnershipStore:
    def __init__(self, database: str = ":memory:") -> None:
        self._con = sqlite3.connect(database)
        self._con.row_factory = sqlite3.Row
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS service_ownership (
                service_id TEXT PRIMARY KEY,
                owner_epoch INTEGER NOT NULL,
                current_owner TEXT NOT NULL,
                previous_owner TEXT,
                source_cursor TEXT NOT NULL,
                drain_complete INTEGER NOT NULL DEFAULT 0,
                cutover_committed INTEGER NOT NULL DEFAULT 0,
                rollback_allowed INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._con.commit()

    def close(self) -> None:
        self._con.close()

    def initialize(self, service_id: str, *, owner: str, source_cursor: str) -> OwnershipRecord:
        if not service_id or not owner or not source_cursor:
            raise ValueError("service ownership identity is required")
        self._con.execute(
            """
            INSERT INTO service_ownership (service_id, owner_epoch, current_owner, source_cursor)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(service_id) DO NOTHING
            """,
            (service_id, owner, source_cursor),
        )
        self._con.commit()
        return self.get(service_id)

    def get(self, service_id: str) -> OwnershipRecord:
        row = self._con.execute("SELECT * FROM service_ownership WHERE service_id=?", (service_id,)).fetchone()
        if row is None:
            raise KeyError(service_id)
        return self._record(row)

    def advance_cursor(
        self,
        service_id: str,
        source_cursor: str,
        *,
        expected_owner: str,
        expected_epoch: int,
    ) -> OwnershipRecord:
        if not source_cursor:
            raise ValueError("source cursor is required")
        self._con.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(service_id)
            if current.current_owner != expected_owner or current.owner_epoch != expected_epoch:
                raise ValueError("owner epoch does not match cursor fence")
            self._con.execute(
                "UPDATE service_ownership SET source_cursor=? WHERE service_id=?",
                (source_cursor, service_id),
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return self.get(service_id)

    def begin_drain(self, service_id: str, *, expected_owner: str) -> OwnershipRecord:
        self._con.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(service_id)
            if current.current_owner != expected_owner or current.cutover_committed:
                raise ValueError("owner does not match drain fence")
            self._con.execute(
                "UPDATE service_ownership SET drain_complete=0, rollback_allowed=0 WHERE service_id=?",
                (service_id,),
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return self.get(service_id)

    def mark_drained(self, service_id: str, *, expected_epoch: int, source_cursor: str) -> OwnershipRecord:
        if not source_cursor:
            raise ValueError("drain source cursor is required")
        self._con.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(service_id)
            if current.owner_epoch != expected_epoch or current.cutover_committed:
                raise ValueError("owner epoch does not match drain fence")
            self._con.execute(
                "UPDATE service_ownership SET drain_complete=1, source_cursor=? WHERE service_id=?",
                (source_cursor, service_id),
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return self.get(service_id)

    def commit_cutover(self, service_id: str, *, expected_epoch: int, next_owner: str) -> OwnershipRecord:
        if not next_owner:
            raise ValueError("next owner is required")
        self._con.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(service_id)
            if current.owner_epoch != expected_epoch:
                raise ValueError("owner epoch does not match cutover fence")
            if not current.drain_complete or current.cutover_committed:
                raise ValueError("service is not ready for cutover")
            self._con.execute(
                """
                UPDATE service_ownership
                SET owner_epoch=owner_epoch+1, previous_owner=current_owner,
                    current_owner=?, cutover_committed=1, rollback_allowed=1
                WHERE service_id=?
                """,
                (next_owner, service_id),
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return self.get(service_id)

    def rollback(self, service_id: str, *, expected_epoch: int) -> OwnershipRecord:
        self._con.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(service_id)
            if current.owner_epoch != expected_epoch or not current.rollback_allowed or not current.previous_owner:
                raise ValueError("rollback fence is not satisfied")
            self._con.execute(
                """
                UPDATE service_ownership
                SET owner_epoch=owner_epoch+1, current_owner=previous_owner,
                    previous_owner=NULL, cutover_committed=0, rollback_allowed=0
                WHERE service_id=?
                """,
                (service_id,),
            )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        return self.get(service_id)

    @staticmethod
    def _record(row: sqlite3.Row) -> OwnershipRecord:
        return OwnershipRecord(
            row["service_id"],
            row["owner_epoch"],
            row["current_owner"],
            row["previous_owner"],
            row["source_cursor"],
            bool(row["drain_complete"]),
            bool(row["cutover_committed"]),
            bool(row["rollback_allowed"]),
        )
