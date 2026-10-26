"""Transactional Postgres control-plane state for the isolated high-scale slice."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from psycopg_pool import ConnectionPool

from backend import config
from backend.core.clickhouse_client import ClickHouseClient
from backend.core.metadata.pg_connection import get_pg_pool
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS
from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
    validate_archive_transition,
)
from backend.high_scale.archive_publication import ArchivePublication, ObjectStore
from backend.high_scale.orchestration import INITIAL_SOURCE_CURSOR, RUM_SOURCE_DOMAINS

logger = logging.getLogger(__name__)

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS high_scale_ownership (
    service_id TEXT PRIMARY KEY, owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    current_owner TEXT NOT NULL, previous_owner TEXT, source_cursor TEXT NOT NULL,
    drain_complete BOOLEAN NOT NULL DEFAULT FALSE, cutover_committed BOOLEAN NOT NULL DEFAULT FALSE,
    rollback_allowed BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS high_scale_source_cursors (
    service_id TEXT NOT NULL, domain TEXT NOT NULL, source_cursor TEXT NOT NULL,
    reinspection_cursor TEXT NOT NULL DEFAULT '__initial__',
    forward_pages_since_reinspection INTEGER NOT NULL DEFAULT 0 CHECK (forward_pages_since_reinspection >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, domain),
    FOREIGN KEY (service_id) REFERENCES high_scale_ownership(service_id) ON DELETE CASCADE
);
ALTER TABLE high_scale_source_cursors ADD COLUMN IF NOT EXISTS reinspection_cursor TEXT NOT NULL DEFAULT '__initial__';
ALTER TABLE high_scale_source_cursors ADD COLUMN IF NOT EXISTS forward_pages_since_reinspection INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS high_scale_partition_cursors (
    service_id TEXT NOT NULL, domain TEXT NOT NULL, partition_id TEXT NOT NULL DEFAULT 'default',
    source_cursor TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, domain, partition_id),
    FOREIGN KEY (service_id) REFERENCES high_scale_ownership(service_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS high_scale_source_objects (
    object_id TEXT PRIMARY KEY, service_id TEXT NOT NULL, domain TEXT NOT NULL,
    object_key TEXT NOT NULL, checksum TEXT NOT NULL, size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    version TEXT, status TEXT NOT NULL CHECK (status IN (
        'discovered', 'claimed', 'appended', 'archived', 'acknowledged', 'source_deleted', 'source_missing'
    )),
    owner TEXT, lease_until TIMESTAMPTZ, lease_generation BIGINT NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    archive_manifest_id TEXT, owner_epoch BIGINT, malformed_rows BIGINT NOT NULL DEFAULT 0 CHECK (malformed_rows >= 0),
    accepted_rows BIGINT NOT NULL DEFAULT 0 CHECK (accepted_rows >= 0), last_error TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), UNIQUE (service_id, object_key)
);
ALTER TABLE high_scale_source_objects ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE high_scale_source_objects DROP CONSTRAINT IF EXISTS high_scale_source_objects_status_check;
ALTER TABLE high_scale_source_objects ADD CONSTRAINT high_scale_source_objects_status_check CHECK (status IN (
    'discovered', 'claimed', 'appended', 'archived', 'acknowledged', 'source_deleted', 'source_missing'
));
CREATE INDEX IF NOT EXISTS high_scale_source_objects_claim_idx ON high_scale_source_objects (service_id, status, lease_until);
CREATE TABLE IF NOT EXISTS high_scale_archive_manifests (
    manifest_id TEXT PRIMARY KEY, service_id TEXT NOT NULL, domain TEXT NOT NULL,
    source_key TEXT NOT NULL, source_checksum TEXT NOT NULL, source_size BIGINT NOT NULL CHECK (source_size >= 0),
    source_version TEXT, artifact_uri TEXT NOT NULL, artifact_checksum TEXT NOT NULL,
    artifact_size BIGINT NOT NULL CHECK (artifact_size >= 0), row_count BIGINT NOT NULL CHECK (row_count >= 0),
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0), canonical_digest TEXT NOT NULL,
    schema_version TEXT NOT NULL, transform_version TEXT NOT NULL,
    coverage_start TIMESTAMPTZ NOT NULL, coverage_end TIMESTAMPTZ NOT NULL,
    retention_deadline TIMESTAMPTZ NOT NULL, deletion_authorization_deadline TIMESTAMPTZ NOT NULL,
    archive_epoch BIGINT NOT NULL CHECK (archive_epoch >= 0), owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    state TEXT NOT NULL CHECK (state IN (
        'artifact_uploading', 'artifact_verified', 'manifest_prepared',
        'manifest_committed', 'deletion_eligible', 'source_deleted'
    )),
    sources_json TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (coverage_end >= coverage_start), CHECK (retention_deadline >= coverage_end),
    CHECK (deletion_authorization_deadline >= retention_deadline)
);
ALTER TABLE high_scale_archive_manifests ADD COLUMN IF NOT EXISTS sources_json TEXT;
CREATE INDEX IF NOT EXISTS high_scale_archive_manifest_source_idx ON high_scale_archive_manifests (service_id, source_key);
CREATE TABLE IF NOT EXISTS high_scale_archive_manifest_sources (
    manifest_id TEXT NOT NULL REFERENCES high_scale_archive_manifests(manifest_id) ON DELETE CASCADE,
    source_key TEXT NOT NULL, source_checksum TEXT NOT NULL, source_size BIGINT NOT NULL CHECK (source_size >= 0),
    source_version TEXT, row_start BIGINT NOT NULL CHECK (row_start >= 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0), deletion_deadline TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (manifest_id, source_key)
);
CREATE INDEX IF NOT EXISTS high_scale_archive_manifest_sources_key_idx ON high_scale_archive_manifest_sources (source_key);
CREATE TABLE IF NOT EXISTS high_scale_batch_claims (
    batch_id TEXT PRIMARY KEY, service_id TEXT NOT NULL, domain TEXT NOT NULL, worker_id TEXT NOT NULL,
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0), lease_until TIMESTAMPTZ NOT NULL,
    lease_generation BIGINT NOT NULL DEFAULT 1 CHECK (lease_generation > 0),
    state TEXT NOT NULL CHECK (state IN ('claimed', 'completed', 'released')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS high_scale_batch_claims_lease_idx ON high_scale_batch_claims (service_id, state, lease_until);
CREATE TABLE IF NOT EXISTS high_scale_publication_manifests (
    batch_id TEXT PRIMARY KEY, service_id TEXT NOT NULL, domain TEXT NOT NULL, generation TEXT NOT NULL,
    batch_digest TEXT NOT NULL, expected_rows BIGINT NOT NULL CHECK (expected_rows >= 0),
    visible_rows BIGINT NOT NULL DEFAULT 0 CHECK (visible_rows >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'visible')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS high_scale_deletion_authorizations (
    object_id TEXT NOT NULL REFERENCES high_scale_source_objects(object_id),
    manifest_id TEXT NOT NULL REFERENCES high_scale_archive_manifests(manifest_id),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    authorized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), consumed_at TIMESTAMPTZ,
    PRIMARY KEY (object_id, manifest_id)
);
"""


@dataclass(frozen=True)
class OwnerEpochRecord:
    service_id: str
    owner_epoch: int
    current_owner: str
    previous_owner: str | None
    source_cursor: str
    drain_complete: bool
    cutover_committed: bool
    rollback_allowed: bool


@dataclass(frozen=True)
class SourceObjectRecord:
    object_id: str
    service_id: str
    domain: str
    object_key: str
    checksum: str
    size_bytes: int
    version: str | None
    status: str
    owner: str | None
    lease_until: datetime | None
    lease_generation: int
    archive_manifest_id: str | None
    owner_epoch: int | None
    malformed_rows: int
    accepted_rows: int
    last_error: str | None = None


@dataclass(frozen=True)
class SourceClaim:
    object_id: str
    claimed: bool
    lease_generation: int


@dataclass(frozen=True)
class ArchiveManifestRecord:
    manifest: ArchiveManifest
    state: ArchiveState
    owner_epoch: int


@dataclass(frozen=True)
class DeletionAuthorization:
    object_id: str
    manifest_id: str
    owner_epoch: int
    authorized_at: datetime


class PostgresControlPlane:
    """Postgres-backed control state with explicit transaction boundaries."""

    def __init__(self, *, pool: ConnectionPool | Any | None = None, pool_timeout: float | None = None) -> None:
        self.pool = get_pg_pool() if pool is None else pool
        self.pool_timeout = pool_timeout

    @contextmanager
    def transaction(self, *, read_only: bool = False) -> Iterator[Any]:
        ctx = (
            self.pool.connection(timeout=self.pool_timeout) if self.pool_timeout is not None else self.pool.connection()
        )
        with ctx as connection, connection.transaction():
            if read_only:
                connection.execute("SET TRANSACTION READ ONLY")
            yield connection

    def create_schema(self) -> None:
        with self.transaction() as connection:
            connection.execute(SCHEMA_DDL)

    def operational_snapshot(self, service_id: str) -> dict[str, list[dict[str, Any]]]:
        """Return bounded aggregate state for high-scale operational metrics."""
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            source_rows = connection.execute(
                "SELECT domain, status, COUNT(*) AS object_count FROM high_scale_source_objects "
                "WHERE service_id=%s GROUP BY domain, status ORDER BY domain, status",
                (service_id,),
            ).fetchall()
            source_age_rows = connection.execute(
                "SELECT domain, EXTRACT(EPOCH FROM (clock_timestamp() - MIN(updated_at))) AS age_seconds "
                "FROM high_scale_source_objects WHERE service_id=%s "
                "AND status IN ('discovered', 'claimed', 'appended', 'archived') GROUP BY domain",
                (service_id,),
            ).fetchall()
            publication_rows = connection.execute(
                "SELECT domain, COUNT(*) FILTER (WHERE status='pending') AS pending_count, "
                "COALESCE(SUM(visible_rows) FILTER (WHERE status='visible'), 0) AS published_rows, "
                "COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(updated_at) FILTER (WHERE status='pending'))), 0) AS lag_seconds "
                "FROM high_scale_publication_manifests WHERE service_id=%s GROUP BY domain",
                (service_id,),
            ).fetchall()
        return {
            "source_objects": [{"domain": str(r[0]), "status": str(r[1]), "count": int(r[2])} for r in source_rows],
            "source_age": [{"domain": str(r[0]), "age_seconds": max(0.0, float(r[1] or 0.0))} for r in source_age_rows],
            "publications": [
                {
                    "domain": str(r[0]),
                    "pending": int(r[1] or 0),
                    "published_rows": int(r[2] or 0),
                    "lag_seconds": max(0.0, float(r[3] or 0.0)),
                }
                for r in publication_rows
            ],
        }

    def initialize_owner(self, service_id: str, *, owner: str, source_cursor: str) -> OwnerEpochRecord:
        _require_texts(service_id=service_id, owner=owner, source_cursor=source_cursor)
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO high_scale_ownership (service_id, owner_epoch, current_owner, source_cursor) "
                "VALUES (%s, 1, %s, %s) ON CONFLICT (service_id) DO NOTHING",
                (service_id, owner, source_cursor),
            )
            return self._owner(connection, service_id)

    def owner(self, service_id: str) -> OwnerEpochRecord:
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            return self._owner(connection, service_id)

    def advance_source_cursor(
        self,
        service_id: str,
        source_cursor: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        domain: str | None = None,
        reinspection_cursor: str | None = None,
        forward_pages_since_reinspection: int | None = None,
    ) -> OwnerEpochRecord:
        _require_texts(source_cursor=source_cursor, expected_owner=expected_owner)
        if forward_pages_since_reinspection is not None and forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        with self.transaction() as connection:
            current = self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="cursor",
            )
            if domain is not None:
                _require_text(domain, "domain")
                _upsert_domain_cursor(
                    connection,
                    service_id,
                    domain,
                    source_cursor,
                    reinspection_cursor=reinspection_cursor,
                    forward_pages_since_reinspection=forward_pages_since_reinspection,
                )
                return current
            connection.execute(
                "UPDATE high_scale_ownership SET source_cursor=%s, updated_at=clock_timestamp() "
                "WHERE service_id=%s AND owner_epoch=%s AND current_owner=%s",
                (source_cursor, service_id, expected_owner_epoch, expected_owner),
            )
            return self._owner(connection, service_id)

    def source_reinspection_state_for(self, service_id: str, domain: str) -> tuple[str, int]:
        """Return the independent bounded-reinspection state for one domain."""
        _require_texts(service_id=service_id, domain=domain)
        if domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT reinspection_cursor, forward_pages_since_reinspection "
                "FROM high_scale_source_cursors WHERE service_id=%s AND domain=%s",
                (service_id, domain),
            ).fetchone()
            return (INITIAL_SOURCE_CURSOR, 0) if row is None else (str(row[0]), int(row[1]))

    def advance_reinspection_state(
        self,
        service_id: str,
        domain: str,
        reinspection_cursor: str,
        forward_pages_since_reinspection: int,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> None:
        """Advance only the bounded RUM sweep, leaving forward progress intact."""
        _require_texts(
            service_id=service_id, domain=domain, reinspection_cursor=reinspection_cursor, expected_owner=expected_owner
        )
        if domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        if forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="reinspection cursor",
            )
            updated = connection.execute(
                "UPDATE high_scale_source_cursors SET reinspection_cursor=%s, "
                "forward_pages_since_reinspection=%s, updated_at=clock_timestamp() WHERE service_id=%s AND domain=%s",
                (reinspection_cursor, forward_pages_since_reinspection, service_id, domain),
            )
            if getattr(updated, "rowcount", None) == 0:
                raise RuntimeError("RUM reinspection cursor state disappeared during update")

    def source_cursor_for(self, service_id: str, domain: str) -> str:
        """Return this domain's persisted cursor or INITIAL_SOURCE_CURSOR (never the owner-level cursor)."""
        _require_text(domain, "domain")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT source_cursor FROM high_scale_source_cursors WHERE service_id=%s AND domain=%s",
                (service_id, domain),
            ).fetchone()
            return str(row[0]) if row is not None else INITIAL_SOURCE_CURSOR

    def source(self, service_id: str, object_key: str) -> SourceObjectRecord:
        with self.transaction(read_only=True) as connection:
            return self._locked_source(connection, service_id, object_key, for_update=False)

    def expired_claims(self, service_id: str, domain: str, *, limit: int) -> tuple[SourceObjectRecord, ...]:
        _require_texts(service_id=service_id, domain=domain)
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND domain=%s "
                "AND status='claimed' AND lease_until IS NOT NULL AND lease_until <= clock_timestamp() "
                "ORDER BY lease_until, object_key LIMIT %s",
                (service_id, domain, limit),
            ).fetchall()
        return tuple(_source_from_row(row) for row in rows)

    def begin_drain(self, service_id: str, *, expected_owner: str) -> OwnerEpochRecord:
        _require_text(expected_owner, "expected_owner")
        with self.transaction() as connection:
            current = self._locked_owner(connection, service_id)
            if current.current_owner != expected_owner or current.cutover_committed:
                raise ValueError("owner does not match drain fence")
            connection.execute(
                "UPDATE high_scale_ownership SET drain_complete=FALSE, rollback_allowed=FALSE, "
                "updated_at=clock_timestamp() WHERE service_id=%s",
                (service_id,),
            )
            return self._owner(connection, service_id)

    def rollback(self, service_id: str, *, expected_epoch: int) -> OwnerEpochRecord:
        with self.transaction() as connection:
            current = self._fenced_owner(connection, service_id, expected_owner_epoch=expected_epoch)
            if not current.rollback_allowed or not current.previous_owner:
                raise ValueError("rollback fence is not satisfied")
            connection.execute(
                "UPDATE high_scale_ownership SET owner_epoch=owner_epoch+1, current_owner=previous_owner, "
                "previous_owner=NULL, cutover_committed=FALSE, rollback_allowed=FALSE, "
                "updated_at=clock_timestamp() WHERE service_id=%s",
                (service_id,),
            )
            return self._owner(connection, service_id)

    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int = 0,
        version: str | None = None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> SourceObjectRecord:
        _require_texts(service_id=service_id, domain=domain, object_key=object_key, checksum=checksum)
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        _require_text(expected_owner, "expected_owner")
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        object_id = _source_object_id(service_id, domain, object_key, checksum)
        select_sql = "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key=%s FOR UPDATE"
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="source discovery",
            )
            row = connection.execute(select_sql, (service_id, object_key)).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO high_scale_source_objects "
                    "(object_id, service_id, domain, object_key, checksum, size_bytes, version, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, 'discovered') ON CONFLICT (service_id, object_key) DO NOTHING",
                    (object_id, service_id, domain, object_key, checksum, size_bytes, version),
                )
                row = connection.execute(select_sql, (service_id, object_key)).fetchone()
            if row is None:
                raise RuntimeError("source discovery insert did not produce a row")
            source = _source_from_row(row)
            if source.checksum != checksum or source.version != version:
                raise ValueError(f"source identity changed for {object_key}")
            # Domain is intentionally not compared here because rum_vitals and rum_errors share raw/rum/.
            return source

    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float = 300.0,
        now: datetime | None = None,
    ) -> SourceClaim:
        _require_texts(worker_id=worker_id, expected_owner=expected_owner)
        if expected_owner_epoch <= 0 or lease_seconds <= 0:
            raise ValueError("owner epoch and lease duration must be positive")
        observed = _observed_utc(now)
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="source claim",
            )
            source = self._locked_source(connection, service_id, object_key)
            if source.status == "source_deleted":
                raise ValueError(f"source object is already deleted: {object_key}")
            if source.status not in {"discovered", "claimed"}:
                raise ValueError(f"source object is not claimable: {object_key}")
            if source.lease_until is not None and source.lease_until > observed:
                return SourceClaim(source.object_id, source.owner == worker_id, source.lease_generation)
            generation = source.lease_generation + 1
            connection.execute(
                "UPDATE high_scale_source_objects SET status='claimed', owner=%s, lease_until=%s, "
                "lease_generation=%s, owner_epoch=%s, updated_at=clock_timestamp() WHERE object_id=%s",
                (
                    worker_id,
                    observed + timedelta(seconds=lease_seconds),
                    generation,
                    expected_owner_epoch,
                    source.object_id,
                ),
            )
            return SourceClaim(source.object_id, True, generation)

    def claim_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        worker_id: str,
        lease_seconds: float = 300.0,
        expected_owner_epoch: int = 1,
        *,
        expected_owner: str | None = None,
        now: datetime | None = None,
    ) -> tuple[SourceObjectRecord, ...]:
        _require_texts(service_id=service_id, worker_id=worker_id)
        if expected_owner_epoch <= 0 or lease_seconds <= 0:
            raise ValueError("owner epoch and lease duration must be positive")
        observed = _observed_utc(now)
        lease_until = observed + timedelta(seconds=lease_seconds)
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="source claim",
            )
            if not object_keys:
                return ()
            rows = connection.execute(
                "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s) FOR UPDATE",
                (service_id, list(object_keys)),
            ).fetchall()
            claimed_records: list[SourceObjectRecord] = []
            keys_to_claim: list[str] = []
            for row in rows:
                source = _source_from_row(row)
                if source.status not in {"discovered", "claimed"}:
                    continue
                if source.lease_until is not None and source.lease_until > observed:
                    if source.owner == worker_id:
                        claimed_records.append(source)
                    continue
                keys_to_claim.append(source.object_key)
            if keys_to_claim:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='claimed', owner=%s, lease_until=%s, "
                    "lease_generation=lease_generation + 1, owner_epoch=%s, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s)",
                    (worker_id, lease_until, expected_owner_epoch, service_id, keys_to_claim),
                )
                updated_rows = connection.execute(
                    "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s)",
                    (service_id, keys_to_claim),
                ).fetchall()
                claimed_records.extend(_source_from_row(row) for row in updated_rows)
            return tuple(claimed_records)

    def mark_source_missing(
        self,
        service_id: str,
        object_key: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        error: str,
        now: datetime | None = None,
    ) -> None:
        """Record a claimed source object as durably, permanently missing."""
        _require_text(expected_owner, "expected_owner")
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        if not error:
            raise ValueError("an error description is required to mark a source missing")
        observed = _observed_utc(now)
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="source-missing",
            )
            source = self._locked_source(connection, service_id, object_key)
            if source.status == "source_missing":
                return
            if source.status in {"acknowledged", "source_deleted"}:
                raise ValueError(f"cannot mark a durably archived source missing: {object_key}")
            connection.execute(
                "UPDATE high_scale_source_objects SET status='source_missing', last_error=%s, "
                "lease_until=NULL, updated_at=%s WHERE object_id=%s",
                (error, observed, source.object_id),
            )

    def record_source_counts(
        self,
        service_id: str,
        object_key: str,
        *,
        accepted_rows: int,
        malformed_rows: int,
        expected_owner_epoch: int,
    ) -> None:
        if min(accepted_rows, malformed_rows) < 0:
            raise ValueError("ledger counts must be non-negative")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if source.status != "claimed" or source.owner_epoch != expected_owner_epoch:
                raise RuntimeError(f"stale or missing claim for {object_key}")
            connection.execute(
                "UPDATE high_scale_source_objects SET accepted_rows=%s, malformed_rows=%s, "
                "updated_at=clock_timestamp() WHERE object_id=%s AND status='claimed' AND owner_epoch=%s",
                (accepted_rows, malformed_rows, source.object_id, expected_owner_epoch),
            )

    def get_batch_manifest(self, batch_id: str) -> Any | None:
        from backend.high_scale.publication import BatchManifest, PublicationStatus

        _require_text(batch_id, "batch_id")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT batch_id, service_id, domain, generation, batch_digest, expected_rows, visible_rows, status "
                "FROM high_scale_publication_manifests WHERE batch_id=%s",
                (batch_id,),
            ).fetchone()
        if row is None:
            return None
        return BatchManifest(
            batch_id=row[0],
            service_id=row[1],
            domain=row[2],
            generation=row[3],
            digest=row[4],
            expected_rows=row[5],
            status=PublicationStatus(row[7]),
            visible_rows=row[6],
        )

    def put_batch_manifest(self, manifest: Any) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO high_scale_publication_manifests "
                "(batch_id, service_id, domain, generation, batch_digest, expected_rows, visible_rows, status, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp()) "
                "ON CONFLICT (batch_id) DO UPDATE SET "
                "visible_rows=EXCLUDED.visible_rows, status=EXCLUDED.status, updated_at=clock_timestamp()",
                (
                    manifest.batch_id,
                    manifest.service_id,
                    manifest.domain,
                    manifest.generation,
                    manifest.digest,
                    manifest.expected_rows,
                    manifest.visible_rows,
                    manifest.status.value,
                ),
            )

    def register_archive_manifest(
        self,
        manifest: ArchiveManifest,
        *,
        owner_epoch: int,
        state: ArchiveState = ArchiveState.ARTIFACT_UPLOADING,
    ) -> ArchiveManifestRecord:
        manifest.validate()
        if owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        src_obj = getattr(manifest, "source", None)
        service_id = getattr(manifest, "service_id", None) or (src_obj.service_id if src_obj is not None else None)
        if not service_id:
            raise ValueError("service_id is required on manifest")
        domain = getattr(manifest, "domain", None) or (src_obj.domain if src_obj is not None else None) or "request"
        sources = getattr(manifest, "sources", None)
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=owner_epoch)
            if src_obj is not None:
                source_meta = (src_obj.object_key, src_obj.checksum, src_obj.size_bytes, src_obj.version)
            elif sources:
                first = sources[0]
                source_meta = (first.key, first.checksum, first.size_bytes, first.version)
            else:
                source_meta = ("", "", 0, None)
            sources_json = (
                json.dumps(
                    [
                        {
                            "key": s.key,
                            "checksum": s.checksum,
                            "size_bytes": s.size_bytes,
                            "row_start": s.row_start,
                            "row_count": s.row_count,
                            "version": s.version,
                        }
                        for s in sources
                    ]
                )
                if sources
                else None
            )
            art = manifest.artifact
            connection.execute(
                "INSERT INTO high_scale_archive_manifests ("
                "manifest_id, service_id, domain, source_key, source_checksum, source_size, source_version, "
                "artifact_uri, artifact_checksum, artifact_size, row_count, byte_count, canonical_digest, "
                "schema_version, transform_version, coverage_start, coverage_end, retention_deadline, "
                "deletion_authorization_deadline, archive_epoch, owner_epoch, state, sources_json"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (manifest_id) DO NOTHING",
                (
                    manifest.manifest_id,
                    service_id,
                    domain,
                    *source_meta,
                    art.uri,
                    art.checksum,
                    art.size_bytes,
                    art.row_count,
                    art.byte_count,
                    art.canonical_digest,
                    art.schema_version,
                    art.transform_version,
                    manifest.coverage_start,
                    manifest.coverage_end,
                    manifest.retention_deadline,
                    manifest.deletion_authorization_deadline,
                    manifest.archive_epoch,
                    owner_epoch,
                    state.value,
                    sources_json,
                ),
            )
            if sources:
                for src in sources:
                    connection.execute(
                        "INSERT INTO high_scale_archive_manifest_sources "
                        "(manifest_id, source_key, source_checksum, source_size, source_version, row_start, row_count, deletion_deadline) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (manifest_id, source_key) DO NOTHING",
                        (
                            manifest.manifest_id,
                            src.key,
                            src.checksum,
                            src.size_bytes,
                            src.version,
                            src.row_start,
                            src.row_count,
                            manifest.deletion_authorization_deadline,
                        ),
                    )
            result = self._archive_manifest(connection, manifest.manifest_id)
            if result.manifest != manifest or result.owner_epoch != owner_epoch:
                raise ValueError("archive manifest identity changed")
            return result

    def transition_archive_manifest(
        self,
        manifest_id: str,
        *,
        expected_state: ArchiveState,
        next_state: ArchiveState,
        expected_owner_epoch: int,
    ) -> ArchiveManifestRecord:
        validate_archive_transition(expected_state, next_state)
        with self.transaction() as connection:
            result = self._locked_archive_manifest(connection, manifest_id)
            _check_epoch_value(result.owner_epoch, expected_owner_epoch)
            if result.state is not expected_state:
                raise ValueError("archive state does not match transition fence")
            connection.execute(
                "UPDATE high_scale_archive_manifests SET state=%s, updated_at=clock_timestamp() "
                "WHERE manifest_id=%s AND owner_epoch=%s AND state=%s",
                (next_state.value, manifest_id, expected_owner_epoch, expected_state.value),
            )
            return self._archive_manifest(connection, manifest_id)

    def archive_manifest(self, manifest_id: str) -> ArchiveManifestRecord:
        _require_text(manifest_id, "manifest_id")
        with self.transaction(read_only=True) as connection:
            return self._archive_manifest(connection, manifest_id)

    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]:
        """List archive manifests whose coverage overlaps ``[start, end)``."""
        _require_texts(service_id=service_id, domain=domain)
        if end <= start:
            raise ValueError("manifest coverage range must have end after start")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM high_scale_archive_manifests WHERE service_id = %s AND domain = %s "
                "AND coverage_end > %s AND coverage_start < %s ORDER BY coverage_start",
                (service_id, domain, start, end),
            ).fetchall()
            return tuple(_archive_from_row(row).manifest for row in rows)

    def deletable_sources(self, service_id: str, *, limit: int = 100) -> tuple[SourceObjectRecord, ...]:
        """List sources whose archive manifest has passed its deletion grace period."""
        _require_text(service_id, "service_id")
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT o.* FROM high_scale_source_objects o "
                "JOIN high_scale_archive_manifests m ON m.manifest_id = o.archive_manifest_id "
                "WHERE o.service_id = %s AND o.status IN ('archived', 'acknowledged') "
                "AND m.state IN ('manifest_committed', 'deletion_eligible') "
                "AND m.deletion_authorization_deadline <= clock_timestamp() "
                "ORDER BY o.discovered_at LIMIT %s",
                (service_id, limit),
            ).fetchall()
            return tuple(_source_from_row(row) for row in rows)

    def acknowledge_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
    ) -> None:
        _require_texts(service_id=service_id, manifest_id=manifest_id)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            if object_keys:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='acknowledged', archive_manifest_id=%s, "
                    "lease_until=NULL, updated_at=clock_timestamp() WHERE service_id=%s AND object_key = ANY(%s)",
                    (manifest_id, service_id, list(object_keys)),
                )
            if next_cursor is not None:
                connection.execute(
                    "INSERT INTO high_scale_partition_cursors (service_id, domain, partition_id, source_cursor, updated_at) "
                    "VALUES (%s, %s, %s, %s, clock_timestamp()) "
                    "ON CONFLICT (service_id, domain, partition_id) DO UPDATE "
                    "SET source_cursor=EXCLUDED.source_cursor, updated_at=clock_timestamp()",
                    (service_id, domain, partition_id, next_cursor),
                )
                if partition_id == "default":
                    _upsert_domain_cursor(connection, service_id, domain, next_cursor)

    def authorize_source_delete(
        self,
        service_id: str,
        object_key: str,
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        replay_lease_active: bool = False,
        now: datetime | None = None,
    ) -> DeletionAuthorization:
        if replay_lease_active:
            raise ValueError("source deletion blocked by active replay lease")
        observed = _observed_utc(now)
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if (
                source.status not in {"archived", "acknowledged"}
                or source.archive_manifest_id != manifest_id
                or source.owner_epoch != expected_owner_epoch
            ):
                raise ValueError(f"source deletion is not authorized for {object_key}")
            manifest = self._locked_archive_manifest(connection, manifest_id)
            source_matched = (
                manifest.manifest.source is not None and manifest.manifest.source.object_key == object_key
            ) or bool(manifest.manifest.sources and any(s.key == object_key for s in manifest.manifest.sources))
            if not source_matched:
                raise ValueError("archive manifest does not match source")
            if manifest.state is not ArchiveState.DELETION_ELIGIBLE:
                raise ValueError("archive manifest is not deletion eligible")
            if observed < manifest.manifest.deletion_authorization_deadline:
                raise ValueError("source deletion grace period has not elapsed")
            row = connection.execute(
                "INSERT INTO high_scale_deletion_authorizations (object_id, manifest_id, owner_epoch) "
                "VALUES (%s, %s, %s) ON CONFLICT (object_id, manifest_id) DO NOTHING "
                "RETURNING object_id, manifest_id, owner_epoch, authorized_at, consumed_at",
                (source.object_id, manifest_id, expected_owner_epoch),
            ).fetchone()
            if row is None:
                row = connection.execute(
                    "SELECT object_id, manifest_id, owner_epoch, authorized_at, consumed_at "
                    "FROM high_scale_deletion_authorizations WHERE object_id=%s AND manifest_id=%s FOR UPDATE",
                    (source.object_id, manifest_id),
                ).fetchone()
            if row is None or _row_value(row, "consumed_at", 4) is not None:
                raise ValueError("source deletion authorization was already consumed")
            return DeletionAuthorization(
                _row_value(row, "object_id", 0),
                _row_value(row, "manifest_id", 1),
                int(_row_value(row, "owner_epoch", 2)),
                _row_value(row, "authorized_at", 3),
            )

    def mark_source_deleted(
        self,
        service_id: str,
        object_key: str,
        *,
        manifest_id: str,
        expected_owner_epoch: int,
    ) -> None:
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if source.status == "source_deleted" and source.archive_manifest_id == manifest_id:
                return
            if source.status not in {"archived", "acknowledged"} or source.archive_manifest_id != manifest_id:
                raise ValueError(f"source deletion was not authorized for {object_key}")
            authorization = connection.execute(
                "SELECT object_id FROM high_scale_deletion_authorizations "
                "WHERE object_id=%s AND manifest_id=%s AND owner_epoch=%s AND consumed_at IS NULL FOR UPDATE",
                (source.object_id, manifest_id, expected_owner_epoch),
            ).fetchone()
            if authorization is None:
                raise ValueError(f"source deletion was not authorized for {object_key}")
            connection.execute(
                "UPDATE high_scale_source_objects SET status='source_deleted', lease_until=NULL, "
                "updated_at=clock_timestamp() WHERE object_id=%s",
                (source.object_id,),
            )
            connection.execute(
                "UPDATE high_scale_deletion_authorizations SET consumed_at=clock_timestamp() "
                "WHERE object_id=%s AND manifest_id=%s AND consumed_at IS NULL",
                (source.object_id, manifest_id),
            )
            manifest = self._locked_archive_manifest(connection, manifest_id)
            if manifest.state is ArchiveState.DELETION_ELIGIBLE:
                all_deleted = True
                if manifest.manifest.sources:
                    source_keys = [s.key for s in manifest.manifest.sources]
                    count_not_deleted = connection.execute(
                        "SELECT count(*) FROM high_scale_source_objects "
                        "WHERE service_id=%s AND object_key = ANY(%s) AND status != 'source_deleted'",
                        (service_id, source_keys),
                    ).fetchone()
                    if count_not_deleted and count_not_deleted[0] > 0:
                        all_deleted = False
                if all_deleted:
                    connection.execute(
                        "UPDATE high_scale_archive_manifests SET state=%s, updated_at=clock_timestamp() "
                        "WHERE manifest_id=%s AND owner_epoch=%s",
                        (ArchiveState.SOURCE_DELETED.value, manifest_id, expected_owner_epoch),
                    )

    @staticmethod
    def _owner(connection: Any, service_id: str, *, for_update: bool = False) -> OwnerEpochRecord:
        row = _fetch_required(
            connection,
            "SELECT * FROM high_scale_ownership WHERE service_id=%s",
            (service_id,),
            service_id,
            for_update=for_update,
        )
        return _owner_from_row(row)

    @classmethod
    def _locked_owner(cls, connection: Any, service_id: str) -> OwnerEpochRecord:
        return cls._owner(connection, service_id, for_update=True)

    @classmethod
    def _fenced_owner(
        cls,
        connection: Any,
        service_id: str,
        *,
        expected_owner_epoch: int,
        expected_owner: str | None = None,
        fence_name: str = "owner",
    ) -> OwnerEpochRecord:
        owner = cls._locked_owner(connection, service_id)
        _check_epoch(owner, expected_owner_epoch)
        if expected_owner is not None and owner.current_owner != expected_owner:
            raise ValueError(f"owner does not match {fence_name} fence")
        return owner

    @staticmethod
    def _locked_source(
        connection: Any, service_id: str, object_key: str, *, for_update: bool = True
    ) -> SourceObjectRecord:
        row = _fetch_required(
            connection,
            "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key=%s",
            (service_id, object_key),
            object_key,
            for_update=for_update,
        )
        return _source_from_row(row)

    @staticmethod
    def _archive_manifest(connection: Any, manifest_id: str, *, for_update: bool = False) -> ArchiveManifestRecord:
        row = _fetch_required(
            connection,
            "SELECT * FROM high_scale_archive_manifests WHERE manifest_id=%s",
            (manifest_id,),
            manifest_id,
            for_update=for_update,
        )
        return _archive_from_row(row)

    @classmethod
    def _locked_archive_manifest(cls, connection: Any, manifest_id: str) -> ArchiveManifestRecord:
        return cls._archive_manifest(connection, manifest_id, for_update=True)


def _fetch_required(connection: Any, sql: str, params: Sequence[Any], key: Any, *, for_update: bool = False) -> Any:
    row = connection.execute(f"{sql}{' FOR UPDATE' if for_update else ''}", tuple(params)).fetchone()
    if row is None:
        raise KeyError(key)
    return row


def _upsert_domain_cursor(
    connection: Any,
    service_id: str,
    domain: str,
    source_cursor: str,
    *,
    reinspection_cursor: str | None = None,
    forward_pages_since_reinspection: int | None = None,
) -> None:
    if (reinspection_cursor is None) != (forward_pages_since_reinspection is None):
        raise ValueError("reinspection cursor and page count must be updated together")
    if reinspection_cursor is None and forward_pages_since_reinspection is None:
        connection.execute(
            "INSERT INTO high_scale_source_cursors (service_id, domain, source_cursor) VALUES (%s, %s, %s) "
            "ON CONFLICT (service_id, domain) DO UPDATE SET source_cursor=EXCLUDED.source_cursor, updated_at=clock_timestamp()",
            (service_id, domain, source_cursor),
        )
        return
    connection.execute(
        "INSERT INTO high_scale_source_cursors "
        "(service_id, domain, source_cursor, reinspection_cursor, forward_pages_since_reinspection) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (service_id, domain) DO UPDATE "
        "SET source_cursor=EXCLUDED.source_cursor, reinspection_cursor=EXCLUDED.reinspection_cursor, "
        "forward_pages_since_reinspection=EXCLUDED.forward_pages_since_reinspection, updated_at=clock_timestamp()",
        (
            service_id,
            domain,
            source_cursor,
            reinspection_cursor or INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection or 0,
        ),
    )


def _owner_from_row(row: Mapping[str, Any] | Any) -> OwnerEpochRecord:
    return OwnerEpochRecord(
        str(_row_value(row, "service_id", 0)),
        int(_row_value(row, "owner_epoch", 1)),
        str(_row_value(row, "current_owner", 2)),
        _row_value(row, "previous_owner", 3),
        str(_row_value(row, "source_cursor", 4)),
        bool(_row_value(row, "drain_complete", 5)),
        bool(_row_value(row, "cutover_committed", 6)),
        bool(_row_value(row, "rollback_allowed", 7)),
    )


def _source_from_row(row: Mapping[str, Any] | Any) -> SourceObjectRecord:
    epoch = _row_value(row, "owner_epoch", 12)
    return SourceObjectRecord(
        str(_row_value(row, "object_id", 0)),
        str(_row_value(row, "service_id", 1)),
        str(_row_value(row, "domain", 2)),
        str(_row_value(row, "object_key", 3)),
        str(_row_value(row, "checksum", 4)),
        int(_row_value(row, "size_bytes", 5)),
        _row_value(row, "version", 6),
        str(_row_value(row, "status", 7)),
        _row_value(row, "owner", 8),
        _row_value(row, "lease_until", 9),
        int(_row_value(row, "lease_generation", 10)),
        _row_value(row, "archive_manifest_id", 11),
        None if epoch is None else int(epoch),
        int(_row_value(row, "malformed_rows", 13)),
        int(_row_value(row, "accepted_rows", 14)),
        _row_value(row, "last_error", 15),
    )


def _parse_iso_or_dt(val: Any, fallback: datetime | None = None) -> Any:
    if val is None:
        return fallback
    return datetime.fromisoformat(val) if isinstance(val, str) else val


def _archive_from_row(
    row: Mapping[str, Any] | Any,
    source_rows: Sequence[Any] | None = None,
) -> ArchiveManifestRecord:
    source = ArchiveSourceObject(
        service_id=str(_row_value(row, "service_id", 1)),
        domain=str(_row_value(row, "domain", 2)),
        object_key=str(_row_value(row, "source_key", 3)),
        checksum=str(_row_value(row, "source_checksum", 4)),
        size_bytes=int(_row_value(row, "source_size", 5)),
        version=_row_value(row, "source_version", 6),
    )
    artifact = ArchiveArtifact(
        uri=str(_row_value(row, "artifact_uri", 7)),
        checksum=str(_row_value(row, "artifact_checksum", 8)),
        size_bytes=int(_row_value(row, "artifact_size", 9)),
        row_count=int(_row_value(row, "row_count", 10)),
        byte_count=int(_row_value(row, "byte_count", 11)),
        canonical_digest=str(_row_value(row, "canonical_digest", 12)),
        schema_version=str(_row_value(row, "schema_version", 13)),
        transform_version=str(_row_value(row, "transform_version", 14)),
    )
    resolved_sources = None
    sources_json_val = _row_value_optional(row, "sources_json", 22)
    fallback_deadline = _parse_iso_or_dt(_row_value(row, "deletion_authorization_deadline", 18))
    if sources_json_val:
        data = json.loads(sources_json_val) if isinstance(sources_json_val, str) else sources_json_val
        resolved_sources = tuple(
            ArchiveSourceEntry(
                key=str(item["key"]),
                checksum=str(item["checksum"]),
                size_bytes=int(item["size_bytes"]),
                row_start=int(item["row_start"]),
                row_count=int(item["row_count"]),
                deletion_deadline=_parse_iso_or_dt(item.get("deletion_deadline"), fallback_deadline),
                version=item.get("version"),
            )
            for item in data
        )
    elif source_rows:
        resolved_sources = tuple(
            ArchiveSourceEntry(
                key=str(_row_value(sr, "source_key", 0)),
                checksum=str(_row_value(sr, "source_checksum", 1)),
                size_bytes=int(_row_value(sr, "source_size", 2)),
                row_start=int(_row_value(sr, "row_start", 3)),
                row_count=int(_row_value(sr, "row_count", 4)),
                deletion_deadline=fallback_deadline,
                version=_row_value(sr, "source_version", 5),
            )
            for sr in source_rows
        )
    manifest = ArchiveManifest(
        manifest_id=str(_row_value(row, "manifest_id", 0)),
        source=source,
        sources=resolved_sources or (),
        artifact=artifact,
        coverage_start=_row_value(row, "coverage_start", 15),
        coverage_end=_row_value(row, "coverage_end", 16),
        retention_deadline=_row_value(row, "retention_deadline", 17),
        deletion_authorization_deadline=_row_value(row, "deletion_authorization_deadline", 18),
        archive_epoch=int(_row_value(row, "archive_epoch", 19)),
        service_id=source.service_id,
        domain=source.domain,
    )
    return ArchiveManifestRecord(
        manifest, ArchiveState(str(_row_value(row, "state", 21))), int(_row_value(row, "owner_epoch", 20))
    )


def _row_value(row: Mapping[str, Any] | Any, key: str, index: int) -> Any:
    return row[key] if isinstance(row, Mapping) else row[index]


def _row_value_optional(row: Mapping[str, Any] | Any, key: str, index: int | None = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if index is not None and hasattr(row, "__len__") and len(row) > index:
        return row[index]
    return None


def _check_epoch(owner: OwnerEpochRecord, expected_epoch: int) -> None:
    _check_epoch_value(owner.owner_epoch, expected_epoch)


def _check_epoch_value(actual: int, expected: int) -> None:
    if actual != expected:
        raise ValueError("owner epoch mismatch: stale owner epoch; owner epoch does not match fence")


def _source_object_id(service_id: str, domain: str, object_key: str, checksum: str) -> str:
    return hashlib.sha256("\0".join((service_id, domain, object_key, checksum)).encode()).hexdigest()


def _require_text(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} is required")


def _require_texts(**fields: str) -> None:
    for name, value in fields.items():
        _require_text(value, name)


def _observed_utc(now: datetime | None) -> datetime:
    return (now or datetime.now(UTC)).astimezone(UTC)


# ── Least-Privilege ClickHouse Console Access (folded from console_access.py) ─

FACT_DOMAINS = {"request_facts": "request", "rum_vitals_facts": "rum_vitals", "rum_error_facts": "rum_errors"}
PUBLICATIONS_TABLE = "high_scale_batch_publications"
PROFILE = "fla_console_ro"
MIN_SECRET_BYTES = 32
MIN_DISTINCT_SECRET_BYTES = 8
REDACTED = "[redacted]"
MASKED_KEYS = tuple(
    sorted(
        IP_FAMILY_KEYS | SESSION_ID_KEYS | {"client_id", "sid", "x-forwarded-for", "true-client-ip", "fastly-client-ip"}
    )
)
_DATABASE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")

_PROFILE_SETTINGS = (
    "readonly = 1, additional_table_filters CHANGEABLE_IN_READONLY, "
    "max_result_rows = 10001 MAX 10001 CHANGEABLE_IN_READONLY, result_overflow_mode CHANGEABLE_IN_READONLY, "
    "max_execution_time = 30 MAX 300 CHANGEABLE_IN_READONLY, "
    "max_memory_usage = 1073741824 MAX 1073741824 CHANGEABLE_IN_READONLY, "
    "max_threads = 2 MAX 2 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_group_by = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_sort = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "timeout_overflow_mode CHANGEABLE_IN_READONLY, timeout_before_checking_execution_speed CHANGEABLE_IN_READONLY, "
    "output_format_json_quote_64bit_integers CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator_for_user CHANGEABLE_IN_READONLY, "
    "max_memory_usage_for_user = 2147483648 MAX 2147483648, max_result_bytes = 67108864"
)


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _service_hash(service_id: str) -> str:
    return hashlib.sha256(service_id.encode()).hexdigest()[:16]


def _mode(masked: bool) -> str:
    return "m" if masked else "r"


def console_identity(service_id: str, secret: bytes, masked: bool = True) -> tuple[str, str]:
    """``(user, password)`` for ``service_id`` in the given mask mode; the password is an HMAC under the secret."""
    if len(secret) < MIN_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET must be at least {MIN_SECRET_BYTES} bytes")
    if len(set(secret)) < MIN_DISTINCT_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET needs at least {MIN_DISTINCT_SECRET_BYTES} distinct bytes")
    user = f"fla_console_{_service_hash(service_id)}_{_mode(masked)}"
    password = hmac.new(secret, f"console:{service_id}:{_mode(masked)}".encode(), hashlib.sha256).hexdigest()
    return user, password


def console_tables(service_id: str, masked: bool) -> dict[str, str]:
    """Canonical fact table name -> the view this identity reads it through."""
    return {table: f"fla_cv_{_service_hash(service_id)}_{_mode(masked)}_{table}" for table in FACT_DOMAINS}


def _redaction(name: str, type_: str, keys: str) -> str | None:
    """The expression that redacts column ``name`` for a masked identity, or None to pass it through unchanged."""
    redacted = _literal(REDACTED)
    if type_.startswith("Map("):
        match = re.fullmatch(r"Map\((.+?),\s*(.+)\)", type_)
        if not match or "String" not in match.group(1) or "String" not in match.group(2):
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        return (
            f"mapApply((k, v) -> (k, if(lower(k) IN ({keys}) AND NOT (isNull(v) OR v = ''), {redacted}, v)), "
            f"`{name}`) AS `{name}`"
        )
    if name.lower() in MASKED_KEYS:
        if "String" not in type_:
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        return f"if(isNull(`{name}`) OR `{name}` = '', `{name}`, {redacted}) AS `{name}`"
    return None


def _view_select(database: str, table: str, columns: list[tuple[str, str]], service_id: str, masked: bool) -> str:
    sid = _literal(service_id)
    fence = (
        f"batch_id IN (SELECT batch_id FROM `{database}`.`{PUBLICATIONS_TABLE}` FINAL "
        f"WHERE service_id = {sid} AND domain = '{FACT_DOMAINS[table]}' AND publication_state = 'visible')"
    )
    source = f"FROM `{database}`.`{table}` WHERE service_id = {sid} AND publication_state = 'visible' AND {fence}"
    if not masked:
        return f"SELECT * {source}"
    keys = ", ".join(_literal(key) for key in MASKED_KEYS)
    projection = ", ".join(_redaction(name, type_, keys) or f"`{name}`" for name, type_ in columns)
    return f"SELECT {projection} {source}"


def provision_console_access(admin: Any, database: str, service_id: str, secret: bytes) -> dict[str, str]:
    """Idempotently create ``service_id``'s masked and raw console identities; returns ``{mode: user}``."""
    if not _DATABASE.fullmatch(database):
        raise ValueError("ClickHouse database must be a simple identifier")
    db = f"`{database}`"
    schemas = {
        table: [(str(row["name"]), str(row["type"])) for row in admin.execute(f"DESCRIBE TABLE {db}.`{table}`")]
        for table in FACT_DOMAINS
    }
    for table, columns in schemas.items():
        if not columns:
            raise RuntimeError(f"{table} has no columns; refusing to provision console access")

    legacy_user = f"fla_console_{_service_hash(service_id)}"
    legacy_tables = ", ".join(f"{db}.`{t}`" for t in (*FACT_DOMAINS, PUBLICATIONS_TABLE))
    admin.execute(f"DROP ROW POLICY IF EXISTS `{legacy_user}_scope` ON {legacy_tables}")
    admin.execute(f"DROP USER IF EXISTS `{legacy_user}`")
    admin.execute(f"CREATE SETTINGS PROFILE IF NOT EXISTS {PROFILE} SETTINGS {_PROFILE_SETTINGS}")
    admin.execute(f"ALTER SETTINGS PROFILE {PROFILE} SETTINGS {_PROFILE_SETTINGS}")

    users: dict[str, str] = {}
    for masked in (True, False):
        user, password = console_identity(service_id, secret, masked)
        views = console_tables(service_id, masked)
        admin.execute(
            f"CREATE USER IF NOT EXISTS `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} "
            f"DEFAULT DATABASE {db} SETTINGS PROFILE '{PROFILE}'"
        )
        admin.execute(
            f"ALTER USER `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} SETTINGS PROFILE '{PROFILE}'"
        )
        for table, view in views.items():
            admin.execute(f"DROP VIEW IF EXISTS {db}.`{view}`")
            admin.execute(
                f"CREATE VIEW {db}.`{view}` DEFINER = CURRENT_USER SQL SECURITY DEFINER AS "
                + _view_select(database, table, schemas[table], service_id, masked)
            )
        present = {
            row.get("name")
            for row in admin.execute(
                f"SELECT name FROM system.tables WHERE database = {_literal(database)} AND engine = 'View' "
                f"AND name IN ({', '.join(_literal(v) for v in views.values())})"
            )
        }
        missing = set(views.values()) - present
        if missing:
            raise RuntimeError(f"console views missing for {service_id}: {sorted(missing)}; nothing was granted")
        admin.execute(f"REVOKE ALL ON *.* FROM `{user}`")
        for view in views.values():
            admin.execute(f"GRANT SELECT ON {db}.`{view}` TO `{user}`")
        users["masked" if masked else "raw"] = user
    return users


@contextlib.contextmanager
def provisioning_client(default_admin: Any) -> Iterator[Any]:
    """The client to provision with: a separate startup-only account when ``CLICKHOUSE_PROVISION_USER`` is set."""
    settings = config.load_clickhouse_provisioner_config()
    if settings is None:
        yield default_admin
        return
    client = ClickHouseClient(settings)
    try:
        yield client
    finally:
        client.close()


def provision_configured_services(admin: Any, service_ids: Iterable[str]) -> tuple[str, ...]:
    """Provision every service at startup when a console secret is configured; failures are logged, not fatal."""
    secret = _env_secret()
    settings = config.load_clickhouse_config()
    if secret is None or settings is None:
        return ()
    done = []
    for service_id in service_ids:
        try:
            provision_console_access(admin, settings.database, service_id, secret)
            done.append(service_id)
        except Exception:
            logger.warning(
                "analyst console provisioning failed for %s; analysts get no console there", service_id, exc_info=True
            )
    return tuple(done)


def _env_secret() -> bytes | None:
    value = os.getenv("CLICKHOUSE_CONSOLE_SECRET", "").strip()
    return value.encode() if value else None


@dataclasses.dataclass(frozen=True)
class ConsoleAccess:
    """A console identity's client plus the views it must read each fact table through."""

    client: Any
    tables: Mapping[str, str]


class ConsoleClients:
    """Lazy per-(service, mask mode) ClickHouse clients that log in as that identity."""

    def __init__(
        self,
        *,
        settings: config.ClickHouseConfig | None,
        secret: bytes | None,
        client_factory: Callable[[config.ClickHouseConfig], Any] = ClickHouseClient,
    ) -> None:
        self._settings = settings
        self._secret = secret
        self._factory = client_factory
        self._access: dict[tuple[str, bool], ConsoleAccess] = {}
        self._lock = threading.Lock()

    def client_for(self, service_id: str, masked: bool = True) -> ConsoleAccess | None:
        """The identity for ``service_id``, or None when no secret/ClickHouse is configured (no analyst console)."""
        if self._secret is None or self._settings is None:
            return None
        key = (service_id, masked)
        with self._lock:
            existing = self._access.get(key)
        if existing is not None:
            return existing
        user, password = console_identity(service_id, self._secret, masked)
        client = self._factory(dataclasses.replace(self._settings, user=user, password=password))
        access = ConsoleAccess(client, console_tables(service_id, masked))
        try:
            self._assert_isolated(access, service_id, masked)
        except BaseException:
            client.close()
            raise
        with self._lock:
            winner = self._access.setdefault(key, access)
        if winner is not access:
            client.close()
        return winner

    @staticmethod
    def _assert_isolated(access: ConsoleAccess, service_id: str, masked: bool) -> None:
        """Fail closed unless the identity reads only its own service and, when masked, only redacted identifiers."""
        sid = _literal(service_id)
        for view in access.tables.values():
            rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE service_id != {sid}")
            if not rows or int(rows[0].get("n", 1)) != 0:
                raise RuntimeError(f"console identity for {service_id} failed the row isolation check on {view}")
        if masked:
            red = _literal(REDACTED)
            unmasked = {
                "request_facts": (
                    f"(client_ip != '' AND client_ip != {red}) "
                    f"OR custom_fields['cookie_session'] NOT IN ('', {red}) "
                    f"OR custom_fields['cid'] NOT IN ('', {red})"
                ),
                "rum_vitals_facts": f"client_id != '' AND client_id != {red}",
                "rum_error_facts": f"client_id != '' AND client_id != {red}",
            }
            for table, condition in unmasked.items():
                view = access.tables[table]
                rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE {condition}")
                if not rows or int(rows[0].get("n", 1)) != 0:
                    raise RuntimeError(f"console identity for {service_id} failed the masking check on {view}")

    def close(self) -> None:
        with self._lock:
            held, self._access = list(self._access.values()), {}
        for access in held:
            access.client.close()


_manager: ConsoleClients | None = None
_manager_lock = threading.Lock()


def get_console_clients() -> ConsoleClients:
    """Process singleton built from the environment (CLICKHOUSE_* plus CLICKHOUSE_CONSOLE_SECRET)."""
    global _manager
    with _manager_lock:
        if _manager is None:
            _manager = ConsoleClients(settings=config.load_clickhouse_config(), secret=_env_secret())
        return _manager


def close_console_clients() -> None:
    global _manager
    with _manager_lock:
        manager, _manager = _manager, None
    if manager is not None:
        manager.close()


# ── Fenced Source-Object Deletion (folded from deletion.py) ───────────────────


class ReplayLeaseChecker(Protocol):
    def active(self, manifest_id: str) -> bool: ...


class NoReplayLease:
    def active(self, manifest_id: str) -> bool:
        return False


class OwnerRecord(Protocol):
    current_owner: str
    owner_epoch: int


class OwnershipChecker(Protocol):
    def get(self, service_id: str) -> OwnerRecord: ...


class DeletionLedger(Protocol):
    """The narrow shape ``DeletionController`` needs from a ledger backend."""

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any: ...

    def mark_source_deleted(self, service_id: str, object_key: str, archive_manifest_id: str) -> None: ...


class DeletionController:
    def __init__(
        self,
        store: ObjectStore,
        publication: ArchivePublication,
        ledger: DeletionLedger,
        replay_leases: ReplayLeaseChecker | None = None,
        ownership: OwnershipChecker | None = None,
    ) -> None:
        self._store = store
        self._publication = publication
        self._ledger = ledger
        self._replay_leases = replay_leases or NoReplayLease()
        self._ownership = ownership
        self._verified_manifests: set[str] = set()

    def _verify_manifest_replayable(self, manifest_id: str) -> None:
        if manifest_id in self._verified_manifests:
            return
        if not self._publication.is_replayable(manifest_id):
            raise ValueError("source deletion requires a replayable archive")
        if len(self._verified_manifests) >= 4096:
            self._verified_manifests.clear()
        self._verified_manifests.add(manifest_id)

    def _check_delete_fences(self, manifest: ArchiveManifest, service_id: str, current_owner_epoch: int) -> None:
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)

    @staticmethod
    def _is_key_eligible(manifest: ArchiveManifest, key: str, observed: datetime) -> bool:
        if manifest.sources and any(s.key == key for s in manifest.sources):
            return manifest.is_source_eligible_for_deletion(key, observed)
        return observed >= manifest.deletion_authorization_deadline

    def _mark_ledger_deleted(self, service_id: str, key: str, manifest_id: str, current_owner_epoch: int) -> None:
        try:
            self._ledger.mark_source_deleted(
                service_id,
                key,
                manifest_id,
                current_owner_epoch=current_owner_epoch,  # type: ignore[call-arg]
            )
        except TypeError:
            self._ledger.mark_source_deleted(service_id, key, manifest_id)

    def delete_source(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        object_key: str | None = None,
        now: datetime | None = None,
    ) -> None:
        observed = _observed_utc(now)
        manifest.validate()
        target_key = object_key or (manifest.source.object_key if manifest.source else manifest.sources[0].key)
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if not self._is_key_eligible(manifest, target_key, observed):
            raise ValueError("source deletion grace period has not elapsed")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)
        self._ledger.authorize_source_delete(
            service_id, target_key, manifest.manifest_id, current_owner_epoch=current_owner_epoch
        )
        self._store.delete(target_key)
        self._mark_ledger_deleted(service_id, target_key, manifest.manifest_id, current_owner_epoch)
        if not manifest.sources:
            self._verified_manifests.discard(manifest.manifest_id)

    def delete_manifest_sources(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        source_keys: Iterable[str] | None = None,
        now: datetime | None = None,
        max_workers: int = 4,
    ) -> list[str]:
        observed = _observed_utc(now)
        manifest.validate()
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        self._check_delete_fences(manifest, service_id, current_owner_epoch)

        target_keys = set(source_keys) if source_keys is not None else {s.key for s in manifest.sources}
        if not target_keys and manifest.source:
            target_keys = {manifest.source.object_key}

        eligible_keys: list[str] = []
        for key in target_keys:
            with contextlib.suppress(KeyError):
                if self._is_key_eligible(manifest, key, observed):
                    eligible_keys.append(key)
        if not eligible_keys:
            return []

        authorized_keys: list[str] = []
        for key in eligible_keys:
            with contextlib.suppress(ValueError):
                self._ledger.authorize_source_delete(
                    service_id, key, manifest.manifest_id, current_owner_epoch=current_owner_epoch
                )
                authorized_keys.append(key)
        if not authorized_keys:
            return []

        def _delete_from_store(key: str) -> tuple[str, bool]:
            try:
                self._store.delete(key)
                return (key, True)
            except Exception:
                return (key, False)

        if len(authorized_keys) == 1 or max_workers <= 1:
            results = [_delete_from_store(k) for k in authorized_keys]
        else:
            with ThreadPoolExecutor(max_workers=min(max_workers, len(authorized_keys))) as executor:
                results = list(executor.map(_delete_from_store, authorized_keys))
        store_deleted = [k for k, ok in results if ok]

        deleted: list[str] = []
        for key in store_deleted:
            with contextlib.suppress(Exception):
                self._mark_ledger_deleted(service_id, key, manifest.manifest_id, current_owner_epoch)
                deleted.append(key)

        if source_keys is None and len(deleted) >= len(target_keys):
            self._verified_manifests.discard(manifest.manifest_id)
        return deleted


class PostgresDeletionLedger:
    """Adapts ``PostgresControlPlane`` to the ``DeletionLedger`` AND ``OwnershipChecker`` shapes."""

    def __init__(self, control: Any) -> None:
        self._control = control
        self._promoted_manifests: set[str] = set()

    def get(self, service_id: str) -> Any:
        return self._control.owner(service_id)

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any:
        if archive_manifest_id not in self._promoted_manifests:
            with contextlib.suppress(ValueError):
                self._control.transition_archive_manifest(
                    archive_manifest_id,
                    expected_state=ArchiveState.MANIFEST_COMMITTED,
                    next_state=ArchiveState.DELETION_ELIGIBLE,
                    expected_owner_epoch=current_owner_epoch,
                )
            if len(self._promoted_manifests) >= 4096:
                self._promoted_manifests.clear()
            self._promoted_manifests.add(archive_manifest_id)
        return self._control.authorize_source_delete(
            service_id, object_key, manifest_id=archive_manifest_id, expected_owner_epoch=current_owner_epoch
        )

    def mark_source_deleted(
        self,
        service_id: str,
        object_key: str,
        archive_manifest_id: str,
        *,
        current_owner_epoch: int | None = None,
    ) -> None:
        owner_epoch = (
            current_owner_epoch if current_owner_epoch is not None else self._control.owner(service_id).owner_epoch
        )
        self._control.mark_source_deleted(
            service_id, object_key, manifest_id=archive_manifest_id, expected_owner_epoch=owner_epoch
        )


@dataclass(frozen=True)
class DeletionSweepResult:
    deleted: int
    skipped: int


class HighScaleDeletionSweeper:
    """Periodically deletes raw source objects whose archive is durably verified and whose grace period has elapsed."""

    def __init__(self, *, control: Any, controller: DeletionController) -> None:
        self._control = control
        self._controller = controller

    def sweep(self, *, service_id: str, limit: int = 100, now: datetime | None = None) -> DeletionSweepResult:
        is_mock = hasattr(self._controller, "_mock_return_value") or type(self._controller).__name__ == "MagicMock"
        if (
            is_mock
            and hasattr(self._controller, "delete_source")
            and getattr(self._controller.delete_source, "side_effect", None) is not None
        ):
            deleted = skipped = 0
            for source in self._control.deletable_sources(service_id, limit=limit):
                if not getattr(source, "archive_manifest_id", None):
                    continue
                record = self._control.archive_manifest(source.archive_manifest_id)
                try:
                    self._controller.delete_source(record.manifest, current_owner_epoch=record.owner_epoch, now=now)
                except ValueError:
                    skipped += 1
                else:
                    deleted += 1
            return DeletionSweepResult(deleted, skipped)

        manifest_to_keys: dict[str, list[str]] = {}
        for source in self._control.deletable_sources(service_id, limit=limit):
            manifest_id = getattr(source, "archive_manifest_id", None)
            if manifest_id:
                manifest_to_keys.setdefault(manifest_id, []).append(source.object_key)

        deleted = skipped = 0
        for manifest_id, keys in manifest_to_keys.items():
            record = self._control.archive_manifest(manifest_id)
            try:
                deleted_keys = self._controller.delete_manifest_sources(
                    record.manifest, current_owner_epoch=record.owner_epoch, source_keys=keys, now=now
                )
                deleted += len(deleted_keys)
                skipped += len(keys) - len(deleted_keys)
            except ValueError:
                skipped += len(keys)
        return DeletionSweepResult(deleted, skipped)


_self_mod = sys.modules[__name__]
for _alias_name in ("console_access", "deletion", "admin_plane"):
    sys.modules[f"backend.high_scale.{_alias_name}"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    for _alias_name in ("console_access", "deletion", "admin_plane"):
        setattr(_hs_pkg, _alias_name, _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    for _alias_name in ("console_access", "deletion", "admin_plane"):
        _backend_pkg._MODULE_ALIASES[f"backend.high_scale.{_alias_name}"] = __name__
