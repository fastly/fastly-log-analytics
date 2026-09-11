"""Transactional Postgres control-plane state for the isolated high-scale slice.

This module is intentionally not wired into either active deployment mode.  It
owns only the durable coordination records that a future high-scale plane
needs: ownership fencing, source-object leases, archive publication state, and
source-deletion authorization.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg_pool import ConnectionPool

from backend.core.high_scale_contracts import ArchiveState, validate_archive_transition
from backend.core.metadata.pg_connection import get_pg_pool
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS high_scale_ownership (
    service_id TEXT PRIMARY KEY,
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    current_owner TEXT NOT NULL,
    previous_owner TEXT,
    source_cursor TEXT NOT NULL,
    drain_complete BOOLEAN NOT NULL DEFAULT FALSE,
    cutover_committed BOOLEAN NOT NULL DEFAULT FALSE,
    rollback_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS high_scale_source_objects (
    object_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    object_key TEXT NOT NULL,
    checksum TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    version TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('discovered', 'claimed', 'appended', 'archived', 'acknowledged', 'source_deleted')
    ),
    owner TEXT,
    lease_until TIMESTAMPTZ,
    lease_generation BIGINT NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    archive_manifest_id TEXT,
    owner_epoch BIGINT,
    malformed_rows BIGINT NOT NULL DEFAULT 0 CHECK (malformed_rows >= 0),
    accepted_rows BIGINT NOT NULL DEFAULT 0 CHECK (accepted_rows >= 0),
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (service_id, object_key)
);

CREATE INDEX IF NOT EXISTS high_scale_source_objects_claim_idx
    ON high_scale_source_objects (service_id, status, lease_until);

CREATE TABLE IF NOT EXISTS high_scale_archive_manifests (
    manifest_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    source_key TEXT NOT NULL,
    source_checksum TEXT NOT NULL,
    source_size BIGINT NOT NULL CHECK (source_size >= 0),
    source_version TEXT,
    artifact_uri TEXT NOT NULL,
    artifact_checksum TEXT NOT NULL,
    artifact_size BIGINT NOT NULL CHECK (artifact_size >= 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
    canonical_digest TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    transform_version TEXT NOT NULL,
    coverage_start TIMESTAMPTZ NOT NULL,
    coverage_end TIMESTAMPTZ NOT NULL,
    retention_deadline TIMESTAMPTZ NOT NULL,
    deletion_authorization_deadline TIMESTAMPTZ NOT NULL,
    archive_epoch BIGINT NOT NULL CHECK (archive_epoch >= 0),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    state TEXT NOT NULL CHECK (
        state IN (
            'artifact_uploading',
            'artifact_verified',
            'manifest_prepared',
            'manifest_committed',
            'deletion_eligible',
            'source_deleted'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (coverage_end >= coverage_start),
    CHECK (retention_deadline >= coverage_end),
    CHECK (deletion_authorization_deadline >= retention_deadline)
);

CREATE INDEX IF NOT EXISTS high_scale_archive_manifest_source_idx
    ON high_scale_archive_manifests (service_id, source_key);

CREATE TABLE IF NOT EXISTS high_scale_deletion_authorizations (
    object_id TEXT NOT NULL REFERENCES high_scale_source_objects(object_id),
    manifest_id TEXT NOT NULL REFERENCES high_scale_archive_manifests(manifest_id),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    authorized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    consumed_at TIMESTAMPTZ,
    PRIMARY KEY (object_id, manifest_id)
);
"""

_SOURCE_STATES = frozenset({"discovered", "claimed", "appended", "archived", "acknowledged", "source_deleted"})


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

    def __init__(
        self,
        *,
        pool: ConnectionPool | Any | None = None,
        pool_timeout: float | None = None,
    ) -> None:
        self.pool = get_pg_pool() if pool is None else pool
        self.pool_timeout = pool_timeout

    @contextmanager
    def transaction(self, *, read_only: bool = False) -> Iterator[Any]:
        connection_context = (
            self.pool.connection(timeout=self.pool_timeout) if self.pool_timeout is not None else self.pool.connection()
        )
        with connection_context as connection, connection.transaction():
            if read_only:
                connection.execute("SET TRANSACTION READ ONLY")
            yield connection

    def create_schema(self) -> None:
        with self.transaction() as connection:
            connection.execute(SCHEMA_DDL)

    def initialize_owner(self, service_id: str, *, owner: str, source_cursor: str) -> OwnerEpochRecord:
        _require_text(service_id, "service_id")
        _require_text(owner, "owner")
        _require_text(source_cursor, "source_cursor")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO high_scale_ownership (service_id, owner_epoch, current_owner, source_cursor)
                VALUES (%s, 1, %s, %s)
                ON CONFLICT (service_id) DO NOTHING
                """,
                (service_id, owner, source_cursor),
            )
            return self._owner(connection, service_id)

    def owner(self, service_id: str) -> OwnerEpochRecord:
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            return self._owner(connection, service_id)

    def begin_drain(self, service_id: str, *, expected_owner: str) -> OwnerEpochRecord:
        _require_text(expected_owner, "expected_owner")
        with self.transaction() as connection:
            current = self._locked_owner(connection, service_id)
            if current.current_owner != expected_owner or current.cutover_committed:
                raise ValueError("owner does not match drain fence")
            connection.execute(
                """
                UPDATE high_scale_ownership
                SET drain_complete=FALSE, rollback_allowed=FALSE, updated_at=clock_timestamp()
                WHERE service_id=%s
                """,
                (service_id,),
            )
            return self._owner(connection, service_id)

    def mark_drained(self, service_id: str, *, expected_epoch: int, source_cursor: str) -> OwnerEpochRecord:
        _require_text(source_cursor, "source_cursor")
        with self.transaction() as connection:
            current = self._locked_owner(connection, service_id)
            _check_epoch(current, expected_epoch)
            if current.cutover_committed:
                raise ValueError("owner epoch does not match drain fence")
            connection.execute(
                """
                UPDATE high_scale_ownership
                SET drain_complete=TRUE, source_cursor=%s, updated_at=clock_timestamp()
                WHERE service_id=%s
                """,
                (source_cursor, service_id),
            )
            return self._owner(connection, service_id)

    def commit_cutover(self, service_id: str, *, expected_epoch: int, next_owner: str) -> OwnerEpochRecord:
        _require_text(next_owner, "next_owner")
        with self.transaction() as connection:
            current = self._locked_owner(connection, service_id)
            _check_epoch(current, expected_epoch)
            if not current.drain_complete or current.cutover_committed:
                raise ValueError("service is not ready for cutover")
            connection.execute(
                """
                UPDATE high_scale_ownership
                SET owner_epoch=owner_epoch+1, previous_owner=current_owner,
                    current_owner=%s, cutover_committed=TRUE, rollback_allowed=TRUE,
                    updated_at=clock_timestamp()
                WHERE service_id=%s
                """,
                (next_owner, service_id),
            )
            return self._owner(connection, service_id)

    def rollback(self, service_id: str, *, expected_epoch: int) -> OwnerEpochRecord:
        with self.transaction() as connection:
            current = self._locked_owner(connection, service_id)
            _check_epoch(current, expected_epoch)
            if not current.rollback_allowed or not current.previous_owner:
                raise ValueError("rollback fence is not satisfied")
            connection.execute(
                """
                UPDATE high_scale_ownership
                SET owner_epoch=owner_epoch+1, current_owner=previous_owner,
                    previous_owner=NULL, cutover_committed=FALSE, rollback_allowed=FALSE,
                    updated_at=clock_timestamp()
                WHERE service_id=%s
                """,
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
        for value, name in (
            (service_id, "service_id"),
            (domain, "domain"),
            (object_key, "object_key"),
            (checksum, "checksum"),
        ):
            _require_text(value, name)
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        _require_text(expected_owner, "expected_owner")
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        object_id = _source_object_id(service_id, domain, object_key, checksum)
        with self.transaction() as connection:
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, expected_owner_epoch)
            if owner.current_owner != expected_owner:
                raise ValueError("owner does not match source discovery fence")
            row = connection.execute(
                """
                SELECT * FROM high_scale_source_objects
                WHERE service_id=%s AND object_key=%s
                FOR UPDATE
                """,
                (service_id, object_key),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO high_scale_source_objects
                        (object_id, service_id, domain, object_key, checksum, size_bytes, version, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'discovered')
                    ON CONFLICT (service_id, object_key) DO NOTHING
                    """,
                    (object_id, service_id, domain, object_key, checksum, size_bytes, version),
                )
                row = connection.execute(
                    """
                    SELECT * FROM high_scale_source_objects
                    WHERE service_id=%s AND object_key=%s
                    FOR UPDATE
                    """,
                    (service_id, object_key),
                ).fetchone()
            if row is None:
                raise RuntimeError("source discovery insert did not produce a row")
            source = _source_from_row(row)
            if source.domain != domain or source.checksum != checksum or source.version != version:
                raise ValueError(f"source identity changed for {object_key}")
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
        _require_text(worker_id, "worker_id")
        _require_text(expected_owner, "expected_owner")
        if expected_owner_epoch <= 0 or lease_seconds <= 0:
            raise ValueError("owner epoch and lease duration must be positive")
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        with self.transaction() as connection:
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, expected_owner_epoch)
            if owner.current_owner != expected_owner:
                raise ValueError("owner does not match source claim fence")
            row = connection.execute(
                """
                SELECT * FROM high_scale_source_objects
                WHERE service_id=%s AND object_key=%s
                FOR UPDATE
                """,
                (service_id, object_key),
            ).fetchone()
            if row is None:
                raise KeyError(object_key)
            source = _source_from_row(row)
            if source.status == "source_deleted":
                raise ValueError(f"source object is already deleted: {object_key}")
            if source.status not in {"discovered", "claimed"}:
                raise ValueError(f"source object is not claimable: {object_key}")
            if source.lease_until is not None and source.lease_until > observed:
                return SourceClaim(source.object_id, False, source.lease_generation)
            generation = source.lease_generation + 1
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET status='claimed', owner=%s, lease_until=%s,
                    lease_generation=%s, owner_epoch=%s, updated_at=clock_timestamp()
                WHERE object_id=%s
                """,
                (
                    worker_id,
                    observed + timedelta(seconds=lease_seconds),
                    generation,
                    expected_owner_epoch,
                    source.object_id,
                ),
            )
            return SourceClaim(source.object_id, True, generation)

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
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if source.status != "claimed" or source.owner_epoch != expected_owner_epoch:
                raise RuntimeError(f"stale or missing claim for {object_key}")
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET accepted_rows=%s, malformed_rows=%s, updated_at=clock_timestamp()
                WHERE object_id=%s AND status='claimed' AND owner_epoch=%s
                """,
                (accepted_rows, malformed_rows, source.object_id, expected_owner_epoch),
            )

    def mark_source_appended(self, service_id: str, object_key: str, *, lease_generation: int) -> None:
        self._transition_source(
            service_id,
            object_key,
            lease_generation=lease_generation,
            expected_status="claimed",
            next_status="appended",
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
        with self.transaction() as connection:
            owner = self._locked_owner(connection, manifest.source.service_id)
            _check_epoch(owner, owner_epoch)
            connection.execute(
                """
                INSERT INTO high_scale_archive_manifests (
                    manifest_id, service_id, domain, source_key, source_checksum, source_size,
                    source_version, artifact_uri, artifact_checksum, artifact_size, row_count,
                    byte_count, canonical_digest, schema_version, transform_version,
                    coverage_start, coverage_end, retention_deadline,
                    deletion_authorization_deadline, archive_epoch, owner_epoch, state
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (manifest_id) DO NOTHING
                """,
                (
                    manifest.manifest_id,
                    manifest.source.service_id,
                    manifest.source.domain,
                    manifest.source.object_key,
                    manifest.source.checksum,
                    manifest.source.size_bytes,
                    manifest.source.version,
                    manifest.artifact.uri,
                    manifest.artifact.checksum,
                    manifest.artifact.size_bytes,
                    manifest.artifact.row_count,
                    manifest.artifact.byte_count,
                    manifest.artifact.canonical_digest,
                    manifest.artifact.schema_version,
                    manifest.artifact.transform_version,
                    manifest.coverage_start,
                    manifest.coverage_end,
                    manifest.retention_deadline,
                    manifest.deletion_authorization_deadline,
                    manifest.archive_epoch,
                    owner_epoch,
                    state.value,
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
                """
                UPDATE high_scale_archive_manifests
                SET state=%s, updated_at=clock_timestamp()
                WHERE manifest_id=%s AND owner_epoch=%s AND state=%s
                """,
                (next_state.value, manifest_id, expected_owner_epoch, expected_state.value),
            )
            return self._archive_manifest(connection, manifest_id)

    def archive_manifest(self, manifest_id: str) -> ArchiveManifestRecord:
        _require_text(manifest_id, "manifest_id")
        with self.transaction(read_only=True) as connection:
            return self._archive_manifest(connection, manifest_id)

    def mark_source_archived(
        self,
        service_id: str,
        object_key: str,
        *,
        lease_generation: int,
        manifest_id: str,
        owner_epoch: int,
    ) -> None:
        with self.transaction() as connection:
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if (
                source.status != "appended"
                or source.lease_generation != lease_generation
                or source.owner_epoch != owner_epoch
            ):
                raise RuntimeError(f"stale or missing claim for {object_key}")
            manifest = self._locked_archive_manifest(connection, manifest_id)
            if (
                manifest.state is not ArchiveState.MANIFEST_COMMITTED
                or manifest.owner_epoch != owner_epoch
                or manifest.manifest.source.service_id != service_id
                or manifest.manifest.source.domain != source.domain
                or manifest.manifest.source.object_key != object_key
                or manifest.manifest.source.checksum != source.checksum
                or manifest.manifest.source.version != source.version
            ):
                raise ValueError("archive manifest is not committed for source")
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET status='archived', lease_until=NULL, archive_manifest_id=%s,
                    updated_at=clock_timestamp()
                WHERE object_id=%s
                """,
                (source.object_id, source.object_id),
            )

    def acknowledge_source(self, service_id: str, object_key: str, *, manifest_id: str) -> None:
        with self.transaction() as connection:
            source = self._locked_source(connection, service_id, object_key)
            if source.status != "archived" or source.archive_manifest_id != manifest_id:
                raise ValueError(f"archive is not complete for {object_key}")
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET status='acknowledged', updated_at=clock_timestamp()
                WHERE object_id=%s
                """,
                (source.object_id,),
            )

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
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        with self.transaction() as connection:
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if (
                source.status not in {"archived", "acknowledged"}
                or source.archive_manifest_id != manifest_id
                or source.owner_epoch != expected_owner_epoch
            ):
                raise ValueError(f"source deletion is not authorized for {object_key}")
            manifest = self._locked_archive_manifest(connection, manifest_id)
            if manifest.manifest.source.object_key != object_key:
                raise ValueError("archive manifest does not match source")
            if manifest.state is not ArchiveState.DELETION_ELIGIBLE:
                raise ValueError("archive manifest is not deletion eligible")
            if observed < manifest.manifest.deletion_authorization_deadline:
                raise ValueError("source deletion grace period has not elapsed")
            cursor = connection.execute(
                """
                INSERT INTO high_scale_deletion_authorizations
                    (object_id, manifest_id, owner_epoch)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id, manifest_id) DO NOTHING
                RETURNING object_id, manifest_id, owner_epoch, authorized_at, consumed_at
                """,
                (source.object_id, manifest_id, expected_owner_epoch),
            )
            row = cursor.fetchone()
            if row is None:
                row = connection.execute(
                    """
                    SELECT object_id, manifest_id, owner_epoch, authorized_at, consumed_at
                    FROM high_scale_deletion_authorizations
                    WHERE object_id=%s AND manifest_id=%s
                    FOR UPDATE
                    """,
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
            owner = self._locked_owner(connection, service_id)
            _check_epoch(owner, expected_owner_epoch)
            source = self._locked_source(connection, service_id, object_key)
            if source.status == "source_deleted" and source.archive_manifest_id == manifest_id:
                return
            if source.status not in {"archived", "acknowledged"} or source.archive_manifest_id != manifest_id:
                raise ValueError(f"source deletion was not authorized for {object_key}")
            authorization = connection.execute(
                """
                SELECT object_id FROM high_scale_deletion_authorizations
                WHERE object_id=%s AND manifest_id=%s AND owner_epoch=%s AND consumed_at IS NULL
                FOR UPDATE
                """,
                (source.object_id, manifest_id, expected_owner_epoch),
            ).fetchone()
            if authorization is None:
                raise ValueError(f"source deletion was not authorized for {object_key}")
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET status='source_deleted', lease_until=NULL, updated_at=clock_timestamp()
                WHERE object_id=%s
                """,
                (source.object_id,),
            )
            connection.execute(
                """
                UPDATE high_scale_deletion_authorizations
                SET consumed_at=clock_timestamp()
                WHERE object_id=%s AND manifest_id=%s AND consumed_at IS NULL
                """,
                (source.object_id, manifest_id),
            )
            manifest = self._locked_archive_manifest(connection, manifest_id)
            if manifest.state is ArchiveState.DELETION_ELIGIBLE:
                connection.execute(
                    """
                    UPDATE high_scale_archive_manifests
                    SET state=%s, updated_at=clock_timestamp()
                    WHERE manifest_id=%s AND owner_epoch=%s
                    """,
                    (ArchiveState.SOURCE_DELETED.value, manifest_id, expected_owner_epoch),
                )

    def _transition_source(
        self,
        service_id: str,
        object_key: str,
        *,
        lease_generation: int,
        expected_status: str,
        next_status: str,
    ) -> None:
        if expected_status not in _SOURCE_STATES or next_status not in _SOURCE_STATES:
            raise ValueError("unknown source state")
        with self.transaction() as connection:
            source = self._locked_source(connection, service_id, object_key)
            if source.status != expected_status or source.lease_generation != lease_generation:
                raise RuntimeError(f"stale or missing claim for {object_key}")
            connection.execute(
                """
                UPDATE high_scale_source_objects
                SET status=%s, lease_until=NULL, updated_at=clock_timestamp()
                WHERE object_id=%s AND status=%s AND lease_generation=%s
                """,
                (next_status, source.object_id, expected_status, lease_generation),
            )

    @staticmethod
    def _owner(connection: Any, service_id: str) -> OwnerEpochRecord:
        row = connection.execute("SELECT * FROM high_scale_ownership WHERE service_id=%s", (service_id,)).fetchone()
        if row is None:
            raise KeyError(service_id)
        return _owner_from_row(row)

    @staticmethod
    def _locked_owner(connection: Any, service_id: str) -> OwnerEpochRecord:
        row = connection.execute(
            "SELECT * FROM high_scale_ownership WHERE service_id=%s FOR UPDATE", (service_id,)
        ).fetchone()
        if row is None:
            raise KeyError(service_id)
        return _owner_from_row(row)

    @staticmethod
    def _locked_source(connection: Any, service_id: str, object_key: str) -> SourceObjectRecord:
        row = connection.execute(
            """
            SELECT * FROM high_scale_source_objects
            WHERE service_id=%s AND object_key=%s
            FOR UPDATE
            """,
            (service_id, object_key),
        ).fetchone()
        if row is None:
            raise KeyError(object_key)
        return _source_from_row(row)

    @staticmethod
    def _archive_manifest(connection: Any, manifest_id: str) -> ArchiveManifestRecord:
        row = connection.execute(
            "SELECT * FROM high_scale_archive_manifests WHERE manifest_id=%s", (manifest_id,)
        ).fetchone()
        if row is None:
            raise KeyError(manifest_id)
        return _archive_from_row(row)

    @staticmethod
    def _locked_archive_manifest(connection: Any, manifest_id: str) -> ArchiveManifestRecord:
        row = connection.execute(
            """
            SELECT * FROM high_scale_archive_manifests
            WHERE manifest_id=%s
            FOR UPDATE
            """,
            (manifest_id,),
        ).fetchone()
        if row is None:
            raise KeyError(manifest_id)
        return _archive_from_row(row)


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
        None if _row_value(row, "owner_epoch", 12) is None else int(_row_value(row, "owner_epoch", 12)),
        int(_row_value(row, "malformed_rows", 13)),
        int(_row_value(row, "accepted_rows", 14)),
    )


def _archive_from_row(row: Mapping[str, Any] | Any) -> ArchiveManifestRecord:
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
    manifest = ArchiveManifest(
        manifest_id=str(_row_value(row, "manifest_id", 0)),
        source=source,
        artifact=artifact,
        coverage_start=_row_value(row, "coverage_start", 15),
        coverage_end=_row_value(row, "coverage_end", 16),
        retention_deadline=_row_value(row, "retention_deadline", 17),
        deletion_authorization_deadline=_row_value(row, "deletion_authorization_deadline", 18),
        archive_epoch=int(_row_value(row, "archive_epoch", 19)),
    )
    return ArchiveManifestRecord(
        manifest,
        ArchiveState(str(_row_value(row, "state", 21))),
        int(_row_value(row, "owner_epoch", 20)),
    )


def _row_value(row: Mapping[str, Any] | Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    return row[index]


def _check_epoch(owner: OwnerEpochRecord, expected_epoch: int) -> None:
    _check_epoch_value(owner.owner_epoch, expected_epoch)


def _check_epoch_value(actual: int, expected: int) -> None:
    if actual != expected:
        raise ValueError("owner epoch does not match fence")


def _source_object_id(service_id: str, domain: str, object_key: str, checksum: str) -> str:
    payload = "\0".join((service_id, domain, object_key, checksum)).encode()
    return hashlib.sha256(payload).hexdigest()


def _require_text(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} is required")
