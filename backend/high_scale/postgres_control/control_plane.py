"""Transactional Postgres control-plane state for the isolated high-scale slice."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg_pool import ConnectionPool

from backend.core.metadata.base import get_pg_pool
from backend.high_scale.archive import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
    validate_archive_transition,
)
from backend.high_scale.ingest_controller import INITIAL_SOURCE_CURSOR, RUM_SOURCE_DOMAINS

logger = logging.getLogger("backend.high_scale.postgres_control")

# Bucket labels match the legacy ``ingested_files`` log-activity shape the usage page joins with Fastly stats.
# About 130 flushes on a ~35-node service: the average settles within ~1% of a 4x larger sample.
REQUEST_FLUSH_SAMPLE_OBJECTS = 5000

# Request keys sort by delivery minute, so bounding ``object_key`` walks the ``(service_id, object_key)`` index.
# The anchor recovers the service's configured key prefix, and request keys all sort below its RUM keys.
_REQUEST_KEY_BASE_CTE = (
    "anchor AS (SELECT left(object_key, strpos(object_key, 'raw/request/') - 1) AS base "
    "FROM high_scale_source_objects WHERE service_id=%s AND domain='request' "
    "AND strpos(object_key, 'raw/request/') > 0 LIMIT 1)"
)
_FLUSH_TIMESTAMP_RE = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
_INGESTED = "status NOT IN ('discovered', 'claimed')"
# Fastly names the minute partition and the basename from the same timestamp; the pad matches the legacy reader.
_FLUSH_PARTITION_PAD = timedelta(hours=2)

_ACTIVITY_BUCKET_FORMATS = {
    "second": 'YYYY-MM-DD"T"HH24:MI:SS',
    "minute": 'YYYY-MM-DD"T"HH24:MI',
    "hour": 'YYYY-MM-DD"T"HH24:"00"',
    "day": "YYYY-MM-DD",
}

# Advisory lock keys; 2080406 is the ClickHouse control schema lock in backend/core/clickhouse/health.py.
SCHEMA_LOCK_KEY = 2080407
COMMITTED_MANIFEST_INDEX_LOCK_KEY = 2080408

# Every pod runs this at boot. The transaction lock serializes them: concurrent CREATE TABLE IF NOT EXISTS races on
# the catalog, and concurrent constraint swaps would each drop the other's constraint.
SCHEMA_DDL = f"""
SELECT pg_advisory_xact_lock({SCHEMA_LOCK_KEY});
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
CREATE INDEX IF NOT EXISTS high_scale_source_objects_domain_status_idx ON high_scale_source_objects (service_id, domain, status);
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
-- Catalog-guarded so a boot with nothing to change takes no lock on the table. ALTER and CREATE INDEX lock it
-- even when IF NOT EXISTS makes them no-ops, which would queue them (and every writer behind them) behind
-- another pod's concurrent index build, and that build would then deadlock waiting for this transaction.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = 'high_scale_archive_manifests'::regclass
          AND attname = 'sources_json' AND NOT attisdropped
    ) THEN
        ALTER TABLE high_scale_archive_manifests ADD COLUMN IF NOT EXISTS sources_json TEXT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = 'high_scale_archive_manifests'::regclass
          AND attname = 'archive_expired_at' AND NOT attisdropped
    ) THEN
        ALTER TABLE high_scale_archive_manifests ADD COLUMN IF NOT EXISTS archive_expired_at TIMESTAMPTZ;
    END IF;
    IF to_regclass('high_scale_archive_manifest_source_idx') IS NULL THEN
        CREATE INDEX IF NOT EXISTS high_scale_archive_manifest_source_idx
            ON high_scale_archive_manifests (service_id, source_key);
    END IF;
    IF to_regclass('high_scale_archive_manifests_domain_expiry_idx') IS NULL THEN
        CREATE INDEX IF NOT EXISTS high_scale_archive_manifests_domain_expiry_idx
            ON high_scale_archive_manifests (service_id, domain, coverage_end)
            WHERE state = 'source_deleted' AND archive_expired_at IS NULL;
    END IF;
END $$;
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
    status TEXT NOT NULL CHECK (status IN ('pending', 'visible', 'superseded', 'abandoned')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'high_scale_publication_manifests'::regclass
          AND conname = 'high_scale_publication_manifests_status_check'
          AND position('abandoned' IN pg_get_constraintdef(oid)) > 0
    ) THEN
        ALTER TABLE high_scale_publication_manifests DROP CONSTRAINT IF EXISTS high_scale_publication_manifests_status_check;
        ALTER TABLE high_scale_publication_manifests ADD CONSTRAINT high_scale_publication_manifests_status_check
            CHECK (status IN ('pending', 'visible', 'superseded', 'abandoned')) NOT VALID;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS high_scale_publication_manifests_domain_status_idx
    ON high_scale_publication_manifests (service_id, domain, status);
CREATE TABLE IF NOT EXISTS high_scale_deletion_authorizations (
    object_id TEXT NOT NULL REFERENCES high_scale_source_objects(object_id),
    manifest_id TEXT NOT NULL REFERENCES high_scale_archive_manifests(manifest_id),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    authorized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), consumed_at TIMESTAMPTZ,
    PRIMARY KEY (object_id, manifest_id)
);
"""

# Built by ``ensure_committed_manifest_index``, not SCHEMA_DDL: CONCURRENTLY cannot run inside a transaction.
COMMITTED_MANIFEST_INDEX = "high_scale_archive_manifests_committed_idx"
_COMMITTED_MANIFEST_INDEX_DDL = (
    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {COMMITTED_MANIFEST_INDEX} "
    "ON high_scale_archive_manifests (service_id, created_at, manifest_id) WHERE state = 'manifest_committed'"
)
# A concurrent build waits for every older transaction in the database; an idle-in-transaction session would
# otherwise stall the booting worker forever. Timing out leaves an INVALID index the next boot rebuilds.
_INDEX_BUILD_LOCK_TIMEOUT = "60s"


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
    discovered_at: datetime | None = None

    @property
    def claim_token(self) -> SourceClaimToken:
        if self.status != "claimed" or not self.owner or self.owner_epoch is None:
            raise ValueError(f"source {self.object_key} does not have an active claim")
        return SourceClaimToken(self.object_key, self.owner, self.owner_epoch, self.lease_generation)


@dataclass(frozen=True)
class SourceClaim:
    object_id: str
    claimed: bool
    lease_generation: int
    owner: str | None = None
    owner_epoch: int | None = None


@dataclass(frozen=True)
class SourceClaimToken:
    object_key: str
    owner: str
    owner_epoch: int
    lease_generation: int


@dataclass(frozen=True)
class ArchiveManifestRecord:
    manifest: ArchiveManifest
    state: ArchiveState
    owner_epoch: int


@dataclass(frozen=True)
class ArchivedPublicationClaim:
    """Sources bound to a committed archive manifest whose publications this worker now retries."""

    manifest_id: str
    object_keys: tuple[str, ...]
    publication_epoch: int
    claim_tokens: tuple[SourceClaimToken, ...] = ()


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
        try:
            self.ensure_committed_manifest_index()
        except psycopg.Error:
            logger.warning(
                "high-scale committed-manifest index build failed; the publication reaper scans without it",
                exc_info=True,
            )

    def ensure_committed_manifest_index(self) -> str:
        """Build the publication reaper's partial index without blocking writers.

        Runs on an autocommit connection after the schema transaction has committed. One pod builds under a
        session advisory lock and the others skip; an INVALID leftover of an interrupted build is dropped and
        rebuilt. The reaper only reads faster with the index. Returns present, built, rebuilt or skipped.
        """
        ctx = (
            self.pool.connection(timeout=self.pool_timeout) if self.pool_timeout is not None else self.pool.connection()
        )
        with ctx as connection:
            autocommit = connection.autocommit
            connection.autocommit = True
            try:
                locked = connection.execute(
                    "SELECT pg_try_advisory_lock(%s)", (COMMITTED_MANIFEST_INDEX_LOCK_KEY,)
                ).fetchone()
                if locked is None or not locked[0]:
                    return "skipped"
                try:
                    return _build_committed_manifest_index(connection)
                finally:
                    connection.execute("SELECT pg_advisory_unlock(%s)", (COMMITTED_MANIFEST_INDEX_LOCK_KEY,))
            finally:
                connection.autocommit = autocommit

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
        partition_id: str = "default",
        reinspection_cursor: str | None = None,
        forward_pages_since_reinspection: int | None = None,
    ) -> OwnerEpochRecord:
        _require_texts(source_cursor=source_cursor, expected_owner=expected_owner, partition_id=partition_id)
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
                if partition_id != "default":
                    connection.execute(
                        "INSERT INTO high_scale_partition_cursors (service_id, domain, partition_id, source_cursor, updated_at) "
                        "VALUES (%s, %s, %s, %s, clock_timestamp()) "
                        "ON CONFLICT (service_id, domain, partition_id) DO UPDATE "
                        "SET source_cursor=EXCLUDED.source_cursor, updated_at=clock_timestamp()",
                        (service_id, domain, partition_id, source_cursor),
                    )
                    return current
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

    def source_cursor_for(self, service_id: str, domain: str, *, partition_id: str = "default") -> str:
        """Return this domain's persisted cursor or INITIAL_SOURCE_CURSOR (never the owner-level cursor)."""
        _require_texts(domain=domain, partition_id=partition_id)
        with self.transaction(read_only=True) as connection:
            if partition_id != "default":
                row = connection.execute(
                    "SELECT source_cursor FROM high_scale_partition_cursors WHERE service_id=%s AND domain=%s AND partition_id=%s",
                    (service_id, domain, partition_id),
                ).fetchone()
                return str(row[0]) if row is not None else INITIAL_SOURCE_CURSOR
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
                "AND ("
                "(status='claimed' AND lease_until IS NOT NULL AND lease_until <= clock_timestamp()) "
                "OR (status='discovered' AND lease_until IS NULL AND "
                "(last_error IS NOT NULL OR updated_at <= clock_timestamp() - interval '30 seconds'))"
                ") "
                "ORDER BY COALESCE(lease_until, updated_at), object_key LIMIT %s",
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
            return source

    def discover_sources_batch(
        self,
        service_id: str,
        domain: str,
        objects: Sequence[Any],
        *,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> dict[str, SourceObjectRecord]:
        _require_texts(service_id=service_id, domain=domain, expected_owner=expected_owner)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        if not objects:
            return {}
        for obj in objects:
            _require_texts(object_key=obj.object_key, checksum=obj.checksum)
            if obj.size_bytes < 0:
                raise ValueError("size_bytes must be non-negative")
        object_keys = [obj.object_key for obj in objects]
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner=expected_owner,
                fence_name="source discovery",
            )
            existing_rows = connection.execute(
                "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s) FOR UPDATE",
                (service_id, object_keys),
            ).fetchall()
            by_key: dict[str, SourceObjectRecord] = {}
            for row in existing_rows:
                rec = _source_from_row(row)
                by_key[rec.object_key] = rec
            missing = [obj for obj in objects if obj.object_key not in by_key]
            if missing:
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, 'discovered')"] * len(missing))
                params: list[Any] = []
                for obj in missing:
                    params.extend(
                        (
                            _source_object_id(service_id, domain, obj.object_key, obj.checksum),
                            service_id,
                            domain,
                            obj.object_key,
                            obj.checksum,
                            obj.size_bytes,
                            obj.version,
                        )
                    )
                connection.execute(
                    "INSERT INTO high_scale_source_objects "
                    "(object_id, service_id, domain, object_key, checksum, size_bytes, version, status) "
                    f"VALUES {placeholders} ON CONFLICT (service_id, object_key) DO NOTHING",
                    tuple(params),
                )
                new_rows = connection.execute(
                    "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s) FOR UPDATE",
                    (service_id, [obj.object_key for obj in missing]),
                ).fetchall()
                for row in new_rows:
                    rec = _source_from_row(row)
                    by_key[rec.object_key] = rec
            for obj in objects:
                source = by_key.get(obj.object_key)
                if source is None:
                    raise RuntimeError(f"source discovery insert did not produce a row for {obj.object_key}")
                if source.checksum != obj.checksum or source.version != obj.version:
                    raise ValueError(f"source identity changed for {obj.object_key}")
            return by_key

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
                return SourceClaim(
                    source.object_id,
                    source.owner == worker_id,
                    source.lease_generation,
                    source.owner,
                    source.owner_epoch,
                )
            generation = source.lease_generation + 1
            connection.execute(
                "UPDATE high_scale_source_objects SET status='claimed', owner=%s, lease_until=%s, "
                "lease_generation=%s, owner_epoch=%s, last_error=NULL, updated_at=clock_timestamp() WHERE object_id=%s",
                (
                    worker_id,
                    observed + timedelta(seconds=lease_seconds),
                    generation,
                    expected_owner_epoch,
                    source.object_id,
                ),
            )
            return SourceClaim(source.object_id, True, generation, worker_id, expected_owner_epoch)

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
                    "lease_generation=lease_generation + 1, owner_epoch=%s, last_error=NULL, updated_at=clock_timestamp() "
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
        self, service_id: str, object_key: str, *, accepted_rows: int, malformed_rows: int, expected_owner_epoch: int
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

    def record_source_counts_batch(
        self,
        service_id: str,
        counts: Sequence[tuple[str, int, int]],
        *,
        expected_owner_epoch: int,
    ) -> None:
        if not counts:
            return
        for _, accepted_rows, malformed_rows in counts:
            if min(accepted_rows, malformed_rows) < 0:
                raise ValueError("ledger counts must be non-negative")
        object_keys = [key for key, _, _ in counts]
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            rows = connection.execute(
                "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s) FOR UPDATE",
                (service_id, object_keys),
            ).fetchall()
            by_key = {rec.object_key: rec for rec in (_source_from_row(r) for r in rows)}
            for object_key, _, _ in counts:
                source = by_key.get(object_key)
                if source is None or source.status != "claimed" or source.owner_epoch != expected_owner_epoch:
                    raise RuntimeError(f"stale or missing claim for {object_key}")
            placeholders = ", ".join(["(%s, %s::bigint, %s::bigint)"] * len(counts))
            params: list[Any] = []
            for object_key, accepted_rows, malformed_rows in counts:
                params.extend((by_key[object_key].object_id, accepted_rows, malformed_rows))
            params.append(expected_owner_epoch)
            connection.execute(
                "UPDATE high_scale_source_objects AS o "
                "SET accepted_rows = v.accepted_rows, malformed_rows = v.malformed_rows, updated_at = clock_timestamp() "
                f"FROM (VALUES {placeholders}) AS v(object_id, accepted_rows, malformed_rows) "
                "WHERE o.object_id = v.object_id AND o.status = 'claimed' AND o.owner_epoch = %s",
                tuple(params),
            )

    def get_batch_manifest(self, batch_id: str) -> Any | None:
        from backend.high_scale.archive import BatchManifest, PublicationStatus

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

    def committed_archive_manifests(
        self,
        service_id: str,
        *,
        min_age_seconds: float,
        max_age_seconds: float | None = None,
        after: tuple[datetime, str] | None = None,
        limit: int = 500,
    ) -> list[tuple[str, str, datetime]]:
        """Archive manifests still ``manifest_committed`` (never acknowledged), oldest first, keyset-paged."""
        _require_text(service_id, "service_id")
        sql = (
            "SELECT manifest_id, domain, created_at FROM high_scale_archive_manifests "
            "WHERE service_id=%s AND state='manifest_committed' "
            "AND created_at < clock_timestamp() - make_interval(secs => %s)"
        )
        params: list[Any] = [service_id, float(min_age_seconds)]
        if max_age_seconds is not None:
            sql += " AND created_at >= clock_timestamp() - make_interval(secs => %s)"
            params.append(float(max_age_seconds))
        if after is not None:
            sql += " AND (created_at, manifest_id) > (%s, %s)"
            params.extend(after)
        sql += " ORDER BY created_at, manifest_id LIMIT %s"
        params.append(max(1, int(limit)))
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(sql, tuple(params)).fetchall()
        return [(str(r[0]), str(r[1]), r[2]) for r in rows]

    def pending_publication_domains(self, service_id: str) -> tuple[str, ...]:
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT DISTINCT domain FROM high_scale_publication_manifests WHERE service_id=%s AND status='pending'",
                (service_id,),
            ).fetchall()
        return tuple(sorted(str(r[0]) for r in rows))

    def publication_manifest_states(self, service_id: str, batch_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        _require_text(service_id, "service_id")
        if not batch_ids:
            return {}
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT batch_id, status, domain, generation, "
                "EXTRACT(EPOCH FROM (clock_timestamp() - updated_at)) FROM high_scale_publication_manifests "
                "WHERE service_id=%s AND batch_id = ANY(%s)",
                (service_id, list(batch_ids)),
            ).fetchall()
        return {
            str(r[0]): {"status": str(r[1]), "domain": str(r[2]), "generation": str(r[3]), "age_seconds": float(r[4])}
            for r in rows
        }

    def archive_manifest_source_states(
        self, service_id: str, manifest_ids: Sequence[str]
    ) -> dict[str, list[tuple[str | None, str | None]]]:
        """``(source status, owning archive manifest)`` for each source of each manifest; ``None`` when untracked."""
        _require_text(service_id, "service_id")
        if not manifest_ids:
            return {}
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT ms.manifest_id, so.status, so.archive_manifest_id FROM high_scale_archive_manifest_sources ms "
                "LEFT JOIN high_scale_source_objects so ON so.service_id=%s AND so.object_key=ms.source_key "
                "WHERE ms.manifest_id = ANY(%s)",
                (service_id, list(manifest_ids)),
            ).fetchall()
        states: dict[str, list[tuple[str | None, str | None]]] = {}
        for manifest_id, status, owner in rows:
            states.setdefault(str(manifest_id), []).append((status, owner))
        return states

    def archive_manifest_domains(self, service_id: str, manifest_ids: Sequence[str]) -> dict[str, str]:
        _require_text(service_id, "service_id")
        if not manifest_ids:
            return {}
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT manifest_id, domain FROM high_scale_archive_manifests WHERE service_id=%s AND manifest_id = ANY(%s)",
                (service_id, list(manifest_ids)),
            ).fetchall()
        return {str(r[0]): str(r[1]) for r in rows}

    def mark_publications_superseded(self, service_id: str, batch_ids: Sequence[str]) -> int:
        """Move still-pending publications to ``superseded``; never touches a row that became visible."""
        return self._retire_pending_publications(service_id, batch_ids, "superseded")

    def mark_publications_abandoned(self, service_id: str, batch_ids: Sequence[str]) -> int:
        """Move still-pending publications to ``abandoned``; never touches a row that became visible."""
        return self._retire_pending_publications(service_id, batch_ids, "abandoned")

    def _retire_pending_publications(self, service_id: str, batch_ids: Sequence[str], status: str) -> int:
        _require_text(service_id, "service_id")
        if not batch_ids:
            return 0
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE high_scale_publication_manifests SET status=%s, updated_at=clock_timestamp() "
                "WHERE service_id=%s AND batch_id = ANY(%s) AND status='pending'",
                (status, service_id, list(batch_ids)),
            )
            return int(cursor.rowcount or 0)

    def register_archive_manifest(
        self, manifest: ArchiveManifest, *, owner_epoch: int, state: ArchiveState = ArchiveState.ARTIFACT_UPLOADING
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
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(sources))
                src_params: list[Any] = []
                for src in sources:
                    src_params.extend(
                        (
                            manifest.manifest_id,
                            src.key,
                            src.checksum,
                            src.size_bytes,
                            src.version,
                            src.row_start,
                            src.row_count,
                            manifest.deletion_authorization_deadline,
                        )
                    )
                connection.execute(
                    "INSERT INTO high_scale_archive_manifest_sources "
                    "(manifest_id, source_key, source_checksum, source_size, source_version, row_start, row_count, deletion_deadline) "
                    f"VALUES {placeholders} ON CONFLICT (manifest_id, source_key) DO NOTHING",
                    tuple(src_params),
                )
            result = self._archive_manifest(connection, manifest.manifest_id)
            if result.manifest != manifest or result.owner_epoch != owner_epoch:
                raise ValueError("archive manifest identity changed")
            return result

    def transition_archive_manifest(
        self, manifest_id: str, *, expected_state: ArchiveState, next_state: ArchiveState, expected_owner_epoch: int
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
                "AND archive_expired_at IS NULL AND coverage_end > %s AND coverage_start < %s ORDER BY coverage_start",
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
                "WHERE o.service_id = %s AND o.status = 'acknowledged' "
                "AND m.state IN ('manifest_committed', 'deletion_eligible') "
                "AND m.deletion_authorization_deadline <= statement_timestamp() "
                "ORDER BY o.discovered_at LIMIT %s",
                (service_id, limit),
            ).fetchall()
            return tuple(_source_from_row(row) for row in rows)

    def expirable_archive_manifests(
        self, service_id: str, domain_retention_days: Mapping[str, int], *, limit: int = 100
    ) -> tuple[ArchiveManifest, ...]:
        """Manifests whose raw source is already gone and whose coverage is older than the domain's retention.

        A domain with a retention of 0 days (or less) is kept forever.
        """
        _require_text(service_id, "service_id")
        if limit <= 0:
            raise ValueError("limit must be positive")
        windows = [(domain, int(days)) for domain, days in domain_retention_days.items() if int(days) > 0]
        if not windows:
            return ()
        predicate = " OR ".join(
            "(domain = %s AND coverage_end < statement_timestamp() - make_interval(days => %s))" for _ in windows
        )
        params = [value for window in windows for value in window]
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM high_scale_archive_manifests WHERE service_id = %s AND state = 'source_deleted' "
                f"AND archive_expired_at IS NULL AND ({predicate}) ORDER BY coverage_end LIMIT %s",
                (service_id, *params, limit),
            ).fetchall()
            return tuple(_archive_from_row(row).manifest for row in rows)

    def archive_storage_summary(self, service_id: str) -> dict[str, int]:
        """FOS footprint of a service's archive: live bytes and objects, and bytes expired in the last 30 days.

        Each non-expired manifest is three objects (artifact, manifest, commit marker). FOS bills a deleted object
        for 30 days, so recently expired artifacts still count towards the bill.
        """
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT "
                "coalesce(sum(artifact_size) FILTER (WHERE archive_expired_at IS NULL), 0), "
                "count(*) FILTER (WHERE archive_expired_at IS NULL), "
                "coalesce(sum(artifact_size) FILTER (WHERE archive_expired_at > statement_timestamp() - interval '30 days'), 0), "
                "coalesce(sum(artifact_size) FILTER (WHERE archive_expired_at IS NULL AND domain LIKE 'rum%%'), 0) "
                "FROM high_scale_archive_manifests WHERE service_id = %s AND state <> 'artifact_uploading'",
                (service_id,),
            ).fetchone()
        return {
            "live_bytes": int(row[0]),
            "live_objects": int(row[1]) * 3,
            "expired_30d_bytes": int(row[2]),
            "live_rum_bytes": int(row[3]),
        }

    def storage_billing_intervals(
        self,
        service_id: str,
        start: datetime,
        end: datetime,
        minimum_days: int = 30,
    ) -> tuple[list[dict[str, Any]], bool, list[str]]:
        """Build selected-period storage billing intervals from Postgres source and archive ledgers."""
        _require_text(service_id, "service_id")
        start_utc = start.astimezone(UTC) if start.tzinfo is not None else start.replace(tzinfo=UTC)
        end_utc = end.astimezone(UTC) if end.tzinfo is not None else end.replace(tzinfo=UTC)
        if end_utc < start_utc:
            raise ValueError("end must be >= start")

        start_ms = int(start_utc.timestamp() * 1000)
        end_ms = int(end_utc.timestamp() * 1000)
        min_days = max(0, int(minimum_days))
        min_window_ms = min_days * 86_400_000

        def _to_ms(dt: datetime | None) -> int | None:
            if dt is None:
                return None
            dt_utc = dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
            return int(dt_utc.timestamp() * 1000)

        intervals: list[dict[str, Any]] = []
        complete = True

        def _append_deleted(
            *,
            base_id: str,
            dataset: str,
            size_bytes: int,
            created_ms: int | None,
            deleted_ms: int | None,
            provenance: str,
            exact_deletion: bool,
        ) -> None:
            nonlocal complete
            if deleted_ms is None:
                complete = False
                intervals.append(
                    {
                        "id": f"{base_id}:min",
                        "dataset": dataset,
                        "kind": "minimum_billed",
                        "bytes": size_bytes,
                        "created_at": created_ms,
                        "deleted_at": None,
                        "billed_from": created_ms if created_ms is not None else start_ms,
                        "billed_until": end_ms,
                        "complete": False,
                        "provenance": provenance,
                    }
                )
                return
            if created_ms is None:
                complete = False
                intervals.append(
                    {
                        "id": f"{base_id}:min",
                        "dataset": dataset,
                        "kind": "minimum_billed",
                        "bytes": size_bytes,
                        "created_at": None,
                        "deleted_at": deleted_ms,
                        "billed_from": deleted_ms,
                        "billed_until": deleted_ms + min_window_ms,
                        "complete": False,
                        "provenance": provenance,
                    }
                )
                return
            is_complete = exact_deletion
            if not is_complete:
                complete = False
            if deleted_ms > created_ms:
                intervals.append(
                    {
                        "id": f"{base_id}:deleted",
                        "dataset": dataset,
                        "kind": "deleted",
                        "bytes": size_bytes,
                        "created_at": created_ms,
                        "deleted_at": deleted_ms,
                        "billed_from": created_ms,
                        "billed_until": deleted_ms,
                        "complete": is_complete,
                        "provenance": provenance,
                    }
                )
            min_until = created_ms + min_window_ms
            if min_until > deleted_ms:
                intervals.append(
                    {
                        "id": f"{base_id}:min",
                        "dataset": dataset,
                        "kind": "minimum_billed",
                        "bytes": size_bytes,
                        "created_at": created_ms,
                        "deleted_at": deleted_ms,
                        "billed_from": deleted_ms,
                        "billed_until": min_until,
                        "complete": is_complete,
                        "provenance": provenance,
                    }
                )

        with self.transaction(read_only=True) as connection:
            source_rows = connection.execute(
                "SELECT object_id, domain, size_bytes, status, discovered_at, updated_at "
                "FROM high_scale_source_objects "
                "WHERE service_id = %s "
                "AND (discovered_at IS NULL OR discovered_at < %s) "
                "AND (status NOT IN ('source_deleted', 'source_missing') "
                "OR GREATEST(updated_at, COALESCE(discovered_at, updated_at) + make_interval(days => %s)) > %s)",
                (service_id, end_utc, min_days, start_utc),
            ).fetchall()
            archive_rows = connection.execute(
                "SELECT manifest_id, domain, artifact_size, created_at, archive_expired_at "
                "FROM high_scale_archive_manifests "
                "WHERE service_id = %s AND state <> 'artifact_uploading' "
                "AND (created_at IS NULL OR created_at < %s) "
                "AND (archive_expired_at IS NULL "
                "OR GREATEST(archive_expired_at, COALESCE(created_at, archive_expired_at) + make_interval(days => %s)) > %s)",
                (service_id, end_utc, min_days, start_utc),
            ).fetchall()

        for object_id, domain, size_bytes, status, discovered_at, updated_at in source_rows:
            size = int(size_bytes)
            dataset = f"raw_{domain}"
            base_id = f"high-scale:source:{object_id}"
            created_ms = _to_ms(discovered_at)
            updated_ms = _to_ms(updated_at)
            if status in ("source_deleted", "source_missing"):
                _append_deleted(
                    base_id=base_id,
                    dataset=dataset,
                    size_bytes=size,
                    created_ms=created_ms,
                    deleted_ms=updated_ms,
                    provenance="high-scale:source_objects",
                    exact_deletion=(status == "source_deleted"),
                )
            elif created_ms is None:
                complete = False
                intervals.append(
                    {
                        "id": base_id,
                        "dataset": dataset,
                        "kind": "live",
                        "bytes": size,
                        "created_at": None,
                        "deleted_at": None,
                        "billed_from": start_ms,
                        "billed_until": end_ms,
                        "complete": False,
                        "provenance": "high-scale:source_objects",
                    }
                )
            else:
                intervals.append(
                    {
                        "id": base_id,
                        "dataset": dataset,
                        "kind": "live",
                        "bytes": size,
                        "created_at": created_ms,
                        "deleted_at": None,
                        "billed_from": created_ms,
                        "billed_until": end_ms,
                        "complete": True,
                        "provenance": "high-scale:source_objects",
                    }
                )

        for manifest_id, domain, artifact_size, created_at, archive_expired_at in archive_rows:
            size = int(artifact_size)
            dataset = f"archive_{domain}"
            base_id = f"high-scale:archive:{manifest_id}"
            created_ms = _to_ms(created_at)
            expired_ms = _to_ms(archive_expired_at)
            if expired_ms is not None:
                _append_deleted(
                    base_id=base_id,
                    dataset=dataset,
                    size_bytes=size,
                    created_ms=created_ms,
                    deleted_ms=expired_ms,
                    provenance="high-scale:archive_manifests",
                    exact_deletion=True,
                )
            elif created_ms is None:
                complete = False
                intervals.append(
                    {
                        "id": base_id,
                        "dataset": dataset,
                        "kind": "live",
                        "bytes": size,
                        "created_at": None,
                        "deleted_at": None,
                        "billed_from": start_ms,
                        "billed_until": end_ms,
                        "complete": False,
                        "provenance": "high-scale:archive_manifests",
                    }
                )
            else:
                intervals.append(
                    {
                        "id": base_id,
                        "dataset": dataset,
                        "kind": "live",
                        "bytes": size,
                        "created_at": created_ms,
                        "deleted_at": None,
                        "billed_from": created_ms,
                        "billed_until": end_ms,
                        "complete": True,
                        "provenance": "high-scale:archive_manifests",
                    }
                )

        return intervals, complete, ["high-scale:archive_manifests", "high-scale:source_objects"]

    def storage_billing_summary(
        self,
        service_id: str,
        start: datetime,
        end: datetime,
        *,
        minimum_days: int = 30,
    ) -> tuple[float, float, float, bool, list[str]]:
        """Aggregate selected-period GB-hours directly in SQL across source objects and archive manifests."""
        _require_text(service_id, "service_id")
        start_utc = _observed_utc(start)
        end_utc = _observed_utc(end)
        if end_utc <= start_utc:
            return 0.0, 0.0, 0.0, True, ["high-scale:archive_manifests", "high-scale:source_objects"]
        min_days = max(int(minimum_days), 1)
        gb_hour_denom = 1_000_000_000.0 * 3600.0

        with self.transaction(read_only=True) as connection:
            src_row = connection.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN status NOT IN ('source_deleted', 'source_missing') THEN "
                "GREATEST(size_bytes, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "%s::timestamptz - GREATEST(COALESCE(discovered_at, %s::timestamptz), %s::timestamptz)"
                "))) / %s ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE WHEN status IN ('source_deleted', 'source_missing') "
                "AND discovered_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at > discovered_at THEN "
                "GREATEST(size_bytes, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(updated_at, %s::timestamptz) - GREATEST(discovered_at, %s::timestamptz)"
                "))) / %s ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE "
                "WHEN status IN ('source_deleted', 'source_missing') AND updated_at IS NULL THEN "
                "GREATEST(size_bytes, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "%s::timestamptz - GREATEST(COALESCE(discovered_at, %s::timestamptz), %s::timestamptz)"
                "))) / %s "
                "WHEN status IN ('source_deleted', 'source_missing') AND discovered_at IS NULL THEN "
                "GREATEST(size_bytes, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(updated_at + make_interval(days => %s), %s::timestamptz) - GREATEST(updated_at, %s::timestamptz)"
                "))) / %s "
                "WHEN status IN ('source_deleted', 'source_missing') "
                "AND discovered_at + make_interval(days => %s) > updated_at THEN "
                "GREATEST(size_bytes, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(discovered_at + make_interval(days => %s), %s::timestamptz) - GREATEST(updated_at, %s::timestamptz)"
                "))) / %s "
                "ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE WHEN discovered_at IS NULL OR status = 'source_missing' "
                "OR (status IN ('source_deleted', 'source_missing') AND updated_at IS NULL) THEN 1 ELSE 0 END), 0) "
                "FROM high_scale_source_objects "
                "WHERE service_id = %s "
                "AND (discovered_at IS NULL OR discovered_at < %s) "
                "AND (status NOT IN ('source_deleted', 'source_missing') "
                "OR GREATEST(updated_at, COALESCE(discovered_at, updated_at) + make_interval(days => %s)) > %s)",
                (
                    end_utc,
                    start_utc,
                    start_utc,
                    gb_hour_denom,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    end_utc,
                    start_utc,
                    start_utc,
                    gb_hour_denom,
                    min_days,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    min_days,
                    min_days,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    service_id,
                    end_utc,
                    min_days,
                    start_utc,
                ),
            ).fetchone()
            arc_row = connection.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN archive_expired_at IS NULL THEN "
                "GREATEST(artifact_size, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "%s::timestamptz - GREATEST(COALESCE(created_at, %s::timestamptz), %s::timestamptz)"
                "))) / %s ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE WHEN archive_expired_at IS NOT NULL "
                "AND created_at IS NOT NULL AND archive_expired_at > created_at THEN "
                "GREATEST(artifact_size, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(archive_expired_at, %s::timestamptz) - GREATEST(created_at, %s::timestamptz)"
                "))) / %s ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE "
                "WHEN archive_expired_at IS NOT NULL AND created_at IS NULL THEN "
                "GREATEST(artifact_size, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(archive_expired_at + make_interval(days => %s), %s::timestamptz) "
                "- GREATEST(archive_expired_at, %s::timestamptz)"
                "))) / %s "
                "WHEN archive_expired_at IS NOT NULL "
                "AND created_at + make_interval(days => %s) > archive_expired_at THEN "
                "GREATEST(artifact_size, 0)::double precision * GREATEST(0.0, EXTRACT(EPOCH FROM ("
                "LEAST(created_at + make_interval(days => %s), %s::timestamptz) "
                "- GREATEST(archive_expired_at, %s::timestamptz)"
                "))) / %s "
                "ELSE 0.0 END), 0.0), "
                "COALESCE(SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END), 0) "
                "FROM high_scale_archive_manifests "
                "WHERE service_id = %s AND state <> 'artifact_uploading' "
                "AND (created_at IS NULL OR created_at < %s) "
                "AND (archive_expired_at IS NULL "
                "OR GREATEST(archive_expired_at, COALESCE(created_at, archive_expired_at) + make_interval(days => %s)) > %s)",
                (
                    end_utc,
                    start_utc,
                    start_utc,
                    gb_hour_denom,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    min_days,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    min_days,
                    min_days,
                    end_utc,
                    start_utc,
                    gb_hour_denom,
                    service_id,
                    end_utc,
                    min_days,
                    start_utc,
                ),
            ).fetchone()

        src_live, src_del, src_min, src_incomplete = src_row or (0.0, 0.0, 0.0, 0)
        arc_live, arc_del, arc_min, arc_incomplete = arc_row or (0.0, 0.0, 0.0, 0)
        complete = int(src_incomplete or 0) == 0 and int(arc_incomplete or 0) == 0
        return (
            float(src_live or 0.0) + float(arc_live or 0.0),
            float(src_del or 0.0) + float(arc_del or 0.0),
            float(src_min or 0.0) + float(arc_min or 0.0),
            complete,
            ["high-scale:archive_manifests", "high-scale:source_objects"],
        )

    def source_activity(self, service_id: str, start: datetime, end: datetime, by: str) -> list[tuple[str, int, int]]:
        """``(bucket, accepted_rows, size_bytes)`` of processed source objects by UTC discovery time.

        The window is inclusive at both ends. Objects still ``discovered`` or ``claimed`` are not processed yet.
        """
        _require_text(service_id, "service_id")
        if by not in _ACTIVITY_BUCKET_FORMATS:
            raise ValueError(f"unsupported activity bucket: {by}")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT to_char(date_trunc(%s, discovered_at AT TIME ZONE 'UTC'), %s) AS bucket, "
                "coalesce(sum(accepted_rows), 0), coalesce(sum(size_bytes), 0) FROM high_scale_source_objects "
                "WHERE service_id=%s AND status NOT IN ('discovered', 'claimed') "
                "AND discovered_at >= %s AND discovered_at <= %s GROUP BY bucket ORDER BY bucket",
                (by, _ACTIVITY_BUCKET_FORMATS[by], service_id, start, end),
            ).fetchall()
        return [(str(r[0]), int(r[1]), int(r[2])) for r in rows]

    def source_storage_window(self, service_id: str, start: datetime, end: datetime) -> dict[str, int]:
        """Source objects discovered in the inclusive window: count, bytes, and the RUM share of those bytes.

        Every discovered object was written to FOS, so each one counts whatever its processing state.
        """
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT count(*), coalesce(sum(size_bytes), 0), "
                "coalesce(sum(size_bytes) FILTER (WHERE domain = ANY(%s)), 0) FROM high_scale_source_objects "
                "WHERE service_id=%s AND discovered_at >= %s AND discovered_at <= %s",
                (sorted(RUM_SOURCE_DOMAINS), service_id, start, end),
            ).fetchone()
        return {"files": int(row[0]), "bytes": int(row[1]), "rum_bytes": int(row[2])}

    def request_files_per_flush(
        self, service_id: str, recent_objects: int = REQUEST_FLUSH_SAMPLE_OBJECTS
    ) -> float | None:
        """Average request source objects per log flush among the most recent request objects.

        Request keys sort by delivery minute, so the newest objects are a backward walk of the
        ``(service_id, object_key)`` index that stops below the service's RUM keys. Flushes group by the
        basename timestamp Fastly writes; the oldest sampled flush may be cut off by the sample and the
        newest may still be arriving, so both are left out.
        """
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                f"WITH {_REQUEST_KEY_BASE_CTE}, "
                f"recent AS (SELECT substring(object_key from '{_FLUSH_TIMESTAMP_RE}') AS flushed_at "
                "FROM high_scale_source_objects WHERE service_id=%s AND domain='request' "
                "AND object_key < (SELECT base || 'raw/rum/' FROM anchor) ORDER BY object_key DESC LIMIT %s), "
                "flushes AS (SELECT flushed_at, count(*) AS files FROM recent WHERE flushed_at IS NOT NULL "
                "GROUP BY flushed_at) "
                "SELECT avg(files) FROM flushes WHERE flushed_at > (SELECT min(flushed_at) FROM flushes) "
                "AND flushed_at < (SELECT max(flushed_at) FROM flushes)",
                (service_id, service_id, recent_objects),
            ).fetchone()
        return None if row[0] is None else float(row[0])

    def request_flush_accounting(
        self, service_id: str, start: datetime, end: datetime, width: int
    ) -> dict[str, tuple[int, int]]:
        """``{emission bucket: (rows, files)}`` for ingested request objects, buckets inclusive of ``start``/``end``.

        Buckets are the first ``width`` characters (13 = hour, 10 = day) of the basename timestamp Fastly writes.
        """
        from backend.provision.cli import minute_list_prefix

        _require_text(service_id, "service_id")
        if width not in (10, 13):
            raise ValueError("width must be 10 (day) or 13 (hour)")
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                f"WITH {_REQUEST_KEY_BASE_CTE}, "
                f"files AS (SELECT left(substring(object_key from '{_FLUSH_TIMESTAMP_RE}'), %s) AS bucket, "
                "accepted_rows FROM high_scale_source_objects "
                f"WHERE service_id=%s AND domain='request' AND {_INGESTED} "
                "AND object_key >= (SELECT base || %s FROM anchor) AND object_key < (SELECT base || %s FROM anchor)) "
                "SELECT bucket, sum(accepted_rows), count(*) FROM files WHERE bucket >= %s AND bucket <= %s "
                "GROUP BY bucket",
                (
                    service_id,
                    width,
                    service_id,
                    minute_list_prefix(start.astimezone(UTC) - _FLUSH_PARTITION_PAD),
                    minute_list_prefix(end.astimezone(UTC) + _FLUSH_PARTITION_PAD),
                    start.astimezone(UTC).strftime("%Y-%m-%dT%H")[:width],
                    end.astimezone(UTC).strftime("%Y-%m-%dT%H")[:width],
                ),
            ).fetchall()
        return {str(bucket): (int(rows_sum or 0), int(files)) for bucket, rows_sum, files in rows}

    def latest_request_ingest(self, service_id: str) -> datetime | None:
        """Discovery time of the newest ingested request object: everything up to it has been picked up."""
        _require_text(service_id, "service_id")
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                f"WITH {_REQUEST_KEY_BASE_CTE} SELECT discovered_at FROM high_scale_source_objects "
                f"WHERE service_id=%s AND domain='request' AND {_INGESTED} "
                "AND object_key < (SELECT base || 'raw/rum/' FROM anchor) ORDER BY object_key DESC LIMIT 1",
                (service_id, service_id),
            ).fetchone()
        return None if row is None else row[0]

    def mark_archive_expired(self, manifest_id: str) -> None:
        _require_text(manifest_id, "manifest_id")
        with self.transaction() as connection:
            connection.execute(
                "UPDATE high_scale_archive_manifests SET archive_expired_at = clock_timestamp(), "
                "updated_at = clock_timestamp() WHERE manifest_id = %s AND archive_expired_at IS NULL",
                (manifest_id,),
            )

    @staticmethod
    def _locked_claim_rows(
        connection: Any,
        service_id: str,
        object_keys: Sequence[str],
        *,
        claim_tokens: Sequence[SourceClaimToken],
        expected_owner_epoch: int,
    ) -> dict[str, SourceObjectRecord]:
        expected_keys = list(dict.fromkeys(object_keys))
        if not claim_tokens:
            raise RuntimeError("source claim tokens are required")
        tokens = {token.object_key: token for token in claim_tokens}
        if len(tokens) != len(claim_tokens) or set(tokens) != set(expected_keys):
            raise RuntimeError("source claim tokens do not match the requested objects")
        if any(token.owner_epoch != expected_owner_epoch or not token.owner for token in tokens.values()):
            raise RuntimeError("stale or missing claim")
        rows = (
            connection.execute(
                "SELECT * FROM high_scale_source_objects WHERE service_id=%s AND object_key = ANY(%s) FOR UPDATE",
                (service_id, expected_keys),
            ).fetchall()
            if expected_keys
            else ()
        )
        sources = {source.object_key: source for source in (_source_from_row(row) for row in rows)}
        if len(sources) != len(expected_keys):
            raise RuntimeError("stale or missing claim")
        for key, token in tokens.items():
            source = sources[key]
            if (
                source.status not in {"claimed", "archived"}
                or source.owner != token.owner
                or source.owner_epoch != token.owner_epoch
                or source.lease_generation != token.lease_generation
            ):
                raise RuntimeError(f"stale or missing claim for {key}")
        return sources

    def acknowledge_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        claim_tokens: Sequence[SourceClaimToken],
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
    ) -> None:
        _require_texts(service_id=service_id, manifest_id=manifest_id)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            self._locked_claim_rows(
                connection,
                service_id,
                object_keys,
                claim_tokens=claim_tokens,
                expected_owner_epoch=expected_owner_epoch,
            )
            if object_keys:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='acknowledged', archive_manifest_id=%s, "
                    "lease_until=NULL, last_error=NULL, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s)",
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

    def mark_sources_appended_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        expected_owner_epoch: int,
        claim_tokens: Sequence[SourceClaimToken],
        next_cursor: str | None = None,
        domain: str = "request",
        partition_id: str = "default",
    ) -> None:
        """Mark claimed sources as 'appended' (visible in ClickHouse, archive flush deferred)."""
        _require_texts(service_id=service_id)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            self._locked_claim_rows(
                connection,
                service_id,
                object_keys,
                claim_tokens=claim_tokens,
                expected_owner_epoch=expected_owner_epoch,
            )
            if object_keys:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='appended', archive_manifest_id=NULL, "
                    "owner=NULL, lease_until=NULL, last_error=NULL, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s)",
                    (service_id, list(object_keys)),
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

    def unarchived_appended_summary(self, service_id: str, domain: str) -> tuple[int, int, datetime | None]:
        """Return (count, total_bytes, oldest_updated_at) for 'appended' sources awaiting archive flush."""
        _require_texts(service_id=service_id, domain=domain)
        with self.transaction(read_only=True) as connection:
            row = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0), MIN(updated_at) "
                "FROM high_scale_source_objects "
                "WHERE service_id=%s AND domain=%s AND status='appended' AND archive_manifest_id IS NULL",
                (service_id, domain),
            ).fetchone()
        if not row:
            return (0, 0, None)
        return (int(row[0] or 0), int(row[1] or 0), _parse_iso_or_dt(row[2]))

    def unarchived_appended_sources(
        self,
        service_id: str,
        domain: str,
        *,
        limit: int = 500,
    ) -> tuple[SourceObjectRecord, ...]:
        """List 'appended' sources awaiting archive flush in discovery order."""
        _require_texts(service_id=service_id, domain=domain)
        with self.transaction(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM high_scale_source_objects "
                "WHERE service_id=%s AND domain=%s AND status='appended' AND archive_manifest_id IS NULL "
                "ORDER BY discovered_at, object_key LIMIT %s",
                (service_id, domain, max(1, int(limit))),
            ).fetchall()
        return tuple(_source_from_row(row) for row in rows)

    def claim_unarchived_appended_sources(
        self,
        service_id: str,
        domain: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        lease_seconds: float = 300.0,
        limit: int = 500,
        max_bytes: int | None = None,
        now: datetime | None = None,
    ) -> tuple[SourceObjectRecord, ...]:
        """Lease a bounded batch of 'appended' sources for consolidated archive flushing."""
        _require_texts(service_id=service_id, domain=domain, worker_id=worker_id)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        lease_until = observed + timedelta(seconds=max(1.0, float(lease_seconds)))
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner="high_scale",
            )
            rows = connection.execute(
                "SELECT * FROM high_scale_source_objects "
                "WHERE service_id=%s AND domain=%s AND status='appended' AND archive_manifest_id IS NULL "
                "AND (lease_until IS NULL OR lease_until <= %s) "
                "ORDER BY discovered_at, object_key LIMIT %s FOR UPDATE SKIP LOCKED",
                (service_id, domain, observed, max(1, int(limit))),
            ).fetchall()
            candidates = [_source_from_row(row) for row in rows]
            if not candidates:
                return ()
            selected: list[SourceObjectRecord] = []
            running_bytes = 0
            for rec in candidates:
                if max_bytes is not None and selected and running_bytes + rec.size_bytes > max_bytes:
                    break
                selected.append(rec)
                running_bytes += rec.size_bytes
            selected_keys = [rec.object_key for rec in selected]
            updated_rows = connection.execute(
                "UPDATE high_scale_source_objects "
                "SET owner=%s, owner_epoch=%s, lease_until=%s, lease_generation=lease_generation+1 "
                "WHERE service_id=%s AND object_key = ANY(%s) RETURNING *",
                (worker_id, expected_owner_epoch, lease_until, service_id, selected_keys),
            ).fetchall()
        updated_by_key = {rec.object_key: rec for rec in (_source_from_row(row) for row in updated_rows)}
        return tuple(updated_by_key[key] for key in selected_keys if key in updated_by_key)

    def release_unarchived_appended_sources(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        worker_id: str,
        error: str | None = None,
    ) -> None:
        """Release flush leases on 'appended' sources after a transient archive error."""
        _require_texts(service_id=service_id, worker_id=worker_id)
        if not object_keys:
            return
        with self.transaction() as connection:
            connection.execute(
                "UPDATE high_scale_source_objects "
                "SET owner=NULL, lease_until=NULL, last_error=%s "
                "WHERE service_id=%s AND object_key = ANY(%s) "
                "AND status='appended' AND archive_manifest_id IS NULL AND owner=%s",
                (error, service_id, list(object_keys), worker_id),
            )

    def attach_archive_manifest_to_sources(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        worker_id: str | None = None,
    ) -> None:
        """Transition 'appended' sources to 'acknowledged' once their archive manifest is committed."""
        _require_texts(service_id=service_id, manifest_id=manifest_id)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        if not object_keys:
            return
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            manifest = self._archive_manifest(connection, manifest_id)
            if manifest.state not in {ArchiveState.MANIFEST_COMMITTED, ArchiveState.DELETION_ELIGIBLE}:
                raise RuntimeError(f"archive manifest {manifest_id} is not committed (state={manifest.state.value})")
            if worker_id is not None:
                connection.execute(
                    "UPDATE high_scale_source_objects "
                    "SET status='acknowledged', archive_manifest_id=%s, owner_epoch=%s, owner=NULL, lease_until=NULL, "
                    "last_error=NULL, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s) "
                    "AND status='appended' AND archive_manifest_id IS NULL AND owner=%s",
                    (manifest_id, expected_owner_epoch, service_id, list(object_keys), worker_id),
                )
            else:
                connection.execute(
                    "UPDATE high_scale_source_objects "
                    "SET status='acknowledged', archive_manifest_id=%s, owner_epoch=%s, owner=NULL, lease_until=NULL, "
                    "last_error=NULL, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s) "
                    "AND status='appended' AND archive_manifest_id IS NULL",
                    (manifest_id, expected_owner_epoch, service_id, list(object_keys)),
                )

    def fail_sources_batch(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        expected_owner_epoch: int,
        error: str,
        claim_tokens: Sequence[SourceClaimToken],
    ) -> None:
        _require_texts(service_id=service_id, error=error)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            self._locked_claim_rows(
                connection,
                service_id,
                object_keys,
                claim_tokens=claim_tokens,
                expected_owner_epoch=expected_owner_epoch,
            )
            if object_keys:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='discovered', owner=NULL, lease_until=NULL, "
                    "last_error=%s, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s) AND status='claimed' AND owner_epoch=%s",
                    (error, service_id, list(object_keys), expected_owner_epoch),
                )

    def bind_sources_to_archive_manifest(
        self,
        service_id: str,
        object_keys: Sequence[str],
        *,
        manifest_id: str,
        expected_owner_epoch: int,
        error: str,
        claim_tokens: Sequence[SourceClaimToken],
    ) -> None:
        """Hold claimed sources on their committed archive so a failed publication retries from it, not from raw."""
        _require_texts(service_id=service_id, manifest_id=manifest_id, error=error)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            self._locked_claim_rows(
                connection,
                service_id,
                object_keys,
                claim_tokens=claim_tokens,
                expected_owner_epoch=expected_owner_epoch,
            )
            state = self._archive_manifest(connection, manifest_id).state
            if state not in {ArchiveState.MANIFEST_COMMITTED, ArchiveState.DELETION_ELIGIBLE}:
                raise ValueError(f"archive manifest {manifest_id} is not committed: {state.value}")
            if object_keys:
                connection.execute(
                    "UPDATE high_scale_source_objects SET status='archived', archive_manifest_id=%s, owner=NULL, "
                    "lease_until=NULL, last_error=%s, updated_at=clock_timestamp() "
                    "WHERE service_id=%s AND object_key = ANY(%s) AND status='claimed' AND owner_epoch=%s",
                    (manifest_id, error, service_id, list(object_keys), expected_owner_epoch),
                )

    def claim_archived_publication_retries(
        self,
        service_id: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        lease_seconds: float,
        min_idle_seconds: float = 0.0,
        limit: int = 1,
        exclude_manifest_ids: Sequence[str] = (),
    ) -> tuple[ArchivedPublicationClaim, ...]:
        """Lease committed manifests whose bound sources still wait for their publications."""
        _require_texts(service_id=service_id, worker_id=worker_id)
        if expected_owner_epoch <= 0 or lease_seconds <= 0:
            raise ValueError("owner epoch and lease duration must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if min_idle_seconds < 0:
            raise ValueError("min_idle_seconds must be non-negative")
        claims: list[ArchivedPublicationClaim] = []
        with self.transaction() as connection:
            self._fenced_owner(
                connection,
                service_id,
                expected_owner_epoch=expected_owner_epoch,
                expected_owner="high_scale",
                fence_name="publication-retry",
            )
            candidates = connection.execute(
                "SELECT o.archive_manifest_id FROM high_scale_source_objects o "
                "JOIN high_scale_archive_manifests m ON m.manifest_id = o.archive_manifest_id "
                "WHERE o.service_id = %s AND o.status = 'archived' "
                "AND m.state IN ('manifest_committed', 'deletion_eligible') "
                "AND NOT (o.archive_manifest_id = ANY(%s)) "
                "GROUP BY o.archive_manifest_id "
                "HAVING bool_and(o.lease_until IS NULL OR o.lease_until <= clock_timestamp()) "
                "AND MAX(o.updated_at) <= clock_timestamp() - make_interval(secs => %s) "
                "ORDER BY MIN(o.updated_at), o.archive_manifest_id LIMIT %s",
                (service_id, list(exclude_manifest_ids), min_idle_seconds, limit),
            ).fetchall()
            for (manifest_id,) in candidates:
                rows = connection.execute(
                    "SELECT object_key, owner_epoch, lease_generation, COALESCE(lease_until > clock_timestamp(), FALSE) "
                    "FROM high_scale_source_objects WHERE service_id = %s AND archive_manifest_id = %s "
                    "AND status = 'archived' ORDER BY object_key FOR UPDATE",
                    (service_id, manifest_id),
                ).fetchall()
                if not rows or any(row[3] for row in rows):
                    continue
                keys = [str(row[0]) for row in rows]
                connection.execute(
                    "UPDATE high_scale_source_objects SET owner=%s, "
                    "lease_generation=lease_generation+1, "
                    "lease_until=clock_timestamp() + make_interval(secs => %s) "
                    "WHERE service_id=%s AND object_key = ANY(%s)",
                    (worker_id, lease_seconds, service_id, keys),
                )
                tokens = tuple(
                    SourceClaimToken(str(row[0]), worker_id, int(row[1] or expected_owner_epoch), int(row[2]) + 1)
                    for row in rows
                )
                claims.append(
                    ArchivedPublicationClaim(
                        str(manifest_id),
                        tuple(keys),
                        max(int(row[1] or expected_owner_epoch) for row in rows),
                        tokens,
                    )
                )
        return tuple(claims)

    def release_archived_publication_retry(
        self,
        service_id: str,
        manifest_id: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        error: str,
    ) -> None:
        """Drop a retry lease and keep the sources bound to their archive manifest for the next attempt."""
        self._end_archived_publication_retry(
            service_id,
            manifest_id,
            worker_id,
            expected_owner_epoch=expected_owner_epoch,
            error=error,
            assignments="",
        )

    def unbind_archived_sources(
        self,
        service_id: str,
        manifest_id: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        error: str,
    ) -> None:
        """Return bound sources to raw ingest when their archive manifest cannot be republished."""
        self._end_archived_publication_retry(
            service_id,
            manifest_id,
            worker_id,
            expected_owner_epoch=expected_owner_epoch,
            error=error,
            assignments="status='discovered', archive_manifest_id=NULL, ",
        )

    def _end_archived_publication_retry(
        self,
        service_id: str,
        manifest_id: str,
        worker_id: str,
        *,
        expected_owner_epoch: int,
        error: str,
        assignments: str,
    ) -> None:
        _require_texts(service_id=service_id, manifest_id=manifest_id, worker_id=worker_id, error=error)
        if expected_owner_epoch <= 0:
            raise ValueError("owner epoch must be positive")
        with self.transaction() as connection:
            self._fenced_owner(connection, service_id, expected_owner_epoch=expected_owner_epoch)
            connection.execute(
                f"UPDATE high_scale_source_objects SET {assignments}owner=NULL, lease_until=NULL, last_error=%s, "
                "updated_at=clock_timestamp() "
                "WHERE service_id=%s AND archive_manifest_id=%s AND status='archived' AND owner=%s",
                (error, service_id, manifest_id, worker_id),
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
        self, service_id: str, object_key: str, *, manifest_id: str, expected_owner_epoch: int
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


def _build_committed_manifest_index(connection: Any) -> str:
    row = connection.execute(
        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(%s)", (COMMITTED_MANIFEST_INDEX,)
    ).fetchone()
    if row is not None and row[0]:
        return "present"
    if row is not None:
        logger.warning("dropping invalid %s left by an interrupted concurrent build", COMMITTED_MANIFEST_INDEX)
        connection.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {COMMITTED_MANIFEST_INDEX}")
    connection.execute(f"SET lock_timeout = '{_INDEX_BUILD_LOCK_TIMEOUT}'")
    try:
        connection.execute(_COMMITTED_MANIFEST_INDEX_DDL)
    finally:
        connection.execute("RESET lock_timeout")
    return "built" if row is None else "rebuilt"


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
        _parse_iso_or_dt(_row_value_optional(row, "discovered_at", 16)),
    )


def _parse_iso_or_dt(val: Any, fallback: datetime | None = None) -> Any:
    if val is None:
        return fallback
    return datetime.fromisoformat(val) if isinstance(val, str) else val


def _archive_from_row(row: Mapping[str, Any] | Any, source_rows: Sequence[Any] | None = None) -> ArchiveManifestRecord:
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


from backend.high_scale.postgres_control.deletion import DeletionAuthorization  # noqa: E402, F401
