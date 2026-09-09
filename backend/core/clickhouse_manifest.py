"""Transactional Postgres manifest; never use the SQLite-shaped commit shim."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import structlog
from psycopg import Connection
from psycopg_pool import ConnectionPool

from backend.core.clickhouse_rows import MAX_ARTIFACT_BYTES, MAX_BATCH_ROWS, MAX_DATASET_ROWS, build_batch_id
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION, require_postgres
from backend.core.metadata.pg_connection import get_pg_pool

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Dataset:
    service_id: str
    dataset_id: str
    catalog_identity: str
    source_table: str
    source_snapshot: int
    coverage_start: datetime
    coverage_end: datetime
    expires_at: datetime
    expected_rows: int
    canonical_digest: str
    schema_version: int = CLICKHOUSE_SCHEMA_VERSION


@dataclass(frozen=True)
class Artifact:
    service_id: str
    dataset_id: str
    batch_id: str
    artifact_uri: str
    content_hash: str
    canonical_digest: str
    byte_size: int
    ordinal_start: int
    row_count: int


def _construct(cls, row):
    return cls(**{name: row[name] for name in cls.__dataclass_fields__})


class PgManifest:
    def __init__(self, *, bounded: bool = False):
        require_postgres()
        self.pool = cast(ConnectionPool[Connection[dict[str, Any]]], get_pg_pool())
        self.bounded = bounded

    @contextmanager
    def transaction(self):
        started = time.monotonic()
        outcome = "error"
        try:
            connection = self.pool.connection(timeout=2) if self.bounded else self.pool.connection()
            with connection as con, con.transaction():
                if self.bounded:
                    con.execute("SET LOCAL statement_timeout = '2s'")
                    con.execute("SET LOCAL lock_timeout = '2s'")
                yield con
            outcome = "success"
        finally:
            logger.info("clickhouse.manifest", outcome=outcome, duration_ms=(time.monotonic() - started) * 1000)

    def admin_status(self, service_id: str, target: str) -> dict:
        """Service-scoped metadata only; counts include expired/retired history.

        Pending age is actionable backlog only. An expired selected dataset
        remains visible, but is never described as ready to serve.
        """
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET TRANSACTION READ ONLY")
            con.execute("SET LOCAL statement_timeout = '2s'")
            schema = con.execute(
                "SELECT schema_version FROM clickhouse_schema_state WHERE target_identity=%s", (target,)
            ).fetchone()
            counts = con.execute(
                "SELECT status,count(*) AS n FROM clickhouse_publications WHERE service_id=%s GROUP BY status",
                (service_id,),
            ).fetchall()
            freshness = con.execute(
                "SELECT MAX(p.published_at) AS last_published_at,"
                "MAX(EXTRACT(EPOCH FROM (clock_timestamp()-p.created_at))) FILTER "
                "(WHERE p.status IN ('pending','claimed','failed') AND g.status='building' "
                "AND d.expires_at>clock_timestamp() AND s.revision=g.selection_revision) AS pending_age "
                "FROM clickhouse_publications p JOIN clickhouse_generations g USING(service_id,generation,dataset_id) "
                "JOIN clickhouse_datasets d USING(service_id,dataset_id) "
                "LEFT JOIN clickhouse_service_selection s ON s.service_id=p.service_id WHERE p.service_id=%s",
                (service_id,),
            ).fetchone()
            active = con.execute(
                "SELECT g.generation,g.dataset_id,g.target_identity,d.coverage_start,d.coverage_end,d.expires_at,"
                "d.expires_at<=clock_timestamp() AS expired,"
                "GREATEST(0,EXTRACT(EPOCH FROM (clock_timestamp()-d.coverage_end))) AS coverage_age_seconds "
                "FROM clickhouse_service_selection s JOIN clickhouse_generations g USING(service_id,generation) "
                "JOIN clickhouse_datasets d USING(service_id,dataset_id) WHERE s.service_id=%s AND g.status='active'",
                (service_id,),
            ).fetchone()
            expired = con.execute(
                "SELECT count(*) AS n FROM clickhouse_datasets WHERE service_id=%s AND expires_at<=clock_timestamp()",
                (service_id,),
            ).fetchone()
        assert freshness is not None and expired is not None
        active_response = None
        if active:
            active_response = {
                "generation": active["generation"],
                "dataset_id": active["dataset_id"],
                "coverage_start": active["coverage_start"].isoformat(),
                "coverage_end": active["coverage_end"].isoformat(),
                "expires_at": active["expires_at"].isoformat(),
                "expired": active["expired"],
                "target_matches": active["target_identity"] == target,
                "coverage_age_seconds": float(active["coverage_age_seconds"]),
            }
        return {
            "schema_version": schema["schema_version"] if schema else None,
            "publication_counts": {
                **dict.fromkeys(("pending", "claimed", "failed", "published"), 0),
                **{row["status"]: row["n"] for row in counts},
            },
            "oldest_pending_age_seconds": (
                max(0.0, float(freshness["pending_age"])) if freshness["pending_age"] is not None else None
            ),
            "last_published_at": (
                freshness["last_published_at"].isoformat() if freshness["last_published_at"] else None
            ),
            "active_generation": active_response,
            "expired_dataset_count": expired["n"],
        }

    def replay_preview(
        self, service_id: str, *, dataset_id: str | None, generation: str | None, target: str, limit: int
    ) -> dict:
        """Read-only reference validation, also bounding final activation work.

        Activation verifies the entire dataset, not just this request's pending
        slice. Refuse datasets above the HTTP cap before allocating a generation.
        """
        if not 1 <= limit <= 100:
            raise ValueError("HTTP replay limit exceeded")
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET TRANSACTION READ ONLY")
            con.execute("SET LOCAL statement_timeout = '2s'")
            if generation:
                gen = con.execute(
                    "SELECT g.dataset_id,g.status,g.target_identity,"
                    "g.selection_revision=s.revision AS current_revision "
                    "FROM clickhouse_generations g JOIN clickhouse_service_selection s USING(service_id) "
                    "WHERE g.service_id=%s AND g.generation=%s",
                    (service_id, generation),
                ).fetchone()
                if gen is None:
                    raise LookupError("reference not found")
                if gen["status"] != "building" or not gen["current_revision"] or gen["target_identity"] != target:
                    raise ValueError("generation cannot resume")
                dataset_id = gen["dataset_id"]
            dataset = con.execute(
                "SELECT expires_at,schema_version FROM clickhouse_datasets WHERE service_id=%s AND dataset_id=%s",
                (service_id, dataset_id),
            ).fetchone()
            if dataset is None:
                raise LookupError("reference not found")
            if dataset["expires_at"] <= datetime.now(UTC) or dataset["schema_version"] != CLICKHOUSE_SCHEMA_VERSION:
                raise ValueError("dataset expired or unsupported")
            count_row = con.execute(
                "SELECT count(*) AS n FROM "
                "(SELECT 1 FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s LIMIT 101) a",
                (service_id, dataset_id),
            ).fetchone()
            assert count_row is not None
            count = count_row["n"]
            if count > 100:
                raise ValueError("dataset exceeds HTTP replay cap")
            pending = count
            if generation:
                pending_row = con.execute(
                    "SELECT count(*) AS n FROM clickhouse_publications "
                    "WHERE service_id=%s AND generation=%s AND status!='published'",
                    (service_id, generation),
                ).fetchone()
                assert pending_row is not None
                pending = pending_row["n"]
        return {"dataset_id": dataset_id, "artifact_count": count, "planned_artifacts": min(pending, limit)}

    def publication_lag_seconds(self) -> float:
        """Oldest actionable backlog age, not the age of retired/expired work.

        NULL after a successful aggregate means no backlog. Database errors
        propagate so observers omit unknown lag instead of reporting zero.
        """
        with self.pool.connection(timeout=2) as con, con.transaction():
            con.execute("SET LOCAL statement_timeout = '2s'")
            row = con.execute(
                "SELECT MAX(EXTRACT(EPOCH FROM (clock_timestamp()-p.created_at))) AS lag_seconds "
                "FROM clickhouse_publications p "
                "JOIN clickhouse_generations g ON g.service_id=p.service_id AND g.generation=p.generation "
                "JOIN clickhouse_datasets d ON d.service_id=g.service_id AND d.dataset_id=g.dataset_id "
                "JOIN clickhouse_service_selection s ON s.service_id=g.service_id "
                "AND s.revision=g.selection_revision "
                "WHERE p.status IN ('pending', 'claimed', 'failed') AND g.status='building' "
                "AND d.expires_at>clock_timestamp()"
            ).fetchone()
        assert row is not None
        return max(0.0, float(row["lag_seconds"])) if row["lag_seconds"] is not None else 0.0

    def seal(self, dataset: Dataset, artifacts: list[Artifact]) -> None:
        """Insert complete immutable membership atomically; failed exports never appear."""
        now = datetime.now(UTC)
        if (
            not 0 <= dataset.expected_rows <= MAX_DATASET_ROWS
            or not dataset.coverage_start <= dataset.coverage_end
            or not now < dataset.expires_at <= now + timedelta(days=1)
            or dataset.schema_version != CLICKHOUSE_SCHEMA_VERSION
        ):
            raise ValueError("invalid bounded dataset")
        end = 0
        for artifact in artifacts:
            if (
                artifact.service_id != dataset.service_id
                or artifact.dataset_id != dataset.dataset_id
                or artifact.ordinal_start != end
                or not 1 <= artifact.row_count <= MAX_BATCH_ROWS
                or not 1 <= artifact.byte_size <= MAX_ARTIFACT_BYTES
                or artifact.batch_id != build_batch_id(dataset.service_id, dataset.dataset_id, artifact.content_hash)
            ):
                raise ValueError("artifact membership is not exact")
            end += artifact.row_count
        if end != dataset.expected_rows:
            raise ValueError("artifact coverage is not exact")
        with self.transaction() as con:
            con.execute(
                "INSERT INTO clickhouse_datasets(service_id,dataset_id,catalog_identity,source_table,"
                "source_snapshot,coverage_start,coverage_end,expires_at,expected_rows,canonical_digest,schema_version)"
                " VALUES (%(service_id)s,%(dataset_id)s,%(catalog_identity)s,%(source_table)s,%(source_snapshot)s,"
                "%(coverage_start)s,%(coverage_end)s,%(expires_at)s,%(expected_rows)s,%(canonical_digest)s,%(schema_version)s)",
                asdict(dataset),
            )
            for artifact in artifacts:
                con.execute(
                    "INSERT INTO clickhouse_artifacts(service_id,dataset_id,batch_id,artifact_uri,content_hash,"
                    "canonical_digest,byte_size,ordinal_start,row_count) VALUES (%(service_id)s,%(dataset_id)s,"
                    "%(batch_id)s,%(artifact_uri)s,%(content_hash)s,%(canonical_digest)s,%(byte_size)s,"
                    "%(ordinal_start)s,%(row_count)s)",
                    asdict(artifact),
                )

    def dataset(self, service_id: str, dataset_id: str) -> Dataset:
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_datasets WHERE service_id=%s AND dataset_id=%s",
                (service_id, dataset_id),
            ).fetchone()
        if row is None:
            raise ValueError("dataset not found")
        dataset = _construct(Dataset, row)
        if dataset.expires_at <= datetime.now(UTC):
            raise ValueError("dataset expired")
        if dataset.schema_version != CLICKHOUSE_SCHEMA_VERSION:
            raise ValueError("dataset schema unsupported")
        return dataset

    def artifacts(self, service_id: str, dataset_id: str) -> list[Artifact]:
        with self.transaction() as con:
            rows = con.execute(
                "SELECT * FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s ORDER BY ordinal_start",
                (service_id, dataset_id),
            ).fetchall()
        return [_construct(Artifact, row) for row in rows]

    def artifact(self, service_id: str, generation: str, batch_id: str) -> Artifact:
        gen = self.generation(service_id, generation)
        self.dataset(service_id, gen["dataset_id"])
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s AND batch_id=%s",
                (service_id, gen["dataset_id"], batch_id),
            ).fetchone()
        if row is None:
            raise ValueError("artifact not in generation")
        return _construct(Artifact, row)

    def generation(self, service_id: str, generation: str) -> dict:
        with self.transaction() as con:
            row = con.execute(
                "SELECT * FROM clickhouse_generations WHERE service_id=%s AND generation=%s",
                (service_id, generation),
            ).fetchone()
        if row is None:
            raise ValueError("generation not found")
        return dict(row)

    def begin_generation(self, service_id: str, dataset_id: str, target: str) -> str:
        self.dataset(service_id, dataset_id)
        generation = uuid4().hex
        with self.transaction() as con:
            con.execute(
                "INSERT INTO clickhouse_service_selection(service_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (service_id,),
            )
            revision = con.execute(
                "SELECT revision FROM clickhouse_service_selection WHERE service_id=%s FOR UPDATE",
                (service_id,),
            ).fetchone()["revision"]
            con.execute(
                "INSERT INTO clickhouse_generations(service_id,generation,dataset_id,target_identity,selection_revision)"
                " VALUES (%s,%s,%s,%s,%s)",
                (service_id, generation, dataset_id, target, revision),
            )
            con.execute(
                "INSERT INTO clickhouse_publications(service_id,generation,dataset_id,batch_id) "
                "SELECT service_id,%s,dataset_id,batch_id FROM clickhouse_artifacts WHERE service_id=%s AND dataset_id=%s",
                (generation, service_id, dataset_id),
            )
        return generation

    def claim(self, service_id: str, generation: str, batch_id: str, *, lease_seconds: int = 120) -> int | None:
        if not 1 <= lease_seconds <= 300:
            raise ValueError("lease must be bounded")
        with self.transaction() as con:
            row = con.execute(
                "UPDATE clickhouse_publications p SET status='claimed', lease_fence=lease_fence+1,"
                "attempt_count=attempt_count+1, lease_until=clock_timestamp()+%s * interval '1 second',"
                "updated_at=clock_timestamp() FROM clickhouse_generations g, clickhouse_datasets d "
                "WHERE p.service_id=%s AND p.generation=%s AND p.batch_id=%s "
                "AND g.service_id=p.service_id AND g.generation=p.generation AND g.status='building' "
                "AND d.service_id=p.service_id AND d.dataset_id=p.dataset_id AND d.expires_at>clock_timestamp() "
                "AND p.status!='published' AND (p.status!='claimed' OR p.lease_until<=clock_timestamp()) "
                "RETURNING p.lease_fence",
                (lease_seconds, service_id, generation, batch_id),
            ).fetchone()
        return row["lease_fence"] if row else None

    def finish(
        self,
        service_id: str,
        generation: str,
        batch_id: str,
        fence: int,
        *,
        digest: str | None,
        error: str | None = None,
    ) -> bool:
        with self.transaction() as con:
            result = con.execute(
                "UPDATE clickhouse_publications p SET status=%s,verified_digest=%s,"
                "published_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,"
                "first_error=COALESCE(first_error,%s),last_error=%s,updated_at=clock_timestamp() "
                "FROM clickhouse_datasets d WHERE p.service_id=%s AND p.generation=%s AND p.batch_id=%s "
                "AND p.lease_fence=%s AND p.status='claimed' AND p.lease_until>clock_timestamp() "
                "AND d.service_id=p.service_id AND d.dataset_id=p.dataset_id AND d.expires_at>clock_timestamp()",
                (
                    "published" if digest else "failed",
                    digest,
                    bool(digest),
                    error,
                    error,
                    service_id,
                    generation,
                    batch_id,
                    fence,
                ),
            )
            return result.rowcount == 1

    def activate(self, service_id: str, generation: str, target: str) -> bool:
        """Caller verified the target; lock + revision CAS prevents late promotion."""
        with self.transaction() as con:
            selection = con.execute(
                "SELECT * FROM clickhouse_service_selection WHERE service_id=%s FOR UPDATE",
                (service_id,),
            ).fetchone()
            gen = con.execute(
                "SELECT g.*, d.expires_at FROM clickhouse_generations g JOIN clickhouse_datasets d "
                "USING(service_id,dataset_id) WHERE g.service_id=%s AND g.generation=%s FOR UPDATE OF g",
                (service_id, generation),
            ).fetchone()
            if (
                not selection
                or not gen
                or (
                    gen["status"] != "building"
                    or gen["target_identity"] != target
                    or gen["selection_revision"] != selection["revision"]
                    or gen["expires_at"] <= datetime.now(UTC)
                )
            ):
                return False
            missing = con.execute(
                "SELECT count(*) AS n FROM clickhouse_artifacts a LEFT JOIN clickhouse_publications p "
                "ON p.service_id=a.service_id AND p.dataset_id=a.dataset_id AND p.batch_id=a.batch_id "
                "AND p.generation=%s WHERE a.service_id=%s AND a.dataset_id=%s "
                "AND (p.status IS DISTINCT FROM 'published' OR p.verified_digest IS DISTINCT FROM a.canonical_digest)",
                (generation, service_id, gen["dataset_id"]),
            ).fetchone()["n"]
            if missing:
                return False
            con.execute(
                "UPDATE clickhouse_generations SET status='retired' WHERE service_id=%s AND status='active'",
                (service_id,),
            )
            con.execute(
                "UPDATE clickhouse_generations SET status='active',activated_at=clock_timestamp() "
                "WHERE service_id=%s AND generation=%s",
                (service_id, generation),
            )
            con.execute(
                "UPDATE clickhouse_service_selection SET generation=%s,revision=revision+1 WHERE service_id=%s",
                (generation, service_id),
            )
            return True

    def selected(self, service_id: str) -> dict | None:
        with self.transaction() as con:
            row = con.execute(
                "SELECT g.* FROM clickhouse_service_selection s JOIN clickhouse_generations g "
                "USING(service_id,generation) WHERE s.service_id=%s AND g.status='active'",
                (service_id,),
            ).fetchone()
        return dict(row) if row else None

    def pending(self, service_id: str, generation: str, limit: int) -> list[str]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        with self.transaction() as con:
            rows = con.execute(
                "SELECT p.batch_id FROM clickhouse_publications p JOIN clickhouse_artifacts a "
                "USING(service_id,dataset_id,batch_id) WHERE p.service_id=%s AND p.generation=%s "
                "AND p.status!='published' ORDER BY a.ordinal_start LIMIT %s",
                (service_id, generation, limit),
            ).fetchall()
        return [r["batch_id"] for r in rows]
