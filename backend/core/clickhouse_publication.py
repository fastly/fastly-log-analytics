"""Fenced, generation-scoped publication of verified immutable payloads.

No ingest hook: only explicit bounded snapshot exports can produce membership.
Retries preserve ordinals and payload; FINAL makes duplicates invisible before
background merges. Partial generations are never selected.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError

from backend.core.clickhouse_client import (
    CLICKHOUSE_FACT_COLUMNS,
    ClickHouseClient,
    get_clickhouse_client,
    record_publication,
)
from backend.core.duckdb import _get_fos_client
from backend.core.metadata import pg_connection
from backend.core.metadata.pg_schema import CLICKHOUSE_CONTROL_DDL
from backend.utils.date_utils import ensure_utc, parse_iso_utc

__all__ = [
    "ARTIFACT_SCHEMA",
    "CLICKHOUSE_FACT_DDL",
    "CLICKHOUSE_SCHEMA_VERSION",
    "DurableBatch",
    "FACT_TYPES",
    "FosArtifacts",
    "MAX_ARTIFACT_BYTES",
    "MAX_BATCH_ROWS",
    "MAX_DATASET_ROWS",
    "MAX_ROW_BYTES",
    "PAYLOAD_COLUMNS",
    "PublicationResult",
    "ReplayResult",
    "_record_schema",
    "activate_generation",
    "build_batch_id",
    "canonical_bytes",
    "canonical_row",
    "create_clickhouse_schema",
    "digest_rows",
    "full_rebuild",
    "main",
    "maintain_clickhouse_system_tables",
    "publish_batch",
    "replay_unpublished",
    "require_postgres",
    "target_identity",
    "utc",
    "verify_generation",
]
logger = structlog.get_logger(__name__)

CLICKHOUSE_SCHEMA_VERSION = 2
FACT_TYPES = (
    ("service_id", "String"),
    ("batch_id", "String"),
    ("source_identity", "String"),
    ("row_ordinal", "UInt64"),
    ("generation", "String"),
    ("timestamp", "DateTime64(6, 'UTC')"),
    ("country", "Nullable(String)"),
    ("ip", "Nullable(String)"),
    ("url", "Nullable(String)"),
    ("conn_requests", "Nullable(Int64)"),
)
CLICKHOUSE_FACT_DDL = """
CREATE TABLE IF NOT EXISTS log_facts (
    service_id String,
    batch_id String,
    source_identity String,
    row_ordinal UInt64,
    generation String,
    timestamp DateTime64(6, 'UTC'),
    country Nullable(String),
    ip Nullable(String),
    url Nullable(String),
    conn_requests Nullable(Int64)
) ENGINE = ReplacingMergeTree
PARTITION BY (service_id, generation)
ORDER BY (service_id, generation, batch_id, row_ordinal)
"""


def require_postgres() -> None:
    if not pg_connection.is_postgres():
        raise RuntimeError("ClickHouse prototype requires Postgres METADATA_DSN; SQLite is unsupported")


def target_identity(client: ClickHouseClient) -> str:
    """Proof of the actual table, not a host name (a replacement volume differs)."""
    tables = client.execute(
        "SELECT toString(uuid) AS uuid, engine, sorting_key, partition_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'log_facts'"
    )
    if (
        len(tables) != 1
        or tables[0].get("engine") != "ReplacingMergeTree"
        or tables[0].get("sorting_key") != "service_id, generation, batch_id, row_ordinal"
        or tables[0].get("partition_key") != "(service_id, generation)"
        or tables[0].get("uuid") in (None, "", "00000000-0000-0000-0000-000000000000")
    ):
        raise RuntimeError("ClickHouse fact schema mismatch or missing table identity")
    columns = client.execute(
        "SELECT name, type FROM system.columns "
        "WHERE database = currentDatabase() AND table = 'log_facts' ORDER BY position"
    )
    if [(r["name"], r["type"]) for r in columns] != list(FACT_TYPES):
        raise RuntimeError("ClickHouse fact schema columns mismatch")
    return tables[0]["uuid"]


def _record_schema(identity: str) -> None:
    require_postgres()
    # The SQLite-shaped wrapper's commit() is a no-op. Use a real psycopg
    # transaction even though the underlying pool connections are autocommit.
    with pg_connection.get_pg_pool().connection() as con, con.transaction():
        con.execute("SELECT pg_advisory_xact_lock(2080406)")
        for ddl in CLICKHOUSE_CONTROL_DDL:
            con.execute(ddl)
        con.execute(
            "INSERT INTO clickhouse_schema_state(target_identity, schema_version) VALUES (%s, %s) "
            "ON CONFLICT (target_identity) DO UPDATE SET "
            "schema_version = EXCLUDED.schema_version, checked_at = clock_timestamp()",
            (identity, CLICKHOUSE_SCHEMA_VERSION),
        )


def maintain_clickhouse_system_tables(client: ClickHouseClient) -> None:
    """Ensure internal system tables do not accumulate unbounded parts and trigger merge OOMs."""
    log = logging.getLogger(__name__)
    for profiler_tbl in ("trace_log", "processors_profile_log"):
        try:
            client.execute(f"TRUNCATE TABLE IF EXISTS system.{profiler_tbl}")
        except Exception as e:
            log.debug("Failed to truncate system.%s: %s", profiler_tbl, e)

    for tbl in ("metric_log", "asynchronous_metric_log", "text_log", "query_log", "part_log", "error_log", "trace_log"):
        try:
            client.execute(f"ALTER TABLE system.{tbl} MODIFY TTL event_date + INTERVAL 1 DAY")
        except Exception as e:
            log.debug("Failed to alter TTL on system.%s: %s", tbl, e)


def create_clickhouse_schema(client: ClickHouseClient) -> None:
    require_postgres()
    maintain_clickhouse_system_tables(client)
    client.execute(CLICKHOUSE_FACT_DDL)
    _record_schema(target_identity(client))


def main() -> None:
    client = get_clickhouse_client()
    if client is None:
        raise RuntimeError("ClickHouse prototype is disabled")
    try:
        create_clickhouse_schema(client)

        try:
            from backend.high_scale.publication import install_clickhouse_schema

            install_clickhouse_schema()
        except ImportError:
            pass

        logging.getLogger(__name__).info("ClickHouse schema v%s verified", CLICKHOUSE_SCHEMA_VERSION)
    finally:
        client.close()
        if pg_connection.is_postgres():
            pg_connection.get_pg_pool().close()


sys.modules["backend.core.clickhouse_schema"] = sys.modules[__name__]
if "backend.core" in sys.modules:
    setattr(sys.modules["backend.core"], "clickhouse_schema", sys.modules[__name__])

try:
    from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest  # noqa: E402
except ImportError:
    pass


def _ensure_manifest_symbols() -> None:
    if "PgManifest" not in globals():
        from backend.core.clickhouse_manifest import Artifact as _A
        from backend.core.clickhouse_manifest import Dataset as _D
        from backend.core.clickhouse_manifest import PgManifest as _M

        globals()["Artifact"] = _A
        globals()["Dataset"] = _D
        globals()["PgManifest"] = _M


MAX_DATASET_ROWS = 1_000_000
MAX_BATCH_ROWS = 10_000
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_ROW_BYTES = 64 * 1024
PAYLOAD_COLUMNS = ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests")
ARTIFACT_SCHEMA = pa.schema(
    [
        ("source_identity", pa.string()),
        ("row_ordinal", pa.uint64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("country", pa.string()),
        ("ip", pa.string()),
        ("url", pa.string()),
        ("conn_requests", pa.int64()),
    ]
)


def utc(value: str | datetime) -> datetime:
    if isinstance(value, str):
        parsed = parse_iso_utc(value)
        if parsed is None:
            raise ValueError(f"Invalid ISO format string: {value!r}")
        return parsed
    return ensure_utc(value)


def canonical_row(row: dict[str, Any]) -> tuple:
    ordinal = row["row_ordinal"]
    if type(ordinal) is not int or not 0 <= ordinal < MAX_DATASET_ROWS:
        raise ValueError("invalid row ordinal")
    timestamp = utc(row["timestamp"])
    if not datetime(1970, 1, 1, tzinfo=UTC) <= timestamp < datetime(2262, 1, 1, tzinfo=UTC):
        raise ValueError("timestamp outside prototype range")
    values = [row["source_identity"], ordinal, timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")]
    if not isinstance(values[0], str):
        raise ValueError("invalid source identity")
    for name in ("country", "ip", "url"):
        value = row[name]
        if value is not None and not isinstance(value, str):
            raise ValueError("invalid nullable dimension")
        values.append(value)
    conn = row["conn_requests"]
    if conn is not None and (type(conn) is not int or not -(2**63) <= conn < 2**63):
        raise ValueError("invalid conn_requests")
    result = (*values, conn)
    if len(canonical_bytes(result)) > MAX_ROW_BYTES:
        raise ValueError("oversized row")
    return result


def canonical_bytes(row: tuple) -> bytes:
    return (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest_rows(rows) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_bytes(row))
    return digest.hexdigest()


def build_batch_id(service_id: str, source_identity: str, content_hash: str) -> str:
    return hashlib.sha256(canonical_bytes((service_id, source_identity, content_hash))).hexdigest()


class FosArtifacts:
    """Bucket/prefix confinement and bounded hash-verified I/O, through existing telemetry."""

    def __init__(self, source: dict, *, client=None):
        self.service_id = source["service_id"]
        self.bucket = source["bucket"]
        if not self.bucket:
            raise ValueError("configured Fastly Object Storage bucket required")
        root = (source.get("prefix") or "").strip("/")
        tenant = hashlib.sha256(self.service_id.encode()).hexdigest()
        self.prefix = f"{root + '/' if root else ''}clickhouse-prototype/{tenant}/"
        self.client = client if client is not None else _get_fos_client(source)

    def _key(self, artifact: Artifact) -> str:
        uri = urlsplit(artifact.artifact_uri)
        if uri.scheme != "s3" or uri.netloc != self.bucket or uri.query or uri.fragment:
            raise ValueError("artifact bucket must match configured Fastly Object Storage bucket")
        key = uri.path.lstrip("/")
        expected = f"{self.prefix}{artifact.dataset_id}/{artifact.content_hash}.parquet"
        if key != expected or artifact.service_id != self.service_id:
            raise ValueError("artifact outside service prototype prefix")
        return key

    def put(self, dataset: Dataset, rows: tuple, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> Artifact:
        _ensure_manifest_symbols()
        table_rows = [dict(zip(PAYLOAD_COLUMNS, (*row[:2], utc(row[2]), *row[3:]), strict=True)) for row in rows]
        table = pa.Table.from_pylist(table_rows, schema=ARTIFACT_SCHEMA)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd", row_group_size=MAX_BATCH_ROWS, version="2.6")
        payload = sink.getvalue().to_pybytes()
        if len(payload) > max_bytes:
            raise ValueError("artifact exceeds byte limit")
        content_hash = hashlib.sha256(payload).hexdigest()
        artifact = Artifact(
            dataset.service_id,
            dataset.dataset_id,
            build_batch_id(dataset.service_id, dataset.dataset_id, content_hash),
            f"s3://{self.bucket}/{self.prefix}{dataset.dataset_id}/{content_hash}.parquet",
            content_hash,
            digest_rows(rows),
            len(payload),
            rows[0][1],
            len(rows),
        )
        key = self._key(artifact)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                IfNoneMatch="*",
                ContentType="application/vnd.apache.parquet",
                Metadata={"sha256": content_hash, "expires-at": dataset.expires_at.isoformat()},
            )
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise RuntimeError("Fastly Object Storage artifact PUT failed") from None
            # A content-addressed retry may encounter an already-present object.
            # It is success only if its bytes and metadata are exactly correct.
            self.load(artifact)
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        if head["ContentLength"] != artifact.byte_size or head.get("Metadata", {}).get("sha256") != content_hash:
            raise ValueError("artifact HEAD verification failed")
        logger.info("clickhouse.artifact", operation="put", rows=len(rows), bytes=artifact.byte_size)
        return artifact

    def load(self, artifact: Artifact) -> DurableBatch:
        if not 1 <= artifact.byte_size <= MAX_ARTIFACT_BYTES or not 1 <= artifact.row_count <= MAX_BATCH_ROWS:
            raise ValueError("artifact exceeds bounds")
        key = self._key(artifact)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        try:
            if response["ContentLength"] != artifact.byte_size:
                raise ValueError("artifact size mismatch")
            payload = body.read(MAX_ARTIFACT_BYTES + 1)
        finally:
            body.close()
        if len(payload) != artifact.byte_size or hashlib.sha256(payload).hexdigest() != artifact.content_hash:
            raise ValueError("artifact hash mismatch")
        parquet = pq.ParquetFile(pa.BufferReader(payload))
        if (
            parquet.metadata.num_rows != artifact.row_count
            or parquet.schema_arrow != ARTIFACT_SCHEMA
            or sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
            > MAX_ARTIFACT_BYTES
        ):
            raise ValueError("artifact decoded schema or size mismatch")
        rows = tuple(canonical_row(row) for batch in parquet.iter_batches(batch_size=1000) for row in batch.to_pylist())
        if digest_rows(rows) != artifact.canonical_digest:
            raise ValueError("artifact canonical hash mismatch")
        logger.info("clickhouse.artifact", operation="get", rows=len(rows), bytes=artifact.byte_size)
        return DurableBatch(artifact, rows)


@dataclass(frozen=True)
class DurableBatch:
    artifact: Artifact
    rows: tuple[tuple, ...]


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    status: str
    row_count: int = 0
    fence: int | None = None


@dataclass(frozen=True)
class ReplayResult:
    generation: str
    attempted: int
    published: int
    activated: bool


def _client(client: ClickHouseClient | None) -> ClickHouseClient:
    result = client if client is not None else get_clickhouse_client()
    if result is None:
        raise RuntimeError("ClickHouse prototype disabled")
    return result


def _target_rows(client: ClickHouseClient, service_id: str, generation: str, artifact: Artifact) -> tuple:
    rows = client.execute(
        "SELECT source_identity,row_ordinal,timestamp,country,ip,url,conn_requests FROM log_facts FINAL "
        "WHERE service_id={service:String} AND generation={generation:String} AND batch_id={batch:String} "
        "ORDER BY row_ordinal LIMIT 10001",
        {"service": service_id, "generation": generation, "batch": artifact.batch_id},
    )
    return tuple(canonical_row(row) for row in rows)


def _validate_payload(artifact: Artifact, rows: tuple) -> None:
    if len(rows) != artifact.row_count or len(rows) > MAX_BATCH_ROWS:
        raise ValueError("artifact payload row count mismatch")
    for index, row in enumerate(rows):
        if (
            canonical_row(dict(zip(PAYLOAD_COLUMNS, row, strict=True))) != row
            or row[1] != artifact.ordinal_start + index
        ):
            raise ValueError("artifact payload ordinals or types mismatch")
    if digest_rows(rows) != artifact.canonical_digest:
        raise ValueError("artifact payload digest mismatch")


def publish_batch(
    service_id: str,
    batch: DurableBatch,
    *,
    generation: str,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> PublicationResult:
    started = time.monotonic()
    outcome = "failed"
    error_kind = None
    try:
        result = _publish_batch(service_id, batch, generation=generation, store=store, client=client)
        outcome = result.status
        return result
    except Exception as exc:
        error_kind = type(exc).__name__
        raise
    finally:
        # The outer boundary includes config, manifest, payload and claim
        # failures. Observers must never replace an original result/error.
        try:
            record_publication(outcome)
        except Exception:
            logger.debug("publish_batch: record_publication failed (ignored)", exc_info=True)
        try:
            log = logger.error if outcome == "failed" else logger.info
            log(
                "clickhouse.publication",
                outcome=outcome,
                error_kind=error_kind,
                batch_id=batch.artifact.batch_id,
                rows=len(batch.rows),
                duration_ms=(time.monotonic() - started) * 1000,
            )
        except Exception:
            logger.debug("publish_batch: log failed (ignored)", exc_info=True)


def _publish_batch(
    service_id: str,
    batch: DurableBatch,
    *,
    generation: str,
    store: PgManifest | None,
    client: ClickHouseClient | None,
) -> PublicationResult:
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _client(client)
    artifact = store.artifact(service_id, generation, batch.artifact.batch_id)
    if artifact != batch.artifact:
        raise ValueError("artifact differs from immutable manifest")
    _validate_payload(artifact, batch.rows)
    gen = store.generation(service_id, generation)
    if target_identity(client) != gen["target_identity"]:
        raise ValueError("target changed; full rebuild required")
    fence = store.claim(service_id, generation, artifact.batch_id)
    if fence is None:
        return PublicationResult(artifact.batch_id, "not_claimed")
    try:
        # Each chunk is at most ~4 MiB of canonical payload; rows are already
        # artifact-validated, so a delayed worker cannot insert different values.
        chunk: list[tuple] = []
        size = 0
        for row in batch.rows:
            if size >= 4 * 1024 * 1024 or len(chunk) >= 1000:
                client.insert_rows("log_facts", list(CLICKHOUSE_FACT_COLUMNS), chunk)
                chunk, size = [], 0
            chunk.append((service_id, artifact.batch_id, row[0], row[1], generation, *row[2:]))
            size += len(canonical_bytes(row))
        client.insert_rows("log_facts", list(CLICKHOUSE_FACT_COLUMNS), chunk)
        actual = _target_rows(client, service_id, generation, artifact)
        if actual != batch.rows:
            raise ValueError("ordered target payload mismatch")
        if target_identity(client) != gen["target_identity"]:
            raise ValueError("target changed during publication")
        published = store.finish(service_id, generation, artifact.batch_id, fence, digest=artifact.canonical_digest)
    except Exception as exc:
        kind = type(exc).__name__
        try:
            store.finish(service_id, generation, artifact.batch_id, fence, digest=None, error=kind)
        except Exception:
            logger.warning("_publish_batch: store.finish failed (ignored)", exc_info=True)
        raise RuntimeError(f"ClickHouse publication failed: {kind}") from None
    return PublicationResult(artifact.batch_id, "published" if published else "stale", len(batch.rows), fence)


def verify_generation(service_id: str, generation: str, *, store: PgManifest, client: ClickHouseClient) -> Dataset:
    gen = store.generation(service_id, generation)
    dataset = store.dataset(service_id, gen["dataset_id"])
    if target_identity(client) != gen["target_identity"]:
        raise ValueError("target changed; full rebuild required")
    digest = hashlib.sha256()
    count = 0
    for artifact in store.artifacts(service_id, dataset.dataset_id):
        if artifact.ordinal_start != count:
            raise ValueError("manifest coverage mismatch")
        rows = _target_rows(client, service_id, generation, artifact)
        _validate_payload(artifact, rows)
        for row in rows:
            digest.update(canonical_bytes(row))
        count += len(rows)
    actual_count = client.execute(
        "SELECT count() AS n FROM log_facts FINAL WHERE service_id={service:String} AND generation={generation:String}",
        {"service": service_id, "generation": generation},
    )[0]["n"]
    if actual_count != count or count != dataset.expected_rows or digest.hexdigest() != dataset.canonical_digest:
        raise ValueError("generation coverage or digest mismatch")
    return dataset


def activate_generation(service_id: str, generation: str, *, store: PgManifest, client: ClickHouseClient) -> bool:
    verify_generation(service_id, generation, store=store, client=client)
    return store.activate(service_id, generation, target_identity(client))


def replay_unpublished(
    service_id: str,
    *,
    generation: str,
    loader: Callable[[Artifact], DurableBatch],
    limit: int = 100,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> ReplayResult:
    """Resume this building generation only. Rebuild uses full_rebuild instead."""
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _client(client)
    gen = store.generation(service_id, generation)
    store.dataset(service_id, gen["dataset_id"])
    batches = store.pending(service_id, generation, limit)
    published = 0
    for batch_id in batches:
        result = publish_batch(
            service_id,
            loader(store.artifact(service_id, generation, batch_id)),
            generation=generation,
            store=store,
            client=client,
        )
        published += result.status == "published"
    activated = False
    if not store.pending(service_id, generation, 1):
        activated = activate_generation(service_id, generation, store=store, client=client)
    return ReplayResult(generation, len(batches), published, activated)


def full_rebuild(
    service_id: str,
    dataset_id: str,
    *,
    loader: Callable[[Artifact], DurableBatch],
    limit: int = 100,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> ReplayResult:
    """Always allocate a new target generation, including previously published rows."""
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be 1..1000")
    _ensure_manifest_symbols()
    store = store or PgManifest()
    client = _client(client)
    generation = store.begin_generation(service_id, dataset_id, target_identity(client))
    return replay_unpublished(service_id, generation=generation, loader=loader, limit=limit, store=store, client=client)


def __getattr__(name: str):
    if name == "readiness":
        return None
    if name in ("Artifact", "Dataset", "PgManifest"):
        _ensure_manifest_symbols()
        return globals()[name]
    raise AttributeError(name)


_this_module = sys.modules[__name__]
sys.modules["backend.core.clickhouse_schema"] = _this_module
_core_pkg = sys.modules.get("backend.core")
if _core_pkg is not None:
    setattr(_core_pkg, "clickhouse_schema", _this_module)


if __name__ == "__main__":
    main()
