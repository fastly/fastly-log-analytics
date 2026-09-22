"""Explicit pinned-snapshot exports to immutable Fastly Object Storage Parquet.

No background jobs or ingestion hooks. A failed export may leave unreferenced
content-addressed objects; it cannot seal a partial Postgres manifest.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from botocore.exceptions import ClientError
from psycopg.conninfo import conninfo_to_dict

from backend import config
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_publication import DurableBatch
from backend.core.clickhouse_rows import (
    MAX_ARTIFACT_BYTES,
    MAX_BATCH_ROWS,
    MAX_DATASET_ROWS,
    PAYLOAD_COLUMNS,
    build_batch_id,
    canonical_bytes,
    canonical_row,
    digest_rows,
    utc,
)
from backend.core.duckdb import _configure_fos, _get_fos_client
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.repositories._base import _safe_table

logger = structlog.get_logger(__name__)
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


def expiry_for(start: datetime, *, retention_days: int, ttl_hours: float = 24, now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    if not 0 < ttl_hours <= 24:
        raise ValueError("TTL must be finite and at most 24 hours")
    if retention_days < 0:
        raise ValueError("invalid customer retention")
    expiry = now + timedelta(hours=ttl_hours)
    if retention_days:
        expiry = min(expiry, utc(start) + timedelta(days=retention_days))
    if expiry <= now:
        raise ValueError("coverage already outside customer retention")
    return expiry


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


def pinned_reader(
    con, source: dict, *, snapshot: int, start: datetime, end: datetime, max_rows: int = MAX_DATASET_ROWS
):
    """ONE ordered Arrow reader over AT VERSION, never mutable repeated paging."""
    if type(snapshot) is not int or snapshot < 0 or not 1 <= max_rows <= MAX_DATASET_ROWS:
        raise ValueError("invalid snapshot or row bound")
    con.execute("SET TimeZone='UTC'")
    table = ducklake_table_name(source)
    # _safe_table is a source-name mapper (adds logs_), not an identifier quote.
    if _safe_table(table) != f"logs_{table}":
        raise ValueError("invalid derived source table")
    reference = f"lake.{table} AT (VERSION => {snapshot})"
    columns = {col[0] for col in con.execute(f"SELECT * FROM {reference} LIMIT 0").description}
    required = {"timestamp", "country", "ip", "url", "conn_requests"}
    if not required <= columns:
        raise ValueError("source lacks required prototype columns")
    if "_source_file" in columns:
        source_expr = "COALESCE(CAST(_source_file AS VARCHAR), '')"
    elif "source_file" in columns:
        source_expr = "COALESCE(CAST(source_file AS VARCHAR), '')"
    else:
        source_expr = "''"
    return con.execute(
        f"SELECT {source_expr} AS source_identity, timestamp, country, ip, url, "
        f"CAST(conn_requests AS BIGINT) AS conn_requests FROM {reference} "
        "WHERE timestamp >= ? AND timestamp <= ? "
        "ORDER BY timestamp, country, ip, url, conn_requests, source_identity LIMIT ?",
        [start, end, max_rows + 1],
    ).fetch_record_batch(1000)


def export_reader(
    reader,
    *,
    dataset: Dataset,
    objects: FosArtifacts,
    store: PgManifest,
    max_rows: int = MAX_DATASET_ROWS,
    batch_rows: int = MAX_BATCH_ROWS,
    max_bytes: int = MAX_ARTIFACT_BYTES,
) -> Dataset:
    if (
        not 1 <= max_rows <= MAX_DATASET_ROWS
        or not 1 <= batch_rows <= MAX_BATCH_ROWS
        or not 4096 <= max_bytes <= MAX_ARTIFACT_BYTES
    ):
        raise ValueError("invalid export bounds")
    digest = hashlib.sha256()
    artifacts = []
    pending: list[tuple] = []
    pending_bytes = 0
    count = 0
    started = time.monotonic()
    for batch in reader:
        for row in batch.to_pylist():
            if count >= max_rows:
                raise ValueError("dataset exceeds row limit; narrow the coverage window")
            row["row_ordinal"] = count
            canonical = canonical_row(row)
            if not dataset.coverage_start <= utc(canonical[2]) <= dataset.coverage_end:
                raise ValueError("row outside dataset coverage")
            encoded = canonical_bytes(canonical)
            if len(encoded) > max_bytes // 2:
                raise ValueError("oversized row for artifact bound")
            if pending and (len(pending) >= batch_rows or pending_bytes + len(encoded) > max_bytes // 2):
                artifacts.append(objects.put(dataset, tuple(pending), max_bytes=max_bytes))
                pending, pending_bytes = [], 0
            pending.append(canonical)
            pending_bytes += len(encoded)
            digest.update(encoded)
            count += 1
    if pending:
        artifacts.append(objects.put(dataset, tuple(pending), max_bytes=max_bytes))
    result = replace(dataset, expected_rows=count, canonical_digest=digest.hexdigest())
    store.seal(result, artifacts)
    logger.info(
        "clickhouse.export", rows=count, artifacts=len(artifacts), duration_ms=(time.monotonic() - started) * 1000
    )
    return result


def source_catalog_identity() -> str:
    """Credential-free location + database/catalog-object identities, hashed.

    Recreating the catalog tables or database changes its identity even when
    the connection hostname is unchanged. Credentials are never persisted.
    """
    dsn = config.DUCKLAKE_CATALOG
    if dsn.startswith("postgres:postgres"):
        dsn = dsn.removeprefix("postgres:")
    if not dsn.startswith(("postgres://", "postgresql://")):
        raise ValueError("prototype export requires Postgres DuckLake catalog")
    info = conninfo_to_dict(dsn)
    with psycopg.connect(dsn, connect_timeout=5, autocommit=True) as con:
        row = con.execute(
            "SELECT (SELECT oid FROM pg_database WHERE datname=current_database()), "
            "to_regclass('ducklake_snapshot')::oid"
        ).fetchone()
    if row is None or row[1] is None:
        raise ValueError("source catalog identity unavailable")
    return hashlib.sha256(
        canonical_bytes((info.get("host"), info.get("port", "5432"), info.get("dbname"), *row))
    ).hexdigest()


def export_snapshot(
    service_id: str,
    *,
    start: datetime,
    end: datetime,
    max_rows: int = MAX_DATASET_ROWS,
    batch_rows: int = MAX_BATCH_ROWS,
    ttl_hours: float = 24,
) -> Dataset:
    """Explicit operator export. No raw-delete or ingest-ledger mutation."""
    cfg = config.load_config(service_id)
    if not cfg or cfg.get("access_level", "read_write") != "read_write":
        raise ValueError("configured admin service required")
    start, end = utc(start), utc(end)
    if start > end:
        raise ValueError("coverage bounds reversed")
    retention = int((cfg.get("provisioning") or {}).get("cron_sync", {}).get("data_retention_days", 30))
    expires = expiry_for(start, retention_days=retention, ttl_hours=ttl_hours)
    store = PgManifest()
    identity = source_catalog_identity()
    source = config.config_to_source(cfg)
    objects = FosArtifacts(source)
    with duckdb.connect(":memory:") as con:
        con.execute("SET memory_limit='512MB'")
        con.execute("SET threads=2")
        con.execute("SET temp_directory=''")
        _configure_fos(con, source)
        if not _ducklake_attach(con, source, read_only=True):
            raise RuntimeError("source DuckLake attach failed")
        con.execute("BEGIN TRANSACTION")
        try:
            snapshot = con.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            if snapshot is None:
                raise ValueError("source snapshot unavailable")
            dataset = Dataset(
                service_id,
                uuid4().hex,
                identity,
                ducklake_table_name(source),
                int(snapshot[0]),
                start,
                end,
                expires,
                0,
                digest_rows(()),
            )
            reader = pinned_reader(
                con, source, snapshot=dataset.source_snapshot, start=start, end=end, max_rows=max_rows
            )
            result = export_reader(
                reader, dataset=dataset, objects=objects, store=store, max_rows=max_rows, batch_rows=batch_rows
            )
            con.execute("COMMIT")
            return result
        except Exception:
            con.execute("ROLLBACK")
            raise
