"""Fenced, generation-scoped publication of verified immutable payloads.

No ingest hook: only explicit bounded snapshot exports can produce membership.
Retries preserve ordinals and payload; FINAL makes duplicates invisible before
background merges. Partial generations are never selected.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from backend.core.clickhouse_client import (
    CLICKHOUSE_FACT_COLUMNS,
    ClickHouseClient,
    ClickHouseError,
    get_clickhouse_client,
)
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_metrics import record_publication
from backend.core.clickhouse_rows import (
    MAX_BATCH_ROWS,
    PAYLOAD_COLUMNS,
    build_batch_id,
    canonical_bytes,
    canonical_row,
    digest_rows,
)
from backend.core.clickhouse_schema import target_identity

__all__ = [
    "DurableBatch",
    "PublicationResult",
    "ReplayResult",
    "build_batch_id",
    "publish_batch",
    "replay_unpublished",
    "full_rebuild",
    "readiness",
]
logger = structlog.get_logger(__name__)


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


@dataclass(frozen=True)
class Readiness:
    eligible: bool
    reason: str
    dataset: Dataset | None = None
    generation: str | None = None


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
            pass
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
            pass


def _publish_batch(
    service_id: str,
    batch: DurableBatch,
    *,
    generation: str,
    store: PgManifest | None,
    client: ClickHouseClient | None,
) -> PublicationResult:
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
            pass
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
    store = store or PgManifest()
    client = _client(client)
    generation = store.begin_generation(service_id, dataset_id, target_identity(client))
    return replay_unpublished(service_id, generation=generation, loader=loader, limit=limit, store=store, client=client)


def readiness(
    service_id: str,
    *,
    start: datetime,
    end: datetime,
    source_snapshot: int,
    catalog_identity: str,
    source_table: str,
    store: PgManifest | None = None,
    client: ClickHouseClient | None = None,
) -> Readiness:
    """Task-7 seam; caller MUST supply the snapshot bound to its actual view.

    A fresh target proof is intentionally required, not a process-local cached
    green flag. This bounded prototype favors correctness over lookup cost.
    """
    store = store or PgManifest()
    selected = store.selected(service_id)
    if selected is None:
        return Readiness(False, "no_active_generation")
    try:
        dataset = store.dataset(service_id, selected["dataset_id"])
    except ValueError:
        return Readiness(False, "expired_or_unsupported_dataset")
    if not dataset.coverage_start <= start <= end <= dataset.coverage_end:
        return Readiness(False, "outside_coverage")
    if (source_snapshot, catalog_identity, source_table) != (
        dataset.source_snapshot,
        dataset.catalog_identity,
        dataset.source_table,
    ):
        return Readiness(False, "source_snapshot_mismatch")
    try:
        verify_generation(service_id, selected["generation"], store=store, client=_client(client))
    except ClickHouseError:
        # An outage is not an ineligible dataset. Serving callers return 503;
        # a healthy but incomplete/empty target still falls back explicitly.
        raise
    except (ValueError, RuntimeError):
        return Readiness(False, "target_unverified")
    if dataset.expires_at <= datetime.now(UTC):
        return Readiness(False, "expired_or_unsupported_dataset")
    return Readiness(True, "ready", dataset, selected["generation"])
