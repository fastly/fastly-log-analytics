"""Fenced source-object controller, aggregate batch adapter, and service registry for high-scale."""

from __future__ import annotations

import inspect
import logging
import os
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from backend.high_scale._common import (
    ArchiveManifestCatalog,
    parse_time_window,
    request_filter_clauses,
    visible_fact_clauses,
)
from backend.high_scale._common import bucket as _bucket
from backend.high_scale.archive_models import (
    ArchiveManifest,
    ArchiveSourceObject,
    ArchiveState,
    ServingWatermark,
)
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import write_batch_archive_checkpoint
from backend.high_scale.publication import (
    BaseClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InsertReceipt,
    PublicationResult,
    PublicationStatus,
    _batch_uuid,
    build_batch_id_from_manifest_id,
    row_matches_serving_domain,
)
from backend.high_scale.query_service import DecodeResult, QueryClient, decode_source_object
from backend.models.performance import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

if TYPE_CHECKING:
    from backend.high_scale.source_discovery import SourceObjectDescriptor

logger = logging.getLogger(__name__)


# ── Registry Bootstrap (folded from registry_bootstrap.py) ────────────────────


@dataclass(frozen=True)
class HighScaleService:
    service_id: str
    client: QueryClient
    cursor_secret: bytes
    request_watermark: ServingWatermark | Callable[[], ServingWatermark]
    rum_watermarks: Mapping[str, ServingWatermark | Callable[[], ServingWatermark]] = field(default_factory=dict)
    cmcd_watermark: ServingWatermark | Callable[[], ServingWatermark] | None = None
    # Cold-tier (beyond ClickHouse warm retention) raw-row queries scan the FOS
    # archive directly and are opt-in: a service with neither configured simply
    # cannot serve QueryTier.COLD_JOB, and the /queries endpoint reports that
    # explicitly rather than silently falling back to an unbounded scan.
    manifest_catalog: ArchiveManifestCatalog | None = None
    archive: ArchivePublication | None = None

    def watermark(self) -> ServingWatermark:
        value = self.request_watermark() if callable(self.request_watermark) else self.request_watermark
        value.validate()
        return value

    def watermark_for(self, domain: str) -> ServingWatermark:
        if domain == "request":
            return self.watermark()
        cmcd_watermark = self.cmcd_watermark
        if domain == "cmcd" and cmcd_watermark is not None:
            cmcd_value = cmcd_watermark
            watermark = cmcd_value() if callable(cmcd_value) else cmcd_value
            watermark.validate()
            return watermark
        value = self.rum_watermarks.get(domain)
        if value is None:
            raise KeyError(domain)
        watermark = value() if callable(value) else value
        watermark.validate()
        return watermark


class HighScaleServiceRegistryProtocol(Protocol):
    def resolve(self, service_id: str) -> HighScaleService | None: ...


class HighScaleServiceRegistry:
    """Small injectable registry; services opt in by explicit registration."""

    def __init__(self, services: Mapping[str, HighScaleService] | None = None) -> None:
        self._services = dict(services or {})

    def register(self, service: HighScaleService) -> None:
        if not service.service_id:
            raise ValueError("high-scale service id is required")
        if not service.cursor_secret:
            raise ValueError("high-scale cursor secret is required")
        self._services[service.service_id] = service

    def resolve(self, service_id: str) -> HighScaleService | None:
        service = self._services.get(service_id)
        if service is None or service.service_id != service_id:
            return None
        return service


_registry = HighScaleServiceRegistry()


def get_high_scale_service_registry() -> HighScaleServiceRegistry:
    """FastAPI dependency for explicit high-scale service bindings."""
    return _registry


class ServingClient(Protocol):
    def execute(
        self, sql: str, params: dict[str, Any] | None = None, *, settings: Mapping[str, str] | None = None
    ) -> list[dict[str, Any]]: ...


_WATERMARK_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}


def register_high_scale_services(
    registry: HighScaleServiceRegistry,
    *,
    client: ServingClient,
    service_ids: Iterable[str],
    cursor_secret: bytes,
    owner_epoch: int = 1,
    manifest_catalog: ArchiveManifestCatalog | None = None,
    archive_for: Callable[[str], ArchivePublication | None] | None = None,
) -> None:
    if not cursor_secret:
        raise ValueError("high-scale cursor secret is required")
    if owner_epoch < 0:
        raise ValueError("high-scale owner epoch must be non-negative")

    for service_id in service_ids:
        if not service_id:
            raise ValueError("high-scale service id is required")

        def _watermark_for(domain: str, bound_service_id: str = service_id) -> Callable[[], ServingWatermark]:
            return lambda: _watermark(client, bound_service_id, domain, owner_epoch=owner_epoch)

        watermarks = {domain: _watermark_for(domain) for domain in _WATERMARK_TABLES}
        # Cold-tier raw queries require BOTH a manifest catalog and an
        # archive reader for this specific service; a service missing
        # either gets neither, so the /queries endpoint reports it as
        # explicitly unconfigured rather than half-wiring a capability that
        # can never actually serve a cold-tier plan.
        archive = archive_for(service_id) if archive_for is not None else None
        registry.register(
            HighScaleService(
                service_id=service_id,
                client=client,
                cursor_secret=cursor_secret,
                request_watermark=watermarks["request"],
                rum_watermarks={
                    "rum_vitals": watermarks["rum_vitals"],
                    "rum_errors": watermarks["rum_errors"],
                },
                cmcd_watermark=watermarks["cmcd"],
                manifest_catalog=manifest_catalog if archive is not None else None,
                archive=archive,
            )
        )


def register_high_scale_services_from_environment(
    registry: HighScaleServiceRegistry,
    *,
    client: ServingClient | None,
    control_factory: Callable[[], ArchiveManifestCatalog] | None = None,
    archive_factory: Callable[[str], ArchivePublication | None] | None = None,
) -> tuple[str, ...]:
    enabled = os.getenv("HIGH_SCALE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return ()
    if client is None:
        raise RuntimeError("high-scale mode requires a ClickHouse client")
    service_ids = tuple(
        service_id.strip() for service_id in os.getenv("HIGH_SCALE_SERVICE_IDS", "").split(",") if service_id.strip()
    )
    if not service_ids:
        from backend import config as app_config

        service_ids = tuple(
            cfg["service_id"] for cfg in app_config.list_configs() if isinstance(cfg, dict) and cfg.get("service_id")
        )
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS must contain at least one service id (none found in configs/)")
    cursor_secret = os.getenv("HIGH_SCALE_CURSOR_SECRET", "").encode("utf-8")

    # Cold-tier wiring is best-effort: the control plane is the same
    # Postgres dependency the isolated worker already hard-requires when
    # HIGH_SCALE_ENABLED=1 (see worker.py's build_worker_from_environment),
    # so a failure here is as fatal as it already is for the worker. A
    # per-service archive/FOS lookup failure is NOT fatal — it degrades
    # that one service to cold-tier-unconfigured (a 404, never a crash of
    # every other high-scale endpoint) via register_high_scale_services'
    # archive_for contract.
    manifest_catalog = (control_factory or _default_control_factory)()

    def _archive_for(service_id: str) -> ArchivePublication | None:
        try:
            return (archive_factory or _default_archive_factory)(service_id)
        except Exception:
            logger.warning("high-scale cold-tier archive wiring failed for %s", service_id, exc_info=True)
            return None

    register_high_scale_services(
        registry,
        client=client,
        service_ids=service_ids,
        cursor_secret=cursor_secret,
        owner_epoch=int(os.getenv("HIGH_SCALE_OWNER_EPOCH", "1")),
        manifest_catalog=manifest_catalog,
        archive_for=_archive_for,
    )
    return service_ids


def _default_control_factory() -> ArchiveManifestCatalog:
    from backend.high_scale.postgres_control import PostgresControlPlane

    return PostgresControlPlane()


def _default_archive_factory(service_id: str) -> ArchivePublication | None:
    from backend.core.duckdb import _get_fos_client, get_source_for_service
    from backend.high_scale.archive_publication import S3ObjectStore

    source = get_source_for_service(service_id)
    if source is None:
        return None
    bucket = source.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return None
    fos_client = _get_fos_client(source)
    archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
    return ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))


def _watermark(
    client: ServingClient,
    service_id: str,
    domain: str,
    *,
    owner_epoch: int,
) -> ServingWatermark:
    from backend.high_scale._common import visible_publication_fence_sql

    table = _WATERMARK_TABLES[domain]
    fence_sql = visible_publication_fence_sql(domain)
    event_id_column = "projection_key" if domain == "cmcd" else "event_id"
    rows = client.execute(
        f"SELECT min(event_timestamp) AS coverage_start, "
        f"max(event_timestamp) AS coverage_end "
        f"FROM {table} "
        f"WHERE service_id={{service_id:String}} AND {fence_sql}",
        {"service_id": service_id},
    )
    row = rows[0] if rows else {}
    coverage_end = _datetime_or_none(row.get("coverage_end"))
    last_visible_event_id: str | None = None
    if coverage_end is not None:
        latest_rows = client.execute(
            f"SELECT toString({event_id_column}) AS last_visible_event_id "
            f"FROM {table} "
            "WHERE service_id={service_id:String} "
            f"AND {fence_sql} "
            "AND event_timestamp={coverage_end:DateTime64(3)} "
            f"ORDER BY {event_id_column} DESC LIMIT 1",
            {"service_id": service_id, "coverage_end": coverage_end},
        )
        latest_row = latest_rows[0] if latest_rows else {}
        last_visible_event_id = _string_or_none(latest_row.get("last_visible_event_id"))
    return ServingWatermark(
        service_id=service_id,
        domain=domain,
        owner_epoch=owner_epoch,
        coverage_start=_datetime_or_none(row.get("coverage_start")),
        coverage_end=coverage_end,
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=last_visible_event_id,
        exact=True,
    )


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        normalized = value.replace(" ", "T", 1)
        if not normalized.endswith(("Z", "+00:00")):
            normalized += "+00:00"
        try:
            return datetime.fromisoformat(normalized).astimezone(UTC)
        except ValueError:
            return None
    return None


def _string_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


# ── Aggregate Batch Adapter (folded from aggregate_batch_adapter.py) ──────────

DIMENSIONS_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    "request": (
        "url",
        "country",
        "client_ip",
        "asn",
        "host",
        "method",
        "status",
        "cache",
        "proto",
        "ua",
        "referer",
        "cookie_session",
        "resp_header_content_encoding",
        "ttl",
        "age",
        "hits",
        "digest",
        "city",
        "region",
        "metro",
        "transport",
        "c_speed",
        "c_type",
        "pop",
        "backend",
        "edge",
        "server_region",
        "tls",
        "is_ipv6",
        "conn_requests",
        "waf",
        "waf_resp",
        "waf_ms",
        "p_type",
        "p_desc",
        "ja3",
        "ja4",
        "tls_ciphers_sha",
        "io_input_format",
        "io_output_format",
    ),
    "rum_vitals": ("metric_name",),
    "rum_errors": ("error_message",),
    "cmcd": ("cmcd_session",),
}
_SOURCE_FIELD_OVERRIDES: dict[str, str] = {
    "client_ip": "ip",
    "cmcd_session": "sid",
}


def compute_dimension_counts(
    domain: str,
    rows: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Count occurrences of each configured dimension's value, per minute bucket."""
    if domain not in DIMENSIONS_BY_DOMAIN:
        raise ValueError(f"unsupported aggregate write domain: {domain}")
    dimensions = DIMENSIONS_BY_DOMAIN[domain]
    counts: Counter[tuple[str, str, datetime]] = Counter()
    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        for dimension in dimensions:
            field_name = _SOURCE_FIELD_OVERRIDES.get(dimension, dimension)
            value = str(row.get(field_name, ""))
            counts[(dimension, value, bucket)] += 1
    return tuple(
        {"dimension": dimension, "value": value, "bucket_start": bucket.isoformat(), "count": count}
        for (dimension, value, bucket), count in counts.items()
    )


_AGGREGATE_TABLES: dict[str, tuple[str, str]] = {
    "request_aggregate": ("request_aggregates", "request_count"),
    "rum_vitals_aggregate": ("rum_vitals_aggregates", "event_count"),
    "rum_errors_aggregate": ("rum_error_aggregates", "error_count"),
    "cmcd_aggregate": ("cmcd_aggregates", "event_count"),
}
_ORIGIN_COLUMNS: dict[str, tuple[str, tuple[str, ...]]] = {
    "origin_summary": (
        "origin_minute_summary",
        (
            "requests",
            "misses",
            "passes",
            "origin_5xx",
            "status_count",
            "origin_bytes",
            "latency_count",
            "ttlb_count",
            "overhead_count",
            "origin_bytes_count",
            "latency_p50_us",
            "latency_p75_us",
            "latency_p95_us",
            "latency_p99_us",
            "ttlb_p50_us",
            "ttlb_p95_us",
            "cdn_overhead_p50_us",
            "origin_bytes_p50",
            "latency_sk_v",
            "latency_sk_w",
            "ttlb_sk_v",
            "ttlb_sk_w",
            "overhead_sk_v",
            "overhead_sk_w",
            "origin_bytes_sk_v",
            "origin_bytes_sk_w",
        ),
    ),
    "origin_dimensions": (
        "origin_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "origin_5xx",
            "origin_bytes",
            "latency_count",
            "latency_p50_us",
            "latency_p95_us",
            "latency_p99_us",
            "latency_sk_v",
            "latency_sk_w",
        ),
    ),
    "performance_dimensions": (
        "performance_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "latency_count",
            "latency_sum_ms",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
        ),
    ),
    "performance_summary": (
        "performance_minute_summary",
        (
            "component_count",
            "edge_processing_sum_ms",
            "origin_wait_sum_ms",
            "origin_download_sum_ms",
            "client_download_sum_ms",
        ),
    ),
    "performance_scatter": (
        "performance_scatter_samples",
        ("sample_key", "origin_ms", "edge_ms", "cache"),
    ),
    "security_dimensions": (
        "security_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "wellknown_bot_name",
            "bot_category",
            "verified_count",
            "impersonator_count",
            "unverified_count",
        ),
    ),
    "network_dimensions": (
        "network_minute_dimensions",
        (
            "dimension",
            "value",
            "c_speed",
            "requests",
            "errors",
            "tcp_rtt_count",
            "tcp_rtt_sum",
            "tcp_rtt_p50_us",
            "tcp_rtt_p95_us",
            "tcp_rtt_p99_us",
            "ploss_sum",
            "ploss_count",
            "ttfb_p50_us",
            "ttfb_p95_us",
        ),
    ),
}


def _bucket_start(value: Any) -> datetime:
    """Batch rows carry bucket_start as an ISO string (HighScaleBatch.digest
    requires JSON-serializable row values) — parse back for the insert."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class AggregateBatchAdapter(BaseClickHouseBatchAdapter):
    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain not in _AGGREGATE_TABLES and batch.domain not in _ORIGIN_COLUMNS:
            raise ValueError(f"unsupported aggregate domain: {batch.domain}")
        if not batch.rows:
            raise ValueError("aggregate batch must contain at least one row")
        batch_uuid = _batch_uuid(batch)
        with self._batch_lock(batch_uuid):
            return self._insert_locked(batch, batch_uuid)

    def _insert_locked(self, batch: HighScaleBatch, batch_uuid: str) -> InsertReceipt:
        if batch.domain in _ORIGIN_COLUMNS:
            table, metric_columns = _ORIGIN_COLUMNS[batch.domain]
            columns = ["service_id", "bucket_start", *metric_columns, "batch_id", "publication_state"]
            mapped_rows = [
                (
                    batch.service_id,
                    _bucket_start(row["bucket_start"]),
                    *(row[column] for column in metric_columns),
                    batch_uuid,
                    "visible",
                )
                for row in batch.rows
            ]
        else:
            table, metric_column = _AGGREGATE_TABLES[batch.domain]
            columns = [
                "service_id",
                "bucket_start",
                "dimension",
                "value",
                metric_column,
                "batch_id",
                "publication_state",
            ]
            mapped_rows = [
                (
                    batch.service_id,
                    _bucket_start(row["bucket_start"]),
                    str(row["dimension"]),
                    str(row["value"]),
                    int(row["count"]),
                    batch_uuid,
                    "visible",
                )
                for row in batch.rows
            ]

        return self._publish_table_rows(
            table=table,
            columns=columns,
            mapped_rows=mapped_rows,
            batch=batch,
            batch_uuid=batch_uuid,
        )


# Register sys.modules aliases BEFORE importing projection builders that depend on HighScaleService
_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.registry_bootstrap"] = _self_mod
sys.modules["backend.high_scale.aggregate_batch_adapter"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "registry_bootstrap", _self_mod)
    setattr(_hs_pkg, "aggregate_batch_adapter", _self_mod)

from backend.high_scale.cmcd import project_cmcd  # noqa: E402
from backend.high_scale.network import build_network_projection_rows  # noqa: E402
from backend.high_scale.origin import build_origin_projection_rows  # noqa: E402
from backend.high_scale.performance_projection import build_performance_projection_rows  # noqa: E402
from backend.high_scale.security import build_security_projection_rows  # noqa: E402

# ── Ingest Controller ─────────────────────────────────────────────────────────

HIGH_SCALE_PAGE_MAX_BYTES = 64 * 1024 * 1024  # 64 MB in-flight byte guard
HIGH_SCALE_MAX_DEAD_LETTER_RATIO = 0.05  # 5% dead-letter limit
_RUM_SIBLING_DOMAIN = {"rum_vitals": "rum_errors", "rum_errors": "rum_vitals"}


@dataclass(frozen=True)
class BatchIngestResult:
    manifest: ArchiveManifest
    publication: PublicationResult
    total_events: int
    quarantined_events: int
    sources_count: int


class HighScaleControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...

    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int,
        version: str | None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> Any: ...

    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float,
        now: datetime | None,
    ) -> Any: ...

    def record_source_counts(
        self,
        service_id: str,
        object_key: str,
        *,
        accepted_rows: int,
        malformed_rows: int,
        expected_owner_epoch: int,
    ) -> None: ...

    def register_archive_manifest(self, manifest: ArchiveManifest, *, owner_epoch: int) -> Any: ...

    def archive_manifest(self, manifest_id: str) -> Any: ...

    def transition_archive_manifest(
        self,
        manifest_id: str,
        *,
        expected_state: ArchiveState,
        next_state: ArchiveState,
        expected_owner_epoch: int,
    ) -> Any: ...

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
    ) -> tuple[Any, ...]: ...

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
    ) -> None: ...


class HighScaleIngestController:
    """Coordinates idempotent, owner-fenced source-object batch ingest."""

    batch_capable: bool = True

    def __init__(
        self,
        *,
        ownership: Any = None,
        ledger: Any = None,
        control_plane: HighScaleControlPlane | None = None,
        archive: ArchivePublication,
        serving: ClickHousePublication,
        worker_id: str,
        aggregates: Any | None = None,
        transform_version: str = "normalize.v1",
        schema_version: str = "archive.v1",
        archive_epoch: int = 0,
        retention_seconds: int = 0,
        deletion_grace_seconds: int = 900,
        lease_seconds: float = 300.0,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if lease_seconds <= 0:
            raise ValueError("lease duration must be positive")
        if control_plane is None and (ownership is None or ledger is None):
            raise ValueError("control_plane or both ownership and ledger are required")
        self._ownership: Any = ownership if ownership is not None else control_plane
        self._ledger: Any = ledger if ledger is not None else control_plane
        self._control = control_plane
        self._archive = archive
        self._serving = serving
        self._aggregates = aggregates
        self._worker_id = worker_id
        self._transform_version = transform_version
        self._schema_version = schema_version
        self._archive_epoch = archive_epoch
        self._retention_seconds = retention_seconds
        self._deletion_grace_seconds = deletion_grace_seconds
        self._lease_seconds = lease_seconds

    def ingest_batch(
        self,
        *,
        service_id: str,
        domain: str,
        sources_payloads: Sequence[tuple[SourceObjectDescriptor, bytes]],
        next_cursor: str | None = None,
        partition_id: str = "default",
        now: datetime | None = None,
        max_dead_letter_ratio: float = HIGH_SCALE_MAX_DEAD_LETTER_RATIO,
    ) -> BatchIngestResult:
        if not sources_payloads:
            return BatchIngestResult(
                manifest=None,  # type: ignore[arg-type]
                publication=PublicationResult(
                    batch_id=f"{service_id}:{domain}:empty",
                    status=PublicationStatus.VISIBLE,
                    rows_visible=0,
                    duplicate=False,
                ),
                total_events=0,
                quarantined_events=0,
                sources_count=0,
            )

        total_bytes = sum(len(payload) for _, payload in sources_payloads)
        if total_bytes > HIGH_SCALE_PAGE_MAX_BYTES:
            raise ValueError(f"high-scale page size {total_bytes} exceeds maximum {HIGH_SCALE_PAGE_MAX_BYTES}")

        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest rejected for {service_id}: owner is {owner.current_owner}")

        decoded_sources: list[tuple[SourceObjectDescriptor, DecodeResult, list[dict[str, Any]]]] = []
        all_events: list[dict[str, Any]] = []
        sibling_events: list[dict[str, Any]] = []
        total_accepted = 0
        total_quarantined = 0
        sibling_quarantined = 0

        for descriptor, payload in sources_payloads:
            if self._control is None:
                src_obj = self._ledger.discover(
                    service_id,
                    domain,
                    descriptor.object_key,
                    descriptor.checksum,
                    size_bytes=len(payload),
                    version=descriptor.version,
                )
                if len(sources_payloads) == 1:
                    domain = src_obj.domain
                if src_obj.status == "discovered":
                    claim = self._ledger.claim(
                        service_id,
                        descriptor.object_key,
                        self._worker_id,
                        lease_seconds=int(self._lease_seconds),
                        now=now,
                    )
                    if not claim.claimed:
                        raise RuntimeError(f"source object {descriptor.object_key} is already claimed")
            archive_source = ArchiveSourceObject(
                service_id=service_id,
                domain=domain,
                object_key=descriptor.object_key,
                checksum=descriptor.checksum,
                size_bytes=len(payload),
                version=descriptor.version,
            )
            decoded = decode_source_object(
                archive_source,
                payload,
                transform_version=self._transform_version,
                domain=domain,
            )
            total_accepted += decoded.accepted_rows
            total_quarantined += decoded.quarantined_rows

            if self._control is not None:
                self._control.record_source_counts(
                    service_id,
                    descriptor.object_key,
                    accepted_rows=decoded.accepted_rows,
                    malformed_rows=decoded.quarantined_rows,
                    expected_owner_epoch=owner.owner_epoch,
                )
            else:
                self._ledger.record_counts(
                    service_id,
                    descriptor.object_key,
                    accepted_rows=decoded.accepted_rows,
                    malformed_rows=decoded.quarantined_rows,
                )

            archive_rows = [dict(event, _record_kind="event") for event in decoded.events]
            archive_rows.extend(
                {
                    "_record_kind": "dead_letter",
                    "source_object_key": item.source_object_key,
                    "line_ordinal": item.line_ordinal,
                    "raw_line_base64": item.raw_line_base64,
                    "reason": item.reason,
                }
                for item in decoded.dead_letters
            )

            decoded_sources.append((descriptor, decoded, archive_rows))
            all_events.extend(decoded.events)
            sibling = _RUM_SIBLING_DOMAIN.get(domain)
            if sibling is not None:
                sibling_decoded = decode_source_object(
                    replace(archive_source, domain=sibling),
                    payload,
                    transform_version=self._transform_version,
                    domain=sibling,
                )
                sibling_quarantined += sibling_decoded.quarantined_rows
                sibling_events.extend(e for e in sibling_decoded.events if row_matches_serving_domain(sibling, e))

        if sibling_quarantined:
            # The claimed-domain archive retains these original raw_json lines,
            # including mixed bodies with a valid exception and invalid metric.
            # Count rejection under the sibling, not as a claimed-domain row.
            logger.warning(
                "high-scale RUM sibling decode rejected retained source records",
                extra={"domain": _RUM_SIBLING_DOMAIN[domain], "quarantined_rows": sibling_quarantined},
            )
        total_rows = total_accepted + total_quarantined
        if total_rows > 0 and (total_quarantined / total_rows) > max_dead_letter_ratio:
            raise ValueError(
                f"high-scale dead letter ratio {total_quarantined}/{total_rows} exceeds maximum {max_dead_letter_ratio}"
            )

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        source_batches = [
            (
                ArchiveSourceObject(
                    service_id=service_id,
                    domain=domain,
                    object_key=desc.object_key,
                    checksum=desc.checksum,
                    size_bytes=len(payload),
                    version=desc.version,
                ),
                rows,
            )
            for (desc, _, rows), (_, payload) in zip(decoded_sources, sources_payloads)
        ]

        with tempfile.TemporaryDirectory(prefix="high-scale-archive-") as root:
            local_manifest = write_batch_archive_checkpoint(
                Path(root),
                service_id=service_id,
                domain=domain,
                source_batches=source_batches,
                archive_epoch=self._archive_epoch,
                schema_version=self._schema_version,
                transform_version=self._transform_version,
                coverage_start=observed,
                coverage_end=observed,
                retention_seconds=self._retention_seconds,
                deletion_grace_seconds=self._deletion_grace_seconds,
            )
            artifact_path = Path(local_manifest.artifact.uri.removeprefix("file://"))
            artifact = artifact_path.read_bytes()
            manifest = replace(
                local_manifest,
                artifact=replace(
                    local_manifest.artifact,
                    uri=f"s3://archive/{local_manifest.manifest_id}.parquet",
                ),
            )
            if self._control is not None:
                try:
                    manifest = self._control.archive_manifest(manifest.manifest_id).manifest
                except KeyError:
                    pass
            self._archive.publish(manifest, artifact)

        if self._control is not None:
            self._publish_control_manifest(manifest, owner.owner_epoch)

        current_owner = (
            self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        )
        if current_owner.current_owner != "high_scale" or current_owner.owner_epoch != owner.owner_epoch:
            raise RuntimeError(f"high-scale owner epoch changed during ingest for {service_id}")

        if domain == "request":
            _enrich_high_scale_ngwaf_bots(tuple(all_events))
        serving_rows = tuple(e for e in all_events if row_matches_serving_domain(domain, e))
        batch_id = build_batch_id_from_manifest_id(manifest.manifest_id)
        if serving_rows:
            batch = HighScaleBatch(
                batch_id=batch_id,
                service_id=service_id,
                domain=domain,
                generation=str(owner.owner_epoch),
                rows=serving_rows,
            )
            publication = self._serving.publish(batch)
        else:
            publication = PublicationResult(
                batch_id=batch_id,
                status=PublicationStatus.VISIBLE,
                rows_visible=0,
                duplicate=False,
            )

        self._publish_aggregates_batch(service_id, domain, manifest.manifest_id, owner.owner_epoch, all_events)
        if domain == "request":
            self._publish_cmcd_projection(
                service_id,
                build_batch_id_from_manifest_id(f"{manifest.manifest_id}:cmcd"),
                owner.owner_epoch,
                all_events,
            )

        sibling = _RUM_SIBLING_DOMAIN.get(domain)
        if sibling is not None and sibling_events:
            self._serving.publish(
                HighScaleBatch(
                    batch_id=build_batch_id_from_manifest_id(f"{manifest.manifest_id}:{sibling}"),
                    service_id=service_id,
                    domain=sibling,
                    generation=str(owner.owner_epoch),
                    rows=tuple(sibling_events),
                )
            )
            self._publish_aggregates_batch(service_id, sibling, manifest.manifest_id, owner.owner_epoch, sibling_events)

        object_keys = tuple(desc.object_key for desc, _ in sources_payloads)
        if self._control is not None:
            self._control.acknowledge_sources_batch(
                service_id,
                object_keys,
                manifest_id=manifest.manifest_id,
                expected_owner_epoch=owner.owner_epoch,
                next_cursor=next_cursor,
                domain=domain,
                partition_id=partition_id,
            )
        else:
            for desc, _ in sources_payloads:
                cur_src = self._ledger.source(service_id, desc.object_key)
                gen = cur_src.lease_generation if cur_src is not None else 1
                if cur_src is None or cur_src.status != "acknowledged":
                    self._ledger.mark_appended(service_id, desc.object_key, gen)
                    self._ledger.mark_archived(
                        service_id,
                        desc.object_key,
                        gen,
                        manifest.manifest_id,
                        owner.owner_epoch,
                    )
                    self._ledger.acknowledge(service_id, desc.object_key, manifest.manifest_id)

        return BatchIngestResult(
            manifest=manifest,
            publication=publication,
            total_events=total_accepted,
            quarantined_events=total_quarantined,
            sources_count=len(sources_payloads),
        )

    def _publish_aggregates_batch(
        self,
        service_id: str,
        domain: str,
        manifest_id: str,
        owner_epoch: int,
        events: Sequence[dict[str, Any]],
    ) -> None:
        if self._aggregates is None or not events:
            return
        try:
            event_tuple: tuple[dict[str, Any], ...] = tuple(events)
            counts = compute_dimension_counts(domain, event_tuple)
            if counts:
                batch = HighScaleBatch(
                    batch_id=build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate"),
                    service_id=service_id,
                    domain=f"{domain}_aggregate",
                    generation=str(owner_epoch),
                    rows=counts,
                )
                self._aggregates.publish(batch)
            if domain == "request":
                origin_proj = build_origin_projection_rows(event_tuple)
                perf_proj = build_performance_projection_rows(event_tuple)
                security_proj = build_security_projection_rows(event_tuple)
                network_proj = build_network_projection_rows(event_tuple)

                for sub_domain, rows in (
                    ("origin_summary", origin_proj.summary_rows),
                    ("origin_dimensions", origin_proj.dimension_rows),
                    ("performance_dimensions", perf_proj.dimension_rows),
                    ("performance_summary", perf_proj.summary_rows),
                    ("performance_scatter", perf_proj.scatter_rows),
                    ("security_dimensions", security_proj.dimension_rows),
                    ("network_dimensions", network_proj.dimension_rows),
                ):
                    if not rows:
                        continue
                    self._aggregates.publish(
                        HighScaleBatch(
                            batch_id=build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}"),
                            service_id=service_id,
                            domain=sub_domain,
                            generation=str(owner_epoch),
                            rows=rows,
                        )
                    )
        except Exception:
            logger.exception(
                "high-scale batch aggregate publish failed",
                extra={"service_id": service_id, "domain": domain, "manifest_id": manifest_id},
            )

    def _publish_cmcd_projection(
        self,
        service_id: str,
        batch_id: str,
        owner_epoch: int,
        events: Sequence[dict[str, Any]],
    ) -> None:
        """Best-effort CMCD projection of request events carrying CMCD fields."""
        try:
            rows: list[dict[str, Any]] = []
            skipped = 0
            for event in events:
                if not event.get("cmcd"):
                    continue
                try:
                    projection = project_cmcd(event)
                except ValueError:
                    skipped += 1
                    continue
                if projection is not None:
                    rows.append(projection)
            if skipped:
                logger.warning(
                    "high-scale cmcd projection skipped malformed events",
                    extra={"service_id": service_id, "skipped": skipped},
                )
            if not rows:
                return
            self._serving.publish(
                HighScaleBatch(
                    batch_id=batch_id,
                    service_id=service_id,
                    domain="cmcd",
                    generation=str(owner_epoch),
                    rows=tuple(rows),
                )
            )
        except Exception:
            logger.exception(
                "high-scale cmcd projection publish failed",
                extra={"service_id": service_id, "batch_id": batch_id},
            )

    def _publish_control_manifest(self, manifest: ArchiveManifest, owner_epoch: int) -> None:
        if self._control is None:
            raise RuntimeError("self._control is unexpectedly None")
        accepts_state = False
        try:
            accepts_state = "state" in inspect.signature(self._control.register_archive_manifest).parameters
        except (TypeError, ValueError):
            accepts_state = False
        if accepts_state:
            reg_fn: Any = self._control.register_archive_manifest
            record = reg_fn(
                manifest,
                owner_epoch=owner_epoch,
                state=ArchiveState.MANIFEST_COMMITTED,
            )
        else:
            record = self._control.register_archive_manifest(manifest, owner_epoch=owner_epoch)
        states = (
            ArchiveState.ARTIFACT_UPLOADING,
            ArchiveState.ARTIFACT_VERIFIED,
            ArchiveState.MANIFEST_PREPARED,
            ArchiveState.MANIFEST_COMMITTED,
        )
        state_index = states.index(record.state)
        for next_state in states[state_index + 1 :]:
            self._control.transition_archive_manifest(
                manifest.manifest_id,
                expected_state=states[state_index],
                next_state=next_state,
                expected_owner_epoch=owner_epoch,
            )
            state_index += 1


def _enrich_high_scale_ngwaf_bots(events: tuple[dict[str, Any], ...]) -> None:
    waf_req_ids = {str(e.get("waf_req_id")) for e in events if e.get("waf_req_id")}
    if not waf_req_ids:
        return

    try:
        from backend.core.metadata import pg_connection

        pconn = pg_connection.get_pg_readonly_connection()
        try:
            placeholders = ",".join("%s" for _ in waf_req_ids)
            cur = pconn.execute(
                f"SELECT waf_req_id, bot_name, category, wellknown_bot_name FROM ngwaf_bots WHERE waf_req_id IN ({placeholders})",
                list(waf_req_ids),
            )
            rows = cur.fetchall()
        finally:
            pconn.close()

        bot_map = {
            (r["waf_req_id"] if isinstance(r, dict) else r[0]): {
                "name": r["bot_name"] if isinstance(r, dict) else r[1],
                "cat": r["category"] if isinstance(r, dict) else r[2],
                "wk": r["wellknown_bot_name"] if isinstance(r, dict) else r[3],
            }
            for r in rows
        }
        for e in events:
            wid = str(e.get("waf_req_id")) if e.get("waf_req_id") else None
            if wid and wid in bot_map:
                bot = bot_map[wid]
                e["_ngwaf_bot_name"] = bot["name"]
                e["_ngwaf_bot_category"] = bot["cat"]
                e["_ngwaf_wellknown_bot_name"] = bot["wk"]
                e["ngwaf_bot_verification_state"] = "verified"
                # Persisted on the fact row (no underscore) so filtered Security
                # queries can group by bot without the waf_req_id join.
                e["ngwaf_bot"] = bot["name"]
                if bot["cat"]:
                    e["ngwaf_bot_category"] = bot["cat"]
                if bot["wk"]:
                    e["ngwaf_wellknown_bot"] = bot["wk"]
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)


from backend.high_scale.source_discovery import SourceObjectDescriptor  # noqa: E402, F401

# ── Assets & Shield Performance Aggregates (folded from assets.py) ────────────

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("high_scale.assets._response_cache", _response_cache)

_ASSET_TYPE_CASE = """
    CASE
        WHEN url_lower LIKE '%.png' OR url_lower LIKE '%.jpg' OR url_lower LIKE '%.jpeg' OR url_lower LIKE '%.gif' OR url_lower LIKE '%.svg' OR url_lower LIKE '%.webp' OR url_lower LIKE '%.ico' THEN 'Images'
        WHEN url_lower LIKE '%.pdf' OR url_lower LIKE '%.doc' OR url_lower LIKE '%.docx' OR url_lower LIKE '%.xls' OR url_lower LIKE '%.xlsx' OR url_lower LIKE '%.ppt' OR url_lower LIKE '%.pptx' OR url_lower LIKE '%.txt' OR url_lower LIKE '%.csv' OR url_lower LIKE '%.zip' OR url_lower LIKE '%.tar' OR url_lower LIKE '%.gz' OR url_lower LIKE '%.tgz' OR url_lower LIKE '%.rar' OR url_lower LIKE '%.7z' THEN 'Documents'
        WHEN url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css' THEN 'JavaScript/CSS'
        WHEN url_lower LIKE '%.woff' OR url_lower LIKE '%.woff2' OR url_lower LIKE '%.ttf' OR url_lower LIKE '%.otf' OR url_lower LIKE '%.eot' THEN 'Fonts'
        WHEN url_lower LIKE '%.m3u8' OR url_lower LIKE '%.ts' OR url_lower LIKE '%.mp4' OR url_lower LIKE '%.m4s' OR url_lower LIKE '%.webm' THEN 'Video'
        ELSE 'API/Dynamic'
    END
"""


def compute_summary_from_dims(
    dim_rows: list[tuple[str, str, str, int, int, int, int]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Compute breakdown, cache_performance, and compression_performance from grouped dimension rows."""
    by_type_reqs: dict[str, int] = defaultdict(int)
    by_type_bytes: dict[str, int] = defaultdict(int)
    by_type_hit_reqs: dict[str, int] = defaultdict(int)
    by_type_cache_denom: dict[str, int] = defaultdict(int)
    by_type_comp_num: dict[str, int] = defaultdict(int)
    by_type_comp_denom: dict[str, int] = defaultdict(int)

    by_cache: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    by_comp: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])

    for asset_type, cache_status, encoding_norm, is_compressed, is_compressible, reqs, sum_bytes in dim_rows:
        r = int(reqs or 0)
        b = int(sum_bytes or 0)
        by_type_reqs[asset_type] += r
        by_type_bytes[asset_type] += b
        if cache_status in ("HIT", "HIT-SYNTHETIC"):
            by_type_hit_reqs[asset_type] += r
        if cache_status in ("HIT", "HIT-SYNTHETIC", "MISS", "PASS", "BYPASS"):
            by_type_cache_denom[asset_type] += r
        if is_compressible == 1:
            by_type_comp_denom[asset_type] += r
            if is_compressed == 1:
                by_type_comp_num[asset_type] += r

        cache_pair = by_cache[(asset_type, cache_status)]
        cache_pair[0] += r
        cache_pair[1] += b

        comp_pair = by_comp[(asset_type, encoding_norm)]
        comp_pair[0] += r
        comp_pair[1] += b

    asset_type_breakdown = [
        {
            "asset_type": at,
            "requests": by_type_reqs[at],
            "egress_bytes": by_type_bytes[at],
            "cache_hit_ratio": (
                float(by_type_hit_reqs[at]) / float(by_type_cache_denom[at]) if by_type_cache_denom[at] > 0 else 0.0
            ),
            "compression_rate": (
                float(by_type_comp_num[at]) / float(by_type_comp_denom[at]) if by_type_comp_denom[at] > 0 else 0.0
            ),
        }
        for at in sorted(by_type_reqs.keys(), key=lambda k: (-by_type_reqs[k], k))
    ]

    cache_performance = [
        {
            "asset_type": at,
            "cache_status": cs,
            "requests": vals[0],
            "bytes": vals[1],
        }
        for (at, cs), vals in sorted(by_cache.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]

    compression_performance = [
        {
            "asset_type": at,
            "content_encoding": ce,
            "requests": vals[0],
            "bytes": vals[1],
        }
        for (at, ce), vals in sorted(by_comp.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]

    return asset_type_breakdown, cache_performance, compression_performance


def assets_aggregates(
    service: HighScaleService,
    req: AssetsRequest,
    start_time: str | None,
    end_time: str | None,
) -> AssetsAggregatesResponse:
    start_dt, end_dt = parse_time_window(start_time, end_time)

    cache_key = digest_cache_key(
        {
            "svc": service.service_id,
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(req.filters),
        },
        {"name": service.service_id},
    )
    cached = _memo.get_with_telemetry(cache_key)
    if cached is not None:
        return AssetsAggregatesResponse.with_telemetry(**cached)

    filter_clauses, filter_params = request_filter_clauses(req.filters)
    where = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "event_timestamp >= {start_time:DateTime64(3)}",
            "event_timestamp <= {end_time:DateTime64(3)}",
            "url != ''",
            *filter_clauses,
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        **filter_params,
    }

    enriched_cte = f"""
        WITH grouped_raw AS (
            SELECT
                url,
                if(custom_fields['cache'] != '', custom_fields['cache'], 'MISS') AS cache,
                custom_fields['resp_header_content_encoding'] AS content_encoding,
                if(custom_fields['status'] != '', toInt32OrZero(custom_fields['status']), 200) AS status,
                count() AS req_cnt,
                sum(toInt64OrZero(custom_fields['resp_bytes'])) AS sum_bytes,
                sum(coalesce(toFloat64OrNull(custom_fields['ttl']), 0.0)) AS sum_ttl,
                countIf(toFloat64OrNull(custom_fields['ttl']) IS NOT NULL) AS cnt_ttl
            FROM request_facts
            WHERE {where}
            GROUP BY url, cache, content_encoding, status
        ),
        grouped_base AS (
            SELECT
                *,
                lower(url) AS url_lower,
                lower(content_encoding) AS enc_lower
            FROM grouped_raw
        ),
        assets_enriched AS (
            SELECT
                url,
                cache,
                content_encoding,
                status,
                req_cnt,
                sum_bytes,
                sum_ttl,
                cnt_ttl,
                {_ASSET_TYPE_CASE} AS asset_type,
                if(enc_lower IN ('gzip', 'br', 'deflate'), 1, 0) AS is_compressed,
                if(
                    url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css'
                    OR url_lower LIKE '%.svg' OR url_lower LIKE '%.html' OR url_lower LIKE '%.json',
                    1,
                    0
                ) AS is_compressible
            FROM grouped_base
        )
    """

    dims_sql = f"""
        /* assets:summary_dims */
        {enriched_cte}
        SELECT
            asset_type,
            cache AS cache_status,
            CASE
                WHEN lower(content_encoding) LIKE '%br%' THEN 'br'
                WHEN lower(content_encoding) LIKE '%gzip%' THEN 'gzip'
                WHEN content_encoding != '' THEN content_encoding
                ELSE 'uncompressed'
            END AS content_encoding_norm,
            is_compressed,
            is_compressible,
            toInt64(sum(req_cnt)) AS requests,
            toInt64(sum(sum_bytes)) AS bytes
        FROM assets_enriched
        GROUP BY asset_type, cache_status, content_encoding_norm, is_compressed, is_compressible
    """

    hotspots_sql = f"""
        /* assets:hotspots */
        {enriched_cte}
        SELECT * FROM (
            SELECT
                'large' AS kind,
                url,
                toInt64(sum(req_cnt)) AS requests,
                toFloat64(sum(sum_bytes)) / nullIf(toFloat64(sum(req_cnt)), 0.0) AS metric_float,
                toInt64(sum(sum_bytes)) AS total_bytes,
                toInt32(status) AS status,
                '' AS asset_type
            FROM assets_enriched
            WHERE is_compressed = 0 AND is_compressible = 1 AND status NOT IN (304, 204)
            GROUP BY url, status
            ORDER BY total_bytes DESC
            LIMIT 20
        )
        UNION ALL
        SELECT * FROM (
            SELECT
                'low_ttl' AS kind,
                url,
                toInt64(sum(req_cnt)) AS requests,
                toFloat64(sum(sum_ttl)) / nullIf(toFloat64(sum(cnt_ttl)), 0.0) AS metric_float,
                toInt64(0) AS total_bytes,
                toInt32(0) AS status,
                asset_type
            FROM assets_enriched
            WHERE cache = 'MISS' AND asset_type != 'API/Dynamic'
            GROUP BY url, asset_type
            ORDER BY requests DESC
            LIMIT 20
        )
    """

    import contextvars
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-assets") as pool:
        fut_dims = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(dims_sql, params))
        fut_hotspots = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(hotspots_sql, params))
        dim_dicts = fut_dims.result()
        hotspot_dicts = fut_hotspots.result()

    dim_tuples = [
        (
            str(d["asset_type"]),
            str(d["cache_status"]),
            str(d["content_encoding_norm"]),
            int(d["is_compressed"] or 0),
            int(d["is_compressible"] or 0),
            int(d["requests"] or 0),
            int(d["bytes"] or 0),
        )
        for d in dim_dicts
    ]
    asset_type_breakdown, cache_performance, compression_performance = compute_summary_from_dims(dim_tuples)

    large_uncompressed_assets: list[dict[str, Any]] = []
    low_ttl_assets: list[dict[str, Any]] = []
    for d in hotspot_dicts:
        if d.get("kind") == "large":
            large_uncompressed_assets.append(
                {
                    "url": str(d["url"]),
                    "requests": int(d["requests"] or 0),
                    "avg_bytes": float(d["metric_float"] or 0.0),
                    "total_bytes": int(d["total_bytes"] or 0),
                    "status": int(d["status"] or 200),
                }
            )
        elif d.get("kind") == "low_ttl":
            low_ttl_assets.append(
                {
                    "url": str(d["url"]),
                    "requests": int(d["requests"] or 0),
                    "avg_ttl": float(d["metric_float"] or 0.0),
                    "asset_type": str(d["asset_type"]),
                }
            )

    payload = {
        "asset_type_breakdown": asset_type_breakdown,
        "cache_performance": cache_performance,
        "compression_performance": compression_performance,
        "large_uncompressed_assets": large_uncompressed_assets,
        "low_ttl_assets": low_ttl_assets,
        "missing_fields": [],
    }
    _memo.put(cache_key, payload)
    return AssetsAggregatesResponse.with_telemetry(**payload)


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.assets"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "assets", _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.high_scale.assets": "backend.high_scale.ingest_controller",
            "backend.high_scale.console_access": "backend.high_scale.postgres_control",
            "backend.high_scale.deletion": "backend.high_scale.postgres_control",
            "backend.high_scale.admin_plane": "backend.high_scale.postgres_control",
            "backend.high_scale.decoder": "backend.high_scale.query_service",
            "backend.high_scale.query": "backend.high_scale.query_service",
            "backend.high_scale.clickhouse": "backend.high_scale.query_service",
            "backend.high_scale.rum_recovery": "backend.high_scale.rum",
            "backend.high_scale.rum_ingest": "backend.high_scale.rum",
        }
    )
