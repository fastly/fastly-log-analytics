"""Fenced source-object controller, aggregate batch adapter, assets, and service registry for high-scale."""

from __future__ import annotations

import contextvars
import inspect
import logging
import os
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from backend.high_scale._common import (  # noqa: F401
    _BOOLEAN,
    _COLUMNS,
    _DOMAIN_IDENT_RE,
    _LOG_GAMMA,
    _ROUNDED,
    _TIMESERIES_METRICS,
    _UNSUPPORTED,
    GAMMA,
    MAX_PAGE_SIZE,
    RELATIVE_ERROR,
    VISIBLE_PUBLICATION_FENCE_SQL,
    ArchiveManifestCatalog,
    KeysetCursor,
    _b64,
    _bucket,
    _bucket_case,
    _centroid,
    _column,
    _row_timestamp,
    _unb64,
    build_clickhouse_filters,
    build_published_facts_where,
    build_published_projection_where,
    finite_float,
    format_iso_utc,
    format_iso_utc_or_empty,
    inclusive_minute_end,
    optional_int,
    parse_time_window,
    percentiles,
    range_value,
    raw_query_producer,
    request_filter_clauses,
    resolve_asn_display_labels,
    sketch,
    text,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceObject,
    ArchiveState,
    BaseClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InsertReceipt,
    PublicationResult,
    PublicationStatus,
    S3ObjectStore,
    ServingWatermark,
    _batch_uuid,
    build_batch_id_from_manifest_id,
    row_matches_serving_domain,
    write_batch_archive_checkpoint,
)
from backend.models.analytics import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger(__name__)


# ── Registry Bootstrap (folded from registry_bootstrap.py) ────────────────────
def _resolve_watermark(wm: ServingWatermark | Callable[[], ServingWatermark]) -> ServingWatermark:
    value = wm() if callable(wm) else wm
    value.validate()
    return value


@dataclass(frozen=True)
class HighScaleService:
    service_id: str
    client: QueryClient
    cursor_secret: bytes
    request_watermark: ServingWatermark | Callable[[], ServingWatermark]
    rum_watermarks: Mapping[str, ServingWatermark | Callable[[], ServingWatermark]] = field(default_factory=dict)
    cmcd_watermark: ServingWatermark | Callable[[], ServingWatermark] | None = None
    manifest_catalog: ArchiveManifestCatalog | None = None
    archive: ArchivePublication | None = None

    def watermark(self) -> ServingWatermark:
        return _resolve_watermark(self.request_watermark)

    def watermark_for(self, domain: str) -> ServingWatermark:
        if domain == "request":
            return self.watermark()
        if domain == "cmcd" and self.cmcd_watermark is not None:
            return _resolve_watermark(self.cmcd_watermark)
        value = self.rum_watermarks.get(domain)
        if value is None:
            raise KeyError(domain)
        return _resolve_watermark(value)


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
        return service if service is not None and service.service_id == service_id else None


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
        archive = archive_for(service_id) if archive_for is not None else None
        registry.register(
            HighScaleService(
                service_id=service_id,
                client=client,
                cursor_secret=cursor_secret,
                request_watermark=watermarks["request"],
                rum_watermarks={"rum_vitals": watermarks["rum_vitals"], "rum_errors": watermarks["rum_errors"]},
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
    if os.getenv("HIGH_SCALE_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return ()
    if client is None:
        raise RuntimeError("high-scale mode requires a ClickHouse client")
    service_ids = tuple(s.strip() for s in os.getenv("HIGH_SCALE_SERVICE_IDS", "").split(",") if s.strip())
    if not service_ids:
        from backend import config as app_config

        service_ids = tuple(
            cfg["service_id"] for cfg in app_config.list_configs() if isinstance(cfg, dict) and cfg.get("service_id")
        )
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS must contain at least one service id (none found in configs/)")
    cursor_secret = os.getenv("HIGH_SCALE_CURSOR_SECRET", "").encode("utf-8")
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

    source = get_source_for_service(service_id)
    bucket = source.get("bucket") if source is not None else None
    if not isinstance(bucket, str) or not bucket or source is None:
        return None
    archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
    return ArchivePublication(S3ObjectStore(_get_fos_client(source), bucket=bucket, prefix=archive_prefix))


def _watermark(client: ServingClient, service_id: str, domain: str, *, owner_epoch: int) -> ServingWatermark:
    table = _WATERMARK_TABLES[domain]
    fence_sql = visible_publication_fence_sql(domain)
    event_id_column = "projection_key" if domain == "cmcd" else "event_id"
    rows = client.execute(
        f"SELECT min(event_timestamp) AS coverage_start, max(event_timestamp) AS coverage_end FROM {table} "
        f"WHERE service_id={{service_id:String}} AND {fence_sql}",
        {"service_id": service_id},
    )
    row = rows[0] if rows else {}
    coverage_end = _datetime_or_none(row.get("coverage_end"))
    last_visible_event_id: str | None = None
    if coverage_end is not None:
        latest_rows = client.execute(
            f"SELECT toString({event_id_column}) AS last_visible_event_id FROM {table} "
            f"WHERE service_id={{service_id:String}} AND {fence_sql} "
            f"AND event_timestamp={{coverage_end:DateTime64(3)}} ORDER BY {event_id_column} DESC LIMIT 1",
            {"service_id": service_id, "coverage_end": coverage_end},
        )
        last_visible_event_id = _string_or_none((latest_rows[0] if latest_rows else {}).get("last_visible_event_id"))
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
_SOURCE_FIELD_OVERRIDES: dict[str, str] = {"client_ip": "ip", "cmcd_session": "sid"}


def compute_dimension_counts(domain: str, rows: tuple[Mapping[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    """Count occurrences of each configured dimension's value, per minute bucket."""
    if domain not in DIMENSIONS_BY_DOMAIN:
        raise ValueError(f"unsupported aggregate write domain: {domain}")
    dimensions = DIMENSIONS_BY_DOMAIN[domain]
    counts: Counter[tuple[str, str, datetime]] = Counter()
    for row in rows:
        bkt = _bucket(row.get("timestamp"))
        if bkt is None:
            continue
        for dimension in dimensions:
            counts[(dimension, str(row.get(_SOURCE_FIELD_OVERRIDES.get(dimension, dimension), "")), bkt)] += 1
    return tuple(
        {"dimension": dimension, "value": value, "bucket_start": bkt.isoformat(), "count": count}
        for (dimension, value, bkt), count in counts.items()
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
    "performance_scatter": ("performance_scatter_samples", ("sample_key", "origin_ms", "edge_ms", "cache")),
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
    """Batch rows carry bucket_start as an ISO string (HighScaleBatch.digest requires JSON-serializable row values)."""
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


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
                    *(row[col] for col in metric_columns),
                    batch_uuid,
                    "visible",
                )
                for row in batch.rows
            ]
        else:
            table, metric_col = _AGGREGATE_TABLES[batch.domain]
            columns = ["service_id", "bucket_start", "dimension", "value", metric_col, "batch_id", "publication_state"]
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
            table=table, columns=columns, mapped_rows=mapped_rows, batch=batch, batch_uuid=batch_uuid
        )


from backend.high_scale.cmcd import project_cmcd  # noqa: E402
from backend.high_scale.network import build_network_projection_rows  # noqa: E402
from backend.high_scale.origin import build_origin_projection_rows  # noqa: E402
from backend.high_scale.performance import build_performance_projection_rows  # noqa: E402
from backend.high_scale.query_service import DecodeResult, QueryClient, decode_source_object  # noqa: E402
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
        self, service_id: str, object_key: str, *, accepted_rows: int, malformed_rows: int, expected_owner_epoch: int
    ) -> None: ...
    def register_archive_manifest(self, manifest: ArchiveManifest, *, owner_epoch: int) -> Any: ...
    def archive_manifest(self, manifest_id: str) -> Any: ...
    def transition_archive_manifest(
        self, manifest_id: str, *, expected_state: ArchiveState, next_state: ArchiveState, expected_owner_epoch: int
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
            pub = PublicationResult(f"{service_id}:{domain}:empty", PublicationStatus.VISIBLE, 0, False)
            return BatchIngestResult(None, pub, 0, 0, 0)  # type: ignore[arg-type]
        total_bytes = sum(len(payload) for _, payload in sources_payloads)
        if total_bytes > HIGH_SCALE_PAGE_MAX_BYTES:
            raise ValueError(f"high-scale page size {total_bytes} exceeds maximum {HIGH_SCALE_PAGE_MAX_BYTES}")
        owner = self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale ingest rejected for {service_id}: owner is {owner.current_owner}")
        source_batches: list[tuple[ArchiveSourceObject, list[dict[str, Any]]]] = []
        all_events: list[dict[str, Any]] = []
        sibling_events: list[dict[str, Any]] = []
        total_accepted = total_quarantined = sibling_quarantined = 0
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
            decoded: DecodeResult = decode_source_object(
                archive_source, payload, transform_version=self._transform_version, domain=domain
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
            source_batches.append((archive_source, archive_rows))
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
            logger.warning(
                "high-scale RUM sibling decode rejected retained source records",
                extra={"domain": _RUM_SIBLING_DOMAIN[domain], "quarantined_rows": sibling_quarantined},
            )
        if domain in _RUM_SIBLING_DOMAIN:
            source_batches = [(replace(src, domain=domain), rows) for src, rows in source_batches]
        elif len(source_batches) == 1 and source_batches[0][0].domain != domain:
            source_batches = [(replace(source_batches[0][0], domain=domain), source_batches[0][1])]
        total_rows = total_accepted + total_quarantined
        if total_rows > 0 and (total_quarantined / total_rows) > max_dead_letter_ratio:
            raise ValueError(
                f"high-scale dead letter ratio {total_quarantined}/{total_rows} exceeds maximum {max_dead_letter_ratio}"
            )
        observed = (now or datetime.now(UTC)).astimezone(UTC)
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
            artifact = Path(local_manifest.artifact.uri.removeprefix("file://")).read_bytes()
            manifest = replace(
                local_manifest,
                artifact=replace(local_manifest.artifact, uri=f"s3://archive/{local_manifest.manifest_id}.parquet"),
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
            publication = self._serving.publish(
                HighScaleBatch(batch_id, service_id, domain, str(owner.owner_epoch), serving_rows)
            )
        else:
            publication = PublicationResult(batch_id, PublicationStatus.VISIBLE, 0, False)
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
            sibling_batch_id = build_batch_id_from_manifest_id(f"{manifest.manifest_id}:{sibling}")
            self._serving.publish(
                HighScaleBatch(sibling_batch_id, service_id, sibling, str(owner.owner_epoch), tuple(sibling_events))
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
                        service_id, desc.object_key, gen, manifest.manifest_id, owner.owner_epoch
                    )
                    self._ledger.acknowledge(service_id, desc.object_key, manifest.manifest_id)
        return BatchIngestResult(manifest, publication, total_accepted, total_quarantined, len(sources_payloads))

    def _publish_aggregates_batch(
        self, service_id: str, domain: str, manifest_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
    ) -> None:
        if self._aggregates is None or not events:
            return
        try:
            event_tuple: tuple[dict[str, Any], ...] = tuple(events)
            counts = compute_dimension_counts(domain, event_tuple)
            if counts:
                agg_id = build_batch_id_from_manifest_id(f"{manifest_id}:{domain}_aggregate")
                self._aggregates.publish(
                    HighScaleBatch(agg_id, service_id, f"{domain}_aggregate", str(owner_epoch), counts)
                )
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
                    if rows:
                        sub_id = build_batch_id_from_manifest_id(f"{manifest_id}:{sub_domain}")
                        self._aggregates.publish(HighScaleBatch(sub_id, service_id, sub_domain, str(owner_epoch), rows))
        except Exception:
            logger.exception(
                "high-scale batch aggregate publish failed",
                extra={"service_id": service_id, "domain": domain, "manifest_id": manifest_id},
            )

    def _publish_cmcd_projection(
        self, service_id: str, batch_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
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
            if rows:
                self._serving.publish(HighScaleBatch(batch_id, service_id, "cmcd", str(owner_epoch), tuple(rows)))
        except Exception:
            logger.exception(
                "high-scale cmcd projection publish failed", extra={"service_id": service_id, "batch_id": batch_id}
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
            record = reg_fn(manifest, owner_epoch=owner_epoch, state=ArchiveState.MANIFEST_COMMITTED)
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
        from backend.core.metadata import base as pg_connection

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
            (r["waf_req_id"] if isinstance(r, dict) else r[0]): (
                r["bot_name"] if isinstance(r, dict) else r[1],
                r["category"] if isinstance(r, dict) else r[2],
                r["wellknown_bot_name"] if isinstance(r, dict) else r[3],
            )
            for r in rows
        }
        for e in events:
            wid = str(e.get("waf_req_id")) if e.get("waf_req_id") else None
            if wid and wid in bot_map:
                name, cat, wk = bot_map[wid]
                e["_ngwaf_bot_name"] = e["ngwaf_bot"] = name
                e["_ngwaf_bot_category"] = cat
                e["_ngwaf_wellknown_bot_name"] = wk
                e["ngwaf_bot_verification_state"] = "verified"
                if cat:
                    e["ngwaf_bot_category"] = cat
                if wk:
                    e["ngwaf_wellknown_bot"] = wk
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)


# ── Assets & Shield Performance Aggregates (folded from assets.py) ────────────
_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("high_scale.assets._response_cache", _response_cache)
_ASSET_TYPE_CASE = """CASE
    WHEN url_lower LIKE '%.png' OR url_lower LIKE '%.jpg' OR url_lower LIKE '%.jpeg' OR url_lower LIKE '%.gif' OR url_lower LIKE '%.svg' OR url_lower LIKE '%.webp' OR url_lower LIKE '%.ico' THEN 'Images'
    WHEN url_lower LIKE '%.pdf' OR url_lower LIKE '%.doc' OR url_lower LIKE '%.docx' OR url_lower LIKE '%.xls' OR url_lower LIKE '%.xlsx' OR url_lower LIKE '%.ppt' OR url_lower LIKE '%.pptx' OR url_lower LIKE '%.txt' OR url_lower LIKE '%.csv' OR url_lower LIKE '%.zip' OR url_lower LIKE '%.tar' OR url_lower LIKE '%.gz' OR url_lower LIKE '%.tgz' OR url_lower LIKE '%.rar' OR url_lower LIKE '%.7z' THEN 'Documents'
    WHEN url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css' THEN 'JavaScript/CSS'
    WHEN url_lower LIKE '%.woff' OR url_lower LIKE '%.woff2' OR url_lower LIKE '%.ttf' OR url_lower LIKE '%.otf' OR url_lower LIKE '%.eot' THEN 'Fonts'
    WHEN url_lower LIKE '%.m3u8' OR url_lower LIKE '%.ts' OR url_lower LIKE '%.mp4' OR url_lower LIKE '%.m4s' OR url_lower LIKE '%.webm' THEN 'Video'
    ELSE 'API/Dynamic'
END"""


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
        r, b = int(reqs or 0), int(sum_bytes or 0)
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
        by_cache[(asset_type, cache_status)][0] += r
        by_cache[(asset_type, cache_status)][1] += b
        by_comp[(asset_type, encoding_norm)][0] += r
        by_comp[(asset_type, encoding_norm)][1] += b
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
        {"asset_type": at, "cache_status": cs, "requests": vals[0], "bytes": vals[1]}
        for (at, cs), vals in sorted(by_cache.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]
    compression_performance = [
        {"asset_type": at, "content_encoding": ce, "requests": vals[0], "bytes": vals[1]}
        for (at, ce), vals in sorted(by_comp.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]
    return asset_type_breakdown, cache_performance, compression_performance


def assets_aggregates(
    service: HighScaleService, req: AssetsRequest, start_time: str | None, end_time: str | None
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
            SELECT url, if(custom_fields['cache'] != '', custom_fields['cache'], 'MISS') AS cache,
                   custom_fields['resp_header_content_encoding'] AS content_encoding,
                   if(custom_fields['status'] != '', toInt32OrZero(custom_fields['status']), 200) AS status,
                   count() AS req_cnt, sum(toInt64OrZero(custom_fields['resp_bytes'])) AS sum_bytes,
                   sum(coalesce(toFloat64OrNull(custom_fields['ttl']), 0.0)) AS sum_ttl,
                   countIf(toFloat64OrNull(custom_fields['ttl']) IS NOT NULL) AS cnt_ttl
            FROM request_facts WHERE {where} GROUP BY url, cache, content_encoding, status
        ),
        grouped_base AS (
            SELECT *, lower(url) AS url_lower, lower(content_encoding) AS enc_lower FROM grouped_raw
        ),
        assets_enriched AS (
            SELECT url, cache, content_encoding, status, req_cnt, sum_bytes, sum_ttl, cnt_ttl,
                   {_ASSET_TYPE_CASE} AS asset_type,
                   if(enc_lower IN ('gzip', 'br', 'deflate'), 1, 0) AS is_compressed,
                   if(url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css'
                      OR url_lower LIKE '%.svg' OR url_lower LIKE '%.html' OR url_lower LIKE '%.json', 1, 0) AS is_compressible
            FROM grouped_base
        )"""
    dims_sql = f"""
        /* assets:summary_dims */
        {enriched_cte}
        SELECT asset_type, cache AS cache_status,
               CASE WHEN lower(content_encoding) LIKE '%br%' THEN 'br'
                    WHEN lower(content_encoding) LIKE '%gzip%' THEN 'gzip'
                    WHEN content_encoding != '' THEN content_encoding
                    ELSE 'uncompressed' END AS content_encoding_norm,
               is_compressed, is_compressible,
               toInt64(sum(req_cnt)) AS requests, toInt64(sum(sum_bytes)) AS bytes
        FROM assets_enriched
        GROUP BY asset_type, cache_status, content_encoding_norm, is_compressed, is_compressible
    """
    hotspots_sql = f"""
        /* assets:hotspots */
        {enriched_cte}
        SELECT * FROM (
            SELECT 'large' AS kind, url, toInt64(sum(req_cnt)) AS requests,
                   toFloat64(sum(sum_bytes)) / nullIf(toFloat64(sum(req_cnt)), 0.0) AS metric_float,
                   toInt64(sum(sum_bytes)) AS total_bytes, toInt32(status) AS status, '' AS asset_type
            FROM assets_enriched
            WHERE is_compressed = 0 AND is_compressible = 1 AND status NOT IN (304, 204)
            GROUP BY url, status ORDER BY total_bytes DESC LIMIT 20
        )
        UNION ALL
        SELECT * FROM (
            SELECT 'low_ttl' AS kind, url, toInt64(sum(req_cnt)) AS requests,
                   toFloat64(sum(sum_ttl)) / nullIf(toFloat64(sum(cnt_ttl)), 0.0) AS metric_float,
                   toInt64(0) AS total_bytes, toInt32(0) AS status, asset_type
            FROM assets_enriched
            WHERE cache = 'MISS' AND asset_type != 'API/Dynamic'
            GROUP BY url, asset_type ORDER BY requests DESC LIMIT 20
        )
    """
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-assets") as pool:
        fut_dims = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(dims_sql, params))
        fut_hotspots = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(hotspots_sql, params))
        dim_dicts, hotspot_dicts = fut_dims.result(), fut_hotspots.result()
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


# ── Worker & Orchestration (folded from worker.py & orchestration.py) ─────────
MAX_SOURCE_PAGE_SIZE = 500
INITIAL_SOURCE_CURSOR = "__initial__"
TERMINAL_SOURCE_CURSOR_PREFIX = "__terminal__:"
RUM_REINSPECTION_INTERVAL = 10
RUM_SOURCE_DOMAINS = frozenset({"rum_vitals", "rum_errors"})


class SourceObjectMissingError(RuntimeError):
    """Raised when a claimed source object is confirmed gone from storage."""


@dataclass(frozen=True)
class SourceObjectDescriptor:
    object_key: str
    checksum: str
    size_bytes: int = 0
    version: str | None = None


@dataclass(frozen=True)
class SourceObjectPage:
    objects: Sequence[SourceObjectDescriptor]
    next_cursor: str | None
    next_reinspection_cursor: str | None = None
    forward_pages_since_reinspection: int | None = None
    reinspection_page: bool = False


class SourceObjectLister(Protocol):
    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
    ) -> SourceObjectPage: ...


class S3SourceObjectLister:
    """Paginated S3-compatible source listing for the high-scale worker plane."""

    _PREFIXES = {"request": "raw/request/", "rum_vitals": "raw/rum/", "rum_errors": "raw/rum/"}

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
    ) -> SourceObjectPage:
        del service_id
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        domain_prefix = self._PREFIXES.get(domain)
        if domain_prefix is None:
            raise ValueError(f"unsupported source domain: {domain}")
        if reinspect and domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        if forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        resolved_prefix = self._key(domain_prefix)
        listing_cursor = (reinspection_cursor or INITIAL_SOURCE_CURSOR) if reinspect else cursor
        request: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": resolved_prefix,
            "PaginationConfig": {"PageSize": page_size},
        }
        if listing_cursor and listing_cursor != INITIAL_SOURCE_CURSOR:
            if listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                start_after = listing_cursor.removeprefix(TERMINAL_SOURCE_CURSOR_PREFIX)
                if start_after.startswith(resolved_prefix):
                    request["StartAfter"] = start_after
            else:
                request["PaginationConfig"]["StartingToken"] = listing_cursor
        page = next(iter(self._client.get_paginator("list_objects_v2").paginate(**request)))
        objects = tuple(
            SourceObjectDescriptor(
                object_key=item["Key"],
                checksum=_source_checksum(item),
                size_bytes=int(item.get("Size", 0)),
                version=item.get("VersionId") or item.get("ETag"),
            )
            for item in page.get("Contents", [])
            if isinstance(item.get("Key"), str)
        )
        next_cursor = page.get("NextToken")
        if domain not in RUM_SOURCE_DOMAINS:
            return SourceObjectPage(objects, next_cursor)
        if reinspect:
            has_more = bool(page.get("NextContinuationToken"))
            next_reinspection = (
                f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
                if has_more and objects
                else INITIAL_SOURCE_CURSOR
            )
            return SourceObjectPage(
                objects,
                cursor or INITIAL_SOURCE_CURSOR,
                next_reinspection_cursor=next_reinspection,
                forward_pages_since_reinspection=0,
                reinspection_page=True,
            )
        return SourceObjectPage(
            objects,
            next_cursor,
            next_reinspection_cursor=reinspection_cursor or INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=forward_pages_since_reinspection + 1,
        )

    def _key(self, key: str) -> str:
        normalized = key.lstrip("/")
        return f"{self._prefix}/{normalized}" if self._prefix else normalized


class S3SourceObjectReader:
    """Reads immutable source objects from an S3-compatible bucket."""

    def __init__(self, client: Any, *, bucket: str) -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        del service_id, domain
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
        except Exception as exc:
            if _client_error_code(exc) in {"NoSuchKey", "404"}:
                raise SourceObjectMissingError(f"source object is missing from storage: {object_key}") from exc
            raise
        return response["Body"].read()


def _client_error_code(exc: Exception) -> str | None:
    """Extract the S3/boto3 error code without importing botocore directly."""
    response = getattr(exc, "response", None)
    error = response.get("Error") if isinstance(response, dict) else None
    code = error.get("Code") if isinstance(error, dict) else None
    return code if isinstance(code, str) else None


def _source_checksum(item: dict[str, object]) -> str:
    checksum = item.get("ChecksumSHA256") or item.get("ChecksumSHA1")
    if isinstance(checksum, str) and checksum:
        return checksum if ":" in checksum else f"checksum:{checksum}"
    etag = item.get("ETag")
    if isinstance(etag, str) and etag:
        return f"etag:{etag.strip(chr(34))}"
    raise ValueError("source object listing item has no checksum or ETag")


class SourceObjectReader(Protocol):
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes: ...


class SourceControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...
    def source_cursor_for(self, service_id: str, domain: str) -> str: ...
    def source_reinspection_state_for(self, service_id: str, domain: str) -> tuple[str, int]: ...
    def advance_reinspection_state(
        self,
        service_id: str,
        domain: str,
        reinspection_cursor: str,
        forward_pages_since_reinspection: int,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> None: ...
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
        now: datetime | None = None,
    ) -> Any: ...


@dataclass(frozen=True)
class PageRun:
    cursor: str | None
    next_cursor: str | None
    discovered: int
    processed: int
    duplicates: int
    failed: int
    missing: int = 0


def _owner_fields(owner: Any) -> tuple[str, int, str | None]:
    return owner.current_owner, int(owner.owner_epoch), getattr(owner, "source_cursor", None)


class HighScaleWorkerCoordinator:
    """Runs bounded discovery and processing under an owner-epoch fence."""

    def __init__(
        self,
        *,
        lister: SourceObjectLister,
        reader: SourceObjectReader,
        controller: HighScaleIngestController,
        ownership: Any,
        ledger: Any,
        worker_id: str,
        control_plane: SourceControlPlane | None = None,
        page_size: int = 100,
        lease_seconds: float = 300.0,
        object_concurrency: int = 1,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if object_concurrency <= 0:
            raise ValueError("object_concurrency must be positive")
        self._lister = lister
        self._reader = reader
        self._controller = controller
        self._ownership = ownership
        self._ledger = ledger
        self._worker_id = worker_id
        self._control = control_plane
        self._page_size = page_size
        self._lease_seconds = lease_seconds
        self._object_concurrency = object_concurrency

    def run_page(
        self, *, service_id: str, domain: str, cursor: str | None = None, now: datetime | None = None
    ) -> PageRun:
        current_owner, epoch, persisted_cursor = _owner_fields(self._owner(service_id))
        self._require_high_scale(current_owner, service_id)
        if cursor is None:
            cursor_reader = self._control if self._control is not None else self._ownership
            if hasattr(cursor_reader, "source_cursor_for"):
                persisted_cursor = cursor_reader.source_cursor_for(service_id, domain)
        listing_cursor = persisted_cursor if cursor is None else cursor
        has_reinspection = self._control is not None and domain in RUM_SOURCE_DOMAINS
        listing_options: dict[str, Any] = {}
        if has_reinspection and self._control is not None:
            reinspection_cursor, fwd_pages = self._control.source_reinspection_state_for(service_id, domain)
            listing_options = {
                "reinspection_cursor": reinspection_cursor,
                "reinspect": fwd_pages >= RUM_REINSPECTION_INTERVAL,
                "forward_pages_since_reinspection": fwd_pages,
            }
        page = self._lister.list_source_objects(
            service_id, domain, page_size=self._page_size, cursor=listing_cursor, **listing_options
        )
        objects = tuple(page.objects)
        if len(objects) > self._page_size:
            raise ValueError("source object listing exceeded page_size")

        expired_sources = self._expired_claim_descriptors(service_id, domain)
        discovered_by_key = {s.object_key: self._discover(service_id, domain, s, current_owner, epoch) for s in objects}
        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                next_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
            elif listing_cursor and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                next_cursor = listing_cursor

        processed = duplicates = failed = missing = 0
        active_sources: list[SourceObjectDescriptor] = list(expired_sources)
        active_keys = {s.object_key for s in active_sources}
        for s in objects:
            if s.object_key in active_keys:
                continue
            rec_status = getattr(discovered_by_key.get(s.object_key), "status", None)
            is_terminal = (
                rec_status in _TERMINAL_SOURCE_STATES
                if isinstance(rec_status, str)
                else self._already_terminal(service_id, s.object_key)
            )
            if is_terminal:
                duplicates += 1
            else:
                active_sources.append(s)
                active_keys.add(s.object_key)

        if active_sources:
            unclaimable_keys: set[str] = set()
            if self._control is not None and (
                getattr(type(self._control), "claim_sources_batch", None) is not None
                or "claim_sources_batch" in vars(self._control)
                or "claim_sources_batch" in getattr(self._control, "_mock_children", {})
            ):
                claims = getattr(self._control, "claim_sources_batch")(
                    service_id,
                    tuple(s.object_key for s in active_sources),
                    self._worker_id,
                    expected_owner=current_owner,
                    expected_owner_epoch=epoch,
                    lease_seconds=self._lease_seconds,
                    now=now,
                )
                claimed_keys = {c.object_key for c in claims}
            elif self._control is not None:
                claimed_keys = set()
                for s in active_sources:
                    try:
                        c = self._control.claim_source(
                            service_id,
                            s.object_key,
                            self._worker_id,
                            expected_owner=current_owner,
                            expected_owner_epoch=epoch,
                            lease_seconds=self._lease_seconds,
                            now=now,
                        )
                        if c.claimed:
                            claimed_keys.add(s.object_key)
                    except ValueError:
                        if not self._already_terminal(service_id, s.object_key):
                            failed += 1
                            unclaimable_keys.add(s.object_key)
            else:
                claimed_keys = {
                    s.object_key
                    for s in active_sources
                    if self._ledger.claim(service_id, s.object_key, self._worker_id).claimed
                }

            to_read: list[SourceObjectDescriptor] = []
            for s in active_sources:
                if s.object_key in claimed_keys:
                    to_read.append(s)
                elif s.object_key not in unclaimable_keys:
                    duplicates += 1

            if to_read:

                def read_one(
                    src: SourceObjectDescriptor,
                ) -> tuple[SourceObjectDescriptor, bytes | None, str | None, bool]:
                    try:
                        return (src, self._reader.read_source_object(service_id, domain, src.object_key), None, False)
                    except SourceObjectMissingError as exc:
                        return (src, None, str(exc), True)
                    except Exception as exc:
                        return (src, None, str(exc), False)

                workers = min(self._object_concurrency, len(to_read))
                if workers <= 1:
                    read_results = [read_one(s) for s in to_read]
                else:
                    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="high-scale-batch-reader") as pool:
                        read_results = list(pool.map(read_one, to_read))

                valid_payloads: list[tuple[SourceObjectDescriptor, bytes]] = []
                for src, data, err, is_missing in read_results:
                    if is_missing:
                        self._mark_missing(
                            service_id,
                            object_key=src.object_key,
                            expected_owner=current_owner,
                            expected_epoch=epoch,
                            error=err or "missing",
                            now=now,
                        )
                        missing += 1
                    elif err is not None:
                        logger.exception(
                            "high-scale source read failed",
                            extra={
                                "service_id": service_id,
                                "domain": domain,
                                "object_key": src.object_key,
                                "error": err,
                            },
                        )
                        failed += 1
                    elif data is not None:
                        valid_payloads.append((src, data))

                if valid_payloads:
                    try:
                        batch_res = self._controller.ingest_batch(
                            service_id=service_id,
                            domain=domain,
                            sources_payloads=valid_payloads,
                            next_cursor=None if failed > 0 or page.reinspection_page else next_cursor,
                            now=now,
                        )
                        processed += batch_res.sources_count
                    except Exception as exc:
                        logger.exception(
                            "high-scale batch ingest failed",
                            extra={"service_id": service_id, "domain": domain, "count": len(valid_payloads)},
                        )
                        if self._control is None and hasattr(self._ledger, "mark_failed"):
                            for src, _ in valid_payloads:
                                cur = self._ledger.source(service_id, src.object_key)
                                if cur is not None:
                                    self._ledger.mark_failed(service_id, src.object_key, cur.lease_generation, str(exc))
                        failed += len(valid_payloads)

        if failed == 0:
            if page.reinspection_page and has_reinspection and self._control is not None:
                self._control.advance_reinspection_state(
                    service_id,
                    domain,
                    page.next_reinspection_cursor or INITIAL_SOURCE_CURSOR,
                    page.forward_pages_since_reinspection or 0,
                    expected_owner=current_owner,
                    expected_owner_epoch=epoch,
                )
            else:
                self._advance_cursor(
                    service_id,
                    domain,
                    next_cursor,
                    current_owner,
                    epoch,
                    reinspection_cursor=page.next_reinspection_cursor if has_reinspection else None,
                    forward_pages_since_reinspection=(
                        page.forward_pages_since_reinspection if has_reinspection else None
                    ),
                )
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed, missing)

    def _expired_claim_descriptors(self, service_id: str, domain: str) -> list[SourceObjectDescriptor]:
        expired_claims = getattr(self._control if self._control is not None else self._ledger, "expired_claims", None)
        if expired_claims is None:
            return []
        return [
            SourceObjectDescriptor(s.object_key, s.checksum, s.size_bytes, s.version)
            for s in expired_claims(service_id, domain, limit=self._page_size)
        ]

    def _mark_missing(
        self,
        service_id: str,
        *,
        object_key: str,
        expected_owner: str,
        expected_epoch: int,
        error: str,
        now: datetime | None,
    ) -> None:
        if self._control is not None and hasattr(self._control, "mark_source_missing"):
            self._control.mark_source_missing(
                service_id,
                object_key,
                expected_owner=expected_owner,
                expected_owner_epoch=expected_epoch,
                error=error,
                now=now,
            )
        elif hasattr(self._ledger, "mark_source_missing"):
            self._ledger.mark_source_missing(service_id, object_key, error=error)

    def _owner(self, service_id: str) -> Any:
        return self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)

    def _discover(
        self, service_id: str, domain: str, source: SourceObjectDescriptor, expected_owner: str, epoch: int
    ) -> Any:
        if self._control is not None:
            return self._control.discover_source(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
                expected_owner=expected_owner,
                expected_owner_epoch=epoch,
            )
        return self._ledger.discover(
            service_id,
            domain,
            source.object_key,
            source.checksum,
            size_bytes=source.size_bytes,
            version=source.version,
        )

    def _advance_cursor(
        self,
        service_id: str,
        domain: str,
        cursor: str | None,
        owner: str,
        epoch: int,
        *,
        reinspection_cursor: str | None = None,
        forward_pages_since_reinspection: int | None = None,
    ) -> None:
        if cursor is None:
            return
        if self._control is not None and hasattr(self._control, "advance_source_cursor"):
            state_kwargs = (
                {
                    "reinspection_cursor": reinspection_cursor,
                    "forward_pages_since_reinspection": forward_pages_since_reinspection,
                }
                if reinspection_cursor is not None and forward_pages_since_reinspection is not None
                else {}
            )
            self._control.advance_source_cursor(
                service_id, cursor, expected_owner=owner, expected_owner_epoch=epoch, domain=domain, **state_kwargs
            )
        elif hasattr(self._ownership, "advance_cursor"):
            self._ownership.advance_cursor(
                service_id, cursor, expected_owner=owner, expected_epoch=epoch, domain=domain
            )

    def _already_terminal(self, service_id: str, object_key: str) -> bool:
        source = None
        if self._control is not None and hasattr(self._control, "source"):
            try:
                source = self._control.source(service_id, object_key)
            except KeyError:
                source = None
        elif hasattr(self._ledger, "source"):
            source = self._ledger.source(service_id, object_key)
        elif hasattr(self._ledger, "get_source"):
            source = self._ledger.get_source(service_id, object_key)
        return getattr(source, "status", None) in _TERMINAL_SOURCE_STATES

    @staticmethod
    def _require_high_scale(owner: str, service_id: str) -> None:
        if owner != "high_scale":
            raise RuntimeError(f"high-scale worker is not the owner for {service_id}")


_TERMINAL_SOURCE_STATES = frozenset({"appended", "archived", "acknowledged", "source_deleted", "source_missing"})


import backend.high_scale.recovery as recovery  # noqa: E402, F401
import backend.high_scale.rum as rum  # noqa: E402, F401
from backend.high_scale._common import bucket as bucket  # noqa: E402, F401
from backend.high_scale.archive import (  # noqa: E402, F401
    ProjectionReplayResult,
    _batch_from_rows,
    _load_manifest_request_events,
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
)
from backend.high_scale.postgres_control import (  # noqa: E402, F401
    DeletionController,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from backend.high_scale.recovery import (  # noqa: E402, F401
    _DOMAINS,
    _IGNORED_CONTENT,
    _ORIGIN_PROJECTION_TABLES,
    _READ_LIMITS,
    _RUM_FACT_TABLES,
    RecoveryCapacityMeasurement,
    RecoveryCapacityReport,
    RecoveryReport,
    RumRecoveryBatchAdapter,
    RumRecoveryPlan,
    _canonical,
    _content,
    _evaluate_capacity_gate,
    _purge_since,
    _rebuild_request_projections_from_fos,
    _replay_all,
    _sorted,
    _SqlClient,
    evaluate_recovery_capacity,
    execute_rum_recovery,
    plan_rum_recovery,
    purge_cmcd_projections,
    purge_origin_projections,
    purge_rum_facts,
    rebuild_cmcd_projections_from_fos,
    rebuild_origin_projections_from_fos,
    rebuild_performance_projections_from_fos,
    rebuild_rum_facts_from_fos,
    rebuild_security_projections_from_fos,
    rebuild_serving_state_from_fos,
    replay_security_projections,
)
from backend.high_scale.rum import (  # noqa: E402, F401
    _BEACON_IDENTITY,
    _FILTER_COLUMNS,
    _RUM_FILTERS_ADAPTER,
    _filter_clauses,
    _iso,
    _ttfb_ms,
    parse_rum_filters,
    rum_analytics,
    rum_beacon_health,
    rum_events,
    rum_live_events,
)

_WORKER_EXPORTS = frozenset(
    {
        "FairPageSelector",
        "HighScaleWorkerLoop",
        "WorkerPageResult",
        "_csv_env",
        "_MultiServiceWorkerLoop",
        "_weights_from_env",
        "build_worker_from_environment",
        "main",
    }
)


def __getattr__(name: str) -> Any:
    if name == "worker" or name in _WORKER_EXPORTS:
        import backend.high_scale.worker as _worker_mod

        globals()["worker"] = _worker_mod
        for _attr in _WORKER_EXPORTS:
            globals()[_attr] = getattr(_worker_mod, _attr)
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


orchestration = sys.modules[__name__]
