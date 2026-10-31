"""Fenced source-object controller, aggregate batch adapter, assets, and service registry for high-scale."""

from __future__ import annotations

import contextvars
import logging
import os
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from backend.high_scale.query_service import QueryClient

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
    ArchivePublication,
    BaseClickHouseBatchAdapter,
    HighScaleBatch,
    InsertReceipt,
    S3ObjectStore,
    ServingWatermark,
    _batch_uuid,
)
from backend.models.analytics import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger("backend.high_scale.ingest_controller")

from backend.high_scale.query_service import QueryClient  # noqa: E402


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
            "s": start_dt.isoformat(),
            "e": end_dt.isoformat(),
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
