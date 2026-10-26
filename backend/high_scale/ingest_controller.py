"""Fenced source-object controller, aggregate batch adapter, worker, recovery, and service registry for high-scale."""

from __future__ import annotations

import base64
import hashlib
import hmac
import inspect
import json
import logging
import math
import os
import random
import re
import sys
import tempfile
import time
from argparse import ArgumentParser
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any, Protocol, overload
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from backend import config as app_config
from backend.core.clickhouse import ClickHouseClient
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceObject,
    ArchiveState,
    S3ObjectStore,
    ServingWatermark,
    from_environment,
    write_batch_archive_checkpoint,
)
from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key
from backend.utils.date_utils import safe_iso

GAMMA = 1.02
RELATIVE_ERROR = (GAMMA - 1.0) / (GAMMA + 1.0)
_LOG_GAMMA = math.log(GAMMA)


def _centroid(value: float) -> float:
    if value == 0.0:
        return 0.0
    magnitude = abs(value)
    index = math.ceil(math.log(magnitude) / _LOG_GAMMA)
    centre = 2.0 * GAMMA**index / (GAMMA + 1.0)
    return math.copysign(centre, value)


def sketch(values: list[float]) -> tuple[list[float], list[int]]:
    """Return ``(centroids, counts)`` sorted by centroid; every observation is counted once."""
    histogram = Counter(_centroid(value) for value in values)
    centroids = sorted(histogram)
    return centroids, [histogram[centroid] for centroid in centroids]


@overload
def range_value(value: str) -> datetime: ...
@overload
def range_value(value: None) -> None: ...
def range_value(value: str | None) -> datetime | None:
    """Parse an ISO-8601 bound as-is: ``Z`` becomes ``+00:00``; naive input stays naive."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def bucket(value: object) -> datetime | None:
    """Floor a datetime or ISO string to its UTC minute; ``None`` when unparseable."""
    if isinstance(value, datetime):
        timestamp = value.astimezone(UTC)
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    else:
        return None
    return timestamp.replace(second=0, microsecond=0)


def text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def finite_float(value: object) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def optional_int(value: object) -> int | None:
    parsed = finite_float(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def percentiles(values: list[float], qs: tuple[float, ...]) -> list[float | None]:
    """Linear-interpolated percentiles (each ``q`` in [0, 1]) from a single sort of ``values``."""
    if not values:
        return [None] * len(qs)
    ordered = sorted(values)
    out: list[float | None] = []
    for q in qs:
        position = (len(ordered) - 1) * q
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            out.append(ordered[lower])
        else:
            out.append(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))
    return out


MAX_PAGE_SIZE = 500


@dataclass(frozen=True)
class KeysetCursor:
    timestamp: datetime
    event_id: str
    service_id: str
    domain: str

    def encode(self, secret: bytes) -> str:
        if not secret:
            raise ValueError("cursor signing secret is required")
        payload = json.dumps(
            {
                "timestamp": self.timestamp.astimezone(UTC).isoformat(),
                "event_id": self.event_id,
                "service_id": self.service_id,
                "domain": self.domain,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        signature = hmac.new(secret, payload, hashlib.sha256).digest()
        return f"{_b64(payload)}.{_b64(signature)}"

    @classmethod
    def decode(cls, token: str, secret: bytes) -> KeysetCursor:
        if not secret:
            raise ValueError("cursor signing secret is required")
        try:
            payload_token, signature_token = token.split(".", 1)
            payload = _unb64(payload_token)
            signature = _unb64(signature_token)
            expected = hmac.new(secret, payload, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("cursor signature mismatch")
            raw = json.loads(payload)
            timestamp = datetime.fromisoformat(raw["timestamp"]).astimezone(UTC)
            if not all(isinstance(raw[key], str) and raw[key] for key in ("event_id", "service_id", "domain")):
                raise ValueError("cursor fields are invalid")
            return cls(timestamp, raw["event_id"], raw["service_id"], raw["domain"])
        except (ValueError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid cursor") from exc


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class ArchiveManifestCatalog(Protocol):
    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]: ...


def raw_query_producer(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    *,
    service_id: str,
    domain: str,
    start: datetime,
    end: datetime,
) -> Callable[[], Iterable[Mapping[str, Any]]]:
    """Build a producer scanning archived rows in ``[start, end)``."""

    def _produce() -> Iterator[dict[str, Any]]:
        manifests = sorted(
            catalog.manifests_covering(service_id, domain, start, end), key=lambda manifest: manifest.coverage_start
        )
        for manifest in manifests:
            payload = archive.read_artifact(manifest)
            table = pq.read_table(pa.BufferReader(payload))
            for row in table.to_pylist():
                timestamp = _row_timestamp(row)
                if timestamp is not None and (timestamp < start or timestamp >= end):
                    continue
                yield row

    return _produce


def _row_timestamp(row: Mapping[str, Any]) -> datetime | None:
    value = row.get("timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return range_value(value).astimezone(UTC)
    return None


_DOMAIN_IDENT_RE = re.compile(r"^[a-z0-9_]+$")
VISIBLE_PUBLICATION_FENCE_SQL = (
    "publication_state='visible' "
    "AND batch_id IN ("
    "SELECT batch_id FROM high_scale_batch_publications FINAL "
    "WHERE service_id={service_id:String} AND publication_state='visible')"
)


def visible_publication_fence_sql(domain: str | None = None) -> str:
    """Return the publication-fence SQL predicate, optionally scoped to ``domain``."""
    if domain is None:
        return VISIBLE_PUBLICATION_FENCE_SQL
    if not _DOMAIN_IDENT_RE.fullmatch(domain):
        raise ValueError(f"Invalid publication domain identifier: {domain!r}")
    return (
        "publication_state='visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' "
        "AND publication_state='visible')"
    )


def visible_fact_clauses(domain: str) -> list[str]:
    """Predicates (with a ``{service_id:String}`` parameter) limiting a fact table to visible batches of ``domain``."""
    if not _DOMAIN_IDENT_RE.fullmatch(domain):
        raise ValueError(f"Invalid publication domain identifier: {domain!r}")
    return [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' "
        "AND publication_state='visible')",
    ]


def parse_time_window(
    start_time: str | None,
    end_time: str | None,
    default: timedelta = timedelta(days=1),
    *,
    utc: bool = False,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Parse ``(start_time, end_time)`` ISO bounds, defaulting ``end`` to ``now`` and ``start`` to ``end - default``."""
    end = range_value(end_time) if end_time else (now or datetime.now(UTC))
    if utc:
        end = end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC)
    start = range_value(start_time) if start_time else (end - default)
    if utc:
        start = start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC)
    return start, end


def inclusive_minute_end(end: datetime) -> datetime:
    """Extend a whole-minute upper bound by 1 minute so ``event_timestamp < end_upper`` includes the minute."""
    return end + timedelta(minutes=1) if end.second == 0 and end.microsecond == 0 else end


_TIMESERIES_METRICS: dict[tuple[str, str], tuple[str, float]] = {
    ("ttfb", "p50"): ("latency_us", 0.5),
    ("ttfb", "p95"): ("latency_us", 0.95),
    ("ttfb", "p99"): ("latency_us", 0.99),
    ("ttlb", "p50"): ("ottlb_us", 0.5),
    ("ttlb", "p95"): ("ottlb_us", 0.95),
    ("ttlb", "p99"): ("ottlb_us", 0.99),
}


def format_iso_utc(value: Any) -> str | None:
    """Format a datetime or timestamp value as an ISO-8601 UTC ``...Z`` string."""
    if isinstance(value, datetime):
        stamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return stamp.isoformat().replace("+00:00", "Z")
    return safe_iso(value)


def format_iso_utc_or_empty(value: Any) -> str:
    """Format a datetime or timestamp value as an ISO-8601 UTC ``...Z`` string (or ``""``)."""
    return format_iso_utc(value) or ""


def _bucket_case(column: str, buckets: tuple[tuple[int, str], ...], last: str) -> str:
    """Build a ClickHouse ``multiIf(col <= limit, 'label', ..., 'last')`` expression."""
    arms = ", ".join(f"{column} <= {limit}, '{label}'" for limit, label in buckets)
    return f"multiIf({arms}, '{last}')"


def resolve_asn_display_labels(service_id: str, asns: Iterable[int]) -> dict[int, str]:
    """Return ``{asn: formatted_display_label}`` for the requested ASN numbers."""
    asn_list = list(asns)
    if not asn_list:
        return {}
    from backend.core import duckdb as _db

    try:
        names = _db.get_asn_names(service_id, asn_list)
    except Exception:
        names = {}
    return {asn: _db.format_asn_label(asn, names.get(asn, "")) for asn in asn_list}


_COLUMNS = {"client_ip": "client_ip", "ip": "client_ip", "country": "country", "url": "url"}
_ROUNDED = frozenset({"age", "ttl"})
_BOOLEAN = frozenset({"edge", "is_ipv6", "waf"})
_UNSUPPORTED = frozenset({"_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind", "edge_score_reason_ind"})


def _column(key: str, params: dict[str, Any], param: str) -> str:
    if key in _COLUMNS:
        return _COLUMNS[key]
    params[param] = key
    lookup = f"custom_fields[{{{param}:String}}]"
    if key in _ROUNDED:
        return f"toString(toInt32(round(toFloat64OrZero({lookup}))))"
    if key in _BOOLEAN:
        return f"if(lower({lookup}) IN ('true', '1'), 'true', if(lower({lookup}) IN ('false', '0'), 'false', {lookup}))"
    return lookup


def request_filter_clauses(
    filters: Mapping[str, Any] | None, *, prefix: str = "flt"
) -> tuple[list[str], dict[str, Any]]:
    """Return ``(clauses, params)`` AND-ing every non-empty filter in ``filters``."""
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        key = normalize_filter_key(raw_key)
        mode = str(filter_spec_attr(spec, "mode") or "include")
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if not (isinstance(value, str) and value.strip() == "")
        ]
        if not values:
            continue
        if key in _UNSUPPORTED or not key:
            raise ValueError(f"high-scale request readers do not support filtering on {raw_key!r}")
        column = _column(key, params, f"{prefix}_k{i}")
        concrete = [value for value in values if value is not None]
        names: list[str] = []
        for j, value in enumerate(concrete):
            name = f"{prefix}_{i}_{j}"
            str_val = str(value)
            if key in _BOOLEAN:
                lowered = str_val.strip().lower()
                if lowered in ("true", "1"):
                    str_val = "true"
                elif lowered in ("false", "0"):
                    str_val = "false"
            params[name] = str_val
            names.append(f"{{{name}:String}}")
        include = mode != "exclude"
        parts: list[str] = []
        if names:
            parts.append(f"{column} {'IN' if include else 'NOT IN'} ({', '.join(names)})")
        if len(concrete) != len(values):
            parts.append(f"{column} {'=' if include else '!='} ''")
        clauses.append(f"({(' OR ' if include else ' AND ').join(parts)})")
    return clauses, params


def build_clickhouse_filters(
    filters: Mapping[str, Any] | None, *, prefix: str = "filter"
) -> tuple[str, dict[str, Any]]:
    """Return ``(sql, params)`` where ``sql`` is ``1=1`` when no filter is active."""
    clauses, params = request_filter_clauses(filters, prefix=prefix)
    return (" AND ".join(clauses) if clauses else "1=1"), params


def build_published_projection_where(
    service_id: str, domain: str, start_time: str | None, end_time: str | None, *, dimension: str | None = None
) -> tuple[str, dict[str, Any]]:
    """Build ClickHouse WHERE predicate and params for published minute projections."""
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={publication_domain:String} "
        "AND publication_state='visible')",
    ]
    params: dict[str, Any] = {"service_id": service_id, "publication_domain": domain}
    start = range_value(start_time)
    end = range_value(end_time)
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start <= {end:DateTime64(3)}")
        params["end"] = end
    if dimension is not None:
        clauses.append("dimension={dimension:String}")
        params["dimension"] = dimension
    return " AND ".join(clauses), params


def build_published_facts_where(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    filters: Mapping[str, Any] | None = None,
    *,
    end_inclusive: bool = True,
    domain: str = "request",
) -> tuple[str, dict[str, Any]]:
    """Build ClickHouse WHERE predicate and params for published fact rows."""
    filter_sql, filter_params = build_clickhouse_filters(dict(filters or {}))
    clauses = ["service_id={service_id:String}", visible_publication_fence_sql(domain), filter_sql]
    params: dict[str, Any] = {"service_id": service_id, **filter_params}
    start = range_value(start_time)
    end = range_value(end_time)
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        op = "<=" if end_inclusive else "<"
        clauses.append(f"event_timestamp {op} {{end:DateTime64(3)}}")
        params["end"] = end
    return " AND ".join(clauses), params


sys.modules["backend.high_scale._filters"] = sys.modules[__name__]
_bucket = bucket
_self_mod = sys.modules[__name__]
for _early_alias in ("_common", "_filters"):
    sys.modules[f"backend.high_scale.{_early_alias}"] = _self_mod
from backend.high_scale.publication import (
    BaseClickHouseBatchAdapter,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    InsertReceipt,
    PostgresBatchManifestStore,
    PublicationResult,
    PublicationStatus,
    _batch_uuid,
    build_batch_id_from_manifest_id,
    row_matches_serving_domain,
)
from backend.models.performance import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

logger = logging.getLogger(__name__)
_worker_logger = logging.getLogger("backend.high_scale.worker")
_recovery_logger = structlog.get_logger(__name__)


# ── Registry Bootstrap (folded from registry_bootstrap.py) ────────────────────
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
    if source is None:
        return None
    bucket = source.get("bucket")
    if not isinstance(bucket, str) or not bucket:
        return None
    fos_client = _get_fos_client(source)
    archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
    return ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))


def _watermark(client: ServingClient, service_id: str, domain: str, *, owner_epoch: int) -> ServingWatermark:
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
_SOURCE_FIELD_OVERRIDES: dict[str, str] = {"client_ip": "ip", "cmcd_session": "sid"}


def compute_dimension_counts(domain: str, rows: tuple[Mapping[str, Any], ...]) -> tuple[dict[str, Any], ...]:
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
    """Batch rows carry bucket_start as an ISO string (HighScaleBatch.digest     requires JSON-serializable row values) — parse back for the insert."""
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
            table=table, columns=columns, mapped_rows=mapped_rows, batch=batch, batch_uuid=batch_uuid
        )


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
                batch_id=batch_id, status=PublicationStatus.VISIBLE, rows_visible=0, duplicate=False
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
                        service_id, desc.object_key, gen, manifest.manifest_id, owner.owner_epoch
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
        self, service_id: str, domain: str, manifest_id: str, owner_epoch: int, events: Sequence[dict[str, Any]]
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
                e["ngwaf_bot"] = bot["name"]
                if bot["cat"]:
                    e["ngwaf_bot_category"] = bot["cat"]
                if bot["wk"]:
                    e["ngwaf_wellknown_bot"] = bot["wk"]
    except Exception:
        logger.warning("high-scale NGWAF bot enrichment failed", exc_info=True)


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


# ── Worker Loop (folded from backend.high_scale.worker) ──────────────────────
# ── Folded from backend/high_scale/orchestration.py ──────────────────────────
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Protocol

MAX_SOURCE_PAGE_SIZE = 500
INITIAL_SOURCE_CURSOR = "__initial__"
TERMINAL_SOURCE_CURSOR_PREFIX = "__terminal__:"
RUM_REINSPECTION_INTERVAL = 10
RUM_SOURCE_DOMAINS = frozenset({"rum_vitals", "rum_errors"})
sys.modules["backend.high_scale.orchestration"] = sys.modules[__name__]
sys.modules["backend.high_scale.source_discovery"] = sys.modules[__name__]

from backend.high_scale.deletion import (  # noqa: E402
    DeletionController,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from backend.high_scale.historical_jobs import (  # noqa: E402
    ProjectionReplayResult,
    _batch_from_rows,
    _load_manifest_request_events,
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
)

logger = logging.getLogger(__name__)


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
        resolved_prefix = self._key(domain_prefix)
        if reinspect and domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        if forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        listing_cursor = (reinspection_cursor or INITIAL_SOURCE_CURSOR) if reinspect else cursor
        paginator = self._client.get_paginator("list_objects_v2")
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
        page = next(iter(paginator.paginate(**request)))
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
            next_reinspection_cursor = (
                f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
                if has_more and objects
                else INITIAL_SOURCE_CURSOR
            )
            return SourceObjectPage(
                objects,
                cursor or INITIAL_SOURCE_CURSOR,
                next_reinspection_cursor=next_reinspection_cursor,
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
            error_code = _client_error_code(exc)
            if error_code in {"NoSuchKey", "404"}:
                raise SourceObjectMissingError(f"source object is missing from storage: {object_key}") from exc
            raise
        return response["Body"].read()


def _client_error_code(exc: Exception) -> str | None:
    """Extract the S3/boto3 error code without importing botocore directly."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
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


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.source_discovery"] = _self_mod


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
        owner = self._owner(service_id)
        current_owner, epoch, persisted_cursor = _owner_fields(owner)
        self._require_high_scale(current_owner, service_id)
        if cursor is None:
            cursor_reader = self._control if self._control is not None else self._ownership
            if hasattr(cursor_reader, "source_cursor_for"):
                persisted_cursor = cursor_reader.source_cursor_for(service_id, domain)
        listing_cursor = persisted_cursor if cursor is None else cursor
        reinspection_cursor = INITIAL_SOURCE_CURSOR
        forward_pages_since_reinspection = 0
        has_reinspection_state = False
        should_reinspect = False
        if self._control is not None and domain in RUM_SOURCE_DOMAINS:
            reinspection_cursor, forward_pages_since_reinspection = self._control.source_reinspection_state_for(
                service_id, domain
            )
            has_reinspection_state = True
            should_reinspect = forward_pages_since_reinspection >= RUM_REINSPECTION_INTERVAL
        listing_options: dict[str, Any] = {}
        if has_reinspection_state:
            listing_options = {
                "reinspection_cursor": reinspection_cursor,
                "reinspect": should_reinspect,
                "forward_pages_since_reinspection": forward_pages_since_reinspection,
            }
        page = self._lister.list_source_objects(
            service_id, domain, page_size=self._page_size, cursor=listing_cursor, **listing_options
        )
        objects = tuple(page.objects)
        if len(objects) > self._page_size:
            raise ValueError("source object listing exceeded page_size")

        expired_sources = self._expired_claim_descriptors(service_id, domain)
        discovered_by_key: dict[str, Any] = {
            source.object_key: self._discover(service_id, domain, source, current_owner, epoch) for source in objects
        }

        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                next_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
            elif listing_cursor and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                next_cursor = listing_cursor

        processed = 0
        duplicates = 0
        failed = 0
        missing = 0

        active_sources: list[SourceObjectDescriptor] = list(expired_sources)
        active_keys = {s.object_key for s in active_sources}
        for s in objects:
            if s.object_key in active_keys:
                continue
            rec = discovered_by_key.get(s.object_key)
            rec_status = getattr(rec, "status", None)
            if isinstance(rec_status, str):
                is_terminal = rec_status in _TERMINAL_SOURCE_STATES
            else:
                is_terminal = self._already_terminal(service_id, s.object_key)
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
                keys = tuple(s.object_key for s in active_sources)
                claim_batch_fn = getattr(self._control, "claim_sources_batch")
                claims = claim_batch_fn(
                    service_id,
                    keys,
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
                claimed_keys = set()
                for s in active_sources:
                    c = self._ledger.claim(service_id, s.object_key, self._worker_id)
                    if c.claimed:
                        claimed_keys.add(s.object_key)

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
                        data = self._reader.read_source_object(service_id, domain, src.object_key)
                        return (src, data, None, False)
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
                        batch_next_cursor = None if failed > 0 or page.reinspection_page else next_cursor
                        batch_res = self._controller.ingest_batch(
                            service_id=service_id,
                            domain=domain,
                            sources_payloads=valid_payloads,
                            next_cursor=batch_next_cursor,
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
            if page.reinspection_page and has_reinspection_state and self._control is not None:
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
                    reinspection_cursor=page.next_reinspection_cursor if has_reinspection_state else None,
                    forward_pages_since_reinspection=(
                        page.forward_pages_since_reinspection if has_reinspection_state else None
                    ),
                )
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed, missing)

    def _expired_claim_descriptors(self, service_id: str, domain: str) -> list[SourceObjectDescriptor]:
        source_store = self._control if self._control is not None else self._ledger
        expired_claims = getattr(source_store, "expired_claims", None)
        if expired_claims is None:
            return []
        return [
            SourceObjectDescriptor(
                object_key=source.object_key,
                checksum=source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
            )
            for source in expired_claims(service_id, domain, limit=self._page_size)
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
            service_id, domain, source.object_key, source.checksum, size_bytes=source.size_bytes, version=source.version
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
            state_kwargs: dict[str, Any] = {}
            if reinspection_cursor is not None and forward_pages_since_reinspection is not None:
                state_kwargs = {
                    "reinspection_cursor": reinspection_cursor,
                    "forward_pages_since_reinspection": forward_pages_since_reinspection,
                }
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

_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.source_discovery"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "source_discovery", _self_mod)
from backend.high_scale.postgres_control import PostgresControlPlane  # noqa: E402


@dataclass(frozen=True)
class WorkerPageResult:
    service_id: str
    domain: str
    page: PageRun | None
    error: Exception | None = None


class FairPageSelector:
    """Chooses which (service, domain) pages run each tick with weighted fairness."""

    def __init__(
        self,
        pairs: tuple[tuple[str, str], ...],
        *,
        capacity: int | None = None,
        weights: Mapping[str, int] | None = None,
    ) -> None:
        services = {service_id for service_id, _ in pairs}
        if capacity is not None and capacity < 0:
            raise ValueError("pages per tick must be non-negative")
        if capacity and capacity < len(services):
            raise ValueError("pages per tick must be at least the number of services")
        self._pairs = pairs
        self._order = {pair: index for index, pair in enumerate(pairs)}
        self._capacity = capacity or None
        self._services = tuple(sorted(services))
        self._weights: dict[str, int] = {}
        for service_id in self._services:
            w = (weights or {}).get(service_id, 1)
            if w <= 0:
                raise ValueError("service weight must be positive")
            self._weights[service_id] = w
        self._queues: dict[str, deque[str]] = {service_id: deque() for service_id in self._services}
        self._deficits: dict[str, int] = {service_id: 0 for service_id in self._services}
        self._queued: set[tuple[str, str]] = set()

    def select(self) -> list[tuple[str, str]]:
        for pair in self._pairs:
            if pair not in self._queued:
                self._queued.add(pair)
                self._queues[pair[0]].append(pair[1])
        capacity = self._capacity if self._capacity is not None else len(self._pairs)
        selected: list[tuple[str, str]] = []
        for service_id in self._services:
            if len(selected) >= capacity:
                break
            queue = self._queues[service_id]
            if queue:
                selected.append((service_id, queue.popleft()))
                self._deficits[service_id] += self._weights[service_id] - 1
        while len(selected) < capacity and any(self._queues.values()):
            progressed = False
            for service_id in self._services:
                queue = self._queues[service_id]
                while queue and self._deficits[service_id] >= 1 and len(selected) < capacity:
                    selected.append((service_id, queue.popleft()))
                    self._deficits[service_id] -= 1
                    progressed = True
            if not progressed:
                for service_id, queue in self._queues.items():
                    if queue:
                        self._deficits[service_id] += self._weights[service_id]
        self._queued.difference_update(selected)
        selected.sort(key=self._order.__getitem__)
        if self._queued:
            _worker_logger.info(
                "high_scale.fair_dispatch",
                extra={"dispatched": len(selected), "deferred": len(self._queued), "capacity": capacity},
            )
        return selected


def _weights_from_env() -> dict[str, int]:
    weights: dict[str, int] = {}
    for entry in _csv_env("HIGH_SCALE_SERVICE_WEIGHTS"):
        service_id, sep, raw = entry.partition("=")
        if not sep or not service_id.strip() or not raw.strip().isdigit() or int(raw) <= 0:
            raise ValueError("HIGH_SCALE_SERVICE_WEIGHTS entries must look like service=positive_integer")
        weights[service_id.strip()] = int(raw)
    return weights


class HighScaleWorkerLoop:
    """Runs bounded source pages without coupling high-scale work to APScheduler."""

    def __init__(
        self,
        *,
        coordinator: HighScaleWorkerCoordinator | None = None,
        coordinators: tuple[tuple[str, HighScaleWorkerCoordinator], ...] | None = None,
        service_ids: tuple[str, ...] = (),
        domains: tuple[str, ...] = ("request", "rum", "cmcd"),
        interval_seconds: float = 5.0,
        stop_event: Event | None = None,
        sleeper: Any = time.sleep,
        deletion_sweeper: HighScaleDeletionSweeper | None = None,
        deletion_sweepers: tuple[tuple[str, HighScaleDeletionSweeper | None], ...] | None = None,
        deletion_sweep_limit: int = 100,
        pages_per_tick: int | None = None,
        service_weights: Mapping[str, int] | None = None,
    ) -> None:
        if coordinators is not None:
            resolved_coordinators = coordinators
            resolved_service_ids = tuple(service_id for service_id, _ in coordinators)
        else:
            if not service_ids:
                raise ValueError("at least one high-scale service is required")
            if coordinator is None:
                raise ValueError("coordinator is required")
            resolved_service_ids = service_ids
            resolved_coordinators = tuple((service_id, coordinator) for service_id in service_ids)
        if not resolved_service_ids:
            raise ValueError("at least one high-scale service is required")
        if not domains:
            raise ValueError("at least one high-scale domain is required")
        if interval_seconds < 0:
            raise ValueError("worker interval must be non-negative")
        self._coordinator = coordinator
        self._coordinators = resolved_coordinators
        self._service_ids = resolved_service_ids
        self._domains = domains
        self._interval_seconds = interval_seconds
        self._stop_event = stop_event or Event()
        self._sleeper = sleeper
        self._deletion_sweeper = deletion_sweeper
        self._deletion_sweepers = (
            deletion_sweepers
            if deletion_sweepers is not None
            else tuple((service_id, deletion_sweeper) for service_id in resolved_service_ids)
        )
        self._deletion_sweep_limit = deletion_sweep_limit
        self._deletion_thread: Thread | None = None
        self._last_system_cleanup = 0.0
        self._selector = FairPageSelector(
            tuple((service_id, domain) for service_id in resolved_service_ids for domain in domains),
            capacity=pages_per_tick,
            weights=service_weights,
        )

    def run_once(self) -> tuple[WorkerPageResult, ...]:
        results: list[WorkerPageResult] = []
        coordinators = dict(self._coordinators)
        for service_id, domain in self._selector.select():
            try:
                page = coordinators[service_id].run_page(service_id=service_id, domain=domain)
            except Exception as exc:
                _worker_logger.exception(
                    "high-scale worker page failed", extra={"service_id": service_id, "domain": domain}
                )
                results.append(WorkerPageResult(service_id, domain, None, exc))
            else:
                results.append(WorkerPageResult(service_id, domain, page))
        self._run_deletion_sweeps()
        self._run_system_cleanups()
        return tuple(results)

    def _deletion_targets(self) -> tuple[tuple[str, HighScaleDeletionSweeper], ...]:
        return tuple((service_id, sweeper) for service_id, sweeper in self._deletion_sweepers if sweeper is not None)

    def _run_deletion_sweeps(self) -> None:
        targets = self._deletion_targets()
        if not targets:
            return
        if self._deletion_thread is not None and self._deletion_thread.is_alive():
            return
        self._deletion_thread = Thread(
            target=self._sweep_targets, args=(targets,), name="high-scale-deletion-sweep", daemon=True
        )
        self._deletion_thread.start()

    def _sweep_targets(self, targets: tuple[tuple[str, HighScaleDeletionSweeper], ...]) -> None:
        for service_id, sweeper in targets:
            try:
                sweeper.sweep(service_id=service_id, limit=self._deletion_sweep_limit)
            except Exception:
                _worker_logger.exception("high-scale deletion sweep failed", extra={"service_id": service_id})

    def wait_for_deletion_sweep(self, timeout: float | None = None) -> None:
        if self._deletion_thread is not None:
            self._deletion_thread.join(timeout)

    def _run_system_cleanups(self) -> None:
        now = time.time()
        if now - self._last_system_cleanup < 300:
            return
        self._last_system_cleanup = now
        for service_id, _ in self._coordinators:
            try:
                from backend.utils.telemetry import run_usage_log_cleanup

                run_usage_log_cleanup(service_id)
            except Exception:
                _worker_logger.debug("periodic usage log cleanup failed", extra={"service_id": service_id})
        try:
            from backend.core.clickhouse_client import get_clickhouse_client
            from backend.core.clickhouse_schema import maintain_clickhouse_system_tables

            client = get_clickhouse_client()
            if client is not None:
                maintain_clickhouse_system_tables(client)
        except Exception:
            _worker_logger.debug("periodic clickhouse system tables maintenance failed")

    def _has_full_page(self, results: tuple[WorkerPageResult, ...]) -> bool:
        if not self._coordinators:
            return False
        coord_map = dict(self._coordinators)
        for r in results:
            if r.page is not None:
                c = coord_map.get(r.service_id)
                page_size = getattr(c, "_page_size", None) if c else None
                if page_size is not None and page_size > 0 and r.page.discovered >= page_size:
                    return True
        return False

    def run(self, *, max_iterations: int | None = None) -> None:
        if max_iterations is not None and max_iterations < 0:
            raise ValueError("max_iterations must be non-negative")
        iterations = 0
        while not self._stop_event.is_set():
            results = self.run_once()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                self.wait_for_deletion_sweep()
                return
            if self._interval_seconds > 0:
                if not self._has_full_page(results):
                    sleep_dur = random.uniform(0.5, 1.0) * self._interval_seconds
                    self._sleeper(sleep_dur)

    def stop(self) -> None:
        self._stop_event.set()


def build_worker_from_environment() -> HighScaleWorkerLoop:
    """Build the durable FOS → archive → ClickHouse worker from process config."""
    from backend.core.duckdb import _get_fos_client, get_source_for_service

    high_scale_config = from_environment()
    if not high_scale_config.enabled:
        raise RuntimeError("high-scale worker requires HIGH_SCALE_ENABLED=1")
    topology = high_scale_config.topology_profile()
    _worker_logger.info(
        "high_scale.topology_profile",
        extra={
            "profile": topology.name,
            "clickhouse_replicas_per_shard": topology.clickhouse_replicas_per_shard,
            "keeper_members": topology.keeper_members,
            "production_ha": topology.production_ha,
            "shard_count": topology.shard_count,
        },
    )
    service_ids = _csv_env("HIGH_SCALE_SERVICE_IDS")
    if not service_ids:
        service_ids = tuple(
            cfg["service_id"] for cfg in app_config.list_configs() if isinstance(cfg, dict) and cfg.get("service_id")
        )
    if not service_ids:
        raise ValueError("HIGH_SCALE_SERVICE_IDS is required (none found in configs/)")
    worker_id = (
        os.getenv("HIGH_SCALE_WORKER_ID", "").strip()
        or os.getenv("POD_NAME", "").strip()
        or f"high-scale-{os.getpid()}"
    )
    page_size = int(os.getenv("HIGH_SCALE_PAGE_SIZE", "100"))
    lease_seconds = float(os.getenv("HIGH_SCALE_LEASE_SECONDS", "300"))
    object_concurrency = int(os.getenv("HIGH_SCALE_OBJECT_CONCURRENCY", "4"))
    interval_seconds = float(os.getenv("HIGH_SCALE_WORKER_INTERVAL_SECONDS", "5"))
    domains = tuple(_csv_env("HIGH_SCALE_DOMAINS") or ("request", "rum_vitals", "rum_errors"))
    control = PostgresControlPlane()
    control.create_schema()
    for service_id in service_ids:
        control.initialize_owner(
            service_id, owner="high_scale", source_cursor=os.getenv("HIGH_SCALE_INITIAL_CURSOR", "__initial__")
        )
    clickhouse_settings = app_config.load_clickhouse_config()
    if clickhouse_settings is None:
        raise RuntimeError("high-scale worker requires CLICKHOUSE_ENABLED=true")
    clickhouse = ClickHouseClient(clickhouse_settings)
    serving = ClickHousePublication(PostgresBatchManifestStore(control), ClickHouseBatchAdapter(clickhouse))
    aggregates_enabled = os.getenv("HIGH_SCALE_AGGREGATES_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    aggregates = (
        ClickHousePublication(PostgresBatchManifestStore(control), AggregateBatchAdapter(clickhouse))
        if aggregates_enabled
        else None
    )
    deletion_enabled = os.getenv("HIGH_SCALE_DELETION_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    deletion_sweep_limit = int(os.getenv("HIGH_SCALE_DELETION_SWEEP_LIMIT", "100"))
    coordinators: list[HighScaleWorkerCoordinator] = []
    sweepers: list[HighScaleDeletionSweeper | None] = []
    for service_id in service_ids:
        source = get_source_for_service(service_id)
        if source is None:
            raise ValueError(f"no configured FOS source for high-scale service {service_id}")
        fos_client = _get_fos_client(source)
        bucket = source.get("bucket")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError(f"FOS bucket is missing for high-scale service {service_id}")
        archive_prefix = os.getenv("HIGH_SCALE_ARCHIVE_PREFIX", "high-scale/archive")
        archive = ArchivePublication(S3ObjectStore(fos_client, bucket=bucket, prefix=archive_prefix))
        controller = HighScaleIngestController(
            ownership=control,  # type: ignore[arg-type]
            ledger=control,  # type: ignore[arg-type]
            control_plane=control,
            archive=archive,
            serving=serving,
            aggregates=aggregates,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            deletion_grace_seconds=int(os.getenv("HIGH_SCALE_DELETION_GRACE_SECONDS", "900")),
        )
        coordinators.append(
            HighScaleWorkerCoordinator(
                lister=S3SourceObjectLister(fos_client, bucket=bucket, prefix=str(source.get("prefix") or "")),
                reader=S3SourceObjectReader(fos_client, bucket=bucket),
                controller=controller,
                ownership=control,
                ledger=control,
                worker_id=worker_id,
                control_plane=control,
                page_size=page_size,
                lease_seconds=lease_seconds,
                object_concurrency=object_concurrency,
            )
        )
        if deletion_enabled:
            raw_store = S3ObjectStore(fos_client, bucket=bucket)
            deletion_ledger = PostgresDeletionLedger(control)
            deletion_controller = DeletionController(raw_store, archive, deletion_ledger, ownership=deletion_ledger)
            sweepers.append(HighScaleDeletionSweeper(control=control, controller=deletion_controller))
        else:
            sweepers.append(None)
    return _MultiServiceWorkerLoop(
        coordinators=tuple(zip(service_ids, coordinators, strict=True)),
        deletion_sweepers=tuple(zip(service_ids, sweepers, strict=True)),
        deletion_sweep_limit=deletion_sweep_limit,
        domains=domains,
        interval_seconds=interval_seconds,
        pages_per_tick=int(os.getenv("HIGH_SCALE_PAGES_PER_TICK", "0") or "0") or None,
        service_weights=_weights_from_env(),
    )


_MultiServiceWorkerLoop = HighScaleWorkerLoop


def _csv_env(name: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.getenv(name, "").split(",") if value.strip())


def main() -> None:
    if os.environ.get("OTEL_EXPORTER") == "prometheus":
        try:
            from prometheus_client import start_http_server

            logging.info("Starting Prometheus metrics server on port 8000")
            start_http_server(8000)
        except Exception as e:
            logging.warning(f"Failed to start prometheus_client: {e}")
    parser = ArgumentParser(description="Run the bounded high-scale source worker")
    parser.add_argument("--once", action="store_true", help="process one bounded page per service and domain")
    parser.add_argument("--iterations", type=int, default=None)
    args = parser.parse_args()
    worker_loop = build_worker_from_environment()
    worker_loop.run(max_iterations=1 if args.once else args.iterations)


# ── FOS-Authoritative Recovery (folded from backend.high_scale.recovery) ─────
@dataclass(frozen=True)
class RecoveryCapacityMeasurement:
    events_per_second: int
    recovery_window_seconds: int
    archive_read_events_per_second: int
    decode_events_per_second: int
    aggregate_events_per_second: int
    clickhouse_insert_events_per_second: int
    replication_factor: int
    live_ingest_reservation: float

    def validate(self) -> None:
        if self.events_per_second <= 0 or self.recovery_window_seconds <= 0:
            raise ValueError("event rate and recovery window must be positive")
        if (
            min(
                self.archive_read_events_per_second,
                self.decode_events_per_second,
                self.aggregate_events_per_second,
                self.clickhouse_insert_events_per_second,
            )
            <= 0
        ):
            raise ValueError("all recovery throughput measurements must be positive")
        if self.replication_factor < 1:
            raise ValueError("replication factor must be positive")
        if not 0 <= self.live_ingest_reservation < 1:
            raise ValueError("live ingest reservation must be in [0, 1)")


@dataclass(frozen=True)
class RecoveryCapacityReport:
    required_events_per_second: float
    limiting_stage: str
    limiting_events_per_second: int
    replication_adjusted_insert_events_per_second: float
    feasible: bool


def evaluate_recovery_capacity(measurement: RecoveryCapacityMeasurement) -> RecoveryCapacityReport:
    """Evaluate a measured recovery profile without claiming unmeasured capacity."""
    measurement.validate()
    required = (
        measurement.events_per_second
        * 86_400
        / measurement.recovery_window_seconds
        / (1 - measurement.live_ingest_reservation)
    )
    stages = {
        "archive_read": measurement.archive_read_events_per_second,
        "decode": measurement.decode_events_per_second,
        "aggregate_rebuild": measurement.aggregate_events_per_second,
        "clickhouse_insert": measurement.clickhouse_insert_events_per_second // measurement.replication_factor,
    }
    limiting_stage, limiting_rate = min(stages.items(), key=lambda item: item[1])
    replication_adjusted = measurement.clickhouse_insert_events_per_second / measurement.replication_factor
    feasible = limiting_rate >= required
    return RecoveryCapacityReport(required, limiting_stage, limiting_rate, replication_adjusted, feasible)


@dataclass(frozen=True)
class RecoveryReport:
    """Recovery availability is reported per capability, not as one flag."""

    ingest_accepted: bool
    triage_available: bool
    raw_query_available: bool
    warm_history_available: bool
    full_rebuild_complete: bool
    manifests_replayed: int
    rows_replayed: int
    errors: tuple[str, ...]
    rebuild_id: str
    capacity: RecoveryCapacityReport | None = None
    recovery_claim_valid: bool | None = None


def _evaluate_capacity_gate(
    measurement: RecoveryCapacityMeasurement | None, *, service_id: str, domain: str, rebuild_id: str
) -> RecoveryCapacityReport | None:
    """Evaluate the measured profile before any replay work starts."""
    if measurement is None:
        return None
    report = evaluate_recovery_capacity(measurement)
    rec_logger = structlog.get_logger(__name__)
    log = rec_logger.info if report.feasible else rec_logger.warning
    log(
        "high_scale.recovery_capacity_gate",
        service_id=service_id,
        domain=domain,
        rebuild_id=rebuild_id,
        feasible=report.feasible,
        limiting_stage=report.limiting_stage,
        limiting_events_per_second=report.limiting_events_per_second,
        required_events_per_second=report.required_events_per_second,
    )
    return report


def rebuild_serving_state_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    domain: str,
    now: datetime | None = None,
    recent_window: timedelta = timedelta(hours=24),
    warm_window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    if recent_window <= timedelta(0):
        raise ValueError("recent_window must be positive")
    if warm_window <= recent_window:
        raise ValueError("warm_window must exceed recent_window")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    recent_start = observed - recent_window
    warm_start = observed - warm_window
    resolved_rebuild_id = rebuild_id or uuid4().hex
    capacity_report = _evaluate_capacity_gate(
        capacity, service_id=service_id, domain=domain, rebuild_id=resolved_rebuild_id
    )
    recent_manifests = _sorted(catalog.manifests_covering(service_id, domain, recent_start, observed))
    warm_manifests = _sorted(catalog.manifests_covering(service_id, domain, warm_start, recent_start))
    recent_rows, recent_replayed, recent_errors = _replay_all(
        recent_manifests, archive, publication, resolved_rebuild_id
    )
    warm_rows, warm_replayed, warm_errors = _replay_all(warm_manifests, archive, publication, resolved_rebuild_id)
    triage_available = not recent_errors
    warm_available = not warm_errors
    return RecoveryReport(
        ingest_accepted=True,
        triage_available=triage_available,
        raw_query_available=triage_available,
        warm_history_available=warm_available,
        full_rebuild_complete=triage_available and warm_available,
        manifests_replayed=recent_replayed + warm_replayed,
        rows_replayed=recent_rows + warm_rows,
        errors=tuple(recent_errors) + tuple(warm_errors),
        rebuild_id=resolved_rebuild_id,
        capacity=capacity_report,
        recovery_claim_valid=(
            None if capacity_report is None else capacity_report.feasible and triage_available and warm_available
        ),
    )


class _SqlClient(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> list[dict]: ...


_ORIGIN_PROJECTION_TABLES = ("origin_minute_summary", "origin_minute_dimensions")


def purge_origin_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's Origin projection rows from ``since`` onward, synchronously."""
    for table in _ORIGIN_PROJECTION_TABLES:
        client.execute(
            f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}} "
            "AND bucket_start >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
            {"service_id": service_id, "since": since},
        )


_RUM_FACT_TABLES = {"rum_vitals": "rum_vitals_facts", "rum_errors": "rum_error_facts"}


def purge_rum_facts(client: _SqlClient, *, domain: str, service_id: str, since: datetime) -> None:
    """Delete one service's RUM fact rows of ``domain`` from ``since`` onward, synchronously."""
    table = _RUM_FACT_TABLES.get(domain)
    if table is None:
        raise ValueError("domain must be rum_vitals or rum_errors")
    client.execute(
        f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}} "
        "AND event_timestamp >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
        {"service_id": service_id, "since": since},
    )


def rebuild_rum_facts_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    domain: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    """Replay one RUM domain over ``window`` (newest day first) through the fact writer."""
    if domain not in _RUM_FACT_TABLES:
        raise ValueError("domain must be rum_vitals or rum_errors")
    return rebuild_serving_state_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        domain=domain,
        now=now,
        recent_window=min(timedelta(hours=24), window / 2),
        warm_window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def purge_cmcd_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's CMCD projection rows from ``since`` onward, synchronously."""
    client.execute(
        "ALTER TABLE `cmcd_projection_facts` DELETE WHERE service_id={service_id:String} "
        "AND event_timestamp >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
        {"service_id": service_id, "since": since},
    )


def _rebuild_request_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    projection_label: str,
    replay_fn: Callable[..., ProjectionReplayResult],
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    if window <= timedelta(0):
        raise ValueError("window must be positive")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    resolved_rebuild_id = rebuild_id or uuid4().hex
    capacity_report = _evaluate_capacity_gate(
        capacity, service_id=service_id, domain="request", rebuild_id=resolved_rebuild_id
    )
    manifests = _sorted(catalog.manifests_covering(service_id, "request", observed - window, observed))
    rows = 0
    replayed = 0
    errors: list[str] = []
    for manifest in manifests:
        try:
            result = replay_fn(manifest, archive, publication, rebuild_id=resolved_rebuild_id)
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild {projection_label} projections: {exc}")
            continue
        rows += result.rows_published
        replayed += 1
    available = not errors
    return RecoveryReport(
        ingest_accepted=True,
        triage_available=available,
        raw_query_available=True,
        warm_history_available=available,
        full_rebuild_complete=available,
        manifests_replayed=replayed,
        rows_replayed=rows,
        errors=tuple(errors),
        rebuild_id=resolved_rebuild_id,
        capacity=capacity_report,
        recovery_claim_valid=None if capacity_report is None else capacity_report.feasible and available,
    )


def rebuild_cmcd_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="CMCD",
        replay_fn=replay_cmcd_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def rebuild_origin_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Origin",
        replay_fn=replay_origin_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def rebuild_performance_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    """Populate the Performance waterfall/scatter projections for already-ingested history."""
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Performance",
        replay_fn=replay_performance_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def replay_security_projections(
    manifest: ArchiveManifest, archive: ArchivePublication, publication: ClickHousePublication, *, rebuild_id: str
) -> ProjectionReplayResult:
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Security")
    _enrich_high_scale_ngwaf_bots(serving_rows)
    projection = build_security_projection_rows(serving_rows)
    published = 0
    if projection.dimension_rows:
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-security:{rebuild_id}:{manifest.manifest_id}:dimensions",
                manifest,
                projection.dimension_rows,
                domain="security_dimensions",
            )
        )
        published = result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def rebuild_security_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    """Populate ``security_dimensions`` from FOS archive manifests over ``window``."""
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Security",
        replay_fn=replay_security_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def _sorted(manifests: tuple[ArchiveManifest, ...]) -> tuple[ArchiveManifest, ...]:
    return tuple(sorted(manifests, key=lambda manifest: manifest.coverage_start))


def _replay_all(
    manifests: tuple[ArchiveManifest, ...],
    archive: ArchivePublication,
    publication: ClickHousePublication,
    rebuild_id: str,
) -> tuple[int, int, list[str]]:
    rows = 0
    replayed = 0
    errors: list[str] = []
    for manifest in manifests:
        try:
            result = replay_manifest(
                manifest, archive, publication, batch_id=f"rebuild:{rebuild_id}:{manifest.manifest_id}"
            )
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to replay: {exc}")
            continue
        rows += result.rows_read
        replayed += 1
    return rows, replayed, errors


# ── Folded from backend/high_scale/rum.py ────────────────────────────────────

import contextvars  # noqa: E402

_iso = format_iso_utc_or_empty
from backend.high_scale.publication import (  # noqa: E402
    _DOMAIN_COLUMNS,
    _DOMAIN_TABLES,
    PartialInsert,
    _rows_for_domain,
)
from backend.high_scale.replay import _read_manifest_rows  # noqa: E402

_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "pathname", "city", "region", "pop", "tls"})
_BEACON_IDENTITY = """
        coalesce(nullIf(toString(request_event_id), ''),
                 concat(client_id, '_', toString(toInt64(round(toUnixTimestamp64Milli(event_timestamp) / 1000.0)))))
    """

from pydantic import TypeAdapter, ValidationError  # noqa: E402

_RUM_FILTERS_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])


def parse_rum_filters(raw: str | None) -> dict[str, Any]:
    """Decode the ``filters`` query parameter (a JSON object of ``{column: {mode, values}}``)."""
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("filters must be valid JSON") from exc
    try:
        return _RUM_FILTERS_ADAPTER.validate_python(payload)
    except ValidationError as exc:
        raise ValueError("filters must be a JSON object") from exc


def _filter_clauses(filters: Mapping[str, Any] | None) -> tuple[list[str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    unsupported: list[str] = []
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        if not values:
            continue
        column = normalize_filter_key(raw_key)
        if column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        names: list[str] = []
        for j, value in enumerate(values):
            name = f"flt_{i}_{j}"
            params[name] = str(value)
            names.append(f"{{{name}:String}}")
        operator = "NOT IN" if str(filter_spec_attr(spec, "mode") or "include") == "exclude" else "IN"
        clauses.append(f"{column} {operator} ({', '.join(names)})")
    if unsupported:
        raise ValueError(f"high-scale RUM cannot filter on: {', '.join(sorted(unsupported))}")
    return clauses, params


def rum_beacon_health(service: HighScaleService, *, source: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(source, dict) and (
        (isinstance(source.get("rum"), dict) and source["rum"].get("enabled") is False)
        or source.get("rum_enabled") is False
    ):
        return {
            "enabled": False,
            "has_data": False,
            "beacons": 0,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM is not enabled for this service.",
        }

    try:
        res = service.client.execute(
            f"SELECT sum(event_count) as c, max(bucket_start) as last_ts FROM rum_vitals_aggregates WHERE service_id = {{service_id:String}} AND dimension = 'metric_name' AND {visible_publication_fence_sql('rum_vitals_aggregate')}",
            {"service_id": service.service_id},
        )
        row = res[0] if res else {}
        beacons = int(row.get("c") or 0)
        last_ts = row.get("last_ts") if beacons > 0 else None
    except Exception:
        res = service.client.execute(
            f"SELECT count() as c, max(event_timestamp) as last_ts FROM rum_vitals_facts WHERE service_id = {{service_id:String}} AND {visible_publication_fence_sql('rum_vitals')}",
            {"service_id": service.service_id},
        )
        row = res[0] if res else {}
        beacons = int(row.get("c") or 0)
        last_ts = row.get("last_ts") if beacons > 0 else None
    has_data = beacons > 0
    last_beacon_time = (_iso(last_ts) or None) if last_ts is not None else None
    return {
        "enabled": True,
        "has_data": has_data,
        "beacons": beacons,
        "beacon_fire_rate": beacons,
        "recent_beacons": beacons,
        "last_beacon_time": last_beacon_time,
        "setup_complete": has_data,
        "message": (
            f"RUM is working! Received {beacons} beacon(s) in the last 15 minutes."
            if has_data
            else "No beacons received in the last 15 minutes. Ensure the RUM script is installed and your site is receiving traffic."
        ),
    }


def rum_analytics(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Serve the standard RUM wire contract from visible, time-filtered facts."""
    start, end = parse_time_window(start_time, end_time, utc=True, now=datetime.now(UTC))
    if end < start:
        raise ValueError("RUM end time precedes start time")
    filter_clauses, filter_params = _filter_clauses(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}

    def scope(domain: str) -> str:
        return " AND ".join(
            [
                *visible_fact_clauses(domain),
                "event_timestamp >= {start:DateTime64(3, 'UTC')}",
                "event_timestamp <= {end:DateTime64(3, 'UTC')}",
                *filter_clauses,
            ]
        )

    where_vitals = scope("rum_vitals")
    where_errors = scope("rum_errors")
    identity = _BEACON_IDENTITY
    events = f"""
        WITH events AS (
            SELECT event_timestamp, {identity} AS beacon_id, 'vitals' AS src,
                   metric_name, metric_value, pathname
            FROM rum_vitals_facts WHERE {where_vitals}
            UNION ALL
            SELECT event_timestamp, {identity} AS beacon_id, 'errors' AS src,
                   '' AS metric_name, CAST(NULL AS Nullable(Float64)) AS metric_value, pathname
            FROM rum_error_facts WHERE {where_errors}
        )
    """
    counts = service.client.execute(
        events
        + """
        /* rum_analytics_counts */
        SELECT uniqExact(beacon_id) AS beacons,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events
        """,
        params,
    )[0]
    no_data = False
    if not counts["beacons"]:
        any_vitals = " AND ".join(visible_fact_clauses("rum_vitals"))
        any_errors = " AND ".join(visible_fact_clauses("rum_errors"))
        any_data = service.client.execute(
            f"""
            /* rum_analytics_any_data */
            SELECT count() AS has_data FROM (
                (SELECT event_id FROM rum_vitals_facts WHERE {any_vitals} LIMIT 1)
                UNION ALL
                (SELECT event_id FROM rum_error_facts WHERE {any_errors} LIMIT 1)
            )
            """,
            {"service_id": service.service_id},
        )
        no_data = not any_data[0]["has_data"]

    hourly = end - start <= timedelta(hours=48)
    bucket_sql = "toStartOfHour(event_timestamp, 'UTC')" if hourly else "toStartOfDay(event_timestamp, 'UTC')"

    vitals_sql = f"""
        /* rum_analytics_vitals */
        SELECT lower(metric_name) AS metric_name,
               quantileExactInclusiveOrNull(0.75)(metric_value) as p75,
               count() as total,
               countIf(metric_rating = 'good') as good,
               countIf(metric_rating IN ('needs-improvement', 'needs_improvement')) as ni,
               countIf(metric_rating = 'poor') as poor
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY metric_name
        """
    environments_sql = f"""
        /* rum_analytics_environments */
        SELECT if(browser = '', 'Unknown', browser) AS env_browser,
               if(os = '', 'Unknown', os) AS env_os,
               if(device = '', 'Unknown', device) AS env_device,
               uniqExact({identity}) AS beacons,
               grouping(browser) AS g_browser, grouping(os) AS g_os, grouping(device) AS g_device
        FROM rum_vitals_facts
        WHERE {where_vitals}
        GROUP BY GROUPING SETS ((browser), (os), (device))
        """
    trends_sql = (
        events
        + f"""
        /* rum_analytics_trends */
        SELECT {bucket_sql} AS bucket,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name NOT LIKE 'event_%') AS pageviews,
               uniqExactIf(beacon_id, src = 'vitals' AND metric_name LIKE 'event_%') AS interactions,
               uniqExactIf(beacon_id, src = 'errors') AS errors
        FROM events GROUP BY bucket ORDER BY bucket
        """
    )
    pages_sql = (
        events
        + """
        /* rum_analytics_pages */
        SELECT pathname AS path,
               uniqExactIf(beacon_id, src = 'vitals') AS views,
               avgOrNullIf(metric_value, metric_name IN
                   ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')) AS avg_load,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp,
               quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls,
               uniqExactIf(beacon_id, src = 'errors') * 100.0
                   / nullIf(views + uniqExactIf(beacon_id, src = 'errors'), 0) AS error_rate
        FROM events GROUP BY pathname HAVING views > 0
        ORDER BY error_rate DESC, avg_load DESC, path LIMIT 5
        """
    )
    exceptions_sql = f"""
        /* rum_analytics_exceptions */
        SELECT error_message, error_file, error_line, error_col, count() AS count
        FROM rum_error_facts WHERE {where_errors}
        GROUP BY error_message, error_file, error_line, error_col
        ORDER BY count DESC, error_message, error_file, error_line, error_col LIMIT 3
        """

    with ThreadPoolExecutor(max_workers=5) as pool:
        f_vitals = pool.submit(contextvars.copy_context().run, service.client.execute, vitals_sql, params)
        f_env = pool.submit(contextvars.copy_context().run, service.client.execute, environments_sql, params)
        f_trends = pool.submit(contextvars.copy_context().run, service.client.execute, trends_sql, params)
        f_pages = pool.submit(contextvars.copy_context().run, service.client.execute, pages_sql, params)
        f_errors = pool.submit(contextvars.copy_context().run, service.client.execute, exceptions_sql, params)
        rows = f_vitals.result()
        environment_rows = f_env.result()
        trend_rows = f_trends.result()
        page_rows = f_pages.result()
        error_rows = f_errors.result()

    browsers: dict[str, int] = {}
    operating_systems: dict[str, int] = {}
    devices: dict[str, int] = {}
    for row in environment_rows:
        if not row["g_browser"]:
            browsers[row["env_browser"]] = int(row["beacons"])
        elif not row["g_os"]:
            operating_systems[row["env_os"]] = int(row["beacons"])
        elif not row["g_device"]:
            devices[row["env_device"]] = int(row["beacons"])

    vitals: dict[str, Any] = {
        "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
        "fid": {"p75": None, "fcp": None, "ttfb": None},
    }

    def seconds(value: float | None) -> float | None:
        return round(value / 1000.0 if value > 20 else value, 2) if value is not None else None

    for r in rows:
        m = r["metric_name"].lower()
        p75 = r["p75"]
        if m in ("lcp", "cls", "inp"):
            vitals[m]["p75"] = (
                (seconds(p75) if m == "lcp" else (round(p75, 3) if m == "cls" else int(p75)))
                if p75 is not None
                else None
            )
            t = r["total"]
            if t:
                good = int(r["good"] * 100 / t)
                poor = int(r["poor"] * 100 / t)
                vitals[m]["distribution"] = {"good": good, "poor": poor, "needs_improvement": max(0, 100 - good - poor)}
        elif m == "fid":
            vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
        elif m in ("fcp", "ttfb"):
            vitals["fid"][m] = seconds(p75)

    trend_by_bucket = {}
    for row in trend_rows:
        ts = row["bucket"]
        dt = ts if isinstance(ts, datetime) else range_value(ts)
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        trend_by_bucket[dt] = row

    trends: dict[str, list[Any]] = {
        "timestamps": [],
        "lcp": [],
        "cls": [],
        "error_rate": [],
        "pageviews": [],
        "interactions": [],
        "errors": [],
    }
    curr = start.replace(minute=0, second=0, microsecond=0)
    target = end.replace(minute=0, second=0, microsecond=0)
    step = timedelta(hours=1) if hourly else timedelta(days=1)
    if not hourly:
        curr = curr.replace(hour=0)
        target = target.replace(hour=0)
    while not no_data and curr <= target:
        bucket_row = trend_by_bucket.get(curr)
        trends["timestamps"].append(curr.isoformat())
        trends["lcp"].append(seconds(bucket_row["lcp"]) if bucket_row else None)
        trends["cls"].append(round(bucket_row["cls"], 3) if bucket_row and bucket_row["cls"] is not None else None)
        denominator = bucket_row["views"] + bucket_row["errors"] if bucket_row else 0
        trends["error_rate"].append(
            round(bucket_row["errors"] * 100.0 / denominator, 2) if bucket_row and denominator else None
        )
        for metric_key in ("pageviews", "interactions", "errors"):
            trends[metric_key].append(int(bucket_row[metric_key]) if bucket_row else 0)
        curr += step

    worst_pages = [
        {
            "path": row["path"],
            "views": row["views"],
            "avg_load_time": seconds(row["avg_load"] or 0.0),
            "lcp_p75": seconds(row["lcp"]),
            "cls_p75": round(row["cls"], 3) if row["cls"] is not None else None,
            "error_rate": round(row["error_rate"], 2) if row["error_rate"] is not None else 0.0,
        }
        for row in page_rows
    ]
    result = {
        "is_mock": False,
        "no_data": no_data,
        "beacon_count": int(counts["beacons"]),
        "pageview_count": int(counts["pageviews"]),
        "interaction_count": int(counts["interactions"]),
        "error_count": int(counts["errors"]),
        "vitals": vitals,
        "worst_pages": worst_pages,
        "errors": [
            {
                "message": row["error_message"],
                "file": row["error_file"] or "unknown.js",
                "line": int(row.get("error_line") or 0),
                "col": int(row.get("error_col") or 0),
                "count": row["count"],
            }
            for row in error_rows
        ],
        "trends": trends,
        "environments": {"browsers": browsers, "os": operating_systems, "devices": devices},
    }
    if no_data:
        result["message"] = "Waiting for real-time RUM user events..."
        for metric in ("lcp", "cls", "inp"):
            vitals[metric]["distribution"] = None
    return result


def _ttfb_ms(value: Any) -> int | None:
    """Beacons report TTFB in seconds or milliseconds; the live feed always shows whole milliseconds."""
    if value is None:
        return None
    number = float(value)
    return int(number * 1000) if number < 15 else int(number)


def rum_live_events(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    filters: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    start_dt, end_dt = parse_time_window(start_time, end_time, now=datetime.now(UTC))
    filter_clauses, filter_params = _filter_clauses(filters)
    window = ["event_timestamp >= {start_time:DateTime64(3)}", "event_timestamp <= {end_time:DateTime64(3)}"]

    def scope(domain: str) -> str:
        return " AND ".join([*visible_fact_clauses(domain), *window, *filter_clauses])

    query = f"""
    /* rum_live_events */
    SELECT * FROM (
    SELECT * FROM (
        SELECT
            event_timestamp AS timestamp,
            'pageview' AS type,
            pathname,
            metric_name,
            metric_value,
            metric_rating,
            browser, os, device,
            client_id AS cid,
            toString(request_event_id) AS req_id,
            '' AS error_message,
            city, region, country, pop, tls, ttfb
        FROM rum_vitals_facts
        WHERE {scope("rum_vitals")}
        ORDER BY event_timestamp DESC
        LIMIT {{limit:UInt32}}
    )

    UNION ALL

    SELECT * FROM (
        SELECT
            event_timestamp AS timestamp,
            'error' AS type,
            pathname,
            '' AS metric_name,
            CAST(NULL AS Nullable(Float64)) AS metric_value,
            '' AS metric_rating,
            browser, os, device,
            client_id AS cid,
            toString(request_event_id) AS req_id,
            error_message,
            city, region, country, pop, tls, ttfb
        FROM rum_error_facts
        WHERE {scope("rum_errors")}
        ORDER BY event_timestamp DESC
        LIMIT {{limit:UInt32}}
    )
    )
    ORDER BY timestamp DESC
    LIMIT {{limit:UInt32}}
    """
    res = service.client.execute(
        query,
        {"service_id": service.service_id, "start_time": start_dt, "end_time": end_dt, "limit": limit, **filter_params},
    )

    events: list[dict[str, Any]] = []
    for d in res:
        etype = d.get("type", "pageview")
        mname = d.get("metric_name")
        mrating = d.get("metric_rating")
        mval = d.get("metric_value")
        emsg = d.get("error_message")
        path = d.get("pathname") or "/"
        browser = d.get("browser") or "Unknown"
        os_name = d.get("os") or "Unknown"
        meta = {
            "browser": {"name": browser},
            "os": {"name": os_name},
            "device": {"type": d.get("device") or "Unknown"},
            "page": {"url": path},
        }
        context = {
            "cid": d.get("cid"),
            "req_id": d.get("req_id"),
            "city": d.get("city") or "Unknown",
            "region": d.get("region") or "Unknown",
            "country": d.get("country") or "Unknown",
            "pop": d.get("pop") or "Unknown",
            "tls": d.get("tls") or "Unknown",
            "ttfb": _ttfb_ms(d.get("ttfb")),
        }
        if etype == "error":
            desc = emsg or "JS Error"
            raw_log: dict[str, Any] = {
                "meta": meta,
                "measurements": [],
                "exceptions": [{"type": "Error", "value": desc}],
                **context,
            }
        else:
            desc = "Page loaded successfully"
            if mname:
                desc = f"Metric {mname.upper()}: {float(mval)}" if mval is not None else f"Metric {mname.upper()}"
                if mrating:
                    desc += f" ({mrating.upper()})"
            raw_log = {
                "meta": meta,
                "measurements": [
                    {
                        "type": "web-vitals",
                        "values": {mname: mval} if mname else {},
                        "context": {"rating": mrating or ""},
                    }
                ]
                if mname
                else [],
                "exceptions": [],
                **context,
            }
        events.append(
            {
                "time": _iso(d.get("timestamp")),
                "type": etype,
                "path": path,
                "desc": desc,
                "browser": browser,
                "os": os_name,
                "raw_log": raw_log,
            }
        )
    return events


# ── Bounded RUM Recovery Planning & Execution (folded from rum_recovery.py) ───

_DOMAINS = ("rum_vitals", "rum_errors")
_IGNORED_CONTENT = {"event_id", "transform_version", "ingest_timestamp", "batch_id", "publication_state"}
_READ_LIMITS = {
    "max_rows_to_read": "100000",
    "read_overflow_mode": "throw",
    "max_result_rows": "1001",
    "result_overflow_mode": "throw",
}


class RumRecoveryBatchAdapter(ClickHouseBatchAdapter):
    """Recovery-only GLOBAL batch verification without one unbounded scan."""

    def _inventory(self, table: str) -> dict[str, int]:
        rows = self._client.execute(
            "SELECT partition_id,sum(rows) AS partition_rows FROM system.parts "
            "WHERE database=currentDatabase() AND table={table:String} AND active "
            "GROUP BY partition_id LIMIT 33",
            {"table": table},
            settings=_READ_LIMITS,
        )
        if len(rows) > 32:
            raise ValueError("RUM repair global partition inventory exceeds bound")
        result: dict[str, int] = {}
        for row in rows:
            key, weight = row["partition_id"], row["partition_rows"]
            if (
                not isinstance(key, str)
                or not key
                or key in result
                or not isinstance(weight, int)
                or isinstance(weight, bool)
                or not 0 <= weight <= 90_000
            ):
                raise ValueError("RUM repair partition cannot be proved under read cap")
            result[key] = weight
        return result

    def _count_rows(self, table: str, batch: HighScaleBatch, batch_uuid: str) -> int:
        if batch.domain not in _DOMAINS or table != _DOMAIN_TABLES[batch.domain] or not 0 < len(batch.rows) <= 1000:
            raise ValueError("RUM repair batch verification scope is invalid")
        inventory = self._inventory(table)
        groups: list[tuple[list[str], int]] = []
        for key, weight in sorted(inventory.items(), key=lambda item: (-item[1], item[0])):
            for index, (keys, total) in enumerate(groups):
                if total + weight <= 90_000:
                    keys.append(key)
                    groups[index] = (keys, total + weight)
                    break
            else:
                groups.append(([key], weight))
        if len(groups) > 6:
            raise ValueError("RUM repair global partition read groups exceed bound")
        expected = [
            dict(zip(_DOMAIN_COLUMNS[batch.domain], values, strict=True))
            for values in _rows_for_domain(batch, batch_uuid)
        ]
        expected_by_id = {str(row["event_id"]): row for row in expected}
        if len(expected_by_id) != len(expected):
            raise ValueError("RUM repair batch candidate identities are ambiguous")
        present: list[dict[str, Any]] = []
        for keys, _ in groups:
            placeholders = ",".join("{part" + str(index) + ":String}" for index in range(len(keys)))
            rows = self._client.execute(
                f"SELECT {','.join(_DOMAIN_COLUMNS[batch.domain])} FROM {table} "
                "WHERE service_id={service_id:String} AND batch_id={batch_id:UUID} "
                "AND _partition_id IN (" + placeholders + ") LIMIT 1001",
                {
                    "service_id": batch.service_id,
                    "batch_id": batch_uuid,
                    **{f"part{index}": key for index, key in enumerate(keys)},
                },
                settings=_READ_LIMITS,
            )
            present.extend(rows)
            if len(present) > 1000:
                raise ValueError("RUM repair global batch facts exceed row bound")
        if set(self._inventory(table)) != set(inventory):
            raise ValueError("RUM repair global partition inventory changed")
        seen: set[str] = set()
        for row in present:
            identity = str(row["event_id"])
            wanted = expected_by_id.get(identity)
            if (
                identity in seen
                or wanted is None
                or str(row["batch_id"]) != batch_uuid
                or _content(row, batch.domain) != _content(wanted, batch.domain)
                or row["transform_version"] != wanted["transform_version"]
                or row["publication_state"] != "visible"
            ):
                raise ValueError("RUM repair existing batch facts are duplicate, conflicting or out of scope")
            seen.add(identity)
        if len(present) not in (0, len(expected)):
            raise PartialInsert(len(present))
        return len(present)

    def inspect_batch(self, batch: HighScaleBatch) -> tuple[int, dict[str, Any] | None]:
        """Read-only preflight: exact global physical proof plus full-key receipt."""
        batch_uuid = _batch_uuid(batch)
        count = self._count_rows(_DOMAIN_TABLES[batch.domain], batch, batch_uuid)
        existing = super()._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
            if existing["publication_state"] == "visible" and count != len(batch.rows):
                raise PartialInsert(count)
        return count, existing

    def _existing_publication(self, batch: HighScaleBatch, batch_uuid: str) -> dict[str, Any] | None:
        return self.inspect_batch(batch)[1]


@dataclass(frozen=True)
class RumRecoveryPlan:
    plan_digest: str
    candidate_counts: dict[str, int]
    missing_counts: dict[str, int]
    exact_identity_counts: dict[str, int]
    equivalent_identity_counts: dict[str, int]
    batches: tuple[HighScaleBatch, ...]
    expected_facts: tuple[dict[str, Any], ...]
    source_count: int
    raw_line_count: int
    quarantined_lines: int

    def summary(self) -> dict[str, Any]:
        """Allowlisted operational evidence: no service/source keys or beacons."""
        return {
            "plan_digest": self.plan_digest,
            "candidate_counts": self.candidate_counts,
            "missing_counts": self.missing_counts,
            "exact_identity_counts": self.exact_identity_counts,
            "equivalent_identity_counts": self.equivalent_identity_counts,
            "batches": len(self.batches),
            "sources": self.source_count,
            "raw_lines": self.raw_line_count,
            "quarantined_lines": self.quarantined_lines,
        }


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds")
    return value


def _content(row: Mapping[str, Any], domain: str) -> str:
    values = {}
    for name in _DOMAIN_COLUMNS[domain]:
        if name in _IGNORED_CONTENT:
            continue
        value = row.get(name)
        if name == "event_timestamp" and isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        if name in {"metric_value", "ttfb"} and isinstance(value, (int, float)) and not isinstance(value, bool):
            value = float(value)
        values[name] = _canonical(value)
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def plan_rum_recovery(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    *,
    owner_epoch: int,
    read_facts: Callable[[str, tuple[str, ...]], Sequence[Mapping[str, Any]]],
    now: datetime | None = None,
    max_rows: int = 1000,
) -> RumRecoveryPlan:
    """Reconstruct each original source line once, preserving ordinals/version."""
    if manifest.domain not in _DOMAINS or owner_epoch < 1 or not 0 < max_rows <= 1000:
        raise ValueError("invalid bounded RUM repair scope")
    sources = manifest.sources
    if not sources or len(sources) > 100 or len({s.key for s in sources}) != len(sources):
        raise ValueError("RUM repair requires unique bounded manifest sources")
    rows = _read_manifest_rows(manifest, archive)
    if len(rows) > max_rows:
        raise ValueError("RUM repair archive exceeds row bound")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    candidates: dict[str, list[dict[str, Any]]] = {domain: [] for domain in _DOMAINS}
    raw_lines = 0
    quarantined: set[tuple[str, int]] = set()
    covered_offsets: set[int] = set()
    for source in sources:
        end = source.row_start + source.row_count
        if source.row_start < 0 or end > len(rows):
            raise ValueError("RUM repair source slice is invalid")
        offsets = set(range(source.row_start, end))
        if covered_offsets & offsets:
            raise ValueError("RUM repair source slices overlap")
        covered_offsets.update(offsets)
        lines: dict[int, str] = {}
        version = source.version or source.checksum
        for row in rows[source.row_start : end]:
            if row.get("_record_kind", "event") != "event":
                raise ValueError("RUM repair requires reviewed event-only source slices")
            ordinal = row.get("line_ordinal")
            raw = row.get("raw_json")
            if (
                row.get("service_id") != manifest.service_id
                or row.get("source_object_key") != source.key
                or row.get("source_object_version") != version
                or row.get("transform_version") != manifest.artifact.transform_version
                or not isinstance(ordinal, int)
                or not 0 <= ordinal < 10000
                or not isinstance(raw, str)
                or "\n" in raw
                or "\r" in raw
            ):
                raise ValueError("RUM repair source provenance is invalid")
            if ordinal in lines and lines[ordinal] != raw:
                raise ValueError("RUM repair source line is ambiguous")
            lines[ordinal] = raw
        raw_lines += len(lines)
        if not lines:
            continue
        payload = "\n".join(lines.get(i, "") for i in range(max(lines) + 1)).encode()
        descriptor = ArchiveSourceObject(
            manifest.service_id, manifest.domain, source.key, source.checksum, source.size_bytes, source.version
        )
        for domain in _DOMAINS:
            decoded = decode_source_object(
                descriptor, payload, domain=domain, transform_version=manifest.artifact.transform_version
            )
            quarantined.update((source.key, item.line_ordinal) for item in decoded.dead_letters)
            for event in decoded.events:
                if row_matches_serving_domain(domain, event):
                    candidates[domain].append(event)
    if covered_offsets != set(range(len(rows))):
        raise ValueError("RUM repair source slices do not cover archive")
    if quarantined:
        raise ValueError("RUM repair found quarantined source lines; review exact archive separately")
    if sum(map(len, candidates.values())) > max_rows:
        raise ValueError("RUM repair expansion exceeds row bound")

    expected: list[dict[str, Any]] = []
    batches: list[HighScaleBatch] = []
    missing_counts: dict[str, int] = {}
    exact_counts = {domain: 0 for domain in _DOMAINS}
    equivalent_counts = {domain: 0 for domain in _DOMAINS}
    for domain in _DOMAINS:
        events = candidates[domain]
        batch = HighScaleBatch("plan", manifest.service_id, domain, str(owner_epoch), tuple(events))
        mapped = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000000")
        facts = [dict(zip(_DOMAIN_COLUMNS[domain], row, strict=True), _repair_domain=domain) for row in mapped]
        ids = [fact["event_id"] for fact in facts]
        if len(ids) != len(set(ids)):
            raise ValueError("RUM repair candidate identities are ambiguous")
        for fact in facts:
            timestamp = fact["event_timestamp"]
            if timestamp < observed - timedelta(days=30) or timestamp > observed:
                raise ValueError("RUM repair event is outside retained fact window")
        expected.extend(facts)
        present = [
            row
            for row in read_facts(domain, tuple(s.key for s in sources))
            if row.get("_repair_domain", domain) == domain
        ]
        if len(present) > max_rows:
            raise ValueError("RUM repair existing facts exceed row bound")
        present_ids = [str(row.get("event_id")) for row in present]
        if len(present_ids) != len(set(present_ids)):
            raise ValueError("RUM repair existing identity is ambiguous or unpublished")
        missing: list[dict[str, Any]] = []
        for event, fact in zip(events, facts, strict=True):
            matches = [
                row
                for row in present
                if str(row.get("event_id")) == fact["event_id"] or _content(row, domain) == _content(fact, domain)
            ]
            if not matches:
                missing.append(event)
            elif (
                len(matches) != 1
                or not matches[0].get("repair_visible")
                or _content(matches[0], domain) != _content(fact, domain)
            ):
                raise ValueError("RUM repair existing identity is ambiguous or unpublished")
            elif str(matches[0]["event_id"]) == fact["event_id"]:
                exact_counts[domain] += 1
            else:
                equivalent_counts[domain] += 1
        missing_counts[domain] = len(missing)
        if missing:
            token = hashlib.sha256("|".join(sorted(event["event_id"] for event in missing)).encode()).hexdigest()
            batches.append(
                HighScaleBatch(
                    f"rum-repair-v1:{manifest.manifest_id}:{domain}:{token}",
                    manifest.service_id,
                    domain,
                    str(owner_epoch),
                    tuple(missing),
                )
            )
    approval = {
        "version": "rum-repair-v1",
        "manifest": manifest.manifest_id,
        "artifact": manifest.artifact.checksum,
        "owner_epoch": owner_epoch,
        "facts": [
            {
                **fact,
                "event_timestamp": _canonical(fact["event_timestamp"]),
                "ingest_timestamp": _canonical(fact["ingest_timestamp"]),
            }
            for fact in expected
        ],
    }
    digest = hashlib.sha256(json.dumps(approval, sort_keys=True, default=str).encode()).hexdigest()
    return RumRecoveryPlan(
        digest,
        {domain: len(candidates[domain]) for domain in _DOMAINS},
        missing_counts,
        exact_counts,
        equivalent_counts,
        tuple(batches),
        tuple(expected),
        len(sources),
        raw_lines,
        len(quarantined),
    )


def execute_rum_recovery(
    make_plan: Callable[[], RumRecoveryPlan],
    publication: ClickHousePublication,
    *,
    apply: bool = False,
    approve_digest: str | None = None,
) -> dict[str, Any]:
    """Caller holds the terminal-source/owner fence for this entire operation."""
    plan = make_plan()
    report = {"applied": False, **plan.summary()}
    if not apply:
        return report
    if approve_digest != plan.plan_digest:
        raise ValueError("RUM repair requires the exact reviewed plan digest")
    for batch in plan.batches:
        publication.publish(batch)
    verified = make_plan()
    if verified.plan_digest != plan.plan_digest or any(verified.missing_counts.values()):
        raise ValueError("RUM repair post-apply identity verification failed")
    return {**report, "applied": True, "remaining_counts": verified.missing_counts}


_self_mod = sys.modules[__name__]
worker = _self_mod
recovery = _self_mod
orchestration = _self_mod
rum = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
for _alias in (
    "_common",
    "_filters",
    "aggregate_batch",
    "aggregate_batch_adapter",
    "assets",
    "orchestration",
    "recovery",
    "registry_bootstrap",
    "rum",
    "rum_ingest",
    "rum_recovery",
    "source_discovery",
    "worker",
):
    if _alias != "worker":
        sys.modules[f"backend.high_scale.{_alias}"] = _self_mod
    if _hs_pkg is not None:
        setattr(_hs_pkg, _alias, _self_mod)
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.high_scale._common": "backend.high_scale.ingest_controller",
            "backend.high_scale._filters": "backend.high_scale.ingest_controller",
            "backend.high_scale.aggregate_batch": "backend.high_scale.ingest_controller",
            "backend.high_scale.aggregate_batch_adapter": "backend.high_scale.ingest_controller",
            "backend.high_scale.assets": "backend.high_scale.ingest_controller",
            "backend.high_scale.orchestration": "backend.high_scale.ingest_controller",
            "backend.high_scale.recovery": "backend.high_scale.ingest_controller",
            "backend.high_scale.registry_bootstrap": "backend.high_scale.ingest_controller",
            "backend.high_scale.rum": "backend.high_scale.ingest_controller",
            "backend.high_scale.rum_ingest": "backend.high_scale.ingest_controller",
            "backend.high_scale.rum_recovery": "backend.high_scale.ingest_controller",
            "backend.high_scale.source_discovery": "backend.high_scale.ingest_controller",
            "backend.high_scale.worker": "backend.high_scale.ingest_controller",
        }
    )
if __name__ == "__main__":
    main()
