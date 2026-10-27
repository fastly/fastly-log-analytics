"""Shared high-scale helpers, constants, filter builders, and types."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, overload

import pyarrow as pa
import pyarrow.parquet as pq

from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key
from backend.utils.date_utils import safe_iso

if TYPE_CHECKING:
    from backend.high_scale.archive import ArchiveManifest, ArchivePublication

GAMMA: float = 1.02
RELATIVE_ERROR: float = (GAMMA - 1.0) / (GAMMA + 1.0)
_LOG_GAMMA: float = math.log(GAMMA)


def _centroid(value: float) -> float:
    if value == 0.0:
        return 0.0
    index = math.ceil(math.log(abs(value)) / _LOG_GAMMA)
    return math.copysign(2.0 * GAMMA**index / (GAMMA + 1.0), value)


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


_bucket = bucket


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


def _flt(value: Any, *, precision: int | None = None) -> float:
    if value is None or value == "":
        return 0.0
    parsed = float(value)
    if not math.isfinite(parsed):
        return 0.0
    return round(parsed, precision) if precision is not None else parsed


def optional_int(value: object) -> int | None:
    parsed = finite_float(value)
    return int(parsed) if parsed is not None and parsed.is_integer() else None


def percentiles(values: list[float], qs: tuple[float, ...]) -> list[float | None]:
    """Linear-interpolated percentiles (each ``q`` in [0, 1]) from a single sort of ``values``."""
    if not values:
        return [None] * len(qs)
    ordered = sorted(values)
    out: list[float | None] = []
    for q in qs:
        pos = (len(ordered) - 1) * q
        lo, hi = math.floor(pos), math.ceil(pos)
        if lo == hi:
            out.append(ordered[lo])
        else:
            out.append(ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo))
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
        raw = {
            "timestamp": self.timestamp.astimezone(UTC).isoformat(),
            "event_id": self.event_id,
            "service_id": self.service_id,
            "domain": self.domain,
        }
        payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        signature = hmac.new(secret, payload, hashlib.sha256).digest()
        return f"{_b64(payload)}.{_b64(signature)}"

    @classmethod
    def decode(cls, token: str, secret: bytes) -> KeysetCursor:
        if not secret:
            raise ValueError("cursor signing secret is required")
        try:
            payload_token, signature_token = token.split(".", 1)
            payload, signature = _unb64(payload_token), _unb64(signature_token)
            if not hmac.compare_digest(signature, hmac.new(secret, payload, hashlib.sha256).digest()):
                raise ValueError("cursor signature mismatch")
            raw = json.loads(payload)
            timestamp = datetime.fromisoformat(raw["timestamp"]).astimezone(UTC)
            if not all(isinstance(raw[k], str) and raw[k] for k in ("event_id", "service_id", "domain")):
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
        manifests = sorted(catalog.manifests_covering(service_id, domain, start, end), key=lambda m: m.coverage_start)
        for manifest in manifests:
            table = pq.read_table(pa.BufferReader(archive.read_artifact(manifest)))
            for row in table.to_pylist():
                timestamp = _row_timestamp(row)
                if timestamp is None or (start <= timestamp < end):
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
    "publication_state='visible' AND batch_id IN ("
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
        "publication_state='visible' AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' AND publication_state='visible')"
    )


def visible_fact_clauses(domain: str) -> list[str]:
    """Predicates (with a ``{service_id:String}`` parameter) limiting a fact table to visible batches of ``domain``."""
    if not _DOMAIN_IDENT_RE.fullmatch(domain):
        raise ValueError(f"Invalid publication domain identifier: {domain!r}")
    return [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' AND publication_state='visible')",
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
    if end.second == 0 and end.microsecond == 0:
        return end + timedelta(minutes=1)
    return end


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
        raw_values = filter_spec_attr(spec, "values") or []
        values = [v for v in raw_values if not (isinstance(v, str) and v.strip() == "")]
        if not values:
            continue
        if key in _UNSUPPORTED or not key:
            raise ValueError(f"high-scale request readers do not support filtering on {raw_key!r}")
        column = _column(key, params, f"{prefix}_k{i}")
        concrete = [v for v in values if v is not None]
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
            op = "IN" if include else "NOT IN"
            parts.append(f"{column} {op} ({', '.join(names)})")
        if len(concrete) != len(values):
            eq_op = "=" if include else "!="
            parts.append(f"{column} {eq_op} ''")
        joiner = " OR " if include else " AND "
        clauses.append(f"({joiner.join(parts)})")
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
        "batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={publication_domain:String} AND publication_state='visible')",
    ]
    params: dict[str, Any] = {"service_id": service_id, "publication_domain": domain}
    start, end = range_value(start_time), range_value(end_time)
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
    start, end = range_value(start_time), range_value(end_time)
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        cmp_op = "<=" if end_inclusive else "<"
        clauses.append(f"event_timestamp {cmp_op} {{end:DateTime64(3)}}")
        params["end"] = end
    return " AND ".join(clauses), params


_FIELD_DIMENSIONS: dict[str, str] = {
    "url": "url",
    "country": "country",
    "ip": "client_ip",
    "asn": "asn",
    "host": "host",
    "method": "method",
    "status": "status",
    "cache": "cache",
    "proto": "proto",
    "ua": "ua",
    "referer": "referer",
    "cookie_session": "cookie_session",
    "resp_header_content_encoding": "resp_header_content_encoding",
    "ttl": "ttl",
    "age": "age",
    "hits": "hits",
    "digest": "digest",
    "city": "city",
    "region": "region",
    "metro": "metro",
    "transport": "transport",
    "c_speed": "c_speed",
    "c_type": "c_type",
    "pop": "pop",
    "backend": "backend",
    "edge": "edge",
    "server_region": "server_region",
    "tls": "tls",
    "is_ipv6": "is_ipv6",
    "conn_requests": "conn_requests",
    "waf": "waf",
    "waf_resp": "waf_resp",
    "waf_ms": "waf_ms",
    "p_type": "p_type",
    "p_desc": "p_desc",
    "ja3": "ja3",
    "ja4": "ja4",
    "tls_ciphers_sha": "tls_ciphers_sha",
    "io_input_format": "io_input_format",
    "io_output_format": "io_output_format",
}
