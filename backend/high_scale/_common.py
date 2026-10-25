"""Shared ClickHouse SQL, projection primitives, pagination, and raw-query helpers."""

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
from typing import Any, Protocol, overload

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
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
            catalog.manifests_covering(service_id, domain, start, end),
            key=lambda manifest: manifest.coverage_start,
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
