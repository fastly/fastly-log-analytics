"""Pure request-event projection for bounded high-scale Network queries."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class NetworkProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _Accumulator:
    requests: int = 0
    errors: int = 0
    tcp_rtts: list[float] = field(default_factory=list)
    ttfbs: list[float] = field(default_factory=list)
    ploss_sum: float = 0.0
    ploss_count: int = 0


def build_network_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> NetworkProjection:
    dimensions: dict[tuple[datetime, str, str, str], _Accumulator] = defaultdict(_Accumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue

        status = _finite_int(row.get("status"))
        is_error = status is not None and status >= 500

        tcp_rtt = _finite_float(row.get("tcp_rtt"))
        ttfb = _finite_float(row.get("ttfb"))
        ploss = _finite_float(row.get("ploss"))

        country = _text(row.get("country"))
        asn = _text(row.get("asn"))
        pop = _text(row.get("pop"))
        region = _text(row.get("region"))
        c_speed = _text(row.get("c_speed"))

        # Dimension tuples: (bucket, dimension_name, value, secondary_value)
        # We use secondary_value for c_speed in 'asn' dimension.

        # 1. Country
        if country:
            acc = dimensions[(bucket, "country", country, "")]
            _update(acc, is_error, tcp_rtt, ttfb, ploss)

        # 2. ASN
        if asn:
            acc = dimensions[(bucket, "asn", asn, c_speed)]
            _update(acc, is_error, tcp_rtt, ttfb, ploss)

        # 3. POP
        if pop:
            acc = dimensions[(bucket, "pop", pop, "")]
            _update(acc, is_error, tcp_rtt, ttfb, ploss)

        # 4. Region
        if region:
            acc = dimensions[(bucket, "region", region, "")]
            _update(acc, is_error, tcp_rtt, ttfb, ploss)

    return NetworkProjection(
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, c_speed, acc)
            for (bucket, dimension, value, c_speed), acc in sorted(dimensions.items())
        ),
    )


def _update(acc: _Accumulator, is_error: bool, tcp_rtt: float | None, ttfb: float | None, ploss: float | None) -> None:
    acc.requests += 1
    if is_error:
        acc.errors += 1
    if tcp_rtt is not None and tcp_rtt > 0:
        acc.tcp_rtts.append(tcp_rtt)
    if ttfb is not None and ttfb > 0:
        acc.ttfbs.append(ttfb)
    if ploss is not None:
        acc.ploss_sum += ploss
        acc.ploss_count += 1


def _dimension_row(
    bucket: datetime,
    dimension: str,
    value: str,
    c_speed: str,
    acc: _Accumulator,
) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "c_speed": c_speed,
        "requests": acc.requests,
        "errors": acc.errors,
        "tcp_rtt_count": len(acc.tcp_rtts),
        "tcp_rtt_sum": sum(acc.tcp_rtts) if acc.tcp_rtts else None,
        "tcp_rtt_p50_us": _percentile(acc.tcp_rtts, 0.50),
        "tcp_rtt_p95_us": _percentile(acc.tcp_rtts, 0.95),
        "tcp_rtt_p99_us": _percentile(acc.tcp_rtts, 0.99),
        "ploss_sum": acc.ploss_sum if acc.ploss_count > 0 else None,
        "ploss_count": acc.ploss_count,
        "ttfb_p50_us": _percentile(acc.ttfbs, 0.50),
        "ttfb_p95_us": _percentile(acc.ttfbs, 0.95),
    }


def _bucket(value: object) -> datetime | None:
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


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _finite_int(value: object) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
