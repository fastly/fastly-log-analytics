"""Pure request-event projection for bounded high-scale Security queries."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class SecurityProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _Accumulator:
    requests: int = 0
    wellknown_bot_name: str = ""
    bot_category: str = ""
    verified_count: int = 0
    impersonator_count: int = 0
    unverified_count: int = 0


def build_security_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> SecurityProjection:
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue

        # NGWAF Bots
        ngwaf_bot = _text(row.get("_ngwaf_bot_name"))
        if ngwaf_bot:
            # Check the verification state
            state = _text(row.get("ngwaf_bot_verification_state", "unverified"))
            acc = dimensions[(bucket, "ngwaf_bot", ngwaf_bot)]
            acc.requests += 1
            if state == "verified":
                acc.verified_count += 1
            elif state == "impersonator":
                acc.impersonator_count += 1
            else:
                acc.unverified_count += 1

            if not acc.wellknown_bot_name:
                acc.wellknown_bot_name = _text(row.get("_ngwaf_wellknown_bot_name"))
            if not acc.bot_category:
                acc.bot_category = _text(row.get("_ngwaf_bot_category"))

    return SecurityProjection(
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
    )


def _dimension_row(
    bucket: datetime,
    dimension: str,
    value: str,
    acc: _Accumulator,
) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "wellknown_bot_name": acc.wellknown_bot_name,
        "bot_category": acc.bot_category,
        "verified_count": acc.verified_count,
        "impersonator_count": acc.impersonator_count,
        "unverified_count": acc.unverified_count,
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


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""
