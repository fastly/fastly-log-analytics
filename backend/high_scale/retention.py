"""Adaptive hot/warm retention recommendations for the high-scale plane.

Pure calculation only — no I/O, no provisioning side effects. The wizard
(and any admin recommendation surface) calls this with an observed or
estimated service profile and the target storage volume's capacity; the
caller is responsible for turning a recommendation into an actual retention
setting (with an explicit per-service override always taking precedence).
"""

from __future__ import annotations

from dataclasses import dataclass

# Targets from the implementation plan's storage policy: ~24h of hot
# full-fidelity rows, up to 30 days of warm compacted data, whenever the
# volume has room for it. A busier service or a smaller volume shrinks both
# below these baselines rather than ever exceeding the protected reserve.
_BASELINE_HOT_DAYS = 1.0
_BASELINE_WARM_DAYS = 30.0
_PROTECTED_RESERVE_FRACTION = 0.20
# Compacted (warm) storage is assumed denser than hot full-fidelity rows —
# this is the same "compaction amplification" factor the plan calls out,
# expressed as the fraction of raw bytes a compacted row still occupies.
_WARM_COMPACTION_RATIO = 0.3
_SECONDS_PER_DAY = 86_400


@dataclass(frozen=True)
class ServiceProfile:
    events_per_second: float
    average_row_bytes: float = 500.0

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if self.average_row_bytes <= 0:
            raise ValueError("average_row_bytes must be positive")


@dataclass(frozen=True)
class StorageProfile:
    total_bytes: int
    protected_reserve_fraction: float = _PROTECTED_RESERVE_FRACTION

    def __post_init__(self) -> None:
        if self.total_bytes <= 0:
            raise ValueError("total_bytes must be positive")
        if not 0.0 <= self.protected_reserve_fraction < 1.0:
            raise ValueError("protected_reserve_fraction must be in [0, 1)")


@dataclass(frozen=True)
class RetentionRecommendation:
    hot_days: float
    warm_days: float
    reasoning: str


def recommend_retention(
    service_profile: ServiceProfile,
    storage_profile: StorageProfile,
) -> RetentionRecommendation:
    usable_bytes = storage_profile.total_bytes * (1.0 - storage_profile.protected_reserve_fraction)
    bytes_per_day = service_profile.events_per_second * service_profile.average_row_bytes * _SECONDS_PER_DAY

    max_hot_days = usable_bytes / bytes_per_day
    hot_days = min(_BASELINE_HOT_DAYS, max_hot_days)

    remaining_bytes = max(0.0, usable_bytes - hot_days * bytes_per_day)
    warm_bytes_per_day = bytes_per_day * _WARM_COMPACTION_RATIO
    max_warm_days = remaining_bytes / warm_bytes_per_day if warm_bytes_per_day > 0 else 0.0
    # warm_days is the TOTAL retention window (hot is its full-fidelity
    # prefix), so it can never be below hot_days and never above baseline.
    warm_days = min(_BASELINE_WARM_DAYS, hot_days + max_warm_days)

    reasoning = (
        f"{service_profile.events_per_second:.0f} events/s at {service_profile.average_row_bytes:.0f} "
        f"bytes/row against {usable_bytes:.0f} usable bytes "
        f"({storage_profile.protected_reserve_fraction:.0%} reserved)"
    )
    return RetentionRecommendation(hot_days=hot_days, warm_days=warm_days, reasoning=reasoning)
