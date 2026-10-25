"""Configuration models for portable high-scale deployment profiles."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

_BASELINE_HOT_DAYS = 1.0
_BASELINE_WARM_DAYS = 30.0
_PROTECTED_RESERVE_FRACTION = 0.20
_WARM_COMPACTION_RATIO = 0.3
_SECONDS_PER_DAY = 86_400


class ArchiveState(StrEnum):
    ARTIFACT_UPLOADING = "artifact_uploading"
    ARTIFACT_VERIFIED = "artifact_verified"
    MANIFEST_PREPARED = "manifest_prepared"
    MANIFEST_COMMITTED = "manifest_committed"
    DELETION_ELIGIBLE = "deletion_eligible"
    SOURCE_DELETED = "source_deleted"


ARCHIVE_STATES = tuple(ArchiveState)


def validate_archive_transition(current: ArchiveState, next_state: ArchiveState) -> None:
    """Reject state regressions and transitions that skip a protocol phase."""
    try:
        current_index = ARCHIVE_STATES.index(current)
        next_index = ARCHIVE_STATES.index(next_state)
    except ValueError as exc:
        raise ValueError("unknown archive state") from exc
    if next_index != current_index + 1:
        raise ValueError("archive state transitions must be monotonic and adjacent")


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
    warm_days = min(_BASELINE_WARM_DAYS, hot_days + max_warm_days)

    reasoning = (
        f"{service_profile.events_per_second:.0f} events/s at {service_profile.average_row_bytes:.0f} "
        f"bytes/row against {usable_bytes:.0f} usable bytes "
        f"({storage_profile.protected_reserve_fraction:.0%} reserved)"
    )
    return RetentionRecommendation(hot_days=hot_days, warm_days=warm_days, reasoning=reasoning)


@dataclass(frozen=True)
class TopologyProfile:
    name: str
    clickhouse_replicas_per_shard: int
    keeper_members: int
    persistent_serving_storage: bool
    production_ha: bool
    shard_count: int = 1


def build_topology_profile(profile_name: str) -> TopologyProfile:
    if profile_name == "local":
        return TopologyProfile("local", 1, 1, True, False, 1)
    if profile_name == "portable-production":
        return TopologyProfile("portable-production", 2, 3, True, True, 3)
    raise ValueError("unknown high-scale topology profile")


@dataclass(frozen=True)
class HighScaleConfig:
    enabled: bool
    clickhouse_url: str | None
    fos_bucket: str | None
    archive_manifest_dsn: str | None
    worker_queue_url: str | None = None
    persistent_storage: bool = True
    profile: str = "local"

    def topology_profile(self) -> TopologyProfile:
        return _resolve_topology_profile(self.profile)

    def validate(self) -> None:
        if not self.enabled:
            return
        if not self.clickhouse_url:
            raise ValueError("high-scale mode requires a ClickHouse URL")
        if not self.fos_bucket:
            raise ValueError("high-scale mode requires an FOS archive bucket")
        if not self.archive_manifest_dsn:
            raise ValueError("high-scale mode requires an archive manifest DSN")
        if not self.worker_queue_url:
            raise ValueError("high-scale mode requires a durable worker queue URL")
        if not self.persistent_storage:
            raise ValueError("high-scale mode requires persistent serving storage")
        self.topology_profile()


def _resolve_topology_profile(profile_name: str) -> TopologyProfile:
    """Single source of truth for profile names and replica/Keeper expectations."""
    try:
        return build_topology_profile(profile_name)
    except ValueError:
        raise ValueError("high-scale profile must be local or portable-production") from None


def from_environment(env: Mapping[str, str] | None = None) -> HighScaleConfig:
    source = os.environ if env is None else env
    config = HighScaleConfig(
        enabled=source.get("HIGH_SCALE_ENABLED", "").lower() in {"1", "true", "yes"},
        clickhouse_url=source.get("HIGH_SCALE_CLICKHOUSE_URL"),
        fos_bucket=source.get("HIGH_SCALE_FOS_BUCKET"),
        archive_manifest_dsn=source.get("HIGH_SCALE_ARCHIVE_MANIFEST_DSN"),
        worker_queue_url=source.get("HIGH_SCALE_WORKER_QUEUE_URL"),
        persistent_storage=source.get("HIGH_SCALE_PERSISTENT_STORAGE", "true").lower() not in {"0", "false", "no"},
        profile=source.get("HIGH_SCALE_PROFILE", "local"),
    )
    config.validate()
    return config


def topology_from_environment(env: Mapping[str, str] | None = None) -> TopologyProfile:
    """Resolve and validate the active topology profile from environment variables."""
    source = os.environ if env is None else env
    return _resolve_topology_profile(source.get("HIGH_SCALE_PROFILE", "local"))
