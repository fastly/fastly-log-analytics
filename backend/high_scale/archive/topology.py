"""Typed archive, deployment profile, serving-volume contracts, and verified archive publication for the high-scale plane."""

from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Protocol

import structlog

logger = structlog.get_logger("backend.high_scale.archive")


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
        current_index, next_index = ARCHIVE_STATES.index(current), ARCHIVE_STATES.index(next_state)
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


def recommend_retention(service_profile: ServiceProfile, storage_profile: StorageProfile) -> RetentionRecommendation:
    usable_bytes = storage_profile.total_bytes * (1.0 - storage_profile.protected_reserve_fraction)
    bytes_per_day = service_profile.events_per_second * service_profile.average_row_bytes * _SECONDS_PER_DAY
    hot_days = min(_BASELINE_HOT_DAYS, usable_bytes / bytes_per_day)
    remaining_bytes = max(0.0, usable_bytes - hot_days * bytes_per_day)
    warm_bytes_per_day = bytes_per_day * _WARM_COMPACTION_RATIO
    max_warm_days = remaining_bytes / warm_bytes_per_day if warm_bytes_per_day > 0 else 0.0
    warm_days = min(_BASELINE_WARM_DAYS, hot_days + max_warm_days)
    reasoning = (
        f"{service_profile.events_per_second:.0f} events/s at {service_profile.average_row_bytes:.0f} "
        f"bytes/row against {usable_bytes:.0f} usable bytes ({storage_profile.protected_reserve_fraction:.0%} reserved)"
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
        for value, message in (
            (self.clickhouse_url, "high-scale mode requires a ClickHouse URL"),
            (self.fos_bucket, "high-scale mode requires an FOS archive bucket"),
            (self.archive_manifest_dsn, "high-scale mode requires an archive manifest DSN"),
            (self.worker_queue_url, "high-scale mode requires a durable worker queue URL"),
            (self.persistent_storage, "high-scale mode requires persistent serving storage"),
        ):
            if not value:
                raise ValueError(message)
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


# ── Serving-volume disk guard and periodic sampler (folded from disk_sampler.py) ──

DiskMode = Literal["normal", "warning", "protective", "emergency"]

_RESERVE_FRACTION = 0.20
_PROTECTIVE_FREE_FRACTION = 0.15
_EMERGENCY_FREE_FRACTION = 0.10


@dataclass(frozen=True)
class VolumeStats:
    total_bytes: int
    free_bytes: int
    one_batch_bytes: int = 0

    def __post_init__(self) -> None:
        for valid, msg in (
            (self.total_bytes > 0, "total_bytes must be positive"),
            (self.free_bytes >= 0, "free_bytes must be non-negative"),
            (self.free_bytes <= self.total_bytes, "free_bytes cannot exceed total_bytes"),
            (self.one_batch_bytes >= 0, "one_batch_bytes must be non-negative"),
        ):
            if not valid:
                raise ValueError(msg)


@dataclass(frozen=True)
class DiskState:
    mode: DiskMode
    free_fraction: float
    reserve_bytes: int
    reasoning: str


def evaluate_disk_state(volume: VolumeStats) -> DiskState:
    free_fraction = volume.free_bytes / volume.total_bytes
    reserve_bytes = max(int(volume.total_bytes * _RESERVE_FRACTION), volume.one_batch_bytes)
    if free_fraction <= _EMERGENCY_FREE_FRACTION:
        mode: DiskMode = "emergency"
        reasoning = f"free space {free_fraction:.1%} at or below the {_EMERGENCY_FREE_FRACTION:.0%} emergency floor"
    elif free_fraction <= _PROTECTIVE_FREE_FRACTION:
        mode = "protective"
        reasoning = f"free space {free_fraction:.1%} at or below the {_PROTECTIVE_FREE_FRACTION:.0%} protective floor"
    elif volume.free_bytes <= reserve_bytes:
        mode = "warning"
        reasoning = f"free space {volume.free_bytes} bytes at or below the {reserve_bytes}-byte protected reserve"
    else:
        mode = "normal"
        reasoning = f"free space {free_fraction:.1%} clears the {reserve_bytes}-byte protected reserve"
    return DiskState(mode=mode, free_fraction=free_fraction, reserve_bytes=reserve_bytes, reasoning=reasoning)


MODE_LEVELS: dict[DiskMode, int] = {"normal": 0, "warning": 1, "protective": 2, "emergency": 3}


class StorageProbe(Protocol):
    def storage_health(self) -> dict: ...


@dataclass(frozen=True)
class DiskSnapshot:
    state: DiskState
    total_bytes: int
    free_bytes: int
    sampled_at: float


_lock = threading.Lock()
_latest: DiskSnapshot | None = None


def _one_batch_bytes() -> int:
    raw = os.getenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("high_scale.disk_batch_bytes_invalid", value=raw)
        return 0


def sample_disk_state(probe: StorageProbe, *, now: float | None = None) -> DiskSnapshot:
    """Sample the serving volume once and record the evaluated mode."""
    health = probe.storage_health()
    volume = VolumeStats(int(health["disk_total"]), int(health["disk_free"]), _one_batch_bytes())
    snapshot = DiskSnapshot(
        evaluate_disk_state(volume), volume.total_bytes, volume.free_bytes, time.time() if now is None else now
    )
    _record(snapshot)
    return snapshot


def _record(snapshot: DiskSnapshot) -> None:
    global _latest
    with _lock:
        previous, _latest = _latest, snapshot
    if previous is None or previous.state.mode != snapshot.state.mode:
        log = logger.warning if MODE_LEVELS[snapshot.state.mode] >= MODE_LEVELS["protective"] else logger.info
        log(
            "high_scale.disk_mode_changed",
            previous_mode=previous.state.mode if previous else None,
            mode=snapshot.state.mode,
            free_fraction=round(snapshot.state.free_fraction, 4),
            reserve_bytes=snapshot.state.reserve_bytes,
            reasoning=snapshot.state.reasoning,
        )


def latest_disk_snapshot() -> DiskSnapshot | None:
    with _lock:
        return _latest


def disk_snapshot_payload(now: float | None = None) -> dict[str, Any] | None:
    """Health-snapshot projection of the latest sample, or None before the first."""
    snapshot = latest_disk_snapshot()
    if snapshot is None:
        return None
    current = time.time() if now is None else now
    return {
        "mode": snapshot.state.mode,
        "free_fraction": snapshot.state.free_fraction,
        "reserve_bytes": snapshot.state.reserve_bytes,
        "total_bytes": snapshot.total_bytes,
        "free_bytes": snapshot.free_bytes,
        "reasoning": snapshot.state.reasoning,
        "sample_age_s": max(0.0, current - snapshot.sampled_at),
    }


def reset_for_tests() -> None:
    global _latest
    with _lock:
        _latest = None
        setattr(sys.modules[__name__], "_latest", None)
