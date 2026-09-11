"""Configuration models for portable high-scale deployment profiles."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionRecommendation:
    hot_retention_seconds: int
    warm_retention_seconds: int
    protected_free_bytes: int
    estimated_hot_bytes: int
    estimated_warm_bytes: int


@dataclass(frozen=True)
class TopologyProfile:
    name: str
    clickhouse_replicas_per_shard: int
    keeper_members: int
    persistent_serving_storage: bool
    production_ha: bool


@dataclass(frozen=True)
class HighScaleConfig:
    enabled: bool
    clickhouse_url: str | None
    fos_bucket: str | None
    archive_manifest_dsn: str | None
    worker_queue_url: str | None = None
    persistent_storage: bool = True
    profile: str = "local"

    @classmethod
    def from_environment(cls) -> HighScaleConfig:
        from backend.high_scale.config import from_environment

        return from_environment()

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
        if self.profile not in {"local", "portable-production"}:
            raise ValueError("high-scale profile must be local or portable-production")


def recommend_retention(
    *,
    service_rate: int,
    row_width_bytes: int,
    disk_bytes: int,
    reserve_fraction: float = 0.20,
) -> RetentionRecommendation:
    if service_rate <= 0 or row_width_bytes <= 0 or disk_bytes <= 0:
        raise ValueError("service rate, row width, and disk capacity must be positive")
    if not 0 < reserve_fraction < 1:
        raise ValueError("reserve_fraction must be between 0 and 1")
    protected = int(disk_bytes * reserve_fraction)
    available = disk_bytes - protected
    bytes_per_second = service_rate * row_width_bytes
    hot_seconds = min(86_400, max(3_600, available // max(bytes_per_second * 4, 1)))
    warm_seconds = min(60 * 86_400, max(hot_seconds, available // max(bytes_per_second, 1)))
    return RetentionRecommendation(
        hot_retention_seconds=int(hot_seconds),
        warm_retention_seconds=int(warm_seconds),
        protected_free_bytes=protected,
        estimated_hot_bytes=int(bytes_per_second * hot_seconds),
        estimated_warm_bytes=int(bytes_per_second * warm_seconds),
    )
