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
class HighScaleQuotaProfile:
    """Per-service bounds for interactive queries and exports."""

    max_concurrent_queries: int
    max_query_rows: int
    max_exports_per_hour: int
    max_export_rows_per_hour: int
    max_export_bytes_per_hour: int


@dataclass(frozen=True)
class HighScaleSizing:
    """Inputs used to size one explicitly opted-in high-scale service."""

    service_id: str
    request_rate_per_second: int
    rum_rate_per_second: int
    row_width_bytes: int
    enabled_field_count: int
    bytes_per_enabled_field: int
    disk_capacity_bytes: int
    protected_reserve_fraction: float = 0.20
    hot_retention_seconds: int = 86_400
    warm_retention_seconds: int = 30 * 86_400
    profile: str = "local"
    shard_count: int = 1
    replicas_per_shard: int = 1
    quotas: HighScaleQuotaProfile = HighScaleQuotaProfile(4, 50_000, 12, 1_000_000, 1_000_000_000)

    def validate(self) -> None:
        if not self.service_id.strip():
            raise ValueError("high-scale service id is required")
        if self.request_rate_per_second <= 0:
            raise ValueError("request rate must be positive")
        if self.rum_rate_per_second < 0:
            raise ValueError("RUM rate cannot be negative")
        if self.row_width_bytes <= 0:
            raise ValueError("row width must be positive")
        if self.enabled_field_count <= 0 or self.bytes_per_enabled_field < 0:
            raise ValueError("enabled field sizing values are invalid")
        if self.disk_capacity_bytes <= 0:
            raise ValueError("disk capacity must be positive")
        if not 0 < self.protected_reserve_fraction < 1:
            raise ValueError("protected reserve must be between 0 and 1")
        if self.hot_retention_seconds <= 0 or self.warm_retention_seconds < self.hot_retention_seconds:
            raise ValueError("warm retention must be at least hot retention")
        if self.shard_count <= 0 or self.replicas_per_shard <= 0:
            raise ValueError("shard and replica counts must be positive")
        self.topology_profile()
        if self.quotas.max_concurrent_queries <= 0:
            raise ValueError("query concurrency quota must be positive")
        if self.quotas.max_query_rows <= 0:
            raise ValueError("query row quota must be positive")
        if self.quotas.max_exports_per_hour <= 0:
            raise ValueError("export quota must be positive")
        if self.quotas.max_export_rows_per_hour <= 0 or self.quotas.max_export_bytes_per_hour <= 0:
            raise ValueError("export limits must be positive")

    def topology_profile(self) -> TopologyProfile:
        return _resolve_topology_profile(self.profile)


@dataclass(frozen=True)
class HighScaleRecommendation:
    """Capacity and quota recommendation for one high-scale service."""

    service_id: str
    profile: str
    shard_count: int
    replicas_per_shard: int
    effective_rate_per_second: int
    effective_row_width_bytes: int
    estimated_bytes_per_second: int
    protected_free_bytes: int
    available_storage_bytes: int
    estimated_hot_bytes: int
    estimated_warm_bytes: int
    required_storage_bytes: int
    feasible: bool
    quotas: HighScaleQuotaProfile
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class TopologyProfile:
    name: str
    clickhouse_replicas_per_shard: int
    keeper_members: int
    persistent_serving_storage: bool
    production_ha: bool
    shard_count: int = 1


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
    from backend.high_scale.topology import build_topology_profile

    try:
        return build_topology_profile(profile_name)
    except ValueError:
        raise ValueError("high-scale profile must be local or portable-production") from None


def recommend_retention(
    *,
    service_rate: int,
    row_width_bytes: int,
    disk_bytes: int,
    reserve_fraction: float = 0.20,
    rum_rate: int = 0,
    enabled_field_count: int = 1,
    bytes_per_enabled_field: int = 0,
) -> RetentionRecommendation:
    if service_rate <= 0 or row_width_bytes <= 0 or disk_bytes <= 0 or rum_rate < 0:
        raise ValueError("service rate, row width, and disk capacity must be positive")
    if enabled_field_count <= 0 or bytes_per_enabled_field < 0:
        raise ValueError("enabled field sizing values are invalid")
    if not 0 < reserve_fraction < 1:
        raise ValueError("reserve_fraction must be between 0 and 1")
    protected = int(disk_bytes * reserve_fraction)
    available = disk_bytes - protected
    total_rate = service_rate + rum_rate
    effective_row_width = row_width_bytes + enabled_field_count * bytes_per_enabled_field
    bytes_per_second = total_rate * effective_row_width
    hot_seconds = min(86_400, max(3_600, available // max(bytes_per_second * 4, 1)))
    warm_seconds = min(60 * 86_400, max(hot_seconds, available // max(bytes_per_second, 1)))
    return RetentionRecommendation(
        hot_retention_seconds=int(hot_seconds),
        warm_retention_seconds=int(warm_seconds),
        protected_free_bytes=protected,
        estimated_hot_bytes=int(bytes_per_second * hot_seconds),
        estimated_warm_bytes=int(bytes_per_second * warm_seconds),
    )
