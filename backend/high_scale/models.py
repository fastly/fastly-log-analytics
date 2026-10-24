"""Configuration models for portable high-scale deployment profiles."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


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
