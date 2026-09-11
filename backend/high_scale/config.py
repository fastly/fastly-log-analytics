"""Environment loading for the opt-in high-scale deployment contract."""

from __future__ import annotations

import os

from backend.high_scale.models import HighScaleConfig


def _optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def from_environment() -> HighScaleConfig:
    config = HighScaleConfig(
        enabled=os.getenv("HIGH_SCALE_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"},
        clickhouse_url=_optional_env("HIGH_SCALE_CLICKHOUSE_URL"),
        fos_bucket=_optional_env("HIGH_SCALE_FOS_BUCKET"),
        archive_manifest_dsn=_optional_env("HIGH_SCALE_ARCHIVE_MANIFEST_DSN"),
        worker_queue_url=_optional_env("HIGH_SCALE_WORKER_QUEUE_URL"),
        persistent_storage=os.getenv("HIGH_SCALE_PERSISTENT_STORAGE", "1").strip().lower()
        in {"1", "true", "yes", "on"},
        profile=os.getenv("HIGH_SCALE_PROFILE", "local").strip().lower(),
    )
    config.validate()
    return config
