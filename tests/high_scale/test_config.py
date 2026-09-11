import pytest

from backend.high_scale.models import HighScaleConfig, recommend_retention


def test_high_scale_requires_clickhouse_and_fos() -> None:
    config = HighScaleConfig(True, None, None, None)
    with pytest.raises(ValueError, match="ClickHouse"):
        config.validate()


def test_disabled_high_scale_config_is_inert() -> None:
    HighScaleConfig(False, None, None, None).validate()


def test_retention_recommendation_preserves_disk_reserve() -> None:
    recommendation = recommend_retention(
        service_rate=100_000,
        row_width_bytes=900,
        disk_bytes=10 * 1024**4,
    )
    assert recommendation.protected_free_bytes >= 0.20 * 10 * 1024**4
    assert recommendation.estimated_hot_bytes <= 0.80 * 10 * 1024**4
