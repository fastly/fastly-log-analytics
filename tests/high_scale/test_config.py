import pytest

from backend.high_scale.models import (
    HighScaleConfig,
    HighScaleQuotaProfile,
    HighScaleSizing,
    recommend_high_scale_sizing,
    recommend_retention,
)


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


def test_retention_recommendation_accounts_for_rum_and_enabled_fields() -> None:
    narrow = recommend_retention(
        service_rate=100,
        row_width_bytes=100,
        disk_bytes=10**10,
    )
    wide = recommend_retention(
        service_rate=100,
        row_width_bytes=100,
        disk_bytes=10**10,
        rum_rate=100,
        enabled_field_count=5,
        bytes_per_enabled_field=100,
    )
    assert wide.hot_retention_seconds < narrow.hot_retention_seconds


def test_high_scale_recommendation_accounts_for_all_service_inputs() -> None:
    sizing = HighScaleSizing(
        service_id="service-a",
        request_rate_per_second=100,
        rum_rate_per_second=25,
        row_width_bytes=200,
        enabled_field_count=3,
        bytes_per_enabled_field=50,
        disk_capacity_bytes=100_000_000,
        hot_retention_seconds=100,
        warm_retention_seconds=200,
        profile="portable-production",
        shard_count=3,
        replicas_per_shard=2,
        quotas=HighScaleQuotaProfile(8, 75_000, 24, 2_000_000, 2_000_000_000),
    )

    recommendation = recommend_high_scale_sizing(sizing)

    assert recommendation.effective_rate_per_second == 125
    assert recommendation.effective_row_width_bytes == 350
    assert recommendation.estimated_bytes_per_second == 43_750
    assert recommendation.shard_count == 3
    assert recommendation.replicas_per_shard == 2
    assert recommendation.quotas.max_query_rows == 75_000
    assert recommendation.feasible is True


def test_high_scale_recommendation_reports_storage_pressure() -> None:
    sizing = HighScaleSizing(
        service_id="service-a",
        request_rate_per_second=100,
        rum_rate_per_second=0,
        row_width_bytes=100,
        enabled_field_count=1,
        bytes_per_enabled_field=0,
        disk_capacity_bytes=10_000,
        hot_retention_seconds=100,
        warm_retention_seconds=200,
    )

    recommendation = recommend_high_scale_sizing(sizing)

    assert recommendation.feasible is False
    assert "exceed available storage" in recommendation.warnings[0]


def test_high_scale_sizing_rejects_invalid_quota() -> None:
    sizing = HighScaleSizing(
        service_id="service-a",
        request_rate_per_second=1,
        rum_rate_per_second=0,
        row_width_bytes=100,
        enabled_field_count=1,
        bytes_per_enabled_field=0,
        disk_capacity_bytes=10_000,
        quotas=HighScaleQuotaProfile(0, 1, 1, 1, 1),
    )

    with pytest.raises(ValueError, match="concurrency"):
        recommend_high_scale_sizing(sizing)
