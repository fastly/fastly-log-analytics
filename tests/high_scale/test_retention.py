from backend.high_scale.retention import ServiceProfile, StorageProfile, recommend_retention


def profile(rate: float, *, average_row_bytes: float = 500.0) -> ServiceProfile:
    return ServiceProfile(events_per_second=rate, average_row_bytes=average_row_bytes)


def storage(total_bytes: int) -> StorageProfile:
    return StorageProfile(total_bytes=total_bytes)


def test_high_volume_service_gets_shorter_hot_recommendation() -> None:
    low = recommend_retention(profile(1_000), storage(1 << 40))
    high = recommend_retention(profile(1_000_000), storage(1 << 40))

    assert low.hot_days > high.hot_days


def test_low_volume_service_hits_the_baseline_hot_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 40))

    assert recommendation.hot_days == 1.0


def test_warm_recommendation_never_exceeds_the_baseline_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 50))

    assert recommendation.warm_days == 30.0


def test_zero_rate_service_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match="events_per_second"):
        recommend_retention(profile(0), storage(1 << 40))


def test_recommendation_never_exceeds_the_protected_reserve() -> None:
    recommendation = recommend_retention(profile(1_000_000), storage(1 << 30))

    assert recommendation.hot_days >= 0.0
    assert recommendation.warm_days >= recommendation.hot_days
