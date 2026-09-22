import pytest

from backend.high_scale.disk_guard import VolumeStats, evaluate_disk_state


def volume_stats(*, free_fraction: float, total_bytes: int = 1_000_000_000, one_batch_bytes: int = 0) -> VolumeStats:
    return VolumeStats(
        total_bytes=total_bytes,
        free_bytes=int(total_bytes * free_fraction),
        one_batch_bytes=one_batch_bytes,
    )


def test_disk_guard_enters_emergency_mode_at_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.10)).mode == "emergency"


def test_disk_guard_enters_emergency_below_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.05)).mode == "emergency"


def test_disk_guard_is_protective_between_ten_and_fifteen_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.12)).mode == "protective"


def test_disk_guard_warns_below_the_twenty_percent_reserve() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.18)).mode == "warning"


def test_disk_guard_is_normal_with_ample_free_space() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.50)).mode == "normal"


def test_disk_guard_reserve_floor_is_the_greater_of_twenty_percent_or_one_batch() -> None:
    # 5% free (50MB) is well above a tiny one-batch size, but still below
    # the 20% floor, so the batch-size floor does not mask a real warning.
    state = evaluate_disk_state(volume_stats(free_fraction=0.19, total_bytes=1_000_000_000, one_batch_bytes=1_000_000))
    assert state.mode == "warning"
    assert state.reserve_bytes == 200_000_000


def test_disk_guard_reserve_floor_uses_batch_size_when_larger_than_twenty_percent() -> None:
    state = evaluate_disk_state(
        volume_stats(free_fraction=0.50, total_bytes=1_000_000_000, one_batch_bytes=900_000_000)
    )
    assert state.reserve_bytes == 900_000_000
    assert state.mode == "warning"


def test_disk_guard_rejects_free_bytes_above_total() -> None:
    with pytest.raises(ValueError, match="free_bytes"):
        VolumeStats(total_bytes=100, free_bytes=200, one_batch_bytes=0)
