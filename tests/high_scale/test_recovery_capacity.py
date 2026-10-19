import pytest

from backend.high_scale.recovery_capacity import (
    RecoveryCapacityMeasurement,
    evaluate_recovery_capacity,
)


def test_two_million_events_per_second_target_requires_capacity_gate() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=750_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.required_events_per_second == pytest.approx(240_000_000)
    assert report.replication_adjusted_insert_events_per_second == pytest.approx(250_000_000)
    assert report.feasible is True


def test_replication_can_make_an_otherwise_fast_recovery_infeasible() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=400_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.limiting_stage == "clickhouse_insert"
    assert report.feasible is False
