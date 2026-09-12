"""Capacity gate for high-scale archive recovery measurements."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RecoveryCapacityMeasurement:
    events_per_second: int
    recovery_window_seconds: int
    archive_read_events_per_second: int
    decode_events_per_second: int
    aggregate_events_per_second: int
    clickhouse_insert_events_per_second: int
    replication_factor: int
    live_ingest_reservation: float

    def validate(self) -> None:
        if self.events_per_second <= 0 or self.recovery_window_seconds <= 0:
            raise ValueError("event rate and recovery window must be positive")
        if (
            min(
                self.archive_read_events_per_second,
                self.decode_events_per_second,
                self.aggregate_events_per_second,
                self.clickhouse_insert_events_per_second,
            )
            <= 0
        ):
            raise ValueError("all recovery throughput measurements must be positive")
        if self.replication_factor < 1:
            raise ValueError("replication factor must be positive")
        if not 0 <= self.live_ingest_reservation < 1:
            raise ValueError("live ingest reservation must be in [0, 1)")


@dataclass(frozen=True)
class RecoveryCapacityReport:
    required_events_per_second: float
    limiting_stage: str
    limiting_events_per_second: int
    replication_adjusted_insert_events_per_second: float
    feasible: bool


def evaluate_recovery_capacity(measurement: RecoveryCapacityMeasurement) -> RecoveryCapacityReport:
    """Evaluate a measured recovery profile without claiming unmeasured capacity."""

    measurement.validate()
    required = (
        measurement.events_per_second
        * 86_400
        / measurement.recovery_window_seconds
        / (1 - measurement.live_ingest_reservation)
    )
    stages = {
        "archive_read": measurement.archive_read_events_per_second,
        "decode": measurement.decode_events_per_second,
        "aggregate_rebuild": measurement.aggregate_events_per_second,
        "clickhouse_insert": measurement.clickhouse_insert_events_per_second // measurement.replication_factor,
    }
    limiting_stage, limiting_rate = min(stages.items(), key=lambda item: item[1])
    replication_adjusted = measurement.clickhouse_insert_events_per_second / measurement.replication_factor
    feasible = limiting_rate >= required
    return RecoveryCapacityReport(required, limiting_stage, limiting_rate, replication_adjusted, feasible)
