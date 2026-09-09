"""Engine-neutral benchmark report, independent of live services and credentials."""

from __future__ import annotations

import math
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

Nonnegative = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Count = Annotated[int, Field(ge=0)]
PositiveCount = Annotated[int, Field(gt=0)]


class _StrictReport(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Freshness(_StrictReport):
    """UTC-aware measured watermarks, not cached dashboard summary extents."""

    observed_at: AwareDatetime
    event_watermark: AwareDatetime
    window_start: AwareDatetime
    window_end: AwareDatetime
    last_commit_at: AwareDatetime
    last_publication_at: AwareDatetime

    @model_validator(mode="after")
    def check_order(self) -> Self:
        if self.window_start >= self.window_end:
            raise ValueError("window_start must precede window_end")
        if any(
            value > self.observed_at
            for value in (self.event_watermark, self.window_end, self.last_commit_at, self.last_publication_at)
        ):
            raise ValueError("freshness watermarks and the closed window must not follow observed_at")
        return self


class BenchmarkReport(_StrictReport):
    """Thirty-plus post-warm-up attempts; RPS counts successes, not offered load.

    Quantiles cover all measured attempts, including failures. Ingest lag is
    maximum discovery-to-commit latency over the fixture's committed ledger
    rows with both timestamps. ClickHouse-only metrics are required nullable
    fields: absence of an engine measurement is never encoded as zero.
    """

    schema_version: Annotated[int, Field(ge=1, le=1)]
    engine: Literal["ducklake", "clickhouse_hybrid"]
    dataset_size_rows: PositiveCount
    eligible_window_rows: PositiveCount
    concurrency: PositiveCount
    warmup_requests: Annotated[int, Field(ge=3)]
    measured_requests: Annotated[int, Field(ge=30)]
    elapsed_seconds: Annotated[float, Field(gt=0, allow_inf_nan=False)]
    achieved_throughput_rps: Nonnegative
    p50_ms: Nonnegative
    p95_ms: Nonnegative
    p99_ms: Nonnegative
    ingest_lag_seconds: Nonnegative
    error_count: Count
    freshness: Freshness
    clickhouse_ingest_lag_seconds: Nonnegative | None
    clickhouse_rebuild_seconds: Nonnegative | None

    @model_validator(mode="after")
    def check_measurements(self) -> Self:
        if not self.p50_ms <= self.p95_ms <= self.p99_ms:
            raise ValueError("quantiles must satisfy p50 <= p95 <= p99")
        if self.eligible_window_rows > self.dataset_size_rows:
            raise ValueError("eligible window rows exceed measured dataset size")
        if self.error_count > self.measured_requests:
            raise ValueError("errors exceed measured attempts")
        expected_rps = (self.measured_requests - self.error_count) / self.elapsed_seconds
        if not math.isclose(self.achieved_throughput_rps, expected_rps, rel_tol=1e-6, abs_tol=1e-9):
            raise ValueError("throughput must equal successful requests / measured wall time")
        if self.engine == "ducklake" and (
            self.clickhouse_ingest_lag_seconds is not None or self.clickhouse_rebuild_seconds is not None
        ):
            raise ValueError("DuckLake baseline must explicitly leave ClickHouse-only measurements null")
        return self
