"""Reader-local planning observations, not retained-history or completeness claims."""

from typing import Literal

from pydantic import BaseModel, Field


class RollupPathCoverage(BaseModel):
    bundle: Literal["security_conn_reuse", "all_fields", "ngwaf_bots"]
    reason: Literal["unknown", "durable_unready", "no_bundled_root", "no_paths", "below_coverage_floor", "planned"] = (
        "unknown"
    )
    minimum_coverage_fraction: float = Field(default=0.5, ge=0, le=1)
    active_hour_start: str | None = None
    closed_hours: int | None = Field(default=None, ge=0)
    covered_hours: int | None = Field(default=None, ge=0)
    coverage_fraction: float | None = Field(default=None, ge=0, le=1)
    partial_start_skipped: bool | None = None
    day_files: int | None = Field(default=None, ge=0)
    hour_files: int | None = Field(default=None, ge=0)
    done_markers: int | None = Field(default=None, ge=0)


class ReaderCoverage(BaseModel):
    reader: Literal["dashboard.conn_requests_histogram", "dashboard.ngwaf_bots"]
    minimum_window_hours: int = Field(ge=0)
    maximum_window_days: int = 366
    eligible: bool | None = None
    eligibility_reason: Literal[
        "unknown",
        "filtered",
        "bounds_missing",
        "top_asns_empty",
        "invalid_window",
        "window_too_short",
        "window_too_long",
        "eligible",
    ] = "unknown"
    window_start: str | None = None
    window_end: str | None = None
    attempts: list[RollupPathCoverage] = Field(default_factory=list, max_length=2)
    outcome: Literal["unknown", "ineligible", "no_coverage", "read_failed", "empty_result", "served"] = "unknown"
    live_top_up: Literal[
        "not_attempted", "outside_window", "missing_column", "unavailable", "empty", "used", "failed"
    ] = "not_attempted"
    live_start: str | None = None
    live_end: str | None = None
