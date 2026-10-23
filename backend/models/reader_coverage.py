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


class HourlyRollupCoverage(BaseModel):
    reason: Literal[
        "unknown", "no_bundled_root", "durable_unready", "writer_behind", "no_paths", "old_gaps", "planned"
    ] = "unknown"
    window_start: str | None = None
    window_end: str | None = None
    active_hour_start: str | None = None
    closed_hours: int | None = Field(default=None, ge=0)
    bundle_hours: int | None = Field(default=None, ge=0)
    verified_empty_hours: int | None = Field(default=None, ge=0)
    missing_hours: int | None = Field(default=None, ge=0)
    covered_hours: int | None = Field(default=None, ge=0)
    coverage_fraction: float | None = Field(default=None, ge=0, le=1)
    heal_horizon_hours: int | None = Field(default=None, ge=0)


class ScalarRollupCoverage(BaseModel):
    reason: Literal["unknown", "trailing_unavailable", "boundary_changed", "planned"] = "unknown"
    window_start: str | None = None
    window_end: str | None = None
    active_hour_start: str | None = None
    closed_hours: int | None = Field(default=None, ge=0)
    prefix_hours: int = Field(default=0, ge=0)
    day_time_series_files: int = Field(default=0, ge=0)
    day_overview_files: int = Field(default=0, ge=0)
    trailing_start: str | None = None
    trailing: HourlyRollupCoverage | None = None
    covered_hours: int | None = Field(default=None, ge=0)
    coverage_fraction: float | None = Field(default=None, ge=0, le=1)


class TopNRollupCoverage(BaseModel):
    coverage_scope: Literal["any_field_file_presence"] = "any_field_file_presence"
    active_hour_start: str | None = None
    closed_hours: int | None = Field(default=None, ge=0)
    any_field_covered_hours: int | None = Field(default=None, ge=0)
    field_complete_hours: int | None = Field(default=None, ge=0)
    bundled_day_files: int = Field(default=0, ge=0)
    bundled_hour_files: int = Field(default=0, ge=0)
    per_field_day_files: int = Field(default=0, ge=0)
    per_field_hour_files: int = Field(default=0, ge=0)
    heal_horizon_hours: int = Field(default=48, ge=0)
    heal_candidates: int | None = Field(default=None, ge=0)
    heal_missing_hours: int | None = Field(default=None, ge=0)
    compacted_hours_skipped: int | None = Field(default=None, ge=0)
    outside_heal_horizon_hours: int | None = Field(default=None, ge=0)
    rolled_read: Literal["not_attempted", "used", "failed"] = "not_attempted"


class HistoryObservation(BaseModel):
    extent_source: Literal["unknown", "status_cache", "live_query", "count_only_query", "unavailable"] = "unknown"
    extent_rows: int | None = Field(default=None, ge=0)
    earliest_event_at: str | None = None
    latest_event_at: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    filters_requested: bool | None = None
    window_rows: int | None = Field(default=None, ge=0)
    window_count_source: Literal["unknown", "rollup", "raw", "filtered_temp"] = "unknown"


class ReaderCoverage(BaseModel):
    reader: Literal[
        "dashboard.conn_requests_histogram",
        "dashboard.ngwaf_bots",
        "dashboard.count",
        "dashboard.time_series",
        "dashboard.top_n",
        "dashboard.ua_bots",
    ]
    minimum_window_hours: int | None = Field(default=None, ge=0)
    maximum_window_days: int | None = Field(default=366, ge=0)
    scalar_plan: ScalarRollupCoverage | None = None
    top_n_plan: TopNRollupCoverage | None = None
    closed_hour_heal: Literal["not_attempted", "planned", "used", "unavailable", "failed"] = "not_attempted"
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
        "unsupported_metric",
        "unsupported_interval",
        "unsafe_fields",
        "unsafe_service",
        "durable_unready",
        "rollups_missing",
    ] = "unknown"
    window_start: str | None = None
    window_end: str | None = None
    attempts: list[RollupPathCoverage] = Field(default_factory=list, max_length=2)
    outcome: Literal["unknown", "ineligible", "no_coverage", "read_failed", "empty_result", "served"] = "unknown"
    live_top_up: Literal[
        "not_attempted",
        "outside_window",
        "missing_column",
        "unavailable",
        "empty",
        "used",
        "failed",
        "view_fallback",
        "unverified",
    ] = "not_attempted"
    live_start: str | None = None
    live_end: str | None = None
