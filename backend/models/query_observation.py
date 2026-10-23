"""Safe correlation and client-phase observations; never server CPU estimates."""

from typing import Literal

from pydantic import BaseModel, Field


class ClickHouseQueryObservation(BaseModel):
    query_id: str
    request_id: str | None = None
    page_load_id: str | None = None
    service_id: str | None = None
    component: str | None = None
    section: str | None = None
    section_id: str | None = None
    parent_section_id: str | None = None
    source: Literal["fact", "projection", "projected_fact", "mixed", "unknown"] = "unknown"
    outcome: Literal["success", "error", "timeout"]
    client_elapsed_ms: float = Field(ge=0, allow_inf_nan=False)
    admission_wait_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    network_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    decode_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    server_elapsed_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    server_elapsed_source: Literal["statistics.elapsed"] | None = None
    server_cpu_ms: None = None
    timing_kind: Literal["client_operation_wall"] = "client_operation_wall"
