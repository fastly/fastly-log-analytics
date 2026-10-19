"""API-only controls for the explicit, bounded ClickHouse prototype."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.models.common import BaseResponse
from backend.models.errors import ErrorDetail

Reference = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")]


class ClickHouseActiveGeneration(BaseModel):
    generation: str
    dataset_id: str
    coverage_start: str
    coverage_end: str
    expires_at: str
    expired: bool
    target_matches: bool
    coverage_age_seconds: float


class ClickHouseStatusResponse(BaseResponse):
    service_id: str
    enabled: bool
    health: Literal["ok", "disabled", "unavailable"]
    schema_version: int | None = None
    publication_counts: dict[str, int] | None = None
    oldest_pending_age_seconds: float | None = None
    last_published_at: str | None = None
    active_generation: ClickHouseActiveGeneration | None = None
    expired_dataset_count: int | None = None


class ClickHouseReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_id: str = Field(min_length=1, max_length=128)
    dataset_id: Reference | None = None
    generation: Reference | None = None
    dry_run: bool = True
    limit: int = Field(default=100, ge=1, le=1000, strict=True)

    @model_validator(mode="after")
    def exactly_one_reference(self) -> Self:
        if (self.dataset_id is None) == (self.generation is None):
            raise ValueError("provide exactly one of dataset_id or generation")
        return self


class ClickHouseReplayResponse(BaseResponse):
    service_id: str
    dry_run: bool
    operation: Literal["rebuild", "resume"]
    dataset_id: str
    generation: str | None
    limit: int
    artifact_count: int
    planned_artifacts: int
    attempted: int = 0
    published: int = 0
    activated: bool = False


class ClickHouseReplayErrorResponse(BaseResponse):
    detail: ErrorDetail
