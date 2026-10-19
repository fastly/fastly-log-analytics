"""Typed views over the Fastly stats and NGWAF responses this app consumes.

Only fields the backend actually reads are declared; everything else Fastly
returns is tolerated (``extra="allow"``) so an additive API change never breaks
parsing. Numeric counters coerce ``None``/garbage to ``0`` (the old
``int(x or 0)`` behavior).
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator


def _count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


Count = Annotated[int, BeforeValidator(_count)]
OptInt = Annotated[int | None, BeforeValidator(_opt_int)]


class _Tolerant(BaseModel):
    model_config = ConfigDict(extra="allow")


class StatsRecord(_Tolerant):
    """One bucket of ``/stats/service/{id}`` or ``/stats/aggregate``."""

    start_time: OptInt = None
    requests: Count = 0
    edge_requests: Count = 0
    object_storage_class_a_operations_count: Count = 0
    object_storage_class_b_operations_count: Count = 0
    object_storage: Any = None

    def fos_ops(self) -> tuple[int, int]:
        """Object Storage (Class A, Class B) op counts.

        Fastly returns the flattened ``object_storage_*`` keys on the aggregate endpoint
        and a nested ``object_storage`` dict elsewhere; accept either.
        """
        a = self.object_storage_class_a_operations_count
        b = self.object_storage_class_b_operations_count
        if a or b:
            return a, b
        sub = self.object_storage
        if isinstance(sub, dict):
            return _count(sub.get("class_a_operations_count")), _count(sub.get("class_b_operations_count"))
        return 0, 0


class StatsResponse(_Tolerant):
    data: list[StatsRecord] = Field(default_factory=list)

    @field_validator("data", mode="before")
    @classmethod
    def _null_data_is_empty(cls, v: Any) -> Any:
        return [] if v is None else v


class NgwafWorkspace(_Tolerant):
    id: str | None = None
    name: str | None = None
    attributes: dict[str, Any] | None = None

    def display_name(self) -> str | None:
        return self.name or (self.attributes or {}).get("name") or self.id


class NgwafWorkspacesResponse(_Tolerant):
    """``GET /ngwaf/v1/workspaces``: list under ``data`` (current) or ``workspaces`` (legacy)."""

    data: list[NgwafWorkspace] | None = None
    workspaces: list[NgwafWorkspace] | None = None

    def workspaces_list(self) -> list[dict[str, str | None]]:
        # Key presence (not truthiness) decides the shape: an empty "data" is authoritative.
        items = self.data if self.data is not None else (self.workspaces or [])
        return [{"id": w.id, "name": w.display_name()} for w in items]
