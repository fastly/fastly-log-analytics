"""Response models for ``GET /api/log-fields/catalog`` (and its bootstrap fold-in).

Producer: ``routers/bootstrap._compute_log_fields_catalog`` built from
``core/log_fields.py`` (built-in entries) and
``get_custom_fields_catalog_entries`` (custom entries). Built-in and custom
entries carry different key sets, so every key except ``id`` is optional and
``WireSafeModel`` omits keys the producer never set.
"""

from __future__ import annotations

from backend.models._base import WireSafeModel


class LogFieldCatalogEntry(WireSafeModel):
    id: str
    # ``None`` for the base-group built-ins; ``"custom"`` for custom fields.
    group: str | None
    label: str
    description: str
    duckdb_type: str
    # Built-in entries only.
    typical_bytes: int | None = None
    required_by: list[str] | None = None
    formatter: str | None = None
    unit: str | None = None
    precision: int | None = None
    individually_toggleable: bool | None = None
    default_off: bool | None = None
    note: str | None = None
    limit: int | None = None
    has_limit: bool | None = None
    # Custom-field entries only.
    show_in_dashboard: bool | None = None
    show_in_logs: bool | None = None
    filterable: bool | None = None
    value_type: str | None = None
    is_custom: bool | None = None


class LogFieldCatalogGroup(WireSafeModel):
    # ``get_groups_for_api`` emits every key for every group; the base group's id is ``None``.
    id: str | None
    label: str
    description: str
    locked: bool
    requires: str | None
    note: str | None
    recommended_with: list[str] | None
    total_bytes: int
    fields: list[str]


class LogFieldInsightDefinition(WireSafeModel):
    id: str
    category: str
    title: str
    description: str
    required_fields: list[str]
    required_groups: list[str]


class LogFieldPreset(WireSafeModel):
    label: str
    description: str
    groups: list[str]


class LogFieldsCatalogResponse(WireSafeModel):
    groups: list[LogFieldCatalogGroup] | None = None
    fields: list[LogFieldCatalogEntry] | None = None
    insights: list[LogFieldInsightDefinition] | None = None
    presets: dict[str, LogFieldPreset] | None = None
