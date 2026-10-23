"""Pydantic models for SQL Query Pad (/api/query, /api/presets, /api/query/schema)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.models._base import JsonRows, SchemaColumn, WireSafeModel
from backend.models.common import BaseResponse

QueryDataset = Literal["logs", "client_vitals", "client_errors"]


class QueryRequest(BaseModel):
    sql: str
    dataset: QueryDataset = "logs"
    # M1: bound max_rows so an analyst can't request tens of millions of rows
    # and OOM the worker (it's interpolated into ``LIMIT max_rows+1`` and the
    # result is fully materialized via ``to_arrow_table().to_pylist()`` before
    # truncation). The Pydantic clamp is the first line of defense; the repo's
    # ``execute_query`` re-clamps against ``MAX_QUERY_ROWS`` so internal callers
    # that bypass the model can't exceed it either.
    max_rows: int = Field(default=500, ge=1, le=10_000)
    explain: bool = False


class QueryResponse(WireSafeModel):
    """``POST /api/query`` result.

    Rows hold DuckDB-native values (Decimal, tz-aware datetimes, bytes) that
    pydantic's JSON mode would encode differently from ``jsonable_encoder``
    (the pre-typing wire path), so ``data`` is pre-encoded the same way.
    """

    columns: list[str]
    data: JsonRows
    row_count: int
    total_rows: int
    truncated: bool
    elapsed_ms: int
    explain_plan: str | None = None


class PresetQuery(WireSafeModel):
    name: str
    description: str
    sql: str


class QuerySchemaResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    source: str | None = None
    table_name: str = "logs"
    service_id: str | None = None
    schema_: list[SchemaColumn] = Field(default_factory=list, alias="schema", serialization_alias="schema")
    columns: list[SchemaColumn] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _sync_schema_and_columns(cls, data: Any) -> Any:
        if isinstance(data, dict):
            raw_cols = data.get("schema") if "schema" in data else data.get("columns", [])
            cols = [
                {
                    **c,
                    "column": c.get("column") or c.get("name") or "",
                    "name": c.get("name") or c.get("column") or "",
                    "type": c.get("type", ""),
                    "min": c.get("min"),
                    "max": c.get("max"),
                    "approx_unique": c.get("approx_unique"),
                    "null_percentage": c.get("null_percentage"),
                    "count": c.get("count"),
                }
                if isinstance(c, dict)
                else c
                for c in (raw_cols or [])
            ]
            return {
                **data,
                "schema": cols,
                "columns": data.get("columns") if "columns" in data else cols,
            }
        return data
