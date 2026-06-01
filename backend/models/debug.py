"""Response models for the debug router.

Surface concrete schemas to the OpenAPI doc so the Debug Panel can use
the typed client (frontend/lib/api.ts) instead of a hand-rolled fetch.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class SqliteProfilerEntry(BaseModel):
    seq: int
    ts: str
    sql: str
    params_kind: str
    time_ms: float
    rows: int
    op: Literal["execute", "executemany", "executescript"]


class RecentSqliteResponse(BaseModel):
    queries: list[SqliteProfilerEntry]
    buffer_size: int
    buffer_cap: int
    dropped: int
    last_seq: int


class ClearSqliteResponse(BaseModel):
    ok: bool = True
    buffer_size: int
    buffer_cap: int
    dropped: int
