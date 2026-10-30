"""Admin state models, audit logs, ASN name cache, and saved dashboard views."""

from __future__ import annotations

import contextlib
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter

from backend.core.metadata.pg_pool import get_con, get_con_readonly
from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)


def _get_fos_client(source: dict[str, Any] | None = None) -> Any:
    from backend.core.duckdb import _get_fos_client as _impl

    return _impl(source or {})


def get_source_for_service(service_id: str | None = None) -> dict[str, Any] | None:
    from backend.core.duckdb import get_source_for_service as _impl

    return _impl(service_id or "")


# ── Pydantic v2 validation & serialization models (Round 30) ──────────────────


_JSON_DICT_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])
_JSON_LIST_ADAPTER: TypeAdapter[list[Any]] = TypeAdapter(list[Any])


def _coerce_str_or_empty(v: Any) -> str:
    return str(v) if v is not None else ""


def _coerce_json_dict(v: Any) -> dict[str, Any]:
    if isinstance(v, (str, bytes)):
        return _JSON_DICT_ADAPTER.validate_json(v or "{}")
    return v or {}


def _coerce_json_list(v: Any) -> list[Any]:
    if not v:
        return []
    if isinstance(v, (str, bytes)):
        return _JSON_LIST_ADAPTER.validate_json(v)
    return list(v)


def _coerce_json_opt_list(v: Any) -> list[Any] | None:
    if not v:
        return None
    if isinstance(v, (str, bytes)):
        return _JSON_LIST_ADAPTER.validate_json(v)
    return list(v)


_StrOrEmpty = Annotated[str, BeforeValidator(_coerce_str_or_empty)]
_JsonDict = Annotated[dict[str, Any], BeforeValidator(_coerce_json_dict)]
_JsonList = Annotated[list[Any], BeforeValidator(_coerce_json_list)]
_JsonOptList = Annotated[list[Any] | None, BeforeValidator(_coerce_json_opt_list)]


class _AuditLogRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    timestamp: Any = None
    source_name: Any = None
    event_type: Any = None
    details: Any = None
    actor: Any = None

    def as_insert_tuple(self) -> tuple[Any, ...]:
        return tuple(self.model_dump().values())

    def as_list_dict(self) -> dict[str, Any]:
        return {**self.model_dump(), "timestamp": str(self.timestamp) if self.timestamp is not None else ""}


class _AuditLogPagedEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Any = None
    timestamp: _StrOrEmpty = ""
    event_type: Any = None
    details: _JsonDict = Field(default_factory=dict)
    actor: Any = None
    source: str = "local"


class _ViewRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Any = None
    service_id: Any = None
    name: Any = None
    filters_json: Any = None
    time_range_type: Any = None
    start_time: Any = None
    end_time: Any = None
    page: Any = None
    created_at: Any = None

    def as_insert_tuple(self) -> tuple[Any, ...]:
        return tuple(self.model_dump().values())

    def as_list_dict(self) -> dict[str, Any]:
        return {**self.model_dump(), "created_at": str(self.created_at) if self.created_at is not None else ""}


class _AdminStatePayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    audit_logs: list[dict[str, Any]] = Field(default_factory=list, alias="_audit_logs")
    views: list[dict[str, Any]] = Field(default_factory=list, alias="_views")
    custom_fields: list[dict[str, Any]] | None = None


class _ScoringMatrixVersionEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    version: str
    key: str
    size_bytes: int = 0
    last_modified: str | None = None


# ── audit_logs ────────────────────────────────────────────────────────────────


def record_audit(service_id: str, event_type: str, details: dict, actor: str = "ui") -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO audit_logs (source_name, event_type, details, actor) VALUES (?, ?, ?, ?)",
        (service_id, event_type, json.dumps(details), actor),
    )
    con.commit()


def list_audit(service_id: str, limit: int = 200, since: str | None = None) -> list[dict]:
    """List audit log entries for a service, most recent first."""
    con = get_con(service_id)
    where_sql = "WHERE source_name = ? AND timestamp >= ?" if since else "WHERE source_name = ?"
    params: tuple[Any, ...] = (service_id, since, limit) if since else (service_id, limit)
    rows = con.execute(
        f"SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
        f"{where_sql} ORDER BY timestamp DESC LIMIT ?",
        params,
    ).fetchall()
    return [_AuditLogRow.model_validate(dict(r)).as_list_dict() for r in rows]


def get_audit_logs(
    service_id: str,
    *,
    event_type: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "timestamp",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated audit log query with optional event_type filter."""
    con = get_con(service_id)
    where = ["source_name = ?"]
    params: list = [service_id]
    if event_type and event_type != "all":
        where.append("event_type = ?")
        params.append(event_type)
    where_sql = "WHERE " + " AND ".join(where)

    total = int(con.execute(f"SELECT count(*) FROM audit_logs {where_sql}", params).fetchone()[0])

    valid_sort_cols = {"timestamp", "event_type", "actor"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "timestamp"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    per_page = max(1, min(per_page, 100))
    offset = max(0, page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, timestamp, event_type, details, actor
            FROM audit_logs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [_AuditLogPagedEntry.model_validate(dict(r)).model_dump() for r in rows]
    return total, entries


def export_audit(service_id: str, limit: int = 200) -> list[dict]:
    """Used by state_sync.export_admin_state — same as list_audit but with a stable column shape."""
    return list_audit(service_id, limit=limit)


# ── asn_names cache ──────────────────────────────────────────────────────────


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    fresh_cutoff = iso_z(datetime.now(UTC) - timedelta(days=max_age_days))
    placeholders = ",".join("?" * len(asns))
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            f"SELECT asn, name FROM asn_names WHERE asn IN ({placeholders}) AND fetched_at >= ?",
            list(asns) + [fresh_cutoff],
        ).fetchall()
    return {int(r["asn"]): r["name"] for r in rows}


def upsert_asn_names(service_id: str, mapping: dict[int, str]) -> None:
    if not mapping:
        return
    con = get_con(service_id)
    now = iso_z_now()
    con.executemany(
        "INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(asn) DO UPDATE SET name = excluded.name, fetched_at = excluded.fetched_at",
        [(int(asn), name, now) for asn, name in mapping.items()],
    )
    con.commit()


def asn_ints_for_search(service_id: str, name_ilike: str) -> list[int]:
    """Return ASN integers whose cached name matches the given LIKE pattern.

    Used by the dashboard ASN search to pre-fetch matching ASNs and inline them
    into a DuckDB IN clause (avoids cross-engine JOINs).
    """
    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
            (name_ilike,),
        ).fetchall()
    return [int(r["asn"]) for r in rows]


# ── Saved-dashboard-view CRUD ────────────────────────────────────────────────


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [_ViewRow.model_validate(dict(r)).as_list_dict() for r in rows]


def save_view(service_id: str, view) -> dict:
    con = get_con(service_id)
    view_id = view.id or str(uuid.uuid4())
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            view_id,
            service_id,
            view.name,
            view.filters_json,
            view.time_range_type,
            view.start_time,
            view.end_time,
            view.page,
        ),
    )
    con.commit()
    return {"id": view_id, "status": "success"}


def delete_view(service_id: str, view_id: str) -> dict:
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE id = ? AND service_id = ?", (view_id, service_id))
    con.commit()
    return {"status": "success"}
