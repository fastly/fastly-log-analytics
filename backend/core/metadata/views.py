"""Saved-dashboard-view CRUD against the ``views`` table in metadata SQLite."""

from __future__ import annotations

from backend.core.metadata.base import get_con


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "service_id": r["service_id"],
            "name": r["name"],
            "filters_json": r["filters_json"],
            "time_range_type": r["time_range_type"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "page": r["page"],
            "created_at": str(r["created_at"]) if r["created_at"] is not None else "",
        }
        for r in rows
    ]


def save_view(service_id: str, view) -> dict:
    import uuid

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
    con.execute("DELETE FROM views WHERE id = ?", (view_id,))
    con.commit()
    return {"status": "success"}


def replace_views_for_service(service_id: str, views: list[dict]) -> None:
    """Replace all saved views for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE service_id = ?", (service_id,))
    if views:
        con.executemany(
            "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    v.get("id"),
                    v.get("service_id"),
                    v.get("name"),
                    v.get("filters_json"),
                    v.get("time_range_type"),
                    v.get("start_time"),
                    v.get("end_time"),
                    v.get("page"),
                    v.get("created_at"),
                )
                for v in views
            ],
        )
    con.commit()


def upsert_views_for_service(service_id: str, views: list[dict]) -> None:
    """Upsert saved views by id WITHOUT deleting local-only rows.

    Used by state_sync.import_admin_state on read_only analyst hosts so
    locally-created views (which the analyst created on their own pod) are
    preserved through every metadata_sync cron tick. Without this, the
    cron's wholesale DELETE+INSERT silently wiped any analyst-side view
    that hadn't been mirrored back to FOS — and ``export_admin_state``
    refuses to push from read_only hosts, so the loss was permanent.
    """
    if not views:
        return
    con = get_con(service_id)
    con.executemany(
        "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "name=excluded.name, filters_json=excluded.filters_json, "
        "time_range_type=excluded.time_range_type, start_time=excluded.start_time, "
        "end_time=excluded.end_time, page=excluded.page, created_at=excluded.created_at",
        [
            (
                v.get("id"),
                v.get("service_id"),
                v.get("name"),
                v.get("filters_json"),
                v.get("time_range_type"),
                v.get("start_time"),
                v.get("end_time"),
                v.get("page"),
                v.get("created_at"),
            )
            for v in views
        ],
    )
    con.commit()
