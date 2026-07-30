"""Cached ASN-name lookups against the ``asn_names`` table in metadata SQLite."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.core.metadata.base import get_con, get_con_readonly
from backend.utils.date_utils import iso_z, iso_z_now


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    import contextlib

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
    import contextlib

    with contextlib.closing(get_con_readonly(service_id)) as con:
        rows = con.execute(
            "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
            (name_ilike,),
        ).fetchall()
    return [int(r["asn"]) for r in rows]
