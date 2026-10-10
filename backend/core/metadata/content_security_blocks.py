"""Streaming → Content Security blocks, in the ``content_security_blocklist`` table.

One row per block pushed to the operator's blocklist HTTP function. A block is
a list of ``{category, value}`` conditions that must ALL match. It is stored
as canonical JSON (sorted by category, compact separators) so the text is
also the dedupe key. A row is active while ``unblocked_at`` is NULL;
unblocking stamps it rather than deleting, so the history survives alongside
the audit log.
"""

from __future__ import annotations

import json
import uuid

from backend.core.metadata.base import get_con
from backend.utils.date_utils import iso_z_now

_COLUMNS = "id, service_id, conditions, created_at, created_by, unblocked_at, unblocked_by"


def canonical_conditions(conditions: list[dict]) -> list[dict]:
    """Conditions in their stored order: sorted by category, only the two wire keys."""
    return sorted(({"category": c["category"], "value": c["value"]} for c in conditions), key=lambda c: c["category"])


def _encode(conditions: list[dict]) -> str:
    return json.dumps(canonical_conditions(conditions), separators=(",", ":"), ensure_ascii=False)


def _row(r) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "conditions": json.loads(r["conditions"]),
        "created_at": str(r["created_at"]),
        "created_by": r["created_by"],
        "unblocked_at": str(r["unblocked_at"]) if r["unblocked_at"] is not None else None,
        "unblocked_by": r["unblocked_by"],
    }


def list_active_blocks(service_id: str) -> list[dict]:
    """Active blocks for a service, newest first."""
    con = get_con(service_id)
    rows = con.execute(
        f"SELECT {_COLUMNS} FROM content_security_blocklist "
        "WHERE service_id = ? AND unblocked_at IS NULL ORDER BY created_at DESC, id",
        (service_id,),
    ).fetchall()
    return [_row(r) for r in rows]


def get_block(service_id: str, block_id: str) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        f"SELECT {_COLUMNS} FROM content_security_blocklist WHERE service_id = ? AND id = ?",
        (service_id, block_id),
    ).fetchone()
    return _row(row) if row else None


def find_active_block(service_id: str, conditions: list[dict]) -> dict | None:
    """The active block with exactly this condition set (order-insensitive), if any."""
    con = get_con(service_id)
    row = con.execute(
        f"SELECT {_COLUMNS} FROM content_security_blocklist "
        "WHERE service_id = ? AND conditions = ? AND unblocked_at IS NULL",
        (service_id, _encode(conditions)),
    ).fetchone()
    return _row(row) if row else None


def insert_block(service_id: str, conditions: list[dict], actor: str) -> dict:
    """Record a block the HTTP function has accepted; returns the stored row."""
    block_id = str(uuid.uuid4())
    con = get_con(service_id)
    con.execute(
        "INSERT INTO content_security_blocklist (id, service_id, conditions, created_by) VALUES (?, ?, ?, ?)",
        (block_id, service_id, _encode(conditions), actor),
    )
    con.commit()
    block = get_block(service_id, block_id)
    assert block is not None
    return block


def mark_unblocked(service_id: str, block_id: str, actor: str) -> dict | None:
    """Stamp an active block as reverted; returns the updated row (None if absent)."""
    con = get_con(service_id)
    con.execute(
        "UPDATE content_security_blocklist SET unblocked_at = ?, unblocked_by = ? "
        "WHERE service_id = ? AND id = ? AND unblocked_at IS NULL",
        (iso_z_now(), actor, service_id, block_id),
    )
    con.commit()
    return get_block(service_id, block_id)
