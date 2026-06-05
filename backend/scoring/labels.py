"""Admin session labels for the edge scorer.

Each label is one (service_id, sid) tuple tagged ``good`` / ``bad`` /
``neutral``. Stored in the per-service metadata SQLite DB (same file as
alerts, views, audit). The unique index on (service_id, sid) means
re-labeling a session updates the existing row rather than producing
duplicates — matches the admin's mental model ("I'm changing my mind
about this session," not "I'm creating a new label").

Labels feed [backend/scoring/evaluate.py](evaluate.py) for ROC-AUC
quality assessment of the trained matrix. Neutral rows are kept for
display but excluded from the AUC computation (intentionally uncertain
→ shouldn't bias precision/recall in either direction).

This module is intentionally thin — schema lives in
[backend/core/metadata_db.py](../core/metadata_db.py); we just provide
the typed CRUD surface plus the upsert-on-sid semantics the API
endpoints want.
"""

from __future__ import annotations

import uuid
from typing import Literal

from backend.core.metadata_db import get_con

Label = Literal["good", "bad", "neutral"]
ALLOWED_LABELS: frozenset[str] = frozenset({"good", "bad", "neutral"})


def _row_to_dict(r) -> dict:
    return {
        "id": r["id"],
        "service_id": r["service_id"],
        "sid": r["sid"],
        "label": r["label"],
        "notes": r["notes"] or "",
        "flagged_by": r["flagged_by"] or "",
        "sample_ip": r["sample_ip"] or "",
        "sample_ua": r["sample_ua"] or "",
        "sample_url": r["sample_url"] or "",
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


def save_label(
    service_id: str,
    sid: str,
    label: str,
    *,
    notes: str = "",
    flagged_by: str = "admin",
    sample_ip: str = "",
    sample_ua: str = "",
    sample_url: str = "",
) -> dict:
    """Upsert a label keyed on (service_id, sid).

    Re-labeling a session that's already labeled overwrites the prior
    label + notes + sample fields and bumps ``updated_at``. The original
    ``created_at`` and ``id`` are preserved so external references
    (e.g. UI rows) survive.
    """
    if label not in ALLOWED_LABELS:
        raise ValueError(f"label must be one of {sorted(ALLOWED_LABELS)}, got {label!r}")
    if not sid:
        raise ValueError("sid is required")

    con = get_con(service_id)
    new_id = str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO scoring_labels (
            id, service_id, sid, label, notes, flagged_by,
            sample_ip, sample_ua, sample_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(service_id, sid) DO UPDATE SET
            label = excluded.label,
            notes = excluded.notes,
            flagged_by = excluded.flagged_by,
            sample_ip = excluded.sample_ip,
            sample_ua = excluded.sample_ua,
            sample_url = excluded.sample_url,
            updated_at = datetime('now')
        """,
        (new_id, service_id, sid, label, notes, flagged_by, sample_ip, sample_ua, sample_url),
    )
    con.commit()
    # Re-read so we return whatever row landed (could be the existing one
    # if this was an UPDATE path, with its original id).
    row = con.execute(
        "SELECT * FROM scoring_labels WHERE service_id = ? AND sid = ?",
        (service_id, sid),
    ).fetchone()
    return _row_to_dict(row) if row else {"id": new_id, "service_id": service_id, "sid": sid, "label": label}


def list_labels(service_id: str, limit: int = 500) -> list[dict]:
    """Most-recent first. Limit is a safety cap; expect 0-10k labels total
    per service in any reasonable use."""
    con = get_con(service_id)
    # ROWID DESC as secondary sort: SQLite's datetime('now') is only
    # second-precision, so rows inserted within the same wall-clock
    # second otherwise return in implementation-defined order (which
    # tripped the most-recent-first test). ROWID is insertion-order
    # so the tie-break matches the admin's mental model.
    rows = con.execute(
        "SELECT * FROM scoring_labels WHERE service_id = ? ORDER BY updated_at DESC, ROWID DESC LIMIT ?",
        (service_id, int(limit)),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_label(service_id: str, sid: str) -> dict | None:
    """Look up the label for a single sid. None if not labeled.

    Test-only convenience: production callers use list_labels (bulk fetch
    for the admin UI) or get_label_by_id (after a save/update returns the
    id). Kept because the labels test suite uses it for round-trip
    verification."""
    con = get_con(service_id)
    row = con.execute(
        "SELECT * FROM scoring_labels WHERE service_id = ? AND sid = ?",
        (service_id, sid),
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_label_by_id(service_id: str, label_id: str) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT * FROM scoring_labels WHERE id = ?",
        (label_id,),
    ).fetchone()
    return _row_to_dict(row) if row else None


def update_label(
    service_id: str,
    label_id: str,
    *,
    label: str | None = None,
    notes: str | None = None,
) -> dict:
    """PATCH semantics — only update the fields that were passed."""
    con = get_con(service_id)
    sets: list[str] = []
    params: list = []
    if label is not None:
        if label not in ALLOWED_LABELS:
            raise ValueError(f"label must be one of {sorted(ALLOWED_LABELS)}, got {label!r}")
        sets.append("label = ?")
        params.append(label)
    if notes is not None:
        sets.append("notes = ?")
        params.append(notes)
    if not sets:
        # No-op update — just return current state without bumping
        # updated_at (avoids cache-buster noise from no-change PATCHes).
        return get_label_by_id(service_id, label_id) or {}
    sets.append("updated_at = datetime('now')")
    params.append(label_id)
    con.execute(f"UPDATE scoring_labels SET {', '.join(sets)} WHERE id = ?", params)
    con.commit()
    return get_label_by_id(service_id, label_id) or {}


def delete_label(service_id: str, label_id: str) -> dict:
    """Hard delete. Idempotent — deleting an already-deleted row returns
    success without raising."""
    con = get_con(service_id)
    con.execute("DELETE FROM scoring_labels WHERE id = ?", (label_id,))
    con.commit()
    return {"status": "success", "id": label_id}


def counts_by_label(service_id: str) -> dict[str, int]:
    """{label: count}. Used by the status panel's "you've labeled N sessions"
    summary. Includes 'good', 'bad', 'neutral' keys with 0 for missing."""
    con = get_con(service_id)
    rows = con.execute(
        "SELECT label, COUNT(*) AS n FROM scoring_labels WHERE service_id = ? GROUP BY label",
        (service_id,),
    ).fetchall()
    out = {"good": 0, "bad": 0, "neutral": 0}
    for r in rows:
        out[r["label"]] = int(r["n"])
    return out
