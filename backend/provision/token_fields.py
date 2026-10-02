"""Token (HMAC / JWT / CAT) subscriber-id extraction: system field + VCL.

The admin supplies a VCL expression that derives a subscriber id from the
request (typically a header or a ``regsub()`` over a token). The generated
``vcl_recv`` code sets ``req.http.x-subscriber-id`` from that expression; the
client-supplied header is unset alongside the other scrubs in
``fastly_api.get_scrub_vcl_statements`` (keyed on this field being enabled) so a
client cannot forge it. The system-managed custom field
``token_subscriber_id`` promotes the header into the log line.

Mirrors CMCD (``cmcd_fields.py``): the field is generated from code, hidden from
the user-editable list by ``_is_system_field`` (exact name), and must be
re-asserted on every ``log_fields`` write via ``system_fields`` — see Trap #27.
Extraction must be emitted BEFORE field capture — see Trap #29.
"""

from __future__ import annotations

from typing import Any

from backend.core.log_fields import validate_custom_field

SUBSCRIBER_ID_HEADER = "req.http.x-subscriber-id"
SUBSCRIBER_ID_FIELD = "token_subscriber_id"

_TOKEN_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": SUBSCRIBER_ID_FIELD,
        "label": "Subscriber ID",
        "vcl_log_expression": SUBSCRIBER_ID_HEADER,
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
        # Same headroom as cmcd_sid: 288//6 = 48 chars.
        "byte_limit": 288,
        "enabled": True,
    },
]

_TOKEN_FIELD_NAMES = {cf["name"] for cf in _TOKEN_CUSTOM_FIELDS}


def get_token_fields(enabled: bool) -> list[dict]:
    """Return the token custom fields if enabled, else an empty list."""
    if enabled:
        return [dict(cf) for cf in _TOKEN_CUSTOM_FIELDS]
    return []


def reconcile_token_custom_fields(custom_fields: list[dict] | None, *, enabled: bool) -> list[dict]:
    """Return ``custom_fields`` with the canonical token fields applied or stripped."""
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _TOKEN_FIELD_NAMES]
    return kept + get_token_fields(enabled)


def validate_subscriber_id_expr(expr: str | None) -> list[str]:
    """Validate the admin-supplied subscriber-id VCL expression.

    Reuses the custom-field expression rules (no semicolons, comments, or raw
    newlines; ≤ 512 chars) since the expression lands in a ``set`` statement
    exactly like a custom field's ``vcl_log_expression``. Warnings are dropped.
    """
    candidate = {
        "name": "token_lint",
        "label": "Subscriber ID",
        "vcl_log_expression": expr or "",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
    }
    return [e for e in validate_custom_field(candidate, []) if not e.startswith("WARN:")]


def generate_token_vcl_lines(subscriber_id_expr: str) -> list[str]:
    """Return the ``vcl_recv`` statements that populate ``x-subscriber-id``.

    Callers emit these inside the edge-only block, after the header scrub (which
    unsets ``x-subscriber-id``) and BEFORE ``get_capture_vcl_statements``.
    """
    errors = validate_subscriber_id_expr(subscriber_id_expr)
    if errors:
        raise ValueError(f"Invalid subscriber id expression: {'; '.join(errors)}")
    return [
        "# Token Extraction (vcl_recv) — must precede field capture",
        f"set {SUBSCRIBER_ID_HEADER} = {subscriber_id_expr.strip()};",
    ]
