"""Token (HMAC / JWT / CAT) extraction: system fields + VCL.

The admin supplies a VCL expression per id — subscriber id and/or content id —
that derives the value from the request (typically a header or a ``regsub()``
over a token). The generated ``vcl_recv`` code sets the id's header
(``req.http.x-cs-sub-id`` / ``req.http.x-cs-content-id``) from that expression;
the client-supplied header is unset alongside the other scrubs in
``fastly_api.get_scrub_vcl_statements`` (keyed on the field being enabled) so a
client cannot forge it. The system-managed custom field (``cs_sub_id`` /
``cs_content_id``) promotes the header into the log line.

Each id is optional (Token needs at least one), and a field exists only while
its expression is configured: the log-line budget reserves a field's cap even
when it logs empty, so an unconfigured id must not take budget (Trap #30).

Mirrors CMCD (``cmcd_fields.py``): fields are generated from code, hidden from
the user-editable list by ``_is_system_field`` (exact name), and must be
re-asserted on every ``log_fields`` write via ``system_fields`` — see Trap #27.
Extraction must be emitted BEFORE field capture — see Trap #29.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, NamedTuple

from backend.core.log_fields import validate_custom_field


class TokenId(NamedTuple):
    config_key: str  # key under cfg["token"] holding the VCL expression
    field: str  # system custom field (log column)
    header: str  # request header the expression sets
    label: str


SUBSCRIBER_ID = TokenId("subscriber_id_expr", "cs_sub_id", "req.http.x-cs-sub-id", "Subscriber ID")
CONTENT_ID = TokenId("content_id_expr", "cs_content_id", "req.http.x-cs-content-id", "Content ID")

# Canonical order — decides custom_fields order, so keep it stable (see the
# deterministic-order note in system_fields.reconcile_system_custom_fields).
TOKEN_IDS: tuple[TokenId, ...] = (SUBSCRIBER_ID, CONTENT_ID)

SUBSCRIBER_ID_HEADER = SUBSCRIBER_ID.header
SUBSCRIBER_ID_FIELD = SUBSCRIBER_ID.field
CONTENT_ID_HEADER = CONTENT_ID.header
CONTENT_ID_FIELD = CONTENT_ID.field


class EnforcementField(NamedTuple):
    field: str  # system custom field (log column)
    header: str  # request header the operator's VCL sets
    label: str
    description: str
    chars: int  # longest value the VCL emits


# Content Security enforcement outcome, stamped on the request by the operator's
# VCL (block / throttle / rate-limit checks) and logged as-is. ``cs_status`` is
# one of allowed / blocked / throttled / check_failed / counted / rate_limited.
# The mode says whether the status was enforced (ENFORCE) or only recorded (WARN),
# so reports must not treat a WARN ``blocked`` as a blocked request.
ENFORCEMENT_FIELDS: tuple[EnforcementField, ...] = (
    EnforcementField(
        "cs_status",
        "req.http.cs-status",
        "Enforcement Status",
        "Content Security outcome: allowed, blocked, throttled, check_failed, counted or rate_limited.",
        12,
    ),
    EnforcementField(
        "cs_mode",
        "req.http.cs-mode",
        "Block Mode",
        "Whether the block/throttle result was enforced (ENFORCE) or only recorded (WARN).",
        7,
    ),
    EnforcementField(
        "cs_rl_mode",
        "req.http.cs-rl-mode",
        "Rate Limit Mode",
        "Whether the rate-limit result was enforced (ENFORCE) or only recorded (WARN).",
        7,
    ),
    EnforcementField(
        "cs_pace_kbps",
        "req.http.cs-pace-kbps",
        "Throttle Rate (KB/s)",
        "Effective throttle rate in KB/s; only set when the request was throttled.",
        6,
    ),
    EnforcementField(
        "cs_policy_id",
        "req.http.cs-policy",
        "Policy ID",
        "The Content Security policy whose rules were evaluated for the request, when one applied.",
        36,
    ),
    EnforcementField(
        "cs_policy_reason",
        "req.http.cs-reason",
        "Policy Reason",
        "Which rule decided the outcome: a category (country, asn, ...), country_regex, subscriber_id, ip, or exempt:<category>.",
        24,
    ),
)

# Field name → header it logs; drives the per-field scrub in fastly_api. The
# enforcement headers are included so a client can't forge them either (the
# operator's VCL sets them after this scrub runs).
TOKEN_FIELD_HEADERS: dict[str, str] = {
    **{t.field: t.header for t in TOKEN_IDS},
    **{e.field: e.header for e in ENFORCEMENT_FIELDS},
}

_TOKEN_FIELD_NAMES = set(TOKEN_FIELD_HEADERS)


def _field_def(t: TokenId) -> dict[str, Any]:
    return {
        "name": t.field,
        "label": t.label,
        "vcl_log_expression": t.header,
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
        # Same headroom as cmcd_sid: 288//6 = 48 chars.
        "byte_limit": 288,
        "enabled": True,
    }


def _enforcement_field_def(e: EnforcementField) -> dict[str, Any]:
    # "deliver" stage: the outcome is only known after vcl_recv's capture has run
    # (the check restarts the request), so it is promoted in vcl_deliver instead.
    return {
        "name": e.field,
        "label": e.label,
        "description": e.description,
        "vcl_log_expression": e.header,
        "collection_stage": "deliver",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": e.chars + 8,
        "byte_limit": e.chars * 6,
        "enabled": True,
    }


_TOKEN_CUSTOM_FIELDS: list[dict[str, Any]] = [_field_def(t) for t in TOKEN_IDS] + [
    _enforcement_field_def(e) for e in ENFORCEMENT_FIELDS
]


def configured_token_fields(token_cfg: dict | None) -> frozenset[str]:
    """Names of the token fields to log: Token enabled AND that id's expression set.

    The enforcement fields are added when ``token_cfg["enforcement"]`` is true: the
    operator's VCL sets those headers, so there is no expression to configure.
    """
    token_cfg = token_cfg if isinstance(token_cfg, dict) else {}
    if not token_cfg.get("enabled"):
        return frozenset()
    names = {t.field for t in TOKEN_IDS if (token_cfg.get(t.config_key) or "").strip()}
    if token_cfg.get("enforcement"):
        names.update(e.field for e in ENFORCEMENT_FIELDS)
    return frozenset(names)


def get_token_fields(fields: Iterable[str]) -> list[dict]:
    """Return the canonical definitions for ``fields``, in canonical order."""
    wanted = set(fields)
    return [dict(cf) for cf in _TOKEN_CUSTOM_FIELDS if cf["name"] in wanted]


def reconcile_token_custom_fields(custom_fields: list[dict] | None, *, fields: Iterable[str]) -> list[dict]:
    """Return ``custom_fields`` with exactly the canonical token ``fields`` applied."""
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _TOKEN_FIELD_NAMES]
    return kept + get_token_fields(fields)


def _validate_expr(expr: str) -> list[str]:
    """Custom-field expression rules (no semicolons, comments, or raw newlines;
    ≤ 512 chars) — the expression lands in a ``set`` statement exactly like a
    custom field's ``vcl_log_expression``. Warnings are dropped."""
    candidate = {
        "name": "token_lint",
        "label": "Token",
        "vcl_log_expression": expr,
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
    }
    return [e for e in validate_custom_field(candidate, []) if not e.startswith("WARN:")]


def validate_token_exprs(
    subscriber_id_expr: str | None, content_id_expr: str | None, *, enforcement: bool = False
) -> list[str]:
    """Validate the Token expressions: each set one safe, and something selected.

    A Content Security config needs at least one of: a subscriber id expression, a
    content id expression, or the enforcement log (which needs no expression).
    """
    exprs = {SUBSCRIBER_ID: (subscriber_id_expr or "").strip(), CONTENT_ID: (content_id_expr or "").strip()}
    if not enforcement and not any(exprs.values()):
        return ["Configure at least one of Subscriber ID, Content ID or the enforcement log"]
    errors: list[str] = []
    for t, expr in exprs.items():
        if expr:
            errors.extend(f"{t.label}: {e}" for e in _validate_expr(expr))
    return errors


def generate_token_vcl_lines(subscriber_id_expr: str | None = None, content_id_expr: str | None = None) -> list[str]:
    """Return the ``vcl_recv`` statements that populate the configured id headers.

    Callers emit these inside the edge-only block, after the header scrub (which
    unsets the headers) and BEFORE ``get_capture_vcl_statements``.
    """
    errors = validate_token_exprs(subscriber_id_expr, content_id_expr, enforcement=True)
    if errors:
        raise ValueError(f"Invalid token expression: {'; '.join(errors)}")
    lines = ["# Token Extraction (vcl_recv) — must precede field capture"]
    for t, expr in ((SUBSCRIBER_ID, subscriber_id_expr), (CONTENT_ID, content_id_expr)):
        if expr and expr.strip():
            lines.append(f"set {t.header} = {expr.strip()};")
    return lines


def resolve_token_exprs(
    stored_token: dict | None, subscriber_id_expr: str | None, content_id_expr: str | None
) -> tuple[str, str]:
    """Merge requested expressions over the stored ones.

    ``None`` keeps the stored expression; any string (including ``""``, which
    clears that id) replaces it. Returned values are stripped.
    """
    stored = stored_token if isinstance(stored_token, dict) else {}
    sub = subscriber_id_expr if subscriber_id_expr is not None else stored.get(SUBSCRIBER_ID.config_key, "")
    content = content_id_expr if content_id_expr is not None else stored.get(CONTENT_ID.config_key, "")
    return (sub or "").strip(), (content or "").strip()
