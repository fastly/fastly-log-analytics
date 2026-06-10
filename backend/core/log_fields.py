"""Log field catalog and format generator for field-aware logging configuration.

Every loggable field is defined here: its VCL expression, DuckDB type, typical
byte cost, and which insights require it.  Nothing else in the codebase should
hard-code VCL log format strings.

Phase 7 migration in progress
-----------------------------
A new frozen-dataclass view of this catalog lives at
``backend/core/field_registry.py`` (`REGISTRY`, `BY_CODE`, `BY_GROUP`,
`WIRE_ORDER`). The new module is derived from `LOG_FIELD_CATALOG` below at
import time and stays byte-for-byte equivalent — a parity test in
``tests/core/test_field_registry.py`` guards both views.

Callers are migrating one-at-a-time per the order in
``pending-docs/phase_7_field_registry_migration.md``. While the migration
is in flight DO NOT add a new field by editing only the new registry: add
it here (as a dict) and the registry will pick it up automatically. After
the final caller migrates, the legacy `LOG_FIELD_CATALOG` literal will
be rewritten in place as `LogField(...)` calls and this module will shed
the dict layer.

Usage
-----
    from backend.core.log_fields import generate_log_format, estimate_log_line_bytes, PRESETS

    cfg = {"groups": ["A", "C", "D", "I"], "field_overrides": {"referer": False}}
    fmt = generate_log_format(cfg)         # → compact single-line JSON VCL string
    size = estimate_log_line_bytes(cfg)    # → ~490

Group IDs
---------
    None  Always-on (locked, cannot be disabled)
    A     Request Identity
    B     Cache Deep-Dive
    C     Infrastructure
    D     Geolocation Basic
    E     Geolocation Precision  (requires D)
    F     Network Quality Core
    G     Network Quality Deep   (requires F)
    H     Security: TLS Fingerprinting
    I     Security: Proxy & Anonymization
    J     WAF / NGWAF
    K     QUIC / HTTP3
    L     Origin Metrics
"""

import hashlib
import re

# ---------------------------------------------------------------------------
# Fastly runtime limits (distinct from the template-size cap enforced at
# provision time in backend/provision/fastly_api.py).
#
# Per https://docs.fastly.com/products/network-services-resource-limits the
# emitted log LINE is capped at 16 KiB for Deliver services (64 KiB for
# Compute). Past the cap Fastly silently truncates — there is no error
# surfaced to the customer, so a config whose typical line lands close to
# 16 KiB will start emitting corrupt JSON the moment per-request values
# (long URLs, fat headers) push it over. The template-size gate (8000
# chars) does NOT protect against this.
#
# We compare the estimate against a headroom-aware threshold rather than the
# raw cap so configs that *typically* fit are still flagged when their
# worst-case lines have no slack. ``estimate_log_line_bytes`` returns
# average bytes; production traffic regularly drives 2-3x that on URL- or
# UA-heavy requests, so the threshold sits at ~60% of the cap.
# ---------------------------------------------------------------------------

FASTLY_LOG_LINE_DELIVER_MAX = 16 * 1024  # 16 KiB hard cap, silent truncation
FASTLY_LOG_LINE_SAFE_MAX = int(FASTLY_LOG_LINE_DELIVER_MAX * 0.60)  # 9830 bytes

# ---------------------------------------------------------------------------
# Custom field validation constants
# ---------------------------------------------------------------------------

VALID_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")

_DUCKDB_RESERVED = frozenset(
    {
        "select",
        "from",
        "where",
        "table",
        "column",
        "index",
        "view",
        "join",
        "on",
        "as",
        "is",
        "in",
        "not",
        "null",
        "true",
        "false",
        "and",
        "or",
        "case",
        "when",
        "then",
        "else",
        "end",
        "order",
        "group",
        "by",
        "having",
        "limit",
        "offset",
        "union",
        "all",
        "distinct",
        "with",
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "add",
        "set",
        "values",
        "into",
        "exists",
        "between",
        "like",
        "ilike",
        "cast",
        "try_cast",
        "extract",
        "interval",
        "timestamp",
        "date",
        "time",
        "integer",
        "varchar",
        "boolean",
        "double",
        "bigint",
        "float",
        "struct",
        "list",
        "map",
        # Partition columns used internally
        "dt",
        "timestamp_hour",
    }
)

_DUCKDB_TYPE_VALUE_TYPE_COMPAT: dict[str, set[str]] = {
    "VARCHAR": {"string", "ip", "url"},
    "INTEGER": {"numeric"},
    "BIGINT": {"numeric"},
    "DOUBLE": {"numeric"},
    "BOOLEAN": {"boolean"},
}

# ---------------------------------------------------------------------------
# Field catalog
# ---------------------------------------------------------------------------

# ──────────────────────────────────────────────────────────────────────────
# Field catalog (carved out to backend/core/_log_fields_data.py for the
# v2.0 file-size sweep — LOG_FIELD_CATALOG alone is ~970 lines).
# ──────────────────────────────────────────────────────────────────────────
from backend.core._log_fields_data import (  # noqa: F401
    LOG_FIELD_CATALOG,
    GROUP_INFO,
    GROUP_DEPENDENCIES,
    PRESETS,
    INSIGHT_DEFINITIONS,
)

def resolve_enabled_fields(cfg: dict) -> set:
    """Expand group selections and per-field overrides into a flat set of enabled field IDs."""
    if cfg is None:
        # Default to standard groups if no config provided
        cfg = {"groups": PRESETS["standard"]["groups"], "field_overrides": {}}

    # Start with always-on fields
    enabled = {f["id"] for f in LOG_FIELD_CATALOG if f["group"] is None}

    # Add all fields from enabled groups (respecting dependency order)
    enabled_groups = set(cfg.get("groups", []))

    # Enforce dependencies: if a group requires another group, auto-enable it
    changed = True
    while changed:
        changed = False
        for grp, required in GROUP_DEPENDENCIES.items():
            if grp in enabled_groups and required not in enabled_groups:
                enabled_groups.add(required)
                changed = True

    for field in LOG_FIELD_CATALOG:
        if field["group"] in enabled_groups:
            enabled.add(field["id"])

    # Apply per-field overrides
    for field_id, on in cfg.get("field_overrides", {}).items():
        if on:
            enabled.add(field_id)
        else:
            enabled.discard(field_id)

    return enabled


def get_required_edge_headers(log_fields_config: dict) -> set:
    """Return the set of x-fos-edge-data keys required by the enabled log fields.

    Analyzes the VCL expressions of all enabled fields to determine which
    subfields of the x-fos-edge-data header must be captured at the edge.
    """
    enabled = resolve_enabled_fields(log_fields_config)
    required = set()
    # Regex to find x-fos-edge-data subfields in VCL expressions
    pattern = re.compile(r"req\.http\.x-fos-edge-data:([a-z0-9_]+)")
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            matches = pattern.findall(field["vcl"])
            required.update(matches)
    return required


def generate_log_format(log_fields_config: dict) -> str:
    """Build the VCL log format string from a log_fields config dict.

    Returns a compact single-line JSON string suitable for Fastly's logging endpoint.
    Fastly's S3/FOS logging endpoint emits one JSON object per line, so the format
    must not contain internal newlines — DuckDB's newline_delimited reader requires this.
    """
    enabled = resolve_enabled_fields(log_fields_config)
    limits = log_fields_config.get("field_limits") or {}

    parts = []
    for field in LOG_FIELD_CATALOG:
        if field["id"] in enabled:
            vcl = field["vcl"]
            if vcl is None:
                continue
            # Inject dynamic limits
            if field["id"] == "url":
                limit = limits.get("url", 2000)
                # Overwrite the static substr limit in the built-in VCL
                vcl = vcl.replace("substr(req.url, 0, 2000)", f"substr(req.url, 0, {limit})")
            elif field["id"] == "ua":
                # Security: keep the substr cap even when generating the
                # alternative VCL variant. The edge-side substr (in vcl_recv)
                # is a *first* truncation — but we never want a 100 KB header
                # to slip through if the edge snippet is missing or fails to
                # run (e.g., on a request that bypasses our snippet stack).
                # An unbounded UA can truncate the entire JSON log line at
                # the 16 KB Fastly limit, dropping the request from the audit
                # trail entirely (repudiation attack).
                ua_limit = limits.get("ua", 1000)
                vcl = (
                    f'"ua":"%{{json.escape(substr(if(req.http.x-fos-edge-data:ua != "",'
                    f' req.http.x-fos-edge-data:ua, req.http.User-Agent), 0, {ua_limit}))}}V"'
                )
            elif field["id"] == "referer":
                # Same reasoning as above — keep the substr cap.
                ref_limit = limits.get("referer", 1000)
                vcl = (
                    f'"referer":"%{{json.escape(substr(if(req.http.x-fos-edge-data:referer != "",'
                    f' req.http.x-fos-edge-data:referer, req.http.Referer), 0, {ref_limit}))}}V"'
                )

            parts.append(vcl)

    # Append enabled custom fields in alphabetical order for determinism
    for cf in sorted(log_fields_config.get("custom_fields", []), key=lambda x: x["name"]):
        if not cf.get("enabled", True):
            continue

        name = cf["name"]
        stage = cf.get("collection_stage", "edge")
        value_type = cf.get("value_type", "string")

        if stage == "deliver":
            # Deliver-stage fields (session-scoring) need TWO gates:
            #   1. edge-only (fastly.ff.visits_this_service == 0) — the
            #      shield POP never ran our scoring snippets, so the
            #      req.http subfields don't exist there.
            #   2. non-empty value — avoid breaking JSON.
            # Combined into ONE if() with compound AND so we don't end up
            # with nested if(if(...) != "", ...) which Fastly's parser
            # rejects ("if() condition must be a simple expression, not a
            # function call").
            raw_expr = cf.get("vcl_log_expression") or f"req.http.x-fos-edge-data:{name}"
            if value_type in ("numeric", "boolean"):
                # 014: ``!= ""`` only rejects empty strings — any other
                # text (`"true"`, ``"abc"``, ``"]"``) flows straight into
                # the JSON log line unquoted and breaks the JSON
                # structure, dropping the line from ingestion (log
                # injection / repudiation). Match a strict numeric form
                # so non-digit values fall through to ``"null"``.
                vcl_macro = (
                    f"if(fastly.ff.visits_this_service == 0 && "
                    f'{raw_expr} ~ "^-?[0-9]+(\\.[0-9]+)?$", {raw_expr}, "null")'
                )
                entry = f'"{name}":%{{{vcl_macro}}}V'
            else:
                # 016: clamp the string-field value to a sane length
                # (default 2000) BEFORE json.escape so a multi-megabyte
                # attacker-controlled custom field cannot push the log
                # line past Fastly's 16 KB limit and silently drop the
                # whole entry. The substr is INSIDE json.escape so the
                # encoded length stays bounded.
                cf_limit = int(cf.get("byte_limit") or limits.get(name) or 2000)
                vcl_macro = (
                    f'json.escape(if(fastly.ff.visits_this_service == 0, substr({raw_expr}, 0, {cf_limit}), ""))'
                )
                entry = f'"{name}":"%{{{vcl_macro}}}V"'
            parts.append(entry)
            continue

        if stage == "edge":
            expr = f"req.http.x-fos-edge-data:{name}"
        elif stage == "origin":
            expr = f"req.http.x-fos-origin-data:{name}"
        else:
            # Fallback if there's old data
            expr = f"req.http.x-fos-edge-data:{name}"

        if value_type in ("numeric", "boolean"):
            # 014: see deliver-stage comment above — strict numeric
            # regex instead of ``!= ""`` so a custom-field header value
            # like ``"]"`` cannot break out of the JSON log line.
            vcl_macro = f'if({expr} ~ "^-?[0-9]+(\\.[0-9]+)?$", {expr}, "null")'
            entry = f'"{name}":%{{{vcl_macro}}}V'
        else:
            # 016: substr-clamp the value before json.escape so an
            # oversized custom string field cannot push the line past
            # Fastly's 16 KB log-line limit.
            cf_limit = int(cf.get("byte_limit") or limits.get(name) or 2000)
            vcl_macro = f"json.escape(substr({expr}, 0, {cf_limit}))"
            entry = f'"{name}":"%{{{vcl_macro}}}V"'

        parts.append(entry)

    raw = "{" + ",".join(parts) + "}"
    # Collapse all whitespace to single spaces (same as the old load_log_format did)
    return re.sub(r"\s+", " ", raw).strip()


def estimate_log_line_bytes(log_fields_config: dict) -> int:
    """Return the estimated average uncompressed log line size in bytes."""
    enabled = resolve_enabled_fields(log_fields_config)
    field_bytes = sum(f["typical_bytes"] for f in LOG_FIELD_CATALOG if f["id"] in enabled)
    # JSON structural overhead: braces (2) + key quotes (2 per field) + colon (1) + comma+space (2 per field except last)
    structural = 2 + len(enabled) * 5

    # Add enabled custom fields
    custom_fields = log_fields_config.get("custom_fields", [])
    custom_count = 0
    for cf in custom_fields:
        if cf.get("enabled", True):
            field_bytes += cf.get("bytes_estimate", 0)
            custom_count += 1
    structural += custom_count * 5

    return structural + field_bytes


def check_log_line_budget(log_fields_config: dict) -> dict | None:
    """Return a warning dict when the estimated emitted log line approaches the
    Fastly Deliver 16 KiB cap, or None when the config is comfortably under.

    The returned shape matches the ``waf_warning`` envelope used elsewhere in
    the log-fields response so the frontend can render it without a new
    component. ``severity`` is "error" past the hard cap and "warn" past the
    safe-max headroom threshold.

    Why a soft threshold: ``estimate_log_line_bytes`` returns the *average*
    line size based on typical_bytes; real-request URLs and UAs routinely run
    2-3x that. A config whose average is already 12 KB will see truncation
    on long-URL traffic without any config-time signal. The safe-max sits at
    ~60% of the cap so the warning fires before production starts losing
    bytes.
    """
    estimate = estimate_log_line_bytes(log_fields_config)
    if estimate >= FASTLY_LOG_LINE_DELIVER_MAX:
        return {
            "code": "LOG_LINE_TOO_LARGE",
            "severity": "error",
            "estimate_bytes": estimate,
            "deliver_max_bytes": FASTLY_LOG_LINE_DELIVER_MAX,
            "safe_max_bytes": FASTLY_LOG_LINE_SAFE_MAX,
            "message": (
                f"Estimated log line is {estimate} bytes; Fastly Deliver services "
                f"silently truncate at {FASTLY_LOG_LINE_DELIVER_MAX} bytes (16 KiB). "
                "Disable some fields to avoid corrupt JSON in ingested logs."
            ),
        }
    if estimate >= FASTLY_LOG_LINE_SAFE_MAX:
        return {
            "code": "LOG_LINE_APPROACHING_LIMIT",
            "severity": "warn",
            "estimate_bytes": estimate,
            "deliver_max_bytes": FASTLY_LOG_LINE_DELIVER_MAX,
            "safe_max_bytes": FASTLY_LOG_LINE_SAFE_MAX,
            "message": (
                f"Estimated log line is {estimate} bytes; Fastly silently truncates "
                f"at {FASTLY_LOG_LINE_DELIVER_MAX} bytes (16 KiB). Per-request "
                "values (long URLs, fat headers) can push real lines past the cap. "
                "Consider trimming optional fields."
            ),
        }
    return None


def estimate_daily_bytes(log_fields_config: dict, req_per_day: int = 1_000_000) -> dict:
    """Return storage estimates for the given config and daily request volume."""
    line_bytes = estimate_log_line_bytes(log_fields_config)
    raw_bytes_day = line_bytes * req_per_day
    # Parquet compressed is roughly 10% of raw JSON (DuckDB ZSTD + dictionary encoding)
    parquet_bytes_day = int(raw_bytes_day * 0.10)
    parquet_bytes_30d = parquet_bytes_day * 30
    return {
        "line_bytes": line_bytes,
        "raw_mb_day": round(raw_bytes_day / (1024 * 1024), 1),
        "parquet_mb_day": round(parquet_bytes_day / (1024 * 1024), 1),
        "parquet_gb_30d": round(parquet_bytes_30d / (1024**3), 2),
    }


def format_hash(log_fields_config: dict) -> str:
    """Return a SHA256 fingerprint of the generated log format for drift detection."""
    fmt = generate_log_format(log_fields_config)
    return "sha256:" + hashlib.sha256(fmt.encode()).hexdigest()


def get_catalog_for_api(field_limits: dict[str, int] | None = None) -> list:
    """Return a simplified catalog suitable for the /api/log-fields/catalog endpoint."""
    result = []
    limits = field_limits or {}
    for f in LOG_FIELD_CATALOG:
        entry = {
            "id": f["id"],
            "group": f["group"],
            "label": f["label"],
            "description": f["description"],
            "duckdb_type": f["duckdb_type"],
            "typical_bytes": f["typical_bytes"],
            "required_by": f.get("required_by", []),
            "formatter": f.get("formatter"),
            "unit": f.get("unit"),
            "precision": f.get("precision"),
        }
        if f.get("individually_toggleable"):
            entry["individually_toggleable"] = True
        if f.get("note"):
            entry["note"] = f["note"]

        if f["id"] == "url":
            entry["limit"] = limits.get("url", 2000)
            entry["has_limit"] = True
        elif f["id"] == "ua":
            entry["limit"] = limits.get("ua", 1000)
            entry["has_limit"] = True
        elif f["id"] == "referer":
            entry["limit"] = limits.get("referer", 1000)
            entry["has_limit"] = True

        result.append(entry)
    return result


def get_groups_for_api() -> list:
    """Return group metadata suitable for the /api/log-fields/catalog endpoint."""
    result = []
    ordered_groups = [None, "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "METRICS"]
    for gid in ordered_groups:
        info = GROUP_INFO[gid]
        fields = [f for f in LOG_FIELD_CATALOG if f["group"] == gid]
        total_bytes = sum(f["typical_bytes"] for f in fields)
        result.append(
            {
                "id": gid,
                "label": info["label"],
                "description": info["description"],
                "locked": info.get("locked", False),
                "requires": info.get("requires"),
                "note": info.get("note"),
                "total_bytes": total_bytes,
                "fields": [f["id"] for f in fields],
            }
        )
    return result


def validate_group_deps(groups: list) -> list:
    """Return a list of error strings for any unsatisfied group dependencies."""
    errors = []
    for grp in groups:
        required = GROUP_DEPENDENCIES.get(grp)
        if required and required not in groups:
            errors.append(
                f"Group {grp} ({GROUP_INFO[grp]['label']}) requires Group {required} "
                f"({GROUP_INFO[required]['label']}) to also be enabled."
            )
    return errors


# ---------------------------------------------------------------------------
# Custom field support
# ---------------------------------------------------------------------------

_BUILTIN_FIELD_NAMES = frozenset(f["id"] for f in LOG_FIELD_CATALOG)
# Iceberg partition columns that must not be used as field names
_RESERVED_PARTITION_COLS = frozenset({"dt", "timestamp_hour"})


def validate_custom_field(field: dict, existing_names: list[str]) -> list[str]:
    """Validate a custom field definition dict.

    Parameters
    ----------
    field : dict
        The candidate custom field (user-supplied, not yet saved).
    existing_names : list[str]
        Names of all currently saved custom fields for this service
        (excluding the field being validated, for update operations).

    Returns
    -------
    list[str]
        List of human-readable error strings. Empty means valid.
        Warnings are prefixed with "WARN: ".
    """
    errors: list[str] = []
    name = field.get("name", "")

    # 1. Required keys
    for key in ("name", "label", "vcl_log_expression", "duckdb_type", "value_type", "bytes_estimate"):
        if key not in field or field[key] is None:
            errors.append(f"Missing required field: '{key}'")
    if errors:
        return errors

    # 2. Name regex
    if not VALID_NAME_RE.match(name):
        errors.append("Field name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars")

    # 3. DuckDB/SQL reserved word
    if name in _DUCKDB_RESERVED:
        errors.append(f"'{name}' is a DuckDB/SQL reserved word and cannot be used as a field name")

    # 4. Built-in field collision
    if name in _BUILTIN_FIELD_NAMES:
        errors.append(f"'{name}' is already a built-in field name")

    # 5. Duplicate custom field
    if name in existing_names:
        errors.append(f"A custom field named '{name}' already exists on this service")

    # 6. Label length
    label = field.get("label", "")
    if not (1 <= len(label) <= 80):
        errors.append("'label' must be 1–80 characters")

    # 7. Description length
    desc = field.get("description", "")
    if len(desc) > 500:
        errors.append("'description' must not exceed 500 characters")

    # 8. VCL expression
    expr = field.get("vcl_log_expression", "")
    if not expr.strip():
        errors.append("'vcl_log_expression' must not be empty")
    elif len(expr) > 512:
        errors.append(f"'vcl_log_expression' must be ≤ 512 characters (got {len(expr)})")
    elif "\n" in expr:
        errors.append("'vcl_log_expression' must not contain raw newlines")
    else:
        # VCL injection protection — semicolons end statements; comments hide injected code.
        # Curly braces are intentionally allowed: %{variable}V is standard VCL interpolation.
        if ";" in expr:
            errors.append("'vcl_log_expression' must not contain semicolons (;)")
        if "//" in expr or "/*" in expr or "#" in expr:
            errors.append("'vcl_log_expression' must not contain VCL comments (//, /*, or #)")

    # 9. duckdb_type enum
    valid_types = set(_DUCKDB_TYPE_VALUE_TYPE_COMPAT)
    duckdb_type = field.get("duckdb_type", "")
    if duckdb_type not in valid_types:
        errors.append(f"'duckdb_type' must be one of: {', '.join(sorted(valid_types))}")

    # 10. value_type enum
    all_value_types = {"string", "numeric", "boolean", "ip", "url"}
    value_type = field.get("value_type", "")
    if value_type not in all_value_types:
        errors.append(f"'value_type' must be one of: {', '.join(sorted(all_value_types))}")

    # 11. duckdb_type / value_type compatibility
    if duckdb_type in _DUCKDB_TYPE_VALUE_TYPE_COMPAT and value_type in all_value_types:
        if value_type not in _DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]:
            compat = ", ".join(sorted(_DUCKDB_TYPE_VALUE_TYPE_COMPAT[duckdb_type]))
            errors.append(
                f"'value_type' '{value_type}' is not compatible with 'duckdb_type' '{duckdb_type}'. "
                f"Compatible value_types: {compat}"
            )

    # 12. bytes_estimate range
    bytes_est = field.get("bytes_estimate", 0)
    try:
        bytes_est = int(bytes_est)
        if not (1 <= bytes_est <= 1024):
            errors.append("'bytes_estimate' must be between 1 and 1024")
    except (TypeError, ValueError):
        errors.append("'bytes_estimate' must be an integer")

    # 13. Validate collection_stage
    stage = field.get("collection_stage", "edge")
    if stage not in ("edge", "origin"):
        errors.append(f"'collection_stage' must be 'edge' or 'origin' (got '{stage}')")

    # 14. Warn on suspiciously low bytes_estimate
    if isinstance(bytes_est, int) and isinstance(name, str):
        min_bytes = len(name) + 5  # key + quotes + colon + value
        if 1 <= bytes_est < min_bytes:
            errors.append(
                f"WARN: 'bytes_estimate' ({bytes_est}) is less than the field name overhead "
                f'({min_bytes} bytes for "{name}":). The estimate is likely too low.'
            )

    return errors


def get_custom_fields_catalog_entries(log_fields_config: dict) -> list[dict]:
    """Return custom fields in the same shape as built-in catalog entries."""
    return [
        {
            "id": cf["name"],
            "label": cf["label"],
            "group": "custom",
            "duckdb_type": cf.get("duckdb_type", "VARCHAR"),
            "description": cf.get("description", ""),
            "show_in_dashboard": cf.get("show_in_dashboard", False),
            "show_in_logs": cf.get("show_in_logs", True),
            "filterable": cf.get("filterable", True),
            "value_type": cf.get("value_type", "string"),
            "is_custom": True,
        }
        for cf in log_fields_config.get("custom_fields", [])
        if cf.get("enabled", True)
    ]


_DEFAULT_LF_CONFIG: dict = {"schema_version": 2, "custom_fields": []}


def get_lf_config(cfg: dict) -> dict:
    """Return the log_fields config from a service config dict, with a safe default.

    Centralises the ``cfg.get("log_fields") or {...}`` pattern used across routers.
    """
    return cfg.get("log_fields") or _DEFAULT_LF_CONFIG.copy()
