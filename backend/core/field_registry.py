"""FieldRegistry — frozen-dataclass read view over the log-field catalog.

Adds typed, immutable LogField rows on top of the dict-literal catalog in
`backend/core/_log_fields_data.py`. Read paths (validators, scoring matrix
labels, debug-panel renderers, SQL-shape inference) use the registry;
authoring stays in dict form because the dict literal is the most readable
shape for declaring ~80 fields with descriptions and VCL expressions.

The duality is intentional, not in-flight migration:

- **Authoring view** (`_log_fields_data.LOG_FIELD_CATALOG`) — dict literals
  grouped by section comments. Optimised for human review of new fields.
- **Read view** (`field_registry.REGISTRY`) — frozen LogField tuple +
  BY_CODE map. Optimised for typed access (`f.duck_type`,
  `f.has_security_hook`) without per-call dict lookups.

`_field_from_dict` is the stable adapter that produces one LogField per dict
entry at import time. Both views must stay byte-for-byte equivalent — the
boot-time test
(`tests/core/test_field_registry.py::test_registry_codes_match_log_fields`)
asserts equality, so any drift fails CI before deploy.

Wire-order invariant
--------------------
The order of `REGISTRY` is byte-for-byte identical to `LOG_FIELD_CATALOG`
because the Rust scorer in `compute/` reads emitted JSON keys positionally
when streaming. Reordering rows here without a coordinated change in
`compute/` will silently break scorer parity.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

# ---------------------------------------------------------------------------
# Enums (typed counterparts for the stringly-typed columns in the catalog)
# ---------------------------------------------------------------------------


class DuckType(StrEnum):
    """DuckDB column types in use across the field catalog.

    Values are kept identical to the strings that appear in the catalog's
    `duckdb_type` key so cross-referencing diffs is trivial.
    """

    TIMESTAMP = "TIMESTAMP"
    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    UTINYINT = "UTINYINT"
    USMALLINT = "USMALLINT"
    UINTEGER = "UINTEGER"
    UBIGINT = "UBIGINT"
    BIGINT = "BIGINT"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"


class Agg(StrEnum):
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    GROUP_BY = "group_by"


class FilterOp(StrEnum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    IN = "in"
    NOT_IN = "not_in"


class Group(StrEnum):
    """Field grouping. Values match the legacy `group` column.

    `CORE` replaces the legacy `None` sentinel for always-on fields so the
    type can stay `Group` end-to-end instead of `Group | None`. The
    `from_legacy` classmethod handles the conversion at boundaries.
    """

    CORE = "CORE"  # legacy None
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    I = "I"  # noqa: E741 — single-letter group code is the public contract
    J = "J"
    K = "K"
    L = "L"
    M = "M"
    METRICS = "METRICS"
    VIRTUAL = "VIRTUAL"
    INTERNAL = "INTERNAL"

    @classmethod
    def from_legacy(cls, raw: str | None) -> Group:
        """Translate the legacy `None | "A".."M" | "METRICS" | "VIRTUAL" | "INTERNAL"`."""
        if raw is None:
            return cls.CORE
        return cls(raw)

    def to_legacy(self) -> str | None:
        """Inverse of `from_legacy`. Lets API serializers keep the wire shape."""
        return None if self is Group.CORE else self.value


# ---------------------------------------------------------------------------
# Group dependencies (mirrors `GROUP_DEPENDENCIES` from log_fields.py)
# ---------------------------------------------------------------------------

_GROUP_REQS: Mapping[Group, Group] = MappingProxyType(
    {
        Group.E: Group.D,  # Precision geo requires basic geo
        Group.G: Group.F,  # Deep network requires core network
    }
)


# ---------------------------------------------------------------------------
# Type-driven derivation: aggregations + filter ops + security hook detection
# ---------------------------------------------------------------------------

_NUMERIC: frozenset[DuckType] = frozenset(
    {
        DuckType.UTINYINT,
        DuckType.USMALLINT,
        DuckType.UINTEGER,
        DuckType.UBIGINT,
        DuckType.BIGINT,
        DuckType.INTEGER,
        DuckType.FLOAT,
        DuckType.DOUBLE,
    }
)


def _aggs_for(t: DuckType) -> frozenset[Agg]:
    """Aggregations allowed on a column of this DuckDB type.

    Single rule for the whole catalog: numeric columns add sum/avg/min/max;
    timestamps add min/max; booleans + varchars only support count and
    grouping. This is the legacy `valid_aggregations` list, derived rather
    than hand-maintained.
    """
    base = {Agg.COUNT, Agg.COUNT_DISTINCT, Agg.GROUP_BY}
    if t in _NUMERIC:
        base |= {Agg.SUM, Agg.AVG, Agg.MIN, Agg.MAX}
    elif t is DuckType.TIMESTAMP:
        base |= {Agg.MIN, Agg.MAX}
    return frozenset(base)


def _ops_for(t: DuckType) -> frozenset[FilterOp]:
    """Filter operators allowed on a column of this DuckDB type."""
    if t is DuckType.BOOLEAN:
        return frozenset({FilterOp.EQ, FilterOp.NEQ})
    if t is DuckType.TIMESTAMP:
        return frozenset(
            {
                FilterOp.EQ,
                FilterOp.NEQ,
                FilterOp.GT,
                FilterOp.GTE,
                FilterOp.LT,
                FilterOp.LTE,
            }
        )
    if t in _NUMERIC:
        return frozenset(
            {
                FilterOp.EQ,
                FilterOp.NEQ,
                FilterOp.GT,
                FilterOp.GTE,
                FilterOp.LT,
                FilterOp.LTE,
                FilterOp.IN,
                FilterOp.NOT_IN,
            }
        )
    # VARCHAR / unknown: full string ops
    return frozenset(
        {
            FilterOp.EQ,
            FilterOp.NEQ,
            FilterOp.CONTAINS,
            FilterOp.STARTS_WITH,
            FilterOp.IN,
            FilterOp.NOT_IN,
        }
    )


# Two patterns mark a field's VCL expression as "interpolates an
# attacker-influenced value": `json.escape(...)` for string-typed values, or a
# digits-only regex `~ "^...$"` for numeric values that would otherwise
# break out of the JSON log line. Detecting these here means the security
# regression sweep can ask "did anyone add a new field without a hook?"
# without re-implementing the rule per call site.
_SECURITY_HOOK_RE = re.compile(r"json\.escape\(|~\s*\"\^")


def _has_security_hook(vcl: str | None) -> bool:
    if vcl is None:
        return False
    return bool(_SECURITY_HOOK_RE.search(vcl))


# ---------------------------------------------------------------------------
# LogField dataclass — the row type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogField:
    """One row of the field catalog.

    Frozen + slotted: mutation throws at runtime, instances are hashable
    (usable as dict keys), per-instance memory is minimised. Constructed
    via the catalog adapter at module init time (`_field_from_dict`); the
    dict-authored catalog is the maintained source, this is the typed read
    view over it.

    Notes on field semantics that the dataclass shape captures:

    - `vcl=None` means the field is derived (computed by DuckDB SQL) or
      virtual (synthesised during analysis). `is_derived` is the public
      accessor.
    - `substr_cap` is the byte cap currently baked into the VCL literal for
      url/ua/referer (and any custom fields routed through this registry).
      `render_vcl(limits)` injects a runtime override without forcing every
      call site to know which fields have caps.
    - `required_by` is a tuple of insight IDs that name this field as a
      hard dependency. The legacy list is mutable; this is intentionally
      not.
    """

    code: str
    label: str
    group: Group
    duck_type: DuckType

    # All optional; defaulted for legacy compat with mostly-bare entries.
    description: str = ""
    vcl: str | None = None
    typical_bytes: int = 0
    required_by: tuple[str, ...] = ()
    substr_cap: int | None = None
    individually_toggleable: bool = False
    # Opt-in field: excluded from ``resolve_enabled_fields`` even when its
    # group is enabled; emits only when explicitly turned on via
    # ``field_overrides``. See ``log_fields.DEFAULT_OFF_FIELD_IDS``.
    default_off: bool = False
    formatter: str | None = None
    unit: str | None = None
    precision: int | None = None
    note: str | None = None

    # ---- Derived properties (zero LOC at call sites) -----------------------

    @property
    def is_derived(self) -> bool:
        """True for metrics/virtual/internal fields (`vcl is None`)."""
        return self.vcl is None

    @property
    def is_always_on(self) -> bool:
        """True for fields in the locked CORE group."""
        return self.group is Group.CORE

    @property
    def has_security_hook(self) -> bool:
        """True if the field's VCL interpolates an attacker-influenced value
        through `json.escape(...)` or a digit regex guard.

        Replaces the implicit "every group L field has `~ \"^[0-9]+$\"`" rule
        with an explicit test the security regression sweep can read.
        """
        return _has_security_hook(self.vcl)

    @property
    def valid_aggs(self) -> frozenset[Agg]:
        """Aggregations allowed in queries (derived from `duck_type`)."""
        return _aggs_for(self.duck_type)

    @property
    def valid_ops(self) -> frozenset[FilterOp]:
        """Filter operators allowed in queries (derived from `duck_type`)."""
        return _ops_for(self.duck_type)

    # ---- Behaviour: render VCL with runtime overrides ----------------------

    def render_vcl(self, limits: Mapping[str, int] | None = None) -> str | None:
        """Return the VCL fragment with the runtime substr cap injected.

        Replaces the `if field["id"] == "url": vcl = vcl.replace(...)` ladder
        in `generate_log_format()`. For fields without a `substr_cap`, the
        VCL is returned unchanged. For derived fields, returns None.

        The cap-substitution is intentionally minimal: it does NOT regenerate
        the URL/UA/Referer VCL from scratch (the legacy code does, for ua and
        referer, because the substr cap there is also baked into the
        json.escape boundary). When the caller migration lands, the
        ua/referer renderers will be expressed as small helper methods on
        the LogField subclasses for those specific codes — but that's a
        Phase 8 concern, not scaffolding.
        """
        if self.vcl is None:
            return None
        if self.substr_cap is None or limits is None:
            return self.vcl
        override = limits.get(self.code)
        if override is None or override == self.substr_cap:
            return self.vcl
        # Replace the literal cap. The replace is targeted at `, 0, {cap})`
        # which only appears inside the substr() boundary in the catalog.
        return self.vcl.replace(f", 0, {self.substr_cap})", f", 0, {override})")


# ---------------------------------------------------------------------------
# Legacy-dict → LogField construction
# ---------------------------------------------------------------------------


_SUBSTR_RE = re.compile(r"substr\([^,]+,\s*0,\s*(\d+)\)")


def _detect_substr_cap(vcl: str | None) -> int | None:
    """Find the byte cap baked into a `substr(expr, 0, N)` call in VCL.

    Mirrors the legacy code's hard-coded knowledge that url/ua/referer (and
    any custom field VCL the catalog ships) carry a cap in the literal.
    Returns None when no substr call is present or the cap can't be parsed.
    """
    if vcl is None:
        return None
    m = _SUBSTR_RE.search(vcl)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:  # pragma: no cover — regex guarantees digits
        return None


def _field_from_dict(d: Mapping[str, Any]) -> LogField:
    """Build a `LogField` from a `LOG_FIELD_CATALOG` dict entry.

    Stable adapter — runs once per field at module init time. The dict-
    literal authoring format and the LogField read view both stay; this
    function is the single bridge that derives the latter from the former.

    ``d`` is typed ``Mapping[str, Any]`` (not ``Mapping[str, object]``) to
    match the catalog literal's annotation in _log_fields_data.py — the
    heterogeneous value types (str / int / bool / list / None) are
    discriminated per-key here rather than at the type-system level.
    """
    code = str(d["id"])
    raw_group = d.get("group")
    group = Group.from_legacy(raw_group if raw_group is None else str(raw_group))
    duck_type = DuckType(str(d["duckdb_type"]))
    vcl_raw = d.get("vcl")
    vcl: str | None = None if vcl_raw is None else str(vcl_raw)
    required_by_raw = d.get("required_by") or ()
    required_by = tuple(str(x) for x in required_by_raw)
    return LogField(
        code=code,
        label=str(d.get("label", code)),
        group=group,
        duck_type=duck_type,
        description=str(d.get("description", "")),
        vcl=vcl,
        typical_bytes=int(d.get("typical_bytes", 0) or 0),
        required_by=required_by,
        substr_cap=_detect_substr_cap(vcl),
        individually_toggleable=bool(d.get("individually_toggleable", False)),
        default_off=bool(d.get("default_off", False)),
        formatter=_opt_str(d.get("formatter")),
        unit=_opt_str(d.get("unit")),
        precision=_opt_int(d.get("precision")),
        note=_opt_str(d.get("note")),
    )


def _opt_str(v: object) -> str | None:
    return None if v is None else str(v)


def _opt_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):  # pragma: no cover — catalog uses ints
        return None


# ---------------------------------------------------------------------------
# Registry construction
# ---------------------------------------------------------------------------

# Import the catalog at module init. The dict literals are the authoring
# format and the LogField tuple is the read view. Both must agree byte-
# for-byte; the cheapest way to guarantee that is to derive one from the
# other, with a boot-time test asserting the equivalence.
from backend.core.log_fields import LOG_FIELD_CATALOG as _CATALOG  # noqa: E402

REGISTRY: tuple[LogField, ...] = tuple(_field_from_dict(entry) for entry in _CATALOG)
"""Tuple of every known log field, in wire order (matches Rust scorer)."""


BY_CODE: Mapping[str, LogField] = MappingProxyType({f.code: f for f in REGISTRY})
"""Code → field lookup, O(1). Read-only view."""

if len(BY_CODE) != len(REGISTRY):  # pragma: no cover — guards a programmer mistake
    raise RuntimeError("duplicate field codes in REGISTRY — check log_fields.py for collisions")


def _group_index() -> Mapping[Group, tuple[LogField, ...]]:
    """Group → ordered tuple of fields belonging to it."""
    bucket: dict[Group, list[LogField]] = {g: [] for g in Group}
    for f in REGISTRY:
        bucket[f.group].append(f)
    return MappingProxyType({g: tuple(items) for g, items in bucket.items()})


BY_GROUP: Mapping[Group, tuple[LogField, ...]] = _group_index()
"""Group → ordered tuple of fields. Read-only view."""


WIRE_ORDER: tuple[str, ...] = tuple(f.code for f in REGISTRY if f.vcl is not None)
"""Codes that emit a token in the VCL log line, in emission order.

This is the byte-pinned contract with the Rust scorer in `compute/`. Any
diff to this tuple needs a coordinated scorer-side change. The test
`test_registry_codes_match_log_fields` is the boot-time gate.
"""


SECURITY_HOOK_CODES: frozenset[str] = frozenset(f.code for f in REGISTRY if f.has_security_hook)
"""Codes whose VCL expressions go through a security guard (json.escape /
digit regex). Read by `test_no_trace_leakage_sweep.py`-style audits to
confirm new fields don't bypass the convention."""


DEFAULT_OFF_CODES: frozenset[str] = frozenset(f.code for f in REGISTRY if f.default_off)
"""Codes flagged opt-in (``default_off``): excluded from
``resolve_enabled_fields`` when their group is enabled, emitted only on an
explicit ``field_overrides`` opt-in. Mirrors ``log_fields.DEFAULT_OFF_FIELD_IDS``
(the two views must agree — parity guarded by the registry tests)."""


# ---------------------------------------------------------------------------
# Lookup helpers (the public API the migration will route callers through)
# ---------------------------------------------------------------------------


def get(code: str) -> LogField:
    """Return the field with the given code. Raises `KeyError` on miss.

    Use this in router code paths where an unknown code is a programmer
    bug, not a user-input failure. For user-input validation use `try_get`.
    """
    return BY_CODE[code]


def try_get(code: str) -> LogField | None:
    """Return the field with the given code, or None when not present."""
    return BY_CODE.get(code)


def in_group(group: Group) -> tuple[LogField, ...]:
    """Return all fields belonging to a specific group, in catalog order."""
    return BY_GROUP[group]


def derived() -> tuple[LogField, ...]:
    """Return all derived fields (`vcl is None` — metrics, virtual, internal)."""
    return tuple(f for f in REGISTRY if f.is_derived)


def loggable() -> tuple[LogField, ...]:
    """Return all fields that emit a VCL log token (`vcl is not None`)."""
    return tuple(f for f in REGISTRY if f.vcl is not None)


def with_aggregation(agg: Agg) -> tuple[LogField, ...]:
    """Return all fields that support a given aggregation."""
    return tuple(f for f in REGISTRY if agg in f.valid_aggs)


def all_codes() -> frozenset[str]:
    """Return every known field code as a frozenset. Cheap; cached at import."""
    return _ALL_CODES


_ALL_CODES: frozenset[str] = frozenset(f.code for f in REGISTRY)


# ---------------------------------------------------------------------------
# Caller-facing API shape helpers + legacy re-exports
# ---------------------------------------------------------------------------
#
# Every helper and constant the migration plan ships on the registry is a
# direct re-export of the legacy `backend.core.log_fields` symbol — same
# function/object identity, zero behavior drift. Callers can flip
# ``from backend.core.log_fields import X`` to
# ``from backend.core.field_registry import X`` and observe identical
# behavior (parity guard: ``tests/core/test_field_registry.py``).
#
# When a downstream symbol gets re-implemented on top of REGISTRY
# primitives, replace the re-export with the new function — the parity
# test will fail loudly until both sides agree.

from backend.core.log_fields import (  # noqa: E402
    GROUP_INFO,
    INSIGHT_DEFINITIONS,
    LOG_FIELD_CATALOG,
    PRESETS,
    VALID_NAME_RE,
    check_log_line_budget,
    estimate_log_line_bytes,
    format_hash,
    generate_log_format,
    get_catalog_for_api,
    get_groups_for_api,
    get_lf_config,
    get_required_edge_headers,
    resolve_enabled_fields,
    validate_custom_field,
)

__all__ = (
    "Agg",
    "BY_CODE",
    "BY_GROUP",
    "DEFAULT_OFF_CODES",
    "DuckType",
    "FilterOp",
    "GROUP_INFO",
    "Group",
    "INSIGHT_DEFINITIONS",
    "LOG_FIELD_CATALOG",
    "LogField",
    "PRESETS",
    "REGISTRY",
    "SECURITY_HOOK_CODES",
    "VALID_NAME_RE",
    "WIRE_ORDER",
    "all_codes",
    "check_log_line_budget",
    "derived",
    "estimate_log_line_bytes",
    "format_hash",
    "generate_log_format",
    "get",
    "get_catalog_for_api",
    "get_groups_for_api",
    "get_lf_config",
    "get_required_edge_headers",
    "in_group",
    "loggable",
    "resolve_enabled_fields",
    "try_get",
    "validate_custom_field",
    "with_aggregation",
)
