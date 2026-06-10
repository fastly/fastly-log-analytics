"""Phase 7 FieldRegistry — frozen-dataclass single source of truth for log fields.

This module is the chosen Phase 7 design (FrozenDataclassFieldRegistry, picked
unanimously by all three judges on the maintainability, mypy-strict, and
migration-cost lenses). It re-expresses the existing `LOG_FIELD_CATALOG`
list-of-dicts in `log_fields.py` as a tuple of immutable `LogField`
dataclasses, with aggregations / filter ops / security-hook detection derived
from the column's DuckDB type and VCL expression instead of being repeated
per-field.

SCAFFOLDING ONLY — callers still read `LOG_FIELD_CATALOG` in `log_fields.py`.
This module is published so the migration can land one caller at a time per
the order in `pending-docs/phase_7_field_registry_migration.md`. The existing
constants (`LOG_FIELD_CATALOG`, `GROUP_INFO`, `PRESETS`, `INSIGHT_DEFINITIONS`,
`_BUILTIN_FIELD_NAMES`) remain authoritative until each caller migrates.

Wire-order invariant
--------------------
The order of `REGISTRY` is byte-for-byte identical to `LOG_FIELD_CATALOG`,
because the Rust scorer in `compute/` reads emitted JSON keys positionally
when streaming. Reordering rows here without a coordinated change in
`compute/` will silently break scorer parity. A boot-time test
(`tests/core/test_field_registry.py::test_registry_codes_match_log_fields`)
asserts equality with the existing catalog so reordering shows up loudly in
CI before any deploy.

Discoverability
---------------
A fresh reader opening this file sees one tuple, one dataclass, two enums,
two derivation helpers. No second file to chase, no decorator side-effects,
no import-time freeze ritual. Adding a field is one literal at the bottom
of `REGISTRY`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

# ---------------------------------------------------------------------------
# Enums (replace stringly-typed columns in the legacy catalog)
# ---------------------------------------------------------------------------


class DuckType(StrEnum):
    """DuckDB column types in use across the field catalog.

    Values are kept identical to the strings that appear in the legacy
    `LOG_FIELD_CATALOG`'s `duckdb_type` key so cross-referencing diffs is
    trivial.
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
    METRICS = "METRICS"
    VIRTUAL = "VIRTUAL"
    INTERNAL = "INTERNAL"

    @classmethod
    def from_legacy(cls, raw: str | None) -> Group:
        """Translate the legacy `None | "A".."L" | "METRICS" | "VIRTUAL" | "INTERNAL"`."""
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
    via the legacy dict at module init time (`_from_legacy_dict`); the
    `kw_only=True` ergonomics are deferred until callers migrate, so the
    legacy `LOG_FIELD_CATALOG` literals don't need to be rewritten in this
    scaffolding PR.

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


def _from_legacy_dict(d: Mapping[str, object]) -> LogField:
    """Build a `LogField` from a legacy `LOG_FIELD_CATALOG` entry.

    Keeps the scaffolding loader trivial: the existing dict literals don't
    move, and the registry construction stays a single comprehension over
    `LOG_FIELD_CATALOG`. When callers migrate to the registry, individual
    rows can be hand-rewritten as `LogField(...)` literals and this helper
    can shrink.
    """
    code = str(d["id"])
    raw_group = d.get("group")
    group = Group.from_legacy(raw_group if raw_group is None else str(raw_group))
    duck_type = DuckType(str(d["duckdb_type"]))
    vcl_raw = d.get("vcl")
    vcl: str | None = None if vcl_raw is None else str(vcl_raw)
    required_by_raw = d.get("required_by") or ()
    required_by = tuple(str(x) for x in required_by_raw)  # type: ignore[arg-type]
    return LogField(
        code=code,
        label=str(d.get("label", code)),
        group=group,
        duck_type=duck_type,
        description=str(d.get("description", "")),
        vcl=vcl,
        typical_bytes=int(d.get("typical_bytes", 0) or 0),  # type: ignore[arg-type]
        required_by=required_by,
        substr_cap=_detect_substr_cap(vcl),
        individually_toggleable=bool(d.get("individually_toggleable", False)),
        formatter=_opt_str(d.get("formatter")),
        unit=_opt_str(d.get("unit")),
        precision=_opt_int(d.get("precision")),
        note=_opt_str(d.get("note")),
    )


def _opt_str(v: object) -> str | None:
    return None if v is None else str(v)


def _opt_int(v: object) -> int | None:
    if v is None:
        return None
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):  # pragma: no cover — catalog uses ints
        return None


# ---------------------------------------------------------------------------
# Registry construction
# ---------------------------------------------------------------------------

# Import the legacy catalog at module init. This is deliberate: while
# callers are mid-migration, both views must agree byte-for-byte, and the
# cheapest way to guarantee that is to derive the new view from the old.
# When the last caller migrates (Phase 8), the legacy dict literals get
# rewritten in-place as `LogField(...)` calls and this import flips.
from backend.core.log_fields import LOG_FIELD_CATALOG as _LEGACY_CATALOG  # noqa: E402

REGISTRY: tuple[LogField, ...] = tuple(_from_legacy_dict(entry) for entry in _LEGACY_CATALOG)
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
# Phase 7 migration step 3 (`backend/routers/bootstrap.py`) routes through
# these names instead of importing them from `backend.core.log_fields`. The
# helpers below intentionally delegate to the legacy module: the legacy
# dict catalog remains authoritative (see module docstring), so the only
# job of the registry-side names is to give callers a single import surface
# and to keep the `@patch("backend.core.log_fields.get_catalog_for_api")`
# regression test in `tests/routers/test_bootstrap.py` working — the
# function lookup happens at call time on the `_lf` module reference, so
# unittest.mock.patch on `log_fields.get_catalog_for_api` still takes
# effect when called via this delegate.
#
# `PRESETS` and `INSIGHT_DEFINITIONS` are re-exported as direct references
# to the legacy objects so a mutation through `log_fields` (rare, but a
# stated hard constraint of the migration) is observed by callers reading
# from the registry.


def get_catalog_for_api(field_limits: Mapping[str, int] | None = None) -> list:
    """API-shaped catalog for `/api/log-fields/catalog`.

    Delegates to `log_fields.get_catalog_for_api` so the legacy module
    remains the single authoritative implementation while the migration
    is in flight. The attribute lookup is deferred to call time so the
    bootstrap test that patches `backend.core.log_fields.get_catalog_for_api`
    continues to exercise the 500 path.
    """
    from backend.core import log_fields as _lf

    return _lf.get_catalog_for_api(dict(field_limits) if field_limits is not None else None)


def get_groups_for_api() -> list:
    """API-shaped group metadata for `/api/log-fields/catalog`.

    Delegates to `log_fields.get_groups_for_api`; see `get_catalog_for_api`
    for the rationale.
    """
    from backend.core import log_fields as _lf

    return _lf.get_groups_for_api()


# Re-exports: same object identity as the legacy module's globals. Any
# mutation through `log_fields` is observed here automatically (the
# migration doc calls this out as a hard constraint).
from backend.core.log_fields import (  # noqa: E402
    INSIGHT_DEFINITIONS,
    PRESETS,
)

__all__ = (
    "Agg",
    "BY_CODE",
    "BY_GROUP",
    "DuckType",
    "FilterOp",
    "Group",
    "INSIGHT_DEFINITIONS",
    "LogField",
    "PRESETS",
    "REGISTRY",
    "SECURITY_HOOK_CODES",
    "WIRE_ORDER",
    "all_codes",
    "derived",
    "get",
    "get_catalog_for_api",
    "get_groups_for_api",
    "in_group",
    "loggable",
    "try_get",
    "with_aggregation",
)
