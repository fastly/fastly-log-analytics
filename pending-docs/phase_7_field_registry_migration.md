# Phase 7 — FieldRegistry migration

## Status

Scaffolding landed. No callers migrated. Both the legacy `LOG_FIELD_CATALOG`
(list-of-dicts in `backend/core/log_fields.py`) and the new typed view
(`backend/core/field_registry.py`) are live and kept in lockstep at import
time. The new view is **derived from** the legacy view in this scaffolding
phase; that flip reverses at the end of the migration.

## Winning design

`FrozenDataclassFieldRegistry`. All three judges (maintainability,
mypy-strict, migration-cost lenses) picked it unanimously.

### Why it won

- **Discoverability** — one file, one tuple, one frozen dataclass.
  A fresh contributor opening `backend/core/field_registry.py` sees zero
  indirection: no decorator side-effects, no second file (TOML) to chase,
  no class-body-mutation idioms. Adding a field is "append a `LogField(...)`
  literal" — the most obvious operation possible in idiomatic Python.
- **Mypy-strict** — zero `Any` escapes in the registry surface. Pure
  stdlib `@dataclass(frozen=True, slots=True)` with enum-typed columns.
  The single Any-narrowing boundary lives in `_from_legacy_dict` and goes
  away the moment the legacy literal moves.
- **Migration cost** — the dataclass design ships with the smallest
  caller-side surface change. `f["id"]` → `f.code`, `f["duckdb_type"]` →
  `f.duck_type`. The shim (legacy dicts remain authoritative until each
  caller migrates) absorbs long-tail caller fallout. No atomic per-PR
  cutover required.
- **Wire-order parity preserved** — `WIRE_ORDER` is a tuple of field codes
  in catalog emission order. The Rust scorer in `compute/` pins byte
  offsets to this order; the scaffolding's
  `test_wire_order_matches_legacy_emission_order` test is the boot-time
  gate.

### Why the alternatives lost

- **TOML-driven** — strongest LOC reduction (1450) but pays a permanent
  `# type: ignore[arg-type]` tax at the `tomllib.load` boundary, leaks
  `Mapping[str, dict]` typing for presets/insights into every caller, and
  punts on where derivation SQL for the 10 metric fields actually lives.
  Strong for a team with non-Python contributors; weaker for a solo dev.
- **DecoratorRegistry** — best home for derivation logic (co-located
  `@staticmethod def derive(ctx)` blocks), but the `class _: pass` idiom
  is jarring, decorator-driven side-effect registration breaks
  grep-for-the-field-code discoverability, and the dual-registry split
  for custom fields was acknowledged by the proposer as "the single
  biggest design tax."

## What scaffolding shipped

- `backend/core/field_registry.py` — 81 fields, exposed via:
  - `REGISTRY: tuple[LogField, ...]` (wire-ordered)
  - `BY_CODE: Mapping[str, LogField]` (O(1) lookup)
  - `BY_GROUP: Mapping[Group, tuple[LogField, ...]]`
  - `WIRE_ORDER: tuple[str, ...]` (codes that emit VCL, in order)
  - `SECURITY_HOOK_CODES: frozenset[str]` (codes with json.escape /
    digit-regex guards)
  - `get(code)`, `try_get(code)`, `in_group(g)`, `derived()`,
    `loggable()`, `with_aggregation(a)`, `all_codes()`
- `tests/core/test_field_registry.py` — 26 tests covering smoke,
  per-field roundtrip, parity with `LOG_FIELD_CATALOG` and
  `_BUILTIN_FIELD_NAMES`, wire-order parity (`@security_regression`),
  security-hook parity (`@security_regression`), group invariants,
  derivation invariants, render_vcl behaviour, type-driven aggregation
  rules, and Hypothesis property-based coverage of every code +
  cap-override combination.
- Docstring banner in `backend/core/log_fields.py` pointing here.

## Behaviour invariants the migration must preserve

These are the contracts callers depend on today. The scaffolding tests
already check each one; per-caller migration PRs must keep them passing.

1. **Wire-order byte parity** — `WIRE_ORDER` matches the order of
   `LOG_FIELD_CATALOG` entries with `vcl is not None`. The Rust scorer
   reads positional JSON keys. Reordering = silent breakage. Gate:
   `tests/core/test_field_registry.py::test_wire_order_matches_legacy_emission_order`.
2. **Security-hook completeness** — every field whose VCL interpolates an
   attacker-influenced value passes through `json.escape(...)` or a
   `~ "^...$"` digit regex. `SECURITY_HOOK_CODES` reflects this; the
   security regression sweep counts on the set staying monotonic. Gate:
   `tests/core/test_field_registry.py::test_security_hook_codes_match_legacy_hooks`.
3. **Custom field name reservation** — `_BUILTIN_FIELD_NAMES` (read by
   `validate_custom_field` in `log_fields.py`) and `REGISTRY` agree on
   what counts as a built-in. A user must not be able to create a custom
   field that shadows a built-in name. Gate:
   `tests/core/test_field_registry.py::test_registry_codes_match_builtin_set`.
4. **VCL substr cap injection** — `url` / `ua` / `referer` carry runtime
   caps overridable via the `field_limits` config block. `render_vcl()`
   preserves the legacy substring-replace behaviour for the `url` case;
   `ua` / `referer` regenerate the VCL from scratch in the legacy code
   (see `generate_log_format`). Migration of `generate_log_format` will
   need helper methods on the `LogField` instances for those two codes,
   OR keep the existing regenerate path and pass `render_vcl` results
   through it. **Phase 8 decision** — out of scope for scaffolding.
5. **Group dependency closure** — `Group.E` requires `Group.D`,
   `Group.G` requires `Group.F`. Encoded in `_GROUP_REQS`. Gate:
   `test_group_dependencies_match_legacy`.

## Per-caller migration order

Migrate in this order. Each step is one PR.

Ordering rationale: the cheapest callers go first (one-import switches
that buy us mypy-strict coverage with near-zero risk), then the
read-heavy hot paths, then the VCL generator itself (which is the
highest-risk single change because byte parity is mandatory), then the
custom-field model (which needs the dual builtin+custom shape worked
out), then state-sync (which serialises the catalog to SQLite).

| # | Module | Reads from legacy today | Switches to | Risk | Notes |
|---|---|---|---|---|---|
| 1 | `backend/repositories/insights/repository.py` | `LOG_FIELD_CATALOG` for required-field lookups | `BY_CODE.get(code)` | Low | Pure read, no wire-format impact. |
| 2 | `backend/routers/usage.py` | Catalog scan for byte sums per group | `in_group(g)` + `f.typical_bytes` | Low | Read-only. |
| 3 | `backend/routers/bootstrap.py` | `get_catalog_for_api` + `get_groups_for_api` | New thin `to_api()` helper on `LogField` | Low | Watch frozenset[Enum] JSON serialisation — add a `.value` projection in the encoder. |
| 4 | `backend/repositories/dashboard.py` | Field metadata for CTE generation | `BY_CODE` + `f.duck_type` + `f.valid_aggs` | Med | Dashboard CTE generator has bespoke per-metric branches today; collapses to one loop on `with_aggregation(Agg.SUM)`. |
| 5 | `backend/routers/services/core.py` | Filter-op validation per duckdb_type | `f.valid_ops` | Med | SQL validator collapses to three lines; preserve existing error message shape. |
| 6 | `backend/provision/cli.py` | Group metadata + per-field config | `BY_GROUP` + `Group` enum | Low | Mostly grep-and-replace. |
| 7 | `backend/provision/orchestrator.py` | Field provisioning | `BY_CODE` | Low | |
| 8 | `backend/provision/fastly_api.py` | VCL format generation + log line budget | `render_vcl()` + `f.typical_bytes` | High | Touches the VCL emission path. Gate on `format_hash()` byte-parity for all six PRESETS. |
| 9 | `backend/core/iceberg.py` | Iceberg schema generation | `f.duck_type` + `loggable()` | Med | Schema column order must match `WIRE_ORDER`. |
| 10 | `backend/core/ingest.py` | Format validation + field resolution | `try_get(code)` | Med | Ingest path; add a smoke test against a real log line before merging. |
| 11 | `backend/models/custom_fields.py` | DuckDB type compatibility + name validation | `all_codes()` for builtin-collision check | Med | Custom-field validation must keep using `_DUCKDB_TYPE_VALUE_TYPE_COMPAT` and `_DUCKDB_RESERVED` (those are unrelated to the registry). |
| 12 | `backend/state_sync.py` | Field state snapshot to SQLite | `REGISTRY` | Med | Schema migration if the snapshot shape changes. |
| 13 | `backend/core/log_fields.py` itself: `generate_log_format`, `estimate_log_line_bytes`, `resolve_enabled_fields`, `get_required_edge_headers`, `validate_group_deps` | All read the legacy dict | Internal switch to `REGISTRY` | High | This is the cutover step that flips the source of truth. After this, the dict literal moves into the registry module as `LogField(...)` calls and the legacy dict gets deleted. |
| 14 | Tests | `LOG_FIELD_CATALOG` / `LOG_FIELD_CATALOG` in `tests/**` | Same lookups against `REGISTRY` | Low | Mechanical; per the survey, the relevant test modules are `test_catalog_metadata.py`, `test_duckdb_type_roundtrip.py`, `test_ingest.py`, `test_log_fields.py`, `test_log_line_budget.py`, `test_vcl_semantics.py`, `test_insights.py`, `test_sessions.py`, `test_bootstrap.py`, `test_service_mutations.py`, `tests/utils/mock_data.py`. |

## Final cutover (post-step-13)

When step 13 ships:

1. Rewrite `LOG_FIELD_CATALOG` as a list of `LogField(...)` literals
   inside `backend/core/field_registry.py`. Delete `_from_legacy_dict`
   and the `_LEGACY_CATALOG` import.
2. Move `GROUP_INFO` and `PRESETS` next to it as small dicts keyed by
   `Group` enum values.
3. In `backend/core/log_fields.py`, replace the dict literals with
   re-exports of `REGISTRY`, `BY_CODE`, `Group`, etc. Keep the module
   alive for one release as a deprecation shim.
4. Drop the shim in the v2.0 cleanup commit. Final state: one file
   (`field_registry.py`), one tuple, one dataclass.

## Failure modes the scaffolding has to live with

These are real footguns documented up front so the migration doesn't get
ambushed:

- **`render_vcl` substring replace for `ua` / `referer`** — the legacy
  code regenerates the VCL from scratch for these two fields, because
  the `json.escape` boundary wraps the substr call and the cap appears
  twice (once in the literal, once in the regenerated form). The
  current scaffolding's `render_vcl` only handles the `url` shape
  cleanly. Migration of `generate_log_format` (step 8) MUST either keep
  the regenerate branch or add per-field render methods. Failing to
  preserve this loses the cap on `ua` / `referer` and lets an
  attacker-supplied 100 KB User-Agent push the log line past Fastly's
  16 KiB cap.
- **`str, Enum` equality is True against bare strings** — `Group.A == "A"`
  evaluates True. Convenient for JSON serialisation, but means a stray
  string literal in a caller silently matches. Mitigation: lint for
  `Group.X == "..."` comparisons in caller migrations.
- **Frozen dataclass + mutable default** — `required_by: tuple[str, ...] = ()`
  is correct; any future field added with `list[str] = []` raises at
  class-body time. Easy to forget for someone unfamiliar with frozen
  dataclasses; the existing test suite catches it instantly.
- **The dict catalog is still the source of truth** — adding a field
  during the migration means editing `LOG_FIELD_CATALOG` (the dict),
  NOT the registry. The registry is rebuilt from the dict at import
  time. After step 13 this reverses.
