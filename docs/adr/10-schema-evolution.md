# ADR-10 — Schema Evolution Contract

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective ([pending-docs/velocity_improvements.md](../../pending-docs/velocity_improvements.md) Tier 2)

## 1. Context & Motivation

The codebase has working schema-evolution machinery across four layers:

- **Built-in log fields** defined as dict literals in [`LOG_FIELD_CATALOG`](../../backend/core/log_fields.py), derived into a frozen [`field_registry.REGISTRY`](../../backend/core/field_registry.py) at import time.
- **Custom log fields** per-service in service-config JSON, validated by `validate_custom_field` (VCL injection guards, type compat, name collisions).
- **Iceberg table schema** derived dynamically from `LOG_FIELD_CATALOG + custom_fields` via `get_iceberg_schema()`; existing tables evolved via `update_schema().add_column()` in `_init_iceberg_table_locked`.
- **Per-service metadata SQLite** versioned by `PRAGMA user_version`; migrations in [`backend/core/sqlite_migrations.py`](../../backend/core/sqlite_migrations.py) are idempotent + transactional.
- **Long-running data backfills** (rollups, hour-bundling) in [`backend/core/data_migrations.py`](../../backend/core/data_migrations.py): non-transactional, tracked in `applied_data_migrations`, idempotent.

The machinery exists. What's missing is the contract for **what gets to change**, **how**, and **what gets guaranteed afterwards**. The README claims "schema evolution handled gracefully" — this ADR is what that sentence promises.

The motivating gap: the next time someone adds a Fastly log field (or worse, wants to rename one), there's no doc that says "OK do these four steps + add this test + bump this version." The work happens, but inconsistently, and field-ID stability (which the Rust scorer depends on) gets re-derived from code every time. One forgotten WIRE_ORDER reorder breaks the scorer silently.

## 2. Decision

Schema evolution is **additive-only by default** at every layer. Removals, renames, and type changes require an explicit migration with a deprecation window. Field-ID stability is load-bearing for the Rust scorer; any change that re-pins WIRE_ORDER also needs a coordinated `compute/scorer/` change in the same PR.

### 2.1 The four schema surfaces and their evolution rules

| Surface | Source of truth | How to add | How to remove |
|---|---|---|---|
| **Built-in log field** | [`LOG_FIELD_CATALOG`](../../backend/core/log_fields.py) dict + [`_FIELD_ORDER`](../../backend/core/iceberg/_core.py) tuple | Append to dict, append to `_FIELD_ORDER` (NEVER insert mid-list — IDs are positional and pinned), append to `WIRE_ORDER` if security-relevant, run `tests/core/test_field_registry.py` parity tests | Mark `deprecated: True` in catalog; keep ID slot reserved; remove from `_FIELD_ORDER` only after 2 minor versions + Rust scorer drops the field |
| **Custom log field** (per-service) | Service config JSON `log_fields.custom_fields` array | POST to service-update endpoint with `validate_custom_field` checks; Iceberg adds column via `update_schema().add_column()` on next ingest | `disabled: True` in config keeps the slot reserved (ID enumeration stable); hard-removing means the next custom field gets a different ID for new services |
| **Iceberg table** (per-service) | Derived from above; mutated via `_init_iceberg_table_locked` | Automatic on first ingest after field added | Iceberg supports `drop_column`; we do not call it. Disabled fields stay in the schema until the table is dropped + re-created. |
| **Metadata SQLite** (per-service) | [`backend/core/sqlite_migrations.py:MIGRATIONS`](../../backend/core/sqlite_migrations.py) dict | Append `_migration_N` callable; bump `user_version`; transactional; idempotent | Migrations apply on open; loss of metadata.db is recovered by re-running all migrations against a fresh DB |
| **Long-running data migration** | [`backend/core/data_migrations.py:MIGRATIONS`](../../backend/core/data_migrations.py) list | Append entry; non-transactional; must be idempotent; runs in daemon thread per-service | Don't remove from list — historical services need the entry. Mark `skip_after_version` if the work is structurally unnecessary post-upgrade |

### 2.2 Field-ID stability rules (Iceberg + Rust scorer)

These are the rules that break things silently when violated. Read them.

1. **Built-in IDs are positional in `_FIELD_ORDER`.** Position N → ID N+1 (Iceberg field IDs are 1-indexed). Never insert; always append.
2. **Custom IDs are derived from sorted custom-field names**, enumerated AFTER built-in IDs. Disabling a custom field reserves its slot so subsequent IDs don't shift. **Never remove a disabled custom field from the slot reservation** without a coordinated re-bind of all referencing data.
3. **`WIRE_ORDER` in [field_registry.py](../../backend/core/field_registry.py)** is byte-identical to the order log lines emit fields. Reordering it without simultaneously updating `compute/scorer/` rust code breaks scorer parity silently — the scorer reads positionally and will mis-attribute every subsequent field. `tests/core/test_field_registry.py::test_wire_order_matches` is the gate; do not delete it.
4. **`SECURITY_HOOK_CODES` in [field_registry.py](../../backend/core/field_registry.py)** drives which fields the scorer inspects for security signals. Adding a security-hook field is a coordinated PR: field-registry + scorer rust code + scorer fixture tests, all in one commit.

### 2.3 Backward-compat guarantees

For each surface, what existing-data callers can rely on:

- **Iceberg table reads**: a query against the table after a field is added returns NULL for that column in historical files. Reading historical data never breaks. Pyiceberg handles the read-time null projection.
- **Parquet ingest**: `read_json_auto(ignore_errors=True)` silently NULLs type-mismatched values into `error_count` — this is by design but it's a known silent-failure surface. If a custom field changes type, historical data isn't auto-converted; querying it returns NULL for the changed column.
- **Frontend field catalog**: served via `/api/log-fields/catalog` (bootstrap endpoint). The frontend re-fetches on service-switch; no client-side cache invalidation needed across schema changes.
- **OpenAPI / typed-client surface**: a Pydantic model addition produces an additive change to `frontend/types/api.generated.ts`. The pre-commit `regen-openapi` hook is the drift gate. See [ADR-12](12-api-versioning.md) for what counts as a breaking change at the HTTP surface.

### 2.4 The four-step workflow for adding a built-in log field

1. **Dict entry** in [`LOG_FIELD_CATALOG`](../../backend/core/log_fields.py) — name, VCL expression, DuckDB type, group, optional `value_type` for enums.
2. **Order pin**: append to [`_FIELD_ORDER`](../../backend/core/iceberg/_core.py), append to [`WIRE_ORDER`](../../backend/core/field_registry.py) if log-line-positional.
3. **Tests**: `tests/core/test_field_registry.py` parity tests run automatically; if the field is security-relevant, also append to `SECURITY_HOOK_CODES` and update `tests/scoring/`.
4. **Documentation**: brief comment in catalog (one line) explaining what the field captures, where it comes from in VCL, and whether it's sensitive (analyst-visible vs admin-only).

Custom fields go through the service-update API; the user does steps 1–4 implicitly via the validator and the auto-Iceberg add-column.

### 2.5 Deprecation timeline

For built-in fields:

- **Soft deprecate**: add `deprecated: True` + a `removal_target_version: X.Y.Z` note in the catalog entry. Field still ingests; queries still work; frontend can choose to hide it. Two minor releases minimum before hard removal.
- **Hard removal**: drop from `_FIELD_ORDER` + `WIRE_ORDER` in the target version. Coordinate `compute/scorer/` removal in the same release. Document in CHANGELOG.md under that version.
- **Reading historical data after removal**: the column stays in the Iceberg schema (we don't `drop_column`). Queries against historical files still see the data. New ingest doesn't populate it.

For custom fields: per-service, the operator sets `disabled: True` and the field stops being ingested. The data stays in Iceberg until the table is rebuilt.

## 3. Out of Scope

- **API versioning doctrine.** Field additions are additive at the HTTP surface; [ADR-12](12-api-versioning.md) covers what counts as breaking and how the OpenAPI client stays in sync.
- **Field-type changes.** Not supported. If a built-in field's type needs to change, hard-remove the old field, add a new one with the new type and a different name, run a data migration to backfill. We've never done this; the contract above defaults to "don't."
- **Multi-region Iceberg coordination.** Single bucket assumption.
- **Rust scorer wire-format evolution beyond field additions.** Scorer changes are coordinated via the scorer's own version pinning in `compute/scorer/`. This ADR's contract ends at field-registry / WIRE_ORDER.
- **Frontend per-field UI labelling / formatter assignment.** Lives in the field catalog as metadata but the UI behavior is the frontend's concern.
- **Cost-model implications of new fields** (storage growth, query cost). Covered by [ADR-07](07-feature-budgets.md) — every new endpoint that touches a new field declares a budget.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| New built-in field inserted MID-LIST in `_FIELD_ORDER` (not appended) | Every subsequent field's ID shifts. Iceberg schemas with the old IDs become incompatible with new code. `test_field_registry.py` catches at pre-commit. If somehow merged: bump to a new table (cannot recover existing data with shifted IDs). |
| `WIRE_ORDER` reordered without scorer change | Scorer mis-attributes every subsequent field; security scores become noise. `test_wire_order_matches` catches; CI gates. If shipped: revert the WIRE_ORDER change, scorer continues to work. |
| Custom field added with name colliding with a built-in | `validate_custom_field` rejects at the service-update endpoint. |
| Custom field disabled then re-enabled with different type | The Iceberg column already exists with the old type; re-enable doesn't change it. Document this gotcha in the operator docs. If forced, hard-remove + add with new name. |
| `read_json_auto(ignore_errors=True)` silently NULLs a type-mismatched field | Visible in `error_count` column for that ingested file. Recovery: investigate which field changed type at the producer side; re-ingest after fixing. |
| Metadata SQLite migration fails mid-run | Transaction rolls back; `user_version` not bumped. Next open re-applies. If the migration is buggy: the same failure repeats — fix the migration and ship. |
| Data migration fails mid-run | `applied_data_migrations` row not written. Next boot re-runs from scratch. Migrations must be idempotent for this to work safely — read [`data_migrations.py:1–45`](../../backend/core/data_migrations.py) docstring before adding a new one. |
| Iceberg `update_schema().add_column()` fails | `_init_iceberg_table_locked` raises; new ingest blocked until resolved. Usually means concurrent writer; retry on next tick. If persistent: read pyiceberg error, investigate manifest state. |

## 5. Verification

This ADR succeeds if:

- A new field added in 2026-Q3 follows the four-step workflow without a code-review comment asking "did you also update `_FIELD_ORDER`?"
- `test_field_registry.py` continues to pass on every PR (the parity guarantee).
- No "scorer parity broke silently" incident ever happens.
- Custom-field type changes are caught at validation, not in production.

It fails if a field gets mid-list inserted, if `WIRE_ORDER` desync ships, or if someone adds a third schema-evolution mechanism without amending this ADR.

## 6. Rollback

The schema-evolution code is load-bearing; the ADR documents existing behavior. Rollback = delete the doc.

A change to one of the rules above (e.g., decide we DO want to support mid-list `_FIELD_ORDER` insertion via remapping logic) requires amending this ADR with the new rule, the trigger for the change, and a coordinated migration plan. Don't silently bend a rule.
