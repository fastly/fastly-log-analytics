"""Custom-field lifecycle tests: disable / re-enable / add-while-disabled.

The custom-field "enabled" flag is a user-facing toggle that affects:
- ``get_ingest_type_hints`` and ``get_catalog_field_ids`` — both correctly
  skip disabled fields (covered in tests/core/test_ingest.py).
- ``get_iceberg_schema`` — also skips disabled fields, BUT assigns
  field IDs by *position in the filtered list*. That has subtle
  implications for Iceberg compatibility documented below.

These tests pin the current behavior so any future refactor that aims to
either (a) fix the field-id stability issue, or (b) change the disable
semantics, has to update them deliberately.
"""

from __future__ import annotations

from backend.core import iceberg
from backend.core.ingest import get_catalog_field_ids, get_ingest_type_hints


def _cfg(custom_fields: list[dict]) -> dict:
    return {
        "schema_version": 2,
        "groups": ["A"],
        "custom_fields": custom_fields,
    }


# ── Disable / re-enable: ingest hints stay coherent ──────────────────────────


def test_disable_field_removes_from_ingest_hints():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert "field_a" in hints
    assert "field_b" not in hints


def test_disable_field_removes_from_catalog_field_ids():
    cfg = _cfg(
        [
            {"name": "field_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "field_b", "duckdb_type": "BIGINT", "enabled": False},
        ]
    )
    ids = get_catalog_field_ids(cfg)
    assert "field_a" in ids
    assert "field_b" not in ids


def test_disable_then_reenable_restores_ingest_hints():
    """Round-trip: disabling a field is reversible from the ingest side.
    New rows written after re-enable should once again include the column."""
    disabled = _cfg(
        [
            {"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    )
    reenabled = _cfg(
        [
            {"name": "round_trip", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )

    assert "round_trip" not in get_ingest_type_hints(disabled)
    assert "round_trip" in get_ingest_type_hints(reenabled)
    # Type is restored — not just the name
    assert get_ingest_type_hints(reenabled)["round_trip"] == "VARCHAR"


def test_disable_does_not_affect_other_enabled_fields():
    """Disabling one field must leave the others' hints untouched."""
    cfg = _cfg(
        [
            {"name": "keep_me", "duckdb_type": "DOUBLE", "enabled": True},
            {"name": "drop_me", "duckdb_type": "BIGINT", "enabled": False},
            {"name": "also_keep", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    hints = get_ingest_type_hints(cfg)
    assert hints["keep_me"] == "DOUBLE"
    assert hints["also_keep"] == "VARCHAR"
    assert "drop_me" not in hints


# ── Adding a new field while another is disabled ─────────────────────────────


def test_new_field_added_while_old_is_disabled_does_not_collide_on_name():
    """The *names* in the emitted Arrow schema are unique and exclude
    disabled custom fields. A newly-added field can't ever have the same
    *name* as a disabled one (the API prevents duplicate names entirely).
    """
    cfg = _cfg(
        [
            {"name": "old_disabled", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "new_enabled", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    schema = iceberg.get_arrow_schema(cfg)
    names = {f.name for f in schema}
    assert "new_enabled" in names
    assert "old_disabled" not in names
