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


# ── Iceberg schema field-id stability across enable/disable cycles ──────────


def test_reenabled_field_gets_same_iceberg_field_id():
    """A field that was enabled, disabled, then re-enabled should land at
    the same Iceberg field_id it had originally — otherwise old parquet
    files written under the original ID can't be read back.

    Today this works only because the field's *name* sort position
    relative to other enabled fields hasn't changed. See the next test
    for the failure mode this guarantee does NOT cover.
    """
    cfg_initial = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    cfg_disabled = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    )
    cfg_reenabled = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )

    s_initial = iceberg.get_iceberg_schema(cfg_initial)
    s_disabled = iceberg.get_iceberg_schema(cfg_disabled)
    s_reenabled = iceberg.get_iceberg_schema(cfg_reenabled)

    assert s_initial.find_field("alpha").field_id == s_disabled.find_field("alpha").field_id
    assert s_initial.find_field("beta").field_id == s_reenabled.find_field("beta").field_id
    assert s_initial.find_field("alpha").field_id == s_reenabled.find_field("alpha").field_id


def test_disabling_middle_field_does_not_shift_later_ids():
    """Disabling a custom field must NOT shift the IDs of alphabetically
    later fields. The fix in ``get_iceberg_schema`` assigns IDs over the
    FULL sorted custom-field list — disabled entries still hold their
    slot, they just don't appear in the emitted schema.

    Without this guarantee, a Parquet file written under field_id=N
    before the disable would be misread as the next field after, which
    is an Iceberg-corruption pattern.
    """
    cfg_all = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "gamma", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    cfg_beta_off = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "gamma", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )

    s_all = iceberg.get_iceberg_schema(cfg_all)
    s_beta_off = iceberg.get_iceberg_schema(cfg_beta_off)

    # alpha and gamma keep their original IDs across the disable.
    assert s_all.find_field("alpha").field_id == s_beta_off.find_field("alpha").field_id
    assert s_all.find_field("gamma").field_id == s_beta_off.find_field("gamma").field_id
    # And beta is genuinely absent from the disabled schema.
    assert "beta" not in {f.name for f in s_beta_off.fields}


def test_disabling_then_adding_new_field_does_not_collide_on_id():
    """Tougher invariant: disabling a custom field reserves its ID slot,
    so a newly-added field gets a fresh ID — never the disabled one's.
    Without slot reservation, a new alphabetically-later field would
    collide with the disabled field's old ID and trigger Iceberg
    schema-mismatch errors at read time.
    """
    cfg_before = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    cfg_after = _cfg(
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "delta", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )

    s_before = iceberg.get_iceberg_schema(cfg_before)
    s_after = iceberg.get_iceberg_schema(cfg_after)

    beta_id = s_before.find_field("beta").field_id
    # delta is alphabetically AFTER beta, so it takes a new slot.
    # Crucially: it MUST NOT reuse beta's ID.
    delta_id = s_after.find_field("delta").field_id
    assert delta_id != beta_id, f"delta got beta's old ID {beta_id} — disabled fields must reserve their slot"


# ── Adding a new field while another is disabled ─────────────────────────────


def test_new_field_added_while_old_is_disabled_does_not_collide_on_name():
    """Even with the field-id stability bug above, the *names* in the
    schema are unique. A newly-added field can't ever have the same
    *name* as a disabled one (the API prevents duplicate names entirely).
    """
    cfg = _cfg(
        [
            {"name": "old_disabled", "duckdb_type": "VARCHAR", "enabled": False},
            {"name": "new_enabled", "duckdb_type": "VARCHAR", "enabled": True},
        ]
    )
    schema = iceberg.get_iceberg_schema(cfg)
    names = {f.name for f in schema.fields}
    assert "new_enabled" in names
    assert "old_disabled" not in names
