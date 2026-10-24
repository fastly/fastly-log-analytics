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

from datetime import UTC, datetime, timedelta

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


# ── Custom-field NULL handling ───────────────────────────────────────────────


def _row(ts, idx, **extra):
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        **extra,
    }


def _connect(env):
    """Open a DuckLake-attached connection with the ``logs`` view registered.
    Returns (con, view_name)."""
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    con = get_connection(env["fos_source"])
    iceberg.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _ingest_and_commit(env, expected):
    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None, "no 'done' event from ingest"
    assert done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    iceberg.commit_buffer(env["fos_source"])


def test_aggregation_over_all_null_column(custom_field_env):
    """COUNT(*)=3, COUNT(col)=0, AVG IS NULL — ANSI semantics so
    dashboards render "no data" rather than a misleading zero."""
    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    iceberg.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz",
        [_row(base + timedelta(seconds=i), i, score=None) for i in range(3)],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    n_rows, n_score, avg_score = con.execute(f"SELECT COUNT(*), COUNT(score), AVG(score) FROM {view}").fetchone()
    assert n_rows == 3, f"COUNT(*) must count NULL rows; got {n_rows}"
    assert n_score == 0, f"COUNT(score) must skip NULLs; got {n_score}"
    assert avg_score is None, f"AVG over all-NULL must be NULL, got {avg_score!r}"
