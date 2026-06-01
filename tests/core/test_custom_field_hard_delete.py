"""Custom-field hard-delete schema-evolution contract.

The existing ``tests/core/test_custom_field_lifecycle.py`` covers the SOFT
path — toggling ``enabled: false`` keeps the field's slot reserved in
``get_iceberg_schema`` so later fields don't shift IDs. This file covers
the HARD path: ``DELETE /api/services/{id}/custom-fields/{name}`` removes
the entry from the config entirely (see
``backend/routers/services/core.py::api_delete_custom_field`` line 962).

Two questions the soft tests don't answer:

1.  **Unit:** what does ``get_iceberg_schema`` produce after a hard
    delete? Today's implementation enumerates ``sorted_customs`` after
    the field is gone, so alphabetically-later fields SHIFT DOWN by one
    ID slot. We pin that surprising behavior — Iceberg's persisted table
    schema is the source of truth, so the in-memory schema computed for
    new writes doesn't actually drive Iceberg field-id assignment for
    columns the catalog already knows about.

2.  **Integration:** after a hard delete + a second ingest, does data
    previously written under the deleted field survive at query time?
    The contract is **yes**: ``init_iceberg_table`` only ADDS columns
    via ``update_schema``, never DROPS them
    (``backend/core/iceberg.py:1316-1351``). So Iceberg keeps the
    column, old parquet files keep their values, new parquet files
    don't populate it, and DuckDB's view stays valid.

These tests are the only thing that prevents a future refactor from
making hard-delete destructive.

Closes TESTING_PLAN_3 item 5.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

# ── Unit: get_iceberg_schema after hard delete ───────────────────────────────


def _lf(custom_fields: list[dict]) -> dict:
    return {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields}


def test_hard_delete_middle_field_shifts_later_field_ids():
    """Surprising-but-real: HARD deleting beta (vs disabling it) lets
    gamma slide into beta's old ID slot. The lifecycle tests prove the
    SOFT path holds the slot; this proves the HARD path does NOT.

    This is documented because it's a real schema-evolution foot-gun.
    The integration test below proves it does not cause data loss in
    practice — Iceberg's persisted catalog schema is what actually
    drives column identity at write time, and that catalog only ever
    has columns ADDED, never removed (see init_iceberg_table).

    If a future refactor either (a) preserves slots across hard delete,
    or (b) starts honoring the in-memory schema for column-drop, update
    this test deliberately.
    """
    from backend.core import iceberg

    s_before = iceberg.get_iceberg_schema(
        _lf(
            [
                {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "gamma", "duckdb_type": "VARCHAR", "enabled": True},
            ]
        )
    )
    s_after_hard_delete = iceberg.get_iceberg_schema(
        _lf(
            [
                {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "gamma", "duckdb_type": "VARCHAR", "enabled": True},
            ]
        )
    )

    # alpha is unaffected (it's first alphabetically, so its slot is stable).
    assert s_before.find_field("alpha").field_id == s_after_hard_delete.find_field("alpha").field_id

    # gamma DOES shift down — this is the documented foot-gun.
    gamma_before = s_before.find_field("gamma").field_id
    gamma_after = s_after_hard_delete.find_field("gamma").field_id
    assert gamma_after == gamma_before - 1, (
        f"Pinned behavior: hard-deleting an alphabetically-earlier custom field "
        f"shifts later field IDs down. gamma_before={gamma_before}, "
        f"gamma_after={gamma_after}. If this changed, see the test docstring."
    )

    # And beta is gone from the emitted schema (not just absent — fully erased).
    assert "beta" not in {f.name for f in s_after_hard_delete.fields}


def test_hard_delete_field_alphabetically_last_is_pure_truncation():
    """When the deleted field is the alphabetical tail, the remaining
    fields keep their original IDs — no shift. This is the most common
    real-world hard-delete pattern (admin tries a field, decides against
    it, removes it before any rows are written), so it's worth pinning
    the safe sub-case explicitly.
    """
    from backend.core import iceberg

    s_before = iceberg.get_iceberg_schema(
        _lf(
            [
                {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "zeta", "duckdb_type": "VARCHAR", "enabled": True},
            ]
        )
    )
    s_after = iceberg.get_iceberg_schema(
        _lf(
            [
                {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True},
                {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True},
            ]
        )
    )

    assert s_before.find_field("alpha").field_id == s_after.find_field("alpha").field_id
    assert s_before.find_field("beta").field_id == s_after.find_field("beta").field_id
    assert "zeta" not in {f.name for f in s_after.fields}


# ── Integration: end-to-end ingest → hard-delete → ingest → query ────────────


@pytest.fixture
def hard_delete_env(s3_mock, fos_source, monkeypatch, tmp_path):
    """Reuse the environment from test_custom_field_type_mismatch but
    expose a mutable cfg so the test can hard-delete fields between
    ingest cycles."""
    cache_path = str(tmp_path / "cache")
    warehouse_path = str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    shim = _CallerHintShim(s3_mock)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: shim)

    from backend.core import iceberg as ice

    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # Mutable cfg holder so tests can rewrite it mid-flight.
    state = {
        "cfg": {
            "service_id": fos_source["service_id"],
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
        }
    }

    def _load_config(_sid):
        return state["cfg"]

    monkeypatch.setattr("backend.config.load_config", _load_config)

    def _set_custom_fields(custom_fields: list[dict]):
        state["cfg"]["log_fields"]["custom_fields"] = custom_fields

    def _seed_gz(key: str, rows: list[dict]) -> None:
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    def _drain_ingest():
        from backend.core import ingest as ing

        return list(ing.ingest(source=fos_source))

    return {
        "fos_source": fos_source,
        "warehouse": warehouse_path,
        "cache": cache_path,
        "set_custom_fields": _set_custom_fields,
        "seed_gz": _seed_gz,
        "drain_ingest": _drain_ingest,
    }


def _well_typed_row(ts: datetime, idx: int, **extras):
    row = {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
    }
    row.update(extras)
    return row


def _sync_warehouse_to_cache(env):
    """Iceberg writes land in the file:// warehouse; DuckDB reads from
    the local cache. The type-mismatch test uses the same trick — copy
    new parquets so the DuckDB view can see them."""
    import glob
    import shutil

    data_dir = os.path.join(env["cache"], "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(env["warehouse"], "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)


def _query_all(env, columns: str) -> list:
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    _sync_warehouse_to_cache(env)
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, env["fos_source"])
    view_name = _safe_table(env["fos_source"]["name"])
    return con.execute(f"SELECT {columns} FROM {view_name} ORDER BY url").fetchall()


def test_hard_delete_preserves_old_data_and_allows_new_ingest(hard_delete_env):
    """End-to-end contract: ingest with field 'doomed' → commit → hard
    delete 'doomed' from config → ingest fresh rows → query.

    Expected:
    - The first batch's rows are still queryable (no data loss).
    - The 'doomed' column is still present in the view (Iceberg's
      catalog schema is authoritative; init_iceberg_table only adds,
      never drops).
    - First batch shows the original values; second batch shows NULL
      under 'doomed'.
    - Total row count = rows_first + rows_second (no orphaning).
    """
    from backend.core import iceberg as ice

    env = hard_delete_env

    # ── Phase 1: ingest with 'doomed' enabled ────────────────────────────
    env["set_custom_fields"]([{"name": "doomed", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"doomed":""'}])
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-22/10/2026-05-22T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, doomed="first"),
            _well_typed_row(base + timedelta(seconds=1), 1, doomed="second"),
        ],
    )
    events1 = env["drain_ingest"]()
    done1 = next(e for e in events1 if e["type"] == "done")
    assert done1["rows_inserted"] == 2
    ice.commit_buffer(env["fos_source"])

    # ── Phase 2: hard-delete 'doomed' from the config ────────────────────
    env["set_custom_fields"]([])

    # Reset iceberg caches so the next init reloads the table fresh
    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    # Re-init the table with the empty custom_fields config — this exercises
    # the schema-evolution code path that would corrupt things if it tried
    # to DROP the doomed column.
    ice.init_iceberg_table(env["fos_source"])

    # ── Phase 3: ingest a fresh row WITHOUT 'doomed' in the payload ──────
    env["seed_gz"](
        "raw/2026-05-22/11/2026-05-22T11-00-00.svc.gz",
        [_well_typed_row(base + timedelta(hours=1), 2)],
    )
    events2 = env["drain_ingest"]()
    done2 = next(e for e in events2 if e["type"] == "done")
    assert done2["rows_inserted"] == 1, (
        f"contract violation: hard delete broke the next ingest cycle. done event: {done2}"
    )
    ice.commit_buffer(env["fos_source"])

    # ── Phase 4: query the view — old data must survive ──────────────────
    rows = _query_all(env, "url, doomed")
    assert len(rows) == 3, f"expected 3 rows total (2 pre-delete + 1 post-delete), got {len(rows)}"
    by_url = {url: doomed for url, doomed in rows}
    assert by_url["/path/0"] == "first", (
        f"contract violation: hard-delete erased pre-existing data. /path/0.doomed = {by_url['/path/0']!r}"
    )
    assert by_url["/path/1"] == "second"
    # The new row had no 'doomed' payload — DuckDB/Iceberg should serve NULL.
    assert by_url["/path/2"] is None, (
        f"contract violation: post-delete row should have NULL for the "
        f"removed column. /path/2.doomed = {by_url['/path/2']!r}"
    )


def test_hard_delete_does_not_corrupt_remaining_field_values(hard_delete_env):
    """Two custom fields ('alpha' alphabetically first, 'beta' second).
    Ingest with both, commit, hard-delete 'alpha' (the field whose
    deletion would, by the unit test above, SHIFT 'beta's in-memory ID),
    then ingest fresh rows.

    Contract: 'beta' values survive intact across the shift. This is the
    real test that Iceberg's catalog schema — not the in-memory
    get_iceberg_schema output — is what drives column identity.
    """
    from backend.core import iceberg as ice

    env = hard_delete_env
    env["set_custom_fields"](
        [
            {"name": "alpha", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"alpha":""'},
            {"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'},
        ]
    )
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-22/10/2026-05-22T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, alpha="A0", beta="B0"),
            _well_typed_row(base + timedelta(seconds=1), 1, alpha="A1", beta="B1"),
        ],
    )
    events1 = env["drain_ingest"]()
    assert next(e for e in events1 if e["type"] == "done")["rows_inserted"] == 2
    ice.commit_buffer(env["fos_source"])

    # Hard-delete alpha. In get_iceberg_schema, beta's in-memory ID will
    # shift from base+2 to base+1 — but the Iceberg catalog still has
    # beta at base+2.
    env["set_custom_fields"]([{"name": "beta", "duckdb_type": "VARCHAR", "enabled": True, "vcl": '"beta":""'}])
    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()
    ice.init_iceberg_table(env["fos_source"])

    env["seed_gz"](
        "raw/2026-05-22/11/2026-05-22T11-00-00.svc.gz",
        [_well_typed_row(base + timedelta(hours=1), 2, beta="B2")],
    )
    events2 = env["drain_ingest"]()
    assert next(e for e in events2 if e["type"] == "done")["rows_inserted"] == 1
    ice.commit_buffer(env["fos_source"])

    # Beta values must round-trip cleanly across the shift.
    rows = _query_all(env, "url, beta")
    by_url = dict(rows)
    assert by_url["/path/0"] == "B0", (
        f"contract violation: beta value corrupted by alpha hard-delete. "
        f"/path/0.beta = {by_url['/path/0']!r} (expected 'B0')"
    )
    assert by_url["/path/1"] == "B1"
    assert by_url["/path/2"] == "B2"
