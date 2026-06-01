"""Custom-field type-mismatch contract.

When admin declares a custom field as INTEGER (or any other numeric/typed
DuckDB type) but the VCL emits a value that can't cast cleanly — most
commonly because the field stays in production as a free-text VARCHAR
upstream and a redeploy of the typed config lands first — the ingest
path must:

1. **Keep the row.** Other fields on the row are still valuable signal;
   dropping the entire row because a single side-car field is malformed
   would silently lose request counts on the dashboard.
2. **NULL the offending cell.** The bad value is unrecoverable for this
   particular cell; coercing to 0 / -1 / "" would lie. NULL is honest.
3. **Continue the batch.** A typed-cast failure on one row must not
   abort the chunk — every other row in the file (and every other file
   in the chunk) still has to land.

These assertions pin the contract end-to-end against the real ingest
generator + real PyIceberg buffer write. The locked contract lives in
TESTING_PLAN_3.md Section 7. The NULL-fill half of the contract is
implemented by DuckDB's ``read_json_auto(..., ignore_errors=true)``; the
"increment a per-file ``error_count``" half is deferred to the schema-
migration framework (TESTING_PLAN_3 item 4) which will add the column
without breaking existing service DBs.

Coverage gap closed: the existing
``tests/core/test_custom_field_lifecycle.py`` exercises enable / disable
/ Iceberg field-id stability — it does not exercise what happens at
ingest time when a declared type doesn't match the value on the wire.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from datetime import UTC, datetime, timedelta

import duckdb
import pytest


@pytest.fixture
def custom_field_env(s3_mock, fos_source, monkeypatch, tmp_path):
    """Lightweight wrapper around the ingest harness used by test_e2e_pipeline.

    Sets up moto S3 + a local-FS PyIceberg warehouse + cache, plus the
    `_get_fos_client` monkeypatch for both the duckdb and ingest module
    namespaces, plus a config loader stub that returns whatever custom
    fields the test asks for. Returns helpers for seeding gzipped log
    rows and draining the ingest generator.
    """
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

    # Reset iceberg caches (autouse fixture does this too)
    from backend.core import iceberg as ice

    ice._catalog_cache.clear()
    ice._snapshot_files_cache.clear()
    ice._table_object_cache.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    def _set_custom_fields(custom_fields: list[dict]):
        cfg = {
            "service_id": fos_source["service_id"],
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": custom_fields,
            },
        }
        monkeypatch.setattr("backend.config.load_config", lambda _sid: cfg)

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


def _well_typed_row(ts: datetime, idx: int, custom_field_name: str, custom_value):
    """Minimum well-typed log row plus one custom field."""
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S+0000"),
        "ip": f"10.0.0.{idx}",
        "status": 200,
        "url": f"/path/{idx}",
        "method": "GET",
        "cache": "HIT",
        "resp_bytes": 1024 + idx,
        "elapsed": 1500 + idx * 10,
        custom_field_name: custom_value,
    }


def _query_view(env, custom_field_name: str) -> list:
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    src = env["fos_source"]
    # Manually sync warehouse → cache so DuckDB sees the parquet
    import glob
    import shutil

    data_dir = os.path.join(env["cache"], "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(env["warehouse"], "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)

    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src)
    view_name = _safe_table(src["name"])
    return con.execute(f"SELECT url, {custom_field_name} FROM {view_name} ORDER BY url").fetchall()


# ── Tests ────────────────────────────────────────────────────────────────


def test_integer_field_with_string_value_nulls_only_that_cell(custom_field_env):
    """Declared INTEGER, VCL emits the string ``"NaN"`` for ONE row in a
    batch of three. Expect: row kept, cell NULL, other rows untouched.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {
                "name": "user_score",
                "duckdb_type": "INTEGER",
                "enabled": True,
                "vcl": '"user_score":%{req.http.X-User-Score}V',
            }
        ]
    )

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, "user_score", 42),
            _well_typed_row(base + timedelta(seconds=1), 1, "user_score", "NaN"),
            _well_typed_row(base + timedelta(seconds=2), 2, "user_score", 17),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None, f"no 'done' event in: {events}"
    # Contract: row is KEPT, not dropped — so all 3 rows make it through
    assert done["rows_inserted"] == 3, (
        f"contract violation: type-mismatched row was DROPPED. "
        f"rows_inserted={done['rows_inserted']} expected 3. Full done event: {done}"
    )

    # Commit + query
    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "user_score")

    # 3 rows back, 2 with valid INTs, 1 NULL for the offending cell
    assert len(rows) == 3, f"expected 3 rows from the view, got {len(rows)}"
    by_url = dict(rows)
    assert by_url["/path/0"] == 42
    assert by_url["/path/1"] is None, (
        f"contract violation: 'NaN' should have become NULL after ignore_errors=true; got {by_url['/path/1']!r}"
    )
    assert by_url["/path/2"] == 17


def test_integer_field_with_string_float_truncates_not_nulls(custom_field_env):
    """Declared INTEGER, VCL emits ``"3.14"`` (a string that *parses* as a
    valid numeric value). Pin the surprising-but-real behavior: DuckDB's
    ``read_json_auto(..., columns={'x': 'INTEGER'}, ignore_errors=true)``
    parses the string as a float and **truncates to integer** rather
    than NULL-filling. The cell becomes ``3``, not ``NULL``.

    This is documented because it's a real foot-gun: if VCL drift causes
    a field to start emitting decimals, the dashboard will silently see
    a truncated histogram. The mitigation is to use DOUBLE for fields
    that might emit decimals.

    Contrast with the ``"NaN"`` case in the previous test where the
    string is structurally non-numeric and DOES fall through to NULL.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":1'}])

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, "score", 1),
            _well_typed_row(base + timedelta(seconds=1), 1, "score", "3.14"),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["rows_inserted"] == 2

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "score")
    by_url = dict(rows)
    assert by_url["/path/0"] == 1
    # Pin truncation, NOT NULL — this is the actual contract today.
    assert by_url["/path/1"] == 3, (
        f"DuckDB JSON parser used to truncate '3.14' to 3 for INTEGER "
        f"columns; got {by_url['/path/1']!r}. If this is now NULL, that's "
        f"a DuckDB-side behavior change — update the contract or pin the "
        f"DuckDB version that introduced it."
    )


def test_boolean_field_with_arbitrary_string_nulls_the_cell(custom_field_env):
    """Declared BOOLEAN, VCL emits ``"maybe"``. Cell → NULL; row kept."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [
            {
                "name": "is_bot",
                "duckdb_type": "BOOLEAN",
                "enabled": True,
                "vcl": '"is_bot":false',
            }
        ]
    )

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz",
        [
            _well_typed_row(base, 0, "is_bot", True),
            _well_typed_row(base + timedelta(seconds=1), 1, "is_bot", "maybe"),
            _well_typed_row(base + timedelta(seconds=2), 2, "is_bot", False),
        ],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["rows_inserted"] == 3

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "is_bot")
    by_url = dict(rows)
    assert by_url["/path/0"] is True
    assert by_url["/path/1"] is None
    assert by_url["/path/2"] is False


def test_batch_proceeds_when_one_file_has_only_type_mismatches(custom_field_env):
    """Two files in the same batch; one is entirely bad values, the other
    is entirely good. Bad file must not abort the good file's commit, and
    the good rows must land in Iceberg cleanly.
    """
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "n", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"n":1'}])

    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.bad.gz",
        [_well_typed_row(base + timedelta(seconds=i), i, "n", "bad") for i in range(3)],
    )
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-05-00.good.gz",
        [_well_typed_row(base + timedelta(minutes=5, seconds=i), 100 + i, "n", 50 + i) for i in range(3)],
    )

    events = env["drain_ingest"]()
    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    # All 6 rows kept (3 with NULL cells, 3 with valid INTs)
    assert done["rows_inserted"] == 6

    ice.commit_buffer(env["fos_source"])
    rows = _query_view(env, "n")
    assert len(rows) == 6
    n_null = sum(1 for _, v in rows if v is None)
    n_good = sum(1 for _, v in rows if v is not None)
    assert n_null == 3, f"expected 3 NULL cells (the 'bad' file), got {n_null}"
    assert n_good == 3, f"expected 3 well-typed cells (the 'good' file), got {n_good}"
