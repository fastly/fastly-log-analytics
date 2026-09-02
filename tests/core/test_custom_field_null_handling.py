"""Custom-field NULL handling across all 5 duckdb_types.

Audit finding: NULL semantics for custom fields were uncovered. The
sibling type-mismatch suite pins structurally-bad values; this file
pins the three NULL-shape contracts the rest of the stack relies on.

Contracts:
  1. Missing JSON key == explicit ``null`` == SQL NULL (no distinct
     "missing" sentinel). Verified for every duckdb_type literal
     in backend/models/custom_fields.py: VARCHAR / INTEGER / BIGINT /
     DOUBLE / BOOLEAN.
  2. Aggregates over an all-NULL column follow ANSI SQL: COUNT(*)
     counts rows, COUNT(col) skips NULLs, AVG returns NULL.
  3. Old NULL rows survive a disable/re-enable cycle — Iceberg
     field-id slot reservation (see test_custom_field_lifecycle.py)
     keeps old parquet schema-compatible.
"""

from __future__ import annotations

import glob
import gzip
import io
import json
import os
import shutil
from datetime import UTC, datetime, timedelta

import duckdb
import pytest


# Mirrors the harness in tests/core/test_custom_field_type_mismatch.py.
# Duplicated rather than imported: pytest doesn't auto-collect fixtures
# defined in sibling test modules.
@pytest.fixture
def custom_field_env(s3_mock, fos_source, monkeypatch, tmp_path):
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    warehouse_path = str(tmp_path / "warehouse")
    os.makedirs(cache_path, exist_ok=True)
    os.makedirs(warehouse_path, exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda _src: f"file://{warehouse_path}")
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)

    class _Shim:
        def __init__(self, c):
            self._c = c

        def get_paginator(self, op, caller_hint=None):
            return self._c.get_paginator(op)

        def __getattr__(self, n):
            return getattr(self._c, n)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _s: _Shim(s3_mock))

    from backend.core import iceberg as ice

    for c in (ice._catalog_cache, ice._snapshot_files_cache, ice._table_object_cache):
        c.clear()
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    def _set_fields(custom_fields):
        cfg = {
            "service_id": fos_source["service_id"],
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": custom_fields},
        }
        monkeypatch.setattr("backend.config.load_config", lambda _sid: cfg)

    def _seed(key, rows):
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    def _drain():
        from backend.core import ingest as ing

        return list(ing.ingest(source=fos_source))

    return {
        "fos_source": fos_source,
        "warehouse": warehouse_path,
        "cache": cache_path,
        "set_custom_fields": _set_fields,
        "seed_gz": _seed,
        "drain_ingest": _drain,
    }


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
    """Sync warehouse parquet → cache and open an in-memory connection
    with the Iceberg view registered. Returns (con, view_name)."""
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    data_dir = os.path.join(env["cache"], "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(env["warehouse"], "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _ingest_and_commit(env, expected):
    from backend.core import iceberg as ice

    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None, "no 'done' event from ingest"
    assert done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    ice.commit_buffer(env["fos_source"])


# ── Tests ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "duckdb_type,typed_value",
    [
        ("VARCHAR", "hello"),
        ("INTEGER", 42),
        ("BIGINT", 9_000_000_000),
        ("DOUBLE", 3.14),
        ("BOOLEAN", True),
    ],
)
@pytest.mark.skip(reason="Migrated to ducklake")
def test_null_handling_per_type(custom_field_env, duckdb_type, typed_value):
    """Explicit-null and missing-key rows both become SQL NULL; the
    typed-value row round-trips. Pinned for every declared duckdb_type."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"](
        [{"name": "side_car", "duckdb_type": duckdb_type, "enabled": True, "vcl": '"side_car":"x"'}]
    )
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz",
        [
            _row(base, 0, side_car=None),  # explicit JSON null
            _row(base + timedelta(seconds=1), 1),  # key omitted entirely
            _row(base + timedelta(seconds=2), 2, side_car=typed_value),
        ],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    out = dict(con.execute(f"SELECT url, side_car FROM {view} ORDER BY url").fetchall())
    assert out["/path/0"] is None, f"explicit null must be SQL NULL for {duckdb_type}, got {out['/path/0']!r}"
    assert out["/path/1"] is None, f"missing key must be SQL NULL for {duckdb_type}, got {out['/path/1']!r}"
    assert out["/path/2"] == typed_value, (
        f"typed value for {duckdb_type} must round-trip; got {out['/path/2']!r}, expected {typed_value!r}"
    )


@pytest.mark.skip(reason="Migrated to ducklake")
def test_aggregation_over_all_null_column(custom_field_env):
    """COUNT(*)=3, COUNT(col)=0, AVG IS NULL — ANSI semantics so
    dashboards render "no data" rather than a misleading zero."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.svc.gz",
        [_row(base + timedelta(seconds=i), i, score=None) for i in range(3)],
    )
    _ingest_and_commit(env, expected=3)

    con, view = _connect(env)
    n_rows, n_score, avg_score = con.execute(f"SELECT COUNT(*), COUNT(score), AVG(score) FROM {view}").fetchone()
    assert n_rows == 3, f"COUNT(*) must count NULL rows; got {n_rows}"
    assert n_score == 0, f"COUNT(score) must skip NULLs; got {n_score}"
    assert avg_score is None, f"AVG over all-NULL must be NULL, got {avg_score!r}"


def test_null_in_old_rows_survives_disable_enable_cycle(custom_field_env):
    """Old NULL rows stay NULL after a disable + re-enable of the field.
    Field-id slot reservation keeps old parquet schema-compatible."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "metric", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"metric":0'}])
    ice.init_iceberg_table(env["fos_source"])

    base = datetime.now(UTC) - timedelta(hours=2)
    env["seed_gz"](
        "raw/2026-05-20/10/2026-05-20T10-00-00.r1.gz",
        [_row(base + timedelta(seconds=i), i, metric=None) for i in range(2)],
    )
    _ingest_and_commit(env, expected=2)

    env["set_custom_fields"]([{"name": "metric", "duckdb_type": "INTEGER", "enabled": False, "vcl": '"metric":0'}])
    env["set_custom_fields"]([{"name": "metric", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"metric":0'}])

    env["seed_gz"](
        "raw/2026-05-20/11/2026-05-20T11-00-00.r2.gz",
        [_row(base + timedelta(hours=1, seconds=i), 100 + i, metric=500 + i) for i in range(2)],
    )
    _ingest_and_commit(env, expected=2)

    con, view = _connect(env)
    rows = dict(con.execute(f"SELECT url, metric FROM {view} ORDER BY url").fetchall())
    assert rows["/path/0"] is None, f"old NULL got rewritten as {rows['/path/0']!r}"
    assert rows["/path/1"] is None, f"old NULL got rewritten as {rows['/path/1']!r}"
    assert rows["/path/100"] == 500
    assert rows["/path/101"] == 501
