"""Custom-field NULL handling.

Pins that aggregates over an all-NULL custom-field column follow ANSI
SQL: COUNT(*) counts rows, COUNT(col) skips NULLs, AVG returns NULL.
The sibling type-mismatch suite covers structurally-bad values.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest


# Mirrors the harness in tests/core/test_custom_field_type_mismatch.py.
# Duplicated rather than imported: pytest doesn't auto-collect fixtures
# defined in sibling test modules.
@pytest.fixture
def custom_field_env(s3_mock, fos_source, monkeypatch, tmp_path):
    # Per-test service_id: DuckLake tables persist in the shared per-worker
    # Postgres catalog, and a reused table name resolves a data dir that was
    # never created under this test's tmp root.
    fos_source["service_id"] = f"nh{uuid.uuid4().hex[:10]}"
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
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
    """Open a DuckLake-attached connection with the ``logs`` view registered.
    Returns (con, view_name)."""
    from backend.core import iceberg as ice
    from backend.core.duckdb import get_connection
    from backend.repositories._base import _safe_table

    con = get_connection(env["fos_source"])
    ice.update_iceberg_view(con, env["fos_source"])
    return con, _safe_table(env["fos_source"]["name"])


def _ingest_and_commit(env, expected):
    from backend.core import iceberg as ice

    done = next((e for e in env["drain_ingest"]() if e["type"] == "done"), None)
    assert done is not None, "no 'done' event from ingest"
    assert done["rows_inserted"] == expected, f"expected {expected} rows, got {done}"
    ice.commit_buffer(env["fos_source"])


# ── Tests ────────────────────────────────────────────────────────────────────


def test_aggregation_over_all_null_column(custom_field_env):
    """COUNT(*)=3, COUNT(col)=0, AVG IS NULL — ANSI semantics so
    dashboards render "no data" rather than a misleading zero."""
    from backend.core import iceberg as ice

    env = custom_field_env
    env["set_custom_fields"]([{"name": "score", "duckdb_type": "INTEGER", "enabled": True, "vcl": '"score":0'}])
    ice.init_iceberg_table(env["fos_source"])

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
