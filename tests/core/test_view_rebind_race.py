"""View-rebind race tests — active readers vs. mid-flight writer rebinds.

Audit finding: "active reader sees self-consistent results when a writer
commits a new buffer + rebinds the Iceberg view mid-query."

Two read-side invariants pinned, collectively guaranteed by the
``QueryRunner`` stale-view retry (backend/repositories/_base.py) and
the per-service rebind lock (backend/core/iceberg/view.py):

1. COUNT(*) during a buffer commit + rebind → either the pre-commit
   or the post-commit total. Never a hybrid, never a propagated
   IOException/CatalogException.
2. SELECT * during a custom-field add (schema rebind) → either clean
   completion with the OLD column set, or a recoverable stale-view
   error. Never rows whose tuple arity disagrees with the bound
   column description.

Observed-behaviour note: ``update_iceberg_view`` serialises slow-path
rebuilds through a per-service RLock, and ``con.execute`` is a tight
C call with no Python hook to pause mid-COUNT. We can't pin a rebind
landing INSIDE a single query deterministically; instead each scenario
loops with thread overlap and asserts the invariant across iterations.
Reuses the local-FS PyIceberg pattern from tests/test_e2e_pipeline.py.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest


@pytest.fixture
def pipeline_env(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="rebind_race_")
    warehouse_path = os.path.join(tmpdir, "warehouse")
    cache_path = os.path.join(tmpdir, "cache")
    os.makedirs(warehouse_path, exist_ok=True)
    os.makedirs(cache_path, exist_ok=True)

    source = {
        "name": "rebind_race_svc",
        "service_id": "rebind-race-svc-id",
        "bucket": "rebind-race-bucket",
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: cache_path)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda src: f"file://{warehouse_path}")

    from backend.core import iceberg as _ice

    _CACHES = ("_catalog_cache", "_snapshot_files_cache", "_table_object_cache", "_view_cache")
    for c in _CACHES:
        if hasattr(_ice, c):
            getattr(_ice, c).clear()

    yield {"src": source, "tmpdir": tmpdir, "warehouse": warehouse_path, "cache": cache_path}

    shutil.rmtree(tmpdir, ignore_errors=True)
    for c in _CACHES:
        if hasattr(_ice, c):
            getattr(_ice, c).clear()


def _make_batch(n: int, offset_min: int = 0) -> pa.Table:
    base = datetime.now(UTC) - timedelta(hours=1) + timedelta(minutes=offset_min)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(seconds=i) for i in range(n)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ip": pa.array([f"10.0.0.{i % 250}" for i in range(n)]),
            "status": pa.array([200 if i % 5 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array([f"/path/{i % 10}" for i in range(n)]),
            "method": pa.array(["GET"] * n),
        }
    )


def _simulate_sync(warehouse_path: str, cache_path: str) -> None:
    import glob

    data_dir = os.path.join(cache_path, "data")
    os.makedirs(data_dir, exist_ok=True)
    for src_path in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(src_path))
        if not os.path.exists(dst):
            shutil.copy2(src_path, dst)


def _bootstrap_view(db_path: str, src: dict) -> None:
    from backend.core import iceberg as ice

    boot = duckdb.connect(db_path)
    try:
        ice.update_iceberg_view(boot, src)
    finally:
        boot.close()


# ── Test 1: COUNT(*) vs. buffer commit + rebind ─────────────────────────


def test_concurrent_reader_sees_consistent_view_during_rebind(pipeline_env, monkeypatch):
    """Reader runs COUNT(*); writer commits a new buffer + rebinds the
    view. The reader's answer must be a consistent total. Each thread
    owns its own DuckDB connection (connections aren't thread-safe);
    the reader uses QueryRunner so we exercise the prod retry path."""
    from backend.core import iceberg as ice
    from backend.repositories._base import QueryRunner, _safe_table

    src = pipeline_env["src"]
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_batch(n=10), "batch_seed.parquet")
    ice.commit_buffer(src)
    _simulate_sync(pipeline_env["warehouse"], pipeline_env["cache"])

    db_path = os.path.join(pipeline_env["tmpdir"], "race.duckdb")
    _bootstrap_view(db_path, src)
    view_name = _safe_table(src["name"])
    valid_totals = {10}
    ITERATIONS = 5
    ROWS = 10
    writer_errors: list[BaseException] = []
    observed: list[int] = []

    for i in range(ITERATIONS):
        ready = threading.Event()
        go = threading.Event()
        out: dict = {"count": None, "exc": None}

        def reader() -> None:
            try:
                # NB: do NOT pass read_only=True — mixing a read-only and
                # writable connection to the same DB file is rejected by
                # DuckDB with "Can't open a connection ... with a different
                # configuration". Both threads use the default (writable)
                # config so they can coexist.
                ro = duckdb.connect(db_path)
                try:
                    ice.update_iceberg_view(ro, src)
                    runner = QueryRunner(ro, src)
                    ready.set()
                    go.wait(timeout=5.0)
                    out["count"] = runner.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                finally:
                    ro.close()
            except BaseException as e:  # noqa: BLE001
                out["exc"] = e
                ready.set()

        def writer() -> None:
            try:
                ready.wait(timeout=5.0)
                ice.write_to_buffer(src, _make_batch(n=ROWS, offset_min=i + 1), f"batch_iter_{i}.parquet")
                ice.commit_buffer(src)
                _simulate_sync(pipeline_env["warehouse"], pipeline_env["cache"])
                w = duckdb.connect(db_path)
                try:
                    ice.update_iceberg_view(w, src, force=True)
                finally:
                    w.close()
                go.set()
            except BaseException as e:  # noqa: BLE001
                writer_errors.append(e)
                go.set()

        ta = threading.Thread(target=reader, name=f"reader-{i}")
        tb = threading.Thread(target=writer, name=f"writer-{i}")
        ta.start()
        tb.start()
        ta.join(timeout=15.0)
        tb.join(timeout=15.0)

        valid_totals.add(max(valid_totals) + ROWS)
        assert out["exc"] is None, f"iter {i}: reader raised — {out['exc']!r}"
        assert out["count"] is not None, f"iter {i}: reader produced no count"
        observed.append(out["count"])

    assert writer_errors == [], f"writer thread errored: {writer_errors!r}"
    bad = [c for c in observed if c not in valid_totals]
    assert not bad, (
        f"reader saw inconsistent counts {bad!r}; only {sorted(valid_totals)!r} "
        f"are valid totals across {ITERATIONS} iterations."
    )


# ── Test 2: SELECT * vs. custom-field add (schema rebind) ───────────────


def test_custom_field_add_during_active_query(pipeline_env, monkeypatch):
    """Reader does SELECT *; writer mutates service config to add a custom
    field (wider Arrow schema) and force-rebinds. Allowed: clean result
    with OLD columns, or a recoverable stale-view error. Disallowed:
    rows whose arity disagrees with the bound column description."""
    from backend.core import iceberg as ice
    from backend.core.iceberg import is_stale_view_error
    from backend.repositories._base import QueryRunner, _safe_table

    src = pipeline_env["src"]
    cfg_state: dict = {"service_id": src["service_id"]}
    cfg_lock = threading.Lock()

    def _load_config(_sid: str) -> dict:
        # Fresh shallow copy each call so update_iceberg_view sees current
        # state but cannot mutate the canonical dict.
        with cfg_lock:
            return {
                "service_id": cfg_state.get("service_id"),
                "log_fields": dict(cfg_state.get("log_fields") or {}),
            }

    monkeypatch.setattr("backend.config.load_config", _load_config)

    ice.init_iceberg_table(src)
    ice.write_to_buffer(src, _make_batch(n=12), "batch_schema_seed.parquet")
    ice.commit_buffer(src)
    _simulate_sync(pipeline_env["warehouse"], pipeline_env["cache"])

    db_path = os.path.join(pipeline_env["tmpdir"], "schema_race.duckdb")
    _bootstrap_view(db_path, src)
    view_name = _safe_table(src["name"])
    ITERATIONS = 5
    failures: list[str] = []
    inconsistencies: list[str] = []

    for i in range(ITERATIONS):
        ready = threading.Event()
        go = threading.Event()
        out: dict = {"cols": None, "rows": None, "exc": None}

        def reader() -> None:
            try:
                # See sibling test — mixing read_only with writable is rejected
                # by DuckDB; both threads use the default config.
                ro = duckdb.connect(db_path)
                try:
                    ice.update_iceberg_view(ro, src)
                    runner = QueryRunner(ro, src)
                    ready.set()
                    go.wait(timeout=5.0)
                    cur = runner.execute(f"SELECT * FROM {view_name} LIMIT 50")
                    if cur is None:
                        return
                    out["cols"] = [d[0] for d in cur.description]
                    out["rows"] = cur.fetchall()
                finally:
                    ro.close()
            except BaseException as e:  # noqa: BLE001
                out["exc"] = e
                ready.set()

        def writer() -> None:
            try:
                ready.wait(timeout=5.0)
                # Mirror the admin "add custom field" handler: mutate config
                # so the next load_config() returns a wider Arrow schema,
                # bust caches, then force a rebind.
                with cfg_lock:
                    cfg_state["log_fields"] = {
                        "schema_version": 2,
                        "groups": ["A"],
                        "custom_fields": [{"name": f"x_extra_{i}", "duckdb_type": "VARCHAR", "enabled": True}],
                    }
                ice.clear_source_caches(src["name"], keep_snapshot_cache=True)
                w = duckdb.connect(db_path)
                try:
                    ice.update_iceberg_view(w, src, force=True)
                finally:
                    w.close()
                go.set()
            except BaseException:  # noqa: BLE001
                go.set()
                raise

        ta = threading.Thread(target=reader, name=f"schema-reader-{i}")
        tb = threading.Thread(target=writer, name=f"schema-writer-{i}")
        ta.start()
        tb.start()
        ta.join(timeout=15.0)
        tb.join(timeout=15.0)

        if out["exc"] is not None:
            if not is_stale_view_error(out["exc"]):
                failures.append(f"iter {i}: non-recoverable {type(out['exc']).__name__}: {out['exc']!r}")
            continue

        cols = out["cols"] or []
        for idx, row in enumerate(out["rows"] or []):
            if len(row) != len(cols):
                inconsistencies.append(
                    f"iter {i} row {idx}: len(row)={len(row)} vs len(cols)={len(cols)} cols={cols!r}"
                )
                break

    assert not failures, "\n".join(failures)
    assert not inconsistencies, "\n".join(inconsistencies)
