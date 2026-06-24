"""Multi-service isolation E2E (audit finding: cross-tenant data leakage).

Two services running concurrent writes — Iceberg commits in one test,
metadata_db mutations in the other — must keep their data fully
disjoint. Pins per-service primitives: ``_catalog_cache`` keyed by
``source.name``, per-service SQLite at
``data/services/{service_id}.metadata.db``, per-service iceberg locks.
A regression that collapsed two services onto one SqlCatalog handle or
routed ``insert_ingested_files`` against a shared file would silently
smear tenants — no single-service E2E catches it.

THREADS, not PROCESSES — same rationale as
``tests/core/test_multi_process_ingest.py``: the isolation primitives
under test are process-local data structures, so threading exercises
the contended path production scheduler ticks actually hit.
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import threading
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest


def _make_log_batch(*, n: int, ip_octet: int, path_prefix: str) -> pa.Table:
    """Per-service batch with service-identifying IPs + URL paths."""
    base = datetime.now(UTC) - timedelta(hours=1)
    return pa.table(
        {
            "timestamp": pa.array(
                [base + timedelta(minutes=i * 2) for i in range(n)], type=pa.timestamp("us", tz="UTC")
            ),
            "ip": pa.array([f"10.{ip_octet}.0.{i}" for i in range(n)]),
            "status": pa.array([200 if i % 5 else 500 for i in range(n)], type=pa.uint16()),
            "url": pa.array([f"/{path_prefix}/{i}" for i in range(n)]),
            "country": pa.array(["US"] * n),
            "method": pa.array(["GET"] * n),
            "ua": pa.array(["Mozilla/5.0"] * n),
            "pop": pa.array(["LAX"] * n),
        }
    )


def _make_source(*, name: str, service_id: str, bucket: str) -> dict:
    """``name`` IS the ``_catalog_cache`` key."""
    return {
        "name": name,
        "service_id": service_id,
        "service_name": f"Multi-Svc {name}",
        "bucket": bucket,
        "prefix": "logs",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "access_level": "read_write",
        "storage_mode": "cloud",
    }


@pytest.fixture
def two_service_env(monkeypatch):
    """Two file:// Iceberg warehouses, one per service. Extends the
    ``pipeline_env`` pattern in ``tests/test_e2e_pipeline.py`` but routes
    ``_warehouse_uri`` per ``source["name"]``. The autouse
    ``isolate_metadata_db`` fixture already redirects ``_cache_dir`` per
    bucket → distinct cache dirs + catalog SQLite files."""
    tmpdir = tempfile.mkdtemp(prefix="multi_svc_e2e_")
    warehouses = {
        "svc_a": os.path.join(tmpdir, "warehouse_a"),
        "svc_b": os.path.join(tmpdir, "warehouse_b"),
    }
    for p in warehouses.values():
        os.makedirs(p, exist_ok=True)
    src_a = _make_source(name="svc_a", service_id="svc-a-id", bucket="svc-a-bucket")
    src_b = _make_source(name="svc_b", service_id="svc-b-id", bucket="svc-b-bucket")

    # A bug smearing source dicts across services surfaces as a misrouted path here.
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", lambda s: f"file://{warehouses[s['name']]}")
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # _reset_module_caches (autouse) drains these; explicit clear future-proofs that contract.
    from backend.core import iceberg as _ice

    for c in (_ice._catalog_cache, _ice._snapshot_files_cache, _ice._table_object_cache):
        c.clear()
    if hasattr(_ice, "_view_cache"):
        _ice._view_cache.clear()

    yield {"src_a": src_a, "src_b": src_b, "warehouses": warehouses}
    shutil.rmtree(tmpdir, ignore_errors=True)


def _simulate_sync_one(source: dict, warehouse_path: str) -> None:
    """Copy warehouse parquet → ``cache/{bucket}/data/`` so the DuckDB
    view binding can read it. Conftest sandboxes ``_cache_dir`` per-bucket."""
    from backend.core.duckdb import _cache_dir

    data_dir = os.path.join(_cache_dir(source), "data")
    os.makedirs(data_dir, exist_ok=True)
    for sp in glob.glob(os.path.join(warehouse_path, "**", "*.parquet"), recursive=True):
        dst = os.path.join(data_dir, os.path.basename(sp))
        if not os.path.exists(dst):
            shutil.copy2(sp, dst)


# ── Test 1: concurrent iceberg commits, per-service isolation ──────────


def test_concurrent_iceberg_commits_two_services_no_cross_contamination(two_service_env):
    """Two services run buffer-write + commit concurrently. Post-commit,
    each iceberg table holds ONLY its own rows. Pins ``_catalog_cache``
    (key: ``source["name"]``) and ``_table_object_cache`` (key:
    ``(bucket, prefix, namespace, name)``). A refactor that collapsed
    either key — or a PyIceberg Transaction race that let one service
    clobber the other's metadata.json — would smear rows across tenants."""
    from backend.core import iceberg as ice
    from backend.repositories._base import _safe_table

    src_a, src_b = two_service_env["src_a"], two_service_env["src_b"]

    # Bootstrap sequentially: ``test_e2e_pipeline.py`` already covers the
    # init path; this test isolates concurrent COMMITS as the failure mode.
    ice.init_iceberg_table(src_a)
    ice.init_iceberg_table(src_b)
    ice.write_to_buffer(src_a, _make_log_batch(n=12, ip_octet=1, path_prefix="alpha"), "svc_a.parquet")
    ice.write_to_buffer(src_b, _make_log_batch(n=18, ip_octet=2, path_prefix="bravo"), "svc_b.parquet")

    barrier = threading.Barrier(2)
    results: dict[str, dict] = {}
    rlock = threading.Lock()

    def commit_worker(label: str, src: dict) -> None:
        outcome: dict = {"label": label}
        try:
            barrier.wait()
            res = ice.commit_buffer(src)
            outcome["rows_committed"] = res.get("rows_committed")
            outcome["snapshot_id"] = res.get("snapshot_id")
        except Exception as exc:
            outcome["error"] = repr(exc)
        with rlock:
            results[label] = outcome

    threads = [
        threading.Thread(target=commit_worker, args=("a", src_a)),
        threading.Thread(target=commit_worker, args=("b", src_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive(), "iceberg commit worker hung past 60s"

    for label, expected in (("a", 12), ("b", 18)):
        r = results[label]
        assert "error" not in r, f"svc_{label} commit raised: {r['error']}"
        assert r["rows_committed"] == expected, (
            f"svc_{label} committed {r['rows_committed']} rows; expected {expected} — "
            "concurrent commit leaked or dropped rows."
        )
        assert r["snapshot_id"] is not None, f"svc_{label} produced no snapshot"
    assert ice.buffer_files(src_a) == [] and ice.buffer_files(src_b) == []

    _simulate_sync_one(src_a, two_service_env["warehouses"]["svc_a"])
    _simulate_sync_one(src_b, two_service_env["warehouses"]["svc_b"])
    con = duckdb.connect(":memory:")
    ice.update_iceberg_view(con, src_a)
    ice.update_iceberg_view(con, src_b)
    view_a, view_b = _safe_table(src_a["name"]), _safe_table(src_b["name"])

    (count_a,) = con.execute(f"SELECT COUNT(*) FROM {view_a}").fetchone()
    (count_b,) = con.execute(f"SELECT COUNT(*) FROM {view_b}").fetchone()
    assert count_a == 12, f"svc_a view has {count_a} rows; expected 12 (no svc_b leak)"
    assert count_b == 18, f"svc_b view has {count_b} rows; expected 18 (no svc_a leak)"

    bleed_a_in_b = con.execute(
        f"SELECT COUNT(*) FROM {view_b} WHERE ip LIKE '10.1.%' OR url LIKE '/alpha/%'"
    ).fetchone()[0]
    bleed_b_in_a = con.execute(
        f"SELECT COUNT(*) FROM {view_a} WHERE ip LIKE '10.2.%' OR url LIKE '/bravo/%'"
    ).fetchone()[0]
    assert bleed_a_in_b == 0, f"{bleed_a_in_b} svc_a rows leaked into svc_b — cache key collision"
    assert bleed_b_in_a == 0, f"{bleed_b_in_a} svc_b rows leaked into svc_a — cache key collision"

    # Writer-side: each warehouse holds its own parquet files (disjoint subtrees).
    a_pq = glob.glob(os.path.join(two_service_env["warehouses"]["svc_a"], "**", "*.parquet"), recursive=True)
    b_pq = glob.glob(os.path.join(two_service_env["warehouses"]["svc_b"], "**", "*.parquet"), recursive=True)
    assert a_pq and b_pq, "one warehouse has no parquet after commit"
    assert not (set(a_pq) & set(b_pq)), "warehouses share a parquet path — fixture routing broke"


# ── Test 2: concurrent metadata_db writes, per-service isolation ───────


def test_concurrent_metadata_db_writes_two_services_no_cross_contamination():
    """Two services run independent metadata_db mutations on separate
    threads — svc-a deletes a seed row, svc-b inserts a new one. Each
    SQLite file must reflect ONLY its own mutation. Pins
    ``get_con(service_id)`` keying — a regression that cached the
    connection on the wrong key (or globalised ``_initialized`` without
    the per-service guard) would write svc-b's rows into svc-a's file.

    DEVIATION FROM SPEC: the spec said "delete a custom field in svc-a".
    Custom fields live in YAML service config (``backend/core/log_fields.py``
    + ``backend/config.py``), NOT in metadata_db. Pinned here instead: a
    raw DELETE on ``ingested_files`` for svc-a + ``insert_ingested_files``
    on svc-b — same isolation contract, real metadata_db surface."""
    from backend.core import metadata as metadata_db

    svc_a, svc_b = "svc-md-a", "svc-md-b"
    seed_key = "raw/2026-06-10/00/seed.gz"
    new_key = "raw/2026-06-10/01/svc_b_only.gz"

    # Same seed name on both sides so a cross-file smear would be visibly wrong.
    metadata_db.insert_ingested_files(svc_a, [(seed_key, 100, 1024)])
    metadata_db.insert_ingested_files(svc_b, [(seed_key, 100, 1024)])
    pre_a = {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)}
    pre_b = {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)}
    assert pre_a == {seed_key} and pre_b == {seed_key}, f"pre={pre_a!r}/{pre_b!r}"

    barrier = threading.Barrier(2)
    errors: list[str] = []
    elock = threading.Lock()

    def deleter_svc_a() -> None:
        try:
            barrier.wait()
            con = metadata_db.get_con(svc_a)
            con.execute(
                "DELETE FROM ingested_files WHERE source_name = ? AND file_name = ?",
                (svc_a, seed_key),
            )
            con.commit()
        except Exception as exc:
            with elock:
                errors.append(f"svc_a deleter: {exc!r}")

    def inserter_svc_b() -> None:
        try:
            barrier.wait()
            metadata_db.insert_ingested_files(svc_b, [(new_key, 250, 2048)])
        except Exception as exc:
            with elock:
                errors.append(f"svc_b inserter: {exc!r}")

    threads = [threading.Thread(target=deleter_svc_a), threading.Thread(target=inserter_svc_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive(), "metadata_db worker hung past 15s"
    assert not errors, f"metadata_db worker(s) raised: {errors}"

    post_a = {r["file_name"] for r in metadata_db.list_ingested_files(svc_a)}
    post_b = {r["file_name"] for r in metadata_db.list_ingested_files(svc_b)}
    assert post_a == set(), (
        f"svc_a should be empty after deletion; got {post_a!r} — "
        "either delete didn't land OR svc_b's insert smeared into svc_a."
    )
    assert post_b == {seed_key, new_key}, (
        f"svc_b should hold seed + concurrently-inserted row; got {post_b!r} — "
        "either insert didn't land OR svc_a's delete smeared into svc_b."
    )

    path_a, path_b = metadata_db.db_path(svc_a), metadata_db.db_path(svc_b)
    assert path_a != path_b, f"two services collapsed onto one SQLite file: {path_a}"
    assert os.path.exists(path_a) and os.path.exists(path_b)
