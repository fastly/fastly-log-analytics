"""Regression tests for the concurrent-attach DuckLake poisoning bug.

Live incident (v3.0.0 dashboard investigation, 2026-09-03): every pooled
connection built while several requests concurrently triggered a fresh
``get_connection()`` came back from ``_ducklake_attach`` reporting success
(no exception), but ``ducklake_snapshots('lake')`` on those SAME
connections then failed forever with ``Catalog "__ducklake_metadata_lake"
does not exist`` — silently poisoning the connection for the rest of its
life in the pool. Reproduced empirically: N threads each opening their own
connection and calling ``_ducklake_attach`` concurrently corrupted every
one of them; serializing the attach with a lock made all of them succeed,
every time. This pins that fix.

Also covers the companion bug in ``_update_iceberg_view_locked``: it used
to unconditionally re-attach ``lake`` with ``read_only=False`` even on a
connection where ``lake`` was already attached read-only, which throws
"database with name 'lake' already exists" — swallowed as a false
success by ``_ducklake_attach``'s broad match, but the mode-mismatched
re-attach attempt is what triggers the corruption in the first place.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from backend.core.duckdb import get_connection
from backend.core.iceberg.buffer import _commit_buffer_impl


def _make_committed_source(tmp_path, name: str) -> dict:
    """A service with one real committed row — matches the production
    scenario (an already-initialized DuckLake catalog with real
    snapshots), which is what actually raced in the live incident. A
    totally fresh, never-written-to local-file catalog has its own
    separate pre-create quirk unrelated to this bug."""
    cache = tmp_path / f"cache_{name}"
    (cache / "buffer").mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    cols = {
        "timestamp": pa.array([ts], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array(["1.2.3.4"]),
        "_source_file": pa.array(["s3://b/raw/a.gz"]),
    }
    path = os.path.join(str(cache), "buffer", "batch_a.parquet")
    pq.write_table(pa.table(cols), path)
    assert _commit_buffer_impl(src)["rows_committed"] == 1
    return src


def test_concurrent_get_connection_all_attach_successfully(tmp_path):
    """N threads racing to build a fresh read-only pooled connection for
    the same already-initialized service must ALL end up with a working
    ``lake`` catalog — not just a non-throwing ``ATTACH`` statement.

    Before the ``_attach_lock`` fix, this reproduced the corruption on
    every racing connection, every run: ``_ducklake_attach`` returned
    True for all of them, but ``ducklake_snapshots('lake')`` then failed
    with ``Catalog "__ducklake_metadata_lake" does not exist`` on all of
    them too.
    """
    name = f"race{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    n_threads = 8
    results: list[tuple[int, str, object]] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        con = get_connection(source=src, read_only=True)
        try:
            row = con.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            with lock:
                results.append((i, "ok", row))
        except Exception as e:  # noqa: BLE001 - recording the failure IS the assertion
            with lock:
                results.append((i, "fail", str(e)))
        finally:
            con.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n_threads
    failures = [r for r in results if r[1] == "fail"]
    assert not failures, f"lake catalog corrupted on {len(failures)}/{n_threads} racing connections: {failures}"


def test_update_iceberg_view_locked_skips_reattach_when_lake_already_attached(tmp_path, monkeypatch):
    """A connection that already has ``lake`` attached must not be
    re-attached — a mode-mismatched re-attach (read_only=False on an
    already read_only=True-attached alias) is what corrupted the
    catalog in the live incident."""
    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"skip{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # get_connection() itself already attaches lake read-only for a pooled
    # connection (mirroring the real request-context path), so by the time
    # _update_iceberg_view_locked runs, lake is already attached.
    con = get_connection(source=src, read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        # lake is already attached — _update_iceberg_view_locked must not
        # re-attach it (that mode-mismatched re-attach is what corrupted
        # the catalog in the live incident).
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [], "lake already attached — _ducklake_attach must not be called again"

        # Calling it again is equally a no-op on the attach front.
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == []

        # The connection is genuinely still functional afterward.
        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()


def test_update_iceberg_view_locked_attaches_matching_connection_mode(tmp_path, monkeypatch):
    """When ``lake`` is NOT yet attached on the connection,
    ``_update_iceberg_view_locked`` must attach it matching the
    connection's actual read-only mode — not hardcode ``read_only=False``
    (which raises on a genuinely read-only connection and, on a
    connection where some OTHER alias attach already exists, is the
    mode-mismatched re-attach that corrupts the catalog)."""
    import duckdb

    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"raw{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # A bare read-only connection to the same file, bypassing
    # get_connection() entirely, so lake is genuinely not attached yet.
    con = duckdb.connect(src["duckdb_path"], read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [True], (
            f"expected a single read_only=True attach to match the RO connection, got {attach_calls}"
        )

        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()
