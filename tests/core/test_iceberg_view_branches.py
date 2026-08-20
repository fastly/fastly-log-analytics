"""Defensive-branch coverage for backend/core/iceberg/view.py.

Mirrors the pattern in tests/repositories/test_*_branches.py — narrow
unit tests for pure functions, lock/cache helpers, and error-path
fallbacks. The heavier view-rebuild / fast-path tests live in
test_iceberg.py and test_view_rebind_race.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.core.iceberg import view as view_mod

# ── configure_duckdb_s3: LOAD-then-INSTALL fallback ────────────────────────


def test_configure_duckdb_s3_happy_path_only_loads():
    """When LOAD succeeds on first try, INSTALL is never called."""
    con = MagicMock()
    view_mod.configure_duckdb_s3(con)
    assert con.execute.call_count == 1
    assert "LOAD iceberg" in con.execute.call_args[0][0]


def test_configure_duckdb_s3_falls_back_to_install_on_load_failure():
    """If extensions aren't yet installed, LOAD raises; we INSTALL then
    re-LOAD. Pinned because backend init runs against a clean DuckDB
    binary and would 500 every analytics endpoint without this fallback."""
    con = MagicMock()
    calls = []

    def _exec(sql):
        calls.append(sql)
        # First LOAD fails (extensions absent); INSTALL succeeds; second LOAD succeeds.
        if len(calls) == 1:
            raise RuntimeError("Extension not found")
        return None

    con.execute.side_effect = _exec
    view_mod.configure_duckdb_s3(con)
    # 1 failed LOAD + 1 INSTALL + 1 successful LOAD = 3 calls
    assert len(calls) == 3
    assert "INSTALL" in calls[1]


def test_configure_duckdb_s3_reraises_when_install_and_load_both_fail():
    """If both LOAD and INSTALL fail (offline, extensions absent), the
    function surfaces the failure rather than swallowing it (b49896a
    "surface extension-load errors"). A connection left silently without
    iceberg/httpfs otherwise fails far downstream with a confusing
    "iceberg_scan does not exist"; failing loud at setup points at the
    real cause."""
    con = MagicMock()
    con.execute.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        view_mod.configure_duckdb_s3(con)


# ── _source_variant_fp: per-variant cache-key fingerprint ──────────────────


def test_source_variant_fp_with_no_time_range_returns_empty_tr_tuple():
    fp = view_mod._source_variant_fp({"name": "s"})
    # (access_level, time_range_tuple, cron_enabled)
    assert fp == ("", (), True)


def test_source_variant_fp_includes_time_range_when_present():
    src = {
        "name": "s",
        "access_level": "read_write",
        "time_range": {"start": "2026-05-01T00:00:00Z", "end": "2026-05-02T00:00:00Z"},
    }
    fp = view_mod._source_variant_fp(src)
    assert fp == ("read_write", ("2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z"), True)


def test_source_variant_fp_reflects_cron_disabled():
    """provisioning.cron_sync.enabled=False flows into the fingerprint
    because the WHERE-clause branch differs."""
    src = {"name": "s", "provisioning": {"cron_sync": {"enabled": False}}}
    fp = view_mod._source_variant_fp(src)
    assert fp == ("", (), False)


def test_source_variant_fp_falls_back_when_provisioning_missing():
    """Missing provisioning dict defaults cron_enabled to True (the
    documented default)."""
    fp = view_mod._source_variant_fp({"name": "s"})
    assert fp[2] is True


# ── is_stale_view_error: substring detection ───────────────────────────────


@pytest.mark.parametrize(
    "msg",
    [
        "No files found: batch_x.parquet",
        "Catalog Error: Table with name foo does not exist",
        "No such file or directory: /tmp/x.parquet",
    ],
)
def test_is_stale_view_error_recognises_known_shapes(msg):
    assert view_mod.is_stale_view_error(RuntimeError(msg)) is True


@pytest.mark.parametrize(
    "msg",
    [
        "Syntax error at end of input",
        "Connection refused",
        "Binder Error: column status is ambiguous",
    ],
)
def test_is_stale_view_error_does_not_match_unrelated_messages(msg):
    """Errors that aren't stale-view shaped (syntax, binder, network)
    must NOT trigger the retry path — they propagate to the caller
    immediately."""
    assert view_mod.is_stale_view_error(RuntimeError(msg)) is False


# ── execute_with_stale_view_retry: non-stale re-raises, stale retries ──────


def test_execute_with_stale_view_retry_reraises_non_stale_immediately():
    """A non-stale exception bypasses the retry path entirely so callers
    aren't penalised for normal SQL errors with an extra view-rebuild."""
    con = MagicMock()
    called = {"n": 0}

    def _fn(c):
        called["n"] += 1
        raise RuntimeError("syntax error at end of input")

    with pytest.raises(RuntimeError, match="syntax error"):
        view_mod.execute_with_stale_view_retry(con, {"name": "svc"}, _fn)
    assert called["n"] == 1


def test_execute_with_stale_view_retry_retries_once_on_stale_view():
    """A stale-view shaped error triggers a clear+rebind+retry. Pin the
    retry runs exactly once — not in a loop — so a persistent stale
    view doesn't cascade into N retries."""
    con = MagicMock()
    calls = {"n": 0}

    def _fn(c):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("No files found: batch_xyz.parquet")
        return "ok"

    with (
        patch.object(view_mod._core_mod, "clear_source_caches") as mock_clear,
        patch.object(view_mod._core_mod, "update_iceberg_view") as mock_update,
    ):
        out = view_mod.execute_with_stale_view_retry(con, {"name": "svc"}, _fn)

    assert out == "ok"
    assert calls["n"] == 2
    mock_clear.assert_called_once_with("svc", keep_snapshot_cache=True)
    mock_update.assert_called_once()


def test_execute_with_stale_view_retry_propagates_second_failure():
    """If the retry also fails, the second exception propagates — no
    third try, no swallowing."""
    con = MagicMock()
    calls = {"n": 0}

    def _fn(c):
        calls["n"] += 1
        raise RuntimeError("No files found: batch.parquet")

    with (
        patch.object(view_mod._core_mod, "clear_source_caches"),
        patch.object(view_mod._core_mod, "update_iceberg_view"),
    ):
        with pytest.raises(RuntimeError, match="No files found"):
            view_mod.execute_with_stale_view_retry(con, {"name": "svc"}, _fn)

    assert calls["n"] == 2


# ── _get_service_lock: per-key locking ─────────────────────────────────────


def test_get_service_lock_returns_same_lock_for_same_key():
    """Per-service locking only works if get_service_lock is stable —
    different calls for the same key return the SAME lock object so
    threads actually contend."""
    a = view_mod._get_service_lock("svc-a")
    b = view_mod._get_service_lock("svc-a")
    assert a is b


def test_get_service_lock_returns_different_lock_for_different_key():
    """Different services don't share a lock — that's the whole point
    of per-service locking (avoid global bottleneck during S3 scans)."""
    a = view_mod._get_service_lock("svc-x")
    b = view_mod._get_service_lock("svc-y")
    assert a is not b


def test_get_service_lock_is_rlock():
    """Same-thread reentrant locking is required — the rebuild path
    acquires the same lock recursively via update_iceberg_view → ... →
    _rebuild_locked. A regular Lock would deadlock."""
    lock = view_mod._get_service_lock("svc-rlock")
    lock.acquire()
    # Reentrant acquire must succeed.
    lock.acquire()
    lock.release()
    lock.release()
    # No exception means RLock semantics held.


# ── clear_source_caches: keep_snapshot_cache flag ──────────────────────────


def test_clear_source_caches_default_wipes_both():
    """Default = drop view AND snapshot caches. Teardown / full reset
    relies on this contract."""
    view_mod._view_cache["svc-clr"] = ("loc", frozenset(), (), "sql", 1.0, False, ())
    view_mod._snapshot_files_cache["svc-clr"] = ("loc", "snap", "/tmp", [])
    view_mod.clear_source_caches("svc-clr")
    assert "svc-clr" not in view_mod._view_cache
    assert "svc-clr" not in view_mod._snapshot_files_cache


def test_clear_source_caches_with_keep_snapshot_preserves_snapshot_cache():
    """The self-heal path wants the view SQL regenerated WITHOUT losing
    the snapshot cache (a transient catalog blip shouldn't collapse the
    view to WHERE false). Pin that the snapshot entry survives."""
    view_mod._view_cache["svc-keep"] = ("loc", frozenset(), (), "sql", 1.0, False, ())
    view_mod._snapshot_files_cache["svc-keep"] = ("loc", "snap", "/tmp", [])
    view_mod.clear_source_caches("svc-keep", keep_snapshot_cache=True)
    assert "svc-keep" not in view_mod._view_cache
    assert "svc-keep" in view_mod._snapshot_files_cache
    # cleanup
    view_mod._snapshot_files_cache.pop("svc-keep", None)


# ── get_last_view_stats + inject_view_debug ────────────────────────────────


def test_get_last_view_stats_returns_empty_when_no_cache():
    """No cache entry → empty dict, not KeyError. Callers (inject_view_debug)
    rely on the empty-dict signal to skip injection."""
    assert view_mod.get_last_view_stats({"name": "svc-no-cache"}) == {}


def test_get_last_view_stats_returns_sql_and_timing_when_cached():
    view_mod._view_cache["svc-stats"] = (
        "metadata_loc",
        frozenset(),
        ("col1",),
        "SELECT 1",
        12.5,
        True,
        (),
    )
    stats = view_mod.get_last_view_stats({"name": "svc-stats"})
    assert stats == {"sql": "SELECT 1", "time_ms": 12.5, "was_fast_path": True}
    view_mod._view_cache.pop("svc-stats", None)


def test_inject_view_debug_no_op_when_no_stats():
    """No cached view SQL → debug_list stays empty. Defends against a
    debug panel that would otherwise show a stale '-- DuckDB Iceberg View
    Resolution --' header with no body."""
    debug: list = []
    view_mod.inject_view_debug(debug, {"name": "svc-no-debug"})
    assert debug == []


def test_inject_view_debug_prepends_entry_when_stats_present():
    """When the view was rebuilt for this source, prepend an entry to
    the debug list so the resolution shows FIRST in the panel (UI
    convention — the heaviest query lives at the top)."""
    view_mod._view_cache["svc-debug"] = (
        "loc",
        frozenset(),
        (),
        "CREATE VIEW v AS SELECT 1",
        7.0,
        False,  # slow path → mode label
        (),
    )
    debug = [{"sql": "SELECT x", "time_ms": 1.0}]
    view_mod.inject_view_debug(debug, {"name": "svc-debug"})
    assert len(debug) == 2
    assert "DuckDB Iceberg View Resolution" in debug[0]["sql"]
    assert "SLOW PATH" in debug[0]["sql"]
    view_mod._view_cache.pop("svc-debug", None)


def test_update_iceberg_view_skips_compacted_files():
    """Verify that update_iceberg_view skips files that have been locally compacted,
    preventing them from triggering an s3 fallback (iceberg_scan)."""
    source = {
        "service_id": "svc_compacted",
        "name": "svc_compacted",
        "access_level": "read_write",
    }

    # Mock scan.plan_files() to return a mock file
    mock_file = MagicMock()
    mock_file.file.file_path = "s3://bucket/data/file1.parquet"
    mock_scan = MagicMock()
    mock_scan.plan_files.return_value = [mock_file]

    mock_table = MagicMock()
    mock_table.scan.return_value = mock_scan
    mock_table.metadata_location = "s3://bucket/metadata/v1.metadata.json"
    mock_table.current_snapshot.return_value.snapshot_id = 123
    mock_table.location.return_value = "s3://bucket"

    # Mock get_locally_compacted_basenames to return "file1.parquet"
    mock_get_compacted = MagicMock(return_value={"file1.parquet"})

    def _exists_side_effect(path):
        if "iceberg_catalog.db" in str(path):
            return True
        return False

    # Mock sqlite3.connect
    mock_sqlite_conn = MagicMock()
    mock_sqlite_conn.__enter__.return_value = mock_sqlite_conn
    mock_sqlite_conn.execute.return_value.fetchone.return_value = ("s3://bucket/metadata/v1.metadata.json",)

    with (
        patch("backend.core.iceberg.view._core_mod._get_catalog"),
        patch("backend.core.iceberg.view._core_mod._load_table_cached", return_value=mock_table),
        patch("backend.core.iceberg.view._core_mod._cloud_uri_to_local_path", return_value="/cache/data/file1.parquet"),
        patch("backend.core.metadata.get_locally_compacted_basenames", mock_get_compacted),
        patch("os.path.exists", side_effect=_exists_side_effect),
        patch("sqlite3.connect", return_value=mock_sqlite_conn),
        patch("backend.core.iceberg.view._save_persistent_cache"),
        patch("backend.core.iceberg.view._core_mod._get_service_lock"),
    ):
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = None
        view_mod.update_iceberg_view(con, source)

        # Check snapshot cache
        cache_key = "svc_compacted"
        assert cache_key in view_mod._snapshot_files_cache
        cached_info = view_mod._snapshot_files_cache[cache_key]
        # cached_info[3] is the list of resolved files.
        # Since file1.parquet was compacted, it should have been skipped entirely
        # rather than being treated as missing and appended as an s3:// fallback path!
        assert "s3://bucket/data/file1.parquet" not in cached_info[3]
        assert "/cache/data/file1.parquet" not in cached_info[3]

    # clean up cache
    view_mod._snapshot_files_cache.pop(cache_key, None)
