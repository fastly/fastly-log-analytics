"""Tests for backend.core.reset.reset_service_logs — the Project Log Reset
("Delete Data") generator.

Focuses on the safety-critical guarantees called out in
local-docs/log_reset_design_plan.md: config preservation vs. operational
wipe, the per-service lock exclusion, the cron_busy guard, the
try/finally scheduler-resume-and-lock-release resilience under a mid-run
failure, and the iceberg/meta/ + raw/ cloud-purge scoping. Iceberg catalog
internals (init_iceberg_table / update_iceberg_view) are covered by their
own test modules, so they're stubbed here to keep this suite focused on
reset.py's own orchestration.
"""

from __future__ import annotations

import os
import time
from unittest.mock import MagicMock

import pytest

from backend import config as svcconfig
from backend.core import quarantine
from backend.core import reset as reset_mod
from backend.core.metadata.base import get_con
from tests.conftest import MOCK_SERVICE_ID


# ── Merged from tests/core/test_reset.py ──
def _save_config(**cron_sync_overrides):
    cron_sync = {"enabled": True, **cron_sync_overrides}
    svcconfig.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "name": "test_service",
            "fos_bucket": "test-bucket",
            "fos_prefix": "",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test-key",
            "fos_secret_access_key": "test-secret",
            "access_level": "read_write",
            "provisioning": {"cron_sync": cron_sync},
        },
    )


def _seed_metadata_rows():
    """Insert one row into every WIPE table and every PRESERVE table."""
    con = get_con(MOCK_SERVICE_ID)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES ('a.gz', ?, 10, 100, '2026-01-01')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingested_files_summary (source_name, file_count, total_rows, total_bytes) VALUES (?, 1, 10, 100)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json) VALUES ('buf1', ?, '[]')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO quarantined_files (file_name, source_name, fos_key, error_key, meta_key) "
        "VALUES ('bad.gz', ?, 'raw/bad.gz', 'errors/bad.bad.jsonl', 'errors/bad.meta.json')",
        (MOCK_SERVICE_ID,),
    )

    con.execute("INSERT INTO sources (name, config, table_name) VALUES (?, '{}', 'logs')", (MOCK_SERVICE_ID,))
    con.execute(
        "INSERT INTO views (id, service_id, name, filters_json) VALUES ('v1', ?, 'My View', '{}')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO alerts (id, service_id, name, metric, operator, threshold, window_min) "
        "VALUES ('al1', ?, 'My Alert', 'error_rate', '>', 5.0, 10.0)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_labels (id, service_id, sid, label) VALUES ('sl1', ?, 'session1', 'good')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_audit (service_id, action, actor) VALUES (?, 'relabel', 'ui')",
        (MOCK_SERVICE_ID,),
    )
    con.execute("INSERT INTO asn_names (asn, name) VALUES (12345, 'Test ASN')")
    con.commit()


def _table_count(table: str) -> int:
    con = get_con(MOCK_SERVICE_ID)
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


@pytest.fixture
def _stub_iceberg_catalog(monkeypatch):
    """Stub the catalog-rebuild steps — exercised by test_iceberg.py, not here."""
    monkeypatch.setattr("backend.core.iceberg._core.init_iceberg_table", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.view.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())


@pytest.fixture
def fake_scheduler():
    """A stand-in for the ``reload_scheduler`` callback the router/CLI inject
    (real callers pass ``lambda: get_scheduler().reload()`` — see the
    layering note on ``reset_service_logs``)."""
    return MagicMock()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_wipes_operational_preserves_config_and_meta(s3_mock, fos_source, fake_scheduler):
    _save_config()
    _seed_metadata_rows()
    evidence = quarantine.capture_evidence(
        MOCK_SERVICE_ID,
        "request",
        "raw/request/bad.gz",
        b'{"invalid":\x00}\n',
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid JSON",
    )
    evidence_path = quarantine._evidence_path(MOCK_SERVICE_ID, int(evidence["id"]))
    assert evidence_path.exists()
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/meta/admin_state.json", Body=b"{}")
    s3_mock.put_object(Bucket="test-bucket", Key="errors/bad.bad.jsonl", Body=b"bad")
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz",
        Body=b"raw",
    )

    events = list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))

    assert events[-1]["type"] == "done"

    for table in (
        "ingested_files",
        "ingested_files_summary",
        "ingest_in_flight",
        "quarantined_files",
        "quarantine_evidence",
    ):
        assert _table_count(table) == 0, f"{table} should be wiped"
    assert not evidence_path.exists()

    for table in ("sources", "views", "alerts", "scoring_labels", "scoring_audit", "asn_names"):
        assert _table_count(table) == 1, f"{table} should be preserved"

    # record_audit("logs_reset") is written on top of the pre-seeded row.
    assert _table_count("audit_logs") == 1

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert "iceberg/default/logs/data/foo.parquet" not in keys
    assert "iceberg/meta/admin_state.json" in keys, "iceberg/meta/ must survive the purge"
    assert "errors/bad.bad.jsonl" not in keys
    assert "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz" in keys, (
        "raw/ must be left alone when delete_raw_logs=False (default)"
    )

    # Scheduler paused then resumed, and the config flag restored.
    assert fake_scheduler.call_count >= 2
    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_delete_raw_logs_true_purges_raw_prefix(s3_mock, fos_source, fake_scheduler):
    _save_config()
    raw_key = "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz"
    s3_mock.put_object(Bucket="test-bucket", Key=raw_key, Body=b"raw")

    list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, delete_raw_logs=True, reload_scheduler=fake_scheduler))

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert raw_key not in keys


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_cron_busy_blocks_reset(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    monkeypatch.setattr("backend.core.metadata.cron_busy", lambda service_id: True)

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="Active sync or commit"):
        next(gen)

    # The lock must not be left held after the guard raises.
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1)
    lock.release()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_concurrent_reset_is_excluded_by_service_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    """RLock is reentrant per-thread, so exclusion only shows up against a
    genuinely different thread holding the lock — the same-thread case
    would silently reacquire and defeat the point of this test."""
    import threading

    _save_config()
    monkeypatch.setattr(reset_mod, "_LOCK_TIMEOUT_S", 0.1)

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    held = threading.Event()
    release = threading.Event()

    def _hold_lock():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold_lock, daemon=True)
    holder.start()
    held.wait(timeout=5)
    try:
        gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
        with pytest.raises(RuntimeError, match="Another operation is in progress"):
            next(gen)
    finally:
        release.set()
        holder.join(timeout=5)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_error_mid_reset_still_resumes_scheduler_and_releases_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    # _purge_prefix only calls the delete_fn for pages that actually list
    # objects — seed one so the mocked failure is reached (an empty
    # bucket would otherwise never invoke it).
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    monkeypatch.setattr(
        "backend.core.ingest._delete_objects_robust",
        MagicMock(side_effect=RuntimeError("FOS unreachable")),
    )

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="FOS unreachable"):
        for _ in gen:
            pass

    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True, "must resume the paused sync even on failure"
    assert fake_scheduler.call_count >= 2

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1), "lock must be released even when the reset raises"
    lock.release()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_no_service_raises_value_error(fake_scheduler):
    with pytest.raises(ValueError, match="No service found"):
        next(reset_mod.reset_service_logs("does-not-exist"))


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_purge_prefix_reports_progress_per_page_and_returns_total():
    """A large prefix (thousands of Iceberg data files) should show live
    progress per listed page instead of going silent until it's all done —
    the exact gap reported live during a real reset of a 100k+-file
    service. Uses a fake paginator so this doesn't depend on moto's
    default 1000-key page size."""

    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/a.parquet"}, {"Key": "iceberg/b.parquet"}]}
            yield {"Contents": [{"Key": "iceberg/c.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _FakePaginator()

    delete_calls = []

    def _fake_delete(fos_client, bucket, keys):
        delete_calls.append(list(keys))
        return len(keys)

    gen = reset_mod._purge_prefix(_FakeFos(), "bucket", "iceberg/", _fake_delete, label="thing(s)")
    result = _drain(gen)

    assert result["return"] == 3
    assert delete_calls == [["iceberg/a.parquet", "iceberg/b.parquet"], ["iceberg/c.parquet"]]
    # One status event per non-empty page, each showing the running total.
    assert [e["message"] for e in result["yielded"]] == [
        "Deleted 2 thing(s) so far...",
        "Deleted 3 thing(s) so far...",
    ]


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_purge_prefix_excludes_prefix_and_skips_empty_pages():
    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/meta/admin_state.json"}]}  # excluded, page has no deletable keys
            yield {"Contents": []}
            yield {"Contents": [{"Key": "iceberg/data/x.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            return _FakePaginator()

    def _fake_delete(fos_client, bucket, keys):
        return len(keys)

    gen = reset_mod._purge_prefix(
        _FakeFos(), "bucket", "iceberg/", _fake_delete, exclude_prefix="iceberg/meta/", label="thing(s)"
    )
    result = _drain(gen)
    assert result["return"] == 1
    assert [e["message"] for e in result["yielded"]] == ["Deleted 1 thing(s) so far..."]


def _drain(gen):
    yielded = []
    try:
        while True:
            yielded.append(next(gen))
    except StopIteration as stop:
        return {"yielded": yielded, "return": stop.value}


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_remove_tree_with_progress_reports_batches_and_removes_everything(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.parquet").write_bytes(b"x")

    gen = reset_mod._remove_tree_with_progress(str(tmp_path), batch_size=2)
    result = _drain(gen)

    assert result["return"] == 5
    # One progress event every 2 files removed (5 files -> events at 2 and 4).
    assert [e["message"] for e in result["yielded"]] == [
        "Removed 2 local cache files so far...",
        "Removed 4 local cache files so far...",
    ]
    assert not os.path.exists(tmp_path)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_and_restore_flags_round_trip_without_inventing_rum_enabled():
    """Restoring after a RUM reset must not create ``rum.enabled=False`` when the key never existed:
    ``is_rum_enabled`` gives ``rum.enabled`` precedence, so a fabricated False silently disables RUM."""
    from backend.config import is_rum_enabled
    from backend.core.reset import _pause_rum_flags, _restore_rum_flags

    cfg = {"rum_enabled": True, "rum": {"faro_version": "1.2.3"}}
    assert is_rum_enabled(cfg)

    paused = _pause_rum_flags(cfg)
    assert paused is not None
    assert not is_rum_enabled(cfg)

    _restore_rum_flags(cfg, paused)
    assert "enabled" not in cfg["rum"]
    assert cfg["rum_enabled"] is True
    assert is_rum_enabled(cfg)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_restore_keeps_an_explicit_rum_enabled_value():
    from backend.core.reset import _pause_rum_flags, _restore_rum_flags

    cfg = {"rum_enabled": False, "rum": {"enabled": True}}
    paused = _pause_rum_flags(cfg)
    assert paused is not None
    _restore_rum_flags(cfg, paused)
    assert cfg["rum"]["enabled"] is True
    assert cfg["rum_enabled"] is False


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_is_a_no_op_when_rum_is_not_enabled():
    from backend.core.reset import _pause_rum_flags

    cfg: dict = {"rum": {}}
    assert _pause_rum_flags(cfg) is None
    assert cfg == {"rum": {}}


# ── Merged from tests/services/test_service_manager.py ──
def _wait_for(condition_fn, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Spin until condition_fn() returns truthy or timeout. Avoids
    sleeping for fixed durations in tests that depend on a background
    thread completing."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def test_get_dir_stats_returns_zero_for_missing_path(tmp_path, monkeypatch):
    """Nonexistent paths settle on (0, 0). The cache stores this so we
    don't re-walk on every subsequent call (a deleted cache dir would
    otherwise cost a background walk on every TTL expiry)."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    missing = str(tmp_path / "does-not-exist")
    # Cold call: (0, 0) placeholder immediately, real (missing-path) walk
    # result lands on the background thread — which also happens to be
    # (0, 0), so this assertion holds on the very first call too.
    assert sm._get_dir_stats(missing) == (0, 0)
    assert _wait_for(lambda: missing in sm._dir_stats_cache)


def test_get_dir_stats_counts_files_recursively(tmp_path, monkeypatch):
    """Walk visits subdirs and returns (total_bytes, file_count).
    Pinned because subdirectory recursion is the actual cost driver
    on cache/ which has 22k+ files across 22k dirs."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.parquet").write_bytes(b"y" * 200)
    (sub / "c.parquet").write_bytes(b"z" * 50)

    path = str(tmp_path)
    # Cold call returns the (0, 0) placeholder immediately; the real
    # count lands once the background walk finishes.
    assert _wait_for(lambda: sm._get_dir_stats(path) == (350, 3))


def test_get_dir_stats_returns_cached_value_within_ttl(tmp_path, monkeypatch):
    """Within TTL, cache hit returns the cached value without re-walking.
    This is the steady-state hot path."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # First call is cold (returns (0, 0) immediately); wait for the
    # background walk to populate the real value before proceeding.
    sm._get_dir_stats(path)
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1))

    # Add a file AFTER caching. If the cache is used the new file should
    # NOT appear in the result.
    (tmp_path / "b.parquet").write_bytes(b"y" * 200)
    size, count = sm._get_dir_stats(path)
    assert count == 1, "cached value should ignore the newly-added file within TTL"
    assert size == 100


def test_get_dir_stats_returns_stale_value_and_refreshes_in_background(tmp_path, monkeypatch):
    """Stale-while-revalidate: when the cache entry is expired but
    present, return the stale value IMMEDIATELY and kick off a
    background refresh. The next call (after the bg refresh lands)
    sees the fresh value.

    This is the key cold-load mitigation. Without SWR the user pays
    the full ~700ms walk every time the cache expires (every 60s in
    the old code, every 5 min in the new code)."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # Prime the cache with stale data.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 999, 999)

    # Add real files post-priming so we can detect when the bg refresh runs.
    (tmp_path / "b.parquet").write_bytes(b"y" * 50)

    t0 = time.monotonic()
    size, count = sm._get_dir_stats(path)
    elapsed = time.monotonic() - t0

    # Critical: the call returned the STALE value (999/999), not the
    # fresh value (2 files / 150 bytes). The bg thread is still walking.
    assert (size, count) == (999, 999), (
        f"SWR must return STALE value on expired cache, not block on walk. Got ({size}, {count})."
    )
    # Returned essentially instantly (no walk on the foreground path).
    assert elapsed < 0.5, f"SWR foreground path must NOT block on the walk; took {elapsed:.3f}s"

    # The background refresh should land within the timeout, updating
    # the cache with the actual values (2 files / 150 bytes).
    assert _wait_for(lambda: sm._dir_stats_cache.get(path, (0, 0, 0))[2] == 2, timeout=2.0), (
        f"background refresh did not land within 2s. Cache state: {sm._dir_stats_cache.get(path)}"
    )

    # Next call sees the refreshed value.
    size2, count2 = sm._get_dir_stats(path)
    assert (size2, count2) == (150, 2), (
        f"after bg refresh, next call should return fresh value. Got ({size2}, {count2})."
    )


def test_get_dir_stats_first_ever_call_is_non_blocking(tmp_path, monkeypatch):
    """No cache entry yet → return the (0, 0) placeholder immediately
    (never block on the walk) and populate the real value on a
    background thread.

    This inverts the original contract on purpose: walking synchronously
    on first call was "fine" for a few thousand files, but for a service
    with 100k+ cache files it turned the very first post-restart
    /api/bootstrap or /api/services call into a multi-minute block —
    see the 2026-08-03 incident where a 162k-file cache dir made
    /admin unreachable for ~3 minutes after every deploy. The cost is
    now paid off the request path, every time, including the first."""
    import time as _time

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 42)
    path = str(tmp_path)

    t0 = _time.monotonic()
    size, count = sm._get_dir_stats(path)
    elapsed = _time.monotonic() - t0

    assert (size, count) == (0, 0), "first-ever call must return the placeholder, not block on the walk"
    assert elapsed < 0.5, f"first-ever call must not block on the walk; took {elapsed:.3f}s"

    assert _wait_for(lambda: sm._get_dir_stats(path) == (42, 1)), (
        "background walk did not populate the real value within the timeout"
    )


def test_get_dir_stats_coalesces_concurrent_refreshes(tmp_path, monkeypatch):
    """When two threads both see a stale entry at the same instant,
    only ONE background refresh should fire — guarded by
    _dir_stats_refresh_in_flight. Otherwise on a 50-tenant fleet with
    50 concurrent /api/bootstrap calls we'd spawn 50 walk threads,
    saturating the filesystem with redundant work."""
    import threading

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # Patch the worker to count invocations and block briefly so
    # concurrent SWR calls overlap with the in-flight marker.
    refresh_count = {"n": 0}
    real_walk = sm._walk_dir_stats

    def _slow_walk(p):
        refresh_count["n"] += 1
        time.sleep(0.05)
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)

    # Prime stale entry.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 0, 0)

    # Fire 10 concurrent SWR-triggering calls.
    threads = [threading.Thread(target=lambda: sm._get_dir_stats(path)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Wait for the (single) background refresh to land.
    assert _wait_for(lambda: len(sm._dir_stats_refresh_in_flight) == 0, timeout=2.0)

    assert refresh_count["n"] == 1, (
        f"coalescing failed: {refresh_count['n']} background walks fired for 10 concurrent calls "
        f"(expected exactly 1). The _dir_stats_refresh_in_flight guard is broken."
    )


def test_get_dir_stats_coalesces_cold_first_arrivals(tmp_path, monkeypatch):
    """When the cache is empty and N threads call _get_dir_stats(path)
    simultaneously, only ONE background walk should fire — the same
    `_dir_stats_refresh_in_flight` guard used for stale-refresh
    coalescing also covers this case, since the cold path no longer has
    a separate blocking code path of its own.

    Pinned because on a fleet cold-start (backend just rebooted, 50
    /api/bootstrap calls land in the first second), without this
    coalescing we'd fire 50 parallel walks for the same dir. Every
    caller gets the (0, 0) placeholder immediately now (none of them
    block), so this only asserts the walk count + eventual convergence,
    not that every caller's return value is already populated."""
    import threading

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    walk_count = {"n": 0}
    real_walk = sm._walk_dir_stats

    walk_blocker = threading.Event()

    def _slow_walk(p):
        walk_count["n"] += 1
        walk_blocker.wait(timeout=5.0)  # Wait until all threads have made their call
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)

    results = []
    results_lock = threading.Lock()

    call_barrier = threading.Barrier(11)  # 10 threads + main thread

    def _call():
        call_barrier.wait(timeout=5.0)
        r = sm._get_dir_stats(path)
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=_call) for _ in range(10)]
    for t in threads:
        t.start()

    # Wait for all threads to be ready to call
    call_barrier.wait(timeout=5.0)

    # Give them a tiny bit of time to actually make the call and get the placeholder
    time.sleep(0.1)

    # Unblock the background walk
    walk_blocker.set()

    for t in threads:
        t.join()

    assert walk_count["n"] == 1, (
        f"cold-path coalescing failed: {walk_count['n']} walks fired for 10 concurrent first-arrivals "
        f"(expected exactly 1). The _dir_stats_refresh_in_flight guard is broken."
    )
    # None of the concurrent callers blocked, so (0, 0) placeholders are
    # expected here — the real assertion is that the walk converges.
    assert all(r == (0, 0) for r in results), f"cold callers must get the placeholder, not block. Got {results}"
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1)), "background walk never converged on the real value"


def test_get_dir_stats_recovers_from_thread_start_failure(tmp_path, monkeypatch):
    """When Thread.start() raises (resource exhaustion, OS thread-limit),
    the in-flight marker must be released so the next reader can try
    again. Otherwise the cache becomes permanently stuck serving stale
    data until process restart — exactly the worst failure mode for a
    self-healing SWR design."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    path = str(tmp_path / "doesnt-matter")
    # Seed stale entry so the SWR branch fires.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 7, 7)

    # Patch threading.Thread on the service_manager module so .start() raises.
    class _ExplodingThread:
        def __init__(self, *a, **kw):
            pass

        def start(self):
            raise RuntimeError("can't start new thread (simulated resource exhaustion)")

    monkeypatch.setattr(sm.threading, "Thread", _ExplodingThread)

    # First SWR call: returns stale, tries to schedule, fails to start.
    result = sm._get_dir_stats(path)
    assert result == (7, 7), "stale value must still be served when thread start fails"

    # The critical invariant: in_flight must NOT contain the path after
    # the failed start, so the next reader can try again.
    assert path not in sm._dir_stats_refresh_in_flight, (
        f"path stuck in _dir_stats_refresh_in_flight after Thread.start() failure — "
        f"cache is now permanently stuck serving stale. Set state: {sm._dir_stats_refresh_in_flight}"
    )


def test_dir_stats_ttl_is_long_enough_for_tab_idle(tmp_path, monkeypatch):
    """Pins _DIR_STATS_TTL_SEC ≥ 300 (5 minutes). The whole point of
    the bump from 60s → 300s is that a typical tab-idle (coffee break)
    no longer pays the cold-walk cost. Regressing this to 60s would
    silently undo half the SWR win."""
    from backend.services import service_manager as sm

    assert sm._DIR_STATS_TTL_SEC >= 300, (
        f"_DIR_STATS_TTL_SEC must be ≥ 300s so tab-idle doesn't pay the cold walk "
        f"on the next /api/bootstrap. Got {sm._DIR_STATS_TTL_SEC}s."
    )


def test_bust_dir_stats_cache_is_not_defined(monkeypatch):
    """The _bust_dir_stats_cache function was dead code (defined, never
    called). The SWR design makes manual invalidation unnecessary —
    every read serves stale + schedules a refresh, so the cache is
    self-healing within one TTL window. Pinned so a future copy-paste
    that re-introduces _bust_dir_stats_cache also re-introduces a
    code reviewer prompt to question whether it's actually needed."""
    from backend.services import service_manager as sm

    assert not hasattr(sm, "_bust_dir_stats_cache"), (
        "_bust_dir_stats_cache was removed as part of the SWR refactor — "
        "the cache is self-healing via stale-while-revalidate. If you genuinely "
        "need manual invalidation (e.g. for an immediate-update UX flow), "
        "document the use case in a comment AND check that the call site "
        "actually matters (the previous incarnation had zero callers across "
        "the entire codebase, including tests)."
    )
