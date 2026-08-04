"""Tests for backend.services.service_manager — stale-while-revalidate
on the dir-stats cache that dominates the /api/bootstrap cold-load."""

import time


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

    def _slow_walk(p):
        walk_count["n"] += 1
        time.sleep(0.1)  # large enough that 10 threads pile up before the first finishes
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)

    results = []
    results_lock = threading.Lock()

    def _call():
        r = sm._get_dir_stats(path)
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=_call) for _ in range(10)]
    for t in threads:
        t.start()
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
