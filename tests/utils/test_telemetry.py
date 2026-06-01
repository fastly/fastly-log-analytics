"""Unit tests for backend.utils.telemetry primitives.

Focus: the process-context propagation primitives that the proxy/iceberg
_inject hooks rely on. The cross-thread fallback test pins behavior that
the ContextVar-only path silently regressed: rows landing as NULL in
usage_log when _inject ran on fsspec's iothread or pyiceberg's executor
threads (which don't inherit ContextVar state from the cron thread).
"""

from __future__ import annotations

import threading

from backend.utils.telemetry import (
    _LATEST_PROCESS_CONTEXT_LOCK,
    get_process_context,
    get_process_context_with_fallback,
    process_context_scope,
    set_process_context,
)


def _reset_global_fallback() -> None:
    """Tests share process state — keep them order-independent."""
    import backend.utils.telemetry as _tel

    with _LATEST_PROCESS_CONTEXT_LOCK:
        _tel._LATEST_PROCESS_CONTEXT = None
        _tel._ACTIVE_CONTEXTS.clear()


def test_set_process_context_visible_in_same_thread():
    _reset_global_fallback()
    set_process_context("cron_alpha")
    assert get_process_context() == "cron_alpha"
    assert get_process_context_with_fallback() == "cron_alpha"


def test_get_process_context_with_fallback_returns_last_set_in_unrelated_thread():
    """A raw threading.Thread does NOT inherit ContextVar state from the
    spawning thread. get_process_context() returns None there; the fallback
    returns the last value set process-wide.

    This is exactly the fsspecIO / pyiceberg ExecutorFactory situation that
    caused 86% of pyiceberg.s3fs telemetry rows to land with NULL
    process_context in production."""
    _reset_global_fallback()
    set_process_context("cron_sync_main_thread")

    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["ctxvar"] is None, (
        "ContextVar must NOT propagate to a raw thread — if this changes, the "
        "fallback is no longer load-bearing and the global mirror can be removed."
    )
    assert captured["fallback"] == "cron_sync_main_thread"


def test_fallback_reflects_most_recent_setter_across_threads():
    """Last-writer-wins: thread B's set_process_context overwrites the
    fallback for thread C's reader. Documents the known limitation that
    concurrent crons can misattribute (worst case) — but never NULL."""
    _reset_global_fallback()
    set_process_context("cron_A")

    barrier = threading.Barrier(3)
    fallback_reads: list[str | None] = []

    def setter_b() -> None:
        barrier.wait()
        set_process_context("cron_B")

    def reader_c() -> None:
        barrier.wait()
        # Tiny race buffer so setter_b's write is observable; the test does
        # not depend on perfect ordering, just that the fallback returns
        # *some* non-None value (proves the cross-thread mirror works).
        import time

        time.sleep(0.05)
        fallback_reads.append(get_process_context_with_fallback())

    tb = threading.Thread(target=setter_b)
    tc = threading.Thread(target=reader_c)
    tb.start()
    tc.start()
    barrier.wait()
    tb.join()
    tc.join()

    assert fallback_reads == ["cron_B"], f"expected reader to see most recent setter's value; got {fallback_reads}"


def test_fallback_returns_none_when_never_set():
    """Before any set_process_context call, both readers return None — the
    fallback must not invent a value out of thin air."""
    _reset_global_fallback()

    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["ctxvar"] is None
    assert captured["fallback"] is None


def test_process_context_scope_resets_contextvar_and_global_on_exit():
    """The cron_task decorator wraps every job in process_context_scope.
    On exit, both the ContextVar and the process-global mirror must be
    cleared — otherwise APScheduler's reused worker threads carry the
    stale ContextVar into the next job, and the fsspec iothread keeps
    reading the stale global. Symptom (2026-05-20): sync_ngwaf_bots
    accumulated 1,100+ misattributed pyiceberg.s3fs rows.

    Run in a fresh thread so the ContextVar starts at its default (None),
    matching how APScheduler's worker threads see things on first job."""
    _reset_global_fallback()

    captured: dict[str, object] = {}

    def worker() -> None:
        with process_context_scope("cron_sync"):
            captured["in_ctxvar"] = get_process_context()
            captured["in_fallback"] = get_process_context_with_fallback()

        captured["out_ctxvar"] = get_process_context()
        captured["out_fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["in_ctxvar"] == "cron_sync"
    assert captured["in_fallback"] == "cron_sync"
    assert captured["out_ctxvar"] is None, (
        "ContextVar must be reset on scope exit; otherwise APScheduler reuses "
        "the worker thread with stale context for the next job."
    )
    assert captured["out_fallback"] is None, (
        "Process-global mirror must be cleared on scope exit; otherwise the "
        "fsspec iothread reads stale context as fallback and mis-attributes "
        "subsequent crons' I/O."
    )


def test_process_context_scope_stack_preserves_outer_when_inner_exits():
    """The active-context stack semantics: when a quick inner cron B exits
    while a long-running outer cron A is still active, the fallback must
    revert to A — NOT None. Pre-fix (CAS-clear) telemetry showed 80% of
    pyiceberg.s3fs rows tagged 'untagged:fsspecIO' because the fsspec
    iothread read None after B's exit during A's continuation."""
    _reset_global_fallback()

    a_started = threading.Event()
    b_done = threading.Event()
    captured: dict[str, str | None] = {}

    def long_running_a() -> None:
        with process_context_scope("cron_A_long"):
            a_started.set()
            b_done.wait(timeout=2.0)
            # After B exited, the fsspec iothread (simulated by a fresh
            # thread reading the fallback) must see cron_A again.
            fallback_reads: dict[str, str | None] = {}

            def iothread_reader() -> None:
                fallback_reads["v"] = get_process_context_with_fallback()

            t = threading.Thread(target=iothread_reader)
            t.start()
            t.join()
            captured["after_b_exit"] = fallback_reads["v"]

    def quick_b() -> None:
        a_started.wait(timeout=2.0)
        with process_context_scope("cron_B_quick"):
            # While B is active, fallback reflects most recent (B).
            captured["during_b"] = _read_fallback_from_iothread()
        b_done.set()

    ta = threading.Thread(target=long_running_a)
    tb = threading.Thread(target=quick_b)
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    assert captured["during_b"] == "cron_B_quick", (
        "While B's scope is active, the stack top (= B) should be the fallback value, not A."
    )
    assert captured["after_b_exit"] == "cron_A_long", (
        "After B exits, the stack pops back to A. The fsspec iothread must "
        "see A — not None (the CAS-clear regression that landed 80% of "
        "rows as untagged:fsspecIO)."
    )


def _read_fallback_from_iothread() -> str | None:
    """Helper: read the fallback from a fresh thread (no ContextVar inherit)
    so the test exercises the iothread path that motivates the global mirror."""
    holder: dict[str, str | None] = {}

    def _r() -> None:
        holder["v"] = get_process_context_with_fallback()

    t = threading.Thread(target=_r)
    t.start()
    t.join()
    return holder["v"]


def test_process_context_scope_handles_concurrent_setter_outside_scope():
    """If something calls set_process_context() OUTSIDE the scope (legacy
    code path or test helper), the scope's exit pops its own name from
    the stack and falls back to the prior stack top — not to the value
    the rogue setter wrote. The rogue value only sticks if the stack is
    otherwise empty."""
    _reset_global_fallback()

    captured: dict[str, str | None] = {}

    def worker() -> None:
        with process_context_scope("cron_A"):
            # Legacy code path that doesn't use the scope.
            set_process_context("rogue_value")
            captured["during"] = _read_fallback_from_iothread()

        captured["after"] = _read_fallback_from_iothread()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["during"] == "rogue_value"
    # After scope exit, A is popped → stack empty → mirror = None.
    # (The rogue setter overwrote the mirror but didn't touch the stack.)
    assert captured["after"] is None, (
        "Scope exit should restore from the stack, not preserve whatever a rogue setter happened to write last."
    )
