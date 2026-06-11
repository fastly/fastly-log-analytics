"""Polling helpers for tests that wait on background-thread state changes.

``time.sleep(X)`` after triggering a background operation is flaky: too short
under CI load → false negatives, too long under local dev → slow suite. Polling
with exponential backoff returns the instant the condition holds, and only
sleeps the maximum interval when actually waiting.
"""

import time
from collections.abc import Callable


def wait_until(
    check_fn: Callable[[], bool],
    timeout: float = 1.0,
    initial_interval: float = 0.001,
    backoff_factor: float = 2.0,
    max_interval: float = 0.05,
    message: str = "",
) -> None:
    """Poll ``check_fn`` until it returns truthy or ``timeout`` elapses.

    Starts at 1ms and backs off exponentially up to 50ms so a tight loop
    doesn't starve the GIL on single-core CI runners. Returns the moment
    the condition holds; raises ``AssertionError`` on timeout.
    """
    start = time.perf_counter()
    interval = initial_interval
    while True:
        if check_fn():
            return
        if time.perf_counter() - start >= timeout:
            suffix = f": {message}" if message else ""
            raise AssertionError(f"Timed out after {timeout}s waiting for condition{suffix}")
        time.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)
