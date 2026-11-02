"""Centralized retry helper backed by ``tenacity``."""

from __future__ import annotations

import time
from collections.abc import Callable

import tenacity
from tenacity.wait import wait_base, wait_exponential, wait_fixed, wait_random


def retry_call[T](
    fn: Callable[[], T],
    *,
    retry_on: Callable[[BaseException], bool],
    attempts: int,
    base_delay: float = 0.0,
    max_delay: float | None = None,
    multiplier: float = 2.0,
    jitter: float = 0.0,
    max_elapsed: float | None = None,
    before_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Execute ``fn`` with exponential backoff and optional jitter via ``tenacity``."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    if base_delay > 0:
        wait_strategy: wait_base = wait_exponential(
            multiplier=base_delay,
            exp_base=multiplier,
            max=max_delay if max_delay is not None else float("inf"),
        )
    else:
        wait_strategy = wait_fixed(0.0)
    if jitter > 0:
        wait_strategy = wait_strategy + wait_random(0.0, jitter)

    def _before_sleep(retry_state: tenacity.RetryCallState) -> None:
        if before_retry is None:
            return
        outcome = retry_state.outcome
        exc = outcome.exception() if outcome is not None else None
        if exc is not None:
            before_retry(exc)

    stop: tenacity.stop.stop_base = tenacity.stop_after_attempt(attempts)
    if max_elapsed is not None:
        stop = stop | tenacity.stop_after_delay(max_elapsed)
    retrying = tenacity.Retrying(
        retry=tenacity.retry_if_exception(retry_on),
        stop=stop,
        wait=wait_strategy,
        before_sleep=_before_sleep if before_retry is not None else None,
        reraise=True,
        sleep=sleep,
    )
    return retrying(fn)


__all__ = ["retry_call"]
