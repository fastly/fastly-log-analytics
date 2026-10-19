"""Shared internal retry policy built on :mod:`tenacity`.

One place to express the repo's bespoke "loop + classify + sleep + give up"
patterns (DuckDB transient query failures, FOS rate-limit backoff, CDN
download retries, stale-view self-heal) so the retryable-vs-fatal decision,
the bounded exponential backoff, and the attempt/time budget live together
instead of being re-derived per call site.

Design constraints this module exists to satisfy:

* **Explicit retryable predicate.** ``retry_on`` decides, per exception,
  whether another attempt happens. A predicate that returns ``False`` makes
  :func:`retry_call` re-raise that exception immediately — it never broadens
  what gets retried.
* **Bounded exponential backoff with optional jitter.** The delay before the
  Nth retry is ``base_delay * multiplier ** (N - 1)``, clamped to
  ``max_delay`` and optionally offset by up to ``jitter`` seconds of uniform
  randomness (jitter de-syncs converging retriers, matching the
  ``wait_exponential + wait_random`` shape already used in
  ``backend/core/fastly/client.py``).
* **Attempt and wall-clock budgets.** ``attempts`` caps the number of calls;
  ``max_elapsed`` optionally caps total time.
* **A between-attempts action hook.** ``before_retry`` runs after a retryable
  failure and before the sleep — the seam the stale-view self-heal uses to
  bust its view cache + force a view rebuild without changing its recovery
  semantics.

This is deliberately *retry mechanics only*. It does not own error
classification or recovery actions — those stay specialized at each call
site (see AGENTS.md "Anti-patterns explicitly rejected": a single unified
executor that also owns recovery becomes a leaky abstraction).
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

import tenacity

__all__ = ["retry_call"]


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
    """Call ``fn`` with bounded, backed-off retries on retryable failures.

    Args:
        fn: Zero-arg callable to invoke. Return value is passed through.
        retry_on: Predicate run against each raised exception. Returns ``True``
            to allow another attempt, ``False`` to re-raise immediately. The
            exception is never swallowed — on give-up (budget exhausted) the
            last exception is re-raised.
        attempts: Maximum number of calls to ``fn`` (``>= 1``). ``attempts=1``
            disables retrying (``fn`` is called once; any exception propagates
            per ``retry_on``).
        base_delay: Delay (seconds) before the first retry. ``0`` means no
            wait between attempts.
        max_delay: Upper clamp on any single backoff delay. ``None`` = no clamp.
        multiplier: Exponential growth factor applied per retry.
        jitter: Maximum extra uniform-random seconds added to each delay.
        max_elapsed: Optional wall-clock budget (seconds). When exceeded, the
            loop stops and the last exception is re-raised even if ``attempts``
            remain.
        before_retry: Optional action invoked with the retryable exception
            after a failed attempt and before the backoff sleep. Use it for a
            specialized recovery step (e.g. busting a cache). Exceptions it
            raises propagate (they are not themselves retried here).
        sleep: Injectable sleep function (test seam).

    Returns:
        Whatever ``fn`` returns on its first successful call.

    Raises:
        The last exception raised by ``fn`` once the budget is exhausted, or
        immediately when ``retry_on`` returns ``False``.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def _wait(retry_state: tenacity.RetryCallState) -> float:
        delay = base_delay * (multiplier ** (retry_state.attempt_number - 1))
        if max_delay is not None:
            delay = min(delay, max_delay)
        if jitter:
            delay += random.uniform(0, jitter)
        return max(delay, 0.0)

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
        wait=_wait,
        before_sleep=_before_sleep if before_retry is not None else None,
        reraise=True,
        sleep=sleep,
    )
    return retrying(fn)
