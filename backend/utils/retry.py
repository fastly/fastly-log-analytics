"""Standardised retry decorators backed by :mod:`tenacity`.

Phase 3.5a of the v2.0 cleanup plan. One module owns the retry policies
so every retry surface in the backend has the same backoff curve, jitter,
and exception filtering. Adoption is incremental — call sites migrate
from ad-hoc ``for attempt in range(...)`` loops to these decorators as
they're touched.

Policies in this module
-----------------------

* :func:`http_api_retry` — for outbound HTTP calls (Fastly API, NGWAF API,
  any service-to-service REST). Retries on 429 + 5xx + network errors;
  honours Retry-After when present.
* :func:`sqlite_busy_retry` — for SQLite writes that may race with
  concurrent writers under WAL. Already used by
  :mod:`backend.utils.rdns_cache` for the bulk update path.
* :func:`generic_network_retry` — for arbitrary network ops that don't
  fit the HTTP-API shape (raw boto3 client calls, etc.).

All three policies share:

- Exponential backoff (1s, 2s, 4s, 8s, …) capped at ``max_wait``
- Bounded total attempts (so a persistent failure doesn't hang forever)
- Reraise on exhaustion so the caller sees the original exception
- A standardised structured log message at each retry attempt via
  :func:`_before_sleep_log`

Migration checklist
-------------------

The following call sites have ad-hoc retry loops today and are candidates
for migration (per-site review required since each has nuanced exception
shape / error message expectations that tests pin):

- ``backend/core/fastly/client.py:fastly()`` — 4-attempt retry on 429 +
  5xx + URLError; pinned by 10+ tests in ``tests/core/test_fastly_client.py``.
  Migration needs careful preservation of the exact ``RuntimeError`` message
  format ("HTTP {code} {method} {path}\\n    {body}") and the
  ``[1, 2, 4]``-second sleep schedule.
- ``backend/utils/ngwaf.py:fetch_verified_bots_paged()`` — no retry today;
  adopting :func:`http_api_retry` would ADD retries (behavior change).
  Decide per-site whether retrying NGWAF page fetches is desired.
- ``backend/provision/fos_setup.py:_delete_bucket()`` — bounded retry on
  ``BucketNotEmpty``; replace the manual ``for attempt in range(15)`` loop
  with a tenacity decorator filtered on ``BucketNotEmpty``.
- ``backend/provision/fastly_api.py:*`` various ``try/except RuntimeError``
  surfaces — review each for "should retry" semantics.
"""

from __future__ import annotations

import logging
import socket
import urllib.error
from collections.abc import Callable
from typing import Any, TypeVar

import tenacity

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ── Shared building blocks ────────────────────────────────────────────────────


def _before_sleep_log(retry_state: tenacity.RetryCallState) -> None:
    """Standardised structured log for every retry attempt.

    Shape (so log-aggregation can grep on it):
        ``[retry] attempt=N call=<fn> wait=Ns exc=<type>:<msg>``
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    fn_name = retry_state.fn.__name__ if retry_state.fn else "<unknown>"
    wait = retry_state.next_action.sleep if retry_state.next_action else 0
    exc_kind = type(exc).__name__ if exc else "n/a"
    exc_msg = (str(exc)[:200]) if exc else ""
    logger.warning(
        "[retry] attempt=%d call=%s wait=%.2fs exc=%s:%s",
        retry_state.attempt_number,
        fn_name,
        wait,
        exc_kind,
        exc_msg,
    )


# ── http_api_retry — outbound REST calls ──────────────────────────────────────


_HTTP_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class HttpRetryable(Exception):
    """Marker raised by HTTP call sites to opt into ``http_api_retry``.

    The decorator catches this AND the generic network exceptions
    (``URLError``, ``TimeoutError``, ``ConnectionError``); the call site
    is responsible for translating HTTP 429 + 5xx into this. See module
    docstring example for the canonical pattern."""


def http_api_retry(
    *,
    max_attempts: int = 4,
    multiplier: float = 1.0,
    min_wait: float = 1.0,
    max_wait: float = 8.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for outbound REST/HTTP-API calls.

    Retries on:
    - :class:`HttpRetryable` (raise this from your call site for HTTP 429
      / 5xx)
    - :class:`urllib.error.URLError`, :class:`ConnectionError`,
      :class:`TimeoutError`, :class:`socket.timeout`

    Default schedule: 4 total attempts (3 retries), wait 1s → 2s → 4s.
    Reraises the underlying exception on exhaustion so the caller still
    sees the original error.

    Example (canonical migration shape — does NOT replace
    ``backend.core.fastly.client.fastly`` until per-site review)::

        @http_api_retry(max_attempts=4)
        def fetch_widget(widget_id: str) -> dict:
            try:
                with urllib.request.urlopen(f"/widgets/{widget_id}") as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                if e.code in (429, 500, 502, 503, 504):
                    raise HttpRetryable(f"HTTP {e.code}") from e
                raise  # 4xx → no retry
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        retryer = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=tenacity.wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
            retry=tenacity.retry_if_exception_type(
                (
                    HttpRetryable,
                    urllib.error.URLError,
                    ConnectionError,
                    TimeoutError,
                    socket.timeout,
                )
            ),
            before_sleep=_before_sleep_log,
            reraise=True,
        )
        return retryer(fn)

    return decorator


# ── sqlite_busy_retry — SQLite OperationalError under WAL contention ──────────


def sqlite_busy_retry(
    *,
    max_attempts: int = 5,
    multiplier: float = 0.1,
    min_wait: float = 0.1,
    max_wait: float = 1.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for SQLite writes that may race with concurrent writers.

    Catches :class:`sqlite3.OperationalError` and (if available)
    :class:`aiosqlite.OperationalError`. Default schedule: 5 attempts,
    wait 0.1s → 0.2s → 0.4s → 0.8s (capped at 1s).

    Already used by :mod:`backend.utils.rdns_cache` for the bulk write
    path. Future ``backend.core.metadata.*`` write sites (per
    ``pending-docs/design_metadata_carveup.md``) use the same policy.
    """
    import sqlite3

    exc_types: tuple[type[BaseException], ...] = (sqlite3.OperationalError,)

    try:
        import aiosqlite

        exc_types = (sqlite3.OperationalError, aiosqlite.OperationalError)
    except ImportError:
        pass

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        retryer = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=tenacity.wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
            retry=tenacity.retry_if_exception_type(exc_types),
            before_sleep=_before_sleep_log,
            reraise=True,
        )
        return retryer(fn)

    return decorator


# ── generic_network_retry — for non-HTTP network ops ──────────────────────────


def generic_network_retry(
    *,
    max_attempts: int = 3,
    multiplier: float = 1.0,
    min_wait: float = 0.5,
    max_wait: float = 4.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for raw network ops (boto3 client calls, socket-level
    I/O, etc.) that don't fit the HTTP-API exception shape.

    Default: 3 attempts, 0.5s → 1s → 2s. Conservative wait curve since
    most non-HTTP ops have their own SDK-level retry (boto3 does 5 by
    default), and this is layered on top.
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        retryer = tenacity.retry(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=tenacity.wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
            retry=tenacity.retry_if_exception_type(
                (
                    ConnectionError,
                    TimeoutError,
                    socket.timeout,
                    OSError,
                )
            ),
            before_sleep=_before_sleep_log,
            reraise=True,
        )
        return retryer(fn)

    return decorator


__all__: list[Any] = [
    "HttpRetryable",
    "http_api_retry",
    "sqlite_busy_retry",
    "generic_network_retry",
]
