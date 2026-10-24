"""Client-side section wall time and explicit ClickHouse query ownership."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from typing import Any, Literal, TypeVar
from uuid import uuid4

import structlog

from backend.core.query_attribution import current_attribution
from backend.utils.telemetry import get_page_load_id

_Result = TypeVar("_Result")
_Source = Literal["fact", "projection", "projected_fact", "mixed", "unknown"]
_CURRENT: ContextVar[dict[str, Any] | None] = ContextVar("high_scale_section", default=None)
_logger = structlog.get_logger(__name__)


def current_clickhouse_scope() -> dict[str, Any]:
    identity = _CURRENT.get()
    if identity is not None:
        return dict(identity)
    attribution = current_attribution.get()
    return {
        "request_id": attribution.request_id if attribution is not None else None,
        "page_load_id": get_page_load_id(),
        "service_id": None,
        "component": None,
        "section": None,
        "section_id": None,
        "parent_section_id": None,
        "source": "unknown",
    }


@contextmanager
def clickhouse_source(source: _Source) -> Iterator[None]:
    identity = {**current_clickhouse_scope(), "source": source}
    token = _CURRENT.set(identity)
    try:
        yield
    finally:
        _CURRENT.reset(token)


class HighScaleSections:
    def __init__(self, service_id: str, component: str) -> None:
        self.service_id = service_id
        self.component = component
        self._lock = threading.Lock()
        self._entries: list[dict[str, Any]] = []
        self._truncated = False

    def submit(
        self,
        pool: Executor,
        section: str,
        fn: Callable[[], _Result],
        *,
        source: _Source = "unknown",
        activity: Literal["compute", "overhead"] = "compute",
    ) -> Future[_Result]:
        queued = time.perf_counter()
        return pool.submit(
            copy_context().run, self.run, section, fn, source=source, activity=activity, submitted_at=queued
        )

    def run(
        self,
        section: str,
        fn: Callable[[], _Result],
        *,
        source: _Source = "unknown",
        submitted_at: float | None = None,
        activity: Literal["compute", "overhead"] = "compute",
    ) -> _Result:
        parent = current_clickhouse_scope()
        identity = {
            **parent,
            "service_id": self.service_id,
            "component": self.component,
            "section": f"{self.component}.{section}",
            "section_id": uuid4().hex,
            "parent_section_id": parent["section_id"],
            "source": source,
        }
        token = _CURRENT.set(identity)
        started = time.perf_counter()
        outcome = "error"
        try:
            result = fn()
            outcome = "success"
            return result
        finally:
            elapsed = (time.perf_counter() - started) * 1000
            _CURRENT.reset(token)
            entry = {
                **identity,
                "time_ms": elapsed,
                "executor_dispatch": "thread_pool" if submitted_at is not None else "inline",
                "executor_wait_ms": max(0.0, (started - submitted_at) * 1000) if submitted_at is not None else None,
                "outcome": outcome,
                "activity": activity,
                "timing_kind": "client_section_wall",
                "current_execution": True,
                "server_cpu_ms": None,
            }
            with self._lock:
                if len(self._entries) < 256:
                    self._entries.append(entry)
                else:
                    self._truncated = True
            try:
                _logger.info("high_scale.section", **entry)
            except Exception:
                logging.getLogger(__name__).debug("section logging failed", exc_info=True)

    def run_parallel(
        self,
        tasks: Sequence[tuple[str, Callable[[], _Result]]],
        *,
        source: _Source | Callable[[str], _Source] = "unknown",
        activity: Literal["compute", "overhead"] | Callable[[str], Literal["compute", "overhead"]] = "compute",
        max_workers: int | None = None,
        thread_name_prefix: str = "",
    ) -> dict[str, _Result]:
        if not tasks:
            return {}

        def _src(key: str) -> _Source:
            return source(key) if callable(source) else source

        def _act(key: str) -> Literal["compute", "overhead"]:
            return activity(key) if callable(activity) else activity

        if len(tasks) == 1:
            key, fn = tasks[0]
            return {key: self.run(key, fn, source=_src(key), activity=_act(key))}

        workers = min(max_workers, len(tasks)) if max_workers is not None else len(tasks)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name_prefix) as pool:
            futures = [(key, self.submit(pool, key, fn, source=_src(key), activity=_act(key))) for key, fn in tasks]
            return {key: fut.result() for key, fut in futures}

    def timings(self) -> list[dict[str, Any]]:
        with self._lock:
            result = [dict(entry) for entry in self._entries]
            if self._truncated:
                result.append(
                    {"section": f"{self.component}.observations_truncated", "time_ms": None, "activity": "unknown"}
                )
            return result
