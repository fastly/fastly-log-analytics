"""Bounded asynchronous jobs for cold historical query plans.

This module is intentionally standalone.  It owns job lifecycle and output
limits, but it does not choose routes, start workers, or connect to storage.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Event, RLock
from typing import Any
from uuid import uuid4

from backend.high_scale.query_router import QueryPlan, QueryTier


class HistoricalJobState(StrEnum):
    """States exposed by the historical job manager."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class HistoricalJob:
    """Immutable view of a historical job and its bounded result."""

    job_id: str
    plan: QueryPlan
    state: HistoricalJobState
    created_at: datetime
    expires_at: datetime
    terminal_at: datetime | None
    rows: tuple[dict[str, Any], ...]
    row_count: int
    output_bytes: int
    truncated: bool
    error: str | None


@dataclass
class _JobRecord:
    job_id: str
    plan: QueryPlan
    created_at: datetime
    expires_at: datetime
    state: HistoricalJobState = HistoricalJobState.QUEUED
    terminal_at: datetime | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    output_bytes: int = 0
    truncated: bool = False
    error: str | None = None
    stop: Event = field(default_factory=Event)
    future: Future[Any] | None = None


class HistoricalJobManager:
    """Runs bounded cold-query producers in a small, caller-owned pool."""

    def __init__(
        self,
        *,
        max_workers: int = 2,
        max_jobs: int = 1000,
        max_rows: int = 50_000,
        ttl: timedelta = timedelta(minutes=15),
        terminal_grace_ttl: timedelta = timedelta(minutes=5),
        max_output_bytes: int = 8 * 1024 * 1024,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if max_jobs <= 0:
            raise ValueError("max_jobs must be positive")
        if max_rows <= 0:
            raise ValueError("max_rows must be positive")
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        if terminal_grace_ttl < timedelta(0):
            raise ValueError("terminal grace ttl must be non-negative")
        if max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be positive")
        self._max_jobs = max_jobs
        self._max_rows = max_rows
        self._ttl = ttl
        self._terminal_grace_ttl = terminal_grace_ttl
        self._max_output_bytes = max_output_bytes
        self._clock = clock or (lambda: datetime.now(UTC))
        self._jobs: dict[str, _JobRecord] = {}
        self._lock = RLock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="historical-job")

    def submit(
        self,
        plan: QueryPlan,
        producer: Callable[[], Iterable[Mapping[str, Any]]],
        *,
        now: datetime | None = None,
    ) -> str:
        """Queue a producer for a validated, bounded cold plan."""

        if plan.tier is not QueryTier.COLD_JOB or not plan.queued or not plan.bounded:
            raise ValueError("historical jobs require a bounded cold query plan")
        plan.validate()
        if plan.max_rows > self._max_rows:
            raise ValueError("query plan exceeds manager row limit")
        observed = self._utc(now or self._clock())
        with self._lock:
            self._purge_locked(observed)
            if len(self._jobs) >= self._max_jobs:
                raise RuntimeError("historical job capacity is full")
            job_id = uuid4().hex
            record = _JobRecord(job_id, plan, observed, observed + self._ttl)
            self._jobs[job_id] = record
            record.future = self._executor.submit(self._run, record, producer)
            return job_id

    def get(self, job_id: str, *, now: datetime | None = None) -> HistoricalJob | None:
        observed = self._utc(now or self._clock())
        with self._lock:
            self._purge_locked(observed)
            record = self._jobs.get(job_id)
            if record is None:
                return None
            if record.state in _ACTIVE_STATES and observed >= record.expires_at:
                self._expire_record_locked(record, observed)
            return self._snapshot(record)

    def cancel(self, job_id: str, *, now: datetime | None = None) -> bool:
        observed = self._utc(now or self._clock())
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.state in _TERMINAL_STATES:
                return False
            if observed >= record.expires_at:
                self._expire_record_locked(record, observed)
                return False
            record.stop.set()
            if record.state is HistoricalJobState.QUEUED:
                self._finish_locked(record, HistoricalJobState.CANCELLED, observed)
            return True

    def expire(self, *, now: datetime | None = None) -> int:
        """Mark all jobs past their TTL as expired and request worker stops."""

        observed = self._utc(now or self._clock())
        with self._lock:
            expired = 0
            for record in self._jobs.values():
                if record.state in _ACTIVE_STATES and observed >= record.expires_at:
                    self._expire_record_locked(record, observed)
                    expired += 1
            self._purge_locked(observed)
            return expired

    def purge(self, *, now: datetime | None = None) -> int:
        """Drop terminal records after their grace period."""

        observed = self._utc(now or self._clock())
        with self._lock:
            before = len(self._jobs)
            self._purge_locked(observed)
            return before - len(self._jobs)

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)

    def _run(self, record: _JobRecord, producer: Callable[[], Iterable[Mapping[str, Any]]]) -> None:
        with self._lock:
            if record.state is not HistoricalJobState.QUEUED:
                return
            if self._expired_locked(record):
                self._expire_record_locked(record, self._utc(self._clock()))
                return
            record.state = HistoricalJobState.RUNNING
        try:
            iterator = iter(producer())
            while True:
                with self._lock:
                    if self._stop_reason_locked(record):
                        return
                    if len(record.rows) == record.plan.max_rows:
                        record.truncated = True
                        self._finish_locked(record, HistoricalJobState.COMPLETED, self._utc(self._clock()))
                        return
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                with self._lock:
                    if self._stop_reason_locked(record):
                        return
                    row = dict(item)
                    row_bytes = len(
                        json.dumps(row, default=str, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    )
                    if record.output_bytes + row_bytes > self._max_output_bytes:
                        record.rows = []
                        record.output_bytes = 0
                        self._finish_locked(
                            record,
                            HistoricalJobState.FAILED,
                            self._utc(self._clock()),
                            error="historical job output exceeds byte limit",
                        )
                        return
                    record.rows.append(row)
                    record.output_bytes += row_bytes
            with self._lock:
                if self._stop_reason_locked(record):
                    return
                self._finish_locked(record, HistoricalJobState.COMPLETED, self._utc(self._clock()))
        except Exception as exc:
            with self._lock:
                if record.state in _ACTIVE_STATES:
                    self._finish_locked(record, HistoricalJobState.FAILED, self._utc(self._clock()), error=str(exc))

    def _stop_reason_locked(self, record: _JobRecord) -> bool:
        if record.state in {HistoricalJobState.CANCELLED, HistoricalJobState.EXPIRED, HistoricalJobState.FAILED}:
            return True
        if self._expired_locked(record):
            self._expire_record_locked(record, self._utc(self._clock()))
            return True
        if record.stop.is_set():
            self._finish_locked(record, HistoricalJobState.CANCELLED, self._utc(self._clock()))
            return True
        return False

    def _expired_locked(self, record: _JobRecord) -> bool:
        return self._utc(self._clock()) >= record.expires_at

    def _expire_record_locked(self, record: _JobRecord, observed: datetime) -> None:
        if record.state in _ACTIVE_STATES:
            record.stop.set()
            self._finish_locked(record, HistoricalJobState.EXPIRED, observed)

    def _finish_locked(
        self,
        record: _JobRecord,
        state: HistoricalJobState,
        observed: datetime,
        *,
        error: str | None = None,
    ) -> None:
        if record.state in _TERMINAL_STATES:
            return
        record.state = state
        record.terminal_at = observed
        record.error = error
        record.future = None

    def _purge_locked(self, observed: datetime) -> None:
        stale = [
            job_id
            for job_id, record in self._jobs.items()
            if record.terminal_at is not None and observed >= record.terminal_at + self._terminal_grace_ttl
        ]
        for job_id in stale:
            del self._jobs[job_id]

    def _snapshot(self, record: _JobRecord) -> HistoricalJob:
        rows = tuple(record.rows)
        return HistoricalJob(
            record.job_id,
            record.plan,
            record.state,
            record.created_at,
            record.expires_at,
            record.terminal_at,
            rows,
            len(rows),
            record.output_bytes,
            record.truncated,
            record.error,
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("job timestamps must be timezone-aware")
        return value.astimezone(UTC)


_ACTIVE_STATES = {HistoricalJobState.QUEUED, HistoricalJobState.RUNNING}
_TERMINAL_STATES = {
    HistoricalJobState.COMPLETED,
    HistoricalJobState.CANCELLED,
    HistoricalJobState.FAILED,
    HistoricalJobState.EXPIRED,
}
