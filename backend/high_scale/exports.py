"""Bounded asynchronous export jobs for high-scale query results."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any


class ExportState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class ExportJob:
    job_id: str
    state: ExportState
    rows_written: int
    bytes_written: int
    payload: bytes | None
    error: str | None


@dataclass
class _MutableJob:
    job_id: str
    expires_at: datetime
    state: ExportState = ExportState.QUEUED
    rows_written: int = 0
    bytes_written: int = 0
    payload: bytes | None = None
    error: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    future: Future[None] | None = None


class ExportManager:
    def __init__(
        self,
        *,
        max_workers: int = 2,
        max_rows: int = 100_000,
        max_bytes: int = 50_000_000,
        ttl_seconds: int = 3_600,
    ) -> None:
        if max_workers <= 0 or max_rows <= 0 or max_bytes <= 0 or ttl_seconds <= 0:
            raise ValueError("export limits must be positive")
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._ttl = timedelta(seconds=ttl_seconds)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="high-scale-export")
        self._jobs: dict[str, _MutableJob] = {}
        self._lock = threading.Lock()

    def submit(self, job_id: str, rows: Iterable[Any], serializer: Callable[[Any], bytes]) -> str:
        with self._lock:
            self._expire_locked(datetime.now(UTC))
            if job_id in self._jobs:
                raise ValueError(f"export job {job_id} already exists")
            job = _MutableJob(job_id=job_id, expires_at=datetime.now(UTC) + self._ttl, cancel_event=threading.Event())
            self._jobs[job_id] = job
            job.future = self._executor.submit(self._run, job, rows, serializer)
        return job_id

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state in {ExportState.COMPLETED, ExportState.CANCELLED, ExportState.FAILED}:
                return False
            job.cancel_event.set()
            if job.future is not None and job.future.cancel():
                job.state = ExportState.CANCELLED
            return True

    def wait(self, job_id: str) -> ExportJob:
        job = self._get_mutable(job_id)
        if job.future is not None:
            job.future.result()
        return self.get(job_id)

    def get(self, job_id: str) -> ExportJob:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            self._expire_locked(datetime.now(UTC))
            return self._snapshot(job)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _run(self, job: _MutableJob, rows: Iterable[Any], serializer: Callable[[Any], bytes]) -> None:
        with self._lock:
            if job.cancel_event.is_set():
                job.state = ExportState.CANCELLED
                return
            job.state = ExportState.RUNNING
        output = bytearray()
        try:
            for row in rows:
                if job.cancel_event.is_set():
                    raise _ExportCancelled
                if job.rows_written >= self._max_rows:
                    raise ValueError("export row limit exceeded")
                encoded = serializer(row)
                if not isinstance(encoded, bytes):
                    raise TypeError("export serializer must return bytes")
                output.extend(encoded)
                output.extend(b"\n")
                job.rows_written += 1
                job.bytes_written = len(output)
                if job.bytes_written > self._max_bytes:
                    raise ValueError("export byte limit exceeded")
            with self._lock:
                if job.cancel_event.is_set() or job.state is not ExportState.RUNNING:
                    job.payload = None
                    return
                job.payload = bytes(output)
                job.state = ExportState.COMPLETED
        except _ExportCancelled:
            with self._lock:
                job.state = ExportState.CANCELLED
                job.payload = None
        except Exception as exc:
            with self._lock:
                job.state = ExportState.FAILED
                job.error = str(exc)
                job.payload = None

    def _get_mutable(self, job_id: str) -> _MutableJob:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            return job

    def _expire_locked(self, now: datetime) -> None:
        for job in self._jobs.values():
            if now >= job.expires_at and job.state not in {ExportState.EXPIRED, ExportState.CANCELLED}:
                job.cancel_event.set()
                job.state = ExportState.EXPIRED
                job.payload = None

    @staticmethod
    def _snapshot(job: _MutableJob) -> ExportJob:
        return ExportJob(job.job_id, job.state, job.rows_written, job.bytes_written, job.payload, job.error)


_default_manager: ExportManager | None = None
_default_manager_lock = threading.Lock()


def get_export_manager() -> ExportManager:
    """Return the process-local bounded manager used by the HTTP surface."""
    global _default_manager
    with _default_manager_lock:
        if _default_manager is None:
            _default_manager = ExportManager()
        return _default_manager


class _ExportCancelled(Exception):
    pass
