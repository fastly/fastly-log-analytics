"""Bounded asynchronous jobs for cold historical query plans.

This module is intentionally standalone.  It owns job lifecycle and output
limits, but it does not choose routes, start workers, or connect to storage.
"""

from __future__ import annotations

import json
import sys
import threading
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Event, Lock, RLock
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import event_digest
from backend.high_scale.cmcd import project_cmcd
from backend.high_scale.origin import build_origin_projection_rows
from backend.high_scale.performance import build_performance_projection_rows
from backend.high_scale.publication import (
    ClickHousePublication,
    PublicationResult,
    PublicationStatus,
    row_matches_serving_domain,
)
from backend.high_scale.query_service import QueryPlan, QueryTier


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


_default_manager: HistoricalJobManager | None = None
_default_manager_lock = Lock()


def get_historical_job_manager() -> HistoricalJobManager:
    """Return the process-local bounded manager used by the HTTP surface."""
    global _default_manager
    with _default_manager_lock:
        if _default_manager is None:
            _default_manager = HistoricalJobManager()
        return _default_manager


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
                with self._lock:
                    job.rows_written += 1
                    job.bytes_written = len(output)
                    bytes_written = job.bytes_written
                if bytes_written > self._max_bytes:
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
        expired_ids: list[str] = []
        for job in self._jobs.values():
            if now >= job.expires_at and job.state not in {ExportState.EXPIRED, ExportState.CANCELLED}:
                job.cancel_event.set()
                job.state = ExportState.EXPIRED
                job.payload = None
            elif now >= job.expires_at + self._ttl and job.state in {
                ExportState.EXPIRED,
                ExportState.CANCELLED,
                ExportState.COMPLETED,
                ExportState.FAILED,
            }:
                expired_ids.append(job.job_id)
        for job_id in expired_ids:
            del self._jobs[job_id]

    @staticmethod
    def _snapshot(job: _MutableJob) -> ExportJob:
        return ExportJob(job.job_id, job.state, job.rows_written, job.bytes_written, job.payload, job.error)


_default_export_manager: ExportManager | None = None
_default_export_manager_lock = threading.Lock()


def get_export_manager() -> ExportManager:
    """Return the process-local bounded manager used by the HTTP surface."""
    global _default_export_manager
    with _default_export_manager_lock:
        if _default_export_manager is None:
            _default_export_manager = ExportManager()
        return _default_export_manager


class _ExportCancelled(Exception):
    pass


# ── Archive-only rebuild of ClickHouse visibility (folded from backend.high_scale.replay) ──


@dataclass(frozen=True)
class ReplayResult:
    manifest_id: str
    rows_read: int
    publication: PublicationResult


@dataclass(frozen=True)
class ProjectionReplayResult:
    manifest_id: str
    rows_read: int
    rows_published: int


def _manifest_domain(manifest: ArchiveManifest) -> str:
    domain = getattr(manifest, "domain", None)
    if domain:
        return str(domain)
    source = getattr(manifest, "source", None)
    if source is not None and getattr(source, "domain", None):
        return str(source.domain)
    return "request"


def _manifest_service_id(manifest: ArchiveManifest) -> str:
    service_id = getattr(manifest, "service_id", None)
    if service_id:
        return str(service_id)
    source = getattr(manifest, "source", None)
    if source is not None and getattr(source, "service_id", None):
        return str(source.service_id)
    return ""


def replay_manifest(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    batch_id: str | None = None,
) -> ReplayResult:
    """Replay one archived manifest into ClickHouse.

    ``batch_id`` defaults to a stable ``replay:{manifest_id}`` id, so a
    genuine retry of the SAME replay attempt is correctly treated as a
    duplicate by :meth:`ClickHousePublication.publish`. Pass an explicit,
    run-scoped ``batch_id`` (e.g. from :func:`~backend.high_scale.recovery.
    rebuild_serving_state_from_fos`) for a disaster-recovery rebuild: the
    manifest-visibility bookkeeping lives in Postgres and can outlive an
    independent loss of the actual ClickHouse data, so trusting the default
    id's "already visible" record would silently skip re-inserting rows
    that are no longer actually there.
    """
    rows = _read_manifest_rows(manifest, archive)
    manifest_domain = _manifest_domain(manifest)
    serving_rows = tuple(
        row
        for row in rows
        if row.get("_record_kind", "event") == "event" and row_matches_serving_domain(manifest_domain, row)
    )
    resolved_batch_id = batch_id or f"replay:{manifest.manifest_id}"
    if not serving_rows:
        return ReplayResult(
            manifest.manifest_id,
            0,
            PublicationResult(
                batch_id=resolved_batch_id,
                status=PublicationStatus.VISIBLE,
                rows_visible=0,
                duplicate=False,
            ),
        )
    batch = _batch_from_rows(resolved_batch_id, manifest, serving_rows)
    result = publication.publish(batch)
    return ReplayResult(manifest.manifest_id, len(serving_rows), result)


def _load_manifest_request_events(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    *,
    projection_label: str,
) -> tuple[dict[str, Any], ...]:
    if _manifest_domain(manifest) != "request":
        raise ValueError(f"{projection_label} projections can only be rebuilt from request manifests")
    rows = _read_manifest_rows(manifest, archive)
    return tuple(row for row in rows if row.get("_record_kind", "event") == "event")


def replay_origin_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    rebuild_id: str,
) -> ProjectionReplayResult:
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Origin")
    projection = build_origin_projection_rows(serving_rows)
    published = 0
    for suffix, domain, projected_rows in (
        ("summary", "origin_summary", projection.summary_rows),
        ("dimensions", "origin_dimensions", projection.dimension_rows),
    ):
        if not projected_rows:
            continue
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-origin:{rebuild_id}:{manifest.manifest_id}:{suffix}",
                manifest,
                projected_rows,
                domain=domain,
            )
        )
        published += result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def replay_performance_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    rebuild_id: str,
) -> ProjectionReplayResult:
    """Rebuild the Performance waterfall, scatter and ASN-dimension projections.

    URL rows in ``performance_minute_dimensions`` already exist for historical data
    and are deliberately not republished: re-adding them would double-count. ASN
    rows never existed before the integer-ASN fix, so they are safe to backfill.
    """
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Performance")
    projection = build_performance_projection_rows(serving_rows)
    published = 0
    for suffix, domain, projected_rows in (
        ("summary", "performance_summary", projection.summary_rows),
        ("scatter", "performance_scatter", projection.scatter_rows),
        (
            "asn",
            "performance_dimensions",
            tuple(row for row in projection.dimension_rows if row["dimension"] == "asn"),
        ),
    ):
        if not projected_rows:
            continue
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-performance:{rebuild_id}:{manifest.manifest_id}:{suffix}",
                manifest,
                projected_rows,
                domain=domain,
            )
        )
        published += result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def replay_cmcd_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    rebuild_id: str,
) -> ProjectionReplayResult:
    """Rebuild one request manifest's ``cmcd_projection_facts`` rows from the archive."""
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="CMCD")
    projected: list[dict[str, Any]] = []
    for row in serving_rows:
        # Parquet unifies the CMCD struct across rows, so absent keys come back as None.
        fields = {key: value for key, value in (row.get("cmcd") or {}).items() if value is not None}
        if not fields:
            continue
        projection = project_cmcd({**row, "cmcd": fields})
        if projection is not None:
            projected.append(projection)
    published = 0
    if projected:
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-cmcd:{rebuild_id}:{manifest.manifest_id}",
                manifest,
                tuple(projected),
                domain="cmcd",
            )
        )
        published = result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def _read_manifest_rows(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
) -> tuple[dict[str, Any], ...]:
    payload = archive.read_artifact(manifest)
    table = pq.read_table(pa.BufferReader(payload))
    rows = tuple(table.to_pylist())
    if len(rows) != manifest.artifact.row_count:
        raise ValueError("replay row count does not match archive manifest")
    if event_digest(rows) != manifest.artifact.canonical_digest:
        raise ValueError("replay event digest does not match archive manifest")
    return rows


def _batch_from_rows(
    batch_id: str,
    manifest: ArchiveManifest,
    rows: tuple[dict[str, Any], ...],
    *,
    domain: str | None = None,
):
    from backend.high_scale.publication import HighScaleBatch

    return HighScaleBatch(
        batch_id=batch_id,
        service_id=_manifest_service_id(manifest),
        domain=domain or _manifest_domain(manifest),
        generation=str(manifest.archive_epoch),
        rows=rows,
    )


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.exports"] = _self_mod
sys.modules["backend.high_scale.replay"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "exports", _self_mod)
    setattr(_hs_pkg, "replay", _self_mod)
