"""Shared per-cron-run lifecycle context manager (``cron_run``).

The ``@cron_task`` decorator (:mod:`backend.cron.decorators`) owns the OUTER
envelope — telemetry context, usage-log flush, watchdog hard-cap. It does NOT
own the INNER per-run lifecycle each job body repeated by hand:

1. look up the source/config for the service (optional),
2. acquire the per-``(service_id, task)`` lease via ``start_cron_run`` — a
   :class:`RuntimeError` here means the lease is already held and the tick is a
   CLEAN SKIP, never an error (Trap #7 teardown "config not found" is the same
   shape: a missing source is a skip, not a failure),
3. reset + start the in-memory progress store,
4. record a terminal ``cron_runs`` row with duration + status +
   exception serialization,
5. guarantee progress teardown (``end_progress``) and the optional
   ``finalize_cron_run_if_running`` / ``finalize_cron_duration`` backstops in a
   ``finally`` block.

``cron_run`` consolidates exactly that inner lifecycle while calling the SAME
primitives the bodies always called. Every primitive is imported at CALL TIME
from its canonical module so existing ``monkeypatch`` targets
(``backend.core.duckdb.*``, ``backend.cron_progress.*``,
``backend.cron.jobs._common.finalize_cron_duration``,
``backend.cron.scheduler._log_and_add_progress`` / ``_extract_log_text``) keep
intercepting them unchanged.

Two other lifecycles are deliberate and stay outside this manager: the HTTP-triggered
thread-spawn helpers (``routers/admin/iceberg.py`` and ``utils/router_utils.py``) own a run id
they hand to the job via ``run_id=``, which ``cron_run(run_id=...)`` then reuses; and
``record_skip_row`` below records a pre-flight skip that never takes the lease.

Usage::

    with cron_run(service_id, "partial_hour_merge", source=src,
                  error_summary="partial-hour merge failed") as run:
        if run.skipped:
            return
        stats = do_work(run.source)
        run.success(f"merged {stats['n']} files")
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("backend.scheduler")


class cron_run:  # noqa: N801 — context-manager factory, lowercase by convention
    """Context manager owning a single cron run's lease/progress/finalize cycle.

    Parameters
    ----------
    service_id:
        The service this run belongs to.
    task:
        Short task name used for BOTH the lease/``cron_runs`` row and the
        progress ``job_name`` (e.g. ``"expire_snapshots"``, ``"local_compact"``).
        This is the metadata task name, independent of the decorator's telemetry
        label.
    source:
        Pre-resolved source dict. When ``None``, ``__enter__`` looks it up via
        ``get_source_for_service``; a still-missing source is a clean skip.
    run_id:
        Externally-owned run id to REUSE instead of acquiring a new lease. When
        provided, ``start_cron_run`` is not called.
    error_summary:
        Summary recorded on the auto-``error`` row when the body raises without
        having recorded a terminal status itself. May be an ``exc -> str``
        callable for jobs whose summary embeds the exception text.
    error_result:
        Value (or ``exc -> value`` callable) assigned to ``self.result`` when the
        body raises — lets dict-returning jobs ``return run.result`` after the
        ``with``.
    finalize_duration:
        When True, call ``finalize_cron_duration`` in the ``finally`` block.
    finalize_running_backstop:
        When True, call ``finalize_cron_run_if_running`` BEFORE ``end_progress``
        in the ``finally`` block (expire's nested-finally guarantee).
    suppress_exceptions:
        When True (default), a body exception is recorded + swallowed so the job
        function returns normally (matching the bodies' historical ``try/except``
        swallow). Set False to re-raise after recording.
    include_log_output:
        When True (default), ``record`` attaches ``_extract_log_text(run_id)`` as
        the ``log_output``.
    skip_log_prefix:
        Optional bracketed prefix for the skip log line (defaults to ``task``).
    progress_task:
        Label passed to ``start_progress(task=...)`` when it differs from the
        lease/``cron_runs`` task name (defaults to ``task``). ``rollup_compact``
        leases as ``rollup_compact_daily`` but reports progress as
        ``rollup_compact``.
    backstop_summary, backstop_error_message:
        When either is set (with ``finalize_running_backstop``), the backstop
        ``finalize_cron_run_if_running`` call also receives ``duration_s``,
        ``summary`` and ``error_message`` so an orphaned ``running`` row is
        finalized with context (``log_discovery``).
    """

    def __init__(
        self,
        service_id: str,
        task: str,
        *,
        source: dict | None = None,
        run_id: int | None = None,
        error_summary: str | Callable[[BaseException], str] | None = None,
        error_result: Any | Callable[[BaseException], Any] | None = None,
        finalize_duration: bool = False,
        finalize_running_backstop: bool = False,
        suppress_exceptions: bool = True,
        include_log_output: bool = True,
        skip_log_prefix: str | None = None,
        progress_task: str | None = None,
        backstop_summary: str | None = None,
        backstop_error_message: str | None = None,
    ) -> None:
        self.service_id = service_id
        self.task = task
        self._source = source
        self._run_id = run_id
        self.error_summary = error_summary
        self.error_result = error_result
        self.finalize_duration = finalize_duration
        self.finalize_running_backstop = finalize_running_backstop
        self.suppress_exceptions = suppress_exceptions
        self.include_log_output = include_log_output
        self.skip_log_prefix = skip_log_prefix
        self.progress_task = progress_task or task
        self.backstop_summary = backstop_summary
        self.backstop_error_message = backstop_error_message

        self.result: Any = None
        self.skipped: bool = False
        self.skip_reason: str | None = None
        self.aborted: bool = False
        self._recorded: bool = False
        self._start_time: float | None = None

    # ------------------------------------------------------------------ props
    @property
    def source(self) -> dict | None:
        return self._source

    @property
    def run_id(self) -> int | None:
        return self._run_id

    @property
    def ok(self) -> bool:
        """True when the lease + progress setup succeeded and the body should
        run. False when the tick was skipped (no lease) OR progress setup
        aborted after the lease (error already recorded)."""
        return not self.skipped and not self.aborted

    def elapsed(self) -> float:
        """Seconds since the managed region started (0.0 before start)."""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    # ----------------------------------------------------------------- enter
    def __enter__(self) -> cron_run:
        from backend.core.duckdb import get_source_for_service, start_cron_run
        from backend.core.metadata.cron_log import CronLeaseHeldError

        if self._source is None:
            self._source = get_source_for_service(self.service_id)
            if self._source is None:
                # Trap #7: a service torn down mid-tick is a normal skip.
                self.skipped = True
                self.skip_reason = "source not found"
                return self

        if self._run_id is None:
            try:
                self._run_id = start_cron_run(self._source, self.task)
            except CronLeaseHeldError as e:
                # Lease already held by a concurrent/overrunning tick — SKIP,
                # never record an error.
                self.skipped = True
                self.skip_reason = str(e)
                self._log_skip(e)
                return self

        # The lease is now held. Progress setup runs INSIDE the protected
        # region (expire's finally-guarantee): a failure here must still
        # record a terminal row + finalize the lease, so it is caught and
        # turned into an abort rather than propagating out of __enter__
        # (which would skip __exit__ entirely and orphan the lease).
        self._start_time = time.time()
        try:
            from backend.cron_progress import cleanup_progress_and_reap, start_progress

            cleanup_progress_and_reap()
            start_progress(self._run_id, service_id=self.service_id, task=self.progress_task)
        except Exception as exc:
            self._record_exception(exc)
            self.aborted = True
        return self

    # ------------------------------------------------------------------ exit
    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.skipped:
            # __enter__ did no progress/lease setup to tear down.
            return False

        if exc is not None and not self._recorded:
            self._record_exception(exc)

        self._run_finally()

        if exc is not None:
            return self.suppress_exceptions
        return False

    # --------------------------------------------------------------- recording
    def record(
        self,
        status: str,
        *,
        summary: str | None = None,
        error_message: str | None = None,
        **log_kwargs: Any,
    ) -> None:
        """Persist the terminal ``cron_runs`` row for this run.

        ``status`` is passed POSITIONALLY to ``log_cron_run`` (arg index 3), the
        same way every migrated body called it. ``log_kwargs`` forwards the
        job-specific counters (``files_downloaded``, ``files_deleted_fos``,
        ``parquet_files_optimized``, ...).
        """
        from backend.core.duckdb import log_cron_run

        if self._source is None:
            # No source resolved (skipped tick) — nothing to persist.
            return

        log_output = None
        if self.include_log_output and self._run_id is not None:
            from backend.cron.scheduler import _extract_log_text

            log_output = _extract_log_text(self._run_id)

        log_cron_run(
            self._source,
            self.task,
            self.elapsed(),
            status,
            summary=summary,
            error_message=error_message,
            run_id=self._run_id,
            log_output=log_output,
            **log_kwargs,
        )
        self._recorded = True

    def success(self, summary: str | None = None, **log_kwargs: Any) -> None:
        self.record("success", summary=summary, **log_kwargs)

    def warn(self, summary: str | None = None, *, error_message: str | None = None, **log_kwargs: Any) -> None:
        self.record("warning", summary=summary, error_message=error_message, **log_kwargs)

    def fail(self, summary: str | None = None, *, error_message: str | None = None, **log_kwargs: Any) -> None:
        self.record("error", summary=summary, error_message=error_message, **log_kwargs)

    def log_progress(self, event: dict, *, job_name: str | None = None) -> None:
        """Emit a progress event through the scheduler helper (best-effort)."""
        from backend.cron.scheduler import _log_and_add_progress

        _log_and_add_progress(self._run_id, self.service_id, event, job_name=job_name or self.task)

    # ----------------------------------------------------------------- helpers
    def _log_skip(self, exc: BaseException) -> None:
        prefix = self.skip_log_prefix or self.task
        logger.info("⏭️  [%s] %s: skipping — %s", prefix, self.service_id, str(exc))

    def _record_exception(self, exc: BaseException) -> None:
        if callable(self.error_summary):
            summary = self.error_summary(exc)
        else:
            summary = self.error_summary or f"{self.task} failed"
        self.record("error", summary=summary, error_message=str(exc))
        self.log_progress({"type": "error", "message": str(exc)})
        logger.exception("[scheduler] %s: %s failed: %s", self.service_id, self.task, exc)
        if self.error_result is not None:
            self.result = self.error_result(exc) if callable(self.error_result) else self.error_result

    def _run_finally(self) -> None:
        from backend.cron_progress import end_progress

        if self.finalize_running_backstop and self._source is not None:
            from backend.core.duckdb import finalize_cron_run_if_running

            backstop_kwargs: dict[str, Any] = {}
            if self.backstop_summary is not None or self.backstop_error_message is not None:
                backstop_kwargs = {
                    "duration_s": self.elapsed(),
                    "summary": self.backstop_summary,
                    "error_message": self.backstop_error_message,
                }
            try:
                finalize_cron_run_if_running(self._source, self.task, self._run_id, **backstop_kwargs)
            finally:
                try:
                    end_progress(self._run_id)
                finally:
                    if self.finalize_duration:
                        self._finalize_duration()
        else:
            try:
                end_progress(self._run_id)
            finally:
                if self.finalize_duration:
                    self._finalize_duration()

    def _finalize_duration(self) -> None:
        if self._source is None or self._start_time is None:
            return
        from backend.cron.jobs._common import finalize_cron_duration

        finalize_cron_duration(self._source, self._run_id, self._start_time)


def record_skip_row(
    source: dict,
    task: str,
    summary: str,
    *,
    status: str = "skipped",
    duration_s: float = 0.0,
    error_message: str | None = None,
    run_id: int | None = None,
) -> None:
    """Persist a terminal ``cron_runs`` row for a tick that never took the lease.

    For pre-flight skips (nothing to evaluate, disk too full) that happen before
    :class:`cron_run` is entered: no lease, no progress, just the one row. Calls
    ``log_cron_run`` through ``backend.core.duckdb`` at call time so existing
    monkeypatch targets keep intercepting it.
    """
    from backend.core.duckdb import log_cron_run

    log_cron_run(source, task, duration_s, status, summary=summary, error_message=error_message, run_id=run_id)
