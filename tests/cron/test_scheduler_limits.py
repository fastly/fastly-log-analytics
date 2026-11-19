"""Bounded tests for scheduler limits (max_files, max_seconds) and watchdog timeout semantics."""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from backend.core.request_telemetry import current_attribution
from backend.cron.jobs.rum_ledger import _discover_high_scale_prefix
from backend.cron.jobs.sync import _run_full_sweep, discover_prefix
from backend.cron.scheduler import lifecycle as sched_lifecycle


def _make_objects(start: int, count: int) -> tuple[SimpleNamespace, ...]:
    return tuple(
        SimpleNamespace(
            object_key=f"raw/request/f-{i:04d}.gz",
            checksum=f"sum-{i:04d}",
            size_bytes=128,
            version=f"v{i}",
        )
        for i in range(start, start + count)
    )


class _FakeLister:
    def __init__(self, pages: list[tuple[tuple[SimpleNamespace, ...], str | None]], *, per_page_sleep: float = 0.0):
        self._pages = pages
        self._per_page_sleep = per_page_sleep
        self.calls: list[tuple[int, str | None]] = []

    def list_source_objects(self, service_id, domain, *, page_size, cursor, partition_prefix=""):
        del service_id, domain, partition_prefix
        if self._per_page_sleep > 0:
            time.sleep(self._per_page_sleep)
        idx = 0 if cursor is None else int(cursor)
        self.calls.append((page_size, cursor))
        objects, next_cursor = self._pages[idx]
        return SimpleNamespace(objects=objects, next_cursor=next_cursor)


class _FakeControlPlane:
    def __init__(self, *, already_known: set[str] | None = None):
        self.already_known = already_known or set()
        self.discovered_batches: list[list[str]] = []

    def initialize_owner(self, service_id, *, owner, source_cursor):
        del service_id, source_cursor
        return SimpleNamespace(current_owner=owner, owner_epoch=1)

    def discover_sources_batch(self, service_id, domain, objects, *, expected_owner, expected_owner_epoch):
        del service_id, domain, expected_owner, expected_owner_epoch
        self.discovered_batches.append([o.object_key for o in objects])
        out = {}
        for o in objects:
            status = "published" if o.object_key in self.already_known else "discovered"
            out[o.object_key] = SimpleNamespace(status=status)
        return out


def _patch_discovery(monkeypatch, lister: _FakeLister, control: _FakeControlPlane) -> None:
    src = {"name": "svc-lim", "service_id": "svc-lim", "bucket": "bkt"}
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.cron.jobs.rum_ledger._get_fos_client", lambda s: object())
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.S3SourceObjectLister", lambda *a, **kw: lister)
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.PostgresControlPlane", lambda: control)


# ── max_files & max_seconds enforcement ──────────────────────────────────────


def test_discover_prefix_enforces_max_files_across_pages(monkeypatch):
    pages = [
        (_make_objects(0, 4), "1"),
        (_make_objects(4, 4), "2"),
        (_make_objects(8, 4), None),
    ]
    lister = _FakeLister(pages)
    control = _FakeControlPlane()
    _patch_discovery(monkeypatch, lister, control)

    discovered = discover_prefix("svc-lim", originating_task="full_sync", max_files=6)

    assert discovered == 6
    assert len(lister.calls) == 2
    assert lister.calls[1][0] == 2  # remaining budget on second page
    assert sum(len(b) for b in control.discovered_batches) == 6


def test_discover_prefix_slices_page_when_lister_exceeds_remaining_max_files(monkeypatch):
    pages = [(_make_objects(0, 10), "1")]
    lister = _FakeLister(pages)
    control = _FakeControlPlane()
    _patch_discovery(monkeypatch, lister, control)

    discovered = _discover_high_scale_prefix("svc-lim", domain="request", max_files=3)

    assert discovered == 3
    assert len(control.discovered_batches) == 1
    assert len(control.discovered_batches[0]) == 3


def test_discover_prefix_zero_max_files_returns_zero_without_listing(monkeypatch):
    lister = _FakeLister([(_make_objects(0, 4), None)])
    control = _FakeControlPlane()
    _patch_discovery(monkeypatch, lister, control)

    assert discover_prefix("svc-lim", max_files=0) == 0
    assert lister.calls == []


def test_discover_prefix_enforces_max_seconds_with_partial_sweep(monkeypatch):
    pages = [
        (_make_objects(0, 3), "1"),
        (_make_objects(3, 3), "2"),
        (_make_objects(6, 3), None),
    ]
    lister = _FakeLister(pages, per_page_sleep=0.04)
    control = _FakeControlPlane()
    _patch_discovery(monkeypatch, lister, control)

    # Trap #41: first bounded page runs, then deadline check stops before page 2.
    discovered = discover_prefix("svc-lim", originating_task="full_sync", max_files=100, max_seconds=0.02)

    assert discovered == 3
    assert len(lister.calls) == 1


def test_run_full_sweep_uses_config_overrides_and_records_partial_sweep(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    src = {"name": "svc-lim", "service_id": "svc-lim", "bucket": "bkt"}
    monkeypatch.setattr("backend.cron.jobs.sync.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", lambda *_a, **_kw: False)
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {"provisioning": {"cron_full_sweep": {"max_files": 42, "max_seconds": 17}}},
    )
    monkeypatch.setattr("backend.core.fos_client.start_cron_run", lambda *_a, **_kw: 99)
    log_run = MagicMock()
    monkeypatch.setattr("backend.core.fos_client.log_cron_run", log_run)
    monkeypatch.setattr("backend.cron.scheduler.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler.start_progress", MagicMock())
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron.scheduler.end_progress", end_progress)
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    finalize_dur = MagicMock()
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)

    seen_kwargs: dict[str, object] = {}

    def _fake_discover(service_id, **kwargs):
        seen_kwargs["service_id"] = service_id
        seen_kwargs.update(kwargs)
        return 5

    monkeypatch.setattr("backend.cron.jobs.sync.discover_prefix", _fake_discover)

    _run_full_sweep.__wrapped__("svc-lim", force=True)

    assert seen_kwargs == {
        "service_id": "svc-lim",
        "run_id": 99,
        "originating_task": "full_sync",
        "max_files": 42,
        "max_seconds": 17,
    }
    log_run.assert_called_once()
    args, kwargs = log_run.call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 5
    end_progress.assert_called_once_with(99)
    finalize_dur.assert_called_once()


# ── Watchdog hard-cap timeout & resource cleanup ─────────────────────────────


def test_cron_task_watchdog_timeout_returns_partial_sweep_and_cleans_resources(monkeypatch, caplog):
    monkeypatch.setattr(sched_lifecycle, "_CRON_HARD_CAP_S", 0.03)
    monkeypatch.setattr(sched_lifecycle, "DEFAULT_INTERVAL_S", 0.01)

    flushed: list[str] = []
    released: list[bool] = []
    monkeypatch.setattr("backend.utils.telemetry.flush_usage_log", lambda sid: flushed.append(sid))
    monkeypatch.setattr("backend.core.metadata.base.release_thread_connection", lambda: released.append(True))

    @sched_lifecycle.cron_task("partial-sweep-timeout", job_name="partial_sweep")
    def _slow_partial_sweep(service_id: str) -> dict[str, int]:
        time.sleep(0.08)
        return {"discovered": 7, "partial": 1}

    caplog.set_level(logging.ERROR, logger="backend.scheduler")
    result = _slow_partial_sweep("svc-cap")

    assert result == {"discovered": 7, "partial": 1}
    assert flushed == ["svc-cap"]
    assert len(released) == 1
    assert current_attribution.get() is None
    assert any("exceeded" in rec.getMessage() and "watchdog threshold" in rec.getMessage() for rec in caplog.records)


def test_cron_task_watchdog_timeout_exception_still_cleans_resources(monkeypatch):
    monkeypatch.setattr(sched_lifecycle, "_CRON_HARD_CAP_S", 0.02)
    monkeypatch.setattr(sched_lifecycle, "DEFAULT_INTERVAL_S", 0.01)

    flushed: list[str] = []
    released: list[bool] = []
    monkeypatch.setattr("backend.utils.telemetry.flush_usage_log", lambda sid: flushed.append(sid))
    monkeypatch.setattr("backend.core.metadata.base.release_thread_connection", lambda: released.append(True))

    @sched_lifecycle.cron_task("timeout-error", job_name="timeout_error")
    def _slow_error(service_id: str) -> None:
        time.sleep(0.06)
        raise RuntimeError("sweep failed after cap")

    with pytest.raises(RuntimeError, match="sweep failed after cap"):
        _slow_error("svc-err")

    assert flushed == ["svc-err"]
    assert len(released) == 1
    assert current_attribution.get() is None
