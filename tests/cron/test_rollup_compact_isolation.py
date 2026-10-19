"""rollup_compact_daily isolates the 13 per-rollup compactors: one failure warns, the rest still run."""

from __future__ import annotations

import pytest

COMPACTORS = [
    ("origin_summary", "compact_origin_summary_closed_days_to_daily"),
    ("network_rtt", "compact_network_rtt_closed_days_to_daily"),
    ("network_speed", "compact_network_speed_closed_days_to_daily"),
    ("network_quality", "compact_network_quality_closed_days_to_daily"),
    ("verified_bots_ts", "compact_verified_bots_ts_closed_days_to_daily"),
    ("perf_latency", "compact_perf_latency_closed_days_to_daily"),
    ("origin_dims", "compact_origin_dims_closed_days_to_daily"),
    ("origin_latency_ts", "compact_origin_latency_ts_closed_days_to_daily"),
    ("security_dims", "compact_security_dims_closed_days_to_daily"),
    ("perf_dims", "compact_perf_dims_closed_days_to_daily"),
    ("ngwaf_bots", "compact_ngwaf_bots_closed_days_to_daily"),
    ("overview", "compact_overview_closed_days_to_daily"),
    ("pop_health", "compact_pop_health_closed_days_to_daily"),
]


@pytest.fixture
def run_job(monkeypatch):
    calls: list[str] = []
    log_calls: list[dict] = []

    def make(name, fail_for):
        def _fn(sid, src, lookback_days=30):
            calls.append(name)
            if name == fail_for:
                raise RuntimeError("boom")
            return 0

        return _fn

    def run(fail_for: str | None):
        calls.clear()
        log_calls.clear()
        for label, attr in COMPACTORS:
            monkeypatch.setattr(f"backend.core.rollups.{attr}", make(label, fail_for))
        monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda sid, src, lookback_days=30: 3)
        monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **kw: 0)
        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
        )
        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
        )
        monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **kw: (0, 0))
        monkeypatch.setattr("backend.core.rollups.recompute.cleanup_old_rollups", lambda *a, **kw: None)
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1)
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append(kw))
        from backend.cron.jobs.rollup_compact import _run_rollup_compact_daily

        return _run_rollup_compact_daily.__wrapped__("svc-iso", manual=True), list(calls), list(log_calls)

    return run


def test_all_compactors_run_in_order_and_job_succeeds(run_job):
    result, calls, _ = run_job(None)
    assert calls == [label for label, _ in COMPACTORS]
    assert result["status"] == "success"
    assert result["errors"] is None


@pytest.mark.parametrize("failing", [label for label, _ in COMPACTORS])
def test_one_failing_compactor_warns_and_others_still_run(run_job, failing):
    result, calls, log_calls = run_job(failing)
    assert calls == [label for label, _ in COMPACTORS]
    assert result["status"] == "warning"
    assert result["errors"] == [f"{failing}: boom"]
    assert log_calls[-1]["error_message"] == f"{failing}: boom"
