import contextvars
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.core.high_scale_sections import HighScaleSections, current_clickhouse_scope
from backend.core.query_attribution import Attribution, current_attribution


def test_sections_propagate_service_and_query_ownership_to_workers() -> None:
    trace = HighScaleSections("service-test", "security")
    token = current_attribution.set(Attribution(kind="admin", request_id="request-test"))
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(contextvars.copy_context().run, trace.run, name, current_clickhouse_scope, source="fact")
                for name in ("tls", "bots")
            ]
            rows = [future.result() for future in futures]
    finally:
        current_attribution.reset(token)
    assert {row["service_id"] for row in rows} == {"service-test"}
    assert {row["request_id"] for row in rows} == {"request-test"}
    assert {row["section"] for row in rows} == {"security.tls", "security.bots"}
    assert len({row["section_id"] for row in rows}) == 2
    assert len(trace.timings()) == 2
    assert all(row["source"] == "fact" and row["current_execution"] is True for row in trace.timings())
    assert current_clickhouse_scope()["service_id"] is None


def test_named_task_queue_wait_is_separate_from_section_execution(monkeypatch) -> None:
    import threading

    clock = [0.0]
    monkeypatch.setattr("backend.core.high_scale_sections.time.perf_counter", lambda: clock[0])
    trace = HighScaleSections("service-test", "network")
    ready, release = threading.Event(), threading.Event()

    def occupy():
        ready.set()
        assert release.wait(3)

    def execute():
        clock[0] += 0.05
        return "result"

    with ThreadPoolExecutor(max_workers=1) as pool:
        blocker = pool.submit(occupy)
        assert ready.wait(3)
        future = trace.submit(pool, "heatmap", execute, source="fact")
        clock[0] = 0.02
        release.set()
        assert future.result(timeout=3) == "result"
        blocker.result(timeout=3)
    observation = trace.timings()[0]
    assert observation["executor_dispatch"] == "thread_pool"
    assert observation["executor_wait_ms"] == pytest.approx(20)
    assert observation["time_ms"] == pytest.approx(50)


def test_persistence_never_falls_back_to_another_active_service(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from backend import config
    from backend.core.metadata import usage_log
    from backend.utils import telemetry

    active = MagicMock(return_value="wrong-active-service")
    persist = MagicMock()
    monkeypatch.setattr(config, "get_active_service_id", active)
    monkeypatch.setattr(usage_log, "log_telemetry_query", persist)
    token = telemetry._PAGE_LOAD_ID.set("page-test")
    try:
        telemetry.record_telemetry_query("clickhouse", "SELECT 1", 2, allow_active_service_fallback=False)
        assert active.call_count == persist.call_count == 0
        telemetry.record_telemetry_query(
            "clickhouse",
            "SELECT 1",
            2,
            service_id="service-test",
            request_id="request-test",
            allow_active_service_fallback=False,
        )
    finally:
        telemetry._PAGE_LOAD_ID.reset(token)
    assert active.call_count == 0
    assert persist.call_args.kwargs["service_id"] == "service-test"
    assert persist.call_args.kwargs["request_id"] == "request-test"


def test_nested_sections_reset_after_errors_and_remain_separate() -> None:
    parent = HighScaleSections("service-parent", "network")
    child = HighScaleSections("service-child", "cmcd")
    scopes = []

    def fails():
        scopes.append(current_clickhouse_scope())
        raise ValueError("private-value")

    def outer():
        scopes.append(current_clickhouse_scope())
        with pytest.raises(ValueError, match="private-value"):
            child.run("overview", fails, source="projected_fact")
        scopes.append(current_clickhouse_scope())

    parent.run("health", outer, source="fact")
    assert [row["service_id"] for row in scopes] == ["service-parent", "service-child", "service-parent"]
    assert scopes[1]["parent_section_id"] == scopes[0]["section_id"]
    assert child.timings()[0]["outcome"] == "error"
    assert "private-value" not in str(child.timings())
    assert current_clickhouse_scope()["section_id"] is None
