from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import Event
from time import monotonic, sleep

import pytest

from backend.high_scale.historical_jobs import (
    HistoricalJob,
    HistoricalJobManager,
    HistoricalJobState,
)
from backend.high_scale.query_router import (
    QueryIntent,
    QueryPlan,
    QueryTier,
)

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def cold_plan(*, max_rows: int = 3) -> QueryPlan:
    return QueryPlan(
        service_id="svc",
        domain="request",
        intent=QueryIntent.RAW,
        tier=QueryTier.COLD_JOB,
        start=NOW - timedelta(days=2),
        end=NOW - timedelta(days=1),
        sources=("queued_query_job",),
        queued=True,
        bounded=True,
        max_rows=max_rows,
        reason="test",
    )


def wait_for(manager: HistoricalJobManager, job_id: str, state: HistoricalJobState) -> HistoricalJob:
    deadline = monotonic() + 2
    while monotonic() < deadline:
        job = manager.get(job_id)
        if job is not None and job.state is state:
            return job
        sleep(0.005)
    raise AssertionError(f"job did not reach {state}")


def test_job_completes_with_a_row_and_byte_bound() -> None:
    manager = HistoricalJobManager(max_output_bytes=100, terminal_grace_ttl=timedelta(seconds=60))

    job_id = manager.submit(cold_plan(), lambda: [{"id": 1}, {"id": 2}])
    job = wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    assert job.rows == ({"id": 1}, {"id": 2})
    assert job.row_count == 2
    assert job.output_bytes <= 100

    manager.shutdown()


def test_iteration_stops_at_max_rows_without_retaining_extra_rows() -> None:
    manager = HistoricalJobManager(max_output_bytes=10_000)
    consumed: list[int] = []

    def rows() -> list[dict[str, int]]:
        return [{"id": index} for index in range(10)]

    def bounded_rows():
        for row in rows():
            consumed.append(row["id"])
            yield row

    job_id = manager.submit(cold_plan(max_rows=2), bounded_rows)
    job = wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    assert job.rows == ({"id": 0}, {"id": 1})
    assert job.row_count == 2
    assert len(consumed) <= 3
    manager.shutdown()


def test_cancellation_is_observed_during_iteration() -> None:
    manager = HistoricalJobManager(max_output_bytes=10_000)
    started = Event()
    release = Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows)
    assert started.wait(1)
    assert manager.cancel(job_id) is True
    release.set()
    job = wait_for(manager, job_id, HistoricalJobState.CANCELLED)

    assert job.rows == ({"id": 1},)
    manager.shutdown()


def test_expiry_is_observed_during_iteration() -> None:
    clock = [NOW]
    manager = HistoricalJobManager(ttl=timedelta(seconds=1), max_output_bytes=10_000, clock=lambda: clock[0])
    started = Event()
    release = Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows, now=clock[0])
    assert started.wait(1)
    clock[0] += timedelta(seconds=2)
    manager.expire(now=clock[0])
    release.set()
    job = wait_for(manager, job_id, HistoricalJobState.EXPIRED)

    assert job.rows == ({"id": 1},)
    manager.shutdown()


def test_output_byte_limit_fails_and_drops_payload() -> None:
    manager = HistoricalJobManager(max_output_bytes=20)
    job_id = manager.submit(cold_plan(max_rows=10), lambda: [{"value": "x" * 100}])

    job = wait_for(manager, job_id, HistoricalJobState.FAILED)

    assert job.rows == ()
    assert job.error == "historical job output exceeds byte limit"
    manager.shutdown()


def test_terminal_jobs_are_purged_after_grace_ttl() -> None:
    manager = HistoricalJobManager(terminal_grace_ttl=timedelta(seconds=1))
    job_id = manager.submit(cold_plan(), lambda: [])
    wait_for(manager, job_id, HistoricalJobState.COMPLETED)

    completed = manager.get(job_id)
    assert completed is not None and completed.terminal_at is not None
    manager.purge(now=completed.terminal_at + timedelta(seconds=2))

    assert manager.get(job_id) is None
    manager.shutdown()


@pytest.mark.parametrize(
    "plan",
    [
        QueryPlan(
            service_id="svc",
            domain="request",
            intent=QueryIntent.RAW,
            tier=QueryTier.RECENT_FACTS,
            start=NOW - timedelta(days=2),
            end=NOW - timedelta(days=1),
            sources=("clickhouse_facts",),
            queued=False,
            bounded=False,
            max_rows=1,
            reason="not cold",
        )
    ],
)
def test_only_bounded_cold_plans_can_be_submitted(plan: QueryPlan) -> None:
    manager = HistoricalJobManager()

    with pytest.raises(ValueError, match="cold"):
        manager.submit(plan, lambda: [])

    manager.shutdown()
