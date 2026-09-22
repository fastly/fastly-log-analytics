from __future__ import annotations

from dataclasses import dataclass

from backend.high_scale.orchestration import PageRun
from backend.high_scale.worker import HighScaleWorkerLoop


@dataclass
class _Coordinator:
    calls: list[tuple[str, str]]

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def test_worker_loop_processes_each_configured_service_domain_once() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc-a", "svc-b"),
        domains=("request", "rum"),
        interval_seconds=0,
    )

    results = loop.run_once()

    assert coordinator.calls == [
        ("svc-a", "request"),
        ("svc-a", "rum"),
        ("svc-b", "request"),
        ("svc-b", "rum"),
    ]
    assert [(result.service_id, result.domain, result.page.processed) for result in results] == [
        ("svc-a", "request", 1),
        ("svc-a", "rum", 1),
        ("svc-b", "request", 1),
        ("svc-b", "rum", 1),
    ]


def test_worker_loop_stops_after_requested_iterations() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=0,
    )

    loop.run(max_iterations=2)

    assert coordinator.calls == [("svc", "request"), ("svc", "request")]
