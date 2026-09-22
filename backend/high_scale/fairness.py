"""Deterministic weighted fairness for shared high-scale worker capacity."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WorkItem:
    service_id: str
    cost: int
    payload: Any

    def __post_init__(self) -> None:
        if not self.service_id:
            raise ValueError("service_id is required")
        if self.cost <= 0:
            raise ValueError("work item cost must be positive")


class FairScheduler:
    def __init__(self) -> None:
        self._queues: dict[str, deque[WorkItem]] = defaultdict(deque)
        self._weights: dict[str, int] = {}

    def register(self, service_id: str, *, weight: int = 1) -> None:
        if not service_id:
            raise ValueError("service_id is required")
        if weight <= 0:
            raise ValueError("service weight must be positive")
        self._weights[service_id] = weight
        self._queues.setdefault(service_id, deque())

    def enqueue(self, item: WorkItem) -> None:
        if item.service_id not in self._weights:
            self.register(item.service_id)
        self._queues[item.service_id].append(item)

    def dispatch(self, capacity: int) -> tuple[WorkItem, ...]:
        if capacity < 0:
            raise ValueError("capacity must be non-negative")
        remaining = capacity
        dispatched: list[WorkItem] = []
        active = [service_id for service_id in sorted(self._queues) if self._queues[service_id]]

        for service_id in active:
            item = self._queues[service_id][0]
            if item.cost <= remaining:
                dispatched.append(self._queues[service_id].popleft())
                remaining -= item.cost

        while remaining:
            made_progress = False
            for service_id in active:
                queue = self._queues[service_id]
                for _ in range(self._weights[service_id]):
                    if not queue or queue[0].cost > remaining:
                        break
                    dispatched.append(queue.popleft())
                    remaining -= dispatched[-1].cost
                    made_progress = True
                    if not remaining:
                        break
                if not remaining:
                    break
            if not made_progress:
                break
        return tuple(dispatched)
