"""Bounded-memory Top-K counting for high-cardinality aggregate dimensions.

Space-Saving (Metwally et al., 2005): tracks at most ``capacity`` distinct
keys. A new key past capacity evicts the current minimum-count entry and
inherits its count, so the estimate for any tracked key is never below its
true count, and the overestimate for any key is bounded by the evicted
entry's count at the time of eviction (``max_error``). Unlike an unbounded
``collections.Counter``, memory is capped regardless of the number of
distinct values seen — required for dimensions like URL or client IP at
high-scale volumes, where an exact per-value count would grow without bound.
"""

from __future__ import annotations


class BoundedHeavyHitters:
    def __init__(self, *, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._counts: dict[str, int] = {}
        self._errors: dict[str, int] = {}
        self._distinct_seen = 0

    def add(self, key: str) -> None:
        if key in self._counts:
            self._counts[key] += 1
            return
        self._distinct_seen += 1
        if len(self._counts) < self._capacity:
            self._counts[key] = 1
            self._errors[key] = 0
            return
        evict_key = min(self._counts, key=lambda k: self._counts[k])
        evict_count = self._counts.pop(evict_key)
        self._errors.pop(evict_key)
        self._counts[key] = evict_count + 1
        self._errors[key] = evict_count

    @property
    def is_exact(self) -> bool:
        return self._distinct_seen <= self._capacity

    def size(self) -> int:
        return len(self._counts)

    def most_common(self, n: int) -> list[tuple[str, int, int]]:
        ranked = sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
        return [(key, count, self._errors[key]) for key, count in ranked[:n]]
