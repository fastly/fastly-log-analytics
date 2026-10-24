"""Single in-process sliding-window limiter (no external dependency).

Backs both the per-IP request/byte quotas in ``remote_access`` and the
share-login failure lockout in ``utils.tunnel``. Single-process and in-memory by
design (serving tier is one pod, ADR-18).

Memory is bounded: keys are kept in least-recently-touched order, entries whose
newest event has left the window are dropped from the front on every mutation,
and the key count never exceeds ``max_keys`` (oldest-touched key is evicted
first). A cardinality flood therefore cannot grow the maps, and — unlike a
clear-everything reset — cannot be used to wipe other clients' counters in one
shot.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable


class SlidingWindowLimiter:
    def __init__(
        self,
        *,
        window_s: float,
        max_keys: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.window_s = window_s
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        # key -> deque[(timestamp, weight)], ordered least- to most-recently touched
        self._events: OrderedDict[str, deque[tuple[float, int]]] = OrderedDict()
        # key -> lockout expiry, insertion-ordered
        self._locks: OrderedDict[str, float] = OrderedDict()

    @property
    def WINDOW_S(self) -> int:
        return int(self.window_s)

    @property
    def MAX_TRACKED_IPS(self) -> int:
        return self.max_keys

    @MAX_TRACKED_IPS.setter
    def MAX_TRACKED_IPS(self, value: int) -> None:
        self.max_keys = value

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.time()

    def _prune_key(self, key: str, cutoff: float) -> deque[tuple[float, int]]:
        events = self._events.get(key)
        if events is None:
            return deque()
        while events and events[0][0] < cutoff:
            events.popleft()
        return events

    def _store(self, key: str, events: deque[tuple[float, int]], cutoff: float) -> None:
        self._events[key] = events
        self._events.move_to_end(key)
        while self._events:
            oldest, history = next(iter(self._events.items()))
            if history and history[-1][0] >= cutoff:
                break
            del self._events[oldest]
        while len(self._events) > self.max_keys:
            self._events.popitem(last=False)

    def try_acquire(
        self,
        key: str,
        weight: int = 0,
        *,
        max_events: int | None = None,
        max_weight: int | None = None,
    ) -> bool:
        """Admit one event if the key is under ``max_events`` in the window.

        The event (and its ``weight``) is recorded whenever the event cap is not
        hit — including when it pushes the weighted total over ``max_weight``,
        in which case the call is refused but the weight still counts.
        """
        with self._lock:
            now = self._now()
            cutoff = now - self.window_s
            events = self._prune_key(key, cutoff)
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, cutoff)
                return False
            events.append((now, max(0, int(weight))))
            self._store(key, events, cutoff)
            return max_weight is None or sum(w for _, w in events) <= max_weight

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        """Record one failure; return True iff this call starts a NEW lockout."""
        with self._lock:
            now = self._now()
            cutoff = now - self.window_s
            events = self._prune_key(key, cutoff)
            events.append((now, 1))
            self._store(key, events, cutoff)
            if len(events) >= threshold and key not in self._locks:
                self._locks[key] = now + lockout_s
                while len(self._locks) > (max_locks if max_locks is not None else self.max_keys):
                    self._locks.popitem(last=False)
                return True
            return False

    def is_locked(self, key: str) -> tuple[bool, int]:
        """``(locked, remaining_seconds)``; an expired lockout is dropped."""
        with self._lock:
            until = self._locks.get(key)
            if until is None:
                return False, 0
            now = self._now()
            if now >= until:
                del self._locks[key]
                return False, 0
            return True, int(until - now)

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        """``(live_event_counts, remaining_lockout_seconds)`` with stale state pruned."""
        with self._lock:
            now = self._now()
            cutoff = now - self.window_s
            counts: dict[str, int] = {}
            for key in list(self._events):
                events = self._prune_key(key, cutoff)
                if events:
                    counts[key] = len(events)
                else:
                    del self._events[key]
            locks: dict[str, int] = {}
            for key, until in list(self._locks.items()):
                if until <= now:
                    del self._locks[key]
                else:
                    locks[key] = int(until - now)
            return counts, locks

    def tracked_keys(self) -> int:
        with self._lock:
            return len(self._events)
