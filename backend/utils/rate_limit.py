"""Sliding-window rate limiter and static asset rate limiting helpers."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable

import cachetools


class SlidingWindowLimiter:
    def __init__(self, *, window_s: float, max_keys: int, clock: Callable[[], float] | None = None) -> None:
        self.window_s = window_s
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._events: cachetools.LRUCache[str, deque[tuple[float, int]]] = cachetools.LRUCache(maxsize=max(1, max_keys))
        self._locks: cachetools.LRUCache[str, float] = cachetools.LRUCache(maxsize=max(1, max_keys))

    @property
    def MAX_TRACKED_IPS(self) -> int:
        return self.max_keys

    @MAX_TRACKED_IPS.setter
    def MAX_TRACKED_IPS(self, value: int) -> None:
        self.max_keys = value
        self._events._Cache__maxsize = max(1, value)
        self._locks._Cache__maxsize = max(1, value)

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.time()

    def _prune_key(self, key: str, cutoff: float) -> deque[tuple[float, int]]:
        events = self._events.get(key)
        if events is None:
            return deque()
        while events and events[0][0] < cutoff:
            events.popleft()
        return events

    def _prune_now(self, key: str) -> tuple[float, float, deque[tuple[float, int]]]:
        now = self._now()
        cutoff = now - self.window_s
        return now, cutoff, self._prune_key(key, cutoff)

    def _store(self, key: str, events: deque[tuple[float, int]], cutoff: float) -> None:
        self._events._Cache__maxsize = max(1, self.max_keys, len(self._events) + 1)
        self._events.pop(key, None)
        self._events[key] = events
        while self._events:
            oldest, history = next(iter(self._events._Cache__data.items()))
            if history and history[-1][0] >= cutoff:
                break
            del self._events[oldest]
        while len(self._events) > self.max_keys:
            self._events.popitem()
        self._events._Cache__maxsize = max(1, self.max_keys)

    def try_acquire(
        self, key: str, weight: int = 0, *, max_events: int | None = None, max_weight: int | None = None
    ) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, cutoff)
                return False
            events.append((now, max(0, int(weight))))
            self._store(key, events, cutoff)
            return max_weight is None or sum(w for _, w in events) <= max_weight

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            events.append((now, 1))
            self._store(key, events, cutoff)
            if len(events) >= threshold and key not in self._locks:
                cap = max_locks if max_locks is not None else self.max_keys
                self._locks._Cache__maxsize = max(1, cap, len(self._locks) + 1)
                self._locks[key] = now + lockout_s
                while len(self._locks) > cap:
                    self._locks.popitem()
                self._locks._Cache__maxsize = max(1, cap)
                return True
            return False

    def is_locked(self, key: str) -> tuple[bool, int]:
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


SlidingWindowRateLimiter = SlidingWindowLimiter


class _StaticAssetLimiter(SlidingWindowLimiter):
    def __init__(
        self,
        *,
        req_limit: int = 600,
        byte_limit: int = 50 * 1024 * 1024,
        window_s: int = 60,
        max_tracked_ips: int = 10_000,
    ) -> None:
        super().__init__(window_s=window_s, max_keys=max_tracked_ips)
        self.REQ_LIMIT = req_limit
        self.BYTE_LIMIT = byte_limit

    def check(self, ip: str, content_length: int) -> bool:
        return self.try_acquire(ip, content_length, max_events=self.REQ_LIMIT, max_weight=self.BYTE_LIMIT)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._locks.clear()
