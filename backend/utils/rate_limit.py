"""Sliding-window rate limiter and static asset rate limiting helpers."""

from __future__ import annotations

import ipaddress
import math
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable

import cachetools


def normalize_rate_limit_ip(ip_str: str) -> str:
    """Canonicalize client IP for rate-limiting, collapsing IPv6 addresses to /64 prefixes."""
    cleaned = (ip_str or "").strip()
    if not cleaned:
        return "unknown"
    try:
        ip = ipaddress.ip_address(cleaned)
    except ValueError:
        return cleaned
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            return str(ip.ipv4_mapped)
        net = ipaddress.IPv6Network((ip, 64), strict=False)
        return f"{net.network_address}/64"
    return str(ip)


class SlidingWindowLimiter:
    def __init__(self, *, window_s: float, max_keys: int, clock: Callable[[], float] | None = None) -> None:
        self.window_s = window_s
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._events: cachetools.LRUCache[str, deque[tuple[float, int]]] = cachetools.LRUCache(maxsize=max(1, max_keys))
        self._locks: cachetools.LRUCache[str, float] = cachetools.LRUCache(maxsize=max(1, max_keys))
        self._saturated: set[str] = set()
        self._single_hit_keys: OrderedDict[str, None] = OrderedDict()
        self._overflow_locked_until: float = 0.0

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

    def _prune_expired_locks(self, now: float) -> None:
        for k, until in list(self._locks.items()):
            if until <= now:
                del self._locks[k]
        if self._overflow_locked_until <= now:
            self._overflow_locked_until = 0.0

    def _prune_key(self, key: str, cutoff: float) -> deque[tuple[float, int]]:
        events = self._events.get(key)
        if events is None:
            return deque()
        while events and events[0][0] < cutoff:
            events.popleft()
        if not events:
            self._saturated.discard(key)
            self._single_hit_keys.pop(key, None)
        return events

    def _prune_now(self, key: str) -> tuple[float, float, deque[tuple[float, int]]]:
        now = self._now()
        cutoff = now - self.window_s
        return now, cutoff, self._prune_key(key, cutoff)

    def _evict_or_make_room(self, incoming_key: str, now: float, cutoff: float) -> bool:
        """Ensure room for ``incoming_key`` without EVER evicting active multi-hit/saturated/locked keys."""
        cap = max(1, self.max_keys)
        if incoming_key in self._events:
            return True
        while self._events:
            oldest, history = next(iter(self._events._Cache__data.items()))
            if history and history[-1][0] >= cutoff:
                break
            del self._events[oldest]
            self._saturated.discard(oldest)
            self._single_hit_keys.pop(oldest, None)
        if len(self._events) < cap:
            return True
        while self._single_hit_keys:
            cand, _ = self._single_hit_keys.popitem(last=False)
            hist = self._events._Cache__data.get(cand)
            if hist is None:
                continue
            lock_until = self._locks._Cache__data.get(cand)
            if lock_until is not None and lock_until > now:
                continue
            if cand in self._saturated or (len(hist) > 1 and hist[-1][0] >= cutoff):
                continue
            del self._events[cand]
            self._saturated.discard(cand)
            if len(self._events) < cap:
                return True
        for k, hist in list(self._events._Cache__data.items()):
            while hist and hist[0][0] < cutoff:
                hist.popleft()
            if not hist:
                del self._events[k]
                self._saturated.discard(k)
                self._single_hit_keys.pop(k, None)
            elif len(hist) == 1 and k not in self._saturated:
                lock_until = self._locks._Cache__data.get(k)
                if lock_until is None or lock_until <= now:
                    self._single_hit_keys[k] = None
        while len(self._events) >= cap and self._single_hit_keys:
            cand, _ = self._single_hit_keys.popitem(last=False)
            if cand in self._events._Cache__data:
                del self._events[cand]
                self._saturated.discard(cand)
        return len(self._events) < cap

    def _store(self, key: str, events: deque[tuple[float, int]], now: float, *, saturated: bool = False) -> None:
        cap = max(1, self.max_keys)
        self._events._Cache__maxsize = max(1, cap, len(self._events) + 1)
        self._events.pop(key, None)
        self._events[key] = events
        if saturated:
            self._saturated.add(key)
        lock_until = self._locks._Cache__data.get(key)
        is_locked_now = lock_until is not None and lock_until > now
        if len(events) == 1 and not saturated and not is_locked_now:
            self._single_hit_keys[key] = None
            self._single_hit_keys.move_to_end(key)
        else:
            self._single_hit_keys.pop(key, None)
        self._events._Cache__maxsize = cap

    def _overflow_retry_after(self, now: float) -> int:
        earliest_expiry: float | None = None
        for hist in self._events._Cache__data.values():
            if hist:
                exp = hist[0][0] + self.window_s
                if earliest_expiry is None or exp < earliest_expiry:
                    earliest_expiry = exp
        if earliest_expiry is None:
            return max(1, math.ceil(self.window_s))
        return max(1, math.ceil(earliest_expiry - now))

    def try_acquire(
        self, key: str, weight: int = 0, *, max_events: int | None = None, max_weight: int | None = None
    ) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if not self._evict_or_make_room(key, now, cutoff):
                return False
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, now, saturated=True)
                return False
            events.append((now, max(0, int(weight))))
            total_weight = sum(w for _, w in events) if max_weight is not None else 0
            over_weight = max_weight is not None and total_weight > max_weight
            reached_cap = (max_events is not None and len(events) >= max_events) or (
                max_weight is not None and total_weight >= max_weight
            )
            self._store(key, events, now, saturated=reached_cap)
            return not over_weight

    def try_acquire_with_retry(self, key: str, *, max_events: int) -> tuple[bool, int]:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if not self._evict_or_make_room(key, now, cutoff):
                return False, self._overflow_retry_after(now)
            if len(events) >= max_events:
                retry_after = max(1, math.ceil(events[0][0] + self.window_s - now)) if events else 1
                self._store(key, events, now, saturated=True)
                return False, retry_after
            events.append((now, 1))
            self._store(key, events, now, saturated=(len(events) >= max_events))
            return True, 0

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            self._prune_expired_locks(now)
            cap = max(1, max_locks if max_locks is not None else self.max_keys)
            if not self._evict_or_make_room(key, now, cutoff):
                self._overflow_locked_until = max(self._overflow_locked_until, now + lockout_s)
                return True
            events.append((now, 1))
            reached = len(events) >= threshold
            self._store(key, events, now, saturated=reached)
            if reached and key not in self._locks:
                self._single_hit_keys.pop(key, None)
                if len(self._locks) >= cap:
                    self._overflow_locked_until = max(self._overflow_locked_until, now + lockout_s)
                    return True
                self._locks._Cache__maxsize = max(1, cap, len(self._locks) + 1)
                self._locks[key] = now + lockout_s
                self._locks._Cache__maxsize = cap
                return True
            return False

    def is_locked(self, key: str) -> tuple[bool, int]:
        with self._lock:
            now = self._now()
            until = self._locks.get(key)
            if until is not None and until <= now:
                del self._locks[key]
                until = None
            if self._overflow_locked_until <= now:
                self._overflow_locked_until = 0.0
            eff_until = max(until or 0.0, self._overflow_locked_until)
            if eff_until <= now:
                return False, 0
            return True, max(1, int(eff_until - now))

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
                    self._saturated.discard(key)
                    self._single_hit_keys.pop(key, None)
            locks: dict[str, int] = {}
            for key, until in list(self._locks.items()):
                if until <= now:
                    del self._locks[key]
                else:
                    locks[key] = int(until - now)
            return counts, locks

    def _reset_for_tests(self) -> None:
        with self._lock:
            self._events.clear()
            self._locks.clear()
            self._saturated.clear()
            self._single_hit_keys.clear()
            self._overflow_locked_until = 0.0


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
        self._reset_for_tests()
