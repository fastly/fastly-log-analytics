"""HyperLogLog cardinality estimation sketches for rollup aggregation."""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable
from typing import Any

DEFAULT_PRECISION = 8
SERIALIZATION_VERSION = 1
_HASH_BITS = 64
HLL_SKETCH_NAMES: tuple[str, ...] = ("hll_sketch",)


def _alpha(m: int) -> float:
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1.0 + 1.079 / m)


def _hash64(item: str | bytes) -> int:
    if isinstance(item, str):
        item = item.encode("utf-8")
    digest = hashlib.md5(item, usedforsecurity=False).digest()  # noqa: S324
    return struct.unpack("<Q", digest[:8])[0]


def _leading_zero_count(value: int, width_bits: int) -> int:
    if value == 0:
        return width_bits
    rank = 1
    msb = 1 << (width_bits - 1)
    while (value & msb) == 0 and rank < width_bits:
        rank += 1
        value <<= 1
    return rank


class HyperLogLog:
    """Compact HyperLogLog sketch supporting binary serialization and associative merge."""

    __slots__ = ("_p", "_m", "_buckets")

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        if not 4 <= precision <= 16:
            raise ValueError(f"precision must be in [4, 16], got {precision}")
        self._p = precision
        self._m = 1 << precision
        self._buckets = bytearray(self._m)

    @property
    def precision(self) -> int:
        return self._p

    @property
    def m(self) -> int:
        return self._m

    def add(self, item: str | bytes) -> None:
        h = _hash64(item)
        bucket = h >> (_HASH_BITS - self._p)
        observed = h & ((1 << (_HASH_BITS - self._p)) - 1)
        rank = _leading_zero_count(observed, _HASH_BITS - self._p)
        if rank > self._buckets[bucket]:
            self._buckets[bucket] = rank

    def update(self, items: Iterable[str | bytes]) -> None:
        for item in items:
            self.add(item)

    def count(self) -> int:
        m = self._m
        raw_sum = sum(2.0**-b for b in self._buckets)
        if raw_sum == 0.0:
            return m
        estimate = _alpha(m) * m * m / raw_sum
        if estimate <= 2.5 * m:
            zeros = sum(1 for b in self._buckets if b == 0)
            if zeros > 0:
                estimate = m * math.log(m / zeros)
        if estimate > (1 << 64) / 30.0:
            estimate = -(1 << 64) * math.log(1 - estimate / (1 << 64))
        return int(round(estimate))

    def merge(self, other: HyperLogLog) -> None:
        if other._p != self._p:
            raise ValueError(f"merge precision mismatch: self.p={self._p} vs other.p={other._p}")
        for i in range(self._m):
            if other._buckets[i] > self._buckets[i]:
                self._buckets[i] = other._buckets[i]

    def to_bytes(self) -> bytes:
        return bytes([SERIALIZATION_VERSION, self._p]) + bytes(self._buckets)

    @classmethod
    def from_bytes(cls, blob: bytes | bytearray) -> HyperLogLog:
        if len(blob) < 2:
            raise ValueError(f"sketch blob too short: {len(blob)} bytes")
        version, precision = blob[0], blob[1]
        if version != SERIALIZATION_VERSION:
            raise ValueError(f"unknown HLL serialization version {version}; expected {SERIALIZATION_VERSION}")
        if not 4 <= precision <= 16:
            raise ValueError(f"invalid precision in blob: {precision}")
        expected_len = 2 + (1 << precision)
        if len(blob) != expected_len:
            raise ValueError(f"sketch blob length mismatch: got {len(blob)}, expected {expected_len}")
        sketch = cls(precision=precision)
        sketch._buckets = bytearray(blob[2:])
        return sketch

    def __len__(self) -> int:
        return self.count()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic only
        return f"HyperLogLog(p={self._p}, estimate≈{self.count()})"


def get_hll_sketch(precision: int = DEFAULT_PRECISION) -> HyperLogLog:
    return HyperLogLog(precision=precision)


def merge_hll_sketches(blobs: Iterable[bytes | bytearray], precision: int = DEFAULT_PRECISION) -> int:
    acc: HyperLogLog | None = None
    for blob in blobs:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if acc is None:
            acc = sketch
        else:
            acc.merge(sketch)
    return acc.count() if acc is not None else 0


query_hll_distinct = merge_hll_sketches
query_hll_distinct_filtered = merge_hll_sketches


def query_hll_distinct_by_bucket(rows: Iterable[tuple[Any, bytes | bytearray]]) -> dict[Any, int]:
    buckets: dict[Any, HyperLogLog] = {}
    for key, blob in rows:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if key not in buckets:
            buckets[key] = sketch
        else:
            buckets[key].merge(sketch)
    return {k: v.count() for k, v in buckets.items()}


__all__ = [
    "DEFAULT_PRECISION",
    "HLL_SKETCH_NAMES",
    "HyperLogLog",
    "SERIALIZATION_VERSION",
    "_HASH_BITS",
    "_alpha",
    "_hash64",
    "_leading_zero_count",
    "get_hll_sketch",
    "merge_hll_sketches",
    "query_hll_distinct",
    "query_hll_distinct_by_bucket",
    "query_hll_distinct_filtered",
]
