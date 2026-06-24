"""Minimal HyperLogLog for cross-hour distinct-IP counts in the rollup tree.

Implements Flajolet et al. 2007 with the standard small/large-range
corrections. Sketches serialize to compact ``bytes`` (~257 bytes at the
default precision ``p=8``) and merge associatively via per-bucket
maximums — the property the rollup reader uses to combine N per-hour
sketches into one window-distinct estimate without pulling raw IPs.

Why a hand-rolled HLL instead of a library? Two reasons:

  1. **Stable serialization.** The on-disk bytes layout is pinned here
     (``[version_byte=1][p_byte][m bytes...]``) so a future library
     upgrade can't silently change the parquet schema's BLOB column
     interpretation. Old rollup files keep working without a migration.
  2. **No supply-chain dep for a 200-line algorithm.** The Flajolet
     paper is well-trod; the property-based tests in
     ``tests/utils/test_hll.py`` validate cardinality estimates,
     merge correctness, and round-trip serialization across a wide
     range of inputs.

Hash choice: ``hashlib.md5`` truncated to 64 bits. MD5 is broken for
cryptography but perfectly fine as a non-cryptographic hash — and
critically, it's STABLE across Python processes (Python's built-in
``hash()`` is randomised per-interpreter, which would make HLL merge
results depend on which process built each sketch). The 64 bits are
plenty: at ``p=8`` we use 8 for the bucket index and the leading-zero
count over the remaining 56 bits caps at 56 (fits in 1 byte per
bucket). The collision-induced large-range correction kicks in only
above ~143 M distinct values, well above any plausible per-(field, value)
count we'd ever build.

Error bound at ``p=8`` (the only precision the rollup writer uses):
~1.04/sqrt(256) ≈ 6.5 % standard error. That's well below the
fingerprint cards' display precision (10s / 100s of IPs) and lets a
single sketch fit in ~257 bytes regardless of input cardinality.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable

# ── Constants ────────────────────────────────────────────────────────────────
# Pinned at p=8 → m=256 buckets. Larger p improves accuracy but linearly
# inflates sketch size + parquet bundle bytes. The fingerprint cards
# don't render to per-IP precision so the 6.5% error bound is plenty.
DEFAULT_PRECISION = 8

# On-disk version tag. Bump when the bytes layout changes; the
# deserializer raises on unknown versions so a forward-incompatible
# sketch can't be silently misinterpreted as the current format.
SERIALIZATION_VERSION = 1

# Hash width. We use the top 64 bits of an MD5 digest. With p=8, the
# bucket index takes 8 bits and the leading-zero rank operates on the
# remaining 56 bits — fits in uint8 (max LZC over 56 bits is 56).
_HASH_BITS = 64


def _alpha(m: int) -> float:
    """Bias-correction constant α_m from the Flajolet paper, table 4.

    The closed-form approximation for ``m >= 128`` is exact enough that
    we don't bother with the small-m table — the rollup writer always
    runs at p>=8 (m>=256)."""
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1.0 + 1.079 / m)


def _hash64(item: str | bytes) -> int:
    """Stable 64-bit hash of an item.

    ``hashlib.md5`` is not for security; we use it purely because its
    output is stable across processes (unlike Python's built-in
    ``hash()``, which is randomised per-interpreter via PYTHONHASHSEED).
    Returns an unsigned 64-bit int from the first 8 bytes of the MD5
    digest. Always-on bandit suppression: the hash is not used in any
    auth / signature / integrity context — solely as a uniformly-mixing
    bit generator for HLL.
    """
    if isinstance(item, str):
        item = item.encode("utf-8")
    digest = hashlib.md5(item, usedforsecurity=False).digest()  # noqa: S324
    return struct.unpack("<Q", digest[:8])[0]


def _leading_zero_count(value: int, width_bits: int) -> int:
    """Return ``min(width_bits, leading_zero_count(value) + 1)``.

    The +1 matches the Flajolet definition (rank = position of the
    leftmost 1-bit, counting from 1). When all observed bits are zero,
    we return ``width_bits`` (the maximum possible rank) rather than
    ``width_bits + 1`` — that's the cap implied by the bucket width.
    """
    if value == 0:
        # Every observed bit was zero — assign the max possible rank.
        # (Probability of this is 2^-width_bits; vanishingly rare for
        # 56 observed bits unless someone hands us pathological hashes.)
        return width_bits
    rank = 1
    # Walk from MSB down until we hit a 1. The bit-shift loop is bounded
    # by width_bits, so this is O(width_bits) per item — fine for the
    # cron-side build path.
    msb = 1 << (width_bits - 1)
    while (value & msb) == 0 and rank < width_bits:
        rank += 1
        value <<= 1
    return rank


class HyperLogLog:
    """A small, mergeable HyperLogLog sketch.

    Wraps a per-bucket leading-zero-rank registry. The estimator
    implements the Flajolet 2007 base estimate plus the standard
    small-range (linear counting) and large-range (hash-collision)
    corrections. Sketches with the same precision merge in O(m) by
    taking per-bucket maximums; merging sketches with different
    precisions raises (the caller can downsample explicitly if needed,
    but the rollup tree only ever uses ``DEFAULT_PRECISION``).
    """

    __slots__ = ("_p", "_m", "_buckets")

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        if not 4 <= precision <= 16:
            raise ValueError(f"precision must be in [4, 16], got {precision}")
        self._p = precision
        self._m = 1 << precision
        # Use a bytearray rather than a list[int]; 256 bytes is the same
        # in-memory footprint as the serialized form so we can ship the
        # buffer straight to bytes() without per-bucket conversion.
        self._buckets = bytearray(self._m)

    # ── Public API ───────────────────────────────────────────────────────────

    @property
    def precision(self) -> int:
        return self._p

    @property
    def m(self) -> int:
        return self._m

    def add(self, item: str | bytes) -> None:
        """Insert one item into the sketch.

        Multiple inserts of the same item are idempotent — that's the
        whole point of HLL — so the rollup writer can stream IPs
        without de-duplicating first."""
        h = _hash64(item)
        # Top ``p`` bits → bucket index. We shift the LOW p bits up to
        # the top, then mask. Both directions work; choosing top bits
        # because the leading-zero count then operates on the LOW
        # (HASH_BITS - p) bits, matching how the rank is observed.
        bucket = h >> (_HASH_BITS - self._p)
        # Remaining (HASH_BITS - p) bits → observation for rank.
        observed = h & ((1 << (_HASH_BITS - self._p)) - 1)
        rank = _leading_zero_count(observed, _HASH_BITS - self._p)
        if rank > self._buckets[bucket]:
            self._buckets[bucket] = rank

    def update(self, items: Iterable[str | bytes]) -> None:
        """Bulk-insert from an iterable. Convenience over an ``add`` loop."""
        for item in items:
            self.add(item)

    def count(self) -> int:
        """Return the cardinality estimate.

        Applies the small-range linear-counting correction when many
        buckets are still empty (the base estimate is severely biased
        in that regime) and the large-range hash-collision correction
        when the estimate approaches the 64-bit hash space ceiling
        (irrelevant for our use case but cheap to include).
        """
        m = self._m
        alpha = _alpha(m)

        # Raw estimate (harmonic mean of 2^M[j]).
        # Use float division to avoid overflow on huge bucket-rank values
        # (rank ≤ 56 in our config, so 2^rank fits comfortably).
        raw_sum = sum(2.0**-b for b in self._buckets)
        if raw_sum == 0.0:
            # All buckets at the max rank; estimator undefined. This
            # shouldn't happen in practice (probability ≈ m * 2^-56).
            return m
        estimate = alpha * m * m / raw_sum

        # Small-range correction: linear counting on empty buckets.
        # Threshold from the Flajolet paper. Without this the base
        # estimator overcounts heavily for n ≲ 2.5 m.
        if estimate <= 2.5 * m:
            zeros = sum(1 for b in self._buckets if b == 0)
            if zeros > 0:
                estimate = m * math.log(m / zeros)

        # Large-range correction: aligns the collision-correction term
        # with the actual 64-bit hash width used by _hash64 (the original
        # Flajolet paper assumed a 32-bit hash space, hence the
        # ``1 << 32`` constants in textbook implementations).
        #
        # F016: with a 64-bit hash but the 32-bit constant, an attacker
        # who could push leading-zero counts past ~25 on enough buckets
        # could inflate ``estimate`` past 2^32, making
        # ``1 - estimate / (1 << 32)`` negative and turning math.log()
        # into a hard ValueError. HLL.merge() takes the per-bucket max,
        # so poisoned ranks persist forever in any downstream rollup —
        # one crafted payload becomes a permanent DoS on the rollup
        # read path. Using 1 << 64 keeps the term positive (max
        # observable rank from a 64-bit hash is 64), so the
        # correction stays defined for any input.
        if estimate > (1 << 64) / 30.0:
            estimate = -(1 << 64) * math.log(1 - estimate / (1 << 64))

        return int(round(estimate))

    def merge(self, other: HyperLogLog) -> None:
        """Union ``other`` into ``self`` in-place.

        HLL union is the per-bucket maximum — semantically equivalent
        to having added every input of ``other`` to ``self``. Pinned
        precision: merging sketches at different precisions would
        require downsampling logic; we explicitly reject that case so
        the rollup tree's invariants are easier to reason about."""
        if other._p != self._p:
            raise ValueError(f"merge precision mismatch: self.p={self._p} vs other.p={other._p}")
        for i in range(self._m):
            if other._buckets[i] > self._buckets[i]:
                self._buckets[i] = other._buckets[i]

    # ── Serialization ────────────────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        """Serialize to a compact ``bytes`` payload.

        Layout:
            byte 0:   serialization version tag
            byte 1:   precision p (4..16)
            bytes 2+: m bucket-rank bytes (uint8 each)

        Total size = 2 + m bytes. At ``p=8`` that's 258 bytes per
        sketch — about as compact as HLL gets while keeping the
        format trivially parseable."""
        header = bytes([SERIALIZATION_VERSION, self._p])
        return header + bytes(self._buckets)

    @classmethod
    def from_bytes(cls, blob: bytes | bytearray) -> HyperLogLog:
        """Deserialize a sketch previously produced by :meth:`to_bytes`.

        Raises ``ValueError`` for unknown version bytes (forward-
        incompatibility guard) and for length mismatches (defends
        against truncated reads / mis-typed BLOB columns)."""
        if len(blob) < 2:
            raise ValueError(f"sketch blob too short: {len(blob)} bytes")
        version = blob[0]
        if version != SERIALIZATION_VERSION:
            raise ValueError(f"unknown HLL serialization version {version}; expected {SERIALIZATION_VERSION}")
        precision = blob[1]
        if not 4 <= precision <= 16:
            raise ValueError(f"invalid precision in blob: {precision}")
        expected_len = 2 + (1 << precision)
        if len(blob) != expected_len:
            raise ValueError(f"sketch blob length mismatch: got {len(blob)}, expected {expected_len}")
        sketch = cls(precision=precision)
        sketch._buckets = bytearray(blob[2:])
        return sketch

    def __len__(self) -> int:
        """``len(sketch)`` returns the cardinality estimate.

        Provided so call-sites can write ``len(merged)`` instead of
        ``merged.count()`` when the intent reads better that way."""
        return self.count()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic only
        return f"HyperLogLog(p={self._p}, estimate≈{self.count()})"


def merge_sketches(sketches: Iterable[HyperLogLog]) -> HyperLogLog | None:
    """Fold an iterable of sketches into a single merged sketch.

    Returns ``None`` if the iterable is empty (the rollup reader uses
    this as the "no rollup data for this (field, value) at all"
    signal). All input sketches must share the same precision —
    raised by :meth:`HyperLogLog.merge`."""
    merged: HyperLogLog | None = None
    for sketch in sketches:
        if merged is None:
            merged = HyperLogLog(precision=sketch.precision)
        merged.merge(sketch)
    return merged
