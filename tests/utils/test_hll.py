"""Property + golden tests for backend.utils.hll.

The HLL implementation has three load-bearing invariants the rollup
tree relies on:

1. **Cardinality estimate is within HLL's documented error bound**
   across small (< m), medium, and large (>> m) input sizes — so the
   security fingerprint cards don't display obviously-wrong distinct
   IP counts.
2. **Round-trip serialization is byte-identical and lossless** — so
   the parquet BLOB column → deserialized sketch → recomputed estimate
   matches the pre-serialization estimate exactly.
3. **Merging is associative and equivalent to having added every input
   to a single sketch** — so the rollup reader can combine N per-hour
   sketches into a window-distinct estimate without bias.

Tests deliberately use deterministic inputs (no ``random``, no
``hypothesis``) so failures are reproducible across CI runs and the
in-memory hash randomization that Python applies to ``hash()`` is a
non-issue (we use ``hashlib.md5`` for stability inside the HLL itself,
but a test that depends on PYTHONHASHSEED would still be flaky).
"""

from __future__ import annotations

import pytest

from backend.utils.hll import (
    DEFAULT_PRECISION,
    SERIALIZATION_VERSION,
    HyperLogLog,
    merge_sketches,
)

# ── Construction + invariants ────────────────────────────────────────────────


def test_construction_default_precision():
    """Default precision yields 256 buckets matching DEFAULT_PRECISION=8."""
    hll = HyperLogLog()
    assert hll.precision == DEFAULT_PRECISION
    assert hll.m == 256
    # Fresh sketch returns 0 (every bucket empty → linear-counting kicks in)
    assert hll.count() == 0


def test_construction_explicit_precision():
    for p in (4, 6, 8, 10, 12, 14, 16):
        hll = HyperLogLog(precision=p)
        assert hll.precision == p
        assert hll.m == 1 << p


def test_construction_rejects_invalid_precision():
    """Precision outside [4, 16] raises — guards against accidental
    tiny sketches (huge error) or huge sketches (storage bloat)."""
    for bad in (-1, 0, 3, 17, 32):
        with pytest.raises(ValueError, match="precision must be in"):
            HyperLogLog(precision=bad)


# ── Cardinality estimation accuracy ──────────────────────────────────────────


@pytest.mark.parametrize(
    "n,tol",
    [
        # Small range (linear counting territory)
        (10, 0.20),
        (50, 0.20),
        # Medium range — close to the documented 1.04/sqrt(m)≈6.5% bound
        # at p=8, with a small safety margin for the per-run variance.
        (500, 0.10),
        (5000, 0.10),
        # Large range
        (50_000, 0.10),
        # Stress: ten times the bucket count, still well under hash-
        # collision-correction territory
        (100_000, 0.10),
    ],
)
def test_count_accuracy_within_tolerance(n: int, tol: float):
    """Inserting N distinct items yields an estimate within ``tol`` of N.

    Uses deterministic string IDs ("item-0", "item-1", ...) so a test
    failure points at a regression in the algorithm, not a random
    seed. Tolerance widens for small N (linear-counting regime has
    larger relative error)."""
    hll = HyperLogLog(precision=DEFAULT_PRECISION)
    for i in range(n):
        hll.add(f"item-{i}")
    estimate = hll.count()
    rel_err = abs(estimate - n) / n
    assert rel_err <= tol, f"n={n}: estimate={estimate}, expected≈{n}, rel_err={rel_err:.3f}"


def test_add_is_idempotent():
    """Adding the same item N times yields the same sketch as adding it once.

    This is the core HLL property the rollup writer depends on — it
    streams (potentially repeated) IPs without de-duplicating first."""
    hll_once = HyperLogLog()
    hll_many = HyperLogLog()
    items = [f"ip-{i}" for i in range(100)]

    for ip in items:
        hll_once.add(ip)
    for _ in range(5):  # five passes over the same items
        for ip in items:
            hll_many.add(ip)

    assert hll_once.to_bytes() == hll_many.to_bytes()
    assert hll_once.count() == hll_many.count()


def test_update_iterable_matches_add_loop():
    """``update(iterable)`` is sugar for an ``add`` loop and must agree byte-for-byte."""
    items = [f"x-{i}" for i in range(200)]
    hll_a = HyperLogLog()
    hll_b = HyperLogLog()
    for item in items:
        hll_a.add(item)
    hll_b.update(items)
    assert hll_a.to_bytes() == hll_b.to_bytes()


# ── Merge correctness ───────────────────────────────────────────────────────


def test_merge_equivalent_to_single_sketch():
    """A merged sketch must produce the same byte image as a sketch built
    from the union of the inputs. Pinned because losing this would
    bias the rollup reader's cross-hour distinct estimates."""
    left_items = [f"a-{i}" for i in range(300)]
    right_items = [f"b-{i}" for i in range(300)]
    overlapping = [f"both-{i}" for i in range(100)]

    hll_left = HyperLogLog()
    hll_right = HyperLogLog()
    hll_left.update(left_items + overlapping)
    hll_right.update(right_items + overlapping)

    hll_merged = HyperLogLog()
    hll_merged.merge(hll_left)
    hll_merged.merge(hll_right)

    hll_combined = HyperLogLog()
    hll_combined.update(left_items + right_items + overlapping)

    assert hll_merged.to_bytes() == hll_combined.to_bytes()


def test_merge_is_associative():
    """``(a ∪ b) ∪ c == a ∪ (b ∪ c)`` — required for the rollup reader's
    N-way fold across hours to produce a deterministic result regardless
    of iteration order."""
    a = HyperLogLog()
    a.update(f"a-{i}" for i in range(50))
    b = HyperLogLog()
    b.update(f"b-{i}" for i in range(50))
    c = HyperLogLog()
    c.update(f"c-{i}" for i in range(50))

    # (a ∪ b) ∪ c
    left = HyperLogLog()
    left.merge(a)
    left.merge(b)
    left.merge(c)
    # a ∪ (b ∪ c)
    bc = HyperLogLog()
    bc.merge(b)
    bc.merge(c)
    right = HyperLogLog()
    right.merge(a)
    right.merge(bc)

    assert left.to_bytes() == right.to_bytes()


def test_merge_rejects_precision_mismatch():
    """Merging sketches at different precisions raises — explicitly
    rejected so the rollup tree's invariants are easier to reason about.
    A future caller that wants cross-precision merge must downsample
    one side first."""
    hll_p8 = HyperLogLog(precision=8)
    hll_p10 = HyperLogLog(precision=10)
    with pytest.raises(ValueError, match="merge precision mismatch"):
        hll_p8.merge(hll_p10)


def test_merge_sketches_returns_none_on_empty_iterable():
    """``merge_sketches([])`` is the reader's "no rollup data for this
    (field, value)" signal — it must return None, not raise or build
    a zero sketch (which would render as a valid-looking 0)."""
    assert merge_sketches([]) is None


def test_merge_sketches_chains_correctly():
    """``merge_sketches([a, b, c])`` matches manual a.merge(b).merge(c)."""
    sketches = []
    for i in range(3):
        s = HyperLogLog()
        s.update(f"item-{i}-{j}" for j in range(100))
        sketches.append(s)

    merged_helper = merge_sketches(sketches)
    assert merged_helper is not None

    merged_manual = HyperLogLog()
    for s in sketches:
        merged_manual.merge(s)

    assert merged_helper.to_bytes() == merged_manual.to_bytes()


# ── Serialization round-trip ────────────────────────────────────────────────


def test_to_bytes_layout_pinned():
    """The on-disk layout is contract: [version][precision][m bytes].

    Pinned so a future refactor can't change the parquet BLOB
    interpretation without bumping ``SERIALIZATION_VERSION`` and
    forcing a migration."""
    hll = HyperLogLog(precision=8)
    hll.update(f"x-{i}" for i in range(100))
    blob = hll.to_bytes()

    assert blob[0] == SERIALIZATION_VERSION
    assert blob[1] == 8
    # 2-byte header + 256 buckets = 258 bytes total at p=8.
    assert len(blob) == 258


def test_round_trip_serialization_lossless():
    """to_bytes → from_bytes preserves both the bucket image and every
    downstream estimate. Pinned because losing this would silently
    corrupt every reader's distinct-count output."""
    hll = HyperLogLog()
    hll.update(f"ip-{i}" for i in range(2500))
    blob = hll.to_bytes()
    restored = HyperLogLog.from_bytes(blob)

    assert restored.to_bytes() == blob
    assert restored.count() == hll.count()


def test_round_trip_across_precisions():
    """Round-trip must work for every supported precision, with the
    serialized length scaling as ``2 + 2^p``."""
    for p in (4, 8, 12, 16):
        hll = HyperLogLog(precision=p)
        hll.update(f"k-{p}-{i}" for i in range(200))
        blob = hll.to_bytes()
        assert len(blob) == 2 + (1 << p)
        restored = HyperLogLog.from_bytes(blob)
        assert restored.precision == p
        assert restored.to_bytes() == blob


def test_from_bytes_rejects_unknown_version():
    """Forward-incompatibility guard: a future bytes layout must surface
    a loud error here rather than silently misinterpreting the BLOB."""
    blob = bytes([99, 8]) + bytes(256)  # version=99 (unknown)
    with pytest.raises(ValueError, match="unknown HLL serialization version"):
        HyperLogLog.from_bytes(blob)


def test_from_bytes_rejects_length_mismatch():
    """Truncated reads / mistyped BLOB columns must raise rather than
    deserialize partial state and produce a low estimate."""
    # Correct version + precision but wrong number of bucket bytes
    blob = bytes([SERIALIZATION_VERSION, 8]) + bytes(100)  # only 100 buckets vs 256 expected
    with pytest.raises(ValueError, match="sketch blob length mismatch"):
        HyperLogLog.from_bytes(blob)


def test_from_bytes_rejects_invalid_precision():
    """Out-of-range precision in the header surfaces immediately."""
    blob = bytes([SERIALIZATION_VERSION, 3])  # precision 3 outside [4,16]
    with pytest.raises(ValueError, match="invalid precision in blob"):
        HyperLogLog.from_bytes(blob)


def test_from_bytes_rejects_too_short():
    """Less than the 2-byte header means we can't even read the version."""
    with pytest.raises(ValueError, match="sketch blob too short"):
        HyperLogLog.from_bytes(b"")
    with pytest.raises(ValueError, match="sketch blob too short"):
        HyperLogLog.from_bytes(b"\x01")


# ── Hash stability across processes (the whole reason we use MD5) ───────────


def test_hash_is_stable_across_calls():
    """A fresh HLL built from the same items produces byte-identical
    sketches every time. Pinned because Python's built-in ``hash()`` is
    randomized per-interpreter — if we ever accidentally swapped MD5 for
    ``hash()`` the rollup writer would produce different sketches across
    cron restarts and the reader's merge would diverge."""
    items = [f"ip-{i}" for i in range(500)]
    sketches = [HyperLogLog() for _ in range(3)]
    for s in sketches:
        s.update(items)
    # All three sketches must be byte-identical.
    a = sketches[0].to_bytes()
    for s in sketches[1:]:
        assert s.to_bytes() == a


# ── Cardinality estimation edge cases ───────────────────────────────────────


def test_count_empty_sketch_is_zero():
    """An empty sketch reports 0 (linear-counting on an all-zero
    register array gives ``m * log(m/m) = 0``)."""
    assert HyperLogLog().count() == 0


def test_count_single_item():
    """A single insert is in the deep small-range regime; linear
    counting should put the estimate very close to 1.

    The estimate may not be exactly 1 (HLL is an approximate algorithm)
    but for a single inserted item we'd expect it to land between 0 and
    a handful at the default precision."""
    hll = HyperLogLog()
    hll.add("solo-ip")
    estimate = hll.count()
    assert 0 < estimate <= 5, f"single-item estimate {estimate} outside expected small range"


def test_len_returns_count():
    """``len(hll)`` is sugar for ``hll.count()``. Provided so call-sites
    can read more naturally."""
    hll = HyperLogLog()
    hll.update(f"x-{i}" for i in range(100))
    assert len(hll) == hll.count()


def test_add_accepts_bytes_and_str():
    """The rollup writer hands us IP strings; tests + callers may also
    use raw bytes. Both must be acceptable and yield consistent hashes."""
    hll_str = HyperLogLog()
    hll_bytes = HyperLogLog()
    items = ["1.2.3.4", "5.6.7.8", "ffff::1"]
    hll_str.update(items)
    hll_bytes.update(s.encode("utf-8") for s in items)
    assert hll_str.to_bytes() == hll_bytes.to_bytes()


# ── Standard-error sanity check ─────────────────────────────────────────────


def test_documented_error_bound_holds_on_average():
    """At p=8, the standard error is ~1.04/√256 ≈ 6.5%. Across a wide
    spread of cardinalities, the *mean* relative error should stay
    comfortably below that bound. Pinned as a regression guard: a
    subtle bug in the rank computation or estimator would push this
    above the bound."""
    relative_errors = []
    for n in (100, 500, 1_000, 5_000, 10_000, 50_000):
        hll = HyperLogLog()
        hll.update(f"item-{i}" for i in range(n))
        estimate = hll.count()
        relative_errors.append(abs(estimate - n) / n)

    mean_err = sum(relative_errors) / len(relative_errors)
    # Generous bound (2x the theoretical SE) so test isn't flaky on
    # particular inputs but still trips on real algorithmic regressions.
    assert mean_err < 0.13, f"mean relative error {mean_err:.3f} exceeds 13%"


def test_alpha_constant_matches_documented_values():
    """α_m closed-form approximation at common precisions matches the
    expected table values within a small numerical tolerance. Pinned
    because a subtle bug in the alpha constant would bias EVERY
    estimate by a fixed factor."""
    from backend.utils.hll import _alpha

    # Exact table values from the Flajolet paper for small m.
    assert _alpha(16) == pytest.approx(0.673)
    assert _alpha(32) == pytest.approx(0.697)
    assert _alpha(64) == pytest.approx(0.709)
    # The closed-form approximation tracks the table at m=128 within ε.
    expected_128 = 0.7213 / (1.0 + 1.079 / 128)
    assert _alpha(128) == pytest.approx(expected_128)
    # Sanity check at the production precision (m=256).
    expected_256 = 0.7213 / (1.0 + 1.079 / 256)
    assert _alpha(256) == pytest.approx(expected_256, rel=1e-12)


def test_count_does_not_raise_math_domain_error_under_extreme_bucket_ranks():
    """Regression for F016 (audit run 7ba15352).

    HLL._hash64 returns a 64-bit hash, but the large-range correction
    used hard-coded ``1 << 32`` constants:
        if estimate > (1 << 32) / 30.0:
            estimate = -(1 << 32) * math.log(1 - estimate / (1 << 32))

    An attacker who could push leading-zero counts on enough buckets
    past ~25 could inflate ``estimate`` past 2^32, making
    ``1 - estimate / (1 << 32)`` negative and turning math.log() into a
    ValueError("math domain error") — and HLL.merge() takes the
    per-bucket MAX, so poisoned high ranks persist forever in any
    downstream rollup, permanently breaking the read path on those
    rollups.

    The audit's reproducer crafts payloads that push leading-zero
    counts to ≥25 — feasible with an unkeyed MD5 hash because the
    attacker can offline-bruteforce strings until they find ones
    that target each of the 256 buckets and produce rank ≥ 25 (≈ 2^24
    attempts per bucket, days of CPU on commodity hardware).

    With the pre-fix 32-bit constants and every bucket at rank 25:
    estimate ≈ alpha * m² / (m * 2^-25) = alpha * 256 * 2^25 ≈ 6.2e9,
    far above 2^32 (4.3e9); the term ``1 - estimate / (1 << 32)``
    evaluates to ≈ -0.44 and math.log raises ValueError.

    With the 64-bit constants the same estimate stays well below 2^64
    so the term is positive and math.log() is defined.
    """
    from backend.utils.hll import HyperLogLog

    hll = HyperLogLog(precision=8)  # m=256
    # Audit's reproducer rank floor (≥ 25 forces the large-range
    # correction past 2^32 with the pre-fix constants).
    hll._buckets = [25 for _ in hll._buckets]

    # Must not raise ValueError("math domain error").
    n = hll.count()
    # Sanity: the estimate is some integer ≥ 0; we don't pin a specific
    # value because the correction term scales with the (now-correct)
    # 2^64 constant. What we care about is that it didn't crash.
    assert isinstance(n, int)
    assert n >= 0
