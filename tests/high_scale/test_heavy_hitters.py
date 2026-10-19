from backend.high_scale.heavy_hitters import BoundedHeavyHitters


def test_exact_counts_under_capacity() -> None:
    hitters = BoundedHeavyHitters(capacity=10)
    for key in ["a", "b", "a", "c", "a", "b"]:
        hitters.add(key)

    assert hitters.is_exact is True
    assert hitters.most_common(10) == [("a", 3, 0), ("b", 2, 0), ("c", 1, 0)]


def test_size_never_exceeds_capacity_under_unbounded_cardinality() -> None:
    hitters = BoundedHeavyHitters(capacity=50)
    for i in range(10_000):
        hitters.add(f"unique-{i}")

    assert hitters.size() <= 50
    assert hitters.is_exact is False


def test_true_heavy_hitter_survives_and_is_never_undercounted() -> None:
    hitters = BoundedHeavyHitters(capacity=10)
    for i in range(500):
        hitters.add("hot-key")
        hitters.add(f"noise-{i}")

    top = hitters.most_common(1)
    assert top[0][0] == "hot-key"
    # Space-Saving guarantees the estimate is never below the true count.
    assert top[0][1] >= 500
    # ...and the overestimate is bounded by the reported max error.
    assert top[0][1] - top[0][2] <= 500


def test_capacity_must_be_positive() -> None:
    import pytest

    with pytest.raises(ValueError):
        BoundedHeavyHitters(capacity=0)
