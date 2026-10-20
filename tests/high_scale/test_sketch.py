"""Log-histogram sketch used to merge latency percentiles exactly across minutes."""

from __future__ import annotations

import math
import random

import pytest

from backend.high_scale._sketch import RELATIVE_ERROR, sketch


def _weighted_quantile(centroids: list[float], counts: list[int], q: float) -> float:
    """Mirror of ClickHouse ``quantileExactWeighted``: first centroid whose cumulative weight reaches ``q``."""
    target = q * sum(counts)
    seen = 0
    for centroid, count in zip(centroids, counts, strict=True):
        seen += count
        if seen >= target:
            return centroid
    return centroids[-1]


def test_empty_input_yields_empty_sketch() -> None:
    assert sketch([]) == ([], [])


def test_counts_conserve_every_observation() -> None:
    values = [0.0, 0.0, 5.0, 5.0, 5.1, 1_000.0, -20.0]

    centroids, counts = sketch(values)

    assert sum(counts) == len(values)
    assert centroids == sorted(centroids)
    assert len(centroids) == len(counts)


def test_zero_and_negative_values_keep_their_sign() -> None:
    centroids, counts = sketch([0.0, -100.0, 100.0])

    assert centroids[0] < 0 < centroids[2]
    assert centroids[1] == 0.0
    assert counts == [1, 1, 1]


def test_nearby_values_share_a_centroid_and_far_values_do_not() -> None:
    centroids, counts = sketch([1000.0, 1000.5, 50_000.0])

    assert counts == [2, 1]
    assert len(centroids) == 2


@pytest.mark.parametrize("q", [0.5, 0.75, 0.95, 0.99])
def test_merged_quantiles_stay_within_the_documented_relative_error(q: float) -> None:
    rng = random.Random(7)
    minutes = [[rng.lognormvariate(8.0, 1.2) for _ in range(300)] for _ in range(20)]
    merged: dict[float, int] = {}
    for minute in minutes:
        centroids, counts = sketch(minute)
        for centroid, count in zip(centroids, counts, strict=True):
            merged[centroid] = merged.get(centroid, 0) + count
    everything = sorted(value for minute in minutes for value in minute)
    exact = everything[math.ceil(q * len(everything)) - 1]

    estimate = _weighted_quantile(sorted(merged), [merged[c] for c in sorted(merged)], q)

    assert estimate == pytest.approx(exact, rel=RELATIVE_ERROR * 1.001)
