"""Shared high-scale primitives keep the exact semantics of the five/four copies they replaced."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.high_scale._projection_utils import bucket, finite_float, percentile, range_value, text


def test_range_value_keeps_naive_for_date_only_and_tz_for_z():
    assert range_value(None) is None
    assert range_value("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert range_value("2026-01-02").tzinfo is None


def test_bucket_floors_to_utc_minute_and_rejects_garbage():
    assert bucket("2026-01-02T03:04:59.9Z") == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket(datetime(2026, 1, 2, 3, 4, 59, 5, tzinfo=UTC)) == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket("not a date") is None
    assert bucket(5) is None


def test_text_and_finite_float():
    assert text("  x ") == "x"
    assert text(5) == ""
    assert finite_float("1.5") == 1.5
    assert finite_float(True) is None
    assert finite_float("") is None
    assert finite_float("nan") is None
    assert finite_float("inf") is None


@pytest.mark.parametrize(
    "values,q,expected",
    [([], 0.5, None), ([4.0], 0.99, 4.0), ([1.0, 2.0, 3.0, 4.0], 0.5, 2.5), ([10.0, 0.0], 0.95, pytest.approx(9.5))],
)
def test_percentile_linear_interpolation(values, q, expected):
    assert percentile(values, q) == expected


def test_percentiles_matches_percentile_for_random_lists():
    import random

    from backend.high_scale._projection_utils import percentiles

    rng = random.Random(1234)
    qs = (0.0, 0.5, 0.75, 0.95, 0.99, 1.0)
    for size in (0, 1, 2, 3, 17, 200):
        values = [rng.uniform(-50, 5000) for _ in range(size)]
        assert percentiles(values, qs) == [percentile(values, q) for q in qs]


def test_percentiles_sorts_once(monkeypatch):
    import builtins

    from backend.high_scale import _projection_utils as utils

    calls = []
    real_sorted = builtins.sorted
    monkeypatch.setattr(utils, "sorted", lambda *a, **k: calls.append(1) or real_sorted(*a, **k), raising=False)
    utils.percentiles([3.0, 1.0, 2.0], (0.5, 0.95, 0.99))
    assert len(calls) == 1
