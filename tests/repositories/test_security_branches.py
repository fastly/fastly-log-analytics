"""Tests for narrow defensive branches in backend.repositories.security.

The rest of the repository's test coverage lives in test_security.py and
heavily exercises the rollup-routing decisions on the happy paths. This
file targets the parse-failure / OSError / empty-input branches that
guard those rollup paths from blowing up at runtime."""

from __future__ import annotations

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.repositories import security
from backend.repositories._base import QueryRunner

# ── _window_eligible_for_rollup ────────────────────────────────────────────


@pytest.mark.parametrize(
    "start,end",
    [
        (None, "2026-01-04T00:00:00Z"),
        ("2026-01-01T00:00:00Z", None),
        (None, None),
        ("", "2026-01-04T00:00:00Z"),
    ],
)
def test_window_eligible_for_rollup_returns_false_when_bounds_missing(start, end):
    """Open-ended requests can't be rollup-served — they fall through to
    live SQL via the False return."""
    assert security._window_eligible_for_rollup(start, end) is False


def test_window_eligible_for_rollup_returns_false_when_parse_raises():
    """parse_iso_utc may raise on garbage input; the wrapper must catch
    rather than let the exception bubble into the request handler."""
    with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
        assert security._window_eligible_for_rollup("bad", "alsobad") is False


def test_window_eligible_for_rollup_returns_false_when_parse_yields_none():
    """parse_iso_utc returns None (not raises) on some malformed inputs.
    The None-guard must trip so the function returns False instead of
    crashing on the subtraction below."""
    with patch("backend.utils.date_utils.parse_iso_utc", return_value=None):
        assert security._window_eligible_for_rollup("x", "y") is False


def test_window_eligible_for_rollup_returns_false_when_end_before_start():
    """A reversed window is rejected — defends against a buggy caller
    that would otherwise produce a negative timedelta."""
    assert security._window_eligible_for_rollup("2026-01-10T00:00:00Z", "2026-01-01T00:00:00Z") is False


def test_window_eligible_for_rollup_returns_true_for_window_at_3d_threshold():
    """Exactly 3 days qualifies (>= boundary check) — pinned because the
    threshold is the documented break-even point."""
    assert security._window_eligible_for_rollup("2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is True


def test_window_eligible_for_rollup_returns_false_below_3d_threshold():
    """Just under 3 days disqualifies — the live path is cheaper there."""
    assert security._window_eligible_for_rollup("2026-01-01T00:00:00Z", "2026-01-03T23:00:00Z") is False


# ── _has_rollup_coverage: input + OSError defensive branches ───────────────


def test_has_rollup_coverage_returns_false_for_unsafe_ident_field(tmp_path):
    """The field name flows into a SQL identifier; an unsafe value
    short-circuits to False before any filesystem work."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    # A space breaks _is_safe_ident.
    assert security._has_rollup_coverage(src, "bad field", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z") is False


def test_has_rollup_coverage_returns_false_when_bounds_missing(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    assert security._has_rollup_coverage(src, "is_ipv6", None, "2026-01-04T00:00:00Z") is False
    assert security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", None) is False


def test_has_rollup_coverage_returns_false_when_parse_returns_none(tmp_path):
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.utils.date_utils.parse_iso_utc", return_value=None):
        assert security._has_rollup_coverage(src, "is_ipv6", "x", "y") is False


def test_has_rollup_coverage_swallows_oserror_on_bundled_listdir(tmp_path):
    """OSError on the bundled directory's listdir is caught — the
    function falls through to the per-field tree (which is also empty
    here) and returns False rather than propagating."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    bundled = tmp_path / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)

    real_listdir = os.listdir

    def _listdir(p):
        if str(p) == str(bundled):
            raise OSError("simulated")
        return real_listdir(p)

    with patch("os.listdir", side_effect=_listdir):
        out = security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is False


def test_has_rollup_coverage_swallows_oserror_on_hour_dir_listdir(tmp_path):
    """OSError on an INNER (per-field hour-dir) listdir is caught with
    ``continue`` — the outer loop keeps walking."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    per_field = tmp_path / "rollups" / "hour" / "field=is_ipv6"
    hour_dir = per_field / "hour=2026-01-02-12"
    hour_dir.mkdir(parents=True)

    real_listdir = os.listdir

    def _listdir(p):
        if str(p) == str(hour_dir):
            raise OSError("simulated inner")
        return real_listdir(p)

    with patch("os.listdir", side_effect=_listdir):
        out = security._has_rollup_coverage(src, "is_ipv6", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is False


# ── _ipv6_per_hour_from_rollups ────────────────────────────────────────────


@contextmanager
def _runner_ctx():
    con = duckdb.connect(":memory:")
    yield QueryRunner(con, {"name": "svc"})
    con.close()


def test_ipv6_per_hour_from_rollups_returns_none_when_bounds_missing(tmp_path):
    with _runner_ctx() as runner:
        assert (
            security._ipv6_per_hour_from_rollups(
                runner, {"name": "svc", "_cache_dir_override": str(tmp_path)}, None, "2026-01-04T00:00:00Z"
            )
            is None
        )
        assert (
            security._ipv6_per_hour_from_rollups(
                runner, {"name": "svc", "_cache_dir_override": str(tmp_path)}, "2026-01-01T00:00:00Z", None
            )
            is None
        )


def test_ipv6_per_hour_from_rollups_returns_none_when_parse_raises(tmp_path):
    with _runner_ctx() as runner:
        src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
        with patch("backend.utils.date_utils.parse_iso_utc", side_effect=ValueError("bad iso")):
            assert security._ipv6_per_hour_from_rollups(runner, src, "bad", "alsobad") is None


def test_ipv6_per_hour_from_rollups_returns_none_when_no_in_window_paths(tmp_path):
    """No bundled or per-field parquets exist on disk → no closed-hour
    data, return None (caller falls back to live SQL)."""
    with _runner_ctx() as runner:
        src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
        out = security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_ipv6_per_hour_from_rollups_returns_none_when_query_raises(tmp_path):
    """A real read_parquet error is caught; the function returns None
    so the caller falls back instead of failing the whole card."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    bundled = tmp_path / "rollups" / "hour_bundled" / "hour=2026-01-02-12"
    bundled.mkdir(parents=True)
    # Write a SENTINEL file (not a real parquet) so listdir finds it,
    # but read_parquet will throw a duckdb.IOException trying to parse it.
    (bundled / "all_fields.parquet").write_bytes(b"not a parquet")

    with _runner_ctx() as runner:
        out = security._ipv6_per_hour_from_rollups(runner, src, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


# ── _proxy_dist_from_rollups ───────────────────────────────────────────────


def test_proxy_dist_from_rollups_returns_none_when_execute_raises():
    """When execute_top_n_rollups raises (cold pool, permission error,
    etc.), the function returns None so _build_security_response falls
    back to live SQL."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.side_effect = RuntimeError("rollup pool unavailable")
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_returns_none_for_empty_rolled():
    """Reader returned no rows (cold service or no p_type values) → None."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = ([], None)
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_filters_other_and_empty_and_nones():
    """The reader may return synthetic ``__other__`` rows, empty strings,
    and DuckDB NULLs. All three must be filtered out. If after filtering
    every row was excluded, return None."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "__other__", 100),
            ("p_type", "", 50),
            ("p_type", None, 25),
        ],
        None,
    )
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out is None


def test_proxy_dist_from_rollups_aggregates_and_sorts_by_count_desc():
    """Valid p_type rows are summed by value and sorted by count
    descending. The aggregate is exact across hours (no approximation)."""
    runner = MagicMock(spec=QueryRunner)
    runner.execute_top_n_rollups.return_value = (
        [
            ("p_type", "datacenter", 100),
            ("p_type", "residential", 250),
            ("p_type", "datacenter", 50),  # additional hour for datacenter
            ("p_type", "__other__", 999),  # filtered
            ("p_type", "vpn", 75),
        ],
        None,
    )
    out = security._proxy_dist_from_rollups(runner, "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z")
    assert out == [
        {"type": "residential", "count": 250},
        {"type": "datacenter", "count": 150},
        {"type": "vpn", "count": 75},
    ]
