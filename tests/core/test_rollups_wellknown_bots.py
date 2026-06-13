"""Tests for the wellknown_bots rollup writer + reader.

The rollup pre-materialises the (ua, ip, count) tuples that
``/api/security/aggregates``'s wellknown_bots block would otherwise
compute via a 500-pattern RE2 prefilter on the request-scoped
temp_table. Writer runs from the sync cron after
``recompute_touched_hours``; reader is called from
``backend/repositories/security.py`` with a live-SQL fallback.

These tests pin the reader's fall-back semantics — the WRITER path
needs an actual DuckDB connection against a base table, which the
broader integration tests cover. The reader is the correctness
boundary (return wrong data → wrong bot counts in the UI), so it gets
the bulk of the coverage here.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


def _write_rollup_hour(
    cache_root: str,
    hour: str,
    rows: list[tuple[str, str, int]],
    pattern_set_version: str = "v1700000000",
) -> str:
    """Write a synthetic wellknown_bots rollup parquet for a single hour.

    Returns the parquet path. Schema mirrors what
    :func:`backend.core.rollups.recompute_wellknown_bots_rollup` writes:
    ``(ua, ip, request_count, pattern_set_version)``.
    """
    d = os.path.join(cache_root, "rollups", "wellknown_bots", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "ua": pa.array([r[0] for r in rows]),
            "ip": pa.array([r[1] for r in rows]),
            "request_count": pa.array([r[2] for r in rows], type=pa.int64()),
            "pattern_set_version": pa.array([pattern_set_version] * len(rows)),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _past_hour_iso(hours_ago: int) -> tuple[str, str]:
    """Return ``(hour_token, iso_string)`` for a fully-closed hour N hours ago.

    Using a delta of >=2 hours guarantees the bucket is closed even
    when the test runs at HH:00:00.001 — the reader's active-hour
    check (which would return None for hour-mix windows) doesn't
    trigger.
    """
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_read_returns_rows_when_window_fully_covered_and_versions_match(tmp_path):
    """Happy path: every hour in the window has a fresh-version
    rollup, reader returns the union sorted DESC by request_count."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-1"}

    h2, _ = _past_hour_iso(2)
    h3, _ = _past_hour_iso(3)
    _, start_iso = _past_hour_iso(3)
    _, end_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 50)])
        _write_rollup_hour(str(cache_root), h3, [("Bingbot/2.0", "157.55.39.1", 20)])

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is not None, "expected rollup hit; got None (fallback)"
    # DESC by request_count.
    assert rows == [("Googlebot/2.1", "66.249.66.1", 50), ("Bingbot/2.0", "157.55.39.1", 20)]


def test_read_falls_back_when_any_hour_missing(tmp_path):
    """Hour-mix window where ONE hour lacks a rollup partition must
    return None so the caller fails over to live SQL — returning a
    partial union would undercount the missing hour's bot traffic."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-2"}

    # Window spans two closed hours; only one has a rollup.
    h2, _ = _past_hour_iso(2)
    _, start_iso = _past_hour_iso(3)
    _, end_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 50)])

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "missing hour must trigger fallback (got rollup data instead)"


def test_read_falls_back_when_pattern_set_version_stale(tmp_path):
    """A bot-sources refresh bumps ``get_pattern_set_version``; any
    rollup written under the previous version must be ignored so a
    newly-added bot pattern doesn't silently miss recent traffic."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-3"}

    h2, _ = _past_hour_iso(2)
    _, start_iso = _past_hour_iso(2)
    _, end_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000999"),
    ):
        _write_rollup_hour(
            str(cache_root),
            h2,
            [("OldBot/1.0", "1.2.3.4", 10)],
            pattern_set_version="v1700000000",
        )

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "stale pattern_set_version must trigger fallback"


def test_read_falls_back_when_window_includes_active_hour(tmp_path):
    """Active (current UTC) hour is never rolled up (live SQL serves
    in-progress traffic). A window that includes it must return None
    to fall back to live SQL across the whole window."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-4"}

    h2, _ = _past_hour_iso(2)
    # End time is RIGHT NOW (active hour).
    end_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    _, start_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        _write_rollup_hour(str(cache_root), h2, [("Googlebot/2.1", "66.249.66.1", 5)])

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None, "active hour in window must trigger fallback"


def test_read_returns_empty_list_when_window_covered_but_no_bot_traffic(tmp_path):
    """An empty parquet (a closed hour with zero matches) is a valid
    "0 bot rows" answer, not a fallback signal. The reader must
    distinguish missing-file from empty-but-covered so a quiet hour
    doesn't trigger an unnecessary live-SQL scan."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-5"}

    h2, _ = _past_hour_iso(2)
    _, start_iso = _past_hour_iso(2)
    _, end_iso = _past_hour_iso(2)

    # Empty parquet — file exists, zero rows.
    d = os.path.join(str(cache_root), "rollups", "wellknown_bots", f"hour={h2}")
    os.makedirs(d, exist_ok=True)
    empty_table = pa.table(
        {
            "ua": pa.array([], type=pa.string()),
            "ip": pa.array([], type=pa.string()),
            "request_count": pa.array([], type=pa.int64()),
            "pattern_set_version": pa.array([], type=pa.string()),
        }
    )
    pq.write_table(empty_table, os.path.join(d, "compacted_empty.parquet"))

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value="v1700000000"),
    ):
        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    # Empty list, not None — the rollup covers the window.
    assert rows == []


def test_read_falls_back_when_pattern_set_version_missing(tmp_path):
    """No source files cached yet → no version → no rollup possible.
    Reader must defer to live-SQL even if parquet files happen to
    exist (e.g. left over from a previous source set)."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bot-6"}

    h2, _ = _past_hour_iso(2)
    _, start_iso = _past_hour_iso(2)
    _, end_iso = _past_hour_iso(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=""),
    ):
        _write_rollup_hour(str(cache_root), h2, [("OldBot/1.0", "1.2.3.4", 10)])

        rows = rollups.read_wellknown_bots_rollup(src, start_iso, end_iso)

    assert rows is None
