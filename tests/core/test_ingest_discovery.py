"""Discovery / filtering tests for backend.core.ingest.

The ``ingest()`` generator's first phase enumerates new files from FOS using
``ListObjectsV2``. To keep that listing cheap on a large bucket, it derives
a ``StartAfter`` key — either from the user-supplied ``start_time`` or, on
incremental cron runs, from a 4-hour lookback before the most recent
already-ingested file. After listing, each filename is parsed for its
embedded timestamp and filtered against the requested range; the loop
short-circuits once it crosses ``end_time + 1h`` to avoid scanning past
the relevant window.

This file covers the pure helpers (``_parse_fastly_filename_dt``,
``_compute_incremental_start_after``) and the listing-phase integration
via a fake paginator that records the ``StartAfter`` arg actually passed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backend.core.ingest import (
    _compute_incremental_start_after,
    _parse_fastly_filename_dt,
    ingest,
)

# ── _parse_fastly_filename_dt ────────────────────────────────────────────────


class TestParseFastlyFilenameDt:
    def test_parses_canonical_format(self):
        dt = _parse_fastly_filename_dt("2026-05-04T18-30-00.svc.gz")
        assert dt == datetime(2026, 5, 4, 18, 30, 0, tzinfo=UTC)

    def test_returns_none_for_non_matching(self):
        assert _parse_fastly_filename_dt("not-a-fastly-file.txt") is None
        assert _parse_fastly_filename_dt("") is None

    def test_returns_none_for_invalid_calendar_date(self):
        # Feb 30 isn't a real date — datetime.fromisoformat raises ValueError
        assert _parse_fastly_filename_dt("2026-02-30T00-00-00.svc.gz") is None

    def test_ignores_trailing_garbage(self):
        # The regex only anchors at the start; trailing chars don't matter
        dt = _parse_fastly_filename_dt("2026-12-31T23-59-59-pop1-shard0.svc.gz")
        assert dt == datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC)

    def test_returned_datetime_is_utc_aware(self):
        dt = _parse_fastly_filename_dt("2026-01-01T00-00-00.gz")
        assert dt is not None and dt.tzinfo is UTC


# ── _compute_incremental_start_after ─────────────────────────────────────────


class TestComputeIncrementalStartAfter:
    def test_empty_already_returns_none(self):
        assert _compute_incremental_start_after(set()) is None

    def test_single_already_file_subtracts_lookback(self):
        already = {"s3://bkt/raw/2026-05-04/18/2026-05-04T18-30-00.svc.gz"}
        # 4h lookback from 18:30 → 14:30 → key bucket 14:00
        assert _compute_incremental_start_after(already, lookback_hours=4) == "raw/2026-05-04/14/"

    def test_uses_max_filename_when_multiple(self):
        already = {
            "s3://bkt/raw/2026-05-04/10/2026-05-04T10-00-00.gz",
            "s3://bkt/raw/2026-05-04/12/2026-05-04T12-00-00.gz",
            "s3://bkt/raw/2026-05-04/11/2026-05-04T11-00-00.gz",
        }
        # max filename is the 12:00 one → 4h lookback → 08:00 key
        assert _compute_incremental_start_after(already, lookback_hours=4) == "raw/2026-05-04/08/"

    def test_lookback_crosses_day_boundary(self):
        already = {"s3://bkt/raw/2026-05-04/02/2026-05-04T02-00-00.gz"}
        # 4h lookback from 02:00 on May 4 → May 3 22:00
        assert _compute_incremental_start_after(already, lookback_hours=4) == "raw/2026-05-03/22/"

    def test_zero_lookback_returns_same_hour(self):
        already = {"s3://bkt/raw/2026-05-04/18/2026-05-04T18-30-00.gz"}
        assert _compute_incremental_start_after(already, lookback_hours=0) == "raw/2026-05-04/18/"

    def test_skips_files_without_raw_segment(self):
        # Files without /raw/ in the path don't carry timestamps to derive from
        already = {"s3://bkt/iceberg/metadata/v1.metadata.json"}
        assert _compute_incremental_start_after(already) is None

    def test_skips_files_with_unparseable_timestamps(self):
        already = {"s3://bkt/raw/2026/01/garbage.gz"}
        assert _compute_incremental_start_after(already) is None


# ── Listing-phase integration ────────────────────────────────────────────────
#
# These exercise the StartAfter selection by spying on the fake paginator's
# kwargs. They don't need moto — we control the paginator directly so we
# can assert on what the discovery loop *asked* the FOS client to do.


def _make_source(**overrides) -> dict:
    base = {
        "name": "test_disc",
        "service_id": "test-disc-svc",
        "service_name": "Disc Test",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "log_period": 60,
        "provisioning": {},
    }
    base.update(overrides)
    return base


class _FakePaginator:
    """Records the kwargs passed to paginate(); yields one empty page so the
    discovery loop completes without downloading anything.
    """

    def __init__(self):
        self.last_kwargs: dict = {}

    def paginate(self, **kwargs):
        self.last_kwargs = kwargs
        return iter([{"Contents": []}])


class _FakeFosClient:
    def __init__(self, paginator: _FakePaginator):
        self._paginator = paginator

    def get_paginator(self, *args, **kwargs):
        return self._paginator


def _drain_until_done(gen) -> list[dict]:
    events = []
    for ev in gen:
        events.append(ev)
        if ev.get("type") in ("done", "error"):
            break
    return events


def test_explicit_start_time_with_no_history_uses_that_as_start_after():
    """First-time import bounded by user-supplied start_time."""
    paginator = _FakePaginator()
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata_db.get_ingested_filenames", return_value=set()),
    ):
        _drain_until_done(ingest(source=src, start_time="2026-05-04T18:00:00Z"))

    assert paginator.last_kwargs.get("StartAfter") == "raw/2026-05-04/18/"


def test_incremental_only_with_history_uses_lookback_start_after():
    """Cron incremental: derives StartAfter from latest already-ingested file."""
    paginator = _FakePaginator()
    src = _make_source()
    already = {
        "s3://test-bucket/raw/2026-05-04/12/2026-05-04T12-00-00.svc.gz",
        "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz",
    }

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata_db.get_ingested_filenames", return_value=already),
    ):
        _drain_until_done(ingest(source=src, incremental_only=True))

    # Latest is 14:00, 4h lookback → 10:00 bucket
    assert paginator.last_kwargs.get("StartAfter") == "raw/2026-05-04/10/"


def test_manual_import_with_history_does_not_use_start_after():
    """Manual imports (incremental_only=False) scan the whole bucket so they
    pick up any back-filled files outside previous import ranges."""
    paginator = _FakePaginator()
    src = _make_source()
    already = {"s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"}

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata_db.get_ingested_filenames", return_value=already),
    ):
        _drain_until_done(ingest(source=src, incremental_only=False))

    # No StartAfter — full bucket scan
    assert "StartAfter" not in paginator.last_kwargs


def test_incremental_only_with_no_history_does_not_set_start_after():
    """First incremental run before any file is ingested → full scan."""
    paginator = _FakePaginator()
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata_db.get_ingested_filenames", return_value=set()),
    ):
        _drain_until_done(ingest(source=src, incremental_only=True))

    assert "StartAfter" not in paginator.last_kwargs


def test_explicit_start_time_with_history_does_not_double_bound():
    """If both start_time AND already are given, fall through to the
    incremental branch — not the explicit-start-time branch."""
    paginator = _FakePaginator()
    src = _make_source()
    already = {"s3://test-bucket/raw/2026-05-10/00/2026-05-10T00-00-00.svc.gz"}

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata_db.get_ingested_filenames", return_value=already),
    ):
        _drain_until_done(ingest(source=src, start_time="2026-01-01T00:00:00Z", incremental_only=True))

    # Should use the incremental lookback, NOT the explicit Jan 1 start_time
    sa = paginator.last_kwargs.get("StartAfter", "")
    assert sa.startswith("raw/2026-05-09/"), f"expected lookback from May 10, got {sa!r}"
    assert not sa.startswith("raw/2026-01"), "should not use explicit start_time when 'already' is non-empty"


# ── Range filtering inside the listing loop ─────────────────────────────────


@pytest.mark.parametrize(
    "fname,start,end,should_skip",
    [
        # File well before start_time (>1h before) → skip
        ("2026-05-04T10-00-00.gz", "2026-05-04T15:00:00Z", None, True),
        # File 30min before start_time → keep (within the 1h grace window)
        ("2026-05-04T14-30-00.gz", "2026-05-04T15:00:00Z", None, False),
        # File within range → keep
        ("2026-05-04T15-30-00.gz", "2026-05-04T15:00:00Z", "2026-05-04T16:00:00Z", False),
        # File well after end_time → skip / break (end_dt + 1h)
        ("2026-05-04T18-00-00.gz", None, "2026-05-04T15:00:00Z", True),
    ],
)
def test_per_file_range_filtering(fname, start, end, should_skip):
    """The listing loop applies the same datetime parser. Verify the helper
    matches the loop's behaviour for the boundary cases."""
    file_dt = _parse_fastly_filename_dt(fname)
    assert file_dt is not None, f"{fname} should parse"

    from datetime import timedelta

    st_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
    et_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None

    # Replicate the production conditions
    skip_due_to_start = bool(st_dt and file_dt < (st_dt - timedelta(hours=1)))
    skip_due_to_end = bool(et_dt and file_dt > (et_dt + timedelta(hours=1)))

    assert (skip_due_to_start or skip_due_to_end) == should_skip
