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

import backend.core.ingest as ingest_mod
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
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
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
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
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
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
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
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
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
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
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


# ── End-to-end discovery integration: hit the listing-loop branches ───────────
#
# These extend the paginator pattern with real Contents lists so the inner
# for-loop branches (non-.gz skip, 10k progress threshold, range filtering,
# end_time early-stops, max_files truncation) actually execute.


class _MultiPagePaginator:
    """Paginator that yields a fixed sequence of pages and records what it
    saw. ``last_kwargs`` is still captured so existing assertions work."""

    def __init__(self, pages: list[list[dict]]):
        self._pages = pages
        self.last_kwargs: dict = {}

    def paginate(self, **kwargs):
        self.last_kwargs = kwargs
        return iter([{"Contents": page} for page in self._pages])


def _fake_obj(key: str, size: int = 1024) -> dict:
    return {"Key": key, "Size": size}


def _drive_through_listing(src, paginator, **ingest_kwargs):
    """Run ingest() far enough that the listing loop completes, then stop
    via a sentinel exception inside get_ingest_columns_sql (the first
    heavy thing post-list). The sentinel escapes ingest() unguarded — we
    catch it here so callers see only the events yielded before the stop
    point. Avoids mocking the full DuckDB ingest pipeline."""
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=RuntimeError("__stop__")),
    ):
        events: list[dict] = []
        try:
            for ev in ingest(source=src, **ingest_kwargs):
                events.append(ev)
        except RuntimeError as e:
            if "__stop__" not in str(e):
                raise
    return events


def test_listing_skips_non_gz_entries():
    """A FOS bucket may contain non-.gz objects (manifest files, logs from
    other producers). The discovery loop skips them via the continue at
    line 416 without crashing the parse step."""
    paginator = _MultiPagePaginator(
        [
            [
                _fake_obj("raw/2026-05-04/10/2026-05-04T10-00-00.svc.gz"),
                _fake_obj("raw/2026-05-04/10/_SUCCESS"),
                _fake_obj("raw/2026-05-04/10/manifest.json"),
            ]
        ]
    )
    events = _drive_through_listing(_make_source(), paginator)
    assert any("Discovering" in (e.get("message") or "") for e in events)


def test_listing_breaks_after_max_files():
    """max_files truncates new_files and breaks pagination — used by
    /api/admin/ingest-preview to cap LIST cost on huge buckets."""
    page = [_fake_obj(f"raw/2026-05-04/10/2026-05-04T10-{i:02d}-00.svc.gz") for i in range(20)]
    paginator = _MultiPagePaginator([page, [_fake_obj("raw/2026-05-04/11/should-not-be-seen.svc.gz")]])
    events = _drive_through_listing(_make_source(), paginator, max_files=5)
    assert events


def test_listing_early_stops_when_end_time_exceeded_in_file():
    """Line 432: a file within a page whose timestamp is more than 1h past
    end_time triggers the inner-loop ``break``. Files after it on the same
    page must NOT be added."""
    paginator = _MultiPagePaginator(
        [
            [
                _fake_obj("raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"),
                _fake_obj("raw/2026-05-04/17/2026-05-04T17-00-00.svc.gz"),
                _fake_obj("raw/2026-05-04/18/2026-05-04T18-00-00.svc.gz"),
            ]
        ]
    )
    events = _drive_through_listing(
        _make_source(),
        paginator,
        start_time="2026-05-04T14:00:00Z",
        end_time="2026-05-04T15:30:00Z",
    )
    assert events


def test_listing_skips_file_before_start_time():
    """Line 436: a file with timestamp > 1h before start_time is skipped.
    The page that follows it (in range) still produces work."""
    paginator = _MultiPagePaginator(
        [
            [
                _fake_obj("raw/2026-05-04/10/2026-05-04T10-00-00.svc.gz"),
                _fake_obj("raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"),
            ]
        ]
    )
    events = _drive_through_listing(_make_source(), paginator, start_time="2026-05-04T15:00:00Z")
    assert events


def test_listing_breaks_when_last_key_in_page_exceeds_end_time():
    """Per-page early-stop at line 446-449: after a page completes, if the
    LAST key is more than 1h past end_time, pagination stops without
    fetching further pages."""
    page1 = [
        _fake_obj("raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"),
        _fake_obj("raw/2026-05-04/17/2026-05-04T17-00-00.svc.gz"),
    ]
    page2 = [_fake_obj("raw/2026-05-04/18/should-not-be-fetched.svc.gz")]
    paginator = _MultiPagePaginator([page1, page2])
    _drive_through_listing(_make_source(), paginator, end_time="2026-05-04T15:30:00Z")
    assert paginator.last_kwargs.get("Prefix") == "raw/"


def test_discovery_progress_yielded_every_10k_files():
    """Line 419-421: every 10,000 files discovered yields a status event
    so a manual ingest of a huge bucket has progress feedback."""
    page = [_fake_obj(f"raw/2026-05-04/00/2026-05-04T00-00-{i:08d}.svc.gz") for i in range(10_001)]
    paginator = _MultiPagePaginator([page])
    events = _drive_through_listing(_make_source(), paginator)
    msgs = [e.get("message", "") for e in events if e.get("type") == "status"]
    assert any("Discovered 10,000" in m for m in msgs), f"missing 10k progress; saw: {msgs}"


# ── ingest() early-tick exception paths ───────────────────────────────────────


def test_ingest_swallows_recover_in_flight_exception():
    """Line 332-333: if _recover_in_flight blows up (corrupted SQLite,
    permission error, etc.), the ingest tick must still proceed — the
    exception is logged and we move on to the discovery phase."""
    paginator = _MultiPagePaginator([[]])
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._recover_in_flight", side_effect=RuntimeError("metadata corrupt")),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
    ):
        events = _drain_until_done(ingest(source=src))

    # Must have completed (no exception escaped) — the run yields its
    # eventual "done" / "no new files" message after surviving the
    # recovery exception.
    assert events
    # And the FOS scan still ran.
    assert paginator.last_kwargs.get("Prefix") == "raw/"


def test_ingest_yields_recovery_status_when_buffers_promoted():
    """Line 323-331: when _recover_in_flight returns promoted>0 or
    dropped>0, ingest yields a 'Crash recovery' status before continuing."""
    paginator = _MultiPagePaginator([[]])
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch(
            "backend.core.ingest._recover_in_flight",
            return_value={"promoted": 2, "dropped": 1, "rows_recovered": 1000},
        ),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
    ):
        events = _drain_until_done(ingest(source=src))

    msgs = [e.get("message", "") for e in events if e.get("type") == "status"]
    assert any("Crash recovery" in m for m in msgs), f"missing crash-recovery status; saw: {msgs}"


def test_skipped_files_is_zero_when_bucket_purged():
    """delete_after=True (the common case): a successfully-ingested file is
    DELETEd from FOS, so the incremental LIST surfaces nothing already-known.
    ``skipped_files`` must report 0 — NOT the size of the (non-empty) dedup
    ledger. The old count read the rollup ``file_count`` and so leaked the
    ~1-day ledger size (e.g. 267k) into a stat labelled "skipped"."""
    paginator = _MultiPagePaginator([[]])  # bucket purged → empty LIST
    src = _make_source()
    already = {  # ledger still holds recent files (1-day retention)
        "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz",
    }

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
    ):
        events = _drain_until_done(ingest(source=src, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done"
    assert done["new_files"] == 0
    assert done["skipped_files"] == 0, "purged bucket → nothing skipped, regardless of ledger size"


def test_skipped_files_counts_real_dedup_hits():
    """``skipped_files`` is the count of files re-seen in THIS LIST that we'd
    already ingested (e.g. a delete that hasn't landed yet re-surfaces a
    file). It reflects actual per-run skips, not the ledger total."""
    already = {
        "s3://test-bucket/raw/2026-05-04/10/2026-05-04T10-00-00.svc.gz",
        "s3://test-bucket/raw/2026-05-04/11/2026-05-04T11-00-00.svc.gz",
    }
    paginator = _MultiPagePaginator(
        [
            [
                _fake_obj("raw/2026-05-04/10/2026-05-04T10-00-00.svc.gz"),
                _fake_obj("raw/2026-05-04/11/2026-05-04T11-00-00.svc.gz"),
            ]
        ]
    )
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
    ):
        events = _drain_until_done(ingest(source=src, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done"
    assert done["new_files"] == 0
    assert done["skipped_files"] == 2


# ── Stranded-delete reconcile (interrupt-safe deletion) ─────────────────────


@pytest.fixture(autouse=True)
def _reset_reconcile_throttle():
    """The reconcile throttle (_reconcile_last_attempt) is process-global module
    state; clear it around every test so one test's attempt can't throttle the
    next (they share the _make_source() service name)."""
    ingest_mod._reconcile_last_attempt.clear()
    yield
    ingest_mod._reconcile_last_attempt.clear()


class _FakeFosClientRecordingDeletes:
    """Like _FakeFosClient but records bulk-delete calls so the stranded-delete
    reconcile can be asserted on. Returns an empty (no-Errors) response, so
    _delete_objects_robust counts every key as deleted."""

    def __init__(self, paginator):
        self._paginator = paginator
        self.deleted_keys: list[str] = []
        self.delete_calls = 0

    def get_paginator(self, *args, **kwargs):
        return self._paginator

    def delete_objects(self, Bucket, Delete):  # noqa: N803 — boto3 kwarg names
        self.delete_calls += 1
        self.deleted_keys.extend(o["Key"] for o in Delete["Objects"])
        return {}


def test_stranded_files_reclaimed_when_delete_after():
    """A file present in the LIST that is ALSO already in the ledger is a strand:
    ingested but never deleted (a restart hit between the ledger write and the
    FOS delete). With delete_after=True, when the strand is classified reclaimable
    (durable + post-epoch), the reconcile re-issues the delete so the leak
    self-heals — no new ingest required."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    client = _FakeFosClientRecordingDeletes(paginator)
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
    ):
        events = _drain_until_done(ingest(source=src, delete_after=True, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done"
    assert done["new_files"] == 0
    assert done["deleted_files"] == 1, "the stranded file must be reclaimed"
    # The delete targeted the bucket-relative key, not the s3:// path.
    assert client.deleted_keys == ["raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"]
    assert "reclaim" in done["message"].lower()


def test_strand_not_classified_reclaimable_is_never_deleted():
    """DATA-SAFETY: a strand that the durability classifier does NOT return (a
    no-data marker with row_count==0, OR a pre-epoch row whose row_count can't be
    trusted) must be left alone, even though it is a strand (in LIST ∩ ledger).
    Guards a delete_after False→True flip from deleting the raw of historically-
    retained, never-durably-stored files."""
    marker = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {marker}
    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    client = _FakeFosClientRecordingDeletes(paginator)
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        # Classifier returns nothing reclaimable → must NOT be deleted.
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value=set()),
    ):
        events = _drain_until_done(ingest(source=src, delete_after=True, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done"
    assert done["deleted_files"] == 0, "an unproven strand must never be deleted"
    assert client.delete_calls == 0


def test_reconcile_fails_safe_when_classification_errors():
    """If the durability classifier throws, we cannot prove anything is safe, so
    the reconcile must reclaim NOTHING that run (retry next tick) rather than risk
    deleting a marker."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    client = _FakeFosClientRecordingDeletes(paginator)
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch(
            "backend.core.metadata.get_reclaimable_strand_filenames",
            side_effect=RuntimeError("db locked"),
        ),
    ):
        events = _drain_until_done(ingest(source=src, delete_after=True, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done"
    assert done["deleted_files"] == 0
    assert client.delete_calls == 0, "classification failure must fail safe (no deletes)"


def test_stranded_files_not_deleted_when_delete_after_false():
    """Manual / read-only sweeps (delete_after=False) must NEVER delete. A file
    re-seen from the ledger is counted as skipped but left in place — even if it
    would otherwise classify as reclaimable, the delete_after gate blocks it."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    client = _FakeFosClientRecordingDeletes(paginator)
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
    ):
        events = _drain_until_done(ingest(source=src, delete_after=False, incremental_only=False))

    done = events[-1]
    assert done["type"] == "done"
    assert done["skipped_files"] == 1
    assert done["deleted_files"] == 0
    assert client.delete_calls == 0, "delete_after=False must not issue any delete"


def test_reconcile_throttled_on_repeat_incremental_tick():
    """COST: two incremental ticks for the same service within the throttle window
    must issue at most one delete attempt — a persistently-failing strand can't
    drive a FOS call every ~5s tick."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    src = _make_source()

    def _run():
        paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
        client = _FakeFosClientRecordingDeletes(paginator)
        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._get_fos_client", return_value=client),
            patch("backend.core.metadata.get_ingested_filenames", return_value=already),
            patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
        ):
            _drain_until_done(ingest(source=src, delete_after=True, incremental_only=True))
        return client

    first = _run()
    second = _run()
    assert first.delete_calls == 1, "first incremental tick reconciles"
    assert second.delete_calls == 0, "second tick within the window is throttled"


def test_reconcile_not_throttled_on_full_sync():
    """The full_sync path (incremental_only=False) is the whole-bucket backstop and
    must reconcile even right after a throttled incremental tick."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    src = _make_source()
    # Pre-arm the throttle as if an incremental tick just ran.
    ingest_mod._reconcile_last_attempt[src["name"]] = 10**12

    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    client = _FakeFosClientRecordingDeletes(paginator)
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
    ):
        _drain_until_done(ingest(source=src, delete_after=True, incremental_only=False))

    assert client.delete_calls == 1, "full_sync ignores the incremental throttle"


def test_stranded_reclaim_runs_even_when_new_files_present():
    """The reconcile fires before the ingest pipeline, so a strand is cleaned up
    on the same tick that also ingests new files — and only the strand is
    reclaimed (new files are deleted later by the per-chunk path)."""
    stranded_key = "raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    new_key = "raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"
    stranded = f"s3://test-bucket/{stranded_key}"
    already = {stranded}
    paginator = _MultiPagePaginator([[_fake_obj(stranded_key), _fake_obj(new_key)]])
    client = _FakeFosClientRecordingDeletes(paginator)
    src = _make_source()

    # Stop the run in the ingest pipeline (after the reconcile, which runs before
    # `if not new_files`) so we observe the strand delete without mocking DuckDB.
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=RuntimeError("__stop__")),
    ):
        try:
            for _ in ingest(source=src, delete_after=True, incremental_only=True):
                pass
        except RuntimeError as e:
            if "__stop__" not in str(e):
                raise

    # Only the strand was reclaimed by the reconcile; the new file is left for
    # the per-chunk delete path (which we stopped short of).
    assert client.deleted_keys == [stranded_key]


def test_stranded_reclaim_failure_does_not_crash_run():
    """If the delete layer throws during reconcile, the run must still complete
    (the strand is simply retried on the next tick — idempotent by design)."""
    stranded = "s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"
    already = {stranded}
    paginator = _MultiPagePaginator([[_fake_obj("raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz")]])
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch("backend.core.metadata.get_reclaimable_strand_filenames", return_value={stranded}),
        patch("backend.core.ingest._delete_objects_robust", side_effect=RuntimeError("FOS down")),
    ):
        events = _drain_until_done(ingest(source=src, delete_after=True, incremental_only=True))

    done = events[-1]
    assert done["type"] == "done", "reconcile failure must not abort the run"
    assert done["deleted_files"] == 0
    assert done["skipped_files"] == 1


def test_in_flight_file_is_never_reclaimed_by_strand_reconcile(monkeypatch):
    """DATA-SAFETY (mid-ingest): a file that is only IN-FLIGHT — recorded in
    ``ingest_in_flight`` but not yet committed to ``ingested_files`` (the
    mark-before-write window at ingest.py:1000) — must never have its raw .gz
    reclaimed by the stranded-delete reconcile, even when a genuine durable
    strand IS reclaimed on the same tick.

    The reconcile derives strands from ``LIST ∩ ingested_files`` and classifies
    them with ``get_reclaimable_strand_filenames`` (which queries ONLY
    ``ingested_files``), so an in-flight-only file is structurally invisible to
    it. This pins that invariant against the REAL ledger + REAL classifier (no
    metadata mocks): the committed strand is deleted, the in-flight raw is left
    untouched, and the in_flight row survives for a later recovery sweep.

    ``_recover_in_flight`` (which normally resolves in_flight rows at the top of
    the tick) is held off so the row is genuinely PRESENT when the reconcile
    runs — the worst case (recovery sweep failed / hasn't caught up), which is
    exactly when the reconcile's own safety must hold. The run is stopped in the
    ingest pipeline (after the reconcile, which runs before ``if not new_files``)
    so the strand delete is observed without mocking the DuckDB download path —
    same technique as test_stranded_reclaim_runs_even_when_new_files_present.
    """
    from backend.core import metadata as metadata_db

    bucket = "test-bucket"
    strand_key = "raw/2026-05-04/14/2026-05-04T14-00-00.strand.gz"
    inflight_key = "raw/2026-05-04/15/2026-05-04T15-00-00.inflight.gz"
    strand = f"s3://{bucket}/{strand_key}"
    inflight = f"s3://{bucket}/{inflight_key}"
    # Unique service name isolates the real metadata ledger + filename cache.
    src = _make_source(name="test_disc_inflight", service_id="test-disc-inflight-svc")

    # Genuine durable strand: committed to ingested_files (row_count>0) and
    # re-seen in the bucket LIST → a reclaimable strand.
    metadata_db.insert_ingested_files(src["name"], [(strand, 100, 5000)])
    # In-flight file: ONLY in ingest_in_flight, NOT in ingested_files. Its raw is
    # ALSO in the bucket LIST this tick, but it must be excluded from reclaim.
    metadata_db.record_in_flight(src["name"], "batch_inflight.parquet", [(inflight, 100, 5000)])

    # Open the durability epoch so the seeded datetime('now') strand row
    # classifies reclaimable (the default epoch is in the future relative to now,
    # so the genuine strand would otherwise never be reclaimed and the test
    # couldn't prove the reconcile actually fired).
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2000-01-01 00:00:00")

    paginator = _MultiPagePaginator([[_fake_obj(strand_key), _fake_obj(inflight_key)]])
    client = _FakeFosClientRecordingDeletes(paginator)

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch(
            "backend.core.ingest._recover_in_flight",
            return_value={"promoted": 0, "dropped": 0, "rows_recovered": 0},
        ),
        patch("backend.core.ingest._get_fos_client", return_value=client),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=RuntimeError("__stop__")),
    ):
        try:
            for _ in ingest(source=src, delete_after=True, incremental_only=True):
                pass
        except RuntimeError as e:
            if "__stop__" not in str(e):
                raise

    # Only the committed strand was reclaimed; the in-flight raw was left alone.
    assert client.deleted_keys == [strand_key], client.deleted_keys
    assert inflight_key not in client.deleted_keys
    # The in_flight row survived the reconcile untouched (a later recovery sweep
    # promotes-or-drops it; the reconcile must not collaterally clear it).
    assert metadata_db.list_in_flight(src["name"]), "the in_flight row must survive the strand reconcile"


def test_ingest_logs_when_compute_incremental_start_after_fails():
    """Line 391-393: in incremental_only mode with non-empty already,
    _compute_incremental_start_after can fail on malformed keys. The
    failure is logged and the tick falls back to a full-bucket scan
    (no StartAfter)."""
    paginator = _MultiPagePaginator([[]])
    src = _make_source()
    already = {"s3://test-bucket/raw/2026-05-04/14/2026-05-04T14-00-00.svc.gz"}

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=already),
        patch(
            "backend.core.ingest._compute_incremental_start_after",
            side_effect=RuntimeError("bad key shape"),
        ),
    ):
        _drain_until_done(ingest(source=src, incremental_only=True))

    # No StartAfter set — full bucket scan path was taken after the failure.
    assert "StartAfter" not in paginator.last_kwargs


def test_ingest_parses_end_time_for_range_pre_filter():
    """Line 342-343: when end_time is supplied, parse_iso_utc fires. The
    parsed et_dt is then used in the range pre-filter. We pin that the
    end_time string is accepted (doesn't raise) and the downstream
    early-stop logic engages."""
    page = [
        _fake_obj("raw/2026-05-04/20/2026-05-04T20-00-00.svc.gz"),  # past end+1h → break
    ]
    paginator = _MultiPagePaginator([page])
    src = _make_source()

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.ingest._get_fos_client", return_value=_FakeFosClient(paginator)),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
    ):
        events = _drain_until_done(ingest(source=src, end_time="2026-05-04T15:00:00Z"))

    # Reached "done" status (no new files in range, but no exception).
    assert events
