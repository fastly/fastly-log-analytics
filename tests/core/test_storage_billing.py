"""Unit tests for the shared selected-period storage billing calculator."""

from __future__ import annotations

import pytest

from backend.core.storage_billing import (
    StorageBillingInterval,
    StorageBillingSummary,
    calculate_storage_billing,
)

pytestmark = pytest.mark.no_postgres

GB = 1_000_000_000
HOUR_MS = 3_600_000


def _h(hours: int) -> int:
    return hours * HOUR_MS


def test_selected_period_intersects_live_and_minimum_billed_objects() -> None:
    intervals = [
        {
            "id": "live-a",
            "dataset": "pq_request",
            "kind": "live",
            "bytes": GB,
            "billed_from": _h(10),
            "billed_until": _h(20),
            "complete": True,
            "provenance": "high-scale:source_objects",
        },
        {
            "id": "deleted-a",
            "dataset": "raw_request",
            "kind": "minimum_billed",
            "bytes": 2 * GB,
            "billed_from": _h(11),
            "billed_until": _h(13),
            "complete": True,
            "provenance": "high-scale:archive_manifests",
        },
    ]

    summary = calculate_storage_billing(intervals, _h(12), _h(14))

    assert summary == StorageBillingSummary(
        live_gb_hours=2.0,
        deleted_gb_hours=0.0,
        minimum_billed_gb_hours=2.0,
        total_gb_hours=4.0,
        complete=True,
        provenance=["high-scale:archive_manifests", "high-scale:source_objects"],
    )


def test_deleted_interval_stops_at_deletion_and_excludes_non_overlapping() -> None:
    intervals = [
        StorageBillingInterval(
            id="del-after-min",
            dataset="raw_request",
            kind="deleted",
            bytes=GB,
            created_at=_h(0),
            deleted_at=_h(730),
            billed_from=_h(0),
            billed_until=_h(730),
            complete=True,
            provenance="high-scale:source_objects",
        ),
        StorageBillingInterval(
            id="outside-before",
            dataset="raw_request",
            kind="deleted",
            bytes=10 * GB,
            billed_from=_h(0),
            billed_until=_h(720),
            complete=False,
            provenance="high-scale:ignored",
        ),
        StorageBillingInterval(
            id="outside-after",
            dataset="raw_request",
            kind="live",
            bytes=10 * GB,
            billed_from=_h(740),
            billed_until=_h(750),
            complete=False,
            provenance="high-scale:ignored-after",
        ),
    ]

    summary = calculate_storage_billing(intervals, _h(725), _h(735))

    assert summary.deleted_gb_hours == pytest.approx(5.0)
    assert summary.live_gb_hours == 0.0
    assert summary.minimum_billed_gb_hours == 0.0
    assert summary.total_gb_hours == pytest.approx(5.0)
    assert summary.complete is True
    assert summary.provenance == ["high-scale:source_objects"]


def test_incomplete_contributing_interval_marks_summary_incomplete() -> None:
    intervals = [
        {
            "id": "incomplete-copy-del",
            "dataset": "cloud_copy",
            "kind": "minimum_billed",
            "bytes": GB,
            "created_at": None,
            "deleted_at": _h(1),
            "billed_from": _h(1),
            "billed_until": _h(5),
            "complete": False,
            "provenance": "high-scale:cloud_copy_deletions",
        }
    ]

    summary = calculate_storage_billing(intervals, _h(2), _h(4))

    assert summary.complete is False
    assert summary.minimum_billed_gb_hours == pytest.approx(2.0)
    assert summary.total_gb_hours == pytest.approx(2.0)
    assert summary.provenance == ["high-scale:cloud_copy_deletions"]


def test_deduplicates_by_interval_id_and_clamps_negative_bytes() -> None:
    intervals = [
        {
            "id": "dup-1",
            "dataset": "raw_request",
            "kind": "live",
            "bytes": GB,
            "billed_from": _h(0),
            "billed_until": _h(4),
            "complete": True,
            "provenance": "high-scale:source_objects",
        },
        {
            "id": "dup-1",
            "dataset": "raw_request",
            "kind": "live",
            "bytes": 99 * GB,
            "billed_from": _h(0),
            "billed_until": _h(4),
            "complete": False,
            "provenance": "high-scale:duplicate",
        },
        {
            "id": "neg-bytes",
            "dataset": "raw_request",
            "kind": "deleted",
            "bytes": -500,
            "billed_from": _h(0),
            "billed_until": _h(4),
            "complete": True,
            "provenance": "high-scale:source_objects",
        },
    ]

    summary = calculate_storage_billing(intervals, _h(1), _h(3))

    assert summary.live_gb_hours == pytest.approx(2.0)
    assert summary.deleted_gb_hours == 0.0
    assert summary.total_gb_hours == pytest.approx(2.0)
    assert summary.complete is True
    assert summary.provenance == ["high-scale:source_objects"]


def test_empty_period_returns_zero_complete_and_empty_provenance() -> None:
    intervals = [
        {
            "id": "live-a",
            "dataset": "raw_request",
            "kind": "live",
            "bytes": GB,
            "billed_from": _h(0),
            "billed_until": _h(10),
            "complete": False,
            "provenance": "high-scale:source_objects",
        }
    ]

    summary = calculate_storage_billing(intervals, _h(5), _h(5))

    assert summary == StorageBillingSummary(
        live_gb_hours=0.0,
        deleted_gb_hours=0.0,
        minimum_billed_gb_hours=0.0,
        total_gb_hours=0.0,
        complete=True,
        provenance=[],
    )


def test_inverted_period_raises_value_error() -> None:
    with pytest.raises(ValueError, match="end_ms"):
        calculate_storage_billing([], _h(10), _h(5))


def test_postgres_control_plane_storage_billing_intervals_lifecycle() -> None:
    """High-Scale Postgres adapter emits live, deleted, minimum_billed, and archive intervals without 3x counting."""
    from contextlib import contextmanager
    from datetime import UTC, datetime, timedelta

    from backend.high_scale.postgres_control.control_plane import PostgresControlPlane

    t0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    window_start = t0 + timedelta(hours=10)
    window_end = t0 + timedelta(hours=14)

    source_rows = [
        # Live source object discovered at t0 + 8h (overlaps [10h, 14h) = 4h)
        ("obj-live", "request", GB, "acknowledged", t0 + timedelta(hours=8), t0 + timedelta(hours=9)),
        # Deleted inside 30d minimum term: discovered at t0 + 9h, deleted at t0 + 11h
        # In [10h, 14h): 1h deleted ([10h, 11h)) + 3h minimum_billed ([11h, 14h))
        ("obj-del-min", "request", 2 * GB, "source_deleted", t0 + timedelta(hours=9), t0 + timedelta(hours=11)),
        # Deleted AFTER 30d minimum term: discovered 35d before t0, deleted at t0 + 12h
        # In [10h, 14h): 2h deleted ([10h, 12h)), 0h minimum_billed
        ("obj-del-after", "rum", GB, "source_deleted", t0 - timedelta(days=35), t0 + timedelta(hours=12)),
    ]
    archive_rows = [
        # Live archive manifest: 4 GB artifact_size counted ONCE (not 3x for 3 FOS objects)
        ("man-live", "request", 4 * GB, t0 + timedelta(hours=12), None),
        # Expired archive manifest inside 30d minimum term: created at t0 + 8h, expired at t0 + 10h
        # In [10h, 14h): 4h minimum_billed
        ("man-exp", "request", GB, t0 + timedelta(hours=8), t0 + timedelta(hours=10)),
    ]

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __init__(self):
            self.queries: list[str] = []

        def execute(self, sql: str, _params=()):
            self.queries.append(sql)
            if "high_scale_source_objects" in sql:
                return FakeCursor(source_rows)
            if "high_scale_archive_manifests" in sql:
                return FakeCursor(archive_rows)
            raise AssertionError(f"Unexpected SQL: {sql}")

    plane = object.__new__(PostgresControlPlane)
    fake_conn = FakeConnection()

    @contextmanager
    def _fake_tx(*, read_only: bool = False):
        assert read_only is True
        yield fake_conn

    plane.transaction = _fake_tx  # type: ignore[method-assign]

    intervals, complete, provenance = plane.storage_billing_intervals("svc-hs", window_start, window_end)
    summary = calculate_storage_billing(
        intervals,
        int(window_start.timestamp() * 1000),
        int(window_end.timestamp() * 1000),
    )

    assert complete is True
    assert summary.complete is True
    assert provenance == ["high-scale:archive_manifests", "high-scale:source_objects"]
    # live: obj-live (1 GB * 4h = 4) + man-live (4 GB * 2h = 8) = 12
    assert summary.live_gb_hours == pytest.approx(12.0)
    # deleted: obj-del-min (2 GB * 1h = 2) + obj-del-after (1 GB * 2h = 2) = 4
    assert summary.deleted_gb_hours == pytest.approx(4.0)
    # minimum_billed: obj-del-min (2 GB * 3h = 6) + man-exp (1 GB * 4h = 4) = 10
    assert summary.minimum_billed_gb_hours == pytest.approx(10.0)
    assert summary.total_gb_hours == pytest.approx(26.0)


def test_postgres_control_plane_missing_creation_timestamp_marks_incomplete() -> None:
    """Missing creation timestamp emits a conservative minimum-billed interval and marks complete=False."""
    from contextlib import contextmanager
    from datetime import UTC, datetime, timedelta

    from backend.high_scale.postgres_control.control_plane import PostgresControlPlane

    t0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    window_start = t0 + timedelta(hours=10)
    window_end = t0 + timedelta(hours=12)

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def execute(self, sql: str, _params=()):
            if "high_scale_source_objects" in sql:
                return FakeCursor([("obj-no-ts", "request", GB, "source_deleted", None, t0 + timedelta(hours=9))])
            return FakeCursor([])

    plane = object.__new__(PostgresControlPlane)

    @contextmanager
    def _fake_tx(*, read_only: bool = False):
        yield FakeConnection()

    plane.transaction = _fake_tx  # type: ignore[method-assign]

    intervals, complete, _provenance = plane.storage_billing_intervals("svc-hs", window_start, window_end)
    summary = calculate_storage_billing(
        intervals,
        int(window_start.timestamp() * 1000),
        int(window_end.timestamp() * 1000),
    )

    assert complete is False
    assert summary.complete is False
    assert summary.minimum_billed_gb_hours == pytest.approx(2.0)
    assert summary.deleted_gb_hours == 0.0
    assert intervals[0]["created_at"] is None


def test_postgres_control_plane_storage_billing_summary_aggregates_in_sql() -> None:
    """High-Scale storage billing summary must aggregate GB-hours in SQL instead of fetching every object row."""
    from contextlib import contextmanager
    from datetime import UTC, datetime, timedelta

    from backend.high_scale.postgres_control.control_plane import PostgresControlPlane

    t0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    window_start = t0 + timedelta(hours=10)
    window_end = t0 + timedelta(hours=14)

    class FakeCursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class FakeConnection:
        def __init__(self):
            self.queries: list[str] = []

        def execute(self, sql: str, _params=()):
            self.queries.append(sql)
            if "high_scale_source_objects" in sql:
                return FakeCursor((4.0, 4.0, 6.0, 0))
            if "high_scale_archive_manifests" in sql:
                return FakeCursor((8.0, 0.0, 4.0, 1))
            raise AssertionError(f"unexpected query: {sql}")

    conn = FakeConnection()
    plane = object.__new__(PostgresControlPlane)

    @contextmanager
    def _fake_tx(*, read_only: bool = False):
        assert read_only is True
        yield conn

    plane.transaction = _fake_tx  # type: ignore[method-assign]

    live_gb_h, deleted_gb_h, min_gb_h, complete, provenance = plane.storage_billing_summary(
        "svc-hs", window_start, window_end
    )

    assert len(conn.queries) == 2
    assert all("SUM(" in q.upper() for q in conn.queries)
    assert live_gb_h == pytest.approx(12.0)
    assert deleted_gb_h == pytest.approx(4.0)
    assert min_gb_h == pytest.approx(10.0)
    assert complete is False
    assert provenance == ["high-scale:archive_manifests", "high-scale:source_objects"]


def test_postgres_control_plane_storage_billing_summary_runs_table_scans_concurrently_with_sargable_predicates() -> (
    None
):
    """Source-object and archive-manifest billing queries run concurrently with index-friendly range predicates."""
    import threading
    from contextlib import contextmanager
    from datetime import UTC, datetime, timedelta

    from backend.high_scale.postgres_control import control_plane as cp_mod

    t0 = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    window_start = t0 + timedelta(hours=10)
    window_end = t0 + timedelta(hours=14)
    barrier = threading.Barrier(2, timeout=2.0)
    seen_sql: list[str] = []

    class FakeCursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class FakeConnection:
        def execute(self, sql: str, _params=()):
            seen_sql.append(sql)
            barrier.wait()
            if "high_scale_source_objects" in sql:
                return FakeCursor((1.0, 2.0, 3.0, 0))
            if "high_scale_archive_manifests" in sql:
                return FakeCursor((4.0, 5.0, 6.0, 0))
            raise AssertionError(f"unexpected query: {sql}")

    plane = object.__new__(cp_mod.PostgresControlPlane)

    @contextmanager
    def _fake_tx(*, read_only: bool = False):
        assert read_only is True
        yield FakeConnection()

    plane.transaction = _fake_tx  # type: ignore[method-assign]

    live_gb_h, deleted_gb_h, min_gb_h, complete, _ = plane.storage_billing_summary("svc-hs", window_start, window_end)
    assert (live_gb_h, deleted_gb_h, min_gb_h, complete) == (5.0, 7.0, 9.0, True)
    assert len(seen_sql) == 2
    # WHERE predicates must be sargable (no GREATEST(... + make_interval(...)) in WHERE)
    import re

    for sql in seen_sql:
        expected_branches = sql.count("UNION ALL") + 1
        assert expected_branches >= 3
        where_clauses = re.findall(r"\bWHERE\s+(.*?)(?=\s+UNION ALL\b|\)\s*AS\b|$)", sql, flags=re.DOTALL)
        assert len(where_clauses) == expected_branches
        for where_clause in where_clauses:
            assert "GREATEST(" not in where_clause
    assert "high_scale_source_objects_discovered_idx" in cp_mod.SCHEMA_DDL
    assert "high_scale_source_objects_updated_idx" in cp_mod.SCHEMA_DDL
    assert "high_scale_archive_manifests_expired_idx" in cp_mod.SCHEMA_DDL


def test_python_matches_typescript_shared_storage_billing_fixtures() -> None:
    """Python and TypeScript calculators produce identical results on every shared fixture."""
    import json
    from pathlib import Path

    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "frontend"
        / "lib"
        / "cloud-engine"
        / "usage"
        / "storageBilling.fixtures.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    cases = payload["cases"]
    assert len(cases) >= 7

    for case in cases:
        py_intervals = [
            {
                "id": item["id"],
                "dataset": item["dataset"],
                "kind": item["kind"],
                "bytes": item["bytes"],
                "created_at": item.get("createdAt"),
                "deleted_at": item.get("deletedAt"),
                "billed_from": item["billedFrom"],
                "billed_until": item["billedUntil"],
                "complete": item["complete"],
                "provenance": item["provenance"],
            }
            for item in case["intervals"]
        ]
        summary = calculate_storage_billing(py_intervals, case["from"], case["to"])
        expected = case["expected"]
        assert summary.live_gb_hours == pytest.approx(expected["liveGbHours"]), case["name"]
        assert summary.deleted_gb_hours == pytest.approx(expected["deletedGbHours"]), case["name"]
        assert summary.minimum_billed_gb_hours == pytest.approx(expected["minimumBilledGbHours"]), case["name"]
        assert summary.total_gb_hours == pytest.approx(expected["totalGbHours"]), case["name"]
        assert summary.complete is expected["complete"], case["name"]
        assert summary.provenance == expected["provenance"], case["name"]


def test_fos_usage_utc_date_helpers_and_ratchet_compliance(monkeypatch: pytest.MonkeyPatch) -> None:
    """fos_usage uses date_utils (ensure_utc / parse_hour_key) and satisfies check_no_raw_dateparse.sh."""
    import re
    import subprocess
    from datetime import UTC, datetime, timedelta, timezone
    from pathlib import Path

    from backend.core.metadata import fos_usage

    captured_params: list[tuple[object, ...]] = []

    class FakeCon:
        def execute(self, _sql: str, params=()):
            captured_params.append(tuple(params))
            return self

        def fetchall(self):
            if len(captured_params) == 1:
                return [("2026-06-01T12:00:00Z", GB, 2)]
            return []

        def close(self) -> None:
            pass

    monkeypatch.setattr(fos_usage, "_ul", lambda _sid: FakeCon())
    monkeypatch.setattr(fos_usage, "open_readonly", lambda _sid: FakeCon())

    # Naive start and non-UTC offset end must both normalize cleanly via ensure_utc:
    naive_start = datetime(2026, 6, 4, 10, 0, 0)
    est = timezone(timedelta(hours=-4))
    offset_end = datetime(2026, 6, 4, 10, 0, 0, tzinfo=est)  # 14:00:00Z
    records = fos_usage.cloud_copy_deletion_records_in_window("svc-fos", naive_start, offset_end, minimum_days=7)
    assert len(records) == 1
    assert captured_params[0] == ("svc-fos", "2026-05-28T10:00:00Z", "2026-06-04T14:00:00Z")

    # Valid 13-char YYYY-MM-DDTHH rolls over midnight cleanly via parse_hour_key:
    captured_params.clear()
    fos_usage.get_usage_operation_buckets("svc-fos", "2026-06-04T23:15:00Z", "2026-06-05T02:00:00.000Z", "hour")
    assert captured_params[0][5] == "2026-06-05T00:00:00.000Z"

    # Malformed non-13-char hour prefix ("2026-06-04T5") must NOT be misparsed as 05:00
    # (which inverted SQLite ASCII bounds '2026-06-04T5' < '2026-06-04T06...'):
    captured_params.clear()
    fos_usage.get_usage_operation_buckets("svc-fos", "2026-06-04T5", "2026-06-05T02:00:00.000Z", "hour")
    assert captured_params[0][5] == "2026-06-04T5:59:59.999Z"

    # Verify fos_usage.py raw date-parse count stays within B5 ratchet baseline (<= 2):
    fos_src = Path(fos_usage.__file__).read_text(encoding="utf-8")
    raw_sites = [
        line
        for line in fos_src.splitlines()
        if not line.lstrip().startswith("#")
        and re.search(r"strptime\(|fromisoformat\(|replace\(tzinfo=(UTC|timezone\.utc)", line)
    ]
    assert len(raw_sites) <= 2, f"fos_usage.py exceeds B5 raw date-parse baseline (found {len(raw_sites)}): {raw_sites}"

    repo_root = Path(fos_usage.__file__).resolve().parents[3]
    ratchet_script = repo_root / "scripts" / "check_no_raw_dateparse.sh"
    if (repo_root / ".git").exists() and ratchet_script.exists():
        proc = subprocess.run(["bash", str(ratchet_script)], cwd=str(repo_root), capture_output=True, text=True)
        assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"
    assert naive_start.replace(tzinfo=UTC) < offset_end.astimezone(UTC)
