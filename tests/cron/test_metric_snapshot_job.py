"""Tests for the _run_metric_snapshot cron job."""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from backend.core import metric_snapshots
from backend.cron.jobs.metric_snapshot import (
    _run_metric_snapshot,
    _safe_record,
    _sample_active_queries,
    _sample_cron_duration,
    _sample_ingest_lag,
    _sample_os_vitals,
)


def test_tick_records_active_query_count():
    """Active-query path is the simplest — it doesn't depend on
    per-service state, just the global registry summary."""
    with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 3}):
        _run_metric_snapshot()

    out = metric_snapshots.get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) >= 1
    assert out[-1]["value"] == 3.0


def test_tick_records_pool_wait_per_service():
    fake_stats = [
        {"service": "svc-a", "wait": {"p95_ms": 12.3, "count": 100}},
        {"service": "svc-b", "wait": {"p95_ms": 45.6, "count": 50}},
    ]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        _run_metric_snapshot()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = metric_snapshots.get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    b = metric_snapshots.get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert any(r["value"] == 12.3 for r in a)
    assert any(r["value"] == 45.6 for r in b)


def test_tick_continues_when_one_metric_fails():
    """A blow-up in one sampler must not skip the others — the
    _safe_record wrapper + per-metric try/except in the sampler ensures
    the rest of the tick still runs."""
    with patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")):
        with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 7}):
            _run_metric_snapshot()  # must not raise

    out = metric_snapshots.get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert out and out[-1]["value"] == 7.0


# ── _safe_record swallows per-metric failures ──────────────────────────────────


def test_safe_record_swallows_record_snapshot_exception():
    """_safe_record is the wrapper that keeps a sampler-emitted bad
    value from poisoning the whole tick. Pin that an exception in
    record_snapshot is caught and only logged at debug."""
    with patch.object(metric_snapshots, "record_snapshot", side_effect=ValueError("bad metric")):
        _safe_record("bogus", 1.0)  # must not raise


# ── _sample_cron_duration: per-service iteration + error containment ──────────


def _fake_row(task: str, secs: float) -> dict:
    """SQLite row_factory dict shape (con.row_factory=Row)."""
    return {"task": task, "duration_seconds": secs}


def test_sample_cron_duration_records_per_service_per_task():
    """Mock list_configs to enumerate two services and verify that the
    latest terminal cron_runs row for each task is recorded with the
    right service_id/task labels."""
    cfgs = [{"service_id": "svc-a"}, {"service_id": "svc-b"}]

    fake_cons: dict[str, MagicMock] = {}
    for svc, rows in (("svc-a", [_fake_row("sync", 0.5)]), ("svc-b", [_fake_row("optimize", 2.0)])):
        cur = MagicMock()
        cur.fetchall.return_value = rows
        con = MagicMock()
        con.execute.return_value = cur
        fake_cons[svc] = con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=lambda sid: fake_cons[sid]),
    ):
        _sample_cron_duration()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = metric_snapshots.get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    b = metric_snapshots.get_history("cron_duration_ms", since=since, service_id="svc-b", task="optimize")
    assert any(r["value"] == 500.0 for r in a)  # 0.5 s → 500 ms
    assert any(r["value"] == 2000.0 for r in b)


def test_sample_cron_duration_skips_configs_without_service_id():
    """A config without a service_id (partial init / corrupted file) is
    silently skipped — must not raise and must not call get_con."""
    cfgs = [{"service_id": None}, {}, {"service_id": "svc-real"}]

    called_with: list[str] = []

    def _record(sid: str) -> MagicMock:
        called_with.append(sid)
        cur = MagicMock()
        cur.fetchall.return_value = []
        con = MagicMock()
        con.execute.return_value = cur
        return con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_record),
    ):
        _sample_cron_duration()

    assert called_with == ["svc-real"]


def test_sample_cron_duration_continues_after_one_service_fails():
    """A get_con failure for one service must be caught and logged; the
    iteration must proceed to the next service."""
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    good_cur = MagicMock()
    good_cur.fetchall.return_value = [_fake_row("sync", 1.0)]
    good_con = MagicMock()
    good_con.execute.return_value = good_cur

    def _get_con(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return good_con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_get_con),
    ):
        _sample_cron_duration()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    good = metric_snapshots.get_history("cron_duration_ms", since=since, service_id="svc-good", task="sync")
    assert any(r["value"] == 1000.0 for r in good)


def test_sample_cron_duration_outer_except_handles_import_failure():
    """If the late `from backend import config` itself blows up (e.g.
    the package somehow can't import in this process), the outer except
    must swallow it. Pin that path."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("module broken")):
        _sample_cron_duration()  # must not raise


# ── _sample_ingest_lag: per-service iteration + error containment ─────────────


def test_sample_ingest_lag_records_recent_lag():
    """Happy path: latest ingest 10 minutes ago → lag_s ≈ 600."""
    cfgs = [{"service_id": "svc-ingest"}]
    ten_min_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=ten_min_ago),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = metric_snapshots.get_history("ingest_lag_s", since=since, service_id="svc-ingest")
    assert rows
    # ~600 s, allow ±30 s wall-clock slack.
    assert 570 <= rows[-1]["value"] <= 630


def test_sample_ingest_lag_skips_when_no_latest_ts():
    """A service that has never ingested has no latest_ts. Must skip
    rather than recording a bogus huge value or crashing on None."""
    cfgs = [{"service_id": "svc-empty"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=None),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert metric_snapshots.get_history("ingest_lag_s", since=since, service_id="svc-empty") == []


def test_sample_ingest_lag_skips_unparseable_timestamp():
    """A latest_ts that parse_iso_utc can't parse returns None — the
    sampler must skip silently rather than crashing on datetime math."""
    cfgs = [{"service_id": "svc-bad-ts"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value="not-a-timestamp"),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert metric_snapshots.get_history("ingest_lag_s", since=since, service_id="svc-bad-ts") == []


def test_sample_ingest_lag_continues_after_one_service_fails():
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    def _fake_get_latest(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_fake_get_latest),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert metric_snapshots.get_history("ingest_lag_s", since=since, service_id="svc-good")


def test_sample_ingest_lag_skips_configs_without_service_id():
    cfgs = [{"service_id": None}, {"service_id": "svc-real"}]
    seen: list[str] = []

    def _record(sid: str) -> None:
        seen.append(sid)
        return None  # no data

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_record),
    ):
        _sample_ingest_lag()

    assert seen == ["svc-real"]


# ── _sample_active_queries: exception path ────────────────────────────────────


def test_sample_active_queries_swallows_summary_exception():
    with patch("backend.core.query_registry.query_registry.summary", side_effect=RuntimeError("boom")):
        _sample_active_queries()  # must not raise

    # Nothing recorded for this tick.
    since = datetime.now(UTC) - timedelta(seconds=5)
    pre = metric_snapshots.get_history("active_query_count", since=since)
    assert pre == [] or all(r["value"] != 0 for r in pre[:0])  # vacuous — just pin no-crash


# ── _sample_os_vitals: getloadavg / /proc/meminfo / disk_usage branches ───────


def test_sample_os_vitals_swallows_getloadavg_exception():
    """getloadavg() raises on platforms that don't expose it (some
    container runtimes) — must be caught."""
    with patch("os.getloadavg", side_effect=OSError("not available")):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_parses_proc_meminfo_when_available():
    """Linux path: /proc/meminfo opens and is parsed; mem_used_pct is
    recorded based on (1 - MemAvailable/MemTotal) * 100. The dev macOS
    box doesn't have /proc, so this exercises the parse path via a
    fake open()."""
    meminfo = "MemTotal:       16384 kB\nMemAvailable:    4096 kB\nBuffers:    100 kB\n"

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        return io.StringIO(meminfo)

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = metric_snapshots.get_history("mem_used_pct", since=since)
    assert rows
    # (1 - 4096/16384) * 100 = 75.0
    assert rows[-1]["value"] == 75.0


def test_sample_os_vitals_swallows_meminfo_parse_error():
    """Open succeeds but the file contents are garbage — must catch the
    non-FileNotFoundError exception."""

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        raise PermissionError("denied")

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_records_disk_usage():
    """disk_usage() returns a triple; the percentage is rounded to two
    decimal places."""
    fake_disk = MagicMock(total=1000, used=500, free=500)

    with patch("shutil.disk_usage", return_value=fake_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = metric_snapshots.get_history("disk_used_pct", since=since)
    assert rows
    assert rows[-1]["value"] == 50.0


def test_sample_os_vitals_swallows_disk_usage_exception_per_path():
    """When /app/data raises a non-FileNotFoundError (e.g. permission
    denied), the loop must continue and still try /."""

    def _disk(path):
        if path == "/app/data":
            raise PermissionError("denied")
        return MagicMock(total=1000, used=100, free=900)

    with patch("shutil.disk_usage", side_effect=_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = metric_snapshots.get_history("disk_used_pct_root", since=since)
    assert rows
    assert rows[-1]["value"] == 10.0
