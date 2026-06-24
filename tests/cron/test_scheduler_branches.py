"""Defensive-branch coverage for backend/cron/scheduler.py helpers.

Targets the pure functions and simple error paths that the existing
test_scheduler.py / test_*_job.py suites don't exercise."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from backend.cron import scheduler as sched

# ── dev_mode_no_crons: env-var gating ──────────────────────────────────────


@pytest.mark.parametrize(
    "env_val,expected",
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("TRUE", True),
        ("0", False),
        ("", False),
        ("no", False),
        ("anything-else", False),
    ],
)
def test_dev_mode_no_crons_recognises_truthy_values(env_val, expected, monkeypatch):
    """The env kill-switch accepts 1/true/yes (case-insensitive). Pinned
    because misspelling FLA_DEV_NO_CRONS=enabled would silently NOT
    disable crons — exactly the failure mode this env var exists to
    prevent."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", env_val)
    assert sched.dev_mode_no_crons() is expected


def test_dev_mode_no_crons_false_when_unset(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    assert sched.dev_mode_no_crons() is False


# ── _display_name / _display_label ─────────────────────────────────────────


def test_display_name_prefers_service_name():
    src = {"service_name": "Friendly Name", "name": "svc"}
    assert sched._display_name(src, "fallback") == "Friendly Name"


def test_display_name_falls_back_to_name():
    src = {"name": "svc-id"}
    assert sched._display_name(src, "fallback") == "svc-id"


def test_display_name_uses_explicit_fallback_when_both_missing():
    assert sched._display_name({}, "fallback") == "fallback"


def test_display_label_collapses_when_name_matches_id():
    """When service_name == service_id, the label is just the id —
    avoids ``svc-1 (svc-1)`` double-print."""
    src = {"service_name": "svc-1", "name": "svc-1"}
    assert sched._display_label(src, "svc-1") == "svc-1"


def test_display_label_includes_both_when_distinct():
    src = {"service_name": "My Service", "name": "svc-1"}
    assert sched._display_label(src, "svc-1") == "My Service (svc-1)"


# ── _claim_heavy_refresh: per-service throttle ─────────────────────────────


def test_claim_heavy_refresh_first_call_wins():
    """First call for a service in a fresh window claims and returns True."""
    # Use a unique service_id so other tests can't pollute.
    sid = "test-claim-heavy-1"
    sched._last_heavy_refresh.pop(sid, None)
    assert sched._claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_returns_false_when_throttled():
    """Second call within the throttle window returns False — line 108."""
    sid = "test-claim-heavy-2"
    sched._last_heavy_refresh[sid] = time.time()  # just claimed
    assert sched._claim_heavy_refresh(sid) is False


def test_claim_heavy_refresh_reopens_window_after_interval():
    sid = "test-claim-heavy-3"
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC - 10
    assert sched._claim_heavy_refresh(sid) is True


# ── _service_has_alerts: defaults to True on error ─────────────────────────


def test_service_has_alerts_returns_count_gt_zero():
    with patch("backend.core.metadata.count_alerts", return_value=3):
        assert sched._service_has_alerts("svc") is True


def test_service_has_alerts_returns_false_when_count_zero():
    with patch("backend.core.metadata.count_alerts", return_value=0):
        assert sched._service_has_alerts("svc") is False


def test_service_has_alerts_defaults_to_true_on_exception():
    """Line 128-129: if count_alerts raises (corrupt SQLite, etc.),
    fail-safe to True so the cron isn't silently disabled."""
    with patch("backend.core.metadata.count_alerts", side_effect=RuntimeError("corrupt")):
        assert sched._service_has_alerts("svc") is True


# ── _extract_log_text: None / empty branches ───────────────────────────────


def test_extract_log_text_returns_empty_for_none_run_id():
    """Line 147: a None run_id (start_cron_run failed) → empty string,
    never calls get_progress."""
    with patch("backend.cron_progress.get_progress") as mock_gp:
        assert sched._extract_log_text(None) == ""
    mock_gp.assert_not_called()


def test_extract_log_text_returns_empty_when_progress_empty():
    """An empty event list short-circuits to '' (avoids producing a
    bare '\\n'-only string)."""
    with patch("backend.cron_progress.get_progress", return_value=[]):
        assert sched._extract_log_text(42) == ""


# ── _elapsed_since: pretty-print branches ──────────────────────────────────


def test_elapsed_since_sub_minute():
    """Under 60s renders as ``Xs`` (one-decimal seconds)."""
    out = sched._elapsed_since(time.time() - 5)
    assert out.endswith("s") and "m" not in out


def test_elapsed_since_over_minute():
    """≥60s renders as ``XmYYs`` (minutes + two-digit zero-padded seconds)."""
    out = sched._elapsed_since(time.time() - 125)
    # Either "2m05s" or similar; assert the m+s shape.
    assert "m" in out
    assert out.endswith("s")


# ── _check_disk_space: OSError + low-space + happy ─────────────────────────


def test_check_disk_space_returns_true_on_oserror():
    """Line 276-279: shutil.disk_usage raising is treated as 'don't
    block' — let the job try and fail naturally if disk really is bad."""
    with patch("shutil.disk_usage", side_effect=OSError("permission")):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_disk_space_blocks_when_below_hard_floor():
    """A near-empty disk → block the cron with a clear message."""
    usage = MagicMock(total=1_000_000_000, free=50_000_000)  # 5% free, well below 10% floor
    with (
        patch("shutil.disk_usage", return_value=usage),
        patch("os.path.isdir", return_value=True),
    ):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is False
    assert "disk almost full" in msg


def test_check_disk_space_happy_path_returns_ok():
    """Plenty of free space → (True, "") proceeds normally."""
    usage = MagicMock(total=1_000_000_000_000, free=500_000_000_000)  # 50% free
    with (
        patch("shutil.disk_usage", return_value=usage),
        patch("os.path.isdir", return_value=True),
    ):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is True
    assert msg == ""


# ── _check_buffer_backlog: thresholds + exception ──────────────────────────


def test_check_buffer_backlog_returns_empty_on_zero_files():
    """No files → no problem → empty string."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0},
    ):
        assert sched._check_buffer_backlog({"name": "s"}, "s", 5) == ""


def test_check_buffer_backlog_returns_empty_when_below_all_thresholds():
    """Few files, small bytes, fresh oldest → no warning. Line 329, 331."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1024, "oldest_age_seconds": 30},
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert out == ""


def test_check_buffer_backlog_warns_on_file_count():
    """Crossing the file-count threshold yields a warning string."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": sched._BACKLOG_FILE_COUNT_WARN + 100,
            "total_bytes": 0,
            "oldest_age_seconds": 0,
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "buffer backlog" in out
    assert "files" in out


def test_check_buffer_backlog_warns_on_oldest_age():
    """oldest_age > 3x commit_interval → warning."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": 10,
            "total_bytes": 0,
            "oldest_age_seconds": 60 * 60,  # 1 hour
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "oldest" in out


def test_check_buffer_backlog_warns_on_total_bytes():
    """>1 GiB un-committed → warning."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": 10,
            "total_bytes": sched._BACKLOG_BYTES_WARN + 1,
            "oldest_age_seconds": 0,
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "MB on disk" in out


def test_check_buffer_backlog_returns_empty_on_probe_exception():
    """If the buffer probe itself fails, we annotate nothing — backlog
    probing must NEVER fail the commit, only annotate it (line 314-316)."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        side_effect=RuntimeError("disk gone"),
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert out == ""
