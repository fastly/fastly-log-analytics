"""Guards the Elevation port-forward blip investigation instrumentation.

The Remote High-Scale (Elevation) RUM-30d check intermittently hits a handful
of transient console/network blips (ERR_CONNECTION_RESET et al) even though
measurement showed the bound pod never restarts and the harness healer never
respawns the tunnel -- the reset happens inside an otherwise-healthy
port-forward. Diagnosing that needs wall-clock-correlatable logging here
(timestamps on every console/network event, plus Playwright's own
request-failure detail) so a future run's browser-side log lines can be lined
up second-for-second against the harness's port-forward stderr capture.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


def _src() -> str:
    return _VERIFY_JS.read_text()


def test_console_log_lines_are_timestamped() -> None:
    src = _src()
    assert re.search(r"function ts\(\)", src), (
        "verify_dashboard.js must define a ts() timestamp helper so console/network "
        "log lines can be correlated against the harness's port-forward logs"
    )
    assert "`${ts()} [Browser Console]" in src, "[Browser Console] log lines must be timestamped"


def test_request_failures_are_logged_with_error_detail() -> None:
    src = _src()
    assert "requestfailed" in src, (
        "verify_dashboard.js must listen for Playwright's 'requestfailed' event -- "
        "it carries the exact URL and error code independent of whatever text the "
        "page happened to console.log, which is the cleaner signal for diagnosing "
        "which upstream (frontend vs backend port-forward) actually dropped"
    )
