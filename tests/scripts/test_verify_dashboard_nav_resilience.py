"""Guards navigation resilience in ``scripts/verify_dashboard.js``.

The RUM 30d consistency check (and other page navigations) must tolerate the
harness's self-induced verify-phase CPU spike: 4 environments render in
parallel on a shared 6-CPU Colima while seeding continues. The page shell can
take well over 10s to paint ``main`` under that load even though the data is
already present and queryable.

A single ``goto`` + ``waitForSelector('main', 10000)`` with NO retry is what
made Local Standard fail its RUM 30d step on a slow paint. Navigation must be
retried a bounded number of times with a shell timeout generous enough to
absorb the spike — without masking a genuinely dead page (retries exhaust and
the run still fails).

Parses the JS source so a regression back to a single tight navigation fails
here instead of only as a flaky deploy.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


def _src() -> str:
    return _VERIFY_JS.read_text()


def test_defines_a_bounded_navigation_retry_helper() -> None:
    src = _src()
    assert "gotoWithShellReady" in src, (
        "verify_dashboard.js must define a bounded navigation retry helper "
        "(gotoWithShellReady) so a slow shell paint under verify-phase load is "
        "retried, not instant-failed"
    )
    m = re.search(r"attempts\s*=\s*(\d+)", src)
    assert m and 2 <= int(m.group(1)) <= 6, "the navigation retry helper must retry a small bounded number of times"


def test_rum_30d_navigation_shell_wait_tolerates_load() -> None:
    # The RUM 30d shell wait must allow more than the old tight 10s, which
    # flapped under the verify-phase CPU spike.
    src = _src()
    assert "gotoWithShellReady(rumPage, rumUrl30d" in src, "RUM 30d navigation must go through the bounded retry helper"
    # The 30d path must not keep the old single-shot direct goto (the retry
    # helper owns navigation now). Other RUM sections keep their 10s shell wait
    # because they already sit inside 4-attempt retry loops.
    assert not re.search(r"rumPage\.goto\(rumUrl30d", src), "RUM 30d must not keep the un-retried single-shot goto"
