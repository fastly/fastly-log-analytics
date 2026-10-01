"""Guards the recency window used by ``scripts/verify_dashboard.js``.

The verifier's "recent ingest" liveness checks (Dashboard-recent and
RUM-recent) assert that freshly seeded edge traffic has flowed all the way
through the pipeline. That window must be:

1. **Symmetric** — requests and RUM share ONE pipeline (identical
   ``latest_log_at`` in steady state), so both recency checks must use the
   same window. An asymmetric window is what made RUM-recent fail at 0 while
   Dashboard-recent passed purely on seeding timing.
2. **At or above the pipeline latency floor** — edge→FOS→discovery→commit is
   6–15 min (FOS delivery 1–5 min + up-to-5-min commit cadence, worse under
   verify-phase CPU load). A window below that floor is structurally empty
   regardless of ingest health, producing flaky false failures.

This parses the JS source (same approach as
``tests/utils/test_insights_defaults.py``) so a drift back to a sub-floor or
asymmetric window fails here instead of only surfacing as a flaky deploy.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"

# The verifier's recency window must cover the measured pipeline latency floor.
_MIN_RECENT_MINUTES = 10


def _recent_range_token() -> str:
    """The shared ``RECENT_RANGE`` constant value (e.g. ``"15m"``)."""
    src = _VERIFY_JS.read_text()
    m = re.search(r'RECENT_RANGE\s*=\s*"(\d+m)"', src)
    assert m, 'verify_dashboard.js must define a RECENT_RANGE = "<n>m" constant'
    return m.group(1)


def _recent_range_reference_count() -> int:
    """How many ``range`` checks use the shared ``RECENT_RANGE`` constant."""
    src = _VERIFY_JS.read_text()
    return len(re.findall(r'"range",\s*RECENT_RANGE\b', src))


def _minutes(token: str) -> int:
    return int(token[:-1])


def test_verify_dashboard_has_two_recency_checks() -> None:
    count = _recent_range_reference_count()
    assert count >= 2, (
        "expected at least two recency checks (Dashboard-recent + RUM-recent) "
        f"to use the shared RECENT_RANGE constant; found {count}"
    )


def test_recency_window_is_symmetric_across_requests_and_rum() -> None:
    # A single shared constant is symmetric by construction, but assert there
    # is no stray literal minute window that would reintroduce asymmetry.
    src = _VERIFY_JS.read_text()
    stray = re.findall(r'"range",\s*"(\d+m)"', src)
    assert not stray, (
        "recency windows must flow through the shared RECENT_RANGE constant; "
        f"found hardcoded minute window(s) {stray!r}"
    )


def test_recency_window_meets_pipeline_latency_floor() -> None:
    token = _recent_range_token()
    assert _minutes(token) >= _MIN_RECENT_MINUTES, (
        f"recency window {token!r} is below the {_MIN_RECENT_MINUTES}m "
        "pipeline latency floor — a window this short is structurally "
        "empty regardless of ingest health"
    )
