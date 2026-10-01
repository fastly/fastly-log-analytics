"""Guards the recency window used by ``scripts/verify_dashboard.js``.

The verifier's "recent ingest" liveness checks (Dashboard-recent and
RUM-recent) assert that freshly seeded edge traffic is queryable right now.
Ingest is near-real-time: measured 2026-10-01, both requests (~8s) and RUM
beacons (~1s) are served from the buffer-stitched DuckLake view and become
queryable within seconds of the edge request — NOT gated on the 5-min commit
tick (the stitched view serves pre-commit buffer data). So the window is NOT a
"pipeline latency floor"; the pipeline has no multi-minute floor.

What the window must be:

1. **Symmetric** — requests and RUM share ONE pipeline and the same
   buffer-stitched view pattern (identical ``latest_log_at`` in steady state),
   so both recency checks must use the same window. An asymmetric window is
   what made RUM-recent fail at 0 while Dashboard-recent passed purely on
   seeding timing.
2. **Wide enough to absorb edge→FOS delivery + verify-phase jitter** — the one
   real source of lag is Fastly batching log delivery to FOS (seconds to a
   couple minutes) plus transient ingest slowdown under the verify-phase CPU
   spike on shared Colima. A modest floor keeps the check from flapping on that
   jitter. The actual freshness guarantee comes from the harness seeding
   CONTINUOUSLY through verify (a real user streaming logs), so the window
   always holds seconds-fresh data — the window size is only a jitter cushion,
   not an endorsement of staleness.

This parses the JS source (same approach as
``tests/utils/test_insights_defaults.py``) so a drift back to a sub-floor or
asymmetric window fails here instead of only surfacing as a flaky deploy.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"

# Minimum window that reliably absorbs edge->FOS delivery + verify-phase CPU
# jitter without flapping. Not a pipeline-latency floor — ingest is real-time.
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


def test_recency_window_absorbs_edge_delivery_jitter() -> None:
    token = _recent_range_token()
    assert _minutes(token) >= _MIN_RECENT_MINUTES, (
        f"recency window {token!r} is below the {_MIN_RECENT_MINUTES}m jitter "
        "cushion — edge->FOS delivery batching + verify-phase CPU spikes can "
        "briefly empty a window this short and flap the check"
    )
