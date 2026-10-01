"""Guards the transient-blip tolerance in ``scripts/verify_dashboard.js``.

The verifier runs 4 environments' full browser verifications in parallel on a
shared 6-CPU Colima while the harness is still seeding continuously and the
audit watcher samples — a self-induced verify-phase load spike. Two KNOWN
self-healing conditions happen in that window with no code defect:

  * a 503 from the documented DuckLake cold-start attach race (AGENTS.md Trap
    #35) and K8s rollout readiness, which React Query retries and the backend
    self-heals within seconds; and
  * ``net::ERR_CONNECTION_REFUSED`` / ``ERR_ABORTED`` from a transient K8s
    port-forward drop that the harness healer re-establishes.

Instant-failing the whole env on the FIRST such blip is what made 4 otherwise
passing environments go red for 4 different single-blip reasons. The verifier
must absorb a small BOUNDED number of these transient blips instead, while the
positive per-section checks (which retry and assert real rendered data) remain
the real pass/fail arbiter — so a PERSISTENTLY broken env still fails and the
tolerance cannot turn a real outage green.

This parses the JS source (same approach as
``test_verify_dashboard_recency.py``) so a regression back to instant-fatal
single-blip handling fails here instead of only as a flaky deploy.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


def _src() -> str:
    return _VERIFY_JS.read_text()


def test_defines_a_small_bounded_transient_budget() -> None:
    src = _src()
    m = re.search(r"TRANSIENT_ERROR_BUDGET\s*=\s*(\d+)", src)
    assert m, "verify_dashboard.js must define a TRANSIENT_ERROR_BUDGET constant"
    budget = int(m.group(1))
    assert 1 <= budget <= 20, (
        f"TRANSIENT_ERROR_BUDGET={budget} must be small/bounded so a persistent outage still exhausts it and fails fast"
    )


def test_503_is_treated_as_transient_not_instant_fatal() -> None:
    # The response listener must route 503 (documented self-healing cold-start /
    # rollout window) through the bounded-transient path, NOT instant-exit.
    src = _src()
    assert re.search(r"status\s*===\s*503", src), (
        "verify_dashboard.js must special-case status 503 as a transient self-healing condition (AGENTS.md Trap #35)"
    )


def test_connection_refused_is_treated_as_transient() -> None:
    src = _src()
    assert "ERR_CONNECTION_REFUSED" in src, (
        "verify_dashboard.js must recognize net::ERR_CONNECTION_REFUSED as a "
        "transient K8s port-forward blip, not an instant-fatal console error"
    )


def test_non_503_api_errors_remain_instant_fatal() -> None:
    # A 4xx (auth/tenancy) or a persistent 500/502/504 must still fail the run —
    # tolerance is scoped to 503 only on the response path.
    src = _src()
    # The listener still fails on >= 400; assert it does not blanket-tolerate
    # every >= 400 as transient (which would mask real auth/5xx regressions).
    assert not re.search(r"status\s*>=\s*400[^\n]*transient\s*=\s*true", src), (
        "all >=400 responses must not be blanket-marked transient; only 503 is"
    )
