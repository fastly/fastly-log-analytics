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


def test_network_30d_navigation_tolerates_portforward_resets() -> None:
    # The Network 30d step issues a raw goto inside its retry loop. On the K8s
    # high-scale env the port-forward drops mid-navigation (net::ERR_CONNECTION_RESET
    # / goto Timeout), so the page never loads and the step falsely reports
    # "No data available" from a blank page even though the network-health
    # backend returns has_data=True in ~0.4s. Navigation must go through the
    # bounded retry helper (which absorbs a reset with backoff before giving
    # up) exactly like RUM 30d.
    src = _src()
    assert "gotoWithShellReady(networkPage, networkUrl30d" in src, (
        "Network 30d navigation must go through the bounded retry helper so a "
        "transient port-forward reset is retried, not reported as missing data"
    )
    assert not re.search(r"networkPage\.goto\(networkUrl30d", src), (
        "Network 30d must not keep the un-retried single-shot goto that dies on a port-forward reset"
    )


def test_rum_24h_vitals_render_is_polled_not_blind_slept() -> None:
    # The RUM 24h step used a blind waitForTimeout(4000) before reading the
    # body, then declared vitals "missing" if the heavy Plotly vitals cards had
    # not painted within that fixed 4s. Under the verify-phase CPU spike (4 envs
    # rendering in parallel on a shared 6-CPU Colima) the data is present and
    # the analytics endpoint answers in ~0.5s, but the client render lags past
    # 4s and the step false-fails. It must POLL for the vitals title+rating to
    # actually render (returning immediately once painted) with a generous
    # bounded timeout, not sleep a fixed 4s and hope.
    src = _src()
    m = re.search(r"RUM_RENDER_SETTLE_MS\s*=\s*(\d+)", src)
    assert m, "verify_dashboard.js must define a named RUM_RENDER_SETTLE_MS render-poll budget"
    assert int(m.group(1)) >= 8000, (
        "the RUM vitals render-poll budget must be generous enough to absorb the verify-phase CPU spike (>=8s)"
    )
    assert "RUM_RENDER_SETTLE_MS" in src and src.count("waitForFunction") >= 1, (
        "RUM 24h must waitForFunction-poll for the vitals render using RUM_RENDER_SETTLE_MS"
    )
    assert not re.search(r"rumPage\.waitForTimeout\(4000\)", src), (
        "RUM 24h must not keep the blind 4s settle that false-fails vitals render under load"
    )
