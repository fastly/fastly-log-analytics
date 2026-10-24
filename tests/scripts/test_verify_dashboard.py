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


# ── Merged from test_verify_dashboard_nav_resilience.py ──

from pathlib import Path


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
    assert "gotoWithShellReady(networkPage, networkUrl30d" in src or (
        "gotoWithShellReady(routePage, targetUrl" in src and "routePath: '/network'" in src
    ), (
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


# ── Merged from test_verify_dashboard_recency.py ──

from pathlib import Path

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


def test_verifier_does_not_claim_a_multiminute_pipeline_floor() -> None:
    # Ingest is near-real-time (measured ~1-8s buffer-stitched, 2026-10-01).
    # The recency window is a jitter cushion, NOT a pipeline latency floor.
    # A comment asserting a multi-minute "floor" is a false premise that
    # misleads future readers into widening the window to mask staleness.
    src = _VERIFY_JS.read_text()
    assert not re.search(r"\d+\s*-\s*\d+\s*min.*floor|floor.*\d+\s*-\s*\d+\s*min", src), (
        "verify_dashboard.js must not describe the recency window as a "
        "multi-minute pipeline floor — ingest is real-time; the window is a "
        "jitter cushion (see module docstring)"
    )


# ── Merged from test_verify_dashboard_resource_classification.py ──

from pathlib import Path


def test_generic_console_resource_failure_echo_is_not_fatal() -> None:
    src = _src()
    assert re.search(r"GENERIC_RESOURCE_FAILURE_RE", src), (
        "verify_dashboard.js must recognize Chrome's generic, URL-less "
        "'Failed to load resource: ...' console echo and not route it through "
        "failOrTolerate -- it carries no URL, so it cannot be reliably "
        "classified as critical vs. cosmetic from its text alone"
    )


def test_critical_resource_classification_is_url_based() -> None:
    src = _src()
    assert "function isCriticalResourceUrl" in src, (
        "verify_dashboard.js must classify a failed resource as critical "
        "(fails the run) vs. cosmetic (logged only) by its actual request "
        "URL (/api/, /_next/data/), not by matching filename substrings in "
        "console text, which breaks for content-hashed asset names"
    )


def test_requestfailed_listener_fails_only_critical_urls() -> None:
    # A connection-level failure (ERR_CONNECTION_RESET etc.) never reaches the
    # 'response' listener at all, since no response was ever received -- the
    # requestfailed listener must be the one applying the critical-vs-cosmetic
    # URL check for that class, or a reset API call could silently pass.
    src = _src()
    assert re.search(r"requestfailed.*?isCriticalResourceUrl", src, re.S), (
        "the 'requestfailed' listener must gate failOrTolerate on "
        "isCriticalResourceUrl(request.url()), so a connection-level reset "
        "on a real API call still fails the run while a static-asset reset "
        "does not"
    )


def test_woff2_substring_filter_removed_in_favor_of_url_classification() -> None:
    # The old substring filter is no longer needed (and would be redundant/
    # misleading) now that classification happens on the actual URL -- its
    # presence would suggest the text-based guess is still load-bearing.
    src = _src()
    assert "text.includes('woff2')" not in src, (
        "the filename-substring 'woff2' check should be removed now that "
        "static-asset vs. API classification happens via isCriticalResourceUrl "
        "on the real request URL, not by guessing from console text"
    )


def test_telemetry_beacons_are_exempt_from_critical_resource_urls() -> None:
    # Client-side observational telemetry beacons (/api/web-vitals, /api/ux-events)
    # are non-critical: dropped beacons under load do not compromise data correctness.
    src = _src()
    assert "/api/web-vitals" in src and "/api/ux-events" in src, (
        "isCriticalResourceUrl in verify_dashboard.js must exempt /api/web-vitals and /api/ux-events"
    )


# ── Merged from test_verify_dashboard_rum_charts.py ──

import json
import subprocess
from pathlib import Path

import pytest

_SOURCE = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


@pytest.mark.parametrize(
    ("charts", "expected"),
    [
        ([], False),
        ([{"width": 524, "height": 300, "data": [{"x": [], "y": []}]}], False),
        ([{"width": 1, "height": 1, "data": [{"x": [0], "y": [0]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [0]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"values": [10]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [None]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": ["10"]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [float("nan")]}]}], False),
    ],
)
def test_rum_chart_requires_real_geometry_and_points(charts: list[dict], expected: bool) -> None:
    source = _SOURCE.read_text()
    start = source.index("function hasPopulatedRumChart()")
    end = source.index("\n}", start) + 2
    script = (
        source[start:end]
        + "\nconst charts = "
        + json.dumps(charts)
        + ";\nglobal.document = {querySelectorAll: selector => {"
        + "if (selector !== 'main .js-plotly-plot') throw Error('Unscoped chart selector');"
        + "return charts.map(c => ({data:c.data,getBoundingClientRect:()=>c}));}};"
        + "\nconsole.log(JSON.stringify(hasPopulatedRumChart()));"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) is expected


def test_remote_rum_chart_failure_cannot_warn_and_proceed() -> None:
    source = _SOURCE.read_text()
    assert "waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 })" in source
    assert "No populated full-sized RUM chart" in source
    assert "No visible Plotly charts found on the RUM page for remote cloud environment" not in source


# ── Merged from test_verify_dashboard_transient_tolerance.py ──

from pathlib import Path

_CLASSIFIER_JS = Path(__file__).resolve().parents[2] / "scripts" / "lib" / "console_blip.js"


def _classifier_src() -> str:
    return _CLASSIFIER_JS.read_text()


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


def test_err_failed_is_treated_as_transient() -> None:
    # A heavy 30d page load whose in-flight request is aborted/reset during the
    # parallel-verify load spike surfaces in Chromium as the generic
    # net::ERR_FAILED rather than ERR_ABORTED/ERR_CONNECTION_REFUSED. It is the
    # same self-healing class the dashboard path already tolerates; RUM must get
    # the same bounded tolerance or it alone goes red on an identical blip.
    classifier = _classifier_src()
    assert "ERR_FAILED" in classifier, (
        "console_blip.js isTransientConsoleBlip must recognize net::ERR_FAILED "
        "so RUM gets the same bounded-transient tolerance as the dashboard path"
    )


def test_chunk_load_error_is_treated_as_transient() -> None:
    # A dropped kubectl port-forward refuses the chunk fetch; Next.js surfaces
    # that identical network failure as a ChunkLoadError, which the healer's
    # tunnel re-establish resolves within seconds. It is the same self-healing
    # class as ERR_CONNECTION_REFUSED (its direct cause), so it must get the same
    # bounded tolerance instead of instant-failing the whole env.
    classifier = _classifier_src()
    assert "ChunkLoadError" in classifier or "Failed to load chunk" in classifier, (
        "console_blip.js isTransientConsoleBlip must recognize ChunkLoadError / "
        "'Failed to load chunk' as the same transient port-forward blip as "
        "ERR_CONNECTION_REFUSED (its direct cause)"
    )


def test_verify_js_uses_shared_classifier() -> None:
    # The classifier is extracted to a single requireable module so the pure
    # classification can be unit-tested; verify_dashboard.js must consume it
    # rather than re-inlining a drifting copy of the regex.
    src = _src()
    assert "require('./lib/console_blip')" in src or 'require("./lib/console_blip")' in src, (
        "verify_dashboard.js must import isTransientConsoleBlip from the shared "
        "scripts/lib/console_blip.js module (single source of truth)"
    )
