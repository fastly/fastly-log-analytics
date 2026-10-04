"""Guards the static-asset-vs-API classification in ``scripts/verify_dashboard.js``.

Measured 2026-10-04: a content-hashed static asset
(``/_next/static/media/<hash>.woff2``) reset mid-transfer on the Elevation
tunnel. Chrome's console echoes every failed resource load as the generic,
URL-less ``Failed to load resource: ...`` line regardless of whether it was a
font or an API call, so the previous filename-substring filter (checking for
``"woff2"`` in the console TEXT) never matched and the whole run failed over a
single decorative font byte -- even though the page's actual data had already
verified correctly.

The fix classifies failures by the real request URL (via the 'requestfailed'
and 'response' listeners, which Playwright gives the URL for) instead of by
guessing from console text, and drops the URL-blind console echo entirely so
it can't re-introduce a fatal false positive for any future unrecognized
static-asset filename pattern.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


def _src() -> str:
    return _VERIFY_JS.read_text()


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
