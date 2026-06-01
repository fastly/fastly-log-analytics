"""vcrpy-driven NGWAF client tests.

The existing [test_ngwaf.py](test_ngwaf.py) tests use MagicMock to fake
``urlopen``. That works, but it relies on the test asserting the exact
call shape (headers, kwargs) the mock expects, so any benign refactor of
the underlying transport — adding a User-Agent header, changing the
timeout argument, wrapping ``urlopen`` in a retry helper — silently
drifts past the assertions.

vcrpy hooks the socket layer, so it survives all of those: it replays
bytes-on-the-wire from a YAML cassette regardless of how the transport
above it is shaped. Cassettes are committed to ``tests/cassettes/``.

The plan (TESTING_PLAN_3 item 8) is gradual migration — these vcrpy
tests run alongside the MagicMock tests, not as a replacement. They
pin the wire-level contract (URL shape, headers, response handling)
which complements what the MagicMock tests pin at the call level.

Re-recording a cassette: delete it, then run with
``record_mode='new_episodes'`` against a real NGWAF workspace. Scrub the
``Fastly-Key`` header before committing.
"""

from __future__ import annotations

import os

import pytest
import vcr

from backend.utils.ngwaf import fetch_verified_bots_paged

CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "..", "cassettes")

# record_mode='none' means: refuse to make a real network call. If the
# cassette doesn't cover the request, the test errors with a clear
# "no match found" — much better than silently hitting prod.
_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    # filter_headers removes secrets from any *new* recording. Existing
    # cassettes are committed with a placeholder Fastly-Key already.
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def test_single_page_yields_two_verified_bots():
    """One page with 2 VERIFIED-BOT signals + 1 non-bot record. The
    extractor must filter the non-bot record out and yield (records,
    latest_ts, raw_page_count) = (2, max-ts, 3)."""
    with _my_vcr.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        pages = list(
            fetch_verified_bots_paged(
                api_key="test-token-not-real",
                workspace_id="ws-test-1",
                from_ts="-1h",
                page_limit=500,
            )
        )

    assert len(pages) == 1, f"expected exactly one page, got {len(pages)}"
    records, latest_ts, raw_count = pages[0]

    assert raw_count == 3, "raw_count should include all records pre-filter"
    assert len(records) == 2, f"only 2 of 3 records have VERIFIED-BOT signal; got {len(records)}: {records!r}"

    # Extraction should pull bot_name from the verified signal's `value`.
    bot_names = sorted(r["bot_name"] for r in records)
    assert bot_names == ["bingbot", "googlebot"]

    # Category comes from the VERIFIED-BOT.<SUBCAT> sibling signal.
    categories = sorted(r["category"] for r in records)
    assert categories == ["SEARCH-ENGINE", "SEARCH-ENGINE"]

    # latest_ts must be the max across yielded (filtered) records.
    assert latest_ts == "2026-05-26T10:05:00Z"


def test_cassette_no_match_raises_loudly():
    """If the URL shape drifts (e.g. someone removes the ``limit`` param
    or changes the path), the request won't match the cassette and
    vcrpy raises. This is the *point* of using vcrpy — call-shape
    regressions become test failures."""
    with _my_vcr.use_cassette("ngwaf_verified_bots_single_page.yaml"):
        # Different workspace_id → different URL path → no cassette match.
        with pytest.raises(Exception) as excinfo:
            list(
                fetch_verified_bots_paged(
                    api_key="test-token-not-real",
                    workspace_id="ws-DIFFERENT",
                    from_ts="-1h",
                    page_limit=500,
                )
            )
        # vcrpy raises CannotOverwriteExistingCassetteException; we
        # don't import it (private path) but the message is stable.
        assert "no match" in str(excinfo.value).lower() or "cannot" in str(excinfo.value).lower(), (
            f"expected vcrpy refusal, got: {excinfo.value!r}"
        )
