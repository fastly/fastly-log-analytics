"""Tests for ``backend.routers.dashboard``.

The dashboard router is thin — each endpoint dispatches to
``backend.repositories.dashboard`` (already covered) — but the
``/bundle`` composite has real logic for sub-response stitching that
the dedicated /aggregates + /top-bots paths don't exercise.

Repository functions are stubbed so the tests focus on the router's
own choices: HTTP shape, composite stitching, debug-key lifting, the
fields-filter short-circuit on top_bots, and CSV response packaging.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def stub_aggregates(monkeypatch):
    """Replace the repository's get_aggregates with a deterministic stub.

    Shape matches ``AggregatesResponse`` (BaseResponse subclass) so the
    typed response_model on /api/dashboard/bundle (finding 013) validates
    cleanly. ``DebugCall``/``DebugQuery`` schemas are populated so the
    debug-key lifting assertion below still has rows to count.
    """
    stub = MagicMock(
        return_value={
            "data": {},
            "time_series": [],
            "map_data": [],
            "where_clause": "1=1",
            "interval": "minute",
            "metric": "requests",
            "total_rows": 100,
            "total_rows_total": 200,
            "section_timings": [{"section": "agg:query", "time_ms": 5.0}],
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "rows": 1, "caller": "agg_caller"}],
            "debug_calls": [
                {
                    "service": "fastly",
                    "method": "GET",
                    "path": "/stats",
                    "time_ms": 0.5,
                    "caller": "agg_caller",
                }
            ],
        }
    )
    monkeypatch.setattr("backend.repositories.dashboard.get_aggregates", stub)
    return stub


@pytest.fixture
def stub_top_bots(monkeypatch):
    stub = MagicMock(
        return_value={
            "bots": [{"name": "Googlebot", "count": 42}],
            "ngwaf_bots": [],
            "section_timings": [{"section": "bots:query", "time_ms": 3.0}],
            "debug_queries": [{"sql": "SELECT bot", "time_ms": 0.5, "rows": 1, "caller": "bots_caller"}],
            "debug_calls": [],
        }
    )
    monkeypatch.setattr("backend.repositories.security.get_top_bots", stub)
    return stub


# ── /api/dashboard/bundle ─────────────────────────────────────────────────────


def test_bundle_returns_both_subresponses(client, stub_aggregates, stub_top_bots):
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "aggregates" in body
    assert "top_bots" in body
    # Composite emits its own top-level _section_timings tracking both
    # sub-queries' wall-clock.
    assert "_section_timings" in body
    sections = [s["section"] for s in body["_section_timings"]]
    assert "bundle:aggregates" in sections
    assert "bundle:top_bots" in sections


def test_bundle_always_fetches_top_bots_even_when_fields_filter_excludes(client, stub_aggregates, stub_top_bots):
    """The dashboard always renders the two bot cards independent of
    which other top-N cards the lazy fields list is hydrating. The
    previous short-circuit (skip when fields excludes _bot_name /
    _ngwaf_bot_name) fired on every lazy load and seeded the React
    Query cache with empty bot arrays — leaving the dashboard cards
    visually blank even though the backend had bot rows available.
    Pin the always-fetch behavior so the short-circuit can't quietly
    come back."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "fields": ["country", "url"],  # no _bot_name / _ngwaf_bot_name
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    # The stub-returned top_bots payload is reflected back — the router
    # doesn't substitute an empty placeholder. Note: with BundleResponse
    # response_model (finding 013), Pydantic strips debug_queries /
    # debug_calls when DEBUG_RESPONSES is off and renames section_timings
    # → _section_timings via serialization_alias, so we compare structure
    # not full equality.
    stub_top_bots.assert_called_once()
    assert body["top_bots"]["bots"] == stub_top_bots.return_value["bots"]
    assert body["top_bots"]["ngwaf_bots"] == stub_top_bots.return_value["ngwaf_bots"]
    stub_aggregates.assert_called_once()


def test_bundle_calls_top_bots_when_bot_field_requested(client, stub_aggregates, stub_top_bots):
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "fields": ["country", "_bot_name"],
        },
    )
    assert resp.status_code == 200
    stub_top_bots.assert_called_once()


def test_bundle_lifts_debug_keys_into_top_level(client, stub_aggregates, stub_top_bots, monkeypatch):
    """Debug keys from both sub-responses are lifted to the top-level
    BundleResponse. Pinned because the frontend DebugPanel reads
    response.data._debug_queries (the serialization_alias). With
    DEBUG_RESPONSES set the lifted lists survive Pydantic's
    BaseResponse._strip_debug_when_disabled serializer; without it
    they're correctly redacted (finding 013)."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    body = resp.json()
    assert "_debug_queries" in body
    assert len(body["_debug_queries"]) == 2  # one from aggregates, one from top_bots
    sql_texts = [q["sql"] for q in body["_debug_queries"]]
    assert "SELECT 1" in sql_texts
    assert "SELECT bot" in sql_texts


def test_bundle_response_model_redacts_debug_when_flag_off(client, stub_aggregates, stub_top_bots, monkeypatch):
    """Finding 013: the pre-fix composite returned an untyped dict that
    bypassed BaseResponse._strip_debug_when_disabled, leaking SQL
    queries + telemetry to clients regardless of DEBUG_RESPONSES. With
    BundleResponse as the response_model, Pydantic's wrap-serializer
    now redacts all *_debug_* keys when DEBUG_RESPONSES is unset.
    Verify the redaction landed at the top level AND in each
    sub-response."""
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    body = resp.json()
    # Top-level lift is redacted to an empty list (or missing entirely).
    assert not body.get("_debug_queries"), f"top-level _debug_queries leaked: {body.get('_debug_queries')}"
    assert not body.get("_debug_calls")
    # Sub-responses must not carry debug telemetry either.
    for sub_name in ("aggregates", "top_bots"):
        sub = body[sub_name]
        assert not sub.get("_debug_queries"), f"{sub_name} _debug_queries leaked: {sub.get('_debug_queries')}"
        assert not sub.get("_debug_calls"), f"{sub_name} _debug_calls leaked: {sub.get('_debug_calls')}"
        # Bare keys must also be absent (they're popped by the serializer).
        assert "debug_queries" not in sub
        assert "debug_calls" not in sub


def test_bundle_renames_subresponse_section_timings(client, stub_aggregates, stub_top_bots):
    """With BundleResponse response_model (finding 013), Pydantic's
    ``serialization_alias`` on BaseResponse.section_timings emits the
    underscored field name (``_section_timings``) for each sub-response,
    matching what the dedicated /aggregates and /top-bots endpoints
    return."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    body = resp.json()
    assert "_section_timings" in body["aggregates"]
    assert "section_timings" not in body["aggregates"]
    assert "_section_timings" in body["top_bots"]


# ── /api/dashboard/raw/csv ───────────────────────────────────────────────────


def test_raw_csv_returns_csv_attachment(client, monkeypatch):
    df = pd.DataFrame({"timestamp": ["2026-06-12T00:00:00Z"], "status": [200]})
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers.get("content-disposition", "")
    # Header + 1 data row.
    text = resp.text
    assert "timestamp,status" in text
    assert "200" in text


def test_raw_csv_returns_empty_body_when_no_rows(client, monkeypatch):
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: pd.DataFrame())

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )

    assert resp.status_code == 200
    assert resp.text == ""


def test_raw_csv_multi_chunk_emits_header_once(client, monkeypatch):
    """a5d6f6f rewrote the body to a 2000-row chunked generator with
    header=True only on the first slice. A regression that reset first=True
    inside the loop would emit the header in every chunk and silently
    corrupt every >2000-row export — the consuming spreadsheet would
    fail to parse mid-stream. Pin the multi-chunk contract."""
    df = pd.DataFrame(
        {"timestamp": [f"2026-06-12T00:{i // 60:02d}:{i % 60:02d}Z" for i in range(2500)], "status": [200] * 2500}
    )
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )

    assert resp.status_code == 200
    text = resp.text
    # Exactly ONE header line across the whole stream.
    assert text.count("timestamp,status") == 1
    # Header + 2500 data rows. Trailing newline yields one trailing empty
    # line; strip-then-split keeps the count honest.
    lines = [ln for ln in text.split("\n") if ln]
    assert len(lines) == 2501


# ── /api/dashboard/bundle + /aggregates sections selector ─────────────────────


def test_bundle_rejects_unknown_section(client, stub_aggregates, stub_top_bots):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    Literal). Either is an explicit reject so the FE never gets a
    silently-degraded 200."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["not_a_section"],
        },
    )
    assert resp.status_code in (400, 422)


def test_bundle_core_only_skips_top_bots_branch(client, stub_aggregates, stub_top_bots):
    """sections=['core'] runs the aggregates branch with the page-shape
    flags forced True and SKIPS top_bots entirely — proves the selector
    suppresses the un-asked-for branch (the load-bearing FE signal).
    Pinned because the FE's parallel /core + /bots fan-out depends on
    each request paying for only its own branch."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["core"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    stub_aggregates.assert_called_once()
    stub_top_bots.assert_not_called()
    assert body.get("top_bots") is None
    # The aggregates branch must have been invoked with the include_* flags
    # forced True (selector overrides any False the caller passed).
    kwargs = stub_aggregates.call_args.kwargs
    assert kwargs["include_time_series"] is True
    assert kwargs["include_conn_requests"] is True
    assert kwargs["include_map_data"] is True
    assert kwargs["include_top_n"] is False


def test_bundle_topten_only_keeps_top_n_drops_page_shape_blocks(client, stub_aggregates, stub_top_bots):
    """sections=['topten'] runs aggregates with the page-shape flags OFF
    and include_top_n=True. Skips top_bots. Critical: the topten section
    must keep the rollup fast-path WHOLE — get_aggregates downstream owns
    that, but the flags surface to the repo are what proves it here."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["topten"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    stub_aggregates.assert_called_once()
    stub_top_bots.assert_not_called()
    kwargs = stub_aggregates.call_args.kwargs
    assert kwargs["include_top_n"] is True
    assert kwargs["include_time_series"] is False
    assert kwargs["include_conn_requests"] is False
    assert kwargs["include_map_data"] is False
    assert body.get("top_bots") is None


def test_bundle_bots_only_skips_aggregates_branch(client, stub_aggregates, stub_top_bots):
    """sections=['bots'] runs ONLY the second-conn top_bots branch.
    Skips the aggregates SQL entirely. Pinned because this is the slice's
    headline parallel-fan-out win — the bots POST must not also pay for
    aggregates."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["bots"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    stub_top_bots.assert_called_once()
    stub_aggregates.assert_not_called()
    assert body.get("aggregates") is None
    assert body["top_bots"]["bots"] == [{"name": "Googlebot", "count": 42}]


def test_bundle_multi_section_core_plus_bots_fires_both(client, stub_aggregates, stub_top_bots):
    """sections=['core','bots'] is the two-branch path — both fire,
    asyncio.gather still runs, and the F015 second_cm guard is still in
    play. Equivalent to sections=None for branch coverage."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["core", "bots"],
        },
    )
    assert resp.status_code == 200
    stub_aggregates.assert_called_once()
    stub_top_bots.assert_called_once()


def test_aggregates_topten_only_passes_top_n_flag_true(client, stub_aggregates):
    """The dedicated /aggregates endpoint also honors the selector —
    sections=['topten'] keeps include_top_n=True and turns off the
    page-shape flags. Pinned so the FE's per-section hooks each pull
    only what they render."""
    resp = client.post(
        "/api/dashboard/aggregates",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["topten"],
        },
    )
    assert resp.status_code == 200
    kwargs = stub_aggregates.call_args.kwargs
    assert kwargs["include_top_n"] is True
    assert kwargs["include_time_series"] is False
    assert kwargs["include_conn_requests"] is False
    assert kwargs["include_map_data"] is False


def test_aggregates_rejects_unknown_section(client, stub_aggregates):
    resp = client.post(
        "/api/dashboard/aggregates",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "sections": ["unknown_section"],
        },
    )
    assert resp.status_code in (400, 422)


def test_bundle_gather_uses_return_exceptions_true():
    """Regression for F015 (audit run 7ba15352).

    On the parallel path, ``dashboard_bundle`` checks out a second
    DuckDB connection and fans out via ``asyncio.gather``. Without
    ``return_exceptions=True``, a branch that raises (or a cancelled
    coroutine) propagates the exception immediately; the surrounding
    ``finally`` then returns ``second_con`` to the pool while the OTHER
    ``asyncio.to_thread`` worker is still executing against it — and
    DuckDB connections are not safe for concurrent use. Subsequent
    checkouts of that connection deadlock on the internal mutex (8
    leaks exhaust DUCKDB_POOL_MAX_SIZE → persistent DoS) or corrupt
    in-process DuckDB state.

    The runtime branch is inside ``if parallel:``, which the test
    harness's single-connection pool fixture never exercises. Verify
    by reading the function's source — source-level check is the most
    reliable pin for this structural invariant.
    """
    import inspect

    from backend.routers.dashboard import dashboard_bundle

    src = inspect.getsource(dashboard_bundle)
    assert "return_exceptions=True" in src, (
        "asyncio.gather() inside dashboard_bundle must pass "
        "return_exceptions=True so both worker threads finish before "
        "the second pool connection is released (F015 regression)."
    )
    # Confirm the manual re-raise is present so a real exception still
    # surfaces as a 5xx rather than getting silently swallowed.
    assert "isinstance(aggregates, BaseException)" in src and "isinstance(top_bots, BaseException)" in src, (
        "gather(return_exceptions=True) must be paired with explicit "
        "BaseException re-raises so failures still propagate to the caller."
    )
