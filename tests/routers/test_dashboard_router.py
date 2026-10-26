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
def forbidden_clickhouse(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_ENABLED", "true")
    adapter = MagicMock(side_effect=AssertionError("archived evaluator reached from dashboard"))
    client = MagicMock(side_effect=AssertionError("ClickHouse client reached from dashboard"))
    monkeypatch.setattr("backend.core.clickhouse_client.get_clickhouse_client", client)
    monkeypatch.setattr("backend.core.clickhouse_client.ClickHouseClient.execute", client)
    yield adapter, client
    adapter.assert_not_called()
    client.assert_not_called()


@pytest.mark.parametrize("endpoint", ["aggregates", "bundle"])
@pytest.mark.parametrize("sections", [None, ["core", "topten"], ["topten"]])
def test_dashboard_stays_ducklake_when_clickhouse_enabled(
    client, endpoint, sections, stub_aggregates, stub_top_bots, forbidden_clickhouse
):
    body = {
        "fields": ["country"],
        "filters": {},
        "chart_interval": "1 hour",
        "chart_metric": "requests",
        "start_time": "2026-09-01T00:00:00Z",
        "end_time": "2026-09-02T00:00:00Z",
    }
    if sections is not None:
        body["sections"] = sections
    resp = client.post(
        f"/api/dashboard/{endpoint}",
        json=body,
    )
    assert resp.status_code == 200, resp.text
    stub_aggregates.assert_called_once()
    args = stub_aggregates.call_args.kwargs
    assert "sections" not in args
    assert args["include_top_n"] is True
    for flag in ("include_time_series", "include_conn_requests", "include_map_data"):
        assert args[flag] is (sections != ["topten"])
    assert args["fields_filter"] == ["country"]
    if endpoint == "bundle" and sections is None:
        stub_top_bots.assert_called_once()
        assert resp.json()["top_bots"]["bots"] == [{"name": "Googlebot", "request_count": 42}]
    else:
        stub_top_bots.assert_not_called()


@pytest.mark.parametrize("endpoint", ["aggregates", "bundle"])
def test_ducklake_receives_analyst_clamp_when_clickhouse_enabled(
    client,
    monkeypatch,
    endpoint,
    stub_aggregates,
    in_memory_duckdb,
    test_service_source,
    forbidden_clickhouse,
):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds
    from tests.conftest import make_test_request_context

    start, end = datetime(2026, 9, 1, 6, tzinfo=UTC), datetime(2026, 9, 1, 8, tzinfo=UTC)
    ctx = make_test_request_context(
        con=in_memory_duckdb,
        service_id=test_service_source["service_id"],
        source=test_service_source,
        session=SimpleNamespace(service_ids=[test_service_source["service_id"]]),
        path=f"/api/dashboard/{endpoint}",
        time_bounds=TimeBounds(start=start, end=end),
    )
    app.dependency_overrides[build_request_context] = lambda: ctx
    resp = client.post(
        f"/api/dashboard/{endpoint}",
        json={
            "fields": ["country"],
            "sections": ["core", "topten"],
            "filters": {},
            "chart_interval": "1 hour",
            "chart_metric": "requests",
            "start_time": "2026-08-01T00:00:00Z",
            "end_time": "2026-09-02T00:00:00Z",
        },
    )
    assert resp.status_code == 200, resp.text
    stub_aggregates.assert_called_once()
    args = stub_aggregates.call_args.kwargs
    assert datetime.fromisoformat(args["start_time"].replace("Z", "+00:00")) == start
    assert datetime.fromisoformat(args["end_time"].replace("Z", "+00:00")) == end
    assert args["src"] is test_service_source
    assert args["con"] is in_memory_duckdb
    assert "sections" not in args


def test_invalid_filter_rejected_before_dispatch(client, monkeypatch, forbidden_clickhouse):
    dispatch = MagicMock(side_effect=AssertionError("invalid request dispatched"))
    monkeypatch.setattr("backend.repositories.dashboard.get_aggregates", dispatch)
    resp = client.post(
        "/api/dashboard/aggregates", json={"filters": {"country": {"mode": "invalid", "values": ["US"]}}}
    )
    assert resp.status_code == 422
    dispatch.assert_not_called()


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
            "bots": [{"name": "Googlebot", "request_count": 42}],
            "ngwaf_bots": [],
            "section_timings": [{"section": "bots:query", "time_ms": 3.0}],
            "debug_queries": [{"sql": "SELECT bot", "time_ms": 0.5, "rows": 1, "caller": "bots_caller"}],
            "debug_calls": [],
        }
    )
    monkeypatch.setattr("backend.repositories.security.get_top_bots", stub)
    return stub


def test_bundle_preserves_component_owned_reader_coverage(client, stub_aggregates, stub_top_bots, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    aggregate_observation = {
        "reader": "dashboard.conn_requests_histogram",
        "minimum_window_hours": 2,
        "eligible": False,
        "eligibility_reason": "window_too_short",
        "outcome": "ineligible",
    }
    bot_observation = {
        "reader": "dashboard.ngwaf_bots",
        "minimum_window_hours": 2,
        "eligible": True,
        "eligibility_reason": "eligible",
        "outcome": "served",
        "attempts": [{"bundle": "ngwaf_bots", "reason": "planned", "closed_hours": 4, "covered_hours": 2}],
    }
    stub_aggregates.return_value["reader_coverage"] = [aggregate_observation]
    stub_top_bots.return_value["reader_coverage"] = [bot_observation]
    response = client.post("/api/dashboard/bundle", json={"filters": {}})
    assert response.status_code == 200
    body = response.json()
    assert body["aggregates"]["_reader_coverage"][0]["reader"] == "dashboard.conn_requests_histogram"
    assert body["top_bots"]["_reader_coverage"][0]["reader"] == "dashboard.ngwaf_bots"
    assert body["top_bots"]["_reader_coverage"][0]["attempts"][0]["covered_hours"] == 2
    assert "_reader_coverage" not in body
    assert "_debug_queries" not in body["aggregates"]
    assert "_debug_queries" not in body["top_bots"]


def test_bundle_omits_unobserved_reader_coverage(client, stub_aggregates, stub_top_bots):
    response = client.post("/api/dashboard/bundle", json={"filters": {}})
    assert response.status_code == 200
    body = response.json()
    assert "_reader_coverage" not in body["aggregates"]
    assert "_reader_coverage" not in body["top_bots"]


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


def _inject_mask_ips_analyst(in_memory_duckdb, test_service_source):
    """Re-point build_request_context so ctx carries a mask_ips analyst session
    (the local TestClient socket classifies as admin in the middleware, so we
    exercise the router's masking branch via the injected ctx.analyst_session)."""
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from tests.conftest import override_request_context

    session = SimpleNamespace(service_ids=[test_service_source["service_id"]], pii_policy={"mask_ips": True})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, _con_override=in_memory_duckdb, session=session
    )


@pytest.mark.security_regression
def test_raw_csv_masks_ip_and_redacts_cookie_session_for_masking_analyst(
    client, monkeypatch, in_memory_duckdb, test_service_source
):
    """Streaming CSV bypasses the middleware JSON PII pass (text/csv), so the
    handler masks itself. A mask_ips analyst export must have IPs masked AND
    session hashes (cookie_session) redacted — the raw hash must never leak,
    empty stays empty, and non-PII columns pass through verbatim."""
    df = pd.DataFrame(
        {
            "ip": ["203.0.113.45", "198.51.100.7"],
            "cookie_session": ["a1b2c3d4deadbeef", ""],
            "url": ["/a", "/b"],
        }
    )
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)
    _inject_mask_ips_analyst(in_memory_duckdb, test_service_source)

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )
    assert resp.status_code == 200, resp.text
    text = resp.text
    assert "deadbeef" not in text  # raw session hash never leaks
    assert "203.0.113.45" not in text  # raw IP never leaks
    assert "[redacted]" in text  # session redacted to the exact sentinel
    assert "203.0.113.xxx" in text  # IP masking still applied
    assert "/a" in text and "/b" in text  # non-PII columns verbatim


@pytest.mark.security_regression
def test_raw_csv_keeps_raw_cookie_session_for_non_masking_caller(client, monkeypatch):
    """The redaction is policy-gated: an admin / non-masking caller (the default
    ctx has no analyst_session) still exports the raw session value, so the mask
    branch can't silently over-redact everyone."""
    df = pd.DataFrame({"cookie_session": ["a1b2c3d4deadbeef"], "url": ["/a"]})
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )
    assert resp.status_code == 200, resp.text
    assert "a1b2c3d4deadbeef" in resp.text


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
    assert body["top_bots"]["bots"] == [{"name": "Googlebot", "request_count": 42}]


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


def test_bundle_primary_prewarm_is_cancellation_guarded():
    """Regression for the Local Std 2026-10-02 "pool saturated at 4" leak.

    The primary pooled connection is pre-warmed in a worker thread so the
    BLOCKING checkout (DuckLake attach + Iceberg view rebuild + lock-retry)
    stays off the event loop. A BARE ``await asyncio.to_thread(lambda: ctx.con)``
    is NOT cancellation-safe: when the client disconnects mid-checkout (e.g.
    the browser closes the page on a slow/503 response), the awaiting
    coroutine raises ``CancelledError`` immediately while the detached worker
    thread is still inside ``_ConnectionHolder.__enter__``. The request-context
    dependency's ``finally`` then races ``__exit__`` against the still-executing
    checkout generator — the connection is acquired (``in_use += 1``) but never
    released, permanently stranding a pool slot. Four such leaks exhaust
    ``DUCKDB_POOL_MAX_SIZE`` and every subsequent request 503s with
    ``pool for <svc> saturated`` until the backend restarts.

    The runtime race is non-deterministic and the single-connection test
    fixture never exercises it, so pin the structural invariant at the source
    level (same approach as ``test_bundle_gather_uses_return_exceptions_true``):
    the pre-warm must be awaited through the guarded
    ``create_task`` + ``shield`` + ``except CancelledError: await task``
    pattern, never a bare ``await asyncio.to_thread(lambda: ctx.con)``.
    """
    import inspect

    from backend.routers.dashboard import dashboard_bundle

    src = inspect.getsource(dashboard_bundle)
    assert "await asyncio.to_thread(lambda: ctx.con)" not in src, (
        "The primary-connection pre-warm must NOT be a bare "
        "``await asyncio.to_thread(lambda: ctx.con)`` — cancelled mid-checkout "
        "it leaks a pool slot (pool-saturated 503 until restart). Wrap it in "
        "the guarded create_task + shield + await-on-cancel pattern."
    )
    # The guarded pre-warm must await the detached checkout task to completion
    # on cancellation so the holder records the connection for the dependency
    # ``finally`` to release.
    assert "ctx.con" in src and "asyncio.shield(" in src, (
        "dashboard_bundle must pre-warm ctx.con through asyncio.shield so a "
        "mid-checkout cancellation still completes the checkout before unwind."
    )


# ── Parallel execution & Fallback scenarios ─────────────────────────────


def test_bundle_parallel_success(client, stub_aggregates, stub_top_bots):
    from unittest.mock import MagicMock, patch

    mock_con = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_con

    # We patch checkout_connection to return our mock context manager
    with patch("backend.core.duckdb_pool.checkout_connection", return_value=mock_cm) as mock_checkout:
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
        mock_checkout.assert_called_once()
        mock_cm.__enter__.assert_called_once()
        mock_cm.__exit__.assert_called_once()


def test_bundle_parallel_pool_busy_fallback(client, stub_aggregates, stub_top_bots):
    from unittest.mock import patch

    from backend.core.duckdb_pool import _PoolBusy

    with patch("backend.core.duckdb_pool.checkout_connection", side_effect=_PoolBusy("busy")) as mock_checkout:
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
        mock_checkout.assert_called_once()


def test_bundle_parallel_generic_exception_fallback(client, stub_aggregates, stub_top_bots):
    from unittest.mock import patch

    with patch("backend.core.duckdb_pool.checkout_connection", side_effect=RuntimeError("err")) as mock_checkout:
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
        mock_checkout.assert_called_once()


async def test_bundle_resolves_primary_connection_off_event_loop(monkeypatch):
    """The first ``ctx.con`` access performs a BLOCKING pool checkout
    (DuckLake attach + Iceberg view rebuild + up-to-max_wait lock retry).
    Evaluated as an argument to ``asyncio.to_thread(_run_aggregates,
    ctx.con)`` it runs ON the asyncio event loop, so under cron-vs-dashboard
    DuckLake contention it freezes ``/api/health`` and every other request —
    cascading one slow dashboard query into a fully-"unhealthy" backend
    (GCE 2026-10-02 py-spy: MainThread blocked in get_connection inside the
    ASGI request path). ``dashboard_bundle`` must pre-resolve the primary
    connection in a worker thread so a contended checkout slows only its own
    request, never the shared event loop.
    """
    import asyncio
    import time as _time
    from unittest.mock import MagicMock

    from backend.models.dashboard import AggregatesRequest

    import backend.routers.dashboard as dash
    from backend.core.request_context import RequestContext

    checkout_block_s = 0.25

    class _BlockingHolder:
        """Stand-in for ``_ConnectionHolder`` whose ``__enter__`` blocks the
        calling thread like a real contended pool checkout."""

        def __init__(self, con):
            self.con = None
            self._real = con
            self.enter_calls = 0

        def __enter__(self):
            self.enter_calls += 1
            _time.sleep(checkout_block_s)
            self.con = self._real
            return self._real

        def __exit__(self, *_a):
            return False

    holder = _BlockingHolder(MagicMock())
    ctx = RequestContext(
        service_id="svc-standard-not-highscale",
        source={"name": "svc", "service_id": "svc-standard-not-highscale"},
        telemetry=MagicMock(),
        _holder=holder,
    )
    monkeypatch.setattr(dash.repo, "get_aggregates", lambda **_k: {})

    req = AggregatesRequest(
        start_time="2026-06-12T00:00:00Z",
        end_time="2026-06-12T01:00:00Z",
        filters={},
        chart_metric="requests",
        chart_interval="minute",
        sections=["core"],
    )

    ticks = 0

    async def _heartbeat():
        nonlocal ticks
        for _ in range(500):
            await asyncio.sleep(0.005)
            ticks += 1

    hb = asyncio.create_task(_heartbeat())
    await dash.dashboard_bundle(req, ctx)
    hb.cancel()

    assert holder.enter_calls >= 1, "the pool checkout must still happen"
    # Off-loop, the 5ms heartbeat advances ~45x across the 0.25s checkout.
    # On-loop (the bug), it cannot tick at all while the loop is blocked.
    assert ticks >= 10, (
        f"event loop was blocked during the pool checkout (only {ticks} "
        "heartbeat ticks observed) — dashboard_bundle must resolve ctx.con "
        "off the event loop"
    )


async def test_high_scale_bundle_runs_off_event_loop(monkeypatch):
    """The high-scale dashboard bundle runs a series of BLOCKING ClickHouse
    aggregate queries (one per field, plus the time series) inside the
    ``async def dashboard_bundle`` handler. The non-high-scale branch already
    offloads its blocking work via ``asyncio.to_thread``; the high-scale
    branch returned ``high_scale_bundle(...)`` synchronously, executing every
    ClickHouse query ON the asyncio event loop. That froze ``/api/health``,
    SSE, and every concurrent request for the whole query — the Elevation
    high-scale 24h "Crunching logs… → Failed to fetch" wedge. The high-scale
    branch must run off the event loop too.
    """
    import asyncio
    import time as _time
    from unittest.mock import MagicMock

    import backend.high_scale.registry_bootstrap as hsreg
    from backend.models.dashboard import AggregatesRequest

    import backend.high_scale.dashboard as hsd
    import backend.routers.dashboard as dash
    from backend.core.request_context import RequestContext

    sentinel_service = object()

    class _Reg:
        def resolve(self, _sid):
            return sentinel_service

    monkeypatch.setattr(hsreg, "get_high_scale_service_registry", lambda: _Reg())

    block_s = 0.25

    def _blocking_bundle(service, req, start, end):
        assert service is sentinel_service
        _time.sleep(block_s)
        return {"aggregates": {}, "top_bots": {}}

    monkeypatch.setattr(hsd, "bundle", _blocking_bundle)

    ctx = RequestContext(
        service_id="svc-highscale",
        source={"name": "svc", "service_id": "svc-highscale"},
        telemetry=MagicMock(),
        _holder=MagicMock(),  # must never be entered on the high-scale path
    )

    req = AggregatesRequest(
        start_time="2026-06-12T00:00:00Z",
        end_time="2026-06-12T01:00:00Z",
        filters={},
        chart_metric="requests",
        chart_interval="minute",
        sections=["core"],
    )

    ticks = 0

    async def _heartbeat():
        nonlocal ticks
        for _ in range(500):
            await asyncio.sleep(0.005)
            ticks += 1

    hb = asyncio.create_task(_heartbeat())
    result = await dash.dashboard_bundle(req, ctx)
    hb.cancel()

    assert result["aggregates"] == {}
    # Off-loop, the 5ms heartbeat advances ~45x across the 0.25s query.
    # On-loop (the bug), it cannot tick at all while the loop is blocked.
    assert ticks >= 10, (
        f"event loop was blocked during the high-scale bundle (only {ticks} "
        "heartbeat ticks observed) — the high-scale branch of dashboard_bundle "
        "must run off the event loop"
    )


# ── Merged from tests/routers/test_query_router.py ──

"""Router-level contract tests for ``/api/query`` and ``/api/presets``.

The happy path for ``/api/query`` is already pinned in
[tests/routers/test_pages.py](tests/routers/test_pages.py); this file
exercises the *error branches* and the ``/api/presets`` endpoint that
``test_pages.py`` doesn't touch.

The router translates a few specific exceptions from the repository
layer into HTTP status codes — that mapping is what the frontend's
query editor depends on to distinguish "you don't have access to this
table" (403) from "syntax error in your SQL" (400), so the mapping
itself is the contract.
"""


from unittest.mock import patch

import pytest

from tests.conftest import MOCK_SERVICE_ID, override_request_context


def _stub_result() -> dict:
    """Minimal execute_query return shape so the router can serialize it."""
    return {
        "columns": [],
        "data": [],
        "row_count": 0,
        "total_rows": 0,
        "truncated": False,
        "elapsed_ms": 0,
        "debug_queries": [],
        "debug_calls": [],
        "section_timings": [],
    }


# ── /api/query: input validation + exception → HTTP mapping ─────────────────


def test_query_empty_sql_returns_400(client):
    """Empty SQL → 400 with structured error (not a generic 422)."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": ""},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "empty_sql"
    assert resp.json()["detail"]["message"] == "No SQL provided"


def test_query_whitespace_only_sql_returns_400(client):
    """Whitespace counts as empty — the router strips before checking
    so the frontend can't accidentally submit a single newline and
    blow up downstream."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "   \n\t"},
    )
    assert resp.status_code == 400


def test_query_permission_error_maps_to_403(client):
    """``PermissionError`` from the repo → 403 (not 400). The frontend
    distinguishes these for the "access denied" UI affordance.

    Unified envelope: ``detail.error`` is the machine code
    (``sql_not_permitted``) and the human-readable message lives in
    ``detail.message``."""
    with patch(
        "backend.repositories.query.execute_query",
        side_effect=PermissionError("not authorized for system catalog"),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 403
    body = resp.json()["detail"]
    assert body["error"] == "sql_not_permitted"
    assert "not authorized" in body["message"]


def test_query_unexpected_exception_maps_to_400(client):
    """Any other exception (syntax error, missing table, etc.) → 400 with
    the exception text in ``detail.message`` (after path redaction) and
    a stable machine code in ``detail.error``."""
    with patch(
        "backend.repositories.query.execute_query",
        side_effect=RuntimeError("table 'nope' does not exist"),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT * FROM nope"},
        )

    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["error"] == "query_failed"
    assert "does not exist" in body["message"]


def test_query_io_error_redacts_filesystem_paths(client):
    """DuckDB IO Errors interpolate absolute paths (``Cannot open file
    '/srv/data/buffers/...'``). The router must strip them before echoing
    so the wire payload doesn't leak server-side directory layout, while
    still keeping the SQL diagnostic context."""
    with patch(
        "backend.repositories.query.execute_query",
        # Two attempts: first 'Cannot open file' triggers the inline
        # retry; second non-match raises through.
        side_effect=[
            RuntimeError("Cannot open file '/srv/data/buffers/x.parquet'"),
            RuntimeError("IO Error: file '/srv/data/iceberg/snap.json' is corrupt"),
        ],
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["error"] == "query_failed"
    assert "/srv/" not in body["message"], "absolute paths must be redacted"
    assert "<path>" in body["message"]


# ── _redact_paths: regex shape ──────────────────────────────────────────────


def test_redact_paths_strips_deploy_root_prefixes():
    """The redaction regex anchors on a conservative list of Unix root
    prefixes (srv|var|tmp|data|home|opt|usr|mnt|app). Each is a documented
    location DuckDB / Iceberg / our code emits in exception strings. ``app``
    is the prod container WORKDIR (/app/data/services/<sid>/...)."""
    from backend.routers.query import _redact_paths

    for prefix in ("/srv", "/var", "/tmp", "/data", "/home", "/opt", "/usr", "/mnt", "/app"):
        msg = f"Cannot open file '{prefix}/foo/bar.parquet'"
        out = _redact_paths(msg)
        assert prefix not in out, f"{prefix} should have been redacted; got: {out!r}"
        assert "<path>" in out


def test_redact_paths_strips_prod_data_root():
    """Regression: the real prod data root is /app/data/services/<sid>/...
    (backend/Dockerfile WORKDIR=/app). Before ``app`` was added to the
    allowlist these absolute paths reached the analyst un-redacted because
    the ``/data`` token in ``/app/data`` is preceded by an alnum and so
    failed the leading-boundary lookbehind."""
    from backend.routers.query import _redact_paths

    msg = "IO Error: No files found that match '/app/data/services/abc123/raw/2026-06-17/x.parquet'"
    out = _redact_paths(msg)
    assert "/app/data" not in out, f"prod data root must be redacted; got: {out!r}"
    assert "abc123" not in out, "service id must not leak"
    assert "<path>" in out


def test_redact_paths_preserves_url_path_segments_that_look_like_server_paths():
    """https://example.com/var/foo is NOT a leak — the /var here is part
    of a URL path. The previous over-broad ``/[A-Za-z0-9_./-]+`` pattern
    stripped it (lost URL context inside SQL error messages); the
    anchored variant requires the leading slash to NOT be preceded by an
    alphanumeric, so URL paths survive."""
    from backend.routers.query import _redact_paths

    msg = "lookup failed for https://example.com/var/foo on row 12"
    out = _redact_paths(msg)
    assert out == msg, "URL path should NOT have been redacted"


def test_redact_paths_leaves_non_root_paths_alone():
    """/api/foo (column ref) and /etc/hosts (non-allowlisted root) both
    stay intact. If a real /etc leak shows up in prod we'll add it; the
    conservative default is to err on the side of preserving diagnostic
    context."""
    from backend.routers.query import _redact_paths

    for msg in ("column /api/foo missing", "ENOENT: /etc/hosts not found"):
        assert _redact_paths(msg) == msg, f"unexpected redaction in: {msg!r}"


def test_redact_paths_strips_quoted_and_whitespace_prefixed_paths():
    """The lookbehind allows whitespace, quotes, and line-start — all the
    contexts DuckDB / Python tracebacks put a leaked path in."""
    from backend.routers.query import _redact_paths

    cases = [
        ("Cannot open '/var/lib/duckdb/foo.db'", "/var/lib/duckdb/foo.db"),
        ('No such file: "/srv/cache/x.parquet"', "/srv/cache/x.parquet"),
        ("Path was /tmp/buffer.json on disk", "/tmp/buffer.json"),
        ("/data/services/svc.metadata.db corrupted", "/data/services/svc.metadata.db"),
    ]
    for msg, leaked in cases:
        out = _redact_paths(msg)
        assert leaked not in out, f"path {leaked!r} survived in: {out!r}"


# ── M1: max_rows model bound ────────────────────────────────────────────────


def test_query_request_rejects_oversized_max_rows():
    """M1: the model caps max_rows at 10_000 (was unbounded → OOM lever)."""
    from backend.models.dashboard import QueryRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        QueryRequest(sql="SELECT 1", max_rows=10_001)
    with pytest.raises(ValidationError):
        QueryRequest(sql="SELECT 1", max_rows=0)
    assert QueryRequest(sql="SELECT 1", max_rows=10_000).max_rows == 10_000


# ── H1/H2 wiring: router forwards window + mask flag to execute_query ────────


def test_admin_request_passes_no_window_and_no_mask(client):
    """An admin request (no analyst session on the context) must reach the
    repo with ``time_filter=None`` (full range) and ``mask_ips=False``."""
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_result()

    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )

    assert resp.status_code == 200, resp.text
    assert captured["time_filter"] is None
    assert captured["mask_ips"] is False


def test_analyst_request_forwards_clamped_window_and_mask(client, in_memory_duckdb, test_service_source):
    """An analyst session → the router clamps to the session window and
    forwards the concrete bounds + the mask_ips flag to execute_query."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-1",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_result()

    # The handler clamps via ctx.clamp, which reads ctx.time_bounds (what
    # build_request_context resolves from get_analyst_time_bounds in prod).
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )
    with patch("backend.repositories.query.execute_query", side_effect=_fake):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT ip FROM logs"},
        )

    assert resp.status_code == 200, resp.text
    assert captured["time_filter"] == (start.isoformat(), end.isoformat())
    assert captured["mask_ips"] is True


# ── /api/presets: source lookup + connection fallback ───────────────────────


def test_presets_no_service_id_returns_empty_list(client):
    """No header AND no configured active service → ``[]`` (don't 500).
    The frontend pre-fetches presets before a service is selected; the
    only way to hit this branch is on a freshly-provisioned install."""
    # get_service_id falls back to svcconfig.get_active_service_id() when
    # no header/query is set, so we have to pin both to None.
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=None):
        resp = client.get("/api/presets")
    assert resp.status_code == 200
    assert resp.json() == []


def test_presets_unknown_service_returns_empty_list(client):
    """Service id present but ``get_source_for_service`` returns None
    → ``[]``. Prevents a 500 when an admin yanks a service while the
    frontend still has its id in the URL."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": "ghost-service"})

    assert resp.status_code == 200
    assert resp.json() == []


def test_presets_returns_repo_output_when_source_resolves(client):
    """Source resolves → router calls ``repo.get_presets`` and returns
    whatever it produces. The router itself doesn't shape the payload
    — that's the repo's job."""
    fake_presets = [{"id": "p1", "name": "Top 5xx", "description": "Top 5xx URLs", "sql": "SELECT 1"}]
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.repositories.query.get_presets", return_value=fake_presets),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert resp.json() == fake_presets


def test_presets_does_not_acquire_a_duckdb_connection(client):
    """Presets are pure SQL templates derived from the service name
    — the endpoint must NOT call ``get_connection``, which would pay
    extension-install / pool-wait cost on cache miss for no benefit."""
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": MOCK_SERVICE_ID},
        ),
        patch("backend.core.duckdb.get_connection") as mock_get_connection,
        patch("backend.repositories.query.get_presets", return_value=[]),
    ):
        resp = client.get("/api/presets", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert mock_get_connection.call_count == 0


# ── End-to-end router + repository SELECT, AST safety, schema & masking ─────


def test_query_select_executes_against_duckdb(client, in_memory_duckdb, monkeypatch):
    """Valid SELECT statement executes against the service table and returns
    tabular columns, rows, and telemetry."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T10:00:00Z', '203.0.113.42', '/index.html', 200)"
    )
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT url, status FROM logs"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["url", "status"]
    assert body["row_count"] == 1
    assert body["data"][0] == {"url": "/index.html", "status": 200}
    assert isinstance(body["_debug_queries"], list) and len(body["_debug_queries"]) >= 1


@pytest.mark.parametrize(
    "unsafe_sql",
    [
        "INSERT INTO logs (url) VALUES ('/x')",
        "UPDATE logs SET status = 500",
        "DELETE FROM logs WHERE status = 200",
        "DROP TABLE logs",
        "ALTER TABLE logs ADD COLUMN extra VARCHAR",
        "ATTACH ':memory:' AS ext_db",
        "COPY logs TO '/tmp/out.parquet'",
    ],
)
def test_query_ast_safety_rejects_non_select_statements(client, unsafe_sql):
    """AST validator strictly rejects mutating and administrative SQL statements
    with HTTP 403 sql_not_permitted."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": unsafe_sql},
    )
    assert resp.status_code == 403, f"Expected 403 for {unsafe_sql!r}, got {resp.status_code}: {resp.text}"
    detail = resp.json()["detail"]
    assert detail["error"] == "sql_not_permitted"


def test_query_syntax_error_rejected_cleanly(client):
    """Malformed SQL syntax is rejected at parse time without leaking server paths."""
    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT FROM WHERE ("},
    )
    assert resp.status_code in (400, 403)
    detail = resp.json()["detail"]
    assert detail["error"] in ("sql_not_permitted", "query_failed")
    assert "parse error" in detail["message"].lower() or "syntax error" in detail["message"].lower()


def test_query_wraps_execution_with_stale_view_retry(client):
    """Verify execute_with_stale_view_retry wraps /api/query execution."""
    with patch(
        "backend.core.iceberg.execute_with_stale_view_retry",
        return_value=_stub_result(),
    ) as mock_retry:
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code == 200
    assert mock_retry.call_count == 1


def test_query_remote_analyst_masks_ips_end_to_end(client, in_memory_duckdb, test_service_source):
    """Analyst session with mask_ips=True redacts raw IP columns and masks IP-shaped values."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    in_memory_duckdb.execute(
        "CREATE TABLE IF NOT EXISTS logs_test_service (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR, status INTEGER)"
    )
    in_memory_duckdb.execute(
        "INSERT INTO logs_test_service (timestamp, ip, url, status) "
        "VALUES (TIMESTAMPTZ '2026-06-17T09:30:00Z', '198.51.100.77', '198.51.100.88', 200)"
    )
    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-mask",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(start=start, end=end),
    )

    resp = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": "SELECT ip, url AS leaked_ip FROM logs"},
    )
    assert resp.status_code == 200, resp.text
    row = resp.json()["data"][0]
    assert row["ip"] == "[redacted]"
    assert row["leaked_ip"] == "198.51.100.xxx"


def test_query_high_scale_validates_ast_and_masks_ips(client, monkeypatch):
    """High-Scale /api/query enforces AST validation and IP masking."""
    from types import SimpleNamespace

    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    fake_hs_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}]  # the console's fail-closed check that additional_table_filters is enforced
            if "system.one" in sql
            else [{"client_ip": "203.0.113.9", "custom_fields['url']": "203.0.113.10"}]
        )
    )
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)

    with patch(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        # 1. Mutating query must 403
        drop_resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "DROP TABLE logs"},
        )
        assert drop_resp.status_code == 403
        assert drop_resp.json()["detail"]["error"] == "sql_not_permitted"

        # 2. Valid SELECT query succeeds and returns QueryResponse shape
        ok_resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) FROM logs"},
        )
        assert ok_resp.status_code == 200
        body = ok_resp.json()
        assert body["row_count"] == 1
        assert len(body["_debug_queries"]) == 1
        assert body["_debug_queries"][0]["engine"] == "ClickHouse"


def test_query_schema_endpoint_returns_columns(client):
    """GET /api/query/schema/{service_id} returns reflected schema columns."""
    with patch(
        "backend.config.get_status",
        return_value={"schema": [{"column": "timestamp", "type": "TIMESTAMPTZ"}, {"column": "url", "type": "VARCHAR"}]},
    ):
        resp = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "test_service"
    assert len(body["schema"]) == 2
    assert body["schema"][0]["column"] == "timestamp"


def test_query_schema_endpoint_falls_back_when_empty(client):
    """GET /api/query/schema/{service_id} returns fallback columns when live schema is empty."""
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", return_value=[]),
    ):
        resp = client.get(f"/api/query/schema/{MOCK_SERVICE_ID}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    col_names = {c["column"] for c in body["schema"]}
    assert {"timestamp", "url", "status"}.issubset(col_names)


@pytest.mark.security_regression
def test_query_high_scale_is_closed_to_analysts_until_clickhouse_enforces_the_boundary(
    client, in_memory_duckdb, test_service_source
):
    """Analysts are an adversary: the console's name-based guards cannot be the only thing between them and ClickHouse."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    calls: list[str] = []
    fake_hs_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=fake_hs_client)
    session = SimpleNamespace(
        session_id="sess-hs",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )

    with patch(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) FROM logs"},
        )

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert calls == [], "nothing may reach ClickHouse for an analyst"


def _analyst_on_high_scale(client, in_memory_duckdb, test_service_source, main_calls, console, mask_ips=True):
    """Post a console query as an analyst against a fake high-scale service; ``console`` is the stand-in manager."""
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    main_client = SimpleNamespace(execute=lambda sql, params=None, settings=None: main_calls.append(sql) or [])
    fake_hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=main_client)
    session = SimpleNamespace(
        session_id="sess-hs2", pii_policy={"mask_ips": mask_ips}, service_ids=[test_service_source["service_id"]]
    )
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )
    with (
        patch(
            "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: fake_hs_service),
        ),
        patch("backend.high_scale.console_access.get_console_clients", return_value=console),
    ):
        return client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) AS n FROM logs"},
        )


@pytest.mark.security_regression
def test_analyst_console_runs_on_the_services_own_least_privilege_identity_through_its_views(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    seen: list[tuple[str, dict]] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: (
            [{"n": 0}] if "system.one" in sql else seen.append((sql, dict(settings or {}))) or [{"n": 7}]
        )
    )
    asked: list[bool] = []

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    main_calls: list[str] = []

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=client_for)
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["data"] == [{"n": 7}]
    sql, settings = seen[0]
    assert "fla_cv_view_req" in sql and "request_facts" not in sql, "analysts read through their view only"
    scope = settings["additional_table_filters"]
    assert "event_timestamp >=" in scope and "service_id" not in scope, "the view fixes the service; only the window"
    assert main_calls == [], "and never on the application's privileged ClickHouse user"
    assert asked == [True], "a mask_ips invite must get the masked identity"


@pytest.mark.security_regression
def test_an_invite_without_mask_ips_gets_the_raw_identity(client, in_memory_duckdb, test_service_source):
    from types import SimpleNamespace

    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, [], SimpleNamespace(client_for=client_for), mask_ips=False
    )

    assert resp.status_code == 200, resp.text
    assert asked == [False]


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_no_console_identity_is_available(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    main_calls: list[str] = []
    console = SimpleNamespace(client_for=lambda sid: None)

    resp = _analyst_on_high_scale(client, in_memory_duckdb, test_service_source, main_calls, console)

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
def test_analyst_console_stays_closed_when_the_identity_fails_its_isolation_check(
    client, in_memory_duckdb, test_service_source
):
    from types import SimpleNamespace

    def refuse(sid):
        raise RuntimeError("console identity failed the row isolation check")

    main_calls: list[str] = []

    resp = _analyst_on_high_scale(
        client, in_memory_duckdb, test_service_source, main_calls, SimpleNamespace(client_for=refuse)
    )

    assert resp.status_code == 403
    assert resp.json()["detail"]["error"] == "console_unavailable_for_analysts"
    assert main_calls == []


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("policy", "expected_masked"),
    [
        (None, True),
        ({}, True),
        ({"mask_ips": None}, True),
        ({"mask_ips": "false"}, True),
        ({"mask_ips": True}, True),
        ({"mask_ips": False}, False),
    ],
)
def test_the_console_picks_the_masked_identity_unless_the_invite_explicitly_disables_masking(
    client, in_memory_duckdb, test_service_source, policy, expected_masked
):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    asked: list[bool] = []
    console_client = SimpleNamespace(
        execute=lambda sql, params=None, settings=None: [{"n": 0}] if "system.one" in sql else [{"n": 1}]
    )

    def client_for(sid, masked=True):
        asked.append(masked)
        return SimpleNamespace(client=console_client, tables={"request_facts": "fla_cv_view_req"})

    session = SimpleNamespace(session_id="s", pii_policy=policy, service_ids=[test_service_source["service_id"]])
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/query",
        time_bounds=TimeBounds(
            start=datetime(2026, 6, 17, 9, 0, tzinfo=UTC), end=datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
        ),
    )
    hs_service = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=lambda *a, **k: []))
    with (
        patch(
            "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
            return_value=SimpleNamespace(resolve=lambda sid: hs_service),
        ),
        patch(
            "backend.high_scale.console_access.get_console_clients",
            return_value=SimpleNamespace(client_for=client_for),
        ),
    ):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT count(*) AS n FROM logs"},
        )

    assert resp.status_code == 200, resp.text
    assert asked == [expected_masked]
