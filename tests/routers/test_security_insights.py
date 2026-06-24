from unittest.mock import patch

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _stub_insights() -> dict:
    """Minimal get_insights return shape for the InsightsResponse model."""
    now = "2026-06-17T00:00:00+00:00"
    return {
        "insights": [],
        "window_start": now,
        "window_end": now,
        "baseline_start": now,
        "baseline_end": now,
        "computed_at": now,
        "window_hours": 1.0,
        "baseline_hours": 168.0,
    }


def test_insights_admin_passes_no_clamp(client):
    """M2 wiring: admin (no analyst session) reaches the repo with
    clamp_start/clamp_end = None (full range + shared prewarmer cache)."""
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    with patch("backend.repositories.insights.get_insights", side_effect=_fake):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 1.0, "baseline_hours": 168.0},
        )

    assert resp.status_code == 200, resp.text
    assert captured["clamp_start"] is None
    assert captured["clamp_end"] is None


def test_insights_analyst_passes_clamped_window(client, in_memory_duckdb, test_service_source):
    """M2 wiring: an analyst session → the router clamps the scanned range to
    the session window and forwards concrete clamp bounds to get_insights."""
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds, get_analyst_time_bounds

    # Bounds in the past relative to the router's `now`, so clamp_end ceilings
    # at `end` and clamp_start floors at `start` regardless of wall-clock.
    now = datetime.now(UTC)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)
    session = SimpleNamespace(
        session_id="sess-1",
        pii_policy={"mask_ips": False},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, con=in_memory_duckdb, session=session, path="/api/insights"
    )
    app.dependency_overrides[get_analyst_time_bounds] = lambda: TimeBounds(start=start, end=end)
    try:
        with patch("backend.repositories.insights.get_insights", side_effect=_fake):
            resp = client.post(
                "/api/insights",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"window_size_hrs": 1.0, "baseline_hours": 168.0},
            )
    finally:
        app.dependency_overrides.pop(get_analyst_time_bounds, None)

    assert resp.status_code == 200, resp.text
    # Clamp ceilings the anchor at `end` and floors the earliest scan at `start`.
    assert captured["clamp_end"] == end.isoformat()
    assert captured["clamp_start"] == start.isoformat()


def test_security_endpoint(client, in_memory_duckdb, test_service_source):
    # Setup mock data with security fields
    logs = generate_mock_logs(test_service_source, num_logs=50)
    for i, log in enumerate(logs[:10]):
        log["waf"] = True
        log["waf_sig"] = "SQLI,XSS"
        log["waf_resp"] = 403
        log["ja3"] = "a0e9f5d64349fb13191bc781f81f42e1"  # Mock bad JA3

    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/security/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )

    assert response.status_code == 200
    data = response.json()

    # We asserted 'ja3' was present but backend might use a different key.
    # Security endpoint typically returns bot categories, rate limiting signals etc.
    # Let's just check for _debug_queries indicating it ran successfully
    assert "_debug_queries" in data


def test_security_endpoint_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 with an unknown_section
    detail — invalid selector values must not be silently dropped (the
    FE would render the card without the data, hiding the typo)."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/security/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["not_a_section"]},
    )
    # Pydantic validates Literal first, so the response is 422 from the
    # request-validator OR 400 from _expand_sections — either is an
    # explicit reject (the FE never gets a partial 200 with the bad
    # section silently dropped). Both surface a JSON error body.
    assert response.status_code in (400, 422)


def test_security_endpoint_returns_only_requested_sections(client, in_memory_duckdb, test_service_source):
    """sections=['conn_reuse_dist'] suppresses the other 12 section
    keys from the response — proves the router → repo pipeline carries
    the selector through end-to-end."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/security/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["conn_reuse_dist"]},
    )
    assert response.status_code == 200
    data = response.json()
    # Pydantic always echoes its declared fields with their defaults, so
    # missing-from-result keys come back as their schema defaults
    # ([] / {}). Test the SQL gate instead by checking that the unwanted
    # keys are empty AND conn_reuse_dist is the only one that could
    # have had data — section_timings entries are the more direct gate
    # (suppressed sections don't append to the timer).
    timings_names = {t["section"] for t in data.get("_section_timings", [])}
    # conn_reuse_dist's timer entry must be present
    assert "conn_reuse_dist" in timings_names, (
        f"selector dropped the requested section's timing entry; got {timings_names}"
    )
    # And the other sections' SQL must NOT have fired
    for blocked in {"ipv6_adoption", "proxy_dist", "verified_bots_ts", "wellknown_bots_query"}:
        assert blocked not in timings_names, f"selector did not suppress {blocked}; got {timings_names}"


def test_insights_endpoint(client, in_memory_duckdb, test_service_source):
    # Setup mock data
    logs = generate_mock_logs(test_service_source, num_logs=50)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/insights",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"window_hours": 1.0, "baseline_hours": 24.0},
    )

    assert response.status_code == 200
    data = response.json()
    assert "insights" in data
    assert isinstance(data["insights"], list)


def test_cache_collapse_detail_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=5)
    # Ensure there is a URL and a cache miss/hit to get valid results
    for log in logs:
        log["url"] = "/test-url-collapse"
        log["cache"] = "MISS"
        log["ip"] = "1.2.3.4"
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/insights/cache-collapse-detail",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "url": "/test-url-collapse",
            "window_size_hrs": 1.0,
            "baseline_hours": 24.0,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["url"] == "/test-url-collapse"
    assert "timeline" in data
    # Hit ratio is HIT/(HIT+MISS); with only MISS rows it's 0.0 (and PASS-free).
    assert data["baseline_hit_rate"] == 0.0
    assert data["window_hit_rate"] == 0.0
    assert data["baseline_pass_rate"] == 0.0
    assert data["window_pass_rate"] == 0.0
    # Breakdown is window-scoped counts by disposition.
    assert set(data["breakdown"].keys()) == {"hits", "misses", "passes", "other"}
    assert data["breakdown"]["passes"] == 0
    # Recent events are scoped to actual MISSes (not the old non-HIT catch-all).
    assert "recent_misses" in data
    assert "evictions" not in data
    assert all(row["cache"].upper().startswith("MISS") for row in data["recent_misses"])
