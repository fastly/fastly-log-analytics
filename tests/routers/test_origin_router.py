"""HTTP-level smoke tests for the origin router.

Each ``/api/origin/*`` endpoint is a thin wrapper around the
corresponding repo function. The repo paths are exhaustively tested
against real seeded DuckDB in ``tests/repositories/test_origin.py``;
this file just pins:
  - every endpoint exists and returns 200 (or a documented status)
  - request validation (extra fields, bad types) returns 422
  - request kwargs reach the repo unchanged (the FastAPI → repo
    plumbing has no integration tests otherwise)
"""

from __future__ import annotations

from unittest.mock import patch

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed_origin_table(con, src) -> None:
    """Populate the test logs table with origin-shaped data so every
    endpoint can return real (non-empty) results."""
    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50_000 + i * 1000
        log["ost"] = 200 if i % 5 != 0 else 500
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
        log["url"] = "/api/foo" if i % 2 else "/api/bar"
        log["edge"] = True
        log["pop"] = "SJC"
    insert_mock_logs(con, _safe_table(src["name"]), logs)


# ── /api/origin/timeseries: parameter validation ───────────────────────────


def test_origin_timeseries_accepts_default_request(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    assert "series" in resp.json() or "has_data" in resp.json()


def test_origin_timeseries_validates_metric_enum(client):
    """``metric`` must be one of {ttfb, ttlb} — anything else → 422.
    Pinned because a typo'd metric would otherwise reach the repo and
    surface as a SQL error rather than a clear validation message."""
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "metric": "totally_made_up"},
    )
    assert resp.status_code == 422


def test_origin_timeseries_validates_percentile_enum(client):
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "percentile": "p42"},
    )
    assert resp.status_code == 422


def test_origin_timeseries_passes_kwargs_to_repo(client, in_memory_duckdb, test_service_source):
    """Every Request field must round-trip into the repo call —
    pinned because misspelling a kwarg in the router would silently
    fall back to the repo default and the user's UI setting would
    be ignored."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_timeseries", return_value={"has_data": False, "series": []}) as mock:
        client.post(
            "/api/origin/timeseries",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "bucket_minutes": 15,
                "split_by_leg": True,
                "metric": "ttlb",
                "percentile": "p99",
            },
        )

    kwargs = mock.call_args.kwargs
    assert kwargs["bucket_minutes"] == 15
    assert kwargs["split_by_leg"] is True
    assert kwargs["metric"] == "ttlb"
    assert kwargs["percentile"] == "p99"


# ── /api/origin/slow-urls: limit + min_requests ────────────────────────────


def test_origin_slow_urls_passes_limit_and_min_requests(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_slow_urls", return_value={"has_data": False, "rows": []}) as mock:
        client.post(
            "/api/origin/slow-urls",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "limit": 5, "min_requests": 25},
        )

    kwargs = mock.call_args.kwargs
    assert kwargs["limit"] == 5
    assert kwargs["min_requests"] == 25


def test_origin_slow_urls_clamps_limit_at_100(client, in_memory_duckdb, test_service_source):
    """``Limit100`` Pydantic type clamps (not rejects) at 100 — pinned
    because a runaway limit on the slow-urls list would dominate the
    result payload, and the silent clamp is what protects the
    frontend from absurd payload sizes."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_slow_urls", return_value={"has_data": False, "rows": []}) as mock:
        resp = client.post(
            "/api/origin/slow-urls",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "limit": 100_000},
        )

    assert resp.status_code == 200
    # The 100_000 input got clamped down to 100 before reaching the repo
    assert mock.call_args.kwargs["limit"] == 100


# ── /api/origin/status-codes ────────────────────────────────────────────────


def test_origin_status_codes_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/status-codes",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200


# ── /api/origin/path-breakdown ─────────────────────────────────────────────


def test_origin_path_breakdown_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/path-breakdown",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200


# ── /api/origin/pop-latency ────────────────────────────────────────────────


def test_origin_pop_latency_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/pop-latency",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "limit": 10},
    )
    assert resp.status_code == 200


# ── /api/origin/ip-health ──────────────────────────────────────────────────


def test_origin_ip_health_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/ip-health",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "limit": 10},
    )
    assert resp.status_code == 200


# ── /api/origin/shielding-analysis ─────────────────────────────────────────


def test_origin_shielding_analysis_accepts_limit_up_to_200(client, in_memory_duckdb, test_service_source):
    """Uses Limit200 (not Limit100) — pinned distinct from the other
    endpoints because the shield-arc map renders more rows than the
    list views."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/shielding-analysis",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "limit": 150},
    )
    assert resp.status_code == 200


def test_origin_shielding_analysis_clamps_limit_at_200(client, in_memory_duckdb, test_service_source):
    """``Limit200`` clamps oversized values (same pattern as Limit100)."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch(
        "backend.repositories.origin.get_shielding_analysis",
        return_value={"has_data": False, "rows": []},
    ) as mock:
        resp = client.post(
            "/api/origin/shielding-analysis",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "limit": 500},
        )

    assert resp.status_code == 200
    assert mock.call_args.kwargs["limit"] == 200


# ── /api/origin/summary error mapping via query_errors decorator ───────────


def test_origin_summary_value_error_maps_to_400(client):
    """The router uses ``@query_errors()`` — a ValueError from the
    repo must surface as 400, not 500. Pinned because the repo's
    filter parsing raises ValueError on bad filter shape and the
    frontend needs the 400 to render the inline error message."""
    with patch("backend.repositories.origin.get_summary", side_effect=ValueError("bad filter")):
        resp = client.post(
            "/api/origin/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}},
        )
    assert resp.status_code == 400


def test_origin_summary_lookup_error_maps_to_404(client):
    with patch("backend.repositories.origin.get_summary", side_effect=KeyError("unknown source")):
        resp = client.post(
            "/api/origin/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}},
        )
    assert resp.status_code == 404


# ── /api/origin/aggregates: selector contract (P-4 slice 4) ────────────────


def test_origin_aggregates_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    Literal). Either is an explicit reject so the FE never gets a
    silently-degraded 200 — pins the standardized selector contract
    across the 5 P-4 pages."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["not_a_section"]},
    )
    assert resp.status_code in (400, 422)


def test_origin_aggregates_summary_only_suppresses_other_timings(client, in_memory_duckdb, test_service_source):
    """sections=['summary'] must skip the other six section reads — the
    section_timings entries are the load-bearing signal the perf
    harness reads to attribute time. A phantom mark on a section that
    didn't actually run would corrupt that attribution."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["summary"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    timings = {t["section"] for t in data.get("_section_timings", [])}
    assert "summary" in timings
    for blocked in ("slow_urls", "timeseries", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
        assert blocked not in timings, f"summary-only selector leaked {blocked} timing; got {timings}"


def test_origin_aggregates_coupling_expands_ts_status_path(client, in_memory_duckdb, test_service_source):
    """sections=['timeseries'] alone must auto-expand to the
    {timeseries, status_codes, path_breakdown} triple (they share
    branch 3's pool conn — splitting them across requests would either
    add another checkout or serialize work that already shares one)."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["timeseries"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    timings = {t["section"] for t in data.get("_section_timings", [])}
    # Coupling rule expanded the request to include the other two on the
    # same branch — all three must fire.
    for need in ("timeseries", "status_codes", "path_breakdown"):
        assert need in timings, f"coupling did not include {need}; got {timings}"
    # Other branches must NOT fire.
    for blocked in ("summary", "slow_urls", "pop_latency", "ip_health"):
        assert blocked not in timings, f"selector did not suppress {blocked}; got {timings}"


def test_origin_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": "2026-08-24T00:00:00Z"}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window", return_value=("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z")
        ),
    ):
        resp = client.post(
            "/api/origin/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "range_token": "30d", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "summary" in resp.json()
