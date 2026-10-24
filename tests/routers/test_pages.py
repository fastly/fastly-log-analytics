from __future__ import annotations

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_network_health_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/network-health", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "available" in response.json()


def test_network_quality_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/network-quality", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "available" in response.json()


def test_origin_summary_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/origin/summary", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "_debug_queries" in response.json()


def test_security_aggregates_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/security/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "tls_fingerprints" in response.json()


def test_usage_log_activity_endpoint(client):
    # No data seeded — endpoint should still return an empty result.
    response = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert "total_rows" in data
    assert "total_bytes" in data


def test_query_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": f"SELECT count(*) FROM {table_name}"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "columns" in data
    assert "data" in data
    assert len(data["data"]) == 1


def test_sessions_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post("/api/sessions", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}})

    assert response.status_code == 200, response.text
    data = response.json()
    assert "sessions" in data
    assert "total" in data


def test_alerts_endpoint(client):
    response = client.get(f"/api/alerts/{MOCK_SERVICE_ID}")
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_views_endpoint(client):
    response = client.get(f"/api/views/{MOCK_SERVICE_ID}")
    assert response.status_code == 200, response.text
    assert isinstance(response.json(), list)


def test_log_activity_endpoint_returns_data_key(client, test_service_source):
    """Regression: /api/usage/log-activity must return 'data' list with row_count/bytes, not generated/processed."""
    from backend.core import metadata as metadata_db

    src_name = test_service_source["name"]
    con = metadata_db.get_con(src_name)
    # ``ingested_at`` is stored as ISO 8601 with a ``T`` separator
    # (see commit 9af3c8f normalising metadata_db writes). The query
    # parameters land as ``YYYY-MM-DDTHH:MM:SSZ`` via parse_date_window,
    # so the stored values must also use ``T`` for the string-range
    # comparison to match.
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("log1.gz", src_name, 100, 5000, "2026-01-01T10:00:00Z"),
            ("log2.gz", src_name, 200, 8000, "2026-01-01T11:00:00Z"),
        ],
    )
    con.commit()

    response = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02", "by": "hour"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data, f"Expected 'data' key, got: {list(data.keys())}"
    assert "total_rows" in data
    assert "total_bytes" in data
    assert "granularity" in data
    assert data["granularity"] == "hour"
    assert isinstance(data["data"], list)
    assert len(data["data"]) > 0
    assert "row_count" in data["data"][0]
    assert "bytes" in data["data"][0]
    assert "time" in data["data"][0]
    assert data["total_rows"] == 300
    assert data["total_bytes"] == 13000
    assert "generated" not in data
    assert "processed" not in data


# ── Merged from test_missing_service_id_400.py ──

import pytest
from fastapi.testclient import TestClient

from backend.main import app

# Routes that require an explicit service id and report it as ``service_id_required``.
EXPLICIT_SERVICE_ID_ROUTES = [
    ("GET", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/slow-queries/count", None),
    ("DELETE", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/clickhouse/status", None),
    ("GET", "/api/admin/vcl-health", None),
    ("GET", "/api/admin/ledger/quarantine", None),
    ("GET", "/api/provision/check-config", {"token": "t", "cdn_service_id": "c", "bucket": "b"}),
    ("GET", "/api/provision/ngwaf-workspaces", {"token": "t"}),
    ("GET", "/api/log-extents/stream", None),
    ("DELETE", "/api/alerts/alert-1", None),
    ("DELETE", "/api/views/view-1", None),
]

# Analytics routes resolve the service through RequestContext and report ``no_service``.
ANALYTICS_ROUTES = [
    ("POST", "/api/dashboard/aggregates"),
    ("POST", "/api/origin/aggregates"),
    ("POST", "/api/security/aggregates"),
    ("POST", "/api/performance/aggregates"),
    ("POST", "/api/network-health"),
]


@pytest.fixture
def bare_client():
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.clear()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(saved)


@pytest.mark.parametrize(("method", "path", "params"), EXPLICIT_SERVICE_ID_ROUTES)
def test_missing_service_id_is_400_service_id_required(bare_client, method, path, params) -> None:
    resp = bare_client.request(method, path, params=params)

    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == {"error": "service_id_required"}


@pytest.mark.parametrize(("method", "path"), ANALYTICS_ROUTES)
def test_analytics_route_without_service_is_400_envelope(bare_client, method, path) -> None:
    resp = bare_client.request(method, path, json={})

    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "no_service"
    assert detail["no_service"] is True


# ── Merged from test_route_methods.py ──

import re

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.routing import Mount

# Substitute path parameters with benign test values so the path matches the route pattern.
_PATH_SUBS = {
    "{service_id}": "test-service-id",
    "{run_id}": "1",
    "{log_id}": "1",
    "{source_id}": "wellknown",
    "{alert_id}": "test-alert-id",
    "{view_id}": "test-view-id",
}

_METHOD_PRIORITY = ["GET", "POST", "PUT", "PATCH", "DELETE"]


def _sub_path(path: str) -> str:
    for placeholder, value in _PATH_SUBS.items():
        path = path.replace(placeholder, value)
    return path


def _pick_wrong_method(registered: set[str]) -> str | None:
    for m in _METHOD_PRIORITY:
        if m not in registered:
            return m
    return None  # route accepts every method — nothing to test


def _normalize_template(path: str) -> str:
    """Replace all {param_name} placeholders with {p} so structurally identical
    path patterns like /views/{service_id} and /views/{view_id} hash together."""
    return re.sub(r"\{[^}]+\}", "{p}", path)


def _norm_to_regex(norm: str) -> re.Pattern[str]:
    """Convert a normalised template to a regex that matches concrete paths."""
    parts = norm.split("{p}")
    return re.compile(
        "".join(re.escape(p) + "([^/]+)" if i < len(parts) - 1 else re.escape(p) for i, p in enumerate(parts))
    )


def _shadowed_by_another_route(path: str, wrong_method: str, own_norm: str) -> bool:
    """Return True if *path* would be matched AND handled by a different route that
    accepts *wrong_method*.  In that case, calling wrong_method on path would NOT
    return 405 — it would hit the other handler instead — so this case is untestable.

    Example: /api/alerts/preview is the literal path for POST /alerts/preview, but
    GET /alerts/{service_id} also matches "preview" as a service_id value.  Testing
    GET on /api/alerts/preview would invoke that handler (500 or 200), not return 405.
    """
    for norm, methods in _methods_by_norm.items():
        if norm == own_norm:
            continue
        if wrong_method not in methods:
            continue
        # Does this other normalised template match our concrete test path?
        if _norm_to_regex(norm).fullmatch(path):
            return True
    return False


# Build an app with the same routes but no static-file mount.
# The static mount at "/" intercepts GET requests on POST-only paths and returns
# 404 (file not found) instead of 405 (method not allowed), making the test useless.
_test_app = FastAPI()
for _r in app.routes:
    if not isinstance(_r, Mount):
        _test_app.routes.append(_r)

# Collect one test case per unique normalised path template, combining methods
# across all routes with the same structure.  Without normalisation, routes like
# /api/views/{service_id} (GET) and /api/views/{view_id} (DELETE) look like
# different templates but match the same URL.  Testing GET on /api/views/test-view-id
# would trigger the {service_id} GET handler (not a 405), masking the real issue.
#
# Canonical substituted path is taken from whichever route we see first.
_methods_by_norm: dict[str, set[str]] = {}
_path_by_norm: dict[str, str] = {}
for _route in app.routes:
    if isinstance(_route, APIRoute):
        _norm = _normalize_template(_route.path)
        _methods_by_norm.setdefault(_norm, set()).update(_route.methods or set())
        if _norm not in _path_by_norm:
            _path_by_norm[_norm] = _sub_path(_route.path)

_cases: list[tuple[str, str, str]] = []
for _norm, _all_methods in sorted(_methods_by_norm.items()):
    _path = _path_by_norm[_norm]
    _wrong = _pick_wrong_method(_all_methods)
    if _wrong and not _shadowed_by_another_route(_path, _wrong, _norm):
        _cases.append((_wrong, _path, str(sorted(_all_methods))))


@pytest.mark.parametrize(
    "wrong_method,path,registered",
    _cases,
    ids=[f"{wrong} {path}" for wrong, path, _ in _cases],
)
def test_wrong_http_method_returns_405_not_404(wrong_method: str, path: str, registered: str) -> None:
    """Calling the wrong HTTP method on a registered route must return 405, not 404.

    405 = route exists, method wrong.
    404 = route not found — the URL the client is calling doesn't match any registered path.
    """
    client = TestClient(_test_app, raise_server_exceptions=False)
    kwargs = {"json": {}} if wrong_method in ("POST", "PUT", "PATCH") else {}
    resp = getattr(client, wrong_method.lower())(path, **kwargs)
    assert resp.status_code == 405, (
        f"{wrong_method} {path} returned {resp.status_code} "
        f"(registered methods: {registered}). "
        f"A 404 here means the client URL doesn't match any route — check path construction."
    )


# ── Merged from test_response_cache_wire.py ──

import json
from datetime import UTC, datetime, timedelta

import pytest

from backend.repositories._base import QueryRunner
from backend.utils import bot_sources


@pytest.fixture
def observation_request(in_memory_duckdb, test_service_source, tmp_path, monkeypatch) -> dict:
    in_memory_duckdb.execute("SET TimeZone = 'UTC'")
    test_service_source["_cache_dir_override"] = str(tmp_path / "request-cache")
    # Fixture rows exist only in the view, not the live-hour buffer or lake.
    monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
    source_dir = tmp_path / "bot-sources"
    source_dir.mkdir()
    (source_dir / "well-known-bots.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "id": "googlebot",
                        "name": "Googlebot",
                        "pattern": {"accepted": ["Googlebot"]},
                        "verification": {"domains": [], "cidrs": []},
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", source_dir)
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    seed_time = datetime.now(UTC)
    closed_time = seed_time.replace(minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    for index, log in enumerate(logs):
        log.update(
            timestamp=(seed_time - timedelta(microseconds=1) if index % 2 else closed_time).isoformat(),
            ua="Googlebot/2.1",
            tcp_rtt=1000,
            asn=15169,
            country="US",
            cmcd_sid="test-session",
            cmcd_bl=1000,
            cmcd_bs=False,
            cmcd_br=1000,
            cmcd_mtp=2000,
            cmcd_ot="v",
            cmcd_sf="h",
            cmcd_cid="test-content",
        )
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    now = datetime.now(UTC)
    return {"start_time": (now - timedelta(hours=2)).isoformat(), "end_time": now.isoformat(), "filters": {}}


@pytest.mark.parametrize(
    "path,sections",
    [
        ("/api/security/aggregates", ["ipv6_adoption"]),
        ("/api/performance/aggregates", ["ttl_dist"]),
        ("/api/cmcd/aggregates", ["overview"]),
        ("/api/dashboard/aggregates", ["core"]),
    ],
)
def test_standard_owner_miss_and_router_peek_hit_survive_wire_serialization(
    client,
    observation_request: dict,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    sections: list[str],
) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    body = {**observation_request, "sections": sections}
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post(path, json=body, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "miss"
    second = client.post(path, json=body, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["_response_cache"]["state"] == "hit"
    assert second.json()["_response_cache"]["prewarmer"] is False
    assert second.json()["_is_cached"] is True
    assert "_debug_queries" not in second.json()


def test_dashboard_bundle_components_keep_independent_observations(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "bypass"
    assert first.json()["aggregates"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["bots"][0]["name"] == "Googlebot"
    assert first.json()["top_bots"]["bots"][0]["request_count"] == 40
    second = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["aggregates"]["_response_cache"]["state"] == "hit"
    assert second.json()["top_bots"]["_response_cache"]["state"] == "hit"


def test_origin_composite_is_bypass_not_a_response_miss(client, observation_request: dict) -> None:
    for _ in range(2):
        response = client.post(
            "/api/origin/aggregates",
            json={**observation_request, "sections": ["summary"]},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert response.status_code == 200, response.text
        assert response.json()["_response_cache"] == {"state": "bypass", "prewarmer": False}


def test_network_bundle_does_not_confuse_health_with_independent_shielding(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    for expected in ("miss", "hit"):
        response = client.post("/api/network/bundle", json=observation_request, headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["_response_cache"]["state"] == "bypass"
        assert body["health"]["_response_cache"]["state"] == expected
        assert body["health"]["_response_cache"]["scope"] == "self"
        assert body["quality"]["_response_cache"]["state"] == "bypass"
        assert body["transport"]["_response_cache"]["state"] == "bypass"
        assert body["health"]["shielding_analysis"]["_response_cache"]["state"] in {"miss", "hit"}
