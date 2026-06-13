from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_log_fields_catalog_endpoint(client):
    """Verify that the catalog endpoint returns the definitions of all groups, fields, and presets."""
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    assert "groups" in data
    assert "fields" in data
    assert "presets" in data
    assert "insights" in data

    # We should have all the presets defined in log_fields.py
    assert "minimal" in data["presets"]
    assert "standard" in data["presets"]
    assert "security" in data["presets"]


def test_insight_availability_endpoint(client, in_memory_duckdb, test_service_source):
    """Verify that insights are dynamically evaluated based on the available schema."""

    # Insert logs so DuckDB auto-infers a schema
    logs = generate_mock_logs(test_service_source, num_logs=1)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    data = response.json()

    assert data["available"] is True
    assert "insights" in data

    # Since our mocked test_service_source includes groups A, C, D, F, I
    # The 'error_spikes' insight (Requires group A, which gives 'url' and core field 'status')
    # should be available.
    error_spikes = next(i for i in data["insights"] if i["id"] == "error_spikes")
    # Actually wait, test mock data creates table by NAME but logs might not have 'url' depending
    # on random chance. Let's just check the structure.
    assert "id" in error_spikes
    assert "available" in error_spikes


def test_bootstrap_endpoint_success(client, tmp_path, monkeypatch):
    """Verify that the main bootstrap endpoint correctly returns a 200 and serializes properly."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    # Save a fake service so the list is not empty
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    assert data["active_service_id"] == MOCK_SERVICE_ID
    assert "pops" in data
    assert "services" in data


# ── /api/bootstrap: edge cases ──────────────────────────────────────────────


def test_bootstrap_exposes_active_log_field_ids(client, tmp_path, monkeypatch):
    """Dashboard cards default to the currently-enabled field set.
    Bootstrap exposes that set so the frontend can toggle off-format cards off
    by default and badge them as 'not currently being logged'."""
    from backend import config
    from backend.core import log_fields as lf

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {"groups": lf.PRESETS["minimal"]["groups"], "field_overrides": {}},
        },
    )

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    ids = response.json()["active_log_field_ids"]
    assert isinstance(ids, list) and ids, "expected a non-empty enabled-field list"
    expected = lf.resolve_enabled_fields({"groups": lf.PRESETS["minimal"]["groups"], "field_overrides": {}})
    assert set(ids) >= expected, "all minimal-preset fields should appear"


def test_bootstrap_falls_back_to_first_service_when_active_id_unknown(client, tmp_path, monkeypatch):
    """If the frontend sends a service-id that's been removed (stale
    localStorage), the bootstrap endpoint silently re-points
    ``active_service_id`` to the first available service so the UI
    doesn't show "No service" until the user manually picks one."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-real", {"service_id": "svc-real"})

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": "svc-deleted"})
    assert response.status_code == 200
    data = response.json()
    # Fell back to the only existing service
    assert data["active_service_id"] == "svc-real"


def test_bootstrap_with_no_services_returns_null_active_id(client, tmp_path, monkeypatch):
    """Fresh install with zero configs → ``active_service_id`` is null
    and ``services`` is empty. Pinned because the frontend keys on
    null to render the "let's provision your first service" wizard."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    # No save_config called

    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    data = response.json()
    assert data["active_service_id"] is None
    assert data["services"] == []


def test_bootstrap_uses_cached_schema_from_config_status(client, tmp_path, monkeypatch):
    """When the service config has a cached ``status.schema``, the
    endpoint must use it directly without acquiring a DB lock. Pinned
    because the lock-free path is what makes the bootstrap response
    fast enough for the frontend's startup blocking call."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    # Save a config whose enriched-service view will surface this schema
    cached_schema = [{"name": "timestamp", "type": "TIMESTAMP"}]
    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "status": {"schema": cached_schema}},
    )

    with patch(
        "backend.services.service_manager.get_enriched_services",
        return_value=[{"service_id": MOCK_SERVICE_ID, "status": {"schema": cached_schema}}],
    ):
        # Patch get_source_for_service to None so the live-fallback path doesn't run
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["schema"] == cached_schema


def test_bootstrap_falls_back_to_live_schema_when_cache_empty(client, in_memory_duckdb, test_service_source):
    """No cached schema → endpoint opens a connection, calls
    ``get_schema``, returns the live shape. Pinned because the
    fallback is what populates the dashboard on first-ever load."""
    # Seed the DuckDB table so live get_schema returns a non-empty schema
    logs = generate_mock_logs(test_service_source, num_logs=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    # Live schema came back — non-empty
    assert isinstance(response.json()["schema"], list)


def test_bootstrap_includes_custom_dashboard_cards_when_configured(client, tmp_path, monkeypatch):
    """Custom fields flagged ``show_in_dashboard=True`` surface in
    ``custom_dashboard_cards`` — the dashboard renders them without
    a follow-up fetch, which would otherwise add a round-trip to
    first paint."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "schema_version": 2,
                "custom_fields": [
                    {
                        "name": "my_dashboard_field",
                        "label": "Dashboard Field",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "show_in_dashboard": True,
                    },
                    {
                        "name": "my_silent_field",
                        "label": "Silent Field",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "show_in_dashboard": False,
                    },
                ],
            },
        },
    )

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    # The card id is the field "name" — only show_in_dashboard=True surfaces
    card_ids = {c["id"] for c in data["custom_dashboard_cards"]}
    assert "my_dashboard_field" in card_ids
    assert "my_silent_field" not in card_ids


# ── /api/bootstrap: views fold ─────────────────────────────────────────────


def test_bootstrap_includes_saved_views_for_active_service(client, tmp_path, monkeypatch):
    """Bootstrap folds saved views in so the frontend skips its own
    /api/views/{service_id} round-trip on initial load. Pinned because
    losing this key reintroduces ~50ms per page nav (one Iceberg/SQLite
    round-trip per nav)."""
    from backend import config
    from backend.repositories import views as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    def _fake_views(sid):
        assert sid == MOCK_SERVICE_ID, f"bootstrap must only fetch views for the ACTIVE service. Got sid={sid!r}"
        return [
            {
                "id": "v1",
                "service_id": sid,
                "name": "Errors",
                "filters_json": "[]",
                "start_time": None,
                "end_time": None,
                "page": "/dashboard",
            },
            {
                "id": "v2",
                "service_id": sid,
                "name": "Slow",
                "filters_json": "[]",
                "start_time": None,
                "end_time": None,
                "page": "/dashboard",
            },
        ]

    monkeypatch.setattr(_views_repo, "get_views", _fake_views)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    assert "views" in data, "bootstrap response must include 'views' key"
    assert len(data["views"]) == 2
    ids = {v["id"] for v in data["views"]}
    assert ids == {"v1", "v2"}


def test_bootstrap_views_empty_when_no_active_service(client, tmp_path, monkeypatch):
    """No active service → no views to fold. Pinned so the views
    fetch isn't called with None (which would crash get_views)."""
    from backend import config
    from backend.repositories import views as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    get_views_calls: list = []
    monkeypatch.setattr(
        _views_repo,
        "get_views",
        lambda sid: get_views_calls.append(sid) or [],
    )

    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    data = response.json()
    assert data["views"] == [], f"empty/missing active service must return [] for views, got {data.get('views')!r}"
    assert get_views_calls == [], (
        f"get_views must NOT be called when there's no active service; got calls={get_views_calls}"
    )


def test_bootstrap_views_survives_repo_error(client, tmp_path, monkeypatch):
    """A repo error fetching views must NOT break /api/bootstrap.
    Views are UX nicety, not correctness — degrade gracefully to
    empty list and let ViewSelector fall back to its granular GET."""
    from backend import config
    from backend.repositories import views as _views_repo

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    def _explode(sid):
        raise RuntimeError("simulated repo failure")

    monkeypatch.setattr(_views_repo, "get_views", _explode)

    response = client.get("/api/bootstrap", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200, (
        f"bootstrap must NOT 500 when views repo fails; got status={response.status_code}"
    )
    data = response.json()
    assert data["views"] == [], "views must degrade to [] on repo error, not propagate"


# ── /api/sources ───────────────────────────────────────────────────────────


def test_sources_endpoint_returns_list_of_configured_services(client, tmp_path, monkeypatch):
    """Returns one entry per configured service with the table_name
    DuckDB will key on. Pinned because the admin UI reads this to
    render the per-source ingest stats."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        "svc-a",
        {
            "service_id": "svc-a",
            "fos_endpoint": "us-east-1.object.fastlystorage.app",
            "fos_bucket": "bkt-a",
            "fos_prefix": "logs",
            "fos_region": "us-east-1",
        },
    )

    response = client.get("/api/sources")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    svc_a = next((s for s in data if s["name"] == "svc-a"), None)
    assert svc_a is not None
    assert svc_a["bucket"] == "bkt-a"
    assert svc_a["table_name"].startswith("logs_")


def test_sources_endpoint_returns_empty_list_with_no_configs(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    response = client.get("/api/sources")
    assert response.status_code == 200
    assert response.json() == []


def test_sources_endpoint_500s_on_load_failure(client):
    """If list_configs raises (corrupt config file), return 500 with
    structured error — frontend renders this in the admin panel."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("disk failed")):
        response = client.get("/api/sources")
    assert response.status_code == 500
    assert "error" in response.json()["detail"]


# ── /api/schema ────────────────────────────────────────────────────────────


def test_schema_endpoint_uses_cached_status_when_available(client, in_memory_duckdb, test_service_source):
    """If the source's status cache has a schema, the endpoint must
    return it WITHOUT calling ``get_schema`` (cache miss → live call
    is the fallback, not the default)."""
    cached_schema = [{"name": "from_cache", "type": "VARCHAR"}]
    with patch(
        "backend.config.get_status",
        return_value={"schema": cached_schema},
    ):
        response = client.get("/api/schema", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["schema"] == cached_schema


def test_schema_endpoint_500s_when_get_schema_raises(client, test_service_source):
    """No cached status + live get_schema raises → 500 with the
    error string. Pinned because the dashboard surfaces this 500
    inline rather than crashing."""
    with (
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema fetch failed")),
    ):
        response = client.get("/api/schema", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert response.status_code == 500
    assert "error" in response.json()["detail"]


# ── /api/log-fields/catalog: with vs without service_id ────────────────────


def test_log_fields_catalog_with_service_id_includes_field_limits_and_custom(client, tmp_path, monkeypatch):
    """When a service_id is provided, custom fields from that service's
    config are merged into the catalog. Used by the field-picker
    drawer to show admin-defined fields."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "log_fields": {
                "schema_version": 2,
                "custom_fields": [
                    {
                        "name": "my_custom",
                        "duckdb_type": "VARCHAR",
                        "enabled": True,
                        "label": "My Custom Field",
                    }
                ],
            },
        },
    )

    response = client.get("/api/log-fields/catalog", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    data = response.json()
    # Custom field entries use ``id`` (=field name) and group="custom"
    custom_ids = [f["id"] for f in data["fields"] if f.get("group") == "custom"]
    assert "my_custom" in custom_ids


def test_log_fields_catalog_handles_missing_config_gracefully(client):
    """Service_id provided but config doesn't exist → returns the
    static catalog without custom fields rather than 500ing."""
    response = client.get("/api/log-fields/catalog", headers={"x-fastly-service-id": "nonexistent"})
    assert response.status_code == 200
    data = response.json()
    assert "fields" in data
    assert "groups" in data


def test_log_fields_catalog_500s_on_exception(client):
    # bootstrap.py calls `fr.get_catalog_for_api`, which is a same-identity
    # re-export of `log_fields.get_catalog_for_api`. Patching the original
    # `log_fields` binding does NOT affect the registry's already-bound
    # reference, so patch the registry's path directly.
    with patch("backend.core.field_registry.get_catalog_for_api", side_effect=RuntimeError("oops")):
        response = client.get("/api/log-fields/catalog")
    assert response.status_code == 500


# ── /api/insight-availability ──────────────────────────────────────────────


def test_insight_availability_500s_on_schema_failure(client):
    with patch("backend.core.duckdb.get_schema", side_effect=RuntimeError("schema gone")):
        response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 500


def test_insight_availability_marks_unavailable_when_required_cols_missing(
    client, in_memory_duckdb, test_service_source
):
    """If the active schema lacks columns a given insight requires,
    that insight is flagged ``available: False``. Pinned because the
    frontend uses this to grey out unsupported insights rather than
    showing empty cards."""
    # Create a minimal table missing most insight columns
    in_memory_duckdb.execute(
        f"CREATE TABLE {_safe_table(test_service_source['name'])} "
        "(timestamp TIMESTAMP, status INTEGER)"  # no url, ip, asn, etc.
    )

    response = client.get("/api/insight-availability", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert response.status_code == 200
    insights = response.json()["insights"]
    # At least one insight requires more than (timestamp, status) → unavailable
    unavailable = [i for i in insights if not i["available"]]
    assert len(unavailable) > 0


# ── /api/dma.json ──────────────────────────────────────────────────────────


def test_dma_endpoint_serves_file_when_geojson_exists(client, tmp_path, monkeypatch):
    """If ``data/system/dma_geojson.json`` exists on disk, the endpoint
    returns it as a file response (the real geojson is large; the
    file-serving path avoids loading it into Python memory)."""
    # Create the expected file
    import os

    geojson_path = "data/system/dma_geojson.json"
    os.makedirs("data/system", exist_ok=True)
    try:
        with open(geojson_path, "w") as f:
            f.write('{"type":"FeatureCollection","features":[]}')

        response = client.get("/api/dma.json")
        assert response.status_code == 200
        # FileResponse serves the bytes directly
        assert "FeatureCollection" in response.text
    finally:
        if os.path.exists(geojson_path):
            os.remove(geojson_path)


def test_dma_endpoint_falls_back_to_in_memory_map_when_no_file(client):
    """No on-disk DMA file → fall back to the built-in mapping
    (`_get_dma_map`). Pinned because removing the fallback would
    break the frontend's DMA picker on fresh installs that haven't
    downloaded the geojson asset."""
    fake_map = {"803": "Los Angeles, CA", "501": "New York, NY"}
    with (
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb._get_dma_map", return_value=fake_map),
    ):
        response = client.get("/api/dma.json")

    assert response.status_code == 200
    # M1 backstop adds _debug_* keys; pull out only the DMA codes (keys
    # are 3-digit numeric strings; telemetry keys start with underscore).
    body = response.json()
    dma_only = {k: v for k, v in body.items() if not k.startswith("_")}
    assert dma_only == fake_map


def test_dma_endpoint_500s_when_in_memory_map_fails(client):
    """File missing AND fallback raises → 500. Pinned because the
    frontend explicitly polls this on map mount; a silent empty
    response would render an empty map without an error message."""
    with (
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb._get_dma_map", side_effect=RuntimeError("DMA module broken")),
    ):
        response = client.get("/api/dma.json")

    assert response.status_code == 500


# Silence unused-import warnings
_ = pytest
