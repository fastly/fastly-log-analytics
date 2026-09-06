"""Tests for ``backend.routers.usage`` — cost estimator + FOS usage telemetry.

The usage router renders the "Estimated cost" panel on the admin
dashboard. Wrong numbers here would mis-inform customer billing
expectations, so the per-endpoint shape contracts are important.

Focused on:
  - ``_extract_fos_ops`` flatten-vs-nested record shapes
  - ``_fastly_api`` URL/auth header construction
  - ``/api/usage/prefill`` global-rates surfacing + log-period propagation
  - ``/api/usage/current-storage`` bucket validation + delete_after branch
  - ``/api/usage/operations`` granularity clamping
  - ``/api/usage/log-activity`` granularity validation
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.routers.usage import _extract_fos_ops, _fastly_api
from tests.conftest import MOCK_SERVICE_ID

# ── _extract_fos_ops: flatten vs nested ────────────────────────────────────


def test_extract_fos_ops_reads_flattened_keys():
    """The aggregate endpoint usually returns flattened keys —
    ``object_storage_class_a_operations_count`` etc. — so the helper
    reads those first."""
    record = {
        "object_storage_class_a_operations_count": 1234,
        "object_storage_class_b_operations_count": 5678,
        "other_metric": 9999,
    }
    a, b = _extract_fos_ops(record)
    assert a == 1234
    assert b == 5678


def test_extract_fos_ops_falls_back_to_nested_shape():
    """If the flattened keys are absent, fall back to the nested
    ``object_storage`` dict. Pinned because Fastly returns both
    shapes across endpoints and dropping the fallback would silently
    zero the customer's billed-ops chart."""
    record = {
        "object_storage": {
            "class_a_operations_count": 100,
            "class_b_operations_count": 200,
        }
    }
    a, b = _extract_fos_ops(record)
    assert a == 100
    assert b == 200


def test_extract_fos_ops_returns_zero_when_no_data():
    """No matching keys → (0, 0). Pinned because the caller sums
    these across days; returning None would crash the sum."""
    a, b = _extract_fos_ops({"other": 42})
    assert a == 0
    assert b == 0


def test_extract_fos_ops_handles_none_values_as_zero():
    """Null values from the API → 0 (not int(None)). Pinned because
    Fastly returns null for buckets with no activity."""
    record = {
        "object_storage_class_a_operations_count": None,
        "object_storage_class_b_operations_count": None,
    }
    a, b = _extract_fos_ops(record)
    assert (a, b) == (0, 0)


def test_extract_fos_ops_prefers_flat_over_nested_when_both_present():
    """When both shapes co-exist (legacy + new API together), the
    flattened form wins. Pinned to lock in the precedence — flipping
    it would double-count for the brief overlap period."""
    record = {
        "object_storage_class_a_operations_count": 999,
        "object_storage_class_b_operations_count": 999,
        "object_storage": {"class_a_operations_count": 1, "class_b_operations_count": 1},
    }
    a, b = _extract_fos_ops(record)
    assert (a, b) == (999, 999)


# ── _fastly_api: URL + headers ─────────────────────────────────────────────


def test_fastly_api_constructs_correct_url_with_auth_header():
    """URL = ``https://api.fastly.com{path}`` and ``Fastly-Key`` header.
    Pinned because losing the Bearer-style auth would 401 every call."""
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"data": []}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
        result = _fastly_api("/services/svc1/details", "test-token")

    assert result == {"data": []}
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://api.fastly.com/services/svc1/details"
    assert req.get_header("Fastly-key") == "test-token"


# ── /api/usage/prefill ─────────────────────────────────────────────────────


def test_prefill_returns_global_rates(client, tmp_path, monkeypatch):
    """The prefill endpoint hydrates the cost estimator with the
    currently-configured per-class rates. Pinned because the
    frontend keys on these exact field names to populate inputs."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"},
    )
    config.save_usage_logging_config(
        {
            "enabled": True,
            "retention_days": 30,
            "class_a_rate_per_1k": 0.123,
            "class_b_rate_per_10k": 0.456,
            "cdn_egress_rate_per_gb": 0.789,
            "storage_rate_per_gb_month": 0.021,
        }
    )

    resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    body = resp.json()
    assert body["class_a_rate_per_1k"] == 0.123
    assert body["class_b_rate_per_10k"] == 0.456
    assert body["cdn_egress_rate_per_gb"] == 0.789
    assert body["storage_rate_per_gb_month"] == 0.021


def test_prefill_returns_defaults_when_no_usage_config_saved(client, tmp_path, monkeypatch):
    """A fresh install has no usage_logging config → defaults
    (0.005 / 0.01 / 0.12 / 0.02) surface. Pinned to lock the
    default values the cost estimator falls back to."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(
        MOCK_SERVICE_ID,
        {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"},
    )

    resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    body = resp.json()
    assert body["class_a_rate_per_1k"] == 0.005
    assert body["class_b_rate_per_10k"] == 0.01


# ── /api/usage/current-storage: required-bucket guard ─────────────────────


def test_current_storage_400s_when_bucket_not_configured(client):
    """Source without ``bucket`` → 400 with a friendly error. Pinned
    because the admin UI keys on this 400 to render the "configure
    FOS" call-to-action."""
    fake_src = {"name": "svc", "service_id": "svc"}  # no bucket

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: fake_src
    try:
        resp = client.get("/api/usage/current-storage")
    finally:
        # leave the fixture-provided override in place — TestClient resets
        pass

    assert resp.status_code == 400
    assert "bucket" in resp.json()["detail"]["error"].lower()


def _complete_src(**overrides) -> dict:
    """Source with all the fields the usage router reads."""
    base = {
        "name": "test_service",
        "service_id": MOCK_SERVICE_ID,
        "logging_service_id": MOCK_SERVICE_ID,
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
    }
    base.update(overrides)
    return base


# ── /api/usage/operations: end-to-end with granularity clamp ───────────────


def test_operations_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """``by`` values outside {hour, day} clamp to ``hour`` (NOT
    rejected). Pinned because the frontend defaults to "minute" for
    other endpoints and we don't want to 500 if the user accidentally
    sends "minute" here.

    Regression: this also exercises the date-format fix where
    ``parse_date_window`` returns ISO-T-Z strings and the route
    must NOT call ``strptime("%Y-%m-%d %H:%M:%S")`` on them.
    """
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    captured: dict = {}

    def _capture_fastly(path, api_key):
        captured["path"] = path
        return {"data": []}

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=_capture_fastly):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"by": "minute"},
        )

    assert resp.status_code == 200
    # The clamped granularity must end up in the API path
    assert "by=hour" in captured["path"]
    # Date-format regression: the from/to timestamps must be valid Unix
    # seconds (i.e. parse_window_str_to_dt successfully parsed the
    # ISO-T-Z value parse_date_window produced).
    assert "from=" in captured["path"] and "to=" in captured["path"]


# ── /api/usage/current-storage: end-to-end (post date-format fix) ─────────


def test_current_storage_delete_after_branch_uses_iceberg_as_live(client):
    """``delete_after=True`` → raw files counted as "deleted" and
    Iceberg files as "live". Pinned because flipping this would
    double-count the customer's bill.

    Regression: this also exercises the date-format fix in
    parse_window_str_to_dt and the None-cfg defensive guard."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Size": 5000}, {"Size": 3000}]},
    ]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        # get_storage_stats is patched below so the real connection is never
        # used; without this stub the route would try to open the default
        # ``logs.duckdb`` relative path read-only and crash with IO Error.
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 8000, "data_files": 2},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 10, "total_bytes": 12345, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["live_bytes"] == 8000  # iceberg only
    assert body["deleted_bytes"] == 12345  # raw → deleted bucket


def test_current_storage_no_delete_branch_sums_raw_and_iceberg_as_live(client):
    """``delete_after=False`` → all bytes are "live" with zero "deleted"."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Size": 1000}]},
    ]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": False, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 1000, "data_files": 1},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 5, "total_bytes": 4000, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["live_bytes"] == 5000  # iceberg (1000) + raw (4000)
    assert body["deleted_bytes"] == 0


def test_current_storage_handles_missing_config_gracefully(client):
    """REGRESSION: when ``load_config(src['name'])`` returns None
    (config missing / mid-teardown), the route must NOT crash with
    AttributeError on ``cfg.get(...)``. Pinned because this is the
    defensive None-guard added in the same commit as the date-format
    fix."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.config.load_config", return_value=None),  # config missing
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 0, "data_files": 0},
        ),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={"total_files": 0, "total_bytes": 0, "debug_queries": []},
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Must not 500
    assert resp.status_code == 200


def test_current_storage_emits_gb_hours_for_billing_card(client):
    """The 30-day minimum billing window is baked into ``total_billed_gb_hours``.
    Pinned because the frontend's cost card multiplies this by the
    per-GB-month rate and divides by 720 — a refactor that changed
    the basis would silently 30x or /30 the bill estimate."""
    from backend.deps import get_source
    from backend.main import app

    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}},
        ),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.repositories.usage.get_storage_stats",
            return_value={
                # 1 GB = 1024**3 bytes
                "total_files": 1,
                "total_bytes": 1024**3,
                "debug_queries": [],
            },
        ),
    ):
        resp = client.get("/api/usage/current-storage", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # 1 GB * (30 * 24 hours) = 720 GB-hours
    assert body["total_billed_gb_hours"] == 720.0


# ── /api/usage/log-activity: granularity validation ────────────────────────


def test_log_activity_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """Mirror of /operations — invalid ``by`` clamps to ``hour``."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    resp = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02", "by": "fortnight"},
    )

    assert resp.status_code == 200
    assert resp.json()["granularity"] == "hour"


def test_log_activity_accepts_supported_granularities(client, tmp_path, monkeypatch):
    """Supported values are {second, minute, hour, day} — all pass
    through unchanged. Pinned because the FE dropdown lists these
    four; adding/removing would diverge from the API contract."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    for by in ("second", "minute", "hour", "day"):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": by},
        )
        assert resp.status_code == 200
        assert resp.json()["granularity"] == by


# ── /api/usage/operations: 403 + 502 mapping + accumulation ───────────────


def test_operations_403_when_no_fastly_api_key(client, tmp_path, monkeypatch):
    """No ``fastly_api_key`` on the service config → 403 with friendly
    error. Pinned because the FE renders the "configure API key" CTA
    keyed on this exact 403."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # No fastly_api_key saved
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})

    app.dependency_overrides[get_source] = lambda: _complete_src()
    resp = client.get(
        "/api/usage/operations",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02"},
    )

    assert resp.status_code == 403
    assert "api key" in resp.json()["detail"]["error"].lower()


def test_operations_accumulates_class_a_and_class_b_per_day(client, tmp_path, monkeypatch):
    """End-to-end accumulation: two records on the same day are summed,
    a different day is its own bucket. Pinned because the billing card
    keys on the per-day shape sort-by-date."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    # Two records on 2026-01-05, one on 2026-01-06
    fake_payload = {
        "data": [
            {
                "start_time": 1767657600,  # 2026-01-05 00:00 UTC
                "object_storage_class_a_operations_count": 100,
                "object_storage_class_b_operations_count": 50,
            },
            {
                "start_time": 1767661200,  # 2026-01-05 01:00 UTC
                "object_storage_class_a_operations_count": 200,
                "object_storage_class_b_operations_count": 25,
            },
            {
                "start_time": 1767744000,  # 2026-01-06 00:00 UTC
                "object_storage_class_a_operations_count": 7,
                "object_storage_class_b_operations_count": 8,
            },
        ]
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", return_value=fake_payload):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "day"},
        )

    assert resp.status_code == 200
    body = resp.json()
    # Totals reflect all three records summed
    assert body["total_class_a"] == 307
    assert body["total_class_b"] == 83
    # Two records on the same day are merged into one bucket, the
    # third record gets its own bucket — total 2 distinct day buckets
    assert len(body["data"]) == 2
    # And the bucket with the same-day pair sums to 300/75
    day_buckets = sorted(body["data"], key=lambda d: d["date"])
    assert day_buckets[0]["class_a"] == 300
    assert day_buckets[0]["class_b"] == 75
    assert day_buckets[1]["class_a"] == 7
    assert day_buckets[1]["class_b"] == 8


def test_operations_maps_http_error_to_502(client, tmp_path, monkeypatch):
    """Fastly Stats upstream failure → 502 with a generic error code +
    correlation id. The upstream status and body are NOT echoed to the
    client (they can contain internal hostnames / token fragments per
    the v2.0 raise_internal sweep); operators triage via the server log
    keyed on ``error_id``. Pinned because the FE distinguishes 502
    (transient upstream) from 4xx (config issue) when retrying.
    """
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    err = RuntimeError("HTTP 503 GET /stats/aggregate\n    service unavailable")

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=err):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502
    body = resp.json()["detail"]
    assert body["error"] == "fastly_stats_aggregate_failed"
    assert "error_id" in body
    # Critical: upstream status/body MUST NOT leak through the wire.
    assert "503" not in body["error"]
    assert "service unavailable" not in body["error"]


def test_operations_maps_generic_exception_to_502(client, tmp_path, monkeypatch):
    """Non-HTTPError exceptions (connection errors, timeouts) also
    map to 502 — same retry semantics on the FE."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("connection refused")):
        resp = client.get(
            "/api/usage/operations",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502


# ── /api/usage/bandwidth: 403 + cdn_service_id branch ────────────────────


def test_bandwidth_403_when_no_fastly_api_key(client, tmp_path, monkeypatch):
    """Mirror of operations: no API key → 403."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID})  # no api_key

    app.dependency_overrides[get_source] = lambda: _complete_src()
    resp = client.get(
        "/api/usage/bandwidth",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 403


def test_bandwidth_skips_fastly_call_when_no_cdn_service_id(client, tmp_path, monkeypatch):
    """When ``cdn_service_id`` is empty, the route returns an empty
    data array WITHOUT calling Fastly. Pinned because making the call
    against an empty service ID would 404 the Stats API and surface
    as a 502 to the user — confusing for unprovisioned services."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="")
    with patch("backend.routers.usage._fastly_api") as mock_api:
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 200
    assert resp.json()["data"] == []
    assert resp.json()["total_bytes"] == 0
    # No Fastly call was made
    mock_api.assert_not_called()


def test_bandwidth_clamps_invalid_granularity_to_hour(client, tmp_path, monkeypatch):
    """``by`` values outside {hour, minute, day} clamp to ``hour``."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    captured = {}

    def _capture(path, api_key):
        captured["path"] = path
        return {"data": []}

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", side_effect=_capture):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "fortnight"},
        )

    assert resp.status_code == 200
    assert resp.json()["granularity"] == "hour"
    assert "by=hour" in captured["path"]


def test_bandwidth_aggregates_payload_into_points(client, tmp_path, monkeypatch):
    """End-to-end: bandwidth records aggregate by timestamp and the
    total_bytes sum lands in the response. Pinned because the cost
    card multiplies total_bytes by the per-GB egress rate."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    payload = {
        "data": [
            {"start_time": 1767657600, "bandwidth": 1000, "requests": 5},
            {"start_time": 1767657600, "bandwidth": 500, "requests": 2},  # same ts → merged
            {"start_time": 1767661200, "bandwidth": 300, "requests": 1},
        ]
    }

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", return_value=payload):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "hour"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_bytes"] == 1800  # 1000 + 500 + 300
    # Two distinct time buckets after merging the duplicate timestamp
    assert len(body["data"]) == 2


def test_bandwidth_maps_fastly_exception_to_502(client, tmp_path, monkeypatch):
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "tok"})

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-1")
    with patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("timeout")):
        resp = client.get(
            "/api/usage/bandwidth",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02"},
        )

    assert resp.status_code == 502


# ── /api/usage/log-activity: enrichment branches ──────────────────────────


def test_log_activity_enriches_with_api_request_counts(client, tmp_path, monkeypatch):
    """When ``fastly_api_key`` + ``logging_service_id`` exist AND
    ``by`` ∈ {minute, hour, day}, the route enriches the repo-returned
    rows with ``api_requests`` from /stats/service. Pinned because
    the "logs generated vs processed" panel keys on this enrichment."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    # Use a time bucket that lines up with the Fastly payload's
    # start_time → the route uses datetime.fromtimestamp(ts, UTC) to
    # derive the time key, so compute it the same way for the repo row.
    from datetime import UTC, datetime

    api_ts = 1767657600
    time_key = datetime.fromtimestamp(api_ts, tz=UTC).strftime("%Y-%m-%dT%H:00")

    fake_repo_data = {
        "data": [{"time": time_key, "row_count": 100, "bytes": 1000}],
        "granularity": "hour",
        "total_rows": 100,
        "total_bytes": 1000,
        "total_api_requests": 0,
    }
    fake_api_payload = {"data": [{"start_time": api_ts, "requests": 200}]}

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api", return_value=fake_api_payload),
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200
    body = resp.json()
    # The repo row got annotated with api_requests
    by_time = {p["time"]: p for p in body["data"]}
    assert by_time[time_key]["api_requests"] == 200
    assert body["total_api_requests"] == 200


def test_log_activity_swallows_fastly_enrichment_failure(client, tmp_path, monkeypatch):
    """When the Fastly API enrichment fails (network error, 5xx),
    the route still returns the repo data unchanged. Pinned because
    raising here would convert a partial-data success into a 500 —
    customers want SOME data over none."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    fake_repo_data = {
        "data": [{"time": "2026-01-05T00:00", "row_count": 100, "bytes": 1000}],
        "granularity": "hour",
        "total_rows": 100,
        "total_bytes": 1000,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("upstream down")),
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200


def test_log_activity_skips_fastly_enrichment_for_by_second(client, tmp_path, monkeypatch):
    """``by=second`` skips the API enrichment entirely (the Stats API
    only supports minute+ granularity). Pinned because making the call
    with by=second would 422 from Fastly and surface confusingly here."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service", "fastly_api_key": "tok"}
    )

    fake_repo_data = {
        "data": [],
        "granularity": "second",
        "total_rows": 0,
        "total_bytes": 0,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api") as mock_api,
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-02", "by": "second"},
        )

    assert resp.status_code == 200
    mock_api.assert_not_called()


def test_log_activity_skips_enrichment_when_no_api_key(client, tmp_path, monkeypatch):
    """Without an API key, no enrichment — and no 500. Pinned because
    a freshly-provisioned service without the API key saved should
    still render log-activity data from the repo."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    # No fastly_api_key
    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "name": "test_service"})

    fake_repo_data = {
        "data": [{"time": "2026-01-05T00:00", "row_count": 50, "bytes": 500}],
        "granularity": "hour",
        "total_rows": 50,
        "total_bytes": 500,
        "total_api_requests": 0,
    }

    app.dependency_overrides[get_source] = lambda: _complete_src()
    with (
        patch("backend.routers.usage.repo.get_log_activity", return_value=fake_repo_data),
        patch("backend.routers.usage._fastly_api") as mock_api,
    ):
        resp = client.get(
            "/api/usage/log-activity",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"start": "2026-01-01", "end": "2026-01-31", "by": "hour"},
        )

    assert resp.status_code == 200
    mock_api.assert_not_called()


# ── /api/usage/prefill: cached_status enrichment ───────────────────────────


def test_prefill_uses_cached_status_for_avg_file_size_and_data_days(client, tmp_path, monkeypatch):
    """When `cached_status` has `avg_log_size_kb` + earliest/latest
    timestamps, the prefill response surfaces them as
    `avg_log_file_size_kb` + `data_days`. Pinned because the cost
    estimator's daily-bytes formula multiplies avg_kb × requests/day
    × data_days — losing this enrichment would render zeros."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"})

    fake_status = {
        "avg_log_size_kb": 42.5,
        "earliest_log_at": "2026-05-01T00:00:00Z",
        "latest_log_at": "2026-05-10T00:00:00Z",
    }

    with patch("backend.config.get_status", return_value=fake_status):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["avg_log_file_size_kb"] == 42.5
    # 10 days inclusive (May 1 → May 10)
    assert body["data_days"] >= 9


def test_prefill_handles_cached_status_parse_failure_gracefully(client, tmp_path, monkeypatch):
    """If parsing earliest/latest timestamps raises, return whatever
    we have without crashing. Pinned because cached_status can have
    malformed timestamps from older versions — the prefill must not
    500 because of that."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"})

    fake_status = {"earliest_log_at": "garbage", "latest_log_at": "also garbage"}
    with patch("backend.config.get_status", return_value=fake_status):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Must not 500
    assert resp.status_code == 200


def test_prefill_propagates_provisioning_settings_into_response(client, tmp_path, monkeypatch):
    """`prov.sample_rate`, `prov.edge_only`, `prov.cron_sync.delete_after`,
    `prov.cron_compact.enabled`, `cfg.log_retention_days` all flow
    into the prefill response. Pinned because the cost estimator
    pre-populates its inputs from these exact field names — a
    rename would render the wizard's "save settings" inputs empty.

    Note: the prefill route calls ``svcconfig.load_config(source["name"])``
    so we patch load_config directly to avoid the source.name vs
    saved-key key mismatch (source.name = ``test_service`` per fixture,
    while the wizard saves keyed by service_id)."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
        "log_retention_days": 60,
        "log_period": 120,
        "provisioning": {
            "sample_rate": 50,
            "edge_only": False,
            "cron_sync": {"delete_after": False, "commit_interval_mins": 10},
            "cron_compact": {"enabled": False},
        },
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["sample_rate"] == 50
    assert body["edge_only"] is False
    assert body["compaction_enabled"] is False
    assert body["delete_after"] is False
    assert body["commit_interval_mins"] == 10
    assert body["log_retention_days"] == 60
    assert body["log_period_seconds"] == 120


def test_prefill_fetches_requests_per_day_from_fastly_stats_when_api_key_set(client, tmp_path, monkeypatch):
    """When `fastly_api_key` is configured AND a stats svc_id exists,
    prefill calls /stats/service to compute requests_per_day +
    edge_requests_per_day averages. Pinned because the cost estimator
    multiplies these by the per-class operation rate."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # 3 days of data, 1000 requests/day each (today filtered out as incomplete)
    fake_payload = {
        "data": [
            {"start_time": 1767225600, "requests": 1000, "edge_requests": 800},  # 2026-01-01
            {"start_time": 1767312000, "requests": 1500, "edge_requests": 1200},  # 2026-01-02
        ]
    }

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
    }

    def fake_complete_src(**overrides):
        return {**_complete_src(), "cdn_service_id": "cdn-svc-id", **overrides}

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: fake_complete_src()

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch("backend.routers.usage._fastly_api", return_value=fake_payload),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # average of 1000 + 1500 = 1250 (today is filtered out, leaving complete days)
    assert body["requests_per_day"] == 1250
    assert body["edge_requests_per_day"] == 1000  # (800 + 1200) / 2


def test_prefill_extracts_sample_rate_from_log_sampling_condition_vcl(client, tmp_path, monkeypatch):
    """When the active version has a "Log Sampling" response_condition,
    prefill parses `randombool(N, …)` from the VCL statement and
    surfaces N as `sample_rate`. Pinned because the FE renders the
    sample-rate slider position from this value — losing the parse
    would render 100% even when actually sampling at 25%."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    from backend.deps import get_source
    from backend.main import app

    # Both cdn_service_id (used for stats svc_id) and logging_service_id
    # required for the VCL-parsing branch to fire
    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-svc-id")

    fake_cfg = {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"}
    fake_endpoint = {
        "period": 60,
        "response_condition": "Log Sampling",
    }
    fake_cond = {"statement": "randombool(25, 100) && req.restarts == 0"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=42),
        patch("backend.core.fastly.client.fastly", return_value=fake_endpoint),
        patch("backend.core.fastly.service.find_condition", return_value=fake_cond),
        patch("backend.routers.usage._fastly_api", return_value={"data": []}),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    body = resp.json()
    assert body["sample_rate"] == 25
    assert body["edge_only"] is True  # req.restarts == 0 → edge-only
    # log_period_seconds comes from endpoint's "period"
    assert body["log_period_seconds"] == 60


def test_prefill_swallows_fastly_api_exception_returns_partial_response(client, tmp_path, monkeypatch):
    """If the Fastly Stats API call raises (network failure), prefill
    still returns the static config-derived fields rather than 500.
    Pinned because losing this would block dashboard from rendering
    on every transient Fastly hiccup."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: _complete_src(cdn_service_id="cdn-id")

    fake_cfg = {"service_id": MOCK_SERVICE_ID, "fos_bucket": "b", "name": "svc"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
        patch("backend.routers.usage._fastly_api", side_effect=RuntimeError("upstream down")),
    ):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    # Returns 200 with static defaults rather than 500
    assert resp.status_code == 200
    body = resp.json()
    # Static defaults still present
    assert "sample_rate" in body
    assert "class_a_rate_per_1k" in body


def test_prefill_computes_estimated_bytes_per_line_from_log_fields(client, tmp_path, monkeypatch):
    """`estimated_bytes_per_line` is computed from the saved
    log_fields config. Pinned because the cost estimator uses this
    × requests/day × bytes/$ as the "log storage" line item."""
    from backend import config

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": "svc",
        "log_fields": {"groups": ["A", "B"], "field_overrides": {}},
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    body = resp.json()
    assert body["estimated_bytes_per_line"] is not None
    # Non-empty config should produce a positive byte count
    assert body["estimated_bytes_per_line"] > 0


def test_prefill_calculates_rum_beacons_per_day(client, tmp_path, monkeypatch):
    """Verify that `/api/usage/prefill` calculates `rum_beacons_per_day`
    based on client_vitals and client_errors ingested files in SQLite."""
    from backend import config
    from backend.deps import get_source
    from backend.main import app

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path / "sys")
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "sys" / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    # Set up specific clean get_source override
    source_name = "test_rum_svc"
    app.dependency_overrides[get_source] = lambda: _complete_src(name=source_name)

    # Initialize metadata DB with ingested_files table
    from backend.core.metadata import get_con

    db = get_con(source_name)

    # Insert mock RUM file records
    db.execute(
        f"""
        INSERT INTO ingested_files (source_name, table_name, file_name, file_date, row_count, file_size_bytes, ingested_at)
        VALUES
        ('{source_name}', 'client_vitals', 'vit1.parquet', '2026-08-10', 1000, 50000, '2026-08-10 12:00:00'),
        ('{source_name}', 'client_errors', 'err1.parquet', '2026-08-10', 500, 25000, '2026-08-10 12:05:00'),
        ('{source_name}', 'client_vitals', 'vit2.parquet', '2026-08-11', 2000, 100000, '2026-08-11 12:00:00')
        """
    )
    db.commit()

    fake_cfg = {
        "service_id": MOCK_SERVICE_ID,
        "fos_bucket": "b",
        "name": source_name,
    }

    with patch("backend.config.load_config", return_value=fake_cfg):
        resp = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    # 2026-08-10 has 1000 + 500 = 1500 beacons
    # 2026-08-11 has 2000 beacons
    # Average should be (1500 + 2000) / 2 = 1750 beacons
    assert body["rum_beacons_per_day"] == 1750


# silence unused-imports
_ = json
_ = pytest
