"""Tier-5 White-Box Backend & Gateway Security Hardening Regression Suite (BUG-HS-01, BUG-HS-PY-01..05, BUG-GW-02)."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import secrets
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import backend.config as svcconfig
from backend.core import fos_client, request_context, share_db
from backend.core import metadata as metadata_db
from backend.core.metadata import pg_pool
from backend.models.common import Alert, CorrelateRequest, SavedView
from backend.routers import (
    alerts as alerts_mod,
)
from backend.routers import (
    bootstrap as bootstrap_mod,
)
from backend.routers import (
    control_room as control_room_mod,
)
from backend.routers import (
    query as query_mod,
)
from backend.routers import (
    security as security_mod,
)
from backend.routers.admin import high_scale as high_scale_mod
from backend.routers.rum import analytics as rum_analytics_mod
from backend.routers.session_scoring import scoring_analytics as scoring_mod
from backend.routers.share_admin import sessions_and_tos as sessions_mod
from backend.utils import remote_access, tunnel
from backend.utils.auth import ANALYST_SESSION_COOKIE

pytestmark = pytest.mark.no_postgres

REPO_ROOT = Path(__file__).resolve().parents[2]

_METADATA_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT,
        metric TEXT,
        evaluation_type TEXT,
        operator TEXT,
        threshold REAL,
        window_min INTEGER,
        comparison_period_min INTEGER,
        status_codes TEXT,
        webhook_url TEXT,
        enabled INTEGER DEFAULT 1,
        last_triggered_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        evaluation_scope TEXT DEFAULT 'all',
        channels_json TEXT DEFAULT '[]',
        zscore_threshold REAL,
        baseline_period_days INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS views (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        filters_json TEXT,
        time_range_type TEXT,
        start_time TEXT,
        end_time TEXT,
        page TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""",
]


@pytest.fixture(autouse=True)
def _isolated_tier5_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    monkeypatch.setattr(svcconfig, "DATA_DIR", data_dir)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "s" * 40)

    share_con = sqlite3.connect(str(tmp_path / "share_tier5.db"), check_same_thread=False)
    share_con.row_factory = sqlite3.Row
    share_con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        share_con.execute(ddl)
    share_con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: share_con)
    share_db.publish_tos_version("v1", "Tier 5 Terms", con=share_con)
    share_db._SETTINGS_CACHE.clear()
    share_db._TOS_CACHE.clear()

    meta_con = sqlite3.connect(str(tmp_path / "meta_tier5.db"), check_same_thread=False)
    meta_con.row_factory = sqlite3.Row
    for ddl in _METADATA_SCHEMA:
        meta_con.execute(ddl)
    meta_con.commit()
    monkeypatch.setattr(pg_pool, "get_con", lambda _sid=None: meta_con)
    monkeypatch.setattr("backend.core.metadata.alerts_db.get_con", lambda _sid=None: meta_con)
    monkeypatch.setattr("backend.core.metadata.admin_state.get_con", lambda _sid=None: meta_con)

    tunnel.reset_for_tests()
    rum_analytics_mod._live_events_cache.clear()
    request_context._key = None
    yield
    request_context._key = None
    rum_analytics_mod._live_events_cache.clear()
    share_db._SETTINGS_CACHE.clear()
    share_db._TOS_CACHE.clear()
    tunnel.reset_for_tests()
    meta_con.close()
    share_con.close()


def _create_analyst_session(
    monkeypatch: pytest.MonkeyPatch,
    *,
    service_ids: list[str],
    mask_ips: bool = True,
    active_service: str = "svc-a",
    configured_services: tuple[str, ...] = ("svc-a", "svc-b"),
    email: str = "analyst-t5@example.com",
    client_ip: str = "198.51.100.25",
    ua_str: str = "Mozilla/5.0 Chrome/131.0",
) -> tuple[object, dict[str, str]]:
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid: {"service_id": sid, "name": sid} if sid in configured_services else None,
    )
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: active_service)
    monkeypatch.setattr(svcconfig, "get_cdn_service_id_map", lambda: {})
    monkeypatch.setattr(
        svcconfig,
        "list_configs",
        lambda: [{"service_id": s, "name": s} for s in configured_services],
    )
    monkeypatch.setattr(
        fos_client,
        "get_source_for_service",
        lambda sid: (
            {"name": sid, "service_id": sid, "storage_mode": "high_scale"} if sid in configured_services else None
        ),
    )
    inv = share_db.create_remote_invite(
        email=email,
        name="Tier5 Analyst",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=service_ids,
        pii_policy={"mask_ips": mask_ips},
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(inv["id"], tos["version"])
    inv_row = share_db.get_remote_invite(inv["id"])
    assert inv_row is not None
    sess = mgr.create_session(invite=inv_row, ip_address=client_ip, user_agent=ua_str, headers={"user-agent": ua_str})
    headers = {
        "X-Proxied-By-Caddy": "true",
        "Origin": "https://share.example.com",
        "User-Agent": ua_str,
    }
    return sess, headers


def test_bug_hs_py_01_mask_ips_blocks_oip_origin_ip_plural_fields_and_query_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess, headers = _create_analyst_session(monkeypatch, service_ids=["svc-a"], mask_ips=True)
    app = FastAPI()
    app.add_middleware(remote_access.RemoteAccessMiddleware)

    called: list[str] = []

    @app.post("/api/dashboard/field-values")
    def _fv() -> dict[str, str]:
        called.append("field-values")
        return {"ok": "1"}

    @app.post("/api/dashboard/aggregates")
    def _agg() -> dict[str, str]:
        called.append("aggregates")
        return {"ok": "1"}

    @app.post("/api/dashboard/raw/csv")
    def _csv() -> dict[str, str]:
        called.append("csv")
        return {"ok": "1"}

    @app.get("/api/services/{service_id}/rum/analytics")
    def _rum(service_id: str) -> dict[str, str]:
        called.append(f"rum:{service_id}")
        return {"ok": "1"}

    @app.post("/api/origin/drilldown")
    def _origin_dd() -> dict[str, str]:
        called.append("origin-drilldown")
        return {"ok": "1"}

    with TestClient(app, client=("198.51.100.25", 50000), base_url="https://share.example.com") as client:
        client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)

        for key in (*sorted(share_db.SHARED_PII_KEYS), "origin_ip", "cmcd_session", "cmcd_sid", "cmcd_cid"):
            r_fv = client.post(
                "/api/dashboard/field-values",
                json={"service_id": "svc-a", "field": key, "search": "198.51.100.77"},
                headers=headers,
            )
            assert r_fv.status_code == 403, f"field={key} returned {r_fv.status_code}: {r_fv.text}"
            assert r_fv.json()["error"] == "pii_policy_violation"

            r_flt = client.post(
                "/api/dashboard/raw/csv",
                json={"service_id": "svc-a", "filters": {key: {"mode": "include", "values": ["198.51.100.77"]}}},
                headers=headers,
            )
            assert r_flt.status_code == 403, f"filter={key} returned {r_flt.status_code}: {r_flt.text}"
            assert r_flt.json()["error"] == "pii_policy_violation"

            r_plural = client.post(
                "/api/dashboard/aggregates",
                json={"service_id": "svc-a", "fields": ["status", key]},
                headers=headers,
            )
            assert r_plural.status_code == 403, (
                f"fields=[status,{key}] returned {r_plural.status_code}: {r_plural.text}"
            )
            assert r_plural.json()["error"] == "pii_policy_violation"

            qf = json.dumps({key: {"mode": "include", "values": ["198.51.100.77"]}})
            r_get_qf = client.get(f"/api/services/svc-a/rum/analytics?filters={qf}", headers=headers)
            assert r_get_qf.status_code == 403, f"GET filters={key} returned {r_get_qf.status_code}: {r_get_qf.text}"
            assert r_get_qf.json()["error"] == "pii_policy_violation"

        assert called == []

        # dimension="oip" remains allowed for /24-bucket-merged aggregate drilldown, whereas dimension="origin_ip" / "ip" is 403
        r_dim_oip = client.post(
            "/api/origin/drilldown", json={"service_id": "svc-a", "dimension": "oip"}, headers=headers
        )
        assert r_dim_oip.status_code == 200
        r_dim_ip = client.post(
            "/api/origin/drilldown", json={"service_id": "svc-a", "dimension": "ip"}, headers=headers
        )
        assert r_dim_ip.status_code == 403

    # control_room_correlate defense-in-depth check blocks client IP / session dimensions (excluding /24-merged "oip")
    fake_req = SimpleNamespace(state=SimpleNamespace(analyst_session=sess))
    for bad_dim in ("ip", "client_ip", "c_ip", "origin_ip", "cookie_session", "cmcd_session"):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                control_room_mod.control_room_correlate(
                    body=CorrelateRequest(dimension=bad_dim, window_minutes=5, limit=5),
                    service_id="svc-a",
                    request=fake_req,
                    _access="svc-a",
                    registry=SimpleNamespace(resolve=lambda _sid: None),
                )
            )
        assert exc_info.value.status_code == 403
        assert exc_info.value.detail["error"] == "pii_policy_violation"


def test_bug_hs_py_02_cmcd_session_aliases_sessions_detail_rum_live_and_schema_pii_stripping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.high_scale.archive import ServingWatermark
    from backend.high_scale.ingest_controller import get_high_scale_service_registry
    from backend.high_scale.postgres_control import console_access

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    assert len(share_db.SHARED_PII_KEYS) == 18
    assert len(share_db.SAVED_VIEW_PII_FILTER_KEYS) == 18
    assert len(console_access.MASKED_KEYS) == 18
    assert share_db.CMCD_SESSION_ALIAS_KEYS == frozenset({"cmcd_session", "cmcd_sid", "cmcd_cid"})

    masked_cmcd = share_db.apply_pii_policy(
        {"cmcd_session": "s-raw-1", "cmcd_sid": "s-raw-2", "cmcd_cid": "c-raw-3"},
        {"mask_ips": True},
    )
    assert masked_cmcd == {"cmcd_session": "[redacted]", "cmcd_sid": "[redacted]", "cmcd_cid": "[redacted]"}
    assert share_db.saved_view_contains_pii({"filters": {"cmcd_sid": ["s-raw-2"]}}) is True

    sess, headers = _create_analyst_session(
        monkeypatch,
        service_ids=["svc-a"],
        mask_ips=True,
        email="pii02@example.com",
    )

    wm = ServingWatermark(
        "svc-a",
        "cmcd",
        1,
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
        "c-1",
        "e-1",
        "e-1",
        True,
    )
    fake_hs = SimpleNamespace(
        service_id="svc-a",
        client=SimpleNamespace(),
        cursor_secret=b"0123456789abcdef0123456789abcdef",
        watermark=lambda: wm,
        watermark_for=lambda _dom: wm,
    )
    fake_reg = SimpleNamespace(resolve=lambda sid: fake_hs if sid == "svc-a" else None)

    monkeypatch.setattr(
        "backend.high_scale.sessions.sessions_endpoint",
        lambda _hs, _req, _s, _e: {
            "sessions": [
                {
                    "ip": "203.0.113.45",
                    "ja4": "t13d1516h2_8daaf6152771_b0da82dd1658",
                    "session_start": "2026-06-01T00:00:00Z",
                    "session_end": "2026-06-01T00:10:00Z",
                    "req_count": 3,
                    "edge_sid": "esid-raw-999",
                    "flagged": False,
                }
            ],
            "total": 1,
            "page": 1,
            "limit": 50,
            "has_rtt": False,
            "has_ja4": True,
            "has_edge": False,
        },
    )
    monkeypatch.setattr(
        "backend.high_scale.sessions.sessions_detail",
        lambda _hs, _req, _s, _e, _ip, _ja4: {
            "columns": [
                "timestamp",
                "ip",
                "client_ip",
                "oip",
                "origin_ip",
                "url",
                "ua",
                "edge_sid",
                "cmcd_sid",
                "cmcd_cid",
            ],
            "data": [
                {
                    "timestamp": "2026-06-01T00:01:00Z",
                    "ip": "203.0.113.45",
                    "client_ip": "203.0.113.45",
                    "oip": "198.51.100.7",
                    "origin_ip": "198.51.100.7",
                    "url": "/checkout?client_ip=203.0.113.45&v6=2001:db8::dead:beef",
                    "ua": "Mozilla/5.0 Chrome/131.0.6778.85 (XFF 203.0.113.45)",
                    "edge_sid": "esid-raw-999",
                    "cmcd_sid": "cmcd-sid-RAW-777",
                    "cmcd_cid": "cmcd-cid-RAW-888",
                }
            ],
        },
    )
    monkeypatch.setattr(
        "backend.high_scale.rum.rum_live_events",
        lambda *_a, **_kw: [
            {
                "time": "2026-06-01T00:02:00Z",
                "type": "pageview",
                "path": "/users/203.0.113.45/settings",
                "desc": "Loaded from 203.0.113.45",
                "browser": "Chrome",
                "os": "macOS",
                "raw_log": {"meta": {"page": {"url": "https://shop.example.com/users/203.0.113.45/settings"}}},
            }
        ],
    )
    monkeypatch.setattr(
        svcconfig,
        "get_status",
        lambda sid: (
            {
                "schema": [
                    *[
                        {"name": k, "type": "VARCHAR"}
                        for k in (*sorted(share_db.SHARED_PII_KEYS), "origin_ip", "cmcd_sid")
                    ],
                    {"name": "url", "type": "VARCHAR"},
                    {"column": "status", "type": "INTEGER"},
                ]
            }
            if sid == "svc-a"
            else {}
        ),
    )

    app = FastAPI()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: fake_reg
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: fake_reg,
    )
    app.add_middleware(remote_access.RemoteAccessMiddleware)
    app.include_router(high_scale_mod.router)
    app.include_router(scoring_mod.sessions_router)
    app.include_router(rum_analytics_mod.rum_analytics_router, prefix="/api/services")
    app.include_router(query_mod.router)
    app.include_router(bootstrap_mod.bootstrap_router)

    # Warm rum_live_events cache as Admin first to verify cache does not leak unmasked events to Analyst
    with TestClient(app, client=("127.0.0.1", 50000), base_url="http://localhost") as admin_client:
        r_admin_rum = admin_client.get(
            "/api/services/svc-a/rum/live-events?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z"
        )
        assert r_admin_rum.status_code == 200
        assert "203.0.113.45" in r_admin_rum.text

    with TestClient(app, client=("198.51.100.25", 50000), base_url="https://share.example.com") as client:
        client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)

        # 1. High-Scale CMCD default dimension ("cmcd_session") & explicit cmcd_session -> 403
        r_cmcd_default = client.post(
            "/api/high-scale/services/svc-a/aggregates",
            json={"domain": "cmcd", "start_time": "2026-06-01T00:00:00Z", "end_time": "2026-06-01T01:00:00Z"},
            headers=headers,
        )
        assert r_cmcd_default.status_code == 403, r_cmcd_default.text
        assert "pii_policy_violation" in r_cmcd_default.text

        r_cmcd_explicit = client.post(
            "/api/high-scale/services/svc-a/aggregates",
            json={
                "domain": "request",
                "dimension": "cmcd_session",
                "start_time": "2026-06-01T00:00:00Z",
                "end_time": "2026-06-01T01:00:00Z",
            },
            headers=headers,
        )
        assert r_cmcd_explicit.status_code == 403

        # 2. Sessions list -> detail with valid session_token returns 200 and scrubs ALL PII + embedded IPs
        r_list = client.post(
            "/api/sessions",
            json={"service_id": "svc-a", "start_time": "2026-06-01T00:00:00Z", "end_time": "2026-06-01T00:30:00Z"},
            headers=headers,
        )
        assert r_list.status_code == 200, r_list.text
        tok = r_list.json()["sessions"][0]["session_token"]

        r_detail = client.post(
            "/api/sessions/detail",
            json={"service_id": "svc-a", "session_token": tok},
            headers=headers,
        )
        assert r_detail.status_code == 200, r_detail.text
        detail_text = r_detail.text
        assert "203.0.113.45" not in detail_text
        assert "198.51.100.7" not in detail_text
        assert "2001:db8::dead:beef" not in detail_text
        assert "cmcd-sid-RAW-777" not in detail_text
        assert "cmcd-cid-RAW-888" not in detail_text
        assert "esid-raw-999" not in detail_text
        row0 = r_detail.json()["data"][0]
        assert row0["cmcd_sid"] == "[redacted]"
        assert row0["cmcd_cid"] == "[redacted]"
        assert row0["edge_sid"] == "[redacted]"
        assert "Chrome/131.0.6778.85" in row0["ua"]

        # 3. RUM live-events scrubs embedded IPs even when _live_events_cache was warmed by Admin
        r_rum = client.get(
            "/api/services/svc-a/rum/live-events?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z",
            headers=headers,
        )
        assert r_rum.status_code == 200, r_rum.text
        assert "203.0.113.45" not in r_rum.text
        assert "203.0.113.xxx" in r_rum.text

        # 4. Query schema & /api/schema strip all 18 SHARED_PII_KEYS + origin_ip + cmcd_sid
        r_qschema = client.get("/api/query/schema/svc-a", headers=headers)
        assert r_qschema.status_code == 200, r_qschema.text
        q_cols = {c.get("name") or c.get("column") for c in r_qschema.json()["schema"]}
        assert q_cols == {"url", "status"}

        r_bschema = client.get("/api/schema?service=svc-a", headers=headers)
        assert r_bschema.status_code == 200, r_bschema.text
        b_cols = {c.get("name") or c.get("column") for c in r_bschema.json()["schema"]}
        assert b_cols == {"url", "status"}

    # Admin still sees unmasked cached RUM event
    with TestClient(app, client=("127.0.0.1", 50000), base_url="http://localhost") as admin_client:
        r_admin_rum2 = admin_client.get(
            "/api/services/svc-a/rum/live-events?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z"
        )
        assert "203.0.113.45" in r_admin_rum2.text


def test_bug_hs_py_03_included_router_path_service_ids_active_cookie_and_no_admin_sid_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.high_scale.archive import ServingWatermark
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    # Admin active service is svc-a, while Analyst is ONLY authorized for svc-b
    sess, headers = _create_analyst_session(
        monkeypatch,
        service_ids=["svc-b"],
        mask_ips=True,
        active_service="svc-a",
        configured_services=("svc-a", "svc-b"),
        email="tenant-b@example.com",
    )

    wm = ServingWatermark(
        "svc-b",
        "request",
        1,
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
        "c-1",
        "e-1",
        "e-1",
        True,
    )
    fake_hs = SimpleNamespace(
        service_id="svc-b",
        client=SimpleNamespace(),
        cursor_secret=b"0123456789abcdef0123456789abcdef",
        watermark=lambda: wm,
        watermark_for=lambda _dom: wm,
    )
    fake_reg = SimpleNamespace(resolve=lambda sid: fake_hs if sid == "svc-b" else None)
    monkeypatch.setattr(
        high_scale_mod,
        "query_request_facts",
        lambda *_a, **_kw: SimpleNamespace(
            rows=[],
            next_cursor=None,
            metadata=SimpleNamespace(
                status="complete",
                exact=True,
                coverage=1.0,
                freshness_lag_seconds=0.0,
                watermark=wm,
                approximation_error=None,
                error=None,
            ),
        ),
    )
    monkeypatch.setattr(svcconfig, "get_status", lambda _sid: {"schema": [{"name": "url", "type": "VARCHAR"}]})

    app = FastAPI()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: fake_reg
    monkeypatch.setattr("backend.high_scale.ingest_controller.get_high_scale_service_registry", lambda: fake_reg)
    app.add_middleware(remote_access.RemoteAccessMiddleware)
    app.include_router(high_scale_mod.router)
    app.include_router(query_mod.router)
    app.include_router(bootstrap_mod.bootstrap_router)

    with TestClient(app, client=("198.51.100.25", 50000), base_url="https://share.example.com") as client:
        client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)

        # 1. Path service_id inside _IncludedRouter (/api/query/schema/svc-b and /api/high-scale/services/svc-b/...)
        r_qs = client.get("/api/query/schema/svc-b", headers=headers)
        assert r_qs.status_code == 200, r_qs.text
        assert r_qs.json()["source"] == "svc-b"

        r_hs = client.post(
            "/api/high-scale/services/svc-b/request-facts",
            json={"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-06-01T01:00:00Z"},
            headers=headers,
        )
        assert r_hs.status_code == 200, r_hs.text

        # 2. Omitted param/cookie fails closed with 403 AND never leaks Admin's active service ("svc-a")!
        r_no_cookie = client.get("/api/presets", headers=headers)
        assert r_no_cookie.status_code == 403
        assert r_no_cookie.json()["error"] == "service_not_authorized"
        assert "svc-a" not in r_no_cookie.text
        assert r_no_cookie.json().get("service") == ""

        # 3. With Cookie: fla.activeServiceId=svc-b, /api/schema and /api/presets succeed scoped to svc-b!
        client.cookies.set("fla.activeServiceId", "svc-b")
        r_presets = client.get("/api/presets", headers=headers)
        assert r_presets.status_code == 200, r_presets.text
        assert any("svc_b" in p["sql"] for p in r_presets.json())

        r_schema = client.get("/api/schema", headers=headers)
        assert r_schema.status_code == 200, r_schema.text
        assert r_schema.json()["table_name"] == "logs_svc_b"

        # 4. Unauthorized Cookie: fla.activeServiceId=svc-a is rejected with 403
        client.cookies.set("fla.activeServiceId", "svc-a")
        r_unauth_cookie = client.get("/api/presets", headers=headers)
        assert r_unauth_cookie.status_code == 403
        assert r_unauth_cookie.json()["error"] == "service_not_authorized"

    # 5. Direct get_service_id fallback for Analyst session falls back to session.service_ids[0] instead of Admin active_sid
    fake_req = SimpleNamespace(
        path_params={},
        query_params={},
        headers={},
        cookies={},
        state=SimpleNamespace(analyst_session=sess),
    )
    assert (
        request_context.get_service_id(fake_req, service=None, sid=None, x_fastly_service_id=None, x_service_id=None)
        == "svc-b"
    )


def test_bug_hs_py_04_proxies_export_validates_params_and_quotes_content_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import h11

    monkeypatch.setattr(
        security_mod,
        "hs_proxies",
        lambda *_a, **_kw: {
            "active_clients": [
                {"ip": "203.0.113.99", "risk_level": "High", "impossible_distance": True},
                {"ip": "198.51.100.88", "risk_level": "Medium", "impossible_distance": False},
                {"ip": "192.0.2.77", "risk_level": "Low", "impossible_distance": False},
            ]
        },
    )
    fake_ctx = SimpleNamespace(
        service_id="svc-a",
        analyst_session=None,
        clamp=lambda s, e: (s, e),
        high_scale=SimpleNamespace(service_id="svc-a"),
    )
    monkeypatch.setattr(security_mod, "require_high_scale", lambda ctx: ctx.high_scale)

    app = FastAPI()
    app.dependency_overrides[request_context.build_request_context] = lambda: fake_ctx
    app.include_router(security_mod.router)

    with TestClient(app) as client:
        for bad_q in (
            'threshold=high;%20filename="evil.html"&format=fastly-acl',
            "threshold=high%0d%0aX-Injected:%201&format=fastly-acl",
            "threshold=unknown&format=fastly-acl",
            'threshold=High&format=evil;%20filename="x.sh"',
            "threshold=High&format=fastly-acl%0d%0aSet-Cookie:%20a=1",
        ):
            r_bad = client.get(
                f"/api/security/proxies/export?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&{bad_q}"
            )
            assert r_bad.status_code == 400, f"{bad_q} -> {r_bad.status_code} {r_bad.text}"

        r_ok = client.get(
            "/api/security/proxies/export?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&threshold=High&format=fastly-acl"
        )
        assert r_ok.status_code == 200
        disp = r_ok.headers["content-disposition"]
        assert disp == 'attachment; filename="watchdog_blocklist_high_fastly-acl.csv"'
        h11.Response(status_code=200, headers=list(r_ok.headers.raw))

        r_txt = client.get(
            "/api/security/proxies/export?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&threshold=medium&format=ip-list"
        )
        assert r_txt.status_code == 200
        assert r_txt.headers["content-disposition"] == 'attachment; filename="blocklist_medium.txt"'
        assert "203.0.113.99" in r_txt.text and "198.51.100.88" in r_txt.text
        assert "192.0.2.77" not in r_txt.text

        r_low_csv = client.get(
            "/api/security/proxies/export?start_time=2026-06-01T00:00:00Z&end_time=2026-06-01T01:00:00Z&threshold=Low&format=csv"
        )
        assert r_low_csv.status_code == 200
        assert r_low_csv.headers["content-disposition"] == 'attachment; filename="watchdog_blocklist_low_csv.csv"'
        assert "192.0.2.77" in r_low_csv.text


def _encrypt_backup_payload(payload: dict, passphrase: str) -> bytes:
    salt = secrets.token_bytes(16)
    nonce = secrets.token_bytes(12)
    key = hashlib.scrypt(passphrase.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    ct = AESGCM(key).encrypt(nonce, json.dumps(payload).encode("utf-8"), None)
    return b"FOSBACKUP\x01" + salt + nonce + ct


def test_bug_hs_py_05_prevent_cross_service_pk_hijack_and_harden_backup_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(
        svcconfig, "load_config", lambda sid: {"service_id": sid} if sid in ("svc-a", "svc-b") else None
    )
    monkeypatch.setattr(alerts_mod, "sync_admin_state", lambda *_a, **_kw: None)

    # 1. Alert & SavedView cross-service PK hijack blocked with 409
    alert_a = Alert(
        id="shared-alert-001",
        service_id="svc-a",
        name="Original Alert A",
        metric="5xx",
        operator=">",
        threshold=10.0,
        window_min=5,
    )
    metadata_db.save_alert("svc-a", alert_a)
    # Same-service update succeeds
    metadata_db.save_alert("svc-a", alert_a.model_copy(update={"name": "Updated Alert A"}))

    with pytest.raises(HTTPException) as exc_alert:
        metadata_db.save_alert("svc-b", alert_a.model_copy(update={"service_id": "svc-b", "name": "Hijacked Alert"}))
    assert exc_alert.value.status_code == 409
    assert [a["name"] for a in metadata_db.list_alerts("svc-a", filter_service_id="svc-a")] == ["Updated Alert A"]
    assert metadata_db.list_alerts("svc-b", filter_service_id="svc-b") == []

    view_a = SavedView(
        id="shared-view-001",
        service_id="svc-a",
        name="Original View A",
        filters_json='[{"field": "status", "value": "500"}]',
    )
    metadata_db.save_view("svc-a", view_a)
    metadata_db.save_view("svc-a", view_a.model_copy(update={"name": "Updated View A"}))

    with pytest.raises(HTTPException) as exc_view:
        metadata_db.save_view("svc-b", view_a.model_copy(update={"service_id": "svc-b", "name": "Hijacked View"}))
    assert exc_view.value.status_code == 409
    assert [v["name"] for v in metadata_db.list_views("svc-a")] == ["Updated View A"]
    assert metadata_db.list_views("svc-b") == []

    # 2. Revoked-invite backup re-import succeeds cleanly without 500 IntegrityError
    passphrase = "very-long-backup-passphrase"
    inv = share_db.create_remote_invite(
        name="Revoked User",
        email="revoked@corp.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist="198.51.100.0/24",
        service_ids=["svc-a"],
        pii_policy={"mask_ips": True},
    )
    blob = share_db.export_backup(passphrase)
    share_db.revoke_remote_invite(inv["id"])
    res_skip = share_db.import_backup(blob, passphrase, mode="skip-collisions")
    assert res_skip == {"inserted": 0, "skipped": 1, "merged": 0}

    # 3. Invalid ip_whitelist or invalid pii_policy in backup raises ValueError
    bad_ip_blob = _encrypt_backup_payload(
        {
            "schema_version": share_db.LATEST_VERSION,
            "invites": [
                {
                    "id": "inv-bad-ip",
                    "name": "Bad IP",
                    "email": "badip@corp.com",
                    "passcode": inv["passcode"],
                    "ip_whitelist": "NOT_A_VALID_IP_OR_CIDR",
                    "pii_policy": '{"mask_ips": true}',
                }
            ],
            "invite_services": [],
            "share_settings": [],
        },
        passphrase,
    )
    with pytest.raises(ValueError):
        share_db.import_backup(bad_ip_blob, passphrase)

    bad_pii_blob = _encrypt_backup_payload(
        {
            "schema_version": share_db.LATEST_VERSION,
            "invites": [
                {
                    "id": "inv-bad-pii",
                    "name": "Bad PII",
                    "email": "badpii@corp.com",
                    "passcode": inv["passcode"],
                    "pii_policy": '{"mask_ips": "not-a-bool", "rogue_flag": "evil"}',
                }
            ],
            "invite_services": [],
            "share_settings": [],
        },
        passphrase,
    )
    with pytest.raises(ValueError):
        share_db.import_backup(bad_pii_blob, passphrase)

    # 4. share_settings allowlist & _SETTINGS_CACHE invalidation
    share_db.set_setting("sharing_enabled", "1")
    assert share_db.get_setting("sharing_enabled") == "1"

    settings_blob = _encrypt_backup_payload(
        {
            "schema_version": share_db.LATEST_VERSION,
            "invites": [],
            "invite_services": [],
            "share_settings": [
                {"key": "sharing_enabled", "value": "0"},
                {"key": "schema_version", "value": "999"},
                {"key": "rogue_setting_key", "value": "injected_val"},
            ],
        },
        passphrase,
    )
    share_db.import_backup(settings_blob, passphrase)
    assert share_db.get_setting("sharing_enabled") == "0"
    assert share_db.get_setting("rogue_setting_key") is None
    assert share_db.get_setting("schema_version") is None

    # 5. Router upload size cap (MAX_BACKUP_UPLOAD_BYTES = 10 MiB) -> 413
    app = FastAPI()
    app.include_router(sessions_mod.sessions_admin_router, prefix="/api/admin/share")
    with TestClient(app) as client:
        oversized = io.BytesIO(b"x" * (10 * 1024 * 1024 + 64))
        r_big = client.post(
            "/api/admin/share/backup/import",
            data={"passphrase": passphrase, "mode": "skip-collisions"},
            files={"file": ("backup.enc", oversized, "application/octet-stream")},
        )
        assert r_big.status_code == 413


def test_bug_gw_02_and_admin_gateway_caddy_header_hygiene() -> None:
    caddy_root = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    pub_gw = (REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics" / "templates" / "public-gateway.yaml").read_text(
        encoding="utf-8"
    )
    admin_gw = (REPO_ROOT / "deploy" / "chart" / "admin-gateway" / "files" / "Caddyfile").read_text(encoding="utf-8")

    for label, text in (("Caddyfile", caddy_root), ("public-gateway.yaml", pub_gw)):
        m_pub = re.search(r"header_regexp\s+fcip_pub\s+Fastly-Client-IP\s+(\S+)", text)
        m_priv = re.search(r"header_regexp\s+fcip_priv\s+Fastly-Client-IP\s+(\S+)", text)
        assert m_pub is not None and m_priv is not None, label
        pub_re = re.compile(m_pub.group(1))
        priv_pat = m_priv.group(1)
        flags = re.IGNORECASE if priv_pat.startswith("^(?i)") else 0
        priv_re = re.compile(priv_pat.replace("^(?i)", "^", 1) if flags else priv_pat, flags)

        for non_public in (
            "127.0.0.1",
            "10.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "0:0:0:0:0:ffff:7f00:1",
            "0000:0000:0000:0000:0000:ffff:0a00:0001",
            "::0:ffff:7f00:1",
            "ff02::1",
            "2002:7f00:1::1",
        ):
            if pub_re.search(non_public):
                assert priv_re.search(non_public) is not None, f"{label} failed to catch non-public {non_public}"

        for public_ip in ("203.0.113.45", "2001:4860:4860::8888", "2606:4700:4700::1111"):
            assert pub_re.search(public_ip) is not None, f"{label} rejected public {public_ip}"
            assert priv_re.search(public_ip) is None, f"{label} falsely marked {public_ip} non-public"

    # Admin gateway strips public/analyst spoof headers BEFORE injecting X-Admin-Gateway-Token and redacts log secrets
    inject_idx = admin_gw.index("request_header X-Admin-Gateway-Token")
    for stripped in (
        "request_header -X-Remote-Analyst",
        "request_header -X-Proxied-By-Caddy",
        "request_header -X-FLA-Public-Gateway",
        "request_header -X-FLA-Frontend-Proxy",
    ):
        assert stripped in admin_gw and admin_gw.index(stripped) < inject_idx, stripped
    assert "request>headers>X-Admin-Gateway-Token delete" in admin_gw
    assert "request>headers>X-FLA-Admin-Gateway delete" in admin_gw
