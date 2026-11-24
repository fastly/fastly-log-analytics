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
        called.clear()

        # SEC-T5-ITER1-01: RFC 6838 +json media types, sniffed JSON on text/plain, malformed/non-dict JSON, and duplicate params
        for plus_ct in (
            "application/problem+json",
            "application/vnd.api+json; charset=utf-8",
            "application/ld+json",
            "text/plain",
        ):
            r_ct_fv = client.post(
                "/api/dashboard/field-values",
                content=json.dumps({"service_id": "svc-a", "field": "oip", "search": "198.51"}).encode("utf-8"),
                headers={**headers, "Content-Type": plus_ct},
            )
            assert r_ct_fv.status_code == 403, f"ct={plus_ct} field=oip -> {r_ct_fv.status_code}: {r_ct_fv.text}"
            assert r_ct_fv.json()["error"] == "pii_policy_violation"

            r_ct_agg = client.post(
                "/api/dashboard/aggregates",
                content=json.dumps({"service_id": "svc-a", "fields": ["cookie_session", "ip"]}).encode("utf-8"),
                headers={**headers, "Content-Type": plus_ct},
            )
            assert r_ct_agg.status_code == 403, f"ct={plus_ct} fields -> {r_ct_agg.status_code}: {r_ct_agg.text}"
            assert r_ct_agg.json()["error"] == "pii_policy_violation"

            r_ct_sid = client.post(
                "/api/dashboard/aggregates?service_id=svc-a",
                content=json.dumps({"service_id": "svc-b", "fields": ["status"]}).encode("utf-8"),
                headers={**headers, "Content-Type": plus_ct},
            )
            assert r_ct_sid.status_code == 403, f"ct={plus_ct} sid -> {r_ct_sid.status_code}: {r_ct_sid.text}"
            assert r_ct_sid.json()["error"] == "service_not_authorized"

        for bad_body in (b"{bad-json", b'["service_id", "ip"]', b'"scalar"'):
            r_bad_json = client.post(
                "/api/dashboard/field-values?service_id=svc-a",
                content=bad_body,
                headers={**headers, "Content-Type": "application/problem+json"},
            )
            assert r_bad_json.status_code == 400, f"bad_body={bad_body!r} -> {r_bad_json.status_code}"
            assert r_bad_json.json()["error"] == "invalid_json_body"

        qf_ok = json.dumps({"url": {"mode": "include", "values": ["/ok"]}})
        qf_pii = json.dumps({"ip": {"mode": "include", "values": ["1.2.3.4"]}})
        r_dup_flt = client.get(
            f"/api/services/svc-a/rum/analytics?filters={qf_ok}&filters={qf_pii}",
            headers=headers,
        )
        assert r_dup_flt.status_code == 403 and r_dup_flt.json()["error"] == "pii_policy_violation"

        r_dup_sid = client.post(
            "/api/origin/drilldown?service_id=svc-a&service_id=svc-b",
            json={"dimension": "oip"},
            headers=headers,
        )
        assert r_dup_sid.status_code == 403 and r_dup_sid.json()["error"] == "service_not_authorized"

        r_dup_svc = client.post(
            "/api/origin/drilldown?service=svc-a&service=svc-b",
            json={"dimension": "oip"},
            headers=headers,
        )
        assert r_dup_svc.status_code == 403 and r_dup_svc.json()["error"] == "service_not_authorized"

        assert called == []

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


def test_recursive_cte_anchor_cannot_shadow_disallowed_base_table() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    for attack_sql in (
        "WITH RECURSIVE ops_secret AS (SELECT * FROM ops_secret UNION ALL SELECT * FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT * FROM ops_secret) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS ((SELECT * FROM ops_secret UNION ALL SELECT 1) UNION ALL SELECT * FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 1 INTERSECT SELECT * FROM ops_secret) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION ALL SELECT secret_col FROM ops_secret UNION ALL SELECT secret_col FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION ALL SELECT secret_col FROM ops_secret EXCEPT SELECT 'b' AS secret_col) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION ALL SELECT secret_col FROM ops_secret INTERSECT SELECT 'a' AS secret_col) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION ALL (SELECT secret_col FROM ops_secret UNION ALL SELECT 'b' AS secret_col WHERE 1=0)) SELECT * FROM ops_secret",
    ):
        with pytest.raises(SQLValidationError) as exc_info:
            validate_user_sql(attack_sql, allowed_tables={"logs"})
        assert exc_info.value.reason == "catalog_blocklist:foreign_table"

    for legit_sql in (
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r",
        "WITH RECURSIVE r AS (SELECT status AS n FROM logs UNION ALL SELECT n + 1 FROM r WHERE n < 205) SELECT * FROM r",
        "WITH RECURSIVE r AS (SELECT status AS n FROM logs UNION ALL SELECT status + 10 AS n FROM logs UNION ALL SELECT n + 1 FROM r WHERE n < 220) SELECT * FROM r",
    ):
        res = validate_user_sql(legit_sql, allowed_tables={"logs"})
        assert res.parse_tree.get("statements")


def test_sec_t5_iter5_py01_py02_py03_subquery_table_from_only_pivot_and_lexer_hardening() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. SEC-T5-ITER5-PY-01: Subqueries after clause/operator keywords & ARRAY(SELECT ...)
    for subq_attack in (
        "SELECT (SELECT name FROM system.tables LIMIT 1) AS leaked FROM logs LIMIT 1",
        "SELECT status FROM logs WHERE EXISTS (SELECT 1 FROM system.tables)",
        "SELECT status FROM logs WHERE url IN (SELECT service_id FROM high_scale_batch_publications)",
        "SELECT CASE WHEN (SELECT count() FROM information_schema.tables) > 0 THEN 1 ELSE 0 END AS leaked FROM logs LIMIT 1",
        "SELECT ARRAY(SELECT service_id FROM high_scale_batch_publications) AS leaked FROM logs LIMIT 1",
        "WITH RECURSIVE ops_secret AS (SELECT (SELECT secret_col FROM ops_secret LIMIT 1) AS secret_col UNION ALL SELECT secret_col FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret",
        "SELECT (SELECT * FROM which_secret('s3://x', 'a') LIMIT 1) AS leaked FROM logs",
        "SELECT (SELECT * FROM my_secret_tvf() LIMIT 1) AS leaked FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(subq_attack, allowed_tables={"logs"})

    # 2. SEC-T5-ITER5-PY-02 & Reviewer Finding 3: TABLE <rel>, (FROM <rel> ...), FROM ONLY, parenthesized JOIN, PIVOT/UNPIVOT, subquery SHOW/DESCRIBE/SUMMARIZE/DESC
    for table_or_stmt_attack in (
        "WITH c AS (TABLE high_scale_batch_publications) SELECT * FROM c",
        "SELECT (FROM high_scale_batch_publications SELECT service_id LIMIT 1) AS leaked FROM logs LIMIT 1",
        "VALUES ((FROM ops_secret SELECT secret_col LIMIT 1))",
        'WITH "only" AS (SELECT 1 AS x) SELECT secret_col FROM ONLY ops_secret',
        'WITH "only" AS (SELECT 1 AS x) SELECT b.secret_col FROM logs AS a CROSS JOIN ONLY ops_secret AS b',
        "SELECT a.secret_col FROM (ops_secret AS a CROSS JOIN logs AS b)",
        "SELECT secret_col FROM (TABLE ops_secret)",
        "SELECT url FROM logs UNION ALL TABLE ops_secret",
        "TABLE ops_secret",
        "FROM ops_secret SELECT secret_col",
        "SELECT * FROM (DESCRIBE ops_secret) sub",
        "SELECT * FROM (SUMMARIZE ops_secret) sub",
        "SELECT * FROM (SHOW TABLES) sub",
        "SELECT status FROM logs WHERE EXISTS (DESC ops_secret)",
        "WITH p AS (PIVOT ops_secret ON secret_col) SELECT * FROM p",
        "WITH p AS (UNPIVOT ops_secret ON secret_col) SELECT * FROM p",
        "WITH p AS (PIVOT_WIDER ops_secret ON secret_col) SELECT * FROM p",
        "WITH p AS (PIVOT_LONGER ops_secret ON secret_col) SELECT * FROM p",
        "SELECT * FROM ONLY WHERE 1=1",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(table_or_stmt_attack, allowed_tables={"logs"})

    # 3. SEC-T5-ITER5-PY-03: WITH RECURSIVE ... UNION [ALL] BY NAME & lexer CR-comment / $tag$ / E'...' / U&'...' / Unicode-ws / ctrl chars
    for lexer_or_by_name_attack in (
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION BY NAME SELECT secret_col FROM ops_secret) SELECT * FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 'a' AS secret_col UNION ALL BY NAME SELECT secret_col FROM ops_secret) SELECT * FROM ops_secret",
        "SELECT status FROM logs -- comment\rUNION ALL SELECT secret_col FROM ops_secret",
        "SELECT $a$'$a$ AS a, secret_col FROM ops_secret WHERE $a$'$a$ != ''",
        r"SELECT E'\'' AS a, secret_col FROM ops_secret WHERE E'\'' != ''",
        "SELECT E'a'\n'\\'' AS a, secret_col FROM ops_secret WHERE 'x' != E'\\''",
        r"SELECT U&'\0027' FROM logs",
        'SELECT U&"ops_secret" FROM logs',
        "SELECT 'a' UESCAPE '#' FROM logs",
        "SELECT status\x0bFROM logs",
        "SELECT status\u2028FROM logs",
        "SELECT * FROM\u200bops_secret",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(lexer_or_by_name_attack, allowed_tables={"logs"})

    # 4. Benign controls on validate_user_sql (TRIM(FROM ...), EXTRACT, SUBSTRING, quoted "only" CTE, ORDER BY DESC, replace/truncate scalar calls, Unicode spaces on allowed table)
    for benign_sql in (
        "SELECT TRIM(FROM url), TRIM(BOTH '/' FROM url), EXTRACT(EPOCH FROM timestamp), SUBSTRING(url FROM 1 FOR 5) FROM logs",
        "SELECT replace(url, 'a', 'b'), truncate(1.234, 2) FROM logs ORDER BY url DESC",
        'WITH "only" AS (SELECT 1 AS x) SELECT * FROM "only"',
        "SELECT status FROM ONLY logs WHERE status = 200",
        "SELECT $tag$keep ' inside$tag$ AS s, E'it\\'s ok' AS e, status\u00a0FROM\u200blogs WHERE'ok'='ok'",
        "SELECT status IS DISTINCT FROM 200 AS d1, status IS NOT DISTINCT FROM 404 AS d2 FROM logs",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    # 5. Live High-Scale query_endpoint blocks all subquery / TABLE / (FROM ...) / ONLY / PIVOT attacks before ClickHouse execution
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"n": 1}]

    stub_ch = _StubCH()
    hs_svc = SimpleNamespace(service_id="svc_alpha", client=stub_ch)
    for hs_attack in (
        "SELECT (SELECT name FROM system.tables LIMIT 1) AS leaked FROM logs LIMIT 1",
        "SELECT status FROM logs WHERE EXISTS (SELECT 1 FROM system.tables)",
        "SELECT status FROM logs WHERE url IN (SELECT service_id FROM high_scale_batch_publications)",
        "SELECT CASE WHEN (SELECT count() FROM information_schema.tables) > 0 THEN 1 ELSE 0 END AS leaked FROM logs LIMIT 1",
        "SELECT ARRAY(SELECT service_id FROM high_scale_batch_publications) AS leaked FROM logs LIMIT 1",
        "WITH c AS (TABLE high_scale_batch_publications) SELECT * FROM c",
        "SELECT (FROM high_scale_batch_publications SELECT service_id LIMIT 1) AS leaked FROM logs LIMIT 1",
        "SELECT * FROM ONLY logs",
        "WITH p AS (PIVOT logs ON status) SELECT * FROM p",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_attack, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
            )
    assert executed == []

    # Legitimate High-Scale scalar / IN / EXISTS subqueries on allowed tables rewrite canonical names & enforce additional_table_filters
    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(
            sql="SELECT (SELECT count() FROM client_vitals) AS v_cnt, pop FROM logs WHERE url IN (SELECT url FROM logs WHERE status = 200)",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
    )
    assert len(executed) == 1
    ran_sql, ran_settings = executed[0]
    assert "fla_cv_m_vit" in ran_sql and "fla_cv_m_req" in ran_sql
    assert "client_vitals" not in ran_sql and "FROM logs" not in ran_sql
    atf_str = str(ran_settings.get("additional_table_filters", ""))
    assert "fla_cv_m_req" in atf_str and "fla_cv_m_vit" in atf_str


def test_sec_t5_iter6_py01_py02_compound_paren_subquery_dot_union_and_tvf_pivot_hardening() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. (PY-A) Leading-LPAREN compound set-op / clause subqueries
    for compound_subq_attack in (
        "SELECT ARRAY((SELECT 'a' FROM logs WHERE 1=0) UNION ALL SELECT secret_col FROM ops_secret) AS leaked FROM logs",
        "SELECT ((SELECT 'a') UNION ALL SELECT name FROM system.tables LIMIT 1) AS leaked FROM logs LIMIT 1",
        "SELECT status FROM logs WHERE EXISTS ((SELECT 1 FROM logs) UNION ALL SELECT 1 FROM system.tables)",
        "SELECT status FROM logs WHERE url IN ((SELECT url FROM logs) UNION ALL SELECT service_id FROM high_scale_batch_publications)",
        "WITH RECURSIVE ops_secret AS (SELECT ((SELECT 'a' FROM logs WHERE 1=0) UNION ALL SELECT secret_col FROM ops_secret LIMIT 1) AS secret_col UNION ALL SELECT secret_col FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret",
        "SELECT (((SELECT 'a') ORDER BY 1) UNION ALL SELECT secret_col FROM ops_secret) AS leaked FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(compound_subq_attack, allowed_tables={"logs"})

    # 2. (PY-B) Dot-qualified .union / .except / .intersect inside WITH RECURSIVE & query bodies
    for dot_setop_attack in (
        "WITH RECURSIVE ops_secret AS (SELECT ({'union': 1}).union AS u, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        'WITH RECURSIVE ops_secret AS (SELECT t.union, b.secret_col FROM (SELECT 1 AS "union") t, ops_secret b) SELECT secret_col FROM ops_secret',
        'WITH RECURSIVE ops_secret AS (SELECT t.except, b.secret_col FROM (SELECT 1 AS "except") t, ops_secret b) SELECT secret_col FROM ops_secret',
        'WITH RECURSIVE ops_secret AS (SELECT t.intersect, b.secret_col FROM (SELECT 1 AS "intersect") t, ops_secret b) SELECT secret_col FROM ops_secret',
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(dot_setop_attack, allowed_tables={"logs"})

    # 3. (PY-C) Post-TVF PIVOT / UNPIVOT / TABLESAMPLE & interior scan + at_branch_start LPAREN statement check
    for tvf_or_branch_stmt_attack in (
        "SELECT * FROM range(1) PIVOT (first(current_setting('threads')) FOR range IN (0))",
        "SELECT * FROM range(1) UNPIVOT (v FOR c IN (range))",
        "SELECT * FROM range(1) PIVOT_WIDER (first(range) FOR range IN (0))",
        "SELECT * FROM range(1) PIVOT_LONGER (v FOR c IN (range))",
        "SELECT * FROM range(1) TABLESAMPLE SYSTEM (50) PIVOT (first(range) FOR range IN (0))",
        "SELECT * FROM range(1) r(current_setting('threads'))",
        "WITH c AS (SELECT 1) DESCRIBE (ops_secret)",
        "WITH c AS (SELECT 1) SUMMARIZE (ops_secret)",
        "WITH c AS (SELECT 1) EXPLAIN (FORMAT JSON) SELECT * FROM logs",
        "WITH c AS (SELECT 1) COPY (SELECT * FROM logs) TO '/tmp/x'",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(tvf_or_branch_stmt_attack, allowed_tables={"logs"})

    # 4. Benign controls on validate_user_sql (MUST pass!)
    for benign_sql in (
        "SELECT ((SELECT count(*) FROM logs) + 1) AS n FROM logs",
        "SELECT status FROM logs WHERE url IN ((SELECT url FROM logs) UNION ALL SELECT url FROM logs)",
        "SELECT ({'union': 1}).union AS u FROM logs",
        "SELECT r.x FROM range(1) AS r(x)",
        "SELECT r.x FROM range(1) r(x)",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    # 5. Live High-Scale query_endpoint blocks compound leading-LPAREN set-op subqueries before ClickHouse execution
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"n": 1}]

    stub_ch = _StubCH()
    hs_svc = SimpleNamespace(service_id="svc_alpha", client=stub_ch)
    for hs_compound_attack in (
        "SELECT ((SELECT 'a') UNION ALL SELECT name FROM system.tables LIMIT 1) AS leaked FROM logs LIMIT 1",
        "SELECT status FROM logs WHERE EXISTS ((SELECT 1 FROM logs) UNION ALL SELECT 1 FROM system.tables)",
        "SELECT status FROM logs WHERE url IN ((SELECT url FROM logs) UNION ALL SELECT service_id FROM high_scale_batch_publications)",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_compound_attack, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
            )
    assert executed == []

    # Benign compound IN ((SELECT ...) UNION ALL SELECT ...) on allowed table passes and rewrites logs -> fla_cv_m_req
    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(
            sql="SELECT status FROM logs WHERE url IN ((SELECT url FROM logs) UNION ALL SELECT url FROM logs)",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req"},
    )
    assert len(executed) == 1
    assert "fla_cv_m_req" in executed[0][0] and "FROM logs" not in executed[0][0]


def test_sec_t5_iter7_sql01_py01_non_keyword_contexts_numeric_boundaries_and_bare_aliases() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. (PY-01a) Numeric-boundary aliases (1_union, 0x1union, etc.) & AS union / ::union inside WITH RECURSIVE
    for rec_attack in (
        "WITH RECURSIVE ops_secret AS (SELECT 1 AS union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 1::union(a INT), secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 1_union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 1.5_union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 1e2_union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 0x1union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 0b1union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
        "WITH RECURSIVE ops_secret AS (SELECT 0o1union, secret_col FROM ops_secret) SELECT secret_col FROM ops_secret",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(rec_attack, allowed_tables={"logs"})

    # 2. (SQL-01) Non-keyword ., ::, AS, Sconst table refs, and bare/AS sample/prewhere/settings/format aliases
    for sql01_attack in (
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.where = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.select = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.order = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.limit = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.union = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON 1::union(x INT) IS NOT NULL, system.tables AS c",
        "SELECT b.service_id FROM logs AS sample, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs sample, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS prewhere, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs prewhere, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS settings, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs settings, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS format, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs format, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs sample(status), high_scale_batch_publications AS b",
        "SELECT secret_col FROM 'ops_secret'",
        "SELECT b.secret_col FROM logs AS a JOIN 'ops_secret' AS b ON true",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(sql01_attack, allowed_tables={"logs"})

    # 3. Benign controls on validate_user_sql MUST pass
    for benign_sql in (
        "WITH RECURSIVE r(n, x) AS (SELECT 1, 2 FROM logs LIMIT 1 UNION ALL SELECT n + 1, 3 AS except FROM r WHERE n < 3) SELECT * FROM r",
        "SELECT 1_000_000 AS union FROM logs",
        "SELECT s.status FROM logs AS sample(status)",
        "SELECT sample.status FROM logs AS sample WHERE sample.status = 200",
        "SELECT sample.status FROM logs sample WHERE sample.status = 200",
        "SELECT 1::union(a INT) AS u FROM logs",
        'SELECT a.where FROM (SELECT 1 AS "where") AS a JOIN logs AS b ON a.where = 1 LIMIT 1',
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    # 4. Live High-Scale query_endpoint blocks all SQL-01 comma-join bypasses before ClickHouse execution
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"n": 1}]

    stub_ch = _StubCH()
    hs_svc = SimpleNamespace(service_id="svc_alpha", client=stub_ch)
    for hs_sql01_attack in (
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.where = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.select = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.order = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.limit = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON a.union = 1, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON 1::union(x INT) IS NOT NULL, system.tables AS c",
        "SELECT b.service_id FROM logs AS sample, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs sample, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS prewhere, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs prewhere, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS settings, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs settings, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs AS format, high_scale_batch_publications AS b",
        "SELECT b.service_id FROM logs format, high_scale_batch_publications AS b",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_sql01_attack, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
            )
    assert executed == []

    # Benign High-Scale controls (AS sample, bare sample, 1::union, ON a.where = 1, logs AS c) rewrite & scope cleanly
    for benign_hs_sql in (
        "SELECT sample.status FROM logs AS sample WHERE sample.status = 200",
        "SELECT sample.status FROM logs sample WHERE sample.status = 200",
        "SELECT 1::union(a INT) AS u FROM logs",
        "SELECT c.status FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON a.where = 1, logs AS c WHERE b.status = 200",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=benign_hs_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, benign_hs_sql
        assert "fla_cv_m_req" in executed[0][0] and "FROM logs" not in executed[0][0], executed[0][0]


def test_sec_t5_iter8_sql01_py01_as_run_parity_float_dot_and_bracket_brace_delimiters() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. validate_user_sql rejects AS-shield cascades, recursive CTE AS-as, and []/{} comma-join/syntax bypasses
    for blocked_sql in (
        "SELECT name AS as FROM system.tables",
        "SELECT service_id AS as FROM high_scale_batch_publications",
        "SELECT c.name, a.as FROM (SELECT 1 AS as FROM logs) AS a, system.tables AS c",
        "SELECT secret_col, {'as': 1}.as FROM ops_secret",
        "SELECT {'as': secret_col}.as FROM ops_secret",
        "SELECT secret_col, {'as': 1}.as AS as FROM ops_secret",
        "SELECT secret_col, 1::as FROM ops_secret",
        'SELECT c.name FROM (SELECT 200 AS "as") AS a JOIN logs AS b ON b.status = a.as JOIN system.tables AS c ON true',
        'WITH RECURSIVE ops_secret AS (SELECT secret_col AS as FROM ops_secret UNION ALL SELECT "as" FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret',
        "WITH RECURSIVE ops_secret AS (SELECT 1 AS as UNION BY NAME SELECT secret_col FROM ops_secret) SELECT * FROM ops_secret",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON [x FOR x IN [1]] IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON [x FOR x IN [1] IF x = 1] IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {format: 1} IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {prewhere: 1} IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {settings: 1} IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {where: 1} IS NOT NULL, ops_secret AS c",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {limit: 1} IS NOT NULL, ops_secret AS c",
        "SELECT c.table_name FROM logs AS a JOIN logs AS b ON [x FOR x IN [1]] IS NOT NULL, information_schema.tables AS c",
        "SELECT c.tablename FROM logs AS a JOIN logs AS b ON {format: 1} IS NOT NULL, pg_tables AS c",
        "SELECT * FROM [ops_secret]",
        "SELECT * FROM {ops_secret: 1}",
        "SELECT [(1]) FROM logs",
        "SELECT {a: [1, 2} FROM logs",
        "SELECT [1, 2 FROM logs",
        "SELECT [(SELECT secret_col FROM ops_secret)] FROM logs",
        "SELECT {a: current_setting('threads')} FROM logs",
    ):
        with pytest.raises(SQLValidationError, match=".*"):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    # 2. Benign controls on validate_user_sql MUST pass
    for benign_sql in (
        "SELECT status, 1. FROM logs",
        "SELECT status AS as FROM logs",
        "SELECT {'as': status}.as AS where FROM logs",
        "WITH RECURSIVE r(n) AS (SELECT 1 AS as UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
        "SELECT [x FOR x IN [1, 2] IF x > 0] FROM logs",
        "SELECT {format: 1, replace: 2} FROM logs",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON [a.status, 1] = [b.status, 1]",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    # 3. Live High-Scale query_endpoint enforces AS-run parity, fla_cv_m_req + additional_table_filters, and []/{} blocking
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "as": "0.0.0.0"}]

    stub_ch = _StubCH()
    hs_svc = SimpleNamespace(service_id="svc_alpha", client=stub_ch)

    for hs_blocked_sql in (
        "SELECT name AS as FROM system.tables",
        "SELECT service_id AS as FROM high_scale_batch_publications",
        "SELECT c.name, a.as FROM (SELECT 1 AS as FROM logs) AS a, system.tables AS c",
        "SELECT secret_col, 1::as FROM ops_secret",
        "SELECT c.name FROM (SELECT 200 AS as FROM logs) AS a JOIN logs AS b ON b.status = a.as JOIN system.tables AS c ON true",
        "WITH RECURSIVE ops_secret AS (SELECT 1 AS as UNION ALL SELECT secret_col FROM ops_secret) SELECT * FROM ops_secret",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON {prewhere: 1} IS NOT NULL, system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs AS b ON {where: 1} IS NOT NULL, system.tables AS c",
        "SELECT c.service_id FROM logs AS a JOIN logs AS b ON {limit: 1} IS NOT NULL, high_scale_batch_publications AS c",
        "SELECT c.service_id FROM logs AS a JOIN logs AS b ON [x FOR x IN (1)] IS NOT NULL, high_scale_batch_publications AS c",
        "SELECT * FROM [logs]",
        "SELECT [(1]) FROM logs",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )
    assert executed == []

    # Critical check: SELECT service_id, client_ip AS as FROM request_facts MUST rewrite to fla_cv_m_req AND set additional_table_filters!
    for hs_benign_sql in (
        "SELECT service_id, client_ip AS as FROM request_facts",
        "SELECT status, 1. FROM logs",
        "SELECT status AS as FROM logs",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON [a.status, 1] = [b.status, 1]",
        """SELECT '{"where": 1}' AS j FROM logs""",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign_sql
        ran_sql, ran_settings = executed[0]
        assert "fla_cv_m_req" in ran_sql, (hs_benign_sql, ran_sql)
        assert "FROM request_facts" not in ran_sql and "FROM logs" not in ran_sql, (hs_benign_sql, ran_sql)
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), (hs_benign_sql, ran_settings)


def test_sec_t5_iter9_py01_clickhouse_bare_for_select_final_and_sample_offset_comma_joins() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. validate_user_sql rejects ClickHouse bare for/select aliases, <alias> FINAL, and SAMPLE <k> OFFSET <m> comma-joins
    for blocked_sql in (
        "SELECT c.name FROM logs for, system.tables AS c",
        "SELECT c.name FROM logs select, system.tables AS c",
        "SELECT c.service_id, c.secret_meta FROM logs for, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs select, high_scale_batch_publications AS c",
        "SELECT c.table_name FROM logs for, information_schema.tables AS c",
        "SELECT c.table_name FROM logs select, information_schema.tables AS c",
        "SELECT c.service_id, c.secret_meta FROM logs AS a JOIN logs for ON true, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs AS a JOIN logs select ON true, high_scale_batch_publications AS c",
        "FROM logs select, high_scale_batch_publications AS c SELECT c.secret_meta",
        "FROM logs for, high_scale_batch_publications AS c SELECT c.secret_meta",
        "SELECT c.secret_meta FROM logs prewhere FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs settings FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs format FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs for FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs select FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs sample FINAL, high_scale_batch_publications AS c",
        "SELECT c.name FROM logs SAMPLE 1/10 OFFSET 1/2, system.tables AS c",
        "SELECT c.service_id, c.secret_meta FROM logs SAMPLE 1/10 OFFSET 1/2, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs SAMPLE 0.5 OFFSET 0.1, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs SAMPLE 10000 OFFSET 5000, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs FINAL SAMPLE 1/10 OFFSET 1/2, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs for FINAL SAMPLE 1/10 OFFSET 1/2, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs select FINAL SAMPLE 1/10 OFFSET 1/2, high_scale_batch_publications AS c",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    # 2. Benign controls on validate_user_sql MUST pass
    for benign_sql in (
        "FROM logs SELECT status, url",
        "FROM logs SELECT DISTINCT status, url",
        "FROM logs SELECT ALL status, url",
        "FROM logs SELECT (status + 1) AS next_status, url",
        "SELECT select.status FROM logs AS select WHERE select.status = 200",
        "FROM logs select, logs AS c SELECT select.status, c.url",
        "SELECT for.status FROM logs for WHERE for.status = 200",
        "SELECT status FROM logs SAMPLE 1/10 OFFSET 1/2",
        "SELECT status FROM logs FINAL SAMPLE 1/10 OFFSET 1/2 WHERE status = 200",
        "SELECT status FROM logs LIMIT 10 OFFSET 5",
        "SELECT status FROM logs SAMPLE 1/10 LIMIT 10 OFFSET 5",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    # 3. Live High-Scale query_endpoint blocks all bare for/select, <alias> FINAL, and SAMPLE OFFSET comma-joins
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "client_ip": "0.0.0.0"}]

    stub_ch = _StubCH()
    hs_svc = SimpleNamespace(service_id="svc_alpha", client=stub_ch)

    for hs_blocked_sql in (
        "SELECT c.name FROM logs for, system.tables AS c",
        "SELECT c.name FROM logs select, system.tables AS c",
        "SELECT c.service_id, c.secret_meta FROM logs for, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs select, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs AS a JOIN logs select ON true, high_scale_batch_publications AS c",
        "FROM logs select, high_scale_batch_publications AS c SELECT c.secret_meta",
        "SELECT c.secret_meta FROM logs prewhere FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs for FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs select FINAL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs sample FINAL, high_scale_batch_publications AS c",
        "SELECT c.name FROM logs SAMPLE 1/10 OFFSET 1/2, system.tables AS c",
        "SELECT c.service_id, c.secret_meta FROM logs SAMPLE 1/10 OFFSET 1/2, high_scale_batch_publications AS c",
        "SELECT c.service_id, c.secret_meta FROM logs SAMPLE 0.5 OFFSET 0.1, high_scale_batch_publications AS c",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
            )
    assert executed == []

    # 4. SAMPLE <k> OFFSET <m>, request_facts AS c MUST rewrite BOTH tables to fla_cv_m_req + additional_table_filters!
    executed.clear()
    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(
            sql="SELECT c.service_id, c.client_ip FROM logs SAMPLE 1/10 OFFSET 1/2, request_facts AS c",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
    )
    assert len(executed) == 1
    sample_sql, sample_settings = executed[0]
    assert "request_facts" not in sample_sql and "FROM logs" not in sample_sql, sample_sql
    assert sample_sql.count("fla_cv_m_req") == 2, sample_sql
    assert "fla_cv_m_req" in str(sample_settings.get("additional_table_filters", "")), sample_settings

    # Cross-domain comma join after SAMPLE <k> OFFSET <m> rewrites client_vitals -> fla_cv_m_vit and scopes BOTH views
    executed.clear()
    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(
            sql="SELECT c.pop FROM logs SAMPLE 1/10 OFFSET 1/2, client_vitals AS c",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
    )
    assert len(executed) == 1
    vit_sql, vit_settings = executed[0]
    assert "client_vitals" not in vit_sql and "fla_cv_m_req" in vit_sql and "fla_cv_m_vit" in vit_sql, vit_sql
    vit_atf = str(vit_settings.get("additional_table_filters", ""))
    assert "fla_cv_m_req" in vit_atf and "fla_cv_m_vit" in vit_atf, vit_settings

    # Benign High-Scale controls (FROM-first SELECT, bare select/for aliases, SAMPLE OFFSET, LIMIT OFFSET)
    for hs_benign_sql in (
        "FROM logs SELECT status, url",
        "FROM logs select, request_facts AS c SELECT c.status",
        "SELECT select.status, select.timestamp FROM logs AS select WHERE select.status = 200",
        "SELECT for.status, for.timestamp FROM logs for WHERE for.status = 200",
        "SELECT status FROM logs SAMPLE 1/10 OFFSET 1/2",
        "SELECT status FROM logs LIMIT 10 OFFSET 5",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign_sql
        ran_sql, ran_settings = executed[0]
        assert "fla_cv_m_req" in ran_sql, (hs_benign_sql, ran_sql)
        assert "request_facts" not in ran_sql and "FROM logs" not in ran_sql, (hs_benign_sql, ran_sql)
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), (hs_benign_sql, ran_settings)


def test_sec_t5_iter10_py01_with_expression_cte_rewind_parametric_call_and_bare_set_alias() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    for blocked_sql in (
        "WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH coalesce((SELECT name FROM system.tables LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH coalesce((SELECT table_name FROM information_schema.tables LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH a AS (SELECT status FROM logs), coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM a",
        "SELECT * FROM (WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM logs)",
        "WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') SELECT status FROM logs",
        "WITH quantile((SELECT 0.5 FROM high_scale_batch_publications LIMIT 1))(1) SELECT status FROM logs",
        "WITH read_parquet('/etc/passwd') AS x SELECT status FROM logs",
        "WITH c(read_parquet('/etc/passwd')) AS (SELECT 1 FROM logs) SELECT * FROM c",
        "SELECT c.name FROM logs set, system.tables AS c",
        "SELECT c.secret_meta FROM logs set, high_scale_batch_publications AS c",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    for benign_sql in (
        "WITH coalesce(url, '/') AS u SELECT u FROM logs",
        "WITH replace(url, '/a', '/b') AS u SELECT u FROM logs",
        "WITH c(s, u) AS (SELECT status, url FROM logs) SELECT s, u FROM c",
        "SELECT c.url FROM logs set, logs AS c",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "client_ip": "0.0.0.0"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked_sql in (
        "WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH coalesce((SELECT name FROM system.tables LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH coalesce((SELECT table_name FROM information_schema.tables LIMIT 1), '') AS x SELECT x FROM logs",
        "WITH a AS (SELECT status FROM logs), coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM a",
        "SELECT * FROM (WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') AS x SELECT x FROM logs)",
        "WITH coalesce((SELECT secret_meta FROM high_scale_batch_publications LIMIT 1), '') SELECT status FROM logs",
        "WITH quantile((SELECT 0.5 FROM high_scale_batch_publications LIMIT 1))(1) SELECT status FROM logs",
        "SELECT c.name FROM logs set, system.tables AS c",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
            )
    assert executed == []

    for hs_with_rf_sql in (
        "WITH (SELECT status FROM request_facts LIMIT 1) AS s SELECT s FROM logs",
        "WITH coalesce((SELECT status FROM request_facts LIMIT 1), '200') AS s SELECT s FROM logs",
        "WITH a AS (SELECT url FROM logs), coalesce((SELECT status FROM request_facts LIMIT 1), '200') AS s SELECT s, url FROM a",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_with_rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
        )
        assert len(executed) == 1, hs_with_rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "FROM logs" not in ran_sql, ran_sql
        assert ran_sql.count("fla_cv_m_req") == 2, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings

    executed.clear()
    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(sql="SELECT c.pop FROM logs set, client_vitals AS c", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
    )
    assert len(executed) == 1
    set_sql, set_settings = executed[0]
    assert "client_vitals" not in set_sql and "FROM logs" not in set_sql, set_sql
    assert "fla_cv_m_req" in set_sql and "fla_cv_m_vit" in set_sql, set_sql
    set_atf = str(set_settings.get("additional_table_filters", ""))
    assert "fla_cv_m_req" in set_atf and "fla_cv_m_vit" in set_atf, set_settings


def test_sec_t5_iter10_join_on_keywords_and_from_first_select_fn_projections() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    for blocked_sql in (
        "SELECT c.name FROM logs AS a JOIN logs select ON a.url = select.url, system.tables AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS where ON a.url = where.url, high_scale_batch_publications AS c",
        "SELECT c.name FROM logs AS a JOIN logs for ON a.url = for.url, system.tables AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS limit ON a.url = limit.url, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS offset ON a.url = offset.url, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS union ON a.url = union.url, high_scale_batch_publications AS c",
        "WITH a AS (SELECT 1 AS format FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON format = 1, ops_secret AS c",
        "SELECT c.* FROM (SELECT 1 AS format FROM logs) AS a JOIN logs AS b ON format = 1, 'secret.parquet' AS c",
        "WITH a AS (SELECT 1 AS prewhere FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON prewhere = 1, ops_secret AS c",
        "WITH a AS (SELECT 1 AS settings FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON settings = 1, ops_secret AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON where = 1, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON 1 = where, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT toUInt8(0) AS where FROM logs) AS b ON NOT where, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT 1 AS offset FROM logs) AS b ON offset IS NOT NULL, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT 1 AS select FROM logs) AS b ON select IN (1, 2), high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT 1 AS having FROM logs) AS b ON CASE WHEN having = 1 THEN 1 ELSE 0 END = 1, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS inner, 1 AS where FROM logs) AS a JOIN logs AS b ON inner = 1 AND where = 1, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS join, 1 AS where FROM logs) AS a JOIN logs AS b ON join = 1 AND where = 1, high_scale_batch_publications AS c",
        "SELECT c.name FROM logs AS a JOIN logs select ON (a.url = select.url), system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs select USING (url), system.tables AS c",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    for benign_sql in (
        "FROM logs SELECT left(url, 3), status",
        "FROM logs SELECT right(url, 3), status",
        "FROM logs SELECT format('{}', url), status",
        "FROM logs SELECT any(url), status",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON a.url = b.url WHERE a.status = 200",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON a.url = b.url WHERE NOT (a.status = 500)",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON a.url = b.url LIMIT +10",
        "SELECT a.status, count() FROM logs AS a JOIN logs AS b ON a.url = b.url GROUP BY a.status",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON a.url = b.url ORDER BY a.status",
        "SELECT a.status FROM logs AS a JOIN logs AS b ON a.url = b.url UNION ALL SELECT status FROM logs",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "client_ip": "0.0.0.0"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked_sql in (
        "SELECT c.name FROM logs AS a JOIN logs select ON a.url = select.url, system.tables AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS where ON a.url = where.url, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs for ON a.url = for.url, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS union ON a.url = union.url, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON where = 1, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON 1 = where, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT toUInt8(0) AS where FROM logs) AS b ON NOT where, high_scale_batch_publications AS c",
        "SELECT c.secret_meta FROM logs AS a JOIN (SELECT 1 AS select FROM logs) AS b ON select IN (1, 2), high_scale_batch_publications AS c",
        "SELECT c.name FROM logs AS a JOIN logs select ON (a.url = select.url), system.tables AS c",
        "SELECT c.name FROM logs AS a JOIN logs select USING (url), system.tables AS c",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
            )
    assert executed == []

    for hs_rf_sql, expected_count in (
        (
            "SELECT c.service_id, c.client_ip FROM logs AS a JOIN logs select ON a.url = select.url, request_facts AS c",
            3,
        ),
        (
            "SELECT c.service_id, c.client_ip FROM logs AS a JOIN logs AS union ON a.url = union.url, request_facts AS c",
            3,
        ),
        (
            "SELECT c.client_ip FROM (SELECT 1 AS where FROM logs) AS a JOIN logs AS b ON where = 1, request_facts AS c",
            3,
        ),
        (
            "SELECT c.client_ip FROM (SELECT 1 AS union FROM logs) AS a JOIN logs AS b ON union = 1, request_facts AS c",
            3,
        ),
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "FROM logs" not in ran_sql, ran_sql
        assert ran_sql.count("fla_cv_m_req") == expected_count, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings

    for hs_benign_sql in (
        "FROM logs SELECT format('{}', url), status",
        "FROM logs SELECT any(url), status",
        "SELECT union.status FROM logs AS union WHERE union.status = 200",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign_sql
        ran_sql, ran_settings = executed[0]
        assert "fla_cv_m_req" in ran_sql and "FROM logs" not in ran_sql, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings


def test_sec_t5_iter10_within_group_ordered_set_aggregate_comma_joins() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    for blocked_sql in (
        "SELECT (SELECT c.secret_col FROM logs AS x JOIN logs AS y ON mode() WITHIN GROUP (ORDER BY outer_logs.status) = 200, ops_secret AS c LIMIT 1) AS leaked FROM logs AS outer_logs",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON quantile(0.5) WITHIN GROUP (ORDER BY a.status) = 200, high_scale_batch_publications AS c",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    for benign_sql in (
        "SELECT a.within, count() FROM (SELECT status AS within FROM logs) AS a GROUP BY within",
        "SELECT a.within, count() FROM (SELECT status AS within FROM logs) AS a JOIN logs AS b ON a.within = b.status GROUP BY a.within",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "client_ip": "0.0.0.0"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    with pytest.raises(PermissionError):
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(
                sql="SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON quantile(0.5) WITHIN GROUP (ORDER BY a.status) = 200, high_scale_batch_publications AS c",
                max_rows=10,
            ),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
    assert executed == []

    hs_query.query_endpoint(
        hs_svc,
        DashboardQueryRequest(
            sql="SELECT c.service_id, c.client_ip FROM logs AS a JOIN logs AS b ON quantile(0.5) WITHIN GROUP (ORDER BY a.status) = 200, request_facts AS c",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req"},
    )
    assert len(executed) == 1
    wg_sql, wg_settings = executed[0]
    assert "request_facts" not in wg_sql and "FROM logs" not in wg_sql, wg_sql
    assert wg_sql.count("fla_cv_m_req") == 3, wg_sql
    assert "fla_cv_m_req" in str(wg_settings.get("additional_table_filters", "")), wg_settings


def test_sec_t5_iter12_as_parity_global_not_in_on_join_skip_union_split_and_tuple_dot() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. Sub-case 1A (AS-parity), 1B (GLOBAL NOT IN & USING), and 1C (ON 1 = <prev_kw> JOIN) attacks blocked
    for blocked_sql in (
        # 1A: AS-run operand prefix swallowing FROM / JOIN
        "WITH 'ok' AS as SELECT name, as FROM system.tables",
        "WITH 'ok' AS as SELECT url, country FROM logs UNION ALL SELECT secret_meta, as FROM high_scale_batch_publications",
        "SELECT c.secret_meta FROM (SELECT 1 AS as, url FROM logs) AS a JOIN logs AS b ON 1 = as JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT 2 * as FROM system.tables",
        "SELECT as FROM system.tables",
        # 1B: GLOBAL NOT IN & USING inside ON keeping in_on_clause=True
        "SELECT c.secret_meta FROM (SELECT 1 AS where, '/ok' AS url FROM logs) AS a JOIN logs AS b ON a.url = b.url AND a.url GLOBAL NOT IN ('/nope') AND where = 1, high_scale_batch_publications AS c",
        "SELECT c.name FROM (SELECT 1 AS where, '/ok' AS url FROM logs) AS a JOIN logs AS b ON a.url = b.url AND a.url GLOBAL NOT IN ('/nope') AND where = 1, system.tables AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS using, 1 AS where FROM logs) AS a JOIN logs AS b ON 1 = using AND where = 1, high_scale_batch_publications AS c",
        "WITH a AS (SELECT 'x' AS format FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON format NOT IN ('y'), ops_secret AS c",
        # 1C: ON 1 = <prev_word> JOIN <disallowed_table | parquet>
        "WITH a AS (SELECT 1 AS zone FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 1 = zone JOIN ops_secret AS c ON 1 = 1",
        "WITH a AS (SELECT 1 AS escape FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 1 = escape JOIN ops_secret AS c ON 1 = 1",
        "WITH a AS (SELECT 1 AS global FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 1 = global JOIN ops_secret AS c ON 1 = 1",
        "SELECT * FROM (SELECT 1 AS zone FROM logs) AS a JOIN logs AS b ON 1 = zone JOIN 'secret.parquet' AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS to FROM logs) AS a JOIN logs AS b ON 1 = to JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS and FROM logs) AS a JOIN logs AS b ON 1 = and JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS on FROM logs) AS a JOIN logs AS b ON 1 = on JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON left(read_parquet('/etc/passwd'), 3) = '/a'",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    # 2. Benign controls (1A AS chains, 1B GLOBAL LEFT JOIN / WHERE NOT, 1C post-ON ORDER BY/GROUP BY/UNION, 1D tuple .1)
    for benign_sql in (
        "WITH 1 AS as SELECT 1 = as AS flag, url FROM logs",
        "SELECT 1 = where AS flag FROM (SELECT 1 AS where FROM logs)",
        "SELECT x.select AS as FROM (SELECT 1 AS select FROM logs) AS x",
        "SELECT a.url FROM logs AS a GLOBAL LEFT JOIN logs AS b ON a.url = b.url",
        "SELECT a.url FROM logs AS a JOIN logs AS b ON a.url GLOBAL NOT IN ('/nope') WHERE NOT (a.status = 500)",
        "WITH b AS (SELECT 200 AS zone FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = zone ORDER BY a.status, a.url",
        "WITH b AS (SELECT 200 AS escape FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = escape GROUP BY a.status, a.url",
        "WITH b AS (SELECT 200 AS global FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = global UNION ALL SELECT status, url FROM logs",
        "WITH b AS (SELECT 200 AS on FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = on WHERE a.status = 200",
        "SELECT status FROM (SELECT tuple(1) AS where, 200 AS status FROM logs) AS s WHERE where.1 = 1",
        "SELECT union.1 FROM (SELECT tuple(200) AS union FROM logs) AS u",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "client_ip": "0.0.0.0"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked_sql in (
        "WITH 'ok' AS as SELECT name, as FROM system.tables",
        "WITH 'ok' AS as SELECT url, country FROM logs UNION ALL SELECT secret_meta, as FROM high_scale_batch_publications",
        "SELECT c.secret_meta FROM (SELECT 1 AS as, url FROM logs) AS a JOIN logs AS b ON 1 = as JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS where, '/ok' AS url FROM logs) AS a JOIN logs AS b ON a.url = b.url AND a.url GLOBAL NOT IN ('/nope') AND where = 1, high_scale_batch_publications AS c",
        "SELECT c.name FROM (SELECT 1 AS where, '/ok' AS url FROM logs) AS a JOIN logs AS b ON a.url = b.url AND a.url GLOBAL NOT IN ('/nope') AND where = 1, system.tables AS c",
        "SELECT c.secret_meta FROM (SELECT 1 AS zone FROM logs) AS a JOIN logs AS b ON 1 = zone JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS escape FROM logs) AS a JOIN logs AS b ON 1 = escape JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS global FROM logs) AS a JOIN logs AS b ON 1 = global JOIN high_scale_batch_publications AS c ON 1 = 1",
        "SELECT c.secret_meta FROM (SELECT 1 AS to FROM logs) AS a JOIN logs AS b ON 1 = to JOIN high_scale_batch_publications AS c ON 1 = 1",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
            )
    assert executed == []

    # 3. request_facts 3x rewrite verification across 1A, 1B, and 1C
    for hs_rf_sql in (
        "SELECT c.service_id, c.url FROM (SELECT 1 AS as, url FROM logs) AS a JOIN logs AS b ON 1 = as JOIN request_facts AS c ON 1 = 1",
        "SELECT c.service_id, c.client_ip FROM (SELECT 1 AS where, '/ok' AS url FROM logs) AS a JOIN logs AS b ON a.url = b.url AND a.url GLOBAL NOT IN ('/nope') AND where = 1, request_facts AS c",
        "SELECT c.service_id, c.url FROM (SELECT 1 AS zone, url FROM logs) AS a JOIN logs AS b ON 1 = zone JOIN request_facts AS c ON 1 = 1",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "FROM logs" not in ran_sql, ran_sql
        assert ran_sql.count("fla_cv_m_req") == 3, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings

    # 4. UNION branch splitting preserves native pop on client_vitals when Branch 1 ends with * or zone
    for union_sql in (
        "FROM (SELECT url, pop AS pop FROM logs) SELECT * UNION ALL SELECT url, pop FROM client_vitals",
        "SELECT url, pop FROM logs zone UNION ALL SELECT url, pop FROM client_vitals",
        "SELECT a.url, a.pop FROM (SELECT 1 AS zone, url, pop FROM logs) AS a JOIN logs AS b ON 1 = zone UNION ALL SELECT url, pop FROM client_vitals",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=union_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req", "rum_vitals_facts": "fla_cv_m_vit"},
        )
        assert len(executed) == 1, union_sql
        u_sql, u_settings = executed[0]
        left_b, right_b = u_sql.split("UNION ALL", 1)
        assert "custom_fields['pop']" in left_b, left_b
        assert "custom_fields['pop']" not in right_b and "fla_cv_m_vit" in right_b, right_b
        u_atf = str(u_settings.get("additional_table_filters", ""))
        assert "fla_cv_m_req" in u_atf and "fla_cv_m_vit" in u_atf, u_settings

    # 5. Benign High-Scale tuple .1, AS-parity, and dot-qualified IN/JOIN alias controls execute cleanly
    for hs_benign_sql in (
        "WITH 1 AS as SELECT 1 = as AS flag, url FROM logs",
        "SELECT status FROM (SELECT tuple(1) AS where, 200 AS status FROM logs) AS s WHERE where.1 = 1",
        "SELECT union.1 FROM (SELECT tuple(200) AS union FROM logs) AS u",
        "WITH b AS (SELECT '1' AS in FROM logs) SELECT a.url FROM logs AS a JOIN b ON '1' = b.in ORDER BY a.url",
        "WITH b AS (SELECT '1' AS join FROM logs) SELECT a.url FROM logs AS a JOIN b ON '1' = b.join ORDER BY a.url",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign_sql


def test_sec_t5_iter13_contextual_as_prefix_leading_dot_float_and_on_join_table_fp() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. Contextual AS-prefix attacks (AT TIME ZONE as [r=1] and <sample|format|to|escape|interval|window|limit> AS as [r=2]) blocked
    for blocked_sql in (
        "WITH 'UTC' AS as SELECT secret_col, toDateTime(0) AT TIME ZONE as FROM ops_secret",
        "WITH 1 AS sample SELECT secret_col, sample AS as FROM ops_secret",
        "WITH 1 AS format SELECT secret_col, format AS as FROM ops_secret",
        "WITH 1 AS to SELECT secret_col, to AS as FROM ops_secret",
        "WITH 1 AS escape SELECT secret_col, escape AS as FROM ops_secret",
        "WITH 1 AS interval SELECT secret_col, interval AS as FROM ops_secret",
        "WITH 1 AS window SELECT secret_col, window AS as FROM ops_secret",
        "WITH 1 AS limit SELECT secret_col, limit AS as FROM ops_secret",
        "WITH a AS (SELECT 1 AS sample FROM logs) SELECT t.c FROM a, LATERAL (SELECT col_val AS c, sample AS as FROM ops_secret) AS t(c, z)",
        "WITH a AS (SELECT 1 AS format FROM logs) SELECT t.c FROM a, LATERAL (SELECT col_val AS c, format AS as FROM ops_secret) AS t(c, z)",
        "SELECT secret_col, .5 AS as FROM ops_secret",
        "WITH a AS (SELECT 1 AS zone FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 1 = zone JOIN ops_secret AS c ON 1 = 1",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(blocked_sql, allowed_tables={"logs"})

    # 2. Benign controls: leading-dot float .5, ON a.status = join/table, AT TIME ZONE as on allowed table, sample AS as on allowed table
    for benign_sql in (
        "FROM logs SELECT .5, url",
        "SELECT .5 AS half, .25e2 AS quarter_hundred, url FROM logs",
        "WITH 'UTC' AS as SELECT url, toDateTime(0) AT TIME ZONE as FROM logs",
        "WITH a AS (SELECT 1 AS sample, url FROM logs) SELECT url, sample AS as FROM a",
        "WITH b AS (SELECT 200 AS join FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = join ORDER BY a.url",
        "WITH b AS (SELECT 200 AS table FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = table ORDER BY a.url",
        "SELECT status FROM (SELECT tuple(1) AS where, 200 AS status FROM logs) AS s WHERE where.1 = 1",
        "SELECT union.1 FROM (SELECT tuple(200) AS union FROM logs) AS u",
    ):
        assert validate_user_sql(benign_sql, allowed_tables={"logs"}).parse_tree.get("statements"), benign_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "url": "/ok"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked_sql in (
        "WITH 'UTC' AS as SELECT secret_meta, toDateTime(0) AT TIME ZONE as FROM high_scale_batch_publications",
        "WITH 1 AS sample SELECT secret_meta, sample AS as FROM high_scale_batch_publications",
        "WITH 1 AS format SELECT secret_meta, format AS as FROM high_scale_batch_publications",
        "WITH 1 AS to SELECT secret_meta, to AS as FROM high_scale_batch_publications",
        "WITH 1 AS escape SELECT secret_meta, escape AS as FROM high_scale_batch_publications",
        "WITH 1 AS interval SELECT secret_meta, interval AS as FROM high_scale_batch_publications",
        "WITH 1 AS window SELECT secret_meta, window AS as FROM high_scale_batch_publications",
        "WITH 1 AS limit SELECT secret_meta, limit AS as FROM high_scale_batch_publications",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )
    assert executed == []

    # Verify request_facts is rewritten and scoped (never left raw!) for AT TIME ZONE as and sample/to AS as
    for rf_sql in (
        "WITH 'UTC' AS as SELECT service_id, url, toDateTime(0) AT TIME ZONE as FROM request_facts",
        "WITH 1 AS sample SELECT service_id, url, sample AS as FROM request_facts",
        "WITH 1 AS to SELECT service_id, url, to AS as FROM request_facts",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "fla_cv_m_req" in ran_sql, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings

    # Benign High-Scale .5 and ON 200 = join controls (+ _rewrite_console_sql_with_canonicals on ON 200 = table)
    for hs_benign in (
        "FROM logs SELECT .5, url",
        "WITH b AS (SELECT 200 AS join FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = join ORDER BY a.url",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign

    rewritten_tbl, tbl_canons = hs_query._rewrite_console_sql_with_canonicals(
        "WITH b AS (SELECT 200 AS table FROM logs) SELECT a.status, a.url FROM logs AS a JOIN b ON a.status = table ORDER BY a.url",
        svc_table="logs_svc_alpha",
        names={"request_facts": "fla_cv_m_req"},
    )
    assert "fla_cv_m_req" in rewritten_tbl and tbl_canons == {"request_facts"}, rewritten_tbl


def test_sec_t5_iter14_word_op_clause_as_select_head_modifiers_and_multiword_on_join() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    blocked_sqls = (
        # Word-operator + _AS_CLAUSE_OPENING_WORDS AS as (r = 2 -> 1)
        "WITH 1 AS set SELECT secret_col, 1 AND set AS as FROM ops_secret",
        "WITH 1 AS by SELECT secret_col, 1 AND by AS as FROM ops_secret",
        "WITH 1 AS where SELECT secret_col, 1 AND where AS as FROM ops_secret",
        "WITH 1 AS values SELECT secret_col, NOT values AS as FROM ops_secret",
        "WITH a AS (SELECT true AS set FROM logs) SELECT t.c FROM a, LATERAL (SELECT secret_col AS c, true AND set AS as FROM ops_secret) AS t(c, z)",
        "WITH a AS (SELECT true AS values FROM logs) SELECT t.c FROM a, LATERAL (SELECT secret_col AS c, NOT values AS as FROM ops_secret) AS t(c, z)",
        "WITH 1 AS x, 1 AS by SELECT secret_col, 1 IS DISTINCT FROM x AND by AS as FROM ops_secret",
        "WITH 1 AS by SELECT secret_col, 1 IS DISTINCT FROM NOT by AS as FROM ops_secret",
        "WITH NULL AS by SELECT secret_col, toDateTime(0) AT TIME ZONE NOT by AS as FROM ops_secret",
        "WITH 1 AS div, 1 AS by SELECT secret_col, 1 = div AND by AS as FROM ops_secret",
        "WITH 1 AS all, 1 AS by SELECT secret_col, 1 = all AND by AS as FROM ops_secret",
        "WITH 0 AS CASE, 1 AS by SELECT secret_col, 1 = CASE AND by AS as FROM ops_secret",
        "WITH 0 AS CASE, 1 AS by SELECT secret_col, 1 = CASE AND by AS as END FROM ops_secret",
        # Multi-word operator + operator-named identifier AS as
        "WITH 'UTC' AS between SELECT secret_col, toDateTime(0) AT TIME ZONE between AS as FROM ops_secret",
        "WITH a AS (SELECT 1 AS between FROM logs) SELECT t.c FROM a, LATERAL (SELECT secret_col AS c, 1 IS DISTINCT FROM between AS as FROM ops_secret) AS t(c, z)",
        "WITH 1 AS escape SELECT secret_col, 1 IS DISTINCT FROM escape AS as FROM ops_secret",
        "WITH 'a' AS set SELECT secret_col, 'a' SIMILAR TO set AS as FROM ops_secret",
        # SELECT-head modifiers (DISTINCT ON (...), TOP <N|0xN|(N)> [WITH TIES]) before as / <clause_kw> AS as
        "WITH url AS as SELECT DISTINCT ON (service_id) as FROM ops_secret",
        "WITH url AS as SELECT TOP 1 as FROM ops_secret",
        "WITH url AS as SELECT TOP (1) as FROM ops_secret",
        "WITH url AS as SELECT TOP 0x1 as FROM ops_secret",
        "WITH url AS as SELECT TOP 0b1 as FROM ops_secret",
        "WITH url AS as SELECT TOP 0o1 as FROM ops_secret",
        "WITH url AS as SELECT ALL TOP 1 as FROM ops_secret",
        "WITH url AS as SELECT DISTINCT TOP 1 as FROM ops_secret",
        "WITH url AS as SELECT TOP 1 WITH TIES as FROM ops_secret ORDER BY 1",
        "WITH 1 AS by SELECT TOP 1 by AS as FROM ops_secret",
        "WITH 1 AS by SELECT DISTINCT ON (service_id) by AS as FROM ops_secret",
        # Multi-word operator + ON ... JOIN bypasses
        "WITH 'UTC' AS and SELECT c.secret_col FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE and JOIN ops_secret AS c ON 1 = 1",
        "WITH 1 AS between SELECT c.secret_col FROM logs AS a JOIN logs AS b ON 1 IS DISTINCT FROM between JOIN ops_secret AS c ON 1 = 1",
        'WITH a AS (SELECT 1 AS "between" FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 1 IS DISTINCT FROM between JOIN ops_secret AS c ON 1 = 1',
        "WITH 'UTC' AS CASE SELECT c.secret_col FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE CASE JOIN ops_secret AS c ON 1 = 1",
        "WITH 'UTC' AS CASE SELECT c.secret_col FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE CASE JOIN ops_secret AS c ON CASE WHEN 1 = 1 THEN 1 END = 1",
    )
    for bad_sql in blocked_sqls:
        with pytest.raises(SQLValidationError):
            validate_user_sql(bad_sql, allowed_tables={"logs"})

    benign_sqls = (
        "WITH 1 AS as SELECT url FROM logs div WHERE as = 1",
        "SELECT a.url FROM logs AS a JOIN logs AS b ON CASE a.status WHEN 200 THEN 1 ELSE 0 END = 1",
        "WITH 1 AS as SELECT a.url FROM logs AS a JOIN logs AS b ON CASE WHEN 1 = 1 THEN 1 ELSE as END = 1",
        "SELECT TOP 1 url FROM logs",
        "SELECT TOP 0x1 url FROM logs",
        "SELECT DISTINCT ON (url) url FROM logs",
        "WITH b AS (SELECT 2 AS div, 1 AS join FROM logs) SELECT a.url FROM logs AS a JOIN b ON 2 = div DIV join ORDER BY a.url",
    )
    for ok_sql in benign_sqls:
        assert validate_user_sql(ok_sql, allowed_tables={"logs"}).parse_tree.get("statements"), ok_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "url": "/ok"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked in (
        "WITH 1 AS set SELECT secret_meta, 1 AND set AS as FROM high_scale_batch_publications",
        "WITH 1 AS by SELECT secret_meta, 1 AND by AS as FROM high_scale_batch_publications",
        "WITH 'UTC' AS between SELECT secret_meta, toDateTime(0) AT TIME ZONE between AS as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT DISTINCT ON (secret_meta) as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP 1 as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP 0x1 as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP 1 WITH TIES as FROM high_scale_batch_publications ORDER BY 1",
        "WITH 'UTC' AS and SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE and JOIN high_scale_batch_publications AS c ON 1 = 1",
        "WITH 'UTC' AS CASE SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE CASE JOIN high_scale_batch_publications AS c ON 1 = 1",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )
    assert executed == []

    # Verify request_facts is rewritten to fla_cv_m_req AND additional_table_filters is populated (never bypassed!)
    for rf_sql in (
        "WITH 1 AS by SELECT service_id, url, 1 AND by AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT DISTINCT ON (service_id) as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT TOP 1 as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT TOP 0x1 as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH 'UTC' AS and SELECT c.service_id, c.url FROM logs AS a JOIN logs AS b ON toDateTime(0) = toDateTime(0) AT TIME ZONE and JOIN request_facts AS c ON 1 = 1",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "fla_cv_m_req" in ran_sql, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings


def test_sec_t5_iter15_hexfloat_signed_top_select_mod_word_op_and_duckdb_factorial() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    blocked_sqls = (
        # Fractional & trailing-dot C99 hexadecimal floating-point literals (0x1., 0x1.8p2, 0x1.p2, 0x0.8p2)
        "SELECT secret_col, 0x1. AS as FROM ops_secret",
        "SELECT secret_col, 0X0. AS as FROM ops_secret",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON 1 = 0x1. JOIN ops_secret AS c ON 1 = 1",
        "WITH secret_col AS as SELECT TOP 0x1.8p2 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP 0x1.p2 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP 0x0.8p2 as FROM ops_secret",
        "WITH secret_col AS by SELECT TOP 0x1.8p2 by AS as FROM ops_secret",
        # Signed SELECT [ALL|DISTINCT|DISTINCT ON (...)] TOP [+|-] <num>
        "WITH secret_col AS as SELECT TOP +1 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP -1 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP - 1 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP +.5 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP +0x1 as FROM ops_secret",
        "WITH secret_col AS as SELECT DISTINCT TOP +1 as FROM ops_secret",
        "WITH secret_col AS as SELECT ALL TOP -1 as FROM ops_secret",
        "WITH secret_col AS as SELECT DISTINCT ON (1) TOP +1 as FROM ops_secret",
        "WITH secret_col AS as SELECT DISTINCT ON (1) TOP +1 WITH TIES as FROM ops_secret ORDER BY 1",
        "WITH secret_col AS by SELECT TOP +1 by AS as FROM ops_secret",
        # SELECT-head modifier + binary word-operator identifier AS as (Sub-vector 1A)
        "WITH secret_col AS AND SELECT TOP 1 AND AS as FROM ops_secret",
        "WITH secret_col AS AND SELECT TOP +1 AND AS as FROM ops_secret",
        "WITH secret_col AS DIV SELECT DISTINCT ON (1) DIV AS as FROM ops_secret",
        "WITH secret_col AS OR SELECT TOP 1 WITH TIES OR AS as FROM ops_secret ORDER BY 1",
        "WITH secret_col AS MOD SELECT TOP (1) MOD AS as FROM ops_secret",
        "WITH secret_col AS ON SELECT DISTINCT ON (1) TOP 1 ON AS as FROM ops_secret",
        # DuckDB postfix factorial `!` (Sub-vector 1B)
        "SELECT secret_col, 5! AS as FROM ops_secret",
        "SELECT secret_col, (2 + 3)! AS as FROM ops_secret",
        "SELECT secret_col, ((3!)::INT)! AS as FROM ops_secret",
        "WITH a AS (SELECT 5 AS x FROM logs) SELECT t.c FROM a, LATERAL (SELECT secret_col AS c, x! AS as FROM ops_secret) AS t(c, z)",
        "SELECT c.secret_col FROM logs AS a JOIN logs AS b ON 120 = 5! JOIN ops_secret AS c ON 1 = 1",
        "WITH a AS (SELECT 5 AS x FROM logs) SELECT c.secret_col FROM a JOIN logs AS b ON 120 = x! JOIN ops_secret AS c ON 1 = 1",
        # Preserve ClickHouse prefix `!as` rejection on non-allowlisted tables
        "WITH 0 AS as SELECT !as FROM ops_secret",
    )
    for bad_sql in blocked_sqls:
        with pytest.raises(SQLValidationError):
            validate_user_sql(bad_sql, allowed_tables={"logs"})

    # Benign queries including `top -1 AS as` / `top +1 AS as` subtraction, hex floats, prefix `!as`, and `ON !join = 1`
    benign_sqls = (
        "WITH 1 AS top SELECT url, top -1 AS as FROM logs",
        "WITH 1 AS top SELECT url, top +1 AS as FROM logs",
        "WITH 1 AS top SELECT url, top -0x1 AS as FROM logs",
        "SELECT 0x1. AS one_hex, 0x1.8p2 AS six_hex, 5! AS fact_five, url FROM logs",
        "WITH 0 AS as SELECT !as AS not_as, url FROM logs",
        "WITH b AS (SELECT 0 AS join FROM logs) SELECT a.url FROM logs AS a JOIN b ON !join = 1",
    )
    for ok_sql in benign_sqls:
        assert validate_user_sql(ok_sql, allowed_tables={"logs"}).parse_tree.get("statements"), ok_sql

    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "url": "/ok"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked in (
        "SELECT secret_meta, 0x1. AS as FROM high_scale_batch_publications",
        "SELECT c.secret_meta FROM logs AS a JOIN logs AS b ON 1 = 0x1. JOIN high_scale_batch_publications AS c ON 1 = 1",
        "WITH secret_meta AS as SELECT TOP 0x1.8p2 as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP +1 as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP - 1 as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT DISTINCT ON (1) TOP +1 as FROM high_scale_batch_publications",
        "WITH secret_meta AS AND SELECT TOP 1 AND AS as FROM high_scale_batch_publications",
        "WITH secret_meta AS DIV SELECT DISTINCT ON (1) DIV AS as FROM high_scale_batch_publications",
        "WITH secret_meta AS OR SELECT TOP 1 WITH TIES OR AS as FROM high_scale_batch_publications ORDER BY 1",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )
    assert executed == []

    # Verify request_facts is rewritten AND additional_table_filters is populated across all Iteration-15 vectors
    for rf_sql in (
        "SELECT service_id, url, 0x1. AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT TOP 0x1.8p2 as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT TOP +1 as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT DISTINCT ON (service_id) TOP +1 as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH concat(service_id, '|', url) AS AND SELECT TOP 1 AND AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH concat(service_id, '|', url) AS DIV SELECT DISTINCT ON (service_id) DIV AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH concat(service_id, '|', url) AS OR SELECT TOP 1 WITH TIES OR AS as FROM request_facts WHERE service_id = 'svc_b' ORDER BY 1",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "fla_cv_m_req" in ran_sql, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings

    # Benign subtraction `top -1 AS as` / `top +1 AS as` must succeed cleanly in High-Scale query_endpoint (0 FPs!)
    for hs_benign in (
        "WITH 1 AS top SELECT url, top -1 AS as FROM logs",
        "WITH 1 AS top SELECT url, top +1 AS as FROM logs",
        "WITH 1 AS top SELECT url, top -0x1 AS as FROM logs",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=hs_benign, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, hs_benign


def test_sec_t5_iter16_duckdb_signed_top_even_as_bypass_and_benign_aliases() -> None:
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale import query as hs_query
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    # 1. DuckDB r==2 / r==4 `SELECT [ALL|DISTINCT|DISTINCT ON (...)] top [+|-] <num> AS as FROM <foreign_table>`
    # and ClickHouse r==1 / r==3 `SELECT TOP [+|-] <num> as FROM <foreign_table>` MUST ALL be blocked!
    blocked_sqls = (
        "SELECT top - 0 AS as FROM ops_secret",
        "SELECT top + 1 AS as FROM ops_secret",
        "SELECT top - 1 AS as FROM ops_secret AS t(s, top)",
        "SELECT ALL top + 0 AS as FROM ops_secret",
        "SELECT DISTINCT top - 1 AS as FROM ops_secret",
        "SELECT DISTINCT ON (1) top + 0 AS as FROM ops_secret",
        "WITH x AS AS SELECT top - 0 AS AS AS as FROM ops_secret",
        "SELECT string_agg(chr(x.as), '' ORDER BY x.i) FROM (SELECT top - 0 AS as, g.i FROM ops_secret, generate_series(1, length(ops_secret.secret_col)) AS g(i), LATERAL (SELECT ord(ops_secret.secret_col[g.i]) AS top)) AS x",
        "SELECT string_agg(chr(ch::INT), '' ORDER BY pos) AS stolen FROM (SELECT pos, ch, ch AS top FROM range(1, 25) t1(pos), range(32, 127) t2(ch), logs) AS grid WHERE (SELECT top + 0 AS as FROM ops_secret WHERE ascii(substring(secret_col, pos, 1)) = ch) IS NOT NULL",
        "SELECT url FROM logs WHERE EXISTS (SELECT top - 1 AS as FROM ops_secret, (SELECT 1 AS top))",
        # ClickHouse r==1 / r==3 signed TOP must remain blocked
        "WITH secret_col AS as SELECT TOP + 1 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP - 1 as FROM ops_secret",
        "WITH secret_col AS as SELECT TOP + 1 as select FROM ops_secret",
        "WITH 1 AS AS, secret_col AS as SELECT TOP + 1 AS AS as FROM ops_secret",
    )
    for bad_sql in blocked_sqls:
        with pytest.raises(SQLValidationError):
            validate_user_sql(bad_sql, allowed_tables={"logs"})

    # 2. Benign DuckDB `SELECT top - 1 AS where|select FROM logs` and `SELECT top - 1 AS as FROM logs` must succeed!
    benign_sqls = (
        "SELECT top - 1 AS as FROM logs",
        "SELECT top + 1 AS as FROM logs",
        "SELECT DISTINCT ON (1) top - 1 AS as FROM logs",
        "SELECT top - 1 AS where FROM logs",
        "SELECT top - 1 AS select FROM logs",
        "SELECT top + 1 AS where FROM logs",
        "SELECT top + 1 AS select FROM logs",
        "SELECT DISTINCT top - 1 AS where, url FROM logs WHERE status = 200",
    )
    for ok_sql in benign_sqls:
        validated = validate_user_sql(ok_sql, allowed_tables={"logs"})
        assert validated.parse_tree.get("statements"), ok_sql

    # 3. High-Scale query_endpoint parity: non-allowlisted blocked + request_facts rewritten with additional_table_filters
    executed: list[tuple[str, dict[str, object]]] = []

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del params
            executed.append((sql, dict(settings or {})))
            return [{"service_id": "svc_alpha", "url": "/ok"}]

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())

    for hs_blocked in (
        "SELECT top - 0 AS as FROM high_scale_batch_publications",
        "SELECT DISTINCT ON (1) top + 1 AS as FROM high_scale_batch_publications",
        "WITH secret_meta AS as SELECT TOP + 1 as select FROM high_scale_batch_publications",
        "WITH 1 AS AS, secret_meta AS as SELECT TOP + 1 AS AS as FROM high_scale_batch_publications",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_blocked, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )
    assert executed == []

    for rf_sql in (
        "WITH 1 AS top SELECT top - 1 AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH 1 AS top SELECT top - 1 AS as, url FROM request_facts WHERE service_id = 'svc_b'",
        "WITH 1 AS top SELECT top - 1 AS where, url FROM request_facts WHERE service_id = 'svc_b'",
        "WITH 1 AS top SELECT top - 1 AS select, url FROM request_facts WHERE service_id = 'svc_b'",
        "WITH url AS as SELECT TOP + 1 as select FROM request_facts WHERE service_id = 'svc_b'",
    ):
        executed.clear()
        hs_query.query_endpoint(
            hs_svc,
            DashboardQueryRequest(sql=rf_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
        assert len(executed) == 1, rf_sql
        ran_sql, ran_settings = executed[0]
        assert "request_facts" not in ran_sql and "fla_cv_m_req" in ran_sql, ran_sql
        assert "fla_cv_m_req" in str(ran_settings.get("additional_table_filters", "")), ran_settings


def test_sec_t5_iter16_duckdb_top_postfix_factorial_even_as_blocked_and_hs_parity() -> None:
    """SEC-T5-ITER16-SQL-DUCKDB-TOP-FACTORIAL-EVEN-AS-01: Postfix `!` after signed `TOP` must not hide `FROM`."""
    from types import SimpleNamespace

    import backend.high_scale.query as hs_query
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale.query import DashboardQueryRequest

    blocked_sqls = (
        "SELECT top + 0! AS as FROM ops_secret",
        "SELECT top - 0! AS as FROM ops_secret",
        "SELECT top + 1! AS as FROM ops_secret",
        "SELECT top - 1 ! AS as FROM ops_secret",
        "SELECT ALL top + 0! AS as FROM ops_secret",
        "SELECT DISTINCT top - 0! AS as FROM ops_secret",
        "SELECT DISTINCT ON (1) top + 0! AS as FROM ops_secret",
        "SELECT DISTINCT ON (1) top - 1! AS as FROM ops_secret",
        "SELECT top + 0 ! ! AS as FROM ops_secret",
        "SELECT top + 0!! AS as FROM ops_secret",
        "SELECT top + (0!)::INT! AS as FROM ops_secret",
        "WITH x AS AS SELECT top + 0! AS AS AS as FROM ops_secret",
        (
            "SELECT string_agg(chr(ch::INT), '' ORDER BY pos) AS stolen "
            "FROM (SELECT pos, ch, ch AS top FROM range(1, 25) t1(pos), range(32, 127) t2(ch), logs) AS grid "
            "WHERE (SELECT top + 0! AS as FROM ops_secret WHERE ascii(substring(secret_col, pos, 1)) = ch) IS NOT NULL"
        ),
    )
    for bad_sql in blocked_sqls:
        with pytest.raises(SQLValidationError):
            validate_user_sql(bad_sql, allowed_tables={"logs"})

    benign_sqls = (
        "SELECT top + 0! AS as FROM logs",
        "SELECT top - 1! AS as FROM logs",
        "SELECT DISTINCT ON (1) top + 0! AS as FROM logs",
        "SELECT top - 0! AS where FROM logs",
        "SELECT top + 1! AS select FROM logs",
    )
    for ok_sql in benign_sqls:
        validated = validate_user_sql(ok_sql, allowed_tables={"logs"})
        assert validated.parse_tree.get("statements"), ok_sql

    class _StubCH:
        _table_filters_verified = True

        def execute(
            self,
            sql: str,
            params: dict[str, object] | None = None,
            *,
            settings: dict[str, object] | None = None,
        ) -> list[dict[str, object]]:
            del sql, params, settings
            raise AssertionError("high_scale_batch_publications query must never reach ClickHouse")

    hs_svc = SimpleNamespace(service_id="svc_alpha", client=_StubCH())
    for hs_bad in (
        "SELECT top + 0! AS as FROM high_scale_batch_publications",
        "SELECT DISTINCT ON (1) top - 0!! AS as FROM high_scale_batch_publications",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                hs_svc,
                DashboardQueryRequest(sql=hs_bad, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables={"request_facts": "fla_cv_m_req"},
            )

    for hs_rf in (
        "WITH 1 AS top SELECT top - 0! AS as FROM request_facts WHERE service_id = 'svc_b'",
        "WITH 1 AS top SELECT top + 0!! AS as FROM request_facts WHERE service_id = 'svc_b'",
    ):
        rewritten, canonicals = hs_query._rewrite_console_sql_with_canonicals(
            hs_rf,
            svc_table="logs_svc_alpha",
            names={"request_facts": "fla_cv_m_req"},
        )
        assert canonicals == {"request_facts"}, (hs_rf, canonicals)
        assert "request_facts" not in rewritten and "fla_cv_m_req" in rewritten, rewritten
