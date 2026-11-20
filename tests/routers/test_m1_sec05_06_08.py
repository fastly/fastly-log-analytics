"""M1 Security Regression Suite — SEC-05, SEC-06, and SEC-08."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

import backend.config as svcconfig
from backend.core import share_db
from backend.models.common.bootstrap_alerts import SavedViewRecord
from backend.routers import alerts as alerts_mod
from backend.routers import bootstrap as bootstrap_mod
from backend.routers import control_room as control_room_mod
from backend.routers import root_system as root_system_mod
from backend.routers.session_scoring import scoring_analytics as scoring_mod
from backend.utils import remote_access, tunnel
from backend.utils.auth import ANALYST_SESSION_COOKIE

pytestmark = pytest.mark.no_postgres


@pytest.fixture(autouse=True)
def _isolated_m1_state(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    monkeypatch.setattr(svcconfig, "DATA_DIR", data_dir)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("METRICS_SCRAPE_CIDRS", raising=False)
    con = sqlite3.connect(str(tmp_path / "share_sec05_08.db"), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        con.execute(ddl)
    con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    share_db.publish_tos_version("v1", "Test Terms of Service", con=con)
    tunnel.reset_for_tests()
    yield
    tunnel.reset_for_tests()
    con.close()


def _build_sec05_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(remote_access.RemoteAccessMiddleware)
    app.include_router(root_system_mod.router)
    app.include_router(bootstrap_mod.router)
    app.include_router(bootstrap_mod.debug_router)
    app.include_router(alerts_mod.views_router)
    return app


# ── SEC-05: /metrics scrape lockdown & K8s pod compatibility ────────────────


def test_sec05_metrics_blocks_proxied_and_analyst_requests_allows_pod_and_gateway(monkeypatch):
    app = _build_sec05_app()
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    secret = "g" * 40
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", secret)

    with TestClient(app) as client:
        # Proxied by Caddy or Public Gateway -> 403 admin_only
        r_caddy = client.get("/metrics", headers={"X-Proxied-By-Caddy": "true", "Host": "testserver"})
        assert r_caddy.status_code == 403
        assert r_caddy.json()["error"] == "admin_only"

        r_pub = client.get("/metrics", headers={"X-FLA-Public-Gateway": "true", "Host": "testserver"})
        assert r_pub.status_code == 403

        # Spoofed X-Forwarded-For -> 403
        r_xff = client.get("/metrics", headers={"X-Forwarded-For": "203.0.113.5", "Host": "testserver"})
        assert r_xff.status_code == 403

        # Authenticated admin-gateway -> 200 (even though middleware strips X-Admin-Gateway-Token!)
        r_gw = client.get("/metrics", headers={"X-Admin-Gateway-Token": secret, "Host": "admin.example.com"})
        assert r_gw.status_code == 200
        assert "text/plain" in r_gw.headers.get("content-type", "")

    # Direct K8s Prometheus pod IP scrape (10.42.1.15:8000, no proxy headers, ADMIN_GATEWAY_REQUIRED=1) -> 200
    with TestClient(app, client=("10.42.1.15", 51234), base_url="http://10.42.1.15:8000") as pod_client:
        r_pod = pod_client.get("/metrics")
        assert r_pod.status_code == 200

        # Same pod IP carrying an analyst session cookie -> 403!
        r_cookie = pod_client.get("/metrics", headers={"Cookie": f"{ANALYST_SESSION_COOKIE}=tok-analyst-123456"})
        assert r_cookie.status_code == 403

        # Explicit METRICS_SCRAPE_CIDRS narrows allowed pod IPs
        monkeypatch.setenv("METRICS_SCRAPE_CIDRS", "10.99.0.0/16")
        r_narrow_deny = pod_client.get("/metrics")
        assert r_narrow_deny.status_code == 403

    with TestClient(app, client=("10.99.2.10", 51234), base_url="http://10.99.2.10:8000") as prom_client:
        r_narrow_ok = prom_client.get("/metrics")
        assert r_narrow_ok.status_code == 200


# ── SEC-05: /api/bootstrap session binding, service projection & debug SQL ──


def test_sec05_bootstrap_enforces_fingerprint_ip_whitelist_and_sharing_active(monkeypatch):
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    inv = share_db.create_remote_invite(
        email="analyst@example.com",
        name="Analyst One",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        service_ids=["svc-a"],
        ip_whitelist="198.51.100.10/32",
        pii_policy={"mask_ips": True},
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(inv["id"], tos["version"])
    inv = share_db.get_remote_invite(inv["id"])
    assert inv is not None
    ua_headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/124.0.0.0"}
    sess = mgr.create_session(
        invite=inv,
        ip_address="198.51.100.10",
        user_agent=ua_headers["user-agent"],
        headers=ua_headers,
    )

    app = FastAPI()
    app.add_middleware(remote_access.RemoteAccessMiddleware)

    @app.get("/api/bootstrap")
    def fake_bootstrap(request: Request):
        s = getattr(request.state, "analyst_session", None)
        return {"authenticated": s is not None, "needs_login": s is None}

    # 1. Valid analyst request from whitelisted IP + matching UA -> 200 authenticated
    with TestClient(app, client=("198.51.100.10", 40000), base_url="https://share.example.com") as ok_client:
        ok_client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)
        r_ok = ok_client.get("/api/bootstrap", headers={"X-Proxied-By-Caddy": "true", **ua_headers})
        assert r_ok.status_code == 200
        assert r_ok.json()["authenticated"] is True

    # 2. Non-whitelisted IP -> 403 ip_not_whitelisted
    with TestClient(app, client=("203.0.113.77", 40000), base_url="https://share.example.com") as bad_ip_client:
        bad_ip_client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)
        r_ip = bad_ip_client.get("/api/bootstrap", headers={"X-Proxied-By-Caddy": "true", **ua_headers})
        assert r_ip.status_code == 403
        assert r_ip.json()["error"] == "ip_not_whitelisted"

    # 3. Live admin tightening of ip_whitelist blocks even current_ip == session.ip_address
    con = share_db.get_global_share_con()
    con.execute("UPDATE remote_invites SET ip_whitelist = ? WHERE id = ?", ("198.51.100.99/32", inv["id"]))
    con.commit()
    with TestClient(app, client=("198.51.100.10", 40000), base_url="https://share.example.com") as tightened_client:
        tightened_client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)
        r_tight = tightened_client.get("/api/bootstrap", headers={"X-Proxied-By-Caddy": "true", **ua_headers})
        assert r_tight.status_code == 403
        assert r_tight.json()["error"] == "ip_not_whitelisted"

    # Restore whitelist and verify UA fingerprint mismatch boots the session -> 401
    con.execute("UPDATE remote_invites SET ip_whitelist = ? WHERE id = ?", ("198.51.100.10/32", inv["id"]))
    con.commit()
    with TestClient(app, client=("198.51.100.10", 40000), base_url="https://share.example.com") as bad_fp_client:
        bad_fp_client.cookies.set(ANALYST_SESSION_COOKIE, sess.session_id)
        r_fp = bad_fp_client.get(
            "/api/bootstrap",
            headers={
                "X-Proxied-By-Caddy": "true",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Firefox/125.0",
            },
        )
        assert r_fp.status_code == 401
        assert r_fp.json()["error"] == "fingerprint_mismatch"
    assert mgr.validate_session(sess.session_id) is None


def test_sec05_bootstrap_projects_analyst_services_without_internal_secrets():
    raw_svc = {
        "service_id": "svc-a",
        "name": "Prod CDN",
        "is_active": True,
        "cmcd_enabled": True,
        "rum_enabled": True,
        "rum_log_period": 60,
        "fos_bucket": "secret-fos-bucket",
        "fos_region": "us-east",
        "cdn_url": "https://internal-origin.fastly.net",
        "cdn_service_id": "FASTLY123",
        "ngwaf_workspace_id": "ws-secret",
        "rum": {"cid_salt": "TOP_SECRET_SALT", "custom_condition": "req.url ~ '^/admin'"},
        "cron_stats": {"enabled": True},
        "status": {
            "schema": [{"name": "url", "type": "String"}, {"name": "cmcd_sid", "type": "String"}],
            "rum": {"internal_cursor": "secret"},
            "request": {"bucket_path": "s3://secret"},
        },
    }
    projected = bootstrap_mod._project_analyst_bootstrap_service(raw_svc)
    assert projected == {
        "service_id": "svc-a",
        "name": "Prod CDN",
        "is_active": True,
        "cmcd_enabled": True,
        "rum_enabled": True,
        "rum_log_period": 60,
        "status": {"schema": [{"name": "url", "type": "String"}, {"name": "cmcd_sid", "type": "String"}]},
    }
    for forbidden in (
        "fos_bucket",
        "fos_region",
        "cdn_url",
        "cdn_service_id",
        "ngwaf_workspace_id",
        "rum",
        "cron_stats",
    ):
        assert forbidden not in projected


def test_sec05_debug_router_blocks_remote_and_analyst_even_when_visibility_both():
    from backend.core.share_db.settings import set_setting

    set_setting("query_debug_visibility", "both")
    app = FastAPI()
    app.include_router(bootstrap_mod.debug_router)

    with TestClient(app) as client:
        # Local admin -> 200
        assert client.get("/api/debug/recent-sqlite").status_code == 200
        assert client.get("/api/debug/state").status_code == 200

        # Remote caller (via proxy header) without middleware -> still 403 admin_only!
        for ep in (
            "/api/debug/recent-sqlite",
            "/api/debug/recent-postgres",
            "/api/debug/state",
            "/api/debug/page-telemetry",
        ):
            r = client.get(ep, headers={"X-Proxied-By-Caddy": "true"})
            assert r.status_code == 403, ep
            assert r.json()["detail"]["error"] == "admin_only"


# ── SEC-06: Historical/future analyst window clamp on live Control Room ─────


def test_sec06_control_room_live_endpoints_reject_historical_and_future_analyst_windows(monkeypatch):
    past_end = (datetime.now(UTC) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    past_start = (datetime.now(UTC) - timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    future_start = (datetime.now(UTC) + timedelta(hours=2)).isoformat().replace("+00:00", "Z")

    historical_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=0,
        query_start_time=past_start,
        query_end_time=past_end,
        pii_policy={"mask_ips": True},
    )
    future_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=0,
        query_start_time=future_start,
        query_end_time=None,
        pii_policy={"mask_ips": True},
    )
    rolling_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=24,
        query_start_time=None,
        query_end_time=None,
        pii_policy={"mask_ips": True},
    )

    def _req(sess):
        return SimpleNamespace(
            state=SimpleNamespace(analyst_session=sess, is_remote=sess is not None),
            headers={},
            query_params={},
            path_params={"service_id": "svc-a"},
        )

    # Historical window -> 403 time_window_expired across all 3 live handlers
    import asyncio

    with pytest.raises(HTTPException) as exc_seed:
        asyncio.run(control_room_mod.realtime_seed(request=_req(historical_sess), service_id="svc-a", _access="svc-a"))
    assert exc_seed.value.status_code == 403
    assert exc_seed.value.detail["error"] == "time_window_expired"

    with pytest.raises(HTTPException) as exc_stream:
        import asyncio

        asyncio.run(
            control_room_mod.realtime_stream(request=_req(historical_sess), service_id="svc-a", _access="svc-a")
        )
    assert exc_stream.value.status_code == 403
    assert exc_stream.value.detail["error"] == "time_window_expired"

    with pytest.raises(HTTPException) as exc_err:
        import asyncio

        asyncio.run(
            control_room_mod.control_room_error_stream(
                request=_req(historical_sess), service_id="svc-a", _access="svc-a"
            )
        )
    assert exc_err.value.status_code == 403
    assert exc_err.value.detail["error"] == "time_window_expired"

    # Future window -> 403 time_window_future
    with pytest.raises(HTTPException) as exc_fut:
        control_room_mod._enforce_live_stream_window(_req(future_sess))
    assert exc_fut.value.status_code == 403
    assert exc_fut.value.detail["error"] == "time_window_future"

    # Rolling window analyst & admin pass without false-positive microsecond race
    control_room_mod._enforce_live_stream_window(_req(rolling_sess))
    control_room_mod._enforce_live_stream_window(_req(None))


# ── SEC-08: Scoring session events gate, top-flagged masking & saved views ──


def test_sec08_scoring_session_events_and_top_flagged_enforce_pii_policy(monkeypatch):
    masked_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=24,
        query_start_time=None,
        query_end_time=None,
        pii_policy={"mask_ips": True},
    )
    unmasked_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=24,
        query_start_time=None,
        query_end_time=None,
        pii_policy={"mask_ips": False},
    )

    def _req(sess):
        return SimpleNamespace(state=SimpleNamespace(analyst_session=sess), headers={}, query_params={})

    monkeypatch.setattr(scoring_mod, "_enforce_scoring_service_scope", lambda req, sid: None)
    monkeypatch.setattr(
        scoring_mod,
        "_fetch_session_events",
        lambda sid, sids, since_days=30, limit_per_sid=500, *, ts_predicate=None: {
            sids[0]: [{"ts": "2026-11-19T00:00:00Z", "ip": "203.0.113.9", "url": "/checkout", "status": 200}]
        },
    )

    # Masked analyst -> 403 pii_policy_violation on scoring_session_events
    with pytest.raises(HTTPException) as exc_masked:
        scoring_mod.scoring_session_events(
            request=_req(masked_sess), service_id="svc-a", sid="deadbeef1234", since_days=7
        )
    assert exc_masked.value.status_code == 403
    assert exc_masked.value.detail["error"] == "pii_policy_violation"

    # Unmasked analyst -> 200
    res_unmasked = scoring_mod.scoring_session_events(
        request=_req(unmasked_sess), service_id="svc-a", sid="deadbeef1234", since_days=7
    )
    assert res_unmasked["event_count"] == 1

    # Direct call to scoring_top_flagged masks IP and redacts edge_sid when mask_ips=True
    scoring_mod._bust_analytics_cache("svc-a")
    monkeypatch.setattr(
        scoring_mod,
        "_prepare_scoring_query",
        lambda req, sid, hrs: ("request_facts", {"edge_score"}, "1=1", "disc"),
    )
    monkeypatch.setattr(
        scoring_mod,
        "_top_flagged_rows",
        lambda sid, src, pred, lim: [
            {"timestamp": "2026-11-19T00:00:00Z", "ip": "203.0.113.42", "edge_sid": "abcdef123456", "edge_score": 95}
        ],
    )
    flagged = scoring_mod.scoring_top_flagged(request=_req(masked_sess), service_id="svc-a", since_hours=24, limit=10)
    assert flagged["rows"][0]["ip"] != "203.0.113.42"
    assert flagged["rows"][0]["edge_sid"] == "[redacted]"


@pytest.mark.parametrize(
    "pii_key",
    sorted(share_db.SAVED_VIEW_PII_FILTER_KEYS),
)
def test_sec08_saved_views_hide_all_18_pii_filter_keys_when_mask_ips_true(pii_key):
    assert len(share_db.SAVED_VIEW_PII_FILTER_KEYS) == 18
    views = [
        SavedViewRecord(
            id="v-pii-list",
            service_id="svc-a",
            name="PII List View",
            filters_json=f'[{{"field": "{pii_key}", "op": "eq", "value": "secret"}}]',
        ),
        SavedViewRecord(
            id="v-pii-dict",
            service_id="svc-a",
            name="PII Dict View",
            filters_json=f'{{"filters": {{"{pii_key}": ["secret"]}}, "_view_extras": {{}}}}',
        ),
        SavedViewRecord(
            id="v-malformed",
            service_id="svc-a",
            name="Broken JSON",
            filters_json="{not-valid-json",
        ),
        SavedViewRecord(
            id="v-safe",
            service_id="svc-a",
            name="Safe Country View",
            filters_json='[{"field": "country", "op": "eq", "value": "US"}]',
        ),
    ]
    filtered = share_db.filter_saved_views_for_pii(views, {"mask_ips": True})
    assert [v.id for v in filtered] == ["v-safe"]

    # When mask_ips=False, all views are returned
    unfiltered = share_db.filter_saved_views_for_pii(views, {"mask_ips": False})
    assert len(unfiltered) == 4


def test_sec08_views_router_filters_for_masked_analyst_and_blocks_analyst_mutations(monkeypatch):
    raw_views = [
        {
            "id": "v-ip",
            "service_id": "svc-a",
            "name": "Admin IP Trace",
            "filters_json": '[{"field": "client_ip", "value": "198.51.100.7"}]',
        },
        {
            "id": "v-ok",
            "service_id": "svc-a",
            "name": "5xx Status",
            "filters_json": '[{"field": "status", "value": "500"}]',
        },
    ]
    monkeypatch.setattr(alerts_mod.repo, "get_views", lambda sid: list(raw_views))

    analyst_req = SimpleNamespace(
        state=SimpleNamespace(
            analyst_session=SimpleNamespace(service_ids=["svc-a"], pii_policy={"mask_ips": True}),
            is_remote=True,
        ),
        headers={},
    )
    out = alerts_mod.list_views("svc-a", analyst_req, limit=100)
    assert [v.id for v in out] == ["v-ok"]

    # Admin sees both
    admin_req = SimpleNamespace(state=SimpleNamespace(analyst_session=None, is_remote=False), headers={})
    out_admin = alerts_mod.list_views("svc-a", admin_req, limit=100)
    assert [v.id for v in out_admin] == ["v-ip", "v-ok"]

    # Live dashboard filter check allows "oip" (out-of-scope contract) while blocking "c_ip" and "edge_sid"
    assert "oip" not in remote_access._EXTRA_FORBIDDEN_FILTER_COLS
    assert "c_ip" in remote_access._EXTRA_FORBIDDEN_FILTER_COLS
    assert "edge_sid" in remote_access._PII_FORBIDDEN_FILTER_COLS


# ── Iteration 2 Hardening Regressions (V4, C1, C2, C3, F2, F6) ───────────────


@pytest.mark.security_regression
def test_sec08_shared_pii_keys_and_saved_view_bypass_matrix() -> None:
    assert len(share_db.SHARED_PII_KEYS) == 18
    assert callable(share_db.saved_view_contains_pii)

    safe_dict_view = {"id": "safe-dict", "filters": {"pop": ["SJC"]}}
    safe_record = SavedViewRecord(
        id="safe-rec",
        service_id="svc-a",
        name="Safe Record",
        filters_json='[{"field": "status", "op": "eq", "value": "200"}]',
    )
    bypass_views = [
        {"id": "bad-malformed", "filters": "{not-valid-json"},
        {"id": "bypass-query-dict", "query": '[{"field": "c_ip", "op": "eq", "value": "1.2.3.4"}]'},
        SimpleNamespace(
            id="bypass-query-attr",
            filters_json="[]",
            query='{"filters": {"true-client-ip": ["1.2.3.4"]}}',
        ),
        {"id": "bypass-nested-query", "filters": {"query": '[{"field": "edge_sid", "value": "s1"}]'}},
        {"id": "bypass-no-op-value", "filters": [{"field": "client_ip", "eq": "198.51.100.4"}]},
        {"id": "bypass-multi-key", "filters": [{"field": "country", "column": "session_id", "value": "US"}]},
        {"id": "bypass-custom-fields", "filters": [{"field": "custom_fields['oip']", "value": "151.101.1.9"}]},
    ]
    kept = share_db.filter_saved_views_for_pii([safe_dict_view, safe_record, *bypass_views], {"mask_ips": True})
    kept_ids = [v["id"] if isinstance(v, dict) else v.id for v in kept]
    assert kept_ids == ["safe-dict", "safe-rec"]

    for bv in bypass_views:
        assert share_db.saved_view_contains_pii(bv) is True
    assert share_db.saved_view_contains_pii(safe_dict_view) is False

    # apply_pii_policy masks oip and clears top_ips when mask_ips=True
    masked = share_db.apply_pii_policy(
        {"oip": "151.101.1.51", "top_ips": ["203.0.113.9", "198.51.100.4"], "edge_sid": "sid-secret"},
        {"mask_ips": True},
    )
    assert masked["oip"] == "151.101.1.xxx"
    assert masked["top_ips"] == []
    assert masked["edge_sid"] == "[redacted]"


@pytest.mark.security_regression
def test_sec05_sec06_control_room_seed_sse_tick_expiry_and_error_scrubbing(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio
    import json

    from backend.core.realtime.publisher import publisher as rt_publisher

    from backend.core.realtime import poller as rt_poller

    masked_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=24,
        query_start_time=None,
        query_end_time=(datetime.now(UTC) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        pii_policy={"mask_ips": True},
    )
    req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=masked_sess, is_remote=True),
        headers={},
        query_params={},
        path_params={"service_id": "svc-a"},
        is_disconnected=lambda: asyncio.sleep(0, result=False),
    )

    raw_tick = {
        "timestamp": "2026-11-19T00:00:00Z",
        "top_ips": ["203.0.113.9"],
        "oip": "198.51.100.4",
        "edge_sid": "s-secret",
        "requests": 42,
    }
    monkeypatch.setattr(rt_publisher, "get_recent_ticks", lambda sid, n=60: [dict(raw_tick)] * 60)
    monkeypatch.setattr(rt_poller, "ensure_polling", lambda sid: None)

    seed_res = asyncio.run(control_room_mod.realtime_seed(request=req, service_id="svc-a", _access="svc-a"))
    assert seed_res.ticks[0]["top_ips"] == []
    assert seed_res.ticks[0]["oip"] == "198.51.100.xxx"
    assert seed_res.ticks[0]["edge_sid"] == "[redacted]"

    # realtime_stream SSE generator masks tick 1 and stops before tick 2 when query_end_time expires mid-stream
    async def _fake_subscribe(_sid: str):
        yield dict(raw_tick)
        masked_sess.query_end_time = (datetime.now(UTC) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        yield {"timestamp": "2026-11-19T00:00:01Z", "top_ips": ["192.0.2.9"], "requests": 99}

    monkeypatch.setattr(rt_publisher, "subscribe", _fake_subscribe)

    async def _collect_sse() -> list[dict]:
        resp = await control_room_mod.realtime_stream(request=req, service_id="svc-a", _access="svc-a")
        collected: list[dict] = []
        async for item in resp.body_iterator:
            text = (item.decode("utf-8") if isinstance(item, bytes) else str(item)).strip()
            if not text:
                continue
            if text.startswith("data:"):
                text = text[5:].strip()
            collected.append(json.loads(text))
        return collected

    emitted = asyncio.run(_collect_sse())
    assert len(emitted) == 1
    assert emitted[0]["top_ips"] == []
    assert emitted[0]["oip"] == "198.51.100.xxx"
    assert emitted[0]["edge_sid"] == "[redacted]"

    # _mask_error_events handles missing client_ip, masks oip/edge_sid, scrubs URL IPs, and preserves UA versions
    events = [
        {
            "event_id": "ev-1",
            "oip": "151.101.1.44",
            "edge_sid": "sid-999",
            "url": "/cb?ip=198.51.100.44&v6=2001:db8::abcd",
            "ua": "Mozilla/5.0 Chrome/124.0.6367.91 Safari/537.36",
        }
    ]
    masked_events = control_room_mod._mask_error_events(events, req)
    assert masked_events[0]["oip"] == "151.101.1.xxx"
    assert masked_events[0]["edge_sid"] == "[redacted]"
    assert "198.51.100.44" not in masked_events[0]["url"]
    assert "198.51.100.xxx" in masked_events[0]["url"]
    assert "2001:db8::abcd" not in masked_events[0]["url"]
    assert masked_events[0]["ua"] == "Mozilla/5.0 Chrome/124.0.6367.91 Safari/537.36"


@pytest.mark.security_regression
def test_sec05_sec06_control_room_correlate_pii_window_and_bucket_merging(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    from backend.models.common.provision import CorrelateRequest

    recent_start = (datetime.now(UTC) - timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
    masked_sess = SimpleNamespace(
        service_ids=["svc-a"],
        query_window_hours=None,
        time_window_hours=1,
        query_start_time=recent_start,
        query_end_time=None,
        pii_policy={"mask_ips": True},
    )
    req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=masked_sess, is_remote=True),
        headers={},
        query_params={},
        path_params={"service_id": "svc-a"},
    )

    captured_params: dict = {}

    def _fake_execute(_sql: str, params: dict):
        captured_params.update(params)
        return [
            {"value": "151.101.1.10", "aggregate_count": 3},
            {"value": "151.101.1.20", "aggregate_count": 5},
        ]

    fake_hs = SimpleNamespace(
        service_id="svc-a",
        client=SimpleNamespace(execute=_fake_execute),
        watermark_for=lambda _dom: SimpleNamespace(
            validate=lambda: None,
            coverage_end=datetime.now(UTC),
        ),
    )
    fake_reg = SimpleNamespace(resolve=lambda sid: fake_hs if sid == "svc-a" else None)

    # 1. Direct handler rejects session-ID dimensions when mask_ips=True
    for sess_dim in ("cookie_session", "edge_sid"):
        with pytest.raises(HTTPException) as exc_sid:
            asyncio.run(
                control_room_mod.control_room_correlate(
                    body=CorrelateRequest(dimension=sess_dim, window_minutes=30, limit=10),
                    service_id="svc-a",
                    request=req,
                    _access="svc-a",
                    registry=fake_reg,
                )
            )
        assert exc_sid.value.status_code == 403
        assert exc_sid.value.detail["error"] == "pii_policy_violation"

    # 2. Correlating by oip clamps start to query_start_time AND merges /24 masked buckets!
    corr = asyncio.run(
        control_room_mod.control_room_correlate(
            body=CorrelateRequest(dimension="oip", window_minutes=60, limit=10),
            service_id="svc-a",
            request=req,
            _access="svc-a",
            registry=fake_reg,
        )
    )
    assert corr["top"] == [{"value": "151.101.1.xxx", "count": 8}]
    assert captured_params["start"] >= datetime.now(UTC) - timedelta(minutes=3)

    # 3. Real HTTP TestClient through RemoteAccessMiddleware blocks dimension="cookie_session" / "ip" -> 403
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid} if sid == "svc-a" else None)
    inv = share_db.create_remote_invite(
        email="corr@example.com",
        name="Correlate Analyst",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svc-a"],
        pii_policy={"mask_ips": True},
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(inv["id"], tos["version"])
    inv_row = share_db.get_remote_invite(inv["id"])
    assert inv_row is not None
    ua_str = "Mozilla/5.0 Chrome/125.0"
    http_sess = mgr.create_session(
        invite=inv_row, ip_address="198.51.100.20", user_agent=ua_str, headers={"user-agent": ua_str}
    )
    http_app = FastAPI()

    @http_app.post("/api/origin/drilldown")
    def _dummy_drilldown() -> dict[str, bool]:
        return {"ok": True}

    http_app.add_middleware(remote_access.RemoteAccessMiddleware)
    http_app.include_router(control_room_mod.router)
    req_hdrs = {
        "X-Proxied-By-Caddy": "true",
        "Origin": "https://share.example.com",
        "User-Agent": ua_str,
    }
    with TestClient(http_app, client=("198.51.100.20", 50000), base_url="https://share.example.com") as hc:
        hc.cookies.set(ANALYST_SESSION_COOKIE, http_sess.session_id)
        for blocked_dim in ("cookie_session", "edge_sid", "ip"):
            r_dim = hc.post(
                "/api/origin/drilldown",
                json={"service_id": "svc-a", "dimension": blocked_dim},
                headers=req_hdrs,
            )
            assert r_dim.status_code == 403, f"{blocked_dim}: {r_dim.status_code} {r_dim.text}"
            assert r_dim.json()["error"] == "pii_policy_violation"
        r_oip_ok = hc.post(
            "/api/origin/drilldown",
            json={"service_id": "svc-a", "dimension": "oip"},
            headers=req_hdrs,
        )
        assert r_oip_ok.status_code == 200
        r_corr_gate = hc.post(
            "/api/services/svc-a/control-room/correlate",
            json={"dimension": "cookie_session", "window_minutes": 5, "limit": 5},
            headers=req_hdrs,
        )
        assert r_corr_gate.status_code == 403
        assert r_corr_gate.json()["error"] == "read_only"


@pytest.mark.security_regression
def test_sec08_dashboard_raw_csv_masks_18_pii_columns_and_scrubs_embedded_ips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pandas as pd

    from backend.core import request_context
    from backend.routers import dashboard as dashboard_mod

    df = pd.DataFrame(
        [
            {
                "timestamp": "2026-11-19T00:00:00Z",
                "ip": "203.0.113.7",
                "oip": "151.101.1.51",
                "c_ip": "198.51.100.88",
                "X-Forwarded-For": "198.51.100.4, 203.0.113.9",
                "custom_fields.edge_sid": "sid-raw-secret",
                "url": "/callback?client=198.51.100.44",
                "ua": "Mozilla/5.0 (Macintosh) Chrome/131.0.6778.85 Safari/537.36",
            }
        ]
    )
    monkeypatch.setattr(dashboard_mod.hs_dashboard, "raw_df", lambda *a, **kw: df)
    masked_ctx = SimpleNamespace(
        clamp=lambda s, e: (s, e),
        analyst_session=SimpleNamespace(service_ids=["svc-a"], pii_policy={"mask_ips": True}),
        service_id="svc-a",
        high_scale_service=SimpleNamespace(service_id="svc-a"),
    )
    monkeypatch.setattr(dashboard_mod, "require_high_scale", lambda ctx: ctx.high_scale_service)

    app = FastAPI()
    app.dependency_overrides[request_context.build_request_context] = lambda: masked_ctx
    app.include_router(dashboard_mod.router)

    with TestClient(app) as client:
        resp = client.post("/api/dashboard/raw/csv", json={})
        assert resp.status_code == 200
        body = resp.text
        assert "203.0.113.7" not in body and "203.0.113.xxx" in body
        assert "151.101.1.51" not in body and "151.101.1.xxx" in body
        assert "198.51.100.88" not in body and "198.51.100.xxx" in body
        assert "198.51.100.xxx, 203.0.113.xxx" in body
        assert "sid-raw-secret" not in body and "[redacted]" in body
        assert "198.51.100.44" not in body and "/callback?client=198.51.100.xxx" in body
        assert "Chrome/131.0.6778.85" in body
