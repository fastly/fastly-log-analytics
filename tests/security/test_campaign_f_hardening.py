"""Campaign F security hardening regression tests.

Covers:
- AUTH-01 (Medium): Fingerprint & IP-whitelist request binding enforced on
  /api/share/tos, /api/share/acknowledge, and /api/share/heartbeat, and
  TOS_ACCEPTED audited with client_ip(request).
- AUTH-02 (Low): OAuth oauth_flow_state AEAD cookie enforces server-side _exp.
- AUTH-03 (Low): POST /api/share/claim/{token} rejects expired invites with 410.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import oauth as oauth_mod
from backend.core import share_db
from backend.routers.share_admin import sessions_and_tos as st_mod
from backend.utils import tunnel

pytestmark = pytest.mark.no_postgres


@pytest.fixture()
def _isolated_share_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    con = sqlite3.connect(str(tmp_path / "share_f.db"), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        con.execute(ddl)
    con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    share_db.publish_tos_version("v1", "Campaign F Terms", con=con)
    tunnel.reset_for_tests()
    try:
        yield con
    finally:
        tunnel.reset_for_tests()
        con.close()


def test_auth01_share_endpoints_enforce_fingerprint_and_ip_whitelist(_isolated_share_db: sqlite3.Connection) -> None:
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    invite = share_db.create_remote_invite(
        name="Analyst F",
        email="f@example.com",
        passcode="cedar-meadow-lantern-88",
        expires_at_utc=None,
        ip_whitelist="198.51.100.10/32",
        service_ids=["svc-f"],
    )
    inv_row = share_db.get_remote_invite(invite["id"])
    assert inv_row is not None

    ua_legit = "Mozilla/5.0 Chrome/125.0"
    ua_attacker = "Mozilla/5.0 Firefox/130.0"

    app = FastAPI()
    app.include_router(st_mod.share_auth_router)

    # 1. Pending ToS session: non-whitelisted IP rejected with 403 on /tos and /acknowledge.
    pending_sess = mgr.create_session(
        invite=inv_row,
        ip_address="198.51.100.10",
        user_agent=ua_legit,
        headers={"user-agent": ua_legit},
    )
    pending_sess.tos_pending = True
    with TestClient(app, client=("203.0.113.99", 50000), base_url="https://share.example.com") as bad_ip_client:
        hdrs = {"User-Agent": ua_legit, "Cookie": f"analyst_pending_session_id={pending_sess.session_id}"}
        assert bad_ip_client.get("/api/share/tos", headers=hdrs).status_code == 403
        assert bad_ip_client.post("/api/share/acknowledge", json={"version": "v1"}, headers=hdrs).status_code == 403

    # 2. Legitimate IP + fingerprint succeeds on /tos and /acknowledge (rotates to active session).
    with TestClient(app, client=("198.51.100.10", 50000), base_url="https://share.example.com") as legit_ip_client:
        ok_hdrs = {"User-Agent": ua_legit, "Cookie": f"analyst_pending_session_id={pending_sess.session_id}"}
        assert legit_ip_client.get("/api/share/tos", headers=ok_hdrs).status_code == 200
        ack = legit_ip_client.post("/api/share/acknowledge", json={"version": "v1"}, headers=ok_hdrs)
        assert ack.status_code == 200
        rotated_sid = ack.cookies.get("analyst_session_id")
        assert rotated_sid

        # 3. Fingerprint mismatch on /heartbeat evicts the active session and returns 401.
        bad_fp_hdrs = {
            "User-Agent": ua_attacker,
            "X-User-Active": "1",
            "Cookie": f"analyst_session_id={rotated_sid}",
        }
        hb = legit_ip_client.get("/api/share/heartbeat", headers=bad_fp_hdrs)
        assert hb.status_code == 401, hb.text
        assert mgr.validate_session(rotated_sid) is None


def test_auth02_oauth_flow_state_enforces_server_side_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "a" * 32)
    payload = {"provider": "google", "state": "s1", "nonce": "n1", "verifier": "v1"}
    sealed = oauth_mod.seal_flow_state(payload)
    assert oauth_mod.open_flow_state(sealed) == payload

    expired = oauth_mod.seal_flow_state(payload, max_age_s=-10)
    with pytest.raises(oauth_mod.FlowStateError, match="expired"):
        oauth_mod.open_flow_state(expired)


def test_auth03_share_claim_rejects_expired_invite(_isolated_share_db: sqlite3.Connection) -> None:
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    invite = share_db.create_remote_invite(
        name="Expired Analyst",
        email="expired@example.com",
        passcode="birch-creek-lantern-11",
        expires_at_utc="2020-01-01T00:00:00Z",
        ip_whitelist=None,
        service_ids=["svc-f"],
    )
    tok = share_db.create_claim_token(invite["id"])

    app = FastAPI()
    app.include_router(st_mod.share_auth_router)
    with TestClient(app, client=("198.51.100.10", 50000), base_url="https://share.example.com") as client:
        resp = client.post(f"/api/share/claim/{tok}")
        assert resp.status_code == 410
        assert resp.json()["detail"]["error"] == "invite_gone"
