"""Unit tests for the TunnelManager: fingerprint, rate limiter, session
lifecycle, multi-device boot, validate_session timeouts, persistence
round-trip via share_db, panic, and start_sharing input validation.

The SSH-to-localhost.run code path was removed in v2.0 — only direct-mode
(HTTPS public_endpoint) is exercised here.
"""

from __future__ import annotations

import time

import pytest

from backend.core import share_db
from backend.utils import tunnel

# ── Fingerprint ─────────────────────────────────────────────────────────────


def test_fingerprint_stable_across_minor_ua_updates():
    """A patch-level Chrome update should NOT change the fingerprint."""
    base = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Chrome/126.0.6478.127 Safari/537.36",
        "sec-ch-ua-platform": '"macOS"',
    }
    patched = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Chrome/126.0.6478.200 Safari/537.36",
        "sec-ch-ua-platform": '"macOS"',
    }
    assert tunnel.compute_fingerprint(base) == tunnel.compute_fingerprint(patched)


def test_fingerprint_changes_with_major_browser_bump():
    a = tunnel.compute_fingerprint({"user-agent": "Chrome/126.0.0.0", "sec-ch-ua-platform": '"macOS"'})
    b = tunnel.compute_fingerprint({"user-agent": "Chrome/127.0.0.0", "sec-ch-ua-platform": '"macOS"'})
    assert a != b


def test_fingerprint_changes_with_os():
    a = tunnel.compute_fingerprint({"user-agent": "Chrome/126 Macintosh", "sec-ch-ua-platform": '"macOS"'})
    b = tunnel.compute_fingerprint({"user-agent": "Chrome/126 Windows NT 10.0", "sec-ch-ua-platform": '"Windows"'})
    assert a != b


# ── Rate limiter ────────────────────────────────────────────────────────────


def test_rate_limiter_locks_after_threshold():
    rl = tunnel._LoginRateLimiter()
    # Below threshold = no lock.
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        triggered = rl.record_failure("1.2.3.4")
        assert not triggered
    locked, _ = rl.is_locked("1.2.3.4")
    assert not locked
    # Threshold-th failure triggers.
    assert rl.record_failure("1.2.3.4") is True
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked
    assert 0 < remaining <= tunnel.LOGIN_LOCKOUT_S


def test_rate_limiter_failures_persist_across_lockout():
    """Per-IP failure counters must NOT be clearable by a successful login —
    otherwise an attacker with valid credentials can interleave brute-force
    guesses with valid logins to keep the counter below threshold indefinitely
    (finding 016). The clear() method was removed."""
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.2.3.4")
    locked, _ = rl.is_locked("1.2.3.4")
    assert locked
    assert not hasattr(rl, "clear"), "clear() method must not exist (finding 016)"


def test_rate_limiter_per_ip_isolation():
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.2.3.4")
    assert rl.is_locked("1.2.3.4")[0]
    assert not rl.is_locked("5.6.7.8")[0]


# ── Session lifecycle ──────────────────────────────────────────────────────


def _seed_invite(service_ids=None, *, allow_concurrent_sessions=False) -> dict:
    return share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=service_ids or ["svcA"],
        allow_concurrent_sessions=allow_concurrent_sessions,
    )


def test_create_session_persists_and_returns_fingerprint():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X", "sec-ch-ua-platform": '"macOS"'},
    )
    assert session.session_id
    assert session.email == "drew@example.com"
    assert session.service_ids == ["svcA"]
    assert session.fingerprint_signature
    # Persisted.
    rows = share_db.get_all_sessions()
    assert len(rows) == 1


@pytest.mark.security_regression
def test_create_session_cap_raises_when_at_capacity():
    mgr = tunnel.get_tunnel_manager()
    invite_a = _seed_invite(allow_concurrent_sessions=True)
    invite_b = share_db.create_remote_invite(
        name="Bea",
        email="bea@example.com",
        passcode="seagull-mountain-river-77",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        allow_concurrent_sessions=True,
    )
    mgr.create_session(invite=invite_a, ip_address="1.2.3.4", user_agent="ua", headers={}, cap=1)
    with pytest.raises(tunnel.TunnelCapacityError) as exc_info:
        mgr.create_session(invite=invite_b, ip_address="5.6.7.8", user_agent="ua", headers={}, cap=1)
    assert exc_info.value.current == 1
    assert exc_info.value.cap == 1
    # The rejected attempt must not have been inserted.
    assert mgr.active_session_count() == 1


def test_create_session_cap_none_preserves_uncapped_behavior():
    """Callers that don't pass ``cap`` (or pass None) keep the old
    unconditional-insert behavior — cap is opt-in, not a breaking change."""
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(allow_concurrent_sessions=True)
    for i in range(5):
        mgr.create_session(invite=invite, ip_address=f"1.2.3.{i}", user_agent="ua", headers={})
    assert mgr.active_session_count() == 5


@pytest.mark.security_regression
def test_create_session_cap_is_atomic_under_concurrent_logins():
    """Regression for finding 016: concurrent logins racing the capacity
    check must not both observe count < cap and both insert. Before the
    fix, active_session_count() and create_session() were two separate
    lock acquisitions in the caller (share_auth.py / share_oauth.py); now
    the check-and-insert happens under ONE acquisition inside
    create_session(cap=...), so the cap is never transiently exceeded no
    matter how many threads race it."""
    import concurrent.futures

    mgr = tunnel.get_tunnel_manager()
    cap = 3
    n_workers = 20
    invites = [
        share_db.create_remote_invite(
            name=f"Analyst{i}",
            email=f"analyst{i}@example.com",
            passcode="ocean-breeze-cabin-42",
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=["svcA"],
            allow_concurrent_sessions=True,
        )
        for i in range(n_workers)
    ]

    def attempt(invite):
        try:
            mgr.create_session(invite=invite, ip_address="1.2.3.4", user_agent="ua", headers={}, cap=cap)
            return "ok"
        except tunnel.TunnelCapacityError:
            return "rejected"

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        outcomes = list(pool.map(attempt, invites))

    assert outcomes.count("ok") == cap
    assert outcomes.count("rejected") == n_workers - cap
    assert mgr.active_session_count() == cap


def test_validate_session_returns_session_when_fresh():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    assert mgr.validate_session(session.session_id) is not None


def test_validate_session_idle_timeout_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Force last_active to far in the past.
    session.last_active_time = "2020-01-01T00:00:00Z"
    assert mgr.validate_session(session.session_id) is None
    # Audit row written.
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SESSION_TIMEOUT" for a in audits)


def test_validate_session_absolute_lifetime_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Login >24h ago.
    session.login_time = "2020-01-01T00:00:00Z"
    session.last_active_time = "2020-01-01T00:00:00Z"
    assert mgr.validate_session(session.session_id) is None


def test_validate_session_revoked_invite_evicts():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    share_db.revoke_remote_invite(invite["id"])
    assert mgr.validate_session(session.session_id) is None


def test_multi_device_boot():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    first = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    second = mgr.create_session(
        invite=invite,
        ip_address="5.6.7.8",
        user_agent="Firefox",
        headers={"user-agent": "Firefox/120 Windows"},
    )
    assert mgr.get_session(first.session_id) is None
    assert mgr.get_session(second.session_id) is not None
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SESSION_BOOT" for a in audits)


def test_shared_invite_allows_concurrent_sessions():
    """An invite opted into shared logins keeps both sessions alive instead of
    booting the first — and writes no SESSION_BOOT audit row."""
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(allow_concurrent_sessions=True)
    first = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    second = mgr.create_session(
        invite=invite,
        ip_address="5.6.7.8",
        user_agent="Firefox",
        headers={"user-agent": "Firefox/120 Windows"},
    )
    # Both sessions coexist under the same invite.
    assert mgr.get_session(first.session_id) is not None
    assert mgr.get_session(second.session_id) is not None
    assert first.session_id != second.session_id
    assert mgr.active_session_count() == 2
    # No boot audit event was written for the shared invite.
    audits = share_db.get_share_audit_logs()
    assert not any(a["event_type"] == "SESSION_BOOT" for a in audits)


def test_touch_session_bumps_last_active_and_ip():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    original = "2026-05-31T20:00:00Z"
    session.last_active_time = original
    # Force forward without real-time sleep
    mgr.touch_session(session.session_id, last_activity="GET /api/bootstrap", new_ip="5.6.7.8")
    assert session.last_activity == "GET /api/bootstrap"
    assert session.ip_address == "5.6.7.8"
    assert session.last_active_time > original


def test_touch_session_ip_roam_does_not_bump_idle_clock():
    """bump_active=False updates the roamed IP WITHOUT resetting the idle clock.

    The IP-roaming touch fires on every request from a new egress IP — on a
    rotating-NAT proxy that's nearly every request, so bumping last_active_time
    there would pin the session alive forever and bypass the X-User-Active idle
    gate. The IP must still update (whitelist re-validation); the clock must not.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    original = "2026-05-31T20:00:00Z"
    session.last_active_time = original
    mgr.touch_session(session.session_id, new_ip="9.9.9.9", bump_active=False)
    assert session.ip_address == "9.9.9.9", "roamed IP must still update"
    assert session.last_active_time == original, "IP roam must NOT reset the idle clock"


def test_boot_sessions_for_invite_counts_correctly():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    s = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    n = mgr.boot_sessions_for_invite(invite["id"], reason="revoked")
    assert n == 1
    assert mgr.get_session(s.session_id) is None


def test_rehydrate_after_restart_keeps_fresh_sessions():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    s = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Drop the singleton (simulates uvicorn restart).
    tunnel.reset_for_tests()
    mgr2 = tunnel.get_tunnel_manager()
    kept = mgr2.rehydrate_sessions()
    assert kept == 1
    assert mgr2.get_session(s.session_id) is not None
    # And the service scope was re-attached.
    assert mgr2.get_session(s.session_id).service_ids == ["svcA"]


def test_rehydrate_prunes_expired_sessions():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Force the row's login_time to be ancient.
    con = share_db.get_global_share_con()
    con.execute(
        "UPDATE remote_sessions SET login_time=?, last_active_time=?", ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
    )
    con.commit()

    tunnel.reset_for_tests()
    mgr2 = tunnel.get_tunnel_manager()
    kept = mgr2.rehydrate_sessions()
    assert kept == 0
    # And the row is deleted from the persistence table.
    assert share_db.get_all_sessions() == []


def test_panic_boots_all_and_writes_audit():
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    result = mgr.panic()
    assert result["sessions_booted"] == 1
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "PANIC_TRIGGERED" for a in audits)


# ── start_sharing input validation ─────────────────────────────────────────


def test_start_sharing_rejects_bare_http():
    mgr = tunnel.get_tunnel_manager()
    with pytest.raises(ValueError, match="HTTPS"):
        mgr.start_sharing(public_endpoint="http://insecure.example.com")


def test_start_sharing_requires_public_endpoint():
    mgr = tunnel.get_tunnel_manager()
    with pytest.raises(ValueError, match="public_endpoint"):
        mgr.start_sharing(public_endpoint=None)


def test_direct_expose_https_records_audit_and_returns_url():
    mgr = tunnel.get_tunnel_manager()
    out = mgr.start_sharing(public_endpoint="https://demo.example.com")
    assert out["public_url"] == "https://demo.example.com"
    assert "tunnel_url" not in out
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SHARE_START" for a in audits)
    mgr.stop_sharing()
    audits = share_db.get_share_audit_logs()
    assert any(a["event_type"] == "SHARE_STOP" for a in audits)


# ── Concurrent session capacity ────────────────────────────────────────────


def test_active_session_count_tracks_dict_size():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.active_session_count() == 0
    a = share_db.create_remote_invite(
        name="A",
        email="a@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    b = share_db.create_remote_invite(
        name="B",
        email="b@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    mgr.create_session(invite=a, ip_address="1.1.1.1", user_agent="x", headers={"user-agent": "Chrome/1 Mac OS X"})
    mgr.create_session(invite=b, ip_address="2.2.2.2", user_agent="x", headers={"user-agent": "Chrome/1 Mac OS X"})
    assert mgr.active_session_count() == 2


# ── Telemetry & observability ──────────────────────────────────────────────


def test_record_heartbeat_unauth_increments_counter():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.get_telemetry()["heartbeat_unauth_count"] == 0
    mgr.record_heartbeat_unauth()
    mgr.record_heartbeat_unauth()
    assert mgr.get_telemetry()["heartbeat_unauth_count"] == 2


def test_rate_limit_snapshot_reports_failures_and_lockouts():
    mgr = tunnel.get_tunnel_manager()
    # 1 failure (sub-threshold) and a full lockout from a second ip.
    mgr.record_login_failure("1.2.3.4", "a@example.com")
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        mgr.record_login_failure("5.6.7.8", "b@example.com")
    snap = mgr.get_rate_limit_snapshot()
    fail_ips = {row["ip"] for row in snap["failures"]}
    lock_ips = {row["ip"] for row in snap["lockouts"]}
    assert "1.2.3.4" in fail_ips
    assert "5.6.7.8" in lock_ips
    # The locked IP also still has a failure record (until pruned by window).
    locked_row = next(r for r in snap["lockouts"] if r["ip"] == "5.6.7.8")
    assert locked_row["remaining_s"] > 0


def test_rate_limit_snapshot_prunes_expired_lockouts(monkeypatch):
    mgr = tunnel.get_tunnel_manager()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        mgr.record_login_failure("9.9.9.9", "c@example.com")
    # Time-travel past the lockout window.
    real_time = time.time
    monkeypatch.setattr(tunnel.time, "time", lambda: real_time() + tunnel.LOGIN_LOCKOUT_S + 1)
    snap = mgr.get_rate_limit_snapshot()
    assert all(row["ip"] != "9.9.9.9" for row in snap["lockouts"])


def test_telemetry_records_uptime_history_on_stop():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    mgr.stop_sharing()
    history = mgr.get_telemetry()["tunnel_uptime_history"]
    assert len(history) == 1
    assert history[0]["reason"] == "stop"
    assert history[0]["duration_s"] >= 0
    assert history[0]["started_at"]
    assert history[0]["ended_at"]


def test_telemetry_records_uptime_history_on_panic():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    mgr.panic()
    history = mgr.get_telemetry()["tunnel_uptime_history"]
    assert any(entry["reason"] == "panic" for entry in history)


def test_telemetry_history_is_bounded():
    mgr = tunnel.get_tunnel_manager()
    # Cycle the tunnel 55 times; ring should retain only the last 50.
    for _ in range(55):
        mgr.start_sharing(public_endpoint="https://demo.example.com")
        mgr.stop_sharing()
    # Internal buffer is bounded; the exposed slice is the last 20.
    assert len(mgr._tunnel_uptime_history) == 50
    assert len(mgr.get_telemetry()["tunnel_uptime_history"]) == 20


def test_telemetry_current_uptime_reflects_running_tunnel():
    mgr = tunnel.get_tunnel_manager()
    assert mgr.get_telemetry()["current_uptime_s"] is None
    mgr.start_sharing(public_endpoint="https://demo.example.com")
    uptime = mgr.get_telemetry()["current_uptime_s"]
    assert uptime is not None and uptime >= 0
    mgr.stop_sharing()
    assert mgr.get_telemetry()["current_uptime_s"] is None


# ── Audit-log filtering ────────────────────────────────────────────────────


def test_get_share_audit_logs_filters_by_event_type():
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="a@example.com", ip_address="1.1.1.1", details="x")
    share_db.log_share_audit_event(event_type="LOGIN_SUCCESS", email="a@example.com", ip_address="1.1.1.1", details="x")
    fails = share_db.get_share_audit_logs(event_type="LOGIN_FAIL")
    assert fails and all(r["event_type"] == "LOGIN_FAIL" for r in fails)


def test_get_share_audit_logs_filters_by_email_substring():
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="bob@example.com", ip_address="1.1.1.1", details="x")
    rows = share_db.get_share_audit_logs(email_substr="alice")
    assert rows and all("alice" in (r["email"] or "") for r in rows)


def test_get_share_audit_logs_filters_by_time_window():
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    before = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    after = (now + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")

    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email="t@example.com", ip_address="1.1.1.1", details="x")
    rows = share_db.get_share_audit_logs(since=before, until=after, email_substr="t@example.com")
    assert rows
    # `until` before window excludes everything.
    rows = share_db.get_share_audit_logs(until=before, email_substr="t@example.com")
    assert not rows


# ── LRU Eviction Under Capacity ────────────────────────────────────────────


def test_rate_limiter_lru_eviction(monkeypatch):
    from backend.utils.tunnel import rate_limiter

    # Set MAX_TRACKED_IPS to 3 for testing.
    monkeypatch.setattr(rate_limiter, "MAX_TRACKED_IPS", 3)

    rl = rate_limiter._LoginRateLimiter()

    # Record 1 failure for 3 different IPs.
    rl.record_failure("1.1.1.1")
    rl.record_failure("2.2.2.2")
    rl.record_failure("3.3.3.3")

    # Order of self._failures should be: "1.1.1.1", "2.2.2.2", "3.3.3.3"
    assert list(rl._failures.keys()) == ["1.1.1.1", "2.2.2.2", "3.3.3.3"]

    # Touch "1.1.1.1" again (moves it to the end/MRU).
    rl.record_failure("1.1.1.1")
    assert list(rl._failures.keys()) == ["2.2.2.2", "3.3.3.3", "1.1.1.1"]

    # Record failure for a 4th IP. "2.2.2.2" (oldest/LRU) should be evicted.
    rl.record_failure("4.4.4.4")
    assert list(rl._failures.keys()) == ["3.3.3.3", "1.1.1.1", "4.4.4.4"]
    assert "2.2.2.2" not in rl._failures

    # Trigger lockout for 3 different IPs.
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("3.3.3.3")
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("1.1.1.1")
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("4.4.4.4")

    # Order of lockouts should be: "3.3.3.3", "1.1.1.1", "4.4.4.4"
    assert list(rl._lockouts.keys()) == ["3.3.3.3", "1.1.1.1", "4.4.4.4"]

    # Trigger a lockout for "5.5.5.5". "3.3.3.3" (oldest lockout) should be evicted.
    for _ in range(rate_limiter.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("5.5.5.5")

    assert list(rl._lockouts.keys()) == ["1.1.1.1", "4.4.4.4", "5.5.5.5"]
    assert "3.3.3.3" not in rl._lockouts


# ── F014: mid-session permission revocation propagation ──────────────────


def test_validate_session_propagates_explicit_pii_policy_null_revocation(monkeypatch):
    """An admin who clears ``pii_policy`` (sets it to None on the invite)
    must immediately downgrade an active session's pii_policy on the next
    validate_session() call.

    Regression for F014 (audit run 7ba15352): the prior
    ``if new_pii_policy is not None: session.pii_policy = ...`` silently
    dropped the revocation. The session kept its cached permissive
    pii_policy until the 2h-idle / 24h-absolute timeout, which defeats
    the entire point of mid-session re-sync.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite()
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    # Pretend the session started with a permissive policy.
    session.pii_policy = {"mode": "passthrough"}

    # Admin explicitly clears the policy on the invite (None means
    # "no policy", not "leave alone").
    fake_invite = {**invite, "pii_policy": None, "service_ids": ["svcA"]}
    monkeypatch.setattr(share_db, "get_remote_invite", lambda _: fake_invite)

    out = mgr.validate_session(session.session_id)
    assert out is not None, "session should still be valid; only permissions downgrade"
    assert out.pii_policy is None, (
        f"explicit pii_policy=None on the invite must propagate to the session; "
        f"got {out.pii_policy!r} (F014 regression)"
    )


def test_validate_session_propagates_explicit_service_ids_null_revocation(monkeypatch):
    """An admin who clears ``service_ids`` (sets it to None on the invite)
    must immediately empty the session's service_ids on the next
    validate_session() call — i.e. no services accessible.

    Regression for F014 (audit run 7ba15352): the prior
    ``if fresh_service_ids is not None: session.service_ids = ...``
    silently dropped the revocation, leaving the cached permissive list
    in place until natural session timeout.
    """
    mgr = tunnel.get_tunnel_manager()
    invite = _seed_invite(service_ids=["svcA", "svcB"])
    session = mgr.create_session(
        invite=invite,
        ip_address="1.2.3.4",
        user_agent="Chrome/126",
        headers={"user-agent": "Chrome/126 Mac OS X"},
    )
    assert session.service_ids == ["svcA", "svcB"]

    # Admin clears service_ids — should empty the session immediately.
    fake_invite = {**invite, "service_ids": None}
    monkeypatch.setattr(share_db, "get_remote_invite", lambda _: fake_invite)

    out = mgr.validate_session(session.session_id)
    assert out is not None, "session is still valid; only permissions downgrade"
    assert out.service_ids == [], (
        f"explicit service_ids=None on the invite must empty the session's list; "
        f"got {out.service_ids!r} (F014 regression)"
    )
