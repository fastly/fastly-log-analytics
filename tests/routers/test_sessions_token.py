"""Session-detail opaque-token wiring + RBAC regression tests.

Covers the fix for: a PII-masking analyst clicking a session saw "No results"
because the masked ``ip`` (``1.2.3.xxx``) was round-tripped as the detail lookup
key and never matched a real stored IP. The fix attaches an opaque AES-GCM
``session_token`` to every list row and keys the detail lookup off it.

Layers:
* wiring — the list endpoint mints a token; that token resolves the detail.
* RBAC (security_regression) — a masking analyst MUST use the token and is
  rejected when supplying a raw/masked ``ip`` (presence-oracle guard); a
  non-masking analyst keeps the legacy raw-ip path; the detail body is masked.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.core.request_context import build_request_context
from backend.core.share_db.validation import apply_pii_policy
from backend.main import app
from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

_HDR = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed(in_memory_duckdb, source, ip="10.0.0.1", n=10):
    # Mock logs store naive TIMESTAMP holding UTC wall-clock. Production clamps
    # to tz-aware bounds (parse_iso_utc → CAST AS TIMESTAMPTZ), so pin the
    # session TZ to UTC (as prod does) — otherwise the naive↔tz-aware compare
    # uses the host's local TZ and the window misses every row.
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    # The schema cache is keyed by table name, not connection — clear it so
    # get_sessions reads THIS fresh in-memory connection's columns rather than
    # a stale (possibly empty) entry left by another test on "test_service".
    _clear_schema_cache()


def _seed_zero_width(in_memory_duckdb, source, ip="10.0.0.9", n=5):
    """Seed a single-request session: every log shares ONE timestamp so the
    aggregated session has session_start == session_end (a zero-width window).
    Returns the pinned timestamp."""
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    ts = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    logs = generate_mock_logs(source, num_logs=n, hours_ago=1)
    for log in logs:
        log["ip"] = ip
        log["timestamp"] = ts
    insert_mock_logs(in_memory_duckdb, _safe_table(source["name"]), logs)
    _clear_schema_cache()
    return ts


def _window():
    now = datetime.now(UTC)
    return (now - timedelta(hours=2)).isoformat(), now.isoformat()


def _use_analyst(in_memory_duckdb, source, *, mask_ips: bool):
    """Re-point build_request_context to inject an analyst session into ctx.
    (The local TestClient socket path classifies as admin in the middleware, so
    we exercise the router's analyst branch via the injected ctx.analyst_session.)"""
    session = SimpleNamespace(service_ids=[source["service_id"]], pii_policy={"mask_ips": mask_ips})
    app.dependency_overrides[build_request_context] = override_request_context(
        source=source, con=in_memory_duckdb, session=session
    )


def _list_first_token(client, in_memory_duckdb, source):
    # Omit start/end → default range (raw session path). An explicit >1h window
    # routes get_sessions to the rollup path, which has no parquet in tests.
    r = client.post("/api/sessions", headers=_HDR, json={})
    assert r.status_code == 200, r.text
    sessions = r.json()["sessions"]
    assert sessions, "expected at least one session row from seeded data"
    return sessions[0]


# ── wiring ──────────────────────────────────────────────────────────────────


def test_list_attaches_session_token(client, in_memory_duckdb, test_service_source):
    """Every session row carries a non-empty opaque token."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row.get("session_token"), "session_token missing/empty on list row"
    # opaque — must not embed the raw ip
    assert "10.0.0.1" not in row["session_token"]


def test_token_resolves_detail(client, in_memory_duckdb, test_service_source):
    """A token taken from the list resolves a non-empty detail result."""
    _seed(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["data"], "token-keyed detail returned no rows"


def test_invalid_token_returns_400(client, in_memory_duckdb, test_service_source):
    """A malformed/stale token surfaces a clear 400, not a silent empty grid."""
    _seed(in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": "garbage.token"})
    assert r.status_code == 400, r.text
    assert r.json()["detail"]["error"] == "session_token_invalid"


def test_zero_width_session_detail_via_token(client, in_memory_duckdb, test_service_source):
    """A single-request session (session_start == session_end) must still
    resolve detail rows. Previously the time clamp rejected the zero-width
    window with 400 'time_range_empty' and the modal showed 'No results' —
    for ~28% of real sessions, all roles. The detail handler now widens a
    zero-width window by 1s/side before clamping."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    assert row["session_start"] == row["session_end"], "fixture should produce a zero-width session"
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail returned no rows"


def test_zero_width_session_detail_via_raw_ip(client, in_memory_duckdb, test_service_source):
    """The same zero-width widening helps the legacy raw-ip path (admin /
    non-masking analyst), not only the token path — the widen is shared."""
    _seed_zero_width(in_memory_duckdb, test_service_source)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": row["ip"], "start_time": row["session_start"], "end_time": row["session_end"]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "zero-width session detail (raw ip) returned no rows"


# ── RBAC regression ─────────────────────────────────────────────────────────


@pytest.mark.security_regression
@pytest.mark.parametrize("raw_ip", ["10.0.0.1", "10.0.0.xxx"])
def test_masking_analyst_raw_ip_rejected(client, in_memory_duckdb, test_service_source, raw_ip):
    """A masking analyst supplying a raw OR masked top-level ip (no token) is
    rejected with 403 — accepting it would be a presence oracle (the masked
    value can never match; a raw value would probe for a guessed real IP)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": raw_ip, "start_time": start, "end_time": end},
    )
    assert r.status_code == 403, r.text
    assert r.json()["detail"]["error"] == "pii_policy_violation"


@pytest.mark.security_regression
def test_masking_analyst_token_returns_rows(client, in_memory_duckdb, test_service_source):
    """The fix itself: a masking analyst drills in via the opaque token and gets
    rows — without ever holding the real IP."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=True)
    row = _list_first_token(client, in_memory_duckdb, test_service_source)
    r = client.post("/api/sessions/detail", headers=_HDR, json={"session_token": row["session_token"]})
    assert r.status_code == 200, r.text
    assert r.json()["data"], "masking analyst token-keyed detail returned no rows"


@pytest.mark.security_regression
def test_non_masking_analyst_raw_ip_allowed(client, in_memory_duckdb, test_service_source):
    """A NON-masking analyst still uses the legacy raw-ip path (the lock is
    gated on pii_policy.mask_ips, not on analyst-hood)."""
    _seed(in_memory_duckdb, test_service_source)
    _use_analyst(in_memory_duckdb, test_service_source, mask_ips=False)
    start, end = _window()
    r = client.post(
        "/api/sessions/detail",
        headers=_HDR,
        json={"ip": "10.0.0.1", "start_time": start, "end_time": end},
    )
    assert r.status_code == 200, r.text
    assert r.json()["data"], "non-masking analyst raw-ip detail returned no rows"


@pytest.mark.security_regression
def test_detail_body_ip_column_is_masked_by_policy():
    """Pins that the detail response shape ({"data":[{"ip":...}], "columns":[...]})
    is caught by apply_pii_policy's key-name masking — the invariant that keeps
    raw IPs out of a masking analyst's detail body once rows are returned. If a
    future change aliases the ip column out of IP_FAMILY_KEYS this fails."""
    body = {
        "columns": ["ip", "url"],
        "data": [
            {"ip": "203.0.113.45", "url": "/a"},
            {"ip": "198.51.100.7", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # non-PII columns stay verbatim
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_detail_body_cookie_session_is_redacted_by_policy():
    """Phase-4 Track C added `cookie_session` — a hashed per-client session id
    captured at the edge. It is client PII, so a mask_ips analyst viewing raw
    /logs or /query rows must NOT see the hash. apply_pii_policy redacts the
    cookie_session column (SESSION_ID_KEYS) wholesale; if a future change drops
    it from the masked set this fails."""
    body = {
        "columns": ["ip", "cookie_session", "url"],
        "data": [
            {"ip": "203.0.113.45", "cookie_session": "a1b2c3d4deadbeef", "url": "/a"},
            {"ip": "198.51.100.7", "cookie_session": "", "url": "/b"},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    # Non-empty session hash → redacted; empty stays empty; raw hash never leaks.
    assert [r["cookie_session"] for r in masked["data"]] == ["[redacted]", ""]
    assert all("deadbeef" not in str(r.get("cookie_session")) for r in masked["data"])
    # IP still masked, url still verbatim.
    assert [r["ip"] for r in masked["data"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    assert [r["url"] for r in masked["data"]] == ["/a", "/b"]


@pytest.mark.security_regression
def test_aggregates_topn_cookie_session_value_is_redacted_by_policy():
    """/api/dashboard/aggregates top-N panels carry the dimension value under a
    generic ``value`` key nested as ``data[<field>]["top"][i]["value"]`` — the
    field name is the PARENT key, not the cell key. Pins that apply_pii_policy
    threads that field context so a cookie_session top-N cell is redacted (a
    mask_ips analyst must not read raw session hashes off the Top card) while
    the IP top-N cell stays masked (unchanged) and url stays verbatim."""
    body = {
        "data": {
            "ip": {"top": [{"value": "203.0.113.45", "count": 9}], "total": 9},
            "cookie_session": {
                "top": [{"value": "a1b2c3d4deadbeef", "count": 7}, {"value": "", "count": 1}],
                "total": 8,
            },
            "url": {"top": [{"value": "/login", "count": 5}], "total": 5},
        }
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    cs_top = masked["data"]["cookie_session"]["top"]
    assert [c["value"] for c in cs_top] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked["data"]["cookie_session"])
    # IP top-N still value-shape masked; url panel untouched.
    assert masked["data"]["ip"]["top"][0]["value"] == "203.0.113.xxx"
    assert masked["data"]["url"]["top"][0]["value"] == "/login"


@pytest.mark.security_regression
def test_field_values_cookie_session_is_redacted_by_policy():
    """/api/dashboard/field-values names its column in a SIBLING ``field`` key
    (``{"field": "cookie_session", "values": [{"value": <hash>}]}``), not the
    parent key. Pins that apply_pii_policy resolves that sibling so the distinct
    cookie_session values are redacted — otherwise a mask_ips analyst could
    enumerate every raw session hash via the value picker."""
    body = {
        "field": "cookie_session",
        "values": [
            {"value": "a1b2c3d4deadbeef", "count": 12},
            {"value": "", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["[redacted]", ""]  # empty preserved
    assert "deadbeef" not in str(masked)
    assert masked["field"] == "cookie_session"  # the field NAME is fine to show


@pytest.mark.security_regression
def test_field_values_ip_is_masked_by_policy():
    """/api/dashboard/field-values with field=ip names the column in the SIBLING
    ``field`` key, so the raw distinct IP ``value`` cells slipped past the
    key-name masker — a mask_ips analyst could enumerate every raw client IP via
    the value picker. Pins that apply_pii_policy resolves the sibling ``field``
    for IP columns too and value-shape masks the cells (non-IP values verbatim)."""
    body = {
        "field": "ip",
        "values": [
            {"value": "203.0.113.45", "count": 12},
            {"value": "198.51.100.7", "count": 3},
        ],
    }
    masked = apply_pii_policy(body, {"mask_ips": True})
    assert [v["value"] for v in masked["values"]] == ["203.0.113.xxx", "198.51.100.xxx"]
    # A non-IP dimension (url) must stay verbatim — even an IP-shaped url value.
    url_body = {"field": "url", "values": [{"value": "/a"}, {"value": "1.2.3.4"}]}
    assert [v["value"] for v in apply_pii_policy(url_body, {"mask_ips": True})["values"]] == ["/a", "1.2.3.4"]
