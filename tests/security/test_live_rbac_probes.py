"""Live end-to-end RBAC probes against a deployed instance.

Pins the five P0 fixes from the 2026-06-15 audit so a future deploy
that regresses any of them fails this suite. Each test asserts a
specific live-confirmed-vulnerable behavior is now blocked.

Skips silently when FLA_PROBE_BASE_URL / FLA_PROBE_EMAIL /
FLA_PROBE_PASSCODE aren't set — see ``conftest.py``.

Pair with the unit-test coverage in tests/utils/ and tests/routers/
(every fix has both layers): these tests catch the failure modes a
mock would miss (middleware order changes, response shape drift,
hook ordering on the openapi-fetch client, etc.).
"""

from __future__ import annotations

import datetime
import json

import pytest

from tests.security.conftest import analyst_request

pytestmark = [pytest.mark.security_regression]


# ── Fix 1 (R-6) — bare debug_queries / debug_calls stripped ────────────────


def test_query_response_does_not_leak_debug_queries(probe_session):
    """Live-confirmed pre-fix: /api/query SELECT 1 leaked the full DuckDB
    Iceberg View Resolution SQL via a bare-name ``debug_queries`` field.
    Post-fix: stripped along with ``_debug_queries``."""
    status, body = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SELECT 1 AS x"},
    )
    assert status == 200, f"SELECT 1 should succeed for analyst, got {status}: {body[:200]!r}"
    resp = json.loads(body)
    for key in ("debug_queries", "_debug_queries", "debug_calls", "_debug_calls", "debug_sqlite", "_debug_sqlite"):
        assert key not in resp, f"{key!r} present in /api/query response — strip helper missed it"


def test_dashboard_bundle_does_not_leak_debug_queries(probe_session):
    """Same fix, different emit site. /api/dashboard/bundle composes
    sub-responses and emits bare ``debug_queries`` at the top level."""
    now = datetime.datetime.now(datetime.UTC)
    body = {
        "service_id": probe_session["service_id"],
        "start_time": (now - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "end_time": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "metric": "requests",
        "interval": "minute",
    }
    status, raw = analyst_request(probe_session, "POST", "/api/dashboard/bundle", body)
    assert status == 200, f"/api/dashboard/bundle failed for analyst: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    for key in ("debug_queries", "_debug_queries", "debug_calls", "_debug_calls", "debug_sqlite", "_debug_sqlite"):
        assert key not in resp, f"{key!r} present in /api/dashboard/bundle response"


# ── Fix 2 (R-4) — /scoring/labels PII projection ───────────────────────────


_LABEL_PII_FIELDS = ("notes", "flagged_by", "sample_ip", "sample_ua", "sample_url")


def test_scoring_labels_strips_pii_for_analyst(probe_session):
    """Live-confirmed pre-fix: GET /scoring/labels returned
    sample_ip / sample_ua / sample_url / flagged_by / notes verbatim.
    Post-fix: rows projected to {id, service_id, sid, label, created_at, updated_at}."""
    status, raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/labels",
    )
    if status == 404:
        pytest.skip("no scoring labels configured on this service — nothing to probe")
    assert status == 200, f"/scoring/labels failed: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    labels = resp.get("labels", [])
    if not labels:
        pytest.skip("no scoring labels exist on this service — nothing to probe")
    for row in labels:
        for pii_field in _LABEL_PII_FIELDS:
            assert pii_field not in row, f"PII field {pii_field!r} present in analyst /scoring/labels response: {row!r}"


# ── Fix 3 (R-3) — /scoring/analytics composite block-bypass ────────────────


def test_scoring_analytics_composite_omits_evaluation_for_analyst(probe_session):
    """Live-confirmed pre-fix: composite included ``evaluation`` and
    ``evaluation_per_reason`` even though the direct
    /scoring/evaluation/per-reason endpoint 403s for analyst.
    Post-fix: composite returns exactly the 4 analyst-safe sub-keys."""
    status, raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/analytics",
    )
    assert status == 200, f"/scoring/analytics failed: {status} {raw[:200]!r}"
    resp = json.loads(raw)
    assert "evaluation" not in resp, "composite leaked 'evaluation' to analyst"
    assert "evaluation_per_reason" not in resp, "composite leaked 'evaluation_per_reason' to analyst"
    # Sanity: analyst-safe keys should still be present.
    for safe_key in ("top_flagged", "score_distribution", "compliance_breakdown", "health"):
        assert safe_key in resp, f"analyst-safe composite key {safe_key!r} missing"


def test_scoring_evaluation_per_reason_direct_is_403(probe_session):
    """The direct admin-only endpoint must still 403 for analyst."""
    status, _raw = analyst_request(
        probe_session,
        "GET",
        f"/api/services/{probe_session['service_id']}/scoring/evaluation/per-reason",
    )
    assert status == 403, f"direct /evaluation/per-reason returned {status}, expected 403"


# ── Fix 4 (R-1) — time-bounds enforcement ──────────────────────────────────


def test_dashboard_aggregates_400s_on_empty_window(probe_session):
    """start_time == end_time → clamp resolves to empty → 400 with
    ``time_range_empty: true`` (matches /api/sessions 7-day clamp's 400
    contract)."""
    now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/dashboard/aggregates",
        {
            "service_id": probe_session["service_id"],
            "start_time": now,
            "end_time": now,
            "metric": "requests",
            "interval": "minute",
        },
    )
    assert status == 400, f"empty-window request should 400, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    detail = body.get("detail") or {}
    assert detail.get("time_range_empty") is True, f"missing time_range_empty marker in 400 body: {body!r}"


def test_dashboard_aggregates_400s_on_inverted_window(probe_session):
    """start_time > end_time → same empty-clamp 400."""
    now = datetime.datetime.now(datetime.UTC)
    later = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    earlier = (now - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/dashboard/aggregates",
        {
            "service_id": probe_session["service_id"],
            "start_time": later,
            "end_time": earlier,
            "metric": "requests",
            "interval": "minute",
        },
    )
    assert status == 400, f"inverted-window should 400, got {status}: {raw[:200]!r}"


# ── Fix 5 (F-8/9/10) — SQL validator allowlist + SHOW_REF reject ───────────


def test_query_blocks_show_tables(probe_session):
    """Pre-fix: SHOW TABLES listed foreign service tables in the pooled
    DuckDB catalog (cross-tenant catalog leak). Post-fix: 403 show_ref."""
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SHOW TABLES"},
    )
    assert status == 403, f"SHOW TABLES should 403, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    assert (
        "SHOW" in (body.get("detail", {}).get("error") or "").upper()
        or "DESCRIBE" in (body.get("detail", {}).get("error") or "").upper()
    )


def test_query_blocks_describe(probe_session):
    """DESCRIBE — same SHOW_REF gate."""
    status, _raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "DESCRIBE logs"},
    )
    assert status == 403


def test_query_blocks_foreign_table_select(probe_session):
    """Pre-fix: ``SELECT COUNT(*) FROM logs_<other_service>`` succeeded
    because the SQL validator had no per-call table allowlist. Post-fix:
    403 catalog_blocklist:foreign_table."""
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {
            "service_id": probe_session["service_id"],
            "mode": "raw",
            "sql": "SELECT COUNT(*) FROM logs_some_other_service_that_does_not_exist",
        },
    )
    assert status == 403, f"foreign-table SELECT should 403, got {status}: {raw[:200]!r}"
    body = json.loads(raw)
    error_str = (body.get("detail") or {}).get("error", "")
    assert "not in the allowed set" in error_str, f"expected allowlist rejection, got: {error_str!r}"


def test_query_allows_canonical_logs(probe_session):
    """SELECT COUNT(*) FROM logs is the analyst's actual query workload —
    must keep working post-fix. Quick smoke that the allowlist isn't
    overly strict."""
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": "SELECT COUNT(*) AS n FROM logs"},
    )
    assert status == 200, f"canonical /api/query failed post-fix: {status} {raw[:200]!r}"
    body = json.loads(raw)
    assert "data" in body and len(body["data"]) == 1
    assert "n" in body["data"][0]


def test_query_allows_recursive_cte(probe_session):
    """Recursive CTEs reference their own alias as a BASE_TABLE — the
    CTE-alias-expansion follow-up to Fix 5 must keep them passing."""
    sql = "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<5) SELECT * FROM x"
    status, raw = analyst_request(
        probe_session,
        "POST",
        "/api/query",
        {"service_id": probe_session["service_id"], "mode": "raw", "sql": sql},
    )
    assert status == 200, f"recursive CTE should pass post-fix: {status} {raw[:200]!r}"


# ── Cross-cutting RBAC gate sanity ─────────────────────────────────────────


@pytest.mark.parametrize(
    "path",
    [
        "/api/admin/services",
        "/api/admin/ingest",
        "/api/admin/share/invites",
        "/api/cron-runs",
        "/api/audit-logs",
        "/api/alerts",
        "/api/usage",
        "/api/provision/services",
        "/api/debug/state",
    ],
)
def test_admin_paths_blocked_for_analyst(probe_session, path):
    """Defense-in-depth: every admin path must 403 for analyst.
    Catches regressions in _ANALYST_BLOCKED_PREFIXES that the
    blocklist-function unit tests might miss if middleware ordering
    changes."""
    status, raw = analyst_request(probe_session, "GET", path)
    assert status == 403, f"{path} should 403 for analyst, got {status}: {raw[:200]!r}"
