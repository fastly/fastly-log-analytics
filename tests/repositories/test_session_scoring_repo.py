"""Tests for :mod:`backend.repositories.session_scoring`.

The repository is a thin DuckDB wrapper. Tests mock the connection layer
(``get_connection`` / ``get_source_for_service``) so we don't need a
real DuckDB instance with seeded data — the value is verifying the
SQL-shape decisions, error mapping, and event-grouping logic.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend.repositories import session_scoring as repo

# ── query_logs ────────────────────────────────────────────────────────────────


def _stub_get_source(monkeypatch, src: dict | None) -> None:
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)


def _stub_connection(monkeypatch, rows: list, columns: list[str] | None = None) -> MagicMock:
    """Make get_connection return a context-manager-ish mock whose execute
    yields ``rows`` with ``columns`` schema. Returns the connection mock so
    tests can assert on .execute calls."""
    mock_con = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_con.execute.return_value = mock_cursor
    if columns is not None:
        mock_con.description = [(c, None) for c in columns]
    else:
        mock_con.description = None
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)
    return mock_con


def test_query_logs_404s_when_service_missing(monkeypatch):
    _stub_get_source(monkeypatch, None)
    with pytest.raises(HTTPException) as ei:
        repo.query_logs("missing-svc", "SELECT 1")
    assert ei.value.status_code == 404
    assert "No service" in ei.value.detail["error"]


def test_query_logs_returns_rows_as_dicts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    con = _stub_connection(monkeypatch, rows=[(1, "a"), (2, "b")], columns=["id", "name"])

    result = repo.query_logs("svc-1", "SELECT id, name FROM logs")

    assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    # Connection closed after use.
    con.close.assert_called_once()


def test_query_logs_passes_params_when_provided(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    con = _stub_connection(monkeypatch, rows=[(1,)], columns=["v"])

    repo.query_logs("svc-1", "SELECT 1 WHERE x IN (?, ?)", params=("a", "b"))

    # The parametrised call shape was used (sql + params), not the bare
    # `execute(sql)` shape.
    con.execute.assert_called_with("SELECT 1 WHERE x IN (?, ?)", ("a", "b"))


def test_query_logs_400s_on_duckdb_error(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    mock_con = MagicMock()
    mock_con.execute.side_effect = RuntimeError("table 'logs' does not exist")
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)

    with pytest.raises(HTTPException) as ei:
        repo.query_logs("svc-1", "SELECT * FROM logs")
    assert ei.value.status_code == 400
    assert "table 'logs'" in ei.value.detail["error"]


def test_query_logs_handles_empty_description(monkeypatch):
    """Some DDL/no-result statements yield ``description=None``; the
    repository must still return an empty list rather than blowing up."""
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[], columns=None)

    result = repo.query_logs("svc-1", "CREATE TABLE t (i INT)")

    assert result == []


def test_query_logs_closes_connection_on_error(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    mock_con = MagicMock()
    mock_con.execute.side_effect = RuntimeError("boom")
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda **kw: mock_con)

    with pytest.raises(HTTPException):
        repo.query_logs("svc-1", "SELECT 1")

    mock_con.close.assert_called_once()


def test_query_logs_appends_to_telemetry_queries(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    _stub_connection(monkeypatch, rows=[(1,)], columns=["v"])

    from backend.utils.telemetry import get_queries, start_call_tracking

    start_call_tracking()
    repo.query_logs("svc-1", "SELECT 1")
    queries = get_queries()

    assert len(queries) >= 1
    last = queries[-1]
    assert "SELECT 1" in last["sql"]
    assert last["rows"] == 1
    assert "time_ms" in last


# ── fetch_session_events ─────────────────────────────────────────────────────


def test_fetch_session_events_returns_empty_when_no_sids():
    assert repo.fetch_session_events("svc-1", []) == {}


def test_fetch_session_events_groups_by_edge_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0, 0), "/", 200, "1.1.1.1", "ua-x", 0.5, "ok", None),
        ("sid-a", datetime(2026, 6, 12, 10, 1, 0), "/p", 200, "1.1.1.1", "ua-x", 0.6, "ok", None),
        ("sid-b", datetime(2026, 6, 12, 10, 2, 0), "/", 404, "2.2.2.2", "ua-y", None, "skipped", "abc"),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a", "sid-b"])

    assert set(result.keys()) == {"sid-a", "sid-b"}
    assert len(result["sid-a"]) == 2
    assert len(result["sid-b"]) == 1
    # ts is ISO-formatted from the datetime.
    assert result["sid-a"][0]["ts"].startswith("2026-06-12T10:00:00")
    # Defaults: url='/' when missing.
    assert result["sid-b"][0]["url"] == "/"


def test_fetch_session_events_drops_rows_with_no_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("", datetime(2026, 6, 12), "/a", 200, None, None, None, None, None),
        ("sid-real", datetime(2026, 6, 12), "/b", 200, None, None, None, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-real"])

    assert list(result.keys()) == ["sid-real"]


def test_fetch_session_events_caps_per_sid(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    # 6 events for the same sid; SQL push-down would cap them, but the
    # Python guard at line 117 also enforces the cap defensively.
    rows = [("sid-a", datetime(2026, 6, 12, 10, i, 0), f"/r{i}", 200, None, None, None, None, None) for i in range(6)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"], limit_per_sid=3)

    assert len(result["sid-a"]) == 3


def test_fetch_session_events_stringifies_non_iso_ts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    # ts is a plain string here (not a datetime). The branch at line 124
    # falls through to ``str(ts)``.
    rows = [("sid-a", "2026-06-12 10:00:00", "/", 200, None, None, None, None, None)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"])

    assert result["sid-a"][0]["ts"] == "2026-06-12 10:00:00"


def test_fetch_session_events_handles_none_ts(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [("sid-a", None, "/", 200, None, None, None, None, None)]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    result = repo.fetch_session_events("svc-1", ["sid-a"])

    assert result["sid-a"][0]["ts"] is None


# ── reconstruct_labeled_sessions ──────────────────────────────────────────────


def test_reconstruct_returns_empty_when_no_labels():
    assert repo.reconstruct_labeled_sessions("svc-1", []) == []


def test_reconstruct_returns_empty_when_labels_missing_sid():
    # Labels without sid keys filter out → empty input dict → no work.
    out = repo.reconstruct_labeled_sessions("svc-1", [{"label": "good"}])
    assert out == []


def test_reconstruct_pairs_sessions_with_labels(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, 0.4, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, 0.7, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "bot"}])

    assert len(out) == 1
    session, label = out[0]
    assert label == "bot"
    assert session["session_id"] == "sid-a"
    assert session["max_edge_score"] == 0.7  # MAX across the session
    assert len(session["events"]) == 2


def test_reconstruct_drops_sids_with_no_events(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    # SQL returns nothing for the requested sids.
    _stub_connection(
        monkeypatch,
        rows=[],
        columns=[
            "edge_sid",
            "ts",
            "url",
            "status",
            "ip",
            "ua",
            "edge_score",
            "edge_cookie_compliance",
            "edge_score_reason",
        ],
    )

    out = repo.reconstruct_labeled_sessions(
        "svc-1",
        [
            {"sid": "sid-a", "label": "bot"},
            {"sid": "sid-b", "label": "human"},
        ],
    )

    assert out == []


def test_reconstruct_max_edge_score_none_when_all_scores_null(monkeypatch):
    _stub_get_source(monkeypatch, {"name": "svc-1"})
    rows = [
        ("sid-a", datetime(2026, 6, 12, 10, 0), "/", 200, None, None, None, None, None),
        ("sid-a", datetime(2026, 6, 12, 10, 1), "/p", 200, None, None, None, None, None),
    ]
    columns = [
        "edge_sid",
        "ts",
        "url",
        "status",
        "ip",
        "ua",
        "edge_score",
        "edge_cookie_compliance",
        "edge_score_reason",
    ]
    _stub_connection(monkeypatch, rows=rows, columns=columns)

    out = repo.reconstruct_labeled_sessions("svc-1", [{"sid": "sid-a", "label": "human"}])

    session, _ = out[0]
    # All None → max_edge_score is None rather than collapsing to 0
    # (so the AUC eval doesn't treat unscored sessions as legit zero scores).
    assert session["max_edge_score"] is None
