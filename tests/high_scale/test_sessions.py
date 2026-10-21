"""High-scale sessions: filters, sorting, real totals, field availability and the visibility fence."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import sessions as hs_sessions
from backend.high_scale.sessions import sessions_detail, sessions_endpoint
from backend.models.dashboard import SessionDetailRequest, SessionsRequest, SessionsResponse

START = "2026-09-15T00:00:00Z"
END = "2026-09-16T00:00:00Z"


def _session_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "ip": "203.0.113.7",
        "ja4": "t13d1516h2_abc",
        "session_start": datetime(2026, 9, 15, 10, 0, 0, tzinfo=UTC),
        "session_end": "2026-09-15 10:20:00.000",
        "req_count": 1500,
        "asn": 7922,
        "country": "US",
        "unique_urls": 40,
        "ua": "curl/8",
        "reqs_4xx": 10,
        "reqs_5xx": 1,
        "total_bytes": 123456,
        "median_rtt_ms": 12.5,
        "edge_count": 1000,
        "shield_count": 500,
        "edge_sid": "es-1",
        "streaming_reqs": 3,
        "flagged": 1,
        "is_streaming": 1,
        "sid": 0,
        "total_sessions": 321,
        "any_rtt": 1,
        "any_ja4": 1,
        "any_edge": 1,
        "any_edge_sid": 1,
        "any_cmcd": 1,
    }
    row.update(overrides)
    return row


class _Client:
    def __init__(self, rows: list[dict[str, Any]] | None = None, *, total: int = 0, fail: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._rows = [_session_row()] if rows is None else rows
        self._total = total
        self._fail = fail

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if self._fail:
            raise RuntimeError("clickhouse unavailable")
        if "sessions:count" in sql:
            return [{"total_sessions": self._total}]
        if "sessions:page" in sql:
            return self._rows
        raise AssertionError(sql)

    def sql(self, marker: str) -> tuple[str, dict]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def _run(client: _Client, **overrides: Any) -> dict[str, Any]:
    req = SessionsRequest(**overrides)
    return sessions_endpoint(SimpleNamespace(service_id="svc", client=client), req, START, END)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _asn_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs_sessions, "_asn_label_map", lambda service_id, asns: {asn: f"AS{asn} Name" for asn in asns})


def test_filters_narrow_the_events_that_are_grouped_into_sessions() -> None:
    client = _Client()

    _run(
        client,
        filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}},
    )

    sql, params = client.sql("sessions:page")
    assert "country IN ({flt_0_0:String})" in sql
    assert "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
    assert params["flt_0_0"] == "US" and params["flt_k1"] == "pop" and params["flt_1_0"] == "SJC"


def test_events_are_fenced_to_visible_published_batches_inside_the_window() -> None:
    client = _Client()

    _run(client)

    sql, params = client.sql("sessions:page")
    assert "publication_state='visible'" in sql
    assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
    assert "event_timestamp >= {start_time:DateTime64(3)}" in sql
    assert params["service_id"] == "svc"


def test_sorting_honours_the_requested_column_and_direction() -> None:
    client = _Client()

    _run(client, sort_by="req_count", sort_dir="asc")

    sql, _ = client.sql("sessions:page")
    assert "ORDER BY req_count ASC" in sql


def test_unknown_sort_columns_fall_back_to_session_start() -> None:
    client = _Client()

    _run(client, sort_by="1; DROP TABLE x")

    sql, _ = client.sql("sessions:page")
    assert "ORDER BY session_start DESC" in sql
    assert "DROP TABLE" not in sql


def test_paging_uses_typed_parameters_and_the_page_offset() -> None:
    client = _Client()

    _run(client, page=3, limit=25)

    sql, params = client.sql("sessions:page")
    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in sql
    assert params["limit"] == 25 and params["offset"] == 50


def test_flag_thresholds_default_like_the_standard_path_and_are_reported() -> None:
    client = _Client()

    result = _run(client)

    _, params = client.sql("sessions:page")
    assert params["min_reqs_flag"] == 1000 and params["min_4xx_pct_flag"] == 20.0
    assert result["min_reqs_flag"] == 1000 and result["min_4xx_pct_flag"] == 20.0
    custom = _run(_Client(), min_reqs_flag=50, min_4xx_pct_flag=5.5)
    assert custom["min_reqs_flag"] == 50 and custom["min_4xx_pct_flag"] == 5.5


def test_streaming_sessions_are_detected_from_the_cmcd_map() -> None:
    client = _Client()

    _run(client, streaming_only=True)

    sql, params = client.sql("sessions:page")
    assert "cmcd['sid']" in sql
    assert "custom_fields['cmcd_sid']" not in sql
    assert "streaming_only" in params and params["streaming_only"] == 1


def test_edge_and_shield_counts_understand_stored_boolean_spellings() -> None:
    client = _Client()

    _run(client)

    sql, _ = client.sql("sessions:page")
    assert "lower(edge) IN ('1', 'true')" in sql
    assert "lower(edge) IN ('0', 'false')" in sql


def test_total_is_the_real_session_count_not_a_page_size_guess() -> None:
    result = _run(_Client(), limit=1)

    assert result["total"] == 321
    assert result["page"] == 1 and result["limit"] == 1


def test_an_empty_page_past_the_end_reports_the_real_total() -> None:
    client = _Client(rows=[], total=40)

    result = _run(client, page=9, limit=5)

    assert result["sessions"] == [] and result["total"] == 40
    assert "sessions:count" in " ".join(sql for sql, _ in client.calls)


def test_an_empty_first_page_needs_no_count_query() -> None:
    client = _Client(rows=[])

    result = _run(client)

    assert result["total"] == 0
    assert all("sessions:count" not in sql for sql, _ in client.calls)


def test_field_availability_comes_from_the_data() -> None:
    present = _run(_Client())
    absent = _run(
        _Client(rows=[_session_row(any_rtt=0, any_ja4=0, any_edge=0, any_edge_sid=0, any_cmcd=0, median_rtt_ms=None)])
    )

    assert (
        present["has_rtt"],
        present["has_ja4"],
        present["has_edge"],
        present["has_edge_sid"],
        present["has_cmcd"],
    ) == (
        True,
        True,
        True,
        True,
        True,
    )
    assert (absent["has_rtt"], absent["has_ja4"], absent["has_edge"], absent["has_edge_sid"], absent["has_cmcd"]) == (
        False,
        False,
        False,
        False,
        False,
    )


def test_rows_are_clean_session_dicts_with_utc_timestamps_and_hoisted_asn_labels() -> None:
    result = _run(_Client())

    (row,) = result["sessions"]
    assert row["session_start"] == "2026-09-15T10:00:00Z"
    assert row["session_end"] == "2026-09-15T10:20:00.000Z"
    assert row["flagged"] is True and row["is_streaming"] is True
    for helper in (
        "sid",
        "total_sessions",
        "any_rtt",
        "any_ja4",
        "any_edge",
        "any_edge_sid",
        "any_cmcd",
        "streaming_reqs",
    ):
        assert helper not in row
    assert result["asn_names"] == {7922: "AS7922 Name"}


def test_output_validates_against_the_standard_response_model() -> None:
    model = SessionsResponse.model_validate(_run(_Client()))

    assert model.total == 321 and model.sessions[0].asn == 7922 and model.asn_names[7922] == "AS7922 Name"


def test_clickhouse_failures_propagate() -> None:
    with pytest.raises(RuntimeError, match="unavailable"):
        _run(_Client(fail=True))


def test_detail_is_fenced_to_visible_batches_and_parameterised() -> None:
    client = _Client()
    client.execute = lambda sql, params=None: (  # type: ignore[method-assign]
        client.calls.append((sql, params or {})) or []
    )

    sessions_detail(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        SessionDetailRequest(),
        START,
        END,
        "203.0.113.7",
        "ja4-x",
    )

    sql, params = client.calls[0]
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "client_ip = {ip:String}" in sql and "custom_fields['ja4'] = {ja4:String}" in sql
    assert params["ip"] == "203.0.113.7" and params["ja4"] == "ja4-x"


def test_two_phase_session_enrichment_defers_heavy_states_to_phase_2() -> None:
    fast_row = _session_row()
    for k in ("asn", "country", "unique_urls", "ua", "median_rtt_ms", "edge_sid"):
        fast_row.pop(k, None)

    calls: list[tuple[str, dict]] = []

    class _TwoPhaseClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append((sql, params or {}))
            if "sessions:page" in sql:
                return [fast_row]
            if "sessions:enrich" in sql:
                return [
                    {
                        "row_idx": 0,
                        "asn": 15169,
                        "country": "DE",
                        "unique_urls": 77,
                        "ua": "Mozilla/5.0",
                        "median_rtt_ms": 8.25,
                        "edge_sid": "es-fast",
                    }
                ]
            raise AssertionError(sql)

    res = sessions_endpoint(
        SimpleNamespace(service_id="svc", client=_TwoPhaseClient()),  # type: ignore[arg-type]
        SessionsRequest(sort_by="req_count", sort_dir="desc"),
        START,
        END,
    )
    assert len(calls) == 2
    page_sql, _ = calls[0]
    enrich_sql, enrich_params = calls[1]
    assert "uniqState" not in page_sql and "medianState" not in page_sql
    assert "sessions:enrich" in enrich_sql
    assert enrich_params["en_ip_0"] == "203.0.113.7"
    assert res["sessions"][0]["asn"] == 15169
    assert res["sessions"][0]["country"] == "DE"
    assert res["sessions"][0]["unique_urls"] == 77
    assert res["sessions"][0]["ua"] == "Mozilla/5.0"
    assert res["sessions"][0]["median_rtt_ms"] == 8.25
    assert res["sessions"][0]["edge_sid"] == "es-fast"
