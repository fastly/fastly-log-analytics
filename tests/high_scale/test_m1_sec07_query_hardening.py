"""M1 Security Regression Suite — SEC-07 ClickHouse Parameterization & Literal-Safe SQL Rewriting."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from backend import config as svcconfig
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import query as hs_query
from backend.high_scale import security as hs_security
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pytestmark = pytest.mark.no_postgres


# ── SEC-07.1: _wellknown_bots typed scalar parameter binding ────────────────


def test_sec07_wellknown_bots_binds_scalar_parameters_without_sql_interpolation(monkeypatch):
    import backend.high_scale.aggregate_query as agg_query
    import backend.utils.bot_sources as bot_sources

    malicious_ua = "Googlebot/2.1'); DROP TABLE request_facts; -- \\"
    executed_queries: list[tuple[str, dict[str, Any]]] = []

    monkeypatch.setattr(
        hs_security,
        "visible_publication_fence_sql",
        lambda *args, **kwargs: "1=1",
    )
    monkeypatch.setattr(agg_query, "can_use_hourly_rollup", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bot_sources,
        "build_well_known_bots_matcher",
        lambda: lambda ua: [{"name": "Googlebot", "category": "search"}] if "Googlebot" in ua else [],
    )

    class _CapturingChTransport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            import re as _re

            raw_body = request.read().decode("utf-8")
            fields = {
                m.group(1): m.group(2)
                for m in _re.finditer(
                    r'Content-Disposition: form-data; name="([^"]+)"\r\n\r\n(.*?)\r\n--',
                    raw_body,
                    flags=_re.S,
                )
            }
            body_sql = fields.get("query", raw_body)
            executed_queries.append((body_sql, fields))
            if "request_aggregates" in body_sql:
                return httpx.Response(200, json={"data": [{"ua": malicious_ua}]})
            return httpx.Response(200, json={"data": [{"ua": malicious_ua, "ip": "66.249.66.1", "cnt": 7}]})

    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=_CapturingChTransport(),
    )
    ch_client._table_filters_verified = True
    svc = SimpleNamespace(service_id="svc-sec07", client=ch_client)
    start_dt = datetime(2026, 11, 19, 0, 0, tzinfo=UTC)
    end_dt = datetime(2026, 11, 19, 0, 15, tzinfo=UTC)
    scope = hs_security._Scope(
        service_id="svc-sec07",
        start=start_dt,
        end=end_dt,
        where="service_id = {service_id:String}",
        params={"service_id": "svc-sec07", "start": start_dt, "end": end_dt},
        filtered=False,
    )

    out = hs_security._wellknown_bots(svc, scope)
    assert len(out) >= 1

    fact_queries = [(sql, params) for sql, params in executed_queries if "FROM request_facts" in sql]
    assert len(fact_queries) == 1
    fact_sql, fact_params = fact_queries[0]
    assert "{wkb_ua_0:String}" in fact_sql
    assert "DROP TABLE" not in fact_sql
    assert "Googlebot" not in fact_sql
    assert "param_wkb_ua_0" in fact_params


# ── SEC-07.2: SQL console literal-safe rewriting & post-rewrite AST gate ────


class _RecordingConsoleClient:
    def __init__(self) -> None:
        self._table_filters_verified = True
        self.last_sql: str = ""
        self.last_settings: dict[str, Any] = {}

    def execute(self, sql: str, params: dict[str, Any] | None = None, *, settings: dict[str, Any] | None = None):
        self.last_sql = sql
        self.last_settings = dict(settings or {})
        return [{"n": 1}]


def test_sec07_query_endpoint_never_rewrites_inside_single_quoted_string_literals():
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)
    # Previously, 'bot status ok' was rewritten into 'bot custom_fields['status'] ok', breaking out of the literal!
    req = DashboardQueryRequest(
        sql="SELECT url, count() AS n FROM logs WHERE ua LIKE '%bot status ok%' AND path = 'logs' AND city = 'client_errors' GROUP BY url",
        max_rows=50,
    )
    hs_query.query_endpoint(
        svc,
        req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert "'%bot status ok%'" in client.last_sql
    assert "'logs'" in client.last_sql
    assert "'client_errors'" in client.last_sql
    assert "FROM request_facts" in client.last_sql
    assert "custom_fields['ua'] LIKE '%bot status ok%'" in client.last_sql


def test_sec07_query_endpoint_preserves_fdt_live_queries_and_rum_top_level_columns():
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)

    # 1. Exact fdt live harness query shape (custom_fields['lt_run'] + bare _FIELD_DIMENSIONS columns)
    fdt_req = DashboardQueryRequest(
        sql="SELECT custom_fields['lt_run'] AS lt_run, count() AS n FROM logs WHERE timestamp >= '2026-11-19T00:00:00Z' AND custom_fields['lt_run'] = 'fdlt-run-1' AND pop = 'DEN' AND status = 200 GROUP BY lt_run",
        max_rows=100,
    )
    hs_query.query_endpoint(
        svc,
        fdt_req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert "FROM request_facts" in client.last_sql
    assert "custom_fields['lt_run'] = 'fdlt-run-1'" in client.last_sql
    assert "custom_fields['pop'] = 'DEN'" in client.last_sql
    assert "custom_fields['status'] = '200'" in client.last_sql
    assert "event_timestamp >= '2026-11-19 00:00:00.000'" in client.last_sql

    # 2. RUM table query: `pop` and `city` are top-level columns on `rum_vitals_facts` (no `custom_fields` column!)
    rum_req = DashboardQueryRequest(
        sql="SELECT pop, city, count() AS n FROM client_vitals GROUP BY pop, city",
        max_rows=100,
    )
    hs_query.query_endpoint(
        svc,
        rum_req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert "FROM rum_vitals_facts" in client.last_sql
    assert "custom_fields['pop']" not in client.last_sql
    assert "SELECT pop, city" in client.last_sql


@pytest.mark.parametrize(
    "bad_sql",
    [
        "SELECT * FROM url('http://169.254.169.254/latest/meta-data/', 'CSV', 'a String')",
        "SELECT * FROM remote('127.0.0.1:9000', 'system', 'tables')",
        "SELECT * FROM s3('https://evil.example/data.parquet')",
        "SELECT * FROM file('/etc/passwd', 'LineAsString')",
        "SELECT * FROM rum_events LIMIT 1",
        "SELECT `url` FROM ``logs`` LIMIT 1",
        "SELECT url FROM logs -- trailing comment",
    ],
)
def test_sec07_query_endpoint_rejects_unsafe_table_functions_internal_tables_and_comments(bad_sql):
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)
    req = DashboardQueryRequest(sql=bad_sql, max_rows=10)
    with pytest.raises(PermissionError):
        hs_query.query_endpoint(
            svc,
            req,
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=False,
        )
    assert client.last_sql == ""


# ── Iteration 2 Hardening Regressions (V4, C3, F3) ────────────────────────────


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("raw_table", "expected_fact_table"),
    [
        ("request_facts", "request_facts"),
        ("rum_vitals_facts", "rum_vitals_facts"),
        ("rum_error_facts", "rum_error_facts"),
        ("rum_page_load_facts", "rum_vitals_facts"),
    ],
)
def test_sec07_direct_fact_tables_and_rum_page_load_facts_enforce_table_filters(
    raw_table: str, expected_fact_table: str
) -> None:
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)
    req = DashboardQueryRequest(sql=f"SELECT count() AS n FROM {raw_table}", max_rows=10)
    hs_query.query_endpoint(
        svc,
        req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert f"FROM {expected_fact_table}" in client.last_sql
    atf_raw = client.last_settings.get("additional_table_filters")
    assert isinstance(atf_raw, str)
    atf = ast.literal_eval(atf_raw)
    assert isinstance(atf, dict) and expected_fact_table in atf
    assert "svc_alpha" in atf[expected_fact_table]
    assert "high_scale_batch_publications FINAL" in atf[expected_fact_table]

    view_name = f"masked_{expected_fact_table}_v1"
    hs_query.query_endpoint(
        svc,
        req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={expected_fact_table: view_name},
    )
    assert f"FROM {view_name}" in client.last_sql
    view_atf_raw = client.last_settings.get("additional_table_filters")
    assert isinstance(view_atf_raw, str)
    view_atf = ast.literal_eval(view_atf_raw)
    assert view_name in view_atf
    assert "event_timestamp >=" in view_atf[view_name]


@pytest.mark.security_regression
def test_sec07_subquery_scoped_rewriting_aliases_map_keys_and_union_all() -> None:
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)

    # 1. Preserves custom_fields['logs'] / custom_fields['client_vitals'] map keys and AS aliases
    alias_req = DashboardQueryRequest(
        sql="SELECT custom_fields['logs'] AS lg, custom_fields['client_vitals'] AS cv, url as pop, status AS  city FROM logs",
        max_rows=20,
    )
    hs_query.query_endpoint(svc, alias_req, "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False)
    assert "custom_fields['logs']" in client.last_sql
    assert "custom_fields['client_vitals']" in client.last_sql
    assert "url as pop" in client.last_sql
    assert "custom_fields['status'] AS  city" in client.last_sql

    # 2. UNION ALL scopes column rewriting per SELECT branch (RUM native pop/city vs logs custom_fields)
    union_req = DashboardQueryRequest(
        sql="SELECT pop, city FROM client_vitals UNION ALL SELECT pop, city FROM logs",
        max_rows=20,
    )
    hs_query.query_endpoint(svc, union_req, "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False)
    assert "SELECT pop, city FROM rum_vitals_facts" in client.last_sql
    assert "SELECT custom_fields['pop'], custom_fields['city'] FROM request_facts" in client.last_sql
    atf_raw = client.last_settings.get("additional_table_filters")
    assert isinstance(atf_raw, str)
    atf = ast.literal_eval(atf_raw)
    assert isinstance(atf, dict)
    assert "rum_vitals_facts" in atf and "request_facts" in atf


@pytest.mark.security_regression
def test_sec07_query_endpoint_isolates_tables_cache_key_and_redacts_18_pii_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    hs_query._HS_QUERY_RESULT_CACHE.clear()

    calls: list[str] = []

    class _RowClient:
        _table_filters_verified = True

        def execute(self, sql: str, params: dict[str, Any] | None = None, *, settings: dict[str, Any] | None = None):
            calls.append(sql)
            return [
                {
                    "oip": "151.101.1.9",
                    "c_ip": "198.51.100.7",
                    "edge_sid": "sid-secret-1",
                    "cookie_session": "cook-secret-1",
                    "arbitrary_alias": "203.0.113.55",
                }
            ]

    svc = SimpleNamespace(service_id="svc_alpha", client=_RowClient())
    req = DashboardQueryRequest(sql="SELECT count() AS n FROM logs", max_rows=10)

    out1 = hs_query.query_endpoint(
        svc,
        req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "masked_req_v1"},
    )
    out2 = hs_query.query_endpoint(
        svc,
        req,
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "masked_req_v2"},
    )
    assert len(calls) == 2
    assert "FROM masked_req_v1" in calls[0]
    assert "FROM masked_req_v2" in calls[1]
    row = out2["data"][0]
    assert row["oip"] == "[redacted]"
    assert row["c_ip"] == "[redacted]"
    assert row["edge_sid"] == "[redacted]"
    assert row["cookie_session"] == "[redacted]"
    assert row["arbitrary_alias"] == "203.0.113.xxx"
    hs_query._HS_QUERY_RESULT_CACHE.clear()
