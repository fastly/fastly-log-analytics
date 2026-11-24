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
        lambda: (
            lambda ua: (
                [{"id": "google-crawler", "name": "Googlebot", "category": "search"}] if "Googlebot" in ua else []
            )
        ),
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


# ── Iteration 3 Hardening Regressions (SEC-07.2 Finding 1 & Challenges 1, 4) ──


@pytest.mark.security_regression
def test_sec07_comma_lateral_nested_union_and_parenthesized_cte_rewrite_and_filter() -> None:
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)
    masked_tables = {
        "request_facts": "fla_cv_m_req",
        "rum_vitals_facts": "fla_cv_m_vit",
        "rum_error_facts": "fla_cv_m_err",
    }

    # 1. Comma join across RUM + request_facts under masked tables map
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT r.client_ip, v.pop FROM client_vitals v, request_facts r", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables=masked_tables,
    )
    assert "FROM fla_cv_m_vit v, fla_cv_m_req r" in client.last_sql
    assert "v.pop" in client.last_sql
    atf1 = ast.literal_eval(client.last_settings["additional_table_filters"])
    assert set(atf1) == {"fla_cv_m_vit", "fla_cv_m_req"}

    # 2. LATERAL join under masked tables map
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT b.client_ip FROM logs a CROSS JOIN LATERAL request_facts b", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables=masked_tables,
    )
    assert "FROM fla_cv_m_req a CROSS JOIN LATERAL fla_cv_m_req b" in client.last_sql
    assert "request_facts" not in client.last_sql.replace("fla_cv_m_req", "")
    atf2 = ast.literal_eval(client.last_settings["additional_table_filters"])
    assert set(atf2) == {"fla_cv_m_req"}

    # 3. Nested parenthesized UNION ALL subquery
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(
            sql="SELECT * FROM ((SELECT url FROM logs) UNION ALL (SELECT page_url FROM rum_page_load_facts))",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables=masked_tables,
    )
    assert "FROM fla_cv_m_req" in client.last_sql
    assert "FROM fla_cv_m_vit" in client.last_sql
    assert "logs" not in client.last_sql and "rum_page_load_facts" not in client.last_sql
    atf3 = ast.literal_eval(client.last_settings["additional_table_filters"])
    assert set(atf3) == {"fla_cv_m_req", "fla_cv_m_vit"}

    # 4. Parenthesized main query after multi-CTE header
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(
            sql="WITH a AS (SELECT count() AS c FROM client_vitals), b AS (SELECT c FROM a) (SELECT client_ip, url FROM request_facts)",
            max_rows=10,
        ),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables=masked_tables,
    )
    assert "FROM fla_cv_m_vit" in client.last_sql
    assert "FROM fla_cv_m_req" in client.last_sql
    assert "request_facts" not in client.last_sql.replace("fla_cv_m_req", "")
    atf4 = ast.literal_eval(client.last_settings["additional_table_filters"])
    assert set(atf4) == {"fla_cv_m_vit", "fla_cv_m_req"}

    # 5. Admin comma self-join (tables=None)
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT a.url, b.url FROM logs a, logs b WHERE a.req_id = b.req_id", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert "FROM request_facts a, request_facts b" in client.last_sql
    assert "logs" not in client.last_sql

    # 6. Case-insensitive custom_fields rewrite on logs vs native RUM columns
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT POP, CITY, STATUS FROM logs", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert (
        "SELECT custom_fields['pop'], custom_fields['city'], custom_fields['status'] FROM request_facts"
        in client.last_sql
    )

    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT POP, CITY FROM client_vitals", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    assert "SELECT POP, CITY FROM rum_vitals_facts" in client.last_sql


@pytest.mark.security_regression
def test_sec07_post_allowed_tables_fails_closed_on_unmapped_tables_and_clickhouse_differentials() -> None:
    client = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=client)

    # Partial tables mapping must fail closed if query touches an unmapped domain
    with pytest.raises(PermissionError):
        hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql="SELECT count() AS n FROM client_vitals", max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables={"request_facts": "fla_cv_m_req"},
        )
    assert client.last_sql == ""

    # ClickHouse parser differentials: FROM (tbl) and bare IN tbl must be rejected before ClickHouse
    for bad_sql in (
        "SELECT * FROM (high_scale_batch_publications)",
        "SELECT * FROM (request_facts)",
        "SELECT url FROM logs WHERE service_id IN high_scale_batch_publications",
        "SELECT url FROM logs WHERE client_ip IN request_facts",
    ):
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=bad_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
            )
        assert client.last_sql == ""


@pytest.mark.security_regression
def test_sec07_query_endpoint_redacts_nested_custom_fields_and_embedded_url_ips_when_masked() -> None:
    class _NestedRowClient:
        _table_filters_verified = True

        def execute(self, sql: str, params: dict[str, Any] | None = None, *, settings: dict[str, Any] | None = None):
            return [
                {
                    "custom_fields": {
                        "cookie_session": "nested-cs-secret",
                        "edge_sid": "nested-sid",
                        "oip": "151.101.1.10",
                        "c_ip": "198.51.100.8",
                    },
                    "url": "/cb?client=198.51.100.44",
                    "ua": "Mozilla/5.0 Chrome/131.0.0.0 client=2001:db8:abcd::99",
                }
            ]

    svc = SimpleNamespace(service_id="svc_alpha", client=_NestedRowClient())
    out = hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT custom_fields, url, ua FROM logs", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
        tables={"request_facts": "fla_cv_m_req"},
    )
    row = out["data"][0]
    assert row["custom_fields"]["cookie_session"] == "[redacted]"
    assert row["custom_fields"]["edge_sid"] == "[redacted]"
    assert row["custom_fields"]["oip"] == "151.101.1.xxx"
    assert row["custom_fields"]["c_ip"] == "198.51.100.xxx"
    assert row["url"] == "/cb?client=198.51.100.xxx"
    assert row["ua"] == "Mozilla/5.0 Chrome/131.0.0.0 client=2001:db8:abcd::"


@pytest.mark.security_regression
def test_sec08_console_access_masked_keys_matches_18_shared_pii_keys_and_masks_underscore_origin_keys() -> None:
    from backend.core.share_db import SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access

    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)
    assert console_access.MASKED_KEYS == tuple(sorted(SHARED_PII_KEYS))

    view_sql = console_access._view_select(
        "fla_db",
        "request_facts",
        [
            ("service_id", "String"),
            ("client_ip", "String"),
            ("custom_fields", "Map(String, String)"),
        ],
        "svc_alpha",
        masked=True,
    )
    for key in ("c_ip", "fastly_client_ip", "oip", "true_client_ip", "x_forwarded_for"):
        assert f"'{key}'" in view_sql, f"Masked ClickHouse view missing PII key {key!r}"


@pytest.mark.security_regression
def test_sec_t5_iter24_05_console_access_cmcd_alias_masking_and_aliased_predicate_coercion() -> None:
    from backend.config import ClickHouseConfig
    from backend.core.share_db import CMCD_SESSION_ALIAS_KEYS, SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access
    from backend.high_scale.query import _rewrite_console_sql

    # 1. Exact 18-key contract on MASKED_KEYS preserved, while Map redaction covers CMCD_SESSION_ALIAS_KEYS + dotted forms:
    assert len(console_access.MASKED_KEYS) == 18
    assert console_access.MASKED_KEYS == tuple(sorted(SHARED_PII_KEYS))
    columns = [
        ("service_id", "String"),
        ("publication_state", "String"),
        ("batch_id", "String"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("url", "String"),
        ("client_ip", "String"),
        ("custom_fields", "Map(String, String)"),
        ("cmcd", "Map(String, String)"),
    ]
    masked_view_sql = console_access._view_select("default", "request_facts", columns, "svc_a", masked=True)
    for alias_key in (*sorted(CMCD_SESSION_ALIAS_KEYS), "cmcd.sid", "cmcd.cid", "cmcd.session"):
        assert f"'{alias_key}'" in masked_view_sql, f"Masked Map view DDL missing {alias_key!r}"

    # Verify ConsoleClients._assert_isolated probes cmcd['cmcd_sid'|'cmcd_cid'|'cmcd_session'] & dotted forms too:
    seen_queries: list[str] = []

    class _FakeCH:
        def execute(self, sql: str, *args: object, **kwargs: object) -> list[dict[str, int]]:
            seen_queries.append(sql)
            return [{"n": 0}]

        def close(self) -> None:
            pass

    secret = bytes(range(40, 88))
    clients = console_access.ConsoleClients(
        settings=ClickHouseConfig(host="127.0.0.1", port=8123, database="default", user="default", password=""),
        secret=secret,
        client_factory=lambda _cfg: _FakeCH(),
    )
    assert clients.client_for("svc_a", masked=True) is not None
    req_mask_check = next(q for q in seen_queries if "request_facts" in q and "client_ip !=" in q)
    for check_expr in (
        "cmcd['cmcd_sid']",
        "cmcd['cmcd_cid']",
        "cmcd['cmcd_session']",
        "cmcd['cmcd.sid']",
        "custom_fields['cmcd_sid']",
    ):
        assert check_expr in req_mask_check, f"_assert_isolated missing {check_expr} check in {req_mask_check}"

    # 2. Aliased predicate coercion on JOIN queries must NOT emit invalid `a.lower(...)` or `a.toFloat64OrZero(...)`:
    rewritten = _rewrite_console_sql(
        "SELECT a.url, b.status FROM logs AS a JOIN logs AS b ON a.url = b.url WHERE a.edge = true AND a.status >= 200",
        svc_table="`default`.`fla_cv_masked`",
        names={"request_facts": "fla_cv_masked"},
    )
    assert "a.lower(" not in rewritten
    assert "a.toFloat64OrZero(" not in rewritten
    assert "lower(a.custom_fields['edge']) IN ('true', '1')" in rewritten
    assert "toFloat64OrZero(a.custom_fields['status']) >= 200" in rewritten


@pytest.mark.security_regression
def test_bug_hs_console_cf_pii_01_excludes_materialized_cf_columns_and_redacts_defense_in_depth() -> None:
    """BUG-HS-CONSOLE-CF-PII-01: Exclude MATERIALIZED/ALIAS _cf_* columns in provision_console_access & defend all layers."""
    import re
    from pathlib import Path

    from backend.config import ClickHouseConfig
    from backend.core.share_db import apply_pii_policy, is_saved_view_pii_key
    from backend.high_scale.postgres_control import console_access

    assert len(console_access.MASKED_KEYS) == 18

    # Parse real request_schema.sql DESCRIBE TABLE shape (15 physical + 74 MATERIALIZED _cf_* columns):
    schema_sql = (Path(__file__).resolve().parents[2] / "backend/high_scale/sql/request_schema.sql").read_text(
        encoding="utf-8"
    )
    create_match = re.search(r"CREATE TABLE IF NOT EXISTS request_facts\s*\((.*?)\)\s*ENGINE", schema_sql, re.S)
    assert create_match is not None
    describe_rows: list[dict[str, str]] = []
    depth = 0
    buf = ""
    for ch in create_match.group(1):
        depth += (ch == "(") - (ch == ")")
        if ch == "," and depth == 0:
            col_parts = buf.strip().split(None, 1)
            if len(col_parts) == 2:
                describe_rows.append({"name": col_parts[0].strip("`"), "type": col_parts[1], "default_type": ""})
            buf = ""
        else:
            buf += ch
    if buf.strip():
        col_parts = buf.strip().split(None, 1)
        if len(col_parts) == 2:
            describe_rows.append({"name": col_parts[0].strip("`"), "type": col_parts[1], "default_type": ""})
    for m_mat in re.finditer(
        r"ADD COLUMN IF NOT EXISTS `(_cf_[a-zA-Z0-9_]+)`\s+([^\s]+(?:\([^)]*\))?)\s+MATERIALIZED",
        schema_sql,
    ):
        describe_rows.append({"name": m_mat.group(1), "type": m_mat.group(2), "default_type": "MATERIALIZED"})
    describe_rows.append({"name": "_alias_secret", "type": "String", "default_type": "ALIAS"})
    assert sum(1 for r in describe_rows if r["name"].startswith("_cf_")) == 74

    class _SchemaAdminCH:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, *_a: Any, **_kw: Any) -> list[dict[str, str]]:
            self.statements.append(sql)
            if sql.startswith("DESCRIBE TABLE"):
                if "request_facts" in sql:
                    return list(describe_rows)
                return [
                    {"name": "service_id", "type": "LowCardinality(String)", "default_type": ""},
                    {"name": "client_id", "type": "String", "default_type": ""},
                ]
            if "FROM system.tables" in sql:
                all_views = [
                    *console_access.console_tables("svc_a", True).values(),
                    *console_access.console_tables("svc_a", False).values(),
                ]
                return [{"name": v} for v in all_views]
            return []

    secret = bytes(range(32, 80))
    admin = _SchemaAdminCH()
    console_access.provision_console_access(admin, "default", "svc_a", secret)
    masked_req_view = console_access.console_tables("svc_a", masked=True)["request_facts"]
    create_masked_stmt = next(s for s in admin.statements if f"CREATE VIEW `default`.`{masked_req_view}`" in s)
    assert "`_cf_" not in create_masked_stmt
    assert "_alias_secret" not in create_masked_stmt

    # Defense-in-depth in _redaction: scalar _cf_<pii> and cmcd_<pii> columns are still masked if ever passed:
    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    for col_name in ("_cf_cookie_session", "_cf_cid", "_cf_edge_sid", "_cf_oip", "_cf_cmcd_sid", "cmcd_sid"):
        red_expr = console_access._redaction(col_name, "LowCardinality(String)", keys_sql)
        assert red_expr is not None and "[redacted]" in red_expr, f"Expected scalar redaction for {col_name}"
    assert console_access._redaction("_cf_pop", "LowCardinality(String)", keys_sql) is None

    # Defense-in-depth in _assert_isolated: fails closed if DESCRIBE TABLE on masked view reveals any _cf_* column:
    class _LeakyCfViewCH:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, sql: str, *_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            if sql.startswith("DESCRIBE TABLE"):
                return [
                    {"name": "service_id", "type": "String"},
                    {"name": "_cf_cookie_session", "type": "LowCardinality(String)"},
                ]
            return [{"n": 0}]

        def close(self) -> None:
            self.closed = True

    leaky_cf = _LeakyCfViewCH()
    leaky_mgr = console_access.ConsoleClients(
        settings=ClickHouseConfig(host="127.0.0.1", port=8123, database="default", user="default", password=""),
        secret=secret,
        client_factory=lambda _cfg: leaky_cf,
    )
    with pytest.raises(RuntimeError, match="masking"):
        leaky_mgr.client_for("svc_a", masked=True)
    assert leaky_cf.closed is True

    # Defense-in-depth in share_db + query_endpoint: _cf_<pii> keys are classified & redacted:
    for pii_cf in ("_cf_cookie_session", "_cf_cid", "_cf_edge_sid", "_cf_oip", "_cf_cmcd_sid"):
        assert is_saved_view_pii_key(pii_cf) is True, f"Expected {pii_cf} to be classified as PII"
    raw_cf_rows = [
        {
            "_cf_cookie_session": "SECRET_SESS_TOKEN_999",
            "_cf_cid": "SECRET_CID_888",
            "_cf_edge_sid": "SECRET_EDGE_SID_777",
            "_cf_oip": "198.51.100.77",
            "_cf_pop": "DEN",
        }
    ]
    assert apply_pii_policy([dict(raw_cf_rows[0])], {"mask_ips": True}) == [
        {
            "_cf_cookie_session": "[redacted]",
            "_cf_cid": "[redacted]",
            "_cf_edge_sid": "[redacted]",
            "_cf_oip": "198.51.100.xxx",
            "_cf_pop": "DEN",
        }
    ]

    class _RawCfRowClient:
        _table_filters_verified = True

        def execute(self, *_a: Any, **_kw: Any) -> list[dict[str, str]]:
            return [dict(raw_cf_rows[0])]

    ep_res = hs_query.query_endpoint(
        SimpleNamespace(service_id="svc_alpha", client=_RawCfRowClient()),
        DashboardQueryRequest(sql="SELECT * FROM logs LIMIT 1", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
    )
    assert ep_res["data"] == [
        {
            "_cf_cookie_session": "[redacted]",
            "_cf_cid": "[redacted]",
            "_cf_edge_sid": "[redacted]",
            "_cf_oip": "[redacted]",
            "_cf_pop": "DEN",
        }
    ]

    # Defense-in-depth in query.py: direct _cf_* identifiers raise ValueError (400 on /api/query),
    # while string literals '_cf_cid' and comments -- _cf_cid / /* _cf_cid */ do not trigger _cf_* rejection:
    rec = _RecordingConsoleClient()
    svc = SimpleNamespace(service_id="svc_alpha", client=rec)
    for bad_cf_sql in (
        "SELECT _cf_cid FROM logs",
        "SELECT a._cf_cookie_session FROM logs AS a",
        "SELECT url FROM logs WHERE _cf_oip != ''",
    ):
        with pytest.raises(ValueError, match="_cf_"):
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=bad_cf_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
            )
    hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT '_cf_cid' AS sample_lit, url FROM logs", max_rows=10),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=True,
    )
    assert "'_cf_cid'" in rec.last_sql
    comment_rewritten = hs_query._rewrite_console_sql(
        "SELECT url -- _cf_cid\n/* _cf_cookie_session */ FROM logs",
        svc_table="svc_alpha",
        names={"request_facts": "fla_cv_masked"},
    )
    assert "FROM fla_cv_masked" in comment_rewritten

    # Defense-in-depth in ClickHouseClient.rewrite_publication_fence: word-bounded request_facts check:
    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={"data": []})),
    )
    ch_client._enable_cf_rewrite_in_tests = True
    view_query = "SELECT custom_fields['url'], custom_fields['cookie_session'] FROM fla_cv_1234_m_request_facts"
    assert ch_client.rewrite_publication_fence(view_query) == view_query
    table_query = "SELECT custom_fields['pop'] FROM request_facts"
    assert "`_cf_pop`" in ch_client.rewrite_publication_fence(table_query)


@pytest.mark.security_regression
def test_bug_t5_iter25_05_catalog_cf_fields_dquote_aliases_and_cte_subquery_rewriting() -> None:
    """BUG-T5-ITER25-05: Catalog CF fields, ANSI double-quoted aliases, and CTE/subquery projection & * scoping."""
    from backend.high_scale.query import _rewrite_console_sql

    names = {"request_facts": "hs_test.v_req_m"}

    # 1. Catalog/DuckDB request fields (bot_score, elapsed, ost, etc.) & boolean fields (is_bot, ipv6, etc.):
    q_join = _rewrite_console_sql(
        "SELECT a.url FROM logs AS a JOIN logs AS b ON a.url = b.url "
        "WHERE a.edge = true AND a.status >= 200 AND a.bot_score > 50 AND a.waf = false AND a.is_bot = true",
        svc_table="svc_alpha",
        names=names,
    )
    assert "toFloat64OrZero(a.custom_fields['bot_score']) > 50" in q_join
    assert "lower(a.custom_fields['is_bot']) IN ('true', '1')" in q_join

    # 2. ANSI double-quoted table aliases and double-quoted column alias masking:
    q_dquote_tbl = _rewrite_console_sql(
        'SELECT "a".url FROM logs AS "a" WHERE "a".edge = true AND "a".status >= 200',
        svc_table="svc_alpha",
        names=names,
    )
    assert '"a".lower(' not in q_dquote_tbl
    assert '"a".toFloat64OrZero(' not in q_dquote_tbl
    assert "lower(\"a\".custom_fields['edge']) IN ('true', '1')" in q_dquote_tbl
    assert "toFloat64OrZero(\"a\".custom_fields['status']) >= 200" in q_dquote_tbl

    q_dquote_col = _rewrite_console_sql(
        "SELECT url AS \"custom_fields['edge'] = true\" FROM logs WHERE edge = true",
        svc_table="svc_alpha",
        names=names,
    )
    assert "AS \"custom_fields['edge'] = true\"" in q_dquote_col
    assert "WHERE lower(custom_fields['edge']) IN ('true', '1')" in q_dquote_col

    # 3. CTEs & FROM (...) subqueries: standalone custom fields get AS <col> inside CTE/subquery,
    # and outer consumers only rewrite to custom_fields[...] when inner query projects *!
    q_cte = _rewrite_console_sql(
        "WITH q AS (SELECT url, status FROM logs WHERE edge = true AND is_ipv6 = true) SELECT url FROM q WHERE status = 200",
        svc_table="svc_alpha",
        names=names,
    )
    assert "custom_fields['status'] AS status" in q_cte
    outer_cte = q_cte.split("SELECT url FROM q", 1)[1]
    assert "custom_fields" not in outer_cte
    assert "status = '200'" in outer_cte

    q_sub = _rewrite_console_sql(
        "SELECT sub.url FROM (SELECT status, url FROM logs) AS sub WHERE sub.status >= 200",
        svc_table="svc_alpha",
        names=names,
    )
    assert "custom_fields['status'] AS status" in q_sub
    outer_sub = q_sub.rsplit("WHERE", 1)[1]
    assert "custom_fields" not in outer_sub
    assert "toFloat64OrZero(sub.status) >= 200" in outer_sub

    q_star_cte = _rewrite_console_sql(
        "WITH q AS (SELECT * FROM logs WHERE edge = true) SELECT q.url, q.bot_score FROM q WHERE q.status = 200",
        svc_table="svc_alpha",
        names=names,
    )
    outer_star = q_star_cte.split(") SELECT", 1)[1]
    assert "q.custom_fields['bot_score']" in outer_star
    assert "q.custom_fields['status'] = '200'" in outer_star


@pytest.mark.security_regression
def test_bug_t5_iter26_01_and_04_admin_console_cf_fence_and_boolean_alias_col_list_parity() -> None:
    """BUG-T5-ITER26-01 & BUG-T5-ITER26-04: Admin SQL Console avoids `_cf_*` rewrite and handles aliased bools, CTE/subquery column lists, and !=/."""
    from backend.high_scale.query import _rewrite_console_sql

    # 1. BUG-T5-ITER26-01: Admin SQL Console (`tables=None`, `_enable_cf_rewrite_in_tests=True`)
    # must NOT rewrite q.custom_fields['pop'] -> q.`_cf_pop` or top-level custom_fields['pop'] -> `_cf_pop`:
    captured_queries: list[str] = []

    def _ch_handler(req: httpx.Request) -> httpx.Response:
        body_text = req.content.decode("utf-8", errors="replace")
        if "SELECT 1" in body_text and "additional_table_filters" in str(req.url):
            return httpx.Response(200, json={"data": [{"1": 1}]})
        captured_queries.append(body_text)
        return httpx.Response(200, json={"data": [{"url": "/ok", "pop": "DEN"}]})

    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(_ch_handler),
    )
    ch_client._enable_cf_rewrite_in_tests = True
    ch_client._table_filters_verified = True  # type: ignore[attr-defined]
    try:
        svc = SimpleNamespace(service_id="svc_alpha", client=ch_client)
        for admin_sql in (
            "WITH q AS (SELECT * FROM logs) SELECT q.url, q.pop FROM q WHERE q.status = 200 ORDER BY q.url",
            "SELECT sub.url, sub.pop FROM (SELECT * FROM logs) AS sub WHERE sub.status = 200 ORDER BY sub.url",
            "SELECT url, pop, status FROM logs WHERE status = 200",
        ):
            captured_queries.clear()
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=admin_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
                tables=None,
            )
            assert len(captured_queries) == 1
            assert "`_cf_" not in captured_queries[0], (
                f"Leaked _cf_ rewrite in Admin SQL Console: {captured_queries[0]}"
            )
            assert "custom_fields['pop']" in captured_queries[0]
    finally:
        ch_client.close()

    # 2. BUG-T5-ITER26-04: Aliased boolean custom fields in CTEs/subqueries, explicit CTE/subquery column lists, and != / <> bool ops:
    names = {"request_facts": "hs_test.v_req_m"}

    q_bool_cte = _rewrite_console_sql(
        "WITH q AS (SELECT url, edge AS is_edge_hit FROM logs) SELECT url FROM q WHERE is_edge_hit = true ORDER BY url",
        svc_table="svc_alpha",
        names=names,
    )
    assert "lower(is_edge_hit) IN ('true', '1')" in q_bool_cte

    q_bool_sub = _rewrite_console_sql(
        "SELECT sub.url FROM (SELECT url, is_bot AS bot_flag FROM logs) AS sub WHERE sub.bot_flag = true ORDER BY sub.url",
        svc_table="svc_alpha",
        names=names,
    )
    assert "lower(sub.bot_flag) IN ('true', '1')" in q_bool_sub

    q_cte_cols = _rewrite_console_sql(
        "WITH q(u, st, ed) AS (SELECT url, status, edge FROM logs) SELECT u FROM q WHERE st = 200 AND ed = true ORDER BY u",
        svc_table="svc_alpha",
        names=names,
    )
    outer_cte_cols = q_cte_cols.split("SELECT u FROM q", 1)[1]
    assert "st = '200'" in outer_cte_cols
    assert "lower(ed) IN ('true', '1')" in outer_cte_cols

    q_sub_cols = _rewrite_console_sql(
        "SELECT sub.u FROM (SELECT url, status, edge FROM logs) AS sub(u, st, ed) WHERE sub.st = 200 AND sub.ed != false ORDER BY sub.u",
        svc_table="svc_alpha",
        names=names,
    )
    outer_sub_cols = q_sub_cols.rsplit("WHERE", 1)[1]
    assert "sub.st = '200'" in outer_sub_cols
    assert "lower(sub.ed) IN ('true', '1')" in outer_sub_cols

    for raw_ne, expected_pred in (
        ("SELECT url FROM logs WHERE edge != false", "lower(custom_fields['edge']) IN ('true', '1')"),
        ("SELECT url FROM logs WHERE edge <> false", "lower(custom_fields['edge']) IN ('true', '1')"),
        ("SELECT url FROM logs WHERE edge != 'false'", "lower(custom_fields['edge']) IN ('true', '1')"),
        ("SELECT url FROM logs WHERE edge != true", "lower(custom_fields['edge']) NOT IN ('true', '1')"),
        ("SELECT url FROM logs WHERE edge <> true", "lower(custom_fields['edge']) NOT IN ('true', '1')"),
        ("SELECT url FROM logs WHERE edge <> 'true'", "lower(custom_fields['edge']) NOT IN ('true', '1')"),
    ):
        rewritten_ne = _rewrite_console_sql(raw_ne, svc_table="svc_alpha", names=names)
        assert expected_pred in rewritten_ne, f"Expected {expected_pred!r} in {rewritten_ne!r}"


@pytest.mark.security_regression
def test_bug_t5_iter27_02_and_03_console_col_lists_unsafe_fn_guard_and_derived_parity() -> None:
    """BUG-T5-ITER27-02 & BUG-T5-ITER27-03: End-to-end query_endpoint CTE/subquery column lists and derived CF coercions."""
    captured_queries: list[str] = []

    def _ch_handler(req: httpx.Request) -> httpx.Response:
        body_text = req.content.decode("utf-8", errors="replace")
        if "SELECT 1" in body_text and "additional_table_filters" in str(req.url):
            return httpx.Response(200, json={"data": [{"1": 1}]})
        captured_queries.append(body_text)
        return httpx.Response(200, json={"data": [{"u": "/ok", "url": "/ok"}]})

    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(_ch_handler),
    )
    ch_client._enable_cf_rewrite_in_tests = True
    ch_client._table_filters_verified = True  # type: ignore[attr-defined]
    try:
        svc = SimpleNamespace(service_id="svc_alpha", client=ch_client)
        from backend.high_scale.postgres_control import console_access

        role_configs: list[tuple[bool, dict[str, str] | None]] = [
            (False, None),
            (False, console_access.console_tables("svc_alpha", masked=False)),
            (True, console_access.console_tables("svc_alpha", masked=True)),
        ]

        # 1. BUG-T5-ITER27-02: CTE & FROM/JOIN column-list aliases execute end-to-end across Admin, Analyst Raw, and Analyst Masked:
        col_list_queries = (
            "WITH q(u, st, ed) AS (SELECT url, status, edge FROM logs) SELECT u FROM q WHERE st = 200 AND ed = true ORDER BY u",
            "WITH a(u, st) AS (SELECT url, status FROM logs), b(u2, st2) AS (SELECT u, st FROM a) SELECT u2 FROM b WHERE st2 = 200 ORDER BY u2",
            "SELECT sub.u FROM (SELECT url, status, edge FROM logs) AS sub(u, st, ed) WHERE sub.st = 200 AND sub.ed != true ORDER BY sub.u",
            "SELECT sub.u FROM (SELECT url, status, edge FROM logs) sub(u, st, ed) WHERE sub.st = 200 AND sub.ed != false ORDER BY sub.u",
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q AS t(u, st) WHERE t.st = 200 ORDER BY t.u",
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q t(u, st) WHERE t.st = 200 ORDER BY t.u",
        )
        for mask_ips, tables in role_configs:
            for sql in col_list_queries:
                captured_queries.clear()
                res = hs_query.query_endpoint(
                    svc,
                    DashboardQueryRequest(sql=sql, max_rows=10),
                    "2026-11-19T00:00:00Z",
                    "2026-11-19T01:00:00Z",
                    mask_ips=mask_ips,
                    tables=tables,
                )
                assert len(res["data"]) == 1
                assert len(captured_queries) == 1

        # Genuine forbidden functions and table functions remain 100% blocked:
        for forbidden_sql in (
            "SELECT file('etc/passwd', 'CSV') FROM logs",
            "SELECT * FROM file(a, b)",
            "SELECT * FROM logs JOIN url(a, b) ON 1 = 1",
            "SELECT * FROM logs LEFT JOIN remote(a, b) ON 1 = 1",
            "SELECT * FROM logs, s3(a, b)",
            "WITH q(u) AS (SELECT file(url) FROM logs) SELECT u FROM q",
            "SELECT url FROM logs WHERE remote(a, b) = 1",
        ):
            with pytest.raises((PermissionError, ValueError)):
                hs_query._reject_unsafe_functions(forbidden_sql)
            with pytest.raises((PermissionError, ValueError)):
                hs_query.query_endpoint(
                    svc,
                    DashboardQueryRequest(sql=forbidden_sql, max_rows=10),
                    "2026-11-19T00:00:00Z",
                    "2026-11-19T01:00:00Z",
                    mask_ips=False,
                    tables=None,
                )

        # 2. BUG-T5-ITER27-03: Mixed SELECT * + aliases, JOIN with logs, bare column aliases,
        # chained SELECT * / q1.* into column-list CTEs, IS [NOT] TRUE/FALSE, and reversed literals:
        coercion_cases: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = [
            (
                "WITH q AS (SELECT *, edge AS b1, status AS st1 FROM logs) SELECT url FROM q WHERE b1 = true AND st1 = 200 ORDER BY url",
                ("lower(b1) IN ('true', '1')", "st1 = '200'"),
                (),
            ),
            (
                "SELECT url FROM (SELECT *, edge AS b1, status AS st1 FROM logs) AS sub WHERE b1 = true AND st1 = 200 ORDER BY url",
                ("lower(b1) IN ('true', '1')", "st1 = '200'"),
                (),
            ),
            (
                "WITH q AS (SELECT url AS qurl, edge AS b1, status AS st1 FROM logs) SELECT l.url FROM logs l JOIN q ON l.url = q.qurl WHERE b1 = true AND st1 = 200 ORDER BY l.url",
                ("lower(b1) IN ('true', '1')", "st1 = '200'"),
                (),
            ),
            (
                "WITH q AS (SELECT url, status st, edge b1 FROM logs) SELECT url FROM q WHERE st = 200 AND b1 = true ORDER BY url",
                ("st = '200'", "lower(b1) IN ('true', '1')"),
                (),
            ),
            (
                "SELECT sub.url FROM (SELECT url, status st, edge b1 FROM logs) sub WHERE sub.st = 200 AND sub.b1 = true ORDER BY sub.url",
                ("sub.st = '200'", "lower(sub.b1) IN ('true', '1')"),
                (),
            ),
            (
                "WITH q1 AS (SELECT url, status, edge FROM logs), q2(u, st, ed) AS (SELECT * FROM q1) SELECT u FROM q2 WHERE st = 200 AND ed = true ORDER BY u",
                ("st = '200'", "lower(ed) IN ('true', '1')"),
                (),
            ),
            (
                "WITH q1 AS (SELECT url, status FROM logs), q2(u, st, ed) AS (SELECT q1.*, true FROM q1) SELECT u FROM q2 WHERE st = 200 AND ed = true ORDER BY u",
                ("st = '200'",),
                ("lower(ed)",),
            ),
            (
                "WITH q AS (SELECT url, edge AS b1, status AS st1 FROM logs) SELECT url FROM q WHERE true = b1 AND 200 = st1 AND 100 < st1 AND b1 IS TRUE ORDER BY url",
                ("lower(b1) IN ('true', '1')", "'200' = st1", "100 < toFloat64OrZero(st1)"),
                (),
            ),
            (
                "SELECT url FROM logs WHERE edge IS TRUE AND is_bot IS NOT TRUE AND waf IS FALSE AND is_ipv6 IS NOT FALSE AND true = edge AND 200 = status AND 500 > status",
                (
                    "lower(custom_fields['edge']) IN ('true', '1')",
                    "lower(custom_fields['is_bot']) NOT IN ('true', '1')",
                    "lower(custom_fields['waf']) IN ('false', '0')",
                    "lower(custom_fields['is_ipv6']) NOT IN ('false', '0')",
                    "'200' = custom_fields['status']",
                    "500 > toFloat64OrZero(custom_fields['status'])",
                ),
                (),
            ),
        ]
        for sql, expected_subs, forbidden_subs in coercion_cases:
            captured_queries.clear()
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
                tables=None,
            )
            assert len(captured_queries) == 1
            emitted = captured_queries[0]
            for exp in expected_subs:
                assert exp in emitted, f"Missing {exp!r} in emitted SQL for {sql!r}:\n{emitted}"
            for forb in forbidden_subs:
                assert forb not in emitted, f"Unexpected {forb!r} in emitted SQL for {sql!r}:\n{emitted}"
    finally:
        ch_client.close()


_REAL_CHDB_DUCKDB_PARITY_SCRIPT = r"""
import json
import os
import tempfile
from types import SimpleNamespace

import chdb
from chdb import session as chdb_session
import duckdb

from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

with tempfile.TemporaryDirectory(prefix="fla_iter29_chdb_") as tmpdir:
    sess = chdb_session.Session(tmpdir)
    sess.query(
        "CREATE TABLE high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String"
        ") ENGINE = ReplacingMergeTree() ORDER BY (service_id, domain, batch_id)"
    )
    sess.query(
        "CREATE TABLE request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
        "url String, client_ip String, req_id String, response_size UInt64, "
        "custom_fields Map(String, String)"
        ") ENGINE = MergeTree() ORDER BY (service_id, event_timestamp, line_ordinal)"
    )
    sess.query("CREATE TABLE rum_vitals_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")
    sess.query("CREATE TABLE rum_error_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")

    sess.query(
        "INSERT INTO high_scale_batch_publications VALUES "
        "('svc_alpha', 'request', 'b1', 'visible'), "
        "('svc_other', 'request', 'b2', 'visible')"
    )
    sess.query(
        "INSERT INTO request_facts VALUES "
        "('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/alpha/hit', '198.51.100.10', 'req-1', 512, {'status': '200', 'edge': '1', 'pop': 'DEN'}), "
        "('svc_alpha', 'b1', 'visible', '2026-11-19 00:15:00.000000', 2, '/alpha/miss', '198.51.100.11', 'req-2', 256, {'status': '404', 'edge': '0', 'pop': 'SJC'}), "
        "('svc_other', 'b2', 'visible', '2026-11-19 00:10:00.000000', 1, '/foreign/leak', '198.51.100.99', 'req-f', 128, {'status': '200', 'edge': '1', 'pop': 'LAX'})"
    )

    desc_rows = json.loads(sess.query("DESCRIBE TABLE default.request_facts", "JSON").bytes())["data"]
    cols = [(r["name"], r["type"]) for r in desc_rows if str(r.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")]
    for masked in (False, True):
        v_map = console_access.console_tables("svc_alpha", masked=masked)
        v_sql = console_access._view_select("default", "request_facts", cols, "svc_alpha", masked=masked)
        sess.query(f"CREATE VIEW `{v_map['request_facts']}` AS {v_sql}")

    class _ChdbClient:
        def execute(self, sql, settings=None, rewrite_cf=False):
            s_parts = []
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}" if s_parts else sql
            raw = sess.query(full_sql, "JSON").bytes()
            return json.loads(raw.decode("utf-8"))["data"] if raw else []

    duck = duckdb.connect(":memory:")
    duck.execute(
        "CREATE TABLE logs (timestamp TIMESTAMPTZ, line_ordinal INT, url VARCHAR, ip VARCHAR, req_id VARCHAR, response_size BIGINT, status INT, edge BOOLEAN, pop VARCHAR)"
    )
    duck.execute(
        "INSERT INTO logs VALUES "
        "('2026-11-19T00:10:00Z', 1, '/alpha/hit', '198.51.100.10', 'req-1', 512, 200, true, 'DEN'), "
        "('2026-11-19T00:15:00Z', 2, '/alpha/miss', '198.51.100.11', 'req-2', 256, 404, false, 'SJC')"
    )

    svc = SimpleNamespace(service_id="svc_alpha", client=_ChdbClient())
    roles = [
        ("admin", False, None),
        ("raw_analyst", False, console_access.console_tables("svc_alpha", masked=False)),
        ("masked_analyst", True, console_access.console_tables("svc_alpha", masked=True)),
    ]
    parity_queries = [
        "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q AS t(u, st) WHERE t.st = 200 ORDER BY t.u",
        "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q t(u, st) WHERE t.st = 200 ORDER BY t.u",
        "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM logs l JOIN q AS t(u, st) ON l.url = t.u WHERE t.st = 200 ORDER BY t.u",
        "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM logs l JOIN q t(u, st) ON l.url = t.u WHERE t.st = 200 ORDER BY t.u",
        "WITH q1 AS (SELECT url, status AS x, edge AS flag FROM logs), q2 AS (SELECT url, pop, 200 AS x, true AS flag FROM logs) SELECT q1.url FROM q1 JOIN q2 ON q1.url = q2.url WHERE q1.x = 200 AND q2.x = 200 AND q1.flag = true AND q2.flag = true ORDER BY q1.url",
        "SELECT q1.url FROM (SELECT url, status AS x, edge AS flag FROM logs) AS q1 JOIN (SELECT url, pop, 200 AS x, true AS flag FROM logs) AS q2 ON q1.url = q2.url WHERE q1.x = 200 AND q2.x = 200 AND q1.flag = true AND q2.flag = true ORDER BY q1.url",
        "SELECT a.u FROM logs l JOIN (SELECT url FROM logs) AS a(u) ON l.url = a.u, (SELECT url, status FROM logs) AS b(u2, st2) WHERE a.u = b.u2 AND b.st2 = 200 ORDER BY a.u",
    ]
    for q_sql in parity_queries:
        duck_rows = [list(r) for r in duck.execute(q_sql).fetchall()]
        assert duck_rows == [["/alpha/hit"]], f"Unexpected duckdb baseline for {q_sql}: {duck_rows}"
        for role_name, mask_ips, tbls in roles:
            res = hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=q_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=mask_ips,
                tables=tbls,
            )
            ch_rows = [[row[col] for col in res["columns"]] for row in res["data"]]
            assert ch_rows == duck_rows, f"Mismatch on {role_name} for {q_sql}: ch={ch_rows} vs duck={duck_rows}"
    print("ITER29_CHDB_DUCKDB_PARITY_OK")
"""


@pytest.mark.security_regression
def test_bug_t5_iter28_01_sec01_hs01_hs02_sql_console_allowlist_and_chdb_alias_parity() -> None:
    """ITER28 remediation: function-allowlist parser-differential hardening and real chdb/duckdb CTE/derived alias parity."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.high_scale.postgres_control import console_access

    # 1. Standalone comment-separated unsafe functions must fail directly in _reject_unsafe_functions (BUG-T5-ITER28-HS-02c):
    for comment_sql in (
        "SELECT * FROM file /* c */ ('etc/passwd', 'CSV')",
        "SELECT * FROM url -- line\n('http://169.254.169.254/', 'CSV')",
    ):
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(comment_sql)

    # 2. All Wave-28 allowlist bypasses (TRIM(FROM ...), FROM-first SELECT comma, [LEFT] ARRAY JOIN keyword confusion)
    # must raise PermissionError in both _reject_unsafe_functions AND query_endpoint across Admin & Masked Analyst:
    captured_queries: list[str] = []

    def _ch_handler(req: httpx.Request) -> httpx.Response:
        body_text = req.content.decode("utf-8", errors="replace")
        if "SELECT 1" in body_text and "additional_table_filters" in str(req.url):
            return httpx.Response(200, json={"data": [{"1": 1}]})
        captured_queries.append(body_text)
        return httpx.Response(200, json={"data": [{"u": "/alpha/hit", "url": "/alpha/hit"}]})

    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(_ch_handler),
    )
    ch_client._enable_cf_rewrite_in_tests = True
    ch_client._table_filters_verified = True  # type: ignore[attr-defined]
    try:
        svc = SimpleNamespace(service_id="svc_alpha", client=ch_client)
        masked_tables = console_access.console_tables("svc_alpha", masked=True)

        bypass_payloads = (
            "SELECT trim(FROM CASE file(url) WHEN '' THEN 'a' ELSE 'b' END) AS x FROM logs",
            "SELECT trim(FROM NOT sleepEachRow(line_ordinal)) AS x FROM logs",
            "SELECT substring(url FROM CASE file(url) WHEN '' THEN 1 ELSE 2 END FOR 3) AS x FROM logs",
            "SELECT * FROM (FROM logs SELECT 1 AS a, NOT sleepEachRow(line_ordinal) AS b)",
            "SELECT * FROM (FROM logs SELECT 1 AS a, CASE file(url) WHEN '' THEN 'x' ELSE 'y' END AS b)",
            "WITH case AS (SELECT 1 FROM logs), p AS (SELECT 0.05 AS s FROM logs) SELECT x FROM p ARRAY JOIN case sleepEachRow(s) WHEN 99 THEN [0] ELSE [1] END AS x",
            "WITH case AS (SELECT 1 FROM logs), p AS (SELECT 'secret.txt' AS fp FROM logs) SELECT x FROM p ARRAY JOIN case file(fp, NULL) WHEN 'x' THEN ['a'] ELSE ['b'] END AS x",
            "WITH case AS (SELECT 1 FROM logs), p AS (SELECT 0.05 AS s FROM logs) SELECT x FROM p LEFT ARRAY JOIN [1] AS y, case sleepEachRow(s) WHEN 99 THEN [0] ELSE [1] END AS x",
        )
        for atk_sql in bypass_payloads:
            with pytest.raises(PermissionError):
                hs_query._reject_unsafe_functions(atk_sql)
            for mask_ips, tbls in ((False, None), (True, masked_tables)):
                captured_queries.clear()
                with pytest.raises(PermissionError):
                    hs_query.query_endpoint(
                        svc,
                        DashboardQueryRequest(sql=atk_sql, max_rows=10),
                        "2026-11-19T00:00:00Z",
                        "2026-11-19T01:00:00Z",
                        mask_ips=mask_ips,
                        tables=tbls,
                    )
                assert captured_queries == []

        # 3. Structural emitted-SQL checks for CTE/table column-list wrapping, shadowed qualifier schemas, and comma-joins:
        for wrap_sql in (
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q AS t(u, st) WHERE t.st = 200 ORDER BY t.u",
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q t(u, st) WHERE t.st = 200 ORDER BY t.u",
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM logs l JOIN q AS t(u, st) ON l.url = t.u WHERE t.st = 200 ORDER BY t.u",
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM logs l JOIN q t(u, st) ON l.url = t.u WHERE t.st = 200 ORDER BY t.u",
        ):
            captured_queries.clear()
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=wrap_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
                tables=None,
            )
            assert "(SELECT * FROM q) AS t(u, st)" in captured_queries[0]
            assert "t.st = '200'" in captured_queries[0]

        shadow_sql = (
            "WITH q1 AS (SELECT url, status AS x, edge AS flag FROM logs), "
            "q2 AS (SELECT url, pop, 200 AS x, true AS flag FROM logs) "
            "SELECT q1.url FROM q1 JOIN q2 ON q1.url = q2.url "
            "WHERE q1.x = 200 AND q2.x = 200 AND q1.flag = true AND q2.flag = true ORDER BY q1.url"
        )
        captured_queries.clear()
        hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=shadow_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=False,
            tables=None,
        )
        emitted_shadow = captured_queries[0]
        assert "q1.x = '200'" in emitted_shadow
        assert "q2.x = 200" in emitted_shadow
        assert "lower(q1.flag) IN ('true', '1')" in emitted_shadow
        assert "lower(q2.flag)" not in emitted_shadow

        comma_join_sql = (
            "SELECT a.u FROM logs l JOIN (SELECT url FROM logs) AS a(u) ON l.url = a.u, "
            "(SELECT url, status FROM logs) AS b(u2, st2) WHERE a.u = b.u2 AND b.st2 = 200 ORDER BY a.u"
        )
        captured_queries.clear()
        hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=comma_join_sql, max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=False,
            tables=None,
        )
        assert "b.st2 = '200'" in captured_queries[0]
    finally:
        ch_client.close()

    # 4. Real chdb + duckdb end-to-end differential execution (in-process if installed, else offline uv cache):
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401
        import duckdb as _duckdb  # noqa: F401

        exec(_REAL_CHDB_DUCKDB_PARITY_SCRIPT, {})
    except ImportError:
        uv_bin = shutil.which("uv")
        if uv_bin is not None:
            py_paths = [repo_root, *site.getsitepackages()]
            env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
            env["PYTHONPATH"] = os.pathsep.join(py_paths)
            probe = subprocess.run(
                [
                    uv_bin,
                    "run",
                    "--offline",
                    "--no-project",
                    "--with",
                    "chdb",
                    "--with",
                    "duckdb",
                    "python",
                    "-c",
                    "import chdb, duckdb",
                ],
                cwd=repo_root,
                env=env,
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0:
                run_res = subprocess.run(
                    [
                        uv_bin,
                        "run",
                        "--offline",
                        "--no-project",
                        "--with",
                        "chdb",
                        "--with",
                        "duckdb",
                        "python",
                        "-c",
                        _REAL_CHDB_DUCKDB_PARITY_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb/duckdb parity failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER29_CHDB_DUCKDB_PARITY_OK" in run_res.stdout


_REAL_CHDB_DUCKDB_ITER30_SCRIPT = r"""
import json
import tempfile
from types import SimpleNamespace

import chdb
from chdb import session as chdb_session
import duckdb

from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

with tempfile.TemporaryDirectory(prefix="fla_iter30_chdb_") as tmpdir:
    sess = chdb_session.Session(tmpdir)
    sess.query(
        "CREATE TABLE high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String"
        ") ENGINE = ReplacingMergeTree() ORDER BY (service_id, domain, batch_id)"
    )
    sess.query(
        "CREATE TABLE request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
        "url String, client_ip String, req_id String, response_size UInt64, "
        "custom_fields Map(String, String), cmcd Map(String, String)"
        ") ENGINE = MergeTree() ORDER BY (service_id, event_timestamp, line_ordinal)"
    )
    sess.query("CREATE TABLE rum_vitals_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")
    sess.query("CREATE TABLE rum_error_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")

    sess.query(
        "INSERT INTO high_scale_batch_publications VALUES "
        "('svc_alpha', 'request', 'b1', 'visible'), "
        "('svc_other', 'request', 'b2', 'visible')"
    )
    pii_cf = (
        "{'status': '200', 'edge': '1', 'pop': 'DEN', "
        "'CMCD-Session': 'cmcd-sess-secret-99', 'cmcd-sid': 'cmcd-sid-secret-99', 'cmcd-cid': 'cmcd-cid-secret-99', "
        "'cookie-session': 'cookie-sess-secret-88', 'session-id': 'sess-id-secret-77', 'client-id': 'cid-secret-66', "
        "'edge-sid': 'esid-secret-55', 'client-ip': '198.51.100.10', 'ip-address': '198.51.100.10', "
        "'remote-addr': '198.51.100.10', 'c-ip': '198.51.100.10', 'x-forwarded-for': '198.51.100.10', "
        "'true-client-ip': '198.51.100.10', 'fastly-client-ip': '198.51.100.10', 'origin-ip': '203.0.113.5'}"
    )
    pii_cmcd = (
        "{'CMCD-Session': 'cmcd-sess-secret-99', 'Session': 'cmcd-sess-secret-99', "
        "'cmcd-sid': 'cmcd-sid-secret-99', 'cmcd-cid': 'cmcd-cid-secret-99', 'br': '3200'}"
    )
    sess.query(
        "INSERT INTO request_facts VALUES "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/alpha/hit', '198.51.100.10', 'req-1', 512, {pii_cf}, {pii_cmcd}), "
        "('svc_alpha', 'b1', 'visible', '2026-11-19 00:15:00.000000', 2, '/alpha/miss', '198.51.100.11', 'req-2', 256, {'status': '404', 'edge': '0', 'pop': 'SJC'}, {'br': '1600'}), "
        "('svc_other', 'b2', 'visible', '2026-11-19 00:10:00.000000', 1, '/foreign/leak', '198.51.100.99', 'req-f', 128, {'status': '200', 'edge': '1', 'pop': 'LAX'}, {})"
    )

    desc_rows = json.loads(sess.query("DESCRIBE TABLE default.request_facts", "JSON").bytes())["data"]
    cols = [(r["name"], r["type"]) for r in desc_rows if str(r.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")]
    for masked in (False, True):
        v_map = console_access.console_tables("svc_alpha", masked=masked)
        v_sql = console_access._view_select("default", "request_facts", cols, "svc_alpha", masked=masked)
        sess.query(f"CREATE VIEW `{v_map['request_facts']}` AS {v_sql}")

    class _ChdbClient:
        _table_filters_verified = True

        def execute(self, sql, settings=None, rewrite_cf=False):
            s_parts = []
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}" if s_parts else sql
            raw = sess.query(full_sql, "JSON").bytes()
            return json.loads(raw.decode("utf-8"))["data"] if raw else []

    svc = SimpleNamespace(service_id="svc_alpha", client=_ChdbClient())
    raw_tables = console_access.console_tables("svc_alpha", masked=False)
    masked_tables = console_access.console_tables("svc_alpha", masked=True)

    # 1. SEC-T5-ITER29-HS-01: Masked view redacts ALL hyphenated/mixed-case PII map keys in custom_fields & cmcd
    # on aliased projections AND blind-oracle WHERE predicates, while raw analyst reads exact values:
    hyphen_pii_keys = (
        "CMCD-Session", "cmcd-sid", "cmcd-cid", "cookie-session", "session-id",
        "client-id", "edge-sid", "client-ip", "ip-address", "remote-addr",
        "c-ip", "x-forwarded-for", "true-client-ip", "fastly-client-ip", "origin-ip",
    )
    proj_exprs = ", ".join(f"custom_fields['{k}'] AS v_{i}" for i, k in enumerate(hyphen_pii_keys))
    proj_sql = f"SELECT {proj_exprs}, cmcd['CMCD-Session'] AS m_sess, cmcd['Session'] AS m_short, cmcd['cmcd-sid'] AS m_sid, cmcd['cmcd-cid'] AS m_cid, cmcd['br'] AS m_br, custom_fields['pop'] AS cf_pop FROM logs WHERE line_ordinal = 1"
    masked_proj = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=proj_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"][0]
    for i, k in enumerate(hyphen_pii_keys):
        assert masked_proj[f"v_{i}"] == "[redacted]", f"Unredacted custom_fields[{k!r}]: {masked_proj[f'v_{i}']!r}"
    for mk in ("m_sess", "m_short", "m_sid", "m_cid"):
        assert masked_proj[mk] == "[redacted]", f"Unredacted cmcd {mk}: {masked_proj[mk]!r}"
    assert masked_proj["m_br"] == "3200"
    assert masked_proj["cf_pop"] == "DEN"

    for oracle_pred in (
        "custom_fields['CMCD-Session'] = 'cmcd-sess-secret-99'",
        "custom_fields['cookie-session'] = 'cookie-sess-secret-88'",
        "custom_fields['client-ip'] = '198.51.100.10'",
        "custom_fields['origin-ip'] = '203.0.113.5'",
        "cmcd['CMCD-Session'] = 'cmcd-sess-secret-99'",
        "cmcd['Session'] = 'cmcd-sess-secret-99'",
    ):
        oracle_sql = f"SELECT count() AS n FROM logs WHERE {oracle_pred}"
        m_res = hs_query.query_endpoint(
            svc, DashboardQueryRequest(sql=oracle_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert int(m_res[0]["n"]) == 0, f"Blind-oracle leak on {oracle_pred}: {m_res}"
        r_res = hs_query.query_endpoint(
            svc, DashboardQueryRequest(sql=oracle_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False, tables=raw_tables,
        )["data"]
        assert int(r_res[0]["n"]) == 1, f"Raw analyst lost exact match on {oracle_pred}: {r_res}"

    # 2. Real chdb vs duckdb differential parity on q.where comma-join, [NOT] BETWEEN, and SELECT * column-list demotion:
    duck = duckdb.connect(":memory:")
    duck.execute(
        "CREATE TABLE logs ("
        "service_id VARCHAR, batch_id VARCHAR, publication_state VARCHAR, "
        "timestamp TIMESTAMPTZ, line_ordinal INT, url VARCHAR, ip VARCHAR, "
        "req_id VARCHAR, response_size BIGINT, status INT, edge BOOLEAN"
        ")"
    )
    duck.execute(
        "INSERT INTO logs VALUES "
        "('svc_alpha', 'b1', 'visible', '2026-11-19T00:10:00Z', 1, '/alpha/hit', '198.51.100.10', 'req-1', 512, 200, true), "
        "('svc_alpha', 'b1', 'visible', '2026-11-19T00:15:00Z', 2, '/alpha/miss', '198.51.100.11', 'req-2', 256, 404, false)"
    )

    roles = [
        ("admin", False, None),
        ("raw_analyst", False, raw_tables),
        ("masked_analyst", True, masked_tables),
    ]
    diff_cases = [
        (
            "WITH q AS (SELECT url, line_ordinal AS where FROM logs), r AS (SELECT url, status FROM logs) SELECT b.u FROM logs l JOIN q ON q.where = l.line_ordinal, r AS b(u, st) WHERE b.u = l.url ORDER BY b.u",
            [["/alpha/hit"], ["/alpha/miss"]],
        ),
        (
            "SELECT url FROM logs WHERE status BETWEEN 200 AND 299 ORDER BY url",
            [["/alpha/hit"]],
        ),
        (
            "SELECT url FROM logs WHERE status NOT BETWEEN 200 AND 299 ORDER BY url",
            [["/alpha/miss"]],
        ),
        (
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q AS t(u, st) WHERE t.st BETWEEN 200 AND 299 ORDER BY t.u",
            [["/alpha/hit"]],
        ),
        (
            "WITH q AS (SELECT url, status FROM logs) SELECT t.u FROM q AS t(u, st) WHERE t.st NOT BETWEEN 200 AND 299 ORDER BY t.u",
            [["/alpha/miss"]],
        ),
        (
            "WITH q1 AS (SELECT url, status AS x FROM logs), q2 AS (SELECT url, response_size AS x FROM logs) SELECT q1.url FROM q1 JOIN q2 ON q1.url = q2.url WHERE q1.x BETWEEN 200 AND 299 AND q2.x BETWEEN 500 AND 600 ORDER BY q1.url",
            [["/alpha/hit"]],
        ),
        (
            "WITH q AS (SELECT * FROM logs) SELECT t.status FROM q AS t(s, b, p, ts, ord, status, cip, req, sz, cf, cm) ORDER BY t.status",
            [["/alpha/hit"], ["/alpha/miss"]],
        ),
        (
            "SELECT t.ip AS v FROM (SELECT * FROM logs) AS t(ip, b, p, ts, ord, u, cip, req, sz, cf, cm) ORDER BY v",
            [["svc_alpha"], ["svc_alpha"]],
        ),
        (
            "WITH q(s, b, p, ts, ord, status, cip, req, sz, cf, cm) AS (SELECT * FROM logs) SELECT q.status FROM q ORDER BY q.status",
            [["/alpha/hit"], ["/alpha/miss"]],
        ),
    ]
    for q_sql, expected in diff_cases:
        duck_rows = [list(r) for r in duck.execute(q_sql).fetchall()]
        assert duck_rows == expected, f"Unexpected duckdb baseline for {q_sql}: {duck_rows}"
        for role_name, mask_ips, tbls in roles:
            res = hs_query.query_endpoint(
                svc, DashboardQueryRequest(sql=q_sql, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=mask_ips, tables=tbls,
            )
            ch_rows = [[row[col] for col in res["columns"]] for row in res["data"]]
            assert ch_rows == duck_rows, f"Mismatch on {role_name} for {q_sql}: ch={ch_rows} vs duck={duck_rows}"

    raw_ip_sql = "SELECT t.ip FROM (SELECT * FROM logs) AS t(ip, b, p, ts, ord, u, cip, req, sz, cf, cm) ORDER BY t.ip"
    assert hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=raw_ip_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False, tables=raw_tables,
    )["data"] == [{"ip": "svc_alpha"}, {"ip": "svc_alpha"}]
    assert hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=raw_ip_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"] == [{"ip": "[redacted]"}, {"ip": "[redacted]"}]
    print("ITER30_CHDB_DUCKDB_PARITY_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter29_01_and_bug_hs01_allowlist_collist_between_and_hyphenated_pii() -> None:
    """ITER30 remediation: SEC-T5-ITER29-01, BUG-T5-ITER29-HS-01a/b, SEC-T5-ITER29-01c, SEC-T5-ITER29-HS-01."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.high_scale.archive import ArchiveSourceObject
    from backend.high_scale.postgres_control import console_access

    # 1. Exact 18-key MASKED_KEYS contract preserved while _redaction canonicalizes hyphenated/mixed-case keys:
    assert len(console_access.MASKED_KEYS) == 18
    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    assert console_access._redaction("_cf_cookie-session", "LowCardinality(String)", keys_sql) is not None
    assert console_access._redaction("_cf_origin-ip", "LowCardinality(String)", keys_sql) is not None
    assert console_access._redaction("_cf_CMCD-Session", "LowCardinality(String)", keys_sql) is not None

    # 2. decode_source_object routes case-insensitive cmcd- / cmcd_ / cmcd. keys to cmcd (not custom_fields):
    src_obj = ArchiveSourceObject(
        service_id="svc_alpha",
        domain="request",
        object_key="raw/2026/11/19/test.ndjson",
        checksum="sha256:abc",
        size_bytes=128,
    )
    dec = hs_query.decode_source_object(
        src_obj,
        (
            b'{"timestamp":"2026-11-19T00:10:00Z","url":"/v","CMCD-Session":"cmcd-s-1",'
            b'"cmcd-sid":"cmcd-s-2","CMCD.cid":"cmcd-c-3","CMCD_BR":"3200","cookie-session":"ck-1"}\n'
        ),
        transform_version="v1",
    )
    assert len(dec.events) == 1
    ev0 = dec.events[0]
    assert ev0["cmcd"]["CMCD-Session"] == "cmcd-s-1"
    assert ev0["cmcd"]["session"] == "cmcd-s-1"
    assert ev0["cmcd"]["cmcd-sid"] == "cmcd-s-2"
    assert ev0["cmcd"]["sid"] == "cmcd-s-2"
    assert ev0["cmcd"]["CMCD.cid"] == "cmcd-c-3"
    assert ev0["cmcd"]["cid"] == "cmcd-c-3"
    assert "CMCD-Session" not in ev0["custom_fields"]
    assert "cmcd-sid" not in ev0["custom_fields"]
    assert ev0["custom_fields"]["cookie-session"] == "ck-1"

    # 3. Allowlist bypasses (SEC-T5-ITER29-01 & BUG-T5-ITER29-HS-01b) MUST fail in _reject_unsafe_functions & query_endpoint:
    captured_queries: list[str] = []

    def _ch_handler(req: httpx.Request) -> httpx.Response:
        body_text = req.content.decode("utf-8", errors="replace")
        if "SELECT 1" in body_text and "additional_table_filters" in str(req.url):
            return httpx.Response(200, json={"data": [{"1": 1}]})
        captured_queries.append(body_text)
        return httpx.Response(200, json={"data": [{"u": "/alpha/hit", "status": "svc", "ip": "svc"}]})

    ch_client = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(_ch_handler),
    )
    ch_client._enable_cf_rewrite_in_tests = True
    ch_client._table_filters_verified = True  # type: ignore[attr-defined]
    try:
        svc = SimpleNamespace(service_id="svc_alpha", client=ch_client)
        masked_tables = console_access.console_tables("svc_alpha", masked=True)

        iter29_bypasses = (
            "SELECT 1 AS join, SOME sleepEachRow(line_ordinal), 1 FROM logs",
            "SELECT 1 AS from, MINUS sleepEachRow(line_ordinal), 1 FROM logs",
            "FROM logs a JOIN logs b ON a.url = b.url SELECT 1 AS c1, SOME sleepEachRow(line_ordinal)",
            "WITH a AS (SELECT 1 AS join, line_ordinal FROM logs) SELECT a.join, SOME sleepEachRow(line_ordinal), 1 FROM a",
            "FROM logs a JOIN logs b ON a.url = b.url SELECT 1 AS x, url file(url)",
            "SELECT 1 AS join, PLUS sleepEachRow(line_ordinal), 1 FROM logs",
            "SELECT 1 AS from, BINARY file(url), 1 FROM logs",
            "SELECT file(url) FROM logs AS file(u)",
        )
        for atk_sql in iter29_bypasses:
            assert hs_query._exempt_col_list_ident_starts(atk_sql) == set() or "AS file(u)" in atk_sql, atk_sql
            with pytest.raises((PermissionError, ValueError)):
                hs_query._reject_unsafe_functions(atk_sql)
            for mask_ips, tbls in ((False, None), (True, masked_tables)):
                captured_queries.clear()
                with pytest.raises((PermissionError, ValueError)):
                    hs_query.query_endpoint(
                        svc,
                        DashboardQueryRequest(sql=atk_sql, max_rows=10),
                        "2026-11-19T00:00:00Z",
                        "2026-11-19T01:00:00Z",
                        mask_ips=mask_ips,
                        tables=tbls,
                    )
                assert captured_queries == []

        # 4. SEC-T5-ITER29-01c: Explicit column-list aliases over SELECT * demote raw fact domains:
        for demote_sql, forbidden_frag in (
            ("WITH q AS (SELECT * FROM logs) SELECT t.status FROM q AS t(s, b, p, status)", "custom_fields"),
            ("SELECT t.ip FROM (SELECT * FROM logs) AS t(ip, b)", "client_ip"),
            ("WITH q(s, b, p, status) AS (SELECT * FROM logs) SELECT q.status FROM q", "custom_fields"),
            ("WITH q(s, b, p, status) AS (SELECT * FROM logs) SELECT status FROM q", "custom_fields"),
            ("SELECT t.timestamp FROM (SELECT * FROM client_vitals) AS t(s, b, p, timestamp)", "t.event_timestamp"),
        ):
            captured_queries.clear()
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=demote_sql, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=False,
                tables=None,
            )
            assert len(captured_queries) == 1
            assert forbidden_frag not in captured_queries[0], f"Unexpected {forbidden_frag!r} in {captured_queries[0]}"
    finally:
        ch_client.close()

    # 5. Real chdb + duckdb execution for SEC-T5-ITER29-HS-01, BUG-T5-ITER29-HS-01a/b, and SEC-T5-ITER29-01c
    # (in-process if installed, else offline uv cache when available):
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401
        import duckdb as _duckdb  # noqa: F401

        exec(_REAL_CHDB_DUCKDB_ITER30_SCRIPT, {})
    except ImportError:
        uv_bin = shutil.which("uv")
        if uv_bin is not None:
            py_paths = [repo_root, *site.getsitepackages()]
            env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
            env["PYTHONPATH"] = os.pathsep.join(py_paths)
            probe = subprocess.run(
                [
                    uv_bin,
                    "run",
                    "--offline",
                    "--no-project",
                    "--with",
                    "chdb",
                    "--with",
                    "duckdb",
                    "python",
                    "-c",
                    "import chdb, duckdb",
                ],
                cwd=repo_root,
                env=env,
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0:
                run_res = subprocess.run(
                    [
                        uv_bin,
                        "run",
                        "--offline",
                        "--no-project",
                        "--with",
                        "chdb",
                        "--with",
                        "duckdb",
                        "python",
                        "-c",
                        _REAL_CHDB_DUCKDB_ITER30_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb/duckdb Iter30 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER30_CHDB_DUCKDB_PARITY_OK" in run_res.stdout


_REAL_CHDB_DUCKDB_ITER31_SCRIPT = """
import json
import tempfile
from types import SimpleNamespace

import chdb
from chdb import session as chdb_session
import duckdb

from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

with tempfile.TemporaryDirectory(prefix="fla_iter31_chdb_") as tmpdir:
    sess = chdb_session.Session(tmpdir)
    sess.query(
        "CREATE TABLE high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String"
        ") ENGINE = ReplacingMergeTree() ORDER BY (service_id, domain, batch_id)"
    )
    sess.query(
        "CREATE TABLE request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
        "url String, client_ip String, req_id String, response_size UInt64, "
        "custom_fields Map(String, String), cmcd Map(String, String)"
        ") ENGINE = MergeTree() ORDER BY (service_id, event_timestamp, line_ordinal)"
    )
    sess.query("CREATE TABLE rum_vitals_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")
    sess.query("CREATE TABLE rum_error_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")

    sess.query("INSERT INTO high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')")
    row1_cf = (
        "{'status': '200', 'bytes': '250', 'origin_time_ms': '-12.5', "
        "'filter-origin-ip': '203.0.113.10', 'filter-session-id': 'fsess-secret-11', "
        "'xfilter-cookie-session': 'xfcookie-secret-22', 'filter-client-ip': '198.51.100.20', "
        "'req.http.session_id': 'req-sess-secret-33', 'header.cookie_session': 'hdr-cookie-secret-44', "
        "'meta[session_id]': 'meta-sess-secret-55', 'x.real.ip': '198.51.100.30', "
        "'cf-connecting-ip': '198.51.100.40', 'x-real-ip': '198.51.100.50', "
        "'Authorization': 'Bearer super-secret-jwt-66', 'set-cookie': 'sid=setcookie-secret-77', "
        "'cookie': 'sid=cookie-secret-88', "
        "'cache-status': 'HIT', 'origin-time': '14', 'session_duration_ms': '900', "
        "'ip_version': '4', 'client_asn': '54113', 'token_count': '7'}"
    )
    row2_cf = "{'status': '200', 'bytes': '100', 'origin_time_ms': '-0.25'}"
    row3_cf = "{'status': '500', 'bytes': '750', 'origin_time_ms': '42.0'}"
    sess.query(
        "INSERT INTO request_facts VALUES "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/r1', '198.51.100.10', 'req-1', 250, {row1_cf}, {{'CMCD_BR': '3200'}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:11:00.000000', 2, '/r2', '198.51.100.11', 'req-2', 100, {row2_cf}, {{}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:12:00.000000', 3, '/r3', '198.51.100.12', 'req-3', 750, {row3_cf}, {{}})"
    )

    desc_rows = json.loads(sess.query("DESCRIBE TABLE default.request_facts", "JSON").bytes())["data"]
    cols = [(r["name"], r["type"]) for r in desc_rows if str(r.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")]
    for masked in (False, True):
        v_map = console_access.console_tables("svc_alpha", masked=masked)
        v_sql = console_access._view_select("default", "request_facts", cols, "svc_alpha", masked=masked)
        sess.query(f"CREATE VIEW `{v_map['request_facts']}` AS {v_sql}")

    class _ChdbClient:
        _table_filters_verified = True

        def execute(self, sql, settings=None, rewrite_cf=False):
            s_parts = []
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\\\", "\\\\\\\\").replace("'", "\\\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}" if s_parts else sql
            raw = sess.query(full_sql, "JSON").bytes()
            return json.loads(raw.decode("utf-8"))["data"] if raw else []

    svc = SimpleNamespace(service_id="svc_alpha", client=_ChdbClient())
    raw_tables = console_access.console_tables("svc_alpha", masked=False)
    masked_tables = console_access.console_tables("svc_alpha", masked=True)

    # 1. Masked view aliased projections & WHERE blind-oracle (oracle_n == 0) across all 13 Iter-31 PII keys + non-PII preservation:
    iter31_pii_pairs = (
        ("filter-origin-ip", "203.0.113.10"),
        ("filter-session-id", "fsess-secret-11"),
        ("xfilter-cookie-session", "xfcookie-secret-22"),
        ("filter-client-ip", "198.51.100.20"),
        ("req.http.session_id", "req-sess-secret-33"),
        ("header.cookie_session", "hdr-cookie-secret-44"),
        ("meta[session_id]", "meta-sess-secret-55"),
        ("x.real.ip", "198.51.100.30"),
        ("cf-connecting-ip", "198.51.100.40"),
        ("x-real-ip", "198.51.100.50"),
        ("Authorization", "Bearer super-secret-jwt-66"),
        ("set-cookie", "sid=setcookie-secret-77"),
        ("cookie", "sid=cookie-secret-88"),
    )
    non_pii_expected = (
        ("cache-status", "HIT"),
        ("origin-time", "14"),
        ("session_duration_ms", "900"),
        ("ip_version", "4"),
        ("client_asn", "54113"),
        ("token_count", "7"),
    )
    proj_sql = (
        "SELECT "
        + ", ".join(f"custom_fields['{k}'] AS p_{i}" for i, (k, _) in enumerate(iter31_pii_pairs))
        + ", "
        + ", ".join(f"custom_fields['{k}'] AS np_{i}" for i, (k, _) in enumerate(non_pii_expected))
        + ", cmcd['CMCD_BR'] AS m_br FROM logs WHERE line_ordinal = 1"
    )
    masked_row = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=proj_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"][0]
    raw_row = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=proj_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False, tables=raw_tables,
    )["data"][0]
    for i, (k, raw_secret) in enumerate(iter31_pii_pairs):
        assert masked_row[f"p_{i}"] == "[redacted]", f"Unredacted {k}: {masked_row[f'p_{i}']!r}"
        assert raw_row[f"p_{i}"] == raw_secret, f"Raw analyst lost {k}: {raw_row[f'p_{i}']!r}"
        oracle_sql = f"SELECT count() AS oracle_n FROM logs WHERE custom_fields['{k}'] = '{raw_secret}'"
        oracle_res = hs_query.query_endpoint(
            svc, DashboardQueryRequest(sql=oracle_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert int(oracle_res[0]["oracle_n"]) == 0, f"Blind-oracle leaked {k}: {oracle_res}"
    for i, (nk, nv) in enumerate(non_pii_expected):
        assert masked_row[f"np_{i}"] == nv, f"False redaction on {nk}: {masked_row[f'np_{i}']!r}"
    assert masked_row["m_br"] == "3200"

    # 2. Real chdb == duckdb numeric differential parity:
    dcon = duckdb.connect(":memory:")
    dcon.execute(
        "CREATE TABLE logs (url VARCHAR, bytes BIGINT, origin_time_ms DOUBLE, status BIGINT); "
        "INSERT INTO logs VALUES "
        "('/r1', 250, -12.5, 200), "
        "('/r2', 100, -0.25, 200), "
        "('/r3', 750, 42.0, 500);"
    )
    parity_queries = (
        "SELECT url FROM logs WHERE (bytes) BETWEEN (100) AND (500) ORDER BY url",
        "SELECT url FROM logs WHERE origin_time_ms BETWEEN (-20.5) AND (-5.0) ORDER BY url",
        "SELECT url FROM logs WHERE bytes = +100 ORDER BY url",
        "SELECT url FROM logs WHERE bytes = 1e2 ORDER BY url",
        "SELECT url FROM logs WHERE origin_time_ms > -.5 AND origin_time_ms < 0 ORDER BY url",
        "SELECT url FROM logs WHERE bytes = 50 + 50 ORDER BY url",
        "SELECT url FROM logs WHERE origin_time_ms <= -10 * 2 + 10 ORDER BY url",
        "WITH t AS (SELECT url, origin_time_ms AS ot, bytes AS b FROM logs) SELECT t.url FROM t WHERE (t.ot) BETWEEN (-20.5) AND (-5.0) AND (t.b) = (125 + 125) ORDER BY t.url",
    )
    for q_sql in parity_queries:
        duck_urls = [r[0] for r in dcon.execute(q_sql).fetchall()]
        assert len(duck_urls) >= 1, f"Empty expected set for {q_sql}"
        for mask_flag, tbls in ((False, raw_tables), (True, masked_tables)):
            ch_urls = [
                r["url"]
                for r in hs_query.query_endpoint(
                    svc, DashboardQueryRequest(sql=q_sql, max_rows=10),
                    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=mask_flag, tables=tbls,
                )["data"]
            ]
            assert ch_urls == duck_urls, f"Parity mismatch ({mask_flag}) on {q_sql}: ch={ch_urls} vs duck={duck_urls}"
    print("ITER31_CHDB_DUCKDB_PARITY_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter30_hs01_01_and_bug_hs01_canonical_pii_numeric_predicates_and_as_clause_guards() -> None:
    """ITER31 remediation: SEC-T5-ITER30-HS-01, SEC-T5-ITER30-01, BUG-T5-ITER30-HS-01."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale.postgres_control import console_access

    # (a) Preserve exact 18-key MASKED_KEYS == SHARED_PII_KEYS invariant while canonicalizing segment/bracket/header PII:
    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)
    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    for pii_col in (
        "_cf_filter-origin-ip",
        "_cf_filter-session-id",
        "_cf_xfilter-cookie-session",
        "_cf_filter-client-ip",
        "req.http.session_id",
        "header.cookie_session",
        "meta[session_id]",
        "x.real.ip",
        "cf-connecting-ip",
        "x-real-ip",
        "Authorization",
        "set-cookie",
        "cookie",
    ):
        assert console_access._redaction(pii_col, "LowCardinality(String)", keys_sql) is not None, pii_col
    for safe_col in (
        "cache-status",
        "origin-time",
        "session_duration_ms",
        "ip_version",
        "client_asn",
        "token_count",
        "CMCD_BR",
    ):
        assert console_access._redaction(safe_col, "LowCardinality(String)", keys_sql) is None, safe_col

    seen_iso_queries: list[str] = []

    class _FakeIsoCH:
        def execute(self, sql: str, *args: object, **kwargs: object) -> list[dict[str, int]]:
            seen_iso_queries.append(sql)
            return [{"n": 0}]

        def close(self) -> None:
            pass

    iso_clients = console_access.ConsoleClients(
        settings=svcconfig.ClickHouseConfig(
            host="127.0.0.1", port=8123, database="default", user="default", password=""
        ),
        secret=bytes(range(40, 88)),
        client_factory=lambda _cfg: _FakeIsoCH(),
    )
    assert iso_clients.client_for("svc_alpha", masked=True) is not None
    req_iso_sql = next(q for q in seen_iso_queries if "request_facts" in q and "client_ip !=" in q)
    for iso_frag in (
        "custom_fields['cf-connecting-ip']",
        "custom_fields['x-real-ip']",
        "custom_fields['authorization']",
        "custom_fields['cookie']",
        "custom_fields['set-cookie']",
    ):
        assert iso_frag in req_iso_sql, f"_assert_isolated missing {iso_frag}"

    # (b) validate_user_sql & _reject_unsafe_functions block AS-clause-keyword & FROM stray-token/ON desyncs
    # while keeping AS select WHERE, AS union WHERE/ON, TABLESAMPLE, and SAMPLE ... OFFSET green:
    disallowed_from_sqls = (
        "FROM logs as SELECT 1, logs sleepEachRow(line_ordinal)",
        "SELECT * FROM logs as WHERE 1, logs sleepEachRow(line_ordinal)",
        "SELECT * FROM logs a 1, logs sleepEachRow(line_ordinal)",
        "SELECT * FROM logs ON ORDER LEFT GROUP, logs file(url)",
    )
    for bad_sql in disallowed_from_sqls:
        with pytest.raises(SQLValidationError):
            validate_user_sql(bad_sql, allowed_tables=frozenset({"logs"}))
        assert hs_query._exempt_col_list_ident_starts(bad_sql) == set(), bad_sql
        with pytest.raises((PermissionError, ValueError)):
            hs_query._reject_unsafe_functions(bad_sql)

    for allowed_sql in (
        "SELECT select.status FROM logs AS select WHERE select.status = 200",
        "SELECT union.status FROM logs AS union WHERE union.status = 200",
        "SELECT a.url FROM logs AS a JOIN logs AS union ON a.url = union.url WHERE union.status = 200",
        "SELECT url FROM logs TABLESAMPLE 10 PERCENT WHERE status = 200",
        "SELECT url FROM logs SAMPLE 1/10 OFFSET 1/2 WHERE status = 200",
    ):
        res_v = validate_user_sql(allowed_sql, allowed_tables=frozenset({"logs"}))
        assert res_v.parse_tree is not None

    # (c) Direct rewriter + post-query Authorization/cookie/nested-IP masking checks:
    rewritten_between, _ = hs_query._rewrite_console_sql_with_canonicals(
        "SELECT url FROM logs WHERE (bytes) BETWEEN (100) AND (500) AND origin_time_ms BETWEEN (-20.5) AND (-5.0) "
        "AND bytes = +100 AND bytes = 1e2 AND origin_time_ms > -.5 AND bytes = 50 + 50 AND origin_time_ms <= -10 * 2",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert "toFloat64OrZero(custom_fields['bytes']) BETWEEN (100) AND (500)" in rewritten_between
    assert "toFloat64OrZero(custom_fields['origin_time_ms']) BETWEEN (-20.5) AND (-5.0)" in rewritten_between
    assert "toFloat64OrNull(nullIf(custom_fields['bytes'], '')) = +100" in rewritten_between
    assert "toFloat64OrNull(nullIf(custom_fields['bytes'], '')) = 1e2" in rewritten_between
    assert "toFloat64OrZero(custom_fields['origin_time_ms']) > -.5" in rewritten_between
    assert "toFloat64OrNull(nullIf(custom_fields['bytes'], '')) = 50 + 50" in rewritten_between
    assert "toFloat64OrZero(custom_fields['origin_time_ms']) <= -10 * 2" in rewritten_between

    def _mock_post_mask_handler(req: httpx.Request) -> httpx.Response:
        body_text = req.content.decode("utf-8", errors="replace")
        if "SELECT 1" in body_text and "additional_table_filters" in str(req.url):
            return httpx.Response(200, json={"data": [{"1": 1}]})
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "Authorization": "Bearer secret-jwt-tok",
                        "cookie": "sid=secret-cookie-val",
                        "set-cookie": "sid=secret-setcookie-val",
                        "filter-session-id": "fsess-99",
                        "oip": "203.0.113.9",
                        "cache-status": "HIT",
                        "custom_fields": {
                            "Authorization": "Bearer nested-jwt",
                            "cookie": "sid=nested-cookie",
                            "req.http.session_id": "nested-req-sess",
                            "cf-connecting-ip": "198.51.100.77",
                            "x-real-ip": "198.51.100.88",
                            "oip": "151.101.1.42",
                            "cache-status": "HIT",
                        },
                    }
                ]
            },
        )

    mock_ch = ClickHouseClient(
        svcconfig.ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(_mock_post_mask_handler),
    )
    mock_ch._enable_cf_rewrite_in_tests = True
    mock_ch._table_filters_verified = True  # type: ignore[attr-defined]
    try:
        svc_mock = SimpleNamespace(service_id="svc_alpha", client=mock_ch)
        post_out = hs_query.query_endpoint(
            svc_mock,
            DashboardQueryRequest(sql="SELECT * FROM logs LIMIT 1", max_rows=10),
            "2026-11-19T00:00:00Z",
            "2026-11-19T01:00:00Z",
            mask_ips=True,
            tables=console_access.console_tables("svc_alpha", masked=True),
        )["data"][0]
        assert post_out["Authorization"] == "[redacted]"
        assert post_out["cookie"] == "[redacted]"
        assert post_out["set-cookie"] == "[redacted]"
        assert post_out["filter-session-id"] == "[redacted]"
        assert post_out["oip"] == "[redacted]"
        assert post_out["cache-status"] == "HIT"
        assert post_out["custom_fields"]["Authorization"] == "[redacted]"
        assert post_out["custom_fields"]["cookie"] == "[redacted]"
        assert post_out["custom_fields"]["req.http.session_id"] == "[redacted]"
        assert post_out["custom_fields"]["cf-connecting-ip"] == "198.51.100.xxx"
        assert post_out["custom_fields"]["x-real-ip"] == "198.51.100.xxx"
        assert post_out["custom_fields"]["oip"] == "151.101.1.xxx"
        assert post_out["custom_fields"]["cache-status"] == "HIT"
    finally:
        mock_ch.close()

    # Real chdb + duckdb execution (in-process if installed, else offline uv cache when available):
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401
        import duckdb as _duckdb  # noqa: F401

        exec(_REAL_CHDB_DUCKDB_ITER31_SCRIPT, {})
    except ImportError:
        uv_bin = shutil.which("uv")
        if uv_bin is not None:
            py_paths = [repo_root, *site.getsitepackages()]
            env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
            env["PYTHONPATH"] = os.pathsep.join(py_paths)
            probe = subprocess.run(
                [
                    uv_bin,
                    "run",
                    "--offline",
                    "--no-project",
                    "--with",
                    "chdb",
                    "--with",
                    "duckdb",
                    "python",
                    "-c",
                    "import chdb, duckdb",
                ],
                cwd=repo_root,
                env=env,
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0:
                run_res = subprocess.run(
                    [
                        uv_bin,
                        "run",
                        "--offline",
                        "--no-project",
                        "--with",
                        "chdb",
                        "--with",
                        "duckdb",
                        "python",
                        "-c",
                        _REAL_CHDB_DUCKDB_ITER31_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb/duckdb Iter31 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER31_CHDB_DUCKDB_PARITY_OK" in run_res.stdout


_REAL_CHDB_DUCKDB_ITER32_SCRIPT = """
import json
import tempfile
from types import SimpleNamespace

import chdb
from chdb import session as chdb_session
import duckdb

from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

with tempfile.TemporaryDirectory(prefix="fla_iter32_chdb_") as tmpdir:
    sess = chdb_session.Session(tmpdir)
    sess.query(
        "CREATE TABLE high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String"
        ") ENGINE = ReplacingMergeTree() ORDER BY (service_id, domain, batch_id)"
    )
    sess.query(
        "CREATE TABLE request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
        "url String, client_ip String, req_id String, response_size UInt64, "
        "custom_fields Map(String, String), cmcd Map(String, String)"
        ") ENGINE = MergeTree() ORDER BY (service_id, event_timestamp, line_ordinal)"
    )
    sess.query("CREATE TABLE rum_vitals_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")
    sess.query("CREATE TABLE rum_error_facts (service_id String, batch_id String, publication_state String, event_timestamp DateTime64(6, 'UTC')) ENGINE = MergeTree() ORDER BY (service_id, event_timestamp)")

    sess.query("INSERT INTO high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')")

    def _ch_lit(s: str) -> str:
        return "'" + s.replace("\\\\", "\\\\\\\\").replace("'", "\\\\'") + "'"

    iter32_pii_pairs = (
        ("meta['user.authorization']", "Bearer meta-auth-1"),
        ("a[b[client_ip]]", "198.51.100.11"),
        ("a[['req_http_authorization']]", "Bearer nested-req-auth-2"),
        ("http/authorization", "Bearer slash-auth-3"),
        ("request:client_ip", "198.51.100.12"),
        ("a%2Fb%5Bclient_ip%5D", "198.51.100.13"),
        ("%61uthorization", "Bearer pct-auth-4"),
        ("clientIP", "198.51.100.14"),
        ("sessionID", "camel-sess-5"),
        ("xAuthToken", "xauth-tok-6"),
        ("xAccessToken", "xaccess-tok-7"),
        ("xSessionId", "xsess-id-8"),
        ("xSessionToken", "xsess-tok-9"),
        ("xForwardedClientIp", "198.51.100.15"),
        ("HTTPAuthorization", "Bearer http-auth-10"),
        ("-filter-cookie", "sid=dash-filter-ck-11"),
        ("session_id_1-", "trail-dash-sess-12"),
        ("client_ip_1!", "198.51.100.16"),
        ("authorization_1/", "Bearer trail-slash-auth-13"),
        ("client_ip1", "198.51.100.17"),
        ("session_id2", "num-sess-14"),
        ("sid1", "num-sid-15"),
        ("authorization_1", "Bearer num-auth-16"),
        ("req_http_filter_x_real_ip", "198.51.100.18"),
        ("x_auth_token", "raw-xauth-17"),
        ("x_access_token", "raw-xaccess-18"),
        ("x_session_id", "raw-xsess-19"),
        ("x_session_token", "raw-xsess-tok-20"),
        ("x_forwarded_client_ip", "198.51.100.19"),
    )
    iter32_safe_pairs = (
        ("cache-status", "HIT"),
        ("origin-time", "14"),
        ("session_duration_ms", "900"),
        ("ip_version", "4"),
        ("client_asn", "54113"),
        ("token_count", "7"),
        ("http_status", "200"),
        ("ipv6", "false"),
    )
    row1_cf_items = [
        "'status': '200'",
        "'bot_score': '10.0'",
        "'bytes': '250'",
    ] + [f"{_ch_lit(k)}: {_ch_lit(v)}" for k, v in iter32_pii_pairs + iter32_safe_pairs]
    row1_cf = "{" + ", ".join(row1_cf_items) + "}"
    row2_cf = "{'status': '200', 'bot_score': '007', 'bytes': '100'}"
    row3_cf = "{'status': '404', 'bot_score': '-0', 'bytes': '0'}"
    row4_cf = "{'status': '500', 'bot_score': '', 'bytes': '500'}"
    row5_cf = "{'status': '503', 'bot_score': 'abc', 'bytes': '750'}"
    row6_cf = "{'status': '201', 'bot_score': '-10', 'bytes': '300'}"
    row1_cmcd = "{'a[b[sid]]': 'cmcd-nested-sid-99', 'CMCD_BR': '3200'}"
    sess.query(
        "INSERT INTO request_facts VALUES "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/r1', '198.51.100.10', 'req-1', 250, {row1_cf}, {row1_cmcd}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:11:00.000000', 2, '/r2', '198.51.100.11', 'req-2', 100, {row2_cf}, {{}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:12:00.000000', 3, '/r3', '198.51.100.12', 'req-3', 0, {row3_cf}, {{}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:13:00.000000', 4, '/r4', '198.51.100.13', 'req-4', 500, {row4_cf}, {{}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:14:00.000000', 5, '/r5', '198.51.100.14', 'req-5', 750, {row5_cf}, {{}}), "
        f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:15:00.000000', 6, '/r6', '198.51.100.15', 'req-6', 300, {row6_cf}, {{}})"
    )

    desc_rows = json.loads(sess.query("DESCRIBE TABLE default.request_facts", "JSON").bytes())["data"]
    cols = [(r["name"], r["type"]) for r in desc_rows if str(r.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")]
    for masked in (False, True):
        v_map = console_access.console_tables("svc_alpha", masked=masked)
        v_sql = console_access._view_select("default", "request_facts", cols, "svc_alpha", masked=masked)
        sess.query(f"CREATE VIEW `{v_map['request_facts']}` AS {v_sql}")

    class _ChdbClient:
        _table_filters_verified = True

        def execute(self, sql, settings=None, rewrite_cf=False):
            s_parts = []
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\\\", "\\\\\\\\").replace("'", "\\\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}" if s_parts else sql
            raw = sess.query(full_sql, "JSON").bytes()
            return json.loads(raw.decode("utf-8"))["data"] if raw else []

    svc = SimpleNamespace(service_id="svc_alpha", client=_ChdbClient())
    raw_tables = console_access.console_tables("svc_alpha", masked=False)
    masked_tables = console_access.console_tables("svc_alpha", masked=True)

    # 1. Masked view redaction + blind-oracle (oracle_n == 0) across all 29 Iter-32 PII keys + cmcd['a[b[sid]]'] + safe preservation:
    def _sql_key(k: str) -> str:
        return k.replace("'", "''")

    proj_sql = (
        "SELECT "
        + ", ".join(f"custom_fields['{_sql_key(k)}'] AS p_{i}" for i, (k, _) in enumerate(iter32_pii_pairs))
        + ", "
        + ", ".join(f"custom_fields['{_sql_key(k)}'] AS np_{i}" for i, (k, _) in enumerate(iter32_safe_pairs))
        + ", cmcd['a[b[sid]]'] AS cmcd_p, cmcd['CMCD_BR'] AS cmcd_s FROM logs WHERE line_ordinal = 1"
    )
    masked_row = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=proj_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"][0]
    raw_row = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql=proj_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False, tables=raw_tables,
    )["data"][0]
    for i, (k, raw_secret) in enumerate(iter32_pii_pairs):
        assert masked_row[f"p_{i}"] == "[redacted]", f"Unredacted {k}: {masked_row[f'p_{i}']!r}"
        assert raw_row[f"p_{i}"] == raw_secret, f"Raw lost {k}: {raw_row[f'p_{i}']!r}"
        oracle_sql = f"SELECT count() AS oracle_n FROM logs WHERE custom_fields['{_sql_key(k)}'] = '{_sql_key(raw_secret)}'"
        oracle_res = hs_query.query_endpoint(
            svc, DashboardQueryRequest(sql=oracle_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert int(oracle_res[0]["oracle_n"]) == 0, f"Blind oracle leaked {k}: {oracle_res}"
    assert masked_row["cmcd_p"] == "[redacted]"
    assert raw_row["cmcd_p"] == "cmcd-nested-sid-99"
    assert masked_row["cmcd_s"] == "3200"
    for i, (nk, nv) in enumerate(iter32_safe_pairs):
        assert masked_row[f"np_{i}"] == nv, f"False redaction on {nk}: {masked_row[f'np_{i}']!r}"

    # 2. Valid ClickHouse syntax: PREWHERE (...) on MergeTree, SELECT ip, * EXCEPT (...), ARRAY JOIN ... JOIN, PASTE JOIN, POSITION('a' IN url):
    prewhere_ok = hs_query.query_endpoint(
        svc, DashboardQueryRequest(sql="SELECT url FROM logs PREWHERE (status = 200) ORDER BY url", max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=False, tables=None,
    )["data"]
    assert [r["url"] for r in prewhere_ok] == ["/r1", "/r2"], prewhere_ok
    for valid_ch_sql in (
        "SELECT ip, * EXCEPT (custom_fields, cmcd) FROM logs WHERE line_ordinal = 1",
        "SELECT l.url, x, l3.url AS u3 FROM logs AS l JOIN logs AS l2 USING (url) ARRAY JOIN [1] AS x JOIN logs AS l3 ON l.url = l3.url WHERE l.line_ordinal = 1",
        "SELECT l.url, r.url AS r_url FROM logs AS l PASTE JOIN logs AS r LIMIT 2",
        "SELECT url FROM logs WHERE POSITION('r1' IN url) > 0",
    ):
        res_ok = hs_query.query_endpoint(
            svc, DashboardQueryRequest(sql=valid_ch_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert len(res_ok) >= 1, f"Expected rows for valid ClickHouse SQL: {valid_ch_sql}"

    # 3. Real chdb == duckdb numeric differential parity across '', 'abc', '10.0', '007', '-0', '-10', IN/NOT IN, %, CAST, CASE, arithmetic LHS, reversed BETWEEN:
    dcon = duckdb.connect(":memory:")
    dcon.execute(
        "CREATE TABLE logs (url VARCHAR, bot_score DOUBLE, bytes BIGINT, status BIGINT); "
        "INSERT INTO logs VALUES "
        "('/r1', 10.0, 250, 200), "
        "('/r2', 7.0, 100, 200), "
        "('/r3', 0.0, 0, 404), "
        "('/r4', NULL, 500, 500), "
        "('/r5', NULL, 750, 503), "
        "('/r6', -10.0, 300, 201);"
    )
    parity_queries = (
        "SELECT url FROM logs WHERE bot_score = 10 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = 7 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = 0 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score != 10 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score < 10 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score >= 0 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score BETWEEN -1 AND 1 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score NOT BETWEEN 1 AND 5 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score IN (10, 7) ORDER BY url",
        "SELECT url FROM logs WHERE (bot_score) NOT IN (10, 7) ORDER BY url",
        "SELECT url FROM logs WHERE (bot_score) IN (-10, +10, 7e0) ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = -(10) ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = +(10) ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = 25 % 15 ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = CAST(10 AS DOUBLE) ORDER BY url",
        "SELECT url FROM logs WHERE bot_score = CASE WHEN status = 200 THEN 10 ELSE 7 END ORDER BY url",
        "SELECT url FROM logs WHERE bot_score + 1 = 11 ORDER BY url",
        "SELECT url FROM logs WHERE -(bot_score) = 10 ORDER BY url",
        "SELECT url FROM logs WHERE 5 BETWEEN bot_score AND 10 ORDER BY url",
        "SELECT url FROM logs WHERE 8 BETWEEN bot_score AND 10 ORDER BY url",
    )
    for q_sql in parity_queries:
        duck_urls = [r[0] for r in dcon.execute(q_sql).fetchall()]
        for mask_flag, tbls in ((False, raw_tables), (True, masked_tables)):
            ch_urls = [
                r["url"]
                for r in hs_query.query_endpoint(
                    svc, DashboardQueryRequest(sql=q_sql, max_rows=10),
                    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=mask_flag, tables=tbls,
                )["data"]
            ]
            assert ch_urls == duck_urls, f"Parity mismatch (mask={mask_flag}) on {q_sql}: ch={ch_urls} vs duck={duck_urls}"
    print("ITER32_CHDB_DUCKDB_PARITY_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter31_sql_dos_array_join_on_star_in_pii_and_numeric_parity() -> None:
    """ITER32 remediation: ARRAY JOIN/PASTE JOIN/ON b.*/ON JOIN <kw>/IN <table>/\\'/backtick, DoS, PREWHERE/EXCEPT, numeric parity, PII canonicalization."""
    import os
    import shutil
    import site
    import subprocess
    import time
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale.postgres_control import console_access

    # 1. Exact 18-key MASKED_KEYS == SHARED_PII_KEYS contract + expanded PII canonicalization in console_access.py:
    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)
    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "mapApply((k, v) -> (k, if((arrayExists(seg -> seg != '' AND (seg IN (" in map_view_expr
    assert "decodeURLComponent(" in map_view_expr

    iter32_pii_idents = (
        "meta['user.authorization']",
        "a[b[client_ip]]",
        "a[['req_http_authorization']]",
        "http/authorization",
        "request:client_ip",
        "cmcd['a[b[sid]]']",
        "a%2Fb%5Bclient_ip%5D",
        "%61uthorization",
        "clientIP",
        "sessionID",
        "xAuthToken",
        "xAccessToken",
        "xSessionId",
        "xSessionToken",
        "xForwardedClientIp",
        "HTTPAuthorization",
        "-filter-cookie",
        "session_id_1-",
        "client_ip_1!",
        "authorization_1/",
        "client_ip1",
        "session_id2",
        "sid1",
        "authorization_1",
        "authorization\r\n",
        "req_http_filter_x_real_ip",
        "x_auth_token",
        "x_access_token",
        "x_session_id",
        "x_session_token",
        "x_forwarded_client_ip",
    )
    for pii_ident in iter32_pii_idents:
        assert console_access._is_console_pii_identifier(pii_ident), f"console_access missed {pii_ident!r}"
        assert console_access._redaction(pii_ident, "String", keys_sql) is not None, f"_redaction missed {pii_ident!r}"
        assert hs_query._classify_console_pii_key(pii_ident) is not None, f"query.py missed {pii_ident!r}"
    for safe_ident in (
        "cache-status",
        "origin-time",
        "session_duration_ms",
        "ip_version",
        "client_asn",
        "token_count",
        "CMCD_BR",
        "http_status",
        "req_id",
        "ipv6",
    ):
        assert not console_access._is_console_pii_identifier(safe_ident), f"console_access FP {safe_ident!r}"
        assert hs_query._classify_console_pii_key(safe_ident) is None, f"query.py FP {safe_ident!r}"

    # 2. CRITICAL cross-tenant / arbitrary-table bypasses blocked directly in validate_user_sql:
    direct_validator_attacks = (
        "SELECT * FROM logs ARRAY JOIN [1] AS x JOIN secret_other_tenant ON 1=1",
        "SELECT * FROM logs ARRAY JOIN [1] AS x LEFT JOIN secret_other_tenant ON 1=1",
        "SELECT * FROM logs ARRAY JOIN [1] AS x GLOBAL LEFT JOIN secret_other_tenant ON 1=1",
        "SELECT * FROM logs ARRAY JOIN [1] AS x ASOF JOIN secret_other_tenant ON 1=1",
        "SELECT * FROM logs ARRAY JOIN [1] AS x PASTE JOIN secret_other_tenant",
        "SELECT * FROM logs AS l PASTE JOIN secret_other_tenant AS r",
        "SELECT * FROM logs AS a JOIN logs AS b ON a.url = b.* JOIN secret_other_tenant AS c ON 1=1",
        "SELECT * FROM logs AS a JOIN logs AS b ON a.url = b.url JOIN at AS x ON 1=1",
        "SELECT * FROM logs AS a JOIN logs AS b ON a.url = b.url JOIN in AS x ON 1=1",
        "SELECT * FROM logs AS a JOIN logs AS b ON a.url = b.url JOIN not AS x ON 1=1",
        "SELECT * FROM logs WHERE 1 IN system.numbers",
        "SELECT * FROM logs WHERE 1 GLOBAL IN system.numbers",
        "SELECT * FROM logs WHERE 1 NOT IN secret_other_tenant",
        "SELECT trim(CASE WHEN 1 GLOBAL IN system.numbers THEN 'a' ELSE 'b' END) FROM logs",
        "SELECT overlay(CASE WHEN 1 IN system.numbers THEN 'a' ELSE 'b' END PLACING 'x' FROM 1) FROM logs",
        "SELECT position('a', CASE WHEN 'x' IN system.tables THEN 'a' ELSE 'b' END) FROM logs",
        "SELECT position(CASE WHEN 'x' IN system.tables THEN 'a' ELSE 'b' END IN url) FROM logs",
        "SELECT 'a\\', ' FROM logs UNION ALL SELECT secret_col FROM secret_other_tenant -- ' FROM logs",
        "SELECT `file`('/etc/passwd') FROM logs",
        "SELECT * FROM `secret_other_tenant`",
    )
    for atk_sql in direct_validator_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk_sql, allowed_tables=frozenset({"logs"}))

    # 3. Direct rewriter defense-in-depth in query.py for TRIM/OVERLAY/POSITION bare IN, ARRAY JOIN, PASTE JOIN, PREWHERE, * EXCEPT (...):
    for bad_in_sql in (
        "SELECT trim(CASE WHEN 1 GLOBAL IN system.numbers THEN 'a' ELSE 'b' END) FROM logs",
        "SELECT overlay(CASE WHEN 1 IN system.numbers THEN 'a' ELSE 'b' END PLACING 'x' FROM 1) FROM logs",
        "SELECT position('a', CASE WHEN 'x' IN system.tables THEN 'a' ELSE 'b' END) FROM logs",
        "SELECT position(CASE WHEN 'x' IN system.tables THEN 'a' ELSE 'b' END IN url) FROM logs",
        "SELECT position('a' IN url AND 1 IN system.numbers) FROM logs",
    ):
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                bad_in_sql,
                svc_table="request_facts",
                names={"request_facts": "request_facts"},
            )

    rw_array_join = hs_query._rewrite_console_sql(
        "SELECT * FROM logs AS l JOIN logs AS l2 USING (url) ARRAY JOIN [1] AS x JOIN logs AS l3 ON 1=1",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert rw_array_join.count("request_facts") == 3, rw_array_join
    assert " logs " not in f" {rw_array_join} ", rw_array_join

    rw_paste = hs_query._rewrite_console_sql(
        "SELECT l.url, r.url FROM logs AS l PASTE JOIN logs AS r",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert rw_paste.count("request_facts") == 2, rw_paste

    rw_prewhere = hs_query._rewrite_console_sql(
        "SELECT url FROM logs PREWHERE (status = 200)",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert "AS PREWHERE" not in rw_prewhere, rw_prewhere
    assert "PREWHERE (" in rw_prewhere and "custom_fields['status']" in rw_prewhere, rw_prewhere

    rw_prewhere_col = hs_query._rewrite_console_sql(
        "SELECT url FROM logs PREWHERE (status)",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert "AS PREWHERE" not in rw_prewhere_col, rw_prewhere_col
    assert "PREWHERE (custom_fields['status'])" in rw_prewhere_col, rw_prewhere_col

    assert validate_user_sql("SELECT url FROM logs PREWHERE (status = 200)", allowed_tables={"logs"}).parse_tree
    assert validate_user_sql("SELECT url FROM logs PREWHERE (status)", allowed_tables={"logs"}).parse_tree
    assert validate_user_sql("SELECT url FROM logs WHERE POSITION('a' IN url) > 0", allowed_tables={"logs"}).parse_tree

    rw_except = hs_query._rewrite_console_sql(
        "SELECT ip, * EXCEPT (status) FROM logs",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert rw_except == "SELECT client_ip, * EXCEPT (status) FROM request_facts", rw_except

    rw_alias_except = hs_query._rewrite_console_sql(
        "SELECT l.ip, l.* EXCEPT (status) FROM logs AS l",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert rw_alias_except == "SELECT l.client_ip, l.* EXCEPT (status) FROM request_facts AS l", rw_alias_except

    # 4. Algorithmic DoS elimination (depth limit + O(N) paren match + O(1) AS-run parity) in BOTH sql_validator.py and query.py:
    deep_parens = "(" * 500 + "1" + ")" * 500
    deep_select = f"SELECT {deep_parens} FROM logs"
    deep_wrapped = "(" * 500 + "SELECT 1 FROM logs" + ")" * 500
    for deep_sql in (deep_parens, deep_select, deep_wrapped):
        t0 = time.perf_counter()
        with pytest.raises(SQLValidationError):
            validate_user_sql(deep_sql, allowed_tables=frozenset({"logs"}))
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        assert elapsed_ms < 50.0, f"validate_user_sql deep parens took {elapsed_ms:.1f}ms"

    t0 = time.perf_counter()
    with pytest.raises(SQLValidationError):
        hs_query._rewrite_console_sql(
            deep_select,
            svc_table="request_facts",
            names={"request_facts": "request_facts"},
        )
    assert (time.perf_counter() - t0) * 1000.0 < 50.0

    as_chain = "SELECT 1" + " AS" * 2000 + " x FROM logs"
    t0 = time.perf_counter()
    try:
        validate_user_sql(as_chain, allowed_tables=frozenset({"logs"}))
    except SQLValidationError:
        pass
    val_as_ms = (time.perf_counter() - t0) * 1000.0
    assert val_as_ms < 50.0, f"validate_user_sql AS-chain took {val_as_ms:.1f}ms"

    t0 = time.perf_counter()
    hs_query._rewrite_console_sql(
        as_chain,
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    rw_as_ms = (time.perf_counter() - t0) * 1000.0
    assert rw_as_ms < 50.0, f"_rewrite_console_sql AS-chain took {rw_as_ms:.1f}ms"

    # 5. Post-processing PII masking in query.py: qualified arrayElement(r.custom_fields, ...), list/tuple parent propagation, int/float scalars:
    assert hs_query._classify_console_pii_key("arrayElement(r.custom_fields, 'authorization')") == "session"
    assert hs_query._classify_console_pii_key("arrayElement(r.custom_fields, 'clientIP')") == "ip"
    masked_rows = hs_query._mask_nested_extra_pii(
        [
            {
                "arrayElement(r.custom_fields, 'authorization')": "Bearer qual-secret",
                "authorization": ["Bearer list-secret-1", "Bearer list-secret-2"],
                "session_id": 987654321,
                "x_auth_token": 42.5,
                "custom_fields": {
                    "authorization": ["Bearer nested-list-1", 111222],
                    "x_forwarded_client_ip": ("198.51.100.1", "203.0.113.2"),
                    "session_id": 123456,
                    "cache-status": "HIT",
                },
                "cache-status": "HIT",
            }
        ]
    )
    m0 = masked_rows[0]
    assert m0["arrayElement(r.custom_fields, 'authorization')"] == "[redacted]"
    assert m0["authorization"] == ["[redacted]", "[redacted]"]
    assert m0["session_id"] == "[redacted]"
    assert m0["x_auth_token"] == "[redacted]"
    assert m0["custom_fields"]["authorization"] == ["[redacted]", "[redacted]"]
    assert m0["custom_fields"]["x_forwarded_client_ip"] == ("198.51.100.xxx", "203.0.113.xxx")
    assert m0["custom_fields"]["session_id"] == "[redacted]"
    assert m0["custom_fields"]["cache-status"] == "HIT"
    assert m0["cache-status"] == "HIT"

    # 6. Numeric predicate coercion in query.py: IN/NOT IN, %, CAST, CASE, arithmetic LHS, reversed BETWEEN, NULL-safe comparisons:
    rw_num = hs_query._rewrite_console_sql(
        "SELECT url FROM logs WHERE bot_score IN (10, 7) AND (bot_score) NOT IN (-1, +2) "
        "AND bot_score = 25 % 15 AND bot_score + 1 = 11 AND 5 BETWEEN bot_score AND 10 "
        "AND bot_score = CAST(10 AS DOUBLE) AND bot_score = CASE WHEN status = 200 THEN 10 ELSE 7 END",
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) IN (10, 7)" in rw_num, rw_num
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) NOT IN (-1, +2)" in rw_num, rw_num
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) = 25 % 15" in rw_num, rw_num
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) + 1 = 11" in rw_num, rw_num
    assert "5 BETWEEN toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) AND 10" in rw_num, rw_num
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) = CAST(10 AS DOUBLE)" in rw_num, rw_num
    assert "toFloat64OrNull(nullIf(custom_fields['bot_score'], '')) = CASE WHEN" in rw_num, rw_num

    # 7. Real chdb + duckdb execution for all Iter-32 PII keys, valid ClickHouse syntax, and numeric differential parity:
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401
        import duckdb as _duckdb  # noqa: F401

        exec(_REAL_CHDB_DUCKDB_ITER32_SCRIPT, {})
    except ImportError:
        uv_bin = shutil.which("uv")
        if uv_bin is not None:
            py_paths = [repo_root, *site.getsitepackages()]
            env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
            env["PYTHONPATH"] = os.pathsep.join(py_paths)
            probe = subprocess.run(
                [
                    uv_bin,
                    "run",
                    "--offline",
                    "--no-project",
                    "--with",
                    "chdb",
                    "--with",
                    "duckdb",
                    "python",
                    "-c",
                    "import chdb, duckdb",
                ],
                cwd=repo_root,
                env=env,
                capture_output=True,
                text=True,
            )
            if probe.returncode == 0:
                run_res = subprocess.run(
                    [
                        uv_bin,
                        "run",
                        "--offline",
                        "--no-project",
                        "--with",
                        "chdb",
                        "--with",
                        "duckdb",
                        "python",
                        "-c",
                        _REAL_CHDB_DUCKDB_ITER32_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb/duckdb Iter32 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER32_CHDB_DUCKDB_PARITY_OK" in run_res.stdout
