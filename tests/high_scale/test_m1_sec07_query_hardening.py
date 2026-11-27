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
    assert "toValidUTF8(" in map_view_expr and "unhex(" in map_view_expr and "splitByChar('%'" in map_view_expr

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

    import sys

    _under_cov = (
        sys.gettrace() is not None
        or bool(os.environ.get("COV_CORE_SOURCE"))
        or (hasattr(sys, "monitoring") and sys.monitoring.get_tool(sys.monitoring.COVERAGE_ID) is not None)
    )
    _as_budget_ms = 1000.0 if _under_cov else 250.0

    as_chain = "SELECT 1" + " AS" * 2000 + " x FROM logs"
    t0 = time.perf_counter()
    try:
        validate_user_sql(as_chain, allowed_tables=frozenset({"logs"}))
    except SQLValidationError:
        pass
    val_as_ms = (time.perf_counter() - t0) * 1000.0
    assert val_as_ms < _as_budget_ms, f"validate_user_sql AS-chain took {val_as_ms:.1f}ms"

    t0 = time.perf_counter()
    hs_query._rewrite_console_sql(
        as_chain,
        svc_table="request_facts",
        names={"request_facts": "request_facts"},
    )
    rw_as_ms = (time.perf_counter() - t0) * 1000.0
    assert rw_as_ms < _as_budget_ms, f"_rewrite_console_sql AS-chain took {rw_as_ms:.1f}ms"

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


_ITER33_WAVE32_PII_KEYS: tuple[str, ...] = (
    "CF-Connecting-IPv6",
    "CF_Connecting_IPv6",
    "Connecting-IPv6",
    "cf_connecting_IPv6",
    "CFConnectingIPv6",
    "req.http.CF-Connecting-IPv6",
    "req.http.CF_Connecting_IPv6",
    "header_cf_connecting_ipv6",
    "x_filter_cf_connecting_ipv6",
    "ClientIPv6",
    "client_ipv6",
    "client_ip_v6",
    "True-Client-IPv6",
    "Fastly-Client-IPv6",
    "X-Real-IPv6",
    "OriginIPv6",
    "auth_token_v2",
    "AuthTokenV2",
    "api_key_v2",
    "session_id_v2",
    "cookie_v2",
    "cookie_session_v2",
    "access_token_v2",
    "session_id_hash",
    "x_real_ip_addr",
    "header__http_cookie",
    "req__http__authorization",
    "client%255fip",
    "req%252ehttp%252ecookie",
    "%43%46%2d%43%6f%6e%6e%65%63%74%69%6e%67%2d%49%50%76%36",
    "foo;cookie",
    "hdr|client_ip",
)

_ITER33_BENIGN_KEYS: tuple[str, ...] = (
    "ipv6",
    "is_ipv6",
    "http_version",
    "cache_status",
    "status_v2",
    "bot_score",
    "request_method",
    "content_type",
    "url_path",
    "country_code",
    "cache-status",
    "origin-time",
    "session_duration_ms",
    "ip_version",
    "client_asn",
    "token_count",
    "CMCD_BR",
    "http_status",
    "req_id",
)

_REAL_CHDB_ITER33_SCRIPT = r"""
import json
from types import SimpleNamespace
import chdb
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pii_keys = (
    "CF-Connecting-IPv6",
    "CF_Connecting_IPv6",
    "Connecting-IPv6",
    "cf_connecting_IPv6",
    "CFConnectingIPv6",
    "req.http.CF-Connecting-IPv6",
    "req.http.CF_Connecting_IPv6",
    "header_cf_connecting_ipv6",
    "x_filter_cf_connecting_ipv6",
    "ClientIPv6",
    "client_ipv6",
    "client_ip_v6",
    "True-Client-IPv6",
    "Fastly-Client-IPv6",
    "X-Real-IPv6",
    "OriginIPv6",
    "auth_token_v2",
    "AuthTokenV2",
    "api_key_v2",
    "session_id_v2",
    "cookie_v2",
    "cookie_session_v2",
    "access_token_v2",
    "session_id_hash",
    "x_real_ip_addr",
    "header__http_cookie",
    "req__http__authorization",
    "client%255fip",
    "req%252ehttp%252ecookie",
    "%43%46%2d%43%6f%6e%6e%65%63%74%69%6e%67%2d%49%50%76%36",
    "foo;cookie",
    "hdr|client_ip",
)
benign_keys = (
    "ipv6",
    "is_ipv6",
    "http_version",
    "cache_status",
    "status_v2",
    "bot_score",
    "request_method",
    "content_type",
    "url_path",
    "country_code",
    "cache-status",
    "origin-time",
    "session_duration_ms",
    "ip_version",
    "client_asn",
    "token_count",
    "CMCD_BR",
    "http_status",
    "req_id",
)

sess = chdb.session.Session()
sess.query(
    "CREATE TABLE high_scale_batch_publications ("
    "service_id String, domain String, batch_id String, publication_state String"
    ") ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
)
sess.query("INSERT INTO high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')")
sess.query(
    "CREATE TABLE request_facts ("
    "service_id String, batch_id String, publication_state String, "
    "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
    "url String, client_ip String, event_id String, bytes UInt64, "
    "custom_fields Map(String, String), cmcd Map(String, String)"
    ") ENGINE = MergeTree ORDER BY (service_id, event_timestamp, line_ordinal)"
)
sess.query("CREATE TABLE tenant_b_secrets (val String) ENGINE = Memory")
sess.query("INSERT INTO tenant_b_secrets VALUES ('TENANT_B_SECRET_VAL')")
sess.query("CREATE TABLE secret_1col (v String) ENGINE = Memory")
sess.query("INSERT INTO secret_1col VALUES ('1'), ('TENANT_B_SECRET_VAL')")
sess.query("CREATE TABLE secret_2col (a UInt32, b String) ENGINE = Memory")
sess.query("INSERT INTO secret_2col VALUES (1, 'SECRET_TENANT_B')")

def _ch_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

pii_map_entries = []
pii_secrets = {}
for idx, k in enumerate(pii_keys):
    secret_val = f"2001:db8:cafe::{idx + 1}" if "ip" in k.lower() else f"secret_tok_{idx}_xyz"
    pii_secrets[k] = secret_val
    pii_map_entries.append(f"{_ch_str(k)}: {_ch_str(secret_val)}")

benign_vals = {}
for idx, k in enumerate(benign_keys):
    bval = f"benign_val_{idx}"
    benign_vals[k] = bval
    pii_map_entries.append(f"{_ch_str(k)}: {_ch_str(bval)}")

pii_map_entries.append("'status': '200'")
pii_map_entries.append("'edge': 'true'")
row1_cf = "{" + ", ".join(pii_map_entries) + "}"
row2_cf = "{'status': '500', 'edge': 'false'}"
sess.query(
    "INSERT INTO request_facts VALUES "
    f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/r1', '198.51.100.10', 'req-1', 250, {row1_cf}, {{}}), "
    f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:11:00.000000', 2, '/r2', '198.51.100.11', 'req-2', 100, {row2_cf}, {{}})"
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
masked_tables = console_access.console_tables("svc_alpha", masked=True)

# 1. Exact 3-way parity across all 32 Wave-32 PII keys + 19 benign controls in real chdb masked view:
view_name = masked_tables["request_facts"]
view_cf = json.loads(
    sess.query(f"SELECT custom_fields FROM `{view_name}` WHERE line_ordinal = 1", "JSON").bytes()
)["data"][0]["custom_fields"]

for k in pii_keys:
    chdb_hit = view_cf.get(k) == "[redacted]"
    ca_py = console_access._is_console_pii_identifier(k)
    q_py = hs_query._classify_console_pii_key(k) is not None
    assert chdb_hit and ca_py and q_py, (
        f"3-way PII parity failure on {k!r}: chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
    )

for k in benign_keys:
    chdb_hit = view_cf.get(k) == "[redacted]"
    ca_py = console_access._is_console_pii_identifier(k)
    q_py = hs_query._classify_console_pii_key(k) is not None
    assert (not chdb_hit) and (not ca_py) and (not q_py), (
        f"3-way benign false-positive on {k!r}: chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
    )
    assert view_cf.get(k) == benign_vals[k], f"Benign value mutated for {k!r}: {view_cf.get(k)!r}"

# 2. End-to-end query_endpoint(..., mask_ips=True) exfiltration probes:
r_rep = hs_query.query_endpoint(
    svc,
    DashboardQueryRequest(
        sql="SELECT replaceAll(custom_fields['CF-Connecting-IPv6'], ':', '_') AS x FROM logs WHERE line_ordinal = 1",
        max_rows=10,
    ),
    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
)["data"]
assert r_rep == [{"x": "[redacted]"}], f"replaceAll exfil leaked: {r_rep}"

r_auth = hs_query.query_endpoint(
    svc,
    DashboardQueryRequest(sql="SELECT custom_fields['auth_token_v2'] AS a FROM logs WHERE line_ordinal = 1", max_rows=10),
    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
)["data"]
assert r_auth == [{"a": "[redacted]"}], f"auth_token_v2 leaked: {r_auth}"

r_hdr = hs_query.query_endpoint(
    svc,
    DashboardQueryRequest(sql="SELECT custom_fields['header__http_cookie'] AS c FROM logs WHERE line_ordinal = 1", max_rows=10),
    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
)["data"]
assert r_hdr == [{"c": "[redacted]"}], f"header__http_cookie leaked: {r_hdr}"

r_vals = hs_query.query_endpoint(
    svc,
    DashboardQueryRequest(sql="SELECT mapValues(custom_fields) AS vals FROM logs WHERE line_ordinal = 1", max_rows=10),
    "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
)["data"][0]["vals"]
for k, raw_secret in pii_secrets.items():
    assert raw_secret not in r_vals, f"mapValues leaked raw secret for {k!r}: {raw_secret!r}"
assert "[redacted]" in r_vals
for bk, bv in benign_vals.items():
    assert bv in r_vals, f"mapValues lost benign value for {bk!r}"

# 3. End-to-end query_endpoint(..., mask_ips=True) blocks ALL IN (<table_ident>) & in(x, <table_ident>) bypasses:
in_bypasses = (
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_VAL' IN (tenant_b_secrets)",
    "SELECT url FROM logs WHERE in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE 0 IN (system.one)",
    "SELECT url FROM logs WHERE in(0, system.one)",
    "SELECT url FROM logs WHERE '1' GLOBAL IN (secret_1col)",
    "SELECT url FROM logs WHERE '1' NOT IN (secret_1col)",
    "SELECT url FROM logs WHERE '1' GLOBAL NOT IN (secret_1col)",
    "SELECT url FROM logs WHERE '1' IN ((secret_1col))",
    "SELECT url FROM logs WHERE '1' IN (((secret_1col)))",
    "SELECT url FROM logs WHERE '1' IN (default.secret_1col)",
    "SELECT url FROM logs WHERE (line_ordinal, 'SECRET_TENANT_B') IN (secret_2col)",
    "SELECT url FROM logs WHERE notIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE globalIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE globalNotIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE nullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE notNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE globalNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE globalNotNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE NOT in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs ORDER BY in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    "SELECT url FROM logs WHERE position(1 AND 1 IN tenant_b_secrets) >= 0",
    "SELECT url FROM logs WHERE position(1 = 1 IN tenant_b_secrets) >= 0",
    "SELECT url FROM logs WHERE position(1 OR 1 IN tenant_b_secrets) >= 0",
)
for atk in in_bypasses:
    try:
        hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=atk, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )
        raise AssertionError(f"query_endpoint FAILED TO BLOCK IN bypass: {atk}")
    except PermissionError:
        pass

# 4. Legitimate IN & POSITION queries still execute cleanly in real chdb:
for ok_sql, expected_urls in (
    ("SELECT url FROM logs WHERE url IN ('/r1', '/r2') ORDER BY url", ["/r1", "/r2"]),
    ("SELECT url FROM logs WHERE status IN (200, 500) ORDER BY url", ["/r1", "/r2"]),
    ("SELECT url FROM logs WHERE url IN (SELECT url FROM logs WHERE status = 200)", ["/r1"]),
    ("SELECT url FROM logs WHERE url NOT IN (SELECT url FROM logs WHERE status = 500)", ["/r1"]),
    ("SELECT url FROM logs WHERE position('r1' IN url) > 0", ["/r1"]),
):
    got = [
        r["url"]
        for r in hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
    ]
    assert got == expected_urls, f"Legitimate query failed {ok_sql}: got={got}, expected={expected_urls}"

print("ITER33_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter33_in_table_bypass_dos_and_pii_3way_parity() -> None:
    """ITER33 remediation: ClickHouse IN (tbl)/in(x,tbl) & position() bypasses, 3 quadratic DoS vectors, and PII 3-way parity."""
    import os
    import shutil
    import site
    import subprocess
    import time
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.high_scale.postgres_control import console_access

    # ── 1. IN (<table_ident>), in(x, <table_ident>), ClickHouse *In functions, and position() defense-in-depth ──
    validator_in_attacks = (
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_VAL' IN (tenant_b_secrets)",
        "SELECT url FROM logs WHERE in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE 0 IN (system.one)",
        "SELECT url FROM logs WHERE in(0, system.one)",
        "SELECT url FROM logs WHERE id GLOBAL IN (secret_1col)",
        "SELECT url FROM logs WHERE id NOT IN (secret_1col)",
        "SELECT url FROM logs WHERE id GLOBAL NOT IN (secret_1col)",
        "SELECT url FROM logs WHERE id IN ((secret_1col))",
        "SELECT url FROM logs WHERE id IN (((secret_1col)))",
        "SELECT url FROM logs WHERE id IN (default.secret_1col)",
        "SELECT url FROM logs WHERE id IN ((default.secret_1col))",
        "SELECT url FROM logs WHERE (id, 'SECRET_TENANT_B') IN (secret_2col)",
        "SELECT url FROM logs WHERE 0 IN (`system`.`one`)",
        'SELECT url FROM logs WHERE 0 IN ("system"."one")',
        "SELECT url FROM logs WHERE NOT in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE in ('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs ORDER BY in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT count() FROM logs GROUP BY in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE 1 ? in('TENANT_B_SECRET_VAL', tenant_b_secrets) : 0",
        "SELECT url FROM logs WHERE arrayExists(x -> in(x, secret_1col), ['TENANT_B_SECRET_VAL'])",
        "SELECT TOP 1 in('TENANT_B_SECRET_VAL', tenant_b_secrets) FROM logs",
        "SELECT url FROM logs WHERE ('TENANT_B_SECRET_VAL' IN (tenant_b_secrets))",
        "SELECT coalesce(in('TENANT_B_SECRET_VAL', tenant_b_secrets), 0) FROM logs",
        "SELECT url FROM logs WHERE notIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNotIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE nullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE notNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNotNullIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE inIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE notInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNotInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE nullInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE notNullInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNullInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalNotNullInIgnoreSet('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT position(1 AND 1 IN secret_table) FROM logs",
        "SELECT position(1 OR 1 IN secret_table) FROM logs",
        "SELECT position(1 = 1 IN secret_table) FROM logs",
        "SELECT position(1 != 2 IN secret_table) FROM logs",
        "SELECT position(1 < 2 IN secret_table) FROM logs",
        "SELECT position(1 >= 2 IN secret_table) FROM logs",
        "SELECT position(NOT 1 IN secret_table) FROM logs",
        "SELECT position(1 AND 1 IN (secret_table)) FROM logs",
    )
    for atk_sql in validator_in_attacks:
        with pytest.raises(SQLValidationError, match=r".+"):
            validate_user_sql(atk_sql, allowed_tables=frozenset({"logs"}))

    # system.* blocked EVEN when allowed_tables=None:
    for sys_atk in (
        "SELECT url FROM logs WHERE 0 IN (system.one)",
        "SELECT url FROM logs WHERE 0 IN ((`system`.`one`))",
        "SELECT url FROM logs WHERE in(0, system.one)",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(sys_atk, allowed_tables=None)

    # Direct query.py defense-in-depth (_reject_unsafe_functions AND _rewrite_console_sql):
    for fn_atk in (
        "SELECT url FROM logs WHERE in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE NOT in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE in(0, system.one)",
        "SELECT url FROM logs ORDER BY in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE notIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE globalIn('TENANT_B_SECRET_VAL', tenant_b_secrets)",
    ):
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(fn_atk)

    for rw_in_atk in (
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_VAL' IN (tenant_b_secrets)",
        "SELECT url FROM logs WHERE in('TENANT_B_SECRET_VAL', tenant_b_secrets)",
        "SELECT url FROM logs WHERE 0 IN (system.one)",
        "SELECT url FROM logs WHERE id IN ((secret_1col))",
        "SELECT url FROM logs WHERE id IN (((secret_1col)))",
        "SELECT url FROM logs WHERE id IN (default.secret_1col)",
        "SELECT url FROM logs WHERE id IN (logs)",
        "SELECT url FROM logs WHERE id IN (request_facts)",
        "SELECT position(1 AND 1 IN secret_table) FROM logs",
        "SELECT position(1 OR 1 IN secret_table) FROM logs",
        "SELECT position(1 = 1 IN secret_table) FROM logs",
    ):
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                rw_in_atk,
                svc_table="request_facts",
                names={"request_facts": "request_facts"},
            )

    # Legitimate IN & POSITION queries MUST still pass:
    for benign_sql in (
        "SELECT url FROM logs WHERE url IN ('/a', '/b')",
        "SELECT url FROM logs WHERE status IN (200, 404)",
        "SELECT url FROM logs WHERE status IN (200)",
        "SELECT url FROM logs WHERE edge IN (true)",
        "SELECT url FROM logs WHERE url IN (SELECT url FROM logs)",
        "SELECT url FROM logs WHERE url NOT IN (SELECT url FROM logs WHERE status = 500)",
        "SELECT url FROM logs WHERE position('a' IN url) > 0",
    ):
        assert validate_user_sql(benign_sql, allowed_tables=frozenset({"logs"})).parse_tree
        hs_query._reject_unsafe_functions(benign_sql)
        assert hs_query._rewrite_console_sql(
            benign_sql, svc_table="request_facts", names={"request_facts": "request_facts"}
        )

    # ── 2. ALL 3 Quadratic CPU DoS Vectors (~60 KiB) MUST stay linear (< 1000 ms, CI-safe) ──
    def _best_ms(fn: Any) -> float:
        best = float("inf")
        for _ in range(3):
            t_start = time.perf_counter()
            fn()
            elapsed = (time.perf_counter() - t_start) * 1000.0
            if elapsed < best:
                best = elapsed
        return best

    # 2a: _qual_re dotted-chain regex backtracking in query.py
    dos_2a = "SELECT " + "a." * 28000 + "x FROM logs WHERE edge = true"
    assert len(dos_2a.encode("utf-8")) <= 64 * 1024
    val_2a_ms = _best_ms(lambda: validate_user_sql(dos_2a, allowed_tables=frozenset({"logs"})))
    assert val_2a_ms < 1000.0, f"DoS 2a validate_user_sql took {val_2a_ms:.1f}ms"
    rw_2a_box: list[str] = []

    def _run_2a_rw() -> None:
        rw_s, _ = hs_query._rewrite_console_sql_with_canonicals(
            dos_2a, svc_table="request_facts", names={"request_facts": "request_facts"}
        )
        rw_2a_box.append(rw_s)

    elapsed_2a_ms = _best_ms(_run_2a_rw)
    assert elapsed_2a_ms < 1000.0, f"DoS 2a (_qual_re) took {elapsed_2a_ms:.1f}ms"
    assert "lower(custom_fields['edge']) IN ('true', '1')" in rw_2a_box[-1]

    # 2b: Unclosed CASE in _parse_numeric_case_sp in query.py
    dos_2b = "SELECT status FROM logs WHERE " + ", ".join(["CASE"] * 10000)
    assert len(dos_2b.encode("utf-8")) <= 64 * 1024
    val_2b_ms = _best_ms(lambda: validate_user_sql(dos_2b, allowed_tables=frozenset({"logs"})))
    assert val_2b_ms < 1000.0, f"DoS 2b validate_user_sql took {val_2b_ms:.1f}ms"
    elapsed_2b_ms = _best_ms(
        lambda: hs_query._rewrite_console_sql_with_canonicals(
            dos_2b, svc_table="request_facts", names={"request_facts": "request_facts"}
        )
    )
    assert elapsed_2b_ms < 1000.0, f"DoS 2b (_parse_numeric_case_sp) took {elapsed_2b_ms:.1f}ms"

    # 2c: Unclosed CASE in _has_matching_case_when_end_ahead (sql_validator.py) & _has_matching_case_when_end_lex (query.py)
    dos_2c_1 = "SELECT " + ", ".join(["CASE FROM AS x"] * 3800) + " FROM logs"
    dos_2c_2 = "SELECT 1 FROM logs" + " JOIN logs ON CASE JOIN logs ON 1=1" * 1750
    for label, dos_sql in (("2c_case_from_as", dos_2c_1), ("2c_join_on_case", dos_2c_2)):
        assert len(dos_sql.encode("utf-8")) <= 64 * 1024

        def _run_val(_sql: str = dos_sql) -> None:
            try:
                validate_user_sql(_sql, allowed_tables=frozenset({"logs"}))
            except SQLValidationError:
                pass

        val_ms = _best_ms(_run_val)
        assert val_ms < 1000.0, f"DoS {label} validate_user_sql took {val_ms:.1f}ms"

        def _run_rw(_sql: str = dos_sql) -> None:
            try:
                hs_query._rewrite_console_sql_with_canonicals(
                    _sql, svc_table="request_facts", names={"request_facts": "request_facts"}
                )
            except (PermissionError, SQLValidationError):
                pass

        rw_ms = _best_ms(_run_rw)
        assert rw_ms < 1000.0, f"DoS {label} _rewrite_console_sql took {rw_ms:.1f}ms"

    # ── 3. High-Scale PII Canonicalization & 18-key / prefix invariants + real chdb 3-way parity ──
    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)
    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "mapApply((k, v) -> (k, if((arrayExists(seg -> seg != '' AND (seg IN (" in map_view_expr
    assert "toValidUTF8(" in map_view_expr and "unhex(" in map_view_expr and "splitByChar('%'" in map_view_expr

    for pii_k in _ITER33_WAVE32_PII_KEYS:
        assert console_access._is_console_pii_identifier(pii_k), f"console_access missed {pii_k!r}"
        assert console_access._redaction(pii_k, "String", keys_sql) is not None, f"_redaction missed {pii_k!r}"
        assert hs_query._classify_console_pii_key(pii_k) is not None, f"query.py missed {pii_k!r}"

    for safe_k in _ITER33_BENIGN_KEYS:
        assert not console_access._is_console_pii_identifier(safe_k), f"console_access FP on {safe_k!r}"
        assert hs_query._classify_console_pii_key(safe_k) is None, f"query.py FP on {safe_k!r}"

    # Real chdb verification (3-way parity + replaceAll/mapValues + end-to-end IN bypass blocking):
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER33_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER33_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter33 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER33_CHDB_ALL_OK" in run_res.stdout


_ITER34_WAVE33_PII_KEYS: tuple[str, ...] = (
    # 3a: RE2 vs Python whitespace (leading, trailing, and separator)
    "foo\x0bclient_ip",
    "foo\u00a0client_ip",
    "foo\u200bclient_ip",
    "foo\u3000cookie",
    "\u00a0client_ip\u3000",
    "\x0bauthorization\ufeff",
    "req\u2028session_id",
    "hdr\u202fapi_key",
    "\u200bclient_ip\u200b",
    "foo\u1680cookie",
    "foo\u205fsession_id",
    "foo\x85client_ip",
    # 3b: Unicode lower() & I-dot / combining-dot / Kelvin sign
    "cl\u0130ent_ip",
    "client_\u0130p",
    "author\u0130zation",
    "cook\u0130e",
    "sess\u0130on_id",
    "ap\u0130_key",
    "api_\u212aey",
    "client_i\u0307p",
    "cl%C4%B0ent_ip",
    # 3c: Bracket-index stripping
    "foo.clie[0]nt_ip",
    "foo.c[0]id",
    "clie[0]nt_ip",
    "foo.auth[0]orization",
    "foo.clie['0']nt_ip",
    # 3d: HTTPIp1/CFIp1/REQIp1/RESPIp1/HTTPIp2 camelCase + CFConnectingIPv6 preserved
    "HTTPIp1",
    "CFIp1",
    "REQIp1",
    "RESPIp1",
    "HTTPIp2",
    "FILTERIp1",
    "HEADERIp1",
    "CFConnectingIPv6",
    "HTTPIPv6",
    "CLIENTIPv6",
    # 3e: New prefixes (raw_, orig_, original_) and new PII keys
    "raw_client_ip",
    "orig_ip",
    "original_cookie",
    "raw_http_authorization",
    "src_ip",
    "remote_ip",
    "peer_ip",
    "x_ip",
    "x_cookie",
    "x_authorization",
    "bearer_token",
    "session_cookie",
    "SrcIp",
    "BearerToken",
    "XAuthorization",
)

_ITER34_BENIGN_KEYS: tuple[str, ...] = (
    "raw_status",
    "orig_url",
    "original_method",
    "ipv6",
    "is_ipv6",
    "http_version",
    "cache_status",
    "status_v2",
    "bot_score",
    "request_method",
    "content_type",
    "url_path",
    "country_code",
    "session_duration_ms",
    "ip_version",
    "client_asn",
    "token_count",
    "CMCD_BR",
    "req_id",
    "items[0].status",
    "foo\u00a0cache_status",
)

_REAL_CHDB_ITER34_SCRIPT = r"""
import json
from types import SimpleNamespace
import chdb
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pii_keys = (
    "foo\x0bclient_ip",
    "foo\u00a0client_ip",
    "foo\u200bclient_ip",
    "foo\u3000cookie",
    "\u00a0client_ip\u3000",
    "\x0bauthorization\ufeff",
    "req\u2028session_id",
    "hdr\u202fapi_key",
    "\u200bclient_ip\u200b",
    "foo\u1680cookie",
    "foo\u205fsession_id",
    "foo\x85client_ip",
    "cl\u0130ent_ip",
    "client_\u0130p",
    "author\u0130zation",
    "cook\u0130e",
    "sess\u0130on_id",
    "ap\u0130_key",
    "api_\u212aey",
    "client_i\u0307p",
    "cl%C4%B0ent_ip",
    "foo.clie[0]nt_ip",
    "foo.c[0]id",
    "clie[0]nt_ip",
    "foo.auth[0]orization",
    "foo.clie['0']nt_ip",
    "HTTPIp1",
    "CFIp1",
    "REQIp1",
    "RESPIp1",
    "HTTPIp2",
    "FILTERIp1",
    "HEADERIp1",
    "CFConnectingIPv6",
    "HTTPIPv6",
    "CLIENTIPv6",
    "raw_client_ip",
    "orig_ip",
    "original_cookie",
    "raw_http_authorization",
    "src_ip",
    "remote_ip",
    "peer_ip",
    "x_ip",
    "x_cookie",
    "x_authorization",
    "bearer_token",
    "session_cookie",
    "SrcIp",
    "BearerToken",
    "XAuthorization",
)
benign_keys = (
    "raw_status",
    "orig_url",
    "original_method",
    "ipv6",
    "is_ipv6",
    "http_version",
    "cache_status",
    "status_v2",
    "bot_score",
    "request_method",
    "content_type",
    "url_path",
    "country_code",
    "session_duration_ms",
    "ip_version",
    "client_asn",
    "token_count",
    "CMCD_BR",
    "req_id",
    "items[0].status",
    "foo\u00a0cache_status",
)

sess = chdb.session.Session()
sess.query(
    "CREATE TABLE high_scale_batch_publications ("
    "service_id String, domain String, batch_id String, publication_state String"
    ") ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
)
sess.query("INSERT INTO high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')")
sess.query(
    "CREATE TABLE request_facts ("
    "service_id String, batch_id String, publication_state String, "
    "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
    "url String, client_ip String, event_id String, bytes UInt64, "
    "custom_fields Map(String, String), cmcd Map(String, String)"
    ") ENGINE = MergeTree ORDER BY (service_id, event_timestamp, line_ordinal)"
)
sess.query("CREATE TABLE secret_tenant_b (v String) ENGINE = Memory")
sess.query("INSERT INTO secret_tenant_b VALUES ('TENANT_B_SECRET_847291')")

def _ch_str(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

map_entries = []
pii_secrets = {}
for idx, k in enumerate(pii_keys):
    secret_val = f"198.51.100.{10 + (idx % 200)}" if "ip" in k.lower() else f"iter34_secret_{idx}_xyz"
    pii_secrets[k] = secret_val
    map_entries.append(f"{_ch_str(k)}: {_ch_str(secret_val)}")

benign_vals = {}
for idx, k in enumerate(benign_keys):
    bval = f"iter34_benign_{idx}"
    benign_vals[k] = bval
    map_entries.append(f"{_ch_str(k)}: {_ch_str(bval)}")

map_entries.append("'status': '200'")
map_entries.append("'edge': 'true'")
row1_cf = "{" + ", ".join(map_entries) + "}"
sess.query(
    "INSERT INTO request_facts VALUES "
    f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/r1', '198.51.100.10', 'req-1', 250, {row1_cf}, {{}})"
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
masked_tables = console_access.console_tables("svc_alpha", masked=True)

# 1. Exact 3-way PII parity across all 51 Wave-33 PII keys + 21 benign keys in live chdb:
view_name = masked_tables["request_facts"]
view_cf = json.loads(
    sess.query(f"SELECT custom_fields FROM `{view_name}` WHERE line_ordinal = 1", "JSON").bytes()
)["data"][0]["custom_fields"]

for k in pii_keys:
    chdb_hit = view_cf.get(k) == "[redacted]"
    ca_py = console_access._is_console_pii_identifier(k)
    q_py = hs_query._classify_console_pii_key(k) is not None
    assert chdb_hit and ca_py and q_py, (
        f"Iter34 3-way PII parity failure on {k!r}: chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
    )

for k in benign_keys:
    chdb_hit = view_cf.get(k) == "[redacted]"
    ca_py = console_access._is_console_pii_identifier(k)
    q_py = hs_query._classify_console_pii_key(k) is not None
    assert (not chdb_hit) and (not ca_py) and (not q_py), (
        f"Iter34 3-way benign false-positive on {k!r}: chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
    )
    assert view_cf.get(k) == benign_vals[k], f"Benign value mutated for {k!r}: {view_cf.get(k)!r}"

# 2. End-to-end query_endpoint(..., mask_ips=True) blocks ALL 1a/1b/1c SQL bypasses:
wave33_bypasses = (
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (secret_tenant_b AS s)",
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((secret_tenant_b) AS s)",
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((secret_tenant_b AS a) AS b)",
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (default.secret_tenant_b AS s)",
    "SELECT 0 IN (system.one AS s) FROM logs",
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' NOT IN (secret_tenant_b AS s)",
    "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' GLOBAL IN (secret_tenant_b AS s)",
    "SELECT * REPLACE ('TENANT_B_SECRET_847291' IN (secret_tenant_b AS s) AS url) FROM logs",
    "SELECT url FROM logs WHERE '198.51.100.10' IN (request_facts AS rf)",
    "SELECT url FROM logs WHERE 'b1' IN (high_scale_batch_publications AS p)",
    "WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    "WITH (secret_tenant_b) AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    "WITH (secret_tenant_b AS s) AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    "WITH system.one AS logs SELECT 0 IN (logs) FROM logs",
    "WITH secret_tenant_b AS x SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (x AS y)",
    "WITH secret_tenant_b AS a, a AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    "WITH c AS (SELECT url FROM logs), secret_tenant_b AS logs SELECT url FROM c WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    "SELECT url FROM logs WHERE 1 IN (SELECT 1 FROM main.default.system.logs)",
    "SELECT DISTINCT ON (url) in('TENANT_B_SECRET_847291', secret_tenant_b) FROM logs",
    "SELECT TOP 1 WITH TIES in('TENANT_B_SECRET_847291', secret_tenant_b) FROM logs",
    "SELECT TOP (1) in('TENANT_B_SECRET_847291', secret_tenant_b) FROM logs",
)
for atk in wave33_bypasses:
    try:
        hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=atk, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )
        raise AssertionError(f"query_endpoint FAILED TO BLOCK Wave-33 bypass: {atk}")
    except PermissionError:
        pass

# 3. Benign expression CTEs and standard queries still execute cleanly in live chdb:
for ok_sql, expected in (
    ("WITH 1 + 1 AS two SELECT two, url FROM logs", [{"two": 2, "url": "/r1"}]),
    ("WITH url AS u SELECT u FROM logs", [{"u": "/r1"}]),
    ("SELECT url FROM logs WHERE status IN (200)", [{"url": "/r1"}]),
):
    got = hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql=ok_sql, max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"]
    assert got == expected, f"Legitimate query failed {ok_sql}: got={got}, expected={expected}"

print("ITER34_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter34_in_alias_with_expr_cte_and_parser_parity() -> None:
    """ITER34 remediation (1a-1c): IN (<tbl> AS alias), WITH <tbl> AS <alias>, 4-part table ref, and _is_prefix_in_call_sig parity."""
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    # 1a: IN (<table_ref> AS <alias>) and nested parenthesized aliases:
    in_alias_attacks = (
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((secret_tenant_b) AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((secret_tenant_b AS a) AS b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (((secret_tenant_b AS a) AS b) AS c)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (default.secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((default.secret_tenant_b) AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' NOT IN (secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' GLOBAL IN (secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' GLOBAL NOT IN ((secret_tenant_b) AS s)",
        "SELECT 'TENANT_B_SECRET_847291' IN (secret_tenant_b AS s) FROM logs",
        "SELECT * REPLACE ('TENANT_B_SECRET_847291' IN (secret_tenant_b AS s) AS url) FROM logs",
        "SELECT 0 IN (system.one AS s) FROM logs",
        "SELECT 0 IN (system.one AS `s`) FROM logs",
        'SELECT 0 IN (system.one AS "s") FROM logs',
        "SELECT url FROM logs WHERE 'b1' IN (high_scale_batch_publications AS p)",
    )
    for atk in in_alias_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))

    # system.* with AS alias blocked EVEN when allowed_tables=None:
    for sys_atk in (
        "SELECT 0 IN (system.one AS s) FROM logs",
        "SELECT 0 IN ((system.one) AS s) FROM logs",
        "SELECT 0 IN ((system.one AS a) AS b) FROM logs",
        "SELECT 0 IN (`system`.`one` AS `s`) FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(sys_atk, allowed_tables=None)

    # Rewriter defense-in-depth blocks ALL IN (<tbl> AS alias), including masked-view bypasses:
    for rw_atk in (
        *in_alias_attacks,
        "SELECT url FROM logs WHERE 'x' IN (request_facts AS rf)",
        "SELECT url FROM logs WHERE 'x' IN ((request_facts) AS rf)",
        "SELECT url FROM logs WHERE 'x' IN ((request_facts AS a) AS b)",
        "SELECT * REPLACE ('x' IN (request_facts AS rf) AS url) FROM logs",
        "SELECT url FROM logs WHERE 'x' IN (logs AS l)",
    ):
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                rw_atk,
                svc_table="request_facts",
                names={"request_facts": "fla_cv_svc_m_request_facts"},
            )

    # 1b: ClickHouse WITH <table_ref> AS <allowed_or_alias> bypasses:
    with_expr_attacks = (
        "WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH (secret_tenant_b) AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH (secret_tenant_b AS s) AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH system.one AS logs SELECT 0 IN (logs) FROM logs",
        "WITH secret_tenant_b AS x SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (x AS y)",
        "WITH secret_tenant_b AS a, a AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH c AS (SELECT url FROM logs), secret_tenant_b AS logs SELECT url FROM c WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH x AS (SELECT 'TENANT_B_SECRET_847291' AS v), secret_tenant_b AS x SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (x)",
        "WITH secret_tenant_b AS logs SELECT url FROM logs",
        "SELECT url FROM (WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs))",
        "WITH secret_tenant_b AS logs SELECT url FROM logs UNION ALL SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    )
    for atk in with_expr_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk,
                svc_table="request_facts",
                names={"request_facts": "fla_cv_svc_m_request_facts"},
            )

    # system.* inside WITH expression CTE blocked EVEN when allowed_tables=None:
    for sys_with_atk in (
        "WITH system.one AS logs SELECT 0 IN (logs) FROM logs",
        "WITH (system.one AS s) AS x SELECT 0 IN (x) FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(sys_with_atk, allowed_tables=None)

    # Masked-view bypass via WITH request_facts AS ... blocked in rewriter:
    for mv_with_atk in (
        "WITH request_facts AS logs SELECT url FROM logs WHERE 'x' IN (logs)",
        "WITH request_facts AS x SELECT url FROM logs WHERE 'x' IN (x AS y)",
        "WITH request_facts AS logs SELECT url FROM logs",
    ):
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                mv_with_atk,
                svc_table="request_facts",
                names={"request_facts": "fla_cv_svc_m_request_facts"},
            )

    # Benign ClickHouse expression CTEs still pass:
    for benign_with in (
        "WITH 1 + 1 AS two SELECT two, url FROM logs",
        "WITH url AS u SELECT u FROM logs",
    ):
        assert validate_user_sql(benign_with, allowed_tables=frozenset({"logs"})).parse_tree
        assert hs_query._rewrite_console_sql(
            benign_with, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
        )

    # 1c: 4+-part table identifier rejected + _is_prefix_in_call_sig parity:
    for four_part_sql in (
        "SELECT * FROM main.default.system.logs",
        "SELECT * FROM a.b.c.d",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(four_part_sql, allowed_tables=frozenset({"logs"}))
        with pytest.raises(SQLValidationError):
            validate_user_sql(four_part_sql, allowed_tables=None)

    prefix_in_cases = (
        "SELECT DISTINCT ON (url) in('a', secret_tbl) FROM logs",
        "SELECT TOP 1 WITH TIES in('a', secret_tbl) FROM logs",
        "SELECT TOP (1) in('a', secret_tbl) FROM logs",
        "SELECT TOP (1) WITH TIES in('a', secret_tbl) FROM logs",
    )
    for p_sql in prefix_in_cases:
        sig = [
            (m.lastgroup or "OTHER", m.group(0), m.start())
            for m in hs_query._CONSOLE_SQL_LEX_RE.finditer(p_sql)
            if (m.lastgroup or "OTHER") not in ("WS", "COMMENT")
        ]
        in_idx = next(i for i, (k, t, _) in enumerate(sig) if k == "IDENT" and t.lower() == "in")
        assert hs_query._is_prefix_in_call_sig(sig, in_idx), f"_is_prefix_in_call_sig missed: {p_sql}"
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(p_sql)
        with pytest.raises(SQLValidationError):
            validate_user_sql(p_sql, allowed_tables=frozenset({"logs"}))

    # Non-prefix (status) in (200, 404) remains False:
    benign_in_sql = "SELECT url FROM logs WHERE (status) in (200, 404)"
    benign_sig = [
        (m.lastgroup or "OTHER", m.group(0), m.start())
        for m in hs_query._CONSOLE_SQL_LEX_RE.finditer(benign_in_sql)
        if (m.lastgroup or "OTHER") not in ("WS", "COMMENT")
    ]
    benign_in_idx = next(i for i, (k, t, _) in enumerate(benign_sig) if k == "IDENT" and t.lower() == "in")
    assert not hs_query._is_prefix_in_call_sig(benign_sig, benign_in_idx)


@pytest.mark.security_regression
def test_sec_t5_iter34_linear_time_dos_and_recursion_safety() -> None:
    """ITER34 remediation (2a-2e): 5 CPU DoS vectors (< 250 ms) and 3+ RecursionError crash vectors."""
    import time

    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    def _run_timed_ms(fn: Any) -> float:
        t0 = time.perf_counter()
        fn()
        return (time.perf_counter() - t0) * 1000.0

    # 2a: Derived boolean column loop + subquery_placeholders loop:
    n_cols = 800
    dos_2a_bool = (
        "WITH c AS (SELECT "
        + ", ".join(f"edge b{i}" for i in range(n_cols))
        + " FROM logs) SELECT "
        + ", ".join(f"b{i}" for i in range(n_cols))
        + " FROM c WHERE b0 = true AND false != b1 AND b2 IS NOT TRUE AND b3 IN ('false')"
    )
    assert len(dos_2a_bool.encode("utf-8")) <= 64 * 1024
    rw_2a_box: list[str] = []
    ms_2a_bool = _run_timed_ms(
        lambda: rw_2a_box.append(
            hs_query._rewrite_console_sql(
                dos_2a_bool, svc_table="request_facts", names={"request_facts": "request_facts"}
            )
        )
    )
    assert ms_2a_bool < 1500.0, f"DoS 2a derived-bool loop took {ms_2a_bool:.1f}ms"
    assert "lower(b0) IN ('true', '1')" in rw_2a_box[-1]
    assert "lower(b1) IN ('true', '1')" in rw_2a_box[-1]
    assert "lower(b2) NOT IN ('true', '1')" in rw_2a_box[-1]
    assert "lower(b3) IN ('false', '0', '')" in rw_2a_box[-1]

    dos_2a_subq = "SELECT " + ", ".join("(SELECT 1 FROM logs)" for _ in range(1500)) + " FROM logs"
    assert len(dos_2a_subq.encode("utf-8")) <= 64 * 1024
    ms_2a_subq = _run_timed_ms(
        lambda: hs_query._rewrite_console_sql(
            dos_2a_subq, svc_table="request_facts", names={"request_facts": "request_facts"}
        )
    )
    assert ms_2a_subq < 1500.0, f"DoS 2a subquery_placeholders loop took {ms_2a_subq:.1f}ms"

    # 2b: position(... IN ...) O(N^2) frame rescan:
    for label, dos_2b in (
        ("pos_in_paren", "SELECT position(a" + " IN (1)" * 4000 + ") FROM logs"),
        ("pos_in_bare", "SELECT position(1" + " IN 1" * 4000 + ") FROM logs"),
    ):
        assert len(dos_2b.encode("utf-8")) <= 64 * 1024

        def _val_2b(_s: str = dos_2b) -> None:
            try:
                validate_user_sql(_s, allowed_tables=frozenset({"logs"}))
            except SQLValidationError:
                pass

        def _rw_2b(_s: str = dos_2b) -> None:
            try:
                hs_query._rewrite_console_sql(_s, svc_table="request_facts", names={"request_facts": "request_facts"})
            except (PermissionError, SQLValidationError):
                pass

        val_ms = _run_timed_ms(_val_2b)
        rw_ms = _run_timed_ms(_rw_2b)
        assert val_ms < 1500.0, f"DoS 2b ({label}) validate_user_sql took {val_ms:.1f}ms"
        assert rw_ms < 1500.0, f"DoS 2b ({label}) _rewrite_console_sql took {rw_ms:.1f}ms"

    # 2c: Consecutive JOIN-modifier lookahead O(N^2) across all 3 sites:
    for label, dos_2c in (
        ("any_join", "SELECT 1 FROM logs " + "ANY " * 15000 + "JOIN logs ON 1=1"),
        ("left_no_join", "SELECT 1 FROM logs " + "LEFT " * 12000),
        ("on_left_no_join", "SELECT 1 FROM logs JOIN logs ON 1=1 " + "LEFT " * 12000),
    ):
        assert len(dos_2c.encode("utf-8")) <= 64 * 1024

        def _val_2c(_s: str = dos_2c) -> None:
            try:
                validate_user_sql(_s, allowed_tables=frozenset({"logs"}))
            except SQLValidationError:
                pass

        def _fn_2c(_s: str = dos_2c) -> None:
            try:
                hs_query._reject_unsafe_functions(_s)
            except (PermissionError, ValueError):
                pass

        def _rw_2c(_s: str = dos_2c) -> None:
            try:
                hs_query._rewrite_console_sql(_s, svc_table="request_facts", names={"request_facts": "request_facts"})
            except (PermissionError, SQLValidationError):
                pass

        v_ms = _run_timed_ms(_val_2c)
        f_ms = _run_timed_ms(_fn_2c)
        r_ms = _run_timed_ms(_rw_2c)
        assert v_ms < 1500.0, f"DoS 2c ({label}) validate_user_sql took {v_ms:.1f}ms"
        assert f_ms < 1500.0, f"DoS 2c ({label}) _reject_unsafe_functions took {f_ms:.1f}ms"
        assert r_ms < 1500.0, f"DoS 2c ({label}) _rewrite_console_sql took {r_ms:.1f}ms"

    # 2d: Nested-paren arithmetic re-parse & inner slice O(D * N):
    dos_2d = "SELECT status, " + "(" * 60 + "1+" * 20000 + "1" + ")" * 60 + " FROM logs"
    assert len(dos_2d.encode("utf-8")) <= 64 * 1024
    val_2d_ms = _run_timed_ms(lambda: validate_user_sql(dos_2d, allowed_tables=frozenset({"logs"})))
    rw_2d_ms = _run_timed_ms(
        lambda: hs_query._rewrite_console_sql(
            dos_2d, svc_table="request_facts", names={"request_facts": "request_facts"}
        )
    )
    assert val_2d_ms < 1500.0, f"DoS 2d validate_user_sql took {val_2d_ms:.1f}ms"
    assert rw_2d_ms < 1500.0, f"DoS 2d _rewrite_console_sql took {rw_2d_ms:.1f}ms"

    # 2e: Unhandled RecursionError crashes (AND chain, "! " chain, "!" chain, AND AS AND chain):
    for label, rec_sql in (
        ("and_chain", "SELECT 1 FROM logs JOIN logs ON a" + " AND" * 1500 + " WHERE 1=1"),
        ("bang_space_chain", "SELECT 1 FROM logs JOIN logs ON 1 " + "! " * 1500 + "WHERE 1=1"),
        ("bang_tight_chain", "SELECT 1 FROM logs JOIN logs ON 1 " + "!" * 1500 + " WHERE 1=1"),
        ("and_as_and_chain", "SELECT 1 FROM logs JOIN logs ON a" + " AND AS AND" * 1500 + " WHERE 1=1"),
    ):
        assert len(rec_sql.encode("utf-8")) <= 64 * 1024

        def _val_rec(_s: str = rec_sql) -> None:
            try:
                validate_user_sql(_s, allowed_tables=frozenset({"logs"}))
            except SQLValidationError:
                pass

        def _rw_rec(_s: str = rec_sql) -> None:
            try:
                hs_query._rewrite_console_sql(_s, svc_table="request_facts", names={"request_facts": "request_facts"})
            except (PermissionError, SQLValidationError):
                pass

        vr_ms = _run_timed_ms(_val_rec)
        rr_ms = _run_timed_ms(_rw_rec)
        assert vr_ms < 1500.0, f"Recursion 2e ({label}) validate_user_sql took {vr_ms:.1f}ms"
        assert rr_ms < 1500.0, f"Recursion 2e ({label}) _rewrite_console_sql took {rr_ms:.1f}ms"


@pytest.mark.security_regression
def test_sec_t5_iter34_pii_3way_parity_and_live_chdb() -> None:
    """ITER34 remediation (3a-3e): 100% 3-way PII parity (console_access == query == live chdb) + live chdb SQL bypass checks."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access

    # Exact 18-key contract strictly preserved:
    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)
    # Exact key-set parity between console_access and query:
    assert set(console_access._MAP_MASKED_KEYS) == (
        hs_query._CONSOLE_EXTRA_IP_PII_KEYS | hs_query._CONSOLE_EXTRA_SESSION_PII_KEYS
    )

    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "mapApply((k, v) -> (k, if((arrayExists(seg -> seg != '' AND (seg IN (" in map_view_expr
    assert "toValidUTF8(" in map_view_expr and "unhex(" in map_view_expr and "splitByChar('%'" in map_view_expr

    for pii_k in _ITER34_WAVE33_PII_KEYS:
        assert console_access._is_console_pii_identifier(pii_k), f"console_access missed {pii_k!r}"
        assert console_access._redaction(pii_k, "String", keys_sql) is not None, f"_redaction missed {pii_k!r}"
        assert hs_query._classify_console_pii_key(pii_k) is not None, f"query.py missed {pii_k!r}"

    for safe_k in _ITER34_BENIGN_KEYS:
        assert not console_access._is_console_pii_identifier(safe_k), f"console_access FP on {safe_k!r}"
        assert hs_query._classify_console_pii_key(safe_k) is None, f"query.py FP on {safe_k!r}"

    # Live chdb verification (3-way PII parity across 51 PII + 21 benign keys + end-to-end 1a/1b/1c SQL bypasses):
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER34_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER34_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter34 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER34_CHDB_ALL_OK" in run_res.stdout


_ITER35_WAVE34_PII_KEYS: tuple[str, ...] = (
    # 4a: Invalid UTF-8 after decodeURLComponent
    "%FFclient_ip",
    "client_ip%FF",
    "clie%FFnt_ip",
    "%FFcookie",
    "%FFauthorization",
    "%FFcl\u0130ent_ip",
    "%FFClientIp",
    "%FFclie[0]nt_ip",
    "%FFraw_client_ip",
    "%25FFcookie",
    # 4b: Uppercase U+0130 / U+212A folding before camelCase / IPv[46] splitting
    "HTTP\u0130p1",
    "HTTP\u0130Pv6",
    "Raw\u0130p",
    "HTTP%C4%B0p1",
    "ap\u0130\u212aey",
    # 4c: Multi-level & quote-bearing bracket stripping (+ inner-bracket PII preserved)
    "clie['a\"b']nt_ip",
    "clie[a'b]nt_ip",
    "foo.clie[[0]]nt_ip",
    "foo.c[[0]]id",
    "foo.auth[[0]]orization",
    "foo.clie[[[0]]]nt_ip",
    "a[client_ip]",
    # 4d: nw_first affix parity across all 3 engines
    "f-i-l-t-e-r_session_id",
    "f-i-l-t-e-r_client_ip",
    "x-f-i-l-t-e-r_client_ip",
    "_c-f_client_ip",
    "f.i.l.t.e.r_client_ip",
    "a[f-i-l-t-e-r_client_ip]",
    "f-i-l-t-e-r_cookie",
    # 4e: Invisible format separators & C0 controls
    "foo\u2060cookie",
    "foo\u180ecookie",
    "foo\u200ccookie",
    "foo\u200dcookie",
    "foo\x01cookie",
    "foo\x1fcookie",
    "foo\x7fcookie",
)

_ITER35_BENIGN_KEYS: tuple[str, ...] = (
    *_ITER34_BENIGN_KEYS,
    "foo\u2060cache_status",
    "foo\u180ecache_status",
    "foo\u200ccache_status",
    "foo\x01cache_status",
    "f-i-l-t-e-r_status",
    "x-f-i-l-t-e-r_cache_status",
    "_c-f_url_path",
    "items['a\"b'].status",
    "items[[0]].status",
    "IPV6",
    "IS_IPV6",
    "%FFstatus",
)

_REAL_CHDB_ITER35_SCRIPT = r"""
import json
from types import SimpleNamespace
import chdb
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pii_keys = (
    "%FFclient_ip",
    "client_ip%FF",
    "clie%FFnt_ip",
    "%FFcookie",
    "%FFauthorization",
    "%FFcl\u0130ent_ip",
    "%FFClientIp",
    "%FFclie[0]nt_ip",
    "%FFraw_client_ip",
    "%25FFcookie",
    "HTTP\u0130p1",
    "HTTP\u0130Pv6",
    "Raw\u0130p",
    "HTTP%C4%B0p1",
    "ap\u0130\u212aey",
    "clie['a\"b']nt_ip",
    "clie[a'b]nt_ip",
    "foo.clie[[0]]nt_ip",
    "foo.c[[0]]id",
    "foo.auth[[0]]orization",
    "foo.clie[[[0]]]nt_ip",
    "a[client_ip]",
    "f-i-l-t-e-r_session_id",
    "f-i-l-t-e-r_client_ip",
    "x-f-i-l-t-e-r_client_ip",
    "_c-f_client_ip",
    "f.i.l.t.e.r_client_ip",
    "a[f-i-l-t-e-r_client_ip]",
    "f-i-l-t-e-r_cookie",
    "foo\u2060cookie",
    "foo\u180ecookie",
    "foo\u200ccookie",
    "foo\u200dcookie",
    "foo\x01cookie",
    "foo\x1fcookie",
    "foo\x7fcookie",
)
benign_keys = (
    "raw_status",
    "orig_url",
    "original_method",
    "ipv6",
    "is_ipv6",
    "IPV6",
    "IS_IPV6",
    "http_version",
    "cache_status",
    "status_v2",
    "bot_score",
    "request_method",
    "content_type",
    "url_path",
    "country_code",
    "session_duration_ms",
    "ip_version",
    "client_asn",
    "token_count",
    "CMCD_BR",
    "req_id",
    "items[0].status",
    "items['a\"b'].status",
    "items[[0]].status",
    "foo\u00a0cache_status",
    "foo\u2060cache_status",
    "foo\u180ecache_status",
    "foo\u200ccache_status",
    "foo\x01cache_status",
    "f-i-l-t-e-r_status",
    "x-f-i-l-t-e-r_cache_status",
    "_c-f_url_path",
    "%FFstatus",
)

sess = chdb.session.Session()
sess.query(
    "CREATE TABLE high_scale_batch_publications ("
    "service_id String, domain String, batch_id String, publication_state String"
    ") ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
)
sess.query("INSERT INTO high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')")
sess.query(
    "CREATE TABLE request_facts ("
    "service_id String, batch_id String, publication_state String, "
    "event_timestamp DateTime64(6, 'UTC'), line_ordinal UInt32, "
    "url String, client_ip String, event_id String, bytes UInt64, "
    "custom_fields Map(String, String), cmcd Map(String, String)"
    ") ENGINE = MergeTree ORDER BY (service_id, event_timestamp, line_ordinal)"
)
sess.query("CREATE TABLE secret_tenant_b (v String) ENGINE = Memory")
sess.query("INSERT INTO secret_tenant_b VALUES ('TENANT_B_SECRET_847291')")

def _ch_str(s: str) -> str:
    out = []
    for ch in s:
        o = ord(ch)
        if ch == "\\":
            out.append("\\\\")
        elif ch == "'":
            out.append("\\'")
        elif o < 0x20 or o == 0x7F:
            out.append(f"\\x{o:02x}")
        else:
            out.append(ch)
    return "'" + "".join(out) + "'"

map_entries = []
pii_secrets = {}
for idx, k in enumerate(pii_keys):
    secret_val = f"198.51.100.{10 + (idx % 200)}" if "ip" in k.lower() else f"iter35_secret_{idx}_xyz"
    pii_secrets[k] = secret_val
    map_entries.append(f"{_ch_str(k)}: {_ch_str(secret_val)}")

benign_vals = {}
for idx, k in enumerate(benign_keys):
    bval = f"iter35_benign_{idx}"
    benign_vals[k] = bval
    map_entries.append(f"{_ch_str(k)}: {_ch_str(bval)}")

map_entries.append("'status': '200'")
map_entries.append("'edge': 'true'")
row1_cf = "{" + ", ".join(map_entries) + "}"
sess.query(
    "INSERT INTO request_facts VALUES "
    f"('svc_alpha', 'b1', 'visible', '2026-11-19 00:10:00.000000', 1, '/r1', '198.51.100.10', 'req-1', 250, {row1_cf}, {{}})"
)

desc_rows = json.loads(sess.query("DESCRIBE TABLE default.request_facts", "JSON").bytes())["data"]
cols = [(r["name"], r["type"]) for r in desc_rows if str(r.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")]
for masked in (False, True):
    v_map = console_access.console_tables("svc_alpha", masked=masked)
    v_sql = console_access._view_select("default", "request_facts", cols, "svc_alpha", masked=masked)
    sess.query(f"CREATE VIEW `{v_map['request_facts']}` AS {v_sql}")

for analyzer_val in (0, 1):
    class _ChdbClient:
        _table_filters_verified = True

        def execute(self, sql, settings=None, rewrite_cf=False):
            s_parts = [f"allow_experimental_analyzer = {analyzer_val}"]
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}"
            raw = sess.query(full_sql, "JSON").bytes()
            return json.loads(raw.decode("utf-8"))["data"] if raw else []

    svc = SimpleNamespace(service_id="svc_alpha", client=_ChdbClient())
    masked_tables = console_access.console_tables("svc_alpha", masked=True)
    view_name = masked_tables["request_facts"]

    # 1. 3-way PII parity + zero mapValues(custom_fields) leak via query_endpoint(..., mask_ips=True):
    view_cf = json.loads(
        sess.query(
            f"SELECT custom_fields FROM `{view_name}` WHERE line_ordinal = 1 SETTINGS allow_experimental_analyzer = {analyzer_val}",
            "JSON",
        ).bytes()
    )["data"][0]["custom_fields"]

    for k in pii_keys:
        chdb_hit = view_cf.get(k) == "[redacted]"
        ca_py = console_access._is_console_pii_identifier(k)
        q_py = hs_query._classify_console_pii_key(k) is not None
        assert chdb_hit and ca_py and q_py, (
            f"Iter35 3-way PII parity failure (analyzer={analyzer_val}) on {k!r}: "
            f"chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
        )

    for k in benign_keys:
        chdb_hit = view_cf.get(k) == "[redacted]"
        ca_py = console_access._is_console_pii_identifier(k)
        q_py = hs_query._classify_console_pii_key(k) is not None
        assert (not chdb_hit) and (not ca_py) and (not q_py), (
            f"Iter35 3-way benign FP (analyzer={analyzer_val}) on {k!r}: "
            f"chdb={chdb_hit} (val={view_cf.get(k)!r}), ca_py={ca_py}, q_py={q_py}"
        )
        assert view_cf.get(k) == benign_vals[k], f"Benign value mutated for {k!r}: {view_cf.get(k)!r}"

    mv_res = hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT mapValues(custom_fields) AS vals FROM logs", max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )["data"]
    leaked_blob = json.dumps(mv_res)
    for k, sec in pii_secrets.items():
        assert sec not in leaked_blob, f"query_endpoint mapValues leaked {k!r} secret {sec!r} (analyzer={analyzer_val})"

    # 2. End-to-end query_endpoint(..., mask_ips=True) blocks ALL Wave 34 SQL bypasses:
    wave34_bypasses = (
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (++secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+(secret_tenant_b))",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+default.secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' NOT IN (+secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' GLOBAL IN (+secret_tenant_b)",
        "SELECT 0 IN (+system.one) AS hit FROM logs",
        "SELECT url FROM logs WHERE ('svc_alpha') IN (+request_facts)",
        "WITH +secret_tenant_b AS x SELECT 'TENANT_B_SECRET_847291' IN (+x) AS hit FROM logs",
        "SELECT * REPLACE ('TENANT_B_SECRET_847291' IN (+secret_tenant_b) AS url) FROM logs",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 1 AS limit SELECT url FROM logs PREWHERE limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs GROUP BY url HAVING limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS select SELECT url FROM logs WHERE select AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs INTERSECT WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs EXCEPT WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs UNION ALL WITH system.one AS logs SELECT url FROM logs",
        "WITH 1 AS values, values + 1 AS d, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS values, values AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS with, with AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 IS DISTINCT FROM 2 AS d, system.one AS logs SELECT 0 IN (logs) FROM logs",
        "WITH 1 AS values, values AS a, system.one AS logs SELECT url FROM logs",
        "WITH 1 AS select, 1 + select AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH pg_shadow AS u SELECT url FROM logs",
        "WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "SELECT url FROM logs ORDER BY url WITH FILL STEP in('TENANT_B_SECRET_847291', secret_tenant_b)",
        "SELECT url FROM logs FETCH NEXT in('TENANT_B_SECRET_847291', secret_tenant_b) ROWS ONLY",
        "SELECT url FROM logs FETCH FIRST in('TENANT_B_SECRET_847291', secret_tenant_b) ROWS ONLY",
    )
    for atk in wave34_bypasses:
        try:
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
            )
            raise AssertionError(f"query_endpoint FAILED TO BLOCK Wave-34 bypass (analyzer={analyzer_val}): {atk}")
        except PermissionError:
            pass

    # 3. Valid ClickHouse UNION ALL WITH standard CTE and benign unary + scalar IN still execute cleanly:
    for ok_sql, expected in (
        ("SELECT url FROM logs UNION ALL WITH c AS (SELECT url FROM logs) SELECT url FROM c", [{"url": "/r1"}, {"url": "/r1"}]),
        ("SELECT url FROM logs WHERE status IN (+200)", [{"url": "/r1"}]),
    ):
        got = hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert got == expected, f"Legitimate query failed (analyzer={analyzer_val}) {ok_sql}: got={got}, expected={expected}"

print("ITER35_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter35_nested_with_expr_cte_linear_time_dos() -> None:
    """ITER35 remediation (Finding 1): O(2^D) CPU DoS in nested WITH (<subquery>) AS aK fixed to linear time (< 25 ms)."""
    import time

    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    def _build_nested_with(depth: int, tail_from_logs: bool) -> str:
        inner = "SELECT 1 FROM logs" if tail_from_logs else "SELECT 1"
        for k in range(depth, 0, -1):
            body = f"SELECT a{k} FROM logs" if tail_from_logs else f"SELECT a{k}"
            inner = f"WITH ({inner}) AS a{k} {body}"
        return inner

    for depth in (14, 18, 30, 50):
        for tail_from_logs in (False, True):
            sql = _build_nested_with(depth, tail_from_logs)
            assert len(sql.encode("utf-8")) <= 64 * 1024

            t0 = time.perf_counter()
            rw = hs_query._rewrite_console_sql(
                sql, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )
            rw_ms = (time.perf_counter() - t0) * 1000.0
            assert rw, f"Empty rewrite for nested WITH depth={depth}"
            assert rw_ms < 500.0, f"Nested WITH depth={depth} (from_logs={tail_from_logs}) rewrite took {rw_ms:.1f}ms"

            t1 = time.perf_counter()
            val = validate_user_sql(sql, allowed_tables=frozenset({"logs"}))
            val_ms = (time.perf_counter() - t1) * 1000.0
            assert val.parse_tree
            assert val_ms < 500.0, (
                f"Nested WITH depth={depth} (from_logs={tail_from_logs}) validate took {val_ms:.1f}ms"
            )

    # Clause-opener + AS chain stays linear with zero RecursionError:
    clause_as_dos = "SELECT 1 FROM logs " + "PREWHERE " * 3000 + "AS " * 3000
    assert len(clause_as_dos.encode("utf-8")) <= 64 * 1024
    for label, fn in (
        ("validate", lambda: validate_user_sql(clause_as_dos, allowed_tables=frozenset({"logs"}))),
        (
            "rewrite",
            lambda: hs_query._rewrite_console_sql(
                clause_as_dos, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            ),
        ),
    ):
        t0 = time.perf_counter()
        try:
            fn()
        except (SQLValidationError, PermissionError):
            pass
        ms = (time.perf_counter() - t0) * 1000.0
        assert ms < 1500.0, f"Clause-opener+AS DoS ({label}) took {ms:.1f}ms"


@pytest.mark.security_regression
def test_sec_t5_iter35_unary_plus_union_with_abort_and_prefix_in() -> None:
    """ITER35 remediation (Findings 2 & 3a-3d): unary +, PREWHERE/HAVING/QUALIFY AS as, UNION WITH, WITH abort, parity, prefix in()."""
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    # Finding 2: Unary + (and interleaved parens / AS) on IN (...) RHS & WITH +tbl AS alias:
    unary_plus_attacks = (
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (++secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+++secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+(secret_tenant_b))",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+secret_tenant_b AS s)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (+default.secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN ((+(+(secret_tenant_b AS a))) AS b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' NOT IN (+secret_tenant_b)",
        "SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' GLOBAL IN (+secret_tenant_b)",
        "SELECT 0 IN (+system.one) AS hit FROM logs",
        "WITH +secret_tenant_b AS x SELECT 'TENANT_B_SECRET_847291' IN (+x) AS hit FROM logs",
        "WITH ++(secret_tenant_b AS s) AS x SELECT 'TENANT_B_SECRET_847291' IN (+x) AS hit FROM logs",
        "WITH +secret_tenant_b AS logs SELECT url FROM logs",
        "SELECT * REPLACE ('TENANT_B_SECRET_847291' IN (+secret_tenant_b) AS url) FROM logs",
    )
    for atk in unary_plus_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    # +system.one blocked even when allowed_tables=None; +request_facts blocked in rewriter:
    for sys_plus in (
        "SELECT 0 IN (+system.one) AS hit FROM logs",
        "SELECT 0 IN (++(system.one AS s)) AS hit FROM logs",
        "WITH +system.one AS x SELECT url FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(sys_plus, allowed_tables=None)

    with pytest.raises((PermissionError, SQLValidationError)):
        hs_query._rewrite_console_sql(
            "SELECT url FROM logs WHERE ('svc_alpha') IN (+request_facts)",
            svc_table="request_facts",
            names={"request_facts": "fla_cv_svc_m_request_facts"},
        )

    # Live-probed PREWHERE/HAVING/QUALIFY/WHERE <clause_kw> AS as IN / UNION bypasses:
    clause_as_as_attacks = (
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 1 AS limit SELECT url FROM logs PREWHERE limit AS as UNION ALL SHOW TABLES",
        "WITH 1 AS limit SELECT url FROM logs PREWHERE limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs GROUP BY url HAVING limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS select SELECT url FROM logs WHERE select AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    )
    for atk in clause_as_as_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))
    for atk in (
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 1 AS limit SELECT url FROM logs PREWHERE limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs GROUP BY url HAVING limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS limit SELECT url FROM logs QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS select SELECT url FROM logs WHERE select AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
    ):
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    # Finding 3a: Unparenthesized UNION/INTERSECT/EXCEPT branch-leading WITH:
    union_with_attacks = (
        "SELECT url FROM logs UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs UNION DISTINCT WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs INTERSECT WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs EXCEPT WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "SELECT url FROM logs UNION ALL WITH system.one AS logs SELECT url FROM logs",
        "SELECT url FROM logs INTERSECT WITH system.one AS logs SELECT url FROM logs",
        "SELECT url FROM logs EXCEPT WITH system.one AS logs SELECT url FROM logs",
    )
    for atk in union_with_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    with pytest.raises(SQLValidationError):
        validate_user_sql(
            "SELECT url FROM logs UNION ALL WITH system.one AS logs SELECT url FROM logs",
            allowed_tables=None,
        )

    # Branch-local CTE does NOT leak into subsequent UNION branches:
    with pytest.raises(SQLValidationError):
        validate_user_sql(
            "SELECT url FROM logs UNION ALL WITH c AS (SELECT url FROM logs) SELECT url FROM c UNION ALL SELECT url FROM c",
            allowed_tables=frozenset({"logs"}),
        )

    # Valid ClickHouse standard CTE in UNION ALL is allowed in BOTH validator and rewriter:
    for ok_union_with in (
        "SELECT url FROM logs UNION ALL WITH c AS (SELECT url FROM logs) SELECT url FROM c",
        "SELECT url FROM logs UNION ALL WITH c AS (SELECT url FROM logs) SELECT url FROM c UNION ALL SELECT url FROM logs",
    ):
        assert validate_user_sql(ok_union_with, allowed_tables=frozenset({"logs"})).parse_tree
        rw_ok = hs_query._rewrite_console_sql(
            ok_union_with, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
        )
        assert "fla_cv_svc_m_request_facts" in rw_ok and "FROM c" in rw_ok

    # Finding 3b: _SUBQUERY_HEAD_KEYWORDS / _with_subq_heads early-abort + fail-closed malformed WITH:
    with_abort_attacks = (
        "WITH 1 AS values, values + 1 AS d, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS values, values AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS with, with AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 IS DISTINCT FROM 2 AS d, system.one AS logs SELECT 0 IN (logs) FROM logs",
        "WITH 1 AS values, values AS a, system.one AS logs SELECT url FROM logs",
        "WITH 1 AS from, from + 1 AS d, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH 1 AS select, 1 + select AS a, secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH x SELECT a AS b FROM logs",
        "WITH a AS (SELECT url FROM logs), SELECT url FROM logs",
    )
    for atk in with_abort_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}))
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    with pytest.raises(SQLValidationError):
        validate_user_sql("WITH 1 AS values, values AS a, system.one AS logs SELECT url FROM logs", allowed_tables=None)

    # Benign keyword-aliased expression CTEs still pass cleanly:
    for benign_kw_with in (
        "WITH 1 AS values, values AS a SELECT a, url FROM logs",
        "WITH 1 AS with, with AS a SELECT a, url FROM logs",
        "WITH 1 IS DISTINCT FROM 2 AS d SELECT d, url FROM logs",
    ):
        assert validate_user_sql(benign_kw_with, allowed_tables=frozenset({"logs"})).parse_tree
        assert hs_query._rewrite_console_sql(
            benign_kw_with, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
        )

    # Finding 3c: query.py defense-in-depth parity on WITH expression CTEs (pg_ prefix & non-main 3-part catalog):
    for parity_atk in (
        "WITH pg_shadow AS u SELECT url FROM logs",
        "WITH +pg_shadow AS u SELECT url FROM logs",
        "WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "WITH +evil_cat.default.tbl AS u SELECT url FROM logs",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(parity_atk, allowed_tables=None)
        with pytest.raises(PermissionError):
            hs_query._rewrite_console_sql(
                parity_atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    # Finding 3d: Prefix in(...) call after WITH FILL STEP / TO and FETCH NEXT / FIRST:
    prefix_in_wave34 = (
        "SELECT url FROM logs ORDER BY url WITH FILL STEP in('TENANT_B_SECRET_847291', secret_tenant_b)",
        "SELECT url FROM logs ORDER BY status WITH FILL TO in('TENANT_B_SECRET_847291', secret_tenant_b)",
        "SELECT url FROM logs FETCH NEXT in('TENANT_B_SECRET_847291', secret_tenant_b) ROWS ONLY",
        "SELECT url FROM logs FETCH FIRST in('TENANT_B_SECRET_847291', secret_tenant_b) ROWS ONLY",
    )
    for p_sql in prefix_in_wave34:
        sig = [
            (m.lastgroup or "OTHER", m.group(0), m.start())
            for m in hs_query._CONSOLE_SQL_LEX_RE.finditer(p_sql)
            if (m.lastgroup or "OTHER") not in ("WS", "COMMENT")
        ]
        in_idx = next(i for i, (k, t, _) in enumerate(sig) if k == "IDENT" and t.lower() == "in")
        assert hs_query._is_prefix_in_call_sig(sig, in_idx), f"_is_prefix_in_call_sig missed: {p_sql}"
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(p_sql)
        with pytest.raises(SQLValidationError):
            validate_user_sql(p_sql, allowed_tables=frozenset({"logs"}))
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                p_sql, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )


@pytest.mark.security_regression
def test_sec_t5_iter35_pii_3way_parity_and_live_chdb() -> None:
    """ITER35 remediation (Findings 4a-4e): 100% 3-way PII parity + live chdb (analyzer=0 and 1) SQL & PII verification."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access

    # Exact 18-key invariant strictly preserved:
    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)

    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "toValidUTF8(" in map_view_expr and "unhex(" in map_view_expr and "splitByChar('%'" in map_view_expr

    for pii_k in _ITER35_WAVE34_PII_KEYS:
        assert console_access._is_console_pii_identifier(pii_k), f"console_access missed {pii_k!r}"
        assert console_access._redaction(pii_k, "String", keys_sql) is not None, f"_redaction missed {pii_k!r}"
        assert hs_query._classify_console_pii_key(pii_k) is not None, f"query.py missed {pii_k!r}"

    for safe_k in _ITER35_BENIGN_KEYS:
        assert not console_access._is_console_pii_identifier(safe_k), f"console_access FP on {safe_k!r}"
        assert hs_query._classify_console_pii_key(safe_k) is None, f"query.py FP on {safe_k!r}"

    # Live chdb verification across allow_experimental_analyzer=0 and 1:
    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER35_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER35_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter35 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER35_CHDB_ALL_OK" in run_res.stdout


_ITER36_WAVE35_PII_KEYS = (
    # Finding 4a: 4+ level bracket nesting
    "foo.c[[[[0]]]]id",
    "auth[x[y[z[0]]]]orization",
    "clie[x[y[z[0]]]]nt_ip",
    "clie[a[b[c[d]]]]nt_ip",
    "clie[a[b[c[d[e[f[g]]]]]]]nt_ip",
    "x_api_[a[b[c[d]]]]key",
    # Finding 4b: %%XX / %<non-hex>%XX pure-ASCII URL decode parity
    "cl%%69ent_ip",
    "x_api_%%6bey",
    "s%%69d",
    "cl%!%69ent_ip",
    "cl%%%69ent_ip",
    "au%%74horization",
    # Finding 4b: split-UTF-8 double-encoding
    "cl%C4%25B0ent_ip",
    "api_%e2%2584%aaey",
    "x_api_%e2%2584%aaey",
)

_ITER36_BENIGN_KEYS = (
    "items[0].status",
    "items[0].a[1].status",
    "items[0][1].status",
    "req[0]status[1]code",
    "cl%z%69ent_ip",
    "status_code",
    "cache_status",
)

_REAL_CHDB_ITER36_SCRIPT = r"""
import chdb
from backend.high_scale.postgres_control import console_access
from backend.high_scale import query as hs_query
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pii_keys = (
    "foo.c[[[[0]]]]id",
    "auth[x[y[z[0]]]]orization",
    "clie[x[y[z[0]]]]nt_ip",
    "clie[a[b[c[d]]]]nt_ip",
    "clie[a[b[c[d[e[f[g]]]]]]]nt_ip",
    "x_api_[a[b[c[d]]]]key",
    "cl%%69ent_ip",
    "x_api_%%6bey",
    "s%%69d",
    "cl%!%69ent_ip",
    "cl%%%69ent_ip",
    "au%%74horization",
    "cl%C4%25B0ent_ip",
    "api_%e2%2584%aaey",
    "x_api_%e2%2584%aaey",
)
benign_keys = (
    "items[0].status",
    "items[0].a[1].status",
    "items[0][1].status",
    "req[0]status[1]code",
    "cl%z%69ent_ip",
    "status_code",
    "cache_status",
)

keys_sql = ", ".join(console_access._literal(k) for k in console_access._MAP_MASKED_KEYS)
map_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
assert map_expr is not None

for analyzer_val in (0, 1):
    s = chdb.session.Session()
    s.query(f"SET allow_experimental_analyzer = {analyzer_val}")
    s.query("CREATE DATABASE IF NOT EXISTS fla_test")
    s.query("DROP TABLE IF EXISTS fla_test.request_facts")
    s.query("DROP TABLE IF EXISTS fla_test.secret_tenant_b")
    s.query("DROP VIEW IF EXISTS fla_test.v_req")
    s.query(
        "CREATE TABLE fla_test.request_facts ("
        "service_id String, event_timestamp DateTime64(3, 'UTC'), url String, status UInt16, "
        "custom_fields Map(String, String)"
        ") ENGINE = MergeTree ORDER BY (service_id, event_timestamp)"
    )
    s.query("CREATE TABLE fla_test.secret_tenant_b (v String) ENGINE = Memory")
    s.query("INSERT INTO fla_test.secret_tenant_b VALUES ('TENANT_B_SECRET_847291')")
    s.query("USE fla_test")

    all_entries = []
    pii_secrets = {}
    for idx, k in enumerate(pii_keys):
        sec = f"ITER36_SECRET_{idx}_TOKEN"
        pii_secrets[k] = sec
        all_entries.append(f"{console_access._literal(k)}: {console_access._literal(sec)}")
    for idx, k in enumerate(benign_keys):
        all_entries.append(f"{console_access._literal(k)}: {console_access._literal(f'SAFE_VAL_{idx}')}")

    map_lit = "{" + ", ".join(all_entries) + "}"
    s.query(
        "INSERT INTO fla_test.request_facts VALUES "
        "('svc_alpha', '2026-11-19 00:10:00.000', '/r1', 200, " + map_lit + "), "
        "('svc_beta', '2026-11-19 00:10:00.000', '/beta_secret', 200, map())"
    )
    s.query(
        f"CREATE VIEW fla_test.v_req AS SELECT service_id, event_timestamp, url, status, {map_expr} "
        "FROM fla_test.request_facts WHERE service_id = 'svc_alpha'"
    )

    # 1. Exact 3-way parity on ClickHouse view vs Python console_access vs Python query.py:
    for k in pii_keys:
        out = str(s.query(f"SELECT custom_fields[{console_access._literal(k)}] FROM fla_test.v_req", "TabSeparatedRaw")).strip()
        assert out == "[redacted]", f"CH view failed to redact {k!r} (analyzer={analyzer_val}): got {out!r}"
        assert console_access._is_console_pii_identifier(k), f"console_access missed {k!r}"
        assert hs_query._classify_console_pii_key(k) is not None, f"query.py missed {k!r}"
    for idx, k in enumerate(benign_keys):
        out = str(s.query(f"SELECT custom_fields[{console_access._literal(k)}] FROM fla_test.v_req", "TabSeparatedRaw")).strip()
        assert out == f"SAFE_VAL_{idx}", f"CH view FP on {k!r} (analyzer={analyzer_val}): got {out!r}"
        assert not console_access._is_console_pii_identifier(k), f"console_access FP on {k!r}"
        assert hs_query._classify_console_pii_key(k) is None, f"query.py FP on {k!r}"

    # Verify live mapValues(custom_fields) via query_endpoint never leaks any PII secret:
    class _ChdbClient:
        _table_filters_verified = True

        def __init__(self, sess):
            self._s = sess

        def execute(self, sql, settings=None, **kwargs):
            import json as _json

            s_parts = [f"allow_experimental_analyzer = {analyzer_val}"]
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}"
            raw = self._s.query(full_sql, "JSON").bytes()
            return _json.loads(raw.decode("utf-8"))["data"] if raw else []

    class _Svc:
        service_id = "svc_alpha"

        def __init__(self, cli):
            self.client = cli

    cli = _ChdbClient(s)
    svc = _Svc(cli)
    masked_tables = {"request_facts": "v_req"}
    mv_res = hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT mapValues(custom_fields) AS vals FROM logs", max_rows=10),
        "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
    )
    leaked_blob = str(mv_res["data"])
    for k, sec in pii_secrets.items():
        assert sec not in leaked_blob, f"query_endpoint mapValues leaked {k!r} secret {sec!r} (analyzer={analyzer_val})"

    # 2. Live chdb check: ClickHouse CTEs ARE case-sensitive, and query_endpoint blocks ALL Wave-35 bypasses:
    wave35_bypasses = (
        # Finding 1: Case-sensitive CTE mismatches & shadowed canonical tables
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
        "WITH REQUEST_FACTS AS (SELECT 1 AS a) SELECT * FROM request_facts",
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM logs JOIN secret_tenant_b ON 1 = 1",
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM logs UNION ALL SELECT * FROM secret_tenant_b",
        "WITH `SECRET_TENANT_B` AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
        "WITH logs AS SECRET_TENANT_B SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (secret_tenant_b)",
        "SELECT url FROM logs UNION ALL WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT v FROM secret_tenant_b",
        # Finding 2: Bare-alias parity inversion & SELECT-head star wildcards
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH pg_shadow AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs",
        "WITH 1 AS limit SELECT 1 by HAVING limit AS as UNION ALL WITH system.tables AS u SELECT 1",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs by PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs offset PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs qualify PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 1 AS limit SELECT url FROM logs by PREWHERE limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 WITH TIES * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH limit AS (SELECT url FROM logs) SELECT DISTINCT ON (url) * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS as SELECT url FROM logs by QUALIFY as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT url FROM logs select PREWHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT url FROM logs table PREWHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT limit by FROM logs WHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
    )
    for atk in wave35_bypasses:
        try:
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
            )
            raise AssertionError(f"query_endpoint FAILED TO BLOCK Wave-35 bypass (analyzer={analyzer_val}): {atk}")
        except PermissionError:
            pass

    # 3. Legitimate same-case CTEs, MixedCase CTEs, TOP 1 *, DISTINCT ON *, and LIMIT 1 BY execute cleanly:
    for ok_sql, expected in (
        ("WITH top_paths AS (SELECT url FROM logs) SELECT url FROM top_paths", [{"url": "/r1"}]),
        ("WITH MixedPaths AS (SELECT url FROM logs) SELECT url FROM MixedPaths", [{"url": "/r1"}]),
        ("SELECT TOP 1 url FROM logs", [{"url": "/r1"}]),
        ("SELECT DISTINCT ON (url) url FROM logs", [{"url": "/r1"}]),
        ("WITH 1 AS limit SELECT url FROM logs LIMIT 1 BY limit AS as UNION ALL SELECT url FROM logs", [{"url": "/r1"}, {"url": "/r1"}]),
    ):
        got = hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )["data"]
        assert got == expected, f"Legitimate query failed (analyzer={analyzer_val}) {ok_sql}: got={got}, expected={expected}"

    s.close()

print("ITER36_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter36_case_sensitive_clickhouse_ctes() -> None:
    """ITER36 remediation (Finding 1): case-sensitive ClickHouse CTE validation & rewriting + no canonical CTE shadowing."""
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    cte_case_mismatch_attacks = (
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM logs JOIN secret_tenant_b ON 1 = 1",
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM logs UNION ALL SELECT * FROM secret_tenant_b",
        'WITH "SECRET_TENANT_B" AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b',
        "WITH `SECRET_TENANT_B` AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
        "WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM (SELECT * FROM secret_tenant_b) sub",
        "SELECT url FROM logs UNION ALL WITH SECRET_TENANT_B AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
        "WITH logs AS SECRET_TENANT_B SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (secret_tenant_b)",
        "WITH ` secret_tenant_b ` AS (SELECT 1 AS a) SELECT * FROM secret_tenant_b",
    )
    for atk in cte_case_mismatch_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True)

    # Shadowing any canonical / allowed / fact table name (in ANY case) must be blocked by the high-scale rewriter:
    shadow_attacks = (
        "WITH REQUEST_FACTS AS (SELECT 1 AS a) SELECT * FROM request_facts",
        "WITH Request_Facts AS (SELECT 1 AS a) SELECT * FROM Request_Facts",
        "WITH LOGS AS (SELECT 1 AS a) SELECT * FROM logs",
        "WITH `REQUEST_FACTS` AS (SELECT 1 AS a) SELECT * FROM request_facts",
        "WITH RUM_VITALS AS (SELECT 1 AS a) SELECT * FROM logs",
        "WITH RUM_ERRORS AS (SELECT 1 AS a) SELECT * FROM logs",
        "WITH HIGH_SCALE_BATCH_PUBLICATIONS AS (SELECT 1 AS a) SELECT * FROM logs",
        "WITH 1 AS REQUEST_FACTS SELECT url FROM logs",
    )
    for atk in shadow_attacks:
        with pytest.raises(PermissionError):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    # Legitimate same-case user CTEs continue to pass both validator and rewriter:
    for ok_sql in (
        "WITH top_paths AS (SELECT url FROM logs) SELECT * FROM top_paths",
        "WITH MixedCase AS (SELECT url FROM logs) SELECT * FROM MixedCase",
    ):
        assert validate_user_sql(ok_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw = hs_query._rewrite_console_sql(
            ok_sql, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
        )
        assert "fla_cv_svc_m_request_facts" in rw


@pytest.mark.security_regression
def test_sec_t5_iter36_bare_alias_parity_and_select_head_star_wildcard() -> None:
    """ITER36 remediation (Finding 2): bare-alias parity inversion & TOP/DISTINCT ON * wildcard in validator & rewriter."""
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    bare_alias_and_star_attacks = (
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH pg_shadow AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs",
        "WITH 1 AS limit SELECT 1 by HAVING limit AS as UNION ALL WITH system.tables AS u SELECT 1",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs by PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs offset PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 'TENANT_B_SECRET_847291' AS limit SELECT url FROM logs qualify PREWHERE limit AS as IN (secret_tenant_b)",
        "WITH 1 AS limit SELECT url FROM logs by PREWHERE limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs WHERE 'TENANT_B_SECRET_847291' IN (logs)",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 WITH TIES * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH limit AS (SELECT url FROM logs) SELECT DISTINCT ON (url) * FROM limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        # Additional r==1 and 5-word-run bare-alias parity inversions:
        "WITH 1 AS as SELECT url FROM logs by QUALIFY as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT url FROM logs select PREWHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT url FROM logs table PREWHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH 1 AS limit SELECT limit by FROM logs WHERE limit AS as UNION ALL SELECT v FROM secret_tenant_b",
        "WITH limit AS (SELECT url FROM logs) SELECT url FROM limit by QUALIFY limit AS as UNION ALL SELECT v FROM secret_tenant_b",
    )
    from types import SimpleNamespace

    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    class _DenyClient:
        _table_filters_verified = True

        def execute(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("attacker SQL should never reach ClickHouse client.execute")

    svc = SimpleNamespace(service_id="svc_alpha", client=_DenyClient())
    masked_tables = {"request_facts": "fla_cv_svc_m_request_facts"}

    for atk in bare_alias_and_star_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True)
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables=masked_tables,
            )

    # Direct _rewrite_console_sql defense-in-depth proof for bare-alias AND SELECT-head star wildcard in query.py:
    rewriter_only_attacks = (
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH pg_shadow AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH secret_tenant_b AS logs SELECT url FROM logs",
        "WITH 1 AS limit SELECT 1 by HAVING limit AS as UNION ALL WITH system.tables AS u SELECT 1",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 * FROM limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH limit AS (SELECT url FROM logs) SELECT TOP 1 WITH TIES * FROM limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH limit AS (SELECT url FROM logs) SELECT DISTINCT ON (url) * FROM limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS as SELECT url FROM logs by QUALIFY as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs select PREWHERE limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs table PREWHERE limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
    )
    for atk in rewriter_only_attacks:
        with pytest.raises((PermissionError, SQLValidationError)):
            hs_query._rewrite_console_sql(
                atk, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )

    # Also prove allowed_tables=None blocks system.tables / pg_shadow / evil_cat on bare-alias queries:
    for atk in (
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH system.tables AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH pg_shadow AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT url FROM logs by QUALIFY limit AS as UNION ALL WITH evil_cat.default.tbl AS u SELECT url FROM logs",
        "WITH 1 AS limit SELECT 1 by HAVING limit AS as UNION ALL WITH system.tables AS u SELECT 1",
    ):
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=None)

    # Legitimate queries with LIMIT BY, TOP 1 *, DISTINCT ON *, and `SELECT as AS from` still pass:
    for ok_sql in (
        "WITH 1 AS limit SELECT url FROM logs LIMIT 1 BY limit AS as UNION ALL SELECT url FROM logs",
        "WITH 1 AS as SELECT as AS from FROM logs",
        "SELECT TOP 1 * FROM logs",
        "SELECT DISTINCT ON (url) * FROM logs",
    ):
        assert validate_user_sql(ok_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        assert hs_query._rewrite_console_sql(
            ok_sql, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
        )


@pytest.mark.security_regression
def test_sec_t5_iter36_linear_time_dos_union_with_and_in_rhs() -> None:
    """ITER36 remediation (Findings 3a & 3b): O(N^2) UNION WITH and IN-RHS AS-chain DoS fixed to linear time (< 150ms)."""
    import os
    import sys
    import time

    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    # Python coverage tracing (`sys.gettrace()` / `sys.monitoring`) + parallel xdist slows pure-Python regex/walker loops ~4-6x on shared runners.
    under_cov = (
        sys.gettrace() is not None
        or bool(os.environ.get("COV_CORE_SOURCE"))
        or (hasattr(sys, "monitoring") and sys.monitoring.get_tool(sys.monitoring.COVERAGE_ID) is not None)
    )
    cov_scale = 6.0 if under_cov else 1.0
    payloads = (
        ("union_with_bare", "SELECT 1" + " UNION WITH" * 4000),
        ("union_with_expr", "SELECT 1" + " UNION WITH 1 AS a SELECT 1" * 2000),
        ("in_rhs_as_chain", "SELECT 1 FROM logs WHERE 1 IN ((1)" + " AS a" * 4000 + ")"),
        ("with_expr_as_chain", "WITH ((1)" + " AS a" * 4000 + ") AS x SELECT 1 FROM logs"),
    )
    for label, sql in payloads:
        assert len(sql.encode("utf-8")) <= 64 * 1024
        t0 = time.perf_counter()
        try:
            validate_user_sql(sql, allowed_tables=frozenset({"logs"}))
        except SQLValidationError:
            pass
        val_ms = (time.perf_counter() - t0) * 1000.0
        assert val_ms < 150.0 * cov_scale, (
            f"DoS {label} validate_user_sql took {val_ms:.1f}ms (target < {150.0 * cov_scale:.0f}ms)"
        )

        t1 = time.perf_counter()
        try:
            hs_query._rewrite_console_sql(
                sql, svc_table="request_facts", names={"request_facts": "fla_cv_svc_m_request_facts"}
            )
        except (PermissionError, SQLValidationError):
            pass
        rw_ms = (time.perf_counter() - t1) * 1000.0
        assert rw_ms < 750.0 * cov_scale, f"DoS {label} _rewrite_console_sql took {rw_ms:.1f}ms"


@pytest.mark.security_regression
def test_sec_t5_iter36_scrub_ip_text_trailing_dot_and_dns_ipv4() -> None:
    """ITER36 remediation (Finding 5): scrub_ip_text masks trailing-dot and DNS-embedded IPv4 while preserving OIDs."""
    from backend.core.share_db import scrub_ip_text

    res_sentence = scrub_ip_text("Request from 192.168.1.50.")
    assert "192.168.1.50" not in res_sentence
    assert res_sentence.endswith(".")

    res_nip = scrub_ip_text("https://192.168.1.50.nip.io/login")
    assert "192.168.1.50" not in res_nip
    assert ".nip.io/login" in res_nip

    res_sslip = scrub_ip_text("http://10.20.30.40.sslip.io:8080/api")
    assert "10.20.30.40" not in res_sslip
    assert ".sslip.io:8080/api" in res_sslip

    # 5-part OID must remain untouched:
    assert scrub_ip_text("OID 1.2.3.4.5 ok") == "OID 1.2.3.4.5 ok"


@pytest.mark.security_regression
def test_sec_t5_iter36_pii_3way_parity_and_live_chdb() -> None:
    """ITER36 remediation (Findings 1, 2, 4a, 4b): 3-way PII parity + live chdb (analyzer=0 and 1) SQL & PII suite."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access

    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)

    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "toValidUTF8(" in map_view_expr and "unhex(" in map_view_expr and "splitByChar('%'" in map_view_expr

    for pii_k in _ITER36_WAVE35_PII_KEYS:
        assert console_access._is_console_pii_identifier(pii_k), f"console_access missed {pii_k!r}"
        assert console_access._redaction(pii_k, "String", keys_sql) is not None, f"_redaction missed {pii_k!r}"
        assert hs_query._classify_console_pii_key(pii_k) is not None, f"query.py missed {pii_k!r}"

    for safe_k in _ITER36_BENIGN_KEYS:
        assert not console_access._is_console_pii_identifier(safe_k), f"console_access FP on {safe_k!r}"
        assert hs_query._classify_console_pii_key(safe_k) is None, f"query.py FP on {safe_k!r}"

    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER36_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER36_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter36 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER36_CHDB_ALL_OK" in run_res.stdout


_REAL_CHDB_ITER37_SCRIPT = r"""
import chdb
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

pii_keys = [
    "cl[a]ient_ip",
    "cl[a[b]]ient_ip",
    "cl[a[b[c[d[e[f]]]]]]ient_ip",
    "cl[a[b[c[d[e[f[g[h[i[j]]]]]]]]]]ient_ip",
    "cl[a[b[c[d[e[f]]]]]]ien[a[b[c[d[e[f]]]]]]t_ip",
    "clie[a.b[c[d[e[f[g]]]]]]nt_ip",
    "]cl[a[b[c[d[e[f]]]]]]ient_ip",
    "cl[é[ü[中[🚀[a[b]]]]]]ient_ip",
    "x[unmatched_open]client_ip",
    "x[client_ip",
]
benign_keys = [
    "my[a[b[c[d[e[f]]]]]]_metric",
    "my[a.b[c[d[e[f[g]]]]]]_metric",
    "my[a[b[c[d[e[f]]]]]]_met[a[b[c[d[e[f]]]]]]_ric",
    "é[a[b[c[d[e[f]]]]]]_safe",
    "a[b[c[d[e[f[g]]]]]]d",
    "]safe[a[b[c[d[e[f]]]]]]_key",
    "[unmatched_safe_metric",
    "x[unmatched_open_client_ip",
]

for analyzer_val in (0, 1):
    s = chdb.session.Session()
    s.query(f"SET allow_experimental_analyzer = {analyzer_val}")
    s.query("CREATE DATABASE IF NOT EXISTS fla_test")
    s.query("USE fla_test")
    s.query("DROP VIEW IF EXISTS fla_test.v_req")
    s.query("DROP TABLE IF EXISTS fla_test.request_facts")
    s.query("DROP TABLE IF EXISTS fla_test.high_scale_batch_publications")
    s.query(
        "CREATE TABLE fla_test.high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String) "
        "ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
    )
    s.query(
        "INSERT INTO fla_test.high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')"
    )
    s.query(
        "CREATE TABLE fla_test.request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(3, 'UTC'), client_ip String, status UInt16, url String, "
        "custom_fields Map(String, String)) ENGINE = Memory"
    )
    map_entries = []
    pii_secrets = {}
    for idx, k in enumerate(pii_keys):
        sec = f"SECRET_ITER37_{idx}"
        pii_secrets[k] = sec
        map_entries.append(f"{console_access._literal(k)}: {console_access._literal(sec)}")
    for idx, k in enumerate(benign_keys):
        map_entries.append(f"{console_access._literal(k)}: 'SAFE_ITER37_{idx}'")
    s.query(
        "INSERT INTO fla_test.request_facts VALUES ("
        "'svc_alpha', 'b1', 'visible', toDateTime64('2026-11-19 00:05:00.000', 3, 'UTC'), "
        "'203.0.113.42', 200, '/iter37', {" + ", ".join(map_entries) + "})"
    )
    cols = [
        ("service_id", "String"),
        ("batch_id", "String"),
        ("publication_state", "String"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_ip", "String"),
        ("status", "UInt16"),
        ("url", "String"),
        ("custom_fields", "Map(String, String)"),
    ]
    view_sql = console_access._view_select("fla_test", "request_facts", cols, "svc_alpha", masked=True)
    s.query(f"CREATE VIEW fla_test.v_req AS {view_sql}")

    # 1. Exact 3-way parity on depth 1..10, multi-group, dot-in-bracket, unmatched brackets, and UTF-8:
    for k in pii_keys:
        out = str(s.query(f"SELECT custom_fields[{console_access._literal(k)}] FROM fla_test.v_req", "TabSeparatedRaw")).strip()
        assert out == "[redacted]", f"CH view failed to redact {k!r} (analyzer={analyzer_val}): got {out!r}"
        assert console_access._is_console_pii_identifier(k), f"console_access missed {k!r}"
        assert hs_query._classify_console_pii_key(k) is not None, f"query.py missed {k!r}"
    for idx, k in enumerate(benign_keys):
        out = str(s.query(f"SELECT custom_fields[{console_access._literal(k)}] FROM fla_test.v_req", "TabSeparatedRaw")).strip()
        assert out == f"SAFE_ITER37_{idx}", f"CH view FP on {k!r} (analyzer={analyzer_val}): got {out!r}"
        assert not console_access._is_console_pii_identifier(k), f"console_access FP on {k!r}"
        assert hs_query._classify_console_pii_key(k) is None, f"query.py FP on {k!r}"

    class _ChdbClient:
        _table_filters_verified = True

        def __init__(self, sess):
            self._s = sess

        def execute(self, sql, settings=None, **kwargs):
            import json as _json

            s_parts = [f"allow_experimental_analyzer = {analyzer_val}"]
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}"
            raw = self._s.query(full_sql, "JSON").bytes()
            return _json.loads(raw.decode("utf-8"))["data"] if raw else []

    class _Svc:
        service_id = "svc_alpha"

        def __init__(self, cli):
            self.client = cli

    cli = _ChdbClient(s)
    svc = _Svc(cli)
    masked_tables = {"request_facts": "v_req"}

    # 2. Live chdb verification of Objective 2 rewritten queries:
    live_ok_queries = (
        ("SELECT TOP 5 ip FROM logs", {"client_ip"}),
        ("SELECT DISTINCT ON (ip) TOP 5 timestamp FROM logs", {"event_timestamp"}),
        ("WITH t AS (SELECT TOP 5 * FROM logs) SELECT ip, timestamp FROM t", {"client_ip", "event_timestamp"}),
        ("WITH t AS (SELECT * REPLACE ('/rewritten' AS url) FROM logs) SELECT ip, url FROM t", {"client_ip", "url"}),
        ("WITH t AS (SELECT r.* REPLACE ('/rewritten' AS url) FROM logs r) SELECT ip, url FROM t", {"client_ip", "url"}),
        ("WITH t AS (SELECT * EXCEPT status FROM logs) SELECT ip, url FROM t", {"client_ip", "url"}),
        ("WITH t AS (SELECT r.* EXCEPT status FROM logs r) SELECT ip, url FROM t", {"client_ip", "url"}),
        ("SELECT * EXCEPT status FROM logs", {"url", "client_ip", "event_timestamp"}),
    )
    for ok_sql, expected_cols in live_ok_queries:
        res = hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )
        rows = res["data"]
        assert len(rows) == 1, f"Expected 1 row (analyzer={analyzer_val}) for {ok_sql}: got {rows}"
        assert expected_cols.issubset(set(rows[0].keys())), (
            f"Missing expected columns (analyzer={analyzer_val}) for {ok_sql}: got={set(rows[0].keys())}, want>={expected_cols}"
        )

    # 3. Live chdb verification that * APPLY attacks are blocked:
    for atk in (
        "SELECT * APPLY file FROM (SELECT '/etc/passwd' AS a)",
        "SELECT * APPLY sleep FROM logs",
        "SELECT r.* APPLY sleepEachRow FROM logs r",
        "SELECT * EXCEPT (status) APPLY sleep FROM logs",
        "SELECT * EXCEPT status APPLY sleep FROM logs",
        "SELECT * REPLACE (200 AS status) APPLY sleep FROM logs",
        "SELECT COLUMNS('.*') APPLY sleep FROM logs",
    ):
        try:
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
            )
            raise AssertionError(f"query_endpoint failed to block APPLY attack (analyzer={analyzer_val}): {atk}")
        except PermissionError:
            pass

    s.close()

print("ITER37_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter37_apply_column_transformer_blocked() -> None:
    """ITER37 Objective 1: ClickHouse APPLY column transformer blocked in both sql_validator and query.py."""
    from types import SimpleNamespace

    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    apply_attacks = (
        "SELECT * APPLY file FROM (SELECT '/etc/passwd' AS a)",
        "SELECT * APPLY sleep FROM logs",
        "SELECT * APPLY sleepEachRow FROM logs",
        "SELECT r.* APPLY sleep FROM logs r",
        "SELECT * EXCEPT (status) APPLY sleep FROM logs",
        "SELECT * EXCEPT status APPLY sleep FROM logs",
        "SELECT * REPLACE (200 AS status) APPLY sleep FROM logs",
        "SELECT * APPLY (sleep) FROM logs",
        "SELECT * APPLY (x -> sleep(x)) FROM logs",
        "SELECT * APPLY toString APPLY sleep FROM logs",
        "SELECT COLUMNS('.*') APPLY sleep FROM logs",
        "SELECT * APPLY `file` FROM logs",
        "SELECT * APPLY url FROM logs",
        "SELECT * APPLY s3 FROM logs",
        "SELECT (SELECT * APPLY file FROM (SELECT '/etc/passwd' AS a)) FROM logs",
        "SELECT * APPLY any FROM logs",
        "SELECT * APPLY array FROM logs",
        "SELECT * APPLY not FROM logs",
        "SELECT * APPLY any APPLY file FROM (SELECT '/etc/passwd' AS a)",
        "SELECT * APPLY array APPLY file FROM (SELECT '/etc/passwd' AS a)",
        "SELECT * APPLY not APPLY file FROM (SELECT '/etc/passwd' AS a)",
        "SELECT * EXCEPT (status) APPLY not APPLY sleep FROM logs",
        "SELECT COLUMNS('.*') APPLY any FROM logs",
        "SELECT COLUMNS('.*') APPLY array FROM logs",
    )

    class _DenyClient:
        _table_filters_verified = True

        def execute(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("APPLY attack must never reach ClickHouse")

    svc = SimpleNamespace(service_id="svc_alpha", client=_DenyClient())
    masked_tables = {"request_facts": "fla_cv_svc_m_request_facts"}

    for atk in apply_attacks:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True)
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(atk, parse_tree=None)
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables=masked_tables,
            )

    # Legitimate ordinary uses of 'apply' as column or alias must remain allowed:
    for ok_sql in (
        "SELECT apply FROM logs",
        "SELECT status AS apply FROM logs",
        "SELECT status apply FROM logs",
        "SELECT count(*) apply FROM logs",
        "SELECT 2 * apply FROM logs",
        "SELECT t.apply FROM logs t",
    ):
        assert validate_user_sql(ok_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        hs_query._reject_unsafe_functions(ok_sql, parse_tree=None)


@pytest.mark.security_regression
def test_sec_t5_iter37_select_projection_top_replace_except() -> None:
    """ITER37 Objective 2: SELECT projection analyzer handles TOP N [WITH TIES], * REPLACE (...), and unparenthesized * EXCEPT col."""
    from backend.core.sql_validator import validate_user_sql

    names = {"request_facts": "fla_cv_svc_m_request_facts"}

    # TOP N [WITH TIES] before and after DISTINCT / DISTINCT ON (...)
    rw_top = hs_query._rewrite_console_sql("SELECT TOP 5 ip FROM logs", svc_table="request_facts", names=names)
    assert "client_ip" in rw_top

    rw_top_cte = hs_query._rewrite_console_sql(
        "WITH t AS (SELECT TOP 5 * FROM logs) SELECT ip, timestamp FROM t",
        svc_table="request_facts",
        names=names,
    )
    assert "client_ip" in rw_top_cte and "event_timestamp" in rw_top_cte

    rw_dist_top = hs_query._rewrite_console_sql(
        "SELECT DISTINCT ON (ip) TOP 5 timestamp FROM logs",
        svc_table="request_facts",
        names=names,
    )
    assert "client_ip" in rw_dist_top and "event_timestamp" in rw_dist_top

    rw_top_ties = hs_query._rewrite_console_sql(
        "WITH t AS (SELECT TOP 5 WITH TIES * FROM logs) SELECT ip, timestamp FROM t",
        svc_table="request_facts",
        names=names,
    )
    assert "client_ip" in rw_top_ties and "event_timestamp" in rw_top_ties

    # * REPLACE (...) and t.* REPLACE (...) propagate projects_star
    for rep_sql in (
        "WITH t AS (SELECT * REPLACE ('/x' AS url) FROM logs) SELECT ip, timestamp FROM t",
        "WITH t AS (SELECT r.* REPLACE ('/x' AS url) FROM logs r) SELECT ip, timestamp FROM t",
    ):
        assert validate_user_sql(rep_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw_rep = hs_query._rewrite_console_sql(rep_sql, svc_table="request_facts", names=names)
        assert "client_ip" in rw_rep and "event_timestamp" in rw_rep, f"projects_star failed for {rep_sql}: {rw_rep}"

    # Unparenthesized * EXCEPT col and t.* EXCEPT col propagate projects_star AND don't corrupt CF cols
    for exc_sql in (
        "WITH t AS (SELECT * EXCEPT status FROM logs) SELECT ip, timestamp FROM t",
        "WITH t AS (SELECT r.* EXCEPT status FROM logs r) SELECT ip, timestamp FROM t",
    ):
        assert validate_user_sql(exc_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw_exc = hs_query._rewrite_console_sql(exc_sql, svc_table="request_facts", names=names)
        assert "client_ip" in rw_exc and "event_timestamp" in rw_exc, f"projects_star failed for {exc_sql}: {rw_exc}"

    for cf_exc_sql in (
        "SELECT * EXCEPT geo_city FROM logs",
        "SELECT * EXCEPT (geo_city, bot_score) FROM logs",
        "SELECT r.* EXCEPT geo_city FROM logs r",
    ):
        assert validate_user_sql(cf_exc_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw_cf = hs_query._rewrite_console_sql(cf_exc_sql, svc_table="request_facts", names=names)
        assert "custom_fields['geo_city']" not in rw_cf, f"EXCEPT col corrupted into custom_fields: {rw_cf}"
        assert "custom_fields['bot_score']" not in rw_cf, f"EXCEPT col corrupted into custom_fields: {rw_cf}"


@pytest.mark.security_regression
def test_sec_t5_iter37_scrub_ip_text_ipv6_colon_ipv4_dot_and_ua_url() -> None:
    """ITER37 Objective 4: scrub_ip_text masks IPv6 after ':', IPv4 after '.', and IPv4 after UA-word URL paths."""
    from backend.core.share_db import scrub_ip_text

    # 1. IPv6 immediately after ':' (headers/keys)
    r_xff = scrub_ip_text("X-Forwarded-For:2001:db8:1234:5678:9abc:def0:1111:2222")
    assert "2001:db8:1234:5678:9abc:def0:1111:2222" not in r_xff
    assert r_xff.startswith("X-Forwarded-For:2001:db8:1234::")

    r_cip = scrub_ip_text("client_ip:2001:db8:1:2:3:4:5:6")
    assert "2001:db8:1:2:3:4:5:6" not in r_cip
    assert r_cip.startswith("client_ip:2001:db8:1::")

    r_foo = scrub_ip_text("foo:2001:db8:1234:5678::1")
    assert "2001:db8:1234:5678::1" not in r_foo
    assert r_foo.startswith("foo:2001:db8:1234::")

    r_ipv6_key = scrub_ip_text("ipv6:2001:db8:1234:5678::1")
    assert "2001:db8:1234:5678::1" not in r_ipv6_key
    assert r_ipv6_key.startswith("ipv6:2001:db8:1234::")

    # Invalid 9-group hex (4-char hextet prefix) must NOT be partially matched,
    # while 5+ digit numeric/hex prefixes (port/ID, impossible as a 1-4 hex IPv6 hextet) DO mask the IPv6:
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:7") == "2001:db8:1:2:3:4:5:6:7"
    assert scrub_ip_text("11111:2001:db8::1") == "11111:2001:db8::"

    # 2. IPv4 immediately after '.' (hostname/DNS label) while preserving 5+ part OIDs
    r_host = scrub_ip_text("host.203.0.113.42.example.com")
    assert "203.0.113.42" not in r_host
    assert r_host.startswith("host.") and r_host.endswith(".example.com")
    assert scrub_ip_text("OID 1.2.3.4.5 ok") == "OID 1.2.3.4.5 ok"

    # 3. IPv4 after case-insensitive UA-word URL paths must be masked, while real UAs stay preserved
    for url in (
        "https://example.com/mobile/203.0.113.42",
        "https://example.com/version/10.20.30.40",
        "https://example.com/Mobile/203.0.113.42",
        "GET /chrome/198.51.100.7 HTTP/1.1",
    ):
        scrubbed = scrub_ip_text(url)
        assert scrubbed != url, f"URL path IPv4 leaked: {url}"
        assert ".xxx" in scrubbed

    real_ua_1 = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0.1.2 Mobile/15E148 Safari/604.1"
    )
    real_ua_2 = (
        "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.6099.109 Mobile Safari/537.36"
    )
    assert scrub_ip_text(real_ua_1) == real_ua_1
    assert scrub_ip_text(real_ua_2) == real_ua_2


@pytest.mark.security_regression
def test_sec_t5_iter37_ch_strip_brackets_parity_and_live_chdb() -> None:
    """ITER37 Objective 3 (+ live chdb Objectives 1 & 2): exact O(N) bracket stripping & 3-way parity in chdb."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    from backend.core.share_db import SHARED_PII_KEYS
    from backend.high_scale.postgres_control import console_access

    assert len(console_access.MASKED_KEYS) == 18
    assert len(SHARED_PII_KEYS) == 18
    assert set(console_access.MASKED_KEYS) == set(SHARED_PII_KEYS)

    keys_sql = ", ".join(f"'{k}'" for k in console_access._MAP_MASKED_KEYS)
    map_view_expr = console_access._redaction("custom_fields", "Map(String, String)", keys_sql)
    assert map_view_expr is not None
    assert "arrayFold(" in map_view_expr
    assert r"\[.*\]" not in map_view_expr

    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER37_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER37_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter37 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER37_CHDB_ALL_OK" in run_res.stdout


_REAL_CHDB_ITER39_SCRIPT = r"""
import chdb.session
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

for analyzer_val in (0, 1):
    s = chdb.session.Session()
    s.query(f"SET allow_experimental_analyzer = {analyzer_val}")
    s.query("CREATE DATABASE IF NOT EXISTS fla_test")
    s.query("USE fla_test")
    s.query("DROP VIEW IF EXISTS fla_test.v_req")
    s.query("DROP TABLE IF EXISTS fla_test.request_facts")
    s.query("DROP TABLE IF EXISTS fla_test.high_scale_batch_publications")
    s.query(
        "CREATE TABLE fla_test.high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String) "
        "ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
    )
    s.query(
        "INSERT INTO fla_test.high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')"
    )
    s.query(
        "CREATE TABLE fla_test.request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(3, 'UTC'), client_ip String, status UInt16, url String, "
        "custom_fields Map(String, String)) ENGINE = Memory"
    )
    s.query(
        "INSERT INTO fla_test.request_facts VALUES ("
        "'svc_alpha', 'b1', 'visible', toDateTime64('2026-11-19 00:05:00.000', 3, 'UTC'), "
        "'203.0.113.42', 200, '/iter39', {'geo_city': 'Denver'})"
    )
    cols = [
        ("service_id", "String"),
        ("batch_id", "String"),
        ("publication_state", "String"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_ip", "String"),
        ("status", "UInt16"),
        ("url", "String"),
        ("custom_fields", "Map(String, String)"),
    ]
    view_sql = console_access._view_select("fla_test", "request_facts", cols, "svc_alpha", masked=True)
    s.query(f"CREATE VIEW fla_test.v_req AS {view_sql}")

    class _ChdbClient:
        _table_filters_verified = True

        def __init__(self, sess):
            self._s = sess

        def execute(self, sql, settings=None, **kwargs):
            import json as _json

            s_parts = [f"allow_experimental_analyzer = {analyzer_val}"]
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}"
            raw = self._s.query(full_sql, "JSON").bytes()
            return _json.loads(raw.decode("utf-8"))["data"] if raw else []

    class _Svc:
        service_id = "svc_alpha"

        def __init__(self, cli):
            self.client = cli

    cli = _ChdbClient(s)
    svc = _Svc(cli)
    masked_tables = {"request_facts": "v_req"}

    # 1. All 7 live APPLY bypasses must be blocked before reaching ClickHouse:
    for atk in (
        "SELECT * APPLY left -> left APPLY file FROM (SELECT '/etc/passwd' AS a FROM logs)",
        "SELECT * APPLY asc -> asc APPLY url FROM (SELECT 'http://169.254.169.254/' AS a FROM logs)",
        "SELECT * APPLY nulls -> nulls APPLY sleep FROM (SELECT 1 AS a FROM logs)",
        "SELECT * EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT * EXCEPT STRICT 'b' APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT a.* EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b) a",
        "SELECT * REPLACE (a AS a) EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
    ):
        try:
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
            )
            raise AssertionError(f"query_endpoint failed to block APPLY bypass (analyzer={analyzer_val}): {atk}")
        except PermissionError:
            pass

    # 2. Chained REPLACE (...) EXCEPT <col> and EXCEPT STRICT / REPLACE STRICT execute cleanly in chdb:
    for ok_sql, expected_cols in (
        (
            "WITH t AS (SELECT * REPLACE ('/x' AS url) EXCEPT status FROM logs) SELECT ip, timestamp FROM t",
            {"client_ip", "event_timestamp"},
        ),
        (
            "SELECT * REPLACE ('/x' AS url) EXCEPT status FROM logs",
            {"client_ip", "event_timestamp", "url"},
        ),
        (
            "SELECT * EXCEPT STRICT status FROM logs",
            {"client_ip", "event_timestamp", "url"},
        ),
        (
            "WITH t AS (SELECT * REPLACE STRICT ('/x' AS url) EXCEPT STRICT status FROM logs) SELECT ip, timestamp FROM t",
            {"client_ip", "event_timestamp"},
        ),
    ):
        res = hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )
        rows = res["data"]
        assert len(rows) == 1, f"Expected 1 row (analyzer={analyzer_val}) for {ok_sql}: got {rows}"
        assert expected_cols.issubset(set(rows[0].keys())), (
            f"Missing expected columns (analyzer={analyzer_val}) for {ok_sql}: got={set(rows[0].keys())}"
        )

    s.close()

print("ITER39_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter39_apply_lambda_and_except_strict_blocked() -> None:
    """ITER39 Objective 1: Block unparenthesized lambda APPLY <stopword> -> ... and EXCEPT STRICT ... APPLY."""
    from types import SimpleNamespace

    from backend.core.sql_validator import SQLValidationError, validate_user_sql
    from backend.models.analytics import QueryRequest as DashboardQueryRequest

    apply_bypasses = (
        "SELECT * APPLY left -> left APPLY file FROM (SELECT '/etc/passwd' AS a FROM logs)",
        "SELECT * APPLY asc -> asc APPLY url FROM (SELECT 'http://169.254.169.254/' AS a FROM logs)",
        "SELECT * APPLY nulls -> nulls APPLY sleep FROM (SELECT 1 AS a FROM logs)",
        "SELECT * EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT * EXCEPT STRICT 'b' APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT a.* EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b) a",
        "SELECT * REPLACE (a AS a) EXCEPT STRICT b APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT * APPLY from -> from APPLY file FROM (SELECT '/etc/passwd' AS a FROM logs)",
        "SELECT * APPLY x -> x + 1 APPLY file FROM (SELECT 1 AS a FROM logs)",
        "SELECT * APPLY x -> 'a' APPLY file FROM (SELECT 1 AS a FROM logs)",
        "SELECT * EXCEPT STRICT (b) APPLY file FROM (SELECT '/etc/hosts' AS a, 1 AS b)",
        "SELECT * APPLY left ->+1 APPLY file FROM (SELECT 1 AS a FROM logs)",
    )

    class _DenyClient:
        _table_filters_verified = True

        def execute(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("APPLY bypass must never reach ClickHouse")

    svc = SimpleNamespace(service_id="svc_alpha", client=_DenyClient())
    masked_tables = {"request_facts": "fla_cv_svc_m_request_facts"}

    for atk in apply_bypasses:
        with pytest.raises(SQLValidationError):
            validate_user_sql(atk, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True)
        with pytest.raises(PermissionError):
            hs_query._reject_unsafe_functions(atk, parse_tree=None)
        with pytest.raises(PermissionError):
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z",
                "2026-11-19T01:00:00Z",
                mask_ips=True,
                tables=masked_tables,
            )

    # Legitimate ordinary uses of 'apply' must remain allowed in both layers:
    for ok_sql in (
        "SELECT apply FROM logs",
        "SELECT apply alias FROM logs",
        "SELECT status apply FROM logs",
        "SELECT status AS apply FROM logs",
        "SELECT count(*) apply FROM logs",
        "SELECT 2 * apply FROM logs",
        "SELECT t.apply FROM logs t",
        "SELECT * FROM (SELECT 1) apply ARRAY JOIN [1] AS arr",
        "SELECT * FROM logs apply ARRAY JOIN [1] AS arr",
        "WITH apply AS (SELECT 1 AS x FROM logs) SELECT a.x FROM apply a",
    ):
        assert validate_user_sql(ok_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        hs_query._reject_unsafe_functions(ok_sql, parse_tree=None)


@pytest.mark.security_regression
def test_sec_t5_iter39_replace_then_except_and_strict_star_transformers() -> None:
    """ITER39 Objective 2: Recognize EXCEPT after REPLACE (...), EXCEPT STRICT, and REPLACE STRICT."""
    from backend.core.sql_validator import validate_user_sql

    names = {"request_facts": "fla_cv_svc_m_request_facts"}

    for cte_sql in (
        "WITH t AS (SELECT * REPLACE ('/x' AS url) EXCEPT status FROM logs) SELECT ip, timestamp FROM t",
        "WITH t AS (SELECT * REPLACE STRICT ('/x' AS url) EXCEPT STRICT status FROM logs) SELECT ip, timestamp FROM t",
        "WITH t AS (SELECT r.* REPLACE ('/x' AS url) EXCEPT STRICT status FROM logs r) SELECT ip, timestamp FROM t",
    ):
        assert validate_user_sql(cte_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw = hs_query._rewrite_console_sql(cte_sql, svc_table="request_facts", names=names)
        assert "custom_fields['status']" not in rw, f"EXCEPT status corrupted: {rw}"
        assert "client_ip" in rw and "event_timestamp" in rw, f"projects_star failed: {rw}"

    for cf_sql in (
        "SELECT * REPLACE (geo_city AS status) EXCEPT geo_city FROM logs",
        "SELECT * REPLACE STRICT (geo_city AS status) EXCEPT STRICT geo_city FROM logs",
        "SELECT * EXCEPT STRICT geo_city FROM logs",
        "SELECT * EXCEPT STRICT (geo_city, bot_score) FROM logs",
    ):
        assert validate_user_sql(cf_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw = hs_query._rewrite_console_sql(cf_sql, svc_table="request_facts", names=names)
        assert "EXCEPT custom_fields['geo_city']" not in rw, f"EXCEPT col corrupted into custom_fields: {rw}"
        assert "EXCEPT STRICT custom_fields['geo_city']" not in rw, f"EXCEPT STRICT col corrupted: {rw}"
        assert "custom_fields['bot_score']" not in rw, f"EXCEPT STRICT tuple col corrupted: {rw}"

    # Set operation EXCEPT must NOT be mistaken for star-except (both unparenthesized and parenthesized):
    from backend.core.sql_validator import _is_star_except_clause, _tokenize_sql

    setop_sql = "SELECT ip FROM logs EXCEPT SELECT ip FROM logs"
    tree = validate_user_sql(setop_sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
    assert tree["statements"][0]["node"]["type"] == "SET_OPERATION_NODE"
    rw_setop = hs_query._rewrite_console_sql(setop_sql, svc_table="request_facts", names=names)
    assert rw_setop.count("client_ip") == 2

    paren_setop_sql = "(SELECT ip FROM logs) EXCEPT (SELECT ip FROM logs)"
    paren_toks, _ = _tokenize_sql(paren_setop_sql)
    assert paren_toks is not None
    ex_idx = next(i for i, t in enumerate(paren_toks) if t.val == "EXCEPT")
    assert not _is_star_except_clause(paren_toks, ex_idx)
    rw_paren_setop = hs_query._rewrite_console_sql(paren_setop_sql, svc_table="request_facts", names=names)
    assert rw_paren_setop.count("client_ip") == 2


@pytest.mark.security_regression
def test_sec_t5_iter39_scrub_ip_text_long_hex_and_dcolon_prefixes() -> None:
    """ITER39 Objective 3: scrub_ip_text masks IPv6 after 5+ hex/digit ':' prefix or non-hex '::' prefix."""
    from backend.core.share_db import scrub_ip_text

    assert scrub_ip_text("12345:2001:db8:85a3::8a2e:370:7334") == "12345:2001:db8:85a3::"
    assert scrub_ip_text("deadbeef:2001:db8:85a3::8a2e:370:7334") == "deadbeef:2001:db8:85a3::"
    assert scrub_ip_text("abcdef:fe80::1") == "abcdef:fe80::"
    assert scrub_ip_text("foo::2001:db8::1") == "foo::2001:db8::"
    assert scrub_ip_text("deadbeef::2001:db8::1") == "deadbeef::2001:db8::"

    # Malformed 9-group IPv6 with 1-4 hex hextet prefix stays unmasked:
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:7") == "2001:db8:1:2:3:4:5:6:7"
    assert scrub_ip_text("2001::1:2:3:4:5:6:7") == "2001::1:2:3:4:5:6:7"


@pytest.mark.security_regression
def test_sec_t5_iter39_live_chdb_apply_and_replace_except() -> None:
    """ITER39 live chdb verification (analyzer=0 & 1) for Objectives 1 & 2."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER39_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER39_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter39 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER39_CHDB_ALL_OK" in run_res.stdout


_REAL_CHDB_ITER40_SCRIPT = r"""
import chdb.session
from backend.high_scale import query as hs_query
from backend.high_scale.postgres_control import console_access
from backend.models.analytics import QueryRequest as DashboardQueryRequest

for analyzer_val in (0, 1):
    s = chdb.session.Session()
    s.query(f"SET allow_experimental_analyzer = {analyzer_val}")
    s.query("CREATE DATABASE IF NOT EXISTS fla_test")
    s.query("USE fla_test")
    s.query("DROP VIEW IF EXISTS fla_test.v_req")
    s.query("DROP TABLE IF EXISTS fla_test.request_facts")
    s.query("DROP TABLE IF EXISTS fla_test.high_scale_batch_publications")
    s.query(
        "CREATE TABLE fla_test.high_scale_batch_publications ("
        "service_id String, domain String, batch_id String, publication_state String) "
        "ENGINE = ReplacingMergeTree ORDER BY (service_id, domain, batch_id)"
    )
    s.query(
        "INSERT INTO fla_test.high_scale_batch_publications VALUES ('svc_alpha', 'request', 'b1', 'visible')"
    )
    s.query(
        "CREATE TABLE fla_test.request_facts ("
        "service_id String, batch_id String, publication_state String, "
        "event_timestamp DateTime64(3, 'UTC'), client_ip String, status UInt16, url String, "
        "custom_fields Map(String, String)) ENGINE = Memory"
    )
    s.query(
        "INSERT INTO fla_test.request_facts VALUES ("
        "'svc_alpha', 'b1', 'visible', toDateTime64('2026-11-19 00:05:00.000', 3, 'UTC'), "
        "'203.0.113.42', 200, '/iter40', {'geo_city': 'Denver'})"
    )
    cols = [
        ("service_id", "String"),
        ("batch_id", "String"),
        ("publication_state", "String"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_ip", "String"),
        ("status", "UInt16"),
        ("url", "String"),
        ("custom_fields", "Map(String, String)"),
    ]
    view_sql = console_access._view_select("fla_test", "request_facts", cols, "svc_alpha", masked=True)
    s.query(f"CREATE VIEW fla_test.v_req AS {view_sql}")

    class _ChdbClient:
        _table_filters_verified = True

        def __init__(self, sess):
            self._s = sess

        def execute(self, sql, settings=None, **kwargs):
            import json as _json

            s_parts = [f"allow_experimental_analyzer = {analyzer_val}"]
            for k, v in (settings or {}).items():
                val_esc = str(v).replace("\\", "\\\\").replace("'", "\\'")
                s_parts.append(f"{k} = '{val_esc}'")
            full_sql = f"{sql} SETTINGS {', '.join(s_parts)}"
            raw = self._s.query(full_sql, "JSON").bytes()
            return _json.loads(raw.decode("utf-8"))["data"] if raw else []

    class _Svc:
        service_id = "svc_alpha"

        def __init__(self, cli):
            self.client = cli

    cli = _ChdbClient(s)
    svc = _Svc(cli)
    masked_tables = {"request_facts": "v_req"}

    # 1. Parenthesis-free REPLACE ... APPLY bypasses must be blocked before reaching ClickHouse:
    for atk in (
        "SELECT * REPLACE 1 AS all APPLY sleep FROM logs",
        "SELECT * REPLACE 'max_threads' AS url REPLACE 1 AS all APPLY getSetting FROM logs",
        "SELECT * REPLACE STRICT 1 AS where APPLY sleep FROM logs",
    ):
        try:
            hs_query.query_endpoint(
                svc,
                DashboardQueryRequest(sql=atk, max_rows=10),
                "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
            )
            raise AssertionError(f"query_endpoint failed to block APPLY bypass (analyzer={analyzer_val}): {atk}")
        except PermissionError:
            pass

    # 2. Parenthesis-free REPLACE ... EXCEPT (status) / EXCEPT status execute cleanly in chdb:
    for ok_sql in (
        "SELECT * REPLACE '/x' AS url EXCEPT (status) FROM logs",
        "SELECT * REPLACE '/x' AS url EXCEPT status FROM logs",
        "SELECT * REPLACE STRICT '/x' AS url EXCEPT STRICT status FROM logs",
    ):
        res = hs_query.query_endpoint(
            svc,
            DashboardQueryRequest(sql=ok_sql, max_rows=10),
            "2026-11-19T00:00:00Z", "2026-11-19T01:00:00Z", mask_ips=True, tables=masked_tables,
        )
        rows = res["data"]
        assert len(rows) == 1, f"expected 1 row for {ok_sql} (analyzer={analyzer_val}), got {rows}"
        row = rows[0]
        assert "status" not in row, f"EXCEPT status leaked into row for {ok_sql}: {row}"
        assert row["url"] == "/x", f"REPLACE url failed for {ok_sql}: {row}"
        assert row["client_ip"] == "[redacted]", f"IP not masked for {ok_sql}: {row}"
        assert "203.0.113.42" not in str(row), f"Raw IP leaked for {ok_sql}: {row}"

    s.close()

print("ITER40_CHDB_ALL_OK")
"""


@pytest.mark.security_regression
def test_sec_t5_iter40_paren_free_replace_apply_bypass() -> None:
    """ITER40 Objective 1A: parenthesis-free REPLACE ... AS <kw> APPLY is blocked in both sql_validator and query."""
    from backend.core.sql_validator import SQLValidationError, validate_user_sql

    bypasses = (
        "SELECT * REPLACE 1 AS all APPLY sleep FROM logs",
        "SELECT * REPLACE 'max_threads' AS url REPLACE 1 AS all APPLY getSetting FROM logs",
        "SELECT * REPLACE STRICT 1 AS where APPLY sleep FROM logs",
        "SELECT * REPLACE 1 AS from APPLY sleep FROM logs",
        "SELECT t.* REPLACE 1 AS all APPLY sleep FROM logs t",
    )
    for sql in bypasses:
        with pytest.raises(SQLValidationError):
            validate_user_sql(sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True)
        with pytest.raises(PermissionError, match="APPLY column transformer"):
            hs_query._reject_unsafe_functions(sql)


@pytest.mark.security_regression
def test_sec_t5_iter40_paren_free_replace_except_rewrite() -> None:
    """ITER40 Objective 1B: parenthesis-free REPLACE [STRICT] <expr> AS <col> EXCEPT preserves EXCEPT cols."""
    from backend.core.sql_validator import _is_star_except_clause, _tokenize_sql, validate_user_sql

    names = {"request_facts": "fla_cv_svc_m_request_facts"}
    for sql in (
        "SELECT * REPLACE 1 AS url EXCEPT (status) FROM logs",
        "SELECT * REPLACE 1 AS url EXCEPT status FROM logs",
        "SELECT * REPLACE STRICT 1 AS url EXCEPT STRICT status FROM logs",
        "SELECT * REPLACE 1 AS url EXCEPT (geo_city, bot_score) FROM logs",
        "SELECT * REPLACE 1 AS url REPLACE 2 AS status EXCEPT (geo_city) FROM logs",
        "SELECT * REPLACE replace(url, 'a', 'b') AS url EXCEPT status FROM logs",
        "SELECT t.* REPLACE 1 AS url EXCEPT status FROM logs t",
    ):
        toks, _ = _tokenize_sql(sql)
        assert toks is not None
        ex_idx = next(i for i, t in enumerate(toks) if t.val.upper() == "EXCEPT")
        assert _is_star_except_clause(toks, ex_idx), f"star EXCEPT not detected in validator: {sql}"
        assert validate_user_sql(sql, allowed_tables=frozenset({"logs"}), case_sensitive_ctes=True).parse_tree
        rw = hs_query._rewrite_console_sql(sql, svc_table="request_facts", names=names)
        assert "custom_fields['status']" not in rw, f"EXCEPT status corrupted into custom_fields: {rw}"
        assert "custom_fields['geo_city']" not in rw, f"EXCEPT geo_city corrupted into custom_fields: {rw}"
        assert "custom_fields['bot_score']" not in rw, f"EXCEPT bot_score corrupted into custom_fields: {rw}"

    # Set-operation EXCEPT after AS <alias> or replace() function must NOT be mistaken for star-except:
    for setop_sql in (
        "SELECT ip AS x EXCEPT (SELECT ip FROM logs)",
        "SELECT replace(url, 'a', 'b') AS x EXCEPT (SELECT url FROM logs)",
    ):
        toks, _ = _tokenize_sql(setop_sql)
        assert toks is not None
        ex_idx = next(i for i, t in enumerate(toks) if t.val.upper() == "EXCEPT")
        assert not _is_star_except_clause(toks, ex_idx), f"set-op EXCEPT misclassified as star-except: {setop_sql}"


@pytest.mark.security_regression
def test_sec_t5_iter40_scrub_ip_text_ipv6_trailing_colon_port_and_short_hex() -> None:
    """ITER40 Objective 2: scrub_ip_text masks IPv6 with trailing colon, 8-group :port, and short alpha hex prefix."""
    from backend.core.share_db import scrub_ip_text

    # 1. Trailing colon followed by space / path / punctuation:
    assert scrub_ip_text("2001:db8::1: connection refused") == "2001:db8::: connection refused"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6: error") == "2001:db8:1::: error"
    assert scrub_ip_text("2001:db8::1:/api") == "2001:db8:::/api"
    assert scrub_ip_text("::1: error") == "::: error"

    # 2. 8-group IPv6 + :port:
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:8080") == "2001:db8:1:::8080"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:443") == "2001:db8:1:::443"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:80") == "2001:db8:1:::80"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:65535") == "2001:db8:1:::65535"

    # 3. Short alphabetic hex prefix (1-4 alpha hex key name + ':' / '::' + IPv6):
    assert scrub_ip_text("a:2001:db8:1:2:3:4:5:6") == "a:2001:db8:1::"
    assert scrub_ip_text("cafe:2001:db8:1:2:3:4:5:6") == "cafe:2001:db8:1::"
    assert scrub_ip_text("a::2001:db8::1") == "a::2001:db8::"

    # 4. Malformed 9-group IPv6 with numeric/mixed hextet prefix stays unmasked:
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:7") == "2001:db8:1:2:3:4:5:6:7"
    assert scrub_ip_text("2001::1:2:3:4:5:6:7") == "2001::1:2:3:4:5:6:7"

    # 5. ITER41: 8-group IPv6 + :port: trailing colon, 7-group trailing '::', IPv4-embedded IPv6, timestamp/key prefixes:
    assert scrub_ip_text("dial tcp 2001:db8:1:2:3:4:5:6:443: i/o timeout") == "dial tcp 2001:db8:1:::443: i/o timeout"
    assert (
        scrub_ip_text("connect 2001:db8:1:2:3:4:5:6:8080: connection refused")
        == "connect 2001:db8:1:::8080: connection refused"
    )
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:8080:/api") == "2001:db8:1:::8080:/api"
    assert scrub_ip_text("2001:db8:85a3:1234:5678:9abc:def0::") == "2001:db8:85a3::"
    assert scrub_ip_text("2001:db8:85a3:1234:5678:8a2e:203.0.113.42") == "2001:db8:85a3::"
    assert scrub_ip_text("16:42:00:2001:0db8:85a3:0000:0000:8a2e:0370:733a") == "16:42:00:2001:db8:85a3::"
    assert scrub_ip_text("c1:2001:0db8:85a3:0000:0000:8a2e:0370:733a") == "c1:2001:db8:85a3::"

    # 6. ITER42: post-IPv6 UA spans (no IPv4 leak / UA corruption), IPv4-embedded IPv6 with :port/:text,
    #    8-group IPv6 + :port:extra, and '::' prefix before compressed IPv6:
    assert (
        scrub_ip_text("src=2001:0db8:85a3:0000:0000:8a2e:0370:733a Chrome/120.0.0.1 client=198.51.100.42")
        == "src=2001:db8:85a3:: Chrome/120.0.0.1 client=198.51.100.xxx"
    )
    assert scrub_ip_text("::ffff:192.168.10.20:80:err") == ":::80:err"
    assert scrub_ip_text("::ffff:192.168.10.20:8") == ":::8"
    assert scrub_ip_text("2001:db8::192.168.10.20:80:abc") == "2001:db8:::80:abc"
    assert scrub_ip_text("2001:db8:1:2:3:4:192.168.10.20:80:abc") == "2001:db8:1:::80:abc"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:80:extra") == "2001:db8:1:::80:extra"
    assert scrub_ip_text("::2001:db8:abcd:ef01::1") == "::2001:db8:abcd::"

    # 7. ITER43: 8-group & compressed IPv6 followed by :<word> starting with hex letters (e.g., :error, :failed):
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:error") == "2001:db8:1:::error"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:extra") == "2001:db8:1:::extra"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:auth") == "2001:db8:1:::auth"
    assert scrub_ip_text("2001:db8:1:2:3:4:5:6:failed") == "2001:db8:1:::failed"
    assert scrub_ip_text("2001:db8::1:error") == "2001:db8:::error"
    assert scrub_ip_text("2001:db8::1:failed") == "2001:db8:::failed"

    # 8. ITER44: 1-4 hex / timestamp prefix before 6-group IPv4-embedded IPv6 with 2-3 digit 1st octet,
    #    plus O(N) performance on 64 KiB UA-heavy input:
    import time as _time

    assert scrub_ip_text("a:2001:db8:1:2:3:4:192.168.10.20") == "a:2001:db8:1::"
    assert scrub_ip_text("cafe:2001:db8:1:2:3:4:203.0.113.42") == "cafe:2001:db8:1::"
    assert scrub_ip_text("c1:2001:db8:1:2:3:4:10.0.0.1") == "c1:2001:db8:1::"
    assert scrub_ip_text("16:42:00:2001:db8:1:2:3:4:203.0.113.42") == "16:42:00:2001:db8:1::"
    ua_64k = "Chrome/1.2.3.4 " * 4369
    t0 = _time.perf_counter()
    assert scrub_ip_text(ua_64k) == ua_64k
    assert (_time.perf_counter() - t0) < 0.15


@pytest.mark.security_regression
def test_sec_t5_iter40_live_chdb_paren_free_replace() -> None:
    """ITER40 live chdb verification (analyzer=0 & 1) for parenthesis-free REPLACE + APPLY / EXCEPT."""
    import os
    import shutil
    import site
    import subprocess
    from pathlib import Path

    repo_root = str(Path(hs_query.__file__).resolve().parents[2])
    try:
        import chdb as _chdb  # noqa: F401

        exec(_REAL_CHDB_ITER40_SCRIPT, {})
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
                    "python",
                    "-c",
                    "import chdb",
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
                        "python",
                        "-c",
                        _REAL_CHDB_ITER40_SCRIPT,
                    ],
                    cwd=repo_root,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                assert run_res.returncode == 0, (
                    f"Real chdb Iter40 suite failed:\nSTDOUT:\n{run_res.stdout}\nSTDERR:\n{run_res.stderr}"
                )
                assert "ITER40_CHDB_ALL_OK" in run_res.stdout


def test_hs_query_explain_and_portable_presets_with_categories() -> None:
    from backend.models.analytics import PresetQuery, QueryResponse
    from backend.routers.query import _get_presets

    seen_sql: list[str] = []

    class _ExplainClient:
        def execute(self, sql: str, params=None, settings=None):
            if "system.one" in sql:
                return [{"n": 0}]
            seen_sql.append(sql)
            if sql.lstrip().upper().startswith("EXPLAIN "):
                return [{"explain": "Expression (Projection)"}, {"explain": "ReadFromMergeTree"}]
            return [{"url": "/index.html", "status": 200}]

    svc = SimpleNamespace(service_id="svc-explain", client=_ExplainClient())
    res = hs_query.query_endpoint(
        svc,
        DashboardQueryRequest(sql="SELECT url, status FROM logs", explain=True),
        "2026-11-19T00:00:00Z",
        "2026-11-19T01:00:00Z",
        mask_ips=False,
    )
    validated = QueryResponse.model_validate(res)
    assert validated.explain_plan == "Expression (Projection)\nReadFromMergeTree"
    assert any(s.lstrip().upper().startswith("EXPLAIN ") for s in seen_sql)

    presets = _get_presets({"name": "svc_explain", "service_id": "svc-explain"})
    assert len(presets) == 26
    for raw in presets:
        preset = PresetQuery.model_validate(raw)
        assert preset.category is not None
        assert "SUMMARIZE" not in preset.sql.upper()
        assert "percentile_cont" not in preset.sql.lower()
        assert "starts_with(" not in preset.sql.lower()
