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
