"""High-scale Security reads return real data under the standard response contract."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.high_scale import dashboard as hs_dashboard
from backend.high_scale import security as hs
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService

START = "2026-10-18T00:00:00Z"
END = "2026-10-19T00:00:00Z"
_EPOCH = int(datetime(2026, 10, 18, 3, 0, tzinfo=UTC).timestamp())
_TAG = re.compile(r"hs_security:(\w+)")


class _Client:
    """Routes each query to canned rows by its ``hs_security:<tag>`` marker and records every call."""

    def __init__(self, rows: dict[str, list[dict[str, Any]]] | None = None, error: Exception | None = None):
        self.rows = rows or {}
        self.error = error
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        match = _TAG.search(sql)
        assert match, f"query is missing its hs_security tag: {sql[:120]}"
        tag = match.group(1)
        self.calls.append((tag, sql, params or {}))
        return self.rows.get(tag, [])

    def tags(self) -> list[str]:
        return [tag for tag, _, _ in self.calls]

    def call(self, tag: str) -> tuple[str, dict[str, Any]]:
        return next((sql, params) for t, sql, params in self.calls if t == tag)


def _service(client: _Client) -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=client,
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc",
            domain="request",
            owner_epoch=1,
            coverage_start=None,
            coverage_end=None,
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )


def _req(filters: dict[str, Any] | None = None, bucket_seconds: int = 3600) -> SimpleNamespace:
    return SimpleNamespace(filters=filters or {}, bucket_seconds=bucket_seconds)


_COUNTRY_US = {"country": {"mode": "include", "values": ["US"]}}
_BOT_ENTRY = {"id": "googlebot", "categories": ["search-engine"], "verification": {"domains": ["googlebot.com"]}}


def _match_googlebot(ua: str | None) -> tuple[dict[str, Any], ...]:
    return (_BOT_ENTRY,) if "Googlebot" in (ua or "") else ()


def test_every_section_uses_the_standard_contract_shapes():
    client = _Client(
        {
            "tls_fingerprints": [{"fingerprint": "abc", "ip_count": "2", "request_count": "5"}],
            "fingerprint_coverage": [{"total_rows": "10", "populated": "4"}],
            "req_size_dist": [{"bucket": "0-256B", "count": "9", "min_val": "0"}],
            "conn_reuse_dist": [{"bucket": "2-5", "count": "4", "min_val": "2"}],
            "ipv6_adoption": [{"bucket_ts": _EPOCH, "pct": 40.0}],
            "proxy_dist": [{"type": "vpn", "count": "7"}],
            "verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_type": "search-engine", "count": "3"}],
            "top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}],
        }
    )
    res = hs.security_aggregates(
        _service(client),
        _req(),
        START,
        END,
        sections={
            "tls_fingerprints",
            "fingerprint_coverage",
            "req_size_dist",
            "conn_reuse_dist",
            "ipv6_adoption",
            "proxy_dist",
            "verified_bots_ts",
            "top_ips_header",
        },
    ).model_dump(exclude_unset=True)

    assert res["tls_fingerprints"] == [{"fingerprint": "abc", "ip_count": 2, "request_count": 5}]
    assert res["fingerprint_coverage"] == {"tls_ciphers_sha": 0.4}
    assert res["req_size_dist"] == [{"bucket": "0-256B", "count": 9}]
    assert res["conn_reuse_dist"] == [{"bucket": "2-5", "count": 4}]
    assert res["ipv6_adoption"] == [{"time": "2026-10-18T03:00:00Z", "pct": 40.0}]
    assert res["proxy_dist"] == [{"type": "vpn", "count": 7}]
    assert res["verified_bots_ts"] == [{"time": "2026-10-18T03:00:00Z", "bot_type": "search-engine", "count": 3}]
    assert res["top_ips_header"] == [{"ip": "10.0.1.5", "max_header": 9000}]


def test_fingerprint_coverage_is_computed_not_hardcoded():
    client = _Client({"fingerprint_coverage": [{"total_rows": "0", "populated": "0"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"fingerprint_coverage"})
    assert res.fingerprint_coverage == {"tls_ciphers_sha": 0.0}


def test_proxy_and_header_sections_read_the_standard_columns():
    client = _Client()
    hs.security_aggregates(
        _service(client), _req(), START, END, sections={"proxy_dist", "req_size_dist", "ipv6_adoption"}
    )
    assert "custom_fields['p_type']" in client.call("proxy_dist")[0]
    assert "req_header_bytes" in client.call("req_size_dist")[0]
    assert "toStartOfHour" in client.call("ipv6_adoption")[0]


def test_sections_selector_only_runs_requested_queries():
    client = _Client()
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    assert client.tags() == ["proxy_dist"]
    assert res.tls_fingerprints == []


def test_no_sections_runs_every_section_including_bots_and_proxies():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(), START, END)
    assert set(client.tags()) >= {
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "verified_bots_ts",
        "ngwaf_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "top_bots_ua",
        "proxy_stats",
        "traffic_quality",
        "suspicious_isps",
        "active_clients",
    }


def test_request_filters_are_bound_into_every_request_facts_query():
    client = _Client()
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.core.duckdb.get_asn_names", return_value={}),
    ):
        hs.security_aggregates(_service(client), _req(_COUNTRY_US), START, END)
    assert client.calls
    for tag, sql, params in client.calls:
        assert params["filter_0_0"] == "US", tag
        assert "country IN ({filter_0_0:String})" in sql, tag


def test_unfiltered_ngwaf_sections_read_the_security_projection():
    client = _Client(
        {
            "ngwaf_bots": [
                {"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"},
                {"bot_name": "other", "wk_name": "", "bot_cat": "", "request_count": "2"},
            ],
            "ngwaf_verified_bots_ts": [{"bucket_ts": _EPOCH, "bot_name": "bingbot", "count": "4"}],
        }
    )
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"):
        res = hs.security_aggregates(
            _service(client), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    summary_sql = client.call("ngwaf_bots")[0]
    assert "FROM security_minute_dimensions" in summary_sql
    assert "dimension='ngwaf_bot'" in summary_sql
    assert "request_facts" not in summary_sql
    assert "FROM security_minute_dimensions" in client.call("ngwaf_verified_bots_ts")[0]
    assert res.ngwaf_configured is True
    assert [r.model_dump() for r in res.ngwaf_verified_bots] == [
        {"bot_name": "bingbot", "wellknown_bot_name": "bing", "category": "search", "request_count": 11},
        {"bot_name": "other", "wellknown_bot_name": None, "category": None, "request_count": 2},
    ]
    assert [r.model_dump() for r in res.ngwaf_verified_bots_ts] == [
        {"time": "2026-10-18T03:00:00Z", "bot_name": "bingbot", "count": 4}
    ]


def test_filtered_ngwaf_sections_read_the_persisted_fact_keys():
    client = _Client()
    hs.security_aggregates(
        _service(client),
        _req(_COUNTRY_US),
        START,
        END,
        sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
    )
    for tag in ("ngwaf_bots", "ngwaf_verified_bots_ts"):
        sql, params = client.call(tag)
        assert "FROM request_facts" in sql
        assert "custom_fields['ngwaf_bot']" in sql
        assert "security_minute_dimensions" not in sql
        assert params["filter_0_0"] == "US"


def test_ngwaf_not_configured_is_reported_from_service_config():
    with patch("backend.config.get_ngwaf_workspace_id", return_value=None):
        res = hs.security_aggregates(
            _service(_Client()), _req(), START, END, sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        )
    assert res.ngwaf_configured is False


def test_wellknown_bots_carry_real_verification_counters():
    client = _Client(
        {
            "wellknown_bots": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "66.249.66.1", "cnt": "8"},
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "ip": "203.0.113.9", "cnt": "2"},
            ]
        }
    )
    hostnames = {
        "66.249.66.1": ("crawl-66-249-66-1.googlebot.com", "resolved", True),
        "203.0.113.9": ("evil.example.net", "resolved", False),
    }
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value="(?i)googlebot"),
        patch("backend.utils.rdns_cache.get_hostnames", return_value=hostnames),
        patch("backend.utils.rdns_cache.enqueue"),
    ):
        res = hs.security_aggregates(_service(client), _req(), START, END, sections={"wellknown_bots"})
    sql, params = client.call("wellknown_bots")
    assert "match(custom_fields['ua'], {bot_pattern:String})" in sql
    assert params["bot_pattern"] == "(?i)googlebot"
    assert len(res.wellknown_bots) == 1
    bot = res.wellknown_bots[0]
    assert (bot.id, bot.request_count) == ("googlebot", 10)
    assert bot.verified_count == 8
    assert bot.impersonator_count == 2
    assert bot.verification_coverage == 1.0


def test_top_bots_classifies_user_agents_and_lists_ngwaf_bots():
    client = _Client(
        {
            "top_bots_ua": [
                {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"},
                {"ua": "curl/8", "cnt": "50"},
            ],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "bing", "bot_cat": "search", "request_count": "11"}],
        }
    )
    with patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot):
        res = hs.top_bots(_service(client), _req(), START, END)
    assert [b.model_dump(exclude_unset=True) for b in res.bots] == [
        {"id": "googlebot", "name": "Googlebot", "category": "search-engine", "request_count": 30}
    ]
    assert [b.model_dump(exclude_unset=True) for b in res.ngwaf_bots] == [
        {"name": "bingbot", "category": "search", "request_count": 11}
    ]
    assert "LIMIT 10" in client.call("ngwaf_bots")[0]


def test_top_bots_honours_request_filters():
    client = _Client()
    hs.top_bots(_service(client), _req(_COUNTRY_US), START, END)
    assert all(params["filter_0_0"] == "US" for _, _, params in client.calls)


_CLIENT_ROW = {
    "ip": "198.51.100.77",
    "asn": "15169",
    "rtt_min_ms": 5.0,
    "tcp_rtt_ms": 100.0,
    "distance_km": 8800.4,
    "pop": "SJC",
    "client_lat": 51.5074,
    "client_lon": -0.1278,
    "pop_lat": 37.3382,
    "pop_lon": -121.8863,
    "impossible_distance": 1,
    "risk_level": "High",
    "country": "GB",
    "city": "",
}


def _proxy_client() -> _Client:
    return _Client(
        {
            "proxy_stats": [
                {"active_proxies_count": "3", "total_requests_count": "40", "distance_mismatches_count": "2"}
            ],
            "traffic_quality": [
                {"type": "Direct Connection", "count": "75"},
                {"type": "Active Tunnel / Proxy", "count": "25"},
            ],
            "suspicious_isps": [{"asn": "15169", "count": "25"}, {"asn": None, "count": "1"}],
            "active_clients": [_CLIENT_ROW],
        }
    )


def test_proxies_report_real_counts_quality_isps_and_clients():
    with patch("backend.core.duckdb.get_asn_names", return_value={15169: "Example Net"}):
        res = hs.security_aggregates(_service(_proxy_client()), _req(), START, END, sections={"proxies"})
    assert (res.active_proxies_count, res.tunnel_requests_count, res.distance_mismatches_count) == (3, 40, 2)
    assert [(q.label, q.value) for q in res.traffic_quality] == [
        ("Direct Connection", 75.0),
        ("Active Tunnel / Proxy", 25.0),
    ]
    assert [(i.isp, i.asn, i.count) for i in res.suspicious_isps] == [
        ("Example Net", 15169, 25),
        ("Unknown ISP", None, 1),
    ]
    client = res.active_clients[0]
    assert client.ip == "198.51.100.77"
    assert client.asn_name == "Example Net"
    assert client.impossible_distance is True
    assert client.risk_level == "High"
    assert client.city is None
    assert client.country == "GB"
    assert client.distance_km == 8800.4


def test_proxies_mask_client_ips_for_analyst_sessions():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        masked = hs.security_aggregates(
            _service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=True
        )
        clear = hs.security_aggregates(
            _service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=False
        )
    assert masked.active_clients[0].ip == "198.51.100.xxx"
    assert clear.active_clients[0].ip == "198.51.100.77"


def test_top_ips_header_masks_ips_for_analyst_sessions():
    client = _Client({"top_ips_header": [{"ip": "10.0.1.5", "max_header": "9000"}]})
    res = hs.security_aggregates(_service(client), _req(), START, END, sections={"top_ips_header"}, mask_ips=True)
    assert res.top_ips_header[0].ip == "10.0.1.xxx"


def test_proxies_section_is_served_inside_aggregates_with_masking():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.security_aggregates(_service(_proxy_client()), _req(), START, END, sections={"proxies"}, mask_ips=True)
    assert res.active_proxies_count == 3
    assert res.active_clients[0].ip == "198.51.100.xxx"


def test_proxies_with_no_rows_report_zero_without_dividing_by_nothing():
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        res = hs.security_aggregates(_service(_Client()), _req(), START, END, sections={"proxies"})
    assert res.active_proxies_count == 0
    assert res.traffic_quality == []
    assert res.active_clients == []


def test_active_clients_list_high_risk_first():
    client = _proxy_client()
    with patch("backend.core.duckdb.get_asn_names", return_value={}):
        hs.security_aggregates(_service(client), _req(), START, END, sections={"proxies"})
    assert "ORDER BY multiIf(risk_level = 'High', 0, risk_level = 'Medium', 1, 2)" in client.call("active_clients")[0]


def test_clickhouse_failures_propagate_instead_of_rendering_empty_cards():
    client = _Client(error=RuntimeError("clickhouse unavailable"))
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.security_aggregates(_service(client), _req(), START, END, sections={"proxy_dist"})
    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        hs.top_bots(_service(client), _req(), START, END)


def test_dashboard_bundle_serves_real_top_bots():
    client = _Client(
        {
            "top_bots_ua": [{"ua": "Mozilla/5.0 (compatible; Googlebot/2.1)", "cnt": "30"}],
            "ngwaf_bots": [{"bot_name": "bingbot", "wk_name": "", "bot_cat": "search", "request_count": "11"}],
        }
    )
    request = SimpleNamespace(filters={}, bucket_seconds=3600, sections=None)
    with (
        patch("backend.utils.bot_sources.build_matcher", return_value=_match_googlebot),
        patch.object(hs_dashboard, "aggregates", return_value="aggregates"),
    ):
        out = hs_dashboard.bundle(_service(client), request, START, END)
    assert out["aggregates"] == "aggregates"
    assert [b.name for b in out["top_bots"].bots] == ["Googlebot"]
    assert [b.name for b in out["top_bots"].ngwaf_bots] == ["bingbot"]


# ── Merged from test_security_ngwaf.py ───────────────────────────────

import gzip
from datetime import UTC, datetime, timedelta

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import HighScaleIngestController, _enrich_high_scale_ngwaf_bots
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
)
from backend.high_scale.publication import _extract_custom_fields as persisted_fact_fields
from backend.high_scale.recovery import rebuild_security_projections_from_fos, replay_security_projections
from backend.high_scale.source_discovery import SourceObjectDescriptor
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore

NOW = datetime(2026, 9, 11, 20, 0, tzinfo=UTC)
_PG_TARGET = "backend.core.metadata.pg_connection.get_pg_readonly_connection"
_BOT_ROW = {"waf_req_id": "req-1", "bot_name": "bingbot", "category": "search", "wellknown_bot_name": "bing"}


class _Pg:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def execute(self, sql: str, params: list[str]) -> _Pg:
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        return None


class _ClickHouse:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def rows(self, domain: str) -> list[dict[str, Any]]:
        return [row for b in self.batches if b.domain == domain for row in b.rows]


class _Catalog:
    def __init__(self, *manifests: Any) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id: str, domain: str, start: datetime, end: datetime) -> tuple[Any, ...]:
        return self._manifests


def _event(waf_req_id: str | None) -> dict[str, Any]:
    return {"timestamp": "2026-09-11T20:00:00Z", "url": "/ok", **({"waf_req_id": waf_req_id} if waf_req_id else {})}


def test_enrichment_tags_events_whose_waf_req_id_is_in_the_bot_cache():
    events = (_event("req-1"), _event("req-unknown"), _event(None))
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _enrich_high_scale_ngwaf_bots(events)
    tagged, unknown, bare = events
    assert tagged["_ngwaf_bot_name"] == "bingbot"
    assert tagged["ngwaf_bot_verification_state"] == "verified"
    assert persisted_fact_fields(tagged) == {
        "ngwaf_bot": "bingbot",
        "ngwaf_bot_category": "search",
        "ngwaf_wellknown_bot": "bing",
    }
    for untouched in (unknown, bare):
        assert not {k for k in untouched if "ngwaf" in k}


def test_enrichment_never_invents_bots_for_synthetic_looking_ids():
    events = (_event("waf-123"),)
    with patch(_PG_TARGET, return_value=_Pg([])):
        _enrich_high_scale_ngwaf_bots(events)
    assert not {k for k in events[0] if "ngwaf" in k}


def test_enrichment_failure_is_logged_not_silent(caplog: pytest.LogCaptureFixture):
    events = (_event("req-1"),)
    with patch(_PG_TARGET, side_effect=RuntimeError("pg down")), caplog.at_level("WARNING"):
        _enrich_high_scale_ngwaf_bots(events)
    assert "NGWAF bot enrichment failed" in caplog.text
    assert "_ngwaf_bot_name" not in events[0]


def _controller() -> tuple[HighScaleIngestController, InMemoryObjectStore, _ClickHouse, _ClickHouse]:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    objects = InMemoryObjectStore()
    facts = _ClickHouse()
    aggregates = _ClickHouse()
    controller = HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(objects),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), facts),
        aggregates=ClickHousePublication(InMemoryBatchManifestStore(), aggregates),
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )
    return controller, objects, facts, aggregates


def _ingest(controller: HighScaleIngestController) -> Any:
    payload = gzip.compress((str(_event("req-1")).replace("'", '"') + "\n").encode())
    desc = SourceObjectDescriptor("raw/request/one.gz", "sha256:source", len(payload), "v1")
    return controller.ingest_batch(
        service_id="svc",
        domain="request",
        sources_payloads=[(desc, payload)],
        now=NOW,
    )


def test_ingest_persists_the_bot_on_the_fact_row_and_projects_it():
    controller, _, facts, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        _ingest(controller)

    fact = facts.rows("request")[0]
    assert persisted_fact_fields(fact)["ngwaf_bot"] == "bingbot"
    projected = aggregates.rows("security_dimensions")
    assert [(r["dimension"], r["value"], r["requests"], r["verified_count"]) for r in projected] == [
        ("ngwaf_bot", "bingbot", 1, 1)
    ]
    assert projected[0]["wellknown_bot_name"] == "bing"
    assert projected[0]["bot_category"] == "search"


def test_archive_stays_raw_so_a_rebuild_can_enrich_against_the_current_cache():
    controller, objects, _, aggregates = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    assert aggregates.rows("security_dimensions") == []

    archive = ArchivePublication(objects)
    rebuilt = _ClickHouse()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)
    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        replayed = replay_security_projections(result.manifest, archive, publication, rebuild_id="drill-1")

    assert (replayed.rows_read, replayed.rows_published) == (1, 1)
    assert [b.domain for b in rebuilt.batches] == ["security_dimensions"]
    assert rebuilt.batches[0].batch_id.startswith("rebuild-security:drill-1:")
    assert rebuilt.rows("security_dimensions")[0]["value"] == "bingbot"


def test_replay_publishes_nothing_when_no_event_resolves_to_a_bot():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
        replayed = replay_security_projections(
            result.manifest,
            ArchivePublication(objects),
            ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
            rebuild_id="drill-2",
        )
    assert (replayed.rows_read, replayed.rows_published) == (1, 0)


def test_rebuild_from_fos_replays_every_request_manifest_and_resumes_idempotently():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    archive = ArchivePublication(objects)
    rebuilt = _ClickHouse()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), rebuilt)

    with patch(_PG_TARGET, return_value=_Pg([_BOT_ROW])):
        first = rebuild_security_projections_from_fos(
            _Catalog(result.manifest), archive, publication, service_id="svc", now=NOW + timedelta(hours=1)
        )
        second = rebuild_security_projections_from_fos(
            _Catalog(result.manifest),
            archive,
            publication,
            service_id="svc",
            now=NOW + timedelta(hours=1),
            rebuild_id=first.rebuild_id,
        )

    assert first.full_rebuild_complete is True
    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 1
    assert len(rebuilt.batches) == 1


def test_rebuild_reports_a_failed_manifest_instead_of_hiding_it():
    controller, objects, _, _ = _controller()
    with patch(_PG_TARGET, return_value=_Pg([])):
        result = _ingest(controller)
    report = rebuild_security_projections_from_fos(
        _Catalog(result.manifest),
        ArchivePublication(InMemoryObjectStore()),
        ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
        service_id="svc",
        now=NOW + timedelta(hours=1),
    )
    assert report.full_rebuild_complete is False
    assert report.manifests_replayed == 0
    assert "failed to rebuild Security projections" in report.errors[0]


def test_rebuild_rejects_a_non_positive_window():
    with pytest.raises(ValueError, match="window must be positive"):
        rebuild_security_projections_from_fos(
            _Catalog(),
            ArchivePublication(InMemoryObjectStore()),
            ClickHousePublication(InMemoryBatchManifestStore(), _ClickHouse()),
            service_id="svc",
            window=timedelta(0),
        )
