"""High-scale Insights: ClickHouse detections feeding the standard processors and cards."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import insights as hs_insights
from backend.high_scale.insight_queries import COLUMNS, DETECTIONS, botnet_grouping
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.models.insights import CacheCollapseDetailRequest, InsightsRequest
from backend.repositories.insights.registry import registry

FINGERPRINT_DETECTIONS = {"botnet_grouping", "repeated_patterns_fp", "impossible_distance"}
EVERY_CARD = set(DETECTIONS) | FINGERPRINT_DETECTIONS
ALL_FIELDS = sorted({name for detection in DETECTIONS.values() for name in detection.columns} - {"signal"}) + [
    "waf_sig",
    "ja4",
    "oconnect_ms",
    "edge",
    "lat",
    "lon",
]


@pytest.fixture(autouse=True)
def _pop_coordinates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )


class _Client:
    """Answers each ``/* insight:<id> */`` query with canned dict rows (column order = processor row schema)."""

    def __init__(
        self,
        rows: dict[str, list[dict[str, Any]]] | None = None,
        *,
        history_hours: float = 400.0,
        fields: list[str] | None = None,
        fail: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._rows = rows or {}
        self._history_hours = history_hours
        self._fields = ALL_FIELDS if fields is None else fields
        self._fail = fail or set()

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        self.calls.append((sql, params))
        marker = re.search(r"/\* insight:(\w+) \*/", sql)
        assert marker, sql
        name = marker.group(1)
        if name in self._fail:
            raise RuntimeError("clickhouse unavailable")
        if name == "history":
            earliest = params["window_end"] - timedelta(hours=self._history_hours)
            return [{"earliest": earliest, "window_total": 1234}]
        if name == "fields":
            return [{"fields": self._fields}]
        return self._rows.get(name, [])

    def queries(self) -> dict[str, tuple[str, dict]]:
        found: dict[str, tuple[str, dict]] = {}
        for sql, params in self.calls:
            found[re.search(r"/\* insight:(\w+) \*/", sql).group(1)] = (sql, params)  # type: ignore[union-attr]
        return found


def _run(client: _Client, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0, **clamp: Any):
    service = SimpleNamespace(service_id="svc", client=client)
    req = InsightsRequest(window_size_hrs=window, baseline_hours=baseline)
    return insights_endpoint(service, req, clamp.get("clamp_start"), clamp.get("clamp_end"), mask_ips)  # type: ignore[arg-type]


def _cards(response) -> dict[str, dict]:
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


def test_every_detection_has_a_registered_definition_with_a_processor_and_known_columns() -> None:
    registered = {definition.id: definition for definition in registry.get_all()}
    for insight_id, detection in DETECTIONS.items():
        assert insight_id in registered, insight_id
        assert registered[insight_id].row_processor is not None
        assert set(detection.columns) <= set(COLUMNS), insight_id
    assert botnet_grouping("ja3").columns == ("ip", "ja3")
    with pytest.raises(ValueError, match="ja4 or ja3"):
        botnet_grouping("x; DROP TABLE y")


def test_detections_surface_as_standard_cards_with_investigate_links() -> None:
    client = _Client(
        {
            "error_spikes": [
                {"url": "/api", "w_rate": 0.62, "b_rate": 0.01, "w_errors": 31, "w_total": 50, "b_total": 400},
                {"url": "/b", "w_rate": 0.10, "b_rate": 0.0, "w_errors": 5, "w_total": 50, "b_total": 400},
            ]
        }
    )

    cards = _cards(_run(client))

    card = cards["error_spikes"]
    assert card["severity"] == "critical"
    assert card["summary"] == "2 URLs with elevated server error rates"
    assert card["category"] == "origin"
    first = card["items"][0]
    assert first["label"] == "/api" and first["unit"] == "% 5xx"
    assert first["current_val"] == pytest.approx(62.0) and first["baseline_val"] == pytest.approx(1.0)
    assert first["meta"]["errors"] == 31 and first["meta"]["filters"] == {"url": "/api"}
    assert first["investigate_url"].startswith("/dashboard?service=svc&start=")
    assert "filter_url=%2Fapi" in first["investigate_url"]
    assert cards["latency_regression"]["severity"] == "clean"
    assert cards["latency_regression"]["summary"] == "No latency regression detected"


def test_every_registered_detection_runs_and_the_response_validates() -> None:
    client = _Client()

    response = _run(client)

    cards = _cards(response)
    assert set(cards) == EVERY_CARD == {definition.id for definition in registry.get_all()}
    assert all(card["severity"] == "clean" for card in cards.values())
    assert json.loads(response.model_dump_json(by_alias=True))["window_hours"] == 24.0


def test_every_detection_query_is_visible_scoped_and_window_bounded() -> None:
    client = _Client()

    _run(client)

    queries = client.queries()
    for insight_id in EVERY_CARD:
        sql, params = queries[insight_id]
        assert "FROM request_facts" in sql
        assert "service_id={service_id:String}" in sql and "publication_state='visible'" in sql
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert "event_timestamp <= {window_end:DateTime64(3)}" in sql
        window_only = insight_id in DETECTIONS and DETECTIONS[insight_id].window_only
        window_only = window_only or insight_id in {"repeated_patterns_fp", "impossible_distance"}
        lower = "{window_start:DateTime64(3)}" if window_only else "{baseline_start:DateTime64(3)}"
        if insight_id != "botnet_grouping":
            assert f"event_timestamp >= {lower}" in sql, insight_id
        assert params["service_id"] == "svc"
        assert params["baseline_start"] < params["window_start"] < params["window_end"]


def test_insufficient_history_is_reported_not_computed() -> None:
    client = _Client(history_hours=5.0)

    cards = _cards(_run(client))

    assert set(cards) == EVERY_CARD
    for insight_id, card in cards.items():
        if insight_id == "impossible_distance":
            # The standard path runs this one without a baseline, so history never gates it.
            assert card["severity"] == "clean"
            continue
        assert card["severity"] == "info"
        assert card["summary"] == "Requires 168h of historical data (only 5.0h available)"
        assert card["items"] == []
    assert {re.search(r"insight:(\w+)", sql).group(1) for sql, _ in client.calls} == {  # type: ignore[union-attr]
        "history",
        "fields",
        "impossible_distance",
    }


def test_detections_whose_log_fields_are_never_emitted_are_omitted_like_the_standard_path() -> None:
    fields = [name for name in ALL_FIELDS if name not in {"elapsed", "ottfb"}]
    client = _Client(fields=fields)

    cards = _cards(_run(client))

    for omitted in ("latency_regression", "tail_latency", "pop_latency_regression", "origin_latency_spike"):
        assert omitted not in cards
        assert omitted not in client.queries()
    assert "error_spikes" in cards


def test_fingerprint_detections_are_omitted_when_no_tls_fingerprint_is_logged() -> None:
    client = _Client(fields=[name for name in ALL_FIELDS if name != "ja4"])

    cards = _cards(_run(client))

    assert not (FINGERPRINT_DETECTIONS & set(cards))
    assert not (FINGERPRINT_DETECTIONS & set(client.queries()))


def test_impossible_distance_skips_pop_codes_that_are_not_plain_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        lambda: {"SJC": (37.36, -121.93), "SJC'; DROP TABLE x": (1.0, 2.0)},
    )
    client = _Client()

    _run(client)

    sql = client.queries()["impossible_distance"][0]
    assert "'SJC'" in sql and "DROP TABLE" not in sql


def test_impossible_distance_is_omitted_without_pop_coordinates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.utils.pop_utils.get_pop_lat_lon_map", dict)

    assert "impossible_distance" not in _cards(_run(_Client()))


def test_botnet_grouping_keys_on_ja3_when_ja4_is_not_logged() -> None:
    with_ja4 = _Client()
    _run(with_ja4)
    assert "ja4 != ''" in with_ja4.queries()["botnet_grouping"][0]

    without = _Client(fields=[name for name in ALL_FIELDS if name != "ja4"] + ["ja3"])
    _run(without)
    assert "ja3 != ''" in without.queries()["botnet_grouping"][0]


def test_a_failing_detection_is_an_error_card_and_does_not_hide_the_others() -> None:
    client = _Client(
        {"error_spikes": [{"url": "/x", "w_rate": 0.9, "b_rate": 0.0, "w_errors": 9, "w_total": 10, "b_total": 99}]},
        fail={"latency_regression"},
    )

    cards = _cards(_run(client))

    assert cards["latency_regression"]["severity"] == "error"
    assert cards["latency_regression"]["summary"] == "Query failed: RuntimeError"
    assert cards["latency_regression"]["items"] == []
    assert cards["error_spikes"]["severity"] == "critical"


def test_history_probe_failures_propagate() -> None:
    with pytest.raises(RuntimeError, match="unavailable"):
        _run(_Client(fail={"history"}))


def test_analyst_ip_masking_applies_to_ip_keyed_detections() -> None:
    rows = {
        "request_size_anomaly": [
            {"ip": "203.0.113.77", "max_bytes": 9000, "avg_bytes": 4000.0, "w_total": 8, "b_p95": 1000.0}
        ]
    }

    plain = _cards(_run(_Client(rows)))["request_size_anomaly"]["items"][0]
    masked = _cards(_run(_Client(rows), mask_ips=True))["request_size_anomaly"]["items"][0]

    assert "203.0.113.77" in json.dumps(plain)
    assert "203.0.113.77" not in json.dumps(masked)


def test_asn_insights_resolve_isp_names(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, list]] = []

    def fake_names(service_id: str, asns: list) -> dict:
        seen.append((service_id, list(asns)))
        return {7922: "Comcast"}

    monkeypatch.setattr("backend.core.duckdb.get_asn_names", fake_names)
    client = _Client({"asn_concentration": [{"asn": 7922, "w_cnt": 90, "b_cnt": 5, "b_total": 1000, "w_total": 120}]})

    card = _cards(_run(client))["asn_concentration"]

    assert seen == [("svc", [7922])]
    assert card["items"][0]["label"] == "AS7922 (Comcast)"
    assert card["severity"] == "critical"


def test_clamp_bounds_the_scanned_range_to_the_analyst_window() -> None:
    now = datetime.now(UTC)
    client = _Client()

    _run(client, clamp_start=(now - timedelta(hours=30)).isoformat(), clamp_end=(now - timedelta(hours=2)).isoformat())

    _, params = client.queries()["history"]
    assert params["window_end"] <= now - timedelta(hours=2) + timedelta(seconds=1)
    assert params["baseline_start"] >= now - timedelta(hours=30) - timedelta(seconds=1)
    assert params["window_start"] >= params["baseline_start"]


def test_window_parameters_are_honoured() -> None:
    client = _Client()

    _run(client, window=6.0, baseline=48.0)

    sql, params = client.queries()["error_spikes"]
    assert params["window_hours"] == 6.0 and params["baseline_hours"] == 48.0
    assert params["window_end"] - params["window_start"] == pytest.approx(timedelta(hours=6), abs=timedelta(seconds=1))
    assert params["window_start"] - params["baseline_start"] == pytest.approx(
        timedelta(hours=48), abs=timedelta(seconds=1)
    )


def test_sql_parameters_never_embed_user_values() -> None:
    client = _Client()

    _run(client)

    for sql, params in client.calls:
        assert "svc" not in sql
        assert re.findall(r"\{(\w+):", sql) and all(name in params for name in re.findall(r"\{(\w+):", sql))


# ── cache collapse detail ────────────────────────────────────────────────────────────


class _DetailClient:
    def __init__(self, *, empty: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._empty = empty

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "cache_collapse_rates" in sql:
            zeros = dict.fromkeys(("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total"), 0)
            if self._empty:
                return [zeros]
            return [
                {
                    **zeros,
                    "w_hits": 10,
                    "w_miss": 30,
                    "w_pass": 10,
                    "w_total": 55,
                    "b_hits": 90,
                    "b_miss": 10,
                    "b_pass": 5,
                    "b_total": 120,
                }
            ]
        if "cache_collapse_timeline" in sql:
            return (
                []
                if self._empty
                else [
                    {"bucket": "2026-09-14 10:00:00", "total_requests": 40, "real_hits": 10, "misses": 30},
                    {"bucket": datetime(2026, 9, 14, 11, tzinfo=UTC), "total_requests": 4, "real_hits": 0, "misses": 0},
                ]
            )
        if "cache_collapse_misses" in sql:
            return (
                []
                if self._empty
                else [
                    {
                        "timestamp": "2026-09-14 10:59:00.000",
                        "cache": "MISS",
                        "pop": "SJC",
                        "ip": "203.0.113.9",
                        "status": 200,
                    },
                    {
                        "timestamp": datetime(2026, 9, 14, 10, 58, tzinfo=UTC),
                        "cache": "",
                        "pop": "",
                        "ip": "",
                        "status": None,
                    },
                ]
            )
        raise AssertionError(sql)


def _detail(client: Any, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0):
    req = CacheCollapseDetailRequest(url="/hot", window_size_hrs=window, baseline_hours=baseline)
    service = SimpleNamespace(service_id="svc", client=client)
    return json.loads(
        cache_collapse_detail_endpoint(service, req, None, None, mask_ips).model_dump_json(by_alias=True)  # type: ignore[arg-type]
    )


def test_cache_collapse_detail_returns_the_real_timeline_breakdown_and_rates() -> None:
    result = _detail(_DetailClient())

    assert result["url"] == "/hot" and result["has_data"] is True
    assert result["breakdown"] == {"hits": 10, "misses": 30, "passes": 10, "other": 5}
    assert result["window_hit_rate"] == pytest.approx(25.0)
    assert result["baseline_hit_rate"] == pytest.approx(90.0)
    assert result["window_pass_rate"] == pytest.approx(10 / 55 * 100)
    assert result["baseline_pass_rate"] == pytest.approx(5 / 120 * 100)
    first = result["timeline"][0]
    assert first["bucket"] == "2026-09-14T10:00:00Z"
    assert first["expected_hits"] == pytest.approx(40 * 0.9) and first["hit_rate"] == pytest.approx(0.25)
    assert result["timeline"][1]["hit_rate"] == 0.0 and result["timeline"][1]["bucket"] == "2026-09-14T11:00:00Z"
    assert result["clusters"] == []


def test_cache_collapse_detail_recent_misses_are_real_and_maskable() -> None:
    plain = _detail(_DetailClient())["recent_misses"]
    masked = _detail(_DetailClient(), mask_ips=True)["recent_misses"]

    assert plain[0] == {
        "timestamp": "2026-09-14T10:59:00.000Z",
        "cache": "MISS",
        "pop": "SJC",
        "ip": "203.0.113.9",
        "status": 200,
    }
    assert plain[1]["cache"] == "UNKNOWN" and plain[1]["status"] is None and plain[1]["pop"] is None
    assert masked[0]["ip"] != "203.0.113.9"


def test_cache_collapse_detail_scopes_every_query_to_the_url_service_and_window() -> None:
    client = _DetailClient()

    _detail(client)

    assert len(client.calls) == 3
    for sql, params in client.calls:
        assert "url = {url:String}" in sql and params["url"] == "/hot"
        assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
        assert "event_timestamp >= {baseline_start:DateTime64(3)}" in sql
        assert params["service_id"] == "svc"


@pytest.mark.parametrize(
    ("window", "baseline", "interval"),
    [
        (1.0, 1.0, "INTERVAL 1 MINUTE"),
        (6.0, 6.0, "INTERVAL 5 MINUTE"),
        (24.0, 96.0, "INTERVAL 1 HOUR"),
        (24.0, 168.0, "INTERVAL 4 HOUR"),
    ],
)
def test_cache_collapse_detail_bucket_size_follows_the_lookback(window: float, baseline: float, interval: str) -> None:
    client = _DetailClient()

    _detail(client, window=window, baseline=baseline)

    timeline_sql = next(sql for sql, _ in client.calls if "cache_collapse_timeline" in sql)
    assert interval in timeline_sql


def test_cache_collapse_detail_for_an_unseen_url_has_no_data() -> None:
    result = _detail(_DetailClient(empty=True))

    assert result["has_data"] is False
    assert result["timeline"] == [] and result["recent_misses"] == []
    assert result["breakdown"] == {"hits": 0, "misses": 0, "passes": 0, "other": 0}


def test_module_exports_only_the_two_endpoints_the_routers_call() -> None:
    assert callable(hs_insights.insights_endpoint) and callable(hs_insights.cache_collapse_detail_endpoint)
