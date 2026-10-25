"""Opt-in real ClickHouse gate for the high-scale readers that fake clients cannot validate.

RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_clickhouse_readers_integration.py

Uses an already-installed Docker image and disposable ``clickhouse local`` processes (no ports, no
deployed infrastructure). The production instrumented HTTP client runs against a local transport that
executes each statement on that engine over the canonical fact schemas and a fixed fixture, so dialect
mistakes (format specifiers, UNION ordering, quantile combinators) fail here instead of in production.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from backend.config import ClickHouseConfig
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import sessions as hs_sessions
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.cmcd import cmcd_aggregates, project_cmcd
from backend.high_scale.dashboard import aggregates as dashboard_aggregates
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.high_scale.origin import aggregates as origin_aggregates
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.sessions import sessions_endpoint
from backend.models.dashboard import AggregatesRequest, SessionsRequest
from backend.models.insights import CacheCollapseDetailRequest, InsightsRequest
from backend.models.performance import CmcdRequest
from backend.repositories.insights import registry

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)

SQL_ROOT = Path("backend/high_scale/sql")
SERVICE = "svc-it"
NOW = datetime.now(UTC).replace(microsecond=0)


def _schema() -> str:
    parts = []
    for name in ("publication_schema.sql", "request_schema.sql", "cmcd_schema.sql", "origin_schema.sql"):
        text = (SQL_ROOT / name).read_text()
        text = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", text
        )
        text = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", text)
        text = re.sub(r"\nTTL [^;]*;", ";", text)
        parts.append(text)
    return "\n".join(parts)


class _Fixture:
    def __init__(self) -> None:
        self.seed = _schema()

    def insert(self, table: str, domain: str, rows: list[dict[str, Any]]) -> None:
        """Insert fact/projection rows and publish every distinct batch as visible."""
        if not rows:
            return
        self.seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows) + "\n;\n"
        batches = {r["batch_id"] for r in rows}
        publications = [
            {
                "batch_id": batch,
                "service_id": SERVICE,
                "domain": domain,
                "generation": "1",
                "batch_digest": "fixture",
                "expected_rows": 1,
                "visible_rows": 1,
                "quorum_acked": 1,
                "publication_state": "visible",
                "manifest_version": 2,
                "updated_at": NOW.isoformat(),
            }
            for batch in batches
        ]
        self.seed += "\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
        self.seed += "\n".join(json.dumps(p) for p in publications) + "\n;\n"

    def request(
        self, when: datetime, url: str = "/ok", *, ip: str = "203.0.113.1", country: str = "US", **fields: Any
    ) -> dict[str, Any]:
        cmcd = {k[5:]: str(fields.pop(k)) for k in [k for k in fields if k.startswith("cmcd_")]}
        custom = {k: str(v) for k, v in fields.items()}
        return {
            "service_id": SERVICE,
            "event_id": str(uuid.uuid4()),
            "event_timestamp": when.isoformat(),
            "ingest_timestamp": when.isoformat(),
            "source_object_key": "raw/fixture.gz",
            "source_object_version": "v1",
            "line_ordinal": 0,
            "transform_version": "t1",
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "publication_state": "visible",
            "country": country,
            "client_ip": ip,
            "url": url,
            "custom_fields": custom,
            "cmcd": cmcd,
        }


def _make_client(seed: str) -> ClickHouseClient:
    def transport(request: httpx.Request) -> httpx.Response:
        form = BytesParser(policy=default).parsebytes(
            f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
        )
        fields: dict[str, str] = {}
        for part in form.iter_parts():
            name = part.get_param("name", header="content-disposition")
            payload = part.get_payload(decode=True)
            assert isinstance(name, str) and isinstance(payload, bytes)
            fields[name] = payload.decode()
        command = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--entrypoint",
            "clickhouse",
            os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13"),
            "local",
            "--multiquery",
            "--format=JSON",
            "--date_time_input_format=best_effort",
            "--output_format_json_quote_64bit_integers=0",
        ]
        command += [f"--{name}={value}" for name, value in fields.items() if name.startswith("param_")]
        # Per-query settings travel as URL settings (see ClickHouseClient.execute(settings=...)).
        for name in ("additional_table_filters", "max_result_rows", "result_overflow_mode"):
            if name in request.url.params:
                command.append(f"--{name}={request.url.params[name]}")
        completed = subprocess.run(
            command, input=seed + "\n" + fields["query"] + ";", text=True, capture_output=True, timeout=120
        )
        if completed.returncode:
            return httpx.Response(500, content=completed.stderr.encode())
        return httpx.Response(200, content=completed.stdout.encode())

    settings = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
    return ClickHouseClient(settings, transport=httpx.MockTransport(transport))


@pytest.fixture
def fixture() -> Iterator[_Fixture]:
    yield _Fixture()


@pytest.fixture
def service(fixture: _Fixture) -> Iterator[SimpleNamespace]:
    client = _make_client(fixture.seed)
    yield SimpleNamespace(service_id=SERVICE, client=client)
    client.close()


def test_unfiltered_dashboard_counts_exact_subminute_event_window(fixture: _Fixture) -> None:
    start = datetime(2026, 1, 1, 0, 0, 17, 125000, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    rows = [
        fixture.request(start - timedelta(milliseconds=1), "/outside"),
        fixture.request(start, "/kept"),
        fixture.request(start + timedelta(seconds=1), "/kept"),
        fixture.request(end - timedelta(milliseconds=1), "/kept"),
        fixture.request(end, "/outside"),
        fixture.request(end + timedelta(seconds=1), "/outside"),
        {**fixture.request(start, "/other"), "service_id": "other-svc"},
    ]
    fixture.insert("request_facts", "request", rows)
    client = _make_client(fixture.seed)
    try:
        service = HighScaleService(
            service_id=SERVICE,
            client=client,
            cursor_secret=b"fixture",
            request_watermark=ServingWatermark(
                service_id=SERVICE,
                domain="request",
                owner_epoch=1,
                coverage_start=start,
                coverage_end=end,
                last_accepted_cursor=None,
                last_archived_event_id=None,
                last_visible_event_id=None,
                exact=True,
            ),
        )
        response = dashboard_aggregates(
            service,
            AggregatesRequest(fields=["url"], include_time_series=False, include_map_data=False),
            start.isoformat(),
            end.isoformat(),
        )
    finally:
        client.close()

    assert response.total_rows == 3
    assert response.data["url"].total == 3
    assert [(row.value, row.count) for row in response.data["url"].top] == [("/kept", 3)]


def _spread(start: datetime, end: datetime, count: int) -> list[datetime]:
    step = (end - start) / max(count, 1)
    return [start + step * i for i in range(count)]


@pytest.fixture
def traffic(fixture: _Fixture) -> _Fixture:
    """Twelve hours of traffic: ten baseline hours, a two-hour window, and one planted anomaly per detection."""
    # NOW is fixed at import while the readers use the wall clock at call time, so both ranges keep a
    # 20-minute guard from the baseline/window boundary and from the baseline start.
    baseline = (NOW - timedelta(hours=11, minutes=40), NOW - timedelta(hours=2, minutes=20))
    window = (NOW - timedelta(hours=1, minutes=40), NOW - timedelta(minutes=1))
    rows: list[dict[str, Any]] = []
    add = rows.append

    def burst(start_end: tuple[datetime, datetime], count: int, url: str = "/ok", **kw: Any) -> None:
        for moment in _spread(*start_end, count):
            add(fixture.request(moment, url, **kw))

    burst(baseline, 400, status=200, cache="HIT", elapsed=100000, ua="Mozilla/5.0", ost=200, ottfb=50000, method="GET")
    burst(window, 100, status=200, cache="HIT", elapsed=100000, ua="Mozilla/5.0", ost=200, ottfb=50000, method="GET")
    burst(baseline, 20, "/err", status=200, cache="HIT", elapsed=100000)
    burst(window, 4, "/err", status=200, cache="HIT", elapsed=100000)
    burst(window, 6, "/err", status=500, cache="MISS", elapsed=100000)
    burst(window, 6, country="NZ", status=200, cache="HIT", elapsed=100000)
    burst(baseline, 36, "/collapse", status=200, cache="HIT")
    burst(baseline, 4, "/collapse", status=200, cache="MISS")
    burst(window, 2, "/collapse", status=200, cache="HIT")
    burst(window, 18, "/collapse", status=200, cache="MISS")
    burst(baseline, 55, "/passflip", status=200, cache="HIT")
    burst(baseline, 5, "/passflip", status=200, cache="PASS")
    burst(window, 2, "/passflip", status=200, cache="HIT")
    burst(window, 18, "/passflip", status=200, cache="PASS")
    burst(baseline, 30, "/slow", status=200, cache="HIT", elapsed=100000)
    burst(window, 10, "/slow", status=200, cache="HIT", elapsed=900000)
    burst(window, 36, "/tail", status=200, cache="HIT", elapsed=10000)
    burst(window, 4, "/tail", status=200, cache="HIT", elapsed=500000)
    burst(window, 300, "/bot", ua="evil-bot/1.0", status=200, cache="HIT", elapsed=100000, asn=64500, method="POST")
    burst(window, 30, "/new-asn", asn=64501, status=200, cache="HIT", elapsed=100000)
    for index in range(30):
        add(fixture.request(window[0] + timedelta(minutes=index), f"/nf/{index}", ip="198.51.100.7", status=404))
    for index in range(25):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index),
                "/login",
                ip="198.51.100.8",
                cookie_session=f"s{index}",
                status=200,
            )
        )
    for index in range(8):
        add(fixture.request(window[0] + timedelta(minutes=index), "/fp", ip=f"192.0.2.{index + 1}", ja4="t13d-shared"))
    burst(window, 10, "/waf", waf_sig="XSS, SQLI", status=403)
    burst(window, 40, "/city", city="Springfield", region="IL", status=200, cache="HIT", elapsed=100000)
    for index in range(30):
        add(fixture.request(window[0] + timedelta(minutes=index), "/login", ip="198.51.100.9", status=401))
    for index, path in enumerate(("/admin", "/.env", "/wp-login", "/admin", "/.env", "/wp-login")):
        add(fixture.request(baseline[0] + timedelta(minutes=30 * index), path, ip="198.51.100.10", status=404))
    for index in range(60):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index % 30, seconds=index),
                f"/a/b/c{index % 12}",
                ip="198.51.100.11",
                status=200,
            )
        )
    for index in range(40):
        add(
            fixture.request(
                window[0] + timedelta(seconds=60 * index),
                "/poll",
                ip="198.51.100.12",
                ua="python-requests/2.31",
                status=200,
            )
        )
    for index in range(3):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index),
                "/spoof",
                ip="198.51.100.13",
                ja4="t13d-spoofed",
                pop="SJC",
                lat=-33.9,
                lon=151.2,
                tcp_rtt=1000,
                edge="1",
                status=200,
            )
        )
    # Newest rows carry every log field so each detection's required fields are present in the field sample.
    full = dict(
        status=200, ost=200, cache="HIT", elapsed=120000, ottfb=60000, oconnect_ms=30, oretries=1, oip="192.0.2.99",
        ua="Mozilla/5.0", referer="https://example.com/", method="GET", p_type="residential", c_type="broadband",
        c_speed="fast", transport="quic", pop="SJC", server_region="us-west", ja3="j3", ja4="j4", tls_ciphers_sha="abc",
        cookie_session="cs", asn=64510, metro=807, city="San Jose", region="CA", delivery_rate=1000000, resp_bytes=2048,
        req_header_bytes=400, conn_requests=3, tcp_rtt=20000, lat=37.3, lon=-121.9, ploss=0.0, rtt_var=1000, retrans=0,
        ttl=3600, hits=1, age=10, edge="1", rid="r1", prid="", resp_header_content_encoding="gzip", waf_sig="XSS",
    )  # fmt: skip
    for index in range(5):
        add(fixture.request(NOW - timedelta(seconds=30 + index), "/all-fields", **full))
    fixture.insert("request_facts", "request", rows)
    return fixture


def _insight_cards(service: SimpleNamespace) -> dict[str, dict[str, Any]]:
    response = insights_endpoint(service, InsightsRequest(window_size_hrs=2, baseline_hours=10), None, None, False)  # type: ignore[arg-type]
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


@pytest.mark.timeout(1800)
def test_every_ported_detection_executes_and_planted_anomalies_are_found(
    traffic: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )
    cards = _insight_cards(service)

    errors = {card_id: card["summary"] for card_id, card in cards.items() if card["severity"] == "error"}
    assert errors == {}
    fired = {card_id for card_id, card in cards.items() if card["items"]}
    assert {
        "error_spikes",
        "new_country_traffic",
        "cache_collapse",
        "cacheability_regression",
        "latency_regression",
        "tail_latency",
        "ua_monoculture",
        "asn_concentration",
        "new_asn_traffic",
        "content_discovery",
        "method_drift",
        "session_harvesting",
        "botnet_grouping",
        "waf_signal_spikes",
        "city_surges",
        "new_city_traffic",
        "credential_enumeration",
        "low_and_slow",
        "orphaned_deep_crawl",
        "repeated_patterns",
        "impossible_distance",
    } <= fired
    assert set(cards) == {definition.id for definition in registry.get_all()}
    spike = cards["error_spikes"]["items"][0]
    assert spike["label"] == "/err" and spike["meta"]["errors"] == 6
    assert cards["new_country_traffic"]["items"][0]["meta"]["filters"] == {"country": "NZ"}
    assert cards["cache_collapse"]["items"][0]["label"] == "/collapse"
    assert cards["session_harvesting"]["items"][0]["current_val"] == 25
    assert {item["label"] for item in cards["waf_signal_spikes"]["items"]} >= {"XSS", "SQLI"}
    assert cards["city_surges"]["items"][0]["label"].startswith("Springfield")
    assert cards["credential_enumeration"]["items"][0]["current_val"] == 30
    poller = next(item for item in cards["repeated_patterns"]["items"] if item["label"] == "198.51.100.12")
    assert poller["meta"]["mode_gap_s"] == 60 and poller["meta"]["n_events"] == 40 and poller["meta"]["score"] >= 90


def test_cache_collapse_detail_reads_real_rows(traffic: _Fixture, service: SimpleNamespace) -> None:
    req = CacheCollapseDetailRequest(url="/collapse", window_size_hrs=2, baseline_hours=10)

    result = cache_collapse_detail_endpoint(service, req, None, None, False)  # type: ignore[arg-type]

    assert result.has_data is True
    assert (result.breakdown.hits, result.breakdown.misses) == (2, 18)
    assert result.window_hit_rate == pytest.approx(10.0)
    assert result.baseline_hit_rate == pytest.approx(90.0)
    assert result.timeline and result.recent_misses
    assert all(point.bucket.endswith("Z") for point in result.timeline)


def test_sessions_group_stitch_count_and_flag_availability(
    traffic: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hs_sessions, "_asn_label_map", lambda service_id, asns: {a: f"AS{a}" for a in asns})
    start, end = (NOW - timedelta(hours=3)).isoformat(), NOW.isoformat()

    result = sessions_endpoint(
        service,  # type: ignore[arg-type]
        SessionsRequest(limit=2, min_reqs_flag=20, sort_by="req_count", sort_dir="desc"),
        start,
        end,
    )

    assert result["total"] >= 3 and len(result["sessions"]) == 2
    counts = [session["req_count"] for session in result["sessions"]]
    assert counts == sorted(counts, reverse=True)
    # Only the all-fields rows carry tcp_rtt/ja4; nothing in this fixture carries CMCD.
    assert result["has_rtt"] is True and result["has_ja4"] is True and result["has_cmcd"] is False
    past_the_end = sessions_endpoint(service, SessionsRequest(limit=2, page=99), start, end)  # type: ignore[arg-type]
    assert past_the_end["sessions"] == [] and past_the_end["total"] == result["total"]


def test_cmcd_buckets_line_up_with_the_padded_axis(
    fixture: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda service_id, asns: {})
    events = []
    for index in range(40):
        moment = NOW - timedelta(minutes=30) + timedelta(seconds=index * 30)
        event = {
            "event_id": str(uuid.uuid4()),
            "service_id": SERVICE,
            "timestamp": moment.isoformat(),
            "country": "US" if index % 2 else "DE",
            "asn": 7922,
            "cmcd": {
                "sid": f"s{index % 4}",
                "br": "2000",
                "bl": "9000",
                "mtp": "9000",
                "bs": "true" if index % 5 == 0 else "false",
                "ot": "v",
                "cid": "movie",
            },
        }
        projection = project_cmcd(event)
        assert projection is not None
        events.append(
            {
                "service_id": SERVICE,
                "projection_key": projection["projection_key"],
                "request_event_id": event["event_id"],
                "event_timestamp": moment.isoformat(),
                "batch_id": "22222222-2222-4222-8222-222222222222",
                "publication_state": "visible",
                "cmcd": projection["fields"],
                "country": projection["country"],
                "asn": projection["asn"],
            }
        )
    fixture.insert("cmcd_projection_facts", "cmcd", events)
    seeded_client = _make_client(fixture.seed)
    try:
        result = cmcd_aggregates(
            SimpleNamespace(service_id=SERVICE, client=seeded_client),  # type: ignore[arg-type]
            CmcdRequest(bucket_seconds=300, top_n=5),
            (NOW - timedelta(minutes=40)).isoformat(),
            NOW.isoformat(),
            None,
            False,
        )
    finally:
        seeded_client.close()

    assert result["has_data"] is True and result["overview"]["active_sessions"] == 4
    populated = [point for point in result["sessions_ts"] if point["concurrent_sessions"]]
    assert populated and sum(point["new_sessions"] for point in populated) == 4
    assert {row["country"] for row in result["rebuffer_by_country"]} == {"US", "DE"}
    assert result["rebuffer_by_asn"][0]["asn"] == 7922
    assert result["top_content"][0]["content_id"] == "movie"


def test_origin_sketches_merge_to_near_exact_percentiles(fixture: _Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    import random

    rng = random.Random(9)
    events = []
    values: list[float] = []
    base = (NOW - timedelta(hours=1)).replace(second=0)
    for minute in range(40):
        for _ in range(60):
            latency = rng.lognormvariate(8, 1.0) * (1 + minute / 20)
            values.append(latency)
            events.append(
                {
                    "timestamp": (base + timedelta(minutes=minute, seconds=rng.randint(0, 59))).isoformat(),
                    "ottfb": latency,
                    "ost": 200,
                    "url": rng.choice(["/a", "/b"]),
                    "pop": "SJC",
                }
            )
    projection = build_origin_projection_rows(tuple(events))
    fixture.insert(
        "origin_minute_summary",
        "origin_summary",
        [
            {
                "service_id": SERVICE,
                "batch_id": "33333333-3333-4333-8333-333333333333",
                "publication_state": "visible",
                **row,
            }
            for row in projection.summary_rows
        ],
    )
    fixture.insert(
        "origin_minute_dimensions",
        "origin_dimensions",
        [
            {
                "service_id": SERVICE,
                "batch_id": "44444444-4444-4444-8444-444444444444",
                "publication_state": "visible",
                **row,
            }
            for row in projection.dimension_rows
        ],
    )
    client = _make_client(fixture.seed)
    try:
        request = SimpleNamespace(
            filters={},
            bucket_minutes=10,
            split_by_leg=False,
            timeseries_metric="ttfb",
            timeseries_percentile="p95",
            slow_urls_limit=5,
            slow_urls_min_requests=1,
            ip_health_limit=5,
            pop_latency_limit=5,
        )
        result = origin_aggregates(
            SimpleNamespace(service_id=SERVICE, client=client),  # type: ignore[arg-type]
            request,
            (NOW - timedelta(hours=2)).isoformat(),
            NOW.isoformat(),
            sections={"summary", "timeseries", "slow_urls", "pop_latency"},
        )
    finally:
        client.close()

    import math

    ordered = sorted(values)
    exact_p95 = ordered[math.ceil(0.95 * len(ordered)) - 1] / 1000.0
    assert result["summary"]["ottfb_p95_ms"] == pytest.approx(exact_p95, rel=0.015)
    assert result["summary"]["_approx"] is True and "histogram sketch" in result["summary"]["_approx_note"]
    assert result["timeseries"]["series"] and all(point["value"] for point in result["timeseries"]["series"])
    assert {row["url"] for row in result["slow_urls"]["rows"]} == {"/a", "/b"}
    assert result["pop_latency"]["rows"][0]["pop"] == "SJC"


def _origin_request(filters: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        filters=filters,
        bucket_minutes=60,
        split_by_leg=False,
        timeseries_metric="ttfb",
        timeseries_percentile="p95",
        slow_urls_limit=5,
        slow_urls_min_requests=1,
        ip_health_limit=5,
        pop_latency_limit=5,
    )


_ORIGIN_SECTIONS = {"summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"}


def _filtered_origin(service: SimpleNamespace, filters: dict[str, Any]) -> dict[str, Any]:
    return origin_aggregates(
        service,
        _origin_request(filters),
        (NOW - timedelta(hours=12)).isoformat(),
        (NOW + timedelta(minutes=1)).isoformat(),
        sections=_ORIGIN_SECTIONS,
    )


def test_filtered_origin_is_computed_exactly_from_request_facts(traffic: _Fixture, service: SimpleNamespace) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["GET"])},
    )

    summary = result["summary"]
    assert summary["has_data"] is True and summary["_approx"] is False
    assert summary["total_misses"] == 0 and summary["total_passes"] == 0
    assert summary["ottfb_p50_ms"] == summary["ottfb_p95_ms"] == 50.0
    assert summary["origin_error_rate"] == 0.0
    assert summary["ottlb_p50_ms"] is None and summary["obytes_p50"] is None
    assert result["status_codes"]["rows"] == [{"status": 200, "count": 500, "pct": 100.0}]
    assert result["slow_urls"]["rows"] == [
        {"url": "/ok", "requests": 500, "p50_ms": 50.0, "p95_ms": 50.0, "p99_ms": 50.0}
    ]
    assert result["timeseries"]["has_data"] and all(p["value"] == 50.0 for p in result["timeseries"]["series"])
    assert sum(p["miss_count"] for p in result["timeseries"]["series"]) == 0
    assert result["path_breakdown"]["has_data"] is False


def test_filtered_origin_with_no_matching_rows_reports_no_data_without_error(
    traffic: _Fixture, service: SimpleNamespace
) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["POST"])},
    )

    assert result["has_data"] is False
    assert result["summary"]["has_data"] is False
    assert result["slow_urls"]["rows"] == [] and result["status_codes"]["rows"] == []
    assert result["timeseries"]["series"] == []


def test_filtered_origin_exclude_mode_and_oip_health(traffic: _Fixture, service: SimpleNamespace) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(service, {"country": FilterSpec(mode="exclude", values=["NZ"])})

    assert result["summary"]["has_data"] is True
    assert {row["url"] for row in result["slow_urls"]["rows"]} >= {"/ok"}
    assert 200 in {row["status"] for row in result["status_codes"]["rows"]}


@pytest.fixture
def two_tenants(traffic: _Fixture) -> _Fixture:
    """``traffic`` plus a second service's rows, so a scope escape is observable as extra rows."""
    rows = []
    for index in range(50):
        row = traffic.request(NOW - timedelta(minutes=90, seconds=index), "/other-tenant", status=200)
        row["service_id"] = "other-svc"
        row["batch_id"] = "22222222-2222-4222-8222-222222222222"
        rows.append(row)
    traffic.insert("request_facts", "request", rows)
    return traffic


def _console(service: SimpleNamespace, sql: str, *, window_minutes: int = 30) -> list[dict[str, Any]]:
    from backend.high_scale.query import query_endpoint
    from backend.models.dashboard import QueryRequest

    result = query_endpoint(
        service,  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=100),
        (NOW - timedelta(minutes=window_minutes)).isoformat(),
        NOW.isoformat(),
        False,
    )
    return result["data"]


@pytest.mark.parametrize(
    "where",
    ["1=1 OR url = '/nope'", "country = 'US' OR 1 = 1", "url = '/ok' OR url != '/ok'"],
    ids=["tautology", "or-country", "complement"],
)
def test_sql_console_cannot_widen_past_the_service_and_window_with_an_or_condition(
    two_tenants: _Fixture, service: SimpleNamespace, where: str
) -> None:
    scoped = _console(service, "SELECT count(*) AS n FROM logs")[0]["n"]

    attacked = _console(service, f"SELECT count(*) AS n FROM logs WHERE {where}")[0]["n"]

    assert 0 < scoped < 300, "fixture sanity: the 30 minute window holds only part of one tenant's rows"
    assert attacked == scoped, "a user OR must not escape the injected service and time-window predicates"
    assert _console(service, "SELECT count(*) AS n FROM logs WHERE url = '/other-tenant'")[0]["n"] == 0


@pytest.mark.parametrize(
    "tail",
    ["-- swallow everything after this", "/* c */", "/* /* */ UNION SELECT 1 FROM system.tables\n-- */ */"],
)
def test_sql_console_rejects_comments_so_a_trailing_one_cannot_swallow_scope_or_row_cap(
    two_tenants: _Fixture, service: SimpleNamespace, tail: str
) -> None:
    with pytest.raises(PermissionError):
        _console(service, f"SELECT url FROM logs {tail}")


def test_sql_console_row_cap_holds_server_side_for_a_huge_explicit_limit(
    two_tenants: _Fixture, service: SimpleNamespace
) -> None:
    from backend.repositories.query import MAX_QUERY_ROWS

    rows = _console(service, "SELECT url FROM logs LIMIT 999999999", window_minutes=60 * 12)

    assert 0 < len(rows) <= 100  # QueryRequest(max_rows=100) truncates after the bounded fetch
    assert MAX_QUERY_ROWS >= len(rows)
