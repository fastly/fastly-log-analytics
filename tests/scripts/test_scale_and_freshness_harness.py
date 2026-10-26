from __future__ import annotations

"""RUM synthetic generators folded into scripts/lib/log_factory.py (S3),
measure_e2e_freshness, scale_harness, and verify_dashboard.js tests.
"""

import hashlib
import json
import random
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

from scripts.dev import measure_e2e_freshness as freshness
from scripts.dev import scale_harness
from scripts.dev.measure_e2e_freshness import (
    compute_lag_statistics,
    configured_polling,
    cron_completion_time,
    load_environment_config,
    new_terminal_run,
    rum_beacon_count,
    seconds_between,
)
from scripts.dev.scale_harness import (
    TrafficProfile,
    build_request,
    build_request_url,
    freshness_lag_seconds,
    parse_stages,
    render_report,
)
from scripts.lib import log_factory as lf
from scripts.lib.log_factory import BOT_USER_AGENTS, build_cmcd_request, build_cmcd_sessions
from scripts.lib.section_measurements import BUDGETS

NOW = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)

RECORD_KEYS = {
    "service_id",
    "timestamp",
    "geo_city",
    "geo_region",
    "geo_country_code",
    "server_pop",
    "browser",
    "os",
    "device",
    "rum_cid",
    "req_id",
    "fastly_req_id",
    "request_event_id",
    "url",
    "rum_body",
}


def _digest(obj: object) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def test_records_are_deterministic_for_a_seed_and_match_golden_digest() -> None:
    a = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    b = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    c = lf.generate_rum_records("svc", 200, NOW, random.Random(8))
    assert a == b and a != c
    assert _digest(a) == "884741d052c6a872939dce4c7b001739062f79dde8d457a4ec4e490d49555614"


def test_record_shape_matches_ingest_contract() -> None:
    records = lf.generate_rum_records("svc-1", 300, NOW, random.Random(1))
    assert len({r["timestamp"] for r in records}) == 300
    assert records[0]["timestamp"] == "2026-10-19T12:00:00.000Z"
    assert records[1]["timestamp"] == "2026-10-19T11:59:59.950Z"
    kinds: set[str] = set()
    for rec in records:
        assert set(rec) == RECORD_KEYS and rec["service_id"] == "svc-1"
        assert rec["req_id"].startswith("req-") and len(rec["req_id"]) == 4 + 16
        assert rec["fastly_req_id"] == rec["request_event_id"] == rec["req_id"]
        assert rec["rum_cid"].startswith("cid-") and rec["url"].startswith("http://localhost/")
        body = json.loads(rec["rum_body"])
        assert body["meta"]["page"]["url"] == rec["url"] and body["meta"]["os"]["name"] == rec["os"]
        assert body["meta"]["browser"] == {"name": rec["browser"], "mobile": rec["device"] == "Mobile"}
        (kind,) = set(body) - {"meta"}
        kinds.add(kind)
    assert kinds == {"measurements", "events", "exceptions"}
    assert len({r["req_id"] for r in records}) == 300


def test_vitals_ratings_follow_thresholds() -> None:
    profile = lf.FILE_RUM_PROFILE
    cases = [
        ("LCP", 2500, "good"),
        ("LCP", 4000, "needs_improvement"),
        ("LCP", 4001, "poor"),
        ("CLS", 0.1, "good"),
        ("CLS", 0.25, "needs_improvement"),
        ("CLS", 0.26, "poor"),
        ("INP", 200, "good"),
        ("INP", 500, "needs_improvement"),
        ("INP", 501, "poor"),
        ("TTFB", 800, "good"),
        ("TTFB", 801, "poor"),
        ("FCP", 100, "good"),
        ("FCP", 101, "poor"),
    ]
    for metric, value, rating in cases:
        assert lf.rum_rating(metric, value, profile) == rating
    assert lf.rum_rating("FID", 700, lf.HARNESS_RUM_PROFILE) == "good"


def test_harness_beacon_url_carries_vcl_identity_parameters_and_is_seeded() -> None:
    url = lf.build_rum_beacon_url("https://logs.example.com/", random.Random(3))
    assert url == lf.build_rum_beacon_url("https://logs.example.com", random.Random(3))
    parsed = urlparse(url)
    assert parsed.path == "/rum-beacon"
    qs = parse_qs(parsed.query)
    assert set(qs) == {"rum_metric_name", "rum_metric_value", "rum_cid", "cid", "req", "rum_pathname"}
    assert qs["cid"] == qs["rum_cid"]
    assert qs["req"][0] == "req-" + qs["cid"][0].removeprefix("cid-")
    assert len(qs["req"][0]) == 4 + 32


def test_harness_beacon_body_golden_and_shape() -> None:
    url = "https://h.example.com/rum-beacon?rum_metric_name=LCP&rum_metric_value=3500&rum_pathname=%2Fdashboard"
    bodies = [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert bodies == [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert _digest(bodies) == "4aaf0a1622b62558e7684d394595ce31be0232b4a2517ece00d8485dc184b2b4"
    seen = set()
    for body in bodies:
        assert body["meta"]["page"]["url"] == "https://h.example.com/dashboard"
        assert body["meta"]["browser"]["name"] in lf.HARNESS_RUM_PROFILE.browsers
        (kind,) = set(body) - {"meta"}
        seen.add(kind)
        if kind == "measurements":
            (m,) = body["measurements"]
            assert m["type"] == "web-vitals" and m["values"] == {"LCP": 3500.0}
            assert m["context"]["rating"] == "needs_improvement"
        if kind == "exceptions":
            assert body["exceptions"][0]["stacktrace"]["frames"][0]["filename"] == "static/chunks/main.js"
    assert seen == {"measurements", "events", "exceptions"}


def test_beacon_body_defaults_when_query_is_missing_or_bad() -> None:
    body = lf.build_rum_beacon_body("https://h.example.com/rum-beacon?rum_metric_value=abc", random.Random(0), NOW)
    assert body["meta"]["page"]["url"] == "https://h.example.com/"
    if "measurements" in body:
        assert body["measurements"][0]["values"] == {"LCP": 1200.0}


def test_source_ip_is_seeded_and_in_range() -> None:
    assert lf.random_source_ip(random.Random(5)) == lf.random_source_ip(random.Random(5))
    rng = random.Random(9)
    for _ in range(200):
        a, b, c, d = (int(x) for x in lf.random_source_ip(rng).split("."))
        assert 12 <= a <= 223 and all(1 <= x <= 254 for x in (b, c, d))


def test_scale_harness_build_request_url_uses_injected_rng() -> None:
    assert scale_harness.build_request_url("https://x.example.com", 0.0, random.Random(1)) == "https://x.example.com"
    seeded = [scale_harness.build_request_url("https://x.example.com", 1.0, random.Random(4)) for _ in range(2)]
    assert seeded[0] == seeded[1] and "/rum-beacon?" in seeded[0]
    rng = random.Random(11)
    beacons = sum(
        "/rum-beacon?" in scale_harness.build_request_url("https://x.example.com", 0.5, rng) for _ in range(200)
    )
    assert 60 < beacons < 140


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_global_random_state_is_untouched_when_rng_is_injected(seed: int) -> None:
    random.seed(1234)
    expected = random.random()
    random.seed(1234)
    lf.generate_rum_records("svc", 50, NOW, random.Random(seed))
    lf.build_rum_beacon_url("https://x.example.com", random.Random(seed))
    assert random.random() == expected


# ── Merged from test_measure_e2e_freshness.py ──────────────────────────────────────────────────────


def test_load_environment_config_reads_only_explicit_environment_values():
    config = load_environment_config(
        "local-std",
        {
            "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
            "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "http://127.0.0.1:8000",
            "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
        },
    )
    assert config == {
        "service_id": "test-service",
        "backend_url": "http://127.0.0.1:8000",
        "cdn_domain": "https://cdn.example.test",
        "name": "Local Standard",
    }


def test_load_environment_config_rejects_missing_and_invalid_urls():
    with pytest.raises(ValueError, match="FLA_FRESHNESS_REMOTE_HS_CDN_DOMAIN"):
        load_environment_config(
            "remote-hs",
            {
                "FLA_FRESHNESS_REMOTE_HS_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_REMOTE_HS_BACKEND_URL": "http://127.0.0.1:8000",
            },
        )
    with pytest.raises(ValueError, match="absolute HTTP"):
        load_environment_config(
            "local-std",
            {
                "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "file:///tmp/backend",
                "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
            },
        )


@pytest.mark.parametrize(
    ("service", "expected"),
    [
        (
            {"log_period": 60, "cron_sync": {"polling_mode": "adaptive", "interval_mins": 2}},
            {
                "mode": "adaptive",
                "interval_seconds": 120,
                "interval_source": "interval_mins",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 60, "cron_sync": {"interval_seconds": 0}},
            {
                "mode": "adaptive",
                "interval_seconds": 30,
                "interval_source": "log_period",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 1, "cron_sync": {"polling_mode": "regular", "interval_seconds": 2}},
            {
                "mode": "regular",
                "interval_seconds": 5,
                "interval_source": "interval_seconds",
                "adaptive_followup_interval_seconds": None,
                "adaptive_max_followups": 0,
            },
        ),
    ],
)
def test_configured_polling_reports_scheduler_interval_and_mode(service, expected):
    assert configured_polling({"services": [service | {"service_id": "svc"}]}, "svc") == expected


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "services response"),
        ({"services": []}, "requested service"),
        ({"services": [{"service_id": "svc", "cron_sync": {"polling_mode": "continuous"}}]}, "polling_mode"),
        ({"services": [{"service_id": "svc", "cron_sync": []}]}, "cron_sync"),
    ],
)
def test_configured_polling_rejects_missing_or_invalid_config(payload, message):
    with pytest.raises(ValueError, match=message):
        configured_polling(payload, "svc")


def test_cron_completion_time_and_new_terminal_run_and_seconds_between():
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": 2.5}) == datetime(
        2026, 9, 29, 12, 0, 2, 500000, tzinfo=UTC
    )
    assert cron_completion_time({"started_at": "invalid", "duration_s": 2}) is None
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": "invalid"}) is None
    assert new_terminal_run({"entries": [{"id": 3, "status": "warning"}]}, 2) == {"id": 3, "status": "warning"}
    for entry in (
        [{"id": 3, "status": "running"}],
        [{"id": 2, "status": "success"}],
        [{"id": "invalid", "status": "success"}],
        [],
    ):
        assert new_terminal_run({"entries": entry}, 2) is None
    later = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
    assert seconds_between(later, datetime(2026, 9, 29, 12, 0, 1, tzinfo=UTC)) == -1.0
    assert seconds_between(later, None) is None


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"beacons": 5}, 5),
        ({"recent_beacons": "8"}, 8),
        ({"beacons": None, "recent_beacons": 3}, 3),
        ({"beacons": "invalid"}, None),
        ("invalid", None),
    ],
)
def test_rum_beacon_count_supports_standard_and_high_scale_responses(response, expected):
    assert rum_beacon_count(response) == expected


def test_compute_lag_statistics_empty_and_percentiles_and_shared_budget(monkeypatch):
    empty = compute_lag_statistics([])
    assert empty["sample_count"] == 0 and empty["target_status"] == "UNMEASURED"
    assert empty["header_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
    assert empty["dashboard_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}

    pct = compute_lag_statistics(
        [{"elapsed_s": 6, "header_lag_s": lag, "dashboard_lag_s": lag} for lag in range(1, 21)]
    )
    assert pct["header_lag"]["p95"] == 19 and pct["header_lag"]["p99"] == 20
    assert pct["dashboard_lag"]["p95"] == 19 and pct["dashboard_lag"]["p99"] == 20

    monkeypatch.setitem(BUDGETS, "peak_freshness_p95_ms", 25000)
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": 25.0, "dashboard_lag_s": 25.0}])
    assert stats["target_p95_s"] == 25 and stats["target_status"] == "WITHIN TARGET"


@pytest.mark.parametrize(
    ("header_lag", "dashboard_lag", "expected"),
    [(30.2, 10.0, "ELEVATED"), (10.0, 30.2, "ELEVATED"), (20.0, 20.0, "WITHIN TARGET"), (20.04, 20.04, "ELEVATED")],
)
def test_compute_lag_statistics_assesses_both_p95s_not_the_mean(header_lag, dashboard_lag, expected):
    stats = compute_lag_statistics(
        [
            {"elapsed_s": 6, "header_lag_s": 1.0, "dashboard_lag_s": 1.0},
            {"elapsed_s": 8, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag},
        ]
    )
    assert stats["header_lag"]["avg"] < 20 and stats["dashboard_lag"]["avg"] < 20 and stats["target_status"] == expected


@pytest.mark.parametrize(("header_lag", "dashboard_lag"), [(None, 10), (10, None)])
def test_compute_lag_statistics_does_not_pass_an_unmeasured_surface(header_lag, dashboard_lag):
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag}])
    assert stats["target_status"] == "UNMEASURED"


def test_compute_lag_statistics_filters_pre_traffic_and_computes_stats():
    t0 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
    samples = [
        {"elapsed_s": 2.0, "header_latest_ts": t0, "header_lag_s": 65.0, "dashboard_lag_s": 70.0},
        {
            "elapsed_s": 4.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 10, tzinfo=UTC),
            "header_lag_s": 10.0,
            "dashboard_lag_s": 12.0,
        },
        {
            "elapsed_s": 6.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 15, tzinfo=UTC),
            "header_lag_s": 15.0,
            "dashboard_lag_s": 16.0,
        },
        {
            "elapsed_s": 8.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 20, tzinfo=UTC),
            "header_lag_s": 20.0,
            "dashboard_lag_s": 22.0,
        },
    ]
    stats = compute_lag_statistics(samples, baseline_latest_ts=t0)
    assert stats["sample_count"] == 3 and stats["total_samples"] == 4
    assert (
        stats["header_lag"]["min"],
        stats["header_lag"]["max"],
        stats["header_lag"]["avg"],
        stats["header_lag"]["p95"],
    ) == (10.0, 20.0, 15.0, 20.0)
    assert (
        stats["dashboard_lag"]["min"],
        stats["dashboard_lag"]["max"],
        stats["dashboard_lag"]["avg"],
        stats["dashboard_lag"]["p95"],
    ) == (12.0, 22.0, 16.7, 22.0)


def test_correlated_vitals_keys_normalise_and_reject_invalid_rows(monkeypatch):
    probe = {"key": "10000001", "sent": 0.0}
    rows = [{"probe_key": 10000001.0, "timestamp": "2026-10-22T00:00:00Z", "matches": 1}]
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": rows, "truncated": False}))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] is None and "first_visible" in probe

    req_probe = {"key": "/?probe=owned&seq=0", "sent": 0.0}
    for bad_rows in (
        [{"probe_key": "/unrelated", "timestamp": "2026-10-22T00:00:00Z", "matches": 1}],
        [{"probe_key": req_probe["key"], "timestamp": "2026-10-22T00:00:00Z", "matches": 2}],
        [{"probe_key": req_probe["key"], "timestamp": None, "matches": 1}],
    ):
        monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": bad_rows, "truncated": False}))
        res = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [req_probe])
        assert res["error"] == "invalid_or_ambiguous_correlation"
        assert "first_visible" not in req_probe and "last_absent" not in req_probe


def test_correlated_sender_separates_payloads_and_never_supplies_request_id(monkeypatch):
    requests = []

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class Opener:
        def open(self, request, **kwargs):
            requests.append(request)
            return Response()

    def build(handler):
        assert handler.redirect_request(None, None, 302, "", {}, "https://unapproved.example.test") is None
        return Opener()

    monkeypatch.setattr(freshness.urllib.request, "build_opener", build)
    for stream in ("request", "vitals", "errors"):
        assert freshness.send_correlated_probe("https://edge.example.test/", stream) == 204
    assert requests[0].data is None
    assert "measurements" in json.loads(requests[1].data) and "exceptions" not in json.loads(requests[1].data)
    assert "exceptions" in json.loads(requests[2].data) and "measurements" not in json.loads(requests[2].data)
    assert all(not req.has_header("Fastly-request-id") for req in requests)


@pytest.mark.parametrize(("status", "payload"), [(503, {}), (0, "private failure"), (200, {"data": []})])
def test_correlated_query_failure_is_not_negative_visibility(monkeypatch, status, payload):
    probe = {"key": "10000001", "sent": 0.0}
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (status, payload))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] == "query_failed_or_incomplete"
    assert "last_absent" not in probe and "first_visible" not in probe and "private failure" not in str(result)


def test_correlated_query_uses_exact_safe_literals_and_separate_datasets(monkeypatch):
    calls = []
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (calls.append(body), (200, {"data": [], "truncated": False}))[1],
    )
    for high_scale in (False, True):
        for stream in ("request", "vitals", "errors"):
            key = "10000001" if stream == "vitals" else "/?customer=O'Brien&probe=owned"
            probe = {"key": key, "sent": 0.0}
            result = freshness.poll_correlated_stream("https://admin.example.test", "svc", stream, [probe], high_scale)
            assert result["error"] is None and "last_absent" in probe
            body = calls[-1]
            column = {"request": "url", "vitals": "metric_value", "errors": "error_message"}[stream]
            literal = "10000001" if stream == "vitals" else "'/?customer=O''Brien&probe=owned'"
            assert f"{column} IN ({literal})" in body["sql"]
            assert "cid" not in body["sql"] and "client_id" not in body["sql"]
            assert body["dataset"] == {"request": "logs", "vitals": "client_vitals", "errors": "client_errors"}[stream]
            assert "LIMIT 100" in body["sql"] and "INTERVAL 1 HOUR" in body["sql"] and body["max_rows"] == 100
            assert ("metric_name = 'LCP'" in body["sql"]) == (stream == "vitals")


def test_correlated_upper_bound_and_censoring_and_population_floor(monkeypatch):
    times = iter([112.0, 115.0, 130.0, 136.0])
    monkeypatch.setattr(freshness.time, "monotonic", lambda: next(times))
    source_time = "2099-01-01T00:00:00Z"
    probe = {"stream": "request", "key": "owned", "sent": 100.0, "response": 101.0, "http_status": 200}
    responses = iter(
        [
            (200, {"data": [], "truncated": False}),
            (200, {"data": [{"probe_key": "owned", "timestamp": source_time, "matches": 1}], "truncated": False}),
        ]
    )
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: next(responses))
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    report = freshness.summarize_correlated([probe], ended=140)
    sample = report["request"]["samples"][0]
    assert sample["event_to_queryable_upper_bound_s"] == 36 and sample["send_to_queryable_interval_s"] == [12, 36]
    assert sample["source_timestamp"] == source_time and sample["left_censored"] is False
    assert report["request"]["target_assessment"] == "incomplete_not_proven_failure"
    assert report["vitals"]["observed"] == 0 and report["errors"]["complete"] is False

    bad_probes = [
        {"stream": "vitals", "key": "quiet", "sent": 1, "response": 2, "http_status": 204},
        {"stream": "errors", "key": "bad", "sent": 3, "response": 4, "http_status": 503, "first_visible": 5},
    ]
    bad_rep = freshness.summarize_correlated(bad_probes, ended=30)
    assert bad_rep["vitals"]["right_censored"] == 1 and bad_rep["vitals"]["upper_bound_s"]["p95"] is None
    assert bad_rep["vitals"]["samples"][0]["send_to_queryable_interval_s"] == [0, None]
    assert bad_rep["vitals"]["samples"][0]["followup_duration_s"] == 29
    assert (
        bad_rep["errors"]["http_errors"] == 1
        and bad_rep["errors"]["observed"] == 0
        and bad_rep["errors"]["complete"] is False
    )

    def pop_report(n):
        ps = [
            {"stream": "request", "key": str(i), "sent": 0, "response": 1, "http_status": 200, "first_visible": 15}
            for i in range(n)
        ]
        return freshness.summarize_correlated(ps, ended=20)["request"]

    assert pop_report(29)["p95_population_sufficient"] is False
    sufficient = pop_report(30)
    assert sufficient["p95_population_sufficient"] is True and sufficient["p99_population_sufficient"] is False
    assert sufficient["left_censored"] == 30 and sufficient["target_assessment"] == "within_target_upper_bound"


def test_correlated_sql_finds_only_exact_recent_events(monkeypatch):
    key = "/approved/O'Brien?probe=owned&seq=0"
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE logs(url VARCHAR, timestamp TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO logs VALUES (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP - INTERVAL 2 HOUR)",
            [key, key + "unrelated", "/old?probe=owned"],
        )

        def post(url, body, headers, **kwargs):
            result = connection.execute(body["sql"])
            columns = [col[0] for col in result.description]
            rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
            for row in rows:
                row["timestamp"] = row["timestamp"].isoformat()
            return 200, {"data": rows, "truncated": False}

        monkeypatch.setattr(freshness, "http_post", post)
        probes = [
            {"stream": "request", "key": k, "sent": 0, "response": 1, "http_status": 200}
            for k in (key, "/old?probe=owned", "/absent?probe=owned")
        ]
        result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", probes)
        assert result["error"] is None and "first_visible" in probes[0]
        assert all("first_visible" not in p and "last_absent" in p for p in probes[1:])


def test_correlated_runner_uses_approved_path_and_separate_paced_beacons(monkeypatch):
    now = [100.0]
    sends, polls = [], []
    monkeypatch.setattr(freshness.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(freshness.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(
        freshness, "http_get", lambda *a, **kw: (200, {"services": [{"service_id": "svc", "rum_enabled": True}]})
    )
    monkeypatch.setattr(
        freshness,
        "send_correlated_probe",
        lambda url, stream, value=None: (
            sends.append((now[0], stream, url, value)),
            204 if stream != "request" else 200,
        )[1],
    )
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (
            polls.append((now[0], body["dataset"])),
            (200, {"data": [], "truncated": False}),
        )[1],
    )

    report = freshness.measure_correlated_freshness(
        {"service_id": "svc", "backend_url": "https://admin.example.test"},
        "https://edge.example.test/?existing=keep",
        num_requests=1,
        num_rum=1,
        num_errors=1,
        max_wait_s=25,
    )
    assert [s[1] for s in sends] == ["request", "vitals", "errors"]
    assert all(b[0] - a[0] >= 1 for a, b in zip(sends, sends[1:]))
    request_url = urlparse(sends[0][2])
    assert request_url.path == "/" and parse_qs(request_url.query)["existing"] == ["keep"]
    assert parse_qs(sends[1][2].split("?", 1)[1])["cid"] != parse_qs(sends[2][2].split("?", 1)[1])["cid"]
    vitals_qs = parse_qs(sends[1][2].split("?", 1)[1])
    assert vitals_qs["rum_metric_value"] == [sends[1][3]] and int(sends[1][3]) >= 10_000_000
    errors_qs = parse_qs(sends[2][2].split("?", 1)[1])
    assert errors_qs["rum_error_message"] == [sends[2][3]] and sends[2][3].startswith("fla-probe-")
    assert sends[0][3] is None
    for dataset in ("logs", "client_vitals", "client_errors"):
        times = [t for t, d in polls if d == dataset]
        assert all(b - a >= 10 for a, b in zip(times, times[1:]))
    assert report["qualification"] == "incomplete"
    assert {"peak_conditions_not_verified", "request_population_incomplete_or_below_floor"} <= set(
        report["qualification_gaps"]
    )
    assert report["measurement_contract"]["event_clock_point_latency_s"] is None
    assert report["streams"]["request"]["right_censored"] == 1 and "probe=" not in json.dumps(report)


@pytest.mark.parametrize("kwargs", [{"poll_interval_s": 9}, {"num_requests": 101}, {"max_wait_s": 601}])
def test_correlated_runner_rejects_unsafe_limits_before_io(monkeypatch, kwargs):
    monkeypatch.setattr(
        freshness,
        "http_get",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unsafe configuration reached I/O")),
    )
    with pytest.raises(ValueError):
        freshness.measure_correlated_freshness(
            {"service_id": "svc", "backend_url": "https://admin.example.test"}, "https://edge.example.test/", **kwargs
        )


# ── Merged from test_scale_harness.py ──────────────────────────────────────────────────────


def test_parse_stages_and_high_rate_gate() -> None:
    stages = parse_stages("100:10,500:2")
    assert [(stage.target_rps, stage.duration_s) for stage in stages] == [(100, 10), (500, 2)]
    with pytest.raises(ValueError, match="allow-high-rate"):
        parse_stages("2500:10")
    assert parse_stages("2500:10", allow_high_rate=True)[0].target_rps == 2500


def test_rum_urls_and_freshness_lag_and_render_report() -> None:
    url = build_request_url("https://logs.example.com", 1.0)
    query = parse_qs(urlparse(url).query)
    assert query["cid"][0] == query["rum_cid"][0]
    assert query["req"][0].startswith("req-") and query["req"][0] != query["cid"][0]

    now = datetime(2026, 9, 6, 17, 42, 35, tzinfo=UTC)
    assert freshness_lag_seconds("2026-09-06T17:32:08Z", now) == 627.0 and freshness_lag_seconds(None, now) is None

    report = {
        "started_at": "2026-09-06T17:00:00+00:00",
        "stages": [
            {
                "index": 1,
                "result": {
                    "target_rps": 100,
                    "achieved_rps": 98,
                    "completed": 10,
                    "successful": 10,
                    "latency_ms": {"p95": 20},
                },
                "checkpoint": {},
            }
        ],
    }
    assert "| 1 | 100 | 98.00 | 10 | 10 | 20 | - |" in render_report(report)


def _cmcd_profile(rng: random.Random) -> TrafficProfile:
    return TrafficProfile(cmcd_ratio=1.0, sessions=build_cmcd_sessions(rng, 5))


def test_cmcd_requests_headers_startup_flag_v2_and_ratio() -> None:
    rng = random.Random(7)
    url, headers = build_request("https://logs.example.com", 0.0, rng, _cmcd_profile(rng))
    parsed = urlparse(url)
    assert parsed.path.startswith("/video/")
    cmcd = parse_qs(parsed.query)["CMCD"][0].split(",")
    assert {"CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"} <= headers.keys()
    for header in ("CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"):
        assert set(headers[header].split(",")) <= set(cmcd)
    assert any(field.startswith("sid=") for field in cmcd) and any(field.startswith("ot=") for field in cmcd)

    rng3 = random.Random(3)
    session = build_cmcd_sessions(rng3, 1)[0]
    session.version = 1
    first = build_cmcd_request(session, rng3)[1]["CMCD-Request"].split(",")
    second = build_cmcd_request(session, rng3)[1]["CMCD-Request"].split(",")
    assert "su" in first and "su" not in second

    rng_v2 = random.Random(3)
    session_v2 = build_cmcd_sessions(rng_v2, 1)[0]
    session_v2.version = 2
    assert "su=?1" in build_cmcd_request(session_v2, rng_v2)[1]["CMCD-Request"].split(",")

    rng11 = random.Random(11)
    profile = TrafficProfile(cmcd_ratio=0.5, sessions=build_cmcd_sessions(rng11, 5))
    cmcd_hits = sum("CMCD" in build_request("https://x.example.com", 0.0, rng11, profile)[0] for _ in range(400))
    assert 120 < cmcd_hits < 280


def test_default_profile_diversify_and_rum_beacon_isolation() -> None:
    assert build_request("https://x.example.com", 0.0, random.Random(1), None) == ("https://x.example.com", {})
    assert build_request("https://x.example.com", 0.0, random.Random(1), TrafficProfile()) == (
        "https://x.example.com",
        {},
    )

    rng = random.Random(5)
    results = [build_request("https://x.example.com", 0.0, rng, TrafficProfile(diversify=True)) for _ in range(300)]
    assert len({urlparse(url).path for url, _ in results}) > 5
    assert any(ua in BOT_USER_AGENTS for ua in (headers["User-Agent"] for _, headers in results))

    rng2 = random.Random(2)
    profile = TrafficProfile(cmcd_ratio=1.0, diversify=True, sessions=build_cmcd_sessions(rng2, 3))
    url, headers = build_request("https://x.example.com", 1.0, rng2, profile)
    assert urlparse(url).path == "/rum-beacon" and headers == {}


# ── Merged from test_verify_dashboard*.py ──────────────────────────────────────────────────────

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"
_CLASSIFIER_JS = Path(__file__).resolve().parents[2] / "scripts" / "lib" / "console_blip.js"
_MIN_RECENT_MINUTES = 10


def _src() -> str:
    return _VERIFY_JS.read_text()


def _classifier_src() -> str:
    return _CLASSIFIER_JS.read_text()


def test_verify_dashboard_logging_and_navigation_resilience_contracts() -> None:
    src = _src()
    assert re.search(r"function ts\(\)", src) and "`${ts()} [Browser Console]" in src
    assert "requestfailed" in src and "gotoWithShellReady" in src
    m_attempts = re.search(r"attempts\s*=\s*(\d+)", src)
    assert m_attempts and 2 <= int(m_attempts.group(1)) <= 6
    assert "gotoWithShellReady(rumPage, rumUrl30d" in src and not re.search(r"rumPage\.goto\(rumUrl30d", src)
    assert "gotoWithShellReady(networkPage, networkUrl30d" in src or (
        "gotoWithShellReady(routePage, targetUrl" in src and "routePath: '/network'" in src
    )
    assert not re.search(r"networkPage\.goto\(networkUrl30d", src)
    m_settle = re.search(r"RUM_RENDER_SETTLE_MS\s*=\s*(\d+)", src)
    assert m_settle and int(m_settle.group(1)) >= 8000
    assert "RUM_RENDER_SETTLE_MS" in src and src.count("waitForFunction") >= 1
    assert not re.search(r"rumPage\.waitForTimeout\(4000\)", src)


def test_verify_dashboard_recency_window_contracts() -> None:
    src = _src()
    m = re.search(r'RECENT_RANGE\s*=\s*"(\d+m)"', src)
    assert m, 'verify_dashboard.js must define a RECENT_RANGE = "<n>m" constant'
    assert int(m.group(1)[:-1]) >= _MIN_RECENT_MINUTES
    assert len(re.findall(r"getRecentWindow\([^,\n]+,\s*RECENT_RANGE\b", src)) >= 2
    assert not re.findall(r'(?:getRecentWindow\([^,\n]+,|"range",)\s*"(\d+m)"', src)
    assert not re.search(r"\d+\s*-\s*\d+\s*min.*floor|floor.*\d+\s*-\s*\d+\s*min", src)


def test_verify_dashboard_resource_classification_and_transient_tolerance() -> None:
    src, classifier = _src(), _classifier_src()
    assert re.search(r"GENERIC_RESOURCE_FAILURE_RE", src) and "function isCriticalResourceUrl" in src
    assert re.search(r"requestfailed.*?isCriticalResourceUrl", src, re.S)
    assert "text.includes('woff2')" not in src and "/api/web-vitals" in src and "/api/ux-events" in src

    m = re.search(r"TRANSIENT_ERROR_BUDGET\s*=\s*(\d+)", src)
    assert m and 1 <= int(m.group(1)) <= 20
    assert re.search(r"status\s*===\s*503", src) and "ERR_CONNECTION_REFUSED" in src
    assert not re.search(r"status\s*>=\s*400[^\n]*transient\s*=\s*true", src)
    assert "ERR_FAILED" in classifier and ("ChunkLoadError" in classifier or "Failed to load chunk" in classifier)
    assert "require('./lib/console_blip')" in src or 'require("./lib/console_blip")' in src


@pytest.mark.parametrize(
    ("charts", "expected"),
    [
        ([], False),
        ([{"width": 524, "height": 300, "data": [{"x": [], "y": []}]}], False),
        ([{"width": 1, "height": 1, "data": [{"x": [0], "y": [0]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [0]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"values": [10]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [None]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": ["10"]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [float("nan")]}]}], False),
    ],
)
def test_rum_chart_requires_real_geometry_and_points(charts: list[dict], expected: bool) -> None:
    source = _src()
    start = source.index("function hasPopulatedRumChart()")
    end = source.index("\n}", start) + 2
    script = (
        source[start:end]
        + f"\nconst charts = {json.dumps(charts)};"
        + "\nglobal.document = {querySelectorAll: selector => {"
        + "if (selector !== 'main .js-plotly-plot') throw Error('Unscoped chart selector');"
        + "return charts.map(c => ({data:c.data,getBoundingClientRect:()=>c}));}};"
        + "\nconsole.log(JSON.stringify(hasPopulatedRumChart()));"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) is expected


def test_remote_rum_chart_failure_cannot_warn_and_proceed() -> None:
    source = _src()
    assert "waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 })" in source
    assert "No populated full-sized RUM chart" in source
    assert "No visible Plotly charts found on the RUM page for remote cloud environment" not in source


def _verify_node(script: str) -> dict:
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _frontend_node_runtime() -> str:
    return r"""
const frontendFs = require('node:fs'), frontendPath = require('node:path'), frontendVm = require('node:vm');
const frontendRoot = frontendPath.resolve('frontend');
const frontendRequire = require('node:module').createRequire(frontendPath.join(frontendRoot, 'package.json'));
const ts = frontendRequire('typescript'), modules = new Map();
let now = Date.parse('2026-10-24T23:39:18.378Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const frontendWindow = {
  location: {}, history: {replaceState(_state, _title, href) {
    frontendWindow.location = {href, search: new URL(href).search};
  }},
};
function loadFrontend(relativePath) {
  if (relativePath === 'lib/date.ts' || relativePath === 'lib/format.ts') relativePath = 'lib/utils.ts';
  const filename = frontendPath.resolve(frontendRoot, relativePath);
  if (modules.has(filename)) return modules.get(filename).exports;
  const module = {exports: {}};
  modules.set(filename, module);
  const code = ts.transpileModule(frontendFs.readFileSync(filename, 'utf8'), {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022}, fileName: filename,
  }).outputText;
  frontendVm.runInNewContext(code, {
    module, exports: module.exports, Date: Clock, window: frontendWindow, URL, URLSearchParams, console,
    require: name => name.startsWith('@/') ? loadFrontend(name.slice(2) + '.ts') : frontendRequire(name),
  }, {filename});
  return module.exports;
}
function resolveNavigation(href) {
  frontendWindow.location = {href, search: new URL(href).search};
  const {useFilterStore} = loadFrontend('stores/filterStore.ts');
  const utilsMod = loadFrontend('lib/utils.ts');
  useFilterStore.getState().resetAll();
  utilsMod._resetUrlHydrationFlag();
  utilsMod.hydrateFilterStoreFromUrl();
  const state = useFilterStore.getState();
  return utilsMod.resolveRangeWire({...state, anchor: state.endTime});
}
"""


@pytest.mark.parametrize("stream", ["REQUEST", "RUM"])
@pytest.mark.parametrize(
    ("age", "fresh"),
    [
        ("0s ago", True),
        ("59s ago", True),
        ("1m 0s ago", True),
        ("14m 59s ago", True),
        ("15m 0s ago", False),
        ("6h 26m ago", False),
        ("6h 26m 1s ago", False),
        ("1d 0h 0m ago", False),
        ("Never", False),
        ("—", False),
        ("-", False),
        ("", False),
        ("just now", False),
        ("tomorrow", False),
        ("ago", False),
    ],
)
def test_verify_header_requires_bounded_event_age(stream: str, age: str, fresh: bool) -> None:
    source = _src()
    helpers = source[source.index("function assertRecentHeader(") : source.index("function isRecentRequestResponse(")]
    header = f"{stream}\nlatest:\n{age}\ntotal: 999999"
    result = _verify_node(
        'const RECENT_RANGE = "15m"; const messages = []; const console = {log: msg => messages.push(msg)};\n'
        + helpers
        + f"\nlet fresh = true; try {{ assertRecentHeader({json.dumps(header)}, {json.dumps(stream)}); }}"
        + " catch { fresh = false; } process.stdout.write(JSON.stringify({fresh, messages}));"
    )
    assert result["fresh"] is fresh and bool(result["messages"]) is fresh
    assert not any("updating!" in message for message in result["messages"])


@pytest.mark.parametrize("stream", ["REQUEST", "RUM"])
@pytest.mark.parametrize(
    ("age_seconds", "expected_age", "fresh"),
    [
        (0, "0s ago", True),
        (59, "59s ago", True),
        (60, "1m 0s ago", True),
        (899, "14m 59s ago", True),
        (900, "15m 0s ago", False),
        (3600, "1h 0m 0s ago", False),
        (23161, "6h 26m 1s ago", False),
        (86400, "1d 0h 0m ago", False),
        (-1, "just now", False),
    ],
)
def test_verify_header_accepts_real_ui_duration_shapes(
    stream: str, age_seconds: int, expected_age: str, fresh: bool
) -> None:
    source = _src()
    helper = source[source.index("function assertRecentHeader(") : source.index("function getRecentWindow(")]
    result = _verify_node(
        _frontend_node_runtime()
        + '\nconst RECENT_RANGE = "15m";\n'
        + helper
        + f"\nconst age = loadFrontend('lib/date.ts').formatTimeAgo(new Clock(now - {age_seconds} * 1000));"
        + "\nlet fresh = true; const originalLog = console.log; console.log = () => {};"
        + f"\ntry {{ assertRecentHeader({json.dumps(stream)} + '\\nlatest:\\n' + age, {json.dumps(stream)}); }}"
        + " catch { fresh = false; } console.log = originalLog; process.stdout.write(JSON.stringify({age, fresh}));"
    )
    assert result == {"age": expected_age, "fresh": fresh}


@pytest.mark.parametrize("route", ["/dashboard", "/rum"])
def test_verify_absolute_url_resolves_to_exact_frontend_request(route: str) -> None:
    source = _src()
    helper = source[source.index("function getRecentWindow(") : source.index("function hasRecentWindow(")]
    result = _verify_node(
        _frontend_node_runtime()
        + "\nconst recentWindow = frontendVm.runInNewContext("
        + json.dumps(
            helper
            + f"\ngetRecentWindow('https://test.invalid{route}?range=24h&start_time=old&end_time=old&view=kept', '15m')"
        )
        + ", {Date: Clock, URL}); const wire = resolveNavigation(recentWindow.url);"
        + "\nprocess.stdout.write(JSON.stringify({recentWindow, wire}));"
    )
    window, wire = result["recentWindow"], result["wire"]
    assert parse_qs(urlparse(window["url"]).query) == {
        "start_time": ["2026-10-24T23:24:18.378Z"],
        "end_time": ["2026-10-24T23:39:18.378Z"],
        "view": ["kept"],
    }
    assert wire["rangeToken"] is None
    assert wire["rangeBody"] == {"start_time": window["start_time"], "end_time": window["end_time"]}
    assert wire["rangeKey"] == f"abs:{window['start_time']}|{window['end_time']}"


@pytest.mark.parametrize(
    ("overrides", "accepted"),
    [
        ({}, True),
        ({"path": "/api/dashboard/aggregates"}, True),
        ({"range": "24h"}, False),
        ({"range": "15m"}, False),
        ({"range": "auto"}, False),
        ({"start": None}, False),
        ({"end": None}, False),
        ({"start": "invalid"}, False),
        ({"start": "2020-01-01T00:00:00Z"}, False),
        ({"end": "2099-01-01T00:00:00Z"}, False),
        ({"offset": 86400000}, False),
        ({"duration": 86400000}, False),
        ({"offset": 1001}, False),
        ({"expected": None}, False),
        ({"path": "/api/admin/sync-status"}, False),
        ({"origin": "https://other.invalid"}, False),
        ({"method": "GET"}, False),
        ({"ok": False}, False),
    ],
)
def test_verify_request_evidence_requires_actual_recent_window(overrides: dict, accepted: bool) -> None:
    source = _src()
    helpers = source[source.index("function hasRecentWindow(") : source.index("// Helper to append")]
    result = _verify_node(
        'const RECENT_RANGE = "15m"; const baseUrl = "https://test.invalid/dashboard";\n'
        + helpers
        + f"\nconst opts = Object.assign({{path: '/api/dashboard/bundle', origin: 'https://test.invalid', range: null, duration: 900000, offset: 1000, method: 'POST', ok: true}}, {json.dumps(overrides)});"
        + """
const selectedEnd = Date.now() - 1000;
const expectedWindow = Object.hasOwn(opts, 'expected') ? opts.expected : {
  start_time: new Date(selectedEnd - 900000).toISOString(), end_time: new Date(selectedEnd).toISOString(),
};
const end = selectedEnd + 1000 - opts.offset;
const body = {
  range_token: opts.range,
  start_time: Object.hasOwn(opts, 'start') ? opts.start : new Date(end - opts.duration).toISOString(),
  end_time: Object.hasOwn(opts, 'end') ? opts.end : new Date(end).toISOString(),
};
const response = {
  url: () => opts.origin + opts.path, ok: () => opts.ok,
  request: () => ({method: () => opts.method, postDataJSON: () => body}),
};
const query = new URLSearchParams(Object.entries(body).filter(([, value]) => value !== null));
const rumResponse = {
  ...response, url: () => opts.origin + '/api/services/test/rum/analytics?' + query,
  request: () => ({method: () => opts.method === 'POST' ? 'GET' : 'POST'}),
};
process.stdout.write(JSON.stringify({
  accepted: isRecentRequestResponse(response, expectedWindow), rumAccepted: isRecentRumResponse(rumResponse, expectedWindow),
}));
"""
    )
    assert result["accepted"] is accepted
    if "path" not in overrides:
        assert result["rumAccepted"] is accepted


@pytest.mark.parametrize("count", [0, 149, 150, 999, None, "999", -1, 1.5, True])
def test_verify_window_count_never_uses_lifetime_total(count: object) -> None:
    source = _src()
    helpers = source[source.index("async function recentRequestCount(") : source.index("// Helper to append")]
    result = _verify_node(
        helpers
        + f"\n(async () => {{ const count = {json.dumps(count)};"
        + """
const values = await Promise.all([
  recentRequestCount(null),
  recentRequestCount({url: () => 'https://test.invalid/api/dashboard/bundle',
    json: async () => ({total_rows: 999999, aggregates: {total_rows: count}})}),
  recentRequestCount({url: () => 'https://test.invalid/api/dashboard/aggregates',
    json: async () => ({total_rows: count})}),
]);
process.stdout.write(JSON.stringify({values}));
})();
"""
    )
    expected = count if type(count) is int and count >= 0 else None
    assert result["values"] == [None, expected, expected]


@pytest.mark.parametrize(
    ("flags", "request_count", "beacons", "age", "charts", "exit_code"),
    [
        (["--no-traffic"], None, 0, "6h 26m ago", True, 0),
        (["--no-traffic"], None, 0, "Never", False, 1),
        (["--no-traffic"], None, 0, "6h 26m ago", "/rum", 1),
        (["--no-traffic"], None, 0, "6h 26m ago", "/streaming", 1),
        ([], None, 100, "1s ago", True, 1),
        ([], 149, 100, "1s ago", True, 1),
        ([], 150, 49, "1s ago", True, 1),
        ([], 150, 50, "6h 26m ago", True, 1),
        ([], 150, 50, {"REQUEST": "1s ago", "RUM": "6h 26m ago"}, True, 1),
        ([], 150, 50, "1s ago", True, 0),
        ([], [149, 150], [49, 50], "1s ago", True, 0),
        (["--no-trafic"], 150, 50, "1s ago", True, 1),
    ],
)
def test_verify_script_modes_without_browser_or_network(
    flags: list[str],
    request_count: int | list[int] | None,
    beacons: int | list[int],
    age: str | dict,
    charts: bool | str,
    exit_code: int,
) -> None:
    options = {"flags": flags, "requestCount": request_count, "beacons": beacons, "age": age, "charts": charts}
    result = _verify_node(
        _frontend_node_runtime()
        + f"\nconst vm = require('node:vm'); const source = {json.dumps(_src())}; const opts = {json.dumps(options)};"
        + r"""
const visits = [], logs = [], resolvedRequests = [], attempts = {};
const chart = {data: [{y: [100]}], getBoundingClientRect: () => ({width: 500, height: 300})};
let currentUrl = '', pendingResponse = null, displayedBeacons = 1000;
const hasCharts = () => opts.charts !== false && (typeof opts.charts !== 'string' || new URL(currentUrl).pathname !== opts.charts);
const document = {
  body: {get innerText() {
    const age = stream => typeof opts.age === 'string' ? opts.age : opts.age[stream];
    return `REQUEST\nlatest:\n${age('REQUEST')}\ntotal: 999,999\nRUM\nlatest:\n${age('RUM')}\ntotal: 999,999\nTOTAL BEACONS\n${displayedBeacons}\n`
      + 'Largest Contentful Paint GOOD Global Health Total Sessions Traffic Quality Breakdown Origin Error Rate Slowest URLs';
  }},
  querySelector: selector => selector === 'footer' ? {innerText: 'commit:test standard local'} : hasCharts() ? chart : null,
  querySelectorAll: () => hasCharts() ? [chart] : [],
};
const page = {
  on() {}, async goto(url) {
    currentUrl = url; visits.push(url); now += 1000; displayedBeacons = 1000;
    if (pendingResponse) {
      const pending = pendingResponse; pendingResponse = null;
      const wire = resolveNavigation(url), body = wire.rangeBody, rum = new URL(url).pathname === '/rum';
      const key = rum ? 'rum' : 'dashboard', attempt = attempts[key] || 0; attempts[key] = attempt + 1;
      const counts = rum ? opts.beacons : opts.requestCount;
      const count = Array.isArray(counts) ? counts[Math.min(attempt, counts.length - 1)] : counts;
      if (rum) displayedBeacons = count;
      const response = {
        url: () => 'https://test.invalid' + (rum ? '/api/services/test/rum/analytics?' + new URLSearchParams(body) : '/api/dashboard/bundle'),
        ok: () => true, request: () => ({method: () => rum ? 'GET' : 'POST', postDataJSON: () => body}),
        json: async () => rum ? {beacon_count: count} : {aggregates: {total_rows: count}},
      };
      const accepted = pending.predicate(response);
      resolvedRequests.push({navigation: url, body, rangeToken: wire.rangeToken, accepted});
      if (count !== null && accepted) pending.resolve(response);
      else pending.reject(Error('no window evidence'));
    }
    return {ok: () => true};
  },
  async waitForSelector() {}, async waitForTimeout(ms) { now += ms; },
  async waitForFunction(fn) { if (!fn()) throw Error('predicate not satisfied'); },
  waitForResponse(predicate) { return new Promise((resolve, reject) => { pendingResponse = {predicate, resolve, reject}; }); },
  locator: () => ({first: () => ({waitFor: async () => { if (!hasCharts()) throw Error('no chart'); }})}),
  async evaluate(fn) { return fn.toString().includes('performance.getEntries') ? {} : fn(); },
  async close() {}, async screenshot() {},
};
const context = {newPage: async () => page, close: async () => {}, request: {get: async () => ({ok: () => false})}};
const browser = {newContext: async () => context, close: async () => {}};
const dependencies = {
  playwright: {chromium: {launch: async () => browser}}, path: require('node:path'), child_process: {},
  './lib/admin_gateway_client.cjs': {adminGatewayContextOptions: () => ({})},
  './lib/section_measurements.cjs': {collectResponseMeasurements: () => ({snapshot: async () => [], gaps: () => [], close() {}}), budgets: {}},
  './lib/console_blip': {isTransientConsoleBlip: () => false},
};
new Promise(resolve => {
  const execution = vm.runInNewContext(source, {
    require(name) { if (!(name in dependencies)) throw Error('unexpected dependency: ' + name); return dependencies[name]; },
    process: {argv: ['node', 'verify_dashboard.js', 'https://test.invalid/dashboard', 'test', 'standard', 'local', ...opts.flags],
      exit(code) { resolve(code); throw Error('exit:' + code); }},
    console: Object.fromEntries(['log', 'warn', 'error'].map(level => [level, (...args) => logs.push(args.join(' '))])),
    URL, URLSearchParams, Date: Clock, document, __dirname: '/scripts',
  });
  execution?.catch(error => { if (!error.message.startsWith('exit:')) throw error; });
}).then(code => process.stdout.write(JSON.stringify({code, visits, resolvedRequests, logs})));
"""
    )
    assert result["code"] == exit_code
    messages = "\n".join(result["logs"])
    assert "Header request count and page metrics are consistent!" not in messages
    if flags == ["--no-traffic"]:
        assert result["resolvedRequests"] == [] and all("start_time" not in url for url in result["visits"])
        assert "[Dashboard 15m] NOT RUN" in messages and "latest event age" not in messages
        if exit_code == 0:
            assert "[RUM 15m] NOT RUN" in messages
            assert {urlparse(url).path for url in result["visits"]} == {
                "/dashboard",
                "/rum",
                "/network",
                "/streaming",
                "/security",
                "/origin",
                "/performance",
            }
    elif not flags:
        assert result["resolvedRequests"]
        for request in result["resolvedRequests"]:
            query = parse_qs(urlparse(request["navigation"]).query)
            assert "range" not in query and request["rangeToken"] is None and request["accepted"] is True
            assert request["body"] == {"start_time": query["start_time"][0], "end_time": query["end_time"][0]}
            start, end = (datetime.fromisoformat(request["body"][key]) for key in ("start_time", "end_time"))
            assert (end - start).total_seconds() == 900
        if isinstance(request_count, list):
            for route in ("/dashboard", "/rum"):
                bounds = [
                    r["body"]["end_time"] for r in result["resolvedRequests"] if urlparse(r["navigation"]).path == route
                ]
                assert len(bounds) == 2 and bounds[0] < bounds[1]
        assert "NOT RUN" not in messages
        if request_count is None:
            assert "UNVERIFIED" in messages and "requested-window aggregate contains" not in messages


@pytest.mark.parametrize("no_traffic", ["0", "1"])
def test_verify_canonical_runner_propagates_no_traffic(no_traffic: str) -> None:
    runner = _VERIFY_JS.parent / "dev" / "deploy_test_all.sh"
    if not runner.exists():
        pytest.skip("Canonical deployment runner is operator-local and gitignored")
    source = runner.read_text()
    start = source.index("    local verify_flags=()")
    end = source.index('\n  } >"$9"', start)
    script = (
        "node() { printf '%s\\n' \"$@\"; }\n"
        "validate_env() {\nlocal NO_TRAFFIC=$1 COMMIT_HASH=test\n"
        "set -- label x x x x https://test.invalid/dashboard standard local\n"
        + source[start:end]
        + f"\n}}\nvalidate_env {no_traffic}"
    )
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, timeout=10)
    assert result.stdout.splitlines() == [
        "../scripts/verify_dashboard.js",
        "https://test.invalid/dashboard",
        "test",
        "standard",
        "local",
        *(["--no-traffic"] if no_traffic == "1" else []),
    ]
