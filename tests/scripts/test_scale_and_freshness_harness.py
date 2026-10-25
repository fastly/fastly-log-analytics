from __future__ import annotations

"""RUM synthetic generators folded into scripts/lib/log_factory.py (S3).

Pins: same seed -> byte-identical output, the record/beacon data shapes the
ingest path and the edge VCL depend on, and that both historical call sites
(FOS seeding script, scale harness) draw only from the injected rng.
"""

import hashlib
import json
import random
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import pytest

from scripts.dev import scale_harness
from scripts.lib import log_factory as lf

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


def test_records_are_deterministic_for_a_seed() -> None:
    a = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    b = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    c = lf.generate_rum_records("svc", 200, NOW, random.Random(8))
    assert a == b
    assert a != c


def test_records_golden_digest() -> None:
    records = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    assert _digest(records) == "884741d052c6a872939dce4c7b001739062f79dde8d457a4ec4e490d49555614"


def test_record_shape_matches_ingest_contract() -> None:
    records = lf.generate_rum_records("svc-1", 300, NOW, random.Random(1))
    assert len({r["timestamp"] for r in records}) == 300
    assert records[0]["timestamp"] == "2026-10-19T12:00:00.000Z"
    assert records[1]["timestamp"] == "2026-10-19T11:59:59.950Z"
    kinds: set[str] = set()
    for rec in records:
        assert set(rec) == RECORD_KEYS
        assert rec["service_id"] == "svc-1"
        assert rec["req_id"].startswith("req-") and len(rec["req_id"]) == 4 + 16
        assert rec["fastly_req_id"] == rec["request_event_id"] == rec["req_id"]
        assert rec["rum_cid"].startswith("cid-")
        assert rec["url"].startswith("http://localhost/")
        body = json.loads(rec["rum_body"])
        assert body["meta"]["page"]["url"] == rec["url"]
        assert body["meta"]["os"]["name"] == rec["os"]
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
            (frame,) = body["exceptions"][0]["stacktrace"]["frames"]
            assert frame["filename"] == "static/chunks/main.js"
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
    urls = [scale_harness.build_request_url("https://x.example.com", 0.5, rng) for _ in range(200)]
    beacons = sum("/rum-beacon?" in u for u in urls)
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

from datetime import UTC, datetime

import pytest

from scripts.dev.measure_e2e_freshness import (
    compute_lag_statistics,
    configured_polling,
    cron_completion_time,
    load_environment_config,
    new_terminal_run,
    rum_beacon_count,
    seconds_between,
)


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


def test_cron_completion_time_uses_started_at_and_duration():
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": 2.5}) == datetime(
        2026, 9, 29, 12, 0, 2, 500000, tzinfo=UTC
    )
    assert cron_completion_time({"started_at": "invalid", "duration_s": 2}) is None
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": "invalid"}) is None


def test_new_terminal_run_requires_a_new_terminal_row():
    assert new_terminal_run({"entries": [{"id": 3, "status": "warning"}]}, 2) == {"id": 3, "status": "warning"}
    assert new_terminal_run({"entries": [{"id": 3, "status": "running"}]}, 2) is None
    assert new_terminal_run({"entries": [{"id": 2, "status": "success"}]}, 2) is None
    assert new_terminal_run({"entries": [{"id": "invalid", "status": "success"}]}, 2) is None
    assert new_terminal_run({"entries": []}, 2) is None


def test_seconds_between_preserves_negative_delays_and_handles_missing_times():
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


def test_compute_lag_statistics_empty_samples():
    stats = compute_lag_statistics([])
    assert stats["sample_count"] == 0
    assert stats["header_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
    assert stats["dashboard_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
    assert stats["target_status"] == "UNMEASURED"


@pytest.mark.parametrize(
    ("header_lag", "dashboard_lag", "expected"),
    [
        (30.2, 10.0, "ELEVATED"),
        (10.0, 30.2, "ELEVATED"),
        (20.0, 20.0, "WITHIN TARGET"),
        (20.04, 20.04, "ELEVATED"),
    ],
)
def test_compute_lag_statistics_assesses_both_p95s_not_the_mean(header_lag, dashboard_lag, expected):
    stats = compute_lag_statistics(
        [
            {"elapsed_s": 6, "header_lag_s": 1.0, "dashboard_lag_s": 1.0},
            {"elapsed_s": 8, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag},
        ]
    )

    assert stats["header_lag"]["avg"] < 20
    assert stats["dashboard_lag"]["avg"] < 20
    assert stats["target_status"] == expected


def test_compute_lag_statistics_uses_shared_freshness_budget(monkeypatch):
    from scripts.lib.section_measurements import BUDGETS

    monkeypatch.setitem(BUDGETS, "peak_freshness_p95_ms", 25000)
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": 25.0, "dashboard_lag_s": 25.0}])

    assert stats["target_p95_s"] == 25
    assert stats["target_status"] == "WITHIN TARGET"


def test_compute_lag_statistics_uses_nearest_rank_tail_percentiles():
    stats = compute_lag_statistics(
        [{"elapsed_s": 6, "header_lag_s": lag, "dashboard_lag_s": lag} for lag in range(1, 21)]
    )

    assert stats["header_lag"]["p95"] == 19
    assert stats["header_lag"]["p99"] == 20
    assert stats["dashboard_lag"]["p95"] == 19
    assert stats["dashboard_lag"]["p99"] == 20


@pytest.mark.parametrize(("header_lag", "dashboard_lag"), [(None, 10), (10, None)])
def test_compute_lag_statistics_does_not_pass_an_unmeasured_surface(header_lag, dashboard_lag):
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag}])

    assert stats["target_status"] == "UNMEASURED"


def test_compute_lag_statistics_filters_pre_traffic_and_computes_stats():
    t0 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 10, 9, 12, 0, 10, tzinfo=UTC)
    t2 = datetime(2026, 10, 9, 12, 0, 15, tzinfo=UTC)
    t3 = datetime(2026, 10, 9, 12, 0, 20, tzinfo=UTC)

    samples = [
        # Idle sample before new traffic arrives
        {
            "elapsed_s": 2.0,
            "header_latest_ts": t0,
            "header_lag_s": 65.0,
            "dashboard_lag_s": 70.0,
        },
        # Active samples after edge ingests new traffic
        {
            "elapsed_s": 4.0,
            "header_latest_ts": t1,
            "header_lag_s": 10.0,
            "dashboard_lag_s": 12.0,
        },
        {
            "elapsed_s": 6.0,
            "header_latest_ts": t2,
            "header_lag_s": 15.0,
            "dashboard_lag_s": 16.0,
        },
        {
            "elapsed_s": 8.0,
            "header_latest_ts": t3,
            "header_lag_s": 20.0,
            "dashboard_lag_s": 22.0,
        },
    ]

    stats = compute_lag_statistics(samples, baseline_latest_ts=t0)
    assert stats["sample_count"] == 3
    assert stats["total_samples"] == 4
    assert stats["header_lag"]["min"] == 10.0
    assert stats["header_lag"]["max"] == 20.0
    assert stats["header_lag"]["avg"] == 15.0
    assert stats["header_lag"]["p95"] == 20.0
    assert stats["dashboard_lag"]["min"] == 12.0
    assert stats["dashboard_lag"]["max"] == 22.0
    assert stats["dashboard_lag"]["avg"] == 16.7
    assert stats["dashboard_lag"]["p95"] == 22.0


def test_correlated_query_rejects_unrelated_and_duplicate_rows(monkeypatch):
    from scripts.dev import measure_e2e_freshness as freshness

    probe = {"key": "/?probe=owned&seq=0", "sent": 0.0}
    for rows in (
        [{"probe_key": "/unrelated", "timestamp": "2026-10-22T00:00:00Z", "matches": 1}],
        [{"probe_key": probe["key"], "timestamp": "2026-10-22T00:00:00Z", "matches": 2}],
        [{"probe_key": probe["key"], "timestamp": None, "matches": 1}],
    ):
        monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": rows, "truncated": False}))
        result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
        assert result["error"] == "invalid_or_ambiguous_correlation"
        assert "first_visible" not in probe
        assert "last_absent" not in probe


def test_correlated_sender_separates_payloads_and_never_supplies_request_id(monkeypatch):
    from scripts.dev import measure_e2e_freshness as freshness

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
    assert "measurements" in json.loads(requests[1].data)
    assert "exceptions" not in json.loads(requests[1].data)
    assert "exceptions" in json.loads(requests[2].data)
    assert "measurements" not in json.loads(requests[2].data)
    assert all(not req.has_header("Fastly-request-id") for req in requests)


@pytest.mark.parametrize(("status", "payload"), [(503, {}), (0, "private failure"), (200, {"data": []})])
def test_correlated_query_failure_is_not_negative_visibility(monkeypatch, status, payload):
    from scripts.dev import measure_e2e_freshness as freshness

    probe = {"key": "owned", "sent": 0.0}
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (status, payload))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] == "query_failed_or_incomplete"
    assert "last_absent" not in probe
    assert "first_visible" not in probe
    assert "private failure" not in str(result)


def test_correlated_query_uses_exact_safe_literals_and_separate_datasets(monkeypatch):
    from scripts.dev import measure_e2e_freshness as freshness

    calls = []

    def post(url, body, headers, **kwargs):
        calls.append(body)
        return 200, {"data": [], "truncated": False}

    monkeypatch.setattr(freshness, "http_post", post)
    for high_scale in (False, True):
        for stream in ("request", "vitals", "errors"):
            probe = {"key": "/?customer=O'Brien&probe=owned", "sent": 0.0}
            result = freshness.poll_correlated_stream("https://admin.example.test", "svc", stream, [probe], high_scale)
            assert result["error"] is None
            assert "last_absent" in probe
            body = calls[-1]
            column = "url" if stream == "request" else "client_id" if high_scale else "cid"
            assert f"{column} IN ('/?customer=O''Brien&probe=owned')" in body["sql"]
            assert body["dataset"] == {"request": "logs", "vitals": "client_vitals", "errors": "client_errors"}[stream]
            assert "LIMIT 100" in body["sql"]
            assert "INTERVAL 1 HOUR" in body["sql"]
            assert body["max_rows"] == 100
            assert ("metric_name = 'LCP'" in body["sql"]) == (stream == "vitals")


def test_correlated_upper_bound_does_not_use_source_clock_or_prove_failure(monkeypatch):
    from scripts.dev import measure_e2e_freshness as freshness

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
    assert sample["event_to_queryable_upper_bound_s"] == 36
    assert sample["send_to_queryable_interval_s"] == [12, 36]
    assert sample["source_timestamp"] == source_time
    assert sample["left_censored"] is False
    assert report["request"]["target_assessment"] == "incomplete_not_proven_failure"
    assert report["vitals"]["observed"] == 0
    assert report["errors"]["complete"] is False


def test_correlated_sql_finds_only_exact_recent_events(monkeypatch):
    import duckdb

    from scripts.dev import measure_e2e_freshness as freshness

    key = "/approved/O'Brien?probe=owned&seq=0"
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE logs(url VARCHAR, timestamp TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO logs VALUES (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP), "
            "(?, CURRENT_TIMESTAMP - INTERVAL 2 HOUR)",
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
        assert result["error"] is None
        assert "first_visible" in probes[0]
        assert all("first_visible" not in p and "last_absent" in p for p in probes[1:])


def test_correlated_missing_and_http_failed_probes_never_pass():
    from scripts.dev import measure_e2e_freshness as freshness

    probes = [
        {"stream": "vitals", "key": "quiet", "sent": 1, "response": 2, "http_status": 204},
        {"stream": "errors", "key": "bad", "sent": 3, "response": 4, "http_status": 503, "first_visible": 5},
    ]
    report = freshness.summarize_correlated(probes, ended=30)
    assert report["vitals"]["right_censored"] == 1
    assert report["vitals"]["upper_bound_s"]["p95"] is None
    assert report["vitals"]["samples"][0]["send_to_queryable_interval_s"] == [0, None]
    assert report["vitals"]["samples"][0]["followup_duration_s"] == 29
    assert report["errors"]["http_errors"] == 1
    assert report["errors"]["observed"] == 0
    assert report["errors"]["complete"] is False


def test_correlated_tail_population_floor_and_left_censoring():
    from scripts.dev import measure_e2e_freshness as freshness

    def report(n):
        probes = [
            {"stream": "request", "key": str(i), "sent": 0, "response": 1, "http_status": 200, "first_visible": 15}
            for i in range(n)
        ]
        return freshness.summarize_correlated(probes, ended=20)["request"]

    assert report(29)["p95_population_sufficient"] is False
    sufficient = report(30)
    assert sufficient["p95_population_sufficient"] is True
    assert sufficient["p99_population_sufficient"] is False
    assert sufficient["left_censored"] == 30
    assert sufficient["target_assessment"] == "within_target_upper_bound"


def test_correlated_runner_uses_approved_path_and_separate_paced_beacons(monkeypatch):
    from scripts.dev import measure_e2e_freshness as freshness

    now = [100.0]
    sends, polls = [], []
    monkeypatch.setattr(freshness.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(freshness.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(
        freshness,
        "http_get",
        lambda *a, **kw: (200, {"services": [{"service_id": "svc", "rum_enabled": True}]}),
    )

    def send(url, stream):
        sends.append((now[0], stream, url))
        return 204 if stream != "request" else 200

    def post(url, body, headers, **kwargs):
        polls.append((now[0], body["dataset"]))
        return 200, {"data": [], "truncated": False}

    monkeypatch.setattr(freshness, "send_correlated_probe", send)
    monkeypatch.setattr(freshness, "http_post", post)
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
    assert request_url.path == "/"
    assert parse_qs(request_url.query)["existing"] == ["keep"]
    assert parse_qs(sends[1][2].split("?", 1)[1])["cid"] != parse_qs(sends[2][2].split("?", 1)[1])["cid"]
    for dataset in ("logs", "client_vitals", "client_errors"):
        times = [t for t, d in polls if d == dataset]
        assert all(b - a >= 10 for a, b in zip(times, times[1:]))
    assert report["qualification"] == "incomplete"
    assert "peak_conditions_not_verified" in report["qualification_gaps"]
    assert "request_population_incomplete_or_below_floor" in report["qualification_gaps"]
    assert report["measurement_contract"]["event_clock_point_latency_s"] is None
    assert report["streams"]["request"]["right_censored"] == 1
    assert "probe=" not in json.dumps(report)


@pytest.mark.parametrize("kwargs", [{"poll_interval_s": 9}, {"num_requests": 101}, {"max_wait_s": 601}])
def test_correlated_runner_rejects_unsafe_limits_before_io(monkeypatch, kwargs):
    from scripts.dev import measure_e2e_freshness as freshness

    def unexpected(*args, **kwargs):
        raise AssertionError("unsafe configuration reached I/O")

    monkeypatch.setattr(freshness, "http_get", unexpected)
    with pytest.raises(ValueError):
        freshness.measure_correlated_freshness(
            {"service_id": "svc", "backend_url": "https://admin.example.test"},
            "https://edge.example.test/",
            **kwargs,
        )


# ── Merged from test_scale_harness.py ──────────────────────────────────────────────────────

from datetime import UTC, datetime

import pytest

from scripts.dev.scale_harness import (
    TrafficProfile,
    build_request,
    build_request_url,
    freshness_lag_seconds,
    parse_stages,
    render_report,
)
from scripts.lib.log_factory import BOT_USER_AGENTS, build_cmcd_request, build_cmcd_sessions


def test_parse_stages_and_high_rate_gate() -> None:
    stages = parse_stages("100:10,500:2")
    assert [(stage.target_rps, stage.duration_s) for stage in stages] == [
        (100, 10),
        (500, 2),
    ]


def test_parse_stages_rejects_high_rate_without_explicit_flag() -> None:
    with pytest.raises(ValueError, match="allow-high-rate"):
        parse_stages("2500:10")


def test_parse_stages_accepts_high_rate_with_explicit_flag() -> None:
    stages = parse_stages("2500:10", allow_high_rate=True)
    assert stages[0].target_rps == 2500


def test_rum_urls_include_vcl_identity_parameters() -> None:
    url = build_request_url("https://logs.example.com", 1.0)
    query = parse_qs(urlparse(url).query)
    assert query["cid"][0] == query["rum_cid"][0]
    assert query["req"][0].startswith("req-")
    assert query["req"][0] != query["cid"][0]


def test_freshness_lag_is_separate_from_missing_data() -> None:
    now = datetime(2026, 9, 6, 17, 42, 35, tzinfo=UTC)
    assert freshness_lag_seconds("2026-09-06T17:32:08Z", now) == 627.0
    assert freshness_lag_seconds(None, now) is None


def test_render_report_does_not_turn_missing_lag_into_zero() -> None:
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


def test_cmcd_requests_carry_headers_and_matching_query() -> None:
    rng = random.Random(7)
    url, headers = build_request("https://logs.example.com", 0.0, rng, _cmcd_profile(rng))
    parsed = urlparse(url)
    assert parsed.path.startswith("/video/")
    cmcd = parse_qs(parsed.query)["CMCD"][0].split(",")
    assert {"CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"} <= headers.keys()
    for header in ("CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"):
        assert set(headers[header].split(",")) <= set(cmcd)
    assert any(field.startswith("sid=") for field in cmcd)
    assert any(field.startswith("ot=") for field in cmcd)


def test_cmcd_startup_flag_only_on_first_request_per_session() -> None:
    rng = random.Random(3)
    session = build_cmcd_sessions(rng, 1)[0]
    session.version = 1
    first = build_cmcd_request(session, rng)[1]["CMCD-Request"].split(",")
    second = build_cmcd_request(session, rng)[1]["CMCD-Request"].split(",")
    assert "su" in first
    assert "su" not in second


def test_cmcd_v2_booleans_use_rfc8941_syntax() -> None:
    rng = random.Random(3)
    session = build_cmcd_sessions(rng, 1)[0]
    session.version = 2
    assert "su=?1" in build_cmcd_request(session, rng)[1]["CMCD-Request"].split(",")


def test_cmcd_ratio_mixes_playback_with_plain_requests() -> None:
    rng = random.Random(11)
    profile = TrafficProfile(cmcd_ratio=0.5, sessions=build_cmcd_sessions(rng, 5))
    cmcd_hits = sum("CMCD" in build_request("https://x.example.com", 0.0, rng, profile)[0] for _ in range(400))
    assert 120 < cmcd_hits < 280


def test_default_profile_keeps_single_url_and_no_extra_headers() -> None:
    assert build_request("https://x.example.com", 0.0, random.Random(1), None) == ("https://x.example.com", {})
    assert build_request("https://x.example.com", 0.0, random.Random(1), TrafficProfile()) == (
        "https://x.example.com",
        {},
    )


def test_diversify_varies_paths_and_includes_bot_user_agents() -> None:
    rng = random.Random(5)
    profile = TrafficProfile(diversify=True)
    results = [build_request("https://x.example.com", 0.0, rng, profile) for _ in range(300)]
    assert len({urlparse(url).path for url, _ in results}) > 5
    assert any(ua in BOT_USER_AGENTS for ua in (headers["User-Agent"] for _, headers in results))


def test_rum_beacons_never_get_cmcd_or_diversified() -> None:
    rng = random.Random(2)
    profile = TrafficProfile(cmcd_ratio=1.0, diversify=True, sessions=build_cmcd_sessions(rng, 3))
    url, headers = build_request("https://x.example.com", 1.0, rng, profile)
    assert urlparse(url).path == "/rum-beacon"
    assert headers == {}


# ── Merged from test_verify_dashboard.py ──────────────────────────────────────────────────────

"""Guards the Elevation port-forward blip investigation instrumentation.

The Remote High-Scale (Elevation) RUM-30d check intermittently hits a handful
of transient console/network blips (ERR_CONNECTION_RESET et al) even though
measurement showed the bound pod never restarts and the harness healer never
respawns the tunnel -- the reset happens inside an otherwise-healthy
port-forward. Diagnosing that needs wall-clock-correlatable logging here
(timestamps on every console/network event, plus Playwright's own
request-failure detail) so a future run's browser-side log lines can be lined
up second-for-second against the harness's port-forward stderr capture.
"""

import re
from pathlib import Path

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


def _src() -> str:
    return _VERIFY_JS.read_text()


def test_console_log_lines_are_timestamped() -> None:
    src = _src()
    assert re.search(r"function ts\(\)", src), (
        "verify_dashboard.js must define a ts() timestamp helper so console/network "
        "log lines can be correlated against the harness's port-forward logs"
    )
    assert "`${ts()} [Browser Console]" in src, "[Browser Console] log lines must be timestamped"


def test_request_failures_are_logged_with_error_detail() -> None:
    src = _src()
    assert "requestfailed" in src, (
        "verify_dashboard.js must listen for Playwright's 'requestfailed' event -- "
        "it carries the exact URL and error code independent of whatever text the "
        "page happened to console.log, which is the cleaner signal for diagnosing "
        "which upstream (frontend vs backend port-forward) actually dropped"
    )


# ── Merged from test_verify_dashboard_nav_resilience.py ──

from pathlib import Path


def test_defines_a_bounded_navigation_retry_helper() -> None:
    src = _src()
    assert "gotoWithShellReady" in src, (
        "verify_dashboard.js must define a bounded navigation retry helper "
        "(gotoWithShellReady) so a slow shell paint under verify-phase load is "
        "retried, not instant-failed"
    )
    m = re.search(r"attempts\s*=\s*(\d+)", src)
    assert m and 2 <= int(m.group(1)) <= 6, "the navigation retry helper must retry a small bounded number of times"


def test_rum_30d_navigation_shell_wait_tolerates_load() -> None:
    # The RUM 30d shell wait must allow more than the old tight 10s, which
    # flapped under the verify-phase CPU spike.
    src = _src()
    assert "gotoWithShellReady(rumPage, rumUrl30d" in src, "RUM 30d navigation must go through the bounded retry helper"
    # The 30d path must not keep the old single-shot direct goto (the retry
    # helper owns navigation now). Other RUM sections keep their 10s shell wait
    # because they already sit inside 4-attempt retry loops.
    assert not re.search(r"rumPage\.goto\(rumUrl30d", src), "RUM 30d must not keep the un-retried single-shot goto"


def test_network_30d_navigation_tolerates_portforward_resets() -> None:
    # The Network 30d step issues a raw goto inside its retry loop. On the K8s
    # high-scale env the port-forward drops mid-navigation (net::ERR_CONNECTION_RESET
    # / goto Timeout), so the page never loads and the step falsely reports
    # "No data available" from a blank page even though the network-health
    # backend returns has_data=True in ~0.4s. Navigation must go through the
    # bounded retry helper (which absorbs a reset with backoff before giving
    # up) exactly like RUM 30d.
    src = _src()
    assert "gotoWithShellReady(networkPage, networkUrl30d" in src or (
        "gotoWithShellReady(routePage, targetUrl" in src and "routePath: '/network'" in src
    ), (
        "Network 30d navigation must go through the bounded retry helper so a "
        "transient port-forward reset is retried, not reported as missing data"
    )
    assert not re.search(r"networkPage\.goto\(networkUrl30d", src), (
        "Network 30d must not keep the un-retried single-shot goto that dies on a port-forward reset"
    )


def test_rum_24h_vitals_render_is_polled_not_blind_slept() -> None:
    # The RUM 24h step used a blind waitForTimeout(4000) before reading the
    # body, then declared vitals "missing" if the heavy Plotly vitals cards had
    # not painted within that fixed 4s. Under the verify-phase CPU spike (4 envs
    # rendering in parallel on a shared 6-CPU Colima) the data is present and
    # the analytics endpoint answers in ~0.5s, but the client render lags past
    # 4s and the step false-fails. It must POLL for the vitals title+rating to
    # actually render (returning immediately once painted) with a generous
    # bounded timeout, not sleep a fixed 4s and hope.
    src = _src()
    m = re.search(r"RUM_RENDER_SETTLE_MS\s*=\s*(\d+)", src)
    assert m, "verify_dashboard.js must define a named RUM_RENDER_SETTLE_MS render-poll budget"
    assert int(m.group(1)) >= 8000, (
        "the RUM vitals render-poll budget must be generous enough to absorb the verify-phase CPU spike (>=8s)"
    )
    assert "RUM_RENDER_SETTLE_MS" in src and src.count("waitForFunction") >= 1, (
        "RUM 24h must waitForFunction-poll for the vitals render using RUM_RENDER_SETTLE_MS"
    )
    assert not re.search(r"rumPage\.waitForTimeout\(4000\)", src), (
        "RUM 24h must not keep the blind 4s settle that false-fails vitals render under load"
    )


# ── Merged from test_verify_dashboard_recency.py ──

from pathlib import Path

# Minimum window that reliably absorbs edge->FOS delivery + verify-phase CPU
# jitter without flapping. Not a pipeline-latency floor — ingest is real-time.
_MIN_RECENT_MINUTES = 10


def _recent_range_token() -> str:
    """The shared ``RECENT_RANGE`` constant value (e.g. ``"15m"``)."""
    src = _VERIFY_JS.read_text()
    m = re.search(r'RECENT_RANGE\s*=\s*"(\d+m)"', src)
    assert m, 'verify_dashboard.js must define a RECENT_RANGE = "<n>m" constant'
    return m.group(1)


def _recent_range_reference_count() -> int:
    """How many absolute-window checks use the shared ``RECENT_RANGE``."""
    src = _VERIFY_JS.read_text()
    return len(re.findall(r"getRecentWindow\([^,\n]+,\s*RECENT_RANGE\b", src))


def _minutes(token: str) -> int:
    return int(token[:-1])


def test_verify_dashboard_has_two_recency_checks() -> None:
    count = _recent_range_reference_count()
    assert count >= 2, (
        "expected at least two recency checks (Dashboard-recent + RUM-recent) "
        f"to use the shared RECENT_RANGE constant; found {count}"
    )


def test_recency_window_is_symmetric_across_requests_and_rum() -> None:
    # A single shared constant is symmetric by construction, but assert there
    # is no stray literal minute window that would reintroduce asymmetry.
    src = _VERIFY_JS.read_text()
    stray = re.findall(r'(?:getRecentWindow\([^,\n]+,|"range",)\s*"(\d+m)"', src)
    assert not stray, (
        "recency windows must flow through the shared RECENT_RANGE constant; "
        f"found hardcoded minute window(s) {stray!r}"
    )


def test_recency_window_absorbs_edge_delivery_jitter() -> None:
    token = _recent_range_token()
    assert _minutes(token) >= _MIN_RECENT_MINUTES, (
        f"recency window {token!r} is below the {_MIN_RECENT_MINUTES}m jitter "
        "cushion — edge->FOS delivery batching + verify-phase CPU spikes can "
        "briefly empty a window this short and flap the check"
    )


def test_verifier_does_not_claim_a_multiminute_pipeline_floor() -> None:
    # Ingest is near-real-time (measured ~1-8s buffer-stitched, 2026-10-01).
    # The recency window is a jitter cushion, NOT a pipeline latency floor.
    # A comment asserting a multi-minute "floor" is a false premise that
    # misleads future readers into widening the window to mask staleness.
    src = _VERIFY_JS.read_text()
    assert not re.search(r"\d+\s*-\s*\d+\s*min.*floor|floor.*\d+\s*-\s*\d+\s*min", src), (
        "verify_dashboard.js must not describe the recency window as a "
        "multi-minute pipeline floor — ingest is real-time; the window is a "
        "jitter cushion (see module docstring)"
    )


# ── Merged from test_verify_dashboard_resource_classification.py ──

from pathlib import Path


def test_generic_console_resource_failure_echo_is_not_fatal() -> None:
    src = _src()
    assert re.search(r"GENERIC_RESOURCE_FAILURE_RE", src), (
        "verify_dashboard.js must recognize Chrome's generic, URL-less "
        "'Failed to load resource: ...' console echo and not route it through "
        "failOrTolerate -- it carries no URL, so it cannot be reliably "
        "classified as critical vs. cosmetic from its text alone"
    )


def test_critical_resource_classification_is_url_based() -> None:
    src = _src()
    assert "function isCriticalResourceUrl" in src, (
        "verify_dashboard.js must classify a failed resource as critical "
        "(fails the run) vs. cosmetic (logged only) by its actual request "
        "URL (/api/, /_next/data/), not by matching filename substrings in "
        "console text, which breaks for content-hashed asset names"
    )


def test_requestfailed_listener_fails_only_critical_urls() -> None:
    # A connection-level failure (ERR_CONNECTION_RESET etc.) never reaches the
    # 'response' listener at all, since no response was ever received -- the
    # requestfailed listener must be the one applying the critical-vs-cosmetic
    # URL check for that class, or a reset API call could silently pass.
    src = _src()
    assert re.search(r"requestfailed.*?isCriticalResourceUrl", src, re.S), (
        "the 'requestfailed' listener must gate failOrTolerate on "
        "isCriticalResourceUrl(request.url()), so a connection-level reset "
        "on a real API call still fails the run while a static-asset reset "
        "does not"
    )


def test_woff2_substring_filter_removed_in_favor_of_url_classification() -> None:
    # The old substring filter is no longer needed (and would be redundant/
    # misleading) now that classification happens on the actual URL -- its
    # presence would suggest the text-based guess is still load-bearing.
    src = _src()
    assert "text.includes('woff2')" not in src, (
        "the filename-substring 'woff2' check should be removed now that "
        "static-asset vs. API classification happens via isCriticalResourceUrl "
        "on the real request URL, not by guessing from console text"
    )


def test_telemetry_beacons_are_exempt_from_critical_resource_urls() -> None:
    # Client-side observational telemetry beacons (/api/web-vitals, /api/ux-events)
    # are non-critical: dropped beacons under load do not compromise data correctness.
    src = _src()
    assert "/api/web-vitals" in src and "/api/ux-events" in src, (
        "isCriticalResourceUrl in verify_dashboard.js must exempt /api/web-vitals and /api/ux-events"
    )


# ── Merged from test_verify_dashboard_rum_charts.py ──

import subprocess
from pathlib import Path

import pytest

_SOURCE = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"


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
    source = _SOURCE.read_text()
    start = source.index("function hasPopulatedRumChart()")
    end = source.index("\n}", start) + 2
    script = (
        source[start:end]
        + "\nconst charts = "
        + json.dumps(charts)
        + ";\nglobal.document = {querySelectorAll: selector => {"
        + "if (selector !== 'main .js-plotly-plot') throw Error('Unscoped chart selector');"
        + "return charts.map(c => ({data:c.data,getBoundingClientRect:()=>c}));}};"
        + "\nconsole.log(JSON.stringify(hasPopulatedRumChart()));"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) is expected


def test_remote_rum_chart_failure_cannot_warn_and_proceed() -> None:
    source = _SOURCE.read_text()
    assert "waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 })" in source
    assert "No populated full-sized RUM chart" in source
    assert "No visible Plotly charts found on the RUM page for remote cloud environment" not in source


# ── Merged from test_verify_dashboard_transient_tolerance.py ──

from pathlib import Path

_CLASSIFIER_JS = Path(__file__).resolve().parents[2] / "scripts" / "lib" / "console_blip.js"


def _classifier_src() -> str:
    return _CLASSIFIER_JS.read_text()


def test_defines_a_small_bounded_transient_budget() -> None:
    src = _src()
    m = re.search(r"TRANSIENT_ERROR_BUDGET\s*=\s*(\d+)", src)
    assert m, "verify_dashboard.js must define a TRANSIENT_ERROR_BUDGET constant"
    budget = int(m.group(1))
    assert 1 <= budget <= 20, (
        f"TRANSIENT_ERROR_BUDGET={budget} must be small/bounded so a persistent outage still exhausts it and fails fast"
    )


def test_503_is_treated_as_transient_not_instant_fatal() -> None:
    # The response listener must route 503 (documented self-healing cold-start /
    # rollout window) through the bounded-transient path, NOT instant-exit.
    src = _src()
    assert re.search(r"status\s*===\s*503", src), (
        "verify_dashboard.js must special-case status 503 as a transient self-healing condition (AGENTS.md Trap #35)"
    )


def test_connection_refused_is_treated_as_transient() -> None:
    src = _src()
    assert "ERR_CONNECTION_REFUSED" in src, (
        "verify_dashboard.js must recognize net::ERR_CONNECTION_REFUSED as a "
        "transient K8s port-forward blip, not an instant-fatal console error"
    )


def test_non_503_api_errors_remain_instant_fatal() -> None:
    # A 4xx (auth/tenancy) or a persistent 500/502/504 must still fail the run —
    # tolerance is scoped to 503 only on the response path.
    src = _src()
    # The listener still fails on >= 400; assert it does not blanket-tolerate
    # every >= 400 as transient (which would mask real auth/5xx regressions).
    assert not re.search(r"status\s*>=\s*400[^\n]*transient\s*=\s*true", src), (
        "all >=400 responses must not be blanket-marked transient; only 503 is"
    )


def test_err_failed_is_treated_as_transient() -> None:
    # A heavy 30d page load whose in-flight request is aborted/reset during the
    # parallel-verify load spike surfaces in Chromium as the generic
    # net::ERR_FAILED rather than ERR_ABORTED/ERR_CONNECTION_REFUSED. It is the
    # same self-healing class the dashboard path already tolerates; RUM must get
    # the same bounded tolerance or it alone goes red on an identical blip.
    classifier = _classifier_src()
    assert "ERR_FAILED" in classifier, (
        "console_blip.js isTransientConsoleBlip must recognize net::ERR_FAILED "
        "so RUM gets the same bounded-transient tolerance as the dashboard path"
    )


def test_chunk_load_error_is_treated_as_transient() -> None:
    # A dropped kubectl port-forward refuses the chunk fetch; Next.js surfaces
    # that identical network failure as a ChunkLoadError, which the healer's
    # tunnel re-establish resolves within seconds. It is the same self-healing
    # class as ERR_CONNECTION_REFUSED (its direct cause), so it must get the same
    # bounded tolerance instead of instant-failing the whole env.
    classifier = _classifier_src()
    assert "ChunkLoadError" in classifier or "Failed to load chunk" in classifier, (
        "console_blip.js isTransientConsoleBlip must recognize ChunkLoadError / "
        "'Failed to load chunk' as the same transient port-forward blip as "
        "ERR_CONNECTION_REFUSED (its direct cause)"
    )


def test_verify_js_uses_shared_classifier() -> None:
    # The classifier is extracted to a single requireable module so the pure
    # classification can be unit-tested; verify_dashboard.js must consume it
    # rather than re-inlining a drifting copy of the regex.
    src = _src()
    assert "require('./lib/console_blip')" in src or 'require("./lib/console_blip")' in src, (
        "verify_dashboard.js must import isTransientConsoleBlip from the shared "
        "scripts/lib/console_blip.js module (single source of truth)"
    )


def _verify_node(script: str) -> dict:
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _frontend_node_runtime() -> str:
    # Transpile/read the real store, URL hydrator, range resolver and date
    # formatter in memory. No frontend source edits, browser, server or network.
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
  const filename = frontendPath.resolve(frontendRoot, relativePath);
  if (modules.has(filename)) return modules.get(filename).exports;
  const module = {exports: {}};
  modules.set(filename, module);
  const code = ts.transpileModule(frontendFs.readFileSync(filename, 'utf8'), {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022},
    fileName: filename,
  }).outputText;
  frontendVm.runInNewContext(code, {
    module, exports: module.exports, Date: Clock, window: frontendWindow, URL, URLSearchParams, console,
    require(name) {
      if (name.startsWith('@/')) return loadFrontend(name.slice(2) + '.ts');
      return frontendRequire(name);
    },
  }, {filename});
  return module.exports;
}
function resolveNavigation(href) {
  frontendWindow.location = {href, search: new URL(href).search};
  const {useFilterStore} = loadFrontend('stores/filterStore.ts');
  const hydration = loadFrontend('lib/urlFilterHydration.ts');
  useFilterStore.getState().resetAll();
  hydration._resetUrlHydrationFlag();
  hydration.hydrateFilterStoreFromUrl();
  const state = useFilterStore.getState();
  return loadFrontend('lib/range-wire.ts').resolveRangeWire({...state, anchor: state.endTime});
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
        'const RECENT_RANGE = "15m"; const messages = [];'
        "const console = {log: msg => messages.push(msg)};\n"
        + helpers
        + f"\nlet fresh = true; try {{ assertRecentHeader({json.dumps(header)}, {json.dumps(stream)}); }}"
        + " catch { fresh = false; } process.stdout.write(JSON.stringify({fresh, messages}));"
    )
    assert result["fresh"] is fresh
    assert bool(result["messages"]) is fresh
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
        + " catch { fresh = false; } console.log = originalLog;"
        + "process.stdout.write(JSON.stringify({age, fresh}));"
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
        + ", {Date: Clock, URL});"
        + "\nconst wire = resolveNavigation(recentWindow.url);"
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
        ({"offset": 1001}, False),  # another equally recent 15m window is not this attempt
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
        + """
const opts = Object.assign({
  path: '/api/dashboard/bundle', origin: 'https://test.invalid',
  range: null, duration: 900000, offset: 1000, method: 'POST', ok: true,
}, """
        + json.dumps(overrides)
        + """);
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
        + "\n(async () => { const count = "
        + json.dumps(count)
        + """;
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
    # Execute the actual CLI entrypoint and every stage with an in-process
    # Playwright stand-in. No browsers, app processes, credentials, or traffic.
    options = {"flags": flags, "requestCount": request_count, "beacons": beacons, "age": age, "charts": charts}
    result = _verify_node(
        _frontend_node_runtime()
        + "\nconst vm = require('node:vm'); const source = "
        + json.dumps(_src())
        + "; const opts = "
        + json.dumps(options)
        + r""";
const visits = [], logs = [], resolvedRequests = [];
const chart = {data: [{y: [100]}], getBoundingClientRect: () => ({width: 500, height: 300})};
let currentUrl = '', pendingResponse = null, displayedBeacons = 1000;
const attempts = {};
const hasCharts = () => opts.charts !== false
  && (typeof opts.charts !== 'string' || new URL(currentUrl).pathname !== opts.charts);
const document = {
  body: {get innerText() {
    const age = stream => typeof opts.age === 'string' ? opts.age : opts.age[stream];
    return `REQUEST\nlatest:\n${age('REQUEST')}\ntotal: 999,999\nRUM\nlatest:\n${age('RUM')}\ntotal: 999,999\n`
      + `TOTAL BEACONS\n${displayedBeacons}\n`
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
      // Resolve what the app would actually send from THIS emitted URL:
      // actual URL hydrator -> actual Zustand store -> actual range resolver.
      const wire = resolveNavigation(url), body = wire.rangeBody;
      const rum = new URL(url).pathname === '/rum';
      const key = rum ? 'rum' : 'dashboard', attempt = attempts[key] || 0;
      attempts[key] = attempt + 1;
      const counts = rum ? opts.beacons : opts.requestCount;
      const count = Array.isArray(counts) ? counts[Math.min(attempt, counts.length - 1)] : counts;
      if (rum) displayedBeacons = count;
      const response = {
        url: () => 'https://test.invalid' + (rum
          ? '/api/services/test/rum/analytics?' + new URLSearchParams(body)
          : '/api/dashboard/bundle'),
        ok: () => true,
        request: () => ({method: () => rum ? 'GET' : 'POST', postDataJSON: () => body}),
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
  async evaluate(fn) { if (fn.toString().includes('performance.getEntries')) return {}; return fn(); },
  async close() {}, async screenshot() {},
};
const context = {
  newPage: async () => page, close: async () => {},
  request: {get: async () => ({ok: () => false})},
};
const browser = {newContext: async () => context, close: async () => {}};
const dependencies = {
  playwright: {chromium: {launch: async () => browser}},
  path: require('node:path'), child_process: {},
  './lib/admin_gateway_client.cjs': {adminGatewayContextOptions: () => ({})},
  './lib/section_measurements.cjs': {
    collectResponseMeasurements: () => ({snapshot: async () => [], gaps: () => [], close() {}}), budgets: {},
  },
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
        assert result["resolvedRequests"] == []
        assert all("start_time" not in url for url in result["visits"])
        assert "[Dashboard 15m] NOT RUN" in messages
        assert "latest event age" not in messages
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
            assert "range" not in query
            assert request["rangeToken"] is None
            assert request["body"] == {
                "start_time": query["start_time"][0],
                "end_time": query["end_time"][0],
            }
            assert request["accepted"] is True
            start, end = (datetime.fromisoformat(request["body"][key]) for key in ("start_time", "end_time"))
            assert (end - start).total_seconds() == 900
        if isinstance(request_count, list):
            for route in ("/dashboard", "/rum"):
                bounds = [
                    request["body"]["end_time"]
                    for request in result["resolvedRequests"]
                    if urlparse(request["navigation"]).path == route
                ]
                assert len(bounds) == 2
                assert bounds[0] < bounds[1]  # each retry advances the absolute selection
        assert "NOT RUN" not in messages
        if request_count is None:
            assert "UNVERIFIED" in messages
            assert "requested-window aggregate contains" not in messages


@pytest.mark.parametrize("no_traffic", ["0", "1"])
def test_verify_canonical_runner_propagates_no_traffic(no_traffic: str) -> None:
    runner = _VERIFY_JS.parent / "dev" / "deploy_test_all.sh"
    if not runner.exists():
        pytest.skip("Canonical deployment runner is operator-local and gitignored")
    source = runner.read_text()
    start = source.index("    local verify_flags=()")
    end = source.index('\n  } >"$9"', start)
    # Execute only the argument construction/call, with node replaced by a
    # recorder. Never source or run the deployment pipeline itself.
    script = (
        "node() { printf '%s\\n' \"$@\"; }\n"
        "validate_env() {\nlocal NO_TRAFFIC=$1 COMMIT_HASH=test\n"
        "set -- label x x x x https://test.invalid/dashboard standard local\n"
        + source[start:end]
        + "\n}\nvalidate_env "
        + no_traffic
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
