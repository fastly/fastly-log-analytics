"""RUM synthetic generators folded into scripts/lib/log_factory.py (S3).

Pins: same seed -> byte-identical output, the record/beacon data shapes the
ingest path and the edge VCL depend on, and that both historical call sites
(FOS seeding script, scale harness) draw only from the injected rng.
"""

from __future__ import annotations

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
