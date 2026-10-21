from __future__ import annotations

import random
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

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
