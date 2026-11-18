from datetime import UTC, datetime

import pytest

from backend.repositories import value as value_repo
from backend.repositories.value import fastly_reported_from_stats, fetch_fastly_reported

H10 = int(datetime(2026, 7, 23, 10, tzinfo=UTC).timestamp())
H11 = H10 + 3600
H12 = H11 + 3600


def _rec(start: int, **fields: int) -> dict:
    return {"start_time": start, **fields}


ALL_NODES = {
    "requests": 1000,
    "resp_body_bytes": 90_000,
    "resp_header_bytes": 10_000,
    "hits": 900,
    "edge_requests": 600,
    "edge_resp_body_bytes": 50_000,
    "edge_resp_header_bytes": 5_000,
    "edge_hit_requests": 540,
}


def test_sums_whole_hours_from_all_nodes() -> None:
    records = [_rec(H10, **ALL_NODES), _rec(H11, **ALL_NODES), _rec(H12, **ALL_NODES)]
    out = fastly_reported_from_stats(records, H10, H12, edge_only=False, sample_rate=100)
    assert out == {
        "window_start": "2026-07-23T10:00:00Z",
        "window_end": "2026-07-23T12:00:00Z",
        "requests": 2000,
        "bandwidth_bytes": 200_000,
        "hit_requests": 1800,
        "origin_offload_pct": 90.0,
        "sample_rate": 100,
        "edge_only": False,
    }


def test_edge_only_uses_edge_fields_and_window_ends_at_last_reported_hour() -> None:
    out = fastly_reported_from_stats([_rec(H10, **ALL_NODES)], H10, H12, edge_only=True, sample_rate=25)
    assert out is not None
    assert (out["window_end"], out["requests"], out["bandwidth_bytes"], out["hit_requests"]) == (
        "2026-07-23T11:00:00Z",
        600,
        55_000,
        540,
    )
    assert (out["origin_offload_pct"], out["sample_rate"], out["edge_only"]) == (90.0, 25, True)


def test_no_reported_hours_in_window_is_none() -> None:
    assert fastly_reported_from_stats([_rec(H12, **ALL_NODES)], H10, H12, edge_only=False, sample_rate=100) is None
    assert fastly_reported_from_stats([], H10, H12, edge_only=False, sample_rate=100) is None


@pytest.fixture
def stats_env(monkeypatch):
    calls: list[dict] = []
    payload: dict = {"status": "success", "data": [_rec(H10, **ALL_NODES), _rec(H11, **ALL_NODES)]}

    def fake_platform_metrics(**kw):
        calls.append(kw)
        if isinstance(payload.get("raise"), Exception):
            raise payload["raise"]
        return payload

    cfg = {"service_id": "logsvc", "fastly_api_key": "tok", "provisioning": {"sample_rate": 50, "edge_only": False}}
    monkeypatch.setattr("backend.core.fastly.client.fastly_platform_metrics", fake_platform_metrics)
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.get_fastly_api_key", lambda sid=None: cfg.get("fastly_api_key", ""))
    monkeypatch.setattr("backend.config.get_fastly_logging_service_id", lambda sid=None: cfg.get("service_id", ""))
    value_repo._FASTLY_REPORTED_CACHE.clear()
    yield calls, payload, cfg
    value_repo._FASTLY_REPORTED_CACHE.clear()


def test_fetch_asks_for_whole_closed_hours_once_and_caches(stats_env) -> None:
    calls, _, _ = stats_env
    now = datetime(2026, 7, 23, 12, 20, tzinfo=UTC)
    args = ("svc", "2026-07-23T09:30:00Z", "2026-07-23T13:00:00Z")
    out = fetch_fastly_reported(*args, now=now)
    assert out is not None and (out["requests"], out["sample_rate"]) == (2000, 50)
    assert len(calls) == 1
    kw = calls[0]
    assert kw["entity_id"] == "logsvc"
    assert kw["granularity"] == "hour"
    assert (kw["from_epoch"], kw["to_epoch"]) == (H10, H12)
    assert set(kw["metrics"]) == set(ALL_NODES)
    assert kw["token"] == "tok" and kw["timeout"] <= 10 and kw["max_retries"] <= 1
    assert fetch_fastly_reported(*args, now=now) == out
    assert len(calls) == 1


def test_fetch_clips_to_log_coverage(stats_env) -> None:
    calls, _, _ = stats_env
    now = datetime(2026, 7, 23, 13, 20, tzinfo=UTC)
    coverage_end = datetime(2026, 7, 23, 11, 40, tzinfo=UTC)
    out = fetch_fastly_reported(
        "svc", "2026-07-23T10:00:00Z", "2026-07-23T13:20:00Z", now=now, coverage_end=coverage_end
    )
    assert (calls[0]["from_epoch"], calls[0]["to_epoch"]) == (H10, H11)
    assert out is not None and out["window_end"] == "2026-07-23T11:00:00Z"


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-07-23T10:10:00Z", "2026-07-23T10:50:00Z"),
        ("2026-06-01T00:00:00Z", "2026-07-23T12:00:00Z"),
        (None, "2026-07-23T12:00:00Z"),
    ],
)
def test_fetch_skips_ranges_stats_cannot_answer(stats_env, start, end) -> None:
    calls, _, _ = stats_env
    assert fetch_fastly_reported("svc", start, end, now=datetime(2026, 7, 23, 13, tzinfo=UTC)) is None
    assert calls == []


def test_fetch_without_token_or_on_error_is_none(stats_env) -> None:
    calls, payload, cfg = stats_env
    now = datetime(2026, 7, 23, 13, tzinfo=UTC)
    payload["raise"] = RuntimeError("401 Unauthorized")
    assert fetch_fastly_reported("svc", "2026-07-23T10:00:00Z", "2026-07-23T12:00:00Z", now=now) is None
    cfg["fastly_api_key"] = ""
    calls.clear()
    assert fetch_fastly_reported("svc", "2026-07-23T10:00:00Z", "2026-07-23T11:00:00Z", now=now) is None
    assert calls == []
