"""Alert webhook / Slack / PagerDuty senders share one retry stack and keep their public names."""

from __future__ import annotations

import httpx
import pytest
import tenacity

from backend.cron.jobs import metadata


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(tenacity.nap.time, "sleep", lambda _s: None)


def _response(status: int) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("POST", "https://hook.example/x"))


def test_old_names_still_exist_and_share_the_json_sender():
    assert metadata._post_alert_webhook is metadata._post_json
    assert metadata._post_slack_notification is metadata._post_json
    assert callable(metadata._post_pagerduty_notification)


def test_retries_transient_5xx_then_succeeds(monkeypatch):
    seen = []
    codes = iter([503, 502, 200])

    def fake_post(url, json=None, timeout=None):
        seen.append((url, json, timeout))
        return _response(next(codes))

    monkeypatch.setattr(httpx, "post", fake_post)
    metadata._post_alert_webhook("https://hook.example/x", {"a": 1})
    assert len(seen) == 3
    assert seen[0] == ("https://hook.example/x", {"a": 1}, 5)


def test_does_not_retry_a_4xx(monkeypatch):
    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append(url)
        return _response(400)

    monkeypatch.setattr(httpx, "post", fake_post)
    metadata._post_slack_notification("https://hook.example/x", {"text": "hi"})
    assert len(calls) == 1


def test_gives_up_after_three_attempts_and_reraises(monkeypatch):
    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append(url)
        return _response(503)

    monkeypatch.setattr(httpx, "post", fake_post)
    with pytest.raises(httpx.HTTPStatusError):
        metadata._post_slack_notification("https://hook.example/x", {})
    assert len(calls) == 3


def test_pagerduty_builds_payload_and_is_not_double_retried(monkeypatch):
    bodies = []

    def fake_post(url, json=None, timeout=None):
        bodies.append(json)
        return _response(503)

    monkeypatch.setattr(httpx, "post", fake_post)
    with pytest.raises(httpx.HTTPStatusError):
        metadata._post_pagerduty_notification("https://pd.example/e", "HighErr", "error_rate", "> 5%", "svc")
    assert len(bodies) == 3
    assert bodies[0]["event_action"] == "trigger"
    assert bodies[0]["payload"]["summary"] == "Alert HighErr triggered on svc: error_rate > 5%"
    assert bodies[0]["payload"]["component"] == "error_rate"
