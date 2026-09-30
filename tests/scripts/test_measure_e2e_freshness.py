from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.dev.measure_e2e_freshness import (
    admin_token_from_environment,
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


def test_admin_token_uses_environment_only_and_rejects_missing_token():
    assert admin_token_from_environment({"ADMIN_SHARED_SECRET": "test-token"}) == "test-token"
    assert admin_token_from_environment({"REMOTE_ADMIN_TOKEN": "remote-token", "ADMIN_TOKEN": "fallback"}) == (
        "remote-token"
    )
    with pytest.raises(ValueError, match="Set REMOTE_ADMIN_TOKEN"):
        admin_token_from_environment({})


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
                "mode": "regular",
                "interval_seconds": 30,
                "interval_source": "log_period",
                "adaptive_followup_interval_seconds": None,
                "adaptive_max_followups": 0,
            },
        ),
        (
            {"log_period": 1, "cron_sync": {"interval_seconds": 2}},
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
