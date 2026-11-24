import asyncio

import pytest

from backend import config
from backend.core import metadata
from backend.core.fastly import client as fastly_client
from backend.routers import usage


@pytest.mark.parametrize(("requests", "edge"), [(1000, None), (1000, 0), (1000, 800), (0, 0)])
@pytest.mark.parametrize("include_logging_id", [True, False])
def test_prefill_measures_selected_logging_service_without_inventing_missing_edges(
    monkeypatch, requests, edge, include_logging_id
):
    source = config.config_to_source({"service_id": "selected-service", "cdn_service_id": "archive-delivery-service"})
    if not include_logging_id:
        source.pop("logging_service_id")
    row = {"start_time": 1767225600, "requests": requests}
    if edge is not None:
        row["edge_requests"] = edge

    def stats(entity_id, granularity, from_ts, to_ts, metrics, api_key):
        if entity_id == "selected-service":
            return {"data": [row]}
        return {"data": [{"start_time": 1767225600, "requests": 50, "edge_requests": 0}]}

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda service_id: "test-token")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda key: key)
    monkeypatch.setattr(usage, "_fastly_api", stats)
    monkeypatch.setattr(fastly_client, "get_active_version", lambda *args: None)
    monkeypatch.setattr(config, "load_config", lambda *args: None)
    monkeypatch.setattr(config, "get_status", lambda *args: {"edge_ratio": 80})
    monkeypatch.setattr(config, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(config, "get_fastly_logging_service_id", lambda: "other-service")
    monkeypatch.setattr(metadata, "get_node_count_avg", lambda *args: None)
    monkeypatch.setattr(metadata, "get_rum_beacons_avg_per_day", lambda *args: None)
    result = asyncio.run(usage.prefill(source))
    assert result.requests_per_day == requests
    assert result.edge_requests_per_day == edge


def test_usage_historical_consumers_select_platform_metrics_fields(monkeypatch):
    source = {
        "name": "svc",
        "service_id": "svc",
        "logging_service_id": "logging-svc",
        "cdn_service_id": "cdn-svc",
    }
    calls = []

    def stats(*args):
        calls.append(args)
        metrics = args[4]
        if "object_storage_class_a_operations_count" in metrics:
            return {
                "data": [
                    {
                        "start_time": 1767225600,
                        "object_storage_class_a_operations_count": 5,
                        "object_storage_class_b_operations_count": 2,
                    }
                ]
            }
        return {"data": [{"start_time": 1767225600, "requests": 9, "bandwidth": 99, "log": 7}]}

    monkeypatch.setattr(usage, "get_fastly_api_key", lambda service_id: "test-token")
    monkeypatch.setattr(usage, "api_key_or_mock", lambda key: key)
    monkeypatch.setattr(usage, "_fastly_api", stats)
    monkeypatch.setattr(
        usage,
        "_get_log_activity",
        lambda *_args: {"data": [], "total_rows": 0, "total_bytes": 0, "granularity": "hour"},
    )

    ops = usage.usage_operations(
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T02:00:00Z",
        by="hour",
        source=source,
    )
    bandwidth = usage.usage_bandwidth(
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T02:00:00Z",
        by="hour",
        source=source,
    )
    usage.usage_log_activity(
        source=source,
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T02:00:00Z",
        by="hour",
    )

    assert ops.source == "local"
    assert bandwidth.total_bytes == 99
    assert calls[0][0] == "cdn-svc" and calls[0][4] == ("bandwidth", "requests")
    assert calls[1][0] == "logging-svc"
    assert calls[1][4] == ("requests", *fastly_client.FASTLY_LOG_FIELDS)


@pytest.mark.no_postgres
def test_prefill_rates_and_prefill_include_snapshot_interval_minutes(monkeypatch):
    source = config.config_to_source({"service_id": "snap-svc", "cdn_service_id": "cdn-svc"})
    monkeypatch.setattr(config, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(config, "get_status", lambda *args: {"edge_ratio": 75})
    monkeypatch.setattr(metadata, "get_node_count_avg", lambda *args: None)
    monkeypatch.setattr(metadata, "get_rum_beacons_avg_per_day", lambda *args: None)
    monkeypatch.setattr(usage, "get_fastly_api_key", lambda service_id: None)

    # Default (no config) -> 5 minutes
    monkeypatch.setattr(config, "load_config", lambda *args: None)
    rates_default = usage.prefill_rates(source)
    assert rates_default.snapshot_interval_minutes == 5
    full_default = asyncio.run(usage.prefill(source))
    assert full_default.snapshot_interval_minutes == 5

    # Configured custom interval -> 60 minutes
    monkeypatch.setattr(
        config,
        "load_config",
        lambda *args: {
            "service_id": "snap-svc",
            "provisioning": {"cron_sync": {"snapshot_interval_minutes": 60, "delete_after": True}},
        },
    )
    rates_custom = usage.prefill_rates(source)
    assert rates_custom.snapshot_interval_minutes == 60
    full_custom = asyncio.run(usage.prefill(source))
    assert full_custom.snapshot_interval_minutes == 60


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    "cfg",
    [
        None,
        "bad",
        {"provisioning": None},
        {"provisioning": "bad"},
        {"provisioning": {"cron_sync": None}},
        {"provisioning": {"cron_sync": "bad"}},
        *[
            {"provisioning": {"cron_sync": {"snapshot_interval_minutes": v}}}
            for v in (
                float("inf"),
                float("-inf"),
                float("nan"),
                True,
                False,
                15.9,
                60.5,
                1440.1,
                0,
                -5,
                10,
                "5m",
                [],
                {},
            )
        ],
    ],
)
def test_resolve_snapshot_interval_minutes_handles_malformed_configs_and_numerics(cfg):
    assert config.resolve_snapshot_interval_minutes(cfg) == 5


@pytest.mark.no_postgres
def test_resolve_snapshot_interval_minutes_requested_edge_and_valid_values():
    for bad in (float("inf"), float("-inf"), float("nan"), True, False, 15.9, 60.5):
        assert config.resolve_snapshot_interval_minutes({}, requested=bad) == 5  # type: ignore[arg-type]

    for valid, expected in ((5, 5), (15, 15), (30, 30), (60, 60), (360, 360), (1440, 1440), (15.0, 15), ("30", 30)):
        assert config.resolve_snapshot_interval_minutes({}, requested=valid) == expected  # type: ignore[arg-type]


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    "bad_cfg",
    [
        {
            "service_id": "s",
            "log_period": 60,
            "provisioning": {"cron_sync": {"snapshot_interval_minutes": float("inf")}},
        },
        {"service_id": "s", "log_period": 60, "provisioning": None},
    ],
)
def test_prefill_rates_survives_inf_and_null_provisioning_blocks(monkeypatch, bad_cfg):
    source = config.config_to_source({"service_id": "s", "cdn_service_id": "cdn-svc"})
    monkeypatch.setattr(config, "load_config", lambda *args: bad_cfg)
    monkeypatch.setattr(config, "get_status", lambda *args: {})
    monkeypatch.setattr(config, "load_usage_logging_config", lambda: {})
    monkeypatch.setattr(metadata, "get_node_count_avg", lambda *args: None)
    monkeypatch.setattr(metadata, "get_rum_beacons_avg_per_day", lambda *args: None)

    rates = usage.prefill_rates(source)
    assert rates.snapshot_interval_minutes == 5
    assert rates.log_period_seconds == 60


@pytest.mark.no_postgres
@pytest.mark.parametrize(
    ("fos_region", "global_rates", "expected_zone", "expected_rate"),
    [
        ("us-east-1", {}, "MBZ100", 0.08),
        ("eu-central-1", {}, "MBZ100", 0.08),
        ("uk-east-1", {}, "MBZ100", 0.08),
        ("jp-central-1", {}, "MBZ200", 0.12),
        ("au-east-1", {}, "MBZ200", 0.12),
        # Explicit Admin override wins over fos_region auto-derivation.
        ("us-east-1", {"billing_zone": "Global", "cdn_egress_rate_per_gb": 0.18}, "Global", 0.18),
    ],
)
def test_prefill_rates_auto_derives_billing_zone_from_fos_region(
    monkeypatch, fos_region, global_rates, expected_zone, expected_rate
):
    source = config.config_to_source({"service_id": "s", "cdn_service_id": "cdn-svc"})
    monkeypatch.setattr(config, "load_config", lambda *args: {"service_id": "s", "fos_region": fos_region})
    monkeypatch.setattr(config, "get_status", lambda *args: {})
    monkeypatch.setattr(config, "load_usage_logging_config", lambda: global_rates)
    monkeypatch.setattr(metadata, "get_node_count_avg", lambda *args: None)
    monkeypatch.setattr(metadata, "get_rum_beacons_avg_per_day", lambda *args: None)

    rates = usage.prefill_rates(source)
    assert rates.fos_region == fos_region
    assert rates.billing_zone == expected_zone
    assert rates.cdn_egress_rate_per_gb == pytest.approx(expected_rate)
