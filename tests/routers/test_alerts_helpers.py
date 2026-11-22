"""Unit, snapshot, and contract tests for shared alert preview/evaluation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from backend.models.common.bootstrap_alerts import (
    Alert,
    AlertEvaluateData,
    AlertEvaluateResponse,
    AlertPreviewData,
    AlertPreviewResponse,
)
from backend.repositories import assets as assets_repo
from backend.routers import alerts as alerts_mod


class _ScriptedClickHouseClient:
    """In-memory fake ClickHouse client returning scripted responses by SQL substring."""

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        return self._handler(sql, params)


def _make_alert(**overrides: Any) -> Alert:
    base: dict[str, Any] = {
        "id": "alert-1",
        "service_id": "svc-123",
        "name": "High 5xx",
        "category": "reliability",
        "metric": "5xx",
        "evaluation_type": "absolute",
        "evaluation_scope": "all",
        "operator": ">",
        "threshold": 10.0,
        "window_min": 5.0,
        "enabled": True,
    }
    base.update(overrides)
    return Alert(**base)


@pytest.mark.parametrize(
    ("metric", "status_codes", "expected_fragment"),
    [
        ("requests", None, "count()"),
        ("5xx", None, "countIf(toInt32OrZero(custom_fields['status']) >= 500)"),
        ("5xx_rate", None, "* 100.0 / count()"),
        ("4xx", None, ">= 400 AND toInt32OrZero(custom_fields['status']) <= 499"),
        ("4xx_rate", None, "* 100.0 / count()"),
        ("specific_status", [502, 503], "IN (502, 503)"),
        ("specific_status", None, "0"),
        ("specific_status_rate", [429], "IN (429)"),
        ("specific_status_rate", None, "0.0"),
        ("p95_latency", None, "quantile(0.95)(toFloat64OrNull(custom_fields['elapsed'])) / 1000.0"),
        ("latency_p95", None, "quantile(0.95)(toFloat64OrNull(custom_fields['elapsed'])) / 1000.0"),
        ("hit_rate", None, "startsWith(upper(custom_fields['cache']), 'HIT')"),
        ("bandwidth", None, "sum(toFloat64OrZero(custom_fields['resp_bytes']))"),
        ("throughput", None, "sum(toFloat64OrZero(custom_fields['resp_bytes']))"),
        ("ttfb", None, "quantile(0.95)(toFloat64OrNull(custom_fields['ottfb'])) / 1000.0"),
        ("req_size", None, "custom_fields['req_bytes']"),
    ],
)
def test_ch_metric_expr_covers_all_alert_metrics(
    metric: str, status_codes: list[int] | None, expected_fragment: str
) -> None:
    expr = alerts_mod._ch_metric_expr(metric, status_codes)
    assert expected_fragment in expr


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("all", ""),
        ("edge", " AND lower(custom_fields['edge']) IN ('true', '1')"),
        ("origin", " AND lower(custom_fields['edge']) IN ('false', '0')"),
    ],
)
def test_ch_scope_clause_contract(scope: str, expected: str) -> None:
    assert alerts_mod._ch_scope_clause(scope) == expected


def test_evaluate_alert_absolute_triggers_and_formats_webhook_snapshot() -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def handler(sql: str, params: dict[str, Any] | None) -> list[dict[str, Any]]:
        if "SELECT max(event_timestamp)" in sql:
            return [{"max_ts": now - timedelta(minutes=1), "cnt": 100}]
        return [{"val": 25.0, "req_count": 100}]

    client = _ScriptedClickHouseClient(handler)
    alert_dict = _make_alert(webhook_url="https://hooks.example.com/alert").model_dump()

    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=client),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
    ):
        fired, wh_url, payload, max_ts = alerts_mod._evaluate_alert(
            "svc-123", {"name": "prod-cdn"}, alert_dict, display_name="Prod CDN"
        )

    assert fired is True
    assert wh_url == "https://hooks.example.com/alert"
    assert max_ts == (now - timedelta(minutes=1)).isoformat()
    assert payload == {
        "text": (
            "🚨 *Fastly Alert Triggered*\n"
            "*Name:* High 5xx\n"
            "*Metric:* 5xx is 25.00 (Threshold: > 10.0)\n"
            "*Service:* Prod CDN\n"
            "*Manage:* https://manage.fastly.com/configure/services/svc-123\n"
            "*Window:* Last 5.0 minutes"
        )
    }


def test_evaluate_alert_guards_stale_data_cooldown_and_min_requests() -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    # 1. Stale data (> 30m old) never triggers
    def stale_handler(sql: str, _params: dict[str, Any] | None) -> list[dict[str, Any]]:
        return [{"max_ts": now - timedelta(minutes=31), "cnt": 100}]

    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_ScriptedClickHouseClient(stale_handler)),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
    ):
        assert alerts_mod._evaluate_alert("svc-123", {"name": "prod"}, _make_alert().model_dump()) == (
            False,
            None,
            None,
            None,
        )

    # 2. Cooldown (< 1h since last_triggered_at) suppresses re-trigger
    def fresh_handler(sql: str, _params: dict[str, Any] | None) -> list[dict[str, Any]]:
        if "SELECT max(event_timestamp)" in sql:
            return [{"max_ts": now - timedelta(minutes=1), "cnt": 100}]
        return [{"val": 25.0, "req_count": 100}]

    cooldown_alert = _make_alert(last_triggered_at=(now - timedelta(minutes=20)).isoformat()).model_dump()
    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_ScriptedClickHouseClient(fresh_handler)),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
    ):
        assert alerts_mod._evaluate_alert("svc-123", {"name": "prod"}, cooldown_alert) == (
            False,
            None,
            None,
            None,
        )

    # 3. Relative alert with < 50 requests in window is suppressed
    def low_vol_rel_handler(sql: str, params: dict[str, Any] | None) -> list[dict[str, Any]]:
        if "SELECT max(event_timestamp)" in sql:
            return [{"max_ts": now - timedelta(minutes=1), "cnt": 100}]
        if params and params["end"] < now - timedelta(minutes=10):
            return [{"val": 10.0, "req_count": 20}]
        return [{"val": 30.0, "req_count": 20}]

    rel_alert = _make_alert(
        evaluation_type="relative_increase",
        comparison_period_min=60.0,
        threshold=50.0,
    ).model_dump()
    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_ScriptedClickHouseClient(low_vol_rel_handler)),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
    ):
        assert alerts_mod._evaluate_alert("svc-123", {"name": "prod"}, rel_alert) == (False, None, None, None)


def test_evaluate_alert_zscore_triggers_and_validates_wire_model() -> None:
    now = datetime.now(UTC).replace(microsecond=0)

    def zscore_handler(sql: str, _params: dict[str, Any] | None) -> list[dict[str, Any]]:
        if "SELECT max(event_timestamp)" in sql:
            return [{"max_ts": now - timedelta(minutes=1), "cnt": 200}]
        if "stddevSamp(val)" in sql:
            return [{"mean_val": 10.0, "stddev_val": 2.0}]
        return [{"val": 20.0, "req_count": 100}]

    zs_alert = _make_alert(
        evaluation_type="anomaly_zscore",
        baseline_period_days=7,
        zscore_threshold=3.0,
        webhook_url="https://hooks.example.com/zs",
    ).model_dump()

    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_ScriptedClickHouseClient(zscore_handler)),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
    ):
        fired, wh_url, payload, max_ts = alerts_mod._evaluate_alert("svc-123", {"name": "prod"}, zs_alert)

    assert fired is True
    assert wh_url == "https://hooks.example.com/zs"
    assert max_ts is not None
    assert payload is not None and "5.00 (Threshold: > 3.0)" in payload["text"]

    # Validate AlertEvaluateResponse contract
    resp = AlertEvaluateResponse.with_telemetry(
        data=AlertEvaluateData(
            service_id="svc-123",
            evaluated=1,
            triggered=1,
            results=[
                {
                    "id": "alert-1",
                    "name": "High 5xx",
                    "metric": "5xx",
                    "triggered": fired,
                    "last_triggered_at": max_ts,
                }
            ],
            evaluated_at=now.isoformat(),
        )
    )
    assert resp.data.triggered == 1


def test_preview_alert_series_relative_naive_datetime_alignment_and_contract() -> None:
    # ClickHouse often returns naive UTC datetimes; hist_map must still align with safe_iso(times).
    t0 = datetime(2026, 11, 18, 12, 0, 0)
    t1 = datetime(2026, 11, 18, 12, 15, 0)
    comp_period = 60.0

    call_idx = {"n": 0}

    def handler(sql: str, _params: dict[str, Any] | None) -> list[dict[str, Any]]:
        call_idx["n"] += 1
        if call_idx["n"] == 1:
            return [{"ts": t0, "val": 12.0}, {"ts": t1, "val": 18.0}]
        return [
            {"ts": t0 - timedelta(minutes=comp_period), "val": 4.0},
            {"ts": t1 - timedelta(minutes=comp_period), "val": 6.0},
        ]

    alert = _make_alert(evaluation_type="relative_increase", comparison_period_min=comp_period)
    with (
        patch.object(alerts_mod, "_resolve_ch_client", return_value=_ScriptedClickHouseClient(handler)),
        patch.object(alerts_mod, "visible_publication_fence_sql", return_value="1=1"),
        patch.object(alerts_mod.svcconfig, "get_status", return_value={"latest_log_at": "2026-11-18T12:15:00Z"}),
    ):
        res = alerts_mod._preview_alert_series("svc-123", {"name": "prod"}, "prod", alert, 4)

    assert res == {
        "times": ["2026-11-18T12:00:00Z", "2026-11-18T12:15:00Z"],
        "values": [12.0, 18.0],
        "type": "relative",
        "hist_values": [4.0, 6.0],
    }
    validated = AlertPreviewResponse.with_telemetry(data=AlertPreviewData(**res))
    assert validated.data is not None and validated.data.hist_values == [4.0, 6.0]


def test_assets_repository_shim_and_evaluate_hook() -> None:
    import backend.repositories as repos_pkg

    assert assets_repo.evaluate_alert is None
    assert getattr(repos_pkg, "alerts", None) is assets_repo
    assert getattr(repos_pkg, "usage", None) is assets_repo
