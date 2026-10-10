"""Contract test suite for Cron 12: alerts_evaluation_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/alerts-evaluation.md:
1. FLA_DEV_NO_CRONS=1 / dev_mode_no_crons() skips execution and scheduler registration.
2. Dynamic registration gating on _service_has_alerts(service_id) and cron_alerts.enabled.
3. Politeness gating (should_defer_cron) defers when active interactive queries are running.
4. Early exit when no enabled alerts exist, logging 'skipped' without opening DuckDB.
5. DuckDB metrics evaluation over lookback windows and query timing instrumentation (track_query category="alerts").
6. Stale log detection (>30 min) bails out without triggering alerts.
7. Resilience against per-alert evaluation failure (individual errors do not halt the loop).
8. Two-phase execution: writing last-triggered timestamps and exporting admin state BEFORE dispatching webhooks.
9. Notification dispatch resilience across legacy webhook, Slack mrkdwn, PagerDuty Events API v2, and generic HTTP.
10. Execution status contract: 'warning' on triggered alerts or webhook failures, 'success' otherwise, 'error' on unhandled exceptions.
11. Telemetry attribution, cron_progress SSE event emission, and duration finalization in guaranteed finally block.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.cron.jobs import metadata as metadata_job
from backend.cron.scheduler import Scheduler
from backend.repositories import alerts as alert_repo

# ── Fixtures & Test Helpers ───────────────────────────────────────────────────


def _sample_source(service_id: str = "svc-alerts-contract") -> dict:
    return {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "logging_service_id": f"log-{service_id}",
        "bucket": "test-fos-bucket",
        "access_level": "read_write",
        "provisioning": {
            "access_level": "read_write",
            "log_period": 60,
            "cron_alerts": {
                "enabled": True,
                "interval_seconds": 60,
            },
        },
    }


def _make_alert(
    service_id: str,
    alert_id: str = "alert-1",
    name: str = "5xx Error Spike",
    metric: str = "5xx",
    threshold: float = 2.0,
    operator: str = ">",
    enabled: bool = True,
    webhook_url: str | None = None,
    channels: list[dict] | None = None,
) -> dict:
    return {
        "id": alert_id,
        "service_id": service_id,
        "name": name,
        "category": "reliability",
        "metric": metric,
        "evaluation_type": "absolute",
        "operator": operator,
        "threshold": threshold,
        "window_min": 5,
        "comparison_period_min": None,
        "status_codes": None,
        "webhook_url": webhook_url,
        "channels": channels or [],
        "enabled": enabled,
        "evaluation_scope": "all",
    }


@pytest.fixture
def alert_db():
    """In-memory DuckDB table simulating recent request logs."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_svc_alerts_contract ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 50 successful requests within the last 2 minutes
    for i in range(50):
        rows.append((now - timedelta(seconds=i * 2), 200, True, 20.0, 50, "HIT", 1024, 200, 100))
    # 5 error requests within the last minute
    for i in range(5):
        rows.append((now - timedelta(seconds=i * 5), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    con.executemany("INSERT INTO logs_svc_alerts_contract VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


# ── 1. Kill-Switch Protection (FLA_DEV_NO_CRONS=1) ───────────────────────────


def test_contract_dev_mode_no_crons_skips_execution(monkeypatch):
    """Under FLA_DEV_NO_CRONS=1, _run_service_alerts_evaluation skips immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    mock_src = MagicMock()
    mock_get_alerts = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", mock_src),
        patch("backend.repositories.alerts.get_alerts", mock_get_alerts),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_src.assert_not_called()
    mock_get_alerts.assert_not_called()


def test_contract_dev_mode_no_crons_skips_scheduler_registration(monkeypatch):
    """Under FLA_DEV_NO_CRONS=1, Scheduler.start() only registers local-safe jobs; alerts is excluded."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    with patch.object(s, "_add_job") as mock_add:
        s.start()

    registered_ids = [call.kwargs.get("id") for call in mock_add.call_args_list]
    assert not any("alerts_evaluation" in (jid or "") for jid in registered_ids)


# ── 2. Dynamic Registration Gating & Rescheduling ─────────────────────────────


def test_contract_registration_gated_on_service_has_alerts(monkeypatch):
    """Job is only registered when the service has at least one configured alert."""
    s = Scheduler()
    seen_ids: set[str] = set()

    # Case A: 0 alerts -> not registered
    with patch("backend.cron.scheduler._service_has_alerts", return_value=False):
        s._register_alerts_evaluation_job("svc-test", seconds=60, seen_ids=seen_ids, enabled=True)
    assert "alerts_evaluation_svc-test" not in seen_ids
    assert "alerts_evaluation_svc-test" not in s._job_ids

    # Case B: >=1 alert -> registered with max_instances=1, coalesce=True, misfire_grace_time=60
    with (
        patch("backend.cron.scheduler._service_has_alerts", return_value=True),
        patch.object(s, "_add_job") as mock_add,
    ):
        s._register_alerts_evaluation_job("svc-test", seconds=60, seen_ids=seen_ids, enabled=True)

    assert "alerts_evaluation_svc-test" in seen_ids
    assert "alerts_evaluation_svc-test" in s._job_ids
    mock_add.assert_called_once()
    _, kwargs = mock_add.call_args
    assert kwargs["id"] == "alerts_evaluation_svc-test"
    assert kwargs["seconds"] == 60
    assert kwargs["max_instances"] == 1
    assert kwargs["coalesce"] is True
    assert kwargs["misfire_grace_time"] == 60


def test_contract_registration_gated_on_cron_alerts_enabled(monkeypatch):
    """When cron_alerts.enabled is False, registration is bypassed."""
    s = Scheduler()
    seen_ids: set[str] = set()

    with patch("backend.cron.scheduler._service_has_alerts", return_value=True):
        s._register_alerts_evaluation_job("svc-disabled", seconds=60, seen_ids=seen_ids, enabled=False)

    assert "alerts_evaluation_svc-disabled" not in seen_ids
    assert "alerts_evaluation_svc-disabled" not in s._job_ids


def test_contract_rescheduling_when_interval_seconds_changes():
    """When interval_seconds changes, existing job is rescheduled."""
    s = Scheduler()
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job.return_value = mock_job
    s._job_ids["alerts_evaluation_svc-resched"] = "alerts_evaluation_svc-resched"

    seen_ids: set[str] = set()
    with patch("backend.cron.scheduler._service_has_alerts", return_value=True):
        s._register_alerts_evaluation_job("svc-resched", seconds=30, seen_ids=seen_ids, enabled=True)

    assert "alerts_evaluation_svc-resched" in seen_ids
    mock_job.reschedule.assert_called_once_with("interval", seconds=30)


def test_contract_unregistration_when_all_alerts_deleted():
    """When a service has no alerts remaining, _sync_jobs omits it and unregisters the job."""
    s = Scheduler()
    service_id = "svc-del-alerts"
    job_id = f"alerts_evaluation_{service_id}"
    s._job_ids[job_id] = job_id
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job.return_value = mock_job

    cfg = {
        "service_id": service_id,
        "access_level": "read_write",
        "provisioning": {"cron_alerts": {"enabled": True, "interval_seconds": 60}},
    }

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.cron.scheduler._service_has_alerts", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value={"service_id": service_id}),
    ):
        s._sync_jobs()

    assert job_id not in s._job_ids
    s._sched.remove_job.assert_called_once_with(job_id)


# ── 3. Politeness Deferral (Interactive UI Query Protection) ───────────────────


def test_contract_politeness_deferral_when_active_requests_present():
    """When should_defer_cron returns True, evaluation defers without touching alerts or DB."""
    src = _sample_source()
    mock_get_alerts = MagicMock()
    mock_get_conn = MagicMock()
    mock_log_cron = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer,
        patch("backend.repositories.alerts.get_alerts", mock_get_alerts),
        patch("backend.core.duckdb.get_connection", mock_get_conn),
        patch("backend.core.duckdb.log_cron_run", mock_log_cron),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_defer.assert_called_once_with("alerts", "svc-alerts-contract")
    mock_get_alerts.assert_not_called()
    mock_get_conn.assert_not_called()
    mock_log_cron.assert_not_called()


# ── 4. Early Exit When No Enabled Alerts Configured ────────────────────────────


def test_contract_early_exit_when_no_alerts_configured():
    """When alerts list is empty, logs 'skipped' with summary and does not open DuckDB."""
    src = _sample_source()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=[]),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_conn.assert_not_called()
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "skipped"
    assert kwargs.get("summary") == "No alerts configured"


def test_contract_early_exit_when_all_alerts_disabled():
    """When all alerts are enabled=False, logs 'skipped' without opening DuckDB or evaluating."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", enabled=False),
        _make_alert("svc-alerts-contract", alert_id="a2", enabled=False),
    ]
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.repositories.alerts.evaluate_alert") as mock_eval,
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    mock_conn.assert_not_called()
    mock_eval.assert_not_called()
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "skipped"
    assert kwargs.get("summary") == "No alerts configured"


# ── 5. DuckDB Metrics Evaluation, Lookback Window & Query Timing ──────────────


def test_contract_duckdb_metrics_evaluation_and_query_attribution(alert_db):
    """Evaluates metrics against DuckDB with window_min and instruments query via track_query."""
    src = _sample_source("svc_alerts_contract")
    alert = _make_alert(
        "svc_alerts_contract",
        metric="5xx",
        threshold=2.0,
        operator=">",
        webhook_url="https://hooks.example.com/alerts",
    )

    tracked_queries = []
    from backend.utils import telemetry

    orig_track_query = telemetry.track_query

    def recording_track_query(con, sql, params, category="query"):
        tracked_queries.append((sql, category))
        return orig_track_query(con, sql, params, category)

    with patch("backend.repositories.alerts.track_query", side_effect=recording_track_query):
        fired, webhook_url, payload, max_ts = alert_repo.evaluate_alert(
            alert_db, src, alert, display_name="Test Service", service_id="svc_alerts_contract"
        )

    # 5 errors > threshold of 2.0 -> fired
    assert fired is True
    assert max_ts is not None
    assert payload is not None
    assert "text" in payload
    assert "*Name:* 5xx Error Spike" in payload["text"]
    assert "*Metric:* 5xx is 5.00 (Threshold: > 2.0)" in payload["text"]
    assert "*Service:* Test Service" in payload["text"]

    # Confirm track_query attribution was category "alerts"
    assert any(cat == "alerts" for _, cat in tracked_queries)


def test_contract_skips_evaluation_if_logs_are_stale():
    """If MAX(timestamp) is older than 30 minutes, evaluate_alert returns not fired."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_stale_svc (timestamp TIMESTAMPTZ, status INTEGER)")
    stale_ts = datetime.now(UTC) - timedelta(minutes=45)
    con.execute("INSERT INTO logs_stale_svc VALUES (?, 500)", (stale_ts,))

    src = {"name": "stale_svc", "service_id": "stale_svc"}
    alert = _make_alert("stale_svc", metric="5xx", threshold=0.0)

    try:
        fired, _, _, _ = alert_repo.evaluate_alert(con, src, alert)
        assert fired is False
    finally:
        con.close()


def test_contract_resilience_per_alert_eval_failure_continues():
    """An exception during evaluation of one alert does not prevent evaluation of subsequent alerts."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="broken-1", name="Broken Alert"),
        _make_alert("svc-alerts-contract", alert_id="healthy-2", name="Healthy Alert"),
    ]
    eval_order = []

    def fake_eval(con, s, alert, **kwargs):
        eval_order.append(alert["id"])
        if alert["id"] == "broken-1":
            raise RuntimeError("Corrupt SQL fragment in custom metric")
        return (True, None, None, "2026-01-01T00:00:00Z")

    fake_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=101),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.repositories.alerts.evaluate_alert", side_effect=fake_eval),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # Both alerts were attempted
    assert eval_order == ["broken-1", "healthy-2"]
    # Connection closed in finally
    fake_con.close.assert_called()


# ── 6. Two-Phase Execution (Debouncing Before Dispatch) ────────────────────────


def test_contract_two_phase_execution_timestamps_and_state_export_before_webhooks():
    """update_last_triggered and export_admin_state MUST execute strictly before any outbound HTTP POST."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", webhook_url="https://webhook.example/hook"),
    ]
    fake_con = MagicMock()
    execution_timeline: list[str] = []

    def mock_close():
        execution_timeline.append("duckdb_closed")

    fake_con.close.side_effect = mock_close

    def mock_update(sid, aid, ts):
        execution_timeline.append(f"update_timestamp:{aid}")

    def mock_export(sid):
        execution_timeline.append(f"export_admin_state:{sid}")

    def mock_post(*args, **kwargs):
        execution_timeline.append("http_post")
        return MagicMock(status_code=200)

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=102),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, "https://webhook.example/hook", {"test": "payload"}, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered", side_effect=mock_update),
        patch("backend.state_sync.export_admin_state", side_effect=mock_export),
        patch("httpx.post", side_effect=mock_post),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # Strict sequence:
    # 1. duckdb_closed
    # 2. update_timestamp:a1
    # 3. export_admin_state:svc-alerts-contract
    # 4. http_post
    assert execution_timeline == [
        "duckdb_closed",
        "update_timestamp:a1",
        "export_admin_state:svc-alerts-contract",
        "http_post",
    ]


# ── 7. Notification Dispatch Resilience & Channel Multi-Payloads ──────────────


def test_contract_notification_dispatch_channel_payloads_and_timeouts():
    """Dispatches bounded 5s requests across Slack, PagerDuty, generic webhook channels, and legacy webhook."""
    src = _sample_source()
    channels = [
        {"type": "slack", "url": "https://hooks.slack.com/services/test"},
        {"type": "pagerduty", "url": "https://events.pagerduty.com/v2/enqueue"},
        {"type": "webhook", "url": "https://custom.webhook.example/alert"},
    ]
    alerts = [
        _make_alert(
            "svc-alerts-contract",
            alert_id="a-multi",
            name="Latency High",
            metric="latency_p95",
            threshold=500.0,
            webhook_url="https://legacy.webhook.example/alert",
            channels=channels,
        ),
    ]
    fake_con = MagicMock()
    captured_posts: list[dict] = []

    def fake_post(url, **kwargs):
        captured_posts.append({"url": url, "kwargs": kwargs})
        resp = MagicMock()
        resp.status_code = 200
        return resp

    payload = {"alert_name": "Latency High", "metric": "latency_p95"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=103),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, "https://legacy.webhook.example/alert", payload, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("httpx.post", side_effect=fake_post),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # 4 outbound requests total (legacy webhook + Slack + PagerDuty + generic webhook)
    assert len(captured_posts) == 4
    for post in captured_posts:
        assert post["kwargs"].get("timeout") == 5

    # Check Legacy Webhook
    legacy_post = next(p for p in captured_posts if p["url"] == "https://legacy.webhook.example/alert")
    assert legacy_post["kwargs"]["json"] == payload

    # Check Slack payload structure
    slack_post = next(p for p in captured_posts if p["url"] == "https://hooks.slack.com/services/test")
    slack_blocks = slack_post["kwargs"]["json"]["blocks"]
    assert any("Alert Triggered: Latency High" in b.get("text", {}).get("text", "") for b in slack_blocks)
    fields = next(b["fields"] for b in slack_blocks if "fields" in b)
    field_texts = [f["text"] for f in fields]
    assert any("latency_p95" in ft for ft in field_texts)
    assert any("500.0" in ft for ft in field_texts)

    # Check PagerDuty Events API v2 payload structure
    pd_post = next(p for p in captured_posts if p["url"] == "https://events.pagerduty.com/v2/enqueue")
    pd_json = pd_post["kwargs"]["json"]
    assert pd_json["event_action"] == "trigger"
    assert pd_json["payload"]["severity"] == "critical"
    assert pd_json["payload"]["component"] == "latency_p95"

    # Check Webhook Channel payload
    webhook_post = next(p for p in captured_posts if p["url"] == "https://custom.webhook.example/alert")
    assert webhook_post["kwargs"]["json"] == payload


def test_contract_notification_failure_resilience_and_collection():
    """A network or timeout error on one channel/webhook does not block remaining dispatches and yields warning."""
    src = _sample_source()
    channels = [
        {"type": "slack", "url": "https://broken-slack.example"},
        {"type": "slack", "url": "https://working-slack.example"},
    ]
    alerts = [
        _make_alert(
            "svc-alerts-contract",
            alert_id="a1",
            webhook_url="https://broken-legacy.example",
            channels=channels,
        ),
    ]
    fake_con = MagicMock()
    attempted_urls: list[str] = []

    def mock_post(url, **kwargs):
        attempted_urls.append(url)
        if "broken" in url:
            raise RuntimeError("Connection timed out (5s)")
        resp = MagicMock()
        resp.status_code = 200
        return resp

    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=104),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, "https://broken-legacy.example", {"test": 1}, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("httpx.post", side_effect=mock_post),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # All three URLs were attempted
    assert attempted_urls == [
        "https://broken-legacy.example",
        "https://broken-slack.example",
        "https://working-slack.example",
    ]

    # Warning status logged with failure details in summary
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert "2 notification failures" in kwargs.get("summary", "")
    assert "broken-legacy" in kwargs.get("summary", "") or "webhook for" in kwargs.get("summary", "")


# ── 8. Execution Status Contract ──────────────────────────────────────────────


def test_contract_status_warning_when_alert_triggers():
    """When an alert triggers, status='warning' with rows_ingested=n_trig and files_downloaded=n_eval."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", name="Alert 1"),
        _make_alert("svc-alerts-contract", alert_id="a2", name="Alert 2"),
    ]
    fake_con = MagicMock()
    log_calls = []

    def mock_eval(con, s, alert, **kwargs):
        if alert["id"] == "a1":
            return (True, None, None, "2026-01-01T00:00:00Z")
        return (False, None, None, None)

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=105),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch("backend.repositories.alerts.evaluate_alert", side_effect=mock_eval),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("files_downloaded") == 2  # n_eval
    assert kwargs.get("rows_ingested") == 1  # n_trig
    assert "Evaluated 2 alerts. 1 alert triggered." in kwargs.get("summary", "")


def test_contract_status_success_when_no_alerts_trigger():
    """When no alerts trigger and no webhooks fail, status='success'."""
    src = _sample_source()
    alerts = [
        _make_alert("svc-alerts-contract", alert_id="a1", name="Alert 1"),
    ]
    fake_con = MagicMock()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=106),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(False, None, None, None),
        ),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 1
    assert kwargs.get("rows_ingested") == 0
    assert "Evaluated 1 alert. 0 alerts triggered." in kwargs.get("summary", "")


def test_contract_status_error_on_unhandled_exception():
    """When an unexpected exception occurs during Phase 2, status='error' with error_message."""
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1")]
    fake_con = MagicMock()
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=107),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw))),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, None, None, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered", side_effect=RuntimeError("Database I/O error")),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "Alerts evaluation failed: Database I/O error" in kwargs.get("summary", "")
    assert "traceback" in kwargs.get("error_message", "").lower()


# ── 9. Telemetry Attribution, Progress Events & Duration Finalization ─────────


def test_contract_progress_events_and_duration_finalization():
    """Emits start, status, and done progress events and finalizes duration in guaranteed finally block."""
    src = _sample_source()
    alerts = [_make_alert("svc-alerts-contract", alert_id="a1", name="Bandwidth Surge")]
    fake_con = MagicMock()
    progress_events = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=108),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, None, None, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("backend.cron_progress.start_progress") as mock_start_prog,
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch(
            "backend.cron.jobs.metadata._log_and_add_progress",
            side_effect=lambda run_id, sid, job_name, event: progress_events.append(event),
        ),
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        metadata_job._run_service_alerts_evaluation("svc-alerts-contract")

    # start_progress called with run_id=108, task="alerts"
    mock_start_prog.assert_called_once_with(108, service_id="svc-alerts-contract", task="alerts")

    # Progress events: status for trigger + done
    assert len(progress_events) == 2
    assert progress_events[0]["type"] == "status"
    assert "Alert triggered: Bandwidth Surge" in progress_events[0]["message"]
    assert progress_events[1]["type"] == "done"
    assert "Evaluated 1 alert" in progress_events[1]["message"]

    # end_progress and finalize_cron_duration called
    mock_end_prog.assert_called_once_with(108)
    mock_finalize.assert_called_once()


def test_contract_alerts_job_module_exports():
    """backend.cron.jobs.alerts exports all required alert evaluation entrypoints."""
    from backend.cron.jobs import alerts as alerts_job

    assert hasattr(alerts_job, "_run_service_alerts_evaluation")
    assert hasattr(alerts_job, "_post_alert_webhook")
    assert hasattr(alerts_job, "_post_slack_notification")
    assert hasattr(alerts_job, "_post_pagerduty_notification")
    assert alerts_job._run_service_alerts_evaluation is metadata_job._run_service_alerts_evaluation
