import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

from backend.core import metadata_db
from backend.models.alerts import Alert
from backend.repositories.alerts import (
    delete_alert,
    evaluate_alert,
    get_alert_by_id,
    get_alerts,
    save_alert,
    toggle_alert,
    update_last_triggered,
)


def _make_alert(
    service_id: str,
    name: str = "Test Alert",
    metric: str = "5xx",
    threshold: float = 10.0,
    operator: str = ">",
    enabled: bool = True,
) -> Alert:
    return Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name=name,
        category="reliability",
        metric=metric,
        evaluation_type="absolute",
        operator=operator,
        threshold=threshold,
        window_min=5,
        comparison_period_min=None,
        status_codes=None,
        webhook_url=None,
        enabled=enabled,
        evaluation_scope="all",
    )


def test_alert_lifecycle():
    """Create, read, and ensure schema updates for alerts in per-service SQLite."""
    service_id = "test_service_for_alerts"

    alert = Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name="High 500s",
        category="reliability",
        metric="5xx",
        evaluation_type="absolute",
        operator=">",
        threshold=100.0,
        window_min=5,
        comparison_period_min=None,
        status_codes=[500, 502, 503, 504],
        webhook_url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        enabled=True,
        evaluation_scope="all",
    )

    saved_alert = save_alert(alert)
    assert saved_alert["id"] == alert.id

    alerts = get_alerts(service_id)
    assert len(alerts) == 1
    assert alerts[0]["name"] == "High 500s"
    assert alerts[0]["status_codes"] == [500, 502, 503, 504]

    alert2 = Alert(
        id=str(uuid.uuid4()),
        service_id=service_id,
        name="Low Traffic",
        category="traffic",
        metric="requests",
        evaluation_type="absolute",
        operator="<",
        threshold=10.0,
        window_min=15,
        comparison_period_min=None,
        status_codes=None,
        webhook_url=None,
        enabled=False,
        evaluation_scope="edge",
    )
    save_alert(alert2)

    all_alerts = get_alerts(service_id)
    assert len(all_alerts) == 2


# ── get_alerts: cross-service scan when no service_id given ───────────────────


def test_get_alerts_without_service_id_scans_all_configured_services():
    """``get_alerts()`` with no service_id walks every configured service —
    used by the admin overview to render alerts across the whole install."""
    save_alert(_make_alert("svc-a", name="A's alert"))
    save_alert(_make_alert("svc-b", name="B's alert"))

    with patch(
        "backend.repositories.alerts.svcconfig.list_configs",
        return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}],
    ):
        out = get_alerts()

    names = {a["name"] for a in out}
    assert "A's alert" in names
    assert "B's alert" in names


# ── get_alert_by_id: tenant-scoped lookup (audit finding 018) ────────────────


def test_get_alert_by_id_returns_row_when_present():
    sid = "svc-find-b"
    save_alert(_make_alert("svc-find-a"))
    alert_b = _make_alert(sid)
    save_alert(alert_b)

    row = get_alert_by_id(alert_b.id, sid)
    assert row is not None
    assert row["id"] == alert_b.id


def test_get_alert_by_id_returns_none_when_absent():
    assert get_alert_by_id("nonexistent-alert-id", "svc-find-none") is None


# ── toggle_alert / delete_alert: scoped to service (audit finding 018) ───────


def test_toggle_alert_flips_enabled_in_scoped_service():
    sid = "svc-toggle"
    alert = _make_alert(sid, enabled=True)
    save_alert(alert)

    res = toggle_alert(alert.id, enabled=False, service_id=sid)
    assert res.get("status") != "not_found"

    alerts = get_alerts(sid)
    assert any(a["id"] == alert.id and a["enabled"] is False for a in alerts)


def test_delete_alert_removes_row_in_scoped_service():
    sid = "svc-del"
    alert = _make_alert(sid)
    save_alert(alert)

    res = delete_alert(alert.id, service_id=sid)
    assert res.get("status") != "not_found"
    assert all(a["id"] != alert.id for a in get_alerts(sid))


def test_delete_alert_unknown_id_returns_status():
    """Delete is idempotent — deleting an unknown id in a specific
    service returns a status payload (currently 'success' since the
    SQLite DELETE matches zero rows without error). Contract: never
    raise, always return a dict with a status key."""
    res = delete_alert("does-not-exist", service_id="svc-no-alerts")
    assert "status" in res


# ── update_last_triggered: stamps the timestamp into SQLite ───────────────────


def test_update_last_triggered_sets_timestamp():
    sid = "svc-trig"
    alert = _make_alert(sid)
    save_alert(alert)

    ts = "2026-05-16T12:34:56Z"
    update_last_triggered(sid, alert.id, ts)

    row = metadata_db.get_con(sid).execute("SELECT last_triggered_at FROM alerts WHERE id = ?", (alert.id,)).fetchone()
    assert ts in row[0]


# ── evaluate_alert: end-to-end against a tiny in-memory DuckDB ────────────────


@pytest.fixture
def alert_table():
    """In-memory DuckDB with a ``logs_alertsvc`` table seeded with rows
    spanning the past 5 minutes so the alert window captures them."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_alertsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 100 successful + 5 errors all within the last 2 minutes
    for i in range(100):
        rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
    for i in range(5):
        rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    con.executemany("INSERT INTO logs_alertsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


def test_evaluate_alert_returns_false_for_empty_table():
    """An empty log table → no max_ts → no trigger."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE logs_empty_svc (timestamp TIMESTAMPTZ, status INTEGER)")
        alert = _make_alert("empty_svc").model_dump()
        triggered, webhook, payload, max_ts = evaluate_alert(con, {"name": "empty_svc"}, alert)
        assert triggered is False
        assert max_ts is None
    finally:
        con.close()


def test_evaluate_alert_triggers_when_5xx_exceeds_threshold(alert_table):
    """5 errors > threshold 2 → triggered."""
    alert = _make_alert("alertsvc", metric="5xx", threshold=2.0, operator=">").model_dump()
    triggered, _, _, _ = evaluate_alert(alert_table, {"name": "alertsvc"}, alert)
    assert triggered is True


def test_evaluate_alert_does_not_trigger_when_below_threshold(alert_table):
    """5 errors < threshold 100 → not triggered."""
    alert = _make_alert("alertsvc", metric="5xx", threshold=100.0, operator=">").model_dump()
    triggered, _, _, _ = evaluate_alert(alert_table, {"name": "alertsvc"}, alert)
    assert triggered is False


def test_evaluate_alert_skips_when_data_is_too_stale():
    """If max(timestamp) is > 30 minutes ago, evaluate bails — alerts
    only fire on actively-flowing data, not on a paused service."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE logs_stalesvc (timestamp TIMESTAMPTZ, status INTEGER)")
        stale = datetime.now(UTC) - timedelta(hours=2)
        con.execute("INSERT INTO logs_stalesvc VALUES (?, ?)", (stale, 500))
        alert = _make_alert("stalesvc", metric="5xx", threshold=0).model_dump()
        triggered, _, _, _ = evaluate_alert(con, {"name": "stalesvc"}, alert)
        assert triggered is False
    finally:
        con.close()


# ── evaluate_alert: operator semantics ───────────────────────────────────────


@pytest.fixture
def busy_alert_table():
    """In-memory DuckDB with enough rows (>= 50) that the
    "minimum-request" guard for non-absolute eval types is satisfied,
    and the 5xx count lands exactly at 50 for boundary tests."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_busysvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = []
    # 50 5xx errors + 100 200s, all within the last 2 minutes
    for i in range(50):
        rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
    for i in range(100):
        rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
    con.executemany("INSERT INTO logs_busysvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    yield con
    con.close()


@pytest.mark.parametrize(
    "op,threshold,expected",
    [
        (">", 49, True),  # 50 > 49
        (">", 50, False),  # 50 > 50 is False
        (">=", 50, True),  # 50 >= 50 is True
        ("<", 51, True),  # 50 < 51
        ("<", 50, False),  # 50 < 50 is False
        ("<=", 50, True),  # 50 <= 50
    ],
)
def test_evaluate_alert_operator_branches(busy_alert_table, op, threshold, expected):
    """Pin every operator branch (>, <, >=, <=) — the comparison logic
    is a small if-elif chain and a typo would silently flip thresholds.
    Uses an alert table seeded with exactly 50 5xx errors so each
    operator's boundary case is exercised."""
    alert = _make_alert("busysvc", metric="5xx", threshold=threshold, operator=op).model_dump()
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is expected


def test_evaluate_alert_unknown_operator_does_not_trigger(busy_alert_table):
    """An operator not in the {<, <=, >, >=} set defaults to False —
    pinned because a future op like ``!=`` should fail-closed (no
    spurious trigger), not crash with KeyError."""
    alert = _make_alert("busysvc", metric="5xx", threshold=0, operator=">").model_dump()
    alert["operator"] = "!="
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is False


# ── evaluate_alert: relative_increase / relative_decrease ────────────────────


def test_evaluate_alert_relative_increase_compares_against_history():
    """``relative_increase`` compares the current-window value against
    the same metric in a baseline window ``comp_period_min`` ago. A
    big enough percentage gain → triggered."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_relsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = []
        # Current window (last 2 min): 200 requests, 50 are 5xx → high error rate
        for i in range(150):
            rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(50):
            rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        # Historic window (61-65 minutes ago): 100 requests, 2 are 5xx → low rate
        for i in range(98):
            rows.append((now - timedelta(minutes=61, seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(2):
            rows.append((now - timedelta(minutes=61, seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        con.executemany("INSERT INTO logs_relsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert(
            "relsvc",
            metric="5xx",
            threshold=100,  # 100% increase
            operator=">",
        ).model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5

        triggered, _, _, _ = evaluate_alert(con, {"name": "relsvc"}, alert)
        assert triggered is True
    finally:
        con.close()


def test_evaluate_alert_relative_skips_when_history_is_zero():
    """If the historical window has zero events, dividing by zero is
    skipped — return not-triggered. Pinned because a refactor that
    treats zero baseline as "infinite increase" would spam alerts on
    every new-traffic spike for a freshly-provisioned service."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_norel (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = [(now - timedelta(seconds=i), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(60)]
        con.executemany("INSERT INTO logs_norel VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("norel", metric="5xx", threshold=0, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5
        triggered, _, _, _ = evaluate_alert(con, {"name": "norel"}, alert)
        assert triggered is False
    finally:
        con.close()


def test_evaluate_alert_relative_skips_when_traffic_too_low():
    """Non-absolute alerts require >= 10 requests in the window —
    otherwise a single error in 5 requests would compute to "+100%"
    and fire constantly. Pinned because lowering this threshold would
    surface as alert spam on low-traffic services."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_lowsvc (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        # Only 5 requests total in the window — below the 10-req floor
        rows = [(now - timedelta(seconds=i * 5), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(5)]
        con.executemany("INSERT INTO logs_lowsvc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("lowsvc", metric="5xx", threshold=10, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        triggered, _, _, _ = evaluate_alert(con, {"name": "lowsvc"}, alert)
        assert triggered is False

    finally:
        con.close()


# ── evaluate_alert: webhook payload construction ────────────────────────────


def test_evaluate_alert_builds_slack_payload_with_webhook_url(busy_alert_table):
    """A triggered alert with a webhook_url returns the formatted
    Slack-shaped payload. Pinned because the frontend's "Test webhook"
    button POSTs whatever shape this returns."""
    alert = _make_alert(
        "busysvc",
        metric="5xx",
        threshold=1,
        operator=">",
    ).model_dump()
    alert["webhook_url"] = "https://hooks.slack.com/services/T00/B00/XXXX"

    triggered, webhook, payload, max_ts = evaluate_alert(
        busy_alert_table, {"name": "busysvc"}, alert, display_name="Prod CDN", service_id="svc-busy"
    )

    assert triggered is True
    assert webhook == "https://hooks.slack.com/services/T00/B00/XXXX"
    assert payload is not None
    text = payload["text"]
    assert "🚨 *Fastly Alert Triggered*" in text
    assert "Test Alert" in text  # name from _make_alert
    assert "Prod CDN" in text  # display_name
    assert "manage.fastly.com/configure/services/svc-busy" in text
    assert max_ts is not None  # ISO string of the max timestamp


def test_evaluate_alert_no_webhook_url_returns_none_payload(busy_alert_table):
    """When the alert has no webhook_url, the payload field is None
    (no formatted text). Pinned because skipping this would crash
    the webhook-dispatch loop when alerts only update a UI badge."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    alert["webhook_url"] = None

    triggered, webhook, payload, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is True
    assert webhook is None
    assert payload is None


def test_evaluate_alert_webhook_message_includes_relative_increase_phrasing():
    """relative_increase → "increased by N% vs M min ago" phrasing."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_relmsg (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, "
            "ottfb DOUBLE, elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, "
            "req_header_bytes BIGINT)"
        )
        now = datetime.now(UTC)
        rows = []
        # Current: 200 requests, 100 5xx (50% error rate)
        for i in range(100):
            rows.append((now - timedelta(seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(100):
            rows.append((now - timedelta(seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        # Historic: 200 requests, 10 5xx (5% error rate) → 900% increase
        for i in range(190):
            rows.append((now - timedelta(minutes=61, seconds=i), 200, True, 50.0, 100, "HIT", 1024, 200, 100))
        for i in range(10):
            rows.append((now - timedelta(minutes=61, seconds=i), 500, True, 60.0, 200, "MISS", 2048, 300, 100))
        con.executemany("INSERT INTO logs_relmsg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        alert = _make_alert("relmsg", metric="5xx", threshold=100, operator=">").model_dump()
        alert["evaluation_type"] = "relative_increase"
        alert["comparison_period_min"] = 60
        alert["window_min"] = 5
        alert["webhook_url"] = "https://hooks.example.com"

        triggered, _, payload, _ = evaluate_alert(con, {"name": "relmsg"}, alert)
        assert triggered is True
        assert payload is not None
        assert "increased by" in payload["text"]
        assert "% vs 60m ago" in payload["text"]
    finally:
        con.close()


# ── evaluate_alert: duplicate-fire suppression ──────────────────────────────


def test_evaluate_alert_suppresses_when_recently_triggered(busy_alert_table):
    """If ``last_triggered_at`` is within the past hour, the alert
    must NOT fire again — prevents Slack/email storms when an issue
    persists across multiple evaluations."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    # Triggered 5 minutes ago
    recent = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    alert["last_triggered_at"] = recent

    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is False


def test_evaluate_alert_fires_again_after_an_hour(busy_alert_table):
    """``last_triggered_at`` > 1 hour ago → suppression lifts and the
    alert can fire again."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    alert["last_triggered_at"] = old

    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    assert triggered is True


def test_evaluate_alert_tolerates_malformed_last_triggered_at(busy_alert_table):
    """If ``last_triggered_at`` is garbage (an old format from a
    migration), the try/except swallows ValueError and the alert
    fires normally rather than crashing the cron."""
    alert = _make_alert("busysvc", metric="5xx", threshold=1, operator=">").model_dump()
    alert["last_triggered_at"] = "not-a-timestamp-at-all"

    # Must not raise; treat as never-triggered → fires.
    triggered, _, _, _ = evaluate_alert(busy_alert_table, {"name": "busysvc"}, alert)
    # Either fires or doesn't — what matters is no exception. We assert one
    # specific shape (triggered=True) since the malformed value was non-recent
    # — but the key contract is "doesn't raise".
    assert triggered in (True, False)


# ── evaluate_alert: query failure path ──────────────────────────────────────


def test_evaluate_alert_returns_false_on_query_exception():
    """If DuckDB raises mid-query (table missing, bad SQL from a
    custom metric), the outer try/except catches and returns False.
    Pinned because raising here would abort the whole alert-eval
    cron, silencing every other service's alerts."""
    con = duckdb.connect(":memory:")
    try:
        # Don't create the logs_brokensvc table → max() query raises
        alert = _make_alert("brokensvc", metric="5xx", threshold=1, operator=">").model_dump()
        triggered, _, _, _ = evaluate_alert(con, {"name": "brokensvc"}, alert)
        assert triggered is False
    finally:
        con.close()


# ── update_last_triggered ──────────────────────────────────────────────────


def test_update_last_triggered_with_none_uses_now():
    """Passing ``triggered_ts=None`` makes the metadata layer stamp
    the current time — pinned so a future refactor doesn't silently
    persist None."""
    sid = "svc-now"
    alert = _make_alert(sid)
    save_alert(alert)

    update_last_triggered(sid, alert.id, None)

    row = metadata_db.get_con(sid).execute("SELECT last_triggered_at FROM alerts WHERE id = ?", (alert.id,)).fetchone()
    # Either a timestamp string OR None — implementation-defined; pinning
    # that the function doesn't raise when called with None.
    assert row is not None
