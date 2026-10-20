"""Tests for precomputed RUM aggregates writer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb

from backend.core.rollups.rum import recompute_rum_aggregates, table_exists


def test_recompute_rum_aggregates_idempotency_and_correctness():
    # 1. Setup in-memory DuckDB connection
    con = duckdb.connect(":memory:")

    # 2. Create raw client_vitals and client_errors tables
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line INT,
            error_col INT,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)

    # 3. Seed some RUM beacons
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hour1 = now - timedelta(hours=2)
    hour2 = now - timedelta(hours=1)

    # Beacons for hour1
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "LCP",
            2.5,
            "good",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "LCP",
            4.5,
            "poor",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user2",
            "req2",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )
    con.execute(
        "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "ReferenceError",
            "app.js",
            10,
            20,
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )

    # Beacons for hour2
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour2,
            "CLS",
            0.05,
            "good",
            "/about",
            "Safari",
            "iOS",
            "mobile",
            "user3",
            "req3",
            "NYC",
            "NY",
            "US",
            "JFK",
            "TLS1.3",
            0.1,
        ],
    )

    # 4. Trigger recomputation
    recompute_rum_aggregates(con, "svc-rum-test")

    # 5. Assertions: check table existence
    assert table_exists(con, "rum_vitals_aggregates")
    assert table_exists(con, "rum_error_aggregates")

    # Check vitals aggregates
    v_rows = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    # Expect:
    # - 'browser' -> Chrome (2), Safari (1)
    # - 'device' -> desktop (2), mobile (1)
    # - 'metric_name' -> LCP (2, good=1, poor=1), CLS (1, good=1)
    # - 'os' -> macOS (2), iOS (1)
    # - 'pathname' -> /home (2), /about (1)
    # - 'total' -> pageviews (2 + 1 = 3)
    dimensions = [r[0] for r in v_rows]
    assert "browser" in dimensions
    assert "metric_name" in dimensions
    assert "total" in dimensions

    # Check metric_name p75_value calculation
    lcp_row = [r for r in v_rows if r[0] == "metric_name" and r[1] == "LCP"][0]
    assert lcp_row[2] == 2  # event_count
    assert lcp_row[3] == 1  # good_count
    assert lcp_row[4] == 1  # poor_count
    assert lcp_row[5] == 4.0  # p75 of [2.5, 4.5] is exact 4.0 under PERCENTILE_CONT

    # Check errors aggregates
    e_rows = con.execute(
        "SELECT dimension, value, error_count, bucket_start FROM rum_error_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    # Expect:
    # - 'total' -> errors (1)
    # - 'pathname' -> /home (1)
    # - 'exception' -> ReferenceError|app.js|10|20 (1)
    assert len(e_rows) == 3
    assert e_rows[2][0] == "total"
    assert e_rows[2][2] == 1

    # 6. Idempotency check: recompute again and verify no duplication
    recompute_rum_aggregates(con, "svc-rum-test")
    v_rows_after = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    assert v_rows_after == v_rows


def test_backfill_missing_rum_aggregates_heals_legacy_hour_without_total_beacons():
    """Hours aggregated before 3fcbcd27 have LCP/pageviews rows in rum_vitals_aggregates
    but lack a 'total_beacons' row. backfill_missing_rum_aggregates() must detect and
    recompute those hours across the 30-day lookback window."""
    from backend.core.rum_ingest import backfill_missing_rum_aggregates

    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line INT,
            error_col INT,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    legacy_hour = now - timedelta(days=12)
    recent_hour = now - timedelta(hours=2)
    beyond_lookback_hour = now - timedelta(days=40)

    for hr, req_id in [
        (legacy_hour, "req-legacy-1"),
        (legacy_hour, "req-legacy-2"),
        (recent_hour, "req-recent-1"),
        (beyond_lookback_hour, "req-old-1"),
    ]:
        con.execute(
            "INSERT INTO client_vitals VALUES (?, 'LCP', 2.1, 'good', '/home', 'Chrome', 'macOS', 'desktop', 'u1', ?, 'SFO', 'CA', 'US', 'SFO', 'TLS1.3', 0.1)",
            [hr, req_id],
        )

    # Initial recompute populates all rows including total_beacons
    recompute_rum_aggregates(con, "svc-rum-test")

    # Simulate pre-3fcbcd27 state on legacy_hour and beyond_lookback_hour:
    # LCP and pageviews rows exist in rum_vitals_aggregates, but total_beacons is missing.
    con.execute(
        "DELETE FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start IN (?, ?) AND value = 'total_beacons'",
        [legacy_hour, beyond_lookback_hour],
    )

    # Confirm LCP aggregate row still exists for legacy_hour, while total_beacons does not
    lcp_cnt = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'metric_name' AND value = 'LCP'",
        [legacy_hour],
    ).fetchone()
    assert lcp_cnt is not None and lcp_cnt[0] == 2
    tb_before = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
        [legacy_hour],
    ).fetchone()
    assert tb_before is None

    # Run backfill with 30-day lookback: should detect and heal legacy_hour (12d ago),
    # skip recent_hour (already has total_beacons), and ignore beyond_lookback_hour (40d ago).
    healed = backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30)
    assert len(healed) == 1

    tb_after = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
        [legacy_hour],
    ).fetchone()
    assert tb_after is not None and tb_after[0] == 2

    # Second run is a clean no-op
    healed_again = backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30)
    assert healed_again == []


def test_rollup_hour_heal_invokes_rum_total_beacons_backfill(monkeypatch, tmp_path):
    """_run_rollup_hour_heal triggers 30-day backfill_missing_rum_aggregates when RUM is enabled."""
    from unittest.mock import MagicMock

    import backend.core.iceberg._ducklake as _dl
    from backend.cron.jobs import compaction

    sid = "svc-rum-heal-cron"
    src = {
        "name": "rum-heal-svc",
        "service_id": sid,
        "access_level": "read_write",
        "cache_dir": str(tmp_path),
        "_cache_dir_override": str(tmp_path),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: str(tmp_path))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **k: 42)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **k: (True, ""))
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda s: {"provisioning": {"cron_sync": {}}, "rum": {"enabled": True}},
    )
    monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        lambda *a, **k: {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0},
    )
    monkeypatch.setattr("backend.core.rollups._common._hour_bundled_root", lambda s: str(tmp_path / "hours"))

    fake_con = MagicMock()

    monkeypatch.setattr("backend.core.duckdb.rum_source_for", lambda s: dict(s))
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: fake_con)
    monkeypatch.setattr(_dl, "_ducklake_attach", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.iceberg.execute_with_stale_view_retry",
        lambda con, s, fn, **kw: fn(con),
    )

    backfill_calls: list[tuple[object, str, int]] = []
    monkeypatch.setattr(
        "backend.core.rollups.rum.backfill_missing_rum_aggregates",
        lambda con, service_id, lookback_days=30: backfill_calls.append((con, service_id, lookback_days)) or [],
    )

    res = compaction._run_rollup_hour_heal.__wrapped__(sid, manual=True)
    assert res["status"] == "success"
    assert backfill_calls == [(fake_con, sid, 30)]
