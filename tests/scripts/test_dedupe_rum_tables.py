from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from scripts.dedupe_rum_tables import dedupe_service_rum_tables


def test_dedupe_service_rum_tables_dry_run_and_apply(tmp_path):
    """Test that dedupe_service_rum_tables dry-run only reports, and apply removes duplicates."""
    catalog_path = tmp_path / "metadata.ducklake"
    data_path = tmp_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS lake (DATA_PATH '{data_path}')")
    con.execute(
        'CREATE TABLE lake."logs_test_svc__client_vitals" ('
        "timestamp TIMESTAMP, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, "
        "pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, "
        "city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE)"
    )

    # Insert 3 unique beacons, where beacon 1 has 3 copies, beacon 2 has 2 copies, beacon 3 has 1 copy (total 6 rows)
    row1 = (
        datetime(2026, 10, 6, 12, 0, tzinfo=UTC),
        "LCP",
        120.0,
        "good",
        "/",
        "Chrome",
        "macOS",
        "Desktop",
        "c1",
        "r1",
        "SF",
        "CA",
        "US",
        "SFO",
        "1.3",
        50.0,
    )
    row2 = (
        datetime(2026, 10, 6, 12, 1, tzinfo=UTC),
        "FID",
        15.0,
        "good",
        "/about",
        "Chrome",
        "macOS",
        "Desktop",
        "c2",
        "r2",
        "SF",
        "CA",
        "US",
        "SFO",
        "1.3",
        50.0,
    )
    row3 = (
        datetime(2026, 10, 6, 12, 2, tzinfo=UTC),
        "CLS",
        0.05,
        "good",
        "/help",
        "Chrome",
        "macOS",
        "Desktop",
        "c3",
        "r3",
        "SF",
        "CA",
        "US",
        "SFO",
        "1.3",
        50.0,
    )

    for _ in range(3):
        con.execute(
            'INSERT INTO lake."logs_test_svc__client_vitals" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            list(row1),
        )
    for _ in range(2):
        con.execute(
            'INSERT INTO lake."logs_test_svc__client_vitals" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            list(row2),
        )
    con.execute(
        'INSERT INTO lake."logs_test_svc__client_vitals" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        list(row3),
    )

    con.execute("DETACH lake")
    con.close()

    src = {"service_id": "test_svc", "name": "test_svc"}

    def make_con(s):
        c = duckdb.connect(":memory:")
        c.execute(f"ATTACH 'ducklake:{catalog_path}' AS lake (DATA_PATH '{data_path}')")
        return c

    # 1. Dry run
    report_dry = dedupe_service_rum_tables(src, dry_run=True, con_factory=make_con)
    assert report_dry["client_vitals"]["total_before"] == 6
    assert report_dry["client_vitals"]["unique_rows"] == 3
    assert report_dry["client_vitals"]["duplicates"] == 3
    assert report_dry["client_vitals"]["deleted"] == 0

    # 2. Apply
    report_apply = dedupe_service_rum_tables(src, dry_run=False, con_factory=make_con)
    assert report_apply["client_vitals"]["total_before"] == 6
    assert report_apply["client_vitals"]["unique_rows"] == 3
    assert report_apply["client_vitals"]["duplicates"] == 3
    assert report_apply["client_vitals"]["deleted"] == 3
    assert report_apply["client_vitals"]["total_after"] == 3
