import duckdb

from backend.repositories import security as repo
from backend.repositories._base import _safe_table, clear_schema_cols_cache


def test_get_security_proxies_mapping():
    # Clear cache to prevent test pollution from other tests in the same process
    clear_schema_cols_cache()
    from backend.core.duckdb import _clear_schema_cache

    _clear_schema_cache()

    con = duckdb.connect(":memory:")
    src = {"name": "test-service"}
    table_name = _safe_table(src["name"])

    # Create mock logs table with required columns
    con.execute(f"""
        CREATE TABLE {table_name} (
            ip VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER,
            lat DOUBLE, lon DOUBLE, asn INTEGER, timestamp TIMESTAMPTZ
        )
    """)

    con.execute(f"""
        INSERT INTO {table_name} VALUES
        ('1.1.1.1', 'SJC', 1000, 1000, 37.0, -121.0, 1111, '2026-08-01 12:00:00Z'),
        ('2.2.2.2', 'SJC', 5, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('3.3.3.3', 'SJC', 10, 1000, 37.0, -121.0, 2222, '2026-08-01 12:00:00Z'),
        ('4.4.4.4', 'SJC', 500, 1000, 37.0, -121.0, 3333, '2026-08-01 12:00:00Z')
    """)

    from unittest.mock import patch

    with patch(
        "backend.core.duckdb.get_asn_names", return_value={1111: "Good ISP", 2222: "Bad ISP 1", 3333: "Mobile ISP"}
    ):
        res = repo.get_security_proxies(
            con=con, src=src, start_time="2026-08-01T00:00:00Z", end_time="2026-08-02T00:00:00Z", filters={}
        )

    # 4 rows total
    # Active Tunnel / Proxy: 2
    # Direct Connection: 1
    # WiFi / Mobile: 1
    tq = {item["label"]: item["value"] for item in res["traffic_quality"]}
    assert tq.get("Active Tunnel / Proxy") == 50.0
    assert tq.get("Direct Connection") == 25.0
    assert tq.get("WiFi / Mobile") == 25.0

    isps = {item["isp"]: item["count"] for item in res["suspicious_isps"]}
    assert isps.get("Bad ISP 1") == 2
    assert "Good ISP" not in isps
