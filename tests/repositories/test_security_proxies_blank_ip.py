"""Requests answered before the capture snippet ran (e.g. redirects) log no client IP, UA or
country but still carry edge RTT/geo, so they used to pass the proxy flags and render as
blank-IP rows in Active Suspicious Clients."""

from __future__ import annotations

import duckdb

from backend.repositories._sql.security import GET_SECURITY_PROXIES_COMBINED


def test_blank_ip_rows_are_not_suspicious_clients_or_counted_as_proxies():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min BIGINT, tcp_rtt BIGINT, lat DOUBLE, lon DOUBLE, "
        "asn BIGINT, country VARCHAR, city VARCHAR)"
    )
    # A real tunnel client and a blank-IP redirect with identical RTT shape.
    con.execute("INSERT INTO t VALUES ('10.0.0.2', 'IAD', 1000, 50000, 39.0, -77.1, 200, 'US', 'Reston')")
    con.execute("INSERT INTO t VALUES ('', 'IAD', 1000, 50000, 39.0, -77.1, 700, '', '')")
    rows = con.execute(
        GET_SECURITY_PROXIES_COMBINED.format(
            temp_table="t",
            select_country_city_inner=",\n            country,\n            city",
            select_country_city_outer=",\n                country,\n                city",
        )
    ).fetchall()

    clients = [r[1] for r in rows if r[0] == "client"]
    assert clients == ["10.0.0.2"]
    stats = next(r for r in rows if r[0] == "stats")
    assert stats[2] == 1  # active proxies: the real client only
    assert stats[3] == 1  # flagged requests
    quality = {r[1]: r[2] for r in rows if r[0] == "quality"}
    assert quality == {"Active Tunnel / Proxy": 2}  # traffic quality still counts every request


def test_rollup_reader_drops_blank_ip_tuples(tmp_path):
    """The rollup path serves the default 24h-48h+ windows, so it needs the same filter."""
    from datetime import UTC, datetime, timedelta
    from unittest.mock import patch

    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.repositories._base import QueryRunner

    base = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base + timedelta(hours=i) for i in range(50)]
    flagged = pa.table(
        {
            "ip": ["10.0.0.2", ""],
            "asn": [200, 700],
            "pop": ["IAD", "IAD"],
            "rtt_min": [1000.0, 1000.0],
            "tcp_rtt": [50000.0, 50000.0],
            "lat": [39.0, 39.0],
            "lon": [-77.1, -77.1],
            "country": ["US", ""],
            "city": ["Reston", ""],
            "cnt": pa.array([1, 1], type=pa.int64()),
        }
    )
    quality = pa.table({"type": ["Active Tunnel / Proxy"], "count": pa.array([2], type=pa.int64())})
    for h in hours:
        d = tmp_path / "rollups" / "hour_bundled" / f"hour={h.strftime('%Y-%m-%d-%H')}"
        d.mkdir(parents=True)
        pq.write_table(flagged, str(d / "security_proxy_flagged.parquet"))
        pq.write_table(quality, str(d / "security_proxy_quality.parquet"))

    runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-blank-rollup"})
    st = hours[0].isoformat().replace("+00:00", "Z")
    et = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        rolled = runner.try_security_proxies_from_rollup(st, et, has_filters=False)

    assert rolled is not None
    assert [c["ip"] for c in rolled["clients"]] == ["10.0.0.2"]
    assert rolled["stats"]["active_proxies_count"] == 1
