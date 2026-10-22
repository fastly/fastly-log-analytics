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
