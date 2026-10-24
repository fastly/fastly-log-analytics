import duckdb

from backend.repositories._sql import security as SQL


def _rows(medium: int, high: int) -> list[tuple]:
    # pop SJC at its own coordinates keeps distance 0, so risk comes only from the RTT ratio.
    medium_rows = [
        (f"203.0.113.{i}", "SJC", 15000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(medium)
    ]
    high_rows = [
        (f"198.51.100.{i}", "SJC", 5000.0, 100000.0, 37.3382, -121.8863, 7922, "US", "San Jose") for i in range(high)
    ]
    return medium_rows + high_rows


def _active_clients(rows: list[tuple]) -> list[tuple]:
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t (ip VARCHAR, pop VARCHAR, rtt_min DOUBLE, tcp_rtt DOUBLE, lat DOUBLE, lon DOUBLE, "
        "asn INTEGER, country VARCHAR, city VARCHAR)"
    )
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    sql = SQL.GET_SECURITY_PROXIES_COMBINED.format(
        temp_table="t",
        select_country_city_inner=",\n            country,\n            city",
        select_country_city_outer=",\n        country,\n        city",
    )
    return [row for row in con.execute(sql).fetchall() if row[0] == "client"]


def test_high_risk_clients_are_listed_first_and_survive_the_100_row_cap() -> None:
    result = _active_clients(_rows(medium=150, high=3))

    risks = [row[14] for row in result]  # risk_level
    assert len(result) == 100
    assert risks[:3] == ["High", "High", "High"]
    assert set(risks[3:]) == {"Medium"}
