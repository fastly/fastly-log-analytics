from backend.repositories._sql import security as SQL


def test_sql_templates_exist():
    assert hasattr(SQL, "GET_PROXY_STATS")
    assert hasattr(SQL, "GET_TRAFFIC_QUALITY")
    assert hasattr(SQL, "GET_SUSPICIOUS_ISPS")
    assert hasattr(SQL, "GET_ACTIVE_PROXY_CLIENTS")
