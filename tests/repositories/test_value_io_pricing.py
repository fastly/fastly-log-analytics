import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

from backend.core.rollups import NETWORK_SUMMARY_BUNDLE_FILENAME, _hour_bundled_root
from backend.core.rollups.network_dims import build_network_summary_bundles
from backend.repositories._base import _safe_table
from backend.repositories.value import _io_monthly_cost, _value_memo, get_summary, peek_value_summary


@pytest.mark.parametrize(
    ("requests", "expected"),
    [
        (0, 0.0),
        (100_000, 0.0),
        (10_000_000, 495.0),
        (50_000_000, 495.0 + 40_000_000 / 10_000 * 0.15),
        (150_000_000, 495.0 + 600.0 + 100_000_000 / 10_000 * 0.07),
        (6_000_000_000, 10_845.0),
    ],
)
def test_io_monthly_cost_tiers(requests: int, expected: float) -> None:
    assert _io_monthly_cost(requests) == pytest.approx(expected)


def test_value_summary_supports_proto_tls_is_ipv6_and_stale_rollup_fallback(tmp_path) -> None:
    _value_memo.cache.clear()
    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)
    src = {"name": "logs_val_proto", "path": str(tmp_path / "logs")}
    table = _safe_table(src["name"])
    prev_hour = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
    st_iso = prev_hour.isoformat().replace("+00:00", "Z")
    et_iso = (prev_hour + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    con.execute(
        f"CREATE TABLE {table} ("
        "timestamp TIMESTAMPTZ, cache VARCHAR, resp_bytes BIGINT, elapsed DOUBLE, "
        "proto VARCHAR, tls VARCHAR, is_ipv6 BOOLEAN, waf INT, waf_resp INT, waf_sig VARCHAR, _bot_name VARCHAR)"
    )
    con.execute(
        f"INSERT INTO {table} VALUES "
        f"(TIMESTAMPTZ '{st_iso}', 'HIT', 1000, 1000.0, '3.0', '1.3', true, 0, 200, NULL, NULL), "
        f"(TIMESTAMPTZ '{st_iso}', 'MISS', 2000, 4000.0, '2.0', '1.3', false, 0, 200, NULL, NULL), "
        f"(TIMESTAMPTZ '{st_iso}', 'HIT', 1000, 1000.0, '1.1', '-', false, 0, 200, NULL, NULL), "
        f"(TIMESTAMPTZ '{st_iso}', 'HIT', 1000, 1000.0, '1.0', '1.2', true, 0, 200, NULL, NULL)"
    )

    # 1. Write a stale all-zero network_summary.parquet bundle; reader must detect and fall back to raw.
    bundled_root = _hour_bundled_root(src)
    hour_token = prev_hour.strftime("%Y-%m-%d-%H")
    hour_dir = os.path.join(bundled_root, f"hour={hour_token}")
    os.makedirs(hour_dir, exist_ok=True)
    net_path = os.path.join(hour_dir, NETWORK_SUMMARY_BUNDLE_FILENAME)
    con.execute(
        f"COPY (SELECT TIMESTAMPTZ '{st_iso}' AS hour_start, "
        "CAST(4 AS BIGINT) AS requests, CAST(0 AS BIGINT) AS http3_requests, "
        "CAST(0 AS BIGINT) AS h2_requests, CAST(0 AS BIGINT) AS h11_requests, "
        "CAST(0 AS BIGINT) AS other_proto_requests, CAST(0 AS BIGINT) AS tls_requests, "
        f"CAST(0 AS BIGINT) AS ipv6_requests) TO '{net_path}' (FORMAT PARQUET)"
    )

    res = get_summary(
        lambda: con,
        src,
        st_iso,
        et_iso,
        {},
        chart_interval="1 hour",
    )
    assert res["network"]["total_requests"] == 4
    assert res["network"]["http3_pct"] == 25.0
    assert res["network"]["h2_pct"] == 25.0
    assert res["network"]["tls_pct"] == 75.0
    assert res["network"]["ipv6_pct"] == 50.0
    assert len(res["network"]["protocol_time_series"]) == 4
    assert res["performance"]["cache_accel_factor"] == 4.0
    assert res["security"]["top_waf_signals"] == []
    assert res["bots"]["top_bots"] == []

    # 2. Verify peek_value_summary hits the response memo.
    cached = peek_value_summary(src, st_iso, et_iso, {}, chart_interval="1 hour")
    assert cached is not None
    assert cached["network"]["http3_pct"] == 25.0

    # 3. Rebuild network_summary.parquet using build_network_summary_bundles with proto/tls/is_ipv6 schema.
    _value_memo.cache.clear()
    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch(
            "backend.core.rollups._common.describe_columns",
            return_value=["timestamp", "proto", "tls", "is_ipv6"],
        ),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        written = build_network_summary_bundles("svc-proto", src, [hour_token])
    assert written == 1

    con2 = duckdb.connect(db_path)
    res_rollup = get_summary(con2, src, st_iso, et_iso, {}, chart_interval="1 hour", sections={"network"})
    assert res_rollup.get("used_rollups") is True
    assert res_rollup["network"]["http3_pct"] == 25.0
    assert res_rollup["network"]["h2_pct"] == 25.0
    assert res_rollup["network"]["tls_pct"] == 75.0
    assert res_rollup["network"]["ipv6_pct"] == 50.0
