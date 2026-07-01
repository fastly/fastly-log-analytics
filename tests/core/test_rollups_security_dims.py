"""Tests for the security_dims (req_size / conn_reuse / topips / cov) per-hour
bundle writers, the day-compaction merge, the backfill driver, and the four
/api/security/aggregates readers (try_security_req_size / conn_reuse / top_ips /
coverage_from_rollup).

Mirrors test_rollups_origin_dims.py in structure — same patch pattern, same
_noop_lock + _past_hour helpers, same _build_patches stack (the shared driver
runs once PER bundle, so a connection factory hands each pass a fresh conn).

security_dims emits FOUR files per closed hour from one module:
  security_req_size.parquet    (req_header_bytes histogram bucket)
  security_conn_reuse.parquet  (conn_requests reuse bucket)
  security_topips.parquet      (ip, top-500 by MAX(req_header_bytes))
  security_cov.parquet         (1 row: total_rows, tls_populated)

ALL FOUR are EXACT (counts / MIN / MAX) — no _approx flag on any reader. The
parity tests assert byte-equal aggregates against the live _sql/security.py
templates (REQ_HEADER_SIZE_DIST / CONN_REUSE_DIST / TOP_IPS_BY_MAX_HEADER /
FINGERPRINT_COVERAGE_BULK) over the SAME closed-hour data.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the security_dims writers read and
    INSERT rows. Columns: timestamp, ip, req_header_bytes, conn_requests,
    tls_ciphers_sha."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ip VARCHAR, req_header_bytes BIGINT, "
        f"  conn_requests BIGINT, tls_ciphers_sha VARCHAR"
        f")"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ip"),
                r.get("req_header_bytes"),
                r.get("conn_requests"),
                r.get("tls_ciphers_sha"),
            ],
        )


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _build_patches(cache_root, table: str, con_factory):
    """The common writer-side patch stack — _cache_dir, the resolved view
    table, the read-only connection, the iceberg lock, and the stale-view
    retry shim. ``con_factory`` returns a FRESH connection each call (the
    shared driver runs once per bundle and closes the conn it's given)."""
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con_factory()),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    )


# ── Writer: happy path / all four bundles + schemas ─────────────────────────


def test_build_security_dims_writes_four_bundles(tmp_path):
    """A closed hour with the full column set produces all four parquet files
    with their documented schemas + exact aggregates."""
    from backend.core.rollups import security_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-1"}
    hour_token, hour_dt = _past_hour(2)

    # 6 rows: header sizes spanning two buckets; conn_requests reuse; two IPs;
    # 4 of 6 have a populated tls fingerprint (one empty string → not populated).
    rows = [
        {
            "timestamp": hour_dt + timedelta(minutes=0),
            "ip": "10.0.0.1",
            "req_header_bytes": 100,
            "conn_requests": 1,
            "tls_ciphers_sha": "abc",
        },
        {
            "timestamp": hour_dt + timedelta(minutes=1),
            "ip": "10.0.0.1",
            "req_header_bytes": 300,
            "conn_requests": 3,
            "tls_ciphers_sha": "abc",
        },
        {
            "timestamp": hour_dt + timedelta(minutes=2),
            "ip": "10.0.0.2",
            "req_header_bytes": 5000,
            "conn_requests": 10,
            "tls_ciphers_sha": "def",
        },
        {
            "timestamp": hour_dt + timedelta(minutes=3),
            "ip": "10.0.0.2",
            "req_header_bytes": 200,
            "conn_requests": 1,
            "tls_ciphers_sha": "def",
        },
        {
            "timestamp": hour_dt + timedelta(minutes=4),
            "ip": "10.0.0.3",
            "req_header_bytes": 50,
            "conn_requests": 1,
            "tls_ciphers_sha": "",
        },
        {
            "timestamp": hour_dt + timedelta(minutes=5),
            "ip": "10.0.0.3",
            "req_header_bytes": 50,
            "conn_requests": 1,
            "tls_ciphers_sha": None,
        },
    ]

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_sd", rows)
        return c

    p = _build_patches(cache_root, "logs_sd", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = security_dims.build_security_dims_bundles("svc-sd-1", src, [hour_token])

    assert n == 4
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
    req_file = hour_dir / "security_req_size.parquet"
    conn_file = hour_dir / "security_conn_reuse.parquet"
    topips_file = hour_dir / "security_topips.parquet"
    cov_file = hour_dir / "security_cov.parquet"
    assert req_file.exists() and conn_file.exists() and topips_file.exists() and cov_file.exists()

    # Schemas. (pyarrow auto-derives the `hour=` hive column from the path when
    # reading a single file inside a hour=H dir; the parquet body itself carries
    # only the COPY's projected columns — assert via issubset like origin_dims.)
    assert {"bucket", "count", "min_val"}.issubset(set(pq.read_table(str(req_file)).column_names))
    assert {"bucket", "count", "min_val"}.issubset(set(pq.read_table(str(conn_file)).column_names))
    assert {"ip", "max_header"}.issubset(set(pq.read_table(str(topips_file)).column_names))
    assert {"total_rows", "tls_populated"}.issubset(set(pq.read_table(str(cov_file)).column_names))

    # req_size buckets: 100,200,50,50 → '0-256B' (count 4, min 50); 300 →
    # '256-512B' (count 1, min 300); 5000 → '4-6KB' (count 1, min 5000).
    req_rows = {r["bucket"]: r for r in pq.read_table(str(req_file)).to_pylist()}
    assert req_rows["0-256B"]["count"] == 4 and req_rows["0-256B"]["min_val"] == 50
    assert req_rows["256-512B"]["count"] == 1 and req_rows["256-512B"]["min_val"] == 300
    assert req_rows["4-6KB"]["count"] == 1 and req_rows["4-6KB"]["min_val"] == 5000

    # conn_reuse: 1×4 → '1 (None)' (count 4, min 1); 3 → '2-5' (count 1, min 3);
    # 10 → '6-20' (count 1, min 10).
    conn_rows = {r["bucket"]: r for r in pq.read_table(str(conn_file)).to_pylist()}
    assert conn_rows["1 (None)"]["count"] == 4 and conn_rows["1 (None)"]["min_val"] == 1
    assert conn_rows["2-5"]["count"] == 1 and conn_rows["2-5"]["min_val"] == 3
    assert conn_rows["6-20"]["count"] == 1 and conn_rows["6-20"]["min_val"] == 10

    # topips: MAX per ip. 10.0.0.1→300, 10.0.0.2→5000, 10.0.0.3→50.
    topips_rows = {r["ip"]: r["max_header"] for r in pq.read_table(str(topips_file)).to_pylist()}
    assert topips_rows == {"10.0.0.1": 300, "10.0.0.2": 5000, "10.0.0.3": 50}

    # cov: 6 total, 4 populated (abc×2, def×2; '' and NULL not populated).
    cov = pq.read_table(str(cov_file)).to_pylist()[0]
    assert cov["total_rows"] == 6 and cov["tls_populated"] == 4


def test_build_security_dims_skips_active_hour(tmp_path):
    """The active UTC hour is still being written; the writer must skip it."""
    from backend.core.rollups import security_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-2"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_sd", [])
        return c

    p = _build_patches(cache_root, "logs_sd", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = security_dims.build_security_dims_bundles("svc-sd-2", src, [active_token])

    assert n == 0
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
    assert not (hour_dir / "security_req_size.parquet").exists()


def test_build_security_dims_missing_conn_requests_skips_that_bundle(tmp_path):
    """A service with no conn_requests column still gets req_size + topips + cov;
    only the conn_reuse bundle is skipped."""
    from backend.core.rollups import security_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-3"}
    hour_token, hour_dt = _past_hour(2)

    def _fresh_con():
        c = duckdb.connect(":memory:")
        c.execute(
            "CREATE TABLE logs_nocr (timestamp TIMESTAMPTZ, ip VARCHAR, "
            "req_header_bytes BIGINT, tls_ciphers_sha VARCHAR)"
        )
        for i in range(4):
            c.execute(
                "INSERT INTO logs_nocr VALUES (?, ?, ?, ?)",
                [hour_dt + timedelta(minutes=i), "10.0.0.1", 100 + i, "abc"],
            )
        return c

    p = _build_patches(cache_root, "logs_nocr", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = security_dims.build_security_dims_bundles("svc-sd-3", src, [hour_token])

    assert n == 3
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
    assert (hour_dir / "security_req_size.parquet").exists()
    assert (hour_dir / "security_topips.parquet").exists()
    assert (hour_dir / "security_cov.parquet").exists()
    assert not (hour_dir / "security_conn_reuse.parquet").exists()


def test_build_security_dims_missing_ip_skips_topips_only(tmp_path):
    """topips needs ip AND req_header_bytes — a service with no ip skips topips
    but still writes req_size (needs only req_header_bytes), conn_reuse, cov."""
    from backend.core.rollups import security_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-3b"}
    hour_token, hour_dt = _past_hour(2)

    def _fresh_con():
        c = duckdb.connect(":memory:")
        c.execute(
            "CREATE TABLE logs_noip (timestamp TIMESTAMPTZ, req_header_bytes BIGINT, "
            "conn_requests BIGINT, tls_ciphers_sha VARCHAR)"
        )
        for i in range(4):
            c.execute(
                "INSERT INTO logs_noip VALUES (?, ?, ?, ?)",
                [hour_dt + timedelta(minutes=i), 100 + i, 1, "abc"],
            )
        return c

    p = _build_patches(cache_root, "logs_noip", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = security_dims.build_security_dims_bundles("svc-sd-3b", src, [hour_token])

    assert n == 3
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
    assert (hour_dir / "security_req_size.parquet").exists()
    assert (hour_dir / "security_conn_reuse.parquet").exists()
    assert (hour_dir / "security_cov.parquet").exists()
    assert not (hour_dir / "security_topips.parquet").exists()


# ── Day-compaction merge ─────────────────────────────────────────────────────


def _write_hour_bundle(hour_dir: str, filename: str, select_sql_values: str, schema_cols: str) -> None:
    """Write a per-hour bundle parquet from a VALUES literal."""
    import os

    os.makedirs(hour_dir, exist_ok=True)
    wcon = duckdb.connect()
    try:
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {select_sql_values}) AS t({schema_cols})) "
            f"TO '{hour_dir}/{filename}' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def test_compact_security_dims_day_merge(tmp_path):
    """Day-compaction over a full closed UTC day: req_size/conn_reuse SUM counts
    + MIN-of-MIN; topips MAX-of-MAX (NOT SUM); cov SUM to one row."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-4"}

    # A full closed day (yesterday) so the compactor doesn't skip it as active.
    day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    day_str = day_dt.strftime("%Y-%m-%d")
    bundled_root = cache_root / "rollups" / "hour_bundled"

    # Two hours of that day.
    for hh in (3, 4):
        hour_token = (day_dt + timedelta(hours=hh)).strftime("%Y-%m-%d-%H")
        hour_dir = str(bundled_root / f"hour={hour_token}")
        # req_size: '0-256B' appears in both hours (counts 4 + 6, mins 50 + 80).
        _write_hour_bundle(
            hour_dir,
            "security_req_size.parquet",
            "('0-256B', CAST(4 AS BIGINT), CAST(50 AS BIGINT))"
            if hh == 3
            else "('0-256B', CAST(6 AS BIGINT), CAST(80 AS BIGINT))",
            "bucket, count, min_val",
        )
        # conn_reuse: '1 (None)' counts 2 + 3, mins 1 + 1.
        _write_hour_bundle(
            hour_dir,
            "security_conn_reuse.parquet",
            "('1 (None)', CAST(2 AS BIGINT), CAST(1 AS BIGINT))"
            if hh == 3
            else "('1 (None)', CAST(3 AS BIGINT), CAST(1 AS BIGINT))",
            "bucket, count, min_val",
        )
        # topips: ip A max 100 (h3) then 900 (h4); ip B max 500 (h3 only).
        _write_hour_bundle(
            hour_dir,
            "security_topips.parquet",
            "('A', CAST(100 AS BIGINT)), ('B', CAST(500 AS BIGINT))" if hh == 3 else "('A', CAST(900 AS BIGINT))",
            "ip, max_header",
        )
        # cov: 10/4 (h3), 20/9 (h4) → day total 30/13.
        _write_hour_bundle(
            hour_dir,
            "security_cov.parquet",
            "(CAST(10 AS BIGINT), CAST(4 AS BIGINT))" if hh == 3 else "(CAST(20 AS BIGINT), CAST(9 AS BIGINT))",
            "total_rows, tls_populated",
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            n = day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-4", src)

    assert n == 4  # one per bundle for the single closed day

    day_root = cache_root / "rollups" / "day_bundled" / f"day={day_str}"

    req = pq.read_table(str(day_root / "security_req_size.parquet")).to_pylist()[0]
    assert req["bucket"] == "0-256B" and req["count"] == 10 and req["min_val"] == 50  # 4+6, MIN(50,80)

    conn = pq.read_table(str(day_root / "security_conn_reuse.parquet")).to_pylist()[0]
    assert conn["count"] == 5 and conn["min_val"] == 1  # 2+3

    # topips MAX-of-MAX: A = max(100, 900) = 900 (NOT 1000 if SUM'd); B = 500.
    topips = {r["ip"]: r["max_header"] for r in pq.read_table(str(day_root / "security_topips.parquet")).to_pylist()}
    assert topips == {"A": 900, "B": 500}

    cov = pq.read_table(str(day_root / "security_cov.parquet")).to_pylist()[0]
    assert cov["total_rows"] == 30 and cov["tls_populated"] == 13


def test_compact_security_dims_topips_recaps_above_500(tmp_path):
    """Day-compaction re-caps topips to 500 IPs by MAX-of-MAX across hours,
    keeping the highest-max IPs."""
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-5"}

    day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    day_str = day_dt.strftime("%Y-%m-%d")
    bundled_root = cache_root / "rollups" / "hour_bundled"

    # 600 distinct IPs in one closed hour; max_header = the IP index so the
    # ranking is deterministic. The day file must keep the top-500 by MAX.
    hour_token = (day_dt + timedelta(hours=5)).strftime("%Y-%m-%d-%H")
    hour_dir = str(bundled_root / f"hour={hour_token}")
    values = ", ".join(f"('ip{i:04d}', CAST({i} AS BIGINT))" for i in range(600))
    _write_hour_bundle(hour_dir, "security_topips.parquet", values, "ip, max_header")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            day_bundles.compact_security_dims_closed_days_to_daily("svc-sd-5", src)

    day_file = cache_root / "rollups" / "day_bundled" / f"day={day_str}" / "security_topips.parquet"
    rows = pq.read_table(str(day_file)).to_pylist()
    assert len(rows) == 500
    maxes = sorted(r["max_header"] for r in rows)
    # Kept the top 500 by max_header: indices 100..599.
    assert maxes[0] == 100 and maxes[-1] == 599


# ── Backfill driver ─────────────────────────────────────────────────────────


def test_backfill_security_dims_heals_each_bundle_independently(tmp_path):
    """The self-heal driver walks EACH of the four bundles independently, so a
    partial prior run (req_size complete, topips/cov missing) still resumes the
    laggards. h2 has only the req_size file; backfill must skip h2 for req_size
    but still build conn_reuse/topips/cov there (the single-sentinel bug)."""
    from backend.core.rollups import security_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-6"}

    h1_token, _ = _past_hour(3)
    h2_token, _ = _past_hour(4)
    bundled_root = cache_root / "rollups" / "hour_bundled"
    for tok in (h1_token, h2_token):
        (bundled_root / f"hour={tok}").mkdir(parents=True)
        (bundled_root / f"hour={tok}" / "all_fields.parquet").write_bytes(b"x")
    # h2 already has ONLY the req_size bundle — the other three are missing.
    (bundled_root / f"hour={h2_token}" / "security_req_size.parquet").write_bytes(b"x")

    # Spy on the shared per-hour driver so each (bundle_filename -> hours) walk
    # is captured without touching the real DuckDB COPY path.
    calls: list[tuple[str, set[str]]] = []

    def _spy_bphb(sid, src, hours, *, bundle_filename, **kw):
        hours = list(hours)
        calls.append((bundle_filename, set(hours)))
        return len(hours)

    with patch("backend.core.rollups.security_dims.build_per_hour_bundles", side_effect=_spy_bphb):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = security_dims.backfill_security_dims_bundles("svc-sd-6", src)

    by_file = dict(calls)
    # req_size: only h1 is missing it (h2 has the sentinel).
    assert by_file[security_dims.SECURITY_REQ_SIZE_BUNDLE_FILENAME] == {h1_token}
    # conn_reuse / topips / cov: BOTH hours are missing them — the laggards
    # the single-sentinel backfill used to strand.
    for fname in (
        security_dims.SECURITY_CONN_REUSE_BUNDLE_FILENAME,
        security_dims.SECURITY_TOPIPS_BUNDLE_FILENAME,
        security_dims.SECURITY_COV_BUNDLE_FILENAME,
    ):
        assert by_file[fname] == {h1_token, h2_token}, f"{fname} did not heal both hours"
    # 1 (req_size) + 2 + 2 + 2 = 7 files written.
    assert n == 7


# ── Reader eligibility gates ────────────────────────────────────────────────


def test_readers_eligibility_gates(tmp_path):
    """All four readers return None for: filters present, window too short,
    missing closed-hour rollup file, malformed timestamps."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-7"}
    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    readers = [
        lambda s, e, hf: runner.try_security_req_size_from_rollup(s, e, has_filters=hf),
        lambda s, e, hf: runner.try_security_conn_reuse_from_rollup(s, e, has_filters=hf),
        lambda s, e, hf: runner.try_security_top_ips_from_rollup(s, e, has_filters=hf),
        lambda s, e, hf: runner.try_security_coverage_from_rollup(s, e, has_filters=hf),
    ]

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        for call in readers:
            # has_filters → None
            assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None
            # window < 48h → None
            assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None
            # empty bundle dir (no coverage) → None
            assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None
            # malformed timestamps → None
            assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None


# ── Parity: rollup reader output == live SQL output over the SAME data ───────

# The live SQL templates (verbatim from backend/repositories/_sql/security.py).
# Imported via the module so a future edit to the template flags this test.
from backend.repositories._sql.security import (  # noqa: E402
    CONN_REUSE_DIST,
    FINGERPRINT_COVERAGE_BULK,
    REQ_HEADER_SIZE_DIST,
    TOP_IPS_BY_MAX_HEADER,
)


def test_rollup_readers_match_live_sql(tmp_path):
    """Seed known rows across a 50-hour closed window; build the security_dims
    rollups; assert each reader's output is byte-equal to the equivalent live
    SQL run over the SAME rows (one temp table holding all 50 hours)."""
    from backend.core.rollups import security_dims
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sd-8"}

    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]

    def _canonical_rows(hour_dt):
        rows = []
        # A spread of header sizes across several buckets + conn reuse tiers +
        # 3 IPs with distinct max headers + a mix of populated/empty TLS.
        specs = [
            ("10.0.0.1", 100, 1, "abc"),
            ("10.0.0.1", 400, 4, "abc"),
            ("10.0.0.2", 5000, 12, "def"),
            ("10.0.0.2", 9000, 1, ""),
            ("10.0.0.3", 200, 50, None),
            ("10.0.0.3", 200, 200, "ghi"),
        ]
        for j, (ip, rhb, cr, tls) in enumerate(specs):
            rows.append(
                {
                    "timestamp": hour_dt + timedelta(seconds=j),
                    "ip": ip,
                    "req_header_bytes": rhb,
                    "conn_requests": cr,
                    "tls_ciphers_sha": tls,
                }
            )
        return rows

    # Build the rollups: seed each closed hour and run the writer once per hour.
    for dt in hours:
        hour_token = dt.strftime("%Y-%m-%d-%H")
        canonical = _canonical_rows(dt)

        def _fresh_con(rows=canonical):
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_par", rows)
            return c

        p = _build_patches(cache_root, "logs_par", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            security_dims.build_security_dims_bundles("svc-sd-8", src, [hour_token])

    # Live SQL over one temp table holding ALL 50 hours of the same rows.
    con = duckdb.connect(":memory:")
    all_rows: list[dict] = []
    for dt in hours:
        all_rows.extend(_canonical_rows(dt))
    _seed_logs(con, "t_par", all_rows)
    runner = QueryRunner(con, src)

    # ── req_size parity ──
    live_req = con.execute(REQ_HEADER_SIZE_DIST.format(temp_table="t_par")).fetchall()
    live_req_pairs = [(r[0], int(r[1])) for r in live_req]  # (bucket, count) in min_val order

    # ── conn_reuse parity ──
    live_conn = con.execute(CONN_REUSE_DIST.format(temp_table="t_par")).fetchall()
    live_conn_pairs = [(r[0], int(r[1])) for r in live_conn]

    # ── topips parity ── (live LIMIT 10)
    live_topips = con.execute(TOP_IPS_BY_MAX_HEADER.format(temp_table="t_par")).fetchall()
    live_topips_pairs = [(r[0], int(r[1])) for r in live_topips]

    # ── coverage parity ── (one populated col over tls_ciphers_sha)
    agg = "count(*) FILTER (WHERE tls_ciphers_sha IS NOT NULL AND tls_ciphers_sha != '') AS populated_0"
    live_cov = con.execute(FINGERPRINT_COVERAGE_BULK.format(temp_table="t_par", agg_cols=agg)).fetchone()
    live_total, live_pop = int(live_cov[0]), int(live_cov[1])

    # Rollup readers over the 50-hour closed window.
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        roll_req = runner.try_security_req_size_from_rollup(st_iso, et_iso, has_filters=False)
        roll_conn = runner.try_security_conn_reuse_from_rollup(st_iso, et_iso, has_filters=False)
        roll_topips = runner.try_security_top_ips_from_rollup(st_iso, et_iso, has_filters=False)
        roll_cov = runner.try_security_coverage_from_rollup(st_iso, et_iso, has_filters=False)

    assert roll_req is not None and roll_conn is not None and roll_topips is not None and roll_cov is not None

    # req_size: exact bucket→count parity AND same min_val ordering.
    assert [(r["bucket"], r["count"]) for r in roll_req] == live_req_pairs
    assert "_approx" not in {k for d in roll_req for k in d}  # EXACT — no _approx key

    # conn_reuse: exact bucket→count parity + ordering.
    assert [(r["bucket"], r["count"]) for r in roll_conn] == live_conn_pairs

    # topips: top-10 by MAX-of-MAX. Each hour has identical rows so MAX across
    # hours == per-hour MAX == the live MAX over all rows.
    assert [(r["ip"], r["max_header"]) for r in roll_topips] == live_topips_pairs

    # coverage: exact SUM parity.
    assert roll_cov == (live_total, live_pop)
