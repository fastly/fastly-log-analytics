"""Tests for backend.repositories.origin.get_aggregates — the composite
endpoint that materialises one TEMP TABLE then fans out seven origin
cards across pool connections.

Targets the unique branches inside ``get_aggregates`` that the
per-endpoint tests in ``test_origin.py`` don't reach:

* Happy path: seven cards populated from a single materialisation.
* Empty schema short-circuit.
* No intersecting columns short-circuit (table exists with only
  ``timestamp`` — the wanted-cols list collapses to empty so the temp
  CREATE is skipped).
* ``_PoolBusy`` from ``checkout_connection`` → serial fallback runs
  all four branches on the primary runner.
* Outer try/finally drops the temp table even if a branch raises.
* ``section_timings`` is populated with the per-section entries
  (temp_table_create + per-branch marks).
"""

from __future__ import annotations

import contextlib
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.repositories import origin as origin_mod
from backend.repositories._base import _safe_table
from backend.repositories.origin import get_aggregates
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


@pytest.fixture(autouse=True)
def _clear_origin_caches():
    """Drop the response memo + schema cache between tests so the
    composite always re-runs end-to-end."""
    _clear_schema_cache()
    origin_mod._response_cache.clear()
    yield
    _clear_schema_cache()
    origin_mod._response_cache.clear()


def _origin_logs(src: dict, num: int = 40) -> list[dict]:
    """Origin-shaped logs: ottfb populated, mixed ostatus codes, mixed oip.

    Mirrors the helper in test_origin.py so the happy path here exercises
    the same column families the granular endpoints expect.
    """
    logs = generate_mock_logs(src, num_logs=num, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50000 + i * 1000  # 50–90ms in microseconds
        log["ost"] = 200 if i % 5 != 0 else 500
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
        log["url"] = "/api/data" if i % 2 == 0 else "/images/logo.png"
        log["edge"] = True
    return logs


# ── 1. Happy path: all seven cards populated ─────────────────────────────────


@pytest.mark.asyncio
async def test_get_aggregates_happy_path_returns_all_seven_cards(in_memory_duckdb, test_service_source):
    """Seeds origin-shaped logs, calls ``get_aggregates`` with the
    representative ctx, asserts the seven sub-card keys are present and
    that each card has the expected ``has_data`` / payload shape."""
    logs = _origin_logs(test_service_source, num=40)
    # All from one POP so pop_latency returns a row; oip mix → ip_health.
    for log in logs:
        log["pop"] = "LAX"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
    )

    # The seven cards + the top-level wrapper keys.
    for key in (
        "has_data",
        "summary",
        "timeseries",
        "slow_urls",
        "status_codes",
        "path_breakdown",
        "pop_latency",
        "ip_health",
        "section_timings",
    ):
        assert key in result, f"missing top-level key: {key}"

    # Top-level has_data tracks summary.has_data.
    assert result["has_data"] is True
    assert result["summary"]["has_data"] is True

    # Each sub-card is a dict shape.
    assert isinstance(result["timeseries"]["series"], list)
    assert isinstance(result["slow_urls"]["rows"], list)
    assert isinstance(result["status_codes"]["rows"], list)
    assert isinstance(result["path_breakdown"]["rows"], list)
    assert isinstance(result["pop_latency"]["rows"], list)
    assert isinstance(result["ip_health"]["rows"], list)


# ── 2. Empty schema → empty_payload skeleton ─────────────────────────────────


@pytest.mark.asyncio
async def test_get_aggregates_empty_schema_returns_empty_payload(in_memory_duckdb, test_service_source, monkeypatch):
    """When ``runner.get_schema_cols()`` returns ``[]`` (the in-process
    iceberg-view self-heal couldn't even reconstruct a default schema),
    short-circuit to the empty_payload skeleton with ``has_data=False``.

    Note: in tests the QueryRunner's self-heal path synthesises a
    default-catalog schema even with no source table, so we explicitly
    force ``get_schema_cols`` to return ``[]`` to exercise the early-
    exit branch the production code guards against. The seven sub-card
    keys must still be present so the FE renderer doesn't crash.
    """
    from backend.repositories._base import QueryRunner

    monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: [])

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
    )

    assert result["has_data"] is False
    # The empty_payload skeleton: summary stays at the literal empty
    # dict rather than the per-card "no data" shape that
    # _shape_summary would produce.
    assert result["summary"] == {}
    assert result["timeseries"] == {"has_data": False, "series": []}
    assert result["slow_urls"] == {"has_data": False, "rows": []}
    assert result["status_codes"] == {"has_data": False, "rows": []}
    assert result["path_breakdown"] == {
        "has_data": False,
        "shielding_detected": False,
        "rows": [],
    }
    assert result["pop_latency"] == {
        "has_data": False,
        "requires_group_c": False,
        "rows": [],
    }
    assert result["ip_health"] == {"has_data": False, "rows": []}
    # No section_timings on the short-circuit branch.
    assert "section_timings" not in result


# ── 3. Schema with only timestamp → no intersecting cols → empty_payload ─────


@pytest.mark.asyncio
async def test_get_aggregates_no_intersecting_cols_returns_empty(in_memory_duckdb, test_service_source, monkeypatch):
    """When ``get_schema_cols()`` returns a schema with no columns in
    common with the ``wanted_cols`` list inside ``get_aggregates``
    (timestamp, cache, edge, url, oip, ost, pop, ottfb, ottlb, ttfb,
    elapsed, obytes), ``select_cols`` collapses to ``[]`` and the
    early return fires BEFORE the TEMP TABLE materialisation.

    Force the schema to a single non-matching column so the
    intersection is provably empty regardless of the QueryRunner's
    iceberg-view self-heal behaviour in tests.
    """
    from backend.repositories._base import QueryRunner

    monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["foo"])

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
    )

    assert result["has_data"] is False
    # The empty_payload skeleton — same as the no-schema branch.
    assert result["summary"] == {}
    assert result["timeseries"] == {"has_data": False, "series": []}
    # section_timings is set up AFTER the select_cols check, so the
    # empty-intersection branch should NOT have it.
    assert "section_timings" not in result


# ── 4. _PoolBusy from checkout_connection → serial fallback ──────────────────


@pytest.mark.asyncio
async def test_get_aggregates_serial_fallback_on_pool_busy(in_memory_duckdb, test_service_source, monkeypatch):
    """Monkeypatch ``backend.core.duckdb_pool.checkout_connection`` to
    raise ``_PoolBusy`` immediately. ``get_aggregates`` must catch it
    and run all four branches serially against the primary runner —
    no asyncio.gather, no extra connections.

    Note: ``get_aggregates`` does ``from backend.core.duckdb_pool import
    _PoolBusy, checkout_connection`` inside the function body, so we
    patch the source module (which is what `from X import Y` will
    resolve on the next call).

    The recon noted the patch must be a real contextmanager factory,
    not a bare MagicMock — the for-loop in ``get_aggregates`` calls
    ``checkout_connection(src, max_wait=...)`` (factory) and then
    ``cm.__enter__()`` on the returned object. A MagicMock factory
    would happen to work, but a real ``@contextmanager`` matches the
    production signature and makes the test intent obvious.
    """
    logs = _origin_logs(test_service_source, num=30)
    for log in logs:
        log["pop"] = "LAX"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from backend.core import duckdb_pool

    @contextlib.contextmanager
    def _always_busy(src, max_wait=10.0):  # noqa: ARG001
        raise duckdb_pool._PoolBusy("test forced saturation")
        yield  # pragma: no cover — generator suite requires a yield even after raise

    monkeypatch.setattr(duckdb_pool, "checkout_connection", _always_busy)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
    )

    # The serial fallback ran all four branches against the single
    # primary runner. has_data + cards still populated, just without
    # parallelism.
    assert result["has_data"] is True
    assert result["summary"]["has_data"] is True
    assert isinstance(result["timeseries"]["series"], list)
    assert isinstance(result["status_codes"]["rows"], list)
    assert isinstance(result["pop_latency"]["rows"], list)
    assert isinstance(result["ip_health"]["rows"], list)

    # The parallel branch tags a marker; on the serial fallback it
    # must NOT be present.
    sections = {entry["section"] for entry in result["section_timings"]}
    assert "origin:parallel" not in sections
    # The per-branch marks fired against the primary runner.
    assert {
        "summary",
        "timeseries",
        "status_codes",
        "path_breakdown",
        "pop_latency",
        "ip_health",
        "slow_urls",
    }.issubset(sections)


# ── 5. Branch raises → outer finally still drops the temp table ──────────────


@pytest.mark.asyncio
async def test_get_aggregates_drops_temp_table_in_finally(in_memory_duckdb, test_service_source, monkeypatch):
    """Monkeypatch one of the inner branches (``_origin_summary_from_temp``)
    to raise. The outer try/finally must STILL issue
    ``DROP TABLE IF EXISTS "<temp_table>"`` against the primary runner
    so the per-request scratch table doesn't leak into the connection's
    catalog.

    We track DROP statements by recording every SQL the in-memory
    DuckDB sees through a sniffer wrapper on ``con.execute``.
    """
    logs = _origin_logs(test_service_source, num=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Sniff every SQL string passed through QueryRunner.execute so we
    # can assert the DROP fired in the finally clause. We monkeypatch
    # the QueryRunner method (not the DuckDB connection — its execute
    # attribute is read-only at the C layer).
    from backend.repositories._base import QueryRunner

    executed_sqls: list[str] = []
    original_execute = QueryRunner.execute

    def _sniff(self, q, p=None):
        executed_sqls.append(q)
        return original_execute(self, q, p)

    monkeypatch.setattr(QueryRunner, "execute", _sniff)

    # Force the summary branch to blow up AFTER temp-table create. Use
    # the serial fallback path so the exception propagates without
    # asyncio.gather wrapping (and so we don't need to mock 3 pool
    # connections too).
    from backend.core import duckdb_pool

    @contextlib.contextmanager
    def _busy(src, max_wait=10.0):  # noqa: ARG001
        raise duckdb_pool._PoolBusy("force serial path")
        yield  # pragma: no cover

    monkeypatch.setattr(duckdb_pool, "checkout_connection", _busy)

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic summary failure")

    monkeypatch.setattr(origin_mod, "_origin_summary_from_temp", _boom)

    with pytest.raises(RuntimeError, match="synthetic summary failure"):
        await get_aggregates(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {},
        )

    # The temp table name is randomised (t_origin_<hex>), so look for
    # the DROP-with-temp-prefix pattern in the recorded SQL trail.
    drop_stmts = [s for s in executed_sqls if "DROP TABLE IF EXISTS" in s and "t_origin_" in s]
    assert len(drop_stmts) >= 1, (
        "outer finally must DROP the per-request temp table even when an "
        f"inner branch raises. Recorded SQLs: {executed_sqls!r}"
    )


# ── 6. section_timings populated with temp_table_create + per-section ────────


@pytest.mark.asyncio
async def test_get_aggregates_section_timings_populated(in_memory_duckdb, test_service_source):
    """``section_timings`` is the perf-harness contract — it must
    include the ``temp_table_create`` mark plus one entry per branch.
    Pinned because dashboard.py, network.py, etc. all surface the same
    shape and the FE Debug Panel reads it directly."""
    logs = _origin_logs(test_service_source, num=25)
    for log in logs:
        log["pop"] = "JFK"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
    )

    timings = result["section_timings"]
    assert isinstance(timings, list)
    assert len(timings) >= 2

    # Each entry has the standard SectionTimer shape.
    for entry in timings:
        assert set(entry.keys()) >= {"section", "time_ms"}
        assert isinstance(entry["section"], str)
        assert isinstance(entry["time_ms"], (int, float))

    sections = {entry["section"] for entry in timings}
    # temp_table_create always fires when materialisation runs.
    assert "temp_table_create" in sections
    # The seven per-branch marks (whether parallel or serial both go
    # through the same _branch_* helpers that mark via SectionTimer).
    for name in (
        "summary",
        "slow_urls",
        "timeseries",
        "status_codes",
        "path_breakdown",
        "pop_latency",
        "ip_health",
    ):
        assert name in sections, f"missing section: {name}"


# ── 7. Section selector (P-4 slice 4) ────────────────────────────────────────


_ALL_ORIGIN_SECTIONS = {
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
}


@pytest.mark.asyncio
async def test_get_aggregates_sections_none_preserves_full_response(in_memory_duckdb, test_service_source):
    """sections=None is the zero-risk default — every section the
    pre-selector path produced must still land in the response."""
    logs = _origin_logs(test_service_source, num=25)
    for log in logs:
        log["pop"] = "DFW"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        sections=None,
    )
    assert _ALL_ORIGIN_SECTIONS.issubset(result.keys()), (
        f"sections=None should return every section; missing: {_ALL_ORIGIN_SECTIONS - result.keys()}"
    )


@pytest.mark.asyncio
async def test_get_aggregates_summary_only_skips_other_branches(in_memory_duckdb, test_service_source):
    """sections={'summary'} returns only the summary card — proves other
    branch helpers don't run AND their section_timings entries don't
    fire. Single-section selectors are the win case the FE will pre-warm
    with (e.g. dashboard hover surfaces ottfb_p95 from /origin's summary
    card without the heavy slow_urls branch)."""
    logs = _origin_logs(test_service_source, num=20)
    for log in logs:
        log["pop"] = "MIA"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        sections={"summary"},
    )
    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == {"summary"}, f"single-section request leaked extra keys; got {present}"
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    # temp_table_create always fires — the shared materialization is the
    # cost floor regardless of how many sections are requested.
    assert "temp_table_create" in timer_names
    assert "summary" in timer_names
    for blocked in ("slow_urls", "timeseries", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


@pytest.mark.asyncio
async def test_get_aggregates_ts_status_path_triple_runs_together(in_memory_duckdb, test_service_source):
    """The {timeseries, status_codes, path_breakdown} triple shares
    branch 3's pool conn — requesting them as the router would after
    coupling-expansion proves the branch still runs all three reads
    sequentially on the same runner (preserving the asyncio.gather
    partition the selector was designed around)."""
    logs = _origin_logs(test_service_source, num=30)
    for log in logs:
        log["pop"] = "ATL"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        sections={"timeseries", "status_codes", "path_breakdown"},
    )
    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == {"timeseries", "status_codes", "path_breakdown"}, (
        f"triple selector emitted unexpected sections; got {present}"
    )
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    for need in ("temp_table_create", "timeseries", "status_codes", "path_breakdown"):
        assert need in timer_names, f"missing required timing entry: {need}"
    # summary/slow_urls/pop/ip branches sit out — their timings must be absent.
    for blocked in ("summary", "slow_urls", "pop_latency", "ip_health"):
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


@pytest.mark.asyncio
async def test_get_aggregates_pop_ip_pair_runs_together(in_memory_duckdb, test_service_source):
    """The {pop_latency, ip_health} pair shares branch 4. Asserts both
    sections land in the response, both timings fire, and the four
    excluded sections (summary + slow_urls + the branch-3 triple) are
    suppressed."""
    logs = _origin_logs(test_service_source, num=20)
    for log in logs:
        log["pop"] = "SEA"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        sections={"pop_latency", "ip_health"},
    )
    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == {"pop_latency", "ip_health"}, f"pair selector got {present}"
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert "temp_table_create" in timer_names
    assert "pop_latency" in timer_names
    assert "ip_health" in timer_names
    for blocked in ("summary", "slow_urls", "timeseries", "status_codes", "path_breakdown"):
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


def test_parallel_gather_uses_return_exceptions_true():
    """Since all branch queries in ``get_aggregates`` read from the temporary
    table (which is connection-scoped in DuckDB and cannot be accessed across
    different pooled connections), parallel execution is disabled and all
    branches run sequentially on the primary connection's QueryRunner.

    Verify this design invariant by confirming that ``get_aggregates``
    source does not use ``checkout_connection`` or ``asyncio.gather`` for branch queries,
    and runs them serially.
    """
    import inspect

    src = inspect.getsource(get_aggregates)
    assert "checkout_connection" not in src, (
        "get_aggregates should not attempt to checkout extra connections "
        "because temporary tables are connection-scoped."
    )
    assert "asyncio.gather(" not in src, (
        "get_aggregates should execute branch queries sequentially on the primary connection's runner."
    )


# ── Part B: skip-temp guard ──────────────────────────────────────────────────
#
# get_aggregates now hoists every requested section's try_*_from_rollup BEFORE
# the temp build. When all requested sections hit, the CREATE TABLE
# (temp_table_create) is skipped and an ``origin:temp_skipped`` marker is
# emitted. Partial-miss / filtered / <48h / split_by_leg / sub-minute keep the
# temp path, byte-identical to pre-B behavior.


@contextmanager
def _noop_lock(_key):
    yield


def _rollup_base_table() -> str:
    return "logs_partb_base"


def _seed_partb_base(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Materialize the base view the rollup writers read from. Columns cover
    every dimension the seven origin rollups touch."""
    con.execute(
        f"CREATE TABLE {_rollup_base_table()} ("
        f"  timestamp TIMESTAMPTZ, cache VARCHAR, edge BOOLEAN, url VARCHAR, "
        f"  oip VARCHAR, ost INTEGER, pop VARCHAR, ottfb DOUBLE, ottlb DOUBLE, "
        f"  ttfb DOUBLE, elapsed DOUBLE, obytes DOUBLE"
        f")"
    )
    cols = [
        "timestamp",
        "cache",
        "edge",
        "url",
        "oip",
        "ost",
        "pop",
        "ottfb",
        "ottlb",
        "ttfb",
        "elapsed",
        "obytes",
    ]
    placeholders = ", ".join("?" for _ in cols)
    for r in rows:
        con.execute(f"INSERT INTO {_rollup_base_table()} VALUES ({placeholders})", [r.get(c) for c in cols])


def _write_all_fields_for_status(cache_root: str, hour: str, status_counts: dict[int, int]) -> None:
    """Write a closed-hour all_fields.parquet (schema: field, value, count)
    carrying field='ost' rows so try_origin_status_from_rollup is served."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    wcon = duckdb.connect()
    try:
        tuples = ", ".join(f"('ost', '{code}', CAST({n} AS BIGINT))" for code, n in status_counts.items())
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {tuples}) AS t(field, value, count)) "
            f"TO '{d}/all_fields.parquet' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def _seed_full_rollup_coverage(cache_root: str, base_rows: list[dict], hour_tokens: list[str]):
    """Drive all seven origin rollup writers against ``base_rows`` to lay down
    the six dedicated bundles (origin_summary / slow_urls / origin_pop /
    origin_ip / origin_path / origin_latency_ts) plus a per-hour
    all_fields.parquet for the status reader, under ``cache_root``.

    Returns nothing — writes files. Each writer opens its own fresh in-memory
    connection seeded with the same base rows (the shared driver closes the
    connection in its finally, so a reusable connection would be closed after
    the first writer). ``_safe_table_for`` is patched to the SAME table name
    ``_fresh_con`` seeds so DESCRIBE resolves."""
    from backend.core import rollups

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_partb_base(c, base_rows)
        return c

    patches = (
        patch("backend.core.duckdb._cache_dir", return_value=cache_root),
        patch("backend.core.rollups._common._safe_table_for", return_value=_rollup_base_table()),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: _fresh_con()),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _src, fn: fn(c)),
    )
    sid = "partb-svc"
    src = {"name": "partb-svc", "service_id": "partb-svc"}
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        rollups.build_origin_summary_bundles(sid, src, hour_tokens)
        rollups.build_slow_urls_bundles(sid, src, hour_tokens)
        rollups.build_origin_dims_bundles(sid, src, hour_tokens)
        rollups.build_origin_latency_ts_bundles(sid, src, hour_tokens)

    # status_codes reads the existing all_fields.parquet bundle — synthesize one
    # per closed hour with field='ost' rows.
    for h in hour_tokens:
        _write_all_fields_for_status(cache_root, h, {200: 80, 404: 10, 500: 10})


def _closed_hours(n: int, *, end_hours_ago: int = 3) -> list[datetime]:
    """``n`` consecutive closed UTC hours ending ``end_hours_ago`` before now."""
    base = (datetime.now(UTC) - timedelta(hours=end_hours_ago + n)).replace(minute=0, second=0, microsecond=0)
    return [base + timedelta(hours=i) for i in range(n)]


def _partb_base_rows(hours: list[datetime]) -> list[dict]:
    """Origin-shaped rows spread across the given closed hours — every
    dimension populated so all six dedicated rollups produce data."""
    rows: list[dict] = []
    for hi, hour_dt in enumerate(hours):
        for i in range(20):
            rows.append(
                {
                    "timestamp": hour_dt + timedelta(minutes=i % 10, seconds=i),
                    "cache": "MISS" if i % 3 else "HIT",
                    "edge": i % 2 == 0,
                    "url": "/api/data" if i % 2 == 0 else "/img/logo.png",
                    "oip": "203.0.113.1" if i < 12 else "203.0.113.2",
                    "ost": 200 if i % 5 else 500,
                    "pop": "LAX" if i < 10 else "JFK",
                    "ottfb": 50_000.0 + hi * 500 + i * 100,
                    "ottlb": 80_000.0 + hi * 500 + i * 100,
                    "ttfb": None,
                    "elapsed": 120_000.0 + i * 100,
                    "obytes": 1024.0 + i,
                }
            )
    return rows


@pytest.mark.asyncio
async def test_get_aggregates_all_rollups_hit_skips_temp(in_memory_duckdb, test_service_source):
    """Wide (>=48h) unfiltered request with full closed-hour rollup coverage:
    every section is served from a rollup, the temp build is skipped, and
    ``origin:temp_skipped`` is present while ``temp_table_create`` is ABSENT."""
    hours = _closed_hours(50)
    base_rows = _partb_base_rows(hours)
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]

    # Seed the base view in the request's in-memory con too (so the no-schema /
    # no-cols guards pass and, if anything missed, the live path would work).
    _seed_partb_base_into(in_memory_duckdb, test_service_source, base_rows)

    import tempfile

    with tempfile.TemporaryDirectory() as cache_root:
        _seed_full_rollup_coverage(cache_root, base_rows, hour_tokens)

        st = hours[0].isoformat()
        et = (hours[-1] + timedelta(hours=1)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=cache_root):
            result = await get_aggregates(
                in_memory_duckdb,
                test_service_source,
                st,
                et,
                {},
            )

    sections_present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert sections_present == _ALL_ORIGIN_SECTIONS, f"missing sections: {_ALL_ORIGIN_SECTIONS - sections_present}"

    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" not in timer_names, f"guard did not fire — temp built. timings: {timer_names}"
    assert "origin:temp_skipped" in timer_names

    # has_data from the summary rollup card.
    assert result["has_data"] is True
    assert result["summary"]["has_data"] is True
    # Approximate sections carry the _approx hint; status_codes is exact.
    assert result["summary"].get("_approx") is True
    assert result["timeseries"].get("_approx") is True
    assert result["slow_urls"].get("_approx") is True
    assert result["pop_latency"].get("_approx") is True
    assert result["ip_health"].get("_approx") is True
    assert result["path_breakdown"].get("_approx") is True
    assert "_approx" not in result["status_codes"]  # exact SUM of counts


def _seed_partb_base_into(con: duckdb.DuckDBPyConnection, src: dict, rows: list[dict]) -> None:
    """Create the request's base view table (named after the service) and
    insert the same rows the rollups were built from, so the live fallback is
    valid if any section misses."""
    table = _safe_table(src["name"])
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, cache VARCHAR, edge BOOLEAN, url VARCHAR, "
        f"  oip VARCHAR, ost INTEGER, pop VARCHAR, ottfb DOUBLE, ottlb DOUBLE, "
        f"  ttfb DOUBLE, elapsed DOUBLE, obytes DOUBLE"
        f")"
    )
    cols = [
        "timestamp",
        "cache",
        "edge",
        "url",
        "oip",
        "ost",
        "pop",
        "ottfb",
        "ottlb",
        "ttfb",
        "elapsed",
        "obytes",
    ]
    placeholders = ", ".join("?" for _ in cols)
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES ({placeholders})", [r.get(c) for c in cols])


@pytest.mark.asyncio
async def test_get_aggregates_partial_miss_builds_temp_for_missed_only(in_memory_duckdb, test_service_source):
    """Remove ONE section's rollup coverage (origin_latency_ts) → that section
    misses, the temp is built, and only the missed section runs live; the other
    six are still served from their rollups."""
    hours = _closed_hours(50)
    base_rows = _partb_base_rows(hours)
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
    _seed_partb_base_into(in_memory_duckdb, test_service_source, base_rows)

    import tempfile

    with tempfile.TemporaryDirectory() as cache_root:
        _seed_full_rollup_coverage(cache_root, base_rows, hour_tokens)
        # Drop every origin_latency_ts.parquet so the timeseries reader fails
        # closed (it requires ALL closed hours present). Leave a per-field
        # marker so collect_hourly_bundle_paths treats the hours as "had data,
        # missing bundle" → None → missed.
        for h in hour_tokens:
            ts_file = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h}", "origin_latency_ts.parquet")
            if os.path.exists(ts_file):
                os.remove(ts_file)
            field_dir = os.path.join(cache_root, "rollups", "hour", "field=ottfb", f"hour={h}")
            os.makedirs(field_dir, exist_ok=True)
            with open(os.path.join(field_dir, "data.parquet"), "wb") as f:
                f.write(b"x")

        st = hours[0].isoformat()
        et = (hours[-1] + timedelta(hours=1)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=cache_root):
            result = await get_aggregates(
                in_memory_duckdb,
                test_service_source,
                st,
                et,
                {},
            )

    timer_names = {t["section"] for t in result["section_timings"]}
    # Temp WAS built (timeseries missed).
    assert "temp_table_create" in timer_names
    assert "origin:temp_skipped" not in timer_names
    # Only the missed section ran live (its branch helper marked it AFTER the
    # hoist already marked it — both marks share the name; the live branch ran).
    assert "timeseries" in timer_names
    # The other six are still served (present in the response).
    assert _ALL_ORIGIN_SECTIONS.issubset(result.keys())
    assert result["timeseries"]["has_data"] is True
    # The rollup-served sections still carry _approx.
    assert result["summary"].get("_approx") is True
    assert result["slow_urls"].get("_approx") is True


@pytest.mark.asyncio
async def test_get_aggregates_filtered_request_builds_temp_all_live(in_memory_duckdb, test_service_source):
    """A filtered request (has_filters=True) makes every rollup reader return
    None → all sections land in ``missed`` → temp built and all sections live,
    byte-identical to pre-B behavior (no _approx anywhere, temp_table_create
    present, origin:temp_skipped absent)."""
    from backend.models.common import FilterSpec

    hours = _closed_hours(50)
    base_rows = _partb_base_rows(hours)
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
    _seed_partb_base_into(in_memory_duckdb, test_service_source, base_rows)

    import tempfile

    with tempfile.TemporaryDirectory() as cache_root:
        # Full coverage exists, but the filter disqualifies every reader.
        _seed_full_rollup_coverage(cache_root, base_rows, hour_tokens)
        st = hours[0].isoformat()
        et = (hours[-1] + timedelta(hours=1)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=cache_root):
            result = await get_aggregates(
                in_memory_duckdb,
                test_service_source,
                st,
                et,
                {"pop": FilterSpec(mode="include", values=["LAX"])},  # a filter → has_filters True
            )

    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" in timer_names
    assert "origin:temp_skipped" not in timer_names
    # Live temp path → no _approx markers anywhere.
    for section in ("summary", "slow_urls", "timeseries", "pop_latency", "ip_health", "path_breakdown"):
        assert "_approx" not in result[section], f"{section} carried _approx on the filtered live path"


@pytest.mark.asyncio
async def test_get_aggregates_selector_subset_all_hit_skips_temp(in_memory_duckdb, test_service_source):
    """sections={'summary','status_codes'} with full coverage → only those two
    are attempted, both hit, temp is skipped, and the other five sections are
    absent from the response (selector contract preserved)."""
    hours = _closed_hours(50)
    base_rows = _partb_base_rows(hours)
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
    _seed_partb_base_into(in_memory_duckdb, test_service_source, base_rows)

    import tempfile

    with tempfile.TemporaryDirectory() as cache_root:
        _seed_full_rollup_coverage(cache_root, base_rows, hour_tokens)
        st = hours[0].isoformat()
        et = (hours[-1] + timedelta(hours=1)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=cache_root):
            result = await get_aggregates(
                in_memory_duckdb,
                test_service_source,
                st,
                et,
                {},
                sections={"summary", "status_codes"},
            )

    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == {"summary", "status_codes"}, f"selector leaked sections: {present}"
    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" not in timer_names
    assert "origin:temp_skipped" in timer_names
    # Only the two requested sections were attempted/timed.
    assert "summary" in timer_names
    assert "status_codes" in timer_names
    for blocked in ("slow_urls", "timeseries", "path_breakdown", "pop_latency", "ip_health"):
        assert blocked not in timer_names
