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
    """Regression for the F015 use-after-return hazard (mirrors the
    dashboard guard in tests/routers/test_dashboard_router.py).

    On the parallel path, ``get_aggregates`` checks out one extra DuckDB
    connection per occupied non-primary branch and fans out via
    ``asyncio.gather``. Without ``return_exceptions=True``, a branch that
    raises (or a cancelled coroutine) propagates immediately; the
    surrounding ``finally`` then returns the extra conns to the pool
    (``errored=False``) while the sibling branches' ``asyncio.to_thread``
    workers are still executing against them — and DuckDB connections are
    not safe for concurrent use. A subsequent checkout of such a conn
    deadlocks on the internal mutex (leaked-active conns exhaust the pool
    → DoS) or corrupts in-process DuckDB state.

    The runtime branch is inside ``if parallel:``, which the
    single-connection test fixtures never exercise. Verify by reading the
    function's source — a source-level check is the most reliable pin for
    this structural invariant.
    """
    import inspect

    src = inspect.getsource(get_aggregates)
    assert "asyncio.gather(*tasks, return_exceptions=True)" in src, (
        "asyncio.gather() inside get_aggregates must pass "
        "return_exceptions=True so every worker thread finishes before "
        "the extra pool connections are released (F015 regression)."
    )
    # Confirm the manual re-raise is present so a real exception still
    # surfaces to the caller rather than getting silently swallowed.
    assert "isinstance(part, BaseException)" in src and "raise part" in src, (
        "gather(return_exceptions=True) must be paired with an explicit "
        "BaseException re-raise so failures still propagate to the caller."
    )
