"""Local real-file Standard bot-reader dispatch evidence, without live calls."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.models.security import SecurityTopBotsResponse
from backend.repositories import security
from backend.repositories._base import QueryRunner, _safe_table
from scripts.lib.section_measurements import describe_response


@pytest.fixture
def bot_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    con = duckdb.connect(":memory:")
    source = {"name": "bot-observation-test", "service_id": "bot-observation-test"}
    table = _safe_table(source["name"])
    con.execute(f'CREATE TABLE "{table}" (timestamp TIMESTAMPTZ, ua VARCHAR, waf_req_id VARCHAR)')
    con.execute("CREATE TEMP TABLE ngwaf_top_ngwaf_bots (waf_req_id VARCHAR, bot_name VARCHAR, category VARCHAR)")
    con.execute("INSERT INTO ngwaf_top_ngwaf_bots VALUES ('matched', 'ExampleBot', 'Test')")
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(cache))
    monkeypatch.setattr(security, "ensure_ngwaf_bots_materialized", lambda *_: True)
    monkeypatch.setattr(
        security, "classify_ua_counts", lambda _rows, _limit: [{"name": "ExampleBot", "request_count": 1}]
    )
    runner = QueryRunner(con, source)
    # Isolate the NGWAF branch: the independent UA reader is already satisfied.
    # NGWAF eligibility, path planning, parquet SQL and fallback SQL remain real.
    monkeypatch.setattr(runner, "execute_top_n_rollups", lambda *_args, **_kwargs: ([("ua", "ExampleBot", 1)], ["ua"]))
    executed: list[str] = []

    def execute(sql: str, params: list | None = None):
        executed.append(sql)
        # Keep the real SQL local; a deliberately broken parquet/join must not
        # enter catalog recovery or attempt operational database connections.
        return con.execute(sql, params) if params is not None else con.execute(sql)

    monkeypatch.setattr(runner, "execute", execute)
    yield con, source, runner, cache, executed
    con.close()


def _bundle(path: Path, count: int = 4) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"bot_name": ["ExampleBot"], "category": ["Test"], "count": pa.array([count], type=pa.int64())}),
        path,
    )


def _get(env: tuple, start: datetime, end: datetime, **extra: Any) -> dict:
    con, source, runner, _, _ = env
    return security.get_top_bots(con, source, start.isoformat(), end.isoformat(), {}, parent_runner=runner, **extra)


def _ngwaf_observation(result: dict) -> dict:
    return next(row for row in result["reader_coverage"] if row["reader"] == "dashboard.ngwaf_bots")


def _phase(result: dict, name: str) -> dict:
    return next(row for row in result["section_timings"] if row["section"] == name)


@pytest.mark.parametrize("tier", ["day", "hour"])
def test_ngwaf_rollup_source_preserves_actual_tier_and_outside_window(bot_reader, tier: str) -> None:
    _, _, _, cache, executed = bot_reader
    start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    if tier == "day":
        _bundle(cache / "rollups" / "day_bundled" / f"day={start:%Y-%m-%d}" / "ngwaf_bots.parquet", 96)
    else:
        for i in range(24):
            _bundle(
                cache
                / "rollups"
                / "hour_bundled"
                / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}"
                / "ngwaf_bots.parquet"
            )
    result = _get(bot_reader, start, start + timedelta(days=1))
    observed = _ngwaf_observation(result)
    assert observed["eligible"] is True
    assert observed["outcome"] == "served"
    assert observed["live_top_up"] == "outside_window"
    attempt = observed["attempts"][0]
    assert attempt["day_files"] == (1 if tier == "day" else 0)
    assert attempt["hour_files"] == (24 if tier == "hour" else 0)
    phase = _phase(result, "top_bots:ngwaf_rollup")
    assert phase["source"] == "rollup"
    assert phase["current_execution"] is True
    assert phase["outcome"] == "success"
    assert result["ngwaf_bots"] == [{"name": "ExampleBot", "category": "Test", "request_count": 96}]
    assert sum("read_parquet" in sql for sql in executed) == 1
    assert not any("INNER JOIN" in sql for sql in executed)


@pytest.mark.parametrize("topup_fails", [False, True])
def test_ngwaf_active_hour_source_is_mixed_and_topup_not_hidden(
    bot_reader, monkeypatch: pytest.MonkeyPatch, topup_fails: bool
) -> None:
    con, _, runner, cache, executed = bot_reader
    active = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = active - timedelta(hours=2)
    for i in range(2):
        _bundle(
            cache / "rollups" / "hour_bundled" / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}" / "ngwaf_bots.parquet"
        )
    event = active + timedelta(seconds=1)
    con.execute(
        f'INSERT INTO "{_safe_table("bot-observation-test")}" VALUES (?, ?, ?)', [event, "ExampleBot", "matched"]
    )
    buffer = cache / "buffer"
    buffer.mkdir()
    pq.write_table(
        pa.table({"timestamp": pa.array([event], type=pa.timestamp("us", tz="UTC")), "waf_req_id": ["matched"]}),
        buffer / "live.parquet",
    )
    if topup_fails:
        execute = runner.execute

        def fail_live(sql: str, params: list | None = None):
            if "INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql:
                executed.append(sql)
                raise duckdb.BinderException("Local top-up fixture failure")
            return execute(sql, params)

        monkeypatch.setattr(runner, "execute", fail_live)
    result = _get(bot_reader, start, active + timedelta(minutes=30))
    observed = _ngwaf_observation(result)
    assert observed["outcome"] == "served"
    assert observed["live_top_up"] == ("failed" if topup_fails else "used")
    assert observed["live_start"] == active.isoformat()
    phase = _phase(result, "top_bots:ngwaf_rollup")
    assert phase["source"] == "mixed"
    assert phase["outcome"] == ("error" if topup_fails else "success")
    assert result["ngwaf_bots"][0]["request_count"] == (8 if topup_fails else 9)
    assert sum("INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql for sql in executed) == 1


@pytest.mark.parametrize("reason", ["window_too_short", "no_coverage", "read_failed"])
def test_ngwaf_live_fallback_keeps_refusal_or_failure_and_actual_source(bot_reader, reason: str) -> None:
    con, _, _, cache, executed = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    end = start + timedelta(hours=1 if reason == "window_too_short" else 3)
    con.execute(
        f'INSERT INTO "{_safe_table("bot-observation-test")}" VALUES (?, ?, ?)',
        [start + timedelta(minutes=1), "ExampleBot", "matched"],
    )
    if reason == "read_failed":
        for i in range(3):
            path = (
                cache
                / "rollups"
                / "hour_bundled"
                / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}"
                / "ngwaf_bots.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            # A valid parquet with a wrong schema exercises the existing read-error fallback.
            pq.write_table(pa.table({"not_bot_name": ["ExampleBot"]}), path)
    result = _get(bot_reader, start, end)
    observed = _ngwaf_observation(result)
    assert observed["outcome"] == ("ineligible" if reason == "window_too_short" else reason)
    assert observed["live_top_up"] == "not_attempted"
    attempt = _phase(result, "top_bots:ngwaf_rollup")
    assert attempt["source"] == ("rollup" if reason == "read_failed" else "unknown")
    assert attempt["outcome"] == ("error" if reason == "read_failed" else "success")
    phase = _phase(result, "top_bots:ngwaf_join_direct")
    assert phase["source"] == "live"
    assert phase["outcome"] == "success"
    assert phase["current_execution"] is True
    assert result["ngwaf_bots"] == [{"name": "ExampleBot", "category": "Test", "request_count": 1}]
    assert sum("INNER JOIN temp.ngwaf_top_ngwaf_bots" in sql for sql in executed) == 1


def test_cached_reader_dispatch_is_historical_after_existing_wire_serialization(bot_reader) -> None:
    _, _, _, cache, _ = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    for i in range(3):
        _bundle(
            cache / "rollups" / "hour_bundled" / f"hour={start + timedelta(hours=i):%Y-%m-%d-%H}" / "ngwaf_bots.parquet"
        )
    fresh = _get(bot_reader, start, start + timedelta(hours=3))
    fresh_report = describe_response(SecurityTopBotsResponse(**fresh).model_dump(by_alias=True))
    fresh_phase = next(
        row for row in fresh_report["components"][0]["sections"] if row["section"] == "top_bots:ngwaf_rollup"
    )
    assert fresh_phase["source_evidence"] == "explicit"
    assert fresh_phase["current_execution"] is True
    assert fresh_report["components"][0]["reader_coverage"][0]["current_observation"] is True
    cached = _get(bot_reader, start, start + timedelta(hours=3))
    assert cached["_response_cache"]["state"] == "hit"
    wire = SecurityTopBotsResponse(**cached).model_dump(by_alias=True)
    report = describe_response(wire)
    phase = next(row for row in report["components"][0]["sections"] if row["section"] == "top_bots:ngwaf_rollup")
    assert phase["source"] == "rollup"
    assert phase["source_evidence"] == "explicit"
    assert phase["current_execution"] is False
    assert report["components"][0]["reader_coverage"][0]["current_observation"] is False


def test_filtered_ngwaf_temp_join_reports_live_without_claiming_rollup_eligibility(bot_reader) -> None:
    con, source, runner, _, _ = bot_reader
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    con.execute(
        f'INSERT INTO "{_safe_table(source["name"])}" VALUES (?, ?, ?)',
        [start + timedelta(minutes=1), "ExampleBot", "matched"],
    )
    result = security.get_top_bots(
        con,
        source,
        start.isoformat(),
        (start + timedelta(hours=3)).isoformat(),
        {"ua": {"values": ["ExampleBot"]}, "waf_req_id": {"values": ["matched"]}},
        parent_runner=runner,
    )
    observed = _ngwaf_observation(result)
    assert observed["eligible"] is False
    assert observed["eligibility_reason"] == "filtered"
    assert observed["outcome"] == "ineligible"
    assert observed["attempts"] == []
    assert observed["live_top_up"] == "not_attempted"
    phase = _phase(result, "top_bots:ngwaf_join")
    assert phase["source"] == "live"
    assert phase["outcome"] == "success"
    assert result["ngwaf_bots"][0]["request_count"] == 1


def test_failed_live_fallback_is_observed_without_claiming_success(bot_reader) -> None:
    con, _, _, _, _ = bot_reader
    # Break only the local join fixture after successful materialization.
    con.execute("DROP TABLE ngwaf_top_ngwaf_bots")
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=6)
    result = _get(bot_reader, start, start + timedelta(hours=1))
    observed = _ngwaf_observation(result)
    assert observed["outcome"] == "ineligible"
    assert observed["eligibility_reason"] == "window_too_short"
    phase = _phase(result, "top_bots:ngwaf_join_direct")
    assert phase["source"] == "live"
    assert phase["outcome"] == "error"
    assert phase["current_execution"] is True
    assert result["ngwaf_bots"] == []
