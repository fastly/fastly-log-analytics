from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import time_machine

from backend.models.reader_coverage import ReaderCoverage, RollupPathCoverage
from backend.repositories import rollup_coverage as coverage

START = datetime(2026, 1, 1, tzinfo=UTC)


def _hour(root: Path, hour: int) -> None:
    path = root / "rollups" / "hour_bundled" / f"hour=2026-01-01-{hour:02d}" / "security_conn_reuse.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


@pytest.mark.parametrize(
    "start,end,filtered,reason",
    [
        (None, None, True, "filtered"),
        (None, None, False, "bounds_missing"),
        ("invalid", "invalid", False, "invalid_window"),
        (START.isoformat(), START.isoformat(), False, "invalid_window"),
        (START.isoformat(), (START + timedelta(hours=1)).isoformat(), False, "window_too_short"),
        (START.isoformat(), (START + timedelta(days=367)).isoformat(), False, "window_too_long"),
        (START.isoformat(), (START + timedelta(hours=2)).isoformat(), False, "eligible"),
    ],
)
def test_eligibility_records_actual_threshold_and_short_circuit(start, end, filtered, reason) -> None:
    observed = ReaderCoverage(reader="dashboard.conn_requests_histogram", minimum_window_hours=2)
    result = coverage.eligible_window(start, end, has_filters=filtered, min_hours=2, observation=observed)
    assert observed.eligibility_reason == reason
    assert observed.eligible is (reason == "eligible")
    assert (result is not None) is (reason == "eligible")
    if reason in {"filtered", "bounds_missing", "invalid_window"}:
        assert observed.window_start is None
        assert observed.window_end is None
    else:
        assert observed.window_start == START.isoformat()
        assert observed.window_end == end
    assert observed.maximum_window_days == 366


@pytest.mark.parametrize("n,reason", [(0, "no_paths"), (1, "below_coverage_floor"), (2, "planned"), (4, "planned")])
def test_day_prefer_counts_missing_hours_in_denominator(tmp_path, n, reason) -> None:
    for hour in range(n):
        _hour(tmp_path, hour)
    (tmp_path / "rollups" / "hour_bundled").mkdir(parents=True, exist_ok=True)
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    with time_machine.travel(START + timedelta(days=1), tick=False):
        result = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=4),
            "security_conn_reuse.parquet",
            observation=observed,
        )
    assert observed.reason == reason
    assert observed.closed_hours == 4
    assert observed.covered_hours == n
    assert observed.coverage_fraction == n / 4
    assert observed.hour_files == n
    assert observed.day_files == 0
    assert observed.minimum_coverage_fraction == 0.5
    assert (result is not None) is (reason == "planned")


def test_missing_tree_does_not_claim_zero_coverage(tmp_path) -> None:
    observed = RollupPathCoverage(bundle="ngwaf_bots")
    result = coverage.collect_day_prefer_paths(
        {"_cache_dir_override": str(tmp_path)},
        START,
        START + timedelta(days=30),
        "ngwaf_bots.parquet",
        observation=observed,
    )
    assert result is None
    assert observed.reason == "no_bundled_root"
    assert observed.closed_hours is None
    assert observed.covered_hours is None
    assert observed.coverage_fraction is None
    assert observed.active_hour_start is None


def test_day_prefer_records_partial_start_and_day_tier_without_probing_more(tmp_path, monkeypatch) -> None:
    day = tmp_path / "rollups" / "day_bundled" / "day=2026-01-02" / "security_conn_reuse.parquet"
    day.parent.mkdir(parents=True)
    day.touch()
    _hour(tmp_path, 23)
    start, end = START + timedelta(hours=22, minutes=30), START + timedelta(days=2)
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    original = coverage.os.path.isfile
    probes: list[str] = []

    def probe(path: str) -> bool:
        probes.append(str(path))
        return original(path)

    monkeypatch.setattr(coverage.os.path, "isfile", probe)
    with time_machine.travel(end + timedelta(hours=1), tick=False):
        expected = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)}, start, end, "security_conn_reuse.parquet"
        )
        baseline_probes = probes.copy()
        probes.clear()
        actual = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)}, start, end, "security_conn_reuse.parquet", observation=observed
        )
    assert (
        actual
        == expected
        == [str(tmp_path / "rollups/hour_bundled/hour=2026-01-01-23/security_conn_reuse.parquet"), str(day)]
    )
    assert probes == baseline_probes
    assert observed.closed_hours == observed.covered_hours == 25
    assert observed.day_files == observed.hour_files == 1
    assert observed.partial_start_skipped is True
    assert observed.active_hour_start == (end + timedelta(hours=1)).isoformat()


def test_done_markers_are_distinct_from_parquet_presence(tmp_path) -> None:
    _hour(tmp_path, 0)
    marker = tmp_path / "rollups/hour_bundled/hour=2026-01-01-01/security_conn_reuse.parquet.done"
    marker.parent.mkdir(parents=True)
    marker.touch()
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    with time_machine.travel(START + timedelta(days=1), tick=False):
        result = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=2),
            "security_conn_reuse.parquet",
            allow_empty_with_done=True,
            observation=observed,
        )
    assert len(result) == 1
    assert observed.covered_hours == 2
    assert observed.hour_files == observed.done_markers == 1
    assert observed.coverage_fraction == 1


def _parquet(root: Path, filename: str, data: dict) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = root / "rollups/hour_bundled/hour=2026-01-01-00" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), path)


def test_histogram_records_both_attempts_and_served_outcome(in_memory_duckdb, tmp_path) -> None:
    from backend.repositories._base import QueryRunner

    _parquet(tmp_path, "all_fields.parquet", {"field": ["conn_requests"], "value": ["7"], "count": [5]})
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        result = runner.try_conn_requests_hist_from_rollup(
            START.isoformat(),
            (START + timedelta(hours=2)).isoformat(),
            has_filters=False,
            actual_cols=["conn_requests"],
        )
    assert result == {"top": [{"value": "6–20", "count": 5}], "total": 5}
    observed = runner.reader_coverage[0]
    assert observed.reader == "dashboard.conn_requests_histogram"
    assert observed.outcome == "served"
    assert observed.live_top_up == "outside_window"
    assert [(a.bundle, a.reason) for a in observed.attempts] == [
        ("security_conn_reuse", "no_paths"),
        ("all_fields", "planned"),
    ]
    assert observed.attempts[1].closed_hours == 2
    assert observed.attempts[1].covered_hours == 1
    assert observed.attempts[1].coverage_fraction == 0.5
    assert runner.telemetry()["reader_coverage"][0]["outcome"] == "served"


@pytest.mark.parametrize("kind", ["histogram", "ngwaf"])
def test_reader_records_live_unavailability_without_claiming_complete_results(
    in_memory_duckdb, tmp_path, monkeypatch, kind
) -> None:
    from backend.repositories._base import QueryRunner

    filename = "security_conn_reuse.parquet" if kind == "histogram" else "ngwaf_bots.parquet"
    data = (
        {"bucket": ["1"], "count": [2]}
        if kind == "histogram"
        else {"bot_name": ["Testbot"], "category": ["test"], "count": [2]}
    )
    _parquet(tmp_path, filename, data)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    monkeypatch.setattr(runner, "get_schema_cols", lambda: ["waf_req_id", "conn_requests"])
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: None)
    with time_machine.travel(START + timedelta(hours=2), tick=False):
        if kind == "histogram":
            result = runner.try_conn_requests_hist_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False
            )
            assert result["total"] == 2
        else:
            result = runner.try_ngwaf_top_bots_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False, n=10
            )
            assert result == [{"name": "Testbot", "category": "test", "request_count": 2}]
    observed = runner.reader_coverage[0]
    assert observed.outcome == "served"
    assert observed.live_top_up == "unavailable"
    assert observed.live_start == (START + timedelta(hours=2)).isoformat()
    assert observed.live_end == (START + timedelta(hours=3)).isoformat()
    assert observed.attempts[0].covered_hours == 1
    assert observed.attempts[0].closed_hours == 2


def test_reader_failure_and_isolation(in_memory_duckdb, tmp_path) -> None:
    from backend.repositories._base import QueryRunner

    _hour(tmp_path, 0)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    other = QueryRunner(in_memory_duckdb, runner.src)
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        result = runner.try_conn_requests_hist_from_rollup(
            START.isoformat(), (START + timedelta(hours=2)).isoformat(), has_filters=False
        )
    assert result is None
    assert runner.reader_coverage[0].outcome == "read_failed"
    assert runner.reader_coverage[0].attempts[0].reason == "planned"
    assert other.reader_coverage == []
    assert "reader_coverage" not in other.telemetry()
    assert str(tmp_path) not in str(runner.telemetry()["reader_coverage"])


@pytest.mark.parametrize("kind", ["histogram", "ngwaf"])
@pytest.mark.parametrize("live_state", ["missing_column", "empty", "used", "failed"])
def test_live_top_up_outcomes_are_not_confused_with_coverage(
    in_memory_duckdb, tmp_path, monkeypatch, kind, live_state
) -> None:
    from backend.repositories._base import QueryRunner

    histogram = kind == "histogram"
    filename = "security_conn_reuse.parquet" if histogram else "ngwaf_bots.parquet"
    data = (
        {"bucket": ["1"], "count": [2]} if histogram else {"bot_name": ["Testbot"], "category": ["test"], "count": [2]}
    )
    _parquet(tmp_path, filename, data)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    column = "conn_requests" if histogram else "waf_req_id"
    monkeypatch.setattr(runner, "get_schema_cols", lambda: [] if live_state == "missing_column" else [column])
    if live_state == "used":
        if histogram:
            in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 1 AS conn_requests")
        else:
            in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 'req-test' AS waf_req_id")
            in_memory_duckdb.execute(
                "CREATE TEMP TABLE ngwaf_top_ngwaf_bots AS SELECT 'req-test' AS waf_req_id, 'Testbot' AS bot_name, 'test' AS category"
            )
    elif live_state == "failed":
        in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 1 AS wrong_column")
    monkeypatch.setattr(
        runner,
        "_create_active_hour_temp_direct",
        lambda *a, **kw: "__empty__" if live_state == "empty" else "live_observation",
    )
    with time_machine.travel(START + timedelta(hours=2), tick=False):
        if histogram:
            result = runner.try_conn_requests_hist_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False
            )
            count = result["total"]
        else:
            result = runner.try_ngwaf_top_bots_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False, n=10
            )
            count = result[0]["request_count"]
    assert count == (3 if live_state == "used" else 2)
    assert runner.reader_coverage[0].live_top_up == live_state
    assert runner.reader_coverage[0].outcome == "served"
    assert runner.reader_coverage[0].attempts[0].covered_hours == 1


def test_durable_gate_does_not_probe_or_invent_coverage(tmp_path, monkeypatch) -> None:
    observed = RollupPathCoverage(bundle="ngwaf_bots")
    monkeypatch.setattr(coverage, "durable_serving_blocked", lambda src: True)

    def forbidden(*args, **kwargs):
        raise AssertionError("durable gate must precede filesystem probes")

    monkeypatch.setattr(coverage.os.path, "isdir", forbidden)
    assert (
        coverage.collect_day_prefer_paths(
            {}, START, START + timedelta(hours=2), "ngwaf_bots.parquet", observation=observed
        )
        is None
    )
    assert observed.reason == "durable_unready"
    assert observed.closed_hours is None
    assert observed.covered_hours is None


def test_empty_asn_gate_preserves_unparsed_bounds() -> None:
    observed = ReaderCoverage(reader="dashboard.ngwaf_bots", minimum_window_hours=48)
    assert (
        coverage.eligible_window(
            "invalid", "invalid", has_filters=False, require_top_asns=[], min_hours=48, observation=observed
        )
        is None
    )
    assert observed.eligibility_reason == "top_asns_empty"
    assert observed.window_start is None


def test_filtered_reader_never_probes_coverage(in_memory_duckdb, monkeypatch) -> None:
    from backend.repositories._base import QueryRunner

    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})

    def forbidden(*args, **kwargs):
        raise AssertionError("filtered eligibility must not inspect coverage")

    monkeypatch.setattr(coverage, "durable_serving_blocked", forbidden)
    assert runner.try_ngwaf_top_bots_from_rollup(None, None, has_filters=True, n=10) is None
    observed = runner.reader_coverage[0]
    assert observed.eligible is False
    assert observed.eligibility_reason == "filtered"
    assert observed.outcome == "ineligible"
    assert observed.attempts == []
