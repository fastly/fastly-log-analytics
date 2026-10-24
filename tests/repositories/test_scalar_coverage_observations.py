from datetime import UTC, datetime, timedelta

import pytest
import time_machine

from backend.models.reader_coverage import HourlyRollupCoverage, ScalarRollupCoverage
from backend.repositories import rollup_coverage
from backend.repositories._rollup_planner import plan_time_series_rollup_window

START = datetime(2026, 1, 1, tzinfo=UTC)


def _bundle(root, day: int, filename: str = "time_series.parquet") -> None:
    path = root / "rollups/day_bundled" / f"day=2026-01-{day:02d}" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def _hour(root, day: int, hour: int) -> None:
    path = root / "rollups/hour_bundled" / f"hour=2026-01-{day:02d}-{hour:02d}" / "time_series.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def test_daily_prefix_preserves_original_denominator_and_trailing_bounds(tmp_path) -> None:
    for day in range(1, 7):
        _bundle(tmp_path, day)
    for hour in range(24):
        _hour(tmp_path, 7, hour)
    observed = ScalarRollupCoverage()
    with time_machine.travel(START + timedelta(days=8), tick=False):
        plan = plan_time_series_rollup_window(
            {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(days=7), observation=observed
        )
    assert plan is not None
    assert observed.closed_hours == 168
    assert observed.prefix_hours == 144
    assert observed.day_time_series_files == 6
    assert observed.day_overview_files == 0
    assert observed.trailing_start == (START + timedelta(days=6)).isoformat()
    assert observed.trailing.closed_hours == 24
    assert observed.trailing.bundle_hours == 24
    assert observed.trailing.missing_hours == 0
    assert observed.covered_hours == 168
    assert observed.coverage_fraction == 1
    assert observed.reason == "planned"


def test_hour_rollover_retains_both_boundaries_without_invalid_combined_fraction(tmp_path, monkeypatch) -> None:
    _hour(tmp_path, 1, 0)
    _hour(tmp_path, 1, 1)
    build = rollup_coverage.CoverageIndex.build
    clocks = iter([START + timedelta(hours=1), START + timedelta(hours=2)])

    def rolling(st, et, **kwargs):
        return build(st, et, now=next(clocks), **kwargs)

    monkeypatch.setattr(rollup_coverage.CoverageIndex, "build", rolling)
    observed = ScalarRollupCoverage()
    plan = plan_time_series_rollup_window(
        {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(hours=2), observation=observed
    )
    assert plan is not None
    assert observed.closed_hours == 1
    assert observed.trailing.closed_hours == 2
    assert observed.reason == "boundary_changed"
    assert observed.coverage_fraction is None
    assert observed.covered_hours is None
    assert ScalarRollupCoverage.model_validate(observed.model_dump()) == observed


def test_missing_trailing_tree_retains_prefix_but_never_certifies_coverage(tmp_path) -> None:
    for day in range(1, 7):
        _bundle(tmp_path, day)
    observed = ScalarRollupCoverage()
    with time_machine.travel(START + timedelta(days=8), tick=False):
        plan = plan_time_series_rollup_window(
            {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(days=7), observation=observed
        )
    assert plan is None
    assert observed.closed_hours == 168
    assert observed.prefix_hours == 144
    assert observed.trailing.reason == "no_bundled_root"
    assert observed.covered_hours is None
    assert observed.coverage_fraction is None
    assert observed.reason == "trailing_unavailable"


@pytest.mark.parametrize("age,reason", [(1, "planned"), (3, "old_gaps")])
def test_hourly_heal_reports_actual_gaps_without_assuming_fifty_percent_threshold(tmp_path, age, reason) -> None:
    _hour(tmp_path, 1, 0)
    observed = HourlyRollupCoverage()
    with time_machine.travel(START + timedelta(days=age), tick=False):
        plan = rollup_coverage.plan_hourly_read(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=3),
            "time_series.parquet",
            allow_all_fields_coverage=True,
            heal=True,
            observation=observed,
        )
    assert (plan is not None) is (reason == "planned")
    assert observed.reason == reason
    assert observed.closed_hours == 3
    assert observed.bundle_hours == 1
    assert observed.missing_hours == 2
    assert observed.verified_empty_hours == 0
    assert observed.heal_horizon_hours == 48
    assert observed.covered_hours == 1
    assert observed.coverage_fraction == pytest.approx(1 / 3)


def test_scalar_readers_record_their_own_gate_and_execution(in_memory_duckdb, tmp_path) -> None:
    from backend.repositories._base import QueryRunner

    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    assert runner.try_count_from_rollup(None, None, "logs", "1=1", [], unfiltered_window=False) is None
    assert runner.try_time_series_from_rollup("p99_latency", "1 hour", None, None, "logs", "1=1", []) is None
    assert [(o.reader, o.eligibility_reason, o.outcome) for o in runner.reader_coverage] == [
        ("dashboard.count", "filtered", "ineligible"),
        ("dashboard.time_series", "unsupported_metric", "ineligible"),
    ]
    assert all(o.minimum_window_hours is None for o in runner.reader_coverage)


def test_scalar_readers_preserve_results_and_record_real_coverage(in_memory_duckdb, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from backend.repositories._base import QueryRunner

    for hour, count in enumerate((2, 3)):
        _hour(tmp_path, 1, hour)
        path = tmp_path / f"rollups/hour_bundled/hour=2026-01-01-{hour:02d}/time_series.parquet"
        pq.write_table(pa.table({"bucket": [START + timedelta(hours=hour)], "requests": [count]}), path)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        count = runner.try_count_from_rollup(
            START.isoformat(), (START + timedelta(hours=2)).isoformat(), "unused", "1=1", [], unfiltered_window=True
        )
        series = runner.try_time_series_from_rollup(
            "requests",
            "1 hour",
            START.isoformat(),
            (START + timedelta(hours=2)).isoformat(),
            "unused",
            "1=1",
            [],
            unfiltered_window=True,
        )
    assert count == 5
    assert [row["value"] for row in series] == [2, 3]
    assert [o.reader for o in runner.reader_coverage] == ["dashboard.count", "dashboard.time_series"]
    for observation in runner.reader_coverage:
        assert observation.minimum_window_hours is None
        assert observation.outcome == "served"
        assert observation.scalar_plan.closed_hours == 2
        assert observation.scalar_plan.covered_hours == 2
        assert observation.scalar_plan.coverage_fraction == 1
        assert observation.live_top_up == "outside_window"


@pytest.mark.parametrize("reader", ["count", "time_series"])
def test_failed_composite_query_does_not_claim_successful_live_top_up(
    in_memory_duckdb, tmp_path, monkeypatch, reader
) -> None:
    from backend.repositories._base import QueryRunner

    _hour(tmp_path, 1, 0)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    in_memory_duckdb.execute("CREATE TEMP TABLE bad_live AS SELECT 1 AS wrong_column")
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: "bad_live")
    monkeypatch.setattr(runner, "_partial_hour_adjusted_live_start", lambda start, *a: (start, [], 0))
    with time_machine.travel(START + timedelta(hours=1), tick=False):
        if reader == "count":
            result = runner.try_count_from_rollup(
                START.isoformat(), (START + timedelta(hours=2)).isoformat(), "unused", "1=1", [], unfiltered_window=True
            )
        else:
            result = runner.try_time_series_from_rollup(
                "requests",
                "1 hour",
                START.isoformat(),
                (START + timedelta(hours=2)).isoformat(),
                "unused",
                "1=1",
                [],
                unfiltered_window=True,
            )
    assert result is None
    assert runner.reader_coverage[0].outcome == "read_failed"
    assert runner.reader_coverage[0].live_top_up == "unverified"


def test_virtual_only_top_n_does_not_claim_a_live_batch_ran(in_memory_duckdb, monkeypatch) -> None:
    from backend.models.reader_coverage import ReaderCoverage
    from backend.repositories._base import QueryRunner
    from backend.repositories.top_n_rollups import SchemaSeeds, TopNWindow

    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    in_memory_duckdb.execute(
        "CREATE TEMP TABLE virtual_live AS SELECT TIMESTAMPTZ '2026-01-01 00:00:00+00' AS timestamp"
    )
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: "virtual_live")
    monkeypatch.setattr(runner, "_partial_hour_adjusted_live_start", lambda start, *a: (start, [], 0))
    observed = ReaderCoverage(reader="dashboard.top_n")
    result = runner._top_n_live_active_hour(
        TopNWindow.build(START, START + timedelta(hours=1), START),
        ["waf_sig_ind"],
        "unused",
        10,
        None,
        SchemaSeeds(["timestamp"], {"timestamp": "TIMESTAMPTZ"}),
        lambda *a: None,
        observation=observed,
    )
    assert result == ([], [])
    assert observed.live_top_up == "missing_column"


def test_top_n_any_field_presence_does_not_claim_all_field_coverage(tmp_path) -> None:
    from backend.models.reader_coverage import TopNRollupCoverage
    from backend.repositories.top_n_rollups import TopNWindow, find_unrolled_hours, plan_top_n_paths

    path = tmp_path / "rollups/hour/field=url/hour=2026-01-01-00/part.parquet"
    path.parent.mkdir(parents=True)
    path.touch()
    win = TopNWindow.build(START, START + timedelta(hours=3), START + timedelta(days=3))
    observed = TopNRollupCoverage()
    plan = plan_top_n_paths(str(tmp_path), ["url", "country"], win, observation=observed)
    gaps = find_unrolled_hours(plan, win, ["url", "country"], 48, observation=observed)
    assert gaps == []
    assert observed.coverage_scope == "any_field_file_presence"
    assert observed.closed_hours == 3
    assert observed.any_field_covered_hours == 1
    assert observed.field_complete_hours is None
    assert observed.per_field_hour_files == 1
    assert observed.heal_candidates == 0
    assert observed.outside_heal_horizon_hours == 3
    assert observed.heal_missing_hours == 0


def test_history_extents_are_observed_from_the_actual_query(in_memory_duckdb, monkeypatch) -> None:
    from backend import config
    from backend.repositories._base import QueryRunner, get_source_extent

    monkeypatch.setattr(config, "get_status", lambda name: None)
    in_memory_duckdb.execute(
        "CREATE TABLE logs_history AS SELECT timestamp FROM (VALUES (TIMESTAMPTZ '2026-01-01 00:00:00+00'), (TIMESTAMPTZ '2026-01-01 02:00:00+00')) AS t(timestamp)"
    )
    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    total, first, last = get_source_extent(runner, runner.src, "logs_history")
    assert total == 2
    assert runner.history.extent_source == "live_query"
    assert runner.history.extent_rows == 2
    assert runner.history.earliest_event_at == first
    assert runner.history.latest_event_at == last
    assert runner.history.window_rows is None
    assert runner.telemetry()["history"]["extent_source"] == "live_query"


def test_failed_extent_observation_is_unknown_not_zero(in_memory_duckdb, monkeypatch) -> None:
    from backend import config
    from backend.repositories._base import QueryRunner, get_source_extent

    monkeypatch.setattr(config, "get_status", lambda name: None)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    assert get_source_extent(runner, runner.src, "logs_absent") == (0, None, None)
    assert runner.history.extent_source == "unavailable"
    assert runner.history.extent_rows is None
    assert runner.history.earliest_event_at is None


def test_writer_behind_short_circuits_without_extra_probes(tmp_path, monkeypatch) -> None:
    _hour(tmp_path, 1, 0)
    per_field = tmp_path / "rollups/hour/field=url/hour=2026-01-01-01"
    per_field.mkdir(parents=True)
    observed = HourlyRollupCoverage()
    with time_machine.travel(START + timedelta(days=1), tick=False):
        original = rollup_coverage.os.path.isfile
        probes = []

        def probe(path):
            probes.append(path)
            return original(path)

        monkeypatch.setattr(rollup_coverage.os.path, "isfile", probe)
        source = {"_cache_dir_override": str(tmp_path)}
        rollup_coverage._listdir_cache.clear()
        expected = rollup_coverage.plan_hourly_read(
            source, START, START + timedelta(hours=3), "time_series.parquet", heal=True
        )
        baseline = probes.copy()
        probes.clear()
        rollup_coverage._listdir_cache.clear()
        actual = rollup_coverage.plan_hourly_read(
            source, START, START + timedelta(hours=3), "time_series.parquet", heal=True, observation=observed
        )
    assert actual is expected is None
    assert probes == baseline
    assert observed.reason == "writer_behind"
    assert observed.closed_hours == 3
    assert observed.covered_hours is None
    assert observed.missing_hours is None
