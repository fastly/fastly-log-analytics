"""Tests for scheduler timing correctness.

Regression suite for the bug where `time.time() - start_time` was used instead
of `time.time() - start_time_exec` in _run_log_discovery_cron, causing a TypeError
when start_time was a str|None date-range parameter.
"""

import inspect
from unittest.mock import patch


class TestRunServiceCronSignature:
    def test_start_time_is_str_not_float(self):
        """start_time in _run_log_discovery_cron must be str|None, not float."""
        from backend.cron.jobs.sync import _run_log_discovery_cron

        sig = inspect.signature(_run_log_discovery_cron)
        ann = sig.parameters["start_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str
        assert "float" not in ann_str, "start_time became float — timing regression risk"

    def test_end_time_is_str_not_float(self):
        from backend.cron.jobs.sync import _run_log_discovery_cron

        sig = inspect.signature(_run_log_discovery_cron)
        ann = sig.parameters["end_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str


class TestRunCommitUsesLocalStartTime:
    """_run_commit uses a local `start_time = time.time()` variable (correct).

    Verify it doesn't reference an outer scope start_time that could be str|None.
    """

    def test_run_commit_no_str_subtraction(self):
        """_run_commit should not raise TypeError even if called with no string start_time."""
        from backend.cron.jobs.commit import _run_commit

        mock_src = {
            "name": "test",
            "service_id": "test",
            "access_level": "read_write",
            "storage_mode": "cloud",
        }

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("lock held")),
        ):
            # RuntimeError from start_cron_run causes early return — no TypeError expected
            try:
                _run_commit("test-service")
            except TypeError as exc:
                raise AssertionError(f"TypeError in _run_commit — likely str used in float arithmetic: {exc}") from exc


class TestLogCronRunDurationIsFloat:
    """Ensure log_cron_run receives a float duration, not a str|None."""

    def test_duration_passed_to_log_cron_run_is_float(self):
        """Mock ingest to yield a done event and capture what log_cron_run receives."""
        from backend.cron.jobs.sync import _run_log_discovery_cron

        mock_src = {
            "name": "test",
            "service_id": "test",
            "service_name": "Test",
            "access_level": "read_write",
            "storage_mode": "cloud",
            "bucket": "b",
            "prefix": "",
            "region": "us-east-1",
            "log_period": 60,
            "provisioning": {},
        }

        captured_duration = {}

        def fake_log_cron_run(src, task, duration_s, status, **kwargs):
            captured_duration["value"] = duration_s
            captured_duration["type"] = type(duration_s).__name__

        def fake_ingest(**kwargs):
            yield {"type": "done", "new_files": 0}

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", return_value=1),
            patch("backend.core.duckdb.log_cron_run", side_effect=fake_log_cron_run),
            patch("backend.core.duckdb.refresh_config_status"),
            patch("backend.core.ingest.ingest", side_effect=fake_ingest),
            patch("backend.cron_progress.cleanup_progress"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron_progress.get_progress", return_value=[]),
            patch("backend.core.duckdb.backfill_fastly_edge_writes"),
            patch("backend.core.duckdb.reconcile_fastly_stats"),
            patch("backend.utils.usage_logger.flush_usage_log"),
            patch("backend.utils.usage_logger.run_usage_log_cleanup"),
            patch("backend.core.duckdb.update_cron_duration"),
        ):
            _run_log_discovery_cron("test", force=True, run_id=1)

        assert "value" in captured_duration, "log_cron_run was never called"
        assert isinstance(captured_duration["value"], float), (
            f"duration_s passed to log_cron_run was {captured_duration['type']}, expected float. "
            "This means start_time (str|None) was subtracted from time.time() instead of start_time_exec."
        )
        assert captured_duration["value"] >= 0.0


class TestIngestTimingVariableNames:
    """Verify the variable naming convention is correct in ingest source."""

    def test_start_time_exec_exists_in_ingest(self):
        """ingest() must define start_time_exec as the wall-clock timer."""
        import ast
        import pathlib

        src = pathlib.Path("backend/core/ingest.py").read_text()
        tree = ast.parse(src)

        # Find the ingest function
        ingest_fn = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "ingest")

        # Look for `start_time_exec = time.time()` assignment
        assignments = [
            node
            for node in ast.walk(ingest_fn)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "start_time_exec" for t in node.targets)
        ]
        assert assignments, "start_time_exec variable not found in ingest() — timing regression risk"

    def test_no_time_minus_start_time_in_ingest(self):
        """ingest() must not subtract the str|None `start_time` param from time.time()."""
        import ast
        import pathlib

        src = pathlib.Path("backend/core/ingest.py").read_text()
        tree = ast.parse(src)

        ingest_fn = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "ingest")

        # Find all BinOp subtractions where right side is a Name("start_time")
        bad_subtractions = [
            node
            for node in ast.walk(ingest_fn)
            if isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Sub)
            and isinstance(node.right, ast.Name)
            and node.right.id == "start_time"
        ]
        assert not bad_subtractions, (
            f"Found {len(bad_subtractions)} instance(s) of `... - start_time` in ingest(). "
            "Use start_time_exec (float) for wall-clock arithmetic, not start_time (str|None)."
        )

    def test_no_time_minus_start_time_in_scheduler(self):
        """_run_log_discovery_cron must not subtract the str|None start_time from time.time()."""
        import ast
        import pathlib

        # After the cron carve, _run_log_discovery_cron lives in backend/cron/jobs/sync.py.
        src = pathlib.Path("backend/cron/jobs/sync.py").read_text()
        tree = ast.parse(src)

        run_cron_fn = next(
            (
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == "_run_log_discovery_cron"
            ),
            None,
        )
        assert run_cron_fn is not None, "_run_log_discovery_cron not found in backend/cron/jobs/sync.py"

        bad_subtractions = [
            node
            for node in ast.walk(run_cron_fn)
            if isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Sub)
            and isinstance(node.right, ast.Name)
            and node.right.id == "start_time"
        ]
        assert not bad_subtractions, (
            f"Found {len(bad_subtractions)} instance(s) of `... - start_time` in _run_log_discovery_cron(). "
            "Use start_time_exec (float) for wall-clock arithmetic."
        )
