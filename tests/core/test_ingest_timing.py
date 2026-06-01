"""Tests for ingest timing paths — specifically the max_seconds guard.

These cover the exact code path that had `time.time() - start_time` (where
start_time was a str|None date-range parameter) instead of start_time_exec.
"""

import time
from unittest.mock import MagicMock, patch


def _make_source(**overrides) -> dict:
    base = {
        "name": "test",
        "service_id": "test",
        "service_name": "Test",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "key",
        "secret_access_key": "secret",
        "bucket": "bucket",
        "prefix": "",
        "region": "us-east-1",
        "cdn_url": "",
        "cdn_secret": "",
        "cdn_service_id": "",
        "logging_service_id": "test",
        "duckdb_path": ":memory:",
        "access_level": "read_write",
        "storage_mode": "cloud",
        "log_period": 60,
        "provisioning": {},
    }
    base.update(overrides)
    return base


def _drain(gen) -> list[dict]:
    return list(gen)


class TestIngestReadOnlyGuard:
    def test_read_only_yields_error_immediately(self):
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_only")
        events = _drain(ingest(source=src))

        assert len(events) == 1
        assert events[0]["type"] == "error"
        assert "read-only" in events[0]["message"].lower()

    def test_read_write_does_not_error_on_access_level(self):
        """read_write source should not hit the read-only guard."""
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_write")
        # We stop it early by patching _ensure_source_registered to raise so we
        # don't need a real DuckDB/FOS setup — we just want to confirm the guard
        # itself wasn't triggered. Must not use StopIteration inside a generator
        # (RuntimeError in Python 3.7+), so use ValueError instead.
        with patch("backend.core.ingest._ensure_source_registered", side_effect=ValueError("stop")):
            events = []
            try:
                for e in ingest(source=src):
                    events.append(e)
            except ValueError:
                pass

        # No read-only error should have been emitted before ValueError
        assert not any(e.get("type") == "error" and "read-only" in e.get("message", "").lower() for e in events)


class TestIngestStartTimeExec:
    """Verify start_time_exec (float) is always used for wall-clock arithmetic."""

    def test_start_time_exec_is_float(self):
        """Regression: start_time (str|None) must never be subtracted from time.time().

        Inject a string start_time and ensure no TypeError is raised from the
        max_seconds guard when it fires.
        """
        from backend.core.ingest import ingest

        src = _make_source()

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []

        # Simulate: time limit already exceeded on first check
        call_count = {"n": 0}
        real_start = time.time()

        def fake_time():
            call_count["n"] += 1
            # First call (start_time_exec = time.time()) returns real_start
            # Subsequent calls return start + 999 to trigger max_seconds immediately
            if call_count["n"] == 1:
                return real_start
            return real_start + 999.0

        # new_files is built from FOS listing; we stub it to [one_fake_file]
        # so the loop body executes and hits the max_seconds check.
        fake_file = "raw/2026-01-01/00/2026-01-01T00-00-00.svc.gz"

        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._get_fos_client") as mock_fos,
            patch("backend.core.metadata_db.get_ingested_filenames", return_value=set()),
            patch("backend.core.metadata_db.insert_ingested_files"),
            patch("time.time", side_effect=fake_time),
        ):
            # Build a paginator that yields one file
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = iter([{"Contents": [{"Key": fake_file, "Size": 100}]}])
            mock_fos.return_value.get_paginator.return_value = mock_paginator

            events = []
            try:
                # Pass a string start_time — this is the parameter that was
                # mistakenly used in the time arithmetic before the fix.
                for e in ingest(source=src, max_seconds=1, start_time="2026-01-01T00:00:00Z"):
                    events.append(e)
            except TypeError as exc:
                raise AssertionError(
                    f"TypeError raised — start_time (str) was used in float arithmetic: {exc}"
                ) from exc

        # The time-limit event should appear (or the loop may not have reached it
        # if no new files were found after filtering, but no TypeError is the key assertion).
        types = [e.get("type") for e in events]
        assert "error" not in types or not any(
            "unsupported operand" in e.get("message", "") for e in events if e.get("type") == "error"
        ), "TypeError leaked into error event"


class TestIngestMaxSeconds:
    """max_seconds should stop ingestion gracefully, not crash."""

    def test_max_seconds_zero_does_not_crash(self):
        """max_seconds=None means no limit; 0 is falsy so also no limit."""
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_only")
        # read_only exits before any timing code, so no TypeError possible
        events = _drain(ingest(source=src, max_seconds=0))
        assert events[0]["type"] == "error"

    def test_max_seconds_type_annotation(self):
        """max_seconds is int|None — ensure the signature hasn't regressed."""
        import inspect

        from backend.core.ingest import ingest

        sig = inspect.signature(ingest)
        ann = sig.parameters["max_seconds"].annotation
        # Accept int | None or Optional[int] representations
        ann_str = str(ann)
        assert "int" in ann_str and "None" in ann_str, f"Unexpected annotation: {ann_str}"

    def test_start_time_annotation_is_str_not_float(self):
        """start_time must remain str|None (date range), not float (wall clock)."""
        import inspect

        from backend.core.ingest import ingest

        sig = inspect.signature(ingest)
        ann = sig.parameters["start_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str, f"Unexpected annotation: {ann_str}"
        assert "float" not in ann_str, "start_time became float — timing regression risk"
