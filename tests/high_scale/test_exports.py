import time

import pytest

from backend.high_scale.exports import ExportManager, ExportState


def test_export_is_bounded_and_completes() -> None:
    manager = ExportManager(max_workers=1, max_rows=2, max_bytes=100)
    try:
        job_id = manager.submit("job-1", [{"id": 1}, {"id": 2}], lambda row: str(row["id"]).encode())
        job = manager.wait(job_id)
        assert job.state is ExportState.COMPLETED
        assert job.rows_written == 2
        assert job.payload == b"1\n2\n"
    finally:
        manager.shutdown()


def test_export_rejects_row_limit_and_duplicate_job() -> None:
    manager = ExportManager(max_workers=1, max_rows=1, max_bytes=100)
    try:
        manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        with pytest.raises(ValueError, match="already exists"):
            manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        job_id = manager.submit("job-2", [{"id": 1}, {"id": 2}], lambda row: b"x")
        assert manager.wait(job_id).state is ExportState.FAILED
    finally:
        manager.shutdown()


def test_export_can_be_cancelled() -> None:
    manager = ExportManager(max_workers=1, max_rows=1000, max_bytes=100_000)
    try:
        job_id = manager.submit("job-1", range(1000), lambda row: (time.sleep(0.001), b"x")[1])
        time.sleep(0.01)
        assert manager.cancel(job_id) is True
        assert manager.wait(job_id).state is ExportState.CANCELLED
    finally:
        manager.shutdown()
