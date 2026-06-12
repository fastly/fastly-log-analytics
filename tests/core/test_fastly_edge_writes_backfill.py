"""Regression tests for backfill_fastly_edge_writes idempotency.

Each ingested raw log file = 1 billable Fastly-edge PUT_OBJECT. The backfill
synthesises those rows in the per-service ``usage_log`` SQLite table so users
see the real bucket cost. It must be safe to call repeatedly (sync may retry,
restart, or run alongside manual triggers) without duplicating rows.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def seeded_metadata_db():
    """Pre-populate the per-service SQLite ``ingested_files`` table.

    Returns the service_id (which keys the per-service metadata file).
    """
    from backend.core import metadata_db

    service_id = "svc1"
    metadata_db.insert_ingested_files(
        service_id,
        [
            ("s3://bkt/raw/2026-05-13/21/2026-05-13T21:46:00.000-aaa.log.gz", 100, 1234),
            ("s3://bkt/raw/2026-05-13/21/2026-05-13T21:47:00.000-bbb.log.gz", 200, 2345),
            ("s3://bkt/raw/2026-05-13/22/2026-05-13T22:00:00.000-ccc.log.gz", 300, 3456),
            ("__seeding_attempted__", 0, 0),
        ],
    )
    return service_id


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.core.metadata_db.log_synthetic_usage")
def test_backfill_calls_metadata_db(_log_synth, _enabled, seeded_metadata_db):
    """backfill should hand a list of synthesised PUT_OBJECT calls to metadata_db.log_synthetic_usage."""
    from backend.core import duckdb as _db

    _log_synth.return_value = 3
    inserted = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert inserted == 3
    assert _log_synth.called
    calls = _log_synth.call_args[0][1]
    # 3 real files; the __seeding_attempted__ sentinel must be excluded.
    assert len(calls) == 3
    paths = sorted(c["path"] for c in calls)
    assert paths[0].endswith("aaa.log.gz")
    assert "__seeding_attempted__" not in paths

    by_path = {c["path"]: c for c in calls}
    assert by_path[paths[0]]["method"] == "PUT_OBJECT"
    assert by_path[paths[0]]["bytes"] == 1234
    assert by_path[paths[0]]["_timestamp_override"] == "2026-05-13T21:46:00Z"


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_backfill_is_idempotent_end_to_end(_enabled, seeded_metadata_db):
    """Running backfill multiple times should not insert duplicate rows."""
    from backend.core import duckdb as _db

    first = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    second = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    third = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert first == 3
    assert second == 0
    assert third == 0

    # usage_log lives in its own SQLite file post-2026-06-12.
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con(seeded_metadata_db)
    total = con.execute("SELECT count(*) FROM usage_log WHERE function_name = 'fastly.edge'").fetchone()[0]
    assert total == 3


@patch("backend.config.is_usage_logging_enabled", return_value=False)
@patch("backend.core.metadata_db.log_synthetic_usage")
def test_backfill_skips_when_usage_logging_disabled(_log_synth, _enabled, seeded_metadata_db):
    from backend.core import duckdb as _db

    inserted = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})

    assert inserted == 0
    assert not _log_synth.called


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_backfill_incremental_skips_already_backfilled_files(_enabled, seeded_metadata_db):
    """After the first backfill, subsequent calls should hand log_synthetic_usage
    an empty list — the NOT EXISTS filter prunes already-backfilled files at the
    SQL level, so we don't even pay the dedup IN-clause cost.
    """
    from backend.core import duckdb as _db
    from backend.core import metadata_db

    first = _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    assert first == 3

    # Add one more ingested file — only THAT file should be handed to log_synthetic_usage.
    metadata_db.insert_ingested_files(
        seeded_metadata_db,
        [("s3://bkt/raw/2026-05-13/22/2026-05-13T22:05:00.000-ddd.log.gz", 400, 4567)],
    )

    with patch("backend.core.metadata_db.log_synthetic_usage") as log_synth:
        log_synth.return_value = 1
        _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
        calls = log_synth.call_args[0][1]
        assert len(calls) == 1
        assert calls[0]["path"].endswith("ddd.log.gz")

    # And after a real second backfill, a third pass sees nothing new.
    _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
    with patch("backend.core.metadata_db.log_synthetic_usage") as log_synth:
        _db.backfill_fastly_edge_writes({"name": seeded_metadata_db})
        # log_synthetic_usage should NOT be called when the unbackfilled list is empty
        assert not log_synth.called
