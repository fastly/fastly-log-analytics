"""RUM ledger pipeline (celery-mode) — the RUM counterpart of
tests/core/test_step4_sweeper.py's regular-log ledger coverage.

Mirrors that file's rigor: real DuckDB connections with a real file-backed
DuckLake attach (tmp_path, not :memory:) and a real SQLite ledger (via
get_con), mocking only genuinely external things (the FOS/S3 client,
celery's .delay()).
"""

import gzip
import json
import time
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.ingest import (
    LEDGER_RECLAIM_AFTER_S,
    _parse_rum_beacon_file,
    convert_rum_object,
    discover_rum_prefix,
    sweep_rum_ledger_once,
)
from backend.core.metadata.base import get_con


def _clear_ledger(service_id: str):
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    con.commit()
    return con, cur


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


# ── discovery ──────────────────────────────────────────────────────────────


def test_discover_rum_prefix_defaults_to_rum_raw_and_inserts_ledger_rows():
    """discover_rum_prefix must LIST rum/raw/ (never plain raw/) and dispatch
    convert_rum — not convert — for every newly discovered file."""
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"

    con, cur = _clear_ledger(service_id)

    captured_kwargs = {}

    def mock_list_fos_files(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 999},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                with patch("backend.core.ingest.convert_rum.delay") as mock_convert_rum_delay:
                    discovered = discover_rum_prefix(service_id)
                    mock_convert_rum_delay.assert_called_once_with(service_id, object_key)

    assert discovered == 1
    assert captured_kwargs["prefix_subpath"] == "rum/raw/"

    row = cur.execute(
        "SELECT status, size_bytes FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["size_bytes"] == 999


def test_discover_rum_prefix_honors_explicit_minute_subpath():
    """The celery-mode discovery job dispatches discover_rum_prefix once per
    minute-scoped subpath (rum_minute_list_prefix) — confirm it's threaded
    through to list_fos_files unchanged."""
    service_id = "test-celery-rum-svc"
    _clear_ledger(service_id)

    captured_kwargs = {}

    def mock_list_fos_files(*args, **kwargs):
        captured_kwargs.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    minute_prefix = "rum/raw/year=2026/month=08/day=27/hour=10/minute=05/"
    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                discover_rum_prefix(service_id, prefix_subpath=minute_prefix)

    assert captured_kwargs["prefix_subpath"] == minute_prefix


# ── convert: parsing ──────────────────────────────────────────────────────


def test_parse_rum_beacon_file_splits_vitals_and_errors_and_quarantines_bad_line(tmp_path):
    """One beacon file: a flat rum_* vitals line, a flat rum_error_* line,
    and a truncated/unparseable line. The bad line must be reported back to
    the caller for quarantine, not silently dropped."""
    service_id = "svc-parse"
    vitals_line = json.dumps(
        {
            "timestamp": "2026-08-27T10:05:00Z",
            "rum_metric_name": "LCP",
            "rum_metric_value": "1234.5",
            "rum_metric_rating": "good",
            "browser": "Chrome",
            "pop": "SEA",
        }
    )
    error_line = json.dumps(
        {
            "timestamp": "2026-08-27T10:05:01Z",
            "rum_error_message": "TypeError: boom",
            "rum_error_file": "app.js",
            "rum_error_line": "42",
        }
    )
    bad_line = '{"timestamp": "2026-08-27T10:05:02Z", "rum_metric_name": "CLS"'  # truncated

    raw_file = tmp_path / "beacons.json"
    with gzip.open(raw_file, "wt", encoding="utf-8") as f:
        f.write(vitals_line + "\n" + error_line + "\n" + bad_line + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(str(raw_file), service_id)

    assert len(vitals_rows) == 1
    assert vitals_rows[0]["metric_name"] == "LCP"
    assert vitals_rows[0]["metric_value"] == 1234.5
    assert vitals_rows[0]["pop"] == "SEA"

    assert len(errors_rows) == 1
    assert errors_rows[0]["error_message"] == "TypeError: boom"
    assert errors_rows[0]["error_line"] == 42

    assert len(corrupt_lines) == 1
    assert corrupt_lines[0][0] == bad_line
    assert corrupt_lines[0][1] == "invalid_json"


def test_parse_rum_beacon_file_skips_rows_for_other_services():
    """A multi-tenant bucket line tagged for a different service is skipped
    silently — not treated as corruption."""
    service_id = "svc-mine"
    other_line = json.dumps({"timestamp": "2026-08-27T10:00:00Z", "service_id": "svc-other", "rum_metric_name": "LCP"})
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        path = tmp.name
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(other_line + "\n")

    vitals_rows, errors_rows, corrupt_lines = _parse_rum_beacon_file(path, service_id)
    assert vitals_rows == []
    assert errors_rows == []
    assert corrupt_lines == []


# ── convert: DuckLake writes ────────────────────────────────────────────────


def _fake_attach_factory(lake_file: str):
    def _fake_attach(con_arg, src_arg, read_only=False):
        try:
            con_arg.execute(f"ATTACH '{lake_file}' AS lake")
        except duckdb.Error:
            pass  # already attached on this connection
        return True

    return _fake_attach


def _write_gz_beacon(path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _run_convert_rum(service_id, object_key, local_file, lake_file):
    """Shared patch scaffold for convert_rum_object, mirroring the pattern
    in tests/core/test_step4_sweeper.py's
    test_convert_object_excludes_null_timestamp_rows_from_lake."""
    src = {"service_id": service_id, "name": service_id, "bucket": "test-bucket", "prefix": ""}
    _real_duckdb_connect = duckdb.connect

    def _fake_download(fos_client, s3_paths, tmpdir):
        return {s3_paths[0]: str(local_file)}, {}

    with patch("backend.core.duckdb.get_source_for_service", return_value=src):
        with patch("backend.core.ingest._get_fos_client", return_value=MagicMock()):
            with patch("backend.core.ingest._download_chunk_to_local", side_effect=_fake_download):
                with patch("duckdb.connect", side_effect=lambda *a, **kw: _real_duckdb_connect()):
                    with patch(
                        "backend.core.iceberg._ducklake._ducklake_attach",
                        side_effect=_fake_attach_factory(lake_file),
                    ):
                        with patch("backend.core.ingest._configure_fos"):
                            with patch(
                                "backend.core.iceberg._ducklake.ducklake_table_name",
                                side_effect=lambda src, table_name="logs": table_name,
                            ):
                                return convert_rum_object(service_id, object_key, "test-worker")


def test_convert_rum_object_splits_into_both_tables_and_commits(tmp_path):
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=05/beacons.json.gz"
    _clear_ledger(service_id)
    con = get_con(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps(
        {"timestamp": "2026-08-27T10:05:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1000"}
    )
    error_line = json.dumps({"timestamp": "2026-08-27T10:05:01Z", "rum_error_message": "boom"})
    raw_file = tmp_path / "beacons.json"
    _write_gz_beacon(raw_file, [vitals_line, error_line])

    lake_file = str(tmp_path / "lake.db")
    status = _run_convert_rum(service_id, object_key, raw_file, lake_file)

    assert status == "committed"

    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    vitals_count = check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0]
    errors_count = check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0]
    assert vitals_count == 1
    assert errors_count == 1

    src_file = check_con.execute("SELECT _source_file FROM lake.client_vitals").fetchone()[0]
    assert src_file == f"s3://test-bucket/{object_key}"

    row = con.execute(
        "SELECT status, committed_at FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "committed"
    assert row["committed_at"] is not None


def test_convert_rum_object_idempotent_under_redelivery(tmp_path):
    """Redelivering the same file (crash-after-commit, sweeper reclaim, or
    a plain retry) must not duplicate rows in either table."""
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=06/redeliver.json.gz"
    con, _ = _clear_ledger(service_id)

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:06:00Z", "rum_metric_name": "CLS", "rum_metric_value": "0.1"})
    error_line = json.dumps({"timestamp": "2026-08-27T10:06:01Z", "rum_error_message": "kaboom"})
    raw_file = tmp_path / "redeliver.json"
    _write_gz_beacon(raw_file, [vitals_line, error_line])
    lake_file = str(tmp_path / "lake.db")

    for _ in range(2):
        con.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) "
            "VALUES (?, ?, 'discovered', ?) ON CONFLICT (service_id, object_key) DO UPDATE SET status='discovered'",
            (service_id, object_key, time.time()),
        )
        con.commit()
        status = _run_convert_rum(service_id, object_key, raw_file, lake_file)
        assert status == "committed"

    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    assert check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1


def test_convert_rum_object_quarantines_malformed_line_and_still_commits_valid_rows(tmp_path):
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=07/mixed.json.gz"
    con, _ = _clear_ledger(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:07:00Z", "rum_metric_name": "FID", "rum_metric_value": "5"})
    bad_line = '{"timestamp": "2026-08-27T10:07:01Z", "rum_metric_name": "broken"'  # truncated
    raw_file = tmp_path / "mixed.json"
    _write_gz_beacon(raw_file, [vitals_line, bad_line])
    lake_file = str(tmp_path / "lake.db")

    src = {"service_id": service_id, "name": service_id, "bucket": "test-bucket", "prefix": ""}
    _real_duckdb_connect = duckdb.connect
    fos_mock = MagicMock()

    def _fake_download(fos_client, s3_paths, tmpdir):
        return {s3_paths[0]: str(raw_file)}, {}

    with patch("backend.core.duckdb.get_source_for_service", return_value=src):
        with patch("backend.core.ingest._get_fos_client", return_value=fos_mock):
            with patch("backend.core.ingest._download_chunk_to_local", side_effect=_fake_download):
                with patch("duckdb.connect", side_effect=lambda *a, **kw: _real_duckdb_connect()):
                    with patch(
                        "backend.core.iceberg._ducklake._ducklake_attach",
                        side_effect=_fake_attach_factory(lake_file),
                    ):
                        with patch("backend.core.ingest._configure_fos"):
                            with patch(
                                "backend.core.iceberg._ducklake.ducklake_table_name",
                                side_effect=lambda src, table_name="logs": table_name,
                            ):
                                with patch("backend.core.ingest.metadata_db.insert_quarantined_file") as mock_insert:
                                    status = convert_rum_object(service_id, object_key, "test-worker")

    assert status == "committed"

    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1

    # Quarantine sidecar written: the bad line + a .meta.json, exactly like
    # the regular-log path's _quarantine_convert_corrupt_lines protocol.
    assert fos_mock.put_object.call_count == 2
    keys = [call.kwargs["Key"] for call in fos_mock.put_object.call_args_list]
    assert any(k.endswith(".bad.jsonl") for k in keys)
    assert any(k.endswith(".meta.json") for k in keys)
    mock_insert.assert_called_once()
    assert mock_insert.call_args.kwargs["corrupt_rows"] == 1
    assert mock_insert.call_args.kwargs["valid_rows"] == 1


def test_convert_rum_object_partial_write_failure_does_not_mark_committed(tmp_path):
    """If the errors-table write fails after vitals succeeded, the ledger
    row must NOT be committed — a partial dual-table write is not a
    success. A retry converges once the failure is fixed.

    Triggers the failure organically (an ``error_line`` value that overflows
    CLIENT_ERRORS_ARROW_SCHEMA's int32 column, raising pyarrow.ArrowInvalid
    inside ``pa.Table.from_pylist``) rather than mocking pyarrow — its
    ``Table`` type is a Cython extension class and its methods can't be
    monkeypatched via ``unittest.mock``."""
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=08/partial.json.gz"
    con, _ = _clear_ledger(service_id)
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    vitals_line = json.dumps({"timestamp": "2026-08-27T10:08:00Z", "rum_metric_name": "TTFB", "rum_metric_value": "50"})
    # rum_error_line overflows int32 (CLIENT_ERRORS_ARROW_SCHEMA's error_line
    # column) — pa.Table.from_pylist raises ArrowInvalid building the
    # errors table, AFTER the vitals table has already been written.
    broken_error_line = json.dumps(
        {"timestamp": "2026-08-27T10:08:01Z", "rum_error_message": "boom", "rum_error_line": "99999999999999"}
    )
    fixed_error_line = json.dumps(
        {"timestamp": "2026-08-27T10:08:01Z", "rum_error_message": "boom", "rum_error_line": "42"}
    )
    raw_file = tmp_path / "partial.json"
    _write_gz_beacon(raw_file, [vitals_line, broken_error_line])
    lake_file = str(tmp_path / "lake.db")

    status = _run_convert_rum(service_id, object_key, raw_file, lake_file)

    assert status == "discovered"  # requeued, not committed

    row = con.execute(
        "SELECT status, committed_at, attempts FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    # The vitals table's write DID go through (its own transaction committed
    # before the errors table blew up) — a real DuckLake table now exists
    # with the one vitals row, even though the ledger never reached
    # 'committed'. A retry after fixing the input must not duplicate it.
    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    check_con.close()

    # Retry (the bad input is now "fixed") converges: vitals row isn't
    # duplicated, and errors now lands too.
    _write_gz_beacon(raw_file, [vitals_line, fixed_error_line])
    con.execute(
        "UPDATE ingest_ledger SET status='discovered' WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    con.commit()
    status2 = _run_convert_rum(service_id, object_key, raw_file, lake_file)
    assert status2 == "committed"

    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    assert check_con.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    assert check_con.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1


# ── sweep ────────────────────────────────────────────────────────────────


def test_sweep_rum_ledger_reclaims_stale_claim_and_redispatches_convert_rum():
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=09/stuck.json.gz"
    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, object_key, stale_claim, stale_claim - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_rum.delay") as mock_convert_rum_delay,
                    patch("backend.core.ingest.convert.delay") as mock_convert_delay,
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_batch_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)
                    mock_convert_rum_delay.assert_called_once_with(service_id, object_key)
                    mock_convert_delay.assert_not_called()
                    mock_convert_batch_delay.assert_not_called()

    assert summary["reclaimed"] == 1
    row = cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    ).fetchone()
    assert row["status"] == "discovered"
    assert row["claimed_by"] is None


def test_sweep_rum_ledger_does_not_touch_regular_log_rows():
    """Scoping check: a stale regular-log row for the same service must be
    left alone by the RUM sweep (and vice versa is exercised by the
    existing regular sweep_ledger_once tests)."""
    service_id = "test-celery-rum-svc"
    con, cur = _clear_ledger(service_id)
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    rum_key = "rum/raw/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    regular_key = "raw/year=2026/month=08/day=27/hour=10/minute=10/stuck.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'w1', ?, ?)",
        (service_id, rum_key, stale, stale - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'w1', ?, ?)",
        (service_id, regular_key, stale, stale - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_rum.delay") as mock_convert_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)
                    mock_convert_rum_delay.assert_called_once_with(service_id, rum_key)

    assert summary["reclaimed"] == 1
    regular_row = cur.execute(
        "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, regular_key)
    ).fetchone()
    assert regular_row["status"] == "claimed", "the regular-log row must be untouched by the RUM sweep"


def test_sweep_rum_ledger_skips_redispatch_when_queue_holds_backlog():
    service_id = "test-celery-rum-svc"
    object_key = "rum/raw/queued.gz"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time() - 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 50}, True)),
                    patch("backend.core.ingest.convert_rum.delay") as mock_convert_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(service_id)

    mock_convert_rum_delay.assert_not_called()
    assert summary["redispatched"] == 0
