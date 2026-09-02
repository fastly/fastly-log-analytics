"""Ledger sweeper (celery-mode crash net) behavior.

Fixture timestamps are epoch floats because that is what the PRODUCER
writes — convert stamps ``claimed_at = time.time()`` and discovery stamps
``discovered_at = time.time()`` (see backend/core/ingest.py).
"""

import time
from unittest.mock import patch

from backend.core.ingest import LEDGER_RECLAIM_AFTER_S, sweep_ledger_once
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


def test_kill_worker_mid_convert_heal_test(monkeypatch):
    """A claim older than the reclaim window is reset to 'discovered' and
    re-dispatched — the crash-recovery path for a dead worker."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

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
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["reclaimed"] == 1
    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["claimed_by"] is None


def test_sweeper_redispatches_stuck_discovered_rows(monkeypatch):
    """A row stuck in 'discovered' (its convert message was lost to a broker
    restart / crash-after-insert) is re-dispatched. This is the terminal
    state the original sweeper could never heal: its re-INSERT hit ON
    CONFLICT DO NOTHING (rowcount 0) so the file was never ingested."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/07/lost_dispatch.json.gz"

    con, cur = _clear_ledger(service_id)
    old = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, old),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["redispatched"] == 1


def test_sweeper_excludes_rum_keys_from_reclaim_and_redispatch(monkeypatch):
    """A stale rum/raw/ row must be left alone by sweep_ledger_once — that's
    sweep_rum_ledger_once's job. Without the object_key exclusion, this
    sweep would reclaim/redispatch it via convert_batch_files.delay (the regular-log
    parser), misparsing the beacon file into the logs table."""
    service_id = "test-celery-svc"
    rum_key = "rum/raw/2026/08/27/10/05/beacons.json.gz"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"

    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    old_discovered = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, rum_key, stale_claim, stale_claim - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, log_key, old_discovered),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    # Only the regular-log key was reclaimed/redispatched — the RUM key
    # untouched (still 'claimed', dead-worker's lease), never handed to
    # the regular-log convert task.
    dispatched_keys = [k for call in mock_convert_delay.call_args_list for k in call.args[1]]
    assert log_key in dispatched_keys
    assert rum_key not in dispatched_keys
    assert summary["reclaimed"] == 0
    assert summary["redispatched"] == 1

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, rum_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "dead-worker-1"


def test_sweeper_discovers_unseen_files(monkeypatch):
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/06/missed_logs.json.gz"

    con, cur = _clear_ledger(service_id)

    def mock_list_fos_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 1234},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                with patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay:
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["discovered"] == 1
    cur.execute(
        "SELECT status, size_bytes, discovered_at FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["size_bytes"] == 1234
    assert row["discovered_at"] is not None


def test_convert_failure_requeues_then_quarantines(monkeypatch):
    """convert_object records failures on the ledger row: attempts increment,
    the row requeues as 'discovered', and after LEDGER_MAX_ATTEMPTS it is
    quarantined instead of retrying forever (the old code leaked the row as
    'claimed' and the sweeper retried it every cycle, invisibly)."""
    from backend.core.ingest import LEDGER_MAX_ATTEMPTS, convert_object

    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/08/poison.json.gz"

    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    def _boom(*args, **kwargs):
        raise RuntimeError("poison file")

    statuses = []
    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with (
                patch("backend.core.ingest._get_fos_client"),
                patch("backend.core.ingest._configure_fos", side_effect=_boom),
            ):
                for _ in range(LEDGER_MAX_ATTEMPTS):
                    statuses.append(convert_object(service_id, object_key, "test-worker"))

    assert statuses[:-1] == ["discovered"] * (LEDGER_MAX_ATTEMPTS - 1)
    assert statuses[-1] == "quarantined"

    row = con.execute(
        "SELECT status, attempts, last_error FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "quarantined"
    assert row["attempts"] == LEDGER_MAX_ATTEMPTS
    assert "poison file" in row["last_error"]


def test_finalize_committed_raw_deletes_and_stamps(monkeypatch):
    """Durable-committed rows past the grace window get their raw file
    deleted (delete_after on) and are stamped raw_deleted_at so re-runs
    are idempotent; fresh commits inside the grace window are untouched."""
    from unittest.mock import MagicMock

    from backend.core.ingest import RAW_DELETE_GRACE_S, finalize_committed_raw

    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    old = time.time() - RAW_DELETE_GRACE_S - 60
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, committed_at) VALUES (?, 'raw/old1.gz', 'committed', ?)",
        (service_id, old),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, committed_at) VALUES (?, 'raw/fresh.gz', 'committed', ?)",
        (service_id, time.time()),
    )
    con.commit()

    fos = MagicMock()
    fos.delete_objects.return_value = {"Errors": []}
    with patch(
        "backend.config.load_config",
        return_value={"service_id": service_id, "provisioning": {"cron_sync": {"delete_after": True}}},
    ):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest._get_fos_client", return_value=fos):
                result = finalize_committed_raw(service_id)

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    fos.delete_objects.assert_called_once()
    assert fos.delete_objects.call_args.kwargs["Delete"]["Objects"] == [{"Key": "raw/old1.gz"}]
    rows = dict(
        con.execute("SELECT object_key, raw_deleted_at FROM ingest_ledger WHERE service_id=?", (service_id,)).fetchall()
    )
    assert rows["raw/old1.gz"] is not None
    assert rows["raw/fresh.gz"] is None

    # Idempotent: second run finds nothing eligible.
    with patch(
        "backend.config.load_config",
        return_value={"service_id": service_id, "provisioning": {"cron_sync": {"delete_after": True}}},
    ):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest._get_fos_client", return_value=fos):
                again = finalize_committed_raw(service_id)
    assert again["deleted"] == 0 and again["eligible"] == 0


def test_finalize_committed_raw_respects_delete_after_off(monkeypatch):
    from unittest.mock import MagicMock

    from backend.core.ingest import RAW_DELETE_GRACE_S, finalize_committed_raw

    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, committed_at) VALUES (?, 'raw/keep.gz', 'committed', ?)",
        (service_id, time.time() - RAW_DELETE_GRACE_S - 60),
    )
    con.commit()

    fos = MagicMock()
    with patch(
        "backend.config.load_config",
        return_value={"service_id": service_id, "provisioning": {"cron_sync": {"delete_after": False}}},
    ):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest._get_fos_client", return_value=fos):
                result = finalize_committed_raw(service_id)

    assert result == {"deleted": 0, "eligible": 1, "delete_after": False}
    fos.delete_objects.assert_not_called()


def test_sweeper_skips_redispatch_when_queue_holds_backlog(monkeypatch):
    """When q.ingest already holds >= pending messages, nothing was lost —
    re-dispatching would just multiply duplicates every sweep (observed
    live: 5k pending ballooned to 25k queued messages)."""
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, 'raw/queued.gz', 'discovered', ?)",
        (service_id, time.time() - 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 50}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    mock_convert_delay.assert_not_called()
    assert summary["redispatched"] == 0


def _read_expr_for(raw_file, columns_sql="{'timestamp': 'VARCHAR', 'url': 'VARCHAR'}") -> str:
    from backend.utils.sql_validator import escape_sql_literal

    safe_local = escape_sql_literal(str(raw_file))
    return (
        f"read_json_auto('{safe_local}', format='newline_delimited', "
        f"records='auto', columns={columns_sql}, ignore_errors=true)"
    )


def test_quarantine_convert_corrupt_lines_detects_and_uploads(tmp_path):
    """A malformed JSON line: ignore_errors=true does NOT drop it, it
    inserts an all-NULL row (verified empirically against real DuckDB) —
    that NULL-timestamp row is the actual corruption signal this function
    must detect and quarantine, not a row-count mismatch."""
    import json as _json
    from unittest.mock import MagicMock, patch

    import duckdb

    from backend.core.ingest import _quarantine_convert_corrupt_lines

    raw_file = tmp_path / "raw.json"
    valid_line_1 = _json.dumps({"timestamp": "2026-01-01T00:00:00Z", "url": "/a"})
    valid_line_2 = _json.dumps({"timestamp": "2026-01-01T00:00:01Z", "url": "/b"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:02Z", "url": "/c"'  # truncated JSON
    raw_file.write_text(f"{valid_line_1}\n{valid_line_2}\n{corrupt_line}\n")

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)
    # Sanity: confirm the row DuckDB produces for the truncated line is a
    # NULL row, not a dropped one — this is the premise the function relies on.
    assert con.execute(f"SELECT count(*) FROM {read_expr}").fetchone()[0] == 3
    assert con.execute(f"SELECT count(*) FROM {read_expr} WHERE timestamp IS NULL").fetchone()[0] == 1

    fos = MagicMock()
    src = {"service_id": "svc-quarantine", "name": "svc-quarantine", "bucket": "test-bucket", "prefix": ""}

    with patch("backend.core.ingest.metadata_db.insert_quarantined_file") as mock_insert:
        _quarantine_convert_corrupt_lines(con, fos, src, read_expr, str(raw_file), "raw/y=2026/file.gz")

    assert fos.put_object.call_count == 2  # .bad.jsonl + .meta.json
    keys = [call.kwargs["Key"] for call in fos.put_object.call_args_list]
    assert any(k.endswith(".bad.jsonl") for k in keys)
    assert any(k.endswith(".meta.json") for k in keys)

    mock_insert.assert_called_once()
    kwargs = mock_insert.call_args.kwargs
    assert kwargs["corrupt_rows"] == 1
    assert kwargs["valid_rows"] == 2
    assert kwargs["service_id"] == "svc-quarantine"


def test_quarantine_convert_corrupt_lines_noop_when_nothing_dropped(tmp_path):
    """The common case (no corrupt lines) must touch neither FOS nor the
    quarantine table."""
    from unittest.mock import MagicMock, patch

    import duckdb

    from backend.core.ingest import _quarantine_convert_corrupt_lines

    raw_file = tmp_path / "raw.json"
    raw_file.write_text('{"timestamp": "2026-01-01T00:00:00Z", "url": "/a"}\n')

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)

    fos = MagicMock()
    src = {"service_id": "svc-clean", "name": "svc-clean", "bucket": "test-bucket", "prefix": ""}

    with patch("backend.core.ingest.metadata_db.insert_quarantined_file") as mock_insert:
        _quarantine_convert_corrupt_lines(con, fos, src, read_expr, str(raw_file), "raw/file.gz")

    fos.put_object.assert_not_called()
    mock_insert.assert_not_called()


def test_convert_object_excludes_null_timestamp_rows_from_lake(tmp_path, monkeypatch):
    """End-to-end guard on the bug this whole fix closes: a corrupt line
    must never land in the queryable lake table, even though
    ignore_errors=true alone would have put a NULL row there."""
    import json as _json
    from unittest.mock import MagicMock, patch

    import duckdb

    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    object_key = "raw/2026/08/27/10/09/mixed.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    raw_file = tmp_path / "mixed.json"
    valid_line = _json.dumps({"timestamp": "2026-01-01T00:00:00Z"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:01Z"'  # truncated
    raw_file.write_text(f"{valid_line}\n{corrupt_line}\n")

    def _fake_download(fos_client, s3_paths, tmpdir):
        return {s3_paths[0]: str(raw_file)}, {}

    src = {"service_id": service_id, "name": service_id, "bucket": "test-bucket", "prefix": ""}
    # File-backed (not :memory:) so state survives convert_object closing
    # ITS connection to the shared "lake" attach — a real DuckDB file, like
    # the real DuckLake catalog, persists across connect/close/reconnect;
    # a fresh :memory: per connection would not.
    lake_file = str(tmp_path / "lake.db")
    _real_duckdb_connect = duckdb.connect

    def _fake_attach(con_arg, src_arg, read_only=False):
        try:
            con_arg.execute(f"ATTACH '{lake_file}' AS lake")
        except duckdb.Error:
            pass  # already attached on this connection
        return True

    with patch("backend.core.duckdb.get_source_for_service", return_value=src):
        with patch("backend.core.ingest._get_fos_client", return_value=MagicMock()):
            with patch("backend.core.ingest._download_chunk_to_local", side_effect=_fake_download):
                with patch("duckdb.connect", side_effect=lambda *a, **kw: _real_duckdb_connect()):
                    with patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_fake_attach):
                        with patch("backend.core.ingest._configure_fos"):
                            with patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"):
                                from backend.core.ingest import convert_object

                                status = convert_object(service_id, object_key, "test-worker")

    assert status == "committed"
    check_con = duckdb.connect()
    check_con.execute(f"ATTACH '{lake_file}' AS lake (READ_ONLY)")
    rows = check_con.execute("SELECT count(*) FROM lake.logs").fetchone()[0]
    null_rows = check_con.execute("SELECT count(*) FROM lake.logs WHERE timestamp IS NULL").fetchone()[0]
    assert rows == 1, "only the valid row should have been written to the lake table"
    assert null_rows == 0, "a corrupt row must never land in the queryable lake table"

    # ingested_files parity: every existing reader (Usage Log /
    # log-line-accounting reconciliation, admin ingested-files list) must
    # keep working for celery-mode services instead of reading zero.
    from backend.core.metadata.ingest_log import get_ingested_files_status_summary

    summary = get_ingested_files_status_summary(service_id)
    assert summary.get("total_rows") == 1, "ingested_files must record only the valid row, not the corrupt one"


def test_merge_lake_files_flushes_inlined_rows_to_parquet(tmp_path):
    """DuckLake inlines small inserts into the metadata catalog instead of
    writing parquet, and NEITHER ducklake_merge_adjacent_files nor
    ducklake_rewrite_data_files promotes them. Since finalize_committed_raw
    deletes the raw .gz after commit, an unflushed catalog leaves the ONLY
    copy of the data in the catalog itself — no FOS parquet, no raw file to
    re-ingest. This pins the flush that makes committed data durable.

    Uses a REAL DuckLake attach (not a plain DuckDB one) because inlining
    is DuckLake-specific behavior a plain attach would never exercise.
    """
    from unittest.mock import patch

    import duckdb

    service_id = "test-flush-svc"
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "data")
    src = {"service_id": service_id, "name": service_id, "bucket": "b", "prefix": ""}

    def _real_ducklake_attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = " , READ_ONLY" if read_only else ""
        try:
            con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
        except duckdb.Error:
            pass  # already attached on this connection
        return True

    seed = duckdb.connect()
    _real_ducklake_attach(seed, src)
    seed.execute("CREATE TABLE lake.logs (ts TIMESTAMP, v INT)")
    for i in range(5):
        seed.execute(f"INSERT INTO lake.logs VALUES (now(), {i})")
    # Premise: every commit so far is inlined — zero materialized files.
    assert seed.execute("SELECT file_count FROM ducklake_table_info('lake')").fetchone()[0] == 0
    seed.close()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_real_ducklake_attach),
    ):
        from backend.core.ingest import merge_lake_files

        merge_lake_files(service_id)

    check = duckdb.connect()
    _real_ducklake_attach(check, src, read_only=True)
    file_count, file_bytes = check.execute(
        "SELECT file_count, file_size_bytes FROM ducklake_table_info('lake')"
    ).fetchone()
    assert file_count > 0, "inlined rows must be flushed to real parquet files, or the data isn't durable"
    assert file_bytes > 0
    # Lossless: the flush promotes rows, it must not drop them.
    assert check.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 5
