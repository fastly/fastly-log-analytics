"""``convert_batch_objects`` failure/edge paths and
``_quarantine_convert_corrupt_lines``.

Companion to tests/core/test_step4_sweeper.py (sweeper + happy-path convert)
and tests/core/test_convert_batch.py. Real file-backed ``ducklake:`` attach,
real SQLite ledger; only the FOS/S3 client is mocked.
"""

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.ingest import (
    _quarantine_convert_corrupt_lines,
    convert_batch_objects,
    discover_prefix,
    get_ingest_columns_sql,
)
from backend.core.metadata.base import get_con
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    clear_ledger,
    ledger_row,
    seed_discovered,
)
from tests._helpers.ledger import (
    attacher as _attacher,
)
from tests._helpers.ledger import (
    download_stub as _download_stub,
)
from tests._helpers.ledger import (
    lake_reader as _lake_reader,
)

SERVICE_ID = "test-convert-object-edges"
BUCKET = "test-bucket"
SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}


def _seed_discovered(con, object_key: str) -> None:
    seed_discovered(con, con.cursor(), SERVICE_ID, [object_key])


def _ledger_row(con, object_key: str):
    return ledger_row(con, SERVICE_ID, object_key)


@contextmanager
def _convert_env(tmp_path, download, fos=None, attach=None):
    with DuckLakeLedgerEnv(
        SERVICE_ID,
        tmp_path,
        download,
        bucket=BUCKET,
        fos=fos,
        attach=attach,
        table_name="logs",
    ) as real_attach:
        yield real_attach


def _write_ndjson(path, records: list[dict]) -> str:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return str(path)


# ── precondition failures ─────────────────────────────────────────────────


def test_convert_object_with_no_registered_source_stays_retryable():
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=00/nosrc.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "no source registered" in row["last_error"]
    assert row["committed_at"] is None


def test_convert_object_with_failed_lake_attach_stays_retryable(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=01/noattach.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    local = _write_ndjson(tmp_path / "noattach.json", [{"timestamp": "2026-08-27T15:01:00Z", "url": "/a"}])
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=lambda *a, **kw: False,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "attach failed" in row["last_error"]


# ── dead key vs transient download failure ────────────────────────────────


def test_convert_object_dead_letters_a_key_whose_object_is_gone(tmp_path):
    """A resurrected ledger row for a file the old pipeline already ingested
    and deleted is terminal — otherwise the sweeper re-GETs it forever."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=02/vanished.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (NoSuchKey) when calling the HeadObject operation")
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["dead_letter"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "dead_letter"
    assert row["attempts"] == 0  # a decision, not a failed attempt
    assert "missing from FOS" in row["last_error"]
    fos.head_object.assert_called_once_with(Bucket=BUCKET, Key=object_key)


def test_convert_object_treats_download_failure_on_a_live_object_as_transient(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=03/blip.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 42}
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "transient" in row["last_error"]


# ── writing onto a pre-existing lake table ────────────────────────────────


def test_convert_object_widens_the_lake_table_for_a_new_custom_field(tmp_path):
    """Custom log fields appear over time. An existing table missing a
    column must be ALTERed rather than the insert failing — a failed widen
    would stall ingest for the whole service the moment a field is added."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=04/widen.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP, url VARCHAR, _source_file VARCHAR)")
    setup.execute("INSERT INTO lake.logs VALUES ('2026-08-01T00:00:00', '/pre-existing', 's3://other/old.gz')")
    setup.close()

    local = _write_ndjson(
        tmp_path / "widen.json",
        [{"timestamp": "2026-08-27T15:04:00Z", "url": "/new", "status": 200}],
    )
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=attach,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    counters = json.loads(
        con.execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        ).fetchone()[0]
    )
    assert counters["valid_records"] == 1
    assert counters["objects_processed"] == 1
    assert counters["objects_successful"] == 1
    assert counters["objects_failed"] == 0
    reader = _lake_reader(attach, read_only=True)
    cols = {r[0] for r in reader.execute("DESCRIBE lake.logs").fetchall()}
    assert "status" in cols, "the new field must have been added to the table"
    rows = reader.execute("SELECT url, _source_file FROM lake.logs ORDER BY url").fetchall()
    reader.close()
    # The pre-existing row is untouched; the new one is attributed to this key.
    assert rows == [("/new", f"s3://{BUCKET}/{object_key}"), ("/pre-existing", "s3://other/old.gz")]


def test_convert_object_rolls_back_and_stays_retryable_when_the_insert_cannot_land(tmp_path):
    """A lake table whose column type the incoming data cannot cast to must
    abort inside the transaction: no partial rows, no committed ledger
    row."""
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=05/drift.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    # url typed INTEGER: '/a/b' cannot cast, so the INSERT raises.
    setup.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP, url INTEGER, _source_file VARCHAR)")
    setup.close()

    local = _write_ndjson(tmp_path / "drift.json", [{"timestamp": "2026-08-27T15:05:00Z", "url": "/a/b"}])
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=attach,
    ):
        summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
        assert summary["failed"] == 1

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 0
    reader.close()


# ── evidence capture failure must be reported without losing valid rows ────


def test_convert_object_commits_even_when_the_quarantine_check_blows_up(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=06/qfail.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    local_path = tmp_path / "qfail.json"
    local = _write_ndjson(local_path, [{"timestamp": "2026-08-27T15:06:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.quarantine.capture_evidence",
            return_value={"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1},
        ):
            local_path.write_text('{"timestamp":"2026-08-27T15:06:00Z","url":"/a"}\n{"url":"/bad"}\n')
            summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    counters = json.loads(
        con.execute(
            "SELECT outcome_counters FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        ).fetchone()[0]
    )
    assert counters["valid_records"] == 1
    assert counters["malformed_records"] == 1
    assert counters["quarantine_capture_failures"] == 1
    assert counters["objects_failed"] == 1
    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
    reader.close()


def test_convert_object_commits_even_when_ingested_files_bookkeeping_fails(tmp_path):
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=07/bkfail.json.gz"
    con, _ = clear_ledger(SERVICE_ID)
    _seed_discovered(con, object_key)

    local = _write_ndjson(tmp_path / "bkfail.json", [{"timestamp": "2026-08-27T15:07:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            summary = convert_batch_objects(SERVICE_ID, [object_key], "w1")
            assert summary["committed"] == 1

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
    reader.close()


# ── discover_prefix guard rails ───────────────────────────────────────────


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def _null_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return None


def test_discover_prefix_is_a_noop_without_config_or_source():
    con, _ = clear_ledger(SERVICE_ID)

    with patch("backend.config.load_config", return_value=None):
        with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
            assert discover_prefix(SERVICE_ID) == 0
            mock_list.assert_not_called()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
                assert discover_prefix(SERVICE_ID) == 0
                mock_list.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 0


def test_discover_prefix_returns_zero_when_the_list_yields_no_result():
    """An aborted LIST is not an empty bucket — reporting it as a successful
    scan of zero files would let the caller advance its watermark."""
    clear_ledger(SERVICE_ID)
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=_null_list_fos):
                with patch("backend.core.ingest.convert_batch_files.delay") as mock_delay:
                    assert discover_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()


def test_discover_prefix_omits_prefix_subpath_when_not_given():
    """Unlike the RUM twin, discover_prefix must NOT pass a prefix_subpath
    of its own — list_fos_files' default (the regular-log root) is the
    intended scan."""
    clear_ledger(SERVICE_ID)
    captured: dict = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=_capture):
                assert discover_prefix(SERVICE_ID) == 0

    assert "prefix_subpath" not in captured
    assert captured["start_time"] is None


def test_discover_prefix_persists_the_originating_cron_run():
    clear_ledger(SERVICE_ID)
    object_key = "raw/request/year=2026/month=08/day=27/hour=13/minute=05/part.json.gz"

    def list_one_file(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://{BUCKET}/{object_key}"],
            "file_sizes": {f"s3://{BUCKET}/{object_key}": 128},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=SRC):
            with patch("backend.core.ingest.list_fos_files", side_effect=list_one_file):
                with patch("backend.core.ingest.convert_batch_files.delay") as dispatch:
                    assert discover_prefix(SERVICE_ID, run_id=812) == 1
                    dispatch.assert_called_once_with(SERVICE_ID, [object_key])
                    assert (
                        discover_prefix(
                            SERVICE_ID,
                            run_id=913,
                            originating_task="full_sync",
                        )
                        == 0
                    )
                    dispatch.assert_called_once_with(SERVICE_ID, [object_key])

    row = (
        get_con(SERVICE_ID)
        .execute(
            "SELECT originating_task, originating_run_id FROM ingest_ledger WHERE service_id=? AND object_key=?",
            (SERVICE_ID, object_key),
        )
        .fetchone()
    )
    assert row["originating_task"] == "log_discovery"
    assert row["originating_run_id"] == 812


# ── _quarantine_convert_corrupt_lines ─────────────────────────────────────


def _read_expr(local_file: str) -> str:
    return (
        f"read_json_auto('{local_file}', format='newline_delimited', "
        f"records='auto', columns={get_ingest_columns_sql(None)}, ignore_errors=true)"
    )


def _clear_quarantine(service_id: str):
    con = get_con(service_id)
    con.execute("DELETE FROM quarantined_files")
    con.commit()


def test_quarantine_convert_corrupt_lines_records_a_truncated_line(tmp_path):
    """The reference case: a truncated line becomes an all-NULL row under
    ignore_errors=true, is excluded from the lake insert by the caller, and
    must still be reported so the row-count shortfall is explainable."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "mixed.json"
    local.write_text(
        json.dumps({"timestamp": "2026-08-27T15:08:00Z", "url": "/ok"})
        + "\n"
        + '{"timestamp": "2026-08-27T15:08:01Z", "url": "/trunc"\n'
    )
    object_key = "raw/request/year=2026/month=08/day=27/hour=15/minute=08/mixed.json.gz"
    fos = MagicMock()
    con = duckdb.connect()

    with patch("backend.core.quarantine.capture_evidence") as capture:
        capture.return_value = {"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0}
        result = _quarantine_convert_corrupt_lines(con, fos, SRC, _read_expr(str(local)), str(local), object_key)
    con.close()

    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 1,
        "malformed_records": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }
    capture.assert_called_once()
    assert capture.call_args.args == (
        SERVICE_ID,
        "request",
        object_key,
        b'{"timestamp": "2026-08-27T15:08:01Z", "url": "/trunc"\n',
    )


def test_request_worker_quarantine_captures_exact_line_bytes_without_fos_upload(tmp_path, monkeypatch):
    from backend.core import quarantine

    local = tmp_path / "mixed.json"
    valid_line = b'{"timestamp":"2026-08-27T15:08:00Z","url":"/ok"}\n'
    bad_line = b'{"timestamp":"2026-08-27T15:08:01Z","url":"/bad"\n'
    local.write_bytes(valid_line + bad_line)
    calls = []

    def capture(*args, **kwargs):
        calls.append((args, kwargs))
        return {"id": 12, "cap_evictions": 1, "quarantine_capture_failures": 0}

    monkeypatch.setattr(quarantine, "capture_evidence", capture)
    con = duckdb.connect()
    try:
        result = _quarantine_convert_corrupt_lines(
            con,
            MagicMock(),
            SRC,
            _read_expr(str(local)),
            str(local),
            "raw/request/mixed.json.gz",
        )
    finally:
        con.close()

    assert result == {
        "valid_records": 1,
        "malformed_records": 1,
        "cap_evictions": 1,
        "quarantine_capture_failures": 0,
    }
    assert len(calls) == 1
    assert calls[0][0][0:3] == (SERVICE_ID, "request", "raw/request/mixed.json.gz")
    assert calls[0][0][3] == bad_line
    assert calls[0][1]["line_ordinal"] == 2
    assert calls[0][1]["byte_offset"] == len(valid_line)


def test_quarantine_convert_corrupt_lines_does_nothing_for_a_clean_file(tmp_path):
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "clean.json"
    local.write_text(json.dumps({"timestamp": "2026-08-27T15:09:00Z", "url": "/ok"}) + "\n")
    fos = MagicMock()
    con = duckdb.connect()

    result = _quarantine_convert_corrupt_lines(
        con,
        fos,
        SRC,
        _read_expr(str(local)),
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=09/clean.json.gz",
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["malformed_records"] == 0


def test_quarantine_convert_corrupt_lines_skips_upload_when_the_detectors_disagree(tmp_path):
    """The row-level NULL count and the raw-line scan are independent
    detectors, kept independent on purpose. A line whose timestamp is
    present-but-uncastable trips the first and not the second: there is
    nothing concrete to attach to a sidecar, so nothing may be uploaded or
    registered (an empty .bad.jsonl would be worse than a log line)."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "disagree.json"
    local.write_text(json.dumps({"timestamp": "definitely-not-a-timestamp", "url": "/x"}) + "\n")
    fos = MagicMock()
    con = duckdb.connect()

    valid, corrupt = con.execute(
        f"SELECT count(*) FILTER (timestamp IS NOT NULL), count(*) FILTER (timestamp IS NULL) "
        f"FROM {_read_expr(str(local))}"
    ).fetchone()
    assert (valid, corrupt) == (0, 1), "precondition: the row-level detector must see this as corrupt"

    result = _quarantine_convert_corrupt_lines(
        con,
        fos,
        SRC,
        _read_expr(str(local)),
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=10/disagree.json.gz",
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["quarantine_capture_failures"] == 1


def test_quarantine_convert_corrupt_lines_skips_a_key_outside_the_raw_prefix(tmp_path):
    """The errors key is built by slicing the raw prefix off the object key —
    a key that does not start with it must be skipped, not mangled into an
    unrelated destination."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "offprefix.json"
    local.write_text('{"timestamp": "2026-08-27T15:11:00Z", "url": "/trunc"\n')
    fos = MagicMock()
    con = duckdb.connect()

    result = _quarantine_convert_corrupt_lines(
        con, fos, SRC, _read_expr(str(local)), str(local), "somewhere/else/offprefix.json.gz"
    )
    con.close()

    fos.put_object.assert_not_called()
    assert result["quarantine_capture_failures"] == 1


def test_quarantine_convert_corrupt_gzip_captures_the_original_container_bytes(tmp_path):
    payload = b"not a gzip container\x00\xff"
    local = tmp_path / "broken.json.gz"
    local.write_bytes(payload)
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (0, 1)
    key = "raw/request/year=2026/month=08/day=27/hour=15/minute=12/broken.json.gz"

    with patch("backend.core.quarantine.capture_evidence") as capture:
        capture.return_value = {"cap_evictions": 1, "quarantine_capture_failures": 0}
        result = _quarantine_convert_corrupt_lines(con, MagicMock(), SRC, "ignored", str(local), key)

    assert result == {
        "valid_records": 0,
        "malformed_records": 0,
        "corrupt_containers": 1,
        "cap_evictions": 1,
        "quarantine_capture_failures": 0,
    }
    assert capture.call_args.args == (SERVICE_ID, "request", key, payload)
    assert capture.call_args.kwargs["error_category"] == "corrupt_container"


def test_quarantine_convert_corrupt_gzip_reports_when_container_cannot_be_read(tmp_path, monkeypatch):
    local = tmp_path / "unreadable.json.gz"
    local.write_bytes(b"not gzip")
    real_open = open

    def fail_open(path, *args, **kwargs):
        if str(path) == str(local):
            raise OSError("local cache unavailable")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fail_open)
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = (0, 1)
    result = _quarantine_convert_corrupt_lines(
        con,
        MagicMock(),
        SRC,
        "ignored",
        str(local),
        "raw/request/year=2026/month=08/day=27/hour=15/minute=13/unreadable.json.gz",
    )

    assert result == {
        "valid_records": 0,
        "malformed_records": 0,
        "corrupt_containers": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 1,
    }
