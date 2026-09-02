"""``convert_object`` failure/edge paths and
``_quarantine_convert_corrupt_lines``.

``convert_object`` is the single-key convert the sweeper falls back to for
dead-key retries and for any pre-deploy message still in flight, so its
failure classification is what decides whether a key is retried forever,
retried once, or given up on:

* a ``committed`` row means ``finalize_committed_raw`` may delete the raw
  ``.gz``, so nothing may reach ``committed`` unless the rows are actually in
  the lake;
* a key whose object is genuinely gone must go ``dead_letter``, or every
  sweep burns LEDGER_MAX_ATTEMPTS GETs on it forever;
* a key whose object is still there must stay retryable — dead-lettering it
  drops a file that is sitting in the bucket.

Companion to tests/core/test_step4_sweeper.py (sweeper + happy-path convert)
and tests/core/test_convert_batch.py (the batched twin). Real file-backed
``ducklake:`` attach, real SQLite ledger; only the FOS/S3 client is mocked.
"""

import json
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.ingest import (
    _quarantine_convert_corrupt_lines,
    convert_object,
    discover_prefix,
    get_ingest_columns_sql,
)
from backend.core.metadata.base import get_con
from backend.core.metadata.quarantine import list_quarantined_files

SERVICE_ID = "test-convert-object-edges"
BUCKET = "test-bucket"
SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}


def _clear_ledger():
    con = get_con(SERVICE_ID)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,))
    con.commit()
    return con, cur


def _seed_discovered(con, object_key: str) -> None:
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) "
        "VALUES (?, ?, 'discovered', ?) ON CONFLICT (service_id, object_key) DO UPDATE SET "
        "status='discovered', attempts=0, last_error=NULL, committed_at=NULL",
        (SERVICE_ID, object_key, time.time()),
    )
    con.commit()


def _ledger_row(con, object_key: str):
    return con.execute(
        "SELECT status, attempts, last_error, committed_at FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (SERVICE_ID, object_key),
    ).fetchone()


def _attacher(catalog: str, data_path: str):
    def _attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if read_only else ""
        try:
            con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
        except duckdb.Error:
            pass  # already attached on this connection
        return True

    return _attach


def _download_stub(mapping: dict[str, str]):
    def _download(fos_client, s3_paths, tmpdir):
        got = {p: mapping[p] for p in s3_paths if p in mapping}
        return got, {v: k for k, v in got.items()}

    return _download


@contextmanager
def _convert_env(tmp_path, download, fos=None, attach=None):
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    real_attach = _attacher(catalog, data_path)
    with (
        patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}),
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.ingest._get_fos_client", return_value=fos if fos is not None else MagicMock()),
        patch("backend.core.ingest._download_chunk_to_local", side_effect=download),
        patch(
            "backend.core.iceberg._ducklake._ducklake_attach",
            side_effect=attach if attach is not None else real_attach,
        ),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
    ):
        yield real_attach


def _write_ndjson(path, records: list[dict]) -> str:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return str(path)


def _lake_reader(attach):
    con = duckdb.connect()
    attach(con, None, read_only=True)
    return con


# ── precondition failures ─────────────────────────────────────────────────


def test_convert_object_with_no_registered_source_stays_retryable():
    object_key = "raw/2026/08/27/15/00/nosrc.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        assert convert_object(SERVICE_ID, object_key, "w1") == "error"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "no source registered" in row["last_error"]
    assert row["committed_at"] is None


def test_convert_object_with_failed_lake_attach_stays_retryable(tmp_path):
    object_key = "raw/2026/08/27/15/01/noattach.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    local = _write_ndjson(tmp_path / "noattach.json", [{"timestamp": "2026-08-27T15:01:00Z", "url": "/a"}])
    with _convert_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": local}),
        attach=lambda *a, **kw: False,
    ):
        assert convert_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "attach failed" in row["last_error"]


# ── dead key vs transient download failure ────────────────────────────────


def test_convert_object_dead_letters_a_key_whose_object_is_gone(tmp_path):
    """A resurrected ledger row for a file the old pipeline already ingested
    and deleted is terminal — otherwise the sweeper re-GETs it forever."""
    object_key = "raw/2026/08/27/15/02/vanished.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (NoSuchKey) when calling the HeadObject operation")
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        assert convert_object(SERVICE_ID, object_key, "w1") == "dead_letter"

    row = _ledger_row(con, object_key)
    assert row["status"] == "dead_letter"
    assert row["attempts"] == 0  # a decision, not a failed attempt
    assert "missing from FOS" in row["last_error"]
    fos.head_object.assert_called_once_with(Bucket=BUCKET, Key=object_key)


def test_convert_object_treats_download_failure_on_a_live_object_as_transient(tmp_path):
    object_key = "raw/2026/08/27/15/03/blip.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 42}
    with _convert_env(tmp_path, _download_stub({}), fos=fos):
        assert convert_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "transient" in row["last_error"]


# ── writing onto a pre-existing lake table ────────────────────────────────


def test_convert_object_widens_the_lake_table_for_a_new_custom_field(tmp_path):
    """Custom log fields appear over time. An existing table missing a
    column must be ALTERed rather than the insert failing — a failed widen
    would stall ingest for the whole service the moment a field is added."""
    object_key = "raw/2026/08/27/15/04/widen.json.gz"
    con, _ = _clear_ledger()
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
    with (
        patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}),
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.ingest._get_fos_client", return_value=MagicMock()),
        patch(
            "backend.core.ingest._download_chunk_to_local",
            side_effect=_download_stub({f"s3://{BUCKET}/{object_key}": local}),
        ),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
    ):
        assert convert_object(SERVICE_ID, object_key, "w1") == "committed"

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
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
    object_key = "raw/2026/08/27/15/05/drift.json.gz"
    con, _ = _clear_ledger()
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
    with (
        patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}),
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.ingest._get_fos_client", return_value=MagicMock()),
        patch(
            "backend.core.ingest._download_chunk_to_local",
            side_effect=_download_stub({f"s3://{BUCKET}/{object_key}": local}),
        ),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
    ):
        assert convert_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 0
    reader.close()


# ── best-effort reporting must never fail a committed convert ─────────────


def test_convert_object_commits_even_when_the_quarantine_check_blows_up(tmp_path):
    object_key = "raw/2026/08/27/15/06/qfail.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    local = _write_ndjson(tmp_path / "qfail.json", [{"timestamp": "2026-08-27T15:06:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.ingest._quarantine_convert_corrupt_lines",
            side_effect=Exception("errors/ prefix unwritable"),
        ):
            assert convert_object(SERVICE_ID, object_key, "w1") == "committed"

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
    reader.close()


def test_convert_object_commits_even_when_ingested_files_bookkeeping_fails(tmp_path):
    object_key = "raw/2026/08/27/15/07/bkfail.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    local = _write_ndjson(tmp_path / "bkfail.json", [{"timestamp": "2026-08-27T15:07:00Z", "url": "/a"}])
    with _convert_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": local})) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            assert convert_object(SERVICE_ID, object_key, "w1") == "committed"

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
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
    con, _ = _clear_ledger()

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
    _clear_ledger()
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
    _clear_ledger()
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
    object_key = "raw/2026/08/27/15/08/mixed.json.gz"
    fos = MagicMock()
    con = duckdb.connect()

    _quarantine_convert_corrupt_lines(con, fos, SRC, _read_expr(str(local)), str(local), object_key)
    con.close()

    keys = [c.kwargs["Key"] for c in fos.put_object.call_args_list]
    assert keys == [
        "errors/2026/08/27/15/08/mixed.json.bad.jsonl",
        "errors/2026/08/27/15/08/mixed.json.bad.jsonl.meta.json",
    ]
    meta = json.loads(fos.put_object.call_args_list[1].kwargs["Body"])
    assert meta["valid_rows"] == 1
    assert meta["corrupt_rows"] == 1
    assert meta["total_rows"] == 2
    assert meta["reason_counts"] == {"invalid_json": 1}

    row = list_quarantined_files(SERVICE_ID)[0]
    assert row["file_name"] == "mixed.json.gz"
    assert row["corrupt_rows"] == 1
    assert row["valid_rows"] == 1


def test_quarantine_convert_corrupt_lines_does_nothing_for_a_clean_file(tmp_path):
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "clean.json"
    local.write_text(json.dumps({"timestamp": "2026-08-27T15:09:00Z", "url": "/ok"}) + "\n")
    fos = MagicMock()
    con = duckdb.connect()

    _quarantine_convert_corrupt_lines(
        con, fos, SRC, _read_expr(str(local)), str(local), "raw/2026/08/27/15/09/clean.json.gz"
    )
    con.close()

    fos.put_object.assert_not_called()
    assert list_quarantined_files(SERVICE_ID) == []


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

    _quarantine_convert_corrupt_lines(
        con, fos, SRC, _read_expr(str(local)), str(local), "raw/2026/08/27/15/10/disagree.json.gz"
    )
    con.close()

    fos.put_object.assert_not_called()
    assert list_quarantined_files(SERVICE_ID) == []


def test_quarantine_convert_corrupt_lines_skips_a_key_outside_the_raw_prefix(tmp_path):
    """The errors key is built by slicing the raw prefix off the object key —
    a key that does not start with it must be skipped, not mangled into an
    unrelated destination."""
    _clear_quarantine(SERVICE_ID)
    local = tmp_path / "offprefix.json"
    local.write_text('{"timestamp": "2026-08-27T15:11:00Z", "url": "/trunc"\n')
    fos = MagicMock()
    con = duckdb.connect()

    _quarantine_convert_corrupt_lines(
        con, fos, SRC, _read_expr(str(local)), str(local), "somewhere/else/offprefix.json.gz"
    )
    con.close()

    fos.put_object.assert_not_called()
    assert list_quarantined_files(SERVICE_ID) == []
