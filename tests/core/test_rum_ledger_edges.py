"""RUM ledger failure/edge paths — ``convert_rum_object``,
``_quarantine_rum_corrupt_lines``, ``discover_rum_prefix`` and
``sweep_rum_ledger_once``.

Companion to tests/core/test_rum_ledger.py (which owns the happy paths and
the dual-table atomicity contract). Everything here is a failure mode whose
wrong behaviour is silent RUM data loss:

* a claim/source/attach/download failure that marks the row ``committed``
  anyway would strand the beacons forever — the raw file is deleted once the
  ledger says committed (``finalize_committed_raw``);
* a quarantine-upload failure that propagates would fail a convert whose
  valid rows are already durably committed, re-running it on every retry;
* a schema-drifted lake table that half-inserts inside its transaction would
  leave the lake with a partial file's rows AND a retryable ledger row,
  double-counting on retry.

Uses a REAL file-backed ``ducklake:`` attach (per tests/core/test_convert_batch.py)
and the real SQLite ledger via ``get_con``; only the FOS/S3 client and
celery's ``.delay()`` are mocked.
"""

import gzip
import json
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core.ingest import (
    LEDGER_RECLAIM_AFTER_S,
    _quarantine_rum_corrupt_lines,
    convert_rum_object,
    discover_rum_prefix,
    sweep_rum_ledger_once,
)
from backend.core.metadata.base import get_con

SERVICE_ID = "test-rum-ledger-edges"
BUCKET = "test-bucket"


def _clear_ledger(service_id: str = SERVICE_ID):
    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
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


def _write_gz(path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _attacher(catalog: str, data_path: str):
    """Real DuckLake attach against a file-backed catalog — a ``:memory:``
    catalog would not survive convert_rum_object closing its connection."""

    def _attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if read_only else ""
        try:
            con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
        except duckdb.Error:
            pass  # already attached on this connection
        return True

    return _attach


@contextmanager
def _rum_env(tmp_path, download, fos=None, attach=None):
    """Patch scaffold for convert_rum_object with a real DuckLake attach.

    Yields the attacher so a test can open its own read-only reader on the
    same catalog after the convert has closed its connection.
    """
    src = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    real_attach = _attacher(catalog, data_path)
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos if fos is not None else MagicMock()),
        patch("backend.core.ingest._download_chunk_to_local", side_effect=download),
        patch(
            "backend.core.iceberg._ducklake._ducklake_attach",
            side_effect=attach if attach is not None else real_attach,
        ),
        patch("backend.core.ingest._configure_fos"),
        patch(
            "backend.core.iceberg._ducklake.ducklake_table_name",
            side_effect=lambda src, table_name="logs": table_name,
        ),
    ):
        yield real_attach


def _download_stub(mapping: dict[str, str]):
    def _download(fos_client, s3_paths, tmpdir):
        got = {p: mapping[p] for p in s3_paths if p in mapping}
        return got, {v: k for k, v in got.items()}

    return _download


def _lake_reader(attach):
    con = duckdb.connect()
    attach(con, None, read_only=True)
    return con


def _table_exists(con, name: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND table_name=? LIMIT 1", (name,)
        ).fetchone()
    )


VITALS_LINE = json.dumps({"timestamp": "2026-08-27T11:00:00Z", "rum_metric_name": "LCP", "rum_metric_value": "1500"})
ERROR_LINE = json.dumps({"timestamp": "2026-08-27T11:00:01Z", "rum_error_message": "boom", "rum_error_line": "9"})


# ── claim / precondition failures ─────────────────────────────────────────


def test_convert_rum_object_returns_not_claimed_when_no_discovered_row_exists(tmp_path):
    """A redelivered message for an already-committed (or absent) key must
    no-op. Re-converting a committed key would re-insert rows for a raw file
    that finalize_committed_raw may already have deleted."""
    object_key = "rum/raw/2026/08/27/11/00/gone.json.gz"
    con, _ = _clear_ledger()
    con.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) "
        "VALUES (?, ?, 'committed', ?, ?)",
        (SERVICE_ID, object_key, time.time(), time.time()),
    )
    con.commit()

    raw = tmp_path / "gone.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach:
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "not_claimed"

    # Untouched: still committed, no attempt burned, and nothing written.
    row = _ledger_row(con, object_key)
    assert row["status"] == "committed"
    assert row["attempts"] == 0
    reader = _lake_reader(attach)
    assert not _table_exists(reader, "client_vitals")
    reader.close()


def test_convert_rum_object_with_no_registered_source_stays_retryable(tmp_path):
    """A service whose source has not been registered yet (worker started
    before config load) must leave the row retryable — not committed and not
    dead-lettered."""
    object_key = "rum/raw/2026/08/27/11/01/nosrc.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "error"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "no source registered" in row["last_error"]
    assert row["committed_at"] is None


def test_convert_rum_object_with_failed_lake_attach_stays_retryable(tmp_path):
    """A DuckLake attach failure (catalog unreachable) is transient — the
    ledger row must be requeued with the reason recorded."""
    object_key = "rum/raw/2026/08/27/11/02/noattach.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    raw = tmp_path / "noattach.json"
    _write_gz(raw, [VITALS_LINE])
    with _rum_env(
        tmp_path,
        _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}),
        attach=lambda *a, **kw: False,
    ):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "attach failed" in row["last_error"]


# ── download failures: dead key vs transient ──────────────────────────────


def test_convert_rum_object_dead_letters_a_key_whose_object_is_gone(tmp_path):
    """A ledger row for an object no longer in FOS (already ingested and
    deleted, or expired) is terminal. Retrying it would burn
    LEDGER_MAX_ATTEMPTS GETs per dead key on every sweep."""
    object_key = "rum/raw/2026/08/27/11/03/vanished.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.side_effect = Exception("An error occurred (404) when calling the HeadObject operation: Not Found")
    with _rum_env(tmp_path, _download_stub({}), fos=fos):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "dead_letter"

    row = _ledger_row(con, object_key)
    assert row["status"] == "dead_letter"
    # dead_letter is a decision, not a failure: no attempt is burned.
    assert row["attempts"] == 0
    assert "missing from FOS" in row["last_error"]
    fos.head_object.assert_called_once_with(Bucket=BUCKET, Key=object_key)


def test_convert_rum_object_treats_download_failure_on_a_live_object_as_transient(tmp_path):
    """The object still HEADs fine, so the download blip is transient: the
    row must stay retryable rather than be dead-lettered (which would
    permanently drop a file that is still sitting in the bucket)."""
    object_key = "rum/raw/2026/08/27/11/04/blip.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    fos = MagicMock()
    fos.head_object.return_value = {"ContentLength": 123}
    with _rum_env(tmp_path, _download_stub({}), fos=fos):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["attempts"] == 1
    assert "transient" in row["last_error"]


# ── quarantine is best-effort ─────────────────────────────────────────────


def test_convert_rum_object_commits_valid_rows_even_when_quarantine_upload_fails(tmp_path):
    """The valid rows are already parsed and about to be durably committed
    when quarantine runs. A FOS failure while uploading the sidecar must not
    fail the convert — otherwise one unwritable errors/ prefix blocks ingest
    for every file that contains a single malformed beacon."""
    object_key = "rum/raw/2026/08/27/11/05/quarantine-fails.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    raw = tmp_path / "qfail.json"
    _write_gz(raw, [VITALS_LINE, '{"timestamp": "2026-08-27T11:05:00Z", "rum_metric_name": "trunc'])

    fos = MagicMock()
    fos.put_object.side_effect = Exception("AccessDenied writing errors/ prefix")
    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}), fos=fos) as attach:
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "committed"

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    reader.close()
    fos.put_object.assert_called_once()  # first sidecar write blew up; no meta.json followed


def test_convert_rum_object_commits_when_ingested_files_bookkeeping_fails(tmp_path):
    """``ingested_files`` is a reader convenience; the ledger is
    authoritative. A bookkeeping failure must not roll back a committed
    convert."""
    object_key = "rum/raw/2026/08/27/11/06/bookkeeping.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    raw = tmp_path / "bk.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE])

    with _rum_env(tmp_path, _download_stub({f"s3://{BUCKET}/{object_key}": str(raw)})) as attach:
        with patch(
            "backend.core.ingest.metadata_db.insert_ingested_files",
            side_effect=Exception("metadata db locked"),
        ):
            assert convert_rum_object(SERVICE_ID, object_key, "w1") == "committed"

    assert _ledger_row(con, object_key)["status"] == "committed"
    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 1
    assert reader.execute("SELECT count(*) FROM lake.client_errors").fetchone()[0] == 1
    reader.close()


# ── writing onto a pre-existing lake table ────────────────────────────────


def test_convert_rum_object_backfills_source_file_column_on_a_v2_created_table(tmp_path):
    """v2's RUM writer (``ingest_rum_logs``) creates ``client_vitals``
    WITHOUT ``_source_file``. This path shares the table, and its
    DELETE-by-``_source_file`` idempotency depends on that column existing —
    so it must be added rather than the insert failing."""
    object_key = "rum/raw/2026/08/27/11/07/widen.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    # Pre-create the table the way v2 would: full RUM schema, no _source_file.
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute(
        "CREATE TABLE lake.client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, metric_rating VARCHAR, "
        "pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, req_id VARCHAR, "
        "city VARCHAR, region VARCHAR, country VARCHAR, pop VARCHAR, tls VARCHAR, ttfb DOUBLE)"
    )
    setup.execute(
        "INSERT INTO lake.client_vitals (timestamp, metric_name, metric_value) "
        "VALUES ('2026-08-01T00:00:00Z', 'v2-legacy-row', 1.0)"
    )
    setup.close()

    raw = tmp_path / "widen.json"
    _write_gz(raw, [VITALS_LINE])
    src = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=MagicMock()),
        patch(
            "backend.core.ingest._download_chunk_to_local",
            side_effect=_download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}),
        ),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch(
            "backend.core.iceberg._ducklake.ducklake_table_name",
            side_effect=lambda src, table_name="logs": table_name,
        ),
    ):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "committed"

    reader = _lake_reader(attach)
    rows = reader.execute("SELECT metric_name, _source_file FROM lake.client_vitals ORDER BY metric_name").fetchall()
    reader.close()
    # The legacy row survives with a NULL _source_file; the new row is
    # attributed and therefore idempotently re-deletable on redelivery.
    assert rows == [
        ("LCP", f"s3://{BUCKET}/{object_key}"),
        ("v2-legacy-row", None),
    ]


def test_convert_rum_object_rolls_back_and_stays_retryable_on_a_schema_drifted_table(tmp_path):
    """If the shared lake table has drifted so the INSERT cannot land, the
    transaction must roll back and the ledger row must stay retryable —
    never a half-written table plus a committed row."""
    object_key = "rum/raw/2026/08/27/11/08/drift.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    # A narrower table than the arrow schema: INSERT ... BY NAME cannot map
    # metric_value/pathname/... and raises inside the transaction.
    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, _source_file VARCHAR)")
    setup.close()

    raw = tmp_path / "drift.json"
    _write_gz(raw, [VITALS_LINE, ERROR_LINE])
    src = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=MagicMock()),
        patch(
            "backend.core.ingest._download_chunk_to_local",
            side_effect=_download_stub({f"s3://{BUCKET}/{object_key}": str(raw)}),
        ),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch(
            "backend.core.iceberg._ducklake.ducklake_table_name",
            side_effect=lambda src, table_name="logs": table_name,
        ),
    ):
        assert convert_rum_object(SERVICE_ID, object_key, "w1") == "discovered"

    row = _ledger_row(con, object_key)
    assert row["status"] == "discovered"
    assert row["committed_at"] is None
    assert row["attempts"] == 1

    reader = _lake_reader(attach)
    assert reader.execute("SELECT count(*) FROM lake.client_vitals").fetchone()[0] == 0
    # The errors table is written second — it must not exist at all, since
    # the vitals write aborted the convert first.
    assert not _table_exists(reader, "client_errors")
    reader.close()


# ── _quarantine_rum_corrupt_lines directly ────────────────────────────────


def test_quarantine_rum_corrupt_lines_writes_sidecar_pair_under_source_prefix():
    """Sidecar protocol: the bad lines as .bad.jsonl plus a .meta.json whose
    counts and reason histogram are what the admin quarantine UI reads."""
    fos = MagicMock()
    src = {"service_id": SERVICE_ID, "name": "prod-source", "bucket": BUCKET, "prefix": "/tenant-a/"}
    object_key = "tenant-a/rum/raw/2026/08/27/11/09/mixed.json.gz"
    corrupt = [("{bad one", "invalid_json"), ("{bad two", "invalid_json"), ('{"a":1}', "parse_error: nope")]

    with patch("backend.core.ingest.metadata_db.insert_quarantined_file") as mock_insert:
        _quarantine_rum_corrupt_lines(fos, src, object_key, corrupt, valid_count=7)

    keys = [c.kwargs["Key"] for c in fos.put_object.call_args_list]
    assert keys == [
        "tenant-a/errors/2026/08/27/11/09/mixed.json.bad.jsonl",
        "tenant-a/errors/2026/08/27/11/09/mixed.json.bad.jsonl.meta.json",
    ]
    assert fos.put_object.call_args_list[0].kwargs["Body"] == b'{bad one\n{bad two\n{"a":1}'

    meta = json.loads(fos.put_object.call_args_list[1].kwargs["Body"])
    assert meta["original_key"] == object_key
    assert meta["valid_rows"] == 7
    assert meta["corrupt_rows"] == 3
    assert meta["total_rows"] == 10
    assert meta["reason_counts"] == {"invalid_json": 2, "parse_error: nope": 1}
    assert meta["source_name"] == "prod-source"

    kw = mock_insert.call_args.kwargs
    assert kw["file_name"] == "mixed.json.gz"
    assert kw["fos_key"] == object_key
    assert kw["error_key"] == keys[0]
    assert kw["meta_key"] == keys[1]
    assert kw["valid_rows"] == 7
    assert kw["corrupt_rows"] == 3
    assert kw["reason_counts"] == {"invalid_json": 2, "parse_error: nope": 1}


def test_quarantine_rum_corrupt_lines_ignores_a_key_outside_the_rum_raw_prefix():
    """Guard against writing an errors/ key derived from an unrelated
    prefix — the quarantine key is built by string surgery on the raw
    prefix, so a non-matching key must be skipped, not mangled."""
    fos = MagicMock()
    src = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}

    with patch("backend.core.ingest.metadata_db.insert_quarantined_file") as mock_insert:
        _quarantine_rum_corrupt_lines(fos, src, "raw/2026/08/27/regular-log.json.gz", [("x", "invalid_json")], 1)

    fos.put_object.assert_not_called()
    mock_insert.assert_not_called()


# ── discovery guard rails ─────────────────────────────────────────────────


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def _null_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return None


def test_discover_rum_prefix_is_a_noop_without_config_or_source():
    """A worker that races ahead of config/source registration must insert
    nothing rather than dispatch converts for keys it cannot resolve."""
    con, _ = _clear_ledger()

    with patch("backend.config.load_config", return_value=None):
        with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
            assert discover_rum_prefix(SERVICE_ID) == 0
            mock_list.assert_not_called()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos) as mock_list:
                assert discover_rum_prefix(SERVICE_ID) == 0
                mock_list.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 0


def test_discover_rum_prefix_returns_zero_when_the_list_yields_no_result():
    """A LIST that produces no result payload at all (aborted generator) is
    not an empty bucket — it must not be reported as a successful scan."""
    _clear_ledger()
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_null_list_fos):
                with patch("backend.core.ingest.convert_rum.delay") as mock_delay:
                    assert discover_rum_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()


def test_discover_rum_prefix_does_not_redispatch_an_already_known_key():
    """ON CONFLICT DO NOTHING means a re-LIST of the same minute must
    dispatch nothing — otherwise every sweep re-enqueues the whole
    lookback window."""
    object_key = "rum/raw/2026/08/27/11/10/known.json.gz"
    con, _ = _clear_ledger()
    _seed_discovered(con, object_key)

    def _list_one(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        path = f"s3://{BUCKET}/{object_key}"
        return {"new_files": [path], "file_sizes": {path: 10}, "skipped_already": 0, "stranded_already": []}

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_list_one):
                with patch("backend.core.ingest.convert_rum.delay") as mock_delay:
                    assert discover_rum_prefix(SERVICE_ID) == 0
                    mock_delay.assert_not_called()

    assert con.execute("SELECT count(*) FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)).fetchone()[0] == 1


# ── sweeper: broker unreachable ───────────────────────────────────────────


def test_sweep_rum_ledger_redispatches_stuck_rows_when_the_broker_check_raises():
    """The lost-message guard skips re-dispatch when the queue already holds
    the pending work. If the depth check itself blows up we cannot know
    that, so the guard must fail OPEN (depth 0) and re-dispatch — a stuck
    row that is never re-dispatched is a permanently un-ingested file.
    Celery de-duplication is not relied on; a duplicate convert is
    idempotent, a lost one is not."""
    con, cur = _clear_ledger()
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    reclaim_key = "rum/raw/2026/08/27/11/11/reclaim.json.gz"
    stuck_key = "rum/raw/2026/08/27/11/11/stuck.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) "
        "VALUES (?, ?, 'claimed', 'dead-worker', ?, ?)",
        (SERVICE_ID, reclaim_key, stale, stale - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (SERVICE_ID, stuck_key, stale - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch(
                        "backend.celery_status.celery_queue_depths",
                        side_effect=Exception("broker unreachable"),
                    ),
                    patch("backend.core.ingest.convert_rum.delay") as mock_rum_delay,
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_batch_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary == {"reclaimed": 1, "redispatched": 2, "discovered": 0}
    assert sorted(c.args[1] for c in mock_rum_delay.call_args_list) == sorted([reclaim_key, stuck_key])
    mock_batch_delay.assert_not_called()
    assert _ledger_row(con, reclaim_key)["status"] == "discovered"


def test_sweep_rum_ledger_scopes_to_the_source_prefix():
    """With a source prefix configured, the RUM keyspace is
    ``<prefix>/rum/raw/`` — an unprefixed key belongs to a different source
    layout and must not be swept by this service's RUM sweep."""
    con, cur = _clear_ledger()
    stale = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    prefixed = "tenant-a/rum/raw/2026/08/27/11/12/mine.json.gz"
    unprefixed = "rum/raw/2026/08/27/11/12/not-mine.json.gz"
    for key in (prefixed, unprefixed):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) "
            "VALUES (?, ?, 'claimed', 'dead-worker', ?, ?)",
            (SERVICE_ID, key, stale, stale - 30),
        )
    con.commit()

    src = {"name": "t", "bucket": BUCKET, "prefix": "/tenant-a/"}
    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value=src):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_rum.delay") as mock_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary["reclaimed"] == 1
    mock_rum_delay.assert_called_once_with(SERVICE_ID, prefixed)
    assert _ledger_row(con, unprefixed)["status"] == "claimed"


@pytest.mark.parametrize("status", ["committed", "quarantined", "dead_letter"])
def test_sweep_rum_ledger_leaves_terminal_rows_alone(status):
    """Only ``claimed``/``discovered`` rows are sweepable. Re-dispatching a
    committed row would re-convert a raw file that may already be deleted;
    re-dispatching a quarantined one would defeat the attempt cap."""
    con, cur = _clear_ledger()
    key = f"rum/raw/2026/08/27/11/13/{status}.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_at, discovered_at) VALUES (?, ?, ?, ?, ?)",
        (SERVICE_ID, key, status, time.time() - 999999, time.time() - 999999),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": SERVICE_ID}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "t", "bucket": BUCKET}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({}, False)),
                    patch("backend.core.ingest.convert_rum.delay") as mock_rum_delay,
                ):
                    summary = sweep_rum_ledger_once(SERVICE_ID)

    assert summary == {"reclaimed": 0, "redispatched": 0, "discovered": 0}
    mock_rum_delay.assert_not_called()
    assert _ledger_row(con, key)["status"] == status
