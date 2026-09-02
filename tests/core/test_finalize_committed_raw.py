"""``finalize_committed_raw`` and ``merge_lake_files`` — the celery-mode
raw-file reaper and the DuckLake compaction task.

``finalize_committed_raw`` is the only thing that deletes raw ``.gz`` files
in celery mode, so both of its failure directions are expensive:

* deleting too eagerly (before the grace window, or for a row whose rows
  aren't durably committed) destroys the only copy of those log lines;
* deleting too little — or losing the ``raw_deleted_at`` stamp and
  re-deleting — grows FOS storage without bound and re-issues Class-A
  deletes for keys already gone.

So every test here asserts the ledger's ``raw_deleted_at`` state and the
exact keys handed to ``delete_objects``, not just the returned counters.

Real SQLite ledger via ``get_con``; a real file-backed ``ducklake:`` attach
for ``merge_lake_files``. Only the FOS/S3 client is mocked.
"""

import time
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core.ingest import RAW_DELETE_GRACE_S, finalize_committed_raw, merge_lake_files
from backend.core.metadata.base import get_con

SERVICE_ID = "test-finalize-raw"
BUCKET = "test-bucket"
SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}


def _clear_ledger():
    con = get_con(SERVICE_ID)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,))
    con.commit()
    return con, cur


def _seed_committed(con, keys: list[str], *, age_s: float, raw_deleted_at: float | None = None) -> None:
    """Seed committed ledger rows whose commit is ``age_s`` seconds old.

    ``committed_at`` is an epoch float because that is what the PRODUCER
    writes (``committed_at=?`` bound to ``time.time()`` in convert_object).
    """
    now = time.time()
    cur = con.cursor()
    for key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at, raw_deleted_at) "
            "VALUES (?, ?, 'committed', ?, ?, ?)",
            (SERVICE_ID, key, now - age_s - 1, now - age_s, raw_deleted_at),
        )
    con.commit()


def _stamps(con) -> dict[str, float | None]:
    return {
        r["object_key"]: r["raw_deleted_at"]
        for r in con.execute(
            "SELECT object_key, raw_deleted_at FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)
        ).fetchall()
    }


def _cfg(delete_after: bool | None) -> dict:
    if delete_after is None:
        return {"service_id": SERVICE_ID}
    return {"service_id": SERVICE_ID, "provisioning": {"cron_sync": {"delete_after": delete_after}}}


def _env(fos, cfg: dict, src: dict | None = SRC):
    return (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos),
    )


def _run(fos, cfg: dict, src: dict | None = SRC, **kwargs) -> dict:
    load_cfg, get_src, get_fos = _env(fos, cfg, src)
    with load_cfg, get_src, get_fos:
        return finalize_committed_raw(SERVICE_ID, **kwargs)


AGED = RAW_DELETE_GRACE_S + 60


# ── the grace window and the delete_after switch ──────────────────────────


def test_finalize_deletes_aged_committed_raw_files_and_stamps_them():
    con, _ = _clear_ledger()
    keys = ["raw/2026/08/27/12/00/a.json.gz", "raw/2026/08/27/12/01/b.json.gz"]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 2, "eligible": 2, "delete_after": True}
    fos.delete_objects.assert_called_once()
    kw = fos.delete_objects.call_args.kwargs
    assert kw["Bucket"] == BUCKET
    assert sorted(o["Key"] for o in kw["Delete"]["Objects"]) == sorted(keys)
    assert kw["Delete"]["Quiet"] is True
    assert all(v is not None for v in _stamps(con).values())


def test_finalize_respects_the_raw_delete_grace_window():
    """A just-committed file may still be visible to a reader holding the
    pre-commit view. Deleting inside the grace window destroys the only copy
    of data a query is mid-scan on."""
    con, _ = _clear_ledger()
    fresh = "raw/2026/08/27/12/02/fresh.json.gz"
    aged = "raw/2026/08/27/12/03/aged.json.gz"
    _seed_committed(con, [fresh], age_s=RAW_DELETE_GRACE_S - 60)
    _seed_committed(con, [aged], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert [o["Key"] for o in fos.delete_objects.call_args.kwargs["Delete"]["Objects"]] == [aged]
    stamps = _stamps(con)
    assert stamps[fresh] is None
    assert stamps[aged] is not None


def test_finalize_deletes_nothing_when_delete_after_is_disabled():
    """``delete_after: false`` means the operator keeps their raw archive.
    The eligible count must still be reported (so the admin UI can show the
    backlog) but nothing may be deleted or stamped."""
    con, _ = _clear_ledger()
    key = "raw/2026/08/27/12/04/keep.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    result = _run(fos, _cfg(False))

    assert result == {"deleted": 0, "eligible": 1, "delete_after": False}
    fos.delete_objects.assert_not_called()
    assert _stamps(con)[key] is None


def test_finalize_treats_an_absent_delete_after_key_as_enabled():
    """The documented default is True — a cron_sync block that simply omits
    the key must still reap, or FOS grows without bound for every service
    provisioned before the key existed."""
    con, _ = _clear_ledger()
    key = "raw/2026/08/27/12/04/default.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, {"service_id": SERVICE_ID, "provisioning": {"cron_sync": {}}})

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert _stamps(con)[key] is not None


def test_finalize_defaults_to_deleting_when_config_is_absent_entirely():
    """A service with no config at all must still have its raw files reaped
    — otherwise FOS grows without bound for every un-migrated service."""
    con, _ = _clear_ledger()
    key = "raw/2026/08/27/12/05/nocfg.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, None)

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert _stamps(con)[key] is not None


def test_finalize_is_idempotent_and_never_re_deletes_a_stamped_row():
    """``raw_deleted_at`` is the idempotency stamp. A second run must issue
    no further deletes — re-deleting is a wasted Class-A op per key, on
    every scheduler tick, forever."""
    con, _ = _clear_ledger()
    key = "raw/2026/08/27/12/06/once.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    first = _run(fos, _cfg(True))
    stamped_at = _stamps(con)[key]
    second = _run(fos, _cfg(True))

    assert first == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert second == {"deleted": 0, "eligible": 0, "delete_after": True}
    assert fos.delete_objects.call_count == 1
    assert _stamps(con)[key] == stamped_at


def test_finalize_ignores_rows_that_are_not_committed():
    """Only ``committed`` rows are safe to delete the raw file for. A
    ``claimed``/``discovered``/``quarantined`` row's lines are NOT in the
    lake yet — deleting its raw file loses them permanently."""
    con, cur = _clear_ledger()
    now = time.time()
    for status in ("discovered", "claimed", "quarantined", "dead_letter"):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (SERVICE_ID, f"raw/2026/08/27/12/07/{status}.json.gz", status, now - AGED, now - AGED),
        )
    con.commit()

    fos = MagicMock()
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 0, "eligible": 0, "delete_after": True}
    fos.delete_objects.assert_not_called()
    assert all(v is None for v in _stamps(con).values())


# ── partial and total delete failures ─────────────────────────────────────


def test_finalize_leaves_per_key_failures_unstamped_for_the_next_run():
    """A batch delete reports per-key errors. An errored key must NOT be
    stamped — otherwise the object is orphaned in FOS with no ledger row
    left willing to retry it."""
    con, _ = _clear_ledger()
    ok_key = "raw/2026/08/27/12/08/ok.json.gz"
    bad_key = "raw/2026/08/27/12/09/denied.json.gz"
    _seed_committed(con, [ok_key, bad_key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {"Errors": [{"Key": bad_key, "Code": "AccessDenied"}]}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 1, "eligible": 2, "delete_after": True}
    stamps = _stamps(con)
    assert stamps[ok_key] is not None
    assert stamps[bad_key] is None

    # Next run picks the failed key back up on its own.
    fos.delete_objects.return_value = {}
    retry = _run(fos, _cfg(True))
    assert retry == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert [o["Key"] for o in fos.delete_objects.call_args.kwargs["Delete"]["Objects"]] == [bad_key]
    assert _stamps(con)[bad_key] is not None


def test_finalize_stamps_nothing_when_the_batch_delete_call_itself_raises():
    """A transport-level failure means we do not know what was deleted, so
    nothing may be stamped: the whole batch has to stay eligible."""
    con, _ = _clear_ledger()
    keys = ["raw/2026/08/27/12/10/x.json.gz", "raw/2026/08/27/12/11/y.json.gz"]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.side_effect = Exception("connection reset by peer")
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 0, "eligible": 2, "delete_after": True}
    assert all(v is None for v in _stamps(con).values())


def test_finalize_without_a_registered_source_deletes_nothing():
    """No source means no bucket to delete from — report the backlog, touch
    nothing."""
    con, _ = _clear_ledger()
    key = "raw/2026/08/27/12/12/nosrc.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    result = _run(fos, _cfg(True), src=None)

    assert result == {"deleted": 0, "eligible": 1, "delete_after": True}
    fos.delete_objects.assert_not_called()
    assert _stamps(con)[key] is None


def test_finalize_honors_batch_size_so_one_tick_cannot_run_away():
    con, _ = _clear_ledger()
    keys = [f"raw/2026/08/27/12/13/{i}.json.gz" for i in range(5)]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True), batch_size=2)

    assert result == {"deleted": 2, "eligible": 2, "delete_after": True}
    assert len(fos.delete_objects.call_args.kwargs["Delete"]["Objects"]) == 2
    assert sum(1 for v in _stamps(con).values() if v is not None) == 2


# ── merge_lake_files ──────────────────────────────────────────────────────


def _attacher(catalog: str, data_path: str):
    def _attach(con_arg, src_arg, read_only=False):
        con_arg.execute("INSTALL ducklake; LOAD ducklake;")
        ro = ", READ_ONLY" if read_only else ""
        try:
            con_arg.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}'{ro})")
        except duckdb.Error:
            pass
        return True

    return _attach


def test_merge_lake_files_flushes_and_compacts_without_losing_rows(tmp_path):
    """Compaction must be row-preserving: it rewrites data files, and a bug
    here silently drops committed log lines."""
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.logs (ts TIMESTAMPTZ, url VARCHAR)")
    for i in range(3):
        setup.execute(f"INSERT INTO lake.logs VALUES ('2026-08-27T12:0{i}:00Z', '/p{i}')")
    setup.close()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
    ):
        merge_lake_files(SERVICE_ID)

    reader = duckdb.connect()
    attach(reader, None, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 3
    assert sorted(r[0] for r in reader.execute("SELECT url FROM lake.logs").fetchall()) == ["/p0", "/p1", "/p2"]
    reader.close()


def test_merge_lake_files_raises_without_a_registered_source():
    """merge_lake_files raises on failure by design — its caller records the
    outcome. Returning quietly would report a compaction that never ran."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        with pytest.raises(RuntimeError, match="no source registered"):
            merge_lake_files(SERVICE_ID)


def test_merge_lake_files_raises_when_the_lake_attach_fails():
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=False),
        patch("backend.core.ingest._configure_fos"),
    ):
        with pytest.raises(RuntimeError, match="attach failed"):
            merge_lake_files(SERVICE_ID)
