"""DuckLake-native weekly maintenance: retention deletion + snapshot expiry.

Steps 1 and 2 of ``_run_cloud_maintenance_impl`` were pyiceberg-based until
this change. Since the commit path moved to DuckLake they ran against a
catalog that receives no commits, so customer data-retention deletion
silently never happened.

These tests use a REAL file-backed DuckLake catalog (``:memory:`` would not
survive the connect/close/reconnect the maintenance job performs) and assert
on observable state — surviving rows, physical parquet, snapshot counts —
rather than on mocked call chains. This is a data-DELETION path: the
load-bearing assertion in every retention test is that rows NEWER than the
cutoff survive.
"""

from __future__ import annotations

import glob
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from backend import config as svcconfig
from backend.core.duckdb import get_connection
from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name

NOW = datetime.now(UTC)


@pytest.fixture(autouse=True)
def _isolated_local_catalog(monkeypatch):
    """Force the per-service file catalog under the conftest sandbox.

    ``_ducklake_attach`` reads ``config.DUCKLAKE_CATALOG``; a developer with
    it set in their shell would otherwise have these tests write into a
    shared (possibly Postgres) catalog. ``METADATA_DSN`` is cleared too so
    the metadata layer stays on the sandbox's SQLite files.
    """
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_DATA_PATH", "")
    monkeypatch.delenv("METADATA_DSN", raising=False)


def _make_source(tmp_path, name: str) -> dict:
    cache = tmp_path / f"cache_{name}"
    (cache / "buffer").mkdir(parents=True)
    return {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }


def _set_knobs(monkeypatch, **cron_sync) -> None:
    monkeypatch.setattr(svcconfig, "load_config", lambda _sid: {"provisioning": {"cron_sync": cron_sync}})


def _lake_con(src: dict):
    con = get_connection(src)
    try:
        con.execute("DETACH lake")
    except Exception:
        pass
    assert _ducklake_attach(con, src, read_only=False)
    return con


def _seed(src: dict, table_name: str, columns: str, rows: list[tuple], *, inline: bool = True):
    """Create ``lake.<per-service table>`` and INSERT ``rows``.

    ``inline=False`` disables DuckLake's small-insert inlining so the rows
    land as real parquet — needed by the byte-reclamation assertions.
    """
    tbl = ducklake_table_name(src, table_name=table_name)
    con = _lake_con(src)
    try:
        if not inline:
            con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        con.execute(f'CREATE TABLE lake."{tbl}" ({columns})')
        placeholders = ", ".join(["?"] * len(rows[0]))
        for row in rows:
            con.execute(f'INSERT INTO lake."{tbl}" VALUES ({placeholders})', list(row))
    finally:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        con.close()
    return tbl


def _read(src: dict, table_name: str = "logs", cols: str = "ip"):
    tbl = ducklake_table_name(src, table_name=table_name)
    con = get_connection(src, read_only=True)
    try:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, src, read_only=True)
        return [r[0] for r in con.execute(f'SELECT {cols} FROM lake."{tbl}" ORDER BY 1').fetchall()]
    finally:
        con.close()


def _catalog_path(src: dict) -> str:
    """The per-service DuckLake catalog file ``_ducklake_attach`` derives."""
    return str(svcconfig.SERVICES_DATA_DIR / f"{src['service_id']}.ducklake")


def _age_catalog(src: dict, days: int, *, schedule_too: bool = False) -> None:
    """Backdate the catalog's snapshot log so ``older_than`` has work to do.

    ``keep_snapshot_days`` floors at 1, and every snapshot in a fresh test
    catalog is seconds old. Attaching the catalog DB directly and rewriting
    ``ducklake_snapshot.snapshot_time`` is the only way to exercise the real
    ``ducklake_expire_snapshots`` cutoff without sleeping for a day.
    """
    import duckdb

    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{_catalog_path(src)}' AS meta")
        con.execute(f"UPDATE meta.ducklake_snapshot SET snapshot_time = snapshot_time - INTERVAL {int(days)} DAY")
        if schedule_too:
            con.execute(
                "UPDATE meta.ducklake_files_scheduled_for_deletion "
                f"SET schedule_start = schedule_start - INTERVAL {int(days)} DAY"
            )
    finally:
        con.close()


def _parquet_files(src: dict) -> list[str]:
    root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "parquet")
    return glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)


def _logs_cols() -> str:
    return "timestamp TIMESTAMPTZ, ip VARCHAR"


def _logs_cols_with_rum() -> str:
    return "timestamp TIMESTAMPTZ, ip VARCHAR, rum_cid VARCHAR"


# ---------------------------------------------------------------------------
# Step 1 — retention deletion
# ---------------------------------------------------------------------------


def test_retention_deletes_old_rows_and_keeps_newer_ones(tmp_path, monkeypatch):
    """The load-bearing assertion for the whole change: the DELETE must be
    bounded by the timestamp predicate. A too-broad delete here destroys
    customer data irreversibly."""
    src = _make_source(tmp_path, f"ret{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols(),
        [
            (NOW - timedelta(days=45), "old-45"),
            (NOW - timedelta(days=31), "old-31"),
            (NOW - timedelta(days=29), "keep-29"),
            (NOW - timedelta(hours=1), "keep-1h"),
        ],
    )
    _set_knobs(monkeypatch, data_retention_days=30, cache_retention_days=0, rollup_retention_months=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert result["data_deleted_before_days"] == 30
    assert result["data_rows_deleted"] == 2
    assert _read(src) == ["keep-1h", "keep-29"], (
        "rows NEWER than the retention cutoff must survive — a delete that took them "
        "too would be irreversible customer-data loss"
    )


def test_data_retention_zero_deletes_nothing(tmp_path, monkeypatch):
    """``0`` means keep forever. Getting this backwards wipes the dataset."""
    src = _make_source(tmp_path, f"zero{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols(),
        [(NOW - timedelta(days=900), "ancient"), (NOW - timedelta(days=1), "fresh")],
    )
    _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deleted_before_days" not in result
    assert "data_deletion_error" not in result, result
    assert _read(src) == ["ancient", "fresh"], "retention_days=0 must delete NOTHING"


def test_data_retention_zero_is_not_capped_by_rum_retention(tmp_path, monkeypatch):
    """``data_retention_days=0`` (forever) + ``rum_retention_days=30``.

    The pyiceberg original took the ``rum_retention_days > data_retention_days``
    branch here, resolving the cutoff to "now" and deleting every row with a
    NULL ``rum_cid`` — plus a ceiling pass at 30 days over everything. It never
    fired because the function was dead against DuckLake data. Now that it
    runs, "0 == forever" has to hold.
    """
    src = _make_source(tmp_path, f"fvr{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols_with_rum(),
        [
            (NOW - timedelta(days=400), "ancient-no-rum", None),
            (NOW - timedelta(days=400), "ancient-rum", "cid-a"),
            (NOW - timedelta(minutes=5), "fresh", None),
        ],
    )
    _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=30, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert _read(src) == ["ancient-no-rum", "ancient-rum", "fresh"], (
        "with log retention disabled the RUM knob must not delete log rows"
    )


def test_rum_retention_longer_than_data_retention_keeps_rum_correlated_rows(tmp_path, monkeypatch):
    """The conditional-prune split: at the data cutoff only rows with no
    ``rum_cid`` go; the rum_cid rows survive until the RUM ceiling."""
    src = _make_source(tmp_path, f"split{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols_with_rum(),
        [
            (NOW - timedelta(days=100), "old-beyond-ceiling", "cid-x"),
            (NOW - timedelta(days=45), "mid-no-rum", None),
            (NOW - timedelta(days=45), "mid-rum", "cid-y"),
            (NOW - timedelta(days=5), "fresh-no-rum", None),
        ],
    )
    _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=90, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert result["data_deleted_before_days"] == 30
    assert result["rum_deleted_before_days"] == 90
    assert _read(src) == ["fresh-no-rum", "mid-rum"], (
        "the 45-day rum_cid row must outlive the 30-day cutoff; the 100-day one must not outlive the 90-day RUM ceiling"
    )


def test_missing_rum_cid_column_falls_back_to_flat_delete(tmp_path, monkeypatch):
    """A service whose schema predates the RUM fields (rum_enabled=False —
    the live default) must still get retention. Silently skipping the step
    is the compliance failure this whole change fixes."""
    src = _make_source(tmp_path, f"norum{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols(),  # no rum_cid column at all
        [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=2), "new")],
    )
    _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=90, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert result["data_rows_deleted"] == 1, "retention must not silently no-op on a pre-RUM schema"
    assert _read(src) == ["new"]


def test_rum_retention_prunes_the_beacon_tables(tmp_path, monkeypatch):
    """RUM telemetry lives in its OWN DuckLake tables keyed by ``cid`` — the
    pyiceberg version only ever touched ``logs``, so ``rum_retention_days``
    never reached the beacon data at all."""
    src = _make_source(tmp_path, f"beacon{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=2), "log-fresh")])
    _seed(
        src,
        "client_vitals",
        "timestamp TIMESTAMPTZ, cid VARCHAR",
        [(NOW - timedelta(days=200), "vital-old"), (NOW - timedelta(days=3), "vital-new")],
    )
    _seed(
        src,
        "client_errors",
        "timestamp TIMESTAMPTZ, cid VARCHAR",
        [(NOW - timedelta(days=200), "err-old"), (NOW - timedelta(days=3), "err-new")],
    )
    _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=90, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert result["rum_beacon_rows_deleted"] == 2
    assert _read(src, "client_vitals", cols="cid") == ["vital-new"]
    assert _read(src, "client_errors", cols="cid") == ["err-new"]


def test_rum_retention_zero_leaves_beacon_tables_alone(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"bzero{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=2), "log-fresh")])
    _seed(
        src,
        "client_vitals",
        "timestamp TIMESTAMPTZ, cid VARCHAR",
        [(NOW - timedelta(days=900), "vital-ancient")],
    )
    _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "rum_beacon_rows_deleted" not in result
    assert _read(src, "client_vitals", cols="cid") == ["vital-ancient"]


def test_absent_beacon_tables_are_not_an_error(tmp_path, monkeypatch):
    """RUM was never provisioned (no client_vitals/client_errors tables) —
    the step must skip them, not record a data_deletion_error."""
    src = _make_source(tmp_path, f"nobeacon{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=1), "new")])
    _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=30, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" not in result, result
    assert "rum_beacon_rows_deleted" not in result
    assert _read(src) == ["new"]


def test_missing_logs_table_records_error_without_raising(tmp_path, monkeypatch):
    """Defensive posture: a failing step records its own ``*_error`` key (which
    the cron wrapper turns into a ``warning`` run) and lets the others run.
    Nothing may propagate out of the function."""
    src = _make_source(tmp_path, f"nolog{uuid.uuid4().hex[:8]}")
    _set_knobs(monkeypatch, data_retention_days=30, cache_retention_days=0)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_deletion_error" in result
    assert "snapshots_before" in result, "expiry must still run after a retention failure"


def test_unsafe_table_name_never_reaches_sql():
    """Trap #4: the interpolated identifier is validated at the point of use."""
    with pytest.raises(ValueError, match="unsafe ducklake table name"):
        buffer_mod._lake_ident('logs"; DROP TABLE x; --')


# ---------------------------------------------------------------------------
# Step 2 — snapshot expiry
# ---------------------------------------------------------------------------


def test_snapshot_expiry_reports_honest_before_after_counts(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"exp{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=1), f"r{i}") for i in range(4)])
    _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0)
    _age_catalog(src, 30)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "snapshot_expiry_error" not in result, result
    before = result["snapshots_before"]
    after = result["snapshots_after"]
    assert before > 0
    assert result["snapshots_expired_before_days"] == 1
    assert after < before, f"aged snapshots must actually be expired ({before} -> {after})"
    assert result["snapshots_expired_count"] == before - after, (
        "the reported count must be the real before/after delta, not an assumption"
    )
    # Expiry is metadata-only; the rows themselves are untouched.
    assert len(_read(src)) == 4


def test_snapshot_expiry_alone_reclaims_no_bytes(tmp_path, monkeypatch):
    """Verified against the real extension: ``ducklake_expire_snapshots``
    reclaims NO bytes on its own, and the retention DELETE leaves the rows
    physically in place behind delete files. The note must say exactly that
    rather than implying storage came back."""
    src = _make_source(tmp_path, f"note{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols(),
        [(NOW - timedelta(days=40 + i), f"old{i}") for i in range(3)] + [(NOW - timedelta(days=1), "fresh")],
        inline=False,
    )
    _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0)
    files_before = _parquet_files(src)
    assert files_before, "premise: the seeded rows must be real parquet, not inlined"
    _age_catalog(src, 30)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "snapshot_expiry_error" not in result, result
    assert result["snapshots_expired_count"] > 0
    note = result["snapshot_expiry_note"]
    assert "not deleted by the expiry itself" in note, note
    assert "ducklake_cleanup_old_files" in note, note
    assert len(_parquet_files(src)) == len(files_before), (
        "expiry must not be reported as reclaiming storage — the parquet is still on disk"
    )
    assert _read(src) == ["fresh"], "retention still deleted the rows logically"


def test_cleanup_pass_unlinks_files_a_later_run_can_reclaim(tmp_path, monkeypatch):
    """The byte-reclamation half of the story. A file is queued for deletion at
    expiry time, so with ``older_than=keep_snapshot_days`` it is always a LATER
    run that unlinks it — which is why the cleanup sweep must not be gated on
    "this run expired something"."""
    src = _make_source(tmp_path, f"reclaim{uuid.uuid4().hex[:8]}")
    _seed(
        src,
        "logs",
        _logs_cols(),
        [(NOW - timedelta(days=40 + i), f"old{i}") for i in range(3)] + [(NOW - timedelta(days=1), "fresh")],
        inline=False,
    )
    _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0)

    # A snapshot that still anchors a live data file cannot be expired, so let
    # the optimize path supersede the seed files first (as the daily job does).
    assert "error" not in buffer_mod._optimize_table_impl(src)
    files_before = _parquet_files(src)
    _age_catalog(src, 30)

    first = buffer_mod._run_cloud_maintenance_impl(src)
    assert first["snapshots_expired_count"] > 0
    assert first["data_files_cleaned"] == 0, "nothing is old enough to unlink on the run that queued it"

    # A week passes: the queued files age past the cutoff.
    _age_catalog(src, 30, schedule_too=True)
    second = buffer_mod._run_cloud_maintenance_impl(src)

    assert "data_file_cleanup_error" not in second, second
    assert second["data_files_cleaned"] >= 1, (
        "the queued files must eventually be unlinked — otherwise retention never reclaims a byte"
    )
    assert len(_parquet_files(src)) < len(files_before)
    assert _read(src) == ["fresh"], "reclamation must not touch surviving rows"


def test_expire_does_not_invalidate_view_caches(tmp_path):
    """Expiry drops OLD snapshot rows; the current snapshot's file membership
    is unchanged, so it must NOT bust the view / snapshot-file caches.

    Exercised through the step helper rather than the whole job because
    ``get_connection`` legitimately repopulates ``_view_cache`` on its way in,
    which would mask the thing being pinned.
    """
    src = _make_source(tmp_path, f"cache{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=1), "r0")])
    _age_catalog(src, 30)

    from backend.core import iceberg as _ice

    con = _lake_con(src)
    _ice._snapshot_files_cache[src["name"]] = ("sentinel",)
    _ice._view_cache[src["name"]] = ("sentinel-too",)
    try:
        out = buffer_mod._ducklake_expire_snapshots(con, src, 1)
        assert out["snapshots_expired_count"] > 0, "premise: the expiry must have done real work"
        assert _ice._snapshot_files_cache.get(src["name"]) == ("sentinel",)
        assert _ice._view_cache.get(src["name"]) == ("sentinel-too",)
    finally:
        _ice._snapshot_files_cache.pop(src["name"], None)
        _ice._view_cache.pop(src["name"], None)
        con.close()


def test_retention_delete_does_invalidate_view_caches(tmp_path):
    """Contrast with the above: a retention delete DOES change file membership,
    so the caches for this service (and its sub-table keys) must be dropped."""
    src = _make_source(tmp_path, f"inval{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=1), "new")])

    from backend.core import iceberg as _ice

    con = _lake_con(src)
    _ice._snapshot_files_cache[src["name"]] = ("stale",)
    _ice._view_cache[f"{src['name']}::client_vitals"] = ("stale-sub",)
    try:
        out = buffer_mod._ducklake_retention_delete(con, src, 30, 0)
        assert out["data_rows_deleted"] == 1
        assert src["name"] not in _ice._snapshot_files_cache
        assert f"{src['name']}::client_vitals" not in _ice._view_cache
    finally:
        _ice._snapshot_files_cache.pop(src["name"], None)
        _ice._view_cache.pop(f"{src['name']}::client_vitals", None)
        con.close()


def test_config_load_failure_returns_error_without_raising(tmp_path, monkeypatch):
    """A total failure returns ``{"error": ...}`` (the cron wrapper's
    ``status='error'`` path) rather than propagating."""
    src = _make_source(tmp_path, f"cfg{uuid.uuid4().hex[:8]}")

    def _boom(_sid):
        raise RuntimeError("config unreadable")

    monkeypatch.setattr(svcconfig, "load_config", _boom)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert result == {"error": "config unreadable"}


# ---------------------------------------------------------------------------
# optimize_table — inlined-data durability
# ---------------------------------------------------------------------------


def test_optimize_table_flushes_inlined_rows_to_parquet(tmp_path, monkeypatch):
    """DuckLake inlines small commits into the metadata catalog, and NEITHER
    ``ducklake_rewrite_data_files`` NOR ``ducklake_merge_adjacent_files``
    promotes them — both only touch already-materialized files. Without an
    explicit ``ducklake_flush_inlined_data`` the table stays at file_count=0
    forever and the only copy of the data lives inside the catalog DB, with
    the raw .gz already deleted.
    """
    src = _make_source(tmp_path, f"flush{uuid.uuid4().hex[:8]}")
    _seed(src, "logs", _logs_cols(), [(NOW - timedelta(hours=i), f"r{i}") for i in range(5)])

    data_root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "parquet")
    assert not glob.glob(os.path.join(data_root, "**", "*.parquet"), recursive=True), (
        "premise: small inserts must have been inlined into the catalog, not written as parquet"
    )

    result = buffer_mod._optimize_table_impl(src)

    assert "error" not in result, result
    assert glob.glob(os.path.join(data_root, "**", "*.parquet"), recursive=True), (
        "inlined rows must be promoted to real parquet — otherwise the catalog DB holds "
        "the only copy of every ingested row"
    )
    assert _read(src) == [f"r{i}" for i in range(5)], "the flush must be lossless"
