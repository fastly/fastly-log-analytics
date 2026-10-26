"""Targeted coverage for backend.core._duckdb_status helpers."""

from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from backend.core._duckdb_status import (
    _clear_schema_cache,
    _schema_cache,
    backfill_fastly_edge_writes,
    enrich_asn_labels,
    format_asn_label,
    get_asn_names,
    get_schema,
    get_sync_status,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    refresh_config_status,
    update_cron_duration,
    update_top_values,
)
from backend.utils.bounded_cache import BoundedTTLCache

from backend.core import _duckdb_status
from backend.core import duckdb as _db
from backend.core import metadata as metadata_db
from backend.core.rollups.rum import rum_count_and_latest
from backend.utils.date_utils import safe_iso


class _StubRow:
    def __init__(self, one=None, all_rows=None):
        self._one = one
        self._all = [] if all_rows is None else all_rows

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _StubCon:
    def execute(self, *_a, **_k):
        return _StubRow()

    def close(self):
        pass


@contextmanager
def _refresh_ctx(captured: dict, cfg: dict | None = None, src: dict | None = None, **overrides):
    default_cfg = {"name": "svc", "bucket": "b", "service_id": "svc"} if cfg is None else cfg
    default_src = {"name": "svc", "bucket": "b"} if src is None else src
    targets = {
        "backend.config.load_config": {"return_value": default_cfg},
        "backend.config.config_to_source": {"return_value": default_src},
        "backend.config.update_status": {"side_effect": lambda sid, st: captured.setdefault("status", st)},
        "backend.core.duckdb.get_connection": {"return_value": _StubCon()},
        "backend.core.duckdb.get_sync_status": {"return_value": {"ingested": 0, "local_rows": 0}},
        "backend.core.duckdb.get_schema": {"return_value": []},
        "backend.core.duckdb.update_top_values": {},
    }
    targets.update(overrides)
    with ExitStack() as stack:
        mocks = {k: stack.enter_context(patch(k, **v)) for k, v in targets.items()}
        yield mocks


# ── update_top_values ────────────────────────────────────────────────


def test_update_top_values_happy_retry_and_no_overlap(in_memory_duckdb, fos_source, tmp_path):
    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}
    (cache_dir / "data" / "ts=1").mkdir(parents=True)
    (cache_dir / "data" / "ts=1" / "f.parquet").write_bytes(b"x")

    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, country VARCHAR, status VARCHAR, method VARCHAR)")
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES "
        "('1.1.1.1', 'US', '200', 'GET'), ('1.1.1.1', 'US', '200', 'GET'), ('2.2.2.2', 'GB', '404', 'POST')"
    )
    schema = [{"name": c, "type": "VARCHAR"} for c in ("ip", "country", "status", "method")]

    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    with patch("backend.core._duckdb_status.get_schema", return_value=schema):
        update_top_values(in_memory_duckdb, src)

    out_path = cache_dir / "top_values.json"
    assert out_path.exists()
    payload = json.loads(out_path.read_text())
    assert set(payload.keys()) == {"ip", "country", "status", "method"}
    assert {item["value"]: item["count"] for item in payload["ip"]} == {"1.1.1.1": 2, "2.2.2.2": 1}
    with _db._top_values_cache_lock:
        assert _db._top_values_cache.get(src["name"]) is not None

    # Catalog error on first CREATE TEMP TABLE triggers iceberg refresh + retry
    out_path.unlink()
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    state = {"creates": 0}

    class _Proxy:
        def execute(self, sql, *a, **kw):
            if sql.startswith("CREATE TEMP TABLE"):
                state["creates"] += 1
                if state["creates"] == 1:
                    raise Exception("Catalog Error: Table with name foo does not exist")
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core._duckdb_status.get_schema", return_value=schema[:2]),
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        update_top_values(_Proxy(), src)
    mock_update.assert_called_once()
    assert state["creates"] == 2
    assert out_path.exists()

    # No whitelist overlap short-circuits before CREATE TEMP TABLE
    out_path.unlink()
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)
    state["creates"] = 0
    no_overlap_schema = [{"name": "timestamp", "type": "TIMESTAMP"}, {"name": "custom_blob", "type": "VARCHAR"}]
    with patch("backend.core._duckdb_status.get_schema", return_value=no_overlap_schema):
        update_top_values(_Proxy(), src)
    assert state["creates"] == 0
    assert not out_path.exists()


# ── refresh_config_status ────────────────────────────────────────────


def test_refresh_config_status_swallow_and_error_branches():
    # Missing config -> silent return
    with patch("backend.config.load_config", return_value=None), patch("backend.config.update_status") as mock_upd:
        refresh_config_status("missing-svc")
    mock_upd.assert_not_called()

    # High-scale service -> skips legacy DuckDB reads
    with (
        patch("backend.config.load_config", return_value={"name": "svc", "bucket": "b", "service_id": "svc"}),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
            return_value=type("Registry", (), {"resolve": lambda self, service_id: object()})(),
        ),
        patch("backend.core.duckdb.get_connection") as get_con,
        patch("backend.config.update_status") as upd_st,
    ):
        refresh_config_status("svc")
    get_con.assert_not_called()
    upd_st.assert_not_called()

    # Buffer walk exception swallowed
    captured: dict = {}

    def _boom(*_a, **_k):
        raise PermissionError("EACCES")

    with _refresh_ctx(
        captured,
        **{
            "os.path.isdir": {"return_value": True},
            "os.walk": {"side_effect": _boom},
            "backend.utils.dir_size._get_dir_size": {"side_effect": _boom},
        },
    ):
        refresh_config_status("svc")
    assert "status" in captured and "buffer_size_bytes" not in captured["status"]

    # Iceberg error envelope skips iceberg_bytes/iceberg_files
    captured.clear()
    with _refresh_ctx(
        captured, **{"backend.core.iceberg.get_table_info": {"return_value": {"error": "catalog unreachable"}}}
    ):
        refresh_config_status("svc")
    assert "iceberg_bytes" not in captured["status"] and "iceberg_files" not in captured["status"]


def test_refresh_config_status_happy_path_and_rum_persisted(tmp_path):
    captured: dict = {}
    buf_dir = tmp_path / "buf"
    buf_dir.mkdir()
    (buf_dir / "f.parquet").write_bytes(b"x" * 17)

    with _refresh_ctx(
        captured,
        **{
            "backend.core._duckdb_status.get_schema": {"return_value": [{"name": "ip", "type": "VARCHAR"}]},
            "backend.core._duckdb_status.update_top_values": {},
            "backend.core._duckdb_status._cache_dir": {"return_value": str(buf_dir)},
            "backend.core.iceberg.get_table_info": {"return_value": {"size_bytes": 12345, "data_files": 7}},
            "backend.repositories.usage.get_edge_ratio": {"return_value": (0.42, None)},
        },
    ):
        refresh_config_status("svc")

    st = captured["status"]
    assert st["buffer_size_bytes"] == 17
    assert (st["iceberg_bytes"], st["iceberg_files"], st["edge_ratio"]) == (12345, 7, 0.42)
    assert st["schema"] == [{"name": "ip", "type": "VARCHAR"}]

    # Persisted RUM freshness (real sub-hour timestamp) surfaced without live lake scan
    captured.clear()
    live_rum = MagicMock(return_value=(0, None))
    rum_cfg = {
        "name": "svc",
        "bucket": "b",
        "service_id": "svc",
        "rum_enabled": True,
        "status": {"rum": {"latest_log_at": "2026-10-02T00:45:30", "total_rows": 155, "last_sync_at": "S"}},
    }
    with _refresh_ctx(
        captured,
        cfg=rum_cfg,
        src=rum_cfg,
        **{
            "backend.core._duckdb_status.get_schema": {"return_value": [{"name": "ip", "type": "VARCHAR"}]},
            "backend.core._duckdb_status.update_top_values": {},
            "backend.core._duckdb_status._cache_dir": {"return_value": "/tmp/nonexistent-rum-fresh"},
            "backend.core.iceberg.get_table_info": {"return_value": {"error": "skip"}},
            "backend.core.rollups.rum.rum_count_and_latest": {"new": live_rum},
        },
    ):
        refresh_config_status("svc")

    live_rum.assert_not_called()
    assert captured["status"]["rum"]["total_rows"] == 155
    assert captured["status"]["rum"]["latest_log_at"].startswith("2026-10-02T00:45:30")


@pytest.mark.parametrize("discovery_time", ["2026-09-08T18:00:00Z", None])
def test_refresh_config_status_skip_top_values_omits_schema_key(discovery_time):
    captured: dict = {}
    schema_calls = {"count": 0}

    def _track_schema(*_a, **_k):
        schema_calls["count"] += 1
        return []

    with _refresh_ctx(
        captured,
        **{
            "backend.core._duckdb_status.get_schema": {"side_effect": _track_schema},
            "backend.core._duckdb_status.update_top_values": {},
            "backend.core.metadata.cron_log.latest_cron_per_task": {
                "return_value": {
                    "sync": {"started_at": "2026-09-01T18:00:00Z"},
                    "log_discovery": {"started_at": discovery_time},
                }
            },
        },
    ) as mocks:
        refresh_config_status("svc", include_top_values=False)

    assert "schema" not in captured["status"]
    mocks["backend.core._duckdb_status.update_top_values"].assert_not_called()
    assert schema_calls["count"] == 0
    assert captured["status"]["request"]["last_sync_at"] == (discovery_time or "2026-09-01T18:00:00Z")


# ── ASN label helpers ────────────────────────────────────────────────


def test_asn_helpers_enrich_lookup_and_format():
    values = [{"value": "7922"}, {"value": "GoogleBot"}, {"value": "15169"}, {"value": "country-code"}]
    with patch("backend.core.duckdb.get_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}):
        out = enrich_asn_labels(values, "svc-1")
    assert out is values
    assert values[0]["label"] == "Comcast Cable (7922)"
    assert "label" not in values[1]
    assert values[2]["label"] == "Google (15169)"
    assert "label" not in values[3]

    assert get_asn_names("svc", []) == {}
    assert get_asn_names("", [7922]) == {}
    with (
        patch("backend.core.metadata.lookup_asn_names", return_value={7922: "Comcast Cable", 15169: "Google"}),
        patch("backend.core.metadata.upsert_asn_names") as mock_upsert,
    ):
        assert get_asn_names("svc", [7922, 15169]) == {7922: "Comcast Cable", 15169: "Google"}
    mock_upsert.assert_not_called()

    assert format_asn_label(7922, "Comcast Cable") == "Comcast Cable (7922)"
    assert format_asn_label(7922, "") == "AS7922"
    assert format_asn_label(7922, "AS7922") == "AS7922"


# ── get_sync_status branches ─────────────────────────────────────────


def test_get_sync_status_unconfigured_and_cached_modes(in_memory_duckdb, fos_source):
    assert get_sync_status(in_memory_duckdb, {"name": "no-svc"}) == {
        "configured": False,
        "local_rows": 0,
        "ingested": 0,
        "fos_total": 0,
        "storage_mode": "cloud",
        "access_level": "read_write",
    }

    cached = {"local_rows": 12345, "ingested": 99, "latest_log_at": "2026-06-15T00:00:00Z"}
    with patch("backend.config.get_status", side_effect=lambda *_a, **_kw: dict(cached)):
        out = get_sync_status(in_memory_duckdb, fos_source, skip_fos=True, force=False)
        out_local = get_sync_status(
            in_memory_duckdb, {**fos_source, "storage_mode": "local"}, skip_fos=True, force=False
        )
    assert (out["local_rows"], out["ingested"], out["access_level"], out["storage_mode"], out["configured"]) == (
        12345,
        99,
        "read_write",
        "cloud",
        True,
    )
    assert out_local["storage_mode"] == "local"

    empty_sum = {
        "file_count": 0,
        "total_rows": 0,
        "total_bytes": 0,
        "count_with_bytes": 0,
        "last_ingested": None,
        "latest_file_name": None,
    }
    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={"local_rows": 0, "latest_log_at": None}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=empty_sum),
    ):
        assert (
            get_sync_status(in_memory_duckdb, {**fos_source, "storage_mode": "local"}, force=True)["storage_mode"]
            == "local"
        )


def test_get_sync_status_stale_catalog_rebuild_and_unknown_error_fallback(in_memory_duckdb, fos_source):
    table_name = _db._safe_table_name(fos_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP)")
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES ('2026-06-15 00:00:00')")
    in_memory_duckdb.execute(
        "CREATE TABLE _cron_run_log (task VARCHAR, started_at VARCHAR, "
        "duration_s DOUBLE, status VARCHAR, error_message VARCHAR, summary VARCHAR)"
    )

    state = {"stats_calls": 0}

    class _RebuildProxy:
        def execute(self, sql, *a, **kw):
            if sql.startswith("SELECT count(*), min(timestamp), max(timestamp) FROM logs_test_service"):
                state["stats_calls"] += 1
                if state["stats_calls"] == 1:
                    raise RuntimeError("Catalog Error: Table with name foo does not exist")
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out = get_sync_status(_RebuildProxy(), fos_source, force=True)

    mock_clear.assert_called_once()
    assert mock_clear.call_args.kwargs.get("keep_snapshot_cache") is True
    mock_update.assert_called_once()
    assert state["stats_calls"] == 2 and out["local_rows"] == 1

    # Unknown exception -> no rebuild, falls back to metadata count
    summary = {
        "file_count": 7,
        "total_rows": 42,
        "total_bytes": 1024,
        "count_with_bytes": 1,
        "last_ingested": "2026-06-15T00:00:00",
        "latest_file_name": "raw/2026-06-15T00:00:00.gz",
    }

    class _ErrProxy:
        def execute(self, sql, *_a, **_kw):
            if "count(*)" in sql and "timestamp" in sql:
                raise RuntimeError("connection reset by peer")
            return _StubRow(one=(0,))

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear2,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update2,
    ):
        out2 = get_sync_status(_ErrProxy(), fos_source, force=True)

    mock_clear2.assert_not_called()
    mock_update2.assert_not_called()
    assert (out2["local_rows"], out2["ingested"]) == (42, 7)


def test_get_sync_status_empty_and_stale_view_lake_reconciliation(in_memory_duckdb, fos_source):
    summary = {
        "file_count": 3,
        "total_rows": 400,
        "total_bytes": 300,
        "count_with_bytes": 3,
        "last_ingested": "2026-06-20T00:00:00",
        "latest_file_name": "raw/2026-06-20T00:00:00.gz",
    }

    class _EmptyViewProxy:
        def execute(self, sql, *_a, **_kw):
            if "count(*)" in sql and "timestamp" in sql:
                return _StubRow(one=(0, None, None))
            return _StubRow(one=(0,))

    # 1. Empty view with prior status prefers last-known-good local_rows
    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch(
            "backend.config.get_status", return_value={"local_rows": 8_090_191, "latest_log_at": "2026-06-20T14:00:00"}
        ),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out = get_sync_status(_EmptyViewProxy(), fos_source, force=True)
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    assert out["local_rows"] == 8_090_191

    # 2. Empty view without prior status falls back to metadata sum
    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.iceberg.update_iceberg_view"),
    ):
        assert get_sync_status(_EmptyViewProxy(), fos_source, force=True)["local_rows"] == 400

    # 3. Empty view reads authoritative lake over stale prior
    table_name = _db._safe_table_name(fos_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP)")
    in_memory_duckdb.execute(
        "CREATE TABLE _cron_run_log (task VARCHAR, started_at VARCHAR, "
        "duration_s DOUBLE, status VARCHAR, error_message VARCHAR, summary VARCHAR)"
    )
    fresh_max = datetime(2026, 10, 2, 20, 31, 27)
    fresh_min = datetime(2026, 10, 1, 0, 0, 0)

    class _LakeEmptyViewProxy:
        def execute(self, sql, *a, **kw):
            if "duckdb_tables()" in sql and "database_name = 'lake'" in sql:
                return _StubRow(one=(1,))
            if 'FROM lake."' in sql and "count(*)" in sql:
                return _StubRow(one=(500, fresh_min, fresh_max))
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch(
            "backend.config.get_status", return_value={"local_rows": 13_948_280, "latest_log_at": "2026-09-22T00:58:06"}
        ),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.buffer_files", return_value=[]),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out_lake = get_sync_status(_LakeEmptyViewProxy(), fos_source, force=True)
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    assert (out_lake["local_rows"], out_lake["latest_log_at"], out_lake["earliest_log_at"]) == (
        500,
        safe_iso(fresh_max),
        safe_iso(fresh_min),
    )

    # 4. Non-empty stale view reconciles extents against authoritative lake
    stale_max = datetime(2026, 9, 22, 0, 58, 6)
    stale_min = datetime(2026, 6, 27, 0, 0, 0)
    wide_min = datetime(2026, 6, 20, 0, 0, 0)

    class _StaleNonEmptyProxy:
        def execute(self, sql, *a, **kw):
            if "duckdb_tables()" in sql and "database_name = 'lake'" in sql:
                return _StubRow(one=(1,))
            if 'FROM lake."' in sql and "count(*)" in sql:
                return _StubRow(one=(600, wide_min, fresh_max))
            if "count(*)" in sql and "min(timestamp)" in sql and "max(timestamp)" in sql:
                return _StubRow(one=(500, stale_min, stale_max))
            return in_memory_duckdb.execute(sql, *a, **kw)

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.buffer_files", return_value=[]),
    ):
        out_reconciled = get_sync_status(_StaleNonEmptyProxy(), fos_source, force=True)
    assert (out_reconciled["local_rows"], out_reconciled["latest_log_at"], out_reconciled["earliest_log_at"]) == (
        500,
        safe_iso(fresh_max),
        safe_iso(wide_min),
    )


def test_get_sync_status_ducklake_snapshot_cached_split_stats(in_memory_duckdb, fos_source, tmp_path):
    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}

    in_memory_duckdb.execute("ATTACH ':memory:' AS lake")
    in_memory_duckdb.execute("CREATE TABLE lake.logs (timestamp TIMESTAMP)")
    in_memory_duckdb.execute("INSERT INTO lake.logs VALUES ('2026-10-12 12:00:00'), ('2026-10-12 12:01:00')")

    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE VIEW {table_name} AS SELECT * FROM lake.logs")

    with _db._data_stats_cache_lock:
        _db._data_stats_cache.pop(src["name"], None)

    def _run_with_snap(snap_id: int):
        with (
            patch("backend.core.iceberg._ducklake.ducklake_table_name", return_value="logs"),
            patch("backend.core.iceberg._ducklake.ducklake_current_snapshot_id", return_value=snap_id),
            patch("backend.config.get_status", return_value={}),
        ):
            return get_sync_status(in_memory_duckdb, src, force=True)

    out1 = _run_with_snap(42)
    assert out1["local_rows"] == 2
    with _db._data_stats_cache_lock:
        cached = _db._data_stats_cache.get(src["name"])
    assert cached is not None and cached[0] == ("ducklake", 42) and cached[1] == 2

    in_memory_duckdb.execute("INSERT INTO lake.logs VALUES ('2026-10-12 12:02:00')")
    assert _run_with_snap(42)["local_rows"] == 2  # Cache hit
    assert _run_with_snap(43)["local_rows"] == 3  # Snapshot bump invalidates cache


# ── get_schema branches ──────────────────────────────────────────────


def test_get_schema_branches_and_cache(in_memory_duckdb, fos_source):
    assert isinstance(_schema_cache, BoundedTTLCache)
    _schema_cache.clear()

    # 1. Missing table -> []
    assert get_schema(in_memory_duckdb, fos_source) == []

    # 2. Cached within TTL short-circuits without executing SQL
    fake_schema = [{"name": "ip", "type": "VARCHAR"}]
    _schema_cache[(fos_source["name"], "logs_test_service", True)] = fake_schema
    exec_calls = {"n": 0}

    class _CountProxy:
        def execute(self, *_a, **_kw):
            exec_calls["n"] += 1
            return _StubRow(one=(0,))

    assert get_schema(_CountProxy(), fos_source) is fake_schema
    assert exec_calls["n"] == 0

    # 3. _clear_schema_cache preserves dict identity and clears selectively or globally
    _schema_cache.clear()
    _schema_cache[("a", "t", True)] = [{"name": "x"}]
    _schema_cache[("a", "t", False)] = [{"name": "x"}]
    _schema_cache[("b", "t", True)] = [{"name": "y"}]
    same = _schema_cache
    _clear_schema_cache("a")
    assert same is _schema_cache
    assert ("a", "t", True) not in _schema_cache and ("a", "t", False) not in _schema_cache
    assert ("b", "t", True) in _schema_cache
    _clear_schema_cache()
    assert len(_schema_cache) == 0

    # 4. SUMMARIZE error falls back to DESCRIBE
    class _FallbackProxy:
        def execute(self, sql, *_a, **_kw):
            if "information_schema.tables" in sql:
                return _StubRow(one=(1,))
            if sql.startswith("SUMMARIZE"):
                raise RuntimeError("Unsupported column type LIST(STRUCT)")
            if sql.startswith("DESCRIBE"):
                return _StubRow(
                    all_rows=[
                        ("ip", "VARCHAR", None, None, None, None),
                        ("status", "VARCHAR", None, None, None, None),
                    ]
                )
            raise AssertionError(f"unexpected SQL: {sql}")

    assert get_schema(_FallbackProxy(), fos_source) == [
        {"name": "ip", "type": "VARCHAR"},
        {"name": "status", "type": "VARCHAR"},
    ]

    # 5. Both SUMMARIZE and DESCRIBE failing returns []
    _schema_cache.clear()

    class _BothFailProxy:
        def execute(self, sql, *_a, **_kw):
            if "information_schema.tables" in sql:
                return _StubRow(one=(1,))
            raise RuntimeError("disk full")

    assert get_schema(_BothFailProxy(), fos_source) == []
    _ = _duckdb_status


# ── usage-log & reconciliation helpers ───────────────────────────────


def test_reconcile_fastly_stats_guards_and_fetch():
    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert reconcile_fastly_stats({"name": "svc"}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value=""),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0

    payload = {
        "data": [
            {
                "start_time": 1760000000,
                "object_storage_class_a_operations_count": 7,
                "object_storage_class_b_operations_count": 3,
            },
            {
                "start_time": 1760003600,
                "object_storage": {"class_a_operations_count": 2, "class_b_operations_count": 1},
            },
            {"start_time": None},
        ]
    }
    fetched: list = []

    def _fake_fastly(method, path, body=None, *, token, **_kw):
        fetched.append((method, path, token))
        return payload

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=_fake_fastly),
        patch("backend.core.metadata.reconcile_fastly_stats", return_value=2) as mock_recon,
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}, hours_back=2) == 2

    assert len(fetched) == 1 and fetched[0][0] == "GET" and fetched[0][2] == "tok"
    assert fetched[0][1].startswith("/stats/aggregate?by=hour&from=")
    assert [(h["class_a"], h["class_b"]) for h in mock_recon.call_args.args[1]] == [(7, 3), (2, 1)]

    # Fastly client errors swallowed
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("backend.core.metadata.get_latest_reconciliation_ts", return_value=None),
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 500")),
    ):
        assert reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"}) == 0


def test_usage_and_cron_forwarding_helpers():
    # purge_usage_log
    with (
        patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7}),
        patch("backend.core.metadata.purge_usage_log") as mock_purge,
    ):
        purge_usage_log({})
        mock_purge.assert_not_called()
        purge_usage_log({"name": "svc"})
        mock_purge.assert_called_once_with("svc", 7)

    # update_cron_duration
    with patch("backend.core.metadata.update_cron_duration") as mock_upd:
        update_cron_duration({}, run_id=1, duration_s=0.5)
        mock_upd.assert_not_called()
        update_cron_duration({"name": "svc"}, run_id=42, duration_s=1.25, log_output="ok")
        mock_upd.assert_called_once_with("svc", 42, 1.25, log_output="ok")

    # log_usage_calls
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET", "path": "/foo"}])
        mock_log.assert_not_called()

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls") as mock_log_on,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET"}], process_context="dashboard")
        mock_log_on.assert_called_once_with("svc", [{"method": "GET"}], process_context="dashboard")

    # backfill_fastly_edge_writes
    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        assert backfill_fastly_edge_writes({}) == 0

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=[]),
        patch("backend.core.metadata.log_synthetic_usage") as mock_synth_empty,
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 0
        mock_synth_empty.assert_not_called()

    files = [
        ("raw/2026-06-15T00:00:00Z/file.gz", "2026-06-15T00:00:00", 100, 5000),
        ("__seeding_attempted__", "2026-06-15T00:00:00", 0, 0),
        ("no-timestamp.gz", "2026-06-15T00:00:00", 50, 2500),
    ]
    captured: dict = {}
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=files),
        patch(
            "backend.core.metadata.log_synthetic_usage",
            side_effect=lambda sid, calls: (captured.update({"sid": sid, "calls": calls}), len(calls))[1],
        ),
    ):
        assert backfill_fastly_edge_writes({"name": "svc"}) == 2

    assert captured["sid"] == "svc" and len(captured["calls"]) == 2
    assert captured["calls"][0]["path"] == "raw/2026-06-15T00:00:00Z/file.gz"
    assert captured["calls"][0]["method"] == "PUT_OBJECT"
    assert captured["calls"][0]["_timestamp_override"] == "2026-06-15T00:00:00Z"
    assert captured["calls"][1]["_timestamp_override"] == "2026-06-15T00:00:00"


# ── Reconcile Fastly stats gate (DB integration) ─────────────────────


def _seed_reconciliation_row(service_id: str, ts_iso: str) -> None:
    from backend.core.metadata import usage_log_db as _usage_log_db

    con = _usage_log_db.get_con(service_id)
    con.execute(
        """
        INSERT INTO usage_log
        (timestamp, service_id, operation_class, operation_type, url, status,
         duration_ms, function_name, process_context, count)
        VALUES (?, ?, 'A', 'RECONCILE_A', 'fastly://stats/aggregate/test', 'OK',
                0.0, 'fastly.reconciliation', 'fastly:reconciliation', 1)
        """,
        (ts_iso, service_id),
    )
    con.commit()


@patch("backend.config.is_usage_logging_enabled", return_value=True)
@patch("backend.config.get_fastly_api_key", return_value="fake-key")
def test_reconcile_gate_recent_vs_old_vs_none(_key, _enabled):
    service_id = "recon-svc-1"
    metadata_db.insert_ingested_files(
        service_id, [("s3://bkt/raw/2026-05-22/12/2026-05-22T12:00:00.000-x.log.gz", 1, 1)]
    )
    src = {"name": service_id, "logging_service_id": "logsvc"}

    # 1. No prior reconciliation -> proceeds past gate
    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        _db.reconcile_fastly_stats(src)
        assert urlopen.called

    # 2. Old reconciliation (>1h ago) -> proceeds past gate
    old = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, old)
    with patch("urllib.request.urlopen", side_effect=RuntimeError("called!")) as urlopen:
        _db.reconcile_fastly_stats(src)
        assert urlopen.called

    # 3. Recent reconciliation (30m ago) -> skipped before HTTP request
    recent = (datetime.now(UTC) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_reconciliation_row(service_id, recent)
    with patch("urllib.request.urlopen") as urlopen:
        assert _db.reconcile_fastly_stats(src) == 0
        assert not urlopen.called


# ── RUM status freshness ─────────────────────────────────────────────


def _make_rum_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE rum_vitals_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, event_count BIGINT, value_sum DOUBLE,
            good_count BIGINT, ni_count BIGINT, poor_count BIGINT,
            p50_value DOUBLE, p75_value DOUBLE, p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE rum_error_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, error_count BIGINT
        )
    """)
    con.execute("CREATE TABLE client_vitals (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    con.execute("CREATE TABLE client_errors (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    return con


def test_rum_count_and_latest_empty_vs_populated_aggregates():
    # Empty aggregates still report raw count and MAX(timestamp)
    con1 = _make_rum_con()
    recent1 = datetime.now(UTC) - timedelta(seconds=30)
    con1.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        ["r1", "c1", recent1, "r2", "c2", recent1, "r3", "c3", recent1],
    )
    count1, latest1 = rum_count_and_latest(con1)
    assert count1 == 3 and latest1 is not None
    assert abs((latest1.replace(tzinfo=UTC) if latest1.tzinfo is None else latest1) - recent1) < timedelta(seconds=1)

    # Populated aggregates fast-path returns aggregate count + raw latest
    con2 = _make_rum_con()
    bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    con2.execute(
        "INSERT INTO rum_vitals_aggregates (service_id, bucket_start, dimension, value, event_count) "
        "VALUES (?, ?, 'total', 'pageviews', 10)",
        ["svc", bucket],
    )
    recent2 = datetime.now(UTC) - timedelta(seconds=15)
    con2.execute("INSERT INTO client_vitals VALUES (?, ?, ?)", ["r1", "c1", recent2])
    count2, latest2 = rum_count_and_latest(con2)
    assert count2 == 10 and latest2 is not None
    assert abs((latest2.replace(tzinfo=UTC) if latest2.tzinfo is None else latest2) - recent2) < timedelta(seconds=1)
