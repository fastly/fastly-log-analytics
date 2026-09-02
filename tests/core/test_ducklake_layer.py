"""Integration tests for the v3 DuckLake read/write path.

Real DuckDB + real DuckLake catalogs (no MagicMock counts): buffer commit
idempotency + tombstone contract, per-service tenant isolation under a
shared catalog, fast-path steady-state behavior, and view schema parity
(computed columns, decode CASEs, analyst time clamp).
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from backend import config as svcconfig
from backend.core.duckdb import _safe_table_name, get_connection
from backend.core.iceberg import manifest as manifest_mod
from backend.core.iceberg import view as view_mod
from backend.core.iceberg._ducklake import ducklake_table_name
from backend.core.iceberg.buffer import _commit_buffer_impl, buffer_files
from backend.core.iceberg.manifest import ducklake_table_exists, get_snapshot_calendar, get_table_info


def _make_source(tmp_path, name: str, **overrides) -> dict:
    cache = tmp_path / f"cache_{name}"
    (cache / "buffer").mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    src.update(overrides)
    return src


def _write_buffer(
    source: dict,
    filename: str,
    *,
    ts: datetime,
    source_file: str,
    n: int = 1,
    extra: dict | None = None,
) -> str:
    cols: dict = {
        "timestamp": pa.array([ts + timedelta(seconds=i) for i in range(n)], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
        "_source_file": pa.array([source_file] * n),
    }
    for k, v in (extra or {}).items():
        cols[k] = pa.array([v] * n)
    path = os.path.join(source["_cache_dir_override"], "buffer", filename)
    pq.write_table(pa.table(cols), path)
    return path


def _lake_count(source: dict) -> int:
    con = get_connection(source)
    try:
        table = ducklake_table_name(source)
        row = con.execute(f'SELECT count(*) FROM lake."{table}"').fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


def test_ducklake_table_name_is_per_service_and_sanitized():
    assert ducklake_table_name({"service_id": "AbC-123"}) == "logs_abc_123"
    assert ducklake_table_name({"name": "svc.two"}) == "logs_svc_two"
    assert ducklake_table_name({"name": "svc.two"}, "client_vitals") == "logs_svc_two__client_vitals"
    # hostile input cannot break out of an identifier
    assert ducklake_table_name({"service_id": 'x"; DROP TABLE lake.logs;--'}) == "logs_x___drop_table_lake_logs"


class TestCommitTombstoneContract:
    """Trap #26: commit must tombstone (not unlink) and be idempotent per file."""

    def test_commit_tombstones_instead_of_unlinking(self, tmp_path):
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        p = _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)

        res = _commit_buffer_impl(src)
        assert res["files_committed"] == 1
        assert res["rows_committed"] == 3
        assert _lake_count(src) == 3

        # The parquet must still exist on disk (views bound pre-commit keep
        # reading it) but be tombstoned out of new view binds.
        assert os.path.exists(p), "commit must NOT hard-unlink buffer parquet"
        assert buffer_files(src) == []
        buf_dir = os.path.dirname(p)
        markers = [f for f in os.listdir(buf_dir) if ".consumed-" in f]
        assert markers, "commit must write a tombstone sidecar"

    def test_recommit_after_lost_tombstone_does_not_duplicate(self, tmp_path):
        """Crash between INSERT and tombstone → the file is visible again on
        the next cycle. The DELETE-by-_source_file + INSERT transaction must
        replace, not append."""
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        p = _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)

        assert _commit_buffer_impl(src)["rows_committed"] == 3
        # Simulate the crash: tombstone never landed.
        buf_dir = os.path.dirname(p)
        for f in os.listdir(buf_dir):
            if ".consumed-" in f:
                os.remove(os.path.join(buf_dir, f))
        assert buffer_files(src) == [p]

        res2 = _commit_buffer_impl(src)
        assert res2["files_committed"] == 1
        assert _lake_count(src) == 3, "re-commit of the same buffer file must not duplicate rows"

    def test_second_source_file_commit_appends(self, tmp_path):
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3
        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2
        assert _lake_count(src) == 5


class TestTenantIsolation:
    def test_shared_catalog_views_cannot_see_other_tenant_rows(self, tmp_path, monkeypatch):
        """Two sources attached to ONE shared DuckLake catalog must not see
        each other's rows through their per-service views."""
        monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", str(tmp_path / "shared.ducklake"))
        a = _make_source(tmp_path, f"ta{uuid.uuid4().hex[:6]}")
        b = _make_source(tmp_path, f"tb{uuid.uuid4().hex[:6]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(a, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", extra={"host": "a.example"})
        _write_buffer(b, "batch_b.parquet", ts=ts, source_file="s3://b/raw/b.gz", extra={"host": "b.example"})

        assert _commit_buffer_impl(a)["rows_committed"] == 1
        assert _commit_buffer_impl(b)["rows_committed"] == 1

        for src, own_host, other_host in ((a, "a.example", "b.example"), (b, "b.example", "a.example")):
            view_mod.clear_source_caches(src["name"])
            con = get_connection(src, read_only=False)
            try:
                view_mod._update_iceberg_view_locked(con, src)
                view_name = _safe_table_name(src["name"])
                hosts = {r[0] for r in con.execute(f"SELECT host FROM {view_name}").fetchall()}
                assert own_host in hosts
                assert other_host not in hosts, f"{src['name']} view leaked another tenant's rows"
            finally:
                con.close()


class TestFastPathSteadyState:
    def test_two_consecutive_checkouts_do_not_rebuild(self, tmp_path, monkeypatch):
        """After one slow-path build, subsequent checkouts must bind via the
        lock-free fast path (the DuckLake-era token used to NEVER match,
        sending every read through the RLock + full rebuild)."""
        src = _make_source(tmp_path, f"fp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        # Open the connection FIRST — get_connection triggers its own view
        # build, which would consume the slow-path call this test counts.
        con = get_connection(src, read_only=False)
        view_mod.clear_source_caches(src["name"])

        slow_calls = {"n": 0}
        orig = view_mod._update_iceberg_view_locked

        def spy(c, source, target_table="logs", force=False):
            slow_calls["n"] += 1
            return orig(c, source, target_table=target_table, force=force)

        monkeypatch.setattr(view_mod._core_mod, "_update_iceberg_view_locked", spy)

        try:
            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 1
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is False

            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 1, "steady-state checkout must not rebuild"
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True

            # A second connection (fresh pool checkout) also fast-paths.
            con2 = get_connection(src, read_only=False)
            try:
                view_mod.update_iceberg_view(con2, src)
                assert slow_calls["n"] == 1
                assert view_mod.get_last_view_stats(src)["was_fast_path"] is True
            finally:
                con2.close()
        finally:
            con.close()

    def test_buffer_only_change_reconstructs_without_lock(self, tmp_path, monkeypatch):
        src = _make_source(tmp_path, f"fp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            view_mod.update_iceberg_view(con, src)

            slow_calls = {"n": 0}
            orig = view_mod._update_iceberg_view_locked

            def spy(c, source, target_table="logs", force=False):
                slow_calls["n"] += 1
                return orig(c, source, target_table=target_table, force=force)

            monkeypatch.setattr(view_mod._core_mod, "_update_iceberg_view_locked", spy)

            # New buffer file, no commit: the fast path must reconstruct the
            # view SQL (buffer-only change) instead of taking the lock.
            _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=3)
            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 0, "buffer-only change must stay on the lock-free fast path"
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True

            view_name = _safe_table_name(src["name"])
            row = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()
            assert row is not None and row[0] == 5, "reconstructed view must union lake + new buffer"
        finally:
            con.close()


class TestViewSchemaParity:
    def test_view_carries_computed_columns_and_decodes(self, tmp_path):
        from backend.utils import field_codes as fc

        src = _make_source(tmp_path, f"vp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 34, tzinfo=UTC)
        _write_buffer(
            src,
            "batch_a.parquet",
            ts=ts,
            source_file="s3://b/raw/a.gz",
            extra={"c_speed": fc.CONN_SPEED_ENCODE["broadband"], "ttl": 3599.7},
        )
        assert _commit_buffer_impl(src)["rows_committed"] == 1

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            view_mod._update_iceberg_view_locked(con, src)
            view_name = _safe_table_name(src["name"])
            row = con.execute(f"SELECT timestamp_hour, dt, c_speed, ttl FROM {view_name}").fetchone()
            assert row == ("2026-08-30-12", "2026-08-30", "broadband", 3600)
        finally:
            con.close()

    def test_analyst_time_range_clamp_applies_to_view(self, tmp_path):
        """Data-scoping regression guard: a read_only source's view must
        clamp to the granted time_range window on BOTH paths."""
        window_start = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        window_end = datetime(2026, 8, 30, 13, 0, tzinfo=UTC)
        src = _make_source(
            tmp_path,
            f"an{uuid.uuid4().hex[:8]}",
            access_level="read_only",
            time_range={"start": window_start.isoformat(), "end": window_end.isoformat()},
        )
        _write_buffer(src, "batch_in.parquet", ts=window_start + timedelta(minutes=5), source_file="s3://b/raw/in.gz")
        _write_buffer(src, "batch_out.parquet", ts=window_start - timedelta(hours=2), source_file="s3://b/raw/out.gz")
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            # Slow path
            view_mod._update_iceberg_view_locked(con, src)
            view_name = _safe_table_name(src["name"])
            rows = con.execute(f"SELECT timestamp FROM {view_name}").fetchall()
            assert len(rows) == 1, f"analyst view must clamp to time_range; got {rows}"
            assert rows[0][0].replace(tzinfo=UTC) >= window_start

            # Fast path (buffer-only change → reconstruction) must clamp too.
            _write_buffer(
                src, "batch_out2.parquet", ts=window_start - timedelta(hours=3), source_file="s3://b/raw/o2.gz"
            )
            view_mod.update_iceberg_view(con, src)
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True
            rows = con.execute(f"SELECT timestamp FROM {view_name}").fetchall()
            assert len(rows) == 1, "fast-path reconstruction dropped the analyst time clamp"
        finally:
            con.close()


class TestActiveHourDirectRead:
    def test_direct_read_includes_committed_lake_rows(self, tmp_path):
        """Committed rows live in lake.<table> post-v3 — the active-hour
        direct read must union them with the (non-tombstoned) buffer, with
        no double count."""
        from backend.repositories._base import QueryRunner

        src = _make_source(tmp_path, f"ah{uuid.uuid4().hex[:8]}")
        live_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_end = live_start + timedelta(hours=1)

        # 3 committed rows (drained buffer → tombstoned) + 2 fresh buffer rows.
        _write_buffer(src, "batch_a.parquet", ts=live_start + timedelta(minutes=1), source_file="s3://b/a.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3
        _write_buffer(src, "batch_b.parquet", ts=live_start + timedelta(minutes=30), source_file="s3://b/b.gz", n=2)

        con = get_connection(src)
        try:
            runner = QueryRunner(con, src)
            tmp_name = runner._create_active_hour_temp_direct(["ip"], ["timestamp", "ip"], live_start, live_end)
            assert tmp_name is not None
            row = con.execute(f'SELECT count(*) FROM "{tmp_name}"').fetchone()
            assert row is not None
            assert row[0] == 5, f"direct active-hour read must union lake (3 committed) + live buffer (2), got {row[0]}"
        finally:
            con.close()


def test_target_file_size_cap_asserted_on_rw_attach(tmp_path):
    """Item: ducklake_merge_adjacent_files must respect the same size cap as
    local compaction (LOCAL_COMPACT_MAX_PARTITION_MB, 256MB default) — the
    option is pinned on every read-write attach."""
    src = _make_source(tmp_path, f"sz{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz")
    assert _commit_buffer_impl(src)["rows_committed"] == 1

    con = get_connection(src)
    try:
        row = con.execute("SELECT value FROM lake.options() WHERE option_name = 'target_file_size'").fetchone()
        assert row is not None, "target_file_size must be set on the DuckLake catalog"
        assert int(row[0]) == 256 * 1024 * 1024
    finally:
        con.close()


class TestDuckLakeTableInfo:
    """get_table_info / get_snapshot_calendar / ducklake_table_exists —
    the DuckLake-native rewrite (v3 write-path cutover). The pyiceberg
    catalog these used to read is permanently frozen post-cutover; these
    now read `lake.*` state via a real DuckLake attach, no mocks."""

    def test_table_missing_before_first_commit(self, tmp_path):
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        assert ducklake_table_exists(src) is False

        info = get_table_info(src)
        assert info["error"]
        assert info["snapshots"] == 0
        assert info["data_files"] == 0
        assert info["size_bytes"] == 0
        assert info["min_timestamp"] is None
        assert info["max_timestamp"] is None
        assert get_snapshot_calendar(src) == {}

    def test_small_commit_is_inlined_but_still_counted(self, tmp_path):
        """Small inserts land "inlined" in DuckLake's catalog metadata
        rather than as parquet files — ducklake_table_info reports
        file_count=0 for a table that genuinely has committed rows. The
        panel must not show a false "0 files"; data_files falls back to
        the scanned row count in that state (see AGENTS.md DuckLake trap)."""
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        assert ducklake_table_exists(src) is True

        con = get_connection(src)
        try:
            table = ducklake_table_name(src)
            file_count = con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = ?", [table]
            ).fetchone()[0]
            assert file_count == 0, "test premise: a 2-row insert must be inlined, not written as a file"
        finally:
            con.close()

        info = get_table_info(src)
        assert "error" not in info
        assert info["snapshots"] == 1
        assert info["data_files"] == 2, "must fall back to the row count while inlined, not report 0"
        assert info["min_timestamp"] == "2026-08-30T12:00:00+00:00"
        assert info["max_timestamp"] == "2026-08-30T12:00:01+00:00"
        assert info["table_location"]

        calendar = get_snapshot_calendar(src)
        assert calendar == {"2026-08-30": {"data_files": 2, "size_bytes": 0}}

    def test_calendar_spans_multiple_days_across_commits(self, tmp_path):
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        day1 = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        day2 = datetime(2026, 8, 31, 6, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=day1, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2
        _write_buffer(src, "batch_b.parquet", ts=day2, source_file="s3://b/raw/b.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3

        calendar = get_snapshot_calendar(src)
        assert calendar == {
            "2026-08-30": {"data_files": 2, "size_bytes": 0},
            "2026-08-31": {"data_files": 3, "size_bytes": 0},
        }
        info = get_table_info(src)
        assert info["data_files"] == 5
        assert info["min_timestamp"] == "2026-08-30T12:00:00+00:00"
        assert info["max_timestamp"] == "2026-08-31T06:00:02+00:00"
        assert info["snapshots"] == 2

    def test_scan_is_cached_across_calls_until_next_commit(self, tmp_path, monkeypatch):
        """The 60s status-poller (_duckdb_status.refresh_config_status)
        calls get_table_info on a steady cadence — a full-table scan on
        every tick would regress the exact scan-cost class this codebase
        is pinned against. Cache key is the DuckLake snapshot id: repeat
        reads with no new commit must not re-scan; a new commit must."""
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        calls = {"n": 0}
        orig = manifest_mod._scan_ducklake_table_metadata

        def counting(con, lake_table):
            calls["n"] += 1
            return orig(con, lake_table)

        monkeypatch.setattr(manifest_mod, "_scan_ducklake_table_metadata", counting)

        get_table_info(src)
        get_snapshot_calendar(src)
        get_table_info(src)
        assert calls["n"] == 1, "repeat reads with no new commit must reuse the cached scan"

        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=1)
        assert _commit_buffer_impl(src)["rows_committed"] == 1
        get_table_info(src)
        assert calls["n"] == 2, "a new commit must invalidate the cached scan"
