"""Tests for ``backend.core.duckdb`` — pure helpers + small wrappers.

The big functions (`get_connection`, the FOS-client factory, the
schema cache, the view-cache initialization) need a real DuckDB + S3
stack and are covered by integration tests via
[test_iceberg.py](tests/core/test_iceberg.py) and the router tests.

This file pins the **pure helpers + small wrappers** that the bigger
code shares:

  - `_safe_iso` — datetime → ISO-Z normalisation
  - `_get_dma_map` — DMA code → name JSON loader (cached)
  - `_safe_table_name` — SQL identifier sanitisation
  - `_fos_glob` — S3 glob pattern with/without prefix
  - `_cache_dir` — local cache dir for a source
  - `is_configured` — required-fields predicate
  - `_build_default_source` / `reload_default_source` — module-global state
  - `get_source_for_service` — service-id → source resolution + cdn fallback
  - `_ensure_source_registered` / `record_audit` / `start_cron_run`
    — metadata_db wrappers
"""

from __future__ import annotations

import gc
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import duckdb
import pytest

import backend.core.duckdb as d
import backend.core.duckdb as duckdb_mod
import backend.core.duckdb_recycle as rec
from backend.core.duckdb import (
    DBBusyError,
    _safe_table_name,
    format_asn_label,
    get_connection,
    rum_duckdb_path,
    rum_source_for,
)
from backend.core.duckdb_pool import _Pool
from backend.core.iceberg.view import _get_service_lock
from backend.core.log_fields import LOG_FIELD_CATALOG


# ── Merged from tests/core/test_duckdb_helpers.py ──
def test_load_httpfs_prefers_cached_extension_without_installing():
    import backend.core.duckdb as db_mod

    con = MagicMock()

    db_mod._load_httpfs(con)

    con.execute.assert_called_once_with("LOAD httpfs;")


def test_load_httpfs_installs_when_cached_extension_is_missing():
    import duckdb

    import backend.core.duckdb as db_mod

    con = MagicMock()
    con.execute.side_effect = [duckdb.IOException("missing extension"), None, None]

    db_mod._load_httpfs(con)

    assert [call.args[0] for call in con.execute.call_args_list] == [
        "LOAD httpfs;",
        "INSTALL httpfs;",
        "LOAD httpfs;",
    ]


def test_safe_iso_none_returns_none():
    from backend.utils.date_utils import safe_iso as _safe_iso

    assert _safe_iso(None) is None


def test_safe_iso_naive_datetime_appends_z():
    """A naive (UTC-assumed) DuckDB datetime gets ``Z`` appended.
    Pinned because losing the Z would let JavaScript interpret the
    timestamp in the user's local timezone — log timestamps would
    drift by hours."""
    from datetime import datetime

    from backend.utils.date_utils import safe_iso as _safe_iso

    dt = datetime(2026, 5, 18, 12, 30, 0)
    out = _safe_iso(dt)
    assert out == "2026-05-18T12:30:00Z"


def test_safe_iso_already_tz_aware_datetime_is_not_double_suffixed():
    """A tz-aware datetime (with ``+00:00`` suffix) should NOT get an
    extra Z appended. Pinned because losing the guard would produce
    "2026-05-18T12:30:00+00:00Z" which fromisoformat rejects."""
    from datetime import UTC, datetime

    from backend.utils.date_utils import safe_iso as _safe_iso

    dt = datetime(2026, 5, 18, 12, 30, 0, tzinfo=UTC)
    out = _safe_iso(dt)
    assert out is not None
    assert not out.endswith("ZZ")


def test_safe_iso_falls_back_to_str_for_unknown_types():
    """Non-datetime objects → str(x). Pinned because DuckDB can
    return strings or other types from min()/max() — losing the
    fallback would crash with AttributeError."""
    from backend.utils.date_utils import safe_iso as _safe_iso

    assert _safe_iso("already-a-string") == "already-a-string"
    assert _safe_iso(42) == "42"


def test_get_dma_map_returns_empty_dict_when_no_data_file(tmp_path, monkeypatch):
    """No DMA file present → empty dict (not crash). Pinned because
    the network panel calls this on every render and a crash would
    break the dashboard."""
    import backend.core.duckdb as db_mod

    monkeypatch.chdir(tmp_path)
    # Reset the module-global cache so the test reads fresh from disk
    db_mod._dma_map_cache = None

    out = db_mod._get_dma_map()
    assert out == {}


def test_get_dma_map_loads_features_from_geojson(tmp_path, monkeypatch):
    """Reads dma_code + name from each feature. Pinned because the
    network panel maps DMA codes → names from this exact shape."""
    import json

    import backend.core.duckdb as db_mod

    monkeypatch.chdir(tmp_path)
    db_mod._dma_map_cache = None

    geo = {
        "features": [
            {"properties": {"dma_code": 501, "name": "New York"}},
            {"properties": {"dma_code": 803, "name": "Los Angeles"}},
        ]
    }
    (tmp_path / "data" / "system").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "system" / "dma_geojson.json").write_text(json.dumps(geo))

    out = db_mod._get_dma_map()
    assert out["501"] == "New York"
    assert out["803"] == "Los Angeles"


def test_get_dma_map_falls_back_to_dma_json_when_geojson_absent(tmp_path, monkeypatch):
    """Second-priority filename `dma.json` is loaded when the
    primary `dma_geojson.json` is missing. Pinned because old
    installs ship the .json variant only."""
    import json

    import backend.core.duckdb as db_mod

    monkeypatch.chdir(tmp_path)
    db_mod._dma_map_cache = None

    geo = {"features": [{"properties": {"dma_code": 101, "dma_name": "Boston"}}]}
    (tmp_path / "data" / "system").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "system" / "dma.json").write_text(json.dumps(geo))

    out = db_mod._get_dma_map()
    assert out["101"] == "Boston"


def test_get_dma_map_caches_result(tmp_path, monkeypatch):
    """Subsequent calls return the same dict object (cached).
    Pinned because the JSON load happens on every dashboard render
    — losing the cache would re-read + parse 100KB on each request."""
    import json

    import backend.core.duckdb as db_mod

    monkeypatch.chdir(tmp_path)
    db_mod._dma_map_cache = None

    (tmp_path / "data" / "system").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "system" / "dma_geojson.json").write_text(
        json.dumps({"features": [{"properties": {"dma_code": 1, "name": "X"}}]})
    )

    out1 = db_mod._get_dma_map()
    out2 = db_mod._get_dma_map()
    # Same object — not just equal
    assert out1 is out2


def test_safe_table_name_strips_non_alphanumeric_and_lowercases():
    """Non-alphanumeric chars → ``_``. Pinned because service names
    contain dots / hyphens that are invalid SQL identifiers — losing
    sanitisation would error at every table query."""
    from backend.core.duckdb import _safe_table_name

    assert _safe_table_name("My-Service.Logs") == "logs_my_service_logs"
    assert _safe_table_name("UPPER_CASE") == "logs_upper_case"


def test_safe_table_name_returns_logs_for_default():
    """``"default"`` → ``"logs"`` (no prefix). Pinned because the
    legacy single-service install uses ``"logs"`` as the table name
    and renaming would break existing deployments."""
    from backend.core.duckdb import _safe_table_name

    assert _safe_table_name("default") == "logs"


def test_safe_table_name_strips_leading_trailing_underscores():
    """Leading/trailing underscores from `re.sub` are stripped.
    Pinned because the underscore-bracketed identifier (``___name___``)
    is technically valid SQL but reads as malformed."""
    from backend.core.duckdb import _safe_table_name

    assert _safe_table_name("---test---") == "logs_test"


def test_cache_dir_uses_bucket_for_scoping():
    """`_cache_dir` is keyed on bucket so two sources with the same
    name but different buckets get separate caches. Pinned because
    sharing would cross-contaminate parquet files between services."""
    from backend.core.duckdb import _cache_dir

    out = _cache_dir({"bucket": "my-bucket"})
    assert out.endswith("my-bucket") or "my-bucket" in out


def test_cache_dir_respects_override():
    """`_cache_dir_override` key bypasses the bucket-based path.
    Pinned because tests + provisioning use this to isolate from
    the production cache dir."""
    from backend.core.duckdb import _cache_dir

    out = _cache_dir({"_cache_dir_override": "/tmp/test-cache"})
    assert out == "/tmp/test-cache"


def test_cache_dir_falls_back_to_default_when_no_bucket():
    """No bucket → ``cache/default``. Pinned because the legacy
    single-service install has no bucket field; losing the fallback
    would render `cache/None`."""
    from backend.core.duckdb import _cache_dir

    out = _cache_dir({})
    assert "default" in out


def test_data_stats_fingerprint_returns_none_when_data_dir_absent(tmp_path):
    """No data/ → None so callers know to skip the data-side cache and
    fall back to the full view query. Pinned because returning a junk
    fingerprint here would cause `(None) == (None)` cache hits on
    services that haven't materialized any parquet yet."""
    from backend.core.duckdb import _data_stats_fingerprint

    src = {"_cache_dir_override": str(tmp_path / "missing")}
    assert _data_stats_fingerprint(src) is None


def test_data_stats_fingerprint_stable_when_no_changes(tmp_path):
    """Two reads with no fs changes return identical tuples. Pinned
    because cache-hit detection relies on bit-exact equality."""
    import os

    from backend.core.duckdb import _data_stats_fingerprint

    cache_root = tmp_path / "cache"
    (cache_root / "data" / "ts=1").mkdir(parents=True)
    (cache_root / "data" / "ts=1" / "f.parquet").write_bytes(b"x")
    src = {"_cache_dir_override": str(cache_root)}

    fp1 = _data_stats_fingerprint(src)
    fp2 = _data_stats_fingerprint(src)
    assert fp1 is not None
    assert fp1 == fp2
    # data dir present with 1 partition
    assert fp1[1] == 1
    # Touch nothing → still equal
    os.stat(cache_root / "data")
    assert _data_stats_fingerprint(src) == fp1


def test_data_stats_fingerprint_changes_when_partition_added(tmp_path):
    """Adding a new partition dir changes the fingerprint so the
    cached COUNT is invalidated. Pinned because stale COUNT after
    optimize or post-commit would surface as the dashboard pinning
    to the pre-compaction row total forever."""
    from backend.core.duckdb import _data_stats_fingerprint

    cache_root = tmp_path / "cache"
    data_dir = cache_root / "data"
    (data_dir / "ts=1").mkdir(parents=True)
    src = {"_cache_dir_override": str(cache_root)}

    fp_before = _data_stats_fingerprint(src)
    # Count differs (1 → 2) so the fingerprint tuple must differ regardless
    # of mtime — no need to sleep past the FS timer.
    (data_dir / "ts=2").mkdir()
    fp_after = _data_stats_fingerprint(src)
    assert fp_before is not None
    assert fp_after is not None
    assert fp_before != fp_after
    assert fp_after[1] == fp_before[1] + 1


def test_data_stats_fingerprint_ignores_buffer_changes(tmp_path):
    """Buffer churn must NOT bust the data-side cache — the whole point
    of splitting the query was so buffer writes every tick don't
    invalidate the heavy data-side count. Buffer-side stats are
    recomputed fresh by the caller, so cache invalidation here would be
    pure waste. Pinned because the previous combined-fingerprint design
    blew up the cache hit rate to ~0% on busy services (see
    update_iceberg_view_clears_schema_cache memory)."""
    from backend.core.duckdb import _data_stats_fingerprint

    cache_root = tmp_path / "cache"
    (cache_root / "data" / "ts=1").mkdir(parents=True)
    buf_dir = cache_root / "buffer"
    buf_dir.mkdir()
    src = {"_cache_dir_override": str(cache_root)}

    fp_before = _data_stats_fingerprint(src)
    (buf_dir / "batch_001.parquet").write_bytes(b"x")
    fp_after_add = _data_stats_fingerprint(src)
    (buf_dir / "batch_001.parquet").unlink()
    fp_after_drain = _data_stats_fingerprint(src)
    assert fp_before is not None
    assert fp_before == fp_after_add == fp_after_drain


def test_is_configured_true_for_fully_populated_source():
    """All four required fields populated → True. Pinned because the
    scheduler skips unconfigured services from job registration."""
    from backend.core.duckdb import is_configured

    src = {"endpoint": "e", "access_key_id": "k", "secret_access_key": "s", "bucket": "b"}
    assert is_configured(src) is True


def test_is_configured_false_when_any_required_field_missing():
    """Missing any of endpoint/access_key_id/secret_access_key/
    bucket → False. Pinned because partial provisioning shouldn't
    register cron jobs that would fail every interval."""
    from backend.core.duckdb import is_configured

    base = {"endpoint": "e", "access_key_id": "k", "secret_access_key": "s", "bucket": "b"}
    for missing in ["endpoint", "access_key_id", "secret_access_key", "bucket"]:
        src = {k: v for k, v in base.items() if k != missing}
        assert is_configured(src) is False, f"should be False when {missing} missing"


def test_is_configured_false_when_required_field_is_empty_string():
    """Empty string counts as missing (not as set). Pinned because
    teardown sets credentials to "" — losing the truthy check would
    keep registering jobs for half-torn-down services."""
    from backend.core.duckdb import is_configured

    src = {"endpoint": "e", "access_key_id": "", "secret_access_key": "s", "bucket": "b"}
    assert is_configured(src) is False


def test_build_default_source_returns_empty_when_no_configs():
    """No services configured → returns a source dict with empty
    credentials but valid structure. Pinned because the dashboard
    pre-config still calls is_configured() on this dict — None
    would crash."""
    import backend.core.duckdb as db_mod

    with patch("backend.config.list_configs", return_value=[]):
        src = db_mod._build_default_source()

    assert src["name"] == "default"
    assert src["endpoint"] == ""
    assert src["bucket"] == ""
    assert src["region"] == "us-east-1"  # Default region


def test_build_default_source_uses_first_configured_service():
    """When services exist, build from the first one. Pinned because
    the legacy single-service installs rely on this fallback to
    auto-pick their only service."""
    import backend.core.duckdb as db_mod

    fake_cfg = {"service_id": "svc-1"}
    fake_src = {"name": "svc-1", "bucket": "b", "endpoint": "e"}

    with (
        patch("backend.config.list_configs", return_value=[fake_cfg]),
        patch("backend.config.config_to_source", return_value=fake_src),
    ):
        src = db_mod._build_default_source()

    assert src["bucket"] == "b"
    assert src["name"] == "svc-1"


def test_reload_default_source_updates_module_globals():
    """`reload_default_source` mutates _DEFAULT_SOURCE in place +
    updates DUCKDB_PATH module-global.
    Pinned because all other modules import these at startup and
    keep their reference — mutate-in-place is the only way to
    propagate."""
    import backend.core.duckdb as db_mod

    original_default = dict(db_mod._DEFAULT_SOURCE)

    fake_cfg = {"service_id": "new-svc"}
    fake_src = {
        "name": "new-svc",
        "endpoint": "new-endpoint",
        "bucket": "new-bucket",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-west-2",
        "duckdb_path": "/tmp/new.duckdb",
        "storage_mode": "local",
        "access_level": "read_only",
    }

    try:
        with (
            patch("backend.config.list_configs", return_value=[fake_cfg]),
            patch("backend.config.config_to_source", return_value=fake_src),
        ):
            db_mod.reload_default_source()

        assert db_mod._DEFAULT_SOURCE["bucket"] == "new-bucket"
        assert db_mod.DUCKDB_PATH == "/tmp/new.duckdb"
    finally:
        # Restore module globals so we don't leak state into other tests
        db_mod._DEFAULT_SOURCE.clear()
        db_mod._DEFAULT_SOURCE.update(original_default)
        db_mod.DUCKDB_PATH = original_default.get("duckdb_path", "logs.duckdb")


def test_get_source_for_service_returns_none_for_unknown_service():
    """No matching config + no cdn_service_id match → None. Pinned
    because callers check `if src` before using — None is the
    distinct "not configured" signal vs an empty dict."""
    from backend.core.duckdb import get_source_for_service

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.list_configs", return_value=[]),
    ):
        assert get_source_for_service("ghost-svc") is None


def test_get_source_for_service_returns_built_source_for_known_service():
    """Known service_id → built source. Pinned because every cron
    job's first call is get_source_for_service to bootstrap."""
    from backend.core.duckdb import get_source_for_service

    fake_cfg = {"service_id": "svc-1"}
    fake_src = {"name": "svc-1", "bucket": "b"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_to_source", return_value=fake_src),
    ):
        out = get_source_for_service("svc-1")
    assert out["name"] == "svc-1"


def test_get_source_for_service_falls_back_to_cdn_service_id_match():
    """When no service config exists for the ID, search all configs
    for a matching ``cdn_service_id``. Pinned because the FE
    sometimes passes the CDN service ID (used by analytics paths)
    and the lookup must find the underlying logging service."""
    from backend.core.duckdb import get_source_for_service

    fake_cfg = {"service_id": "log-svc", "cdn_service_id": "cdn-id"}
    fake_src = {"name": "log-svc", "bucket": "b"}

    with (
        patch("backend.config.load_config", return_value=None),  # No direct match
        patch("backend.config.list_configs", return_value=[fake_cfg]),
        patch("backend.config.config_to_source", return_value=fake_src),
    ):
        out = get_source_for_service("cdn-id")
    assert out["name"] == "log-svc"


def test_ensure_source_registered_passes_config_json_to_metadata_db():
    """Builds a JSON blob of the source's FOS+CDN config and registers
    it via metadata_db. Pinned because the persisted blob is what
    ``get_sources`` later returns to the dashboard."""
    from backend.core.duckdb import _ensure_source_registered

    src = {
        "name": "test-svc",
        "endpoint": "ep",
        "bucket": "b",
        "prefix": "p",
        "region": "us-east-1",
        "cdn_url": "https://cdn.x",
        "cdn_secret": "shh",
    }

    with patch("backend.core.metadata.register_source") as mock_register:
        table_name = _ensure_source_registered(src)

    # Returns the sanitised table name
    assert table_name == "logs_test_svc"
    # The registered JSON blob includes the right keys
    args = mock_register.call_args[0]
    import json as _json

    payload = _json.loads(args[2])
    assert payload["bucket"] == "b"
    assert payload["cdn_url"] == "https://cdn.x"
    assert payload["cdn_secret"] == "shh"


def test_start_cron_run_delegates_to_metadata():
    """start_cron_run delegates directly to metadata.start_cron_run
    without per-tick purging. Retention is handled by the daily
    cleanup_metadata job (reconciliation._CLEANUP_TABLES)."""
    from backend.core.duckdb import start_cron_run

    with patch("backend.core.metadata.start_cron_run", return_value=99) as mock_start:
        run_id = start_cron_run({"name": "svc-1"}, "sync")

    assert run_id == 99
    mock_start.assert_called_once_with("svc-1", "sync")


def test_execute_query_with_retry_returns_immediately_on_success():
    """Successful execute → return the result without retrying.
    Pinned because the retry loop adds backoff sleep — losing the
    success short-circuit would add seconds of latency to every
    DuckDB query."""
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.return_value = "result"

    out = _execute_query_with_retry(fake_con, "SELECT 1")
    assert out == "result"
    assert fake_con.execute.call_count == 1


def test_execute_query_with_retry_fails_fast_on_401():
    """`HTTP 401` in error message → raise RuntimeError immediately
    (no retry). Pinned because 401s are auth failures — retrying
    wastes time AND can lock out the customer's account."""
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = RuntimeError("HTTP 401 on 'https://x.example/key'")

    with pytest.raises(RuntimeError, match="401"):
        _execute_query_with_retry(fake_con, "SELECT 1")
    # No retry — single call
    assert fake_con.execute.call_count == 1


def test_execute_query_with_retry_fails_fast_on_403():
    """Same as 401 — 403 is also auth, no retry."""
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = RuntimeError("HTTP 403 forbidden")

    with pytest.raises(RuntimeError, match="403"):
        _execute_query_with_retry(fake_con, "SELECT 1")
    assert fake_con.execute.call_count == 1


def test_execute_query_with_retry_raises_immediately_on_non_transient_error():
    """Non-network errors (syntax error, etc.) → raise immediately
    without retry. Pinned because retrying a syntax error would
    add 30s of pointless delay before surfacing the real bug."""
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = RuntimeError("Parser error: syntax error near 'FROOM'")

    with pytest.raises(RuntimeError, match="Parser error"):
        _execute_query_with_retry(fake_con, "SELEKT 1")
    assert fake_con.execute.call_count == 1


def test_execute_query_with_retry_retries_on_transient_then_succeeds():
    """A transient `io error` followed by success → return the
    successful result. Pinned because losing the retry would surface
    every momentary S3 hiccup as a failed dashboard render.

    Transient DuckDB failures surface as ``duckdb.IOException`` (and
    kin), never ``RuntimeError`` — the latter is reserved for the
    translated auth-failure, which is intentionally non-retryable."""
    import duckdb

    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = [
        duckdb.IOException("IO Error: timeout"),
        "success",
    ]

    with patch("time.sleep"):  # don't actually sleep
        out = _execute_query_with_retry(fake_con, "SELECT 1", max_retries=2)
    assert out == "success"
    assert fake_con.execute.call_count == 2


def test_execute_query_with_retry_gives_up_after_max_retries():
    """After max_retries+1 attempts, the original exception is
    re-raised. Pinned because losing the bound would loop forever
    on permanent network failures."""
    import duckdb

    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = duckdb.IOException("connection refused")

    with patch("time.sleep"):
        with pytest.raises(duckdb.IOException, match="connection refused"):
            _execute_query_with_retry(fake_con, "SELECT 1", max_retries=2)
    # max_retries=2 → 3 total attempts (0, 1, 2)
    assert fake_con.execute.call_count == 3


def test_clear_initialization_state_removes_path_from_set():
    """`clear_initialization_state` removes the path from the
    `_initialized_paths` global. Pinned because DDL re-runs depend on
    this — losing the discard would skip re-init when a DB file is
    deleted and recreated."""
    import backend.core.duckdb as db_mod

    db_mod._initialized_paths.add("/tmp/test-path.duckdb")
    db_mod.clear_initialization_state("/tmp/test-path.duckdb")
    assert "/tmp/test-path.duckdb" not in db_mod._initialized_paths


def test_clear_initialization_state_is_noop_for_absent_path():
    """Clearing an unknown path doesn't raise. Pinned because
    teardown calls this unconditionally even when DDL never ran."""
    from backend.core.duckdb import clear_initialization_state

    # Should NOT raise
    clear_initialization_state("/tmp/never-initialized.duckdb")


def test_close_all_connections_is_noop():
    """`close_all_connections` is a no-op (kept for backward-compat).
    Pinned because conftest fixture calls it; raising would break
    pytest teardown."""
    from backend.core.duckdb import close_all_connections

    # Should NOT raise
    close_all_connections()


def test_dbbusy_error_is_an_exception_subclass():
    """`DBBusyError` is an Exception subclass admin endpoints catch
    to convert to 503. Pinned because losing the exception base
    would let `try: ... except DBBusyError` not match anymore."""
    from backend.core.duckdb import DBBusyError

    assert issubclass(DBBusyError, Exception)
    # Instantiable + carries message
    err = DBBusyError("locked")
    assert str(err) == "locked"


def test_log_cron_run_persists_error_runs_regardless_of_log_enabled():
    """Error runs are ALWAYS persisted, even when
    `cron_*.log_enabled=False`. Pinned because losing this would
    hide failures from the system-jobs panel — admins must see
    errors regardless of the success-suppression toggle."""
    from backend.core.duckdb import log_cron_run

    log_calls = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"log_enabled": False}}},
        ),
        patch(
            "backend.core.metadata.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.5, "error", error_message="bad", run_id=42)

    assert len(log_calls) == 1


def test_log_cron_run_deletes_pending_when_success_with_log_disabled():
    """`log_enabled=False` + status=success → DELETE the pending
    "running" row (don't leave it lingering). Pinned because losing
    this would leave forever-running phantom runs in the UI."""
    from backend.core.duckdb import log_cron_run

    deleted = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"log_enabled": False}}},
        ),
        patch(
            "backend.core.metadata.delete_cron_run",
            side_effect=lambda sid, rid: deleted.append((sid, rid)),
        ),
        patch("backend.core.metadata.log_cron_run") as mock_log,
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", run_id=42)

    # Persistence was suppressed
    mock_log.assert_not_called()
    # But the pending row was deleted
    assert deleted == [("svc", 42)]


def test_log_cron_run_upgrades_status_to_partial_success_on_corrupt_rows():
    """Success status + corrupt_rows > 0 → upgraded to
    `partial_success`. Pinned because the FE renders a yellow
    badge for partial vs green for full success.

    DUMB-01: the status reaches ``metadata_db.log_cron_run`` as POSITIONAL arg
    index 3 (``service_id, task, duration_s, status, …`` — see duckdb.py). The
    old assertion captured only ``kwargs``, so ``"partial" in str(captured)``
    was always False and the trailing ``or True`` made the whole test a no-op.
    Capture positional args and assert the actual status."""
    from backend.core.duckdb import log_cron_run

    captured = {}

    with (
        patch("backend.config.load_config", return_value={"provisioning": {}}),
        patch(
            "backend.core.metadata.log_cron_run",
            side_effect=lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs),
        ),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", corrupt_rows=5)

    # status is positional arg 3 (service_id, task, duration_s, status).
    assert captured["args"][3] == "partial_success", (
        f"status reached metadata_db as {captured['args'][3]!r}, expected 'partial_success'"
    )


def test_log_cron_run_keeps_success_when_no_corrupt_rows():
    """Negative case for the upgrade above (pattern: test_scheduler.py): with
    corrupt_rows == 0 the status stays 'success' (green badge). Pins that the
    partial_success upgrade is CONDITIONAL — a blanket upgrade would mislabel
    every clean sync as partial."""
    from backend.core.duckdb import log_cron_run

    captured = {}

    with (
        patch("backend.config.load_config", return_value={"provisioning": {}}),
        patch(
            "backend.core.metadata.log_cron_run",
            side_effect=lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs),
        ),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", corrupt_rows=0)

    assert captured["args"][3] == "success", (
        f"status reached metadata_db as {captured['args'][3]!r}; corrupt_rows=0 must not upgrade"
    )


def test_log_cron_run_only_local_compact_consults_cron_compact_config():
    """Only `local_compact` consults cron_compact.log_enabled — and `sync`
    consults cron_sync.log_enabled. Every other task (commit, optimize,
    expire, full_sync, gap_heal, alerts, ngwaf_sync, metadata_sync,
    metadata_cleanup) ignores both flags and always persists success rows.

    Pinned to the 2026-06-04 fix: the prior code used a
    `"cron_sync" if task=="sync" else "cron_compact"` ternary, which
    silently coupled every non-sync task to cron_compact.log_enabled.
    Setting cron_compact.log_enabled=false on a service was therefore
    suppressing success rows for everything except sync — including the
    new metadata_cleanup cron, which has no relationship to compaction."""
    from backend.core.duckdb import log_cron_run

    # local_compact still respects cron_compact.log_enabled — drop row.
    deleted_lc: list = []
    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_compact": {"log_enabled": False}}},
        ),
        patch(
            "backend.core.metadata.delete_cron_run",
            side_effect=lambda sid, rid: deleted_lc.append((sid, rid)),
        ),
        patch("backend.core.metadata.log_cron_run"),
    ):
        log_cron_run({"name": "svc"}, "local_compact", 1.0, "success", run_id=11)
    assert deleted_lc == [("svc", 11)], "local_compact should honor cron_compact.log_enabled"

    # Unrelated tasks ignore cron_compact.log_enabled — log persists.
    for task in ("commit", "optimize", "expire", "full_sync", "metadata_cleanup", "metadata_sync"):
        deleted: list = []
        logged: list = []
        with (
            patch(
                "backend.config.load_config",
                return_value={"provisioning": {"cron_compact": {"log_enabled": False}}},
            ),
            patch(
                "backend.core.metadata.delete_cron_run",
                side_effect=lambda sid, rid: deleted.append((sid, rid)),
            ),
            patch(
                "backend.core.metadata.log_cron_run",
                side_effect=lambda *args, **kwargs: logged.append((args, kwargs)),
            ),
        ):
            log_cron_run({"name": "svc"}, task, 1.0, "success", run_id=42)
        assert deleted == [], f"{task} must NOT delete its row when cron_compact is disabled"
        assert len(logged) == 1, f"{task} must persist its success row regardless of cron_compact.log_enabled"


def test_log_cron_run_swallows_metadata_db_exception():
    """If `metadata_db.log_cron_run` raises (DB locked), log a
    warning and return — don't crash the cron worker. Pinned
    because losing this would kill the scheduler thread on a
    transient SQLite lock."""
    from backend.core.duckdb import log_cron_run

    with (
        patch("backend.config.load_config", return_value={"provisioning": {}}),
        patch(
            "backend.core.metadata.log_cron_run",
            side_effect=RuntimeError("database is locked"),
        ),
    ):
        # Should NOT raise
        log_cron_run({"name": "svc"}, "sync", 1.0, "success")


def test_get_sync_status_returns_configured_false_when_not_configured():
    """Unconfigured source → minimal dict with `configured=False`.
    Pinned because the FE renders the "first-time setup" CTA from
    this exact shape — losing fields would crash the dashboard."""
    from backend.core.duckdb import get_sync_status

    # No bucket/endpoint/creds → is_configured returns False
    src = {"name": "fresh-svc"}
    out = get_sync_status(MagicMock(), src)

    assert out["configured"] is False
    assert out["local_rows"] == 0
    assert out["ingested"] == 0
    assert out["fos_total"] == 0


def test_get_sync_status_derives_fos_fields_from_local_ingested_files():
    """``get_sync_status`` MUST NOT issue a ``glob('s3://.../raw/**/*.gz')``
    to compute ``fos_total`` / ``latest_available_file_at``.

    After Phase 4 (proxy default ON), DuckDB httpfs routes through the
    telemetry proxy → Fastly CDN, and the CDN VCL doesn't authorize LIST.
    Every glob therefore returns HTTP 403 — wasted bandwidth on a code
    path that should never have touched the cloud for a dashboard read.

    The deal we make: derive the fields from local ``ingested_files``.
    ``refresh_config_status`` only runs AFTER the ingest cron, so ingest
    just LISTed FOS (via boto3, signed direct) and persisted what it
    found. ``fos_total`` collapses to the local count and
    ``latest_available_file_at`` collapses to ``latest_ingested_file_at``.
    The lag is exactly one ingest cycle — the user explicitly preferred
    "a little behind" over "extra reads".
    """
    from backend.core import duckdb as _db
    from backend.core.duckdb import get_sync_status

    src = {
        "name": "svc-derive",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
        "prefix": "p",
    }

    # Summary shape matches get_ingested_files_status_summary rollup:
    # aggregates derived from three files with row_count 1000/2000/3000
    # and bytes 100/200/300. latest_file_name encodes the Fastly
    # YYYY-MM-DDTHH-MM-SS pattern so latest_available_file_at can be
    # parsed without hitting FOS.
    summary = {
        "file_count": 3,
        "total_rows": 6000,
        "total_bytes": 600,
        "count_with_bytes": 3,
        "last_ingested": "2026-05-19T02:00:01Z",
        "latest_file_name": "s3://b/p/raw/2026-05-19/02/2026-05-19T02-00-00.ccc.gz",
    }

    class _FakeRow:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

        def fetchall(self):
            return [self.row] if self.row else []

    class _FakeCon:
        def execute(self, sql, *args):
            # No iceberg table in this test — keep the local-derivation
            # branch isolated. The function tolerates missing tables.
            if "information_schema.tables" in sql:
                return _FakeRow(None)
            return _FakeRow(None)

    # Fail loudly if anything attempts a glob — that's the contract.
    def _fail_if_glob(con, query, **_kw):
        assert "glob(" not in query.lower(), f"get_sync_status MUST NOT glob FOS for dashboard fields; saw: {query}"
        return _FakeRow(None)

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch.object(_db, "_execute_query_with_retry", side_effect=_fail_if_glob),
    ):
        out = get_sync_status(_FakeCon(), src, skip_fos=False, force=True)

    assert out["fos_total"] == 3, f"expected fos_total to equal local ingest count; got {out['fos_total']}"
    assert out["latest_available_file_at"] is not None
    assert out["latest_available_file_at"] == out["latest_ingested_file_at"], (
        "with derivation, latest_available_file_at collapses to latest_ingested_file_at — "
        f"got latest_available_file_at={out['latest_available_file_at']!r}, "
        f"latest_ingested_file_at={out['latest_ingested_file_at']!r}"
    )


def test_get_sync_status_skip_fos_returns_cached_status_immediately():
    """When `skip_fos=True` and a cached status exists, return it
    without hitting the table or S3. Pinned because the header
    status badge polls this every few seconds; a single uncached
    call would create dashboard latency spikes."""
    from backend.core.duckdb import get_sync_status

    src = {
        "name": "svc",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
    }
    cached = {"local_rows": 999, "ingested": 100, "fos_new": 5, "fos_total": 200}

    with (
        patch("backend.config.get_status", return_value=dict(cached)),
        patch("backend.core.metadata.get_ingested_files_status_summary") as mock_summary,
    ):
        out = get_sync_status(MagicMock(), src, skip_fos=True)

    # Cached values surface
    assert out["local_rows"] == 999
    assert out["ingested"] == 100
    # Runtime fields injected
    assert out["configured"] is True
    assert "access_level" in out
    # Avoided the metadata_db query entirely
    mock_summary.assert_not_called()


def test_get_raw_tree_node_returns_empty_children_when_not_configured():
    """Unconfigured source → empty children list (not crash). Pinned
    because the admin file-browser endpoint calls this on every
    page load — must render an empty tree instead of 500."""
    from backend.core.duckdb import get_raw_tree_node

    out = get_raw_tree_node({"name": "fresh"})
    assert out == {"children": []}


def test_get_raw_tree_node_builds_dirs_and_files_from_paginated_objects():
    """Immediate files become file entries; nested keys aggregate
    into a directory entry with summed size + count. Pinned because
    the admin file-browser's tree rendering keys on these exact
    shapes."""
    from datetime import UTC, datetime

    from backend.core.duckdb import get_raw_tree_node

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/file1.gz", "Size": 100, "LastModified": datetime.now(UTC)},
                {"Key": "raw/subdir/a.gz", "Size": 50, "LastModified": datetime.now(UTC)},
                {"Key": "raw/subdir/b.gz", "Size": 70, "LastModified": datetime.now(UTC)},
            ]
        }
    ]
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator

    src = {
        "name": "svc",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
        "prefix": "",
    }

    with patch("backend.core.duckdb._get_fos_client", return_value=fake_s3):
        out = get_raw_tree_node(src, prefix_filter="", root="raw")

    children = out["children"]
    # 1 directory + 1 file = 2 children
    assert len(children) == 2
    # Directory first (sorted alphabetically)
    dir_entry = next(c for c in children if c["type"] == "directory")
    assert dir_entry["name"] == "subdir"
    assert dir_entry["size"] == 120  # 50 + 70

    file_entry = next(c for c in children if c["type"] == "file")
    assert file_entry["name"] == "file1.gz"
    assert file_entry["size"] == 100


def test_get_raw_tree_node_swallows_s3_exception_returns_empty():
    """An S3 list_objects failure → empty children + log (no crash).
    Pinned because losing this would 500 the admin file-browser
    on any transient S3 issue."""
    from backend.core.duckdb import get_raw_tree_node

    src = {
        "name": "svc",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
        "prefix": "",
    }

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("S3 timeout")):
        out = get_raw_tree_node(src)

    assert out == {"children": []}


def test_format_asn_label_falls_back_to_AS_number_when_name_missing():
    """Empty/None name → ``AS{number}``. Pinned because the
    dashboard's ASN dropdown rows would otherwise render as
    "( 7922)" with a leading space when the name is empty."""
    from backend.core.duckdb import format_asn_label

    assert format_asn_label(7922, "") == "AS7922"
    assert format_asn_label(7922, None) == "AS7922"


def test_format_asn_label_strips_redundant_AS_prefix():
    """When the name is already the AS-form (`AS7922`), render
    plain `AS{n}` instead of `AS7922 (7922)`. Pinned because
    losing this would produce ugly double-AS labels."""
    from backend.core.duckdb import format_asn_label

    assert format_asn_label(7922, "AS7922") == "AS7922"


def test_format_asn_label_renders_name_with_number_in_parens():
    """Real name → `"Comcast Cable (7922)"` form. Pinned because
    the FE picker shows both fields — losing the parens would
    confuse two ASNs with the same name."""
    from backend.core.duckdb import format_asn_label

    assert format_asn_label(7922, "Comcast Cable") == "Comcast Cable (7922)"


def test_get_asn_names_returns_empty_for_empty_input():
    """Empty list → empty dict (no DB query, no whois). Pinned
    because the picker calls this on every render and an empty
    page shouldn't trigger a network roundtrip."""
    from backend.core.duckdb import get_asn_names

    assert get_asn_names("svc-1", []) == {}


def test_get_asn_names_returns_empty_when_service_id_empty():
    """No service_id → empty dict (would crash metadata_db
    otherwise). Pinned because the global default source has no
    service_id."""
    from backend.core.duckdb import get_asn_names

    assert get_asn_names("", [7922]) == {}


def test_get_asn_names_returns_cached_values_without_whois_lookup():
    """When all requested ASNs hit the cache, skip the cymruwhois
    network call. Pinned because losing this would call
    cymruwhois on every dashboard render — slow and rate-limited."""
    from backend.core.duckdb import get_asn_names

    cached = {7922: "Comcast", 15169: "Google"}
    with (
        patch("backend.core.metadata.lookup_asn_names", return_value=dict(cached)),
        patch("cymruwhois.Client") as mock_cw,
    ):
        out = get_asn_names("svc-1", [7922, 15169])

    assert out == cached
    # No cymruwhois call
    mock_cw.assert_not_called()


def test_get_asn_names_fallback_to_AS_number_when_resolution_fails():
    """ASNs not in cache AND cymruwhois fails → fallback to
    `"AS{n}"` for each unresolved ASN. Pinned because the picker
    must render SOMETHING for every ASN — None values would crash
    the row template."""
    from backend.core.duckdb import get_asn_names

    with (
        patch("backend.core.metadata.lookup_asn_names", return_value={}),
        patch("cymruwhois.Client", side_effect=ImportError("not installed")),
    ):
        out = get_asn_names("svc-1", [7922, 15169])

    assert out == {7922: "AS7922", 15169: "AS15169"}


def test_get_asn_names_swallows_metadata_db_lookup_exception():
    """If `metadata_db.lookup_asn_names` raises (DB locked), proceed
    as if the cache is empty — still resolve via whois + fallback.
    Pinned because losing this would break the entire ASN picker
    on a single SQLite hiccup."""
    from backend.core.duckdb import get_asn_names

    with (
        patch("backend.core.metadata.lookup_asn_names", side_effect=RuntimeError("locked")),
        patch("cymruwhois.Client", side_effect=ImportError()),
    ):
        out = get_asn_names("svc-1", [7922])

    # Falls back to AS-form
    assert out == {7922: "AS7922"}


def test_enrich_asn_labels_mutates_list_in_place_with_labels():
    """`enrich_asn_labels` adds a 'label' key to each ASN-numeric
    dict in the input list (in-place). Pinned because callers do
    `values = enrich_asn_labels(values, sid)` and the contract is
    same-list-reference for downstream sort/filter ops."""
    from backend.core.duckdb import enrich_asn_labels

    values = [{"value": "7922"}, {"value": "15169"}, {"value": "non-numeric"}]

    with patch("backend.core.duckdb.get_asn_names", return_value={7922: "Comcast", 15169: "Google"}):
        out = enrich_asn_labels(values, "svc-1")

    # Same list reference
    assert out is values
    # Numeric ASNs got labels
    assert values[0]["label"] == "Comcast (7922)"
    assert values[1]["label"] == "Google (15169)"
    # Non-numeric was skipped (no label key)
    assert "label" not in values[2]


def test_enrich_asn_labels_skips_lookup_when_no_numeric_values():
    """When the list has no numeric values, skip the whois call
    entirely. Pinned because a country-picker (no ASN) shouldn't
    trigger ASN lookups."""
    from backend.core.duckdb import enrich_asn_labels

    values = [{"value": "US"}, {"value": "GB"}]

    with patch("backend.core.duckdb.get_asn_names") as mock_get:
        enrich_asn_labels(values, "svc-1")

    mock_get.assert_not_called()


def test_update_cron_duration_extracts_service_id_from_source_name():
    """`source["name"]` is the service_id for metadata_db. Pinned
    because the source dict has both `name` and `service_id` and
    using the wrong one would cross-contaminate cron runs across
    services."""
    from backend.core.duckdb import update_cron_duration

    captured = []

    with patch(
        "backend.core.metadata.update_cron_duration",
        side_effect=lambda sid, rid, dur, log_output=None: captured.append((sid, rid, dur, log_output)),
    ):
        update_cron_duration({"name": "svc-1", "service_id": "other-id"}, 42, 1.5)

    assert captured == [("svc-1", 42, 1.5, None)]


def test_update_cron_duration_no_op_when_no_service_id_resolvable():
    """Source without `name` or `service_id` → no-op (don't crash).
    Pinned because the global default source may not have one."""
    from backend.core.duckdb import update_cron_duration

    with patch("backend.core.metadata.update_cron_duration") as mock_update:
        update_cron_duration({}, 42, 1.0)

    mock_update.assert_not_called()


def test_log_usage_calls_no_op_when_usage_logging_disabled():
    """When usage_logging is globally disabled, skip the write.
    Pinned because customers can opt out — losing this would
    persist telemetry against their preference."""
    from backend.core.duckdb import log_usage_calls

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc-1"}, [{"method": "GET", "service": "FOS"}])

    mock_log.assert_not_called()


def test_log_usage_calls_no_op_when_no_service_id():
    """Source without service identifier → silent no-op. Pinned
    because pre-config dashboard renders pass empty sources."""
    from backend.core.duckdb import log_usage_calls

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({}, [{"method": "GET", "service": "FOS"}])

    mock_log.assert_not_called()


def test_log_usage_calls_propagates_process_context():
    """`process_context` kwarg flows through to metadata_db. Pinned
    because the usage-log UI filters by process_context — losing
    the propagation would lose the cron/api distinction."""
    from backend.core.duckdb import log_usage_calls

    captured = {}

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda sid, calls, process_context=None: captured.update(
                {"sid": sid, "calls": calls, "ctx": process_context}
            ),
        ),
    ):
        log_usage_calls(
            {"name": "svc-1"},
            [{"method": "GET", "service": "FOS"}],
            process_context="cron:sync",
        )

    assert captured["sid"] == "svc-1"
    assert captured["ctx"] == "cron:sync"


def test_purge_usage_log_reads_retention_from_global_config():
    """`retention_days` comes from the global usage_logging config.
    Pinned because admins control retention globally; losing this
    would purge with hard-coded 30d regardless of config."""
    from backend.core.duckdb import purge_usage_log

    captured = []

    with (
        patch("backend.config.load_usage_logging_config", return_value={"retention_days": 14}),
        patch(
            "backend.core.metadata.purge_usage_log",
            side_effect=lambda sid, days: captured.append((sid, days)),
        ),
    ):
        purge_usage_log({"name": "svc-1"})

    assert captured == [("svc-1", 14)]


def test_purge_usage_log_defaults_to_30_days_when_config_missing():
    """No retention setting → default 30 days. Pinned to lock the
    default — bumping it without bumping the global cron schedule
    would let logs accumulate indefinitely."""
    from backend.core.duckdb import purge_usage_log

    captured = []

    with (
        patch("backend.config.load_usage_logging_config", return_value={}),
        patch(
            "backend.core.metadata.purge_usage_log",
            side_effect=lambda sid, days: captured.append(days),
        ),
    ):
        purge_usage_log({"name": "svc-1"})

    assert captured == [30]


def test_get_sync_status_busts_view_cache_before_retrying_on_no_files_found():
    """When the first count(*) fails with "No files found" (the
    classic stale-view-cache symptom — a buffer parquet was deleted
    after a commit but the cached view SQL still references it), the
    retry path MUST call iceberg.clear_source_caches BEFORE
    update_iceberg_view. Otherwise update_iceberg_view's own lock-busy
    fallback will re-execute the same stale SQL and loop on the same
    error, spamming sync-status polls every few seconds until the
    ingest lock happens to be released during the next poll window."""
    from backend.core.duckdb import get_sync_status

    src = {
        "name": "stale-cache-svc",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
    }

    call_order: list[str] = []

    # con.execute returns sensible values for the table-existence check
    # and triggers the retry path on the first count(*).
    count_calls = {"n": 0}

    class _FakeStatsRow:
        def __init__(self, rows):
            self.rows = rows

        def fetchone(self):
            return self.rows

    class _FakeCon:
        def execute(self, sql, *args):
            if "information_schema.tables" in sql:
                return _FakeStatsRow((1,))
            if "count(*)" in sql:
                count_calls["n"] += 1
                if count_calls["n"] == 1:
                    raise Exception("IO Error: No files found that match the pattern cache/.../buffer/batch_x.parquet")
                # Successful second pass
                return _FakeStatsRow((42, "2026-05-19T00:00:00Z", "2026-05-19T05:00:00Z"))
            return _FakeStatsRow(None)

    def _fake_clear(_key, **_kwargs):
        # keep_snapshot_cache is passed as a kwarg from the retry path
        call_order.append("clear_source_caches")

    def _fake_update(_con, _src, lock_timeout=5.0):  # noqa: ARG001
        call_order.append("update_iceberg_view")

    _empty_summary = {
        "file_count": 0,
        "total_rows": 0,
        "total_bytes": 0,
        "count_with_bytes": 0,
        "last_ingested": None,
        "latest_file_name": None,
    }
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=_empty_summary),
        patch("backend.core.iceberg.clear_source_caches", side_effect=_fake_clear) as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view", side_effect=_fake_update) as mock_update,
    ):
        out = get_sync_status(_FakeCon(), src, skip_fos=True, force=True)

    # The exact ordering matters — clear must come BEFORE update.
    assert call_order == ["clear_source_caches", "update_iceberg_view"], (
        f"expected clear_source_caches → update_iceberg_view, got {call_order}"
    )
    mock_clear.assert_called_once_with("stale-cache-svc", keep_snapshot_cache=True)
    mock_update.assert_called_once()

    # The retry succeeded — fields from the second count(*) call surface
    assert out["local_rows"] == 42


def test_get_sync_status_prefers_ingested_count_when_view_returns_zero():
    """When the Iceberg view is the "WHERE false" empty fallback (created
    during a transient catalog-load failure), SELECT count(*) succeeds and
    returns 0 — even though metadata_db.ingested_files knows we have
    millions of ingested rows. The fix: local_rows = max(view_count,
    local_rows_ingested) so we never UNDER-report. Pinned because the
    'Total Logs: 0' header bug came back exactly through this path."""
    from backend.core.duckdb import get_sync_status

    src = {
        "name": "svc-with-empty-view",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
    }

    # Three ingested files, total row_count 1,272,371 (matches the prod
    # symptom that surfaced this bug).
    expected_ingested_rows = 400_000 + 400_000 + 472_371
    summary = {
        "file_count": 3,
        "total_rows": expected_ingested_rows,
        "total_bytes": 300_000,
        "count_with_bytes": 3,
        "last_ingested": "2026-05-19T14:02:00Z",
        "latest_file_name": "raw/2026-05-19/14/c.log.gz",
    }

    class _FakeRow:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

        def fetchall(self):
            return [self.row] if self.row else []

    class _FakeCon:
        def execute(self, sql, *args):
            if "information_schema.tables" in sql:
                return _FakeRow((1,))  # table exists
            if "count(*)" in sql:
                # The empty-view fallback returns (0, None, None) cleanly
                return _FakeRow((0, None, None))
            return _FakeRow(None)

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
    ):
        out = get_sync_status(_FakeCon(), src, skip_fos=True, force=True)

    assert out["local_rows"] == expected_ingested_rows, (
        f"local_rows must fall back to ingested count when view returns 0; "
        f"got {out['local_rows']} expected {expected_ingested_rows}"
    )
    # Timestamps stay None when view said 0 rows — better than showing
    # stale or absent extents.
    assert out["earliest_log_at"] is None
    assert out["latest_log_at"] is None


def test_get_sync_status_trusts_view_count_when_smaller_than_ingested():
    """When the Iceberg view returns a real (non-zero) count that's SMALLER
    than the sum of ingested_files.row_count, trust the view. row_count
    captures raw JSON parse counts BEFORE the WHERE timestamp IS NOT NULL
    filter at ingest.py:645 and BEFORE any time-range filter, plus Iceberg
    compaction rewrites/dedupes parquets without back-updating ingested_files
    — so the metadata sum consistently over-reports. The view is truth.

    Pinned because the old `max(view_rows, local_rows_ingested)` guard
    over-reported by 2.35x in prod (3,844,405 header vs 1,635,368 from
    SELECT count(*) FROM logs)."""
    from backend.core.duckdb import get_sync_status

    src = {
        "name": "svc-with-overcounted-ingest",
        "endpoint": "ep",
        "access_key_id": "k",
        "secret_access_key": "s",
        "bucket": "b",
    }

    # Summary sums to 3,845,401 — what the over-counting metadata reports.
    summary = {
        "file_count": 3,
        "total_rows": 1_500_000 + 1_500_000 + 845_401,
        "total_bytes": 300_000,
        "count_with_bytes": 3,
        "last_ingested": "2026-05-19T14:02:00Z",
        "latest_file_name": "raw/2026-05-19/14/c.log.gz",
    }

    class _FakeRow:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

        def fetchall(self):
            return [self.row] if self.row else []

    class _FakeCon:
        def execute(self, sql, *args):
            if "information_schema.tables" in sql:
                return _FakeRow((1,))
            if "count(*)" in sql:
                # Real view returning 1,635,368 rows — much less than the
                # 3,845,401 sum in ingested_files.
                return _FakeRow((1_635_368, "2026-05-15T17:30:00Z", "2026-05-25T22:04:29Z"))
            return _FakeRow(None)

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.config.get_status", return_value=None),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
    ):
        out = get_sync_status(_FakeCon(), src, skip_fos=True, force=True)

    assert out["local_rows"] == 1_635_368, (
        f"local_rows must trust the view (1,635,368) when it returns a real count, "
        f"not the over-counted ingested_files sum (3,845,401); got {out['local_rows']}"
    )
    # Timestamps come from the view when view_rows > 0
    assert out["earliest_log_at"] == "2026-05-15T17:30:00Z"
    assert out["latest_log_at"] == "2026-05-25T22:04:29Z"


def test_clear_source_caches_keep_snapshot_cache_wipes_view_cache():
    """The retry path in get_sync_status uses ``keep_snapshot_cache=True``
    to wipe the view-SQL cache (forcing regeneration of the SELECT)."""
    from backend.core import iceberg as _ice

    _ice._view_cache["svc"] = ("loc", set(), tuple(), "SELECT * FROM existing_view", 1.0, False)

    _ice.clear_source_caches("svc", keep_snapshot_cache=True)

    # View cache wiped (forces SELECT regeneration on next call)
    assert "svc" not in _ice._view_cache


def test_clear_source_caches_default_wipes_everything():
    """Default behavior (teardown use-case) wipes view caches AND the
    per-service lock."""
    from backend.core import iceberg as _ice

    _ice._view_cache["teardown-svc"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _ice._view_cache["teardown-svc::client_vitals"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)

    _ice.clear_source_caches("teardown-svc")  # no flag → full wipe

    assert "teardown-svc" not in _ice._view_cache
    assert "teardown-svc::client_vitals" not in _ice._view_cache


def test_refresh_config_status_opens_read_only_connection():
    """refresh_config_status runs from the per-service cron every minute,
    purely to update the cached status cache. Opening as a writer was the
    cause of long-tail contention with ingest's writer lock — the comment
    even said "read-only mode to avoid locking" but the code did the
    opposite. Pin the RO + skip_view_update so a future refactor doesn't
    regress us."""
    from backend.core.duckdb import refresh_config_status

    captured: dict = {}

    def _fake_get_connection(source, **kwargs):
        captured.update(kwargs)
        captured["source"] = source

        class _StubCon:
            def execute(self, *_a, **_k):
                class _R:
                    def fetchone(self_inner):
                        return None

                    def fetchall(self_inner):
                        return []

                return _R()

            def close(self):
                pass

        return _StubCon()

    with (
        patch("backend.config.load_config", return_value={"name": "svc", "bucket": "b", "service_id": "svc"}),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch("backend.config.update_status"),
        patch("backend.core.duckdb.get_connection", side_effect=_fake_get_connection),
        patch("backend.core.duckdb.get_sync_status", return_value={"ingested": 0}),
        patch("backend.core.duckdb.get_schema", return_value=[]),
    ):
        refresh_config_status("svc")

    assert captured.get("read_only") is True, (
        "refresh_config_status MUST open RO — was taking exclusive writer locks every minute"
    )
    assert captured.get("skip_view_update") is True, (
        "refresh_config_status MUST pass skip_view_update — CREATE OR REPLACE VIEW on RO "
        "would fail and the cached view is the wrong layer to refresh from a status cron"
    )


_ = MagicMock

_ = pytest


# ── Merged from tests/core/test_duckdb_utils.py ──
class TestFormatAsnLabel:
    def test_name_and_number(self):
        assert format_asn_label(7922, "Comcast Cable Communications") == "Comcast Cable Communications (7922)"

    def test_empty_name_returns_as_prefix(self):
        assert format_asn_label(1234, "") == "AS1234"

    def test_name_already_as_prefixed_returns_as_prefix(self):
        # "AS7922" is a bare ASN alias, not a human name — collapse it.
        assert format_asn_label(7922, "AS7922") == "AS7922"

    def test_name_with_special_chars(self):
        result = format_asn_label(1, "O'Reilly & Associates")
        assert result == "O'Reilly & Associates (1)"

    def test_large_asn(self):
        assert format_asn_label(4294967295, "Max ASN") == "Max ASN (4294967295)"

    def test_asn_zero(self):
        assert format_asn_label(0, "") == "AS0"


class TestSafeTableName:
    def test_simple_name(self):
        assert _safe_table_name("my_service") == "logs_my_service"

    def test_strips_special_chars(self):
        # The impl strips trailing underscores, so trailing ! becomes _ then is stripped.
        assert _safe_table_name("my-service!") == "logs_my_service"

    def test_lowercases(self):
        assert _safe_table_name("MyService") == "logs_myservice"

    def test_default_name(self):
        assert _safe_table_name("default") == "logs"

    def test_leading_trailing_underscores_stripped(self):
        result = _safe_table_name("_my_service_")
        assert not result[len("logs_") :].startswith("_")
        assert not result.endswith("_")

    def test_spaces_become_underscores(self):
        assert _safe_table_name("my service") == "logs_my_service"

    def test_dots_become_underscores(self):
        result = _safe_table_name("service.name.v2")
        assert result == "logs_service_name_v2"


class TestGetConnectionLockHandling:
    """``get_connection`` treats certain DuckDB error messages as transient
    locks and retries; everything else propagates immediately.

    Regression scope: the "different configuration" message (DuckDB raises
    it when a connection already exists with a different ``read_only``
    flag — e.g. a cron is writing while a dashboard request opens
    read-only) must be treated as a lock. Without that mapping, the
    dashboard request 500s instead of waiting for the cron to finish.
    """

    def test_classic_locked_error_retries_then_raises_db_busy(self):
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("database is locked"),
        ):
            with pytest.raises(DBBusyError, match=r"locked by another process.*path=/tmp/_test\.duckdb"):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_lock_timeout_logs_topology_context(self, caplog):
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("database is locked"),
        ):
            with caplog.at_level("ERROR", logger="backend.core.duckdb"):
                with pytest.raises(DBBusyError):
                    get_connection(
                        source={"logging_service_id": "svc-1", "duckdb_path": "/tmp/_test.duckdb"},
                        max_wait=0.1,
                        read_only=True,
                    )

        record = next(r for r in caplog.records if r.getMessage().startswith("duckdb_connection_lock_timeout"))
        assert record.args["service_id"] == "svc-1"
        assert record.args["db_path"] == "/tmp/_test.duckdb"
        assert record.args["read_only"] is True
        assert record.args["deployment_mode"] == "standard"

    def test_conflict_error_retries_then_raises_db_busy(self):
        """``"conflict"`` in the message is the original lock signal —
        pinned alongside the new "different configuration" case so the
        deny-list doesn't regress."""
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("write-write conflict on table"),
        ):
            with pytest.raises(DBBusyError):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_different_configuration_error_treated_as_transient_lock(self):
        """REGRESSION: DuckDB raises ``"Can't open a connection to same
        database file with a different configuration..."`` when an existing
        connection has a different ``read_only`` flag. Before the fix this
        message wasn't in the lock-detection deny-list, so it propagated
        immediately as a 500. Now it retries and converts to DBBusyError
        — the request layer maps DBBusyError → 503 → React Query keeps
        cached data instead of clearing the UI."""
        err_msg = (
            "Connection Error: Can't open a connection to same database file "
            "with a different configuration than existing connections"
        )
        with patch("backend.core.duckdb.duckdb.connect", side_effect=Exception(err_msg)):
            with pytest.raises(DBBusyError):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_unexpected_error_propagates_immediately(self):
        """Non-lock errors (perms, missing parent dir, OOM) must propagate
        immediately — pinned because retrying through these wastes max_wait
        seconds and the actual error message gets shadowed by DBBusyError."""
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("Permission denied: /var/duckdb"),
        ):
            with pytest.raises(Exception, match="Permission denied"):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)


def test_threads_setting_is_instance_global(tmp_path):
    db = str(tmp_path / "svc.duckdb")
    a = duckdb.connect(db)
    b = duckdb.connect(db)
    try:
        a.execute("SET threads = 1")
        b.execute("SET threads = 3")
        assert a.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        a.close()
        b.close()


def test_pool_fresh_build_never_sets_threads(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_CONN_THREADS", "1")
    pool = _Pool(service_key="test_no_threads", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mock_conn),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        pool.acquire(src={"name": "test_no_threads"}, max_wait=0.5)
    executed = [str(c.args[0]).lower() for c in mock_conn.execute.call_args_list if c.args]
    assert not any("threads" in s for s in executed), executed


def test_ensure_threads_only_sets_when_value_differs(tmp_path):
    con = duckdb.connect(str(tmp_path / "svc.duckdb"))
    try:
        con.execute("SET threads = 2")
        spy = MagicMock(wraps=con)
        duckdb_mod._ensure_threads(spy, 2)
        assert not any("SET threads" in str(c.args[0]) for c in spy.execute.call_args_list)
        duckdb_mod._ensure_threads(spy, 3)
        assert con.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        con.close()


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("/data/.duckdb/svc.duckdb", "/data/.duckdb/svc.rum.duckdb"),
        ("svc", "svc.rum.duckdb"),
        ("/a.duckdb.duckdb", "/a.duckdb.rum.duckdb"),
    ],
)
def test_rum_duckdb_path(given: str, expected: str) -> None:
    assert rum_duckdb_path(given) == expected


def test_rum_source_for_only_rewrites_suffix() -> None:
    out = rum_source_for({"name": "svc", "duckdb_path": "/data/.duckdb/svc.duckdb"})
    assert out["duckdb_path"] == "/data/.duckdb/svc.rum.duckdb"
    assert out["name"] == "svc::rum"


def test_rum_source_for_is_shallow_and_leaves_original_untouched() -> None:
    nested = {"k": "v"}
    source = {"name": "svc", "duckdb_path": "/d/svc.duckdb", "opts": nested}

    out = rum_source_for(source)

    assert source == {"name": "svc", "duckdb_path": "/d/svc.duckdb", "opts": nested}
    assert out is not source
    # Nested values are shared by design: callers only read them.
    assert out["opts"] is nested


# ── Merged from tests/core/test_duckdb_type_roundtrip.py ──
def test_catalog_uses_only_known_duckdb_types():
    """Sanity: ingest assumes a fixed set of types. New ones must be
    added to ``_TYPE_BOUNDARIES`` below before they'll be tested."""
    seen = Counter(f["duckdb_type"] for f in LOG_FIELD_CATALOG)
    unknown = set(seen) - set(_TYPE_BOUNDARIES)
    assert not unknown, f"new duckdb_type(s) in catalog need test coverage: {unknown}"


_TYPE_BOUNDARIES: dict[str, list] = {
    "TIMESTAMP": [
        "2026-05-15 12:00:00",
        "1970-01-01 00:00:00",
        "2099-12-31 23:59:59.999",
    ],
    "VARCHAR": [
        "",
        "ascii",
        "héllo wörld",  # latin-1 + diacritics
        "日本語",  # multi-byte CJK
        "with 'quote' and \"dquote\" and \\backslash",
        "x" * 5000,  # long string
    ],
    "BOOLEAN": [True, False],
    "UTINYINT": [0, 255, 1, 127],  # 0..255
    "USMALLINT": [0, 65535, 1, 32768],  # 0..65535
    "UINTEGER": [0, 4_294_967_295, 1, 2_147_483_648],  # 0..2^32-1
    "UBIGINT": [0, 18_446_744_073_709_551_615, 1, 9_223_372_036_854_775_808],  # 0..2^64-1
    "BIGINT": [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 0, -1],
    "FLOAT": [0.0, 3.14159, -1.5, 1e10, -1e10, 1e-10],
    "DOUBLE": [0.0, 3.141592653589793, -1.5e100, 1e300, -1e300],
}


@pytest.mark.parametrize("ddb_type", sorted(_TYPE_BOUNDARIES))
def test_type_round_trip(ddb_type):
    """For each declared catalog type, boundary values write and read back
    correctly. Catches sign-flip, truncation, NaN handling, unicode bugs."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE TABLE t (val {ddb_type})")
        for v in _TYPE_BOUNDARIES[ddb_type]:
            con.execute("INSERT INTO t VALUES (?)", (v,))

        rows = con.execute("SELECT val FROM t ORDER BY rowid").fetchall()
        got = [r[0] for r in rows]

        # For floats, allow tiny drift; for everything else require exact.
        if ddb_type in ("FLOAT", "DOUBLE"):
            assert len(got) == len(_TYPE_BOUNDARIES[ddb_type])
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                if expected == 0.0:
                    assert actual == 0.0
                else:
                    rel = abs(actual - expected) / abs(expected)
                    assert rel < 1e-5, f"{ddb_type} drift: {expected!r} → {actual!r} (rel={rel})"
        elif ddb_type == "TIMESTAMP":
            # DuckDB returns datetime objects; compare ISO strings
            for expected, actual in zip(_TYPE_BOUNDARIES[ddb_type], got):
                assert str(actual).startswith(str(expected)[:19]), (
                    f"timestamp lost precision: {expected!r} → {actual!r}"
                )
        else:
            assert got == _TYPE_BOUNDARIES[ddb_type]
    finally:
        con.close()


def test_unsigned_types_reject_negative_values():
    """Sanity: attempting to insert a negative value into an unsigned
    column must error, not silently wrap. If it ever silently wraps,
    we'd lose data integrity in custom-field ingest."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (val UINTEGER)")
        with pytest.raises(duckdb.Error):
            con.execute("INSERT INTO t VALUES (-1)")
    finally:
        con.close()


def test_varchar_preserves_full_unicode_byte_length():
    """A 4-byte UTF-8 codepoint must round-trip without truncation."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (val VARCHAR)")
        emoji = "🚀" * 100  # 4-byte codepoint repeated
        con.execute("INSERT INTO t VALUES (?)", (emoji,))
        # DuckDB: ``length`` counts codepoints, ``strlen`` counts UTF-8 bytes.
        got = con.execute("SELECT val, length(val), strlen(val) FROM t").fetchone()
        assert got[0] == emoji
        assert got[1] == 100  # 100 codepoints
        assert got[2] == 400  # 4 bytes each → 400 bytes total
    finally:
        con.close()


def test_double_preserves_more_precision_than_float():
    """``ottfb`` and ``ottlb`` use DOUBLE to preserve sub-microsecond
    timing. A regression to FLOAT would silently lose ~5 decimal places.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t (f FLOAT, d DOUBLE)")
        precise = 123456.789012345
        con.execute("INSERT INTO t VALUES (?, ?)", (precise, precise))
        f_val, d_val = con.execute("SELECT f, d FROM t").fetchone()
        # IEEE-754 single (FLOAT) has ~7 significant decimal digits; for a
        # value with 6 integer digits that's roughly 4-5 e-5 precision.
        # IEEE-754 double has ~15 digits → effectively exact here.
        assert abs(f_val - precise) > 1e-5, f"FLOAT should drift past 1e-5, drift={f_val - precise}"
        assert abs(d_val - precise) < 1e-9, f"DOUBLE should be exact-ish: drift={d_val - precise}"
    finally:
        con.close()


def test_timestamptz_with_explicit_offsets_round_trip():
    """``backend.core.ingest`` casts the ``timestamp`` field to TIMESTAMPTZ
    (not plain TIMESTAMP) and the ``end_time`` filter binds ``?::TIMESTAMPTZ``.
    The plain TIMESTAMP cases above don't validate that values with
    explicit timezone offsets survive the round-trip — pinned here so a
    DuckDB minor that changed TZ semantics would fail loudly.
    """
    con = duckdb.connect(":memory:")
    try:
        # Load ICU so TIMESTAMPTZ formatting is deterministic across hosts.
        con.execute("INSTALL icu; LOAD icu;")
        con.execute("SET TimeZone = 'UTC'")
        con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
        # ISO 8601 strings with explicit offsets — DuckDB casts each to
        # UTC internally; the round-trip must preserve the instant.
        values = [
            ("2026-05-15T12:00:00+00:00", "2026-05-15 12:00:00+00"),
            ("2026-05-15T12:00:00-07:00", "2026-05-15 19:00:00+00"),
            ("2026-05-15T17:30:00+05:30", "2026-05-15 12:00:00+00"),
        ]
        for raw, _ in values:
            con.execute("INSERT INTO t VALUES (?::TIMESTAMPTZ)", (raw,))

        # Sort by ts so the comparison order is deterministic.
        rows = con.execute(
            "SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') AS s, "
            "       (ts AT TIME ZONE 'UTC')::TIMESTAMP AS utc_ts "
            "FROM t ORDER BY rowid"
        ).fetchall()
        # Strings normalise to UTC; the three rows above all encode the
        # same UTC instant 2026-05-15 12:00:00 EXCEPT the second row
        # (which is 19:00:00 UTC). Cross-check on the UTC string.
        utc_strs = [r[0] for r in rows]
        assert utc_strs == ["2026-05-15 12:00:00", "2026-05-15 19:00:00", "2026-05-15 12:00:00"]
    finally:
        con.close()


def test_timestamptz_end_time_filter_pushdown_excludes_future_rows():
    """The ``end_time`` filter binds the cutoff as ``?::TIMESTAMPTZ``.
    Pin that ``ts <= ?::TIMESTAMPTZ`` correctly excludes a row whose
    timestamp is past the cutoff, even when row TZ != cutoff TZ.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL icu; LOAD icu;")
        con.execute("SET TimeZone = 'UTC'")
        con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
        # Row at 2026-05-15 11:59:59 UTC (under cutoff).
        con.execute("INSERT INTO t VALUES ('2026-05-15T11:59:59Z'::TIMESTAMPTZ)")
        # Row at 2026-05-15 12:00:01 UTC (over cutoff).
        con.execute("INSERT INTO t VALUES ('2026-05-15T12:00:01Z'::TIMESTAMPTZ)")
        # Cutoff supplied in a non-UTC offset to exercise the cast path
        # ingest actually uses (the API serialises whatever the analyst
        # picked in their tz).
        cutoff = "2026-05-15T05:00:00-07:00"  # = 12:00:00 UTC
        n = con.execute("SELECT COUNT(*) FROM t WHERE ts <= ?::TIMESTAMPTZ", (cutoff,)).fetchone()[0]
        assert n == 1, f"expected 1 row at/under cutoff, got {n}"
    finally:
        con.close()


def test_microsecond_precision_for_elapsed_ottfb_round_trip_via_parquet():
    """Pin that microsecond-scale values for elapsed / ottfb / ottlb
    survive a parquet write + read cycle without truncation. Catches
    Arrow / Parquet codec drift that would silently lose precision on
    timing fields (e.g. p99 latency reported as 63 ms instead of 63 µs).
    """
    import os
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Realistic microsecond-scale values: 50ms request, 1.234s request,
    # 63s long-poll (prod p99 per memory note), and one large UBIGINT
    # that DOUBLE could not represent precisely.
    elapsed_us = [50_000, 1_234_567, 63_000_000, 9_007_199_254_740_993]
    # ottfb/ottlb are also UBIGINT per the catalog. Use sub-ms values
    # that would round if accidentally cast to ms.
    ottfb_us = [128, 4_287, 12_345_678, 1_000_001]

    table = pa.table(
        {
            "elapsed": pa.array(elapsed_us, type=pa.uint64()),
            "ottfb": pa.array(ottfb_us, type=pa.uint64()),
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "timing.parquet")
        pq.write_table(table, path, compression="zstd")

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(f"SELECT elapsed, ottfb FROM read_parquet('{path}') ORDER BY elapsed").fetchall()
        finally:
            con.close()

    # Sort the expected list by elapsed to match the SQL ORDER BY.
    expected = sorted(zip(elapsed_us, ottfb_us))
    assert rows == expected, f"microsecond values drifted across parquet RT: {rows} != {expected}"


# ── Merged from tests/core/test_duckdb_concurrency.py ──
def _src(db_path: str) -> dict:
    """Minimal source dict the connection setup will accept.

    ``_configure_fos`` runs unconditionally and reads several keys; supply
    empty strings to match the default-source pattern in
    [_build_default_source](../../backend/core/duckdb.py#L93).
    """
    return {
        "name": "test",
        "duckdb_path": db_path,
        "endpoint": "",
        "access_key_id": "",
        "secret_access_key": "",
        "region": "us-east-1",
        "bucket": "",
        "prefix": "",
        "cdn_url": "",
        "cdn_secret": "",
    }


def _make_failing_connect(fail_count: int, real_connect, target_path: str):
    """Return a duckdb.connect stand-in that fails ``fail_count`` times
    with a lock error, then delegates to the real connect.
    """
    state = {"calls": 0}

    def _fake(db_path, read_only=False):
        if db_path != target_path:
            return real_connect(db_path, read_only=read_only)
        state["calls"] += 1
        if state["calls"] <= fail_count:
            # The substring must match one of the patterns get_connection
            # treats as a lock error: "conflict", "locked", or
            # "different configuration". Use "locked" — most representative.
            raise duckdb.Error("Could not set lock on file: database is locked")
        return real_connect(db_path, read_only=read_only)

    return _fake, state


def test_succeeds_after_transient_lock_errors(tmp_path, monkeypatch):
    """Three lock failures, then success."""
    db_path = str(tmp_path / "retry.duckdb")
    # Pre-create the file so the `if not os.path.exists` branch in get_connection doesn't run and confuse the call counts
    duckdb.connect(db_path).close()

    real_connect = duckdb.connect
    fake, state = _make_failing_connect(fail_count=3, real_connect=real_connect, target_path=db_path)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    # Reproduce an earlier test's background work while these patches are live.
    def unrelated_work():
        duckdb.connect(str(tmp_path / "unrelated.duckdb")).close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(unrelated_work).result()
    assert state["calls"] == 0

    src = _src(db_path)
    con = get_connection(src, max_wait=5.0, read_only=False)
    try:
        assert con.execute("SELECT 1").fetchone() == (1,)
    finally:
        con.close()

    assert state["calls"] == 4, "should have retried 3 times then succeeded on the 4th call"


def test_uses_exponential_backoff(tmp_path, monkeypatch):
    """Sleep durations should roughly double up to the cap. We capture the
    sleep arguments rather than measuring wall time so the test isn't flaky
    on slow CI."""
    db_path = str(tmp_path / "backoff.duckdb")
    duckdb.connect(db_path).close()

    real_connect = duckdb.connect
    fake, _ = _make_failing_connect(fail_count=5, real_connect=real_connect, target_path=db_path)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    sleeps: list[float] = []
    real_sleep = time.sleep

    def _capture_sleep(seconds):
        if threading.current_thread() is threading.main_thread():
            sleeps.append(seconds)
            real_sleep(0)
        else:
            real_sleep(seconds)

    monkeypatch.setattr(duckdb_mod.time, "sleep", _capture_sleep)

    # Trigger the False branch of _capture_sleep from a background thread
    bg_sleeps = []

    def bg_thread():
        duckdb_mod.time.sleep(1.23)
        bg_sleeps.append(1.23)

    t = threading.Thread(target=bg_thread)
    t.start()
    t.join()

    src = _src(db_path)
    con = get_connection(src, max_wait=30.0, read_only=False)
    con.close()

    # Five retries → five sleeps.
    assert len(sleeps) == 5
    assert bg_sleeps == [1.23]
    # 50ms, 100ms, 200ms, 400ms, then capped at 500ms.
    # Allow tiny floating-point fuzz.
    expected = [0.05, 0.10, 0.20, 0.40, 0.50]
    for i, (got, want) in enumerate(zip(sleeps, expected)):
        assert abs(got - want) < 1e-6, f"sleep #{i + 1}: expected {want}s, got {got}s. Full sequence: {sleeps}"


def test_deadline_exceeded_raises_dbbusyerror(tmp_path, monkeypatch):
    """When the deadline is reached, the caller sees DBBusyError — not a
    raw DuckDB exception. This is the contract dashboards rely on to
    translate to a clean 'system busy, retry' response."""
    db_path = str(tmp_path / "busy.duckdb")

    real_connect = duckdb.connect
    calls = {"n": 0}

    def _always_locked(path, read_only=False):
        if path != db_path:
            return real_connect(path, read_only=read_only)
        calls["n"] += 1
        raise duckdb.Error("Could not set lock on file: database is locked")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _always_locked)

    # Patch sleep to a no-op so the test runs fast — the deadline still
    # advances via time.monotonic().
    real_sleep = time.sleep
    owner = threading.current_thread()
    monkeypatch.setattr(
        duckdb_mod.time, "sleep", lambda s: None if threading.current_thread() is owner else real_sleep(s)
    )

    src = _src(db_path)
    with pytest.raises(DBBusyError) as excinfo:
        get_connection(src, max_wait=0.2)

    assert "locked by another process" in str(excinfo.value)
    # __cause__ preserves the original DuckDB error for forensic debugging.
    assert excinfo.value.__cause__ is not None
    assert calls["n"] > 1


def test_non_lock_error_is_not_retried(tmp_path, monkeypatch):
    """Random errors must propagate immediately — retry would mask bugs."""
    db_path = str(tmp_path / "boom.duckdb")
    calls = {"n": 0}
    real_connect = duckdb.connect

    def _boom(path, read_only=False):
        if path != db_path:
            return real_connect(path, read_only=read_only)
        calls["n"] += 1
        raise duckdb.Error("Catastrophic failure: something unrelated to locking")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _boom)

    src = _src(db_path)
    with pytest.raises(duckdb.Error, match="Catastrophic failure"):
        get_connection(src, max_wait=5.0, read_only=False)

    assert calls["n"] == 1, "non-lock errors must not be retried"


def test_writer_then_reader_release_path(tmp_path):
    """Sanity: open a writer, close it, then a reader succeeds without
    retries. Ensures the retry path is contention-only — no false positives
    in the no-contention case."""
    db_path = str(tmp_path / "release.duckdb")

    boot = duckdb.connect(db_path)
    boot.execute("CREATE TABLE t(x INTEGER)")
    boot.close()

    src = _src(db_path)

    # Open and immediately close a writer.
    w = get_connection(src, read_only=False)
    w.close()

    # Reader should sail through.
    r = get_connection(src, read_only=True)
    try:
        r.execute("SELECT count(*) FROM t").fetchone()
    finally:
        r.close()


# ── Merged from tests/core/test_duckdb_recycle.py ──
def test_live_set_hits_zero_with_real_conns(tmp_path):
    """Real DuckDB conns registered in the liveness set drop to zero once closed
    and GC'd — the authoritative signal the recycle waits on."""
    dbfile = str(tmp_path / "real.duckdb")
    path = os.path.abspath(dbfile)
    assert d.live_connection_count(path) == 0

    conns = [duckdb.connect(dbfile) for _ in range(3)]
    for c in conns:
        d._register_live_connection(c, path)
    assert d.live_connection_count(path) == 3

    for c in conns:
        c.close()
    conns.clear()
    c = None  # drop the lingering loop-variable ref to the last conn
    gc.collect()
    assert d.live_connection_count(path) == 0


def test_recycle_once_no_services(monkeypatch):
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: [])
    assert rec.recycle_once() == "no services configured"


def test_sources_grouped_by_db_path(monkeypatch):
    """Two services sharing a duckdb_path land in one group (one instance)."""
    cfgs = [
        {"name": "svc1", "duckdb_path": "/tmp/shared.duckdb"},
        {"name": "svc2", "duckdb_path": "/tmp/shared.duckdb"},
    ]
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: cfgs)
    monkeypatch.setattr(rec._db, "_source_from_config", lambda c: c)

    groups = rec._sources_by_db_path()
    assert len(groups) == 1
    ((only_path, sources),) = groups.items()
    assert only_path == os.path.abspath("/tmp/shared.duckdb")
    assert {s["name"] for s in sources} == {"svc1", "svc2"}


def test_recycle_db_path_success_releases_barrier_and_lock(tmp_path):
    """With no pools and no live conns, a recycle reports 'recycled' and always
    leaves the barrier cleared and the service lock released."""
    path = os.path.abspath(str(tmp_path / "ok.duckdb"))
    src = {"name": "rec_ok_svc", "duckdb_path": path}

    result = rec._recycle_db_path(path, [src])

    assert result["status"] == "recycled"
    assert result["live_after"] == 0
    # Barrier cleared.
    assert path not in d._recycle_barrier_active
    # Lock fully released (acquirable again right away).
    lock = _get_service_lock("rec_ok_svc")
    assert lock.acquire(blocking=False) is True
    lock.release()


def test_recycle_db_path_skips_when_write_lock_busy(tmp_path, monkeypatch):
    """If a writer holds the service lock, recycle aborts cleanly (no barrier)."""
    monkeypatch.setenv("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "100")
    path = os.path.abspath(str(tmp_path / "busy.duckdb"))
    src = {"name": "rec_busy_svc", "duckdb_path": path}
    lock = _get_service_lock("rec_busy_svc")

    held = threading.Event()
    release = threading.Event()

    def holder():
        lock.acquire()
        held.set()
        release.wait(timeout=5.0)
        lock.release()

    th = threading.Thread(target=holder)
    th.start()
    held.wait(timeout=2.0)
    try:
        result = rec._recycle_db_path(path, [src])
        assert result["status"] == "skipped_locked"
        # Recycle never raised the barrier because it bailed before acquiring.
        assert path not in d._recycle_barrier_active
    finally:
        release.set()
        th.join(timeout=2.0)


def test_barrier_no_block_when_clear():
    """_barrier_wait returns immediately when no barrier is active."""
    t0 = time.monotonic()
    d._barrier_wait("/tmp/no-barrier.duckdb")
    assert time.monotonic() - t0 < 0.1


def test_barrier_blocks_then_fails_open(monkeypatch):
    """With a barrier set and never cleared, _barrier_wait returns after the cap
    (fail-open) — it must never block a connection open indefinitely."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "150")
    path = "/tmp/failopen.duckdb"
    d.set_recycle_barrier(path, True)
    try:
        t0 = time.monotonic()
        d._barrier_wait(path)
        elapsed = time.monotonic() - t0
        # Returned by fail-open, ~cap, even though the barrier is still set.
        assert 0.1 <= elapsed < 0.6
        assert path in d._recycle_barrier_active
    finally:
        d.set_recycle_barrier(path, False)


def test_barrier_cleared_unblocks_immediately(monkeypatch):
    """Clearing the barrier wakes a parked waiter well before the cap."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "5000")
    path = "/tmp/cleared.duckdb"
    d.set_recycle_barrier(path, True)
    elapsed = {}

    def waiter():
        t0 = time.monotonic()
        d._barrier_wait(path)
        elapsed["t"] = time.monotonic() - t0

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.1)
    d.set_recycle_barrier(path, False)  # clear → should wake the waiter now
    th.join(timeout=2.0)
    assert not th.is_alive()
    assert elapsed["t"] < 1.0  # nowhere near the 5s cap


def test_barrier_per_db_path_isolation(monkeypatch):
    """A barrier on one db file must not block opens against a different file."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "5000")
    a, b = "/tmp/path_a.duckdb", "/tmp/path_b.duckdb"
    d.set_recycle_barrier(a, True)
    try:
        t0 = time.monotonic()
        d._barrier_wait(b)  # different path — must not block
        assert time.monotonic() - t0 < 0.2
    finally:
        d.set_recycle_barrier(a, False)


def test_live_connection_set_tracks_and_drops():
    """Registered conns count as live; once unreferenced + GC'd they drop out."""
    path = "/tmp/liveset.duckdb"
    before = d.live_connection_count(path)
    conns = [MagicMock() for _ in range(3)]
    for c in conns:
        d._register_live_connection(c, path)
    assert d.live_connection_count(path) == before + 3
    conns.clear()
    c = None  # drop the lingering loop-variable ref to the last conn
    gc.collect()
    assert d.live_connection_count(path) == before


def test_current_rss_bytes_returns_positive():
    rss = d.current_rss_bytes()
    assert rss is None or rss > 0
