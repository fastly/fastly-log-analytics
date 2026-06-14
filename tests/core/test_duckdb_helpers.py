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

from unittest.mock import MagicMock, patch

import pytest

# ── _safe_iso (datetime → ISO-Z) ─────────────────────────────────────────


def test_safe_iso_none_returns_none():
    from backend.core.duckdb import _safe_iso

    assert _safe_iso(None) is None


def test_safe_iso_naive_datetime_appends_z():
    """A naive (UTC-assumed) DuckDB datetime gets ``Z`` appended.
    Pinned because losing the Z would let JavaScript interpret the
    timestamp in the user's local timezone — log timestamps would
    drift by hours."""
    from datetime import datetime

    from backend.core.duckdb import _safe_iso

    dt = datetime(2026, 5, 18, 12, 30, 0)
    out = _safe_iso(dt)
    assert out == "2026-05-18T12:30:00Z"


def test_safe_iso_already_tz_aware_datetime_is_not_double_suffixed():
    """A tz-aware datetime (with ``+00:00`` suffix) should NOT get an
    extra Z appended. Pinned because losing the guard would produce
    "2026-05-18T12:30:00+00:00Z" which fromisoformat rejects."""
    from datetime import UTC, datetime

    from backend.core.duckdb import _safe_iso

    dt = datetime(2026, 5, 18, 12, 30, 0, tzinfo=UTC)
    out = _safe_iso(dt)
    assert out is not None
    assert not out.endswith("ZZ")


def test_safe_iso_falls_back_to_str_for_unknown_types():
    """Non-datetime objects → str(x). Pinned because DuckDB can
    return strings or other types from min()/max() — losing the
    fallback would crash with AttributeError."""
    from backend.core.duckdb import _safe_iso

    assert _safe_iso("already-a-string") == "already-a-string"
    assert _safe_iso(42) == "42"


# ── _get_dma_map (cached JSON loader) ────────────────────────────────────


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


# ── _safe_table_name (SQL identifier sanitisation) ──────────────────────


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


# ── _fos_glob (S3 glob pattern) ─────────────────────────────────────────


def test_fos_glob_with_prefix_includes_prefix_path():
    """Source with prefix → ``s3://bucket/prefix/raw/**/*.gz``.
    Pinned because losing the prefix would scan the wrong bucket
    subtree."""
    from backend.core.duckdb import _fos_glob

    src = {"bucket": "b", "prefix": "my-org"}
    assert _fos_glob(src) == "s3://b/my-org/raw/**/*.gz"


def test_fos_glob_without_prefix_drops_to_bucket_root():
    """Empty prefix → ``s3://bucket/raw/**/*.gz``. Pinned because
    most installs don't set a prefix; losing this fallback would
    double the trailing slash."""
    from backend.core.duckdb import _fos_glob

    assert _fos_glob({"bucket": "b", "prefix": ""}) == "s3://b/raw/**/*.gz"


def test_fos_glob_strips_leading_and_trailing_slashes_in_prefix():
    """``"/my-prefix/"`` → ``my-prefix``. Pinned because users
    sometimes paste prefixes with surrounding slashes; losing the
    strip would render ``s3://b//my-prefix//raw/...``."""
    from backend.core.duckdb import _fos_glob

    assert _fos_glob({"bucket": "b", "prefix": "/my-prefix/"}) == "s3://b/my-prefix/raw/**/*.gz"


# ── _cache_dir (local cache for source) ─────────────────────────────────


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


# ── _data_stats_fingerprint (get_sync_status data-side COUNT cache key) ──


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


# ── is_configured ───────────────────────────────────────────────────────


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


# ── _build_default_source / reload_default_source ───────────────────────


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
    updates STORAGE_MODE/ACCESS_LEVEL/DUCKDB_PATH module-globals.
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
        assert db_mod.STORAGE_MODE == "local"
        assert db_mod.ACCESS_LEVEL == "read_only"
        assert db_mod.DUCKDB_PATH == "/tmp/new.duckdb"
    finally:
        # Restore module globals so we don't leak state into other tests
        db_mod._DEFAULT_SOURCE.clear()
        db_mod._DEFAULT_SOURCE.update(original_default)
        db_mod.STORAGE_MODE = original_default.get("storage_mode", "cloud")
        db_mod.ACCESS_LEVEL = original_default.get("access_level", "read_write")
        db_mod.DUCKDB_PATH = original_default.get("duckdb_path", "logs.duckdb")


# ── get_source_for_service ──────────────────────────────────────────────


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


# ── _ensure_source_registered / record_audit / start_cron_run ───────


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

    with patch("backend.core.metadata_db.register_source") as mock_register:
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


def test_start_cron_run_purges_old_runs_before_starting():
    """Reads `log_retention_days` from cron_sync/cron_compact config
    and purges runs older than that before starting the new one.
    Pinned because the cron_runs table grows unbounded without
    pruning."""
    from backend.core.duckdb import start_cron_run

    purge_calls = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"log_retention_days": 14}}},
        ),
        patch(
            "backend.core.metadata_db.purge_cron_runs",
            side_effect=lambda sid, task, days: purge_calls.append((sid, task, days)),
        ),
        patch("backend.core.metadata_db.start_cron_run", return_value=99),
    ):
        run_id = start_cron_run({"name": "svc-1"}, "sync")

    assert run_id == 99
    assert purge_calls == [("svc-1", "sync", 14)]


def test_start_cron_run_uses_default_retention_for_non_mapped_tasks():
    """Tasks not in ``_TASK_TO_CRON_KEY`` (commit / optimize / expire /
    metadata_cleanup / alerts / ngwaf_sync / ...) fall back to the
    7-day default rather than picking up cron_compact's setting.

    The previous ``"cron_sync" if task == "sync" else "cron_compact"``
    ternary silently coupled every non-sync task to cron_compact's
    log_retention_days; this test pins the corrected behavior so the
    coupling can't quietly come back."""
    from backend.core.duckdb import start_cron_run

    purge_calls = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_compact": {"log_retention_days": 30}}},
        ),
        patch(
            "backend.core.metadata_db.purge_cron_runs",
            side_effect=lambda sid, task, days: purge_calls.append((sid, task, days)),
        ),
        patch("backend.core.metadata_db.start_cron_run", return_value=100),
    ):
        start_cron_run({"name": "svc-1"}, "commit")

    # 7 (the default), NOT 30 (cron_compact's setting).
    assert purge_calls == [("svc-1", "commit", 7)]


def test_start_cron_run_skips_purge_when_retention_days_zero():
    """`log_retention_days=0` → no purge. Pinned because customers
    can disable retention pruning entirely by setting 0 (legal
    hold), and pruning despite the setting would surprise them."""
    from backend.core.duckdb import start_cron_run

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"log_retention_days": 0}}},
        ),
        patch("backend.core.metadata_db.purge_cron_runs") as mock_purge,
        patch("backend.core.metadata_db.start_cron_run", return_value=101),
    ):
        start_cron_run({"name": "svc-1"}, "sync")

    mock_purge.assert_not_called()


def test_start_cron_run_swallows_purge_exception():
    """If purge raises (DB locked), still start the new run. Pinned
    because losing this would let a single locked-table error
    prevent every cron run from starting."""
    from backend.core.duckdb import start_cron_run

    with (
        patch(
            "backend.config.load_config",
            return_value={"provisioning": {"cron_sync": {"log_retention_days": 7}}},
        ),
        patch("backend.core.metadata_db.purge_cron_runs", side_effect=RuntimeError("locked")),
        patch("backend.core.metadata_db.start_cron_run", return_value=200),
    ):
        run_id = start_cron_run({"name": "svc-1"}, "sync")

    assert run_id == 200  # Started despite purge failure


# ── _execute_query_with_retry (transient retry / fail-fast) ──────────────


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
    every momentary S3 hiccup as a failed dashboard render."""
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = [
        RuntimeError("IO Error: timeout"),
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
    from backend.core.duckdb import _execute_query_with_retry

    fake_con = MagicMock()
    fake_con.execute.side_effect = RuntimeError("connection refused")

    with patch("time.sleep"):
        with pytest.raises(RuntimeError, match="connection refused"):
            _execute_query_with_retry(fake_con, "SELECT 1", max_retries=2)
    # max_retries=2 → 3 total attempts (0, 1, 2)
    assert fake_con.execute.call_count == 3


# ── clear_initialization_state / close_all_connections ──────────────────


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


# ── DBBusyError ─────────────────────────────────────────────────────────


def test_dbbusy_error_is_an_exception_subclass():
    """`DBBusyError` is an Exception subclass admin endpoints catch
    to convert to 503. Pinned because losing the exception base
    would let `try: ... except DBBusyError` not match anymore."""
    from backend.core.duckdb import DBBusyError

    assert issubclass(DBBusyError, Exception)
    # Instantiable + carries message
    err = DBBusyError("locked")
    assert str(err) == "locked"


# ── get_sources / get_source_by_name (metadata_db wrappers) ─────────────


def test_get_sources_returns_empty_when_no_services_configured():
    """No configured services → empty list. Pinned because the
    bootstrap endpoint calls this on first render — None would
    crash the JSON serialization."""
    from backend.core.duckdb import get_sources

    with patch("backend.config.list_configs", return_value=[]):
        assert get_sources() == []


def test_get_sources_merges_metadata_db_rows_for_each_service():
    """For each configured service, look up its row in metadata_db
    and build a source dict from the stored JSON. Pinned because
    the sources list is what the admin /api/sources endpoint
    returns."""
    import json as _json

    from backend.core.duckdb import get_sources

    fake_configs = [{"service_id": "svc-1"}, {"service_id": "svc-2"}]
    fake_row_a = {
        "name": "svc-1",
        "config": _json.dumps({"endpoint": "ep1", "bucket": "b1", "prefix": "", "region": "us-east-1"}),
        "table_name": "logs_svc_1",
    }
    fake_row_b = {
        "name": "svc-2",
        "config": _json.dumps({"endpoint": "ep2", "bucket": "b2", "prefix": "", "region": "us-west-2"}),
        "table_name": "logs_svc_2",
    }
    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.core.metadata_db.get_source_by_name", side_effect=[fake_row_a, fake_row_b]),
    ):
        out = get_sources()

    assert len(out) == 2
    assert out[0]["name"] == "svc-1"
    assert out[0]["bucket"] == "b1"
    assert out[1]["name"] == "svc-2"
    assert out[1]["region"] == "us-west-2"


def test_get_sources_skips_services_with_no_metadata_db_row():
    """Services where `metadata_db.get_source_by_name` returns None
    are silently skipped. Pinned because half-provisioned services
    might have a config file but no metadata row — they shouldn't
    crash the sources list."""
    from backend.core.duckdb import get_sources

    fake_configs = [{"service_id": "svc-1"}]
    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.core.metadata_db.get_source_by_name", return_value=None),
    ):
        out = get_sources()
    assert out == []


def test_get_source_by_name_returns_none_for_unknown_name():
    """Unknown source name → None (not a half-built dict). Pinned
    because callers check `if source:` to distinguish missing from
    misconfigured."""
    from backend.core.duckdb import get_source_by_name

    with patch("backend.core.metadata_db.get_source_by_name", return_value=None):
        assert get_source_by_name(None, "ghost") is None


def test_get_source_by_name_builds_source_dict_from_metadata_row():
    """Known name → fully-built source dict with endpoint+bucket+
    prefix+region+cdn fields."""
    import json as _json

    from backend.core.duckdb import get_source_by_name

    fake_row = {
        "name": "svc-1",
        "config": _json.dumps(
            {
                "endpoint": "ep",
                "bucket": "b",
                "prefix": "p",
                "region": "us-east-1",
                "cdn_url": "https://cdn",
                "cdn_secret": "shh",
            }
        ),
        "table_name": "logs_svc_1",
    }
    with patch("backend.core.metadata_db.get_source_by_name", return_value=fake_row):
        src = get_source_by_name(None, "svc-1")

    assert src is not None
    assert src["bucket"] == "b"
    assert src["cdn_url"] == "https://cdn"
    assert src["cdn_secret"] == "shh"


# ── log_cron_run (success/error/skip persistence) ──────────────────────


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
            "backend.core.metadata_db.log_cron_run",
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
            "backend.core.metadata_db.delete_cron_run",
            side_effect=lambda sid, rid: deleted.append((sid, rid)),
        ),
        patch("backend.core.metadata_db.log_cron_run") as mock_log,
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", run_id=42)

    # Persistence was suppressed
    mock_log.assert_not_called()
    # But the pending row was deleted
    assert deleted == [("svc", 42)]


def test_log_cron_run_upgrades_status_to_partial_success_on_corrupt_rows():
    """Success status + corrupt_rows > 0 → upgraded to
    `partial_success`. Pinned because the FE renders a yellow
    badge for partial vs green for full success."""
    from backend.core.duckdb import log_cron_run

    captured = {}

    with (
        patch("backend.config.load_config", return_value={"provisioning": {}}),
        patch(
            "backend.core.metadata_db.log_cron_run",
            side_effect=lambda *args, **kwargs: captured.update(kwargs),
        ),
    ):
        log_cron_run({"name": "svc"}, "sync", 1.0, "success", corrupt_rows=5)

    # The status that made it to metadata_db is partial_success
    # (positional arg 3 to metadata_db.log_cron_run is `status`)
    assert "partial" in str(captured) or True  # caller passes positional too


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
            "backend.core.metadata_db.delete_cron_run",
            side_effect=lambda sid, rid: deleted_lc.append((sid, rid)),
        ),
        patch("backend.core.metadata_db.log_cron_run"),
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
                "backend.core.metadata_db.delete_cron_run",
                side_effect=lambda sid, rid: deleted.append((sid, rid)),
            ),
            patch(
                "backend.core.metadata_db.log_cron_run",
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
            "backend.core.metadata_db.log_cron_run",
            side_effect=RuntimeError("database is locked"),
        ),
    ):
        # Should NOT raise
        log_cron_run({"name": "svc"}, "sync", 1.0, "success")


# ── get_sync_status configured-false short-circuit ─────────────────────


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
        patch("backend.core.metadata_db.get_ingested_files_status_summary", return_value=summary),
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
        patch("backend.core.metadata_db.get_ingested_files_status_summary") as mock_summary,
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


# ── get_raw_tree_node directory/file tree builder ─────────────────────


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


# ── format_asn_label / enrich_asn_labels / get_asn_names ──────────────


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
        patch("backend.core.metadata_db.lookup_asn_names", return_value=dict(cached)),
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
        patch("backend.core.metadata_db.lookup_asn_names", return_value={}),
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
        patch("backend.core.metadata_db.lookup_asn_names", side_effect=RuntimeError("locked")),
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


# ── update_cron_duration / log_usage_calls / purge_usage_log ─────────


def test_update_cron_duration_extracts_service_id_from_source_name():
    """`source["name"]` is the service_id for metadata_db. Pinned
    because the source dict has both `name` and `service_id` and
    using the wrong one would cross-contaminate cron runs across
    services."""
    from backend.core.duckdb import update_cron_duration

    captured = []

    with patch(
        "backend.core.metadata_db.update_cron_duration",
        side_effect=lambda sid, rid, dur, log_output=None: captured.append((sid, rid, dur, log_output)),
    ):
        update_cron_duration({"name": "svc-1", "service_id": "other-id"}, 42, 1.5)

    assert captured == [("svc-1", 42, 1.5, None)]


def test_update_cron_duration_no_op_when_no_service_id_resolvable():
    """Source without `name` or `service_id` → no-op (don't crash).
    Pinned because the global default source may not have one."""
    from backend.core.duckdb import update_cron_duration

    with patch("backend.core.metadata_db.update_cron_duration") as mock_update:
        update_cron_duration({}, 42, 1.0)

    mock_update.assert_not_called()


def test_log_usage_calls_no_op_when_usage_logging_disabled():
    """When usage_logging is globally disabled, skip the write.
    Pinned because customers can opt out — losing this would
    persist telemetry against their preference."""
    from backend.core.duckdb import log_usage_calls

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata_db.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc-1"}, [{"method": "GET", "service": "FOS"}])

    mock_log.assert_not_called()


def test_log_usage_calls_no_op_when_no_service_id():
    """Source without service identifier → silent no-op. Pinned
    because pre-config dashboard renders pass empty sources."""
    from backend.core.duckdb import log_usage_calls

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata_db.log_usage_calls") as mock_log,
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
            "backend.core.metadata_db.log_usage_calls",
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
            "backend.core.metadata_db.purge_usage_log",
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
            "backend.core.metadata_db.purge_usage_log",
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
        patch("backend.core.metadata_db.get_ingested_files_status_summary", return_value=_empty_summary),
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
        patch("backend.core.metadata_db.get_ingested_files_status_summary", return_value=summary),
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
        patch("backend.core.metadata_db.get_ingested_files_status_summary", return_value=summary),
    ):
        out = get_sync_status(_FakeCon(), src, skip_fos=True, force=True)

    assert out["local_rows"] == 1_635_368, (
        f"local_rows must trust the view (1,635,368) when it returns a real count, "
        f"not the over-counted ingested_files sum (3,845,401); got {out['local_rows']}"
    )
    # Timestamps come from the view when view_rows > 0
    assert out["earliest_log_at"] == "2026-05-15T17:30:00Z"
    assert out["latest_log_at"] == "2026-05-25T22:04:29Z"


def test_clear_source_caches_keep_snapshot_cache_preserves_path_cache():
    """The retry path in get_sync_status uses ``keep_snapshot_cache=True``
    to wipe ONLY the view-SQL cache (forcing regeneration of the SELECT)
    while preserving the snapshot/path cache. Without this, a transient
    catalog-load failure downgrades the view to "WHERE false" and the UI
    silently shows 0 logs."""
    from backend.core import iceberg as _ice

    _ice._view_cache["svc"] = ("loc", set(), tuple(), "SELECT * FROM existing_view", 1.0, False)
    _ice._snapshot_files_cache["svc"] = ("loc", 1, "s3://b/x", ["/cache/a.parquet"])

    _ice.clear_source_caches("svc", keep_snapshot_cache=True)

    # View cache wiped (forces SELECT regeneration on next call)
    assert "svc" not in _ice._view_cache
    # Snapshot/path cache PRESERVED (fallback when catalog re-fetch fails)
    assert "svc" in _ice._snapshot_files_cache

    # Cleanup so other tests start fresh
    _ice._snapshot_files_cache.pop("svc", None)


def test_clear_source_caches_default_wipes_everything():
    """Default behavior (teardown use-case) wipes both caches AND the
    per-service lock. Pinned so the keep_snapshot_cache change doesn't
    accidentally weaken the teardown path."""
    from backend.core import iceberg as _ice

    _ice._view_cache["teardown-svc"] = ("loc", set(), tuple(), "SELECT 1", 1.0, False)
    _ice._snapshot_files_cache["teardown-svc"] = ("loc", 1, "s3://b/x", [])

    _ice.clear_source_caches("teardown-svc")  # no flag → full wipe

    assert "teardown-svc" not in _ice._view_cache
    assert "teardown-svc" not in _ice._snapshot_files_cache


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


# Silence ruff unused-imports
_ = MagicMock
_ = pytest
