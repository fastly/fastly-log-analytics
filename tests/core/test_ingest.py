"""Credential-cache invalidation for the boto3 FOS client.

A teardown→re-provision of the same service id can rotate FOS or CDN
credentials. The process-wide ``_fos_client_cache`` must not keep serving
stale auth — otherwise every ingest GET/HEAD + parquet read can 401 until the
backend restarts (the prod incident this guards against). Two mechanisms are pinned:

  * the cache is keyed on ``(name, access_key_id, cdn_url, cdn_secret)`` so
    rotated credentials MISS the cache and rebuild with fresh auth
    (defense-in-depth that auto-heals a rotation a caller forgets to invalidate), and
  * ``clear_fos_client`` drops every entry for a service name — the explicit
    invalidation called from the provision/teardown/ingest router seams.

Mirrors ``tests/utils/test_telemetry_proxy_phase4.py`` in exercising the real
``_get_fos_client`` (boto3 client construction is offline; the telemetry proxy
start is idempotent).
"""

from __future__ import annotations

import os
import string
import threading
import time
from functools import partial
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    rule,
)
from tests._helpers.sources import make_source

from backend.core import duckdb as _ddb
from backend.core import ingest
from backend.core import metadata as metadata_db
from backend.core.ingest import (
    _delete_objects_robust,
    _delete_objects_robust_with_failures,
    get_catalog_field_ids,
    get_ingest_columns_sql,
    get_ingest_type_hints,
)
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata.base import get_con
from tests.core.test_ingest_recovery_and_ledger import (  # reuse fixture helpers
    _install_mock_fos,
    _seed_local,
    ingest_local_env,  # noqa: F401 — pytest fixture re-export
)


# ── Merged from tests/core/test_ingest.py ──
def test_ingest_type_hints():
    """Verify that ingest type hints map correctly from the log catalog."""
    hints = get_ingest_type_hints()
    expected_len = len([f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None])
    assert len(hints) == expected_len

    # Check a specific type casting behavior defined in ingest.py
    assert hints["timestamp"] == "TIMESTAMPTZ"

    # Check that others match duckdb_type from catalog
    ip_field = next(f for f in LOG_FIELD_CATALOG if f["id"] == "ip")
    assert hints["ip"] == ip_field["duckdb_type"]


def test_ingest_type_hints_with_custom_fields():
    """Custom fields are added to the type hints dict when provided."""
    config = {
        "custom_fields": [
            {"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "my_int", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "my_disabled", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    hints = get_ingest_type_hints(config)
    assert hints["my_field"] == "VARCHAR"
    assert hints["my_int"] == "BIGINT"
    assert "my_disabled" not in hints
    # Base fields still present
    assert hints["timestamp"] == "TIMESTAMPTZ"


def test_get_catalog_field_ids_with_custom_fields():
    """Custom fields are appended sorted by name; disabled fields excluded."""
    config = {
        "custom_fields": [
            {"name": "zzz_field", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "aaa_field", "duckdb_type": "BIGINT", "enabled": True},
            {"name": "disabled_field", "duckdb_type": "VARCHAR", "enabled": False},
        ]
    }
    ids = get_catalog_field_ids(config)
    # aaa sorts before zzz
    assert ids.index("aaa_field") < ids.index("zzz_field")
    assert "disabled_field" not in ids
    # All base fields present
    assert "timestamp" in ids


def test_get_ingest_columns_sql_renders_duckdb_types_map():
    """DuckDB ``read_csv(..., types={...})`` requires a brace-wrapped
    list of ``'fid': 'TYPE'`` pairs. Pinned because losing the braces
    would silently fall back to inferred types, breaking the
    partitioning. (Field ids and type names are wrapped in
    ``escape_sql_literal``-quoted single quotes per finding Jun14-001;
    the timestamp partition column must still be present and explicitly
    TIMESTAMPTZ.)"""
    sql = get_ingest_columns_sql()
    assert sql.startswith("{") and sql.endswith("}")
    assert "'timestamp': 'TIMESTAMPTZ'" in sql


def test_get_ingest_columns_sql_includes_enabled_custom_fields_only():
    cfg = {
        "custom_fields": [
            {"name": "extra_a", "duckdb_type": "VARCHAR", "enabled": True},
            {"name": "extra_disabled", "duckdb_type": "BIGINT", "enabled": False},
        ]
    }
    sql = get_ingest_columns_sql(cfg)
    assert "'extra_a': 'VARCHAR'" in sql
    assert "extra_disabled" not in sql


def test_get_ingest_columns_sql_escapes_quote_in_field_metadata():
    """Finding Jun14-001: ``get_ingest_columns_sql`` previously
    interpolated custom-field ids and DuckDB type names raw into the
    DuckDB types-map literal that drives ``read_csv``. A custom field
    with a hostile ``duckdb_type`` like ``VARCHAR'; ATTACH '/etc/passwd' AS x; --``
    would have broken out of the SQL string and executed multi-statement
    SQL against the ingest connection. The fix wraps both halves of
    every map entry with ``escape_sql_literal`` (doubles embedded
    single quotes), so a quote in either position renders as ``''``
    inside the string instead of terminating it.

    Admin-write surface (custom-field schema is validated on save by
    a strict regex today), but defense in depth on the ingest hot
    path — the only thing standing between a misconfigured /
    bypassed validator and a fully-controlled DuckDB statement."""
    cfg = {
        "custom_fields": [
            {
                "name": "hostile",
                # The validator would reject this; this test bypasses by
                # constructing the config dict directly to verify the
                # SQL-string-escaping behaviour at the ingest seam.
                "duckdb_type": "VARCHAR'; DROP TABLE",
                "enabled": True,
            }
        ]
    }
    sql = get_ingest_columns_sql(cfg)
    # The embedded single quote must be doubled, neutralising the
    # statement-terminator. ``escape_sql_literal`` does ' → ''.
    assert "VARCHAR''" in sql, f"single-quote in duckdb_type must be doubled; got: {sql}"
    # And there must be no UNESCAPED quote sequence that would close
    # the map-entry string — a bare ``'; DROP`` after the value is the
    # bad shape. After escaping, ``'; DROP`` becomes ``''; DROP``
    # which stays inside the quoted literal.
    assert "VARCHAR'; DROP" not in sql, f"raw quote sequence escaped from the SQL literal; got: {sql}"


def test_delete_objects_robust_returns_zero_for_empty_list():
    """No keys → no-op, no API call. Pinned because a refactor that
    sent ``Delete={"Objects": []}`` would surface as a 400 from S3."""
    fake_s3 = MagicMock()
    assert _delete_objects_robust(fake_s3, "bkt", []) == 0
    fake_s3.delete_objects.assert_not_called()


def test_delete_objects_robust_uses_bulk_delete_for_happy_path():
    """All keys fit in one batch → single ``delete_objects`` call
    with the full list."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}  # no errors

    keys = [f"k{i}" for i in range(20)]
    deleted = _delete_objects_robust(fake_s3, "bkt", keys)

    assert deleted == 20
    fake_s3.delete_objects.assert_called_once()
    payload = fake_s3.delete_objects.call_args.kwargs["Delete"]
    assert len(payload["Objects"]) == 20
    assert payload["Quiet"] is True  # silences per-key success entries


def test_delete_objects_robust_reports_exact_partial_failures():
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Key": "k2", "Code": "InternalError", "Message": "retry later"}]}

    deleted, failed_keys = _delete_objects_robust_with_failures(fake_s3, "bkt", ["k1", "k2", "k3"])

    assert deleted == 2
    assert failed_keys == {"k2"}


def test_delete_objects_robust_batches_in_500_chunks():
    """Bulk delete is paginated at 500 (chosen for safety vs AWS's
    1000 limit since FOS implements only the subset). Pinned because
    sending > 1000 would 400."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {}

    keys = [f"k{i}" for i in range(1234)]
    deleted = _delete_objects_robust(fake_s3, "bkt", keys)

    assert deleted == 1234
    # 1234 / 500 → 3 calls (500, 500, 234)
    assert fake_s3.delete_objects.call_count == 3


def test_delete_objects_robust_short_circuits_on_access_denied_in_bulk():
    """When the first batch returns AccessDenied, stop the batch loop
    (no point continuing without perms) and return how many succeeded.
    Pinned because attempting all 500 batches against a misconfigured
    bucket would burn the cron's time budget for no benefit."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied", "Message": "x"}]}

    deleted = _delete_objects_robust(fake_s3, "bkt", [f"k{i}" for i in range(10)])

    # 0 returned because first batch failed mid-stream
    assert deleted == 0


def test_delete_objects_robust_falls_back_to_individual_on_bulk_exception():
    """If bulk delete raises (FOS endpoint that doesn't support
    DeleteObjects), fall through to individual ``delete_object``
    calls. Pinned because this is the compatibility fallback for
    older FOS shards."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("UnsupportedOperation")
    fake_s3.delete_object.return_value = {}  # individual calls succeed

    keys = ["k1", "k2", "k3"]
    deleted = _delete_objects_robust(fake_s3, "bkt", keys)

    assert deleted == 3
    # Each key got its own delete_object call
    assert fake_s3.delete_object.call_count == 3


def test_delete_objects_robust_returns_zero_when_bulk_fails_with_access_denied():
    """An ``AccessDenied`` exception (not just an Errors entry) →
    return 0 immediately, don't even try the individual fallback.
    Pinned because retrying with the same creds would just hit the
    same wall."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("AccessDenied: not allowed")

    deleted = _delete_objects_robust(fake_s3, "bkt", ["k1", "k2"])

    assert deleted == 0
    fake_s3.delete_object.assert_not_called()


def test_delete_objects_robust_individual_fallback_stops_on_access_denied():
    """Within the individual fallback loop, an AccessDenied on ONE
    key stops the loop (same as the bulk path). Pinned because
    grinding through 1000 individual AccessDenied errors would log-
    spam and waste time."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")

    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("AccessDenied")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail

    deleted = _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3", "k4"])

    # One success, then AccessDenied stops the loop
    assert deleted == 1
    assert call_count["n"] == 2


def test_delete_objects_robust_individual_fallback_continues_on_other_errors():
    """A per-key non-AccessDenied error (e.g., NoSuchKey from a race)
    → log + continue. Pinned because losing this would let a single
    deleted-by-something-else key abort the whole batch."""
    fake_s3 = MagicMock()
    fake_s3.delete_objects.side_effect = RuntimeError("bulk failed")

    call_count = {"n": 0}

    def _maybe_fail(*, Bucket, Key):
        call_count["n"] += 1
        if Key == "k2":
            raise RuntimeError("NoSuchKey: gone")
        return {}

    fake_s3.delete_object.side_effect = _maybe_fail

    deleted = _delete_objects_robust(fake_s3, "bkt", ["k1", "k2", "k3"])

    # 2 succeeded (k1 + k3), k2 failed but loop continued
    assert deleted == 2
    assert call_count["n"] == 3
    _ = patch  # quiet ruff


def test_parse_fastly_filename_dt_parses_standard_format():
    """Fastly raw log filename format:
    ``YYYY-MM-DDTHH-MM-SS.<svcuid>.gz`` → UTC datetime. Pinned because
    the discovery loop uses this to early-stop pagination once it
    passes the end_time bound; a regression silently degrades to
    full-bucket scans on every cron run."""
    from datetime import UTC, datetime

    from backend.core.ingest import _parse_fastly_filename_dt

    dt = _parse_fastly_filename_dt("2026-05-04T18-30-00.svc1234.gz")
    assert dt == datetime(2026, 5, 4, 18, 30, 0, tzinfo=UTC)


def test_parse_fastly_filename_dt_returns_none_for_unrelated_filename():
    """Filenames that don't match the timestamp prefix (e.g., manifest
    files, .DS_Store) → None. Pinned because returning a bogus
    datetime here would trigger early-stop on real log files."""
    from backend.core.ingest import _parse_fastly_filename_dt

    assert _parse_fastly_filename_dt("manifest.json") is None
    assert _parse_fastly_filename_dt("readme.txt") is None
    assert _parse_fastly_filename_dt(".DS_Store") is None


def test_parse_fastly_filename_dt_handles_dashes_in_time_only():
    """REGRESSION: the previous impl did ``.replace("-", ":", 2)``
    which replaced dashes in the DATE half (``2026:05:04T...``),
    silently failing fromisoformat. This test pins the fix: split
    on ``T`` and only convert dashes in the time half."""
    from datetime import UTC, datetime

    from backend.core.ingest import _parse_fastly_filename_dt

    dt = _parse_fastly_filename_dt("2026-01-15T09-00-00.svc.gz")
    assert dt is not None
    # The DATE half must NOT have colons (only the time half does)
    assert dt == datetime(2026, 1, 15, 9, 0, 0, tzinfo=UTC)


def test_parse_fastly_filename_dt_handles_colon_time_format():
    """REGRESSION: production Fastly raw filenames use colons in the
    time half (``2026-05-26T18:36:50.000-<random>.log.gz``). The
    initial regex only accepted dashes, so every cron run silently
    fell back to a full-bucket LIST instead of the bounded 4 h scan.
    This test pins acceptance of both separators."""
    from datetime import UTC, datetime

    from backend.core.ingest import _parse_fastly_filename_dt

    dt = _parse_fastly_filename_dt("2026-05-26T18:36:50.000-WFFKlnSyTWAMB3qwtVDD.log.gz")
    assert dt == datetime(2026, 5, 26, 18, 36, 50, tzinfo=UTC)


def test_compute_incremental_start_after_returns_none_for_empty_set():
    """Empty `already` → None (caller falls back to full-bucket
    scan). Pinned because the first cron run after teardown has an
    empty set; returning a bogus key would skip files."""
    from backend.core.ingest import _compute_incremental_start_after

    assert _compute_incremental_start_after(set()) is None


def test_compute_incremental_start_after_subtracts_lookback_hours_from_latest():
    """The returned StartAfter key is the latest filename's hour
    minus N hours (lookback to catch late-arriving POP logs).
    Pinned because losing the lookback would skip late-arriving
    POP files; making it too large defeats the optimisation."""
    from backend.core.ingest import _compute_incremental_start_after

    already = {
        "s3://b/raw/2026-05-04/12/2026-05-04T12-00-00.svc.gz",
        "s3://b/raw/2026-05-04/15/2026-05-04T15-30-00.svc.gz",
    }
    # latest = 2026-05-04 15:30 UTC, minus 4 hours = 2026-05-04 11:30
    out = _compute_incremental_start_after(already, lookback_hours=4)
    # Format is raw/YYYY-MM-DD/HH/ — at the floor of the lookback hour
    assert out == "raw/2026-05-04/11/"


def test_compute_incremental_start_after_returns_none_when_no_filename_matches():
    """When no file in `already` matches the Fastly format (e.g. an
    upgrade left bogus entries), return None for full-bucket safety."""
    from backend.core.ingest import _compute_incremental_start_after

    already = {"s3://b/raw/garbage-entry.gz", "s3://b/raw/another.json"}
    assert _compute_incremental_start_after(already) is None


def test_compute_incremental_start_after_respects_custom_lookback():
    """Larger lookback_hours = bigger window. Pinned because the
    manual-import path can pass a longer window to catch more
    historical files."""
    from backend.core.ingest import _compute_incremental_start_after

    already = {"s3://b/raw/2026-05-04/15/2026-05-04T15-00-00.svc.gz"}
    out = _compute_incremental_start_after(already, lookback_hours=24)
    # latest 2026-05-04 15:00 - 24h = 2026-05-03 15:00
    assert out == "raw/2026-05-03/15/"


def _drain_ingest(gen):
    """Drain the ingest() generator and return (events, exception_or_None)."""
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def test_ingest_yields_error_when_read_only_source():
    """Read-only sources can't ingest (write op). Pinned because
    analyst-replicas have `access_level=read_only` and triggering
    ingest from there would mint orphan files that can't be cleaned
    up without admin perms."""
    from backend.core.ingest import ingest

    src = {"name": "test-svc", "service_id": "svc-1", "access_level": "read_only", "bucket": "b"}
    with patch("backend.core.ingest._ensure_source_registered"):
        events, _ = _drain_ingest(ingest(source=src))
    assert any(e.get("type") == "error" and "read-only" in e["message"].lower() for e in events)


def test_ingest_yields_error_when_bucket_missing():
    """Source without a bucket → error event (cron-recoverable on the
    next config save). Pinned because mid-teardown can leave a config
    with no bucket, and crashing here would freeze the scheduler."""
    from backend.core.ingest import ingest

    src = {"name": "test-svc", "service_id": "svc-1", "bucket": ""}
    with patch("backend.core.ingest._ensure_source_registered"):
        events, _ = _drain_ingest(ingest(source=src))
    assert any(e.get("type") == "error" and "bucket" in e["message"].lower() for e in events)


def test_ingest_short_circuits_to_done_when_no_new_files():
    """When the FOS scan returns no new files, yield a done event with zero
    new files and ``skipped_files`` = the count actually re-seen this run.
    An empty LIST means nothing was skipped → 0, regardless of how many
    files sit in the dedup ledger. Pinned because this is the steady-state
    cron behavior under delete_after=True (bucket purged post-ingest)."""
    from backend.core.ingest import ingest

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = []  # No files
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator

    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value={"already1.gz", "already2.gz"}),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest(source=src))

    done = next((e for e in events if e["type"] == "done"), None)
    assert done is not None
    assert done["new_files"] == 0
    # Empty LIST → nothing re-seen → 0 skipped, even though the ledger holds 2.
    assert done["skipped_files"] == 0


def test_ingest_yields_error_on_fos_list_failure():
    """If the FOS list_objects_v2 call fails (network, auth), yield a
    typed error event. Pinned because losing this would crash the
    scheduler thread on transient S3 issues."""
    from backend.core.ingest import ingest

    fake_s3 = MagicMock()
    fake_s3.get_paginator.side_effect = RuntimeError("S3 connection refused")

    src = {"name": "test-svc", "service_id": "svc-1", "bucket": "b", "prefix": ""}

    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
    ):
        events, _ = _drain_ingest(ingest(source=src))

    error = next((e for e in events if e["type"] == "error"), None)
    assert error is not None
    assert "connection refused" in error["message"].lower() or "list" in error["message"].lower()


def test_ingest_sets_force_download_on_mem_con():
    """The ingest mem_con must have ``force_download=true`` set so DuckDB
    skips the per-file HEAD-before-GET probe on every ``.log.gz`` read.
    Telemetry on 2026-05-20 showed 392K of those HEADs/day with zero
    benefit (gzip isn't seekable; we always read the whole file). Pinned
    because dropping this setting would silently double the per-file
    round-trip count on every ingest cycle."""
    import duckdb

    from backend.core.ingest import ingest

    executed_sql: list[str] = []

    class _RecordingCon:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            executed_sql.append(sql)
            return self._real.execute(sql, *args, **kwargs)

        def close(self):
            return self._real.close()

        def __getattr__(self, name):
            return getattr(self._real, name)

    def _fake_get_mem_con(src):
        con = duckdb.connect(":memory:")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        return _RecordingCon(con)

    fake_page = {"Contents": [{"Key": "raw/2026-05-20/00/2026-05-20T00-00-00.000.gz", "Size": 1024}]}
    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [fake_page]
    fake_s3 = MagicMock()
    fake_s3.get_paginator.return_value = fake_paginator

    src = {
        "name": "test-svc",
        "service_id": "svc-1",
        "bucket": "b",
        "prefix": "",
    }
    with (
        patch("backend.core.ingest._ensure_source_registered"),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
        patch("backend.core.ingest._get_fos_client", return_value=fake_s3),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_fake_get_mem_con),
        # Short-circuit the actual read_json_auto call — we only care that
        # the SET statements ran before the loop body would have executed.
        patch("backend.core.ingest._execute_query_with_retry", side_effect=RuntimeError("synthetic stop")),
    ):
        _drain_ingest(ingest(source=src))

    normalized = [" ".join(s.split()).lower() for s in executed_sql]
    assert any("set force_download = true" in s for s in normalized), (
        f"ingest mem_con never ran SET force_download = true; executed: {normalized[:10]}"
    )


# ── Merged from tests/core/test_ingest_fos_clients.py ──
def _src(name: str, akid: str, *, cdn_secret: str = "") -> dict:
    return {
        "name": name,
        "service_id": name,
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
        "access_key_id": akid,
        "secret_access_key": "secret",
        "cdn_url": "https://cdn.example.test",
        "cdn_secret": cdn_secret,
    }


def _reset_cache() -> None:
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()


def test_same_creds_reuse_cached_client():
    _reset_cache()
    try:
        c1 = _ddb._get_fos_client(_src("svc-a", "KEY1"))
        c2 = _ddb._get_fos_client(_src("svc-a", "KEY1"))
        assert c1 is c2, "same credentials must reuse the cached client"
    finally:
        _reset_cache()


def test_rotated_key_misses_cache():
    """A new access key for the same service id is a cache MISS — the client is
    rebuilt rather than serving the stale (now-deleted) key."""
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-rot", "OLDKEY"))
        c_new = _ddb._get_fos_client(_src("svc-rot", "NEWKEY"))
        assert c_old is not c_new, "rotated key must not return the stale client"
        keys = {k[:2] for k in _ddb._fos_client_cache if k[0] == "svc-rot"}
        assert ("svc-rot", "OLDKEY") in keys and ("svc-rot", "NEWKEY") in keys
    finally:
        _reset_cache()


def test_rotated_cdn_secret_misses_cache():
    """A changed CDN secret must rebuild the hook that injects x-fastly-key."""
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="OLDSECRET"))
        c_new = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="NEWSECRET"))
        assert c_old is not c_new, "rotated CDN secret must not reuse the stale client hook"
    finally:
        _reset_cache()


def test_clear_fos_client_drops_all_entries_for_name():
    _reset_cache()
    try:
        _ddb._get_fos_client(_src("svc-clear", "OLDKEY"))
        _ddb._get_fos_client(_src("svc-clear", "NEWKEY"))
        _ddb._get_fos_client(_src("other", "K"))

        # accepts a bare service name
        _ddb.clear_fos_client("svc-clear")
        remaining = set(_ddb._fos_client_cache)
        assert not any(k[0] == "svc-clear" for k in remaining), "every svc-clear entry cleared"
        assert any(k[:2] == ("other", "K") for k in remaining), "other services untouched"

        # ...and a source dict
        _ddb.clear_fos_client({"name": "other"})
        assert not any(k[:2] == ("other", "K") for k in _ddb._fos_client_cache)
    finally:
        _reset_cache()


def test_clear_fos_client_is_noop_when_absent():
    _reset_cache()
    # Must not raise on an unknown service / empty cache.
    _ddb.clear_fos_client("never-built")
    _ddb.clear_fos_client({"name": "never-built"})


# ── Merged from tests/core/test_ingest_in_flight.py ──
def test_deterministic_buffer_name_is_stable_across_runs():
    name1 = ingest._deterministic_buffer_name(["s3://b/a.gz", "s3://b/b.gz"])
    name2 = ingest._deterministic_buffer_name(["s3://b/b.gz", "s3://b/a.gz"])
    assert name1 == name2, "buffer name must be order-independent"
    assert name1.startswith("batch_") and name1.endswith(".parquet")
    assert len(name1) == len("batch_") + 16 + len(".parquet")


def test_deterministic_buffer_name_differs_for_distinct_chunks():
    n1 = ingest._deterministic_buffer_name(["s3://b/a.gz"])
    n2 = ingest._deterministic_buffer_name(["s3://b/b.gz"])
    assert n1 != n2


def test_record_and_list_in_flight_roundtrip():
    sid = "svc-inflight-roundtrip"
    rows = [("s3://b/a.gz", 100, 4096), ("s3://b/b.gz", 200, 8192)]
    metadata_db.record_in_flight(sid, "batch_abc.parquet", rows)

    pending = metadata_db.list_in_flight(sid)
    assert len(pending) == 1
    name, tuples = pending[0]
    assert name == "batch_abc.parquet"
    assert tuples == rows


def test_record_in_flight_overwrites_on_same_buffer_name():
    """A re-ingest of the same chunk (same deterministic name) must
    overwrite the prior manifest cleanly — never raise on UNIQUE."""
    sid = "svc-inflight-upsert"
    metadata_db.record_in_flight(sid, "batch_xyz.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid, "batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])

    pending = metadata_db.list_in_flight(sid)
    assert len(pending) == 1
    assert pending[0][1] == [("a", 1, 10), ("b", 2, 20)]


def test_clear_in_flight_is_idempotent():
    sid = "svc-inflight-clear"
    metadata_db.record_in_flight(sid, "batch_clr.parquet", [("a", 1, 10)])
    metadata_db.clear_in_flight(sid, "batch_clr.parquet")
    metadata_db.clear_in_flight(sid, "batch_clr.parquet")  # must not raise
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_promotes_when_buffer_exists(tmp_path):
    """Buffer present on disk → the in_flight tuples are committed to
    ``ingested_files`` and the in_flight row is cleared."""
    sid = "svc-recover-promote"
    src = {"name": sid}

    rows = [("s3://b/x.gz", 50, 1000), ("s3://b/y.gz", 75, 2000)]
    metadata_db.record_in_flight(sid, "batch_present.parquet", rows)

    # Buffer file exists on disk.
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_present.parquet").write_bytes(b"PAR1stub")

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 1
    assert result["dropped"] == 0
    assert result["rows_recovered"] == 125

    # Tuples promoted into ingested_files
    committed = metadata_db.get_ingested_filenames(sid)
    assert committed == {"s3://b/x.gz", "s3://b/y.gz"}

    # in_flight row cleared
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_drops_when_buffer_missing(tmp_path):
    """Buffer missing on disk → drop in_flight row WITHOUT committing.
    The source files will re-LIST and re-ingest cleanly next tick."""
    sid = "svc-recover-drop"
    src = {"name": sid}

    rows = [("s3://b/q.gz", 10, 100)]
    metadata_db.record_in_flight(sid, "batch_missing.parquet", rows)

    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()  # empty dir, no parquet

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 0
    assert result["dropped"] == 1

    # ingested_files was NOT touched
    assert metadata_db.get_ingested_filenames(sid) == set()

    # in_flight row cleared so the next sweep doesn't replay it
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_is_noop_when_no_pending(tmp_path):
    sid = "svc-recover-empty"
    src = {"name": sid}

    with patch("backend.core.iceberg._buffer_dir", return_value=str(tmp_path)):
        result = ingest._recover_in_flight(src)
    assert result == {"promoted": 0, "dropped": 0, "rows_recovered": 0}


def test_recover_in_flight_handles_mixed_state(tmp_path):
    """One buffer present, one missing — recovery handles each independently."""
    sid = "svc-recover-mixed"
    src = {"name": sid}

    metadata_db.record_in_flight(sid, "batch_here.parquet", [("a.gz", 5, 50)])
    metadata_db.record_in_flight(sid, "batch_gone.parquet", [("b.gz", 7, 70)])

    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_here.parquet").write_bytes(b"PAR1stub")

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 1
    assert result["dropped"] == 1
    assert result["rows_recovered"] == 5

    committed = metadata_db.get_ingested_filenames(sid)
    assert committed == {"a.gz"}  # b.gz NOT committed
    assert metadata_db.list_in_flight(sid) == []


def test_list_in_flight_is_per_service():
    """Two services' in_flight rows must not bleed across each other."""
    sid_a = "svc-inflight-a"
    sid_b = "svc-inflight-b"
    metadata_db.record_in_flight(sid_a, "batch_a.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid_b, "batch_b.parquet", [("b", 2, 20)])

    a_pending = metadata_db.list_in_flight(sid_a)
    b_pending = metadata_db.list_in_flight(sid_b)
    assert len(a_pending) == 1 and a_pending[0][0] == "batch_a.parquet"
    assert len(b_pending) == 1 and b_pending[0][0] == "batch_b.parquet"


def test_recover_in_flight_survives_buffer_dir_missing(tmp_path):
    """The buffer directory itself may not exist yet (fresh service);
    the recovery sweep should treat that as 'no buffer present' for
    every pending row and drop them rather than crash."""
    sid = "svc-recover-no-dir"
    src = {"name": sid}

    metadata_db.record_in_flight(sid, "batch_nodir.parquet", [("a.gz", 1, 1)])
    missing_dir = str(tmp_path / "does_not_exist" / "buffer")
    assert not os.path.exists(missing_dir)

    with patch("backend.core.iceberg._buffer_dir", return_value=missing_dir):
        result = ingest._recover_in_flight(src)
    assert result["dropped"] == 1
    assert result["promoted"] == 0


# ── Merged from tests/core/test_ingest_task_wrappers.py ──
SERVICE_ID = "test-ingest-task-wrappers"


def _depths(depth: int, ok: bool = True):
    return patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": depth}, ok))


def test_convert_batch_files_task_converts_the_whole_batch_in_one_call():
    """One call with N keys — not N calls — is the entire point of the
    batched task (one DuckLake catalog commit per batch)."""
    keys = ["raw/a.json.gz", "raw/b.json.gz", "raw/c.json.gz"]
    with (
        patch.object(ingest, "convert_batch_objects", return_value={"committed": 3}) as mock_fn,
        patch.object(ingest, "convert_batch_rum_objects") as mock_rum,
    ):
        assert ingest.convert_batch_files.apply(args=(SERVICE_ID, keys)).get() == {"committed": 3}
    mock_fn.assert_called_once()
    assert mock_fn.call_args.args[0] == SERVICE_ID
    assert mock_fn.call_args.args[1] == keys
    assert mock_fn.call_args.args[2]
    mock_rum.assert_not_called()


def test_convert_batch_rum_files_task_converts_beacon_batch_not_regular_batch():
    keys = ["raw/rum/a.json.gz", "raw/rum/b.json.gz"]
    with (
        patch.object(ingest, "convert_batch_rum_objects", return_value={"committed": 2}) as mock_rum,
        patch.object(ingest, "convert_batch_objects") as mock_regular,
    ):
        assert ingest.convert_batch_rum_files.apply(args=(SERVICE_ID, keys)).get() == {"committed": 2}
    mock_rum.assert_called_once()
    assert mock_rum.call_args.args[0] == SERVICE_ID
    assert mock_rum.call_args.args[1] == keys
    assert mock_rum.call_args.args[2]
    mock_regular.assert_not_called()


def test_every_ledger_task_is_registered_under_its_module_path():
    """The worker's queue routing keys off these names; a rename silently
    stops the task from ever being dispatched."""
    expected = {
        "backend.core.ingest.convert_batch_files": ingest.convert_batch_files,
        "backend.core.ingest.convert_batch_rum_files": ingest.convert_batch_rum_files,
    }
    for name, task in expected.items():
        assert task.name == name
        assert name in ingest.app.tasks
        assert ingest.app.tasks[name].name == name


def test_celery_ledger_claim_semantics():
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status) VALUES (?, ?, 'discovered')",
        (service_id, object_key),
    )
    cur.execute(
        "UPDATE ingest_ledger SET status='claimed', claimed_by='worker-1' WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    con.commit()

    with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test"}):
        ingest.convert_batch_files.push_request(id="worker-2")
        try:
            ingest.convert_batch_files(service_id, [object_key])
        finally:
            ingest.convert_batch_files.pop_request()

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "worker-1"


@pytest.mark.parametrize("label", ["", "RUM "])
def test_dispatches_reclaimed_then_stuck_in_batches(label: str) -> None:
    dispatch = MagicMock()
    n = ingest.LEDGER_CONVERT_BATCH_SIZE
    reclaimed = [f"r{i}" for i in range(3)]
    stuck = [f"s{i}" for i in range(n)]
    with _depths(0):
        redispatched, broker_ok = ingest._redispatch_pending("svc", reclaimed, stuck, dispatch, label)
    assert (redispatched, broker_ok) == (n + 3, True)
    sent = [c.args for c in dispatch.call_args_list]
    assert all(a[0] == "svc" for a in sent)
    assert [k for a in sent for k in a[1]] == reclaimed + stuck
    assert [len(a[1]) for a in sent] == [n, 3]


def test_skips_when_queue_already_holds_enough_messages() -> None:
    dispatch = MagicMock()
    with _depths(10):
        assert ingest._redispatch_pending("svc", [], ["a"], dispatch, "") == (0, True)
    dispatch.assert_not_called()


def test_skips_when_broker_unavailable() -> None:
    dispatch = MagicMock()
    with _depths(0, ok=False):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "RUM ") == (0, False)
    dispatch.assert_not_called()


def test_broker_exception_is_treated_as_unavailable() -> None:
    dispatch = MagicMock()
    with patch("backend.celery_status.celery_queue_depths", side_effect=RuntimeError("boom")):
        assert ingest._redispatch_pending("svc", ["a"], [], dispatch, "") == (0, False)
    dispatch.assert_not_called()


def test_nothing_pending_is_a_noop() -> None:
    dispatch = MagicMock()
    assert ingest._redispatch_pending("svc", [], [], dispatch, "") == (0, True)
    dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("sweep_name", "delay_name", "prefix"),
    [
        ("sweep_ledger_once", "convert_batch_files", "raw/2026/08/27"),
        ("sweep_rum_ledger_once", "convert_batch_rum_files", "raw/rum/2026/08/27"),
    ],
)
def test_sweep_dispatches_each_pending_key_once_with_5000_rows(sweep_name: str, delay_name: str, prefix: str) -> None:
    from tests._helpers.ledger import clear_ledger

    service_id = "test-celery-svc"
    con, cur = clear_ledger(service_id)
    now = time.time()
    stale_claim = now - ingest.LEDGER_RECLAIM_AFTER_S - 60
    old = now - 3600
    rows = []
    for i in range(2500):
        rows.append((service_id, f"{prefix}/claimed_{i:05d}.json.gz", "claimed", "dead", stale_claim, old))
    for i in range(2500):
        rows.append((service_id, f"{prefix}/stuck_{i:05d}.json.gz", "discovered", None, None, old))
    cur.executemany(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.commit()

    def _no_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "b"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_no_files),
        patch("backend.core.ingest.discover_prefix", return_value=0),
        patch("backend.core.ingest.discover_rum_prefix", return_value=0),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
        patch(f"backend.core.ingest.{delay_name}.delay") as delay,
    ):
        summary = getattr(ingest, sweep_name)(service_id)

    keys = [k for c in delay.call_args_list for k in c.args[1]]
    assert summary["reclaimed"] == 2500
    assert summary["redispatched"] == 5000
    assert len(keys) == len(set(keys)) == 5000
    assert delay.call_count == -(-5000 // ingest.LEDGER_CONVERT_BATCH_SIZE)


# ── Merged from tests/core/test_ingest_timing.py ──
_make_source = partial(
    make_source,
    kind="cloud",
    cdn_url="",
    cdn_secret="",
    cdn_service_id="",
    logging_service_id="test",
    duckdb_path=":memory:",
    log_period=60,
    provisioning={},
)


def _drain(gen) -> list[dict]:
    return list(gen)


class TestIngestReadOnlyGuard:
    def test_read_only_yields_error_immediately(self):
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_only")
        events = _drain(ingest(source=src))

        assert len(events) == 1
        assert events[0]["type"] == "error"
        assert "read-only" in events[0]["message"].lower()

    def test_read_write_does_not_error_on_access_level(self):
        """read_write source should not hit the read-only guard."""
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_write")
        # We stop it early by patching _ensure_source_registered to raise so we
        # don't need a real DuckDB/FOS setup — we just want to confirm the guard
        # itself wasn't triggered. Must not use StopIteration inside a generator
        # (RuntimeError in Python 3.7+), so use ValueError instead.
        with patch("backend.core.ingest._ensure_source_registered", side_effect=ValueError("stop")):
            events = []
            try:
                for e in ingest(source=src):
                    events.append(e)
            except ValueError:
                pass

        # No read-only error should have been emitted before ValueError
        assert not any(e.get("type") == "error" and "read-only" in e.get("message", "").lower() for e in events)


class TestIngestStartTimeExec:
    """Verify start_time_exec (float) is always used for wall-clock arithmetic."""

    def test_start_time_exec_is_float(self):
        """Regression: start_time (str|None) must never be subtracted from time.time().

        Inject a string start_time and ensure no TypeError is raised from the
        max_seconds guard when it fires.
        """
        from backend.core.ingest import ingest

        src = _make_source()

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []

        # Simulate: time limit already exceeded on first check
        call_count = {"n": 0}
        real_start = time.time()

        def fake_time():
            call_count["n"] += 1
            # First call (start_time_exec = time.time()) returns real_start
            # Subsequent calls return start + 999 to trigger max_seconds immediately
            if call_count["n"] == 1:
                return real_start
            return real_start + 999.0

        # new_files is built from FOS listing; we stub it to [one_fake_file]
        # so the loop body executes and hits the max_seconds check.
        fake_file = "raw/2026-01-01/00/2026-01-01T00-00-00.svc.gz"

        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._get_fos_client") as mock_fos,
            patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
            patch("backend.core.metadata.insert_ingested_files"),
            patch("time.time", side_effect=fake_time),
        ):
            # Build a paginator that yields one file
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = iter([{"Contents": [{"Key": fake_file, "Size": 100}]}])
            mock_fos.return_value.get_paginator.return_value = mock_paginator

            events = []
            try:
                # Pass a string start_time — this is the parameter that was
                # mistakenly used in the time arithmetic before the fix.
                for e in ingest(source=src, max_seconds=1, start_time="2026-01-01T00:00:00Z"):
                    events.append(e)
            except TypeError as exc:
                raise AssertionError(
                    f"TypeError raised — start_time (str) was used in float arithmetic: {exc}"
                ) from exc

        # The time-limit event should appear (or the loop may not have reached it
        # if no new files were found after filtering, but no TypeError is the key assertion).
        types = [e.get("type") for e in events]
        assert "error" not in types or not any(
            "unsupported operand" in e.get("message", "") for e in events if e.get("type") == "error"
        ), "TypeError leaked into error event"


class TestIngestMaxSeconds:
    """max_seconds should stop ingestion gracefully, not crash."""

    def test_expired_listing_budget_still_attempts_first_batch(self):
        """A slow FOS LIST must not prevent every incremental run from making progress."""
        from backend.core.ingest import ingest

        src = _make_source()
        fake_file = "s3://bucket/raw/request/year=2026/month=01/day=01/hour=00/minute=00/test.log.gz"

        def fake_list_fos_files(**_kwargs):
            yield {"type": "status", "message": "Discovering"}
            return {
                "new_files": [fake_file],
                "file_sizes": {fake_file: 100},
                "skipped_already": 0,
                "stranded_already": [],
            }

        mock_con = MagicMock()
        with (
            patch("backend.core.ingest._ensure_source_registered"),
            patch("backend.core.ingest._recover_in_flight", return_value={"promoted": 0, "dropped": 0}),
            patch("backend.core.ingest._get_fos_client"),
            patch("backend.core.ingest.metadata_db.get_ingested_filenames", return_value=set()),
            patch("backend.core.ingest.list_fos_files", side_effect=fake_list_fos_files),
            patch("backend.core.duckdb.get_memory_connection", return_value=mock_con),
            patch("backend.core.ingest._download_chunk_to_local", return_value=({}, {})) as download,
            patch("backend.config.load_config", return_value={}),
            patch("backend.core.ingest.time.time", side_effect=[1_000.0, 1_999.0, 1_999.0, 1_999.0]),
        ):
            events = _drain(ingest(source=src, max_seconds=240, incremental_only=True))

        download.assert_called_once()
        assert any("Processing 1 files in batch 1/1" in event.get("message", "") for event in events)

    def test_max_seconds_zero_does_not_crash(self):
        """max_seconds=None means no limit; 0 is falsy so also no limit."""
        from backend.core.ingest import ingest

        src = _make_source(access_level="read_only")
        # read_only exits before any timing code, so no TypeError possible
        events = _drain(ingest(source=src, max_seconds=0))
        assert events[0]["type"] == "error"

    def test_max_seconds_type_annotation(self):
        """max_seconds is int|None — ensure the signature hasn't regressed."""
        import inspect

        from backend.core.ingest import ingest

        sig = inspect.signature(ingest)
        ann = sig.parameters["max_seconds"].annotation
        # Accept int | None or Optional[int] representations
        ann_str = str(ann)
        assert "int" in ann_str and "None" in ann_str, f"Unexpected annotation: {ann_str}"

    def test_start_time_annotation_is_str_not_float(self):
        """start_time must remain str|None (date range), not float (wall clock)."""
        import inspect

        from backend.core.ingest import ingest

        sig = inspect.signature(ingest)
        ann = sig.parameters["start_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str, f"Unexpected annotation: {ann_str}"
        assert "float" not in ann_str, "start_time became float — timing regression risk"


# ── Merged from tests/core/test_multi_process_ingest.py ──
def _run_workers(targets):
    """Start each (target, args) pair in a thread, then join with a timeout."""
    threads = [threading.Thread(target=t, args=a) for t, a in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), "worker thread hung past 30s"


def test_concurrent_ingest_calls_one_wins_one_is_no_op(ingest_local_env, monkeypatch):  # noqa: F811
    """Two threads try to start the same (service, task) cron run.

    OBSERVED behaviour (pinned here):
      - ``start_cron_run`` enforces the gate via a SELECT-then-INSERT inside
        a single SQLite connection. The loser raises ``RuntimeError`` with
        "already running" in the message.
      - The winner runs ``ingest()`` to completion (rows_inserted == 1).
      - The loser short-circuits at the gate (no FOS list, no DuckDB read).
      - ``get_ingested_filenames`` shows the file exactly once — no
        duplicate-row leak even if the gate were bypassed, because the
        dedup set is consulted at the top of ingest().
    """
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    service_id = src["name"]

    key = "raw/2026-06-10/08/2026-06-10T08-00-00.solo.gz"
    _seed_local(log_dir, key, "2026-06-10T08:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])

    # Synchronise both threads at the start_cron_run call so the SQLite
    # gate is the actual race, not whichever thread happened to be
    # scheduled first.
    barrier = threading.Barrier(2)
    results: dict[str, dict] = {}
    lock = threading.Lock()

    def worker(label: str) -> None:
        barrier.wait()
        outcome: dict = {"label": label}
        try:
            run_id = metadata_db.start_cron_run(service_id, "sync")
            outcome["run_id"] = run_id
            outcome["winner"] = True
            try:
                events = _drain(ingest.ingest(source=src))
                done = next((e for e in events if e["type"] == "done"), None)
                outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            finally:
                # Mark the run terminal so a leaked 'running' row doesn't
                # poison any follow-up test that re-uses the same service_id
                # within this pytest session (autouse fixtures rebuild the
                # sandbox, but defensive).
                try:
                    metadata_db.log_cron_run(service_id, "sync", 0.0, "success", run_id=run_id)
                except Exception:
                    pass
        except RuntimeError as exc:
            outcome["winner"] = False
            outcome["error"] = str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a",)), (worker, ("b",))])

    winners = [r for r in results.values() if r.get("winner")]
    losers = [r for r in results.values() if not r.get("winner")]

    assert len(winners) == 1, f"expected exactly one winner; winners={winners!r} losers={losers!r}"
    assert len(losers) == 1, f"expected exactly one loser; winners={winners!r} losers={losers!r}"
    assert "already running" in losers[0]["error"].lower(), (
        f"loser raised an unexpected RuntimeError: {losers[0]['error']!r} — "
        "the gate string contract drifted; update the assertion AND the "
        "scheduler call sites that match on this string."
    )
    assert winners[0]["rows_inserted"] == 1, f"winner ingest did not surface the seeded row; outcome={winners[0]!r}"

    # Dedup layer confirms the file is recorded exactly once.
    ingested = metadata_db.get_ingested_filenames(service_id)
    matching = [n for n in ingested if "solo.gz" in n]
    assert len(matching) == 1, (
        f"file appears {len(matching)} times in ingested_files; expected 1. "
        f"duplicate-row leak from concurrent ingest. ingested={ingested!r}"
    )


def test_concurrent_ingest_on_different_sources_both_succeed(ingest_local_env, monkeypatch):  # noqa: F811
    """Two threads against two different ``service_id`` values BOTH win.

    The gate is keyed on (service_id, task) — a sync for service A must not
    block a sync for service B in parallel. A regression that lifted this
    to a process-wide lock would surface as one thread becoming a 'loser'.

    We clone the fixture's source with a fresh name/service_id for the
    second worker. metadata_db is service_id-keyed; each ingest gets its
    own DuckDB connection and cache dir, and both share one mock bucket.
    """
    import duckdb

    import backend.core.duckdb as my_duckdb
    from tests.core.test_ingest_recovery_and_ledger import _S3RewritingConn

    log_dir = ingest_local_env["log_dir"]
    src_a = dict(ingest_local_env["src"])
    src_b = {**src_a, "name": "test_service_b", "service_id": "test-service-b"}

    # The shared fixture hands every ingest the SAME in-memory DuckDB
    # connection and ONE cache dir, so two concurrent sources collide on the
    # staging table and see each other's buffers. Give each ingest its own
    # connection and a per-service cache dir — what production does, since
    # each service has its own pool/cache.
    bucket_prefix = f"s3://{src_a['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    opened: list = []

    def fresh_memory_connection(_src):
        con = duckdb.connect(":memory:")
        opened.append(con)
        return _S3RewritingConn(con, bucket_prefix, file_prefix)

    monkeypatch.setattr(my_duckdb, "get_memory_connection", fresh_memory_connection)
    monkeypatch.setattr(
        my_duckdb, "_cache_dir", lambda s: str(log_dir.parent / "cache" / str(s.get("service_id") or s["name"]))
    )

    key_a = "raw/2026-06-11/09/2026-06-11T09-00-00.a.gz"
    key_b = "raw/2026-06-11/09/2026-06-11T09-00-00.b.gz"
    _seed_local(log_dir, key_a, "2026-06-11T09:00:00Z")
    _seed_local(log_dir, key_b, "2026-06-11T09:00:00Z")
    # Both sources see both keys via the same mock FOS; per-service dedup
    # via ingested_files keeps that scoped. Contract here is "both gates
    # open", not "exactly one key per source".
    _install_mock_fos(monkeypatch, log_dir, keys=[key_a, key_b])

    barrier = threading.Barrier(2)
    results: dict[str, dict] = {}
    lock = threading.Lock()

    def worker(label: str, src: dict) -> None:
        barrier.wait()
        outcome: dict = {"label": label, "service_id": src["name"]}
        try:
            run_id = metadata_db.start_cron_run(src["name"], "sync")
            outcome["run_id"] = run_id
            outcome["winner"] = True
            events = _drain(ingest.ingest(source=src))
            done = next((e for e in events if e["type"] == "done"), None)
            outcome["rows_inserted"] = done.get("rows_inserted") if done else None
            try:
                metadata_db.log_cron_run(src["name"], "sync", 0.0, "success", run_id=run_id)
            except Exception:
                pass
        except RuntimeError as exc:
            outcome["winner"] = False
            outcome["error"] = str(exc)
        with lock:
            results[label] = outcome

    _run_workers([(worker, ("a", src_a)), (worker, ("b", src_b))])

    for label, r in results.items():
        assert r.get("winner") is True, (
            f"source {label!r} ({r.get('service_id')}) lost the gate; "
            f"the per-service scope contract is broken. outcome={r!r}"
        )
        assert r["rows_inserted"] >= 1, (
            f"source {label!r} won the gate but ingested no rows; "
            f"the second-source path is silently broken. outcome={r!r}"
        )

    # Both services have independent ingested-files state.
    assert metadata_db.get_ingested_filenames(src_a["name"]), "source a has no ingested_files after concurrent run"
    assert metadata_db.get_ingested_filenames(src_b["name"]), "source b has no ingested_files after concurrent run"


# ── Merged from tests/core/test_ingest_stateful.py ──
_buffer_names = st.text(alphabet=string.ascii_lowercase, min_size=3, max_size=8).map(lambda s: f"batch_{s}.parquet")

_file_names = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=3, max_size=8).map(
    lambda s: f"raw/2026-06-17/00/{s}.gz"
)

_row_counts = st.integers(min_value=0, max_value=10_000)

_file_sizes = st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000))

_file_manifest = st.tuples(_file_names, _row_counts, _file_sizes)

_file_manifests = st.lists(_file_manifest, min_size=1, max_size=4, unique_by=lambda t: t[0])


class IngestMetadataStateMachine(RuleBasedStateMachine):
    """Model the SQLite ingest-metadata half of the ingest tick.

    Each rule mirrors one production call site (record / promote /
    clear); the shadow state tracks what the DB SHOULD show after
    that call. Invariants then cross-check the DB against the shadow.

    Bundle('buffers') holds buffer-filenames that are currently
    in_flight — Hypothesis draws from it for the "commit" and "drop"
    rules so they only act on buffers that exist (avoiding the
    no-op-only state space).
    """

    buffers = Bundle("buffers")

    @initialize()
    def setup(self):
        # One service per state-machine instance. The autouse
        # ``isolate_metadata_db`` fixture sandboxes this worker's Postgres
        # schema per pytest function — but Hypothesis runs MANY
        # state-machine instances within a single pytest function, and
        # every instance shares the SAME (service-scoped) rows in that
        # schema. Reset this service's own rows in every table the
        # invariants below check so each instance starts from an empty
        # slate; otherwise the second instance's shadow is empty while the
        # DB still carries rows from the first instance → invariant
        # violation → Hypothesis raises FlakyStrategyDefinition.
        #
        # ``metadata_db.teardown()`` used to do this (it deleted the whole
        # per-service SQLite file); under the shared Postgres metadata store
        # every service's rows live in the same tables as every other
        # service, so teardown() is a no-op there (by design — see
        # backend/core/metadata/base.py's teardown() docstring) and this
        # reset is done directly, scoped by service_id/source_name so it
        # can never touch another service's rows.
        self.service_id = "svc-hypothesis-ingest"
        con = metadata_db.get_con(self.service_id)
        con.execute("DELETE FROM ingest_in_flight WHERE source_name = ?", (self.service_id,))
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (self.service_id,))
        con.execute("DELETE FROM ingested_files_summary WHERE source_name = ?", (self.service_id,))
        con.commit()
        metadata_db.teardown(self.service_id)
        self.shadow_in_flight: dict[str, list[tuple[str, int, int | None]]] = {}
        self.shadow_ingested: set[str] = set()

    @rule(target=buffers, buffer=_buffer_names, files=_file_manifests)
    def record_in_flight(self, buffer, files):
        """Production call: ingest.py:_buffer_chunk() before writing parquet."""
        # Coerce to tuples so the equality compare with DB-roundtripped
        # tuples stays clean (lists vs tuples otherwise diff).
        files_t = [tuple(f) for f in files]
        metadata_db.record_in_flight(self.service_id, buffer, files_t)
        # Upsert semantics: re-record overwrites the prior manifest.
        self.shadow_in_flight[buffer] = files_t
        return buffer

    @rule(buffer=consumes(buffers))
    def promote_and_clear(self, buffer):
        """Happy-path commit: write_to_buffer succeeded, parquet on disk,
        cron promotes the manifest to ingested_files and clears the
        in_flight row. This is the production sequence inside
        ``commit_buffer`` + tombstone."""
        files = self.shadow_in_flight.pop(buffer, None)
        if files is None:
            # consumes() should have given us a live buffer; tolerate the
            # double-consume edge in case Hypothesis explores it.
            return
        metadata_db.insert_ingested_files(self.service_id, files)
        metadata_db.clear_in_flight(self.service_id, buffer)
        self.shadow_ingested.update(fname for (fname, _, _) in files)

    @rule(buffer=consumes(buffers))
    def crash_drop(self, buffer):
        """Recovery path for "crash before write_to_buffer": the
        in_flight row exists but the buffer parquet was never written.
        Recovery drops the row WITHOUT promoting — the files will be
        re-LISTed on the next ingest tick and re-ingested cleanly.

        Mirrors the ``else`` branch in ``_recover_in_flight``."""
        self.shadow_in_flight.pop(buffer, None)
        metadata_db.clear_in_flight(self.service_id, buffer)

    @rule(buffer=_buffer_names)
    def idempotent_clear(self, buffer):
        """``clear_in_flight`` on a buffer that doesn't exist is a no-op.
        Pinned because the production code calls it from a finally block
        regardless of upstream success — any future change that started
        raising on missing rows would break crash recovery."""
        # Only test "missing" — if the buffer happens to be in_flight,
        # this would behave like crash_drop and confuse the shadow.
        if buffer not in self.shadow_in_flight:
            metadata_db.clear_in_flight(self.service_id, buffer)

    @invariant()
    def in_flight_matches_shadow(self):
        db_state = {buffer: list(files) for buffer, files in metadata_db.list_in_flight(self.service_id)}
        assert set(db_state.keys()) == set(self.shadow_in_flight.keys()), (
            f"in_flight key drift: db={set(db_state.keys())!r} shadow={set(self.shadow_in_flight.keys())!r}"
        )
        for buffer, shadow_files in self.shadow_in_flight.items():
            assert db_state[buffer] == shadow_files, (
                f"in_flight payload drift for {buffer!r}: db={db_state[buffer]!r} shadow={shadow_files!r}"
            )

    @invariant()
    def ingested_matches_shadow(self):
        db_set = metadata_db.get_ingested_filenames(self.service_id)
        assert db_set == self.shadow_ingested, (
            f"ingested_files drift: db_only={db_set - self.shadow_ingested!r} "
            f"shadow_only={self.shadow_ingested - db_set!r}"
        )

    @invariant()
    def rollup_count_matches_shadow(self):
        """``ingested_files_summary.file_count`` must stay in lockstep
        with the actual ingested_files cardinality. The rollup is
        maintained in the same transaction; a refactor that split that
        transaction would surface here."""
        from backend.core.metadata import base as metadata_base

        con = metadata_base.get_con(self.service_id)
        row = con.execute(
            "SELECT file_count FROM ingested_files_summary WHERE source_name = ?",
            (self.service_id,),
        ).fetchone()
        rollup_count = row["file_count"] if row else 0
        assert rollup_count == len(self.shadow_ingested), (
            f"rollup file_count drift: rollup={rollup_count} shadow={len(self.shadow_ingested)}"
        )


TestIngestMetadataStateMachine = IngestMetadataStateMachine.TestCase

TestIngestMetadataStateMachine.settings = settings(
    max_examples=20,
    deadline=None,  # SQLite-on-tmpfs jitter at <50ms blew past Hypothesis' 200ms default
    stateful_step_count=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
