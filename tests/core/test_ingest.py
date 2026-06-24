from unittest.mock import MagicMock, patch

from backend.core.ingest import (
    _delete_objects_robust,
    get_catalog_field_ids,
    get_ingest_columns_sql,
    get_ingest_type_hints,
)
from backend.core.log_fields import LOG_FIELD_CATALOG


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


# ── get_ingest_columns_sql: DuckDB read_csv types map ───────────────────────


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


# ── _delete_objects_robust: bulk delete with fallback ──────────────────────


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


# ── _parse_fastly_filename_dt (pure helper) ─────────────────────────────


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


# ── _compute_incremental_start_after (lookback marker) ──────────────────


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


# ── ingest() early-exit paths ──────────────────────────────────────────


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
