"""Targeted coverage for backend.core._duckdb_status helpers.

Covers four under-tested surfaces of the carved-out status module:

* ``update_top_values`` — happy path, catalog-error retry path, and the
  empty-fields short-circuit when the schema has no overlap with the
  27-field whitelist.
* ``delete_ingested_files`` generator — explicit-files bulk-delete path,
  empty-list short-circuit, read-only refusal, and the multi-pass error
  path when bucket listing fails.
* ``refresh_config_status`` — the two ``try/except: pass`` branches around
  the buffer-size walk and the iceberg.get_table_info error envelope, so
  that an underlying failure can't surface as a 500 on the status cron.
* ``enrich_asn_labels`` — digit-only value enrichment plus the mixed
  digit / non-digit case that only enriches the digit entries.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

# ── update_top_values ────────────────────────────────────────────────


def test_update_top_values_writes_cache_file_and_fingerprints(in_memory_duckdb, fos_source, tmp_path, monkeypatch):
    """Happy path: reservoir sample + 27-field GROUP BY writes
    top_values.json to the source's cache dir AND records a
    post-write fingerprint in _top_values_cache so the next
    invocation can short-circuit. Pinned because the dashboard's
    filter-picker autocomplete reads the JSON directly; losing this
    write would make filter suggestions stale by 60 s on every
    deploy."""
    from backend.core import duckdb as _db
    from backend.core._duckdb_status import update_top_values

    # Point _cache_dir at the test sandbox so the JSON write lands in
    # tmp_path rather than the repo-relative cache/ tree.
    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}

    # Seed a synthetic data dir so _data_stats_fingerprint returns a
    # non-None tuple — that's what update_top_values writes into
    # _top_values_cache after the JSON dump.
    (cache_dir / "data" / "ts=1").mkdir(parents=True)
    (cache_dir / "data" / "ts=1" / "f.parquet").write_bytes(b"x")

    # Build a real table on the connection so the existence probe + the
    # CREATE TEMP TABLE ... SAMPLE reservoir path succeed against
    # actual DuckDB. Only the fields the function expects are populated
    # to keep the test focused; the whitelist filter prunes the rest.
    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, country VARCHAR, status VARCHAR, method VARCHAR)")
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES "
        "('1.1.1.1', 'US', '200', 'GET'), "
        "('1.1.1.1', 'US', '200', 'GET'), "
        "('2.2.2.2', 'GB', '404', 'POST')"
    )

    # Force get_schema to return exactly the populated columns so the
    # function builds a SELECT we know matches the table.
    schema = [
        {"name": "ip", "type": "VARCHAR"},
        {"name": "country", "type": "VARCHAR"},
        {"name": "status", "type": "VARCHAR"},
        {"name": "method", "type": "VARCHAR"},
    ]

    # Reset the per-source cache so the test reliably exercises the
    # write path even if a prior test populated the fingerprint.
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)

    with patch("backend.core._duckdb_status.get_schema", return_value=schema):
        update_top_values(in_memory_duckdb, src)

    out_path = cache_dir / "top_values.json"
    assert out_path.exists(), "update_top_values must write top_values.json to the cache dir"

    with open(out_path) as fp:
        payload = json.load(fp)

    # Each whitelisted field that exists in schema must be a key in
    # the JSON. ip + country + status + method are all in the 27-field
    # whitelist and the schema, so all four must appear.
    assert set(payload.keys()) == {"ip", "country", "status", "method"}
    # ip='1.1.1.1' has count 2, '2.2.2.2' has count 1
    ip_values = {item["value"]: item["count"] for item in payload["ip"]}
    assert ip_values == {"1.1.1.1": 2, "2.2.2.2": 1}

    # Post-write fingerprint cached so the next refresh tick short-circuits.
    with _db._top_values_cache_lock:
        cached_fp = _db._top_values_cache.get(src["name"])
    assert cached_fp is not None, (
        "post-write fingerprint must be cached so the next refresh tick "
        "skips the 100k reservoir scan when the data dir hasn't changed"
    )


def test_update_top_values_retries_on_catalog_error_after_iceberg_refresh(in_memory_duckdb, fos_source, tmp_path):
    """When the first CREATE TEMP TABLE fails with a 'Catalog Error'
    (a buffer parquet was unlinked by a commit job between view
    refresh and our scan), update_top_values must call
    iceberg.update_iceberg_view and retry the same statement. Pinned
    because the failure mode appears as a ~60 s cron-tick gap on the
    filter-picker cache otherwise — every other tick crashes and
    leaves the JSON un-refreshed."""
    from backend.core import duckdb as _db
    from backend.core._duckdb_status import update_top_values

    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}

    # Make sure no fingerprint short-circuits the function before we
    # even reach the CREATE TEMP TABLE.
    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)

    # Build the real table so the existence probe passes; the retry
    # CREATE will then succeed against actual DuckDB.
    table_name = _db._safe_table_name(src["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (ip VARCHAR, status VARCHAR)")
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES ('1.1.1.1', '200')")

    schema = [
        {"name": "ip", "type": "VARCHAR"},
        {"name": "status", "type": "VARCHAR"},
    ]

    # Wrap the connection in a thin proxy so we can intercept .execute
    # without mutating the read-only DuckDBPyConnection attribute. The
    # FIRST CREATE TEMP TABLE raises a Catalog Error and all subsequent
    # calls (drop + retry-create + union-all read) pass through. Patterns
    # match the substring check inside update_top_values.
    state = {"create_calls": 0}

    class _ConProxy:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            if sql.startswith("CREATE TEMP TABLE"):
                state["create_calls"] += 1
                if state["create_calls"] == 1:
                    raise Exception("Catalog Error: Table with name foo does not exist")
            return self._real.execute(sql, *args, **kwargs)

    proxy = _ConProxy(in_memory_duckdb)
    with (
        patch("backend.core._duckdb_status.get_schema", return_value=schema),
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        update_top_values(proxy, src)

    mock_update.assert_called_once()
    # Two CREATE attempts: the failing one + the retry that succeeded.
    assert state["create_calls"] == 2, f"expected 1 initial CREATE + 1 retry, got {state['create_calls']}"

    # Retry path still writes the JSON file.
    assert (cache_dir / "top_values.json").exists()


def test_update_top_values_short_circuits_when_schema_has_no_overlap(in_memory_duckdb, fos_source, tmp_path):
    """A source whose schema has no overlap with the 27-field
    whitelist must NOT run the reservoir + GROUP BY at all — the
    SELECT list would be empty and the SQL would crash. Pinned
    because Iceberg services with only timestamp + custom fields
    used to crash this path with a 'syntax error near FROM'."""
    from backend.core import duckdb as _db
    from backend.core._duckdb_status import update_top_values

    cache_dir = tmp_path / "cache" / fos_source["bucket"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    src = {**fos_source, "_cache_dir_override": str(cache_dir)}

    with _db._top_values_cache_lock:
        _db._top_values_cache.pop(src["name"], None)

    table_name = _db._safe_table_name(src["name"])
    # Real table with only fields OUTSIDE the whitelist so the function
    # can pass the existence probe but must short-circuit before SELECT.
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP, custom_blob VARCHAR)")

    schema = [
        {"name": "timestamp", "type": "TIMESTAMP"},
        {"name": "custom_blob", "type": "VARCHAR"},
    ]

    state = {"sample_creates": 0}

    class _ConProxy:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            if sql.startswith("CREATE TEMP TABLE"):
                state["sample_creates"] += 1
            return self._real.execute(sql, *args, **kwargs)

    proxy = _ConProxy(in_memory_duckdb)
    with patch("backend.core._duckdb_status.get_schema", return_value=schema):
        update_top_values(proxy, src)

    assert state["sample_creates"] == 0, (
        "no overlap with the 27-field whitelist must short-circuit BEFORE "
        f"the CREATE TEMP TABLE; got {state['sample_creates']} create calls"
    )
    # No JSON written either — nothing to write.
    assert not (cache_dir / "top_values.json").exists()


# ── delete_ingested_files generator ─────────────────────────────────


def test_delete_ingested_files_explicit_files_mode_bulk_deletes(s3_mock, fos_source, in_memory_duckdb):
    """explicit_files mode yields status → progress → done and
    bulk-deletes every key in moto. Pinned because the admin
    'Delete Ingested' button passes the explicit list and the
    progress events drive the SSE progress bar; missing events
    would hang the UI banner."""
    from backend.core._duckdb_status import delete_ingested_files

    bucket = fos_source["bucket"]
    keys = [f"raw/2026-05-19/00/file-{i:02d}.gz" for i in range(10)]
    for key in keys:
        s3_mock.put_object(Bucket=bucket, Key=key, Body=b"{}")

    explicit = [f"s3://{bucket}/{k}" for k in keys]

    events = list(delete_ingested_files(in_memory_duckdb, fos_source, explicit_files=explicit))

    event_types = [e["type"] for e in events]
    assert event_types[0] == "status"
    assert "progress" in event_types
    assert event_types[-1] == "done"
    done = events[-1]
    assert done["deleted_files"] == 10

    # Bucket is now empty.
    remaining = s3_mock.list_objects_v2(Bucket=bucket)
    assert remaining.get("KeyCount", 0) == 0, f"expected all 10 keys deleted, got {remaining.get('Contents', [])}"


def test_delete_ingested_files_explicit_files_empty_yields_status(fos_source, in_memory_duckdb):
    """Empty explicit_files (after filtering out keys that don't
    belong to the source's bucket) yields a single 'No valid
    files' status event and returns without touching S3."""
    from backend.core._duckdb_status import delete_ingested_files

    # An explicit list with keys outside the source bucket — they all
    # get filtered out before the delete loop runs.
    explicit = ["s3://some-other-bucket/raw/foo.gz"]

    events = list(delete_ingested_files(in_memory_duckdb, fos_source, explicit_files=explicit))

    assert len(events) == 1
    assert events[0]["type"] == "status"
    assert "No valid files" in events[0]["message"]


def test_delete_ingested_files_read_only_yields_error_and_returns(in_memory_duckdb):
    """A source flagged ``access_level: read_only`` must refuse
    deletes and emit a clear error event. Pinned because the
    read-only escape hatch is the only safeguard between an
    analyst-shared service and accidental destruction."""
    from backend.core._duckdb_status import delete_ingested_files

    src = {"name": "ro-svc", "bucket": "ro-bucket", "access_level": "read_only"}

    events = list(delete_ingested_files(in_memory_duckdb, src))

    assert len(events) == 1
    assert events[0]["type"] == "error"
    assert "read-only" in events[0]["message"].lower()


def test_delete_ingested_files_multi_pass_error_on_list_failure(s3_mock, fos_source, in_memory_duckdb):
    """When the multi-pass intersection branch tries to list the
    bucket via _execute_query_with_retry and that raises, emit an
    error event mentioning 'Failed to list bucket' and break the
    loop. Pinned because the cleanup sweep would otherwise loop
    three times on an outage and stack three identical errors."""
    from backend.core._duckdb_status import delete_ingested_files

    def _raise(con, query, **_):
        raise RuntimeError("network gone")

    with patch("backend.core._duckdb_status._execute_query_with_retry", side_effect=_raise):
        events = list(delete_ingested_files(in_memory_duckdb, fos_source))

    error_events = [e for e in events if e["type"] == "error"]
    assert len(error_events) == 1, f"expected exactly one error event after first failed pass; got {events}"
    assert "Failed to list bucket" in error_events[0]["message"]


# ── refresh_config_status swallow branches ───────────────────────────


def test_refresh_config_status_swallows_buffer_size_exception(monkeypatch):
    """When os.walk on the buffer dir raises (permissions, FS
    corruption, removed mid-walk), refresh_config_status must
    swallow and continue writing status. Pinned because the cron
    fires every minute and a single broken FS read shouldn't kill
    the entire status cache update."""
    from backend.core._duckdb_status import refresh_config_status

    captured: dict = {}

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

    def _walk_explode(*_a, **_k):
        raise PermissionError("EACCES")

    with (
        patch(
            "backend.config.load_config",
            return_value={"name": "svc", "bucket": "b", "service_id": "svc"},
        ),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.config.update_status",
            side_effect=lambda sid, status: captured.setdefault("status", status),
        ),
        patch("backend.core.duckdb.get_connection", return_value=_StubCon()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"ingested": 0, "local_rows": 0},
        ),
        patch("backend.core.duckdb.get_schema", return_value=[]),
        patch("backend.core.duckdb.update_top_values"),
        # os.path.isdir returns True so the walk path is taken; os.walk then
        # raises and the except: pass at the call site must swallow it.
        patch("os.path.isdir", return_value=True),
        patch("os.walk", side_effect=_walk_explode),
    ):
        # Must NOT raise.
        refresh_config_status("svc")

    # update_status still fired even though the buffer walk crashed.
    assert "status" in captured, (
        "refresh_config_status must still call update_status when the buffer "
        "walk fails — the swallow branch is load-bearing for the status cron"
    )
    # buffer_size_bytes was never set because the walk threw before the
    # status dict assignment.
    assert "buffer_size_bytes" not in captured["status"]


def test_refresh_config_status_skips_iceberg_keys_when_get_table_info_errors():
    """If iceberg.get_table_info returns an envelope with an
    'error' key (catalog unreachable, snapshot lock contention),
    refresh_config_status must skip the iceberg_bytes/iceberg_files
    assignment instead of pinning the cost panel to zero. Pinned
    because the /api/usage/current-storage fast-path keys on the
    presence of those fields — writing zeros would make the
    Storage cost render 0 KB until the next successful tick."""
    from backend.core._duckdb_status import refresh_config_status

    captured: dict = {}

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

    with (
        patch(
            "backend.config.load_config",
            return_value={"name": "svc", "bucket": "b", "service_id": "svc"},
        ),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.config.update_status",
            side_effect=lambda sid, status: captured.setdefault("status", status),
        ),
        patch("backend.core.duckdb.get_connection", return_value=_StubCon()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"ingested": 0, "local_rows": 0},
        ),
        patch("backend.core.duckdb.get_schema", return_value=[]),
        patch("backend.core.duckdb.update_top_values"),
        # The error envelope: iceberg signals failure via {'error': '...'}
        # rather than raising; refresh_config_status checks the key directly.
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"error": "catalog unreachable"},
        ),
    ):
        refresh_config_status("svc")

    status = captured["status"]
    assert "iceberg_bytes" not in status, (
        "error envelope from get_table_info must skip iceberg_bytes — "
        "writing 0 would corrupt the storage cost panel until next tick"
    )
    assert "iceberg_files" not in status


# ── enrich_asn_labels ────────────────────────────────────────────────


def test_enrich_asn_labels_formats_digit_strings_via_get_asn_names():
    """Digit-string values get an "Owner (NNNN)" label sourced
    from get_asn_names. Pinned because the dashboard ASN dropdown
    relies on the formatted label for user-readable rows."""
    from backend.core._duckdb_status import enrich_asn_labels

    values = [{"value": "7922"}, {"value": "15169"}]

    with patch(
        "backend.core.duckdb.get_asn_names",
        return_value={7922: "Comcast Cable", 15169: "Google"},
    ):
        out = enrich_asn_labels(values, "svc-1")

    assert out is values  # same-list reference contract
    assert values[0]["label"] == "Comcast Cable (7922)"
    assert values[1]["label"] == "Google (15169)"


def test_enrich_asn_labels_mixed_digits_only_enriches_digit_entries():
    """When the list mixes digit and non-digit values, only the
    digit entries acquire a 'label' key. Pinned because the
    filter-picker hands the function bot/country/asn results in
    a single batch and accidentally tagging a non-digit row as
    AS<garbage> would crash format_asn_label."""
    from backend.core._duckdb_status import enrich_asn_labels

    values = [
        {"value": "7922"},
        {"value": "GoogleBot"},
        {"value": "15169"},
        {"value": "country-code"},
    ]

    with patch(
        "backend.core.duckdb.get_asn_names",
        return_value={7922: "Comcast Cable", 15169: "Google"},
    ):
        enrich_asn_labels(values, "svc-1")

    assert values[0]["label"] == "Comcast Cable (7922)"
    assert "label" not in values[1]
    assert values[2]["label"] == "Google (15169)"
    assert "label" not in values[3]


# ── get_sync_status branches (round 2) ──────────────────────────────


def test_get_sync_status_unconfigured_short_circuits_with_defaults(in_memory_duckdb):
    """An unconfigured source must return the canonical empty
    envelope WITHOUT touching DuckDB or S3 — the dashboard's first
    poll on a fresh deploy depends on this to render the 'configure
    first' state instead of a 500."""
    from backend.core._duckdb_status import get_sync_status

    src = {"name": "no-svc"}  # missing endpoint/keys/bucket → is_configured False

    out = get_sync_status(in_memory_duckdb, src)

    assert out == {
        "configured": False,
        "local_rows": 0,
        "ingested": 0,
        "fos_total": 0,
        "storage_mode": "cloud",
        "access_level": "read_write",
    }


def test_get_sync_status_returns_cached_when_skip_fos_and_cache_present(in_memory_duckdb, fos_source):
    """skip_fos=True with a cached status dict in config returns
    the cache verbatim (plus the three runtime overrides) and never
    runs the iceberg view or metadata_db calls. Pinned because the
    header polls this every 5 s and the cache-hit path is the only
    thing keeping that off the analytic engine."""
    from backend.core._duckdb_status import get_sync_status

    cached = {"local_rows": 12345, "ingested": 99, "latest_log_at": "2026-06-15T00:00:00Z"}

    with patch("backend.config.get_status", return_value=cached):
        out = get_sync_status(in_memory_duckdb, fos_source, skip_fos=True, force=False)

    # Runtime overrides land on TOP of the cached dict.
    assert out["local_rows"] == 12345
    assert out["ingested"] == 99
    assert out["access_level"] == "read_write"
    assert out["storage_mode"] == "cloud"
    assert out["configured"] is True


def test_get_sync_status_recovers_from_stale_catalog_via_iceberg_rebuild(in_memory_duckdb, fos_source):
    """When the inner stats query raises 'Catalog Error: Table
    with name foo does not exist' (stale view, parquet unlinked
    by commit), get_sync_status must clear iceberg caches, call
    update_iceberg_view, and re-run the stats query. Pinned
    because the recovery path is the only thing keeping the
    header counter alive during the post-commit window."""
    # Real table to satisfy the retry query after the simulated rebuild.
    from backend.core import duckdb as _db
    from backend.core._duckdb_status import get_sync_status

    table_name = _db._safe_table_name(fos_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP)")
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES ('2026-06-15 00:00:00')")
    in_memory_duckdb.execute(
        "CREATE TABLE _cron_run_log (task VARCHAR, started_at VARCHAR, "
        "duration_s DOUBLE, status VARCHAR, error_message VARCHAR, summary VARCHAR)"
    )

    state = {"stats_calls": 0}
    real_execute = in_memory_duckdb.execute

    def _patched_execute(sql, *args, **kwargs):
        if sql.startswith("SELECT count(*), min(timestamp), max(timestamp) FROM logs_test_service"):
            state["stats_calls"] += 1
            if state["stats_calls"] == 1:
                raise RuntimeError("Catalog Error: Table with name foo does not exist")
        return real_execute(sql, *args, **kwargs)

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            return _patched_execute(sql, *args, **kwargs)

    with (
        # Skip the split-stats path so the outer SELECT count(*) FROM table
        # is exercised — that's the call we want to fail.
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out = get_sync_status(_ConProxy(), fos_source, force=True)

    # Recovery branch fired exactly once.
    mock_clear.assert_called_once()
    args, kwargs = mock_clear.call_args
    assert kwargs.get("keep_snapshot_cache") is True, "must preserve snapshot cache to avoid stale empty view"
    mock_update.assert_called_once()
    # Two stats SELECTs: the failing one + the retry that succeeded.
    assert state["stats_calls"] == 2
    # The recovery branch populated local_rows from the rebuilt view.
    assert out["local_rows"] == 1


def test_get_sync_status_unknown_exception_falls_back_to_ingested_count(in_memory_duckdb, fos_source):
    """An exception that does NOT match the four 'stale cache'
    substrings ("No files found", "Catalog Error: Table with name",
    "does not exist", "No such file or directory") must NOT trigger
    the iceberg rebuild — instead the function logs a warning and
    falls back to the metadata-derived row count. Pinned because the
    rebuild path is expensive (snapshot load + view rewrite); firing
    it on every transient error would amplify any DuckDB outage."""
    from backend.core._duckdb_status import get_sync_status

    summary = {
        "file_count": 7,
        "total_rows": 42,
        "total_bytes": 1024,
        "count_with_bytes": 1,
        "last_ingested": "2026-06-15T00:00:00",
        "latest_file_name": "raw/2026-06-15T00:00:00.gz",
    }

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            # Stats query raises a non-stale error.
            if "count(*)" in sql and "timestamp" in sql:
                raise RuntimeError("connection reset by peer")

            # cron stats query: return an empty result via a stub cursor.
            class _R:
                def fetchone(self_inner):
                    return (0,)

                def fetchall(self_inner):
                    return []

            return _R()

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out = get_sync_status(_ConProxy(), fos_source, force=True)

    # Recovery branch must NOT fire on unknown errors.
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    # Falls back to metadata count.
    assert out["local_rows"] == 42
    assert out["ingested"] == 7


def test_get_sync_status_empty_view_prefers_last_known_good_local_rows(in_memory_duckdb, fos_source):
    """An empty view (0 rows, e.g. a 'WHERE false' view mid catalog-rebuild)
    must NOT degrade local_rows to the metadata sum. The metadata rollup is
    the retention-trimmed ingested_files sum (~1 day), a poor proxy for the
    parquet lake — so the fallback prefers the last-known-good persisted
    local_rows (the real parquet count from the previous successful poll) so
    the header sticks at the true value until the view recovers. Pinned: this
    is what keeps the 'Total Logs' badge honest during transient blips."""
    from backend.core._duckdb_status import get_sync_status

    # Trimmed metadata sum — deliberately different from the persisted value
    # so the assertion proves which source the fallback chose.
    summary = {
        "file_count": 3,
        "total_rows": 400,
        "total_bytes": 300,
        "count_with_bytes": 3,
        "last_ingested": "2026-06-20T00:00:00",
        "latest_file_name": "raw/2026-06-20T00:00:00.gz",
    }
    prior_status = {"local_rows": 8_090_191, "latest_log_at": "2026-06-20T14:00:00"}

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            # Stats query: empty view → 0 rows, null extents.
            if "count(*)" in sql and "timestamp" in sql:

                class _R0:
                    def fetchone(self_inner):
                        return (0, None, None)

                return _R0()

            class _R:
                def fetchone(self_inner):
                    return (0,)

                def fetchall(self_inner):
                    return []

            return _R()

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value=prior_status),
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        out = get_sync_status(_ConProxy(), fos_source, force=True)

    # Empty view is not an error → no expensive rebuild.
    mock_clear.assert_not_called()
    mock_update.assert_not_called()
    # Fallback chose the last-known-good persisted count, not the trimmed sum.
    assert out["local_rows"] == 8_090_191


def test_get_sync_status_empty_view_falls_back_to_metadata_without_prior(in_memory_duckdb, fos_source):
    """First-ever poll (no persisted local_rows yet): an empty view still
    degrades to the metadata sum so the header reads something non-zero
    rather than 0 while data exists on disk."""
    from backend.core._duckdb_status import get_sync_status

    summary = {
        "file_count": 3,
        "total_rows": 400,
        "total_bytes": 300,
        "count_with_bytes": 3,
        "last_ingested": "2026-06-20T00:00:00",
        "latest_file_name": "raw/2026-06-20T00:00:00.gz",
    }

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            if "count(*)" in sql and "timestamp" in sql:

                class _R0:
                    def fetchone(self_inner):
                        return (0, None, None)

                return _R0()

            class _R:
                def fetchone(self_inner):
                    return (0,)

                def fetchall(self_inner):
                    return []

            return _R()

    with (
        patch("backend.core._duckdb_status._data_stats_fingerprint", return_value=None),
        patch("backend.config.get_status", return_value={}),  # no prior status
        patch("backend.core.metadata.get_ingested_files_status_summary", return_value=summary),
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.iceberg.update_iceberg_view"),
    ):
        out = get_sync_status(_ConProxy(), fos_source, force=True)

    assert out["local_rows"] == 400


# ── refresh_config_status remaining branches ─────────────────────────


def test_refresh_config_status_returns_silently_when_config_missing():
    """A delete-config race or stale service_id passed by the cron
    must not raise — refresh_config_status is fired by APScheduler
    and an uncaught exception kills the entire status loop. Pinned
    because a single deleted service used to take down the cron for
    every other service in the same process."""
    from backend.core._duckdb_status import refresh_config_status

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.update_status") as mock_update,
    ):
        refresh_config_status("missing-svc")

    # Never reached update_status because load_config bailed early.
    mock_update.assert_not_called()


def test_refresh_config_status_writes_iceberg_and_edge_ratio_on_happy_path(monkeypatch, tmp_path):
    """When get_table_info succeeds and get_edge_ratio returns a
    value, both iceberg_bytes/iceberg_files AND edge_ratio land in
    the status dict. Pinned because the storage-cost panel and the
    prefill router both fast-path on the presence of these keys —
    omitting them on a successful tick would force a 3.6 s cold
    re-query on every poll."""
    from backend.core._duckdb_status import refresh_config_status

    captured: dict = {}

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

    buf_dir = tmp_path / "buf"
    buf_dir.mkdir()
    (buf_dir / "f.parquet").write_bytes(b"x" * 17)

    with (
        patch(
            "backend.config.load_config",
            return_value={"name": "svc", "bucket": "b", "service_id": "svc"},
        ),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.config.update_status",
            side_effect=lambda sid, status: captured.setdefault("status", status),
        ),
        patch("backend.core.duckdb.get_connection", return_value=_StubCon()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"ingested": 0, "local_rows": 0},
        ),
        patch("backend.core._duckdb_status.get_schema", return_value=[{"name": "ip", "type": "VARCHAR"}]),
        patch("backend.core._duckdb_status.update_top_values"),
        patch("backend.core._duckdb_status._cache_dir", return_value=str(buf_dir)),
        patch(
            "backend.core.iceberg.get_table_info",
            return_value={"size_bytes": 12345, "data_files": 7},
        ),
        patch("backend.repositories.usage.get_edge_ratio", return_value=(0.42, None)),
    ):
        refresh_config_status("svc")

    status = captured["status"]
    # Buffer walk landed.
    assert status["buffer_size_bytes"] == 17
    # Iceberg fast-path populated.
    assert status["iceberg_bytes"] == 12345
    assert status["iceberg_files"] == 7
    # Edge-ratio fast-path populated.
    assert status["edge_ratio"] == 0.42
    # Schema landed on the heavy tick.
    assert status["schema"] == [{"name": "ip", "type": "VARCHAR"}]


def test_refresh_config_status_skip_top_values_omits_schema_key():
    """include_top_values=False MUST skip the schema SUMMARIZE
    write — that's the load-bearing optimisation that lets the
    high-cadence (5s) tick avoid the ~800 ms SUMMARIZE cost.
    Pinned because re-introducing the schema fetch on every tick
    used to dominate the 5 s ingest budget on services with
    >2 k parquets."""
    from backend.core._duckdb_status import refresh_config_status

    captured: dict = {}

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

    schema_calls = {"count": 0}

    def _track_schema(*_a, **_k):
        schema_calls["count"] += 1
        return []

    with (
        patch(
            "backend.config.load_config",
            return_value={"name": "svc", "bucket": "b", "service_id": "svc"},
        ),
        patch("backend.config.config_to_source", return_value={"name": "svc", "bucket": "b"}),
        patch(
            "backend.config.update_status",
            side_effect=lambda sid, status: captured.setdefault("status", status),
        ),
        patch("backend.core.duckdb.get_connection", return_value=_StubCon()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"ingested": 0, "local_rows": 0},
        ),
        patch("backend.core._duckdb_status.get_schema", side_effect=_track_schema),
        patch("backend.core._duckdb_status.update_top_values") as mock_tv,
    ):
        refresh_config_status("svc", include_top_values=False)

    # Schema key MUST be absent on the light tick.
    assert "schema" not in captured["status"]
    # update_top_values must not run either.
    mock_tv.assert_not_called()
    # And get_schema must not have been called by the refresh path.
    assert schema_calls["count"] == 0


# ── delete_ingested_files remaining paths ────────────────────────────


def test_delete_ingested_files_multi_pass_deletes_then_verifies_clean(s3_mock, fos_source, in_memory_duckdb):
    """The 3-pass loop: pass 1 finds + deletes the intersection,
    pass 2 sees an empty intersection and emits the
    'Verification complete' message. Pinned because the SSE
    contract surfaces the verification line to the admin UI —
    losing the second-pass message would leave the banner showing
    'in progress' indefinitely after a clean run."""
    from backend.core._duckdb_status import delete_ingested_files

    bucket = fos_source["bucket"]
    keys = [f"raw/2026-06-15/00/file-{i:02d}.gz" for i in range(3)]
    for key in keys:
        s3_mock.put_object(Bucket=bucket, Key=key, Body=b"{}")

    # First pass: glob returns all 3 keys, metadata reports all 3 ingested.
    # Second pass: glob returns nothing (we deleted them); intersection empty,
    # take the "Verification complete" branch and break.
    s3_paths = [f"s3://{bucket}/{k}" for k in keys]
    glob_responses = [
        [(p,) for p in s3_paths],  # pass 1
        [],  # pass 2: bucket emptied
    ]

    class _Cursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def _fake_retry(con, query, **_):
        return _Cursor(glob_responses.pop(0))

    with (
        patch("backend.core._duckdb_status._execute_query_with_retry", side_effect=_fake_retry),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set(s3_paths)),
        # Skip the eventual-consistency sleep so the test runs fast.
        patch("backend.core._duckdb_status.time.sleep"),
    ):
        events = list(delete_ingested_files(in_memory_duckdb, fos_source))

    # Pass 1: status (checking) → status (deleting N) → progress → pass 2 status (checking) → status (verified) → done
    messages = [e.get("message", "") for e in events]
    assert any("Pass 1/3" in m and "Checking" in m for m in messages)
    assert any("Deleting 3 files" in m for m in messages)
    assert any("Pass 2/3" in m and "Checking" in m for m in messages)
    assert any("Verification complete" in m for m in messages), (
        f"second-pass clean branch must emit the verification message; got {messages}"
    )
    done = events[-1]
    assert done["type"] == "done"
    assert done["deleted_files"] == 3
    # All keys bulk-deleted from moto.
    remaining = s3_mock.list_objects_v2(Bucket=bucket).get("KeyCount", 0)
    assert remaining == 0


def test_delete_ingested_files_first_pass_finds_nothing_emits_no_files(in_memory_duckdb, fos_source):
    """If pass 1's glob returns zero ingested files, emit the
    'No ingested files found to delete.' status (NOT the
    'Verification complete' variant, which is reserved for later
    passes) and break out. Pinned because the admin UI shows the
    pass-1 message verbatim — using the verify message at pass 1
    would mislead the user into thinking work was done."""
    from backend.core._duckdb_status import delete_ingested_files

    class _Cursor:
        def fetchall(self):
            return []

    with (
        patch("backend.core._duckdb_status._execute_query_with_retry", return_value=_Cursor()),
        patch("backend.core.metadata.get_ingested_filenames", return_value=set()),
    ):
        events = list(delete_ingested_files(in_memory_duckdb, fos_source))

    messages = [e.get("message", "") for e in events]
    assert any("No ingested files found to delete" in m for m in messages)
    # Must NOT use the verification message on pass 1.
    assert not any("Verification complete" in m for m in messages)
    done = events[-1]
    assert done["type"] == "done"
    assert done["deleted_files"] == 0


# ── get_schema branches ──────────────────────────────────────────────


def test_get_schema_returns_empty_when_table_missing(in_memory_duckdb, fos_source):
    """If the information_schema check shows no rows, get_schema
    must return [] WITHOUT attempting SUMMARIZE (which would
    fail with 'table does not exist' and waste a recovery cycle).
    Pinned because every fresh service hits this path on first
    poll before ingest creates the table."""
    from backend.core._duckdb_status import _schema_cache, get_schema

    # Drain cache so we hit the live query branch.
    _schema_cache.clear()

    out = get_schema(in_memory_duckdb, fos_source)
    assert out == []


def test_get_schema_returns_cached_within_ttl(in_memory_duckdb, fos_source):
    """A second call within _SCHEMA_CACHE_TTL must return the
    cached list without touching DuckDB. Pinned because the
    /schema endpoint fires on every page load and a cache miss
    triggers an 800 ms SUMMARIZE — the TTL gate is load-bearing
    for sub-second page renders."""
    from backend.core import _duckdb_status
    from backend.core._duckdb_status import _schema_cache, get_schema

    _schema_cache.clear()
    table_name = "logs_test_service"
    fake_schema = [{"name": "ip", "type": "VARCHAR"}]
    # Seed cache with a recent timestamp.
    import time as _t

    _schema_cache[(fos_source["name"], table_name, True)] = (_t.time(), fake_schema)

    state = {"executes": 0}

    class _ConProxy:
        def execute(self, *_a, **_k):
            state["executes"] += 1

            class _R:
                def fetchone(self_inner):
                    return (0,)

                def fetchall(self_inner):
                    return []

            return _R()

    out = get_schema(_ConProxy(), fos_source)
    assert out is fake_schema
    # Zero executes — the TTL-gated cache hit short-circuited.
    assert state["executes"] == 0
    _ = _duckdb_status  # silence ruff


def test_get_schema_falls_back_to_describe_when_summarize_errors(in_memory_duckdb, fos_source):
    """SUMMARIZE can fail on tables with exotic column types
    (e.g. lists of structs older DuckDB versions choke on); the
    fallback DESCRIBE path returns a stripped schema (name + type
    only) so the filter-picker still has something to render.
    Pinned because losing the fallback would break the entire
    /schema endpoint on a single bad column type."""
    from backend.core._duckdb_status import _schema_cache, get_schema

    _schema_cache.clear()

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            # information_schema check → table exists.
            if "information_schema.tables" in sql:

                class _R1:
                    def fetchone(self_inner):
                        return (1,)

                return _R1()
            # SUMMARIZE raises.
            if sql.startswith("SUMMARIZE"):
                raise RuntimeError("Unsupported column type LIST(STRUCT)")
            # DESCRIBE succeeds with minimal rows.
            if sql.startswith("DESCRIBE"):

                class _R2:
                    def fetchall(self_inner):
                        return [
                            ("ip", "VARCHAR", None, None, None, None),
                            ("status", "VARCHAR", None, None, None, None),
                        ]

                return _R2()
            raise AssertionError(f"unexpected SQL: {sql}")

    out = get_schema(_ConProxy(), fos_source)
    assert out == [{"name": "ip", "type": "VARCHAR"}, {"name": "status", "type": "VARCHAR"}]


def test_get_schema_returns_empty_when_describe_also_fails(in_memory_duckdb, fos_source):
    """When both SUMMARIZE and DESCRIBE fail (table corruption,
    disk full mid-statement), get_schema must return [] rather
    than raising. Pinned because the schema endpoint MUST stay
    up — UI degrades to 'no filters available' but doesn't 500."""
    from backend.core._duckdb_status import _schema_cache, get_schema

    _schema_cache.clear()

    class _ConProxy:
        def execute(self, sql, *args, **kwargs):
            if "information_schema.tables" in sql:

                class _R1:
                    def fetchone(self_inner):
                        return (1,)

                return _R1()
            # Both SUMMARIZE and DESCRIBE raise.
            raise RuntimeError("disk full")

    out = get_schema(_ConProxy(), fos_source)
    assert out == []


# ── get_asn_names / format_asn_label branches ────────────────────────


def test_get_asn_names_empty_input_short_circuits():
    """Empty list or empty service_id must return {} WITHOUT
    touching the metadata cache or cymruwhois — both are
    network-shaped and shouldn't fire on a no-op call."""
    from backend.core._duckdb_status import get_asn_names

    assert get_asn_names("svc", []) == {}
    assert get_asn_names("", [7922]) == {}


def test_get_asn_names_cache_hit_skips_cymruwhois():
    """When the SQLite cache covers every requested ASN, the
    cymruwhois client MUST NOT be constructed — that's a DNS
    round-trip and a steady-state autocomplete tick must stay
    on the local cache. Pinned because the ASN dropdown polls
    every keypress."""
    from backend.core._duckdb_status import get_asn_names

    with (
        patch(
            "backend.core.metadata.lookup_asn_names",
            return_value={7922: "Comcast Cable", 15169: "Google"},
        ),
        patch("backend.core.metadata.upsert_asn_names") as mock_upsert,
    ):
        out = get_asn_names("svc", [7922, 15169])

    assert out == {7922: "Comcast Cable", 15169: "Google"}
    # No upsert because cymruwhois never produced new results.
    mock_upsert.assert_not_called()


def test_format_asn_label_handles_unknown_and_known():
    """An empty/AS<digits> name falls back to the raw 'AS<n>'
    form; a real owner string becomes 'Owner (n)'. Pinned
    because the dashboard ASN column relies on both forms."""
    from backend.core._duckdb_status import format_asn_label

    assert format_asn_label(7922, "Comcast Cable") == "Comcast Cable (7922)"
    assert format_asn_label(7922, "") == "AS7922"
    assert format_asn_label(7922, "AS7922") == "AS7922"


# ── usage-log helper guards ──────────────────────────────────────────


def test_reconcile_fastly_stats_returns_zero_when_usage_logging_disabled():
    """Reconciliation must no-op when global usage logging is
    off — otherwise we'd hit Fastly's /stats/aggregate every
    tick on customers who opted out of the cost panel."""
    from backend.core._duckdb_status import reconcile_fastly_stats

    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        out = reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"})
    assert out == 0


def test_reconcile_fastly_stats_returns_zero_when_no_logging_service_id():
    """Without a logging_service_id we have no API path to call;
    the function must early-return 0 instead of throwing a
    KeyError or building a malformed URL."""
    from backend.core._duckdb_status import reconcile_fastly_stats

    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        out = reconcile_fastly_stats({"name": "svc"})  # no logging_service_id
    assert out == 0


def test_reconcile_fastly_stats_returns_zero_when_no_api_key():
    """If the API key lookup returns empty (key revoked, config
    drift), the function must skip the HTTP call. Pinned because
    a missing key historically surfaced as a 401 on every cron
    tick and spammed the error log."""
    from backend.core._duckdb_status import reconcile_fastly_stats

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.get_fastly_api_key", return_value=""),
    ):
        out = reconcile_fastly_stats({"name": "svc", "logging_service_id": "lsi"})
    assert out == 0


def test_purge_usage_log_no_service_id_returns_silently():
    """A source dict with neither name nor service_id must NOT
    crash purge_usage_log — the function is called from the
    cron loop and a single misconfigured source would otherwise
    take down the entire retention sweep."""
    from backend.core._duckdb_status import purge_usage_log

    with (
        patch(
            "backend.config.load_usage_logging_config",
            return_value={"retention_days": 30},
        ),
        patch("backend.core.metadata.purge_usage_log") as mock_purge,
    ):
        purge_usage_log({})  # no name + no service_id

    mock_purge.assert_not_called()


def test_update_cron_duration_skips_when_source_has_no_id():
    """update_cron_duration is called from multiple post-ingest
    branches; if the source dict is missing both name and
    service_id, it must early-return rather than write a
    duration into the wrong row."""
    from backend.core._duckdb_status import update_cron_duration

    with patch("backend.core.metadata.update_cron_duration") as mock_upd:
        update_cron_duration({}, run_id=1, duration_s=0.5)

    mock_upd.assert_not_called()


def test_backfill_fastly_edge_writes_returns_zero_when_usage_logging_disabled():
    """The backfill must skip cleanly when global usage logging
    is off — otherwise it would read ingested-files metadata on
    every cron tick for nothing."""
    from backend.core._duckdb_status import backfill_fastly_edge_writes

    with patch("backend.config.is_usage_logging_enabled", return_value=False):
        out = backfill_fastly_edge_writes({"name": "svc"})
    assert out == 0


def test_backfill_fastly_edge_writes_returns_zero_when_source_has_no_id():
    """A source dict missing both name and service_id must skip
    cleanly — the function is called from post-ingest paths and
    must NOT raise."""
    from backend.core._duckdb_status import backfill_fastly_edge_writes

    with patch("backend.config.is_usage_logging_enabled", return_value=True):
        out = backfill_fastly_edge_writes({})
    assert out == 0


def test_backfill_fastly_edge_writes_synthesises_one_row_per_unbackfilled_file():
    """The happy path: pulls unbackfilled files from metadata_db,
    parses the timestamp from the filename, and writes one
    PUT_OBJECT/Class A row per file via log_synthetic_usage.
    Pinned because the cost-panel Class A tally is the only
    metric the cron tick can produce — losing this synthesis
    leaves the dashboard showing 0 ops/hour for FOS writes."""
    from backend.core._duckdb_status import backfill_fastly_edge_writes

    files = [
        ("raw/2026-06-15T00:00:00Z/file.gz", "2026-06-15T00:00:00", 100, 5000),
        ("__seeding_attempted__", "2026-06-15T00:00:00", 0, 0),  # filtered
        ("no-timestamp.gz", "2026-06-15T00:00:00", 50, 2500),  # uses f_ingested fallback
    ]

    captured: dict = {}

    def _fake_log(service_id, calls):
        captured["calls"] = calls
        captured["service_id"] = service_id
        return len(calls)

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=files),
        patch("backend.core.metadata.log_synthetic_usage", side_effect=_fake_log),
    ):
        out = backfill_fastly_edge_writes({"name": "svc"})

    # 2 rows synthesised (the __seeding_attempted__ sentinel is filtered).
    assert out == 2
    assert captured["service_id"] == "svc"
    assert len(captured["calls"]) == 2
    # First row: timestamp parsed from filename → '...Z' suffix appended.
    assert captured["calls"][0]["path"] == "raw/2026-06-15T00:00:00Z/file.gz"
    assert captured["calls"][0]["method"] == "PUT_OBJECT"
    assert captured["calls"][0]["service"] == "FOS"
    assert captured["calls"][0]["_timestamp_override"] == "2026-06-15T00:00:00Z"
    # Second row: no timestamp in name → uses f_ingested verbatim.
    assert captured["calls"][1]["_timestamp_override"] == "2026-06-15T00:00:00"


def test_backfill_fastly_edge_writes_returns_zero_when_no_pending_files():
    """When the metadata query returns an empty list, skip the
    synthesis entirely. Pinned because the call site is on the
    hot post-ingest path and a no-op tick must NOT touch the
    usage_log INSERT."""
    from backend.core._duckdb_status import backfill_fastly_edge_writes

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.list_unbackfilled_fastly_edge_files", return_value=[]),
        patch("backend.core.metadata.log_synthetic_usage") as mock_log,
    ):
        out = backfill_fastly_edge_writes({"name": "svc"})

    assert out == 0
    mock_log.assert_not_called()


def test_log_usage_calls_skips_when_global_usage_logging_disabled():
    """log_usage_calls must early-return when usage logging is
    globally off — calls from every analytic router check this
    and an unconditional write would explode the SQLite db on
    customers who opted out."""
    from backend.core._duckdb_status import log_usage_calls

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET", "path": "/foo"}])

    mock_log.assert_not_called()


def test_log_usage_calls_forwards_to_metadata_db_when_enabled():
    """The happy path: when usage logging is enabled and a
    service_id is present, forward the calls to metadata_db
    with the process_context."""
    from backend.core._duckdb_status import log_usage_calls

    captured: dict = {}

    def _capture(service_id, calls, process_context=None):
        captured["sid"] = service_id
        captured["calls"] = calls
        captured["ctx"] = process_context

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        log_usage_calls({"name": "svc"}, [{"method": "GET"}], process_context="dashboard")

    assert captured == {"sid": "svc", "calls": [{"method": "GET"}], "ctx": "dashboard"}


def test_update_cron_duration_forwards_to_metadata_db_when_id_present():
    """The happy path: a populated source dict must forward to
    metadata_db with the run_id, duration, and optional log
    output. Pinned because the post-ingest phase calls this
    after every sync to keep the cron-runs panel accurate."""
    from backend.core._duckdb_status import update_cron_duration

    captured: dict = {}

    def _capture(service_id, run_id, duration_s, log_output=None):
        captured.update({"sid": service_id, "rid": run_id, "dur": duration_s, "log": log_output})

    with patch("backend.core.metadata.update_cron_duration", side_effect=_capture):
        update_cron_duration({"name": "svc"}, run_id=42, duration_s=1.25, log_output="ok")

    assert captured == {"sid": "svc", "rid": 42, "dur": 1.25, "log": "ok"}


def test_purge_usage_log_forwards_retention_to_metadata_db():
    """Happy path: load the retention_days from config, then call
    metadata_db.purge_usage_log with the service_id + retention."""
    from backend.core._duckdb_status import purge_usage_log

    captured: dict = {}

    def _capture(service_id, retention_days):
        captured.update({"sid": service_id, "retention": retention_days})

    with (
        patch(
            "backend.config.load_usage_logging_config",
            return_value={"retention_days": 7},
        ),
        patch("backend.core.metadata.purge_usage_log", side_effect=_capture),
    ):
        purge_usage_log({"name": "svc"})

    assert captured == {"sid": "svc", "retention": 7}


# Silence ruff unused-imports
_ = MagicMock
_ = pytest
_ = os
