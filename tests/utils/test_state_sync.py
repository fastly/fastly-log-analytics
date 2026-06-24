"""Tests for admin state export/import.

``state_sync`` mirrors the *admin-managed* per-service state
(audit log, saved views, custom fields) into a JSON blob in FOS so
read-only analyst replicas can pick it up via ``import_admin_state``.
It explicitly does NOT include alerts (those are per-service
operational state, not config) — the file's namesake test pins that.

The other branches matter for the analyst flow:
  - export skips read-only sources (analyst pods don't write back)
  - import silently no-ops on 404 / NoSuchKey (cold-start scenario)
  - import merges custom_fields into the local config so the
    analyst's catalog matches what admin defined
"""

import json
import urllib.error
from importlib import reload
from unittest.mock import MagicMock, patch

import backend.state_sync as state_sync_module


def _make_s3_mock():
    s3 = MagicMock()
    s3.put_object = MagicMock()
    return s3


def test_export_does_not_include_alerts():
    """export_admin_state must not include _alerts in the uploaded JSON."""
    source = {
        "name": "svc1",
        "bucket": "bkt",
        "prefix": "",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_level": "read_write",
    }

    s3_mock = _make_s3_mock()
    cfg_mock = {"log_fields": {"schema_version": 2, "custom_fields": []}}

    captured_body: dict = {}

    def capture_put(**kwargs):
        captured_body.update(json.loads(kwargs["Body"].decode("utf-8")))

    s3_mock.put_object.side_effect = capture_put

    with (
        patch("backend.state_sync.get_source_for_service", return_value=source),
        patch("backend.state_sync._get_fos_client", return_value=s3_mock),
        patch("backend.state_sync.svcconfig.load_config", return_value=cfg_mock),
        # The metadata_db autouse fixture isolates per-service SQLite to tmp_path,
        # so list_views / export_audit / replace_audit_for_service all hit a fresh empty file.
    ):
        from backend.state_sync import export_admin_state

        export_admin_state("svc1")

    assert "_alerts" not in captured_body, "_alerts should not be exported to admin_state.json"
    assert "_views" in captured_body
    assert "_audit_logs" in captured_body
    assert "custom_fields" in captured_body


def test_import_ignores_alerts_key():
    """import_admin_state silently ignores any _alerts key in the state file."""
    source = {
        "name": "svc1",
        "bucket": "bkt",
        "prefix": "",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
    }

    legacy_state = json.dumps(
        {
            "_views": [],
            "_audit_logs": [],
            "log_format_history": [],
            "custom_fields": [],
            "_alerts": [{"id": "a1", "name": "Old alert"}],
        }
    ).encode("utf-8")

    s3_mock = MagicMock()
    s3_mock.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=legacy_state))}
    s3_mock.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    cfg_mock = {"log_fields": {"schema_version": 2, "custom_fields": []}}

    with (
        patch("backend.state_sync.get_source_for_service", return_value=source),
        patch("backend.state_sync._get_fos_client", return_value=s3_mock),
        patch("backend.state_sync.svcconfig.load_config", return_value=cfg_mock),
        patch("backend.state_sync.svcconfig.save_config"),
    ):
        reload(state_sync_module)
        state_sync_module.import_admin_state("svc1")

    # Alerts table must remain untouched — verify by querying the per-service SQLite directly.
    from backend.core import metadata as metadata_db

    alerts = metadata_db.list_alerts("svc1")
    assert alerts == [], "import_admin_state should not write to the alerts table"


# ── get_admin_state_key: key construction ────────────────────────────────────


def test_admin_state_key_with_empty_prefix():
    """Bare prefix → ``iceberg/meta/admin_state.json`` (no leading slash).
    Pinned because a leading slash breaks S3 listing semantics."""
    from backend.state_sync import get_admin_state_key

    key = get_admin_state_key({"prefix": ""})
    assert key == "iceberg/meta/admin_state.json"


def test_admin_state_key_with_prefix_strips_slashes():
    """``/customer1/`` → ``customer1/iceberg/...`` (slashes stripped
    from both ends). Prevents double-slash keys when admins paste a
    prefix with trailing slashes."""
    from backend.state_sync import get_admin_state_key

    key = get_admin_state_key({"prefix": "/customer1/"})
    assert key == "customer1/iceberg/meta/admin_state.json"


# ── export_admin_state: early-return branches ────────────────────────────────


def _src(**overrides) -> dict:
    base = {
        "name": "svc1",
        "bucket": "bkt",
        "prefix": "",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_level": "read_write",
    }
    base.update(overrides)
    return base


def test_export_skips_when_source_is_read_only():
    """Read-only sources (analyst replicas) MUST NOT write back —
    otherwise an analyst pod would clobber the admin's state."""
    s3 = MagicMock()
    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src(access_level="read_only")),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import export_admin_state

        export_admin_state("svc1")

    s3.put_object.assert_not_called()


def test_export_skips_when_source_not_found():
    """Unknown service id → no-op. The frontend may call sync for a
    service that was just torn down; we must not raise."""
    s3 = MagicMock()
    with (
        patch("backend.state_sync.get_source_for_service", return_value=None),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import export_admin_state

        export_admin_state("ghost-svc")

    s3.put_object.assert_not_called()


def test_export_swallows_unexpected_exceptions():
    """Any error during export (S3 down, bad config) must be caught and
    logged — export is best-effort, called from request handlers that
    shouldn't 500 because the state mirror failed."""
    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", side_effect=RuntimeError("S3 broken")),
    ):
        from backend.state_sync import export_admin_state

        # Must not raise
        export_admin_state("svc1")


def test_export_omits_custom_fields_when_config_missing():
    """If ``load_config`` returns None (service was torn down between
    the source lookup and config load), the export still uploads —
    just without the custom_fields key."""
    s3 = MagicMock()
    captured: dict = {}

    def capture_put(**kwargs):
        captured.update(json.loads(kwargs["Body"].decode("utf-8")))

    s3.put_object.side_effect = capture_put

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
        patch("backend.state_sync.svcconfig.load_config", return_value=None),
    ):
        from backend.state_sync import export_admin_state

        export_admin_state("svc1")

    s3.put_object.assert_called_once()
    assert "custom_fields" not in captured
    assert "_views" in captured  # but the other state IS exported


# ── _cdn_get: URL construction + telemetry ───────────────────────────────────


def test_cdn_get_builds_url_with_secret_query_param():
    src = _src(cdn_url="https://cdn-test.fastly.net/", cdn_secret="topsecret")

    fake_resp = MagicMock()
    fake_resp.read.return_value = b"payload"
    fake_resp.headers = {}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm) as mock_open,
        patch("backend.utils.telemetry.record_cdn_call"),
    ):
        from backend.state_sync import _cdn_get

        body = _cdn_get(src, "iceberg/meta/admin_state.json")

    assert body == b"payload"
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://cdn-test.fastly.net/iceberg/meta/admin_state.json?key=topsecret"


def test_cdn_get_records_telemetry_with_byte_count():
    """Every CDN hit must record a `record_cdn_call` entry so the usage
    page attributes the bytes — drift here under-reports egress."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    payload = b"x" * 4096

    fake_resp = MagicMock()
    fake_resp.read.return_value = payload
    fake_resp.headers = {}
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False

    with (
        patch("urllib.request.urlopen", return_value=cm),
        patch("backend.utils.telemetry.record_cdn_call") as mock_record,
    ):
        from backend.state_sync import _cdn_get

        _cdn_get(src, "some/key.json")

    mock_record.assert_called_once()
    assert mock_record.call_args.kwargs["bytes_count"] == 4096
    assert mock_record.call_args.kwargs["caller"] == "state_sync._cdn_get"


# ── import_admin_state: branch coverage ──────────────────────────────────────


def test_import_skips_when_source_not_found():
    """Unknown id → silent no-op (same shape as export)."""
    with patch("backend.state_sync.get_source_for_service", return_value=None):
        from backend.state_sync import import_admin_state

        import_admin_state("ghost")  # must not raise


def test_import_s3_path_silently_returns_on_no_such_key():
    """First sync ever for a service → admin_state.json doesn't exist
    → ``NoSuchKey``. Must be treated as "nothing to import" and return,
    not surface as an error."""
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey
    s3.get_object.side_effect = _NoSuchKey("missing")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")  # must not raise


def test_import_cdn_path_silently_returns_on_404():
    """CDN variant of the cold-start case: 404 → silent return.
    Anything else from the CDN re-raises into the outer except so
    transient issues are logged."""
    src = _src(cdn_url="https://cdn-test.fastly.net")

    not_found = urllib.error.HTTPError(
        url="https://cdn-test.fastly.net/missing",
        code=404,
        msg="Not Found",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", side_effect=not_found),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")  # must not raise


def test_import_cdn_path_swallows_non_404_errors():
    """A 500/503 from the CDN must be caught by the outer try (the
    inner re-raises, the outer logs+returns) — same best-effort
    contract as export."""
    src = _src(cdn_url="https://cdn-test.fastly.net")

    server_err = urllib.error.HTTPError(
        url="https://cdn-test.fastly.net/key",
        code=503,
        msg="Service Unavailable",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", side_effect=server_err),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")  # must not raise


def test_import_cdn_path_parses_payload_and_calls_metadata_writes():
    """Happy CDN path: body parsed, audit + views replaced, no exception."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    payload = json.dumps(
        {
            "_views": [{"id": "v1", "name": "My View"}],
            "_audit_logs": [{"event_type": "test"}],
        }
    ).encode("utf-8")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", return_value=payload),
        patch("backend.state_sync.svcconfig.load_config", return_value=None),
        patch("backend.core.metadata.replace_audit_for_service") as mock_audit,
        patch("backend.core.metadata.replace_views_for_service") as mock_views,
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    mock_audit.assert_called_once()
    mock_views.assert_called_once()
    assert mock_views.call_args[0][1] == [{"id": "v1", "name": "My View"}]


def test_import_merges_custom_fields_into_local_config():
    """When the synced state has ``custom_fields``, the import path
    must merge them into the local service config so the analyst's
    field catalog matches admin's definitions."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    payload = json.dumps(
        {
            "_views": [],
            "_audit_logs": [],
            "custom_fields": [{"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True}],
        }
    ).encode("utf-8")

    starting_cfg = {"log_fields": {"schema_version": 2, "custom_fields": []}}

    saved: dict = {}

    def capture_save(svc, cfg):
        saved["service_id"] = svc
        saved["cfg"] = cfg

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", return_value=payload),
        patch("backend.state_sync.svcconfig.load_config", return_value=starting_cfg),
        patch("backend.state_sync.svcconfig.save_config", side_effect=capture_save),
        patch("backend.core.metadata.replace_audit_for_service"),
        patch("backend.core.metadata.replace_views_for_service"),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    assert saved["service_id"] == "svc1"
    assert saved["cfg"]["log_fields"]["custom_fields"] == [
        {"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True}
    ]


def test_import_skips_custom_fields_merge_when_local_config_missing():
    """If ``load_config`` returns None mid-import (service torn down),
    the custom_fields merge is skipped — must not raise."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    payload = json.dumps(
        {
            "_views": [],
            "_audit_logs": [],
            "custom_fields": [{"name": "x", "duckdb_type": "VARCHAR", "enabled": True}],
        }
    ).encode("utf-8")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", return_value=payload),
        patch("backend.state_sync.svcconfig.load_config", return_value=None),
        patch("backend.state_sync.svcconfig.save_config") as mock_save,
        patch("backend.core.metadata.replace_audit_for_service"),
        patch("backend.core.metadata.replace_views_for_service"),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    mock_save.assert_not_called()


# ── Regression: scoring custom_fields must survive import_admin_state ────────


def test_import_reinjects_scoring_fields_when_scoring_enabled():
    """REGRESSION: 2026-06-02 prod incident — a stale admin_state.json in
    FOS (last written before scoring was enabled) had custom_fields=[],
    and the metadata_sync cron on the GCE read_only backend called
    ``import_admin_state`` every ~30s, silently overwriting the local
    config's scoring custom_fields with []. This made the ingest path
    drop the scoring columns from its read_json_auto columns spec, so
    every new parquet row had edge_score / edge_sid / etc. NULL — even
    though Fastly was still emitting the values correctly.

    The fix: ``import_admin_state`` MERGES rather than overwrites. When
    ``cfg["scoring"]["enabled"]`` is True, the canonical entries from
    ``_SCORING_CUSTOM_FIELDS`` are always re-added after merge. This test
    pins the merge semantics so the bug can't silently regress."""
    src = _src(cdn_url="https://cdn-test.fastly.net")

    # Remote admin_state.json carries an UNRELATED custom_field but no
    # scoring entries (the stale-pre-scoring file shape from the incident).
    remote_payload = json.dumps(
        {
            "_views": [],
            "_audit_logs": [],
            "custom_fields": [
                {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        }
    ).encode("utf-8")

    # Local cfg has scoring enabled. The 6 fields are already on disk
    # (added by enable_scoring → _add_scoring_custom_fields). The test
    # verifies that after import, those 6 + the unrelated remote field
    # are all present.
    cfg_mock = {
        "scoring": {"enabled": True, "scoring_service_id": "scorer-svc"},
        "log_fields": {
            "schema_version": 2,
            "custom_fields": [
                # Pretend these were locally injected by enable_scoring.
                # The merge should preserve them via re-injection from code.
                {"name": "edge_score", "duckdb_type": "INTEGER", "enabled": True},
            ],
        },
    }

    saved = {}

    def capture_save(service_id, cfg):
        saved["cfg"] = cfg

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", return_value=remote_payload),
        patch("backend.state_sync.svcconfig.load_config", return_value=cfg_mock),
        patch("backend.state_sync.svcconfig.save_config", side_effect=capture_save),
        patch("backend.core.metadata.replace_audit_for_service"),
        patch("backend.core.metadata.replace_views_for_service"),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

    saved_fields = saved["cfg"]["log_fields"]["custom_fields"]
    saved_names = {cf["name"] for cf in saved_fields}

    # All 6 scoring fields must be present after import.
    for name in _SCORING_FIELD_NAMES:
        assert name in saved_names, f"scoring field {name!r} missing after import_admin_state"

    # And the unrelated remote field must also have made it through.
    assert "my_custom" in saved_names, "non-scoring remote custom_field was wrongly stripped"


def test_get_scoring_matrix_key_construction():
    """scoring_matrix.json lives next to admin_state.json under iceberg/meta/."""
    from backend.state_sync import get_scoring_matrix_key

    assert get_scoring_matrix_key({"prefix": ""}) == "iceberg/meta/scoring_matrix.json"
    assert get_scoring_matrix_key({"prefix": "/c1/"}) == "c1/iceberg/meta/scoring_matrix.json"


def test_publish_matrix_skips_on_read_only_source():
    """Same guard shape as export_admin_state — analyst pods must NOT
    write back, otherwise a read_only host could clobber the admin host's
    matrix with stale data."""
    s3 = MagicMock()
    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src(access_level="read_only")),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import publish_matrix_to_fos

        publish_matrix_to_fos("svc1", {"version": "v"})
    s3.put_object.assert_not_called()


def test_publish_matrix_uploads_to_iceberg_meta():
    s3 = MagicMock()
    captured: dict = {}

    def capture_put(**kwargs):
        captured["key"] = kwargs["Key"]
        captured["body"] = json.loads(kwargs["Body"].decode("utf-8"))

    s3.put_object.side_effect = capture_put

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import publish_matrix_to_fos

        publish_matrix_to_fos("svc1", {"version": "2026-06-03-a", "counts": {"/": {"/login": 5}}})

    assert captured["key"] == "iceberg/meta/scoring_matrix.json"
    assert captured["body"]["version"] == "2026-06-03-a"


def test_fetch_matrix_returns_none_on_no_such_key():
    """First-time call: matrix hasn't been published yet → NoSuchKey
    → return None (not raise) so _load_matrix's fallback chain can move
    on to the default-empty bundled matrix."""
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey
    s3.get_object.side_effect = _NoSuchKey("missing")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import fetch_matrix_from_fos

        assert fetch_matrix_from_fos("svc1") is None


def test_import_does_not_reinject_scoring_fields_when_scoring_disabled():
    """If scoring is NOT enabled in the local cfg, ``import_admin_state``
    behaves as before: it takes the remote custom_fields verbatim. This
    pins that the re-inject path is gated on scoring.enabled and doesn't
    accidentally drag scoring fields into services that never enabled it."""
    src = _src(cdn_url="https://cdn-test.fastly.net")
    remote_payload = json.dumps(
        {
            "_views": [],
            "_audit_logs": [],
            "custom_fields": [{"name": "remote_only", "duckdb_type": "VARCHAR", "enabled": True}],
        }
    ).encode("utf-8")

    # No scoring block, or scoring.enabled=false.
    cfg_mock = {"log_fields": {"schema_version": 2, "custom_fields": []}}
    saved = {}

    def capture_save(service_id, cfg):
        saved["cfg"] = cfg

    with (
        patch("backend.state_sync.get_source_for_service", return_value=src),
        patch("backend.state_sync._cdn_get", return_value=remote_payload),
        patch("backend.state_sync.svcconfig.load_config", return_value=cfg_mock),
        patch("backend.state_sync.svcconfig.save_config", side_effect=capture_save),
        patch("backend.core.metadata.replace_audit_for_service"),
        patch("backend.core.metadata.replace_views_for_service"),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    saved_names = {cf["name"] for cf in saved["cfg"]["log_fields"]["custom_fields"]}
    assert saved_names == {"remote_only"}, "scoring fields should NOT be injected when scoring is disabled"


# ── Matrix history: key construction, listing, restore, snapshot-before-overwrite


def test_get_scoring_matrix_history_key_with_empty_prefix():
    """Historical matrices live next to scoring_matrix.json under a
    scoring_matrix_history/ subdir keyed by version string."""
    from backend.state_sync import get_scoring_matrix_history_key

    key = get_scoring_matrix_history_key({"prefix": ""}, "2026-06-03-a")
    assert key == "iceberg/meta/scoring_matrix_history/2026-06-03-a.json"


def test_get_scoring_matrix_history_key_strips_prefix_slashes():
    """Mirrors get_admin_state_key/get_scoring_matrix_key: slashes are
    stripped from both ends of the customer prefix to prevent double-slash
    keys when an admin pastes ``/customer1/`` in the source config."""
    from backend.state_sync import get_scoring_matrix_history_key

    key = get_scoring_matrix_history_key({"prefix": "/customer1/"}, "v7")
    assert key == "customer1/iceberg/meta/scoring_matrix_history/v7.json"


def test_list_scoring_matrix_versions_returns_empty_when_source_missing():
    """Same shape as the other matrix helpers: unknown service id → []
    (not None, not raise) so the routes layer can render an empty table."""
    with patch("backend.state_sync.get_source_for_service", return_value=None):
        from backend.state_sync import list_scoring_matrix_versions

        assert list_scoring_matrix_versions("ghost-svc") == []


def test_list_scoring_matrix_versions_sorts_descending_by_last_modified():
    """The matrix-history UI shows newest-first so the operator's most
    recent rollback target is at the top — this pins the sort direction
    so a future ``sort(... reverse=False)`` typo can't silently invert it.

    The paginator is mocked with two pages, deliberately yielding entries
    out-of-order across the two pages so the final sort (not page order)
    is what's exercised.
    """
    import datetime as _dt

    s3 = MagicMock()

    def _obj(version: str, ts_iso: str, size: int = 1024) -> dict:
        return {
            "Key": f"iceberg/meta/scoring_matrix_history/{version}.json",
            "Size": size,
            "LastModified": _dt.datetime.fromisoformat(ts_iso),
        }

    page1 = {
        "Contents": [
            _obj("v1", "2026-06-01T10:00:00+00:00"),
            _obj("v3", "2026-06-03T10:00:00+00:00"),
        ]
    }
    page2 = {
        "Contents": [
            _obj("v2", "2026-06-02T10:00:00+00:00"),
            _obj("v4", "2026-06-04T10:00:00+00:00"),
        ]
    }
    paginator = MagicMock()
    paginator.paginate.return_value = iter([page1, page2])
    s3.get_paginator.return_value = paginator

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import list_scoring_matrix_versions

        rows = list_scoring_matrix_versions("svc1")

    # The paginator must be invoked against the bucket + the history prefix.
    paginator.paginate.assert_called_once_with(Bucket="bkt", Prefix="iceberg/meta/scoring_matrix_history/")
    assert [r["version"] for r in rows] == ["v4", "v3", "v2", "v1"], (
        "list_scoring_matrix_versions must sort by last_modified DESC"
    )
    # Spot-check the shape of one row so a future refactor that drops a field
    # (e.g. size_bytes) is caught here too.
    assert rows[0]["key"] == "iceberg/meta/scoring_matrix_history/v4.json"
    assert rows[0]["size_bytes"] == 1024
    assert rows[0]["last_modified"] == "2026-06-04T10:00:00+00:00"


def test_list_scoring_matrix_versions_swallows_s3_errors():
    """Best-effort: any S3 error (auth, network, missing bucket) → []
    rather than bubbling up, because the matrix-history panel renders
    inline on the admin scoring page and shouldn't 500 it."""
    s3 = MagicMock()
    s3.get_paginator.side_effect = RuntimeError("S3 down")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import list_scoring_matrix_versions

        assert list_scoring_matrix_versions("svc1") == []


def test_restore_scoring_matrix_version_returns_none_when_source_read_only():
    """Read_only analyst pods MUST NOT mutate FOS — same guard shape as
    publish_matrix_to_fos and export_admin_state."""
    s3 = MagicMock()
    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src(access_level="read_only")),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import restore_scoring_matrix_version

        assert restore_scoring_matrix_version("svc1", "v7") is None
    s3.copy_object.assert_not_called()


def test_restore_scoring_matrix_version_returns_none_when_version_missing():
    """Operator picked a version that no longer exists (manually deleted,
    or stale UI). head_object raises → return None so the route layer
    can 404 cleanly instead of attempting a copy_object that would
    create an empty 'restored' matrix."""
    s3 = MagicMock()
    s3.head_object.side_effect = Exception("NoSuchKey")

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import restore_scoring_matrix_version

        assert restore_scoring_matrix_version("svc1", "missing-version") is None
    s3.copy_object.assert_not_called()


def test_restore_scoring_matrix_version_happy_path():
    """Version exists → copy_object lands on the current scoring_matrix.json
    key and we return {"version": ..., "restored_at": ...}."""
    s3 = MagicMock()
    # head_object returns OK (no exception) → version exists.
    s3.head_object.return_value = {"ContentLength": 2048}

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None
    assert result["version"] == "v7"
    assert "restored_at" in result

    # The last copy_object call must target the current matrix key, with
    # the historical version as the CopySource.
    final_copy = s3.copy_object.call_args_list[-1]
    assert final_copy.kwargs["Key"] == "iceberg/meta/scoring_matrix.json"
    assert final_copy.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "iceberg/meta/scoring_matrix_history/v7.json",
    }


def test_publish_matrix_snapshots_prior_before_overwriting():
    """SNAPSHOT-BEFORE-OVERWRITE: when publish_matrix_to_fos is called and
    a prior live matrix exists, the prior matrix bytes must be copied to
    the scoring_matrix_history/{prior_version}.json key BEFORE the new
    matrix is written to scoring_matrix.json. If the order were reversed,
    a fresh retrain would clobber the prior matrix and the history slot
    would receive the NEW bytes — making rollback impossible."""
    s3 = MagicMock()

    prior_matrix = {"version": "old-v1", "counts": {"/": {"/login": 3}}}
    prior_bytes = json.dumps(prior_matrix).encode("utf-8")
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=prior_bytes))}

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import publish_matrix_to_fos

        publish_matrix_to_fos("svc1", {"version": "new-v2", "counts": {}})

    # Two put_object calls in order: (1) history snapshot, (2) new live matrix.
    put_calls = s3.put_object.call_args_list
    assert len(put_calls) == 2, f"expected 2 put_object calls (history + current), got {len(put_calls)}"

    # First put writes the PRIOR bytes to the history slot, keyed by the
    # prior matrix's version string.
    history_call = put_calls[0]
    assert history_call.kwargs["Key"] == "iceberg/meta/scoring_matrix_history/old-v1.json"
    assert history_call.kwargs["Body"] == prior_bytes

    # Second put writes the NEW matrix to the live key.
    current_call = put_calls[1]
    assert current_call.kwargs["Key"] == "iceberg/meta/scoring_matrix.json"
    assert json.loads(current_call.kwargs["Body"].decode("utf-8"))["version"] == "new-v2"


def test_restore_scoring_matrix_version_snapshots_live_before_restore():
    """SNAPSHOT-BEFORE-OVERWRITE (restore variant): if the operator picks
    the wrong historical version, the only way to recover is if the
    pre-restore live matrix was archived first. This pins that
    restore_scoring_matrix_version calls copy_object TWICE in order:
    (1) current → pre-restore-{epoch_ms} snapshot, then
    (2) historical → current.

    If the order were reversed, the pre-restore snapshot would capture
    the JUST-RESTORED matrix (i.e. the historical version itself),
    silently losing the prior-live matrix forever.
    """
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 1024}  # historical version exists

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
        # Pin time.time() so the snapshot key is deterministic and the
        # ordering assertion below can reference an exact key string.
        patch("backend.state_sync.time.time", return_value=1717420800.0),
    ):
        from backend.state_sync import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None and result["version"] == "v7"

    copy_calls = s3.copy_object.call_args_list
    assert len(copy_calls) == 2, f"expected 2 copy_object calls (snapshot + restore), got {len(copy_calls)}"

    # FIRST copy_object: snapshot current matrix to pre-restore-{epoch_ms}.
    snapshot_call = copy_calls[0]
    expected_snapshot_key = f"iceberg/meta/scoring_matrix_history/pre-restore-{int(1717420800.0 * 1000)}.json"
    assert snapshot_call.kwargs["Key"] == expected_snapshot_key, (
        "first copy_object must write the pre-restore snapshot, not the restore itself"
    )
    assert snapshot_call.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "iceberg/meta/scoring_matrix.json",
    }

    # SECOND copy_object: historical version → current key.
    restore_call = copy_calls[1]
    assert restore_call.kwargs["Key"] == "iceberg/meta/scoring_matrix.json"
    assert restore_call.kwargs["CopySource"] == {
        "Bucket": "bkt",
        "Key": "iceberg/meta/scoring_matrix_history/v7.json",
    }


def test_restore_scoring_matrix_version_proceeds_when_snapshot_fails():
    """The pre-restore snapshot is best-effort: if it fails (e.g. no
    prior live matrix exists yet, or a transient S3 error), the restore
    itself must still proceed. The operator's active intent always wins
    over the safety net — otherwise a first-ever restore would always
    fail because there's nothing to snapshot."""
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 1024}

    # First copy_object (snapshot) raises; second (actual restore) succeeds.
    s3.copy_object.side_effect = [
        Exception("NoSuchKey: nothing to snapshot"),
        None,
    ]

    with (
        patch("backend.state_sync.get_source_for_service", return_value=_src()),
        patch("backend.state_sync._get_fos_client", return_value=s3),
    ):
        from backend.state_sync import restore_scoring_matrix_version

        result = restore_scoring_matrix_version("svc1", "v7")

    assert result is not None, "restore must succeed even when snapshot step fails"
    assert result["version"] == "v7"
    assert len(s3.copy_object.call_args_list) == 2


def test_cdn_get_blocks_invalid_redirects():
    """Verify that SafeRedirectHandler allows redirects to safe cdn URLs
    but blocks any redirects to forbidden URLs with URLError."""
    import urllib.error
    import urllib.request
    from unittest.mock import MagicMock, patch

    import pytest

    src = {"bucket": "test", "cdn_url": "https://cdn-test.fastly.net"}
    from backend.state_sync import _cdn_get

    captured_handler = None

    def fake_build_opener(*handlers):
        nonlocal captured_handler
        for h in handlers:
            if h.__class__.__name__ == "SafeRedirectHandler" or (
                isinstance(h, type) and h.__name__ == "SafeRedirectHandler"
            ):
                captured_handler = h
        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value.read.return_value = b"{}"
        mock_opener.open.return_value.__enter__.return_value.headers = {}
        return mock_opener

    # Ensure hasattr(urlopen, "assert_called") is False during this call so the opener code path is taken
    with (
        patch("urllib.request.build_opener", side_effect=fake_build_opener),
        patch("urllib.request.urlopen", new=lambda *a, **kw: None),
        patch("backend.utils.telemetry.record_cdn_call"),
    ):
        _cdn_get(src, "some/key.json")

    assert captured_handler is not None
    handler_inst = captured_handler() if isinstance(captured_handler, type) else captured_handler

    # 1. Test redirect to safe URL
    req_mock = MagicMock()
    # It delegates to super().redirect_request, so let's mock the super class call or verify it doesn't raise
    with patch("urllib.request.HTTPRedirectHandler.redirect_request") as mock_super_redirect:
        handler_inst.redirect_request(req_mock, None, 302, "Found", {}, "https://cdn-another.fastly.net/safe")
        mock_super_redirect.assert_called_once_with(
            req_mock, None, 302, "Found", {}, "https://cdn-another.fastly.net/safe"
        )

    # 2. Test redirect to unsafe URL (e.g. localhost, cloud metadata, or anything not ending in .fastly.net/.fastlystorage.app)
    with pytest.raises(urllib.error.URLError) as excinfo:
        handler_inst.redirect_request(req_mock, None, 302, "Found", {}, "http://169.254.169.254/")
    assert "Redirected to an invalid URL" in str(excinfo.value)
