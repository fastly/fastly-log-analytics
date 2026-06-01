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
    from backend.core import metadata_db

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
    src = _src(cdn_url="https://cdn.example.com/", cdn_secret="topsecret")

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
    assert req.full_url == "https://cdn.example.com/iceberg/meta/admin_state.json?key=topsecret"


def test_cdn_get_records_telemetry_with_byte_count():
    """Every CDN hit must record a `record_cdn_call` entry so the usage
    page attributes the bytes — drift here under-reports egress."""
    src = _src(cdn_url="https://cdn.example.com")
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
    src = _src(cdn_url="https://cdn.example.com")

    not_found = urllib.error.HTTPError(
        url="https://cdn.example.com/missing",
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
    src = _src(cdn_url="https://cdn.example.com")

    server_err = urllib.error.HTTPError(
        url="https://cdn.example.com/key",
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
    src = _src(cdn_url="https://cdn.example.com")
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
        patch("backend.core.metadata_db.replace_audit_for_service") as mock_audit,
        patch("backend.core.metadata_db.replace_views_for_service") as mock_views,
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
    src = _src(cdn_url="https://cdn.example.com")
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
        patch("backend.core.metadata_db.replace_audit_for_service"),
        patch("backend.core.metadata_db.replace_views_for_service"),
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
    src = _src(cdn_url="https://cdn.example.com")
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
        patch("backend.core.metadata_db.replace_audit_for_service"),
        patch("backend.core.metadata_db.replace_views_for_service"),
    ):
        from backend.state_sync import import_admin_state

        import_admin_state("svc1")

    mock_save.assert_not_called()
