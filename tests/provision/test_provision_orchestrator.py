"""Tests for ``backend.provision.orchestrator`` — pure helpers and provision/teardown flows."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
import vcr

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.fastly.client import fastly as real_fastly
from backend.core.log_fields import resolve_enabled_fields
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.provision import orchestrator, rum_assets
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState
from backend.provision.orchestrator import _reject_unsafe_fos_component
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

CASSETTE_DIR = os.path.join(os.path.dirname(__file__), "..", "cassettes")

_my_vcr = vcr.VCR(
    cassette_library_dir=CASSETTE_DIR,
    record_mode="none",
    filter_headers=[("Fastly-Key", "REDACTED"), ("Authorization", "REDACTED")],
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def _ns(**kwargs) -> SimpleNamespace:
    base = dict(preset=None, enable_group=None, disable_group=None, enable_field=None, disable_field=None)
    base.update(kwargs)
    return SimpleNamespace(**base)


def _make_state(**overrides) -> dict:
    base = {
        "logging_service_id": "svc-test",
        "service_name": "Test Service",
        "fos_region": "us-east-1",
        "fos_bucket_name": "test-bucket",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "secret",
        "cdn_url": "https://test.example",
        "cdn_secret": "cdn-secret",
        "cdn_service_id": "cdn-1",
        "admin_token": "tok",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": True,
        "log_fields": {"groups": ["A"]},
    }
    base.update(overrides)
    return base


def _write_cfg(tmp_path: Path, state: dict, *, existing_cfg: object = ...):
    saved_cfgs: list[tuple[str, dict]] = []
    patches = [
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ]
    if existing_cfg is not ...:
        patches.append(patch("backend.config.load_config", return_value=existing_cfg))
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        orchestrator.write_service_config(state)
    return saved_cfgs[0][1]


def _provision_cfg(**overrides) -> dict:
    base = {
        "admin_token": "tok",
        "logging_service_id": "svc-prov-test",
        "service_name": "Test Service",
        "fos_region": "us-east-1",
        "fos_bucket_name": "test-bucket",
        "fos_prefix": "",
        "endpoint_name": "Test Endpoint",
        "sample_rate": 100,
        "edge_only": True,
        "log_period": 60,
        "cdn_service_name": "Test CDN",
        "cdn_url": "https://test.example",
        "cdn_secret": "secret",
        "log_fields": {"groups": ["A"]},
    }
    base.update(overrides)
    return base


def _provision_cfg_provision_fastly_failures(**overrides) -> dict:
    return _provision_cfg(
        admin_token="test-token-not-real",
        logging_service_id="SU3xxxxxxxxxxxxxx0000",
        **overrides,
    )


def _fos_key(suffix: str = "X") -> dict:
    return {"access_key": f"AK_{suffix}", "secret_key": f"SK_{suffix}", "id": f"ID_{suffix}"}


def _consume(gen):
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


@contextlib.contextmanager
def _mocked_provision_steps(
    *,
    bucket_side_effect=None,
    logging_return=42,
    logging_side_effect=None,
    write_side_effect=...,
    upload_faro_side_effect=...,
    source_return=None,
    iceberg_side_effect=None,
    teardown_side_effect=None,
):
    reconcile_kw = (
        {"side_effect": logging_side_effect} if logging_side_effect is not None else {"return_value": logging_return}
    )
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch("backend.provision.orchestrator.validate_log_format", return_value=[]))
        stack.enter_context(
            patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=[_fos_key("T"), _fos_key("P")])
        )
        stack.enter_context(patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=bucket_side_effect))
        stack.enter_context(patch("backend.provision.orchestrator.delete_fos_access_key"))
        stack.enter_context(
            patch("backend.provision.orchestrator.ensure_cdn_service", return_value={"id": "cdn-svc-id"})
        )
        stack.enter_context(patch("backend.provision.orchestrator.ensure_logging_via_reconciler", **reconcile_kw))
        if write_side_effect is not ...:
            stack.enter_context(
                patch("backend.provision.orchestrator.write_service_config", side_effect=write_side_effect)
            )
        mock_upload = None
        if upload_faro_side_effect is not ...:
            mock_upload = stack.enter_context(
                patch("backend.provision.orchestrator._upload_faro_bundle_sync", side_effect=upload_faro_side_effect)
            )
        if teardown_side_effect is not None:
            stack.enter_context(
                patch("backend.provision.orchestrator.perform_teardown", side_effect=teardown_side_effect)
            )
        stack.enter_context(patch("backend.core.duckdb.get_source_for_service", return_value=source_return))
        if source_return is not None:
            stack.enter_context(patch("backend.core.duckdb.get_connection"))
        mock_iceberg = stack.enter_context(
            patch("backend.core.iceberg.init_iceberg_table", side_effect=iceberg_side_effect)
        )
        yield {"upload": mock_upload, "iceberg": mock_iceberg}


def _teardown_state(**overrides) -> dict:
    base = {
        "logging_service_id": "svc-td-test",
        "fos_bucket_name": "test-bucket",
        "fos_region": "us-east-1",
        "endpoint_name": "Test Endpoint",
        "cdn_service_id": "cdn-svc-id",
        "cdn_service_name": "CDN Name",
    }
    base.update(overrides)
    return base


@contextlib.contextmanager
def _mock_teardown_env(
    *, remove_logging_se=None, delete_bucket_se=None, delete_cdn_se=None, fastly_se=None, scoring_se=None
):
    fastly_kw = {"side_effect": fastly_se} if fastly_se else {"return_value": {"data": []}}
    scoring_kw = {"side_effect": scoring_se} if scoring_se else {"return_value": []}
    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=remove_logging_se) as m_log,
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=delete_bucket_se) as m_bkt,
        patch("backend.provision.orchestrator.delete_cdn_service", side_effect=delete_cdn_se) as m_cdn,
        patch("backend.provision.orchestrator.ensure_fos_access_key", return_value=_fos_key("T")),
        patch("backend.provision.orchestrator.delete_fos_access_key") as m_key,
        patch("backend.provision.orchestrator.fastly", **fastly_kw) as m_fastly,
        patch("backend.provision.session_scoring_orchestrator.teardown_scoring_resources", **scoring_kw) as m_score,
    ):
        yield {
            "remove_logging": m_log,
            "delete_bucket": m_bkt,
            "delete_cdn": m_cdn,
            "delete_key": m_key,
            "fastly": m_fastly,
            "scoring": m_score,
        }


# ── _build_log_fields_config tests ──


def test_build_log_fields_config_defaults_to_standard_preset():
    cfg = orchestrator._build_log_fields_config(_ns())
    assert cfg["preset"] == "standard" and cfg["schema_version"] == 2
    assert isinstance(cfg["groups"], list) and cfg["groups"] == sorted(cfg["groups"])


def test_build_log_fields_config_adds_enable_groups_without_duplicates():
    cfg = orchestrator._build_log_fields_config(_ns(preset="standard", enable_group=["L", "L"]))
    assert cfg["groups"].count("L") <= 1


def test_build_log_fields_config_disable_group_removes_from_set():
    cfg = orchestrator._build_log_fields_config(_ns(preset="standard", disable_group=["A"]))
    assert "A" not in cfg["groups"]


def test_build_log_fields_config_field_overrides_merge_with_disable_last():
    cfg = orchestrator._build_log_fields_config(_ns(enable_field=["fld.a", "fld.b"], disable_field=["fld.b"]))
    assert cfg["field_overrides"]["fld.a"] is True and cfg["field_overrides"]["fld.b"] is False


def test_build_log_fields_config_seeds_preset_field_overrides():
    cfg = orchestrator._build_log_fields_config(_ns(preset="security"))
    assert cfg["field_overrides"].get("tls_ciphers_sha") is True


def test_build_log_fields_config_opts_in_default_off_field():
    base_cfg = orchestrator._build_log_fields_config(_ns(preset="security"))
    assert "cookie_session" not in resolve_enabled_fields(base_cfg)

    optin_cfg = orchestrator._build_log_fields_config(_ns(preset="security", enable_field=["cookie_session"]))
    assert optin_cfg["field_overrides"].get("cookie_session") is True
    assert "cookie_session" in resolve_enabled_fields(optin_cfg)


# ── write_service_config tests ──


def test_write_service_config_honors_sub_30s_sync_interval(tmp_path):
    assert _write_cfg(tmp_path, _make_state(log_period=10))["provisioning"]["cron_sync"]["interval_seconds"] == 10
    assert _write_cfg(tmp_path, _make_state(log_period=120))["provisioning"]["cron_sync"]["interval_seconds"] == 120
    assert _write_cfg(tmp_path, _make_state(log_period=2))["provisioning"]["cron_sync"]["interval_seconds"] == 5


def test_write_service_config_commit_interval_respects_state_override(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(log_period=60, commit_interval_mins=15))
    assert cfg["provisioning"]["cron_sync"]["commit_interval_mins"] == 15


def test_write_service_config_defaults_log_period_from_provisioning_nested(tmp_path):
    state = _make_state()
    state.pop("log_period", None)
    state["provisioning"] = {"log_period": 120}
    assert _write_cfg(tmp_path, state)["log_period"] == 120


def test_write_service_config_propagates_access_level_and_storage_mode(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(access_level="read_only", storage_mode="local"))
    assert cfg["access_level"] == "read_only" and cfg["storage_mode"] == "local"


def test_write_service_config_uses_service_name_or_id_as_name(tmp_path):
    state = _make_state()
    state.pop("service_name", None)
    state.pop("name", None)
    assert _write_cfg(tmp_path, state)["name"] == "svc-test"


def test_write_service_config_threads_cron_sync_enabled_flag(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(enable_cron_sync=False, enable_cron_compact=False))
    assert cfg["provisioning"]["cron_sync"]["enabled"] is False
    assert cfg["provisioning"]["cron_compact"]["enabled"] is False


def test_write_service_config_delete_after_threads_into_cron_sync(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(delete_after=False))
    assert cfg["provisioning"]["cron_sync"]["delete_after"] is False


def test_write_service_config_log_retention_days_propagates(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(log_retention_days=90))
    assert cfg["log_retention_days"] == 90
    assert cfg["provisioning"]["cron_sync"]["log_retention_days"] == 90
    assert cfg["provisioning"]["cron_compact"]["log_retention_days"] == 90


def test_write_service_config_persists_detected_rate_limiting_false(tmp_path):
    assert _write_cfg(tmp_path, _make_state(rate_limiting=False))["provisioning"]["rate_limiting"] is False


def test_write_service_config_rate_limiting_defaults_true_when_never_detected(tmp_path):
    assert _write_cfg(tmp_path, _make_state())["provisioning"]["rate_limiting"] is True


def test_write_service_config_rate_limiting_carries_from_provisioning_block(tmp_path):
    state = _make_state(provisioning={"rate_limiting": False})
    assert _write_cfg(tmp_path, state)["provisioning"]["rate_limiting"] is False


# ── run_with_events & state persistence tests ──


def test_run_with_events_yields_status_events_in_order():
    def fake_func(status_cb=None):
        status_cb("step one")
        status_cb("step two")
        return "ok"

    assert list(orchestrator.run_with_events(fake_func)) == [
        {"type": "status", "message": "step one"},
        {"type": "status", "message": "step two"},
    ]


def test_run_with_events_propagates_thread_exception():
    def fake_func(status_cb=None):
        status_cb("about to fail")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        list(orchestrator.run_with_events(fake_func))


def test_run_with_events_returns_function_result_via_generator_return():
    captured = {}

    def driver():
        captured["v"] = yield from orchestrator.run_with_events(lambda status_cb=None: "got-it")

    for _ in driver():
        pass
    assert captured["v"] == "got-it"


def test_save_then_load_state_roundtrips():
    orchestrator.save_state({"k": "v", "n": 42})
    assert os.path.exists(orchestrator._state_file_path())
    assert orchestrator.load_state() == {"k": "v", "n": 42}


def test_load_state_returns_empty_when_file_missing():
    assert orchestrator.load_state() == {}


def test_load_state_swallows_corrupt_json():
    Path(orchestrator._state_file_path()).write_text("{not valid json")
    assert orchestrator.load_state() == {}


def test_save_state_swallows_io_error():
    with patch("builtins.open", side_effect=OSError("read-only fs")):
        orchestrator.save_state({"k": "v"})


# ── cleanup_local_data & generate_analyst_invite tests ──


def test_cleanup_local_data_removes_service_config(tmp_path):
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=False)
    assert not cfg_file.exists()


def test_cleanup_local_data_removes_duckdb_when_remove_data_true(tmp_path):
    db, wal = tmp_path / "x.duckdb", tmp_path / "x.duckdb.wal"
    db.write_text("")
    wal.write_text("")
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(db)),
        patch("backend.core.metadata.teardown"),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=True)
    assert not db.exists() and not wal.exists()


def test_cleanup_local_data_swallows_metadata_db_teardown_exception(tmp_path):
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "noexist.duckdb")),
        patch("backend.core.metadata.teardown", side_effect=RuntimeError("locked")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=True)


def test_cleanup_local_data_removes_cache_directory_for_bucket(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cache_dir = tmp_path / "cache" / "test-bucket"
    cache_dir.mkdir(parents=True)
    (cache_dir / "stale.parquet").write_text("data")
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "no.duckdb")),
        patch("backend.core.metadata.teardown"),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", bucket="test-bucket", remove_data=True)
    assert not cache_dir.exists()


def test_analyst_path_a_supported_only_for_non_celery(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    assert orchestrator.analyst_path_a_supported() is True
    assert orchestrator.analyst_path_a_supported({"deployment_mode": "high_scale"}) is False

    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    assert orchestrator.analyst_path_a_supported() is False
    assert orchestrator.analyst_path_a_supported({"deployment_mode": "standard"}) is True


def test_generate_analyst_invite_raises_when_service_missing():
    with patch("backend.config.load_config", return_value=None):
        with pytest.raises(RuntimeError, match="not found"):
            orchestrator.generate_analyst_invite("ghost-svc")


def test_generate_analyst_invite_rejects_read_only_service():
    with patch("backend.config.load_config", return_value={"access_level": "read_only", "fastly_api_key": "k"}):
        with pytest.raises(RuntimeError, match="read_write"):
            orchestrator.generate_analyst_invite("svc")


def test_generate_analyst_invite_rejects_celery_topology():
    with (
        patch("backend.config.DEPLOYMENT_MODE", "high_scale"),
        patch("backend.config.load_config", return_value={"access_level": "read_write", "fastly_api_key": "k"}),
    ):
        with pytest.raises(RuntimeError, match="DEPLOYMENT_MODE=high_scale"):
            orchestrator.generate_analyst_invite("svc")


def test_generate_analyst_invite_returns_payload_with_credentials():
    fake_cfg = {
        "access_level": "read_write",
        "fastly_api_key": "tok",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "fos_prefix": "logs/",
        "name": "MyService",
        "cdn_url": "https://cdn.example",
        "cdn_service_id": "cdn-id",
        "cdn_secret": "cdn-sec",
    }
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.provision.orchestrator.fastly", return_value={"access_key": "AKIANEW", "secret_key": "SECRETNEW"}
        ),
    ):
        out = orchestrator.generate_analyst_invite("svc")

    assert (out["access_key_id"], out["secret_key"], out["fos_bucket"], out["cdn_url"]) == (
        "AKIANEW",
        "SECRETNEW",
        "b",
        "https://cdn.example",
    )
    assert out["fos_endpoint"].endswith("fastlystorage.app") and out["iceberg_metadata_location"] is None


def test_generate_analyst_invite_sends_correct_fastly_payload():
    captured = []

    def fake_fastly(method, path, body=None, **kwargs):
        captured.append((method, path, body, kwargs))
        return {"access_key": "AK", "secret_key": "SK"}

    with (
        patch(
            "backend.config.load_config",
            return_value={"access_level": "read_write", "fastly_api_key": "tok", "fos_bucket": "b"},
        ),
        patch("backend.provision.orchestrator.fastly", side_effect=fake_fastly),
    ):
        orchestrator.generate_analyst_invite("svc")

    assert len(captured) == 1
    method, path, body, kwargs = captured[0]
    assert (method, path, body["permission"], body["buckets"], kwargs["token"]) == (
        "POST",
        "/resources/object-storage/access-keys",
        "read-only-objects",
        ["b"],
        "tok",
    )
    assert "fos-log-analysis-analyst" in body["description"]


def test_sync_crontab_swallows_scheduler_import_failure():
    with patch("backend.cron.scheduler.get_scheduler", side_effect=ImportError("no scheduler")):
        orchestrator._sync_crontab()


def test_sync_crontab_swallows_reload_exception():
    class _Sched:
        def reload(self):
            raise CronLeaseHeldError("scheduler busy")

    with patch("backend.cron.scheduler.get_scheduler", return_value=_Sched()):
        orchestrator._sync_crontab()


# ── provision() tests ──


def test_provision_yields_error_event_on_preflight_failure(tmp_path, monkeypatch):
    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG"]),
        patch("backend.provision.orchestrator.ensure_fos_access_key") as mock_create_key,
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None and events[-1]["type"] == "error" and "Log format" in events[-1]["message"]
    mock_create_key.assert_not_called()


def test_provision_completes_all_8_steps_when_apis_succeed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with _mocked_provision_steps(write_side_effect=None):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None
    progress_events = [e for e in events if e["type"] == "progress"]
    assert len(progress_events) == 9 and progress_events[0]["current"] == 0 and progress_events[-1]["current"] == 8
    assert events[-1]["type"] == "done" and "complete" in events[-1]["message"].lower()


def test_provision_uploads_faro_bundle_before_reconcile_when_rum_enabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    call_order: list[str] = []

    def fake_upload(service_id, version, token, *, status_cb=None):
        call_order.append(f"upload:{service_id}:{version}")
        return {"version": version}

    def fake_reconcile(state, token, status_cb=None):
        call_order.append("reconcile")
        return 42

    with _mocked_provision_steps(
        write_side_effect=None, upload_faro_side_effect=fake_upload, logging_side_effect=fake_reconcile
    ):
        events, exc = _consume(
            orchestrator.provision(_provision_cfg(rum_enabled=True, rum={"enabled": True, "faro_version": "1.2.3"}))
        )

    assert exc is None and events[-1]["type"] == "done"
    assert call_order == ["upload:svc-prov-test:1.2.3", "reconcile"]


def test_provision_resolves_default_faro_version_when_rum_enabled_and_unpinned(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    uploaded_versions: list[str] = []

    def fake_upload(service_id, version, token, *, status_cb=None):
        uploaded_versions.append(version)
        return {"version": version}

    with _mocked_provision_steps(write_side_effect=None, upload_faro_side_effect=fake_upload):
        events, exc = _consume(orchestrator.provision(_provision_cfg(rum_enabled=True, rum={"enabled": True})))

    assert exc is None and uploaded_versions == [DEFAULT_FARO_VERSION]


def test_provision_skips_faro_upload_when_rum_disabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with _mocked_provision_steps(write_side_effect=None, upload_faro_side_effect=None) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None
    env["upload"].assert_not_called()


def test_provision_satisfies_faro_route_and_object_invariant_end_to_end(tmp_path, monkeypatch):
    config_dir = tmp_path / "configs"
    config_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)
    monkeypatch.setattr(svcconfig, "duckdb_path", lambda service_id: str(tmp_path / "db.duckdb"))

    chosen_version = "2.8.5"
    sample_bundle = (
        b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"
    )

    async def fake_fetch(version: str) -> bytes:
        return sample_bundle

    monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)

    uploaded: dict[str, bytes] = {}
    real_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT" and request.url.path.endswith(f"faro-web-sdk-v{chosen_version}.iife.js"):
            uploaded["bundle"] = request.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(
        httpx, "Client", lambda *a, **kw: real_client(*a, **{**kw, "transport": httpx.MockTransport(handler)})
    )

    with _mocked_provision_steps():
        events, exc = _consume(
            orchestrator.provision(
                _provision_cfg(rum_enabled=True, rum={"enabled": True, "faro_version": chosen_version})
            )
        )

    assert exc is None and events[-1]["type"] == "done" and uploaded["bundle"] == sample_bundle
    written_cfg = json.loads((config_dir / "svc-prov-test.json").read_text())
    assert written_cfg["rum"]["faro_version"] == chosen_version
    assert '"/js/faro-sdk.js"' in generate_consolidated_snippet(FeatureState.from_config(written_cfg), "vcl_recv")


def test_provision_faro_upload_failure_is_non_fatal_and_pin_persists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "configs"
    config_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)
    monkeypatch.setattr(svcconfig, "duckdb_path", lambda service_id: str(tmp_path / "db.duckdb"))

    teardown_called: list[bool] = []

    def fake_teardown(state, token, opts=None):
        teardown_called.append(True)
        yield {"type": "status", "message": "rollback step"}

    with _mocked_provision_steps(
        upload_faro_side_effect=RuntimeError("unpkg.com: connection refused"),
        teardown_side_effect=fake_teardown,
    ):
        events, exc = _consume(
            orchestrator.provision(_provision_cfg(rum_enabled=True, rum={"enabled": True, "faro_version": "1.2.3"}))
        )

    assert exc is None and teardown_called == [] and events[-1]["type"] == "done"
    assert any("Faro Web SDK upload failed" in e["message"] for e in events if e["type"] == "status")
    written_cfg = json.loads((config_dir / "svc-prov-test.json").read_text())
    assert written_cfg["rum"]["faro_version"] == "1.2.3"


def test_provision_faro_upload_reads_back_persisted_hashes_not_pre_upload_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "configs"
    config_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)
    monkeypatch.setattr(svcconfig, "duckdb_path", lambda service_id: str(tmp_path / "db.duckdb"))

    def fake_upload(service_id, version, token, *, status_cb=None):
        cfg = svcconfig.load_config(service_id) or {}
        cfg["rum"] = {
            **(cfg.get("rum") or {}),
            "faro_version": version,
            "faro_content_hash": "sha256-fake-content-hash",
            "faro_fos_etag_md5": "fake-etag-md5",
        }
        svcconfig.save_config(service_id, cfg)
        return {"version": version}

    with _mocked_provision_steps(upload_faro_side_effect=fake_upload):
        events, exc = _consume(
            orchestrator.provision(_provision_cfg(rum_enabled=True, rum={"enabled": True, "faro_version": "1.2.3"}))
        )

    assert exc is None and events[-1]["type"] == "done"
    written_cfg = json.loads((config_dir / "svc-prov-test.json").read_text())
    assert (
        written_cfg["rum"]["faro_version"],
        written_cfg["rum"]["faro_content_hash"],
        written_cfg["rum"]["faro_fos_etag_md5"],
    ) == ("1.2.3", "sha256-fake-content-hash", "fake-etag-md5")


def test_provision_runs_teardown_rollback_on_mid_step_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    teardown_called = []

    def fake_teardown(state, token, opts=None):
        teardown_called.append(True)
        yield {"type": "status", "message": "rollback step"}

    with (
        _mocked_provision_steps(
            bucket_side_effect=RuntimeError("BucketAlreadyExists"),
            teardown_side_effect=fake_teardown,
        ),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert isinstance(exc, RuntimeError) and "BucketAlreadyExists" in str(exc)
    assert teardown_called == [True]
    assert any("BucketAlreadyExists" in e["message"] for e in events if e["type"] == "error")


def test_provision_loads_state_when_resume_from_state_true(tmp_path, monkeypatch):
    orchestrator.save_state(
        {"temp_admin_key_id": "PREV_ID", "temp_admin_access_key": "PREV_AK", "temp_admin_secret_key": "PREV_SK"}
    )
    captured_state = {}

    def capture_create(desc, state, token, **kwargs):
        captured_state["state"] = dict(state)
        return _fos_key("NEW")

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=capture_create),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=RuntimeError("abort early")),
        patch("backend.provision.orchestrator.perform_teardown", return_value=iter([])),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        _consume(orchestrator.provision(_provision_cfg(), _resume_from_state=True))

    assert captured_state["state"]["temp_admin_key_id"] == "PREV_ID"


def test_provision_surfaces_warning_when_no_source_resolved(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with _mocked_provision_steps(write_side_effect=None, source_return=None) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None and events[-1]["type"] == "done"
    env["iceberg"].assert_not_called()
    assert any(e["type"] == "status" and "not initialized" in e["message"].lower() for e in events)


def test_provision_surfaces_warning_when_iceberg_init_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with _mocked_provision_steps(
        write_side_effect=None,
        source_return={"name": "svc", "service_id": "sid", "logging_service_id": "lsid"},
        iceberg_side_effect=RuntimeError("FOS not ready"),
    ) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None and events[-1]["type"] == "done"
    env["iceberg"].assert_called_once()
    assert any(e["type"] == "status" and "deferred to first commit" in e["message"].lower() for e in events)


# ── perform_teardown() tests ──


def test_perform_teardown_calls_all_5_steps_with_default_opts():
    with _mock_teardown_env() as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))

    assert exc is None
    env["scoring"].assert_not_called()
    assert env["remove_logging"].called and env["delete_bucket"].called and env["delete_cdn"].called
    assert len([e for e in events if e["type"] == "progress"]) == 5


def test_perform_teardown_refuses_to_delete_logging_service_as_cdn(caplog):
    with _mock_teardown_env() as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(cdn_service_id="svc-td-test"), "tok"))
    assert exc is None and not env["delete_cdn"].called and "refusing to delete CDN service" in caplog.text


def test_perform_teardown_deletes_scoring_when_enabled():
    scoring_block = {
        "enabled": True,
        "scoring_service_id": "SCORESVC",
        "scoring_keys_store_id": "KEYS",
        "scoring_config_store_id": "CFG",
        "scoring_matrix_store_id": "MTX",
    }
    state = _teardown_state(scoring=scoring_block)
    with _mock_teardown_env() as env:
        events, exc = _consume(orchestrator.perform_teardown(state, "tok"))

    assert exc is None
    env["scoring"].assert_called_once()
    assert env["scoring"].call_args.args[:3] == (state["logging_service_id"], scoring_block, "tok")


def test_perform_teardown_skips_scoring_when_remove_scoring_opt_false():
    state = _teardown_state(scoring={"enabled": True, "scoring_service_id": "SCORESVC"})
    with _mock_teardown_env() as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                state,
                "tok",
                opts={"remove_logging": True, "remove_cdn": True, "remove_bucket": True, "remove_scoring": False},
            )
        )
    assert exc is None and not env["scoring"].called


def test_perform_teardown_swallows_scoring_exception_with_warning():
    state = _teardown_state(scoring={"enabled": True, "scoring_service_id": "SCORESVC"})
    with _mock_teardown_env(scoring_se=RuntimeError("scoring upstream 503")) as env:
        events, exc = _consume(orchestrator.perform_teardown(state, "tok"))

    assert exc is None and env["delete_cdn"].called
    aborted = [e for e in events if e["type"] == "status" and "aborted" in e["message"].lower()]
    assert aborted and "left in place" in aborted[0]["message"].lower()


def test_perform_teardown_skips_logging_when_remove_logging_opt_false():
    with _mock_teardown_env() as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(), "tok", opts={"remove_logging": False, "remove_cdn": True, "remove_bucket": True}
            )
        )
    assert exc is None and not env["remove_logging"].called


def test_perform_teardown_skips_bucket_when_remove_bucket_opt_false():
    fastly_paths = []
    with _mock_teardown_env(fastly_se=lambda *a, **_kw: (fastly_paths.append(a[1]), {"data": []})[1]) as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(), "tok", opts={"remove_logging": True, "remove_cdn": True, "remove_bucket": False}
            )
        )
    assert exc is None and not env["delete_bucket"].called
    assert not any("access-keys" in p for p in fastly_paths)


def test_perform_teardown_swallows_remove_logging_exception_with_warning():
    with _mock_teardown_env(remove_logging_se=RuntimeError("upstream 502")) as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))
    assert exc is None and env["delete_bucket"].called
    assert any("Warning" in e.get("message", "") for e in events)


def test_write_service_config_preserves_scoring_block_when_state_omits_it(tmp_path):
    saved_cfgs = []
    existing_cfg = {
        "service_id": "svc-test",
        "scoring": {
            "enabled": True,
            "domain": "example.com",
            "scoring_service_id": "scorer-svc",
            "enabled_at": "2026-06-02T13:59:02+00:00",
        },
        "ngwaf_workspace_id": "ngwaf-abc",
        "log_fields": {
            "groups": ["A"],
            "custom_fields": [
                {"name": "edge_score", "duckdb_type": "INTEGER", "enabled": True},
                {"name": "my_field", "duckdb_type": "VARCHAR", "enabled": True},
            ],
        },
    }
    with (
        patch("backend.config.load_config", return_value=existing_cfg),
        patch(
            "backend.config._save_config_unlocked", side_effect=lambda sid, cfg, **_kw: saved_cfgs.append((sid, cfg))
        ),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state())

    _, cfg = saved_cfgs[0]
    assert cfg.get("scoring", {}).get("enabled") is True and cfg["scoring"]["scoring_service_id"] == "scorer-svc"
    assert cfg.get("ngwaf_workspace_id") == "ngwaf-abc"
    custom_names = {cf["name"] for cf in cfg["log_fields"]["custom_fields"]}
    assert "my_field" in custom_names
    for name in _SCORING_FIELD_NAMES:
        assert name not in custom_names
    state = FeatureState.from_config(cfg)
    all_field_names = {f["name"] for f in state.log_fields.custom_fields}
    for name in _SCORING_FIELD_NAMES:
        assert name in all_field_names


def test_write_service_config_first_ever_ingest_has_no_existing_cfg(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(), existing_cfg=None)
    assert "scoring" not in cfg and "ngwaf_workspace_id" not in cfg


def test_perform_teardown_filters_fastly_keys_by_managed_description_prefix():
    fake_keys = {
        "data": [
            {"access_key": "MANAGED1", "description": "fos-log-analysis-svc-td-test"},
            {"access_key": "TEMPADMIN", "description": "fos-log-analysis-temp-admin-svc-td-test"},
            {"access_key": "TEMPTEAR", "description": "temp-teardown-svc-td-test"},
            {"access_key": "UNMANAGED", "description": "customer-other-thing"},
        ]
    }
    deleted_keys = []

    def fake_delete_fos_tokens(service_id, token, status_cb=None):
        patterns = [
            f"fos-log-analysis-{service_id}",
            f"fos-log-analysis-temp-admin-{service_id}",
            f"temp-teardown-{service_id}",
        ]
        for key in fake_keys["data"]:
            if any(key.get("description", "").startswith(p) for p in patterns):
                deleted_keys.append(key["access_key"])

    with (
        _mock_teardown_env(),
        patch("backend.provision.fos_setup.delete_fos_tokens_for_service", side_effect=fake_delete_fos_tokens),
    ):
        _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))

    assert set(deleted_keys) == {"MANAGED1", "TEMPADMIN", "TEMPTEAR"}
    assert "UNMANAGED" not in deleted_keys


def test_write_service_config_preserves_existing_ngwaf_workspace_id(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svc_id = "svc-ngwaf-preserve"
    svcconfig.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "ngwaf_workspace_id": "ws_preserved_xyz",
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
        },
    )
    orchestrator.write_service_config(
        _make_state(
            service_id=svc_id,
            service_name=svc_id,
            fos_bucket_name="b",
            fos_prefix="logs",
            fos_access_key_id="AK",
            fos_secret_access_key="SK",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            cdn_service_id="CDN",
            cdn_url="https://b.global.ssl.fastly.net",
            logging_service_id="SU",
            endpoint_name="fastly_log_analysis",
            edge_only=False,
            log_fields={"schema_version": 2, "groups": ["A"], "custom_fields": []},
        )
    )
    assert svcconfig.load_config(svc_id).get("ngwaf_workspace_id") == "ws_preserved_xyz"


def test_write_service_config_ngwaf_routing_separated_from_provision_state(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svc_id = "svc-ngwaf-routing"
    svcconfig.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "ngwaf_workspace_id": "ws_existing",
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
        },
    )
    orchestrator.write_service_config(
        _make_state(
            service_id=svc_id,
            service_name=svc_id,
            ngwaf_workspace_id="ws_in_state_should_be_ignored",
            fos_bucket_name="b",
            fos_prefix="logs",
            fos_access_key_id="AK",
            fos_secret_access_key="SK",
            fos_endpoint="us-east-1.object.fastlystorage.app",
            cdn_service_id="CDN",
            cdn_url="https://b.global.ssl.fastly.net",
            logging_service_id="SU",
            endpoint_name="fastly_log_analysis",
            edge_only=False,
            log_fields={"schema_version": 2, "groups": ["A"], "custom_fields": []},
        )
    )
    saved = svcconfig.load_config(svc_id)
    assert "ngwaf_workspace_id" not in saved or saved.get("ngwaf_workspace_id") == "ws_existing"


def test_reject_unsafe_fos_component_bucket_rejects_path_tokens():
    for bad in ("../etc", "a/b", "a\\b", "..", "a\x00b"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_bucket", bad, allow_slash=False)
    _reject_unsafe_fos_component("fos_bucket", "my-fos-bucket", allow_slash=False)
    _reject_unsafe_fos_component("fos_bucket", "", allow_slash=False)


def test_reject_unsafe_fos_component_prefix_allows_slash_rejects_traversal():
    _reject_unsafe_fos_component("fos_prefix", "raw/logs/2026/", allow_slash=True)
    for bad in ("../up", "a\\b", "..", "a\x00"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_prefix", bad, allow_slash=True)


def test_write_service_config_missing_service_id():
    with pytest.raises(ValueError, match="ingest state missing"):
        orchestrator.write_service_config({})


def test_write_service_config_rum_enabled_sets_enabled_at_timestamp(tmp_path):
    cfg = _write_cfg(tmp_path, _make_state(rum_enabled=True), existing_cfg=None)
    assert cfg.get("rum_enabled") is True and cfg.get("rum", {}).get("enabled") is True
    assert cfg["rum"].get("enabled_at") is not None


def test_ensure_logging_via_reconciler_missing_logging_service_id():
    with pytest.raises(ValueError, match="state missing logging_service_id"):
        next(orchestrator.ensure_logging_via_reconciler({}, "tok"))


def test_ensure_logging_via_reconciler_invalid_feature_state(tmp_path):
    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", side_effect=KeyError("missing_field")),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        with pytest.raises(RuntimeError, match="Invalid feature state configuration"):
            next(orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok"))


def test_ensure_logging_via_reconciler_reconciliation_failure():
    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", return_value=None),
        patch("backend.provision.orchestrator.reconcile_vcl_state", side_effect=RuntimeError("VCL validation failed")),
    ):
        with pytest.raises(RuntimeError, match="VCL reconciliation failed"):
            next(orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok"))


def test_cleanup_local_data_rejects_bucket_with_path_traversal_tokens(tmp_path):
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "noexist.duckdb")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", bucket="../../../tmp", remove_data=True)
        orchestrator.cleanup_local_data("svc", bucket="a/b", remove_data=True)
    assert not cfg_file.exists()


def test_cleanup_local_data_skips_cache_when_symlink_escapes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")

    cache_dir, external_dir = tmp_path / "cache", tmp_path / "external"
    cache_dir.mkdir()
    external_dir.mkdir()
    symlink = cache_dir / "bucket_symlink"
    symlink.symlink_to(external_dir)

    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "noexist.duckdb")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", bucket="bucket_symlink", remove_data=True)
    assert symlink.exists()


def test_ensure_logging_via_reconciler_no_active_version():
    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", return_value=None),
        patch(
            "backend.provision.orchestrator.reconcile_vcl_state",
            return_value=type("Result", (), {"activated_version": None})(),
        ),
        patch("backend.core.fastly.service.get_active_version", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="has no active version"):
            next(orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok"))


def test_ensure_logging_via_reconciler_no_changes_needed():
    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", return_value=None),
        patch(
            "backend.provision.orchestrator.reconcile_vcl_state",
            return_value=type("Result", (), {"activated_version": None})(),
        ),
        patch("backend.core.fastly.service.get_active_version", return_value=42),
    ):
        assert orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok") == 42


# ── Fastly failure & retry cassette tests ──


def test_provision_survives_fastly_429_with_retry(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    real_call_results = []

    def fake_bucket(*args, **kwargs):
        real_call_results.append(real_fastly("GET", "/service/SU3xxxxxxxxxxxxxx0000", token="test-token-not-real"))

    with (
        patch("tenacity.nap.time.sleep"),
        _my_vcr.use_cassette("fastly_429_then_success.yaml"),
        _mocked_provision_steps(bucket_side_effect=fake_bucket, write_side_effect=None),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg_provision_fastly_failures()))

    assert exc is None and events[-1]["type"] == "done"
    assert real_call_results == [{"id": "SU3xxxxxxxxxxxxxx0000", "name": "Test CDN", "active_version": {"number": 42}}]


def test_provision_survives_fastly_503_with_retry(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    real_call_results = []

    def fake_bucket(*args, **kwargs):
        real_call_results.append(real_fastly("GET", "/account", token="test-token-not-real"))

    with (
        patch("tenacity.nap.time.sleep"),
        _my_vcr.use_cassette("fastly_503_then_success.yaml"),
        _mocked_provision_steps(bucket_side_effect=fake_bucket, write_side_effect=None),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg_provision_fastly_failures()))

    assert exc is None and events[-1]["type"] == "done"
    assert real_call_results == [{"id": "acct-1", "name": "Test Account"}]


def test_provision_partial_failure_at_step_7_triggers_rollback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    teardown_calls = []

    def fake_teardown(state, token, opts=None):
        teardown_calls.append({"state": dict(state), "token": token, "opts": opts})
        yield {"type": "status", "message": "rollback step"}

    call_count = 0

    def raising_write(state):
        nonlocal call_count
        call_count += 1
        if call_count > 2:
            raise RuntimeError("config write failed mid-flow")

    with (
        _mocked_provision_steps(logging_return=99, write_side_effect=raising_write, teardown_side_effect=fake_teardown),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg_provision_fastly_failures()))

    assert isinstance(exc, RuntimeError) and "config write failed mid-flow" in str(exc)
    assert len(teardown_calls) == 1
    ts = teardown_calls[0]["state"]
    assert (ts["activated_logging_version"], ts["logging_service_id"], ts["cdn_service_id"], ts["fos_key_id"]) == (
        99,
        "SU3xxxxxxxxxxxxxx0000",
        "cdn-svc-id",
        "ID_P",
    )
    assert ts.get("endpoint_name", "Test Endpoint") == "Test Endpoint"
    assert any("config write failed mid-flow" in e["message"] for e in events if e["type"] == "error")


def test_provision_persists_config_before_cdn_and_logging_steps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_mock = MagicMock()
    calls_before_cdn = None

    with (
        _mocked_provision_steps(write_side_effect=write_mock),
    ):
        gen = orchestrator.provision(_provision_cfg_provision_fastly_failures())
        try:
            for e in gen:
                if "Step 6/8" in e.get("message", ""):
                    calls_before_cdn = write_mock.call_count
                    break
        finally:
            gen.close()

    assert calls_before_cdn is not None and calls_before_cdn >= 1
