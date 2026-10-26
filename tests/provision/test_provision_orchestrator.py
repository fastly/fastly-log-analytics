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
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.provision.declarative.generators import generate_consolidated_snippet
from backend.provision.declarative.state import FeatureState

from backend import config as svcconfig
from backend.core.fastly.client import fastly as real_fastly
from backend.core.log_fields import resolve_enabled_fields
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.provision import orchestrator, rum_assets
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


# ── Merged from tests/provision/test_vcl_builder.py ──

"""Tests for the VclBuilder VCL-assembly primitives (B15 consolidation)."""


import inspect
from datetime import UTC, datetime

from backend.core.fastly.rum_provisioning import RUM_ASSET_FETCH_NAME, generate_rum_asset_fetch_vcl
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, reconcile_cmcd_custom_fields
from backend.provision.utils import (
    ANALYTICS_LOG_LAYOUT,
    VclBuilder,
    analytics_log_path,
    draft_version,
    minute_list_prefix,
    rum_log_path,
    rum_minute_list_prefix,
)

from backend.provision import cmcd_fields as orch
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend


# ── Merged from tests/provision/test_vcl_builder.py ──
class TestLineMode:
    def test_join_lines_static(self) -> None:
        assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"


class TestSectionMode:
    def test_join_sections_static_matches_legacy_idiom(self) -> None:
        sections = ["a", "", "  ", "b"]
        assert VclBuilder.join_sections(sections) == "\n\n".join(s for s in sections if s.strip())


def test_remove_custom_fields_strips_cmcd_fields_only():
    cfg = {
        "log_fields": {
            "custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True),
        }
    }
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names

    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names)
    assert "mine" in names


def test_remove_custom_fields_tolerates_missing_blocks():
    for cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(cfg)  # must not raise


class TestAnalyticsLogPath:
    def test_fixed_root(self):
        assert analytics_log_path() == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestRumLogPath:
    def test_fixed_root(self):
        assert rum_log_path() == "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert minute_list_prefix(dt) == "raw/request/year=2026/month=08/day=27/hour=16/minute=53/"


class TestRumMinuteListPrefix:
    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert rum_minute_list_prefix(dt) == "raw/rum/year=2026/month=08/day=27/hour=16/minute=53/"


class TestProducersUseCanonicalLayout:
    def test_declarative_generator_matches(self):
        import backend.provision.declarative.generators as gen

        src = inspect.getsource(gen)
        assert "raw/%Y-%m-%d" not in src, "declarative generator must not use the dash layout"
        assert "log_paths" in src, "declarative generator must build paths via log_paths"

    def test_fastly_api_has_no_dash_layout_literal(self):
        import backend.provision.fastly_api as fa

        src = inspect.getsource(fa)
        assert "raw/%Y-%m-%d" not in src, "add_logging_endpoint must use log_paths, not the dash layout"

    def test_services_router_has_no_dash_layout_literal(self):
        import backend.routers.services.core as sc

        src = inspect.getsource(sc)
        assert "raw/%Y-%m-%d" not in src, "logging-settings update must use log_paths, not the dash layout"

    def test_layout_constant_matches_live_bucket_shape(self):
        assert ANALYTICS_LOG_LAYOUT == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def _minimal_rum_state(*, faro_version: str | None) -> FeatureState:
    return FeatureState(
        service_id="svc_test",
        log_period=60,
        sample_rate=100,
        edge_only=False,
        custom_condition="",
        fos_prefix="",
        fos_endpoint="fos.example.com",
        logging_enabled=False,
        rum_enabled=True,
        faro_version=faro_version,
    )


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_without_faro():
    state = _minimal_rum_state(faro_version=None)
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv


def test_vcl_recv_asset_routing_sourced_from_rum_provisioning_with_faro():
    state = _minimal_rum_state(faro_version="2.9.0")
    shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
    shared_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version="2.9.0")[RUM_ASSET_FETCH_NAME]
    assert shared_copy in shipped_recv


# ── Merged from tests/provision/test_draft_version.py ──
SID = "svc"


class _Fastly:
    def __init__(self, *, validate_ok=True, rollback_raises=False):
        self.calls: list[tuple[str, str]] = []
        self.validate_ok = validate_ok
        self.rollback_raises = rollback_raises

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        if path.endswith("/clone"):
            return {"number": 9}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad"]}
        if self.rollback_raises and path == f"/service/{SID}/version/5/activate":
            raise RuntimeError("rollback failed")
        return {}


def test_success_call_order_with_comment_and_status_messages():
    fake = _Fastly()
    msgs: list[str] = []
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", status_cb=msgs.append) as new_ver:
        assert new_ver == 9
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("PUT", f"/service/{SID}/version/9"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]
    assert msgs == ["Validating draft version 9...", "Activating version 9..."]


def test_no_comment_skips_the_comment_put():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake):
        pass
    assert ("PUT", f"/service/{SID}/version/9") not in fake.calls


def test_body_failure_rolls_back_to_base_and_never_validates():
    fake = _Fastly()
    with pytest.raises(ValueError, match="mutation boom"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("mutation boom")
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")
    assert not any(p.endswith("/validate") for _, p in fake.calls)


def test_validation_failure_raises_and_rolls_back_without_activating_draft():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake):
            pass
    assert ("PUT", f"/service/{SID}/version/9/activate") not in fake.calls
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_rollback_disabled_makes_no_rollback_call():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake, rollback=False):
            pass
    assert ("PUT", f"/service/{SID}/version/5/activate") not in fake.calls


def test_failed_rollback_does_not_mask_the_original_error():
    fake = _Fastly(rollback_raises=True)
    with pytest.raises(ValueError, match="original"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("original")


def test_comment_before_activate_puts_comment_between_validate_and_activate():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", comment_before_activate=True):
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]


def test_validate_false_skips_validation_entirely():
    fake = _Fastly(validate_ok=False)
    with draft_version(SID, "t", base_version=5, call=fake, validate=False):
        pass
    assert not any(p.endswith("/validate") for _, p in fake.calls)
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/9/activate")


def test_expect_empty_activate_applies_to_the_draft_activation_only():
    seen: list[tuple[str, dict]] = []

    def call(method, path, body=None, **kwargs):
        seen.append((path, kwargs))
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call, expect_empty_activate=True):
        pass
    activate = [kw for p, kw in seen if p.endswith("/version/9/activate")]
    assert activate == [{"token": "t", "expect_empty": True}]


def test_expect_empty_not_sent_by_default():
    seen: list[dict] = []

    def call(method, path, body=None, **kwargs):
        if path.endswith("/activate"):
            seen.append(kwargs)
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call):
        pass
    assert seen == [{"token": "t"}]


def test_custom_status_messages_replace_the_defaults():
    msgs: list[str] = []
    with draft_version(
        SID,
        "t",
        base_version=5,
        call=_Fastly(),
        status_cb=msgs.append,
        validate_msg="V{ver}",
        activate_msg="A{ver}",
    ):
        pass
    assert msgs == ["V9", "A9"]


def test_on_failure_runs_before_the_rollback_call():
    fake = _Fastly()
    seen_at: list[int] = []
    with pytest.raises(ValueError, match="boom"):
        with draft_version(
            SID,
            "t",
            base_version=5,
            call=fake,
            on_failure=lambda exc: seen_at.append(len(fake.calls)),
        ):
            raise ValueError("boom")
    assert seen_at == [1]
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_failed_rollback_is_logged_at_warning(caplog):
    import logging

    fake = _Fastly(rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(ValueError, match="original"):
            with draft_version(SID, "t", base_version=5, call=fake):
                raise ValueError("original")
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


def test_rollback_to_version_logs_and_swallows_runtime_error(caplog):
    import logging

    from backend.provision.utils import rollback_to_version

    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        rollback_to_version(SID, "t", 5, call=_Fastly(rollback_raises=True))
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


# ── Merged from tests/provision/test_sharing_domain.py ──
def test_deploy_remote_frontend_success():
    """deploy_remote_frontend issues the correct sequential calls via the shared fastly() client."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 80
    use_ssl = False
    token = "fake-token"

    # Sequential parsed-JSON responses returned by the shared client.
    responses = [
        {"id": "service123", "name": service_name},  # Create Service
        {"number": 1, "service_id": "service123"},  # Verify Draft Version
        {"name": domain_name},  # Attach Domain
        {"name": "gce_vm_origin"},  # Attach Backend Origin
        {"number": 1, "active": True},  # Activate Version
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        assert token == "fake-token"
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
        )

    assert res == {
        "service_id": "service123",
        "version": 1,
        "domain_name": domain_name,
        "origin_host": origin_host,
    }
    assert calls == [
        ("POST", "/service", {"name": service_name, "type": "vcl"}),
        ("POST", "/service/service123/version", None),
        ("POST", "/service/service123/version/1/domain", {"name": domain_name}),
        (
            "POST",
            "/service/service123/version/1/backend",
            {
                "name": "gce_vm_origin",
                "address": origin_host,
                "port": origin_port,
                "use_ssl": use_ssl,
                "ssl_check_cert": False,
            },
        ),
        ("PUT", "/service/service123/version/1/activate", None),
    ]


@pytest.mark.parametrize(
    "step_fail,client_err,expected_step",
    [
        (0, "HTTP 400 POST /service\n    Name already taken", "Create Service"),
        (1, "HTTP 400 POST /service/service123/version\n    Failed to create version", "Verify Draft Version"),
        (2, "HTTP 422 POST /service/service123/version/1/domain\n    Invalid domain name", "Attach Domain"),
        (3, "HTTP 400 POST /service/service123/version/1/backend\n    Invalid backend config", "Attach Backend Origin"),
        (4, "HTTP 500 PUT /service/service123/version/1/activate\n    Could not activate", "Activate Version"),
    ],
)
def test_deploy_remote_frontend_failures(step_fail, client_err, expected_step):
    """Any failed Fastly call is re-raised with the human-readable step name preserved."""
    token = "fake-token"

    ok_responses = [
        {"id": "service123"},
        {"number": 1},
        {},
        {},
        {"active": True},
    ]
    state = {"i": 0}

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        idx = state["i"]
        state["i"] += 1
        if idx == step_fail:
            raise RuntimeError(client_err)
        return ok_responses[idx]

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token=token,
            )

    msg = str(exc_info.value)
    assert f"Fastly API error during {expected_step}:" in msg
    # The underlying client error (HTTP code + body) is preserved.
    assert client_err in msg


def test_deploy_remote_frontend_missing_keys_in_json():
    """Error raised when service_id is missing from the create-service response."""
    with patch("backend.provision.fastly_api.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve service ID" in str(exc_info.value)


def test_deploy_remote_frontend_missing_version_number():
    """Error raised when the draft-version response has no number."""
    responses = [{"id": "service123"}, {"service_id": "service123"}]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend(
                service_name="test-service",
                domain_name="test-domain.global.ssl.fastly.net",
                origin_host="1.2.3.4",
                origin_port=80,
                use_ssl=False,
                token="fake-token",
            )
    assert "Failed to retrieve version number" in str(exc_info.value)


def test_deploy_remote_frontend_with_override_host_success():
    """override_host adds override_host; SNI/cert fields only when use_ssl is True."""
    service_name = "test-service"
    domain_name = "test-domain.global.ssl.fastly.net"
    origin_host = "1.2.3.4"
    origin_port = 443
    use_ssl = True
    token = "fake-token"
    override_host = "my-override.example.com"

    responses = [
        {"id": "service123", "name": service_name},
        {"number": 1},
        {"name": domain_name},
        {"name": "gce_vm_origin"},
        {"active": True},
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path, body))
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=origin_port,
            use_ssl=use_ssl,
            token=token,
            override_host=override_host,
        )

    assert res["service_id"] == "service123"
    backend_call = next(c for c in calls if c[1] == "/service/service123/version/1/backend")
    backend_payload = backend_call[2]
    assert backend_payload["override_host"] == override_host
    assert backend_payload["ssl_sni_hostname"] == override_host
    assert backend_payload["ssl_cert_hostname"] == override_host


def test_delete_remote_frontend_deactivates_then_deletes():
    """delete_remote_frontend deactivates active versions, then deletes the service."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 2, "active": True}, {"number": 1, "active": False}],  # Get Versions
        {},  # Deactivate Version 2
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("PUT", f"/service/{remote_service_id}/version/2/deactivate"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_no_active_versions():
    """No deactivation call is issued when there are no active versions."""
    token = "fake-token"
    remote_service_id = "remote123"
    responses = [
        [{"number": 1, "active": False}],  # Get Versions
        {"status": "ok"},  # Delete Service
    ]
    calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        calls.append((method, path))
        return responses.pop(0)

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)

    assert calls == [
        ("GET", f"/service/{remote_service_id}/version"),
        ("DELETE", f"/service/{remote_service_id}"),
    ]


def test_delete_remote_frontend_tolerates_404():
    """A 404 during version lookup/delete is swallowed (service already gone)."""

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        raise RuntimeError("Fastly API error during Get Versions: HTTP 404 GET /service/x/version\n    not found")

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        # Should not raise.
        delete_remote_frontend(remote_service_id="remote123", token="fake-token")


def test_delete_remote_frontend_reraises_non_404():
    """A non-404 error during deletion propagates."""
    responses = [[], RuntimeError("HTTP 500 DELETE /service/remote123\n    boom")]

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            delete_remote_frontend(remote_service_id="remote123", token="fake-token")
    assert "500" in str(exc_info.value)


# ── Merged from tests/provision/test_rum_orchestrator.py ──

"""Tests for RUM asset management and RUM orchestrator v2 provisioning."""


import copy
import hashlib
import logging
import re
from unittest.mock import AsyncMock

import pytest

from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.provision.rum_assets import (
    cleanup_old_faro_versions,
    detect_faro_version_change,
    download_and_upload_faro,
    faro_bundle_intact,
    faro_tracker_ready,
    generate_rum_tracker_js,
    upload_rum_tracker_js,
)
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from tests._helpers.config_fakes import (
    config_lock_held as _lock_held,
)
from tests._helpers.config_fakes import (
    mutate_via_save,
)
from tests._helpers.config_fakes import (
    read_persisted_config as _persisted,
)

FAKE_CFG = {
    "service_id": "svc_test",
    "fos_access_key_id": "AKIAFAKEEXAMPLE",
    "fos_secret_access_key": "fakeSecretKeyExample",
    "fos_bucket": "fake-test-bucket",
    "fos_region": "us-east-1",
}
_FOS_CFG = {
    "fos_bucket": "my-bucket",
    "fos_region": "us-east-1",
    "fos_access_key_id": "test_access_key",
    "fos_secret_access_key": "test_secret_key",
}
SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"
OTHER_BUNDLE = b"!function(e){/* a different, re-released build */}(window);"
_REAL_CLIENT = httpx.Client


def _mock_client_factory(handler):
    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_CLIENT(*args, **kwargs)

    return factory


@pytest.fixture
def mock_fos(monkeypatch):
    def install(handler):
        monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

    return install


@pytest.fixture
def mock_fetch_bundle(monkeypatch):
    def install(bundle: bytes = SAMPLE_BUNDLE, *, raise_if_called: bool = False):
        async def fake_fetch(version: str) -> bytes:
            if raise_if_called:
                raise AssertionError(f"fetch_faro_bundle must not be called for {version!r}")
            return bundle

        monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)

    return install


def _config_pair(monkeypatch, cfg: dict):
    saved: dict = {}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(
        rum_assets.svcconfig,
        "mutate_config",
        mutate_via_save(lambda sid, new_cfg: saved.update(service_id=sid, cfg=new_cfg)),
    )
    return saved


def _seed(cfg: dict) -> None:
    svcconfig.save_config(cfg["service_id"], dict(cfg))


def _disk(service_id: str = "srv_test") -> dict:
    return json.loads(svcconfig.config_path(service_id).read_text())


# ─── Faro Asset Download, Upload, Change Detection & Cleanup ─────────────────


async def test_download_and_upload_faro_flows(monkeypatch, mock_fos, mock_fetch_bundle):
    # 1. Happy path + preserves existing rum keys
    cfg = {**FAKE_CFG, "rum": {"sampling_rate": 0.5, "faro_version": "2.8.0", "faro_content_hash": "stale"}}
    saved = _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    seen: dict = {}
    mock_fos(lambda req: (seen.update(method=req.method, url=str(req.url), body=req.content), httpx.Response(200))[1])

    result = await download_and_upload_faro("svc_test", "2.9.0", "tok")
    expected_hash = hashlib.sha256(SAMPLE_BUNDLE).hexdigest()
    expected_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    assert result["version"] == "2.9.0"
    assert result["path"] == "rum/faro-web-sdk-v2.9.0.iife.js"
    assert result["bytes_uploaded"] == len(SAMPLE_BUNDLE)
    assert result["content_hash"] == expected_hash
    assert result["fos_key"] == "s3://fake-test-bucket/rum/faro-web-sdk-v2.9.0.iife.js"
    assert seen["method"] == "PUT" and seen["body"] == SAMPLE_BUNDLE
    assert saved["cfg"]["rum"]["sampling_rate"] == 0.5
    assert saved["cfg"]["rum"]["faro_version"] == "2.9.0"
    assert saved["cfg"]["rum"]["faro_content_hash"] == expected_hash
    assert saved["cfg"]["rum"]["faro_fos_etag_md5"] == expected_md5

    # 2. Deleted service config is not resurrected
    resurrected: list = []
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: dict(FAKE_CFG))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: None)
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: resurrected.append(a))
    await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert resurrected == []

    # 3. Upload failure raises RuntimeError and does not persist
    saved_fail = _config_pair(monkeypatch, FAKE_CFG)
    mock_fos(lambda req: httpx.Response(500, content=b"upstream error"))
    with pytest.raises(RuntimeError):
        await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert saved_fail == {}


@pytest.mark.parametrize(
    ("concurrent_version", "expected_updated", "expected_stored_version"),
    [
        ("3.0.0", False, "3.0.0"),
        ("2.9.0", True, "2.9.0"),
    ],
)
async def test_download_and_upload_faro_compare_and_set(
    monkeypatch, mock_fos, mock_fetch_bundle, concurrent_version, expected_updated, expected_stored_version
):
    store = {"svc_test": {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_content_hash": "old-hash"}}}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: copy.deepcopy(store.get(sid)))
    monkeypatch.setattr(
        rum_assets.svcconfig,
        "mutate_config",
        mutate_via_save(lambda sid, new_cfg: store.__setitem__(sid, copy.deepcopy(new_cfg))),
    )
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda req: httpx.Response(200))

    store["svc_test"]["rum"]["faro_version"] = concurrent_version
    result = await download_and_upload_faro("svc_test", "2.9.0", "tok", expected_current_version="2.9.0")
    assert result["config_updated"] is expected_updated
    assert store["svc_test"]["rum"]["faro_version"] == expected_stored_version


@pytest.mark.parametrize(
    ("rum_block", "bundle_bytes", "raise_if_called", "expected"),
    [
        (None, SAMPLE_BUNDLE, True, True),
        (
            {"faro_version": "2.8.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            SAMPLE_BUNDLE,
            True,
            True,
        ),
        ({"faro_version": "2.9.0", "faro_content_hash": None}, SAMPLE_BUNDLE, True, True),
        (
            {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            OTHER_BUNDLE,
            False,
            True,
        ),
        (
            {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()},
            SAMPLE_BUNDLE,
            False,
            False,
        ),
    ],
)
async def test_detect_faro_version_change(
    monkeypatch, mock_fetch_bundle, rum_block, bundle_bytes, raise_if_called, expected
):
    cfg = dict(FAKE_CFG)
    if rum_block is not None:
        cfg["rum"] = rum_block
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(bundle_bytes, raise_if_called=raise_if_called)
    assert await detect_faro_version_change("svc_test", "2.9.0") is expected


_LIST_RESPONSE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>fake-test-bucket</Name>
  <Contents><Key>rum/faro-web-sdk-v2.8.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.9.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.10.0.iife.js</Key></Contents>
</ListBucketResult>
"""


@pytest.mark.parametrize(
    ("keep_current", "expected_deleted"),
    [
        (True, ["rum/faro-web-sdk-v2.8.0.iife.js", "rum/faro-web-sdk-v2.9.0.iife.js"]),
        (
            False,
            [
                "rum/faro-web-sdk-v2.10.0.iife.js",
                "rum/faro-web-sdk-v2.8.0.iife.js",
                "rum/faro-web-sdk-v2.9.0.iife.js",
            ],
        ),
    ],
)
async def test_cleanup_old_faro_versions(monkeypatch, mock_fos, keep_current, expected_deleted):
    _config_pair(monkeypatch, {**FAKE_CFG, "rum": {"faro_version": "2.10.0", "faro_content_hash": "irrelevant"}})
    deleted_keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            assert "list-type=2" in str(request.url)
            return httpx.Response(200, content=_LIST_RESPONSE_TEMPLATE.encode())
        if request.method == "DELETE":
            deleted_keys.append(request.url.path.split("/", 2)[-1])
            return httpx.Response(204)
        raise AssertionError(f"unexpected {request.method}")

    mock_fos(handler)
    await cleanup_old_faro_versions("svc_test", keep_current=keep_current)
    assert sorted(deleted_keys) == expected_deleted


async def test_cleanup_old_faro_versions_swallows_error(monkeypatch, mock_fos):
    _config_pair(monkeypatch, {**FAKE_CFG, "rum": {"faro_version": "2.10.0"}})
    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("FOS unreachable", request=req)))
    assert await cleanup_old_faro_versions("svc_test") is None


def test_generate_rum_tracker_js_invariants():
    js = generate_rum_tracker_js("svc_test")
    assert "Faro.ErrorsInstrumentation" in js
    assert "if (Faro.ErrorInstrumentation)" not in js
    assert "new Faro.ErrorInstrumentation()" not in js
    assert "console.warn" in js and "no error instrumentation export found" in js
    assert "script.src = '/js/faro-sdk.js';" in js
    assert "cdn.jsdelivr.net" not in js and "unpkg.com" not in js
    assert not re.search(r"https?://", js)


def test_faro_tracker_readiness_and_upload_gates(monkeypatch, mock_fos, caplog):
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

    # 1. No version pinned
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"enabled": True}})
    assert ready is False and "no Faro version pinned" in reason

    # 2. Bundle missing in FOS (404)
    mock_fos(lambda req: httpx.Response(404))
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"}})
    assert ready is False and "not present/intact in FOS" in reason

    # 3. Intact bundle in FOS (200 + ETag match)
    cfg_ready = {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": etag_md5}}
    mock_fos(lambda req: httpx.Response(200, headers={"ETag": f'"{etag_md5}"'}))
    ready, reason = faro_tracker_ready(cfg_ready)
    assert ready is True and reason == ""
    assert faro_bundle_intact(cfg_ready, "2.9.0") is True

    # 4. Transient HEAD error logs concisely without traceback
    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("connection failed")))
    with caplog.at_level(logging.WARNING):
        assert faro_bundle_intact(cfg_ready, "2.9.0") is False
    rec = next(r for r in caplog.records if "integrity HEAD check failed" in r.getMessage())
    assert rec.exc_info is None and "ConnectError" in rec.getMessage()

    # 5. upload_rum_tracker_js skips when unpinned or bundle missing, publishes when ready
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: {})
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: {**FAKE_CFG, "rum": {"enabled": True}})
    mock_fos(lambda req: (_ for _ in ()).throw(AssertionError(f"unexpected {req.method}")))
    res_unpinned = upload_rum_tracker_js("svc_test", "tok")
    assert res_unpinned["skipped"] is True and res_unpinned["bytes_uploaded"] == 0

    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: dict(cfg_ready))
    mock_fos(lambda req: httpx.Response(404) if req.method == "HEAD" else (_ for _ in ()).throw(AssertionError()))
    assert upload_rum_tracker_js("svc_test", "tok")["skipped"] is True

    put_bodies: dict[str, bytes] = {}

    def _ready_handler(req: httpx.Request) -> httpx.Response:
        if req.method == "HEAD" and "faro-web-sdk-v" in req.url.path:
            return httpx.Response(200, headers={"ETag": f'"{etag_md5}"'})
        if req.method == "HEAD" and req.url.path.endswith("rum-tracker.js"):
            return httpx.Response(404)
        if req.method == "PUT" and req.url.path.endswith("rum-tracker.js"):
            put_bodies["tracker"] = req.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_ready_handler)
    res_pub = upload_rum_tracker_js("svc_test", "tok")
    assert "skipped" not in res_pub and res_pub["bytes_uploaded"] > 0
    assert "/js/faro-sdk.js" in put_bodies["tracker"].decode()


# ─── End-to-End Integration & Self-Heal Cron ─────────────────────────────────


@pytest.fixture
def temp_config_dir(tmp_path, monkeypatch):
    config_dir = Path(tmp_path) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)
    return config_dir


def test_faro_self_hosting_end_to_end_flows(monkeypatch, temp_config_dir, mock_fos, mock_fetch_bundle):
    service_id = "test_service_id"
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(
        json.dumps(
            {
                "service_id": service_id,
                "service_name": "Test Service",
                "rum_enabled": False,
                "last_activated_version": 1,
                **_FOS_CFG,
            }
        )
    )
    chosen_version = "2.8.5"
    mock_fetch_bundle(SAMPLE_BUNDLE)
    uploads: dict[str, bytes] = {}
    etag = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

    def _handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if req.method == "PUT" and "faro-web-sdk-v" in path:
            uploads["bundle"] = req.content
            return httpx.Response(200)
        if req.method == "HEAD" and "faro-web-sdk-v" in path:
            return httpx.Response(200, headers={"ETag": f'"{etag}"'})
        if req.method == "HEAD" and path.endswith("rum-tracker.js"):
            return httpx.Response(404)
        if req.method == "PUT" and path.endswith("rum-tracker.js"):
            uploads["tracker"] = req.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_handler)
    status_messages: list[str] = []
    with patch(
        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
        return_value=MagicMock(activated_version=2, draft_version=None, changes_applied={}),
    ) as mock_reconcile:
        result = enable_rum(service_id, "test_token", status_cb=status_messages.append, faro_version=chosen_version)

    cfg = json.loads(config_path.read_text())
    assert cfg["rum_enabled"] is True and "rum_enabled_at" in cfg
    assert cfg["rum"]["faro_version"] == chosen_version
    assert uploads["bundle"] == SAMPLE_BUNDLE
    tracker_js = uploads["tracker"].decode()
    assert "/js/faro-sdk.js" in tracker_js and not re.search(r"https?://", tracker_js)
    assert mock_reconcile.call_count == 1 and result["activated"] is True
    assert any("Using FOS bucket" in m for m in status_messages)
    assert any("Uploading RUM tracker JS" in m for m in status_messages)
    assert any("Reconciling VCL state" in m for m in status_messages)

    # Upgrade Faro version end-to-end
    mock_fetch_bundle(OTHER_BUNDLE)
    with (
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=3, draft_version=None, changes_applied={}),
        ),
        patch("backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock) as mock_clean,
    ):
        up_res = upgrade_faro_version(service_id, "2.9.0", "test_token")
    assert up_res["previous_version"] == chosen_version and up_res["version"] == "2.9.0"
    assert json.loads(config_path.read_text())["rum"]["faro_version"] == "2.9.0"
    mock_clean.assert_called_once_with(service_id, keep_current=True)

    # Disable RUM end-to-end
    with (
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_del,
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=6, changes_applied={}),
        ),
    ):
        dis_res = disable_rum(service_id, "test_token")
    assert dis_res["deactivated"] is True and dis_res["logging_service_active_version"] == 6
    assert json.loads(config_path.read_text())["rum_enabled"] is False
    mock_del.assert_called_once()


def test_reconcile_cron_self_heals_unpinned_service_without_thrashing(
    monkeypatch, temp_config_dir, mock_fos, mock_fetch_bundle
):
    from backend.provision.declarative.generators import generate_consolidated_snippet
    from backend.provision.declarative.state import FeatureState

    service_id = "self_heal_service"
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(
        json.dumps(
            {
                "service_id": service_id,
                "service_name": "Self Heal Service",
                "rum_enabled": True,
                "log_period": 60,
                "fastly_api_key": "test_token",
                "last_activated_version": 2,
                "rum": {},
                **_FOS_CFG,
            }
        )
    )
    mock_fetch_bundle(SAMPLE_BUNDLE)
    expected_etag = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    counts = {"put": 0, "head": 0}

    def _handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in req.url.path:
            counts["put"] += 1
            return httpx.Response(200)
        if req.method == "HEAD" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in req.url.path:
            counts["head"] += 1
            return httpx.Response(200, headers={"ETag": f'"{expected_etag}"'})
        raise AssertionError(f"unexpected {req.method} {req.url}")

    mock_fos(_handler)
    monkeypatch.setattr(rum_sync_mod.time, "time", lambda: 1_800_000_000.0)
    reconcile_calls: list[tuple] = []
    purge_calls: list[tuple] = []
    monkeypatch.setattr(
        "backend.provision.declarative.reconciler.reconcile_vcl_state",
        lambda sid, tok, *a, **kw: (
            reconcile_calls.append((sid, tok)),
            MagicMock(activated_version=3, draft_version=None),
        )[1],
    )
    monkeypatch.setattr(
        "backend.core.fastly.client.fastly",
        lambda method, path, *, token, expect_empty=False, **kw: purge_calls.append((method, path, token)),
    )

    rum_sync_mod._reconcile_faro_bundle(service_id, None)
    cfg = json.loads(config_path.read_text())
    assert cfg["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert counts == {"put": 1, "head": 1}
    assert reconcile_calls == [(service_id, "test_token")]
    assert purge_calls == [("POST", f"/service/{service_id}/purge/rum-faro-sdk", "test_token")]
    assert '"/js/faro-sdk.js"' in generate_consolidated_snippet(FeatureState.from_config(cfg), "vcl_recv")

    # Second tick: converged, no re-upload or re-reconcile
    rum_sync_mod._reconcile_faro_bundle(service_id, None)
    assert counts["put"] == 1 and reconcile_calls == [(service_id, "test_token")]


# ─── Orchestrator v2 Unit Tests (enable_rum / disable_rum / upgrade / sha) ───


def test_enable_and_disable_rum_early_return_and_missing_config():
    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": True, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res = enable_rum("srv_test", "tok")
        assert res["activated"] is False and res["logging_service_active_version"] == 5
        mock_rec.assert_not_called()

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": False, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res_dis = disable_rum("srv_test", "tok")
        assert res_dis["deactivated"] is False and res_dis["logging_service_active_version"] == 5
        mock_rec.assert_not_called()

    with patch("backend.config.load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            enable_rum("srv_nonexistent", "tok")


@pytest.mark.parametrize("missing", ["fos_bucket", "fos_region", "fos_access_key_id", "fos_secret_access_key"])
def test_enable_rum_missing_single_fos_setting_raises_before_any_save(missing):
    cfg = {
        "service_id": "srv_test",
        "rum_enabled": False,
        "rum": {"faro_version": "1.0.0"},
        "last_activated_version": 1,
        **_FOS_CFG,
    }
    del cfg[missing]
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config") as mock_save,
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock) as mock_faro,
    ):
        with pytest.raises(RuntimeError, match="missing FOS configuration"):
            enable_rum("srv_test", "test_token")
    mock_save.assert_not_called()
    mock_faro.assert_not_called()
    assert cfg["rum_enabled"] is False and cfg["rum"] == {"faro_version": "1.0.0"}


def test_enable_rum_ordering_and_non_blocking_tracker_upload():
    service_id = "srv_test"
    _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})
    call_order: list[str] = []

    async def _upload_faro(*_a, **_kw):
        call_order.append("upload_faro")
        return {"version": "2.9.0", "bytes_uploaded": 5000}

    def _reconcile(*_a, **_kw):
        assert _disk(service_id)["rum"]["faro_version"] == "2.9.0"
        call_order.append("reconcile")
        return MagicMock(activated_version=2, draft_version=None)

    def _upload_js(*_a, **_kw):
        call_order.append("upload_js")
        raise RuntimeError("FOS unreachable after activation")

    with (
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", side_effect=_upload_faro),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=_reconcile),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js", side_effect=_upload_js),
    ):
        result = enable_rum(service_id, "test_token", faro_version="2.9.0")

    assert call_order == ["upload_faro", "reconcile", "upload_js"]
    assert result["activated"] is True and _disk(service_id)["rum_enabled"] is True


def test_enable_and_disable_rum_activate_false_and_non_blocking_delete():
    service_id = "srv_test"
    _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})
    with (
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload_js,
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=3),
        ) as mock_rec,
    ):
        res_en = enable_rum(service_id, "test_token", activate=False)
        mock_rec.assert_called_once_with(service_id, "test_token", dry_run=False, status_cb=None, activate=False)
        mock_upload_js.assert_not_called()
        assert res_en["activated"] is False and res_en["logging_service_active_version"] == 3

    _seed({"service_id": service_id, "rum_enabled": True, "last_activated_version": 5, **_FOS_CFG})
    with (
        patch("backend.provision.rum_assets.delete_rum_tracker_js", side_effect=RuntimeError("FOS unreachable")),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=6),
        ) as mock_rec_dis,
    ):
        res_dis = disable_rum(service_id, "test_token", activate=False)
        mock_rec_dis.assert_called_once_with(service_id, "test_token", dry_run=False, status_cb=None, activate=False)
        assert res_dis["deactivated"] is False and res_dis["logging_service_active_version"] == 6
        assert _disk(service_id)["rum_enabled"] is False


def test_upgrade_faro_version_orchestration():
    # Invalid version rejected before config load
    with patch("backend.config.load_config") as mock_load:
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            enable_rum("srv_test", "tok", faro_version="not-a-version")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            upgrade_faro_version("srv_test", "not-a-version", "tok")
        mock_load.assert_not_called()

    # Refused when RUM not enabled
    with patch("backend.config.load_config", return_value={"service_id": "srv_test", "rum_enabled": False}):
        with pytest.raises(RuntimeError, match="RUM is not enabled"):
            upgrade_faro_version("srv_test", "2.0.0", "tok")

    # Happy path + purge failure non-blocking + never calls upload_rum_tracker_js + activate=False skips cleanup
    base_cfg = {
        "service_id": "srv_test",
        "rum_enabled": True,
        "cdn_service_id": "cdn_test_service",
        "rum": {"faro_version": "1.0.0", "faro_content_hash": "abc123"},
        "last_activated_version": 5,
        **_FOS_CFG,
    }
    call_order: list[str] = []
    with (
        patch("backend.config.load_config", return_value=dict(base_cfg)),
        patch("backend.config.save_config"),
        patch(
            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
            side_effect=AsyncMock(side_effect=lambda *a, **k: (call_order.append("upload"), {"version": "2.0.0"})[1]),
        ),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            side_effect=lambda *a, **k: (
                call_order.append("reconcile"),
                MagicMock(activated_version=7, draft_version=None),
            )[1],
        ),
        patch(
            "backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key", side_effect=RuntimeError("purge down")
        ),
        patch(
            "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
            side_effect=AsyncMock(side_effect=lambda *a, **k: call_order.append("cleanup")),
        ),
        patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_tracker,
    ):
        res = upgrade_faro_version("srv_test", "2.0.0", "tok")
    assert call_order == ["upload", "reconcile", "cleanup"]
    mock_tracker.assert_not_called()
    assert res["previous_version"] == "1.0.0" and res["version"] == "2.0.0" and res["activated"] is True

    # activate=False skips cleanup and sha update
    with (
        patch("backend.config.load_config", return_value=dict(base_cfg)),
        patch("backend.config.save_config") as mock_save,
        patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", new_callable=AsyncMock),
        patch(
            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
            return_value=MagicMock(activated_version=None, draft_version=8),
        ),
        patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge,
        patch("backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock) as mock_clean,
    ):
        res_draft = upgrade_faro_version("srv_test", "2.0.0", "tok", activate=False)
    mock_clean.assert_not_called()
    mock_purge.assert_called_once()
    mock_save.assert_not_called()
    assert res_draft["activated"] is False and res_draft["logging_service_active_version"] == 8


def test_rum_vcl_fingerprint_invariants(monkeypatch):
    def _cfg(**overrides):
        return {
            "service_id": "srv_test",
            "fos_region": "us-east-1",
            "cdn_shield": "",
            "rum": {"faro_version": "2.9.0"},
            **overrides,
        }

    assert rum_vcl_fingerprint("srv_test", _cfg(rum={"faro_version": "2.9.0"})) != rum_vcl_fingerprint(
        "srv_test", _cfg(rum={"faro_version": "3.0.0"})
    )
    assert rum_vcl_fingerprint("srv_test", _cfg(rum={})) != rum_vcl_fingerprint(
        "srv_test", _cfg(rum={"faro_version": "2.9.0"})
    )
    c = _cfg()
    assert rum_vcl_fingerprint("srv_test", c) == rum_vcl_fingerprint("srv_test", c)
    assert rum_vcl_fingerprint("srv_test", _cfg(fos_region="us-east-1")) != rum_vcl_fingerprint(
        "srv_test", _cfg(fos_region="uk-east-1")
    )

    disk_cfg = _cfg(rum={"faro_version": "1.2.3"})
    monkeypatch.setattr("backend.config.load_config", lambda sid: disk_cfg)
    assert rum_vcl_fingerprint("srv_test") == rum_vcl_fingerprint("srv_test", disk_cfg)


# ─── Config Merge & Lock Hygiene (test_rum_orchestrator_v2_config_merge.py) ──

SID_rum_orchestrator = "logsvc1"
OLD_VERSION = "1.0.0"
NEW_VERSION = "1.2.3"
_FOS = {"fos_bucket": "bkt", "fos_region": "us-east-1", "fos_access_key_id": "AK", "fos_secret_access_key": "SK"}


def _concurrent_writer(**top_level):
    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("rum", {})["concurrent_rum_key"] = "kept"

    assert not _lock_held(), "config lock held across a Fastly/FOS call"
    svcconfig.mutate_config(SID_rum_orchestrator, _apply)


@pytest.fixture
def disabled_cfg():
    svcconfig.save_config(
        SID_rum_orchestrator, {"service_id": SID_rum_orchestrator, "keep_me": 1, "rum_enabled": False, **_FOS}
    )
    return svcconfig.load_config(SID_rum_orchestrator)


@pytest.fixture
def enabled_cfg():
    svcconfig.save_config(
        SID_rum_orchestrator,
        {
            "service_id": SID_rum_orchestrator,
            "keep_me": 1,
            "rum_enabled": True,
            "rum_vcl_sha": "stale",
            "rum": {"faro_version": OLD_VERSION, "faro_content_hash": "oldhash", "faro_fos_etag_md5": "oldmd5"},
            **_FOS,
        },
    )
    return svcconfig.load_config(SID_rum_orchestrator)


@pytest.fixture
def stubs():
    mod = "backend.provision.rum_orchestrator_v2"
    with (
        patch(f"{mod}.download_and_upload_faro", new_callable=AsyncMock) as upload,
        patch(f"{mod}.reconcile_vcl_state") as reconcile,
        patch(f"{mod}.upload_rum_tracker_js") as tracker,
        patch(f"{mod}.cleanup_old_faro_versions", new_callable=AsyncMock),
        patch(f"{mod}._purge_faro_surrogate_key"),
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as delete_js,
    ):
        reconcile.return_value = MagicMock(activated_version=7, draft_version=None)
        yield MagicMock(upload=upload, reconcile=reconcile, tracker=tracker, delete_js=delete_js)


def test_enable_and_disable_persist_owned_keys_without_holding_lock(disabled_cfg, stubs):
    salt = disabled_cfg["rum"]["cid_salt"]
    held: list[bool] = []

    async def _upload(*_a, **_kw):
        held.append(_lock_held())

    def _reconcile(*_a, **_kw):
        held.append(_lock_held())
        return MagicMock(activated_version=7, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    out = enable_rum(SID_rum_orchestrator, "tok", faro_version=NEW_VERSION)
    assert held == [False, False]
    saved = _persisted()
    assert saved["rum_enabled"] is True and saved["rum_enabled_at"] == out["enabled_at"]
    assert saved["rum"]["faro_version"] == NEW_VERSION and saved["rum"]["cid_salt"] == salt
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved) and saved["keep_me"] == 1

    # Now disable and check lock + owned keys
    held.clear()
    stubs.delete_js.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    stubs.reconcile.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), MagicMock(activated_version=2))[1]
    disable_rum(SID_rum_orchestrator, "tok")
    assert held == [False, False]
    saved_dis = _persisted()
    assert saved_dis["rum_enabled"] is False and "rum_vcl_sha" not in saved_dis and saved_dis["keep_me"] == 1


@pytest.mark.parametrize("fail_stage", ["upload", "reconcile"])
def test_enable_rollback_keeps_concurrent_writes_and_restores_pin(disabled_cfg, stubs, fail_stage):
    salt = disabled_cfg["rum"]["cid_salt"]
    if fail_stage == "upload":

        async def _boom(*_a, **_kw):
            _concurrent_writer(concurrent_key="kept")
            raise RuntimeError("upload failed")

        stubs.upload.side_effect = _boom
    else:

        def _boom(*_a, **_kw):
            _concurrent_writer(concurrent_key="kept")
            raise RuntimeError("reconcile failed")

        stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError):
        enable_rum(SID_rum_orchestrator, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False and "rum_vcl_sha" not in saved
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept" and saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == salt

    # Also verify pre-existing pin restoration on reconcile failure
    svcconfig.save_config(
        SID_rum_orchestrator,
        {"service_id": SID_rum_orchestrator, "rum_enabled": False, "rum": {"faro_version": OLD_VERSION}, **_FOS},
    )
    stubs.upload.side_effect = None
    stubs.reconcile.side_effect = RuntimeError("nope")
    with pytest.raises(RuntimeError):
        enable_rum(SID_rum_orchestrator, "tok", faro_version=NEW_VERSION)
    assert _persisted()["rum"]["faro_version"] == OLD_VERSION


def test_upgrade_and_disable_concurrent_write_preservation(enabled_cfg, stubs):
    # 1. Upgrade reconcile failure restores pin and keeps concurrent writes
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(
            SID_rum_orchestrator,
            lambda c: c["rum"].update(
                faro_version=NEW_VERSION, faro_content_hash="newhash", faro_fos_etag_md5="newmd5"
            ),
        )

    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _boom
    with pytest.raises(RuntimeError, match="reconcile failed"):
        upgrade_faro_version(SID_rum_orchestrator, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum"]["faro_version"] == OLD_VERSION
    assert saved["rum"]["faro_content_hash"] == "oldhash"
    assert saved["rum"]["faro_fos_etag_md5"] == "oldmd5"
    assert saved["rum"]["concurrent_rum_key"] == "kept" and saved["concurrent_key"] == "kept"

    # 2. Upgrade success refreshes fingerprint and keeps concurrent writes
    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        _concurrent_writer(concurrent_key="kept2"),
        MagicMock(activated_version=9, draft_version=None),
    )[1]
    upgrade_faro_version(SID_rum_orchestrator, NEW_VERSION, "tok")
    saved_ok = _persisted()
    assert (
        saved_ok["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved_ok)
        and saved_ok["rum_vcl_sha"] != "stale"
    )
    assert saved_ok["concurrent_key"] == "kept2"

    # 3. Disable rollback restores enabled and keeps concurrent writes
    stubs.reconcile.side_effect = _boom
    with pytest.raises(RuntimeError, match="reconcile failed"):
        disable_rum(SID_rum_orchestrator, "tok")
    saved_dis = _persisted()
    assert saved_dis["rum_enabled"] is True
    assert saved_dis["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved_dis)
    assert saved_dis["concurrent_key"] == "kept"
