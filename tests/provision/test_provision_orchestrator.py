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
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.rum_ingest import DEFAULT_FARO_VERSION

from backend import config as svcconfig
from backend.core.fastly.client import fastly as real_fastly
from backend.core.log_fields import resolve_enabled_fields
from backend.provision import orchestrator, rum_assets
from backend.provision.declarative.reconciler import FeatureState, generate_consolidated_snippet
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
    storage_side_effect=None,
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
        mock_storage = stack.enter_context(
            patch("backend.core.storage.init_storage_table", side_effect=storage_side_effect)
        )
        yield {"upload": mock_upload, "storage": mock_storage}


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


def test_build_log_fields_config_variants():
    cfg = orchestrator._build_log_fields_config(_ns())
    assert cfg["preset"] == "standard" and cfg["schema_version"] == 2
    assert isinstance(cfg["groups"], list) and cfg["groups"] == sorted(cfg["groups"])

    cfg_dup = orchestrator._build_log_fields_config(_ns(preset="standard", enable_group=["L", "L"]))
    assert cfg_dup["groups"].count("L") <= 1

    cfg_dis = orchestrator._build_log_fields_config(_ns(preset="standard", disable_group=["A"]))
    assert "A" not in cfg_dis["groups"]

    cfg_ovr = orchestrator._build_log_fields_config(_ns(enable_field=["fld.a", "fld.b"], disable_field=["fld.b"]))
    assert cfg_ovr["field_overrides"]["fld.a"] is True and cfg_ovr["field_overrides"]["fld.b"] is False

    sec_cfg = orchestrator._build_log_fields_config(_ns(preset="security"))
    assert sec_cfg["field_overrides"].get("tls_ciphers_sha") is True
    assert "cookie_session" not in resolve_enabled_fields(sec_cfg)

    optin_cfg = orchestrator._build_log_fields_config(_ns(preset="security", enable_field=["cookie_session"]))
    assert optin_cfg["field_overrides"].get("cookie_session") is True
    assert "cookie_session" in resolve_enabled_fields(optin_cfg)


# ── write_service_config tests ──


def test_write_service_config_options_and_defaults(tmp_path):
    assert _write_cfg(tmp_path, _make_state(log_period=10))["provisioning"]["cron_sync"]["interval_seconds"] == 10
    assert _write_cfg(tmp_path, _make_state(log_period=120))["provisioning"]["cron_sync"]["interval_seconds"] == 120
    assert _write_cfg(tmp_path, _make_state(log_period=2))["provisioning"]["cron_sync"]["interval_seconds"] == 5

    cfg = _write_cfg(tmp_path, _make_state(log_period=60, commit_interval_mins=15))
    assert cfg["provisioning"]["cron_sync"]["commit_interval_mins"] == 15

    state = _make_state()
    state.pop("log_period", None)
    state["provisioning"] = {"log_period": 120}
    assert _write_cfg(tmp_path, state)["log_period"] == 120

    cfg_local = _write_cfg(tmp_path, _make_state(storage_mode="local"))
    assert cfg_local["storage_mode"] == "local" and "access_level" not in cfg_local

    state_noname = _make_state()
    state_noname.pop("service_name", None)
    state_noname.pop("name", None)
    assert _write_cfg(tmp_path, state_noname)["name"] == "svc-test"

    cfg_cron = _write_cfg(tmp_path, _make_state(enable_cron_sync=False, enable_cron_compact=False, delete_after=False))
    assert cfg_cron["provisioning"]["cron_sync"]["enabled"] is False
    assert cfg_cron["provisioning"]["cron_compact"]["enabled"] is False
    assert cfg_cron["provisioning"]["cron_sync"]["delete_after"] is False

    cfg_ret = _write_cfg(tmp_path, _make_state(log_retention_days=90))
    assert cfg_ret["log_retention_days"] == 90
    assert cfg_ret["provisioning"]["cron_sync"]["log_retention_days"] == 90
    assert cfg_ret["provisioning"]["cron_compact"]["log_retention_days"] == 90

    assert _write_cfg(tmp_path, _make_state(rate_limiting=False))["provisioning"]["rate_limiting"] is False
    assert _write_cfg(tmp_path, _make_state())["provisioning"]["rate_limiting"] is True
    assert (
        _write_cfg(tmp_path, _make_state(provisioning={"rate_limiting": False}))["provisioning"]["rate_limiting"]
        is False
    )


# ── run_with_events & state persistence tests ──


def test_run_with_events_and_state_persistence():
    def fake_func(status_cb=None):
        status_cb("step one")
        status_cb("step two")
        return "ok"

    assert list(orchestrator.run_with_events(fake_func)) == [
        {"type": "status", "message": "step one"},
        {"type": "status", "message": "step two"},
    ]

    def fail_func(status_cb=None):
        status_cb("about to fail")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        list(orchestrator.run_with_events(fail_func))

    captured = {}

    def driver():
        captured["v"] = yield from orchestrator.run_with_events(lambda status_cb=None: "got-it")

    for _ in driver():
        pass
    assert captured["v"] == "got-it"

    assert orchestrator.load_state() == {}
    orchestrator.save_state({"k": "v", "n": 42})
    assert os.path.exists(orchestrator._state_file_path())
    assert orchestrator.load_state() == {"k": "v", "n": 42}

    Path(orchestrator._state_file_path()).write_text("{not valid json")
    assert orchestrator.load_state() == {}

    with patch("builtins.open", side_effect=OSError("read-only fs")):
        orchestrator.save_state({"k": "v"})


# ── cleanup_local_data tests ──


def test_cleanup_local_data_variants(tmp_path, monkeypatch):
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=False)
    assert not cfg_file.exists()

    db, wal = tmp_path / "x.duckdb", tmp_path / "x.duckdb.wal"
    db.write_text("")
    wal.write_text("")
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(db)),
        patch("backend.core.metadata.teardown", side_effect=RuntimeError("locked")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=True)
    assert not db.exists() and not wal.exists()

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


def test_cleanup_local_data_drops_ducklake_state_only_with_remove_data(tmp_path):
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "no.duckdb")),
        patch("backend.core.metadata.teardown"),
        patch("backend.core.duckdb.drop_service_ducklake_state", return_value={"tables_dropped": 3}) as drop,
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=False)
        drop.assert_not_called()
        orchestrator.cleanup_local_data("svc", remove_data=True)
    drop.assert_called_once_with("svc")


def test_cleanup_local_data_continues_when_ducklake_cleanup_fails(tmp_path):
    db = tmp_path / "x.duckdb"
    db.write_text("")
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(db)),
        patch("backend.core.metadata.teardown"),
        patch("backend.core.duckdb.drop_service_ducklake_state", side_effect=RuntimeError("catalog down")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=True)
    assert not db.exists()


def test_sync_crontab_tolerates_missing_scheduler_and_busy_lease():
    with patch("backend.cron.scheduler.get_scheduler", side_effect=ImportError("no scheduler")):
        orchestrator._sync_crontab()

    class _Sched:
        def reload(self):
            raise CronLeaseHeldError("scheduler busy")

    with patch("backend.cron.scheduler.get_scheduler", return_value=_Sched()):
        orchestrator._sync_crontab()


# ── provision() tests ──


def test_provision_preflight_and_happy_path(tmp_path, monkeypatch):
    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG"]),
        patch("backend.provision.orchestrator.ensure_fos_access_key") as mock_create_key,
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None and events[-1]["type"] == "error" and "Log format" in events[-1]["message"]
    mock_create_key.assert_not_called()

    monkeypatch.chdir(tmp_path)
    with _mocked_provision_steps(write_side_effect=None) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None
    progress_events = [e for e in events if e["type"] == "progress"]
    assert len(progress_events) == 9 and progress_events[0]["current"] == 0 and progress_events[-1]["current"] == 8
    assert events[-1]["type"] == "done" and "complete" in events[-1]["message"].lower()

    with _mocked_provision_steps(write_side_effect=None, upload_faro_side_effect=None) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None
    env["upload"].assert_not_called()


def test_provision_faro_upload_ordering_and_default_version(tmp_path, monkeypatch):
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

    uploaded_versions: list[str] = []
    with _mocked_provision_steps(
        write_side_effect=None,
        upload_faro_side_effect=lambda sid, ver, tok, **kw: (uploaded_versions.append(ver), {"version": ver})[1],
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg(rum_enabled=True, rum={"enabled": True})))
    assert exc is None and uploaded_versions == [DEFAULT_FARO_VERSION]


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


def test_provision_faro_upload_failure_and_hash_persistence(tmp_path, monkeypatch):
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
    assert json.loads((config_dir / "svc-prov-test.json").read_text())["rum"]["faro_version"] == "1.2.3"

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


def test_provision_rollback_resume_and_storage_warnings(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    teardown_called = []

    def fake_teardown(state, token, opts=None):
        teardown_called.append(True)
        yield {"type": "status", "message": "rollback step"}

    with (
        _mocked_provision_steps(
            bucket_side_effect=RuntimeError("BucketAlreadyExists"), teardown_side_effect=fake_teardown
        ),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert isinstance(exc, RuntimeError) and "BucketAlreadyExists" in str(exc) and teardown_called == [True]
    assert any("BucketAlreadyExists" in e["message"] for e in events if e["type"] == "error")

    orchestrator.save_state(
        {"temp_admin_key_id": "PREV_ID", "temp_admin_access_key": "PREV_AK", "temp_admin_secret_key": "PREV_SK"}
    )
    captured_state = {}
    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            side_effect=lambda desc, state, token, **kw: (
                captured_state.update(state=dict(state)),
                _fos_key("NEW"),
            )[1],
        ),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=RuntimeError("abort early")),
        patch("backend.provision.orchestrator.perform_teardown", return_value=iter([])),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        _consume(orchestrator.provision(_provision_cfg(), _resume_from_state=True))
    assert captured_state["state"]["temp_admin_key_id"] == "PREV_ID"

    with _mocked_provision_steps(write_side_effect=None, source_return=None) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None and events[-1]["type"] == "done" and not env["storage"].called
    assert any(e["type"] == "status" and "not initialized" in e["message"].lower() for e in events)

    with _mocked_provision_steps(
        write_side_effect=None,
        source_return={"name": "svc", "service_id": "sid", "logging_service_id": "lsid"},
        storage_side_effect=RuntimeError("FOS not ready"),
    ) as env:
        events, exc = _consume(orchestrator.provision(_provision_cfg()))
    assert exc is None and events[-1]["type"] == "done"
    env["storage"].assert_called_once()
    assert any(e["type"] == "status" and "deferred to first commit" in e["message"].lower() for e in events)


# ── perform_teardown() tests ──


def test_perform_teardown_variants(caplog):
    with _mock_teardown_env() as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))
    assert exc is None and not env["scoring"].called
    assert env["remove_logging"].called and env["delete_bucket"].called and env["delete_cdn"].called
    assert len([e for e in events if e["type"] == "progress"]) == 5

    with _mock_teardown_env() as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(cdn_service_id="svc-td-test"), "tok"))
    assert exc is None and not env["delete_cdn"].called and "refusing to delete CDN service" in caplog.text

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

    with _mock_teardown_env() as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                state,
                "tok",
                opts={"remove_logging": True, "remove_cdn": True, "remove_bucket": True, "remove_scoring": False},
            )
        )
    assert exc is None and not env["scoring"].called

    with _mock_teardown_env(scoring_se=RuntimeError("scoring upstream 503")) as env:
        events, exc = _consume(orchestrator.perform_teardown(state, "tok"))
    assert exc is None and env["delete_cdn"].called
    aborted = [e for e in events if e["type"] == "status" and "aborted" in e["message"].lower()]
    assert aborted and "left in place" in aborted[0]["message"].lower()

    with _mock_teardown_env() as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(), "tok", opts={"remove_logging": False, "remove_cdn": True, "remove_bucket": True}
            )
        )
    assert exc is None and not env["remove_logging"].called

    fastly_paths = []
    with _mock_teardown_env(fastly_se=lambda *a, **_kw: (fastly_paths.append(a[1]), {"data": []})[1]) as env:
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(), "tok", opts={"remove_logging": True, "remove_cdn": True, "remove_bucket": False}
            )
        )
    assert exc is None and not env["delete_bucket"].called and not any("access-keys" in p for p in fastly_paths)

    with _mock_teardown_env(remove_logging_se=RuntimeError("upstream 502")) as env:
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))
    assert exc is None and env["delete_bucket"].called and any("Warning" in e.get("message", "") for e in events)


def test_write_service_config_preserves_scoring_and_ngwaf(tmp_path, monkeypatch):
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
    assert "my_field" in custom_names and not (_SCORING_FIELD_NAMES & custom_names)
    all_field_names = {f["name"] for f in FeatureState.from_config(cfg).log_fields.custom_fields}
    assert _SCORING_FIELD_NAMES <= all_field_names

    cfg_none = _write_cfg(tmp_path, _make_state(), existing_cfg=None)
    assert "scoring" not in cfg_none and "ngwaf_workspace_id" not in cfg_none

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    for svc_id, ws_id, extra in [
        ("svc-ngwaf-preserve", "ws_preserved_xyz", {}),
        ("svc-ngwaf-routing", "ws_existing", {"ngwaf_workspace_id": "ws_in_state_should_be_ignored"}),
    ]:
        svcconfig.save_config(
            svc_id,
            {
                "service_id": svc_id,
                "ngwaf_workspace_id": ws_id,
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
                **extra,
            )
        )
        assert svcconfig.load_config(svc_id).get("ngwaf_workspace_id") == ws_id


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


def test_fos_path_safety_and_reconciler_edge_cases(tmp_path, monkeypatch):
    for bad in ("../etc", "a/b", "a\\b", "..", "a\x00b"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_bucket", bad, allow_slash=False)
    _reject_unsafe_fos_component("fos_bucket", "my-fos-bucket", allow_slash=False)
    _reject_unsafe_fos_component("fos_bucket", "", allow_slash=False)
    _reject_unsafe_fos_component("fos_prefix", "raw/logs/2026/", allow_slash=True)
    for bad in ("../up", "a\\b", "..", "a\x00"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_prefix", bad, allow_slash=True)

    with pytest.raises(ValueError, match="ingest state missing"):
        orchestrator.write_service_config({})

    cfg = _write_cfg(tmp_path, _make_state(rum_enabled=True), existing_cfg=None)
    assert cfg.get("rum_enabled") is True and cfg["rum"].get("enabled") is True and cfg["rum"].get("enabled_at")

    with pytest.raises(ValueError, match="state missing logging_service_id"):
        next(orchestrator.ensure_logging_via_reconciler({}, "tok"))

    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", side_effect=KeyError("missing_field")),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        with pytest.raises(RuntimeError, match="Invalid feature state configuration"):
            next(orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok"))

    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", return_value=None),
        patch("backend.provision.orchestrator.reconcile_vcl_state", side_effect=RuntimeError("VCL validation failed")),
    ):
        with pytest.raises(RuntimeError, match="VCL reconciliation failed"):
            next(orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok"))

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

    with (
        patch("backend.provision.declarative.state.FeatureState.from_config", return_value=None),
        patch(
            "backend.provision.orchestrator.reconcile_vcl_state",
            return_value=type("Result", (), {"activated_version": None})(),
        ),
        patch("backend.core.fastly.service.get_active_version", return_value=42),
    ):
        assert orchestrator.ensure_logging_via_reconciler({"logging_service_id": "svc-test"}, "tok") == 42

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
        orchestrator.cleanup_local_data("svc", bucket="../../../tmp", remove_data=True)
        orchestrator.cleanup_local_data("svc", bucket="a/b", remove_data=True)
        orchestrator.cleanup_local_data("svc", bucket="bucket_symlink", remove_data=True)
    assert not cfg_file.exists() and symlink.exists()


# ── Fastly failure & retry cassette tests ──


@pytest.mark.parametrize(
    ("cassette", "path", "expected"),
    [
        (
            "fastly_429_then_success.yaml",
            "/service/SU3xxxxxxxxxxxxxx0000",
            [{"id": "SU3xxxxxxxxxxxxxx0000", "name": "Test CDN", "active_version": {"number": 42}}],
        ),
        (
            "fastly_503_then_success.yaml",
            "/account",
            [{"id": "acct-1", "name": "Test Account"}],
        ),
    ],
)
def test_provision_survives_fastly_transient_errors_with_retry(tmp_path, monkeypatch, cassette, path, expected):
    monkeypatch.chdir(tmp_path)
    real_call_results = []
    with (
        patch("tenacity.nap.time.sleep"),
        _my_vcr.use_cassette(cassette),
        _mocked_provision_steps(
            bucket_side_effect=lambda *a, **kw: real_call_results.append(
                real_fastly("GET", path, token="test-token-not-real")
            ),
            write_side_effect=None,
        ),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg_provision_fastly_failures()))
    assert exc is None and events[-1]["type"] == "done" and real_call_results == expected


def test_provision_partial_failure_at_step_7_and_early_config_persist(tmp_path, monkeypatch):
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

    assert isinstance(exc, RuntimeError) and "config write failed mid-flow" in str(exc) and len(teardown_calls) == 1
    ts = teardown_calls[0]["state"]
    assert (ts["activated_logging_version"], ts["logging_service_id"], ts["cdn_service_id"], ts["fos_key_id"]) == (
        99,
        "SU3xxxxxxxxxxxxxx0000",
        "cdn-svc-id",
        "ID_P",
    )
    assert any("config write failed mid-flow" in e["message"] for e in events if e["type"] == "error")

    write_mock = MagicMock()
    calls_before_cdn = None
    with _mocked_provision_steps(write_side_effect=write_mock):
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
    rollback_to_version,
    rum_log_path,
    rum_minute_list_prefix,
)

from backend.provision import cmcd_fields as orch
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend


def test_vcl_builder_and_cmcd_fields_and_log_paths():
    assert VclBuilder.join_lines(["a", "b", "c"]) == "a\nb\nc"
    sections = ["a", "", "  ", "b"]
    assert VclBuilder.join_sections(sections) == "\n\n".join(s for s in sections if s.strip())

    cfg = {"log_fields": {"custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True)}}
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names
    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names) and "mine" in names

    for empty_cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(empty_cfg)

    assert analytics_log_path() == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    assert rum_log_path() == "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
    assert minute_list_prefix(dt) == "raw/request/year=2026/month=08/day=27/hour=16/minute=53/"
    assert rum_minute_list_prefix(dt) == "raw/rum/year=2026/month=08/day=27/hour=16/minute=53/"
    assert ANALYTICS_LOG_LAYOUT == "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def test_producers_use_canonical_layout_and_rum_asset_routing():
    import backend.provision.declarative.generators as gen
    import backend.routers.services.core as sc

    import backend.provision.fastly_api as fa

    for mod in (gen, fa, sc):
        src = inspect.getsource(mod)
        assert "raw/%Y-%m-%d" not in src
    assert "log_paths" in inspect.getsource(gen)

    for faro_ver in (None, "2.9.0"):
        state = FeatureState(
            service_id="svc_test",
            log_period=60,
            sample_rate=100,
            edge_only=False,
            custom_condition="",
            fos_prefix="",
            fos_endpoint="fos.example.com",
            logging_enabled=False,
            rum_enabled=True,
            faro_version=faro_ver,
        )
        shipped_recv = generate_consolidated_snippet(state, "vcl_recv")
        shared_copy = generate_rum_asset_fetch_vcl("iad-va-us", faro_version=faro_ver)[RUM_ASSET_FETCH_NAME]
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


def test_draft_version_lifecycle_and_options(caplog):
    import logging

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

    fake_nocomment = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake_nocomment):
        pass
    assert ("PUT", f"/service/{SID}/version/9") not in fake_nocomment.calls

    fake_before = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake_before, comment="c", comment_before_activate=True):
        fake_before.calls.append(("BODY", "mutate"))
    assert fake_before.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]

    fake_noval = _Fastly(validate_ok=False)
    with draft_version(SID, "t", base_version=5, call=fake_noval, validate=False):
        pass
    assert not any(p.endswith("/validate") for _, p in fake_noval.calls)
    assert fake_noval.calls[-1] == ("PUT", f"/service/{SID}/version/9/activate")

    for expect_empty, expected_kw in [(True, {"token": "t", "expect_empty": True}), (False, {"token": "t"})]:
        seen: list[tuple[str, dict]] = []
        with draft_version(
            SID,
            "t",
            base_version=5,
            call=lambda m, p, body=None, **kw: (
                seen.append((p, kw)),
                {"number": 9} if p.endswith("/clone") else {"status": "ok"},
            )[1],
            expect_empty_activate=expect_empty,
        ):
            pass
        assert [kw for p, kw in seen if p.endswith("/version/9/activate")] == [expected_kw]

    custom_msgs: list[str] = []
    with draft_version(
        SID,
        "t",
        base_version=5,
        call=_Fastly(),
        status_cb=custom_msgs.append,
        validate_msg="V{ver}",
        activate_msg="A{ver}",
    ):
        pass
    assert custom_msgs == ["V9", "A9"]

    fake_fail = _Fastly()
    seen_at: list[int] = []
    with pytest.raises(ValueError, match="mutation boom"):
        with draft_version(
            SID, "t", base_version=5, call=fake_fail, on_failure=lambda exc: seen_at.append(len(fake_fail.calls))
        ):
            raise ValueError("mutation boom")
    assert seen_at == [1] and fake_fail.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")
    assert not any(p.endswith("/validate") for _, p in fake_fail.calls)

    for rb, expect_rb in [(True, True), (False, False)]:
        fake_vfail = _Fastly(validate_ok=False)
        with pytest.raises(RuntimeError, match="Validation failed"):
            with draft_version(SID, "t", base_version=5, call=fake_vfail, rollback=rb):
                pass
        assert ("PUT", f"/service/{SID}/version/9/activate") not in fake_vfail.calls
        assert (("PUT", f"/service/{SID}/version/5/activate") in fake_vfail.calls) is expect_rb

    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(ValueError, match="original"):
            with draft_version(SID, "t", base_version=5, call=_Fastly(rollback_raises=True)):
                raise ValueError("original")
        assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)
        caplog.clear()
        rollback_to_version(SID, "t", 5, call=_Fastly(rollback_raises=True))
        assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


# ── Merged from tests/provision/test_sharing_domain.py ──


def test_deploy_and_delete_remote_frontend_flows():
    service_name, domain_name, origin_host = "test-service", "test-domain.global.ssl.fastly.net", "1.2.3.4"
    responses = [
        {"id": "service123", "name": service_name},
        {"number": 1, "service_id": "service123"},
        {"name": domain_name},
        {"name": "gce_vm_origin"},
        {"number": 1, "active": True},
    ]
    calls: list[tuple] = []
    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=lambda m, p, body=None, *, token, **kw: (calls.append((m, p, body)), responses.pop(0))[1],
    ):
        res = deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=80,
            use_ssl=False,
            token="fake-token",
        )
    assert res == {"service_id": "service123", "version": 1, "domain_name": domain_name, "origin_host": origin_host}
    assert len(calls) == 5 and calls[0] == ("POST", "/service", {"name": service_name, "type": "vcl"})

    # With override_host + SSL
    resp_ssl = [{"id": "service123"}, {"number": 1}, {"name": domain_name}, {"name": "gce_vm_origin"}, {"active": True}]
    calls_ssl: list[tuple] = []
    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=lambda m, p, body=None, *, token, **kw: (calls_ssl.append((m, p, body)), resp_ssl.pop(0))[1],
    ):
        deploy_remote_frontend(
            service_name=service_name,
            domain_name=domain_name,
            origin_host=origin_host,
            origin_port=443,
            use_ssl=True,
            token="fake-token",
            override_host="my-override.example.com",
        )
    backend_payload = next(c[2] for c in calls_ssl if c[1] == "/service/service123/version/1/backend")
    assert (
        backend_payload["override_host"]
        == backend_payload["ssl_sni_hostname"]
        == backend_payload["ssl_cert_hostname"]
        == "my-override.example.com"
    )

    # Missing ID / version number
    with patch("backend.provision.fastly_api.fastly", return_value={"not_id": "service123"}):
        with pytest.raises(RuntimeError, match="Failed to retrieve service ID"):
            deploy_remote_frontend(service_name, domain_name, origin_host, 80, False, "fake-token")
    resp_nover = [{"id": "service123"}, {"service_id": "service123"}]
    with patch("backend.provision.fastly_api.fastly", side_effect=lambda *a, **k: resp_nover.pop(0)):
        with pytest.raises(RuntimeError, match="Failed to retrieve version number"):
            deploy_remote_frontend(service_name, domain_name, origin_host, 80, False, "fake-token")

    # Delete remote frontend: active version, no active version, 404, 500
    for vers, expected_calls in [
        (
            [{"number": 2, "active": True}, {"number": 1, "active": False}],
            [("GET", "/service/r1/version"), ("PUT", "/service/r1/version/2/deactivate"), ("DELETE", "/service/r1")],
        ),
        ([{"number": 1, "active": False}], [("GET", "/service/r1/version"), ("DELETE", "/service/r1")]),
    ]:
        del_resp = [vers, {}, {"status": "ok"}] if len(expected_calls) == 3 else [vers, {"status": "ok"}]
        del_calls: list[tuple] = []
        with patch(
            "backend.provision.fastly_api.fastly",
            side_effect=lambda m, p, body=None, **kw: (del_calls.append((m, p)), del_resp.pop(0))[1],
        ):
            delete_remote_frontend(remote_service_id="r1", token="fake-token")
        assert del_calls == expected_calls

    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=RuntimeError("Fastly API error during Get Versions: HTTP 404 GET /service/x/version"),
    ):
        delete_remote_frontend(remote_service_id="r1", token="fake-token")

    with patch(
        "backend.provision.fastly_api.fastly",
        side_effect=[[], RuntimeError("HTTP 500 DELETE /service/r1\n    boom")],
    ):
        with pytest.raises(RuntimeError, match="500"):
            delete_remote_frontend(remote_service_id="r1", token="fake-token")


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
    ok_responses = [{"id": "service123"}, {"number": 1}, {}, {}, {"active": True}]
    state = {"i": 0}

    def fake_fastly(method, path, body=None, *, token, **kwargs):
        idx = state["i"]
        state["i"] += 1
        if idx == step_fail:
            raise RuntimeError(client_err)
        return ok_responses[idx]

    with patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly):
        with pytest.raises(RuntimeError) as exc_info:
            deploy_remote_frontend("test-service", "test-domain.global.ssl.fastly.net", "1.2.3.4", 80, False, "tok")
    assert f"Fastly API error during {expected_step}:" in str(exc_info.value) and client_err in str(exc_info.value)


# ── Merged from tests/provision/test_rum_orchestrator.py ──

import copy
import hashlib
import logging
import re
from unittest.mock import AsyncMock

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

from backend.cron.jobs import rum_sync as rum_sync_mod
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


@pytest.fixture
def mock_fos(monkeypatch):
    def install(handler):
        monkeypatch.setattr(
            httpx, "Client", lambda *a, **kw: _REAL_CLIENT(*a, **{**kw, "transport": httpx.MockTransport(handler)})
        )

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


async def test_download_and_upload_faro_flows(monkeypatch, mock_fos, mock_fetch_bundle):
    cfg = {**FAKE_CFG, "rum": {"sampling_rate": 0.5, "faro_version": "2.8.0", "faro_content_hash": "stale"}}
    saved = _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    seen: dict = {}
    mock_fos(lambda req: (seen.update(method=req.method, url=str(req.url), body=req.content), httpx.Response(200))[1])

    result = await download_and_upload_faro("svc_test", "2.9.0", "tok")
    expected_hash = hashlib.sha256(SAMPLE_BUNDLE).hexdigest()
    expected_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    assert (result["version"], result["path"], result["bytes_uploaded"], result["content_hash"]) == (
        "2.9.0",
        "rum/faro-web-sdk-v2.9.0.iife.js",
        len(SAMPLE_BUNDLE),
        expected_hash,
    )
    assert result["fos_key"] == "s3://fake-test-bucket/rum/faro-web-sdk-v2.9.0.iife.js"
    assert seen["method"] == "PUT" and seen["body"] == SAMPLE_BUNDLE
    assert saved["cfg"]["rum"] == {
        "sampling_rate": 0.5,
        "faro_version": "2.9.0",
        "faro_content_hash": expected_hash,
        "faro_fos_etag_md5": expected_md5,
    }

    resurrected: list = []
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda sid: dict(FAKE_CFG))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: None)
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: resurrected.append(a))
    await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert resurrected == []

    saved_fail = _config_pair(monkeypatch, FAKE_CFG)
    mock_fos(lambda req: httpx.Response(500, content=b"upstream error"))
    with pytest.raises(RuntimeError):
        await download_and_upload_faro("svc_test", "2.9.0", "tok")
    assert saved_fail == {}


@pytest.mark.parametrize(
    ("concurrent_version", "expected_updated", "expected_stored_version"),
    [("3.0.0", False, "3.0.0"), ("2.9.0", True, "2.9.0")],
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
    assert (
        result["config_updated"] is expected_updated
        and store["svc_test"]["rum"]["faro_version"] == expected_stored_version
    )


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
    cfg = {**FAKE_CFG, **({"rum": rum_block} if rum_block is not None else {})}
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(bundle_bytes, raise_if_called=raise_if_called)
    assert await detect_faro_version_change("svc_test", "2.9.0") is expected


_LIST_RESPONSE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>fake-test-bucket</Name>
  <Contents><Key>rum/faro-web-sdk-v2.8.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.9.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.10.0.iife.js</Key></Contents>
</ListBucketResult>"""


@pytest.mark.parametrize(
    ("keep_current", "expected_deleted"),
    [
        (True, ["rum/faro-web-sdk-v2.8.0.iife.js", "rum/faro-web-sdk-v2.9.0.iife.js"]),
        (
            False,
            ["rum/faro-web-sdk-v2.10.0.iife.js", "rum/faro-web-sdk-v2.8.0.iife.js", "rum/faro-web-sdk-v2.9.0.iife.js"],
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
        assert request.method == "DELETE"
        deleted_keys.append(request.url.path.split("/", 2)[-1])
        return httpx.Response(204)

    mock_fos(handler)
    await cleanup_old_faro_versions("svc_test", keep_current=keep_current)
    assert sorted(deleted_keys) == expected_deleted


async def test_cleanup_old_faro_versions_swallows_error(monkeypatch, mock_fos):
    _config_pair(monkeypatch, {**FAKE_CFG, "rum": {"faro_version": "2.10.0"}})
    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("FOS unreachable", request=req)))
    assert await cleanup_old_faro_versions("svc_test") is None


def test_generate_rum_tracker_js_invariants():
    js = generate_rum_tracker_js("svc_test")
    assert "Faro.ErrorsInstrumentation" in js and "if (Faro.ErrorInstrumentation)" not in js
    assert "new Faro.ErrorInstrumentation()" not in js and "no error instrumentation export found" in js
    assert "script.src = '/js/faro-sdk.js';" in js and not re.search(r"https?://", js)


def test_faro_tracker_readiness_and_upload_gates(monkeypatch, mock_fos, caplog):
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"enabled": True}})
    assert ready is False and "no Faro version pinned" in reason

    mock_fos(lambda req: httpx.Response(404))
    ready, reason = faro_tracker_ready({**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"}})
    assert ready is False and "not present/intact in FOS" in reason

    cfg_ready = {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": etag_md5}}
    mock_fos(lambda req: httpx.Response(200, headers={"ETag": f'"{etag_md5}"'}))
    assert faro_tracker_ready(cfg_ready) == (True, "") and faro_bundle_intact(cfg_ready, "2.9.0") is True

    mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("connection failed")))
    with caplog.at_level(logging.WARNING):
        assert faro_bundle_intact(cfg_ready, "2.9.0") is False
    rec = next(r for r in caplog.records if "integrity HEAD check failed" in r.getMessage())
    assert rec.exc_info is None and "ConnectError" in rec.getMessage()

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
    assert (
        "skipped" not in res_pub
        and res_pub["bytes_uploaded"] > 0
        and "/js/faro-sdk.js" in put_bodies["tracker"].decode()
    )


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
    assert cfg["rum_enabled"] is True and "rum_enabled_at" in cfg and cfg["rum"]["faro_version"] == chosen_version
    assert uploads["bundle"] == SAMPLE_BUNDLE
    tracker_js = uploads["tracker"].decode()
    assert "/js/faro-sdk.js" in tracker_js and not re.search(r"https?://", tracker_js)
    assert mock_reconcile.call_count == 1 and result["activated"] is True
    assert all(
        any(sub in m for m in status_messages)
        for sub in ("Using FOS bucket", "Uploading RUM tracker JS", "Reconciling VCL state")
    )

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
    assert cfg["rum"]["faro_version"] == DEFAULT_FARO_VERSION and counts == {"put": 1, "head": 1}
    assert reconcile_calls == [(service_id, "test_token")]
    assert purge_calls == [("POST", f"/service/{service_id}/purge/rum-faro-sdk", "test_token")]
    assert '"/js/faro-sdk.js"' in generate_consolidated_snippet(FeatureState.from_config(cfg), "vcl_recv")

    rum_sync_mod._reconcile_faro_bundle(service_id, None)
    assert counts["put"] == 1 and reconcile_calls == [(service_id, "test_token")]


def test_enable_and_disable_rum_early_return_and_missing_config():
    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": True, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res = enable_rum("srv_test", "tok")
        assert res["activated"] is False and res["logging_service_active_version"] == 5 and not mock_rec.called

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "srv_test", "rum_enabled": False, "last_activated_version": 5},
        ),
        patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_rec,
    ):
        res_dis = disable_rum("srv_test", "tok")
        assert (
            res_dis["deactivated"] is False and res_dis["logging_service_active_version"] == 5 and not mock_rec.called
        )

    with patch("backend.config.load_config", return_value=None), pytest.raises(RuntimeError, match="No config found"):
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
        pytest.raises(RuntimeError, match="missing FOS configuration"),
    ):
        enable_rum("srv_test", "test_token")
    assert (
        not mock_save.called
        and not mock_faro.called
        and cfg["rum_enabled"] is False
        and cfg["rum"] == {"faro_version": "1.0.0"}
    )


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
        assert (
            not mock_upload_js.called and res_en["activated"] is False and res_en["logging_service_active_version"] == 3
        )

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
        assert (
            res_dis["deactivated"] is False
            and res_dis["logging_service_active_version"] == 6
            and _disk(service_id)["rum_enabled"] is False
        )


def test_upgrade_faro_version_orchestration():
    with patch("backend.config.load_config") as mock_load:
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            enable_rum("srv_test", "tok", faro_version="not-a-version")
        with pytest.raises(ValueError, match="faro_version must be a plain"):
            upgrade_faro_version("srv_test", "not-a-version", "tok")
        mock_load.assert_not_called()

    with (
        patch("backend.config.load_config", return_value={"service_id": "srv_test", "rum_enabled": False}),
        pytest.raises(RuntimeError, match="RUM is not enabled"),
    ):
        upgrade_faro_version("srv_test", "2.0.0", "tok")

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
    assert call_order == ["upload", "reconcile", "cleanup"] and not mock_tracker.called
    assert res["previous_version"] == "1.0.0" and res["version"] == "2.0.0" and res["activated"] is True

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
    assert not mock_clean.called and mock_purge.call_count == 1 and not mock_save.called
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


SID_rum_orchestrator = "logsvc1"
OLD_VERSION = "1.0.0"
NEW_VERSION = "1.2.3"
_FOS = {"fos_bucket": "bkt", "fos_region": "us-east-1", "fos_access_key_id": "AK", "fos_secret_access_key": "SK"}


def _concurrent_writer(**top_level):
    assert not _lock_held(), "config lock held across a Fastly/FOS call"
    svcconfig.mutate_config(
        SID_rum_orchestrator,
        lambda c: (c.update(top_level), c.setdefault("rum", {}).__setitem__("concurrent_rum_key", "kept")),
    )


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
    stubs.upload.side_effect = AsyncMock(side_effect=lambda *_a, **_kw: held.append(_lock_held()))
    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        MagicMock(activated_version=7, draft_version=None),
    )[1]

    out = enable_rum(SID_rum_orchestrator, "tok", faro_version=NEW_VERSION)
    assert held == [False, False]
    saved = _persisted()
    assert saved["rum_enabled"] is True and saved["rum_enabled_at"] == out["enabled_at"]
    assert saved["rum"]["faro_version"] == NEW_VERSION and saved["rum"]["cid_salt"] == salt
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved) and saved["keep_me"] == 1

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
        stubs.upload.side_effect = AsyncMock(
            side_effect=lambda *_a, **_kw: (
                _concurrent_writer(concurrent_key="kept"),
                (_ for _ in ()).throw(RuntimeError("upload failed")),
            )
        )
    else:
        stubs.reconcile.side_effect = lambda *_a, **_kw: (
            _concurrent_writer(concurrent_key="kept"),
            (_ for _ in ()).throw(RuntimeError("reconcile failed")),
        )

    with pytest.raises(RuntimeError):
        enable_rum(SID_rum_orchestrator, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False and "rum_vcl_sha" not in saved and "faro_version" not in saved["rum"]
    assert (
        saved["concurrent_key"] == "kept"
        and saved["rum"]["concurrent_rum_key"] == "kept"
        and saved["rum"]["cid_salt"] == salt
    )

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
    stubs.upload.side_effect = AsyncMock(
        side_effect=lambda *_a, **_kw: svcconfig.mutate_config(
            SID_rum_orchestrator,
            lambda c: c["rum"].update(
                faro_version=NEW_VERSION, faro_content_hash="newhash", faro_fos_etag_md5="newmd5"
            ),
        )
    )
    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        _concurrent_writer(concurrent_key="kept"),
        (_ for _ in ()).throw(RuntimeError("reconcile failed")),
    )
    with pytest.raises(RuntimeError, match="reconcile failed"):
        upgrade_faro_version(SID_rum_orchestrator, NEW_VERSION, "tok")

    saved = _persisted()
    assert (saved["rum"]["faro_version"], saved["rum"]["faro_content_hash"], saved["rum"]["faro_fos_etag_md5"]) == (
        OLD_VERSION,
        "oldhash",
        "oldmd5",
    )
    assert saved["rum"]["concurrent_rum_key"] == "kept" and saved["concurrent_key"] == "kept"

    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        _concurrent_writer(concurrent_key="kept2"),
        MagicMock(activated_version=9, draft_version=None),
    )[1]
    upgrade_faro_version(SID_rum_orchestrator, NEW_VERSION, "tok")
    saved_ok = _persisted()
    assert (
        saved_ok["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved_ok)
        and saved_ok["rum_vcl_sha"] != "stale"
        and saved_ok["concurrent_key"] == "kept2"
    )

    stubs.reconcile.side_effect = lambda *_a, **_kw: (
        _concurrent_writer(concurrent_key="kept"),
        (_ for _ in ()).throw(RuntimeError("reconcile failed")),
    )
    with pytest.raises(RuntimeError, match="reconcile failed"):
        disable_rum(SID_rum_orchestrator, "tok")
    saved_dis = _persisted()
    assert (
        saved_dis["rum_enabled"] is True
        and saved_dis["rum_vcl_sha"] == rum_vcl_fingerprint(SID_rum_orchestrator, saved_dis)
        and saved_dis["concurrent_key"] == "kept"
    )


def test_cli_restore_data_runs_the_cloud_restore_for_the_service():
    from typer.testing import CliRunner

    from backend.provision.cli import app

    src = {"service_id": "svc-r", "name": "svc-r", "bucket": "bkt"}
    result = {"remote_files": 4, "downloaded": 3, "registered": 4, "reindexed": 7}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.storage.backup.restore_from_cloud", return_value=result) as restore,
    ):
        out = CliRunner().invoke(app, ["restore-data", "--service-id", "svc-r"])
    assert (
        out.exit_code == 0 and "4 cloud file(s): 3 downloaded, 4 registered, 7 raw object(s) re-indexed" in out.output
    )
    assert restore.call_args.args[0] == src

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        assert CliRunner().invoke(app, ["restore-data", "--service-id", "nope"]).exit_code != 0
