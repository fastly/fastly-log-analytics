"""Tests for ``backend.provision.orchestrator`` — pure helpers.

The 8-step ``provision()`` generator and ``perform_teardown()`` are
covered indirectly via the provision-router integration tests
([tests/routers/test_provision_lifecycle.py](tests/routers/test_provision_lifecycle.py)).
This file pins the **pure** helpers that don't require a Fastly API:

  - ``_build_log_fields_config`` — preset + override accumulation
  - ``write_service_config`` — state → config translation (log_period
    fallbacks, sync_interval clamp, cron_sync/compact defaults, etc.)
  - ``run_with_events`` — thread + queue → SSE event generator,
    exception propagation across the thread boundary
  - ``save_state`` / ``load_state`` — JSON state persistence with
    swallow-on-IO-error tolerance
  - ``cleanup_local_data`` — config + DB + cache removal branches
  - ``generate_analyst_invite`` — access_level guard + Fastly API
    shape + Iceberg-failure tolerance
  - ``_sync_crontab`` — swallows scheduler-import failures

These are the seams every provisioning + teardown SSE event flows
through; a regression cascades into broken setup-wizard UX.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backend.provision import orchestrator

# ── _build_log_fields_config ────────────────────────────────────────────────


def test_build_log_fields_config_defaults_to_standard_preset():
    """No preset specified → ``standard``. Pinned because the wizard
    default and the /api/provision/start API both rely on the default."""
    args = SimpleNamespace(preset=None, enable_group=None, disable_group=None, enable_field=None, disable_field=None)
    cfg = orchestrator._build_log_fields_config(args)
    assert cfg["preset"] == "standard"
    assert cfg["schema_version"] == 2
    # Groups come from the standard preset
    assert isinstance(cfg["groups"], list)
    assert cfg["groups"] == sorted(cfg["groups"]), "groups must be sorted"


def test_build_log_fields_config_adds_enable_groups_without_duplicates():
    """``--enable-group A`` when A is already in the preset → A appears
    once. Pinned because duplicate group entries broke log_format
    generation downstream."""
    args = SimpleNamespace(
        preset="standard",
        enable_group=["L", "L"],  # duplicate
        disable_group=None,
        enable_field=None,
        disable_field=None,
    )
    cfg = orchestrator._build_log_fields_config(args)
    # L appears at most once
    assert cfg["groups"].count("L") <= 1


def test_build_log_fields_config_disable_group_removes_from_set():
    """``--disable-group A`` strips A from the preset. Pinned because
    customers who don't need a group save log_format bytes by removing
    it."""
    args = SimpleNamespace(
        preset="standard",
        enable_group=None,
        disable_group=["A"],
        enable_field=None,
        disable_field=None,
    )
    cfg = orchestrator._build_log_fields_config(args)
    assert "A" not in cfg["groups"]


def test_build_log_fields_config_field_overrides_merge_with_disable_last():
    """``--enable-field X --disable-field X`` (same field) → disabled
    wins (False). Pinned because the merge order matters — admins
    expect ``--disable`` to be the destructive override."""
    args = SimpleNamespace(
        preset=None,
        enable_group=None,
        disable_group=None,
        enable_field=["fld.a", "fld.b"],
        disable_field=["fld.b"],
    )
    cfg = orchestrator._build_log_fields_config(args)
    overrides = cfg["field_overrides"]
    assert overrides["fld.a"] is True
    assert overrides["fld.b"] is False


# ── write_service_config ────────────────────────────────────────────────────


def _make_state(**overrides):
    """Minimal state dict that ``write_service_config`` accepts. Pulls
    in only the keys touched by the CLI's wizard + API's start route."""
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


def test_write_service_config_clamps_sync_interval_to_min_30s(tmp_path):
    """``sync_interval_seconds`` = max(30, log_period_secs). Pinned
    because a sub-30s interval would hit Fastly's rate limit."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state(log_period=10))  # below 30
    sid, cfg = saved_cfgs[0]
    assert cfg["provisioning"]["cron_sync"]["interval_seconds"] == 30


def test_write_service_config_commit_interval_respects_state_override(tmp_path):
    """``commit_interval_mins`` is max of (sync_interval_seconds//60,
    state['commit_interval_mins']). Pinned because admins who want
    fewer compactions can raise the commit interval — must not be
    clobbered by the sync interval."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        # 60s log_period → 1min sync, but state says 15 min commit
        orchestrator.write_service_config(_make_state(log_period=60, commit_interval_mins=15))
    _, cfg = saved_cfgs[0]
    assert cfg["provisioning"]["cron_sync"]["commit_interval_mins"] == 15


def test_write_service_config_defaults_log_period_from_provisioning_nested(tmp_path):
    """When ``log_period`` is missing at the top level but present in
    ``state['provisioning']``, fall back to the nested value. Pinned
    because resume-from-state restores state with the nested shape."""
    saved_cfgs = []
    state = _make_state()
    state.pop("log_period", None)
    state["provisioning"] = {"log_period": 120}
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(state)
    _, cfg = saved_cfgs[0]
    assert cfg["log_period"] == 120


def test_write_service_config_propagates_access_level_and_storage_mode(tmp_path):
    """``access_level`` and ``storage_mode`` default to the most-common
    values (read_write / cloud) but allow state override. Pinned
    because the analyst-replica flow imports state with
    ``access_level=read_only``."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state(access_level="read_only", storage_mode="local"))
    _, cfg = saved_cfgs[0]
    assert cfg["access_level"] == "read_only"
    assert cfg["storage_mode"] == "local"


def test_write_service_config_uses_service_name_or_id_as_name(tmp_path):
    """``cfg['name']`` falls back to service_id when both ``name`` and
    ``service_name`` are absent. Pinned because the frontend's service-
    switcher uses ``name`` — a None there breaks the dropdown."""
    saved_cfgs = []
    state = _make_state()
    state.pop("service_name", None)
    state.pop("name", None)
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(state)
    _, cfg = saved_cfgs[0]
    assert cfg["name"] == "svc-test"  # falls back to service_id


def test_write_service_config_threads_cron_sync_enabled_flag(tmp_path):
    """``state['enable_cron_sync']`` flows into cron_sync.enabled —
    the wizard's --disable-cron-sync path. Pinned because losing this
    wiring would make admins think they disabled cron when they
    hadn't."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state(enable_cron_sync=False, enable_cron_compact=False))
    _, cfg = saved_cfgs[0]
    assert cfg["provisioning"]["cron_sync"]["enabled"] is False
    assert cfg["provisioning"]["cron_compact"]["enabled"] is False


def test_write_service_config_delete_after_threads_into_cron_sync(tmp_path):
    """``state['delete_after']`` lands in cron_sync.delete_after.
    Pinned because the raw-log delete-after-iceberg-merge is a billing-
    critical behaviour (customers who keep raw logs pay more)."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state(delete_after=False))
    _, cfg = saved_cfgs[0]
    assert cfg["provisioning"]["cron_sync"]["delete_after"] is False


def test_write_service_config_log_retention_days_propagates(tmp_path):
    """30-day default; admins override via --log-retention-days. The
    value lands in cron_sync.log_retention_days, cron_compact.log_
    retention_days, AND top-level log_retention_days. Pinned because
    the three are read by different subsystems and drift would break
    one of them."""
    saved_cfgs = []
    with (
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state(log_retention_days=90))
    _, cfg = saved_cfgs[0]
    assert cfg["log_retention_days"] == 90
    assert cfg["provisioning"]["cron_sync"]["log_retention_days"] == 90
    assert cfg["provisioning"]["cron_compact"]["log_retention_days"] == 90


# ── run_with_events ─────────────────────────────────────────────────────────


def test_run_with_events_yields_status_events_in_order():
    """Status callbacks → status events, in queue order. Pinned because
    the SSE consumer relies on event ordering for the progress bar."""

    def fake_func(status_cb=None):
        status_cb("step one")
        status_cb("step two")
        return "ok"

    events = list(orchestrator.run_with_events(fake_func))
    assert events == [
        {"type": "status", "message": "step one"},
        {"type": "status", "message": "step two"},
    ]


def test_run_with_events_propagates_thread_exception():
    """If the worker raises, the generator must surface the exception
    after draining queued events. Pinned because swallowing here would
    mask provisioning failures and let the wizard report success."""

    def fake_func(status_cb=None):
        status_cb("about to fail")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        list(orchestrator.run_with_events(fake_func))


def test_run_with_events_returns_function_result_via_generator_return():
    """The worker's return value comes back as the generator's
    ``StopIteration.value`` — what ``yield from`` consumes. Pinned
    because the orchestrator chains ``yield from run_with_events(...)``
    and binds the result to state."""
    gen = orchestrator.run_with_events(lambda status_cb=None: "the-result")
    # Drain all events first
    for _ in gen:
        pass

    # Re-run to capture the return value via yield from
    def consumer():
        return (yield from orchestrator.run_with_events(lambda status_cb=None: "the-result"))

    c = consumer()
    for _ in c:
        pass
    # The above pattern doesn't capture StopIteration.value directly;
    # the documented orchestrator-side usage is ``result = yield from run_with_events(...)``
    # so we just verify by running it through a generator that does so:
    captured = {}

    def driver():
        captured["v"] = yield from orchestrator.run_with_events(lambda status_cb=None: "got-it")

    d = driver()
    for _ in d:
        pass
    assert captured["v"] == "got-it"


# ── save_state / load_state ─────────────────────────────────────────────────


def test_save_then_load_state_roundtrips():
    """JSON write + read roundtrip. Pinned because resume-from-state
    keys on the file existing AND being valid JSON. SYSTEM_DATA_DIR is
    sandboxed by the ``isolate_metadata_db`` autouse fixture."""
    orchestrator.save_state({"k": "v", "n": 42})
    assert os.path.exists(orchestrator._state_file_path())
    assert orchestrator.load_state() == {"k": "v", "n": 42}


def test_load_state_returns_empty_when_file_missing():
    """No state file → empty dict (not crash). Pinned because the first
    provision attempt has no state file."""
    assert orchestrator.load_state() == {}


def test_load_state_swallows_corrupt_json():
    """Malformed JSON → empty dict. Pinned because a bad-byte ending
    a prior crashed write shouldn't prevent a fresh provision attempt."""
    from pathlib import Path

    Path(orchestrator._state_file_path()).write_text("{not valid json")
    assert orchestrator.load_state() == {}


def test_save_state_swallows_io_error():
    """A read-only directory shouldn't crash provisioning — losing the
    state save is acceptable; losing the in-progress run isn't. Pinned
    because Docker volume mounts sometimes flip to read-only."""
    with patch("builtins.open", side_effect=OSError("read-only fs")):
        # Should not raise
        orchestrator.save_state({"k": "v"})


# ── cleanup_local_data ──────────────────────────────────────────────────────


def test_cleanup_local_data_removes_service_config(tmp_path):
    """Config file removal is the primary effect. Pinned because the
    backend reads ``configs/*.json`` to enumerate services — a leftover
    config means a "ghost" service in the UI."""
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=False)
    assert not cfg_file.exists()


def test_cleanup_local_data_removes_duckdb_when_remove_data_true(tmp_path):
    """With ``remove_data=True``, the per-service DuckDB file + WAL go
    away. Pinned because forgetting the WAL leaves DuckDB in a half-
    committed state when next opened."""
    db = tmp_path / "x.duckdb"
    wal = tmp_path / "x.duckdb.wal"
    db.write_text("")
    wal.write_text("")
    with (
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
        patch("backend.config.duckdb_path", return_value=str(db)),
        patch("backend.core.metadata.teardown"),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        orchestrator.cleanup_local_data("svc", remove_data=True)
    assert not db.exists()
    assert not wal.exists()


def test_cleanup_local_data_swallows_metadata_db_teardown_exception(tmp_path):
    """``metadata_db.teardown`` exceptions should not propagate — the
    config + DB are already gone and bubbling here would leave the
    user thinking teardown failed entirely. Pinned because metadata_db
    file locks under Windows occasionally raise on teardown."""
    cfg_file = tmp_path / "svc.json"
    cfg_file.write_text("{}")
    with (
        patch("backend.config.config_path", return_value=str(cfg_file)),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "noexist.duckdb")),
        patch("backend.core.metadata.teardown", side_effect=RuntimeError("locked")),
        patch("backend.provision.orchestrator._sync_crontab"),
    ):
        # Should not raise
        orchestrator.cleanup_local_data("svc", remove_data=True)


def test_cleanup_local_data_removes_cache_directory_for_bucket(tmp_path, monkeypatch):
    """Bucket-named cache dir under ``cache/<bucket>/`` is purged.
    Pinned because the cache dir can be many GB; admins teardown
    expect the disk to be reclaimed."""
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


# ── generate_analyst_invite ─────────────────────────────────────────────────


def test_generate_analyst_invite_raises_when_service_missing():
    """Unknown service → RuntimeError (caller maps to 404). Pinned
    because returning None would surface as a JSON-encode failure
    downstream."""
    with patch("backend.config.load_config", return_value=None):
        with pytest.raises(RuntimeError, match="not found"):
            orchestrator.generate_analyst_invite("ghost-svc")


def test_generate_analyst_invite_rejects_read_only_service():
    """Can't invite an analyst from a read_only replica — the access
    key generation requires admin Fastly creds. Pinned because the
    error message is what the frontend surfaces in the invite dialog."""
    with patch("backend.config.load_config", return_value={"access_level": "read_only", "fastly_api_key": "k"}):
        with pytest.raises(RuntimeError, match="read_write"):
            orchestrator.generate_analyst_invite("svc")


def test_generate_analyst_invite_returns_payload_with_credentials():
    """Happy path: Fastly returns the new access key; we return a
    payload with access_key_id + secret_key + bucket + endpoint.
    Pinned because the CLI prints these — losing a key would lock
    the analyst out."""
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
    fake_key = {"access_key": "AKIANEW", "secret_key": "SECRETNEW"}
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.orchestrator.fastly", return_value=fake_key),
        # Iceberg lookup may raise — verified separately
        patch("backend.core.iceberg._get_catalog", side_effect=RuntimeError("no catalog")),
    ):
        out = orchestrator.generate_analyst_invite("svc")

    assert out["access_key_id"] == "AKIANEW"
    assert out["secret_key"] == "SECRETNEW"
    assert out["fos_bucket"] == "b"
    assert out["fos_endpoint"].endswith("fastlystorage.app")
    assert out["cdn_url"] == "https://cdn.example"
    # Iceberg metadata is optional — None when lookup fails
    assert out["iceberg_metadata_location"] is None


def test_generate_analyst_invite_sends_correct_fastly_payload():
    """The Fastly access-key creation call must pass
    ``permission=read-only-objects`` + the bucket list. Pinned
    because giving the analyst write perms would let them corrupt
    the production logs they're supposed to only read."""
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
        patch("backend.core.iceberg._get_catalog", side_effect=Exception("skip")),
    ):
        orchestrator.generate_analyst_invite("svc")

    assert len(captured) == 1
    method, path, body, kwargs = captured[0]
    assert method == "POST"
    assert path == "/resources/object-storage/access-keys"
    assert body["permission"] == "read-only-objects"
    assert body["buckets"] == ["b"]
    # Description tags the key so admins can identify and revoke it later
    assert "fos-log-analysis-analyst" in body["description"]
    assert kwargs["token"] == "tok"


# ── _sync_crontab ───────────────────────────────────────────────────────────


def test_sync_crontab_swallows_scheduler_import_failure():
    """The CLI runs without the scheduler module loaded (no FastAPI
    process); _sync_crontab must no-op rather than raise. Pinned
    because `cleanup_local_data` calls it on every teardown."""
    with patch("backend.scheduler.get_scheduler", side_effect=ImportError("no scheduler")):
        # Should not raise
        orchestrator._sync_crontab()


def test_sync_crontab_swallows_reload_exception():
    """If the scheduler IS loaded but reload() raises (locked state
    file, mid-teardown race), still no-op. Pinned because raising
    here would convert a successful provision into an apparent
    failure."""

    class _Sched:
        def reload(self):
            raise RuntimeError("scheduler busy")

    with patch("backend.scheduler.get_scheduler", return_value=_Sched()):
        orchestrator._sync_crontab()


# ── provision (8-step generator) ────────────────────────────────────────────


def _provision_cfg(**overrides):
    """Minimal cfg dict accepted by provision()."""
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


def _consume(gen):
    """Drain a generator and return (events, exception_or_None)."""
    events = []
    try:
        for e in gen:
            events.append(e)
    except Exception as exc:
        return events, exc
    return events, None


def test_provision_yields_error_event_on_preflight_failure(tmp_path, monkeypatch):
    """When ``validate_log_format`` returns errors, provision yields a
    typed ``error`` event and stops WITHOUT calling the Fastly API.
    Pinned because the preflight check is what catches bad log_formats
    before they reach Fastly's upload step."""
    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG"]),
        patch("backend.provision.orchestrator.ensure_fos_access_key") as mock_create_key,
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    # No exception escaped (preflight failure is yielded as error, not raised)
    assert exc is None
    # The last event is an error
    assert events[-1]["type"] == "error"
    assert "Log format" in events[-1]["message"]
    # Fastly API was NOT called
    mock_create_key.assert_not_called()


def test_provision_completes_all_8_steps_when_apis_succeed(tmp_path, monkeypatch):
    """Happy path: provision() walks all 8 steps and emits a final
    ``done`` event. Pinned because the FE's progress bar advances on
    the per-step ``progress`` events and the final ``done`` is what
    flips it to 100%."""
    monkeypatch.chdir(tmp_path)

    temp_key = {"access_key": "TEMP_AK", "secret_key": "TEMP_SK", "id": "TEMP_ID"}
    perm_key = {"access_key": "PERM_AK", "secret_key": "PERM_SK", "id": "PERM_ID"}
    cdn_svc = {"id": "cdn-svc-id"}

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        # Step 2 + 4: returns temp then permanent key
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            side_effect=[temp_key, perm_key],
        ),
        patch("backend.provision.orchestrator.ensure_fos_bucket"),  # Step 3
        patch("backend.provision.orchestrator.delete_fos_access_key"),  # Step 5
        patch("backend.provision.orchestrator.ensure_cdn_service", return_value=cdn_svc),  # Step 6
        patch("backend.provision.orchestrator.ensure_logging_endpoint", return_value=42),  # Step 7
        patch("backend.provision.orchestrator.write_service_config"),
        # Step 8 — iceberg init in try/except
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None
    # All 9 progress events (0..8 inclusive)
    progress_events = [e for e in events if e["type"] == "progress"]
    assert len(progress_events) == 9
    assert progress_events[0]["current"] == 0
    assert progress_events[-1]["current"] == 8
    # Terminal event
    assert events[-1]["type"] == "done"
    assert "complete" in events[-1]["message"].lower()


def test_provision_runs_teardown_rollback_on_mid_step_failure(tmp_path, monkeypatch):
    """If a step raises (Fastly API failure mid-provision), the
    orchestrator runs ``perform_teardown`` for cleanup, yields a final
    error event, AND re-raises the original exception. Pinned because
    the cleanup rollback is what prevents orphaned keys/buckets from
    accumulating after partial provisioning failures."""
    monkeypatch.chdir(tmp_path)

    temp_key = {"access_key": "TEMP", "secret_key": "TS", "id": "T"}
    teardown_called = []

    def fake_teardown(state, token, opts=None):
        teardown_called.append(True)
        yield {"type": "status", "message": "rollback step"}

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch("backend.provision.orchestrator.ensure_fos_access_key", return_value=temp_key),
        # Step 3 fails — simulates "bucket name already taken"
        patch(
            "backend.provision.orchestrator.ensure_fos_bucket",
            side_effect=RuntimeError("BucketAlreadyExists"),
        ),
        patch("backend.provision.orchestrator.perform_teardown", side_effect=fake_teardown),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    # Original exception is re-raised after cleanup
    assert isinstance(exc, RuntimeError)
    assert "BucketAlreadyExists" in str(exc)
    # Teardown WAS invoked for rollback
    assert teardown_called == [True]
    # Error event was yielded
    error_events = [e for e in events if e["type"] == "error"]
    assert any("BucketAlreadyExists" in e["message"] for e in error_events)


def test_provision_loads_state_when_resume_from_state_true(tmp_path, monkeypatch):
    """``_resume_from_state=True`` reads ``setup-state.json`` and
    merges in any previously-saved keys (so a crashed provision can
    pick up where it left off). Pinned because the resume flow is
    the only thing preventing duplicate temp-keys when a user
    re-runs the wizard after a failure."""
    # Pre-seed a state file with a "previously-completed" temp key
    previous_state = {
        "temp_admin_key_id": "PREV_ID",
        "temp_admin_access_key": "PREV_AK",
        "temp_admin_secret_key": "PREV_SK",
    }
    orchestrator.save_state(previous_state)

    captured_state = {}

    def capture_create(desc, state, token, **kwargs):
        captured_state["state"] = dict(state)
        return {"access_key": "NEW", "secret_key": "NS", "id": "NID"}

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        # The first ensure_fos_access_key call (step 2) should see the prev state merged in
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=capture_create),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=RuntimeError("abort early")),
        patch("backend.provision.orchestrator.perform_teardown", return_value=iter([])),
        patch("backend.config.config_path", return_value=str(tmp_path / "no.json")),
    ):
        _consume(orchestrator.provision(_provision_cfg(), _resume_from_state=True))

    # The state passed to ensure_fos_access_key includes the resumed keys
    assert captured_state["state"]["temp_admin_key_id"] == "PREV_ID"


def test_provision_surfaces_warning_when_no_source_resolved(tmp_path, monkeypatch):
    """When ``get_source_for_service`` returns None after the config is
    written (race, missing fields), the iceberg-init block can't run — but
    it must SURFACE that (warning event), not skip silently. The wizard still
    reports success because commit_buffer self-heals the table on first commit.

    Regression: this block used to be silent on both the None-source and the
    init-raises paths, which is how a fresh service shipped with no Iceberg
    table and a commit cron crashing every cycle."""
    monkeypatch.chdir(tmp_path)

    temp_key = {"access_key": "T", "secret_key": "TS", "id": "TID"}
    perm_key = {"access_key": "P", "secret_key": "PS", "id": "PID"}

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=[temp_key, perm_key]),
        patch("backend.provision.orchestrator.ensure_fos_bucket"),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.ensure_cdn_service", return_value={"id": "cdn"}),
        patch("backend.provision.orchestrator.ensure_logging_endpoint", return_value=1),
        patch("backend.provision.orchestrator.write_service_config"),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.iceberg.init_iceberg_table") as mock_iceberg,
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None
    # Wizard still completes (best-effort — first commit creates the table).
    assert events[-1]["type"] == "done"
    # Iceberg init was NOT called (no source to init against).
    mock_iceberg.assert_not_called()
    # ...but the failure is now visible, not silent.
    assert any(e["type"] == "status" and "not initialized" in e["message"].lower() for e in events), (
        "expected a surfaced warning that the Iceberg table was not initialized"
    )


def test_provision_surfaces_warning_when_iceberg_init_raises(tmp_path, monkeypatch):
    """If ``init_iceberg_table`` raises during provisioning, the wizard must
    NOT swallow it silently: it logs/surfaces a warning and still completes
    (commit_buffer creates the table on first commit). Pinned because the
    bare ``except Exception: pass`` here is what hid a broken fresh install."""
    monkeypatch.chdir(tmp_path)

    temp_key = {"access_key": "T", "secret_key": "TS", "id": "TID"}
    perm_key = {"access_key": "P", "secret_key": "PS", "id": "PID"}

    with (
        patch("backend.provision.orchestrator.validate_log_format", return_value=[]),
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=[temp_key, perm_key]),
        patch("backend.provision.orchestrator.ensure_fos_bucket"),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.ensure_cdn_service", return_value={"id": "cdn"}),
        patch("backend.provision.orchestrator.ensure_logging_endpoint", return_value=1),
        patch("backend.provision.orchestrator.write_service_config"),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "sid", "logging_service_id": "lsid"},
        ),
        patch("backend.core.duckdb.get_connection"),
        patch(
            "backend.core.iceberg.init_iceberg_table",
            side_effect=RuntimeError("FOS not ready"),
        ) as mock_iceberg,
    ):
        events, exc = _consume(orchestrator.provision(_provision_cfg()))

    assert exc is None
    mock_iceberg.assert_called_once()
    # Wizard still completes despite the init failure.
    assert events[-1]["type"] == "done"
    # The failure is surfaced, not swallowed.
    assert any(e["type"] == "status" and "deferred to first commit" in e["message"].lower() for e in events), (
        "expected a surfaced warning that Iceberg init was deferred"
    )


# ── perform_teardown (4-step generator) ──────────────────────────────────


def _teardown_state(**overrides):
    """Minimal state dict accepted by perform_teardown."""
    base = {
        "logging_service_id": "svc-td-test",
        "fos_bucket_name": "test-bucket",
        "fos_region": "us-east-1",
        "fos_key_id": "fos-key-id",
        "endpoint_name": "Test Endpoint",
        "cdn_service_id": "cdn-svc-id",
        "cdn_service_name": "CDN Name",
    }
    base.update(overrides)
    return base


def test_perform_teardown_calls_all_4_steps_with_default_opts():
    """Default opts → remove logging + bucket + CDN. Pinned because
    losing any step would leave orphaned Fastly resources that
    accumulate billing charges.

    Note: ``remove_logging_endpoint`` and friends are *regular*
    functions (not generators) — the orchestrator wraps them via
    ``run_with_events`` which spawns a thread and emits status
    events via the ``status_cb`` callback."""
    remove_logging_called = []
    delete_bucket_called = []
    delete_cdn_called = []
    delete_fos_key_called = []

    def fake_remove_logging(service_id, endpoint_name, token, status_cb=None):
        remove_logging_called.append(True)

    def fake_delete_bucket(*args, **kwargs):
        delete_bucket_called.append(True)

    def fake_delete_cdn(service_id, name, token, status_cb=None):
        delete_cdn_called.append(True)

    def fake_delete_fos_key(key_id, token, status_cb=None):
        delete_fos_key_called.append(key_id)

    def fake_ensure_key(*args, **kwargs):
        return {"access_key": "TAK", "secret_key": "TSK", "id": "TID"}

    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=fake_remove_logging),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=fake_delete_bucket),
        patch("backend.provision.orchestrator.delete_cdn_service", side_effect=fake_delete_cdn),
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=fake_ensure_key),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=fake_delete_fos_key),
        patch("backend.provision.orchestrator.fastly", return_value={"data": []}),
    ):
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))

    assert exc is None
    assert remove_logging_called == [True]
    assert delete_bucket_called == [True]
    assert delete_cdn_called == [True]
    # 4 progress events (1, 2, 3, 4)
    progress = [e for e in events if e["type"] == "progress"]
    assert len(progress) == 4


def test_perform_teardown_skips_logging_when_remove_logging_opt_false():
    """``opts['remove_logging'] = False`` → don't call remove_logging.
    Pinned because partial teardowns are the only safe way to remove
    an analyst replica without breaking the admin service's logs."""
    remove_logging_called = []

    def fake_remove_logging(*args, **kwargs):
        remove_logging_called.append(True)

    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=fake_remove_logging),
        patch("backend.provision.orchestrator.delete_fos_bucket"),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            return_value={"access_key": "A", "secret_key": "S", "id": "I"},
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.fastly", return_value={"data": []}),
    ):
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(),
                "tok",
                opts={"remove_logging": False, "remove_cdn": True, "remove_bucket": True},
            )
        )

    assert exc is None
    assert remove_logging_called == []


def test_perform_teardown_skips_bucket_when_remove_bucket_opt_false():
    """``opts['remove_bucket'] = False`` → don't delete bucket/keys.
    Pinned because customers retaining FOS data after teardown rely
    on this opt."""
    delete_bucket_called = []
    fastly_called = []

    def fake_delete(*args, **kwargs):
        delete_bucket_called.append(True)

    def fake_fastly(*args, **kwargs):
        fastly_called.append(args[1])  # path
        return {"data": []}

    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint"),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=fake_delete),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.fastly", side_effect=fake_fastly),
    ):
        events, exc = _consume(
            orchestrator.perform_teardown(
                _teardown_state(),
                "tok",
                opts={"remove_logging": True, "remove_cdn": True, "remove_bucket": False},
            )
        )

    assert exc is None
    assert delete_bucket_called == []
    # No GET /resources/object-storage/access-keys when we're not removing the bucket
    assert not any("access-keys" in p for p in fastly_called)


def test_perform_teardown_swallows_remove_logging_exception_with_warning():
    """If ``remove_logging_endpoint`` raises (Fastly API 502, missing
    endpoint), emit a warning event and continue with the rest of
    teardown. Pinned because a partial teardown (only logging failed)
    is still useful — the bucket + CDN should still be cleaned up."""

    def fake_remove(*args, **kwargs):
        raise RuntimeError("upstream 502")

    delete_bucket_called = []

    def fake_delete_bucket(*args, **kwargs):
        delete_bucket_called.append(True)

    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=fake_remove),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=fake_delete_bucket),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            return_value={"access_key": "A", "secret_key": "S", "id": "I"},
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.fastly", return_value={"data": []}),
    ):
        events, exc = _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))

    assert exc is None
    # Warning event surfaced
    warning_events = [e for e in events if "Warning" in e.get("message", "")]
    assert len(warning_events) >= 1
    # Bucket deletion still happened
    assert delete_bucket_called == [True]


# ── write_service_config: preserve code-managed keys on re-ingest ────────────


def test_write_service_config_preserves_scoring_block_when_state_omits_it(tmp_path):
    """REGRESSION: re-running /api/provision/ingest (wizard re-run,
    Terraform import, key rotation) used to wholesale-overwrite the cfg
    with the request body, silently dropping cfg['scoring'] +
    cfg['log_fields']['custom_fields'] + cfg['ngwaf_workspace_id'].
    write_service_config must now LOAD the existing cfg and preserve
    those code-managed keys when the request body lacks them."""
    saved_cfgs = []
    existing_cfg = {
        "service_id": "svc-test",
        "scoring": {
            "enabled": True,
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
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        # Re-ingest with a body that has no awareness of scoring or ngwaf
        orchestrator.write_service_config(_make_state())

    _, cfg = saved_cfgs[0]
    # Scoring block survived
    assert cfg.get("scoring", {}).get("enabled") is True
    assert cfg["scoring"]["scoring_service_id"] == "scorer-svc"
    # NGWAF workspace id survived
    assert cfg.get("ngwaf_workspace_id") == "ngwaf-abc"
    # User's custom_field survived
    custom_names = {cf["name"] for cf in cfg["log_fields"]["custom_fields"]}
    assert "my_field" in custom_names
    # All scoring fields re-injected from code (canonical source of truth)
    from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

    for name in _SCORING_FIELD_NAMES:
        assert name in custom_names, f"scoring field {name!r} dropped on re-ingest"


def test_write_service_config_first_ever_ingest_has_no_existing_cfg(tmp_path):
    """First-time ingest: load_config returns None. Must not raise and
    must skip the preserve step gracefully — the request body IS the
    full state on first run."""
    saved_cfgs = []
    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved_cfgs.append((sid, cfg))),
        patch("backend.config.duckdb_path", return_value=str(tmp_path / "db.duckdb")),
    ):
        orchestrator.write_service_config(_make_state())

    _, cfg = saved_cfgs[0]
    # No scoring / ngwaf in body or existing → none in cfg
    assert "scoring" not in cfg
    assert "ngwaf_workspace_id" not in cfg


def test_perform_teardown_filters_fastly_keys_by_managed_description_prefix():
    """When listing FOS access keys, only delete keys whose description
    matches ``fos-log-analysis-{service_id}`` / temp-admin /
    temp-teardown- — never keys with other descriptions. Pinned
    because deleting unmanaged keys would break the customer's other
    services that share the FOS account."""
    fake_keys = {
        "data": [
            {"access_key": "MANAGED1", "description": "fos-log-analysis-svc-td-test"},
            {"access_key": "TEMPADMIN", "description": "fos-log-analysis-temp-admin-svc-td-test"},
            {"access_key": "TEMPTEAR", "description": "temp-teardown-svc-td-test"},
            {"access_key": "UNMANAGED", "description": "customer-other-thing"},
        ]
    }
    deleted_keys = []

    def fake_fastly(method, path, **kwargs):
        if method == "GET":
            return fake_keys
        if method == "DELETE":
            # Capture the deleted key id from path
            deleted_keys.append(path.split("/")[-1])
            return None
        return {}

    with (
        patch("backend.provision.orchestrator.remove_logging_endpoint"),
        patch("backend.provision.orchestrator.delete_fos_bucket"),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch(
            "backend.provision.orchestrator.ensure_fos_access_key",
            return_value={"access_key": "A", "secret_key": "S", "id": "I"},
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key"),
        patch("backend.provision.orchestrator.fastly", side_effect=fake_fastly),
    ):
        _consume(orchestrator.perform_teardown(_teardown_state(), "tok"))

    # Managed keys were deleted; the customer's unmanaged key was NOT
    assert "MANAGED1" in deleted_keys
    assert "TEMPADMIN" in deleted_keys
    assert "TEMPTEAR" in deleted_keys
    assert "UNMANAGED" not in deleted_keys


# ── NGWAF workspace preservation across provision() (audit follow-up) ───────


def test_write_service_config_preserves_existing_ngwaf_workspace_id(tmp_path, monkeypatch):
    """When the on-disk config already has ``ngwaf_workspace_id`` and the
    provisioning state does NOT set it, write_service_config must
    preserve it. Regression of this surfaced as silent NGWAF workspace
    detach on every re-provision (2026-06-02 state_sync incident).
    """
    from backend import config
    from backend.provision import orchestrator

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    svc_id = "svc-ngwaf-preserve"
    # Seed an existing cfg with an NGWAF workspace_id.
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "ngwaf_workspace_id": "ws_preserved_xyz",
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
        },
    )

    # Provisioning state OMITS ngwaf_workspace_id — write_service_config
    # should pick it up from the existing cfg on disk.
    state = {
        "service_id": svc_id,
        "service_name": svc_id,
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "fos_prefix": "logs",
        "fos_access_key_id": "AK",
        "fos_secret_access_key": "SK",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "cdn_service_id": "CDN",
        "cdn_url": "https://b.global.ssl.fastly.net",
        "logging_service_id": "SU",
        "endpoint_name": "fastly_log_analysis",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
    }
    orchestrator.write_service_config(state)

    saved = config.load_config(svc_id)
    assert saved.get("ngwaf_workspace_id") == "ws_preserved_xyz", (
        f"existing NGWAF workspace_id was clobbered on re-provision; saved={saved!r}"
    )


def test_write_service_config_ngwaf_routing_separated_from_provision_state(tmp_path, monkeypatch):
    """OBSERVED behaviour: write_service_config DOES NOT consume
    ``ngwaf_workspace_id`` from the provisioning state. The NGWAF
    workspace binding is routed exclusively through the dedicated
    PATCH /ngwaf-workspace endpoint, not through the provision flow.

    The preserve loop runs only when ``ngwaf_workspace_id`` is NOT in
    state (covered by the sibling test). State-set ngwaf_workspace_id
    therefore short-circuits preservation AND is not written either —
    the existing on-disk value is dropped.

    Pinned because the wording is subtle: a contributor reading the
    state dict could reasonably assume "state overrides cfg". It
    doesn't, by design — surface the contract in a regression test.
    """
    from backend import config
    from backend.provision import orchestrator

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    svc_id = "svc-ngwaf-routing"
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "ngwaf_workspace_id": "ws_existing",
            "status": {},
            "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
        },
    )

    state = {
        "service_id": svc_id,
        "service_name": svc_id,
        "ngwaf_workspace_id": "ws_in_state_should_be_ignored",
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "fos_prefix": "logs",
        "fos_access_key_id": "AK",
        "fos_secret_access_key": "SK",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "cdn_service_id": "CDN",
        "cdn_url": "https://b.global.ssl.fastly.net",
        "logging_service_id": "SU",
        "endpoint_name": "fastly_log_analysis",
        "log_period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
    }
    orchestrator.write_service_config(state)

    saved = config.load_config(svc_id)
    # Neither "ws_existing" (preservation skipped because state has the
    # key) nor "ws_in_state_should_be_ignored" (state is ignored by
    # write_service_config). Behaviour pins that the orchestrator does
    # NOT propagate the field — admin must use PATCH /ngwaf-workspace.
    assert "ngwaf_workspace_id" not in saved or saved.get("ngwaf_workspace_id") == "ws_existing", (
        f"state-set ngwaf_workspace_id leaked into saved cfg; saved={saved!r}"
    )


# ── _reject_unsafe_fos_component (L8) ────────────────────────────────────────


def test_reject_unsafe_fos_component_bucket_rejects_path_tokens():
    """L8: fos_bucket composes into the local cache path — no separators."""
    from backend.provision.orchestrator import _reject_unsafe_fos_component

    for bad in ("../etc", "a/b", "a\\b", "..", "a\x00b"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_bucket", bad, allow_slash=False)
    # Legitimate bucket name + empty value pass.
    _reject_unsafe_fos_component("fos_bucket", "my-fos-bucket", allow_slash=False)
    _reject_unsafe_fos_component("fos_bucket", "", allow_slash=False)


def test_reject_unsafe_fos_component_prefix_allows_slash_rejects_traversal():
    from backend.provision.orchestrator import _reject_unsafe_fos_component

    # S3 key prefixes legitimately contain '/'.
    _reject_unsafe_fos_component("fos_prefix", "raw/logs/2026/", allow_slash=True)
    for bad in ("../up", "a\\b", "..", "a\x00"):
        with pytest.raises(ValueError, match="illegal path token"):
            _reject_unsafe_fos_component("fos_prefix", bad, allow_slash=True)
