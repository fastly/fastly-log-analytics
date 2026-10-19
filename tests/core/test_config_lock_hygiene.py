"""config_lock hygiene: loud degradation, bounded wait, re-entrancy, salt safety, audited rotation."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
import time

import pytest

from backend import config as svcconfig

pytestmark = pytest.mark.security_regression

SID = "svc-lock"


@pytest.fixture(autouse=True)
def _reset_lock_state(monkeypatch):
    monkeypatch.setattr(svcconfig, "_lock_degraded", False)
    svcconfig._ensure_dirs()
    svcconfig.save_config(SID, {"service_id": SID})
    yield


def _salt() -> str:
    return json.loads(svcconfig.config_path(SID).read_text())["rum"]["cid_salt"]


def test_flock_oserror_is_loud_once_and_flags_health(monkeypatch, caplog):
    def boom(fd, op):
        raise OSError(30, "Read-only file system")

    monkeypatch.setattr(svcconfig.fcntl, "flock", boom)
    with caplog.at_level(logging.ERROR, logger="backend.config"):
        with svcconfig.config_lock(SID):
            pass
        with svcconfig.config_lock(SID):
            pass
    errors = [r for r in caplog.records if "config_lock_degraded" in r.getMessage() and r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert svcconfig.config_lock_degraded() is True


def test_health_snapshot_surfaces_config_lock_degraded(monkeypatch):
    from backend.routers.admin.health import health_snapshot

    assert health_snapshot()["config_lock_degraded"] is False
    monkeypatch.setattr(svcconfig, "_lock_degraded", True)
    assert health_snapshot()["config_lock_degraded"] is True


def test_lock_wait_is_bounded_and_raises_a_clear_timeout(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        start = time.monotonic()
        with pytest.raises(svcconfig.ConfigLockTimeout, match=SID):
            with svcconfig.config_lock(SID):
                pass
        assert time.monotonic() - start < 3
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)


def test_lock_is_acquired_after_the_holder_releases(monkeypatch):
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    threading.Timer(0.2, lambda: (fcntl.flock(holder, fcntl.LOCK_UN), os.close(holder))).start()
    with svcconfig.config_lock(SID):
        pass


def test_mutate_callback_saving_the_same_service_does_not_self_deadlock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 1.0)

    def _mutate(cfg):
        cfg["a"] = 1
        inner = svcconfig.load_config(SID)
        inner["b"] = 2
        svcconfig.save_config(SID, inner)
        svcconfig.mutate_config(SID, lambda c: c.__setitem__("c", 3))

    seen: dict = {}

    def _outer(cfg):
        _mutate(cfg)
        seen.update(svcconfig.load_config(SID))

    svcconfig.mutate_config(SID, _outer)
    # nested writes were applied (visible inside the callback); the outer save then lands last
    assert seen["b"] == 2 and seen["c"] == 3
    assert svcconfig.load_config(SID)["a"] == 1


def test_reentrancy_is_per_thread(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    result: list[str] = []

    def other():
        try:
            with svcconfig.config_lock(SID):
                result.append("acquired")
        except svcconfig.ConfigLockTimeout:
            result.append("timeout")

    with svcconfig.config_lock(SID):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert result == ["timeout"]


def test_update_status_goes_through_mutate_config(monkeypatch):
    calls = []
    real = svcconfig.mutate_config

    def spy(sid, fn, **kw):
        calls.append(sid)
        return real(sid, fn, **kw)

    monkeypatch.setattr(svcconfig, "mutate_config", spy)
    svcconfig.update_status(SID, {"x": 1})
    assert calls == [SID]
    assert svcconfig.get_status(SID)["x"] == 1


def test_update_status_does_not_clobber_a_concurrent_writer():
    def _mutate(cfg):
        cfg["marker"] = "kept"

    def racing_update():
        svcconfig.update_status(SID, {"y": 2})

    # A concurrent update_status in another thread blocks on the lock and applies on a fresh read.
    with svcconfig.config_lock(SID):
        t = threading.Thread(target=racing_update)
        t.start()
        time.sleep(0.05)
        cfg = svcconfig._read_config_fresh(SID)
        _mutate(cfg)
        svcconfig._save_config_unlocked(SID, cfg)
    t.join()
    final = svcconfig.load_config(SID)
    assert final["marker"] == "kept" and final["status"]["y"] == 2


def test_delete_config_takes_the_lock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        with pytest.raises(svcconfig.ConfigLockTimeout):
            svcconfig.delete_config(SID)
        assert svcconfig.config_path(SID).exists()
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)
    svcconfig.delete_config(SID)
    assert not svcconfig.config_path(SID).exists()


def test_transient_read_failure_aborts_the_save_instead_of_minting_a_new_salt(monkeypatch):
    before = _salt()

    def failing_read(sid):
        raise OSError(5, "Input/output error")

    with monkeypatch.context() as m:
        m.setattr(svcconfig, "_read_config_fresh", failing_read)
        with pytest.raises(svcconfig.CidSaltReadError):
            svcconfig.save_config(SID, {"service_id": SID, "rum": {}})
    assert _salt() == before


def test_unreadable_missing_file_still_mints_a_salt_for_a_new_config(monkeypatch):
    svcconfig.save_config("svc-new", {"service_id": "svc-new"})
    assert json.loads(svcconfig.config_path("svc-new").read_text())["rum"]["cid_salt"]


def test_save_config_cannot_rotate_the_salt():
    before = _salt()
    svcconfig.save_config(SID, {"service_id": SID, "rum": {"cid_salt": "attacker-chosen-salt-value"}})
    assert _salt() == before


def test_rotate_cid_salt_changes_the_salt_and_is_audited(monkeypatch):
    from backend.core import duckdb as ddb
    from backend.core import metadata as metadata_db

    audits = []
    monkeypatch.setattr(metadata_db, "record_audit", lambda *a, **k: audits.append((a, k)))
    before = _salt()
    new = ddb.rotate_cid_salt(SID, actor="tester")
    assert new != before and _salt() == new and len(new) == 64
    assert ddb.get_or_generate_cid_salt(SID) == new
    ((args, kwargs),) = audits
    assert args[1] == "cid_salt_rotated" and kwargs["actor"] == "tester"
    assert before not in json.dumps(args) and new not in json.dumps(args)
    # and a later ordinary save keeps the rotated salt
    svcconfig.update_status(SID, {"z": 1})
    assert _salt() == new


def test_rotate_cid_salt_unknown_service_raises():
    from backend.core import duckdb as ddb

    with pytest.raises(LookupError):
        ddb.rotate_cid_salt("does-not-exist")


def test_rotate_cid_salt_is_not_a_route():
    from backend.main import app

    assert not [r for r in app.routes if "rotate" in getattr(r, "path", "") and "salt" in getattr(r, "path", "")]
