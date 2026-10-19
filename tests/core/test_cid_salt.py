"""cid pseudonymization salt: no public fallback, no cross-process first-use race, one generator."""

from __future__ import annotations

import ast
import inspect
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time

import pytest

pytestmark = pytest.mark.security_regression

HEX64 = re.compile(r"[0-9a-f]{64}")


@pytest.fixture
def fake_store(monkeypatch):
    """Real config files in the conftest-sandboxed CONFIGS_DIR; ``saves`` records each disk write."""
    from backend import config as svcconfig

    saves: list[str] = []
    real_write = svcconfig._atomic_write_json

    def spy(path, data):
        time.sleep(0.005)  # widen the read-modify-write window so an unguarded race is reproducible
        saves.append(path.stem)
        real_write(path, data)

    monkeypatch.setattr(svcconfig, "_atomic_write_json", spy)

    class Store:
        def put(self, sid, cfg):
            real_write(svcconfig.config_path(sid), cfg)
            svcconfig._config_cache.pop(sid, None)

        def get(self, sid):
            return json.loads(svcconfig.config_path(sid).read_text())

        def exists(self, sid):
            return svcconfig.config_path(sid).exists()

    svcconfig._ensure_dirs()
    return Store(), saves


def test_missing_config_never_uses_the_public_hardcoded_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    salt = get_or_generate_cid_salt("svc-none")
    assert "default_test_salt" not in salt
    assert HEX64.fullmatch(salt)
    assert saves == []
    assert not store.exists("svc-none")


def test_missing_config_logs_loudly(fake_store, caplog):
    from backend.core.duckdb import get_or_generate_cid_salt

    with caplog.at_level(logging.WARNING):
        get_or_generate_cid_salt("svc-none")
    assert "cid_salt_no_config" in caplog.text


def test_missing_config_salt_is_stable_within_the_process(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    assert get_or_generate_cid_salt("svc-a") == get_or_generate_cid_salt("svc-b")


def test_existing_salt_is_returned_unchanged_and_not_resaved(fake_store, caplog):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    store.put("svc", {"rum": {"cid_salt": "abc123"}})
    with caplog.at_level(logging.WARNING):
        assert get_or_generate_cid_salt("svc") == "abc123"
    assert saves == []
    assert "cid_salt_missing" not in caplog.text


def test_first_use_generates_once_persists_and_logs(fake_store, caplog):
    from backend import config as svcconfig
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    # Legacy config written before salts existed (bypasses save_config's first-save generation).
    store.put("svc", {"service_id": "svc", "rum": {}})
    with caplog.at_level(logging.WARNING):
        salt = get_or_generate_cid_salt("svc")
    assert HEX64.fullmatch(salt)
    assert store.get("svc")["rum"]["cid_salt"] == salt
    assert saves == ["svc"]
    assert "cid_salt_missing" in caplog.text
    assert svcconfig.load_config("svc")["rum"]["cid_salt"] == salt


def test_lazy_generation_does_not_clobber_other_config_fields(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "name": "keep", "rum": {"enabled": True}})
    get_or_generate_cid_salt("svc")
    cfg = store.get("svc")
    assert cfg["name"] == "keep"
    assert cfg["rum"]["enabled"] is True


def test_concurrent_first_use_agrees_on_a_single_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "rum": {}})
    barrier = threading.Barrier(8)
    results: list[str] = []

    def worker():
        barrier.wait()
        results.append(get_or_generate_cid_salt("svc"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(results)) == 1
    assert store.get("svc")["rum"]["cid_salt"] == results[0]


_RACER = """
import sys, time
from pathlib import Path
import backend.config as c
c.CONFIGS_DIR = Path(sys.argv[1])
import backend.core.duckdb as d
orig = c._atomic_write_json
def slow(path, data):
    time.sleep(0.05)
    orig(path, data)
c._atomic_write_json = slow
go = Path(sys.argv[2])
while not go.exists():
    time.sleep(0.001)
print(d.get_or_generate_cid_salt("svc"))
"""


def test_separate_processes_racing_first_use_agree_on_one_salt(tmp_path):
    cfg_dir = tmp_path / "cfgs"
    cfg_dir.mkdir()
    (cfg_dir / "svc.json").write_text(json.dumps({"service_id": "svc", "rum": {}}))
    go = tmp_path / "go"
    procs = [
        subprocess.Popen(
            [sys.executable, "-I", "-c", _RACER, str(cfg_dir), str(go)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONPATH": os.getcwd()},
        )
        for _ in range(4)
    ]
    time.sleep(4)  # let every child finish importing before releasing them together
    go.write_text("")
    outs = [p.communicate(timeout=60) for p in procs]
    for p, (_, err) in zip(procs, outs, strict=True):
        assert p.returncode == 0, err
    salts = {out.strip().splitlines()[-1] for out, _ in outs}
    assert len(salts) == 1
    assert json.loads((cfg_dir / "svc.json").read_text())["rum"]["cid_salt"] in salts


def test_new_config_gets_salt_on_first_save(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("fresh", {"service_id": "fresh"})
    assert HEX64.fullmatch(store.get("fresh")["rum"]["cid_salt"])


def test_save_config_never_rekeys_an_existing_salt(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"enabled": True}})
    original = store.get("svc")["rum"]["cid_salt"]

    # Stale writers: replace rum wholesale, mint their own salt, or drop rum entirely.
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"enabled": False}})
    assert store.get("svc")["rum"]["cid_salt"] == original
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"cid_salt": "f" * 64}})
    assert store.get("svc")["rum"]["cid_salt"] == original
    svcconfig.save_config("svc", {"service_id": "svc"})
    assert store.get("svc")["rum"]["cid_salt"] == original


def test_mutate_config_serializes_concurrent_partial_edits(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "counters": {}})
    n = 12
    barrier = threading.Barrier(n)

    def worker(i):
        barrier.wait()
        svcconfig.mutate_config("svc", lambda c, i=i: c["counters"].__setitem__(str(i), i))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(store.get("svc")["counters"]) == sorted(str(i) for i in range(n))


def test_mutate_config_missing_service_returns_none(fake_store):
    from backend import config as svcconfig

    assert svcconfig.mutate_config("nope", lambda c: None) is None


def test_ensure_cid_salt_generates_when_absent_and_keeps_existing():
    from backend.core.duckdb import ensure_cid_salt

    cfg: dict = {}
    first = ensure_cid_salt(cfg)
    assert HEX64.fullmatch(first)
    assert cfg["cid_salt"] == first
    assert ensure_cid_salt(cfg) == first
    kept = {"cid_salt": "keep-me"}
    assert ensure_cid_salt(kept) == "keep-me"


def test_services_router_does_not_mint_its_own_salt():
    from backend.routers.services import core as services_core

    tree = ast.parse(inspect.getsource(services_core))
    minted = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "token_hex"
    ]
    assert minted == []
    assert "ensure_cid_salt" in inspect.getsource(services_core)
