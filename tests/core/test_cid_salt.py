"""cid pseudonymization salt: no public fallback, no first-use race, one generator."""

from __future__ import annotations

import ast
import inspect
import re
import threading
import time

import pytest

pytestmark = pytest.mark.security_regression

HEX64 = re.compile(r"[0-9a-f]{64}")


@pytest.fixture
def fake_store(monkeypatch):
    """In-memory config store patched over backend.config load/save."""
    store: dict[str, dict] = {}
    saves: list[str] = []

    def load(sid):
        time.sleep(0.005)  # widen the read-modify-write window so an unlocked race is reproducible
        cfg = store.get(sid)
        return (
            None
            if cfg is None
            else {**cfg, "rum": dict(cfg["rum"]) if isinstance(cfg.get("rum"), dict) else cfg.get("rum")}
        )

    def save(sid, cfg):
        saves.append(sid)
        store[sid] = cfg

    monkeypatch.setattr("backend.config.load_config", load)
    monkeypatch.setattr("backend.config.save_config", save)
    return store, saves


def test_missing_config_never_uses_the_public_hardcoded_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    salt = get_or_generate_cid_salt("svc-none")
    assert "default_test_salt" not in salt
    assert HEX64.fullmatch(salt)
    assert saves == []
    assert store == {}


def test_missing_config_salt_is_stable_within_the_process(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    assert get_or_generate_cid_salt("svc-a") == get_or_generate_cid_salt("svc-b")


def test_existing_salt_is_returned_unchanged_and_not_resaved(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    store["svc"] = {"rum": {"cid_salt": "abc123"}}
    assert get_or_generate_cid_salt("svc") == "abc123"
    assert saves == []


def test_first_use_generates_once_and_persists(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    store["svc"] = {"rum": {}}
    salt = get_or_generate_cid_salt("svc")
    assert HEX64.fullmatch(salt)
    assert store["svc"]["rum"]["cid_salt"] == salt
    assert saves == ["svc"]


def test_concurrent_first_use_agrees_on_a_single_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    store["svc"] = {"rum": {}}
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
    assert store["svc"]["rum"]["cid_salt"] == results[0]
    assert saves == ["svc"]


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
