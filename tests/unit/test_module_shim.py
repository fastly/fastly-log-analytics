"""Unit tests for ``backend.utils.module_shim.install_shim_module``."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from backend.utils.module_shim import _ShimModule, install_shim_module


def _make_temp_modules(monkeypatch: pytest.MonkeyPatch, *names: str) -> list[ModuleType]:
    mods: list[ModuleType] = []
    for name in names:
        mod = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, mod)
        mods.append(mod)
    return mods


def test_monkeypatch_setattr_propagates_across_parent_and_submodules(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, sub_a, sub_b = _make_temp_modules(
        monkeypatch,
        "tests._shim_pkg",
        "tests._shim_pkg.sub_a",
        "tests._shim_pkg.sub_b",
    )
    sub_a.shared_fn = lambda: "orig"  # type: ignore[attr-defined]
    sub_b.shared_fn = sub_a.shared_fn  # type: ignore[attr-defined]
    parent.shared_fn = sub_a.shared_fn  # type: ignore[attr-defined]

    shim_cls = install_shim_module("tests._shim_pkg", [sub_a, sub_b])
    assert shim_cls is _ShimModule
    assert isinstance(parent, _ShimModule)
    assert isinstance(sub_a, _ShimModule)
    assert isinstance(sub_b, _ShimModule)

    monkeypatch.setattr(parent, "shared_fn", lambda: "patched_from_parent")
    assert sub_a.shared_fn() == "patched_from_parent"
    assert sub_b.shared_fn() == "patched_from_parent"

    monkeypatch.setattr(sub_a, "shared_fn", lambda: "patched_from_sub_a")
    assert parent.shared_fn() == "patched_from_sub_a"
    assert sub_b.shared_fn() == "patched_from_sub_a"


def test_only_existing_attrs_and_sync_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, sub_a, sub_b = _make_temp_modules(
        monkeypatch,
        "tests._shim_existing",
        "tests._shim_existing.sub_a",
        "tests._shim_existing.sub_b",
    )
    sub_a.only_a = "a_init"  # type: ignore[attr-defined]
    sub_a._SCHEMA = "schema_a"  # type: ignore[attr-defined]
    sub_b._SCHEMA = "schema_b"  # type: ignore[attr-defined]
    parent.only_a = sub_a.only_a  # type: ignore[attr-defined]
    parent._SCHEMA = sub_a._SCHEMA  # type: ignore[attr-defined]

    install_shim_module(
        "tests._shim_existing",
        [sub_a, sub_b],
        only_existing_attrs=True,
        sync_filter=lambda src, dst, name, _val: not (name == "_SCHEMA" and dst is sub_b and src is not sub_b),
    )

    monkeypatch.setattr(parent, "only_a", "a_updated")
    assert sub_a.only_a == "a_updated"
    assert "only_a" not in sub_b.__dict__

    monkeypatch.setattr(sub_a, "_SCHEMA", "schema_a_v2")
    assert parent._SCHEMA == "schema_a_v2"
    assert sub_b._SCHEMA == "schema_b"


def test_delattr_propagates_across_synced_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, sub_a, sub_b = _make_temp_modules(
        monkeypatch,
        "tests._shim_del",
        "tests._shim_del.sub_a",
        "tests._shim_del.sub_b",
    )
    parent.ephemeral = 42  # type: ignore[attr-defined]
    sub_a.ephemeral = 42  # type: ignore[attr-defined]
    sub_b.ephemeral = 42  # type: ignore[attr-defined]

    install_shim_module("tests._shim_del", [sub_a, sub_b])

    delattr(parent, "ephemeral")
    assert "ephemeral" not in parent.__dict__
    assert "ephemeral" not in sub_a.__dict__
    assert "ephemeral" not in sub_b.__dict__
    with pytest.raises(AttributeError):
        _ = parent.ephemeral


def test_fallback_getattr_and_custom_module_getattr(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, sub_a = _make_temp_modules(
        monkeypatch,
        "tests._shim_getattr",
        "tests._shim_getattr.sub_a",
    )
    sub_a.from_submodule = "found_in_sub_a"  # type: ignore[attr-defined]

    def _custom_getattr(name: str) -> Any:
        if name == "lazy_computed":
            return "computed_value"
        raise AttributeError(name)

    parent.__getattr__ = _custom_getattr  # type: ignore[method-assign]
    install_shim_module("tests._shim_getattr", [sub_a])

    assert parent.from_submodule == "found_in_sub_a"
    assert parent.lazy_computed == "computed_value"
    with pytest.raises(AttributeError):
        _ = parent.non_existent_symbol


def test_live_and_lazy_delegated_attrs(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, sub_a, external = _make_temp_modules(
        monkeypatch,
        "tests._shim_delegated",
        "tests._shim_delegated.sub_a",
        "tests._shim_external",
    )
    sub_a._counter = 1  # type: ignore[attr-defined]
    parent._counter = 1  # type: ignore[attr-defined]
    external.remote_val = "orig_remote"  # type: ignore[attr-defined]

    install_shim_module(
        "tests._shim_delegated",
        [sub_a],
        delegated_attrs={
            "_counter": sub_a,
            "remote_val": "tests._shim_external",
        },
    )

    # Direct mutation inside sub_a.__dict__ is reflected live when reading parent._counter
    sub_a.__dict__["_counter"] = 99
    assert parent._counter == 99

    # Lazy external module delegation supports getattr, setattr, and restore cleanup
    assert parent.remote_val == "orig_remote"
    parent.remote_val = "patched_remote"  # type: ignore[attr-defined]
    assert external.remote_val == "patched_remote"
    parent.remote_val = "orig_remote"  # type: ignore[attr-defined]
    assert external.remote_val == "orig_remote"
    assert "remote_val" not in parent.__dict__


def test_transitive_nested_shim_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    pkg, mid, leaf = _make_temp_modules(
        monkeypatch,
        "tests._shim_nested",
        "tests._shim_nested.mid",
        "tests._shim_nested.leaf",
    )
    leaf.target = "v1"  # type: ignore[attr-defined]
    mid.target = "v1"  # type: ignore[attr-defined]
    pkg.target = "v1"  # type: ignore[attr-defined]

    install_shim_module("tests._shim_nested.mid", [leaf])
    install_shim_module("tests._shim_nested", [mid, leaf])

    monkeypatch.setattr(leaf, "target", "v2_from_leaf")
    assert mid.target == "v2_from_leaf"
    assert pkg.target == "v2_from_leaf"

    monkeypatch.setattr(mid, "target", "v3_from_mid")
    assert leaf.target == "v3_from_mid"
    assert pkg.target == "v3_from_mid"
