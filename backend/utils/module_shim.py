"""Canonical module-proxy helper for package/module split shims.

Provides :func:`install_shim_module` to replace a module's ``__class__`` (and
optionally its target submodules' ``__class__``) with a :class:`types.ModuleType`
subclass that propagates ``__setattr__``, ``__delattr__``, and fallback
``__getattr__`` across decomposed submodules. This preserves 100% backward
compatibility for existing import paths and ``monkeypatch.setattr`` call sites.
"""

from __future__ import annotations

import importlib
import sys
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any

_DEFAULT_SKIP_ATTRS: frozenset[str] = frozenset(
    {
        "__name__",
        "__doc__",
        "__package__",
        "__loader__",
        "__spec__",
        "__path__",
        "__file__",
        "__cached__",
        "__builtins__",
        "__all__",
        "__class__",
        "__dict__",
        "__annotations__",
        "__shim_rules__",
        "_ShimModule",
        "_SyncedStorageModule",
        "_IngestModule",
        "_SUBMODULES",
        "_SYNC_MODULES",
        "_ALL_MODULES",
        "_ALL_SYNC_MODULES",
        "_SYNCED_MODULES",
        "_SHIM_INITIALIZED",
        "_SKIP_SHIM_ATTRS",
        "_SKIP_ATTRS",
        "_MODULE_LOCAL_ATTRS",
        "_orig_delegated",
    }
)

_SYNC_GUARD = threading.local()

SyncFilter = Callable[[ModuleType, ModuleType, str, Any], bool]


@dataclass
class _ShimRule:
    root_mod: ModuleType
    all_modules: list[ModuleType]
    only_existing_attrs: bool = False
    always_sync_targets: set[int] = field(default_factory=set)
    extra_module_names: tuple[str, ...] = ()
    skip_attrs: frozenset[str] = _DEFAULT_SKIP_ATTRS
    live_delegated: dict[str, ModuleType] = field(default_factory=dict)
    lazy_delegated: dict[str, str] = field(default_factory=dict)
    orig_delegated: dict[str, Any] = field(default_factory=dict)
    lazy_getattr: Callable[[str], Any] | None = None
    fallback_getattr: bool = True
    sync_filter: SyncFilter | None = None


def _get_rules(mod: ModuleType) -> list[_ShimRule]:
    rules: list[_ShimRule] | None = mod.__dict__.get("__shim_rules__")
    if rules is None:
        rules = []
        mod.__dict__["__shim_rules__"] = rules
    return rules


def _iter_rule_peers(rule: _ShimRule) -> list[ModuleType]:
    peers = list(rule.all_modules)
    for mod_name in rule.extra_module_names:
        resolved = sys.modules.get(mod_name)
        if resolved is not None and all(resolved is not m for m in peers):
            peers.append(resolved)
    return peers


def _propagate_setattr(source_mod: ModuleType, name: str, value: Any, visited: set[int]) -> None:
    for rule in _get_rules(source_mod):
        if name in rule.skip_attrs:
            continue
        for peer in _iter_rule_peers(rule):
            if peer is source_mod or id(peer) in visited:
                continue
            if name in _DEFAULT_SKIP_ATTRS or name in rule.skip_attrs:
                continue
            if rule.sync_filter is not None and not rule.sync_filter(source_mod, peer, name, value):
                continue
            should_set = (
                not rule.only_existing_attrs
                or name in peer.__dict__
                or peer is rule.root_mod
                or id(peer) in rule.always_sync_targets
            )
            if should_set:
                visited.add(id(peer))
                ModuleType.__setattr__(peer, name, value)
                _propagate_setattr(peer, name, value, visited)


def _propagate_delattr(source_mod: ModuleType, name: str, visited: set[int]) -> None:
    for rule in _get_rules(source_mod):
        if name in rule.skip_attrs:
            continue
        for peer in _iter_rule_peers(rule):
            if peer is source_mod or id(peer) in visited:
                continue
            if name in _DEFAULT_SKIP_ATTRS or name in rule.skip_attrs:
                continue
            visited.add(id(peer))
            if name in peer.__dict__:
                try:
                    ModuleType.__delattr__(peer, name)
                except AttributeError:
                    pass
            _propagate_delattr(peer, name, visited)


class _ShimModule(ModuleType):
    """Unified module proxy propagating attribute mutations and fallback lookups across submodules."""

    def __getattribute__(self, name: str) -> Any:
        if not (name.startswith("__") and name.endswith("__")):
            mod_dict = super().__getattribute__("__dict__")
            rules: list[_ShimRule] | None = mod_dict.get("__shim_rules__")
            if rules:
                for rule in rules:
                    if self is rule.root_mod and name in rule.live_delegated:
                        target_mod = rule.live_delegated[name]
                        val = getattr(target_mod, name)
                        mod_dict[name] = val
                        return val
        return super().__getattribute__(name)

    def __getattr__(self, name: str) -> Any:
        mod_dict = super().__getattribute__("__dict__")
        rules: list[_ShimRule] = mod_dict.get("__shim_rules__") or []

        for rule in rules:
            if (mod_name := rule.lazy_delegated.get(name)) is not None:
                return getattr(importlib.import_module(mod_name), name)

        for rule in rules:
            if rule.lazy_getattr is not None:
                try:
                    return rule.lazy_getattr(name)
                except AttributeError:
                    pass

        custom_getattr = mod_dict.get("__getattr__")
        if callable(custom_getattr):
            try:
                return custom_getattr(name)
            except AttributeError:
                pass

        for rule in rules:
            if rule.fallback_getattr:
                for peer in _iter_rule_peers(rule):
                    if peer is not self and name in peer.__dict__:
                        return getattr(peer, name)

        raise AttributeError(f"module {self.__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        mod_dict = super().__getattribute__("__dict__")
        rules: list[_ShimRule] = mod_dict.get("__shim_rules__") or []

        for rule in rules:
            if (mod_name := rule.lazy_delegated.get(name)) is not None:
                target_mod = importlib.import_module(mod_name)
                if name not in rule.orig_delegated:
                    rule.orig_delegated[name] = getattr(target_mod, name)
                setattr(target_mod, name, value)
                if value is rule.orig_delegated.get(name):
                    rule.orig_delegated.pop(name, None)
                    for m in _iter_rule_peers(rule):
                        m.__dict__.pop(name, None)
                    return

        super().__setattr__(name, value)
        if (name.startswith("__") and name.endswith("__")) or name in _DEFAULT_SKIP_ATTRS:
            return

        visited: set[int] | None = getattr(_SYNC_GUARD, "visited", None)
        if visited is not None:
            return
        visited = {id(self)}
        _SYNC_GUARD.visited = visited
        try:
            _propagate_setattr(self, name, value, visited)
        finally:
            _SYNC_GUARD.visited = None

    def __delattr__(self, name: str) -> None:
        mod_dict = super().__getattribute__("__dict__")
        rules: list[_ShimRule] = mod_dict.get("__shim_rules__") or []

        for rule in rules:
            if (mod_name := rule.lazy_delegated.get(name)) is not None and name in rule.orig_delegated:
                target_mod = importlib.import_module(mod_name)
                setattr(target_mod, name, rule.orig_delegated.pop(name))
                for m in _iter_rule_peers(rule):
                    m.__dict__.pop(name, None)
                return

        try:
            super().__delattr__(name)
        except AttributeError:
            # Allow deleting an attribute that only exists on a synced peer or was already removed.
            found_on_peer = any(
                name in peer.__dict__ for rule in rules for peer in _iter_rule_peers(rule) if peer is not self
            )
            if not found_on_peer:
                raise

        if (name.startswith("__") and name.endswith("__")) or name in _DEFAULT_SKIP_ATTRS:
            return

        visited: set[int] | None = getattr(_SYNC_GUARD, "visited", None)
        if visited is not None:
            return
        visited = {id(self)}
        _SYNC_GUARD.visited = visited
        try:
            _propagate_delattr(self, name, visited)
        finally:
            _SYNC_GUARD.visited = None


def install_shim_module(
    module_name: str,
    sync_modules: Sequence[ModuleType],
    *,
    install_on_submodules: bool = True,
    only_existing_attrs: bool = False,
    always_sync_targets: Sequence[ModuleType | str] = (),
    extra_module_names: Sequence[str] = (),
    skip_attrs: Iterable[str] = (),
    delegated_attrs: Mapping[str, ModuleType | str] | None = None,
    lazy_getattr: Callable[[str], Any] | None = None,
    fallback_getattr: bool = True,
    sync_filter: SyncFilter | None = None,
    seed_submodules_from_root: bool = False,
    bidirectional_seed: bool = False,
) -> type[ModuleType]:
    """Install :class:`_ShimModule` on ``module_name`` and optionally ``sync_modules``.

    Parameters
    ----------
    module_name:
        Name of the parent/shim module in ``sys.modules`` (usually ``__name__``).
    sync_modules:
        Submodules whose attributes should stay synchronized with ``module_name``.
    install_on_submodules:
        When ``True`` (default), also sets ``__class__ = _ShimModule`` on each module
        in ``sync_modules`` so mutations on a submodule propagate back to the parent
        and sibling modules.
    only_existing_attrs:
        When ``True``, attribute assignments only propagate to peers that already define
        the attribute (plus ``root_mod`` and ``always_sync_targets``).
    always_sync_targets:
        Modules (or module names in ``sys.modules``) that always receive propagated
        attributes even when ``only_existing_attrs=True``.
    extra_module_names:
        Additional module names in ``sys.modules`` resolved dynamically on each mutation
        (useful for breakable circular shim references).
    skip_attrs:
        Additional module-local attribute names that should never propagate.
    delegated_attrs:
        Mapping of ``attr_name -> target_module_or_module_name``. Module instances are
        read live in ``__getattribute__`` on ``root_mod``; string module names are
        imported lazily in ``__getattr__`` / ``__setattr__`` / ``__delattr__``.
    lazy_getattr:
        Optional fallback hook ``(name: str) -> Any`` invoked before submodule fallback.
    fallback_getattr:
        When ``True`` (default), missing attributes on the module fall back to scanning
        ``sync_modules``.
    sync_filter:
        Optional predicate ``(source_mod, target_mod, name, value) -> bool`` to veto
        specific cross-module attribute writes.
    seed_submodules_from_root:
        When ``True``, copies non-dunder attributes from ``root_mod`` into each submodule
        via ``setdefault`` before activating the shim.
    bidirectional_seed:
        When ``True``, copies non-dunder attributes across all participating modules
        via ``setdefault`` in both directions.
    """
    root_mod = sys.modules[module_name]
    all_modules: list[ModuleType] = [root_mod]
    for mod in sync_modules:
        if mod is not None and all(mod is not existing for existing in all_modules):
            all_modules.append(mod)

    merged_skip = _DEFAULT_SKIP_ATTRS | frozenset(skip_attrs)

    if seed_submodules_from_root:
        for mod in all_modules:
            if mod is root_mod:
                continue
            for k, v in list(root_mod.__dict__.items()):
                if k not in merged_skip and not k.startswith("__"):
                    mod.__dict__.setdefault(k, v)

    if bidirectional_seed:
        for mod_a in all_modules:
            for mod_b in all_modules:
                if mod_a is mod_b:
                    continue
                for k, v in list(mod_a.__dict__.items()):
                    if k not in merged_skip and not k.startswith("__"):
                        mod_b.__dict__.setdefault(k, v)

    live_delegated: dict[str, ModuleType] = {}
    lazy_delegated: dict[str, str] = {}
    if delegated_attrs:
        for attr_name, target in delegated_attrs.items():
            if isinstance(target, str):
                lazy_delegated[attr_name] = target
            elif isinstance(target, ModuleType):
                live_delegated[attr_name] = target

    always_ids: set[int] = {id(root_mod)}
    extra_names: list[str] = list(extra_module_names)
    for target in always_sync_targets:
        if isinstance(target, str):
            if target not in extra_names:
                extra_names.append(target)
            resolved = sys.modules.get(target)
            if resolved is not None:
                always_ids.add(id(resolved))
        elif isinstance(target, ModuleType):
            always_ids.add(id(target))

    rule = _ShimRule(
        root_mod=root_mod,
        all_modules=all_modules,
        only_existing_attrs=only_existing_attrs,
        always_sync_targets=always_ids,
        extra_module_names=tuple(extra_names),
        skip_attrs=merged_skip,
        live_delegated=live_delegated,
        lazy_delegated=lazy_delegated,
        lazy_getattr=lazy_getattr,
        fallback_getattr=fallback_getattr,
        sync_filter=sync_filter,
    )

    targets_to_install = all_modules if install_on_submodules else [root_mod]
    for mod in targets_to_install:
        rules = _get_rules(mod)
        rules.append(rule)
        if not isinstance(mod, _ShimModule):
            mod.__class__ = _ShimModule
        mod.__dict__["_SHIM_INITIALIZED"] = True

    root_mod.__dict__["_ShimModule"] = _ShimModule
    return _ShimModule


__all__ = [
    "SyncFilter",
    "_ShimModule",
    "install_shim_module",
]
