"""Backend package initialization with zero-eager-import module aliasing.

Installs a lightweight ``sys.meta_path`` finder/loader that maps consolidated
submodules to their folded target modules on first import without importing
any backend submodule at package initialization time (keeping grimp /
lint-imports dependency graph clean).
"""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
import types
from collections.abc import Sequence

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

_MODULE_ALIASES: dict[str, str] = {
    "backend.core.local_compaction": "backend.core.storage._core",
}


class _AliasLoader(importlib.abc.MetaPathFinder, importlib.abc.ExecutionLoader):
    def __init__(self) -> None:
        self._canon_specs: dict[str, tuple[importlib.machinery.ModuleSpec | None, importlib.abc.Loader | None]] = {}

    def _resolve_origin(self, fullname: str) -> tuple[str, str | None]:
        target_name = _MODULE_ALIASES.get(fullname, fullname)
        target_mod = sys.modules.get(target_name)
        if target_mod is not None:
            mod_file = getattr(target_mod, "__file__", None)
            if mod_file:
                return target_name, mod_file
        if target_name.startswith("backend."):
            rel_parts = target_name.split(".")[1:]
            candidate_py = os.path.join(_BACKEND_DIR, *rel_parts) + ".py"
            if os.path.isfile(candidate_py):
                return target_name, candidate_py
            candidate_init = os.path.join(_BACKEND_DIR, *rel_parts, "__init__.py")
            if os.path.isfile(candidate_init):
                return target_name, candidate_init
        spec = importlib.machinery.PathFinder.find_spec(target_name)
        return target_name, (spec.origin if spec else None)

    def _has_real_file(self, fullname: str) -> bool:
        if not fullname.startswith("backend."):
            return False
        rel_parts = fullname.split(".")[1:]
        candidate_py = os.path.join(_BACKEND_DIR, *rel_parts) + ".py"
        candidate_init = os.path.join(_BACKEND_DIR, *rel_parts, "__init__.py")
        return os.path.isfile(candidate_py) or os.path.isfile(candidate_init)

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: types.ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if self._has_real_file(fullname):
            _MODULE_ALIASES.pop(fullname, None)
            return None
        if fullname in _MODULE_ALIASES:
            _, origin = self._resolve_origin(fullname)
            return importlib.machinery.ModuleSpec(fullname, self, origin=origin)
        return None

    def is_package(self, fullname: str) -> bool:
        return False

    def get_filename(self, fullname: str) -> str:
        _, origin = self._resolve_origin(fullname)
        return origin or ""

    def get_source(self, fullname: str) -> str | None:
        target_name, origin = self._resolve_origin(fullname)
        if origin and origin.endswith(".py"):
            return importlib.machinery.SourceFileLoader(target_name, origin).get_source(target_name)
        return None

    def get_code(self, fullname: str) -> types.CodeType | None:
        target_name, origin = self._resolve_origin(fullname)
        if origin and origin.endswith(".py"):
            return importlib.machinery.SourceFileLoader(target_name, origin).get_code(target_name)
        return None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> types.ModuleType | None:
        target_name = _MODULE_ALIASES[spec.name]
        mod = importlib.import_module(target_name)
        if target_name not in self._canon_specs and not isinstance(getattr(mod, "__loader__", None), _AliasLoader):
            self._canon_specs[target_name] = (getattr(mod, "__spec__", None), getattr(mod, "__loader__", None))
        sys.modules[spec.name] = mod
        parent_name, _, child_name = spec.name.rpartition(".")
        parent_mod = sys.modules.get(parent_name)
        if parent_mod is not None:
            setattr(parent_mod, child_name, mod)
        return mod

    def exec_module(self, module: types.ModuleType) -> None:
        requested_name = module.__name__
        canon_name = _MODULE_ALIASES.get(requested_name, requested_name)
        module.__name__ = canon_name
        canon_spec, _canon_loader = self._canon_specs.get(canon_name, (None, None))
        if canon_spec is not None:
            canon_spec.loader = self
            module.__spec__ = canon_spec
        module.__loader__ = self
        for alias, target_name in list(_MODULE_ALIASES.items()):
            if target_name == canon_name:
                if self._has_real_file(alias):
                    _MODULE_ALIASES.pop(alias, None)
                    continue
                sys.modules[alias] = module
                parent_name, _, child_name = alias.rpartition(".")
                parent_mod = sys.modules.get(parent_name)
                if parent_mod is not None:
                    setattr(parent_mod, child_name, module)


if not any(isinstance(f, _AliasLoader) for f in sys.meta_path):
    sys.meta_path.insert(0, _AliasLoader())
