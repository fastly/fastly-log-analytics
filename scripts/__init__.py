"""Script package root and module alias registry for consolidated script modules."""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
from types import ModuleType

_MODULE_ALIASES: dict[str, str] = {
    "scripts.check_environment_health": "scripts.lib.environments",
    "scripts.dev.validate_credentials": "scripts.lib.environments",
    "validate_credentials": "scripts.lib.environments",
    "scripts.lib.port_forward": "scripts.lib.environments",
    "scripts.lib.admin_tls": "scripts.lib.environments",
    "scripts.lib.observability_ui": "scripts.lib.readiness",
    "scripts.dev.profile_insights": "scripts.lib.section_measurements",
}


class _AliasLoader(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Redirect legacy script module paths to their consolidated canonical modules."""

    @classmethod
    def find_spec(
        cls,
        fullname: str,
        path: object = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname not in _MODULE_ALIASES:
            return None
        canonical_spec = importlib.util.find_spec(_MODULE_ALIASES[fullname])
        if canonical_spec is None:
            return None
        spec = importlib.machinery.ModuleSpec(fullname, cls, origin=canonical_spec.origin)
        spec.has_location = bool(canonical_spec.origin)
        return spec

    _canonical_attrs: dict[str, tuple[str, object]] = {}

    @classmethod
    def create_module(cls, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        target = importlib.import_module(_MODULE_ALIASES[spec.name])
        cls._canonical_attrs[spec.name] = (target.__name__, target.__spec__)
        sys.modules[spec.name] = target
        return target

    @classmethod
    def exec_module(cls, module: ModuleType) -> None:
        alias_name = getattr(getattr(module, "__spec__", None), "name", "")
        if alias_name in cls._canonical_attrs:
            orig_name, orig_spec = cls._canonical_attrs.pop(alias_name)
            module.__name__ = orig_name
            module.__spec__ = orig_spec  # type: ignore[assignment]

    @classmethod
    def get_code(cls, fullname: str):
        target_name = _MODULE_ALIASES.get(fullname)
        if not target_name:
            return None
        canonical_spec = importlib.util.find_spec(target_name)
        if canonical_spec and canonical_spec.loader and hasattr(canonical_spec.loader, "get_code"):
            return canonical_spec.loader.get_code(target_name)
        return None


if _AliasLoader not in sys.meta_path:
    sys.meta_path.insert(0, _AliasLoader)
