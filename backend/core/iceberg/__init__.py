"""``backend.core.iceberg`` — DuckLake / Parquet integration for FOS log analysis.

This file re-exports ``_core.py`` symbols so the historical flat-module import
surface keeps working:

    from backend.core.iceberg import init_iceberg_table, get_arrow_schema, ...
    from backend.core import iceberg; iceberg.init_iceberg_table(src)
    monkeypatch.setattr("backend.core.iceberg._buffer_dir", ...)

We install a ``ModuleType`` proxy into ``sys.modules`` whose ``__setattr__``
mirrors writes to ``_core``, so test patches reach the actual call sites.
"""

from __future__ import annotations

import sys as _sys
import types as _types

from backend.core.iceberg import _core as _core_module  # noqa: F401
from backend.core.iceberg._core import *  # noqa: F401,F403


class _IcebergPackageProxy(_types.ModuleType):
    """Delegates attribute reads/writes to ``_core`` while preserving package metadata."""

    _PACKAGE_ONLY = frozenset(
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
            "_core",
            "_ducklake",
            "buffer",
            "lake_info",
            "manifest",
            "rum_schema",
            "sync",
            "view",
        }
    )

    def __getattr__(self, name):  # only called when normal lookup fails
        return getattr(_core_module, name)

    def __setattr__(self, name, value):
        if name in self._PACKAGE_ONLY or name.startswith("__"):
            object.__setattr__(self, name, value)
            return
        setattr(_core_module, name, value)
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name in self._PACKAGE_ONLY or name.startswith("__"):
            object.__delattr__(self, name)
            return
        if hasattr(_core_module, name):
            try:
                delattr(_core_module, name)
            except AttributeError:
                pass
        try:
            object.__delattr__(self, name)
        except AttributeError:
            pass


_self = _sys.modules[__name__]
_proxy = _IcebergPackageProxy(__name__)
_proxy.__dict__.update(_self.__dict__)
for _k, _v in vars(_core_module).items():
    if _k.startswith("__"):
        continue
    if _k in _proxy.__dict__:
        continue
    _proxy.__dict__[_k] = _v
del _k, _v
_proxy.__path__ = _self.__path__  # type: ignore[attr-defined]
_proxy.__file__ = _self.__file__
_proxy.__spec__ = _self.__spec__
from backend.core.iceberg._core import (
    DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S,
    DuckLakeAdmissionTimeout,
    _ducklake_attach,
    _reset_for_tests,
    ducklake_current_snapshot_id,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
)

_proxy.__dict__["_ducklake"] = _core_module
_proxy.__dict__["DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S"] = DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S
_proxy.__dict__["DuckLakeAdmissionTimeout"] = DuckLakeAdmissionTimeout
_proxy.__dict__["_ducklake_attach"] = _ducklake_attach
_proxy.__dict__["_reset_for_tests"] = _reset_for_tests
_proxy.__dict__["ducklake_current_snapshot_id"] = ducklake_current_snapshot_id
_proxy.__dict__["ducklake_table_name"] = ducklake_table_name
_proxy.__dict__["ducklake_write_admission"] = ducklake_write_admission
_proxy.__dict__["get_admission_stats"] = get_admission_stats
_proxy.__dict__["lake_info"] = _proxy
_proxy.__dict__["manifest"] = _proxy
_proxy.__dict__["rum_schema"] = _proxy
_proxy.__dict__["sync"] = _proxy
_sys.modules[__name__] = _proxy
_sys.modules[f"{__name__}._ducklake"] = _core_module
_sys.modules[f"{__name__}.lake_info"] = _proxy
_sys.modules[f"{__name__}.manifest"] = _proxy
_sys.modules[f"{__name__}.rum_schema"] = _proxy
_proxy.__dict__["view"] = _proxy
_proxy.__dict__["buffer"] = _core_module
_sys.modules[f"{__name__}.sync"] = _proxy
_sys.modules[f"{__name__}.view"] = _proxy
_sys.modules[f"{__name__}.buffer"] = _core_module
_sys.modules["backend.core.local_compaction"] = _core_module

del _self, _proxy
