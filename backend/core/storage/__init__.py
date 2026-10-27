"""``backend.core.storage`` — DuckLake / Parquet integration for FOS log analysis.

This package re-exports the ``backend.core.storage`` surface across its domain
submodules (``catalog``, ``view``, ``buffer``, ``compaction``) and ``_core`` so
the historical flat-module import surface keeps working:

    from backend.core.storage import init_storage_table, get_arrow_schema, ...
    from backend.core import storage; storage.init_storage_table(src)
    monkeypatch.setattr("backend.core.storage._buffer_dir", ...)
"""

from __future__ import annotations

import sys as _sys

from backend.core.storage import (
    _core as _core_module,
)
from backend.core.storage import (
    buffer as buffer,
)
from backend.core.storage import (
    catalog as catalog,
)
from backend.core.storage import (
    compaction as compaction,
)
from backend.core.storage import (
    view as view,
)
from backend.core.storage._core import *  # noqa: F401,F403
from backend.core.storage._core import __all__ as __all__
from backend.core.storage._core import register_storage_module

_self = _sys.modules[__name__]
register_storage_module(_self)

_self.__dict__["_core"] = _core_module
_self.__dict__["_ducklake"] = _core_module
_self.__dict__["buffer"] = buffer
_self.__dict__["catalog"] = catalog
_self.__dict__["compaction"] = compaction
_self.__dict__["view"] = view
_self.__dict__["manifest"] = _self
_self.__dict__["rum_schema"] = _self
_self.__dict__["sync"] = _self

_sys.modules[f"{__name__}._ducklake"] = _core_module
_sys.modules[f"{__name__}.manifest"] = _self
_sys.modules[f"{__name__}.rum_schema"] = _self
_sys.modules[f"{__name__}.sync"] = _self
_sys.modules["backend.core.local_compaction"] = _core_module

del _self
