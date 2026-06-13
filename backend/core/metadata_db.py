"""Backward-compatible shim for the carved ``backend.core.metadata`` package.

The historical monolith ``backend.core.metadata_db`` (3168 lines of
per-service-SQLite helpers) has been split into the cohesive
``backend.core.metadata`` package. Every public symbol — connection
management, schema, alerts, views, audit, ingested-files, cron runs,
ASN cache, source registry, usage log, retention/cleanup, and the
test-facing module globals (``_DATA_DIR``, ``_initialized``, ``_local``,
``_init_lock``, ``_init_schema``, ``_clear_ingested_filenames_cache``,
``_ingested_filenames_cache``, etc.) — is re-exported here so existing
callers using ``from backend.core import metadata_db`` or
``from backend.core.metadata_db import X`` continue to work unchanged.

New code should import from ``backend.core.metadata`` (or its concern-
specific submodules) directly. This shim stays for the import sites that
still spell the old path; deleting it is a separate breaking change.

Mutable module state — sharp edge worth pinning. ``_DATA_DIR`` /
``_initialized`` / ``_local`` / ``_init_lock`` are owned by
``backend.core.metadata.base``. The shim swaps in a custom module class
(``_ShimModule``) so that ``metadata_db.X = ...`` — the form used by
``monkeypatch.setattr`` and a handful of tests — is mirrored onto the base
module. Without the proxy, tests that patch ``metadata_db._DATA_DIR``
would rebind only the shim's attribute and leave the live ``get_con``
reading the un-patched value out of ``base``.
"""

from __future__ import annotations

import sys
from types import ModuleType

# Re-export the whole package surface. Star-import is intentional here —
# ``backend.core.metadata.__init__`` declares an explicit ``__all__`` that
# enumerates every public symbol plus the test-facing private ones, so this
# captures exactly the historical metadata_db surface.
from backend.core.metadata import *  # noqa: F401,F403
from backend.core.metadata import __all__  # noqa: F401
from backend.core.metadata import base as _base


class _ShimModule(ModuleType):
    """Module type that mirrors writes for shared state into ``metadata.base``.

    Reads stay on the shim's own ``__dict__`` for cheap attribute lookup;
    writes for the small set of attributes that ``get_con`` / ``teardown``
    consult dynamically are mirrored onto the base module so the live
    bindings actually swap. Any other attribute write falls through to the
    default module ``__setattr__`` semantics.
    """

    # The set of attributes whose canonical home is ``metadata.base``. When
    # one of these is rebound on the shim, mirror it onto base so the
    # connection-management functions see the swap.
    _MIRRORED_TO_BASE = frozenset(
        {
            "_DATA_DIR",
            "_initialized",
            "_local",
            "_init_lock",
            "_init_schema",
            "_SCHEMA",
            "_all_connections",
            "_all_connections_lock",
            "_ingested_filenames_cache",
            "_ingested_filenames_cache_lock",
            "_FILE_DATE_RE",
            "_ORPHAN_THRESHOLD_MINS",
            "_parse_file_date",
        }
    )

    def __setattr__(self, name: str, value) -> None:
        if name in self._MIRRORED_TO_BASE:
            setattr(_base, name, value)
        super().__setattr__(name, value)


# Swap this module's class so future ``setattr`` operations route through
# ``_ShimModule.__setattr__``. ``sys.modules[__name__]`` is the live module
# object; rebinding its ``__class__`` is a documented pattern for
# module-level descriptors (PEP 549 / 562 family).
sys.modules[__name__].__class__ = _ShimModule
