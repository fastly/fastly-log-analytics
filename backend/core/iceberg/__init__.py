"""``backend.core.iceberg`` — PyIceberg integration for FOS log analysis.

Module carve (Phase 4a, refactor/cleanup):
- ``fs``    — s3fs/botocore monkeypatches. MUST import first so the seams
              are in place before pyiceberg/s3fs are touched anywhere.
- ``_core`` — everything else (schema, catalog, commits, sync, views).

This file re-exports the union of ``fs.py`` and ``_core.py`` symbols so the
historical flat-module import surface keeps working:

    from backend.core.iceberg import init_iceberg_table, _get_catalog, ...
    from backend.core import iceberg; iceberg._warehouse_uri(src)
    monkeypatch.setattr("backend.core.iceberg._warehouse_uri", ...)

The third pattern is the load-bearing one: pytest's ``monkeypatch.setattr``
sets the attribute on the ``backend.core.iceberg`` module object. If we
just did ``from ._core import *``, the patched name would live on the
package while the real call site (inside ``_core``) would keep resolving
to the original. We work around that by installing a ``ModuleType`` proxy
into ``sys.modules`` whose ``__setattr__`` mirrors writes to ``_core``,
so test patches reach the actual call sites.
"""

from __future__ import annotations

# Imports below are intentionally NOT sorted by ruff/isort — the order is
# load-bearing. ``fs`` must execute first so the s3fs/botocore monkeypatches
# are installed before ``_core`` triggers any pyiceberg import (pyiceberg
# itself imports s3fs lazily, but other call paths can race it). Keep the
# isort: skip_file directive on this module.
# isort: skip_file
# ruff: noqa: I001
import sys as _sys
import types as _types

from backend.core.iceberg import fs as _fs_module  # noqa: F401
from backend.core.iceberg import _core as _core_module  # noqa: F401

# Re-export everything from _core (which itself re-exports from fs). The
# wildcard is intentional: tests reach in by name for many private helpers
# (``_get_catalog``, ``_DUCKDB_TO_ICEBERG``, ``_buffer_dir``,
# ``_catalog_cache``, etc.), so the public surface is "every non-dunder
# name defined in _core or fs".
from backend.core.iceberg._core import *  # noqa: F401,F403


class _IcebergPackageProxy(_types.ModuleType):
    """Delegates attribute reads/writes to ``_core`` while preserving the
    ``fs`` submodule and package metadata.

    Reads: fall back to ``_core`` for any name not found on the package
    itself (so callers can reach every helper, including names added to
    ``_core`` after this module is constructed).

    Writes: forwarded to ``_core`` so ``monkeypatch.setattr`` reaches the
    real call sites. Dunder attributes and the explicit submodule names
    (``fs``, ``_core``) are kept on the package itself so the import
    machinery and ``sys.modules`` lookup stay consistent.
    """

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
            "fs",
            "_core",
        }
    )

    def __getattr__(self, name):  # only called when normal lookup fails
        # Names starting with ``_`` aren't picked up by ``from fs import *``
        # in _core, so the proxy must consult fs explicitly. _core takes
        # precedence so any future re-binding via ``setattr`` (which forwards
        # to _core) is reflected.
        try:
            return getattr(_core_module, name)
        except AttributeError:
            return getattr(_fs_module, name)

    def __setattr__(self, name, value):
        if name in self._PACKAGE_ONLY or name.startswith("__"):
            object.__setattr__(self, name, value)
            return
        # Mirror to _core/fs so call sites inside those modules see the new
        # value. The patched s3fs methods close over fs module globals;
        # _core's helpers read _core's globals.
        in_core = name in vars(_core_module)
        in_fs = name in vars(_fs_module)
        if in_core:
            setattr(_core_module, name, value)
        if in_fs:
            setattr(_fs_module, name, value)
        if not in_core and not in_fs:
            # New name (e.g. a test adding an attribute) — mirror to _core so
            # any future _core code that references it sees the value.
            setattr(_core_module, name, value)
        # Only commit to the proxy's own __dict__ when _core owns the name.
        # Fs-only names (e.g. ``_manifest_cache_size`` which gets rebound by
        # ``global`` inside ``_cache_put``) must always fall through to
        # ``__getattr__`` so reads see the live fs value, not a stale snapshot.
        if in_core or (not in_fs):
            object.__setattr__(self, name, value)
        else:
            # Drop the proxy-level shadow if one already exists, so future
            # reads fall through to fs.
            self.__dict__.pop(name, None)

    def __delattr__(self, name):
        # Mirror deletions to _core/fs so the package and its submodules
        # stay in sync. (Without this, ``unittest.mock.patch`` exiting on
        # an attribute it created — ``create=True`` — would leave a stale
        # entry in _core's namespace.)
        if name in self._PACKAGE_ONLY or name.startswith("__"):
            object.__delattr__(self, name)
            return
        if hasattr(_core_module, name):
            try:
                delattr(_core_module, name)
            except AttributeError:
                pass
        if hasattr(_fs_module, name):
            try:
                delattr(_fs_module, name)
            except AttributeError:
                pass
        try:
            object.__delattr__(self, name)
        except AttributeError:
            pass


_self = _sys.modules[__name__]
_proxy = _IcebergPackageProxy(__name__)
# Copy over everything the original module accumulated (including the wildcard
# imports above) so direct attribute access keeps working without falling
# through to ``__getattr__`` for the common case.
_proxy.__dict__.update(_self.__dict__)
# Pre-populate every name from _core (including underscore-prefixed helpers
# the wildcard import skips). This is load-bearing for ``unittest.mock.patch``:
# its ``is_local`` check uses ``attr in target.__dict__`` to decide whether to
# restore-via-setattr (correct) vs. delattr-on-exit (wipes the value from
# _core too via our mirroring ``__setattr__``). Without pre-populating,
# every ``patch("backend.core.iceberg._foo")`` would permanently destroy the
# real ``_core._foo`` on context exit.
#
# We do NOT pre-populate fs-only names. fs has scalar module globals that
# get rebound from inside fs functions (``_manifest_cache_size`` via
# ``global`` in ``_cache_put``); a snapshot copy in the proxy would diverge
# from the live value. Letting those fall through to ``__getattr__`` (which
# reads from fs) keeps the package view consistent with the actual state.
for _k, _v in vars(_core_module).items():
    if _k.startswith("__"):
        continue
    if _k in _proxy.__dict__:
        continue
    _proxy.__dict__[_k] = _v
del _k, _v
# Restore package metadata that update() may have copied verbatim.
_proxy.__path__ = _self.__path__  # type: ignore[attr-defined]
_proxy.__file__ = _self.__file__
_proxy.__spec__ = _self.__spec__
_sys.modules[__name__] = _proxy

# Don't leak the construction helpers as iceberg attributes.
del _self, _proxy
